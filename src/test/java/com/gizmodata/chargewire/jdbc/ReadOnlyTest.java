package com.gizmodata.chargewire.jdbc;

import static com.gizmodata.chargewire.jdbc.FakeChargewireServer.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReadOnlyTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  void creationCarriesReadOnlyAndCannotBeDowngraded() throws Exception {
    try (FakeChargewireServer server = new FakeChargewireServer();
        Connection connection = DriverManager.getConnection(server.url("readOnly=true"))) {
      assertTrue(connection.isReadOnly());
      var creation = server.requests.stream().filter(r -> r.path.endsWith("/sessions")).findFirst().orElseThrow();
      assertTrue(MAPPER.readTree(creation.body).path("options").path("read_only").booleanValue());
      assertEquals("25006", assertThrows(SQLException.class, () -> connection.setReadOnly(false)).getSQLState());
      connection.setReadOnly(true);
      assertTrue(connection.isReadOnly());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"{}", "{\"read_only\":false}", "{\"read_only\":\"true\"}"})
  void creationRequiresBooleanConfirmation(String options) throws Exception {
    try (FakeChargewireServer server = new FakeChargewireServer()) {
      server.route("POST", "/v1/sessions", (exchange, request, path) ->
          json(exchange, 201, "{\"session_id\":\"s1\",\"options\":" + options + "}"));
      SQLException error = assertThrows(SQLException.class, () -> DriverManager.getConnection(server.url("readOnly=true")));
      assertTrue(error.getMessage().contains("confirm read_only=true"), error.getMessage());
      assertTrue(server.requests.stream().anyMatch(r -> r.method.equals("DELETE") && r.path.endsWith("/sessions/s1")));
      assertFalse(server.requests.stream().anyMatch(r -> r.method.equals("PATCH") || r.path.endsWith("/statements")));
    }
  }

  @Test
  void lazyRequestIsOneWayAndGetterConfirmsIt() throws Exception {
    try (FakeChargewireServer server = new FakeChargewireServer()) {
      server.state = "stopped";
      try (Connection connection = DriverManager.getConnection(server.url(""))) {
        connection.setReadOnly(true);
        assertThrows(SQLException.class, () -> connection.setReadOnly(false));
        assertEquals(0, server.sessions.get());
        assertTrue(connection.isReadOnly());
        assertEquals(1, server.sessions.get());
      }
    }
  }

  @Test
  void patchMustConfirmAndMissingConfirmationClosesConnection() throws Exception {
    for (boolean confirmed : new boolean[] {false, true}) {
      try (FakeChargewireServer server = new FakeChargewireServer()) {
        server.route("PATCH", "/v1/sessions/s1", (exchange, request, path) ->
            json(exchange, 200, confirmed ? "{\"options\":{\"read_only\":true}}" : "{\"options\":{}}"));
        try (Connection connection = DriverManager.getConnection(server.url(""))) {
          if (confirmed) {
            connection.setReadOnly(true);
            assertTrue(connection.isReadOnly());
            assertThrows(SQLException.class, () -> connection.setReadOnly(false));
          } else {
            assertThrows(SQLException.class, () -> connection.setReadOnly(true));
            assertTrue(connection.isClosed());
            assertThrows(SQLException.class, connection::createStatement);
          }
        }
      }
    }
  }

  @Test
  void explicitRefusalIsReportedWithoutClaimingReadOnly() throws Exception {
    try (FakeChargewireServer server = new FakeChargewireServer()) {
      server.route("PATCH", "/v1/sessions/s1", (exchange, request, path) ->
          json(exchange, 200, "{\"options\":{},\"errors\":{\"read_only\":{\"code\":\"unknown_option\"}}}"));
      try (Connection connection = DriverManager.getConnection(server.url(""))) {
        assertThrows(SQLException.class, () -> connection.setReadOnly(true));
        assertFalse(connection.isReadOnly());
      }
    }
  }

  @Test
  void replacementReplaysServerImposedReadOnlyAndRequiresConfirmation() throws Exception {
    for (boolean confirmReplacement : new boolean[] {false, true}) {
      try (FakeChargewireServer server = new FakeChargewireServer()) {
        AtomicInteger creates = new AtomicInteger();
        server.route("POST", "/v1/sessions", (exchange, request, path) -> {
          int n = creates.incrementAndGet();
          if (n > 1) assertTrue(MAPPER.readTree(request.body).path("options").path("read_only").booleanValue());
          String options = n == 1 || confirmReplacement ? "{\"read_only\":true}" : "{}";
          json(exchange, 201, "{\"session_id\":\"s" + n + "\",\"transaction\":\"none\",\"options\":" + options + "}");
        });
        server.route("GET", "/v1/sessions/s1", (exchange, request, path) -> problem(exchange, 404, "session_not_found", ""));
        try (Connection connection = DriverManager.getConnection(server.url(""))) {
          assertTrue(connection.isReadOnly());
          ChargewireConnection raw = connection.unwrap(ChargewireConnection.class);
          if (confirmReplacement) {
            raw.withSession(id -> raw.client().getSession(id));
            assertTrue(connection.isReadOnly());
          } else {
            assertThrows(SQLException.class, () -> raw.withSession(id -> raw.client().getSession(id)));
          }
          assertEquals(2, creates.get());
        }
      }
    }
  }
}
