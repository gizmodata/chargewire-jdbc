package com.gizmodata.chargewire.jdbc;

import static com.gizmodata.chargewire.jdbc.FakeChargewireServer.json;
import static com.gizmodata.chargewire.jdbc.FakeChargewireServer.problem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@code arrowLosslessConversion}: the driver turns a DuckDB server's session option {@code
 * duckdb_arrow_lossless_conversion} on, in every session it opens, where sessions have the option;
 * a server without it is never asked.
 */
class LosslessTypesTest {
  private static final String OPTION = "duckdb_arrow_lossless_conversion";
  private final FakeChargewireServer server;

  LosslessTypesTest() throws Exception {
    server = new FakeChargewireServer();
    server.route(
        "PATCH",
        "/v1/sessions/([^/]+)",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"options\":{\"catalog\":\"cat\",\"schema\":\"main\",\""
                    + OPTION
                    + "\":true},"
                    + "\"errors\":{}}"));
  }

  @AfterEach
  void close() {
    server.close();
  }

  /** The bodies of the session requests after the sign-in, in order: {@code METHOD body}. */
  private List<String> sessionRequests() {
    List<String> sent = new ArrayList<>();
    synchronized (server.requests) {
      for (FakeChargewireServer.Request request : server.requests) {
        if (request.path.startsWith("/chargewire/v1/sessions")
            && !request.method.equals("DELETE")) {
          sent.add(request.method + " " + request.body());
        }
      }
    }
    return sent;
  }

  @Test
  void theOptionIsTurnedOnWhereASessionHasIt() throws Exception {
    server.losslessOption = false;
    try (Connection connection = DriverManager.getConnection(server.url(""))) {
      assertEquals(
          List.of(
              "POST {\"options\":{\"autocommit\":true}}",
              "PATCH {\"options\":{\"" + OPTION + "\":true}}"),
          sessionRequests());
      assertEquals(
          "/chargewire/v1/sessions/s1",
          server.requests("PATCH", "/v1/sessions/[^/]+").get(0).path,
          "in the session just opened");
    }
  }

  @Test
  void aServerWithoutTheOptionIsNotAsked() throws Exception {
    server.losslessOption = null;
    try (Connection connection = DriverManager.getConnection(server.url(""))) {
      assertEquals(List.of("POST {\"options\":{\"autocommit\":true}}"), sessionRequests());
    }
  }

  @Test
  void aSessionThatHasItOnAlreadyIsLeftAlone() throws Exception {
    server.losslessOption = true;
    try (Connection connection = DriverManager.getConnection(server.url(""))) {
      assertEquals(1, sessionRequests().size());
    }
  }

  @Test
  void withThePropertyOffNothingIsAsked() throws Exception {
    server.losslessOption = false;
    try (Connection connection =
        DriverManager.getConnection(server.url("arrowLosslessConversion=false"))) {
      assertEquals(List.of("POST {\"options\":{\"autocommit\":true}}"), sessionRequests());
    }
    SQLException invalid =
        assertThrows(
            SQLException.class,
            () -> DriverManager.getConnection(server.url("arrowLosslessConversion=maybe")));
    assertEquals("08001", invalid.getSQLState());
    assertEquals(
        "Invalid value 'maybe' for arrowLosslessConversion: expected true or false",
        invalid.getMessage());
  }

  /** The session that replaces a lost one gets the option too. */
  @Test
  void theSessionThatReplacesALostOneIsAskedAgain() throws Exception {
    server.losslessOption = false;
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> problem(exchange, 404, "session_not_found", ""));
    server.route(
        "POST",
        "/v1/sessions/s2/statements",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                FakeChargewireServer.document(
                    "q1", "succeeded", "{\"kind\":\"update\",\"rows_affected\":1}")));
    try (Connection connection = DriverManager.getConnection(server.url(""));
        java.sql.Statement statement = connection.createStatement()) {
      // The driver knows no transaction was open (it read the session), so it may run it again.
      assertTrue(connection.isValid(5));
      assertEquals(1, statement.executeUpdate("INSERT INTO t VALUES (1)"));
      List<FakeChargewireServer.Request> patches = server.requests("PATCH", "/v1/sessions/[^/]+");
      assertEquals(2, patches.size());
      assertNotEquals(patches.get(0).path, patches.get(1).path);
      assertEquals("/chargewire/v1/sessions/s2", patches.get(1).path);
    }
  }
}
