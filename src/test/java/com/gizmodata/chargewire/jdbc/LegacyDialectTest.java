package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/** A connection at the default root falls back to a server from before the protocol's rename. */
class LegacyDialectTest {
  private static final String FORMER_DISCOVERY =
      "{\"protocol\":\"warp-sql\",\"protocol_versions\":[\"1\"],"
          + "\"server\":{\"name\":\"former\",\"version\":\"1.0.0\"},"
          + "\"capabilities\":[\"prepared_statements\",\"transactions\"],"
          + "\"auth\":{\"grant_types\":[\"password\"]}}";

  /** A fake that answers only at the former root, in the former spelling. */
  private static FakeChargewireServer formerServer() throws Exception {
    FakeChargewireServer server = new FakeChargewireServer("/warp");
    server.route(
        "GET",
        "",
        (exchange, request, path) -> FakeChargewireServer.json(exchange, 200, FORMER_DISCOVERY));
    server.route(
        "POST",
        "/v1/sessions/[^/]+/statements",
        (exchange, request, path) ->
            FakeChargewireServer.json(
                exchange,
                200,
                FakeChargewireServer.document(
                    "q1", "succeeded", "{\"kind\":\"update\",\"rows_affected\":0}")));
    return server;
  }

  private static String url(FakeChargewireServer server, String options) {
    return "jdbc:chargewire://127.0.0.1:"
        + server.port()
        + "?useEncryption=false&user=joe&password=pw"
        + (options.isEmpty() ? "" : "&" + options);
  }

  private static List<String> paths(FakeChargewireServer server) {
    List<String> paths = new ArrayList<>();
    synchronized (server.requests) {
      for (FakeChargewireServer.Request request : server.requests) {
        paths.add(request.method + " " + request.path);
      }
    }
    return paths;
  }

  @Test
  void onlyTheDefaultRootFallsBack() {
    assertSame(LegacyDialect.DIALECT, LegacyDialect.fallbackFor("/chargewire"));
    assertSame(LegacyDialect.DIALECT, LegacyDialect.fallbackFor("/chargewire/"));
    assertSame(LegacyDialect.DIALECT, LegacyDialect.fallbackFor("chargewire"));
    assertSame(
        LegacyDialect.DIALECT, LegacyDialect.fallbackFor(LegacyUrls.basePath("/warp")), "mapped");
    assertNull(LegacyDialect.fallbackFor("/db/chargewire"));
    assertNull(LegacyDialect.fallbackFor("/other"));
  }

  @Test
  void aFormerServerIsSpokenToInItsOwnSpellingSilently() throws Exception {
    PrintStream out = System.out;
    PrintStream err = System.err;
    ByteArrayOutputStream printed = new ByteArrayOutputStream();
    try (FakeChargewireServer server = formerServer()) {
      PrintStream capture = new PrintStream(printed, true, StandardCharsets.UTF_8);
      System.setOut(capture);
      System.setErr(capture);
      try (Connection connection = DriverManager.getConnection(url(server, ""));
          Statement statement = connection.createStatement()) {
        assertTrue(connection.isValid(1));
        statement.setQueryTimeout(5);
        statement.execute("SELECT 1");
      } finally {
        System.setOut(out);
        System.setErr(err);
      }
      List<String> paths = paths(server);
      assertEquals("GET /chargewire", paths.get(0), paths.toString());
      assertEquals(1, paths.stream().filter(p -> p.startsWith("GET /chargewire")).count());
      assertTrue(
          paths.subList(1, paths.size()).stream().allMatch(p -> p.contains(" /warp")),
          paths.toString());
      FakeChargewireServer.Request submitted =
          server.requests("POST", "/v1/sessions/[^/]+/statements").get(0);
      assertEquals("5000", submitted.header("Warp-Statement-Timeout"));
      assertNull(submitted.header("Chargewire-Statement-Timeout"));
    }
    assertEquals("", printed.toString(StandardCharsets.UTF_8), "the fallback is silent");
  }

  @Test
  void aFormerProblemTypeIsRead() throws Exception {
    try (FakeChargewireServer server = formerServer()) {
      server.route(
          "POST",
          "/v1/sessions",
          (exchange, request, path) ->
              FakeChargewireServer.send(
                  exchange,
                  403,
                  "application/problem+json",
                  "{\"type\":\"urn:warp-sql:error:read_only\",\"title\":\"Read only\",\"status\":403}"
                      .getBytes(StandardCharsets.UTF_8)));
      SQLException refused =
          assertThrows(SQLException.class, () -> DriverManager.getConnection(url(server, "")));
      assertEquals("read_only", ChargewireProblem.codeOf(refused), refused.toString());
    }
  }

  @Test
  void aCustomBasePathNeverFallsBack() throws Exception {
    try (FakeChargewireServer server = formerServer()) {
      SQLException refused =
          assertThrows(
              SQLException.class,
              () -> DriverManager.getConnection(url(server, "basePath=/db/chargewire")));
      assertEquals(List.of("GET /db/chargewire"), paths(server));
      assertFalse(
          refused.getMessage().toLowerCase(Locale.ROOT).contains("warp"), refused.toString());
    }
  }

  @Test
  void withoutAFormerServerTheFirstErrorStands() throws Exception {
    // Nothing at either root.
    try (FakeChargewireServer server = new FakeChargewireServer("/elsewhere")) {
      SQLException refused =
          assertThrows(SQLException.class, () -> DriverManager.getConnection(url(server, "")));
      assertEquals(List.of("GET /chargewire", "GET /warp"), paths(server));
      assertEquals(404, ChargewireProblem.of(refused).status());
      assertFalse(refused.toString().toLowerCase(Locale.ROOT).contains("warp"), refused.toString());
    }
    // Something at the former root, but not the former protocol.
    try (FakeChargewireServer server = new FakeChargewireServer("/warp")) {
      SQLException refused =
          assertThrows(SQLException.class, () -> DriverManager.getConnection(url(server, "")));
      assertEquals(List.of("GET /chargewire", "GET /warp"), paths(server));
      assertEquals(404, ChargewireProblem.of(refused).status());
      assertFalse(refused.toString().toLowerCase(Locale.ROOT).contains("warp"), refused.toString());
    }
  }

  @Test
  void theFormerProtocolAtTodaysRootIsStillRefused() throws Exception {
    try (FakeChargewireServer server = new FakeChargewireServer()) {
      server.route(
          "GET",
          "",
          (exchange, request, path) -> FakeChargewireServer.json(exchange, 200, FORMER_DISCOVERY));
      SQLException refused =
          assertThrows(SQLException.class, () -> DriverManager.getConnection(url(server, "")));
      assertTrue(refused.getMessage().contains("is not a Chargewire server"), refused.toString());
      assertEquals(List.of("GET /chargewire"), paths(server));
    }
  }

  @Test
  void todaysServerIsNeverAskedForTheFormerRoot() throws Exception {
    try (FakeChargewireServer server = new FakeChargewireServer()) {
      server.route(
          "POST",
          "/v1/sessions/[^/]+/statements",
          (exchange, request, path) ->
              FakeChargewireServer.json(
                  exchange,
                  200,
                  FakeChargewireServer.document(
                      "q1", "succeeded", "{\"kind\":\"update\",\"rows_affected\":0}")));
      try (Connection connection = DriverManager.getConnection(url(server, ""));
          Statement statement = connection.createStatement()) {
        statement.setQueryTimeout(5);
        statement.execute("SELECT 1");
      }
      List<String> paths = paths(server);
      assertTrue(paths.stream().noneMatch(p -> p.contains("/warp")), paths.toString());
      assertEquals(
          "5000",
          server
              .requests("POST", "/v1/sessions/[^/]+/statements")
              .get(0)
              .header("Chargewire-Statement-Timeout"));
    }
  }
}
