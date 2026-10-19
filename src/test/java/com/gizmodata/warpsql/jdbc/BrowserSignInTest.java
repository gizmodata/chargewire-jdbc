package com.gizmodata.warpsql.jdbc;

import static com.gizmodata.warpsql.jdbc.FakeWarpServer.json;
import static com.gizmodata.warpsql.jdbc.FakeWarpServer.problem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Browser sign-in ({@code authType=oauth_browser}, spec §4.3) against a fake whose flow answers
 * {@code pending} a few times and then what the test chose.
 */
class BrowserSignInTest {
  private static final String SECRET = "poll-secret-7f3a";
  private static final String TOKEN = "browser-token-91";
  private static final String AUTHORIZE = "https://idp.example/authorize?state=abc&nonce=n";

  private final FakeWarpServer server;
  private final AtomicInteger polls = new AtomicInteger();
  private final List<Long> pollTimes = new ArrayList<>();
  private volatile int pendingPolls = 1;
  private volatile String outcome =
      "{\"state\":\"complete\",\"access_token\":\""
          + TOKEN
          + "\",\"token_type\":\"Bearer\",\"expires_in\":3600}";
  private volatile int pollStatus = 200;
  private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
  private PrintStream savedErr;

  @TempDir Path temp;

  BrowserSignInTest() throws Exception {
    server = new FakeWarpServer();
    server.capabilities = "[\"long_poll\",\"oauth_browser\"]";
    server.authExtra = ",\"oauth_browser\":{\"provider\":\"Test IdP\"}";
    server.route(
        "POST",
        "/v1/auth/oauth/flows",
        (exchange, request, path) -> {
          exchange.getResponseHeaders().set("Location", "/warp/v1/auth/oauth/flows/f_1");
          json(
              exchange,
              201,
              "{\"flow_id\":\"f_1\",\"poll_secret\":\""
                  + SECRET
                  + "\",\"authorize_url\":\""
                  + AUTHORIZE
                  + "\",\"poll_interval_s\":1,\"expires_at\":\"2030-01-01T00:00:00Z\"}");
        });
    server.route(
        "POST",
        "/v1/auth/oauth/flows/([^/]+)",
        (exchange, request, path) -> {
          synchronized (pollTimes) {
            pollTimes.add(System.nanoTime());
          }
          if (!"f_1".equals(path.group(1))
              || !request.body().equals("{\"poll_secret\":\"" + SECRET + "\"}")) {
            problem(exchange, 404, "flow_not_found", "");
            return;
          }
          if (polls.incrementAndGet() <= pendingPolls) {
            json(exchange, 200, "{\"state\":\"pending\"}");
          } else if (pollStatus != 200) {
            problem(exchange, pollStatus, "flow_not_found", "");
          } else {
            json(exchange, 200, outcome);
          }
        });
  }

  @BeforeEach
  void captureStderr() {
    savedErr = System.err;
    System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
  }

  @AfterEach
  void close() {
    System.setErr(savedErr);
    server.close();
  }

  private String url(String options) {
    return "jdbc:warp://127.0.0.1:" + server.port() + "?useEncryption=false&" + options;
  }

  /** A "browser" that writes the URL it was given to a file. */
  private Path opener(Path out) throws Exception {
    Path script = temp.resolve("open.sh");
    Files.writeString(script, "#!/bin/sh\nprintf '%s' \"$1\" > '" + out + "'\n");
    Files.setPosixFilePermissions(script, PosixFilePermissions.fromString("rwx------"));
    return script;
  }

  @Test
  void signsInAndUsesTheFlowsToken() throws Exception {
    Path opened = temp.resolve("opened.txt");
    try (Connection connection =
        DriverManager.getConnection(url("authType=oauth_browser&oauthBrowser=" + opener(opened)))) {
      assertFalse(connection.isClosed());
    }
    // The browser got the identity provider's URL.
    for (int i = 0; i < 50 && !Files.exists(opened); i++) {
      Thread.sleep(20);
    }
    assertEquals(AUTHORIZE, Files.readString(opened));
    // The session was opened with the flow's token.
    assertEquals(
        "Bearer " + TOKEN, server.requests("POST", "/v1/sessions").get(0).header("Authorization"));
    // The secret travelled only in poll bodies: never in a URL, a query or a header.
    for (FakeWarpServer.Request request : server.requests) {
      assertFalse((request.path + request.query).contains(SECRET), request.path);
      request.headers.values().forEach(v -> assertFalse(v.toString().contains(SECRET)));
    }
    assertEquals(2, server.requests("POST", "/v1/auth/oauth/flows/f_1").size());
    assertEquals(0, server.requests("POST", "/v1/auth/token").size());
    String messages = stderr.toString(StandardCharsets.UTF_8);
    assertTrue(messages.contains("fake with Test IdP"), messages);
    assertFalse(messages.contains(SECRET) || messages.contains(TOKEN), messages);
  }

  @Test
  void externalIsAnAliasAndNonePrintsTheUrl() throws Exception {
    try (Connection connection =
        DriverManager.getConnection(url("authType=EXTERNAL&oauthBrowser=none"))) {
      assertFalse(connection.isClosed());
    }
    String messages = stderr.toString(StandardCharsets.UTF_8);
    assertTrue(messages.contains("Open this URL in a browser") && messages.contains(AUTHORIZE));
  }

  @Test
  void pollsNoFasterThanTheServerAsks() throws Exception {
    pendingPolls = 2;
    try (Connection connection =
        DriverManager.getConnection(url("authType=oauth_browser&oauthBrowser=none"))) {
      assertFalse(connection.isClosed());
    }
    synchronized (pollTimes) {
      assertEquals(3, pollTimes.size());
      for (int i = 1; i < pollTimes.size(); i++) {
        long gapMillis = (pollTimes.get(i) - pollTimes.get(i - 1)) / 1_000_000;
        assertTrue(gapMillis >= 900, "polled after " + gapMillis + " ms");
      }
    }
  }

  @Test
  void refusedWithoutTheCapability() {
    server.capabilities = "[\"long_poll\"]";
    SQLException e =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(url("authType=oauth_browser&oauthBrowser=none")));
    assertTrue(e.getMessage().contains("no capability oauth_browser"), e.getMessage());
    assertEquals("28000", e.getSQLState());
    assertEquals(0, server.requests("POST", "/v1/auth/oauth/flows").size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"access_denied", "invalid_credentials"})
  void aRefusedSignInIsAnAuthorizationError(String code) {
    outcome = "{\"state\":\"failed\",\"error\":{\"code\":\"" + code + "\",\"detail\":\"not you\"}}";
    SQLException e =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(url("authType=oauth_browser&oauthBrowser=none")));
    assertEquals("28000", e.getSQLState());
    assertTrue(e.getMessage().contains(code) && e.getMessage().contains("not you"), e.getMessage());
    assertEquals(0, server.requests("POST", "/v1/sessions").size());
  }

  @Test
  void anIdentityProviderFailureIsAConnectionError() {
    outcome =
        "{\"state\":\"failed\",\"error\":{\"code\":\"identity_provider_error\","
            + "\"detail\":\"unreachable\"}}";
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> DriverManager.getConnection(url("authType=oauth_browser&oauthBrowser=none")));
    assertEquals("08004", e.getSQLState());
    assertTrue(
        e.getMessage().contains("identity_provider_error: unreachable")
            || e.getMessage().contains("(identity_provider_error): unreachable"),
        e.getMessage());
  }

  @Test
  void anExpiredOrCollectedFlowSaysSo() {
    pollStatus = 404;
    SQLException e =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(url("authType=oauth_browser&oauthBrowser=none")));
    assertTrue(e.getMessage().contains("expired or was already used"), e.getMessage());
  }

  @Test
  void givesUpAfterTheTimeout() {
    pendingPolls = Integer.MAX_VALUE;
    long started = System.nanoTime();
    SQLException e =
        assertThrows(
            SQLTimeoutException.class,
            () ->
                DriverManager.getConnection(
                    url("authType=oauth_browser&oauthBrowser=none&oauthTimeout=1")));
    assertTrue(e.getMessage().contains("not completed within 1 s"), e.getMessage());
    assertTrue((System.nanoTime() - started) / 1_000_000 < 5000);
  }

  @ParameterizedTest
  @ValueSource(strings = {"user=joe", "password=pw", "token=t", "subjectToken=s"})
  void takesNoOtherCredential(String credential) {
    SQLException e =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () ->
                DriverManager.getConnection(
                    url("authType=oauth_browser&oauthBrowser=none&" + credential)));
    assertTrue(e.getMessage().contains("remove " + credential.split("=")[0]), e.getMessage());
    assertEquals(0, server.requests("POST", "/v1/auth/oauth/flows").size());
  }

  @Test
  void anUnknownAuthTypeNamesTheChoices() {
    SQLException e =
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url("authType=sso")));
    assertTrue(e.getMessage().contains("oauth_browser (or external)"), e.getMessage());
  }

  @Test
  void aZeroTimeoutIsRefused() {
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> DriverManager.getConnection(url("authType=oauth_browser&oauthTimeout=0")));
    assertTrue(e.getMessage().contains("oauthTimeout"), e.getMessage());
  }
}
