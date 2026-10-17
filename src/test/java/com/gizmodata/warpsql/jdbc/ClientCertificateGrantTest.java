package com.gizmodata.warpsql.jdbc;

import static com.gizmodata.warpsql.jdbc.FakeWarpServer.arrow;
import static com.gizmodata.warpsql.jdbc.FakeWarpServer.document;
import static com.gizmodata.warpsql.jdbc.FakeWarpServer.json;
import static com.gizmodata.warpsql.jdbc.FakeWarpServer.problem;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.arrow.TestStreams;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.KeyStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Base64;
import java.util.List;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The {@code client_certificate} grant (spec §4.4, {@code authType=client_certificate}) against an
 * HTTPS fake that records the client certificate of every request.
 */
class ClientCertificateGrantTest {
  private static final String CLIENT_SUBJECT = "CN=warp";
  private final BufferAllocator allocator = new RootAllocator();
  private final FakeWarpServer server;

  ClientCertificateGrantTest() throws Exception {
    server = new FakeWarpServer(serverContext());
    server.capabilities = "[\"long_poll\",\"result_retention\",\"mtls_identity\"]";
  }

  @AfterEach
  void close() {
    server.close();
    allocator.close();
  }

  static String cert(String name) {
    return Path.of("src", "test", "resources", "certs", name).toAbsolutePath().toString();
  }

  private static SSLContext serverContext() throws Exception {
    char[] password = "changeit".toCharArray();
    KeyStore keys = KeyStore.getInstance("PKCS12");
    try (InputStream in =
        ClientCertificateGrantTest.class.getResourceAsStream("/certs/server.p12")) {
      keys.load(in, password);
    }
    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keys, password);
    KeyStore trusted = KeyStore.getInstance("PKCS12");
    try (InputStream in =
        ClientCertificateGrantTest.class.getResourceAsStream("/certs/truststore.p12")) {
      trusted.load(in, password);
    }
    TrustManagerFactory trustManagers =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustManagers.init(trusted);
    SSLContext context = SSLContext.getInstance("TLS");
    context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
    return context;
  }

  private String url() {
    return "jdbc:warp://127.0.0.1:" + server.port();
  }

  /** TLS trusting the test CA, presenting the client certificate, signing in with it. */
  private static Properties withCertificate(String authType) {
    Properties properties = new Properties();
    properties.setProperty("tlsRootCerts", cert("ca.pem"));
    properties.setProperty("useSystemTrustStore", "false");
    properties.setProperty("clientCertificate", cert("client.pem"));
    properties.setProperty("clientKey", cert("client-key.pem"));
    properties.setProperty("authType", authType);
    return properties;
  }

  private void assertEveryRequestPresentedTheCertificate() {
    synchronized (server.requests) {
      assertFalse(server.requests.isEmpty());
      for (FakeWarpServer.Request request : server.requests) {
        assertEquals(
            CLIENT_SUBJECT,
            request.clientCertificate,
            request.method + " " + request.path + " came without the client certificate");
      }
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"client_certificate", "mtls", "CLIENT_CERTIFICATE"})
  void signsInWithTheCertificateAndNoPassword(String authType) throws Exception {
    try (Connection connection = DriverManager.getConnection(url(), withCertificate(authType))) {
      assertTrue(connection.isValid(1));
    }
    List<FakeWarpServer.Request> grants = server.requests("POST", "/v1/auth/token");
    assertEquals(1, grants.size());
    assertEquals("{\"grant_type\":\"client_certificate\"}", grants.get(0).body());
    assertEquals(
        "Bearer t1", server.requests("POST", "/v1/sessions").get(0).header("Authorization"));
    assertEveryRequestPresentedTheCertificate();
  }

  @Test
  void theTokenIsRenewedWithTheCertificate() throws Exception {
    server.expiresIn = 1;
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                document("q1", "succeeded", "{\"kind\":\"update\",\"rows_affected\":0}")));
    try (Connection connection =
            DriverManager.getConnection(url(), withCertificate("client_certificate"));
        Statement statement = connection.createStatement()) {
      statement.execute("SELECT 1");
    }
    List<FakeWarpServer.Request> grants = server.requests("POST", "/v1/auth/token");
    assertTrue(grants.size() >= 2, "renewed within 30 s of expiry: " + grants.size());
    for (FakeWarpServer.Request grant : grants) {
      assertEquals("{\"grant_type\":\"client_certificate\"}", grant.body());
    }
  }

  @Test
  void anExpiredBoundTokenIsRenewedWithTheCertificate() throws Exception {
    int[] calls = {0};
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) -> {
          if (++calls[0] == 1) {
            problem(exchange, 401, "invalid_token", "");
          } else {
            json(
                exchange,
                200,
                document("q1", "succeeded", "{\"kind\":\"update\",\"rows_affected\":4}"));
          }
        });
    try (Connection connection =
            DriverManager.getConnection(url(), withCertificate("client_certificate"));
        Statement statement = connection.createStatement()) {
      assertEquals(4, statement.executeUpdate("UPDATE t SET x = 1"));
    }
    assertEquals(2, server.requests("POST", "/v1/auth/token").size());
  }

  @Test
  void partitionReadersPresentTheCertificateToo() throws Exception {
    Schema ints = new Schema(List.of(Field.nullable("i", new ArrowType.Int(64, true))));
    byte[] schema = TestStreams.schemaStream(allocator, ints);
    String partitions =
        "[{\"index\":0,\"location\":null},{\"index\":1,\"location\":\"" + server.base() + "\"}]";
    server.route(
        "POST",
        "/v1/sessions/s1/statements",
        (exchange, request, path) ->
            json(
                exchange,
                201,
                document(
                    "q1",
                    "succeeded",
                    "{\"kind\":\"rows\",\"schema\":\""
                        + Base64.getEncoder().encodeToString(schema)
                        + "\",\"partitions\":"
                        + partitions
                        + ",\"retained\":true}")));
    byte[] first =
        TestStreams.stream(allocator, ints, List.of(List.<Object[]>of(new Object[] {1L})));
    byte[] second =
        TestStreams.stream(allocator, ints, List.of(List.<Object[]>of(new Object[] {2L})));
    server.route(
        "GET",
        "/v1/sessions/s1/statements/q1/result",
        (exchange, request, path) ->
            arrow(exchange, "q1", request.query.startsWith("partition=0") ? first : second));
    Properties properties = withCertificate("client_certificate");
    properties.setProperty("async", "true");
    try (Connection connection = DriverManager.getConnection(url(), properties);
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT i FROM split")) {
      long sum = 0;
      while (rows.next()) {
        sum += rows.getLong(1);
      }
      assertEquals(3, sum);
    }
    assertEquals(2, server.requests("GET", "/v1/sessions/s1/statements/q1/result").size());
    assertEveryRequestPresentedTheCertificate();
  }

  @Test
  void aServerWithoutTheCapabilityIsRefusedBeforeSigningIn() {
    server.capabilities = "[\"long_poll\"]";
    SQLInvalidAuthorizationSpecException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(url(), withCertificate("client_certificate")));
    assertEquals("28000", error.getSQLState());
    assertTrue(error.getMessage().contains("mtls_identity"), error.getMessage());
    assertTrue(server.requests("POST", "/v1/auth/token").isEmpty());
  }

  @Test
  void theServersRefusalKeepsItsCode() {
    server.route(
        "POST",
        "/v1/auth/token",
        (exchange, request, path) -> problem(exchange, 401, "unknown_certificate_identity", ""));
    SQLInvalidAuthorizationSpecException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(url(), withCertificate("client_certificate")));
    assertEquals("28000", error.getSQLState());
    assertEquals("unknown_certificate_identity", WarpProblem.codeOf(error));
  }

  // ---- Configurations the grant cannot work with: refused before anything is sent ----

  private void assertRefusedUpFront(Properties properties, String messagePart) {
    SQLException error =
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url(), properties));
    assertTrue(error.getMessage().contains(messagePart), error.getMessage());
    assertTrue(server.requests.isEmpty(), "nothing is sent for a bad configuration");
  }

  @Test
  void withoutACertificateTheGrantIsRefused() {
    Properties properties = withCertificate("client_certificate");
    properties.remove("clientCertificate");
    properties.remove("clientKey");
    assertRefusedUpFront(properties, "set clientCertificate and clientKey");
    SQLInvalidAuthorizationSpecException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(url(), properties));
    assertEquals("28000", error.getSQLState());
    Properties keyOnly = withCertificate("mtls");
    keyOnly.remove("clientCertificate");
    assertRefusedUpFront(keyOnly, "set clientCertificate and clientKey");
  }

  @ParameterizedTest
  @ValueSource(strings = {"user", "password", "token", "subjectToken"})
  void anotherCredentialBesideTheCertificateIsRefused(String key) {
    Properties properties = withCertificate("client_certificate");
    properties.setProperty(key, "x");
    assertRefusedUpFront(properties, "remove " + key);
  }

  @Test
  void theGrantNeedsTls() {
    Properties properties = withCertificate("client_certificate");
    properties.setProperty("useEncryption", "false");
    assertRefusedUpFront(properties, "useEncryption=true");
  }

  @Test
  void anUnknownAuthTypeIsRefused() {
    Properties properties = withCertificate("kerberos");
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(url(), properties));
    assertTrue(error.getMessage().contains("authType"), error.getMessage());
  }

  @Test
  void thePasswordGrantStaysTheDefault() throws Exception {
    Properties properties = withCertificate("password");
    properties.setProperty("user", "joe");
    properties.setProperty("password", "pw");
    try (Connection connection = DriverManager.getConnection(url(), properties)) {
      assertTrue(connection.isValid(1));
    }
    String body = server.requests("POST", "/v1/auth/token").get(0).body();
    assertTrue(body.contains("\"grant_type\":\"password\""), body);
  }
}
