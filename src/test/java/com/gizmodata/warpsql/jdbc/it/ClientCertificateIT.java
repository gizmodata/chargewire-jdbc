package com.gizmodata.warpsql.jdbc.it;

import static com.gizmodata.warpsql.jdbc.it.TlsIT.cert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.gizmodata.warpsql.jdbc.WarpProblem;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.Statement;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Signing in with the mutual-TLS client certificate ({@code authType=client_certificate}, spec
 * §4.4): the identity is the certificate's e-mail subject alternative name, and the token the
 * server issues is bound to the certificate.
 */
class ClientCertificateIT {
  private static final String IDENTITY = "client@example.com";
  private static final Pattern TOKEN = Pattern.compile("\"access_token\"\\s*:\\s*\"([^\"]+)\"");
  private static final Pattern FIELD =
      Pattern.compile("\"(session_id|statement_id)\"\\s*:\\s*\"([^\"]+)\"");

  private static WarpServer server;

  @BeforeAll
  static void start() {
    server =
        WarpServer.start(
            true,
            "--tls-cert",
            cert("server.pem"),
            "--tls-key",
            cert("server-key.pem"),
            WarpServer.MTLS_FLAG,
            cert("ca.pem"),
            "--mtls-certificate-sign-in",
            "--mtls-admin-identities",
            IDENTITY);
  }

  @AfterAll
  static void stop() {
    server.close();
  }

  /** TLS trusting the test CA and presenting {@code client} (its file name prefix). */
  private static Properties presenting(String client) {
    Properties properties = new Properties();
    properties.setProperty("tlsRootCerts", cert("ca.pem"));
    properties.setProperty("clientCertificate", cert(client + ".pem"));
    properties.setProperty("clientKey", cert(client + "-key.pem"));
    return properties;
  }

  private static Properties signingInAs(String client) {
    Properties properties = presenting(client);
    properties.setProperty("authType", "client_certificate");
    return properties;
  }

  private static long scalar(Connection connection, String sql) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery(sql)) {
      assertTrue(rows.next());
      return rows.getLong(1);
    }
  }

  @Test
  void signsInWithTheCertificateAlone() throws SQLException {
    for (String authType : new String[] {"client_certificate", "mtls"}) {
      Properties properties = presenting("client");
      properties.setProperty("authType", authType);
      try (Connection connection = DriverManager.getConnection(server.url(""), properties)) {
        assertEquals(42, scalar(connection, "SELECT 42"));
      }
    }
  }

  @Test
  void aSplitResultIsReadWithTheBoundToken() throws SQLException {
    // Every partition reader opens its own connection, and each must present the certificate.
    try (Connection connection =
        DriverManager.getConnection(server.url("resultPartitions=4"), signingInAs("client"))) {
      assertEquals(
          1_000_000L * 999_999 / 2, scalar(connection, "SELECT sum(i) FROM range(1000000) t(i)"));
      try (Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("SELECT i FROM range(1000000) t(i)")) {
        long count = 0;
        while (rows.next()) {
          count++;
        }
        assertEquals(1_000_000, count);
      }
    }
  }

  @Test
  void anIdentityTheServerDoesNotKnowIsRefused() throws SQLException {
    SQLInvalidAuthorizationSpecException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url(""), signingInAs("other-client")));
    assertEquals("28000", error.getSQLState());
    assertEquals("unknown_certificate_identity", WarpProblem.codeOf(error));
    // The twin: a known identity from the same CA signs in.
    try (Connection connection =
        DriverManager.getConnection(server.url(""), signingInAs("client"))) {
      assertEquals(1, scalar(connection, "SELECT 1"));
    }
  }

  @Test
  void aBoundTokenFailsOverAnotherCertificate() throws Exception {
    String token = new Raw("client.p12").token();
    Properties other = presenting("other-client");
    other.setProperty("token", token);
    SQLInvalidAuthorizationSpecException error =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url(""), other));
    assertEquals("certificate_mismatch", WarpProblem.codeOf(error));
    // The same token over the certificate it is bound to works.
    Properties same = presenting("client");
    same.setProperty("token", token);
    try (Connection connection = DriverManager.getConnection(server.url(""), same)) {
      assertEquals(2, scalar(connection, "SELECT 2"));
    }
  }

  @Test
  void eachPartitionOfABoundTokensResultNeedsTheCertificate() throws Exception {
    Raw alice = new Raw("client.p12");
    String sessionId =
        alice.field(alice.post("/warp/v1/sessions", "{}", alice.token()), "session_id");
    HttpResponse<String> submitted =
        alice.http.send(
            alice
                .request("/warp/v1/sessions/" + sessionId + "/statements", alice.token())
                .header("Content-Type", "application/json")
                .header("Prefer", "respond-async, wait=60")
                .header("Warp-Retain-Result", "true")
                .header("Warp-Result-Partitions", "4")
                .POST(
                    HttpRequest.BodyPublishers.ofString(
                        "{\"sql\":\"SELECT i FROM range(1000000) t(i)\"}"))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    String statementId = alice.field(submitted.body(), "statement_id");
    String statement = "/warp/v1/sessions/" + sessionId + "/statements/" + statementId;
    String document = submitted.body();
    for (int i = 0; i < 12 && !document.contains("\"succeeded\""); i++) {
      document =
          alice
              .http
              .send(
                  alice.request(statement, alice.token()).header("Prefer", "wait=10").GET().build(),
                  HttpResponse.BodyHandlers.ofString())
              .body();
    }
    assertTrue(document.contains("\"index\":1"), "the result was not split: " + document);
    String partition = statement + "/result?partition=1";
    assertEquals(200, alice.get(partition, alice.token()).statusCode());
    HttpResponse<String> stolen = new Raw("other-client.p12").get(partition, alice.token());
    assertEquals(401, stolen.statusCode());
    assertTrue(stolen.body().contains("\"code\":\"certificate_mismatch\""), stolen.body());
  }

  @Test
  void aServerThatDoesNotVerifyClientCertificatesIsRefusedClearly() throws SQLException {
    try (WarpServer plain =
        WarpServer.start(
            true, "--tls-cert", cert("server.pem"), "--tls-key", cert("server-key.pem"))) {
      SQLInvalidAuthorizationSpecException error =
          assertThrows(
              SQLInvalidAuthorizationSpecException.class,
              () -> DriverManager.getConnection(plain.url(""), signingInAs("client")));
      assertTrue(error.getMessage().contains("mtls_identity"), error.getMessage());
      // The twin: the same server takes the password.
      Properties password = plain.properties();
      password.setProperty("tlsRootCerts", cert("ca.pem"));
      try (Connection connection = DriverManager.getConnection(plain.url(""), password)) {
        assertEquals(3, scalar(connection, "SELECT 3"));
      }
    }
  }

  /** HTTPS calls beside the driver, presenting a client key store's certificate. */
  private static final class Raw {
    final HttpClient http;
    private String token;

    Raw(String keyStore) throws Exception {
      char[] password = "changeit".toCharArray();
      KeyStore keys = KeyStore.getInstance("PKCS12");
      try (InputStream in = Files.newInputStream(Path.of(cert(keyStore)))) {
        keys.load(in, password);
      }
      KeyManagerFactory keyManagers =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagers.init(keys, password);
      KeyStore trusted = KeyStore.getInstance("PKCS12");
      try (InputStream in = Files.newInputStream(Path.of(cert("truststore.p12")))) {
        trusted.load(in, password);
      }
      TrustManagerFactory trustManagers =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagers.init(trusted);
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
      http = HttpClient.newBuilder().sslContext(context).build();
    }

    /** A token from the client_certificate grant over this certificate. */
    String token() throws Exception {
      if (token == null) {
        String body = post("/warp/v1/auth/token", "{\"grant_type\":\"client_certificate\"}", null);
        Matcher matcher = TOKEN.matcher(body);
        assertTrue(matcher.find(), body);
        token = matcher.group(1);
      }
      return token;
    }

    HttpRequest.Builder request(String path, String bearer) {
      HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(server.httpBase() + path));
      return bearer == null ? builder : builder.header("Authorization", "Bearer " + bearer);
    }

    String post(String path, String json, String bearer) throws Exception {
      return http.send(
              request(path, bearer)
                  .header("Content-Type", "application/json")
                  .POST(HttpRequest.BodyPublishers.ofString(json))
                  .build(),
              HttpResponse.BodyHandlers.ofString())
          .body();
    }

    HttpResponse<String> get(String path, String bearer) throws Exception {
      return http.send(request(path, bearer).GET().build(), HttpResponse.BodyHandlers.ofString());
    }

    String field(String json, String name) {
      Matcher matcher = FIELD.matcher(json);
      while (matcher.find()) {
        if (matcher.group(1).equals(name)) {
          return matcher.group(2);
        }
      }
      throw new AssertionError("no " + name + " in " + json);
    }
  }
}
