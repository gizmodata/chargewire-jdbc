package com.gizmodata.warpsql.jdbc.it;

import static com.gizmodata.warpsql.jdbc.it.TlsIT.cert;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Mutual TLS: the server requires a client certificate (ported from {@code
 * ConnectionMutualTlsTest}).
 */
class MutualTlsIT {
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
            "--tls-client-ca",
            cert("ca.pem"));
  }

  @AfterAll
  static void stop() {
    server.close();
  }

  private static Properties trusting() {
    Properties properties = server.properties();
    properties.setProperty("tlsRootCerts", cert("ca.pem"));
    return properties;
  }

  @ParameterizedTest
  @ValueSource(strings = {"client-key.pem", "client-key-pkcs1.pem"})
  void connectsWithAClientCertificate(String key) throws SQLException {
    Properties properties = trusting();
    properties.setProperty("clientCertificate", cert("client.pem"));
    properties.setProperty("clientKey", cert(key));
    try (Connection connection = DriverManager.getConnection(server.url(""), properties);
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 1")) {
      assertTrue(rows.next());
    }
  }

  @Test
  void withoutAClientCertificateTheServerRefuses() {
    SQLException error =
        assertThrows(
            SQLException.class, () -> DriverManager.getConnection(server.url(""), trusting()));
    assertTrue(
        error.getSQLState().startsWith("08"), error.getSQLState() + " " + error.getMessage());
  }

  @Test
  void aCertificateWithoutItsKeyIsAConfigurationError() {
    Properties properties = trusting();
    properties.setProperty("clientCertificate", cert("client.pem"));
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertTrue(
        error.getMessage().contains("both clientCertificate and clientKey"), error.getMessage());
  }

  @Test
  void aMissingKeyFileIsReported() {
    Properties properties = trusting();
    properties.setProperty("clientCertificate", cert("client.pem"));
    properties.setProperty("clientKey", cert("no-such-key.pem"));
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("08001", error.getSQLState());
    assertTrue(error.getMessage().contains("no-such-key.pem"), error.getMessage());
  }

  @Test
  void aMissingCertificateFileIsReported() {
    Properties properties = trusting();
    properties.setProperty("clientCertificate", cert("no-such-cert.pem"));
    properties.setProperty("clientKey", cert("client-key.pem"));
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertTrue(error.getMessage().contains("no-such-cert.pem"), error.getMessage());
  }
}
