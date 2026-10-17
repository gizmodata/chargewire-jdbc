package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
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

/**
 * TLS: the trust sources, host name verification and its explicit bypass (ported from {@code
 * ConnectionTlsTest} and {@code ConnectionTlsRootCertsTest}).
 */
class TlsIT {
  private static WarpServer server;
  private static WarpServer wrongHost;

  static String cert(String name) {
    return Path.of("src", "test", "resources", "certs", name).toAbsolutePath().toString();
  }

  @BeforeAll
  static void start() {
    server = WarpServer.start(true, "--tls-cert", cert("server.pem"), "--tls-key", cert("server-key.pem"));
    wrongHost =
        WarpServer.start(true, "--tls-cert", cert("wrong-host.pem"), "--tls-key", cert("wrong-host-key.pem"));
  }

  @AfterAll
  static void stop() {
    server.close();
    wrongHost.close();
  }

  private static void assertQueries(Connection connection) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 'over TLS' AS greeting")) {
      assertTrue(rows.next());
      assertEquals("over TLS", rows.getString(1));
    }
  }

  @Test
  void trustsAPemRootCertificate() throws SQLException {
    Properties properties = server.properties();
    properties.setProperty("tlsRootCerts", cert("ca.pem"));
    try (Connection connection = DriverManager.getConnection(server.url(""), properties)) {
      assertQueries(connection);
    }
    try (Connection connection =
        DriverManager.getConnection("jdbc:warp://localhost:" + server.port, properties)) {
      assertQueries(connection);
    }
  }

  @Test
  void trustsATrustStore() throws SQLException {
    Properties properties = server.properties();
    properties.setProperty("trustStore", cert("truststore.p12"));
    properties.setProperty("trustStorePassword", "changeit");
    try (Connection connection = DriverManager.getConnection(server.url(""), properties)) {
      assertQueries(connection);
    }
  }

  @Test
  void aTrustStoreWithTheWrongPasswordFails() {
    Properties properties = server.properties();
    properties.setProperty("trustStore", cert("truststore.p12"));
    properties.setProperty("trustStorePassword", "wrong");
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("08001", error.getSQLState());
    assertTrue(error.getMessage().startsWith("Cannot set up TLS"), error.getMessage());
  }

  @Test
  void anUntrustedCertificateFails() {
    Properties properties = server.properties();
    properties.setProperty("tlsRootCerts", cert("other-ca.pem"));
    properties.setProperty("useSystemTrustStore", "false");
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertEquals("08001", error.getSQLState());
    assertTrue(error.getMessage().contains("TLS"), error.getMessage());
  }

  @Test
  void theSystemTrustStoreDoesNotKnowTheTestCa() {
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(server.url(""), server.properties()));
    assertEquals("08001", error.getSQLState());
  }

  @Test
  void noTrustSourceAtAllIsAConfigurationError() {
    Properties properties = server.properties();
    properties.setProperty("useSystemTrustStore", "false");
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(server.url(""), properties));
    assertTrue(error.getMessage().contains("no trusted certificates"), error.getMessage());
  }

  @Test
  void theHostNameIsVerified() {
    Properties properties = wrongHost.properties();
    properties.setProperty("tlsRootCerts", cert("ca.pem"));
    SQLNonTransientConnectionException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> DriverManager.getConnection(wrongHost.url(""), properties));
    assertEquals("08001", error.getSQLState());
  }

  @Test
  void disablingVerificationSkipsTrustAndHostName() throws SQLException {
    Properties properties = wrongHost.properties();
    properties.setProperty("disableCertificateVerification", "true");
    try (Connection connection = DriverManager.getConnection(wrongHost.url(""), properties)) {
      assertQueries(connection);
    }
  }

  @Test
  void plainHttpToATlsServerFails() {
    Properties properties = server.properties();
    properties.setProperty("useEncryption", "false");
    SQLException error =
        assertThrows(SQLException.class, () -> DriverManager.getConnection(server.url(""), properties));
    assertTrue(error.getSQLState().startsWith("08"), error.getSQLState() + " " + error.getMessage());
  }
}
