package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Browser sign-in ({@code authType=oauth_browser}, spec §4.3) against a server whose identity
 * provider is {@link MockIdentityProvider}. The "browser" is {@code curl}, which follows the
 * provider's redirect back to the server's callback, as a browser does once the person has signed
 * in.
 *
 * <p>The server's flags for its identity provider are a template, {@code
 * CHARGEWIRE_JDBC_TEST_SERVER_OAUTH_ARGS} (default: chargewire-duckdb's), with the placeholders
 * {@code {issuer}}, {@code {client_id}}, {@code {identity}} and {@code {base_url}}; the client
 * secret goes in the variable {@code CHARGEWIRE_JDBC_TEST_SERVER_OAUTH_SECRET_ENV} names (default
 * {@code CHARGEWIRE_DUCKDB_OAUTH_CLIENT_SECRET}).
 */
class BrowserSignInIT {
  private static final String IDENTITY = "alice@example.com";
  private static final String DEFAULT_ARGS =
      "--oauth-issuer {issuer} --oauth-client-id {client_id} --oauth-identities {identity}"
          + " --oauth-base-url {base_url} --oauth-provider-name Mock";

  private static MockIdentityProvider idp;
  private static ChargewireServer server;
  private static Path browser;

  @BeforeAll
  static void start() throws Exception {
    idp = new MockIdentityProvider();
    String template = System.getenv("CHARGEWIRE_JDBC_TEST_SERVER_OAUTH_ARGS");
    if (template == null || template.isBlank()) {
      template = DEFAULT_ARGS;
    }
    String secretVariable = System.getenv("CHARGEWIRE_JDBC_TEST_SERVER_OAUTH_SECRET_ENV");
    if (secretVariable == null || secretVariable.isBlank()) {
      secretVariable = "CHARGEWIRE_DUCKDB_OAUTH_CLIENT_SECRET";
    }
    List<String> args = new ArrayList<>();
    for (String arg : template.trim().split("\\s+")) {
      args.add(
          arg.replace("{issuer}", idp.issuer())
              .replace("{client_id}", idp.clientId)
              .replace("{identity}", IDENTITY)
              .replace("{base_url}", "http://127.0.0.1:{port}"));
    }
    server =
        ChargewireServer.start(
            Map.of(secretVariable, idp.clientSecret), false, args.toArray(new String[0]));
    browser = Files.createTempFile("chargewire-jdbc-browser", ".sh");
    Files.writeString(browser, "#!/bin/sh\nexec curl -s -L -o /dev/null \"$1\"\n");
    Files.setPosixFilePermissions(browser, PosixFilePermissions.fromString("rwx------"));
  }

  @AfterAll
  static void stop() throws Exception {
    if (server != null) {
      server.close();
    }
    if (idp != null) {
      idp.close();
    }
    if (browser != null) {
      Files.deleteIfExists(browser);
    }
  }

  @AfterEach
  void reset() {
    idp.email = IDENTITY;
    idp.deny = false;
  }

  private static Properties browserSignIn(String authType) {
    Properties properties = new Properties();
    properties.setProperty("useEncryption", "false");
    properties.setProperty("authType", authType);
    properties.setProperty("oauthBrowser", browser.toString());
    properties.setProperty("oauthTimeout", "30");
    return properties;
  }

  @Test
  void signsInThroughTheBrowserAndRunsSql() throws SQLException {
    for (String authType : List.of("oauth_browser", "external")) {
      try (Connection connection =
              DriverManager.getConnection(server.url(""), browserSignIn(authType));
          Statement statement = connection.createStatement();
          ResultSet rows = statement.executeQuery("SELECT 41 + 1 AS answer")) {
        assertTrue(rows.next());
        assertEquals(42, rows.getInt(1));
      }
    }
  }

  @Test
  void aRefusalAtTheProviderIsAnAuthorizationError() {
    idp.deny = true;
    SQLException e =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url(""), browserSignIn("oauth_browser")));
    assertEquals("28000", e.getSQLState());
    assertTrue(e.getMessage().contains("access_denied"), e.getMessage());
  }

  @Test
  void aUserTheServerDoesNotKnowIsRefused() {
    idp.email = "mallory@example.org";
    SQLException e =
        assertThrows(
            SQLInvalidAuthorizationSpecException.class,
            () -> DriverManager.getConnection(server.url(""), browserSignIn("oauth_browser")));
    assertTrue(e.getMessage().contains("invalid_credentials"), e.getMessage());
  }

  @Test
  void aServerWithoutBrowserSignInSaysSo() {
    try (ChargewireServer plain = ChargewireServer.start()) {
      SQLException e =
          assertThrows(
              SQLInvalidAuthorizationSpecException.class,
              () -> DriverManager.getConnection(plain.url(""), browserSignIn("oauth_browser")));
      assertTrue(e.getMessage().contains("no capability oauth_browser"), e.getMessage());
    }
  }
}
