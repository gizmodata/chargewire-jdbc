package com.gizmodata.chargewire.jdbc;

import com.gizmodata.chargewire.jdbc.client.ChargewireClient;
import com.gizmodata.chargewire.jdbc.client.ConnectionConfig;
import com.gizmodata.chargewire.jdbc.client.IpcCompression;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The Chargewire JDBC driver: {@code jdbc:chargewire://host[:port][/catalog][?key=value&...]}.
 *
 * <p>It registers itself with {@link DriverManager} when loaded, and through {@code
 * META-INF/services/java.sql.Driver}. See {@link ChargewireProperty} for the connection properties.
 */
public final class ChargewireDriver implements Driver {
  /** The driver's name, as {@link java.sql.DatabaseMetaData#getDriverName()} reports it. */
  public static final String NAME = "Chargewire JDBC Driver";

  private static final String VERSION = readVersion();

  static {
    try {
      DriverManager.registerDriver(new ChargewireDriver());
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** Creates the driver; applications normally let {@link DriverManager} do this. */
  public ChargewireDriver() {}

  private static String readVersion() {
    try (InputStream in = ChargewireDriver.class.getResourceAsStream("driver.properties")) {
      Properties properties = new Properties();
      if (in != null) {
        properties.load(in);
      }
      return properties.getProperty("version", "0.0.0");
    } catch (IOException e) {
      return "0.0.0";
    }
  }

  /**
   * Returns the driver's version.
   *
   * @return for example {@code 0.1.0}
   */
  public static String version() {
    return VERSION;
  }

  static int versionPart(int index) {
    String[] parts = VERSION.split("[.-]");
    try {
      return index < parts.length ? Integer.parseInt(parts[index]) : 0;
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    ChargewireUrl parsed = ChargewireUrl.parse(url);
    Map<String, String> properties = merge(parsed, info);
    ConnectionConfig config = config(parsed, properties);
    return ChargewireConnection.open(config, url, properties);
  }

  @Override
  public boolean acceptsURL(String url) {
    return ChargewireUrl.accepts(url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Map<String, String> properties =
        acceptsURL(url) ? merge(ChargewireUrl.parse(url), info) : merge(null, info);
    ChargewireProperty[] all = ChargewireProperty.values();
    DriverPropertyInfo[] result = new DriverPropertyInfo[all.length];
    for (int i = 0; i < all.length; i++) {
      result[i] = all[i].info(properties);
    }
    return result;
  }

  @Override
  public int getMajorVersion() {
    return versionPart(0);
  }

  @Override
  public int getMinorVersion() {
    return versionPart(1);
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("The driver does not log through java.util.logging");
  }

  /** Lower-cases keys; URL options win over {@code info}. */
  private static Map<String, String> merge(ChargewireUrl url, Properties info) {
    Map<String, String> properties = new HashMap<>();
    if (info != null) {
      for (String key : info.stringPropertyNames()) {
        properties.put(key.toLowerCase(Locale.ROOT), info.getProperty(key));
      }
      // Values put() as Boolean or Integer are not "string properties", but are meant all the same.
      for (Map.Entry<Object, Object> entry : info.entrySet()) {
        if (entry.getKey() != null
            && entry.getValue() != null
            && !(entry.getValue() instanceof String)) {
          properties.put(
              entry.getKey().toString().toLowerCase(Locale.ROOT), entry.getValue().toString());
        }
      }
    }
    if (url != null) {
      properties.putAll(url.options);
      if (url.catalog != null) {
        properties.put(ChargewireProperty.CATALOG.key().toLowerCase(Locale.ROOT), url.catalog);
      }
    }
    return properties;
  }

  private static ConnectionConfig config(ChargewireUrl url, Map<String, String> properties)
      throws SQLException {
    String compression = IpcCompression.parse(ChargewireProperty.IPC_COMPRESSION.get(properties));
    if (compression == null) {
      throw invalid(
          ChargewireProperty.IPC_COMPRESSION,
          ChargewireProperty.IPC_COMPRESSION.get(properties),
          "auto, lz4_frame, zstd or none");
    }
    String retain = ChargewireProperty.RETAIN_RESULTS.get(properties);
    long partitions = number(properties, ChargewireProperty.RESULT_PARTITIONS);
    if (partitions < 1 || partitions > ChargewireClient.MAX_RESULT_PARTITIONS) {
      throw invalid(
          ChargewireProperty.RESULT_PARTITIONS,
          Long.toString(partitions),
          "a number from 1 to " + ChargewireClient.MAX_RESULT_PARTITIONS);
    }
    boolean anyOrder = resultOrderIsAny(ChargewireProperty.RESULT_ORDER.get(properties));
    boolean certificateGrant = clientCertificateGrant(properties);
    ConnectionConfig.Builder builder = new ConnectionConfig.Builder();
    if (browserSignIn(properties)) {
      long timeout = number(properties, ChargewireProperty.OAUTH_TIMEOUT);
      if (timeout == 0) {
        throw invalid(ChargewireProperty.OAUTH_TIMEOUT, "0", "a number of seconds above zero");
      }
      String command = ChargewireProperty.OAUTH_BROWSER.get(properties);
      builder.browserSignIn(
          command == null || command.isBlank() ? null : command.trim(),
          Duration.ofSeconds(timeout));
    }
    return builder
        .clientCertificateGrant(certificateGrant)
        .address(url.host, url.port)
        .basePath(ChargewireProperty.BASE_PATH.get(properties))
        .credentials(
            ChargewireProperty.USER.get(properties),
            ChargewireProperty.PASSWORD.get(properties),
            ChargewireProperty.TOKEN.get(properties),
            ChargewireProperty.SUBJECT_TOKEN.get(properties))
        .tls(
            bool(properties, ChargewireProperty.USE_ENCRYPTION),
            bool(properties, ChargewireProperty.DISABLE_CERTIFICATE_VERIFICATION),
            bool(properties, ChargewireProperty.USE_SYSTEM_TRUST_STORE),
            path(properties, ChargewireProperty.TLS_ROOT_CERTS),
            path(properties, ChargewireProperty.TRUST_STORE),
            ChargewireProperty.TRUST_STORE_PASSWORD.get(properties),
            path(properties, ChargewireProperty.CLIENT_CERTIFICATE),
            path(properties, ChargewireProperty.CLIENT_KEY))
        .connectTimeout(milliseconds(number(properties, ChargewireProperty.CONNECT_TIMEOUT)))
        .execution(
            compression,
            retain == null ? null : bool(properties, ChargewireProperty.RETAIN_RESULTS),
            bool(properties, ChargewireProperty.ASYNC),
            (int) number(properties, ChargewireProperty.POLL_WAIT))
        .partitions((int) partitions, anyOrder)
        .build();
  }

  /**
   * Reads {@code authType}: {@code password}, {@code client_certificate} (alias {@code mtls}) or
   * {@code oauth_browser} (alias {@code external}), in any case.
   *
   * @param properties the connection's properties, keyed in lower case
   * @return {@code password}, {@code client_certificate} or {@code oauth_browser}
   * @throws SQLException for any other value
   */
  static String authType(Map<String, String> properties) throws SQLException {
    String value = ChargewireProperty.AUTH_TYPE.get(properties);
    String type = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    switch (type) {
      case "password":
        return "password";
      case "client_certificate":
      case "mtls":
        return "client_certificate";
      case "oauth_browser":
      case "external":
        return "oauth_browser";
      default:
        throw invalid(
            ChargewireProperty.AUTH_TYPE,
            value,
            "password, client_certificate (or mtls) or oauth_browser (or external)");
    }
  }

  /**
   * Checks that a browser sign-in ({@code authType=oauth_browser}, spec §4.3) has no other
   * credential.
   *
   * @param properties the connection's properties, keyed in lower case
   * @return {@code true} for a browser sign-in
   * @throws SQLException for an unknown {@code authType} or a credential beside it
   */
  static boolean browserSignIn(Map<String, String> properties) throws SQLException {
    if (!"oauth_browser".equals(authType(properties))) {
      return false;
    }
    for (ChargewireProperty credential :
        new ChargewireProperty[] {
          ChargewireProperty.USER,
          ChargewireProperty.PASSWORD,
          ChargewireProperty.TOKEN,
          ChargewireProperty.SUBJECT_TOKEN
        }) {
      if (!blank(credential.get(properties))) {
        throw new SQLInvalidAuthorizationSpecException(
            "authType=oauth_browser signs in through the browser with the server's identity"
                + " provider; remove "
                + credential.key(),
            "28000");
      }
    }
    return true;
  }

  /**
   * Reads {@code authType}, and checks that a {@code client_certificate} sign-in has a client
   * certificate over TLS and no other credential.
   *
   * @param properties the connection's properties, keyed in lower case
   * @return {@code true} for the {@code client_certificate} grant
   * @throws SQLException for an unknown value or a configuration the grant cannot work with
   */
  static boolean clientCertificateGrant(Map<String, String> properties) throws SQLException {
    String value = ChargewireProperty.AUTH_TYPE.get(properties);
    String type = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    if (!"client_certificate".equals(authType(properties))) {
      return false;
    }
    if (!bool(properties, ChargewireProperty.USE_ENCRYPTION)) {
      throw new SQLInvalidAuthorizationSpecException(
          "authType="
              + type
              + " signs in with a TLS client certificate: it needs useEncryption=true",
          "28000");
    }
    if (blank(ChargewireProperty.CLIENT_CERTIFICATE.get(properties))
        || blank(ChargewireProperty.CLIENT_KEY.get(properties))) {
      throw new SQLInvalidAuthorizationSpecException(
          "authType="
              + type
              + " signs in with a TLS client certificate: set clientCertificate and clientKey",
          "28000");
    }
    for (ChargewireProperty credential :
        new ChargewireProperty[] {
          ChargewireProperty.USER,
          ChargewireProperty.PASSWORD,
          ChargewireProperty.TOKEN,
          ChargewireProperty.SUBJECT_TOKEN
        }) {
      if (!blank(credential.get(properties))) {
        throw new SQLInvalidAuthorizationSpecException(
            "authType="
                + type
                + " signs in with the client certificate alone; remove "
                + credential.key(),
            "28000");
      }
    }
    return true;
  }

  private static boolean blank(String value) {
    return value == null || value.isEmpty();
  }

  /**
   * Reads a {@code resultOrder} value.
   *
   * @param value {@code preserve} or {@code any}, in any case
   * @return {@code true} for {@code any}
   * @throws SQLException for any other value
   */
  static boolean resultOrderIsAny(String value) throws SQLException {
    String order = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    if ("any".equals(order)) {
      return true;
    }
    if (!"preserve".equals(order)) {
      throw invalid(ChargewireProperty.RESULT_ORDER, value, "preserve or any");
    }
    return false;
  }

  static boolean bool(Map<String, String> properties, ChargewireProperty property)
      throws SQLException {
    String value = property.get(properties).trim();
    if ("true".equalsIgnoreCase(value) || "1".equals(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value) || "0".equals(value)) {
      return false;
    }
    throw invalid(property, value, "true or false");
  }

  static long number(Map<String, String> properties, ChargewireProperty property)
      throws SQLException {
    String value = property.get(properties).trim();
    try {
      long number = Long.parseLong(value);
      if (number < 0) {
        throw invalid(property, value, "a number of zero or more");
      }
      return number;
    } catch (NumberFormatException e) {
      throw invalid(property, value, "a number");
    }
  }

  /** A timeout in milliseconds; 0 means none. */
  private static Duration milliseconds(long value) {
    return value == 0 ? null : Duration.ofMillis(value);
  }

  private static Path path(Map<String, String> properties, ChargewireProperty property) {
    String value = property.get(properties);
    return value == null || value.isEmpty() ? null : Path.of(value);
  }

  private static SQLException invalid(ChargewireProperty property, String value, String expected) {
    return new SQLNonTransientConnectionException(
        "Invalid value '" + value + "' for " + property.key() + ": expected " + expected, "08001");
  }
}
