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
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
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
    List<String> foreign = unknownKeys(parsed, info);
    Map<String, String> properties = merge(parsed, info);
    ConnectionConfig config = config(parsed, properties);
    ChargewireConnection connection = ChargewireConnection.open(config, url, properties);
    for (String key : foreign) {
      connection.addWarning(
          new SQLWarning(
              "The connection property '"
                  + key
                  + "' is not one of this driver's and was ignored (ChargewireProperty lists"
                  + " them)",
              "01S00"));
    }
    return connection;
  }

  /**
   * Refuses keys the driver has no property for, so that a misspelled setting is never silently not
   * applied: every such key in the URL, and in the {@code Properties} those that resemble one of
   * the driver's or name TLS or signing in. Values are never part of the message: a misspelled
   * {@code password} key holds a password. Nor is a URL's key that follows a hidden value: it may
   * be the rest of a password that holds an unencoded {@code &} or {@code ;}.
   *
   * @return the other unknown keys of the {@code Properties}, which frameworks put there for
   *     themselves: tolerated, and reported as warnings on the connection
   * @throws SQLException (SQLSTATE 08001) for a refused key
   */
  static List<String> unknownKeys(ChargewireUrl url, Properties info) throws SQLException {
    boolean unnamed = false;
    for (String lowerCaseKey : url.options.keySet()) {
      if (ChargewireProperty.of(lowerCaseKey) != null) {
        continue;
      }
      if (!url.shown.named.contains(lowerCaseKey)) {
        unnamed = true;
        continue;
      }
      String key = url.writtenKeys.getOrDefault(lowerCaseKey, lowerCaseKey);
      ChargewireProperty meant = ChargewireProperty.resembling(key);
      throw unknown(
          key,
          "the URL",
          meant != null
              ? "did you mean '" + meant.key() + "'?"
              : "the driver's properties are " + keys(List.of(ChargewireProperty.values())));
    }
    if (unnamed) {
      throw new SQLNonTransientConnectionException(
          "Unknown connection property in the URL"
              + (url.shown.hiddenFrom == null
                  ? " (not named: it may be part of a password"
                  : " after '"
                      + url.shown.hiddenFrom
                      + "' (not named: it may be the rest of that"
                      + " value")
              + ", in which an '&' or ';' is written %26 or %3B); the driver's properties are "
              + keys(List.of(ChargewireProperty.values())),
          "08001");
    }
    List<String> foreign = new ArrayList<>();
    if (info != null) {
      Set<String> keys = new TreeSet<>(info.stringPropertyNames());
      for (Object key : info.keySet()) {
        if (key != null) {
          keys.add(key.toString());
        }
      }
      for (String key : keys) {
        if (ChargewireProperty.of(key) != null) {
          continue;
        }
        ChargewireProperty meant = ChargewireProperty.resembling(key);
        if (meant != null) {
          throw unknown(
              key,
              "the connection's Properties",
              "did you mean '"
                  + meant.key()
                  + "'? A key that resembles one of the driver's is refused rather than ignored");
        }
        if (ChargewireProperty.namesSecurity(key)) {
          throw unknown(
              key,
              "the connection's Properties",
              "it names TLS or signing in, so it is refused rather than ignored; the driver's"
                  + " properties for those are "
                  + keys(ChargewireProperty.SECURITY));
        }
        foreign.add(key);
      }
    }
    return foreign;
  }

  private static String keys(List<ChargewireProperty> properties) {
    StringBuilder keys = new StringBuilder();
    for (ChargewireProperty property : properties) {
      keys.append(keys.length() == 0 ? "" : ", ").append(property.key());
    }
    return keys.toString();
  }

  private static SQLException unknown(String key, String where, String hint) {
    return new SQLNonTransientConnectionException(
        "Unknown connection property '" + key + "' in " + where + ": " + hint, "08001");
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
  static Map<String, String> merge(ChargewireUrl url, Properties info) {
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

  static ConnectionConfig config(ChargewireUrl url, Map<String, String> properties)
      throws SQLException {
    String compression = IpcCompression.parse(ChargewireProperty.IPC_COMPRESSION.get(properties));
    if (compression == null) {
      throw invalid(ChargewireProperty.IPC_COMPRESSION, "auto, lz4_frame, zstd or none");
    }
    String retain = ChargewireProperty.RETAIN_RESULTS.get(properties);
    long partitions = number(properties, ChargewireProperty.RESULT_PARTITIONS);
    if (partitions < 1 || partitions > ChargewireClient.MAX_RESULT_PARTITIONS) {
      throw invalid(
          ChargewireProperty.RESULT_PARTITIONS,
          "a number from 1 to " + ChargewireClient.MAX_RESULT_PARTITIONS);
    }
    boolean anyOrder = resultOrderIsAny(ChargewireProperty.RESULT_ORDER.get(properties));
    boolean certificateGrant = clientCertificateGrant(properties);
    String basePath = LegacyUrls.basePath(ChargewireProperty.BASE_PATH.get(properties));
    ConnectionConfig.Builder builder = new ConnectionConfig.Builder();
    if (browserSignIn(properties)) {
      long timeout = number(properties, ChargewireProperty.OAUTH_TIMEOUT);
      if (timeout == 0) {
        throw invalid(ChargewireProperty.OAUTH_TIMEOUT, "a number of seconds above zero");
      }
      String command = ChargewireProperty.OAUTH_BROWSER.get(properties);
      builder.browserSignIn(
          command == null || command.isBlank() ? null : command.trim(),
          Duration.ofSeconds(timeout));
    }
    return builder
        .clientCertificateGrant(certificateGrant)
        .address(url.host, url.port)
        .basePath(basePath)
        .fallback(LegacyDialect.fallbackFor(basePath))
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
        .resumeTimeout(Duration.ofSeconds(number(properties, ChargewireProperty.RESUME_TIMEOUT)))
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
      throw invalid(ChargewireProperty.RESULT_ORDER, "preserve or any");
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
    throw invalid(property, "true or false");
  }

  static long number(Map<String, String> properties, ChargewireProperty property)
      throws SQLException {
    String value = property.get(properties).trim();
    try {
      long number = Long.parseLong(value);
      if (number < 0) {
        throw invalid(property, "a number of zero or more");
      }
      return number;
    } catch (NumberFormatException e) {
      throw invalid(property, "a number");
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

  /**
   * The value is not quoted: what is given for one option by mistake may be meant for another, a
   * password among them.
   */
  private static SQLException invalid(ChargewireProperty property, String expected) {
    return new SQLNonTransientConnectionException(
        "Invalid value for " + property.key() + ": expected " + expected, "08001");
  }
}
