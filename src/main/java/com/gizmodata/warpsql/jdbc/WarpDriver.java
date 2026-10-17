package com.gizmodata.warpsql.jdbc;

import com.gizmodata.warpsql.jdbc.client.ConnectionConfig;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLNonTransientConnectionException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * The Warp SQL JDBC driver: {@code jdbc:warp://host[:port][/catalog][?key=value&...]}.
 *
 * <p>It registers itself with {@link DriverManager} when loaded, and through {@code
 * META-INF/services/java.sql.Driver}. See {@link WarpProperty} for the connection properties.
 */
public final class WarpDriver implements Driver {
  /** The driver's name, as {@link java.sql.DatabaseMetaData#getDriverName()} reports it. */
  public static final String NAME = "Warp SQL JDBC Driver";

  private static final String VERSION = readVersion();

  static {
    try {
      DriverManager.registerDriver(new WarpDriver());
    } catch (SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  /** Creates the driver; applications normally let {@link DriverManager} do this. */
  public WarpDriver() {}

  private static String readVersion() {
    try (InputStream in = WarpDriver.class.getResourceAsStream("driver.properties")) {
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
    WarpUrl parsed = WarpUrl.parse(url);
    Map<String, String> properties = merge(parsed, info);
    ConnectionConfig config = config(parsed, properties);
    return WarpConnection.open(config, url, properties);
  }

  @Override
  public boolean acceptsURL(String url) {
    return WarpUrl.accepts(url);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    Map<String, String> properties =
        acceptsURL(url) ? merge(WarpUrl.parse(url), info) : merge(null, info);
    WarpProperty[] all = WarpProperty.values();
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
  private static Map<String, String> merge(WarpUrl url, Properties info) {
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
        properties.put(WarpProperty.CATALOG.key().toLowerCase(Locale.ROOT), url.catalog);
      }
    }
    return properties;
  }

  private static ConnectionConfig config(WarpUrl url, Map<String, String> properties)
      throws SQLException {
    String compression = WarpProperty.IPC_COMPRESSION.get(properties).toLowerCase(Locale.ROOT);
    if (!"none".equals(compression) && !"zstd".equals(compression)) {
      throw invalid(WarpProperty.IPC_COMPRESSION, compression, "none or zstd");
    }
    String retain = WarpProperty.RETAIN_RESULTS.get(properties);
    return new ConnectionConfig.Builder()
        .address(url.host, url.port)
        .basePath(WarpProperty.BASE_PATH.get(properties))
        .credentials(
            WarpProperty.USER.get(properties),
            WarpProperty.PASSWORD.get(properties),
            WarpProperty.TOKEN.get(properties),
            WarpProperty.SUBJECT_TOKEN.get(properties))
        .tls(
            bool(properties, WarpProperty.USE_ENCRYPTION),
            bool(properties, WarpProperty.DISABLE_CERTIFICATE_VERIFICATION),
            bool(properties, WarpProperty.USE_SYSTEM_TRUST_STORE),
            path(properties, WarpProperty.TLS_ROOT_CERTS),
            path(properties, WarpProperty.TRUST_STORE),
            WarpProperty.TRUST_STORE_PASSWORD.get(properties),
            path(properties, WarpProperty.CLIENT_CERTIFICATE),
            path(properties, WarpProperty.CLIENT_KEY))
        .connectTimeout(milliseconds(number(properties, WarpProperty.CONNECT_TIMEOUT)))
        .execution(
            "zstd".equals(compression),
            retain == null ? null : bool(properties, WarpProperty.RETAIN_RESULTS),
            bool(properties, WarpProperty.ASYNC),
            (int) number(properties, WarpProperty.POLL_WAIT))
        .build();
  }

  static boolean bool(Map<String, String> properties, WarpProperty property) throws SQLException {
    String value = property.get(properties).trim();
    if ("true".equalsIgnoreCase(value) || "1".equals(value)) {
      return true;
    }
    if ("false".equalsIgnoreCase(value) || "0".equals(value)) {
      return false;
    }
    throw invalid(property, value, "true or false");
  }

  static long number(Map<String, String> properties, WarpProperty property) throws SQLException {
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

  private static Path path(Map<String, String> properties, WarpProperty property) {
    String value = property.get(properties);
    return value == null || value.isEmpty() ? null : Path.of(value);
  }

  private static SQLException invalid(WarpProperty property, String value, String expected) {
    return new SQLNonTransientConnectionException(
        "Invalid value '" + value + "' for " + property.key() + ": expected " + expected, "08001");
  }
}
