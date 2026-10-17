package com.gizmodata.warpsql.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A {@link DataSource} for the Warp SQL driver: a URL, credentials, and any other {@link
 * WarpProperty} as {@link #setProperties(Properties) properties}.
 */
public final class WarpDataSource implements DataSource {
  private final WarpDriver driver = new WarpDriver();
  private String url;
  private String user;
  private String password;
  private Properties properties = new Properties();
  private int loginTimeout;
  private PrintWriter logWriter;

  /** Creates an unconfigured data source; set at least its URL. */
  public WarpDataSource() {}

  /**
   * Sets the JDBC URL.
   *
   * @param url a {@code jdbc:warp://} URL
   */
  public void setUrl(String url) {
    this.url = url;
  }

  /**
   * Returns the JDBC URL.
   *
   * @return the URL
   */
  public String getUrl() {
    return url;
  }

  /**
   * Sets the user name for {@link #getConnection()}.
   *
   * @param user the user name
   */
  public void setUser(String user) {
    this.user = user;
  }

  /**
   * Returns the user name.
   *
   * @return the user name
   */
  public String getUser() {
    return user;
  }

  /**
   * Sets the password for {@link #getConnection()}.
   *
   * @param password the password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Sets other connection properties ({@link WarpProperty} keys).
   *
   * @param properties the properties; they are copied
   */
  public void setProperties(Properties properties) {
    this.properties = new Properties();
    if (properties != null) {
      this.properties.putAll(properties);
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return getConnection(user, password);
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    if (url == null) {
      throw new SQLException("The data source has no URL", "08001");
    }
    Properties all = new Properties();
    all.putAll(properties);
    if (username != null) {
      all.setProperty(WarpProperty.USER.key(), username);
    }
    if (password != null) {
      all.setProperty(WarpProperty.PASSWORD.key(), password);
    }
    if (loginTimeout > 0 && !all.containsKey(WarpProperty.CONNECT_TIMEOUT.key())) {
      all.setProperty(WarpProperty.CONNECT_TIMEOUT.key(), Long.toString(loginTimeout * 1000L));
    }
    Connection connection = driver.connect(url, all);
    if (connection == null) {
      throw new SQLException("Not a Warp SQL URL: " + WarpUrl.redact(url), "08001");
    }
    return connection;
  }

  @Override
  public PrintWriter getLogWriter() {
    return logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) {
    this.logWriter = out;
  }

  /** {@inheritDoc} Used as the connect timeout unless {@code connectTimeout} is set. */
  @Override
  public void setLoginTimeout(int seconds) {
    this.loginTimeout = seconds;
  }

  @Override
  public int getLoginTimeout() {
    return loginTimeout;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("The driver does not log through java.util.logging");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("Not a wrapper for " + iface.getName(), "HY000");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
