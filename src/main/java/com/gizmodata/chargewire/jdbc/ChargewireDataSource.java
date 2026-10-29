package com.gizmodata.chargewire.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * A {@link DataSource} for the Chargewire driver: a URL, credentials, and any other {@link
 * ChargewireProperty} as {@link #setProperties(Properties) properties}.
 */
public final class ChargewireDataSource implements DataSource {
  private final ChargewireDriver driver = new ChargewireDriver();
  private String url;
  private String user;
  private String password;
  private Properties properties = new Properties();
  private int loginTimeout;
  private PrintWriter logWriter;

  /** Creates an unconfigured data source; set at least its URL. */
  public ChargewireDataSource() {}

  /**
   * Sets the JDBC URL.
   *
   * @param url a {@code jdbc:chargewire://} URL
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
   * Sets other connection properties ({@link ChargewireProperty} keys).
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
      all.setProperty(ChargewireProperty.USER.key(), username);
    }
    if (password != null) {
      all.setProperty(ChargewireProperty.PASSWORD.key(), password);
    }
    if (loginTimeout > 0 && !all.containsKey(ChargewireProperty.CONNECT_TIMEOUT.key())) {
      all.setProperty(
          ChargewireProperty.CONNECT_TIMEOUT.key(), Long.toString(loginTimeout * 1000L));
    }
    Connection connection = driver.connect(url, all);
    if (connection == null) {
      throw new SQLException("Not a Chargewire URL: " + ChargewireUrl.redact(url), "08001");
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
