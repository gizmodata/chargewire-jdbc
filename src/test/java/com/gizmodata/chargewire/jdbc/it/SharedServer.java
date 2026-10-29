package com.gizmodata.chargewire.jdbc.it;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** One plain-HTTP server for every integration test in the run, started on first use. */
final class SharedServer {
  private static ChargewireServer server;

  private SharedServer() {}

  static synchronized ChargewireServer get() {
    if (server == null) {
      server = ChargewireServer.start();
    }
    return server;
  }

  static Connection connect() throws SQLException {
    return connect("");
  }

  /** Connects with extra URL options, for example {@code async=true}. */
  static Connection connect(String options) throws SQLException {
    ChargewireServer chargewire = get();
    return DriverManager.getConnection(chargewire.url(options), chargewire.properties());
  }

  static Connection connect(Properties extra) throws SQLException {
    ChargewireServer chargewire = get();
    Properties properties = chargewire.properties();
    properties.putAll(extra);
    return DriverManager.getConnection(chargewire.url(""), properties);
  }
}
