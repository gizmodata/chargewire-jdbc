package com.gizmodata.warpsql.jdbc.it;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/** One plain-HTTP server for every integration test in the run, started on first use. */
final class SharedServer {
  private static WarpServer server;

  private SharedServer() {}

  static synchronized WarpServer get() {
    if (server == null) {
      server = WarpServer.start();
    }
    return server;
  }

  static Connection connect() throws SQLException {
    return connect("");
  }

  /** Connects with extra URL options, for example {@code async=true}. */
  static Connection connect(String options) throws SQLException {
    WarpServer warp = get();
    return DriverManager.getConnection(warp.url(options), warp.properties());
  }

  static Connection connect(Properties extra) throws SQLException {
    WarpServer warp = get();
    Properties properties = warp.properties();
    properties.putAll(extra);
    return DriverManager.getConnection(warp.url(""), properties);
  }
}
