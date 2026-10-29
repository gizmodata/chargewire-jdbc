package com.gizmodata.chargewire.jdbc.it;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Connection URLs from before the protocol's rename run queries on a real server. */
class LegacyUrlsIT {
  @ParameterizedTest
  @ValueSource(strings = {"jdbc:warp:", "jdbc:warpsql:"})
  void aFormerUrlRunsAQuery(String prefix) throws SQLException {
    ChargewireServer server = SharedServer.get();
    String url = prefix + "//127.0.0.1:" + server.port;
    try (Connection connection = DriverManager.getConnection(url, server.properties());
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 40 + 2 AS answer, 'ok' AS word")) {
      assertTrue(rows.next());
      assertEquals(42, rows.getInt("answer"));
      assertEquals("ok", rows.getString("word"));
      assertFalse(rows.next());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"basePath=/warp", "basePath=/chargewire"})
  void theFormerRootReachesTheServer(String option) throws SQLException {
    ChargewireServer server = SharedServer.get();
    String url = "jdbc:warp://127.0.0.1:" + server.port + "?" + option;
    try (Connection connection = DriverManager.getConnection(url, server.properties());
        Statement statement = connection.createStatement();
        ResultSet rows = statement.executeQuery("SELECT 1")) {
      assertTrue(rows.next());
      assertEquals(1, rows.getInt(1));
    }
  }
}
