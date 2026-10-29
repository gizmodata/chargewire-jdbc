package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** URLs and base paths from before the protocol's rename work as today's, silently. */
class LegacyUrlsTest {
  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:warp://localhost:1",
        "jdbc:warpsql://localhost:1",
        "JDBC:WARP://localhost:1",
        "Jdbc:WarpSql://localhost:1"
      })
  void theFormerPrefixesAreAccepted(String url) throws SQLException {
    assertTrue(new ChargewireDriver().acceptsURL(url));
    assertInstanceOf(ChargewireDriver.class, DriverManager.getDriver(url));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"warp://localhost:1", "jdbc:warp-sql://localhost:1", "jdbc:warpx://localhost:1"})
  void otherSpellingsAreStillDeclined(String url) throws SQLException {
    ChargewireDriver driver = new ChargewireDriver();
    assertFalse(driver.acceptsURL(url));
    assertNull(driver.connect(url, new Properties()));
  }

  @Test
  void aFormerUrlParsesAsTodaysDoes() throws SQLException {
    String options = "//db.example.com:31337/lake?user=joe&useEncryption=false;networkTimeout=5";
    ChargewireUrl current = ChargewireUrl.parse("jdbc:chargewire:" + options);
    for (String prefix : new String[] {"jdbc:warp:", "jdbc:warpsql:", "JDBC:WARPSQL:"}) {
      ChargewireUrl former = ChargewireUrl.parse(prefix + options);
      assertEquals(current.host, former.host);
      assertEquals(current.port, former.port);
      assertEquals(current.catalog, former.catalog);
      assertEquals(current.options, former.options);
    }
    assertEquals(
        Map.of("user", "joe", "useencryption", "false", "networktimeout", "5"), current.options);
  }

  @Test
  void anInvalidFormerUrlIsQuotedAsWritten() {
    SQLException refused =
        assertThrows(
            SQLNonTransientConnectionException.class, () -> ChargewireUrl.parse("jdbc:warp:x"));
    assertTrue(refused.getMessage().contains("'jdbc:warp:x'"), refused.getMessage());
  }

  @Test
  void theFormerRootBecomesTodays() {
    assertEquals("/chargewire", LegacyUrls.basePath("/warp"));
    assertEquals("/chargewire", LegacyUrls.basePath("/warp/"));
    assertEquals("/chargewire", LegacyUrls.basePath("warp"));
    assertEquals("/db/warp", LegacyUrls.basePath("/db/warp"), "a proxy's own path is kept");
    assertEquals("/warpdrive", LegacyUrls.basePath("/warpdrive"));
    assertEquals("/chargewire", LegacyUrls.basePath("/chargewire"));
    assertNull(LegacyUrls.basePath(null));
  }

  @Test
  void aFormerUrlConnectsToTodaysRootWithoutSayingSo() throws Exception {
    PrintStream out = System.out;
    PrintStream err = System.err;
    ByteArrayOutputStream printed = new ByteArrayOutputStream();
    try (FakeChargewireServer server = new FakeChargewireServer()) {
      String url =
          "jdbc:warp://127.0.0.1:"
              + server.port()
              + "?useEncryption=false&user=joe&password=pw&basePath=/warp";
      PrintStream capture = new PrintStream(printed, true, StandardCharsets.UTF_8);
      System.setOut(capture);
      System.setErr(capture);
      try (Connection connection = DriverManager.getConnection(url)) {
        assertTrue(connection.isValid(1));
        assertEquals(url.replace("pw", "***"), connection.getMetaData().getURL());
      } finally {
        System.setOut(out);
        System.setErr(err);
      }
      assertEquals(1, server.requests("POST", "/v1/sessions").size());
    }
    assertEquals("", printed.toString(StandardCharsets.UTF_8), "the mapping is silent");
  }
}
