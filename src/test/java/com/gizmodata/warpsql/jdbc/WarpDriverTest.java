package com.gizmodata.warpsql.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The driver's registration and URL handling (ported from {@code ArrowFlightJdbcDriverTest}). */
class WarpDriverTest {
  @Test
  void isRegisteredWithDriverManager() throws SQLException {
    assertInstanceOf(WarpDriver.class, DriverManager.getDriver("jdbc:warp://localhost:31337"));
  }

  @Test
  void isFoundByServiceLoader() {
    assertTrue(
        ServiceLoader.load(Driver.class).stream()
            .anyMatch(provider -> provider.type() == WarpDriver.class));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:postgresql://localhost:5432",
        "jdbc:arrow-flight-sql://localhost:31337",
        "warp://localhost:1",
        "jdbc:warpsql://localhost:1"
      })
  void declinesOtherUrls(String url) throws SQLException {
    WarpDriver driver = new WarpDriver();
    assertFalse(driver.acceptsURL(url));
    assertNull(driver.connect(url, new Properties()), "JDBC: return null for another driver's URL");
  }

  @Test
  void acceptsTheSchemeInAnyCase() {
    assertTrue(new WarpDriver().acceptsURL("JDBC:WARP://localhost:1"));
  }

  @Test
  void parsesHostPortCatalogAndOptions() throws SQLException {
    WarpUrl url =
        WarpUrl.parse("jdbc:warp://db.example.com:31337/lake?user=joe&useEncryption=false");
    assertEquals("db.example.com", url.host);
    assertEquals(31337, url.port);
    assertEquals("lake", url.catalog);
    assertEquals(Map.of("user", "joe", "useencryption", "false"), url.options);
  }

  @Test
  void parsesSemicolonOptions() throws SQLException {
    WarpUrl url = WarpUrl.parse("jdbc:warp://localhost:1234;user=joe;password=secret");
    assertEquals("localhost", url.host);
    assertEquals(1234, url.port);
    assertEquals(Map.of("user", "joe", "password", "secret"), url.options);
  }

  @Test
  void parsesOneSemicolonOption() throws SQLException {
    assertEquals(
        Map.of("user", "joe"), WarpUrl.parse("jdbc:warp://localhost:1234;user=joe").options);
  }

  @Test
  void parsesSemicolonsInsideTheQuery() throws SQLException {
    assertEquals(
        Map.of("a", "1", "b", "2"), WarpUrl.parse("jdbc:warp://localhost:1234?a=1;b=2").options);
  }

  @Test
  void parsesIpAddresses() throws SQLException {
    assertEquals("127.0.0.1", WarpUrl.parse("jdbc:warp://127.0.0.1:1234").host);
    WarpUrl ipv6 = WarpUrl.parse("jdbc:warp://[::1]:1234");
    assertEquals("::1", ipv6.host);
    assertEquals(1234, ipv6.port);
  }

  @Test
  void decodesPercentEncodedValuesAndKeepsPlus() throws SQLException {
    WarpUrl url =
        WarpUrl.parse("jdbc:warp://localhost:1234?password=p%40ss%3Aw%26rd%23%3B&token=a+b");
    assertEquals("p@ss:w&rd#;", url.options.get("password"));
    assertEquals("a+b", url.options.get("token"));
  }

  @Test
  void portIsOptional() throws SQLException {
    assertEquals(-1, WarpUrl.parse("jdbc:warp://localhost").port);
    assertNull(WarpUrl.parse("jdbc:warp://localhost/").catalog);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:warp:",
        "jdbc:warp:localhost:1",
        "jdbc:warp://",
        "jdbc:warp://:1234",
        "jdbc:warp://localhost:1/a/b",
        "jdbc:warp://localhost:1?novalue",
        "jdbc:warp://localhost:x",
        "jdbc:warp://localhost:1#fragment"
      })
  void rejectsMalformedUrls(String url) {
    SQLException error =
        assertThrows(SQLNonTransientConnectionException.class, () -> WarpUrl.parse(url));
    assertEquals("08001", error.getSQLState());
  }

  @Test
  void redactsSecrets() {
    assertEquals(
        "jdbc:warp://h?user=joe&password=***;token=***&trustStorePassword=***",
        WarpUrl.redact("jdbc:warp://h?user=joe&password=s3cr3t;token=abc&trustStorePassword=x"));
  }

  @Test
  void propertyInfoListsEveryPropertyWithUrlValues() throws SQLException {
    Properties info = new Properties();
    info.setProperty("USER", "from-properties");
    DriverPropertyInfo[] properties =
        new WarpDriver().getPropertyInfo("jdbc:warp://h:1/lake?useEncryption=false", info);
    assertEquals(WarpProperty.values().length, properties.length);
    Map<String, String> values = new java.util.HashMap<>();
    for (DriverPropertyInfo property : properties) {
      values.put(property.name, property.value);
    }
    assertEquals("from-properties", values.get("user"), "keys are case-insensitive");
    assertEquals("false", values.get("useEncryption"));
    assertEquals("lake", values.get("catalog"), "the URL path is the catalog");
    assertEquals("/warp", values.get("basePath"));
    DriverPropertyInfo compression =
        Arrays.stream(properties).filter(p -> p.name.equals("ipcCompression")).findFirst().get();
    assertEquals(Arrays.asList("none", "zstd"), Arrays.asList(compression.choices));
  }

  @Test
  void propertiesPutAsOtherTypesCount() throws SQLException {
    Properties info = new Properties();
    info.put("useEncryption", false);
    info.put("connectTimeout", 5000);
    Properties defaults = new Properties();
    defaults.setProperty("user", "from-defaults");
    Properties withDefaults = new Properties(defaults);
    withDefaults.putAll(info);
    Map<String, String> values = new java.util.HashMap<>();
    for (DriverPropertyInfo property :
        new WarpDriver().getPropertyInfo("jdbc:warp://h:1", withDefaults)) {
      values.put(property.name, property.value);
    }
    assertEquals("false", values.get("useEncryption"));
    assertEquals("5000", values.get("connectTimeout"));
    assertEquals("from-defaults", values.get("user"), "defaults count too");
  }

  @Test
  void connectingWithNullPropertiesReachesTheServer() {
    // Nothing listens on port 1: the driver gets as far as connecting, then reports why it can't.
    SQLException error =
        assertThrows(
            SQLException.class,
            () -> new WarpDriver().connect("jdbc:warp://127.0.0.1:1?useEncryption=false", null));
    assertEquals("08001", error.getSQLState());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "useEncryption=maybe",
        "connectTimeout=soon",
        "connectTimeout=-1",
        "ipcCompression=gzip",
        "retainResults=perhaps"
      })
  void rejectsInvalidPropertyValues(String option) {
    SQLException error =
        assertThrows(
            SQLException.class,
            () -> new WarpDriver().connect("jdbc:warp://127.0.0.1:1?" + option, null));
    assertEquals("08001", error.getSQLState());
    assertTrue(error.getMessage().startsWith("Invalid value"), error.getMessage());
  }

  @Test
  void reportsItsVersion() {
    WarpDriver driver = new WarpDriver();
    assertTrue(WarpDriver.version().matches("\\d+\\.\\d+\\.\\d+.*"), WarpDriver.version());
    assertEquals(WarpDriver.versionPart(0), driver.getMajorVersion());
    assertEquals(WarpDriver.versionPart(1), driver.getMinorVersion());
    assertFalse(driver.jdbcCompliant());
  }
}
