package com.gizmodata.chargewire.jdbc;

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
class ChargewireDriverTest {
  @Test
  void isRegisteredWithDriverManager() throws SQLException {
    assertInstanceOf(
        ChargewireDriver.class, DriverManager.getDriver("jdbc:chargewire://localhost:31337"));
  }

  @Test
  void isFoundByServiceLoader() {
    assertTrue(
        ServiceLoader.load(Driver.class).stream()
            .anyMatch(provider -> provider.type() == ChargewireDriver.class));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:postgresql://localhost:5432",
        "jdbc:arrow-flight-sql://localhost:31337",
        "chargewire://localhost:1",
        "jdbc:chargewiresql://localhost:1"
      })
  void declinesOtherUrls(String url) throws SQLException {
    ChargewireDriver driver = new ChargewireDriver();
    assertFalse(driver.acceptsURL(url));
    assertNull(driver.connect(url, new Properties()), "JDBC: return null for another driver's URL");
  }

  @Test
  void acceptsTheSchemeInAnyCase() {
    assertTrue(new ChargewireDriver().acceptsURL("JDBC:CHARGEWIRE://localhost:1"));
  }

  @Test
  void parsesHostPortCatalogAndOptions() throws SQLException {
    ChargewireUrl url =
        ChargewireUrl.parse(
            "jdbc:chargewire://db.example.com:31337/lake?user=joe&useEncryption=false");
    assertEquals("db.example.com", url.host);
    assertEquals(31337, url.port);
    assertEquals("lake", url.catalog);
    assertEquals(Map.of("user", "joe", "useencryption", "false"), url.options);
  }

  @Test
  void parsesSemicolonOptions() throws SQLException {
    ChargewireUrl url =
        ChargewireUrl.parse("jdbc:chargewire://localhost:1234;user=joe;password=secret");
    assertEquals("localhost", url.host);
    assertEquals(1234, url.port);
    assertEquals(Map.of("user", "joe", "password", "secret"), url.options);
  }

  @Test
  void parsesOneSemicolonOption() throws SQLException {
    assertEquals(
        Map.of("user", "joe"),
        ChargewireUrl.parse("jdbc:chargewire://localhost:1234;user=joe").options);
  }

  @Test
  void parsesSemicolonsInsideTheQuery() throws SQLException {
    assertEquals(
        Map.of("a", "1", "b", "2"),
        ChargewireUrl.parse("jdbc:chargewire://localhost:1234?a=1;b=2").options);
  }

  @Test
  void parsesIpAddresses() throws SQLException {
    assertEquals("127.0.0.1", ChargewireUrl.parse("jdbc:chargewire://127.0.0.1:1234").host);
    ChargewireUrl ipv6 = ChargewireUrl.parse("jdbc:chargewire://[::1]:1234");
    assertEquals("::1", ipv6.host);
    assertEquals(1234, ipv6.port);
  }

  @Test
  void decodesPercentEncodedValuesAndKeepsPlus() throws SQLException {
    ChargewireUrl url =
        ChargewireUrl.parse(
            "jdbc:chargewire://localhost:1234?password=p%40ss%3Aw%26rd%23%3B&token=a+b");
    assertEquals("p@ss:w&rd#;", url.options.get("password"));
    assertEquals("a+b", url.options.get("token"));
  }

  @Test
  void portIsOptional() throws SQLException {
    assertEquals(-1, ChargewireUrl.parse("jdbc:chargewire://localhost").port);
    assertNull(ChargewireUrl.parse("jdbc:chargewire://localhost/").catalog);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:chargewire:",
        "jdbc:chargewire:localhost:1",
        "jdbc:chargewire://",
        "jdbc:chargewire://:1234",
        "jdbc:chargewire://localhost:1/a/b",
        "jdbc:chargewire://localhost:1?novalue",
        "jdbc:chargewire://localhost:x",
        "jdbc:chargewire://localhost:1#fragment"
      })
  void rejectsMalformedUrls(String url) {
    SQLException error =
        assertThrows(SQLNonTransientConnectionException.class, () -> ChargewireUrl.parse(url));
    assertEquals("08001", error.getSQLState());
  }

  @Test
  void redactsSecrets() {
    assertEquals(
        "jdbc:chargewire://h?user=joe&password=***;token=***&trustStorePassword=***",
        ChargewireUrl.redact(
            "jdbc:chargewire://h?user=joe&password=s3cr3t;token=abc&trustStorePassword=x"));
  }

  @Test
  void propertyInfoListsEveryPropertyWithUrlValues() throws SQLException {
    Properties info = new Properties();
    info.setProperty("USER", "from-properties");
    DriverPropertyInfo[] properties =
        new ChargewireDriver()
            .getPropertyInfo("jdbc:chargewire://h:1/lake?useEncryption=false", info);
    assertEquals(ChargewireProperty.values().length, properties.length);
    Map<String, String> values = new java.util.HashMap<>();
    for (DriverPropertyInfo property : properties) {
      values.put(property.name, property.value);
    }
    assertEquals("from-properties", values.get("user"), "keys are case-insensitive");
    assertEquals("false", values.get("useEncryption"));
    assertEquals("lake", values.get("catalog"), "the URL path is the catalog");
    assertEquals("/chargewire", values.get("basePath"));
    DriverPropertyInfo compression =
        Arrays.stream(properties).filter(p -> p.name.equals("ipcCompression")).findFirst().get();
    assertEquals(
        Arrays.asList("auto", "lz4_frame", "zstd", "none"), Arrays.asList(compression.choices));
    assertEquals("auto", compression.value, "the default");
    DriverPropertyInfo partitions =
        Arrays.stream(properties).filter(p -> p.name.equals("resultPartitions")).findFirst().get();
    assertEquals("32", partitions.value, "the default");
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
        new ChargewireDriver().getPropertyInfo("jdbc:chargewire://h:1", withDefaults)) {
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
            () ->
                new ChargewireDriver()
                    .connect("jdbc:chargewire://127.0.0.1:1?useEncryption=false", null));
    assertEquals("08001", error.getSQLState());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "useEncryption=maybe",
        "connectTimeout=soon",
        "connectTimeout=-1",
        "ipcCompression=gzip",
        "ipcCompression=lz4",
        "ipcCompression=",
        "ipcCompression=zstd,lz4_frame",
        "retainResults=perhaps"
      })
  void rejectsInvalidPropertyValues(String option) {
    SQLException error =
        assertThrows(
            SQLException.class,
            () -> new ChargewireDriver().connect("jdbc:chargewire://127.0.0.1:1?" + option, null));
    assertEquals("08001", error.getSQLState());
    assertTrue(error.getMessage().startsWith("Invalid value"), error.getMessage());
  }

  @Test
  void reportsItsVersion() {
    ChargewireDriver driver = new ChargewireDriver();
    assertTrue(
        ChargewireDriver.version().matches("\\d+\\.\\d+\\.\\d+.*"), ChargewireDriver.version());
    assertEquals(ChargewireDriver.versionPart(0), driver.getMajorVersion());
    assertEquals(ChargewireDriver.versionPart(1), driver.getMinorVersion());
    assertFalse(driver.jdbcCompliant());
  }
}
