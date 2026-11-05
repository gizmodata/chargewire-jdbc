package com.gizmodata.chargewire.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The address the driver connects to. */
class ConnectionConfigTest {
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "db.example.com | https://db.example.com:443/chargewire",
        "db.example.com. | https://db.example.com:443/chargewire",
        "DB.Example.COM. | https://DB.Example.COM:443/chargewire",
        "localhost. | https://localhost:443/chargewire",
        "db.example.com.. | https://db.example.com.:443/chargewire",
        "127.0.0.1 | https://127.0.0.1:443/chargewire",
        "::1 | https://[::1]:443/chargewire"
      })
  void connectsToTheHostWithoutOneFinalDot(String host, String expected) {
    // Java's TLS stack refuses an SNI name with a trailing dot, so a fully qualified name
    // ("db.example.com.") is connected to without it: the URI, SNI and the certificate check all
    // use the dotless name.
    ConnectionConfig config =
        new ConnectionConfig.Builder().address(host, -1).basePath("/chargewire").build();
    assertEquals(expected, config.baseUri.toString());
  }
}
