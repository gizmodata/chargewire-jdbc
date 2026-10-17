package com.gizmodata.warpsql.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A parsed {@code jdbc:warp://host[:port][/catalog][?key=value&...][;key=value;...]} URL.
 *
 * <p>Option values are percent-decoded; option keys are case-insensitive and kept here in lower
 * case. The port defaults to 443 (TLS) or 80 ({@code useEncryption=false}), because Warp SQL is
 * plain HTTP and works through ordinary reverse proxies.
 */
final class WarpUrl {
  static final String PREFIX = "jdbc:warp:";

  final String host;
  final int port;
  final String catalog;
  final Map<String, String> options;

  private WarpUrl(String host, int port, String catalog, Map<String, String> options) {
    this.host = host;
    this.port = port;
    this.catalog = catalog;
    this.options = options;
  }

  static boolean accepts(String url) {
    return url != null && url.regionMatches(true, 0, PREFIX, 0, PREFIX.length());
  }

  static WarpUrl parse(String url) throws SQLException {
    if (!accepts(url)) {
      throw invalid(url, "it must start with " + PREFIX + "//");
    }
    String rest = url.substring(PREFIX.length());
    if (!rest.startsWith("//")) {
      throw invalid(url, "it must start with " + PREFIX + "//");
    }
    Map<String, String> options = new LinkedHashMap<>();
    int semicolon = rest.indexOf(';');
    if (semicolon >= 0) {
      parseOptions(url, rest.substring(semicolon + 1), ';', options);
      rest = rest.substring(0, semicolon);
    }
    URI uri;
    try {
      uri = new URI("warp:" + rest);
    } catch (URISyntaxException e) {
      throw invalid(url, e.getMessage());
    }
    String host = uri.getHost();
    if (host == null || host.isEmpty()) {
      throw invalid(url, "it has no host");
    }
    if (uri.getRawQuery() != null) {
      parseOptions(url, uri.getRawQuery(), '&', options);
    }
    if (uri.getRawFragment() != null) {
      throw invalid(url, "it has a fragment ('#'); percent-encode '#' in option values");
    }
    String catalog = null;
    String path = uri.getPath();
    if (path != null && !path.isEmpty() && !"/".equals(path)) {
      catalog = path.substring(1);
      if (catalog.endsWith("/")) {
        catalog = catalog.substring(0, catalog.length() - 1);
      }
      if (catalog.contains("/")) {
        throw invalid(url, "the path may only name a catalog");
      }
    }
    if (host.startsWith("[") && host.endsWith("]")) {
      host = host.substring(1, host.length() - 1);
    }
    return new WarpUrl(host, uri.getPort(), catalog, options);
  }

  private static void parseOptions(
      String url, String text, char separator, Map<String, String> options) throws SQLException {
    for (String pair : text.split(separator == '&' ? "[&;]" : ";")) {
      if (pair.isEmpty()) {
        continue;
      }
      int equals = pair.indexOf('=');
      if (equals <= 0) {
        throw invalid(url, "option '" + pair + "' is not key=value");
      }
      String key = decode(pair.substring(0, equals)).toLowerCase(Locale.ROOT);
      options.put(key, decode(pair.substring(equals + 1)));
    }
  }

  private static String decode(String text) {
    return URLDecoder.decode(text.replace("+", "%2B"), StandardCharsets.UTF_8);
  }

  private static SQLException invalid(String url, String why) {
    return new SQLNonTransientConnectionException(
        "Invalid Warp SQL URL '" + redact(url) + "': " + why, "08001");
  }

  /** Hides password and token values, so a URL can appear in messages and logs. */
  static String redact(String url) {
    if (url == null) {
      return null;
    }
    return url.replaceAll("(?i)((?:password|token|subjecttoken|truststorepassword)=)[^&;]*", "$1***");
  }
}
