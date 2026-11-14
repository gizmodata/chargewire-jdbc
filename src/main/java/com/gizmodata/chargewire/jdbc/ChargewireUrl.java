package com.gizmodata.chargewire.jdbc;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A parsed {@code jdbc:chargewire://host[:port][/catalog][?key=value&...][;key=value;...]} URL.
 *
 * <p>Option values are percent-decoded; option keys are case-insensitive and kept here in lower
 * case. The port defaults to 443 (TLS) or 80 ({@code useEncryption=false}), because Chargewire is
 * plain HTTP and works through ordinary reverse proxies.
 */
final class ChargewireUrl {
  static final String PREFIX = "jdbc:chargewire:";

  final String host;
  final int port;
  final String catalog;
  final Map<String, String> options;

  /** The option keys as they were written, by their lower-case form, for messages. */
  final Map<String, String> writtenKeys;

  private ChargewireUrl(
      String host,
      int port,
      String catalog,
      Map<String, String> options,
      Map<String, String> writtenKeys) {
    this.host = host;
    this.port = port;
    this.catalog = catalog;
    this.options = options;
    this.writtenKeys = writtenKeys;
  }

  static boolean accepts(String url) {
    String current = LegacyUrls.modernize(url);
    return current != null && current.regionMatches(true, 0, PREFIX, 0, PREFIX.length());
  }

  static ChargewireUrl parse(String url) throws SQLException {
    if (!accepts(url)) {
      throw invalid(url, "it must start with " + PREFIX + "//");
    }
    String rest = LegacyUrls.modernize(url).substring(PREFIX.length());
    if (!rest.startsWith("//")) {
      throw invalid(url, "it must start with " + PREFIX + "//");
    }
    Map<String, String> options = new LinkedHashMap<>();
    Map<String, String> writtenKeys = new LinkedHashMap<>();
    int semicolon = rest.indexOf(';');
    if (semicolon >= 0) {
      parseOptions(url, rest.substring(semicolon + 1), ';', options, writtenKeys);
      rest = rest.substring(0, semicolon);
    }
    URI uri;
    try {
      uri = new URI("chargewire:" + rest);
    } catch (URISyntaxException e) {
      throw invalid(url, e.getMessage());
    }
    String host = uri.getHost();
    if (host == null || host.isEmpty()) {
      throw invalid(url, "it has no host");
    }
    if (uri.getRawQuery() != null) {
      parseOptions(url, uri.getRawQuery(), '&', options, writtenKeys);
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
    return new ChargewireUrl(host, uri.getPort(), catalog, options, writtenKeys);
  }

  private static void parseOptions(
      String url,
      String text,
      char separator,
      Map<String, String> options,
      Map<String, String> writtenKeys)
      throws SQLException {
    for (String pair : text.split(separator == '&' ? "[&;]" : ";")) {
      if (pair.isEmpty()) {
        continue;
      }
      int equals = pair.indexOf('=');
      if (equals <= 0) {
        throw invalid(url, "option '" + pair + "' is not key=value");
      }
      String written = decode(pair.substring(0, equals));
      String key = written.toLowerCase(Locale.ROOT);
      options.put(key, decode(pair.substring(equals + 1)));
      writtenKeys.put(key, written);
    }
  }

  private static String decode(String text) {
    return URLDecoder.decode(text.replace("+", "%2B"), StandardCharsets.UTF_8);
  }

  private static SQLException invalid(String url, String why) {
    return new SQLNonTransientConnectionException(
        "Invalid Chargewire URL '" + redact(url) + "': " + why, "08001");
  }

  private static final Pattern SCHEME =
      Pattern.compile("[A-Za-z][A-Za-z0-9+.-]*:[A-Za-z][A-Za-z0-9+.-]*:");
  private static final Pattern HOST_AND_PORT =
      Pattern.compile("(\\[[^\\]@/]*\\]|[^:@/\\[\\]]*)(:[0-9]*)?");

  /** What a message may show of a URL. */
  static final class Shown {
    /** The URL with everything that may be part of a password or token as {@code ***}. */
    final String text;

    /** The option keys {@link #text} names, in lower case. */
    final Set<String> named;

    /**
     * The key, as written, of the first hidden value: unknown keys after it are not named, because
     * a password with an unencoded {@code &} or {@code ;} continues there. {@code null} when no
     * value is hidden, or when what is hidden starts with something else.
     */
    final String hiddenFrom;

    Shown(String text, Set<String> named, String hiddenFrom) {
      this.text = text;
      this.named = named;
      this.hiddenFrom = hiddenFrom;
    }
  }

  /**
   * Hides everything in a URL that may be part of a password or token, so the URL can appear in
   * messages, logs and {@code DatabaseMetaData.getURL()}.
   *
   * <p>A password that was not percent-encoded can hold any character, so what is hidden is decided
   * from the text, never from a parse that such a password has already misled:
   *
   * <ul>
   *   <li>The value of a password or token option, and the value of every key the driver does not
   *       know (a misspelled {@code password} key holds a password all the same), is {@code ***}.
   *   <li>After the first hidden value, only the driver's own options are shown: {@code &} and
   *       {@code ;} end a value, so the rest of a password that holds one reads as further options.
   *       An unknown key and an option without {@code =} are {@code ***} whole there, and a
   *       fragment is {@code #***} anywhere.
   *   <li>{@code user:password@} before the host is {@code ***@}.
   *   <li>An {@code @} in the path, in an option that is not {@code key=value}, in an unknown key's
   *       option or in the fragment is the end of such credentials that held a {@code /}, {@code
   *       ?}, {@code ;} or {@code #}: then only the scheme is shown. So is a host and port that is
   *       not {@code host[:digits]}, which is how the start of such credentials reads.
   * </ul>
   *
   * <p>What stays shown is what the URL says to the driver: a password that itself spells {@code
   * &user=…}, with one of the driver's own keys, is a URL with that {@code user} option.
   */
  static String redact(String url) {
    return url == null ? null : shown(url).text;
  }

  static Shown shown(String url) {
    Matcher scheme = SCHEME.matcher(url);
    if (!scheme.lookingAt()) {
      return new Shown("***", Set.of(), null);
    }
    String head = url.startsWith("//", scheme.end()) ? scheme.group() + "//" : scheme.group();
    String body = url.substring(head.length());
    int at = firstOf(body, 0, "?;#");
    String address = body.substring(0, at);
    StringBuilder options = new StringBuilder();
    Set<String> named = new LinkedHashSet<>();
    String hiddenFrom = null;
    boolean hidden = false;
    boolean stray = false;
    while (at < body.length()) {
      char separator = body.charAt(at);
      if (separator == '#') {
        options.append("#***");
        stray |= body.indexOf('@', at) >= 0;
        break;
      }
      int next = firstOf(body, at + 1, "?&;#");
      String segment = body.substring(at + 1, next);
      at = next;
      options.append(separator);
      if (segment.isEmpty()) {
        continue;
      }
      int equals = segment.indexOf('=');
      String key = equals > 0 ? decodedKey(segment.substring(0, equals)) : null;
      ChargewireProperty property = key == null ? null : ChargewireProperty.of(key);
      if (property != null && !property.secret()) {
        options.append(segment);
        named.add(key.toLowerCase(Locale.ROOT));
        continue;
      }
      if (property == null && segment.indexOf('@') >= 0) {
        stray = true;
      }
      if (key != null && (property != null || !hidden)) {
        options.append(segment, 0, equals).append("=***");
        named.add(key.toLowerCase(Locale.ROOT));
        if (!hidden) {
          hiddenFrom = key;
        }
      } else {
        options.append("***");
      }
      hidden = true;
    }
    int slash = address.indexOf('/');
    String authority = slash < 0 ? address : address.substring(0, slash);
    String path = slash < 0 ? "" : address.substring(slash);
    String hostAndPort = authority.substring(authority.lastIndexOf('@') + 1);
    if (stray || path.indexOf('@') >= 0 || !HOST_AND_PORT.matcher(hostAndPort).matches()) {
      return new Shown(head + "***", Set.of(), null);
    }
    return new Shown(
        head + (authority.indexOf('@') >= 0 ? "***@" : "") + hostAndPort + path + options,
        named,
        hiddenFrom);
  }

  private static int firstOf(String text, int from, String characters) {
    for (int i = from; i < text.length(); i++) {
      if (characters.indexOf(text.charAt(i)) >= 0) {
        return i;
      }
    }
    return text.length();
  }

  private static String decodedKey(String key) {
    try {
      return decode(key);
    } catch (IllegalArgumentException e) {
      // Not percent-encoding: no key the driver knows.
      return null;
    }
  }
}
