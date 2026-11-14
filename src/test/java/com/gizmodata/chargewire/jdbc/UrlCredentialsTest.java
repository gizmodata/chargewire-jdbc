package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * No part of a password or token written in a URL is ever shown: not when it holds characters that
 * should have been percent-encoded, which is when the URL is refused and its text is in the
 * message.
 */
class UrlCredentialsTest {
  /** The two halves of the password or token of every URL here; neither may ever be shown. */
  static final String HEAD = "Zq7Kv";

  static final String TAIL = "Xw9Jp";

  /** Characters a generated password holds and a URL gives a meaning to, or refuses. */
  static final String[] SPECIAL = {
    "#", "&", ";", "?", "=", "/", "@", ":", "%", " ", "^", "|", "{", "}", "\"", "<", ">", "\\", "`",
    "[", "]", "+", "\t", "é", "%zz", "&x=", ";x=", "?x=", "#x=", "&&", "&=", "=&", "@/", "/@", "//"
  };

  /** Where a password or token is written; {@code $} is its place. */
  static final String[] PLACES = {
    "jdbc:chargewire://nohost.invalid:1/db?password=$",
    "jdbc:chargewire://nohost.invalid:1/db?user=joe&password=$&useEncryption=false",
    "jdbc:chargewire://nohost.invalid:1/db;user=joe;password=$",
    "jdbc:chargewire://nohost.invalid:1/db?useEncryption=false;password=$;user=joe",
    "jdbc:chargewire://nohost.invalid:1?token=$",
    "jdbc:chargewire://nohost.invalid:1?authType=oauth_browser&subjectToken=$",
    "jdbc:chargewire://nohost.invalid:1/db?trustStore=/nonexistent.p12&trustStorePassword=$",
    "jdbc:chargewire://nohost.invalid:1/db?pasword=$",
    "jdbc:chargewire://nohost.invalid:1/db?user=joe&Password=$&catalog=lake",
    "jdbc:chargewire://joe:$@nohost.invalid:1/db",
    "jdbc:chargewire://joe:$@nohost.invalid:1/db?useEncryption=false",
    "jdbc:chargewire://$@nohost.invalid:1",
    "jdbc:warp://nohost.invalid:1/db?password=$",
  };

  /** Every place with every form of the password: whole, and with each character inside it. */
  static List<String> urls() {
    List<String> urls = new ArrayList<>();
    for (String place : PLACES) {
      urls.add(place.replace("$", HEAD + TAIL));
      for (String special : SPECIAL) {
        urls.add(place.replace("$", HEAD + special + TAIL));
        urls.add(place.replace("$", special + HEAD + TAIL));
        urls.add(place.replace("$", HEAD + TAIL + special));
        urls.add(place.replace("$", HEAD + special + TAIL + special + HEAD));
      }
    }
    return urls;
  }

  static void assertHidden(String shown, String url) {
    assertFalse(shown.contains(HEAD), () -> url + " is shown as " + shown);
    assertFalse(shown.contains(TAIL), () -> url + " is shown as " + shown);
  }

  @Test
  void aHashInAPasswordDoesNotShowWhatFollowsIt() {
    SQLException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> ChargewireUrl.parse("jdbc:chargewire://h:1/db?password=" + HEAD + "#" + TAIL));
    assertEquals(
        "Invalid Chargewire URL 'jdbc:chargewire://h:1/db?password=***#***': it has a fragment"
            + " ('#'); percent-encode '#' in option values",
        error.getMessage());
    error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () -> ChargewireUrl.parse("jdbc:chargewire://h:1/db?user=u&password=x#" + TAIL));
    assertEquals(
        "Invalid Chargewire URL 'jdbc:chargewire://h:1/db?user=u&password=***#***': it has a"
            + " fragment ('#'); percent-encode '#' in option values",
        error.getMessage());
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = ' ',
      value = {
        // Nothing to hide.
        "jdbc:chargewire://h:1/db?user=joe&useEncryption=false"
            + " jdbc:chargewire://h:1/db?user=joe&useEncryption=false",
        "jdbc:chargewire://[::1]:8443/db;catalog=lake jdbc:chargewire://[::1]:8443/db;catalog=lake",
        // After the first hidden value, the driver's own options and nothing else.
        "jdbc:chargewire://h:1/db?user=joe&password=p&useEncryption=false&sslmode=x&novalue"
            + " jdbc:chargewire://h:1/db?user=joe&password=***&useEncryption=false&***&***",
        "jdbc:chargewire://h:1/db?password=Zq7Kv&Xw9Jp jdbc:chargewire://h:1/db?password=***&***",
        "jdbc:chargewire://h:1/db?password=Zq7Kv;Xw9Jp=1 jdbc:chargewire://h:1/db?password=***;***",
        "jdbc:chargewire://h:1/db;password=Zq7Kv?Xw9Jp jdbc:chargewire://h:1/db;password=***?***",
        // (A password that spells one of the driver's options is a URL with that option.)
        "jdbc:chargewire://h:1/db?token=Zq7Kv&user=joe jdbc:chargewire://h:1/db?token=***&user=joe",
        // A key the driver does not know may be a misspelled password: the same from there.
        "jdbc:chargewire://h:1/db?pasword=Zq7Kv&user=joe&x=Xw9Jp"
            + " jdbc:chargewire://h:1/db?pasword=***&user=joe&***",
        "jdbc:chargewire://h:1/db?user=joe&sslmode=Zq7Kv&Xw9Jp=1"
            + " jdbc:chargewire://h:1/db?user=joe&sslmode=***&***",
        "jdbc:chargewire://h:1/db?Xw9Jp jdbc:chargewire://h:1/db?***",
        "jdbc:chargewire://h:1/db?%70assword=Zq7Kv jdbc:chargewire://h:1/db?%70assword=***",
        "jdbc:chargewire://h:1/db?%zz=Zq7Kv&user=joe jdbc:chargewire://h:1/db?***&user=joe",
        // An '@' in a value is a value's.
        "jdbc:chargewire://h:1/db?user=joe@example.com&password=p@ss"
            + " jdbc:chargewire://h:1/db?user=joe@example.com&password=***",
        // Credentials before the host, and what is left of them when they hold a delimiter.
        "jdbc:chargewire://joe:Zq7Kv@h:1/db?user=joe jdbc:chargewire://***@h:1/db?user=joe",
        "jdbc:chargewire://joe:Zq7Kv/Xw9Jp@h:1/db jdbc:chargewire://***",
        "jdbc:chargewire://joe:Zq7Kv?Xw9Jp@h:1/db jdbc:chargewire://***",
        "jdbc:chargewire://joe:12345;Xw9Jp@h:1/db jdbc:chargewire://***",
        "jdbc:chargewire://joe:12345#Xw9Jp@h:1/db jdbc:chargewire://***",
        "jdbc:chargewire://joe:12345?x=Xw9Jp@h:1/db jdbc:chargewire://***",
        "jdbc:chargewire://joe:Zq7Kv?user=Xw9Jp jdbc:chargewire://***",
        // The former scheme, and text that is no URL of this driver's.
        "jdbc:warp://h?token=Zq7Kv jdbc:warp://h?token=***",
        "jdbc:chargewire:h:1/db?password=Zq7Kv jdbc:chargewire:h:1/db?password=***",
        "jdbc:chargewire:joe:Zq7Kv:Xw9Jp@h:1/db jdbc:chargewire:***@h:1/db",
        "password=Zq7Kv ***",
      })
  void aUrlIsShownWithoutAnythingThatMayBeACredential(String url, String shown) {
    assertEquals(shown, ChargewireUrl.redact(url));
  }

  @Test
  void noFormOfAPasswordIsShownWhereverItIsWritten() {
    for (String url : urls()) {
      assertHidden(ChargewireUrl.redact(url), url);
    }
  }

  // ---- Credentials before the host ----

  @ParameterizedTest
  @CsvSource({
    "jdbc:chargewire://joe:Zq7KvXw9Jp@h:1/db,jdbc:chargewire://***@h:1/db",
    "jdbc:chargewire://joe@h:1/db?useEncryption=false,"
        + "jdbc:chargewire://***@h:1/db?useEncryption=false",
    "jdbc:chargewire://joe:Zq7Kv:Xw9Jp@h,jdbc:chargewire://***@h",
    "jdbc:chargewire://joe:Zq7Kv@Xw9Jp@h:1/db;user=joe,jdbc:chargewire://***@h:1/db;user=joe",
    "jdbc:chargewire://joe:Zq7Kv%40Xw9Jp@[::1]:8443,jdbc:chargewire://***@[::1]:8443",
    "jdbc:warp://joe:Zq7KvXw9Jp@h,jdbc:warp://***@h",
  })
  void credentialsBeforeTheHostAreRefusedAndNotShown(String url, String shown) {
    // 2.2.6 connected without them, and without a word.
    SQLException error =
        assertThrows(SQLNonTransientConnectionException.class, () -> ChargewireUrl.parse(url));
    assertEquals("08001", error.getSQLState());
    assertEquals(
        "Invalid Chargewire URL '"
            + shown
            + "': it has credentials before the host ('user:password@'), which this driver does"
            + " not read from there; give them as the user and password properties, or as the"
            + " URL's user and password options",
        error.getMessage());
  }

  // ---- What a refusal says ----

  /** Every message of a failure: its causes, the suppressed and the chained ones. */
  static String messages(Throwable failure) {
    StringBuilder all = new StringBuilder();
    java.util.Deque<Throwable> left = new java.util.LinkedList<>(List.of(failure));
    java.util.Set<Throwable> seen =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    while (!left.isEmpty()) {
      Throwable next = left.pop();
      if (next == null || !seen.add(next)) {
        continue;
      }
      all.append(next).append('\n');
      left.add(next.getCause());
      left.addAll(List.of(next.getSuppressed()));
      if (next instanceof SQLException) {
        left.add(((SQLException) next).getNextException());
      }
    }
    return all.toString();
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        // java.net.URI's own message quotes its whole input; only its reason is passed on.
        "jdbc:chargewire://h:1/db?password=Zq7Kv Xw9Jp"
            + "|Invalid Chargewire URL 'jdbc:chargewire://h:1/db?password=***': Illegal character"
            + " in query (in a value, every character but letters, digits and -._~ is"
            + " percent-encoded: a space is %20, a literal % is %25)",
        "jdbc:chargewire://h:1/db?password=Zq7Kv%Xw9Jp"
            + "|Invalid Chargewire URL 'jdbc:chargewire://h:1/db?password=***': Malformed escape"
            + " pair (in a value, every character but letters, digits and -._~ is"
            + " percent-encoded: a space is %20, a literal % is %25)",
        "jdbc:chargewire://h:1/db;password=Zq7Kv%Xw9Jp"
            + "|Invalid Chargewire URL 'jdbc:chargewire://h:1/db;password=***': an option has a"
            + " '%' that two hexadecimal digits do not follow; a literal % is written %25",
        // An option that is not key=value is not quoted: it may be the rest of a password.
        "jdbc:chargewire://h:1/db?password=Zq7Kv&Xw9Jp"
            + "|Invalid Chargewire URL 'jdbc:chargewire://h:1/db?password=***&***': an option is"
            + " not key=value; an '&' or ';' inside a value is written %26 or %3B",
        "jdbc:chargewire://h:1/db?password=Zq7Kv;Xw9Jp"
            + "|Invalid Chargewire URL 'jdbc:chargewire://h:1/db?password=***;***': an option is"
            + " not key=value; an '&' or ';' inside a value is written %26 or %3B",
        "jdbc:chargewire://h:1/db?Xw9Jp"
            + "|Invalid Chargewire URL 'jdbc:chargewire://h:1/db?***': an option is"
            + " not key=value; an '&' or ';' inside a value is written %26 or %3B",
      })
  void aRefusedUrlIsNotQuotedInTheReason(String url, String message) {
    SQLException error =
        assertThrows(SQLNonTransientConnectionException.class, () -> ChargewireUrl.parse(url));
    assertEquals("08001", error.getSQLState());
    assertEquals(message, error.getMessage());
  }

  @Test
  void anUnknownKeyAfterAHiddenValueIsNotNamed() throws SQLException {
    // The rest of a password that holds an unencoded '&' reads as a key: 2.2.6 ignored it, and
    // naming it as an unknown property would show it.
    SQLException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () ->
                ChargewireDriver.unknownKeys(
                    ChargewireUrl.parse(
                        "jdbc:chargewire://h:1/db?user=joe&password=" + HEAD + "&" + TAIL + "=1"),
                    new Properties()));
    assertEquals("08001", error.getSQLState());
    assertEquals(
        "Unknown connection property in the URL after 'password' (not named: it may be the rest"
            + " of that value, in which an '&' or ';' is written %26 or %3B); the driver's"
            + " properties are "
            + PROPERTIES,
        error.getMessage());
    // The first unknown key is named as before, with what it resembles.
    error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () ->
                ChargewireDriver.unknownKeys(
                    ChargewireUrl.parse("jdbc:chargewire://h:1/db?user=joe&pasword=" + HEAD),
                    new Properties()));
    assertEquals(
        "Unknown connection property 'pasword' in the URL: did you mean 'password'?",
        error.getMessage());
    error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () ->
                ChargewireDriver.unknownKeys(
                    ChargewireUrl.parse(
                        "jdbc:chargewire://h:1/db;sslmode=require?pasword=" + HEAD + "&x=1"),
                    new Properties()));
    assertEquals(
        "Unknown connection property 'sslmode' in the URL: the driver's properties are "
            + PROPERTIES,
        error.getMessage());
  }

  private static final String PROPERTIES =
      java.util.Arrays.stream(ChargewireProperty.values())
          .map(ChargewireProperty::key)
          .collect(java.util.stream.Collectors.joining(", "));

  @ParameterizedTest
  @CsvSource({
    "useEncryption,true or false",
    "connectTimeout,a number",
    "authType,'password, client_certificate (or mtls) or oauth_browser (or external)'",
    "ipcCompression,'auto, lz4_frame, zstd or none'",
    "resultOrder,preserve or any",
    "retainResults,true or false",
  })
  void anInvalidValueIsNotQuoted(String key, String expected) {
    // A password pasted as another option's value, or the rest of one after an unencoded '&'.
    SQLException error =
        assertThrows(
            SQLNonTransientConnectionException.class,
            () ->
                new ChargewireDriver()
                    .connect("jdbc:chargewire://127.0.0.1:1?" + key + "=" + HEAD + TAIL, null));
    assertEquals("08001", error.getSQLState());
    assertEquals("Invalid value for " + key + ": expected " + expected, error.getMessage());
  }

  /**
   * Every place and form of the password, through everything a connection attempt does before it
   * reaches the network: the URL is read, its keys are checked, and its options become the
   * connection's settings. Whatever is refused is refused with a {@code SQLException}, and no
   * message of it holds either half of the password.
   */
  @Test
  void noRefusalShowsAnyPartOfAPassword() {
    java.util.Map<String, Integer> kinds = new java.util.TreeMap<>();
    for (String url : urls()) {
      for (Properties properties : List.of(new Properties(), credentials())) {
        try {
          ChargewireUrl parsed = ChargewireUrl.parse(url);
          ChargewireDriver.unknownKeys(parsed, properties);
          ChargewireDriver.config(parsed, ChargewireDriver.merge(parsed, properties));
          kinds.merge("accepted", 1, Integer::sum);
        } catch (SQLException e) {
          assertTrue(List.of("08001", "28000").contains(e.getSQLState()), url);
          String messages = messages(e);
          assertHidden(messages, url);
          kinds.merge(messages.replaceAll("^[^:]*: |[':(\n].*", "").trim(), 1, Integer::sum);
        }
      }
    }
    // The sweep reached every kind of refusal, not one of them two thousand times.
    assertEquals(
        java.util.Set.of(
            "accepted",
            "Invalid Chargewire URL",
            "authType=oauth_browser signs in through the browser with the server",
            "Unknown connection property",
            "Unknown connection property in the URL",
            "Unknown connection property in the URL after"),
        kinds.keySet(),
        kinds.toString());
  }

  /**
   * The same through whole connection attempts, to a port nothing listens on, for the password as
   * it is and with a character in it that a URL takes in a value.
   */
  @Test
  void noFailureToConnectShowsAnyPartOfAPassword() throws Exception {
    int port;
    try (java.net.ServerSocket free = new java.net.ServerSocket(0)) {
      port = free.getLocalPort();
    }
    ChargewireDriver driver = new ChargewireDriver();
    int unreachable = 0;
    for (String place : PLACES) {
      for (String password : List.of(HEAD + TAIL, HEAD + "@" + TAIL + "=" + HEAD)) {
        String url = place.replace("$", password).replace("nohost.invalid:1", "127.0.0.1:" + port);
        SQLException error =
            assertThrows(SQLException.class, () -> driver.connect(url, credentials()).close(), url);
        String messages = messages(error);
        assertHidden(messages, url);
        if (messages.contains("127.0.0.1:" + port)) {
          unreachable++;
        }
      }
    }
    assertTrue(unreachable >= 10, "connections that got as far as the network: " + unreachable);
  }

  @Test
  void aTrustStoreThatCannotBeReadIsRefusedWithoutItsPassword(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
    java.nio.file.Path store = directory.resolve("trust.p12");
    java.security.KeyStore empty = java.security.KeyStore.getInstance("PKCS12");
    empty.load(null, null);
    try (java.io.OutputStream out = java.nio.file.Files.newOutputStream(store)) {
      empty.store(out, "changeit".toCharArray());
    }
    ChargewireDriver driver = new ChargewireDriver();
    for (java.nio.file.Path file : List.of(store, directory.resolve("missing.p12"), directory)) {
      String url =
          "jdbc:chargewire://127.0.0.1:1/db?trustStore="
              + file.toUri().getRawPath()
              + "&trustStorePassword="
              + HEAD
              + TAIL;
      SQLException error =
          assertThrows(
              SQLNonTransientConnectionException.class,
              () -> driver.connect(url, credentials()).close(),
              url);
      assertEquals("08001", error.getSQLState());
      assertTrue(error.getMessage().startsWith("Cannot set up TLS: "), error.getMessage());
      assertHidden(messages(error), url);
    }
  }

  /** A password and a token in the Properties too: they are in no message either. */
  private static Properties credentials() {
    Properties properties = new Properties();
    properties.setProperty("user", "joe");
    properties.setProperty("password", TAIL + HEAD + TAIL);
    properties.setProperty("connectTimeout", "2000");
    return properties;
  }
}
