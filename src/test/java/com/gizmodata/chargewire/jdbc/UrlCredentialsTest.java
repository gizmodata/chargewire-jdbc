package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.List;
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
}
