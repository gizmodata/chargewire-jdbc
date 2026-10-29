package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * SQL LIKE patterns for column filters (ported from {@code
 * ArrowDatabaseMetadataTest#testSqlToRegexLike}).
 */
class ChargewireDatabaseMetaDataTest {
  private static boolean matches(String like, String name) {
    Pattern pattern = ChargewireDatabaseMetaData.likePattern(like);
    return pattern.matcher(name).matches();
  }

  @Test
  void likePatterns() {
    assertTrue(matches("%", "anything"));
    assertTrue(matches("%", ""));
    assertTrue(matches("a_c", "abc"));
    assertFalse(matches("a_c", "abbc"));
    assertTrue(matches("col%", "column"));
    assertFalse(matches("col%", "a_column"));
    assertTrue(matches("at\\_time", "at_time"));
    assertFalse(matches("at\\_time", "atXtime"), "an escaped _ is literal");
    assertTrue(matches("100\\%", "100%"));
    assertTrue(matches("a.b*c", "a.b*c"), "regex characters are literal");
    assertFalse(matches("a.b", "axb"));
    assertTrue(matches("line%", "line\nbreak"));
  }
}
