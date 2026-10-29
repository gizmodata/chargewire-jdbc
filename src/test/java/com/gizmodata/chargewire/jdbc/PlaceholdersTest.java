package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Counting parameters in SQL text, for servers that cannot prepare statements. */
class PlaceholdersTest {
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "SELECT 1 | 0",
        "SELECT ?, ? | 2",
        "SELECT '?', \"?\" | 0",
        "SELECT 'it''s ?', ? | 1",
        "SELECT ? -- a comment ?\\n, ? | 2",
        "SELECT /* ? */ ? | 1",
        "SELECT $1, $2, $1 | 2",
        "SELECT $10 | 10",
        "SELECT '$1' | 0",
        "SELECT 'unterminated ? | 0"
      })
  void counts(String sql, int expected) {
    assertEquals(expected, Placeholders.count(sql.replace("\\n", "\n")));
  }
}
