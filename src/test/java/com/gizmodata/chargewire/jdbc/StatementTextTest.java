package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** What a statement's text may do to its session. */
class StatementTextTest {
  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "SELECT 1 | NEITHER",
        "INSERT INTO t VALUES (1) | NEITHER",
        "BEGIN | MAY_OPEN",
        "begin transaction | MAY_OPEN",
        "  BEGIN; | MAY_OPEN",
        "START TRANSACTION | MAY_OPEN",
        "START | MAY_OPEN",
        "EXECUTE p(1) | MAY_OPEN",
        "execute p | MAY_OPEN",
        "EXPLAIN ANALYZE EXECUTE p | MAY_OPEN",
        "EXPLAIN ANALYZE SELECT 1 | NEITHER",
        "SET autocommit = false | MAY_OPEN",
        "PRAGMA something('AutoCommit') | MAY_OPEN",
        "SELECT 1 -- autocommit | MAY_OPEN",
        "COMMIT | MAY_END",
        "commit transaction | MAY_END",
        "ROLLBACK | MAY_END",
        "END | MAY_END",
        "end transaction | MAY_END",
        "ABORT | MAY_END",
        "COMMIT; BEGIN | BOTH",
        "BEGIN; INSERT INTO t VALUES (1); COMMIT | BOTH",
        "SELECT 1; BEGIN | MAY_OPEN",
        "SELECT 1; ROLLBACK | MAY_END",
        "SELECT CASE WHEN a THEN 1 END FROM t | NEITHER",
        "SELECT start, begin_date, \"begin\" FROM t | NEITHER",
        "SELECT 'x; BEGIN' | NEITHER",
        "SELECT 'it''s; BEGIN' | NEITHER",
        "SELECT E'it\\'s; BEGIN' | NEITHER",
        "SELECT \"a;\"\"b\"; BEGIN | MAY_OPEN",
        "SELECT $$; BEGIN$$ | NEITHER",
        "SELECT $tag$; BEGIN $$ ;$tag$ | NEITHER",
        "SELECT $1; BEGIN | MAY_OPEN",
        "/* BEGIN */ SELECT 1 | NEITHER",
        "/* a /* nested */ ; BEGIN */ SELECT 1 | NEITHER",
        "/* a comment */ BEGIN | MAY_OPEN",
        "-- BEGIN\\nSELECT 1 | NEITHER",
        "-- a comment\\nBEGIN | MAY_OPEN",
        "(SELECT 1); COMMIT | MAY_END",
        "'' | NEITHER"
      })
  void readsWhatTheTextMayDo(String sql, StatementText.Effect expected) {
    assertEquals(expected, StatementText.of(sql.replace("\\n", "\n")), sql);
  }

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      value = {
        "SELECT 1 | false | false",
        "USE other | true | false",
        "use other.analytics | true | false",
        "/* c */ USE x | true | false",
        "SELECT 1; USE x | true | false",
        "SET threads = 4 | false | true",
        "set TimeZone = 'UTC' | false | true",
        "RESET threads | false | true",
        "PRAGMA enable_profiling | false | true",
        "UPDATE t SET a = 1 | false | false",
        "INSERT INTO t SELECT 'USE x; SET y' | false | false",
        "SELECT \"use\", \"set\" FROM t | false | false",
        "-- SET x\\nSELECT 1 | false | false",
        "USE x; SET threads = 1 | true | true",
        "EXPLAIN ANALYZE SET x = 1 | false | true"
      })
  void readsWhetherTheTextChangesWhereOrHowStatementsRun(
      String sql, boolean place, boolean settings) {
    StatementText.Reading reading = StatementText.read(sql.replace("\\n", "\n"));
    assertEquals(place, reading.changesPlace(), sql);
    assertEquals(settings, reading.changesSettings(), sql);
  }

  @Test
  void nullTextChangesNothing() {
    assertFalse(StatementText.read(null).changesPlace());
    assertFalse(StatementText.read(null).changesSettings());
    assertEquals(StatementText.Effect.NEITHER, StatementText.read(null).transaction());
  }
}
