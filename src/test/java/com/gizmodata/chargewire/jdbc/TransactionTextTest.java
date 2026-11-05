package com.gizmodata.chargewire.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** What a statement's text may do to the session's transaction. */
class TransactionTextTest {
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
  void readsWhatTheTextMayDo(String sql, TransactionText.Effect expected) {
    assertEquals(expected, TransactionText.of(sql.replace("\\n", "\n")), sql);
  }
}
