package com.gizmodata.warpsql.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import org.junit.jupiter.api.Test;

/** Statement documents (spec §6.2). */
class StatementDocumentTest {
  private final ObjectMapper mapper = new ObjectMapper();

  private StatementDocument parse(String json) throws Exception {
    return StatementDocument.parse(mapper.readTree(json));
  }

  @Test
  void rowsResult() throws Exception {
    StatementDocument document = parse(
        "{\"statement_id\":\"q_1\",\"state\":\"succeeded\",\"progress\":{\"fraction\":1},"
            + "\"result\":{\"kind\":\"rows\",\"schema\":\"AQID\",\"partitions\":["
            + "{\"index\":0,\"location\":null,\"batches\":2,\"rows\":10},"
            + "{\"index\":1,\"location\":\"https://node2:443/warp\"}],"
            + "\"retained\":true,\"expires_at\":\"2026-10-16T22:04:05Z\"},\"future\":true}");
    assertEquals("q_1", document.statementId());
    assertEquals(StatementDocument.State.SUCCEEDED, document.state());
    assertTrue(document.state().isTerminal());
    assertTrue(document.hasRows());
    assertTrue(document.retained());
    assertArrayEquals(new byte[] {1, 2, 3}, document.schema());
    assertEquals(2, document.partitions().size());
    assertNull(document.partitions().get(0).location());
    assertEquals(URI.create("https://node2:443/warp"), document.partitions().get(1).location());
    assertEquals(1, document.partitions().get(1).index());
  }

  @Test
  void updateResults() throws Exception {
    StatementDocument counted = parse(
        "{\"statement_id\":\"q_2\",\"state\":\"succeeded\",\"result\":{\"kind\":\"update\",\"rows_affected\":42}}");
    assertFalse(counted.hasRows());
    assertEquals(42, counted.rowsAffected());
    StatementDocument ddl =
        parse("{\"statement_id\":\"q_3\",\"state\":\"succeeded\",\"result\":{\"kind\":\"update\"}}");
    assertEquals(-1, ddl.rowsAffected(), "DDL has no count");
  }

  @Test
  void failedAndCancelled() throws Exception {
    StatementDocument failed = parse(
        "{\"statement_id\":\"q_4\",\"state\":\"failed\",\"result\":null,"
            + "\"error\":{\"code\":\"execution_error\",\"detail\":\"boom\",\"status\":500}}");
    assertEquals("execution_error", failed.error().code());
    assertEquals("boom", failed.error().detail());
    StatementDocument cancelled = parse(
        "{\"statement_id\":\"q_5\",\"state\":\"cancelled\",\"cancel_reason\":\"timeout\","
            + "\"error\":{\"code\":\"statement_timeout\"}}");
    assertEquals("timeout", cancelled.cancelReason());
    assertEquals("statement_timeout", cancelled.error().code());
  }

  @Test
  void runningStatesAreNotTerminal() throws Exception {
    assertFalse(parse("{\"statement_id\":\"q\",\"state\":\"queued\"}").state().isTerminal());
    assertFalse(parse("{\"statement_id\":\"q\",\"state\":\"running\"}").state().isTerminal());
  }

  @Test
  void rejectsWhatIsNotAStatementDocument() {
    assertThrows(IllegalArgumentException.class, () -> parse("{\"state\":\"running\"}"));
    assertThrows(IllegalArgumentException.class, () -> parse("{\"statement_id\":\"q\",\"state\":\"exploded\"}"));
  }
}
