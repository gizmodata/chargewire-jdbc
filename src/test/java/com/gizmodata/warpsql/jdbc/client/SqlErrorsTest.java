package com.gizmodata.warpsql.jdbc.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gizmodata.warpsql.jdbc.WarpProblem;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLNonTransientException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Problem documents become the right {@link SQLException} subclass and SQLSTATE (spec §11). */
class SqlErrorsTest {
  private static WarpProblem problem(int status, String code, String sqlState) {
    return new WarpProblem(status, code, "title", "detail", sqlState, "r_1", null);
  }

  @ParameterizedTest
  @CsvSource({
    "400, bad_request, HY000, java.sql.SQLNonTransientException",
    "400, unknown_option, HY092, java.sql.SQLNonTransientException",
    "400, invalid_option_value, HY024, java.sql.SQLNonTransientException",
    "400, sql_error, 42000, java.sql.SQLSyntaxErrorException",
    "401, invalid_credentials, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "401, invalid_token, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "400, unsupported_grant_type, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "401, certificate_required, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "401, unknown_certificate_identity, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "401, certificate_revoked, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "401, certificate_mismatch, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "401, certificate_identity_mismatch, 28000, java.sql.SQLInvalidAuthorizationSpecException",
    "401, session_ended, 08003, java.sql.SQLNonTransientConnectionException",
    "403, permission_denied, 42501, java.sql.SQLSyntaxErrorException",
    "403, read_only, 25006, java.sql.SQLNonTransientException",
    "404, session_not_found, 08003, java.sql.SQLNonTransientConnectionException",
    "404, statement_not_found, 26000, java.sql.SQLNonTransientException",
    "404, prepared_not_found, 26000, java.sql.SQLNonTransientException",
    "404, partition_not_found, 24000, java.sql.SQLNonTransientException",
    "404, ingest_not_found, HY000, java.sql.SQLNonTransientException",
    "404, not_found, 0A000, java.sql.SQLFeatureNotSupportedException",
    "405, method_not_allowed, 0A000, java.sql.SQLFeatureNotSupportedException",
    "409, result_not_ready, HY000, java.sql.SQLNonTransientException",
    "409, result_not_resumable, 24000, java.sql.SQLNonTransientException",
    "409, ingest_incomplete, HY000, java.sql.SQLNonTransientException",
    "409, ingest_part_exists, HY000, java.sql.SQLNonTransientException",
    "409, statement_cancelled, 57014, java.sql.SQLException",
    "409, statement_timeout, HYT00, java.sql.SQLTimeoutException",
    "410, result_expired, 24000, java.sql.SQLNonTransientException",
    "410, ingest_expired, HY000, java.sql.SQLNonTransientException",
    "413, payload_too_large, 54000, java.sql.SQLNonTransientException",
    "415, unsupported_arrow_type, 0A000, java.sql.SQLFeatureNotSupportedException",
    "421, session_elsewhere, 08001, java.sql.SQLTransientConnectionException",
    "422, idempotency_key_reused, HY000, java.sql.SQLNonTransientException",
    "431, headers_too_large, 54000, java.sql.SQLNonTransientException",
    "429, too_many_sessions, 53000, java.sql.SQLTransientException",
    "429, result_quota_exceeded, 53000, java.sql.SQLTransientException",
    "429, rate_limited, 53000, java.sql.SQLTransientException",
    "0, statement_timeout, HYT00, java.sql.SQLTimeoutException",
    "0, statement_cancelled, 57014, java.sql.SQLException",
    "422, execution_error, HY000, java.sql.SQLNonTransientException",
    "0, execution_error, HY000, java.sql.SQLNonTransientException",
    "500, internal_error, HY000, java.sql.SQLException",
    "503, server_draining, 08004, java.sql.SQLTransientConnectionException",
    "501, not_implemented, 0A000, java.sql.SQLFeatureNotSupportedException",
  })
  void everySpecCodeMaps(int status, String code, String sqlState, String type) throws Exception {
    SQLException error = SqlErrors.toSqlException(problem(status, code, null));
    assertEquals(sqlState, error.getSQLState(), code);
    assertEquals(Class.forName(type), error.getClass(), code);
    assertEquals(status, error.getErrorCode());
    assertEquals(code, WarpProblem.codeOf(error));
  }

  @Test
  void theServersSqlStateWinsAndPicksTheSubclass() {
    assertInstanceOf(
        SQLIntegrityConstraintViolationException.class,
        SqlErrors.toSqlException(problem(422, "execution_error", "23505")));
    assertInstanceOf(
        SQLDataException.class, SqlErrors.toSqlException(problem(422, "execution_error", "22012")));
    assertInstanceOf(
        SQLTransactionRollbackException.class,
        SqlErrors.toSqlException(problem(422, "execution_error", "40001")));
    SQLException syntax = SqlErrors.toSqlException(problem(400, "sql_error", "42601"));
    assertInstanceOf(SQLSyntaxErrorException.class, syntax);
    assertEquals("42601", syntax.getSQLState());
    assertEquals(
        "HY000",
        SqlErrors.toSqlException(problem(422, "execution_error", "bogus")).getSQLState(),
        "a malformed SQLSTATE is ignored");
  }

  @Test
  void unknownCodesMapByStatus() {
    assertInstanceOf(
        SQLInvalidAuthorizationSpecException.class,
        SqlErrors.toSqlException(problem(401, "future_auth_code", null)));
    assertInstanceOf(
        SQLSyntaxErrorException.class, SqlErrors.toSqlException(problem(403, "future", null)));
    assertInstanceOf(
        SQLTransientException.class, SqlErrors.toSqlException(problem(429, "future", null)));
    assertInstanceOf(
        SQLTransientConnectionException.class,
        SqlErrors.toSqlException(problem(503, "future", null)));
    assertInstanceOf(
        SQLFeatureNotSupportedException.class,
        SqlErrors.toSqlException(problem(501, "future", null)));
    assertInstanceOf(
        SQLNonTransientException.class, SqlErrors.toSqlException(problem(400, "future", null)));
    SQLException server = SqlErrors.toSqlException(problem(502, "client_http_error", null));
    assertEquals(SQLException.class, server.getClass());
    assertEquals("HY000", server.getSQLState());
  }

  @Test
  void messagesNameTheCodeAndRequest() {
    SQLException error =
        SqlErrors.toSqlException(
            new WarpProblem(400, "sql_error", "t", "no such table", null, "r_9", null));
    assertEquals("no such table [sql_error, request r_9]", error.getMessage());
    SQLException ended =
        SqlErrors.toSqlException(
            new WarpProblem(401, "session_ended", "Ended", null, null, null, "killed"));
    assertEquals("Ended [session_ended: killed]", ended.getMessage());
    assertSame(WarpProblem.of(ended), WarpProblem.of(new SQLException("wrapper", ended)));
    assertNull(WarpProblem.of(new SQLException("not from the server")));
    assertNull(WarpProblem.codeOf(new RuntimeException()));
  }

  @Test
  void problemDocumentsParse() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    WarpProblem full =
        Problems.parse(
            mapper.readTree(
                "{\"type\":\"urn:warp-sql:error:permission_denied\",\"title\":\"Permission denied\","
                    + "\"status\":403,\"code\":\"permission_denied\",\"detail\":\"SELECT on t\","
                    + "\"sqlstate\":\"42501\",\"request_id\":\"r_5\",\"extra\":1}"),
            400);
    assertEquals(403, full.status());
    assertEquals("permission_denied", full.code());
    assertEquals("42501", full.sqlState());
    assertEquals("r_5", full.requestId());
    WarpProblem typeOnly =
        Problems.parse(mapper.readTree("{\"type\":\"urn:warp-sql:error:read_only\"}"), 403);
    assertEquals("read_only", typeOnly.code(), "the code can come from the type URN");
    assertEquals(403, typeOnly.status(), "and the status from the response");
    assertEquals("client_http_error", Problems.parse(mapper.readTree("{}"), 418).code());
    WarpProblem page = Problems.fromResponse(502, "<html>Bad Gateway</html>", "r_x");
    assertEquals("client_http_error", page.code());
    WarpProblem cancelled =
        Problems.parse(
            mapper.readTree(
                "{\"code\":\"statement_cancelled\",\"status\":409,\"cancel_reason\":\"killed\"}"),
            409);
    assertEquals("killed", cancelled.cancelReason());
    WarpProblem revoked =
        Problems.parse(
            mapper.readTree("{\"code\":\"invalid_token\",\"reason\":\"token_revoked\"}"), 401);
    assertEquals("token_revoked", revoked.reason());
    assertEquals("unexpected HTTP 502: <html>Bad Gateway</html>", page.detail());
  }

  @Test
  void clientMadeCodesBeginWithClient() {
    SQLException protocol = SqlErrors.protocolError("no Warp-Result-Order");
    assertInstanceOf(SQLNonTransientConnectionException.class, protocol);
    assertEquals("08P01", protocol.getSQLState());
    assertEquals("client_protocol_error", WarpProblem.codeOf(protocol));
    assertEquals("no Warp-Result-Order [client_protocol_error]", protocol.getMessage());
    assertTrue(WarpProblem.CLIENT_HTTP_ERROR.startsWith("client_"));
  }

  @Test
  void tlsAlertsAreTheRefusalsTheyStandFor() {
    assertEquals(
        "certificate_required",
        WarpClient.tlsAlert(
            new SSLHandshakeException("Received fatal alert: certificate_required")));
    assertEquals(
        "certificate_revoked",
        WarpClient.tlsAlert(
            new java.io.IOException(
                "wrapped", new SSLException("Received fatal alert: certificate_revoked"))));
    assertNull(WarpClient.tlsAlert(new SSLHandshakeException("PKIX path building failed")));
    assertNull(WarpClient.tlsAlert(new java.io.IOException("certificate_required")));
  }

  @Test
  void connectionErrorsAreNotProblems() {
    assertInstanceOf(
        SQLNonTransientConnectionException.class,
        SqlErrors.toSqlException(problem(404, "session_not_found", null)));
    assertInstanceOf(
        SQLTimeoutException.class,
        SqlErrors.toSqlException(problem(0, "statement_timeout", "57014")),
        "a timeout stays a timeout whatever SQLSTATE the server gives");
  }
}
