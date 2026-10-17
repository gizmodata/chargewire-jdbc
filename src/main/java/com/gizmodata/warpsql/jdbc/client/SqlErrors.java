package com.gizmodata.warpsql.jdbc.client;

import com.gizmodata.warpsql.jdbc.WarpProblem;
import com.gizmodata.warpsql.jdbc.WarpProblemException;
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

/**
 * Turns Warp SQL problem documents into {@link SQLException}s of the right subclass.
 *
 * <p>The SQLSTATE is the server's when it sent one; otherwise it follows from the problem's {@code
 * code}, and for a code this driver does not know, from the HTTP status (spec §12). The subclass
 * follows from the SQLSTATE's class, as {@link SQLException}'s subclasses are defined.
 */
public final class SqlErrors {
  private SqlErrors() {}

  /**
   * Builds the exception for a problem.
   *
   * @param problem the server's error
   * @return an exception whose cause is a {@link WarpProblemException}
   */
  public static SQLException toSqlException(WarpProblem problem) {
    String sqlState =
        problem.sqlState() != null && problem.sqlState().length() == 5
            ? problem.sqlState()
            : defaultSqlState(problem);
    SQLException error = create(problem, problem.message(), sqlState);
    error.initCause(new WarpProblemException(problem));
    return error;
  }

  /**
   * Returns the SQLSTATE for a problem the server sent without one.
   *
   * @param problem the server's error
   * @return a five-character SQLSTATE
   */
  static String defaultSqlState(WarpProblem problem) {
    switch (problem.code()) {
      case "invalid_credentials":
      case "invalid_token":
        return "28000";
      case "session_ended":
      case "session_not_found":
        return "08003";
      case "session_elsewhere":
        return "08001";
      case "server_draining":
        return "08004";
      case "permission_denied":
        return "42501";
      case "read_only":
        return "25006";
      case "sql_error":
        return "42000";
      case "statement_timeout":
        return "HYT00";
      case "statement_cancelled":
        return "57014";
      case "unknown_option":
        return "HY092";
      case "invalid_option_value":
        return "HY024";
      case "statement_not_found":
      case "prepared_not_found":
        return "26000";
      case "too_many_sessions":
      case "result_quota_exceeded":
      case "rate_limited":
        return "53000";
      case "payload_too_large":
      case "headers_too_large":
        return "54000";
      case "unsupported_arrow_type":
      case "not_implemented":
      case "not_found":
      case "method_not_allowed":
        return "0A000";
      default:
        return defaultSqlStateForStatus(problem.status());
    }
  }

  private static String defaultSqlStateForStatus(int status) {
    if (status == 401) {
      return "28000";
    }
    if (status == 403) {
      return "42501";
    }
    if (status == 429) {
      return "53000";
    }
    if (status == 501) {
      return "0A000";
    }
    if (status == 503) {
      return "08004";
    }
    return "HY000";
  }

  private static SQLException create(WarpProblem problem, String message, String sqlState) {
    int vendorCode = problem.status();
    if ("statement_timeout".equals(problem.code())) {
      return new SQLTimeoutException(message, sqlState, vendorCode);
    }
    if (problem.status() == 429 || "server_draining".equals(problem.code())) {
      return sqlState.startsWith("08")
          ? new SQLTransientConnectionException(message, sqlState, vendorCode)
          : new SQLTransientException(message, sqlState, vendorCode);
    }
    switch (sqlState.substring(0, 2)) {
      case "08":
        return "session_elsewhere".equals(problem.code())
            ? new SQLTransientConnectionException(message, sqlState, vendorCode)
            : new SQLNonTransientConnectionException(message, sqlState, vendorCode);
      case "0A":
        return new SQLFeatureNotSupportedException(message, sqlState, vendorCode);
      case "22":
        return new SQLDataException(message, sqlState, vendorCode);
      case "23":
        return new SQLIntegrityConstraintViolationException(message, sqlState, vendorCode);
      case "28":
        return new SQLInvalidAuthorizationSpecException(message, sqlState, vendorCode);
      case "40":
        return new SQLTransactionRollbackException(message, sqlState, vendorCode);
      case "42":
        return new SQLSyntaxErrorException(message, sqlState, vendorCode);
      case "HY":
        return problem.status() >= 500
            ? new SQLException(message, sqlState, vendorCode)
            : new SQLNonTransientException(message, sqlState, vendorCode);
      default:
        return problem.status() >= 400 && problem.status() < 500
            ? new SQLNonTransientException(message, sqlState, vendorCode)
            : new SQLException(message, sqlState, vendorCode);
    }
  }
}
