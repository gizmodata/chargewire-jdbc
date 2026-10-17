package com.gizmodata.warpsql.jdbc;

import java.sql.SQLException;
import java.util.Objects;

/**
 * An error the server reported: an RFC 9457 problem document (Warp SQL spec §11).
 *
 * <p>Every {@link SQLException} the driver raises for a server error carries one: its cause is a
 * {@link WarpProblemException}, and {@link #of(Throwable)} finds it. {@link #code()} is the stable,
 * machine-readable part; {@link #detail()} is for people.
 */
public final class WarpProblem {
  private final int status;
  private final String code;
  private final String title;
  private final String detail;
  private final String sqlState;
  private final String requestId;
  private final String reason;

  /**
   * Creates a problem.
   *
   * @param status HTTP status, or 0 for an error inside a statement document
   * @param code the stable error code, for example {@code sql_error}
   * @param title short summary, or {@code null}
   * @param detail explanation for people, or {@code null}
   * @param sqlState five-character SQLSTATE the server gave, or {@code null}
   * @param requestId the server's request id, or {@code null}
   * @param reason the {@code reason} of a {@code session_ended} error, or {@code null}
   */
  public WarpProblem(
      int status,
      String code,
      String title,
      String detail,
      String sqlState,
      String requestId,
      String reason) {
    this.status = status;
    this.code = Objects.requireNonNull(code, "code");
    this.title = title;
    this.detail = detail;
    this.sqlState = sqlState;
    this.requestId = requestId;
    this.reason = reason;
  }

  /**
   * Finds the problem behind an exception.
   *
   * @param error an exception the driver raised
   * @return the problem, or {@code null} when the error did not come from the server
   */
  public static WarpProblem of(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof WarpProblemException) {
        return ((WarpProblemException) t).getProblem();
      }
      if (t.getCause() == t) {
        break;
      }
    }
    return null;
  }

  /**
   * Returns the problem's code, or {@code null}, for an exception.
   *
   * @param error an exception the driver raised
   * @return the code, for example {@code session_not_found}
   */
  public static String codeOf(Throwable error) {
    WarpProblem problem = of(error);
    return problem == null ? null : problem.code();
  }

  /**
   * Returns the HTTP status.
   *
   * @return the status, or 0 for an error inside a statement document
   */
  public int status() {
    return status;
  }

  /**
   * Returns the stable error code.
   *
   * @return for example {@code sql_error} or {@code statement_timeout}
   */
  public String code() {
    return code;
  }

  /**
   * Returns the short summary.
   *
   * @return the title, or {@code null}
   */
  public String title() {
    return title;
  }

  /**
   * Returns the explanation for people.
   *
   * @return the detail, or {@code null}
   */
  public String detail() {
    return detail;
  }

  /**
   * Returns the SQLSTATE the server gave.
   *
   * @return five characters, or {@code null}
   */
  public String sqlState() {
    return sqlState;
  }

  /**
   * Returns the id the server logged the request under.
   *
   * @return the request id, or {@code null}
   */
  public String requestId() {
    return requestId;
  }

  /**
   * Returns why the server ended a session ({@code session_ended} only).
   *
   * @return for example {@code killed}, or {@code null}
   */
  public String reason() {
    return reason;
  }

  /**
   * Returns the message the driver's exceptions carry.
   *
   * @return the detail (or title, or code), then the code and request id
   */
  public String message() {
    String text = detail != null && !detail.isEmpty() ? detail : title != null ? title : code;
    StringBuilder message = new StringBuilder(text).append(" [").append(code);
    if (reason != null) {
      message.append(": ").append(reason);
    }
    if (requestId != null) {
      message.append(", request ").append(requestId);
    }
    return message.append(']').toString();
  }

  @Override
  public String toString() {
    return "WarpProblem{status=" + status + ", " + message() + "}";
  }
}
