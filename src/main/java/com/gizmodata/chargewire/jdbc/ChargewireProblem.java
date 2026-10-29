package com.gizmodata.chargewire.jdbc;

import java.sql.SQLException;
import java.util.Objects;

/**
 * An error the server reported: an RFC 9457 problem document (Chargewire spec §11).
 *
 * <p>Every {@link SQLException} the driver raises for a server error carries one: its cause is a
 * {@link ChargewireProblemException}, and {@link #of(Throwable)} finds it. {@link #code()} is the
 * stable, machine-readable part; {@link #detail()} is for people. Decide on the code, never on the
 * title or detail. New codes and reasons can appear in later versions of the protocol (spec §12):
 * handle an unknown code by its {@link #status()}.
 *
 * <p>A few failures the driver finds itself, with no problem document from the server, carry a
 * problem too; their codes begin with {@code client_} (spec §11), which servers never send: {@link
 * #CLIENT_HTTP_ERROR} and {@link #CLIENT_PROTOCOL_ERROR}.
 */
public final class ChargewireProblem {
  /**
   * The code of an error response that was not a problem document (a proxy's error page, say), or a
   * problem document without a code.
   */
  public static final String CLIENT_HTTP_ERROR = "client_http_error";

  /** The code of a response that breaks the protocol, such as a split result without its order. */
  public static final String CLIENT_PROTOCOL_ERROR = "client_protocol_error";

  private final int status;
  private final String code;
  private final String title;
  private final String detail;
  private final String sqlState;
  private final String requestId;
  private final String reason;
  private final String cancelReason;

  /**
   * Creates a problem without a {@code cancel_reason}.
   *
   * @param status HTTP status, or 0 for an error inside a statement document
   * @param code the stable error code, for example {@code sql_error}
   * @param title short summary, or {@code null}
   * @param detail explanation for people, or {@code null}
   * @param sqlState five-character SQLSTATE the server gave, or {@code null}
   * @param requestId the server's request id, or {@code null}
   * @param reason the {@code reason} of an {@code invalid_token} or {@code session_ended} error, or
   *     {@code null}
   */
  public ChargewireProblem(
      int status,
      String code,
      String title,
      String detail,
      String sqlState,
      String requestId,
      String reason) {
    this(status, code, title, detail, sqlState, requestId, reason, null);
  }

  /**
   * Creates a problem.
   *
   * @param status HTTP status, or 0 for an error inside a statement document
   * @param code the stable error code, for example {@code sql_error}
   * @param title short summary, or {@code null}
   * @param detail explanation for people, or {@code null}
   * @param sqlState five-character SQLSTATE the server gave, or {@code null}
   * @param requestId the server's request id, or {@code null}
   * @param reason the {@code reason} of an {@code invalid_token} or {@code session_ended} error, or
   *     {@code null}
   * @param cancelReason the {@code cancel_reason} of a {@code statement_cancelled} or {@code
   *     statement_timeout} error, or {@code null}
   */
  public ChargewireProblem(
      int status,
      String code,
      String title,
      String detail,
      String sqlState,
      String requestId,
      String reason,
      String cancelReason) {
    this.status = status;
    this.code = Objects.requireNonNull(code, "code");
    this.title = title;
    this.detail = detail;
    this.sqlState = sqlState;
    this.requestId = requestId;
    this.reason = reason;
    this.cancelReason = cancelReason;
  }

  /**
   * Finds the problem behind an exception.
   *
   * @param error an exception the driver raised
   * @return the problem, or {@code null} when the error did not come from the server (a broken
   *     connection, for example) and has no {@code client_} code
   */
  public static ChargewireProblem of(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause()) {
      if (t instanceof ChargewireProblemException) {
        return ((ChargewireProblemException) t).getProblem();
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
    ChargewireProblem problem = of(error);
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
   * @return for example {@code sql_error} or {@code statement_timeout}, or a {@code client_} code
   *     for a failure the driver found itself
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
   * Returns why the server refused a token ({@code invalid_token}, spec §4.2) or ended a session
   * ({@code session_ended}, spec §5.6). Later versions may add reasons: treat one you don't know as
   * a generic one.
   *
   * @return for example {@code token_revoked} or {@code killed}, or {@code null}
   */
  public String reason() {
    return reason;
  }

  /**
   * Returns why a statement was cancelled ({@code statement_cancelled} and {@code
   * statement_timeout}, spec §6.2).
   *
   * @return for example {@code client}, {@code timeout} or {@code killed}, or {@code null}
   */
  public String cancelReason() {
    return cancelReason;
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
    if (cancelReason != null) {
      message.append(", cancelled: ").append(cancelReason);
    }
    if (requestId != null) {
      message.append(", request ").append(requestId);
    }
    return message.append(']').toString();
  }

  @Override
  public String toString() {
    return "ChargewireProblem{status=" + status + ", " + message() + "}";
  }
}
