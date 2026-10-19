package com.gizmodata.warpsql.jdbc.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.gizmodata.warpsql.jdbc.WarpProblem;

/** Reads problem documents (spec §11). */
final class Problems {
  private static final String TYPE_PREFIX = "urn:warp-sql:error:";

  private Problems() {}

  /**
   * Reads a problem document.
   *
   * @param json the document
   * @param httpStatus the response's status, used when the document has none
   * @return the problem
   */
  static WarpProblem parse(JsonNode json, int httpStatus) {
    String code = text(json, "code");
    if (code == null) {
      String type = text(json, "type");
      code =
          type != null && type.startsWith(TYPE_PREFIX)
              ? type.substring(TYPE_PREFIX.length())
              : WarpProblem.CLIENT_HTTP_ERROR;
    }
    int status = json.path("status").asInt(httpStatus);
    return new WarpProblem(
        status,
        code,
        text(json, "title"),
        text(json, "detail"),
        text(json, "sqlstate"),
        text(json, "request_id"),
        text(json, "reason"),
        text(json, "cancel_reason"));
  }

  /** A problem for a response that was not a problem document, such as a proxy's error page. */
  static WarpProblem fromResponse(int status, String body, String requestId) {
    String snippet = body == null ? "" : body.strip();
    if (snippet.length() > 200) {
      snippet = snippet.substring(0, 200) + "...";
    }
    return new WarpProblem(
        status,
        WarpProblem.CLIENT_HTTP_ERROR,
        "HTTP " + status,
        "unexpected HTTP " + status + (snippet.isEmpty() ? "" : ": " + snippet),
        null,
        requestId,
        null);
  }

  private static String text(JsonNode json, String field) {
    JsonNode value = json.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }
}
