package com.gizmodata.chargewire.jdbc.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.gizmodata.chargewire.jdbc.ChargewireProblem;

/** Reads problem documents (spec §11). */
final class Problems {
  private Problems() {}

  /**
   * Reads a problem document in today's spelling.
   *
   * @param json the document
   * @param httpStatus the response's status, used when the document has none
   * @return the problem
   */
  static ChargewireProblem parse(JsonNode json, int httpStatus) {
    return parse(json, httpStatus, Dialect.CURRENT.problemTypePrefix());
  }

  /**
   * Reads a problem document whose types start with {@code typePrefix}.
   *
   * @param json the document
   * @param httpStatus the response's status, used when the document has none
   * @param typePrefix the connection's problem-type prefix, before the code
   * @return the problem
   */
  static ChargewireProblem parse(JsonNode json, int httpStatus, String typePrefix) {
    String code = text(json, "code");
    if (code == null) {
      String type = text(json, "type");
      code =
          type != null && type.startsWith(typePrefix)
              ? type.substring(typePrefix.length())
              : ChargewireProblem.CLIENT_HTTP_ERROR;
    }
    int status = json.path("status").asInt(httpStatus);
    return new ChargewireProblem(
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
  static ChargewireProblem fromResponse(int status, String body, String requestId) {
    String snippet = body == null ? "" : body.strip();
    if (snippet.length() > 200) {
      snippet = snippet.substring(0, 200) + "...";
    }
    return new ChargewireProblem(
        status,
        ChargewireProblem.CLIENT_HTTP_ERROR,
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
