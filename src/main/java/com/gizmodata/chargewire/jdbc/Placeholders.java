package com.gizmodata.chargewire.jdbc;

/**
 * Counts a statement's parameters when the server cannot say: {@code ?} markers, or the highest
 * {@code $n}, outside quotes and comments.
 */
final class Placeholders {
  private Placeholders() {}

  static int count(String sql) {
    int questionMarks = 0;
    int highestNumbered = 0;
    int length = sql.length();
    for (int i = 0; i < length; i++) {
      char c = sql.charAt(i);
      if (c == '\'' || c == '"') {
        i = skipQuoted(sql, i, c);
      } else if (c == '-' && i + 1 < length && sql.charAt(i + 1) == '-') {
        int end = sql.indexOf('\n', i);
        i = end < 0 ? length : end;
      } else if (c == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
        int end = sql.indexOf("*/", i + 2);
        i = end < 0 ? length : end + 1;
      } else if (c == '?') {
        questionMarks++;
      } else if (c == '$' && i + 1 < length && Character.isDigit(sql.charAt(i + 1))) {
        int end = i + 1;
        while (end < length && Character.isDigit(sql.charAt(end))) {
          end++;
        }
        highestNumbered = Math.max(highestNumbered, Integer.parseInt(sql.substring(i + 1, end)));
        i = end - 1;
      }
    }
    return Math.max(questionMarks, highestNumbered);
  }

  /** Returns the index of the closing quote; a doubled quote is part of the text. */
  private static int skipQuoted(String sql, int start, char quote) {
    for (int i = start + 1; i < sql.length(); i++) {
      if (sql.charAt(i) == quote) {
        if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) {
          i++;
        } else {
          return i;
        }
      }
    }
    return sql.length();
  }
}
