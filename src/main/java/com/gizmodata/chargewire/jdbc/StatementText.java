package com.gizmodata.chargewire.jdbc;

import java.util.Locale;

/**
 * What a statement's SQL text may do to its session, judged from the text alone: to the transaction
 * (open one or change how one opens, end one, both, or neither), and to where or how later
 * statements run ({@code USE}, {@code SET}, {@code RESET}, {@code PRAGMA}).
 *
 * <p>The driver needs this because a session document read right after a statement may not show
 * what the statement did yet: Chargewire servers up to 2.2.0 (every Hardcharger through 2.7.0)
 * answer a statement without rows on the streamed path as soon as its outcome is known, and copy
 * the session's transaction state into the session document slightly after that. So a read right
 * after {@code BEGIN} can still say {@code none}, and one right after {@code COMMIT} can still say
 * {@code open}. {@link ChargewireConnection} therefore does not let a {@code none} clear what a
 * statement that {@link Effect#mayOpen() may open} a transaction did (see its {@code
 * statementRan}); a stale read after a statement that may end one can only lag toward {@code open},
 * which errs on the safe side.
 *
 * <p>A lost session's statement must also not run again on a new session that differs from the old
 * one in what SQL changed: {@code USE} (the catalog and schema, which the driver restores only from
 * a session read it knows is fresh) and {@code SET}, {@code RESET} and {@code PRAGMA} (settings,
 * which the driver does not restore). See {@link Reading}.
 *
 * <p>Each statement of the text (split at semicolons outside quotes and comments) counts by its
 * first word, after any {@code EXPLAIN} or {@code ANALYZE}: {@code BEGIN}, {@code START} and {@code
 * EXECUTE} (a prepared statement may be any of these) may open a transaction; {@code COMMIT},
 * {@code ROLLBACK}, {@code END} and {@code ABORT} may end one; {@code USE} changes the catalog or
 * schema; {@code SET}, {@code RESET} and {@code PRAGMA} change settings ({@code UPDATE … SET} does
 * not count: only first words do). The word {@code autocommit} anywhere in the text, quoted or not,
 * counts as may-open. Quotes ({@code '…'}, {@code E'…'}, {@code "…"}, {@code $tag$…$tag$}) and
 * comments ({@code --}, and block comments, which may nest) are skipped. When in doubt the answer
 * leans toward may-open, the safe side.
 */
final class StatementText {
  private StatementText() {}

  /** What a statement's text may do to the session's transaction. */
  enum Effect {
    /** Nothing in the text names a transaction. */
    NEITHER(false, false),
    /** The text may open a transaction, or change how one opens. */
    MAY_OPEN(true, false),
    /** The text may end a transaction, and nothing in it may open one. */
    MAY_END(false, true),
    /** The text may both open and end one: it counts as may-open. */
    BOTH(true, true);

    private final boolean mayOpen;
    private final boolean mayEnd;

    Effect(boolean mayOpen, boolean mayEnd) {
      this.mayOpen = mayOpen;
      this.mayEnd = mayEnd;
    }

    /** Whether the text may open a transaction or change how one opens. */
    boolean mayOpen() {
      return mayOpen;
    }

    /** Whether the text may end a transaction. */
    boolean mayEnd() {
      return mayEnd;
    }

    static Effect of(boolean mayOpen, boolean mayEnd) {
      if (mayOpen) {
        return mayEnd ? BOTH : MAY_OPEN;
      }
      return mayEnd ? MAY_END : NEITHER;
    }
  }

  /** What a statement's text may do to its session. */
  static final class Reading {
    static final Reading NOTHING = new Reading(Effect.NEITHER, false, false);

    private final Effect transaction;
    private final boolean changesPlace;
    private final boolean changesSettings;

    Reading(Effect transaction, boolean changesPlace, boolean changesSettings) {
      this.transaction = transaction;
      this.changesPlace = changesPlace;
      this.changesSettings = changesSettings;
    }

    /** What the text may do to the session's transaction. */
    Effect transaction() {
      return transaction;
    }

    /**
     * Whether a statement of the text starts with {@code USE}: it changes the catalog or schema,
     * which the driver restores in a new session only from a session read it knows is fresh.
     */
    boolean changesPlace() {
      return changesPlace;
    }

    /**
     * Whether a statement of the text starts with {@code SET}, {@code RESET} or {@code PRAGMA}: it
     * may change settings, which the driver does not restore in a new session.
     */
    boolean changesSettings() {
      return changesSettings;
    }
  }

  /**
   * Reads what a statement's text may do to the session's transaction.
   *
   * @param sql the SQL text, one statement or several
   * @return what it may do to the session's transaction
   */
  static Effect of(String sql) {
    return read(sql).transaction();
  }

  /**
   * Reads a statement's text.
   *
   * @param sql the SQL text, one statement or several
   * @return what it may do to its session
   */
  static Reading read(String sql) {
    if (sql == null) {
      return Reading.NOTHING;
    }
    boolean mayOpen = sql.toLowerCase(Locale.ROOT).contains("autocommit");
    boolean mayEnd = false;
    boolean changesPlace = false;
    boolean changesSettings = false;
    // Whether the current statement's first word (after EXPLAIN or ANALYZE) has been read.
    boolean statementStarted = false;
    int length = sql.length();
    int i = 0;
    while (i < length) {
      char c = sql.charAt(i);
      if (c == ';') {
        statementStarted = false;
        i++;
      } else if (c == '\'') {
        i = skipQuoted(sql, i, '\'', false);
        statementStarted = true;
      } else if (c == '"') {
        i = skipQuoted(sql, i, '"', false);
        statementStarted = true;
      } else if (c == '-' && i + 1 < length && sql.charAt(i + 1) == '-') {
        int end = sql.indexOf('\n', i);
        i = end < 0 ? length : end + 1;
      } else if (c == '/' && i + 1 < length && sql.charAt(i + 1) == '*') {
        i = skipBlockComment(sql, i);
      } else if (c == '$') {
        int end = skipDollarQuoted(sql, i);
        statementStarted = statementStarted || end > i + 1;
        i = end;
      } else if (Character.isLetter(c) || c == '_') {
        int end = i + 1;
        while (end < length && isWordPart(sql.charAt(end))) {
          end++;
        }
        if ((c == 'e' || c == 'E') && end == i + 1 && end < length && sql.charAt(end) == '\'') {
          // E'…': backslash escapes.
          i = skipQuoted(sql, end, '\'', true);
          statementStarted = true;
          continue;
        }
        if (!statementStarted) {
          String word = sql.substring(i, end).toUpperCase(Locale.ROOT);
          switch (word) {
            case "EXPLAIN":
            case "ANALYZE":
            case "ANALYSE":
              // EXPLAIN ANALYZE runs the statement that follows: read its first word instead.
              break;
            case "BEGIN":
            case "START":
            case "EXECUTE":
              mayOpen = true;
              statementStarted = true;
              break;
            case "COMMIT":
            case "ROLLBACK":
            case "END":
            case "ABORT":
              mayEnd = true;
              statementStarted = true;
              break;
            case "USE":
              changesPlace = true;
              statementStarted = true;
              break;
            case "SET":
            case "RESET":
            case "PRAGMA":
              changesSettings = true;
              statementStarted = true;
              break;
            default:
              statementStarted = true;
          }
        }
        i = end;
      } else {
        if (!Character.isWhitespace(c) && c != '(') {
          statementStarted = true;
        }
        i++;
      }
    }
    return new Reading(Effect.of(mayOpen, mayEnd), changesPlace, changesSettings);
  }

  private static boolean isWordPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }

  /** Returns the index after the closing quote; a doubled quote is part of the text. */
  private static int skipQuoted(String sql, int start, char quote, boolean backslashEscapes) {
    int length = sql.length();
    for (int i = start + 1; i < length; i++) {
      char c = sql.charAt(i);
      if (backslashEscapes && c == '\\') {
        i++;
      } else if (c == quote) {
        if (i + 1 < length && sql.charAt(i + 1) == quote) {
          i++;
        } else {
          return i + 1;
        }
      }
    }
    return length;
  }

  /** Returns the index after a block comment, which may nest. */
  private static int skipBlockComment(String sql, int start) {
    int length = sql.length();
    int depth = 0;
    int i = start;
    while (i < length) {
      if (i + 1 < length && sql.charAt(i) == '/' && sql.charAt(i + 1) == '*') {
        depth++;
        i += 2;
      } else if (i + 1 < length && sql.charAt(i) == '*' && sql.charAt(i + 1) == '/') {
        depth--;
        i += 2;
        if (depth == 0) {
          return i;
        }
      } else {
        i++;
      }
    }
    return length;
  }

  /**
   * Returns the index after a dollar-quoted string ({@code $$…$$}, {@code $tag$…$tag$}), or after
   * the {@code $} alone when it starts none (a {@code $1} parameter).
   */
  private static int skipDollarQuoted(String sql, int start) {
    int length = sql.length();
    int tagEnd = start + 1;
    if (tagEnd < length && Character.isDigit(sql.charAt(tagEnd))) {
      return start + 1;
    }
    while (tagEnd < length
        && (Character.isLetterOrDigit(sql.charAt(tagEnd)) || sql.charAt(tagEnd) == '_')) {
      tagEnd++;
    }
    if (tagEnd >= length || sql.charAt(tagEnd) != '$') {
      return start + 1;
    }
    String tag = sql.substring(start, tagEnd + 1);
    int close = sql.indexOf(tag, tagEnd + 1);
    return close < 0 ? length : close + tag.length();
  }
}
