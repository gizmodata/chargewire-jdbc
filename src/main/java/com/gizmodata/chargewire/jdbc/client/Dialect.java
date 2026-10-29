package com.gizmodata.chargewire.jdbc.client;

/**
 * The protocol's spellings: the name its discovery document announces, the path it is mounted at,
 * its header prefix, its problem-type prefix and its column-default metadata key. A connection
 * speaks {@link #CURRENT} unless discovery found a server that speaks an earlier spelling.
 *
 * <p>Internal API.
 */
public final class Dialect {
  /** Today's spellings. */
  public static final Dialect CURRENT =
      new Dialect(
          "chargewire",
          "/chargewire",
          "Chargewire-",
          "urn:chargewire:error:",
          "CHARGEWIRE:SQL:COLUMN_DEFAULT");

  private final String protocol;
  private final String root;
  private final String headerPrefix;
  private final String problemTypePrefix;
  private final String columnDefaultKey;

  /**
   * Creates a dialect.
   *
   * @param protocol the discovery document's {@code protocol}
   * @param root the path the protocol is mounted at by default, for example {@code /chargewire}
   * @param headerPrefix the prefix of the protocol's own headers, for example {@code Chargewire-}
   * @param problemTypePrefix the prefix of problem types, before the code
   * @param columnDefaultKey the field metadata key of a column's default
   */
  public Dialect(
      String protocol,
      String root,
      String headerPrefix,
      String problemTypePrefix,
      String columnDefaultKey) {
    this.protocol = protocol;
    this.root = root;
    this.headerPrefix = headerPrefix;
    this.problemTypePrefix = problemTypePrefix;
    this.columnDefaultKey = columnDefaultKey;
  }

  String protocol() {
    return protocol;
  }

  String root() {
    return root;
  }

  /** The protocol header named {@code name} after its prefix, for example {@code Request-Id}. */
  String header(String name) {
    return headerPrefix + name;
  }

  String problemTypePrefix() {
    return problemTypePrefix;
  }

  /**
   * Returns the field metadata key of a column's default (SQL text).
   *
   * @return the key
   */
  public String columnDefaultKey() {
    return columnDefaultKey;
  }
}
