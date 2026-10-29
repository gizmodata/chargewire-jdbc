package com.gizmodata.chargewire.jdbc;

import com.gizmodata.chargewire.jdbc.client.Dialect;

/**
 * The protocol as servers from before its rename to Chargewire speak it: mounted at {@code /warp},
 * announced as {@code warp-sql}, with {@code Warp-*} headers, {@code urn:warp-sql:error:} problem
 * types and the {@code WARP:SQL:COLUMN_DEFAULT} metadata key. A connection at the default root
 * whose discovery answers 404 tries this dialect, so the driver also works with servers that have
 * not been upgraded. Silently, like {@link LegacyUrls}; the two are the only places the former
 * names appear.
 */
final class LegacyDialect {
  static final Dialect DIALECT =
      new Dialect("warp-sql", "/warp", "Warp-", "urn:warp-sql:error:", "WARP:SQL:COLUMN_DEFAULT");

  private LegacyDialect() {}

  /**
   * Returns the dialect to fall back to for a connection mounted at {@code basePath}: this one at
   * today's default root, none at a custom base path (a proxy's own path is never second-guessed).
   *
   * @param basePath the base path, after {@link LegacyUrls#basePath(String)}
   * @return the fallback, or {@code null}
   */
  static Dialect fallbackFor(String basePath) {
    String path = basePath.startsWith("/") ? basePath : "/" + basePath;
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path.equals(ChargewireProperty.BASE_PATH.defaultValue()) ? DIALECT : null;
  }
}
