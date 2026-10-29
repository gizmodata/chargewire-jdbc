package com.gizmodata.chargewire.jdbc;

import java.util.List;

/**
 * Connection settings written before the protocol was renamed Chargewire, read as today's: URLs
 * starting {@code jdbc:warp:} or {@code jdbc:warpsql:} (any case), and {@code basePath=/warp}, the
 * protocol's former root. The mapping is silent, so existing configurations keep working unchanged.
 * This class and {@link LegacyDialect} are the only places the former names appear.
 */
final class LegacyUrls {
  private static final List<String> PREFIXES = List.of("jdbc:warp:", "jdbc:warpsql:");
  private static final String BASE_PATH = "/warp";

  private LegacyUrls() {}

  /**
   * Returns {@code url} with a former prefix replaced by {@link ChargewireUrl#PREFIX}, or {@code
   * url} itself (also {@code null}) when it has none.
   */
  static String modernize(String url) {
    if (url != null) {
      for (String prefix : PREFIXES) {
        if (url.regionMatches(true, 0, prefix, 0, prefix.length())) {
          return ChargewireUrl.PREFIX + url.substring(prefix.length());
        }
      }
    }
    return url;
  }

  /**
   * Returns today's root for the former one ({@code /warp}, {@code /warp/} or {@code warp}), and
   * any other base path unchanged. A server answers the former root in the former spelling, which
   * this driver doesn't read; it answers today's root at the same address.
   */
  static String basePath(String basePath) {
    if (basePath == null) {
      return null;
    }
    String path = basePath.startsWith("/") ? basePath : "/" + basePath;
    if (path.equals(BASE_PATH) || path.equals(BASE_PATH + "/")) {
      return ChargewireProperty.BASE_PATH.defaultValue();
    }
    return basePath;
  }
}
