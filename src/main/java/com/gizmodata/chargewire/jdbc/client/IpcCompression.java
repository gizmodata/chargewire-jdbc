package com.gizmodata.chargewire.jdbc.client;

import java.time.Duration;
import java.util.Locale;
import java.util.Set;

/**
 * The Arrow buffer compression a connection asks results in (spec §8.2), from its {@code
 * ipcCompression} setting and what the server offers. The same rules as the Chargewire ADBC driver:
 * a codec the server does not list is never used, and {@code auto} picks by the round trip of the
 * discovery request.
 *
 * <p>Internal API.
 */
public final class IpcCompression {
  /** Pick by the link: {@link #LZ4_FRAME} on a near link, {@link #ZSTD} on a far one. */
  public static final String AUTO = "auto";

  /** Uncompressed Arrow. */
  public static final String NONE = "none";

  /** Arrow's {@code LZ4_FRAME}: cheap to decode, compresses less. */
  public static final String LZ4_FRAME = "lz4_frame";

  /** Arrow's {@code ZSTD}: compresses more, for links where bandwidth is scarcer than CPU. */
  public static final String ZSTD = "zstd";

  /** Every value of the setting. */
  public static final String[] SETTINGS = {AUTO, LZ4_FRAME, ZSTD, NONE};

  /**
   * The round trip from which {@code auto} chooses {@code zstd}: a link that slow is a WAN, where
   * bandwidth, not CPU, limits a transfer.
   */
  public static final Duration FAR_LINK = Duration.ofMillis(10);

  private IpcCompression() {}

  /**
   * Reads a setting.
   *
   * @param value {@code auto}, {@code lz4_frame}, {@code zstd} or {@code none}, in any case
   * @return the setting in lower case, or {@code null} if it is none of them
   */
  public static String parse(String value) {
    String setting = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    for (String known : SETTINGS) {
      if (known.equals(setting)) {
        return known;
      }
    }
    return null;
  }

  /**
   * The codec {@code auto} uses on a link with this round trip.
   *
   * @param roundTrip the measured round trip
   * @return {@link #LZ4_FRAME} or {@link #ZSTD}
   */
  public static String autoCodec(Duration roundTrip) {
    return roundTrip.compareTo(FAR_LINK) < 0 ? LZ4_FRAME : ZSTD;
  }

  /**
   * The codec to ask results in.
   *
   * @param setting a parsed setting
   * @param capabilities the server's capabilities
   * @param roundTrip the discovery round trip; used only for {@code auto}
   * @return {@link #LZ4_FRAME}, {@link #ZSTD}, or {@code null} for uncompressed results
   */
  public static String codec(String setting, Set<String> capabilities, Duration roundTrip) {
    String codec = AUTO.equals(setting) ? autoCodec(roundTrip) : setting;
    if (LZ4_FRAME.equals(codec) && capabilities.contains("ipc_compression_lz4_frame")) {
      return LZ4_FRAME;
    }
    if (ZSTD.equals(codec) && capabilities.contains("ipc_compression_zstd")) {
      return ZSTD;
    }
    return null;
  }
}
