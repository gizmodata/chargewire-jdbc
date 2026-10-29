package com.gizmodata.chargewire.jdbc.arrow;

import org.apache.arrow.compression.ZstdCompressionCodec;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * The Arrow buffer codecs results may use (spec §8.2): {@code LZ4_FRAME} through {@link
 * Lz4FrameCodec}, {@code ZSTD} through Arrow's zstd-jni codec.
 */
final class ChargewireCompressionFactory implements CompressionCodec.Factory {
  static final ChargewireCompressionFactory INSTANCE = new ChargewireCompressionFactory();

  private ChargewireCompressionFactory() {}

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
    switch (codecType) {
      case LZ4_FRAME:
        return Lz4FrameCodec.INSTANCE;
      case ZSTD:
        return new ZstdCompressionCodec();
      default:
        throw new IllegalArgumentException("Compression type not supported: " + codecType);
    }
  }

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
    return createCodec(codecType);
  }
}
