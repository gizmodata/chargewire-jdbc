package com.gizmodata.warpsql.jdbc.arrow;

import java.io.ByteArrayInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.MessageChannelReader;

/**
 * Measures how fast the driver decodes Arrow IPC streams, compressed or not: not a test, run it by
 * hand with {@code java -cp ... DecodeBenchmark [--threads n] [--commons] stream.arrows ...}.
 * Throughput is the decoded Arrow buffers' bytes per second of one thread.
 */
public final class DecodeBenchmark {
  private DecodeBenchmark() {}

  public static void main(String[] args) throws Exception {
    int threads = 1;
    int rounds = 5;
    boolean commons = false;
    List<Path> files = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--threads":
          threads = Integer.parseInt(args[++i]);
          break;
        case "--rounds":
          rounds = Integer.parseInt(args[++i]);
          break;
        case "--commons":
          commons = true;
          break;
        default:
          files.add(Path.of(args[i]));
      }
    }
    CompressionCodec.Factory factory =
        commons ? CommonsCompressionFactory.INSTANCE : WarpCompressionFactory.INSTANCE;
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try (BufferAllocator allocator = new RootAllocator()) {
      for (Path file : files) {
        byte[] stream = Files.readAllBytes(file);
        for (int round = 0; round < rounds; round++) {
          List<Future<Long>> decoded = new ArrayList<>();
          long start = System.nanoTime();
          for (int t = 0; t < threads; t++) {
            decoded.add(pool.submit(() -> decode(stream, allocator, factory)));
          }
          long bytes = 0;
          for (Future<Long> result : decoded) {
            bytes += result.get();
          }
          double seconds = (System.nanoTime() - start) / 1e9;
          System.out.printf(
              Locale.ROOT,
              "%s%s threads=%d round=%d: %.2f GB decoded in %.3f s, %.2f GB/s per thread%n",
              file.getFileName(),
              commons ? " (commons)" : "",
              threads,
              round,
              bytes / 1e9,
              seconds,
              bytes / 1e9 / seconds / threads);
        }
      }
    } finally {
      pool.shutdown();
    }
  }

  private static long decode(
      byte[] stream, BufferAllocator allocator, CompressionCodec.Factory factory) throws Exception {
    long bytes = 0;
    try (ArrowIpc.HeapReadChannel channel =
            new ArrowIpc.HeapReadChannel(new ByteArrayInputStream(stream));
        ArrowStreamReader reader =
            new ArrowStreamReader(
                new MessageChannelReader(channel, allocator), allocator, factory)) {
      while (reader.loadNextBatch()) {
        for (FieldVector vector : reader.getVectorSchemaRoot().getFieldVectors()) {
          bytes += vector.getBufferSize();
        }
      }
    }
    return bytes;
  }
}
