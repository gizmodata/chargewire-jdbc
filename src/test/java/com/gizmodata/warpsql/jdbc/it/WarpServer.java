package com.gizmodata.warpsql.jdbc.it;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A Warp SQL server started for the tests on a free port, with a fresh password, its own empty home
 * directory and an environment of its own.
 *
 * <p>The binary comes from {@code WARP_SQL_JDBC_TEST_SERVER_BINARY}, or else from a sibling {@code
 * warp-sql} checkout ({@code ../warp-sql/cpp/build-main/warp-duckdb}, then {@code
 * .../build/warp-duckdb}). Without one, every test that needs a server fails: these tests gate
 * changes, and a skipped gate looks like a passed one.
 *
 * <p>Any server with warp-duckdb's flags ({@code --host}, {@code --port}, {@code --username},
 * {@code --tls-cert}, {@code --tls-key}) works. For another one, name its password variable, its
 * client-CA flag, its user-name flag and its product name (Hardcharger: {@code
 * HARDCHARGER_ADMIN_PASSWORD}, {@code --mtls-ca-cert}, {@code --admin-username}, {@code
 * Hardcharger}):
 *
 * <pre>
 * WARP_SQL_JDBC_TEST_SERVER_PASSWORD_ENV  (default WARP_DUCKDB_PASSWORD)
 * WARP_SQL_JDBC_TEST_SERVER_MTLS_FLAG     (default --tls-client-ca)
 * WARP_SQL_JDBC_TEST_SERVER_USERNAME_FLAG (default --username)
 * WARP_SQL_JDBC_TEST_SERVER_NAME          (default warp-duckdb)
 * </pre>
 */
final class WarpServer implements AutoCloseable {
  static final String USER = "warp";
  private static final String BINARY_VARIABLE = "WARP_SQL_JDBC_TEST_SERVER_BINARY";

  /** The server's product name, as DatabaseMetaData reports it. */
  static final String NAME = environment("WARP_SQL_JDBC_TEST_SERVER_NAME", "warp-duckdb");

  /** The flag that makes the server require client certificates from a CA file. */
  static final String MTLS_FLAG =
      environment("WARP_SQL_JDBC_TEST_SERVER_MTLS_FLAG", "--tls-client-ca");

  /** The flag that names the server's administrator. */
  private static final String USERNAME_FLAG =
      environment("WARP_SQL_JDBC_TEST_SERVER_USERNAME_FLAG", "--username");

  private static final String PASSWORD_VARIABLE =
      environment("WARP_SQL_JDBC_TEST_SERVER_PASSWORD_ENV", "WARP_DUCKDB_PASSWORD");

  private static String environment(String name, String fallback) {
    String value = System.getenv(name);
    return value == null || value.isEmpty() ? fallback : value;
  }

  final int port;

  /**
   * Contains {@code :} and {@code @}: credentials travel as JSON fields, so nothing may split them.
   */
  final String password;

  final boolean tls;
  private final Process process;
  private final Path home;
  private final Path log;

  private WarpServer(int port, String password, boolean tls, Process process, Path home, Path log) {
    this.port = port;
    this.password = password;
    this.tls = tls;
    this.process = process;
    this.home = home;
    this.log = log;
  }

  static Path binary() {
    String configured = System.getenv(BINARY_VARIABLE);
    if (configured != null && !configured.isEmpty()) {
      Path path = Path.of(configured);
      if (!Files.isExecutable(path)) {
        fail(BINARY_VARIABLE + "=" + configured + " is not an executable file");
      }
      return path;
    }
    Path checkout = Path.of(System.getProperty("user.dir"), "..", "warp-sql", "cpp").normalize();
    for (String build : List.of("build-main", "build")) {
      Path sibling = checkout.resolve(build).resolve("warp-duckdb");
      if (Files.isExecutable(sibling)) {
        return sibling;
      }
    }
    return fail(
        "No Warp SQL server to test against: set "
            + BINARY_VARIABLE
            + " to a server binary (looked in "
            + checkout
            + "/build-main and /build too)");
  }

  /** Starts a plain-HTTP server. */
  static WarpServer start(String... extraArgs) {
    return start(false, extraArgs);
  }

  /** Starts a server; with {@code tls}, the arguments must include the TLS flags. */
  static WarpServer start(boolean tls, String... extraArgs) {
    Path binary = binary();
    int port = freePort();
    byte[] random = new byte[12];
    new SecureRandom().nextBytes(random);
    String password = Base64.getUrlEncoder().withoutPadding().encodeToString(random) + ":p@ss:word";
    try {
      Path home = Files.createTempDirectory("warp-sql-jdbc-server");
      Path log = home.resolve("server.log");
      List<String> command =
          new ArrayList<>(
              List.of(
                  binary.toString(),
                  "--host",
                  "127.0.0.1",
                  "--port",
                  Integer.toString(port),
                  USERNAME_FLAG,
                  USER));
      command.addAll(List.of(extraArgs));
      ProcessBuilder builder =
          new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile());
      // Only what the server needs: servers also read settings and credentials from variables
      // a developer's shell may export (GIZMOSQL_PASSWORD, for one), and DuckDB reads secrets
      // from the home directory.
      Map<String, String> environment = builder.environment();
      String path = environment.get("PATH");
      environment.clear();
      environment.put("PATH", path == null ? "/usr/bin:/bin" : path);
      environment.put("HOME", home.toString());
      environment.put("TMPDIR", home.toString());
      environment.put(PASSWORD_VARIABLE, password);
      Process process = builder.start();
      WarpServer server = new WarpServer(port, password, tls, process, home, log);
      Runtime.getRuntime().addShutdownHook(new Thread(server::close));
      server.awaitUp();
      return server;
    } catch (IOException e) {
      return fail("Cannot start " + binary + ": " + e.getMessage(), e);
    }
  }

  private static int freePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException e) {
      return fail("No free port: " + e.getMessage(), e);
    }
  }

  /** Ready once it accepts connections: TLS servers may demand a client certificate. */
  private void awaitUp() {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    IOException last = null;
    while (System.nanoTime() < deadline) {
      if (!process.isAlive()) {
        fail("The server exited with " + process.exitValue() + ": " + logText());
      }
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("127.0.0.1", port), 1000);
        return;
      } catch (IOException e) {
        last = e;
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        fail("Interrupted while waiting for the server");
      }
    }
    close();
    fail("No Warp SQL server listened on port " + port + " (" + last + "): " + logText());
  }

  String logText() {
    try {
      return Files.readString(log);
    } catch (IOException e) {
      return "(no log: " + e.getMessage() + ")";
    }
  }

  /** {@code jdbc:warp://127.0.0.1:port}, plus any URL options. */
  String url(String options) {
    String base = "jdbc:warp://127.0.0.1:" + port;
    return options == null || options.isEmpty() ? base : base + "?" + options;
  }

  /** The user and password, plus plain HTTP for a server without TLS. */
  Properties properties() {
    Properties properties = new Properties();
    properties.setProperty("user", USER);
    properties.setProperty("password", password);
    if (!tls) {
      properties.setProperty("useEncryption", "false");
    }
    return properties;
  }

  String httpBase() {
    return (tls ? "https" : "http") + "://127.0.0.1:" + port;
  }

  @Override
  public void close() {
    if (process.isAlive()) {
      // The first SIGTERM drains, the second stops at once.
      process.destroy();
      process.destroy();
      try {
        if (!process.waitFor(20, TimeUnit.SECONDS)) {
          process.destroyForcibly();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
    }
  }
}
