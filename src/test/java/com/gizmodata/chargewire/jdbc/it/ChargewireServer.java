package com.gizmodata.chargewire.jdbc.it;

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
 * A Chargewire server started for the tests on a free port, with a fresh password, its own empty
 * home directory and an environment of its own.
 *
 * <p>The binary comes from {@code CHARGEWIRE_JDBC_TEST_SERVER_BINARY}, or else from a sibling
 * {@code chargewire} checkout ({@code ../chargewire/cpp/build-main/chargewire-duckdb}, then {@code
 * .../build/chargewire-duckdb}). Without one, every test that needs a server fails: these tests
 * gate changes, and a skipped gate looks like a passed one.
 *
 * <p>Any server with chargewire-duckdb's flags ({@code --host}, {@code --port}, {@code --username},
 * {@code --tls-cert}, {@code --tls-key}) works. For another one, name its password variable, its
 * client-CA flag, its user-name flag and its product name (Hardcharger: {@code
 * HARDCHARGER_ADMIN_PASSWORD}, {@code --mtls-ca-cert}, {@code --admin-username}, {@code
 * Hardcharger}):
 *
 * <pre>
 * CHARGEWIRE_JDBC_TEST_SERVER_PASSWORD_ENV  (default CHARGEWIRE_DUCKDB_PASSWORD)
 * CHARGEWIRE_JDBC_TEST_SERVER_MTLS_FLAG     (default --tls-client-ca)
 * CHARGEWIRE_JDBC_TEST_SERVER_USERNAME_FLAG (default --username)
 * CHARGEWIRE_JDBC_TEST_SERVER_NAME          (default chargewire-duckdb)
 * </pre>
 */
final class ChargewireServer implements AutoCloseable {
  static final String USER = "chargewire";
  private static final String BINARY_VARIABLE = "CHARGEWIRE_JDBC_TEST_SERVER_BINARY";

  /** The server's product name, as DatabaseMetaData reports it. */
  static final String NAME = environment("CHARGEWIRE_JDBC_TEST_SERVER_NAME", "chargewire-duckdb");

  /** The flag that makes the server require client certificates from a CA file. */
  static final String MTLS_FLAG =
      environment("CHARGEWIRE_JDBC_TEST_SERVER_MTLS_FLAG", "--tls-client-ca");

  /** The flag that names the server's administrator. */
  private static final String USERNAME_FLAG =
      environment("CHARGEWIRE_JDBC_TEST_SERVER_USERNAME_FLAG", "--username");

  private static final String PASSWORD_VARIABLE =
      environment("CHARGEWIRE_JDBC_TEST_SERVER_PASSWORD_ENV", "CHARGEWIRE_DUCKDB_PASSWORD");

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

  private ChargewireServer(
      int port, String password, boolean tls, Process process, Path home, Path log) {
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
    Path checkout = Path.of(System.getProperty("user.dir"), "..", "chargewire", "cpp").normalize();
    for (String build : List.of("build-main", "build")) {
      Path sibling = checkout.resolve(build).resolve("chargewire-duckdb");
      if (Files.isExecutable(sibling)) {
        return sibling;
      }
    }
    return fail(
        "No Chargewire server to test against: set "
            + BINARY_VARIABLE
            + " to a server binary (looked in "
            + checkout
            + "/build-main and /build too)");
  }

  /** Starts a plain-HTTP server. */
  static ChargewireServer start(String... extraArgs) {
    return start(false, extraArgs);
  }

  /** Starts a server; with {@code tls}, the arguments must include the TLS flags. */
  static ChargewireServer start(boolean tls, String... extraArgs) {
    return start(Map.of(), tls, extraArgs);
  }

  /**
   * Starts a server with more environment variables; {@code {port}} in an argument becomes the
   * server's port.
   */
  static ChargewireServer start(
      Map<String, String> extraEnvironment, boolean tls, String... extraArgs) {
    Path binary = binary();
    int port = freePort();
    byte[] random = new byte[12];
    new SecureRandom().nextBytes(random);
    String password = Base64.getUrlEncoder().withoutPadding().encodeToString(random) + ":p@ss:word";
    try {
      Path home = Files.createTempDirectory("chargewire-jdbc-server");
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
      for (String arg : extraArgs) {
        command.add(arg.replace("{port}", Integer.toString(port)));
      }
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
      environment.putAll(extraEnvironment);
      environment.put(PASSWORD_VARIABLE, password);
      Process process = builder.start();
      ChargewireServer server = new ChargewireServer(port, password, tls, process, home, log);
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
    fail("No Chargewire server listened on port " + port + " (" + last + "): " + logText());
  }

  String logText() {
    try {
      return Files.readString(log);
    } catch (IOException e) {
      return "(no log: " + e.getMessage() + ")";
    }
  }

  /** {@code jdbc:chargewire://127.0.0.1:port}, plus any URL options. */
  String url(String options) {
    String base = "jdbc:chargewire://127.0.0.1:" + port;
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
