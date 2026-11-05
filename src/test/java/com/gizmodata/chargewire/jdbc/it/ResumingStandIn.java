package com.gizmodata.chargewire.jdbc.it;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * What answers at a stopped server's address while it starts again (spec §11.2), in front of a real
 * server: requests matching an armed pattern get {@code 503 server_resuming} with {@code
 * Retry-After}, each answer held for a while if asked, until the armed count runs out; every other
 * request is forwarded to the server unchanged. It records each request it sees (method, path and
 * body), so tests can check the driver repeated the same request.
 */
final class ResumingStandIn implements AutoCloseable {
  /** One request as the stand-in saw it. */
  record Seen(String method, String path, String body, boolean answeredResuming) {}

  /** Headers the forwarding client sets itself and refuses from the caller. */
  private static final Set<String> RESTRICTED =
      Set.of("connection", "content-length", "expect", "host", "upgrade", "transfer-encoding");

  private final HttpServer listener;
  private final String target;
  private final HttpClient client =
      HttpClient.newBuilder()
          .version(HttpClient.Version.HTTP_1_1)
          .connectTimeout(Duration.ofSeconds(10))
          .build();
  private final ConcurrentLinkedQueue<Seen> seen = new ConcurrentLinkedQueue<>();
  private volatile Pattern armed;
  private volatile int left;
  private volatile String retryAfter = "1";
  private volatile Duration hold = Duration.ZERO;

  /**
   * Starts a stand-in on a free loopback port.
   *
   * @param target the real server's base URL, for example {@code http://127.0.0.1:41234}
   */
  ResumingStandIn(String target) throws IOException {
    this.target = target;
    this.listener =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    listener.setExecutor(Executors.newCachedThreadPool());
    listener.createContext("/", this::handle);
    listener.start();
  }

  int port() {
    return listener.getAddress().getPort();
  }

  /** The JDBC URL of the stand-in, plus any URL options. */
  String url(String options) {
    String base = "jdbc:chargewire://127.0.0.1:" + port();
    return options == null || options.isEmpty() ? base : base + "?" + options;
  }

  /**
   * Answers the next {@code times} requests whose {@code METHOD path} matches {@code pattern} with
   * {@code server_resuming}, each held for {@code hold} first.
   */
  synchronized void resume(String pattern, int times, String retryAfter, Duration hold) {
    this.retryAfter = retryAfter;
    this.hold = hold;
    this.left = times;
    this.armed = Pattern.compile(pattern);
  }

  /** The requests seen so far whose {@code METHOD path} matches {@code pattern}. */
  List<Seen> seen(String pattern) {
    Pattern p = Pattern.compile(pattern);
    return seen.stream().filter(s -> p.matcher(s.method() + " " + s.path()).matches()).toList();
  }

  private boolean takeResuming(String line) {
    synchronized (this) {
      if (armed != null && left > 0 && armed.matcher(line).matches()) {
        left--;
        return true;
      }
      return false;
    }
  }

  private void handle(HttpExchange exchange) throws IOException {
    try (exchange) {
      byte[] body = exchange.getRequestBody().readAllBytes();
      String path = exchange.getRequestURI().getRawPath();
      String query = exchange.getRequestURI().getRawQuery();
      String method = exchange.getRequestMethod();
      boolean resuming = takeResuming(method + " " + path);
      seen.add(new Seen(method, path, new String(body, StandardCharsets.UTF_8), resuming));
      if (resuming) {
        answerResuming(exchange);
        return;
      }
      forward(exchange, method, path + (query == null ? "" : "?" + query), body);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void answerResuming(HttpExchange exchange) throws IOException, InterruptedException {
    if (!hold.isZero()) {
      Thread.sleep(hold.toMillis());
    }
    byte[] problem =
        ("{\"type\":\"urn:chargewire:error:server_resuming\",\"title\":\"Server resuming\","
                + "\"status\":503,\"code\":\"server_resuming\","
                + "\"detail\":\"the server is resuming; retry after "
                + retryAfter
                + " s\"}")
            .getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/problem+json");
    exchange.getResponseHeaders().set("Retry-After", retryAfter);
    exchange.sendResponseHeaders(503, problem.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(problem);
    }
  }

  private void forward(HttpExchange exchange, String method, String pathAndQuery, byte[] body)
      throws IOException, InterruptedException {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create(target + pathAndQuery))
            .method(
                method,
                body.length == 0
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofByteArray(body));
    for (Map.Entry<String, List<String>> header : exchange.getRequestHeaders().entrySet()) {
      if (RESTRICTED.contains(header.getKey().toLowerCase(Locale.ROOT))) {
        continue;
      }
      for (String value : header.getValue()) {
        request.header(header.getKey(), value);
      }
    }
    HttpResponse<InputStream> response =
        client.send(request.build(), HttpResponse.BodyHandlers.ofInputStream());
    response
        .headers()
        .map()
        .forEach(
            (name, values) -> {
              String lower = name.toLowerCase(Locale.ROOT);
              if (!lower.equals("content-length")
                  && !lower.equals("transfer-encoding")
                  && !lower.equals("connection")
                  && !lower.startsWith(":")) {
                exchange.getResponseHeaders().put(name, values);
              }
            });
    int status = response.statusCode();
    boolean noBody = status == 204 || status == 304 || method.equals("HEAD");
    exchange.sendResponseHeaders(status, noBody ? -1 : 0);
    try (InputStream in = response.body();
        OutputStream out = exchange.getResponseBody()) {
      if (!noBody) {
        in.transferTo(out);
      }
    }
  }

  @Override
  public void close() {
    listener.stop(0);
  }
}
