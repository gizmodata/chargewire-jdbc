package com.gizmodata.warpsql.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsExchange;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLPeerUnverifiedException;

/**
 * A scripted Warp SQL server for testing the driver's client logic: discovery, sign-in and session
 * creation answer by default; each test adds routes for the requests it is about, and can read back
 * every request the driver sent.
 */
final class FakeWarpServer implements AutoCloseable {
  /** A request the driver sent. */
  static final class Request {
    final String method;
    final String path;
    final String query;
    final Headers headers;
    final byte[] body;

    /** The client certificate's subject on an HTTPS fake, or {@code null} when none was sent. */
    final String clientCertificate;

    Request(HttpExchange exchange, byte[] body) {
      String subject = null;
      if (exchange instanceof HttpsExchange) {
        try {
          java.security.cert.Certificate[] chain =
              ((HttpsExchange) exchange).getSSLSession().getPeerCertificates();
          subject = ((X509Certificate) chain[0]).getSubjectX500Principal().getName();
        } catch (SSLPeerUnverifiedException e) {
          subject = null;
        }
      }
      this.clientCertificate = subject;
      this.method = exchange.getRequestMethod();
      this.path = exchange.getRequestURI().getPath();
      this.query = exchange.getRequestURI().getRawQuery();
      this.headers = exchange.getRequestHeaders();
      this.body = body;
    }

    String body() {
      return new String(body, StandardCharsets.UTF_8);
    }

    String header(String name) {
      return headers.getFirst(name);
    }
  }

  /** Answers one request. */
  @FunctionalInterface
  interface Route {
    void answer(HttpExchange exchange, Request request, Matcher path) throws IOException;
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpServer server;
  private final boolean tls;
  private final String basePath;
  private final Map<Pattern, Route> routes = new LinkedHashMap<>();
  final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
  final AtomicInteger tokens = new AtomicInteger();
  final AtomicInteger sessions = new AtomicInteger();
  volatile String capabilities =
      "[\"prepared_statements\",\"transactions\",\"result_retention\",\"long_poll\","
          + "\"ipc_compression_zstd\"]";
  volatile String protocolVersions = "[\"1\"]";
  volatile int expiresIn = 3600;

  FakeWarpServer() throws IOException {
    this("/warp");
  }

  FakeWarpServer(String basePath) throws IOException {
    this(basePath, null);
  }

  /**
   * An HTTPS fake that asks every connection for a client certificate (and accepts one without):
   * {@code tls} is the server's context.
   */
  FakeWarpServer(SSLContext tls) throws IOException {
    this("/warp", tls);
  }

  private FakeWarpServer(String basePath, SSLContext tls) throws IOException {
    this.basePath = basePath;
    this.tls = tls != null;
    InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
    if (tls == null) {
      server = HttpServer.create(address, 0);
    } else {
      HttpsServer https = HttpsServer.create(address, 0);
      https.setHttpsConfigurator(
          new HttpsConfigurator(tls) {
            @Override
            public void configure(HttpsParameters params) {
              SSLParameters parameters = tls.getDefaultSSLParameters();
              parameters.setWantClientAuth(true);
              params.setSSLParameters(parameters);
            }
          });
      server = https;
    }
    server.setExecutor(Executors.newCachedThreadPool());
    server.createContext("/", this::handle);
    route(
        "GET",
        "",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"protocol\":\"warp-sql\",\"protocol_versions\":"
                    + protocolVersions
                    + ",\"server\":{\"name\":\"fake\",\"version\":\"9.8.7\"},\"capabilities\":"
                    + capabilities
                    + ",\"auth\":{\"grant_types\":[\"password\",\"token_exchange\","
                    + "\"client_certificate\"]}}"));
    route(
        "POST",
        "/v1/auth/token",
        (exchange, request, path) -> {
          if (request.body().contains("\"wrong\"")) {
            problem(exchange, 401, "invalid_credentials", "");
            return;
          }
          json(
              exchange,
              200,
              "{\"access_token\":\"t"
                  + tokens.incrementAndGet()
                  + "\",\"token_type\":\"Bearer\",\"expires_in\":"
                  + expiresIn
                  + "}");
        });
    route(
        "POST",
        "/v1/sessions",
        (exchange, request, path) -> {
          ObjectNode options = (ObjectNode) MAPPER.readTree(request.body).path("options");
          if (!options.has("catalog")) {
            options.put("catalog", "cat");
          }
          if (!options.has("schema")) {
            options.put("schema", "main");
          }
          ObjectNode session =
              MAPPER.createObjectNode().put("session_id", "s" + sessions.incrementAndGet());
          session.set("options", options);
          json(exchange, 201, session.put("idle_timeout_s", 900).toString());
        });
    route("DELETE", "/v1/sessions/([^/]+)", (exchange, request, path) -> empty(exchange, 204));
    route(
        "GET",
        "/v1/sessions/([^/]+)",
        (exchange, request, path) ->
            json(
                exchange,
                200,
                "{\"session_id\":\""
                    + path.group(1)
                    + "\",\"options\":{\"catalog\":\"cat\",\"schema\":\"main\"}}"));
    server.start();
  }

  /** Adds a route, tried before the earlier ones; {@code pathPattern} follows the base path. */
  synchronized FakeWarpServer route(String method, String pathPattern, Route route) {
    Map<Pattern, Route> previous = new LinkedHashMap<>(routes);
    routes.clear();
    routes.put(Pattern.compile(method + " " + Pattern.quote(basePath) + pathPattern), route);
    routes.putAll(previous);
    return this;
  }

  String url(String options) {
    return "jdbc:warp://127.0.0.1:"
        + server.getAddress().getPort()
        + "?useEncryption=false&user=joe&password=pw&basePath="
        + basePath
        + (options.isEmpty() ? "" : "&" + options);
  }

  /** The port the fake listens on. */
  int port() {
    return server.getAddress().getPort();
  }

  String base() {
    return (tls ? "https" : "http") + "://127.0.0.1:" + server.getAddress().getPort() + basePath;
  }

  List<Request> requests(String method, String pathRegex) {
    List<Request> matching = new ArrayList<>();
    synchronized (requests) {
      for (Request request : requests) {
        if (request.method.equals(method)
            && request.path.matches(Pattern.quote(basePath) + pathRegex)) {
          matching.add(request);
        }
      }
    }
    return matching;
  }

  private void handle(HttpExchange exchange) throws IOException {
    Request request = new Request(exchange, exchange.getRequestBody().readAllBytes());
    requests.add(request);
    String key = request.method + " " + request.path;
    Map<Pattern, Route> snapshot;
    synchronized (this) {
      snapshot = new LinkedHashMap<>(routes);
    }
    for (Map.Entry<Pattern, Route> entry : snapshot.entrySet()) {
      Matcher matcher = entry.getKey().matcher(key);
      if (matcher.matches()) {
        Matcher path =
            Pattern.compile(entry.getKey().pattern().substring(request.method.length() + 1))
                .matcher(request.path);
        path.matches();
        entry.getValue().answer(exchange, request, path);
        return;
      }
    }
    problem(exchange, 404, "not_found", "");
  }

  static void json(HttpExchange exchange, int status, String json) throws IOException {
    send(exchange, status, "application/json", json.getBytes(StandardCharsets.UTF_8));
  }

  static void problem(HttpExchange exchange, int status, String code, String extra)
      throws IOException {
    send(
        exchange,
        status,
        "application/problem+json",
        ("{\"type\":\"urn:warp-sql:error:"
                + code
                + "\",\"title\":\""
                + code
                + "\",\"status\":"
                + status
                + ",\"code\":\""
                + code
                + "\",\"detail\":\"fake "
                + code
                + "\""
                + extra
                + "}")
            .getBytes(StandardCharsets.UTF_8));
  }

  static void arrow(HttpExchange exchange, String statementId, byte[] stream) throws IOException {
    exchange.getResponseHeaders().set("Warp-Statement-Id", statementId);
    send(exchange, 200, "application/vnd.apache.arrow.stream", stream);
  }

  static void empty(HttpExchange exchange, int status) throws IOException {
    exchange.sendResponseHeaders(status, -1);
    exchange.close();
  }

  static void send(HttpExchange exchange, int status, String contentType, byte[] body)
      throws IOException {
    exchange.getResponseHeaders().set("Content-Type", contentType);
    exchange.sendResponseHeaders(status, 0);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(body);
    }
  }

  /** A statement document. */
  static String document(String id, String state, String result) {
    return "{\"statement_id\":\""
        + id
        + "\",\"state\":\""
        + state
        + "\""
        + (result == null ? "" : ",\"result\":" + result)
        + "}";
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
