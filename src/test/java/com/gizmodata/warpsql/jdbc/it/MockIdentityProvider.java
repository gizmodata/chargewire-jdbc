package com.gizmodata.warpsql.jdbc.it;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * A small OpenID Connect identity provider for the browser sign-in tests (spec §4.3): discovery, a
 * JWKS, an authorization endpoint that approves at once (redirecting the browser back to the
 * server's callback with a code, as a provider does after the person signs in) and a token endpoint
 * that trades the code for an RS256 ID token. It checks the client id and secret, the redirect URI,
 * single-use codes and PKCE (S256), and puts the request's nonce in the ID token. {@code email} and
 * {@code deny} decide what the next sign-in gets.
 */
final class MockIdentityProvider implements AutoCloseable {
  final String clientId = "warp-sql-jdbc-test";
  final String clientSecret;
  volatile String email = "alice@example.com";
  volatile boolean deny;

  private final HttpServer server;
  private final KeyPair key;
  private final Map<String, Map<String, String>> codes = new ConcurrentHashMap<>();
  private final SecureRandom random = new SecureRandom();

  MockIdentityProvider() throws IOException, GeneralSecurityException {
    clientSecret = token() + "+/=";
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    key = generator.generateKeyPair();
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.setExecutor(Executors.newCachedThreadPool());
    server.createContext("/", this::handle);
    server.start();
  }

  String issuer() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  private String token() {
    byte[] bytes = new byte[16];
    random.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private static String b64(byte[] bytes) {
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private static String unsigned(BigInteger value) {
    byte[] bytes = value.toByteArray();
    if (bytes.length > 1 && bytes[0] == 0) {
      bytes = java.util.Arrays.copyOfRange(bytes, 1, bytes.length);
    }
    return b64(bytes);
  }

  private static Map<String, String> form(String text) {
    Map<String, String> out = new LinkedHashMap<>();
    if (text == null || text.isEmpty()) {
      return out;
    }
    for (String pair : text.split("&")) {
      int eq = pair.indexOf('=');
      String name = eq < 0 ? pair : pair.substring(0, eq);
      String value = eq < 0 ? "" : pair.substring(eq + 1);
      out.put(
          URLDecoder.decode(name, StandardCharsets.UTF_8),
          URLDecoder.decode(value, StandardCharsets.UTF_8));
    }
    return out;
  }

  private static String query(Map<String, String> params) {
    StringBuilder out = new StringBuilder();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      if (out.length() > 0) {
        out.append('&');
      }
      out.append(URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8))
          .append('=')
          .append(URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8));
    }
    return out.toString();
  }

  private void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    try {
      if ("GET".equals(exchange.getRequestMethod())) {
        if (path.equals("/.well-known/openid-configuration")) {
          String issuer = issuer();
          reply(
              exchange,
              200,
              "{\"issuer\":\""
                  + issuer
                  + "\",\"jwks_uri\":\""
                  + issuer
                  + "/jwks\",\"authorization_endpoint\":\""
                  + issuer
                  + "/authorize\",\"token_endpoint\":\""
                  + issuer
                  + "/token\"}",
              null);
        } else if (path.equals("/jwks")) {
          RSAPublicKey pub = (RSAPublicKey) key.getPublic();
          reply(
              exchange,
              200,
              "{\"keys\":[{\"kty\":\"RSA\",\"kid\":\"k1\",\"alg\":\"RS256\",\"use\":\"sig\",\"n\":\""
                  + unsigned(pub.getModulus())
                  + "\",\"e\":\""
                  + unsigned(pub.getPublicExponent())
                  + "\"}]}",
              null);
        } else if (path.equals("/authorize")) {
          authorize(exchange, form(exchange.getRequestURI().getRawQuery()));
        } else {
          reply(exchange, 404, "{\"error\":\"not_found\"}", null);
        }
      } else if (path.equals("/token")) {
        token(
            exchange,
            form(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
      } else {
        reply(exchange, 404, "{\"error\":\"not_found\"}", null);
      }
    } catch (GeneralSecurityException e) {
      reply(exchange, 500, "{\"error\":\"server_error\"}", null);
    }
  }

  private void authorize(HttpExchange exchange, Map<String, String> q) throws IOException {
    String redirect = q.get("redirect_uri");
    if (!clientId.equals(q.get("client_id")) || redirect == null) {
      reply(exchange, 400, "{\"error\":\"invalid_request\"}", null);
      return;
    }
    Map<String, String> back = new LinkedHashMap<>();
    if (deny) {
      back.put("error", "access_denied");
      back.put("error_description", "The user said no");
    } else {
      if (!"code".equals(q.get("response_type"))
          || !"S256".equals(q.get("code_challenge_method"))) {
        reply(exchange, 400, "{\"error\":\"invalid_request\"}", null);
        return;
      }
      String code = token();
      Map<String, String> grant = new LinkedHashMap<>();
      grant.put("challenge", q.getOrDefault("code_challenge", ""));
      grant.put("nonce", q.getOrDefault("nonce", ""));
      grant.put("redirect_uri", redirect);
      grant.put("email", email);
      codes.put(code, grant);
      back.put("code", code);
    }
    back.put("state", q.getOrDefault("state", ""));
    reply(exchange, 302, "{}", redirect + "?" + query(back));
  }

  private void token(HttpExchange exchange, Map<String, String> f)
      throws IOException, GeneralSecurityException {
    if (!clientId.equals(f.get("client_id")) || !clientSecret.equals(f.get("client_secret"))) {
      reply(exchange, 401, "{\"error\":\"invalid_client\"}", null);
      return;
    }
    Map<String, String> grant = codes.remove(f.getOrDefault("code", "")); // single use
    if (grant == null || !grant.get("redirect_uri").equals(f.get("redirect_uri"))) {
      reply(exchange, 400, "{\"error\":\"invalid_grant\"}", null);
      return;
    }
    byte[] digest =
        MessageDigest.getInstance("SHA-256")
            .digest(f.getOrDefault("code_verifier", "").getBytes(StandardCharsets.US_ASCII));
    if (!b64(digest).equals(grant.get("challenge"))) {
      reply(exchange, 400, "{\"error\":\"invalid_grant\",\"error_description\":\"PKCE\"}", null);
      return;
    }
    long now = System.currentTimeMillis() / 1000;
    String header = "{\"alg\":\"RS256\",\"typ\":\"JWT\",\"kid\":\"k1\"}";
    String payload =
        "{\"iss\":\""
            + issuer()
            + "\",\"aud\":\""
            + clientId
            + "\",\"sub\":\""
            + grant.get("email")
            + "\",\"email\":\""
            + grant.get("email")
            + "\",\"nonce\":\""
            + grant.get("nonce")
            + "\",\"iat\":"
            + now
            + ",\"exp\":"
            + (now + 300)
            + "}";
    String input =
        b64(header.getBytes(StandardCharsets.UTF_8))
            + "."
            + b64(payload.getBytes(StandardCharsets.UTF_8));
    Signature signer = Signature.getInstance("SHA256withRSA");
    signer.initSign(key.getPrivate());
    signer.update(input.getBytes(StandardCharsets.US_ASCII));
    String idToken = input + "." + b64(signer.sign());
    reply(
        exchange,
        200,
        "{\"access_token\":\"unused\",\"token_type\":\"Bearer\",\"id_token\":\"" + idToken + "\"}",
        null);
  }

  private static void reply(HttpExchange exchange, int status, String body, String location)
      throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    if (location != null) {
      exchange.getResponseHeaders().set("Location", location);
    }
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
