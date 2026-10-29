package com.gizmodata.chargewire.jdbc.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.gizmodata.chargewire.jdbc.ChargewireProblem;
import com.gizmodata.chargewire.jdbc.ChargewireProblemException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.sql.SQLException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import javax.net.ssl.SSLException;

/**
 * Speaks the Chargewire protocol over {@link HttpClient}: discovery, sign-in, and one method per
 * endpoint.
 *
 * <p>Every error response becomes an {@link SQLException} (see {@link SqlErrors}). Requests that
 * are safe to repeat (reads, deletes, and submissions carrying an {@code Idempotency-Key}) are
 * retried when the connection fails before a response arrives. A token the server refuses with
 * {@code invalid_token} is renewed with the connection's credential and the request repeated once,
 * unless the server said the token was revoked or its principal's credentials changed or were
 * dropped (spec §4.2): signing in again would not help, so that error reaches the application with
 * its reason.
 *
 * <p>Internal API.
 */
public final class ChargewireClient implements AutoCloseable {
  /** The media type of Arrow IPC streams. */
  public static final String ARROW_STREAM = "application/vnd.apache.arrow.stream";

  private static final String JSON = "application/json";
  private static final int MAX_ATTEMPTS = 3;
  private static final int MAX_ERROR_BODY = 1 << 20;

  /** The most partitions a client may ask for (spec §6.10). */
  public static final int MAX_RESULT_PARTITIONS = 256;

  /** How long to poll a statement without long-polling. */
  private static final long SHORT_POLLING_NANOS = Duration.ofMillis(250).toNanos();

  /** Renew a token this long before the server says it expires. */
  private static final long TOKEN_MARGIN_NANOS = Duration.ofSeconds(30).toNanos();

  /**
   * The {@code reason}s of an {@code invalid_token} (spec §4.2) or {@code session_ended} (§5.6)
   * after which the client must not sign in again on its own: someone withdrew the access, or the
   * credential no longer works. Any other reason, none, or one this driver does not know, allows
   * one new sign-in.
   */
  private static final Set<String> WITHDRAWN =
      Set.of("token_revoked", "credentials_changed", "principal_dropped");

  private final ConnectionConfig config;
  private final HttpClient http;
  private final ObjectMapper mapper = new ObjectMapper();
  private final String userAgent;
  private volatile Duration requestTimeout;
  private Set<String> capabilities = Set.of();
  private String serverName;
  private String serverVersion;
  private String browserProvider;
  private Duration roundTrip = Duration.ZERO;
  private String resultCompression;

  private final Object tokenLock = new Object();
  private String accessToken;
  private long tokenRenewAtNanos;
  private boolean tokenExpires;

  private ChargewireClient(ConnectionConfig config, String userAgent, Duration requestTimeout)
      throws SQLException {
    this.config = config;
    this.userAgent = userAgent;
    this.requestTimeout = requestTimeout;
    HttpClient.Builder builder =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .followRedirects(HttpClient.Redirect.NEVER);
    if (config.connectTimeout != null) {
      builder.connectTimeout(config.connectTimeout);
    }
    if (config.useEncryption) {
      try {
        builder.sslContext(TlsContexts.create(config));
      } catch (GeneralSecurityException | IOException e) {
        throw new SQLNonTransientConnectionException(
            "Cannot set up TLS: " + e.getMessage(), "08001", e);
      }
    }
    this.http = builder.build();
  }

  /**
   * Connects: reads the server's discovery document and signs in.
   *
   * @param config where and how to connect
   * @param userAgent the {@code User-Agent} to send
   * @param requestTimeout how long to wait for each response's headers, or {@code null}
   * @return a signed-in client
   * @throws SQLException if the server cannot be reached, does not speak Chargewire v1, or refuses
   *     the credential
   */
  public static ChargewireClient connect(
      ConnectionConfig config, String userAgent, Duration requestTimeout) throws SQLException {
    // Until the connection is up, wait for answers no longer than for the TCP connect: opening a
    // connection must not hang on a server that accepts connections but never answers.
    ChargewireClient client =
        new ChargewireClient(
            config, userAgent, requestTimeout != null ? requestTimeout : config.connectTimeout);
    try {
      client.discover();
      client.signIn();
      return client;
    } catch (SQLException | RuntimeException e) {
      client.close();
      throw e;
    }
  }

  /**
   * Returns a new JSON object for request bodies.
   *
   * @return an empty object
   */
  public ObjectNode newObject() {
    return mapper.createObjectNode();
  }

  /**
   * Returns the connection's configuration.
   *
   * @return the configuration
   */
  public ConnectionConfig config() {
    return config;
  }

  /**
   * Sets how long to wait for each response's headers.
   *
   * @param timeout the timeout, or {@code null} to wait forever
   */
  public void setRequestTimeout(Duration timeout) {
    this.requestTimeout = timeout;
  }

  /**
   * Returns how long the client waits for each response's headers.
   *
   * @return the timeout, or {@code null} for none
   */
  public Duration requestTimeout() {
    return requestTimeout;
  }

  /**
   * Returns whether the server lists an optional capability (spec §3.1).
   *
   * @param capability for example {@code prepared_statements}
   * @return {@code true} if listed
   */
  public boolean hasCapability(String capability) {
    return capabilities.contains(capability);
  }

  /**
   * Returns the server's name from its discovery document.
   *
   * @return the name, or {@code null}
   */
  public String serverName() {
    return serverName;
  }

  /**
   * Returns the server's version from its discovery document.
   *
   * @return the version, or {@code null}
   */
  public String serverVersion() {
    return serverVersion;
  }

  // ---- Discovery and authentication ----

  private void discover() throws SQLException {
    long[] times = new long[2];
    JsonNode doc = json(send(timedDiscovery(times), timed(times), true, false), "discovery");
    if (!"chargewire".equals(doc.path("protocol").asText())) {
      throw new SQLNonTransientConnectionException(
          config.baseUri + " is not a Chargewire server (no \"protocol\": \"chargewire\")",
          "08001");
    }
    boolean v1 = false;
    for (JsonNode version : doc.path("protocol_versions")) {
      v1 |= "1".equals(version.asText());
    }
    if (!v1) {
      throw new SQLNonTransientConnectionException(
          "The server does not speak Chargewire version 1: " + doc.path("protocol_versions"),
          "08001");
    }
    Set<String> listed = new HashSet<>();
    for (JsonNode capability : doc.path("capabilities")) {
      listed.add(capability.asText());
    }
    capabilities = Set.copyOf(listed);
    serverName = doc.path("server").path("name").asText(null);
    serverVersion = doc.path("server").path("version").asText(null);
    browserProvider = doc.path("auth").path("oauth_browser").path("provider").asText(null);
    roundTrip = Duration.ofNanos(times[1] - times[0]);
    if (IpcCompression.AUTO.equals(config.ipcCompression)
        && roundTrip.compareTo(IpcCompression.FAR_LINK) >= 0) {
      // The first exchange also opened the connection (TCP, and TLS), so it took a round trip or
      // more longer than one; time a second on the open connection before calling the link far.
      try {
        send(timedDiscovery(times), timed(times), false, false);
        Duration again = Duration.ofNanos(times[1] - times[0]);
        if (again.compareTo(roundTrip) < 0) {
          roundTrip = again;
        }
      } catch (SQLException e) {
        // Keep the first measurement.
      }
    }
    resultCompression = IpcCompression.codec(config.ipcCompression, capabilities, roundTrip);
  }

  /** The discovery request, noting in {@code times[0]} when it goes out. */
  private Supplier<HttpRequest.Builder> timedDiscovery(long[] times) {
    return () -> {
      times[0] = System.nanoTime();
      return request(config.baseUri).GET();
    };
  }

  /** Reads a body, noting in {@code times[1]} when the response's headers arrived. */
  private static HttpResponse.BodyHandler<byte[]> timed(long[] times) {
    return info -> {
      times[1] = System.nanoTime();
      return HttpResponse.BodyHandlers.ofByteArray().apply(info);
    };
  }

  /**
   * Returns the Arrow compression results are asked in (spec §8.2).
   *
   * @return {@code lz4_frame}, {@code zstd}, or {@code none}
   */
  public String resultCompression() {
    return resultCompression == null ? IpcCompression.NONE : resultCompression;
  }

  /**
   * Returns how long the discovery request took, from sending it to its response's headers: about
   * one network round trip.
   *
   * @return the round trip
   */
  public Duration discoveryRoundTrip() {
    return roundTrip;
  }

  private void signIn() throws SQLException {
    ConnectionConfig.Credential credential = config.credential();
    switch (credential) {
      case TOKEN:
        synchronized (tokenLock) {
          accessToken = config.token;
          tokenExpires = false;
        }
        return;
      case PASSWORD:
        if (config.password == null) {
          throw new SQLInvalidAuthorizationSpecException(
              "A password is required with user '" + config.user + "'", "28000");
        }
        requestToken(credential);
        return;
      case SUBJECT_TOKEN:
        requestToken(credential);
        return;
      case CLIENT_CERTIFICATE:
        // Only a server that maps certificates to users offers the grant (spec §4.4).
        if (!hasCapability("mtls_identity")) {
          throw new SQLInvalidAuthorizationSpecException(
              "The server does not offer sign-in with a client certificate (no capability"
                  + " mtls_identity); use authType=password",
              "28000");
        }
        requestToken(credential);
        return;
      case BROWSER:
        if (!hasCapability("oauth_browser")) {
          throw new SQLInvalidAuthorizationSpecException(
              "The server does not offer browser sign-in (no capability oauth_browser); use"
                  + " authType=password",
              "28000");
        }
        browserSignIn();
        return;
      default:
        throw new SQLInvalidAuthorizationSpecException(
            "No credentials: set user and password, token, or subjectToken", "28000");
    }
  }

  private void requestToken(ConnectionConfig.Credential credential) throws SQLException {
    ObjectNode body = newObject();
    if (credential == ConnectionConfig.Credential.PASSWORD) {
      body.put("grant_type", "password")
          .put("username", config.user)
          .put("password", config.password);
    } else if (credential == ConnectionConfig.Credential.CLIENT_CERTIFICATE) {
      // The credential is the certificate this connection presents; the token is bound to it.
      body.put("grant_type", "client_certificate");
    } else {
      body.put("grant_type", "token_exchange").put("subject_token", config.subjectToken);
    }
    byte[] bytes = bytes(body);
    long now = System.nanoTime();
    HttpResponse<byte[]> response =
        send(
            () ->
                request(uri("/v1/auth/token"))
                    .header("Content-Type", JSON)
                    .POST(HttpRequest.BodyPublishers.ofByteArray(bytes)),
            handlerBytes(),
            true,
            false);
    JsonNode doc = json(response, "token");
    String token = doc.path("access_token").asText(null);
    if (token == null || token.isEmpty()) {
      throw new SQLNonTransientConnectionException(
          "The token response has no access_token", "08001");
    }
    long expiresIn = doc.path("expires_in").asLong(0);
    synchronized (tokenLock) {
      accessToken = token;
      tokenExpires = expiresIn > 0;
      tokenRenewAtNanos = now + Duration.ofSeconds(expiresIn).toNanos() - TOKEN_MARGIN_NANOS;
    }
  }

  /**
   * Browser sign-in (spec §4.3): creates a flow, shows its authorize URL, and polls with the flow's
   * secret (in the body, never logged) until the person has signed in.
   */
  private void browserSignIn() throws SQLException {
    HttpResponse<byte[]> created =
        send(
            () -> postJson(uri("/v1/auth/oauth/flows"), "{}".getBytes(StandardCharsets.UTF_8)),
            handlerBytes(),
            true,
            false);
    JsonNode flow = json(created, "browser sign-in");
    String flowId = flow.path("flow_id").asText("");
    String pollSecret = flow.path("poll_secret").asText("");
    String authorizeUrl = flow.path("authorize_url").asText("");
    if (flowId.isEmpty() || pollSecret.isEmpty() || authorizeUrl.isEmpty()) {
      throw SqlErrors.protocolError(
          "The browser sign-in response lacks flow_id, poll_secret or authorize_url");
    }
    long interval = Math.max(1, flow.path("poll_interval_s").asLong(2));
    String what =
        (serverName == null ? "the server" : serverName)
            + (browserProvider == null || browserProvider.isEmpty()
                ? ""
                : " with " + browserProvider);
    BrowserOpener.open(config.browserCommand, authorizeUrl, what, System.err);

    byte[] poll = bytes(newObject().put("poll_secret", pollSecret));
    URI target = uri("/v1/auth/oauth/flows/" + encode(flowId));
    long deadline = System.nanoTime() + config.browserTimeout.toNanos();
    while (true) {
      long now = System.nanoTime();
      HttpResponse<byte[]> response;
      try {
        // Not retried: a poll that completed but whose answer was lost can't be repeated
        // (the token is handed out once).
        response = http.send(postJson(target, poll).build(), handlerBytes());
      } catch (HttpTimeoutException e) {
        throw new SQLTimeoutException("Timed out waiting for the server's response", "HYT01", e);
      } catch (IOException e) {
        throw connectionError(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("Interrupted during the browser sign-in", "HY008", e);
      }
      if (response.statusCode() >= 400) {
        ChargewireProblem problem = readProblem(response);
        if ("flow_not_found".equals(problem.code())) {
          SQLException expired =
              new SQLInvalidAuthorizationSpecException(
                  "The browser sign-in expired or was already used; connect again to sign in",
                  "28000",
                  problem.status());
          expired.initCause(new ChargewireProblemException(problem));
          throw expired;
        }
        throw SqlErrors.toSqlException(problem);
      }
      JsonNode doc = json(response, "browser sign-in");
      String state = doc.path("state").asText("");
      if ("complete".equals(state)) {
        String token = doc.path("access_token").asText("");
        if (token.isEmpty()) {
          throw SqlErrors.protocolError("The browser sign-in completed without an access_token");
        }
        long expiresIn = doc.path("expires_in").asLong(0);
        synchronized (tokenLock) {
          accessToken = token;
          tokenExpires = expiresIn > 0;
          tokenRenewAtNanos = now + Duration.ofSeconds(expiresIn).toNanos() - TOKEN_MARGIN_NANOS;
        }
        return;
      }
      if ("failed".equals(state)) {
        throw failedFlow(doc.path("error"));
      }
      // pending, or a state of a later version: not terminal, so keep polling (spec §12).
      if (System.nanoTime() - deadline > 0) {
        throw new SQLTimeoutException(
            "The browser sign-in was not completed within "
                + config.browserTimeout.toSeconds()
                + " s",
            "HYT00");
      }
      try {
        Thread.sleep(Math.min(Duration.ofSeconds(interval).toMillis(), remainingMillis(deadline)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("Interrupted during the browser sign-in", "HY008", e);
      }
    }
  }

  /**
   * The error of a failed browser sign-in (spec §4.3.2). Its code arrives inside a {@code 200} poll
   * answer, so the problem has no HTTP status. {@code identity_provider_error} is the server
   * failing to reach the provider; {@code access_denied}, {@code invalid_credentials} and a code
   * this driver does not know (a generic refusal, spec §12) are authorization errors.
   */
  static SQLException failedFlow(JsonNode error) {
    String code = error.path("code").asText("");
    String detail = error.path("detail").asText("");
    String text =
        "The browser sign-in failed"
            + (code.isEmpty() ? "" : " (" + code + ")")
            + (detail.isEmpty() ? "" : ": " + detail);
    if (code.isEmpty()) {
      return new SQLInvalidAuthorizationSpecException(text, "28000");
    }
    boolean providerFailed = "identity_provider_error".equals(code);
    return SqlErrors.toSqlException(
        new ChargewireProblem(0, code, null, text, providerFailed ? "08004" : "28000", null, null));
  }

  private static long remainingMillis(long deadlineNanos) {
    return Math.max(1, Duration.ofNanos(deadlineNanos - System.nanoTime()).toMillis() + 1);
  }

  private boolean canRenewToken() {
    ConnectionConfig.Credential credential = config.credential();
    return credential == ConnectionConfig.Credential.PASSWORD
        || credential == ConnectionConfig.Credential.SUBJECT_TOKEN
        || credential == ConnectionConfig.Credential.CLIENT_CERTIFICATE;
  }

  /**
   * Returns whether a token refused for this {@code reason} may be replaced by signing in again
   * with the same credential (spec §4.2, §5.6).
   *
   * @param reason the problem's {@code reason}, or {@code null}
   * @return {@code false} after {@code token_revoked}, {@code credentials_changed} and {@code
   *     principal_dropped}
   */
  public static boolean mayRenewAfter(String reason) {
    return reason == null || !WITHDRAWN.contains(reason);
  }

  /**
   * Gets a new access token with the connection's credential (after {@code session_ended}).
   *
   * @throws SQLException if the credential cannot be renewed or is refused
   */
  public void renewToken() throws SQLException {
    if (config.credential() == ConnectionConfig.Credential.BROWSER) {
      throw new SQLInvalidAuthorizationSpecException(
          "The access token from the browser sign-in can no longer be used; reconnect to sign in"
              + " again",
          "28000");
    }
    if (!canRenewToken()) {
      throw new SQLInvalidAuthorizationSpecException(
          "The access token can no longer be used and the connection has no credential to get a"
              + " new one; reconnect with a new token",
          "28000");
    }
    requestToken(config.credential());
  }

  private String currentToken() throws SQLException {
    boolean renew;
    synchronized (tokenLock) {
      renew = tokenExpires && System.nanoTime() - tokenRenewAtNanos > 0 && canRenewToken();
    }
    if (renew) {
      renewToken();
    }
    synchronized (tokenLock) {
      return accessToken;
    }
  }

  // ---- Sessions (spec §5) ----

  /**
   * Creates a session.
   *
   * @param options the session options to start with
   * @return the session document
   * @throws SQLException on any error
   */
  public JsonNode createSession(ObjectNode options) throws SQLException {
    byte[] body = bytes(newObject().set("options", options));
    return json(
        send(() -> postJson(uri("/v1/sessions"), body), handlerBytes(), true, true), "session");
  }

  /**
   * Reads a session document.
   *
   * @param sessionId the session
   * @return the session document
   * @throws SQLException on any error, {@code session_not_found} included
   */
  public JsonNode getSession(String sessionId) throws SQLException {
    return getSession(sessionId, null);
  }

  /**
   * Reads a session document, waiting at most a given time for the answer.
   *
   * @param sessionId the session
   * @param timeout how long to wait, or {@code null} for the connection's request timeout
   * @return the session document
   * @throws SQLException on any error, {@code session_not_found} included
   */
  public JsonNode getSession(String sessionId, Duration timeout) throws SQLException {
    return json(
        send(
            () -> {
              HttpRequest.Builder builder = request(sessionUri(sessionId, "")).GET();
              if (timeout != null) {
                builder.timeout(timeout);
              }
              return builder;
            },
            handlerBytes(),
            true,
            true),
        "session");
  }

  /**
   * Sets session options.
   *
   * @param sessionId the session
   * @param options the options to set
   * @return the response, with {@code options} and perhaps {@code errors}
   * @throws SQLException on any error
   */
  public JsonNode patchSession(String sessionId, ObjectNode options) throws SQLException {
    byte[] body = bytes(newObject().set("options", options));
    return json(
        send(
            () ->
                request(sessionUri(sessionId, ""))
                    .header("Content-Type", JSON)
                    .method("PATCH", HttpRequest.BodyPublishers.ofByteArray(body)),
            handlerBytes(),
            true,
            true),
        "session options");
  }

  /**
   * Ends a session; a session the server no longer has is already ended.
   *
   * @param sessionId the session
   * @throws SQLException on any other error
   */
  public void deleteSession(String sessionId) throws SQLException {
    try {
      send(() -> request(sessionUri(sessionId, "")).DELETE(), handlerDiscard(), true, true);
    } catch (SQLException e) {
      if (!"session_not_found".equals(ChargewireProblem.codeOf(e))) {
        throw e;
      }
    }
  }

  /**
   * Commits or rolls back the session's transaction (spec §5.5).
   *
   * @param sessionId the session
   * @param commit {@code true} to commit, {@code false} to roll back
   * @throws SQLException on any error
   */
  public void endTransaction(String sessionId, boolean commit) throws SQLException {
    URI target = sessionUri(sessionId, commit ? "/commit" : "/rollback");
    send(
        () -> request(target).POST(HttpRequest.BodyPublishers.noBody()),
        handlerDiscard(),
        false,
        true);
  }

  // ---- Statements (spec §6, §7) ----

  /** How to submit one statement. */
  public static final class SubmitOptions {
    final long timeoutMillis;
    final boolean async;
    final int resultPartitions;
    final boolean anyOrder;

    /**
     * Creates submit options for a result in one partition.
     *
     * @param timeoutMillis {@code Chargewire-Statement-Timeout}, or 0 for none
     * @param async submit with {@code respond-async} and expect a status document
     */
    public SubmitOptions(long timeoutMillis, boolean async) {
      this(timeoutMillis, async, 1, false);
    }

    /**
     * Creates submit options.
     *
     * @param timeoutMillis {@code Chargewire-Statement-Timeout}, or 0 for none
     * @param async submit with {@code respond-async} and expect a status document
     * @param resultPartitions {@code Chargewire-Result-Partitions}: the most partitions to read at
     *     once (spec §6.10); 1 asks for none
     * @param anyOrder {@code Chargewire-Result-Order: any}: the rows' order does not matter
     */
    public SubmitOptions(
        long timeoutMillis, boolean async, int resultPartitions, boolean anyOrder) {
      this.timeoutMillis = timeoutMillis;
      this.async = async;
      this.resultPartitions = resultPartitions;
      this.anyOrder = anyOrder;
    }

    /**
     * Returns whether the submission asks for {@code Chargewire-Result-Order: any}.
     *
     * @return {@code true} if the rows' order does not matter
     */
    public boolean anyOrder() {
      return anyOrder;
    }
  }

  /** The answer to a submission: a result stream, or a statement document. */
  public static final class Submission {
    private final StatementDocument document;
    private final ArrowResponse arrow;

    Submission(StatementDocument document, ArrowResponse arrow) {
      this.document = document;
      this.arrow = arrow;
    }

    /**
     * Returns the statement document, for a JSON answer.
     *
     * @return the document, or {@code null} when the result is streaming
     */
    public StatementDocument document() {
      return document;
    }

    /**
     * Returns the streaming result, for an Arrow answer.
     *
     * @return the response, or {@code null} for a JSON answer
     */
    public ArrowResponse arrow() {
      return arrow;
    }

    /**
     * Returns the statement's id.
     *
     * @return the id
     */
    public String statementId() {
      return document != null ? document.statementId() : arrow.statementId();
    }
  }

  /** An Arrow IPC stream in a response body. */
  public static final class ArrowResponse {
    private final InputStream body;
    private final String statementId;
    private final String partitions;
    private final String order;

    ArrowResponse(InputStream body, String statementId) {
      this(body, statementId, null, null);
    }

    ArrowResponse(InputStream body, String statementId, String partitions, String order) {
      this.body = body;
      this.statementId = statementId;
      this.partitions = partitions;
      this.order = order;
    }

    /**
     * Returns the {@code Chargewire-Result-Partitions} header of a streamed result (spec §6.10):
     * how many partitions the result is split into, this response being partition 0.
     *
     * @return the header's value, or {@code null} for a result in one partition
     */
    public String partitions() {
      return partitions;
    }

    /**
     * Returns the {@code Chargewire-Result-Order} header of a streamed result split into
     * partitions.
     *
     * @return {@code interleaved} or {@code any}, or {@code null}
     */
    public String order() {
      return order;
    }

    /**
     * Returns the body. Closing it before the end releases the connection.
     *
     * @return the body
     */
    public InputStream body() {
      return body;
    }

    /**
     * Returns the {@code Chargewire-Statement-Id} the response named.
     *
     * @return the id, or {@code null}
     */
    public String statementId() {
      return statementId;
    }
  }

  /**
   * Submits a statement (spec §6.1).
   *
   * @param sessionId the session
   * @param body {@code {"sql": …, "parameters": …}}
   * @param options timeout and path
   * @return a result stream or a statement document
   * @throws SQLException if the server refuses the statement
   */
  public Submission submit(String sessionId, ObjectNode body, SubmitOptions options)
      throws SQLException {
    byte[] bytes = bytes(body);
    return execute(sessionUri(sessionId, "/statements"), JSON, bytes, options);
  }

  /**
   * Prepares a statement (spec §7.1).
   *
   * @param sessionId the session
   * @param sql the statement
   * @return {@code {prepared_id, parameter_schema, result_schema}}
   * @throws SQLException if the server refuses the statement
   */
  public JsonNode prepare(String sessionId, String sql) throws SQLException {
    byte[] body = bytes(newObject().put("sql", sql));
    return json(
        send(() -> postJson(sessionUri(sessionId, "/prepared"), body), handlerBytes(), false, true),
        "prepared statement");
  }

  /**
   * Executes a prepared statement (spec §7.2).
   *
   * @param sessionId the session
   * @param preparedId the prepared statement
   * @param contentType {@code application/json} or the Arrow stream type
   * @param body the parameters
   * @param options timeout and path
   * @return a result stream or a statement document
   * @throws SQLException if the server refuses the execution
   */
  public Submission executePrepared(
      String sessionId, String preparedId, String contentType, byte[] body, SubmitOptions options)
      throws SQLException {
    URI target = sessionUri(sessionId, "/prepared/" + preparedId + "/execute");
    return execute(target, contentType, body, options);
  }

  /**
   * Closes a prepared statement; one the server no longer has is already closed.
   *
   * @param sessionId the session
   * @param preparedId the prepared statement
   * @throws SQLException on any other error
   */
  public void closePrepared(String sessionId, String preparedId) throws SQLException {
    try {
      send(
          () -> request(sessionUri(sessionId, "/prepared/" + preparedId)).DELETE(),
          handlerDiscard(),
          true,
          true);
    } catch (SQLException e) {
      String code = ChargewireProblem.codeOf(e);
      if (!"prepared_not_found".equals(code) && !"session_not_found".equals(code)) {
        throw e;
      }
    }
  }

  private Submission execute(URI target, String contentType, byte[] body, SubmitOptions options)
      throws SQLException {
    String idempotencyKey = UUID.randomUUID().toString();
    HttpResponse<InputStream> response =
        send(
            () -> {
              HttpRequest.Builder builder =
                  request(target)
                      .header("Content-Type", contentType)
                      .header("Idempotency-Key", idempotencyKey)
                      .POST(HttpRequest.BodyPublishers.ofByteArray(body));
              if (options.async) {
                builder.header("Accept", JSON).header("Prefer", "respond-async");
              } else {
                builder.header("Accept", ARROW_STREAM);
              }
              if (options.timeoutMillis > 0) {
                builder.header(
                    "Chargewire-Statement-Timeout", Long.toString(options.timeoutMillis));
              }
              if (config.retainResults != null) {
                builder.header("Chargewire-Retain-Result", config.retainResults.toString());
              }
              if (options.resultPartitions > 1 && hasCapability("result_partitions")) {
                builder.header(
                    "Chargewire-Result-Partitions",
                    Integer.toString(Math.min(options.resultPartitions, MAX_RESULT_PARTITIONS)));
                builder.header("Chargewire-Result-Order", options.anyOrder ? "any" : "preserve");
              }
              if (followsLocations()) {
                builder.header("Chargewire-Result-Locations", "remote");
              }
              acceptCompression(builder);
              return builder;
            },
            HttpResponse.BodyHandlers.ofInputStream(),
            true,
            true);
    if (isArrow(response)) {
      return new Submission(
          null,
          new ArrowResponse(
              response.body(),
              response.headers().firstValue("Chargewire-Statement-Id").orElse(null),
              response.headers().firstValue("Chargewire-Result-Partitions").orElse(null),
              response.headers().firstValue("Chargewire-Result-Order").orElse(null)));
    }
    return new Submission(statementDocument(readJson(response)), null);
  }

  /**
   * Returns whether the driver reads partitions held by other servers: only from a server with the
   * {@code result_locations} capability (spec §6.2).
   *
   * @return {@code true} if submissions ask for remote locations
   */
  public boolean followsLocations() {
    return hasCapability("result_locations");
  }

  /**
   * Reads a statement's status (spec §6.3).
   *
   * @param sessionId the session
   * @param statementId the statement
   * @param waitSeconds how long the server may hold the answer for a state change, or 0
   * @return the statement document
   * @throws SQLException on any error
   */
  public StatementDocument status(String sessionId, String statementId, int waitSeconds)
      throws SQLException {
    int wait = hasCapability("long_poll") ? waitSeconds : 0;
    URI target = sessionUri(sessionId, "/statements/" + statementId);
    HttpResponse<byte[]> response =
        send(
            () -> {
              HttpRequest.Builder builder = request(target).GET();
              if (wait > 0) {
                builder.header("Prefer", "wait=" + wait);
                Duration timeout = requestTimeout;
                if (timeout != null) {
                  builder.timeout(timeout.plusSeconds(wait));
                }
              }
              return builder;
            },
            handlerBytes(),
            true,
            true);
    return statementDocument(json(response, "statement"));
  }

  /**
   * Waits until a statement reaches a terminal state, long-polling when the server offers it.
   *
   * @param sessionId the session
   * @param statementId the statement
   * @return the terminal statement document
   * @throws SQLException on any error, or if the waiting thread is interrupted
   */
  public StatementDocument awaitTerminal(String sessionId, String statementId) throws SQLException {
    // Most statements finish within milliseconds, so poll briefly before long-polling: some
    // servers hold a long-poll on a statement that has already finished for its whole wait
    // (spec §6.3 now forbids it). Long-poll waits then grow from a second to pollWait.
    long shortPollsEnd = System.nanoTime() + SHORT_POLLING_NANOS;
    long pauseMillis = 1;
    int wait = 1;
    for (; ; ) {
      boolean longPoll =
          hasCapability("long_poll")
              && config.pollWaitSeconds > 0
              && System.nanoTime() - shortPollsEnd > 0;
      StatementDocument document =
          status(sessionId, statementId, longPoll ? Math.min(wait, config.pollWaitSeconds) : 0);
      if (document.state().isTerminal()) {
        return document;
      }
      if (longPoll) {
        wait = Math.min(wait * 2, config.pollWaitSeconds);
      } else {
        try {
          Thread.sleep(pauseMillis);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException("Interrupted while waiting for a statement", "HY008", e);
        }
        pauseMillis = Math.min(pauseMillis * 2, 1000);
      }
    }
  }

  /**
   * Fetches a result partition from a batch on (spec §6.4).
   *
   * @param sessionId the session
   * @param statementId the statement
   * @param partition the partition's index
   * @param location the partition's server, or {@code null} for this one
   * @param fromBatch the first batch to send
   * @return the Arrow stream
   * @throws SQLException on any error, {@code result_not_ready} included
   */
  public ArrowResponse result(
      String sessionId, String statementId, int partition, URI location, long fromBatch)
      throws SQLException {
    String path = "/statements/" + statementId + "/result?partition=" + partition;
    if (fromBatch > 0) {
      path += "&from_batch=" + fromBatch;
    }
    URI target =
        location == null
            ? sessionUri(sessionId, path)
            : URI.create(
                trimSlash(checkLocation(location).toString()) + "/v1/sessions/" + sessionId + path);
    HttpResponse<InputStream> response =
        send(
            () -> {
              HttpRequest.Builder builder = request(target).header("Accept", ARROW_STREAM).GET();
              acceptCompression(builder);
              return builder;
            },
            HttpResponse.BodyHandlers.ofInputStream(),
            true,
            true);
    if (!isArrow(response)) {
      discard(response.body());
      throw SqlErrors.protocolError("The result response is not an Arrow stream");
    }
    return new ArrowResponse(response.body(), statementId);
  }

  /**
   * Checks a partition's location before the access token goes there: the driver asked for remote
   * locations, and the location is an absolute HTTP URL that keeps TLS when the connection uses it.
   */
  private URI checkLocation(URI location) throws SQLException {
    if (!followsLocations()) {
      throw SqlErrors.protocolError(
          "The server gave a partition the location "
              + location
              + " though the driver did not send Chargewire-Result-Locations");
    }
    String scheme =
        location.getScheme() == null ? "" : location.getScheme().toLowerCase(Locale.ROOT);
    boolean allowed = "https".equals(scheme) || ("http".equals(scheme) && !config.useEncryption);
    if (!location.isAbsolute() || location.getHost() == null || !allowed) {
      throw SqlErrors.protocolError(
          "A partition's location is not an absolute "
              + (config.useEncryption ? "https" : "http")
              + " URL: "
              + location);
    }
    return location;
  }

  /**
   * Cancels a statement (spec §6.8).
   *
   * @param sessionId the session
   * @param statementId the statement
   * @throws SQLException on any error
   */
  public void cancel(String sessionId, String statementId) throws SQLException {
    URI target = sessionUri(sessionId, "/statements/" + statementId + "/cancel");
    send(
        () -> request(target).POST(HttpRequest.BodyPublishers.noBody()),
        handlerDiscard(),
        true,
        true);
  }

  /**
   * Cancels every statement the session is running or has queued (spec §6.8), for a statement whose
   * id the client does not know yet.
   *
   * @param sessionId the session
   * @throws SQLException on any error
   */
  public void cancelSession(String sessionId) throws SQLException {
    URI target = sessionUri(sessionId, "/cancel");
    send(
        () -> request(target).POST(HttpRequest.BodyPublishers.noBody()),
        handlerDiscard(),
        true,
        true);
  }

  /**
   * Tells the server the client is done with a statement (spec §6.8): its result is freed, a query
   * still running is cancelled, and a write runs to its end. One the server no longer has is
   * already done.
   *
   * @param sessionId the session
   * @param statementId the statement
   * @throws SQLException on any other error
   */
  public void deleteStatement(String sessionId, String statementId) throws SQLException {
    try {
      send(
          () -> request(sessionUri(sessionId, "/statements/" + statementId)).DELETE(),
          handlerDiscard(),
          true,
          true);
    } catch (SQLException e) {
      String code = ChargewireProblem.codeOf(e);
      if (!"statement_not_found".equals(code) && !"session_not_found".equals(code)) {
        throw e;
      }
    }
  }

  // ---- Metadata (spec §10) ----

  /**
   * Fetches a metadata result (spec §10).
   *
   * @param sessionId the session
   * @param kind for example {@code tables}
   * @param parameters query parameters; {@code null} values are left out
   * @return the Arrow stream
   * @throws SQLException on any error
   */
  public ArrowResponse metadata(String sessionId, String kind, Map<String, String> parameters)
      throws SQLException {
    StringBuilder path = new StringBuilder("/metadata/").append(kind);
    char separator = '?';
    for (Map.Entry<String, String> parameter : parameters.entrySet()) {
      if (parameter.getValue() != null) {
        path.append(separator)
            .append(parameter.getKey())
            .append('=')
            .append(encode(parameter.getValue()));
        separator = '&';
      }
    }
    URI target = sessionUri(sessionId, path.toString());
    HttpResponse<InputStream> response =
        send(
            () -> request(target).header("Accept", ARROW_STREAM).GET(),
            HttpResponse.BodyHandlers.ofInputStream(),
            true,
            true);
    if (!isArrow(response)) {
      discard(response.body());
      throw SqlErrors.protocolError("The metadata response is not an Arrow stream");
    }
    return new ArrowResponse(response.body(), null);
  }

  /**
   * Reads the server-info document (spec §10).
   *
   * @param sessionId the session
   * @return the document
   * @throws SQLException on any error
   */
  public JsonNode serverInfo(String sessionId) throws SQLException {
    return json(
        send(
            () -> request(sessionUri(sessionId, "/metadata/server-info")).GET(),
            handlerBytes(),
            true,
            true),
        "server info");
  }

  @Override
  public void close() {
    if (http instanceof AutoCloseable) {
      try {
        ((AutoCloseable) http).close();
      } catch (Exception e) {
        // Nothing left to release.
      }
    }
  }

  // ---- HTTP plumbing ----

  private <T> HttpResponse<T> send(
      Supplier<HttpRequest.Builder> builder,
      HttpResponse.BodyHandler<T> handler,
      boolean retryable,
      boolean authenticated)
      throws SQLException {
    boolean renewed = false;
    for (int attempt = 1; ; attempt++) {
      HttpRequest.Builder request = builder.get();
      if (authenticated) {
        request.header("Authorization", "Bearer " + currentToken());
      }
      HttpResponse<T> response;
      try {
        response = http.send(request.build(), handler);
      } catch (HttpConnectTimeoutException e) {
        throw new SQLNonTransientConnectionException(
            "Timed out connecting to " + config.baseUri, "08001", e);
      } catch (HttpTimeoutException e) {
        throw new SQLTimeoutException("Timed out waiting for the server's response", "HYT01", e);
      } catch (IOException e) {
        if (retryable
            && attempt < MAX_ATTEMPTS
            && !(e instanceof SSLException)
            && tlsAlert(e) == null) {
          pause(attempt);
          continue;
        }
        throw connectionError(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("Interrupted while waiting for the server", "HY008", e);
      }
      int status = response.statusCode();
      if (status < 400) {
        return response;
      }
      ChargewireProblem problem = readProblem(response);
      if (status == 401
          && authenticated
          && !renewed
          && "invalid_token".equals(problem.code())
          && mayRenewAfter(problem.reason())
          && canRenewToken()) {
        renewToken();
        renewed = true;
        attempt--;
        continue;
      }
      throw SqlErrors.toSqlException(problem);
    }
  }

  private SQLException connectionError(IOException e) {
    String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
    String alert = tlsAlert(e);
    if (alert != null) {
      // The server refused the client certificate at the handshake (spec §4.4): the refusal it
      // stands for, not a server the driver could not reach.
      SQLException refusal =
          SqlErrors.toSqlException(
              new ChargewireProblem(
                  0,
                  alert,
                  "certificate_revoked".equals(alert)
                      ? "Certificate revoked"
                      : "Certificate required",
                  "The server refused the TLS handshake with " + config.baseUri + ": " + message,
                  null,
                  null,
                  null));
      refusal.addSuppressed(e);
      return refusal;
    }
    if (e instanceof ConnectException) {
      return new SQLNonTransientConnectionException(
          "Cannot connect to " + config.baseUri + ": " + message, "08001", e);
    }
    if (e instanceof SSLException) {
      return new SQLNonTransientConnectionException(
          "TLS failure with " + config.baseUri + ": " + message, "08001", e);
    }
    return new SQLTransientConnectionException(
        "Connection to " + config.baseUri + " failed: " + message, "08006", e);
  }

  /**
   * Returns the problem code a TLS alert from the server stands for: {@code certificate_required}
   * or {@code certificate_revoked}, or {@code null} for any other failure.
   */
  static String tlsAlert(Throwable error) {
    for (Throwable t = error; t != null; t = t.getCause() == t ? null : t.getCause()) {
      String text = t instanceof SSLException ? t.getMessage() : null;
      if (text != null && text.contains("alert")) {
        if (text.contains("certificate_revoked")) {
          return "certificate_revoked";
        }
        if (text.contains("certificate_required")) {
          return "certificate_required";
        }
      }
    }
    return null;
  }

  private static void pause(int attempt) throws SQLException {
    try {
      Thread.sleep(100L * attempt);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new SQLException("Interrupted while retrying a request", "HY008", e);
    }
  }

  private ChargewireProblem readProblem(HttpResponse<?> response) {
    byte[] body;
    Object raw = response.body();
    if (raw instanceof byte[]) {
      body = (byte[]) raw;
    } else if (raw instanceof InputStream) {
      try (InputStream in = (InputStream) raw) {
        body = in.readNBytes(MAX_ERROR_BODY);
      } catch (IOException e) {
        body = new byte[0];
      }
    } else {
      body = new byte[0];
    }
    String requestId = response.headers().firstValue("Chargewire-Request-Id").orElse(null);
    String contentType = response.headers().firstValue("Content-Type").orElse("");
    if (contentType.toLowerCase(Locale.ROOT).contains("json") && body.length > 0) {
      try {
        JsonNode json = mapper.readTree(body);
        if (json.isObject()) {
          return Problems.parse(json, response.statusCode());
        }
      } catch (IOException e) {
        // Not JSON after all; report the raw response below.
      }
    }
    return Problems.fromResponse(
        response.statusCode(), new String(body, StandardCharsets.UTF_8), requestId);
  }

  private HttpRequest.Builder request(URI target) {
    HttpRequest.Builder builder = HttpRequest.newBuilder(target).header("User-Agent", userAgent);
    Duration timeout = requestTimeout;
    if (timeout != null) {
      builder.timeout(timeout);
    }
    return builder;
  }

  private HttpRequest.Builder postJson(URI target, byte[] body) {
    return request(target)
        .header("Content-Type", JSON)
        .POST(HttpRequest.BodyPublishers.ofByteArray(body));
  }

  /** Asks for the connection's Arrow compression, on every request that can return Arrow. */
  private void acceptCompression(HttpRequest.Builder builder) {
    if (resultCompression != null) {
      builder.header("Chargewire-IPC-Compression", resultCompression);
    }
  }

  private URI uri(String path) {
    return URI.create(config.baseUri + path);
  }

  private URI sessionUri(String sessionId, String path) {
    return URI.create(config.baseUri + "/v1/sessions/" + sessionId + path);
  }

  private static String trimSlash(String text) {
    return text.endsWith("/") ? text.substring(0, text.length() - 1) : text;
  }

  private static String encode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static boolean isArrow(HttpResponse<?> response) {
    return response
        .headers()
        .firstValue("Content-Type")
        .map(type -> type.toLowerCase(Locale.ROOT).startsWith(ARROW_STREAM))
        .orElse(false);
  }

  private static HttpResponse.BodyHandler<byte[]> handlerBytes() {
    return HttpResponse.BodyHandlers.ofByteArray();
  }

  private static HttpResponse.BodyHandler<Void> handlerDiscard() {
    return HttpResponse.BodyHandlers.discarding();
  }

  private byte[] bytes(JsonNode node) throws SQLException {
    try {
      return mapper.writeValueAsBytes(node);
    } catch (IOException e) {
      throw new SQLException("Cannot encode a request: " + e.getMessage(), "HY000", e);
    }
  }

  private JsonNode json(HttpResponse<byte[]> response, String what) throws SQLException {
    try {
      JsonNode node = mapper.readTree(response.body());
      if (node == null || !node.isObject()) {
        throw new IOException("not a JSON object");
      }
      return node;
    } catch (IOException e) {
      throw SqlErrors.protocolError(
          "The server's " + what + " response is not valid JSON: " + e.getMessage(), e);
    }
  }

  private JsonNode readJson(HttpResponse<InputStream> response) throws SQLException {
    try (InputStream in = response.body()) {
      JsonNode node = mapper.readTree(in);
      if (node == null || !node.isObject()) {
        throw new IOException("not a JSON object");
      }
      return node;
    } catch (IOException e) {
      throw SqlErrors.protocolError(
          "The server's statement response is not valid JSON: " + e.getMessage(), e);
    }
  }

  private static StatementDocument statementDocument(JsonNode json) throws SQLException {
    try {
      return StatementDocument.parse(json);
    } catch (IllegalArgumentException e) {
      throw SqlErrors.protocolError(e.getMessage(), e);
    }
  }

  private static void discard(InputStream body) {
    try (InputStream in = body) {
      in.transferTo(java.io.OutputStream.nullOutputStream());
    } catch (IOException e) {
      // The body is being thrown away anyway.
    }
  }
}
