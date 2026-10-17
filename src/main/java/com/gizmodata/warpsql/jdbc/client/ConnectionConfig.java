package com.gizmodata.warpsql.jdbc.client;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;

/**
 * Everything a {@link WarpClient} needs to reach and sign in to a server, already validated.
 *
 * <p>Internal API: applications configure the driver with {@link
 * com.gizmodata.warpsql.jdbc.WarpProperty} keys instead.
 */
public final class ConnectionConfig {
  /** How the client proves who it is. */
  public enum Credential {
    /** {@code password} grant with a user name and password. */
    PASSWORD,
    /** A pre-issued access token, sent as is. */
    TOKEN,
    /** {@code token_exchange} grant with an identity-provider token. */
    SUBJECT_TOKEN,
    /** {@code client_certificate} grant: the mutual-TLS client certificate (spec §4.4). */
    CLIENT_CERTIFICATE,
    /** No credential: only unauthenticated endpoints work. */
    NONE
  }

  final URI baseUri;
  final String user;
  final String password;
  final String token;
  final String subjectToken;
  final boolean clientCertificateGrant;
  final boolean useEncryption;
  final boolean disableCertificateVerification;
  final boolean useSystemTrustStore;
  final Path tlsRootCerts;
  final Path trustStore;
  final String trustStorePassword;
  final Path clientCertificate;
  final Path clientKey;
  final Duration connectTimeout;
  final boolean acceptZstd;
  final Boolean retainResults;
  final boolean async;
  final int pollWaitSeconds;
  final int resultPartitions;
  final boolean anyOrder;

  private ConnectionConfig(Builder builder) {
    String scheme = builder.useEncryption ? "https" : "http";
    int port = builder.port > 0 ? builder.port : builder.useEncryption ? 443 : 80;
    String host = builder.host.contains(":") ? "[" + builder.host + "]" : builder.host;
    String basePath =
        builder.basePath.endsWith("/")
            ? builder.basePath.substring(0, builder.basePath.length() - 1)
            : builder.basePath;
    this.baseUri = URI.create(scheme + "://" + host + ":" + port + basePath);
    this.user = builder.user;
    this.password = builder.password;
    this.token = builder.token;
    this.subjectToken = builder.subjectToken;
    this.clientCertificateGrant = builder.clientCertificateGrant;
    this.useEncryption = builder.useEncryption;
    this.disableCertificateVerification = builder.disableCertificateVerification;
    this.useSystemTrustStore = builder.useSystemTrustStore;
    this.tlsRootCerts = builder.tlsRootCerts;
    this.trustStore = builder.trustStore;
    this.trustStorePassword = builder.trustStorePassword;
    this.clientCertificate = builder.clientCertificate;
    this.clientKey = builder.clientKey;
    this.connectTimeout = builder.connectTimeout;
    this.acceptZstd = builder.acceptZstd;
    this.retainResults = builder.retainResults;
    this.async = builder.async;
    this.pollWaitSeconds = builder.pollWaitSeconds;
    this.resultPartitions = builder.resultPartitions;
    this.anyOrder = builder.anyOrder;
  }

  /**
   * Returns the user name, if the connection signs in with a password.
   *
   * @return the user name, or {@code null}
   */
  public String user() {
    return user;
  }

  /**
   * Returns which credential the connection uses: the client certificate when the configuration
   * asks for that grant; otherwise a token wins over a subject token, which wins over a user name
   * and password.
   *
   * @return the credential kind
   */
  public Credential credential() {
    if (clientCertificateGrant) {
      return Credential.CLIENT_CERTIFICATE;
    }
    if (token != null) {
      return Credential.TOKEN;
    }
    if (subjectToken != null) {
      return Credential.SUBJECT_TOKEN;
    }
    if (user != null) {
      return Credential.PASSWORD;
    }
    return Credential.NONE;
  }

  /**
   * Returns whether statements are submitted asynchronously and polled.
   *
   * @return {@code true} for the submit, poll and fetch path
   */
  public boolean async() {
    return async;
  }

  /**
   * Returns the {@code Warp-Retain-Result} preference.
   *
   * @return {@code true} or {@code false} to ask, {@code null} to let the server decide
   */
  public Boolean retainResults() {
    return retainResults;
  }

  /**
   * Returns how many partitions of a result to read at once (spec §6.10).
   *
   * @return 1 (results in one partition) to 256
   */
  public int resultPartitions() {
    return resultPartitions;
  }

  /**
   * Returns whether results may come back in any order when they are split into partitions.
   *
   * @return {@code true} for {@code resultOrder=any}
   */
  public boolean anyOrder() {
    return anyOrder;
  }

  /** Builds a {@link ConnectionConfig}. */
  public static final class Builder {
    private String host;
    private int port = -1;
    private String basePath = "/warp";
    private String user;
    private String password;
    private String token;
    private String subjectToken;
    private boolean clientCertificateGrant;
    private boolean useEncryption = true;
    private boolean disableCertificateVerification;
    private boolean useSystemTrustStore = true;
    private Path tlsRootCerts;
    private Path trustStore;
    private String trustStorePassword;
    private Path clientCertificate;
    private Path clientKey;
    private Duration connectTimeout = Duration.ofSeconds(30);
    private boolean acceptZstd;
    private Boolean retainResults;
    private boolean async;
    private int pollWaitSeconds = 10;
    private int resultPartitions = 8;
    private boolean anyOrder;

    /**
     * Sets the host and port.
     *
     * @param host host name or IP address (IPv6 without brackets)
     * @param port port, or a negative number for the scheme's default
     * @return this builder
     */
    public Builder address(String host, int port) {
      this.host = host;
      this.port = port;
      return this;
    }

    /**
     * Sets where the protocol is mounted.
     *
     * @param basePath a path starting with {@code /}
     * @return this builder
     */
    public Builder basePath(String basePath) {
      this.basePath = basePath.startsWith("/") ? basePath : "/" + basePath;
      return this;
    }

    /**
     * Sets the credentials; any of them may be {@code null}.
     *
     * @param user user name for the password grant
     * @param password password for the password grant
     * @param token an access token to send as is
     * @param subjectToken an identity-provider token to exchange
     * @return this builder
     */
    public Builder credentials(String user, String password, String token, String subjectToken) {
      this.user = user;
      this.password = password;
      this.token = token;
      this.subjectToken = subjectToken;
      return this;
    }

    /**
     * Signs in with the {@code client_certificate} grant (spec §4.4) instead of the credentials.
     *
     * @param clientCertificateGrant {@code true} to sign in with the TLS client certificate
     * @return this builder
     */
    public Builder clientCertificateGrant(boolean clientCertificateGrant) {
      this.clientCertificateGrant = clientCertificateGrant;
      return this;
    }

    /**
     * Sets the TLS options.
     *
     * @param useEncryption {@code false} for plain HTTP
     * @param disableCertificateVerification accept any certificate and host name
     * @param useSystemTrustStore trust the JDK's and the operating system's CAs
     * @param tlsRootCerts PEM file of extra CAs, or {@code null}
     * @param trustStore trust store file of extra CAs, or {@code null}
     * @param trustStorePassword the trust store's password, or {@code null}
     * @param clientCertificate PEM client certificate for mutual TLS, or {@code null}
     * @param clientKey PEM private key for mutual TLS, or {@code null}
     * @return this builder
     */
    public Builder tls(
        boolean useEncryption,
        boolean disableCertificateVerification,
        boolean useSystemTrustStore,
        Path tlsRootCerts,
        Path trustStore,
        String trustStorePassword,
        Path clientCertificate,
        Path clientKey) {
      this.useEncryption = useEncryption;
      this.disableCertificateVerification = disableCertificateVerification;
      this.useSystemTrustStore = useSystemTrustStore;
      this.tlsRootCerts = tlsRootCerts;
      this.trustStore = trustStore;
      this.trustStorePassword = trustStorePassword;
      this.clientCertificate = clientCertificate;
      this.clientKey = clientKey;
      return this;
    }

    /**
     * Sets the connect timeout.
     *
     * @param connectTimeout how long to wait for TCP and TLS, and for answers while connecting;
     *     {@code null} waits forever
     * @return this builder
     */
    public Builder connectTimeout(Duration connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Sets how statements run and results travel.
     *
     * @param acceptZstd accept ZSTD-compressed Arrow buffers
     * @param retainResults the {@code Warp-Retain-Result} preference, or {@code null}
     * @param async submit, poll and fetch instead of streaming in one request
     * @param pollWaitSeconds seconds each status long-poll may wait
     * @return this builder
     */
    public Builder execution(
        boolean acceptZstd, Boolean retainResults, boolean async, int pollWaitSeconds) {
      this.acceptZstd = acceptZstd;
      this.retainResults = retainResults;
      this.async = async;
      this.pollWaitSeconds = pollWaitSeconds;
      return this;
    }

    /**
     * Sets how results are read (spec §6.10).
     *
     * @param resultPartitions the most partitions of a result to read at once; 1 for none
     * @param anyOrder the rows of a split result may come back in any order
     * @return this builder
     */
    public Builder partitions(int resultPartitions, boolean anyOrder) {
      this.resultPartitions = resultPartitions;
      this.anyOrder = anyOrder;
      return this;
    }

    /**
     * Builds the configuration.
     *
     * @return the configuration
     */
    public ConnectionConfig build() {
      if (host == null || host.isEmpty()) {
        throw new IllegalStateException("host is required");
      }
      return new ConnectionConfig(this);
    }
  }
}
