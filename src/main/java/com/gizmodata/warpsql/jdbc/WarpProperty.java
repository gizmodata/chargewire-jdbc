package com.gizmodata.warpsql.jdbc;

import java.sql.DriverPropertyInfo;
import java.util.Locale;
import java.util.Map;

/**
 * The connection properties the driver understands. Keys are case-insensitive and can be given in
 * the URL ({@code ?key=value&...} or {@code ;key=value;...}) or in the {@link java.util.Properties}
 * passed to {@link java.sql.DriverManager#getConnection(String, java.util.Properties)}; a value in
 * the URL wins over the same key in the properties.
 */
public enum WarpProperty {
  /** User name for the {@code password} grant. */
  USER("user", null, "User name"),
  /** Password for the {@code password} grant. Any character is allowed, {@code :} included. */
  PASSWORD("password", null, "Password"),
  /** A Warp SQL access token to use as is, instead of signing in. */
  TOKEN("token", null, "Access token to send as the bearer token (no sign-in)"),
  /** An identity-provider token to exchange for an access token ({@code token_exchange} grant). */
  SUBJECT_TOKEN("subjectToken", null, "Identity-provider token for the token_exchange grant"),
  /** Whether to use TLS ({@code https}). Plain HTTP is for loopback and test servers only. */
  USE_ENCRYPTION("useEncryption", "true", "Use TLS (https); false for plain http"),
  /** Accept any server certificate and host name. For development only. */
  DISABLE_CERTIFICATE_VERIFICATION(
      "disableCertificateVerification",
      "false",
      "Skip server certificate and host name verification (development only)"),
  /** Trust the JDK's and the operating system's certificate authorities. */
  USE_SYSTEM_TRUST_STORE(
      "useSystemTrustStore", "true", "Trust the JDK's and the operating system's root CAs"),
  /** A PEM file of extra certificate authorities to trust. */
  TLS_ROOT_CERTS("tlsRootCerts", null, "PEM file of CA certificates to trust"),
  /** A JKS or PKCS#12 trust store of extra certificate authorities to trust. */
  TRUST_STORE("trustStore", null, "Trust store file (JKS or PKCS#12) of CAs to trust"),
  /** The trust store's password. */
  TRUST_STORE_PASSWORD("trustStorePassword", null, "Trust store password"),
  /** A PEM client certificate (chain) for mutual TLS. */
  CLIENT_CERTIFICATE("clientCertificate", null, "PEM client certificate for mutual TLS"),
  /** The PEM private key (PKCS#8, or PKCS#1 for RSA) of {@link #CLIENT_CERTIFICATE}. */
  CLIENT_KEY("clientKey", null, "PEM private key (PKCS#8 or PKCS#1) for mutual TLS"),
  /** The session's initial catalog (also the URL path). */
  CATALOG("catalog", null, "Initial catalog"),
  /** The session's initial schema. */
  SCHEMA("schema", null, "Initial schema"),
  /** Where the server mounts the protocol. */
  BASE_PATH("basePath", "/warp", "Path the server mounts Warp SQL under"),
  /** Milliseconds to wait for a TCP and TLS connection. */
  CONNECT_TIMEOUT("connectTimeout", "30000", "Connect timeout in milliseconds"),
  /** Milliseconds to wait for each response's headers; 0 waits forever. */
  NETWORK_TIMEOUT("networkTimeout", "0", "Response timeout in milliseconds (0: none)"),
  /** Arrow buffer compression to accept for results: {@code none} or {@code zstd}. */
  IPC_COMPRESSION("ipcCompression", "none", "Arrow result compression to accept: none or zstd"),
  /** Ask the server to keep (or not keep) results so they can be re-read and resumed. */
  RETAIN_RESULTS(
      "retainResults", null, "true or false: ask the server to retain results for resumption"),
  /** Submit every statement asynchronously and poll for it instead of holding one request. */
  ASYNC(
      "async",
      "false",
      "Submit, poll and fetch instead of streaming in one request (for long queries behind"
          + " proxies)"),
  /** Seconds a status long-poll may wait on the server. */
  POLL_WAIT("pollWait", "10", "Seconds each status long-poll may wait"),
  /**
   * The most memory, in bytes, an unread result may take when the connection buffers it because
   * another statement needs the session.
   */
  RESULT_BUFFER_SIZE(
      "resultBufferSize",
      "134217728",
      "Bytes an unread result may buffer when another statement needs the session"),
  /**
   * The most partitions of a result to read at once, each over its own connection (spec §6.10);
   * {@code 1} reads every result over one connection.
   */
  RESULT_PARTITIONS(
      "resultPartitions",
      "1",
      "Partitions of a result to read at once over separate connections (1 to 256; 1: one)"),
  /** Whether the rows of a result split into partitions must keep their order. */
  RESULT_ORDER(
      "resultOrder",
      "preserve",
      "preserve or any: whether a result read in partitions must keep its row order");

  private final String key;
  private final String defaultValue;
  private final String description;

  WarpProperty(String key, String defaultValue, String description) {
    this.key = key;
    this.defaultValue = defaultValue;
    this.description = description;
  }

  /**
   * Returns the property's key as documented.
   *
   * @return the key, for example {@code useEncryption}
   */
  public String key() {
    return key;
  }

  /**
   * Returns the value used when the property is not set.
   *
   * @return the default, or {@code null} for none
   */
  public String defaultValue() {
    return defaultValue;
  }

  /**
   * Looks the property up in a map whose keys are lower case.
   *
   * @param lowerCaseProperties properties keyed by lower-case key
   * @return the value, or the default when it is not set
   */
  String get(Map<String, String> lowerCaseProperties) {
    String value = lowerCaseProperties.get(key.toLowerCase(Locale.ROOT));
    return value == null ? defaultValue : value;
  }

  DriverPropertyInfo info(Map<String, String> lowerCaseProperties) {
    DriverPropertyInfo info = new DriverPropertyInfo(key, get(lowerCaseProperties));
    info.description = description;
    info.required = false;
    if ("true".equals(defaultValue) || "false".equals(defaultValue) || this == RETAIN_RESULTS) {
      info.choices = new String[] {"true", "false"};
    } else if (this == IPC_COMPRESSION) {
      info.choices = new String[] {"none", "zstd"};
    } else if (this == RESULT_ORDER) {
      info.choices = new String[] {"preserve", "any"};
    }
    return info;
  }
}
