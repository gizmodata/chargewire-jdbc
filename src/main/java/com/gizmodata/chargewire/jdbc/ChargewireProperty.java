package com.gizmodata.chargewire.jdbc;

import com.gizmodata.chargewire.jdbc.client.IpcCompression;
import java.sql.DriverPropertyInfo;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The connection properties the driver understands. Keys are case-insensitive and can be given in
 * the URL ({@code ?key=value&...} or {@code ;key=value;...}) or in the {@link java.util.Properties}
 * passed to {@link java.sql.DriverManager#getConnection(String, java.util.Properties)}; a value in
 * the URL wins over the same key in the properties.
 *
 * <p>A key the driver does not know is never ignored silently. In the URL, which is the driver's
 * own, it fails the connection (SQLSTATE {@code 08001}). In the {@code Properties}, which
 * frameworks fill with keys of their own (Spark passes its read options, Hibernate its {@code
 * hibernate.connection.*} settings), a key fails the connection when it is a misspelling of one of
 * the driver's ({@link #resembling(String)}) or names TLS or signing in ({@link
 * #namesSecurity(String)}); any other is left alone and reported as a warning on the connection
 * (SQLSTATE {@code 01S00}).
 */
public enum ChargewireProperty {
  /**
   * How the connection signs in: {@code password} (the default: {@link #USER} and {@link
   * #PASSWORD}, or {@link #TOKEN} or {@link #SUBJECT_TOKEN}), {@code client_certificate} (alias
   * {@code mtls}): the mutual-TLS client certificate ({@link #CLIENT_CERTIFICATE}, {@link
   * #CLIENT_KEY}) is the credential, with no password (spec §4.4), or {@code oauth_browser} (alias
   * {@code external}): the person signs in with the server's identity provider in a browser (spec
   * §4.3; see {@link #OAUTH_BROWSER} and {@link #OAUTH_TIMEOUT}).
   */
  AUTH_TYPE(
      "authType",
      "password",
      "password, client_certificate (alias mtls): sign in with the mutual-TLS client"
          + " certificate, or oauth_browser (alias external): sign in in a browser with the"
          + " server's identity provider"),
  /**
   * For {@code authType=oauth_browser}: the command that opens the sign-in page, given its URL as
   * the last argument (split on spaces, no shell); {@code none} only prints the URL to standard
   * error. Default: the desktop's browser ({@link java.awt.Desktop}), else the system's opener
   * ({@code open}, {@code xdg-open}, {@code rundll32}), else the URL is printed.
   */
  OAUTH_BROWSER(
      "oauthBrowser",
      null,
      "Command that opens the browser sign-in page (URL appended); none prints the URL"),
  /** For {@code authType=oauth_browser}: seconds to wait for the person to sign in. */
  OAUTH_TIMEOUT("oauthTimeout", "300", "Seconds to wait for a browser sign-in"),
  /** User name for the {@code password} grant. */
  USER("user", null, "User name"),
  /** Password for the {@code password} grant. Any character is allowed, {@code :} included. */
  PASSWORD("password", null, "Password"),
  /** A Chargewire access token to use as is, instead of signing in. */
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
  /** Request a server-enforced, one-way read-only session from creation. */
  READ_ONLY("readOnly", "false", "Require a server-enforced read-only session"),
  /** Where the server mounts the protocol. */
  BASE_PATH("basePath", "/chargewire", "Path the server mounts Chargewire under"),
  /** Milliseconds to wait for a TCP and TLS connection. */
  CONNECT_TIMEOUT("connectTimeout", "30000", "Connect timeout in milliseconds"),
  /**
   * Milliseconds to wait for each response's headers; 0 waits forever. Every request but discovery
   * waits at least 30 s, because a resuming server may hold an answer for up to 25 s.
   */
  NETWORK_TIMEOUT("networkTimeout", "0", "Response timeout in milliseconds (0: none)"),
  /**
   * Seconds to keep repeating a request the server answers with {@code server_resuming} (a stopped
   * server starting again), each time after the answer's {@code Retry-After}; {@code 0} reports the
   * first such answer at once. A resuming server may hold an answer for up to 25 s, so every
   * request but discovery waits at least 30 s for its answer.
   */
  RESUME_TIMEOUT(
      "resumeTimeout",
      "600",
      "Seconds to keep retrying while the server answers server_resuming (0: no retry)"),
  /**
   * Arrow buffer compression to ask results in: {@code auto} ({@code lz4_frame} on a link with a
   * round trip under 10 ms, {@code zstd} on a slower one), {@code lz4_frame}, {@code zstd} or
   * {@code none}. A codec the server does not offer is never used.
   */
  IPC_COMPRESSION(
      "ipcCompression",
      "auto",
      "Arrow result compression: auto (lz4_frame under 10 ms round trip, else zstd), lz4_frame,"
          + " zstd or none"),
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
   * {@code 1} reads every result over one connection. The server decides: a result of fewer than
   * two batches comes as one stream, and a server without the capability is never asked.
   */
  RESULT_PARTITIONS(
      "resultPartitions",
      "32",
      "Partitions of a result to read at once over separate connections (1 to 256; 1: one)"),
  /** Whether the rows of a result split into partitions must keep their order. */
  RESULT_ORDER(
      "resultOrder",
      "preserve",
      "preserve or any: whether a result read in partitions must keep its row order"),
  /**
   * Whether the driver asks a DuckDB server for its exact Arrow types: on a server whose sessions
   * have the option {@code duckdb_arrow_lossless_conversion} (Hardcharger, chargewire-duckdb), the
   * driver turns it on as each session opens. UUID then arrives as a UUID (not text), TIME WITH
   * TIME ZONE with its offset, BIT as bits, and HUGEINT and UHUGEINT exactly through their whole
   * range (not as DECIMAL(38, 0), which fails above 38 digits). {@code false} leaves the session as
   * the server opens it, which is what the driver did before this property existed. Either way a
   * statement can {@code SET SESSION arrow_lossless_conversion}, and the driver reads both forms.
   */
  ARROW_LOSSLESS_CONVERSION(
      "arrowLosslessConversion",
      "true",
      "Ask a DuckDB server for exact Arrow types (UUID, TIME WITH TIME ZONE, BIT, HUGEINT);"
          + " false: as the server opens the session");

  private final String key;
  private final String defaultValue;
  private final String description;

  ChargewireProperty(String key, String defaultValue, String description) {
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
   * Returns the property with a key, in any case.
   *
   * @param key a key as written in a URL or in {@link java.util.Properties}
   * @return the property, or {@code null} when the driver has none of that name
   */
  public static ChargewireProperty of(String key) {
    if (key != null) {
      for (ChargewireProperty property : values()) {
        if (property.key.equalsIgnoreCase(key)) {
          return property;
        }
      }
    }
    return null;
  }

  /**
   * Whether the property's value is a credential, which no message may show.
   *
   * @return {@code true} for passwords and tokens
   */
  boolean secret() {
    return this == PASSWORD
        || this == TOKEN
        || this == SUBJECT_TOKEN
        || this == TRUST_STORE_PASSWORD;
  }

  /** The properties that decide whom the connection trusts and how it signs in. */
  static final List<ChargewireProperty> SECURITY =
      List.of(
          AUTH_TYPE,
          USER,
          PASSWORD,
          TOKEN,
          SUBJECT_TOKEN,
          USE_ENCRYPTION,
          DISABLE_CERTIFICATE_VERIFICATION,
          USE_SYSTEM_TRUST_STORE,
          TLS_ROOT_CERTS,
          TRUST_STORE,
          TRUST_STORE_PASSWORD,
          CLIENT_CERTIFICATE,
          CLIENT_KEY);

  /**
   * Parts of other drivers' names for TLS and sign-in settings ({@code sslmode}, {@code useSSL}).
   */
  private static final List<String> SECURITY_WORDS =
      List.of(
          "ssl",
          "tls",
          "cert",
          "trust",
          "encrypt",
          "verif",
          "auth",
          "passw",
          "pwd",
          "secret",
          "credential",
          "token",
          "keystore",
          "privatekey",
          "clientkey");

  /** Letters and digits in lower case: {@code tls-root-certs} and {@code TLS_ROOT_CERTS} alike. */
  private static String letters(String key) {
    StringBuilder letters = new StringBuilder(key.length());
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (Character.isLetterOrDigit(c)) {
        letters.append(Character.toLowerCase(c));
      }
    }
    return letters.toString();
  }

  /**
   * Returns the property an unknown key is most likely a misspelling of: the same letters with
   * other separators ({@code use_encryption}), one or two letters off ({@code tlsRootCert}, {@code
   * useEncrypton}; one for keys of five letters or fewer), or a longer or shorter form of it
   * ({@code clientCert}, {@code connectTimeoutMs}).
   *
   * @param key a key the driver has no property for
   * @return the property meant, or {@code null} when the key resembles none
   */
  static ChargewireProperty resembling(String key) {
    String given = letters(key);
    if (given.isEmpty()) {
      return null;
    }
    ChargewireProperty best = null;
    int bestRank = Integer.MAX_VALUE;
    for (ChargewireProperty property : values()) {
      String known = letters(property.key);
      int distance = editDistance(given, known);
      boolean longEnough = Math.min(given.length(), known.length()) >= 6;
      // The closest reading wins: the same letters, one letter off, a shortened form, two letters
      // off, then a lengthened form.
      int rank;
      if (distance <= 1) {
        rank = distance;
      } else if (longEnough && known.startsWith(given)) {
        rank = 2;
      } else if (distance == 2 && known.length() > 5) {
        rank = 3;
      } else if (longEnough && given.startsWith(known)) {
        rank = 4;
      } else {
        continue;
      }
      if (rank < bestRank) {
        best = property;
        bestRank = rank;
      }
    }
    return best;
  }

  /**
   * Whether an unknown key names TLS or signing in, in some driver's vocabulary ({@code sslmode},
   * {@code trustServerCertificate}, {@code authMechanism}): ignoring one would leave the connection
   * less protected, or protected differently, than its author believes.
   *
   * @param key a key the driver has no property for
   * @return {@code true} if the key reads as a security setting
   */
  static boolean namesSecurity(String key) {
    String given = letters(key);
    for (String word : SECURITY_WORDS) {
      if (given.contains(word)) {
        return true;
      }
    }
    return false;
  }

  /** Edits (insert, delete, replace, swap two neighbours) that turn one text into the other. */
  private static int editDistance(String a, String b) {
    int[][] d = new int[a.length() + 1][b.length() + 1];
    for (int i = 0; i <= a.length(); i++) {
      d[i][0] = i;
    }
    for (int j = 0; j <= b.length(); j++) {
      d[0][j] = j;
    }
    for (int i = 1; i <= a.length(); i++) {
      for (int j = 1; j <= b.length(); j++) {
        int cost = a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1;
        d[i][j] = Math.min(Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1), d[i - 1][j - 1] + cost);
        if (i > 1
            && j > 1
            && a.charAt(i - 1) == b.charAt(j - 2)
            && a.charAt(i - 2) == b.charAt(j - 1)) {
          d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + 1);
        }
      }
    }
    return d[a.length()][b.length()];
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
      info.choices = IpcCompression.SETTINGS.clone();
    } else if (this == RESULT_ORDER) {
      info.choices = new String[] {"preserve", "any"};
    } else if (this == AUTH_TYPE) {
      info.choices =
          new String[] {"password", "client_certificate", "mtls", "oauth_browser", "external"};
    }
    return info;
  }
}
