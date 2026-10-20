<p align="center">
  <a href="https://github.com/gizmodata/warp-sql">
    <img src="docs/assets/warp-sql-jdbc-banner.svg" alt="Warp SQL JDBC: one jar, Java 17+, no gRPC. By GizmoData." width="100%">
  </a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/JDBC-4.2-red.svg?logo=openjdk" alt="JDBC 4.2">
  <img src="https://img.shields.io/badge/Java-17%2B-orange.svg?logo=openjdk" alt="Java 17+">
  <a href="https://github.com/gizmodata/warp-sql"><img src="https://img.shields.io/badge/protocol-Warp%20SQL-31a8db.svg" alt="Warp SQL protocol"></a>
  <img src="https://img.shields.io/badge/JVM%20flags-none%20needed-brightgreen.svg" alt="No JVM flags needed">
  <img src="https://img.shields.io/badge/gRPC-not%20needed-lightgrey.svg" alt="No gRPC">
  <a href="https://github.com/gizmodata/hardcharger"><img src="https://img.shields.io/badge/tested%20with-Hardcharger-f15a24.svg" alt="Tested with Hardcharger"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-brightgreen.svg" alt="Apache 2.0 license"></a>
  <a href="https://gizmodata.com"><img src="https://img.shields.io/badge/by-GizmoData-f15a24.svg" alt="GizmoData"></a>
</p>

<h3 align="center">Warp SQL for Java applications and JDBC tools: drop in one jar.</h3>

---

## ☕ What is it?

A JDBC driver for the [Warp SQL protocol](https://github.com/gizmodata/warp-sql): SQL over
plain HTTP, with results and parameters as Apache Arrow IPC streams. It is generic: it talks to
any server that implements the Warp SQL spec, and is tested against the reference server,
`warp-duckdb`, and [Hardcharger](https://github.com/gizmodata/hardcharger).

**Status: 1.0.0-SNAPSHOT**, for Warp SQL 1.0. Nothing is published yet: 1.0.0 goes to Maven
Central at the Hardcharger launch.

- JDBC 4.2 driver, registered with `DriverManager` and `ServiceLoader`
- One standalone jar, every dependency relocated inside it
- Java 17 or later, with **no `--add-opens` or other JVM options**
- Results stream batch by batch; nothing loads a whole result into memory
- Long queries survive real networks: an asynchronous mode for proxies with short timeouts, and
  dropped downloads of retained results resume from the next batch
- TLS by default, with PEM or trust-store roots, mutual TLS, and an explicit development-only
  switch to skip verification

## 📦 Install

<!-- LAUNCH: these coordinates resolve once 1.0.0 is on Maven Central (Hardcharger GA). -->

Maven:

```xml
<dependency>
  <groupId>com.gizmodata</groupId>
  <artifactId>warp-sql-jdbc</artifactId>
  <version>1.0.0</version>
</dependency>
```

Gradle:

```groovy
implementation 'com.gizmodata:warp-sql-jdbc:1.0.0'
```

The artifact is the standalone jar, with Arrow, Jackson and the rest relocated inside it, so it
brings no dependencies and can't clash with an application's own. For a tool's class path, download
`warp-sql-jdbc.jar` from the [latest release](https://github.com/gizmodata/warp-sql-jdbc/releases/latest).

## 🛠️ Build

```bash
mvn package            # unit tests, then target/warp-sql-jdbc.jar
mvn verify             # also the integration tests (needs a server binary; see Testing)
```

`target/warp-sql-jdbc.jar` is the driver: put it on the class path, nothing else.

## 🚀 Connect

```
jdbc:warp://host[:port][/catalog][?key=value&...]
```

The port defaults to 443 (TLS) or 80 (`useEncryption=false`): Warp SQL is HTTP and works through
ordinary reverse proxies. Options can also follow semicolons (`jdbc:warp://host:31337;user=joe`)
or come in the `Properties`; keys are case-insensitive, and a value in the URL wins. Percent-encode
`&`, `;`, `#` and `%` in URL values.

```java
Properties properties = new Properties();
properties.setProperty("user", "joe");
properties.setProperty("password", "s3:cret");
try (Connection connection =
        DriverManager.getConnection("jdbc:warp://db.example.com:31337/lake", properties);
    PreparedStatement statement =
        connection.prepareStatement("SELECT region, sum(amount) FROM sales WHERE year = ? GROUP BY region");
) {
  statement.setInt(1, 2026);
  try (ResultSet rows = statement.executeQuery()) {
    while (rows.next()) {
      System.out.println(rows.getString(1) + " " + rows.getBigDecimal(2));
    }
  }
}
```

For a local server without TLS, add `useEncryption=false`.

### Properties

| Property | Default | Meaning |
|---|---|---|
| `user`, `password` | | Sign in with the `password` grant. Any character is allowed in the password. |
| `token` | | A Warp SQL access token to send as is (no sign-in; not renewable) |
| `subjectToken` | | An identity-provider token for the `token_exchange` grant |
| `authType` | `password` | `client_certificate` (alias `mtls`): sign in with the mutual-TLS client certificate (`clientCertificate`, `clientKey`) and no password, on servers with the `mtls_identity` capability (spec §4.4). The server binds the token to the certificate; the driver renews it with the certificate. Refused with SQLSTATE 28000 without a certificate, without TLS, or beside `user`, `password`, `token` or `subjectToken`. `oauth_browser` (alias `external`): browser sign-in (spec §4.3), see [Browser sign-in](#-browser-sign-in) |
| `oauthBrowser` | the desktop's | With `authType=oauth_browser`: the command that opens the sign-in page, given its URL as the last argument (split on spaces, no shell); `none` prints the URL to standard error. Default: `java.awt.Desktop`, else `open`/`xdg-open`/`rundll32`, else the URL is printed |
| `oauthTimeout` | `300` | With `authType=oauth_browser`: seconds to wait for the person to sign in |
| `catalog`, `schema` | server's | The session's initial catalog (also the URL path) and schema |
| `useEncryption` | `true` | `false` for plain HTTP (loopback and test servers) |
| `tlsRootCerts` | | PEM file of extra CA certificates to trust |
| `trustStore`, `trustStorePassword` | | JKS or PKCS#12 trust store of extra CAs |
| `useSystemTrustStore` | `true` | Trust the JDK's CAs and, where the JDK can read them, the macOS keychain's or Windows' |
| `clientCertificate`, `clientKey` | | PEM certificate and key (PKCS#8, PKCS#1 RSA or SEC1 EC, unencrypted) for mutual TLS |
| `disableCertificateVerification` | `false` | Accept any certificate and host name. **Development only.** |
| `basePath` | `/warp` | Where the server mounts the protocol |
| `connectTimeout` | `30000` | Milliseconds to connect, and to wait for answers while connecting; `0` waits forever |
| `networkTimeout` | `0` | Milliseconds to wait for each response's headers (also `Connection.setNetworkTimeout`) |
| `async` | `false` | Submit with `respond-async`, long-poll, then fetch: no request stays open for a whole query |
| `pollWait` | `10` | Seconds each status long-poll may wait on the server |
| `retainResults` | server's | `true` asks the server to keep results, so a dropped download resumes; `false` asks it not to |
| `ipcCompression` | `auto` | Arrow compression to ask results in: `auto`, `lz4_frame`, `zstd` or `none` (see below) |
| `resultBufferSize` | `134217728` | Bytes an unread result may buffer when another statement needs its session (see below) |
| `resultPartitions` | `32` | Read up to this many partitions of a result at once, each over its own connection (1 to 256; 1: one stream; see below) |
| `resultOrder` | `preserve` | `any` lets a result read in partitions come back in any row order, which can be faster |

`WarpDataSource` takes the same URL and properties.

## 🧠 How statements run

- **Streamed (the default).** One `POST` runs the statement and streams its result; statements
  without rows answer with a document carrying the update count. `setQueryTimeout` becomes
  `Warp-Statement-Timeout`, and `cancel()` posts the statement's cancel request. A `cancel()`
  that arrives before the server has answered the submission (so the statement has no id yet)
  cancels what the session is running instead; on a connection shared between threads, that
  includes the other threads' statements.
- **Closing a result early.** Closing a `ResultSet` before its end tells the server the client is
  done with it: a query stops, but a write (`INSERT ... RETURNING`) still runs to its end.
- **Asynchronous (`async=true`).** The driver submits, long-polls the status, and then fetches the
  result, which the server retains. Use it when a proxy between you and the server cuts long
  requests.
- **Resumption.** If a download breaks off and the server retained the result, the driver fetches
  it again from the next unread batch. A result that was not retained cannot be resumed; set
  `retainResults=true` to have the server keep results (at the cost of its memory).
- **Partitioned results** (by default). With `resultPartitions` above 1 (32 unless set), on a server with the
  `result_partitions` capability, the driver asks the server to split large results
  (`Warp-Result-Partitions`, spec §6.10) and reads every partition at once, each over its own HTTP
  connection on its own thread, a few batches ahead, into one `ResultSet`. With the default
  `resultOrder=preserve` it takes the batches back round robin in partition order, so an
  `ORDER BY` keeps its order; with `resultOrder=any` it takes whichever partition has a batch
  ready. The server decides how many partitions a result gets and keeps small results (fewer
  than two batches) in one; a server without the capability never sees the request. If
  any partition breaks off, the whole result fails with the statement's error (a retained
  partition resumes from its next batch first); closing the result or `cancel()` stops every
  partition. Per statement: `statement.unwrap(WarpStatement.class).setResultPartitions(4)` and
  `setResultOrder("any")`. A split result that does not say how it is split (no
  `Warp-Result-Order` header, no `partition_order`) is refused as a protocol error. On a server
  with the `result_locations` capability, submissions also accept partitions held by other
  servers (`Warp-Result-Locations: remote`): the driver reads each where its `location` says (on
  the streamed path, from the statement document), with the same token and TLS settings,
  concurrently with the others. Without the capability every partition is read from the
  connection's server.
- **Compression.** Results can come with Arrow's own buffer compression (spec §8.2), in each
  partition's stream. `ipcCompression=auto` (the default) times the discovery request when the
  connection opens: under 10 ms (a LAN or the same host) it asks for `lz4_frame`, which is cheap to
  decode; from 10 ms on (a WAN) it asks for `zstd`, which compresses more. `lz4_frame` and `zstd`
  ask for that codec whatever the link; `none` asks for none. A codec the server does not list
  (`ipc_compression_lz4_frame`, `ipc_compression_zstd`) is never asked for, so `auto` against a
  server with only `zstd` gets uncompressed results on a near link. The driver decodes LZ4 itself
  (about 2.7 GB/s of TPC-H `lineitem` per thread on an Apple M5 Max, against about 2 GB/s for
  `zstd`), including the linked blocks Arrow C++ writes; it never compresses what it uploads.
- **Several open results on one connection.** A server may hold other work on a session until a
  streaming result has been read (warp-duckdb does). So before the driver sends another statement,
  a metadata call or an option change on a session with an unread streaming result, it reads the
  rest of that result into memory, up to `resultBufferSize`. A larger result is dropped instead,
  and reading past the buffered part raises an error that says so. Read results before running the
  next statement, or use `retainResults=true`, to avoid both.
- **Prepared statements** are prepared on the server. Parameters travel as an Arrow stream in the
  server's parameter types, so decimals, timestamps and binaries are exact; where the server does
  not know a parameter's type, the Java value decides it. `executeBatch()` sends the whole batch as
  one request, one row per execution; the server reports one total, so each entry of a batch of
  several is `SUCCESS_NO_INFO`.
- **Lost sessions.** If the server no longer has the session (idle eviction, restart), the driver
  opens a new one, replays catalog, schema and autocommit, re-prepares prepared statements, and
  reports the loss of temporary state as a warning on the connection. If a transaction was open,
  its work is gone and the statement fails with SQLSTATE `08007` instead. A transaction is open
  when autocommit is off and a statement ran since the last commit, or, with autocommit on, when
  the session document last read said so (a SQL `BEGIN`):
  `connection.unwrap(WarpConnection.class).getTransactionState()` reads it (`NONE`, `OPEN`,
  `ABORTED`, or `UNKNOWN`).
- **Ended sessions.** A session the server ended because its token expired is reopened after
  signing in again. One it ended for any other reason (`killed`, `token_revoked`,
  `credentials_changed`, `principal_dropped`, or a reason this driver does not know) closes the
  connection with SQLSTATE `08003` and the reason in the message.

Every request that creates a statement carries an `Idempotency-Key`, so retrying one whose response
was lost cannot run it twice. A token the server refuses (`invalid_token`) is renewed once with the
connection's credential, unless the server says it was revoked or its credentials changed or were
dropped (`token_revoked`, `credentials_changed`, `principal_dropped`): signing in again would not
help, so that error reaches the application with its reason.

## 🔢 Types

| Arrow | JDBC type | `getObject` returns |
|---|---|---|
| int8, int16, int32, int64 | TINYINT, SMALLINT, INTEGER, BIGINT | Byte, Short, Integer, Long |
| uint8, uint16, uint32, uint64 | SMALLINT, INTEGER, BIGINT, NUMERIC(20) | Short, Integer, Long, BigDecimal |
| float16, float32, float64 | REAL, REAL, DOUBLE | Float, Float, Double |
| decimal128, decimal256 | DECIMAL | BigDecimal |
| bool | BOOLEAN | Boolean |
| utf8, large utf8, utf8 view | VARCHAR, LONGVARCHAR, VARCHAR | String |
| binary, large binary, binary view, fixed size binary | VARBINARY, LONGVARBINARY, VARBINARY, BINARY | byte[] |
| date | DATE | java.sql.Date |
| time | TIME | java.sql.Time |
| timestamp without / with time zone | TIMESTAMP / TIMESTAMP_WITH_TIMEZONE | java.sql.Timestamp |
| interval (year-month, day-time, month-day-nano) | OTHER | Period, Duration, WarpInterval |
| duration | OTHER | Duration |
| list, large list, fixed size list | ARRAY | java.util.List (`getArray` gives a java.sql.Array) |
| struct | STRUCT | java.util.Map (`getObject(i, Struct.class)` gives a java.sql.Struct) |
| map | JAVA_OBJECT | java.util.Map |
| arrow.uuid | OTHER | UUID |
| dictionary-encoded | the value type's | the value type's |

`getObject(column, Class)` also converts to `LocalDate`, `LocalTime`, `LocalDateTime`,
`OffsetDateTime`, `ZonedDateTime` and `Instant` (the last three only for timestamps with a time
zone), `BigInteger` and `UUID`. A timestamp without a time zone is a wall-clock value: a
`Calendar` argument says which zone to read it in. A timestamp with a time zone is an instant, and
a `Calendar` never shifts it. Numbers that do not fit the requested type raise SQLSTATE `22003`
rather than wrap around, and text that does not parse raises `22018`.

## ⚠️ Errors

Server errors are RFC 9457 problem documents. The driver raises the `SQLException` subclass that
their SQLSTATE calls for (the server's, or one that follows from the problem's `code`), with the
HTTP status as the vendor code. The problem itself is the cause:

```java
} catch (SQLException e) {
  String code = WarpProblem.codeOf(e);   // for example "sql_error" or "statement_timeout"
}
```

Decide on the `code`, never on the message. `WarpProblem.of(e)` also gives the `reason` of an
`invalid_token` or `session_ended` error and the `cancel_reason` of a cancelled statement (a
statement cancelled before its submission was answered fails at once with `statement_cancelled`).
A code the driver does not know maps by its HTTP status. Failures the driver finds itself carry
codes that begin with `client_`: `client_http_error` (an error response that is not a problem
document, such as a proxy's page) and `client_protocol_error` (SQLSTATE `08P01`, a response that
breaks the protocol). A TLS handshake the server refuses with the alert `certificate_required` or
`certificate_revoked` is reported as that code (SQLSTATE `28000`).

## 🦫 DBeaver and other tools

1. **Database > Driver Manager > New.**
2. Driver name `Warp SQL`; class name `com.gizmodata.warpsql.jdbc.WarpDriver`; URL template
   `jdbc:warp://{host}[:{port}][/{database}]`; default port your server's.
3. **Libraries > Add File:** `warp-sql-jdbc.jar`.
4. Create a connection with that driver, enter the user and password, and under **Driver
   properties** set `useEncryption=false` for a plain-HTTP server, or `tlsRootCerts` for a private
   CA.

No JVM options are needed. Keep DBeaver's default of a separate connection for metadata reads.

## 🚫 Not supported

Generated keys (use `INSERT ... RETURNING`), savepoints, stored procedure calls, scrollable or
updatable result sets, named cursors, `Array.getResultSet`, struct parameters, and index metadata
(Warp SQL has no index endpoint, so `getIndexInfo` is empty). `getTypeInfo` is empty on servers
without the `type-info` endpoint. Bulk ingest (`POST .../ingest`) has no JDBC API.

## 🆕 Running on newer JDKs

The driver targets Java 17 and runs on newer JDKs; CI runs its unit tests on 17, 21 and 25. On
JDK 24 and later the JVM prints a one-time warning about `sun.misc.Unsafe`, which Arrow uses to
allocate memory; start the JVM with `--sun-misc-unsafe-memory-access=allow` to silence it.

## 🌐 Browser sign-in

On a server with the `oauth_browser` capability (its identity provider configured server-side),
`authType=oauth_browser` signs the person in with that provider in a web browser, with no
password in the application:

```java
Properties properties = new Properties();
properties.setProperty("authType", "oauth_browser");
try (Connection connection =
    DriverManager.getConnection("jdbc:warp://db.example.com:31337", properties)) {
  // signed in as whoever signed in to the identity provider
}
```

`getConnection` creates a sign-in flow (spec §4.3), opens its URL in the browser (`oauthBrowser`
picks the command; `none` prints the URL for you to open), and waits, polling the server at the
interval it asks for, until the sign-in is done or `oauthTimeout` passes. The flow's poll secret
travels only in the poll's request body, and neither it nor the access token is ever printed or
logged. A refusal (`access_denied`, or a user the server doesn't admit: `invalid_credentials`) is
an `SQLInvalidAuthorizationSpecException` with SQLSTATE 28000 and the server's reason; a provider
the server couldn't reach is SQLSTATE 08004; a flow that expired or was already collected says so.
A server without the capability is refused before any browser opens. The access token is not
renewed by itself: when it expires, connect again. `user`, `password`, `token` and `subjectToken`
can't be given beside it.

## ✅ Testing

Unit tests (`mvn package`) need nothing else. Integration tests (`mvn verify`) start a Warp SQL
server for themselves on a free port, with a fresh password and an empty home directory:

```bash
WARP_SQL_JDBC_TEST_SERVER_BINARY=/path/to/warp-sql/cpp/build/warp-duckdb mvn verify
```

Mutual TLS that carries identity (`ClientCertificateIT`) starts its servers with
`--mtls-certificate-sign-in --mtls-admin-identities client@example.com` (the test client
certificate's e-mail SAN), flags warp-duckdb and Hardcharger share. Browser sign-in
(`BrowserSignInIT`) starts its server with a mock OpenID Connect provider and uses `curl` as the
browser; the server's provider flags are a template, `WARP_SQL_JDBC_TEST_SERVER_OAUTH_ARGS`
(default warp-duckdb's `--oauth-*`), and the client secret goes in the variable
`WARP_SQL_JDBC_TEST_SERVER_OAUTH_SECRET_ENV` names. The test certificates in `src/test/resources/certs`
come from `dev/generate-test-certs.sh`.

Without the variable they use a sibling checkout's `../warp-sql/cpp/build-main/warp-duckdb`, or
else `../warp-sql/cpp/build/warp-duckdb`. With no server they fail; they never skip.

Other servers run the same suite when told how they differ from `warp-duckdb`:

| Variable | Default | Meaning |
|---|---|---|
| `WARP_SQL_JDBC_TEST_SERVER_BINARY` | sibling `warp-duckdb` | The server to start |
| `WARP_SQL_JDBC_TEST_SERVER_PASSWORD_ENV` | `WARP_DUCKDB_PASSWORD` | The environment variable it reads its password from |
| `WARP_SQL_JDBC_TEST_SERVER_MTLS_FLAG` | `--tls-client-ca` | Its flag for the CA that client certificates must chain to |
| `WARP_SQL_JDBC_TEST_SERVER_NAME` | `warp-duckdb` | The product name its `server-info` reports |
| `WARP_SQL_JDBC_TEST_SERVER_USERNAME_FLAG` | `--username` | The flag that names its administrator |

For example, for Hardcharger:

```bash
WARP_SQL_JDBC_TEST_SERVER_BINARY=/path/to/hardcharger/server/build/hardcharger \
WARP_SQL_JDBC_TEST_SERVER_PASSWORD_ENV=HARDCHARGER_ADMIN_PASSWORD \
WARP_SQL_JDBC_TEST_SERVER_MTLS_FLAG=--mtls-ca-cert \
WARP_SQL_JDBC_TEST_SERVER_USERNAME_FLAG=--admin-username \
WARP_SQL_JDBC_TEST_SERVER_NAME=Hardcharger \
WARP_SQL_JDBC_TEST_SERVER_OAUTH_ARGS='--token-allowed-issuer {issuer} --token-allowed-audience {client_id} --token-default-role admin --token-authorized-emails {identity} --oauth-client-id {client_id} --oauth-base-url {base_url} --oauth-provider-name Mock' \
WARP_SQL_JDBC_TEST_SERVER_OAUTH_SECRET_ENV=HARDCHARGER_OAUTH_CLIENT_SECRET \
mvn verify
```

The server is started with only `PATH`, `HOME` (an empty directory), `TMPDIR` and its password
variable in its environment, so credentials in your shell never reach it. They run against `target/warp-sql-jdbc.jar` with no other
dependency on the class path, so they also prove the jar is self-contained, and without
`--add-opens`. CI runs the unit tests on JDK 17, 21 and 25, and the integration tests on JDK 17
and 25 against `hardcharger-server` from the image named in `.github/workflows/ci.yml`
(`ghcr.io/gizmodata/hardcharger:main`; pulled with the workflow's `GITHUB_TOKEN`, so the package
grants this repository read access).

Tests disabled with `@Disabled` document server bugs; each names the bug and fails if enabled
(`-Djunit.jupiter.conditions.deactivate='org.junit.*DisabledCondition'`). The test certificates
come from `dev/generate-test-certs.sh`.

Formatting is google-java-format, checked by Spotless: `mvn spotless:apply`.

## 🚢 Releases

The version in `pom.xml` is the next release with `-SNAPSHOT`. To release X.Y.Z: set
`<version>X.Y.Z</version>`, move `[Unreleased]` in the [CHANGELOG](CHANGELOG.md) to
`## [X.Y.Z] - <date>`, commit, and push the commit and the tag `vX.Y.Z` together.
`.github/workflows/release.yml` then runs the full test suite (unit and integration) on the tagged
commit, builds the jar, sources and Javadoc jars, signs each file with GizmoData's Maven Central key
(`BAAC1F9A2083FE825C9834D39CB426669B8EEEAA`), checks the bundle (`dev/check-central-bundle.sh`),
uploads it to Maven Central and waits until it is published, and makes a GitHub Release with the
jar, its signature and the CHANGELOG section. Pushing the tag is the decision to publish; Maven
Central never takes a version twice.

A dry run, from a `release/*` branch: **Actions > release > Run workflow**. It runs the tests and
builds, signs and checks the bundle (a workflow artifact) without uploading. With `central_upload`
it also uploads it as USER_MANAGED: Central validates it and holds it under **Deployments** on
[central.sonatype.com](https://central.sonatype.com/publishing/deployments) for Publish or Drop.
Drop it there to test without publishing.

By hand: `mvn -P release deploy` with the Central Portal token as server `central` in
`settings.xml`, the key in gpg's keyring and its passphrase in `MAVEN_GPG_PASSPHRASE`. It uploads
USER_MANAGED (`-Dcentral.autoPublish=true` publishes as soon as it validates), and
`-Dcentral.skipUpload=true` only builds `target/central-publishing/central-bundle.zip`.

## 📄 License

[Apache License 2.0](LICENSE).

---

<p align="center">
  <a href="https://gizmodata.com"><img src="docs/assets/gizmodata-logo.png" alt="GizmoData" width="56"></a><br>
  <sub>Built by <a href="https://gizmodata.com">GizmoData</a></sub>
</p>

---

<p align="center"><sub>Hardcharger™ and GizmoData™ are trademarks of GizmoData LLC.</sub></p>
