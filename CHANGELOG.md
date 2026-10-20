# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Release

- Maven Central publishing, for 1.0.0 at the Hardcharger launch: `com.gizmodata:warp-sql-jdbc`
  is the standalone jar (every dependency relocated inside, so its POM lists none), with sources
  and Javadoc jars, each file GPG-signed (`-P release`, through `central-publishing-maven-plugin`
  0.12.0; uploads are USER_MANAGED unless `-Dcentral.autoPublish=true`). The POM carries the
  developer, SCM and issue metadata Central requires. The version line moves to `1.0.0-SNAPSHOT`.
- `release.yml`: on a `v*` tag, the full test suite on the tagged commit, then the signed bundle
  is checked (`dev/check-central-bundle.sh`: every file, signature by the release key, checksums,
  POM metadata, no SNAPSHOT dependency) and published to Maven Central, and a GitHub Release gets
  the jar, its signature and the CHANGELOG section. By hand from a `release/*` branch it is a dry
  run, or with `central_upload` a USER_MANAGED upload to validate and drop.
- CI runs the integration tests, against `hardcharger-server` from its image, on JDK 17 and 25:
  the release gate now includes them. Builds skip documentation-only changes.

### Fixed

- The integration tests run in the project directory again: with the standalone jar as the
  project's jar, failsafe ran them in `target/`, where the TLS tests' certificate paths don't exist.

### Changed

- Follows Warp SQL 1.0.0, the protocol's first stable version (no fallbacks for the shapes
  before it):
  - `DatabaseMetaData.isReadOnly` reads the `server-info` key `server_read_only` (was
    `read_only`), and `supportsTransactions` reads only `sql_transactions_supported` (with the
    `transactions` capability); `getMaxSchemaNameLength` reads only `sql_db_schema_name_length`.
    A missing key means unknown, and the JDBC default holds.
  - A split result is always read over every partition at once, partition 0 included (each is
    requested before any answers), also when its partitions are held by other servers; those
    were read one after another before. Remote partitions need the `result_locations`
    capability: only then does the driver send `Warp-Result-Locations: remote`, and on the
    streamed path it reads the statement document for the locations of partitions 1 to k-1
    (spec §6.2); a `location` it did not ask for, or one that is not an absolute URL keeping the
    connection's TLS, is refused before the token goes there. A split result without
    `Warp-Result-Order` or `partition_order` is a protocol error.
  - Codes the driver makes up begin with `client_`: `http_error` is now `client_http_error`, and
    responses that break the protocol (SQLSTATE `08P01`) carry `client_protocol_error`
    (`WarpProblem.CLIENT_HTTP_ERROR`, `WarpProblem.CLIENT_PROTOCOL_ERROR`).
  - Error mapping follows the 1.0 statuses: `execution_error` (422) and every other 4xx is a
    `SQLNonTransientException` unless its SQLSTATE says more, `internal_error` (500) a plain
    `SQLException`, `statement_cancelled` (409) a `SQLException` with SQLSTATE `57014` and
    `statement_timeout` a `SQLTimeoutException`, whatever status carried them. New codes map:
    `certificate_required` (was `client_certificate_required`) and `unsupported_grant_type` to
    `28000`, `partition_not_found`, `result_not_resumable` and `result_expired` to `24000`. A
    TLS handshake refused with the alert `certificate_required` or `certificate_revoked` is
    reported as that code (`28000`), not as an unreachable server.
  - A statement cancelled by the time its submission is answered is an error response (409
    `statement_cancelled`); `WarpProblem.cancelReason()` gives its `cancel_reason`, also for a
    cancelled statement document's error. A `failed` or `cancelled` document is the statement's
    error whatever the status.
  - `invalid_token` is renewed once only without a `reason`, after `token_expired`, or after a
    reason the driver does not know; after `token_revoked`, `credentials_changed` or
    `principal_dropped` the error reaches the application with its reason, and the driver does
    not sign in again.
  - A forgotten statement is `statement_not_found`; `not_found` (no such endpoint) is no longer
    taken for one when the driver deletes a statement.
  - Open enumerations (spec §12): an unknown statement state is not terminal (the driver keeps
    polling, `StatementDocument.State.OTHER`), and unknown reasons and cancel reasons pass
    through.
  - Browser sign-in follows the same rules: an unknown flow `state` keeps polling; a failed
    flow's `error.code` (`access_denied`, `invalid_credentials`, `identity_provider_error`, or
    an unknown one, a generic refusal with SQLSTATE `28000`) is the problem's code, with no HTTP
    status (it arrives in a `200` poll answer); `flow_not_found` keeps its code; malformed
    answers are `client_protocol_error`.

- `ipcCompression` defaults to `auto` (was `none`), and accepts `auto`, `lz4_frame`, `zstd` and
  `none`. `auto` times the discovery request when the connection opens and asks for `lz4_frame`
  under a 10 ms round trip, `zstd` from 10 ms on, the same rule as the Warp SQL ADBC driver; a
  codec the server does not list is never asked for. The `Warp-IPC-Compression` request header
  names the one codec chosen, on the submission and on every partition's and every resumed fetch.
- `resultPartitions` defaults to 32 (was 1), like the Go driver: large results are split and read
  over several connections at once. The server decides how many partitions a result gets (a
  small one still comes as one stream), and a server without `result_partitions` never sees the
  request. Set `resultPartitions=1` for one stream. The driver has no bulk-ingest path (batches of a
  prepared statement are bound parameters, spec §7), so partitioned ingest (§8.3) does not
  apply to it.

### Fixed

- A split result whose partition's connection broke reported "the statement was cancelled"
  (57014), the server's reaction, instead of the broken connection (08006, with the advice
  to set `retainResults`), whenever another partition saw the cancellation first. A
  cancellation the application didn't ask for now waits (up to 2 s) for the partition that
  caused it to report its own error.

### Added

- DuckDB's `HUGEINT` and `UHUGEINT` are read exactly. With `SET arrow_lossless_conversion = true`
  a DuckDB-based server sends them as its own 16-byte type (`arrow.opaque`, vendor `DuckDB`),
  which the driver reads as `BigDecimal` (`getObject`, `getBigDecimal`, `getString`; `getLong`
  throws 22003 past a `long`), reported as `NUMERIC` named `HUGEINT` or `UHUGEINT`, precision 39,
  scale 0, signed for `HUGEINT` only. Without it a `HUGEINT` arrives as decimal128(38, 0), and a
  value of 39 digits is refused by the server (`execution_error`, naming
  `arrow_lossless_conversion`) rather than sent wrong; `TypesIT` covers both.
- Browser sign-in (Warp SQL spec §4.3): `authType=oauth_browser` (alias `external`) creates a
  sign-in flow, opens its URL with `oauthBrowser` (a command, `none` to print the URL, default the
  desktop's browser), and polls with the flow's secret in the request body at the server's
  `poll_interval_s` until the person has signed in, for up to `oauthTimeout` seconds (default 300).
  Refusals are SQLSTATE 28000 with the server's code and detail (`access_denied`,
  `invalid_credentials`), a provider failure 08004, an expired or collected flow says so, and a
  server without the `oauth_browser` capability is refused before a browser opens. Never logs
  the poll secret or the token. Tests: 15 unit tests against the fake server, and
  `BrowserSignInIT` (4) against warp-duckdb with a mock OpenID Connect provider.

- `WarpConnection.getTransactionState()` reads the session document's `transaction` (`NONE`,
  `OPEN`, `ABORTED`, or `UNKNOWN` for a value the driver does not know). With autocommit on, a
  session lost while the last session document read said a transaction was open (a SQL
  `BEGIN`) fails the statement with `08007` instead of running it in the new session.

- LZ4_FRAME Arrow results (spec §8.2, capability `ipc_compression_lz4_frame`). The driver
  decodes LZ4 frames itself instead of through Arrow Java's commons-compress codec (about 0.01
  GB/s): about 2.7 GB/s per thread end to end on SF1 `lineitem` (Apple M5 Max), against 2.0 GB/s
  for `zstd`. It reads linked blocks (what Arrow C++ writes, and lz4-java's frame reader refuses)
  and independent ones, every block size, block and content checksums (verified), content sizes,
  stored blocks, concatenated and skippable frames, and buffers the server left uncompressed. No
  new dependency. `DecodeBenchmark` (test sources) measures it.
- Sign-in with the mutual-TLS client certificate (Warp SQL spec §4.4): `authType=client_certificate`
  (alias `mtls`) sends `{"grant_type":"client_certificate"}` over the TLS connection that presents
  `clientCertificate`/`clientKey`, with no user name or password, and renews the token the same
  way before it expires. The server binds the token to the certificate; every connection the
  driver opens (partition readers included) presents it. The driver refuses the setting up front
  (SQLSTATE 28000) without a client certificate, without TLS, or beside `user`, `password`,
  `token` or `subjectToken`, and before signing in when the server lacks the `mtls_identity`
  capability. The new problem codes `certificate_required`,
  `unknown_certificate_identity`, `certificate_revoked`, `certificate_mismatch` and
  `certificate_identity_mismatch` map to SQLSTATE 28000 (`SQLInvalidAuthorizationSpecException`),
  with the code in `WarpProblem.codeOf`. Tests: `ClientCertificateGrantTest` (an HTTPS fake that
  records each request's client certificate) and `ClientCertificateIT` (both servers). The test
  client certificates now carry e-mail SANs, and the test certificates were regenerated.

- Partitioned results (Warp SQL spec §6.10): with `resultPartitions=<n>` (1 to 256, default 1),
  on servers with the `result_partitions` capability, the driver sends
  `Warp-Result-Partitions` and `Warp-Result-Order`, and reads a result the server splits over
  one HTTP connection per partition, all at once, with a few batches buffered per partition, as
  one `ResultSet`. `resultOrder=preserve` (the default) rebuilds the result's order from the
  interleaved partitions, so `ORDER BY` holds; `resultOrder=any` returns batches as they arrive.
  Both work on the streamed path (the `Warp-Result-Partitions`/`Warp-Result-Order` response
  headers) and with `async=true` (`result.partitions` and `partition_order`), and for prepared
  statements; `WarpStatement.setResultPartitions`/`setResultOrder` set them per statement. A
  result is complete only when every partition ended with its end-of-stream marker: any partition
  that breaks off fails the whole result with the statement's error, a retained partition first
  resumes from its own next batch, and closing the result, `cancel()` or `setMaxRows` stop every
  partition's download.

- `getColumns` reports `COLUMN_DEF` from the server's `WARP:SQL:COLUMN_DEFAULT` (warp-sql
  0d5a36c). The tests for typed-NULL, list and interval parameters and for column
  comments and defaults run again: the server bugs they were disabled for are fixed.

- A JDBC 4.2 driver for the Warp SQL protocol, version 1: `jdbc:warp://host[:port][/catalog]`,
  registered with `DriverManager` and `ServiceLoader`, as one standalone jar
  (`target/warp-sql-jdbc.jar`) with every dependency relocated. Java 17 or later; no
  `--add-opens` needed, because Arrow buffers are read and written through heap copies.
- Connecting: discovery (`GET /warp`, protocol version 1 required), the `password` and
  `token_exchange` grants or a pre-issued `token`, renewal of expiring tokens, TLS by default
  with PEM roots, trust stores, the JDK's and the operating system's CAs, mutual TLS with PEM
  keys (PKCS#8, PKCS#1, SEC1), and `disableCertificateVerification` for development.
- Sessions as connections: catalog and schema (also from the URL path), autocommit, commit and
  rollback, `isValid`, network timeouts, and `close` ending the server session. A session the
  server lost is reopened with its options replayed and reported as a warning; with a
  transaction open, the statement fails with SQLSTATE `08007`.
- Statements on the streamed path, or with `async=true` on the submit, long-poll and fetch path;
  `setQueryTimeout` as `Warp-Statement-Timeout`, `cancel` (of the session's work while a
  submission has not been answered yet), `setMaxRows`, update counts, and batches. Closing a
  result early leaves a write that returns rows to finish. Every submission carries an `Idempotency-Key`, and a request whose response was lost
  is retried with it.
- Results that stream batch by batch, read up to their first rows before `execute` returns (so a
  statement that fails as it starts fails there), with incomplete streams detected (the
  statement's own error is raised), resumption of retained results from the next batch (`retainResults`),
  results in several partitions, ZSTD Arrow compression (`ipcCompression=zstd`), and buffering
  of an unread result before other work on its session (`resultBufferSize`).
- Prepared statements prepared on the server, with parameters as Arrow streams in the server's
  parameter types (inferred from the Java values where the server does not know them), a batch
  sent as one request, re-preparation after a lost session, and JSON parameters on servers
  without prepared statements.
- Getters for every common Arrow type with JDBC conversions, `java.time` types through
  `getObject(int, Class)`, calendars for wall-clock values, arrays, structs, maps, unions,
  dictionaries and UUIDs; `WarpInterval` for month-day-nanosecond intervals.
- `DatabaseMetaData` from the metadata endpoints (catalogs, schemas, tables, table types,
  columns from each table's Arrow schema (nullability, auto-increment, and precision and scale of
  decimals, strings and binaries), primary, imported and exported keys, cross references,
  type info) and `server-info`; empty results with JDBC's columns for metadata Warp SQL has no
  endpoint for.
- Errors as `SQLException` subclasses chosen by SQLSTATE, with the server's problem document as
  the cause (`WarpProblem.codeOf`).
- `WarpDataSource`.
- Unit tests, including the client logic against a scripted server, and integration tests
  against a live `warp-duckdb`, or another Warp SQL server named by environment variables, that
  fail, never skip, without one; GizmoSQL 1.x's JDBC tests
  ported where they apply. CI runs the unit tests on JDK 17, 21 and 25.
