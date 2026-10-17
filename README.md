# warp-sql-jdbc

A JDBC driver for the [Warp SQL protocol](https://github.com/gizmodata/warp-sql): SQL over
plain HTTP, with results and parameters as Apache Arrow IPC streams. It is generic: it talks to
any server that implements the Warp SQL spec, and is tested against the reference server,
`warp-duckdb`.

**Status: 0.1.0-SNAPSHOT**, written against Warp SQL draft 0.1.0. Nothing is published yet.

- JDBC 4.2 driver, registered with `DriverManager` and `ServiceLoader`
- One standalone jar, every dependency relocated inside it
- Java 17 or later, with **no `--add-opens` or other JVM options**
- Results stream batch by batch; nothing loads a whole result into memory
- Long queries survive real networks: an asynchronous mode for proxies with short timeouts, and
  dropped downloads of retained results resume from the next batch
- TLS by default, with PEM or trust-store roots, mutual TLS, and an explicit development-only
  switch to skip verification

## Build

```bash
mvn package            # unit tests, then target/warp-sql-jdbc.jar
mvn verify             # also the integration tests (needs a server binary; see Testing)
```

`target/warp-sql-jdbc.jar` is the driver: put it on the class path, nothing else.

## Connect

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
| `ipcCompression` | `none` | `zstd` accepts ZSTD-compressed Arrow buffers, if the server offers them |
| `resultBufferSize` | `134217728` | Bytes an unread result may buffer when another statement needs its session (see below) |

`WarpDataSource` takes the same URL and properties.

## How statements run

- **Streamed (the default).** One `POST` runs the statement and streams its result; statements
  without rows answer with a document carrying the update count. `setQueryTimeout` becomes
  `Warp-Statement-Timeout`, and `cancel()` posts the statement's cancel request.
- **Asynchronous (`async=true`).** The driver submits, long-polls the status, and then fetches the
  result, which the server retains. Use it when a proxy between you and the server cuts long
  requests.
- **Resumption.** If a download breaks off and the server retained the result, the driver fetches
  it again from the next unread batch. A result that was not retained cannot be resumed; set
  `retainResults=true` to have the server keep results (at the cost of its memory).
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
  its work is gone and the statement fails with SQLSTATE `08007` instead.

Every request that creates a statement carries an `Idempotency-Key`, so retrying one whose response
was lost cannot run it twice. An expired access token is renewed with the connection's credential.

## Types

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

## Errors

Server errors are RFC 9457 problem documents. The driver raises the `SQLException` subclass that
their SQLSTATE calls for (the server's, or one that follows from the problem's `code`), with the
HTTP status as the vendor code. The problem itself is the cause:

```java
} catch (SQLException e) {
  String code = WarpProblem.codeOf(e);   // for example "sql_error" or "statement_timeout"
}
```

## DBeaver and other tools

1. **Database > Driver Manager > New.**
2. Driver name `Warp SQL`; class name `com.gizmodata.warpsql.jdbc.WarpDriver`; URL template
   `jdbc:warp://{host}[:{port}][/{database}]`; default port your server's.
3. **Libraries > Add File:** `warp-sql-jdbc.jar`.
4. Create a connection with that driver, enter the user and password, and under **Driver
   properties** set `useEncryption=false` for a plain-HTTP server, or `tlsRootCerts` for a private
   CA.

No JVM options are needed. Keep DBeaver's default of a separate connection for metadata reads.

## Not supported

Generated keys (use `INSERT ... RETURNING`), savepoints, stored procedure calls, scrollable or
updatable result sets, named cursors, `Array.getResultSet`, struct parameters, and index metadata
(Warp SQL has no index endpoint, so `getIndexInfo` is empty). `getTypeInfo` is empty on servers
without the `type-info` endpoint. Bulk ingest (`POST .../ingest`) has no JDBC API.

## Running on newer JDKs

The driver targets Java 17 and runs on newer JDKs; CI runs its unit tests on 17, 21 and 25. On
JDK 24 and later the JVM prints a one-time warning about `sun.misc.Unsafe`, which Arrow uses to
allocate memory; start the JVM with `--sun-misc-unsafe-memory-access=allow` to silence it.

## Testing

Unit tests (`mvn package`) need nothing else. Integration tests (`mvn verify`) start a Warp SQL
server for themselves on a free port, with a fresh password and an empty home directory:

```bash
WARP_SQL_JDBC_TEST_SERVER_BINARY=/path/to/warp-sql/cpp/build/warp-duckdb mvn verify
```

Without the variable they use a sibling checkout's `../warp-sql/cpp/build/warp-duckdb`. With no
server they fail; they never skip. They run against `target/warp-sql-jdbc.jar` with no other
dependency on the class path, so they also prove the jar is self-contained, and without
`--add-opens`. CI runs the unit tests only, since the server build comes from a private repository.

Tests disabled with `@Disabled` document server bugs; each names the bug and fails if enabled
(`-Djunit.jupiter.conditions.deactivate='org.junit.*DisabledCondition'`). The test certificates
come from `dev/generate-test-certs.sh`.

Formatting is google-java-format, checked by Spotless: `mvn spotless:apply`.

## License

[Apache License 2.0](LICENSE).
