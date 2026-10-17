# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `getColumns` reports `COLUMN_DEF` from the server's `WARP:SQL:COLUMN_DEFAULT` (warp-sql
  0d5a36c). The tests for typed-NULL, list and interval parameters and for column
  comments and defaults run again: the server bugs they were disabled for are fixed.

- A JDBC 4.2 driver for the Warp SQL protocol (draft 0.1.0): `jdbc:warp://host[:port][/catalog]`,
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
