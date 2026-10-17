package com.gizmodata.warpsql.jdbc;

import com.fasterxml.jackson.databind.JsonNode;
import com.gizmodata.warpsql.jdbc.accessor.Accessor;
import com.gizmodata.warpsql.jdbc.accessor.Accessors;
import com.gizmodata.warpsql.jdbc.arrow.ArrowIpc;
import com.gizmodata.warpsql.jdbc.arrow.Rows;
import com.gizmodata.warpsql.jdbc.arrow.SqlTypes;
import com.gizmodata.warpsql.jdbc.client.WarpClient;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Database metadata from Warp SQL's metadata endpoints (spec §10), whose results have Arrow Flight
 * SQL's schemas, and from {@code server-info}. Metadata Warp SQL has no endpoint for (procedures,
 * privileges, indexes, user-defined types) is an empty result with JDBC's columns.
 */
public final class WarpDatabaseMetaData implements DatabaseMetaData {
  private static final Pattern VERSION = Pattern.compile("(\\d+)\\.(\\d+)");

  private final WarpConnection connection;

  WarpDatabaseMetaData(WarpConnection connection) {
    this.connection = connection;
  }

  // ---- Building and reading metadata results ----

  /** Columns as {@code NAME} (text) or {@code NAME:type}, type one of int, short, long, bool. */
  private static Schema columns(String... specs) {
    List<Field> fields = new ArrayList<>();
    for (String spec : specs) {
      int colon = spec.indexOf(':');
      String name = colon < 0 ? spec : spec.substring(0, colon);
      String type = colon < 0 ? "text" : spec.substring(colon + 1);
      ArrowType arrow;
      switch (type) {
        case "int":
          arrow = new ArrowType.Int(32, true);
          break;
        case "short":
          arrow = new ArrowType.Int(16, true);
          break;
        case "long":
          arrow = new ArrowType.Int(64, true);
          break;
        case "bool":
          arrow = ArrowType.Bool.INSTANCE;
          break;
        default:
          arrow = ArrowType.Utf8.INSTANCE;
      }
      fields.add(Field.nullable(name, arrow));
    }
    return new Schema(fields);
  }

  private ResultSet result(Schema schema, List<Object[]> rows) throws SQLException {
    connection.checkOpen();
    VectorSchemaRoot root = Rows.toRoot(connection.allocator(), schema, rows);
    WarpResultSet[] result = new WarpResultSet[1];
    result[0] =
        new WarpResultSet(
            null, new ResultBatches.InMemory(root), 0, () -> connection.forget(result[0]));
    return connection.track(result[0]);
  }

  private ResultSet empty(Schema schema) throws SQLException {
    return result(schema, List.of());
  }

  /** One metadata row, by Flight SQL column name. */
  private static final class Row {
    private final Map<String, Object> values;

    Row(Map<String, Object> values) {
      this.values = values;
    }

    String text(String name) {
      Object value = values.get(name);
      return value == null ? null : value.toString();
    }

    Integer number(String name) {
      Object value = values.get(name);
      return value == null ? null : ((Number) value).intValue();
    }

    Object get(String name) {
      return values.get(name);
    }
  }

  private List<Row> fetch(String kind, Map<String, String> parameters) throws SQLException {
    WarpClient client = connection.client();
    WarpClient.ArrowResponse response =
        connection.withSession(id -> client.metadata(id, kind, parameters));
    List<Row> rows = new ArrayList<>();
    try (InputStream body = response.body();
        ArrowIpc.HeapReadChannel channel = new ArrowIpc.HeapReadChannel(body);
        ArrowStreamReader reader = ArrowIpc.reader(channel, connection.allocator())) {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        List<FieldVector> vectors = root.getFieldVectors();
        List<Accessor> accessors = new ArrayList<>();
        for (FieldVector vector : vectors) {
          accessors.add(Accessors.create(vector, reader));
        }
        for (int r = 0; r < root.getRowCount(); r++) {
          Map<String, Object> values = new LinkedHashMap<>();
          for (int c = 0; c < vectors.size(); c++) {
            Accessor accessor = accessors.get(c);
            values.put(vectors.get(c).getName(), accessor.isNull(r) ? null : accessor.getObject(r));
          }
          rows.add(new Row(values));
        }
      }
      if (channel.reachedEnd()) {
        throw new IOException("the metadata stream ended without its end-of-stream marker");
      }
    } catch (IOException e) {
      throw new SQLNonTransientConnectionException(
          "Cannot read the " + kind + " metadata: " + e.getMessage(), "08006", e);
    }
    return rows;
  }

  private static Map<String, String> parameters(String... keysAndValues) {
    Map<String, String> parameters = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      parameters.put(keysAndValues[i], keysAndValues[i + 1]);
    }
    return parameters;
  }

  /** Turns a SQL LIKE pattern ({@code %}, {@code _}, {@code \} escape) into a regex. */
  static Pattern likePattern(String like) {
    StringBuilder regex = new StringBuilder();
    for (int i = 0; i < like.length(); i++) {
      char c = like.charAt(i);
      if (c == '\\' && i + 1 < like.length()) {
        regex.append(Pattern.quote(String.valueOf(like.charAt(++i))));
      } else if (c == '%') {
        regex.append(".*");
      } else if (c == '_') {
        regex.append('.');
      } else {
        regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }

  // ---- Server info ----

  private JsonNode info(String... keys) throws SQLException {
    JsonNode info = connection.serverInfo();
    for (String key : keys) {
      JsonNode value = info.get(key);
      if (value != null && !value.isNull()) {
        return value;
      }
    }
    return null;
  }

  private String infoText(String fallback, String... keys) throws SQLException {
    JsonNode value = info(keys);
    if (value == null) {
      return fallback;
    }
    if (value.isArray()) {
      List<String> parts = new ArrayList<>();
      value.forEach(part -> parts.add(part.asText()));
      return String.join(",", parts);
    }
    return value.asText();
  }

  private boolean infoFlag(boolean fallback, String... keys) throws SQLException {
    JsonNode value = info(keys);
    return value == null ? fallback : value.asBoolean(fallback);
  }

  private int infoNumber(int fallback, String... keys) throws SQLException {
    JsonNode value = info(keys);
    return value == null || !value.canConvertToInt() ? fallback : value.asInt();
  }

  // ---- Catalogs, schemas, tables, columns ----

  @Override
  public ResultSet getCatalogs() throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    for (Row row : fetch("catalogs", Map.of())) {
      rows.add(new Object[] {row.text("catalog_name")});
    }
    return result(columns("TABLE_CAT"), rows);
  }

  @Override
  public ResultSet getSchemas() throws SQLException {
    return getSchemas(null, null);
  }

  @Override
  public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    for (Row row :
        fetch(
            "db-schemas",
            parameters("catalog", catalog, "db_schema_filter_pattern", schemaPattern))) {
      rows.add(new Object[] {row.text("db_schema_name"), row.text("catalog_name")});
    }
    return result(columns("TABLE_SCHEM", "TABLE_CATALOG"), rows);
  }

  @Override
  public ResultSet getTableTypes() throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    for (Row row : fetch("table-types", Map.of())) {
      rows.add(new Object[] {row.text("table_type")});
    }
    return result(columns("TABLE_TYPE"), rows);
  }

  private List<Row> tables(
      String catalog, String schemaPattern, String tablePattern, String[] types, boolean schemas)
      throws SQLException {
    return fetch(
        "tables",
        parameters(
            "catalog", catalog,
            "db_schema_filter_pattern", schemaPattern,
            "table_name_filter_pattern", tablePattern,
            "table_types", types == null ? null : String.join(",", types),
            "include_schema", schemas ? "true" : null));
  }

  @Override
  public ResultSet getTables(
      String catalog, String schemaPattern, String tableNamePattern, String[] types)
      throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    for (Row row : tables(catalog, schemaPattern, tableNamePattern, types, false)) {
      rows.add(
          new Object[] {
            row.text("catalog_name"),
            row.text("db_schema_name"),
            row.text("table_name"),
            row.text("table_type"),
            null,
            null,
            null,
            null,
            null,
            null
          });
    }
    return result(
        columns(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "TABLE_TYPE",
            "REMARKS",
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "SELF_REFERENCING_COL_NAME",
            "REF_GENERATION"),
        rows);
  }

  /**
   * {@inheritDoc}
   *
   * <p>Built from each table's Arrow schema ({@code include_schema=true}) and its Flight SQL column
   * metadata keys.
   */
  @Override
  public ResultSet getColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    Pattern columnPattern = columnNamePattern == null ? null : likePattern(columnNamePattern);
    List<Object[]> rows = new ArrayList<>();
    for (Row table : tables(catalog, schemaPattern, tableNamePattern, null, true)) {
      byte[] serialized = (byte[]) table.get("table_schema");
      if (serialized == null) {
        continue;
      }
      Schema schema;
      try {
        schema = ArrowIpc.readSchema(serialized);
      } catch (IOException e) {
        throw new SQLException(
            "Cannot read the schema of table " + table.text("table_name") + ": " + e.getMessage(),
            "08P01",
            e);
      }
      int position = 0;
      for (Field field : schema.getFields()) {
        position++;
        if (columnPattern != null && !columnPattern.matcher(field.getName()).matches()) {
          continue;
        }
        rows.add(columnRow(table, field, position));
      }
    }
    return result(
        columns(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE:int",
            "TYPE_NAME",
            "COLUMN_SIZE:int",
            "BUFFER_LENGTH:int",
            "DECIMAL_DIGITS:int",
            "NUM_PREC_RADIX:int",
            "NULLABLE:int",
            "REMARKS",
            "COLUMN_DEF",
            "SQL_DATA_TYPE:int",
            "SQL_DATETIME_SUB:int",
            "CHAR_OCTET_LENGTH:int",
            "ORDINAL_POSITION:int",
            "IS_NULLABLE",
            "SCOPE_CATALOG",
            "SCOPE_SCHEMA",
            "SCOPE_TABLE",
            "SOURCE_DATA_TYPE:short",
            "IS_AUTOINCREMENT",
            "IS_GENERATEDCOLUMN"),
        rows);
  }

  private static Object[] columnRow(Row table, Field field, int position) {
    Map<String, String> metadata = field.getMetadata() == null ? Map.of() : field.getMetadata();
    ArrowType type = field.getType();
    boolean described = SqlTypes.takesPrecisionMetadata(type);
    Integer size = described ? number(metadata.get(SqlTypes.PRECISION)) : null;
    if (size == null) {
      size = SqlTypes.columnSize(type);
      if (type instanceof ArrowType.FloatingPoint) {
        size =
            ((ArrowType.FloatingPoint) type).getPrecision() == FloatingPointPrecision.DOUBLE
                ? 53
                : 24;
      }
    }
    Integer digits = described ? number(metadata.get(SqlTypes.SCALE)) : null;
    if (digits == null) {
      digits = SqlTypes.decimalDigits(type);
    }
    Integer radix =
        SqlTypes.isNumeric(type) ? (type instanceof ArrowType.FloatingPoint ? 2 : 10) : null;
    String autoIncrement = metadata.get(SqlTypes.IS_AUTO_INCREMENT);
    return new Object[] {
      table.text("catalog_name"),
      table.text("db_schema_name"),
      table.text("table_name"),
      field.getName(),
      SqlTypes.jdbcType(field),
      SqlTypes.typeName(field),
      size,
      null,
      digits,
      radix,
      field.isNullable() ? columnNullable : columnNoNulls,
      metadata.get(SqlTypes.REMARKS),
      null,
      null,
      null,
      SqlTypes.isText(type) ? size : null,
      position,
      field.isNullable() ? "YES" : "NO",
      null,
      null,
      null,
      null,
      autoIncrement == null
          ? ""
          : "1".equals(autoIncrement) || "true".equalsIgnoreCase(autoIncrement) ? "YES" : "NO",
      ""
    };
  }

  private static Integer number(String text) {
    try {
      return text == null ? null : Integer.valueOf(text.trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  // ---- Keys ----

  @Override
  public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    for (Row row :
        fetch(
            "primary-keys", parameters("catalog", catalog, "db_schema", schema, "table", table))) {
      rows.add(
          new Object[] {
            row.text("catalog_name"),
            row.text("db_schema_name"),
            row.text("table_name"),
            row.text("column_name"),
            row.number("key_sequence").shortValue(),
            row.text("key_name")
          });
    }
    return result(
        columns(
            "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ:short", "PK_NAME"),
        rows);
  }

  private static final Schema KEY_COLUMNS =
      columns(
          "PKTABLE_CAT",
          "PKTABLE_SCHEM",
          "PKTABLE_NAME",
          "PKCOLUMN_NAME",
          "FKTABLE_CAT",
          "FKTABLE_SCHEM",
          "FKTABLE_NAME",
          "FKCOLUMN_NAME",
          "KEY_SEQ:short",
          "UPDATE_RULE:short",
          "DELETE_RULE:short",
          "FK_NAME",
          "PK_NAME",
          "DEFERRABILITY:short");

  /**
   * Flight SQL's rule codes are JDBC's: cascade 0, restrict 1, set null 2, no action 3, default 4.
   */
  private ResultSet keys(String kind, Map<String, String> parameters) throws SQLException {
    List<Object[]> rows = new ArrayList<>();
    for (Row row : fetch(kind, parameters)) {
      rows.add(
          new Object[] {
            row.text("pk_catalog_name"),
            row.text("pk_db_schema_name"),
            row.text("pk_table_name"),
            row.text("pk_column_name"),
            row.text("fk_catalog_name"),
            row.text("fk_db_schema_name"),
            row.text("fk_table_name"),
            row.text("fk_column_name"),
            row.number("key_sequence").shortValue(),
            row.number("update_rule").shortValue(),
            row.number("delete_rule").shortValue(),
            row.text("fk_key_name"),
            row.text("pk_key_name"),
            (short) importedKeyNotDeferrable
          });
    }
    return result(KEY_COLUMNS, rows);
  }

  @Override
  public ResultSet getImportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return keys(
        "imported-keys", parameters("catalog", catalog, "db_schema", schema, "table", table));
  }

  @Override
  public ResultSet getExportedKeys(String catalog, String schema, String table)
      throws SQLException {
    return keys(
        "exported-keys", parameters("catalog", catalog, "db_schema", schema, "table", table));
  }

  @Override
  public ResultSet getCrossReference(
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    return keys(
        "cross-reference",
        parameters(
            "pk_catalog", parentCatalog,
            "pk_db_schema", parentSchema,
            "pk_table", parentTable,
            "fk_catalog", foreignCatalog,
            "fk_db_schema", foreignSchema,
            "fk_table", foreignTable));
  }

  // ---- Types ----

  private static final Schema TYPE_INFO_COLUMNS =
      columns(
          "TYPE_NAME",
          "DATA_TYPE:int",
          "PRECISION:int",
          "LITERAL_PREFIX",
          "LITERAL_SUFFIX",
          "CREATE_PARAMS",
          "NULLABLE:short",
          "CASE_SENSITIVE:bool",
          "SEARCHABLE:short",
          "UNSIGNED_ATTRIBUTE:bool",
          "FIXED_PREC_SCALE:bool",
          "AUTO_INCREMENT:bool",
          "LOCAL_TYPE_NAME",
          "MINIMUM_SCALE:short",
          "MAXIMUM_SCALE:short",
          "SQL_DATA_TYPE:int",
          "SQL_DATETIME_SUB:int",
          "NUM_PREC_RADIX:int");

  /**
   * {@inheritDoc}
   *
   * <p>From the server's {@code type-info} endpoint (Flight SQL's {@code CommandGetXdbcTypeInfo});
   * empty if the server does not offer it.
   */
  @Override
  public ResultSet getTypeInfo() throws SQLException {
    List<Row> types;
    try {
      types = fetch("type-info", Map.of());
    } catch (SQLException e) {
      String code = WarpProblem.codeOf(e);
      if ("not_found".equals(code) || "not_implemented".equals(code)) {
        return empty(TYPE_INFO_COLUMNS);
      }
      throw e;
    }
    List<Object[]> rows = new ArrayList<>();
    for (Row row : types) {
      Object params = row.get("create_params");
      rows.add(
          new Object[] {
            row.text("type_name"),
            row.number("data_type"),
            row.number("column_size"),
            row.text("literal_prefix"),
            row.text("literal_suffix"),
            params instanceof List ? String.join(",", stringList((List<?>) params)) : null,
            shortOf(row.number("nullable")),
            row.get("case_sensitive"),
            shortOf(row.number("searchable")),
            row.get("unsigned_attribute"),
            row.get("fixed_prec_scale"),
            row.get("auto_increment"),
            row.text("local_type_name"),
            shortOf(row.number("minimum_scale")),
            shortOf(row.number("maximum_scale")),
            row.number("sql_data_type"),
            row.number("datetime_subcode"),
            row.number("num_prec_radix")
          });
    }
    return result(TYPE_INFO_COLUMNS, rows);
  }

  private static List<String> stringList(List<?> values) {
    List<String> strings = new ArrayList<>();
    for (Object value : values) {
      strings.add(String.valueOf(value));
    }
    return strings;
  }

  private static Short shortOf(Integer value) {
    return value == null ? null : value.shortValue();
  }

  // ---- Metadata Warp SQL has no endpoint for ----

  /** {@inheritDoc} Warp SQL has no index metadata, so the result is empty. */
  @Override
  public ResultSet getIndexInfo(
      String catalog, String schema, String table, boolean unique, boolean approximate)
      throws SQLException {
    return empty(
        columns(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "NON_UNIQUE:bool",
            "INDEX_QUALIFIER",
            "INDEX_NAME",
            "TYPE:short",
            "ORDINAL_POSITION:short",
            "COLUMN_NAME",
            "ASC_OR_DESC",
            "CARDINALITY:long",
            "PAGES:long",
            "FILTER_CONDITION"));
  }

  @Override
  public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern)
      throws SQLException {
    return empty(
        columns(
            "PROCEDURE_CAT",
            "PROCEDURE_SCHEM",
            "PROCEDURE_NAME",
            "RESERVED1",
            "RESERVED2",
            "RESERVED3",
            "REMARKS",
            "PROCEDURE_TYPE:short",
            "SPECIFIC_NAME"));
  }

  @Override
  public ResultSet getProcedureColumns(
      String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern)
      throws SQLException {
    return empty(
        columns(
            "PROCEDURE_CAT",
            "PROCEDURE_SCHEM",
            "PROCEDURE_NAME",
            "COLUMN_NAME",
            "COLUMN_TYPE:short",
            "DATA_TYPE:int",
            "TYPE_NAME",
            "PRECISION:int",
            "LENGTH:int",
            "SCALE:short",
            "RADIX:short",
            "NULLABLE:short",
            "REMARKS",
            "COLUMN_DEF",
            "SQL_DATA_TYPE:int",
            "SQL_DATETIME_SUB:int",
            "CHAR_OCTET_LENGTH:int",
            "ORDINAL_POSITION:int",
            "IS_NULLABLE",
            "SPECIFIC_NAME"));
  }

  @Override
  public ResultSet getColumnPrivileges(
      String catalog, String schema, String table, String columnNamePattern) throws SQLException {
    return empty(
        columns(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "GRANTOR",
            "GRANTEE",
            "PRIVILEGE",
            "IS_GRANTABLE"));
  }

  @Override
  public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return empty(
        columns(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "GRANTOR",
            "GRANTEE",
            "PRIVILEGE",
            "IS_GRANTABLE"));
  }

  @Override
  public ResultSet getBestRowIdentifier(
      String catalog, String schema, String table, int scope, boolean nullable)
      throws SQLException {
    return empty(rowIdentifierColumns());
  }

  @Override
  public ResultSet getVersionColumns(String catalog, String schema, String table)
      throws SQLException {
    return empty(rowIdentifierColumns());
  }

  private static Schema rowIdentifierColumns() {
    return columns(
        "SCOPE:short",
        "COLUMN_NAME",
        "DATA_TYPE:int",
        "TYPE_NAME",
        "COLUMN_SIZE:int",
        "BUFFER_LENGTH:int",
        "DECIMAL_DIGITS:short",
        "PSEUDO_COLUMN:short");
  }

  @Override
  public ResultSet getUDTs(
      String catalog, String schemaPattern, String typeNamePattern, int[] types)
      throws SQLException {
    return empty(
        columns(
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "CLASS_NAME",
            "DATA_TYPE:int",
            "REMARKS",
            "BASE_TYPE:short"));
  }

  @Override
  public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern)
      throws SQLException {
    return empty(
        columns(
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "SUPERTYPE_CAT",
            "SUPERTYPE_SCHEM",
            "SUPERTYPE_NAME"));
  }

  @Override
  public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern)
      throws SQLException {
    return empty(columns("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "SUPERTABLE_NAME"));
  }

  @Override
  public ResultSet getAttributes(
      String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern)
      throws SQLException {
    return empty(
        columns(
            "TYPE_CAT",
            "TYPE_SCHEM",
            "TYPE_NAME",
            "ATTR_NAME",
            "DATA_TYPE:int",
            "ATTR_TYPE_NAME",
            "ATTR_SIZE:int",
            "DECIMAL_DIGITS:int",
            "NUM_PREC_RADIX:int",
            "NULLABLE:int",
            "REMARKS",
            "ATTR_DEF",
            "SQL_DATA_TYPE:int",
            "SQL_DATETIME_SUB:int",
            "CHAR_OCTET_LENGTH:int",
            "ORDINAL_POSITION:int",
            "IS_NULLABLE",
            "SCOPE_CATALOG",
            "SCOPE_SCHEMA",
            "SCOPE_TABLE",
            "SOURCE_DATA_TYPE:short"));
  }

  @Override
  public ResultSet getClientInfoProperties() throws SQLException {
    return empty(columns("NAME", "MAX_LEN:int", "DEFAULT_VALUE", "DESCRIPTION"));
  }

  @Override
  public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
      throws SQLException {
    return empty(
        columns(
            "FUNCTION_CAT",
            "FUNCTION_SCHEM",
            "FUNCTION_NAME",
            "REMARKS",
            "FUNCTION_TYPE:short",
            "SPECIFIC_NAME"));
  }

  @Override
  public ResultSet getFunctionColumns(
      String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
      throws SQLException {
    return empty(
        columns(
            "FUNCTION_CAT",
            "FUNCTION_SCHEM",
            "FUNCTION_NAME",
            "COLUMN_NAME",
            "COLUMN_TYPE:short",
            "DATA_TYPE:int",
            "TYPE_NAME",
            "PRECISION:int",
            "LENGTH:int",
            "SCALE:short",
            "RADIX:short",
            "NULLABLE:short",
            "REMARKS",
            "CHAR_OCTET_LENGTH:int",
            "ORDINAL_POSITION:int",
            "IS_NULLABLE",
            "SPECIFIC_NAME"));
  }

  @Override
  public ResultSet getPseudoColumns(
      String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws SQLException {
    return empty(
        columns(
            "TABLE_CAT",
            "TABLE_SCHEM",
            "TABLE_NAME",
            "COLUMN_NAME",
            "DATA_TYPE:int",
            "COLUMN_SIZE:int",
            "DECIMAL_DIGITS:int",
            "NUM_PREC_RADIX:int",
            "COLUMN_USAGE",
            "REMARKS",
            "CHAR_OCTET_LENGTH:int",
            "IS_NULLABLE"));
  }

  // ---- Product and driver ----

  @Override
  public String getDatabaseProductName() throws SQLException {
    String discovered = connection.client().serverName();
    return infoText(discovered == null ? "Warp SQL" : discovered, "server_name");
  }

  @Override
  public String getDatabaseProductVersion() throws SQLException {
    String discovered = connection.client().serverVersion();
    return infoText(discovered == null ? "" : discovered, "server_version");
  }

  private int productVersionPart(int group) throws SQLException {
    Matcher matcher = VERSION.matcher(getDatabaseProductVersion());
    return matcher.find() ? Integer.parseInt(matcher.group(group)) : 0;
  }

  @Override
  public int getDatabaseMajorVersion() throws SQLException {
    return productVersionPart(1);
  }

  @Override
  public int getDatabaseMinorVersion() throws SQLException {
    return productVersionPart(2);
  }

  @Override
  public String getDriverName() {
    return WarpDriver.NAME;
  }

  @Override
  public String getDriverVersion() {
    return WarpDriver.version();
  }

  @Override
  public int getDriverMajorVersion() {
    return WarpDriver.versionPart(0);
  }

  @Override
  public int getDriverMinorVersion() {
    return WarpDriver.versionPart(1);
  }

  @Override
  public int getJDBCMajorVersion() {
    return 4;
  }

  @Override
  public int getJDBCMinorVersion() {
    return 2;
  }

  @Override
  public String getURL() {
    return WarpUrl.redact(connection.url());
  }

  @Override
  public String getUserName() {
    return connection.user();
  }

  @Override
  public Connection getConnection() {
    return connection;
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return infoFlag(connection.isReadOnly(), "read_only");
  }

  // ---- Identifiers and SQL ----

  @Override
  public String getIdentifierQuoteString() throws SQLException {
    return infoText("\"", "sql_identifier_quote_char");
  }

  @Override
  public String getSQLKeywords() throws SQLException {
    return infoText("", "sql_keywords");
  }

  @Override
  public String getNumericFunctions() throws SQLException {
    return infoText("", "sql_numeric_functions");
  }

  @Override
  public String getStringFunctions() throws SQLException {
    return infoText("", "sql_string_functions");
  }

  @Override
  public String getSystemFunctions() throws SQLException {
    return infoText("", "sql_system_functions");
  }

  @Override
  public String getTimeDateFunctions() throws SQLException {
    return infoText("", "sql_datetime_functions");
  }

  @Override
  public String getSearchStringEscape() throws SQLException {
    return infoText("\\", "sql_search_string_escape");
  }

  @Override
  public String getExtraNameCharacters() throws SQLException {
    return infoText("", "sql_extra_name_characters");
  }

  /**
   * Flight SQL's identifier case: 0 unknown, 1 case-insensitive (stored as written), 2 upper, 3
   * lower. It has no value for case-sensitive, so unquoted identifiers are never reported as such.
   */
  private int identifierCase(String key) throws SQLException {
    return infoNumber(0, key);
  }

  @Override
  public boolean supportsMixedCaseIdentifiers() {
    return false;
  }

  @Override
  public boolean storesUpperCaseIdentifiers() throws SQLException {
    return identifierCase("sql_identifier_case") == 2;
  }

  @Override
  public boolean storesLowerCaseIdentifiers() throws SQLException {
    return identifierCase("sql_identifier_case") == 3;
  }

  @Override
  public boolean storesMixedCaseIdentifiers() throws SQLException {
    return identifierCase("sql_identifier_case") == 1;
  }

  /**
   * Quoted identifiers are case-sensitive, as in standard SQL, unless the server says otherwise.
   */
  @Override
  public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
    return identifierCase("sql_quoted_identifier_case") == 0;
  }

  @Override
  public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
    return identifierCase("sql_quoted_identifier_case") == 2;
  }

  @Override
  public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
    return identifierCase("sql_quoted_identifier_case") == 3;
  }

  @Override
  public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
    return identifierCase("sql_quoted_identifier_case") == 1;
  }

  /** Flight SQL's null ordering: 0 sorted high, 1 sorted low, 2 at start, 3 at end. */
  private int nullOrdering() throws SQLException {
    return infoNumber(0, "sql_null_ordering");
  }

  @Override
  public boolean nullsAreSortedHigh() throws SQLException {
    return nullOrdering() == 0;
  }

  @Override
  public boolean nullsAreSortedLow() throws SQLException {
    return nullOrdering() == 1;
  }

  @Override
  public boolean nullsAreSortedAtStart() throws SQLException {
    return nullOrdering() == 2;
  }

  @Override
  public boolean nullsAreSortedAtEnd() throws SQLException {
    return nullOrdering() == 3;
  }

  @Override
  public String getSchemaTerm() throws SQLException {
    return infoText("schema", "sql_schema_term");
  }

  @Override
  public String getProcedureTerm() throws SQLException {
    return infoText("procedure", "sql_procedure_term");
  }

  @Override
  public String getCatalogTerm() throws SQLException {
    return infoText("catalog", "sql_catalog_term");
  }

  @Override
  public boolean isCatalogAtStart() {
    return true;
  }

  @Override
  public String getCatalogSeparator() {
    return ".";
  }

  // ---- Transactions ----

  @Override
  public boolean supportsTransactions() throws SQLException {
    return infoFlag(
        connection.client().hasCapability("transactions"),
        "sql_transactions_supported",
        "transactions_supported");
  }

  /** Flight SQL's isolation codes 0 to 4, as JDBC's constants. */
  @Override
  public int getDefaultTransactionIsolation() throws SQLException {
    switch (infoNumber(2, "sql_default_transaction_isolation")) {
      case 0:
        return Connection.TRANSACTION_NONE;
      case 1:
        return Connection.TRANSACTION_READ_UNCOMMITTED;
      case 3:
        return Connection.TRANSACTION_REPEATABLE_READ;
      case 4:
        return Connection.TRANSACTION_SERIALIZABLE;
      default:
        return Connection.TRANSACTION_READ_COMMITTED;
    }
  }

  @Override
  public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
    return level == getDefaultTransactionIsolation();
  }

  @Override
  public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
    return supportsTransactions();
  }

  @Override
  public boolean supportsDataManipulationTransactionsOnly() {
    return false;
  }

  @Override
  public boolean dataDefinitionCausesTransactionCommit() {
    return false;
  }

  @Override
  public boolean dataDefinitionIgnoredInTransactions() {
    return false;
  }

  @Override
  public boolean supportsMultipleTransactions() {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossCommit() {
    return true;
  }

  @Override
  public boolean supportsOpenCursorsAcrossRollback() {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossCommit() {
    return true;
  }

  @Override
  public boolean supportsOpenStatementsAcrossRollback() {
    return true;
  }

  @Override
  public boolean supportsSavepoints() {
    return false;
  }

  @Override
  public boolean autoCommitFailureClosesAllResultSets() {
    return false;
  }

  // ---- Result sets ----

  @Override
  public boolean supportsResultSetType(int type) {
    return type == ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public boolean supportsResultSetConcurrency(int type, int concurrency) {
    return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public boolean supportsResultSetHoldability(int holdability) {
    return holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
        || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public int getResultSetHoldability() {
    return ResultSet.HOLD_CURSORS_OVER_COMMIT;
  }

  @Override
  public boolean ownUpdatesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean ownDeletesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean ownInsertsAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersUpdatesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersDeletesAreVisible(int type) {
    return false;
  }

  @Override
  public boolean othersInsertsAreVisible(int type) {
    return false;
  }

  @Override
  public boolean updatesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean deletesAreDetected(int type) {
    return false;
  }

  @Override
  public boolean insertsAreDetected(int type) {
    return false;
  }

  // ---- SQL support ----

  @Override
  public boolean allProceduresAreCallable() {
    return true;
  }

  @Override
  public boolean allTablesAreSelectable() {
    return false;
  }

  @Override
  public boolean usesLocalFiles() {
    return false;
  }

  @Override
  public boolean usesLocalFilePerTable() {
    return false;
  }

  @Override
  public boolean supportsAlterTableWithAddColumn() {
    return true;
  }

  @Override
  public boolean supportsAlterTableWithDropColumn() {
    return true;
  }

  @Override
  public boolean supportsColumnAliasing() {
    return true;
  }

  @Override
  public boolean nullPlusNonNullIsNull() {
    return true;
  }

  @Override
  public boolean supportsConvert() {
    return false;
  }

  @Override
  public boolean supportsConvert(int fromType, int toType) {
    return false;
  }

  @Override
  public boolean supportsTableCorrelationNames() {
    return true;
  }

  @Override
  public boolean supportsDifferentTableCorrelationNames() {
    return false;
  }

  @Override
  public boolean supportsExpressionsInOrderBy() {
    return true;
  }

  @Override
  public boolean supportsOrderByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsGroupBy() {
    return true;
  }

  @Override
  public boolean supportsGroupByUnrelated() {
    return true;
  }

  @Override
  public boolean supportsGroupByBeyondSelect() {
    return true;
  }

  @Override
  public boolean supportsLikeEscapeClause() {
    return true;
  }

  @Override
  public boolean supportsMultipleResultSets() {
    return false;
  }

  @Override
  public boolean supportsNonNullableColumns() {
    return true;
  }

  @Override
  public boolean supportsMinimumSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsCoreSQLGrammar() {
    return true;
  }

  @Override
  public boolean supportsExtendedSQLGrammar() {
    return false;
  }

  @Override
  public boolean supportsANSI92EntryLevelSQL() {
    return true;
  }

  @Override
  public boolean supportsANSI92IntermediateSQL() {
    return false;
  }

  @Override
  public boolean supportsANSI92FullSQL() {
    return false;
  }

  @Override
  public boolean supportsIntegrityEnhancementFacility() {
    return false;
  }

  @Override
  public boolean supportsOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsFullOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsLimitedOuterJoins() {
    return true;
  }

  @Override
  public boolean supportsSchemasInDataManipulation() {
    return true;
  }

  @Override
  public boolean supportsSchemasInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsSchemasInTableDefinitions() {
    return true;
  }

  @Override
  public boolean supportsSchemasInIndexDefinitions() {
    return true;
  }

  @Override
  public boolean supportsSchemasInPrivilegeDefinitions() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInDataManipulation() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInProcedureCalls() {
    return false;
  }

  @Override
  public boolean supportsCatalogsInTableDefinitions() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInIndexDefinitions() {
    return true;
  }

  @Override
  public boolean supportsCatalogsInPrivilegeDefinitions() {
    return true;
  }

  @Override
  public boolean supportsPositionedDelete() {
    return false;
  }

  @Override
  public boolean supportsPositionedUpdate() {
    return false;
  }

  @Override
  public boolean supportsSelectForUpdate() {
    return false;
  }

  @Override
  public boolean supportsStoredProcedures() {
    return false;
  }

  @Override
  public boolean supportsSubqueriesInComparisons() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInExists() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInIns() {
    return true;
  }

  @Override
  public boolean supportsSubqueriesInQuantifieds() {
    return true;
  }

  @Override
  public boolean supportsCorrelatedSubqueries() {
    return true;
  }

  @Override
  public boolean supportsUnion() {
    return true;
  }

  @Override
  public boolean supportsUnionAll() {
    return true;
  }

  @Override
  public boolean supportsBatchUpdates() {
    return true;
  }

  @Override
  public boolean supportsNamedParameters() {
    return false;
  }

  @Override
  public boolean supportsMultipleOpenResults() {
    return false;
  }

  @Override
  public boolean supportsGetGeneratedKeys() {
    return false;
  }

  @Override
  public boolean generatedKeyAlwaysReturned() {
    return false;
  }

  @Override
  public boolean supportsStatementPooling() {
    return false;
  }

  @Override
  public boolean supportsStoredFunctionsUsingCallSyntax() {
    return false;
  }

  @Override
  public boolean locatorsUpdateCopy() {
    return false;
  }

  @Override
  public int getSQLStateType() {
    return sqlStateSQL;
  }

  @Override
  public RowIdLifetime getRowIdLifetime() {
    return RowIdLifetime.ROWID_UNSUPPORTED;
  }

  @Override
  public boolean doesMaxRowSizeIncludeBlobs() {
    return false;
  }

  // ---- Limits: 0 means none or unknown ----

  @Override
  public int getMaxBinaryLiteralLength() throws SQLException {
    return infoNumber(0, "sql_max_binary_literal_length");
  }

  @Override
  public int getMaxCharLiteralLength() throws SQLException {
    return infoNumber(0, "sql_max_char_literal_length");
  }

  @Override
  public int getMaxColumnNameLength() throws SQLException {
    return infoNumber(0, "sql_max_column_name_length");
  }

  @Override
  public int getMaxColumnsInGroupBy() throws SQLException {
    return infoNumber(0, "sql_max_columns_in_group_by");
  }

  @Override
  public int getMaxColumnsInIndex() throws SQLException {
    return infoNumber(0, "sql_max_columns_in_index");
  }

  @Override
  public int getMaxColumnsInOrderBy() throws SQLException {
    return infoNumber(0, "sql_max_columns_in_order_by");
  }

  @Override
  public int getMaxColumnsInSelect() throws SQLException {
    return infoNumber(0, "sql_max_columns_in_select");
  }

  @Override
  public int getMaxColumnsInTable() throws SQLException {
    return infoNumber(0, "sql_max_columns_in_table");
  }

  @Override
  public int getMaxConnections() throws SQLException {
    return infoNumber(0, "sql_max_connections");
  }

  @Override
  public int getMaxCursorNameLength() throws SQLException {
    return infoNumber(0, "sql_max_cursor_name_length");
  }

  @Override
  public int getMaxIndexLength() throws SQLException {
    return infoNumber(0, "sql_max_index_length");
  }

  @Override
  public int getMaxSchemaNameLength() throws SQLException {
    return infoNumber(0, "sql_db_schema_name_length", "sql_max_schema_name_length");
  }

  @Override
  public int getMaxProcedureNameLength() throws SQLException {
    return infoNumber(0, "sql_max_procedure_name_length");
  }

  @Override
  public int getMaxCatalogNameLength() throws SQLException {
    return infoNumber(0, "sql_max_catalog_name_length");
  }

  @Override
  public int getMaxRowSize() throws SQLException {
    return infoNumber(0, "sql_max_row_size");
  }

  @Override
  public int getMaxStatementLength() throws SQLException {
    return infoNumber(0, "sql_max_statement_length");
  }

  @Override
  public int getMaxStatements() throws SQLException {
    return infoNumber(0, "sql_max_statements");
  }

  @Override
  public int getMaxTableNameLength() throws SQLException {
    return infoNumber(0, "sql_max_table_name_length");
  }

  @Override
  public int getMaxTablesInSelect() throws SQLException {
    return infoNumber(0, "sql_max_tables_in_select");
  }

  @Override
  public int getMaxUserNameLength() throws SQLException {
    return infoNumber(0, "sql_max_username_length");
  }

  // ---- Wrapper ----

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    throw new SQLException("Not a wrapper for " + iface.getName(), "HY000");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return iface.isInstance(this);
  }
}
