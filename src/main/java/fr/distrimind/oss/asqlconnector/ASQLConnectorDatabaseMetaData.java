package fr.distrimind.oss.asqlconnector;

import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.MergeCursor;
import fr.distrimind.oss.flexilogxml.common.FlexiLogXML;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ASQLConnectorDatabaseMetaData implements DatabaseMetaData {
	/**
	 * Pattern used to extract a named primary key.
	 */
	protected final static Pattern FK_NAMED_PATTERN =
			Pattern.compile(".* constraint +(.*?) +foreign +key *\\((.*?)\\).*", Pattern.CASE_INSENSITIVE);
	/**
	 * Pattern used to extract column order for an unnamed primary key.
	 */
	protected final static Pattern PK_UNNAMED_PATTERN =
			Pattern.compile(".* primary +key *\\((.*?,+.*?)\\).*", Pattern.CASE_INSENSITIVE);
	/**
	 * Pattern used to extract a named primary key.
	 */
	protected final static Pattern PK_NAMED_PATTERN =
			Pattern.compile(".* constraint +(.*?) +primary +key *\\((.*?)\\).*", Pattern.CASE_INSENSITIVE);
	private final static Map<String, Integer> RULE_MAP = new HashMap<>();
	private static final int SQLITE_DONE = 101;
	private static final String VIEW_TYPE = "VIEW";
	private static final String TABLE_TYPE = "TABLE";
	public static final String SELECT = "select ";
	public static final String PARENTHESIS_END_SQL_QUERY = "');";
	public static final String AS_DT_UNION = " as dt union";

	static {
		RULE_MAP.put("NO ACTION", importedKeyNoAction);
		RULE_MAP.put("CASCADE", importedKeyCascade);
		RULE_MAP.put("RESTRICT", importedKeyRestrict);
		RULE_MAP.put("SET NULL", importedKeySetNull);
		RULE_MAP.put("SET DEFAULT", importedKeySetDefault);
	}

	ASQLConnectorConnection con;
	private PreparedStatement psAttributes;
	private PreparedStatement psBestRowIdentifier;
	private PreparedStatement psCatalogs;
	private PreparedStatement psColumnPrivileges;
	private PreparedStatement psProcedureColumns;
	private PreparedStatement psProcedures;
	private PreparedStatement psSuperTypes;
	private PreparedStatement psTablePrivileges;
	private PreparedStatement psTableTypes;
	private PreparedStatement psUDTs;
	private PreparedStatement psVersionColumns;

	public ASQLConnectorDatabaseMetaData(ASQLConnectorConnection con) {
		this.con = con;
	}

	/**
	 * Adds SQL string quotes to the given string.
	 *
	 * @param tableName The string to quote.
	 * @return The quoted string.
	 */
	private static String quote(String tableName) {
		if (tableName == null) {
			return "null";
		} else {
			return String.format("'%s'", tableName);
		}
	}

	@Override
	public boolean allProceduresAreCallable() {
		return false;
	}

	@Override
	public boolean allTablesAreSelectable() {
		return true;
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
	public boolean deletesAreDetected(int type) {
		return false;
	}

	@Override
	public boolean doesMaxRowSizeIncludeBlobs() {
		return false;
	}

	@Override
	public ResultSet getAttributes(String catalog, String schemaPattern,
								   String typeNamePattern, String attributeNamePattern)
			throws SQLException {
		if (psAttributes == null) {
			psAttributes = con.prepareStatement("select null as TYPE_CAT, null as TYPE_SCHEM, " +
					"null as TYPE_NAME, null as ATTR_NAME, null as DATA_TYPE, " +
					"null as ATTR_TYPE_NAME, null as ATTR_SIZE, null as DECIMAL_DIGITS, " +
					"null as NUM_PREC_RADIX, null as NULLABLE, null as REMARKS, null as ATTR_DEF, " +
					"null as SQL_DATA_TYPE, null as SQL_DATETIME_SUB, null as CHAR_OCTET_LENGTH, " +
					"null as ORDINAL_POSITION, null as IS_NULLABLE, null as SCOPE_CATALOG, " +
					"null as SCOPE_SCHEMA, null as SCOPE_TABLE, null as SOURCE_DATA_TYPE limit 0;");
		}

		return psAttributes.executeQuery();
	}

	@Override
	public ResultSet getBestRowIdentifier(String catalog, String schema,
										  String table, int scope, boolean nullable) throws SQLException {
		if (psBestRowIdentifier == null) {
			psBestRowIdentifier = con.prepareStatement("select null as SCOPE, null as COLUMN_NAME, " +
					"null as DATA_TYPE, null as TYPE_NAME, null as COLUMN_SIZE, " +
					"null as BUFFER_LENGTH, null as DECIMAL_DIGITS, null as PSEUDO_COLUMN limit 0;");
		}

		return psBestRowIdentifier.executeQuery();
	}

	@Override
	public String getCatalogSeparator() {
		return ".";
	}

	@Override
	public String getCatalogTerm() {
		return "catalog";
	}

	@Override
	public ResultSet getCatalogs() throws SQLException {
		if (psCatalogs == null) {
			psCatalogs = con.prepareStatement("select null as TABLE_CAT limit 0;");
		}

		return psCatalogs.executeQuery();
	}

	@Override
	public ResultSet getColumnPrivileges(String c, String s, String t, String colPat) throws SQLException {
		if (psColumnPrivileges == null) {
			psColumnPrivileges = con.prepareStatement("select null as TABLE_CAT, null as TABLE_SCHEM, " +
					"null as TABLE_NAME, null as COLUMN_NAME, null as GRANTOR, null as GRANTEE, " +
					"null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
		}

		return psColumnPrivileges.executeQuery();
	}

	@Override
	@SuppressWarnings({"PMD.UseTryWithResources", "PMD.CloseResource"})
	public ResultSet getColumns(String catalog, String schemaPattern,
								String tableNamePattern, String columnNamePattern) throws SQLException {

		// get the list of tables matching the pattern (getTables)
		// create a Matrix Cursor for each of the tables
		// create a merge cursor from all the Matrix Cursors
		// and return the columname and type from:
		//	"PRAGMA table_info(tablename)"
		// which returns data like this:
		//		sqlite> PRAGMA lastyear.table_info(gross_sales);
		//		cid|name|type|notnull|dflt_value|pk
		//		0|year|INTEGER|0|'2006'|0
		//		1|month|TEXT|0||0
		//		2|monthlygross|REAL|0||0
		//		3|sortcol|INTEGER|0||0
		//		sqlite>

		// and then make the cursor have these columns
		//		TABLE_CAT String => table catalog (may be null)
		//		TABLE_SCHEM String => table schema (may be null)
		//		TABLE_NAME String => table name
		//		COLUMN_NAME String => column name
		//		DATA_TYPE int => SQL type from java.sql.Types
		//		TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
		//		COLUMN_SIZE int => column size.
		//		BUFFER_LENGTH is not used.
		//		DECIMAL_DIGITS int => the number of fractional digits. Null is returned for data types where DECIMAL_DIGITS is not applicable.
		//		NUM_PREC_RADIX int => Radix (typically either 10 or 2)
		//		NULLABLE int => is NULL allowed.
		//		columnNoNulls - might not allow NULL values
		//		columnNullable - definitely allows NULL values
		//		columnNullableUnknown - nullability unknown
		//		REMARKS String => comment describing column (may be null)
		//		COLUMN_DEF String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be null)
		//		SQL_DATA_TYPE int => unused
		//		SQL_DATETIME_SUB int => unused
		//		CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
		//		ORDINAL_POSITION int => index of column in table (starting at 1)
		//		IS_NULLABLE String => ISO rules are used to determine the nullability for a column.
		//		YES --- if the parameter can include NULLs
		//		NO --- if the parameter cannot include NULLs
		//		empty string --- if the nullability for the parameter is unknown
		//		SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
		//		SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
		//		SCOPE_TABLE String => table name that this the scope of a reference attribure (null if the DATA_TYPE isn't REF)
		//		SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null if DATA_TYPE isn't DISTINCT or user-generated REF)
		//		IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
		//		YES --- if the column is auto incremented
		//		NO --- if the column is not auto incremented
		//		empty string --- if it cannot be determined whether the column is auto incremented parameter is unknown
		final String[] columnNames = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME",
				"DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX",
				"NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
				"ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATLOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE",
				"IS_AUTOINCREMENT"};
		final Object[] columnValues = new Object[]{null, null, null, null, null, null, null, null, null, 10,
				2 /* columnNullableUnknown */, null, null, null, null, -1, -1, "",
				null, null, null, null, ""};

		ASQLConnectorDatabase db = con.getDb();
		final String[] types = new String[]{TABLE_TYPE, VIEW_TYPE};
		ResultSet rs = null;
		List<Cursor> cursorList = new ArrayList<>();
		try {
			rs = getTables(catalog, schemaPattern, tableNamePattern, types);
			while (rs.next()) {
				String tableName = rs.getString(3);
				String pragmaStatement = "PRAGMA table_info('" + tableName + "')";   // ?)";  substitutions don't seem to work in a pragma statment...
				try (Cursor c=db.rawQuery(pragmaStatement, new String[]{})){
					MatrixCursor mc = new MatrixCursor(columnNames, c.getCount());
					while (c.moveToNext()) {
						Object[] column = columnValues.clone();
						column[2] = tableName;
						column[3] = c.getString(1);
						String type = c.getString(2);
						column[5] = type;
						type = type.toUpperCase(FlexiLogXML.getLocale());
						// types are (as far as I can tell, the pragma document is not specific):
						if ("TEXT".equals(type) || type.startsWith("CHAR")) {
							column[4] = java.sql.Types.VARCHAR;
						} else if ("NUMERIC".equals(type)) {
							column[4] = java.sql.Types.NUMERIC;
						} else if (type.startsWith("INT")) {
							column[4] = java.sql.Types.INTEGER;
						} else if ("REAL".equals(type)) {
							column[4] = java.sql.Types.REAL;
						} else if ("BLOB".equals(type)) {
							column[4] = java.sql.Types.BLOB;
						} else {  // manufactured columns, eg select 100 as something from tablename, may not have a type.
							column[4] = java.sql.Types.NULL;
						}
						int nullable = c.getInt(3);
						//public static final int columnNoNulls   0
						//public static final int columnNullable  1
						//public static final int columnNullableUnknown   2
						if (nullable == 0) {
							column[10] = 1;
						} else if (nullable == 1) {
							column[10] = 0;
						}
						column[12] = c.getString(4);  // we should check the type for this, but I'm not going to.
						mc.addRow(column);
					}
					cursorList.add(mc);
				} catch (SQLException ignored) {
					// failure of one query will no affect the others...
					// this will already have been printed.  e.printStackTrace();
				}
			}
		} finally {
			if (rs != null) {
				try {
					rs.close();
				} catch (Exception e) {
					Log.error(() -> "Impossible to close result set", e);
				}
			}
		}

		ASQLConnectorResultSet resultSet;
		Cursor[] cursors = new Cursor[cursorList.size()];
		cursors = cursorList.toArray(cursors);

		if (cursors.length == 0) {
			resultSet = new ASQLConnectorResultSet(new MatrixCursor(columnNames, 0));
		} else if (cursors.length == 1) {
			resultSet = new ASQLConnectorResultSet(cursors[0]);
		} else {
			resultSet = new ASQLConnectorResultSet(new MergeCursor(cursors));
		}
		return resultSet;
	}

	@Override
	public Connection getConnection() {
		return con;
	}

	@Override
	public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft) throws SQLException {
		if (pt == null) {
			return getExportedKeys(fc, fs, ft);
		}

		if (ft == null) {
			return getImportedKeys(pc, ps, pt);
		}

		String query = SELECT + quote(pc) + " as PKTABLE_CAT, " +
				quote(ps) + " as PKTABLE_SCHEM, " + quote(pt) + " as PKTABLE_NAME, " +
				"'' as PKCOLUMN_NAME, " + quote(fc) + " as FKTABLE_CAT, " +
				quote(fs) + " as FKTABLE_SCHEM, " + quote(ft) + " as FKTABLE_NAME, " +
				"'' as FKCOLUMN_NAME, -1 as KEY_SEQ, 3 as UPDATE_RULE, 3 as DELETE_RULE, '' as FK_NAME, '' as PK_NAME, " +
				importedKeyInitiallyDeferred + " as DEFERRABILITY limit 0 ";

		return con.createStatement().executeQuery(query);
	}

	@Override
	public int getDatabaseMajorVersion() {
		return con.getDb().getSqliteDatabase().getVersion();
	}

	@Override
	public int getDatabaseMinorVersion() {
		return 0;
	}

	@Override
	public String getDatabaseProductName() {
		return "SQLite for Android";
	}

	@Override
	public String getDatabaseProductVersion() {
		return "";
	}

	@Override
	public int getDefaultTransactionIsolation() {
		return Connection.TRANSACTION_SERIALIZABLE;
	}

	@Override
	public int getDriverMajorVersion() {
		return 1;
	}

	@Override
	public int getDriverMinorVersion() {
		return 1;
	}

	@Override
	public String getDriverName() {
		return "DM-A-SQLConnector";
	}

	@Override
	public String getDriverVersion() {
		return "0.0.1 alpha";
	}

	@Override
	@SuppressWarnings("PMD.UseTryWithResources")
	public ResultSet getExportedKeys(String _catalog, String _schema, String table)
			throws SQLException {
		PrimaryKeyFinder pkFinder = new PrimaryKeyFinder(table);
		List<String> pkColumns = pkFinder.getColumns();
		try(final ASQLConnectorStatement stat = con.createStatement()) {

			String catalog = (_catalog != null) ? quote(_catalog) : null;
			String schema = (_schema != null) ? quote(_schema) : null;

			StringBuilder exportedKeysQuery = new StringBuilder(512);

			int count = 0;
			if (pkColumns != null) {
				// retrieve table list
				ResultSet rs = stat.executeQuery("select name from sqlite_master where type = 'table'");
				List<String> tableList = new ArrayList<>();

				while (rs.next()) {
					tableList.add(rs.getString(1));
				}

				rs.close();

				String target = table.toLowerCase(FlexiLogXML.getLocale());
				// find imported keys for each table
				for (String tbl : tableList) {
					ResultSet fk;
					try {
						fk = stat.executeQuery("pragma foreign_key_list('" + escape(tbl) + "')");
					} catch (SQLException e) {
						if (e.getErrorCode() == SQLITE_DONE)
							continue; // expected if table has no foreign keys

						throw e;
					}

					Statement stat2 = null;
					try {
						stat2 = con.createStatement();

						while (fk.next()) {
							int keySeq = fk.getInt(2) + 1;
							String PKTabName = fk.getString(3);
							if (PKTabName != null)
								PKTabName = PKTabName.toLowerCase(FlexiLogXML.getLocale());

							if (PKTabName == null || !PKTabName.equals(target)) {
								continue;
							}

							String PKColName = fk.getString(5);
							PKColName = (PKColName == null) ? pkColumns.get(0) : PKColName.toLowerCase(FlexiLogXML.getLocale());

							exportedKeysQuery
									.append(count > 0 ? " union all select " : SELECT)
									.append(keySeq).append(" as ks, lower('")
									.append(escape(tbl)).append("') as fkt, lower('")
									.append(escape(fk.getString(4))).append("') as fcn, '")
									.append(escape(PKColName)).append("' as pcn, ")
									.append(RULE_MAP.get(fk.getString(6))).append(" as ur, ")
									.append(RULE_MAP.get(fk.getString(7))).append(" as dr, ");

							rs = stat2.executeQuery("select sql from sqlite_master where" +
									" lower(name) = lower('" + escape(tbl) + "')");

							if (rs.next()) {
								Matcher matcher = FK_NAMED_PATTERN.matcher(rs.getString(1));

								if (matcher.find()) {
									exportedKeysQuery.append("'").append(escape(Objects.requireNonNull(matcher.group(1)).toLowerCase(FlexiLogXML.getLocale()))).append("' as fkn");
								} else {
									exportedKeysQuery.append("'' as fkn");
								}
							}

							rs.close();
							count++;
						}
					} finally {
						try {
							if (rs != null) rs.close();
						} catch (SQLException ignored) {
						}
						try {
							if (stat2 != null) stat2.close();
						} catch (SQLException ignored) {
						}
						try {
							if (fk != null) fk.close();
						} catch (SQLException ignored) {
						}
					}
				}
			}

			boolean hasImportedKey = (count > 0);
			StringBuilder sql = new StringBuilder(512);
			sql.append(SELECT)
					.append(catalog).append(" as PKTABLE_CAT, ")
					.append(schema).append(" as PKTABLE_SCHEM, ")
					.append(quote(table)).append(" as PKTABLE_NAME, ")
					.append(hasImportedKey ? "pcn" : "''").append(" as PKCOLUMN_NAME, ")
					.append(catalog).append(" as FKTABLE_CAT, ")
					.append(schema).append(" as FKTABLE_SCHEM, ")
					.append(hasImportedKey ? "fkt" : "''").append(" as FKTABLE_NAME, ")
					.append(hasImportedKey ? "fcn" : "''").append(" as FKCOLUMN_NAME, ")
					.append(hasImportedKey ? "ks" : "-1").append(" as KEY_SEQ, ")
					.append(hasImportedKey ? "ur" : "3").append(" as UPDATE_RULE, ")
					.append(hasImportedKey ? "dr" : "3").append(" as DELETE_RULE, ")
					.append(hasImportedKey ? "fkn" : "''").append(" as FK_NAME, ")
					.append(pkFinder.getName() != null ? pkFinder.getName() : "''").append(" as PK_NAME, ")
					.append(importedKeyInitiallyDeferred) // FIXME: Check for pragma foreign_keys = true ?
					.append(" as DEFERRABILITY ");

			if (hasImportedKey) {
				sql.append("from (").append(exportedKeysQuery).append(") order by fkt");
			} else {
				sql.append("limit 0");
			}

			ResultSet rs = stat.executeQuery(sql.toString());
			stat.freeResultSetWithoutClosingIt();
			return rs;
		}
	}

	@Override
	public String getExtraNameCharacters()  {
		return "";
	}

	@Override
	public String getIdentifierQuoteString() {
		return " ";
	}

	@Override
	@SuppressWarnings("PMD.CheckResultSet")
	public ResultSet getImportedKeys(String catalog, String schema, String table)
			throws SQLException {

		try(ASQLConnectorStatement stat = con.createStatement()) {
			StringBuilder sql = new StringBuilder(700);

			sql.append(SELECT).append(quote(catalog)).append(" as PKTABLE_CAT, ")
					.append(quote(schema)).append(" as PKTABLE_SCHEM, ")
					.append("ptn as PKTABLE_NAME, pcn as PKCOLUMN_NAME, ")
					.append(quote(catalog)).append(" as FKTABLE_CAT, ")
					.append(quote(schema)).append(" as FKTABLE_SCHEM, ")
					.append(quote(table)).append(" as FKTABLE_NAME, ")
					.append("fcn as FKCOLUMN_NAME, ks as KEY_SEQ, ur as UPDATE_RULE, dr as DELETE_RULE, '' as FK_NAME, '' as PK_NAME, ")
					.append(importedKeyInitiallyDeferred).append(" as DEFERRABILITY from (");

			// Use a try catch block to avoid "query does not return ResultSet" error
			ResultSet rs;
			try {
				rs = stat.executeQuery("pragma foreign_key_list('" + escape(table) + PARENTHESIS_END_SQL_QUERY);
			} catch (SQLException e) {
				sql.append("select -1 as ks, '' as ptn, '' as fcn, '' as pcn, ")
						.append(importedKeyNoAction).append(" as ur, ")
						.append(importedKeyNoAction).append(" as dr) limit 0;");

				return stat.executeQuery(sql.toString());
			}

			boolean rsHasNext = false;

			for (int i = 0; rs.next(); i++) {
				rsHasNext = true;
				int keySeq = rs.getInt(2) + 1;
				String PKTabName = rs.getString(3);
				String FKColName = rs.getString(4);
				String PKColName = rs.getString(5);

				if (PKColName == null) {
					PKColName = new PrimaryKeyFinder(PKTabName).getColumns().get(0);
				}

				String updateRule = rs.getString(6);
				String deleteRule = rs.getString(7);

				if (i > 0) {
					sql.append(" union all ");
				}

				sql.append(SELECT).append(keySeq).append(" as ks,")
						.append("'").append(escape(PKTabName)).append("' as ptn, '")
						.append(escape(FKColName)).append("' as fcn, '")
						.append(escape(PKColName)).append("' as pcn,")
						.append("case '").append(escape(updateRule)).append("'")
						.append(" when 'NO ACTION' then ").append(importedKeyNoAction)
						.append(" when 'CASCADE' then ").append(importedKeyCascade)
						.append(" when 'RESTRICT' then ").append(importedKeyRestrict)
						.append(" when 'SET NULL' then ").append(importedKeySetNull)
						.append(" when 'SET DEFAULT' then ").append(importedKeySetDefault).append(" end as ur, ")
						.append("case '").append(escape(deleteRule)).append("'")
						.append(" when 'NO ACTION' then ").append(importedKeyNoAction)
						.append(" when 'CASCADE' then ").append(importedKeyCascade)
						.append(" when 'RESTRICT' then ").append(importedKeyRestrict)
						.append(" when 'SET NULL' then ").append(importedKeySetNull)
						.append(" when 'SET DEFAULT' then ").append(importedKeySetDefault).append(" end as dr");
			}
			rs.close();

			if (!rsHasNext) {
				sql.append("select -1 as ks, '' as ptn, '' as fcn, '' as pcn, ")
						.append(importedKeyNoAction).append(" as ur, ")
						.append(importedKeyNoAction).append(" as dr) limit 0;");
			}

			rs = stat.executeQuery(sql.append(");").toString());
			stat.freeResultSetWithoutClosingIt();
			return rs;
		}
	}

	@Override
	public ResultSet getIndexInfo(String catalog, String schema, String table,
								  boolean unique, boolean approximate) throws SQLException {

		try(ASQLConnectorStatement stat = con.createStatement()) {
			StringBuilder sql = new StringBuilder(500);

			sql.append("select null as TABLE_CAT, null as TABLE_SCHEM, '")
					.append(escape(table)).append("' as TABLE_NAME, un as NON_UNIQUE, null as INDEX_QUALIFIER, n as INDEX_NAME, ")
					.append(Integer.toString(tableIndexOther)).append(" as TYPE, op as ORDINAL_POSITION, ")
					.append("cn as COLUMN_NAME, null as ASC_OR_DESC, 0 as CARDINALITY, 0 as PAGES, null as FILTER_CONDITION from (");

			// Use a try catch block to avoid "query does not return ResultSet" error
			ResultSet rs;
			try {
				rs = stat.executeQuery("pragma index_list('" + escape(table) + PARENTHESIS_END_SQL_QUERY);
			} catch (SQLException e) {
				sql.append("select null as un, null as n, null as op, null as cn) limit 0;");

				return stat.executeQuery(sql.toString());
			}

			List<List<Object>> indexList = new ArrayList<>();
			while (rs.next()) {
				indexList.add(new ArrayList<>());
				indexList.get(indexList.size() - 1).add(rs.getString(2));
				indexList.get(indexList.size() - 1).add(rs.getInt(3));
			}
			rs.close();

			int i = 0;
			Iterator<List<Object>> indexIterator = indexList.iterator();
			List<Object> currentIndex;

			while (indexIterator.hasNext()) {
				currentIndex = indexIterator.next();
				String indexName = currentIndex.get(0).toString();
				rs = stat.executeQuery("pragma index_info('" + escape(indexName) + PARENTHESIS_END_SQL_QUERY);

				while (rs.next()) {
					if (i++ > 0) {
						sql.append(" union all ");
					}

					sql.append(SELECT).append(1 - (Integer) currentIndex.get(1)).append(" as un,'")
							.append(escape(indexName)).append("' as n,")
							.append(rs.getInt(1) + 1).append(" as op,'")
							.append(escape(rs.getString(3))).append("' as cn");
				}

				rs.close();
			}

			rs=stat.executeQuery(sql.append(");").toString());
			stat.freeResultSetWithoutClosingIt();
			return rs;
		}
	}

	@Override
	public int getJDBCMajorVersion() {
		return 2;
	}

	@Override
	public int getJDBCMinorVersion() {
		return 1;
	}

	@Override
	public int getMaxBinaryLiteralLength() {
		return 0;
	}

	@Override
	public int getMaxCatalogNameLength() {
		return 0;
	}

	@Override
	public int getMaxCharLiteralLength() {
		return 0;
	}

	@Override
	public int getMaxColumnNameLength() {
		return 0;
	}

	@Override
	public int getMaxColumnsInGroupBy() {
		return 0;
	}

	@Override
	public int getMaxColumnsInIndex() {
		return 0;
	}

	@Override
	public int getMaxColumnsInOrderBy() {
		return 0;
	}

	@Override
	public int getMaxColumnsInSelect() {
		return 0;
	}

	@Override
	public int getMaxColumnsInTable() {
		return 0;
	}

	@Override
	public int getMaxConnections() {
		return 0;
	}

	@Override
	public int getMaxCursorNameLength() {
		return 0;
	}

	@Override
	public int getMaxIndexLength() {
		return 0;
	}

	@Override
	public int getMaxProcedureNameLength() {
		return 0;
	}

	@Override
	public int getMaxRowSize() {
		return 0;
	}

	@Override
	public int getMaxSchemaNameLength() {
		return 0;
	}

	@Override
	public int getMaxStatementLength() {
		return 0;
	}

	@Override
	public int getMaxStatements() {
		return 0;
	}

	@Override
	public int getMaxTableNameLength() {
		return 0;
	}

	@Override
	public int getMaxTablesInSelect() {
		return 0;
	}

	@Override
	public int getMaxUserNameLength() {
		return 0;
	}

	@Override
	public String getNumericFunctions() {
		return "";
	}

	@Override
	public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
		final String[] columnNames = new String[]{"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "KEY_SEQ", "PK_NAME"};
		final Object[] columnValues = new Object[]{null, null, null, null, null, null};
		ASQLConnectorDatabase db = con.getDb();

		try(Cursor c = db.rawQuery("pragma table_info('" + table + "')", new String[]{})) {
			MatrixCursor mc = new MatrixCursor(columnNames);
			while (c.moveToNext()) {
				if (c.getInt(5) > 0) {
					Object[] column = columnValues.clone();
					column[2] = table;
					column[3] = c.getString(1);
					mc.addRow(column);
				}
			}
			// The matrix cursor should be sorted by column name, but isn't
			return new ASQLConnectorResultSet(mc);
		}

	}

	@Override
	public ResultSet getProcedureColumns(String c, String s, String p, String colPat) throws SQLException {
		if (psProcedures == null) {
			psProcedureColumns = con.prepareStatement("select null as PROCEDURE_CAT, " +
					"null as PROCEDURE_SCHEM, null as PROCEDURE_NAME, null as COLUMN_NAME, " +
					"null as COLUMN_TYPE, null as DATA_TYPE, null as TYPE_NAME, null as PRECISION, " +
					"null as LENGTH, null as SCALE, null as RADIX, null as NULLABLE, " +
					"null as REMARKS limit 0;");
		}
		return psProcedureColumns.executeQuery();

	}

	@Override
	public String getProcedureTerm() {
		return "not_implemented";
	}

	@Override
	public ResultSet getProcedures(String catalog, String schemaPattern,
								   String procedureNamePattern) throws SQLException {
		if (psProcedures == null) {
			psProcedures = con.prepareStatement("select null as PROCEDURE_CAT, null as PROCEDURE_SCHEM, " +
					"null as PROCEDURE_NAME, null as UNDEF1, null as UNDEF2, null as UNDEF3, " +
					"null as REMARKS, null as PROCEDURE_TYPE limit 0;");
		}
		return psProcedures.executeQuery();
	}

	@Override
	public int getResultSetHoldability() {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public String getSQLKeywords() {
		return "";
	}

	@Override
	public int getSQLStateType() {
		return sqlStateSQL99;
	}

	@Override
	public String getSchemaTerm() {
		return "schema";
	}

	@Override
	public ResultSet getSchemas() {
		throw new UnsupportedOperationException("getSchemas not supported by SQLite");
	}

	@Override
	public String getSearchStringEscape() {
		return null;
	}

	@Override
	public String getStringFunctions() {
		return "";
	}

	@Override
	public ResultSet getSuperTables(String catalog, String schemaPattern,
									String tableNamePattern) throws SQLException {
		if (psSuperTypes == null) {
			psSuperTypes = con.prepareStatement("select null as TYPE_CAT, null as TYPE_SCHEM, " +
					"null as TYPE_NAME, null as SUPERTYPE_CAT, null as SUPERTYPE_SCHEM, " +
					"null as SUPERTYPE_NAME limit 0;");
		}
		return psSuperTypes.executeQuery();
	}

	@Override
	public ResultSet getSuperTypes(String catalog, String schemaPattern,
								   String typeNamePattern) throws SQLException {
		if (psSuperTypes == null) {
			psSuperTypes = con.prepareStatement("select null as TYPE_CAT, null as TYPE_SCHEM, " +
					"null as TYPE_NAME, null as SUPERTYPE_CAT, null as SUPERTYPE_SCHEM, " +
					"null as SUPERTYPE_NAME limit 0;");
		}
		return psSuperTypes.executeQuery();
	}

	@Override
	public String getSystemFunctions() {
		return "";
	}

	@Override
	public ResultSet getTablePrivileges(String catalog, String schemaPattern,
										String tableNamePattern) throws SQLException {
		if (psTablePrivileges == null) {
			psTablePrivileges = con.prepareStatement("select  null as TABLE_CAT, "
					+ "null as TABLE_SCHEM, null as TABLE_NAME, null as GRANTOR, null "
					+ "GRANTEE,  null as PRIVILEGE, null as IS_GRANTABLE limit 0;");
		}
		return psTablePrivileges.executeQuery();
	}

	@Override
	public ResultSet getTableTypes() throws SQLException {
		checkOpen();
		if (psTableTypes == null) {
			psTableTypes = con.prepareStatement("select 'TABLE' as TABLE_TYPE "
					+ "union select 'VIEW' as TABLE_TYPE;");
		}
		psTableTypes.clearParameters();
		return psTableTypes.executeQuery();
	}

	@Override
	@SuppressWarnings("PMD.CloseResource")
	public ResultSet getTables(String catalog, String schemaPattern,
							   String _tableNamePattern, String[] _types) throws SQLException {
		String tableNamePattern = Objects.requireNonNullElse(_tableNamePattern, "%");
		String[] types;
		if (_types == null) {
			types = new String[]{TABLE_TYPE};
		}
		else
			types=_types;
		//		.tables command from here:
		//			http://www.sqlite.org/sqlite.html
		//
		//	  SELECT name FROM sqlite_master
		//		WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%'
		//		UNION ALL
		//		SELECT name FROM sqlite_temp_master
		//		WHERE type IN ('table','view')
		//		ORDER BY 1

		// Documentation for getTables() mandates a certain format for the returned result set.
		// To make the return here compatible with the standard, the following statement is
		// executed.  Note that the only returned value of any consequence is still the table name
		// but now it's the third column in the result set and all the other columns are present
		// The type, which can be 'view', 'table' (maybe also 'index') is returned as the type.
		// The sort will be wrong if multiple types are selected.  The solution would be to select
		// one time with type = ('table' | 'view' ), etc. but I think these would have to be
		// substituted by hand (that is, I don't think a ? option could be used - but I could be wrong about that.
		final String selectStringStart = "SELECT null AS TABLE_CAT,null AS TABLE_SCHEM, tbl_name as TABLE_NAME, '";
		final String selectStringMiddle = "' as TABLE_TYPE, 'No Comment' as REMARKS, null as TYPE_CAT, null as TYPE_SCHEM, null as TYPE_NAME, null as SELF_REFERENCING_COL_NAME, null as REF_GENERATION" +
				" FROM sqlite_master WHERE tbl_name LIKE ? AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'android_metadata' AND upper(type) = ?" +
				" UNION ALL SELECT null AS TABLE_CAT,null AS TABLE_SCHEM, tbl_name as TABLE_NAME, '";
		final String selectStringEnd = "' as TABLE_TYPE, 'No Comment' as REMARKS, null as TYPE_CAT, null as TYPE_SCHEM, null as TYPE_NAME, null as SELF_REFERENCING_COL_NAME, null as REF_GENERATION" +
				" FROM sqlite_temp_master WHERE tbl_name LIKE ? AND name NOT LIKE 'android_metadata' AND upper(type) = ? ORDER BY 3";

		ASQLConnectorDatabase db = con.getDb();
		List<Cursor> cursorList = new ArrayList<>();
		for (String tableType : types) {
			String selectString = selectStringStart +
					tableType +
					selectStringMiddle +
					tableType +
					selectStringEnd;
			Cursor c = db.rawQuery(selectString, new String[]{
					tableNamePattern, tableType.toUpperCase(FlexiLogXML.getLocale()),
					tableNamePattern, tableType.toUpperCase(FlexiLogXML.getLocale())});
			cursorList.add(c);
		}
		ASQLConnectorResultSet resultSet;
		Cursor[] cursors = new Cursor[cursorList.size()];
		cursors = cursorList.toArray(cursors);

		if (cursors.length == 0) {
			resultSet = null;  // is this a valid return?? I think this can only occur on a SQL exception
		} else if (cursors.length == 1) {
			resultSet = new ASQLConnectorResultSet(cursors[0]);
		} else {
			resultSet = new ASQLConnectorResultSet(new MergeCursor(cursors));
		}
		return resultSet;
	}

	@Override
	public String getTimeDateFunctions() {
		return "";
	}
	@Override
	public ResultSet getTypeInfo() throws SQLException {
		String sql = SELECT
				+ "tn as TYPE_NAME, "
				+ "dt as DATA_TYPE, "
				+ "0 as PRECISION, "
				+ "null as LITERAL_PREFIX, "
				+ "null as LITERAL_SUFFIX, "
				+ "null as CREATE_PARAMS, "
				+ typeNullable + " as NULLABLE, "
				+ "1 as CASE_SENSITIVE, "
				+ typeSearchable + " as SEARCHABLE, "
				+ "0 as UNSIGNED_ATTRIBUTE, "
				+ "0 as FIXED_PREC_SCALE, "
				+ "0 as AUTO_INCREMENT, "
				+ "null as LOCAL_TYPE_NAME, "
				+ "0 as MINIMUM_SCALE, "
				+ "0 as MAXIMUM_SCALE, "
				+ "0 as SQL_DATA_TYPE, "
				+ "0 as SQL_DATETIME_SUB, "
				+ "10 as NUM_PREC_RADIX from ("
				+ "    select 'BLOB' as tn, " + Types.BLOB + AS_DT_UNION
				+ "    select 'NULL' as tn, " + Types.NULL + AS_DT_UNION
				+ "    select 'REAL' as tn, " + Types.REAL + AS_DT_UNION
				+ "    select 'TEXT' as tn, " + Types.VARCHAR + AS_DT_UNION
				+ "    select 'INTEGER' as tn, " + Types.INTEGER + " as dt"
				+ ") order by TYPE_NAME";

		return new ASQLConnectorResultSet(con.getDb().rawQuery(sql, new String[0]));
	}

	@Override
	public ResultSet getUDTs(String catalog, String schemaPattern,
							 String typeNamePattern, int[] types) throws SQLException {
		if (psUDTs == null) {
			psUDTs = con.prepareStatement("select  null as TYPE_CAT, null as TYPE_SCHEM, "
					+ "null as TYPE_NAME,  null as CLASS_NAME,  null as DATA_TYPE, null as REMARKS, "
					+ "null as BASE_TYPE " + "limit 0;");
		}

		psUDTs.clearParameters();
		return psUDTs.executeQuery();
	}

	@Override
	public String getURL() {
		return con.getURL();
	}

	@Override
	public String getUserName() {
		return null;
	}

	@Override
	public ResultSet getVersionColumns(String catalog, String schema,
									   String table) throws SQLException {
		if (psVersionColumns == null) {
			psVersionColumns = con.prepareStatement("select null as SCOPE, null as COLUMN_NAME, "
					+ "null as DATA_TYPE, null as TYPE_NAME, null as COLUMN_SIZE, "
					+ "null as BUFFER_LENGTH, null as DECIMAL_DIGITS, null as PSEUDO_COLUMN limit 0;");
		}
		return psVersionColumns.executeQuery();
	}

	@Override
	public boolean insertsAreDetected(int type) {
		return false;
	}

	@Override
	public boolean isCatalogAtStart() {
		return true;
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		return con.isReadOnly();
	}

	@Override
	public boolean locatorsUpdateCopy() {
		return false;
	}

	@Override
	public boolean nullPlusNonNullIsNull() {
		return true;
	}

	@Override
	public boolean nullsAreSortedAtEnd() {
		return !nullsAreSortedAtStart();
	}

	@Override
	public boolean nullsAreSortedAtStart() {
		return true;
	}

	@Override
	public boolean nullsAreSortedHigh() {
		return true;
	}

	@Override
	public boolean nullsAreSortedLow() {
		return !nullsAreSortedHigh();
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
	public boolean othersUpdatesAreVisible(int type) {
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
	public boolean ownUpdatesAreVisible(int type) {
		return false;
	}

	@Override
	public boolean storesLowerCaseIdentifiers() {
		return false;
	}

	@Override
	public boolean storesLowerCaseQuotedIdentifiers() {
		return false;
	}

	@Override
	public boolean storesMixedCaseIdentifiers() {
		return true;
	}

	@Override
	public boolean storesMixedCaseQuotedIdentifiers() {
		return false;
	}

	@Override
	public boolean storesUpperCaseIdentifiers() {
		return false;
	}

	@Override
	public boolean storesUpperCaseQuotedIdentifiers() {
		return false;
	}

	@Override
	public boolean supportsANSI92EntryLevelSQL() {
		return false;
	}

	@Override
	public boolean supportsANSI92FullSQL() {
		return false;
	}

	@Override
	public boolean supportsANSI92IntermediateSQL() {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithAddColumn() {
		return false;
	}

	@Override
	public boolean supportsAlterTableWithDropColumn() {
		return false;
	}

	@Override
	public boolean supportsBatchUpdates() {
		return true;
	}

	@Override
	public boolean supportsCatalogsInDataManipulation() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInIndexDefinitions() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInPrivilegeDefinitions() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInProcedureCalls() {
		return false;
	}

	@Override
	public boolean supportsCatalogsInTableDefinitions() {
		return false;
	}

	@Override
	public boolean supportsColumnAliasing() {
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
	public boolean supportsCoreSQLGrammar() {
		return true;
	}

	@Override
	public boolean supportsCorrelatedSubqueries() {
		return false;
	}

	@Override
	public boolean supportsDataDefinitionAndDataManipulationTransactions() {
		return true;
	}

	@Override
	public boolean supportsDataManipulationTransactionsOnly() {
		return false;
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
	public boolean supportsExtendedSQLGrammar() {
		return false;
	}

	@Override
	public boolean supportsFullOuterJoins() {
		return false;
	}

	@Override
	public boolean supportsGetGeneratedKeys() {
		return true;
	}

	@Override
	public boolean supportsGroupBy() {
		return true;
	}

	@Override
	public boolean supportsGroupByBeyondSelect() {
		return false;
	}

	@Override
	public boolean supportsGroupByUnrelated() {
		return false;
	}

	@Override
	public boolean supportsIntegrityEnhancementFacility() {
		return false;
	}

	@Override
	public boolean supportsLikeEscapeClause() {
		return false;
	}

	@Override
	public boolean supportsLimitedOuterJoins() {
		return true;
	}

	@Override
	public boolean supportsMinimumSQLGrammar() {
		return true;
	}

	@Override
	public boolean supportsMixedCaseIdentifiers() {
		return true;
	}

	@Override
	public boolean supportsMixedCaseQuotedIdentifiers() {
		return false;
	}

	@Override
	public boolean supportsMultipleOpenResults() {
		return false;
	}

	@Override
	public boolean supportsMultipleResultSets() {
		return false;
	}

	@Override
	public boolean supportsMultipleTransactions() {
		return true;
	}

	@Override
	public boolean supportsNamedParameters() {
		return true;
	}

	@Override
	public boolean supportsNonNullableColumns() {
		return true;
	}

	@Override
	public boolean supportsOpenCursorsAcrossCommit() {
		return false;
	}

	@Override
	public boolean supportsOpenCursorsAcrossRollback() {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossCommit() {
		return false;
	}

	@Override
	public boolean supportsOpenStatementsAcrossRollback() {
		return false;
	}

	@Override
	public boolean supportsOrderByUnrelated() {
		return false;
	}

	@Override
	public boolean supportsOuterJoins() {
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
	public boolean supportsResultSetConcurrency(int t, int c) {
		return t == ResultSet.TYPE_FORWARD_ONLY && c == ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean supportsResultSetHoldability(int h) {
		return h == ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public boolean supportsResultSetType(int t) {
		return t == ResultSet.TYPE_FORWARD_ONLY;
	}

	@Override
	public boolean supportsSavepoints() {
		return false;
	}

	@Override
	public boolean supportsSchemasInDataManipulation() {
		return false;
	}

	@Override
	public boolean supportsSchemasInIndexDefinitions() {
		return false;
	}

	@Override
	public boolean supportsSchemasInPrivilegeDefinitions() {
		return false;
	}

	@Override
	public boolean supportsSchemasInProcedureCalls() {
		return false;
	}

	@Override
	public boolean supportsSchemasInTableDefinitions() {
		return false;
	}

	@Override
	public boolean supportsSelectForUpdate() {
		return false;
	}

	@Override
	public boolean supportsStatementPooling() {
		return false;
	}

	@Override
	public boolean supportsStoredProcedures() {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInComparisons() {
		return false;
	}

	@Override
	public boolean supportsSubqueriesInExists() {
		return true;
	} // TODO: check

	@Override
	public boolean supportsSubqueriesInIns() {
		return true;
	} // TODO: check

	@Override
	public boolean supportsSubqueriesInQuantifieds() {
		return false;
	}

	@Override
	public boolean supportsTableCorrelationNames() {
		return false;
	}

	@Override
	public boolean supportsTransactionIsolationLevel(int level) {
		return level == Connection.TRANSACTION_SERIALIZABLE;
	}

	@Override
	public boolean supportsTransactions() {
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
	public boolean updatesAreDetected(int type) {
		return false;
	}

	@Override
	public boolean usesLocalFilePerTable() {
		return false;
	}

	@Override
	public boolean usesLocalFiles() {
		return true;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return iface != null && iface.isAssignableFrom(getClass());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		if (isWrapperFor(iface)) {
			return (T) this;
		}
		throw new SQLException(getClass() + " does not wrap " + iface);
	}

	@Override
	public boolean autoCommitFailureClosesAllResultSets() {
		// TODO Evaluate if this is a sufficient implementation (if so, remove this comment)
		return false;
	}

	@Override
	public ResultSet getClientInfoProperties() {
		// TODO Evaluate if this is a sufficient implementation (if so, remove this comment)
		return null;
	}

	@Override
	public ResultSet getFunctionColumns(String catalog, String schemaPattern,
										String functionNamePattern, String columnNamePattern) {
		throw new UnsupportedOperationException("getFunctionColumns not supported");
	}

	@Override
	public ResultSet getFunctions(String catalog, String schemaPattern,
								  String functionNamePattern) {
		throw new UnsupportedOperationException("getFunctions not implemented yet");
	}

	@Override
	public RowIdLifetime getRowIdLifetime() {
		return RowIdLifetime.ROWID_UNSUPPORTED;
	}

	@Override
	public ResultSet getSchemas(String catalog, String schemaPattern) {
		throw new UnsupportedOperationException("getSchemas not implemented yet");
	}

	// methods added for JDK7 compilation

	@Override
	public boolean supportsStoredFunctionsUsingCallSyntax() {
		// TODO Evaluate if this is a sufficient implementation (if so, remove this comment)
		return false;
	}
	@SuppressWarnings("PMD.MissingOverride")
	public boolean generatedKeyAlwaysReturned() {
		throw new UnsupportedOperationException("generatedKeyAlwaysReturned not implemented yet");
	}
	@SuppressWarnings("PMD.MissingOverride")
	public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) {
		throw new UnsupportedOperationException("getPseudoColumns not implemented yet");
	}

	/**
	 * Applies SQL escapes for special characters in a given string.
	 *
	 * @param val The string to escape.
	 * @return The SQL escaped string.
	 */
	private String escape(final String val) {
		// TODO: this function is ugly, pass this work off to SQLite, then we
		//       don't have to worry about Unicode 4, other characters needing
		//       escaping, etc.
		int len = val.length();
		StringBuilder buf = new StringBuilder(len);
		for (int i = 0; i < len; i++) {
			if (val.charAt(i) == '\'') {
				buf.append('\'');
			}
			buf.append(val.charAt(i));
		}
		return buf.toString();
	}

	/**
	 * @throws SQLException if a problem occurs
	 */
	private void checkOpen() throws SQLException {
		if (con == null) {
			throw new SQLException("connection closed");
		}
	}

	/**
	 * Parses the sqlite_master table for a table's primary key
	 */
	class PrimaryKeyFinder {
		/**
		 * The table name.
		 */
		String table;

		/**
		 * The primary key name.
		 */
		String pkName = null;

		/**
		 * The column(s) for the primary key.
		 */
		List<String> pkColumns = null;
		private void initPkColumns(String ...columns)
		{
			List<String> pkColumns=new ArrayList<>(columns.length);
			for (String c : columns)
			{
				pkColumns.add(c.toLowerCase(FlexiLogXML.getLocale()).trim());
			}
			this.pkColumns=Collections.unmodifiableList(pkColumns);
		}
		/**
		 * Constructor.
		 *
		 * @param table The table for which to get find a primary key.
		 * @throws SQLException if a problem occurs
		 */
		@SuppressWarnings("PMD.UseTryWithResources")
		public PrimaryKeyFinder(String table) throws SQLException {
			this.table = table;

			if (table == null || table.trim().isEmpty()) {
				throw new SQLException("Invalid table name: '" + this.table + "'");
			}

			try(Statement stat = con.createStatement()) {

				// read create SQL script for table
				try(ResultSet rs = stat.executeQuery("select sql from sqlite_master where" +
						" lower(name) = lower('" + escape(table) + "') and type = 'table'")) {

					if (!rs.next())
						throw new SQLException("Table not found: '" + table + "'");

					Matcher matcher = PK_NAMED_PATTERN.matcher(rs.getString(1));
					if (matcher.find()) {
						pkName = '\'' + escape(Objects.requireNonNull(matcher.group(1)).toLowerCase(FlexiLogXML.getLocale())) + '\'';
						initPkColumns(Objects.requireNonNull(matcher.group(2)).split(","));
					} else {
						matcher = PK_UNNAMED_PATTERN.matcher(rs.getString(1));
						if (matcher.find()) {
							initPkColumns(Objects.requireNonNull(matcher.group(1)).split(","));
						}
					}
				}


			}
			if (pkColumns == null) {
				try (Statement stat = con.createStatement()) {
					try (ResultSet rs = stat.executeQuery("pragma table_info('" + escape(table) + PARENTHESIS_END_SQL_QUERY)) {
						while (rs.next()) {
							if (rs.getBoolean(6))
								initPkColumns(rs.getString(2));
						}
					}
				}
			}
		}

		/**
		 * @return The primary key name if any.
		 */
		public String getName() {
			return pkName;
		}

		/**
		 * @return List of primary key column(s) if any.
		 */
		public List<String> getColumns() {
			return pkColumns;
		}
	}


}
