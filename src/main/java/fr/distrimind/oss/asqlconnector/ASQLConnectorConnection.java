package fr.distrimind.oss.asqlconnector;

import fr.distrimind.oss.flexilogxml.common.ReflectionTools;

import java.io.File;
import java.lang.reflect.Constructor;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ASQLConnectorConnection implements Connection {
	/**
	 * A map to a single instance of a SQLiteDatabase per DB.
	 */
	private static final Map<String, ASQLConnectorDatabase> dbMap =
			new HashMap<>();

	/**
	 * A map from a connection to a SQLiteDatabase instance.
	 * Used to track the use of each instance, and close the database when last conneciton is closed.
	 */
	private static final Map<ASQLConnectorConnection, ASQLConnectorDatabase> clientMap =
			new HashMap<>();
	public static final String BACK_SLASH = " \"";
	public static final String BACK_SLASH2 = "\" ";
	/**
	 * Will have the value 9 or greater the version of SQLException has the constructor:
	 * SQLException(Throwable theCause) otherwise false.
	 * API levels 9 or greater have this constructor.
	 * If the value is positive and less than 9 then the SQLException does not have the constructor.
	 * If the value is &lt; 0 then the capabilities of SQLException have not been determined.
	 */
	protected static int sqlThrowable = -1;
	private final String url;
	/**
	 * The Android sqlitedb.
	 */
	private ASQLConnectorDatabase sqlitedb;
	private boolean autoCommit = true;
	/**
	 * A cached prepare statement for the count of changed rows
	 */
	private PreparedStatement changedRowsCountStatement = null;
	/**
	 * A cached prepare statement for the last row id generated by the database
	 */
	private PreparedStatement generatedRowIdStatement = null;
	private int transactionIsolation = TRANSACTION_SERIALIZABLE;

	/**
	 * Connect to the database with the given url and properties.
	 *
	 * @param url  the URL string, typically something like
	 *             "jdbc:sqlite:/data/data/your-package/databasefilename" so for example:
	 *             "jdbc:sqlite:/data/data/fr.distrimind.oss.asqlconnector.examples/databases/sqlite.db"
	 * @param info Properties object with options.  Supported options are "timeout", "retry", and "shared".
	 */
	public ASQLConnectorConnection(String url, Properties info) throws SQLException {
		Log.trace(() -> "ASQLConnectorConnection: " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
		Log.trace(() -> "New sqlite jdbc from url '" + url + "', " + "'" + info + "'");

		this.url = url;
		// Make a filename from url
		String dbQname;
		if (url.startsWith(ASQLConnectorDriver.xerialPrefix)) {
			dbQname = url.substring(ASQLConnectorDriver.xerialPrefix.length());
		} else {
			// there does not seem to be any possibility of error handling.
			// So we could check that the url starts with ASQLConnectorDriver.aSQLConnectorPrefix
			// but if it doesn't there's nothing we can do (no Exception is specified)
			// so it has to be assumed that the URL is valid when passed to this method.
			dbQname = url.substring(ASQLConnectorDriver.aSQLConnectorPrefix.length());
		}
		long timeout = 0;  // default to no retries to be consistent with other JDBC implemenations.
		long retryInterval = 50; // this was 1000 in the original code.  1 second is too long for each loop.
		int queryPart = dbQname.indexOf('?');

		// if there's a query part, we accept "timeout=xxx" and "retry=yyy"
		if (queryPart > 0) {
			String options = dbQname.substring(queryPart).trim();
			dbQname = dbQname.substring(0, queryPart);
			while (!options.isEmpty()) {
				int optionEnd = options.lastIndexOf('&');
				if (optionEnd == -1) {
					optionEnd = options.length();
				}
				int equals = options.lastIndexOf('=', optionEnd);
				if (equals==-1)
					Log.error(() -> "Error Parsing URL \"" + url);
				String optionName = options.substring(0, equals).trim();
				String optionValueString = options.substring(equals + 1, optionEnd).trim();
				long optionValue;
				try {
					optionValue = Long.parseLong(optionValueString);
					if ("timeout".equals(optionName)) {
						timeout = optionValue;
					} else if ("retry".equals(optionName)) {
						timeout = optionValue;
						retryInterval = optionValue;
					}
					long to=timeout;
					Log.trace(() -> "Timeout: " + to);
				} catch (NumberFormatException nfe) {
					// print and ignore
					Log.error(() -> "Error Parsing URL \"" + url + "\" Timeout String \"" + optionValueString + "\" is not a valid long", nfe);
				}
				if (optionEnd==options.length())
					break;
				options = options.substring(optionEnd + 1);
			}
		}
		String dbn=dbQname;
		Log.trace(() -> "opening database " + dbn);
		ensureDbFileCreation(dbQname);
		int flags = android.database.sqlite.SQLiteDatabase.CREATE_IF_NECESSARY
				| android.database.sqlite.SQLiteDatabase.OPEN_READWRITE
				| android.database.sqlite.SQLiteDatabase.NO_LOCALIZED_COLLATORS;
		if (info != null) {
			if (info.getProperty(ASQLConnectorDriver.DATABASE_FLAGS) != null) {

				try {
					flags = Integer.parseInt(info.getProperty(ASQLConnectorDriver.DATABASE_FLAGS));
				} catch (NumberFormatException nfe) {
					Log.error(() -> "Error Parsing DatabaseFlags \"" + info.getProperty(ASQLConnectorDriver.DATABASE_FLAGS) + " not a number ", nfe);
				}
			} else if (info.getProperty(ASQLConnectorDriver.ADDITONAL_DATABASE_FLAGS) != null) {
				try {
					int extraFlags = Integer.parseInt(info.getProperty(ASQLConnectorDriver.ADDITONAL_DATABASE_FLAGS));
					flags |= extraFlags;
				} catch (NumberFormatException nfe) {
					Log.error(() -> "Error Parsing DatabaseFlags \"" + info.getProperty(ASQLConnectorDriver.ADDITONAL_DATABASE_FLAGS) + " not a number ", nfe);
				}
			}
		}
		synchronized (dbMap) {
			sqlitedb = dbMap.get(dbQname);
			if (sqlitedb == null) {
				Log.info(() -> "ASQLConnectorConnection: " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this + " Opening new database: " + dbn);
				sqlitedb = new ASQLConnectorDatabase(dbQname, timeout, retryInterval, flags);
				dbMap.put(dbQname, sqlitedb);
			}
			clientMap.put(this, sqlitedb);
		}
	}

	/**
	 * This will create and return an exception.  For API levels less than 9 this will return
	 * a ASQLConnectorException, for later APIs it will return a SQLException.
	 */
	@SuppressWarnings("PMD.UnusedAssignment")
	public static SQLException chainException(android.database.SQLException sqlException) {
		if (sqlThrowable < 0 || sqlThrowable >= 9) {
			try {
				sqlThrowable = 9;
				//return new SQLException (sqlException);
				// creating by reflection is significantly slower, but since Exceptions should be unusual
				// this should not be a performance issue.
				final Constructor<?> c = SQLException.class.getDeclaredConstructor(Throwable.class);
				return (SQLException) c.newInstance(sqlException);
			} catch (Exception e) {
				sqlThrowable = 1;
			}
		}
		// if the code above worked correctly, then the exception will have been returned.  Otherwise, we need
		// to go through this clause and create a ASQLConnectorException
		try {
			// avoid a direct reference to the ASQLConnectorException so that app > API level 9 do not need that class.
			final Constructor<?> c = ReflectionTools.getClassLoader().loadClass("fr.distrimind.oss.asqlconnector.ASQLConnectorException").getDeclaredConstructor(android.database.SQLException.class);
			// ASQLConnectorException is an instance of (direct subclass of) SQLException, so the cast below is correct although
			// the instance created will always be a ASQLConnectorException
			return (SQLException) c.newInstance(sqlException);
		} catch (Exception e) {
			return new SQLException("Unable to Chain SQLException " + sqlException.getMessage());
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private void ensureDbFileCreation(String dbQname) throws SQLException {
		File dbFile = new File(dbQname);
		if (dbFile.isDirectory()) {
			throw new SQLException("Can't create " + dbFile + " - it already exists as a directory");
		} else if (dbFile.getParentFile().exists() && !dbFile.getParentFile().isDirectory()) {
			throw new SQLException("Can't create " + dbFile + " - it because " + dbFile.getParent() + " exists as a regular file");
		} else if (!dbFile.getParentFile().exists()) {
			dbFile.getParentFile().mkdirs();
			if (!dbFile.getParentFile().isDirectory()) {
				throw new SQLException("Could not create " + dbFile.getParent() + " as parent directory for " + dbFile);
			}
		}
	}

	/**
	 * Returns the delegate SQLiteDatabase.
	 */
	public ASQLConnectorDatabase getDb() {
		return sqlitedb;
	}

	@Override
	public void clearWarnings() {
	}

	@Override
	public void close() throws SQLException {
		Log.trace(() -> "ASQLConnectorConnection.close(): " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
		if (sqlitedb != null) {
			synchronized (dbMap) {
				clientMap.remove(this);
				if (!clientMap.containsValue(sqlitedb)) {
					Log.info(() -> "ASQLConnectorConnection.close(): " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this + " Closing the database since since last connection was closed.");
					setAutoCommit(true);
					sqlitedb.close();
					dbMap.remove(sqlitedb.dbQname);
				}
			}
			sqlitedb = null;
		} else {
			Log.error(() -> "ASQLConnectorConnection.close(): " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this + " Duplicate close!");
		}
	}

	@Override
	public void commit() throws SQLException {
		if (autoCommit) {
			throw new SQLException("database in auto-commit mode");
		}
		sqlitedb.setTransactionSuccessful();
		Log.debug(() -> "END TRANSACTION  (commit) " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
		sqlitedb.endTransaction();
		Log.debug(() -> "BEGIN TRANSACTION (after commit) " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
		sqlitedb.beginTransaction();
	}

	@Override
	public ASQLConnectorStatement createStatement() {
		return new ASQLConnectorStatement(this);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
		return createStatement(resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLFeatureNotSupportedException("createStatement supported with TYPE_FORWARD_ONLY");
		}
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new SQLFeatureNotSupportedException("createStatement supported with CONCUR_READ_ONLY");
		}
		if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
			throw new SQLFeatureNotSupportedException("createStatement supported with CLOSE_CURSORS_AT_COMMIT");
		}
		return createStatement();
	}

	@Override
	public boolean getAutoCommit() {
		return autoCommit;
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		if (this.autoCommit == autoCommit) {
			return;
		}
		this.autoCommit = autoCommit;
		if (autoCommit) {
			if (sqlitedb.inTransaction()) { // to be on safe side.
				sqlitedb.setTransactionSuccessful();
				Log.debug(() -> "END TRANSACTION (autocommit on) " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
				sqlitedb.endTransaction();
			}
		} else {
			Log.debug(() -> "BEGIN TRANSACTION (autocommit off) " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
			sqlitedb.beginTransaction();
		}
	}

	@Override
	public String getCatalog() {
		return null;
	}

	@Override
	public void setCatalog(String catalog) {
		// From spec:
		// If the driver does not support catalogs, it will silently ignore this request.
	}

	@Override
	public int getHoldability() {
		return ResultSet.CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
			throw new SQLException("DM-A-SQLConnector only supports CLOSE_CURSORS_AT_COMMIT");
	}

	@Override
	public DatabaseMetaData getMetaData() {
		return new ASQLConnectorDatabaseMetaData(this);
	}

	@SuppressWarnings("MagicConstant")
	@Override
	public int getTransactionIsolation() {
		return transactionIsolation;
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		// TODO: Xerial implements this with PRAGMA read_uncommitted
		if (level != TRANSACTION_SERIALIZABLE) {
			throw new SQLException("DM-A-SQLConnector supports only TRANSACTION_SERIALIZABLE.");
		}
		transactionIsolation = level;
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		throw new SQLFeatureNotSupportedException("getTypeMap not supported");
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException("setTypeMap not supported");
	}

	@Override
	public SQLWarning getWarnings() {
		// TODO: Is this a sufficient implementation? (If so, delete comment and logging)
		Log.error(() -> " ********************* not implemented @ " + DebugPrinter.getFileName() + " line "
				+ DebugPrinter.getLineNumber());
		return null;
	}

	@Override
	public boolean isClosed() {
		// assuming that "isOpen" doesn't throw a locked exception..
		if (sqlitedb != null && sqlitedb.getSqliteDatabase() != null) {
			return !sqlitedb.getSqliteDatabase().isOpen();
		}
		return true;
	}

	@Override
	public boolean isReadOnly() {
		return false;
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		if (readOnly != isReadOnly()) {
			throw new SQLException("Cannot change read-only flag after establishing a connection");
		}
	}

	@Override
	public String nativeSQL(String sql) {
		return sql;
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		return prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
				ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return prepareCall(sql, resultSetType, resultSetConcurrency,
				ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency,
										 int resultSetHoldability) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareCall not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		return prepareStatement(sql, PreparedStatement.NO_GENERATED_KEYS);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) {
		return new ASQLConnectorPreparedStatement(sql, this, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareStatement(String,int[]) not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
		throw new SQLFeatureNotSupportedException("prepareStatement(String,String[]) not supported");
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
		return prepareStatement(sql, resultSetType, resultSetConcurrency, ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
		if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
			throw new SQLFeatureNotSupportedException("createStatement supported with TYPE_FORWARD_ONLY");
		}
		if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
			throw new SQLFeatureNotSupportedException("createStatement supported with CONCUR_READ_ONLY");
		}
		if (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
			throw new SQLFeatureNotSupportedException("createStatement supported with CLOSE_CURSORS_AT_COMMIT");
		}
		return prepareStatement(sql);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		// TODO: Implemented in Xerial as db.exec(String.format("RELEASE SAVEPOINT %s", savepoint.getSavepointName()));
		throw new SQLFeatureNotSupportedException("releaseSavepoint not supported");
	}

	@Override
	public void rollback() throws SQLException {
		if (autoCommit) {
			throw new SQLException("database in auto-commit mode");
		}
		Log.debug(() -> "END TRANSACTION (rollback) " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
		sqlitedb.endTransaction();
		Log.debug(() -> "BEGIN TRANSACTION (after rollback) " + Thread.currentThread().getId() + BACK_SLASH + Thread.currentThread().getName() + BACK_SLASH2 + this);
		sqlitedb.beginTransaction();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		// TODO: Implemented in Xerial as db.exec(String.format("ROLLBACK TO SAVEPOINT %s", savepoint.getSavepointName()));
		throw new SQLFeatureNotSupportedException("rollback not supported");
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		// TODO: In Xerial: db.exec(String.format("SAVEPOINT %s", sp.getSavepointName()))
		throw new SQLFeatureNotSupportedException("setSavepoint not supported");
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		throw new SQLFeatureNotSupportedException("setSavepoint not supported");
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
	public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new SQLFeatureNotSupportedException("createArrayOf not supported");
	}

	@Override
	public Blob createBlob() throws SQLException {
		// TODO: Can return new ASQLConnectorBlob(new byte[0]) once setBytes is implemented
		throw new SQLFeatureNotSupportedException("createBlob not supported");
	}

	@Override
	public ASQLConnectorClob createClob() throws SQLException {
		// TODO: Can return new ASQLConnectorClob("") once setString is implemented
		throw new SQLFeatureNotSupportedException("createClob not supported");
	}

	@Override
	public NClob createNClob() throws SQLException {
		return createClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		throw new SQLFeatureNotSupportedException("createSQLXML not supported");
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new SQLFeatureNotSupportedException("createStruct not supported");
	}

	@Override
	public Properties getClientInfo() {
		// TODO Evaluate if this is a sufficient implementation (if so, remove this comment)
		return new Properties();
	}

	@Override
	public void setClientInfo(Properties properties) {
		// TODO Evaluate if this is a sufficient implementation (if so, remove this comment)
	}

	@Override
	public String getClientInfo(String name) {
		// TODO Evaluate if this is a sufficient implementation (if so, remove this comment)
		return null;
	}

	@Override
	public boolean isValid(int timeout) {
		// TODO createStatement().execute("select 1");
		return true;
	}

	@Override
	public void setClientInfo(String name, String value) {
		// TODO Evaluate if this is a sufficient implementation (if so, remove this comment)
	}

	/**
	 * @return Where the database is located.
	 */
	public String getURL() {
		return url;
	}

	// methods added for JDK7 compilation
	public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
		throw new SQLFeatureNotSupportedException("setNetworkTimeout not supported");
	}
	public int getNetworkTimeout() throws SQLException {
		throw new SQLFeatureNotSupportedException("getNetworkTimeout not supported");
	}
	public void abort(Executor executor) throws SQLException {
		close();
	}
	public String getSchema() {
		return null;
	}
	public void setSchema(String schema) {
	}

	/**
	 * @return The number of database rows that were changed or inserted or deleted
	 * by the most recently completed INSERT, DELETE, or UPDATE statement.
	 */
	public int changedRowsCount() {
		int changedRows = -1;
		try {
			changedRowsCountStatement = getChangedRowsCountStatement();
			try (ResultSet changedRowsCountResultSet = changedRowsCountStatement.executeQuery()) {
				if (changedRowsCountResultSet != null && changedRowsCountResultSet.first()) {
					changedRows = (int) changedRowsCountResultSet.getLong(1);
					// System.out.println("In ASQLConnectorConnection.changedRowsCount(), changedRows=" + changedRows);
				}
			}
		} catch (SQLException ignored) {
			// ignore
		}
		return changedRows;
	}

	/**
	 * @return A cached prepare statement for the last row id generated
	 * by the database when executing an INSERT statement or create a
	 * new prepare statement and then return that.
	 * @throws SQLException if a problem occurs
	 */
	public ResultSet getGeneratedRowIdResultSet() throws SQLException {
		if (generatedRowIdStatement == null) {
			generatedRowIdStatement = prepareStatement("select last_insert_rowid();");
		}

		return generatedRowIdStatement.executeQuery();
	}


	/**
	 * @return A cached prepare statement for the count of changed rows or create one and return that.
	 * @throws SQLException if a problem occurs
	 */
	private PreparedStatement getChangedRowsCountStatement() throws SQLException {
		if (changedRowsCountStatement == null) {
			changedRowsCountStatement = prepareStatement("select changes();");
		}

		return changedRowsCountStatement;
	}
}
