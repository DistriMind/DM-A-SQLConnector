package fr.distrimind.oss.asqlconnector;

import fr.distrimind.oss.flexilogxml.common.ReflectionTools;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.sql.*;
import java.util.*;

import static org.junit.Assert.*;

@SuppressWarnings({"ResultOfMethodCallIgnored","PMD.CheckResultSet", "PMD.JUnitUseExpected"})
public class AndroidDriverUnitTest {

	final static int NO_LOCALIZED_COLLATORS = 16;
	public static final String NAME = "name";
	public static final String VALUE = "value";
	public static final String CURRENT_NAME_IS_NULL = "Current name is null";
	public static final String ALSO = "\" also \"";
	public static final String TABLE_NAME = "TABLE_NAME";
	public static final String TO_ROWS_UPDATED_ = "To Rows updated ";
	public static final String VALUE_FOR_A_STRING = "Value for aString";
	public static final String VALUE_FOR_ID = "Value for id";
	public static final String BLAHBLAH = "blahblah";
	public static final String CURRENT_VALUE_IS_NULL = "Current value is null";
	public static final String INSERT_INTO_DUMMYTABLE_ID_A_STRING_A_BYTE_A_SHORT_AN_INT_A_LONG_A_BOOL_A_FLOAT_A_DOUBLE_A_TEXT_VALUES = "INSERT INTO dummytable(id, aString, aByte, aShort, anInt, aLong, aBool, aFloat, aDouble, aText) VALUES ";
	public static final String SHOULD_BE_NO_MORE_RESULTS = "Should  be no more results ";
	public static final String SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH = "Should have thrown an IllegalArgumentException because of an non-existent column name blahblah";
	public static final String ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH = "Only IllegalArgumentException expected for a non-existent column name blahblah";
	/**
	 * Going to use DM-A-SQLConnector JDBC Driver
	 */
	protected String driverName = "fr.distrimind.oss.asqlconnector.ASQLConnectorDriver";
	/**
	 * Common prefix for creating JDBC URL
	 */
	protected String JDBC_URL_PREFIX = "jdbc:asqlconnector:";
	/**
	 * Package name of this app
	 */
	protected String packageName = "fr.distrimind.oss.asqlconnector";
	/**
	 * Database file directory for this app on Android
	 */
	// TODO: This should be /data/data/fr.distrimind.oss.asqlconnector/databases/ if running on device
	protected String DB_DIRECTORY = "/data/data/" + packageName + "/databases/";
	/**
	 * Name of an in-memory database
	 */
	protected String dummyDatabase = "dummydatabase.db";
	/**
	 * The URL to the in-memory database.
	 */
	protected String databaseURL = JDBC_URL_PREFIX + dummyDatabase;
	/**
	 * The table create statement.
	 */
	protected String createTable = "CREATE TABLE dummytable (name VARCHAR(254), value int)";
	/**
	 * Some data for the table.
	 */
	protected String[] inserts = {
			"INSERT INTO dummytable(name,value) VALUES('Apple', 100)",
			"INSERT INTO dummytable(name,value) VALUES('Orange', 200)",
			"INSERT INTO dummytable(name,value) VALUES('Banana', 300)",
			"INSERT INTO dummytable(name,value) VALUES('Kiwi', 400)"};
	/**
	 * A select statement.
	 */
	protected String select = "SELECT * FROM dummytable WHERE value < 250";

	/**
	 * Creates the directory structure for the database file and loads the JDBC driver.
	 *
	 * @param dbFile the database file name
	 * @throws Exception if a problem occurs
	 */
	protected void setupDatabaseFileAndJDBCDriver(String dbFile) throws Exception {
		// If the database file already exists, delete it, else create the parent directory for it.
		File f = new File(dbFile);
		if (f.exists()) {
			f.delete();
		} else {
			if (null != f.getParent()) {
				f.getParentFile().mkdirs();
			}
		}
		// Loads and registers the JDBC driver
		DriverManager.registerDriver((Driver) (ReflectionTools.getClassLoader().loadClass(driverName).getConstructor().newInstance()));
	}

	public Blob selectBlob(Connection con, int key) throws Exception {
		try(PreparedStatement stmt = con.prepareStatement("SELECT value,key FROM blobtest where key = ?")) {
			stmt.setInt(1, key);
			try(ResultSet rs = stmt.executeQuery()) {
				assertNotNull("Executed", rs);
				Assert.assertTrue(rs.next());
				Log.error("blob record \"" + rs.getBlob(1).toString() + "\" key " + rs.getString(2));
				assertTrue(" Only one record ", rs.isLast());
				return rs.getBlob(1);
			}
		}
	}

	/**
	 * Test the serialization of the various value objects.
	 */
	@Test
	public void testBlob() throws Exception {
		String dbName = "bolbtest.db";
		String dbFile = DB_DIRECTORY + dbName;
		setupDatabaseFileAndJDBCDriver(dbFile);

		try(Connection con = DriverManager.getConnection(JDBC_URL_PREFIX + dbFile)) {

			con.createStatement().execute("create table blobtest (key int, value blob)");

			// create a blob
			final int blobSize = 70000;
			byte[] aBlob = new byte[blobSize];
			for (int counter = 0; counter < blobSize; counter++) {
				aBlob[counter] = (byte) (counter % 10);
			}
			final int blobSize1 = 1024;
			byte[] aBlob1 = new byte[blobSize1];
			for (int counter = 0; counter < blobSize1; counter++) {
				aBlob1[counter] = (byte) (counter % 10 + 0x30);
			}

			final String stringBlob = "ABlob";
			/* Some data for the table. */
			final String[] blobInserts = {
					"INSERT INTO blobtest(key,value) VALUES (101, '" + stringBlob + "')",
					"INSERT INTO blobtest(key,value) VALUES (?, ?)",
					"INSERT INTO blobtest(key,value) VALUES (?, ?)",
					"INSERT INTO blobtest(key,value) VALUES (401, ?)"};

			Log.info("Insert statement is:" + blobInserts[0]);
			try(Statement stat=con.createStatement()) {
				stat.execute(blobInserts[0]);
				Blob b = selectBlob(con, 101);
				String s=new String(b.getBytes(1, (int)b.length()-1));
				assertEquals ("String blob length", stringBlob.length(), s.length());
				assertEquals ("String blob", stringBlob, s);
			}


			try(PreparedStatement stmt = con.prepareStatement(blobInserts[1])) {
				stmt.setInt(1, blobSize);
				stmt.setBlob(2, new ByteArrayInputStream(aBlob), aBlob.length);
				stmt.execute();
				Blob b = selectBlob(con, blobSize);
				assertEquals(" Correct Length ", blobSize, b.length());
				byte[] bytes = b.getBytes(1, blobSize);
				for (int counter = 0; counter < blobSize; counter++) {
					assertEquals(" Blob Element " + counter, (counter % 10), bytes[counter]);
				}
			}
			try(PreparedStatement stmt = con.prepareStatement(blobInserts[2])) {
				stmt.setInt(1, blobSize1);
				stmt.setBlob(2, new ByteArrayInputStream(aBlob1), aBlob1.length);
				stmt.execute();
				Blob b = selectBlob(con, blobSize1);
				assertEquals(" Correct 1 Length ", blobSize1, b.length());
				byte[] bytes = b.getBytes(1, blobSize1);
				for (int counter = 0; counter < blobSize1; counter++) {
					assertEquals(" Blob1 Element " + counter, (counter % 10 + 0x30), bytes[counter]);
				}
			}

			try(PreparedStatement stmt = con.prepareStatement(blobInserts[3])) {
				stmt.setBlob(1, new ByteArrayInputStream(aBlob), aBlob.length);
				stmt.execute();
				Blob b = selectBlob(con, 401);
				assertEquals(" Correct Length ", blobSize, b.length());
				byte[] bytes = b.getBytes(1, blobSize);
				for (int counter = 0; counter < blobSize; counter++) {
					assertEquals(" Blob Element " + counter, (counter % 10), bytes[counter]);
				}
			}
		}
	}

	@Test
	public void testCursors() throws Exception {
		String dbName = "cursortest.db";
		String dbFile = DB_DIRECTORY + dbName;
		setupDatabaseFileAndJDBCDriver(dbFile);

		Connection con = DriverManager.getConnection(JDBC_URL_PREFIX + dbFile);
		try(Statement stat=con.createStatement()) {
			stat.execute(createTable);
		}

		for (String insertSQL : inserts) {
			con.createStatement().execute(insertSQL);
		}
		ResultSet rsOld;
		try(Statement stat=con.createStatement()) {
			try(ResultSet rs =rsOld= stat.executeQuery("SELECT * FROM dummytable order by value")) {
				checkResultSet(rs, false, true, false, false, false);
				Assert.assertTrue(rs.next());
				checkResultSet(rs, false, false, false, true, false);
				checkValues(rs, "Apple", 100);
				Assert.assertTrue(rs.next());
				checkResultSet(rs, false, false, false, false, false);
				checkValues(rs, "Orange", 200);
				Assert.assertTrue(rs.next());
				checkResultSet(rs, false, false, false, false, false);
				checkValues(rs, "Banana", 300);
				Assert.assertTrue(rs.next());  // to last
				checkResultSet(rs, false, false, false, false, true);
				checkValues(rs, "Kiwi", 400);
				Assert.assertFalse(rs.next());  // after last
				checkResultSet(rs, false, false, true, false, false);
				Assert.assertTrue(rs.first());
				checkResultSet(rs, false, false, false, true, false);
				Assert.assertTrue(rs.last());
				checkResultSet(rs, false, false, false, false, true);
				rs.afterLast();
				checkResultSet(rs, false, false, true, false, false);
				rs.beforeFirst();
				checkResultSet(rs, false, true, false, false, false);
			}
		}
		checkResultSet(rsOld, true, false, false, false, false);
		try(PreparedStatement stmt = con.prepareStatement("SELECT ?,? FROM dummytable order by ?")) {
			stmt.setString(1, NAME);
			stmt.setString(2, VALUE);
			stmt.setString(3, VALUE);
			try(ResultSet rs = stmt.executeQuery()) {
				assertNotNull("Executed", rs);
				Assert.assertTrue(rs.last());
				assertEquals("Enough rows ", 4, rs.getRow());
			}
		}

		// Add a null value for name
		try(Statement stat=con.createStatement()) {
			stat.execute("INSERT INTO dummytable(name, value) VALUES(null, 500)");
		}
		try(Statement stat=con.createStatement()) {
			try(ResultSet rs = stat.executeQuery("SELECT name, value FROM dummytable order by value")) {
				assertEquals("Name column position", 1, rs.findColumn(NAME));
				assertEquals("Value column position", 2, rs.findColumn(VALUE));

				// In the first row, name is Apple and value is 100.
				assertTrue("Cursor on the first row", rs.first());
				assertEquals("Name in the first row using column name", "Apple", rs.getString(NAME));
				assertFalse(CURRENT_NAME_IS_NULL, rs.wasNull());
				assertEquals("Value in the first row using column name", 100, rs.getInt(VALUE));
				assertFalse(CURRENT_VALUE_IS_NULL, rs.wasNull());
				assertEquals("Name in the first row using column number", "Apple", rs.getString(1));
				assertEquals("Value in the first row using column number", 100, rs.getInt(2));
				assertFalse("Current value for Apple is null", rs.wasNull());

				// In the second row, name is Orange and value is 200.
				Assert.assertTrue(rs.next());
				assertEquals("Name in the second row using column name", "Orange", rs.getString(NAME));
				assertEquals("Value in the second row using column name", 200, rs.getInt(VALUE));
				assertFalse("Current value for Banana is null", rs.wasNull());
				assertEquals("Name in the second row using column number", "Orange", rs.getString(1));
				assertEquals("Value in the second row using column number", 200, rs.getInt(2));
				assertFalse("Current value for Banana is null", rs.wasNull());

				// In the last row, name is null and value is 500.
				Assert.assertTrue(rs.last());
				assertNull("Name in the last row using column name", rs.getString(NAME));
				assertTrue("Current name is not null", rs.wasNull());
				assertEquals("Value in the last row using column name", 500, rs.getInt(VALUE));
				assertFalse(CURRENT_VALUE_IS_NULL, rs.wasNull());
				assertNull("Name in the last row using column number", rs.getString(1));
				assertTrue("Current name is not null", rs.wasNull());
				assertEquals("Value in the last row using column number", 500, rs.getInt(2));
				assertFalse(CURRENT_VALUE_IS_NULL, rs.wasNull());
			}
		}
	}

	@Test
	public void testResultSets() throws Exception {
		String dbName = "resultsetstest.db";
		String dbFile = DB_DIRECTORY + dbName;
		setupDatabaseFileAndJDBCDriver(dbFile);

		try(Connection con = DriverManager.getConnection(JDBC_URL_PREFIX + dbFile)) {

			String createTableStatement = "CREATE TABLE dummytable (id int, aString VARCHAR(254), aByte byte, "
					+ "aShort short, anInt int, aLong long, aBool boolean, aFloat float, aDouble, double, aText text)";

			final String[] insertStatements = {
					INSERT_INTO_DUMMYTABLE_ID_A_STRING_A_BYTE_A_SHORT_AN_INT_A_LONG_A_BOOL_A_FLOAT_A_DOUBLE_A_TEXT_VALUES
							+ "(1, 'string1', 1,     1,      10,    100,   0,    1.0,    10.0,   'text1')",
					INSERT_INTO_DUMMYTABLE_ID_A_STRING_A_BYTE_A_SHORT_AN_INT_A_LONG_A_BOOL_A_FLOAT_A_DOUBLE_A_TEXT_VALUES
							+ "(2, 'string2', 2,     2,     20,    200,   1,    2.0,    20.0,   'text2')",
					INSERT_INTO_DUMMYTABLE_ID_A_STRING_A_BYTE_A_SHORT_AN_INT_A_LONG_A_BOOL_A_FLOAT_A_DOUBLE_A_TEXT_VALUES
							+ "(3, null,    null,   null,   30,    300,   0,    3.0,     30.0,   null)",
					INSERT_INTO_DUMMYTABLE_ID_A_STRING_A_BYTE_A_SHORT_AN_INT_A_LONG_A_BOOL_A_FLOAT_A_DOUBLE_A_TEXT_VALUES
							+ "(4, 'string4', 4,     4,     null,  null,  null,   4.0,    40.0,  'text4')",
					INSERT_INTO_DUMMYTABLE_ID_A_STRING_A_BYTE_A_SHORT_AN_INT_A_LONG_A_BOOL_A_FLOAT_A_DOUBLE_A_TEXT_VALUES
							+ "(5, 'string5', 5,     5,     50,    500,   1,     null,    null, 'text5')"};
			try (Statement statement = con.createStatement()) {
				statement.execute(createTableStatement);
			}
			for (String insertSQL : insertStatements) {
				try (Statement statement = con.createStatement()) {
					statement.execute(insertSQL);
				}
			}
			try (Statement statement = con.createStatement()) {
				try (ResultSet rs = statement.executeQuery(
						"SELECT id, aString, aByte, aShort, anInt, aLong, aBool, aFloat, aDouble, aText FROM dummytable order by id")) {

					rs.first();
					try {
						rs.findColumn(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getString(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getByte(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getShort(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getInt(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getLong(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getBoolean(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getFloat(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception ignored) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getDouble(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception ignored) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getBlob(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					try {
						rs.getObject(BLAHBLAH);
						fail(SHOULD_HAVE_THROWN_AN_ILLEGAL_ARGUMENT_EXCEPTION_BECAUSE_OF_AN_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					} catch (IllegalArgumentException ignored) {
						// OK
					} catch (Exception e) {
						fail(ONLY_ILLEGAL_ARGUMENT_EXCEPTION_EXPECTED_FOR_A_NON_EXISTENT_COLUMN_NAME_BLAHBLAH);
					}

					assertEquals(VALUE_FOR_ID, 1, rs.getInt("id"));
					assertEquals(VALUE_FOR_A_STRING, "string1", rs.getString("aString"));
					assertEquals("Value for aByte", 1, rs.getByte("aByte"));
					assertEquals("Value for aShort", 1, rs.getShort("aShort"));
					assertEquals("Value for anInt", 10, rs.getInt("anInt"));
					assertEquals("Value for aLong", 100, rs.getLong("aLong"));
					assertFalse("Value for aBool", rs.getBoolean("aBool"));

					// Compare strings to avoid Float precision problems
					assertEquals("Value for aFloat", "1.0",
							Float.valueOf(rs.getFloat("aFloat")).toString());
					assertFalse("Current value for aFloat is null", rs.wasNull());

					assertEquals("Value for aDouble", 10.0, rs.getDouble("aDouble"), 0.01);
					assertFalse("Current value for aDouble is null", rs.wasNull());
					assertEquals("Value for aText", "text1", rs.getString("aText"));

					rs.next(); // 2nd Row
					// No values should be null in this row
					assertEquals(VALUE_FOR_ID, 2, rs.getInt(1));
					assertEquals(VALUE_FOR_A_STRING, "string2", rs.getString(2));
					assertEquals("Value for aByte", 2, rs.getByte(3));
					assertEquals("Value for aShort", 2, rs.getShort(4));
					assertEquals("Value for anInt", 20, rs.getInt(5));
					assertEquals("Value for aLong", 200, rs.getLong(6));
					assertTrue("Value for aBool", rs.getBoolean(7));

					// Compare strings to avoid Float precision problems
					assertEquals("Value for aFloat", "2.0",
							Float.valueOf(rs.getFloat(8)).toString());
					assertFalse("Current value for aFloat is null", rs.wasNull());

					assertEquals("Value for aDouble", 20.0, rs.getDouble(9), 0.01);
					assertFalse("Current value for aDouble is null", rs.wasNull());
					assertEquals("Value for aText", "text2", rs.getString(10));

					rs.next(); // 3rd row
					// Values for aString, aByte, aShort and aText should be null in this row
					assertEquals(VALUE_FOR_ID, 3, rs.getInt(1));
					assertNull(VALUE_FOR_A_STRING, rs.getString(2));
					assertTrue("Current value for aStrnig is not null", rs.wasNull());
					assertNull("Value for aByte", rs.getString(3));
					assertTrue("Current value for aByte is not null", rs.wasNull());
					assertNull("Value for aShort", rs.getString("aShort"));
					assertTrue("Current value for aShort is not null", rs.wasNull());
					assertNull("Value for aText", rs.getString("aText"));
					assertTrue("Current value for aText is not null", rs.wasNull());

					rs.last(); // 5th row
					// Values for aFloat and aDouble columns should be null in this row
					assertEquals(VALUE_FOR_ID, 5, rs.getInt(1));
					assertEquals(VALUE_FOR_A_STRING, "string5", rs.getString(2));
					assertFalse(CURRENT_VALUE_IS_NULL, rs.wasNull());
					assertTrue("Value for aBool", rs.getBoolean("aBool"));
					assertFalse(CURRENT_VALUE_IS_NULL, rs.wasNull());

					// Compare strings to avoid Float precision problems
					assertEquals("Value for aFloat", "0.0",
							Float.valueOf(rs.getFloat("aFloat")).toString()); // a null float column value is returned as 0.0

					assertTrue("Current value for aFloat is not null", rs.wasNull());

					assertEquals("Value for aDouble", 0.0, rs.getDouble("aDouble"), 0.01); // a null double column value is returned as 0.0
					assertTrue("Current value for aDouble is not null", rs.wasNull());

					assertEquals("Enough rows ", insertStatements.length, rs.getRow());

					rs.previous(); // 4th row
					// Values for anInt, aLong and aBool columns should be null in this row
					assertEquals(VALUE_FOR_ID, 4, rs.getInt(1));
					rs.getInt("anInt");
					assertTrue("Current value for anInt is not null", rs.wasNull());
					rs.getLong("aLong");
					assertTrue("Current value for aLong is not null", rs.wasNull());
					rs.getBoolean("aBool");
					assertTrue("Current value for aBool is not null", rs.wasNull());

				}
			}
		}
	}

	@Test
	public void testExecute() throws Exception {
		String dbName = "executetest.db";
		String dbFile = DB_DIRECTORY + dbName;
		setupDatabaseFileAndJDBCDriver(dbFile);

		try (Connection con = DriverManager.getConnection(JDBC_URL_PREFIX + dbFile)) {
			try (Statement statement = con.createStatement()) {
				statement.execute(createTable);
			}


			for (String insertSQL : inserts) {
				try (Statement statement = con.createStatement()) {
					statement.execute(insertSQL);
				}
			}

			try (Statement statement = con.createStatement()) {
				boolean hasResultSet = statement.execute("SELECT * FROM dummytable order by value");
				assertTrue("Should return a result set", hasResultSet);
				assertEquals("Should be -1 ", -1, statement.getUpdateCount());
				assertNotNull("Result Set should be non-null ", statement.getResultSet());
				// second time this will be true.
				boolean noMoreResults = ((!statement.getMoreResults()) && (statement.getUpdateCount() == -1));
				assertTrue(SHOULD_BE_NO_MORE_RESULTS, noMoreResults);
				assertNull("Result Set should be non-null ", statement.getResultSet());
			}

			try (Statement statement = con.createStatement()) {
				statement.execute("SELECT * FROM dummytable where name = 'fig'");  // no matching result

				assertNotNull("Result Set should not be null ", statement.getResultSet());
				assertEquals("Should not be -1 ", -1, statement.getUpdateCount());
				// second time this will be true.
				boolean noMoreResults = ((!statement.getMoreResults()) && (statement.getUpdateCount() == -1));
				assertTrue(SHOULD_BE_NO_MORE_RESULTS, noMoreResults);
				assertNull("Result Set should be null - no results ", statement.getResultSet());
			}

			try (PreparedStatement stmt = con.prepareStatement("SELECT ?,? FROM dummytable order by ?")) {
				stmt.setString(1, NAME);
				stmt.setString(2, VALUE);
				stmt.setString(3, VALUE);
				boolean hasResultSet = stmt.execute();
				assertTrue("Should return a result set", hasResultSet);
				assertEquals("Should not be -1 ", -1, stmt.getUpdateCount());
				// second time this will be true.
				boolean noMoreResults = ((!stmt.getMoreResults()) && (stmt.getUpdateCount() == -1));
				assertTrue(SHOULD_BE_NO_MORE_RESULTS, noMoreResults);
				assertNull("Result Set should be null ", stmt.getResultSet());  // no more results
			}

			try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM dummytable where name = 'fig'")) {
				boolean hasResultSet = stmt.execute();  // no matching result but an empty Result Set should be returned
				assertTrue("Should return a result set", hasResultSet);
				assertNotNull("Result Set should not be null ", stmt.getResultSet());
				assertEquals("Should not be -1 ", -1, stmt.getUpdateCount());
				// second time this will be true.
				boolean noMoreResults = ((!stmt.getMoreResults()) && (stmt.getUpdateCount() == -1));
				assertTrue(SHOULD_BE_NO_MORE_RESULTS, noMoreResults);
				assertNull("Result Set should be null - no results ", stmt.getResultSet());
			}

			try (PreparedStatement stmt = con.prepareStatement("update dummytable set name='Kumquat' where name = 'Orange' OR name = 'Kiwi'")) {
				stmt.execute();
				assertEquals(TO_ROWS_UPDATED_, 2, stmt.getUpdateCount());
				for (String insertSQL : inserts) {
					try(Statement s = con.createStatement()) {
						s.execute(insertSQL);
						assertEquals(TO_ROWS_UPDATED_, 1, s.getUpdateCount());
					}
				}
				int rows = stmt.executeUpdate();
				assertEquals(TO_ROWS_UPDATED_, 2, rows);
				assertEquals(TO_ROWS_UPDATED_, 2, stmt.getUpdateCount());
			}

			for (String insertSQL : inserts) {
				try (PreparedStatement stmt = con.prepareStatement(insertSQL)) {
					stmt.execute();

					assertEquals(TO_ROWS_UPDATED_, 1, stmt.getUpdateCount());
				}
			}

			try (Statement statement = con.createStatement()) {
				boolean hasResultSet = statement.execute("update dummytable set name='Kumquat' where name = 'Orange' OR name = 'Kiwi'");  // no matching result
				assertFalse("Should not return a result set", hasResultSet);
				for (String insertSQL : inserts) {
					con.createStatement().execute(insertSQL);
				}
				int r1 = statement.executeUpdate("update dummytable set name='Kumquat' where name = 'Orange' OR name = 'Kiwi'");  // no matching result
				assertEquals(TO_ROWS_UPDATED_, 2, statement.getUpdateCount());
				assertEquals(TO_ROWS_UPDATED_, 2, r1);
			}

			try (Statement statement = con.createStatement()) {
				for (String insertSQL : inserts) {
					con.createStatement().execute(insertSQL);
				}
				int numRows = statement.executeUpdate("DELETE FROM dummytable where name = 'Orange' OR name = 'Kiwi'");  // 2 rows should be deleted
				assertEquals("Two Rows deleted ", 2, numRows);
			}
			try (PreparedStatement stmt = con.prepareStatement("SELECT * FROM dummytable where name = 'Banana'")) {
				try (ResultSet rs = stmt.executeQuery()) {
					int rowCount = 0;
					if (rs.last()) {
						rowCount = rs.getRow();
					}
					try (Statement statement = con.createStatement()) {
						int numRows = statement.executeUpdate("DELETE FROM dummytable where name = 'Banana'");
						assertEquals("Banana rows deleted ", rowCount, numRows);
					}
				}

			}

		}
	}

	public void checkResultSet(ResultSet rs, boolean isClosed, boolean isBeforeFirst, boolean isAfterLast, boolean isFirst, boolean isLast) throws Exception {
		assertEquals("Is Closed", isClosed, rs.isClosed());
		assertEquals("Is Before First", isBeforeFirst, rs.isBeforeFirst());
		assertEquals("Is after Last", isAfterLast, rs.isAfterLast());
		assertEquals("Is First", isFirst, rs.isFirst());
		assertEquals("Is Laset", isLast, rs.isLast());
	}

	public void checkValues(ResultSet rs, String fruit, int value) throws Exception {
		assertEquals("Fruit", fruit, rs.getString(1));
		assertEquals("Value", value, rs.getInt(2));
	}

	@Test
	public void testMetaData() throws Exception {
		String dbName = "schematest.db";
		String dbFile = DB_DIRECTORY + dbName;
		setupDatabaseFileAndJDBCDriver(dbFile);

		try (Connection con = DriverManager.getConnection(JDBC_URL_PREFIX + dbFile)) {

			try (Statement statement = con.createStatement()) {
				statement.execute("CREATE TABLE PASTIMES (count INT, pastime CHAR(200))");
			}
			try (Statement statement = con.createStatement()) {
				statement.execute("CREATE TABLE STRIP_PASTIMES (count INT, pastime CHAR(200))");
			}
			//  "pingpong", "4,750,000 " "1,360,000"  on 5/16/2011
			//  "chess" "90,500,000", "6,940,000"
			//  "poker" "353,000,000", "9,230,000"
			try (Statement statement = con.createStatement()) {
				statement.execute("INSERT INTO PASTIMES (count, pastime) VALUES (4750000, 'pingpong')");
			}
			try (Statement statement = con.createStatement()) {
				statement.execute("INSERT INTO PASTIMES (count, pastime) VALUES (90500000, 'chess')");
			}
			try (Statement statement = con.createStatement()) {
				statement.execute("INSERT INTO PASTIMES (count, pastime) VALUES (353000000, 'poker')");
			}
			try (Statement statement = con.createStatement()) {
				statement.execute("INSERT INTO STRIP_PASTIMES (count, pastime) VALUES (1360000, 'pingpong')");
			}
			try (Statement statement = con.createStatement()) {
				statement.execute("INSERT INTO STRIP_PASTIMES (count, pastime) VALUES (6940000, 'chess')");
			}
			try (Statement statement = con.createStatement()) {
				statement.execute("INSERT INTO STRIP_PASTIMES (count, pastime) VALUES (9230000, 'poker')");
			}
			try (Statement statement = con.createStatement()) {
				statement.execute("CREATE VIEW PERCENTAGES AS SELECT PASTIMES.pastime, PASTIMES.count , STRIP_PASTIMES.count as stripcount, " +
						" (CAST(STRIP_PASTIMES.count AS REAL)/PASTIMES.count*100.00) as percent FROM PASTIMES, STRIP_PASTIMES " +
						" WHERE PASTIMES.pastime = STRIP_PASTIMES.pastime");
			}

			try (ResultSet rs = con.getMetaData().getTables(null, null, "%", new String[]{"table"})) {
				// rs.next() returns true is there is 1 or more rows
				// should be two tables
				List<String> tableNames = new ArrayList<>(Arrays.asList("PASTIMES", "STRIP_PASTIMES"));
				while (rs.next()) {
					Log.error("Table Name \"" + rs.getString(TABLE_NAME) + ALSO + rs.getString(3) + "\"");
					assertTrue("Table must be in the list", tableNames.remove(rs.getString(3)));
				}
				assertEquals("All tables accounted for", 0, tableNames.size());
			}
			try (ResultSet rs = con.getMetaData().getTables(null, null, "%", new String[]{"view"})) {
				List<String> viewNames = new ArrayList<>();  // only one, not a very good test
				viewNames.add("PERCENTAGES");
				while (rs.next()) {
					assertTrue("View must be in the list", viewNames.remove(rs.getString(TABLE_NAME)));  // mix up name and index
					Log.error("View Name \"" + rs.getString(TABLE_NAME) + ALSO + rs.getString(3) + "\"");
				}
				assertEquals("All views accounted for", 0, viewNames.size());
				try (ResultSet rs2 = con.getMetaData().getTables(null, null, "%", new String[]{"view", "table"})) {
					List<String> allNames = new ArrayList<>(Arrays.asList("PERCENTAGES", "PASTIMES", "STRIP_PASTIMES"));  // all of the views and tables.
					while (rs2.next()) {
						Log.error("View or Table Name \"" + rs2.getString(TABLE_NAME) + ALSO + rs2.getString(3) + "\"");
						assertTrue("View/Table must be in the list", allNames.remove(rs2.getString(TABLE_NAME)));  // mix up name and index
					}
					assertEquals("All views/tables accounted for", 0, viewNames.size());
				}
			}

			try (ResultSet rs = con.getMetaData().getColumns(null, null, "%", "%")) {
				String[] columnNames = new String[]{"count", "pastime", "count", "pastime", "pastime", "count", "stripcount", "percent"};  //  I think I should get these, but I'm not sure
				int columnCounter = 0;
				while (rs.next()) {
					Log.error("Column Name \"" + rs.getString("COLUMN_NAME") + ALSO + rs.getString(4) + "\" type " + rs.getInt(5));
					//      Column Name "count" also "count" type 4
					//      Column Name "pastime" also "pastime" type 12
					//      Column Name "count" also "count" type 4
					//      Column Name "pastime" also "pastime" type 12
					//      Column Name "pastime" also "pastime" type 12
					//      Column Name "count" also "count" type 4
					//      Column Name "stripcount" also "stripcount" type 4
					//      Column Name "percent" also "percent" type 0
					assertEquals("All columns accounted for", columnNames[columnCounter], rs.getString("COLUMN_NAME"));
					assertEquals("All columns accounted for", columnNames[columnCounter], rs.getString(4));
					columnCounter++;
				}
			}
			try (Statement statement = con.createStatement()) {
				try (ResultSet rs = statement.executeQuery("SELECT * FROM PERCENTAGES ORDER BY percent")) {

					while (rs.next()) {
						int count = rs.getMetaData().getColumnCount();
						//List<String> viewColumnNames =  new ArrayList<String>(Arrays.asList(new String[]{"pastime", "count", "stripcount"}));
						for (int counter = 0; counter < count; counter++) {
							Log.error(" " + rs.getMetaData().getColumnName(counter + 1) + " = " + rs.getString(counter + 1));
							//        pastime = poker count = 353000000 stripcount = 9230000 percent = 2.61473087818697
							//        pastime = chess count = 90500000 stripcount = 6940000 percent = 7.66850828729282
							//        pastime = pingpong count = 4750000 stripcount = 1360000 percent = 28.6315789473684
							//assertTrue ("Column Name must be in the list" , viewColumnNames.remove(rs.getString("COLUMN_NAME")) );  // mix up name and index
						}
						// assertEquals ("All columns accounted for", 0, viewColumnNames.size());
					}
				}
			}
			try (Statement statement = con.createStatement()) {
				try (ResultSet rs = statement.executeQuery("SELECT * FROM PASTIMES")) {
					//List<String> tableColumnNames =  new ArrayList<String>(Arrays.asList(new String[]{"pastime", "count"}));
					while (rs.next()) {
						int count = rs.getMetaData().getColumnCount();
						for (int counter = 0; counter < count; counter++) {
							//assertTrue ("Table Column Name must be in the list" , tableColumnNames.remove(rs.getString(4)) );  // mix up name and index
							Log.error(" " + rs.getMetaData().getColumnName(counter + 1) + " = " + rs.getString(counter + 1));
						}
						//assertEquals ("All columns accounted for", 0, tableColumnNames.size());
					}

				}
			}
		}
	}

	@Test
	public void testAutoCommit() throws Exception {
		String dbName = "autocommittest.db";
		String dbFile = DB_DIRECTORY + dbName;
		setupDatabaseFileAndJDBCDriver(dbFile);

		String jdbcURL = JDBC_URL_PREFIX + dbFile;

		Properties removeLocale = new Properties();
		removeLocale.put(ASQLConnectorDriver.ADDITONAL_DATABASE_FLAGS, NO_LOCALIZED_COLLATORS);
		try(Connection conn1 = DriverManager.getConnection(jdbcURL, removeLocale)) {
			Log.info("After getting connection...1");

			conn1.setAutoCommit(true);

			try (Connection ignored = DriverManager.getConnection(jdbcURL)) {
				Log.info("After getting connection...2");

				conn1.setAutoCommit(false);

				try (Connection ignored2 = DriverManager.getConnection(jdbcURL)) {
					Log.info("After getting connection...3");

					try(Statement stat = conn1.createStatement()) {

						// Create table if not already there
						stat.executeUpdate("create table if not exists primes (number int);");

						// Delete any existing records
						stat.executeUpdate("delete from primes;");

						// Populate table
						stat.executeUpdate("insert into primes values (2);");
						stat.executeUpdate("insert into primes values (3);");
						stat.executeUpdate("insert into primes values (5);");
						stat.executeUpdate("insert into primes values (7);");

						// Retrieve records
						try (ResultSet rs = stat.executeQuery("select * from primes")) {
							boolean b = rs.first();
							while (b) {
								String info = "Prime=" + rs.getInt(1);
								Log.info(info);
								b = rs.next();
							}
						}
					}
				}
			}
		}
	}

	@Test
	public void testTimestamp() throws Exception {
		String dbName = "timestamptest.db";
		String dbFile = DB_DIRECTORY + dbName;
		setupDatabaseFileAndJDBCDriver(dbFile);

		try (Connection conn = DriverManager.getConnection(JDBC_URL_PREFIX + dbFile)) {
			try (Statement statement = conn.createStatement()) {
				statement
						.execute("create table timestamptest (id integer, created_at timestamp)");

				// Make sure timestamp is around noon to check for DateFormat bug
				Calendar calendar = new GregorianCalendar(2016, Calendar.AUGUST, 15, 12, 0, 0);
				Timestamp timestamp = new Timestamp(calendar.getTimeInMillis() + 853); // make sure millis are included

				int id = 23432;
				try(PreparedStatement insertStmt = conn.prepareStatement("insert into timestamptest values (?, ?)")) {
					insertStmt.setInt(1, id);
					insertStmt.setTimestamp(2, timestamp);
					insertStmt.executeUpdate();
				}

				try(PreparedStatement selectStmt = conn.prepareStatement("select * from timestamptest where id = ?")) {
					selectStmt.setInt(1, id);
					try(ResultSet rs = selectStmt.executeQuery()) {
						Assert.assertTrue(rs.next());

						assertEquals(timestamp, rs.getTimestamp("created_at"));
					}
				}

			}
		}
	}
}
