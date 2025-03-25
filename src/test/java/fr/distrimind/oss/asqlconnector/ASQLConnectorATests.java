package fr.distrimind.oss.asqlconnector;


import junit.framework.AssertionFailedError;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Pattern;


@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(Parameterized.class)
public class ASQLConnectorATests {

	@Parameterized.Parameters(name = "{index}: {0}")
	public static Collection<Object[]> getTests()
	{
		List<Object[]> tests=new ArrayList<>();
		tests.add(new Object[]{
				"shouldRetrieveInsertedBasicTypes", "basic-types.db", (ITest) conn -> {
			String createTableStatement = "CREATE TABLE dummytable (id int, aString VARCHAR(254), aByte byte, "
					+ "aShort short, anInt int, aLong long, aBool boolean, aFloat float, aDouble double, aText text)";
			conn.createStatement().execute(createTableStatement);

			int id = 4325;
			String string = "test";
			byte b = 23;
			short s = 421;
			int i = 12551;
			long l = 23423525322L;
			boolean bool = false;
			float f = 324235.0f;
			double d = 123425.125;
			String text = "some potentially very long text";

			String insertStmt = "insert into dummytable "
					+ "(id, aString, aByte, aShort, anInt, aLong, aBool, aFloat, aDouble, aText) "
					+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
				stmt.setInt(1, id);
				stmt.setString(2, string);
				stmt.setByte(3, b);
				stmt.setShort(4, s);
				stmt.setInt(5, i);
				stmt.setLong(6, l);
				stmt.setBoolean(7, bool);
				stmt.setFloat(8, f);
				stmt.setDouble(9, d);
				stmt.setString(10, text);
				int rowCount = stmt.executeUpdate();
				Assert.assertEquals(1, rowCount);
			}


			String selectStmt = "SELECT aString, aByte, aShort, anInt, aLong, aBool, aFloat, aDouble, aText "
					+ " FROM dummytable where id = ?";
			try (PreparedStatement stmt = conn.prepareStatement(selectStmt)) {
				stmt.setInt(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertEquals(string, rs.getString(1));
					Assert.assertEquals(string, rs.getString("aString"));
					Assert.assertEquals(string, rs.getObject(1));
					Assert.assertEquals(string, rs.getObject("aString"));

					Assert.assertEquals(b, rs.getByte(2));
					Assert.assertEquals(b, rs.getByte("aByte"));

					Assert.assertEquals(s, rs.getShort(3));
					Assert.assertEquals(s, rs.getShort("aShort"));
					Assert.assertEquals((long) s, rs.getObject(3));
					Assert.assertEquals((long) s, rs.getObject("aShort"));

					Assert.assertEquals(i, rs.getInt(4));
					Assert.assertEquals(i, rs.getInt("anInt"));
					Assert.assertEquals(i, rs.getLong(4));
					Assert.assertEquals(i, rs.getLong("anInt"));
					Assert.assertEquals((long) i, rs.getObject(4));
					Assert.assertEquals((long) i, rs.getObject("anInt"));

					Assert.assertEquals(l, rs.getLong(5));
					Assert.assertEquals(l, rs.getLong("aLong"));
					Assert.assertEquals(l, rs.getObject(5));
					Assert.assertEquals(l, rs.getObject("aLong"));

					Assert.assertEquals(bool, rs.getBoolean(6));
					Assert.assertEquals(bool, rs.getBoolean("aBool"));
					Assert.assertEquals(0, rs.getInt(6));
					Assert.assertEquals(0, rs.getInt("aBool"));
					Assert.assertEquals(0L, rs.getObject(6));
					Assert.assertEquals(0L, rs.getObject("aBool"));

					Assert.assertEquals(f, rs.getFloat(7), 0.0f);
					Assert.assertEquals(f, rs.getFloat("aFloat"), 0.0f);
					Assert.assertEquals(f, rs.getDouble(7), 0.0f);
					Assert.assertEquals(f, rs.getDouble("aFloat"), 0.0f);
					Assert.assertEquals((double) f, rs.getObject(7));
					Assert.assertEquals((double) f, rs.getObject("aFloat"));

					Assert.assertEquals(d, rs.getDouble(8), 0.0);
					Assert.assertEquals(d, rs.getDouble("aDouble"), 0.0);
					Assert.assertEquals((float) d, rs.getFloat(8), 0.0);
					Assert.assertEquals((float) d, rs.getFloat("aDouble"), 0.0);
					Assert.assertEquals(d, rs.getObject(8));
					Assert.assertEquals(d, rs.getObject("aDouble"));

					Assert.assertEquals(text, rs.getString(9));
					Assert.assertEquals(text, rs.getString("aText"));
					Assert.assertEquals(text, rs.getObject(9));
					Assert.assertEquals(text, rs.getObject("aText"));

				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldRetrieveInsertedBigDecimals", "basic-types.db", (ITest) conn -> {
			String createTableStatement = "CREATE TABLE bdTable (id int, aBigDecimal text)";
			conn.createStatement().execute(createTableStatement);

			int id = 100500;
			BigDecimal bigDecimal = new BigDecimal("10005000.00050001");

			String insertStmt = "insert into bdTable (id, aBigDecimal) values (?, ?)";
			try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
				stmt.setInt(1, id);
				stmt.setBigDecimal(2, bigDecimal);
				int rowCount = stmt.executeUpdate();
				Assert.assertEquals(1, rowCount);
			}

			String selectStmt = "SELECT aBigDecimal FROM bdTable where id = ?";
			try (PreparedStatement stmt = conn.prepareStatement(selectStmt)) {
				stmt.setInt(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();

					Assert.assertEquals(bigDecimal, rs.getBigDecimal(1));
					Assert.assertEquals(bigDecimal, rs.getBigDecimal("aBigDecimal"));
					Assert.assertEquals(bigDecimal, rs.getObject(1));
					Assert.assertEquals(bigDecimal, rs.getObject("aBigDecimal"));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldRetrieveInsertedNullValues", "null-values.db", (ITest) conn -> {
			String createTableStatement = "CREATE TABLE dummytable (id int, aString VARCHAR(254), aByte byte, "
					+ "aShort short, anInt int, aLong long, aBool boolean, aFloat float, aDouble double, aText text)";
			conn.createStatement().execute(createTableStatement);

			int id = 13155;

			String insertStmt = "insert into dummytable "
					+ "(id, aString, aByte, aShort, anInt, aLong, aBool, aFloat, aDouble, aText) "
					+ "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
			try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
				stmt.setInt(1, id);
				for (int i = 2; i <= 10; i++) {
					stmt.setObject(i, null);
				}
				stmt.executeUpdate();
			}


			String selectStmt = "SELECT aString, aByte, aShort, anInt, aLong, aBool, aFloat, aDouble, aText "
					+ " FROM dummytable where id = ?";
			try (PreparedStatement stmt = conn.prepareStatement(selectStmt)) {
				stmt.setInt(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertNull(rs.getString(1));

					Assert.assertNull(rs.getObject(2));
					Assert.assertEquals(0, rs.getByte(2));
					Assert.assertTrue(rs.wasNull());

					Assert.assertNull(rs.getObject(3));
					Assert.assertEquals(0, rs.getShort(3));
					Assert.assertTrue(rs.wasNull());

					Assert.assertNull(rs.getObject(4));
					Assert.assertEquals(0, rs.getInt(4));
					Assert.assertTrue(rs.wasNull());

					Assert.assertNull(rs.getObject(5));
					Assert.assertEquals(0, rs.getLong(5));
					Assert.assertTrue(rs.wasNull());

					Assert.assertNull(rs.getObject(6));
					Assert.assertFalse(rs.getBoolean(6));
					Assert.assertTrue(rs.wasNull());

					Assert.assertNull(rs.getObject(7));
					Assert.assertEquals(0.0f, rs.getFloat(7), 0.0f);
					Assert.assertTrue(rs.wasNull());

					Assert.assertNull(rs.getObject(8));
					Assert.assertEquals(0.0, rs.getDouble(8), 0.0);
					Assert.assertTrue(rs.wasNull());

					Assert.assertNull(rs.getObject(9));
					Assert.assertNull(rs.getString(9));
				}
			}
		}
		});
		tests.add(new Object[]{
				"testUpdate", "test-update.db", (ITest) conn -> {
			String createTableStatement = "CREATE TABLE upTable (id int, aValue int)";
			conn.createStatement().execute(createTableStatement);

			final int id = 100500;
			final int invalidId = 1;
			final int value=42;
			final int updateValue=42;

			final String insertStmt = "insert into upTable (id, aValue) values (?, ?)";
			try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
				stmt.setInt(1, id);
				stmt.setInt(2, value);
				int rowCount = stmt.executeUpdate();
				Assert.assertEquals(1, rowCount);
			}

			final String selectStmt = "SELECT aValue FROM upTable where id = ?";
			try (PreparedStatement stmt = conn.prepareStatement(selectStmt)) {
				stmt.setInt(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();

					Assert.assertEquals(value, rs.getInt(1));
				}
			}

			final String updateStmt = "update upTable set aValue=? where id=?";
			try (PreparedStatement stmt = conn.prepareStatement(updateStmt)) {
				stmt.setInt(1, updateValue);
				stmt.setInt(2, id);
				int rowCount = stmt.executeUpdate();
				Assert.assertEquals(1, rowCount);
			}

			try (PreparedStatement stmt = conn.prepareStatement(selectStmt)) {
				stmt.setInt(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();

					Assert.assertEquals(updateValue, rs.getInt(1));
				}
			}

			try (PreparedStatement stmt = conn.prepareStatement(updateStmt)) {
				stmt.setInt(1, updateValue);
				stmt.setInt(2, invalidId);
				int rowCount = stmt.executeUpdate();
				Assert.assertEquals(0, rowCount);
			}

			try (PreparedStatement stmt = conn.prepareStatement(insertStmt)) {
				stmt.setInt(1, id+1);
				stmt.setInt(2, value+1);
				stmt.addBatch();
				stmt.setInt(1, id+2);
				stmt.setInt(2, value+2);
				stmt.addBatch();
				stmt.executeBatch();
				Assert.assertEquals(2, stmt.getUpdateCount());
			}
			for (int i=1;i<3;i++) {
				try (PreparedStatement stmt = conn.prepareStatement(selectStmt)) {
					stmt.setInt(1, id+i);
					try (ResultSet rs = stmt.executeQuery()) {
						rs.next();

						Assert.assertEquals(value+i, rs.getInt(1));
					}
				}
			}
			try (PreparedStatement stmt = conn.prepareStatement("update upTable set aValue=?")) {
				stmt.setInt(1, updateValue);
				int rowCount = stmt.executeUpdate();
				Assert.assertEquals(3, rowCount);
			}
			for (int i=0;i<3;i++) {
				try (PreparedStatement stmt = conn.prepareStatement(selectStmt)) {
					stmt.setInt(1, id+1);
					try (ResultSet rs = stmt.executeQuery()) {
						rs.next();

						Assert.assertEquals(updateValue, rs.getInt(1));
					}
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldRetrieveSavedBlob", "arrays.db", (ITest) conn -> {
			conn.createStatement().execute("create table arraytest (key int, value text)");

			byte[] byteArray = randomByteArray();

			final int id = 442;
			try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO arraytest(key,value) VALUES (?, ?)")) {
				stmt.setInt(1, id);
				stmt.setBytes(2, byteArray);
				stmt.executeUpdate();
			}

			try (PreparedStatement stmt = conn.prepareStatement("SELECT value,key FROM arraytest where key = ?")) {
				stmt.setInt(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					Assert.assertTrue(rs.next());
					Assert.assertEquals(id, rs.getInt(2));
					byte[] array = rs.getBytes(1);
					Assert.assertArrayEquals(byteArray, array);

					array = (byte[]) rs.getObject(1);
					Assert.assertArrayEquals(byteArray, array);
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldRetrieveSavedBlob", "blobs.db", (ITest) conn -> {
			conn.createStatement().execute("create table blobtest (key int, value blob)");

			byte[] byteArray = randomByteArray();

			Blob blob=new ASQLConnectorBlob(new ByteArrayInputStream(byteArray), byteArray.length);
			Assert.assertArrayEquals(byteArray, blob.getBytes(1, byteArray.length));
			Assert.assertEquals(byteArray.length, blob.length());
			Assert.assertArrayEquals(Arrays.copyOfRange(byteArray, 1, byteArray.length - 2), blob.getBytes(2, byteArray.length - 3));


			int id = 441;
			try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO blobtest(key,value) VALUES (?, ?)")) {
				stmt.setInt(1, id);
				stmt.setBlob(2, new ByteArrayInputStream(byteArray), byteArray.length);
				stmt.executeUpdate();
			}

			try (PreparedStatement stmt = conn.prepareStatement("SELECT value,key FROM blobtest where key = ?")) {
				stmt.setInt(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					blob = rs.getBlob(1);
					Assert.assertArrayEquals(byteArray, blob.getBytes(1, byteArray.length));
					Assert.assertEquals(byteArray.length, blob.length());
					Assert.assertArrayEquals(Arrays.copyOfRange(byteArray, 1, byteArray.length - 2), blob.getBytes(2, byteArray.length - 3));

					blob = (Blob) rs.getObject(1);
					Assert.assertArrayEquals(byteArray, blob.getBytes(1, (int) blob.length()));
					Assert.assertArrayEquals(Arrays.copyOfRange(byteArray, 1, byteArray.length - 2), blob.getBytes(2, byteArray.length - 3));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldSaveAndRetrieveTimestamps", "timestamps.db", (ITest) conn -> {
			conn.createStatement()
					.execute("create table timestamptest (id integer primary key autoincrement, created_at timestamp)");

			long id;

			Calendar calendar = new GregorianCalendar(2016, Calendar.AUGUST, 15, 12, 0, 0);
			Timestamp timestamp = new Timestamp(calendar.getTimeInMillis() + 985);

			try (PreparedStatement stmt = conn.prepareStatement("insert into timestamptest (created_at) values (?)",
					PreparedStatement.RETURN_GENERATED_KEYS)) {
				stmt.setTimestamp(1, timestamp);
				stmt.executeUpdate();
				try (ResultSet rs = conn.createStatement().executeQuery("select last_insert_rowid();")) {
					rs.next();
					id = rs.getLong(1);
				}
			}

			try (PreparedStatement stmt = conn.prepareStatement("select created_at from timestamptest where id = ?")) {
				stmt.setLong(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertEquals(timestamp, rs.getTimestamp(1));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldSaveAndRetrieveTimestamps2", "timestamps.db", (ITest) conn -> {
			conn.createStatement()
					.execute("create table timestamptest (id integer primary key autoincrement, created_at timestamp)");

			Timestamp timestamp = new Timestamp(new GregorianCalendar(2016, Calendar.AUGUST, 15, 12, 0, 0).getTimeInMillis());
			long id = executeForGeneratedKey(conn, "insert into timestamptest (created_at) values (?)", timestamp);

			try (PreparedStatement stmt = conn.prepareStatement("select created_at from timestamptest where id = ?")) {
				stmt.setLong(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertEquals(timestamp, rs.getTimestamp(1));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldRetrieveSavedStringAsBlob", "timestamps.db", (ITest) conn -> {
			conn.createStatement().execute("CREATE TABLE stringblobtest (value CLOB)");

			String s = "a random test string";

			try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO stringblobtest (value) VALUES (?)")) {
				stmt.setClob(1, new ASQLConnectorClob(s));
				stmt.executeUpdate();
			}

			try (PreparedStatement stmt = conn.prepareStatement("SELECT value FROM stringblobtest")) {
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Clob clob = rs.getClob(1);
					Assert.assertEquals(s, clob.getSubString(1, s.length()));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldReturnGeneratedKeys", "simple-types.db", (ITest) conn -> {
			conn.createStatement().execute("create table simpletest (id integer primary key autoincrement, value varchar(255))");

			long id;
			String randomString = UUID.randomUUID().toString();
			try (PreparedStatement stmt = conn.prepareStatement("insert into simpletest (value) values (?)", PreparedStatement.RETURN_GENERATED_KEYS)) {
				stmt.setString(1, randomString);
				stmt.executeUpdate();
				try (ResultSet rs = stmt.getGeneratedKeys()) {
					rs.next();
					id = rs.getLong(1);
				}
			}

			try (PreparedStatement stmt = conn.prepareStatement("select value from simpletest where id = ?")) {
				stmt.setLong(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertEquals(randomString, rs.getString(1));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldReturnGeneratedKeys2", "simple-types.db", (ITest) conn -> {
			conn.createStatement()
					.execute("create table simpletest (id integer primary key autoincrement, value varchar(255))");

			String randomString = UUID.randomUUID().toString();
			long id = executeForGeneratedKey(conn, "insert into simpletest (value) values (?)", randomString);

			try (PreparedStatement stmt = conn.prepareStatement("select value from simpletest where id = ?")) {
				stmt.setLong(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertEquals(randomString, rs.getString(1));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldSaveAndRetrieveDates", "null-dates.db", (ITest) conn -> {
			conn.createStatement().execute("create table datetest (id integer primary key autoincrement, created_at date)");

			long id;

			Calendar calendar = new GregorianCalendar(2016, Calendar.AUGUST, 15);
			Date date = new Date(calendar.getTimeInMillis());

			try (PreparedStatement stmt = conn.prepareStatement("insert into datetest (created_at) values (?)", PreparedStatement.RETURN_GENERATED_KEYS)) {
				stmt.setDate(1, date);
				stmt.executeUpdate();
				try (ResultSet rs = conn.createStatement().executeQuery("select last_insert_rowid();")) {
					rs.next();
					id = rs.getLong(1);
				}
			}

			try (PreparedStatement stmt = conn.prepareStatement("select created_at from datetest where id = ?")) {
				stmt.setLong(1, id);
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertEquals(date, rs.getDate(1));
					Assert.assertEquals(date, new Date(rs.getTimestamp(1).getTime()));
					Assert.assertEquals(date, rs.getDate("created_at"));
				}
			}
		}
		});
		tests.add(new Object[]{
				"shouldRetrieveDefaultDates", "null-dates.db", (ITest) conn -> {
			conn.createStatement().execute("CREATE TABLE datetime_now_test (datetimecol TEXT NOT NULL DEFAULT (datetime('now')), unused TEXT)");
			conn.createStatement().executeUpdate("INSERT INTO datetime_now_test (unused) VALUES (null)");

			try (PreparedStatement stmt = conn.prepareStatement("SELECT datetimecol FROM datetime_now_test")) {
				try (ResultSet rs = stmt.executeQuery()) {
					rs.next();
					Assert.assertTrue(Pattern.compile("20\\d\\d-\\d\\d-\\d\\d.*").matcher(rs.getTimestamp("datetimecol").toString()).matches());
				}
			}
		}
		});
		return tests;
	}


	static final File DB_DIR = new File("/data/data/fr.distrimind.oss.asqlconnector/databases/");
	static final Random random = new Random(System.currentTimeMillis());

	/*static {
		registerDriver();
	}*/

	private static void registerDriver() {
		try {
			DriverManager.registerDriver((Driver) (Class
					.forName("fr.distrimind.oss.asqlconnector.ASQLConnectorDriver", true, ASQLConnectorATests.class.getClassLoader()).getConstructor().newInstance()));
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException |
				 NoSuchMethodException | InvocationTargetException e) {
			throw new AssertionFailedError(e.toString());
		}
	}
	private final String dbFileName;
	private final ITest test;
	public ASQLConnectorATests(String testName, String dbFileName, ITest test)
	{
		if (testName==null)
			throw new NullPointerException();
		if (dbFileName==null)
			throw new NullPointerException();
		if (test==null)
			throw new NullPointerException();
		this.dbFileName=dbFileName;
		this.test=test;
	}

	@Test
	public void launchTestA() throws SQLException {
		registerDriver();
		try (Connection conn = DriverManager.getConnection(createDatabase(dbFileName))) {
			test.test(conn);
		}
	}


	private static long executeForGeneratedKey(Connection conn, String query, Object... parameters) throws SQLException {
		return executeForGeneratedKeyWithList(conn, query, Arrays.asList(parameters));
	}

	private static long executeForGeneratedKeyWithList(Connection conn, String query, List<Object> parameters) throws SQLException {
		try (PreparedStatement stmt = conn.prepareStatement(query, PreparedStatement.RETURN_GENERATED_KEYS)) {
			for (int i = 0; i < parameters.size(); i++) {
				stmt.setObject(i + 1, parameters.get(i));
			}
			stmt.executeUpdate();
			try (ResultSet rs = stmt.getGeneratedKeys()) {
				rs.next();
				return rs.getLong(1);
			}
		}
	}

	private String createDatabase(String filename) {
		DB_DIR.mkdirs();
		Assert.assertTrue(DB_DIR.exists());

		File dbFile = new File(DB_DIR, filename);
		dbFile.delete();
		Assert.assertFalse(dbFile.exists());

		return "jdbc:asqlconnector:" + dbFile.getAbsolutePath();
	}

	private static byte[] randomByteArray() {
		int blobSize = 1000 + random.nextInt(10_000);
		byte[] aBlob = new byte[blobSize];
		random.nextBytes(aBlob);
		return aBlob;
	}
}
