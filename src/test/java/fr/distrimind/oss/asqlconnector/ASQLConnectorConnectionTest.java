package fr.distrimind.oss.asqlconnector;


import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


@SuppressWarnings("ResultOfMethodCallIgnored")
public class ASQLConnectorConnectionTest {
	public static final int CREATE_IF_NECESSARY = 268435456;
	public static final int OPEN_READWRITE = 0;
	private static final File DB_DIR = new File("/data/data/fr.distrimind.oss.asqlconnector/databases/");
	public static final String JDBC_ASQLCONNECTOR = "jdbc:asqlconnector:";

	public static boolean checkFolderRecursive(File folderPath) throws IOException {
		if (!(folderPath.exists())) {
			File parent = folderPath.getParentFile();
			if (parent != null)
				checkFolderRecursive(parent);
			if (!folderPath.mkdir())
				throw new IOException();
			return true;
		}
		return false;
	}

	@SuppressWarnings("EmptyTryBlock")
	@Test
	public void shouldConnectToEmptyFile() throws SQLException, IOException {
		Properties properties = new Properties();
		properties.put(ASQLConnectorDriver.ADDITONAL_DATABASE_FLAGS,
				CREATE_IF_NECESSARY
						| OPEN_READWRITE);

		File dbFile = cleanDbFile("exisising-file.db");
		try (FileOutputStream ignored = new FileOutputStream(dbFile)) {
		}
		Assert.assertTrue(dbFile.exists());

		String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath();
		try(Connection conn = new ASQLConnectorDriver().connect(jdbcUrl, properties)) {
			Assert.assertFalse(conn.isClosed());
		}
	}

	@Test
	public void shouldSupportQueryPartOfGetURL() throws SQLException, IOException {
		File dbFile = cleanDbFile("query-test.db");
		String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath() + "?timeout=30";
		try(Connection conn = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
			Assert.assertFalse(conn.isClosed());
		}
	}

	@Test
	public void shouldDealWithInvalidDirectoryGivenAsFile() throws IOException {
		File dbFile = cleanDbFile("db-as-dir.db");
		final String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath();
		try (Connection ignored = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
			Assert.assertTrue(dbFile.exists());
			Assert.assertTrue(dbFile.isFile());
		} catch (SQLException e) {
			Assert.assertEquals("Can't create", e.getMessage());
			Assert.assertNotNull(dbFile.getParent());
			Assert.assertFalse(dbFile.getParent().isEmpty());
			Assert.fail();
		}

	}

	@SuppressWarnings("EmptyTryBlock")
	@Test
	public void shouldDealWithDirectoryNameAsExistingFile() throws IOException {
		File dbDir = cleanDbDirectory("subdir");
		File dbFile = new File(dbDir, "dbfile.db");
		final String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath();
		try (Connection ignored = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
			Assert.assertTrue(dbFile.exists());
			Assert.assertTrue(dbFile.isFile());
		} catch (SQLException e) {
			Assert.assertEquals("Can't create", e.getMessage());
			Assert.assertNotNull(dbFile.getAbsolutePath());
			Assert.assertFalse(dbFile.getAbsolutePath().isEmpty());
			Assert.fail();
		}
	}

	@Test
	public void shouldCreateMissingSubdirectory() throws SQLException {
		DB_DIR.mkdirs();
		Assert.assertTrue(DB_DIR.isDirectory());
		File dbSubdir = new File(DB_DIR, "non-existing-dir");
		File dbFile = new File(dbSubdir, "database.db");
		dbFile.delete();
		dbSubdir.delete();
		Assert.assertFalse(dbSubdir.exists());

		final String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath();
		new ASQLConnectorDriver().connect(jdbcUrl, new Properties()).close();
		Assert.assertTrue(dbFile.exists());
	}

	@Test
	@SuppressWarnings("PMD.CloseResource")
	public void shouldSupportReconnectAfterAbortedTransaction() throws SQLException, IOException {
		File dbFile = cleanDbFile("aborted-transaction.db");
		final String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath();

		{
			Connection conn;
			try (Connection connection = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
				Assert.assertTrue(dbFile.exists());
				Assert.assertFalse(connection.isClosed());
				conn = connection;
				connection.setAutoCommit(false);
				Assert.assertFalse(connection.isClosed());
			}
			Assert.assertTrue(conn.isClosed());
		}
		{
			Connection conn;
			try (Connection connection = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
				Assert.assertFalse(connection.isClosed());
				conn = connection;
			}
			Assert.assertTrue(conn.isClosed());
		}
	}

	@Test
	public void shouldAllowNewTransactionAfterCommit() throws SQLException, IOException {
		File dbFile = cleanDbFile("transaction.db");
		final String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath();
		try (Connection connection = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
			connection.setAutoCommit(false);
			connection.commit();
		}

		try (Connection connection = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
			// The following line should not throw an exception "database is
			// locked"
			connection.setAutoCommit(false);
		}
	}

	@Test
	public void shouldAllowMultipleConnections() throws SQLException, IOException {
		File dbFile = cleanDbFile("multiconnect.db");
		final String jdbcUrl = JDBC_ASQLCONNECTOR + dbFile.getAbsolutePath();
		try(Connection ignored = new ASQLConnectorDriver().connect(jdbcUrl, new Properties())) {
			try(Connection connection2 = new ASQLConnectorDriver().connect(jdbcUrl, new Properties()))
			{
				connection2.createStatement().executeQuery("select 1");
			}
		}
	}

	private File cleanDbDirectory(String dirName) throws IOException {
		File r = new File(DB_DIR, dirName);
		checkFolderRecursive(r);
		Assert.assertTrue(DB_DIR.exists());
		Assert.assertTrue(r.exists());
		Assert.assertTrue(r.isDirectory());
		return r;
	}

	private File cleanDbFile(String filename) throws IOException {

		checkFolderRecursive(DB_DIR);
		Assert.assertTrue(DB_DIR.isDirectory());

		File dbFile = new File(DB_DIR, filename);
		dbFile.delete();
		Assert.assertFalse(dbFile.exists());
		return dbFile;
	}
}
