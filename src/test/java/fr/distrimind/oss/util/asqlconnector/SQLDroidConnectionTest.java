package fr.distrimind.oss.util.asqlconnector;


import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;


@SuppressWarnings("ResultOfMethodCallIgnored")
public class SQLDroidConnectionTest {
    public static final int CREATE_IF_NECESSARY = 268435456;
    public static final int OPEN_READWRITE = 0;
    @SuppressWarnings("EmptyTryBlock")
	@Test
    public void shouldConnectToEmptyFile() throws SQLException, IOException {
        Properties properties = new Properties();
        properties.put(SQLDroidDriver.ADDITONAL_DATABASE_FLAGS,
                CREATE_IF_NECESSARY
                        | OPEN_READWRITE);

        File dbFile = cleanDbFile("exisising-file.db");
        try (FileOutputStream ignored = new FileOutputStream(dbFile)) {
        }
        Assert.assertTrue(dbFile.exists());

        String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        Connection conn = new SQLDroidDriver().connect(jdbcUrl, properties);
        Assert.assertFalse(conn.isClosed());
        conn.close();
    }

    @Test
    public void shouldSupportQueryPartOfURL() throws SQLException {
        File dbFile = cleanDbFile("query-test.db");
        String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath() + "?timeout=30";
        Connection conn = new SQLDroidDriver().connect(jdbcUrl, new Properties());
        Assert.assertFalse(conn.isClosed());
        conn.close();
    }

    @Test
    public void shouldDealWithInvalidDirectoryGivenAsFile() throws SQLException, IOException {
        File dbFile = cleanDbFile("db-as-dir.db");
        final String jdbcUrl = "jdbc:sqlite:" + dbFile.getParentFile().getAbsolutePath();
        try(Connection ignored=new SQLDroidDriver().connect(jdbcUrl, new Properties()))
        {
            Assert.fail();
        }
        catch (SQLException e)
        {
            Assert.assertEquals("Can't create", e.getMessage());
            Assert.assertNotNull(dbFile.getParent());
            Assert.assertFalse(dbFile.getParent().isEmpty());
        }

    }

    @SuppressWarnings("EmptyTryBlock")
	@Test
    public void shouldDealWithDirectoryNameAsExistingFile() throws SQLException, IOException {
        File dbDir = cleanDbFile("subdir");
        try (FileOutputStream ignored = new FileOutputStream(dbDir)) {
        }
        File dbFile = new File(dbDir, "dbfile.db");
        final String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        try(Connection ignored=new SQLDroidDriver().connect(jdbcUrl, new Properties()))
        {
            Assert.fail();
        }
        catch (SQLException e)
        {
            Assert.assertEquals("Can't create", e.getMessage());
            Assert.assertNotNull(dbFile.getAbsolutePath());
            Assert.assertFalse(dbFile.getAbsolutePath().isEmpty());
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

        final String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        new SQLDroidDriver().connect(jdbcUrl, new Properties()).close();
        Assert.assertTrue(dbFile.exists());
    }

    @Test
    public void shouldSupportReconnectAfterAbortedTransaction() throws SQLException {
        File dbFile = cleanDbFile("aborted-transaction.db");
        final String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        try (Connection connection = new SQLDroidDriver().connect(jdbcUrl, new Properties())) {
            connection.setAutoCommit(false);
        }
        Connection conn = new SQLDroidDriver().connect(jdbcUrl, new Properties());
        Assert.assertFalse(conn.isClosed());
        conn.close();
    }

    @Test
    public void shouldAllowNewTransactionAfterCommit() throws SQLException {
        File dbFile = cleanDbFile("transaction.db");
        final String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        try (Connection connection = new SQLDroidDriver().connect(jdbcUrl, new Properties())) {
            connection.setAutoCommit(false);
            connection.commit();
        }

        try (Connection connection = new SQLDroidDriver().connect(jdbcUrl, new Properties())) {
            // The following line should not throw an exception "database is
            // locked"
            connection.setAutoCommit(false);
        }
    }

    @Test
    public void shouldAllowMultipleConnections() throws SQLException {
        File dbFile = cleanDbFile("multiconnect.db");
        final String jdbcUrl = "jdbc:sqlite:" + dbFile.getAbsolutePath();
        Connection connection1 = new SQLDroidDriver().connect(jdbcUrl, new Properties());
        Connection connection2 = new SQLDroidDriver().connect(jdbcUrl, new Properties());
        connection1.close();
        connection2.createStatement().executeQuery("select 1");
        connection2.close();
    }

    private static final File DB_DIR = new File("./target/data/fr.distrimind.oss.util.asqlconnector/databases/");

    private File cleanDbFile(String filename) {
        DB_DIR.mkdirs();
        Assert.assertTrue(DB_DIR.isDirectory());

        File dbFile = new File(DB_DIR, filename);
        dbFile.delete();
        Assert.assertFalse(dbFile.exists());
        return dbFile;
    }
}
