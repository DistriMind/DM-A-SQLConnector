package fr.distrimind.oss.asqlconnector;

import fr.distrimind.oss.flexilogxml.common.ReflectionTools;
import junit.framework.AssertionFailedError;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;


/**
 * This is a refactoring is ASQLConnectorTest that shares quite a bit of common code between test methods.
 * I'm not sure which approach reads the best.
 *
 * @author Johannes Brodwall <johannes@brodwall.com>
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(Parameterized.class)
public class ASQLConnectorBTests {

	@Parameterized.Parameters(name = "{index}: {0}")
	public static Collection<Object[]> getTests()
	{
		return ASQLConnectorATests.getTests();
	}
	static final File DB_DIR = ASQLConnectorATests.DB_DIR;
	private Connection conn;
	private final ITest test;
	public ASQLConnectorBTests(String testName, String dbFileName, ITest test)
	{
		if (testName==null)
			throw new NullPointerException();
		if (dbFileName==null)
			throw new NullPointerException();
		if (test==null)
			throw new NullPointerException();
		this.test=test;
	}
	@Test
	public void launchTestB() throws SQLException {
		test.test(conn);
	}


	@Before
	public void createConnection() throws SQLException {
		try {
			DriverManager.registerDriver((Driver) (ReflectionTools.getClassLoader().loadClass("fr.distrimind.oss.asqlconnector.ASQLConnectorDriver").getConstructor().newInstance()));
		} catch (InstantiationException | IllegalAccessException | ClassNotFoundException | SQLException |
				 NoSuchMethodException | InvocationTargetException e) {
			throw new AssertionFailedError(e.toString());
		}
		this.conn = DriverManager.getConnection(createDatabase());
	}

	@After
	public void closeConnection() throws SQLException {
		conn.close();
	}

	private String createDatabase() {
		String filename = "asqlconnector-test.db";
		DB_DIR.mkdirs();
		Assert.assertTrue(DB_DIR.exists());

		File dbFile = new File(DB_DIR, filename);
		dbFile.delete();
		Assert.assertFalse(dbFile.exists());

		return "jdbc:asqlconnector:" + dbFile.getAbsolutePath();
	}


}
