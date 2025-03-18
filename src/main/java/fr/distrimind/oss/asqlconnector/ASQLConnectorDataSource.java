package fr.distrimind.oss.asqlconnector;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class ASQLConnectorDataSource implements DataSource {
	protected String description = "Android Sqlite Data Source";
	protected String packageName;
	protected String databaseName;
	Connection connection = null;

	public ASQLConnectorDataSource() {

	}

	public ASQLConnectorDataSource(String packageName, String databaseName) {
		setPackageName(packageName);
		setDatabaseName(databaseName);
	}

	@Override
	public Connection getConnection() throws SQLException {
		String url = "jdbc:asqlconnector:" + "/data/data/" + packageName + "/" + databaseName + ".db";
		connection = new ASQLConnectorDriver().connect(url, new Properties());
		return connection;
	}

	@Override
	public Connection getConnection(String username, String password)
			throws SQLException {
		return getConnection();
	}
	private static final String logFileName="asqlconnector.log";
	@Override
	public PrintWriter getLogWriter() throws SQLException {
		PrintWriter logWriter = null;

		try {
			logWriter = new PrintWriter(logFileName);
		} catch (FileNotFoundException e) {
			Log.e("File "+logFileName+" not found", e);
		}
		return logWriter;
	}

	@Override
	public void setLogWriter(PrintWriter out) throws SQLException {
		try {
			DriverManager.setLogWriter(new PrintWriter(logFileName));
		} catch (FileNotFoundException e) {
			Log.e("File "+logFileName+" not found", e);
		}
	}

	@Override
	public int getLoginTimeout() throws SQLException {
		return 0;
	}

	@Override
	public void setLoginTimeout(int seconds) throws SQLException {
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String desc) {
		description = desc;
	}

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
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

	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		return null;
	}

}
