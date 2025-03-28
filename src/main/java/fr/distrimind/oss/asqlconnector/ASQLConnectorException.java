package fr.distrimind.oss.asqlconnector;

import android.database.SQLException;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.Objects;


public class ASQLConnectorException extends java.sql.SQLException {
	private static final long serialVersionUID = -7299376329007161001L;

	/**
	 * The exception that this exception was created for.
	 */
	SQLException androidSQLiteException;

	/**
	 * Create a hard java.sql.SQLException from the RuntimeException android.database.SQLException.
	 */
	public ASQLConnectorException(SQLException sqlException) {
		this.androidSQLiteException = sqlException;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ASQLConnectorException)
			return androidSQLiteException.equals(((ASQLConnectorException) o).androidSQLiteException);
		else
			return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(androidSQLiteException);
	}

	@Override
	public Throwable fillInStackTrace() {
		return androidSQLiteException.fillInStackTrace();
	}

	@Override
	public Throwable getCause() {
		return androidSQLiteException.getCause();
	}

	@Override
	public String getLocalizedMessage() {
		return androidSQLiteException.getLocalizedMessage();
	}

	@Override
	public String getMessage() {
		return androidSQLiteException.getMessage();
	}

	@Override
	public StackTraceElement[] getStackTrace() {
		return androidSQLiteException.getStackTrace();
	}

	@Override
	public void printStackTrace() {
		Log.error(androidSQLiteException);
	}

	@Override
	public void printStackTrace(PrintStream err) {
		androidSQLiteException.printStackTrace(err);
	}

	@Override
	public void printStackTrace(PrintWriter err) {
		androidSQLiteException.printStackTrace(err);
	}

	@Override
	public String toString() {
		return androidSQLiteException.toString();
	}

	public SQLException getAndroidSQLiteException() {
		return androidSQLiteException;
	}
}
