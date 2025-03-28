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
	SQLException androidSqlLiteException;

	/**
	 * Create a hard java.sql.SQLException from the RuntimeException android.database.SQLException.
	 */
	public ASQLConnectorException(SQLException sqlException) {
		this.androidSqlLiteException = sqlException;
	}

	@Override
	public boolean equals(Object o) {
		if (o instanceof ASQLConnectorException)
			return androidSqlLiteException.equals(((ASQLConnectorException) o).androidSqlLiteException);
		else
			return false;
	}

	@Override
	public int hashCode() {
		return Objects.hashCode(androidSqlLiteException);
	}

	@Override
	public Throwable fillInStackTrace() {
		return androidSqlLiteException.fillInStackTrace();
	}

	@Override
	public Throwable getCause() {
		return androidSqlLiteException.getCause();
	}

	@Override
	public String getLocalizedMessage() {
		return androidSqlLiteException.getLocalizedMessage();
	}

	@Override
	public String getMessage() {
		return androidSqlLiteException.getMessage();
	}

	@Override
	public StackTraceElement[] getStackTrace() {
		return androidSqlLiteException.getStackTrace();
	}

	@Override
	public void printStackTrace() {
		Log.error(androidSqlLiteException);
	}

	@Override
	public void printStackTrace(PrintStream err) {
		androidSqlLiteException.printStackTrace(err);
	}

	@Override
	public void printStackTrace(PrintWriter err) {
		androidSqlLiteException.printStackTrace(err);
	}

	@Override
	public String toString() {
		return androidSqlLiteException.toString();
	}

	public SQLException getAndroidSqlLiteException() {
		return androidSqlLiteException;
	}
}
