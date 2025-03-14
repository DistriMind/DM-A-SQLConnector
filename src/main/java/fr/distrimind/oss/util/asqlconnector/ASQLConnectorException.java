package fr.distrimind.oss.util.asqlconnector;

import java.io.PrintStream;
import java.io.PrintWriter;

import android.database.SQLException;



public class ASQLConnectorException extends java.sql.SQLException {
  private static final long serialVersionUID = -7299376329007161001L;

  /** The exception that this exception was created for. */
  SQLException sqlException;
  
  /** Create a hard java.sql.SQLException from the RuntimeException android.database.SQLException. */ 
  public ASQLConnectorException(SQLException sqlException) {
    this.sqlException = sqlException;
  }

  @Override
  public boolean equals(Object o) {
    return sqlException.equals(o);
  }

  @Override
  public Throwable fillInStackTrace() {
    return sqlException.fillInStackTrace();
  }

  @Override
  public Throwable getCause() {
    return sqlException.getCause();
  }

  @Override
  public String getLocalizedMessage() {
    return sqlException.getLocalizedMessage();
  }
  @Override
  public String getMessage() {
    return sqlException.getMessage();
  }

  @Override
  public StackTraceElement[] getStackTrace() {
    return sqlException.getStackTrace();
  }
  @Override
  public void printStackTrace() {
    sqlException.printStackTrace();
  }

  @Override
  public void printStackTrace(PrintStream err) {
    sqlException.printStackTrace(err);
  }

  @Override
  public void printStackTrace(PrintWriter err) {
    sqlException.printStackTrace(err);
  }
  @Override
  public String toString() {
    return sqlException.toString();
  }
  
}
