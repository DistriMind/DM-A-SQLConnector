package fr.distrimind.oss.asqlconnector;

import android.database.Cursor;

import java.lang.reflect.Method;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class ASQLConnectorResultSetMetaData implements ResultSetMetaData {

	public static final String NOT_IMPLEMENTED = " ********************* not implemented @ ";
	public static final String _LINE_ = " line ";
	private static Method getTypeMethod;

	static {
		try {
			getTypeMethod = Cursor.class.getMethod("getType", int.class);
		} catch (Exception e) {
			getTypeMethod = null;
		}
	}

	private final Cursor cursor;

	public ASQLConnectorResultSetMetaData(Cursor cursor) {
		if (cursor == null) {
			throw new NullPointerException("Cursor required to be not null.");
		}
		this.cursor = cursor;
	}

	static int getType(Cursor cursor, int column) {
		if (getTypeMethod != null) {
			try {
				return (Integer) getTypeMethod.invoke(cursor, column);
			} catch (Exception ignored) {
			}
		}
		return Types.OTHER; // return something that can be used to understand that the type is unknown.
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		return getTableName(column);
	}

	@Override
	public String getColumnClassName(int column) {
		// TODO In Xerial this is implemented as return "java.lang.Object";
		// TODO To a lookup based on getColumnType
		throw new UnsupportedOperationException("getColumnClassName not implemented yet");
	}

	@Override
	public int getColumnCount() {
		return cursor.getColumnCount();
	}

	@Override
	public int getColumnDisplaySize(int column) {
		return Integer.MAX_VALUE;
	}

	@Override
	public String getColumnLabel(int column) {
		return cursor.getColumnName(column - 1);
	}

	@Override
	public String getColumnName(int column) {
		return cursor.getColumnName(column - 1);
	}

	@Override
	public int getColumnType(int column) {
		int oldPos = cursor.getPosition();
		boolean moved = false;
		if (cursor.isBeforeFirst() || cursor.isAfterLast()) {
			boolean resultSetEmpty = cursor.getCount() == 0 || cursor.isAfterLast();
			if (resultSetEmpty) {
				return Types.NULL;
			}
			cursor.moveToFirst();
			moved = true;
		}
		int nativeType = getType(cursor, column - 1);
		int type;
		switch (nativeType) {
			case 0: // Cursor.FIELD_TYPE_NULL:
				type = Types.NULL;
				break;
			case 1: // Cursor.FIELD_TYPE_INTEGER:
				type = Types.INTEGER;
				break;
			case 2: // Cursor.FIELD_TYPE_FLOAT:
				type = Types.FLOAT;
				break;
			case 3: // Cursor.FIELD_TYPE_STRING:
				type = Types.VARCHAR;
				break;
			case 4: // Cursor.FIELD_TYPE_BLOB:
				type = Types.BLOB;
				break;
			default:
				type = Types.NULL;
				break;
		}
		if (moved) {
			cursor.moveToPosition(oldPos);
		}
		return type;
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		switch (getColumnType(column)) {
			case Types.NULL:
				return "NULL";
			case Types.INTEGER:
				return "INTEGER";
			case Types.FLOAT:
				return "FLOAT";
			case Types.VARCHAR:
				return "VARCHAR";
			case Types.BLOB:
				return "BLOB";
			default:
				return "OTHER";
		}
	}

	@Override
	public int getPrecision(int column) {
		throw new UnsupportedOperationException("getPrecision not implemented yet");
	}

	@Override
	public int getScale(int column) {
		throw new UnsupportedOperationException("getScale not implemented yet");
	}

	@Override
	public String getSchemaName(int column) {
		return "";
	}

	@Override
	public String getTableName(int column) {
		// TODO Supported in Xerial driver with db.column_table_name(stmt.pointer, checkCol(col))
		throw new UnsupportedOperationException("getTableName not implemented yet");
	}

	@Override
	public boolean isAutoIncrement(int column) {
		throw new UnsupportedOperationException("isAutoIncrement not implemented yet");
	}

	@Override
	public boolean isCaseSensitive(int column) {
		return true;
	}

	@Override
	public boolean isCurrency(int column) {
		return false;
	}

	@Override
	public boolean isDefinitelyWritable(int column) {
		// TODO Evaluate if this is a sufficient implementation (if so, delete comment and log)
		Log.error(() -> NOT_IMPLEMENTED + DebugPrinter.getFileName() + _LINE_
				+ DebugPrinter.getLineNumber());
		return false;
	}

	@Override
	public int isNullable(int column) {
		throw new UnsupportedOperationException("isNullable not implemented yet");
	}

	@Override
	public boolean isReadOnly(int column) {
		// TODO Evaluate if the implementation is sufficient (if so, delete comment and log)
		Log.error(() -> NOT_IMPLEMENTED + DebugPrinter.getFileName() + _LINE_
				+ DebugPrinter.getLineNumber());
		return true;
	}

	@Override
	public boolean isSearchable(int column) {
		// TODO Evaluate if the implementation is sufficient (if so, delete comment and log)
		Log.error(() -> NOT_IMPLEMENTED + DebugPrinter.getFileName() + _LINE_
				+ DebugPrinter.getLineNumber());
		return true;
	}

	@Override
	public boolean isSigned(int column) {
		// TODO Evaluate if the implementation is sufficient (if so, delete comment and log)
		Log.error(() -> NOT_IMPLEMENTED + DebugPrinter.getFileName() + _LINE_
				+ DebugPrinter.getLineNumber());
		return true;
	}

	@Override
	public boolean isWritable(int column) {
		// TODO Evaluate if the implementation is sufficient (if so, delete comment and log)
		Log.error(() -> NOT_IMPLEMENTED + DebugPrinter.getFileName() + _LINE_
				+ DebugPrinter.getLineNumber());
		return false;
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
}
