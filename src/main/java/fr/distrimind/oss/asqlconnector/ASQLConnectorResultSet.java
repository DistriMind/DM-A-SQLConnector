package fr.distrimind.oss.asqlconnector;

import android.database.Cursor;
import fr.distrimind.oss.flexilogxml.common.FlexiLogXML;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;


public class ASQLConnectorResultSet implements ResultSet {
	public static final String UPDATE_ASCII_STREAM_NOT_SUPPORTED = "updateAsciiStream not supported";
	private static final String DATE_PATTERN = "yyyy-MM-dd";

	private static final String TIMESTAMP_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
	private static final String TIMESTAMP_PATTERN_NO_MILLIS = "yyyy-MM-dd HH:mm:ss";
	public static final String SPACE_PIPE_SPACE = " | ";
	public static final String UPDATE_BIG_DECIMAL_NOT_SUPPORTED = "updateBigDecimal not supported";
	public static final String UPDATE_BINARY_STREAM_NOT_SUPPORTED = "updateBinaryStream not supported";
	public static final String UPDATE_CHARACTER_STREAM_NOT_SUPPORTED = "updateCharacterStream not supported";
	public static final String UPDATE_OBJECT_NOT_SUPPORTED = "updateObject not supported";
	public static final String UPDATE_N_CHARACTER_STREAM_NOT_SUPPORTED = "updateNCharacterStream not supported";
	public static final String UPDATE_BLOB_NOT_SUPPORTED = "updateBlob not supported";
	public static final String UPDATE_CLOB_NOT_SUPPORTED = "updateClob not supported";
	public static final String UPDATE_N_CLOB_NOT_SUPPORTED = "updateNClob not supported";

	public static boolean dump = false;

	private final Cursor c;
	private int lastColumnRead; // JDBC style column index starting from 1

	// TODO: Implement behavior (as Xerial driver)
	private int limitRows = 0;

	public ASQLConnectorResultSet(Cursor c) throws SQLException {
		this.c = c;
		if (dump) {
			dumpResultSet();
		}
	}
	@SuppressWarnings("PMD.CloseResource")
	private void dumpResultSet() throws SQLException {
		ResultSet rs = this;
		boolean headerDrawn = false;
		while (rs.next()) {
			if (!headerDrawn) {
				for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
					Log.info(SPACE_PIPE_SPACE);
					Log.info(rs.getMetaData().getColumnLabel(i));
				}
				Log.info(SPACE_PIPE_SPACE);
				headerDrawn = true;
			}
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				Log.info(SPACE_PIPE_SPACE);
				Log.info(rs.getString(i));
				if (rs.getString(i) != null) {
					Log.info(" (" + rs.getString(i).length() + ")");
				}
			}
			Log.info(SPACE_PIPE_SPACE);
		}
		rs.beforeFirst();
	}


	/**
	 * convert JDBC column index (one-based) to sqlite column index (zero-based)
	 *
	 * @param colID the column id
	 */
	private int ci(int colID) {
		return colID - 1;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		// Not supported by SQLite
		throw new SQLFeatureNotSupportedException("ResultSet is TYPE_FORWARD_ONLY");
	}

	@Override
	public void afterLast() throws SQLException {
		try {
			c.moveToLast();
			c.moveToNext();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public void beforeFirst() throws SQLException {
		try {
			c.moveToFirst();
			c.moveToPrevious();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		// Not supported by SQLite
		throw new SQLFeatureNotSupportedException("ResultSet.cancelRowUpdates not supported");
	}

	@Override
	public void clearWarnings() {
		// TODO: Evaluate if implementation is sufficient (if so, delete comment and log)
		Log.error(() -> " ********************* not implemented @ " + DebugPrinter.getFileName() + " line " + DebugPrinter.getLineNumber());
	}

	@Override
	public void close() throws SQLException {
		try {
			if (c != null) {
				c.close();
			}
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public void deleteRow() throws SQLException {
		// Not supported by SQLite
		throw new SQLFeatureNotSupportedException("ResultSet.deleteRow not supported");
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		try {
			// JDBC style column index starts from 1; Android database cursor has zero-based index
			return (c.getColumnIndexOrThrow(columnName) + 1);
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean first() throws SQLException {
		try {
			return c.moveToFirst();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public Array getArray(int colID) throws SQLException {
		// Not supported by SQLite
		throw new SQLFeatureNotSupportedException("getArray is not supported");
	}


	@Override
	public Array getArray(String columnName) throws SQLException {
		return getArray(findColumn(columnName));
	}

	@Override
	public InputStream getAsciiStream(int colID) throws SQLException {
		String s = getString(colID);
		return s != null ? new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8)) : null;
	}

	@Override
	public InputStream getAsciiStream(String columnName) throws SQLException {
		return getAsciiStream(findColumn(columnName));
	}

	@Override
	public BigDecimal getBigDecimal(int colID) throws SQLException {
		Object o=getObject(colID);
		if (o==null)
			return null;
		else if (o instanceof BigDecimal)
			return (BigDecimal)o;
		else if (o instanceof Long)
			return BigDecimal.valueOf((long)o);
		else if (o instanceof Double)
			return BigDecimal.valueOf((double)o);
		else if (o instanceof String)
			return new BigDecimal((String)o);
		else
			throw new SQLException();
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return getBigDecimal(findColumn(columnName));
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int colID, int scale) throws SQLException {
		return getBigDecimal(colID).setScale(scale, RoundingMode.CEILING);
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(String columnName, int scale)
			throws SQLException {
		return getBigDecimal(findColumn(columnName), scale);
	}

	@Override
	public InputStream getBinaryStream(int colID) throws SQLException {
		byte[] bytes = getBytes(colID);
		return bytes != null ? new ByteArrayInputStream(bytes) : null;
	}

	@Override
	public InputStream getBinaryStream(String columnName) throws SQLException {
		return getBinaryStream(findColumn(columnName));
	}

	@Override
	public ASQLConnectorBlob getBlob(int index) throws SQLException {
		byte[] b=getBlobImpl(index);

		if (b == null) {
			return null;
		}
		if (b[0]!=ASQLConnectorBlobType.BYTE_ARRAY_TYPE)
			return new ASQLConnectorBlob(b);
		ASQLConnectorBlob r=new ASQLConnectorBlob();
		r.b=b;
		return r;
	}

	@Override
	public Blob getBlob(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getBlob(index);
	}

	@Override
	public boolean getBoolean(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getShort(ci(index)) != 0;
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getBoolean(index);
	}

	@Override
	public byte getByte(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return (byte) c.getShort(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getByte(index);
	}

	@Override
	public byte[] getBytes(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return Utils.getUntypedBytesArray(getStringImpl(index));

		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	private byte[] getBlobImpl(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getBlob(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getBytes(index);
	}

	@Override
	public Reader getCharacterStream(int colID) throws SQLException {
		String s = getString(colID);
		return s != null ? new StringReader(s) : null;
	}

	@Override
	public Reader getCharacterStream(String columnName) throws SQLException {
		return getCharacterStream(findColumn(columnName));
	}

	@Override
	public ASQLConnectorClob getClob(int colID) throws SQLException {
		byte[] b=getBlobImpl(colID);

		if (b == null) {
			return null;
		}
		if (b[0]!=ASQLConnectorBlobType.STRING_TYPE)
			throw new SQLException();
		return new ASQLConnectorClob(new String(b, 1, b.length-1, StandardCharsets.UTF_8));
	}

	@Override
	public Clob getClob(String colName) throws SQLException {
		int index = findColumn(colName);
		return getClob(index);
	}

	@Override
	public int getConcurrency() {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLFeatureNotSupportedException("getCursorName not supported");
	}

	@Override
	public Date getDate(int index) throws SQLException {
		try {
			lastColumnRead = index;
			ResultSetMetaData md = getMetaData();
			Date date = null;
			switch (md.getColumnType(index)) {
				case Types.NULL:
					return null;
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.DATE:
					date = new Date(getLong(index));
					break;
				default:
					// format 2011-07-11 11:36:30.009
					try {
						SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_PATTERN, FlexiLogXML.getLocale());
						java.util.Date parsedDate = dateFormat.parse(getStringImpl(index));
						date = new Date(parsedDate.getTime());
					} catch (Exception e) {
						Log.error(() -> "Impossible to parse date", e);
					}
					break;

			}
			return date;
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public Date getDate(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getDate(index);
	}

	@Override
	public Date getDate(int colID, Calendar cal) throws SQLException {
		Calendar c=((Calendar)cal.clone());
		c.setTimeInMillis(getTimestamp(colID).getTime());
		return new Date(c.getTimeInMillis());
	}

	@Override
	public Date getDate(String columnName, Calendar cal) throws SQLException {
		return getDate(findColumn(columnName), cal);
	}

	@Override
	public double getDouble(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getDouble(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getDouble(index);
	}

	@Override
	public int getFetchDirection() {
		return ResultSet.FETCH_FORWARD;
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		if (direction != ResultSet.FETCH_FORWARD) {
			throw new SQLException("only FETCH_FORWARD direction supported");
		}
	}

	@Override
	public int getFetchSize() {
		return limitRows;
	}

	@Override
	public void setFetchSize(int rows) {
		// TODO: Implement as max row number for next()
		if (rows != 0) {
			throw new UnsupportedOperationException("Not implemented yet");
		}
		limitRows = rows;
	}

	@Override
	public float getFloat(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getFloat(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getFloat(index);
	}

	@Override
	public int getInt(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getInt(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getInt(index);
	}

	@Override
	public long getLong(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getLong(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getLong(index);
	}

	@Override
	public ResultSetMetaData getMetaData() {
		return new ASQLConnectorResultSetMetaData(c);
	}

	@Override
	public Object getObject(int colID) throws SQLException {
		lastColumnRead = colID;
		int newIndex = ci(colID);
		switch (ASQLConnectorResultSetMetaData.getType(c, newIndex)) {
			case 4: // Cursor.FIELD_TYPE_BLOB:
				//CONVERT TO BYTE[] OBJECT
			{
				byte[] blob=c.getBlob(newIndex);
				switch (blob[0])
				{
					case ASQLConnectorBlobType.NULL_TYPE:
						return null;
					case ASQLConnectorBlobType.STRING_TYPE:
						return new ASQLConnectorClob(new String(blob, 1, blob.length-1, StandardCharsets.UTF_8));
					default: {
						ASQLConnectorBlob r=new ASQLConnectorBlob();
						r.b=blob;
						return r;
					}
				}

			}
			case 2: // Cursor.FIELD_TYPE_FLOAT:
				return c.getDouble(newIndex);
			case 1: // Cursor.FIELD_TYPE_INTEGER:
				return c.getLong(newIndex);
			case 3: // Cursor.FIELD_TYPE_STRING:
			{
				String s = c.getString(newIndex);
				switch (s.charAt(0)) {
					case ASLConnectorStringType.NULL_TYPE:
						return null;
					case ASLConnectorStringType.BIG_DECIMAL_TYPE:
						return Utils.bigDecimalFromString(s);
					case ASLConnectorStringType.STRING_TYPE:
						return Utils.getUntypedString(s);
					case ASLConnectorStringType.BYTE_ARRAY_TYPE:
						return Utils.getUntypedBytesArray(s);
					default:
						return s;
				}
			}
			case 0: // Cursor.FIELD_TYPE_NULL:
				return null;
			default:

				return c.getString(newIndex);
		}
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getObject(index);
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> clazz)
			throws SQLException {
		throw new SQLFeatureNotSupportedException("Conversion not supported.  No conversions are supported.  This method will always throw.");
	}

	@Override
	public Object getObject(String columnName, Map<String, Class<?>> clazz)
			throws SQLException {
		return getObject(findColumn(columnName), clazz);
	}
	@SuppressWarnings("PMD.MissingOverride")
	public <T> T getObject(int columnIndex, Class<T> clazz) throws SQLException {
		// This method is entitled to throw if the conversion is not supported, so,
		// since we don't support any conversions we'll throw.
		// The only problem with this is that we're required to support certain conversion as specified in the docs.
		throw new SQLFeatureNotSupportedException("Conversion not supported.  No conversions are supported.  This method will always throw.");
	}
	@SuppressWarnings("PMD.MissingOverride")
	public <T> T getObject(String columnName, Class<T> clazz) throws SQLException {
		return getObject(findColumn(columnName), clazz);
	}

	@Override
	public Ref getRef(int colID) throws SQLException {
		throw new SQLFeatureNotSupportedException("getRef not supported");
	}

	@Override
	public Ref getRef(String columnName) throws SQLException {
		return getRef(findColumn(columnName));
	}

	@Override
	public int getRow() throws SQLException {
		try {
			// convert to jdbc standard (counting from one)
			return c.getPosition() + 1;
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public short getShort(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getShort(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getShort(index);
	}

	@Override
	public Statement getStatement() {
		// TODO: Implement as Xerial driver (which takes Statement as constructor argument)
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public String getString(int index) throws SQLException {
		return Utils.getUntypedString(getStringImpl(index));
	}

	public String getStringImpl(int index) throws SQLException {
		try {
			lastColumnRead = index;
			return c.getString(ci(index));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public String getString(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getString(index);
	}

	@Override
	public Time getTime(int colID) throws SQLException {
		return new Time(getTimestamp(colID).getTime());
	}

	@Override
	public Time getTime(String columnName) throws SQLException {
		return getTime(findColumn(columnName));
	}

	@Override
	public Time getTime(int colID, Calendar cal) throws SQLException {
		Calendar c=((Calendar)cal.clone());
		c.setTimeInMillis(getTimestamp(colID).getTime());
		return new Time(c.getTimeInMillis());
	}

	@Override
	public Time getTime(String columnName, Calendar cal) throws SQLException {
		return getTime(findColumn(columnName), cal);
	}

	@Override
	public Timestamp getTimestamp(int index) throws SQLException {
		try {
			lastColumnRead = index;
			switch (getMetaData().getColumnType(index)) {
				case Types.NULL:
					return null;
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.TIMESTAMP:
				case Types.DATE:
					return new Timestamp(getLong(index));
				default:
					// format 2011-07-11 11:36:30.009 OR 2011-07-11 11:36:30
					SimpleDateFormat timeStampFormat = new SimpleDateFormat(TIMESTAMP_PATTERN, FlexiLogXML.getLocale());
					SimpleDateFormat timeStampFormatNoMillis = new SimpleDateFormat(TIMESTAMP_PATTERN_NO_MILLIS, FlexiLogXML.getLocale());
					try {
						return new Timestamp(timeStampFormat.parse(getStringImpl(index)).getTime());
					} catch (ParseException e) {
						try {
							return new Timestamp(timeStampFormatNoMillis.parse(getStringImpl(index)).getTime());
						} catch (ParseException e1) {
							return new Timestamp(getDate(index).getTime());
						}
					}
			}
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public Timestamp getTimestamp(String columnName) throws SQLException {
		int index = findColumn(columnName);
		return getTimestamp(index);
	}

	@Override
	public Timestamp getTimestamp(int colID, Calendar cal)
			throws SQLException {
		Calendar c=((Calendar)cal.clone());
		c.setTimeInMillis(getTimestamp(colID).getTime());
		return new Timestamp(c.getTimeInMillis());
	}

	@Override
	public Timestamp getTimestamp(String columnName, Calendar cal)
			throws SQLException {
		return getTimestamp(findColumn(columnName), cal);
	}

	@Override
	public int getType() {
		return ResultSet.TYPE_SCROLL_SENSITIVE;
	}

	@Override
	public URL getURL(int colID) throws SQLException {
		throw new SQLFeatureNotSupportedException("ResultSet.getURL not supported");
	}

	@Override
	public URL getURL(String columnName) throws SQLException {
		return getURL(findColumn(columnName));
	}

	/**
	 * @deprecated since JDBC 2.0, use getCharacterStream
	 */
	@Override
	@Deprecated
	public InputStream getUnicodeStream(int colID) throws SQLException {
		throw new SQLFeatureNotSupportedException("ResultSet.getUnicodeStream deprecated, use getCharacterStream instead");
	}

	/**
	 * @deprecated since JDBC 2.0, use getCharacterStream
	 */
	@Override
	@Deprecated
	public InputStream getUnicodeStream(String columnName) throws SQLException {
		return getUnicodeStream(findColumn(columnName));
	}

	@Override
	public SQLWarning getWarnings() {
		// TODO: It may be that this is better implemented as "return null"
		throw new UnsupportedOperationException("ResultSet.getWarnings not implemented yet");
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("ResultSet.insertRow not implemented yet");
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		if (isClosed()) {
			return false;
		}
		try {
			return c.isAfterLast();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		if (isClosed()) {
			return false;
		}
		try {
			return c.isBeforeFirst();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean isFirst() throws SQLException {
		if (isClosed()) {
			return false;
		}
		try {
			return c.isFirst();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean isLast() throws SQLException {
		if (isClosed()) {
			return false;
		}
		try {
			return c.isLast();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean last() throws SQLException {
		try {
			return c.moveToLast();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("moveToCurrentRow not supported");
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("moveToCurrentRow not supported");
	}

	@Override
	public boolean next() throws SQLException {
		try {
			return c.moveToNext();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean previous() throws SQLException {
		try {
			return c.moveToPrevious();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@SuppressWarnings("deprecation")
	@Override
	public void refreshRow() throws SQLException {
		try {
			c.requery();
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throw new SQLFeatureNotSupportedException("relative not supported");
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		throw new SQLFeatureNotSupportedException("rowDeleted not supported");
	}

	@Override
	public boolean rowInserted() throws SQLException {
		throw new SQLFeatureNotSupportedException("rowInserted not supported");
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		throw new SQLFeatureNotSupportedException("rowUpdated not supported");
	}

	@Override
	public void updateArray(int colID, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateArray not supported");
	}

	@Override
	public void updateArray(String columnName, Array x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateArray not supported");
	}

	@Override
	public void updateAsciiStream(int colID, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_ASCII_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_ASCII_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBigDecimal(int colID, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BIG_DECIMAL_NOT_SUPPORTED);
	}

	@Override
	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BIG_DECIMAL_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int colID, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BINARY_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BINARY_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int colID, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String columnName, Blob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateBoolean(int colID, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateBoolean not supported");
	}

	@Override
	public void updateBoolean(String columnName, boolean x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateBoolean not supported");
	}

	@Override
	public void updateByte(int colID, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateByte not supported");
	}

	@Override
	public void updateByte(String columnName, byte x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateByte not supported");
	}

	@Override
	public void updateBytes(int colID, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateBytes not supported");
	}

	@Override
	public void updateBytes(String columnName, byte[] x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateBytes not supported");
	}

	@Override
	public void updateCharacterStream(int colID, Reader x, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int colID, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String columnName, Clob x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateDate(int colID, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateDate not supported");
	}

	@Override
	public void updateDate(String columnName, Date x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateDate not supported");
	}

	@Override
	public void updateDouble(int colID, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateDouble not supported");
	}

	@Override
	public void updateDouble(String columnName, double x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateDouble not supported");
	}

	@Override
	public void updateFloat(int colID, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateFloat not supported");
	}

	@Override
	public void updateFloat(String columnName, float x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateFloat not supported");
	}

	@Override
	public void updateInt(int colID, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateInt not supported");
	}

	@Override
	public void updateInt(String columnName, int x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateInt not supported");
	}

	@Override
	public void updateLong(int colID, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateLong not supported");
	}

	@Override
	public void updateLong(String columnName, long x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateLong not supported");
	}

	@Override
	public void updateNull(int colID) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateNull not supported");
	}

	@Override
	public void updateNull(String columnName) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateNull not supported");
	}

	@Override
	public void updateObject(int colID, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_OBJECT_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(String columnName, Object x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_OBJECT_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(int colID, Object x, int scale) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_OBJECT_NOT_SUPPORTED);
	}

	@Override
	public void updateObject(String columnName, Object x, int scale) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_OBJECT_NOT_SUPPORTED);
	}

	@Override
	public void updateRef(int colID, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateRef not supported");
	}

	@Override
	public void updateRef(String columnName, Ref x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateRef not supported");
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLFeatureNotSupportedException("updateRow not supported");
	}

	@Override
	public void updateShort(int colID, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateShort not supported");
	}

	@Override
	public void updateShort(String columnName, short x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateShort not supported");
	}

	@Override
	public void updateString(int colID, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateString not supported");
	}

	@Override
	public void updateString(String columnName, String x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateString not supported");
	}

	@Override
	public void updateTime(int colID, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateTime not supported");
	}

	@Override
	public void updateTime(String columnName, Time x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateTime not supported");
	}

	@Override
	public void updateTimestamp(int colID, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateTimestamp not supported");
	}

	@Override
	public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateTimestamp not supported");
	}

	@Override
	public boolean wasNull() throws SQLException {
		try {
			return c.isNull(ci(lastColumnRead));
		} catch (android.database.SQLException e) {
			throw ASQLConnectorConnection.chainException(e);
		}
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

	@Override
	public int getHoldability() {
		return CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		return getCharacterStream(columnIndex);
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		return getNCharacterStream(findColumn(columnLabel));
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		return getClob(columnIndex);
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return getNClob(findColumn(columnLabel));
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return getString(columnIndex);
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return getNString(findColumn(columnLabel));
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getRowId not supported");
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return getRowId(findColumn(columnLabel));
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLFeatureNotSupportedException("getSQLXML not supported");
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return getSQLXML(findColumn(columnLabel));
	}

	@Override
	public boolean isClosed() {
		return c.isClosed();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_ASCII_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_ASCII_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_ASCII_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_ASCII_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BINARY_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BINARY_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BINARY_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BINARY_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_BLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CHARACTER_STREAM_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLFeatureNotSupportedException(UPDATE_N_CLOB_NOT_SUPPORTED);
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateNString not supported");
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateNString not supported");
	}

	@Override
	public void updateRowId(int columnIndex, RowId value) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateRowId not supported");
	}

	@Override
	public void updateRowId(String columnLabel, RowId value) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateRowId not supported");
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateSQLXML not supported");
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLFeatureNotSupportedException("updateSQLXML not supported");
	}

	Cursor getCursor() {
		return c;
	}
}
