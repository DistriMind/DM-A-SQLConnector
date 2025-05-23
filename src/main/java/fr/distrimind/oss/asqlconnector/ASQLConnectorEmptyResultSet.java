package fr.distrimind.oss.asqlconnector;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * @author Jason Mahdjoub
 * @since DM-A-SQLConnector 1.0
 * @version 1.O
 */

public class ASQLConnectorEmptyResultSet implements ResultSet {
	static final ASQLConnectorEmptyResultSet SINGLETON =new ASQLConnectorEmptyResultSet();
	private ASQLConnectorEmptyResultSet() {
	}

	@Override
	public boolean next() {
		return false;
	}

	@Override
	public void close() {

	}

	@Override
	public boolean wasNull() throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		throw new SQLException();
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getString(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public float getFloat(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	@Deprecated
	public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
		throw new SQLException();
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Date getDate(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Time getTime(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public InputStream getAsciiStream(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	@Deprecated
	public InputStream getUnicodeStream(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public InputStream getBinaryStream(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		throw new SQLException();
	}

	@Override
	public void clearWarnings() {

	}

	@Override
	public String getCursorName() throws SQLException {
		throw new SQLException();
	}

	@Override
	public ResultSetMetaData getMetaData() {
		return ASQLConnectorEmptyResultSetMetaData.SINGLETON;
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Object getObject(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int findColumn(String columnLabel) {
		return -1;
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Reader getCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isBeforeFirst(){
		return false;
	}

	@Override
	public boolean isAfterLast() {
		return true;
	}

	@Override
	public boolean isFirst(){
		return false;
	}

	@Override
	public boolean isLast() {
		return false;
	}

	@Override
	public void beforeFirst() {

	}

	@Override
	public void afterLast() {

	}

	@Override
	public boolean first() {
		return false;
	}

	@Override
	public boolean last(){
		return false;
	}

	@Override
	public int getRow(){
		return -1;
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean previous() {
		return false;
	}

	@Override
	public void setFetchDirection(int direction) {

	}

	@Override
	public int getFetchDirection() {
		return FETCH_FORWARD;
	}

	@Override
	public void setFetchSize(int rows) {

	}

	@Override
	public int getFetchSize() {
		return 0;
	}

	@Override
	public int getType() {
		return TYPE_FORWARD_ONLY;
	}

	@Override
	public int getConcurrency() {
		return ResultSet.CONCUR_READ_ONLY;
	}

	@Override
	public boolean rowUpdated() {
		return false;
	}

	@Override
	public boolean rowInserted() {
		return false;
	}

	@Override
	public boolean rowDeleted() {
		return false;
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNull(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBoolean(String columnLabel, boolean x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateByte(String columnLabel, byte x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateShort(String columnLabel, short x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateInt(String columnLabel, int x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateLong(String columnLabel, long x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateFloat(String columnLabel, float x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateDouble(String columnLabel, double x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateString(String columnLabel, String x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBytes(String columnLabel, byte[] x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateDate(String columnLabel, Date x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateTime(String columnLabel, Time x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateObject(String columnLabel, Object x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void insertRow() throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateRow() throws SQLException {
		throw new SQLException();
	}

	@Override
	public void deleteRow() throws SQLException {
		throw new SQLException();
	}

	@Override
	public void refreshRow() throws SQLException {
		throw new SQLException();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		throw new SQLException();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		throw new SQLException();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException();
	}

	@Override
	public Statement getStatement() throws SQLException {
		throw new SQLException();
	}

	@Override
	public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Ref getRef(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Blob getBlob(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Clob getClob(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Array getArray(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Ref getRef(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Blob getBlob(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Clob getClob(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Array getArray(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Date getDate(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Time getTime(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
		throw new SQLException();
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public URL getURL(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateRef(String columnLabel, Ref x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBlob(String columnLabel, Blob x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateClob(String columnLabel, Clob x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateArray(String columnLabel, Array x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int getHoldability() {
		return CLOSE_CURSORS_AT_COMMIT;
	}

	@Override
	public boolean isClosed() {
		return true;
	}

	@Override
	public void updateNString(int columnIndex, String nString) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNString(String columnLabel, String nString) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
		throw new SQLException();
	}

	@Override
	public NClob getNClob(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new SQLException();
	}

	@Override
	public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new SQLException();
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		throw new SQLException();
	}
	@SuppressWarnings("PMD")
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		throw new SQLException();
	}
	@SuppressWarnings("PMD")
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		throw new SQLException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) {
		return false;
	}
}
