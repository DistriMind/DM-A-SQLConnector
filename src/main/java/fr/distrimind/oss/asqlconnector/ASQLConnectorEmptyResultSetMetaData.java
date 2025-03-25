package fr.distrimind.oss.asqlconnector;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * @author Jason Mahdjoub
 * @since DM-A-SQLConnector 1.0
 * @version 1.O
 */
public class ASQLConnectorEmptyResultSetMetaData implements ResultSetMetaData {
	static ASQLConnectorEmptyResultSetMetaData SINGLETON=new ASQLConnectorEmptyResultSetMetaData();
	private ASQLConnectorEmptyResultSetMetaData() {
	}

	@Override
	public int getColumnCount() throws SQLException {
		return 0;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int isNullable(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int getScale(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		throw new SQLException();
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new SQLException();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return false;
	}
}
