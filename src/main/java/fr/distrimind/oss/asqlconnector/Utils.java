package fr.distrimind.oss.asqlconnector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;
import java.util.Base64;

/**
 * @author Jason Mahdjoub
 * @since DM-A-SQLConnector 1.0
 * @version 1.O
 */
public class Utils {


	public static String getTypedBytesArray(byte[] t) {
		if (t == null)
			return null;
		else {
			String e=Base64.getEncoder().encodeToString(t);
			return ASLConnectorStringType.BYTE_ARRAY_TYPE+e;
		}
	}

	@SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
	public static byte[] getUntypedBytesArray(String s) throws SQLException {
		if (s == null)
			return null;
		else {
			if (s.isEmpty())
				throw new SQLException();
			if (s.charAt(0)!=ASLConnectorStringType.BYTE_ARRAY_TYPE)
				throw new SQLException();
			return Base64.getDecoder().decode(s.substring(1));
		}
	}
	public static String getTypedString(String s) {
		if (s == null)
			return null;
		else {
			return ASLConnectorStringType.STRING_TYPE+s;
		}
	}

	public static String getUntypedString(String s) {
		if (s == null)
			return null;
		else {
			if (s.isEmpty())
				return s;
			if (s.charAt(0)!=ASLConnectorStringType.STRING_TYPE)
				return s;
			return s.substring(1);
		}
	}


	@SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
	public static String bigDecimalToString(BigDecimal bigDecimal) {
		if (bigDecimal == null)
			return null;
		else {
			byte[] tab = bigDecimal.unscaledValue().toByteArray();
			byte[] res = new byte[tab.length + 4];
			System.arraycopy(tab, 0, res, 4, tab.length);
			putInt(res, 0, bigDecimal.scale());
			return ASLConnectorStringType.BIG_DECIMAL_TYPE+Base64.getEncoder().encodeToString(res);
		}
	}

	public static BigDecimal bigDecimalFromString(String s) throws SQLException {
		if (s == null || s.length()<2)
			return null;
		else {
			if (s.charAt(0) != ASLConnectorStringType.BIG_DECIMAL_TYPE)
				throw new SQLException();
			byte[] tab=Base64.getDecoder().decode(s.substring(1));
			byte[] t = new byte[tab.length - 4];
			System.arraycopy(tab, 4, t, 0, t.length);
			return new BigDecimal(new BigInteger(t), getInt(tab, 0));
		}
	}

	private static int getInt(byte[] b, int off) {
		return ((b[off + 3] & 0xFF)) + ((b[off + 2] & 0xFF) << 8) + ((b[off + 1] & 0xFF) << 16) + ((b[off]) << 24);
	}

	private static void putInt(byte[] b, int off, int val) {
		b[off] = (byte) (val >>> 24);
		b[off + 1] = (byte) (val >>> 16);
		b[off + 2] = (byte) (val >>> 8);
		b[off + 3] = (byte) (val);
	}
}
