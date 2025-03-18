package fr.distrimind.oss.asqlconnector;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.SQLException;

class Utils {

	static byte[] getTypedBytesArray(Blob b) throws SQLException {
		if (b instanceof ASQLConnectorBlob)
			return getTypedBytesArray(((ASQLConnectorBlob) b).b);
		else
			return getTypedBytesArray(b.getBytes(1, (int) b.length()));
	}

	static byte[] getTypedBytesArray(byte[] t) {
		if (t == null)
			return null;
		else {
			byte[] r = new byte[t.length + 1];
			r[0] = ASLConnectorBytesArrayType.BYTES_ARRAY_TYPE;
			System.arraycopy(t, 0, r, 1, t.length);
			return r;
		}
	}

	static byte[] getUntypedBytesArray(byte[] t) {
		if (t == null)
			return null;
		else {
			byte[] r = new byte[t.length - 1];
			System.arraycopy(t, 1, r, 0, r.length);
			return r;
		}
	}


	@SuppressWarnings("PMD.ReturnEmptyCollectionRatherThanNull")
	static byte[] bigDecimalToBytes(BigDecimal bigDecimal) {
		if (bigDecimal == null)
			return null;
		else {
			byte[] tab = bigDecimal.unscaledValue().toByteArray();
			byte[] res = new byte[tab.length + 5];
			res[0] = ASLConnectorBytesArrayType.BIG_DECIMAL_TYPE;
			System.arraycopy(tab, 0, res, 5, tab.length);
			putInt(res, 1, bigDecimal.scale());
			return res;
		}
	}

	static BigDecimal bigDecimalFromBytes(byte[] tab) {
		if (tab == null)
			return null;
		else {
			if (tab[0] != ASLConnectorBytesArrayType.BIG_DECIMAL_TYPE)
				throw new IllegalArgumentException();
			byte[] t = new byte[tab.length - 5];
			System.arraycopy(tab, 5, t, 0, t.length);
			return new BigDecimal(new BigInteger(t), getInt(tab, 1));
		}
	}

	static int getInt(byte[] b, int off) {
		return ((b[off + 3] & 0xFF)) + ((b[off + 2] & 0xFF) << 8) + ((b[off + 1] & 0xFF) << 16) + ((b[off]) << 24);
	}

	static void putInt(byte[] b, int off, int val) {
		b[off] = (byte) (val >>> 24);
		b[off + 1] = (byte) (val >>> 16);
		b[off + 2] = (byte) (val >>> 8);
		b[off + 3] = (byte) (val);
	}
}
