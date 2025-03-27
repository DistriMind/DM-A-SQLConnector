package fr.distrimind.oss.asqlconnector;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;

public class ASQLConnectorBlob implements Blob {

	byte[] b;
	ASQLConnectorBlob() {

	}
	public ASQLConnectorBlob(byte[] blob) {
		this(blob, ASQLConnectorBlobType.BYTE_ARRAY_TYPE);
	}
	public ASQLConnectorBlob(InputStream inputStream, long length) throws SQLException {
		if (inputStream==null)
			throw new NullPointerException();
		if (length>Integer.MAX_VALUE-10)
			throw new SQLException();
		try {
			int l = (int) length;
			this.b = new byte[1 + l];
			this.b[0] = ASQLConnectorBlobType.BYTE_ARRAY_TYPE;
			int pos = 1;
			while (l > 0) {
				int s = inputStream.read(b, pos, l);
				if (s <0) {
					throw new SQLException();
				}
				l -= s;
				pos += s;
			}
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}

	public ASQLConnectorBlob(Reader reader, long length) throws SQLException {
		if (reader==null)
			throw new NullPointerException();
		if (length>Integer.MAX_VALUE-10)
			throw new SQLException();
		try {
			int l = (int) length;
			char[] chars=new char[(int)length];
			int pos = 1;
			while (l > 0) {
				int s = reader.read(chars, pos, l);
				if (s < 0) {
					throw new SQLException();
				}
				l -= s;
				pos += s;
			}
			byte[] bb=StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars)).array();
			this.b= new byte[bb.length+1];
			this.b[0]=ASQLConnectorBlobType.STRING_TYPE;
			System.arraycopy(bb, 0, this.b, 1, bb.length);
		}
		catch (IOException e)
		{
			throw new SQLException(e);
		}
	}
	private ASQLConnectorBlob(byte[] blob, byte type) {
		if (blob==null)
			throw new NullPointerException();
		else
		{
			this.b=new byte[blob.length+1];
			System.arraycopy(blob, 0, b, 1, blob.length);
		}
		this.b[0]=type;
	}
	public ASQLConnectorBlob(Clob clob) throws SQLException {
		this(clob==null?null:(clob instanceof ASQLConnectorClob?(((ASQLConnectorClob)clob).getString()==null?null:((ASQLConnectorClob)clob).getString().getBytes(StandardCharsets.UTF_8)):clob.getSubString(1, (int)clob.length()).getBytes(StandardCharsets.UTF_8)), ASQLConnectorBlobType.STRING_TYPE);
	}



	@Override
	public InputStream getBinaryStream() throws SQLException {
		return getBinaryStream(1L, b.length);
	}

	@Override
	public InputStream getBinaryStream(long pos, long length) throws SQLException {
		checkLimits(pos, length);
		return new ByteArrayInputStream(b, (int) pos, (int) length);
	}
	private void checkLimits(long pos, long length) throws SQLException {
		if (pos <= 0) {
			throw new SQLException("pos must be > 0");
		}
		if (length < 0) {
			throw new SQLException("length must be > 0");
		}
		int l = (int) (b.length - pos);
		if (length > l) {
			throw new SQLException("length "+length+" must be <= " + l);
		}
	}

	@Override
	public byte[] getBytes(long pos, int length) throws SQLException {
		checkLimits(pos, length);
		if (b==null)
			return new byte[0];
		byte[] r = new byte[length];
		System.arraycopy(b, (int) pos , r, 0, length);
		return r;
	}


	public byte[] getBytes() throws SQLException {
		return getBytes(1, (int)length());
	}

	@Override
	public long length() {
		if (b==null)
			return 0;
		return b.length-1;
	}

	@Override
	public long position(Blob pattern, long start) throws SQLException {
		long p=pattern.length();
		if (p>Integer.MAX_VALUE-10)
			throw new SQLException();
		return position(pattern.getBytes(1, (int) p), start);
	}

	@Override
	public long position(byte[] pattern, long start) throws SQLException {
		if (pattern==null)
			throw new NullPointerException();
		if (b==null)
			return -1;
		if (start<1)
			throw new SQLException();
		if (start>b.length+1)
			return -1;
		int s=b.length-pattern.length;
		for (int i=(int)start;i<s;i++)
		{
			boolean allOk=true;
			for (int j=0;j<pattern.length;j++)
			{
				if (pattern[j]!=b[i+j]) {
					allOk = false;
					break;
				}
			}
			if (allOk)
				return i;
		}
		return -1;
	}

	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException {
		if (b==null)
			throw new SQLException();
		return new BufferedOutputStream(new OutputStream() {
			private int pos=b.length;
			private final byte[] oneByte=new byte[1];
			@Override
			public void write(int i) throws IOException {
				try {
					oneByte[0]=(byte)i;
					setBytes(pos, oneByte, 0, 1);
					++pos;
				} catch (SQLException e) {
					throw new IOException(e);
				}
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
                try {
                    setBytes(pos, b, off, len);
					pos+=len;
                } catch (SQLException e) {
                    throw new IOException(e);
                }
            }
		});
	}

	@Override
	public int setBytes(long pos, byte[] theBytes) throws SQLException {
		return setBytes(pos, theBytes, 0, theBytes.length);
	}

	@Override
	@SuppressWarnings("PMD.CompareObjectsWithEquals")
	public int setBytes(long pos, byte[] theBytes, int offset, int len) throws SQLException {
		if (b==null)
			throw new SQLException();
		if (pos<0)
			throw new NullPointerException();
		if (theBytes==null)
			throw new NullPointerException();
		if (offset<0)
			throw new SQLException();
		if (offset>=theBytes.length)
			throw new SQLException();
		if (len<0)
			throw new SQLException();
		if (len==0)
			return 0;
		if (len+offset>theBytes.length)
			throw new SQLException();
		if (pos+len>Integer.MAX_VALUE-10)
			throw new SQLException();
		int sTabMin=(int)(pos+len);
		byte[] r;
		if (sTabMin>b.length)
		{
			r=new byte[sTabMin];
			System.arraycopy(b, 0, r,0, (int)pos );
		}
		else
			r=b;
		System.arraycopy(theBytes, offset, r, (int)pos, len);
		if (r!=b)
			free();
		b=r;
		return len;
	}

	@Override
	public void truncate(long len) throws SQLException {
		if (b==null)
			throw new SQLException();
		if (b.length-1<len)
			throw new SQLException();
		if (b.length-1==len)
			return;
		byte[] r=new byte[(int)len+1];
		System.arraycopy(b, 0, r, 0, r.length);
		free();
		b=r;
	}

	@Override
	public void free() {
		if (b!=null) {
			Arrays.fill(b, (byte) 0);
			b = null;
		}
	}

	/**
	 * Print the length of the blob along with the first 10 characters.
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if (b == null) {
			sb.append("Null Blob");
		}
		else {
			sb.append("Blob length ");
			long length = length();
			sb.append(length);

			sb.append(" ");
			if (length > 10) {
				length = 10;
			}
			for (int counter = 0; counter < length; counter++) {
				sb.append("0x");
				sb.append(Integer.toHexString(b[counter]));
				sb.append(" ");
			}
			sb.append("(");
			for (int counter = 0; counter < length; counter++) {
				sb.append((char) b[counter]);
				if (counter < length - 1) {
					sb.append(" ");
				}
			}
			sb.append(")");
		}
		return sb.toString();
	}

	public String toBase64() throws SQLException {
		if (b==null)
			throw new SQLException();
		return Base64.getEncoder().encodeToString(b);
	}

}
