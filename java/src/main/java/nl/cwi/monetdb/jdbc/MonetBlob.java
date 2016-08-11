/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.util.Arrays;
import java.io.*;

/**
 * The MonetBlob class implements the {@link java.sql.Blob} interface.  Because
 * MonetDB/SQL currently has no support for streams, this class is a
 * shallow wrapper of a byte[].  It is more or less supplied to
 * enable an application that depends on it to run.  It may be obvious
 * that it is a real resource expensive workaround that contradicts the
 * benefits for a Blob: avoidance of huge resource consumption.
 *
 * @author Fabian Groffen
 */
public class MonetBlob implements Blob {
	private byte[] buf;

	protected MonetBlob(byte[] data) {
		buf = data;
	}
	
	static MonetBlob create(String in) {
		int len = in.length() / 2;
		byte[] buf = new byte[len];
		for (int i = 0; i < len; i++)
			buf[i] = (byte)Integer.parseInt(in.substring(2 * i, (2 * i) + 2), 16);
		return new MonetBlob(buf);
	}
	

	//== begin interface Blob
	
	/**
	 * This method frees the Blob object and releases the resources that
	 * it holds. The object is invalid once the free method is called.
	 *
	 * After free has been called, any attempt to invoke a method other
	 * than free will result in a SQLException being thrown. If free is
	 * called multiple times, the subsequent calls to free are treated
	 * as a no-op.
	 *
	 * @throws SQLException if an error occurs releasing the Blob's
	 *         resources
	 * @throws SQLFeatureNotSupportedException - if the JDBC driver does
	 *         not support this method
	 */
	@Override
	public void free() throws SQLException {
		buf = null;
	}
	
	/**
	 * Retrieves the BLOB value designated by this Blob instance as a
	 * stream.
	 *
	 * @return a stream containing the BLOB data
	 * @throws SQLException if there is an error accessing the BLOB value
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does
	 *         not support this method
	 */
	@Override
	public InputStream getBinaryStream() throws SQLException {
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		return new ByteArrayInputStream(buf);
	}

	/**
	 * Returns an InputStream object that contains a partial Blob value,
	 * starting with the byte specified by pos, which is length bytes in
	 * length.
	 *
	 * @param pos the offset to the first byte of the partial value to
	 *        be retrieved. The first byte in the Blob is at position 1
	 * @param length the length in bytes of the partial value to be
	 *        retrieved
	 * @return InputStream through which the partial Blob value can be
	 *         read.
	 * @throws SQLException if pos is less than 1 or if pos is
	 *         greater than the number of bytes in the Blob or if pos +
	 *         length is greater than the number of bytes in the Blob
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does
	 *         not support this method
	 */
	@Override
	public InputStream getBinaryStream(long pos, long length)
		throws SQLException
	{
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		if (pos < 1)
			throw new SQLException("pos is less than 1", "M1M05");
		if (pos - 1 > buf.length)
			throw new SQLException("pos is greater than the number of bytes in the Blob", "M1M05");
		if (pos - 1 + length > buf.length)
			throw new SQLException("pos + length is greater than the number of bytes in the Blob", "M1M05");
		return new ByteArrayInputStream(buf, (int)(pos - 1), (int)length);
	}

	/**
	 * Retrieves all or part of the BLOB value that this Blob object
	 * represents, as an array of bytes.  This byte array contains up to
	 * length consecutive bytes starting at position pos.
	 *
	 * @param pos the ordinal position of the first byte in the BLOB
	 *        value to be extracted; the first byte is at position 1.
	 * @param length the number of consecutive bytes to be copied
	 * @return a byte array containing up to length consecutive bytes
	 *         from the BLOB value designated by this Blob object,
	 *         starting with the byte at position pos.
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value
	 */
	@Override
	public byte[] getBytes(long pos, int length) throws SQLException {
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		try {
			return Arrays.copyOfRange(buf, (int) pos - 1, (int) pos - 1 + length);
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage(), "M0M10");
		}
	}

	/**
	 * Returns the number of bytes in the BLOB value designated by this
	 * Blob object.
	 *
	 * @return length of the BLOB in bytes
	 * @throws SQLException if there is an error accessing the length
	 *         of the BLOB value
	 */
	@Override
	public long length() throws SQLException {
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		return (long)buf.length;
	}

	/**
	 * Retrieves the byte position in the BLOB value designated by this
	 * Blob object at which pattern begins.  The search begins at
	 * position start.
	 *
	 * @param pattern the Blob object designating the BLOB value for
	 *        which to search
	 * @param start the position in the BLOB value at which to begin
	 *        searching; the first position is 1
	 * @return the position at which the pattern begins, else -1
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value
	 */
	@Override
	public long position(Blob pattern, long start) throws SQLException {
		return position(pattern.getBytes(1L, (int)pattern.length()), start);
	}

	/**
	 * Retrieves the byte position at which the specified byte array
	 * pattern begins within the BLOB value that this Blob object
	 * represents.  The search for pattern begins at position start.
	 *
	 * @param pattern the byte array for which to search
	 * @param start the position at which to begin searching;
	 *        the first position is 1
	 * @return the position at which the pattern appears, else -1
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value
	 */
	@Override
	public long position(byte[] pattern, long start) throws SQLException {
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		try {
			for (int i = (int)(start - 1); i < buf.length - pattern.length; i++) {
				int j;
				for (j = 0; j < pattern.length; j++) {
					if (buf[i + j] != pattern[j])
						break;
				}
				if (j == pattern.length)
					return i;
			}
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage(), "M0M10");
		}
		return -1;
	}

	/**
	 * Retrieves a stream that can be used to write to the BLOB value
	 * that this Blob object represents. The stream begins at position
	 * pos. The bytes written to the stream will overwrite the existing
	 * bytes in the Blob object starting at the position pos. If the end
	 * of the Blob value is reached while writing to the stream, then
	 * the length of the Blob value will be increased to accomodate the
	 * extra bytes.
	 *
	 * @param pos the position in the BLOB value at which to start
	 *            writing; the first position is 1
	 * @return a java.io.OutputStream object to which data can be
	 *         written
	 * @throws SQLException if there is an error accessing the BLOB
	 *         value or if pos is less than 1
	 * @throws SQLFeatureNotSupportedException if the JDBC driver does
	 *         not support this method
	 */
	@Override
	public OutputStream setBinaryStream(long pos) throws SQLException {
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		if (pos < 1)
			throw new SQLException("pos is less than 1", "M1M05");
		throw new SQLFeatureNotSupportedException("Operation setBinaryStream(long pos) currently not supported", "0A000");
	}

	/**
	 * Writes the given array of bytes to the BLOB value that this Blob
	 * object represents, starting at position pos, and returns the
	 * number of bytes written.
	 *
	 * @param pos the position in the BLOB object at which to start
	 *        writing
	 * @param bytes the array of bytes to be written to the BLOB  value
	 *        that this Blob object represents
	 * @return the number of bytes written
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value
	 */
	@Override
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		return setBytes(pos, bytes, 1, bytes.length);
	}

	/**
	 * Writes all or part of the given byte array to the BLOB value that
	 * this Blob object represents and returns the number of bytes
	 * written.  Writing starts at position pos in the BLOB  value; len
	 * bytes from the given byte array are written.
	 *
	 * @param pos the position in the BLOB object at which to start
	 *        writing
	 * @param bytes the array of bytes to be written to this BLOB
	 *        object
	 * @param offset the offset into the array bytes at which to start
	 *        reading the bytes to be set
	 * @param len the number of bytes to be written to the BLOB  value
	 *        from the array of bytes bytes
	 * @return the number of bytes written
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value
	 */
	@Override
	public int setBytes(long pos, byte[] bytes, int offset, int len)
		throws SQLException
	{
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		try {
			/* transactions? what are you talking about? */
			for (int i = (int)pos; i < len; i++)
				buf[i] = bytes[offset - 1 + i];
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage(), "M0M10");
		}
		return len;
	}

	/**
	 * Truncates the BLOB value that this Blob  object represents to be
	 * len bytes in length.
	 *
	 * @param len the length, in bytes, to which the BLOB value
	 *        should be truncated
	 * @throws SQLException if there is an error accessing the
	 *         BLOB value
	 */
	@Override
	public void truncate(long len) throws SQLException {
		if (buf == null)
			throw new SQLException("This Blob object has been freed", "M1M20");
		if (buf.length > len) {
			byte[] newbuf = new byte[(int)len];
			for (int i = 0; i < len; i++)
				newbuf[i] = buf[i];
			buf = newbuf;
		}
	}
}
