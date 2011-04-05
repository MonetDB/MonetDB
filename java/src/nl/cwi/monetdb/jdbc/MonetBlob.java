/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.jdbc;

import java.sql.*;
import java.io.*;

/**
 * The MonetBlob class implements the java.sql.Blob interface.  Because
 * MonetDB/SQL currently has no support for streams, this class is a
 * shallow wrapper of a StringBuffer.  It is more or less supplied to
 * enable an application that depends on it to run.  It may be obvious
 * that it is a real resource expensive workaround that contradicts the
 * benefits for a Blob: avoidance of huge resource consumption.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetBlob implements Blob {
	private byte[] buf;

	protected MonetBlob(String in) {
		int len = in.length() / 2;
		buf = new byte[len];
		for (int i = 0; i < len; i++)
			buf[i] = (byte)Integer.parseInt(in.substring(2 * i, (2 * i) + 2), 16);
	}

	//== begin interface Blob
	
	public InputStream getBinaryStream() throws SQLException {
		return(new ByteArrayInputStream(buf));
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
	public byte[] getBytes(long pos, int length) throws SQLException {
		try {
			byte[] r = new byte[length];
			for (int i = 0; i < length; i++)
				r[i] = buf[(int)pos - 1 + i];
			return(r);
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage());
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
	public long length() throws SQLException {
		return((long)buf.length);
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
	public long position(Blob pattern, long start) throws SQLException {
		return(position(pattern.getBytes(1L, (int)pattern.length()), start));
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
	public long position(byte[] pattern, long start) throws SQLException {
		try {
			for (int i = (int)(start - 1); i < buf.length - pattern.length; i++) {
				int j;
				for (j = 0; j < pattern.length; j++) {
					if (buf[i + j] != pattern[j])
						break;
				}
				if (j == pattern.length)
					return(i);
			}
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage());
		}
		return(-1);
	}

	public OutputStream setBinaryStream(long pos) throws SQLException {
		throw new SQLException("Operation setBinaryStream(long pos) currently not supported");
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
	public int setBytes(long pos, byte[] bytes) throws SQLException {
		return(setBytes(pos, bytes, 1, bytes.length));
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
	public int setBytes(long pos, byte[] bytes, int offset, int len)
		throws SQLException
	{
		try {
			/* transactions? what are you talking about? */
			for (int i = (int)pos; i < len; i++)
				buf[i] = bytes[offset - 1 + i];
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage());
		}
		return(len);
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
	public void truncate(long len) {
		if (buf.length > len) {
			byte[] newbuf = new byte[(int)len];
			for (int i = 0; i < len; i++)
				newbuf[i] = buf[i];
			buf = newbuf;
		}
	}
}
