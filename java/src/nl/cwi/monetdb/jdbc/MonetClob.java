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
 * The MonetClob class implements the java.sql.Clob interface.  Because
 * MonetDB/SQL currently has no support for streams, this class is a
 * shallow wrapper of a StringBuffer.  It is more or less supplied to
 * enable an application that depends on it to run.  It may be obvious
 * that it is a real resource expensive workaround that contradicts the
 * sole reason for a Clob: avoidance of huge resource consumption.
 * <b>Use of this class is highly discouraged.</b>
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetClob implements Clob {
	private StringBuffer buf;

	protected MonetClob(String in) {
		buf = new StringBuffer(in);
	}

	//== begin interface Clob
	
	public InputStream getAsciiStream() throws SQLException {
		throw new SQLException("Operation getAsciiStream() currently not supported");
	}

	public Reader getCharacterStream() throws SQLException {
		throw new SQLException("Operation getCharacterStream() currently not supported");
	}

	/**
	 * Retrieves a copy of the specified substring in the CLOB value
	 * designated by this Clob object.  The substring begins at
	 * position pos and has up to length consecutive characters.
	 *
	 * @param pos the first character of the substring to be
	 *        extracted. The first character is at position 1.
	 * @param length the number of consecutive characters to be copied
	 * @return a String that is the specified substring in the
	 *         CLOB value designated by this Clob object
	 * @throws SQLException if there is an error accessing the
	 *         CLOB value
	 */
	public String getSubString(long pos, int length) throws SQLException {
		try {
			return(buf.substring((int)(pos - 1), (int)(pos - 1 + length)));
		} catch (IndexOutOfBoundsException e) {
			throw new SQLException(e.getMessage());
		}
	}

	/**
	 * Retrieves the number of characters in the CLOB value designated
	 * by this Clob object.
	 *
	 * @return length of the CLOB in characters
	 * @throws SQLException if there is an error accessing the length
	 *         of the CLOB value
	 */
	public long length() throws SQLException {
		return((long)buf.length());
	}

	/**
	 * Retrieves the character position at which the specified Clob
	 * object searchstr appears in this Clob object.  The search
	 * begins at position start.
	 *
	 * @param searchstr the Clob object for which to search
	 * @param start the position at which to begin searching;
	 *        the first position is 1
	 * @return the position at which the Clob object appears or
	 *         -1 if it is not present; the first position is 1
	 * @throws SQLException if there is an error accessing the
	 *         CLOB value
	 */
	public long position(Clob searchstr, long start) throws SQLException {
		return(position(searchstr.getSubString(1L, (int)(searchstr.length())), start));
	}

	/**
	 * Retrieves the character position at which the specified
	 * substring searchstr appears in the SQL CLOB value represented
	 * by this Clob object.  The search begins at position start.
	 *
	 * @param searchstr the substring for which to search
	 * @param start the position at which to begin searching;
	 *        the first position is 1
	 * @return the position at which the substring appears or
	 *         -1 if it is not present; the first position is 1
	 * @throws SQLException if there is an error accessing the
	 *         CLOB value
	 */
	public long position(String searchstr, long start) throws SQLException {
		return((long)(buf.indexOf(searchstr, (int)(start - 1))));
	}

	public OutputStream setAsciiStream(long pos) throws SQLException {
		throw new SQLException("Operation setAsciiStream(long pos) currently not supported");
	}

	public Writer setCharacterStream(long pos) throws SQLException {
		throw new SQLException("Operation setCharacterStream(long pos) currently not supported");
	}

	/**
	 * Writes the given Java String to the CLOB  value that this
	 * Clob object designates at the position pos.
	 *
	 * @param pos the position at which to start writing to the
	 *        CLOB value that this Clob object represents
	 * @param str the string to be written to the CLOB value that
	 *        this Clob designates
	 * @return the number of characters written
	 * @throws SQLException if there is an error accessing the
	 *         CLOB value
	 */
	public int setString(long pos, String str) throws SQLException {
		return(setString(pos, str, 1, str.length()));
	}

	/**
	 * Writes len characters of str, starting at character offset,
	 * to the CLOB value that this Clob represents.
	 *
	 * @param pos the position at which to start writing to this
	 *        CLOB object
	 * @param str the string to be written to the CLOB value that
	 *        this Clob object represents
	 * @param offset the offset into str to start reading the
	 *        characters to be written
	 * @param len the number of characters to be written
	 * @return the number of characters written
	 * @throws SQLException if there is an error accessing the
	 *         CLOB value
	 */
	public int setString(long pos, String str, int offset, int len)
		throws SQLException
	{
		int buflen = buf.length();
		int retlen = Math.min(buflen, (int)(pos - 1 + len));
		
		if (retlen > 0) {
			buf.replace((int)(pos - 1), (int)(pos + retlen), str.substring(offset - 1, (offset + len)));
			return(retlen);
		} else {
			return(0);
		}
	}

	/**
	 * Truncates the CLOB value that this Clob designates to
	 * have a length of len characters.
	 *
	 * @param len the length, in bytes, to which the CLOB value
	 *        should be truncated
	 * @throws SQLException if there is an error accessing the
	 *         CLOB value
	 */
	public void truncate(long len) {
		// this command is a no-op
	}

	/**
	 * Returns the String behind this Clob.  This is a MonetClob
	 * extension that does not violate nor is described in the Clob
	 * interface.
	 *
	 * @return the String this Clob wraps.
	 */
	public String toString() {
		return(buf.toString());
	}
}
