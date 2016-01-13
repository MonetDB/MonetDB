/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.parser;

import java.nio.CharBuffer;

/**
 * The StartOfHeaderParser allows easy examination of a start of header
 * line.  It does not fit into the general MCLParser framework because
 * it uses a different interface.  While the parser is very shallow, it
 * requires the caller to know about the header lines that are parsed.
 * All this parser does is detect the (valid) type of a soheader, and
 * allow to return the fields in it as integer or string.  An extra
 * bonus is that it can return if another field should be present in the
 * soheader.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class StartOfHeaderParser {
	private CharBuffer soh = null;
	private int len;
	private int pos;

	/* Query types (copied from sql_query.mx) */

	/** A parse response (not handled) */
	public final static int Q_PARSE    = '0';
	/** A tabular response (typical ResultSet) */
	public final static int Q_TABLE    = '1';
	/** A response to an update statement, contains number of affected
	 * rows and generated key-id */
	public final static int Q_UPDATE   = '2';
	/** A response to a schema update */
	public final static int Q_SCHEMA   = '3';
	/** A response to a transation statement (start, rollback, abort,
	 * commit) */
	public final static int Q_TRANS    = '4';
	/** A tabular response in response to a PREPARE statement containing
	 * information about the wildcard values that need to be supplied */
	public final static int Q_PREPARE  = '5';
	/** A tabular continuation response (for a ResultSet) */
	public final static int Q_BLOCK    = '6';
	/** An unknown and unsupported response */
	public final static int Q_UNKNOWN  =  0 ;

	public final int parse(String in) throws MCLParseException {
		soh = CharBuffer.wrap(in);
		soh.get();	// skip the &
		int type = soh.get();
		switch (type) {
			default:
				throw new MCLParseException("invalid or unknown header", 1);
			case Q_PARSE:
			case Q_SCHEMA:
				len = 0;
			break;
			case Q_TABLE:
			case Q_PREPARE:
				len = 4;
				soh.get();
			break;
			case Q_UPDATE:
				len = 2;
				soh.get();
			break;
			case Q_TRANS:
				len = 1;
				soh.get();
			break;
			case Q_BLOCK:
				len = 3;
				soh.get();
			break;
		}
		pos = 0;
		return type;
	}

	public final boolean hasNext() {
		return pos < len;
	}

	/**
	 * Returns the next token in the CharBuffer as integer. The value is
	 * considered to end at the end of the CharBuffer or at a space.  If
	 * a non-numeric character is encountered an MCLParseException is
	 * thrown.
	 *
	 * @return The next token in the CharBuffer as integer
	 * @throws MCLParseException if no numeric value could be read
	 */
	public final int getNextAsInt() throws MCLParseException {
		pos++;
		if (!soh.hasRemaining()) throw
			new MCLParseException("unexpected end of string", soh.position() - 1);
		int tmp;
		char chr = soh.get();
		// note: don't use Character.isDigit() here, because
		// we only want ISO-LATIN-1 digits
		if (chr >= '0' && chr <= '9') {
			tmp = (int)chr - (int)'0';
		} else {
			throw new MCLParseException("expected a digit", soh.position() - 1);
		}

		while (soh.hasRemaining() && (chr = soh.get()) != ' ') {
			tmp *= 10;
			if (chr >= '0' && chr <= '9') {
				tmp += (int)chr - (int)'0';
			} else {
				throw new MCLParseException("expected a digit", soh.position() - 1);
			}
		}

		return tmp;
	}

	public final String getNextAsString() throws MCLParseException {
		pos++;
		if (!soh.hasRemaining()) throw
			new MCLParseException("unexpected end of string", soh.position() - 1);
		int cnt = 0;
		soh.mark();
		while (soh.hasRemaining() && soh.get() != ' ') {
			cnt++;
		}

		soh.reset();

		return soh.subSequence(0, cnt).toString();
	}
}
