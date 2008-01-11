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
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.mcl.parser;

import java.util.*;
import java.nio.*;

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

	/** Query types (copied from sql_query.mx) */
	public final static int Q_PARSE    = '0';
	public final static int Q_TABLE    = '1';
	public final static int Q_UPDATE   = '2';
	public final static int Q_SCHEMA   = '3';
	public final static int Q_TRANS    = '4';
	public final static int Q_PREPARE  = '5';
	public final static int Q_BLOCK    = '6';
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
		return(type);
	}

	public final boolean hasNext() {
		return(pos < len);
	}

	/**
	 * Returns the next token in the CharBuffer as integer.  The value is
	 * considered to end at the end of the CharBuffer or at a space.  If
	 * a non-numeric character is encountered an MCLParseException is
	 * thrown.
	 *
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

		return(tmp);
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
		return(soh.subSequence(0, cnt).toString());
	}
}
