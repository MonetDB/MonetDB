/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.parser;

import java.text.ParseException;

/**
 * When an MCLParseException is thrown, the MCL protocol is violated by
 * the sender.  In general a stream reader throws an
 * MCLParseException as soon as something that is read cannot be
 * understood or does not conform to the specifications (e.g. a
 * missing field).  The instance that throws the exception will try to
 * give an error offset whenever possible.  Alternatively it makes sure
 * that the error message includes the offending data read.
 */
public class MCLParseException extends ParseException {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MCLParseException(String e) {
		super(e, -1);
	}

	public MCLParseException(String e, int offset) {
		super(e, offset);
	}
}
