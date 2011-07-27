/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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

package nl.cwi.monetdb.mcl.parser;

import java.util.*;

/**
 * Interface for parsers in MCL.  The parser family in MCL is set up as
 * a reusable object.  This allows the same parser to be used again for
 * the same type of work.  While this is a very unnatural solution in
 * the Java language, it prevents many object creations on a low level
 * of the protocol.  This favours performance.
 * <br /><br />
 * A typical parser has a method parse() which takes a String, and the
 * methods hasNext() and next() to retrieve the values that were
 * extracted by the parser.  Parser specific methods may be available to
 * perform common tasks.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public abstract class MCLParser {
	/** The String values found while parsing.  Public, you may touch it. */
	public final String values[];
	/** The int values found while parsing.  Public, you may touch it. */
	public final int intValues[];
	private int colnr;

	/**
	 * Creates an MCLParser targetted at a given number of field values.
	 * The lines parsed by an instance of this MCLParser should have
	 * exactly capacity field values.
	 *
	 * @param capacity the number of field values to expect
	 */
	protected MCLParser(int capacity) {
		values = new String[capacity];
		intValues = new int[capacity];
	}

	/**
	 * Parse the given string, and populate the internal field array
	 * to allow for next() and hasNext() calls.
	 *
	 * @param source the String containing the line to parse
	 * @throws MCLParseException if source cannot be (fully) parsed by
	 * this parser
	 * @see #next()
	 * @see #nextInt()
	 * @see #hasNext()
	 */
	abstract public int parse(String source) throws MCLParseException;

	/**
	 * Repositions the internal field offset to the start, such that the
	 * next call to next() will return the first field again.
	 */
	final public void reset() {
		colnr = 0;
	}

	/**
	 * Returns whether the next call to next() or nextInt() succeeds.
	 *
	 * @return true if the next call to next() or nextInt() is bound to
	 * succeed
	 * @see #next()
	 * @see #nextInt()
	 */
	final public boolean hasNext() {
		return(colnr < values.length);
	}

	/**
	 * Returns the current field value, and advances the field counter
	 * to the next value.  This method may fail with a RuntimeError if
	 * the current field counter is out of bounds.  Call hasNext() to
	 * determine if the call to next() will succeed.
	 *
	 * @return the current field value
	 * @see #nextInt()
	 * @see #hasNext()
	 */
	final public String next() {
		return(values[colnr++]);
	}

	/**
	 * Returns the current field value as integer, and advances the
	 * field counter to the next value.  This method has the same
	 * characteristics as the next() method, apart from returning the
	 * field value as an integer.
	 *
	 * @return the current field value as integer
	 * @see #next()
	 */
	final public int nextInt() {
		return(intValues[colnr++]);
	}
}
