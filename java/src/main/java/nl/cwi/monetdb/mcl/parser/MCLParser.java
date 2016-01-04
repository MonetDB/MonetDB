/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.parser;


/**
 * Interface for parsers in MCL.  The parser family in MCL is set up as
 * a reusable object.  This allows the same parser to be used again for
 * the same type of work.  While this is a very unnatural solution in
 * the Java language, it prevents many object creations on a low level
 * of the protocol.  This favours performance.
 * 
 * A typical parser has a method parse() which takes a String, and the
 * methods hasNext() and next() to retrieve the values that were
 * extracted by the parser.  Parser specific methods may be available to
 * perform common tasks.
 *
 * @author Fabian Groffen
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
	 * @return value
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
		return colnr < values.length;
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
		return values[colnr++];
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
		return intValues[colnr++];
	}
}
