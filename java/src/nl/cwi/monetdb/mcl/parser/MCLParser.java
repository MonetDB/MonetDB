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

	protected MCLParser(int capacity) {
		values = new String[capacity];
		intValues = new int[capacity];
	}

	abstract public int parse(String source) throws MCLParseException;

	final public void reset() {
		colnr = 0;
	}

	final public boolean hasNext() {
		return(colnr < values.length);
	}

	final public String next() {
		return(values[colnr++]);
	}

	final public int nextInt() {
		return(intValues[colnr++]);
	}

	final public void remove() {
		throw new AssertionError("Not implemented");
	}
}
