/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.mcl.parser;

/**
 * The TupleLineParser extracts the values from a given tuple.  The
 * number of values that are expected are known upfront to speed up
 * allocation and validation.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class TupleLineParser extends MCLParser {
	/**
	 * Constructs a TupleLineParser which expects columncount columns.
	 *
	 * @param columncount the number of columns in the to be parsed string
	 */
	public TupleLineParser(int columncount) {
		super(columncount);
	}

	/**
	 * Parses the given String source as tuple line.  If source cannot
	 * be parsed, a ParseException is thrown.  The columncount argument
	 * is used for allocation of the returned array.  While this seems
	 * illogical, the caller should know this size, since the
	 * StartOfHeader contains this information.
	 *
	 * @param source a String which should be parsed
	 * @return 0, as there is no 'type' of TupleLine
	 * @throws ParseException if an error occurs during parsing
	 */
	@Override
	public int parse(String source) throws MCLParseException {
		int len = source.length();
		char[] chrLine = new char[len];
		source.getChars(0, len, chrLine, 0);

		// first detect whether this is a single value line (=) or a
		// real tuple ([)
		if (chrLine[0] == '=') {
			if (values.length != 1)
				throw new MCLParseException(values.length +
						" columns expected, but only single value found");

			// return the whole string but the leading =
			values[0] = source.substring(1);

			// reset colnr
			reset();

			return 0;
		}

		// extract separate fields by examining string, char for char
		boolean inString = false, escaped = false;
		int cursor = 2, column = 0, i = 2;
		StringBuilder uesc = new StringBuilder();
		for (; i < len; i++) {
			switch(chrLine[i]) {
				default:
					escaped = false;
				break;
				case '\\':
					escaped = !escaped;
				break;
				case '"':
					/**
					 * If all strings are wrapped between two quotes, a \" can
					 * never exist outside a string. Thus if we believe that we
					 * are not within a string, we can safely assume we're about
					 * to enter a string if we find a quote.
					 * If we are in a string we should stop being in a string if
					 * we find a quote which is not prefixed by a \, for that
					 * would be an escaped quote. However, a nasty situation can
					 * occur where the string is like "test \\" as obvious, a
					 * test for a \ in front of a " doesn't hold here for all
					 * cases. Because "test \\\"" can exist as well, we need to
					 * know if a quote is prefixed by an escaping slash or not.
					 */
					if (!inString) {
						inString = true;
					} else if (!escaped) {
						inString = false;
					}

					// reset escaped flag
					escaped = false;
				break;
				case '\t':
					if (!inString &&
						(i > 0 && chrLine[i - 1] == ',') ||
						(i + 1 == len - 1 && chrLine[++i] == ']')) // dirty
					{
						// split!
						if (chrLine[cursor] == '"' &&
							chrLine[i - 2] == '"')
						{
							// reuse the StringBuilder by cleaning it
							uesc.delete(0, uesc.length());
							// prevent capacity increasements
							uesc.ensureCapacity((i - 2) - (cursor + 1));
							for (int pos = cursor + 1; pos < i - 2; pos++) {
								if (chrLine[pos] == '\\' && pos + 1 < i - 2) {
									pos++;
									// strToStr and strFromStr in gdk_atoms.mx only
									// support \t \n \\ \" and \377
									switch (chrLine[pos]) {
										case '\\':
											uesc.append('\\');
										break;
										case 'n':
											uesc.append('\n');
										break;
										case 't':
											uesc.append('\t');
										break;
										case '"':
											uesc.append('"');
										break;
										case '0': case '1': case '2': case '3':
											// this could be an octal number, let's check it out
											if (pos + 2 < i - 2 &&
												chrLine[pos + 1] >= '0' && chrLine[pos + 1] <= '7' &&
												chrLine[pos + 2] >= '0' && chrLine[pos + 2] <= '7'
											) {
												// we got the number!
												try {
													uesc.append((char)(Integer.parseInt("" + chrLine[pos] + chrLine[pos + 1] + chrLine[pos + 2], 8)));
													pos += 2;
													break;
												} catch (NumberFormatException e) {
													// hmmm, this point should never be reached actually...
													throw new AssertionError("Flow error, should never try to parse non-number");
												}
											}
											// do default action if number seems not to be correct
										default:
											// this is wrong, just ignore the escape, and print the char
											uesc.append(chrLine[pos]);
										break;
									}
								} else {
									uesc.append(chrLine[pos]);
								}
							}

							// put the unescaped string in the right place
							values[column++] = uesc.toString();
						} else if ((i - 1) - cursor == 4 &&
								source.indexOf("NULL", cursor) == cursor)
						{
							values[column++] = null;
						} else {
							values[column++] =
								source.substring(cursor, i - 1);
						}
						cursor = i + 1;
					}

					// reset escaped flag
					escaped = false;
				break;
			}
		}
		// check if this result is of the size we expected it to be
		if (column != values.length)
			throw new MCLParseException("illegal result length: " + column + "\nlast read: " + (column > 0 ? values[column - 1] : "<none>"));

		// reset colnr
		reset();
		
		return 0;
	}
}
