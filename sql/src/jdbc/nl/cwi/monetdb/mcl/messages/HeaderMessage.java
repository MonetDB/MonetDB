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
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.mcl.messages;

import java.util.*;
import nl.cwi.monetdb.mcl.*;

/**
 * An HeaderMessage is a server originated message, sent by the
 * server in order to indicate a result set is available.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class HeaderMessage extends MCLMessage {
	/** The character that identifies this message */
	public static final char identifier = 't';

	private final static MCLSentence startOfMessageSentence;
	
	static {
		try {
			startOfMessageSentence = new MCLSentence('&', "" + identifier);
		} catch (MCLException e) {
			throw new AssertionError("Unable to create core sentence");
		}
	}

	// these represent the internal values of this Message
	private String id;
	private int columncount;
	private int tuplecount;
	private String ctype;
	private String[] column;
	private String[] table;
	private String[] schema;
	private String[] type;
	private int[] digits;
	private int[] scale;
	private int[] width;
	
	/**
	 * Constructs an empty HeaderMessage.  The sentences need to
	 * be added using the addSentence() method.  This constructor is
	 * suitable when reconstructing messages from a stream.
	 */
	public HeaderMessage() {
		// nothing has to be done here
		sentences = new MCLSentence[11];
	}

	/**
	 * Constructs a filled HeaderMessage.  All MCL metadata information
	 * is supplied and stored in this HeaderMessage.
	 *
	 * @param id the server side result id
	 * @param columncount the number of columns for this result
	 * @param tuplecount the total number of tuples for this result
	 * @param ctype the C-type column value types
	 * @throws MCLException if the id or ctype string is null, or
	 * columncount is zero
	 */
	public HeaderMessage(
			String id,
			int columncount,
			int tuplecount,
			String ctype
	) throws MCLException {
		if (id == null) throw
			new MCLException("Result set id may not be null");
		if (ctype == null) throw
			new MCLException("C-Type may not be null");
		if (columncount == 0) throw
			new MCLException("A result with no columns makes no sense");
		if (ctype.length() != columncount) throw
			new MCLException("ctype size (" + ctype.length() + ") and columncount (" + columncount + ") are not equal");
		
		sentences = new MCLSentence[11];
		this.id = id;
		this.columncount = columncount;
		this.tuplecount = tuplecount;
		this.ctype = ctype;
		sentences[0] = new MCLSentence('$', "id", id);
		sentences[1] = new MCLSentence('$', "columncount", "" + columncount);
		sentences[2] = new MCLSentence('$', "tuplecount", "" + tuplecount);
		sentences[3] = new MCLSentence('$', "ctype", ctype);
	}

	/**
	 * Sets the column header if not yet set.
	 *
	 * @param columns an array of Strings representing the column names
	 * @throws MCLException if the columncount header is not yet set,
	 * the number of Strings in the array doesn't match the columncount,
	 * or the column header is already set.
	 */
	public void addColumn(String[] columns) throws MCLException {
		if (columncount == 0) throw
			new MCLException("columncount header not yet set");
		if (columns.length != columncount) throw
			new MCLException("There should be " + columncount + " columns, only " + columns.length + " found");
		if (sentences[4] != null) throw
			new MCLException("column header already set");

		column = columns;
		sentences[4] = new MCLSentence(MCLSentence.METADATA, "column", columns);
	}

	/**
	 * Sets the table header if not yet set.
	 *
	 * @param tables an array of Strings representing the table names
	 * @throws MCLException if the columncount header is not yet set,
	 * the number of Strings in the array doesn't match the columncount,
	 * or the table header is already set.
	 */
	public void addTable(String[] tables) throws MCLException {
		if (columncount == 0) throw
			new MCLException("columncount header not yet set");
		if (tables.length != columncount) throw
			new MCLException("There should be " + columncount + " columns, only " + tables.length + " found");
		if (sentences[5] != null) throw
			new MCLException("table header already set");

		table = tables;
		sentences[5] = new MCLSentence(MCLSentence.METADATA, "table", tables);
	}

	/**
	 * Sets the schema header if not yet set.
	 *
	 * @param schemas an array of Strings representing the schema names
	 * @throws MCLException if the columncount header is not yet set,
	 * the number of Strings in the array doesn't match the columncount,
	 * or the schema header is already set.
	 */
	public void addSchema(String[] schemas) throws MCLException {
		if (columncount == 0) throw
			new MCLException("columncount header not yet set");
		if (schemas.length != columncount) throw
			new MCLException("There should be " + columncount + " columns, only " + schemas.length + " found");
		if (sentences[6] != null) throw
			new MCLException("schema header already set");

		schema = schemas;
		sentences[6] = new MCLSentence(MCLSentence.METADATA, "schema", schemas);
	}

	/**
	 * Sets the type header if not yet set.
	 *
	 * @param types an array of Strings representing the schema names
	 * @throws MCLException if the columncount header is not yet set,
	 * the number of Strings in the array doesn't match the columncount,
	 * or the schema header is already set.
	 */
	public void addTypes(String[] types) throws MCLException {
		if (columncount == 0) throw
			new MCLException("columncount header not yet set");
		if (types.length != columncount) throw
			new MCLException("There should be " + columncount + " columns, only " + types.length + " found");
		if (sentences[7] != null) throw
			new MCLException("type header already set");

		type = types;
		sentences[7] = new MCLSentence(MCLSentence.METADATA, "type", types);
	}

	/**
	 * Sets the digits header if not yet set.
	 *
	 * @param digits an array of Strings representing the schema names
	 * @throws MCLException if the columncount header is not yet set,
	 * the number of Strings in the array doesn't match the columncount,
	 * or the schema header is already set.
	 */
	public void addDigits(int[] digits) throws MCLException {
		if (columncount == 0) throw
			new MCLException("columncount header not yet set");
		if (digits.length != columncount) throw
			new MCLException("There should be " + columncount + " columns, only " + digits.length + " found");
		if (sentences[8] != null) throw
			new MCLException("digits header already set");

		this.digits = digits;
		sentences[8] = new MCLSentence(MCLSentence.METADATA, "digits", digits);
	}

	/**
	 * Sets the scale header if not yet set.
	 *
	 * @param scales an array of Strings representing the schema names
	 * @throws MCLException if the columncount header is not yet set,
	 * the number of Strings in the array doesn't match the columncount,
	 * or the schema header is already set.
	 */
	public void addScale(int[] scales) throws MCLException {
		if (columncount == 0) throw
			new MCLException("columncount header not yet set");
		if (scales.length != columncount) throw
			new MCLException("There should be " + columncount + " columns, only " + scales.length + " found");
		if (sentences[9] != null) throw
			new MCLException("scale header already set");

		scale = scales;
		sentences[9] = new MCLSentence(MCLSentence.METADATA, "scale", scales);
	}

	/**
	 * Sets the width header if not yet set.
	 *
	 * @param widths an array of Strings representing the schema names
	 * @throws MCLException if the columncount header is not yet set,
	 * the number of Strings in the array doesn't match the columncount,
	 * or the schema header is already set.
	 */
	public void addWidth(int[] widths) throws MCLException {
		if (columncount == 0) throw
			new MCLException("columncount header not yet set");
		if (widths.length != columncount) throw
			new MCLException("There should be " + columncount + " columns, only " + widths.length + " found");
		if (sentences[10] != null) throw
			new MCLException("width header already set");

		width = widths;
		sentences[10] = new MCLSentence(MCLSentence.METADATA, "width", widths);
	}

	/**
	 * Returns the type of this Message as an integer type.
	 * 
	 * @return an integer value that represents the type of this Message
	 */
	public int getType() {
		return(identifier);
	}

	/**
	 * Returns the start of message sentence for this Message: &amp;e.
	 *
	 * @return the start of message sentence
	 */
	public MCLSentence getSomSentence() {
		return(startOfMessageSentence);
	}


	/**
	 * Adds the given String to this Message if it matches the Message
	 * type.  The sentence is parsed as far as that is considered to be
	 * necessary to validate it against the Message type.  If a sentence
	 * is not valid, an MCLException is thrown.
	 * 
	 * @param in an MCLSentence object
	 * @throws MCLException if the given sentence is not considered to
	 * be valid
	 */
	public void addSentence(MCLSentence in) throws MCLException {
		String prop;
		// see if it is a supported header
		switch (in.getType()) {
			case MCLSentence.MCLMETADATA:
				prop = in.getField(1);
				if (prop == null) throw
					new MCLException("Illegal sentence (no property): " + in.getString());
				String value = in.getField(2);
				if (value == null) throw
					new MCLException("Illegal sentence (no value): " + in.getString());

				if (prop.equals("id")) {
					id = value;
					sentences[0] = in;
				} else if (prop.equals("columncount")) {
					try {
						columncount = Integer.parseInt(value);
						if (columncount < 0) throw
							new MCLException("Columncount cannot be less than zero (" + columncount + ")");
						sentences[1] = in;
					} catch (NumberFormatException e) {
						throw new MCLException("Illegal value for header 'columncount': " + value);
					}
				} else if (prop.equals("tuplecount")) {
					try {
						tuplecount = Integer.parseInt(value);
						if (tuplecount < 0) throw
							new MCLException("Tuplecount cannot be less than zero (" + tuplecount + ")");
						sentences[2] = in;
					} catch (NumberFormatException e) {
						throw new MCLException("Illegal value for header 'tuplecount': " + value);
					}
				} else if (prop.equals("ctype")) {
					ctype = value;
					sentences[3] = in;
				} else {
					throw new MCLException("Illegal property '" + prop + "' for this Message");
				}
			break;
			case MCLSentence.METADATA:
				if (columncount == 0) throw
					new MCLException("columncount header not yet set");
				String[] values = in.getFields();
				prop = values[0];
				if (values.length - 1 != columncount) throw
					new MCLException("Unexpected number of fields (" + values.length + "), expected: " + columncount);

				if ("column".equals(prop)) {
					column = new String[columncount];
					for (int i = 1; i < values.length; i++)
						column[i - 1] = values[i];
					sentences[4] = in;
				} else if ("table".equals(prop)) {
					table = new String[columncount];
					for (int i = 1; i < values.length; i++)
						table[i - 1] = values[i];
					sentences[5] = in;
				} else if ("schema".equals(prop)) {
					schema = new String[columncount];
					for (int i = 1; i < values.length; i++)
						schema[i - 1] = values[i];
					sentences[6] = in;
				} else if ("type".equals(prop)) {
					type = new String[columncount];
					for (int i = 1; i < values.length; i++)
						type[i - 1] = values[i];
					sentences[7] = in;
				} else if ("digits".equals(prop)) {
					digits = new int[columncount];
					int i = 0;
					try {
						for (i = 1; i < values.length; i++)
							digits[i - 1] = Integer.parseInt(values[i]);
					} catch(NumberFormatException e) {
						throw new MCLException("Illegal field value for header digits: " + values[i]);
					}
					sentences[8] = in;
				} else if ("scale".equals(prop)) {
					scale = new int[columncount];
					int i = 0;
					try {
						for (i = 1; i < values.length; i++)
							scale[i - 1] = Integer.parseInt(values[i]);
					} catch(NumberFormatException e) {
						throw new MCLException("Illegal field value for header scale: " + values[i]);
					}
					sentences[9] = in;
				} else if ("width".equals(prop)) {
					width = new int[columncount];
					int i = 0;
					try {
						for (i = 1; i < values.length; i++)
							width[i - 1] = Integer.parseInt(values[i]);
					} catch(NumberFormatException e) {
						throw new MCLException("Illegal field value for header width: " + values[i]);
					}
					sentences[10] = in;
				} else {
					throw new MCLException("Unknown property: " + prop);
				}
			break;
			default:
				throw new MCLException("Sentence type not allowed for this message: " + (char)in.getType());
		}
	}


	// the following are message specific getters that retrieve the
	// values inside the message

	/**
	 * Retrieves the result set id contained in this Message object.
	 *
	 * @return the result set id
	 */
	public String getId() {
		return(id);
	}

	/**
	 * Retrieves the column count contained in this Message object.
	 *
	 * @return the column count
	 */
	public int getColumnCount() {
		return(columncount);
	}

	/**
	 * Retrieves the tuple count contained in this Message object.
	 *
	 * @return the tuple count
	 */
	public int getTupleCount() {
		return(tuplecount);
	}

	/**
	 * Retrieves the C-Type string contained in this Message object.
	 *
	 * @return the C-Type string
	 */
	public String getCType() {
		return(ctype);
	}
}
