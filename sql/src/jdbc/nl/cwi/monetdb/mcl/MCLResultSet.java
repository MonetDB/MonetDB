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

package nl.cwi.monetdb.mcl;

import nl.cwi.monetdb.mcl.messages.*;
import java.util.*;
import java.lang.reflect.Array; // for dealing with the anonymous columns

/**
 * An MCLResultSet is a container for tabular data.  An MCLServer
 * instance uses this oject to generate a HeaderMessage and DataMessages
 * as appropriately.  An MCLResultSet can store a number of 'columns'
 * and their metadata.  Each column stored in an MCLResultSet must have
 * an equal number of values (tuples).
 *
 * Note: this class is the equivalent of the rsbox in MonetDB/Five.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MCLResultSet {
	private boolean isValid;
	private List columns;
	private String id;

	private static idcnt = 0;
	
	/**
	 * Creates a new MCLResultSet that is in intially empty.  An empty
	 * MCLResultSet is not valid when being supplied to an MCLServer.
	 * This constructor is synchronised to avoid race conditions when
	 * determining its id from idcnt.
	 */
	public synchronized MCLResultSet() {
		isValid = false;
		columns = new ArrayList();
		id = "rs" + idcnt++;
	}

	/**
	 * Adds the given Object array as column to this MCLResultSet.
	 *
	 * @param values the column values to add
	 * @throws MCLException if this MCLResultSet was already marked
	 *         valid
	 */
	public void addColumn(Object[] values) throws MCLException {
		if (isValid) throw
			new MCLException("Cannot append to a valid ResultSet");

		columns.add(new Column(values));
	}
	
	/**
	 * Checks and marks whether this MCLResultSet is valid.  An
	 * MCLResultSet is considered to be valid if:
	 * <ul>
	 * <li>there is at least one column</li>
	 * <li>all columns are of equal length</li>
	 * </ul>
	 * Note that if this method is called on an already valid
	 * MCLResultSet, this method returns directly.
	 *
	 * @throws MCLException if one of the conditions described above is
	 *         not met
	 */
	public void setValid() throws MCLException {
		if (isValid) return;
		if (columns.size() == 0) throw
			new MCLException("This MCLResultSet has no columns yet");
		int lastLength = -1, thisLength;
		for (Iterator it = columns.iterator(); it.hasNext(); ) {
			thisLength = Array.getLength(it.next());
			if (lastLength == -1) lastLength = thisLength;
			if (thisLength != lastLength) throw
				new MCLException("Not all columns are of equal length " +
						"(" + lastLength + "/" + thisLength + ")");
		}
		isValid = true;
	}

	/**
	 * Returns whether this MCLResultSet is valid.
	 *
	 * @return true if this MCLResultset is valid, false otherwise
	 */
	public boolean isValid() {
		return(isValid);
	}

	/**
	 * Returns the id for this MCLResultSet.
	 *
	 * @return the id
	 */
	public String getId() {
		return(id);
	}

	/**
	 * Retrieves the metadata contained in this MCLResultSet as
	 * HeaderMessage.  If this MCLResultSet is not valid, an
	 * MCLException is thrown.
	 *
	 * @return a HeaderMessage populated with the metadata stored in
	 *         this MCLResultSet
	 * @throws MCLException if this MCLResultSet is not (yet) valid
	 */
	public HeaderMessage getHeaderMessage() throws MCLException {
		if (!isValid) throw
			new MCLException("MCLResultSet should be valid");

		String ctype = "";
		for (int i = 0; i < columns.size(); i++)
			ctype += ((Column)(columns.get(i))).ctype;

		HeaderMessage ret = new HeaderMessage(
				id,
				columns.size(),
				((Column)(columns.get(0))).data.length,
				ctype
		);
		String tmpS[] = new String[columns.size()];
		for (int i = 0; i < columns.size(); i++)
			tmpS[i] = ((Column)(columns.get(0))).column;
		ret.addColumn(tmpS);
		for (int i = 0; i < columns.size(); i++)
			tmpS[i] = ((Column)(columns.get(0))).schema;
		ret.addSchema(tmpS);
		for (int i = 0; i < columns.size(); i++)
			tmpS[i] = ((Column)(columns.get(0))).table;
		ret.addTable(tmpS);
		for (int i = 0; i < columns.size(); i++)
			tmpS[i] = ((Column)(columns.get(0))).type;
		ret.addTypes(tmpS);
		int tmpI[] = new int[columns.size()];
		for (int i = 0; i < columns.size(); i++)
			tmpI[i] = ((Column)(columns.get(0))).digits;
		ret.addDigits(tmpI);
		for (int i = 0; i < columns.size(); i++)
			tmpI[i] = ((Column)(columns.get(0))).scale;
		ret.addScale(tmpI);
		for (int i = 0; i < columns.size(); i++)
			tmpI[i] = ((Column)(columns.get(0))).width;
		ret.addWidth(tmpI);
		
		return(ret);
	}

	/**
	 * Retrieves a DataMessage for the given index position range.  If
	 * the start or stop positions are out of the possible range an
	 * MCLException is thrown.
	 *
	 * @param start the index position to start at, inclusive
	 * @param stop the index position to stop at, exclusive
	 * @throws MCLException if this MCLResultSet is not (yet) valid, or
	 *         start and/or stop are out of range.
	 */
	public DataMessage getDataMessage(int start, int stop)
		throws MCLException
	{
		int dataSize = ((Column)(columns.get(0))).data.length;
		if (start >= dataSize || start < 0) throw
			new MCLException("start index either too small or too large (0 <= " + start + " < " + dataSize + ")");
		if (stop > dataSize || stop <= 0) throw
			new MCLException("stop index either too small or too large (0 < " + stop + " <= " + dataSize + ")");

		DataMessage ret = new DataMessage(stop - start);
		String tmpS[] = new String[columns.size()];
		for ( ; start < stop; start++) {
			for (int i = 0; i < columns.size(); i++) {
				Column col = (Column)(columns.get(i));
				switch (col.ctype) {
					case 'B':
						tmpS[i] = ((Boolean)(col.data[i])).booleanValue() ? "1" : "0";
					break;
					case 'c':
						tmpS[i] = "" + ((Character)(col.data[i])).charValue();
					break;
					case 'S':
						tmpS[i] = col.data[i].toString().length() + ":" + col.data[i].toString();
					break;				}
					case 'o':
						// not known (yet?)
						tmpS[i] = "";
					break;
					case 'b':
					case 's':
					case 'i':
						tmpS[i] = "" + ((Number)(col.data[i])).intValue();
					break;
					case 'l':
						tmpS[i] = "" + ((Number)(col.data[i])).longValue();
					break;
					case 'f':
					case 'd':
						tmpS[i] = col.data[i].toString();
					break;
					case 'L':
						// not (yet?) supported
						tmpS[i] = "";
					break;
					case 'T':
					{
						long millis = ((java.util.Date)(col.data[i])).getTime();
						Calendar cal = Calendar.getInstance();
						cal.setTime((java.util.Date)(col.data[i]));
						int zone = -(cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / (60 * 1000);
						tmpS[i] = prefixZero(milis / 1000, 10) +
							prefixZero(millis % 1000 * 10, 7) +
							(zone < 0 ? "-" : "+") +
							prefixZero(zone < 0 ? -zone : zone, 5);
					}
					break;
					case 't':
					{
						long millis = ((java.sql.Time)(col.data[i])).        getTime();
						Calendar cal = Calendar.getInstance();
						cal.setTime((java.sql.Time)(col.data[i]));
						int zone = -(cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET)) / (60 * 1000);
						tmpS[i] = prefixZero(milis / 1000, 5) +
							(zone < 0 ? "-" : "+") +
							prefixZero(zone < 0 ? -zone : zone, 5);
					}
					break;
					case 'D':
					{
						long millis = ((java.sql.Date)(col.data[i])).getTime();
						tmpS[i] = prefixZero(milis / 1000, 10);
					}
					break;
					case 'u':
					{
						String clazz = col.data[i].getClass().getName();
						clazz = clazz.substring(clazz.lastIndexOf(".") + 1);
						tmpS[i] = clazz + ":" +
							col.data[i].toString().length() + ":" +
							col.data[i].toString();
					}
					break;
			}
			red.addSentence(new MCLSentence(MCLSentence.DATA, tmpS));
		}

		return(ret);
	}

	/**
	 * Utility function that returns the given numerical object prefixed
	 * with as much zeroes as necessary to return a String of the
	 * desired length.
	 *
	 * @param num the numerical object
	 * @param len the desired length
	 * @return a String of length len representing the numerical object
	 */
	private String prefixZero(long num, int len) throws MCLException {
		if (num < 0) throw
			new MCLException("Negative values not supported");

		StringBuffer ret = new StringBuffer(len);
		ret.append("" + num);

		if (ret.length() > len) throw
			new MCLException("Given value does not fit in the desired length");

		while (ret.length() < len) ret.insert(0, '0');

		return(ret.toString());
	}

	/**
	 * Small inner class that represents a column within an MCLResultSet
	 * and its associated metadata.
	 */
	public class Column {
		private Object[] data;
		private char ctype;
		private String column;
		private String table;
		private String schema;
		private String type;
		private int digits;
		private int scale;
		private int width;

		/**
		 * Constructs a new column using the given values.  For each
		 * column added, the minimum required set of metadata is
		 * automatically generated.
		 *
		 * @param data the values to populate the column with
		 * @throws MCLException if data is null
		 */
		public Column(Object[] data) throws MCLException {
			if (data == null) throw
				new MCLException("Column values may not be absent");

			this.data = data;
			// set default values based on what we have
			if (data instanceof Boolean[]) {
				ctype = 'B';
				type = "boolean";
				width = 1; // either 0 or 1
			} else if (data instanceof Character[]) {
				ctype = 'c';
				type = "char";
				width = 1;
			} else if (data instanceof String[]) {
				ctype = 'S';
				type = "varchar";
				width = 0;
				for (int i = 0; i < data.length; i++) {
					if (width < data[i].toString().length())
						width = data[i].toString().length();
				}
			} else if (data instanceof Byte[]) {
				ctype = 'b';
				type = "tinyint";
				byte tmp = 0;
				for (int i = 0; i < data.length; i++) {
					if (tmp < ((Byte)(data[i])).byteValue())
						tmp = ((Byte)(data[i])).byteValue();
				}
				width = 0;
				while ((tmp /= 10) > 0) width++;
			} else if (data instanceof Short[]) {
				ctype = 's';
				type = "smallint";
				short tmp = 0;
				for (int i = 0; i < data.length; i++) {
					if (tmp < ((Short)(data[i])).shortValue())
						tmp = ((Short)(data[i])).shortValue();
				}
				width = 0;
				while ((tmp /= 10) > 0) width++;
			} else if (data instanceof Integer[]) {
				ctype = 'i';
				type = "int";
				int tmp = 0;
				for (int i = 0; i < data.length; i++) {
					if (tmp < ((Integer)(data[i])).intValue())
						tmp = ((Integer)(data[i])).intValue();
				}
				width = 0;
				while ((tmp /= 10) > 0) width++;
			} else if (data instanceof Long[]) {
				ctype = 'l';
				type = "bigint";
				long tmp = 0;
				for (int i = 0; i < data.length; i++) {
					if (tmp < ((Long)(data[i])).longValue())
						tmp = ((Long)(data[i])).longValue();
				}
				width = 0;
				while ((tmp /= 10) > 0) width++;
			} else if (data instanceof Float[]) {
				ctype = 'f';
				type = "real";
				width = 12; // SQL99 max
			} else if (data instanceof Double[]) {
				ctype = 'd';
				type = "double";
				width = 24; // SQL99 max
			} else if (data instanceof java.sql.Timestamp[] ||
					data instanceof java.util.Date[]) {
				ctype = 'T';
				type = "timestampz";
				width = 22;
			} else if (data instanceof java.sql.Time[]) {
				ctype = 't';
				type = "timez";
				width = 11;
			} else if (data instanceof java.sql.Date[]) {
				ctype = 'D';
				type = "date";
				width = 10;
			} else {
				ctype = 'S';	// we map anything else to a String
				type = "varchar";
				width = 0;
				for (int i = 0; i < data.length; i++) {
					if (width < data[i].toString().length())
						width = data[i].toString().length();
				}
			}
			// name defaults to column_x
			column = "column_" + this.hashCode();
			table = null;
			schema = null;
			digits = -1;
			scale = -1;
		}

		/**
		 * Sets the column name for this column.
		 *
		 * @param name the column name
		 */
		public void setColumnName(String name) {
			column = name;
		}

		/**
		 * Sets the table name for this column.
		 *
		 * @param name the table name
		 */
		public void setTableName(String name) {
			table = name;
		}

		/**
		 * Sets the schema name for this column.
		 *
		 * @param name the schema name
		 */
		public void setSchemaName(String name) {
			schema = name;
		}

		/**
		 * Sets the (SQL) type for this column.
		 *
		 * @param name the type name
		 */
		public void setType(String name) {
			type = name;
		}

		/**
		 * Sets the digits component for this column.
		 *
		 * @param num the number of digits
		 */
		public void setDigits(int num) {
			digits = num;
		}

		/**
		 * Sets the scale component for this column.
		 *
		 * @param num the number of scale digits
		 */
		public void setScale(int num) {
			scale = num;
		}

		/**
		 * Sets the (character) width for this column.
		 *
		 * @param num the character width
		 */
		public void setWidth(int num) {
			width = num;
		}
	}
}
