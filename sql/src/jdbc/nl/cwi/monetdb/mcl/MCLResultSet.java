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
	
	/**
	 * Creates a new MCLResultSet that is in intially empty.  An empty
	 * MCLResultSet is not valid when being supplied to an MCLServer.
	 */
	public MCLResultSet() {
		isValid = false;
		columns = new ArrayList();
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
				ctype = 'b';
				type = "boolean";
				width = 5; // "false"
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
