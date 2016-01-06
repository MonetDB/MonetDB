/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.util;

import java.io.PrintWriter;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;

public class SQLExporter extends Exporter {
	private int outputMode;
	private Stack<String> lastSchema;

	public final static int TYPE_OUTPUT		= 1;
	public final static int VALUE_INSERT	= 0;
	public final static int VALUE_COPY		= 1;
	public final static int VALUE_TABLE		= 2;

	public SQLExporter(PrintWriter out) {
		super(out);
	}

	/**
	 * A helper method to generate SQL CREATE code for a given table.
	 * This method performs all required lookups to find all relations and
	 * column information, as well as additional indices.
	 *
	 * @param dbmd a DatabaseMetaData object to query on
	 * @param type the type of the object, e.g. VIEW, TABLE
	 * @param catalog the catalog the object is in
	 * @param schema the schema the object is in
	 * @param name the table to describe in SQL CREATE format
	 * @throws SQLException if a database related error occurs
	 */
	public void dumpSchema(
			DatabaseMetaData dbmd,
			String type,
			String catalog,
			String schema,
			String name)
		throws SQLException
	{
		if (useSchema)
			changeSchema(schema);

		// handle views directly
		if (type.indexOf("VIEW") != -1) {
			String[] types = new String[1];
			types[0] = type;
			ResultSet tbl = dbmd.getTables(catalog, schema, name, types);
			if (!tbl.next()) throw new SQLException("Whoops no data for " + name);

			// This will probably only work for MonetDB
			String remarks = tbl.getString("REMARKS");
			if (remarks == null) {
				out.println("-- invalid " + type + " " +
						(!useSchema ? dq(schema) + "." : "") + dq(name) +
						": no definition found");
			} else {
				out.print("CREATE " + type + " " +
					(!useSchema ? dq(schema) + "." : "") + dq(name));
				out.print(" AS ");
				out.println(remarks.replaceFirst("create view [^ ]+ as", "").trim());
			}
			return;
		}

		int i;
		String s;
		out.println("CREATE " + type + " " +
			(!useSchema ? dq(schema) + "." : "") + dq(name) + " (");

		// put all columns with their type in place
		ResultSet cols = dbmd.getColumns(catalog, schema, name, null);
		ResultSetMetaData rsmd = cols.getMetaData();
		int colwidth = rsmd.getColumnDisplaySize(cols.findColumn("COLUMN_NAME"));
		int typewidth = rsmd.getColumnDisplaySize(cols.findColumn("TYPE_NAME"));
		for (i = 0; cols.next(); i++) {
			if (i > 0) out.println(",");
			// print column name
			s = dq(cols.getString("COLUMN_NAME"));
			out.print("\t" + s + repeat(' ', (colwidth - s.length()) + 3));

			s = cols.getString("TYPE_NAME");
			int ctype = cols.getInt("DATA_TYPE");
			int size = cols.getInt("COLUMN_SIZE");
			int digits = cols.getInt("DECIMAL_DIGITS");
			// small hack to get desired behaviour: set digits when we
			// have a time or timestamp with time zone and at the same
			// time masking the internal types
			if (s.equals("timetz")) {
				digits = 1;
				s = "time";
			} else if (s.equals("timestamptz")) {
				digits = 1;
				s = "timestamp";
			}
			// print column type
			out.print(s + repeat(' ', typewidth - s.length()));

			// do some type specifics 
		 	if (ctype == Types.FLOAT ||
				ctype == Types.VARCHAR ||
				ctype == Types.LONGVARCHAR ||
				ctype == Types.CHAR
			) {
				if (size <= 0) throw
					new SQLException("Illegal value for precision of type " + cols.getString("TYPE_NAME") + " (" + size + ")");
		 		out.print("(" + size + ")");
			} else if (ctype == Types.CLOB) {
				if (size > 0) out.print("(" + size + ")");
			} else if (ctype == Types.DECIMAL ||
				ctype == Types.NUMERIC
			) {
				if (digits < 0) throw
					new SQLException("Illegal value for scale of decimal type (" + digits + ")");
		 		out.print("(" + size + "," + digits + ")");
			} else if (ctype == Types.TIMESTAMP ||
				ctype == Types.TIME
			) {
		 		out.print("(" + (size  - 1) + ")");
				if (digits != 0) out.print(" WITH TIME ZONE");
			}
			if (cols.getInt("NULLABLE") == DatabaseMetaData.columnNoNulls)
				out.print("\tNOT NULL");
			if ((s = cols.getString("COLUMN_DEF")) != null)
				out.print("\tDEFAULT " + q(s));
		}
		cols.close();

		// the primary key constraint
		// unfortunately some idiot defined that getPrimaryKeys()
		// returns the primary key columns sorted by column name, not
		// key sequence order.  So we have to sort ourself :(
		cols = dbmd.getPrimaryKeys(
				catalog,
				schema,
				name);
		// first make an 'index' of the KEY_SEQ column
		SortedMap<Integer, Integer> seqIndex = new TreeMap<Integer, Integer>();
		for (i = 1; cols.next(); i++) {
			seqIndex.put(
					Integer.valueOf(cols.getInt("KEY_SEQ")),
					Integer.valueOf(i));
		}
		if (seqIndex.size() > 0) {
			// terminate the previous line
			out.println(",");
			cols.absolute(1);
			out.print("\tCONSTRAINT " + dq(cols.getString("PK_NAME")) +
				" PRIMARY KEY (");
			i = 0;
			for (Iterator<Map.Entry<Integer, Integer>> it = seqIndex.entrySet().iterator();
					it.hasNext(); i++)
			{
				Map.Entry<Integer, Integer> e = it.next();
				cols.absolute(e.getValue().intValue());
				if (i > 0) out.print(", ");
				out.print(dq(cols.getString("COLUMN_NAME")));
			}
			out.print(")");
		}
		cols.close();

		// unique constraints
		cols = dbmd.getIndexInfo(catalog, schema, name, true, true);
		while (cols.next()) {
			String idxname = cols.getString("INDEX_NAME");
			out.println(",");
			out.print("\tCONSTRAINT " + dq(idxname) + " UNIQUE (" +
				dq(cols.getString("COLUMN_NAME")));

			boolean next;
			while ((next = cols.next()) && idxname != null &&
				idxname.equals(cols.getString("INDEX_NAME"))) {
				out.print(", " + dq(cols.getString("COLUMN_NAME")));
			}
			// go back one, we've gone one too far
			if (next) cols.previous();

			out.print(")");
		}
		cols.close();

		// foreign keys
		cols = dbmd.getImportedKeys(catalog, schema, name);
		while (cols.next()) {
			out.println(",");
			out.print("\tCONSTRAINT " + dq(cols.getString("FK_NAME")) + " FOREIGN KEY (");

			boolean next;
			Set<String> fk = new LinkedHashSet<String>();
			fk.add(cols.getString("FKCOLUMN_NAME").intern());
			Set<String> pk = new LinkedHashSet<String>();
			pk.add(cols.getString("PKCOLUMN_NAME").intern());

			while ((next = cols.next()) &&
				cols.getInt("KEY_SEQ") != 0)
			{
				fk.add(cols.getString("FKCOLUMN_NAME").intern());
				pk.add(cols.getString("PKCOLUMN_NAME").intern());
			}
			// go back one
			if (next) cols.previous();

			Iterator<String> it = fk.iterator();
			for (i = 0; it.hasNext(); i++) {
				if (i > 0) out.print(", ");
				out.print(dq((String)it.next()));
			}
			out.print(") ");

			out.print("REFERENCES " + dq(cols.getString("PKTABLE_SCHEM")) +
				"." + dq(cols.getString("PKTABLE_NAME")) + " (");
			it = pk.iterator();
			for (i = 0; it.hasNext(); i++) {
				if (i > 0) out.print(", ");
				out.print(dq((String)it.next()));
			}
		 	out.print(")");
		}
		cols.close();
		out.println();
		// end the create statement
		out.println(");");

		// create indexes
		cols = dbmd.getIndexInfo(catalog, schema, name, false, true);
		while (cols.next()) {
			if (!cols.getBoolean("NON_UNIQUE")) {
				// we already covered this one as UNIQUE
				continue;
			} else {
				String idxname = cols.getString("INDEX_NAME");
				out.print("CREATE INDEX " + dq(idxname) + " ON " +
					dq(cols.getString("TABLE_NAME")) + " (" +
					dq(cols.getString("COLUMN_NAME")));

				boolean next;
				while ((next = cols.next()) && idxname != null &&
					idxname.equals(cols.getString("INDEX_NAME")))
				{
					out.print(", " + dq(cols.getString("COLUMN_NAME")));
				}
				// go back one
				if (next) cols.previous();

				out.println(");");
			}
		}
		cols.close();
	}

	/**
	 * Dumps the given ResultSet as specified in the form variable.
	 *
	 * @param rs the ResultSet to dump
	 * @throws SQLException if a database error occurs
	 */
	public void dumpResultSet(ResultSet rs) throws SQLException {
		switch (outputMode) {
			case VALUE_INSERT:
				resultSetToSQL(rs);
			break;
			case VALUE_COPY:
				resultSetToSQLDump(rs);
			break;
			case VALUE_TABLE:
				resultSetToTable(rs);
			break;
		}
	}

	public void setProperty(int type, int value) throws Exception {
		switch (type) {
			case TYPE_OUTPUT:
				switch (value) {
					case VALUE_INSERT:
					case VALUE_COPY:
					case VALUE_TABLE:
						outputMode = value;
					break;
					default:
						throw new Exception("Illegal value " + value + " for TYPE_OUTPUT");
				}
			break;
			default:
				throw new Exception("Illegal type " + type);
		}
	}

	public int getProperty(int type) throws Exception {
		switch (type) {
			case TYPE_OUTPUT:
				return outputMode;
			default:
				throw new Exception("Illegal type " + type);
		}
	}

	private final static int AS_IS = 0;
	private final static int QUOTE = 1;

	/**
	 * Helper method to dump the contents of a table in SQL INSERT INTO
	 * format.
	 *
	 * @param rs the ResultSet to convert into INSERT INTO statements
	 * @param absolute if true, dumps table name prepended with schema name 
	 * @throws SQLException if a database related error occurs
	 */
	private void resultSetToSQL(ResultSet rs)
		throws SQLException
	{
		ResultSetMetaData rsmd = rs.getMetaData();
		String statement = "INSERT INTO ";
		if (!useSchema) statement += dq(rsmd.getSchemaName(1)) + ".";
		statement += dq(rsmd.getTableName(1)) + " VALUES (";

		int cols = rsmd.getColumnCount();
		int[] types = new int[cols];
		for (int i = 0; i < cols; i++) {
			switch (rsmd.getColumnType(i + 1)) {
				case Types.CHAR:
				case Types.VARCHAR:
				case Types.LONGVARCHAR:
				case Types.CLOB:
				case Types.DATE:
				case Types.TIME:
				case Types.TIMESTAMP:
					types[i] = QUOTE;
				break;
				case Types.NUMERIC:
				case Types.DECIMAL:
				case Types.BIT: // we don't use type BIT, it's here for completeness
				case Types.BOOLEAN:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
				case Types.BIGINT:
				case Types.REAL:
				case Types.FLOAT:
				case Types.DOUBLE:
					types[i] = AS_IS;
				break;

				default:
					types[i] = AS_IS;
			}
		}

		while (rs.next()) {
			out.print(statement);
			for (int i = 1; i <= cols; i++) {
				if (i > 1) out.print(", ");
				if (rs.getString(i) == null) {
					out.print("NULL");
					continue;
				}
				switch (types[i - 1]) {
					case AS_IS:
						out.print(rs.getString(i));
					break;
					case QUOTE:
						out.print(q(rs.getString(i)));
					break;
				}
			}
			out.println(");");
		}
	}

	public void resultSetToSQLDump(ResultSet rs) {
		// write copy into statement
	}

	/**
	 * Helper method to write a ResultSet in a convenient table format
	 * to the output writer.
	 *
	 * @param rs the ResultSet to write out
	 */
	public void resultSetToTable(ResultSet rs) throws SQLException {
		ResultSetMetaData md = rs.getMetaData();
		// find the widths of the columns
		int[] width = new int[md.getColumnCount()];
		for (int j = 0; j < md.getColumnCount(); j++) {
			if (md.getColumnDisplaySize(j + 1) == 0) {
				width[j] = md.getColumnLabel(j + 1).length();
			} else {
				width[j] = Math.max(md.getColumnDisplaySize(j + 1), md.getColumnLabel(j + 1).length());
			}
			if (md.isNullable(j + 1) != ResultSetMetaData.columnNoNulls) {
				width[j] = Math.max("<NULL>".length(), width[j]);
			}
		}

		out.print("+");
		for (int j = 0; j < width.length; j++)
			out.print("-" + repeat('-', width[j]) + "-+");
		out.println();

		out.print("|");
		for (int j = 0; j < width.length; j++) {
			out.print(" " + md.getColumnLabel(j + 1) + repeat(' ', width[j] - md.getColumnLabel(j + 1).length()) +  " |");
		}
		out.println();

		out.print("+");
		for (int j = 0; j < width.length; j++)
			out.print("=" + repeat('=', width[j]) + "=+");
		out.println();

		int count = 0;
		for (; rs.next(); count++) {
			out.print("|");
			for (int j = 0; j < width.length; j++) {
				Object rdata = rs.getObject(j + 1);
				String data;
				if (rdata == null) {
					data = "<NULL>";
				} else {
					data = rdata.toString();
				}
				if (md.isSigned(j + 1)) {
					// we have a numeric type here
					out.print(" " + repeat(' ', Math.max(width[j] - data.length(), 0)) + data +  " |");
				} else {
					// something else
					out.print(" " + data + repeat(' ', Math.max(width[j] - data.length(), 0)) +  " |");
				}
			}
			out.println();
		}

		out.print("+");
		for (int j = 0; j < width.length; j++)
			out.print("-" + repeat('-', width[j]) + "-+");
		out.println();

		out.println(count + " row" + (count != 1 ? "s" : ""));
	}

	private void changeSchema(String schema) {
		if (lastSchema == null) {
			lastSchema = new Stack<String>();
			lastSchema.push(null);
		}

		if (!schema.equals(lastSchema.peek())) {
			if (!lastSchema.contains(schema)) {
				// create schema
				out.print("CREATE SCHEMA ");
				out.print(dq(schema));
				out.println(";\n");
				lastSchema.push(schema);
			}
		
			out.print("SET SCHEMA ");
			out.print(dq(schema));
			out.println(";\n");
		}
	}
}
