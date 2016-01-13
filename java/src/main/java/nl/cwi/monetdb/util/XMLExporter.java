/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

package nl.cwi.monetdb.util;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.*;

public class XMLExporter extends Exporter {
	private boolean useNil;

	public final static int TYPE_NIL	= 1;
	public final static int VALUE_OMIT	= 0;
	public final static int VALUE_XSI	= 1;

	public XMLExporter(PrintWriter out) {
		super(out);
	}

	public void dumpSchema(
			DatabaseMetaData dbmd,
			String type,
			String catalog,
			String schema,
			String name)
		throws SQLException
	{
		if (type.indexOf("VIEW") != -1) {
			String[] types = new String[1];
			types[0] = type;
			ResultSet tbl = dbmd.getTables(catalog, schema, name, types);
			if (!tbl.next()) throw new SQLException("Whoops no data for " + name);

			// This will probably only work for MonetDB
			out.print("<!-- unable to represent: CREATE " + type + " " +
				(!useSchema ? dq(schema) + "." : "") + dq(name));
			out.print(" AS ");
		 	out.print(tbl.getString("REMARKS").trim());
			out.print(" -->");
			return;
		}

		out.println("<xsd:schema>");

		ResultSet cols = dbmd.getColumns(catalog, schema, name, null);
		String ident;
		Set<String> types = new HashSet<String>();
		// walk through the ResultSet and create the types
		// for a bit of a clue on the types, see this url:
		// http://books.xmlschemata.org/relaxng/relax-CHP-19.html
		while (cols.next()) {
			switch (cols.getInt("DATA_TYPE")) {
				case java.sql.Types.CHAR:
					ident = "CHAR_" + cols.getString("COLUMN_SIZE");
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:string\">");
					out.print("      <xsd:length value=");
					out.print(dq(cols.getString("COLUMN_SIZE")));
					out.println(" />");
					out.println("    </xsd:restriction>");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.VARCHAR:
				case java.sql.Types.LONGVARCHAR:
					ident = "VARCHAR_" + cols.getString("COLUMN_SIZE");
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:string\">");
					out.print("      <xsd:maxLength value=");
					out.print(dq(cols.getString("COLUMN_SIZE")));
					out.println(" />");
					out.println("    </xsd:restriction>");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.CLOB:
					ident = "CLOB";
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:string\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.DECIMAL:
				case java.sql.Types.NUMERIC:
					ident = "DECIMAL_" + cols.getString("COLUMN_SIZE") +
						"_" + cols.getString("DECIMAL_DIGITS");
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:decimal\">");
					out.print("      <xsd:totalDigits value=");
					out.print(dq(cols.getString("COLUMN_SIZE")));
					out.println(" />");
					out.print("      <xsd:fractionDigits value=");
					out.print(dq(cols.getString("DECIMAL_DIGITS")));
					out.println(" />");
					out.println("    </xsd:restriction>");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.TINYINT:
					ident = "TINYINT";
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:byte\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.SMALLINT:
					ident = "SMALLINT";
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:short\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.INTEGER:
					ident = "INTEGER";
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:integer\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.BIGINT:
					ident = "BIGINT";
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:long\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.BIT:
				case java.sql.Types.BOOLEAN:
					ident = "BOOLEAN";
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:boolean\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.DATE:
					ident = "DATE";
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:date\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.TIME:
					if ("timetz".equals(cols.getString("TYPE_NAME"))) {
						ident = "TIME_WTZ";
					} else {
						ident = "TIME";
					}
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:time\" />");
					out.println("  </xsd:simpleType>");
				break;
				case java.sql.Types.TIMESTAMP:
					if ("timestamptz".equals(cols.getString("TYPE_NAME"))) {
						ident = "TIMESTAMP_WTZ";
					} else {
						ident = "TIMESTAMP";
					}
					if (types.contains(ident)) break;
					types.add(ident);

					out.print("  <xsd:simpleType name=");
					out.print(dq(ident));
					out.println(">");
					out.println("    <xsd:restriction base=\"xsd:dateTime\" />");
					out.println("  </xsd:simpleType>");
				break;
			}
		}

		// rewind the ResultSet
		cols.beforeFirst();

		// create the RowType
		out.print("  <xsd:complexType name=");
		out.print(dq("RowType." + catalog.replaceAll("\\.", "_x002e_") +
					"." + schema.replaceAll("\\.", "_x002e_") +
					"." + name.replaceAll("\\.", "_x002e_")));
		out.println(">");
		out.println("    <xsd:sequence>");
		while (cols.next()) {
			out.print("      <xsd:element name=");
			out.print(dq(cols.getString("COLUMN_NAME")));
			out.print(" type=");
			switch (cols.getInt("DATA_TYPE")) {
				case java.sql.Types.CHAR:
					ident = "CHAR_" + cols.getString("COLUMN_SIZE");
				break;
				case java.sql.Types.VARCHAR:
				case java.sql.Types.LONGVARCHAR:
					ident = "VARCHAR_" + cols.getString("COLUMN_SIZE");
				break;
				case java.sql.Types.CLOB:
					ident = "CLOB";
				break;
				case java.sql.Types.DECIMAL:
				case java.sql.Types.NUMERIC:
					ident = "DECIMAL_" + cols.getString("COLUMN_SIZE") +
						"_" + cols.getString("DECIMAL_DIGITS");
				break;
				case java.sql.Types.TINYINT:
					ident = "TINYINT";
				break;
				case java.sql.Types.SMALLINT:
					ident = "SMALLINT";
				break;
				case java.sql.Types.INTEGER:
					ident = "INTEGER";
				break;
				case java.sql.Types.BIGINT:
					ident = "BIGINT";
				break;
				case java.sql.Types.BIT:
				case java.sql.Types.BOOLEAN:
					ident = "BOOLEAN";
				break;
				case java.sql.Types.DATE:
					ident = "DATE";
				break;
				case java.sql.Types.TIME:
					if ("timetz".equals(cols.getString("TYPE_NAME"))) {
						ident = "TIME_WTZ";
					} else {
						ident = "TIME";
					}
				break;
				case java.sql.Types.TIMESTAMP:
					if ("timestamptz".equals(cols.getString("TYPE_NAME"))) {
						ident = "TIMESTAMP_WTZ";
					} else {
						ident = "TIMESTAMP";
					}
				break;
				default:
					ident = "(unknown)";
				break;
			}
			out.print(dq(ident));
			out.println(" />");
		}
		out.println("    </xsd:sequence>");
		out.println("  </xsd:complexType>");

		out.print("  <xsd:complexType name=");
		out.print(dq("TableType." + catalog.replaceAll("\\.", "_x002e_") +
					"." + schema.replaceAll("\\.", "_x002e_") +
					"." + name.replaceAll("\\.", "_x002e_")));
		out.println(">");
		out.println("    <xsd:sequence>");
		out.print("      <xsd:element name=\"row\" type=");
		out.print(dq("RowType." + catalog.replaceAll("\\.", "_x002e_") +
					"." + schema.replaceAll("\\.", "_x002e_") +
					"." + name.replaceAll("\\.", "_x002e_")));
		out.println(" minOccurs=\"0\" maxOccurs=\"unbounded\" />");
		out.println("    </xsd:sequence>");
		out.println("  </xsd:complexType>");

		out.println("</xsd:schema>");
	}

	private final static SimpleDateFormat xsd_ts =
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
	private final static SimpleDateFormat xsd_tstz =
		new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");

	/**
	 * Generates an XML representation of the given ResultSet.
	 *
	 * @param rs the ResultSet
	 */
	public void dumpResultSet(ResultSet rs) throws SQLException {
		// write simple XML serialisation
		ResultSetMetaData rsmd = rs.getMetaData();
		if (!useSchema) out.println("<" + rsmd.getSchemaName(1) + ">");
		out.println("<" + rsmd.getTableName(1) + ">");
		String data;
		while (rs.next()) {
			out.println("  <row>");
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {
				switch (rsmd.getColumnType(i)) {
					case java.sql.Types.TIMESTAMP:
						Timestamp ts = rs.getTimestamp(i);
						if ("timestamptz".equals(rsmd.getColumnTypeName(i))) {
							data = xsd_tstz.format(ts).toString();
						} else {
							data = xsd_ts.format(ts).toString();
						}
					break;
					default:
						data = rs.getString(i);
					break;
				}
				if (data == null) {
					if (useNil) {
						// "nil" method: write <tag xsi:nil="true" />
						out.print("    ");
						out.print("<" + rsmd.getColumnLabel(i));
						out.println(" xsi:nil=\"true\" />");
					} else {
						// This is the "absent" method (of completely
						// hiding the tag if null
					}
				} else {
					out.print("    ");
					out.print("<" + rsmd.getColumnLabel(i));
					if (data.length() == 0) {
						out.println(" />");
					} else {
						out.print(">" + data.replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;"));
						out.println("</" + rsmd.getColumnLabel(i) + ">");
					}
				}
			}
			out.println("  </row>");
		}
		out.println("</" + rsmd.getTableName(1) + ">");
		if (!useSchema) out.println("</" + rsmd.getSchemaName(1) + ">");
	}

	public void setProperty(int type, int value) throws Exception {
		switch (type) {
			case TYPE_NIL:
				switch (value) {
					case VALUE_OMIT:
						useNil = false;
					break;
					case VALUE_XSI:
						useNil = true;
					break;
					default:
						throw new Exception("Illegal value " + value + " for TYPE_NIL");
				}
			break;
			default:
				throw new Exception("Illegal type " + type);
		}
	}

	public int getProperty(int type) throws Exception {
		switch (type) {
			case TYPE_NIL:
				return useNil ? VALUE_XSI : VALUE_OMIT;
			default:
				throw new Exception("Illegal type " + type);
		}
	}
}
