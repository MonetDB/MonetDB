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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
 */

import java.sql.*;
import java.util.*;

public class Test_Dobjects {
	private static void dumpResultSet(ResultSet rs) throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		System.out.println("Resultset with " + rsmd.getColumnCount() + " columns");
		for (int col = 1; col <= rsmd.getColumnCount(); col++) {
			System.out.print(rsmd.getColumnName(col) + "\t");
		}
		System.out.println();
		while (rs.next()) {
			for (int col = 1; col <= rsmd.getColumnCount(); col++) {
				System.out.print(rs.getString(col) + "\t");
			}
			System.out.println();
		}
	}

	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;
		DatabaseMetaData dbmd = con.getMetaData();

		try {
			// inspect the catalog by use of dbmd functions
			dumpResultSet(dbmd.getCatalogs());
			dumpResultSet(dbmd.getSchemas());
			dumpResultSet(dbmd.getSchemas(null, "sys"));
			dumpResultSet(dbmd.getTables(null, null, null, null));
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
