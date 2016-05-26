/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
//			dumpResultSet(dbmd.getSchemas());	// this produces different outputs on different platforms due to dependency on SAMTOOLS and NETCDF. so exclude it
			dumpResultSet(dbmd.getSchemas(null, "sys"));
			dumpResultSet(dbmd.getTables(null, "sys", null, null));
			dumpResultSet(dbmd.getUDTs(null, "sys", null, null));
			int[] UDTtypes = { Types.STRUCT, Types.DISTINCT };
			dumpResultSet(dbmd.getUDTs(null, "sys", null, UDTtypes));
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
