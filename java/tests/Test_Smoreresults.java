/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Smoreresults {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con.getAutoCommit());

		try {
			System.out.print("1. more results?...");
			if (stmt.getMoreResults() != false || stmt.getUpdateCount() != -1)
				throw new SQLException("more results on an unitialised Statement, how can that be?");
			System.out.println(" nope :)");

			System.out.print("2. SELECT 1...");
			if (stmt.execute("SELECT 1;") == false)
				throw new SQLException("SELECT 1 returns update or no results");
			System.out.println(" ResultSet :)");

			System.out.print("3. more results?...");
			if (stmt.getMoreResults() != false || stmt.getUpdateCount() != -1)
				throw new SQLException("more results after SELECT 1 query, how can that be?");
			System.out.println(" nope :)");

			System.out.print("4. even more results?...");
			if (stmt.getMoreResults() != false || stmt.getUpdateCount() != -1)
				throw new SQLException("more results after SELECT 1 query, how can that be?");
			System.out.println(" nope :)");

		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		if (rs != null) rs.close();

		con.close();
	}
}
