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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
