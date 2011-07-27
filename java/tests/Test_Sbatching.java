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

public class Test_Sbatching {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con.getAutoCommit());

		try {
			System.out.print("1. create...");
			if (stmt.executeUpdate("CREATE TABLE table_Test_Sbatching ( id int )") != Statement.SUCCESS_NO_INFO)
				throw new SQLException("Wrong return status");
			System.out.println("passed :)");

			// start batching a large amount of inserts
			for (int i = 1; i <= 30000; i++) {
				stmt.addBatch("INSERT INTO table_Test_Sbatching VALUES (" + i + ")");
				if (i % 400 == 0) {
					System.out.print("2. executing batch (400 inserts)...");
					int[] cnts = stmt.executeBatch();
					System.out.println("passed :)");
					System.out.print("3. checking number of update counts...");
					if (cnts.length != 400) throw new SQLException("Invalid size: " + cnts.length);
					System.out.println(cnts.length + " passed :)");
					System.out.print("4. checking update counts (should all be 1)...");
					for (int j = 0; j < cnts.length; j++) {
						if (cnts[j] != 1) throw new SQLException("Unexpected value: " + cnts[j]);
					}
					System.out.println("passed :)");
					System.out.print("5. clearing the batch...");
					stmt.clearBatch();
					System.out.println("passed :)");
				}
			}
			System.out.print("6. executing batch...");
			stmt.executeBatch();
			System.out.println("passed :)");

			System.out.print("7. checking table count...");
			rs = stmt.executeQuery("SELECT COUNT(*) FROM table_Test_Sbatching");
			rs.next();
			System.out.println(rs.getInt(1) + " passed :)");

			System.out.print("8. clean up mess we made...");
			if (stmt.executeUpdate("DROP TABLE table_Test_Sbatching") != Statement.SUCCESS_NO_INFO)
				throw new SQLException("Wrong return status");
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		if (rs != null) rs.close();

		con.close();
	}
}
