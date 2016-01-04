/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Csavepoints {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con.getAutoCommit());

		// savepoints require a non-autocommit connection
		try {
			System.out.print("1. savepoint...");
			con.setSavepoint();
			System.out.println("PASSED :(");
			System.out.println("ABORTING TEST!!!");
			con.close();
			System.exit(-1);
		} catch (SQLException e) {
			System.out.println("failed :) " + e.getMessage());
		}

		con.setAutoCommit(false);
		// >> true: auto commit should be on by default
		System.out.println("0. false\t" + con.getAutoCommit());

		try {
			System.out.print("2. savepoint...");
			/* make a savepoint, and discard it */
			con.setSavepoint();
			System.out.println("passed :)");

			stmt.executeUpdate("CREATE TABLE table_Test_Csavepoints ( id int, PRIMARY KEY (id) )");

			System.out.print("3. savepoint...");
			Savepoint sp2 = con.setSavepoint("empty table");
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			int i = 0;
			int items = 0;
			System.out.print("4. table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (1)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (2)");
			stmt.executeUpdate("INSERT INTO table_Test_Csavepoints VALUES (3)");

			System.out.print("5. savepoint...");
			Savepoint sp3 = con.setSavepoint("three values");
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			System.out.print("6. table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			System.out.print("7. release...");
			con.releaseSavepoint(sp3);
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 3;
			System.out.print("8. table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			System.out.print("9. rollback...");
			con.rollback(sp2);
			System.out.println("passed :)");

			rs = stmt.executeQuery("SELECT id FROM table_Test_Csavepoints");
			i = 0;
			items = 0;
			System.out.print("10. table " + items + " items");
			while (rs.next()) {
				System.out.print(", " + rs.getString("id"));
				i++;
			}
			if (i != items) {
				System.out.println(" FAILED (" + i + ") :(");
				System.out.println("ABORTING TEST!!!");
				con.close();
				System.exit(-1);
			}
			System.out.println(" passed :)");

			con.rollback();
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
