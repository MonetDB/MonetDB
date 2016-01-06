/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Creplysize {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con1 = DriverManager.getConnection(args[0]);
		Statement stmt1 = con1.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		con1.setAutoCommit(false);
		// >> true: auto commit should be off by now
		System.out.println("0. true\t" + con1.getAutoCommit());

		// test commit by checking if a change is visible in another connection
		try {
			System.out.print("1. create... ");
			stmt1.executeUpdate("CREATE TABLE table_Test_Creplysize ( id int )");
			System.out.println("passed :)");

			System.out.print("2. populating with 21 records... ");
			for (int i = 0; i < 21; i++)
				stmt1.executeUpdate("INSERT INTO table_Test_Creplysize (id) values (" + (i + 1) + ")");
			System.out.println("passed :)");

			System.out.print("3. hinting the driver to use fetchsize 10... ");
			stmt1.setFetchSize(10);
			System.out.println("passed :)");

			System.out.print("4. selecting all values... ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			int i = 0;
			while (rs.next()) i++;
			rs.close();
			if (i == 21) {
				System.out.println("passed :)");
			} else {
				throw new SQLException("got " + i + " records!!!");
			}

			System.out.print("5. resetting driver fetchsize hint... ");
			stmt1.setFetchSize(0);
			System.out.println("passed :)");

			System.out.print("6. instructing the driver to return at max 10 rows...  ");
			stmt1.setMaxRows(10);
			System.out.println("passed :)");

			System.out.print("7. selecting all values...  ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			i = 0;
			while (rs.next()) i++;
			rs.close();
			if (i == 10) {
				System.out.println("passed :)");
			} else {
				throw new SQLException("got " + i + " records!!!");
			}

			System.out.print("8. hinting the driver to use fetchsize 5... ");
			stmt1.setFetchSize(5);
			System.out.println("passed :)");

			System.out.print("9. selecting all values... ");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Creplysize");
			i = 0;
			while (rs.next()) i++;
			rs.close();
			if (i == 10) {
				System.out.println("passed :)");
			} else {
				throw new SQLException("got " + i + " records!!!");
			}
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			con1.close();
			System.exit(-1);
		}

		con1.close();
	}
}
