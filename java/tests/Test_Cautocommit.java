/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Cautocommit {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con1 = DriverManager.getConnection(args[0]);
		Connection con2 = DriverManager.getConnection(args[0]);
		Statement stmt1 = con1.createStatement();
		Statement stmt2 = con2.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con1.getAutoCommit());
		System.out.println("0. true\t" + con2.getAutoCommit());

		// test commit by checking if a change is visible in another connection
		try {
			System.out.print("1. create...");
			stmt1.executeUpdate("CREATE TABLE table_Test_Cautocommit ( id int )");
			System.out.println("passed :)");
			System.out.print("2. select...");
			rs = stmt2.executeQuery("SELECT * FROM table_Test_Cautocommit");
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			con1.close();
			con2.close();
			System.exit(-1);
		}

		// turn off auto commit
		con1.setAutoCommit(false);
		con2.setAutoCommit(false);

		// >> false: we just disabled it
		System.out.println("3. false\t" + con1.getAutoCommit());
		System.out.println("4. false\t" + con2.getAutoCommit());

		// a change would not be visible now
		try {
			System.out.print("5. drop...");
			stmt2.executeUpdate("DROP TABLE table_Test_Cautocommit");
			System.out.println("passed :)");
			System.out.print("6. select...");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Cautocommit");
			System.out.println("passed :)");
			System.out.print("7. commit...");
			con2.commit();
			System.out.println("passed :)");
			System.out.print("8. select...");
			rs = stmt1.executeQuery("SELECT * FROM table_Test_Cautocommit");
			System.out.println("passed :)");
			System.out.print("9. commit...");
			con1.commit();
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :(");
			System.out.println("ABORTING TEST!!!");
		}

		if (rs != null) rs.close();

		con1.close();
		con2.close();
	}
}
