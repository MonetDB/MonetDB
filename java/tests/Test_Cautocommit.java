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
