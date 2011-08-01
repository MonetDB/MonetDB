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
