/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Ctransaction {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con1 = DriverManager.getConnection(args[0]);
		Statement stmt1 = con1.createStatement();
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con1.getAutoCommit());

		// test commit by checking if a change is visible in another connection
		try {
			System.out.print("1. commit...");
			con1.commit();
			System.out.println("PASSED :(");
			con1.close();
			System.exit(-1);
		} catch (SQLException e) {
			// this means we get what we expect
			System.out.println("failed :) " + e.getMessage());
		}

		// turn off auto commit
		con1.setAutoCommit(false);

		// >> false: we just disabled it
		System.out.println("2. false\t" + con1.getAutoCommit());

		// a change would not be visible now
		try {
			System.out.print("3. commit...");
			con1.commit();
			System.out.println("passed :)");
			System.out.print("4. commit...");
			con1.commit();
			System.out.println("passed :)");
			System.out.print("5. rollback...");
			con1.rollback();
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			con1.close();
			System.exit(-1);
		}

		// turn off auto commit
		con1.setAutoCommit(true);

		// >> false: we just disabled it
		System.out.println("6. true\t" + con1.getAutoCommit());

		try {
			System.out.print("7. start transaction...");
			stmt1.executeUpdate("START TRANSACTION");
			System.out.println("passed :)");
			System.out.print("8. commit...");
			con1.commit();
			System.out.println("passed :)");
			System.out.println("9. true\t" + con1.getAutoCommit());
			System.out.print("10. start transaction...");
			stmt1.executeUpdate("START TRANSACTION");
			System.out.println("passed :)");
			System.out.print("11. rollback...");
			con1.rollback();
			System.out.println("passed :)");
			System.out.println("12. true\t" + con1.getAutoCommit());
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :(");
			System.out.println("ABORTING TEST!!!");
		}

		try {
			// a commit now should fail
			System.out.print("13. commit...");
			con1.commit();
			System.out.println("PASSED :(");
		} catch (SQLException e) {
			// this means we get what we expect
			System.out.println("failed :) " + e.getMessage());
		}

		con1.close();
	}
}
