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
