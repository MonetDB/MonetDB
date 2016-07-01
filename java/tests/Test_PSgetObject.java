/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_PSgetObject {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		final Connection con = DriverManager.getConnection(args[0]);
		con.setAutoCommit(false);
		// >> false: auto commit was just switched off
		System.out.println("0. false\t" + con.getAutoCommit());

		final Statement stmt = con.createStatement();
		try {
			System.out.print("1. creating test table...");
			stmt.executeUpdate("CREATE TABLE table_Test_PSgetObject (ti tinyint, si smallint, i int, bi bigint)");
			stmt.close();
			System.out.println(" passed :)");
		} catch (SQLException e) {
			System.out.println(e);
			System.out.println("Creation of test table failed! :(");
			System.out.println("ABORTING TEST!!!");
			System.exit(-1);
		}

		PreparedStatement pstmt;
		try {
			System.out.print("2a. inserting 3 records as batch...");
			pstmt = con.prepareStatement("INSERT INTO table_Test_PSgetObject (ti,si,i,bi) VALUES (?,?,?,?)");
			pstmt.setShort(1, (short)1);
			pstmt.setShort(2, (short)1);
			pstmt.setInt (3, 1);
			pstmt.setLong(4, (long)1);
			pstmt.addBatch();

			pstmt.setShort(1, (short)127);
			pstmt.setShort(2, (short)12700);
			pstmt.setInt (3, 1270000);
			pstmt.setLong(4, (long)127000000);
			pstmt.addBatch();

			pstmt.setShort(1, (short)-127);
			pstmt.setShort(2, (short)-12700);
			pstmt.setInt (3, -1270000);
			pstmt.setLong(4, (long)-127000000);
			pstmt.addBatch();

			pstmt.executeBatch();
			System.out.println(" passed :)");

			System.out.print("2b. closing PreparedStatement...");
			pstmt.close();
			System.out.println(" passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED to INSERT data:( "+ e.getMessage());
			while ((e = e.getNextException()) != null)
				System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		try {
			System.out.print("3a. selecting records...");
			pstmt = con.prepareStatement("SELECT ti,si,i,bi FROM table_Test_PSgetObject ORDER BY ti,si,i,bi");
			ResultSet rs = pstmt.executeQuery();
			System.out.println(" passed :)");

			while (rs.next()) {
				// test fix for https://www.monetdb.org/bugzilla/show_bug.cgi?id=4026
				Short ti = (Short) rs.getObject(1);
				Short si = (Short) rs.getObject(2);
				Integer i = (Integer) rs.getObject(3);
				Long bi = (Long) rs.getObject(4);

				System.out.println("    Retrieved row data: ti=" + ti + " si=" + si + " i=" + i + " bi=" + bi);
			}

			System.out.print("3b. closing ResultSet...");
			rs.close();
			System.out.println(" passed :)");

			System.out.print("3c. closing PreparedStatement...");
			pstmt.close();
			System.out.println(" passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED to RETRIEVE data:( "+ e.getMessage());
			while ((e = e.getNextException()) != null)
				System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		System.out.print("4. Rollback changes...");
		con.rollback();
		System.out.println(" passed :)");

		System.out.print("5. Close connection...");
		con.close();
		System.out.println(" passed :)");
	}
}
