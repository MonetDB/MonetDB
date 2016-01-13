/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class BugConcurrent_sequences {
	public static void main(String[] args) throws Exception {
		Connection con1 = null, con2 = null;
		Statement stmt1 = null, stmt2 = null;
		ResultSet rs1 = null, rs2 = null;
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		con1 = DriverManager.getConnection(args[0]);
		con2 = DriverManager.getConnection(args[0]);
		stmt1 = con1.createStatement();
		stmt2 = con2.createStatement();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con1.getAutoCommit());
		System.out.println("0. true\t" + con2.getAutoCommit());

		// create a table
		try {
			System.out.print("1. create table t1 using client 1... ");
			stmt1.executeUpdate("CREATE TABLE t1 ( id serial, who varchar(12) )");
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (that sux)
			System.out.println("FAILED 1 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			con1.close();
			con2.close();
			System.exit(-1);
		}

		// test the insertion of values with concurrent clients
		try {
			System.out.print("2. insert into t1 using client 1 and 2... ");
			stmt1.executeUpdate("INSERT INTO t1(who) VALUES('client1')");
			System.out.println("client 1 passed :)");
			con2.setAutoCommit(false);
			stmt2.executeUpdate("INSERT INTO t1(who) VALUES('client2')");
			System.out.println("transaction on client 2 :)");
			stmt1.executeUpdate("INSERT INTO t1(who) VALUES('client1')");
			System.out.println("client 1 passed :)");
			try {
				con2.commit();
				System.out.println("transaction client 2 PASSED :(");
				System.exit(-1);
			} catch (SQLException e) {
				System.out.println("transaction client 2 failed :)");
			}
			con2.setAutoCommit(true);
			stmt2.executeUpdate("INSERT INTO t1(who) VALUES('client2')");
			System.out.println("passed :)");
			System.out.println("2.1. check table status with client 1...");
			rs1 = stmt1.executeQuery("SELECT * FROM t1");
			while (rs1.next())
				System.out.println(rs1.getInt("id") + ", " +
						rs1.getString("who"));
			System.out.println("passed :)");
			System.out.println("2.2. check table status with client 2...");
			rs2 = stmt2.executeQuery("SELECT * FROM t1");
			while (rs2.next())
				System.out.println(rs2.getInt("id") + ", " +
						rs2.getString("who"));
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED 2 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			if (rs1 != null) rs1.close();
			if (rs2 != null) rs2.close();
			con1.close();
			con2.close();
			System.exit(-1);
		}

		// drop the table (not dropping the sequence) from client 1
		try {
			System.out.print("3.1. drop table t1 using client 1... ");
			stmt1.executeUpdate("DROP TABLE t1");
			System.out.println("passed :)");
			System.out.print("3.1. recreate t1 using client 1... ");
			stmt1.executeUpdate("CREATE TABLE t1 ( id serial, who varchar(12) )");
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED 3 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			if (rs1 != null) rs1.close();
			if (rs2 != null) rs2.close();
			con1.close();
			con2.close();
			System.exit(-1);
		}

		// re-establish connection
		try {
			System.out.print("x. Reconnecting client 1 and 2... ");
			con1.close();
			con2.close();
			con1 = DriverManager.getConnection(args[0]);
			con2 = DriverManager.getConnection(args[0]);
			stmt1 = con1.createStatement();
			stmt2 = con2.createStatement();
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED x :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			if (rs1 != null) rs1.close();
			if (rs2 != null) rs2.close();
			con1.close();
			con2.close();
			System.exit(-1);
		}

		// insert and print, should get 1,2
		try {
			System.out.println("4. insert into t1 using client 1 and 2...");
			stmt1.executeUpdate("INSERT INTO t1(who) VALUES('client1')");
			System.out.println("passed :)");
			con2.setAutoCommit(false);
			stmt2.executeUpdate("INSERT INTO t1(who) VALUES('client2')");
			con2.commit();
			con2.setAutoCommit(true);
			System.out.println("passed :)");
			System.out.println("4.1. check table status with client 1...");
			rs1 = stmt1.executeQuery("SELECT * FROM t1 ORDER BY who");
			for (int cntr = 1; rs1.next(); cntr++) {
				int id = rs1.getInt("id");
				System.out.println(id + ", " +
						rs1.getString("who"));
				if (id != cntr)
					throw new SQLException("expected " + cntr + ", got " + id);
			}
			System.out.println("passed :)");
			System.out.println("4.2. check table status with client 2...");
			rs2 = stmt2.executeQuery("SELECT * FROM t1 ORDER BY who");
			for (int cntr = 1; rs2.next(); cntr++) {
				int id = rs2.getInt("id");
				System.out.println(id + ", " +
						rs2.getString("who"));
				if (id != cntr)
					throw new SQLException("expected " + cntr + ", got " + id);
			}
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED 4 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			if (rs1 != null) rs1.close();
			if (rs2 != null) rs2.close();
			con1.close();
			con2.close();
			System.exit(-1);
		}

		if (rs1 != null) rs1.close();
		if (rs2 != null) rs2.close();

		// cleanup
		try {
			stmt1.executeUpdate("DROP TABLE t1");
		} catch (SQLException e) {
			System.out.println("FAILED to clean up! :( " + e.getMessage());
		}

		con1.close();
		con2.close();
	}
}
