/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class BugConcurrent_clients_SF_1504657 {
	public static void main(String[] args) throws Exception{
		Connection con1 = null, con2 = null, con3 = null;	
		Statement stmt1 = null, stmt2 = null, stmt3 = null;
		ResultSet rs1 = null, rs2= null, rs3 = null;
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		con1 = DriverManager.getConnection(args[0]);
		con2 = DriverManager.getConnection(args[0]);
		con3 = DriverManager.getConnection(args[0]);
		stmt1 = con1.createStatement();
		stmt2 = con2.createStatement();
		stmt3 = con3.createStatement();
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con1.getAutoCommit());
		System.out.println("0. true\t" + con2.getAutoCommit());
		System.out.println("0. true\t" + con3.getAutoCommit());

		// test the creation of a table with concurrent clients
		try {
			System.out.println("1.1. create table t1 using client 1...");
			stmt1.executeUpdate("CREATE TABLE t1 ( id int, name varchar(1024) )");
			System.out.println("passed :)");
			System.out.println("1.2. check table existence in client 2...");
			rs2 = stmt2.executeQuery("SELECT name FROM tables where name LIKE 't1'");
			while (rs2.next())
				System.out.println(rs2.getString("name"));
			System.out.println("passed :)");
			System.out.println("1.3. check table existence in client 3...");
			rs3 = stmt3.executeQuery("SELECT name FROM tables where name LIKE 't1'");
			while (rs3.next())
				System.out.println(rs3.getString("name"));
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED 1 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			con1.close();
			con2.close();
			con3.close();
			System.exit(-1);
		}

		// test the insertion of values with concurrent clients
		try {
			System.out.println("2 insert into t1 using client 1...");
			stmt1.executeUpdate("INSERT INTO t1 values( 1, 'monetdb' )");
			System.out.println("passed :)");
			stmt1.executeUpdate("INSERT INTO t1 values( 2, 'monet' )");
			System.out.println("passed :)");
			stmt1.executeUpdate("INSERT INTO t1 values( 3, 'mon' )");
			System.out.println("passed :)");
			System.out.println("2.1. check table status with client 1...");
			rs1 = stmt1.executeQuery("SELECT * FROM t1");
			while (rs1.next())
				System.out.println(rs1.getInt("id")+", "+ rs1.getString("name"));
			System.out.println("passed :)");
			System.out.println("2.2. check table status with client 2...");
			rs2 = stmt2.executeQuery("SELECT * FROM t1");
			while (rs2.next())
				System.out.println(rs2.getInt("id")+", "+ rs2.getString("name"));
			System.out.println("passed :)");
			System.out.println("2.3. check table status with client 3...");
			rs3 = stmt3.executeQuery("SELECT * FROM t1");
			while (rs3.next())
				System.out.println(rs3.getInt("id")+", "+ rs3.getString("name"));
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED 2 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			if (rs1 != null) rs1.close();
			if (rs2 != null) rs2.close();
			if (rs3 != null) rs3.close();
			con1.close();
			con2.close();
			con3.close();
			System.exit(-1);
		}

		// test the insertion of values with concurrent clients
		try {
			System.out.println("3 insert into t1 using client 2...");
			stmt2.executeUpdate("INSERT INTO t1 values( 4, 'monetdb' )");
			System.out.println("passed :)");
			stmt2.executeUpdate("INSERT INTO t1 values( 5, 'monet' )");
			System.out.println("passed :)");
			stmt2.executeUpdate("INSERT INTO t1 values( 6, 'mon' )");
			System.out.println("passed :)");
			System.out.println("3.1. check table status with client 1...");
			rs1 = stmt1.executeQuery("SELECT * FROM t1");
			while (rs1.next())
				System.out.println(rs1.getInt("id")+", "+ rs1.getString("name"));
			System.out.println("passed :)");
			System.out.println("3.2. check table status with client 2...");
			rs2 = stmt2.executeQuery("SELECT * FROM t1");
			while (rs2.next())
				System.out.println(rs2.getInt("id")+", "+ rs2.getString("name"));
			System.out.println("passed :)");
			System.out.println("3.3. check table status with client 3...");
			rs3 = stmt3.executeQuery("SELECT * FROM t1");
			while (rs3.next())
				System.out.println(rs3.getInt("id")+", "+ rs3.getString("name"));
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED 3 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			if (rs1 != null) rs1.close();
			if (rs2 != null) rs2.close();
			if (rs3 != null) rs3.close();
			con1.close();
			con2.close();
			con3.close();
			System.exit(-1);
		}

		// test the insertion of values with concurrent clients
		try {
			System.out.println("4 insert into t1 using client 3...");
			stmt3.executeUpdate("INSERT INTO t1 values( 7, 'monetdb' )");
			System.out.println("passed :)");
			stmt3.executeUpdate("INSERT INTO t1 values( 8, 'monet' )");
			System.out.println("passed :)");
			stmt3.executeUpdate("INSERT INTO t1 values( 9, 'mon' )");
			System.out.println("passed :)");
			System.out.println("4.1. check table status with client 1...");
			rs1 = stmt1.executeQuery("SELECT * FROM t1");
			while (rs1.next())
				System.out.println(rs1.getInt("id")+", "+ rs1.getString("name"));
			System.out.println("passed :)");
			System.out.println("4.2. check table status with client 2...");
			rs2 = stmt2.executeQuery("SELECT * FROM t1");
			while (rs2.next())
				System.out.println(rs2.getInt("id")+", "+ rs2.getString("name"));
			System.out.println("passed :)");
			System.out.println("4.3. check table status with client 3...");
			rs3 = stmt3.executeQuery("SELECT * FROM t1");
			while (rs3.next())
				System.out.println(rs3.getInt("id")+", "+ rs3.getString("name"));
			System.out.println("passed :)");
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED 4 :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			if (rs1 != null) rs1.close();
			if (rs2 != null) rs2.close();
			if (rs3 != null) rs3.close();
			con1.close();
			con2.close();
			con3.close();
			System.exit(-1);
		}

		if (rs1 != null) rs1.close();
		if (rs2 != null) rs2.close();
		if (rs3 != null) rs3.close();

		// cleanup
		try {
			stmt3.executeUpdate("DROP TABLE t1");
		} catch (SQLException e) {
			System.out.println("FAILED to clean up! :( " + e.getMessage());
		}

		con1.close();
		con2.close();
		con3.close();
	}
}
