/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

import java.sql.*;




public class Test_Remote_Location {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection demo_con = DriverManager.getConnection("jdbc:monetdb://localhost:50000/demo", "monetdb", "monetdb");
		Statement demo = demo_con.createStatement();
		ResultSet rs = null;

		try {
			System.out.println("1. create test...");
			demo.executeUpdate("create table t1 (id int, name varchar(1024));");
			demo.executeUpdate("create table t1@demo (id int, name varchar(1024));");
			demo.executeUpdate("create table t1@test1 (id int, name varchar(1024));");
			demo.executeUpdate("create table t1@test2 (id int, name varchar(1024));");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("2. Insert test...");
			demo.executeUpdate("insert into t1 values(1,'romulo');");
			demo.executeUpdate("insert into t1@demo values(1,'fabian');");
			demo.executeUpdate("insert into t1@demo values(2,'martin');");
			demo.executeUpdate("insert into t1@test1 values(1,'romulo');");
			demo.executeUpdate("insert into t1@test3 values(1,'romulo');");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("3. select test...");
			rs = demo.executeQuery("select * from t1;");
			rs = demo.executeQuery("select * from t1@demo where id >0;");
			rs = demo.executeQuery("select * from t1@test4 where id >0;");
			rs = demo.executeQuery("select * from t2@test1 where id >0;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}

		try {
			System.out.println("4. delete test...");
			demo.executeUpdate("delete from t1@demo where id>1;");
			demo.executeUpdate("delete from t1@test2 where id=0;");
			demo.executeUpdate("delete from t3@test1 where id >0;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("5. update test...");
			demo.executeUpdate("update t1@test1 set name='niels' where id=0;");
			demo.executeUpdate("update t1@test2 set name='martin' where id=1;");
			demo.executeUpdate("update t1@test1 set name='martin' where id=4;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("6. alter test...");
			demo.executeUpdate("alter table t1 add place varchar(1024);");
			demo.executeUpdate("alter table t1@demo drop place ;");
			demo.executeUpdate("alter table t1@test1 drop place ;");
			demo.executeUpdate("alter table t1@test2 add place varchar(1024);");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("7. drop test...");
			demo.executeUpdate("drop table t1@test1;");
			demo.executeUpdate("drop table t2@test1;");
			demo.executeUpdate("drop table t1@test2;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("8. db state...");
			rs = demo.executeQuery("select * from t1;");
			rs = demo.executeQuery("select * from t1@test1;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("2. Clean DB...");
			demo.executeUpdate("drop table t1;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			System.exit(-1);
		}
		
		if (rs != null) rs.close();

		demo_con.close();
	}
}
