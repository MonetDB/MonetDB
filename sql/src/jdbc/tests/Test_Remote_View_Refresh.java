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




public class Test_Remote_View_Refresh {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection demo_con = DriverManager.getConnection("jdbc:monetdb://localhost:50000/demo", "monetdb", "monetdb");
		Connection test1_con = DriverManager.getConnection("jdbc:monetdb://localhost:40000/test1", "monetdb", "monetdb");
		Statement demo = demo_con.createStatement();
		Statement test1 = test1_con.createStatement();
		ResultSet rs = null;

		try {
			System.out.println("1. load catalog information (demo)...");
			demo.executeUpdate("CREATE TABLE t1@demo (id int,name varchar(1024));");
			demo.executeUpdate("CREATE TABLE t1@test1 (id int,name varchar(1024), age int);");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("2. load catalog information (test1)...");
			test1.executeUpdate("CREATE TABLE t1@demo (id int,name varchar(1024));");
			test1.executeUpdate("CREATE TABLE t1@test1 (id int,name varchar(1024), age int);");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("3. create views...");
			System.out.println("Materialized/PUSH...");
			test1.executeUpdate("CREATE VIEW view1 as select * from t1@demo with data using push;");
			System.out.println("PULL...");
			test1.executeUpdate("CREATE VIEW view2 as select * from t1@demo using pull;");
			System.out.println("PUSH...");
			test1.executeUpdate("CREATE VIEW view3 as select id from t1@demo using push R_TIME 13;");
			System.out.println("Materialized/PULL...");
			test1.executeUpdate("CREATE VIEW view4 as select id from t1@demo with data using push R_TIME 13;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("4. views state...");
			System.out.println("Materialized/PUSH...");
			rs = test1.executeQuery("SELECT * from view1@test1;");
			System.out.println("PULL...");
			rs = test1.executeQuery("SELECT * from view2@test1;");
			System.out.println("PUSH...");
			rs = test1.executeQuery("SELECT * from view3@test1;");
			System.out.println("Materialized/PULL...");
			rs = test1.executeQuery("SELECT * from view4@test1;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("5. load data (demo)...");
			demo.executeUpdate("INSERT INTO t1 values(0,'monetdb');");
			demo.executeUpdate("INSERT INTO t1 values(1,'monet');");
			demo.executeUpdate("INSERT INTO t1 values(2,'mon');");
			demo.executeUpdate("INSERT INTO t1 values(3,'mo');");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}

		try {
			System.out.println("6. refresh test (both servers on)...");
			System.out.println("Materialized/PUSH...");
			rs = test1.executeQuery("SELECT * from view1@test1;");
			System.out.println("PULL...");
			rs = test1.executeQuery("SELECT * from view2@test1;");
			System.out.println("PUSH...");
			rs = test1.executeQuery("SELECT * from view3@test1;");
			System.out.println("Materialized/PULL...");
			rs = test1.executeQuery("SELECT * from view4@test1;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("7. load data 2 (demo)...");
			demo.executeUpdate("INSERT INTO t1 values(10,'2-monetdb');");
			demo.executeUpdate("INSERT INTO t1 values(11,'2-monet');");
			demo.executeUpdate("INSERT INTO t1 values(12,'2-mon');");
			demo.executeUpdate("INSERT INTO t1 values(13,'2-mo');");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		demo_con.close();
		
		try {
			System.out.println("8. refresh test (demo off)...");
			System.out.println("Materialized/PUSH...");
			rs = test1.executeQuery("SELECT * from view1@test1;");
			System.out.println("PULL...");
			rs = test1.executeQuery("SELECT * from view2@test1;");
			System.out.println("PUSH...");
			rs = test1.executeQuery("SELECT * from view3@test1;");
			System.out.println("Materialized/PULL...");
			rs = test1.executeQuery("SELECT * from view4@test1;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("9. Delete catalog information (demo)...");
			demo.executeUpdate("DROP TABLE t1@demo;");
			demo.executeUpdate("DROP TABLE t1@test1;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("10. Delete catalog information (test1)...");
			test1.executeUpdate("DROP TABLE t1@demo;");
			test1.executeUpdate("DROP TABLE t1@test1;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		if (rs != null) rs.close();

		test1_con.close();
	}
}
