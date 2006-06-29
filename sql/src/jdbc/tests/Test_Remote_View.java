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




public class Test_Remote_View {
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
			System.out.println("2. load data (test1)...");
			test1.executeUpdate("INSERT INTO t1 values(0,'fabian',10);");
			test1.executeUpdate("INSERT INTO t1 values(1,'martin',11);");
			test1.executeUpdate("INSERT INTO t1 values(2,'niels',12);");
			test1.executeUpdate("INSERT INTO t1 values(3,'romulo',13);");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("3. simple and complex views...");
			test1.executeUpdate("CREATE VIEW view1 as select id from t1@demo where id>0 USING PUSH;");
			test1.executeUpdate("CREATE VIEW view2 as select view1.id,table1.name from t1@demo as table1, view1 where id>0 USING PUSH;");
			test1.executeUpdate("CREATE VIEW view3 as select id,name from t1@demo where id<0 WITH DATA;");
			test1.executeUpdate("CREATE VIEW view4 as select id from t1@test1 WITH DATA;");
			test1.executeUpdate("CREATE VIEW view5 as select * from t1@demo where id >1 and name LIKE 'mon' WITH DATA;");
			test1.executeUpdate("CREATE VIEW view6 as select table1.id,table2.age from t1@demo as table1, t1@test1 as table2 where id>1 or id<4 WITH DATA;");
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
			rs = test1.executeQuery("SELECT * from view1;");
			rs = test1.executeQuery("SELECT * from view2;");
			rs = test1.executeQuery("SELECT * from view3;");
			rs = test1.executeQuery("SELECT * from view4;");
			rs = test1.executeQuery("SELECT * from view5;");
			rs = test1.executeQuery("SELECT * from view6;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}

		try {
			System.out.println("5. Catalog state...");
			rs = test1.executeQuery("SELECT * FROM _tables;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("6. Replication Catalog...");
			rs = test1.executeQuery("SELECT * FROM replication;");
			rs = test1.executeQuery("SELECT * FROM replication_columns;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("7. drop views...");
			test1.executeUpdate("DROP VIEW view1;");
			test1.executeUpdate("DROP VIEW view2;");
			test1.executeUpdate("DROP VIEW view3;");
			test1.executeUpdate("DROP VIEW view4;");
			test1.executeUpdate("DROP VIEW view5;");
			test1.executeUpdate("DROP VIEW view6;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("8. Delete catalog information (demo)...");
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
			System.out.println("9. Delete catalog information (test1)...");
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

		demo_con.close();
		test1_con.close();
	}
}
