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




public class Test_Remote_View_Syntax {
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
			System.out.println("3. load data (demo)...");
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
			System.out.println("4. load data (test1)...");
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
			System.out.println("5. DB name not used...");
			demo.executeUpdate("CREATE VIEW view1 as select id from t1 where id>0;");
			demo.executeUpdate("CREATE VIEW view2 as select * from t1;");
			demo.executeUpdate("CREATE VIEW view3 as select id,name from t1@test1 where id<0;");
			demo.executeUpdate("CREATE VIEW view4 as select * from t1@test1;");
			demo.executeUpdate("CREATE VIEW view5 as select age from t1@test1;");
			demo.executeUpdate("CREATE VIEW view6 as select id,name from t2@test1;");
			demo.executeUpdate("CREATE VIEW view7 as select id,name from t1@test3;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("6. information into the views...");
			rs = demo.executeQuery("SELECT * from view1;");
			rs = demo.executeQuery("SELECT * from view2;");
			rs = demo.executeQuery("SELECT * from view3;");
			rs = demo.executeQuery("SELECT * from view4;");
			rs = demo.executeQuery("SELECT * from view5;");
			rs = demo.executeQuery("SELECT * from view6;");
			rs = demo.executeQuery("SELECT * from view7;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("7. Drop views...");
			demo.executeUpdate("DROP VIEW view1;");
			demo.executeUpdate("DROP VIEW view2;");
			demo.executeUpdate("DROP VIEW view3;");
			demo.executeUpdate("DROP VIEW view4;");
			demo.executeUpdate("DROP VIEW view5;");
			demo.executeUpdate("DROP VIEW view6;");
			demo.executeUpdate("DROP VIEW view7;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}

		try {
			System.out.println("8. dbname is used...");
			demo.executeUpdate("CREATE VIEW view1@demo as select id from t1 where id>0;");
			demo.executeUpdate("CREATE VIEW view2@demo as select * from t1;");
			demo.executeUpdate("CREATE VIEW view3@demo as select id,name from t1@test1 where id<0;");
			demo.executeUpdate("CREATE VIEW view4@demo as select * from t1@test1;");
			demo.executeUpdate("CREATE VIEW view5@demo as select age from t1@test1;");
			demo.executeUpdate("CREATE VIEW view6@demo as select id,name from t2@test1;");
			demo.executeUpdate("CREATE VIEW view7@demo as select id,name from t1@test3;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("9. information into the views...");
			rs = demo.executeQuery("SELECT * from view1;");
			rs = demo.executeQuery("SELECT * from view2;");
			rs = demo.executeQuery("SELECT * from view3;");
			rs = demo.executeQuery("SELECT * from view4;");
			rs = demo.executeQuery("SELECT * from view5;");
			rs = demo.executeQuery("SELECT * from view6;");
			rs = demo.executeQuery("SELECT * from view7;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("10. Drop views...");
			demo.executeUpdate("DROP VIEW view1;");
			demo.executeUpdate("DROP VIEW view2;");
			demo.executeUpdate("DROP VIEW view3;");
			demo.executeUpdate("DROP VIEW view4;");
			demo.executeUpdate("DROP VIEW view5;");
			demo.executeUpdate("DROP VIEW view6;");
			demo.executeUpdate("DROP VIEW view7;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}

		try {
			System.out.println("11. USING options...");
			demo.executeUpdate("CREATE VIEW view1 as select id from t1@test1 where id>0 USING PULL;");
			demo.executeUpdate("CREATE VIEW view2 as select * from t1@test1 USING PUSH;");
			demo.executeUpdate("CREATE VIEW view3 as select id,name from t1@test1 where id<0 USING PUU;");
			demo.executeUpdate("CREATE VIEW view4 as select * from t1@test1 USING PULL, R_TIME 34;");
			demo.executeUpdate("CREATE VIEW view5 as select name from t1@test1 USING PUSH, R_TIME 234;");
			demo.executeUpdate("CREATE VIEW view6 as select id,name from t1@test1 USING R_TIME 500;");
			demo.executeUpdate("CREATE VIEW view7 as select id,name from t1@test1 USING PULL, R_TIME;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("12. Replication catalog...");
			rs = demo.executeQuery("select * from replication;");
			rs = demo.executeQuery("select * from replication_columns;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("13. Drop views...");
			demo.executeUpdate("DROP VIEW view1;");
			demo.executeUpdate("DROP VIEW view2;");
			demo.executeUpdate("DROP VIEW view3;");
			demo.executeUpdate("DROP VIEW view4;");
			demo.executeUpdate("DROP VIEW view5;");
			demo.executeUpdate("DROP VIEW view6;");
			demo.executeUpdate("DROP VIEW view7;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}

		try {
			System.out.println("14. WITH options...");
			demo.executeUpdate("CREATE VIEW view1 as select id from t1@test1 where id>0 WITH DATA USING PULL;");
			demo.executeUpdate("CREATE VIEW view2 as select * from t1@test1 WITH CHECK OPTION;");
			demo.executeUpdate("CREATE VIEW view3 as select id,name from t1@test1 where id<0 USING CHECK OPTION, DATA ;");
			demo.executeUpdate("CREATE VIEW view4 as select * from t1@test1 USING PULL, R_TIME 34 WITH DATA;");
			demo.executeUpdate("CREATE VIEW view5 as select name from t1@test1 WITH NO DATA USING PUSH, R_TIME 234;");
			demo.executeUpdate("CREATE VIEW view6 as select id,name from t1@test1 WITH CHECK OPTION USING R_TIME 500;");
			demo.executeUpdate("CREATE VIEW view7 as select id,name from t1@test1 WITH NO DATA , CHECK OPTION;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("15. Replication catalog...");
			rs = demo.executeQuery("select * from replication;");
			rs = demo.executeQuery("select * from replication_columns;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}
		
		try {
			System.out.println("16. Drop views...");
			demo.executeUpdate("DROP VIEW view1;");
			demo.executeUpdate("DROP VIEW view2;");
			demo.executeUpdate("DROP VIEW view3;");
			demo.executeUpdate("DROP VIEW view4;");
			demo.executeUpdate("DROP VIEW view5;");
			demo.executeUpdate("DROP VIEW view6;");
			demo.executeUpdate("DROP VIEW view7;");
			System.out.println("passed :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
			demo_con.close();
			test1_con.close();
			System.exit(-1);
		}

		try {
			System.out.println("17. Delete catalog information (demo)...");
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
			System.out.println("18. Delete catalog information (test1)...");
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
