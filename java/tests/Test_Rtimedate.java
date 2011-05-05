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
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

import java.sql.*;

public class Test_Rtimedate {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		con.setAutoCommit(false);
		// >> false: auto commit should be off now
		System.out.println("false\t" + con.getAutoCommit());

		try {
			stmt.executeUpdate("CREATE TABLE table_Test_Rtimedate ( id int, ts timestamp, t time, d date, vc varchar(30), PRIMARY KEY (id) )");

			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, ts) VALUES (1, timestamp '2004-04-24 11:43:53.000')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, t) VALUES (2, time '11:43:53.000')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, d) VALUES (3, date '2004-04-24')");

			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (4, '2004-04-24 11:43:53.000000')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (5, '11:43:53')");
			stmt.executeUpdate("INSERT INTO table_Test_Rtimedate(id, vc) VALUES (6, '2004-04-24')");

			rs = stmt.executeQuery("SELECT * FROM table_Test_Rtimedate");

			rs.next();
			// the next three should all go well
			System.out.println("1. " + rs.getString("id") + ", " + rs.getString("ts") + ", " + rs.getTimestamp("ts"));
			System.out.println("2. " + rs.getString("id") + ", " + rs.getString("ts") + ", " + rs.getTime("ts"));
			System.out.println("3. " + rs.getString("id") + ", " + rs.getString("ts") + ", " + rs.getDate("ts"));
			rs.next();
			// the next two should go fine
			System.out.println("4. " + rs.getString("id") + ", " + rs.getString("t") + ", " + rs.getTimestamp("t"));
			System.out.println("5. " + rs.getString("id") + ", " + rs.getString("t") + ", " + rs.getTime("t"));
			// this one should return 0
			System.out.println("6. " + rs.getString("id") + ", " + rs.getString("t") + ", " + rs.getDate("t"));
			rs.next();
			// the next one passes
			System.out.println("7. " + rs.getString("id") + ", " + rs.getString("d") + ", " + rs.getTimestamp("d"));
			// this one should return 0
			System.out.println("8. " + rs.getString("id") + ", " + rs.getString("d") + ", " + rs.getTime("d"));
			// and this one should pass again
			System.out.println("9. " + rs.getString("id") + ", " + rs.getString("d") + ", " + rs.getDate("d"));

			// in the tests below a bare string is parsed
			// everything will fail except the ones commented on
			rs.next();
			// timestamp -> timestamp should go
			System.out.println("1. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTimestamp("vc"));
			System.out.println("2. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTime("vc"));
			// timestamp -> date goes because the begin is the same
			System.out.println("3. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getDate("vc"));
			rs.next();
			System.out.println("4. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTimestamp("vc"));
			// time -> time should fit
			System.out.println("5. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTime("vc"));
			System.out.println("6. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getDate("vc"));
			rs.next();
			System.out.println("7. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTimestamp("vc"));
			System.out.println("8. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTime("vc"));
			// date -> date should be fine
			System.out.println("9. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getDate("vc"));

			SQLWarning w = con.getWarnings();
			while (w != null) {
				System.out.println("warning: " + w.toString());
				w = w.getNextWarning();
			}
		} catch (SQLException e) {
			System.out.println("failed :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.rollback();
		con.close();
	}
}
