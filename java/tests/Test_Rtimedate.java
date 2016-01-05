/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// the next three should all go well
			System.out.println("1. " + rs.getString("id") + ", " + rs.getString("ts") + ", " + rs.getTimestamp("ts"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("2. " + rs.getString("id") + ", " + rs.getString("ts") + ", " + rs.getTime("ts"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("3. " + rs.getString("id") + ", " + rs.getString("ts") + ", " + rs.getDate("ts"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			rs.next();
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// the next two should go fine
			System.out.println("4. " + rs.getString("id") + ", " + rs.getString("t") + ", " + rs.getTimestamp("t"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("5. " + rs.getString("id") + ", " + rs.getString("t") + ", " + rs.getTime("t"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// this one should return 0
			System.out.println("6. " + rs.getString("id") + ", " + rs.getString("t") + ", " + rs.getDate("t"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			rs.next();
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// the next one passes
			System.out.println("7. " + rs.getString("id") + ", " + rs.getString("d") + ", " + rs.getTimestamp("d"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// this one should return 0
			System.out.println("8. " + rs.getString("id") + ", " + rs.getString("d") + ", " + rs.getTime("d"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// and this one should pass again
			System.out.println("9. " + rs.getString("id") + ", " + rs.getString("d") + ", " + rs.getDate("d"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();

			// in the tests below a bare string is parsed
			// everything will fail except the ones commented on
			rs.next();
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// timestamp -> timestamp should go
			System.out.println("1. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTimestamp("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("2. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTime("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// timestamp -> date goes because the begin is the same
			System.out.println("3. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getDate("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			rs.next();
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("4. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTimestamp("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// time -> time should fit
			System.out.println("5. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTime("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("6. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getDate("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			rs.next();
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("7. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTimestamp("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			System.out.println("8. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getTime("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();
			// date -> date should be fine
			System.out.println("9. " + rs.getString("id") + ", " + rs.getString("vc") + ", " + rs.getDate("vc"));
			readWarnings(rs.getWarnings());
			rs.clearWarnings();

			readWarnings(stmt.getWarnings());
			readWarnings(con.getWarnings());
		} catch (SQLException e) {
			System.out.println("failed :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.rollback();
		con.close();
	}

	private static void readWarnings(SQLWarning w) {
		while (w != null) {
			System.out.println("warning: " + w.toString());
			w = w.getNextWarning();
		}
	}
}
