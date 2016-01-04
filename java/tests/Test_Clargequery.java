/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Clargequery {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con1 = DriverManager.getConnection(args[0]);
		Statement stmt1 = con1.createStatement();
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on by default
		System.out.println("0. true\t" + con1.getAutoCommit());

		final String query = 
			"-- When a query larger than the send buffer is being " +
			"sent, a deadlock situation can occur when the server writes " +
			"data back, blocking because we as client are sending as well " +
			"and not reading.  Hence, to avoid this deadlock, in JDBC a " +
			"separate thread is started in the background such that results " +
			"from the server can be read, while data is still being sent to " +
			"the server.  To test this, we need to trigger the SendThread " +
			"being started, which we do with a quite large query.  We " +
			"construct it by repeating some stupid query plus a comment " +
			"a lot of times.  And as you're guessing by now, you're reading " +
			"this stupid comment that we use :)\n" +
			"select 1;\n";

		int size = 1000;
		StringBuffer bigq = new StringBuffer(query.length() * size);
		for (int i = 0; i < size; i++) {
			bigq.append(query);
		}

		// test commit by checking if a change is visible in another connection
		try {
			System.out.print("1. sending");
			stmt1.execute(bigq.toString());
			int i = 1;	// we skip the first "getResultSet()"
			while (stmt1.getMoreResults() != false) {
				i++;
			}
			if (stmt1.getUpdateCount() != -1) {
				System.out.println("found an update count for a SELECT query");
				throw new SQLException("boo");
			}
			if (i != size) {
				System.out.println("expecting " + size + " tuples, only got " + i);
				throw new SQLException("boo");
			}
		} catch (SQLException e) {
			// this means we failed (table not there perhaps?)
			System.out.println("FAILED :(");
			System.out.println("ABORTING TEST!!!");
		}

		if (rs != null) rs.close();

		con1.close();
	}
}
