/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_FetchSize {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("SELECT * FROM _tables");

		System.out.println("Statement fetch size before set: " + stmt.getFetchSize());
		System.out.println("ResultSet fetch size before set: " + rs.getFetchSize());

		rs.setFetchSize(16384);

		System.out.println("Statement fetch size before set: " + stmt.getFetchSize());
		System.out.println("ResultSet fetch size before set: " + rs.getFetchSize());

		rs.close();
		con.close();
	}
}
