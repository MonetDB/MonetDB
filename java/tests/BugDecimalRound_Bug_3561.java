/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;
import java.math.BigDecimal;

public class BugDecimalRound_Bug_3561 {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt1 = con.createStatement();
		PreparedStatement st;
		Statement stmt2;
		ResultSet rs;
		BigDecimal bd = new BigDecimal("112.125");

		stmt1.executeUpdate("CREATE TABLE bug3561 (d decimal(14,4))");
		st = con.prepareStatement("INSERT INTO bug3561 VALUES (?)");
		st.setBigDecimal(1, bd);
		st.executeUpdate();
		stmt2 = con.createStatement();
		rs = stmt2.executeQuery("SELECT d FROM bug3561");
		while (rs.next())
			System.out.println(rs.getString(1));
		rs.close();
		stmt1.executeUpdate("DROP TABLE bug3561");
		con.close();
	}
}
