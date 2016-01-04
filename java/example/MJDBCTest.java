/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

/**
 * This example assumes there exists tables a and b filled with some data.
 * On these tables some queries are executed and the JDBC driver is tested
 * on it's accuracy and robustness against 'users'.
 *
 * @author Fabian Groffen
 */
public class MJDBCTest {
	public static void main(String[] args) throws Exception {
		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		// turn on debugging (disabled)
		//nl.cwi.monetdb.jdbc.MonetConnection.setDebug(true);
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/notused", "monetdb", "monetdb");
		Statement st = con.createStatement();
		ResultSet rs;

		rs = st.executeQuery("SELECT a.var1, COUNT(b.id) as total FROM a, b WHERE a.var1 = b.id AND a.var1 = 'andb' GROUP BY a.var1 ORDER BY a.var1, total;");
		// get meta data and print columns with their type
		ResultSetMetaData md = rs.getMetaData();
		for (int i = 1; i <= md.getColumnCount(); i++) {
			System.out.print(md.getColumnName(i) + ":" +
				md.getColumnTypeName(i) + "\t");
		}
		System.out.println("");
		// print the data: only the first 5 rows, while there probably are
		// a lot more. This shouldn't cause any problems afterwards since the
		// result should get properly discarded on the next query
		for (int i = 0; rs.next() && i < 5; i++) {
			for (int j = 1; j <= md.getColumnCount(); j++) {
				System.out.print(rs.getString(j) + "\t");
			}
			System.out.println("");
		}
		
		// tell the driver to only return 5 rows, it can optimize on this
		// value, and will not fetch any more than 5 rows.
		st.setMaxRows(5);
		// we ask the database for 22 rows, while we set the JDBC driver to
		// 5 rows, this shouldn't be a problem at all...
		rs = st.executeQuery("select * from a limit 22");
		// read till the driver says there are no rows left
		for (int i = 0; rs.next(); i++) {
			System.out.print("[" + rs.getString("var1") + "]");
			System.out.print("[" + rs.getString("var2") + "]");
			System.out.print("[" + rs.getInt("var3") + "]");
			System.out.println("[" + rs.getString("var4") + "]");
		}
		
		// this close is not needed, should be done by next execute(Query) call
		// however if there can be some time between this point and the next
		// execute call, it is from a resource perspective better to close it.
		//rs.close();
		
		// unset the row limit; 0 means as much as the database sends us
		st.setMaxRows(0);
		// we only ask 10 rows
		rs = st.executeQuery("select * from b limit 10;");
		// and simply print them
		while (rs.next()) {
			System.out.print(rs.getInt("rowid") + ", ");
			System.out.print(rs.getString("id") + ", ");
			System.out.print(rs.getInt("var1") + ", ");
			System.out.print(rs.getInt("var2") + ", ");
			System.out.print(rs.getString("var3") + ", ");
			System.out.println(rs.getString("var4"));
		}
		
		// this close is not needed, as the Statement will close the last
		// ResultSet around when it's closed
		// again, if that can take some time, it's nicer to close immediately
		// the reason why these closes are commented out here, is to test if
		// the driver really cleans up it's mess like it should
		//rs.close();

		// perform a ResultSet-less query (with no trailing ; since that should
		// be possible as well and is JDBC standard)
		// Note that this method should return the number of updated rows. This
		// method however always returns -1, since Monet currently doesn't
		// support returning the affected rows.
		st.executeUpdate("delete from a where var1 = 'zzzz'");

		// closing the connection should take care of closing all generated
		// statements from it...
		// Don't forget to do it yourself if the connection is reused or much
		// longer alive, since the Statement object contains a lot of things
		// you probably want to reclaim if you don't need them anymore.
		//st.close();
		con.close();
	}
}
