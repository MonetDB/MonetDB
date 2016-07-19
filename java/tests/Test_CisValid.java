/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_CisValid {
	/* Test that after an error has occurred during a transaction, one can
	 * still test if the connection is valid or not. 
	 * The function Connection.isValid() should only return TRUE or FALSE. It
	 * shall never alter the state of this connection */
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection conn = DriverManager.getConnection(args[0]);
		Statement stmt = conn.createStatement();

		try {
			conn.setAutoCommit(false); // start a transaction
			stmt.execute("SELECT COUNT(*) FROM doesnotexist;"); // let's trigger an error
		} catch (SQLException e) {
			// e.printStackTrace();
			System.err.println("Expected error: " + e);

			try {
				// test calling conn.isValid()
				System.out.println("Validating connection: conn.isValid? " + conn.isValid(30));
				// Can we rollback on this connection without causing an error?
				conn.rollback();
			} catch (SQLException e2) {
				System.err.println("UnExpected error: " + e2);
			}
		}

		stmt.close();
		conn.close();
	}
}
