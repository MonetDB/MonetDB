/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;
import java.util.*;

/* Create a lot of PreparedStatements, to emulate webloads such as those
 * from Hibernate. */

public class Test_PSlargeamount {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;

		// >> true: auto commit should be on
		System.out.println("0. true\t" + con.getAutoCommit());

		try {
			System.out.println("1. Preparing and executing a unique statement");
			for (int i = 0; i < 100000; i++) {
				pstmt = con.prepareStatement("select " + i + ", " + i + " = ?");
				pstmt.setInt(1, i);
				ResultSet rs = pstmt.executeQuery();
				if (rs.next() && i % 1000 == 0) {
					System.out.println(rs.getInt(1) + ", " + rs.getBoolean(2));
				}
				/* this call should cause resources on the server to be
				 * freed */
				pstmt.close();
			}
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
