/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_Csendthread {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");

		System.out.println("0. active threads: " + Thread.activeCount());

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT 1");
		for (int i = 0; i < 256; i++) {
			sb.append("-- ADDING DUMMY TEXT AS COMMENT TO MAKE THE QUERY VERY VERY VERY VERY LONG\n");
		}
		sb.append(";\n");
		String longQuery = sb.toString();

		for (int i = 0; i < 10; i++) {
			for (int j = 0; j < 10; j++) {
				Connection conn = DriverManager.getConnection(args[0]);
				try {
					Statement st = conn.createStatement();
					st.execute(longQuery);
					st.close();
				} finally {
					conn.close();
				}
			}
			System.out.println("1. active threads: " + Thread.activeCount());
		}
		System.out.println("2. active threads: " + Thread.activeCount());
	}
}
