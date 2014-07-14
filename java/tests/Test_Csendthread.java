/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
