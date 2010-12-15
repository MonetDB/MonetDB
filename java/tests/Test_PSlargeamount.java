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
 * Copyright August 2008-2010 MonetDB B.V.
 * All Rights Reserved.
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
		// retrieve this to simulate a bug report
		DatabaseMetaData dbmd = con.getMetaData();

		// >> true: auto commit should be on
		System.out.println("0. true\t" + con.getAutoCommit());

		try {
			System.out.print("1. DatabaseMetadata environment retrieval... ");
			System.out.println(dbmd.getURL());

			System.out.println("2. Preparing and executing a unique statement");
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
