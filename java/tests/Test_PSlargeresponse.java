/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;
import java.util.*;

public class Test_PSlargeresponse {
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

			pstmt = con.prepareStatement("select * from columns");
			System.out.print("2. empty call...");
			try {
				// should succeed (no arguments given)
				pstmt.execute();
				System.out.println(" passed :)");
			} catch (SQLException e) {
				System.out.println(" FAILED :(");
				System.out.println("ABORTING TEST!!!");
				System.exit(-1);
			}
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
