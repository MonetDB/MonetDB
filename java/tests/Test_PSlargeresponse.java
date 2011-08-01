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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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
