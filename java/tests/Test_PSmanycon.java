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

public class Test_PSmanycon {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		List pss = new ArrayList(100);	// Connections go in here

		try {
			// spawn a lot of Connections, just for fun...
			int i;
			for (i = 0; i < 50; i++) {
				System.out.print("Establishing Connection " + i + "...");
				Connection con = DriverManager.getConnection(args[0]);
				System.out.print(" done...");

				// do something with the connection to test if it works
				PreparedStatement pstmt = con.prepareStatement("select " + i);
				System.out.println(" alive");

				pss.add(pstmt);
			}

			// now try to nicely execute them
			i = 0;
			for (Iterator it = pss.iterator(); it.hasNext(); i++) {
				PreparedStatement pstmt = (PreparedStatement)(it.next());

				// see if the connection still works
				System.out.print("Executing PreparedStatement " + i + "...");
				if (!pstmt.execute())
					throw new Exception("should have seen a ResultSet!");

				ResultSet rs = pstmt.getResultSet();
				if (!rs.next())
					throw new Exception("ResultSet is empty");
				System.out.print(" result: " + rs.getString(1));
				pstmt.getConnection().close();
				System.out.println(", done");

				if ((i + 1) % 23 == 0) {
					// inject a failed transaction
					Connection con = DriverManager.getConnection(args[0]);
					Statement stmt = con.createStatement();
					try {
						int affrows = stmt.executeUpdate("update foo where bar is wrong");
						System.out.println("oops, faulty statement just got through :(");
					} catch (SQLException e) {
						System.out.println("Forced transaction failure");
					}
				}
			}
		} catch (SQLException e) {
			System.out.println("FAILED! " + e.getMessage());
		}
	}
}
