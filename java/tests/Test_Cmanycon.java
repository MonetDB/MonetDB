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

public class Test_Cmanycon {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		List cons = new ArrayList(100);	// Connections go in here

		try {
			// spawn a lot of Connections, just for fun...
			int i;
			for (i = 0; i < 50; i++) {
				System.out.print("Establishing Connection " + i + "...");
				Connection con = DriverManager.getConnection(args[0]);
				System.out.print(" done...");

				// do something with the connection to test if it works
				con.setAutoCommit(false);
				System.out.println(" alive");

				cons.add(con);
			}

			// now try to nicely close them
			i = 0;
			for (Iterator it = cons.iterator(); it.hasNext(); i++) {
				Connection con = (Connection)(it.next());

				// see if the connection still works
				System.out.print("Closing Connection " + i + "...");
				con.setAutoCommit(true);
				System.out.print(" still alive...");
				con.close();
				System.out.println(" done");
			}
		} catch (SQLException e) {
			System.out.println("FAILED! " + e.getMessage());
		}
	}
}
