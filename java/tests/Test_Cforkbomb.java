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

public class Test_Cforkbomb {
	private static String args[];

	static class Worker extends Thread {
		private int id;

		public Worker(int id) {
			this.id = id;
		}

		public void run() {
			try {
				System.out.print("Establishing Connection " + id + "...");
				Connection con = DriverManager.getConnection(args[0]);
				System.out.println(" done...");

				// do something with the connection to test if it works
				Statement stmt = con.createStatement();
				ResultSet rs = stmt.executeQuery("SELECT " + id);
				if (!rs.next()) {
					System.out.println("thread " + id + " got no response from server :(");
				} else {
					if (rs.getInt(1) == id) {
						System.out.println("thread " + id + ": connection ok");
					} else {
						System.out.println("thread " + id + ": got garbage: " + rs.getString(1));
					}
				}

				con.close();
			} catch (SQLException e) {
				System.out.println("thread " + id + " unhappy: " + e.toString());
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Test_Cforkbomb.args = args;
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");

		// just DoS the server full throttle :)
		int i;
		for (i = 0; i < 200; i++) {
			Worker w = new Worker(i);
			w.start();
		}
	}
}
