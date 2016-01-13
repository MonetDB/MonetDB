/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
