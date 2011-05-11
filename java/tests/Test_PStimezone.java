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
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

import java.sql.*;
import java.util.*;
import java.text.*;

public class Test_PStimezone {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;
		ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		con.setAutoCommit(false);
		// >> false: auto commit was just switched off
		System.out.println("0. false\t" + con.getAutoCommit());

		try {
			stmt.executeUpdate("CREATE TABLE table_Test_PStimezone (ts timestamp, tsz timestamp with time zone, t time, tz time with time zone)");
		} catch (SQLException e) {
			System.out.println(e);
			System.out.println("Creation of test table failed! :(");
			System.out.println("ABORTING TEST!!!");
			System.exit(-1);
		}

		try {
			pstmt = con.prepareStatement("INSERT INTO table_Test_PStimezone VALUES (?, ?, ?, ?)");
			System.out.print("1. empty call...");
			try {
				// should fail (no arguments given)
				pstmt.execute();
				System.out.println(" PASSED :(");
				System.out.println("ABORTING TEST!!!");
				System.exit(-1);
			} catch (SQLException e) {
				System.out.println(" failed :)");
			}

			System.out.println("2. inserting records...");

			// make sure this test is reproducable regardless timezone
			// setting, by overriding the VM's default
			TimeZone.setDefault(TimeZone.getTimeZone("Africa/Windhoek"));

			SimpleDateFormat tsz =
				new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSZ");
			SimpleDateFormat tz =
				new SimpleDateFormat("HH:mm:ss.SSSZ");

			java.sql.Timestamp ts = new java.sql.Timestamp(0L);
			java.sql.Time t = new java.sql.Time(0L);

			Calendar c = Calendar.getInstance();
			tsz.setTimeZone(c.getTimeZone());
			tz.setTimeZone(tsz.getTimeZone());
			System.out.println("inserting (" + c.getTimeZone().getID() + ") " + tsz.format(ts) + ", " + tz.format(t));

			pstmt.setTimestamp(1, ts);
			pstmt.setTimestamp(2, ts);
			pstmt.setTime(3, t);
			pstmt.setTime(4, t);
			pstmt.executeUpdate();
			
			c.setTimeZone(TimeZone.getTimeZone("UTC"));

			System.out.println("inserting with calendar timezone " + c.getTimeZone().getID());
			pstmt.setTimestamp(1, ts, c);
			pstmt.setTimestamp(2, ts, c);
			pstmt.setTime(3, t, c);
			pstmt.setTime(4, t, c);
			pstmt.executeUpdate();
			
			c.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
			System.out.println("inserting with calendar timezone " + c.getTimeZone().getID());
			pstmt.setTimestamp(1, ts, c);
			pstmt.setTimestamp(2, ts);
			pstmt.setTime(3, t, c);
			pstmt.setTime(4, t);
			pstmt.executeUpdate();
			
			c.setTimeZone(TimeZone.getTimeZone("GMT+04:15"));
			System.out.println("inserting with calendar timezone " + c.getTimeZone().getID());
			pstmt.setTimestamp(1, ts);
			pstmt.setTimestamp(2, ts, c);
			pstmt.setTime(3, t);
			pstmt.setTime(4, t, c);
			pstmt.executeUpdate();
			
			System.out.println("done");
			System.out.print("3. closing PreparedStatement...");
			pstmt.close();
			System.out.println(" passed :)");

			System.out.print("4. selecting records...");
			pstmt = con.prepareStatement("SELECT * FROM table_Test_PStimezone");
			rs = pstmt.executeQuery();
			System.out.println(" passed :)");

			// The tz fields should basically always be the same
			// (exactly 1st Jan 1970) since whatever timezone is used,
			// the server retains it, and Java restores it.
			// The zoneless fields will show differences since the time
			// is inserted translated to the given timezones, and
			// retrieved as in they were given in those timezones.  When
			// the insert zone matches the retrieve zone, Java should
			// eventually see 1st Jan 1970.
			while (rs.next()) {
				System.out.println("retrieved row (String):\n\t" +
						rs.getString("ts") + "   \t" +
						rs.getString("tsz") + "\t" +
						rs.getString("t") + "         \t" +
						rs.getString("tz"));

				tsz.setTimeZone(TimeZone.getDefault());
				tz.setTimeZone(tsz.getTimeZone());
				System.out.println("default (" + tsz.getTimeZone().getID() + "):\n\t" +
						tsz.format(rs.getTimestamp("ts")) + "\t" +
						tsz.format(rs.getTimestamp("tsz")) + "    \t" +
						tz.format(rs.getTime("t")) + "\t" +
						tz.format(rs.getTime("tz")));

				c.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
				System.out.println(c.getTimeZone().getID() + ":\n\t" +
						rs.getTimestamp("ts", c) + "      \t" +
						rs.getTimestamp("tsz", c) + "           \t" +
						rs.getTime("t", c) + "              \t" +
						rs.getTime("tz", c) + "      ");

				c.setTimeZone(TimeZone.getTimeZone("UTC"));
				System.out.println(c.getTimeZone().getID() + ":\n\t" +
						rs.getTimestamp("ts", c) + "      \t" +
						rs.getTimestamp("tsz", c) + "           \t" +
						rs.getTime("t", c) + "               \t" +
						rs.getTime("tz", c) + "      ");

				System.out.println();
				SQLWarning w = rs.getWarnings();
				while (w != null) {
					System.out.println(w.getMessage());
					w = w.getNextWarning();
				}
			}

			con.rollback();
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
