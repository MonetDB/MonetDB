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
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

import java.sql.*;
import java.util.*;

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

			System.out.print("2. inserting a records...");
			
			Calendar c = Calendar.getInstance();
			c.setTime(new java.util.Date(0L));
			
			pstmt.setTimestamp(1, new java.sql.Timestamp(c.getTime().getTime()));
			pstmt.setTimestamp(2, new java.sql.Timestamp(c.getTime().getTime()));
			pstmt.setTime(3, new java.sql.Time(c.getTime().getTime()));
			pstmt.setTime(4, new java.sql.Time(c.getTime().getTime()));
			pstmt.executeUpdate();
			
			pstmt.setTimestamp(1, new java.sql.Timestamp(c.getTime().getTime()), c);
			pstmt.setTimestamp(2, new java.sql.Timestamp(c.getTime().getTime()), c);
			pstmt.setTime(3, new java.sql.Time(c.getTime().getTime()), c);
			pstmt.setTime(4, new java.sql.Time(c.getTime().getTime()), c);
			pstmt.executeUpdate();
			
			c.setTimeZone(TimeZone.getTimeZone("PST"));
			pstmt.setTimestamp(1, new java.sql.Timestamp(c.getTime().getTime()));
			pstmt.setTimestamp(2, new java.sql.Timestamp(c.getTime().getTime()), c);
			pstmt.setTime(3, new java.sql.Time(c.getTime().getTime()));
			pstmt.setTime(4, new java.sql.Time(c.getTime().getTime()), c);
			pstmt.executeUpdate();
			
			c.setTimeZone(TimeZone.getTimeZone("GMT+04:15"));
			pstmt.setTimestamp(1, new java.sql.Timestamp(c.getTime().getTime()), c);
			pstmt.setTimestamp(2, new java.sql.Timestamp(c.getTime().getTime()));
			pstmt.setTime(3, new java.sql.Time(c.getTime().getTime()), c);
			pstmt.setTime(4, new java.sql.Time(c.getTime().getTime()));
			pstmt.executeUpdate();
			
			System.out.println(" passed :)");
			System.out.print("3. closing PreparedStatement...");
			pstmt.close();
			System.out.println(" passed :)");

			System.out.print("4. selecting records...");
			pstmt = con.prepareStatement("SELECT * FROM table_Test_PStimezone");
			rs = pstmt.executeQuery();
			System.out.println(" passed :)");

			while (rs.next()) {
//				System.out.println(rs.getString("ts") + "\t" + rs.getString("tsz"));
				System.out.println(rs.getTimestamp("ts") + "\t" + rs.getTimestamp("tsz"));
				c.setTimeZone(TimeZone.getTimeZone("PST"));
				System.out.println(rs.getTimestamp("ts", c) + "\t" + rs.getTimestamp("tsz", c));
				c.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
				System.out.println(rs.getTimestamp("ts", c) + "\t" + rs.getTimestamp("tsz", c));
//				System.out.println(rs.getString("t") + "\t" + rs.getString("tz"));
				System.out.println(rs.getTime("t") + "\t" + rs.getTime("tz"));
				c.setTimeZone(TimeZone.getTimeZone("PST"));
				System.out.println(rs.getTime("t", c) + "\t" + rs.getTime("tz", c));
				c.setTimeZone(TimeZone.getTimeZone("GMT+00:00"));
				System.out.println(rs.getTime("t", c) + "\t" + rs.getTime("tz", c));
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
