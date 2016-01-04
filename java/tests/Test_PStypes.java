/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class Test_PStypes {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;
		//ResultSet rs = null;
		//DatabaseMetaData dbmd = con.getMetaData();

		con.setAutoCommit(false);
		// >> false: auto commit was just switched off
		System.out.println("0. false\t" + con.getAutoCommit());

		try {
			stmt.executeUpdate(
"CREATE TABLE htmtest (" +
"       htmid    bigint       NOT NULL," +
"       ra       double ," +
"       decl     double ," +
"       dra      double ," +
"       ddecl    double ," +
"       flux     double ," +
"       dflux    double ," +
"       freq     double ," +
"       bw       double ," +
"       type     decimal(1,0)," +
"       imageurl varchar(100)," +
"       comment  varchar(100)," +
"       CONSTRAINT htmtest_htmid_pkey PRIMARY KEY (htmid)" +
")"
);
			// index is not used, but the original bug had it too
			stmt.executeUpdate("CREATE INDEX htmid ON htmtest (htmid)");
		} catch (SQLException e) {
			System.out.println(e);
			System.out.println("Creation of test table failed! :(");
			System.out.println("ABORTING TEST!!!");
			System.exit(-1);
		}

		try {
			pstmt = con.prepareStatement(
"INSERT INTO HTMTEST (HTMID,RA,DECL,FLUX,COMMENT) VALUES (?,?,?,?,?)"
);
			System.out.print("1. inserting a record...");

			pstmt.setLong(1, 1L);
			pstmt.setFloat(2, (float)1.2);
			pstmt.setDouble(3, 2.4);
			pstmt.setDouble(4, 3.2);
			pstmt.setString(5, "vlavbla");
			pstmt.executeUpdate();

			System.out.println("success :)");

			// try an update like bug #1757923
			pstmt = con.prepareStatement(
"UPDATE HTMTEST set COMMENT=?, TYPE=? WHERE HTMID=?"
);
			System.out.print("2. updating record...");

			pstmt.setString(1, "some update");
			pstmt.setObject(2, (float)3.2);
			pstmt.setLong(3, 1L);
			pstmt.executeUpdate();

			System.out.println("success :)");
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.rollback();
		con.close();
	}
}
