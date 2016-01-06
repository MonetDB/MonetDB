/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;
import java.util.*;
import java.nio.charset.Charset;

public class Test_PSlargebatchval {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		Statement stmt = con.createStatement();
		PreparedStatement pstmt;

		// >> true: auto commit should be on
		System.out.println("0. true\t" + con.getAutoCommit());

		byte[] errorBytes = new byte[] { (byte) 0xe2, (byte) 0x80, (byte) 0xa7 };
		String errorStr = new String(errorBytes, Charset.forName("UTF-8"));
		StringBuilder repeatedErrorStr = new StringBuilder();
		for (int i = 0; i < 8170;i++) {
			repeatedErrorStr.append(errorStr);
		}

		try {
			stmt.execute("CREATE TABLE x (c INT, a CLOB, b DOUBLE)");
			pstmt = con.prepareStatement("INSERT INTO x VALUES (?,?,?)");
			pstmt.setLong(1, 1);
			pstmt.setString(2, repeatedErrorStr.toString());
			pstmt.setDouble(3, 1.0);
			pstmt.addBatch();
			pstmt.executeBatch();
			stmt.execute("DROP TABLE x");

			pstmt.close();
			stmt.close();
		} catch (SQLException e) {
			System.out.println("FAILED :( "+ e.getMessage());
			while ((e = e.getNextException()) != null)
				System.out.println("FAILED :( " + e.getMessage());
			System.out.println("ABORTING TEST!!!");
		}

		con.close();
	}
}
