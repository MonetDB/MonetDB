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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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
