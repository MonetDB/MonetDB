/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

import java.sql.*;

public class BugDatabaseMetaData_Bug_3356 {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection(args[0]);
		DatabaseMetaData dbmd = con.getMetaData();
		ResultSet rs = dbmd.getColumns("mTests_sql_jdbc_tests", "sys", "_tables", "id");
		rs.next();
		String tableName1 = rs.getString("TABLE_NAME");
		String tableName2 = rs.getString(3);
		String isNullable1 = rs.getString("IS_NULLABLE");
		String isNullable2 = rs.getString(18);
		System.out.println(tableName1);
		System.out.println(tableName2);
		System.out.println(isNullable1);
		System.out.println(isNullable2);
		rs.close();
		con.close();
	}
}
