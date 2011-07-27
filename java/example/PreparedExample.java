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

/**
 * This example shows the use of the PreparedStatement
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class PreparedExample {
	public static void main(String[] args) throws Exception {
		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/notused", "monetdb", "monetdb");
		PreparedStatement st = con.prepareStatement("SELECT ? AS a1, ? AS a2");
		ResultSet rs;

		st.setString(1, "te\\s't");
		st.setInt(2, 10);

		rs = st.executeQuery();
		// get meta data and print columns with their type
		ResultSetMetaData md = rs.getMetaData();
		for (int i = 1; i <= md.getColumnCount(); i++) {
			System.out.print(md.getColumnName(i) + ":" +
				md.getColumnTypeName(i) + "\t");
		}
		System.out.println("");

		while (rs.next()) {
			for (int j = 1; j <= md.getColumnCount(); j++) {
				System.out.print(rs.getString(j) + "\t");
			}
			System.out.println("");
		}

		con.close();
	}
}
