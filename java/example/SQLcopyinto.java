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
import java.io.*;
import java.util.*;
import nl.cwi.monetdb.mcl.net.*;
import nl.cwi.monetdb.mcl.io.*;

/**
 * This example demonstrates how the MonetDB JDBC driver can facilitate
 * in performing COPY INTO sequences.  This is mainly meant to show how
 * a quick load can be performed from Java.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */

public class SQLcopyinto {
	public static void main(String[] args) throws Exception {
		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		// request a connection suitable for Monet from the driver manager
		// note that the database specifier is currently not implemented, for
		// Monet itself can't access multiple databases.
		// turn on debugging
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/database", "monetdb", "monetdb");

		// get a statement to execute on
		Statement stmt = con.createStatement();

		String query = "CREATE TABLE example (id int, val varchar(24))";
		try {
			stmt.execute(query);
		} catch (SQLException e) {
			System.out.println(query + ": " + e.getMessage());
			System.exit(1);
		}

		// now create a connection manually to perform a load, this can
		// of course also be done simultaneously with the JDBC
		// connection being kept connected

		MapiSocket server = new MapiSocket();

		server.setDatabase("database");
		server.setLanguage("sql");

		try {
			List warning = 
				server.connect("localhost", 50000, "monetdb", "monetdb");
			if (warning != null) {
				for (Iterator it = warning.iterator(); it.hasNext(); ) {
					System.out.println(it.next().toString());
				}
			}

			BufferedMCLReader in = server.getReader();
			BufferedMCLWriter out = server.getWriter();

			String error = in.waitForPrompt();
			if (error != null)
				throw new Exception(error);

			query = "COPY INTO example FROM STDIN USING DELIMITERS ',','\\n';";
			// the leading 's' is essential, since it is a protocol
			// marker that should not be ommitted, as much as the
			// trailing semi-colon
			out.write('s');
			out.write(query);
			out.newLine();
			for (int i = 0; i < 100; i++) {
				out.write("" + i + ",val_" + i);
				out.newLine();
			}
			out.writeLine(""); // need this one for synchronisation over flush()
			error = in.waitForPrompt();
			if (error != null)
				throw new Exception(error);
			out.writeLine(""); // server wants more, we're going to tell it, this is it
			error = in.waitForPrompt();
			if (error != null)
				throw new Exception(error);
			// disconnect from server
			server.close();
		} catch (IOException e) {
			System.err.println("unable to connect: " + e.getMessage());
			System.exit(-1);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(-1);
		}

		query = "SELECT COUNT(*) FROM example";
		ResultSet rs = null;
		try {
			rs = stmt.executeQuery(query);
		} catch (SQLException e) {
			System.out.println(query + ": " + e.getMessage());
			System.exit(1);
		}
		if (rs != null && rs.next())
			System.out.println(rs.getString(1));

		// free resources, close the statement
		stmt.close();
		// close the connection with the database
		con.close();

	}
}
