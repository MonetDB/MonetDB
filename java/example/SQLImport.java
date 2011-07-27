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

/**
 * This simple example somewhat emulates the mclient command. However in
 * it's simpleness it only supports SQL queries which entirely are on one line.
 *
 * This program reads a file line by line, and feeds the line into a running
 * Mserver on the localhost. Upon error, the error is reported and the program
 * continues reading and executing lines.
 * A very lousy way of implementing options is used to somewhat configure the
 * behaviour of the program in order to be a bit more verbose or commit after
 * each (sucessfully) executed line.
 *
 * The program uses a debuglog in which the exact conversation between the
 * JDBC driver and Mserver is reported. The log file is put in the current
 * working directory and names like monet_[unix timestamp].log
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */

public class SQLImport {
	public static void main(String[] args) throws Exception {
		if (args.length < 1) {
			System.out.println("synopsis: java SQLImport filename [autocommit] [verbose]");
			System.exit(-1);
		}

		// open the file
		BufferedReader fr = new BufferedReader(new FileReader(args[0]));

		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		// request a connection suitable for Monet from the driver manager
		// note that the database specifier is currently not implemented, for
		// Monet itself can't access multiple databases.
		// turn on debugging
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/database?debug=true", "monetdb", "monetdb");

		boolean beVerbose = false;
		if (args.length == 3) {
			// turn on verbose mode
			beVerbose = true;
		}
		if (args.length < 2) {
			// disable auto commit using the driver
			con.setAutoCommit(false);
		}

		// get a statement to execute on
		Statement stmt = con.createStatement();

		String query;
		for (int i = 1; (query = fr.readLine()) != null; i++) {
			if (beVerbose) System.out.println(query);
			try {
				// execute the query, no matter what it is
				stmt.execute(query);
			} catch (SQLException e) {
				System.out.println("Error on line " + i + ": " + e.getMessage());
				if (!beVerbose) System.out.println(query);
			}
		}

		// free resources, close the statement
		stmt.close();
		// close the connection with the database
		con.close();
		fr.close();
	}
}
