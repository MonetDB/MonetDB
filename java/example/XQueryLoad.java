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
 * This is an example showing the use of loading ("shredding") XML
 * documents into MonetDB/XQuery.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class XQueryLoad {
	public static void main(String[] args) throws Exception {
		// make sure the driver is loaded
		Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		Connection con = DriverManager.getConnection("jdbc:monetdb://localhost/notused?language=xquery&xdebug=true", "monetdb", "monetdb");
		Statement st = con.createStatement();

		st.addBatch("<?xml version=\"1.0\" encoding=\"utf-8\"?>");
		st.addBatch("<doc>");
		st.addBatch(" <greet kind=\"informal\">Hi </greet>");
		st.addBatch(" <greet kind=\"casual\">Hello </greet>");
		st.addBatch(" <location kind=\"global\">World</location>");
		st.addBatch(" <location kind=\"local\">Amsterdam</location>");
		st.addBatch("</doc>");

		st.executeBatch();

		/* The name of the document is written as warning to the
		 * Connection's warning stack.  This is kind of dirty, but since
		 * the batch cannot return a string, there is no other way here.
		 */
		SQLWarning w = con.getWarnings();
		while (w != null) {
			System.out.println(w.getMessage());
			w = w.getNextWarning();
		}

		st.close();
		con.close();
	}
}
