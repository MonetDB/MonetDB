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

package nl.cwi.monetdb.util;

import java.io.*;
import java.sql.*;


public abstract class Exporter {
	protected PrintWriter out;
	protected boolean useSchema;

	protected Exporter(PrintWriter out) {
		this.out = out;
	}

	public abstract void dumpSchema(
			DatabaseMetaData dbmd,
			String type,
			String catalog,
			String schema,
			String name) throws SQLException;

	public abstract void dumpResultSet(ResultSet rs) throws SQLException;

	public abstract void setProperty(int type, int value) throws Exception;
	public abstract int getProperty(int type) throws Exception;
	
	//=== shared utilities
	
	public void useSchemas(boolean use) {
		useSchema = use;
	}
	
	/**
	 * returns the given string between two double quotes for usage as
	 * identifier such as column or table name in SQL queries.
	 *
	 * @param in the string to quote
	 * @return the quoted string
	 */
	protected static String dq(String in) {
		return("\"" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"") + "\"");
	}

	/**
	 * returns the given string between two single quotes for usage as
	 * string literal in SQL queries.
	 *
	 * @param in the string to quote
	 * @return the quoted string
	 */
	protected static String q(String in) {
		return("'" + in.replaceAll("\\\\", "\\\\\\\\").replaceAll("'", "\\\\'") + "'");
	}

	/**
	 * Simple helper function to repeat a given character a number of
	 * times.
	 *
	 * @param chr the character to repeat
	 * @param cnt the number of times to repeat chr
	 * @return a String holding cnt times chr
	 */
	protected static String repeat(char chr, int cnt) {
		if (cnt < 0) return("");
		StringBuffer sb = new StringBuffer(cnt);
		for (int i = 0; i < cnt; i++) sb.append(chr);
		return(sb.toString());
	}
}
