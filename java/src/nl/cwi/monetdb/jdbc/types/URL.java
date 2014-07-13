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

package nl.cwi.monetdb.jdbc.types;

import java.sql.*;
import java.net.*;

/**
 * The URL class represents the URL datatype in MonetDB.  It
 * represents an URL, that is, a well-formed string conforming to
 * RFC2396.
 */
public class URL implements SQLData {
	private String url;

	public String getSQLTypeName() {
		return "url";
	}

	public void readSQL(SQLInput stream, String typeName) throws SQLException {
		if (typeName.compareTo("url") != 0)
			throw new SQLException("can only use this class with 'url' type",
					"M1M05");
		url = stream.readString();
	}

	public void writeSQL(SQLOutput stream) throws SQLException {
		stream.writeString(url);
	}

	public String toString() {
		return url;
	}

	public void fromString(String newurl) throws Exception {
		if (newurl == null) {
			url = newurl;
			return;
		}
		new java.net.URL(newurl);
		// if above doesn't fail (throws an Exception), it is fine
		url = newurl;
	}

	public java.net.URL getURL() throws SQLException {
		if (url == null)
			return null;

		try {
			return new java.net.URL(url);
		} catch (MalformedURLException mue) {
			throw new SQLException("data is not a valid URL", "M0M27");
		}
	}

	public void setURL(java.net.URL nurl) throws Exception {
		url = nurl.toString();	
	}
}
