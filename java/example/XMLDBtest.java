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

import org.xmldb.api.*;
import org.xmldb.api.base.*;
import org.xmldb.api.modules.*;

/**
 * Quick example demonstrating the XML:DB driver.
 *
 * To compile and run this example, MonetDB_XMLDB.jar, MonetDB_JDBC.jar
 * and xmldb.jar (from the lib dir) should be in the classpath.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class XMLDBtest {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.xmldb.base.MonetDBDatabase");
		try {
			Collection col = DatabaseManager.getCollection("xmldb:monetdb://localhost/demo", "monetdb", "monetdb"); 

			XQueryService xqs = (XQueryService)col.getService("XQueryService", "1");
			ResourceSet set = xqs.query("(<foo>1</foo>,<bar />)");
			ResourceIterator it = set.getIterator();
			while(it.hasMoreResources()) {
				Resource r = it.nextResource();
				System.out.println(r.getContent());
			}
			
		} catch (XMLDBException e) {
			e.printStackTrace();
		}
	}
}
