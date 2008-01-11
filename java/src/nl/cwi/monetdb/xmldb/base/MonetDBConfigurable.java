/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2008 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.xmldb.base;

import org.xmldb.api.base.*;
import java.util.*;

/**
 * This class provides the ability to configure properties about an
 * object.  The properties are simply stored in a HashMap, as such get
 * and set inherit the behaviour of the java.util.Map interface.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetDBConfigurable implements Configurable {
	private Map properties = new HashMap();
	
	/**
	 * Returns the value of the property identified by name.
	 *
	 * @param name the name of the property to retrieve.
	 * @return the property value or null if no property exists.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getProperty(String name) throws XMLDBException {
		return((String)(properties.get(name)));
	}

	/**
	 * Sets the property name to have the value provided in value.
	 *
	 * @param name the name of the property to set.
	 * @param value the value to set for the property.
	 * @exception XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 */
	public void setProperty(String name, String value) throws XMLDBException {
		properties.put(name, value);
	}
}

