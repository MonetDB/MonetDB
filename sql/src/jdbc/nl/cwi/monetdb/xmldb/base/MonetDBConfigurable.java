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
	String getProperty(String name) throws XMLDBException {
		return(properties.get(name));
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
	void setProperty(String name, String value) throws XMLDBException {
		properties.put(name, value);
	}
}

