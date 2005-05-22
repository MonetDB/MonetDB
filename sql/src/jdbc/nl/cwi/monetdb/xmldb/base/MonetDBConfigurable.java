package nl.cwi.monetdb.xmldb.base;

import org.xmldb.api.base.*;

/**
 * This interface provides the ability to configure properties about an
 * object.  However, with the current MonetDB/XQuery implementation this
 * is not supported, and as such this implementation functions as an
 * interface satisfaction object ;)
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetDBConfigurable {
	/**
	 * Returns the value of the property identified by name.
	 *
	 * @param name the name of the property to retrieve.
	 * @return the property value or null if no property exists.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 */
	String getProperty(String name) throws XMLDBException {
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED, "Not implemented");
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
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED, "Not implemented");
	}
}

