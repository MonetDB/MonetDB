package nl.cwi.monetdb.xmldb.modules;

import org.xmldb.api.base.*;
import org.xmldb.api.modules.*;

import org.w3c.dom.*;
import org.xml.sax.*;

/**
 * Provides access to XML resources stored in the database.  An
 * XMLResource can be accessed either as text XML or via the DOM or SAX
 * APIs.<br />
 * <br />
 * The default behavior for getContent and setContent is to work with
 * XML data as text so these methods work on String content.
 */
public class MonetDBXMLResource implements XMLResource {

	//== interface org.xmldb.api.base.Resource

	/**
	 * Returns the Collection instance that this resource is associated
	 * with.  All resources must exist within the context of a
	 * Collection.
	 *
	 * @return the Collection associated with this Resource
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public Collection getParentCollection() throws XMLDBException {
		// should return MonetDBCollection thingher
		return(myParent);
	}

	/**
	 * Returns the unique id for this Resource or null if the Resource
	 * is anonymous.  The Resource will be anonymous if it is obtained
	 * as the result of a query.
	 *
	 * @returns the id for the Resource or null if no id exists.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getId() throws XMLDBException {
		// we're anonymous if we haven't got a Collection parent
		if (myParent != null) {
			return(toString());
		} else {
			return(null);
		}
	}

	/**
	 * Returns the resource type for this Resource.<br />
	 * <br />
	 * XML:DB defined resource types are:<br />
	 * XMLResource - all XML data stored in the database
	 * BinaryResource - Binary blob data stored in the database
	 *
	 * @return the resource type for the Resource.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getResourceType() throws XMLDBException {
		// RESOURCE_TYPE is defined in the interface XMLResource
		return(RESOURCE_TYPE);
	}

	/**
	 * Retrieves the content from the resource.  The type of the content
	 * varies depending what type of resource is being used.
	 *
	 * @return the content of the resource as XML String
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public Object getContent() throws XMLDBException {
		// we return as String
		return(content);
	}

	/**
	 * Sets the content for this resource.  The type of content that can
	 * be set depends on the type of resource being used.
	 *
	 * @param value the content value to set for the Resource
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public void setContent(Object value) throws XMLDBException {
		// can we do this without penalty?
		content = value.toString();
	}

	//== end interface Resource
}

