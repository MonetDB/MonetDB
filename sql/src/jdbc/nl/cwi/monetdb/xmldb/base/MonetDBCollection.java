package nl.cwi.monetdb.xmldb.base;

import org.xmldb.api.base;


/**
 * A Collection represents a collection of Resources stored within an
 * XML database. An XML database MAY expose collections as a
 * hierarchical set of parent and child collections.
 * <p />
 * MonetDB/XQuery at the moment exposes no collection at all.
 * <p />
 * A Collection provides access to the Resources stored by the
 * Collection and to Service instances that can operate against the
 * Collection and the Resources stored within it.  The Service mechanism
 * provides the ability to extend the functionality of a Collection in
 * ways that allows optional functionality to be enabled for the
 * Collection. 
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public interface Collection extends Configurable {
	/**
	 * Returns the name associated with the Collection instance.
	 *
	 * @return the name of the object.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 */
	String getName() throws XMLDBException {
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED, "Not implemented");
	}

	/**
	 * Provides a list of all services known to the collection.  If no
	 * services are known an empty list is returned.
	 * <p />
	 * In MonetDB/XQuery's case we return an empty list.
	 *
	 * @return An array of registered Service implementations.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection<br />
	 */
	Service[] getServices() throws XMLDBException {
		// I don't know about any services we support
		return(new Service[0]);
	}

	/**
	 * Returns a Service instance for the requested service name and
	 * version.  If no Service exists for those parameters a null value
	 * is returned.
	 *
	 * @param name Description of Parameter
	 * @param version Description of Parameter
	 * @return the Service instance or null if no Service could be found.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection<br />
	 */
	Service getService(String name, String version) throws XMLDBException {
		// I don't know about the service, so I return null in any case
		// (there is no compiler complaining about unused variables here)
		return(null);
	}

	/**
	 * Returns the parent collection for this collection or null if no
	 * parent collection exists.
	 *
	 * @return the parent Collection instance.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection<br />
	 */
	Collection getParentCollection() throws XMLDBException {
		// I do it quick 'n' dirty for now.  There exists no recursion,
		// so there is never a parent, and I don't care about whether
		// we're closed or not.
		return(null);
	}

	/**
	 * Returns the number of child collections under this Collection or
	 * 0 if no child collections exist.
	 *
	 * @return the number of child collections.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection<br />
	 */
	int getChildCollectionCount() throws XMLDBException {
		// again quick 'n' dirty (see above)
		return(0);
	}

	/**
	 * Returns a list of collection names naming all child collections
	 * of the current collection.  If no child collections exist an
	 * empty list is returned.
	 *
	 * @return an array containing collection names for all child
	 *         collections.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection<br />
	 */
	String[] listChildCollections() throws XMLDBException {
		// quick 'n' dirty! (see above)
		return(new String[0]);
	}

	/**
	 * Returns a Collection instance for the requested child collection
	 * if it exists.
	 *
	 * @param name the name of the child collection to retrieve.
	 * @return the requested child collection or null if it couldn't be found.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection<br />
	 */
	Collection getChildCollection(String name) throws XMLDBException {
		// we don't have children, so we always return null regardless
		// the input
		return(null);
	}

	/**
	 * Returns the number of resources currently stored in this collection or 0
	 * if the collection is empty.
	 *
	 * @return the number of resource in the collection.
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 *  <code>ErrorCodes.COLLECTION_CLOSED</code> if the <code>close</code> 
	 *  method has been called on the <code>Collection</code><br />
	 */
	int getResourceCount() throws XMLDBException;

	/**
	 * Returns a list of the ids for all resources stored in the collection.
	 *
	 * @return a string array containing the names for all 
	 *  <code>Resource</code>s in the collection.
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 *  <code>ErrorCodes.COLLECTION_CLOSED</code> if the <code>close</code> 
	 *  method has been called on the <code>Collection</code><br />
	 */
	String[] listResources() throws XMLDBException;

	/**
	 * Creates a new empty <code>Resource</code> with the provided id. 
	 * The type of <code>Resource</code>
	 * returned is determined by the <code>type</code> parameter. The XML:DB API currently 
	 * defines "XMLResource" and "BinaryResource" as valid resource types.
	 * The <code>id</code> provided must be unique within the scope of the 
	 * collection. If 
	 * <code>id</code> is null or its value is empty then an id is generated by   
	 * calling <code>createId()</code>. The
	 * <code>Resource</code> created is not stored to the database until 
	 * <code>storeResource()</code> is called.
	 *
	 * @param id the unique id to associate with the created <code>Resource</code>.
	 * @param type the <code>Resource</code> type to create.
	 * @return an empty <code>Resource</code> instance.    
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 *  <code>ErrorCodes.UNKNOWN_RESOURCE_TYPE</code> if the <code>type</code>
	 *   parameter is not a known <code>Resource</code> type.
	 *  <code>ErrorCodes.COLLECTION_CLOSED</code> if the <code>close</code> 
	 *  method has been called on the <code>Collection</code><br />
	 */
	Resource createResource(String id, String type) throws XMLDBException;

	/**
	 * Removes the <code>Resource</code> from the database.
	 *
	 * @param res the resource to remove.
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 *  <code>ErrorCodes.INVALID_RESOURCE</code> if the <code>Resource</code> is
	 *   not valid.<br />
	 *  <code>ErrorCodes.NO_SUCH_RESOURCE</code> if the <code>Resource</code> is
	 *   not known to this <code>Collection</code>.
	 *  <code>ErrorCodes.COLLECTION_CLOSED</code> if the <code>close</code> 
	 *  method has been called on the <code>Collection</code><br />
	 */
	void removeResource(Resource res) throws XMLDBException;

	/**
	 * Stores the provided resource into the database. If the resource does not
	 * already exist it will be created. If it does already exist it will be
	 * updated.
	 *
	 * @param res the resource to store in the database.
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 *  <code>ErrorCodes.INVALID_RESOURCE</code> if the <code>Resource</code> is
	 *   not valid.
	 *  <code>ErrorCodes.COLLECTION_CLOSED</code> if the <code>close</code> 
	 *  method has been called on the <code>Collection</code><br />
	 */
	void storeResource(Resource res) throws XMLDBException;

	/**
	 * Retrieves a <code>Resource</code> from the database. If the 
	 * <code>Resource</code> could not be
	 * located a null value will be returned.
	 *
	 * @param id the unique id for the requested resource.
	 * @return The retrieved <code>Resource</code> instance.
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />    
	 *  <code>ErrorCodes.COLLECTION_CLOSED</code> if the <code>close</code> 
	 *  method has been called on the <code>Collection</code><br />
	 */
	Resource getResource(String id) throws XMLDBException;

	/**
	 * Creates a new unique ID within the context of the <code>Collection</code>
	 *
	 * @return the created id as a string.
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 *  <code>ErrorCodes.COLLECTION_CLOSED</code> if the <code>close</code> 
	 *  method has been called on the <code>Collection</code><br />
	 */
	String createId() throws XMLDBException;

	/**
	 * Returns true if the  <code>Collection</code> is open false otherwise.
	 * Calling the <code>close</code> method on 
	 * <code>Collection</code> will result in <code>isOpen</code>
	 * returning false. It is not safe to use <code>Collection</code> instances
	 * that have been closed.
	 *
	 * @return true if the <code>Collection</code> is open, false otherwise.
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 */
	boolean isOpen() throws XMLDBException;

	/**
	 * Releases all resources consumed by the <code>Collection</code>. 
	 * The <code>close</code> method must
	 * always be called when use of a <code>Collection</code> is complete. It is
	 * not safe to use a  <code>Collection</code> after the <code>close</code>
	 * method has been called.
	 *
	 * @throws XMLDBException with expected error codes.<br />
	 *  <code>ErrorCodes.VENDOR_ERROR</code> for any vendor
	 *  specific errors that occur.<br />
	 */
	void close() throws XMLDBException;
}

