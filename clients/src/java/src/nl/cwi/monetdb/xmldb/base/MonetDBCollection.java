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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.xmldb.base;

import org.xmldb.api.base.*;
import java.sql.*;
import nl.cwi.monetdb.jdbc.*;
import nl.cwi.monetdb.xmldb.modules.*;


/**
 * A Collection represents a collection of Resources stored within an
 * XML database.  An XML database MAY expose collections as a
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
public class MonetDBCollection extends MonetDBConfigurable implements Collection {
	private Service[] serviceInstances;
	private final Connection jdbccon;

	/**
	 * Constructs a new MonetDB Collection and initialises its
	 * knownServices array.
	 *
	 * @param con a JDBC connection to a MonetDB database
	 */
	MonetDBCollection(MonetConnection con) {
		jdbccon = con;
		// initially fill the serviceInstances array
		serviceInstances = new Service[1];
		serviceInstances[0] = new MonetDBXQueryService();
	}
	
	/**
	 * Returns the name associated with the Collection instance.
	 *
	 * @return the name of the object.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getName() throws XMLDBException {
		return("MonetDBCollection");
	}

	/**
	 * Provides a list of all services known to the collection.  If no
	 * services are known an empty list is returned.
	 * <p />
	 * In MonetDB/XQuery's case we return a list with an XQueryService
	 * implementation.
	 *
	 * @return An array of registered Service implementations.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public Service[] getServices() throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);

		// We should return a list with all supported Services.
		return((Service[])(serviceInstances.clone()));
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
	 *  on the Collection
	 */
	public Service getService(String name, String version) throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// Iterate over all known Services.  Get their name and version
		// using getName() and getVersion and do the comparison.
		for (int i = 0; i < serviceInstances.length; i++) {
			if (serviceInstances[i].getName().equals(name) &&
					serviceInstances[i].getVersion().equals(version))
			{
				// use reflection to call the right constructor
				try {
					Class[] param = { MonetStatement.class, MonetDBCollection.class };
					Object[] args = new Object[2];
					args[0] = (MonetStatement)(jdbccon.createStatement());
					args[1] = this;
					return((Service)
						serviceInstances[i].getClass().getConstructor(param).newInstance(args));
				} catch (NoSuchMethodException e) {
					throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "Internal error: no suitable constructor for the requested service found!");
				} catch (Exception e) {
					// intentionally include the Exception name, this
					// includes: InstantiationException,
					// IllegalAccessException,
					// InvocationTargetException, SQLException
					throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.toString());
				}
			}
		}

		// finally, if not found, return null
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
	 *  on the Collection
	 */
	public Collection getParentCollection() throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// I do it quick 'n' dirty for now.  There exists no recursion,
		// so there is never a parent.
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
	 *  on the Collection
	 */
	public int getChildCollectionCount() throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
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
	 *  on the Collection
	 */
	public String[] listChildCollections() throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
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
	 *  on the Collection
	 */
	public Collection getChildCollection(String name) throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// we don't have children, so we always return null regardless
		// the input
		return(null);
	}

	/**
	 * Returns the number of resources currently stored in this
	 * collection or 0 if the collection is empty.
	 *
	 * @return the number of resource in the collection.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public int getResourceCount() throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// We cannot know upfront how many tuples there are to come
		// using JDBC.  I don't know...
		return(0);
	}

	/**
	 * Returns a list of the ids for all resources stored in the
	 * collection.
	 *
	 * @return a string array containing the names for all Resources in
	 * the collection.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public String[] listResources() throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);

		// somehow resources have IDs...  I'm affraid we have to take
		// the hash of the tuples here or something.
		return(new String[0]);
	}

	/**
	 * Creates a new empty Resource with the provided id.  The type of
	 * Resource returned is determined by the type parameter.  The
	 * XML:DB API currently defines "XMLResource" and "BinaryResource"
	 * as valid resource types.  The id provided must be unique within
	 * the scope of the collection.  If id is null or its value is empty
	 * then an id is generated by calling createId().  The Resource
	 * created is not stored to the database until storeResource() is
	 * called.
	 *
	 * @param id the unique id to associate with the created Resource.
	 * @param type the Resource type to create.
	 * @return an empty Resource instance.    
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.UNKNOWN_RESOURCE_TYPE if the type parameter is not a
	 *  known Resource type.
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public Resource createResource(String id, String type) throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// we don't have updateable resultsets (yet)
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "Operation not supported, sorry.");
	}

	/**
	 * Removes the Resource from the database.
	 *
	 * @param res the resource to remove.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.INVALID_RESOURCE if the Resource is not valid.<br />
	 *  ErrorCodes.NO_SUCH_RESOURCE if the Resource is not known to this
	 *  Collection.
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public void removeResource(Resource res) throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// we don't have updateable resultsets (yet)
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "Operation not supported, sorry.");
	}

	/**
	 * Stores the provided resource into the database. If the resource
	 * does not already exist it will be created. If it does already
	 * exist it will be updated.
	 *
	 * @param res the resource to store in the database.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.INVALID_RESOURCE if the Resource is not valid.
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public void storeResource(Resource res) throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// we don't have updateable resultsets (yet)
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "Operation not supported, sorry.");
	}

	/**
	 * Retrieves a Resource from the database. If the Resource could not
	 * be located a null value will be returned.
	 *
	 * @param id the unique id for the requested resource.
	 * @return The retrieved Resource instance.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />    
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public Resource getResource(String id) throws XMLDBException {
		// do something like return the row requested
		// currently: do nothing
		return(null);
	}

	/**
	 * Creates a new unique ID within the context of the Collection.
	 *
	 * @return the created id as a string.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.COLLECTION_CLOSED if the close method has been called
	 *  on the Collection
	 */
	public String createId() throws XMLDBException {
		if (!isOpen()) throw
			new XMLDBException(ErrorCodes.COLLECTION_CLOSED);
		
		// we don't have updateable resultsets (yet)
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "Operation not supported, sorry.");
	}

	/**
	 * Returns true if the Collection is open false otherwise.  Calling
	 * the close method on Collection will result in isOpen returning
	 * false. It is not safe to use Collection instances that have been
	 * closed.
	 *
	 * @return true if the Collection is open, false otherwise.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public boolean isOpen() throws XMLDBException {
		try {
			return(!jdbccon.isClosed());
		} catch (SQLException e) {
			// intentionally include the SQLException class name
			throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.toString());
		}
	}

	/**
	 * Releases all resources consumed by the Collection.  The close
	 * method must always be called when use of a Collection is
	 * complete. It is not safe to use a  Collection after the close
	 * method has been called.
	 *
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public void close() throws XMLDBException {
		try {
			jdbccon.close();
		} catch (SQLException e) {
			// intentionally include the SQLException class name
			throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.toString());
		}
	}
}

