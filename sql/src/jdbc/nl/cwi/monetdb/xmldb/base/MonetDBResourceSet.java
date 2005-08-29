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
 * Portions created by CWI are Copyright (C) 1997-2005 CWI.
 * All Rights Reserved.
 */

package nl.cwi.monetdb.xmldb.base;

import org.xmldb.api.base.*;
import java.sql.*;
import java.util.*;
import nl.cwi.monetdb.jdbc.*;
import nl.cwi.monetdb.xmldb.base.*;
import nl.cwi.monetdb.xmldb.modules.*;

/**
 * ResourceSet is a container for a set of resources.  Generally a
 * ResourceSet is obtained as the result of a query.<br />
 * <br />
 * Because MonetDB/XQuery's XML:DB implementation is based on JDBC, the
 * implementation of this ResourceSet is based on a JDBC ResultSet.
 * Because JDBC does not provide the means to know upfront how many is
 * about to come -- it is based on iterator structures -- all Resources
 * (= tuples from the ResultSet) are fetched into a List, such that the
 * count will be known.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetDBResourceSet implements ResourceSet {
	private final List resources;
	private final MonetDBCollection collectionParent;
	
	/**
	 * Constructor which takes a ResultSet.  The ResultSet provides the
	 * base for this ResourceSet, where each tuple represents a
	 * Resource.
	 *
	 * @param rs a MonetResultSet containing the XML data
	 * @throws XMLDBException if a database error occurs
	 */
	public MonetDBResourceSet(MonetResultSet rs, MonetDBCollection parent)
		throws XMLDBException
	{
		resources = new ArrayList();
		collectionParent = parent;

		// read out results and fill the resources list
		try {
			while (rs.next()) {
				// we just *know* that there is just one column, and
				// that it should be what we're looking for: the result
				// of the query
				resources.add(new MonetDBXMLResource(rs.getString(1), collectionParent));
			}
			// we won't need the ResultSet any more, since we got all
			// its data
			rs.close();
		} catch (SQLException e) {
			// intentionally add exception name to error string
			throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.toString());
		}
	}

	/**
	 * Returns the Resource instance stored at the index specified by
	 * index.  If the underlying implementation uses a paging or
	 * streaming optimization for retrieving Resource instances,
	 * calling this method MAY result in a block until the requested
	 * Resource has been downloaded. <br />
	 * This implementation currently does not, which means that any
	 * Resource is immediately available.
	 * 
	 * @param index the index of the resource to retrieve, the first
	 *              result is at position 1
	 * @return the Resource instance
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.<br />
	 *  ErrorCodes.NO_SUCH_RESOURCE if the index is out of range for the
	 *  set.
	 */
	public Resource getResource(long index) throws XMLDBException {
		// walk through the list
		if (index < 1 || index > resources.size()) throw
			new XMLDBException(ErrorCodes.NO_SUCH_RESOURCE, "Resource index out of bounds: " + index);
		return((Resource)(resources.get((int)index - 1)));
	}

	/**
	 * Adds a Resource instance to the set.<br />
	 * <br />
	 * Note that although you are allowed to add a Resource, it is only
	 * for this ResourceSet, and not synced with the backend!
	 *
	 * @param res The Resource to add to the set.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public void addResource(Resource res) throws XMLDBException {
		synchronized(resources) {
			resources.add(res);
		}
	}

	/**
	 * Adds all Resource instances in the resourceSet to this set.<br />
	 * <br />
	 * Note that although you are allowed to add a Resource, it is only
	 * for this ResourceSet, and not synced with the backend!<br />
	 * This operation is *not* atomic: if halfway some Resource cannot
	 * be added, the previously inserted ones will stay.
	 *
	 * @param rSet the ResourceSet containing all the Resources to add
	 *             to the set
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public void addAll(ResourceSet rSet) throws XMLDBException {
		// just loop over all the Resources in the set and add them...
		ResourceIterator it = rSet.getIterator();
		while (it.hasMoreResources()) addResource(it.nextResource());
	}

	/**
	 * Removes the Resource located at index from the set.  The first
	 * Resource is located at index position 1.
	 *
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public void removeResource(long index) throws XMLDBException {
		// actually, we should trow an out of bounds if the long is
		// bigger than an int can hold
		synchronized(resources) {
			resources.remove((int)index - 1);
		}
	}

	/**
	 * Returns an iterator over all Resource instances stored in the
	 * set.
	 *
	 * @return a ResourceIterator over all the Resource instances in the
	 *         set
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public ResourceIterator getIterator() throws XMLDBException {
		// do it inline
		return(new ResourceIterator() {
			private int index = 0;

			/**
			 * Returns true as long as there are still more resources to
			 * be iterated.
			 *
			 * @return true if there are more resources to iterate,
			 *         false otherwise.
			 * @throws XMLDBException with expected error codes.<br />
			 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros
			 *  that occur.
			 */
			public boolean hasMoreResources() {
				return(index < resources.size());
			}

			/**
			 * Returns the next Resource instance in the iterator.
			 *
			 * @returns the next Resource instance in the iterator.
			 * @throws XMLDBException with expected error codes.<br />
			 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors
			 *  that occur.<br />
			 *  ErrorCodes.NO_SUCH_RESOURCE if the resource iterator is
			 *  empty or all resources have already been retrieved.
			 */
			public Resource nextResource() throws XMLDBException {
				synchronized(resources) {
					if (index >= resources.size()) throw
						new XMLDBException(ErrorCodes.NO_SUCH_RESOURCE, "no such resource with index: " + index);

					return((Resource)(resources.get(index++)));
				}
			}
		});
	}

	/**
	 * Returns a Resource containing an XML representation of all
	 * resources stored in the set.<br />
	 * <br />
	 * TODO: Specify the schema used for this 
	 * 
	 * @return A Resource instance containing an XML representation
	 *         of all set members.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public Resource getMembersAsResource() throws XMLDBException {
		String xml = "";

		ResourceIterator it = getIterator();
		while (it.hasMoreResources()) {
			xml += "<resource>" + it.nextResource().getContent().toString() + "</resource>";
		}
		return(new MonetDBXMLResource(xml, collectionParent));
	}

	/**
	 * Returns the number of resources contained in the set.  If the
	 * underlying implementation uses a paging or streaming optimization
	 * for retrieving Resource instances, calling this method MAY force
	 * the downloading of all set members before the size can be
	 * determined.<br />
	 * <br />
	 * This implementation currently discards any opportunity to
	 * optimise on this, and always fetches all Resources at once before
	 * allowing to retrieve them.
	 * 
	 * @return the number of Resource instances in the set
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public long getSize() throws XMLDBException {
		return((long)(resources.size()));
	}

	/**
	 * Removes all Resource instances from the set.
	 *
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public void clear() throws XMLDBException {
		resources.clear();
	}
}

