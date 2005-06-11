package nl.cwi.monetdb.xmldb.base;

import org.xmldb.api.base.*;
import java.sql.*;

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
	
	/**
	 * Constructor which takes a ResultSet.  The ResultSet provides the
	 * base for this ResourceSet, where each tuple represents a
	 * Resource.
	 *
	 * @param rs a MonetResultSet containing the XML data
	 * @throws XMLDBException if a database error occurs
	 */
	public MonetDBResourceSet(MonetResultSet rs) throws XMLDBException {
		resources = new ArrayList();

		// read out results and fill the resources list
		try {
			while (rs.next()) {
				// we just *know* that there is just one column, and
				// that it should be what we're looking for: the result
				// of the query
				resources.add(new MonetDBXMLResource(rs.getString(1), this));
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
	public Resource getResource(int index) throws XMLDBException {
		// walk through the list
		if (index < 1 || index > resources.size()) throw
			new XMLDBException(ErrorCodes.NO_SUCH_RESOURCE, "Resource index out of bounds: " + index);
		return((Resource)(resources.get(index - 1)));
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
	 * @return removes the Resource located at index from the set.
	 *
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific erros that
	 *  occur.
	 */
	public void removeResource(int index) throws XMLDBException {
		synchronized(resources) {
			resources.remove(index - 1);
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

					return(resource.get(index - 1));
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
		return(new MonetDBXMLResource(xml, this));
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
	public int getSize() throws XMLDBException {
		return(resources.size());
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

