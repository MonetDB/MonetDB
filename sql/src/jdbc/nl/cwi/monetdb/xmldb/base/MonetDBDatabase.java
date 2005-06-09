package nl.cwi.monetdb.xmldb.base;

import org.xmldb.api.base.*;
import java.sql.*;

/**
 * The Database class is an encapsulation of the database driver
 * functionality that is necessary to access an XML database.  Each
 * vendor must provide their own implementation of the Database
 * interface.  The implementation is registered with the DatabaseManager
 * to provide access to the resources of the XML database.
 * <p />
 * In general usage client applications should only access Database
 * implementations directly during initialization.
 * <p />
 * The MonetDB/XQuery Database class is currently a thin wrapper around
 * JDBC which does the actual communication with the database.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetDBDatabase extends MonetDBConfigurable implements Database {
	/**
	 * DEPRECATED.
	 * Returns the name associated with the Database instance.
	 *
	 * @return the name of the object.
	 * @throws XMLDBException because this method is deprecated and
	 * should not be used any more
	 */
	String getName() throws XMLDBException {
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "This method is deprecated, use getNames() instead");
	}
   
	/**
	 * Retrieves a Collection instance based on the URI provided in the
	 * uri parameter. The format of the URI is defined in the
	 * documentation for DatabaseManager.getCollection().
	 * <p />
	 * Authentication is handled via username and password however it is
	 * not required that the database support authentication.  Databases
	 * that do not support authentication MUST ignore the username and
	 * password if those provided are not null. 
	 * <p />
	 * Accepted URIs follow the same rules as for JDBC, apart from that
	 * they start with xmldb instead of jdbc.  An example url:
	 * <tt>xmldb:monetdb://localhost</tt>.
	 *
	 * @param uri the URI to use to locate the collection.
	 * @param password The password to use for authentication to the
	 *        database or null if the database does not support
	 *        authentication.
	 * @return A Collection instance for the requested collection or null
	 *         if the collection could not be found.
	 * @return The Collection instance
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.INVALID_URI if the URI is not in a valid format.<br />
	 *  ErrorCodes.PERMISSION_DENIED if the username and password were
	 *  not accepted by the database.
	 */
	Collection getCollection(String uri, String username, String password) 
		throws XMLDBException
	{
		Connection con;
		
		if (!acceptsURI(uri))
			throw new XMLDBException(ErrorCodes.INVALID_URI, "uri " + uri + " not valid");
		
		class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
		// We do a pretty simple hack here: we just cut off the xmldb
		// part and put jdbc for it instead.  Then it's up to the JDBC
		// driver to determine whether it can do something with it.
		try {
			con = DriverManager.getConnection(
					"jdbc" + uri.substring(4),
					username,
					password
			);
		} catch (SQLException e) {
			// Simply pass the message on.  We cannot determine easily
			// whether this is a permission denied message, so we just
			// use 'vendor error' with the SQL error message which is
			// usually descriptive enough to tell what's the problem
			throw new XMLDBException(ErrorCodes.VENDOR_ERROR, e.getMessage());
		}
		
		// TODO: create a Collection here and return it
		return(null);
	}

	/**
	 * acceptsURI determines whether this Database implementation can
	 * handle the given URI.  It should return true if the Database
	 * instance knows how to handle the URI and false otherwise.
	 * <p />
	 * This is a very lazy check, for it only checks whether the uri
	 * starts with <tt>xmldb:monetdb://</tt>.
	 *
	 * @param uri the URI to check for.
	 * @return true if the URI can be handled, false otherwise.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.<br />
	 *  ErrorCodes.INVALID_URI if the URI is not in a valid format.<br />
	 */
	boolean acceptsURI(String uri) throws XMLDBException {
		if (uri == null)
			throw new XMLDBException(ErrorCodes.INVALID_URI, "URI is null");

		return(uri.startsWith("xmldb:monetdb://"));
	}

	/**
	 * Returns the XML:DB API Conformance level for the implementation.
	 * This can be used by client programs to determine what
	 * functionality is available to them.
	 *
	 * @return the XML:DB API conformance level for this implementation.
	 */
	String getConformanceLevel() {
		// I have no clue what to put here, cannot find information on
		// it quickly too...
		return("We try to support the basics...");
	}
}

