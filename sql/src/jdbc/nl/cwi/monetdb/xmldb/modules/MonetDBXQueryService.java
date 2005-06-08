package nl.cwi.monetdb.xmldb.modules;

import org.xmldb.api.base.*;
import org.xmldb.api.modules.*;


/**
 * An XQueryService is able to execute an XQuery query and return a
 * ResourceSet with the answers.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MonetDBXQueryService extends MonetDBConfigurable implements XQueryService {

	/**
	 * Constructs a new MonetDB Collection and initialises its
	 * knownService array.
	 */
	MonetDBXQuery(MonetDBCollection col) {
		// do something?
	}


	//== Interface org.xmldb.api.base.Service
	
	/**
	 * Returns the name associated with the Service instance.
	 *
	 * @return the name of the object.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getName() throws XMLDBException {
		// SERVICE_NAME is defined in the XQueryInterface
		return(SERVICE_NAME);
	}

	/**
	 * Gets the Version attribute of the Service object.
	 *
	 * @return the Version value.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public String getVersion() throws XMLDBException {
		// My best guess is 1
		return("1");
	}

	/**
	 * Sets the Collection attribute of the Service object.
	 *
	 * @param col The new Collection value
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public void setCollection(Collection col) throws XMLDBException {
		if (!(col instanceof MonetDBCollection)) throw
			new XMLDBException(ErrorCodes.VENDOR_ERROR, "Can only operate on MonetDBCollection objects");

		??? mycol = col; ???
	}

	//== end interface org.xmldb.api.base.Service

	//== Interface org.xmldb.api.modules.XQueryService

	/**
	 * Sets a namespace mapping in the internal namespace map used to
	 * evaluate queries.  If prefix is null or empty the default
	 * namespace is associated with the provided URI.  A null or empty
	 * uri results in an exception being thrown.
	 *
	 * @param prefix The prefix to set in the map. If prefix is empty or
	 *               null the default namespace will be associated with
	 *               the provided URI.
	 * @param uri The URI for the namespace to be associated with prefix
	 * @throws XMLDBException if something goes wrong
	 */
	public void setNamespace(String prefix, String uri) throws XMLDBException {
		if (uri == null || uri.trim().equals("")) throw
			new XMLDBException(ErrorCodes.INVALID_URI);

		// do something
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED);
	}

	/**
	 * Returns the URI string associated with prefix from the internal
	 * namespace map.  If prefix is null or empty the URI for the
	 * default namespace will be returned.  If a mapping for the prefix
	 * can not be found null is returned.
	 *
	 * @param prefix The prefix to retrieve from the namespace map.
	 * @return The URI associated with prefix
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public java.lang.String getNamespace(String prefix) throws XMLDBException {
		// do something
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "Not implemented, sorry");
	}

	/**
	 * Removes the namespace mapping associated with prefix from the
	 * internal namespace map.  If prefix is null or empty the mapping
	 * for the default namespace will be removed.
	 *
	 * @param prefix The prefix to remove from the namespace map.  If
	 *               prefix is null or empty the mapping for the default
	 *               namespace will be removed.
	 * @throws XMLDBException with expected error codes.<br />
	 *  ErrorCodes.VENDOR_ERROR for any vendor specific errors that
	 *  occur.
	 */
	public void removeNamespace(String prefix) throws XMLDBException {
		// do something
		throw new XMLDBException(ErrorCodes.VENDOR_ERROR, "Not implemented, sorry");
	}

	/**
	 * Clears all namespaces from the internal namespace map.
	 *
	 * @throws XMLDBException if something goes wrong
	 */
	public void clearNamespaces() throws XMLDBException {
		// do something
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED);
	}

	/**
	 * Executes the given query string and returns the result as a
	 * ResourceSet.
	 *
	 * @param query the XQuery query string
	 * @throws XMLDBException if something goes wrong
	 */
	public ResourceSet query(String query) throws XMLDBException {
		// this should do something like:
		return(new MonetDBResourceSet(stmt.executeQuery(query)));
	}

	/**
	 * Executes the given query agains the given resource.  This is the
	 * best documentation I can come up with, but I currently have no
	 * frikkin' idea what or how.
	 *
	 *
	 * @param id the ID of the resource to query against
	 * @param query the XQuery query to execute
	 * @throws XMLDBException if something goes wrong
	 */
	public ResourceSet queryResource(String id, String query) throws XMLDBException {
		// do something
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED, "Tell me what this should do!");
	}

	/**
	 * Compiles the specified XQuery and returns a handle to the
	 * compiled code, which can then be passed to
	 * execute(MonetDBCompiledExpression).  In the future, that is.
	 * Perhaps.  Maybe.
	 *
	 * @param query the XQuery query to execute
	 * @throws XMLDBException if something goes wrong
	 */
	public CompiledExpression compile(String query) throws XMLDBException {
		// do something
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED);
	}

	/**
	 * Execute a compiled XQuery.  The implementation should pass all
	 * namespaces and variables declared through XQueryService to the
	 * compiled XQuery code.
	 *
	 * @param expression a previously compiled expression to execute
	 * @throws XMLDBException is something goes wrong
	 */
	public ResourceSet execute(MonetDBCompiledExpression expression)
		throws XMLDBException
	{
		// TODO: this will cause compilation errors, as we don't have
		// this class yet.  Need to make a wrapper.
		
		// do something
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED);
	}

	/**
	 * Declare a global, external XQuery variable and assign a value to
	 * it.  The variable has the same status as a variable declare
	 * through the declare variable statement in the XQuery prolog.  The
	 * variable can be referenced inside the XQuery expression as
	 * $variable.  For example, if you declare a variable with
	 * <pre>declareVariable("name", "HAMLET");</pre>
	 * you may use the variable in an XQuery expression as follows:
	 * <pre>//SPEECH[SPEAKER=$name]</pre>
	 *
	 * @param qname a valid QName by which the variable is identified.
	 *              Any prefix should have been mapped to a namespace,
	 *              using setNamespace(String, String).  For example, if
	 *              a variable is called x:name, a prefix/namespace
	 *              mapping should have been defined for prefix x before
	 *              calling this method.
	 * @param initialValue the initial value, which is assigned to the
	 *                     variable
	 * @throws XMLDBException if something goes wrong
	 */
	public void declareVariable(String qname, Object initialValue)
		throws XMLDBException
	{
		// do something
		throw new XMLDBException(ErrorCodes.NOT_IMPLEMENTED);
	}

	/**
	 * Enable or disable XPath 1.0 compatibility mode.  In XPath 1.0
	 * compatibility mode, some XQuery expressions will behave
	 * different.  In particular, additional automatic type conversions
	 * will be applied to the operands of numeric operators.
	 *
	 * @param backwardsCompatible whether backwards compatability should
	 *                            be enabled or not
	 */
	public void setXPathCompatibility(boolean backwardsCompatible) {
		// what else can we do but ignoring this?
	}

	/**
	 * Sets the module load path.  This is probably very specific to a
	 * certain database implementation.  Can be some XQuery 'feature' as
	 * well, but I'm no XQuery expert at all.  At least Wolfgang Meier
	 * doesn't want to tell anything more either in his docs.
	 *
	 * @param path the path I guess
	 */
	public void setModuleLoadPath(String path) {
		// ignore is the best we can
	}
}

