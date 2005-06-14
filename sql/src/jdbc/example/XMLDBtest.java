import org.xmldb.api.*;
import org.xmldb.api.base.*;
import org.xmldb.api.modules.*;

/**
 * Quick example demonstrating the XML:DB driver.
 *
 * To compile and run this example, MonetDB_XMLDB.jar, MonetDB_JDBC.jar
 * and xmldb.jar (from the lib dir) should be in the classpath.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class XMLDBtest {
	public static void main(String[] args) throws Exception {
		Class.forName("nl.cwi.monetdb.xmldb.base.MonetDBDatabase");
		try {
			Collection col = DatabaseManager.getCollection("xmldb:monetdb://localhost/demo", "monetdb", "monetdb"); 

			XQueryService xqs = (XQueryService)col.getService("XQueryService", "1");
			ResourceSet set = xqs.query("(<foo>1</foo>,<bar />)");
			ResourceIterator it = set.getIterator();
			while(it.hasMoreResources()) {
				Resource r = it.nextResource();
				System.out.println(r.getContent());
			}
			
		} catch (XMLDBException e) {
			e.printStackTrace();
		}
	}
}
