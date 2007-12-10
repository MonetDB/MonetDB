/**
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html

 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
 * the License for the specific language governing rights and
 * limitations under the License.

 * The Original Code is the MonetDB Database System.

 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
 * All Rights Reserved.
**/

package nl.cwi.monetdb.xquery.xrpc.api;

import java.util.*;
import javax.xml.namespace.*;

/**
 * This class implements <code>NamespaceContex</code>, which is needed
 * to evaluate an <code>XPath</code> expression on an XML document
 * containing namespaces.
 * This class provides functions to store and retrieve (prefix, uri)
 * mappings for Namespaces.
 * See the documentation of the interface <a
 * href="http://java.sun.com/j2se/1.5.0/docs/api/javax/xml/namespace/NamespaceContext.html">NamespaceContext</a>
 * for more information.
 *
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class NamespaceContextImpl implements NamespaceContext{
    private Map map;

    /**
     * Creates a <code>NamespaceContextImpl</code> object.
     */
    public NamespaceContextImpl(){
        map = new HashMap();
    }

    /**
     * Creates a <code>NamespaceContextImpl</code> object with an initial binding of
     * the given (<code>prefix</code>, <code>uri</code>).
     *
     * @param prefix Prefix of the Namespace
     * @param uri URI of the Namespace
     */
    public NamespaceContextImpl(String prefix, String uri){
        map = new HashMap();
        map.put(prefix, uri);
    }

    /**
     * Add a new binding of the given (<code>prefix</code>,
     * <code>uri</code>).
     *
     * @param prefix Prefix of the Namespace
     * @param uri URI of the Namespace
     */
    public void add(String prefix, String uri){
        map.put(prefix, uri);
    }

    /**
     * Get Namespace URI bound to the given <code>prefix</code> in the
     * current scope.
     *
     * @param prefix prefix to look up
     * @return Namespace URI bound to prefix in the current scope.
     */
    public String getNamespaceURI(String prefix){
        return (String) map.get(prefix);
    }

    /**
     * Get prefix bound to Namespace URI in the current scope.
     * To get all prefixes bound to a Namespace URI in the current
     * scope, use {@link #getPrefixes(String namespaceURI)}.
     *
     * @param namespaceURI URI of Namespace to lookup
     * @return prefix bound to Namespace URI in current context
     */
    public String getPrefix(String namespaceURI){
        String[] prefixes = (String[]) map.keySet().toArray(new String[0]);

        for (int i = 0; i < prefixes.length; i++) {
            if(((String)map.get(prefixes[i])).equals(namespaceURI))
                return prefixes[i];
        }
        return null;
    }

    /**
     * Get all prefixes bound to a Namespace URI in the current scope.
     * An <code>Iterator</code> over <code>String</code> elements is
     * returned in an arbitrary, <b>implementation dependent</b>, order.
     *
     * @param namespaceURI URI of Namespace to lookup
     * @return <code>Iterator</code> for all prefixed bound to the
     * Namespace URI in the current scope
     */
    public Iterator getPrefixes(String namespaceURI){
        List prefixes = new ArrayList();
        String[] keys = (String[]) map.keySet().toArray(new String[0]);

        for (int i = 0; i < keys.length; i++) {
            if(((String)map.get(keys[i])).equals(namespaceURI))
                prefixes.add(keys[i]);
        }
        return prefixes.iterator();
    }
}
