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

package nl.cwi.monetdb.xquery.util;

import java.util.*;
import javax.xml.namespace.*;

/**
 * @author Ying Zhang <Y.Zhang@cwi.nl>
 * @version 0.1
 */

public class NamespaceContextImpl implements NamespaceContext{
    private Map map;

    public NamespaceContextImpl(){
        map = new HashMap();
    }

    public NamespaceContextImpl(String prefix, String uri){
        map = new HashMap();
        map.put(prefix, uri);
    }

    public void add(String prefix, String uri){
        map.put(prefix, uri);
    }

    public String getNamespaceURI(String prefix){
        return (String) map.get(prefix);
    }

    public String getPrefix(String namespaceURI){
        String[] prefixes = (String[]) map.keySet().toArray(new String[0]);

        for (int i = 0; i < prefixes.length; i++) {
            if(((String)map.get(prefixes[i])).equals(namespaceURI))
                return prefixes[i];
        }
        return null;
    }

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
