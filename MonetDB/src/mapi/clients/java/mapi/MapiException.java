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
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
 * All Rights Reserved.
 */

/*
This exception is thrown by Mapi indicating an error in the Monet/java
communication. 
MapiException can be constructed with or without an exception message.
*/

package mapi;

public
class MapiException extends Exception {
    public MapiException() {
        super();
    }

    public MapiException(String s) {
        super(s);
    }

    public String toString() {
        String message = getMessage();
        return (message != null) ? (message) : "";
    }
}

