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

package nl.cwi.monetdb.mcl.io;

import nl.cwi.monetdb.mcl.*;

/**
 * An MCLInputStream allows to conveniently read sentences of the
 * associated input stream.  MCL sentences are presented on the wire as
 * sequences of bytes prefixed with their length represented in an
 * 4-byte integer value.  It may be efficient to supply a
 * BufferedInputStream to the constructor of this class to increase
 * performance.
 *
 * @author Fabian Groffen <Fabian.Groffen>
 */
public class MCLIOException extends MCLException {
	public MCLIOException(String msg) {
		super(msg);
	}
}
