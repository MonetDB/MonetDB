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

package nl.cwi.monetdb.mcl;

import java.util.*;
import java.lang.reflect.Array; // for dealing with the anonymous columns

/**
 * An MCLResultSet is a container for tabular data.  An MCLServer
 * instance uses this oject to generate a HeaderMessage and DataMessages
 * as appropriately.  An MCLResultSet can store a number of 'columns'
 * and their metadata.  Each column stored in an MCLResultSet must have
 * an equal number of values (tuples).
 *
 * Note: this class is the equivalent of the rsbox in MonetDB/Five.
 *
 * @author Fabian Groffen <Fabian.Groffen@cwi.nl>
 */
public class MCLResultSet {
	private boolean isValid;
	private List columns;

	/**
	 * Creates a new MCLResultSet that is in intially empty.  An empty
	 * MCLResultSet is not valid when being supplied to an MCLServer.
	 */
	public MCLResultSet() {
		isValid = false;
		columns = new ArrayList();
	}

	/**
	 * Adds the given Object array as column to this MCLResultSet.
	 *
	 * @param column the column to add
	 */
	public void addColumn(Object[] column) {
		columns.add(column);
	}
	
	/**
	 * Checks and marks whether this MCLResultSet is valid.  An
	 * MCLResultSet is considered to be valid if:
	 * <ul>
	 * <li>there is at least one column</li>
	 * <li>all columns are of equal length</li>
	 * </ul>
	 * Note that if this method is called on an already valid
	 * MCLResultSet, this method returns directly.
	 *
	 * @throws MCLException if one of the conditions described above is
	 *         not met
	 */
	public void setValid() throws MCLException {
		if (isValid) return;
		if (columns.size() == 0) throw
			new MCLException("This MCLResultSet has no columns yet");
		int lastLength = -1, thisLength;
		for (Iterator it = columns.iterator(); it.hasNext(); ) {
			thisLength = Array.getLength(it.next());
			if (lastLength == -1) lastLength = thisLength;
			if (thisLength != lastLength) throw
				new MCLException("Not all columns are of equal length " +
						"(" + lastLength + "/" + thisLength + ")");
		}
		isValid = true;
	}
}
