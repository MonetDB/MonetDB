/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************
 * ODBCUtil.c
 *
 * Description:
 * This file contains utility functions for
 * the ODBC driver implementation.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCUtil.h"


/*
 * Utility function to duplicate an ODBC string (with a
 * length/special code specified, may be non null terminated)
 * to a normal C string (null terminated) or NULL.
 *
 * Precondition: none
 * Postcondition: returns a new allocated null terminated string or NULL.
 */
char * copyODBCstr2Cstr(SQLCHAR * inStr, SQLSMALLINT lenCode)
{
	if (inStr == NULL) {
		/* no valid string is provided, so nothing to copy */
		return NULL;
	}

	if (lenCode == SQL_NTS) {
		/* its a Null Terminated String (NTS) */
		return (char *) strdup(inStr);
	}
	if (lenCode >= 0) {
		/* String length is specified. It may not be Null Terminated. */
		if (inStr[lenCode] == '\0') {
			/* it is null terminated, so we can use strdup */
			return (char *) strdup(inStr);
		} else {
			/* need to copy lenCode chars and null terminate it */
			char * tmp = (char *) malloc((lenCode +1) * sizeof(char));
			assert(tmp);
			strncpy(tmp, inStr, lenCode);
			tmp[lenCode] = '\0';	/* make it null terminated */
			return tmp;
		}
	}
	if (lenCode == SQL_NULL_DATA) {
		/* special code which states that the string is NULL */
		return NULL;
	}

	/* All other values for lenCode are Invalid. Cannot copy it. */
	return NULL;
}

