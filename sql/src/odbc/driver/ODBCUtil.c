/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 * 
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
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
 * Postcondition: returns a newly allocated null terminated string or NULL.
 */
char *
dupODBCstring(SQLCHAR *inStr, SQLSMALLINT lenCode)
{
	if (inStr == NULL || lenCode == SQL_NULL_DATA) {
		/* no valid string is provided, so nothing to copy */
		return NULL;
	}

	if (lenCode == SQL_NTS) {
		/* its a Null Terminated String (NTS) */
		return (char *) strdup((char *) inStr);
	}
	if (lenCode >= 0) {
		/* String length is specified. It may not be Null Terminated. */
		/* need to copy lenCode chars and null terminate it */
		char *tmp = (char *) malloc((lenCode + 1) * sizeof(char));
		assert(tmp);
		strncpy(tmp, (char *) inStr, lenCode);
		tmp[lenCode] = '\0';   /* make it null terminated */
		return tmp;
	}

	/* All other values for lenCode are Invalid. Cannot copy it. */
	return NULL;
}
