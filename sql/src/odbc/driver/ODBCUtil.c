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
 * Utility function to duplicate an ODBC string (with a length
 * specified, may not be null terminated) to a normal C string (null
 * terminated).
 *
 * Precondition: inStr != NULL
 * Postcondition: returns a newly allocated null terminated string.
 */
char *
dupODBCstring(SQLCHAR *inStr, size_t length)
{
	char *tmp = (char *) malloc((length + 1) * sizeof(char));

	assert(tmp);
	strncpy(tmp, (char *) inStr, length);
	tmp[length] = '\0';   /* make it null terminated */
	return tmp;
}
