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
 * ODBCUtil.h
 *
 * Description:
 * This file contains ODBC driver utility function prototypes.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************/

#ifndef _H_ODBCUTIL
#define _H_ODBCUTIL

#include "ODBCGlobal.h"


/*
 * Utility function to duplicate an ODBC string (with a length
 * specified, may be non null terminated) to a normal C string (null
 * terminated).
 *
 * Precondition: inStr != NULL
 * Postcondition: returns a newly allocated null terminated string
 */
char *dupODBCstring(SQLCHAR *inStr, size_t length);

/*
 * Utility macro to fix up args that represent an ODBC string.  If len
 * == SQL_NTS, the string is NULL-terminated, so set len accordingly;
 * if len == SQL_NULL_DATA, there is no data, so set str and len both
 * accordingly.  If str == NULL, set len to 0.
 * We can still make a distinction between str = "", len = 0 and str = NULL.
 */
#define fixODBCstring(str, len, errfunc, hdl)				\
	do {								\
		if (str == NULL)					\
			len = 0;					\
		if (len == SQL_NTS)					\
			len = str ? strlen((char*)str) : 0;		\
		else if (len == SQL_NULL_DATA) {			\
			str = NULL;					\
			len = 0;					\
		} else if (len < 0) {					\
			/* HY090: Invalid string or buffer length */	\
			errfunc(hdl, "HY090", NULL, 0);			\
			return SQL_ERROR;				\
		}							\
	} while (0)


#endif /* _H_ODBCUTIL */
