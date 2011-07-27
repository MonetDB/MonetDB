/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

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
extern char *dupODBCstring(const SQLCHAR *inStr, size_t length);

/*
 * Utility macro to fix up args that represent an ODBC string.  If len
 * == SQL_NTS, the string is NULL-terminated, so set len accordingly;
 * if len == SQL_NULL_DATA, there is no data, so set str and len both
 * accordingly.  If str == NULL, set len to 0.
 * We can still make a distinction between str = "", len = 0 and str = NULL.
 */
#define fixODBCstring(str, len, lent, errfunc, hdl, ret)		\
	do {								\
		if (str == NULL)					\
			len = 0;					\
		if (len == SQL_NTS)					\
			len = (lent) (str ? strlen((char*)str) : 0);	\
		else if (len == SQL_NULL_DATA) {			\
			str = NULL;					\
			len = 0;					\
		} else if (len < 0) {					\
			/* Invalid string or buffer length */		\
			errfunc(hdl, "HY090", NULL, 0);			\
			ret;						\
		}							\
	} while (0)


/*
  Function to translate an ODBC SQL query to native format.
  The return value is a freshly allocated null-terminated string.
  For now this function just calls dupODBCstring.
*/
extern char *ODBCTranslateSQL(const SQLCHAR *query, size_t length, SQLUINTEGER noscan);

/* Utility macro to copy a string to an output argument.  In the ODBC
   API there are generally three arguments involved: the pointer to a
   buffer, the length of that buffer, and a pointer to where the
   actual string length is to be stored. */
#define copyString(str, strlen, buf, len, lenp, lent, errfunc, hdl, ret)	\
	do {								\
		lent _l;						\
		if ((len) < 0) {					\
			/* Invalid string or buffer length */		\
			errfunc((hdl), "HY090", NULL, 0);		\
			ret;						\
		}							\
		_l = (str) ? (lent) (strlen) : 0;			\
		if (buf)						\
			strncpy((char *) (buf), (str) ? (const char *) (str) : "", (len)); \
		if (lenp)						\
			*(lenp) = _l;					\
		if ((buf) == NULL || _l >= (len))			\
			/* String data, right-truncated */		\
			errfunc((hdl), "01004", NULL, 0);		\
	} while (0)

#ifdef WITH_WCHAR
extern SQLCHAR *ODBCwchar2utf8(const SQLWCHAR *s, SQLLEN length, char **errmsg);
extern char *ODBCutf82wchar(const SQLCHAR *s, SQLINTEGER length, SQLWCHAR *buf, SQLLEN buflen, SQLSMALLINT *buflenout);

#define fixWcharIn(ws, wsl, t, s, errfunc, hdl, exit)			\
	do {								\
		char *e;						\
		(s) = (t *) ODBCwchar2utf8((ws), (wsl), &e);		\
		if (e) {						\
			/* General error */				\
			assert((s) == NULL);				\
			errfunc((hdl),					\
				strcmp(e, "Memory allocation error") == 0 ? \
					"HY001" : "HY000", e, 0);	\
			exit;						\
		}							\
	} while (0)
#define fixWcharOut(r, s, sl, ws, wsl, wslp, cw, errfunc, hdl)		\
	do {								\
		if (SQL_SUCCEEDED(r)) {					\
			char *e = ODBCutf82wchar((s), (sl), (ws), (wsl) / (cw), &(sl)); \
			if (e) {					\
				/* General error */			\
				errfunc((hdl), "HY000", e, 0);		\
				(r) = SQL_ERROR;			\
			} else if ((sl) * (cw) >= (wsl)) {		\
				/* String data, right-truncated */	\
				errfunc((hdl), "01004", NULL, 0);	\
				(r) = SQL_SUCCESS_WITH_INFO;		\
			}						\
			if (wslp)					\
				*(wslp) = (sl) * (cw);			\
		}							\
		free(s);						\
	} while (0)
#endif /* WITH_WCHAR */

/* SQL_DESC_CONCISE_TYPE, SQL_DESC_DATETIME_INTERVAL_CODE, and
   SQL_DESC_TYPE are interdependent and setting one affects the other.
   Also, setting them affect other fields.  This is all encoded in
   this table.  If a field is equal to UNAFFECTED, it is (you guessed
   it) not affected. */
#define UNAFFECTED	(-1)

struct sql_types {
	int concise_type;
	int type;
	int code;
	int precision;
	int datetime_interval_precision;
	int length;
	int scale;
	int radix;
	int fixed;
};

extern struct sql_types ODBC_c_types[], ODBC_sql_types[];

#endif /* _H_ODBCUTIL */
