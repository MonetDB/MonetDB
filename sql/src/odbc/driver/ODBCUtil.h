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
			/* Invalid string or buffer length */		\
			errfunc(hdl, "HY090", NULL, 0);			\
			return SQL_ERROR;				\
		}							\
	} while (0)


/*
  Function to translate an ODBC SQL query to native format.
  The return value is a freshly allocated null-terminated string.
  For now this function just calls dupODBCstring.
*/
extern char *ODBCTranslateSQL(const SQLCHAR *query, size_t length);

/* Utility macro to copy a string to an output argument.  In the ODBC
   API there are generally three arguments involved: the pointer to a
   buffer, the length of that buffer, and a pointer to where the
   actual string length is to be stored. */
#define copyString(str, buf, len, lenp, errfunc, hdl)			\
	do {								\
		size_t _l;						\
		if ((len) < 0) {					\
			/* Invalid string or buffer length */		\
			errfunc((hdl), "HY090", NULL, 0);		\
			return SQL_ERROR;				\
		}							\
		_l = (str) ? strlen((char *) (str)) : 0;		\
		if (buf)						\
			strncpy((char *) (buf), (str) ? (char *) (str) : "", (len)); \
		if (lenp)						\
			*(lenp) = _l;					\
		if ((buf) == NULL || _l >= (size_t) (len))		\
			/* String data, right-truncated */		\
			errfunc((hdl), "01004", NULL, 0);		\
	} while (0)

#ifdef WITH_WCHAR
extern SQLCHAR *ODBCwchar2utf8(const SQLWCHAR *s, SQLINTEGER length,
			       char **errmsg);
extern char *ODBCutf82wchar(const SQLCHAR *s, SQLINTEGER length,
			    SQLWCHAR *buf, SQLINTEGER buflen,
			    SQLSMALLINT *buflenout);

#define fixWcharIn(ws, wsl, s, errfunc, hdl, exit)	\
	do {						\
		char *e;				\
		(s) = ODBCwchar2utf8((ws), (wsl), &e);	\
		if (e) {				\
			/* General error */		\
			errfunc((hdl), "HY000", e, 0);	\
			exit;				\
		}					\
	} while (0)
#define prepWcharOut(s, wsl)	 (s) = malloc((wsl) * 4)
#define fixWcharOut(r, s, sl, ws, wsl, wslp, cw, errfunc, hdl)		\
	do {								\
		if (SQL_SUCCEEDED(r)) {					\
			char *e = ODBCutf82wchar((s), (sl), (ws), (wsl), &(sl)); \
			if (e) {					\
				/* General error */			\
				errfunc((hdl), "HY000", e, 0);		\
				(r) = SQL_ERROR;			\
			} else if ((sl) >= (wsl)) {			\
				/* String data, right-truncated */	\
				errfunc((hdl), "01004", NULL, 0);	\
				(r) = SQL_SUCCESS_WITH_INFO;		\
			}						\
			if (wslp)					\
				*(wslp) = (sl) * (cw);			\
		}							\
		free(s);						\
	} while (0)
#endif	/* WITH_WCHAR */

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
