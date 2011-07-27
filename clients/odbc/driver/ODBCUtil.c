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
#include <float.h>


#ifdef WIN32
/* Windows seems to need this */
BOOL WINAPI
DllMain(HINSTANCE hinstDLL, DWORD reason, LPVOID reserved)
{
#ifdef ODBCDEBUG
	ODBCLOG("DllMain %ld\n", (long) reason);
#endif
	(void) hinstDLL;
	(void) reason;
	(void) reserved;

	return TRUE;
}
#endif

/*
 * Utility function to duplicate an ODBC string (with a length
 * specified, may not be null terminated) to a normal C string (null
 * terminated).
 *
 * Precondition: inStr != NULL
 * Postcondition: returns a newly allocated null terminated string.
 */
char *
dupODBCstring(const SQLCHAR *inStr, size_t length)
{
	char *tmp = (char *) malloc((length + 1) * sizeof(char));

	assert(tmp);
	strncpy(tmp, (const char *) inStr, length);
	tmp[length] = '\0';	/* make it null terminated */
	return tmp;
}

#ifdef WITH_WCHAR
/* Conversion to and from SQLWCHAR */
static int utf8chkmsk[] = {
	0x0000007f,
	0x00000780,
	0x0000f800,
	0x001f0000,
	0x03e00000,
	0x7c000000
};

#define LEAD_OFFSET		(0xD800 - (0x10000 >> 10))
#define SURROGATE_OFFSET	(0x10000 - (0xD800 << 10) - 0xDC00)

/* Convert a SQLWCHAR (UTF-16 encoded string) to UTF-8.  On success,
   clears the location pointed to by errmsg and returns NULL or a
   newly allocated buffer.  On error, assigns a string with an error
   message to the location pointed to by errmsg and returns NULL.
   The first two arguments describe the input string in the normal
   ODBC fashion.
*/
SQLCHAR *
ODBCwchar2utf8(const SQLWCHAR *s, SQLLEN length, char **errmsg)
{
	const SQLWCHAR *s1, *e;
	unsigned long c;
	SQLCHAR *buf, *p;
	int l, n;

	if (errmsg)
		*errmsg = NULL;
	if (s == NULL)
		return NULL;
	if (length == SQL_NTS)
		for (s1 = s, length = 0; *s1; s1++)
			length++;
	else if (length == SQL_NULL_DATA)
		return NULL;
	else if (length < 0) {
		if (errmsg)
			*errmsg = "Invalid length parameter";
		return NULL;
	}
	e = s + length;
	/* count necessary length */
	l = 1;			/* space for NULL byte */
	for (s1 = s; s1 < e; s1++) {
		c = *s1;
		if (0xD800 <= c && c <= 0xDBFF) {
			/* high surrogate, must be followed by low surrogate */
			s1++;
			if (s1 >= e || *s1 < 0xDC00 || *s1 > 0xDFFF) {
				if (errmsg)
					*errmsg = "High surrogate not followed by low surrogate";
				return NULL;
			}
			c = (c << 10) + *s1 + SURROGATE_OFFSET;
		} else if (0xDC00 <= c && c <= 0xDFFF) {
			/* low surrogate--illegal */
			if (errmsg)
				*errmsg = "Low surrogate not preceded by high surrogate";
			return NULL;
		}
		for (n = 5; n > 0; n--)
			if (c & utf8chkmsk[n])
				break;
		l += n + 1;
	}
	/* convert */
	buf = (SQLCHAR *) malloc(l);
	if (buf == NULL) {
		if (errmsg)
			*errmsg = "Memory allocation error";
		return NULL;
	}
	for (s1 = s, p = buf; s1 < e; s1++) {
		c = *s1;
		if (0xD800 <= c && c <= 0xDBFF) {
			/* high surrogate followed by low surrogate */
			s1++;
			c = (c << 10) + *s1 + SURROGATE_OFFSET;
		}
		for (n = 5; n > 0; n--)
			if (c & utf8chkmsk[n])
				break;
		if (n == 0)
			*p++ = (SQLCHAR) c;
		else {
			*p++ = (SQLCHAR) (((c >> (n * 6)) | (0x1F80 >> n)) & 0xFF);
			while (--n >= 0)
				*p++ = (SQLCHAR) (((c >> (n * 6)) & 0x3F) | 0x80);
		}
	}
	*p = 0;
	return buf;
}

/* Convert a UTF-8 encoded string to UTF-16 (SQLWCHAR).  On success
   returns NULL, on error returns a string with an error message.  The
   first two arguments describe the input, the last three arguments
   describe the output, both in the normal ODBC fashion. */
char *
ODBCutf82wchar(const SQLCHAR *s,
	       SQLINTEGER length,
	       SQLWCHAR * buf,
	       SQLLEN buflen,
	       SQLSMALLINT *buflenout)
{
	SQLWCHAR *p;
	const SQLCHAR *e;
	int m, n;
	unsigned int c;
	SQLSMALLINT len = 0;

	if (s == NULL || length == SQL_NULL_DATA) {
		if (buf && buflen > 0)
			*buf = 0;
		if (buflenout)
			*buflenout = 0;
		return NULL;
	}
	if (length == SQL_NTS)
		length = (SQLINTEGER) strlen((const char *) s);
	else if (length < 0)
		return "Invalid length parameter";

	if (buf == NULL)
		buflen = 0;

	for (p = buf, e = s + length; s < e; ) {
		c = *s++;
		if ((c & 0x80) != 0) {
			for (n = 0, m = 0x40; c & m; n++, m >>= 1)
				;
			/* n now is number of 10xxxxxx bytes that
			 * should follow */
			if (n == 0 || n >= 6)
				return "Illegal UTF-8 sequence";
			if (s + n > e)
				return "Truncated UTF-8 sequence";
			c &= ~(0xFFC0) >> n;
			while (--n >= 0) {
				c <<= 6;
				c |= *s++ & 0x3F;
			}
		}
		if ((c & 0xF8) == 0xD8) {
			/* UTF-8 encoded high or low surrogate */
			return "Illegal code point";
		}
		if (c > 0x10FFFF) {
			/* cannot encode as UTF-16 */
			return "Codepoint too large to be representable in UTF-16";
		}
		if (c <= 0xFFFF) {
			if (--buflen > 0 && p != NULL)
				*p++ = c;
			len++;
		} else {
			if ((buflen -= 2) > 0 && p != NULL) {
				*p++ = LEAD_OFFSET + (c >> 10);
				*p++ = 0xDC00 + (c & 0x3FF);
			}
			len += 2;
		}
	}
	if (p != NULL)
		*p = 0;
	if (buflenout)
		*buflenout = len;
	return NULL;
}
#endif /* WITH_WCHAR */

/*
 * Translate an ODBC-compatible query to one that the SQL server
 * understands.
 *
 * Precondition: query != NULL
 * Postcondition: returns a newly allocated null terminated strings.
 */
/*
  Escape sequences:
  {d 'yyyy-mm-dd'}
  {t 'hh:mm:ss'}
  {ts 'yyyy-mm-dd hh:mm:ss[.f...]'}
  {fn scalar-function}
  {escape 'escape-character'}
  {oj outer-join}
  where outer-join is:
	table-reference {LEFT|RIGHT|FULL} OUTER JOIN
	{table-reference | outer-join} ON search condition
  {[?=]call procedure-name[([parameter][,[parameter]]...)]}
 */

char *
ODBCTranslateSQL(const SQLCHAR *query, size_t length, SQLUINTEGER noscan)
{
	char *nquery;
	char *p;
	char buf[512];

	nquery = dupODBCstring(query, length);
	if (noscan)
		return nquery;
	p = nquery;
	while ((p = strchr(p, '{')) != NULL) {
		char *q = p;
		unsigned yr, mt, dy, hr, mn, sc;
		unsigned long fr = 0;
		int n, pr;

		if (sscanf(p, "{ts '%u-%u-%u %u:%u:%u%n", &yr, &mt, &dy, &hr, &mn, &sc, &n) >= 6) {
			p += n;
			pr = 0;
			if (*p == '.') {
				char *e;

				p++;
				fr = strtoul(p, &e, 10);
				if (e > p) {
					pr = (int) (e - p);
					p = e;
				} else
					goto skip;
			}
			if (*p++ != '\'' || *p++ != '}') {
				p--;
				goto skip;
			}
			if (pr > 0)
				snprintf(buf, sizeof(buf), "TIMESTAMP '%u-%u-%u %u:%u:%u.%0*lu'",
					 yr, mt, dy, hr, mn, sc, pr, fr);
			else
				snprintf(buf, sizeof(buf), "TIMESTAMP '%u-%u-%u %u:%u:%u'",
					 yr, mt, dy, hr, mn, sc);
			n = (int) (q - nquery);
			pr = (int) (p - q);
			q = malloc(length - pr + strlen(buf) + 1);
			sprintf(q, "%.*s%s%s", n, nquery, buf, p);
			free(nquery);
			nquery = q;
			p = q + n;
		} else if (sscanf(p, "{t '%u:%u:%u%n", &hr, &mn, &sc, &n) >= 3) {
			p += n;
			pr = 0;
			if (*p == '.') {
				char *e;

				p++;
				fr = strtoul(p, &e, 10);
				if (e > p) {
					pr = (int) (e - p);
					p = e;
				} else
					goto skip;
			}
			if (*p++ != '\'' || *p++ != '}') {
				p--;
				goto skip;
			}
			if (pr > 0)
				snprintf(buf, sizeof(buf), "TIME '%u:%u:%u.%0*lu'",
					 hr, mn, sc, pr, fr);
			else
				snprintf(buf, sizeof(buf), "TIME '%u:%u:%u'",
					 hr, mn, sc);
			n = (int) (q - nquery);
			pr = (int) (p - q);
			q = malloc(length - pr + strlen(buf) + 1);
			sprintf(q, "%.*s%s%s", n, nquery, buf, p);
			free(nquery);
			nquery = q;
			p = q + n;
		} else if (sscanf(p, "{d '%u-%u-%u'}%n", &yr, &mt, &dy, &n) >= 3) {
			p += n;
			pr = 0;
			snprintf(buf, sizeof(buf), "DATE '%u-%u-%u'",
					 yr, mt, dy);
			n = (int) (q - nquery);
			pr = (int) (p - q);
			q = malloc(length - pr + strlen(buf) + 1);
			sprintf(q, "%.*s%s%s", n, nquery, buf, p);
			free(nquery);
			nquery = q;
			p = q + n;
		}
	  skip:
		p++;
	}
	return nquery;
}

struct sql_types ODBC_sql_types[] = {
	{SQL_CHAR, SQL_CHAR, 0, 0, UNAFFECTED, 1, UNAFFECTED, 0, SQL_FALSE},
	{SQL_VARCHAR, SQL_VARCHAR, 0, 0, UNAFFECTED, 1, UNAFFECTED, 0, SQL_FALSE},
	{SQL_LONGVARCHAR, SQL_LONGVARCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_WCHAR, SQL_WCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_WVARCHAR, SQL_WVARCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_WLONGVARCHAR, SQL_WLONGVARCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_DECIMAL, SQL_DECIMAL, 0, 17, UNAFFECTED, UNAFFECTED, 0, 10, SQL_TRUE},
	{SQL_NUMERIC, SQL_NUMERIC, 0, 17, UNAFFECTED, UNAFFECTED, 0, 10, SQL_TRUE},
	{SQL_BIT, SQL_BIT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_TINYINT, SQL_TINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_SMALLINT, SQL_SMALLINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_INTEGER, SQL_INTEGER, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_BIGINT, SQL_BIGINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_REAL, SQL_REAL, 0, FLT_MANT_DIG, UNAFFECTED, UNAFFECTED, UNAFFECTED, 2, SQL_FALSE},
	{SQL_FLOAT, SQL_FLOAT, 0, DBL_MANT_DIG, UNAFFECTED, UNAFFECTED, UNAFFECTED, 2, SQL_FALSE},
	{SQL_DOUBLE, SQL_DOUBLE, 0, DBL_MANT_DIG, UNAFFECTED, UNAFFECTED, UNAFFECTED, 2, SQL_FALSE},
	{SQL_BINARY, SQL_BINARY, 0, 0, UNAFFECTED, 1, UNAFFECTED, 0, SQL_FALSE},
	{SQL_VARBINARY, SQL_VARBINARY, 0, 0, UNAFFECTED, 1, UNAFFECTED, 0, SQL_FALSE},
	{SQL_LONGVARBINARY, SQL_LONGVARBINARY, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_GUID, SQL_GUID, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_TYPE_DATE, SQL_DATETIME, SQL_CODE_DATE, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_TYPE_TIME, SQL_DATETIME, SQL_CODE_TIME, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_TYPE_TIMESTAMP, SQL_DATETIME, SQL_CODE_TIMESTAMP, 6, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_MONTH, SQL_INTERVAL, SQL_CODE_MONTH, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_YEAR, SQL_INTERVAL, SQL_CODE_YEAR, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_YEAR_TO_MONTH, SQL_INTERVAL, SQL_CODE_YEAR_TO_MONTH, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_DAY, SQL_INTERVAL, SQL_CODE_DAY, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_HOUR, SQL_INTERVAL, SQL_CODE_HOUR, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_MINUTE, SQL_INTERVAL, SQL_CODE_MINUTE, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_SECOND, SQL_INTERVAL, SQL_CODE_SECOND, 6, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL, SQL_CODE_DAY_TO_HOUR, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_DAY_TO_MINUTE, SQL_INTERVAL, SQL_CODE_DAY_TO_MINUTE, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL, SQL_CODE_DAY_TO_SECOND, 6, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL, SQL_CODE_HOUR_TO_MINUTE, 0, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL, SQL_CODE_HOUR_TO_SECOND, 6, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL, SQL_CODE_MINUTE_TO_SECOND, 6, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{0, 0, 0, 0, 0, 0, 0, 0, 0},	/* sentinel */
};

struct sql_types ODBC_c_types[] = {
	{SQL_C_CHAR, SQL_C_CHAR, 0, 0, UNAFFECTED, 1, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_WCHAR, SQL_C_WCHAR, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_BIT, SQL_C_BIT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_NUMERIC, SQL_C_NUMERIC, 0, 17, UNAFFECTED, UNAFFECTED, 0, 10, SQL_TRUE},
	{SQL_C_STINYINT, SQL_C_STINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_UTINYINT, SQL_C_UTINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_TINYINT, SQL_C_TINYINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_SBIGINT, SQL_C_SBIGINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_UBIGINT, SQL_C_UBIGINT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_SSHORT, SQL_C_SSHORT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_USHORT, SQL_C_USHORT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_SHORT, SQL_C_SHORT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_SLONG, SQL_C_SLONG, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_ULONG, SQL_C_ULONG, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_LONG, SQL_C_LONG, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 10, SQL_TRUE},
	{SQL_C_FLOAT, SQL_C_FLOAT, 0, FLT_MANT_DIG, UNAFFECTED, UNAFFECTED, UNAFFECTED, 2, SQL_FALSE},
	{SQL_C_DOUBLE, SQL_C_DOUBLE, 0, DBL_MANT_DIG, UNAFFECTED, UNAFFECTED, UNAFFECTED, 2, SQL_FALSE},
	{SQL_C_BINARY, SQL_C_BINARY, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_TYPE_DATE, SQL_DATETIME, SQL_CODE_DATE, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_TYPE_TIME, SQL_DATETIME, SQL_CODE_TIME, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_TYPE_TIMESTAMP, SQL_DATETIME, SQL_CODE_TIMESTAMP, 6, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_MONTH, SQL_INTERVAL, SQL_CODE_MONTH, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_YEAR, SQL_INTERVAL, SQL_CODE_YEAR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_YEAR_TO_MONTH, SQL_INTERVAL, SQL_CODE_YEAR_TO_MONTH, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_DAY, SQL_INTERVAL, SQL_CODE_DAY, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_HOUR, SQL_INTERVAL, SQL_CODE_HOUR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_MINUTE, SQL_INTERVAL, SQL_CODE_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_SECOND, SQL_INTERVAL, SQL_CODE_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL, SQL_CODE_DAY_TO_HOUR, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_DAY_TO_MINUTE, SQL_INTERVAL, SQL_CODE_DAY_TO_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL, SQL_CODE_DAY_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL, SQL_CODE_HOUR_TO_MINUTE, UNAFFECTED, 2, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_HOUR_TO_SECOND, SQL_INTERVAL, SQL_CODE_HOUR_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_INTERVAL_MINUTE_TO_SECOND, SQL_INTERVAL, SQL_CODE_MINUTE_TO_SECOND, UNAFFECTED, 6, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_GUID, SQL_C_GUID, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{SQL_C_DEFAULT, SQL_C_DEFAULT, 0, UNAFFECTED, UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, SQL_FALSE},
	{0, 0, 0, 0, 0, 0, 0, 0, 0},	/* sentinel */
};

#ifdef ODBCDEBUG
#if !defined(__STDC_VERSION__) || __STDC_VERSION__ < 199901
#include <stdarg.h>

void
ODBCLOG(const char *fmt, ...)
{
	va_list ap;
	char *s = getenv("ODBCDEBUG");

	va_start(ap, fmt);
	if (s && *s) {
		FILE *f;

		f = fopen(s, "a");
		if (f) {
			vfprintf(f, fmt, ap);
			fclose(f);
		} else
			vfprintf(stderr, fmt, ap);
	}
	va_end(ap);
}
#endif
#endif
