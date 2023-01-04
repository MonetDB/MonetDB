/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCUtil.h"
#include "ODBCDbc.h"
#include <float.h>


#ifdef WIN32
/* Windows seems to need this */
BOOL WINAPI
DllMain(HINSTANCE hinstDLL, DWORD reason, LPVOID reserved)
{
#ifdef ODBCDEBUG
	ODBCLOG("DllMain %ld (MonetDB %s)\n", (long) reason, MONETDB_VERSION);
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

	if (tmp == NULL)
		return NULL;
	strcpy_len(tmp, (const char *) inStr, length + 1);
	return tmp;
}

/* Convert a SQLWCHAR (UTF-16 encoded string) to UTF-8.  On success,
   clears the location pointed to by errmsg and returns NULL or a
   newly allocated buffer.  On error, assigns a string with an error
   message to the location pointed to by errmsg and returns NULL.
   The first two arguments describe the input string in the normal
   ODBC fashion.
*/
SQLCHAR *
ODBCwchar2utf8(const SQLWCHAR *src, SQLLEN length, const char **errmsg)
{
	size_t i = 0;
	SQLLEN j = 0;
	uint32_t c;
	SQLCHAR *dest;

	if (errmsg)
		*errmsg = NULL;
	if (src == NULL || length == SQL_NULL_DATA)
		return NULL;
	if (length == SQL_NTS) {
		/* a very large (positive) number that fits in SQLLEN */
		length = (SQLLEN) (~(SQLULEN)0 >> 1);
	} else if (length < 0) {
		if (errmsg)
			*errmsg = "Invalid length parameter";
		return NULL;
	}
	if (src[j] == 0xFEFF)
		j++;
	while (j < length && src[j]) {
		if (src[j] <= 0x7F) {
			i += 1;
		} else if (src[j] <= 0x7FF) {
			i += 2;
		} else if (
#if SIZEOF_SQLWCHAR == 2
			(src[j] & 0xFC00) != 0xD800
#else
			src[j] <= 0xFFFF
#endif
			) {
			if ((src[j] & 0xF800) == 0xD800) {
				if (errmsg)
					*errmsg = "Illegal surrogate";
				return NULL;
			}
			i += 3;
		} else {
#if SIZEOF_SQLWCHAR == 2
			/* (src[j] & 0xFC00) == 0xD800, i.e. high surrogate */
			if ((src[j+1] & 0xFC00) != 0xDC00) {
				if (errmsg)
					*errmsg = "Illegal surrogate";
				return NULL;
			}
			j++;
#else
			c = src[j+0];
			if (c > 0x10FFFF || (c & 0x1FF800) == 0xD800) {
				if (errmsg)
					*errmsg = "Illegal wide character value";
				return NULL;
			}
#endif
			i += 4;
		}
		j++;
	}
	length = j;	/* figured out the real length (might not change) */
	dest = malloc((i + 1) * sizeof(SQLCHAR));
	if (dest == NULL)
		return NULL;
	i = 0;
	j = 0;
	if (src[j] == 0xFEFF)
		j++;
	while (j < length) {
		if (src[j] <= 0x7F) {
			dest[i++] = (SQLCHAR) src[j];
		} else if (src[j] <= 0x7FF) {
			dest[i++] = 0xC0 | (src[j] >> 6);
			dest[i++] = 0x80 | (src[j] & 0x3F);
		} else if (
#if SIZEOF_SQLWCHAR == 2
			(src[j] & 0xFC00) != 0xD800
#else
			src[j] <= 0xFFFF
#endif
			) {
			dest[i++] = 0xE0 | (src[j] >> 12);
			dest[i++] = 0x80 | ((src[j] >> 6) & 0x3F);
			dest[i++] = 0x80 | (src[j] & 0x3F);
		} else {
#if SIZEOF_SQLWCHAR == 2
			c = ((src[j+0] & 0x03FF) + 0x40) << 10
				| (src[j+1] & 0x03FF);
			j++;
#else
			c = src[j+0];
#endif
			dest[i++] = 0xF0 | (c >> 18);
			dest[i++] = 0x80 | ((c >> 12) & 0x3F);
			dest[i++] = 0x80 | ((c >> 6) & 0x3F);
			dest[i++] = 0x80 | (c & 0x3F);
		}
		j++;
	}
	dest[i] = 0;
	return dest;
}

/* Convert a UTF-8 encoded string to UTF-16 (SQLWCHAR).  On success
   returns NULL, on error returns a string with an error message.  The
   first two arguments describe the input, the next three arguments
   describe the output, both in the normal ODBC fashion.
   The last argument is the count of the number of input bytes
   actually converted to the output. */
const char *
ODBCutf82wchar(const SQLCHAR *src,
	       SQLINTEGER length,
	       SQLWCHAR *buf,
	       SQLLEN buflen,
	       SQLSMALLINT *buflenout,
	       size_t *consumed)
{
	SQLLEN i = 0;
	SQLINTEGER j = 0;
	uint32_t c;

	if (buf == NULL)
		buflen = 0;
	else if (buflen == 0)
		buf = NULL;

	if (src == NULL || length == SQL_NULL_DATA) {
		if (buflen > 0)
			buf[0] = 0;
		if (buflenout)
			*buflenout = 0;
		if (consumed)
			*consumed = 0;
		return NULL;
	}
	if (length == SQL_NTS)
		length = (SQLINTEGER) (~(SQLUINTEGER)0 >> 1);
	else if (length < 0)
		return "Invalid length parameter";

	while (j < length && i + 1 < buflen && src[j]) {
		if ((src[j+0] & 0x80) == 0) {
			buf[i++] = src[j+0];
			j += 1;
		} else if (j + 1 < length
			   && (src[j+0] & 0xE0) == 0xC0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+0] & 0x1E) != 0) {
			buf[i++] = (src[j+0] & 0x1F) << 6
				| (src[j+1] & 0x3F);
			j += 2;
		} else if (j + 2 < length
			   && (src[j+0] & 0xF0) == 0xE0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && ((src[j+0] & 0x0F) != 0
			       || (src[j+1] & 0x20) != 0)) {
			buf[i++] = (src[j+0] & 0x0F) << 12
				| (src[j+1] & 0x3F) << 6
				| (src[j+2] & 0x3F);
			j += 3;
		} else if (j + 3 < length
			   && (src[j+0] & 0xF8) == 0xF0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && (src[j+3] & 0xC0) == 0x80
			   && ((src[j+0] & 0x07) != 0
			       || (src[j+1] & 0x30) != 0)) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
			if (c > 0x10FFFF || (c & 0x1FF800) == 0x00D800)
				return "Illegal code point";
#if SIZEOF_SQLWCHAR == 2
				if (i + 2 >= buflen)
					break;
				buf[i++] = 0xD800 | ((c - 0x10000) >> 10);
				buf[i++] = 0xDC00 | (c & 0x3FF);
#else
				buf[i++] = c;
#endif
			j += 4;
		} else {
			return "Illegal code point";
		}
	}
	if (buflen > 0)
		buf[i] = 0;
	if (consumed)
		*consumed = (size_t) j;
	while (j < length && src[j]) {
		i++;
		if ((src[j+0] & 0x80) == 0) {
			j += 1;
		} else if (j + 1 < length
			   && (src[j+0] & 0xE0) == 0xC0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+0] & 0x1E) != 0) {
			j += 2;
		} else if (j + 2 < length
			   && (src[j+0] & 0xF0) == 0xE0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && ((src[j+0] & 0x0F) != 0
			       || (src[j+1] & 0x20) != 0)) {
			j += 3;
		} else if (j + 3 < length
			   && (src[j+0] & 0xF8) == 0xF0
			   && (src[j+1] & 0xC0) == 0x80
			   && (src[j+2] & 0xC0) == 0x80
			   && (src[j+3] & 0xC0) == 0x80
			   && ((src[j+0] & 0x07) != 0
			       || (src[j+1] & 0x30) != 0)) {
			c = (src[j+0] & 0x07) << 18
				| (src[j+1] & 0x3F) << 12
				| (src[j+2] & 0x3F) << 6
				| (src[j+3] & 0x3F);
			if (c > 0x10FFFF || (c & 0x1FF800) == 0x00D800)
				return "Illegal code point";
#if SIZEOF_SQLWCHAR == 2
			i++;
#endif
			j += 4;
		} else {
			return "Illegal code point";
		}
	}
	if (buflenout)
		*buflenout = (SQLSMALLINT) i;
	return NULL;
}

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
  {interval ...}
  {guid 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX'}
  {fn scalar-function}
  {escape 'escape-character'}
  {oj outer-join}
  where outer-join is:
	table-reference {LEFT|RIGHT|FULL} OUTER JOIN
	{table-reference | outer-join} ON search condition
  {[?=]call procedure-name[([parameter][,[parameter]]...)]}
 */

static struct scalars {
	const char *name;
	int nargs;
	const char *repl;
} scalars[] = {
	{
		.name = "abs",
		.nargs = 1,
		.repl = "sys.\"abs\"",
	},
	{
		.name = "acos",
		.nargs = 1,
		.repl = "sys.\"acos\"",
	},
	{
		.name = "ascii",
		.nargs = 1,
		.repl = "sys.\"ascii\"",
	},
	{
		.name = "asin",
		.nargs = 1,
		.repl = "sys.\"asin\"",
	},
	{
		.name = "atan",
		.nargs = 1,
		.repl = "sys.\"atan\"",
	},
	{
		.name = "atan2",
		.nargs = 2,
		.repl = "sys.\"atan\"", /* note: not atan2 */
	},
	{
		.name = "bit_length",
		.nargs = 1,
		.repl = NULL,
	},
	{
		.name = "ceiling",
		.nargs = 1,
		.repl = "sys.\"ceiling\"",
	},
	{
		.name = "char",
		.nargs = 1,
		.repl = "sys.\"code\"",
	},
	{
		.name = "character_length",
		.nargs = 1,
		.repl = "sys.\"character_length\"",
	},
	{
		.name = "char_length",
		.nargs = 1,
		.repl = "sys.\"char_length\"",
	},
	{
		.name = "concat",
		.nargs = 2,
		.repl = "sys.\"concat\"",
	},
	{
		/* second argument is an ODBC type name that also gets
		 * translated.  Note: convert is implemented in the
		 * parser */
		.name = "convert",
		.nargs = 2,
		.repl = NULL,
	},
	{
		.name = "cos",
		.nargs = 1,
		.repl = "sys.\"cos\"",
	},
	{
		.name = "cot",
		.nargs = 1,
		.repl = "sys.\"cot\"",
	},
	{
		.name = "curdate",
		.nargs = 0,
		.repl = "sys.\"curdate\"",
	},
	{
		.name = "current_date",
		.nargs = 0,
		.repl = "sys.\"current_date\"",
	},
	{
		.name = "current_time",
		.nargs = 0,
		.repl = "sys.\"current_time\"",
	},
	{
		.name = "current_time",
		.nargs = 1,
		.repl = NULL,
	},
	{
		.name = "current_timestamp",
		.nargs = 0,
		.repl = "sys.\"current_timestamp\"",
	},
	{
		.name = "current_timestamp",
		.nargs = 1,
		.repl = NULL,
	},
	{
		.name = "curtime",
		.nargs = 0,
		.repl = "sys.\"curtime\"",
	},
	{
		.name = "database",
		.nargs = 0,
		.repl = NULL,
	},
	{
		.name = "dayname",
		.nargs = 1,
		.repl = NULL,
	},
	{
		.name = "dayofmonth",
		.nargs = 1,
		.repl = "sys.\"dayofmonth\"",
	},
	{
		.name = "dayofweek",
		.nargs = 1,
		.repl = "sys.\"dayofweek\"",
	},
	{
		.name = "dayofyear",
		.nargs = 1,
		.repl = "sys.\"dayofyear\"",
	},
	{
		.name = "degrees",
		.nargs = 1,
		.repl = "sys.\"degrees\"",
	},
	{
		.name = "difference",
		.nargs = 2,
		.repl = "sys.\"difference\"",
	},
	{
		.name = "exp",
		.nargs = 1,
		.repl = "sys.\"exp\"",
	},
	{
		.name = "extract",
		.nargs = 1,
		.repl = "EXTRACT", /* include "X FROM " in argument */
	},
	{
		.name = "floor",
		.nargs = 1,
		.repl = "sys.\"floor\"",
	},
	{
		.name = "hour",
		.nargs = 1,
		.repl = "sys.\"hour\"",
	},
	{
		.name = "ifnull",
		.nargs = 2,
		.repl = "sys.\"coalesce\"",
	},
	{
		.name = "insert",
		.nargs = 4,
		.repl = "sys.\"insert\"",
	},
	{
		.name = "lcase",
		.nargs = 1,
		.repl = "sys.\"lcase\"",
	},
	{
		.name = "left",
		.nargs = 2,
		.repl = "sys.\"left\"",
	},
	{
		.name = "length",
		.nargs = 1,
		.repl = "sys.\"length\"",
	},
	{
		.name = "locate",
		.nargs = 2,
		.repl = "sys.\"locate\"",
	},
	{
		.name = "locate",
		.nargs = 3,
		.repl = "sys.\"locate\"",
	},
	{
		.name = "log10",
		.nargs = 1,
		.repl = "sys.\"log10\"",
	},
	{
		.name = "log",
		.nargs = 1,
		.repl = "sys.\"log\"",
	},
	{
		.name = "ltrim",
		.nargs = 1,
		.repl = "sys.\"ltrim\"",
	},
	{
		.name = "minute",
		.nargs = 1,
		.repl = "sys.\"minute\"",
	},
	{
		.name = "mod",
		.nargs = 2,
		.repl = "sys.\"mod\"",
	},
	{
		.name = "month",
		.nargs = 1,
		.repl = "sys.\"month\"",
	},
	{
		.name = "monthname",
		.nargs = 1,
		.repl = NULL,
	},
	{
		.name = "now",
		.nargs = 0,
		.repl = "sys.\"now\"",
	},
	{
		.name = "octet_length",
		.nargs = 1,
		.repl = "sys.\"octet_length\"",
	},
	{
		.name = "pi",
		.nargs = 0,
		.repl = "sys.\"pi\"",
	},
	{
		.name = "position",
		.nargs = 1,
		 /* includes " IN str" in first argument. Note:
		  * POSITION is implemented in the parser. */
		.repl = "position",
	},
	{
		.name = "power",
		.nargs = 2,
		.repl = "sys.\"power\"",
	},
	{
		.name = "quarter",
		.nargs = 1,
		.repl = "sys.\"quarter\"",
	},
	{
		.name = "radians",
		.nargs = 1,
		.repl = "sys.\"radians\"",
	},
	{
		.name = "rand",
		.nargs = 0,
		.repl = "sys.\"rand\"",
	},
	{
		.name = "rand",
		.nargs = 1,
		.repl = "sys.\"rand\"",
	},
	{
		.name = "repeat",
		.nargs = 2,
		.repl = "sys.\"repeat\"",
	},
	{
		.name = "replace",
		.nargs = 3,
		.repl = "sys.\"replace\"",
	},
	{
		.name = "right",
		.nargs = 2,
		.repl = "sys.\"right\"",
	},
	{
		.name = "round",
		.nargs = 2,
		.repl = "sys.\"round\"",
	},
	{
		.name = "rtrim",
		.nargs = 1,
		.repl = "sys.\"rtrim\"",
	},
	{
		.name = "second",
		.nargs = 1,
		.repl = "sys.\"second\"",
	},
	{
		.name = "sign",
		.nargs = 1,
		.repl = "sys.\"sign\"",
	},
	{
		.name = "sin",
		.nargs = 1,
		.repl = "sys.\"sin\"",
	},
	{
		.name = "soundex",
		.nargs = 1,
		.repl = "sys.\"soundex\"",
	},
	{
		.name = "space",
		.nargs = 1,
		.repl = "sys.\"space\"",
	},
	{
		.name = "sqrt",
		.nargs = 1,
		.repl = "sys.\"sqrt\"",
	},
	{
		.name = "substring",
		.nargs = 3,
		.repl = "sys.\"substring\"",
	},
	{
		.name = "tan",
		.nargs = 1,
		.repl = "sys.\"tan\"",
	},
	{
		.name = "timestampadd",
		.nargs = 3,
		.repl = NULL,
	},
	{
		.name = "timestampdiff",
		.nargs = 3,
		.repl = NULL,
	},
	{
		.name = "truncate",
		.nargs = 2,
		.repl = "sys.\"ms_trunc\"",
	},
	{
		.name = "ucase",
		.nargs = 1,
		.repl = "sys.\"ucase\"",
	},
	{
		.name = "user",
		.nargs = 0,
		.repl = NULL,
	},
	{
		.name = "week",
		.nargs = 1,
		.repl = "sys.\"week\"",
	},
	{
		.name = "year",
		.nargs = 1,
		.repl = "sys.\"year\"",
	},
	{
		0		/* sentinel */
	},
};

static struct convert {
	const char *odbc;
	const char *server;
} convert[] = {
	{
		.odbc =  "SQL_BIGINT",
		.server = "bigint",
	},
	{
		.odbc =  "SQL_BINARY",
		.server = "binary large object",
	},
	{
		.odbc =  "SQL_BIT",
		.server = "boolean",
	},
	{
		.odbc =  "SQL_CHAR",
		.server = "character",
	},
	{
		.odbc =  "SQL_DATE",
		.server = "date",
	},
	{
		.odbc = "SQL_DECIMAL",
		.server = "decimal(18,7)",
	},
	{
		.odbc =  "SQL_DOUBLE",
		.server = "double",
	},
	{
		.odbc =  "SQL_FLOAT",
		.server = "float",
	},
	{
		.odbc =  "SQL_GUID",
		.server = "uuid",
	},
	{
		.odbc =  "SQL_HUGEINT",
		.server = "hugeint",
	},
	{
		.odbc =  "SQL_INTEGER",
		.server = "integer",
	},
	{
		.odbc =  "SQL_INTERVAL_DAY_TO_HOUR",
		.server = "interval day to hour",
	},
	{
		.odbc =  "SQL_INTERVAL_DAY_TO_MINUTE",
		.server = "interval day to minute",
	},
	{
		.odbc =  "SQL_INTERVAL_DAY_TO_SECOND",
		.server = "interval day to second",
	},
	{
		.odbc =  "SQL_INTERVAL_DAY",
		.server = "interval day",
	},
	{
		.odbc =  "SQL_INTERVAL_HOUR_TO_MINUTE",
		.server = "interval hour to minute",
	},
	{
		.odbc =  "SQL_INTERVAL_HOUR_TO_SECOND",
		.server = "interval hour to second",
	},
	{
		.odbc =  "SQL_INTERVAL_HOUR",
		.server = "interval hour",
	},
	{
		.odbc =  "SQL_INTERVAL_MINUTE_TO_SECOND",
		.server = "interval minute to second",
	},
	{
		.odbc =  "SQL_INTERVAL_MINUTE",
		.server = "interval minute",
	},
	{
		.odbc =  "SQL_INTERVAL_MONTH",
		.server = "interval month",
	},
	{
		.odbc =  "SQL_INTERVAL_SECOND",
		.server = "interval second",
	},
	{
		.odbc =  "SQL_INTERVAL_YEAR_TO_MONTH",
		.server = "interval year to month",
	},
	{
		.odbc =  "SQL_INTERVAL_YEAR",
		.server = "interval year",
	},
	{
		.odbc =  "SQL_LONGVARBINARY",
		.server = "binary large object",
	},
	{
		.odbc =  "SQL_LONGVARCHAR",
		.server = "character large object",
	},
	{
		.odbc = "SQL_NUMERIC",
		.server = "numeric(18,7)",
	},
	{
		.odbc =  "SQL_REAL",
		.server = "real",
	},
	{
		.odbc =  "SQL_SMALLINT",
		.server = "smallint",
	},
	{
		.odbc =  "SQL_TIMESTAMP",
		.server = "timestamp",
	},
	{
		.odbc =  "SQL_TIME",
		.server = "time",
	},
	{
		.odbc =  "SQL_TINYINT",
		.server = "tinyint",
	},
	{
		.odbc =  "SQL_VARBINARY",
		.server = "binary large object",
	},
	{
		.odbc =  "SQL_VARCHAR",
		.server = "character varying",
	},
	{
		.odbc =  "SQL_WCHAR",
		.server = "character",
	},
	{
		.odbc =  "SQL_WLONGVARCHAR",
		.server = "character large object",
	},
	{
		.odbc =  "SQL_WVARCHAR",
		.server = "character varying",
	},
	{
		0		/* sentinel */
	},
};

char *
ODBCTranslateSQL(ODBCDbc *dbc, const SQLCHAR *query, size_t length, SQLULEN noscan)
{
	/* we only need to read limited amounts of data into these
	 * buffers (timestamp, interval, function name), so 128 bytes is
	 * plenty long enough */
	char buf[128], buf2[128];

	if (noscan != SQL_NOSCAN_ON) {
		char *nquery;
		bool quoted = false, rawstring = false, dquoted = false;

		for (size_t i = 0; i < length; i++) {
			if (quoted && query[i] == '\\') {
				i++;
				continue;
			}
			if (quoted || rawstring) {
				if (query[i] == '\'')
					quoted = rawstring = false;
				continue;
			}
			if (dquoted) {
				if (query[i] == '"')
					dquoted = false;
				continue;
			}
			if (query[i] == '\'') {
				if (dbc->raw_strings ?
				    (i > 0 &&
				     query[i - 1] != 'e' &&
				     query[i - 1] != 'E') :
				    (i > 0 &&
				     (query[i - 1] == 'r' ||
				      query[i - 1] == 'R')))
					rawstring = true;
				else
					quoted = true;
				continue;
			}
			if (query[i] == '"') {
				dquoted = true;
				continue;
			}
			if (query[i] != '{')
				continue;
			size_t n = 0;
			if (sscanf((const char *) query + i, "{ ts '%127[0-9:. -]' }%zn", buf, &n) >= 1 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(rest);
				nquery = malloc(i + len + 13);
				snprintf(nquery, i + len + 13,
					 "%.*sTIMESTAMP '%s'%s",
					 (int) i, query, buf, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ t '%127[0-9:]' }%zn", buf, &n) >= 1 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(rest);
				nquery = malloc(i + len + 8);
				snprintf(nquery, i + len + 8,
					 "%.*sTIME '%s'%s",
					 (int) i, query, buf, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ d '%127[0-9-]' }%zn", buf, &n) >= 1 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(rest);
				nquery = malloc(i + len + 8);
				snprintf(nquery, i + len + 8,
					 "%.*sDATE '%s'%s",
					 (int) i, query, buf, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ guid '%127[0-9a-fA-F-]' }%zn", buf, &n) >= 1 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(rest);
				nquery = malloc(i + len + 8);
				snprintf(nquery, i + len + 8,
					 "%.*sUUID '%s'%s",
					 (int) i, query, buf, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ escape '%127[^']' }%zn", buf, &n) >= 1 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(rest);
				nquery = malloc(i + len + 10);
				snprintf(nquery, i + len + 10,
					 "%.*sESCAPE '%s'%s",
					 (int) i, query, buf, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ interval '%127[^']' %127[a-zA-Z ] }%zn", buf, buf2, &n) >= 2 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(buf2) + strlen(rest);
				nquery = malloc(i + len + 14);
				snprintf(nquery, i + len + 14,
					 "%.*sINTERVAL '%s' %s %s",
					 (int) i, query, buf, buf2, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ interval + '%127[^']' %127[a-zA-Z ] }%zn", buf, buf2, &n) >= 2 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(buf2) + strlen(rest);
				nquery = malloc(i + len + 15);
				snprintf(nquery, i + len + 15,
					 "%.*sINTERVAL +'%s' %s %s",
					 (int) i, query, buf, buf2, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ interval - '%127[^']' %127[a-zA-Z ] }%zn", buf, buf2, &n) >= 2 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t len = strlen(buf) + strlen(buf2) + strlen(rest);
				nquery = malloc(i + len + 15);
				snprintf(nquery, i + len + 15,
					 "%.*sINTERVAL -'%s' %s %s",
					 (int) i, query, buf, buf2, rest);
				free(rest);
				return nquery;
			}
			if (sscanf((const char *) query + i, "{ oj %zn", &n) >= 0 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				for (size_t j = 0; rest[j]; j++) {
					if (quoted && rest[j] == '\\') {
						j++;
						continue;
					}
					if (quoted || rawstring) {
						if (rest[j] == '\'')
							quoted = rawstring = false;
						continue;
					}
					if (dquoted) {
						if (rest[j] == '"')
							dquoted = false;
						continue;
					}
					if (rest[j] == '\'') {
						if (dbc->raw_strings ?
						    (j > 0 &&
						     rest[j - 1] != 'e' &&
						     rest[j - 1] != 'E') :
						    (j > 0 &&
						     (rest[j - 1] == 'r' ||
						      rest[j - 1] == 'R')))
							rawstring = true;
						else
							quoted = true;
						continue;
					}
					if (rest[j] == '"') {
						dquoted = true;
						continue;
					}
					if (rest[j] == '}') {
						size_t len = strlen(rest);
						nquery = malloc(i + len + 2);
						snprintf(nquery, i + len + 2,
							 "%.*s %.*s %s",
							 (int) i, query,
							 (int) j, rest,
							 rest + j + 1);
						free(rest);
						return nquery;
					}
				}
				free(rest);
				continue;
			}
			if (sscanf((const char *) query + i, "{ call %zn", &n) >= 0 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				for (size_t j = 0; rest[j]; j++) {
					if (quoted && rest[j] == '\\') {
						j++;
						continue;
					}
					if (quoted || rawstring) {
						if (rest[j] == '\'')
							quoted = rawstring = false;
						continue;
					}
					if (dquoted) {
						if (rest[j] == '"')
							dquoted = false;
						continue;
					}
					if (rest[j] == '\'') {
						if (dbc->raw_strings ?
						    (j > 0 &&
						     rest[j - 1] != 'e' &&
						     rest[j - 1] != 'E') :
						    (j > 0 &&
						     (rest[j - 1] == 'r' ||
						      rest[j - 1] == 'R')))
							rawstring = true;
						else
							quoted = true;
						continue;
					}
					if (rest[j] == '"') {
						dquoted = true;
						continue;
					}
					if (rest[j] == '}') {
						size_t len = strlen(rest);
						nquery = malloc(i + len + 6);
						snprintf(nquery, i + len + 6,
							 "%.*sCALL %.*s %s",
							 (int) i, query,
							 (int) j, rest,
							 rest + j + 1);
						free(rest);
						return nquery;
					}
				}
				free(rest);
				continue;
			}
			if (sscanf((const char *) query + i, "{ fn %127[a-zA-Z0-9_] ( %zn", buf, &n) >= 1 && n > 0) {
				char *rest = ODBCTranslateSQL(dbc, query + i + n, length - i - n, noscan);
				size_t arglen = 0;
				size_t lastarg = 0;
				int nargs = 0;
				int nparen = 1;
				bool seenarg = false;

				for (size_t j = 0; rest[j] && nargs < 5; j++) {
					if (quoted && rest[j] == '\\') {
						j++;
						continue;
					}
					if (quoted || rawstring) {
						if (rest[j] == '\'')
							quoted = rawstring = false;
						continue;
					}
					if (dquoted) {
						if (rest[j] == '"')
							dquoted = false;
						continue;
					}
					if (rest[j] == '\'') {
						seenarg = true;
						if (dbc->raw_strings ?
						    (j > 0 &&
						     rest[j - 1] != 'e' &&
						     rest[j - 1] != 'E') :
						    (j > 0 &&
						     (rest[j - 1] == 'r' ||
						      rest[j - 1] == 'R')))
							rawstring = true;
						else
							quoted = true;
						continue;
					}
					if (rest[j] == '"') {
						seenarg = true;
						dquoted = true;
						continue;
					}
					if (rest[j] == '(') {
						seenarg = true;
						nparen++;
						continue;
					}
					if (nparen > 1) {
						if (rest[j] == ')')
							nparen--;
						continue;
					}
					if (rest[j] == ',') {
						if (!seenarg)
							break;
						nargs++;
						seenarg = false;
						continue;
					}
					if (rest[j] == ')') {
						if (--nparen < 0)
							break;
						arglen = j;
						if (seenarg)
							nargs++;
						else if (nargs > 0)
							break;
						seenarg = false;
						continue;
					}
					if (rest[j] == '}') {
						if (nparen != 0 || seenarg)
							break;
						for (struct scalars *func = scalars; func->name; func++) {
							if (strcasecmp(func->name, buf) == 0 && func->nargs == nargs) {
								const char *repl = func->repl;
								const char *repl2 = "";
								const char *repl3 = "";
								const char *p1 = "(";
								const char *p2 = ")";
								const char *quote = "";
								size_t repl3len = 0;
								if (repl == NULL) {
									if (strcmp(func->name, "user") == 0) {
										repl = dbc->uid;
										p1 = p2 = "";
										quote = "'";
									} else if (strcmp(func->name, "database") == 0) {
										repl = dbc->dbname;
										p1 = p2 = "";
										quote = "'";
									} else if (strcmp(func->name, "convert") == 0) {
										repl = "convert";
										for (struct convert *c = convert; c->odbc; c++) {
											if (strncasecmp(rest + lastarg, c->odbc, strlen(c->odbc)) == 0) {
												repl2 = c->server;
												repl3len = arglen - lastarg - strlen(c->odbc);
												repl3 = rest + lastarg + strlen(c->odbc);
												arglen = lastarg;
												break;
											}
										}
									}
									if (repl == NULL)
										break;
								}
								size_t l = i + strlen(repl) + 2 + arglen + strlen(rest) - j + 1 + strlen(repl2) + repl3len;
								nquery = malloc(l);
								snprintf(nquery, l, "%.*s%s%s%s%s%.*s%s%.*s%s%s", (int) i, query, quote, repl, quote, p1, (int) arglen, rest, repl2, (int) repl3len, repl3, p2, rest + j + 1);
								free(rest);
								return nquery;
							}
						}
						break;
					}
					if (!seenarg && !isspace((signed char) rest[j])) {
						lastarg = j;
						seenarg = true;
					}
				}
				free(rest);
				continue;
			}
		}
	}
	return dupODBCstring(query, length);
}

char *
ODBCParseOA(const char *tab, const char *col, const char *arg, size_t len)
{
	size_t i;
	char *res;
	const char *s;

	/* count length, counting ' and \ double */
	for (i = 0, s = arg; s < arg + len; i++, s++) {
		if (*s == '\'' || *s == '\\')
			i++;
	}
	i += strlen(tab) + strlen(col) + 10; /* ""."" = '' */
	res = malloc(i + 1);
	if (res == NULL)
		return NULL;
	snprintf(res, i, "\"%s\".\"%s\" = '", tab, col);
	for (i = strlen(res), s = arg; s < arg + len; s++) {
		if (*s == '\'' || *s == '\\')
			res[i++] = *s;
		res[i++] = *s;
	}
	res[i++] = '\'';
	res[i] = 0;
	return res;
}

char *
ODBCParsePV(const char *tab, const char *col, const char *arg, size_t len, const ODBCDbc *dbc)
{
	size_t i;
	char *res;
	const char *s;

	/* count length, counting ' and \ double */
	for (i = 0, s = arg; s < arg + len; i++, s++) {
		if (*s == '\'' || *s == '\\')
			i++;
	}
	i += strlen(tab) + strlen(col) + 25; /* ""."" like '' escape r'\' */
	res = malloc(i + 1);
	if (res == NULL)
		return NULL;
	snprintf(res, i, "\"%s\".\"%s\" like '", tab, col);
	for (i = strlen(res), s = arg; s < arg + len; s++) {
		if (*s == '\'' || *s == '\\')
			res[i++] = *s;
		res[i++] = *s;
	}
	/* raw strings prefix syntax is only supported by servers since Jun2020 (11.37) */
	if (dbc->major == 11 && dbc->minor >= 37) {
		s = "' escape r'\\'";
	} else {
		s = "' escape '\\\\'";
	}
	for (; *s; s++)
		res[i++] = *s;
	res[i] = 0;
	return res;
}

char *
ODBCParseID(const char *tab, const char *col, const char *arg, size_t len)
{
	size_t i;
	char *res;
	const char *s;
	int fold = 1;

	while (len > 0 && (arg[--len] == ' ' || arg[len] == '\t'))
		;
	len++;
	if (len >= 2 && *arg == '"' && arg[len - 1] == '"') {
		arg++;
		len -= 2;
		fold = 0;
	}

	for (i = 0, s = arg; s < arg + len; i++, s++) {
		if (*s == '\'' || *s == '\\')
			i++;
	}
	i += strlen(tab) + strlen(col) + 10; /* ""."" = '' */
	if (fold)
		i += 14;	/* 2 times upper() */
	res = malloc(i + 1);
	if (res == NULL)
		return NULL;
	if (fold)
		snprintf(res, i, "upper(\"%s\".\"%s\") = upper('", tab, col);
	else
		snprintf(res, i, "\"%s\".\"%s\" = '", tab, col);
	for (i = strlen(res); len != 0; len--, arg++) {
		if (*arg == '\'' || *arg == '\\')
			res[i++] = *arg;
		res[i++] = *arg;
	}
	res[i++] = '\'';
	if (fold)
		res[i++] = ')';
	res[i] = 0;
	return res;
}

struct sql_types ODBC_sql_types[] = {
	{
		.concise_type = SQL_CHAR,
		.type = SQL_CHAR,
		.datetime_interval_precision = UNAFFECTED,
		.length = 1,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_VARCHAR,
		.type = SQL_VARCHAR,
		.datetime_interval_precision = UNAFFECTED,
		.length = 1,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_LONGVARCHAR,
		.type = SQL_LONGVARCHAR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_WCHAR,
		.type = SQL_WCHAR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_WVARCHAR,
		.type = SQL_WVARCHAR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_WLONGVARCHAR,
		.type = SQL_WLONGVARCHAR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_DECIMAL,
		.type = SQL_DECIMAL,
		.precision = 17,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_NUMERIC,
		.type = SQL_NUMERIC,
		.precision = 17,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_BIT,
		.type = SQL_BIT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_TINYINT,
		.type = SQL_TINYINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_SMALLINT,
		.type = SQL_SMALLINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_INTEGER,
		.type = SQL_INTEGER,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_BIGINT,
		.type = SQL_BIGINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_HUGEINT,
		.type = SQL_HUGEINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_REAL,
		.type = SQL_REAL,
		.precision = FLT_MANT_DIG,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 2,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_FLOAT,
		.type = SQL_FLOAT,
		.precision = DBL_MANT_DIG,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 2,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_DOUBLE,
		.type = SQL_DOUBLE,
		.precision = DBL_MANT_DIG,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 2,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_BINARY,
		.type = SQL_BINARY,
		.datetime_interval_precision = UNAFFECTED,
		.length = 1,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_VARBINARY,
		.type = SQL_VARBINARY,
		.datetime_interval_precision = UNAFFECTED,
		.length = 1,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_LONGVARBINARY,
		.type = SQL_LONGVARBINARY,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_GUID,
		.type = SQL_GUID,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_TYPE_DATE,
		.type = SQL_DATETIME,
		.code = SQL_CODE_DATE,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_TYPE_TIME,
		.type = SQL_DATETIME,
		.code = SQL_CODE_TIME,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_TYPE_TIMESTAMP,
		.type = SQL_DATETIME,
		.code = SQL_CODE_TIMESTAMP,
		.precision = 6,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_MONTH,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_MONTH,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_YEAR,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_YEAR,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_YEAR_TO_MONTH,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_YEAR_TO_MONTH,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_DAY,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_HOUR,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_HOUR,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_MINUTE,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_MINUTE,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_SECOND,
		.precision = 6,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_DAY_TO_HOUR,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY_TO_HOUR,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_DAY_TO_MINUTE,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY_TO_MINUTE,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_DAY_TO_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY_TO_SECOND,
		.precision = 6,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_HOUR_TO_MINUTE,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_HOUR_TO_MINUTE,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_HOUR_TO_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_HOUR_TO_SECOND,
		.precision = 6,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_INTERVAL_MINUTE_TO_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_MINUTE_TO_SECOND,
		.precision = 6,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		0		/* sentinel */
	},
};

struct sql_types ODBC_c_types[] = {
	{
		.concise_type = SQL_C_CHAR,
		.type = SQL_C_CHAR,
		.datetime_interval_precision = UNAFFECTED,
		.length = 1,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_WCHAR,
		.type = SQL_C_WCHAR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_BIT,
		.type = SQL_C_BIT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_NUMERIC,
		.type = SQL_C_NUMERIC,
		.precision = 17,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_STINYINT,
		.type = SQL_C_STINYINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_UTINYINT,
		.type = SQL_C_UTINYINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_TINYINT,
		.type = SQL_C_TINYINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_SBIGINT,
		.type = SQL_C_SBIGINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_UBIGINT,
		.type = SQL_C_UBIGINT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_SSHORT,
		.type = SQL_C_SSHORT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_USHORT,
		.type = SQL_C_USHORT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_SHORT,
		.type = SQL_C_SHORT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_SLONG,
		.type = SQL_C_SLONG,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_ULONG,
		.type = SQL_C_ULONG,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_LONG,
		.type = SQL_C_LONG,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 10,
		.fixed = SQL_TRUE,
	},
	{
		.concise_type = SQL_C_FLOAT,
		.type = SQL_C_FLOAT,
		.precision = FLT_MANT_DIG,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 2,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_DOUBLE,
		.type = SQL_C_DOUBLE,
		.precision = DBL_MANT_DIG,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.radix = 2,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_BINARY,
		.type = SQL_C_BINARY,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_TYPE_DATE,
		.type = SQL_DATETIME,
		.code = SQL_CODE_DATE,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_TYPE_TIME,
		.type = SQL_DATETIME,
		.code = SQL_CODE_TIME,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_TYPE_TIMESTAMP,
		.type = SQL_DATETIME,
		.code = SQL_CODE_TIMESTAMP,
		.precision = 6,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_MONTH,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_MONTH,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_YEAR,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_YEAR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_YEAR_TO_MONTH,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_YEAR_TO_MONTH,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_DAY,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_HOUR,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_HOUR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_MINUTE,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_MINUTE,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_SECOND,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 6,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_DAY_TO_HOUR,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY_TO_HOUR,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_DAY_TO_MINUTE,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY_TO_MINUTE,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_DAY_TO_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_DAY_TO_SECOND,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 6,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_HOUR_TO_MINUTE,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_HOUR_TO_MINUTE,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 2,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_HOUR_TO_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_HOUR_TO_SECOND,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 6,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_INTERVAL_MINUTE_TO_SECOND,
		.type = SQL_INTERVAL,
		.code = SQL_CODE_MINUTE_TO_SECOND,
		.precision = UNAFFECTED,
		.datetime_interval_precision = 6,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_GUID,
		.type = SQL_C_GUID,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		.concise_type = SQL_C_DEFAULT,
		.type = SQL_C_DEFAULT,
		.precision = UNAFFECTED,
		.datetime_interval_precision = UNAFFECTED,
		.length = UNAFFECTED,
		.scale = UNAFFECTED,
		.fixed = SQL_FALSE,
	},
	{
		0		/* sentinel */
	},
};

#ifdef ODBCDEBUG

#ifdef NATIVE_WIN32
const wchar_t *ODBCdebug;
#else
const char *ODBCdebug;
#endif
static char unknown[32];

char *
translateCType(SQLSMALLINT ValueType)
{
	switch (ValueType) {
	case SQL_C_CHAR:
		return "SQL_C_CHAR";
	case SQL_C_WCHAR:
		return "SQL_C_WCHAR";
	case SQL_C_BINARY:
		return "SQL_C_BINARY";
	case SQL_C_BIT:
		return "SQL_C_BIT";
	case SQL_C_STINYINT:
		return "SQL_C_STINYINT";
	case SQL_C_UTINYINT:
		return "SQL_C_UTINYINT";
	case SQL_C_TINYINT:
		return "SQL_C_TINYINT";
	case SQL_C_SSHORT:
		return "SQL_C_SSHORT";
	case SQL_C_USHORT:
		return "SQL_C_USHORT";
	case SQL_C_SHORT:
		return "SQL_C_SHORT";
	case SQL_C_SLONG:
		return "SQL_C_SLONG";
	case SQL_C_ULONG:
		return "SQL_C_ULONG";
	case SQL_C_LONG:
		return "SQL_C_LONG";
	case SQL_C_SBIGINT:
		return "SQL_C_SBIGINT";
	case SQL_C_UBIGINT:
		return "SQL_C_UBIGINT";
	case SQL_C_NUMERIC:
		return "SQL_C_NUMERIC";
	case SQL_C_FLOAT:
		return "SQL_C_FLOAT";
	case SQL_C_DOUBLE:
		return "SQL_C_DOUBLE";
	case SQL_C_TYPE_DATE:
		return "SQL_C_TYPE_DATE";
	case SQL_C_TYPE_TIME:
		return "SQL_C_TYPE_TIME";
	case SQL_C_TYPE_TIMESTAMP:
		return "SQL_C_TYPE_TIMESTAMP";
	case SQL_C_INTERVAL_YEAR:
		return "SQL_C_INTERVAL_YEAR";
	case SQL_C_INTERVAL_MONTH:
		return "SQL_C_INTERVAL_MONTH";
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
		return "SQL_C_INTERVAL_YEAR_TO_MONTH";
	case SQL_C_INTERVAL_DAY:
		return "SQL_C_INTERVAL_DAY";
	case SQL_C_INTERVAL_HOUR:
		return "SQL_C_INTERVAL_HOUR";
	case SQL_C_INTERVAL_MINUTE:
		return "SQL_C_INTERVAL_MINUTE";
	case SQL_C_INTERVAL_SECOND:
		return "SQL_C_INTERVAL_SECOND";
	case SQL_C_INTERVAL_DAY_TO_HOUR:
		return "SQL_C_INTERVAL_DAY_TO_HOUR";
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
		return "SQL_C_INTERVAL_DAY_TO_MINUTE";
	case SQL_C_INTERVAL_DAY_TO_SECOND:
		return "SQL_C_INTERVAL_DAY_TO_SECOND";
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
		return "SQL_C_INTERVAL_HOUR_TO_MINUTE";
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
		return "SQL_C_INTERVAL_HOUR_TO_SECOND";
	case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		return "SQL_C_INTERVAL_MINUTE_TO_SECOND";
	case SQL_C_GUID:
		return "SQL_C_GUID";
	case SQL_C_DEFAULT:
		return "SQL_C_DEFAULT";
	case SQL_ARD_TYPE:
		return "SQL_ARD_TYPE";
	case SQL_DATETIME:
		return "SQL_DATETIME";
	case SQL_INTERVAL:
		return "SQL_INTERVAL";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) ValueType);
		return unknown;
	}
}

char *
translateSQLType(SQLSMALLINT ParameterType)
{
	switch (ParameterType) {
	case SQL_CHAR:
		return "SQL_CHAR";
	case SQL_VARCHAR:
		return "SQL_VARCHAR";
	case SQL_LONGVARCHAR:
		return "SQL_LONGVARCHAR";
	case SQL_BINARY:
		return "SQL_BINARY";
	case SQL_VARBINARY:
		return "SQL_VARBINARY";
	case SQL_LONGVARBINARY:
		return "SQL_LONGVARBINARY";
	case SQL_TYPE_DATE:
		return "SQL_TYPE_DATE";
	case SQL_INTERVAL_MONTH:
		return "SQL_INTERVAL_MONTH";
	case SQL_INTERVAL_YEAR:
		return "SQL_INTERVAL_YEAR";
	case SQL_INTERVAL_YEAR_TO_MONTH:
		return "SQL_INTERVAL_YEAR_TO_MONTH";
	case SQL_INTERVAL_DAY:
		return "SQL_INTERVAL_DAY";
	case SQL_INTERVAL_HOUR:
		return "SQL_INTERVAL_HOUR";
	case SQL_INTERVAL_MINUTE:
		return "SQL_INTERVAL_MINUTE";
	case SQL_INTERVAL_DAY_TO_HOUR:
		return "SQL_INTERVAL_DAY_TO_HOUR";
	case SQL_INTERVAL_DAY_TO_MINUTE:
		return "SQL_INTERVAL_DAY_TO_MINUTE";
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		return "SQL_INTERVAL_HOUR_TO_MINUTE";
	case SQL_TYPE_TIME:
		return "SQL_TYPE_TIME";
	case SQL_TYPE_TIMESTAMP:
		return "SQL_TYPE_TIMESTAMP";
	case SQL_INTERVAL_SECOND:
		return "SQL_INTERVAL_SECOND";
	case SQL_INTERVAL_DAY_TO_SECOND:
		return "SQL_INTERVAL_DAY_TO_SECOND";
	case SQL_INTERVAL_HOUR_TO_SECOND:
		return "SQL_INTERVAL_HOUR_TO_SECOND";
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		return "SQL_INTERVAL_MINUTE_TO_SECOND";
	case SQL_DECIMAL:
		return "SQL_DECIMAL";
	case SQL_NUMERIC:
		return "SQL_NUMERIC";
	case SQL_FLOAT:
		return "SQL_FLOAT";
	case SQL_REAL:
		return "SQL_REAL";
	case SQL_DOUBLE:
		return "SQL_DOUBLE";
	case SQL_WCHAR:
		return "SQL_WCHAR";
	case SQL_WVARCHAR:
		return "SQL_WVARCHAR";
	case SQL_WLONGVARCHAR:
		return "SQL_WLONGVARCHAR";
	case SQL_BIT:
		return "SQL_BIT";
	case SQL_TINYINT:
		return "SQL_TINYINT";
	case SQL_SMALLINT:
		return "SQL_SMALLINT";
	case SQL_INTEGER:
		return "SQL_INTEGER";
	case SQL_BIGINT:
		return "SQL_BIGINT";
	case SQL_HUGEINT:
		return "SQL_HUGEINT";
	case SQL_GUID:
		return "SQL_GUID";
	case SQL_DATETIME:
		return "SQL_DATETIME";
	case SQL_INTERVAL:
		return "SQL_INTERVAL";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) ParameterType);
		return unknown;
	}
}

char *
translateFieldIdentifier(SQLSMALLINT FieldIdentifier)
{
	switch (FieldIdentifier) {
	case SQL_COLUMN_LENGTH:
		return "SQL_COLUMN_LENGTH";
	case SQL_COLUMN_PRECISION:
		return "SQL_COLUMN_PRECISION";
	case SQL_COLUMN_SCALE:
		return "SQL_COLUMN_SCALE";
	case SQL_DESC_ALLOC_TYPE:
		return "SQL_DESC_ALLOC_TYPE";
	case SQL_DESC_ARRAY_SIZE:
		return "SQL_DESC_ARRAY_SIZE";
	case SQL_DESC_ARRAY_STATUS_PTR:
		return "SQL_DESC_ARRAY_STATUS_PTR";
	case SQL_DESC_AUTO_UNIQUE_VALUE:
		return "SQL_DESC_AUTO_UNIQUE_VALUE";
	case SQL_DESC_BASE_COLUMN_NAME:
		return "SQL_DESC_BASE_COLUMN_NAME";
	case SQL_DESC_BASE_TABLE_NAME:
		return "SQL_DESC_BASE_TABLE_NAME";
	case SQL_DESC_BIND_OFFSET_PTR:
		return "SQL_DESC_BIND_OFFSET_PTR";
	case SQL_DESC_BIND_TYPE:
		return "SQL_DESC_BIND_TYPE";
	case SQL_DESC_CASE_SENSITIVE:
		return "SQL_DESC_CASE_SENSITIVE";
	case SQL_DESC_CATALOG_NAME:
		return "SQL_DESC_CATALOG_NAME";
	case SQL_DESC_CONCISE_TYPE:
		return "SQL_DESC_CONCISE_TYPE";
	case SQL_DESC_COUNT:
		return "SQL_DESC_COUNT";
	case SQL_DESC_DATA_PTR:
		return "SQL_DESC_DATA_PTR";
	case SQL_DESC_DATETIME_INTERVAL_CODE:
		return "SQL_DESC_DATETIME_INTERVAL_CODE";
	case SQL_DESC_DATETIME_INTERVAL_PRECISION:
		return "SQL_DESC_DATETIME_INTERVAL_PRECISION";
	case SQL_DESC_DISPLAY_SIZE:
		return "SQL_DESC_DISPLAY_SIZE";
	case SQL_DESC_FIXED_PREC_SCALE:
		return "SQL_DESC_FIXED_PREC_SCALE";
	case SQL_DESC_INDICATOR_PTR:
		return "SQL_DESC_INDICATOR_PTR";
	case SQL_DESC_LABEL:
		return "SQL_DESC_LABEL";
	case SQL_DESC_LENGTH:
		return "SQL_DESC_LENGTH";
	case SQL_DESC_LITERAL_PREFIX:
		return "SQL_DESC_LITERAL_PREFIX";
	case SQL_DESC_LITERAL_SUFFIX:
		return "SQL_DESC_LITERAL_SUFFIX";
	case SQL_DESC_LOCAL_TYPE_NAME:
		return "SQL_DESC_LOCAL_TYPE_NAME";
	case SQL_DESC_NAME:
		return "SQL_DESC_NAME";
	case SQL_DESC_NULLABLE:
		return "SQL_DESC_NULLABLE";
	case SQL_DESC_NUM_PREC_RADIX:
		return "SQL_DESC_NUM_PREC_RADIX";
	case SQL_DESC_OCTET_LENGTH:
		return "SQL_DESC_OCTET_LENGTH";
	case SQL_DESC_OCTET_LENGTH_PTR:
		return "SQL_DESC_OCTET_LENGTH_PTR";
	case SQL_DESC_PARAMETER_TYPE:
		return "SQL_DESC_PARAMETER_TYPE";
	case SQL_DESC_PRECISION:
		return "SQL_DESC_PRECISION";
	case SQL_DESC_ROWS_PROCESSED_PTR:
		return "SQL_DESC_ROWS_PROCESSED_PTR";
	case SQL_DESC_ROWVER:
		return "SQL_DESC_ROWVER";
	case SQL_DESC_SCALE:
		return "SQL_DESC_SCALE";
	case SQL_DESC_SCHEMA_NAME:
		return "SQL_DESC_SCHEMA_NAME";
	case SQL_DESC_SEARCHABLE:
		return "SQL_DESC_SEARCHABLE";
	case SQL_DESC_TABLE_NAME:
		return "SQL_DESC_TABLE_NAME";
	case SQL_DESC_TYPE:
		return "SQL_DESC_TYPE";
	case SQL_DESC_TYPE_NAME:
		return "SQL_DESC_TYPE_NAME";
	case SQL_DESC_UNNAMED:
		return "SQL_DESC_UNNAMED";
	case SQL_DESC_UNSIGNED:
		return "SQL_DESC_UNSIGNED";
	case SQL_DESC_UPDATABLE:
		return "SQL_DESC_UPDATABLE";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) FieldIdentifier);
		return unknown;
	}
}

char *
translateFetchOrientation(SQLUSMALLINT FetchOrientation)
{
	switch (FetchOrientation) {
	case SQL_FETCH_NEXT:
		return "SQL_FETCH_NEXT";
	case SQL_FETCH_FIRST:
		return "SQL_FETCH_FIRST";
	case SQL_FETCH_LAST:
		return "SQL_FETCH_LAST";
	case SQL_FETCH_PRIOR:
		return "SQL_FETCH_PRIOR";
	case SQL_FETCH_RELATIVE:
		return "SQL_FETCH_RELATIVE";
	case SQL_FETCH_ABSOLUTE:
		return "SQL_FETCH_ABSOLUTE";
	case SQL_FETCH_BOOKMARK:
		return "SQL_FETCH_BOOKMARK";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%u)", (unsigned int) FetchOrientation);
		return unknown;
	}
}

char *
translateConnectAttribute(SQLINTEGER Attribute)
{
	switch (Attribute) {
	case SQL_ATTR_ACCESS_MODE:
		return "SQL_ATTR_ACCESS_MODE";
#ifdef SQL_ATTR_ANSI_APP
	case SQL_ATTR_ANSI_APP:
		return "SQL_ATTR_ANSI_APP";
#endif
#ifdef SQL_ATTR_ASYNC_DBC_EVENT
	case SQL_ATTR_ASYNC_DBC_EVENT:
		return "SQL_ATTR_ASYNC_DBC_EVENT";
#endif
#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
	case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE:
		return "SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE";
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
	case SQL_ATTR_ASYNC_DBC_PCALLBACK:
		return "SQL_ATTR_ASYNC_DBC_PCALLBACK";
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
	case SQL_ATTR_ASYNC_DBC_PCONTEXT:
		return "SQL_ATTR_ASYNC_DBC_PCONTEXT";
#endif
	case SQL_ATTR_ASYNC_ENABLE:
		return "SQL_ATTR_ASYNC_ENABLE";
	case SQL_ATTR_AUTOCOMMIT:
		return "SQL_ATTR_AUTOCOMMIT";
	case SQL_ATTR_AUTO_IPD:
		return "SQL_ATTR_AUTO_IPD";
	case SQL_ATTR_CONNECTION_DEAD:
		return "SQL_ATTR_CONNECTION_DEAD";
	case SQL_ATTR_CONNECTION_TIMEOUT:
		return "SQL_ATTR_CONNECTION_TIMEOUT";
	case SQL_ATTR_CURRENT_CATALOG:
		return "SQL_ATTR_CURRENT_CATALOG";
#ifdef SQL_ATTR_DBC_INFO_TOKEN
	case SQL_ATTR_DBC_INFO_TOKEN:
		return "SQL_ATTR_DBC_INFO_TOKEN";
#endif
	case SQL_ATTR_DISCONNECT_BEHAVIOR:
		return "SQL_ATTR_DISCONNECT_BEHAVIOR";
	case SQL_ATTR_ENLIST_IN_DTC:
		return "SQL_ATTR_ENLIST_IN_DTC";
	case SQL_ATTR_ENLIST_IN_XA:
		return "SQL_ATTR_ENLIST_IN_XA";
	case SQL_ATTR_LOGIN_TIMEOUT:
		return "SQL_ATTR_LOGIN_TIMEOUT";
	case SQL_ATTR_METADATA_ID:
		return "SQL_ATTR_METADATA_ID";
	case SQL_ATTR_ODBC_CURSORS:
		return "SQL_ATTR_ODBC_CURSORS";
	case SQL_ATTR_PACKET_SIZE:
		return "SQL_ATTR_PACKET_SIZE";
	case SQL_ATTR_QUIET_MODE:
		return "SQL_ATTR_QUIET_MODE";
	case SQL_ATTR_TRACE:
		return "SQL_ATTR_TRACE";
	case SQL_ATTR_TRACEFILE:
		return "SQL_ATTR_TRACEFILE";
	case SQL_ATTR_TRANSLATE_LIB:
		return "SQL_ATTR_TRANSLATE_LIB";
	case SQL_ATTR_TRANSLATE_OPTION:
		return "SQL_ATTR_TRANSLATE_OPTION";
	case SQL_ATTR_TXN_ISOLATION:
		return "SQL_ATTR_TXN_ISOLATION";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) Attribute);
		return unknown;
	}
}

char *
translateConnectOption(SQLUSMALLINT Option)
{
	switch (Option) {
	case SQL_ACCESS_MODE:
		return "SQL_ACCESS_MODE";
	case SQL_AUTOCOMMIT:
		return "SQL_AUTOCOMMIT";
	case SQL_LOGIN_TIMEOUT:
		return "SQL_LOGIN_TIMEOUT";
	case SQL_ODBC_CURSORS:
		return "SQL_ODBC_CURSORS";
	case SQL_OPT_TRACE:
		return "SQL_OPT_TRACE";
	case SQL_PACKET_SIZE:
		return "SQL_PACKET_SIZE";
	case SQL_TRANSLATE_OPTION:
		return "SQL_TRANSLATE_OPTION";
	case SQL_TXN_ISOLATION:
		return "SQL_TXN_ISOLATION";
	case SQL_QUIET_MODE:
		return "SQL_QUIET_MODE";
	case SQL_CURRENT_QUALIFIER:
		return "SQL_CURRENT_QUALIFIER";
	case SQL_OPT_TRACEFILE:
		return "SQL_OPT_TRACEFILE";
	case SQL_TRANSLATE_DLL:
		return "SQL_TRANSLATE_DLL";
	default:
		return translateConnectAttribute((SQLSMALLINT) Option);
	}
}

char *
translateEnvAttribute(SQLINTEGER Attribute)
{
	switch (Attribute) {
	case SQL_ATTR_ODBC_VERSION:
		return "SQL_ATTR_ODBC_VERSION";
	case SQL_ATTR_OUTPUT_NTS:
		return "SQL_ATTR_OUTPUT_NTS";
	case SQL_ATTR_CONNECTION_POOLING:
		return "SQL_ATTR_CONNECTION_POOLING";
	case SQL_ATTR_CP_MATCH:
		return "SQL_ATTR_CP_MATCH";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) Attribute);
		return unknown;
	}
}

char *
translateStmtAttribute(SQLINTEGER Attribute)
{
	switch (Attribute) {
	case SQL_ATTR_APP_PARAM_DESC:
		return "SQL_ATTR_APP_PARAM_DESC";
	case SQL_ATTR_APP_ROW_DESC:
		return "SQL_ATTR_APP_ROW_DESC";
	case SQL_ATTR_ASYNC_ENABLE:
		return "SQL_ATTR_ASYNC_ENABLE";
	case SQL_ATTR_CONCURRENCY:
		return "SQL_ATTR_CONCURRENCY";
	case SQL_ATTR_CURSOR_SCROLLABLE:
		return "SQL_ATTR_CURSOR_SCROLLABLE";
	case SQL_ATTR_CURSOR_SENSITIVITY:
		return "SQL_ATTR_CURSOR_SENSITIVITY";
	case SQL_ATTR_CURSOR_TYPE:
		return "SQL_ATTR_CURSOR_TYPE";
	case SQL_ATTR_IMP_PARAM_DESC:
		return "SQL_ATTR_IMP_PARAM_DESC";
	case SQL_ATTR_IMP_ROW_DESC:
		return "SQL_ATTR_IMP_ROW_DESC";
	case SQL_ATTR_MAX_LENGTH:
		return "SQL_ATTR_MAX_LENGTH";
	case SQL_ATTR_MAX_ROWS:
		return "SQL_ATTR_MAX_ROWS";
	case SQL_ATTR_NOSCAN:
		return "SQL_ATTR_NOSCAN";
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
		return "SQL_ATTR_PARAM_BIND_OFFSET_PTR";
	case SQL_ATTR_PARAM_BIND_TYPE:
		return "SQL_ATTR_PARAM_BIND_TYPE";
	case SQL_ATTR_PARAM_OPERATION_PTR:
		return "SQL_ATTR_PARAM_OPERATION_PTR";
	case SQL_ATTR_PARAM_STATUS_PTR:
		return "SQL_ATTR_PARAM_STATUS_PTR";
	case SQL_ATTR_PARAMS_PROCESSED_PTR:
		return "SQL_ATTR_PARAMS_PROCESSED_PTR";
	case SQL_ATTR_PARAMSET_SIZE:
		return "SQL_ATTR_PARAMSET_SIZE";
	case SQL_ATTR_RETRIEVE_DATA:
		return "SQL_ATTR_RETRIEVE_DATA";
	case SQL_ATTR_ROW_ARRAY_SIZE:
		return "SQL_ATTR_ROW_ARRAY_SIZE";
	case SQL_ROWSET_SIZE:
		return "SQL_ROWSET_SIZE";
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:
		return "SQL_ATTR_ROW_BIND_OFFSET_PTR";
	case SQL_ATTR_ROW_BIND_TYPE:
		return "SQL_ATTR_ROW_BIND_TYPE";
	case SQL_ATTR_ROW_NUMBER:
		return "SQL_ATTR_ROW_NUMBER";
	case SQL_ATTR_ROW_OPERATION_PTR:
		return "SQL_ATTR_ROW_OPERATION_PTR";
	case SQL_ATTR_ROW_STATUS_PTR:
		return "SQL_ATTR_ROW_STATUS_PTR";
	case SQL_ATTR_ROWS_FETCHED_PTR:
		return "SQL_ATTR_ROWS_FETCHED_PTR";
	case SQL_ATTR_METADATA_ID:
		return "SQL_ATTR_METADATA_ID";
	case SQL_ATTR_ENABLE_AUTO_IPD:
		return "SQL_ATTR_ENABLE_AUTO_IPD";
	case SQL_ATTR_FETCH_BOOKMARK_PTR:
		return "SQL_ATTR_FETCH_BOOKMARK_PTR";
	case SQL_ATTR_KEYSET_SIZE:
		return "SQL_ATTR_KEYSET_SIZE";
	case SQL_ATTR_QUERY_TIMEOUT:
		return "SQL_ATTR_QUERY_TIMEOUT";
	case SQL_ATTR_SIMULATE_CURSOR:
		return "SQL_ATTR_SIMULATE_CURSOR";
	case SQL_ATTR_USE_BOOKMARKS:
		return "SQL_ATTR_USE_BOOKMARKS";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) Attribute);
		return unknown;
	}
}

char *
translateStmtOption(SQLUSMALLINT Option)
{
	switch (Option) {
	case SQL_QUERY_TIMEOUT:
		return "SQL_QUERY_TIMEOUT";
	case SQL_MAX_ROWS:
		return "SQL_MAX_ROWS";
	case SQL_NOSCAN:
		return "SQL_NOSCAN";
	case SQL_MAX_LENGTH:
		return "SQL_MAX_LENGTH";
	case SQL_ASYNC_ENABLE:
		return "SQL_ASYNC_ENABLE";
	case SQL_BIND_TYPE:
		return "SQL_BIND_TYPE";
	case SQL_CURSOR_TYPE:
		return "SQL_CURSOR_TYPE";
	case SQL_CONCURRENCY:
		return "SQL_CONCURRENCY";
	case SQL_KEYSET_SIZE:
		return "SQL_KEYSET_SIZE";
	case SQL_ROWSET_SIZE:
		return "SQL_ROWSET_SIZE";
	case SQL_SIMULATE_CURSOR:
		return "SQL_SIMULATE_CURSOR";
	case SQL_RETRIEVE_DATA:
		return "SQL_RETRIEVE_DATA";
	case SQL_USE_BOOKMARKS:
		return "SQL_USE_BOOKMARKS";
	case SQL_ROW_NUMBER:
		return "SQL_ROW_NUMBER";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%u)", (unsigned int) Option);
		return unknown;
	}
}

char *
translateCompletionType(SQLSMALLINT CompletionType)
{
	switch (CompletionType) {
	case SQL_COMMIT:
		return "SQL_COMMIT";
	case SQL_ROLLBACK:
		return "SQL_ROLLBACK";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) CompletionType);
		return unknown;
	}
}

#if !defined(__STDC_VERSION__) || __STDC_VERSION__ < 199901
void
ODBCLOG(const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	if (ODBCdebug == NULL) {
#ifdef NATIVE_WIN32
		if ((ODBCdebug = _wgetenv(L"ODBCDEBUG")) == NULL)
			ODBCdebug = _wcsdup(L"");
		else
			ODBCdebug = _wcsdup(ODBCdebug);
#else
		if ((ODBCdebug = getenv("ODBCDEBUG")) == NULL)
			ODBCdebug = strdup("");
		else
			ODBCdebug = strdup(ODBCdebug);
#endif
	}
	if (ODBCdebug != NULL && *ODBCdebug != 0) {
		FILE *f;

#ifdef NATIVE_WIN32
		f = _wfopen(ODBCdebug, L"a");
#else
		f = fopen(ODBCdebug, "a");
#endif
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
