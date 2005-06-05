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

/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 * 
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

/**********************************************************************
 * SQLFetch()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"
#include <errno.h>
#include <time.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif

#if SIZEOF_INT==8
# define ULL_CONSTANT(val)	(val)
# define ULLFMT			"u"
#elif SIZEOF_LONG==8
# define ULL_CONSTANT(val)	(val##UL)
# define ULLFMT			"lu"
#elif defined(HAVE_LONG_LONG)
# define ULL_CONSTANT(val)	(val##ULL)
# define ULLFMT			"llu"
#elif defined(HAVE___INT64)
# define ULL_CONSTANT(val)	(val##ui64)
# define ULLFMT			"I64u"
#endif

#define MAXBIGNUM10	ULL_CONSTANT(1844674407370955161)	/* (2**64-1)/10 */
#define MAXBIGNUMLAST	'5'	/* (2**64-1)%10 */

#define space(c)	((c) == ' ' || (c) == '\t')

typedef struct {
	unsigned char precision;
	signed char scale;
	unsigned char sign;	/* 1 pos, 0 neg */
	SQLUBIGINT val;
} bignum_t;

#ifndef HAVE_STRNCASECMP
static int
strncasecmp(const char *s1, const char *s2, size_t n)
{
	int c1, c2;

	while (n > 0) {
		c1 = (unsigned char) *s1++;
		c2 = (unsigned char) *s2++;
		if (c1 == 0)
			return -c2;
		if (c2 == 0)
			return c1;
		if (tolower(c1) != tolower(c2))
			return tolower(c1) - tolower(c2);
		n--;
	}
	return 0;
}
#endif

/* Parse a number and store in a bignum_t.
   1 is returned if all is well;
   2 is returned if there is loss of precision (i.e. overflow of the value);
   0 is returned if the string is not a number, or if scale doesn't fit.
*/
static int
parseint(const char *data, bignum_t * nval)
{
	int fraction = 0;	/* inside the fractional part */
	int scale = 0;
	int overflow = 0;

	nval->val = 0;
	nval->precision = 0;
	scale = 0;
	while (space(*data))
		data++;
	if (*data == '-') {
		nval->sign = 0;
		data++;
	} else {
		nval->sign = 1;
		if (*data == '+')
			data++;
	}
	while (*data && *data != 'e' && *data != 'E' && !space(*data)) {
		if (*data == '.')
			fraction = 1;
		else if ('0' <= *data && *data <= '9') {
			if (overflow || nval->val > MAXBIGNUM10 || (nval->val == MAXBIGNUM10 && *data > MAXBIGNUMLAST)) {
				overflow = 1;
				if (!fraction)
					scale--;
			} else {
				nval->precision++;
				if (fraction)
					scale++;
				nval->val *= 10;
				nval->val += *data - '0';
			}
		} else
			return 0;
		data++;
	}
	/* normalize scale */
	while (scale > 0 && nval->val % 10 == 0) {
		scale--;
		nval->val /= 10;
	}
	if (*data == 'e' || *data == 'E') {
		char *p;
		long i;

		i = strtol(data, &p, 10);
		if (p == data || *p)
			return 0;
		scale -= i;
		/* normalize scale */
		while (scale > 0 && nval->val % 10 == 0) {
			scale--;
			nval->val /= 10;
		}
		while (scale < 0 && nval->val <= MAXBIGNUM10) {
			scale++;
			nval->val *= 10;
		}
	}
	if (scale < -128 || scale > 127)
		return 0;
	nval->scale = scale;
	while (space(*data))
		data++;
	if (*data)
		return 0;
	return 1 + overflow;
}

static int
parsesecondinterval(bignum_t * nval, SQL_INTERVAL_STRUCT * ival)
{
	unsigned int f = 1;
	int ivalscale = 0;

	ival->intval.day_second.fraction = 0;
	while (nval->scale > 0) {
		if (f < 1000000000) {
			ivalscale++;
			ival->intval.day_second.fraction += (SQLUINTEGER) ((nval->val % 10) * f);
			f *= 10;
		}
		nval->val /= 10;
		nval->scale--;
	}
	/* normalize scale */
	while (ivalscale > 0 && ival->intval.day_second.fraction % 10 != 0) {
		ivalscale--;
		ival->intval.day_second.fraction /= 10;
	}
	ival->interval_type = SQL_IS_DAY_TO_SECOND;
	ival->interval_sign = nval->sign ? SQL_FALSE : SQL_TRUE;
	ival->intval.day_second.second = (SQLUINTEGER) (nval->val % 60);
	nval->val /= 60;
	ival->intval.day_second.minute = (SQLUINTEGER) (nval->val % 60);
	nval->val /= 60;
	ival->intval.day_second.hour = (SQLUINTEGER) (nval->val % 24);
	nval->val /= 24;
	ival->intval.day_second.day = (SQLUINTEGER) nval->val;
	return ivalscale;
}

static void
parsemonthinterval(bignum_t * nval, SQL_INTERVAL_STRUCT * ival)
{
	while (nval->scale > 0) {
		/* ignore fraction */
		nval->scale--;
		nval->val /= 10;
	}
	ival->interval_type = SQL_IS_YEAR_TO_MONTH;
	ival->interval_sign = nval->sign ? SQL_FALSE : SQL_TRUE;
	ival->intval.year_month.year = (SQLUINTEGER) (nval->val / 12);
	ival->intval.year_month.month = (SQLUINTEGER) (nval->val % 12);
}

static short monthlengths[] = {
	0,			/* dummy */
	31,			/* Jan */
	29,			/* Feb */
	31,			/* Mar */
	30,			/* Apr */
	31,			/* May */
	30,			/* Jun */
	31,			/* Jul */
	31,			/* Aug */
	30,			/* Sep */
	31,			/* Oct */
	30,			/* Nov */
	31,			/* Dec */
};

#define isLeap(y)	((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))

static int
parsedate(const char *data, DATE_STRUCT * dval)
{
	int n;

	while (space(*data))
		data++;
	if (sscanf(data, "%hd-%hu-%hu%n", &dval->year, &dval->month, &dval->day, &n) < 3)
		return 0;
	if (dval->month == 0 || dval->month > 12 || dval->day == 0 || dval->day > monthlengths[dval->month] || (dval->month == 2 && !isLeap(dval->year) && dval->day == 29))
		return 0;
	data += n;
	while (space(*data))
		data++;
	if (*data)
		return 0;
	return 1;
}

static int
parsetime(const char *data, TIME_STRUCT * tval)
{
	int n;

	while (space(*data))
		data++;
	if (sscanf(data, "%hu:%hu:%hu%n", &tval->hour, &tval->minute, &tval->second, &n) < 3)
		return 0;
	/* seconds can go up to 61(!) because of leap seconds */
	if (tval->hour > 23 || tval->minute > 59 || tval->second > 61)
		return 0;
	data += n;
	n = 1;			/* tentative return value */
	if (*data == '.') {
		while (*++data && '0' <= *data && *data <= '9') ;
		n = 2;		/* indicate loss of precision */
	}
	while (space(*data))
		data++;
	if (*data)
		return 0;
	return n;
}

static int
parsetimestamp(const char *data, TIMESTAMP_STRUCT * tsval)
{
	int n;

	while (space(*data))
		data++;
	if (sscanf(data, "%hd-%hu-%hu %hu:%hu:%hu%n", &tsval->year, &tsval->month, &tsval->day, &tsval->hour, &tsval->minute, &tsval->second, &n) < 6)
		return 0;
	if (tsval->month == 0 || tsval->month > 12 || tsval->day == 0 || tsval->day > monthlengths[tsval->month] || (tsval->month == 2 && !isLeap(tsval->year) && tsval->day == 29) || tsval->hour > 23 || tsval->minute > 59 || tsval->second > 61)
		return 0;
	tsval->fraction = 0;
	data += n;
	n = 1000000000;
	if (*data == '.') {
		while (*++data && '0' <= *data && *data <= '9') {
			n /= 10;
			tsval->fraction += (*data - '0') * n;
		}
	}
	while (space(*data))
		data++;
	if (*data)
		return 0;
	if (n == 0)
		return 2;	/* fractional digits truncated */
	return 1;
}

static int
parsedouble(const char *data, double *fval)
{
	char *p;

	while (space(*data))
		data++;
	errno = 0;
	*fval = strtod(data, &p);
	if (p == NULL || p == data || errno == ERANGE)
		return 0;
	while (space(*p))
		p++;
	if (*p)
		return 0;
	return 1;
}

SQLRETURN
ODBCFetch(ODBCStmt *stmt, SQLUSMALLINT col, SQLSMALLINT type, SQLPOINTER ptr, SQLINTEGER buflen, SQLINTEGER *lenp, SQLINTEGER *nullp, SQLSMALLINT precision, SQLSMALLINT scale, SQLINTEGER datetime_interval_precision, SQLINTEGER offset, int row)
{
	char *data;
	SQLSMALLINT sql_type;
	SQLUINTEGER maxdatetimeval;
	ODBCDesc *ard, *ird;
	ODBCDescRec *irdrec, *ardrec;
	SQLUINTEGER bind_type;

	/* various interpretations of the input data */
	bignum_t nval;
	SQL_INTERVAL_STRUCT ival;
	int i = 0;		/* scale of ival fraction, or counter */
	DATE_STRUCT dval;
	TIME_STRUCT tval;
	TIMESTAMP_STRUCT tsval;
	double fval = 0;

	ird = stmt->ImplRowDescr;
	ard = stmt->ApplRowDescr;

	if (col == 0 || col > ird->sql_desc_count) {
		/* Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);

		return SQL_ERROR;
	}
	bind_type = ard->sql_desc_bind_type;
	irdrec = &ird->descRec[col];
	ardrec = col <= ard->sql_desc_count ? &ard->descRec[col] : NULL;
	sql_type = irdrec->sql_desc_concise_type;

	if (ptr &&offset)
		ptr = (SQLPOINTER) ((char *) ptr +offset);

	if (lenp && offset)
		lenp = (SQLINTEGER *) ((char *) lenp + offset + row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(SQLINTEGER) : bind_type));
	if (nullp && offset)
		nullp = (SQLINTEGER *) ((char *) nullp + offset + row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(SQLINTEGER) : bind_type));

	/* translate default type */
	/* note, type can't be SQL_ARD_TYPE since when this function
	   is called from SQLFetch, type is already the ARD concise
	   type, and when it is called from SQLGetData, it has already
	   been translated */

	if (type == SQL_C_DEFAULT) {
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_LONGVARCHAR:
		case SQL_DECIMAL:
		case SQL_NUMERIC:
			type = SQL_C_CHAR;
			break;
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_WLONGVARCHAR:
			type = SQL_C_WCHAR;
			break;
		case SQL_BIT:
			type = SQL_C_BIT;
			break;
		case SQL_TINYINT:
			type = irdrec->sql_desc_unsigned ? SQL_C_UTINYINT : SQL_C_STINYINT;
			break;
		case SQL_SMALLINT:
			type = irdrec->sql_desc_unsigned ? SQL_C_USHORT : SQL_C_SSHORT;
			break;
		case SQL_INTEGER:
			type = irdrec->sql_desc_unsigned ? SQL_C_ULONG : SQL_C_SLONG;
			break;
		case SQL_BIGINT:
			type = irdrec->sql_desc_unsigned ? SQL_C_UBIGINT : SQL_C_SBIGINT;
			break;
		case SQL_REAL:
			type = SQL_C_FLOAT;
			break;
		case SQL_FLOAT:
		case SQL_DOUBLE:
			type = SQL_C_DOUBLE;
			break;
		case SQL_BINARY:
		case SQL_VARBINARY:
		case SQL_LONGVARBINARY:
			type = SQL_C_BINARY;
			break;
		case SQL_TYPE_DATE:
			type = SQL_C_TYPE_DATE;
			break;
		case SQL_TYPE_TIME:
			type = SQL_C_TYPE_TIME;
			break;
		case SQL_TYPE_TIMESTAMP:
			type = SQL_C_TYPE_TIMESTAMP;
			break;
		case SQL_INTERVAL_YEAR:
			type = SQL_C_INTERVAL_YEAR;
			break;
		case SQL_INTERVAL_MONTH:
			type = SQL_C_INTERVAL_MONTH;
			break;
		case SQL_INTERVAL_YEAR_TO_MONTH:
			type = SQL_C_INTERVAL_YEAR_TO_MONTH;
			break;
		case SQL_INTERVAL_DAY:
			type = SQL_C_INTERVAL_DAY;
			break;
		case SQL_INTERVAL_HOUR:
			type = SQL_C_INTERVAL_HOUR;
			break;
		case SQL_INTERVAL_MINUTE:
			type = SQL_C_INTERVAL_MINUTE;
			break;
		case SQL_INTERVAL_SECOND:
			type = SQL_C_INTERVAL_SECOND;
			break;
		case SQL_INTERVAL_DAY_TO_HOUR:
			type = SQL_C_INTERVAL_DAY_TO_HOUR;
			break;
		case SQL_INTERVAL_DAY_TO_MINUTE:
			type = SQL_C_INTERVAL_DAY_TO_MINUTE;
			break;
		case SQL_INTERVAL_DAY_TO_SECOND:
			type = SQL_C_INTERVAL_DAY_TO_SECOND;
			break;
		case SQL_INTERVAL_HOUR_TO_MINUTE:
			type = SQL_C_INTERVAL_HOUR_TO_MINUTE;
			break;
		case SQL_INTERVAL_HOUR_TO_SECOND:
			type = SQL_C_INTERVAL_HOUR_TO_SECOND;
			break;
		case SQL_INTERVAL_MINUTE_TO_SECOND:
			type = SQL_C_INTERVAL_MINUTE_TO_SECOND;
			break;
		case SQL_GUID:
			type = SQL_C_GUID;
			break;
		}
	}

	if (precision == UNAFFECTED || scale == UNAFFECTED || datetime_interval_precision == UNAFFECTED) {
		if (ardrec) {
			if (precision == UNAFFECTED)
				precision = ardrec->sql_desc_precision;
			if (scale == UNAFFECTED)
				scale = ardrec->sql_desc_scale;
			if (datetime_interval_precision == UNAFFECTED)
				datetime_interval_precision = ardrec->sql_desc_datetime_interval_precision;
		} else {
			if (precision == UNAFFECTED)
				precision = type == SQL_C_NUMERIC ? 10 : 6;
			if (scale == UNAFFECTED)
				scale = 0;
			if (datetime_interval_precision == UNAFFECTED)
				datetime_interval_precision = 2;
		}
	}
	i = datetime_interval_precision;
	maxdatetimeval = 1;
	while (i-- > 0)
		maxdatetimeval *= 10;

	data = mapi_fetch_field(stmt->hdl, col - 1);
	if (mapi_error(stmt->Dbc->mid)) {
		/* General error */
		addStmtError(stmt, "HY000", NULL, 0);

		return SQL_ERROR;
	}
	if (nullp)
		*nullp = SQL_NULL_DATA;
	if (lenp)
		*lenp = SQL_NULL_DATA;
	if (data == NULL) {
		if (nullp == NULL) {
			/* Indicator variable required but not supplied */
			addStmtError(stmt, "22002", NULL, 0);

			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	}

	/* first convert to internal (binary) format */

	/* see SQLExecute.c for possible types */
	switch (sql_type) {
	case SQL_CHAR:
		break;
	case SQL_DECIMAL:
	case SQL_TINYINT:
	case SQL_SMALLINT:
	case SQL_INTEGER:
	case SQL_BIGINT:
	case SQL_INTERVAL_MONTH:
	case SQL_INTERVAL_SECOND:
		if (!parseint(data, &nval)) {
			/* shouldn't happen: getting here means SQL
			   server told us a value was of a certain
			   type, but in reality it wasn't. */
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);

			return SQL_ERROR;
		}

		/* interval types are transferred as ints but need to
		   be converted to the internal interval formats */
		if (sql_type == SQL_INTERVAL_SECOND)
			i = parsesecondinterval(&nval, &ival);
		else if (sql_type == SQL_INTERVAL_MONTH)
			parsemonthinterval(&nval, &ival);
		break;
	case SQL_DOUBLE:
	case SQL_REAL:
		if (!parsedouble(data, &fval)) {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);

			return SQL_ERROR;
		}
		break;
	case SQL_BIT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		while (space(*data))
			data++;
		if (strncasecmp(data, "true", 4) == 0) {
			data += 4;
			nval.val = 1;
		} else if (strncasecmp(data, "false", 5) == 0) {
			data += 5;
			nval.val = 0;
		} else {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);

			return SQL_ERROR;
		}
		while (space(*data))
			data++;
		if (*data) {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);

			return SQL_ERROR;
		}
		break;
	case SQL_TYPE_DATE:
		if (!parsedate(data, &dval)) {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);

			return SQL_ERROR;
		}
		break;
	case SQL_TYPE_TIME:
		if (!parsetime(data, &tval)) {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);

			return SQL_ERROR;
		}
		break;
	case SQL_TYPE_TIMESTAMP:
		if (!parsetimestamp(data, &tsval)) {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);

			return SQL_ERROR;
		}
		break;
	default:
		/* any other type can only be converted to SQL_C_CHAR */
		break;
	}

	/* then convert to desired format */

	switch (type) {
	case SQL_C_CHAR:
	case SQL_C_WCHAR:{
		SQLPOINTER origptr;
		SQLINTEGER origbuflen;
		SQLINTEGER *origlenp;

		if (buflen < 0) {
			/* Invalid string or buffer length */
			addStmtError(stmt, "HY090", NULL, 0);

			return SQL_ERROR;
		}
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? ardrec->sql_desc_octet_length : bind_type));

		/* if SQL_C_WCHAR is requested, first convert to UTF-8
		 * (SQL_C_CHAR), and at the end convert to UTF-16 */
		origptr = ptr;

		origbuflen = buflen;
		origlenp = lenp;
		if (type == SQL_C_WCHAR) {
			/* allocate temporary space */
			buflen *= 4;
			ptr = malloc(buflen + 1);

			lenp = NULL;
		}
		switch (sql_type) {
			int sz;

		default:
		case SQL_CHAR:
			copyString(data, ptr, buflen, lenp, addStmtError, stmt);

			break;
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT:{
			int f, n;

			data = (char *) ptr;

			for (n = 0, f = 1; n < nval.scale; n++)
				f *= 10;
			sz = snprintf(data, buflen, "%s%" ULLFMT, nval.sign ? "" : "-", nval.val / f);
			if (sz < 0 || sz >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);

				return SQL_ERROR;
			}
			if (lenp)
				*lenp = sz;
			if (nval.scale > 0) {
				data += sz;
				buflen -= sz;
				if (lenp)
					*lenp += nval.scale + 1;
				if (buflen > 2)
					sz = snprintf(data, buflen, ".%0*" ULLFMT, nval.scale, nval.val % f);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		}
		case SQL_DOUBLE:
		case SQL_REAL:{
			int i;
			data = (char *) ptr;

			for (i = 0; i < 18; i++) {
				sz = snprintf(data, buflen, "%.*g", i, fval);
				if (sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					if (i == 0) {
						/* Numeric value out
						   of range */
						addStmtError(stmt, "22003", NULL, 0);

						if (type == SQL_C_WCHAR)
							free(ptr);

						return SQL_ERROR;
					}
					/* current precision (i) doesn't fit,
					   but previous did, so use that */
					snprintf(data, buflen, "%.*g", i - 1, fval);
					/* max space that would have
					   been needed */
					sz = strlen(data) + 17 - i;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
					break;
				}
				if (fval == strtod(data, NULL))
					break;
			}
			if (lenp)
				*lenp = sz;
			break;
		}
		case SQL_TYPE_DATE:
			if (buflen < 11) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);

				return SQL_ERROR;
			}
			data = (char *) ptr;

			sz = snprintf(data, buflen, "%04u-%02u-%02u", dval.year, dval.month, dval.day);
			if (sz < 0 || sz >= buflen) {
				data[buflen - 1] = 0;
				/* String data, right-truncated */
				addStmtError(stmt, "01004", NULL, 0);
			}
			if (lenp)
				*lenp = sz;
			break;
		case SQL_TYPE_TIME:
			if (buflen < 9) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);

				return SQL_ERROR;
			}
			data = (char *) ptr;

			sz = snprintf(data, buflen, "%02u:%02u:%02u", tval.hour, tval.minute, tval.second);
			if (sz < 0 || sz >= buflen) {
				data[buflen - 1] = 0;
				/* String data, right-truncated */
				addStmtError(stmt, "01004", NULL, 0);
			}
			if (lenp)
				*lenp = sz;
			break;
		case SQL_TYPE_TIMESTAMP:
			data = (char *) ptr;

			sz = snprintf(data, buflen, "%04u-%02u-%02u %02u:%02u:%02u", tsval.year, tsval.month, tsval.day, tsval.hour, tsval.minute, tsval.second);
			if (sz < 0 || sz >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);

				return SQL_ERROR;
			}
			if (lenp)
				*lenp = sz;
			if (tsval.fraction) {
				int scale = 9;

				data += sz;
				buflen += sz;
				while (tsval.fraction % 10 == 0) {
					tsval.fraction /= 10;
					scale--;
				}
				if (lenp)
					*lenp += scale + 1;
				if (buflen > 2)
					sz = snprintf(data, buflen, ".%0*u", scale, tsval.fraction);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		case SQL_INTERVAL_MONTH:
			sz = snprintf((char *) ptr, buflen, "%s%04u-%02u", ival.interval_sign ? "-" : "", ival.intval.year_month.year, ival.intval.year_month.month);

			if (sz < 0 || sz >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);

				return SQL_ERROR;
			}
			if (lenp)
				*lenp = sz;
			break;
		case SQL_INTERVAL_SECOND:
			data = (char *) ptr;

			sz = snprintf(data, buflen, "%s%u %02u:%02u:%02u", ival.interval_sign ? "-" : "", ival.intval.day_second.day, ival.intval.day_second.hour, ival.intval.day_second.minute, ival.intval.day_second.second);
			if (sz < 0 || sz >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);

				return SQL_ERROR;
			}
			if (lenp)
				*lenp = sz;
			if (ival.intval.day_second.fraction) {
				data += sz;
				buflen -= sz;
				if (lenp)
					*lenp += i + 1;
				if (buflen > 2)
					sz = snprintf(data, buflen, ".%0*u", i, ival.intval.day_second.fraction);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		}
		if (type == SQL_C_WCHAR) {
			SQLSMALLINT n;

			ODBCutf82wchar((SQLCHAR *) ptr, SQL_NTS, (SQLWCHAR *) origptr, origbuflen, &n);

			if (origlenp)
				*origlenp = n * 2;	/* # of bytes, not chars */
			free(ptr);
		}
		break;
	}
	case SQL_C_BINARY:
		if (buflen < 0) {
			/* Invalid string or buffer length */
			addStmtError(stmt, "HY090", NULL, 0);

			return SQL_ERROR;
		}
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? ardrec->sql_desc_octet_length : bind_type));

		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_REAL:
		case SQL_DOUBLE:
		case SQL_BIT:
		case SQL_TYPE_DATE:
		case SQL_TYPE_TIME:
		case SQL_TYPE_TIMESTAMP:
		case SQL_INTERVAL_MONTH:
		case SQL_INTERVAL_SECOND:
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		break;
	case SQL_C_BIT:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(unsigned char) : bind_type));

		if (lenp)
			*lenp = 1;
		switch (sql_type) {
		case SQL_CHAR:
			if (!parsedouble(data, &fval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			/* fall through */
		case SQL_REAL:
		case SQL_DOUBLE:
			if (fval < 0 || fval >= 2) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				return SQL_ERROR;
			}
			*(unsigned char *) ptr = fval >= 1;

			if (fval != 0 && fval != 1)
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

			break;
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT:{
			int truncated = nval.scale > 0;

			while (nval.scale > 0) {
				nval.val /= 10;
				nval.scale--;
			}
			/* -0 is ok, but -1 or -0.5 isn't */
			if (nval.val > 1 || (!nval.sign && (nval.val == 1 || truncated))) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				return SQL_ERROR;
			}
			*(unsigned char *) ptr = (unsigned char) nval.val;

			if (truncated)
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);

			break;
		}
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		break;
	case SQL_C_STINYINT:
	case SQL_C_TINYINT:
	case SQL_C_SSHORT:
	case SQL_C_SHORT:
	case SQL_C_SLONG:
	case SQL_C_LONG:
	case SQL_C_SBIGINT:{
		SQLUBIGINT maxval = 1;

		switch (type) {
		case SQL_C_STINYINT:
		case SQL_C_TINYINT:
			maxval <<= 7;
			if (lenp)
				*lenp = sizeof(signed char);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(signed char) : bind_type));

			break;
		case SQL_C_SSHORT:
		case SQL_C_SHORT:
			maxval <<= 15;
			if (lenp)
				*lenp = sizeof(short);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(short) : bind_type));

			break;
		case SQL_C_SLONG:
		case SQL_C_LONG:
			maxval <<= 31;
			if (lenp)
				*lenp = sizeof(long);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(long) : bind_type));

			break;
		case SQL_C_SBIGINT:
			maxval <<= 63;
			if (lenp)
				*lenp = sizeof(SQLBIGINT);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(SQLBIGINT) : bind_type));

			break;
		}
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DOUBLE:
		case SQL_REAL:
			/* reparse double and float, parse char */
			if (!parseint(data, &nval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			/* fall through */
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT:{
			int truncated = nval.scale > 0;

			/* scale is normalized, so if negative, number
			   is too large even for SQLUBIGINT */
			while (nval.scale > 0) {
				nval.val /= 10;
				nval.scale--;
			}
			if (nval.scale < 0 || nval.val > maxval || (nval.val == maxval && nval.sign)) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				return SQL_ERROR;
			}
			if (truncated)
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);

			switch (type) {
			case SQL_C_STINYINT:
			case SQL_C_TINYINT:
				*(signed char *) ptr = nval.sign ? (signed char) nval.val : -(signed char) nval.val;

				break;
			case SQL_C_SSHORT:
			case SQL_C_SHORT:
				*(short *) ptr = nval.sign ? (short) nval.val : -(short) nval.val;

				break;
			case SQL_C_SLONG:
			case SQL_C_LONG:
				*(long *) ptr = nval.sign ? (long) nval.val : -(long) nval.val;

				break;
			case SQL_C_SBIGINT:
				*(SQLBIGINT *) ptr = nval.sign ? (SQLBIGINT) nval.val : -(SQLBIGINT) nval.val;

				break;
			}
			break;
		}
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		break;
	}
	case SQL_C_UTINYINT:
	case SQL_C_USHORT:
	case SQL_C_ULONG:
	case SQL_C_UBIGINT:{
		SQLUBIGINT maxval = 1;

		switch (type) {
		case SQL_C_UTINYINT:
			maxval <<= 8;
			if (lenp)
				*lenp = sizeof(unsigned char);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(unsigned char) : bind_type));

			break;
		case SQL_C_USHORT:
			maxval <<= 16;
			if (lenp)
				*lenp = sizeof(unsigned short);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(unsigned short) : bind_type));

			break;
		case SQL_C_ULONG:
			maxval <<= 32;
			if (lenp)
				*lenp = sizeof(unsigned long);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(unsigned long) : bind_type));

			break;
		case SQL_C_UBIGINT:
			if (lenp)
				*lenp = sizeof(SQLUBIGINT);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(SQLUBIGINT) : bind_type));

			break;
		}
		maxval--;
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DOUBLE:
		case SQL_REAL:
			/* reparse double and float, parse char */
			if (!parseint(data, &nval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			/* fall through */
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT:{
			int truncated = nval.scale > 0;

			/* scale is normalized, so if negative, number
			   is too large even for SQLUBIGINT */
			while (nval.scale > 0) {
				nval.val /= 10;
				nval.scale--;
			}
			if (nval.scale < 0 || !nval.sign || (maxval != 0 && nval.val >= maxval)) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				return SQL_ERROR;
			}
			if (truncated)
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);

			switch (type) {
			case SQL_C_UTINYINT:
				*(unsigned char *) ptr = (unsigned char) nval.val;

				break;
			case SQL_C_USHORT:
				*(unsigned short *) ptr = (unsigned short) nval.val;

				break;
			case SQL_C_ULONG:
				*(unsigned long *) ptr = (unsigned long) nval.val;

				break;
			case SQL_C_UBIGINT:
				*(SQLUBIGINT *) ptr = (SQLUBIGINT) nval.val;

				break;
			}
			break;
		}
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		break;
	}
	case SQL_C_NUMERIC:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(SQL_NUMERIC_STRUCT) : bind_type));

		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DOUBLE:
		case SQL_REAL:
			/* reparse double and float, parse char */
			if (!(i = parseint(data, &nval))) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			if (i == 2)
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);

			/* fall through */
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT:
			while (nval.precision > precision) {
				nval.val /= 10;
				nval.scale--;
				nval.precision--;
			}
			while (nval.scale < scale) {
				nval.val *= 10;
				nval.scale++;
			}
			while (nval.scale > scale) {
				nval.val /= 10;
				nval.scale--;
				nval.precision--;
			}
			((SQL_NUMERIC_STRUCT *) ptr)->precision = nval.precision;
			((SQL_NUMERIC_STRUCT *) ptr)->scale = nval.scale;
			((SQL_NUMERIC_STRUCT *) ptr)->sign = nval.sign;
			for (i = 0; i < SQL_MAX_NUMERIC_LEN; i++) {
				((SQL_NUMERIC_STRUCT *) ptr)->val[i] = (SQLCHAR) (nval.val & 0xFF);
				nval.val >>= 8;
			}
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		if (lenp)
			*lenp = sizeof(SQL_NUMERIC_STRUCT);
		break;
	case SQL_C_FLOAT:
	case SQL_C_DOUBLE:
		switch (sql_type) {
		case SQL_CHAR:
			if (!parsedouble(data, &fval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			break;
		case SQL_DOUBLE:
		case SQL_REAL:
			break;
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT:
			fval = (double) (SQLBIGINT) nval.val;
			i = 1;
			while (nval.scale > 0) {
				nval.scale--;
				i *= 10;
			}
			fval /= (double) i;
			i = 1;
			while (nval.scale < 0) {
				nval.scale++;
				i *= 10;
			}
			fval *= (double) i;
			if (!nval.sign)
				fval = -fval;
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		if (type == SQL_C_FLOAT) {
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(float) : bind_type));
			*(float *) ptr = (float) fval;

			if ((double) *(float *) ptr !=fval) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				return SQL_ERROR;
			}
			if (lenp)
				*lenp = sizeof(float);
		} else {
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(double) : bind_type));
			*(double *) ptr = fval;

			if (lenp)
				*lenp = sizeof(double);
		}
		break;
	case SQL_C_TYPE_DATE:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(DATE_STRUCT) : bind_type));

		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
			i = parsetimestamp(data, &tsval);
			/* fall through */
		case SQL_TYPE_TIMESTAMP:	/* note i==1 unless we fell through */
			if (i) {
				if (tsval.hour || tsval.minute || tsval.second || tsval.fraction || i == 2) {
					/* Fractional truncation */
					addStmtError(stmt, "01S07", NULL, 0);
				}
				dval.year = tsval.year;
				dval.month = tsval.month;
				dval.day = tsval.day;
			} else if (!parsedate(data, &dval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			/* fall through */
		case SQL_TYPE_DATE:
			*(DATE_STRUCT *) ptr = dval;

			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		if (lenp)
			*lenp = sizeof(DATE_STRUCT);
		break;
	case SQL_C_TYPE_TIME:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(TIME_STRUCT) : bind_type));

		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
			i = parsetimestamp(data, &tsval);
			/* fall through */
		case SQL_TYPE_TIMESTAMP:	/* note i==1 unless we fell through */
			if (i) {
				if (tsval.fraction || i == 2) {
					/* Fractional truncation */
					addStmtError(stmt, "01S07", NULL, 0);
				}
				tval.hour = tsval.hour;
				tval.minute = tsval.minute;
				tval.second = tsval.second;
			} else if (!parsetime(data, &tval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			/* fall through */
		case SQL_TYPE_TIME:
			*(TIME_STRUCT *) ptr = tval;

			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		if (lenp)
			*lenp = sizeof(TIME_STRUCT);
		break;
	case SQL_C_TYPE_TIMESTAMP:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(TIMESTAMP_STRUCT) : bind_type));

		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
			i = parsetimestamp(data, &tsval);
			if (i == 0) {
				i = parsetime(data, &tval);
				if (i) {
					struct tm tm;
					time_t t;

		case SQL_TYPE_TIME:
					(void) time(&t);
#ifdef HAVE_LOCALTIME_R
					(void) localtime_r(&t, &tm);
#else
					tm = *localtime(&t);
#endif
					tsval.year = tm.tm_year + 1900;
					tsval.month = tm.tm_mon + 1;
					tsval.day = tm.tm_mday;
					tsval.hour = tval.hour;
					tsval.minute = tval.minute;
					tsval.second = tval.second;
					tsval.fraction = 0;
				} else {
					i = parsedate(data, &dval);
					if (i) {
		case SQL_TYPE_DATE:
						tsval.year = dval.year;
						tsval.month = dval.month;
						tsval.day = dval.day;
						tsval.hour = 0;
						tsval.minute = 0;
						tsval.second = 0;
						tsval.fraction = 0;
					} else {
						/* Invalid character
						   value for cast
						   specification */
						addStmtError(stmt, "22018", NULL, 0);

						return SQL_ERROR;
					}
				}
			}
			/* fall through */
		case SQL_TYPE_TIMESTAMP:	/* note i==1 unless we fell through */
			*(TIMESTAMP_STRUCT *) ptr = tsval;

			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
		if (lenp)
			*lenp = sizeof(TIMESTAMP_STRUCT);
		break;
	case SQL_C_INTERVAL_YEAR:
	case SQL_C_INTERVAL_MONTH:
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(SQL_INTERVAL_STRUCT) : bind_type));

		switch (sql_type) {
		case SQL_CHAR:{
			int n;

			ival.interval_type = SQL_IS_YEAR_TO_MONTH;
			ival.interval_sign = SQL_TRUE;
			if (sscanf(data, "%d-%u%n", &i, &ival.intval.year_month.month, &n) < 2) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			data += n;
			while (space(*data))
				data++;
			if (*data) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			if (i < 0) {
				ival.interval_sign = SQL_FALSE;
				i = -i;
			}
			ival.intval.year_month.year = i;
			break;
		}
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			parsemonthinterval(&nval, &ival);
			break;
		case SQL_INTERVAL_MONTH:
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
#define p ((SQL_INTERVAL_STRUCT *) ptr)	/* abbrev. */
		p->interval_sign = ival.interval_sign;
		p->intval.year_month.year = 0;
		p->intval.year_month.month = 0;
		switch (type) {
		case SQL_C_INTERVAL_YEAR:
			p->interval_type = SQL_IS_YEAR;
			if ((p->intval.year_month.year = ival.intval.year_month.year) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			if (ival.intval.year_month.month) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_MONTH:
			p->interval_type = SQL_IS_MONTH;
			if ((p->intval.year_month.month = ival.intval.year_month.month + 12 * ival.intval.year_month.year) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			break;
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
			p->interval_type = SQL_IS_YEAR_TO_MONTH;
			if ((p->intval.year_month.year = ival.intval.year_month.year) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.year_month.month = ival.intval.year_month.month;
			break;
		}
#undef p
		if (lenp)
			*lenp = sizeof(SQL_INTERVAL_STRUCT);
		break;
	case SQL_C_INTERVAL_DAY:
	case SQL_C_INTERVAL_HOUR:
	case SQL_C_INTERVAL_MINUTE:
	case SQL_C_INTERVAL_SECOND:
	case SQL_C_INTERVAL_DAY_TO_HOUR:
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
	case SQL_C_INTERVAL_DAY_TO_SECOND:
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
	case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr +row * (bind_type == SQL_BIND_BY_COLUMN ? sizeof(SQL_INTERVAL_STRUCT) : bind_type));

		switch (sql_type) {
		case SQL_CHAR:{
			int n;

			ival.interval_type = SQL_IS_DAY_TO_SECOND;
			ival.interval_sign = SQL_TRUE;
			if (sscanf(data, "%d %u:%u:%u%n", &i, &ival.intval.day_second.hour, &ival.intval.day_second.minute, &ival.intval.day_second.second, &n) < 4) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			if (i < 0) {
				ival.interval_sign = SQL_FALSE;
				i = -i;
			}
			ival.intval.day_second.day = i;
			ival.intval.day_second.fraction = 0;
			data += n;
			i = 0;
			if (*data == '.') {
				n = 1;
				while (*++data && '0' <= *data && *data <= '9') {
					if (n < 1000000000) {
						i++;
						n *= 10;
						ival.intval.day_second.fraction *= 10;
						ival.intval.day_second.fraction += *data - '0';
					}
				}
			}
			while (space(*data))
				data++;
			if (*data) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);

				return SQL_ERROR;
			}
			break;
		}
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			i = parsesecondinterval(&nval, &ival);
			break;
		case SQL_INTERVAL_SECOND:
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);

			return SQL_ERROR;
		}
#define p ((SQL_INTERVAL_STRUCT *) ptr)	/* abbrev. */
		p->interval_sign = ival.interval_sign;
		p->intval.day_second.day = 0;
		p->intval.day_second.hour = 0;
		p->intval.day_second.minute = 0;
		p->intval.day_second.second = 0;
		p->intval.day_second.fraction = 0;
		switch (type) {
		case SQL_C_INTERVAL_DAY:
			p->interval_type = SQL_IS_DAY;
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			if (ival.intval.day_second.hour || ival.intval.day_second.minute || ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_HOUR:
			p->interval_type = SQL_IS_HOUR;
			if ((p->intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			if (ival.intval.day_second.minute || ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_MINUTE:
			p->interval_type = SQL_IS_MINUTE;
			if ((p->intval.day_second.minute = ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day)) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			if (ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_SECOND:
			p->interval_type = SQL_IS_SECOND;
			if ((p->intval.day_second.second = ival.intval.day_second.second + 60 * (ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day))) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_DAY_TO_HOUR:
			p->interval_type = SQL_IS_HOUR;
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.day_second.hour = ival.intval.day_second.hour;
			if (ival.intval.day_second.minute || ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_DAY_TO_MINUTE:
			p->interval_type = SQL_IS_DAY_TO_MINUTE;
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.day_second.hour = ival.intval.day_second.hour;
			p->intval.day_second.minute = ival.intval.day_second.minute;
			if (ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_DAY_TO_SECOND:
			p->interval_type = SQL_IS_DAY_TO_SECOND;
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.day_second.hour = ival.intval.day_second.hour;
			p->intval.day_second.minute = ival.intval.day_second.minute;
			p->intval.day_second.second = ival.intval.day_second.second;
			p->intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_HOUR_TO_MINUTE:
			p->interval_type = SQL_IS_HOUR_TO_MINUTE;
			if ((p->intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.day_second.minute = ival.intval.day_second.minute;
			if (ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_HOUR_TO_SECOND:
			p->interval_type = SQL_IS_HOUR_TO_SECOND;
			if ((p->intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.day_second.minute = ival.intval.day_second.minute;
			p->intval.day_second.second = ival.intval.day_second.second;
			p->intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_MINUTE_TO_SECOND:
			p->interval_type = SQL_IS_MINUTE_TO_SECOND;
			if ((p->intval.day_second.minute = ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day)) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);

				return SQL_ERROR;
			}
			p->intval.day_second.second = ival.intval.day_second.second;
			p->intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		}
		if (p->intval.day_second.fraction) {
			while (i < precision) {
				i++;
				p->intval.day_second.fraction *= 10;
			}
			while (i > precision) {
				i--;
				p->intval.day_second.fraction /= 10;
			}
		}
#undef p
		if (lenp)
			*lenp = sizeof(SQL_INTERVAL_STRUCT);
		break;
	default:
		/* Invalid application buffer type */
		addStmtError(stmt, "HY003", NULL, 0);

		return SQL_ERROR;
	}
	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN
SQLFetch_(ODBCStmt *stmt)
{
	ODBCDesc *desc;
	ODBCDescRec *rec;
	int i;
	unsigned int row;
	SQLINTEGER offset;
	SQLUSMALLINT *statusp;

	desc = stmt->ApplRowDescr;

	stmt->retrieved = 0;
	stmt->currentCol = 0;

	stmt->startRow += stmt->rowSetSize;
	stmt->rowSetSize = 0;
	stmt->currentRow = stmt->startRow + 1;
	if (mapi_seek_row(stmt->hdl, stmt->startRow, MAPI_SEEK_SET) != MOK) {
		/* Row value out of range */
		addStmtError(stmt, "HY107", mapi_error_str(stmt->Dbc->mid), 0);

		return SQL_ERROR;
	}

	stmt->State = FETCHED;

	statusp = desc->sql_desc_array_status_ptr;

	if (stmt->retrieveData == SQL_RD_OFF) {
		/* don't really retrieve the data, just do as if,
		   updating the SQL_DESC_ARRAY_STATUS_PTR */
		stmt->rowSetSize = desc->sql_desc_array_size;

		if (stmt->startRow + stmt->rowSetSize > stmt->rowcount)
			stmt->rowSetSize = stmt->rowcount - stmt->startRow;

		if (stmt->rowSetSize <= 0) {
			stmt->rowSetSize = 0;

			return SQL_NO_DATA;
		}
		if (statusp) {
			for (row = 0; row < stmt->rowSetSize; row++)
				*statusp++ = SQL_ROW_SUCCESS;
			for (; row < desc->sql_desc_array_size; row++)
				*statusp++ = SQL_ROW_NOROW;
		}
		return SQL_SUCCESS;
	}

	for (row = 0; row < desc->sql_desc_array_size; row++) {
		if (mapi_fetch_row(stmt->hdl) == 0) {
			if (desc->sql_desc_rows_processed_ptr)
				*desc->sql_desc_rows_processed_ptr = row;
			switch (mapi_error(stmt->Dbc->mid)) {
			case MOK:
				if (row == 0)
					return SQL_NO_DATA;
				break;
			case MTIMEOUT:
				if (statusp)
					*statusp = SQL_ROW_ERROR;
				/* Communication link failure */
				addStmtError(stmt, "08S01", mapi_error_str(stmt->Dbc->mid), 0);

				return SQL_ERROR;
			default:
				if (statusp)
					*statusp = SQL_ROW_ERROR;
				/* General error */
				addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);

				return SQL_ERROR;
			}
			break;
		}
		if (statusp)
			*statusp = SQL_ROW_SUCCESS;

		stmt->rowSetSize++;

		if (desc->sql_desc_bind_offset_ptr)
			offset = *desc->sql_desc_bind_offset_ptr;
		else
			offset = 0;
		for (i = 1; i <= desc->sql_desc_count; i++) {
			rec = &desc->descRec[i];
			if (rec->sql_desc_data_ptr == NULL)
				continue;
			stmt->retrieved = 0;
			if (ODBCFetch
			    (stmt, i, rec->sql_desc_concise_type, rec->sql_desc_data_ptr, rec->sql_desc_octet_length, rec->sql_desc_octet_length_ptr, rec->sql_desc_indicator_ptr, rec->sql_desc_precision, rec->sql_desc_scale,
			     rec->sql_desc_datetime_interval_precision, offset, row) == SQL_ERROR) {
				if (statusp)
					*statusp = SQL_ROW_SUCCESS_WITH_INFO;
			}
		}
		if (statusp)
			statusp++;
	}
	if (desc->sql_desc_rows_processed_ptr)
		*desc->sql_desc_rows_processed_ptr = stmt->rowSetSize;

	if (statusp)
		while (row++ < desc->sql_desc_array_size)
			*statusp++ = SQL_ROW_NOROW;

	if (stmt->rowSetSize > 1) {
		mapi_seek_row(stmt->hdl, stmt->startRow, MAPI_SEEK_SET);
		mapi_fetch_row(stmt->hdl);
	}

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLFetch(SQLHSTMT hStmt)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLFetch " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	assert(stmt->hdl);

	/* check statement cursor state, query should be executed */
	if (stmt->State < EXECUTED0 || stmt->State == EXTENDEDFETCHED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);

		return SQL_ERROR;
	}
	if (stmt->State == EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);

		return SQL_ERROR;
	}

	return SQLFetch_(stmt);
}
