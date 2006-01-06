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

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"
#include <time.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif

#if SIZEOF_INT==8
# define ULL_CONSTANT(val)	(val)
# define O_ULLFMT			"u"
#elif SIZEOF_LONG==8
# define ULL_CONSTANT(val)	(val##UL)
# define O_ULLFMT			"lu"
#elif defined(HAVE_LONG_LONG)
# define ULL_CONSTANT(val)	(val##ULL)
# define O_ULLFMT			"llu"
#elif defined(HAVE___INT64)
# define ULL_CONSTANT(val)	(val##ui64)
# define O_ULLFMT			"I64u"
#endif

#define MAXBIGNUM10	ULL_CONSTANT(1844674407370955161)	/* (2**64-1)/10 */
#define MAXBIGNUMLAST	'5'	/* (2**64-1)%10 */

#define space(c)	((c) == ' ' || (c) == '\t')

typedef struct {
	unsigned char precision; /* total number of digits */
	signed char scale;	/* how far to shift decimal point (>
				   0: shift left, i.e. number has
				   fraction; < 0: shift right,
				   i.e. multiply with power of 10) */
	unsigned char sign;	/* 1 pos, 0 neg */
	SQLUBIGINT val;		/* the value */
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
parseint(const char *data, bignum_t *nval)
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
parsesecondinterval(bignum_t *nval, SQL_INTERVAL_STRUCT *ival)
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
parsemonthinterval(bignum_t *nval, SQL_INTERVAL_STRUCT *ival)
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
parsedate(const char *data, DATE_STRUCT *dval)
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
parsetime(const char *data, TIME_STRUCT *tval)
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
		while (*++data && '0' <= *data && *data <= '9')
			;
		n = 2;		/* indicate loss of precision */
	}
	while (space(*data))
		data++;
	if (*data)
		return 0;
	return n;
}

static int
parsetimestamp(const char *data, TIMESTAMP_STRUCT *tsval)
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

static SQLSMALLINT
ODBCDefaultType(ODBCDescRec *rec)
{
	switch (rec->sql_desc_concise_type) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_DECIMAL:
	case SQL_NUMERIC:
		return SQL_C_CHAR;
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		return SQL_C_WCHAR;
	case SQL_BIT:
		return SQL_C_BIT;
	case SQL_TINYINT:
		return rec->sql_desc_unsigned ? SQL_C_UTINYINT : SQL_C_STINYINT;
	case SQL_SMALLINT:
		return rec->sql_desc_unsigned ? SQL_C_USHORT : SQL_C_SSHORT;
	case SQL_INTEGER:
		return rec->sql_desc_unsigned ? SQL_C_ULONG : SQL_C_SLONG;
	case SQL_BIGINT:
		return rec->sql_desc_unsigned ? SQL_C_UBIGINT : SQL_C_SBIGINT;
	case SQL_REAL:
		return SQL_C_FLOAT;
	case SQL_FLOAT:
	case SQL_DOUBLE:
		return SQL_C_DOUBLE;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		return SQL_C_BINARY;
	case SQL_TYPE_DATE:
		return SQL_C_TYPE_DATE;
	case SQL_TYPE_TIME:
		return SQL_C_TYPE_TIME;
	case SQL_TYPE_TIMESTAMP:
		return SQL_C_TYPE_TIMESTAMP;
	case SQL_INTERVAL_YEAR:
		return SQL_C_INTERVAL_YEAR;
	case SQL_INTERVAL_MONTH:
		return SQL_C_INTERVAL_MONTH;
	case SQL_INTERVAL_YEAR_TO_MONTH:
		return SQL_C_INTERVAL_YEAR_TO_MONTH;
	case SQL_INTERVAL_DAY:
		return SQL_C_INTERVAL_DAY;
	case SQL_INTERVAL_HOUR:
		return SQL_C_INTERVAL_HOUR;
	case SQL_INTERVAL_MINUTE:
		return SQL_C_INTERVAL_MINUTE;
	case SQL_INTERVAL_SECOND:
		return SQL_C_INTERVAL_SECOND;
	case SQL_INTERVAL_DAY_TO_HOUR:
		return SQL_C_INTERVAL_DAY_TO_HOUR;
	case SQL_INTERVAL_DAY_TO_MINUTE:
		return SQL_C_INTERVAL_DAY_TO_MINUTE;
	case SQL_INTERVAL_DAY_TO_SECOND:
		return SQL_C_INTERVAL_DAY_TO_SECOND;
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		return SQL_C_INTERVAL_HOUR_TO_MINUTE;
	case SQL_INTERVAL_HOUR_TO_SECOND:
		return SQL_C_INTERVAL_HOUR_TO_SECOND;
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		return SQL_C_INTERVAL_MINUTE_TO_SECOND;
	case SQL_GUID:
		return SQL_C_GUID;
	}
	return 0;
}

SQLRETURN
ODBCFetch(ODBCStmt *stmt, SQLUSMALLINT col, SQLSMALLINT type, SQLPOINTER ptr,
	  SQLINTEGER buflen, SQLINTEGER *lenp, SQLINTEGER *nullp,
	  SQLSMALLINT precision, SQLSMALLINT scale,
	  SQLINTEGER datetime_interval_precision, SQLINTEGER offset, int row)
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

	if (type == SQL_C_DEFAULT)
		type = ODBCDefaultType(irdrec);

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
	case SQL_C_WCHAR: {
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
		case SQL_BIT: {
			int f, n;

			data = (char *) ptr;

			for (n = 0, f = 1; n < nval.scale; n++)
				f *= 10;
			sz = snprintf(data, buflen, "%s%" O_ULLFMT, nval.sign ? "" : "-", nval.val / f);
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
					sz = snprintf(data, buflen, ".%0*" O_ULLFMT, nval.scale, nval.val % f);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		}
		case SQL_DOUBLE:
		case SQL_REAL: {
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

			if (fval != 0 && fval != 1) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);
			}
			break;
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT: {
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

			if (truncated) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
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
	case SQL_C_SBIGINT: {
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
		case SQL_BIT: {
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
			if (truncated) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}

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
	case SQL_C_UBIGINT: {
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
		case SQL_BIT: {
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
			if (truncated) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}

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
			if (i == 2) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}

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
		case SQL_CHAR: {
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
		case SQL_CHAR: {
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

#define assign(buf,bufpos,buflen,value,stmt)				\
		do {							\
			if (bufpos >= buflen) {				\
				buf = realloc(buf, buflen += 1024);	\
				if (buf == NULL) {			\
					/* Memory allocation error */	\
					addStmtError(stmt, "HY001", NULL, 0); \
					return SQL_ERROR;		\
				}					\
			}						\
			buf[bufpos++] = (value);			\
		} while (0)
#define assigns(buf,bufpos,buflen,value,stmt)				\
		do {							\
			size_t _len = strlen(value);			\
			size_t _i;					\
			while (bufpos + _len >= buflen) {		\
				buf = realloc(buf, buflen += 1024);	\
				if (buf == NULL) {			\
					/* Memory allocation error */	\
					addStmtError(stmt, "HY001", NULL, 0); \
					return SQL_ERROR;		\
				}					\
			}						\
			for (_i = 0; _i < _len; _i++)			\
				buf[bufpos++] = (value)[_i];		\
		} while (0)

SQLRETURN
ODBCStore(ODBCStmt *stmt, SQLUSMALLINT param, char **bufp, size_t *bufposp, size_t *buflenp, char *sep)
{
	ODBCDescRec *ipdrec, *apdrec;
	SQLSMALLINT ctype, sqltype;
	SQLCHAR *sval = NULL;
	SQLINTEGER slen = 0;
	bignum_t nval;
	double fval;
	DATE_STRUCT dval;
	TIME_STRUCT tval;
	TIMESTAMP_STRUCT tsval;
	SQL_INTERVAL_STRUCT ival;
	char *buf = *bufp;
	size_t bufpos = *bufposp;
	size_t buflen = *buflenp;
	char data[256];
	int i;

	ipdrec = stmt->ImplParamDescr->descRec + param;
	apdrec = stmt->ApplParamDescr->descRec + param;

	ctype = apdrec->sql_desc_concise_type;
	sqltype = ipdrec->sql_desc_concise_type;
	if (ctype == SQL_C_DEFAULT)
		ctype = ODBCDefaultType(ipdrec);

	switch (ctype) {
	case SQL_C_TINYINT:
		ctype = apdrec->sql_desc_unsigned ? SQL_C_UTINYINT : SQL_C_STINYINT;
		break;
	case SQL_C_SHORT:
		ctype = apdrec->sql_desc_unsigned ? SQL_C_USHORT : SQL_C_SSHORT;
		break;
	case SQL_C_LONG:
		ctype = apdrec->sql_desc_unsigned ? SQL_C_ULONG : SQL_C_SLONG;
		break;
	default:
		break;
	}
	switch (ctype) {
	case SQL_C_CHAR:
	case SQL_C_BINARY:
		slen = apdrec->sql_desc_octet_length_ptr ? *apdrec->sql_desc_octet_length_ptr : SQL_NTS;
		sval = (SQLCHAR *) apdrec->sql_desc_data_ptr;
		fixODBCstring(sval, slen, addStmtError, stmt);
		break;
	case SQL_C_WCHAR:
		slen = apdrec->sql_desc_octet_length_ptr ? *apdrec->sql_desc_octet_length_ptr : SQL_NTS;
		sval = (SQLCHAR *) apdrec->sql_desc_data_ptr;
		fixWcharIn((SQLWCHAR *) apdrec->sql_desc_data_ptr, slen, sval, addStmtError, stmt, return SQL_ERROR);
		break;
	case SQL_C_BIT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLCHAR *) apdrec->sql_desc_data_ptr != 0;
		break;
	case SQL_C_STINYINT:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLSCHAR *) apdrec->sql_desc_data_ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLSCHAR *) apdrec->sql_desc_data_ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLSCHAR *) apdrec->sql_desc_data_ptr;
		}
		break;
	case SQL_C_UTINYINT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLCHAR *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_SSHORT:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLSMALLINT *) apdrec->sql_desc_data_ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLSMALLINT *) apdrec->sql_desc_data_ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLSMALLINT *) apdrec->sql_desc_data_ptr;
		}
		break;
	case SQL_C_USHORT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLUSMALLINT *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_SLONG:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLINTEGER *) apdrec->sql_desc_data_ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLINTEGER *) apdrec->sql_desc_data_ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLINTEGER *) apdrec->sql_desc_data_ptr;
		}
		break;
	case SQL_C_ULONG:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLUINTEGER *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_SBIGINT:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLBIGINT *) apdrec->sql_desc_data_ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLBIGINT *) apdrec->sql_desc_data_ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLBIGINT *) apdrec->sql_desc_data_ptr;
		}
		break;
	case SQL_C_UBIGINT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLUBIGINT *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_NUMERIC:
		nval.precision = apdrec->sql_desc_precision;
		nval.scale = apdrec->sql_desc_scale;
		nval.sign = ((SQL_NUMERIC_STRUCT *) apdrec->sql_desc_data_ptr)->sign;
		nval.val = 0;
		for (i = 0; i < SQL_MAX_NUMERIC_LEN; i++)
			nval.val |= ((SQL_NUMERIC_STRUCT *) apdrec->sql_desc_data_ptr)->val[i] << (i * 8);
		break;
	case SQL_C_FLOAT:
		fval = * (SQLREAL *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_DOUBLE:
		fval = * (SQLDOUBLE *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_TYPE_DATE:
		dval = * (SQL_DATE_STRUCT *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_TYPE_TIME:
		tval = * (SQL_TIME_STRUCT *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_TYPE_TIMESTAMP:
		tsval = * (SQL_TIMESTAMP_STRUCT *) apdrec->sql_desc_data_ptr;
		break;
	case SQL_C_INTERVAL_YEAR:
		ival.interval_type = SQL_IS_YEAR;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.year_month.year = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.year_month.year;
		ival.intval.year_month.month = 0;
		break;
	case SQL_C_INTERVAL_MONTH:
		ival.interval_type = SQL_IS_MONTH;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.year_month.year = 0;
		ival.intval.year_month.month = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.year_month.month;
		break;
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
		ival.interval_type = SQL_IS_YEAR_TO_MONTH;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.year_month.year = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.year_month.year;
		ival.intval.year_month.month = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.year_month.month;
		break;
	case SQL_C_INTERVAL_DAY:
		ival.interval_type = SQL_IS_DAY;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.day;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_HOUR:
		ival.interval_type = SQL_IS_HOUR;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_MINUTE:
		ival.interval_type = SQL_IS_MINUTE;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.minute;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_SECOND:
		ival.interval_type = SQL_IS_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.fraction;
		break;
	case SQL_C_INTERVAL_DAY_TO_HOUR:
		ival.interval_type = SQL_IS_DAY_TO_HOUR;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.day;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
		ival.interval_type = SQL_IS_DAY_TO_MINUTE;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.day;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.minute;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_DAY_TO_SECOND:
		ival.interval_type = SQL_IS_DAY_TO_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.day;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.minute;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.fraction;
		break;
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
		ival.interval_type = SQL_IS_HOUR_TO_MINUTE;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.minute;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
		ival.interval_type = SQL_IS_HOUR_TO_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.minute;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.fraction;
		break;
	case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		ival.interval_type = SQL_IS_MINUTE_TO_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.minute;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) apdrec->sql_desc_data_ptr)->intval.day_second.fraction;
		break;
	case SQL_C_GUID:
		break;
	}

	assigns(buf, bufpos, buflen, sep, stmt);
	/* just the types supported by the server */
	switch (sqltype) {
	case SQL_CHAR:
	case SQL_VARCHAR:
		assign(buf, bufpos, buflen, '\'', stmt);
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			for (i = 0; i < slen; i++) {
				SQLCHAR c = sval[i];

				if (c < 0x20 || c >= 0x7F) {
					assign(buf, bufpos, buflen, '\\', stmt);
					assign(buf, bufpos, buflen, c >> 6, stmt);
					assign(buf, bufpos, buflen, (c >> 3) & 0x7, stmt);
					assign(buf, bufpos, buflen, c & 0x7, stmt);
				} else if (c == '\\') {
					assign(buf, bufpos, buflen, '\\', stmt);
					assign(buf, bufpos, buflen, '\\', stmt);
				} else if (c == '\'') {
					assign(buf, bufpos, buflen, '\\', stmt);
					assign(buf, bufpos, buflen, '\'', stmt);
				} else {
					assign(buf, bufpos, buflen, c, stmt);
				}
			}
			break;
		case SQL_C_BIT:
			if (nval.val)
				assigns(buf, bufpos, buflen, "true", stmt);
			else
				assigns(buf, bufpos, buflen, "false", stmt);
			break;
		case SQL_C_STINYINT:
		case SQL_C_UTINYINT:
		case SQL_C_SSHORT:
		case SQL_C_USHORT:
		case SQL_C_SLONG:
		case SQL_C_ULONG:
		case SQL_C_SBIGINT:
		case SQL_C_UBIGINT:
		case SQL_C_NUMERIC: {
			int f, n;

			for (n = 0, f = 1; n < nval.scale; n++)
				f *= 10;
			snprintf(data, sizeof(data), "%s%" O_ULLFMT, nval.sign ? "" : "-", nval.val / f);
			assigns(buf, bufpos, buflen, data, stmt);
			if (nval.scale > 0) {
				snprintf(data, sizeof(data), ".%0*" O_ULLFMT, nval.scale, nval.val % f);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		}
		case SQL_C_FLOAT:
		case SQL_C_DOUBLE: {
			for (i = 0; i < 18; i++) {
				snprintf(data, sizeof(data), "%.*g", i, fval);
				if (fval == strtod(data, NULL))
					break;
			}
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		}
		case SQL_C_TYPE_DATE:
			snprintf(data, sizeof(data), "%04d-%02u-%02u", dval.year, dval.month, dval.day);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_TYPE_TIME:
			snprintf(data, sizeof(data), "%02u:%02u:%02u", tval.hour, tval.minute, tval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_TYPE_TIMESTAMP:
			snprintf(data, sizeof(data), "%04d-%02u-%02u %02u:%02u:%02u", tsval.year, tsval.month, tsval.day, tsval.hour, tsval.minute, tsval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (tsval.fraction) {
				snprintf(data, sizeof(data), ".%09u", tsval.fraction);
				/* remove trailing zeros */
				for (i = 9; i > 0 && data[i] == '0'; i--)
					data[i] = 0;
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_YEAR:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.year_month.year);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_MONTH:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.year_month.month);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
			snprintf(data, sizeof(data), "%s%0*u-%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.year_month.year, ival.intval.year_month.month);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_DAY:
			snprintf(data, sizeof(data), "%s%0*d", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.day);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_HOUR:
			snprintf(data, sizeof(data), "%s%0*d", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.hour);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_MINUTE:
			snprintf(data, sizeof(data), "%s%0*d", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.minute);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_SECOND:
			snprintf(data, sizeof(data), "%s%0*d", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && apdrec->sql_desc_precision > 0) {
				snprintf(data, sizeof(data), ".%0*u", (int) apdrec->sql_desc_precision, ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_DAY_TO_HOUR:
			snprintf(data, sizeof(data), "%s%0*u %02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.day, ival.intval.day_second.hour);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_DAY_TO_MINUTE:
			snprintf(data, sizeof(data), "%s%0*u %02u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.day, ival.intval.day_second.hour, ival.intval.day_second.minute);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_DAY_TO_SECOND:
			snprintf(data, sizeof(data), "%s%0*u %02u:%02u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.day, ival.intval.day_second.hour, ival.intval.day_second.minute, ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && apdrec->sql_desc_precision > 0) {
				snprintf(data, sizeof(data), ".%0*u", (int) apdrec->sql_desc_precision, ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_HOUR_TO_MINUTE:
			snprintf(data, sizeof(data), "%s%0*u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.hour, ival.intval.day_second.minute);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_HOUR_TO_SECOND:
			snprintf(data, sizeof(data), "%s%0*u:%02u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.hour, ival.intval.day_second.minute, ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && apdrec->sql_desc_precision > 0) {
				snprintf(data, sizeof(data), ".%0*u", (int) apdrec->sql_desc_precision, ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_MINUTE_TO_SECOND:
			snprintf(data, sizeof(data), "%s%0*u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, ival.intval.day_second.minute, ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && apdrec->sql_desc_precision > 0) {
				snprintf(data, sizeof(data), ".%0*u", (int) apdrec->sql_desc_precision, ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_GUID:
			break;
		}
		assign(buf, bufpos, buflen, '\'', stmt);
		break;
	case SQL_TYPE_DATE:
		i = 1;
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			i = parsetimestamp((char *) sval, &tsval);
			/* fall through */
		case SQL_C_TYPE_TIMESTAMP:
			if (i) {
				if (tsval.hour || tsval.minute || tsval.second || tsval.fraction || i == 2) {
					/* Datetime field overflow */
					addStmtError(stmt, "22008", NULL, 0);
				}
				dval.year = tsval.year;
				dval.month = tsval.month;
				dval.day = tsval.day;
			} else if (!parsedate((char *) sval, &dval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		case SQL_C_TYPE_DATE:
			snprintf(data, sizeof(data), "DATE '%u-%02u-%02u'", dval.year, dval.month, dval.day);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_TYPE_TIME:
		i = 1;
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			i = parsetimestamp((char *) sval, &tsval);
			/* fall through */
		case SQL_C_TYPE_TIMESTAMP:
			if (i) {
				if (tsval.fraction || i == 2) {
					/* Datetime field overflow */
					addStmtError(stmt, "22008", NULL, 0);
				}
				tval.hour = tsval.hour;
				tval.minute = tsval.minute;
				tval.second = tsval.second;
			} else if (!parsetime((char *) sval, &tval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		case SQL_C_TYPE_TIME:
			snprintf(data, sizeof(data), "TIME '%u:%02u:%02u'", tval.hour, tval.minute, tval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_TYPE_TIMESTAMP:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			i = parsetimestamp((char *) sval, &tsval);
			if (i == 0) {
				i = parsetime((char *) sval, &tval);
				if (i) {
					struct tm tm;
					time_t t;

		case SQL_C_TYPE_TIME:
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
					i = parsedate((char *) sval, &dval);
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
		case SQL_C_TYPE_TIMESTAMP:
			snprintf(data, sizeof(data), "TIMESTAMP '%u-%02d-%02d %02u:%02u:%02u", tsval.year, tsval.month, tsval.day, tsval.hour, tsval.minute, tsval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (tsval.fraction) {
				snprintf(data, sizeof(data), ".%09u", tsval.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			assign(buf, bufpos, buflen, '\'', stmt);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_INTERVAL_MONTH:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY: {
			int n;

			ival.interval_sign = SQL_TRUE;
			if (sscanf((char *) sval, "%d-%u%n", &i, &ival.intval.year_month.month, &n) < 2) {
				if (sscanf((char *) sval, "%d%n", &i, &n) < 1) {
					/* Invalid character value for
					   cast specification */
					addStmtError(stmt, "22018", NULL, 0);
					return SQL_ERROR;
				}
				ival.interval_type = SQL_IS_MONTH;
				if (i < 0) {
					ival.interval_sign = SQL_FALSE;
					i = -i;
				}
				ival.intval.year_month.month = i;
			} else {
				sval += n;
				while (space(*sval))
					sval++;
				if (*sval) {
					/* Invalid character value for cast
					   specification */
					addStmtError(stmt, "22018", NULL, 0);
					return SQL_ERROR;
				}
				ival.interval_type = SQL_IS_YEAR_TO_MONTH;
				if (i < 0) {
					ival.interval_sign = SQL_FALSE;
					i = -i;
				}
				ival.intval.year_month.year = i;
			}
			break;
		}
		case SQL_C_BIT:
		case SQL_C_STINYINT:
		case SQL_C_UTINYINT:
		case SQL_C_TINYINT:
		case SQL_C_SSHORT:
		case SQL_C_USHORT:
		case SQL_C_SHORT:
		case SQL_C_SLONG:
		case SQL_C_ULONG:
		case SQL_C_LONG:
		case SQL_C_SBIGINT:
		case SQL_C_UBIGINT:
		case SQL_C_NUMERIC:
			parsemonthinterval(&nval, &ival);
			break;
		case SQL_C_INTERVAL_YEAR:
		case SQL_C_INTERVAL_MONTH:
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		switch (ival.interval_type) {
		case SQL_IS_YEAR:
			snprintf(data, sizeof(data), "INTERVAL %s'%u' YEAR", ival.interval_sign ? "" : "- ", ival.intval.year_month.year);
			break;
		case SQL_IS_MONTH:
			snprintf(data, sizeof(data), "INTERVAL %s'%u' MONTH", ival.interval_sign ? "" : "- ", ival.intval.year_month.month);
			break;
		case SQL_IS_YEAR_TO_MONTH:
			snprintf(data, sizeof(data), "INTERVAL %s'%u-%u' YEAR TO MONTH", ival.interval_sign ? "" : "- ", ival.intval.year_month.year, ival.intval.year_month.month);
			break;
		default:
			break;
		}
		assigns(buf, bufpos, buflen, data, stmt);
		break;
	case SQL_INTERVAL_SECOND:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
		case SQL_C_BIT:
		case SQL_C_STINYINT:
		case SQL_C_UTINYINT:
		case SQL_C_TINYINT:
		case SQL_C_SSHORT:
		case SQL_C_USHORT:
		case SQL_C_SHORT:
		case SQL_C_SLONG:
		case SQL_C_ULONG:
		case SQL_C_LONG:
		case SQL_C_SBIGINT:
		case SQL_C_UBIGINT:
		case SQL_C_NUMERIC:
		case SQL_C_FLOAT:
		case SQL_C_DOUBLE:
		case SQL_C_TYPE_DATE:
		case SQL_C_TYPE_TIME:
		case SQL_C_TYPE_TIMESTAMP:
		case SQL_C_INTERVAL_YEAR:
		case SQL_C_INTERVAL_MONTH:
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
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
		case SQL_C_GUID:
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_DECIMAL:
	case SQL_SMALLINT:
	case SQL_INTEGER:
	case SQL_BIGINT:
	case SQL_BIT:
		/* first convert to nval (if not already done) */
		switch (ctype) {
		case SQL_C_FLOAT:
		case SQL_C_DOUBLE:
			for (i = 0; i < 18; i++) {
				snprintf(data, sizeof(data), "%.*g", i, fval);
				if (fval == strtod(data, NULL))
					break;
			}
			sval = (SQLCHAR *) data;
			/* fall through */
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			/* parse character data, reparse floating
			   point number */
			if (!parseint((char *) sval, &nval)) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		case SQL_C_BIT:
		case SQL_C_STINYINT:
		case SQL_C_UTINYINT:
		case SQL_C_TINYINT:
		case SQL_C_SSHORT:
		case SQL_C_USHORT:
		case SQL_C_SHORT:
		case SQL_C_SLONG:
		case SQL_C_ULONG:
		case SQL_C_LONG:
		case SQL_C_SBIGINT:
		case SQL_C_UBIGINT:
		case SQL_C_NUMERIC:
			break;
		case SQL_C_INTERVAL_YEAR:
			nval.precision = 0;
			nval.scale = 0;
			nval.sign = !ival.interval_sign;
			nval.val = ival.intval.year_month.year;
			break;
		case SQL_C_INTERVAL_MONTH:
			nval.precision = 0;
			nval.scale = 0;
			nval.sign = !ival.interval_sign;
			nval.val = 12 * ival.intval.year_month.year + ival.intval.year_month.month;
			break;
		case SQL_C_INTERVAL_DAY:
			nval.precision = 0;
			nval.scale = 0;
			nval.sign = !ival.interval_sign;
			nval.val = ival.intval.day_second.day;
			break;
		case SQL_C_INTERVAL_HOUR:
			nval.precision = 0;
			nval.scale = 0;
			nval.sign = !ival.interval_sign;
			nval.val = 24 * ival.intval.day_second.day + ival.intval.day_second.hour;
			break;
		case SQL_C_INTERVAL_MINUTE:
			nval.precision = 0;
			nval.scale = 0;
			nval.sign = !ival.interval_sign;
			nval.val = 60 * (24 * ival.intval.day_second.day + ival.intval.day_second.hour) + ival.intval.day_second.minute;
			break;
		case SQL_C_INTERVAL_SECOND:
			nval.precision = 0;
			nval.scale = 0;
			nval.sign = !ival.interval_sign;
			nval.val = 60 * (60 * (24 * ival.intval.day_second.day + ival.intval.day_second.hour) + ival.intval.day_second.minute) + ival.intval.day_second.second;
			if (ival.intval.day_second.fraction && apdrec->sql_desc_precision > 0) {
				for (i = 0; i < apdrec->sql_desc_precision; i++) {
					nval.val *= 10;
					nval.scale++;
				}
				nval.val += ival.intval.day_second.fraction;
			}
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		/* now store value contained in nval */
		{
			int f = 1;

			for (i = 0; i < nval.scale; i++)
				f *= 10;
			if (sqltype == SQL_BIT) {
				switch (nval.val / f) {
				case 0:
					assigns(buf, bufpos, buflen, "false", stmt);
					break;
				case 1:
					assigns(buf, bufpos, buflen, "true", stmt);
					break;
				default:
					/* Numeric value out of range */
					addStmtError(stmt, "22003", NULL, 0);
					return SQL_ERROR;
				}
				if (f > 1 && nval.val % f) {
					/* String data, right truncation */
					addStmtError(stmt, "22001", NULL, 0);
				}
			} else {
				snprintf(data, sizeof(data), "%s%" O_ULLFMT, nval.sign ? "" : "-", nval.val / f);
				assigns(buf, bufpos, buflen, data, stmt);
				if (nval.scale > 0) {
					if (sqltype == SQL_DECIMAL) {
						snprintf(data, sizeof(data), ".%0*" O_ULLFMT, nval.scale, nval.val % f);
						assigns(buf, bufpos, buflen, data, stmt);
					} else {
						/* Fractional truncation */
						addStmtError(stmt, "01S07", NULL, 0);
					}
				} else {
					for (i = nval.scale; i < 0; i++)
						assign(buf, bufpos, buflen, '0', stmt);
				}
			}
		}
		break;
	case SQL_REAL:
	case SQL_DOUBLE:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			if (!parsedouble((char *) sval, &fval)) {
				/* Invalid character value for cast specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			break;
		case SQL_C_BIT:
		case SQL_C_STINYINT:
		case SQL_C_UTINYINT:
		case SQL_C_TINYINT:
		case SQL_C_SSHORT:
		case SQL_C_USHORT:
		case SQL_C_SHORT:
		case SQL_C_SLONG:
		case SQL_C_ULONG:
		case SQL_C_LONG:
		case SQL_C_SBIGINT:
		case SQL_C_UBIGINT:
		case SQL_C_NUMERIC:
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
		case SQL_C_FLOAT:
		case SQL_C_DOUBLE:
			break;
		case SQL_C_INTERVAL_YEAR:
			fval = (double) ival.intval.year_month.year;
			if (ival.interval_sign)
				fval = -fval;
			break;
		case SQL_C_INTERVAL_MONTH:
			fval = (double) (12 * ival.intval.year_month.year + ival.intval.year_month.month);
			if (ival.interval_sign)
				fval = -fval;
			break;
		case SQL_C_INTERVAL_DAY:
			fval = (double) ival.intval.day_second.day;
			if (ival.interval_sign)
				fval = -fval;
			break;
		case SQL_C_INTERVAL_HOUR:
			fval = (double) (24 * ival.intval.day_second.day + ival.intval.day_second.hour);
			if (ival.interval_sign)
				fval = -fval;
			break;
		case SQL_C_INTERVAL_MINUTE:
			fval = (double) (60 * (24 * ival.intval.day_second.day + ival.intval.day_second.hour) + ival.intval.day_second.minute);
			if (ival.interval_sign)
				fval = -fval;
			break;
		case SQL_C_INTERVAL_SECOND:
			fval = (double) (60 * (60 * (24 * ival.intval.day_second.day + ival.intval.day_second.hour) + ival.intval.day_second.minute) + ival.intval.day_second.second);
			if (ival.intval.day_second.fraction && apdrec->sql_desc_precision > 0) {
				int f = 1;
				
				for (i = 0; i < apdrec->sql_desc_precision; i++)
					f *= 10;
				fval += ival.intval.day_second.fraction / (double) f;
			}
			if (ival.interval_sign)
				fval = -fval;
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		for (i = 1; i < 18; i++) {
			snprintf(data, sizeof(data), "%.*e", i, fval);
			if (fval == strtod(data, NULL))
				break;
		}
		assigns(buf, bufpos, buflen, data, stmt);
		break;
	}
	*bufp = buf;
	*bufposp = bufpos;
	*buflenp = buflen;
	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
