/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"
#include <errno.h>
#include <time.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif
#include <float.h>		/* for FLT_MAX */

#if SIZEOF_INT==8
# define ULL_CONSTANT(val)	(val)
# define O_ULLFMT		"u"
# define O_ULLCAST	(unsigned int)
#elif SIZEOF_LONG==8
# define ULL_CONSTANT(val)	(val##UL)
# define O_ULLFMT		"lu"
# define O_ULLCAST	(unsigned long)
#elif defined(HAVE_LONG_LONG)
# define ULL_CONSTANT(val)	(val##ULL)
# define O_ULLFMT		"llu"
# define O_ULLCAST	(unsigned long long)
#elif defined(HAVE___INT64)
# define ULL_CONSTANT(val)	(val##ui64)
# define O_ULLFMT		"I64u"
# define O_ULLCAST	(unsigned __int64)
#endif

#define MAXBIGNUM10	ULL_CONSTANT(1844674407370955161) /* (2**64-1)/10 */
#define MAXBIGNUMLAST	'5'	/* (2**64-1)%10 */

#define space(c)	((c) == ' ' || (c) == '\t')

typedef struct {
	unsigned char precision; /* total number of digits */
	signed char scale;	/* how far to shift decimal point (>
				 * 0: shift left, i.e. number has
				 * fraction; < 0: shift right,
				 * i.e. multiply with power of 10) */
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
		if (c1 != c2 && tolower(c1) != tolower(c2))
			return tolower(c1) - tolower(c2);
		n--;
	}
	return 0;
}
#endif

/* Parse a number and store in a bignum_t.
 * 1 is returned if all is well;
 * 2 is returned if there is loss of precision (i.e. overflow of the value);
 * 0 is returned if the string is not a number, or if scale doesn't fit.
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
			if (overflow ||
			    nval->val > MAXBIGNUM10 ||
			    (nval->val == MAXBIGNUM10 &&
			     *data > MAXBIGNUMLAST)) {
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
parsesecondinterval(bignum_t *nval, SQL_INTERVAL_STRUCT *ival, int type)
{
	unsigned int f = 1;
	int ivalscale = 0;

	/* convert value to second */
	switch (type) {
	case SQL_INTERVAL_DAY:	/* SQL_C_INTERVAL_DAY */
		nval->val *= 24;
		/* fall through */
	case SQL_INTERVAL_HOUR: /* SQL_C_INTERVAL_HOUR */
	case SQL_INTERVAL_DAY_TO_HOUR: /* SQL_C_INTERVAL_DAY_TO_HOUR */
		nval->val *= 60;
		/* fall through */
	case SQL_INTERVAL_MINUTE: /* SQL_C_INTERVAL_MINUTE */
	case SQL_INTERVAL_HOUR_TO_MINUTE: /* SQL_C_INTERVAL_HOUR_TO_MINUTE */
	case SQL_INTERVAL_DAY_TO_MINUTE: /* SQL_C_INTERVAL_DAY_TO_MINUTE */
		nval->val *= 60;
		/* fall through */
	case SQL_INTERVAL_SECOND: /* SQL_C_INTERVAL_SECOND */
	case SQL_INTERVAL_MINUTE_TO_SECOND: /* SQL_C_INTERVAL_MINUTE_TO_SECOND */
	case SQL_INTERVAL_HOUR_TO_SECOND: /* SQL_C_INTERVAL_HOUR_TO_SECOND */
	case SQL_INTERVAL_DAY_TO_SECOND: /* SQL_C_INTERVAL_DAY_TO_SECOND */
		break;
	default:
		assert(0);
	}
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
	ival->interval_type = SQL_IS_DAY_TO_SECOND;
	ival->interval_sign = !nval->sign;
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
parsemonthinterval(bignum_t *nval, SQL_INTERVAL_STRUCT *ival, int type)
{
	/* convert value to months */
	switch (type) {
	case SQL_INTERVAL_YEAR: /* SQL_C_INTERVAL_YEAR */
		nval->val *= 12;
	case SQL_INTERVAL_YEAR_TO_MONTH: /* SQL_C_INTERVAL_YEAR_TO_MONTH */
	case SQL_INTERVAL_MONTH: /* SQL_C_INTERVAL_MONTH */
		break;
	default:
		assert(0);
	}
	/* ignore fraction */
	while (nval->scale > 0) {
		nval->scale--;
		nval->val /= 10;
	}
	ival->interval_type = SQL_IS_YEAR_TO_MONTH;
	ival->interval_sign = !nval->sign;
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

	memset(dval, 0, sizeof(*dval));
	while (space(*data))
		data++;
	if (sscanf(data, "{d '%hd-%hu-%hu'}%n",
		   &dval->year, &dval->month, &dval->day, &n) < 3 &&
	    sscanf(data, "%hd-%hu-%hu%n",
		   &dval->year, &dval->month, &dval->day, &n) < 3)
		return 0;
	if (dval->month == 0 || dval->month > 12 ||
	    dval->day == 0 || dval->day > monthlengths[dval->month] ||
	    (dval->month == 2 && !isLeap(dval->year) && dval->day == 29))
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
	int braces;

	memset(tval, 0, sizeof(*tval));
	while (space(*data))
		data++;
	if (sscanf(data, "{t '%hu:%hu:%hu%n",
		   &tval->hour, &tval->minute, &tval->second, &n) < 3 &&
	    sscanf(data, "%hu:%hu:%hu%n",
		   &tval->hour, &tval->minute, &tval->second, &n) < 3)
		return 0;
	/* seconds can go up to 61(!) because of leap seconds */
	if (tval->hour > 23 || tval->minute > 59 || tval->second > 61)
		return 0;
	braces = *data == '{';
	data += n;
	n = 1;			/* tentative return value */
	if (*data == '.') {
		while (*++data && '0' <= *data && *data <= '9')
			;
		n = 2;		/* indicate loss of precision */
	}
	if (*data == '+' || *data == '-') {
		/* time zone (which we ignore) */
		short tzhour, tzmin;
		int i;

		if (sscanf(data, "%hd:%hd%n", &tzhour, &tzmin, &i) < 2)
			return 0;
		data += i;
		tzmin = tzhour < 0 ? tzhour * 60 - tzmin : tzhour * 60 + tzmin;
		(void) tzhour;
		(void) tzmin;
	}
	if (braces && *data++ != '\'' && *data++ != '}')
		return 0;
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
	int braces;

	memset(tsval, 0, sizeof(*tsval));
	while (space(*data))
		data++;
	if (sscanf(data, "{TS '%hd-%hu-%hu %hu:%hu:%hu%n",
		   &tsval->year, &tsval->month, &tsval->day,
		   &tsval->hour, &tsval->minute, &tsval->second, &n) < 6 &&
	    sscanf(data, "%hd-%hu-%hu %hu:%hu:%hu%n",
		   &tsval->year, &tsval->month, &tsval->day,
		   &tsval->hour, &tsval->minute, &tsval->second, &n) < 6)
		return 0;
	if (tsval->month == 0 || tsval->month > 12 ||
	    tsval->day == 0 || tsval->day > monthlengths[tsval->month] ||
	    (tsval->month == 2 && !isLeap(tsval->year) && tsval->day == 29) ||
	    tsval->hour > 23 || tsval->minute > 59 || tsval->second > 61)
		return 0;
	braces = *data == '{';
	tsval->fraction = 0;
	data += n;
	n = 1000000000;
	if (*data == '.') {
		while (*++data && '0' <= *data && *data <= '9') {
			n /= 10;
			tsval->fraction += (*data - '0') * n;
		}
	}
	if (*data == '+' || *data == '-') {
		/* time zone (which we ignore) */
		short tzhour, tzmin;
		int i;

		if (sscanf(data, "%hd:%hd%n", &tzhour, &tzmin, &i) < 2)
			return 0;
		data += i;
		tzmin = tzhour < 0 ? tzhour * 60 - tzmin : tzhour * 60 + tzmin;
		(void) tzhour;
		(void) tzmin;
	}
	if (braces && *data++ != '\'' && *data++ != '}')
		return 0;
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
	case SQL_GUID:
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
	}
	return 0;
}

static SQLRETURN
parseoptionalbracketednumber(char **svalp,
			     SQLLEN *slenp,
			     int *val1p,
			     int *val2p)
{
	char *sval = *svalp;
	SQLLEN slen = *slenp;
	char *eptr;
	long val;

	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	if (slen == 0 || *sval != '(') {
		/* don't touch *valp, it contains the default */
		return SQL_SUCCESS;
	}
	slen--;
	sval++;
	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	/* make sure there is a closing parenthesis in the string:
	 * this makes the calls to strtol safe */
	{
		SQLLEN i;

		for (eptr = sval, i = slen; i > 0 && *eptr != ')'; i--, eptr++)
			;
		if (i == 0)
			return SQL_ERROR;
	}
	if (slen > 0 && (*sval == '+' || *sval == '-'))
		return SQL_ERROR;
	val = strtol(sval, &eptr, 10);
	if (eptr == sval)
		return SQL_ERROR;
	slen -= (int) (eptr - sval);
	sval = eptr;
	*val1p = (int) val;
	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	if (val2p != NULL && slen > 0 && *sval == ',') {
		slen--;
		sval++;
		while (slen > 0 && isspace((unsigned char) *sval)) {
			slen--;
			sval++;
		}
		if (slen > 0 && (*sval == '+' || *sval == '-'))
			return SQL_ERROR;
		val = strtol(sval, &eptr, 10);
		if (eptr == sval)
			return SQL_ERROR;
		slen -= (int) (eptr - sval);
		sval = eptr;
		*val2p = (int) val;
	}

	if (slen == 0 || *sval != ')')
		return SQL_ERROR;
	slen--;
	sval++;
	*svalp = sval;
	*slenp = slen;
	return SQL_SUCCESS;
}

static SQLRETURN
parsemonthintervalstring(char **svalp,
			 SQLLEN *slenp,
			 SQL_INTERVAL_STRUCT *ival)
{
	char *sval = *svalp;
	SQLLEN slen = slenp ? *slenp : (SQLLEN) strlen(sval);
	char *eptr;
	long val1 = -1, val2 = -1;
	SQLLEN leadingprecision;

	memset(ival, 0, sizeof(*ival));
	if (slen < 8 || strncasecmp(sval, "interval", 8) != 0)
		return SQL_ERROR;
	sval += 8;
	slen -= 8;
	if (slen == 0 || !isspace((unsigned char) *sval))
		return SQL_ERROR;
	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	if (slen > 0 && *sval == '-') {
		slen--;
		sval++;
		ival->interval_sign = SQL_TRUE;
	} else
		ival->interval_sign = SQL_FALSE;
	if (slen == 0 || *sval != '\'')
		return SQL_ERROR;
	slen--;
	sval++;
	/* make sure there is another quote in the string: this makes
	 * the calls to strtol safe */
	for (eptr = sval, leadingprecision = slen;
	     leadingprecision > 0 && *eptr != '\'';
	     leadingprecision--, eptr++)
		;
	if (leadingprecision == 0)
		return SQL_ERROR;
	if (*sval == '+' || *sval == '-')
		return SQL_ERROR;
	val1 = strtol(sval, &eptr, 10);
	if (eptr == sval)
		return SQL_ERROR;
	leadingprecision = (SQLLEN) (eptr - sval);
	slen -= leadingprecision;
	sval = eptr;
	while (isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	if (*sval == '-') {
		slen--;
		sval++;
		while (isspace((unsigned char) *sval)) {
			slen--;
			sval++;
		}
		if (*sval == '+' || *sval == '-')
			return SQL_ERROR;
		val2 = strtol(sval, &eptr, 10);
		if (eptr == sval)
			return SQL_ERROR;
		if (eptr - sval > 2)
			return SQL_ERROR;
		slen -= (int) (eptr - sval);
		sval = eptr;
		while (isspace((unsigned char) *sval)) {
			slen--;
			sval++;
		}
		ival->interval_type = SQL_IS_YEAR_TO_MONTH;
		ival->intval.year_month.year = val1;
		ival->intval.year_month.month = val2;
		if (val2 >= 12)
			return SQL_ERROR;
	}
	if (*sval != '\'')
		return SQL_ERROR;
	slen--;
	sval++;
	if (slen == 0 || !isspace((unsigned char) *sval))
		return SQL_ERROR;
	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	if (slen >= 4 && strncasecmp(sval, "year", 4) == 0) {
		int p = 2;

		slen -= 4;
		sval += 4;
		if (parseoptionalbracketednumber(&sval, &slen, &p, NULL) == SQL_ERROR)
			return SQL_ERROR;
		if (leadingprecision > p)
			return SQL_ERROR;
		if (val2 == -1) {
			ival->interval_type = SQL_IS_YEAR;
			ival->intval.year_month.year = val1;
			ival->intval.year_month.month = 0;
		}
		if (slen > 0 && isspace((unsigned char) *sval)) {
			while (slen > 0 && isspace((unsigned char) *sval)) {
				slen--;
				sval++;
			}
			if (slen > 2 && strncasecmp(sval, "to", 2) == 0) {
				slen -= 2;
				sval += 2;
				if (val2 == -1)
					return SQL_ERROR;
				if (slen == 0 || !isspace((unsigned char) *sval))
					return SQL_ERROR;
				while (slen > 0 && isspace((unsigned char) *sval)) {
					slen--;
					sval++;
				}
				if (slen >= 5 && strncasecmp(sval, "month", 5) == 0) {
					slen -= 5;
					sval += 5;
					while (slen > 0 && isspace((unsigned char) *sval)) {
						slen--;
						sval++;
					}
				} else
					return SQL_ERROR;
			}
		}
		if (slen > 0)
			return SQL_ERROR;
	} else if (slen >= 5 && strncasecmp(sval, "month", 5) == 0) {
		int p = 2;

		slen -= 5;
		sval += 5;
		if (parseoptionalbracketednumber(&sval, &slen, &p, NULL) == SQL_ERROR)
			return SQL_ERROR;
		if (leadingprecision > p)
			return SQL_ERROR;
		while (slen > 0 && isspace((unsigned char) *sval)) {
			slen--;
			sval++;
		}
		if (slen != 0)
			return SQL_ERROR;
		ival->interval_type = SQL_IS_MONTH;
		ival->intval.year_month.year = val1 / 12;
		ival->intval.year_month.month = val1 % 12;
	} else
		return SQL_ERROR;

	return SQL_SUCCESS;
}

static SQLRETURN
parsesecondintervalstring(char **svalp,
			  SQLLEN *slenp,
			  SQL_INTERVAL_STRUCT *ival,
			  int *secprecp)
{
	char *sval = *svalp;
	SQLLEN slen = slenp ? *slenp : (SQLLEN) strlen(sval);
	char *eptr;
	SQLLEN leadingprecision;
	int secondprecision = 0;
	unsigned v1, v2, v3, v4;
	int n;

	memset(ival, 0, sizeof(*ival));
	if (slen < 8 || strncasecmp(sval, "interval", 8) != 0)
		return SQL_ERROR;
	sval += 8;
	slen -= 8;
	if (slen == 0 || !isspace((unsigned char) *sval))
		return SQL_ERROR;
	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	if (slen > 0 && *sval == '-') {
		slen--;
		sval++;
		ival->interval_sign = SQL_TRUE;
	} else
		ival->interval_sign = SQL_FALSE;
	if (slen == 0 || *sval != '\'')
		return SQL_ERROR;
	slen--;
	sval++;
	/* make sure there is another quote in the string: this makes
	 * the calls to sscanf safe */
	for (eptr = sval, leadingprecision = slen;
	     leadingprecision > 0 && *eptr != '\'';
	     leadingprecision--, eptr++)
		;
	if (leadingprecision == 0)
		return SQL_ERROR;
	if (*sval == '+' || *sval == '-')
		return SQL_ERROR;
	/* note that the first bit is a bogus comparison (sval does
	 * not start with '-', so is not negative) but this keeps the
	 * compiler happy */
	if (strtol(sval, &eptr, 10) < 0 || /* we parse the actual value again later */
	    eptr == sval)
		return SQL_ERROR;
	leadingprecision = (int) (eptr - sval);

	ival->interval_type = (SQLINTERVAL)0; /* unknown as yet */
	ival->intval.day_second.day = 0;
	ival->intval.day_second.hour = 0;
	ival->intval.day_second.minute = 0;
	ival->intval.day_second.second = 0;
	ival->intval.day_second.fraction = 0;
	if (sscanf(sval, "%u %2u:%2u:%2u%n", &v1, &v2, &v3, &v4, &n) >= 4) {
		ival->interval_type = SQL_IS_DAY_TO_SECOND;
		if (v2 >= 24 || v3 >= 60 || v4 >= 60)
			return SQL_ERROR;
		ival->intval.day_second.day = v1;
		ival->intval.day_second.hour = v2;
		ival->intval.day_second.minute = v3;
		ival->intval.day_second.second = v4;
		sval += n;
		slen -= n;
	} else if (sscanf(sval, "%u %2u:%2u%n", &v1, &v2, &v3, &n) >= 3) {
		ival->interval_type = SQL_IS_DAY_TO_MINUTE;
		if (v2 >= 24 || v3 >= 60)
			return SQL_ERROR;
		ival->intval.day_second.day = v1;
		ival->intval.day_second.hour = v2;
		ival->intval.day_second.minute = v3;
		sval += n;
		slen -= n;
	} else if (sscanf(sval, "%u %2u%n", &v1, &v2, &n) >= 2) {
		ival->interval_type = SQL_IS_DAY_TO_HOUR;
		if (v2 >= 60)
			return SQL_ERROR;
		ival->intval.day_second.day = v1;
		ival->intval.day_second.hour = v2;
		sval += n;
		slen -= n;
	} else if (sscanf(sval, "%u:%2u:%2u%n", &v1, &v2, &v3, &n) >= 3) {
		ival->interval_type = SQL_IS_HOUR_TO_SECOND;
		if (v2 >= 60 || v3 >= 60)
			return SQL_ERROR;
		ival->intval.day_second.day = v1 / 24;
		ival->intval.day_second.hour = v1 % 24;
		ival->intval.day_second.minute = v2;
		ival->intval.day_second.second = v3;
		sval += n;
		slen -= n;
	} else if (sscanf(sval, "%u:%2u%n", &v1, &v2, &n) >= 2) {
		sval += n;
		slen -= n;
		if (*sval == '.') {
			ival->interval_type = SQL_IS_MINUTE_TO_SECOND;
			if (v2 >= 60)
				return SQL_ERROR;
			ival->intval.day_second.day = v1 / (24 * 60);
			ival->intval.day_second.hour = (v1 / 60) % 24;
			ival->intval.day_second.minute = v1 % 60;
			ival->intval.day_second.second = v2;
		}
		n = 2;	/* two valid values */
	} else if (sscanf(sval, "%u%n", &v1, &n) >= 1) {
		sval += n;
		slen -= n;
		if (*sval == '.') {
			ival->interval_type = SQL_IS_SECOND;
			ival->intval.day_second.day = v1 / (24 * 60 * 60);
			ival->intval.day_second.hour = (v1 / (60 * 60)) % 24;
			ival->intval.day_second.minute = (v1 / 60) % 60;
			ival->intval.day_second.second = v1 % 60;
		}
		n = 1;	/* one valid value */
	}
	if (*sval == '.') {
		if (ival->interval_type != SQL_IS_SECOND &&
		    ival->interval_type != SQL_IS_MINUTE_TO_SECOND &&
		    ival->interval_type != SQL_IS_HOUR_TO_SECOND &&
		    ival->interval_type != SQL_IS_DAY_TO_SECOND)
			return SQL_ERROR;
		sval++;
		slen--;
		secondprecision = 0;
		while ('0' <= *sval && *sval <= '9') {
			if (secondprecision < 9) {
				secondprecision++;
				ival->intval.day_second.fraction *= 10;
				ival->intval.day_second.fraction += *sval - '0';
			}
			sval++;
			slen--;
		}
	}
	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}
	if (*sval != '\'')
		return SQL_ERROR;
	slen--;
	sval++;
	if (slen == 0 || !isspace((unsigned char) *sval))
		return SQL_ERROR;
	while (slen > 0 && isspace((unsigned char) *sval)) {
		slen--;
		sval++;
	}

	if (slen >= 3 && strncasecmp(sval, "day", 3) == 0) {
		sval += 3;
		slen -= 3;
		if (ival->interval_type == 0 && n == 1) {
			ival->interval_type = SQL_IS_DAY;
			ival->intval.day_second.day = v1;
		}
		if (ival->interval_type != SQL_IS_DAY &&
		    ival->interval_type != SQL_IS_DAY_TO_HOUR &&
		    ival->interval_type != SQL_IS_DAY_TO_MINUTE &&
		    ival->interval_type != SQL_IS_DAY_TO_SECOND)
			return SQL_ERROR;
	} else if (slen >= 4 && strncasecmp(sval, "hour", 4) == 0) {
		slen -= 4;
		sval += 4;
		if (ival->interval_type == 0) {
			if (n == 1) {
				ival->interval_type = SQL_IS_HOUR;
				ival->intval.day_second.day = v1 / 24;
				ival->intval.day_second.hour = v1 % 24;
			} else {
				assert(n == 2);
				ival->interval_type = SQL_IS_HOUR_TO_MINUTE;
				if (v2 >= 60)
					return SQL_ERROR;
				ival->intval.day_second.day = v1 / 24;
				ival->intval.day_second.hour = v1 % 24;
				ival->intval.day_second.minute = v2;
			}
		}
		if (ival->interval_type != SQL_IS_HOUR &&
		    ival->interval_type != SQL_IS_HOUR_TO_MINUTE &&
		    ival->interval_type != SQL_IS_HOUR_TO_SECOND)
			return SQL_ERROR;
	} else if (slen >= 6 && strncasecmp(sval, "minute", 6) == 0) {
		slen -= 6;
		sval += 6;
		if (ival->interval_type == 0) {
			if (n == 1) {
				ival->interval_type = SQL_IS_MINUTE;
				ival->intval.day_second.day = v1 / (24 * 60);
				ival->intval.day_second.hour = (v1 / 60) % 24;
				ival->intval.day_second.minute = v1 % 60;
			} else {
				assert(n == 2);
				ival->interval_type = SQL_IS_MINUTE_TO_SECOND;
				if (v2 >= 60)
					return SQL_ERROR;
				ival->intval.day_second.day = v1 / (24 * 60);
				ival->intval.day_second.hour = (v1 / 60) % 24;
				ival->intval.day_second.minute = v1 % 60;
				ival->intval.day_second.second = v2;
			}
		}
		if (ival->interval_type != SQL_IS_MINUTE &&
		    ival->interval_type != SQL_IS_MINUTE_TO_SECOND)
			return SQL_ERROR;
	} else if (slen >= 6 && strncasecmp(sval, "second", 6) == 0) {
		slen -= 6;
		sval += 6;
		if (ival->interval_type == 0) {
			if (n == 1) {
				ival->interval_type = SQL_IS_SECOND;
				ival->intval.day_second.day = v1 / (24 * 60 * 60);
				ival->intval.day_second.hour = (v1 / (60 * 60)) % 24;
				ival->intval.day_second.minute = (v1 / 60) % 60;
				ival->intval.day_second.second = v1 % 60;
			}
		}
		if (ival->interval_type != SQL_IS_SECOND)
			return SQL_ERROR;
	}
	{
		int p = 2;
		int q = 6;

		if (parseoptionalbracketednumber(&sval, &slen, &p, ival->interval_type == SQL_IS_SECOND ? &q : NULL) == SQL_ERROR)
			return SQL_ERROR;
		if (leadingprecision > p)
			return SQL_ERROR;
		if (ival->interval_type == SQL_IS_SECOND && secondprecision > q)
			return SQL_ERROR;
	}
	if (slen > 0 && isspace((unsigned char) *sval)) {
		while (slen > 0 && isspace((unsigned char) *sval)) {
			slen--;
			sval++;
		}
		if (slen > 2 && strncasecmp(sval, "to", 2) == 0) {
			slen -= 2;
			sval += 2;
			if (slen == 0 || !isspace((unsigned char) *sval))
				return SQL_ERROR;
			while (slen > 0 && isspace((unsigned char) *sval)) {
				slen--;
				sval++;
			}
			if (slen >= 4 && strncasecmp(sval, "hour", 4) == 0) {
				slen -= 4;
				sval += 4;
				if (ival->interval_type != SQL_IS_DAY_TO_HOUR)
					return SQL_ERROR;
			} else if (slen >= 6 && strncasecmp(sval, "minute", 6) == 0) {
				slen -= 6;
				sval += 6;
				if (ival->interval_type != SQL_IS_DAY_TO_MINUTE &&
				    ival->interval_type != SQL_IS_HOUR_TO_MINUTE)
					return SQL_ERROR;
			} else if (slen >= 6 && strncasecmp(sval, "second", 6) == 0) {
				int p = 6;

				slen -= 6;
				sval += 6;
				if (ival->interval_type != SQL_IS_DAY_TO_SECOND &&
				    ival->interval_type != SQL_IS_HOUR_TO_SECOND &&
				    ival->interval_type != SQL_IS_MINUTE_TO_SECOND)
					return SQL_ERROR;
				while (slen > 0 && isspace((unsigned char) *sval)) {
					slen--;
					sval++;
				}
				if (parseoptionalbracketednumber(&sval, &slen, &p, NULL) == SQL_ERROR)
					return SQL_ERROR;
				if (p < secondprecision)
					return SQL_ERROR;
			} else
				return SQL_ERROR;
			while (slen > 0 && isspace((unsigned char) *sval)) {
				slen--;
				sval++;
			}
		}
	}
	if (slen > 0)
		return SQL_ERROR;
	*secprecp = secondprecision;
	return SQL_SUCCESS;
}

SQLRETURN
ODBCFetch(ODBCStmt *stmt,
	  SQLUSMALLINT col,
	  SQLSMALLINT type,
	  SQLPOINTER ptr,
	  SQLLEN buflen,
	  SQLLEN *lenp,
	  SQLLEN *nullp,
	  SQLSMALLINT precision,
	  SQLSMALLINT scale,
	  SQLINTEGER datetime_interval_precision,
	  SQLLEN offset,
	  SQLULEN row)
{
	char *data;
	size_t datalen;
	SQLSMALLINT sql_type;
	SQLUINTEGER maxdatetimeval;
	ODBCDesc *ard, *ird;
	ODBCDescRec *irdrec, *ardrec;
	SQLINTEGER bind_type;

	/* various interpretations of the input data */
	bignum_t nval;
	SQL_INTERVAL_STRUCT ival;
	int ivalprec = 0;	/* interval second precision */
	int i;
	DATE_STRUCT dval;
	TIME_STRUCT tval;
	TIMESTAMP_STRUCT tsval;
	double fval = 0;

	/* staging variables for output data */
	SQL_NUMERIC_STRUCT nmval;
	SQL_INTERVAL_STRUCT ivval;

	assert(ptr != NULL);

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

	if (offset > 0)
		ptr = (SQLPOINTER) ((char *) ptr + offset);

	if (lenp)
		lenp = (SQLLEN *) ((char *) lenp + offset + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(*lenp) : bind_type));
	if (nullp)
		nullp = (SQLLEN *) ((char *) nullp + offset + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(*nullp) : bind_type));

	/* translate default type */
	/* note, type can't be SQL_ARD_TYPE since when this function
	 * is called from SQLFetch, type is already the ARD concise
	 * type, and when it is called from SQLGetData, it has already
	 * been translated */

	if (type == SQL_C_DEFAULT)
		type = ODBCDefaultType(irdrec);

	if (precision == UNAFFECTED ||
	    scale == UNAFFECTED ||
	    datetime_interval_precision == UNAFFECTED) {
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
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
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
	datalen = mapi_fetch_field_len(stmt->hdl, col - 1);

	/* first convert to internal (binary) format */

	/* see SQLExecute.c for possible types */
	switch (sql_type) {
	case SQL_DECIMAL:
	case SQL_TINYINT:
	case SQL_SMALLINT:
	case SQL_INTEGER:
	case SQL_BIGINT:
	case SQL_INTERVAL_YEAR:
	case SQL_INTERVAL_YEAR_TO_MONTH:
	case SQL_INTERVAL_MONTH:
	case SQL_INTERVAL_DAY:
	case SQL_INTERVAL_DAY_TO_HOUR:
	case SQL_INTERVAL_DAY_TO_MINUTE:
	case SQL_INTERVAL_DAY_TO_SECOND:
	case SQL_INTERVAL_HOUR:
	case SQL_INTERVAL_HOUR_TO_MINUTE:
	case SQL_INTERVAL_HOUR_TO_SECOND:
	case SQL_INTERVAL_MINUTE:
	case SQL_INTERVAL_MINUTE_TO_SECOND:
	case SQL_INTERVAL_SECOND:
		if (!parseint(data, &nval)) {
			/* shouldn't happen: getting here means SQL
			 * server told us a value was of a certain
			 * type, but in reality it wasn't. */
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}

		/* interval types are transferred as ints but need to
		 * be converted to the internal interval formats */
		switch (sql_type) {
		case SQL_INTERVAL_YEAR:
		case SQL_INTERVAL_YEAR_TO_MONTH:
		case SQL_INTERVAL_MONTH:
			parsemonthinterval(&nval, &ival, SQL_INTERVAL_MONTH);
			break;
		case SQL_INTERVAL_DAY:
		case SQL_INTERVAL_DAY_TO_HOUR:
		case SQL_INTERVAL_DAY_TO_MINUTE:
		case SQL_INTERVAL_DAY_TO_SECOND:
		case SQL_INTERVAL_HOUR:
		case SQL_INTERVAL_HOUR_TO_MINUTE:
		case SQL_INTERVAL_HOUR_TO_SECOND:
		case SQL_INTERVAL_MINUTE:
		case SQL_INTERVAL_MINUTE_TO_SECOND:
		case SQL_INTERVAL_SECOND:
			ivalprec = parsesecondinterval(&nval, &ival, SQL_INTERVAL_SECOND);
			break;
		default:
			break;
		}
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
		while (datalen != 0 && space(*data)) {
			data++;
			datalen--;
		}
		if (datalen >= 4 && strncasecmp(data, "true", 4) == 0) {
			data += 4;
			datalen -= 4;
			nval.val = 1;
		} else if (datalen >= 5 && strncasecmp(data, "false", 5) == 0) {
			data += 5;
			datalen -= 5;
			nval.val = 0;
		} else {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}
		while (datalen != 0 && space(*data)) {
			data++;
			datalen--;
		}
		if (datalen != 0) {
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
	case SQL_GUID:
		/* nothing special to do here */
	default:
		/* any other type can only be converted to SQL_C_CHAR */
		break;
	}

	/* then convert to desired format */

	switch (type) {
	case SQL_C_CHAR:
	case SQL_C_WCHAR:
	{
		SQLPOINTER origptr;
		SQLLEN origbuflen;
		SQLLEN *origlenp;
		SQLLEN sz;

		if (buflen < 0) {
			/* Invalid string or buffer length */
			addStmtError(stmt, "HY090", NULL, 0);
			return SQL_ERROR;
		}
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? ardrec->sql_desc_octet_length : bind_type));

		/* if SQL_C_WCHAR is requested, first convert to UTF-8
		 * (SQL_C_CHAR), and at the end convert to UTF-16 */
		origptr = ptr;

		origbuflen = buflen;
		origlenp = lenp;
		if (type == SQL_C_WCHAR) {
			/* allocate temporary space */
			buflen = 511; /* should be enough for most types */
			if (data != NULL &&
			    (sql_type == SQL_CHAR ||
			     sql_type == SQL_VARCHAR ||
			     sql_type == SQL_LONGVARCHAR ||
			     sql_type == SQL_WCHAR ||
			     sql_type == SQL_WVARCHAR ||
			     sql_type == SQL_WLONGVARCHAR))
				buflen = (SQLLEN) datalen + 1; /* but this is certainly enough for strings */
			ptr = malloc(buflen);
			if (ptr == NULL) {
				/* Memory allocation error */
				addStmtError(stmt, "HY001", NULL, 0);
				return SQL_ERROR;
			}

			lenp = NULL;
		}
		switch (sql_type) {
		default:
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_LONGVARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_WLONGVARCHAR:
		case SQL_GUID:
			if (irdrec->already_returned > datalen) {
				data += datalen;
				datalen = 0;
			} else {
				data += irdrec->already_returned;
				datalen -= irdrec->already_returned;
			}
			if (datalen == 0 && irdrec->already_returned != 0) {
				/* no more data to return */
				if (type == SQL_C_WCHAR)
					free(ptr);
				return SQL_NO_DATA;
			}
			copyString(data, datalen, ptr, buflen, lenp, SQLLEN,
				   addStmtError, stmt, return SQL_ERROR);
			if (datalen < (size_t) buflen)
				irdrec->already_returned += datalen;
			else
				irdrec->already_returned += buflen;
			break;
		case SQL_BINARY:
		case SQL_VARBINARY:
		case SQL_LONGVARBINARY: {
			size_t k;
			int n;
			unsigned char c = 0;
			SQLLEN j = 0;
			unsigned char *p = ptr;

			if (buflen < 0) {
				/* Invalid string or buffer length */
				addStmtError(stmt, "HY090", NULL, 0);
				if (type == SQL_C_WCHAR)
					free(ptr);
				return SQL_ERROR;
			}
			if (irdrec->already_returned > datalen) {
				data += datalen;
				datalen = 0;
			} else {
				data += irdrec->already_returned;
				datalen -= irdrec->already_returned;
			}
			if (datalen == 0 && irdrec->already_returned != 0) {
				/* no more data to return */
				if (type == SQL_C_WCHAR)
					free(ptr);
				return SQL_NO_DATA;
			}
			for (k = 0; k < datalen; k++) {
				if ('0' <= data[k] && data[k] <= '9')
					n = data[k] - '0';
				else if ('A' <= data[k] && data[k] <= 'F')
					n = data[k] - 'A' + 10;
				else if ('a' <= data[k] && data[k] <= 'f')
					n = data[k] - 'a' + 10;
				else {
					/* should not happen */
					/* General error */
					addStmtError(stmt, "HY000", "Unexpected data from server", 0);
					if (type == SQL_C_WCHAR)
						free(ptr);
					return SQL_ERROR;
				}
				if (k & 1) {
					c |= n;
					if (j < buflen)
						p[j] = c;
					j++;
				} else
					c = n << 4;
			}
			irdrec->already_returned += k;
			if (lenp)
				*lenp = j;
			break;
		}
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT: {
			SQLUBIGINT f;
			int n;

			data = (char *) ptr;

			for (n = 0, f = 1; n < nval.scale; n++)
				f *= 10;
			sz = snprintf(data, buflen, "%s%" O_ULLFMT, nval.sign ? "" : "-", O_ULLCAST (nval.val / f));
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
					sz = (SQLLEN) snprintf(data, buflen, ".%0*" O_ULLFMT, nval.scale, O_ULLCAST (nval.val % f));
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
			data = (char *) ptr;

			for (i = 4; i < 18; i++) {
				sz = (SQLLEN) snprintf(data, buflen, "%.*g", i, fval);
				if (sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					if (i == 0) {
						/* Numeric value out
						 * of range */
						addStmtError(stmt, "22003", NULL, 0);

						if (type == SQL_C_WCHAR)
							free(ptr);
						return SQL_ERROR;
					}
					/* current precision (i) doesn't fit,
					 * but previous did, so use that */
					snprintf(data, buflen, "%.*g", i - 1, fval);
					/* max space that would have
					 * been needed */
					sz = (SQLLEN) strlen(data) + 17 - i;
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

			sz = snprintf(data, buflen, "%04u-%02u-%02u",
				      (unsigned int) dval.year,
				      (unsigned int) dval.month,
				      (unsigned int) dval.day);
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

			sz = snprintf(data, buflen, "%02u:%02u:%02u",
				      (unsigned int) tval.hour,
				      (unsigned int) tval.minute,
				      (unsigned int) tval.second);
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

			sz = snprintf(data, buflen,
				      "%04u-%02u-%02u %02u:%02u:%02u",
				      (unsigned int) tsval.year,
				      (unsigned int) tsval.month,
				      (unsigned int) tsval.day,
				      (unsigned int) tsval.hour,
				      (unsigned int) tsval.minute,
				      (unsigned int) tsval.second);
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
				int fscale = 9;

				data += sz;
				buflen += sz;
				while (tsval.fraction % 10 == 0) {
					tsval.fraction /= 10;
					fscale--;
				}
				if (lenp)
					*lenp += fscale + 1;
				if (buflen > 2)
					sz = snprintf(data, buflen, ".%0*u",
						      fscale, (unsigned int) tsval.fraction);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		case SQL_INTERVAL_YEAR:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u' YEAR",
				      ival.interval_sign ? "-" : "",
				      (unsigned int) ival.intval.year_month.year);

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
		case SQL_INTERVAL_YEAR_TO_MONTH:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u-%02u' YEAR TO MONTH",
				      ival.interval_sign ? "-" : "",
				      (unsigned int) ival.intval.year_month.year,
				      (unsigned int) ival.intval.year_month.month);

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
		case SQL_INTERVAL_MONTH:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u' MONTH",
				      ival.interval_sign ? "-" : "",
				      (unsigned int) (12 * ival.intval.year_month.year +
						      ival.intval.year_month.month));

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
		case SQL_INTERVAL_DAY:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u' DAY",
				      ival.interval_sign ? "-" : "",
				      (unsigned int) ival.intval.day_second.day);

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
		case SQL_INTERVAL_DAY_TO_HOUR:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u %02u' DAY TO HOUR",
				      ival.interval_sign ? "-" : "",
				      (unsigned int) ival.intval.day_second.day,
				      (unsigned int) ival.intval.day_second.hour);

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
		case SQL_INTERVAL_DAY_TO_MINUTE:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u %02u:%02u' DAY TO MINUTE",
				      ival.interval_sign ? "-" : "",
				      (unsigned int) ival.intval.day_second.day,
				      (unsigned int) ival.intval.day_second.hour,
				      (unsigned int) ival.intval.day_second.minute);

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
		case SQL_INTERVAL_DAY_TO_SECOND: {
			int w;

			data = (char *) ptr;

			w = 14;	/* space needed for "'DAY TO SECOND" */

			sz = snprintf(data, buflen, "INTERVAL %s'%u %02u:%02u:%02u",
				      ival.interval_sign ? "-" : "",
				      (unsigned int) ival.intval.day_second.day,
				      (unsigned int) ival.intval.day_second.hour,
				      (unsigned int) ival.intval.day_second.minute,
				      (unsigned int) ival.intval.day_second.second);
			if (sz < 0 || sz + w >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);
				return SQL_ERROR;
			}
			data += sz;
			buflen -= sz;

			if (lenp)
				*lenp = sz;
			if (ivalprec > 0) {
				if (lenp)
					*lenp += ivalprec + 1;
				if (buflen > w + 2)
					sz = snprintf(data, buflen, ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				if (buflen <= w + 2 || sz < 0 || sz + w >= buflen) {
					sz = buflen - w - 1;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
				data += sz;
				buflen -= sz;
			}
			/* this should now fit */
			sz = snprintf(data, buflen, "' DAY TO SECOND");
			if (lenp && sz > 0)
				*lenp += sz;
			break;
		}
		case SQL_INTERVAL_HOUR:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u' HOUR",
				      ival.interval_sign ? "-" : "",
				      (unsigned) (24 * ival.intval.day_second.day +
						  ival.intval.day_second.hour));

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
		case SQL_INTERVAL_HOUR_TO_MINUTE:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u:%02u' HOUR TO MINUTE",
				      ival.interval_sign ? "-" : "",
				      (unsigned) (24 * ival.intval.day_second.day +
						  ival.intval.day_second.hour),
				      (unsigned int) ival.intval.day_second.minute);

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
		case SQL_INTERVAL_HOUR_TO_SECOND: {
			int w;

			data = (char *) ptr;

			w = 15;	/* space needed for "'HOUR TO SECOND" */

			sz = snprintf(data, buflen, "INTERVAL %s'%u:%02u:%02u",
				      ival.interval_sign ? "-" : "",
				      (unsigned) (24 * ival.intval.day_second.day +
						  ival.intval.day_second.hour),
				      (unsigned int) ival.intval.day_second.minute,
				      (unsigned int) ival.intval.day_second.second);
			if (sz < 0 || sz + w >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);
				return SQL_ERROR;
			}
			data += sz;
			buflen -= sz;

			if (lenp)
				*lenp = sz;
			if (ivalprec > 0) {
				if (lenp)
					*lenp += ivalprec + 1;
				if (buflen > w + 2)
					sz = snprintf(data, buflen, ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				if (buflen <= w + 2 || sz < 0 || sz + w >= buflen) {
					sz = buflen - w - 1;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
				data += sz;
				buflen -= sz;
			}
			/* this should now fit */
			sz = snprintf(data, buflen, "' HOUR TO SECOND");
			if (lenp && sz > 0)
				*lenp += sz;
			break;
		}
		case SQL_INTERVAL_MINUTE:
			sz = snprintf((char *) ptr, buflen,
				      "INTERVAL %s'%u' MINUTE",
				      ival.interval_sign ? "-" : "",
				      (unsigned) (60 * (24 * ival.intval.day_second.day +
							ival.intval.day_second.hour) +
						  ival.intval.day_second.minute));

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
		case SQL_INTERVAL_MINUTE_TO_SECOND: {
			int w;

			data = (char *) ptr;

			w = 17;	/* space needed for "'MINUTE TO SECOND" */

			sz = snprintf(data, buflen, "INTERVAL %s'%u:%02u",
				      ival.interval_sign ? "-" : "",
				      (unsigned) (60 * (24 * ival.intval.day_second.day +
							ival.intval.day_second.hour) +
						  ival.intval.day_second.minute),
				      (unsigned int) ival.intval.day_second.second);
			if (sz < 0 || sz + w >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);
				return SQL_ERROR;
			}
			data += sz;
			buflen -= sz;

			if (lenp)
				*lenp = sz;
			if (ivalprec > 0) {
				if (lenp)
					*lenp += ivalprec + 1;
				if (buflen > w + 2)
					sz = snprintf(data, buflen, ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				if (buflen <= w + 2 || sz < 0 || sz + w >= buflen) {
					sz = buflen - w - 1;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
				data += sz;
				buflen -= sz;
			}
			/* this should now fit */
			sz = snprintf(data, buflen, "' MINUTE TO SECOND");
			if (lenp && sz > 0)
				*lenp += sz;
			break;
		}
		case SQL_INTERVAL_SECOND: {
			int w;

			data = (char *) ptr;

			w = 7;	/* space needed for "'SECOND" */

			sz = snprintf(data, buflen, "INTERVAL %s'%u",
				      ival.interval_sign ? "-" : "",
				      (unsigned) (60 * (60 * (24 * ival.intval.day_second.day +
							      ival.intval.day_second.hour) +
							ival.intval.day_second.minute) +
						  ival.intval.day_second.second));
			if (sz < 0 || sz + w >= buflen) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);

				if (type == SQL_C_WCHAR)
					free(ptr);
				return SQL_ERROR;
			}
			data += sz;
			buflen -= sz;

			if (lenp)
				*lenp = sz;
			if (ivalprec > 0) {
				if (lenp)
					*lenp += ivalprec + 1;
				if (buflen > w + 2)
					sz = snprintf(data, buflen, ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				if (buflen <= w + 2 || sz < 0 || sz + w >= buflen) {
					sz = buflen - w - 1;
					/* String data, right-truncated */
					addStmtError(stmt, "01004", NULL, 0);
				}
				data += sz;
				buflen -= sz;
			}
			/* this should now fit */
			sz = snprintf(data, buflen, "' SECOND");
			if (lenp && sz > 0)
				*lenp += sz;
			break;
		}
		}
		if (type == SQL_C_WCHAR) {
			SQLSMALLINT n;

			ODBCutf82wchar((SQLCHAR *) ptr, SQL_NTS, (SQLWCHAR *) origptr, origbuflen, &n);
#ifdef ODBCDEBUG
			ODBCLOG("Writing %d bytes to " PTRFMT "\n",
				(int) (n * sizeof(SQLWCHAR)),
				PTRFMTCAST origptr);
#endif

			if (origlenp)
				*origlenp = n * sizeof(SQLWCHAR); /* # of bytes, not chars */
			free(ptr);
		}
#ifdef ODBCDEBUG
		else
			ODBCLOG("Writing %d bytes to " PTRFMT "\n",
				(int) strlen(ptr), PTRFMTCAST ptr);
#endif
		break;
	}
	case SQL_C_BINARY:
		if (buflen < 0) {
			/* Invalid string or buffer length */
			addStmtError(stmt, "HY090", NULL, 0);
			return SQL_ERROR;
		}
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? ardrec->sql_desc_octet_length : bind_type));

		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
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
		case SQL_INTERVAL_YEAR:
		case SQL_INTERVAL_YEAR_TO_MONTH:
		case SQL_INTERVAL_MONTH:
		case SQL_INTERVAL_DAY:
		case SQL_INTERVAL_DAY_TO_HOUR:
		case SQL_INTERVAL_DAY_TO_MINUTE:
		case SQL_INTERVAL_DAY_TO_SECOND:
		case SQL_INTERVAL_HOUR:
		case SQL_INTERVAL_HOUR_TO_MINUTE:
		case SQL_INTERVAL_HOUR_TO_SECOND:
		case SQL_INTERVAL_MINUTE:
		case SQL_INTERVAL_MINUTE_TO_SECOND:
		case SQL_INTERVAL_SECOND:
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		/* break;  -- not reached */
	case SQL_C_BIT:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(unsigned char) : bind_type));

		if (lenp)
			*lenp = 1;
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
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
			WriteData(ptr, fval >= 1, unsigned char);

			if (fval != 0 && fval != 1) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
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
			WriteData(ptr, (unsigned char) nval.val, unsigned char);

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
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(signed char) : bind_type));
			break;
		case SQL_C_SSHORT:
		case SQL_C_SHORT:
			maxval <<= 15;
			if (lenp)
				*lenp = sizeof(short);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(short) : bind_type));
			break;
		case SQL_C_SLONG:
		case SQL_C_LONG:
			maxval <<= 31;
			if (lenp)
				*lenp = sizeof(int);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(int) : bind_type));
			break;
		case SQL_C_SBIGINT:
			maxval <<= 63;
			if (lenp)
				*lenp = sizeof(SQLBIGINT);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQLBIGINT) : bind_type));
			break;
		}
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_DOUBLE:
		case SQL_REAL:
			/* reparse double and float, parse char */
			if (!parseint(data, &nval)) {
				/* Invalid character value for cast
				 * specification */
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
			 * is too large even for SQLUBIGINT */
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
				WriteData(ptr, nval.sign ? (signed char) nval.val : -(signed char) nval.val, signed char);
				break;
			case SQL_C_SSHORT:
			case SQL_C_SHORT:
				WriteData(ptr, nval.sign ? (short) nval.val : -(short) nval.val, short);
				break;
			case SQL_C_SLONG:
			case SQL_C_LONG:
				WriteData(ptr, nval.sign ? (int) nval.val : -(int) nval.val, int);
				break;
			case SQL_C_SBIGINT:
				WriteData(ptr, nval.sign ? (SQLBIGINT) nval.val : -(SQLBIGINT) nval.val, SQLBIGINT);
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
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(unsigned char) : bind_type));
			break;
		case SQL_C_USHORT:
			maxval <<= 16;
			if (lenp)
				*lenp = sizeof(unsigned short);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(unsigned short) : bind_type));
			break;
		case SQL_C_ULONG:
			maxval <<= 32;
			if (lenp)
				*lenp = sizeof(unsigned int);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(unsigned int) : bind_type));
			break;
		case SQL_C_UBIGINT:
			if (lenp)
				*lenp = sizeof(SQLUBIGINT);
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQLUBIGINT) : bind_type));
			break;
		}
		maxval--;
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_DOUBLE:
		case SQL_REAL:
			/* reparse double and float, parse char */
			if (!parseint(data, &nval)) {
				/* Invalid character value for cast
				 * specification */
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
			 * is too large even for SQLUBIGINT */
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
				WriteData(ptr, (unsigned char) nval.val, unsigned char);
				break;
			case SQL_C_USHORT:
				WriteData(ptr, (unsigned short) nval.val, unsigned short);
				break;
			case SQL_C_ULONG:
				WriteData(ptr, (unsigned int) nval.val, unsigned int);
				break;
			case SQL_C_UBIGINT:
				WriteData(ptr, (SQLUBIGINT) nval.val, SQLUBIGINT);
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
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQL_NUMERIC_STRUCT) : bind_type));

		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_DOUBLE:
		case SQL_REAL:
			/* reparse double and float, parse char */
			if (!(i = parseint(data, &nval))) {
				/* Invalid character value for cast
				 * specification */
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
			memset(&nmval, 0, sizeof(nmval));
			nmval.precision = nval.precision;
			nmval.scale = nval.scale;
			nmval.sign = nval.sign;
			for (i = 0; i < SQL_MAX_NUMERIC_LEN; i++) {
				nmval.val[i] = (SQLCHAR) (nval.val & 0xFF);
				nval.val >>= 8;
			}
			WriteData(ptr, nmval, SQL_NUMERIC_STRUCT);
			if (lenp)
				*lenp = sizeof(SQL_NUMERIC_STRUCT);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_C_FLOAT:
	case SQL_C_DOUBLE:
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
			if (!parsedouble(data, &fval)) {
				/* Invalid character value for cast
				 * specification */
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
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(float) : bind_type));
			if (fval < -FLT_MAX || fval > FLT_MAX) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			WriteData(ptr, (float) fval, float);
			if (lenp)
				*lenp = sizeof(float);
		} else {
			if (ardrec && row > 0)
				ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(double) : bind_type));
			WriteData(ptr, fval, double);

			if (lenp)
				*lenp = sizeof(double);
		}
		break;
	case SQL_C_TYPE_DATE:
		if (ardrec && row > 0)
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(DATE_STRUCT) : bind_type));

		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
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
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		case SQL_TYPE_DATE:
			WriteData(ptr, dval, DATE_STRUCT);
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
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(TIME_STRUCT) : bind_type));

		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
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
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		case SQL_TYPE_TIME:
			WriteData(ptr, tval, TIME_STRUCT);
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
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(TIMESTAMP_STRUCT) : bind_type));

		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
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
						 * value for cast
						 * specification */
						addStmtError(stmt, "22018", NULL, 0);
						return SQL_ERROR;
					}
				}
			}
			/* fall through */
		case SQL_TYPE_TIMESTAMP:	/* note i==1 unless we fell through */
			WriteData(ptr, tsval, TIMESTAMP_STRUCT);
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
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQL_INTERVAL_STRUCT) : bind_type));

		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
			if (parsemonthintervalstring(&data, NULL, &ival) == SQL_ERROR) {
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			break;
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			parsemonthinterval(&nval, &ival, type);
			break;
		case SQL_INTERVAL_YEAR:
		case SQL_INTERVAL_YEAR_TO_MONTH:
		case SQL_INTERVAL_MONTH:
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		memset(&ivval, 0, sizeof(ivval));
		ivval.interval_sign = ival.interval_sign;
		ivval.intval.year_month.year = 0;
		ivval.intval.year_month.month = 0;
		switch (type) {
		case SQL_C_INTERVAL_YEAR:
			ivval.interval_type = SQL_IS_YEAR;
			if ((ivval.intval.year_month.year = ival.intval.year_month.year) >= maxdatetimeval) {
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
			ivval.interval_type = SQL_IS_MONTH;
			if ((ivval.intval.year_month.month = ival.intval.year_month.month + 12 * ival.intval.year_month.year) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			break;
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
			ivval.interval_type = SQL_IS_YEAR_TO_MONTH;
			if ((ivval.intval.year_month.year = ival.intval.year_month.year) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.year_month.month = ival.intval.year_month.month;
			break;
		}
		WriteData(ptr, ivval, SQL_INTERVAL_STRUCT);
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
			ptr = (SQLPOINTER) ((char *) ptr + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQL_INTERVAL_STRUCT) : bind_type));

		switch (sql_type) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
			if (parsesecondintervalstring(&data, NULL, &ival, &ivalprec) == SQL_ERROR) {
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			break;
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			ivalprec = parsesecondinterval(&nval, &ival, type);
			break;
		case SQL_INTERVAL_DAY:
		case SQL_INTERVAL_DAY_TO_HOUR:
		case SQL_INTERVAL_DAY_TO_MINUTE:
		case SQL_INTERVAL_DAY_TO_SECOND:
		case SQL_INTERVAL_HOUR:
		case SQL_INTERVAL_HOUR_TO_MINUTE:
		case SQL_INTERVAL_HOUR_TO_SECOND:
		case SQL_INTERVAL_MINUTE:
		case SQL_INTERVAL_MINUTE_TO_SECOND:
		case SQL_INTERVAL_SECOND:
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		memset(&ivval, 0, sizeof(ivval));
		ivval.interval_sign = ival.interval_sign;
		ivval.intval.day_second.day = 0;
		ivval.intval.day_second.hour = 0;
		ivval.intval.day_second.minute = 0;
		ivval.intval.day_second.second = 0;
		ivval.intval.day_second.fraction = 0;
		switch (type) {
		case SQL_C_INTERVAL_DAY:
			ivval.interval_type = SQL_IS_DAY;
			if ((ivval.intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
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
			ivval.interval_type = SQL_IS_HOUR;
			if ((ivval.intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= maxdatetimeval) {
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
			ivval.interval_type = SQL_IS_MINUTE;
			if ((ivval.intval.day_second.minute = ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day)) >= maxdatetimeval) {
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
			ivval.interval_type = SQL_IS_SECOND;
			if ((ivval.intval.day_second.second = ival.intval.day_second.second + 60 * (ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day))) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_DAY_TO_HOUR:
			ivval.interval_type = SQL_IS_DAY_TO_HOUR;
			if ((ivval.intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.day_second.hour = ival.intval.day_second.hour;
			if (ival.intval.day_second.minute || ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_DAY_TO_MINUTE:
			ivval.interval_type = SQL_IS_DAY_TO_MINUTE;
			if ((ivval.intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.day_second.hour = ival.intval.day_second.hour;
			ivval.intval.day_second.minute = ival.intval.day_second.minute;
			if (ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_DAY_TO_SECOND:
			ivval.interval_type = SQL_IS_DAY_TO_SECOND;
			if ((ivval.intval.day_second.day = ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.day_second.hour = ival.intval.day_second.hour;
			ivval.intval.day_second.minute = ival.intval.day_second.minute;
			ivval.intval.day_second.second = ival.intval.day_second.second;
			ivval.intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_HOUR_TO_MINUTE:
			ivval.interval_type = SQL_IS_HOUR_TO_MINUTE;
			if ((ivval.intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.day_second.minute = ival.intval.day_second.minute;
			if (ival.intval.day_second.second || ival.intval.day_second.fraction) {
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			}
			break;
		case SQL_C_INTERVAL_HOUR_TO_SECOND:
			ivval.interval_type = SQL_IS_HOUR_TO_SECOND;
			if ((ivval.intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.day_second.minute = ival.intval.day_second.minute;
			ivval.intval.day_second.second = ival.intval.day_second.second;
			ivval.intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_MINUTE_TO_SECOND:
			ivval.interval_type = SQL_IS_MINUTE_TO_SECOND;
			if ((ivval.intval.day_second.minute = ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day)) >= maxdatetimeval) {
				/* Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			ivval.intval.day_second.second = ival.intval.day_second.second;
			ivval.intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		}
		if (ivval.intval.day_second.fraction) {
			while (ivalprec < precision) {
				ivalprec++;
				ivval.intval.day_second.fraction *= 10;
			}
			while (ivalprec > precision) {
				ivalprec--;
				if (stmt->Error == NULL &&
				    ivval.intval.day_second.fraction % 10 != 0) {
					/* Fractional truncation */
					addStmtError(stmt, "01S07", NULL, 0);
				}
				ivval.intval.day_second.fraction /= 10;
			}
		}
		WriteData(ptr, ivval, SQL_INTERVAL_STRUCT);
		if (lenp)
			*lenp = sizeof(SQL_INTERVAL_STRUCT);
		break;
	case SQL_C_GUID:
		if (datalen != 36) {
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
#ifdef ODBCDEBUG
		ODBCLOG("Writing 16 bytes to " PTRFMT "\n", PTRFMTCAST ptr);
#endif
		for (i = 0; i < 16; i++) {
			if (i == 8 || i == 12 || i == 16 || i == 20) {
				if (*data != '-') {
					/* Restricted data type
					 * attribute violation */
					addStmtError(stmt, "07006", NULL, 0);
					return SQL_ERROR;
				}
				data++;
			}
			if ('0' <= *data && *data <= '9')
				((unsigned char *) ptr)[i] = *data - '0';
			else if ('a' <= *data && *data <= 'f')
				((unsigned char *) ptr)[i] = *data - 'a' + 10;
			else if ('A' <= *data && *data <= 'F')
				((unsigned char *) ptr)[i] = *data - 'A' + 10;
			else {
				/* Restricted data type attribute
				 * violation */
				addStmtError(stmt, "07006", NULL, 0);
				return SQL_ERROR;
			}
			((unsigned char *) ptr)[i] <<= 4;
			data++;
			if ('0' <= *data && *data <= '9')
				((unsigned char *) ptr)[i] |= *data - '0';
			else if ('a' <= *data && *data <= 'f')
				((unsigned char *) ptr)[i] |= *data - 'a' + 10;
			else if ('A' <= *data && *data <= 'F')
				((unsigned char *) ptr)[i] |= *data - 'A' + 10;
			else {
				/* Restricted data type attribute
				 * violation */
				addStmtError(stmt, "07006", NULL, 0);
				return SQL_ERROR;
			}
			data++;
		}
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
				char *b = realloc(buf, buflen += 1024);	\
				if (b == NULL) {			\
					free(buf);			\
					if (ctype == SQL_C_WCHAR && sval) \
						free(sval);		\
					/* Memory allocation error */	\
					addStmtError(stmt, "HY001", NULL, 0); \
					return SQL_ERROR;		\
				}					\
				buf = b;				\
			}						\
			buf[bufpos++] = (value);			\
		} while (0)
#define assigns(buf,bufpos,buflen,value,stmt)				\
		do {							\
			size_t _len = strlen(value);			\
			size_t _i;					\
			while (bufpos + _len >= buflen) {		\
				char *b = realloc(buf, buflen += 1024);	\
				if (b == NULL) {			\
					free(buf);			\
					if (ctype == SQL_C_WCHAR && sval) \
						free(sval);		\
					/* Memory allocation error */	\
					addStmtError(stmt, "HY001", NULL, 0); \
					return SQL_ERROR;		\
				}					\
				buf = b;				\
			}						\
			for (_i = 0; _i < _len; _i++)			\
				buf[bufpos++] = (value)[_i];		\
		} while (0)

SQLRETURN
ODBCStore(ODBCStmt *stmt,
	  SQLUSMALLINT param,
	  SQLLEN offset,
	  SQLULEN row,
	  char **bufp,
	  size_t *bufposp,
	  size_t *buflenp,
	  char *sep)
{
	ODBCDescRec *ipdrec, *apdrec;
	SQLPOINTER ptr;
	SQLLEN *strlen_or_ind_ptr;
	SQLINTEGER bind_type;
	SQLSMALLINT ctype, sqltype;
	char *sval = NULL;
	SQLLEN slen = 0;
	bignum_t nval;
	double fval = 0.0;
	DATE_STRUCT dval;
	TIME_STRUCT tval;
	TIMESTAMP_STRUCT tsval;
	int ivalprec = 0;	/* interval second precision */
	SQL_INTERVAL_STRUCT ival;
	char *buf = *bufp;
	size_t bufpos = *bufposp;
	size_t buflen = *buflenp;
	char data[256];
	int i;

	assert(param <= stmt->ImplParamDescr->sql_desc_count);
	assert(param <= stmt->ApplParamDescr->sql_desc_count);
	ipdrec = stmt->ImplParamDescr->descRec + param;
	apdrec = stmt->ApplParamDescr->descRec + param;

	bind_type = stmt->ApplParamDescr->sql_desc_bind_type;
	ptr = apdrec->sql_desc_data_ptr;
	if (ptr && offset)
		ptr = (SQLPOINTER) ((char *) ptr + offset + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQLPOINTER) : bind_type));
	strlen_or_ind_ptr = apdrec->sql_desc_indicator_ptr;
	if (strlen_or_ind_ptr && offset)
		strlen_or_ind_ptr = (SQLLEN *) ((char *) strlen_or_ind_ptr + offset + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQLINTEGER) : bind_type));
	if (ptr == NULL &&
	    (strlen_or_ind_ptr == NULL || *strlen_or_ind_ptr != SQL_NULL_DATA)) {
		/* COUNT field incorrect */
		addStmtError(stmt, "07002", NULL, 0);
		return SQL_ERROR;
	}

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

	if (strlen_or_ind_ptr != NULL && *strlen_or_ind_ptr == SQL_NULL_DATA) {
		assigns(buf, bufpos, buflen, "NULL", stmt);
		*bufp = buf;
		*bufposp = bufpos;
		*buflenp = buflen;
		return SQL_SUCCESS;
	}

	strlen_or_ind_ptr = apdrec->sql_desc_octet_length_ptr;
	if (strlen_or_ind_ptr && offset)
		strlen_or_ind_ptr = (SQLLEN *) ((char *) strlen_or_ind_ptr + offset + row * (bind_type == SQL_BIND_BY_COLUMN ? (SQLINTEGER) sizeof(SQLINTEGER) : bind_type));

	switch (ctype) {
	case SQL_C_CHAR:
	case SQL_C_BINARY:
		slen = strlen_or_ind_ptr ? *strlen_or_ind_ptr : SQL_NTS;
		sval = (char *) ptr;
		fixODBCstring(sval, slen, SQLLEN, addStmtError, stmt, return SQL_ERROR);
		break;
	case SQL_C_WCHAR:
		slen = strlen_or_ind_ptr ? *strlen_or_ind_ptr : SQL_NTS;
		fixWcharIn((SQLWCHAR *) ptr, slen, char, sval, addStmtError, stmt, return SQL_ERROR);
		if (sval == NULL) {
			sval = strdup("");
			if (sval == NULL) {
				addStmtError(stmt, "HY001", NULL, 0);
				return SQL_ERROR;
			}
		}
		slen = strlen(sval);
		break;
	case SQL_C_BIT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLCHAR *) ptr != 0;
		break;
	case SQL_C_STINYINT:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLSCHAR *) ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLSCHAR *) ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLSCHAR *) ptr;
		}
		break;
	case SQL_C_UTINYINT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLCHAR *) ptr;
		break;
	case SQL_C_SSHORT:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLSMALLINT *) ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLSMALLINT *) ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLSMALLINT *) ptr;
		}
		break;
	case SQL_C_USHORT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLUSMALLINT *) ptr;
		break;
	case SQL_C_SLONG:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLINTEGER *) ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLINTEGER *) ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLINTEGER *) ptr;
		}
		break;
	case SQL_C_ULONG:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLUINTEGER *) ptr;
		break;
	case SQL_C_SBIGINT:
		nval.precision = 1;
		nval.scale = 0;
		if (* (SQLBIGINT *) ptr < 0) {
			nval.sign = 0;
			nval.val = - * (SQLBIGINT *) ptr;
		} else {
			nval.sign = 1;
			nval.val = * (SQLBIGINT *) ptr;
		}
		break;
	case SQL_C_UBIGINT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = * (SQLUBIGINT *) ptr;
		break;
	case SQL_C_NUMERIC:
		nval.precision = (unsigned char) apdrec->sql_desc_precision;
		nval.scale = (signed char) apdrec->sql_desc_scale;
		nval.sign = ((SQL_NUMERIC_STRUCT *) ptr)->sign;
		nval.val = 0;
		for (i = 0; i < SQL_MAX_NUMERIC_LEN; i++)
			nval.val |= (SQLUBIGINT) ((SQL_NUMERIC_STRUCT *) ptr)->val[i] << (i * 8);
		break;
	case SQL_C_FLOAT:
		fval = * (SQLREAL *) ptr;
		break;
	case SQL_C_DOUBLE:
		fval = * (SQLDOUBLE *) ptr;
		break;
	case SQL_C_TYPE_DATE:
		dval = * (SQL_DATE_STRUCT *) ptr;
		break;
	case SQL_C_TYPE_TIME:
		tval = * (SQL_TIME_STRUCT *) ptr;
		break;
	case SQL_C_TYPE_TIMESTAMP:
		tsval = * (SQL_TIMESTAMP_STRUCT *) ptr;
		break;
	case SQL_C_INTERVAL_YEAR:
		ival.interval_type = SQL_IS_YEAR;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.year_month.year = ((SQL_INTERVAL_STRUCT *) ptr)->intval.year_month.year;
		ival.intval.year_month.month = 0;
		break;
	case SQL_C_INTERVAL_MONTH:
		ival.interval_type = SQL_IS_MONTH;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.year_month.year = 0;
		ival.intval.year_month.month = ((SQL_INTERVAL_STRUCT *) ptr)->intval.year_month.month;
		break;
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
		ival.interval_type = SQL_IS_YEAR_TO_MONTH;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.year_month.year = ((SQL_INTERVAL_STRUCT *) ptr)->intval.year_month.year;
		ival.intval.year_month.month = ((SQL_INTERVAL_STRUCT *) ptr)->intval.year_month.month;
		break;
	case SQL_C_INTERVAL_DAY:
		ival.interval_type = SQL_IS_DAY;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.day;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_HOUR:
		ival.interval_type = SQL_IS_HOUR;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_MINUTE:
		ival.interval_type = SQL_IS_MINUTE;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.minute;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_SECOND:
		ival.interval_type = SQL_IS_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.fraction;
		ivalprec = apdrec->sql_desc_precision;
		break;
	case SQL_C_INTERVAL_DAY_TO_HOUR:
		ival.interval_type = SQL_IS_DAY_TO_HOUR;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.day;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = 0;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
		ival.interval_type = SQL_IS_DAY_TO_MINUTE;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.day;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.minute;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_DAY_TO_SECOND:
		ival.interval_type = SQL_IS_DAY_TO_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.day;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.minute;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.fraction;
		ivalprec = apdrec->sql_desc_precision;
		break;
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
		ival.interval_type = SQL_IS_HOUR_TO_MINUTE;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.minute;
		ival.intval.day_second.second = 0;
		ival.intval.day_second.fraction = 0;
		break;
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
		ival.interval_type = SQL_IS_HOUR_TO_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.hour;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.minute;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.fraction;
		ivalprec = apdrec->sql_desc_precision;
		break;
	case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		ival.interval_type = SQL_IS_MINUTE_TO_SECOND;
		ival.interval_sign = ((SQL_INTERVAL_STRUCT *) ptr)->interval_sign;
		ival.intval.day_second.day = 0;
		ival.intval.day_second.hour = 0;
		ival.intval.day_second.minute = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.minute;
		ival.intval.day_second.second = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.second;
		ival.intval.day_second.fraction = ((SQL_INTERVAL_STRUCT *) ptr)->intval.day_second.fraction;
		ivalprec = apdrec->sql_desc_precision;
		break;
	case SQL_C_GUID:
		break;
	}

	assigns(buf, bufpos, buflen, sep, stmt);
	*bufp = buf;
	/* just the types supported by the server */
	switch (sqltype) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
		assign(buf, bufpos, buflen, '\'', stmt);
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			for (i = 0; i < slen; i++) {
				unsigned char c = (unsigned char) sval[i];

				if (c == 0) {
					break;
				} else if (c < 0x20 /* || c >= 0x7F */) {
					assign(buf, bufpos, buflen, '\\', stmt);
					assign(buf, bufpos, buflen, '0' + (c >> 6), stmt);
					assign(buf, bufpos, buflen, '0' + ((c >> 3) & 0x7), stmt);
					assign(buf, bufpos, buflen, '0' + (c & 0x7), stmt);
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
			snprintf(data, sizeof(data), "%s%" O_ULLFMT, nval.sign ? "" : "-", O_ULLCAST (nval.val / f));
			assigns(buf, bufpos, buflen, data, stmt);
			if (nval.scale > 0) {
				snprintf(data, sizeof(data), ".%0*" O_ULLFMT, nval.scale, O_ULLCAST (nval.val % f));
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
			snprintf(data, sizeof(data), "%04d-%02u-%02u",
				 (int) dval.year,
				 (unsigned int) dval.month,
				 (unsigned int) dval.day);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_TYPE_TIME:
			snprintf(data, sizeof(data), "%02u:%02u:%02u",
				 (unsigned int) tval.hour,
				 (unsigned int) tval.minute,
				 (unsigned int) tval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_TYPE_TIMESTAMP:
			snprintf(data, sizeof(data),
				 "%04d-%02u-%02u %02u:%02u:%02u",
				 (int) tsval.year,
				 (unsigned int) tsval.month,
				 (unsigned int) tsval.day,
				 (unsigned int) tsval.hour,
				 (unsigned int) tsval.minute,
				 (unsigned int) tsval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (tsval.fraction) {
				snprintf(data, sizeof(data), ".%09u", (unsigned int) tsval.fraction);
				/* remove trailing zeros */
				for (i = 9; i > 0 && data[i] == '0'; i--)
					data[i] = 0;
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_YEAR:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.year_month.year);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_MONTH:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.year_month.month);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
			snprintf(data, sizeof(data), "%s%0*u-%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.year_month.year, (unsigned int) ival.intval.year_month.month);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_DAY:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.day);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_HOUR:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.hour);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_MINUTE:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.minute);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_SECOND:
			snprintf(data, sizeof(data), "%s%0*u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && ivalprec > 0) {
				snprintf(data, sizeof(data), ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_DAY_TO_HOUR:
			snprintf(data, sizeof(data), "%s%0*u %02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.day, (unsigned int) ival.intval.day_second.hour);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_DAY_TO_MINUTE:
			snprintf(data, sizeof(data), "%s%0*u %02u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.day, (unsigned int) ival.intval.day_second.hour, (unsigned int) ival.intval.day_second.minute);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_DAY_TO_SECOND:
			snprintf(data, sizeof(data), "%s%0*u %02u:%02u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.day, (unsigned int) ival.intval.day_second.hour, (unsigned int) ival.intval.day_second.minute, (unsigned int) ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && ivalprec > 0) {
				snprintf(data, sizeof(data), ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_HOUR_TO_MINUTE:
			snprintf(data, sizeof(data), "%s%0*u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.hour, (unsigned int) ival.intval.day_second.minute);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		case SQL_C_INTERVAL_HOUR_TO_SECOND:
			snprintf(data, sizeof(data), "%s%0*u:%02u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.hour, (unsigned int) ival.intval.day_second.minute, (unsigned int) ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && ivalprec > 0) {
				snprintf(data, sizeof(data), ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_INTERVAL_MINUTE_TO_SECOND:
			snprintf(data, sizeof(data), "%s%0*u:%02u", ival.interval_sign ? "-" : "", (int) apdrec->sql_desc_datetime_interval_precision, (unsigned int) ival.intval.day_second.minute, (unsigned int) ival.intval.day_second.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (ival.intval.day_second.fraction && ivalprec > 0) {
				snprintf(data, sizeof(data), ".%0*u", ivalprec, (unsigned int) ival.intval.day_second.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			break;
		case SQL_C_GUID:
			snprintf(data, sizeof(data),
				 "%02x%02x%02x%02x-%02x%02x-%02x%02x-"
				 "%02x%02x-%02x%02x%02x%02x%02x%02x",
				 ((unsigned char *) ptr)[0],
				 ((unsigned char *) ptr)[1],
				 ((unsigned char *) ptr)[2],
				 ((unsigned char *) ptr)[3],
				 ((unsigned char *) ptr)[4],
				 ((unsigned char *) ptr)[5],
				 ((unsigned char *) ptr)[6],
				 ((unsigned char *) ptr)[7],
				 ((unsigned char *) ptr)[8],
				 ((unsigned char *) ptr)[9],
				 ((unsigned char *) ptr)[10],
				 ((unsigned char *) ptr)[11],
				 ((unsigned char *) ptr)[12],
				 ((unsigned char *) ptr)[13],
				 ((unsigned char *) ptr)[14],
				 ((unsigned char *) ptr)[15]);
			break;
		}
		assign(buf, bufpos, buflen, '\'', stmt);
		break;
	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_BINARY:
			assigns(buf, bufpos, buflen, "blob '", stmt);
			for (i = 0; i < slen; i++) {
				unsigned char c = (unsigned char) sval[i];

				assign(buf, bufpos, buflen, "0123456789ABCDEF"[c >> 4], stmt);
				assign(buf, bufpos, buflen, "0123456789ABCDEF"[c & 0xF], stmt);
			}
			assign(buf, bufpos, buflen, '\'', stmt);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		break;
	case SQL_TYPE_DATE:
		i = 1;
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			i = parsetimestamp(sval, &tsval);
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
			} else if (!parsedate(sval, &dval)) {
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				goto failure;
			}
			/* fall through */
		case SQL_C_TYPE_DATE:
			snprintf(data, sizeof(data), "DATE '%u-%02u-%02u'",
				 (unsigned int) dval.year,
				 (unsigned int) dval.month,
				 (unsigned int) dval.day);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		break;
	case SQL_TYPE_TIME:
		i = 1;
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			i = parsetimestamp(sval, &tsval);
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
			} else if (!parsetime(sval, &tval)) {
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				goto failure;
			}
			/* fall through */
		case SQL_C_TYPE_TIME:
			snprintf(data, sizeof(data), "TIME '%u:%02u:%02u'",
				 (unsigned int) tval.hour,
				 (unsigned int) tval.minute,
				 (unsigned int) tval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		break;
	case SQL_TYPE_TIMESTAMP:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			i = parsetimestamp(sval, &tsval);
			if (i == 0) {
				i = parsetime(sval, &tval);
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
					i = parsedate(sval, &dval);
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
						 * value for cast
						 * specification */
						addStmtError(stmt, "22018", NULL, 0);
						goto failure;
					}
				}
			}
			/* fall through */
		case SQL_C_TYPE_TIMESTAMP:
			snprintf(data, sizeof(data),
				 "TIMESTAMP '%u-%02u-%02u %02u:%02u:%02u",
				 (unsigned int) tsval.year,
				 (unsigned int) tsval.month,
				 (unsigned int) tsval.day,
				 (unsigned int) tsval.hour,
				 (unsigned int) tsval.minute,
				 (unsigned int) tsval.second);
			assigns(buf, bufpos, buflen, data, stmt);
			if (tsval.fraction) {
				snprintf(data, sizeof(data), ".%09u", (unsigned int) tsval.fraction);
				assigns(buf, bufpos, buflen, data, stmt);
			}
			assign(buf, bufpos, buflen, '\'', stmt);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		break;
	case SQL_INTERVAL_MONTH:
	case SQL_INTERVAL_YEAR:
	case SQL_INTERVAL_YEAR_TO_MONTH:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			if (parsemonthintervalstring(&sval, &slen, &ival) == SQL_ERROR) {
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				goto failure;
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
			parsemonthinterval(&nval, &ival, sqltype);
			break;
		case SQL_C_INTERVAL_YEAR:
		case SQL_C_INTERVAL_MONTH:
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		switch (ival.interval_type) {
		case SQL_IS_YEAR:
			snprintf(data, sizeof(data), "INTERVAL %s'%u' YEAR", ival.interval_sign ? "" : "- ", (unsigned int) ival.intval.year_month.year);
			break;
		case SQL_IS_MONTH:
			snprintf(data, sizeof(data), "INTERVAL %s'%u' MONTH", ival.interval_sign ? "" : "- ", (unsigned int) ival.intval.year_month.month);
			break;
		case SQL_IS_YEAR_TO_MONTH:
			snprintf(data, sizeof(data), "INTERVAL %s'%u-%u' YEAR TO MONTH", ival.interval_sign ? "" : "- ", (unsigned int) ival.intval.year_month.year, (unsigned int) ival.intval.year_month.month);
			break;
		default:
			break;
		}
		assigns(buf, bufpos, buflen, data, stmt);
		break;
	case SQL_INTERVAL_DAY:
	case SQL_INTERVAL_HOUR:
	case SQL_INTERVAL_MINUTE:
	case SQL_INTERVAL_SECOND:
	case SQL_INTERVAL_DAY_TO_HOUR:
	case SQL_INTERVAL_DAY_TO_MINUTE:
	case SQL_INTERVAL_DAY_TO_SECOND:
	case SQL_INTERVAL_HOUR_TO_MINUTE:
	case SQL_INTERVAL_HOUR_TO_SECOND:
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			if (parsesecondintervalstring(&sval, &slen, &ival, &ivalprec) == SQL_ERROR) {
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				goto failure;
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
			parsesecondinterval(&nval, &ival, sqltype);
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
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		snprintf(data, sizeof(data), "INTERVAL %s'%u %u:%u:%u", ival.interval_sign ? "" : "- ", (unsigned int) ival.intval.day_second.day, (unsigned int) ival.intval.day_second.hour, (unsigned int) ival.intval.day_second.minute, (unsigned int) ival.intval.day_second.second);
		assigns(buf, bufpos, buflen, data, stmt);
		assigns(buf, bufpos, buflen, "' DAY TO SECOND", stmt);
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
			sval = data;
			/* fall through */
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
		case SQL_C_BINARY:
			/* parse character data, reparse floating
			 * point number */
			if (!parseint(sval, &nval)) {
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				goto failure;
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
			if (ival.intval.day_second.fraction && ivalprec > 0) {
				for (i = 0; i < ivalprec; i++) {
					nval.val *= 10;
					nval.scale++;
				}
				nval.val += ival.intval.day_second.fraction;
			}
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
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
					goto failure;
				}
				if (f > 1 && nval.val % f) {
					/* String data, right truncation */
					addStmtError(stmt, "22001", NULL, 0);
				}
			} else {
				snprintf(data, sizeof(data), "%s%" O_ULLFMT, nval.sign ? "" : "-", O_ULLCAST (nval.val / f));
				assigns(buf, bufpos, buflen, data, stmt);
				if (nval.scale > 0) {
					if (sqltype == SQL_DECIMAL) {
						snprintf(data, sizeof(data), ".%0*" O_ULLFMT, nval.scale, O_ULLCAST (nval.val % f));
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
			if (!parsedouble(sval, &fval)) {
				/* Invalid character value for cast specification */
				addStmtError(stmt, "22018", NULL, 0);
				goto failure;
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
			if (ival.intval.day_second.fraction && ivalprec > 0) {
				int f = 1;

				for (i = 0; i < ivalprec; i++)
					f *= 10;
				fval += ival.intval.day_second.fraction / (double) f;
			}
			if (ival.interval_sign)
				fval = -fval;
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		for (i = 1; i < 18; i++) {
			snprintf(data, sizeof(data), "%.*e", i, fval);
			if (fval == strtod(data, NULL))
				break;
		}
		assigns(buf, bufpos, buflen, data, stmt);
		break;
	case SQL_GUID:
		switch (ctype) {
		case SQL_C_CHAR:
		case SQL_C_WCHAR:
			if (slen != 36) {
				/* not sure this is the correct error */
				/* Invalid character value for cast
				 * specification */
				addStmtError(stmt, "22018", NULL, 0);
				goto failure;
			}
			for (i = 0; i < 36; i++) {
				if (strchr("0123456789abcdefABCDEF-",
					   sval[i]) == NULL) {
					/* not sure this is the
					 * correct error */
					/* Invalid character value for
					 * cast specification */
					addStmtError(stmt, "22018", NULL, 0);
					goto failure;
				}
			}
			snprintf(data, sizeof(data), "%.36s", sval);
			break;
		case SQL_C_GUID:
			snprintf(data, sizeof(data), "%08lx-%04x-%04x-%02x%02x-"
				 "%02x%02x%02x%02x%02x%02x",
				 (unsigned long) ((SQLGUID *) ptr)->Data1,
				 (unsigned int) ((SQLGUID *) ptr)->Data2,
				 (unsigned int) ((SQLGUID *) ptr)->Data3,
				 (unsigned int) ((SQLGUID *) ptr)->Data4[0],
				 (unsigned int) ((SQLGUID *) ptr)->Data4[1],
				 (unsigned int) ((SQLGUID *) ptr)->Data4[2],
				 (unsigned int) ((SQLGUID *) ptr)->Data4[3],
				 (unsigned int) ((SQLGUID *) ptr)->Data4[4],
				 (unsigned int) ((SQLGUID *) ptr)->Data4[5],
				 (unsigned int) ((SQLGUID *) ptr)->Data4[6],
				 (unsigned int) ((SQLGUID *) ptr)->Data4[7]);
			break;
		default:
			/* Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			goto failure;
		}
		assigns(buf, bufpos, buflen, data, stmt);
		break;
	}
	if (ctype == SQL_C_WCHAR)
		free(sval);
	*bufp = buf;
	*bufposp = bufpos;
	*buflenp = buflen;
	return SQL_SUCCESS;

  failure:
	if (ctype == SQL_C_WCHAR)
		free(sval);
	return SQL_ERROR;
}
