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
#include <errno.h>
#include <time.h>

#if SIZEOF_INT==8
# define ULL_CONSTANT(val)	(val)
# define ULLFMT			"%u"
#elif SIZEOF_LONG==8
# define ULL_CONSTANT(val)	(val##UL)
# define ULLFMT			"%lu"
#elif defined(HAVE_LONG_LONG)
# define ULL_CONSTANT(val)	(val##ULL)
# define ULLFMT			"%llu"
#elif defined(HAVE___INT64)
# define ULL_CONSTANT(val)	(val##ui64)
# define ULLFMT			"%I64u"
#endif

#define MAXBIGNUM10	ULL_CONSTANT(1844674407370955161) /* (2**64-1)/10 */
#define MAXBIGNUMLAST	'5'	/* (2**64-1)%10 */

typedef struct {
	unsigned char precision;
	signed char scale;
	unsigned char sign;	/* 1 pos, 0 neg */
	SQLUBIGINT val;
} bignum_t;

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
	if (*data == '-') {
		nval->sign = 0;
		data++;
	} else {
		nval->sign = 1;
		if (*data == '+')
			data++;
	}
	while (*data && *data != 'e' && *data != 'E') {
		if (*data == '.')
			fraction = 1;
		else if ('0' <= *data && *data <= '9') {
			if (overflow ||
			    nval->val > MAXBIGNUM10 ||
			    (nval->val == MAXBIGNUM10 && *data > MAXBIGNUMLAST)) {
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
	return 1 + overflow;
}

static void
parsesecondinterval(bignum_t *nval, SQL_INTERVAL_STRUCT *ival, int *ivalscale)
{
	int f = 1;

	ival->intval.day_second.fraction = 0;
	*ivalscale = 0;
	while (nval->scale > 0) {
		if (f < 1000000000) {
			(*ivalscale)++;
			ival->intval.day_second.fraction += (nval->val % 10) * f;
			f *= 10;
		}
		nval->val /= 10;
		nval->scale--;
	}
	/* normalize scale */
	while (*ivalscale > 0 && ival->intval.day_second.fraction % 10 != 0) {
		(*ivalscale)--;
		ival->intval.day_second.fraction /= 10;
	}
	ival->interval_type = SQL_IS_DAY_TO_SECOND;
	ival->interval_sign = nval->sign ? SQL_FALSE : SQL_TRUE;
	ival->intval.day_second.second = nval->val % 60;
	nval->val /= 60;
	ival->intval.day_second.minute = nval->val % 60;
	nval->val /= 60;
	ival->intval.day_second.hour = nval->val % 24;
	nval->val /= 24;
	ival->intval.day_second.day = nval->val;
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
	ival->intval.year_month.year = nval->val / 12;
	ival->intval.year_month.month = nval->val % 12;
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

	if (sscanf(data, "%hd-%hu-%hu%n",
		   &dval->year, &dval->month, &dval->day, &n) < 3 ||
	    data[n] != 0)
		return 0;
	if (dval->month == 0 || dval->month > 12 ||
	    dval->day == 0 || dval->day > monthlengths[dval->month] ||
	    (dval->month == 2 && !isLeap(dval->year) && dval->day == 29))
		return 0;
	return 1;
}

static int
parsetime(const char *data, TIME_STRUCT *tval)
{
	int n;

	if (sscanf(data, "%hu:%hu:%hu%n",
		   &tval->hour, &tval->minute, &tval->second, &n) < 3)
		return 0;
	data += n;
	if (*data == '.') {
		while (*++data)
			if (*data < '0' || *data > '9')
				return 0;
		return 2;
	}
	return *data == 0;
}

static int
parsetimestamp(const char *data, TIMESTAMP_STRUCT *tsval)
{
	int n;

	if (sscanf(data, "%hd-%hu-%hu %hu:%hu:%hu%n",
		   &tsval->year, &tsval->month, &tsval->day,
		   &tsval->hour, &tsval->minute, &tsval->second, &n) < 6)
		return 0;
	if (tsval->month == 0 || tsval->month > 12 ||
	    tsval->day == 0 || tsval->day > monthlengths[tsval->month] ||
	    (tsval->month == 2 && !isLeap(tsval->year) && tsval->day == 29))
		return 0;
	tsval->fraction = 0;
	data += n;
	if (*data == '.') {
		n = 1000000000;
		while (*++data) {
			if (*data < '0' || *data > '9')
				return 0;
			n /= 10;
			tsval->fraction += (*data - '0') * n;
		}
		if (n == 0)
			return 2; /* fractional digits truncated */
	}
	return *data == 0;
}

SQLRETURN
ODBCFetch(ODBCStmt *stmt, SQLUSMALLINT col, SQLSMALLINT type,
	  SQLPOINTER ptr, SQLINTEGER buflen, SQLINTEGER *lenp,
	  SQLINTEGER *nullp, SQLSMALLINT precision, SQLSMALLINT scale,
	  SQLINTEGER datetime_interval_precision, SQLINTEGER offset)
{
	char *data;
	SQLSMALLINT sql_type;

	/* various interpretations of the input data */
	bignum_t nval;
	SQL_INTERVAL_STRUCT ival;
	int i = 0;		/* scale of ival fraction, or counter */
	DATE_STRUCT dval;
	TIME_STRUCT tval;
	TIMESTAMP_STRUCT tsval;
	double fval = 0;

	if (ptr && offset)
		ptr = (SQLPOINTER) ((char *) ptr + offset);
	if (lenp && offset)
		lenp = (SQLINTEGER *) ((char *) lenp + offset);
	if (nullp && offset)
		nullp = (SQLINTEGER *) ((char *) nullp + offset);

	if (precision == UNAFFECTED || scale == UNAFFECTED ||
	    datetime_interval_precision == UNAFFECTED) {
		if (col <= stmt->ApplRowDescr->sql_desc_count) {
			ODBCDescRec *rec = &stmt->ApplRowDescr->descRec[col];

			if (precision == UNAFFECTED)
				precision = rec->sql_desc_precision;
			if (scale == UNAFFECTED)
				scale = rec->sql_desc_scale;
			if (datetime_interval_precision == UNAFFECTED)
				datetime_interval_precision = rec->sql_desc_datetime_interval_precision;
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
	datetime_interval_precision = 1;
	while (i-- > 0)
		datetime_interval_precision *= 10;

	sql_type = stmt->ImplRowDescr->descRec[col].sql_desc_concise_type;

	data = mapi_fetch_field(stmt->hdl, col - 1);
	if (mapi_error(stmt->Dbc->mid)) {
		/* HY000: General error */
		addStmtError(stmt, "HY000", NULL, 0);
		return SQL_ERROR;
	}
	if (nullp)
		*nullp = SQL_NULL_DATA;
	if (lenp)
		*lenp = SQL_NULL_DATA;
	if (data == NULL) {
		if (nullp == NULL) {
			/* 22002: Indicator variable required but not
			   supplied */
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
			/* 22018: Invalid character value for cast
			   specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}

		/* interval types are transferred as ints but need to
		   be converted to the internal interval formats */
		if (sql_type == SQL_INTERVAL_SECOND)
			parsesecondinterval(&nval, &ival, &i);
		else if (sql_type == SQL_INTERVAL_MONTH)
			parsemonthinterval(&nval, &ival);
		break;
	case SQL_DOUBLE:
	case SQL_FLOAT: {
		char *p;

		errno = 0;
		fval = strtod(data, &p);
		if (p == data || *p || errno == ERANGE) {
			/* 22018: Invalid character value for cast
			   specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}
		break;
	}
	case SQL_BIT:
		nval.precision = 1;
		nval.scale = 0;
		nval.sign = 1;
		nval.val = strcmp(data, "true") == 0;
		break;
	case SQL_TYPE_DATE:
		if (!parsedate(data, &dval)) {
			/* 22018: Invalid character value for cast
			   specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_TYPE_TIME:
		if (!parsetime(data, &tval)) {
			/* 22018: Invalid character value for cast
			   specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_TYPE_TIMESTAMP:
		if (!parsetimestamp(data, &tsval)) {
			/* 22018: Invalid character value for cast
			   specification */
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
		if (buflen < 0) {
			/* HY090: Invalid string or buffer length */
			addStmtError(stmt, "HY090", NULL, 0);
			return SQL_ERROR;
		}
		switch (sql_type) {
			int sz;

		default:
		case SQL_CHAR:
			strncpy((char *) ptr, data, buflen);
			if ((sz = strlen(data)) >= buflen) {
				/* 01004: String data, right truncation */
				addStmtError(stmt, "01004", NULL, 0);
			}
			if (lenp)
				*lenp = sz;
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
			sz = snprintf(data, buflen, "%s" ULLFMT,
				      nval.sign ? "" : "-",
				      nval.val / f);
			if (sz < 0 || sz >= buflen) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			if (nval.scale > 0) {
				data += sz;
				buflen -= sz;
				if (buflen > 2)
					sz = snprintf(data, buflen, ".%0*u",
						      nval.scale,
						      nval.val % f);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* 01004: String data, right
					   truncation */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		}
		case SQL_DOUBLE:
		case SQL_FLOAT: {
			int i;
			data = (char *) ptr;
			for (i = 0; i < 18; i++) {
				sz = snprintf(data, buflen, "%.*g", i, fval);
				if (sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					if (i == 0) {
						/* 22003: Numeric
						   value out of
						   range */
						addStmtError(stmt, "22003", NULL, 0);
						return SQL_ERROR;
					}
					/* 01004: String data, right
					   truncation */
					addStmtError(stmt, "01004", NULL, 0);
					break;
				}
				if (fval == strtod(data, NULL))
					break;
			}
			break;
		}
		case SQL_TYPE_DATE:
			if (buflen < 11) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			data = (char *) ptr;
			sz = snprintf(data, buflen, "%04u-%02u-%02u",
				      dval.year, dval.month, dval.day);
			if (sz < 0 || sz >= buflen) {
				data[buflen - 1] = 0;
				/* 01004: String data, right truncation */
				addStmtError(stmt, "01004", NULL, 0);
			}
			if (lenp)
				*lenp = sz;
			break;
		case SQL_TYPE_TIME:
			if (buflen < 9) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			data = (char *) ptr;
			sz = snprintf(data, buflen, "%02u:%02u:%02u",
				      tval.hour, tval.minute, tval.second);
			if (sz < 0 || sz >= buflen) {
				data[buflen - 1] = 0;
				/* 01004: String data, right truncation */
				addStmtError(stmt, "01004", NULL, 0);
			}
			if (lenp)
				*lenp = sz;
			break;
		case SQL_TYPE_TIMESTAMP:
			if (buflen < 20) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			data = (char *) ptr;
			sz = snprintf(data, buflen,
				      "%04u-%02u-%02u %02u:%02u:%02u",
				      tsval.year, tsval.month, tsval.day,
				      tsval.hour, tsval.minute, tsval.second);
			if (sz < 0 || sz >= buflen) {
				data[buflen - 1] = 0;
				/* 01004: String data, right truncation */
				addStmtError(stmt, "01004", NULL, 0);
			}
			if (lenp)
				*lenp = sz;
			if (tsval.fraction && sz < buflen) {
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
					sz = snprintf(data, buflen, ".%0*u",
						      scale, tsval.fraction);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* 01004: String data, right
					   truncation */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		case SQL_INTERVAL_MONTH:
			sz = snprintf((char *) ptr, buflen, "%s%04u-%02u",
				      ival.interval_sign ? "-" : "",
				      ival.intval.year_month.year,
				      ival.intval.year_month.month);
			if (sz < 0 || sz >= buflen) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			if (lenp)
				*lenp = sz;
			break;
		case SQL_INTERVAL_SECOND:
			data = (char *) ptr;
			sz = snprintf(data, buflen, "%s%u %02u:%02u:%02u",
				      ival.interval_sign ? "-" : "",
				      ival.intval.day_second.day,
				      ival.intval.day_second.hour,
				      ival.intval.day_second.minute,
				      ival.intval.day_second.second);
			if (sz < 0 || sz >= buflen) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
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
					sz = snprintf(data, buflen, ".%0*u", i,
						      ival.intval.day_second.fraction);
				if (buflen <= 2 || sz < 0 || sz >= buflen) {
					data[buflen - 1] = 0;
					/* 01004: String data, right
					   truncation */
					addStmtError(stmt, "01004", NULL, 0);
				}
			}
			break;
		}
		break;
	case SQL_C_BINARY:
		if (buflen < 0) {
			/* HY090: Invalid string or buffer length */
			addStmtError(stmt, "HY090", NULL, 0);
			return SQL_ERROR;
		}
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_FLOAT:
		case SQL_DOUBLE:
		case SQL_BIT:
		case SQL_TYPE_DATE:
		case SQL_TYPE_TIME:
		case SQL_TYPE_TIMESTAMP:
		case SQL_INTERVAL_MONTH:
		case SQL_INTERVAL_SECOND:
			break;
		}
		break;
	case SQL_C_BIT:
		if (lenp)
			*lenp = 1;
		switch (sql_type) {
		case SQL_CHAR: {
			char *p;

			errno - 0;
			fval = strtod(data, &p);
			if (p == data || *p) {
				/* 22018: Invalid character value for
				   cast specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			if (errno == ERANGE) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		}
		case SQL_FLOAT:
		case SQL_DOUBLE:
			if (fval < 0 || fval >= 2) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			* (unsigned char *) ptr = fval >= 1;
			if (fval != 0 && fval != 1)
				/* 22018: Invalid character value for
				   cast specification */
				addStmtError(stmt, "22018", NULL, 0);
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
			if (nval.val > 1 ||
			    (!nval.sign &&
			     (nval.val == 1 || truncated))) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			* (unsigned char *) ptr = (unsigned char) nval.val;
			if (truncated)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		}
		default:
			/* 07006: Restricted data type attribute violation */
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
			break;
		case SQL_C_SSHORT:
		case SQL_C_SHORT:
			maxval <<= 15;
			if (lenp)
				*lenp = sizeof(short);
			break;
		case SQL_C_SLONG:
		case SQL_C_LONG:
			maxval <<= 31;
			if (lenp)
				*lenp = sizeof(long);
			break;
		case SQL_C_SBIGINT:
			maxval <<= 63;
			if (lenp)
				*lenp = sizeof(SQLBIGINT);
			break;
		}
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DOUBLE:
		case SQL_FLOAT:
			/* reparse double and float, parse char */
			if (!parseint(data, &nval)) {
				/* 22018: Invalid character value for
				   cast specification */
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
			if (nval.scale < 0 ||
			    nval.val > maxval ||
			    (nval.val == maxval && nval.sign)) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			if (truncated)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			switch (type) {
			case SQL_C_STINYINT:
			case SQL_C_TINYINT:
				* (signed char *) ptr = nval.sign ? nval.val : -nval.val;
				break;
			case SQL_C_SSHORT:
			case SQL_C_SHORT:
				* (short *) ptr = nval.sign ? nval.val : -nval.val;
				break;
			case SQL_C_SLONG:
			case SQL_C_LONG:
				* (long *) ptr = nval.sign ? nval.val : -nval.val;
				break;
			case SQL_C_SBIGINT:
				* (SQLBIGINT *) ptr = nval.sign ? nval.val : -nval.val;
				break;
			}
			break;
		}
		default:
			/* 07006: Restricted data type attribute violation */
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
			break;
		case SQL_C_USHORT:
			maxval <<= 16;
			if (lenp)
				*lenp = sizeof(short);
			break;
		case SQL_C_ULONG:
			maxval <<= 32;
			if (lenp)
				*lenp = sizeof(long);
			break;
		case SQL_C_UBIGINT:
			if (lenp)
				*lenp = sizeof(SQLBIGINT);
			break;
		}
		maxval--;
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DOUBLE:
		case SQL_FLOAT:
			/* reparse double and float, parse char */
			if (!parseint(data, &nval)) {
				/* 22018: Invalid character value for
				   cast specification */
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
			if (nval.scale < 0 || !nval.sign ||
			    (maxval != 0 && nval.val >= maxval)) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			if (truncated)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			switch (type) {
			case SQL_C_UTINYINT:
				* (unsigned char *) ptr = nval.val;
				break;
			case SQL_C_USHORT:
				* (unsigned short *) ptr = nval.val;
				break;
			case SQL_C_ULONG:
				* (unsigned long *) ptr = nval.val;
				break;
			case SQL_C_UBIGINT:
				* (SQLUBIGINT *) ptr = nval.val;
				break;
			}
			break;
		}
		default:
			/* 07006: Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	}
	case SQL_C_NUMERIC:
		switch (sql_type) {
		case SQL_CHAR:
		case SQL_DOUBLE:
		case SQL_FLOAT:
			/* reparse double and float, parse char */
			if (!(i = parseint(data, &nval))) {
				/* 22018: Invalid character value for
				   cast specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			if (i == 2)
				/* 01S07: Fractional truncation */
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
			/* 07006: Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_C_FLOAT:
	case SQL_C_DOUBLE:
		switch (sql_type) {
		case SQL_CHAR: {
			char *p;

			errno = 0;
			fval = strtod(data, &p);
			if (p == data || *p || errno == ERANGE) {
				/* 22018: Invalid character value for
				   cast specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			break;
		}
		case SQL_DOUBLE:
		case SQL_FLOAT:
			break;
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_BIT:
			fval = (double) nval.val;
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
			/* 07006: Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		if (type == SQL_C_FLOAT) {
			* (float *) ptr = (float) fval;
			if ((double) * (float *) ptr != fval) {
				/* 22003: Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			if (lenp)
				*lenp = sizeof(float);
		} else {
			* (double *) ptr = fval;
			if (lenp)
				*lenp = sizeof(double);
		}
		break;
	case SQL_C_TYPE_DATE:
		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
			i = parsetimestamp(data, &tsval);
			/* fall through */
		case SQL_TYPE_TIMESTAMP: /* note i==1 unless we fell through */
			if (i) {
				if (tsval.hour || tsval.minute ||
				    tsval.second || tsval.fraction || i == 2)
					/* 01S07: Fractional truncation */
					addStmtError(stmt, "01S07", NULL, 0);
				dval.year = tsval.year;
				dval.month = tsval.month;
				dval.day = tsval.day;
			} else if (!parsedate(data, &dval)) {
				/* 22018: Invalid character value for
				   cast specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		case SQL_TYPE_DATE:
			* (DATE_STRUCT *) ptr = dval;
			break;
		default:
			/* 07006: Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_C_TYPE_TIME:
		i = 1;
		switch (sql_type) {
		case SQL_CHAR:
			i = parsetimestamp(data, &tsval);
			/* fall through */
		case SQL_TYPE_TIMESTAMP: /* note i==1 unless we fell through */
			if (i) {
				if (tsval.fraction || i == 2)
					/* 01S07: Fractional truncation */
					addStmtError(stmt, "01S07", NULL, 0);
				tval.hour = tsval.hour;
				tval.minute = tsval.minute;
				tval.second = tsval.second;
			} else if (!parsetime(data, &tval)) {
				/* 22018: Invalid character value for
				   cast specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			/* fall through */
		case SQL_TYPE_TIME:
			* (TIME_STRUCT *) ptr = tval;
			break;
		default:
			/* 07006: Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_C_TYPE_TIMESTAMP:
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
					tsval.year = tm.tm_year;
					tsval.month = tm.tm_mon;
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
						/* 22018: Invalid
						   character value for
						   cast
						   specification */
						addStmtError(stmt, "22018", NULL, 0);
						return SQL_ERROR;
					}
				}
			}
			/* fall through */
		case SQL_TYPE_TIMESTAMP: /* note i==1 unless we fell through */
			* (TIMESTAMP_STRUCT *) ptr = tsval;
			break;
		default:
			/* 07006: Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_C_INTERVAL_YEAR:
	case SQL_C_INTERVAL_MONTH:
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
		switch (sql_type) {
		case SQL_CHAR: {
			int n;

			ival.interval_type = SQL_IS_YEAR_TO_MONTH;
			ival.interval_sign = SQL_TRUE;
			if (sscanf(data, "%d-%u%n", &i,
				   &ival.intval.year_month.month, &n) < 2 ||
			    data[n] != 0) {
				/* 22018: Invalid character value for
				   cast specification */
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
			/* 07006: Restricted data type attribute violation */
			addStmtError(stmt, "07006", NULL, 0);
			return SQL_ERROR;
		}
#define p ((SQL_INTERVAL_STRUCT *) ptr)	/* abbrev. */
		p->interval_sign = ival.interval_sign;
		switch (type) {
		case SQL_C_INTERVAL_YEAR:
			p->interval_type = SQL_IS_YEAR;
			if ((p->intval.year_month.year = ival.intval.year_month.year) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			if (ival.intval.year_month.month)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		case SQL_C_INTERVAL_MONTH:
			p->interval_type = SQL_IS_MONTH;
			if ((p->intval.year_month.month = ival.intval.year_month.month + 12 * ival.intval.year_month.year) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			break;
		case SQL_C_INTERVAL_YEAR_TO_MONTH:
			p->interval_type = SQL_IS_YEAR_TO_MONTH;
			if ((p->intval.year_month.year = ival.intval.year_month.year) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			p->intval.year_month.month = ival.intval.year_month.month;
			break;
		}
#undef p
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
		switch (sql_type) {
		case SQL_CHAR: {
			int n;

			ival.interval_type = SQL_IS_DAY_TO_SECOND;
			ival.interval_sign = SQL_TRUE;
			if (sscanf(data, "%d %u:%u:%u%n", &i,
				   &ival.intval.day_second.hour,
				   &ival.intval.day_second.minute,
				   &ival.intval.day_second.second, &n) < 4 ||
			    (data[n] != 0 && data[n] != '.')) {
				/* 22018: Invalid character value for
				   cast specification */
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
				while (*++data) {
					if (*data < '0' || *data > '9') {
						/* 22018: Invalid
						   character value for
						   cast
						   specification */
						addStmtError(stmt, "22018",
							     NULL, 0);
						return SQL_ERROR;
					}
					if (n < 1000000000) {
						i++;
						n *= 10;
						ival.intval.day_second.fraction *= 10;
						ival.intval.day_second.fraction += *data - '0';
					}
				}
			}
			break;
		}
		case SQL_DECIMAL:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			parsesecondinterval(&nval, &ival, &i);
			break;
		case SQL_INTERVAL_SECOND:
			break;
		default:
			/* 07006: Restricted data type attribute violation */
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
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			if (ival.intval.day_second.hour ||
			    ival.intval.day_second.minute ||
			    ival.intval.day_second.second ||
			    ival.intval.day_second.fraction)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		case SQL_C_INTERVAL_HOUR:
			p->interval_type = SQL_IS_HOUR;
			if ((p->intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			if (ival.intval.day_second.minute ||
			    ival.intval.day_second.second ||
			    ival.intval.day_second.fraction)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		case SQL_C_INTERVAL_MINUTE:
			p->interval_type = SQL_IS_MINUTE;
			if ((p->intval.day_second.minute = ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day)) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			if (ival.intval.day_second.second ||
			    ival.intval.day_second.fraction)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		case SQL_C_INTERVAL_SECOND:
			p->interval_type = SQL_IS_SECOND;
			if ((p->intval.day_second.second = ival.intval.day_second.second + 60 * (ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day))) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			p->intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_DAY_TO_HOUR:
			p->interval_type = SQL_IS_HOUR;
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			p->intval.day_second.hour = ival.intval.day_second.hour;
			if (ival.intval.day_second.minute ||
			    ival.intval.day_second.second ||
			    ival.intval.day_second.fraction)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		case SQL_C_INTERVAL_DAY_TO_MINUTE:
			p->interval_type = SQL_IS_DAY_TO_MINUTE;
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			p->intval.day_second.hour = ival.intval.day_second.hour;
			p->intval.day_second.minute = ival.intval.day_second.minute;
			if (ival.intval.day_second.second ||
			    ival.intval.day_second.fraction)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		case SQL_C_INTERVAL_DAY_TO_SECOND:
			p->interval_type = SQL_IS_DAY_TO_SECOND;
			if ((p->intval.day_second.day = ival.intval.day_second.day) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
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
			if ((p->intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			p->intval.day_second.minute = ival.intval.day_second.minute;
			if (ival.intval.day_second.second ||
			    ival.intval.day_second.fraction)
				/* 01S07: Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
			break;
		case SQL_C_INTERVAL_HOUR_TO_SECOND:
			p->interval_type = SQL_IS_HOUR_TO_SECOND;
			if ((p->intval.day_second.hour = ival.intval.day_second.hour + 24 * ival.intval.day_second.day) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
				addStmtError(stmt, "22015", NULL, 0);
				return SQL_ERROR;
			}
			p->intval.day_second.minute = ival.intval.day_second.minute;
			p->intval.day_second.second = ival.intval.day_second.second;
			p->intval.day_second.fraction = ival.intval.day_second.fraction;
			break;
		case SQL_C_INTERVAL_MINUTE_TO_SECOND:
			p->interval_type = SQL_IS_MINUTE_TO_SECOND;
			if ((p->intval.day_second.minute = ival.intval.day_second.minute + 60 * (ival.intval.day_second.hour + 24 * ival.intval.day_second.day)) >= datetime_interval_precision) {
				/* 22015: Interval field overflow */
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
		break;
	}
	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN
SQLFetch_(ODBCStmt *stmt)
{
	ODBCDesc *desc;
	ODBCDescRec *rec;
	int i;
	SQLINTEGER offset;
	SQLRETURN retval = SQL_SUCCESS;

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	assert(stmt->hdl);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or SQLExecDirect first */
		/* HY010: Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	stmt->retrieved = 0;
	stmt->currentCol = 0;

	if (mapi_fetch_row(stmt->hdl) == 0) {
		switch (mapi_error(stmt->Dbc->mid)) {
		case MOK:
			return SQL_NO_DATA;
		case MTIMEOUT:
			/* 08S01: Communication link failure */
			addStmtError(stmt, "08S01",
				     mapi_error_str(stmt->Dbc->mid), 0);
			return SQL_ERROR;
		default:
			/* HY000: General error */
			addStmtError(stmt, "HY000",
				     mapi_error_str(stmt->Dbc->mid), 0);
			return SQL_ERROR;
		}
	}

	desc = stmt->ApplRowDescr;
	if (desc->sql_desc_bind_offset_ptr)
		offset = *desc->sql_desc_bind_offset_ptr;
	else
		offset = 0;
	for (i = 1; i <= desc->sql_desc_count; i++) {
		SQLRETURN rc;

		rec = &desc->descRec[i];
		if (rec->sql_desc_data_ptr == NULL)
			continue;
		stmt->retrieved = 0;
		rc = ODBCFetch(stmt, i,
			       rec->sql_desc_concise_type,
			       rec->sql_desc_data_ptr,
			       rec->sql_desc_octet_length,
			       rec->sql_desc_octet_length_ptr,
			       rec->sql_desc_indicator_ptr,
			       rec->sql_desc_precision,
			       rec->sql_desc_scale,
			       rec->sql_desc_datetime_interval_precision,
			       offset);
		switch (rc) {
		case SQL_ERROR:
			retval = rc;
			break;
		case SQL_NO_DATA:
			if (retval != SQL_ERROR)
				retval = rc;
			break;
		case SQL_SUCCESS_WITH_INFO:
			if (retval == SQL_SUCCESS)
				retval = rc;
			break;
		case SQL_SUCCESS:
		default:
			break;
		}
	}

	stmt->currentRow++;

	return SQL_SUCCESS;
}

SQLRETURN
SQLFetch(SQLHSTMT hStmt)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLFetch\n");
#endif

	return SQLFetch_((ODBCStmt *) hStmt);
}
