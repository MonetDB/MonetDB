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
# define ULL_CONSTANT(val)     (val)
#elif SIZEOF_LONG==8
# define ULL_CONSTANT(val)     (val##UL)
#elif defined(HAVE_LONG_LONG)
# define ULL_CONSTANT(val)     (val##ULL)
#elif defined(HAVE___INT64)
# define ULL_CONSTANT(val)     (val##ui64)
#endif

struct c_int_types {
	int type;		/* name of the type */
	long sign;		/* signed vs. unsigned */
	int size;		/* sizeof */
	SQLUBIGINT maxdiv10;	/* floor(max value / 10) */
	int lastdig;		/* max value mod 10 as char */
} c_int_types[] = {
	/* SQL_C_UBIGINT must be first for getnumeric() */
	{SQL_C_UBIGINT, 0, sizeof(SQLUBIGINT), ULL_CONSTANT(1844674407370955161), '5'},
	{SQL_C_STINYINT, 1, sizeof(signed char), 12, '7'},
	{SQL_C_UTINYINT, 0, sizeof(unsigned char), 25, '5'},
	{SQL_C_TINYINT, 1, sizeof(signed char), 12, '7'},
	{SQL_C_SSHORT, 1, sizeof(short), 3276, '7'},
	{SQL_C_USHORT, 0, sizeof(unsigned short), 6553, '5'},
	{SQL_C_SHORT, 1, sizeof(short), 3276, '7'},
	{SQL_C_SLONG, 1, sizeof(long), 214748364L, '7'},
	{SQL_C_ULONG, 0, sizeof(unsigned long), 429496729UL, '5'},
	{SQL_C_LONG, 1, sizeof(long), 214748364L, '7'},
	{SQL_C_SBIGINT, 1, sizeof(SQLBIGINT), ULL_CONSTANT(922337203685477580), '7'},
	{SQL_C_BIT, 0, sizeof(unsigned char), 0, '1'},
};
#define NC_INT_TYPES	(sizeof(c_int_types) / sizeof(c_int_types[0]))

/*
  State machine to check whether a string is a number.  Possible
  results: isINT, isFLOAT, isBAD.

  Recognized syntax:
	[+-]? [0-9]* ( "." [0-9]* )? ( [eE] [+-]? [0-9]+ )?
*/

enum number {
	 isEMPTY, isSIGN, isINT, isFLOAT, isFRACT, isEXP, isEXPSIGN, isMANT, isBAD,
};

static enum number
checknum(const char *s, struct c_int_types *p, SQLUBIGINT *valp, int *signp,
	 int *scalep, int *precisionp)
{
	int state = isEMPTY;
	SQLUBIGINT ival = 0;
	int scale = 0;		/* number of digits behind decimal point */
	int precision = 0;	/* number of significant decimal digits */
	int exp = 0;

	*signp = 1;
	while (*s == ' ' || *s == '\t')
		s++;

	while (*s) {
		if ('0' <= *s && *s <= '9') {
			if (state == isEMPTY || state == isSIGN)
				state = isINT;
			else if (state == isFLOAT)
				state = isFRACT;
			else if (state == isEXP || state == isEXPSIGN)
				state = isMANT;
			/* else unchanged as isINT, isFRACT or isMANT */
			if (state == isINT) {
				precision++;
				if (ival > p->maxdiv10 ||
				    (ival == p->maxdiv10 && *s > p->lastdig))
					*signp = 0; /* overflow */
				else {
					ival *= 10;
					ival += *s - '0';
				}
			} else if (state == isFRACT && scalep) {
				if (ival <= p->maxdiv10 &&
				    (ival != p->maxdiv10 ||
				     *s <= p->lastdig)) {
					precision++;
					scale++;
					ival *= 10;
					ival += *s - '0';
				}
			} else if (state == isMANT) {
				if (exp > 1000000)
					*signp = 0;
				else {
					exp *= 10;
					exp += *s - '0';
				}
			}
		} else if (*s == '.') {
			if (state == isINT)
				state = isFLOAT;
			else
				return isBAD;
		} else if (*s == 'e' || *s == 'E') {
			if (state == isINT || state == isFLOAT || state == isFRACT)
				state = isEXP;
			else
				return isBAD;
		} else if (*s == '+' || *s == '-') {
			if (state == isEMPTY) {
				if (*s == '-')
					*signp = -1;
				state = isSIGN;
			} else if (state == isEXP)
				state = isEXPSIGN;
			else
				return isBAD;
		} else if (*s == ' ' || *s == '\t') {
			while (*++s && (*s == ' ' || *s == '\t'))
				;
			if (*s == 0)
				break;
			return isBAD;
		} else
			return isBAD;
		s++;
	}
	if (!p->sign && *signp < 0 && ival != 0)
		*signp = 0;
	*valp = ival;
	if (scalep)
		*scalep = scale - exp;
	if (precisionp)
		*precisionp = precision;
	if (state == isFLOAT || state == isFRACT || isMANT)
		return isFLOAT;
	if (state == isINT)
		return isINT;
	return isBAD;
}

static SQLRETURN
getnum(ODBCStmt *stmt, const char *data, SQLSMALLINT type,
       SQLPOINTER ptr, SQLINTEGER *lenp)
{
	struct c_int_types *p;
	SQLRETURN rc = SQL_SUCCESS;
	SQLUBIGINT ival;
	int sign;
	enum number num;

	for (p = c_int_types; p < &c_int_types[NC_INT_TYPES]; p++) {
		if (p->type == type) {
			num = checknum(data, p, &ival, &sign, NULL, NULL);
			if (num == isBAD) {
				/* Invalid character value for cast
				   specification */
				addStmtError(stmt, "22018", NULL, 0);
				return SQL_ERROR;
			}
			if (sign == 0) {
				/* Numeric value out of range */
				addStmtError(stmt, "22003", NULL, 0);
				return SQL_ERROR;
			}
			if (num == isFLOAT) {
				double d;
				char *e;
				errno = 0;
				d = strtod(data, &e);
				if (e == data || errno == ERANGE ||
				    d > 10 * (double) (SQLBIGINT) p->maxdiv10 ||
				    (d < 0 && !p->sign)) {
					/* Numeric value out of range */
					addStmtError(stmt, "22003", NULL, 0);
					return SQL_ERROR;
				}
				if (d < 0) {
					sign = -1;
					d = -d;
				}
				ival = (SQLUBIGINT) d;
				/* Fractional truncation */
				addStmtError(stmt, "01S07", NULL, 0);
				rc = SQL_SUCCESS_WITH_INFO;
			}
			/* don't use a switch because of possible duplicates */
			if (p->size == sizeof(char)) {
				if (p->sign)
					* (signed char *) ptr = (signed char) ival * sign;
				else
					* (unsigned char *) ptr = (unsigned char) ival;
			} else if (p->size == sizeof(short)) {
				if (p->sign)
					* (short *) ptr = (short) ival * sign;
				else
					* (unsigned short *) ptr = (unsigned short) ival;
			} else if (p->size == sizeof(int)) {
				if (p->sign)
					* (int *) ptr = (int) ival * sign;
				else
					* (unsigned int *) ptr = (unsigned int) ival;
			} else if (p->size == sizeof(long)) {
				if (p->sign)
					* (long *) ptr = (long) ival * sign;
				else
					* (unsigned long *) ptr = (unsigned long) ival;
			} else if (p->size == sizeof(SQLBIGINT)) {
				if (p->sign)
					* (SQLBIGINT *) ptr = (SQLBIGINT) ival * sign;
				else
					* (SQLUBIGINT *) ptr = (SQLUBIGINT) ival;
			}
			if (lenp)
				*lenp = p->size;
			return rc;
		}
	}
	assert(0);
	return SQL_ERROR;
}

static SQLRETURN
getfloat(ODBCStmt *stmt, const char *data, SQLSMALLINT type,
	 SQLPOINTER ptr, SQLINTEGER *lenp)
{
	double d;
	char *e;

	errno = 0;
	d = strtod(data, &e);
	if (e == data || *e != 0) {
		/* Invalid character value for cast specification */
		addStmtError(stmt, "22018", NULL, 0);
		return SQL_ERROR;
	}
	if (errno == ERANGE) {
		/* Numeric value out of range */
		addStmtError(stmt, "22003", NULL, 0);
		return SQL_ERROR;
	}
	if (type == SQL_C_FLOAT) {
		* (float *) ptr = d;
		if (* (float *) ptr != d) {
			/* Numeric value out of range */
			addStmtError(stmt, "22003", NULL, 0);
			return SQL_ERROR;
		}
		if (lenp)
			*lenp = sizeof(float);
	} else {
		* (double *) ptr = d;
		if (lenp)
			*lenp = sizeof(double);
	}
	return SQL_SUCCESS;
}

static SQLRETURN
getnumeric(ODBCStmt *stmt, const char *data, SQLSMALLINT precision,
	   SQLSMALLINT scale, SQLPOINTER ptr, SQLINTEGER *lenp)
{
	SQL_NUMERIC_STRUCT *num = (SQL_NUMERIC_STRUCT *) ptr;
	SQLUBIGINT val;
	int sign;
	int sc;
	int prec;

	if (checknum(data, &c_int_types[0], &val, &sign, &sc, &prec) == isBAD) {
		/* Invalid character value for cast specification */
		addStmtError(stmt, "22018", NULL, 0);
		return SQL_ERROR;
	}
	if (sign == 0) {
		/* Numeric value out of range */
		addStmtError(stmt, "22003", NULL, 0);
		return SQL_ERROR;
	}

	/* fix up scale and precision */
	while (prec > precision) {
		/* too much precision, loose digits at the end */
		val /= 10;
		sc--;
		prec--;
	}
	while (sc < scale) {
		if (val >= c_int_types[0].maxdiv10) {
			/* Numeric value out of range */
			addStmtError(stmt, "22003", NULL, 0);
			return SQL_ERROR;
		}
		val *= 10;
		sc++;
	}
	while (sc > scale) {
		val /= 10;
		sc--;
		prec--;		/* we're loosing significant digits */
	}

	num->precision = prec;
	num->scale = sc;
	num->sign = sign > 0 ? 1 : 0; /* 1 = pos, 0 = neg */
	/* sc is not needed anymore so we can use it as counter */
	for (sc = 0; sc < SQL_MAX_NUMERIC_LEN; sc++) {
		num->val[sc] = (SQLCHAR) (val & 0xFF);
		val >>= 8;
	}

	if (lenp)
		*lenp = sizeof(*num);
	return SQL_SUCCESS;
}

static short monthlengths[] = {
	0,
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

static SQLRETURN
getdatetime(ODBCStmt *stmt, const char *data,
	    SQLSMALLINT *yearp, SQLUSMALLINT *monthp, SQLUSMALLINT *dayp,
	    SQLUSMALLINT *hourp, SQLUSMALLINT *minutep, SQLUSMALLINT *secondp,
	    SQLUINTEGER *fractionp)
{
	short year;
	unsigned short month, day, hour, minute, second;
	unsigned int fraction = 0;
	int n, n1;

	n = sscanf(data, "%hd-%hu-%hu %hu:%hu:%hu%n", &year, &month, &day,
		   &hour, &minute, &second, &n1);
	if (n >= 6) {
		if (data[n1] == '.') {
			/* fraction part */
			unsigned int fac = 1000000000;

			for (n1++; isdigit((int) data[n1]); n1++) {
				fac /= 10;
				fraction += (data[n1] - '0') * fac;
			}
		}
		if (data[n1] || month == 0 || month > 12 || day == 0 ||
		    day > monthlengths[month] || hour >= 24 || minute >= 60 ||
		    second >= 60 || (month == 2 && !isLeap(year) && day == 29)) {
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}
	} else if (n >= 3) {
		hour = minute = second = 0;
		n = sscanf(data, "%hd-%hu-%hu%n", &year, &month, &day, &n1);
		if (n < 3 || data[n1] != 0 || month == 0 || month > 12 ||
		    day == 0 || day > monthlengths[month] ||
		    (month == 2 && !isLeap(year) && day == 29) ||
		    yearp == NULL) {
			/* values out of range, or requested SQL_C_TIME */
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}
	} else {
		if (yearp) {
			/* year/month/day defaults to current day, but
			   only bother finding out current day if we
			   need the values */
			struct tm tm;
			time_t t;

			(void) time(&t);
#ifdef HAVE_LOCALTIME_R
			(void) localtime_r(&t, &tm);
#else
			tm = *localtime(&t); /* copy as quickly as possible */
#endif
			year = tm.tm_year;
			month = tm.tm_mon;
			day = tm.tm_mday;
		} else {
			year = 0;
			month = day = 0;
		}
		n = sscanf(data, "%hu:%hu:%hu%n",
			   &hour, &minute, &second, &n1);
		if (n >= 3 && data[n1] == '.') {
			/* fraction part */
			unsigned int fac = 1000000000;

			for (n1++; isdigit((int) data[n1]); n1++) {
				fac /= 10;
				fraction += (data[n1] - '0') * fac;
			}
		}
		if (n < 3 || data[n1] != 0 || hour >= 24 || minute >= 60 ||
		    second >= 60 || hourp == NULL) {
			/* values out of range, or requested SQL_C_DATE */
			/* Invalid character value for cast specification */
			addStmtError(stmt, "22018", NULL, 0);
			return SQL_ERROR;
		}
	}
	if (yearp)
		*yearp = year;
	if (monthp)
		*monthp = month;
	if (dayp)
		*dayp = day;
	if (hourp)
		*hourp = hour;
	if (minutep)
		*minutep = minute;
	if (secondp)
		*secondp = second;
	if (fractionp)
		*fractionp = fraction;
	if (fractionp == NULL && fraction != 0) {
		/* Fractional truncation */
		addStmtError(stmt, "01S07", NULL, 0);
		return SQL_SUCCESS_WITH_INFO;
	}
	return SQL_SUCCESS;
}

SQLRETURN
ODBCFetch(ODBCStmt *stmt, SQLUSMALLINT nCol, SQLSMALLINT type,
	  SQLPOINTER ptr, SQLINTEGER buflen, SQLINTEGER *lenp,
	  SQLINTEGER *nullp, SQLSMALLINT precision, SQLSMALLINT scale,
	  SQLINTEGER datetime_interval_precision)
{
	char *data;

	if (precision == UNAFFECTED || scale == UNAFFECTED ||
	    datetime_interval_precision == UNAFFECTED) {
		ODBCDesc *desc = stmt->ApplRowDescr;
		ODBCDescRec *rec;

		if (nCol <= desc->sql_desc_count) {
			rec = &desc->descRec[nCol];
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

	(void) datetime_interval_precision;

	data = mapi_fetch_field(stmt->hdl, nCol - 1);
	if (mapi_error(stmt->Dbc->mid)) {
		addStmtError(stmt, "HY000", NULL, 0);
		return SQL_ERROR;
	}
	if (strcmp(data, "nil") == 0) {
		if (nullp == NULL) {
			addStmtError(stmt, "22002", NULL, 0);
			return SQL_ERROR;
		}
		*nullp = SQL_NULL_DATA;
		return SQL_SUCCESS;
	}

	switch (type) {
	case SQL_C_CHAR:
	case SQL_C_BINARY:
		if (buflen < 0) {
			addStmtError(stmt, "HY090", NULL, 0);
			return SQL_ERROR;
		} else {
			size_t length = strlen(data);
			SQLRETURN rc = SQL_SUCCESS;

			length -= stmt->retrieved;
			if (length <= 0)
				return SQL_NO_DATA;
			data += stmt->retrieved;

			if (buflen > 0) {
				strncpy((char *) ptr, data, buflen - 1);
				stmt->retrieved += buflen - 1;
				if (length >= buflen) {
					((char *) ptr)[buflen - 1] = 0;
					addStmtError(stmt, "01004", NULL, 0);
					rc = SQL_SUCCESS_WITH_INFO;
				}
			} else {
				/* zero length buffer, it'll never fit */
				addStmtError(stmt, "01004", NULL, 0);
				rc = SQL_SUCCESS_WITH_INFO;
			}
			if (lenp)
				*lenp = length;
			return rc;
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
		return getnum(stmt, data, type, ptr, lenp);
	case SQL_C_NUMERIC:
		return getnumeric(stmt, data, precision, scale, ptr, lenp);
	case SQL_C_FLOAT:
	case SQL_C_DOUBLE:
		return getfloat(stmt, data, type, ptr, lenp);
	case SQL_C_TYPE_DATE:
		return getdatetime(stmt, data, &((DATE_STRUCT *) ptr)->year,
				   &((DATE_STRUCT *) ptr)->month,
				   &((DATE_STRUCT *) ptr)->day,
				   NULL, NULL, NULL, NULL);
	case SQL_C_TYPE_TIME:
		return getdatetime(stmt, data, NULL, NULL, NULL,
				   &((TIME_STRUCT *) ptr)->hour,
				   &((TIME_STRUCT *) ptr)->minute,
				   &((TIME_STRUCT *) ptr)->second, NULL);
	case SQL_C_TYPE_TIMESTAMP:
		return getdatetime(stmt, data,
				   &((TIMESTAMP_STRUCT *) ptr)->year,
				   &((TIMESTAMP_STRUCT *) ptr)->month,
				   &((TIMESTAMP_STRUCT *) ptr)->day,
				   &((TIMESTAMP_STRUCT *) ptr)->hour,
				   &((TIMESTAMP_STRUCT *) ptr)->minute,
				   &((TIMESTAMP_STRUCT *) ptr)->second,
				   &((TIMESTAMP_STRUCT *) ptr)->fraction);
	case SQL_C_INTERVAL_YEAR:
	case SQL_C_INTERVAL_MONTH:
	case SQL_C_INTERVAL_DAY:
	case SQL_C_INTERVAL_HOUR:
	case SQL_C_INTERVAL_MINUTE:
	case SQL_C_INTERVAL_SECOND:
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
	case SQL_C_INTERVAL_DAY_TO_HOUR:
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
	case SQL_C_INTERVAL_DAY_TO_SECOND:
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
		break;
	}
	addStmtError(stmt, "HYC00", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN
SQLFetch_(ODBCStmt *stmt)
{
	ODBCDesc *desc;
	ODBCDescRec *rec;
	int i;
	SQLRETURN retval = SQL_SUCCESS;

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	assert(stmt->hdl);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	stmt->retrieved = 0;
	stmt->currentCol = 0;

	if (mapi_fetch_row(stmt->hdl) == 0)
		return SQL_NO_DATA;

	desc = stmt->ApplRowDescr;
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
			       rec->sql_desc_datetime_interval_precision);
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
