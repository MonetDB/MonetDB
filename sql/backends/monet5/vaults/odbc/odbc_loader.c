/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "gdk.h"	// COLnew(), BUNappend()
#include "gdk_time.h"	// date_create(), daytime_create(), timestamp_create()
#include "mal_exception.h"
#include "mal_builder.h"
#include "mal_client.h"
#include "mutils.h"	/* utf8toutf16(), utf16toutf8() */
#include "rel_proto_loader.h"
#include "rel_exp.h"
// #include "sql_decimal.h"	/* decimal_from_str() */

#ifdef _MSC_VER
#include <WTypes.h>
#endif
#include <stdint.h>
#include <ctype.h>
#include <wchar.h>

/**** Define the ODBC Version our ODBC application complies with ****/
#define ODBCVER 0x0352		/* Important: this must be defined before include of sql.h and sqlext.h */
#include <sql.h>
#include <sqlext.h>


#define ODBC_RELATION 1
#define ODBC_LOADER   2

#define QUERY_MAX_COLUMNS 4096
#define MAX_COL_NAME_LEN  1023
#define MAX_TBL_NAME_LEN  1023

#ifdef HAVE_HGE
#define MAX_PREC  38
#else
#define MAX_PREC  18
#endif

/* MonetDB ODBC Driver defines in ODBCGlobal.h  SQL_HUGEINT 0x4000 */
#define SQL_HUGEINT	0x4000

typedef struct {
	SQLSMALLINT dataType;		/* ODBC datatype */
	SQLULEN columnSize;		/* ODBC colsize, contains precision for decimals */
	SQLSMALLINT decimalDigits;	/* ODBC dec. digits, contains scale for decimals */
	int battype;			/* MonetDB atom type, used to create the BAT */
	BAT * bat;			/* MonetDB BAT */
} rescol_t;

/* map ODBC SQL datatype to MonetDB SQL datatype */
static sql_subtype *
map_rescol_type(SQLSMALLINT dataType, SQLULEN columnSize, SQLSMALLINT decimalDigits, mvc * sql)
{
	char * typenm;
	unsigned int interval_type = 0;

	switch (dataType) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
	default:	/* all other ODBC types are also mapped to varchar for now */
		/* all ODBC char datatypes are mapped to varchar. char and clob are internally not used anymore */
		if (columnSize > (SQLULEN) INT_MAX)
			columnSize = INT_MAX;
		return sql_bind_subtype(sql->sa, "varchar", (unsigned int) columnSize, 0);

	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		if (columnSize > (SQLULEN) INT_MAX)
			columnSize = INT_MAX;
		return sql_bind_subtype(sql->sa, "blob", (unsigned int) columnSize, 0);

	case SQL_DECIMAL:
	case SQL_NUMERIC:
	{
		/* columnSize contains the defined number of digits, so precision. */
		/* decimalDigits contains the scale (which can be negative). */
		if (columnSize > MAX_PREC || abs(decimalDigits) > MAX_PREC) {
			/* too large precision/scale, not supported by MonetDB. Map this column to a string */
			if (columnSize > (SQLULEN) INT_MAX)
				columnSize = INT_MAX;
			return sql_bind_subtype(sql->sa, "varchar", (unsigned int) columnSize +3, 0);
		}

		return sql_bind_subtype(sql->sa, "varchar", (unsigned int) columnSize +3, 0);
//		unsigned int prec = MAX(1, columnSize); /* precision must be >= 1 */
//		unsigned int scale = MAX(0, decimalDigits); /* negative scales are not supported by MonetDB */
//		if (prec < scale)
//			prec = scale;	/* make precision large enough to contain all decimal digits */
//		return sql_bind_subtype(sql->sa, "decimal", prec, scale);
	}

	case SQL_BIT:
		typenm = "boolean";
		break;

	case SQL_TINYINT:
		typenm = "tinyint";
		break;
	case SQL_SMALLINT:
		typenm = "smallint";
		break;
	case SQL_INTEGER:
		typenm = "int";
		break;
	case SQL_BIGINT:
		typenm = "bigint";
		break;
#ifdef HAVE_HGE
	case SQL_HUGEINT:
		typenm = "hugeint";
		break;
#endif

	case SQL_REAL:
		typenm = "real";
		break;
	case SQL_DOUBLE:
		typenm = "double";
		break;
	case SQL_FLOAT:
		/* the precision of SQL_FLOAT can be either 24 or 53:
		   if it is 24, the SQL_FLOAT data type is the same as SQL_REAL;
		   if it is 53, the SQL_FLOAT data type is the same as SQL_DOUBLE. */
		typenm = (columnSize == 7) ? "real" : "double";
		break;

	case SQL_TYPE_DATE:
		typenm = "date";
		break;
	case SQL_TYPE_TIME:
		/* decimalDigits contains the precision of fractions of a second */
		typenm = "time";
		break;
	case SQL_DATETIME:
	case SQL_TYPE_TIMESTAMP:
		/* decimalDigits contains the precision of fractions of a second */
		typenm = "timestamp";
		break;

	case SQL_INTERVAL_YEAR:
		typenm = "month_interval";
		interval_type = 1;
		break;
	case SQL_INTERVAL_YEAR_TO_MONTH:
		typenm = "month_interval";
		interval_type = 2;
		break;
	case SQL_INTERVAL_MONTH:
		typenm = "month_interval";
		interval_type = 3;
		break;
	case SQL_INTERVAL_DAY:
		typenm = "day_interval";
		interval_type = 4;
		break;
	case SQL_INTERVAL_HOUR:
		typenm = "sec_interval";
		interval_type = 8;
		break;
	case SQL_INTERVAL_MINUTE:
		typenm = "sec_interval";
		interval_type = 11;
		break;
	case SQL_INTERVAL_SECOND:
		typenm = "sec_interval";
		interval_type = 13;
		break;
	case SQL_INTERVAL_DAY_TO_HOUR:
		typenm = "sec_interval";
		interval_type = 5;
		break;
	case SQL_INTERVAL_DAY_TO_MINUTE:
		typenm = "sec_interval";
		interval_type = 6;
		break;
	case SQL_INTERVAL_DAY_TO_SECOND:
		typenm = "sec_interval";
		interval_type = 7;
		break;
	case SQL_INTERVAL_HOUR_TO_MINUTE:
		typenm = "sec_interval";
		interval_type = 9;
		break;
	case SQL_INTERVAL_HOUR_TO_SECOND:
		typenm = "sec_interval";
		interval_type = 10;
		break;
	case SQL_INTERVAL_MINUTE_TO_SECOND:
		typenm = "sec_interval";
		interval_type = 12;
		break;

	case SQL_GUID:
		/* represents a uuid of length 36, such as: dbe7343c-1f11-4fa9-a9c8-a31cd26f92fe */
		typenm = "uuid";
		break;
	}
	return sql_bind_subtype(sql->sa, typenm, interval_type, 0);
}

/* return name for ODBC SQL datatype */
static char *
nameofSQLtype(SQLSMALLINT dataType)
{
	/* https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-data-types */
	switch (dataType) {
	case SQL_CHAR:		return "CHAR";
	case SQL_VARCHAR:	return "VARCHAR";
	case SQL_LONGVARCHAR:	return "LONG VARCHAR";
	case SQL_WCHAR:		return "WCHAR";
	case SQL_WVARCHAR:	return "WVARCHAR";
	case SQL_WLONGVARCHAR:	return "WLONGVARCHAR";
	case SQL_DECIMAL:	return "DECIMAL";
	case SQL_NUMERIC:	return "NUMERIC";
	case SQL_SMALLINT:	return "SMALLINT";
	case SQL_INTEGER:	return "INTEGER";
	case SQL_REAL:		return "REAL";
	case SQL_FLOAT:		return "FLOAT";
	case SQL_DOUBLE:	return "DOUBLE";
	case SQL_BIT:		return "BIT";
	case SQL_TINYINT:	return "TINYINT";
	case SQL_BIGINT:	return "BIGINT";
	case SQL_BINARY:	return "BINARY";
	case SQL_VARBINARY:	return "VARBINARY";
	case SQL_LONGVARBINARY:	return "LONG VARBINARY";
	case SQL_DATETIME:	return "DATETIME";
	case SQL_TYPE_DATE:	return "DATE";
	case SQL_TYPE_TIME:	return "TIME";
	case SQL_TYPE_TIMESTAMP:	return "TIMESTAMP";
	case SQL_INTERVAL_MONTH:	return "INTERVAL MONTH";
	case SQL_INTERVAL_YEAR:		return "INTERVAL YEAR";
	case SQL_INTERVAL_YEAR_TO_MONTH: return "INTERVAL YEAR TO MONTH";
	case SQL_INTERVAL_DAY:		return "INTERVAL DAY";
	case SQL_INTERVAL_HOUR:		return "INTERVAL HOUR";
	case SQL_INTERVAL_MINUTE:	return "INTERVAL MINUTE";
	case SQL_INTERVAL_SECOND:	return "INTERVAL SECOND";
	case SQL_INTERVAL_DAY_TO_HOUR:	return "INTERVAL DAY TO HOUR";
	case SQL_INTERVAL_DAY_TO_MINUTE:	return "INTERVAL DAY TO MINUTE";
	case SQL_INTERVAL_DAY_TO_SECOND:	return "INTERVAL DAY TO SECOND";
	case SQL_INTERVAL_HOUR_TO_MINUTE:	return "INTERVAL HOUR TO MINUTE";
	case SQL_INTERVAL_HOUR_TO_SECOND:	return "INTERVAL HOUR TO SECOND";
	case SQL_INTERVAL_MINUTE_TO_SECOND:	return "INTERVAL MINUTE TO SECOND";
	case SQL_GUID:		return "GUID";
	case SQL_HUGEINT:	return "HUGEINT";
	default:		return "Driver specific type";
	}
}

/* name of ODBC SQLRETURN codes */
static char *
nameOfRetCode(SQLRETURN code)
{
	switch (code) {
	case SQL_SUCCESS:		return "SQL_SUCCESS";
	case SQL_SUCCESS_WITH_INFO:	return "SQL_SUCCESS_WITH_INFO";
	case SQL_ERROR:			return "SQL_ERROR";
	case SQL_INVALID_HANDLE:	return "SQL_INVALID_HANDLE";
	case SQL_STILL_EXECUTING:	return "SQL_STILL_EXECUTING";
	case SQL_NEED_DATA:		return "SQL_NEED_DATA";
	case SQL_NO_DATA:		return "SQL_NO_DATA";
	default:		return "SQLRETURN ??";
	}
}

#ifdef HAVE_HGE
static hge
str_to_hge(const char *s) {
	char c;
	char sign = '+';
	int i = 0;
	hge ret = 0;

	if (!s)
		return 0;

	c = s[i];
	if (c == '-' || c == '+') {
		sign = c;
		c = s[++i];
	}
	while (c) {
		if (c >= '0' && c <= '9') {
			ret *= 10;
			ret += (int) c - '0';
		}
		c = s[++i];
	}
	if (sign == '-')
		ret = -ret;
	return ret;
}
#endif

/* an ODBC function call returned an error, get the error msg from the ODBC driver */
static char *
getErrMsg(SQLSMALLINT handleType, SQLHANDLE handle) {
	SQLRETURN ret;
	SQLCHAR state[SQL_SQLSTATE_SIZE +1];
	SQLINTEGER errnr;
	SQLCHAR msg[SQL_MAX_MESSAGE_LENGTH] = { 0 };
	SQLSMALLINT msglen = SQL_MAX_MESSAGE_LENGTH -1;

	if (handle == SQL_NULL_HSTMT)
		return NULL;

	// TODO use ODBC W function
	ret = SQLGetDiagRec(handleType, handle, 1, state, &errnr, msg, SQL_MAX_MESSAGE_LENGTH -1, &msglen);
	if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
		const char format[] = "SQLSTATE %s, Error code %d, Message %s";
		if (msglen <= 0) {
			/* e.g SQL_NTS */
			msglen = (SQLSMALLINT) strlen((char *)msg);
		}
		char * retmsg = (char *) GDKmalloc(sizeof(format) + SQL_SQLSTATE_SIZE + 10 + msglen);
		if (retmsg != NULL) {
			if (state[SQL_SQLSTATE_SIZE] != '\0')
				state[SQL_SQLSTATE_SIZE] = '\0';
			sprintf(retmsg, format, (char *)state, errnr, (char *)msg);
			return retmsg;
		}
	}
	return NULL;
}

/* utility function to safely close all opened ODBC resources */
static void
odbc_cleanup(SQLHANDLE env, SQLHANDLE dbc, SQLHANDLE stmt) {
	SQLRETURN ret = SQL_SUCCESS;

	if (stmt != SQL_NULL_HSTMT) {
		ret = SQLFreeStmt(stmt, SQL_CLOSE);
		if (ret != SQL_INVALID_HANDLE)
			SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	}
	if (dbc != SQL_NULL_HDBC) {
		ret = SQLDisconnect(dbc);
		if (ret != SQL_INVALID_HANDLE)
			SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	}
	if (env != SQL_NULL_HENV) {
		SQLFreeHandle(SQL_HANDLE_ENV, env);
	}
}

/* copied from monetdb5/modules/mal/tablet.c */
static BAT *
bat_create(int adt, BUN nr)
{
	BAT *b = COLnew(0, adt, nr, TRANSIENT);

	/* check for correct structures */
	if (b == NULL)
		return NULL;
	if ((b = BATsetaccess(b, BAT_APPEND)) == NULL) {
		return NULL;
	}

	/* disable all properties here */
	b->tsorted = false;
	b->trevsorted = false;
	b->tnosorted = 0;
	b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = 0;
	b->tnokey[1] = 0;
	return b;
}

/*
 * odbc_query() contains the logic for both odbc_relation() and ODBCloader()
 * the caller argument is ODBC_RELATION when called from odbc_relation and ODBC_LOADER when called from ODBCloader
 */
static str
odbc_query(int caller, mvc *sql, sql_subfunc *f, char *url, list *res_exps, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bool trace_enabled = false;	/* used for development only */

	if (sql == NULL)
		return "Missing mvc value.";
	if (f == NULL)
		return "Missing sql_subfunc value.";

	/* check received url and extract the ODBC connection string and the SQL query */
	if (!url || (url && strncasecmp("odbc:", url, 5) != 0))
		return "Invalid URI. Must start with 'odbc:'.";

	// skip 'odbc:' prefix from url so we get a connection string including the query
	char * con_str = &url[5];
	/* the connection string must start with 'DSN=' or 'FILEDSN=' or 'DRIVER='
	   else the ODBC driver manager can't load the ODBC driver */
	if (con_str
	  && (strncmp("DSN=", con_str, 4) != 0)
	  && (strncmp("DRIVER=", con_str, 7) != 0)
	  && (strncmp("FILEDSN=", con_str, 8) != 0))
		return "Invalid ODBC connection string. Must start with 'DSN=' or 'FILEDSN=' or 'DRIVER='.";

	// locate the 'QUERY=' part to extract the SQL query string to execute
	char * qry_str = strstr(con_str, "QUERY=");
	if (qry_str == NULL)
		return "Incomplete ODBC URI string. Missing 'QUERY=' part to specify the SQL SELECT query to execute.";

	char * query = GDKstrdup(&qry_str[6]);	// we expect that QUERY= is at the end of the connection string
	if (query == NULL || *query == 0) {
		GDKfree(query);
		return "Incomplete ODBC URI string. Missing SQL SELECT query after 'QUERY='.";
	}

	// create a new ODBC connection string without the QUERY= part
	char * odbc_con_str = GDKstrndup(con_str, qry_str - con_str);
	if (odbc_con_str == NULL) {
		GDKfree(query);
		return "Missing ODBC connection string.";
	}

	// trace_enabled = true;
	if (trace_enabled)
		printf("\nExtracted ODBC connection string: %s\n  and SQL query: %s\n", odbc_con_str, query);

	/* now we can (try to) connect to the ODBC driver and execute the SQL query */
	SQLRETURN ret = SQL_INVALID_HANDLE;
	SQLHANDLE env = SQL_NULL_HENV;
	SQLHANDLE dbc = SQL_NULL_HDBC;
	SQLHANDLE stmt = SQL_NULL_HSTMT;
	char * errmsg = NULL;

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC ENV handle failed.";
		goto finish;
	}
	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) (uintptr_t) SQL_OV_ODBC3, 0);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLSetEnvAttr(SQL_ATTR_ODBC_VERSION ODBC3) failed.";
		goto finish;
	}

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC DBC handle failed.";
		goto finish;
	}
	/* to avoid an endless blocking SQLDriverConnect() set a login timeout of 8s */
	ret = SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) (uintptr_t) 8UL, 0);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLSetConnectAttr(SQL_ATTR_LOGIN_TIMEOUT 8 sec) failed.";
		goto finish;
	}

	SQLSMALLINT len = 0;
	uint16_t * odbc_con_Wstr = utf8toutf16(odbc_con_str);
#define MAX_CONNECT_OUT_STR 2048
	if (odbc_con_Wstr != NULL) {
		SQLWCHAR outstr[MAX_CONNECT_OUT_STR];
		ret = SQLDriverConnectW(dbc, NULL, (SQLWCHAR *) odbc_con_Wstr, SQL_NTS, outstr, MAX_CONNECT_OUT_STR, &len, SQL_DRIVER_NOPROMPT);
		/* we no longer need odbc_con_Wstr */
		free(odbc_con_Wstr);
	} else {
		SQLCHAR outstr[MAX_CONNECT_OUT_STR];
		ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *) odbc_con_str, SQL_NTS, outstr, MAX_CONNECT_OUT_STR, &len, SQL_DRIVER_NOPROMPT);
	}
	if (ret == SQL_SUCCESS_WITH_INFO && caller == ODBC_RELATION) {
		/* show the info warning, but only once */
		char * ODBCmsg = getErrMsg(SQL_HANDLE_DBC, dbc);
		printf("SQLDriverConnect(%s) returned %s ODBCmsg: %s\n", odbc_con_str, nameOfRetCode(ret), (ODBCmsg) ? ODBCmsg : "");
		if (ODBCmsg)
			GDKfree(ODBCmsg);
	} else if (trace_enabled) {
		printf("SQLDriverConnect(%s) returned %s\n", odbc_con_str, nameOfRetCode(ret));
	}
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLDriverConnect failed.";
		goto finish;
	}
	/* we no longer need odbc_con_str */
	GDKfree(odbc_con_str);
	odbc_con_str = NULL;

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC STMT handle failed.";
		goto finish;
	}

#ifdef HAVE_HGE
	{
		char DBMSname[128];
		ret = SQLGetInfo(dbc, SQL_DBMS_NAME, (SQLPOINTER) &DBMSname, 127, NULL);
		if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			if (trace_enabled)
				printf("SQLGetInfo(dbc, SQL_DBMS_NAME) returned %s\n", DBMSname);
			if (strcmp("MonetDB", DBMSname) == 0) {
				/* enable the MonetDB ODBC driver to return SQL_HUGEINT as column datatype */
				ret = SQLGetTypeInfo(stmt, SQL_HUGEINT);
				if (trace_enabled)
					printf("SQLGetTypeInfo(stmt, SQL_HUGEINT) returned %s\n", nameOfRetCode(ret));
				if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
					ret = SQLCloseCursor(stmt);
				}
			}
		}
	}
#endif

	uint16_t * query_Wstr = utf8toutf16(query);
	if (query_Wstr != NULL) {
		ret = SQLExecDirectW(stmt, (SQLWCHAR *) query_Wstr, SQL_NTS);
		/* we no longer need query_Wstr */
		free(query_Wstr);
	} else {
		ret = SQLExecDirect(stmt, (SQLCHAR *) query, SQL_NTS);
	}
	if (ret == SQL_SUCCESS_WITH_INFO && caller == ODBC_RELATION) {
		/* show the info warning, but only once */
		char * ODBCmsg = getErrMsg(SQL_HANDLE_STMT, stmt);
		printf("SQLExecDirect(%s) returned %s ODBCmsg: %s\n", query, nameOfRetCode(ret), (ODBCmsg) ? ODBCmsg : "");
		if (ODBCmsg)
			GDKfree(ODBCmsg);
	} else if (trace_enabled) {
		printf("SQLExecDirect(%s) returned %s\n", query, nameOfRetCode(ret));
	}
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLExecDirect query failed.";
		goto finish;
	}
	/* we no longer need query string */
	GDKfree(query);
	query = NULL;

	SQLSMALLINT nr_cols = 0;
	ret = SQLNumResultCols(stmt, &nr_cols);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLNumResultCols failed.";
		goto finish;
	}
	if (nr_cols <= 0) {
		errmsg = "ODBC query did not return a resultset.";
		goto finish;
	}
	if (trace_enabled)
		printf("Query has %d result columns\n", nr_cols);
	if (nr_cols > QUERY_MAX_COLUMNS) {
		/* limit the number of data columns, as we do not want to block or blow up the mserver */
		nr_cols = QUERY_MAX_COLUMNS;
		printf("\nODBC_loader limited Query result to first %d columns.\n", nr_cols);
	}

	/* when called from odbc_relation() */
	if (caller == ODBC_RELATION) {
		char tname[MAX_TBL_NAME_LEN +1];
		char cname[MAX_COL_NAME_LEN +1];
		char * tblname;
		char * colname;
		SQLSMALLINT dataType = 0;
		SQLULEN columnSize = 0;
		SQLSMALLINT decimalDigits = 0;
		sql_subtype * sql_mtype;
		list * typelist = sa_list(sql->sa);
		list * nameslist = sa_list(sql->sa);
		for (SQLUSMALLINT col = 1; col <= (SQLUSMALLINT) nr_cols; col++) {
			/* for each result column get name, datatype, size and decdigits */
			// TODO use ODBC W function
			ret = SQLDescribeCol(stmt, col, (SQLCHAR *) cname, (SQLSMALLINT) MAX_COL_NAME_LEN,
					NULL, &dataType, &columnSize, &decimalDigits, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				errmsg = "SQLDescribeCol failed.";
				goto finish;
			}
			if (trace_enabled)
				printf("ResCol %u, name: %s, type %d (%s), size %u, decdigits %d\n",
					col, cname, dataType, nameofSQLtype(dataType), (unsigned int)columnSize, decimalDigits);
			colname = sa_strdup(sql->sa, cname);
			list_append(nameslist, colname);
			sql_mtype = map_rescol_type(dataType, columnSize, decimalDigits, sql);
			list_append(typelist, sql_mtype);

			if (res_exps) {
				/* also get the table name for this result column */
				// TODO use ODBC W function
				ret = SQLColAttribute(stmt, col, SQL_DESC_TABLE_NAME, (SQLPOINTER) tname, (SQLSMALLINT) MAX_TBL_NAME_LEN, NULL, NULL);
				if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
					strcpy(tname, "");
				}
				tblname = sa_strdup(sql->sa, tname);
				sql_exp *ne = exp_column(sql->sa, tblname, colname, sql_mtype, CARD_MULTI, 1, 0, 0);
				set_basecol(ne);
				ne->alias.label = -(sql->nid++);
				list_append(res_exps, ne);
			}
		}

		f->tname = sa_strdup(sql->sa, tname);
		f->colnames = nameslist;
		f->coltypes = typelist;
		f->res = typelist;
		goto finish;
	}

	/* when called from ODBCloader() */
	if (caller == ODBC_LOADER) {
		rescol_t * colmetadata = (rescol_t *) GDKzalloc(nr_cols * sizeof(rescol_t));
		if (colmetadata == NULL) {
			errmsg = "GDKzalloc colmetadata[nr_cols] failed.";
			goto finish;
		}

		SQLULEN largestStringSize = 0;
		bool hasBlobCols = false;
		SQLULEN largestBlobSize = 0;
		/* make bats with right atom type */
		for (SQLUSMALLINT col = 0; col < (SQLUSMALLINT) nr_cols; col++) {
			char cname[MAX_COL_NAME_LEN +1];
			SQLSMALLINT dataType = 0;
			SQLULEN columnSize = 0;
			SQLSMALLINT decimalDigits = 0;
			int battype = TYPE_str;
			BAT * b = NULL;

			/* for each result column get SQL datatype, size and decdigits */
			// TODO use ODBC W function
			ret = SQLDescribeCol(stmt, col+1, (SQLCHAR *) cname, (SQLSMALLINT) MAX_COL_NAME_LEN,
					NULL, &dataType, &columnSize, &decimalDigits, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				errmsg = "SQLDescribeCol failed.";
				/* cleanup already created bats */
				while (col > 0) {
					col--;
					BBPreclaim(colmetadata[col].bat);
				}
				GDKfree(colmetadata);
				goto finish;
			}
			if (trace_enabled)
				printf("DescCol %u, name: %s, type %d (%s), size %u, decdigits %d\n",
					col+1, cname, dataType, nameofSQLtype(dataType), (unsigned int)columnSize, decimalDigits);
			/* use same logic as used in map_rescol_type() for SQL_FLOAT and SQL_DECIMAL */
			if (dataType == SQL_FLOAT) {
				dataType = (columnSize == 7) ? SQL_REAL : SQL_DOUBLE;
			} else
			if (dataType == SQL_HUGEINT) {
				dataType = SQL_VARCHAR;	/* read it as string */
				columnSize = 50;
			} else
			if (dataType == SQL_DECIMAL || dataType == SQL_NUMERIC) {
				/* MonetDB has limits for the precision and scale */
				if (columnSize > MAX_PREC || abs(decimalDigits) > MAX_PREC) {
					/* very large precision/scale, not supported by MonetDB. Map this column to a string */
					dataType = SQL_VARCHAR;
//					columnSize += 3;	/* add 3 for sign, leading 0 and decimal separator */
				}
//				columnSize = MAX(1, columnSize); /* precision must be >= 1 */
//				decimalDigits = MAX(0, decimalDigits); /* negative scales are not supported by MonetDB */
//				if ((int)columnSize < decimalDigits)
//					columnSize = decimalDigits;	/* make precision large enough to contain all decimal digits */
				dataType = SQL_VARCHAR;	/* read it as string */
				columnSize += 3;	/* add 3 for sign, leading 0 and decimal separator */
			}
			colmetadata[col].dataType = dataType;
			colmetadata[col].columnSize = columnSize;
			colmetadata[col].decimalDigits = decimalDigits;
			battype = getBatType(getArgType(mb, pci, col));
			colmetadata[col].battype = battype;
			if (battype == TYPE_str) {
				if (columnSize > largestStringSize) {
					largestStringSize = columnSize;
				}
			} else
			if (battype == TYPE_blob) {
				hasBlobCols = true;
				if (columnSize > largestBlobSize) {
					largestBlobSize = columnSize;
				}
			}
			if (trace_enabled)
				printf("ResCol %u, name: %s, type %d (%s), size %u, decdigits %d, battype %d\n",
					col+1, cname, dataType, nameofSQLtype(dataType), (unsigned int)columnSize, decimalDigits, battype);

			if (trace_enabled)
				printf("Before create BAT %d type %d\n", col+1, battype);
			b = bat_create(battype, 0);
			if (b) {
				colmetadata[col].bat = b;
				if (trace_enabled)
					printf("Created BAT %d\n", col+1);
			} else {
				errmsg = "Failed to create bat.";
				/* cleanup already created bats */
				while (col > 0) {
					col--;
					BBPreclaim(colmetadata[col].bat);
				}
				GDKfree(colmetadata);
				goto finish;
			}
		}

		/* allocate storage for all the fixed size atom types. */
		bit bit_val = 0;
		bte bte_val = 0;
		sht sht_val = 0;
		int int_val = 0;
		lng lng_val = 0;
#ifdef HAVE_HGE
		hge hge_val = 0;	// for hugeint and decimals with precision > 18
#endif
		flt flt_val = 0;
		dbl dbl_val = 0;
		DATE_STRUCT date_val;
		TIME_STRUCT time_val;
		TIMESTAMP_STRUCT ts_val;
		SQL_INTERVAL_STRUCT itv_val;
		SQLGUID guid_val;
		uuid uuid_val = uuid_nil;

		/* allocate storage for all the var sized atom types. */
		char * str_val = NULL;		// TODO: change to wchar
		bte * blob_val = NULL;
		if (largestStringSize == 0)	// no valid string length, use 65535 (64kB) as default
			largestStringSize = 65535;
		if (largestStringSize < 256)
			largestStringSize = 256;
		if (largestStringSize > 16777215) // string length too large, limit to 16MB
			largestStringSize = 16777215;
		str_val = (char *)GDKzalloc((largestStringSize +1) * sizeof(char));	// +1 for the eos char
		if (!str_val) {
			errmsg = "Failed to alloc memory for largest rescol string.";
			goto finish_fetch;
		}
		if (trace_enabled)
			printf("Allocated str_val buffer of size %zu\n", (largestStringSize +1) * sizeof(char));

		if (hasBlobCols) {
			if (largestBlobSize == 0)	// no valid blob length, assume 65536 (64kB) as default
				largestBlobSize = 65532;
			if (largestBlobSize > 16777212) // blob length too large, limit to 16MB
				largestBlobSize = 16777212;
			blob_val = (bte *)GDKzalloc(sizeof(unsigned int) + (largestBlobSize * sizeof(bte)));
			if (!blob_val) {
				errmsg = "Failed to alloc memory for largest rescol blob.";
				goto finish_fetch;
			}
			if (trace_enabled)
				printf("Allocated blob_val buffer of size %zu\n", largestBlobSize * sizeof(bte));
		}

		gdk_return gdkret = GDK_SUCCEED;
		unsigned long row = 0;
		ret = SQLFetch(stmt);	// TODO optimisation: use SQLExtendedFetch() to pull data array wise and use BUNappendmulti()
		while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			row++;
			if (trace_enabled)
				printf("Fetched row %lu\n", row);

			for (SQLUSMALLINT col = 0; col < (SQLUSMALLINT) nr_cols; col++) {
				SQLSMALLINT sqltype = colmetadata[col].dataType;
				BAT * b = colmetadata[col].bat;
				SQLSMALLINT targetType;
				SQLPOINTER * targetValuePtr;
				SQLLEN bufferLength = 0;
				SQLLEN strLen = 0;
				/* mapping based on https://learn.microsoft.com/en-us/sql/odbc/reference/appendixes/c-data-types */
				switch(sqltype) {
					case SQL_CHAR:
					case SQL_VARCHAR:
					case SQL_LONGVARCHAR:
					case SQL_WCHAR:
					case SQL_WVARCHAR:
					case SQL_WLONGVARCHAR:
					default:
						targetType = SQL_C_CHAR;	// TODO later: SQL_C_WCHAR
						targetValuePtr = (SQLPOINTER *) str_val;
						bufferLength = largestStringSize;
						break;
					case SQL_BIT:
						targetType = SQL_C_BIT;
						targetValuePtr = (SQLPOINTER *) &bit_val;
						break;
					case SQL_TINYINT:
						targetType = SQL_C_STINYINT;
						targetValuePtr = (SQLPOINTER *) &bte_val;
						break;
					case SQL_SMALLINT:
						targetType = SQL_C_SSHORT;
						targetValuePtr = (SQLPOINTER *) &sht_val;
						break;
					case SQL_INTEGER:
						targetType = SQL_C_SLONG;
						targetValuePtr = (SQLPOINTER *) &int_val;
						break;
					case SQL_BIGINT:
						targetType = SQL_C_SBIGINT;
						targetValuePtr = (SQLPOINTER *) &lng_val;
						break;
					case SQL_HUGEINT:
						/* read huge int data as string data as there is no SQL_C_SHUGEINT */
						targetType = SQL_C_CHAR;
						targetValuePtr = (SQLPOINTER *) str_val;
						bufferLength = largestStringSize;
						break;
					case SQL_DECIMAL:
					case SQL_NUMERIC:
						/* read decimal data always as string data and convert it to the right internal decimal format and bat type */
						targetType = SQL_C_CHAR;
						targetValuePtr = (SQLPOINTER *) str_val;
						bufferLength = largestStringSize;
						break;
					case SQL_REAL:
						targetType = SQL_C_FLOAT;
						targetValuePtr = (SQLPOINTER *) &flt_val;
						break;
					case SQL_FLOAT:
						if (colmetadata[col].battype == TYPE_flt) {
							targetType = SQL_C_FLOAT;
							targetValuePtr = (SQLPOINTER *) &flt_val;
						} else {
							targetType = SQL_C_DOUBLE;
							targetValuePtr = (SQLPOINTER *) &dbl_val;
						}
						break;
					case SQL_DOUBLE:
						targetType = SQL_C_DOUBLE;
						targetValuePtr = (SQLPOINTER *) &dbl_val;
						break;
					case SQL_TYPE_DATE:
						targetType = SQL_C_TYPE_DATE;
						targetValuePtr = (SQLPOINTER *) &date_val;
						break;
					case SQL_TYPE_TIME:
						targetType = SQL_C_TYPE_TIME;
						targetValuePtr = (SQLPOINTER *) &time_val;
						break;
					case SQL_DATETIME:
					case SQL_TYPE_TIMESTAMP:
						targetType = SQL_C_TYPE_TIMESTAMP;
						targetValuePtr = (SQLPOINTER *) &ts_val;
						break;
					case SQL_INTERVAL_YEAR:
						targetType = SQL_C_INTERVAL_YEAR;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_YEAR_TO_MONTH:
						targetType = SQL_C_INTERVAL_YEAR_TO_MONTH;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_MONTH:
						targetType = SQL_C_INTERVAL_MONTH;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_DAY:
						targetType = SQL_C_INTERVAL_DAY;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_HOUR:
						targetType = SQL_C_INTERVAL_HOUR;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_MINUTE:
						targetType = SQL_C_INTERVAL_MINUTE;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_SECOND:
						targetType = SQL_C_INTERVAL_SECOND;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_DAY_TO_HOUR:
						targetType = SQL_C_INTERVAL_DAY_TO_HOUR;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_DAY_TO_MINUTE:
						targetType = SQL_C_INTERVAL_DAY_TO_MINUTE;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_DAY_TO_SECOND:
						targetType = SQL_C_INTERVAL_DAY_TO_SECOND;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_HOUR_TO_MINUTE:
						targetType = SQL_C_INTERVAL_HOUR_TO_MINUTE;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_HOUR_TO_SECOND:
						targetType = SQL_C_INTERVAL_HOUR_TO_SECOND;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_INTERVAL_MINUTE_TO_SECOND:
						targetType = SQL_C_INTERVAL_MINUTE_TO_SECOND;
						targetValuePtr = (SQLPOINTER *) &itv_val;
						break;
					case SQL_GUID:
						/* read guid data as string data */
//						targetType = SQL_C_CHAR;
//						targetValuePtr = (SQLPOINTER *) str_val;
//						bufferLength = largestStringSize;
						targetType = SQL_C_GUID;
						targetValuePtr = (SQLPOINTER *) &guid_val;
						bufferLength = 16;
						break;
					case SQL_BINARY:
					case SQL_VARBINARY:
					case SQL_LONGVARBINARY:
						targetType = SQL_C_BINARY;
						targetValuePtr = (SQLPOINTER *) &blob_val;
						bufferLength = largestBlobSize;
						break;
				}
				if (trace_enabled)
					printf("Before SQLGetData(col %u C_type %d buflen %d\n", col+1, targetType, (int)bufferLength);
				ret = SQLGetData(stmt, col+1, targetType, targetValuePtr, bufferLength, &strLen);
				if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
					if (trace_enabled) {
						char * ODBCmsg = getErrMsg(SQL_HANDLE_STMT, stmt);
						printf("Failed to get C_type %d data for col %u of row %lu. ODBCmsg: %s\n",
							targetType, col+1, row, (ODBCmsg) ? ODBCmsg : "");
						if (ODBCmsg)
							GDKfree(ODBCmsg);
					}
					/* as all bats need to be the same length, append NULL value */
					if (BUNappend(b, ATOMnilptr(b->ttype), false) != GDK_SUCCEED)
						if (trace_enabled)
							printf("BUNappend(b, NULL, false) failed\n");
				} else {
					if (strLen == SQL_NULL_DATA) {
						if (trace_enabled)
							printf("Data row %lu col %u: NULL\n", row, col+1);
						if (BUNappend(b, ATOMnilptr(b->ttype), false) != GDK_SUCCEED)
							if (trace_enabled)
								printf("BUNappend(b, ATOMnilptr(b->ttype), false) failed\n");
					} else {
						switch(sqltype) {
							case SQL_CHAR:
							case SQL_VARCHAR:
							case SQL_LONGVARCHAR:
							case SQL_WCHAR:
							case SQL_WVARCHAR:
							case SQL_WLONGVARCHAR:
							default:
								if (strLen != SQL_NTS && strLen >= 0) {
									/* make sure it is a Nul Terminated String */
									if ((SQLULEN) strLen < largestStringSize) {
										if (str_val[strLen] != '\0')
											str_val[strLen] = '\0';
									} else {
										if (str_val[largestStringSize] != '\0')
											str_val[largestStringSize] = '\0';
									}
								}
								if (trace_enabled)
									printf("Data row %lu col %u: %s\n", row, col+1, str_val);
								switch (colmetadata[col].battype) {
									case TYPE_str:
										gdkret = BUNappend(b, (void *) str_val, false);
										break;
#ifdef HAVE_HGE
									case TYPE_hge:
										/* HUGEINT values are read as string */
										hge_val = str_to_hge(str_val);
										gdkret = BUNappend(b, (void *) &hge_val, false);
										break;
#endif
									default:
										if (BUNappend(b, ATOMnilptr(b->ttype), false) != GDK_SUCCEED) {
											if (trace_enabled)
												printf("BUNappend(b, ATOMnilptr(b->ttype), false) failed\n");
										}
										break;
								}
								break;
							case SQL_BIT:
								if (trace_enabled)
									printf("Data row %lu col %u: %x\n", row, col+1, bit_val);
								gdkret = BUNappend(b, (void *) &bit_val, false);
								break;
							case SQL_TINYINT:
								if (trace_enabled)
									printf("Data row %lu col %u: %hd\n", row, col+1, (sht) bte_val);
								gdkret = BUNappend(b, (void *) &bte_val, false);
								break;
							case SQL_SMALLINT:
								if (trace_enabled)
									printf("Data row %lu col %u: %hd\n", row, col+1, sht_val);
								gdkret = BUNappend(b, (void *) &sht_val, false);
								break;
							case SQL_INTEGER:
								if (trace_enabled)
									printf("Data row %lu col %u: %d\n", row, col+1, int_val);
								gdkret = BUNappend(b, (void *) &int_val, false);
								break;
							case SQL_BIGINT:
								if (trace_enabled)
									printf("Data row %lu col %u: %" PRId64 "\n", row, col+1, lng_val);
								gdkret = BUNappend(b, (void *) &lng_val, false);
								break;
							case SQL_DECIMAL:
							case SQL_NUMERIC:
							{
								int battype = colmetadata[col].battype;
								if (battype == TYPE_str) {
									if (strLen != SQL_NTS && strLen >= 0) {
										/* make sure it is a Nul Terminated String */
										if ((SQLULEN) strLen < largestStringSize) {
											if (str_val[strLen] != '\0')
												str_val[strLen] = '\0';
										} else {
											if (str_val[largestStringSize] != '\0')
												str_val[largestStringSize] = '\0';
										}
									}
									if (trace_enabled)
										printf("Data row %lu col %u: %s\n", row, col+1, str_val);
									gdkret = BUNappend(b, (void *) str_val, false);
								}
								break;
							}
							case SQL_REAL:
								if (trace_enabled)
									printf("Data row %lu col %u: %f\n", row, col+1, flt_val);
								gdkret = BUNappend(b, (void *) &flt_val, false);
								break;
							case SQL_DOUBLE:
								if (trace_enabled)
									printf("Data row %lu col %u: %f\n", row, col+1, dbl_val);
								gdkret = BUNappend(b, (void *) &dbl_val, false);
								break;
							case SQL_FLOAT:
								if (colmetadata[col].battype == TYPE_flt) {
									if (trace_enabled)
										printf("Data row %lu col %u: %f\n", row, col+1, flt_val);
									gdkret = BUNappend(b, (void *) &flt_val, false);
								} else {
									if (trace_enabled)
										printf("Data row %lu col %u: %f\n", row, col+1, dbl_val);
									gdkret = BUNappend(b, (void *) &dbl_val, false);
								}
								break;
							case SQL_TYPE_DATE:
							{
								date mdate_val = date_create(date_val.year, date_val.month, date_val.day);
								if (trace_enabled)
									printf("Data row %lu col %u: date(%04d-%02u-%02u)\n", row, col+1, date_val.year, date_val.month, date_val.day);
								gdkret = BUNappend(b, (void *) &mdate_val, false);
								break;
							}
							case SQL_TYPE_TIME:
							{
								daytime daytime_val = daytime_create(time_val.hour, time_val.minute, time_val.second, 0);
								if (trace_enabled)
									printf("Data row %lu col %u: daytime(%02u:%02u:%02u)\n", row, col+1, time_val.hour, time_val.minute, time_val.second);
								gdkret = BUNappend(b, (void *) &daytime_val, false);
								break;
							}
							case SQL_DATETIME:
							case SQL_TYPE_TIMESTAMP:
							{
								date mdate_val = date_create(ts_val.year, ts_val.month, ts_val.day);
								daytime daytime_val = daytime_create(ts_val.hour, ts_val.minute, ts_val.second, ts_val.fraction);
								timestamp timestamp_val = timestamp_create(mdate_val, daytime_val);
								if (trace_enabled)
									printf("Data row %lu col %u: timestamp(%04d-%02u-%02u %02u:%02u:%02u.%06u)\n", row, col+1,
										ts_val.year, ts_val.month, ts_val.day, ts_val.hour, ts_val.minute, ts_val.second, ts_val.fraction);
								gdkret = BUNappend(b, (void *) &timestamp_val, false);
								break;
							}
							case SQL_INTERVAL_YEAR:
							case SQL_INTERVAL_YEAR_TO_MONTH:
							case SQL_INTERVAL_MONTH:
							{
								switch (itv_val.interval_type) {
								case SQL_IS_YEAR:
									int_val = (int) itv_val.intval.year_month.year *12;
									break;
								case SQL_IS_YEAR_TO_MONTH:
									int_val = (int) (itv_val.intval.year_month.year *12)
										+ itv_val.intval.year_month.month;
									break;
								case SQL_IS_MONTH:
									int_val = (int) itv_val.intval.year_month.month;
									break;
								default:
									int_val = 0;
								}

								if (itv_val.interval_sign == SQL_TRUE)
									int_val = -int_val;
								if (trace_enabled)
									printf("Data row %lu col %u: %d\n", row, col+1, int_val);
								gdkret = BUNappend(b, (void *) &int_val, false);
								break;
							}
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
							{
								switch (itv_val.interval_type) {
								case SQL_IS_DAY:
									lng_val = (lng) itv_val.intval.day_second.day * (24*60*60*1000);
									break;
								case SQL_IS_HOUR:
									lng_val = (lng) itv_val.intval.day_second.hour * (60*60*1000);
									break;
								case SQL_IS_MINUTE:
									lng_val = (lng) itv_val.intval.day_second.minute * (60*1000);
									break;
								case SQL_IS_SECOND:
									lng_val = (lng) (itv_val.intval.day_second.second * 1000)
										+ (itv_val.intval.day_second.fraction / 1000);
									break;
								case SQL_IS_DAY_TO_HOUR:
									lng_val = (lng) ((itv_val.intval.day_second.day *24)
										+ itv_val.intval.day_second.hour) * (60*60*1000);
									break;
								case SQL_IS_DAY_TO_MINUTE:
									lng_val = (lng) ((((itv_val.intval.day_second.day *24)
										+ itv_val.intval.day_second.hour) *60)
										+ itv_val.intval.day_second.minute) * (60*1000);
									break;
								case SQL_IS_DAY_TO_SECOND:
									lng_val = (lng) (((((((itv_val.intval.day_second.day *24)
										+ itv_val.intval.day_second.hour) *60)
										+ itv_val.intval.day_second.minute) *60)
										+ itv_val.intval.day_second.second) *1000)
										+ (itv_val.intval.day_second.fraction / 1000);
									break;
								case SQL_IS_HOUR_TO_MINUTE:
									lng_val = (lng) ((itv_val.intval.day_second.hour *60)
										+ itv_val.intval.day_second.minute) * (60*1000);
									break;
								case SQL_IS_HOUR_TO_SECOND:
									lng_val = (lng) (((((itv_val.intval.day_second.hour *60)
										+ itv_val.intval.day_second.minute) *60)
										+ itv_val.intval.day_second.second) *1000)
										+ (itv_val.intval.day_second.fraction / 1000);
									break;
								case SQL_IS_MINUTE_TO_SECOND:
									lng_val = (lng) (((itv_val.intval.day_second.minute *60)
										+ itv_val.intval.day_second.second) *1000)
										+ (itv_val.intval.day_second.fraction / 1000);
									break;
								default:
									lng_val = 0;
								}

								if (itv_val.interval_sign == SQL_TRUE)
									lng_val = -lng_val;
								if (trace_enabled)
									printf("Data row %lu col %u: %" PRId64 "\n", row, col+1, lng_val);
								gdkret = BUNappend(b, (void *) &lng_val, false);
								break;
							}
							case SQL_GUID:
								if (trace_enabled)
									printf("Data row %lu col %u: %08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x\n", row, col+1,
										guid_val.Data1, guid_val.Data2, guid_val.Data3, guid_val.Data4[0], guid_val.Data4[1], guid_val.Data4[2],
										guid_val.Data4[3], guid_val.Data4[4], guid_val.Data4[5], guid_val.Data4[6], guid_val.Data4[7]);
								// uuid is 16 bytes, same as SQLGUID guid_val
								memcpy((void *) &uuid_val, (void *) &guid_val, sizeof(uuid));
								gdkret = BUNappend(b, (void *) &uuid_val, false);
								break;
							case SQL_BINARY:
							case SQL_VARBINARY:
							case SQL_LONGVARBINARY:
							case TYPE_blob:
							{
								//TODO convert blob_val to blob struct which starts with length (4 bytes) and next data bytes.
								if (trace_enabled)
									printf("Data row %lu col %u: blob_val\n", row, col+1);
								// TODO gdkret = BUNappend(b, (void *) blob_val, false);
								/* for now append NULL value, as all bats need to be the correct length */
								gdkret = BUNappend(b, ATOMnilptr(b->ttype), false);
								break;
							}
						}
						if (gdkret != GDK_SUCCEED)
							if (trace_enabled)
								printf("BUNappend(b, val, false) failed!\n");
					}
				}
			}
			ret = SQLFetch(stmt);	// get data of next row
		}
		/* the last SQLFetch() will return SQL_NO_DATA at end, treat it as success */
		if (ret == SQL_NO_DATA)
			ret = SQL_SUCCESS;	// we retrieved all rows

  finish_fetch:
		if (str_val)
			GDKfree(str_val);
		if (blob_val)
			GDKfree(blob_val);

		/* pass bats to caller */
		if (colmetadata) {
			for (int col = 0; col < (int) nr_cols; col++) {
				bat * rescol = getArgReference_bat(stk, pci, col);
				BAT * b = colmetadata[col].bat;
				if (rescol && b) {
					*rescol = b->batCacheid;
					BBPkeepref(b);
				}
			}
			/* free locally allocated memory */
			GDKfree(colmetadata);
		}
	} /* end of: if (caller == ODBC_LOADER) */

  finish:
	if (query)
		GDKfree(query);
	if (odbc_con_str)
		GDKfree(odbc_con_str);

	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		/* an ODBC function call returned an error or warning, get the error msg from the ODBC driver */
		SQLSMALLINT handleType;
		SQLHANDLE handle;
		str retmsg;
		char * ODBCmsg;

		/* get err message(s) from the right handle */
		if (stmt != SQL_NULL_HSTMT) {
			handleType = SQL_HANDLE_STMT;
			handle = stmt;
		} else
		if (dbc != SQL_NULL_HDBC) {
			handleType = SQL_HANDLE_DBC;
			handle = dbc;
		} else {
			handleType = SQL_HANDLE_ENV;
			handle = env;
		}
		ODBCmsg = getErrMsg(handleType, handle);
		if (errmsg != NULL) {
			retmsg = sa_message(sql->sa, "odbc_loader" " %s %s", errmsg, (ODBCmsg) ? ODBCmsg : "");
		} else {
			retmsg = sa_message(sql->sa, "odbc_loader" " %s", (ODBCmsg) ? ODBCmsg : "");
		}
		if (ODBCmsg)
			GDKfree(ODBCmsg);
		odbc_cleanup(env, dbc, stmt);
		return retmsg;
	}
	odbc_cleanup(env, dbc, stmt);
	return (errmsg != NULL) ? (str)errmsg : MAL_SUCCEED;
}

/*
 * returns an error string (static or via tmp sa_allocator allocated), NULL on success
 *
 * Extend the subfunc f with result columns, ie.
	f->res = typelist;
	f->coltypes = typelist;
	f->colnames = nameslist; use tname if passed, for the relation name
 * Fill the list res_exps, with one result expressions per resulting column.
 */
static str
odbc_relation(mvc *sql, sql_subfunc *f, char *url, list *res_exps, char *aname)
{
	(void) aname;
	return odbc_query(ODBC_RELATION, sql, f, url, res_exps, NULL, NULL, NULL);
}

static void *
odbc_load(void *BE, sql_subfunc *f, char *url, sql_exp *topn)
{
	backend *be = (backend*)BE;
	if (!f)
		return NULL;

	(void)topn;

	InstrPtr q = newStmtArgs(be->mb, "odbc", "loader", list_length(f->coltypes) + 2);
	int col = 0;
	list *l = sa_list(be->mvc->sa);
	for (node *n = f->coltypes->h, *nn = f->colnames->h; n && nn; col++, n = n->next, nn = nn->next) {
		const char *name = nn->data;
		sql_subtype *tp = n->data;
		int type = newBatType(tp->type->localtype);
		if (col)
			q = pushReturn(be->mb, q, newTmpVariable(be->mb, type));
		else
			getArg(q, 0) = newTmpVariable(be->mb, type);
		stmt *s = stmt_blackbox_result(be, q, col, tp);
		s = stmt_alias(be, s, col+1, f->tname, name);
		list_append(l, s);
	}
	q = pushStr(be->mb, q, url);
	q = pushPtr(be->mb, q, f);
	pushInstruction(be->mb, q);
	return stmt_list(be, l);
}

static str
ODBCloader(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	backend *be = NULL;
	str msg;
	if ((msg = getBackendContext(cntxt, &be)) != NULL)
		return msg;
	str uri = *getArgReference_str(stk, pci, pci->retc);
	sql_subfunc *f = *(sql_subfunc**)getArgReference_ptr(stk, pci, pci->retc+1);

	return odbc_query(ODBC_LOADER, be->mvc, f, uri, NULL, mb, stk, pci);
	//return MAL_SUCCEED;
}

static str
ODBCprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;
	pl_register("odbc", &odbc_relation, &odbc_load);
	return MAL_SUCCEED;
}

static str
ODBCepilogue(void *ret)
{
	(void)ret;
	pl_unregister("odbc");
	return MAL_SUCCEED;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func odbc_init_funcs[] = {
	pattern("odbc", "prelude", ODBCprelude, false, "", noargs),
	command("odbc", "epilogue", ODBCepilogue, false, "", noargs),
	pattern("odbc", "loader", ODBCloader, true, "Import a query result via the odbc uri", args(1,3, batvarargany("",0),arg("uri",str),arg("func",ptr))),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_odbc_mal)
{ mal_module("odbc", NULL, odbc_init_funcs); }
