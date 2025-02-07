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
#include "rel_proto_loader.h"
#include "rel_exp.h"

#include "mal_exception.h"
#include "mal_client.h"

//#ifdef _MSC_VER
//#include <WTypes.h>
//#endif
//#include <stdint.h>
//#include <ctype.h>
//#include <wchar.h>

/**** Define the ODBC Version our ODBC application complies with ****/
#define ODBCVER 0x0352		/* Important: this must be defined before include of sql.h and sqlext.h */
#include <sql.h>
#include <sqlext.h>

typedef struct odbc_loader_t {
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	SQLSMALLINT nr_cols;
} odbc_loader_t;

static void
odbc_cleanup(SQLHANDLE env, SQLHANDLE dbc, SQLHANDLE stmt) {
	if (stmt != SQL_NULL_HSTMT) {
		SQLFreeStmt(stmt, SQL_CLOSE);
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	}
	if (dbc != SQL_NULL_HDBC) {
		SQLDisconnect(dbc);
		SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	}
	if (env != SQL_NULL_HENV) {
		SQLFreeHandle(SQL_HANDLE_ENV, env);
	}
}

static sql_subtype *
map_rescol_type(SQLSMALLINT dataType, SQLULEN columnSize, SQLSMALLINT decimalDigits, mvc * sql)
{
	char * typenm;
	int interval_type = 0;

	switch (dataType) {
	case SQL_CHAR:
	case SQL_VARCHAR:
	case SQL_LONGVARCHAR:
	case SQL_WCHAR:
	case SQL_WVARCHAR:
	case SQL_WLONGVARCHAR:
	default:	/* all other ODBC types are also mapped to varchar for now */
		/* all ODBC char datatypes are mapped to varchar. char and clob are internally not used anymore */
		return sql_bind_subtype(sql->sa, "varchar", (int) columnSize, 0);

	case SQL_BINARY:
	case SQL_VARBINARY:
	case SQL_LONGVARBINARY:
		return sql_bind_subtype(sql->sa, "blob", (int) columnSize, 0);

	case SQL_DECIMAL:
	case SQL_NUMERIC:
		/* columnSize contains the defined number of digits, so precision. */
		/* decimalDigits contains the scale (which can be negative). */
		return sql_bind_subtype(sql->sa, "decimal", (int) columnSize, (int) decimalDigits);

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

	case SQL_REAL:
		typenm = "real";
		break;
	case SQL_DOUBLE:
		typenm = "double";
		break;
	case SQL_FLOAT:
		/* the precision of SQL_FLOAT can be either 24 or 53: if it is 24, the SQL_FLOAT data type is the same as SQL_REAL; if it is 53, the SQL_FLOAT data type is the same as SQL_DOUBLE. */
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

	case SQL_INTERVAL_MONTH:
		typenm = "month_interval";
		interval_type = 3;
		break;
	case SQL_INTERVAL_YEAR:
		typenm = "month_interval";
		interval_type = 1;
		break;
	case SQL_INTERVAL_YEAR_TO_MONTH:
		typenm = "month_interval";
		interval_type = 2;
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
/*	case SQL_HUGEINT:	return "HUGEINT";	0x4000 (defined in ODBCGlobal.h) */
	default:		return "Driver specific type";
	}
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
odbc_query(mvc *sql, sql_subfunc *f, char *url, list *res_exps, sql_exp *topn, int caller)
{
	(void) topn;
	bool trace_enabled = true;	/* used for development only */

	/* check received url and extract the ODBC connection string and yhe SQL query */
	if (!url || (url && strncasecmp("odbc:", url, 5) != 0))
		return "Invalid URI. Must start with 'odbc:'.";

	// skip 'odbc:' prefix from url so we get a connection string including the query
	char * con_str = &url[5];
	/* the connection string must start with 'DSN=' or 'DRIVER=' or 'FILEDSN='
	   else the ODBC driver manager can't load the ODBC driver */
	if (con_str
	  && (strncmp("DSN=", con_str, 4) != 0)
	  && (strncmp("DRIVER=", con_str, 7) != 0)
	  && (strncmp("FILEDSN=", con_str, 8) != 0))
		return "Invalid ODBC connection string. Should start with 'DSN=' or 'DRIVER=' or 'FILEDSN='.";

	// locate the 'QUERY=' part to extract the SQL query string to execute
	char * qry_str = strstr(con_str, "QUERY=");
	if (qry_str == NULL)
		return "Incomplete ODBC connection string. Missing 'QUERY=' part (to specify the SQL SELECT query to execute).";

	char * query = GDKstrdup(&qry_str[6]);	// we expect that QUERY= is at the end of the connection string

	// create a new ODBC connection string without the QUERY= part
	char * odbc_con_str = GDKstrndup(con_str, qry_str - con_str);

	if (trace_enabled)
		printf("\nExtracted ODBC connection string: %s\nand SQL query: %s\n", odbc_con_str, query);


	/* now we can try to connect to the ODBC driver and execute the SQL query */
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
		errmsg = "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3) failed.";
		goto finish;
	}
	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC DBC handle failed.";
		goto finish;
	}

	SQLSMALLINT len = 0;
	ret = SQLDriverConnect(dbc, NULL, (SQLCHAR *) odbc_con_str, SQL_NTS, NULL, 0, &len, SQL_DRIVER_NOPROMPT);
	if (trace_enabled)
		printf("After SQLDriverConnect(%s) returned %d\n", odbc_con_str, ret);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Could not connect. SQLDriverConnect failed.";
		goto finish;
	}

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "Allocate ODBC STMT handle failed.";
		goto finish;
	}

	ret = SQLExecDirect(stmt, (SQLCHAR *) query, SQL_NTS);
	if (trace_enabled)
		printf("After SQLExecDirect(%s) returned %d\n", query, ret);
	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		errmsg = "SQLExecDirect query failed.";
		goto finish;
	}

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

	/* when called from odbc_relation() */
	if (caller == 1) {
		char tname[1024];
		char cname[1024];
		SQLSMALLINT dataType = 0;
		SQLULEN columnSize = 0;
		SQLSMALLINT decimalDigits = 0;
		sql_subtype * sql_mtype;
		list * typelist = sa_list(sql->sa);
		list * nameslist = sa_list(sql->sa);
		for (SQLUSMALLINT col = 1; col <= (SQLUSMALLINT) nr_cols; col++) {
			/* for each result column get name, datatype, size and decdigits */
			ret = SQLDescribeCol(stmt, col, (SQLCHAR *) cname, (SQLSMALLINT) sizeof(cname),
				NULL, &dataType, &columnSize, &decimalDigits, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				errmsg = "SQLDescribeCol failed.";
				goto finish;
			}
			if (trace_enabled)
				printf("ResCol %d, name: %s, type %d (%s), size %d, decdigits %d\n",
					col, cname, (int)dataType, nameofSQLtype(dataType), (int)columnSize, (int)decimalDigits);
			list_append(nameslist, cname);
			sql_mtype = map_rescol_type(dataType, columnSize, decimalDigits, sql);
			list_append(typelist, sql_mtype);

			/* also get the table name for this result column */
			ret = SQLColAttribute(stmt, col, SQL_DESC_TABLE_NAME, (SQLPOINTER) tname, (SQLSMALLINT) sizeof(tname), NULL, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				strcpy(tname, "");
			}
			sql_exp *ne = exp_column(sql->sa, tname, cname, sql_mtype, CARD_MULTI, 1, 0, 0);
			set_basecol(ne);
			ne->alias.label = -(sql->nid++);
			list_append(res_exps, ne);
		}

		/* f->tname = sa_strdup(sql->sa, aname); */
		f->res = typelist;
		f->coltypes = typelist;
		f->colnames = nameslist;

		odbc_loader_t *r = (odbc_loader_t *)sa_alloc(sql->sa, sizeof(odbc_loader_t));
		r->env = env;
		r->dbc = dbc;
		r->stmt = stmt;
		r->nr_cols = nr_cols;
		f->sname = (char*)r; /* pass odbc_loader */

		goto finish;
	}

	/* when called from odbc_load() we can now fetch the data */
	if (caller == 2 && stmt != SQL_NULL_HSTMT) {
		// TODO create an internal transient table to store fetched data
		// if (mvc_create_table(&t, be->mvc, be->mvc->session->tr->tmp /* misuse tmp schema */, r->tname /*gettable name*/, tt_remote, false, SQL_DECLARED_TABLE, 0, 0, false) != LOG_OK)

		for (SQLUSMALLINT col = 1; col <= (SQLUSMALLINT) nr_cols; col++) {
			// TODO for each result column create a buffer and bind it. Also create a BAT column.
			// ret = SQLBindCol(stmt, 1, );
			// if (!tp || mvc_create_column(&c, be->mvc, t, name, tp) != LOG_OK) {
		}

		// repeat fetching data, adding data work table
		ret = SQLFetch(stmt);
		while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			// TODO for each result column append to created transient table
			for (SQLUSMALLINT col = 1; col <= (SQLUSMALLINT) nr_cols; col++) {
				// ret = SQLGetData(stmt, col, ...);
				// copy buffer value to BUN and append
			}
			ret = SQLFetch(stmt);	// get data of next row
		}
	}

  finish:
  	if (query)
  		GDKfree(query);
  	if (odbc_con_str)
  		GDKfree(odbc_con_str);

	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		SQLSMALLINT handleType;
		SQLHANDLE handle;
		SQLCHAR state[6];
		SQLINTEGER errnr;
		SQLCHAR msg[2048];
		SQLSMALLINT msglen;

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
		ret = SQLGetDiagRec(handleType, handle, 1, state, &errnr, msg, sizeof(msg), &msglen);
		if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			str retmsg;
			if (errmsg != NULL) {
				retmsg = sa_message(sql->sa, "odbc_loader" " %s SQLstate %s, Errnr %d, Message %s", errmsg, (char*)state, (int)errnr, (char*)msg);
			} else {
				retmsg = sa_message(sql->sa, "odbc_loader" " SQLstate %s, Errnr %d, Message %s", (char*)state, (int)errnr, (char*)msg);
			}
			odbc_cleanup(env, dbc, stmt);
			return retmsg;
		}
	}
	odbc_cleanup(env, dbc, stmt);
	return (str)errmsg;
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
	return odbc_query(sql, f, url, res_exps, NULL, 1);
}

static void *
odbc_load(void *BE, sql_subfunc *f, char *url, sql_exp *topn)
{
	backend *be = (backend*)BE;
	mvc *sql = be->mvc;
	return odbc_query(sql, f, url, NULL, topn, 2);
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
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_odbc_mal)
{ mal_module("odbc", NULL, odbc_init_funcs); }

