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
#include "mal_builder.h"
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

/* map ODBC SQL datatype to MonetDB SQL datatype */
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
/*	case SQL_HUGEINT:	return "HUGEINT";	0x4000 (defined in ODBCGlobal.h) */
	default:		return "Driver specific type";
	}
}

/* utility function to nicely close all opened ODBC resources */
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

typedef struct odbc_loader_t {
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	SQLSMALLINT nr_cols;
} odbc_loader_t;


/*
 * odbc_query() contains the logic for both odbc_relation() and ODBCloader()
 * the caller arg is 1 when called from odbc_relation and 2 when called from ODBCloader
 */
static str
odbc_query(mvc *sql, sql_subfunc *f, char *url, list *res_exps, int caller)
{
	bool trace_enabled = false;	/* used for development only */

	/* check received url and extract the ODBC connection string and the SQL query */
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
		errmsg = "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3) failed.";
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
		errmsg = "SQLSetConnectAttr (SQL_ATTR_LOGIN_TIMEOUT 8s) failed.";
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
			ret = SQLDescribeCol(stmt, col, (SQLCHAR *) cname, (SQLSMALLINT) sizeof(cname),
					NULL, &dataType, &columnSize, &decimalDigits, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				errmsg = "SQLDescribeCol failed.";
				goto finish;
			}
			if (trace_enabled)
				printf("ResCol %d, name: %s, type %d (%s), size %d, decdigits %d\n",
					col, cname, (int)dataType, nameofSQLtype(dataType), (int)columnSize, (int)decimalDigits);
			colname = sa_strdup(sql->sa, cname);
			list_append(nameslist, colname);
			sql_mtype = map_rescol_type(dataType, columnSize, decimalDigits, sql);
			list_append(typelist, sql_mtype);

			/* also get the table name for this result column */
			ret = SQLColAttribute(stmt, col, SQL_DESC_TABLE_NAME, (SQLPOINTER) tname, (SQLSMALLINT) sizeof(tname), NULL, NULL);
			if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
				strcpy(tname, "");
			}
			tblname = sa_strdup(sql->sa, tname);
			sql_exp *ne = exp_column(sql->sa, tblname, colname, sql_mtype, CARD_MULTI, 1, 0, 0);
			set_basecol(ne);
			ne->alias.label = -(sql->nid++);
			list_append(res_exps, ne);
		}

		f->tname = sa_strdup(sql->sa, tname);
		f->colnames = nameslist;
		f->coltypes = typelist;
		f->res = typelist;

		odbc_loader_t *r = (odbc_loader_t *)sa_alloc(sql->sa, sizeof(odbc_loader_t));
		r->env = env;
		r->dbc = dbc;
		r->stmt = stmt;
		r->nr_cols = nr_cols;
		f->sname = (char *)r; /* pass odbc_loader */

		goto finish;
	}

	/* when called from odbc_load() */
	if (caller == 2) {
		sql_table *t;

		if (trace_enabled)
			printf("Before mvc_create_table(%s)\n", f->tname);
		// create an internal transient table to store fetched data
		if (mvc_create_table(&t, sql, sql->session->tr->tmp /* misuse tmp schema */,
				f->tname /*gettable name*/, tt_table, false, SQL_DECLARED_TABLE, 0, 0, false) != LOG_OK)
			/* alloc error */
			return NULL;
		if (trace_enabled)
			printf("After mvc_create_table()\n");

		node *n, *nn = f->colnames->h, *tn = f->coltypes->h;
		int col = 1;
		for (n = f->res->h; n && col <= nr_cols; col++, n = n->next, nn = nn->next, tn = tn->next) {
			const char *name = nn->data;
			sql_subtype *tp = tn->data;
			sql_column *c = NULL;

			if (trace_enabled)
				printf("%d Before mvc_create_column(%s)\n", col, name);
			if (!tp || mvc_create_column(&c, sql, t, name, tp) != LOG_OK) {
				return NULL;
			}
			if (trace_enabled)
				printf("After mvc_create_column()\n");
		}

		// repeat fetching data, adding data work table
		long rows = 0;
		ret = SQLFetch(stmt);
		while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			SQLLEN buflen = 65535;
			char buf[65536];
			SQLLEN strLen;

			rows++;
			if (trace_enabled)
				printf("Fetched row %ld\n", rows);

			// TODO for each result column append to created transient table
			for (SQLUSMALLINT col = 1; col <= (SQLUSMALLINT) nr_cols; col++) {
				buf[buflen] = '\0';
				strLen = SQL_NTS;

				ret = SQLGetData(stmt, col, SQL_C_CHAR, (SQLPOINTER *) buf, buflen, &strLen);
				if (buf[buflen] != '\0')
					buf[buflen] = '\0';
				if (strLen != SQL_NTS && strLen > 0) {
					if (strLen < buflen)
						if (buf[strLen] != '\0')
							buf[strLen] = '\0';
				}
				if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
					if (trace_enabled)
						printf("Failed to get data for col %d of row %ld\n", col, rows);
				} else {
					// TODO copy buffer value to BUN and append
					if (trace_enabled)
						printf("Got data for col %d of row %ld: %s\n", col, rows, buf);
				}
			}
			ret = SQLFetch(stmt);	// get data of next row
		}
		/* the last SQLFetch() should have returned SQL_NO_DATA */
		if (ret == SQL_NO_DATA)
			ret = SQL_SUCCESS;	// we retrieved all rows
	}

  finish:
  	if (query)
  		GDKfree(query);
  	if (odbc_con_str)
  		GDKfree(odbc_con_str);

	if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
		SQLSMALLINT handleType;
		SQLHANDLE handle;
		SQLCHAR state[SQL_SQLSTATE_SIZE +1];
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
		ret = SQLGetDiagRec(handleType, handle, 1, state, &errnr, msg, (sizeof(msg) -1), &msglen);
		if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			str retmsg;
			if (state[SQL_SQLSTATE_SIZE] != '\0')
				state[SQL_SQLSTATE_SIZE] = '\0';
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
	if (errmsg != NULL)
		return (str)errmsg;
	else
		return MAL_SUCCEED;
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
	return odbc_query(sql, f, url, res_exps, 1);
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
	(void)mb;
	str uri = *getArgReference_str(stk, pci, pci->retc);
	sql_subfunc *f = *(sql_subfunc**)getArgReference_ptr(stk, pci, pci->retc+1);

	return odbc_query(be->mvc, f, uri, NULL, 2);
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
	pattern("odbc", "loader", ODBCloader, true, "Import a table via the odbc uri", args(1,3, batvarargany("",0),arg("uri",str),arg("func",ptr))),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_odbc_mal)
{ mal_module("odbc", NULL, odbc_init_funcs); }

