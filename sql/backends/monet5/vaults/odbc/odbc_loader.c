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
	char *url;
	SQLCHAR *query;
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
	(void) res_exps;
	(void) aname;
//	list *typelist = sa_list(sql->sa);
//	list *nameslist = sa_list(sql->sa);

	// TODO validate the url, should/could start with 'odbc:', if so
	// remove 'odbc:' prefix from url so we get an ODBC connection string

	SQLCHAR * con_str = (SQLCHAR *) url;
	// TODO validate the ODBC connection string, should start with 'DSN=' or 'DRIVER='

	// TODO get the SQL query string. Not from the url but from an additional provided parameter
	// for test we use a static query:
	SQLCHAR * query = (SQLCHAR *) "SELECT * FROM INFORMATION_SCHEMA.TABLES";

	SQLHANDLE env = SQL_NULL_HENV;
	SQLHANDLE dbc = SQL_NULL_HDBC;
	SQLHANDLE stmt = SQL_NULL_HSTMT;
	SQLRETURN ret;
	SQLSMALLINT nr_cols = 0;
	char * errmsg;

	ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
	if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
		ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) (uintptr_t) SQL_OV_ODBC3, 0);
		if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
			if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
				SQLSMALLINT len = 0;
				ret = SQLDriverConnect(dbc, NULL, con_str, SQL_NTS, NULL, 0, &len, SQL_DRIVER_NOPROMPT);
				if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
					ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
					if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
						ret = SQLExecDirect(stmt, query, SQL_NTS);
						if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
							ret = SQLNumResultCols(stmt, &nr_cols);
							// TODO for each column get the name, type, size/digits and scale
								// list_append(nameslist, name);
								// list_append(typelist, type);
						} else {
							errmsg = "ODBC SQLExecDirect query failed.\n";
							goto failure;
						}
					} else {
						errmsg = "Allocate ODBC STMT handle failed.\n";
						goto failure;
					}
				} else {
					errmsg = "ODBC SQLDriverConnect failed.\n";
					goto failure;
				}
			} else {
				errmsg = "Allocate ODBC DBC handle failed.\n";
				goto failure;
			}
		} else {
			errmsg = "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3) failed.\n";
			goto failure;
		}
	} else {
		errmsg = "Allocate ODBC environment handle failed.\n";
		goto failure;
	}

	odbc_loader_t *r = (odbc_loader_t *)sa_alloc(sql->sa, sizeof(odbc_loader_t));
	r->url = url;
	r->query = query;
	r->env = env;
	r->dbc = dbc;
	r->stmt = stmt;
	r->nr_cols = nr_cols;
	f->sname = (char*)r; /* pass odbc_loader */
	return NULL;
  failure:
	// TODO get DiagRecMsg to get driver err message and sqlstate
	odbc_cleanup(env, dbc, stmt);
	return errmsg;
}

static void *
odbc_load(void *BE, sql_subfunc *f, char *url, sql_exp *topn)
{
	(void) url;
	(void) topn;
	(void) BE;
//	backend *be = (backend*)BE;
//	mvc *sql = be->mvc;
	odbc_loader_t *r = (odbc_loader_t*)f->sname;
	SQLHANDLE stmt = r->stmt;
	SQLSMALLINT nr_cols = r->nr_cols;

	if (stmt != SQL_NULL_HSTMT) {
		SQLRETURN ret;
		// TODO create an internal transient table to store fetched data
		// if (mvc_create_table(&t, be->mvc, be->mvc->session->tr->tmp /* misuse tmp schema */, r->tname /*gettable name*/, tt_remote, false, SQL_DECLARED_TABLE, 0, 0, false) != LOG_OK)

		for (SQLSMALLINT i = 1; i <= nr_cols; i++) {
			// TODO for each result column create a buffer and bind it. Also create a BAT column.
			// ret = SQLBindCol(stmt, 1, );
			// if (!tp || mvc_create_column(&c, be->mvc, t, name, tp) != LOG_OK) {
		}

		// repeat fetching data, adding data work table
		ret = SQLFetch(stmt);
		while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
			// TODO for each result column append to created transient table
			for (SQLSMALLINT i = 1; i <= nr_cols; i++) {
				// copy buffer value to BUN and append
			}
			ret = SQLFetch(stmt);	// get data of next row
		}
	}

	// finally cleanup
	odbc_cleanup(r->env, r->dbc, stmt);
	return NULL;
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

