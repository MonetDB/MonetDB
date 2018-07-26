/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifdef _MSC_VER
#include <WTypes.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sql.h>
#include <sqlext.h>

#ifdef _MSC_VER
#define snprintf _snprintf
#endif

static void
prerr(SQLSMALLINT tpe, SQLHANDLE hnd, const char *func, const char *pref)
{
	SQLCHAR state[6];
	SQLINTEGER errnr;
	SQLCHAR msg[256];
	SQLSMALLINT msglen;
	SQLRETURN ret;

	ret = SQLGetDiagRec(tpe, hnd, 1, state, &errnr, msg, sizeof(msg), &msglen);
	switch (ret) {
	case SQL_SUCCESS_WITH_INFO:
		if (msglen >= (signed int) sizeof(msg))
			fprintf(stderr, "(message truncated)\n");
		/* fall through */
	case SQL_SUCCESS:
		fprintf(stderr, "%s: %s: SQLstate %s, Errnr %d, Message %s\n", func, pref, (char*)state, (int)errnr, (char*)msg);
		break;
	case SQL_INVALID_HANDLE:
		fprintf(stderr, "%s: %s, invalid handle passed to error function\n", func, pref);
		break;
	case SQL_ERROR:
		fprintf(stderr, "%s: %s, unexpected error from SQLGetDiagRec\n", func, pref);
		break;
	case SQL_NO_DATA:
		break;
	default:
		fprintf(stderr, "%s: %s, weird return value from SQLGetDiagRec\n", func, pref);
		break;
	}
}

static void
check(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func)
{
	switch (ret) {
	case SQL_SUCCESS:
		break;
	case SQL_SUCCESS_WITH_INFO:
		prerr(tpe, hnd, func, "Info");
		break;
	case SQL_ERROR:
		prerr(tpe, hnd, func, "Error");
		break;
	case SQL_INVALID_HANDLE:
		fprintf(stderr, "%s: Error: invalid handle\n", func);
		exit(1);
	default:
		fprintf(stderr, "%s: Unexpected return value\n", func);
		break;
	}
}

static void
GetSetReGetStmtAttr(SQLHANDLE stmt, SQLINTEGER attribute, const char * attr_name, SQLULEN value)
{
	SQLRETURN ret;
	SQLULEN ul;
	SQLINTEGER resultlen;

	// first get the actual value from the server
	ret = SQLGetStmtAttr(stmt, attribute, &ul, sizeof(ul), &resultlen);
	fprintf(stderr, "Get %s: %lu\n", attr_name, (long unsigned int) ul);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLGetStmtAttr");

	// next change the value on the server
	ret = SQLSetStmtAttr(stmt, attribute, &value, 0);
	fprintf(stderr, "Set %s: %lu\n", attr_name, (long unsigned int) value);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");

	// next re-get the value from the server, should be the same as the set value
	ul = 123456789;
	ret = SQLGetStmtAttr(stmt, attribute, &ul, sizeof(ul), &resultlen);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLGetStmtAttr");
	fprintf(stderr, "Get changed %s: %lu", attr_name, (long unsigned int) ul);
	if (ul != value)
		fprintf(stderr, " which is different from %lu !!", (long unsigned int) value);
	fprintf(stderr, "\n\n");
}

int
main(int argc, char **argv)
{
	SQLRETURN ret;
	SQLHANDLE env;
	SQLHANDLE dbc;
	SQLHANDLE stmt;
	char *dsn = "MonetDB";
	char *user = "monetdb";
	char *pass = "monetdb";

	if (argc > 1)
		dsn = argv[1];
	if (argc > 2)
		user = argv[2];
	if (argc > 3)
		pass = argv[3];
	if (argc > 4 || *dsn == '-') {
		fprintf(stderr, "Usage: %s [datasource [user [password]]]\n", argv[0]);
		exit(1);
	}

	ret = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env);
	if (ret != SQL_SUCCESS) {
		fprintf(stderr, "Cannot allocate ODBC environment handle!\n");
		exit(1);
	}

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) (uintptr_t) SQL_OV_ODBC3, 0);
	check(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	check(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");

	ret = SQLConnect(dbc, (SQLCHAR *) dsn, SQL_NTS, (SQLCHAR *) user, SQL_NTS, (SQLCHAR *) pass, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLConnect");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle (STMT)");

	/* run actual tests */
	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, "SQL_ATTR_QUERY_TIMEOUT", -1);	/* test also what happens with a negative value */
	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, "SQL_ATTR_QUERY_TIMEOUT", 0);
	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, "SQL_ATTR_QUERY_TIMEOUT", 3600);
	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, "SQL_ATTR_QUERY_TIMEOUT", 2147483647);

	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, "SQL_ATTR_MAX_LENGTH", -2);	/* test also what happens with a negative value */
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, "SQL_ATTR_MAX_LENGTH", 0);
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, "SQL_ATTR_MAX_LENGTH", 65535);
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, "SQL_ATTR_MAX_LENGTH", 2147483647);

	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, "SQL_ATTR_MAX_ROWS", -3);	/* test also what happens with a negative value */
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, "SQL_ATTR_MAX_ROWS", 0);
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, "SQL_ATTR_MAX_ROWS", 100000);
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, "SQL_ATTR_MAX_ROWS", 2147483647);

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle (STMT)");

	ret = SQLDisconnect(dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLFreeHandle (DBC)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	check(ret, SQL_HANDLE_ENV, env, "SQLFreeHandle (ENV)");

	return 0;
}
