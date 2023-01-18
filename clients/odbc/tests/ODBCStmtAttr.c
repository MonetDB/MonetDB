/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifdef _MSC_VER
#include <WTypes.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

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
		if ((strcmp(func,"SQLSetStmtAttr") != 0)
		 || (strcmp(pref,"Info") != 0)
		 || (strcmp((char*)state,"01S02") != 0)
		 || errnr != 0
		 || (strncmp((char*)msg,"[MonetDB][ODBC Driver 11.", 25) != 0))
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
	case SQL_NO_DATA:
		break;
	case SQL_INVALID_HANDLE:
		fprintf(stderr, "%s: Error: invalid handle\n", func);
		break;
	default:
		fprintf(stderr, "%s: Unexpected return value\n", func);
		break;
	}
}

static const char *
StmtAttribute2name(SQLINTEGER attribute)
{
	switch (attribute) {
	case SQL_ATTR_MAX_LENGTH:
		return "SQL_ATTR_MAX_LENGTH";
	case SQL_ATTR_MAX_ROWS:
		return "SQL_ATTR_MAX_ROWS";
	case SQL_ATTR_QUERY_TIMEOUT:
		return "SQL_ATTR_QUERY_TIMEOUT";
	default:
		fprintf(stderr, "StmtAttribute2name: Unexpected value %ld\n", (long) attribute);
		return "NOT YET IMPLEMENTED";
	}
}

static void
GetSetReGetStmtAttr(SQLHANDLE stmt, SQLINTEGER attribute, SQLULEN value, const char * expected)
{
	SQLRETURN ret;
	SQLULEN ul;
	SQLINTEGER resultlen;
	size_t expct_len = strlen(expected);
	size_t outp_len = expct_len + 1000;
	char * outp = malloc(outp_len);
	size_t pos = 0;
	const char * attr_name = StmtAttribute2name(attribute);

	// first get the actual value from the server
	ret = SQLGetStmtAttr(stmt, attribute, &ul, sizeof(ul), &resultlen);
	pos += snprintf(outp + pos, outp_len - pos, "Get %s: %lu\n", attr_name, (long unsigned int) ul);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLGetStmtAttr");

	// next change the value on the server
	ret = SQLSetStmtAttr(stmt, attribute, &value, 0);
	pos += snprintf(outp + pos, outp_len - pos, "Set %s: %lu\n", attr_name, (long unsigned int) value);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLSetStmtAttr");	// this produces: SQLSetStmtAttr: Info: SQLstate 01S02, Errnr 0, Message [MonetDB][ODBC Driver 11.44.0]Option value changed

	// next re-get the value from the server, should be the same as the set value
	ul = 123456789;
	ret = SQLGetStmtAttr(stmt, attribute, &ul, sizeof(ul), &resultlen);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLGetStmtAttr");
	pos += snprintf(outp + pos, outp_len - pos, "Get %s: %lu\n", attr_name, (long unsigned int) ul);
	if (ul != value)
		pos += snprintf(outp + pos, outp_len - pos, " which is different from %lu !!\n", (long unsigned int) value);

	if (strcmp(expected, outp) != 0) {
		fprintf(stderr, "Testing %s\nExpected:\n%s\nGotten:\n%s\n",
			attr_name, expected, outp);
	}

	/* cleanup */
	free(outp);
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
		fprintf(stderr, "Wrong arguments. Usage: %s [datasource [user [password]]]\n", argv[0]);
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
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, 0,
		"Get SQL_ATTR_MAX_LENGTH: 0\n"
		"Set SQL_ATTR_MAX_LENGTH: 0\n"
		"Get SQL_ATTR_MAX_LENGTH: 0\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, 65535,
		"Get SQL_ATTR_MAX_LENGTH: 0\n"
		"Set SQL_ATTR_MAX_LENGTH: 65535\n"
		"Get SQL_ATTR_MAX_LENGTH: 0\n"
		" which is different from 65535 !!\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, 2147483641,
		"Get SQL_ATTR_MAX_LENGTH: 0\n"
		"Set SQL_ATTR_MAX_LENGTH: 2147483641\n"
		"Get SQL_ATTR_MAX_LENGTH: 0\n"
		" which is different from 2147483641 !!\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_LENGTH, -1,	/* test also what happens with a negative value */
		"Get SQL_ATTR_MAX_LENGTH: 0\n"
		"Set SQL_ATTR_MAX_LENGTH: 18446744073709551615\n"
		"Get SQL_ATTR_MAX_LENGTH: 0\n"
		" which is different from 18446744073709551615 !!\n");

	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, 0,
		"Get SQL_ATTR_MAX_ROWS: 0\n"
		"Set SQL_ATTR_MAX_ROWS: 0\n"
		"Get SQL_ATTR_MAX_ROWS: 0\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, 100000,
		"Get SQL_ATTR_MAX_ROWS: 0\n"
		"Set SQL_ATTR_MAX_ROWS: 100000\n"
		"Get SQL_ATTR_MAX_ROWS: 0\n"
		" which is different from 100000 !!\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, 2147483642,
		"Get SQL_ATTR_MAX_ROWS: 0\n"
		"Set SQL_ATTR_MAX_ROWS: 2147483642\n"
		"Get SQL_ATTR_MAX_ROWS: 0\n"
		" which is different from 2147483642 !!\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_MAX_ROWS, -2,	/* test also what happens with a negative value */
		"Get SQL_ATTR_MAX_ROWS: 0\n"
		"Set SQL_ATTR_MAX_ROWS: 18446744073709551614\n"
		"Get SQL_ATTR_MAX_ROWS: 0\n"
		" which is different from 18446744073709551614 !!\n");

	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, 0,
		"Get SQL_ATTR_QUERY_TIMEOUT: 0\n"
		"Set SQL_ATTR_QUERY_TIMEOUT: 0\n"
		"Get SQL_ATTR_QUERY_TIMEOUT: 2147483647\n"
		" which is different from 0 !!\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, 3600,
		"Get SQL_ATTR_QUERY_TIMEOUT: 2147483647\n"
		"Set SQL_ATTR_QUERY_TIMEOUT: 3600\n"
		"Get SQL_ATTR_QUERY_TIMEOUT: 2147483647\n"
		" which is different from 3600 !!\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, 2147483643,
		"Get SQL_ATTR_QUERY_TIMEOUT: 2147483647\n"
		"Set SQL_ATTR_QUERY_TIMEOUT: 2147483643\n"
		"Get SQL_ATTR_QUERY_TIMEOUT: 2147483647\n"
		" which is different from 2147483643 !!\n");
	GetSetReGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, -3,	/* test also what happens with a negative value */
		"Get SQL_ATTR_QUERY_TIMEOUT: 2147483647\n"
		"Set SQL_ATTR_QUERY_TIMEOUT: 18446744073709551613\n"
		"Get SQL_ATTR_QUERY_TIMEOUT: 2147483647\n"
		" which is different from 18446744073709551613 !!\n");

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
