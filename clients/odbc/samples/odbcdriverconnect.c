/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

// borrowed from odbcsample1
static void
prerr(SQLSMALLINT tpe, SQLHANDLE hnd, const char *func, const char *pref)
{
	SQLCHAR state[6];
	SQLINTEGER errnr;
	SQLCHAR msg[256];
	SQLSMALLINT msglen;

	switch (SQLGetDiagRec(tpe, hnd, 1, state, &errnr, msg, sizeof(msg), &msglen)) {
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

// borrowed from odbcsample1, with changes.
// return 0 on success, proper exit code on error.
static int
check(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func)
{
	switch (ret) {
	case SQL_SUCCESS:
		return 0;
	case SQL_SUCCESS_WITH_INFO:
		prerr(tpe, hnd, func, "Info");
		return 0;
	case SQL_ERROR:
		prerr(tpe, hnd, func, "Error");
		return 1;
	case SQL_INVALID_HANDLE:
		fprintf(stderr, "%s: Error: invalid handle\n", func);
		return 1;
	default:
		fprintf(stderr, "%s: Unexpected return value: %d\n", func, ret);
		return 1;
	}
}

int
main(int argc, char **argv)
{
	int exit_code;
	SQLRETURN ret;
	SQLHANDLE env = NULL;
	SQLHANDLE conn = NULL;
	char *connection_string;
	SQLCHAR prompt[1024];
	SQLSMALLINT prompt_size = (SQLSMALLINT) sizeof(prompt);
	SQLSMALLINT required_size;

	if (argc != 2) {
		fprintf(stderr, "Usage: odbcbrowse CONNECTSTRING\n");
		fprintf(stderr, "Exit code: 2 = need more data, 1 = other error\n");
		exit_code = 1;
		goto end;
	}
	connection_string = argv[1];

	// Prepare

	ret = SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env);
	exit_code = check(ret, SQL_HANDLE_ENV, NULL, "SQLAllocHandle ENV");
	if (exit_code)
		goto end;

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, 0);
	exit_code = check(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr SQL_ATTR_ODBC_VERSION");
	if (exit_code)
		goto end;

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &conn);
	exit_code = check(ret, SQL_HANDLE_DBC, env, "SQLAllocHandle DBC");
	if (exit_code)
		goto end;

	// We have a connection handle, let's connect

	ret = SQLDriverConnect(conn, NULL, (SQLCHAR*)connection_string, SQL_NTS, prompt, prompt_size, &required_size, SQL_DRIVER_NOPROMPT);
	// ret = SQLBrowseConnectA(conn, (SQLCHAR*)connection_string, SQL_NTS, prompt, prompt_size, &required_size);
	if (required_size > prompt_size - 1) {
		fprintf(stderr, "Please ask a wizard to enlarge me");
		exit_code = 1;
		goto end;
	}
	printf("%s\n", (char*)prompt);

	exit_code = check(ret, SQL_HANDLE_DBC, conn, "SQLBrowseConnectA");

end:
	if (conn) {
		SQLDisconnect(conn);
		SQLFreeHandle(SQL_HANDLE_DBC, conn);
	}
	if (env)
		SQLFreeHandle(SQL_HANDLE_ENV, env);
	return exit_code;
}
