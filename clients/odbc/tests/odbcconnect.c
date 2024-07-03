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
#ifdef _MSC_VER
/* suppress deprecation warning for snprintf */
#define _CRT_SECURE_NO_WARNINGS

#include <WTypes.h>
#endif

#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include <sqlucode.h>

static const char *USAGE =
	"Usage:\n"
	"        odbcconnect [-d | -c | -b ] [-v] [-u USER] [-p PASSWORD] TARGET..\n"
	"Options:\n"
	"        -d              Target is connection string, call SQLDriverConnect()\n"
	"        -b              Target is connection string, call SQLBrowseConnect()\n"
	"        -l              List registered drivers and data sources\n"
	"        -u USER\n"
	"        -p PASSWORD\n"
	"        -q SQL          Execute SQL statement when connection succeeds\n"
	"        -0              use counted strings rather than nul-terminated arguments\n"
	"        -v              Be verbose\n"
	"        TARGET          DSN or with -d and -b, Connection String\n";

typedef int (action_t)(const char *);

static action_t do_sqlconnect;
static action_t do_sqldriverconnect;
static action_t do_sqlbrowseconnect;

static int do_actions(action_t action, int ntargets, char **targets);

static int do_listdrivers(void);
static int do_listdsns(const char *prefix, SQLSMALLINT dir);

static int do_execute_stmt(void);

static void ensure_ok_impl(SQLSMALLINT type, SQLHANDLE handle, const char *message, SQLRETURN ret, int lineno);

#define ensure_ok(type, handle, message, ret)    ensure_ok_impl(type, handle, message, ret, __LINE__)

static void make_arg(const char *arg, SQLCHAR**bufp, SQLSMALLINT *buflen);


int verbose = 0;
char *user = NULL;
char *password = NULL;
char *query = NULL;
bool use_counted_strings = false;

SQLHANDLE env = NULL;
SQLHANDLE conn = NULL;
SQLHANDLE stmt = NULL;

SQLCHAR outbuf[4096];
SQLCHAR attrbuf[4096];

// This free-list will be processed by cleanup().
// It is added to by alloc()
unsigned int ngarbage = 0;
void *garbage[100] = { NULL };


static void*
alloc(size_t size)
{
	void *p = calloc(size, 1);
	assert(p);
	if (ngarbage < sizeof(garbage) / sizeof(garbage[0]))
		garbage[ngarbage++] = p;
	return p;
}

static void
cleanup(void)
{
	if (stmt)
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	if (conn) {
		SQLDisconnect(conn);
		SQLFreeHandle(SQL_HANDLE_DBC, conn);
	}
	if (env)
		SQLFreeHandle(SQL_HANDLE_DBC, env);

	while (ngarbage > 0) {
		free(garbage[--ngarbage]);
	}
}

int
main(int argc, char **argv)
{
	int (*action)(const char*);
	action = do_sqlconnect;
	char **targets = alloc(argc * sizeof(argv[0]));
	int ntargets = 0;
	int ret;

	for (int i = 1; i < argc; i++) {
		char *arg = argv[i];
		if (strcmp(arg, "-d") == 0)
			action = do_sqldriverconnect;
		else if (strcmp(arg, "-b") == 0)
			action = do_sqlbrowseconnect;
		else if (strcmp(arg, "-l") == 0)
			action = NULL;
		else if (strcmp(arg, "-u") == 0 && i + 1 < argc)
			user = argv[++i];
		else if (strcmp(arg, "-p") == 0 && i + 1 < argc)
			password = argv[++i];
		else if (strcmp(arg, "-q") == 0 && i + 1 < argc)
			query = argv[++i];
		else if (strcmp(arg, "-0") == 0)
			use_counted_strings = true;
		else if (strcmp(arg, "-v") == 0)
			verbose += 1;
		else if (arg[0] != '-')
			targets[ntargets++] = arg;
		else {
			fprintf(stderr, "\nERROR: invalid argument: %s\n%s", arg, USAGE);
			ret = 1;
			goto end;
		}
	}

	ensure_ok(
		SQL_HANDLE_ENV, NULL, "allocate env handle",
		SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env));

	ensure_ok(
		SQL_HANDLE_ENV, env, "set odbc version",
		SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));

	if (action) {
		if (ntargets == 0) {
			fprintf(stderr, "\nERROR: pass at least one target\n%s", USAGE);
			ret = 1;
			goto end;
		}
		ret = do_actions(action, ntargets, targets);
	} else {
		if (ntargets != 0) {
			fprintf(stderr, "\nERROR: -l does not take arguments\n%s", USAGE);
			ret = 1;
			goto end;
		}
		ret = do_listdrivers();
		ret |= do_listdsns("SYSTEM", SQL_FETCH_FIRST_SYSTEM);
		ret |= do_listdsns("USER", SQL_FETCH_FIRST_USER);
	}

end:
	cleanup();

	return ret;
}


static void
ensure_ok_impl(SQLSMALLINT type, SQLHANDLE handle, const char *message, SQLRETURN ret, int lineno)
{
	static const char *filename = NULL;
	if (!filename) {
		filename = __FILE__;
		for (const char *p = filename; *p; p++) {
			if (*p == '/' || *p == '\\')
				filename = p + 1;
		}
	}

	char *class = "Info";
	switch (ret) {
		case SQL_SUCCESS:
		case SQL_NEED_DATA:
			if (verbose)
				printf(" Succeeded: %s (%s:%d)\n", message, filename, lineno);
			break;
		case SQL_SUCCESS_WITH_INFO:
			class = "Warning";
			break;
		case SQL_ERROR:
			class = "Error";
			break;
		default:
			printf("Internal error: %s (%s:%d): unknown SQLRETURN %d", message, filename, lineno, ret);
			break;
	}

	SQLCHAR state[6];
	SQLINTEGER error;
	SQLCHAR explanation[256];
	SQLSMALLINT len;

	bool printed_something = false;
	for (int i = 1; ; i++) {
		SQLRETURN diag_ret = SQLGetDiagRecA(
			type, handle, i,
			state, &error, explanation, sizeof(explanation), &len);
		if (!SQL_SUCCEEDED(diag_ret))
			break;
		if (class) {
			printf("%s: %s (%s:%d)\n", class, message, filename, lineno);
			class = NULL;
		}
		printf("    - %s: %s\n", state, explanation);
		printed_something = true;
	}

	if (!SQL_SUCCEEDED(ret) && ret != SQL_NEED_DATA) {
		if (!printed_something) {
			printf("%s: %s\n", class, message);
			printf("    - failed without explanation\n");
		}
		cleanup();
		exit(1);
	}
}


static int
do_actions(action_t action, int ntargets, char **targets)
{
	ensure_ok(
		SQL_HANDLE_ENV, env, "allocate conn handle",
		SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

	for (int i = 0; i < ntargets; i++) {
		char *t = targets[i];
		if (verbose)
			printf("\nTarget: %s\n", t);
		outbuf[0] = '\0';
		int ret = action(t);
		if (ret)
			return ret;
	}

	return 0;
}

static int
do_sqlconnect(const char *target)
{
	SQLCHAR *target_buf;
	SQLSMALLINT target_len = SQL_NTS;
	make_arg(target, &target_buf, &target_len);

	SQLCHAR *user_buf;
	SQLSMALLINT user_len = SQL_NTS;
	make_arg(user, &user_buf, &user_len);

	SQLCHAR *password_buf;
	SQLSMALLINT password_len = SQL_NTS;
	make_arg(password, &password_buf, &password_len);

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLConnect",
		SQLConnectA(conn, target_buf, target_len, user_buf, user_len, password_buf, password_len));
	printf("OK\n");

	int exitcode = do_execute_stmt();

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLDisconnect",
		SQLDisconnect(conn));

	return exitcode;
}

static int
do_sqldriverconnect(const char *target)
{
	SQLCHAR *target_buf;
	SQLSMALLINT target_len = SQL_NTS;
	make_arg(target, &target_buf, &target_len);

	SQLSMALLINT n;

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLDriverConnect",
		SQLDriverConnectA(
			conn, NULL,
			target_buf, target_len,
			outbuf, sizeof(outbuf), &n,
			SQL_DRIVER_NOPROMPT
		));

	printf("OK %s\n", outbuf);

	int exitcode = do_execute_stmt();

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLDisconnect",
		SQLDisconnect(conn));

	return exitcode;
}

static int
do_sqlbrowseconnect(const char *target)
{
	SQLCHAR *target_buf;
	SQLSMALLINT target_len = SQL_NTS;
	make_arg(target, &target_buf, &target_len);


	SQLSMALLINT n;

	SQLRETURN ret = SQLBrowseConnectA(
		conn,
		target_buf, target_len,
		outbuf, sizeof(outbuf), &n
	);
	ensure_ok(SQL_HANDLE_DBC, conn, "SQLBrowseConnect", ret);
	printf("%s %s\n",
		ret == SQL_NEED_DATA ? "BROWSE" : "OK",
		outbuf
	);

	int exitcode = 0;
	if (ret != SQL_NEED_DATA)
		exitcode = do_execute_stmt();

	// Do not call SQLDisconnect, SQLBrowseConnect is intended to
	// be invoked multiple times without disconnecting inbetween

	return exitcode;
}

static int
do_listdrivers(void)
{
	SQLSMALLINT dir = SQL_FETCH_FIRST;
	SQLSMALLINT len1, len2;
	int count = 0;

	while (1) {
		outbuf[0] = attrbuf[0] = '\0';
		SQLRETURN ret = SQLDriversA(
			env, dir,
			outbuf, sizeof(outbuf), &len1,
			attrbuf, sizeof(attrbuf), &len2
		);
		if (ret == SQL_NO_DATA)
			break;
		ensure_ok(SQL_HANDLE_ENV, env, "SQLDrivers", ret);
		dir = SQL_FETCH_NEXT;
		count += 1;
		printf("DRIVER={%s}\n", outbuf);
		for (char *p = (char*)attrbuf; *p; p += strlen(p) + 1) {
			printf("    %s\n", (char*)p);
		}
	}

	if (count == 0)
		printf("no drivers.\n");

	return 0;
}

static int
do_listdsns(const char *prefix, SQLSMALLINT dir)
{
	SQLSMALLINT len1, len2;

	while (1) {
		outbuf[0] = attrbuf[0] = '\0';
		SQLRETURN ret = SQLDataSourcesA(
			env, dir,
			outbuf, sizeof(outbuf), &len1,
			attrbuf, sizeof(attrbuf), &len2
		);
		if (ret == SQL_NO_DATA)
			break;
		ensure_ok(SQL_HANDLE_ENV, env, "SQLDataSources", ret);
		dir = SQL_FETCH_NEXT;
		printf("%s DSN=%s\n    Driver=%s\n", prefix, outbuf, attrbuf);
	}

	return 0;
}


static int
do_execute_stmt(void)
{
	SQLCHAR *query_buf;
	SQLSMALLINT query_len = SQL_NTS;

	if (query == NULL)
		return 0;

	if (verbose)
		printf("Statement: %s\n", query);

	make_arg(query, &query_buf, &query_len);

	ensure_ok(
		SQL_HANDLE_ENV, conn, "allocate stmt handle",
		SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt));

	ensure_ok(
		SQL_HANDLE_STMT, stmt, "SQLExecDirect",
		SQLExecDirectA(stmt, query_buf, query_len));

	do {
		SQLLEN rowcount = -1;
		SQLSMALLINT colcount = -1;

		ensure_ok(
			SQL_HANDLE_STMT, stmt, "SQLRowCount",
			SQLRowCount(stmt, &rowcount));

		ensure_ok(
			SQL_HANDLE_STMT, stmt, "SQLNumResultCols",
			SQLNumResultCols(stmt, &colcount));

		printf("RESULT rows=%ld cols=%d \n", rowcount, colcount);

		while (colcount > 0 && SQL_SUCCEEDED(SQLFetch(stmt))) {
			printf("    - ");
			for (int i = 1; i <= colcount; i++) {
				SQLLEN n;
				outbuf[0] = '\0';
				SQLRETURN ret = SQLGetData(stmt, i, SQL_C_CHAR, outbuf, sizeof(outbuf), &n);
				if (!SQL_SUCCEEDED(ret))
					ensure_ok(SQL_HANDLE_STMT, stmt, "SQLGetData", ret);
				printf("%s;", outbuf);
			}
			printf("\n");
		}

	} while (SQL_SUCCEEDED(SQLMoreResults(stmt)));

	ensure_ok(
		SQL_HANDLE_STMT, stmt, "SQLFreeHandle",
		SQLFreeHandle(SQL_HANDLE_STMT, stmt));
	stmt = NULL;

	return 0;
}


static void
make_arg(const char *arg, SQLCHAR**bufp, SQLSMALLINT *buflen)
{
	if (arg == NULL) {
		*bufp = NULL;
		*buflen = SQL_NTS;
		return;
	}

	size_t len = strlen(arg);
	if (!use_counted_strings) {
		*bufp = (SQLCHAR*)alloc(len + 1);
		memmove(*bufp, arg, len);
		// alloc() has initialized the final byte to \0
		*buflen = SQL_NTS;
		return;
	}

	const char *garbage = "GARBAGE";
	size_t garbage_len = strlen(garbage);
	*bufp = alloc(len + garbage_len + 1);
	memmove(*bufp, arg, len);
	memmove(*bufp + len, garbage, garbage_len);
	// alloc() has initialized the final byte to \0

	*buflen = (SQLSMALLINT)len;
}
