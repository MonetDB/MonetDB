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

static const char *USAGE =
	"Usage:\n"
	"        odbcconnect [-d | -c | -b ] [-v] [-u USER] [-p PASSWORD] TARGET..\n"
	"Options:\n"
	"        -d              Target is connection string, call SQLDriverConnect()\n"
	"        -b              Target is connection string, call SQLBrowseConnect()\n"
	"        -l              List registered drivers and data sources\n"
	"        -u USER\n"
	"        -p PASSWORD\n"
	"        -0              use counted strings rather than nul-terminated arguments\n"
	"        -v              Be verbose\n"
	"        TARGET          DSN or with -d and -b, Connection String\n";

typedef int (action_t)(SQLCHAR *);

static int do_actions(action_t action, int ntargets, SQLCHAR **targets);

static action_t do_sqlconnect;
static action_t do_sqldriverconnect;
static action_t do_sqlbrowseconnect;

static int do_listdrivers(void);
static int do_listdsns(const char *prefix, SQLSMALLINT dir);

static void ensure_ok(SQLSMALLINT type, SQLHANDLE handle, const char *message, SQLRETURN ret);

#define MARGIN 100
static SQLCHAR* sqldup_with_margin(const char *str);
static void fuzz_sql_nts(SQLCHAR **str, SQLSMALLINT *len);

int verbose = 0;
SQLCHAR *user = NULL;
SQLSMALLINT user_len = SQL_NTS;
SQLCHAR *password = NULL;
SQLSMALLINT password_len = SQL_NTS;
bool use_counted_strings = false;

SQLHANDLE env = NULL;
SQLHANDLE conn = NULL;

SQLCHAR outbuf[4096];
SQLCHAR attrbuf[4096];

static void
cleanup(void)
{
	if (conn) {
		SQLDisconnect(conn);
		SQLFreeHandle(SQL_HANDLE_DBC, conn);
	}
	if (env)
		SQLFreeHandle(SQL_HANDLE_DBC, env);
}

int
main(int argc, char **argv)
{
	int (*action)(SQLCHAR *);
	action = do_sqlconnect;
	SQLCHAR **targets = calloc(argc, sizeof(argv[0]));
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
			user = sqldup_with_margin(argv[++i]);
		else if (strcmp(arg, "-p") == 0 && i + 1 < argc)
			password = sqldup_with_margin(argv[++i]);
		else if (strcmp(arg, "-0") == 0)
			use_counted_strings = true;
		else if (strcmp(arg, "-v") == 0)
			verbose += 1;
		else if (arg[0] != '-')
			targets[ntargets++] = sqldup_with_margin(arg);
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

	if (use_counted_strings) {
		fuzz_sql_nts(&user, &user_len);
		fuzz_sql_nts(&password, &password_len);
	}

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
		ret |= do_listdsns("SYSTEM", SQL_FETCH_FIRST_USER);
	}

end:
	free(user);
	free(password);
	for (int i = 0; i < ntargets; i++)
		free(targets[i]);
	free(targets);
	cleanup();

	return ret;
}


static void
ensure_ok(SQLSMALLINT type, SQLHANDLE handle, const char *message, SQLRETURN ret)
{

	char *class = "Info";
	switch (ret) {
		case SQL_SUCCESS:
		case SQL_NEED_DATA:
			if (verbose)
				printf("Succeeded: %s\n", message);
			break;
		case SQL_SUCCESS_WITH_INFO:
			class = "Warning";
			break;
		case SQL_ERROR:
			class = "Error";
			break;
		default:
			printf("Internal error: %s: unknown SQLRETURN %d", message, ret);
			break;
	}

	SQLCHAR state[6];
	SQLINTEGER error;
	SQLCHAR explanation[256];
	SQLSMALLINT len;

	for (int i = 1; ; i++) {
		SQLRETURN diag_ret = SQLGetDiagRec(
			type, handle, i,
			state, &error, explanation, sizeof(explanation), &len);
		if (!SQL_SUCCEEDED(diag_ret))
			break;
		if (class) {
			printf("%s: %s\n", class, message);
			class = NULL;
		}
		printf("    - %s: %s\n", state, explanation);
	}

	if (!SQL_SUCCEEDED(ret) && ret != SQL_NEED_DATA) {
		cleanup();
		exit(1);
	}
}


static int
do_actions(action_t action, int ntargets, SQLCHAR **targets)
{
	ensure_ok(
		SQL_HANDLE_ENV, env, "allocate conn handle",
		SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

	for (int i = 0; i < ntargets; i++) {
		SQLCHAR *t = targets[i];
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
do_sqlconnect(SQLCHAR *target)
{
	SQLSMALLINT target_len = SQL_NTS;
	if (use_counted_strings)
		fuzz_sql_nts(&target, &target_len);

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLConnect",
		SQLConnect(conn, target, target_len, user, user_len, password, password_len));
	printf("OK\n");

	return 0;
}

static int
do_sqldriverconnect(SQLCHAR *target)
{
	SQLSMALLINT n;
	SQLSMALLINT target_len = SQL_NTS;
	if (use_counted_strings)
		fuzz_sql_nts(&target, &target_len);

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLDriverConnect",
		SQLDriverConnect(
			conn, NULL,
			target, target_len,
			outbuf, sizeof(outbuf), &n,
			SQL_DRIVER_NOPROMPT
		));

	printf("OK %s\n", outbuf);
	return 0;
}

static int
do_sqlbrowseconnect(SQLCHAR *target)
{
	SQLSMALLINT n;
	SQLSMALLINT target_len = SQL_NTS;
	if (use_counted_strings)
		fuzz_sql_nts(&target, &target_len);

	SQLRETURN ret = SQLBrowseConnect(
		conn,
		target, target_len,
		outbuf, sizeof(outbuf), &n
	);
	ensure_ok(SQL_HANDLE_DBC, conn, "SQLBrowseConnect", ret);
	printf("%s %s\n",
		ret == SQL_NEED_DATA ? "BROWSE" : "OK",
		outbuf
	);
	return 0;
}

static int
do_listdrivers(void)
{
	SQLSMALLINT dir = SQL_FETCH_FIRST;
	SQLSMALLINT len1, len2;
	int count = 0;

	while (1) {
		outbuf[0] = attrbuf[0] = '\0';
		SQLRETURN ret = SQLDrivers(
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
		SQLRETURN ret = SQLDataSources(
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


static SQLCHAR*
sqldup_with_margin(const char *str)
{
	size_t len = strlen(str);
	char *buf = malloc(len + MARGIN);
	memmove(buf, str, len);
	memset(buf + len, 0, MARGIN);
	return (SQLCHAR*)buf;
}

static void
fuzz_sql_nts(SQLCHAR **str, SQLSMALLINT *len)
{
	if (*str != NULL) {
		// append garbage so it's no longer properly NUL terminated,
		// indicate original length through 'len'
		size_t n = strlen((char*)*str);
		const char *garbage = "GARBAGE";
		size_t garblen = strlen(garbage);
		memmove(*str + n, garbage, garblen + 1); // include the trailing NUL
		*len = (SQLSMALLINT)n;
	}
}
