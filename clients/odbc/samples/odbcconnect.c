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
	"        -d              Target is DSN, call SQLConnect()\n"
	"        -c              Target is connection string, call SQLDriverConnect()\n"
	"        -b              Target is connection string, call SQLBrowseConnect()\n"
	"        -u USER\n"
	"        -p PASSWORD\n"
	"        -v              Be verbose\n"
	"        TARGET          Connection String or DSN\n";

static int do_sqlconnect(SQLCHAR *target);
static int do_sqldriverconnect(SQLCHAR *target);
static int do_sqlbrowseconnect(SQLCHAR *target);

static void ensure_ok(SQLSMALLINT type, SQLHANDLE handle, const char *message, SQLRETURN ret);


int verbose = 0;
SQLCHAR *user = NULL;
SQLCHAR *password = NULL;

SQLHANDLE env = NULL;
SQLHANDLE conn = NULL;

SQLCHAR outbuf[4096];

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
	int ret = 0;

	for (int i = 1; i < argc; i++) {
		char *arg = argv[i];
		if (strcmp(arg, "-d") == 0)
			action = do_sqlconnect;
		else if (strcmp(arg, "-c") == 0)
			action = do_sqldriverconnect;
		else if (strcmp(arg, "-b") == 0)
			action = do_sqlbrowseconnect;
		else if (strcmp(arg, "-u") == 0 && i + 1 < argc)
			user = (SQLCHAR*)argv[++i];
		else if (strcmp(arg, "-p") == 0 && i + 1 < argc)
			password = (SQLCHAR*)argv[++i];
		else if (strcmp(arg, "-v") == 0)
			verbose += 1;
		else if (arg[0] != '-')
			targets[ntargets++] = (SQLCHAR*)arg;
		else {
			fprintf(stderr, "\nERROR: invalid argument: %s\n%s", arg, USAGE);
			return 1;
		}
	}

	if (ntargets == 0) {
		fprintf(stderr, "\nERROR: pass at least one target\n%s", USAGE);
		return 1;
	}

	ensure_ok(
		SQL_HANDLE_ENV, NULL, "allocate env handle",
		SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env));

	ensure_ok(
		SQL_HANDLE_ENV, env, "set odbc version",
		SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0));

	ensure_ok(
		SQL_HANDLE_ENV, env, "allocate conn handle",
		SQLAllocHandle(SQL_HANDLE_DBC, env, &conn));

	for (int i = 0; i < ntargets; i++) {
		SQLCHAR *t = targets[i];
		if (verbose)
			fprintf(stderr, "\nTarget: %s\n", t);
		outbuf[0] = '\0';
		int ret = action(t);
		if (ret)
			break;
	}

	free(targets);
	cleanup();

	return ret;
}


static void
ensure_ok(SQLSMALLINT type, SQLHANDLE handle, const char *message, SQLRETURN ret)
{

	char *class;
	switch (ret) {
		case SQL_SUCCESS:
		case SQL_NEED_DATA:
			if (verbose)
				fprintf(stderr, "Succeeded: %s\n", message);
			class = "Info";
			break;
		case SQL_SUCCESS_WITH_INFO:
			class = "Warning";
			break;
		case SQL_ERROR:
			class = "Error";
			break;
		default:
			fprintf(stderr, "Internal error: %s: unknown SQLRETURN %d", message, ret);
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
			fprintf(stderr, "%s: %s\n", class, message);
			class = NULL;
		}
		fprintf(stderr, "    - %s: %s\n", state, explanation);
	}

	if (!SQL_SUCCEEDED(ret)) {
		cleanup();
		exit(1);
	}
}


static int
do_sqlconnect(SQLCHAR *target)
{
	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLConnect",
		SQLConnect(conn, target, SQL_NTS, user, SQL_NTS, password, SQL_NTS));
	printf("OK\n");

	return 0;
}

static int
do_sqldriverconnect(SQLCHAR *target)
{
	SQLSMALLINT n;
	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLDriverConnect",
		SQLDriverConnect(
			conn, NULL,
			target, SQL_NTS,
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
	SQLRETURN ret = SQLBrowseConnect(
		conn,
		target, SQL_NTS,
		outbuf, sizeof(outbuf), &n
	);
	ensure_ok(SQL_HANDLE_DBC, conn, "SQLBrowseConnect", ret);
	printf("%s %s\n",
		ret == SQL_NEED_DATA ? "BROWSE" : "OK",
		outbuf
	);
	return 0;
}

