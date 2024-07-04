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

#include "mutf8.h"

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
	"        -w              use the wide-char (unicode) interface\n"
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

static void make_arg(bool wide, const char *arg, void **bufp, SQLSMALLINT *buflen);
static void make_arga(const char *arg, void **bufp, SQLSMALLINT *buflen);
static void make_argw(const char *arg, void **bufp, SQLSMALLINT *buflen, bool bytes_not_chars);


static SQLWCHAR *gen_utf16(SQLWCHAR *dest, const char *src, size_t len);
static void convert_outw_outa(size_t n);


int verbose = 0;
char *user = NULL;
char *password = NULL;
char *query = NULL;
bool use_counted_strings = false;
bool use_wide = false;

SQLHANDLE env = NULL;
SQLHANDLE conn = NULL;
SQLHANDLE stmt = NULL;

#define OUTBUF_SIZE 4096
SQLCHAR attrbuf[4096];
SQLCHAR outabuf[OUTBUF_SIZE];
SQLWCHAR outwbuf[OUTBUF_SIZE];


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
		else if (strcmp(arg, "-w") == 0)
			use_wide = true;
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
		outabuf[0] = '\0';
		outwbuf[0] = 0;
		int ret = action(t);
		if (ret)
			return ret;
	}

	return 0;
}

static int
do_sqlconnect(const char *target)
{
	void *target_buf;
	SQLSMALLINT target_len = SQL_NTS;
	make_arg(use_wide, target, &target_buf, &target_len);

	void *user_buf;
	SQLSMALLINT user_len = SQL_NTS;
	make_arg(use_wide, user, &user_buf, &user_len);

	void *password_buf;
	SQLSMALLINT password_len = SQL_NTS;
	make_arg(use_wide, password, &password_buf, &password_len);

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLConnect",
		use_wide
			? SQLConnectW(conn, target_buf, target_len, user_buf, user_len, password_buf, password_len)
			: SQLConnectA(conn, target_buf, target_len, user_buf, user_len, password_buf, password_len)
	);
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
	void *target_buf;
	SQLSMALLINT target_len = SQL_NTS;
	make_arg(use_wide, target, &target_buf, &target_len);

	SQLSMALLINT n;

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLDriverConnect",
		use_wide
			? SQLDriverConnectW(conn, NULL, target_buf, target_len, outwbuf, OUTBUF_SIZE, &n, SQL_DRIVER_NOPROMPT)
			: SQLDriverConnectA(conn, NULL, target_buf, target_len, outabuf, OUTBUF_SIZE, &n, SQL_DRIVER_NOPROMPT)
	);
	if (use_wide)
		convert_outw_outa(n);

	printf("OK %s\n", outabuf);

	int exitcode = do_execute_stmt();

	ensure_ok(
		SQL_HANDLE_DBC, conn, "SQLDisconnect",
		SQLDisconnect(conn));

	return exitcode;
}

static int
do_sqlbrowseconnect(const char *target)
{
	void *target_buf;
	SQLSMALLINT target_len = SQL_NTS;
	make_arg(use_wide, target, &target_buf, &target_len);


	SQLSMALLINT n;

	SQLRETURN ret = use_wide
		? SQLBrowseConnectW(conn, target_buf, target_len, outwbuf, OUTBUF_SIZE, &n)
		: SQLBrowseConnectA(conn, target_buf, target_len, outabuf, OUTBUF_SIZE, &n);
	ensure_ok(SQL_HANDLE_DBC, conn, "SQLBrowseConnect", ret);
	if (use_wide)
		convert_outw_outa(n);
	printf("%s %s\n",
		ret == SQL_NEED_DATA ? "BROWSE" : "OK",
		outabuf
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
		outabuf[0] = attrbuf[0] = '\0';
		SQLRETURN ret = SQLDriversA(
			env, dir,
			outabuf, OUTBUF_SIZE, &len1,
			attrbuf, sizeof(attrbuf), &len2
		);
		if (ret == SQL_NO_DATA)
			break;
		ensure_ok(SQL_HANDLE_ENV, env, "SQLDrivers", ret);
		dir = SQL_FETCH_NEXT;
		count += 1;
		printf("DRIVER={%s}\n", outabuf);
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
		outabuf[0] = attrbuf[0] = '\0';
		SQLRETURN ret = SQLDataSourcesA(
			env, dir,
			outabuf, OUTBUF_SIZE, &len1,
			attrbuf, sizeof(attrbuf), &len2
		);
		if (ret == SQL_NO_DATA)
			break;
		ensure_ok(SQL_HANDLE_ENV, env, "SQLDataSources", ret);
		dir = SQL_FETCH_NEXT;
		printf("%s DSN=%s\n    Driver=%s\n", prefix, outabuf, attrbuf);
	}

	return 0;
}


static int
do_execute_stmt(void)
{
	void *query_buf;
	SQLSMALLINT query_len = SQL_NTS;

	if (query == NULL)
		return 0;

	if (verbose)
		printf("Statement: %s\n", query);

	make_arg(use_wide, query, &query_buf, &query_len);

	ensure_ok(
		SQL_HANDLE_ENV, conn, "allocate stmt handle",
		SQLAllocHandle(SQL_HANDLE_STMT, conn, &stmt));

	ensure_ok(
		SQL_HANDLE_STMT, stmt, "SQLExecDirect",
		use_wide
			? SQLExecDirectW(stmt, query_buf, query_len)
			: SQLExecDirectA(stmt, query_buf, query_len)
	);

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
				outabuf[0] = '\0';
				SQLRETURN ret = use_wide
					? SQLGetData(stmt, i, SQL_C_WCHAR, outwbuf, OUTBUF_SIZE * sizeof(SQLWCHAR), &n)
					: SQLGetData(stmt, i, SQL_C_CHAR, outabuf, OUTBUF_SIZE, &n);
				if (!SQL_SUCCEEDED(ret))
					ensure_ok(SQL_HANDLE_STMT, stmt, "SQLGetData", ret);
				if (use_wide)
					convert_outw_outa(n);
				printf("%s;", outabuf);
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
make_arg(bool wide, const char *arg, void**bufp, SQLSMALLINT *buflen)
{
	if (arg == NULL) {
		*bufp = NULL;
		*buflen = SQL_NTS;
		return;
	}

	if (wide)
		make_argw(arg, bufp, buflen, false);
	else
		make_arga(arg, bufp, buflen);
}

static void
make_arga(const char *arg, void**bufp, SQLSMALLINT *buflen)
{
	size_t len = strlen(arg);
	char *buf = alloc(len + 100);
	*bufp = (SQLCHAR*)buf;
	*buflen = SQL_NTS;

	char *p = buf;
	memmove(p, arg, len);
	p += len;

	if (use_counted_strings) {
		*buflen = (SQLSMALLINT)(p - buf);
		const char *garbage = "GARBAGE";
		size_t garbage_len = strlen(garbage);
		memmove(p, garbage, garbage_len);
		p += garbage_len;
	}

	*p = '\0';
}

static void
make_argw(const char *arg, void **bufp, SQLSMALLINT *buflen, bool bytes_not_chars)
{
	if (arg == NULL) {
		*bufp = NULL;
		*buflen = SQL_NTS;
		return;
	}

	size_t len = strlen(arg);
	SQLWCHAR *buf = alloc((len + 100) * 4);
	*bufp = buf;
	*buflen = SQL_NTS;

	SQLWCHAR *p = buf;
	p = gen_utf16(p, arg, len);

	if (use_counted_strings) {
		*buflen = (SQLSMALLINT)(p - buf);
		const char *garbage = "GARBAGE";
		size_t garbage_len = strlen(garbage);
		p = gen_utf16(p, garbage, garbage_len);
	}

	*p = '\0';

	if (bytes_not_chars)
		*buflen *= 2;
}


static SQLWCHAR*
gen_utf16(SQLWCHAR *dest, const char *src, size_t len)
{
	SQLWCHAR *p = dest;
	uint32_t state = UTF8_ACCEPT;
	for (size_t i = 0; i < len; i++) {
		unsigned char byte = (unsigned char)src[i];
		uint32_t codepoint;
		switch (decode(&state, &codepoint, byte)) {
		case UTF8_ACCEPT:
			if (codepoint <= 0xFFFF) {
				*p++ = (SQLWCHAR)codepoint;
			} else {
				uint16_t hi = (codepoint - 0x10000) >> 10;
				uint16_t lo = (codepoint - 0x10000) & 0x3FF;
				*p++ = (SQLWCHAR)(0xD800 + hi);
				*p++ = (SQLWCHAR)(0xDC00 + lo);
			}
			break;
		case UTF8_REJECT:
			fprintf(stderr, "\n\ninvalid utf8!\n");
			exit(1);
		default:
			break;
		}
	}
	if (state != UTF8_ACCEPT) {
			fprintf(stderr, "\n\nInvalid utf8!\n");
			exit(1);
	}

	return p;
}

static inline SQLCHAR
continuation_byte(uint32_t val, int n)
{
	val >>= 6 * n; // chop off right hand bits
	val &= 0x3F;   // chop off left hand bits
	val |= 0x80;   // add continuation marker bit
	return val;
}

static void
convert_outw_outa(size_t n)
{
	SQLWCHAR *end = &outwbuf[n];
	SQLWCHAR *in = &outwbuf[0];
	SQLCHAR *out = &outabuf[0];

	while (in < end) {
		SQLWCHAR w = *in++;
		uint32_t codepoint;
		if (w < 0xD800 || w >= 0xE000) {
			codepoint = w;
		} else if (w < 0xDC00 && in < end && *in >= 0xDC00 && *in < 0xE000) {
			uint32_t hi = w - 0xD800;
			uint32_t lo = *in++ - 0xDC00;
			codepoint = 0x10000 + (hi << 10) + lo;
		} else {
			strcpy((char*)out, "!!INVALID UTF-16 OR A BUG IN THE TEST ITSELF!!");
			break;
		}
		if (codepoint == 0xFEFF && out == &outabuf[0]) {
			// skip the BOM
		} else if (codepoint < 0x80) {
			*out++ = codepoint;
		} else if (codepoint < 0x800) {
			*out++ = 0xC0 | (codepoint >> 6);
			*out++ = continuation_byte(codepoint, 0);
		} else if (codepoint < 0x10000) {
			*out++ = 0xE0 | (codepoint >> 12);
			*out++ = continuation_byte(codepoint, 1);
			*out++ = continuation_byte(codepoint, 0);
		} else {
			assert(codepoint < 0x110000);
			*out++ = 0xF0 | (codepoint >> 18);
			*out++ = continuation_byte(codepoint, 2);
			*out++ = continuation_byte(codepoint, 1);
			*out++ = continuation_byte(codepoint, 0);
		}
	}

	*out = '\0';
}
