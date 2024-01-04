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
#include <WTypes.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>
#include <ctype.h>
#include <inttypes.h>
#include <wchar.h>

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

static size_t
retrieveDiagMsg(SQLHANDLE stmt, char * outp, size_t outp_len)
{
	SQLCHAR state[6];
	SQLINTEGER errnr = 0;
	char msg[256];
	SQLSMALLINT msglen = 0;
	SQLRETURN ret = SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &errnr, (SQLCHAR *) msg, sizeof(msg), &msglen);
	if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
		/* The message layout is: "[MonetDB][ODBC Driver 11.46.0][MonetDB-Test]error/warning text".
		   The ODBC driver version numbers changes in time. Overwrite it to get a stable output */
		if (strncmp(msg, "[MonetDB][ODBC Driver 11.", 25) == 0) {
			return snprintf(outp, outp_len, "SQLstate %s, Errnr %d, Message [MonetDB][ODBC Driver 11.##.#]%s\n", (char*)state, (int)errnr, strchr(msg + 25, ']') + 1);
		}
		return snprintf(outp, outp_len, "SQLstate %s, Errnr %d, Message %s\n", (char*)state, (int)errnr, (char*)msg);
	}
	return 0;
}

static void
compareResult(char * testname, char * testresult, char * expected)
{
	if (strcmp(expected, testresult) != 0) {
		fprintf(stderr, "Testing %s\nGotten:\n%s\nExpected:\n%s\n", testname, testresult, expected);
	}
}

#define LLFMT                   "%" PRId64

static SQLRETURN
testGetDataTruncatedString(SQLHANDLE stmt, SWORD ctype)
{
	SQLRETURN ret;
	SQLLEN RowCount = 0;
	SWORD NumResultCols = 0;

	size_t outp_len = 800;
	char * outp = malloc(outp_len);
	size_t pos = 0;

//	char * sql = "select tag as \"qtag\", sessionid as \"sessionid\", username as \"username\", started as \"started\", status as \"status\", query as \"query\", finished as \"finished\", maxworkers as \"maxworkers\", footprint as \"footprint\" from sys.queue() where query like '/*e6dbd251960c49bbbeff9784fa70c86d*/%';";
	char * sql = "select cast('12345678901234567890 abcdefghijklmnopqrstuvwxyz' as clob) as val;";
	ret = SQLExecDirect(stmt, (SQLCHAR *) sql, SQL_NTS);
	pos += snprintf(outp + pos, outp_len - pos, "SQLExecDirect\n");
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect");

	ret = SQLRowCount(stmt, &RowCount);
	pos += snprintf(outp + pos, outp_len - pos, "SQLRowCount is " LLFMT "\n", (int64_t) RowCount);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLRowCount");

	ret = SQLNumResultCols(stmt, &NumResultCols);
	pos += snprintf(outp + pos, outp_len - pos, "SQLNumResultCols is %d\n", NumResultCols);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLNumResultCols");

	ret = SQLFetch(stmt);
	pos += snprintf(outp + pos, outp_len - pos, "SQLFetch\n");
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch");

	for (SWORD col = 1; col <= NumResultCols; col++) {
		char buf[99];
		wchar_t wbuf[99];
		char buf2[99];
		wchar_t wbuf2[99];
		SQLLEN vallen = 0;
		SQLLEN NumAttr = 0;
		char * ctype_str = (ctype == SQL_C_CHAR ? "SQL_C_CHAR" : ctype == SQL_C_WCHAR ? "SQL_C_WCHAR" : "NYI");

		/* retrieve query result column metadata */
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_CONCISE_TYPE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_CONCISE_TYPE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_LENGTH, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_LENGTH) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_DISPLAY_SIZE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_DISPLAY_SIZE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);

		/* test SQLGetData(SQL_C_(W)CHAR, 20) with a restricted buffer size (20) for the queried string value (47) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)ctype, ctype == SQL_C_WCHAR ? (PTR)&wbuf : (PTR)&buf, (SQLLEN)20, &vallen);
		if (ctype == SQL_C_WCHAR) {
			/* snprintf does not allow printing wchar strings. convert it to a char string */
			/* tried: wcstombs(buf, wbuf, 99); but it doesn't work */
			/* workaround: just empty the buffer to get a stable output on all platforms (power8 gives a different output) */
			buf[0] = 0;
		}
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, %s, 20) returns %d, vallen " LLFMT ", buf: '%s'\n", col, ctype_str, ret, (int64_t) vallen, buf);
		/* we expect SQL_SUCCESS_WITH_INFO with warning msg set, fetch them */
		if (ret == SQL_SUCCESS_WITH_INFO) {
			pos += retrieveDiagMsg(stmt, outp + pos, outp_len - pos);

			/* get the next data part of the value (this is how SQLGetData is intended to be used to get large data in chunks) */
			ret = SQLGetData(stmt, (UWORD)col, (SWORD)ctype, ctype == SQL_C_WCHAR ? (PTR)&wbuf2 : (PTR)&buf2, (SQLLEN)30, &vallen);
			if (ctype == SQL_C_WCHAR) {
				/* tried: wcstombs(buf2, wbuf2, 99); but it doesn't work */
				/* workaround: just empty the buffer to get a stable output on all platforms (power8 gives a different output) */
				buf2[0] = 0;
			}
			pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, %s, 30) returns %d, vallen " LLFMT ", buf: '%s'\n", col, ctype_str, ret, (int64_t) vallen, buf2);
			if (ret == SQL_SUCCESS_WITH_INFO) {
				pos += retrieveDiagMsg(stmt, outp + pos, outp_len - pos);
				ret = SQL_SUCCESS;
			}
		}
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col)");
	}

	if (ctype == SQL_C_CHAR) {
		compareResult("testGetDataTruncatedString(SQL_C_CHAR)", outp,
			"SQLExecDirect\nSQLRowCount is 1\nSQLNumResultCols is 1\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr -10\n"	/* -10 = SQL_WLONGVARCHAR */
			"SQLColAttribute(1, SQL_DESC_LENGTH) returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 47\n"
			"SQLGetData(1, SQL_C_CHAR, 20) returns 1, vallen 47, buf: '1234567890123456789'\n"
			"SQLstate 01004, Errnr 0, Message [MonetDB][ODBC Driver 11.##.#][MonetDB-Test]String data, right truncated\n"
			"SQLGetData(1, SQL_C_CHAR, 30) returns 0, vallen 28, buf: '0 abcdefghijklmnopqrstuvwxyz'\n");
	} else
	if (ctype == SQL_C_WCHAR) {
		compareResult("testGetDataTruncatedString(SQL_C_WCHAR)", outp,
			"SQLExecDirect\nSQLRowCount is 1\nSQLNumResultCols is 1\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr -10\n"	/* -10 = SQL_WLONGVARCHAR */
			"SQLColAttribute(1, SQL_DESC_LENGTH) returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 47\n"
			"SQLGetData(1, SQL_C_WCHAR, 20) returns 1, vallen 94, buf: ''\n"
			"SQLstate 01004, Errnr 0, Message [MonetDB][ODBC Driver 11.##.#][MonetDB-Test]String data, right truncated\n"
			"SQLGetData(1, SQL_C_WCHAR, 30) returns 1, vallen 76, buf: ''\n"
			"SQLstate 01004, Errnr 0, Message [MonetDB][ODBC Driver 11.##.#][MonetDB-Test]String data, right truncated\n");
	}

	/* cleanup */
	free(outp);
	return ret;
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

	/* run tests */
	ret = testGetDataTruncatedString(stmt, SQL_C_CHAR);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataTruncatedString(STMT, SQL_C_CHAR)");

	ret = SQLCloseCursor(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");

	ret = testGetDataTruncatedString(stmt, SQL_C_WCHAR);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataTruncatedString(STMT, SQL_C_WCHAR)");

	/* cleanup */
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
