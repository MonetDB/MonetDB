/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifdef _MSC_VER
#include <WTypes.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <ctype.h>
#include <inttypes.h>
#include <wchar.h>

/**** Define the ODBC Version our ODBC driver complies with ****/
#define ODBCVER 0x0352		/* Important: this must be defined before include of sql.h and sqlext.h */
#include <sql.h>
#include <sqlext.h>

static void
prerr(SQLSMALLINT tpe, SQLHANDLE hnd, const char *func, const char *pref)
{
	SQLCHAR state[SQL_SQLSTATE_SIZE +1];
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
		ret = SQLColAttributeW(stmt, (UWORD)col, SQL_DESC_LITERAL_PREFIX, (PTR)&wbuf, (SQLLEN)99, NULL, &NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_LITERAL_PREFIX, (PTR)&buf, (SQLLEN)99, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_LITERAL_PREFIX: %s) returns %d, NumAttr " LLFMT "\n", col, buf, ret, (int64_t) NumAttr);
		ret = SQLColAttributeW(stmt, (UWORD)col, SQL_DESC_LITERAL_SUFFIX, (PTR)&wbuf, (SQLLEN)99, NULL, &NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_LITERAL_SUFFIX, (PTR)&buf, (SQLLEN)99, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_LITERAL_SUFFIX: %s) returns %d, NumAttr " LLFMT "\n", col, buf, ret, (int64_t) NumAttr);

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
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr -9\n"	/* -9 = SQL_WVARCHAR */
			"SQLColAttribute(1, SQL_DESC_LENGTH) returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_PREFIX: ') returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_SUFFIX: ') returns 0, NumAttr 47\n"
			"SQLGetData(1, SQL_C_CHAR, 20) returns 1, vallen 47, buf: '1234567890123456789'\n"
			"SQLstate 01004, Errnr 0, Message [MonetDB][ODBC Driver 11.##.#][MonetDB-Test]String data, right truncated\n"
			"SQLGetData(1, SQL_C_CHAR, 30) returns 0, vallen 28, buf: '0 abcdefghijklmnopqrstuvwxyz'\n");
	} else
	if (ctype == SQL_C_WCHAR) {
		compareResult("testGetDataTruncatedString(SQL_C_WCHAR)", outp,
			"SQLExecDirect\nSQLRowCount is 1\nSQLNumResultCols is 1\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr -9\n"	/* -9 = SQL_WVARCHAR */
			"SQLColAttribute(1, SQL_DESC_LENGTH) returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_PREFIX: ') returns 0, NumAttr 47\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_SUFFIX: ') returns 0, NumAttr 47\n"
			"SQLGetData(1, SQL_C_WCHAR, 20) returns 1, vallen 94, buf: ''\n"
			"SQLstate 01004, Errnr 0, Message [MonetDB][ODBC Driver 11.##.#][MonetDB-Test]String data, right truncated\n"
			"SQLGetData(1, SQL_C_WCHAR, 30) returns 1, vallen 76, buf: ''\n"
			"SQLstate 01004, Errnr 0, Message [MonetDB][ODBC Driver 11.##.#][MonetDB-Test]String data, right truncated\n");
	}

	ret = SQLCloseCursor(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");

	/* cleanup */
	free(outp);
	return ret;
}

static SQLRETURN
testGetDataGUID(SQLHANDLE stmt)
{
	SQLRETURN ret;
	SQLLEN RowCount = 0;
	SWORD NumResultCols = 0;

	size_t outp_len = 1800;
	char * outp = malloc(outp_len);
	size_t pos = 0;

	char * sql = "select cast(NULL as uuid) as valnil, cast('eda7b074-3e0f-4bef-bdec-19c61bedb18f' as uuid) as val1, cast('beefc4f7-0264-4735-9b7a-75fd371ef803' as uuid) as val2;";
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
		SQLGUID guid_val;
		char guid_str_val[40];
		SQLLEN vallen = 0;
		SQLLEN NumAttr = 0;

		/* retrieve query result column metadata */
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_CONCISE_TYPE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_CONCISE_TYPE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_DISPLAY_SIZE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_DISPLAY_SIZE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_LITERAL_PREFIX, (PTR)&buf, (SQLLEN)99, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_LITERAL_PREFIX: %s) returns %d, NumAttr " LLFMT "\n", col, buf, ret, (int64_t) NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_LITERAL_SUFFIX, (PTR)&buf, (SQLLEN)99, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_LITERAL_SUFFIX: %s) returns %d, NumAttr " LLFMT "\n", col, buf, ret, (int64_t) NumAttr);

		/* test SQLGetData(SQL_C_CHAR) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)SQL_C_CHAR, (PTR)&guid_str_val, (SQLLEN)40, &vallen);
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, SQL_C_CHAR, 36) returns %d, vallen " LLFMT ", str_val: '%s'\n",
			col, ret, (int64_t) vallen, (vallen == SQL_NULL_DATA) ? "NULL" : guid_str_val);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col)");

		/* test SQLGetData(SQL_C_GUID) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)SQL_C_GUID, (PTR)&guid_val, (SQLLEN)16, &vallen);
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, SQL_C_GUID, 16) returns %d, vallen " LLFMT ", data_val: ", col, ret, (int64_t) vallen);
		if (vallen == SQL_NULL_DATA)
			pos += snprintf(outp + pos, outp_len - pos, "NULL\n");
		else
			pos += snprintf(outp + pos, outp_len - pos, "%08x-%04x-%04x-%02x%02x-%02x%02x%02x%02x%02x%02x\n",
					(unsigned int) guid_val.Data1, guid_val.Data2, guid_val.Data3,
				guid_val.Data4[0], guid_val.Data4[1], guid_val.Data4[2], guid_val.Data4[3], guid_val.Data4[4], guid_val.Data4[5], guid_val.Data4[6], guid_val.Data4[7]);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col)");
	}

	compareResult("testGetDataGUID()", outp,
			"SQLExecDirect\nSQLRowCount is 1\nSQLNumResultCols is 3\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr -11\n"	/* -11 = SQL_GUID */
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 36\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_PREFIX: uuid ') returns 0, NumAttr 36\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_SUFFIX: ') returns 0, NumAttr 36\n"
			"SQLGetData(1, SQL_C_CHAR, 36) returns 0, vallen -1, str_val: 'NULL'\n"
			"SQLGetData(1, SQL_C_GUID, 16) returns 0, vallen -1, data_val: NULL\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr -11\n"	/* -11 = SQL_GUID */
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 36\n"
			"SQLColAttribute(2, SQL_DESC_LITERAL_PREFIX: uuid ') returns 0, NumAttr 36\n"
			"SQLColAttribute(2, SQL_DESC_LITERAL_SUFFIX: ') returns 0, NumAttr 36\n"
			"SQLGetData(2, SQL_C_CHAR, 36) returns 0, vallen 36, str_val: 'eda7b074-3e0f-4bef-bdec-19c61bedb18f'\n"
			"SQLGetData(2, SQL_C_GUID, 16) returns 0, vallen 16, data_val: eda7b074-3e0f-4bef-bdec-19c61bedb18f\n"
			"SQLColAttribute(3, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr -11\n"	/* -11 = SQL_GUID */
			"SQLColAttribute(3, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 36\n"
			"SQLColAttribute(3, SQL_DESC_LITERAL_PREFIX: uuid ') returns 0, NumAttr 36\n"
			"SQLColAttribute(3, SQL_DESC_LITERAL_SUFFIX: ') returns 0, NumAttr 36\n"
			"SQLGetData(3, SQL_C_CHAR, 36) returns 0, vallen 36, str_val: 'beefc4f7-0264-4735-9b7a-75fd371ef803'\n"
			"SQLGetData(3, SQL_C_GUID, 16) returns 0, vallen 16, data_val: beefc4f7-0264-4735-9b7a-75fd371ef803\n");

	ret = SQLCloseCursor(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");

	/* cleanup */
	free(outp);
	return ret;
}

static SQLRETURN
testGetDataIntervalDay(SQLHANDLE stmt, int sqlquery)
{
	SQLRETURN ret;
	SQLLEN RowCount = 0;
	SWORD NumResultCols = 0;

	size_t outp_len = 1800;
	char * outp = malloc(outp_len);
	size_t pos = 0;

	char * sql1 = "select cast(NULL as interval day) as valnil, cast('99' as interval day) as val1, cast('-99' as interval day) as val2;";
	char * sql2 = "select cast(NULL as interval day) as valnil, cast('101' as interval day) as val1, cast('-102' as interval day) as val2;";	/* Interval field overflow */
	ret = SQLExecDirect(stmt, (sqlquery == 1) ? (SQLCHAR *) sql1 : (SQLCHAR *) sql2, SQL_NTS);
	pos += snprintf(outp + pos, outp_len - pos, "SQLExecDirect query %d\n", sqlquery);
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
		char str_val[42];
		int int_val;
		SQL_INTERVAL_STRUCT itv_val;
		SQLLEN vallen = 0;
		SQLLEN NumAttr = 0;

		/* retrieve query result column metadata */
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_CONCISE_TYPE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_CONCISE_TYPE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_DISPLAY_SIZE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_DISPLAY_SIZE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);
		ret = SQLColAttributeW(stmt, (UWORD)col, SQL_DESC_LITERAL_PREFIX, (PTR)&wbuf, (SQLLEN)99, NULL, &NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_LITERAL_PREFIX, (PTR)&buf, (SQLLEN)99, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_LITERAL_PREFIX: %s) returns %d, NumAttr " LLFMT "\n", col, buf, ret, (int64_t) NumAttr);
		ret = SQLColAttributeW(stmt, (UWORD)col, SQL_DESC_LITERAL_SUFFIX, (PTR)&wbuf, (SQLLEN)99, NULL, &NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_LITERAL_SUFFIX, (PTR)&buf, (SQLLEN)99, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_LITERAL_SUFFIX: %s) returns %d, NumAttr " LLFMT "\n", col, buf, ret, (int64_t) NumAttr);

		/* test SQLGetData(SQL_C_CHAR) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)SQL_C_CHAR, (PTR)&str_val, (SQLLEN)41, &vallen);
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, SQL_C_CHAR, 41) returns %d, vallen " LLFMT ", str_val: '%s'\n",
			col, ret, (int64_t) vallen, (vallen == SQL_NULL_DATA) ? "NULL" : str_val);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col) as str");

		/* test SQLGetData(SQL_C_SLONG) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)SQL_C_SLONG, (PTR)&int_val, (SQLLEN)4, &vallen);
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, SQL_C_SLONG) returns %d, vallen " LLFMT ", int_val: ", col, ret, (int64_t) vallen);
		if (vallen == SQL_NULL_DATA)
			pos += snprintf(outp + pos, outp_len - pos, "NULL\n");
		else
			pos += snprintf(outp + pos, outp_len - pos, "%d\n", int_val);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col) as int");	/* SQLstate 07006 Restricted data type attribute violation */

		/* test SQLGetData(SQL_C_INTERVAL_DAY) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)SQL_C_INTERVAL_DAY, (PTR)&itv_val, (SQLLEN)sizeof(SQL_INTERVAL_STRUCT), &vallen);
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, SQL_C_INTERVAL_DAY) returns %d, vallen " LLFMT ", itv_day_val: ", col, ret, (int64_t) vallen);
		if (vallen == SQL_NULL_DATA)
			pos += snprintf(outp + pos, outp_len - pos, "NULL\n");
		else
			pos += snprintf(outp + pos, outp_len - pos, "%lu (type %u, sign %d)\n", (unsigned long) itv_val.intval.day_second.day, (unsigned) itv_val.interval_type, itv_val.interval_sign);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col) as int");
	}

	compareResult("testGetDataIntervalDay()", outp,
		(sqlquery == 1)
		?	"SQLExecDirect query 1\nSQLRowCount is 1\nSQLNumResultCols is 3\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 103\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 21\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_PREFIX: interval ') returns 0, NumAttr 21\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_SUFFIX: ' day) returns 0, NumAttr 21\n"
			"SQLGetData(1, SQL_C_CHAR, 41) returns 0, vallen -1, str_val: 'NULL'\n"
			"SQLGetData(1, SQL_C_SLONG) returns 0, vallen -1, int_val: NULL\n"
			"SQLGetData(1, SQL_C_INTERVAL_DAY) returns 0, vallen -1, itv_day_val: NULL\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 103\n"
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 21\n"
			"SQLColAttribute(2, SQL_DESC_LITERAL_PREFIX: interval ') returns 0, NumAttr 21\n"
			"SQLColAttribute(2, SQL_DESC_LITERAL_SUFFIX: ' day) returns 0, NumAttr 21\n"
			"SQLGetData(2, SQL_C_CHAR, 41) returns 0, vallen 17, str_val: 'INTERVAL '99' DAY'\n"
			"SQLGetData(2, SQL_C_SLONG) returns 0, vallen 4, int_val: 99\n"	/* SQLstate 07006 Restricted data type attribute violation */
			"SQLGetData(2, SQL_C_INTERVAL_DAY) returns 0, vallen 28, itv_day_val: 99 (type 3, sign 0)\n"
			"SQLColAttribute(3, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 103\n"
			"SQLColAttribute(3, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 21\n"
			"SQLColAttribute(3, SQL_DESC_LITERAL_PREFIX: interval ') returns 0, NumAttr 21\n"
			"SQLColAttribute(3, SQL_DESC_LITERAL_SUFFIX: ' day) returns 0, NumAttr 21\n"
			"SQLGetData(3, SQL_C_CHAR, 41) returns 0, vallen 18, str_val: 'INTERVAL -'99' DAY'\n"
			"SQLGetData(3, SQL_C_SLONG) returns 0, vallen 4, int_val: -99\n"	/* SQLstate 07006 Restricted data type attribute violation */
			"SQLGetData(3, SQL_C_INTERVAL_DAY) returns 0, vallen 28, itv_day_val: 99 (type 3, sign 1)\n"
		:	"SQLExecDirect query 2\nSQLRowCount is 1\nSQLNumResultCols is 3\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 103\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 21\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_PREFIX: interval ') returns 0, NumAttr 21\n"
			"SQLColAttribute(1, SQL_DESC_LITERAL_SUFFIX: ' day) returns 0, NumAttr 21\n"
			"SQLGetData(1, SQL_C_CHAR, 41) returns 0, vallen -1, str_val: 'NULL'\n"
			"SQLGetData(1, SQL_C_SLONG) returns 0, vallen -1, int_val: NULL\n"
			"SQLGetData(1, SQL_C_INTERVAL_DAY) returns 0, vallen -1, itv_day_val: NULL\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 103\n"
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 21\n"
			"SQLColAttribute(2, SQL_DESC_LITERAL_PREFIX: interval ') returns 0, NumAttr 21\n"
			"SQLColAttribute(2, SQL_DESC_LITERAL_SUFFIX: ' day) returns 0, NumAttr 21\n"
			"SQLGetData(2, SQL_C_CHAR, 41) returns 0, vallen 18, str_val: 'INTERVAL '101' DAY'\n"
			"SQLGetData(2, SQL_C_SLONG) returns 0, vallen 4, int_val: 101\n"	/* SQLstate 07006 Restricted data type attribute violation */
			"SQLGetData(2, SQL_C_INTERVAL_DAY) returns 0, vallen 28, itv_day_val: 101 (type 3, sign 0)\n"
			"SQLColAttribute(3, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 103\n"
			"SQLColAttribute(3, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 21\n"
			"SQLColAttribute(3, SQL_DESC_LITERAL_PREFIX: interval ') returns 0, NumAttr 21\n"
			"SQLColAttribute(3, SQL_DESC_LITERAL_SUFFIX: ' day) returns 0, NumAttr 21\n"
			"SQLGetData(3, SQL_C_CHAR, 41) returns 0, vallen 19, str_val: 'INTERVAL -'102' DAY'\n"
			"SQLGetData(3, SQL_C_SLONG) returns 0, vallen 4, int_val: -102\n"	/* SQLstate 07006 Restricted data type attribute violation */
			"SQLGetData(3, SQL_C_INTERVAL_DAY) returns 0, vallen 28, itv_day_val: 102 (type 3, sign 1)\n"
		);

	ret = SQLCloseCursor(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");

	/* cleanup */
	free(outp);
	return ret;
}

#ifdef HAVE_HGE
static SQLRETURN
testGetDataDecimal(SQLHANDLE stmt, int sqlquery)
{
	SQLRETURN ret;
	SQLLEN RowCount = 0;
	SWORD NumResultCols = 0;

	size_t outp_len = 1800;
	char * outp = malloc(outp_len);
	size_t pos = 0;

	char * sql1 = "select cast(99999999909999999990999999999012345678. as decimal(38,0)) as val1, cast(-99999999909999999990999999999012345678. as decimal(38,0)) as val2;";
	char * sql2 = "select cast(92345678901234567890.123456789 as decimal(29,9)) as val1, cast(-92345678901234567890.123456789 as decimal(29,9)) as val2;";
	char * sql3 = "select cast(92345678901234567890.123456789012345678 as decimal(38,18)) as val1, cast(-9234567890123456789.1234567890123456789 as decimal(38,19)) as val2;";
	char * sql4 = "select cast(987654321.12345678901234567890123456789 as decimal(38,29)) as val1, cast(-987654321.12345678901234567890123456789 as decimal(38,29)) as val2;";
	char * sql5 = "select cast(.99999999909999999990999999999012345678 as decimal(38,38)) as val1, cast(-.99999999909999999990999999999012345678 as decimal(38,38)) as val2;";
	char * sql = (sqlquery == 1) ? sql1 : (sqlquery == 2) ? sql2 : (sqlquery == 3) ? sql3 : (sqlquery == 4) ? sql4 : sql5;

	ret = SQLExecDirect(stmt, (SQLCHAR *) sql, SQL_NTS);
	pos += snprintf(outp + pos, outp_len - pos, "SQLExecDirect query %d: %s\n", sqlquery, sql);
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
		char dec_str_val[42];
		SQL_NUMERIC_STRUCT dec_num_val;
		SQLLEN vallen = 0;
		SQLLEN NumAttr = 0;

		/* retrieve query result column metadata */
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_CONCISE_TYPE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_CONCISE_TYPE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);
		ret = SQLColAttribute(stmt, (UWORD)col, SQL_DESC_DISPLAY_SIZE, (PTR)&buf, (SQLLEN)20, NULL, &NumAttr);
		pos += snprintf(outp + pos, outp_len - pos, "SQLColAttribute(%d, SQL_DESC_DISPLAY_SIZE) returns %d, NumAttr " LLFMT "\n", col, ret, (int64_t) NumAttr);

		/* test SQLGetData(SQL_C_CHAR) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)SQL_C_CHAR, (PTR)&dec_str_val, (SQLLEN)42, &vallen);
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, SQL_C_CHAR, 42) returns %d, vallen " LLFMT ", str_val: '%s'\n",
			col, ret, (int64_t) vallen, (vallen == SQL_NULL_DATA) ? "NULL" : dec_str_val);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col)");

		/* test SQLGetData(SQL_C_NUMERIC) */
		ret = SQLGetData(stmt, (UWORD)col, (SWORD)SQL_C_NUMERIC, (PTR)&dec_num_val, (SQLLEN)sizeof(SQL_NUMERIC_STRUCT), &vallen);
		pos += snprintf(outp + pos, outp_len - pos, "SQLGetData(%d, SQL_C_NUMERIC, 19) returns %d, vallen " LLFMT ", data_val: ", col, ret, (int64_t) vallen);
		if (ret == SQL_SUCCESS) {
			if (vallen == SQL_NULL_DATA)
				pos += snprintf(outp + pos, outp_len - pos, "NULL\n");
			else {
				pos += snprintf(outp + pos, outp_len - pos, "%u %u %u %u %u %u %u %u %u %u %u %u %u %u %u %u\n",
					(uint8_t)dec_num_val.val[0], (uint8_t)dec_num_val.val[1], (uint8_t)dec_num_val.val[2], (uint8_t)dec_num_val.val[3],
					(uint8_t)dec_num_val.val[4], (uint8_t)dec_num_val.val[5], (uint8_t)dec_num_val.val[6], (uint8_t)dec_num_val.val[7],
					(uint8_t)dec_num_val.val[8], (uint8_t)dec_num_val.val[9], (uint8_t)dec_num_val.val[10], (uint8_t)dec_num_val.val[11],
					(uint8_t)dec_num_val.val[12], (uint8_t)dec_num_val.val[13], (uint8_t)dec_num_val.val[14], (uint8_t)dec_num_val.val[15]);
			}
		}
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(col)");
	}

	compareResult("testGetDataDecimal()", outp,
		(sqlquery == 1)
		?	"SQLExecDirect query 1: select cast(99999999909999999990999999999012345678. as decimal(38,0)) as val1, cast(-99999999909999999990999999999012345678. as decimal(38,0)) as val2;\n"
			"SQLRowCount is 1\nSQLNumResultCols is 2\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(1, SQL_C_CHAR, 42) returns 0, vallen 38, str_val: '99999999909999999990999999999012345678'\n"
			"SQLGetData(1, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 0 0 96 117 10 24 156 203 180 104 23 167 76 59 75\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(2, SQL_C_CHAR, 42) returns 0, vallen 39, str_val: '-99999999909999999990999999999012345678'\n"
			"SQLGetData(2, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 0 0 96 117 10 24 156 203 180 104 23 167 76 59 75\n"
		: (sqlquery == 2)
		?	"SQLExecDirect query 2: select cast(92345678901234567890.123456789 as decimal(29,9)) as val1, cast(-92345678901234567890.123456789 as decimal(29,9)) as val2;\n"
			"SQLRowCount is 1\nSQLNumResultCols is 2\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 31\n"
			"SQLGetData(1, SQL_C_CHAR, 42) returns 0, vallen 30, str_val: '92345678901234567890.123456789'\n"
			"SQLGetData(1, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 8 201 240 176 193 141 1 5 0 0 0 0 0 0 0\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 31\n"
			"SQLGetData(2, SQL_C_CHAR, 42) returns 0, vallen 31, str_val: '-92345678901234567890.123456789'\n"
			"SQLGetData(2, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 8 201 240 176 193 141 1 5 0 0 0 0 0 0 0\n"
		: (sqlquery == 3)
		?	"SQLExecDirect query 3: select cast(92345678901234567890.123456789012345678 as decimal(38,18)) as val1, cast(-9234567890123456789.1234567890123456789 as decimal(38,19)) as val2;\n"
			"SQLRowCount is 1\nSQLNumResultCols is 2\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(1, SQL_C_CHAR, 42) returns 0, vallen 39, str_val: '92345678901234567890.123456789012345678'\n"
			"SQLGetData(1, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 8 201 240 176 193 141 1 5 0 0 0 0 0 0 0\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(2, SQL_C_CHAR, 42) returns 0, vallen 40, str_val: '-9234567890123456789.1234567890123456789'\n"
			"SQLGetData(2, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 180 173 177 145 198 39 128 0 0 0 0 0 0 0 0\n"
		: (sqlquery == 4)
		?	"SQLExecDirect query 4: select cast(987654321.12345678901234567890123456789 as decimal(38,29)) as val1, cast(-987654321.12345678901234567890123456789 as decimal(38,29)) as val2;\n"
			"SQLRowCount is 1\nSQLNumResultCols is 2\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(1, SQL_C_CHAR, 42) returns 0, vallen 39, str_val: '987654321.12345678901234567890123456789'\n"
			"SQLGetData(1, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 177 104 222 58 0 0 0 0 0 0 0 0 0 0 0 0\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(2, SQL_C_CHAR, 42) returns 0, vallen 40, str_val: '-987654321.12345678901234567890123456789'\n"
			"SQLGetData(2, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 177 104 222 58 0 0 0 0 0 0 0 0 0 0 0 0\n"
		:	"SQLExecDirect query 5: select cast(.99999999909999999990999999999012345678 as decimal(38,38)) as val1, cast(-.99999999909999999990999999999012345678 as decimal(38,38)) as val2;\n"
			"SQLRowCount is 1\nSQLNumResultCols is 2\nSQLFetch\n"
			"SQLColAttribute(1, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(1, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(1, SQL_C_CHAR, 42) returns 0, vallen 40, str_val: '0.99999999909999999990999999999012345678'\n"
			"SQLGetData(1, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
			"SQLColAttribute(2, SQL_DESC_CONCISE_TYPE) returns 0, NumAttr 3\n"
			"SQLColAttribute(2, SQL_DESC_DISPLAY_SIZE) returns 0, NumAttr 40\n"
			"SQLGetData(2, SQL_C_CHAR, 42) returns 0, vallen 41, str_val: '-0.99999999909999999990999999999012345678'\n"
			"SQLGetData(2, SQL_C_NUMERIC, 19) returns 0, vallen 19, data_val: 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n"
		);

	ret = SQLCloseCursor(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");

	/* cleanup */
	free(outp);
	return ret;
}
#endif

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
	if (!SQL_SUCCEEDED(ret)) {
		fprintf(stderr, "Cannot allocate ODBC environment handle!\n");
		exit(1);
	}

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) (uintptr_t) SQL_OV_ODBC3, 0);
	check(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr (SQL_ATTR_ODBC_VERSION ODBC3)");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	check(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle (DBC)");

	ret = SQLConnect(dbc, (SQLCHAR *) dsn, SQL_NTS, (SQLCHAR *) user, SQL_NTS, (SQLCHAR *) pass, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLConnect");
	if (!SQL_SUCCEEDED(ret)) {
		fprintf(stderr, "Cannot connect!\n");
		exit(1);
	}

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle (STMT)");

	/**** run tests ****/
	ret = testGetDataTruncatedString(stmt, SQL_C_CHAR);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataTruncatedString(STMT, SQL_C_CHAR)");
	ret = testGetDataTruncatedString(stmt, SQL_C_WCHAR);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataTruncatedString(STMT, SQL_C_WCHAR)");

	ret = testGetDataGUID(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataGUID(STMT)");

	ret = testGetDataIntervalDay(stmt, 1);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataIntervalDay(STMT, 99, -99)");
	ret = testGetDataIntervalDay(stmt, 2);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataIntervalDay(STMT, 101, -102)");

#ifdef HAVE_HGE
	ret = testGetDataDecimal(stmt, 1);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataDecimal(STMT, dec(38,0))");
	ret = testGetDataDecimal(stmt, 2);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataDecimal(STMT, dec(29,9))");
	ret = testGetDataDecimal(stmt, 3);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataDecimal(STMT, dec(38,19))");
	ret = testGetDataDecimal(stmt, 4);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataDecimal(STMT, dec(38,29))");
	ret = testGetDataDecimal(stmt, 5);
	check(ret, SQL_HANDLE_STMT, stmt, "testGetDataDecimal(STMT, dec(38,38))");
#endif

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
