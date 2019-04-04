/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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

int
main(int argc, char **argv)
{
	SQLHANDLE env;
	SQLHANDLE dbc;
	char *dsn = "MonetDB";
	char *user = "monetdb";
	char *pass = "monetdb";
	SQLRETURN ret;
	char str[2048];
	SQLSMALLINT resultlen;
	SQLUSMALLINT si;
	SQLUINTEGER i;

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

	if (SQLAllocHandle(SQL_HANDLE_ENV, NULL, &env) != SQL_SUCCESS) {
		fprintf(stderr, "Cannot allocate ODBC environment handle\n");
		exit(1);
	}

	ret = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) (uintptr_t) SQL_OV_ODBC3, 0);
	check(ret, SQL_HANDLE_ENV, env, "SQLSetEnvAttr");

	ret = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
	check(ret, SQL_HANDLE_ENV, env, "SQLAllocHandle 1");

	ret = SQLConnect(dbc, (SQLCHAR *) dsn, SQL_NTS, (SQLCHAR *) user, SQL_NTS, (SQLCHAR *) pass, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLConnect");

	ret = SQLGetInfo(dbc, SQL_ACCESSIBLE_PROCEDURES, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ACCESSIBLE_PROCEDURES: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_ACCESSIBLE_TABLES, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ACCESSIBLE_TABLES: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_ACTIVE_ENVIRONMENTS, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ACTIVE_ENVIRONMENTS: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_AGGREGATE_FUNCTIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_AGGREGATE_FUNCTIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_ALTER_DOMAIN, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ALTER_DOMAIN: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_ALTER_TABLE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ALTER_TABLE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_ASYNC_MODE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ASYNC_MODE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_BATCH_ROW_COUNT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_BATCH_ROW_COUNT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_BATCH_SUPPORT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_BATCH_SUPPORT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_BOOKMARK_PERSISTENCE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_BOOKMARK_PERSISTENCE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CATALOG_LOCATION, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CATALOG_LOCATION: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_CATALOG_NAME_SEPARATOR, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CATALOG_NAME_SEPARATOR: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_CATALOG_NAME, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CATALOG_NAME: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_CATALOG_TERM, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CATALOG_TERM: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_CATALOG_USAGE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CATALOG_USAGE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_COLLATION_SEQ, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_COLLATION_SEQ: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_COLUMN_ALIAS, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_COLUMN_ALIAS: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_CONCAT_NULL_BEHAVIOR, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONCAT_NULL_BEHAVIOR: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_CONVERT_BIGINT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_BIGINT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_BINARY, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_BINARY: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_BIT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_BIT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_CHAR, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_CHAR: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_DATE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_DATE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_DECIMAL, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_DECIMAL: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_DOUBLE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_DOUBLE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_FLOAT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_FLOAT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_FUNCTIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_FUNCTIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_INTEGER, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_INTEGER: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_INTERVAL_DAY_TIME, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_INTERVAL_DAY_TIME: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_INTERVAL_YEAR_MONTH, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_INTERVAL_YEAR_MONTH: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_LONGVARBINARY, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_LONGVARBINARY: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_LONGVARCHAR, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_LONGVARCHAR: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_NUMERIC, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_NUMERIC: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_REAL, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_REAL: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_SMALLINT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_SMALLINT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_TIME, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_TIME: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_TIMESTAMP, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_TIMESTAMP: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_TINYINT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_TINYINT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_VARBINARY, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_VARBINARY: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CONVERT_VARCHAR, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CONVERT_VARCHAR: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CORRELATION_NAME, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CORRELATION_NAME: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_CREATE_SCHEMA, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CREATE_SCHEMA: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CREATE_TABLE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CREATE_TABLE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CREATE_TRANSLATION, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CREATE_TRANSLATION: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CREATE_VIEW, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CREATE_VIEW: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_CURSOR_COMMIT_BEHAVIOR, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CURSOR_COMMIT_BEHAVIOR: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_CURSOR_ROLLBACK_BEHAVIOR, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CURSOR_ROLLBACK_BEHAVIOR: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_CURSOR_SENSITIVITY, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_CURSOR_SENSITIVITY: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DATABASE_NAME, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DATABASE_NAME: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DATA_SOURCE_NAME, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DATA_SOURCE_NAME: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DATA_SOURCE_READ_ONLY, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DATA_SOURCE_READ_ONLY: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DBMS_NAME, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DBMS_NAME: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DBMS_VER, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("#SQL_DBMS_VER: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DDL_INDEX, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DDL_INDEX: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DEFAULT_TXN_ISOLATION, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DEFAULT_TXN_ISOLATION: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DESCRIBE_PARAMETER, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DESCRIBE_PARAMETER: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DM_VER, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DM_VER: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DRIVER_NAME, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DRIVER_NAME: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DRIVER_ODBC_VER, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DRIVER_ODBC_VER: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DRIVER_VER, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("#SQL_DRIVER_VER: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_DROP_ASSERTION, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_ASSERTION: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DROP_CHARACTER_SET, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_CHARACTER_SET: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DROP_COLLATION, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_COLLATION: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DROP_DOMAIN, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_DOMAIN: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DROP_SCHEMA, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_SCHEMA: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DROP_TABLE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_TABLE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DROP_TRANSLATION, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_TRANSLATION: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DROP_VIEW, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DROP_VIEW: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DYNAMIC_CURSOR_ATTRIBUTES1: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_DYNAMIC_CURSOR_ATTRIBUTES2: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_EXPRESSIONS_IN_ORDERBY, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_EXPRESSIONS_IN_ORDERBY: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_FETCH_DIRECTION, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_FETCH_DIRECTION: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_FILE_USAGE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_FILE_USAGE: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_GETDATA_EXTENSIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_GETDATA_EXTENSIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_GROUP_BY, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_GROUP_BY: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_IDENTIFIER_CASE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_IDENTIFIER_CASE: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_IDENTIFIER_QUOTE_CHAR, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_IDENTIFIER_QUOTE_CHAR: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_INFO_SCHEMA_VIEWS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_INFO_SCHEMA_VIEWS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_INSERT_STATEMENT, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_INSERT_STATEMENT: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_INTEGRITY, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_INTEGRITY: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_KEYSET_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_KEYSET_CURSOR_ATTRIBUTES1: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_KEYSET_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_KEYSET_CURSOR_ATTRIBUTES2: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_KEYWORDS, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_KEYWORDS: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_LIKE_ESCAPE_CLAUSE, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_LIKE_ESCAPE_CLAUSE: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_LOCK_TYPES, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_LOCK_TYPES: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_ASYNC_CONCURRENT_STATEMENTS: %u\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_MAX_BINARY_LITERAL_LEN, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_BINARY_LITERAL_LEN: %u\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_MAX_CATALOG_NAME_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_CATALOG_NAME_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_CHAR_LITERAL_LEN, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_CHAR_LITERAL_LEN: %u\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMN_NAME_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_COLUMN_NAME_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_GROUP_BY, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_COLUMNS_IN_GROUP_BY: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_INDEX, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_COLUMNS_IN_INDEX: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_ORDER_BY, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_COLUMNS_IN_ORDER_BY: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_SELECT, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_COLUMNS_IN_SELECT: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_TABLE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_COLUMNS_IN_TABLE: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_CONCURRENT_ACTIVITIES, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_CONCURRENT_ACTIVITIES: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_CURSOR_NAME_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_CURSOR_NAME_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_DRIVER_CONNECTIONS, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_DRIVER_CONNECTIONS: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_IDENTIFIER_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_IDENTIFIER_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_INDEX_SIZE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_INDEX_SIZE: %u\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_MAX_PROCEDURE_NAME_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_PROCEDURE_NAME_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_ROW_SIZE_INCLUDES_LONG, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_ROW_SIZE_INCLUDES_LONG: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_MAX_ROW_SIZE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_ROW_SIZE: %u\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_MAX_SCHEMA_NAME_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_SCHEMA_NAME_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_STATEMENT_LEN, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_STATEMENT_LEN: %u\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_MAX_TABLE_NAME_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_TABLE_NAME_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_TABLES_IN_SELECT, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_TABLES_IN_SELECT: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MAX_USER_NAME_LEN, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MAX_USER_NAME_LEN: %u\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_MULTIPLE_ACTIVE_TXN, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MULTIPLE_ACTIVE_TXN: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_MULT_RESULT_SETS, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_MULT_RESULT_SETS: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_NEED_LONG_DATA_LEN, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_NEED_LONG_DATA_LEN: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_NON_NULLABLE_COLUMNS, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_NON_NULLABLE_COLUMNS: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_NULL_COLLATION, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_NULL_COLLATION: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_NUMERIC_FUNCTIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_NUMERIC_FUNCTIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_ODBC_API_CONFORMANCE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ODBC_API_CONFORMANCE: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_ODBC_INTERFACE_CONFORMANCE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ODBC_INTERFACE_CONFORMANCE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_ODBC_SAG_CLI_CONFORMANCE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ODBC_SAG_CLI_CONFORMANCE: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_ODBC_SQL_CONFORMANCE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ODBC_SQL_CONFORMANCE: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_ODBC_VER, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ODBC_VER: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_OJ_CAPABILITIES, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_OJ_CAPABILITIES: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_ORDER_BY_COLUMNS_IN_SELECT, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ORDER_BY_COLUMNS_IN_SELECT: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_OUTER_JOINS, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_OUTER_JOINS: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_PARAM_ARRAY_ROW_COUNTS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_PARAM_ARRAY_ROW_COUNTS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_PARAM_ARRAY_SELECTS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_PARAM_ARRAY_SELECTS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_POSITIONED_STATEMENTS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_POSITIONED_STATEMENTS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_POS_OPERATIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_POS_OPERATIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_PROCEDURES, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_PROCEDURES: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_PROCEDURE_TERM, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_PROCEDURE_TERM: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_QUOTED_IDENTIFIER_CASE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_QUOTED_IDENTIFIER_CASE: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_ROW_UPDATES, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_ROW_UPDATES: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_SCHEMA_TERM, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SCHEMA_TERM: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_SCHEMA_USAGE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SCHEMA_USAGE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_SCROLL_CONCURRENCY, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SCROLL_CONCURRENCY: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_SCROLL_OPTIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SCROLL_OPTIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_SEARCH_PATTERN_ESCAPE, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SEARCH_PATTERN_ESCAPE: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_SERVER_NAME, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SERVER_NAME: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_SPECIAL_CHARACTERS, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SPECIAL_CHARACTERS: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_SQL_CONFORMANCE, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SQL_CONFORMANCE: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_STATIC_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_STATIC_CURSOR_ATTRIBUTES1: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_STATIC_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_STATIC_CURSOR_ATTRIBUTES2: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_STRING_FUNCTIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_STRING_FUNCTIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_SUBQUERIES, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SUBQUERIES: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_SYSTEM_FUNCTIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_SYSTEM_FUNCTIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_TABLE_TERM, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_TABLE_TERM: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_TIMEDATE_ADD_INTERVALS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_TIMEDATE_ADD_INTERVALS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_TIMEDATE_DIFF_INTERVALS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_TIMEDATE_DIFF_INTERVALS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_TIMEDATE_FUNCTIONS, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_TIMEDATE_FUNCTIONS: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_TXN_CAPABLE, &si, sizeof(si), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_TXN_CAPABLE: 0x%x\n", (unsigned int) si);

	ret = SQLGetInfo(dbc, SQL_TXN_ISOLATION_OPTION, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_TXN_ISOLATION_OPTION: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_UNION, &i, sizeof(i), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_UNION: 0x%x\n", (unsigned int) i);

	ret = SQLGetInfo(dbc, SQL_USER_NAME, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_USER_NAME: %.*s\n", resultlen, str);

	ret = SQLGetInfo(dbc, SQL_XOPEN_CLI_YEAR, str, sizeof(str), &resultlen);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo");
	printf("SQL_XOPEN_CLI_YEAR: %.*s\n", resultlen, str);

	ret = SQLDisconnect(dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLFreeHandle (DBC)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	check(ret, SQL_HANDLE_ENV, env, "SQLFreeHandle (ENV)");

	return 0;
}
