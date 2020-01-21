/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#ifdef _MSC_VER
#include <WTypes.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>

/**** Define the ODBC Version this ODBC driver complies with ****/
/* also see ODBCGlobal.h */
#define ODBCVER 0x0352		/* Important: this must be defined before include of sqlext.h */

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

static bool
check(SQLRETURN ret, SQLSMALLINT tpe, SQLHANDLE hnd, const char *func)
{
	switch (ret) {
	case SQL_SUCCESS:
		return true;
	case SQL_SUCCESS_WITH_INFO:
		prerr(tpe, hnd, func, "Info");
		return true;
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
	return false;
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
	SQLUSMALLINT s;
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
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ACCESSIBLE_PROCEDURES")) {
		printf("SQL_ACCESSIBLE_PROCEDURES: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_ACCESSIBLE_TABLES, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ACCESSIBLE_TABLES")) {
		printf("SQL_ACCESSIBLE_TABLES: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_ACTIVE_ENVIRONMENTS, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ACTIVE_ENVIRONMENTS")) {
	}

	ret = SQLGetInfo(dbc, SQL_AGGREGATE_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_AGGREGATE_FUNCTIONS")) {
		printf("SQL_AGGREGATE_FUNCTIONS:");
		if (i & SQL_AF_ALL)
			printf(" SQL_AF_ALL");
		if (i & SQL_AF_AVG)
			printf(" SQL_AF_AVG");
		if (i & SQL_AF_COUNT)
			printf(" SQL_AF_COUNT");
		if (i & SQL_AF_DISTINCT)
			printf(" SQL_AF_DISTINCT");
		if (i & SQL_AF_MAX)
			printf(" SQL_AF_MAX");
		if (i & SQL_AF_MIN)
			printf(" SQL_AF_MIN");
		if (i & SQL_AF_SUM)
			printf(" SQL_AF_SUM");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_ALTER_DOMAIN, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ALTER_DOMAIN")) {
		printf("SQL_ALTER_DOMAIN:");
		if (i & SQL_AD_ADD_DOMAIN_CONSTRAINT)
			printf(" SQL_AD_ADD_DOMAIN_CONSTRAINT");
		if (i & SQL_AD_ADD_DOMAIN_DEFAULT)
			printf(" SQL_AD_ADD_DOMAIN_DEFAULT");
		if (i & SQL_AD_CONSTRAINT_NAME_DEFINITION)
			printf(" SQL_AD_CONSTRAINT_NAME_DEFINITION");
		if (i & SQL_AD_DROP_DOMAIN_CONSTRAINT)
			printf(" SQL_AD_DROP_DOMAIN_CONSTRAINT");
		if (i & SQL_AD_DROP_DOMAIN_DEFAULT)
			printf(" SQL_AD_DROP_DOMAIN_DEFAULT");
		if (i & SQL_AD_ADD_CONSTRAINT_DEFERRABLE)
			printf(" SQL_AD_ADD_CONSTRAINT_DEFERRABLE");
		if (i & SQL_AD_ADD_CONSTRAINT_NON_DEFERRABLE)
			printf(" SQL_AD_ADD_CONSTRAINT_NON_DEFERRABLE");
		if (i & SQL_AD_ADD_CONSTRAINT_INITIALLY_DEFERRED)
			printf(" SQL_AD_ADD_CONSTRAINT_INITIALLY_DEFERRED");
		if (i & SQL_AD_ADD_CONSTRAINT_INITIALLY_IMMEDIATE)
			printf(" SQL_AD_ADD_CONSTRAINT_INITIALLY_IMMEDIATE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_ALTER_TABLE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ALTER_TABLE")) {
		printf("SQL_ALTER_TABLE:");
		if (i & SQL_AT_ADD_COLUMN_COLLATION)
			printf(" SQL_AT_ADD_COLUMN_COLLATION");
		if (i & SQL_AT_ADD_COLUMN_DEFAULT)
			printf(" SQL_AT_ADD_COLUMN_DEFAULT");
		if (i & SQL_AT_ADD_COLUMN_SINGLE)
			printf(" SQL_AT_ADD_COLUMN_SINGLE");
		if (i & SQL_AT_ADD_CONSTRAINT)
			printf(" SQL_AT_ADD_CONSTRAINT");
		if (i & SQL_AT_ADD_TABLE_CONSTRAINT)
			printf(" SQL_AT_ADD_TABLE_CONSTRAINT");
		if (i & SQL_AT_CONSTRAINT_NAME_DEFINITION)
			printf(" SQL_AT_CONSTRAINT_NAME_DEFINITION");
		if (i & SQL_AT_DROP_COLUMN_CASCADE)
			printf(" SQL_AT_DROP_COLUMN_CASCADE");
		if (i & SQL_AT_DROP_COLUMN_DEFAULT)
			printf(" SQL_AT_DROP_COLUMN_DEFAULT");
		if (i & SQL_AT_DROP_COLUMN_RESTRICT)
			printf(" SQL_AT_DROP_COLUMN_RESTRICT");
		if (i & SQL_AT_DROP_TABLE_CONSTRAINT_CASCADE)
			printf(" SQL_AT_DROP_TABLE_CONSTRAINT_CASCADE");
		if (i & SQL_AT_DROP_TABLE_CONSTRAINT_RESTRICT)
			printf(" SQL_AT_DROP_TABLE_CONSTRAINT_RESTRICT");
		if (i & SQL_AT_SET_COLUMN_DEFAULT)
			printf(" SQL_AT_SET_COLUMN_DEFAULT");
		if (i & SQL_AT_CONSTRAINT_INITIALLY_DEFERRED)
			printf(" SQL_AT_CONSTRAINT_INITIALLY_DEFERRED");
		if (i & SQL_AT_CONSTRAINT_INITIALLY_IMMEDIATE)
			printf(" SQL_AT_CONSTRAINT_INITIALLY_IMMEDIATE");
		if (i & SQL_AT_CONSTRAINT_DEFERRABLE)
			printf(" SQL_AT_CONSTRAINT_DEFERRABLE");
		if (i & SQL_AT_CONSTRAINT_NON_DEFERRABLE)
			printf(" SQL_AT_CONSTRAINT_NON_DEFERRABLE");
		printf("\n");
	}

#ifdef SQL_ASYNC_DBC_FUNCTIONS
	ret = SQLGetInfo(dbc, SQL_ASYNC_DBC_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ASYNC_DBC_FUNCTIONS")) {
		printf("SQL_ASYNC_DBC_FUNCTIONS: ");
		switch (i) {
		case SQL_ASYNC_DBC_CAPABLE:
			printf("SQL_ASYNC_DBC_CAPABLE");
			break;
		case SQL_ASYNC_DBC_NOT_CAPABLE:
			printf("SQL_ASYNC_DBC_NOT_CAPABLE");
			break;
		}
		printf("\n");
	}
#endif

	ret = SQLGetInfo(dbc, SQL_ASYNC_MODE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ASYNC_MODE")) {
		printf("SQL_ASYNC_MODE: ");
		switch (i) {
		case SQL_AM_CONNECTION:
			printf("SQL_AM_CONNECTION");
			break;
		case SQL_AM_STATEMENT:
			printf("SQL_AM_STATEMENT");
			break;
		case SQL_AM_NONE:
			printf("SQL_AM_NONE");
			break;
		}
		printf("\n");
	}

#ifdef SQL_ASYNC_NOTIFICATION
	ret = SQLGetInfo(dbc, SQL_ASYNC_NOTIFICATION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ASYNC_NOTIFICATION")) {
		printf("SQL_ASYNC_NOTIFICATION: ");
		switch (i) {
		case SQL_ASYNC_NOTIFICATION_CAPABLE:
			printf("SQL_ASYNC_NOTIFICATION_CAPABLE");
			break;
		case SQL_ASYNC_NOTIFICATION_NOT_CAPABLE:
			printf("SQL_ASYNC_NOTIFICATION_NOT_CAPABLE");
			break;
		}
		printf("\n");
	}
#endif

	ret = SQLGetInfo(dbc, SQL_BATCH_ROW_COUNT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_BATCH_ROW_COUNT")) {
		printf("SQL_BATCH_ROW_COUNT:");
		if (i & SQL_BRC_ROLLED_UP)
			printf(" SQL_BRC_ROLLED_UP");
		if (i & SQL_BRC_PROCEDURES)
			printf(" SQL_BRC_PROCEDURES");
		if (i & SQL_BRC_EXPLICIT)
			printf(" SQL_BRC_EXPLICIT");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_BATCH_SUPPORT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_BATCH_SUPPORT")) {
		printf("SQL_BATCH_SUPPORT:");
		if (i & SQL_BS_SELECT_EXPLICIT)
			printf(" SQL_BS_SELECT_EXPLICIT");
		if (i & SQL_BS_ROW_COUNT_EXPLICIT)
			printf(" SQL_BS_ROW_COUNT_EXPLICIT");
		if (i & SQL_BS_SELECT_PROC)
			printf(" SQL_BS_SELECT_PROC");
		if (i & SQL_BS_ROW_COUNT_PROC)
			printf(" SQL_BS_ROW_COUNT_PROC");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_BOOKMARK_PERSISTENCE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_BOOKMARK_PERSISTENCE")) {
		printf("SQL_BOOKMARK_PERSISTENCE:");
		if (i & SQL_BP_CLOSE)
			printf(" SQL_BP_CLOSE");
		if (i & SQL_BP_DELETE)
			printf(" SQL_BP_DELETE");
		if (i & SQL_BP_DROP)
			printf(" SQL_BP_DROP");
		if (i & SQL_BP_TRANSACTION)
			printf(" SQL_BP_TRANSACTION");
		if (i & SQL_BP_UPDATE)
			printf(" SQL_BP_UPDATE");
		if (i & SQL_BP_OTHER_HSTMT)
			printf(" SQL_BP_OTHER_HSTMT");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CATALOG_LOCATION, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CATALOG_LOCATION")) {
		printf("SQL_CATALOG_LOCATION: ");
		switch (s) {
		case SQL_CL_START:
			printf("SQL_CL_START");
			break;
		case SQL_CL_END:
			printf("SQL_CL_END");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CATALOG_NAME, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CATALOG_NAME")) {
		printf("SQL_CATALOG_NAME: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_CATALOG_NAME_SEPARATOR, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CATALOG_NAME_SEPARATOR")) {
		printf("SQL_CATALOG_NAME_SEPARATOR: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_CATALOG_TERM, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CATALOG_TERM")) {
		printf("SQL_CATALOG_TERM: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_CATALOG_USAGE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CATALOG_USAGE")) {
		printf("SQL_CATALOG_USAGE:");
		if (i & SQL_CU_DML_STATEMENTS)
			printf(" SQL_CU_DML_STATEMENTS");
		if (i & SQL_CU_PROCEDURE_INVOCATION)
			printf(" SQL_CU_PROCEDURE_INVOCATION");
		if (i & SQL_CU_TABLE_DEFINITION)
			printf(" SQL_CU_TABLE_DEFINITION");
		if (i & SQL_CU_INDEX_DEFINITION)
			printf(" SQL_CU_INDEX_DEFINITION");
		if (i & SQL_CU_PRIVILEGE_DEFINITION)
			printf(" SQL_CU_PRIVILEGE_DEFINITION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_COLLATION_SEQ, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_COLLATION_SEQ")) {
		printf("SQL_COLLATION_SEQ: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_COLUMN_ALIAS, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_COLUMN_ALIAS")) {
		printf("SQL_COLUMN_ALIAS: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_CONCAT_NULL_BEHAVIOR, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONCAT_NULL_BEHAVIOR")) {
		printf("SQL_CONCAT_NULL_BEHAVIOR: ");
		switch (s) {
		case SQL_CB_NULL:
			printf("SQL_CB_NULL");
			break;
		case SQL_CB_NON_NULL:
			printf("SQL_CB_NON_NULL");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_BIGINT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_BIGINT")) {
		printf("SQL_CONVERT_BIGINT:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_BINARY, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_BINARY")) {
		printf("SQL_CONVERT_BINARY:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_BIT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_BIT")) {
		printf("SQL_CONVERT_BIT:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_CHAR, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_CHAR")) {
		printf("SQL_CONVERT_CHAR:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_DATE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_DATE")) {
		printf("SQL_CONVERT_DATE:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_DECIMAL, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_DECIMAL")) {
		printf("SQL_CONVERT_DECIMAL:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_DOUBLE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_DOUBLE")) {
		printf("SQL_CONVERT_DOUBLE:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_FLOAT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_FLOAT")) {
		printf("SQL_CONVERT_FLOAT:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_FUNCTIONS")) {
		printf("SQL_CONVERT_FUNCTIONS:");
		if (i & SQL_FN_CVT_CAST)
			printf(" SQL_FN_CVT_CAST");
		if (i & SQL_FN_CVT_CONVERT)
			printf(" SQL_FN_CVT_CONVERT");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_INTEGER, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_INTEGER")) {
		printf("SQL_CONVERT_INTEGER:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_INTERVAL_DAY_TIME, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_INTERVAL_DAY_TIME")) {
		printf("SQL_CONVERT_INTERVAL_DAY_TIME:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_INTERVAL_YEAR_MONTH, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_INTERVAL_YEAR_MONTH")) {
		printf("SQL_CONVERT_INTERVAL_YEAR_MONTH:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_LONGVARBINARY, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_LONGVARBINARY")) {
		printf("SQL_CONVERT_LONGVARBINARY:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_LONGVARCHAR, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_LONGVARCHAR")) {
		printf("SQL_CONVERT_LONGVARCHAR:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_NUMERIC, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_NUMERIC")) {
		printf("SQL_CONVERT_NUMERIC:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_REAL, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_REAL")) {
		printf("SQL_CONVERT_REAL:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_SMALLINT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_SMALLINT")) {
		printf("SQL_CONVERT_SMALLINT:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_TIME, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_TIME")) {
		printf("SQL_CONVERT_TIME:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_TIMESTAMP, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_TIMESTAMP")) {
		printf("SQL_CONVERT_TIMESTAMP:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_TINYINT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_TINYINT")) {
		printf("SQL_CONVERT_TINYINT:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_VARBINARY, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_VARBINARY")) {
		printf("SQL_CONVERT_VARBINARY:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CONVERT_VARCHAR, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CONVERT_VARCHAR")) {
		printf("SQL_CONVERT_VARCHAR:");
		if (i & SQL_CVT_BIGINT)
			printf(" SQL_CVT_BIGINT");
		if (i & SQL_CVT_BINARY)
			printf(" SQL_CVT_BINARY");
		if (i & SQL_CVT_BIT)
			printf(" SQL_CVT_BIT");
		if (i & SQL_CVT_GUID)
			printf(" SQL_CVT_GUID");
		if (i & SQL_CVT_CHAR)
			printf(" SQL_CVT_CHAR");
		if (i & SQL_CVT_DATE)
			printf(" SQL_CVT_DATE");
		if (i & SQL_CVT_DECIMAL)
			printf(" SQL_CVT_DECIMAL");
		if (i & SQL_CVT_DOUBLE)
			printf(" SQL_CVT_DOUBLE");
		if (i & SQL_CVT_FLOAT)
			printf(" SQL_CVT_FLOAT");
		if (i & SQL_CVT_INTEGER)
			printf(" SQL_CVT_INTEGER");
		if (i & SQL_CVT_INTERVAL_YEAR_MONTH)
			printf(" SQL_CVT_INTERVAL_YEAR_MONTH");
		if (i & SQL_CVT_INTERVAL_DAY_TIME)
			printf(" SQL_CVT_INTERVAL_DAY_TIME");
		if (i & SQL_CVT_LONGVARBINARY)
			printf(" SQL_CVT_LONGVARBINARY");
		if (i & SQL_CVT_LONGVARCHAR)
			printf(" SQL_CVT_LONGVARCHAR");
		if (i & SQL_CVT_NUMERIC)
			printf(" SQL_CVT_NUMERIC");
		if (i & SQL_CVT_REAL)
			printf(" SQL_CVT_REAL");
		if (i & SQL_CVT_SMALLINT)
			printf(" SQL_CVT_SMALLINT");
		if (i & SQL_CVT_TIME)
			printf(" SQL_CVT_TIME");
		if (i & SQL_CVT_TIMESTAMP)
			printf(" SQL_CVT_TIMESTAMP");
		if (i & SQL_CVT_TINYINT)
			printf(" SQL_CVT_TINYINT");
		if (i & SQL_CVT_VARBINARY)
			printf(" SQL_CVT_VARBINARY");
		if (i & SQL_CVT_VARCHAR)
			printf(" SQL_CVT_VARCHAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CORRELATION_NAME, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CORRELATION_NAME")) {
		printf("SQL_CORRELATION_NAME: ");
		switch (s) {
		case SQL_CN_NONE:
			printf("SQL_CN_NONE");
			break;
		case SQL_CN_DIFFERENT:
			printf("SQL_CN_DIFFERENT");
			break;
		case SQL_CN_ANY:
			printf("SQL_CN_ANY");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_ASSERTION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_ASSERTION")) {
		printf("SQL_CREATE_ASSERTION:");
		if (i & SQL_CA_CREATE_ASSERTION)
			printf(" SQL_CA_CREATE_ASSERTION");
		if (i & SQL_CA_CONSTRAINT_INITIALLY_DEFERRED)
			printf(" SQL_CA_CONSTRAINT_INITIALLY_DEFERRED");
		if (i & SQL_CA_CONSTRAINT_INITIALLY_IMMEDIATE)
			printf(" SQL_CA_CONSTRAINT_INITIALLY_IMMEDIATE");
		if (i & SQL_CA_CONSTRAINT_DEFERRABLE)
			printf(" SQL_CA_CONSTRAINT_DEFERRABLE");
		if (i & SQL_CA_CONSTRAINT_NON_DEFERRABLE)
			printf(" SQL_CA_CONSTRAINT_NON_DEFERRABLE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_CHARACTER_SET, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_CHARACTER_SET")) {
		printf("SQL_CREATE_CHARACTER_SET:");
		if (i & SQL_CCS_CREATE_CHARACTER_SET)
			printf(" SQL_CCS_CREATE_CHARACTER_SET");
		if (i & SQL_CCS_COLLATE_CLAUSE)
			printf(" SQL_CCS_COLLATE_CLAUSE");
		if (i & SQL_CCS_LIMITED_COLLATION)
			printf(" SQL_CCS_LIMITED_COLLATION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_COLLATION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_COLLATION")) {
		printf("SQL_CREATE_COLLATION:");
		if (i & SQL_CCOL_CREATE_COLLATION)
			printf(" SQL_CCOL_CREATE_COLLATION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_DOMAIN, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_DOMAIN")) {
		printf("SQL_CREATE_DOMAIN:");
		if (i & SQL_CDO_CREATE_DOMAIN)
			printf(" SQL_CDO_CREATE_DOMAIN");
		if (i & SQL_CDO_CONSTRAINT_NAME_DEFINITION)
			printf(" SQL_CDO_CONSTRAINT_NAME_DEFINITION");
		if (i & SQL_CDO_DEFAULT)
			printf(" SQL_CDO_DEFAULT");
		if (i & SQL_CDO_CONSTRAINT)
			printf(" SQL_CDO_CONSTRAINT");
		if (i & SQL_CDO_COLLATION)
			printf(" SQL_CDO_COLLATION");
		if (i & SQL_CDO_CONSTRAINT_INITIALLY_DEFERRED)
			printf(" SQL_CDO_CONSTRAINT_INITIALLY_DEFERRED");
		if (i & SQL_CDO_CONSTRAINT_INITIALLY_IMMEDIATE)
			printf(" SQL_CDO_CONSTRAINT_INITIALLY_IMMEDIATE");
		if (i & SQL_CDO_CONSTRAINT_DEFERRABLE)
			printf(" SQL_CDO_CONSTRAINT_DEFERRABLE");
		if (i & SQL_CDO_CONSTRAINT_NON_DEFERRABLE)
			printf(" SQL_CDO_CONSTRAINT_NON_DEFERRABLE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_SCHEMA, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_SCHEMA")) {
		printf("SQL_CREATE_SCHEMA:");
		if (i & SQL_CS_CREATE_SCHEMA)
			printf(" SQL_CS_CREATE_SCHEMA");
		if (i & SQL_CS_AUTHORIZATION)
			printf(" SQL_CS_AUTHORIZATION");
		if (i & SQL_CS_DEFAULT_CHARACTER_SET)
			printf(" SQL_CS_DEFAULT_CHARACTER_SET");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_TABLE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_TABLE")) {
		printf("SQL_CREATE_TABLE:");
		if (i & SQL_CT_CREATE_TABLE)
			printf(" SQL_CT_CREATE_TABLE");
		if (i & SQL_CT_TABLE_CONSTRAINT)
			printf(" SQL_CT_TABLE_CONSTRAINT");
		if (i & SQL_CT_CONSTRAINT_NAME_DEFINITION)
			printf(" SQL_CT_CONSTRAINT_NAME_DEFINITION");
		if (i & SQL_CT_COMMIT_PRESERVE)
			printf(" SQL_CT_COMMIT_PRESERVE");
		if (i & SQL_CT_COMMIT_DELETE)
			printf(" SQL_CT_COMMIT_DELETE");
		if (i & SQL_CT_GLOBAL_TEMPORARY)
			printf(" SQL_CT_GLOBAL_TEMPORARY");
		if (i & SQL_CT_LOCAL_TEMPORARY)
			printf(" SQL_CT_LOCAL_TEMPORARY");
		if (i & SQL_CT_COLUMN_CONSTRAINT)
			printf(" SQL_CT_COLUMN_CONSTRAINT");
		if (i & SQL_CT_COLUMN_DEFAULT)
			printf(" SQL_CT_COLUMN_DEFAULT");
		if (i & SQL_CT_COLUMN_COLLATION)
			printf(" SQL_CT_COLUMN_COLLATION");
		if (i & SQL_CT_CONSTRAINT_INITIALLY_DEFERRED)
			printf(" SQL_CT_CONSTRAINT_INITIALLY_DEFERRED");
		if (i & SQL_CT_CONSTRAINT_INITIALLY_IMMEDIATE)
			printf(" SQL_CT_CONSTRAINT_INITIALLY_IMMEDIATE");
		if (i & SQL_CT_CONSTRAINT_DEFERRABLE)
			printf(" SQL_CT_CONSTRAINT_DEFERRABLE");
		if (i & SQL_CT_CONSTRAINT_NON_DEFERRABLE)
			printf(" SQL_CT_CONSTRAINT_NON_DEFERRABLE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_TRANSLATION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_TRANSLATION")) {
		printf("SQL_CREATE_TRANSLATION:");
		if (i & SQL_CTR_CREATE_TRANSLATION)
			printf(" SQL_CTR_CREATE_TRANSLATION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CREATE_VIEW, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CREATE_VIEW")) {
		printf("SQL_CREATE_VIEW:");
		if (i & SQL_CV_CREATE_VIEW)
			printf(" SQL_CV_CREATE_VIEW");
		if (i & SQL_CV_CHECK_OPTION)
			printf(" SQL_CV_CHECK_OPTION");
		if (i & SQL_CV_CASCADED)
			printf(" SQL_CV_CASCADED");
		if (i & SQL_CV_LOCAL)
			printf(" SQL_CV_LOCAL");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CURSOR_COMMIT_BEHAVIOR, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CURSOR_COMMIT_BEHAVIOR")) {
		printf("SQL_CURSOR_COMMIT_BEHAVIOR: ");
		switch (s) {
		case SQL_CB_DELETE:
			printf("SQL_CB_DELETE");
			break;
		case SQL_CB_CLOSE:
			printf("SQL_CB_CLOSE");
			break;
		case SQL_CB_PRESERVE:
			printf("SQL_CB_PRESERVE");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CURSOR_ROLLBACK_BEHAVIOR, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CURSOR_ROLLBACK_BEHAVIOR")) {
		printf("SQL_CURSOR_ROLLBACK_BEHAVIOR: ");
		switch (s) {
		case SQL_CB_DELETE:
			printf("SQL_CB_DELETE");
			break;
		case SQL_CB_CLOSE:
			printf("SQL_CB_CLOSE");
			break;
		case SQL_CB_PRESERVE:
			printf("SQL_CB_PRESERVE");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_CURSOR_SENSITIVITY, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_CURSOR_SENSITIVITY")) {
		printf("SQL_CURSOR_SENSITIVITY: ");
		switch (i) {
		case SQL_INSENSITIVE:
			printf("SQL_INSENSITIVE");
			break;
		case SQL_UNSPECIFIED:
			printf("SQL_UNSPECIFIED");
			break;
		case SQL_SENSITIVE:
			printf("SQL_SENSITIVE");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DATA_SOURCE_NAME, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DATA_SOURCE_NAME")) {
		printf("SQL_DATA_SOURCE_NAME: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DATA_SOURCE_READ_ONLY, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DATA_SOURCE_READ_ONLY")) {
		printf("SQL_DATA_SOURCE_READ_ONLY: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DATABASE_NAME, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DATABASE_NAME")) {
		printf("SQL_DATABASE_NAME: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DATETIME_LITERALS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DATETIME_LITERALS")) {
		printf("SQL_DATETIME_LITERALS:");
		if (i & SQL_DL_SQL92_DATE)
			printf(" SQL_DL_SQL92_DATE");
		if (i & SQL_DL_SQL92_TIME)
			printf(" SQL_DL_SQL92_TIME");
		if (i & SQL_DL_SQL92_TIMESTAMP)
			printf(" SQL_DL_SQL92_TIMESTAMP");
		if (i & SQL_DL_SQL92_INTERVAL_YEAR)
			printf(" SQL_DL_SQL92_INTERVAL_YEAR");
		if (i & SQL_DL_SQL92_INTERVAL_MONTH)
			printf(" SQL_DL_SQL92_INTERVAL_MONTH");
		if (i & SQL_DL_SQL92_INTERVAL_DAY)
			printf(" SQL_DL_SQL92_INTERVAL_DAY");
		if (i & SQL_DL_SQL92_INTERVAL_HOUR)
			printf(" SQL_DL_SQL92_INTERVAL_HOUR");
		if (i & SQL_DL_SQL92_INTERVAL_MINUTE)
			printf(" SQL_DL_SQL92_INTERVAL_MINUTE");
		if (i & SQL_DL_SQL92_INTERVAL_SECOND)
			printf(" SQL_DL_SQL92_INTERVAL_SECOND");
		if (i & SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH)
			printf(" SQL_DL_SQL92_INTERVAL_YEAR_TO_MONTH");
		if (i & SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR)
			printf(" SQL_DL_SQL92_INTERVAL_DAY_TO_HOUR");
		if (i & SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE)
			printf(" SQL_DL_SQL92_INTERVAL_DAY_TO_MINUTE");
		if (i & SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND)
			printf(" SQL_DL_SQL92_INTERVAL_DAY_TO_SECOND");
		if (i & SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE)
			printf(" SQL_DL_SQL92_INTERVAL_HOUR_TO_MINUTE");
		if (i & SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND)
			printf(" SQL_DL_SQL92_INTERVAL_HOUR_TO_SECOND");
		if (i & SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND)
			printf(" SQL_DL_SQL92_INTERVAL_MINUTE_TO_SECOND");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DBMS_NAME, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DBMS_NAME")) {
		printf("SQL_DBMS_NAME: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DBMS_VER, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DBMS_VER")) {
		printf("#SQL_DBMS_VER: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DDL_INDEX, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DDL_INDEX")) {
		printf("SQL_DDL_INDEX: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_DEFAULT_TXN_ISOLATION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DEFAULT_TXN_ISOLATION")) {
		printf("SQL_DEFAULT_TXN_ISOLATION: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_DESCRIBE_PARAMETER, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DESCRIBE_PARAMETER")) {
		printf("SQL_DESCRIBE_PARAMETER: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DM_VER, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DM_VER")) {
		printf("SQL_DM_VER: %.*s\n", resultlen, str);
	}

#ifdef SQL_DRIVER_AWARE_POOLING_SUPPORTED
	ret = SQLGetInfo(dbc, SQL_DRIVER_AWARE_POOLING_SUPPORTED, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DRIVER_AWARE_POOLING_SUPPORTED")) {
		printf("SQL_DRIVER_AWARE_POOLING_SUPPORTED: %u\n", (unsigned int) i);
	}
#endif

	ret = SQLGetInfo(dbc, SQL_DRIVER_NAME, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DRIVER_NAME")) {
		printf("SQL_DRIVER_NAME: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DRIVER_ODBC_VER, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DRIVER_ODBC_VER")) {
		printf("SQL_DRIVER_ODBC_VER: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DRIVER_VER, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DRIVER_VER")) {
		printf("#SQL_DRIVER_VER: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_DROP_ASSERTION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_ASSERTION")) {
		printf("SQL_DROP_ASSERTION:");
		if (i & SQL_DA_DROP_ASSERTION)
			printf(" SQL_DA_DROP_ASSERTION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DROP_CHARACTER_SET, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_CHARACTER_SET")) {
		printf("SQL_DROP_CHARACTER_SET:");
		if (i & SQL_DCS_DROP_CHARACTER_SET)
			printf(" SQL_DCS_DROP_CHARACTER_SET");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DROP_COLLATION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_COLLATION")) {
		printf("SQL_DROP_COLLATION:");
		if (i & SQL_DC_DROP_COLLATION)
			printf(" SQL_DC_DROP_COLLATION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DROP_DOMAIN, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_DOMAIN")) {
		printf("SQL_DROP_DOMAIN:");
		if (i & SQL_DD_DROP_DOMAIN)
			printf(" SQL_DD_DROP_DOMAIN");
		if (i & SQL_DD_CASCADE)
			printf(" SQL_DD_CASCADE");
		if (i & SQL_DD_RESTRICT)
			printf(" SQL_DD_RESTRICT");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DROP_SCHEMA, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_SCHEMA")) {
		printf("SQL_DROP_SCHEMA:");
		if (i & SQL_DS_DROP_SCHEMA)
			printf(" SQL_DS_DROP_SCHEMA");
		if (i & SQL_DS_CASCADE)
			printf(" SQL_DS_CASCADE");
		if (i & SQL_DS_RESTRICT)
			printf(" SQL_DS_RESTRICT");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DROP_TABLE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_TABLE")) {
		printf("SQL_DROP_TABLE:");
		if (i & SQL_DT_DROP_TABLE)
			printf(" SQL_DT_DROP_TABLE");
		if (i & SQL_DT_CASCADE)
			printf(" SQL_DT_CASCADE");
		if (i & SQL_DT_RESTRICT)
			printf(" SQL_DT_RESTRICT");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DROP_TRANSLATION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_TRANSLATION")) {
		printf("SQL_DROP_TRANSLATION:");
		if (i & SQL_DTR_DROP_TRANSLATION)
			printf(" SQL_DTR_DROP_TRANSLATION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DROP_VIEW, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DROP_VIEW")) {
		printf("SQL_DROP_VIEW:");
		if (i & SQL_DV_DROP_VIEW)
			printf(" SQL_DV_DROP_VIEW");
		if (i & SQL_DV_CASCADE)
			printf(" SQL_DV_CASCADE");
		if (i & SQL_DV_RESTRICT)
			printf(" SQL_DV_RESTRICT");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DYNAMIC_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DYNAMIC_CURSOR_ATTRIBUTES1")) {
		printf("SQL_DYNAMIC_CURSOR_ATTRIBUTES1:");
		if (i & SQL_CA1_NEXT)
			printf(" SQL_CA1_NEXT");
		if (i & SQL_CA1_ABSOLUTE)
			printf(" SQL_CA1_ABSOLUTE");
		if (i & SQL_CA1_RELATIVE)
			printf(" SQL_CA1_RELATIVE");
		if (i & SQL_CA1_BOOKMARK)
			printf(" SQL_CA1_BOOKMARK");
		if (i & SQL_CA1_LOCK_EXCLUSIVE)
			printf(" SQL_CA1_LOCK_EXCLUSIVE");
		if (i & SQL_CA1_LOCK_NO_CHANGE)
			printf(" SQL_CA1_LOCK_NO_CHANGE");
		if (i & SQL_CA1_LOCK_UNLOCK)
			printf(" SQL_CA1_LOCK_UNLOCK");
		if (i & SQL_CA1_POS_POSITION)
			printf(" SQL_CA1_POS_POSITION");
		if (i & SQL_CA1_POS_UPDATE)
			printf(" SQL_CA1_POS_UPDATE");
		if (i & SQL_CA1_POS_DELETE)
			printf(" SQL_CA1_POS_DELETE");
		if (i & SQL_CA1_POS_REFRESH)
			printf(" SQL_CA1_POS_REFRESH");
		if (i & SQL_CA1_POSITIONED_UPDATE)
			printf(" SQL_CA1_POSITIONED_UPDATE");
		if (i & SQL_CA1_POSITIONED_DELETE)
			printf(" SQL_CA1_POSITIONED_DELETE");
		if (i & SQL_CA1_SELECT_FOR_UPDATE)
			printf(" SQL_CA1_SELECT_FOR_UPDATE");
		if (i & SQL_CA1_BULK_ADD)
			printf(" SQL_CA1_BULK_ADD");
		if (i & SQL_CA1_BULK_UPDATE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_UPDATE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_DELETE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_DELETE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_FETCH_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_FETCH_BY_BOOKMARK");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_DYNAMIC_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_DYNAMIC_CURSOR_ATTRIBUTES2")) {
		printf("SQL_DYNAMIC_CURSOR_ATTRIBUTES2:");
		if (i & SQL_CA2_READ_ONLY_CONCURRENCY)
			printf(" SQL_CA2_READ_ONLY_CONCURRENCY");
		if (i & SQL_CA2_LOCK_CONCURRENCY)
			printf(" SQL_CA2_LOCK_CONCURRENCY");
		if (i & SQL_CA2_OPT_ROWVER_CONCURRENCY)
			printf(" SQL_CA2_OPT_ROWVER_CONCURRENCY");
		if (i & SQL_CA2_OPT_VALUES_CONCURRENCY)
			printf(" SQL_CA2_OPT_VALUES_CONCURRENCY");
		if (i & SQL_CA2_SENSITIVITY_ADDITIONS)
			printf(" SQL_CA2_SENSITIVITY_ADDITIONS");
		if (i & SQL_CA2_SENSITIVITY_DELETIONS)
			printf(" SQL_CA2_SENSITIVITY_DELETIONS");
		if (i & SQL_CA2_SENSITIVITY_UPDATES)
			printf(" SQL_CA2_SENSITIVITY_UPDATES");
		if (i & SQL_CA2_MAX_ROWS_SELECT)
			printf(" SQL_CA2_MAX_ROWS_SELECT");
		if (i & SQL_CA2_MAX_ROWS_INSERT)
			printf(" SQL_CA2_MAX_ROWS_INSERT");
		if (i & SQL_CA2_MAX_ROWS_DELETE)
			printf(" SQL_CA2_MAX_ROWS_DELETE");
		if (i & SQL_CA2_MAX_ROWS_UPDATE)
			printf(" SQL_CA2_MAX_ROWS_UPDATE");
		if (i & SQL_CA2_MAX_ROWS_CATALOG)
			printf(" SQL_CA2_MAX_ROWS_CATALOG");
		if (i & SQL_CA2_MAX_ROWS_AFFECTS_ALL)
			printf(" SQL_CA2_MAX_ROWS_AFFECTS_ALL");
		if (i & SQL_CA2_CRC_EXACT)
			printf(" SQL_CA2_CRC_EXACT");
		if (i & SQL_CA2_CRC_APPROXIMATE)
			printf(" SQL_CA2_CRC_APPROXIMATE");
		if (i & SQL_CA2_SIMULATE_NON_UNIQUE)
			printf(" SQL_CA2_SIMULATE_NON_UNIQUE");
		if (i & SQL_CA2_SIMULATE_TRY_UNIQUE)
			printf(" SQL_CA2_SIMULATE_TRY_UNIQUE");
		if (i & SQL_CA2_SIMULATE_UNIQUE)
			printf(" SQL_CA2_SIMULATE_UNIQUE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_EXPRESSIONS_IN_ORDERBY, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_EXPRESSIONS_IN_ORDERBY")) {
		printf("SQL_EXPRESSIONS_IN_ORDERBY: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_FILE_USAGE, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_FILE_USAGE")) {
		printf("SQL_FILE_USAGE: ");
		switch (s) {
		case SQL_FILE_NOT_SUPPORTED:
			printf("SQL_FILE_NOT_SUPPORTED");
			break;
		case SQL_FILE_TABLE:
			printf("SQL_FILE_TABLE");
			break;
		case SQL_FILE_CATALOG:
			printf("SQL_FILE_CATALOG");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1")) {
		printf("SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:");
		if (i & SQL_CA1_NEXT)
			printf(" SQL_CA1_NEXT");
		if (i & SQL_CA1_LOCK_EXCLUSIVE)
			printf(" SQL_CA1_LOCK_EXCLUSIVE");
		if (i & SQL_CA1_LOCK_NO_CHANGE)
			printf(" SQL_CA1_LOCK_NO_CHANGE");
		if (i & SQL_CA1_LOCK_UNLOCK)
			printf(" SQL_CA1_LOCK_UNLOCK");
		if (i & SQL_CA1_POS_POSITION)
			printf(" SQL_CA1_POS_POSITION");
		if (i & SQL_CA1_POS_UPDATE)
			printf(" SQL_CA1_POS_UPDATE");
		if (i & SQL_CA1_POS_DELETE)
			printf(" SQL_CA1_POS_DELETE");
		if (i & SQL_CA1_POS_REFRESH)
			printf(" SQL_CA1_POS_REFRESH");
		if (i & SQL_CA1_POSITIONED_UPDATE)
			printf(" SQL_CA1_POSITIONED_UPDATE");
		if (i & SQL_CA1_POSITIONED_DELETE)
			printf(" SQL_CA1_POSITIONED_DELETE");
		if (i & SQL_CA1_SELECT_FOR_UPDATE)
			printf(" SQL_CA1_SELECT_FOR_UPDATE");
		if (i & SQL_CA1_BULK_ADD)
			printf(" SQL_CA1_BULK_ADD");
		if (i & SQL_CA1_BULK_UPDATE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_UPDATE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_DELETE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_DELETE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_FETCH_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_FETCH_BY_BOOKMARK");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2")) {
		printf("SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:");
		if (i & SQL_CA2_READ_ONLY_CONCURRENCY)
			printf(" SQL_CA2_READ_ONLY_CONCURRENCY");
		if (i & SQL_CA2_LOCK_CONCURRENCY)
			printf(" SQL_CA2_LOCK_CONCURRENCY");
		if (i & SQL_CA2_OPT_ROWVER_CONCURRENCY)
			printf(" SQL_CA2_OPT_ROWVER_CONCURRENCY");
		if (i & SQL_CA2_OPT_VALUES_CONCURRENCY)
			printf(" SQL_CA2_OPT_VALUES_CONCURRENCY");
		if (i & SQL_CA2_SENSITIVITY_ADDITIONS)
			printf(" SQL_CA2_SENSITIVITY_ADDITIONS");
		if (i & SQL_CA2_SENSITIVITY_DELETIONS)
			printf(" SQL_CA2_SENSITIVITY_DELETIONS");
		if (i & SQL_CA2_SENSITIVITY_UPDATES)
			printf(" SQL_CA2_SENSITIVITY_UPDATES");
		if (i & SQL_CA2_MAX_ROWS_SELECT)
			printf(" SQL_CA2_MAX_ROWS_SELECT");
		if (i & SQL_CA2_MAX_ROWS_INSERT)
			printf(" SQL_CA2_MAX_ROWS_INSERT");
		if (i & SQL_CA2_MAX_ROWS_DELETE)
			printf(" SQL_CA2_MAX_ROWS_DELETE");
		if (i & SQL_CA2_MAX_ROWS_UPDATE)
			printf(" SQL_CA2_MAX_ROWS_UPDATE");
		if (i & SQL_CA2_MAX_ROWS_CATALOG)
			printf(" SQL_CA2_MAX_ROWS_CATALOG");
		if (i & SQL_CA2_MAX_ROWS_AFFECTS_ALL)
			printf(" SQL_CA2_MAX_ROWS_AFFECTS_ALL");
		if (i & SQL_CA2_CRC_EXACT)
			printf(" SQL_CA2_CRC_EXACT");
		if (i & SQL_CA2_CRC_APPROXIMATE)
			printf(" SQL_CA2_CRC_APPROXIMATE");
		if (i & SQL_CA2_SIMULATE_NON_UNIQUE)
			printf(" SQL_CA2_SIMULATE_NON_UNIQUE");
		if (i & SQL_CA2_SIMULATE_TRY_UNIQUE)
			printf(" SQL_CA2_SIMULATE_TRY_UNIQUE");
		if (i & SQL_CA2_SIMULATE_UNIQUE)
			printf(" SQL_CA2_SIMULATE_UNIQUE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_GETDATA_EXTENSIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_GETDATA_EXTENSIONS")) {
		printf("SQL_GETDATA_EXTENSIONS:");
		if (i & SQL_GD_ANY_COLUMN)
			printf(" SQL_GD_ANY_COLUMN");
		if (i & SQL_GD_ANY_ORDER)
			printf(" SQL_GD_ANY_ORDER");
		if (i & SQL_GD_BLOCK)
			printf(" SQL_GD_BLOCK");
		if (i & SQL_GD_BOUND)
			printf(" SQL_GD_BOUND");
#ifdef SQL_GD_OUTPUT_PARAMS
		if (i & SQL_GD_OUTPUT_PARAMS)
			printf(" SQL_GD_OUTPUT_PARAMS");
#endif
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_GROUP_BY, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_GROUP_BY")) {
		printf("SQL_GROUP_BY: ");
		switch (s) {
		case SQL_GB_COLLATE:
			printf("SQL_GB_COLLATE");
			break;
		case SQL_GB_NOT_SUPPORTED:
			printf("SQL_GB_NOT_SUPPORTED");
			break;
		case SQL_GB_GROUP_BY_EQUALS_SELECT:
			printf("SQL_GB_GROUP_BY_EQUALS_SELECT");
			break;
		case SQL_GB_GROUP_BY_CONTAINS_SELECT:
			printf("SQL_GB_GROUP_BY_CONTAINS_SELECT");
			break;
		case SQL_GB_NO_RELATION:
			printf("SQL_GB_NO_RELATION");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_IDENTIFIER_CASE, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_IDENTIFIER_CASE")) {
		printf("SQL_IDENTIFIER_CASE: ");
		switch (s) {
		case SQL_IC_UPPER:
			printf("SQL_IC_UPPER");
			break;
		case SQL_IC_LOWER:
			printf("SQL_IC_LOWER");
			break;
		case SQL_IC_SENSITIVE:
			printf("SQL_IC_SENSITIVE");
			break;
		case SQL_IC_MIXED:
			printf("SQL_IC_MIXED");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_IDENTIFIER_QUOTE_CHAR, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_IDENTIFIER_QUOTE_CHAR")) {
		printf("SQL_IDENTIFIER_QUOTE_CHAR: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_INDEX_KEYWORDS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_INDEX_KEYWORDS")) {
		printf("SQL_INDEX_KEYWORDS: ");
		switch (i) {
		case SQL_IK_NONE:
			printf("SQL_IK_NONE");
			break;
		case SQL_IK_ASC:
			printf("SQL_IK_ASC");
			break;
		case SQL_IK_DESC:
			printf("SQL_IK_DESC");
			break;
		case SQL_IK_ALL:
			printf("SQL_IK_ALL");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_INFO_SCHEMA_VIEWS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_INFO_SCHEMA_VIEWS")) {
		printf("SQL_INFO_SCHEMA_VIEWS:");
		if (i & SQL_ISV_ASSERTIONS)
			printf(" SQL_ISV_ASSERTIONS");
		if (i & SQL_ISV_CHARACTER_SETS)
			printf(" SQL_ISV_CHARACTER_SETS");
		if (i & SQL_ISV_CHECK_CONSTRAINTS)
			printf(" SQL_ISV_CHECK_CONSTRAINTS");
		if (i & SQL_ISV_COLLATIONS)
			printf(" SQL_ISV_COLLATIONS");
		if (i & SQL_ISV_COLUMN_DOMAIN_USAGE)
			printf(" SQL_ISV_COLUMN_DOMAIN_USAGE");
		if (i & SQL_ISV_COLUMN_PRIVILEGES)
			printf(" SQL_ISV_COLUMN_PRIVILEGES");
		if (i & SQL_ISV_COLUMNS)
			printf(" SQL_ISV_COLUMNS");
		if (i & SQL_ISV_CONSTRAINT_COLUMN_USAGE)
			printf(" SQL_ISV_CONSTRAINT_COLUMN_USAGE");
		if (i & SQL_ISV_CONSTRAINT_TABLE_USAGE)
			printf(" SQL_ISV_CONSTRAINT_TABLE_USAGE");
		if (i & SQL_ISV_DOMAIN_CONSTRAINTS)
			printf(" SQL_ISV_DOMAIN_CONSTRAINTS");
		if (i & SQL_ISV_DOMAINS)
			printf(" SQL_ISV_DOMAINS");
		if (i & SQL_ISV_KEY_COLUMN_USAGE)
			printf(" SQL_ISV_KEY_COLUMN_USAGE");
		if (i & SQL_ISV_REFERENTIAL_CONSTRAINTS)
			printf(" SQL_ISV_REFERENTIAL_CONSTRAINTS");
		if (i & SQL_ISV_SCHEMATA)
			printf(" SQL_ISV_SCHEMATA");
		if (i & SQL_ISV_SQL_LANGUAGES)
			printf(" SQL_ISV_SQL_LANGUAGES");
		if (i & SQL_ISV_TABLE_CONSTRAINTS)
			printf(" SQL_ISV_TABLE_CONSTRAINTS");
		if (i & SQL_ISV_TABLE_PRIVILEGES)
			printf(" SQL_ISV_TABLE_PRIVILEGES");
		if (i & SQL_ISV_TABLES)
			printf(" SQL_ISV_TABLES");
		if (i & SQL_ISV_TRANSLATIONS)
			printf(" SQL_ISV_TRANSLATIONS");
		if (i & SQL_ISV_USAGE_PRIVILEGES)
			printf(" SQL_ISV_USAGE_PRIVILEGES");
		if (i & SQL_ISV_VIEW_COLUMN_USAGE)
			printf(" SQL_ISV_VIEW_COLUMN_USAGE");
		if (i & SQL_ISV_VIEW_TABLE_USAGE)
			printf(" SQL_ISV_VIEW_TABLE_USAGE");
		if (i & SQL_ISV_VIEWS)
			printf(" SQL_ISV_VIEWS");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_INSERT_STATEMENT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_INSERT_STATEMENT")) {
		printf("SQL_INSERT_STATEMENT:");
		if (i & SQL_IS_INSERT_LITERALS)
			printf(" SQL_IS_INSERT_LITERALS");
		if (i & SQL_IS_INSERT_SEARCHED)
			printf(" SQL_IS_INSERT_SEARCHED");
		if (i & SQL_IS_SELECT_INTO)
			printf(" SQL_IS_SELECT_INTO");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_INTEGRITY, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_INTEGRITY")) {
		printf("SQL_INTEGRITY: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_KEYSET_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_KEYSET_CURSOR_ATTRIBUTES1")) {
		printf("SQL_KEYSET_CURSOR_ATTRIBUTES1:");
		if (i & SQL_CA1_NEXT)
			printf(" SQL_CA1_NEXT");
		if (i & SQL_CA1_ABSOLUTE)
			printf(" SQL_CA1_ABSOLUTE");
		if (i & SQL_CA1_RELATIVE)
			printf(" SQL_CA1_RELATIVE");
		if (i & SQL_CA1_BOOKMARK)
			printf(" SQL_CA1_BOOKMARK");
		if (i & SQL_CA1_LOCK_EXCLUSIVE)
			printf(" SQL_CA1_LOCK_EXCLUSIVE");
		if (i & SQL_CA1_LOCK_NO_CHANGE)
			printf(" SQL_CA1_LOCK_NO_CHANGE");
		if (i & SQL_CA1_LOCK_UNLOCK)
			printf(" SQL_CA1_LOCK_UNLOCK");
		if (i & SQL_CA1_POS_POSITION)
			printf(" SQL_CA1_POS_POSITION");
		if (i & SQL_CA1_POS_UPDATE)
			printf(" SQL_CA1_POS_UPDATE");
		if (i & SQL_CA1_POS_DELETE)
			printf(" SQL_CA1_POS_DELETE");
		if (i & SQL_CA1_POS_REFRESH)
			printf(" SQL_CA1_POS_REFRESH");
		if (i & SQL_CA1_POSITIONED_UPDATE)
			printf(" SQL_CA1_POSITIONED_UPDATE");
		if (i & SQL_CA1_POSITIONED_DELETE)
			printf(" SQL_CA1_POSITIONED_DELETE");
		if (i & SQL_CA1_SELECT_FOR_UPDATE)
			printf(" SQL_CA1_SELECT_FOR_UPDATE");
		if (i & SQL_CA1_BULK_ADD)
			printf(" SQL_CA1_BULK_ADD");
		if (i & SQL_CA1_BULK_UPDATE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_UPDATE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_DELETE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_DELETE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_FETCH_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_FETCH_BY_BOOKMARK");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_KEYSET_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_KEYSET_CURSOR_ATTRIBUTES2")) {
		printf("SQL_KEYSET_CURSOR_ATTRIBUTES2:");
		if (i & SQL_CA2_READ_ONLY_CONCURRENCY)
			printf(" SQL_CA2_READ_ONLY_CONCURRENCY");
		if (i & SQL_CA2_LOCK_CONCURRENCY)
			printf(" SQL_CA2_LOCK_CONCURRENCY");
		if (i & SQL_CA2_OPT_ROWVER_CONCURRENCY)
			printf(" SQL_CA2_OPT_ROWVER_CONCURRENCY");
		if (i & SQL_CA2_OPT_VALUES_CONCURRENCY)
			printf(" SQL_CA2_OPT_VALUES_CONCURRENCY");
		if (i & SQL_CA2_SENSITIVITY_ADDITIONS)
			printf(" SQL_CA2_SENSITIVITY_ADDITIONS");
		if (i & SQL_CA2_SENSITIVITY_DELETIONS)
			printf(" SQL_CA2_SENSITIVITY_DELETIONS");
		if (i & SQL_CA2_SENSITIVITY_UPDATES)
			printf(" SQL_CA2_SENSITIVITY_UPDATES");
		if (i & SQL_CA2_MAX_ROWS_SELECT)
			printf(" SQL_CA2_MAX_ROWS_SELECT");
		if (i & SQL_CA2_MAX_ROWS_INSERT)
			printf(" SQL_CA2_MAX_ROWS_INSERT");
		if (i & SQL_CA2_MAX_ROWS_DELETE)
			printf(" SQL_CA2_MAX_ROWS_DELETE");
		if (i & SQL_CA2_MAX_ROWS_UPDATE)
			printf(" SQL_CA2_MAX_ROWS_UPDATE");
		if (i & SQL_CA2_MAX_ROWS_CATALOG)
			printf(" SQL_CA2_MAX_ROWS_CATALOG");
		if (i & SQL_CA2_MAX_ROWS_AFFECTS_ALL)
			printf(" SQL_CA2_MAX_ROWS_AFFECTS_ALL");
		if (i & SQL_CA2_CRC_EXACT)
			printf(" SQL_CA2_CRC_EXACT");
		if (i & SQL_CA2_CRC_APPROXIMATE)
			printf(" SQL_CA2_CRC_APPROXIMATE");
		if (i & SQL_CA2_SIMULATE_NON_UNIQUE)
			printf(" SQL_CA2_SIMULATE_NON_UNIQUE");
		if (i & SQL_CA2_SIMULATE_TRY_UNIQUE)
			printf(" SQL_CA2_SIMULATE_TRY_UNIQUE");
		if (i & SQL_CA2_SIMULATE_UNIQUE)
			printf(" SQL_CA2_SIMULATE_UNIQUE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_KEYWORDS, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_KEYWORDS")) {
		printf("SQL_KEYWORDS: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_LIKE_ESCAPE_CLAUSE, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_LIKE_ESCAPE_CLAUSE")) {
		printf("SQL_LIKE_ESCAPE_CLAUSE: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_ASYNC_CONCURRENT_STATEMENTS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_ASYNC_CONCURRENT_STATEMENTS")) {
		printf("SQL_MAX_ASYNC_CONCURRENT_STATEMENTS: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_BINARY_LITERAL_LEN, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_BINARY_LITERAL_LEN")) {
		printf("SQL_MAX_BINARY_LITERAL_LEN: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_CATALOG_NAME_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_CATALOG_NAME_LEN")) {
		printf("SQL_MAX_CATALOG_NAME_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_CHAR_LITERAL_LEN, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_CHAR_LITERAL_LEN")) {
		printf("SQL_MAX_CHAR_LITERAL_LEN: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMN_NAME_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_COLUMN_NAME_LEN")) {
		printf("SQL_MAX_COLUMN_NAME_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_GROUP_BY, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_COLUMNS_IN_GROUP_BY")) {
		printf("SQL_MAX_COLUMNS_IN_GROUP_BY: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_INDEX, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_COLUMNS_IN_INDEX")) {
		printf("SQL_MAX_COLUMNS_IN_INDEX: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_ORDER_BY, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_COLUMNS_IN_ORDER_BY")) {
		printf("SQL_MAX_COLUMNS_IN_ORDER_BY: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_SELECT, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_COLUMNS_IN_SELECT")) {
		printf("SQL_MAX_COLUMNS_IN_SELECT: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_COLUMNS_IN_TABLE, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_COLUMNS_IN_TABLE")) {
		printf("SQL_MAX_COLUMNS_IN_TABLE: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_CURSOR_NAME_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_CURSOR_NAME_LEN")) {
		printf("SQL_MAX_CURSOR_NAME_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_DRIVER_CONNECTIONS, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_DRIVER_CONNECTIONS")) {
		printf("SQL_MAX_DRIVER_CONNECTIONS: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_IDENTIFIER_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_IDENTIFIER_LEN")) {
		printf("SQL_MAX_IDENTIFIER_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_INDEX_SIZE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_INDEX_SIZE")) {
		printf("SQL_MAX_INDEX_SIZE: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_PROCEDURE_NAME_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_PROCEDURE_NAME_LEN")) {
		printf("SQL_MAX_PROCEDURE_NAME_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_ROW_SIZE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_ROW_SIZE")) {
		printf("SQL_MAX_ROW_SIZE: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_ROW_SIZE_INCLUDES_LONG, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_ROW_SIZE_INCLUDES_LONG")) {
		printf("SQL_MAX_ROW_SIZE_INCLUDES_LONG: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_SCHEMA_NAME_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_SCHEMA_NAME_LEN")) {
		printf("SQL_MAX_SCHEMA_NAME_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_STATEMENT_LEN, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_STATEMENT_LEN")) {
		printf("SQL_MAX_STATEMENT_LEN: %u\n", (unsigned int) i);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_TABLE_NAME_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_TABLE_NAME_LEN")) {
		printf("SQL_MAX_TABLE_NAME_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_TABLES_IN_SELECT, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_TABLES_IN_SELECT")) {
		printf("SQL_MAX_TABLES_IN_SELECT: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MAX_USER_NAME_LEN, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MAX_USER_NAME_LEN")) {
		printf("SQL_MAX_USER_NAME_LEN: %u\n", (unsigned int) s);
	}

	ret = SQLGetInfo(dbc, SQL_MULTIPLE_ACTIVE_TXN, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MULTIPLE_ACTIVE_TXN")) {
		printf("SQL_MULTIPLE_ACTIVE_TXN: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_MULT_RESULT_SETS, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_MULT_RESULT_SETS")) {
		printf("SQL_MULT_RESULT_SETS: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_NEED_LONG_DATA_LEN, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_NEED_LONG_DATA_LEN")) {
		printf("SQL_NEED_LONG_DATA_LEN: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_NON_NULLABLE_COLUMNS, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_NON_NULLABLE_COLUMNS")) {
		printf("SQL_NON_NULLABLE_COLUMNS: ");
		switch (s) {
		case SQL_NNC_NULL:
			printf("SQL_NNC_NULL");
			break;
		case SQL_NNC_NON_NULL:
			printf("SQL_NNC_NON_NULL");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_NULL_COLLATION, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_NULL_COLLATION")) {
		printf("SQL_NULL_COLLATION: ");
		switch (s) {
		case SQL_NC_END:
			printf("SQL_NC_END");
			break;
		case SQL_NC_HIGH:
			printf("SQL_NC_HIGH");
			break;
		case SQL_NC_LOW:
			printf("SQL_NC_LOW");
			break;
		case SQL_NC_START:
			printf("SQL_NC_START");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_NUMERIC_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_NUMERIC_FUNCTIONS")) {
		printf("SQL_NUMERIC_FUNCTIONS:");
		if (i & SQL_FN_NUM_ABS)
			printf(" SQL_FN_NUM_ABS");
		if (i & SQL_FN_NUM_ACOS)
			printf(" SQL_FN_NUM_ACOS");
		if (i & SQL_FN_NUM_ASIN)
			printf(" SQL_FN_NUM_ASIN");
		if (i & SQL_FN_NUM_ATAN)
			printf(" SQL_FN_NUM_ATAN");
		if (i & SQL_FN_NUM_ATAN2)
			printf(" SQL_FN_NUM_ATAN2");
		if (i & SQL_FN_NUM_CEILING)
			printf(" SQL_FN_NUM_CEILING");
		if (i & SQL_FN_NUM_COS)
			printf(" SQL_FN_NUM_COS");
		if (i & SQL_FN_NUM_COT)
			printf(" SQL_FN_NUM_COT");
		if (i & SQL_FN_NUM_DEGREES)
			printf(" SQL_FN_NUM_DEGREES");
		if (i & SQL_FN_NUM_EXP)
			printf(" SQL_FN_NUM_EXP");
		if (i & SQL_FN_NUM_FLOOR)
			printf(" SQL_FN_NUM_FLOOR");
		if (i & SQL_FN_NUM_LOG)
			printf(" SQL_FN_NUM_LOG");
		if (i & SQL_FN_NUM_LOG10)
			printf(" SQL_FN_NUM_LOG10");
		if (i & SQL_FN_NUM_MOD)
			printf(" SQL_FN_NUM_MOD");
		if (i & SQL_FN_NUM_PI)
			printf(" SQL_FN_NUM_PI");
		if (i & SQL_FN_NUM_POWER)
			printf(" SQL_FN_NUM_POWER");
		if (i & SQL_FN_NUM_RADIANS)
			printf(" SQL_FN_NUM_RADIANS");
		if (i & SQL_FN_NUM_RAND)
			printf(" SQL_FN_NUM_RAND");
		if (i & SQL_FN_NUM_ROUND)
			printf(" SQL_FN_NUM_ROUND");
		if (i & SQL_FN_NUM_SIGN)
			printf(" SQL_FN_NUM_SIGN");
		if (i & SQL_FN_NUM_SIN)
			printf(" SQL_FN_NUM_SIN");
		if (i & SQL_FN_NUM_SQRT)
			printf(" SQL_FN_NUM_SQRT");
		if (i & SQL_FN_NUM_TAN)
			printf(" SQL_FN_NUM_TAN");
		if (i & SQL_FN_NUM_TRUNCATE)
			printf(" SQL_FN_NUM_TRUNCATE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_ODBC_INTERFACE_CONFORMANCE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ODBC_INTERFACE_CONFORMANCE")) {
		printf("SQL_ODBC_INTERFACE_CONFORMANCE: ");
		switch (i) {
		case SQL_OIC_CORE:
			printf("SQL_OIC_CORE");
			break;
		case SQL_OIC_LEVEL1:
			printf("SQL_OIC_LEVEL1");
			break;
		case SQL_OIC_LEVEL2:
			printf("SQL_OIC_LEVEL2");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_ODBC_VER, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ODBC_VER")) {
		printf("SQL_ODBC_VER: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_OJ_CAPABILITIES, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_OJ_CAPABILITIES")) {
		printf("SQL_OJ_CAPABILITIES:");
		if (i & SQL_OJ_LEFT)
			printf(" SQL_OJ_LEFT");
		if (i & SQL_OJ_RIGHT)
			printf(" SQL_OJ_RIGHT");
		if (i & SQL_OJ_FULL)
			printf(" SQL_OJ_FULL");
		if (i & SQL_OJ_NESTED)
			printf(" SQL_OJ_NESTED");
		if (i & SQL_OJ_NOT_ORDERED)
			printf(" SQL_OJ_NOT_ORDERED");
		if (i & SQL_OJ_INNER)
			printf(" SQL_OJ_INNER");
		if (i & SQL_OJ_ALL_COMPARISON_OPS)
			printf(" SQL_OJ_ALL_COMPARISON_OPS");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_ORDER_BY_COLUMNS_IN_SELECT, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ORDER_BY_COLUMNS_IN_SELECT")) {
		printf("SQL_ORDER_BY_COLUMNS_IN_SELECT: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_PARAM_ARRAY_ROW_COUNTS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_PARAM_ARRAY_ROW_COUNTS")) {
		printf("SQL_PARAM_ARRAY_ROW_COUNTS: ");
		switch (i) {
		case SQL_PARC_BATCH:
			printf("SQL_PARC_BATCH");
			break;
		case SQL_PARC_NO_BATCH:
			printf("SQL_PARC_NO_BATCH");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_PARAM_ARRAY_SELECTS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_PARAM_ARRAY_SELECTS")) {
	}

	ret = SQLGetInfo(dbc, SQL_POS_OPERATIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_POS_OPERATIONS")) {
		printf("SQL_POS_OPERATIONS:");
		if (i & SQL_POS_POSITION)
			printf(" SQL_POS_POSITION");
		if (i & SQL_POS_REFRESH)
			printf(" SQL_POS_REFRESH");
		if (i & SQL_POS_UPDATE)
			printf(" SQL_POS_UPDATE");
		if (i & SQL_POS_DELETE)
			printf(" SQL_POS_DELETE");
		if (i & SQL_POS_ADD)
			printf(" SQL_POS_ADD");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_PROCEDURES, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_PROCEDURES")) {
		printf("SQL_PROCEDURES: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_PROCEDURE_TERM, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_PROCEDURE_TERM")) {
		printf("SQL_PROCEDURE_TERM: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_QUOTED_IDENTIFIER_CASE, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_QUOTED_IDENTIFIER_CASE")) {
		printf("SQL_QUOTED_IDENTIFIER_CASE: ");
		switch (s) {
		case SQL_IC_UPPER:
			printf("SQL_IC_UPPER");
			break;
		case SQL_IC_LOWER:
			printf("SQL_IC_LOWER");
			break;
		case SQL_IC_SENSITIVE:
			printf("SQL_IC_SENSITIVE");
			break;
		case SQL_IC_MIXED:
			printf("SQL_IC_MIXED");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_ROW_UPDATES, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_ROW_UPDATES")) {
		printf("SQL_ROW_UPDATES: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_SCHEMA_TERM, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SCHEMA_TERM")) {
		printf("SQL_SCHEMA_TERM: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_SCHEMA_USAGE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SCHEMA_USAGE")) {
		printf("SQL_SCHEMA_USAGE:");
		if (i & SQL_SU_DML_STATEMENTS)
			printf(" SQL_SU_DML_STATEMENTS");
		if (i & SQL_SU_PROCEDURE_INVOCATION)
			printf(" SQL_SU_PROCEDURE_INVOCATION");
		if (i & SQL_SU_TABLE_DEFINITION)
			printf(" SQL_SU_TABLE_DEFINITION");
		if (i & SQL_SU_INDEX_DEFINITION)
			printf(" SQL_SU_INDEX_DEFINITION");
		if (i & SQL_SU_PRIVILEGE_DEFINITION)
			printf(" SQL_SU_PRIVILEGE_DEFINITION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SCROLL_OPTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SCROLL_OPTIONS")) {
		printf("SQL_SCROLL_OPTIONS:");
		if (i & SQL_SO_FORWARD_ONLY)
			printf(" SQL_SO_FORWARD_ONLY");
		if (i & SQL_SO_STATIC)
			printf(" SQL_SO_STATIC");
		if (i & SQL_SO_KEYSET_DRIVEN)
			printf(" SQL_SO_KEYSET_DRIVEN");
		if (i & SQL_SO_DYNAMIC)
			printf(" SQL_SO_DYNAMIC");
		if (i & SQL_SO_MIXED)
			printf(" SQL_SO_MIXED");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SEARCH_PATTERN_ESCAPE, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SEARCH_PATTERN_ESCAPE")) {
		printf("SQL_SEARCH_PATTERN_ESCAPE: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_SERVER_NAME, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SERVER_NAME")) {
		printf("SQL_SERVER_NAME: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_SPECIAL_CHARACTERS, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SPECIAL_CHARACTERS")) {
		printf("SQL_SPECIAL_CHARACTERS: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_SQL_CONFORMANCE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL_CONFORMANCE")) {
		printf("SQL_SQL_CONFORMANCE: ");
		switch (i) {
		case SQL_SC_SQL92_ENTRY:
			printf("SQL_SC_SQL92_ENTRY");
			break;
		case SQL_SC_FIPS127_2_TRANSITIONAL:
			printf("SQL_SC_FIPS127_2_TRANSITIONAL");
			break;
		case SQL_SC_SQL92_FULL:
			printf("SQL_SC_SQL92_FULL");
			break;
		case SQL_SC_SQL92_INTERMEDIATE:
			printf("SQL_SC_SQL92_INTERMEDIATE");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_DATETIME_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_DATETIME_FUNCTIONS")) {
		printf("SQL_SQL92_DATETIME_FUNCTIONS:");
		if (i & SQL_SDF_CURRENT_DATE)
			printf(" SQL_SDF_CURRENT_DATE");
		if (i & SQL_SDF_CURRENT_TIME)
			printf(" SQL_SDF_CURRENT_TIME");
		if (i & SQL_SDF_CURRENT_TIMESTAMP)
			printf(" SQL_SDF_CURRENT_TIMESTAMP");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_FOREIGN_KEY_DELETE_RULE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_FOREIGN_KEY_DELETE_RULE")) {
		printf("SQL_SQL92_FOREIGN_KEY_DELETE_RULE:");
		if (i & SQL_SFKD_CASCADE)
			printf(" SQL_SFKD_CASCADE");
		if (i & SQL_SFKD_NO_ACTION)
			printf(" SQL_SFKD_NO_ACTION");
		if (i & SQL_SFKD_SET_DEFAULT)
			printf(" SQL_SFKD_SET_DEFAULT");
		if (i & SQL_SFKD_SET_NULL)
			printf(" SQL_SFKD_SET_NULL");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_FOREIGN_KEY_UPDATE_RULE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_FOREIGN_KEY_UPDATE_RULE")) {
		printf("SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:");
		if (i & SQL_SFKU_CASCADE)
			printf(" SQL_SFKU_CASCADE");
		if (i & SQL_SFKU_NO_ACTION)
			printf(" SQL_SFKU_NO_ACTION");
		if (i & SQL_SFKU_SET_DEFAULT)
			printf(" SQL_SFKU_SET_DEFAULT");
		if (i & SQL_SFKU_SET_NULL)
			printf(" SQL_SFKU_SET_NULL");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_GRANT, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_GRANT")) {
		printf("SQL_SQL92_GRANT:");
		if (i & SQL_SG_DELETE_TABLE)
			printf(" SQL_SG_DELETE_TABLE");
		if (i & SQL_SG_INSERT_COLUMN)
			printf(" SQL_SG_INSERT_COLUMN");
		if (i & SQL_SG_INSERT_TABLE)
			printf(" SQL_SG_INSERT_TABLE");
		if (i & SQL_SG_REFERENCES_TABLE)
			printf(" SQL_SG_REFERENCES_TABLE");
		if (i & SQL_SG_REFERENCES_COLUMN)
			printf(" SQL_SG_REFERENCES_COLUMN");
		if (i & SQL_SG_SELECT_TABLE)
			printf(" SQL_SG_SELECT_TABLE");
		if (i & SQL_SG_UPDATE_COLUMN)
			printf(" SQL_SG_UPDATE_COLUMN");
		if (i & SQL_SG_UPDATE_TABLE)
			printf(" SQL_SG_UPDATE_TABLE");
		if (i & SQL_SG_USAGE_ON_DOMAIN)
			printf(" SQL_SG_USAGE_ON_DOMAIN");
		if (i & SQL_SG_USAGE_ON_CHARACTER_SET)
			printf(" SQL_SG_USAGE_ON_CHARACTER_SET");
		if (i & SQL_SG_USAGE_ON_COLLATION)
			printf(" SQL_SG_USAGE_ON_COLLATION");
		if (i & SQL_SG_USAGE_ON_TRANSLATION)
			printf(" SQL_SG_USAGE_ON_TRANSLATION");
		if (i & SQL_SG_WITH_GRANT_OPTION)
			printf(" SQL_SG_WITH_GRANT_OPTION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_NUMERIC_VALUE_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_NUMERIC_VALUE_FUNCTIONS")) {
		printf("SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:");
		if (i & SQL_SNVF_BIT_LENGTH)
			printf(" SQL_SNVF_BIT_LENGTH");
		if (i & SQL_SNVF_CHAR_LENGTH)
			printf(" SQL_SNVF_CHAR_LENGTH");
		if (i & SQL_SNVF_CHARACTER_LENGTH)
			printf(" SQL_SNVF_CHARACTER_LENGTH");
		if (i & SQL_SNVF_EXTRACT)
			printf(" SQL_SNVF_EXTRACT");
		if (i & SQL_SNVF_OCTET_LENGTH)
			printf(" SQL_SNVF_OCTET_LENGTH");
		if (i & SQL_SNVF_POSITION)
			printf(" SQL_SNVF_POSITION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_PREDICATES, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_PREDICATES")) {
		printf("SQL_SQL92_PREDICATES:");
		if (i & SQL_SP_BETWEEN)
			printf(" SQL_SP_BETWEEN");
		if (i & SQL_SP_COMPARISON)
			printf(" SQL_SP_COMPARISON");
		if (i & SQL_SP_EXISTS)
			printf(" SQL_SP_EXISTS");
		if (i & SQL_SP_IN)
			printf(" SQL_SP_IN");
		if (i & SQL_SP_ISNOTNULL)
			printf(" SQL_SP_ISNOTNULL");
		if (i & SQL_SP_ISNULL)
			printf(" SQL_SP_ISNULL");
		if (i & SQL_SP_LIKE)
			printf(" SQL_SP_LIKE");
		if (i & SQL_SP_MATCH_FULL)
			printf(" SQL_SP_MATCH_FULL");
		if (i & SQL_SP_MATCH_PARTIAL)
			printf(" SQL_SP_MATCH_PARTIAL");
		if (i & SQL_SP_MATCH_UNIQUE_FULL)
			printf(" SQL_SP_MATCH_UNIQUE_FULL");
		if (i & SQL_SP_MATCH_UNIQUE_PARTIAL)
			printf(" SQL_SP_MATCH_UNIQUE_PARTIAL");
		if (i & SQL_SP_OVERLAPS)
			printf(" SQL_SP_OVERLAPS");
		if (i & SQL_SP_QUANTIFIED_COMPARISON)
			printf(" SQL_SP_QUANTIFIED_COMPARISON");
		if (i & SQL_SP_UNIQUE)
			printf(" SQL_SP_UNIQUE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_RELATIONAL_JOIN_OPERATORS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_RELATIONAL_JOIN_OPERATORS")) {
		printf("SQL_SQL92_RELATIONAL_JOIN_OPERATORS:");
		if (i & SQL_SRJO_CORRESPONDING_CLAUSE)
			printf(" SQL_SRJO_CORRESPONDING_CLAUSE");
		if (i & SQL_SRJO_CROSS_JOIN)
			printf(" SQL_SRJO_CROSS_JOIN");
		if (i & SQL_SRJO_EXCEPT_JOIN)
			printf(" SQL_SRJO_EXCEPT_JOIN");
		if (i & SQL_SRJO_FULL_OUTER_JOIN)
			printf(" SQL_SRJO_FULL_OUTER_JOIN");
		if (i & SQL_SRJO_INNER_JOIN)
			printf(" SQL_SRJO_INNER_JOIN");
		if (i & SQL_SRJO_INTERSECT_JOIN)
			printf(" SQL_SRJO_INTERSECT_JOIN");
		if (i & SQL_SRJO_LEFT_OUTER_JOIN)
			printf(" SQL_SRJO_LEFT_OUTER_JOIN");
		if (i & SQL_SRJO_NATURAL_JOIN)
			printf(" SQL_SRJO_NATURAL_JOIN");
		if (i & SQL_SRJO_RIGHT_OUTER_JOIN)
			printf(" SQL_SRJO_RIGHT_OUTER_JOIN");
		if (i & SQL_SRJO_UNION_JOIN)
			printf(" SQL_SRJO_UNION_JOIN");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_REVOKE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_REVOKE")) {
		printf("SQL_SQL92_REVOKE:");
		if (i & SQL_SR_CASCADE)
			printf(" SQL_SR_CASCADE");
		if (i & SQL_SR_DELETE_TABLE)
			printf(" SQL_SR_DELETE_TABLE");
		if (i & SQL_SR_GRANT_OPTION_FOR)
			printf(" SQL_SR_GRANT_OPTION_FOR");
		if (i & SQL_SR_INSERT_COLUMN)
			printf(" SQL_SR_INSERT_COLUMN");
		if (i & SQL_SR_INSERT_TABLE)
			printf(" SQL_SR_INSERT_TABLE");
		if (i & SQL_SR_REFERENCES_COLUMN)
			printf(" SQL_SR_REFERENCES_COLUMN");
		if (i & SQL_SR_REFERENCES_TABLE)
			printf(" SQL_SR_REFERENCES_TABLE");
		if (i & SQL_SR_RESTRICT)
			printf(" SQL_SR_RESTRICT");
		if (i & SQL_SR_SELECT_TABLE)
			printf(" SQL_SR_SELECT_TABLE");
		if (i & SQL_SR_UPDATE_COLUMN)
			printf(" SQL_SR_UPDATE_COLUMN");
		if (i & SQL_SR_UPDATE_TABLE)
			printf(" SQL_SR_UPDATE_TABLE");
		if (i & SQL_SR_USAGE_ON_DOMAIN)
			printf(" SQL_SR_USAGE_ON_DOMAIN");
		if (i & SQL_SR_USAGE_ON_CHARACTER_SET)
			printf(" SQL_SR_USAGE_ON_CHARACTER_SET");
		if (i & SQL_SR_USAGE_ON_COLLATION)
			printf(" SQL_SR_USAGE_ON_COLLATION");
		if (i & SQL_SR_USAGE_ON_TRANSLATION)
			printf(" SQL_SR_USAGE_ON_TRANSLATION");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_ROW_VALUE_CONSTRUCTOR, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_ROW_VALUE_CONSTRUCTOR")) {
		printf("SQL_SQL92_ROW_VALUE_CONSTRUCTOR:");
		if (i & SQL_SRVC_VALUE_EXPRESSION)
			printf(" SQL_SRVC_VALUE_EXPRESSION");
		if (i & SQL_SRVC_NULL)
			printf(" SQL_SRVC_NULL");
		if (i & SQL_SRVC_DEFAULT)
			printf(" SQL_SRVC_DEFAULT");
		if (i & SQL_SRVC_ROW_SUBQUERY)
			printf(" SQL_SRVC_ROW_SUBQUERY");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_STRING_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_STRING_FUNCTIONS")) {
		printf("SQL_SQL92_STRING_FUNCTIONS:");
		if (i & SQL_SSF_CONVERT)
			printf(" SQL_SSF_CONVERT");
		if (i & SQL_SSF_LOWER)
			printf(" SQL_SSF_LOWER");
		if (i & SQL_SSF_UPPER)
			printf(" SQL_SSF_UPPER");
		if (i & SQL_SSF_SUBSTRING)
			printf(" SQL_SSF_SUBSTRING");
		if (i & SQL_SSF_TRANSLATE)
			printf(" SQL_SSF_TRANSLATE");
		if (i & SQL_SSF_TRIM_BOTH)
			printf(" SQL_SSF_TRIM_BOTH");
		if (i & SQL_SSF_TRIM_LEADING)
			printf(" SQL_SSF_TRIM_LEADING");
		if (i & SQL_SSF_TRIM_TRAILING)
			printf(" SQL_SSF_TRIM_TRAILING");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SQL92_VALUE_EXPRESSIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SQL92_VALUE_EXPRESSIONS")) {
		printf("SQL_SQL92_VALUE_EXPRESSIONS:");
		if (i & SQL_SVE_CASE)
			printf(" SQL_SVE_CASE");
		if (i & SQL_SVE_CAST)
			printf(" SQL_SVE_CAST");
		if (i & SQL_SVE_COALESCE)
			printf(" SQL_SVE_COALESCE");
		if (i & SQL_SVE_NULLIF)
			printf(" SQL_SVE_NULLIF");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_STANDARD_CLI_CONFORMANCE, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_STANDARD_CLI_CONFORMANCE")) {
		printf("SQL_STANDARD_CLI_CONFORMANCE:");
		if (i & SQL_SCC_XOPEN_CLI_VERSION1)
			printf(" SQL_SCC_XOPEN_CLI_VERSION1");
		if (i & SQL_SCC_ISO92_CLI)
			printf(" SQL_SCC_ISO92_CLI");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_STATIC_CURSOR_ATTRIBUTES1, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_STATIC_CURSOR_ATTRIBUTES1")) {
		printf("SQL_STATIC_CURSOR_ATTRIBUTES1:");
		if (i & SQL_CA1_NEXT)
			printf(" SQL_CA1_NEXT");
		if (i & SQL_CA1_ABSOLUTE)
			printf(" SQL_CA1_ABSOLUTE");
		if (i & SQL_CA1_RELATIVE)
			printf(" SQL_CA1_RELATIVE");
		if (i & SQL_CA1_BOOKMARK)
			printf(" SQL_CA1_BOOKMARK");
		if (i & SQL_CA1_LOCK_NO_CHANGE)
			printf(" SQL_CA1_LOCK_NO_CHANGE");
		if (i & SQL_CA1_LOCK_EXCLUSIVE)
			printf(" SQL_CA1_LOCK_EXCLUSIVE");
		if (i & SQL_CA1_LOCK_UNLOCK)
			printf(" SQL_CA1_LOCK_UNLOCK");
		if (i & SQL_CA1_POS_POSITION)
			printf(" SQL_CA1_POS_POSITION");
		if (i & SQL_CA1_POS_UPDATE)
			printf(" SQL_CA1_POS_UPDATE");
		if (i & SQL_CA1_POS_DELETE)
			printf(" SQL_CA1_POS_DELETE");
		if (i & SQL_CA1_POS_REFRESH)
			printf(" SQL_CA1_POS_REFRESH");
		if (i & SQL_CA1_POSITIONED_UPDATE)
			printf(" SQL_CA1_POSITIONED_UPDATE");
		if (i & SQL_CA1_POSITIONED_DELETE)
			printf(" SQL_CA1_POSITIONED_DELETE");
		if (i & SQL_CA1_SELECT_FOR_UPDATE)
			printf(" SQL_CA1_SELECT_FOR_UPDATE");
		if (i & SQL_CA1_BULK_ADD)
			printf(" SQL_CA1_BULK_ADD");
		if (i & SQL_CA1_BULK_UPDATE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_UPDATE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_DELETE_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_DELETE_BY_BOOKMARK");
		if (i & SQL_CA1_BULK_FETCH_BY_BOOKMARK)
			printf(" SQL_CA1_BULK_FETCH_BY_BOOKMARK");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_STATIC_CURSOR_ATTRIBUTES2, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_STATIC_CURSOR_ATTRIBUTES2")) {
		printf("SQL_STATIC_CURSOR_ATTRIBUTES2:");
		if (i & SQL_CA2_READ_ONLY_CONCURRENCY)
			printf(" SQL_CA2_READ_ONLY_CONCURRENCY");
		if (i & SQL_CA2_LOCK_CONCURRENCY)
			printf(" SQL_CA2_LOCK_CONCURRENCY");
		if (i & SQL_CA2_OPT_ROWVER_CONCURRENCY)
			printf(" SQL_CA2_OPT_ROWVER_CONCURRENCY");
		if (i & SQL_CA2_OPT_VALUES_CONCURRENCY)
			printf(" SQL_CA2_OPT_VALUES_CONCURRENCY");
		if (i & SQL_CA2_SENSITIVITY_ADDITIONS)
			printf(" SQL_CA2_SENSITIVITY_ADDITIONS");
		if (i & SQL_CA2_SENSITIVITY_DELETIONS)
			printf(" SQL_CA2_SENSITIVITY_DELETIONS");
		if (i & SQL_CA2_SENSITIVITY_UPDATES)
			printf(" SQL_CA2_SENSITIVITY_UPDATES");
		if (i & SQL_CA2_MAX_ROWS_SELECT)
			printf(" SQL_CA2_MAX_ROWS_SELECT");
		if (i & SQL_CA2_MAX_ROWS_INSERT)
			printf(" SQL_CA2_MAX_ROWS_INSERT");
		if (i & SQL_CA2_MAX_ROWS_DELETE)
			printf(" SQL_CA2_MAX_ROWS_DELETE");
		if (i & SQL_CA2_MAX_ROWS_UPDATE)
			printf(" SQL_CA2_MAX_ROWS_UPDATE");
		if (i & SQL_CA2_MAX_ROWS_CATALOG)
			printf(" SQL_CA2_MAX_ROWS_CATALOG");
		if (i & SQL_CA2_MAX_ROWS_AFFECTS_ALL)
			printf(" SQL_CA2_MAX_ROWS_AFFECTS_ALL");
		if (i & SQL_CA2_CRC_EXACT)
			printf(" SQL_CA2_CRC_EXACT");
		if (i & SQL_CA2_CRC_APPROXIMATE)
			printf(" SQL_CA2_CRC_APPROXIMATE");
		if (i & SQL_CA2_SIMULATE_NON_UNIQUE)
			printf(" SQL_CA2_SIMULATE_NON_UNIQUE");
		if (i & SQL_CA2_SIMULATE_TRY_UNIQUE)
			printf(" SQL_CA2_SIMULATE_TRY_UNIQUE");
		if (i & SQL_CA2_SIMULATE_UNIQUE)
			printf(" SQL_CA2_SIMULATE_UNIQUE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_STRING_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_STRING_FUNCTIONS")) {
		printf("SQL_STRING_FUNCTIONS:");
		if (i & SQL_FN_STR_ASCII)
			printf(" SQL_FN_STR_ASCII");
		if (i & SQL_FN_STR_BIT_LENGTH)
			printf(" SQL_FN_STR_BIT_LENGTH");
		if (i & SQL_FN_STR_CHAR)
			printf(" SQL_FN_STR_CHAR");
		if (i & SQL_FN_STR_CHAR_LENGTH)
			printf(" SQL_FN_STR_CHAR_LENGTH");
		if (i & SQL_FN_STR_CHARACTER_LENGTH)
			printf(" SQL_FN_STR_CHARACTER_LENGTH");
		if (i & SQL_FN_STR_CONCAT)
			printf(" SQL_FN_STR_CONCAT");
		if (i & SQL_FN_STR_DIFFERENCE)
			printf(" SQL_FN_STR_DIFFERENCE");
		if (i & SQL_FN_STR_INSERT)
			printf(" SQL_FN_STR_INSERT");
		if (i & SQL_FN_STR_LCASE)
			printf(" SQL_FN_STR_LCASE");
		if (i & SQL_FN_STR_LEFT)
			printf(" SQL_FN_STR_LEFT");
		if (i & SQL_FN_STR_LENGTH)
			printf(" SQL_FN_STR_LENGTH");
		if (i & SQL_FN_STR_LOCATE)
			printf(" SQL_FN_STR_LOCATE");
		if (i & SQL_FN_STR_LTRIM)
			printf(" SQL_FN_STR_LTRIM");
		if (i & SQL_FN_STR_OCTET_LENGTH)
			printf(" SQL_FN_STR_OCTET_LENGTH");
		if (i & SQL_FN_STR_POSITION)
			printf(" SQL_FN_STR_POSITION");
		if (i & SQL_FN_STR_REPEAT)
			printf(" SQL_FN_STR_REPEAT");
		if (i & SQL_FN_STR_REPLACE)
			printf(" SQL_FN_STR_REPLACE");
		if (i & SQL_FN_STR_RIGHT)
			printf(" SQL_FN_STR_RIGHT");
		if (i & SQL_FN_STR_RTRIM)
			printf(" SQL_FN_STR_RTRIM");
		if (i & SQL_FN_STR_SOUNDEX)
			printf(" SQL_FN_STR_SOUNDEX");
		if (i & SQL_FN_STR_SPACE)
			printf(" SQL_FN_STR_SPACE");
		if (i & SQL_FN_STR_SUBSTRING)
			printf(" SQL_FN_STR_SUBSTRING");
		if (i & SQL_FN_STR_UCASE)
			printf(" SQL_FN_STR_UCASE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SUBQUERIES, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SUBQUERIES")) {
		printf("SQL_SUBQUERIES:");
		if (i & SQL_SQ_CORRELATED_SUBQUERIES)
			printf(" SQL_SQ_CORRELATED_SUBQUERIES");
		if (i & SQL_SQ_COMPARISON)
			printf(" SQL_SQ_COMPARISON");
		if (i & SQL_SQ_EXISTS)
			printf(" SQL_SQ_EXISTS");
		if (i & SQL_SQ_IN)
			printf(" SQL_SQ_IN");
		if (i & SQL_SQ_QUANTIFIED)
			printf(" SQL_SQ_QUANTIFIED");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_SYSTEM_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_SYSTEM_FUNCTIONS")) {
		printf("SQL_SYSTEM_FUNCTIONS:");
		if (i & SQL_FN_SYS_DBNAME)
			printf(" SQL_FN_SYS_DBNAME");
		if (i & SQL_FN_SYS_IFNULL)
			printf(" SQL_FN_SYS_IFNULL");
		if (i & SQL_FN_SYS_USERNAME)
			printf(" SQL_FN_SYS_USERNAME");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_TABLE_TERM, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_TABLE_TERM")) {
		printf("SQL_TABLE_TERM: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_TIMEDATE_ADD_INTERVALS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_TIMEDATE_ADD_INTERVALS")) {
		printf("SQL_TIMEDATE_ADD_INTERVALS:");
		if (i & SQL_FN_TSI_FRAC_SECOND)
			printf(" SQL_FN_TSI_FRAC_SECOND");
		if (i & SQL_FN_TSI_SECOND)
			printf(" SQL_FN_TSI_SECOND");
		if (i & SQL_FN_TSI_MINUTE)
			printf(" SQL_FN_TSI_MINUTE");
		if (i & SQL_FN_TSI_HOUR)
			printf(" SQL_FN_TSI_HOUR");
		if (i & SQL_FN_TSI_DAY)
			printf(" SQL_FN_TSI_DAY");
		if (i & SQL_FN_TSI_WEEK)
			printf(" SQL_FN_TSI_WEEK");
		if (i & SQL_FN_TSI_MONTH)
			printf(" SQL_FN_TSI_MONTH");
		if (i & SQL_FN_TSI_QUARTER)
			printf(" SQL_FN_TSI_QUARTER");
		if (i & SQL_FN_TSI_YEAR)
			printf(" SQL_FN_TSI_YEAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_TIMEDATE_DIFF_INTERVALS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_TIMEDATE_DIFF_INTERVALS")) {
		printf("SQL_TIMEDATE_DIFF_INTERVALS:");
		if (i & SQL_FN_TSI_FRAC_SECOND)
			printf(" SQL_FN_TSI_FRAC_SECOND");
		if (i & SQL_FN_TSI_SECOND)
			printf(" SQL_FN_TSI_SECOND");
		if (i & SQL_FN_TSI_MINUTE)
			printf(" SQL_FN_TSI_MINUTE");
		if (i & SQL_FN_TSI_HOUR)
			printf(" SQL_FN_TSI_HOUR");
		if (i & SQL_FN_TSI_DAY)
			printf(" SQL_FN_TSI_DAY");
		if (i & SQL_FN_TSI_WEEK)
			printf(" SQL_FN_TSI_WEEK");
		if (i & SQL_FN_TSI_MONTH)
			printf(" SQL_FN_TSI_MONTH");
		if (i & SQL_FN_TSI_QUARTER)
			printf(" SQL_FN_TSI_QUARTER");
		if (i & SQL_FN_TSI_YEAR)
			printf(" SQL_FN_TSI_YEAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_TIMEDATE_FUNCTIONS, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_TIMEDATE_FUNCTIONS")) {
		printf("SQL_TIMEDATE_FUNCTIONS:");
		if (i & SQL_FN_TD_CURRENT_DATE)
			printf(" SQL_FN_TD_CURRENT_DATE");
		if (i & SQL_FN_TD_CURRENT_TIME)
			printf(" SQL_FN_TD_CURRENT_TIME");
		if (i & SQL_FN_TD_CURRENT_TIMESTAMP)
			printf(" SQL_FN_TD_CURRENT_TIMESTAMP");
		if (i & SQL_FN_TD_CURDATE)
			printf(" SQL_FN_TD_CURDATE");
		if (i & SQL_FN_TD_CURTIME)
			printf(" SQL_FN_TD_CURTIME");
		if (i & SQL_FN_TD_DAYNAME)
			printf(" SQL_FN_TD_DAYNAME");
		if (i & SQL_FN_TD_DAYOFMONTH)
			printf(" SQL_FN_TD_DAYOFMONTH");
		if (i & SQL_FN_TD_DAYOFWEEK)
			printf(" SQL_FN_TD_DAYOFWEEK");
		if (i & SQL_FN_TD_DAYOFYEAR)
			printf(" SQL_FN_TD_DAYOFYEAR");
		if (i & SQL_FN_TD_EXTRACT)
			printf(" SQL_FN_TD_EXTRACT");
		if (i & SQL_FN_TD_HOUR)
			printf(" SQL_FN_TD_HOUR");
		if (i & SQL_FN_TD_MINUTE)
			printf(" SQL_FN_TD_MINUTE");
		if (i & SQL_FN_TD_MONTH)
			printf(" SQL_FN_TD_MONTH");
		if (i & SQL_FN_TD_MONTHNAME)
			printf(" SQL_FN_TD_MONTHNAME");
		if (i & SQL_FN_TD_NOW)
			printf(" SQL_FN_TD_NOW");
		if (i & SQL_FN_TD_QUARTER)
			printf(" SQL_FN_TD_QUARTER");
		if (i & SQL_FN_TD_SECOND)
			printf(" SQL_FN_TD_SECOND");
		if (i & SQL_FN_TD_TIMESTAMPADD)
			printf(" SQL_FN_TD_TIMESTAMPADD");
		if (i & SQL_FN_TD_TIMESTAMPDIFF)
			printf(" SQL_FN_TD_TIMESTAMPDIFF");
		if (i & SQL_FN_TD_WEEK)
			printf(" SQL_FN_TD_WEEK");
		if (i & SQL_FN_TD_YEAR)
			printf(" SQL_FN_TD_YEAR");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_TXN_CAPABLE, &s, sizeof(s), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_TXN_CAPABLE")) {
		printf("SQL_TXN_CAPABLE: ");
		switch (s) {
		case SQL_TC_NONE:
			printf("SQL_TC_NONE");
			break;
		case SQL_TC_DML:
			printf("SQL_TC_DML");
			break;
		case SQL_TC_DDL_COMMIT:
			printf("SQL_TC_DDL_COMMIT");
			break;
		case SQL_TC_DDL_IGNORE:
			printf("SQL_TC_DDL_IGNORE");
			break;
		case SQL_TC_ALL:
			printf("SQL_TC_ALL");
			break;
		}
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_TXN_ISOLATION_OPTION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_TXN_ISOLATION_OPTION")) {
		printf("SQL_TXN_ISOLATION_OPTION:");
		if (i & SQL_TXN_READ_UNCOMMITTED)
			printf(" SQL_TXN_READ_UNCOMMITTED");
		if (i & SQL_TXN_READ_COMMITTED)
			printf(" SQL_TXN_READ_COMMITTED");
		if (i & SQL_TXN_REPEATABLE_READ)
			printf(" SQL_TXN_REPEATABLE_READ");
		if (i & SQL_TXN_SERIALIZABLE)
			printf(" SQL_TXN_SERIALIZABLE");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_UNION, &i, sizeof(i), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_UNION")) {
		printf("SQL_UNION:");
		if (i & SQL_U_UNION)
			printf(" SQL_U_UNION");
		if (i & SQL_U_UNION_ALL)
			printf(" SQL_U_UNION_ALL");
		printf("\n");
	}

	ret = SQLGetInfo(dbc, SQL_USER_NAME, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_USER_NAME")) {
		printf("SQL_USER_NAME: %.*s\n", resultlen, str);
	}

	ret = SQLGetInfo(dbc, SQL_XOPEN_CLI_YEAR, str, sizeof(str), &resultlen);
	if (check(ret, SQL_HANDLE_DBC, dbc, "SQLGetInfo SQL_XOPEN_CLI_YEAR")) {
		printf("SQL_XOPEN_CLI_YEAR: %.*s\n", resultlen, str);
	}

	ret = SQLDisconnect(dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLDisconnect");

	ret = SQLFreeHandle(SQL_HANDLE_DBC, dbc);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLFreeHandle (DBC)");

	ret = SQLFreeHandle(SQL_HANDLE_ENV, env);
	check(ret, SQL_HANDLE_ENV, env, "SQLFreeHandle (ENV)");

	return 0;
}
