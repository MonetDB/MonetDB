/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifdef _MSC_VER
#include <WTypes.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>

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
		exit(1);
	default:
		fprintf(stderr, "%s: Unexpected return value\n", func);
		break;
	}
}

static void
renewStmtHandle(SQLHANDLE dbc, SQLHANDLE stmt)
{
	SQLRETURN ret;

	ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFreeHandle (STMT)");

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle (STMT)");
}

static void
compareResult(SQLHANDLE stmt, const char * functionname, const char * expected)
{
	SQLRETURN ret;
	SQLSMALLINT columns;	// Number of columns in result-set
	size_t expct_len = strlen(expected);
	size_t outp_len = expct_len + 1000;
	char * outp = malloc(outp_len);
	size_t pos = 0;
	SQLUSMALLINT col;
	SQLLEN indicator;
	char buf[255];

	SQLSMALLINT nameLength;
	SQLSMALLINT dataType;

	// How many columns are there
	ret = SQLNumResultCols(stmt, &columns);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLNumResultCols()");
	pos += snprintf(outp + pos, outp_len - pos, "Resultset with %d columns\n", columns);

	// get Result Column Names and print them
	for (col = 1; col <= columns; col++) {
		ret = SQLDescribeCol(stmt, col, (SQLCHAR *) buf, sizeof(buf),
			&nameLength, &dataType, NULL, NULL, NULL);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeCol()");
		pos += snprintf(outp + pos, outp_len - pos,
				(col > 1) ? "\t%s" : "%s", buf);
	}
	pos += snprintf(outp + pos, outp_len - pos, "\n");

	/* Loop through the rows in the result-set */
	ret = SQLFetch(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch(1)");
	while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
		// Loop through the columns
		for (col = 1; col <= columns; col++) {
			// Retrieve column data as a string
			ret = SQLGetData(stmt, col, SQL_C_CHAR, buf, sizeof(buf), &indicator);
			check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData()");
			if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
				// Handle null columns
				if (indicator == SQL_NULL_DATA)
					strcpy(buf, "NULL");

				pos += snprintf(outp + pos, outp_len - pos,
					(col > 1) ? "\t%s" : "%s", buf);
			}
		}
		pos += snprintf(outp + pos, outp_len - pos, "\n");
		ret = SQLFetch(stmt);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch(n)");
	}

	if (strcmp(expected, outp) != 0) {
		fprintf(stderr, "Testing %s\nExpected:\n%s\nGotten:\n%s\n",
			functionname, expected, outp);
	}

	// cleanup
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
		fprintf(stderr, "Usage: %s [datasource [user [password]]]\n", argv[0]);
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
	// All catalogs query
	ret = SQLTables(stmt, (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLTables (SQL_ALL_CATALOGS)");
	compareResult(stmt, "SQLTables (SQL_ALL_CATALOGS)",
			"Resultset with 5 columns\n"
			"table_cat	table_schem	table_name	table_type	remarks\n"
			"mTests_sql_odbc_samples	NULL	NULL	NULL	NULL\n");
	renewStmtHandle(dbc, stmt);

	// All schemas query
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)SQL_ALL_SCHEMAS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLTables (SQL_ALL_SCHEMAS)");
	compareResult(stmt, "SQLTables (SQL_ALL_SCHEMAS)",
			"Resultset with 5 columns\n"
			"table_cat	table_schem	table_name	table_type	remarks\n"
			"NULL	json	NULL	NULL	NULL\n"
			"NULL	logging	NULL	NULL	NULL\n"
			"NULL	profiler	NULL	NULL	NULL\n"
			"NULL	sys	NULL	NULL	NULL\n"
			"NULL	tmp	NULL	NULL	NULL\n"
			"NULL	wlc	NULL	NULL	NULL\n"
			"NULL	wlr	NULL	NULL	NULL\n");
	renewStmtHandle(dbc, stmt);

	// All table types query
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)SQL_ALL_TABLE_TYPES, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLTables (SQL_ALL_TABLE_TYPES)");
	compareResult(stmt, "SQLTables (SQL_ALL_TABLE_TYPES)",
			"Resultset with 5 columns\n"
			"table_cat	table_schem	table_name	table_type	remarks\n"
			"NULL	NULL	NULL	GLOBAL TEMPORARY TABLE	NULL\n"
			"NULL	NULL	NULL	LOCAL TEMPORARY TABLE	NULL\n"
			"NULL	NULL	NULL	MERGE TABLE	NULL\n"
			"NULL	NULL	NULL	REMOTE TABLE	NULL\n"
			"NULL	NULL	NULL	REPLICA TABLE	NULL\n"
			"NULL	NULL	NULL	SYSTEM TABLE	NULL\n"
			"NULL	NULL	NULL	SYSTEM VIEW	NULL\n"
			"NULL	NULL	NULL	TABLE	NULL\n"
			"NULL	NULL	NULL	UNLOGGED TABLE	NULL\n"
			"NULL	NULL	NULL	VIEW	NULL\n");

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
