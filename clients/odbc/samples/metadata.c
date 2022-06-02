/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#ifdef _MSC_VER
/* Visual Studio 8 has deprecated lots of stuff: suppress warnings */
#ifndef _CRT_SECURE_NO_DEPRECATE
#define _CRT_SECURE_NO_DEPRECATE 1
#endif
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
compareResult(SQLHANDLE stmt, SQLRETURN retcode, const char * functionname, const char * expected)
{
	SQLRETURN ret;
	SQLSMALLINT columns;	// Number of columns in result-set
	size_t expct_len = strlen(expected);
	size_t outp_len = expct_len + 1000;
	char * outp = malloc(outp_len);
	size_t pos = 0;
	SQLUSMALLINT col;
	SQLLEN indicator;
	char buf[2048];

	if (outp == NULL) {
		fprintf(stderr, "Failed to allocate %zu memory!\n", outp_len);
		return;
	}

	check(retcode, SQL_HANDLE_STMT, stmt, functionname);

	// How many columns are there
	ret = SQLNumResultCols(stmt, &columns);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLNumResultCols()");
	pos += snprintf(outp + pos, outp_len - pos, "Resultset with %d columns\n", columns);

	// get Result Column Names and print them
	for (col = 1; col <= columns; col++) {
		ret = SQLDescribeCol(stmt, col, (SQLCHAR *) buf, sizeof(buf),
			NULL, NULL, NULL, NULL, NULL);
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
				pos += snprintf(outp + pos, outp_len - pos,
					(col > 1) ? "\t%s" : "%s",
					// Handle null columns
					(indicator == SQL_NULL_DATA) ? "NULL" : buf);
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

	ret = SQLCloseCursor(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");
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

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"CREATE SCHEMA odbctst;\n"
		"SET SCHEMA odbctst;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (create and set schema script)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"CREATE TABLE odbctst.pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE);\n"
		"CREATE LOCAL TEMP TABLE tmp.tmp_pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE);\n"
		"CREATE GLOBAL TEMP TABLE tmp.glbl_pk_uc (id1 INT NOT NULL PRIMARY KEY, name1 VARCHAR(99) UNIQUE);\n"
		"CREATE TABLE odbctst.nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE);\n"
		"CREATE LOCAL TEMP TABLE tmp.tmp_nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE);\n"
		"CREATE GLOBAL TEMP TABLE tmp.glbl_nopk_twoucs (id2 INT NOT NULL UNIQUE, name2 VARCHAR(99) UNIQUE);\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (create tables script)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"CREATE INDEX pk_uc_i ON odbctst.pk_uc (id1, name1);\n"
		"CREATE INDEX tmp_pk_uc_i ON tmp.tmp_pk_uc (id1, name1);\n"
		"CREATE INDEX glbl_pk_uc_i ON tmp.glbl_pk_uc (id1, name1);\n"
		"CREATE INDEX nopk_twoucs_i ON odbctst.nopk_twoucs (id2, name2);\n"
		"CREATE INDEX tmp_nopk_twoucs_i ON tmp.tmp_nopk_twoucs (id2, name2);\n"
		"CREATE INDEX glbl_nopk_twoucs_i ON tmp.glbl_nopk_twoucs (id2, name2);\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (create indices script)");

/* run actual metadata query tests */
	// All catalogs query
	ret = SQLTables(stmt, (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_CATALOGS)",
		"Resultset with 5 columns\n"
		"table_cat	table_schem	table_name	table_type	remarks\n"
		"mTests_sql_odbc_samples	NULL	NULL	NULL	NULL\n");

	// All schemas query
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)SQL_ALL_SCHEMAS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_SCHEMAS)",
		"Resultset with 5 columns\n"
		"table_cat	table_schem	table_name	table_type	remarks\n"
		"NULL	json	NULL	NULL	NULL\n"
		"NULL	logging	NULL	NULL	NULL\n"
		"NULL	odbctst	NULL	NULL	NULL\n"
		"NULL	profiler	NULL	NULL	NULL\n"
		"NULL	sys	NULL	NULL	NULL\n"
		"NULL	tmp	NULL	NULL	NULL\n"
		"NULL	wlc	NULL	NULL	NULL\n"
		"NULL	wlr	NULL	NULL	NULL\n");

	// All table types query
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)SQL_ALL_TABLE_TYPES, SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_TABLE_TYPES)",
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

	// All tables in schema odbctst
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			(SQLCHAR*)"TABLE, VIEW, SYSTEM TABLE, GLOBAL TEMPORARY TABLE, LOCAL TEMPORARY TABLE, ALIAS, SYNONYM", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (odbctst, %)",
		"Resultset with 5 columns\n"
		"table_cat	table_schem	table_name	table_type	remarks\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	TABLE	NULL\n");

	// All user tables and views
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			(SQLCHAR*)"TABLE, VIEW, GLOBAL TEMPORARY TABLE, LOCAL TEMPORARY TABLE", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (%, %, TABLE, VIEW, GLOBAL TEMPORARY TABLE, LOCAL TEMPORARY TABLE)",
		"Resultset with 5 columns\n"
		"table_cat	table_schem	table_name	table_type	remarks\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	GLOBAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	GLOBAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	LOCAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	LOCAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	TABLE	NULL\n");

	// All columns of the odbctst tables
	ret = SQLColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"%pk%", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumns (odbctst, %pk%, %)",
		"Resultset with 18 columns\n"
		"table_cat	table_schem	table_name	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	num_prec_radix	nullable	remarks	column_def	sql_data_type	sql_datetime_sub	char_octet_length	ordinal_position	is_nullable\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	id2	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	name2	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	id1	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n");

	// sys.table_types
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (sys, table_types)",
		"Resultset with 6 columns\n"
		"table_cat	table_schem	table_name	column_name	key_seq	pk_name\n"
		"mTests_sql_odbc_samples	sys	table_types	table_type_id	1	table_types_table_type_id_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (sys, table_types)",
		"Resultset with 8 columns\n"
		"scope	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	pseudo_column\n"
		"1	table_type_id	5	SMALLINT	16	6	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (sys, table_types, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_id_pkey	2	1	table_type_id	NULL	10	NULL	NULL\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_name_unique	2	1	table_type_name	NULL	10	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (sys, table_types, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_id_pkey	2	1	table_type_id	NULL	10	NULL	NULL\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_name_unique	2	1	table_type_name	NULL	10	NULL	NULL\n");

	// odbctst.pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, pk_uc)",
		"Resultset with 6 columns\n"
		"table_cat	table_schem	table_name	column_name	key_seq	pk_name\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	id1	1	pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, pk_uc)",
		"Resultset with 8 columns\n"
		"scope	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	pseudo_column\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (odbctst, pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (odbctst, pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	1	NULL	pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	1	NULL	pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	// tmp.tmp_pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, tmp_pk_uc)",
		"Resultset with 6 columns\n"
		"table_cat	table_schem	table_name	column_name	key_seq	pk_name\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	id1	1	tmp_pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, tmp_pk_uc)",
		"Resultset with 8 columns\n"
		"scope	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	pseudo_column\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_id1_pkey	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_name1_unique	2	1	name1	NULL	NULL	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_id1_pkey	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_name1_unique	2	1	name1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	1	NULL	tmp_pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	1	NULL	tmp_pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	// tmp.glbl_pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, glbl_pk_uc)",
		"Resultset with 6 columns\n"
		"table_cat	table_schem	table_name	column_name	key_seq	pk_name\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	id1	1	glbl_pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, glbl_pk_uc)",
		"Resultset with 8 columns\n"
		"scope	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	pseudo_column\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	1	NULL	glbl_pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	1	NULL	glbl_pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	// odbctst.nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, nopk_twoucs)",
		"Resultset with 6 columns\n"
		"table_cat	table_schem	table_name	column_name	key_seq	pk_name\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, nopk_twoucs)",
		"Resultset with 8 columns\n"
		"scope	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	pseudo_column\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (odbctst, nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (odbctst, nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	1	NULL	nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	1	NULL	nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	// tmp.tmp_nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, tmp_nopk_twoucs)",
		"Resultset with 6 columns\n"
		"table_cat	table_schem	table_name	column_name	key_seq	pk_name\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, tmp_nopk_twoucs)",
		"Resultset with 8 columns\n"
		"scope	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	pseudo_column\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_id2_unique	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_name2_unique	2	1	name2	NULL	NULL	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_id2_unique	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_name2_unique	2	1	name2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	1	NULL	tmp_nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	1	NULL	tmp_nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	// tmp.glbl_nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, glbl_nopk_twoucs)",
		"Resultset with 6 columns\n"
		"table_cat	table_schem	table_name	column_name	key_seq	pk_name\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, glbl_nopk_twoucs)",
		"Resultset with 8 columns\n"
		"scope	column_name	data_type	type_name	column_size	buffer_length	decimal_digits	pseudo_column\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"table_cat	table_schem	table_name	non_unique	index_qualifier	index_name	type	ordinal_position	column_name	asc_or_desc	cardinality	pages	filter_condition\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	1	NULL	glbl_nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	1	NULL	glbl_nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	// cleanup
	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"DROP INDEX odbctst.pk_uc_i;\n"
		"DROP INDEX tmp.tmp_pk_uc_i;\n"
		"DROP INDEX tmp.glbl_pk_uc_i;\n"
		"DROP INDEX odbctst.nopk_twoucs_i;\n"
		"DROP INDEX tmp.tmp_nopk_twoucs_i;\n"
		"DROP INDEX tmp.glbl_nopk_twoucs_i;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (drop indices script)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"DROP TABLE odbctst.pk_uc;\n"
		"DROP TABLE tmp.tmp_pk_uc;\n"
		"DROP TABLE tmp.glbl_pk_uc;\n"
		"DROP TABLE odbctst.nopk_twoucs;\n"
		"DROP TABLE tmp.tmp_nopk_twoucs;\n"
		"DROP TABLE tmp.glbl_nopk_twoucs;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (drop tables script)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"SET SCHEMA sys;\n"
		"DROP SCHEMA odbctst;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (drop schema script)");

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
