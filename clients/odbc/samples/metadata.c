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
		/* next 3 tables copied from example in https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlforeignkeys-function?view=sql-server-ver15 */
		"CREATE TABLE \"CUSTOMERS\" (\"CUSTID\" INT PRIMARY KEY, \"NAME\" VARCHAR(60) NOT NULL, \"ADDRESS\" VARCHAR(90), \"PHONE\" VARCHAR(20));\n"
		"CREATE TABLE \"ORDERS\" (\"ORDERID\" INT PRIMARY KEY, \"CUSTID\" INT NOT NULL REFERENCES \"CUSTOMERS\" (\"CUSTID\"), \"OPENDATE\" DATE NOT NULL, \"SALESPERSON\" VARCHAR(60), \"STATUS\" VARCHAR(10) NOT NULL);\n"
		"CREATE TABLE \"LINES\" (\"ORDERID\" INT NOT NULL REFERENCES \"ORDERS\" (\"ORDERID\"), \"LINES\" INT, PRIMARY KEY (\"ORDERID\", \"LINES\"), \"PARTID\" INT NOT NULL, \"QUANTITY\" DECIMAL(9,3) NOT NULL);\n"
		/* also test situation where one table has multiple fks to the same multi column pk */
		"CREATE TABLE odbctst.pk2c (pkc1 INT, pkc2 VARCHAR(99), name1 VARCHAR(99) UNIQUE, PRIMARY KEY (pkc2, pkc1));\n"
		"CREATE TABLE odbctst.fk2c (fkc1 INT NOT NULL PRIMARY KEY, fkc2 VARCHAR(99), fkc3 INT"
		", FOREIGN KEY (fkc2, fkc1) REFERENCES odbctst.pk2c (pkc2, pkc1) ON UPDATE CASCADE ON DELETE RESTRICT"
		", FOREIGN KEY (fkc2, fkc3) REFERENCES odbctst.pk2c (pkc2, pkc1) ON UPDATE SET NULL ON DELETE NO ACTION);\n"
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

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"GRANT SELECT ON TABLE odbctst.pk_uc TO PUBLIC;\n"
		"GRANT INSERT, UPDATE, DELETE ON TABLE odbctst.pk_uc TO monetdb;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (add privileges script)");

/* run actual metadata query tests */
	// All catalogs query
	ret = SQLTables(stmt, (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_CATALOGS)",
		"Resultset with 5 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"mTests_sql_odbc_samples	NULL	NULL	NULL	NULL\n");

	// All schemas query
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)SQL_ALL_SCHEMAS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_SCHEMAS)",
		"Resultset with 5 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
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
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
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
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"mTests_sql_odbc_samples	odbctst	CUSTOMERS	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	LINES	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	ORDERS	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	fk2c	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	TABLE	NULL\n");

	// All user tables and views
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			(SQLCHAR*)"TABLE, VIEW, GLOBAL TEMPORARY TABLE, LOCAL TEMPORARY TABLE", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (%, %, TABLE, VIEW, GLOBAL TEMPORARY TABLE, LOCAL TEMPORARY TABLE)",
		"Resultset with 5 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	GLOBAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	GLOBAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	LOCAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	LOCAL TEMPORARY TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	CUSTOMERS	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	LINES	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	ORDERS	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	fk2c	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	TABLE	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	TABLE	NULL\n");

	// All columns of odbctst tables containg 'pk' in their name
	ret = SQLColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"%pk%", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumns (odbctst, %pk%, %)",
		"Resultset with 18 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	id2	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	name2	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	-9	VARCHAR	99	198	NULL	NULL	0	NULL	NULL	-9	NULL	198	2	NO\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	3	YES\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	id1	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n");

	// sys.table_types
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (sys, table_types)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"mTests_sql_odbc_samples	sys	table_types	table_type_id	1	table_types_table_type_id_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (sys, table_types)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"1	table_type_id	5	SMALLINT	16	6	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (sys, table_types, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_id_pkey	2	1	table_type_id	NULL	10	NULL	NULL\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_name_unique	2	1	table_type_name	NULL	10	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (sys, table_types, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_id_pkey	2	1	table_type_id	NULL	10	NULL	NULL\n"
		"mTests_sql_odbc_samples	sys	table_types	0	NULL	table_types_table_type_name_unique	2	1	table_type_name	NULL	10	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (sys, table_types)",
		"Resultset with 7 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (sys, table_types, %)",
		"Resultset with 8 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n");

	// odbctst.pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, pk_uc)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	id1	1	pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, pk_uc, SQL_BEST_ROWID)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLSpecialColumns(stmt, SQL_ROWVER, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, pk_uc, SQL_ROWVER)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (odbctst, pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (odbctst, pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	0	NULL	pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	1	NULL	pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	1	NULL	pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (odbctst, pk_uc)",
		"Resultset with 7 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	_SYSTEM	monetdb	DELETE	NO\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	_SYSTEM	monetdb	INSERT	NO\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	monetdb	PUBLIC	SELECT	NO\n"
		"mTests_sql_odbc_samples	odbctst	pk_uc	_SYSTEM	monetdb	UPDATE	NO\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (odbctst, pk_uc, %)",
		"Resultset with 8 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n");

	// tmp.tmp_pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, tmp_pk_uc)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	id1	1	tmp_pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, tmp_pk_uc)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_id1_pkey	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_name1_unique	2	1	name1	NULL	NULL	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_id1_pkey	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_name1_unique	2	1	name1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	1	NULL	tmp_pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_pk_uc	1	NULL	tmp_pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	// tmp.glbl_pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, glbl_pk_uc)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	id1	1	glbl_pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, glbl_pk_uc)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	1	NULL	glbl_pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_pk_uc	1	NULL	glbl_pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	// odbctst.nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, nopk_twoucs)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, nopk_twoucs)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (odbctst, nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (odbctst, nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	1	NULL	nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	odbctst	nopk_twoucs	1	NULL	nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	// tmp.tmp_nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, tmp_nopk_twoucs)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, tmp_nopk_twoucs)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_id2_unique	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_name2_unique	2	1	name2	NULL	NULL	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_id2_unique	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_name2_unique	2	1	name2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	1	NULL	tmp_nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	tmp_nopk_twoucs	1	NULL	tmp_nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	// tmp.glbl_nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, glbl_nopk_twoucs)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, glbl_nopk_twoucs)",
		"Resultset with 8 columns\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	1	NULL	glbl_nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"mTests_sql_odbc_samples	tmp	glbl_nopk_twoucs	1	NULL	glbl_nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	// odbctst.CUSTOMERS, odbctst.ORDERS and odbctst.LINES
	/* next tests are copied from code examples on https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlforeignkeys-function?view=sql-server-ver15 */
	ret = SQLPrimaryKeys(stmt, NULL, 0, NULL, 0, (SQLCHAR*)"ORDERS", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (NULL, ORDERS)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"mTests_sql_odbc_samples	odbctst	ORDERS	ORDERID	1	ORDERS_ORDERID_pkey\n");

	ret = SQLForeignKeys(stmt, NULL, 0, NULL, 0, (SQLCHAR*)"ORDERS", SQL_NTS, NULL, 0, NULL, 0, NULL, 0);
	compareResult(stmt, ret, "SQLForeignKeys (NULL, ORDERS, NULL, NULL)",
		"Resultset with 14 columns\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"mTests_sql_odbc_samples	odbctst	ORDERS	ORDERID	mTests_sql_odbc_samples	odbctst	LINES	ORDERID	1	1	1	LINES_ORDERID_fkey	ORDERS_ORDERID_pkey	7\n");

	ret = SQLForeignKeys(stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR*)"ORDERS", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (NULL, NULL, NULL, ORDERS)",
		"Resultset with 14 columns\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"mTests_sql_odbc_samples	odbctst	CUSTOMERS	CUSTID	mTests_sql_odbc_samples	odbctst	ORDERS	CUSTID	1	1	1	ORDERS_CUSTID_fkey	CUSTOMERS_CUSTID_pkey	7\n");

	// odbctst.pk2c and odbctst.fk2c (tests multi-column pks and multiple multi-column fks from one table */
	ret = SQLPrimaryKeys(stmt, NULL, 0, (SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk2c", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, pk2c)",
		"Resultset with 6 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	1	pk2c_pkc2_pkc1_pkey\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	2	pk2c_pkc2_pkc1_pkey\n");

	ret = SQLForeignKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk2c", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (odbctst, pk2c, , )",
		"Resultset with 14 columns\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	mTests_sql_odbc_samples	odbctst	fk2c	fkc2	1	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	mTests_sql_odbc_samples	odbctst	fk2c	fkc1	2	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	mTests_sql_odbc_samples	odbctst	fk2c	fkc2	1	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	mTests_sql_odbc_samples	odbctst	fk2c	fkc3	2	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n");

	ret = SQLForeignKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"fk2c", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (, , odbctst, fk2c)",
		"Resultset with 14 columns\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	mTests_sql_odbc_samples	odbctst	fk2c	fkc2	1	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	mTests_sql_odbc_samples	odbctst	fk2c	fkc1	2	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	mTests_sql_odbc_samples	odbctst	fk2c	fkc2	1	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	mTests_sql_odbc_samples	odbctst	fk2c	fkc3	2	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n");

	ret = SQLForeignKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk2c", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"fk2c", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (odbctst, pk2c, odbctst, fk2c)",
		"Resultset with 14 columns\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	mTests_sql_odbc_samples	odbctst	fk2c	fkc2	1	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	mTests_sql_odbc_samples	odbctst	fk2c	fkc1	2	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc2	mTests_sql_odbc_samples	odbctst	fk2c	fkc2	1	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"mTests_sql_odbc_samples	odbctst	pk2c	pkc1	mTests_sql_odbc_samples	odbctst	fk2c	fkc3	2	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n");

	// TODO add tables with procedures such that below calls also return data rows
	ret = SQLProcedures(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"myproc", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedures (odbctst, myproc)",
		"Resultset with 8 columns\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	NUM_INPUT_PARAMS	NUM_OUTPUT_PARAMS	NUM_RESULT_SETS	REMARKS	PROCEDURE_TYPE\n");

	ret = SQLProcedureColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"myproc", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedureColumns (odbctst, myproc, %)",
		"Resultset with 19 columns\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	COLUMN_NAME	COLUMN_TYPE	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE\n");

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
		"DROP TABLE odbctst.\"LINES\";\n"
		"DROP TABLE odbctst.\"ORDERS\";\n"
		"DROP TABLE odbctst.\"CUSTOMERS\";\n"
		"DROP TABLE odbctst.fk2c;\n"
		"DROP TABLE odbctst.pk2c;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLExecDirect (drop tables script)");

	// All tables in schema odbctst should be empty now, else we missed some DROP statements
	ret = SQLTables(stmt, NULL, 0,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			NULL, 0);
	compareResult(stmt, ret, "SQLTables (odbctst, %, NULL)",
		"Resultset with 5 columns\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n");

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
