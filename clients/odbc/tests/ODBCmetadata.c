/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * MonetDB program to test ODBC metadata/catalog functions (all return a result-set):
 * SQLTables()
 * SQLColumns()
 * SQLSpecialColumns()
 * SQLPrimaryKeys()
 * SQLForeignKeys()
 * SQLStatistics()
 * SQLTablePrivileges()
 * SQLColumnPrivileges()
 * SQLProcedures()
 * SQLProcedureColumns()
 * SQLGetTypeInfo()
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
#include <inttypes.h>
#define ODBCVER 0x0352		/* Important: this must be defined before include of sql.h and sqlext.h */
#include <sql.h>
#include <sqlext.h>
#include <string.h>

#define SQL_HUGEINT	0x4000	/* as defined in ODBCGlobal.h */

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

static char *
nameofSQLtype(SQLSMALLINT dataType)
{
	/* https://docs.microsoft.com/en-us/sql/odbc/reference/appendixes/sql-data-types */
	switch (dataType) {
	case SQL_CHAR:		return "CHAR";
	case SQL_VARCHAR:	return "VARCHAR";
	case SQL_LONGVARCHAR:	return "LONG VARCHAR";
	case SQL_WCHAR:		return "WCHAR";
	case SQL_WVARCHAR:	return "WVARCHAR";
	case SQL_WLONGVARCHAR:	return "WLONGVARCHAR";
	case SQL_DECIMAL:	return "DECIMAL";
	case SQL_NUMERIC:	return "NUMERIC";
	case SQL_SMALLINT:	return "SMALLINT";
	case SQL_INTEGER:	return "INTEGER";
	case SQL_REAL:		return "REAL";
	case SQL_FLOAT:		return "FLOAT";
	case SQL_DOUBLE:	return "DOUBLE";
	case SQL_BIT:   	return "BOOLEAN";	/* MonetDB boolean type is mapped to SQL_BIT in ODBC (see msql_types[] in SQLExecute.c) */
	case SQL_TINYINT:	return "TINYINT";
	case SQL_BIGINT:	return "BIGINT";
	case SQL_BINARY:	return "BINARY";
	case SQL_VARBINARY:	return "VARBINARY";
	case SQL_LONGVARBINARY:	return "LONG VARBINARY";
	case SQL_DATETIME:	return "DATETIME";
	case SQL_TYPE_DATE:	return "DATE";
	case SQL_TYPE_TIME:	return "TIME";
	case SQL_TYPE_TIMESTAMP:	return "TIMESTAMP";
	case SQL_INTERVAL_MONTH:	return "INTERVAL MONTH";
	case SQL_INTERVAL_YEAR: 	return "INTERVAL YEAR";
	case SQL_INTERVAL_YEAR_TO_MONTH: return "INTERVAL YEAR TO MONTH";
	case SQL_INTERVAL_DAY:		return "INTERVAL DAY";
	case SQL_INTERVAL_HOUR:		return "INTERVAL HOUR";
	case SQL_INTERVAL_MINUTE:	return "INTERVAL MINUTE";
	case SQL_INTERVAL_SECOND:	return "INTERVAL SECOND";
	case SQL_INTERVAL_DAY_TO_HOUR:	return "INTERVAL DAY TO HOUR";
	case SQL_INTERVAL_DAY_TO_MINUTE:	return "INTERVAL DAY TO MINUTE";
	case SQL_INTERVAL_DAY_TO_SECOND:	return "INTERVAL DAY TO SECOND";
	case SQL_INTERVAL_HOUR_TO_MINUTE:	return "INTERVAL HOUR TO MINUTE";
	case SQL_INTERVAL_HOUR_TO_SECOND:	return "INTERVAL HOUR TO SECOND";
	case SQL_INTERVAL_MINUTE_TO_SECOND:	return "INTERVAL MINUTE TO SECOND";
	case SQL_GUID:		return "GUID";
	case SQL_HUGEINT:	return "HUGEINT";	/* 0x4000 (defined in ODBCGlobal.h) */
	default:		return "Undefined";
	}
}

static void
compareResultOptClose(SQLHANDLE stmt, SQLRETURN retcode, const char * functionname, const char * expected, int closeCursor)
{
	SQLRETURN ret;
	SQLSMALLINT columns;	/* Number of columns in result-set */
	SQLLEN rows;		/* Number of rows in result-set */
	size_t expct_len = strlen(expected);
	size_t outp_len = expct_len + 10000;
	char * outp = NULL;
	size_t pos = 0;
	SQLUSMALLINT col;
	SQLLEN indicator;
	char buf[2048];
	SQLSMALLINT dataType = 0;
	SQLULEN columnSize = 0;
	SQLSMALLINT decimalDigits = 0;
	int replaceId = 0;	/* used to replace system id values in column SPECIFIC_NAME of getProcedures and getProcedureColumns */
	int replaceTraceData = 0; /* used to replace second result set data of a TRACE query */

	check(retcode, SQL_HANDLE_STMT, stmt, functionname);
	if (retcode != SQL_SUCCESS && retcode != SQL_SUCCESS_WITH_INFO) {
		fprintf(stderr, "Invalid retcode (%d). Skipping compareResult(%s)\n", retcode, functionname);
		return;
	}

	outp = malloc(outp_len);
	if (outp == NULL) {
		fprintf(stderr, "Failed to allocate %zu memory!\n", outp_len);
		return;
	}

	/* How many columns are there */
	ret = SQLNumResultCols(stmt, &columns);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLNumResultCols()");
	pos += snprintf(outp + pos, outp_len - pos, "Resultset with %d columns\n", columns);

	/* How many rows are there */
	ret = SQLRowCount(stmt, &rows);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLRowCount()");
	pos += snprintf(outp + pos, outp_len - pos, "Resultset with %"PRId64" rows\n", (int64_t) rows);

	/* get Result Column Names and print them */
	for (col = 1; col <= columns; col++) {
		ret = SQLDescribeCol(stmt, col, (SQLCHAR *) buf, sizeof(buf),
			NULL, NULL, NULL, NULL, NULL);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeCol(colName)");
		pos += snprintf(outp + pos, outp_len - pos,
				(col > 1) ? "\t%s" : "%s", buf);
	}
	pos += snprintf(outp + pos, outp_len - pos, "\n");
	/* get Result Column Data Types and print them */
	for (col = 1; col <= columns; col++) {
		ret = SQLDescribeCol(stmt, col, (SQLCHAR *) buf, sizeof(buf),
			NULL, &dataType, &columnSize, &decimalDigits, NULL);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLDescribeCol(colType)");
		pos += snprintf(outp + pos, outp_len - pos,
				(col > 1) ? "\t%s" : "%s", nameofSQLtype(dataType));
		switch (dataType) {
		case SQL_CHAR:
		case SQL_VARCHAR:
		case SQL_LONGVARCHAR:
		case SQL_WCHAR:
		case SQL_WVARCHAR:
		case SQL_WLONGVARCHAR:
		case SQL_DECIMAL:
		case SQL_NUMERIC:
		case SQL_BINARY:
		case SQL_VARBINARY:
		case SQL_LONGVARBINARY:
			if (columnSize != 0) {
				if (decimalDigits != 0) {
					pos += snprintf(outp + pos, outp_len - pos,
						"(%d,%d)", (int) columnSize, (int) decimalDigits);
				} else {
					pos += snprintf(outp + pos, outp_len - pos,
						"(%d)", (int) columnSize);
				}
			}
			break;
		}
	}
	pos += snprintf(outp + pos, outp_len - pos, "\n");

	/* detect if special handling of data of column SPECIFIC_NAME returned by SQLProcedures and SQLProcedureColumns
	   is needed as it contains system generated id values which can differ per version and platform */
	if (columns == 9 || columns == 20) {
		/* this result could be from SQLProcedures or SQLProcedureColumns */
		if ((strncmp("SQLProcedures", functionname, 13) == 0)
		 || (strncmp("SQLProcedureColumns", functionname, 19) == 0)) {
			if (strncmp("SPECIFIC_NAME", buf, 13) == 0)
				replaceId = 1;
		}
	}

	/* detect if special handling of data returned by second TRACE resultset is needed */
	if (columns == 2 && (strncmp("TRACE(2) ", functionname, 9) == 0)) {
		replaceTraceData = 1;
	}

	/* Loop through the rows in the result-set */
	ret = SQLFetch(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch(1)");
	while (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
		/* Loop through the columns */
		for (col = 1; col <= columns; col++) {
			/* Retrieve column data as a string */
			ret = SQLGetData(stmt, col, SQL_C_CHAR, buf, sizeof(buf), &indicator);
			check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData()");
			if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
				/* some rows of EXPLAIN output (which has only 1 result column) must be surpressed to get stable output */
				if (columns == 1 &&
				    (strncmp(buf, "# optimizer.", 12) == 0 ||
				     strncmp(buf, "barrier X_", 10) == 0 ||
				     strncmp(buf, "exit X_", 7) == 0) ) {
					continue;
				}

				if (replaceTraceData == 1) {
					pos += snprintf(outp + pos, outp_len - pos,
						(col == 1) ? "4" : "\tvariable output");
					continue;
				}

				/* Check if we need to replace the system id values to get stable output */
				if (replaceId == 0 ||
				   (replaceId == 1 && col < columns)) {
					pos += snprintf(outp + pos, outp_len - pos,
						(col > 1) ? "\t%s" : "%s",
						/* Handle null columns */
						(indicator == SQL_NULL_DATA) ? "NULL" : buf);
				} else {
					pos += snprintf(outp + pos, outp_len - pos, "\treplacedId");
				}
			}
		}
		pos += snprintf(outp + pos, outp_len - pos, "\n");
		ret = SQLFetch(stmt);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch(n)");
	}

	if (strcmp(expected, outp) != 0) {
		size_t len_expected = strlen(expected);
		size_t len_outp = strlen(outp);
		int c = 0;
		int line = 1;
		int pos = 1;

		fprintf(stderr, "Testing %s\nExpected (strlen=%zu):\n%s\nGotten (strlen=%zu):\n%s\n",
			functionname, len_expected, expected, len_outp, outp);

		/* scan string to find location (line and position in line) of first character difference */
		while (expected[c] != '\0' && outp[c] != '\0' && expected[c] == outp[c]) {
			if (expected[c] == '\n') {
				line++;
				pos = 0;
			}
			c++;
			pos++;
		}
		fprintf(stderr, "First difference found at line %d, position %d, data: %-20s\n\n",
			line, pos, (expected[c] != '\0' ? &expected[c] : &outp[c]) );
	}

	/* cleanup */
	free(outp);

	if (closeCursor == 1) {
		ret = SQLCloseCursor(stmt);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");
	}
}

#define compareResultNoClose(stmt, retcode, functionname, expected)  compareResultOptClose(stmt, retcode, functionname, expected, 0)
#define compareResult(stmt, retcode, functionname, expected)         compareResultOptClose(stmt, retcode, functionname, expected, 1)

/*
 * Utility function to query the gdk_nr_threads value from the server.
 * The output of some queries (EXPLAIN, TRACE) differ when the server
 * is started with 1 thread, as is done in our testweb.
 */
static int
getNrOfServerThreads(SQLHANDLE dbc)
{
	SQLRETURN ret;
	SQLHANDLE stmt;
	SQLLEN indicator;
	int threads = 0;

	ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
	check(ret, SQL_HANDLE_DBC, dbc, "SQLAllocHandle (STMT)");

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"SELECT cast(value as int) as val from sys.env() where name = 'gdk_nr_threads';"
		, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "select gdk_nr_threads");

	ret = SQLFetch(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLFetch(gdk_nr_threads)");
	if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
		ret = SQLGetData(stmt, 1, SQL_C_LONG, &threads, sizeof(threads), &indicator);
		check(ret, SQL_HANDLE_STMT, stmt, "SQLGetData(gdk_nr_threads)");
	}
	ret = SQLCloseCursor(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLCloseCursor");

	/* fprintf(stderr, "getNrOfServerThreads: %d\n", threads); */
	return threads;
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
	int nrServerThreads = 0;

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

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"CREATE SCHEMA odbctst;\n"
		"SET SCHEMA odbctst;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (create and set schema script)");

	// create tables to populate catalog. Used for testing SQLTables(),
	// SQLColumns(), SQLSpecialColumns(), SQLPrimaryKeys() and SQLForeignKeys()
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
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (create tables script)");

	// create indexes to populate catalog. Used for testing SQLStatistics()
	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"CREATE INDEX pk_uc_i ON odbctst.pk_uc (id1, name1);\n"
		"CREATE INDEX tmp_pk_uc_i ON tmp.tmp_pk_uc (id1, name1);\n"
		"CREATE INDEX glbl_pk_uc_i ON tmp.glbl_pk_uc (id1, name1);\n"
		"CREATE INDEX nopk_twoucs_i ON odbctst.nopk_twoucs (id2, name2);\n"
		"CREATE INDEX tmp_nopk_twoucs_i ON tmp.tmp_nopk_twoucs (id2, name2);\n"
		"CREATE INDEX glbl_nopk_twoucs_i ON tmp.glbl_nopk_twoucs (id2, name2);\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (create indices script)");

	// grant privileges to populate catalog. Used for testing SQLTablePrivileges() and SQLColumnPrivileges()
	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"GRANT SELECT ON TABLE odbctst.pk_uc TO PUBLIC;\n"
		"GRANT INSERT, UPDATE, DELETE ON TABLE odbctst.pk_uc TO monetdb;\n"
		"GRANT SELECT (id2, name2), UPDATE (name2) ON TABLE odbctst.nopk_twoucs TO monetdb;\n"
		"GRANT INSERT, DELETE ON TABLE tmp.tmp_pk_uc TO monetdb;\n"
		"GRANT SELECT (id1, name1), UPDATE (name1) ON TABLE tmp.tmp_pk_uc TO monetdb;\n"
		"GRANT INSERT, DELETE ON TABLE tmp.glbl_pk_uc TO monetdb;\n"
		"GRANT SELECT (id1, name1), UPDATE (name1) ON TABLE tmp.glbl_pk_uc TO monetdb;\n"
		"GRANT INSERT, DELETE ON TABLE tmp.tmp_nopk_twoucs TO monetdb;\n"
		"GRANT SELECT (id2, name2), UPDATE (name2) ON TABLE tmp.tmp_nopk_twoucs TO monetdb;\n"
		"GRANT DELETE, INSERT ON TABLE tmp.glbl_nopk_twoucs TO monetdb;\n"
		"GRANT SELECT (id2, name2), UPDATE (name2) ON TABLE tmp.glbl_nopk_twoucs TO monetdb;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (add privileges script)");

	// TODO add user procedures / functions to test SQLProcedures() and SQLProcedureColumns() more

	// set COMMENT ON schema, tables, columns, indexes, procedures and functions to fetch (and test) data in the REMARKS result column
	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"COMMENT ON SCHEMA odbctst IS 'odbctst schema comment';\n"
		"COMMENT ON TABLE odbctst.pk_uc IS 'odbctst.pk_uc table comment';\n"
		"COMMENT ON TABLE odbctst.nopk_twoucs IS 'odbctst.nopk_twoucs table comment';\n"
		"COMMENT ON COLUMN odbctst.nopk_twoucs.id2 IS 'odbctst.nopk_twoucs.id2 column comment';\n"
		"COMMENT ON COLUMN odbctst.nopk_twoucs.name2 IS 'odbctst.nopk_twoucs.name2 column comment';\n"
		"COMMENT ON INDEX odbctst.pk_uc_i IS 'odbctst.pk_uc_i index comment';\n"
		"COMMENT ON INDEX odbctst.nopk_twoucs_i IS 'odbctst.nopk_twoucs_i index comment';\n"
		"COMMENT ON PROCEDURE sys.analyze() IS 'sys.analyze() procedure comment';\n"
		"COMMENT ON FUNCTION sys.sin(double) IS 'sys.sin(double) function comment';\n"
		"COMMENT ON FUNCTION sys.env() IS 'sys.env() function comment';\n"
		"COMMENT ON FUNCTION sys.statistics() IS 'sys.statistics() function comment';\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (add comments script)");

/* run actual metadata query tests */
	// All catalogs query. MonetDB should return no rows. Catalog qualifier not supported.
	ret = SQLTables(stmt, (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_CATALOGS)",
		"Resultset with 5 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"WVARCHAR(1)	WVARCHAR(1)	WVARCHAR(1)	WVARCHAR(1)	WVARCHAR(1)\n");

	// All schemas query. All columns except the TABLE_SCHEM column should contain NULLs.
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)SQL_ALL_SCHEMAS, SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_SCHEMAS)",
		"Resultset with 5 columns\n"
		"Resultset with 8 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1)	WVARCHAR(1)	WVARCHAR(1)\n"
		"NULL	json	NULL	NULL	NULL\n"
		"NULL	logging	NULL	NULL	NULL\n"
		"NULL	odbctst	NULL	NULL	NULL\n"
		"NULL	profiler	NULL	NULL	NULL\n"
		"NULL	sys	NULL	NULL	NULL\n"
		"NULL	tmp	NULL	NULL	NULL\n"
		"NULL	wlc	NULL	NULL	NULL\n"
		"NULL	wlr	NULL	NULL	NULL\n");

	// All table types query. All columns except the TABLE_TYPE column should contain NULLs.
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)SQL_ALL_TABLE_TYPES, SQL_NTS);
	compareResult(stmt, ret, "SQLTables (SQL_ALL_TABLE_TYPES)",
		"Resultset with 5 columns\n"
		"Resultset with 10 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"WVARCHAR(1)	WVARCHAR(1)	WVARCHAR(1)	WVARCHAR(25)	WVARCHAR(1)\n"
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
			(SQLCHAR*)"TABLE,VIEW,SYSTEM TABLE,GLOBAL TEMPORARY TABLE,LOCAL TEMPORARY TABLE,ALIAS,SYNONYM", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (odbctst, %)",
		"Resultset with 5 columns\n"
		"Resultset with 7 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(25)	WVARCHAR(65000)\n"
		"NULL	odbctst	CUSTOMERS	TABLE	NULL\n"
		"NULL	odbctst	LINES	TABLE	NULL\n"
		"NULL	odbctst	ORDERS	TABLE	NULL\n"
		"NULL	odbctst	fk2c	TABLE	NULL\n"
		"NULL	odbctst	nopk_twoucs	TABLE	odbctst.nopk_twoucs table comment\n"
		"NULL	odbctst	pk2c	TABLE	NULL\n"
		"NULL	odbctst	pk_uc	TABLE	odbctst.pk_uc table comment\n");

	// All user tables and views in schema odbctst
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			(SQLCHAR*)"'TABLE' , 'VIEW'", SQL_NTS);	// using quotes around the type names
	compareResult(stmt, ret, "SQLTables (odbctst, %, 'TABLE' , 'VIEW')",
		"Resultset with 5 columns\n"
		"Resultset with 7 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(25)	WVARCHAR(65000)\n"
		"NULL	odbctst	CUSTOMERS	TABLE	NULL\n"
		"NULL	odbctst	LINES	TABLE	NULL\n"
		"NULL	odbctst	ORDERS	TABLE	NULL\n"
		"NULL	odbctst	fk2c	TABLE	NULL\n"
		"NULL	odbctst	nopk_twoucs	TABLE	odbctst.nopk_twoucs table comment\n"
		"NULL	odbctst	pk2c	TABLE	NULL\n"
		"NULL	odbctst	pk_uc	TABLE	odbctst.pk_uc table comment\n");

	// All user tables and views in all schemas
	ret = SQLTables(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			(SQLCHAR*)"TABLE, VIEW, GLOBAL TEMPORARY TABLE, LOCAL TEMPORARY TABLE", SQL_NTS);
	compareResult(stmt, ret, "SQLTables (%, %, TABLE, VIEW, GLOBAL TEMPORARY TABLE, LOCAL TEMPORARY TABLE)",
		"Resultset with 5 columns\n"
		"Resultset with 11 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(25)	WVARCHAR(65000)\n"
		"NULL	tmp	glbl_nopk_twoucs	GLOBAL TEMPORARY TABLE	NULL\n"
		"NULL	tmp	glbl_pk_uc	GLOBAL TEMPORARY TABLE	NULL\n"
		"NULL	tmp	tmp_nopk_twoucs	LOCAL TEMPORARY TABLE	NULL\n"
		"NULL	tmp	tmp_pk_uc	LOCAL TEMPORARY TABLE	NULL\n"
		"NULL	odbctst	CUSTOMERS	TABLE	NULL\n"
		"NULL	odbctst	LINES	TABLE	NULL\n"
		"NULL	odbctst	ORDERS	TABLE	NULL\n"
		"NULL	odbctst	fk2c	TABLE	NULL\n"
		"NULL	odbctst	nopk_twoucs	TABLE	odbctst.nopk_twoucs table comment\n"
		"NULL	odbctst	pk2c	TABLE	NULL\n"
		"NULL	odbctst	pk_uc	TABLE	odbctst.pk_uc table comment\n");

	// All columns of odbctst tables containg 'pk' in their name
	ret = SQLColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"%pk%", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumns (odbctst, %pk%, %)",
		"Resultset with 18 columns\n"
		"Resultset with 7 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(65000)	WVARCHAR(2048)	SMALLINT	SMALLINT	INTEGER	INTEGER	WVARCHAR(3)\n"
		"NULL	odbctst	nopk_twoucs	id2	4	INTEGER	32	11	0	2	0	odbctst.nopk_twoucs.id2 column comment	NULL	4	NULL	NULL	1	NO\n"
		"NULL	odbctst	nopk_twoucs	name2	-9	VARCHAR	99	198	NULL	NULL	1	odbctst.nopk_twoucs.name2 column comment	NULL	-9	NULL	198	2	YES\n"
		"NULL	odbctst	pk2c	pkc1	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"NULL	odbctst	pk2c	pkc2	-9	VARCHAR	99	198	NULL	NULL	0	NULL	NULL	-9	NULL	198	2	NO\n"
		"NULL	odbctst	pk2c	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	3	YES\n"
		"NULL	odbctst	pk_uc	id1	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"NULL	odbctst	pk_uc	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n");

	// All columns of all tmp tables containg 'pk' in their name
	ret = SQLColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"%pk%", SQL_NTS,
			(SQLCHAR*)"%%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumns (tmp, %pk%, %%)",
		"Resultset with 18 columns\n"
		"Resultset with 8 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(65000)	WVARCHAR(2048)	SMALLINT	SMALLINT	INTEGER	INTEGER	WVARCHAR(3)\n"
		"NULL	tmp	glbl_nopk_twoucs	id2	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"NULL	tmp	glbl_nopk_twoucs	name2	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"NULL	tmp	glbl_pk_uc	id1	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"NULL	tmp	glbl_pk_uc	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"NULL	tmp	tmp_nopk_twoucs	id2	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"NULL	tmp	tmp_nopk_twoucs	name2	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"NULL	tmp	tmp_pk_uc	id1	4	INTEGER	32	11	0	2	0	NULL	NULL	4	NULL	NULL	1	NO\n"
		"NULL	tmp	tmp_pk_uc	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n");

	// All columns of all tmp tables containg 'pk' in their name and the column matching name_ pattern
	ret = SQLColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"%pk%", SQL_NTS,
			(SQLCHAR*)"name_", SQL_NTS);
	compareResult(stmt, ret, "SQLColumns (tmp, %pk%, name_)",
		"Resultset with 18 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(65000)	WVARCHAR(2048)	SMALLINT	SMALLINT	INTEGER	INTEGER	WVARCHAR(3)\n"
		"NULL	tmp	glbl_nopk_twoucs	name2	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"NULL	tmp	glbl_pk_uc	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"NULL	tmp	tmp_nopk_twoucs	name2	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n"
		"NULL	tmp	tmp_pk_uc	name1	-9	VARCHAR	99	198	NULL	NULL	1	NULL	NULL	-9	NULL	198	2	YES\n");

	ret = SQLSpecialColumns(stmt, SQL_ROWVER, (SQLCHAR*)NULL, 0,
			(SQLCHAR*)"%", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (%, %, SQL_ROWVER)",
		"Resultset with 8 columns\n"
		"Resultset with 0 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1)	SMALLINT	WVARCHAR(4)	INTEGER	INTEGER	SMALLINT	SMALLINT\n");

	// sys.table_types
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (sys, table_types)",
		"Resultset with 6 columns\n"
		"Resultset with 1 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n"
		"NULL	sys	table_types	table_type_id	1	table_types_table_type_id_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (sys, table_types)",
		"Resultset with 8 columns\n"
		"Resultset with 1 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	table_type_id	5	SMALLINT	16	6	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (sys, table_types, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	sys	table_types	0	NULL	table_types_table_type_id_pkey	2	1	table_type_id	NULL	10	NULL	NULL\n"
		"NULL	sys	table_types	0	NULL	table_types_table_type_name_unique	2	1	table_type_name	NULL	10	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (sys, table_types, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	sys	table_types	0	NULL	table_types_table_type_id_pkey	2	1	table_type_id	NULL	10	NULL	NULL\n"
		"NULL	sys	table_types	0	NULL	table_types_table_type_name_unique	2	1	table_type_name	NULL	10	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (sys, table_types)",
		"Resultset with 7 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"table_types", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (sys, table_types, %)",
		"Resultset with 8 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n");

	// odbctst.pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, pk_uc)",
		"Resultset with 6 columns\n"
		"Resultset with 1 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n"
		"NULL	odbctst	pk_uc	id1	1	pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, pk_uc, SQL_BEST_ROWID)",
		"Resultset with 8 columns\n"
		"Resultset with 1 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLSpecialColumns(stmt, SQL_ROWVER, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, pk_uc, SQL_ROWVER)",
		"Resultset with 8 columns\n"
		"Resultset with 0 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1)	SMALLINT	WVARCHAR(4)	INTEGER	INTEGER	SMALLINT	SMALLINT\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (odbctst, pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	odbctst	pk_uc	0	NULL	pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"NULL	odbctst	pk_uc	0	NULL	pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (odbctst, pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	odbctst	pk_uc	0	NULL	pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"NULL	odbctst	pk_uc	0	NULL	pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n"
		"NULL	odbctst	pk_uc	1	NULL	pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"NULL	odbctst	pk_uc	1	NULL	pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (odbctst, pk_uc)",
		"Resultset with 7 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	odbctst	pk_uc	_SYSTEM	monetdb	DELETE	NO\n"
		"NULL	odbctst	pk_uc	_SYSTEM	monetdb	INSERT	NO\n"
		"NULL	odbctst	pk_uc	monetdb	PUBLIC	SELECT	NO\n"
		"NULL	odbctst	pk_uc	_SYSTEM	monetdb	UPDATE	NO\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_uc", SQL_NTS,
			(SQLCHAR*)"%1", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (odbctst, pk_uc, %1)",
		"Resultset with 8 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n");

	// tmp.tmp_pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, tmp_pk_uc)",
		"Resultset with 6 columns\n"
		"Resultset with 1 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n"
		"NULL	tmp	tmp_pk_uc	id1	1	tmp_pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, tmp_pk_uc)",
		"Resultset with 8 columns\n"
		"Resultset with 1 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_id1_pkey	2	1	id1	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_name1_unique	2	1	name1	NULL	NULL	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_id1_pkey	2	1	id1	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_pk_uc	0	NULL	tmp_pk_uc_name1_unique	2	1	name1	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_pk_uc	1	NULL	tmp_pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_pk_uc	1	NULL	tmp_pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (tmp, tmp_pk_uc)",
		"Resultset with 7 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	tmp_pk_uc	_SYSTEM	monetdb	DELETE	NO\n"
		"NULL	tmp	tmp_pk_uc	_SYSTEM	monetdb	INSERT	NO\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_pk_uc", SQL_NTS,
			(SQLCHAR*)"%1", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (tmp, tmp_pk_uc, %1)",
		"Resultset with 8 columns\n"
		"Resultset with 3 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	tmp_pk_uc	id1	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	tmp_pk_uc	name1	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	tmp_pk_uc	name1	_SYSTEM	monetdb	UPDATE	NO\n");

	// tmp.glbl_pk_uc
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, glbl_pk_uc)",
		"Resultset with 6 columns\n"
		"Resultset with 1 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n"
		"NULL	tmp	glbl_pk_uc	id1	1	glbl_pk_uc_id1_pkey\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, glbl_pk_uc)",
		"Resultset with 8 columns\n"
		"Resultset with 1 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	id1	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_pk_uc, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"NULL	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_pk_uc, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_id1_pkey	2	1	id1	NULL	0	NULL	NULL\n"
		"NULL	tmp	glbl_pk_uc	0	NULL	glbl_pk_uc_name1_unique	2	1	name1	NULL	0	NULL	NULL\n"
		"NULL	tmp	glbl_pk_uc	1	NULL	glbl_pk_uc_i	2	1	id1	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	glbl_pk_uc	1	NULL	glbl_pk_uc_i	2	2	name1	NULL	NULL	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (tmp, glbl_pk_uc)",
		"Resultset with 7 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	glbl_pk_uc	_SYSTEM	monetdb	DELETE	NO\n"
		"NULL	tmp	glbl_pk_uc	_SYSTEM	monetdb	INSERT	NO\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_pk_uc", SQL_NTS,
			(SQLCHAR*)"%1", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (tmp, glbl_pk_uc, %1)",
		"Resultset with 8 columns\n"
		"Resultset with 3 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	glbl_pk_uc	id1	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	glbl_pk_uc	name1	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	glbl_pk_uc	name1	_SYSTEM	monetdb	UPDATE	NO\n");

	// odbctst.nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, nopk_twoucs)",
		"Resultset with 6 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (odbctst, nopk_twoucs)",
		"Resultset with 8 columns\n"
		"Resultset with 1 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (odbctst, nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"NULL	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (odbctst, nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"NULL	odbctst	nopk_twoucs	0	NULL	nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n"
		"NULL	odbctst	nopk_twoucs	1	NULL	nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"NULL	odbctst	nopk_twoucs	1	NULL	nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (odbctst, nopk_twoucs)",
		"Resultset with 7 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"nopk_twoucs", SQL_NTS,
			(SQLCHAR*)"%2", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (odbctst, nopk_twoucs, %2)",
		"Resultset with 8 columns\n"
		"Resultset with 3 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	odbctst	nopk_twoucs	id2	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	odbctst	nopk_twoucs	name2	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	odbctst	nopk_twoucs	name2	_SYSTEM	monetdb	UPDATE	NO\n");

	// tmp.tmp_nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, tmp_nopk_twoucs)",
		"Resultset with 6 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, tmp_nopk_twoucs)",
		"Resultset with 8 columns\n"
		"Resultset with 1 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_id2_unique	2	1	id2	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_name2_unique	2	1	name2	NULL	NULL	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, tmp_nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_id2_unique	2	1	id2	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_nopk_twoucs	0	NULL	tmp_nopk_twoucs_name2_unique	2	1	name2	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_nopk_twoucs	1	NULL	tmp_nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	tmp_nopk_twoucs	1	NULL	tmp_nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (tmp, tmp_nopk_twoucs)",
		"Resultset with 7 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	tmp_nopk_twoucs	_SYSTEM	monetdb	DELETE	NO\n"
		"NULL	tmp	tmp_nopk_twoucs	_SYSTEM	monetdb	INSERT	NO\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"tmp_nopk_twoucs", SQL_NTS,
			(SQLCHAR*)"%2", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (tmp, tmp_nopk_twoucs, %2)",
		"Resultset with 8 columns\n"
		"Resultset with 3 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	tmp_nopk_twoucs	id2	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	tmp_nopk_twoucs	name2	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	tmp_nopk_twoucs	name2	_SYSTEM	monetdb	UPDATE	NO\n");

	// tmp.glbl_nopk_twoucs
	ret = SQLPrimaryKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (tmp, glbl_nopk_twoucs)",
		"Resultset with 6 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n");

	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (tmp, glbl_nopk_twoucs)",
		"Resultset with 8 columns\n"
		"Resultset with 1 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	id2	4	INTEGER	32	11	0	1\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_INDEX_UNIQUE, SQL_ENSURE);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_nopk_twoucs, SQL_INDEX_UNIQUE, SQL_ENSURE)",
		"Resultset with 13 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"NULL	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n");

	ret = SQLStatistics(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			SQL_INDEX_ALL, SQL_QUICK);
	compareResult(stmt, ret, "SQLStatistics (tmp, glbl_nopk_twoucs, SQL_INDEX_ALL, SQL_QUICK)",
		"Resultset with 13 columns\n"
		"Resultset with 4 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	NON_UNIQUE	INDEX_QUALIFIER	INDEX_NAME	TYPE	ORDINAL_POSITION	COLUMN_NAME	ASC_OR_DESC	CARDINALITY	PAGES	FILTER_CONDITION\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1)	WVARCHAR(1024)	SMALLINT	SMALLINT	WVARCHAR(1024)	WCHAR(1)	INTEGER	INTEGER	WVARCHAR(1)\n"
		"NULL	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_id2_unique	2	1	id2	NULL	0	NULL	NULL\n"
		"NULL	tmp	glbl_nopk_twoucs	0	NULL	glbl_nopk_twoucs_name2_unique	2	1	name2	NULL	0	NULL	NULL\n"
		"NULL	tmp	glbl_nopk_twoucs	1	NULL	glbl_nopk_twoucs_i	2	1	id2	NULL	NULL	NULL	NULL\n"
		"NULL	tmp	glbl_nopk_twoucs	1	NULL	glbl_nopk_twoucs_i	2	2	name2	NULL	NULL	NULL	NULL\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (tmp, glbl_nopk_twoucs)",
		"Resultset with 7 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	glbl_nopk_twoucs	_SYSTEM	monetdb	DELETE	NO\n"
		"NULL	tmp	glbl_nopk_twoucs	_SYSTEM	monetdb	INSERT	NO\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"tmp", SQL_NTS, (SQLCHAR*)"glbl_nopk_twoucs", SQL_NTS,
			(SQLCHAR*)"%2", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (tmp, glbl_nopk_twoucs, %2)",
		"Resultset with 8 columns\n"
		"Resultset with 3 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n"
		"NULL	tmp	glbl_nopk_twoucs	id2	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	glbl_nopk_twoucs	name2	_SYSTEM	monetdb	SELECT	NO\n"
		"NULL	tmp	glbl_nopk_twoucs	name2	_SYSTEM	monetdb	UPDATE	NO\n");

	// sys.storagemodelinput
	ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"storagemodelinput", SQL_NTS,
			SQL_SCOPE_SESSION, SQL_NO_NULLS);
	compareResult(stmt, ret, "SQLSpecialColumns (sys, storagemodelinput)",
		"Resultset with 8 columns\n"
		"Resultset with 10 rows\n"
		"SCOPE	COLUMN_NAME	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	PSEUDO_COLUMN\n"
		"SMALLINT	WVARCHAR(1024)	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT\n"
		"1	schema	-9	VARCHAR	1024	2048	NULL	1\n"
		"1	table	-9	VARCHAR	1024	2048	NULL	1\n"
		"1	column	-9	VARCHAR	1024	2048	NULL	1\n"
		"1	type	-9	VARCHAR	1024	2048	NULL	1\n"
		"1	typewidth	4	INTEGER	32	11	0	1\n"
		"1	count	-5	BIGINT	64	20	0	1\n"
		"1	distinct	-5	BIGINT	64	20	0	1\n"
		"1	atomwidth	4	INTEGER	32	11	0	1\n"
		"1	reference	-7	BOOLEAN	1	1	NULL	1\n"
		"1	isacolumn	-7	BOOLEAN	1	1	NULL	1\n");

	// odbctst.CUSTOMERS, odbctst.ORDERS and odbctst.LINES
	/* next tests are copied from code examples on https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlforeignkeys-function?view=sql-server-ver15 */
	ret = SQLPrimaryKeys(stmt, NULL, 0, NULL, 0, (SQLCHAR*)"ORDERS", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (NULL, ORDERS)",
		"Resultset with 6 columns\n"
		"Resultset with 1 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n"
		"NULL	odbctst	ORDERS	ORDERID	1	ORDERS_ORDERID_pkey\n");

	ret = SQLForeignKeys(stmt, NULL, 0, NULL, 0, (SQLCHAR*)"ORDERS", SQL_NTS, NULL, 0, NULL, 0, NULL, 0);
	compareResult(stmt, ret, "SQLForeignKeys (NULL, ORDERS, NULL, NULL)",
		"Resultset with 14 columns\n"
		"Resultset with 1 rows\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT\n"
		"NULL	odbctst	ORDERS	ORDERID	NULL	odbctst	LINES	ORDERID	1	1	1	LINES_ORDERID_fkey	ORDERS_ORDERID_pkey	7\n");

	ret = SQLForeignKeys(stmt, NULL, 0, NULL, 0, NULL, 0, NULL, 0, NULL, 0, (SQLCHAR*)"ORDERS", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (NULL, NULL, NULL, ORDERS)",
		"Resultset with 14 columns\n"
		"Resultset with 1 rows\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT\n"
		"NULL	odbctst	CUSTOMERS	CUSTID	NULL	odbctst	ORDERS	CUSTID	1	1	1	ORDERS_CUSTID_fkey	CUSTOMERS_CUSTID_pkey	7\n");

	/* odbctst.pk2c and odbctst.fk2c (tests multi-column pks and multiple multi-column fks from one table */
	ret = SQLPrimaryKeys(stmt, NULL, 0, (SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk2c", SQL_NTS);
	compareResult(stmt, ret, "SQLPrimaryKeys (odbctst, pk2c)",
		"Resultset with 6 columns\n"
		"Resultset with 2 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	KEY_SEQ	PK_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	WVARCHAR(1024)\n"
		"NULL	odbctst	pk2c	pkc2	1	pk2c_pkc2_pkc1_pkey\n"
		"NULL	odbctst	pk2c	pkc1	2	pk2c_pkc2_pkc1_pkey\n");

	ret = SQLForeignKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk2c", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (odbctst, pk2c, , )",
		"Resultset with 14 columns\n"
		"Resultset with 4 rows\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT\n"
		"NULL	odbctst	pk2c	pkc2	NULL	odbctst	fk2c	fkc2	1	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc1	NULL	odbctst	fk2c	fkc1	2	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc2	NULL	odbctst	fk2c	fkc2	1	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc1	NULL	odbctst	fk2c	fkc3	2	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n");

	ret = SQLForeignKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"fk2c", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (, , odbctst, fk2c)",
		"Resultset with 14 columns\n"
		"Resultset with 4 rows\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT\n"
		"NULL	odbctst	pk2c	pkc2	NULL	odbctst	fk2c	fkc2	1	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc1	NULL	odbctst	fk2c	fkc1	2	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc2	NULL	odbctst	fk2c	fkc2	1	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc1	NULL	odbctst	fk2c	fkc3	2	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n");

	ret = SQLForeignKeys(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk2c", SQL_NTS,
			(SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"fk2c", SQL_NTS);
	compareResult(stmt, ret, "SQLForeignKeys (odbctst, pk2c, odbctst, fk2c)",
		"Resultset with 14 columns\n"
		"Resultset with 4 rows\n"
		"PKTABLE_CAT	PKTABLE_SCHEM	PKTABLE_NAME	PKCOLUMN_NAME	FKTABLE_CAT	FKTABLE_SCHEM	FKTABLE_NAME	FKCOLUMN_NAME	KEY_SEQ	UPDATE_RULE	DELETE_RULE	FK_NAME	PK_NAME	DEFERRABILITY\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(1024)	WVARCHAR(1024)	SMALLINT\n"
		"NULL	odbctst	pk2c	pkc2	NULL	odbctst	fk2c	fkc2	1	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc1	NULL	odbctst	fk2c	fkc1	2	0	1	fk2c_fkc2_fkc1_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc2	NULL	odbctst	fk2c	fkc2	1	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n"
		"NULL	odbctst	pk2c	pkc1	NULL	odbctst	fk2c	fkc3	2	2	3	fk2c_fkc2_fkc3_fkey	pk2c_pkc2_pkc1_pkey	7\n");

	ret = SQLTablePrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_2c", SQL_NTS);
	compareResult(stmt, ret, "SQLTablePrivileges (odbctst, pk_2c)",
		"Resultset with 7 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n");

	ret = SQLColumnPrivileges(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"pk_2c", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLColumnPrivileges (odbctst, pk_2c, %)",
		"Resultset with 8 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	COLUMN_NAME	GRANTOR	GRANTEE	PRIVILEGE	IS_GRANTABLE\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(1024)	WCHAR(1024)	WCHAR(1024)	WVARCHAR(40)	WCHAR(3)\n");


	// test procedure sys.analyze(). There are 4 overloaded variants of this procedure in MonetDB with 0, 1, 2 or 3 input parameters.
	ret = SQLProcedures(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"analyze", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedures (sys, analyze)",
		"Resultset with 9 columns\n"
		"Resultset with 4 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	NUM_INPUT_PARAMS	NUM_OUTPUT_PARAMS	NUM_RESULT_SETS	REMARKS	PROCEDURE_TYPE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	TINYINT	TINYINT	TINYINT	WVARCHAR(65000)	SMALLINT	WVARCHAR(10)\n"
		"NULL	sys	analyze	0	0	0	sys.analyze() procedure comment	1	replacedId\n"
		"NULL	sys	analyze	0	0	0	NULL	1	replacedId\n"
		"NULL	sys	analyze	0	0	0	NULL	1	replacedId\n"
		"NULL	sys	analyze	0	0	0	NULL	1	replacedId\n");

	ret = SQLProcedureColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"analyze", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedureColumns (sys, analyze, %)",
		"Resultset with 20 columns\n"
		"Resultset with 6 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	COLUMN_NAME	COLUMN_TYPE	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	WVARCHAR(256)	SMALLINT	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(65000)	WVARCHAR(1)	SMALLINT	SMALLINT	INTEGER	INTEGER	WCHAR	WVARCHAR(10)\n"
		"NULL	sys	analyze	sname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	1		replacedId\n"
		"NULL	sys	analyze	sname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	1		replacedId\n"
		"NULL	sys	analyze	tname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	analyze	sname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	1		replacedId\n"
		"NULL	sys	analyze	tname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	analyze	cname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	3		replacedId\n");

	// test function sys.sin(). There are 2 overloaded variants of this function in MonetDB: sys.sin(real) and sys.sin(double).
	ret = SQLProcedures(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"sin", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedures (sys, sin)",
		"Resultset with 9 columns\n"
		"Resultset with 2 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	NUM_INPUT_PARAMS	NUM_OUTPUT_PARAMS	NUM_RESULT_SETS	REMARKS	PROCEDURE_TYPE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	TINYINT	TINYINT	TINYINT	WVARCHAR(65000)	SMALLINT	WVARCHAR(10)\n"
		"NULL	sys	sin	0	0	0	NULL	2	replacedId\n"
		"NULL	sys	sin	0	0	0	sys.sin(double) function comment	2	replacedId\n");

	ret = SQLProcedureColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"sin", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedureColumns (sys, sin, %)",
		"Resultset with 20 columns\n"
		"Resultset with 4 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	COLUMN_NAME	COLUMN_TYPE	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	WVARCHAR(256)	SMALLINT	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(65000)	WVARCHAR(1)	SMALLINT	SMALLINT	INTEGER	INTEGER	WCHAR	WVARCHAR(10)\n"
		"NULL	sys	sin	arg_1	1	7	REAL	24	14	7	2	2	NULL	NULL	7	NULL	NULL	1		replacedId\n"
		"NULL	sys	sin	res_0	5	7	REAL	24	14	7	2	2	NULL	NULL	7	NULL	NULL	0		replacedId\n"
		"NULL	sys	sin	arg_1	1	8	DOUBLE	53	24	15	2	2	NULL	NULL	8	NULL	NULL	1		replacedId\n"
		"NULL	sys	sin	res_0	5	8	DOUBLE	53	24	15	2	2	NULL	NULL	8	NULL	NULL	0		replacedId\n");

	// test table returning function sys.env(). It has no input parameters. Only 2 result columns.
	ret = SQLProcedures(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"env", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedures (sys, env)",
		"Resultset with 9 columns\n"
		"Resultset with 1 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	NUM_INPUT_PARAMS	NUM_OUTPUT_PARAMS	NUM_RESULT_SETS	REMARKS	PROCEDURE_TYPE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	TINYINT	TINYINT	TINYINT	WVARCHAR(65000)	SMALLINT	WVARCHAR(10)\n"
		"NULL	sys	env	0	0	0	sys.env() function comment	2	replacedId\n");

	ret = SQLProcedureColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"env", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedureColumns (sys, env, %)",
		"Resultset with 20 columns\n"
		"Resultset with 2 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	COLUMN_NAME	COLUMN_TYPE	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	WVARCHAR(256)	SMALLINT	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(65000)	WVARCHAR(1)	SMALLINT	SMALLINT	INTEGER	INTEGER	WCHAR	WVARCHAR(10)\n"
		"NULL	sys	env	name	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	1		replacedId\n"
		"NULL	sys	env	value	3	-9	VARCHAR	2048	4096	NULL	NULL	2	NULL	NULL	-9	NULL	4096	2		replacedId\n");

	// test table returning function sys.statistics(). 4 overloaded variants with 0, 1, 2 or 3 input parameters. 13 result columns.
	ret = SQLProcedures(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"statistics", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedures (sys, statistics)",
		"Resultset with 9 columns\n"
		"Resultset with 4 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	NUM_INPUT_PARAMS	NUM_OUTPUT_PARAMS	NUM_RESULT_SETS	REMARKS	PROCEDURE_TYPE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	TINYINT	TINYINT	TINYINT	WVARCHAR(65000)	SMALLINT	WVARCHAR(10)\n"
		"NULL	sys	statistics	0	0	0	sys.statistics() function comment	2	replacedId\n"
		"NULL	sys	statistics	0	0	0	NULL	2	replacedId\n"
		"NULL	sys	statistics	0	0	0	NULL	2	replacedId\n"
		"NULL	sys	statistics	0	0	0	NULL	2	replacedId\n");

	ret = SQLProcedureColumns(stmt, (SQLCHAR*)"", SQL_NTS,
			(SQLCHAR*)"sys", SQL_NTS, (SQLCHAR*)"statistics", SQL_NTS,
			(SQLCHAR*)"%", SQL_NTS);
	compareResult(stmt, ret, "SQLProcedureColumns (sys, statistics, %)",
		"Resultset with 20 columns\n"
		"Resultset with 58 rows\n"
		"PROCEDURE_CAT	PROCEDURE_SCHEM	PROCEDURE_NAME	COLUMN_NAME	COLUMN_TYPE	DATA_TYPE	TYPE_NAME	COLUMN_SIZE	BUFFER_LENGTH	DECIMAL_DIGITS	NUM_PREC_RADIX	NULLABLE	REMARKS	COLUMN_DEF	SQL_DATA_TYPE	SQL_DATETIME_SUB	CHAR_OCTET_LENGTH	ORDINAL_POSITION	IS_NULLABLE	SPECIFIC_NAME\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(256)	WVARCHAR(256)	SMALLINT	SMALLINT	WCHAR(25)	INTEGER	INTEGER	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(65000)	WVARCHAR(1)	SMALLINT	SMALLINT	INTEGER	INTEGER	WCHAR	WVARCHAR(10)\n"
		// 0 input argument and 13 result columns of sys.statistics()
		"NULL	sys	statistics	column_id	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	1		replacedId\n"
		"NULL	sys	statistics	schema	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	statistics	table	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	3		replacedId\n"
		"NULL	sys	statistics	column	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	4		replacedId\n"
		"NULL	sys	statistics	type	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	5		replacedId\n"
		"NULL	sys	statistics	width	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	6		replacedId\n"
		"NULL	sys	statistics	count	3	-5	BIGINT	64	20	0	2	2	NULL	NULL	-5	NULL	NULL	7		replacedId\n"
		"NULL	sys	statistics	unique	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	8		replacedId\n"
		"NULL	sys	statistics	nils	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	9		replacedId\n"
		"NULL	sys	statistics	minval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	10		replacedId\n"
		"NULL	sys	statistics	maxval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	11		replacedId\n"
		"NULL	sys	statistics	sorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	12		replacedId\n"
		"NULL	sys	statistics	revsorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	13		replacedId\n"

		// 1 input argument and 13 result columns of sys.statistics(sname)
		"NULL	sys	statistics	sname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	1		replacedId\n"
		"NULL	sys	statistics	column_id	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	1		replacedId\n"
		"NULL	sys	statistics	schema	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	statistics	table	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	3		replacedId\n"
		"NULL	sys	statistics	column	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	4		replacedId\n"
		"NULL	sys	statistics	type	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	5		replacedId\n"
		"NULL	sys	statistics	width	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	6		replacedId\n"
		"NULL	sys	statistics	count	3	-5	BIGINT	64	20	0	2	2	NULL	NULL	-5	NULL	NULL	7		replacedId\n"
		"NULL	sys	statistics	unique	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	8		replacedId\n"
		"NULL	sys	statistics	nils	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	9		replacedId\n"
		"NULL	sys	statistics	minval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	10		replacedId\n"
		"NULL	sys	statistics	maxval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	11		replacedId\n"
		"NULL	sys	statistics	sorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	12		replacedId\n"
		"NULL	sys	statistics	revsorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	13		replacedId\n"

		// 2 input arguments and 13 result columns of sys.statistics(sname, tname)
		"NULL	sys	statistics	sname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	1		replacedId\n"
		"NULL	sys	statistics	tname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	statistics	column_id	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	1		replacedId\n"
		"NULL	sys	statistics	schema	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	statistics	table	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	3		replacedId\n"
		"NULL	sys	statistics	column	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	4		replacedId\n"
		"NULL	sys	statistics	type	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	5		replacedId\n"
		"NULL	sys	statistics	width	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	6		replacedId\n"
		"NULL	sys	statistics	count	3	-5	BIGINT	64	20	0	2	2	NULL	NULL	-5	NULL	NULL	7		replacedId\n"
		"NULL	sys	statistics	unique	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	8		replacedId\n"
		"NULL	sys	statistics	nils	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	9		replacedId\n"
		"NULL	sys	statistics	minval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	10		replacedId\n"
		"NULL	sys	statistics	maxval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	11		replacedId\n"
		"NULL	sys	statistics	sorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	12		replacedId\n"
		"NULL	sys	statistics	revsorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	13		replacedId\n"

		// 3 input arguments and 13 result columns of sys.statistics(sname, tname, cname)
		"NULL	sys	statistics	sname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	1		replacedId\n"
		"NULL	sys	statistics	tname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	statistics	cname	1	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	3		replacedId\n"
		"NULL	sys	statistics	column_id	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	1		replacedId\n"
		"NULL	sys	statistics	schema	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	2		replacedId\n"
		"NULL	sys	statistics	table	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	3		replacedId\n"
		"NULL	sys	statistics	column	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	4		replacedId\n"
		"NULL	sys	statistics	type	3	-9	VARCHAR	1024	2048	NULL	NULL	2	NULL	NULL	-9	NULL	2048	5		replacedId\n"
		"NULL	sys	statistics	width	3	4	INTEGER	32	11	0	2	2	NULL	NULL	4	NULL	NULL	6		replacedId\n"
		"NULL	sys	statistics	count	3	-5	BIGINT	64	20	0	2	2	NULL	NULL	-5	NULL	NULL	7		replacedId\n"
		"NULL	sys	statistics	unique	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	8		replacedId\n"
		"NULL	sys	statistics	nils	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	9		replacedId\n"
		"NULL	sys	statistics	minval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	10		replacedId\n"
		"NULL	sys	statistics	maxval	3	-10	CHARACTER LARGE OBJECT	0	0	NULL	NULL	2	NULL	NULL	-10	NULL	0	11		replacedId\n"
		"NULL	sys	statistics	sorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	12		replacedId\n"
		"NULL	sys	statistics	revsorted	3	-7	BOOLEAN	1	1	NULL	NULL	2	NULL	NULL	-7	NULL	NULL	13		replacedId\n");

	ret = SQLGetTypeInfo(stmt, SQL_ALL_TYPES);
	compareResult(stmt, ret, "SQLGetTypeInfo(stmt, SQL_ALL_TYPES)",
		"Resultset with 19 columns\n"
		"Resultset with 44 rows\n"
		"TYPE_NAME	DATA_TYPE	COLUMN_SIZE	LITERAL_PREFIX	LITERAL_SUFFIX	CREATE_PARAMS	NULLABLE	CASE_SENSITIVE	SEARCHABLE	UNSIGNED_ATTRIBUTE	FIXED_PREC_SCALE	AUTO_UNIQUE_VALUE	LOCAL_TYPE_NAME	MINIMUM_SCALE	MAXIMUM_SCALE	SQL_DATA_TYPE	SQL_DATETIME_SUB	NUM_PREC_RADIX	INTERVAL_PRECISION\n"
		"WCHAR(128)	SMALLINT	INTEGER	WCHAR(11)	WCHAR(1)	WCHAR(15)	SMALLINT	SMALLINT	SMALLINT	SMALLINT	SMALLINT	SMALLINT	WCHAR(16)	SMALLINT	SMALLINT	SMALLINT	SMALLINT	INTEGER	SMALLINT\n"
		"uuid	-11	36	uuid '	'	NULL	1	0	2	-1	0	-1	uuid	-1	-1	-11	-1	-1	-1\n"
		"character large object	-10	1000000	'	'	NULL	1	1	3	-1	0	0	NULL	-1	-1	-10	-1	-1	-1\n"
		"json	-10	1000000	json '	'	NULL	1	1	3	-1	0	0	json	-1	-1	-10	-1	-1	-1\n"
		"url	-10	1000000	url '	'	NULL	1	1	3	-1	0	0	url	-1	-1	-10	-1	-1	-1\n"
		"varchar	-9	1000000	'	'	length	1	1	3	-1	0	-1	NULL	-1	-1	-9	-1	-1	-1\n"
		"character	-8	1000000	'	'	length	1	1	3	-1	0	0	NULL	-1	-1	-8	-1	-1	-1\n"
		"char	-8	1000000	'	'	length	1	1	3	-1	0	0	NULL	-1	-1	-8	-1	-1	-1\n"
		"boolean	-7	1	NULL	NULL	NULL	1	0	2	1	1	0	boolean	-1	-1	-7	-1	-1	-1\n"
		"tinyint	-6	3	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	-6	-1	10	-1\n"
		"bigint	-5	19	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	-5	-1	10	-1\n"
		"bigserial	-5	19	NULL	NULL	NULL	0	0	2	0	0	1	bigserial	0	0	-5	-1	10	-1\n"
		"binary large object	-4	1000000	x'	'	NULL	1	1	3	-1	0	0	NULL	-1	-1	-4	-1	-1	-1\n"
		"binary large object	-3	1000000	x'	'	length	1	1	3	-1	0	0	blob(max_length)	-1	-1	-3	-1	-1	-1\n"
		"character large object	-1	1000000	'	'	NULL	1	1	3	-1	0	0	NULL	-1	-1	-1	-1	-1	-1\n"
		"char	1	1000000	'	'	length	1	1	3	-1	0	0	NULL	-1	-1	1	-1	-1	-1\n"
		"numeric	2	19	NULL	NULL	precision,scale	1	0	2	0	0	0	NULL	0	18	2	-1	10	-1\n"
		"decimal	3	19	NULL	NULL	precision,scale	1	0	2	0	0	0	NULL	0	18	3	-1	10	-1\n"
		"integer	4	10	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	4	-1	10	-1\n"
		"int	4	10	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	4	-1	10	-1\n"
		"mediumint	4	10	NULL	NULL	NULL	1	0	2	0	0	0	int	0	0	4	-1	10	-1\n"
		"serial	4	10	NULL	NULL	NULL	0	0	2	0	0	1	serial	0	0	4	-1	10	-1\n"
		"smallint	5	5	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	5	-1	10	-1\n"
		"float	6	53	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	6	-1	2	-1\n"
		"real	7	24	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	7	-1	2	-1\n"
		"double	8	53	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	8	-1	2	-1\n"
		"varchar	12	1000000	'	'	length	1	1	3	-1	0	-1	NULL	-1	-1	12	-1	-1	-1\n"
		"date	91	10	date '	'	NULL	1	0	2	-1	0	-1	NULL	-1	-1	9	1	-1	-1\n"
		"time	92	8	time '	'	NULL	1	0	2	-1	0	-1	NULL	0	0	9	2	-1	-1\n"
		"time(precision)	92	15	time '	'	precision	1	0	2	-1	0	-1	NULL	0	6	9	2	-1	-1\n"
		"timestamp	93	19	timestamp '	'	NULL	1	0	2	-1	0	-1	NULL	0	0	9	3	-1	-1\n"
		"timestamp(precision)	93	26	timestamp '	'	precision	1	0	2	-1	0	-1	NULL	0	6	9	3	-1	-1\n"
		"interval year	101	9	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	1	-1	9\n"
		"interval month	102	10	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	2	-1	10\n"
		"interval day	103	5	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	3	-1	5\n"
		"interval hour	104	6	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	4	-1	6\n"
		"interval minute	105	8	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	5	-1	8\n"
		"interval second	106	10	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	6	-1	10\n"
		"interval year to month	107	12	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	7	-1	9\n"
		"interval day to hour	108	8	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	8	-1	5\n"
		"interval day to minute	109	11	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	9	-1	5\n"
		"interval day to second	110	14	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	10	-1	5\n"
		"interval hour to minute	111	9	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	11	-1	6\n"
		"interval hour to second	112	12	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	12	-1	6\n"
		"interval minute to second	113	13	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	13	-1	10\n");

	/* MonetDB specific type "hugeint" is currently not returned by SQLGetTypeInfo(SQL_ALL_TYPES). However it can be queried when requested explicitly. Test it. */
	ret = SQLGetTypeInfo(stmt, SQL_HUGEINT);
	compareResult(stmt, ret, "SQLGetTypeInfo(stmt, SQL_HUGEINT)",
		"Resultset with 19 columns\n"
		"Resultset with 1 rows\n"
		"TYPE_NAME	DATA_TYPE	COLUMN_SIZE	LITERAL_PREFIX	LITERAL_SUFFIX	CREATE_PARAMS	NULLABLE	CASE_SENSITIVE	SEARCHABLE	UNSIGNED_ATTRIBUTE	FIXED_PREC_SCALE	AUTO_UNIQUE_VALUE	LOCAL_TYPE_NAME	MINIMUM_SCALE	MAXIMUM_SCALE	SQL_DATA_TYPE	SQL_DATETIME_SUB	NUM_PREC_RADIX	INTERVAL_PRECISION\n"
		"WVARCHAR(128)	SMALLINT	INTEGER	WVARCHAR(128)	WVARCHAR(128)	WVARCHAR(128)	SMALLINT	SMALLINT	SMALLINT	SMALLINT	SMALLINT	SMALLINT	WVARCHAR(128)	SMALLINT	SMALLINT	SMALLINT	SMALLINT	INTEGER	SMALLINT\n"
		"hugeint	16384	38	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	16384	-1	10	-1\n");

	/* strangely when we repeat SQLGetTypeInfo(stmt, SQL_ALL_TYPES) now after calling SQLGetTypeInfo(stmt, SQL_HUGEINT), it suddenly does include hugeint in the result! */
	ret = SQLGetTypeInfo(stmt, SQL_ALL_TYPES);
	compareResult(stmt, ret, "SQLGetTypeInfo(stmt, SQL_ALL_TYPES)",
		"Resultset with 19 columns\n"
		"Resultset with 45 rows\n"
		"TYPE_NAME	DATA_TYPE	COLUMN_SIZE	LITERAL_PREFIX	LITERAL_SUFFIX	CREATE_PARAMS	NULLABLE	CASE_SENSITIVE	SEARCHABLE	UNSIGNED_ATTRIBUTE	FIXED_PREC_SCALE	AUTO_UNIQUE_VALUE	LOCAL_TYPE_NAME	MINIMUM_SCALE	MAXIMUM_SCALE	SQL_DATA_TYPE	SQL_DATETIME_SUB	NUM_PREC_RADIX	INTERVAL_PRECISION\n"
		"WCHAR(128)	SMALLINT	INTEGER	WCHAR(11)	WCHAR(1)	WCHAR(15)	SMALLINT	SMALLINT	SMALLINT	SMALLINT	SMALLINT	SMALLINT	WCHAR(16)	SMALLINT	SMALLINT	SMALLINT	SMALLINT	INTEGER	SMALLINT\n"
		"uuid	-11	36	uuid '	'	NULL	1	0	2	-1	0	-1	uuid	-1	-1	-11	-1	-1	-1\n"
		"character large object	-10	1000000	'	'	NULL	1	1	3	-1	0	0	NULL	-1	-1	-10	-1	-1	-1\n"
		"json	-10	1000000	json '	'	NULL	1	1	3	-1	0	0	json	-1	-1	-10	-1	-1	-1\n"
		"url	-10	1000000	url '	'	NULL	1	1	3	-1	0	0	url	-1	-1	-10	-1	-1	-1\n"
		"varchar	-9	1000000	'	'	length	1	1	3	-1	0	-1	NULL	-1	-1	-9	-1	-1	-1\n"
		"character	-8	1000000	'	'	length	1	1	3	-1	0	0	NULL	-1	-1	-8	-1	-1	-1\n"
		"char	-8	1000000	'	'	length	1	1	3	-1	0	0	NULL	-1	-1	-8	-1	-1	-1\n"
		"boolean	-7	1	NULL	NULL	NULL	1	0	2	1	1	0	boolean	-1	-1	-7	-1	-1	-1\n"
		"tinyint	-6	3	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	-6	-1	10	-1\n"
		"bigint	-5	19	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	-5	-1	10	-1\n"
		"bigserial	-5	19	NULL	NULL	NULL	0	0	2	0	0	1	bigserial	0	0	-5	-1	10	-1\n"
		"binary large object	-4	1000000	x'	'	NULL	1	1	3	-1	0	0	NULL	-1	-1	-4	-1	-1	-1\n"
		"binary large object	-3	1000000	x'	'	length	1	1	3	-1	0	0	blob(max_length)	-1	-1	-3	-1	-1	-1\n"
		"character large object	-1	1000000	'	'	NULL	1	1	3	-1	0	0	NULL	-1	-1	-1	-1	-1	-1\n"
		"char	1	1000000	'	'	length	1	1	3	-1	0	0	NULL	-1	-1	1	-1	-1	-1\n"
		"numeric	2	19	NULL	NULL	precision,scale	1	0	2	0	0	0	NULL	0	18	2	-1	10	-1\n"
		"decimal	3	19	NULL	NULL	precision,scale	1	0	2	0	0	0	NULL	0	18	3	-1	10	-1\n"
		"integer	4	10	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	4	-1	10	-1\n"
		"int	4	10	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	4	-1	10	-1\n"
		"mediumint	4	10	NULL	NULL	NULL	1	0	2	0	0	0	int	0	0	4	-1	10	-1\n"
		"serial	4	10	NULL	NULL	NULL	0	0	2	0	0	1	serial	0	0	4	-1	10	-1\n"
		"smallint	5	5	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	5	-1	10	-1\n"
		"float	6	53	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	6	-1	2	-1\n"
		"real	7	24	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	7	-1	2	-1\n"
		"double	8	53	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	8	-1	2	-1\n"
		"varchar	12	1000000	'	'	length	1	1	3	-1	0	-1	NULL	-1	-1	12	-1	-1	-1\n"
		"date	91	10	date '	'	NULL	1	0	2	-1	0	-1	NULL	-1	-1	9	1	-1	-1\n"
		"time	92	8	time '	'	NULL	1	0	2	-1	0	-1	NULL	0	0	9	2	-1	-1\n"
		"time(precision)	92	15	time '	'	precision	1	0	2	-1	0	-1	NULL	0	6	9	2	-1	-1\n"
		"timestamp	93	19	timestamp '	'	NULL	1	0	2	-1	0	-1	NULL	0	0	9	3	-1	-1\n"
		"timestamp(precision)	93	26	timestamp '	'	precision	1	0	2	-1	0	-1	NULL	0	6	9	3	-1	-1\n"
		"interval year	101	9	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	1	-1	9\n"
		"interval month	102	10	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	2	-1	10\n"
		"interval day	103	5	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	3	-1	5\n"
		"interval hour	104	6	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	4	-1	6\n"
		"interval minute	105	8	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	5	-1	8\n"
		"interval second	106	10	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	6	-1	10\n"
		"interval year to month	107	12	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	7	-1	9\n"
		"interval day to hour	108	8	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	8	-1	5\n"
		"interval day to minute	109	11	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	9	-1	5\n"
		"interval day to second	110	14	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	10	-1	5\n"
		"interval hour to minute	111	9	'	'	NULL	1	0	2	-1	0	-1	NULL	0	0	10	11	-1	6\n"
		"interval hour to second	112	12	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	12	-1	6\n"
		"interval minute to second	113	13	'	'	precision	1	0	2	-1	0	-1	NULL	0	0	10	13	-1	10\n"
		"hugeint	16384	38	NULL	NULL	NULL	1	0	2	0	0	0	NULL	0	0	16384	-1	10	-1\n");


	nrServerThreads = getNrOfServerThreads(dbc);

	// test SELECT query
	ret = SQLExecDirect(stmt, (SQLCHAR *) "SELECT * from odbctst.\"LINES\";", SQL_NTS);
	compareResult(stmt, ret, "SELECT * from odbctst.\"LINES\"",
		"Resultset with 4 columns\n"
		"Resultset with 0 rows\n"
		"ORDERID	LINES	PARTID	QUANTITY\n"
		"INTEGER	INTEGER	INTEGER	DECIMAL(9,3)\n");

	// test PLAN SELECT query
	ret = SQLExecDirect(stmt, (SQLCHAR *) "PLAN SELECT * from odbctst.\"LINES\";", SQL_NTS);
	compareResult(stmt, ret, "PLAN SELECT * from odbctst.\"LINES\"",
		"Resultset with 1 columns\n"
		"Resultset with 3 rows\n"
		"rel\n"
		"WLONGVARCHAR(176)\n"
		"project (\n"
		"| table(\"odbctst\".\"LINES\") [ \"LINES\".\"ORDERID\" NOT NULL UNIQUE HASHCOL , \"LINES\".\"LINES\" NOT NULL UNIQUE, \"LINES\".\"PARTID\" NOT NULL UNIQUE, \"LINES\".\"QUANTITY\" NOT NULL UNIQUE ]\n"
		") [ \"LINES\".\"ORDERID\" NOT NULL UNIQUE HASHCOL , \"LINES\".\"LINES\" NOT NULL UNIQUE, \"LINES\".\"PARTID\" NOT NULL UNIQUE, \"LINES\".\"QUANTITY\" NOT NULL UNIQUE ]\n");

	// test EXPLAIN SELECT query
	ret = SQLExecDirect(stmt, (SQLCHAR *) "EXPLAIN SELECT * from odbctst.\"LINES\";", SQL_NTS);
	compareResult(stmt, ret, "EXPLAIN SELECT * from odbctst.\"LINES\"",
	    nrServerThreads > 1 ?
		"Resultset with 1 columns\n"
		"Resultset with 48 rows\n"
		"mal\n"
		"WLONGVARCHAR(174)\n"
		"function user.main():void;\n"
		"    X_1:void := querylog.define(\"explain select * from odbctst.\\\"LINES\\\";\":str, \"default_pipe\":str, 26:int);\n"
		"\n"
		"    X_33:bat[:int] := bat.new(nil:int);\n"
		"    X_34:bat[:int] := bat.new(nil:int);\n"
		"    X_35:bat[:int] := bat.new(nil:int);\n"
		"    X_36:bat[:int] := bat.new(nil:int);\n"
		"    X_38:bat[:str] := bat.pack(\"odbctst.LINES\":str, \"odbctst.LINES\":str, \"odbctst.LINES\":str, \"odbctst.LINES\":str);\n"
		"    X_39:bat[:str] := bat.pack(\"ORDERID\":str, \"LINES\":str, \"PARTID\":str, \"QUANTITY\":str);\n"
		"    X_40:bat[:str] := bat.pack(\"int\":str, \"int\":str, \"int\":str, \"decimal\":str);\n"
		"    X_41:bat[:int] := bat.pack(32:int, 32:int, 32:int, 9:int);\n"
		"    X_42:bat[:int] := bat.pack(0:int, 0:int, 0:int, 3:int);\n"
		"\n"
		"    X_37:int := sql.resultSet(X_38:bat[:str], X_39:bat[:str], X_40:bat[:str], X_41:bat[:int], X_42:bat[:int], X_33:bat[:int], X_34:bat[:int], X_35:bat[:int], X_36:bat[:int]);\n"
		"end user.main;\n"
		"\n\n\n\n\n\n\n\n\n\n"
		"\n\n\n\n\n\n\n\n\n\n"
		"\n\n\n\n\n\n\n\n\n\n"
		"\n\n\n"
	    :
		"Resultset with 1 columns\n"
		"Resultset with 46 rows\n"
		"mal\n"
		"WLONGVARCHAR(174)\n"
		"function user.main():void;\n"
		"    X_1:void := querylog.define(\"explain select * from odbctst.\\\"LINES\\\";\":str, \"default_pipe\":str, 26:int);\n"
		"    X_33:bat[:int] := bat.new(nil:int);\n"
		"    X_34:bat[:int] := bat.new(nil:int);\n"
		"    X_35:bat[:int] := bat.new(nil:int);\n"
		"    X_36:bat[:int] := bat.new(nil:int);\n"
		"    X_38:bat[:str] := bat.pack(\"odbctst.LINES\":str, \"odbctst.LINES\":str, \"odbctst.LINES\":str, \"odbctst.LINES\":str);\n"
		"    X_39:bat[:str] := bat.pack(\"ORDERID\":str, \"LINES\":str, \"PARTID\":str, \"QUANTITY\":str);\n"
		"    X_40:bat[:str] := bat.pack(\"int\":str, \"int\":str, \"int\":str, \"decimal\":str);\n"
		"    X_41:bat[:int] := bat.pack(32:int, 32:int, 32:int, 9:int);\n"
		"    X_42:bat[:int] := bat.pack(0:int, 0:int, 0:int, 3:int);\n"
		"    X_37:int := sql.resultSet(X_38:bat[:str], X_39:bat[:str], X_40:bat[:str], X_41:bat[:int], X_42:bat[:int], X_33:bat[:int], X_34:bat[:int], X_35:bat[:int], X_36:bat[:int]);\n"
		"end user.main;\n"
		"\n\n\n\n\n\n\n\n\n\n"
		"\n\n\n\n\n\n\n\n\n\n"
		"\n\n\n\n\n\n\n\n\n\n"
		"\n\n\n");

	// test TRACE SELECT query.
	// This will return two resultsets: first with the query results and next with the trace results
	// We use (and thus test) SQLMoreResults() to get the next/second result.
	ret = SQLExecDirect(stmt, (SQLCHAR *) "TRACE SELECT * from odbctst.\"LINES\";", SQL_NTS);
	compareResultNoClose(stmt, ret, "TRACE(1) SELECT * from odbctst.\"LINES\"",
		"Resultset with 4 columns\n"
		"Resultset with 0 rows\n"
		"ORDERID	LINES	PARTID	QUANTITY\n"
		"INTEGER	INTEGER	INTEGER	DECIMAL(9,3)\n");
	ret = SQLMoreResults(stmt);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLMoreResults()");
	if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
		compareResult(stmt, ret, "TRACE(2) SELECT * from odbctst.\"LINES\"",
		    nrServerThreads > 1 ?
			"Resultset with 2 columns\n"
			"Resultset with 12 rows\n"
			"usec	statement\n"
			"BIGINT	WLONGVARCHAR(213)\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
		    :
			"Resultset with 2 columns\n"
			"Resultset with 11 rows\n"
			"usec	statement\n"
			"BIGINT	WLONGVARCHAR(213)\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n"
			"4	variable output\n");
		/* next is the original output but it is varying a lot on usec values, X_## values and even the order of rows,
		   so all data is replaced (see above) for stable output comparison.
			"1	    X_1=0@0:void := querylog.define(\"trace select * from odbctst.\\\"LINES\\\";\":str, \"default_pipe\":str, 26:int);\n"
			"9	    X_33=[0]:bat[:int] := bat.new(nil:int);\n"
			"8	    X_34=[0]:bat[:int] := bat.new(nil:int);\n"
			"7	    X_36=[0]:bat[:int] := bat.new(nil:int);\n"
			"6	    X_35=[0]:bat[:int] := bat.new(nil:int);\n"
			"8	    X_41=[4]:bat[:int] := bat.pack(32:int, 32:int, 32:int, 9:int);\n"
			"13	    X_42=[4]:bat[:int] := bat.pack(0:int, 0:int, 0:int, 3:int);\n"
			"14	    X_38=[4]:bat[:str] := bat.pack(\"odbctst.LINES\":str, \"odbctst.LINES\":str, \"odbctst.LINES\":str, \"odbctst.LINES\":str);\n"
			"16	    X_40=[4]:bat[:str] := bat.pack(\"int\":str, \"int\":str, \"int\":str, \"decimal\":str);\n"
			"15	    X_39=[4]:bat[:str] := bat.pack(\"ORDERID\":str, \"LINES\":str, \"PARTID\":str, \"QUANTITY\":str);\n"
			"316	barrier X_106=false:bit := language.dataflow();\n"
			"22	    X_37=76:int := sql.resultSet(X_38=[4]:bat[:str], X_39=[4]:bat[:str], X_40=[4]:bat[:str], X_41=[4]:bat[:int], X_42=[4]:bat[:int], X_33=[0]:bat[:int], X_34=[0]:bat[:int], X_35=[0]:bat[:int], X_36=[0]:bat[:int]);\n");
		*/
	}

	// test DEBUG SELECT query.
	// DEBUG statements are *not* supported in ODBC and should produce an Error
	ret = SQLExecDirect(stmt, (SQLCHAR *) "DEBUG SELECT * from odbctst.\"LINES\";", SQL_NTS);
	if (ret != SQL_ERROR) {
		/* Error: SQLstate 42000, Errnr 0, Message [MonetDB][ODBC Driver 11.45.0][MonetDB-Test]SQL debugging only supported in interactive mode in: "debug" */
		compareResult(stmt, ret, "DEBUG SELECT * from odbctst.\"LINES\"",
			"We do not expect DEBUG to be possible via ODBC API. Only supported in mclient.\n");
	}


	// cleanup
	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"DROP INDEX odbctst.pk_uc_i;\n"
		"DROP INDEX tmp.tmp_pk_uc_i;\n"
		"DROP INDEX tmp.glbl_pk_uc_i;\n"
		"DROP INDEX odbctst.nopk_twoucs_i;\n"
		"DROP INDEX tmp.tmp_nopk_twoucs_i;\n"
		"DROP INDEX tmp.glbl_nopk_twoucs_i;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (drop indices script)");

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
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (drop tables script)");

	// All tables in schema odbctst should now be gone, else we missed some DROP statements
	ret = SQLTables(stmt, NULL, 0,
			(SQLCHAR*)"odbctst", SQL_NTS, (SQLCHAR*)"%", SQL_NTS,
			NULL, 0);
	compareResult(stmt, ret, "SQLTables (odbctst, %, NULL)",
		"Resultset with 5 columns\n"
		"Resultset with 0 rows\n"
		"TABLE_CAT	TABLE_SCHEM	TABLE_NAME	TABLE_TYPE	REMARKS\n"
		"WVARCHAR(1)	WVARCHAR(1024)	WVARCHAR(1024)	WVARCHAR(25)	WVARCHAR(65000)\n");

	ret = SQLExecDirect(stmt, (SQLCHAR *)
		"SET SCHEMA sys;\n"
		"DROP SCHEMA odbctst;\n"
		, SQL_NTS);
	check(ret, SQL_HANDLE_STMT, stmt, "SQLExecDirect (drop schema script)");

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
