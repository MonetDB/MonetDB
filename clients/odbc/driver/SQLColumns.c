/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 *
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

/**********************************************************************
 * SQLColumns()
 * CLI Compliance: X/Open
 *
 * Note: catalogs are not supported, we ignore any value set for szCatalogName
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

#define NCOLUMNS	18

static const char *columnnames[NCOLUMNS] = {
	"table_cat",
	"table_schem",
	"table_name",
	"column_name",
	"data_type",
	"type_name",
	"column_size",
	"buffer_length",
	"decimal_digits",
	"num_prec_radix",
	"nullable",
	"remarks",
	"column_def",
	"sql_data_type",
	"sql_datetime_sub",
	"char_octet_length",
	"ordinal_position",
	"is_nullable",
};

static const char *columntypes[NCOLUMNS] = {
	"varchar",
	"varchar",
	"varchar",
	"varchar",
	"smallint",
	"varchar",
	"int",
	"int",
	"smallint",
	"smallint",
	"smallint",
	"varchar",
	"varchar",
	"smallint",
	"smallint",
	"int",
	"int",
	"varchar",
};

static SQLRETURN
SQLColumns_(ODBCStmt *stmt,
	    SQLCHAR *CatalogName,
	    SQLSMALLINT NameLength1,
	    SQLCHAR *SchemaName,
	    SQLSMALLINT NameLength2,
	    SQLCHAR *TableName,
	    SQLSMALLINT NameLength3,
	    SQLCHAR *ColumnName,
	    SQLSMALLINT NameLength4)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ColumnName, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, (char *) CatalogName,
		(int) NameLength2, (char *) SchemaName,
		(int) NameLength3, (char *) TableName,
		(int) NameLength4, (char *) ColumnName);
#endif

	/* construct the query now */
	query = (char *) malloc(1200 + NameLength2 + NameLength3 + NameLength4);
	assert(query);
	query_end = query;

	/* SQLColumns returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name NOT NULL
	   VARCHAR      column_name NOT NULL
	   SMALLINT     data_type NOT NULL
	   VARCHAR      type_name NOT NULL
	   INTEGER      column_size
	   INTEGER      buffer_length
	   SMALLINT     decimal_digits
	   SMALLINT     num_prec_radix
	   SMALLINT     nullable NOT NULL
	   VARCHAR      remarks
	   VARCHAR      column_def
	   SMALLINT     sql_data_type NOT NULL
	   SMALLINT     sql_datetime_sub
	   INTEGER      char_octet_length
	   INTEGER      ordinal_position NOT NULL
	   VARCHAR      is_nullable
	 */

	sprintf(query_end,
		"select "
		"cast(NULL as varchar(1)) as table_cat, "
		"s.\"name\" as table_schem, "
		"t.\"name\" as table_name, "
		"c.\"name\" as column_name, "
		"cast(0 as smallint) as data_type, " /* filled in later */
		"c.\"type\" as type_name, "
		"cast(c.\"type_digits\" as integer) as column_size, "
		"cast(c.\"type_digits\" as integer) as buffer_length, "
		"cast(c.\"type_scale\" as smallint) as decimal_digits, "
		"cast(0 as smallint) as num_prec_radix, "
		"case c.\"null\" when true then cast(%d as smallint) "
		/* XXX should this be SQL_NULLABLE_UNKNOWN instead of
		 * SQL_NO_NULLS? */
		"when false then cast(%d as smallint) end as nullable, "
		"cast('' as varchar(1)) as remarks, "
		"c.\"default\" as column_def, "
		"cast(0 as smallint) as sql_data_type, " /* filled in later */
		"cast(0 as smallint) as sql_datetime_sub, " /* filled in later */
		"case c.\"type\" when 'varchar' then cast(c.\"type_digits\" as integer) else cast(NULL as integer) end as char_octet_length, "
		"cast(c.\"number\" + 1 as integer) as ordinal_position, "
		"case c.\"null\" when true then cast('yes' as varchar(3)) "
		/* should this be '' instead of 'no'? */
		"when false then cast('no' as varchar(3)) end as is_nullable "
		"from sys.\"schemas\" s, sys.\"tables\" t, sys.\"columns\" c "
		"where s.\"id\" = t.\"schema_id\" and t.\"id\" = c.\"table_id\"",
		SQL_NULLABLE, SQL_NO_NULLS);
	query_end += strlen(query_end);

	/* depending on the input parameter values we must add a
	   variable selection condition dynamically */

	/* Construct the selection condition query part */
	if (NameLength2 > 0) {
		/* filtering requested on schema name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped. Check it and
		 * maybe convert it. */
		sprintf(query_end, " and s.\"name\" %s '%.*s'",
			memchr(SchemaName, '%', NameLength2) || memchr(SchemaName, '_', NameLength2) ? "like" : "=",
			NameLength2, (char*)SchemaName);
		query_end += strlen(query_end);
	}

	if (NameLength3 > 0) {
		/* filtering requested on table name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check it and
		 * maybe convert it. */
		sprintf(query_end, " and t.\"name\" %s '%.*s'",
			memchr(TableName, '%', NameLength3) || memchr(TableName, '_', NameLength3) ? "like" : "=",
			NameLength3, (char*)TableName);
		query_end += strlen(query_end);
	}

	if (NameLength4 > 0) {
		/* filtering requested on column name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check it and
		 * maybe convert it. */
		sprintf(query_end, " and c.\"name\" %s '%.*s'",
			memchr(ColumnName, '%', NameLength4) || memchr(ColumnName, '_', NameLength4) ? "like" : "=",
			NameLength4, (char*)ColumnName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end,
	       " order by table_cat, table_schem, "
	       "table_name, ordinal_position");
	query_end += strlen(query_end);
	assert(query_end - query < 1200 + NameLength2 + NameLength3 + NameLength4);

	/* query the MonetDB data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	if (rc == SQL_SUCCESS) {
		const char ***tuples;
		int j;
		SQLULEN i, n;
		char *data;
		int concise_type, data_type, sql_data_type, sql_datetime_sub;
		int columnlengths[NCOLUMNS];

		n = stmt->rowcount;

		tuples = malloc(sizeof(*tuples) * (size_t) n);
		for (j = 0; j < NCOLUMNS; j++)
			columnlengths[j] = mapi_get_len(stmt->hdl, j);

		for (i = 0; i < n; i++) {
			mapi_fetch_row(stmt->hdl);

			tuples[i] = malloc(sizeof(**tuples) * NCOLUMNS);
			for (j = 0; j < NCOLUMNS; j++) {
				data = mapi_fetch_field(stmt->hdl, j);

				tuples[i][j] = data && j != 4 && j != 13 && j != 14 ? strdup(data) : NULL;
			}
			concise_type = ODBCConciseType(tuples[i][5]);
			free((void *) tuples[i][5]);
			switch (concise_type) {
			case SQL_INTERVAL_MONTH: {
				int q1 = atoi(tuples[i][6]);

				/* we assume a leading precision of 6 */
				free((void *) tuples[i][6]);
				tuples[i][6] = NULL;
				free((void *) tuples[i][7]);
				tuples[i][7] = NULL;
				free((void *) tuples[i][8]);
				tuples[i][8] = NULL;
				switch (q1) {
				case 1:
					concise_type = SQL_INTERVAL_YEAR;
					tuples[i][6] = strdup("26");
					tuples[i][7] = strdup("26");
					tuples[i][8] = strdup("0");
					break;
				case 2:
					concise_type = SQL_INTERVAL_YEAR_TO_MONTH;
					tuples[i][6] = strdup("38");
					tuples[i][7] = strdup("38");
					tuples[i][8] = strdup("0");
					break;
				case 3:
					concise_type = SQL_INTERVAL_MONTH;
					tuples[i][6] = strdup("27");
					tuples[i][7] = strdup("27");
					tuples[i][8] = strdup("0");
					break;
				default:
					assert(0);
				}
				break;
			}
			case SQL_INTERVAL_SECOND: {
				int q1 = atoi(tuples[i][6]);

				/* we assume a leading precision of 6
				   and a second precision of 0 */
				free((void *) tuples[i][6]);
				tuples[i][6] = NULL;
				free((void *) tuples[i][7]);
				tuples[i][7] = NULL;
				free((void *) tuples[i][8]);
				tuples[i][8] = NULL;
				switch (q1) {
				case 4:
					concise_type = SQL_INTERVAL_DAY;
					tuples[i][6] = strdup("25");
					tuples[i][7] = strdup("25");
					tuples[i][8] = strdup("0");
					break;
				case 5:
					concise_type = SQL_INTERVAL_DAY_TO_HOUR;
					tuples[i][6] = strdup("36");
					tuples[i][7] = strdup("36");
					tuples[i][8] = strdup("0");
					break;
				case 6:
					concise_type = SQL_INTERVAL_DAY_TO_MINUTE;
					tuples[i][6] = strdup("41");
					tuples[i][7] = strdup("41");
					tuples[i][8] = strdup("0");
					break;
				case 7:
					concise_type = SQL_INTERVAL_DAY_TO_SECOND;
					tuples[i][6] = strdup("47");
					tuples[i][7] = strdup("47");
					tuples[i][8] = strdup("0");
					break;
				case 8:
					concise_type = SQL_INTERVAL_HOUR;
					tuples[i][6] = strdup("26");
					tuples[i][7] = strdup("26");
					tuples[i][8] = strdup("0");
					break;
				case 9:
					concise_type = SQL_INTERVAL_HOUR_TO_MINUTE;
					tuples[i][6] = strdup("39");
					tuples[i][7] = strdup("39");
					tuples[i][8] = strdup("0");
					break;
				case 10:
					concise_type = SQL_INTERVAL_HOUR_TO_SECOND;
					tuples[i][6] = strdup("45");
					tuples[i][7] = strdup("45");
					tuples[i][8] = strdup("0");
					break;
				case 11:
					concise_type = SQL_INTERVAL_MINUTE;
					tuples[i][6] = strdup("28");
					tuples[i][7] = strdup("28");
					tuples[i][8] = strdup("0");
					break;
				case 12:
					concise_type = SQL_INTERVAL_MINUTE_TO_SECOND;
					tuples[i][6] = strdup("44");
					tuples[i][7] = strdup("44");
					tuples[i][8] = strdup("0");
					break;
				case 13:
					concise_type = SQL_INTERVAL_SECOND;
					tuples[i][6] = strdup("30");
					tuples[i][7] = strdup("30");
					tuples[i][8] = strdup("0");
					break;
				default:
					assert(0);
				}
				break;
			}
			case SQL_DOUBLE:
			case SQL_REAL:
				free((void *) tuples[i][9]);
				tuples[i][9] = strdup("2");
				break;
			case SQL_BIGINT:
				free((void *) tuples[i][6]);
				tuples[i][6] = strdup("19");
				free((void *) tuples[i][7]);
				tuples[i][7] = strdup("19");
				free((void *) tuples[i][9]);
				tuples[i][9] = strdup("10");
				break;
			case SQL_DECIMAL:
				free((void *) tuples[i][9]);
				tuples[i][9] = strdup("10");
				break;
			case SQL_INTEGER:
				free((void *) tuples[i][6]);
				tuples[i][6] = strdup("10");
				free((void *) tuples[i][7]);
				tuples[i][7] = strdup("10");
				free((void *) tuples[i][9]);
				tuples[i][9] = strdup("10");
				break;
			case SQL_SMALLINT:
				free((void *) tuples[i][6]);
				tuples[i][6] = strdup("5");
				free((void *) tuples[i][7]);
				tuples[i][7] = strdup("5");
				free((void *) tuples[i][9]);
				tuples[i][9] = strdup("10");
				break;
			}

			tuples[i][5] = ODBCGetTypeInfo(concise_type,
						       &data_type,
						       &sql_data_type,
						       &sql_datetime_sub);
			if (tuples[i][5] != NULL) {
				tuples[i][5] = strdup(tuples[i][5]);
				data = malloc(7);
				sprintf(data, "%d", data_type);
				tuples[i][4] = data;
				data = malloc(7);
				sprintf(data, "%d", sql_data_type);
				tuples[i][13] = data;
				data = malloc(7);
				sprintf(data, "%d", sql_datetime_sub);
				tuples[i][14] = data;
			}
		}

		ODBCResetStmt(stmt);

		mapi_virtual_result(stmt->hdl, NCOLUMNS, columnnames,
				    columntypes, columnlengths,
				    (int) n, tuples);

		for (i = 0; i < n; i++) {
			for (j = 0; j < NCOLUMNS; j++)
				if (tuples[i][j])
					free((void *) tuples[i][j]);
			free((void *) tuples[i]);
		}
		free((void *) tuples);
		return ODBCInitResult(stmt);
	}

	return rc;
}

SQLRETURN SQL_API
SQLColumns(SQLHSTMT StatementHandle,
	   SQLCHAR *CatalogName,
	   SQLSMALLINT NameLength1,
	   SQLCHAR *SchemaName,
	   SQLSMALLINT NameLength2,
	   SQLCHAR *TableName,
	   SQLSMALLINT NameLength3,
	   SQLCHAR *ColumnName,
	   SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumns " PTRFMT, PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLColumns_(stmt,
			   CatalogName, NameLength1,
			   SchemaName, NameLength2,
			   TableName, NameLength3,
			   ColumnName, NameLength4);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLColumnsA(SQLHSTMT StatementHandle,
	    SQLCHAR *CatalogName,
	    SQLSMALLINT NameLength1,
	    SQLCHAR *SchemaName,
	    SQLSMALLINT NameLength2,
	    SQLCHAR *TableName,
	    SQLSMALLINT NameLength3,
	    SQLCHAR *ColumnName,
	    SQLSMALLINT NameLength4)
{
	return SQLColumns(StatementHandle,
			  CatalogName, NameLength1,
			  SchemaName, NameLength2,
			  TableName, NameLength3,
			  ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLColumnsW(SQLHSTMT StatementHandle,
	    SQLWCHAR *CatalogName,
	    SQLSMALLINT NameLength1,
	    SQLWCHAR *SchemaName,
	    SQLSMALLINT NameLength2,
	    SQLWCHAR *TableName,
	    SQLSMALLINT NameLength3,
	    SQLWCHAR *ColumnName,
	    SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL, *column = NULL;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnsW " PTRFMT, PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto exit);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto exit);
	fixWcharIn(TableName, NameLength3, SQLCHAR, table,
		   addStmtError, stmt, goto exit);
	fixWcharIn(ColumnName, NameLength4, SQLCHAR, column,
		   addStmtError, stmt, goto exit);

	rc = SQLColumns_(stmt,
			 catalog, SQL_NTS,
			 schema, SQL_NTS,
			 table, SQL_NTS,
			 column, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);
	if (column)
		free(column);
	return rc;
}
#endif /* WITH_WCHAR */
