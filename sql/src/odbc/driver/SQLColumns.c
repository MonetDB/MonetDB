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
 * Author: Martin van Dinther
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
	    SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	    SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	    SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	    SQLCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

	fixODBCstring(szCatalogName, nCatalogNameLength, addStmtError, stmt);
	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);
	fixODBCstring(szColumnName, nColumnNameLength, addStmtError, stmt);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		nCatalogNameLength, szCatalogName,
		nSchemaNameLength, szSchemaName,
		nTableNameLength, szTableName,
		nColumnNameLength, szColumnName);
#endif

	/* construct the query now */
	query = (char *) malloc(1200 + nSchemaNameLength + nTableNameLength +
				nColumnNameLength);
	assert(query);
	query_end = query;

	/* SQLColumns returns a table with the following columns:
	   VARCHAR	table_cat
	   VARCHAR	table_schem
	   VARCHAR	table_name NOT NULL
	   VARCHAR	column_name NOT NULL
	   SMALLINT	data_type NOT NULL
	   VARCHAR	type_name NOT NULL
	   INTEGER	column_size
	   INTEGER	buffer_length
	   SMALLINT	decimal_digits
	   SMALLINT	num_prec_radix
	   SMALLINT	nullable NOT NULL
	   VARCHAR	remarks
	   VARCHAR	column_def
	   SMALLINT	sql_data_type NOT NULL
	   SMALLINT	sql_datetime_sub
	   INTEGER	char_octet_length
	   INTEGER	ordinal_position NOT NULL
	   VARCHAR	is_nullable
	 */

	sprintf(query_end,
		"select "
		"cast('' as varchar(1)) as table_cat, "
		"s.\"name\" as table_schem, "
		"t.\"name\" as table_name, "
		"c.\"name\" as column_name, "
		"cast(0 as smallint) as data_type, "
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
		"cast('' as varchar(1)) as column_def, "
		"cast(0 as smallint) as sql_data_type, "
		"cast(0 as smallint) as sql_datetime_sub, "
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
	if (nSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped. Check it and may
		   be convert it. */
		sprintf(query_end, " and s.\"name\" %s '%.*s'",
			memchr(szSchemaName, '%', nSchemaNameLength) ||
			memchr(szSchemaName, '_', nSchemaNameLength) ?
			"like" : "=",
			nSchemaNameLength, szSchemaName);
		query_end += strlen(query_end);
	}

	if (nTableNameLength > 0) {
		/* filtering requested on table name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check it and
		   may be convert it. */
		sprintf(query_end, " and t.\"name\" %s '%.*s'",
			memchr(szTableName, '%', nTableNameLength) ||
			memchr(szTableName, '_', nTableNameLength) ?
			"like" : "=",
			nTableNameLength, szTableName);
		query_end += strlen(query_end);
	}

	if (nColumnNameLength > 0) {
		/* filtering requested on column name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check it and
		   may be convert it. */
		sprintf(query_end, " and c.\"name\" %s '%.*s'",
			memchr(szColumnName, '%', nColumnNameLength) ||
			memchr(szColumnName, '_', nColumnNameLength) ?
			"like" : "=",
			nColumnNameLength, szColumnName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end,
	       " order by table_cat, table_schem, "
	       "table_name, ordinal_position");
	query_end += strlen(query_end);
	assert(query_end - query < 1200 + nSchemaNameLength + nTableNameLength + nColumnNameLength);

	/* query the MonetDB data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	if (rc == SQL_SUCCESS) {
		const char ***tuples;
		int i, j, n;
		char *data;
		int concise_type, data_type, sql_data_type, sql_datetime_sub;
		int columnlengths[NCOLUMNS];

		n = stmt->rowcount;
		tuples = malloc(sizeof(*tuples) * n);
		for (j = 0; j < NCOLUMNS; j++)
			columnlengths[j] = mapi_get_len(stmt->hdl, j);
		for (i = 0; i < n; i++) {
			mapi_fetch_row(stmt->hdl);
			tuples[i] = malloc(sizeof(**tuples) * NCOLUMNS);
			for (j = 0; j < NCOLUMNS; j++) {
				data = mapi_fetch_field(stmt->hdl, j);
				tuples[i][j] = data && j != 4 && j != 13 &&
					j != 14 ? strdup(data) : NULL;
			}
			concise_type = ODBCConciseType(tuples[i][5]);
			free((void *) tuples[i][5]);
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
				    columntypes, columnlengths, n, tuples);
		for (i = 0; i < n; i++) {
			for (j = 0; j < NCOLUMNS; j++)
				if (tuples[i][j])
					free((void *) tuples[i][j]);
			free(tuples[i]);
		}
		free(tuples);
		return ODBCInitResult(stmt);
	}

	return rc;
}

SQLRETURN SQL_API
SQLColumns(SQLHSTMT hStmt,
	   SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	   SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	   SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	   SQLCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumns " PTRFMT, PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLColumns_(stmt, szCatalogName, nCatalogNameLength,
			   szSchemaName, nSchemaNameLength,
			   szTableName, nTableNameLength,
			   szColumnName, nColumnNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLColumnsA(SQLHSTMT hStmt,
	    SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	    SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	    SQLCHAR *szTableName, SQLSMALLINT nTableNameLength,
	    SQLCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	return SQLColumns(hStmt, szCatalogName, nCatalogNameLength,
			  szSchemaName, nSchemaNameLength,
			  szTableName, nTableNameLength,
			  szColumnName, nColumnNameLength);
}

SQLRETURN SQL_API
SQLColumnsW(SQLHSTMT hStmt,
	    SQLWCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	    SQLWCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	    SQLWCHAR *szTableName, SQLSMALLINT nTableNameLength,
	    SQLWCHAR *szColumnName, SQLSMALLINT nColumnNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL, *column = NULL;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLColumnsW " PTRFMT, PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, catalog,
		   addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, schema,
		   addStmtError, stmt, goto exit);
	fixWcharIn(szTableName, nTableNameLength, table,
		   addStmtError, stmt, goto exit);
	fixWcharIn(szColumnName, nColumnNameLength, column,
		   addStmtError, stmt, goto exit);

	rc = SQLColumns_(stmt, catalog, SQL_NTS, schema, SQL_NTS,
			 table, SQL_NTS, column, SQL_NTS);
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
#endif	/* WITH_WCHAR */
