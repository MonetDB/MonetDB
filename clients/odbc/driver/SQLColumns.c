/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

#define NCOLUMNS	18

static SQLRETURN
MNDBColumns(ODBCStmt *stmt,
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
	char *cat = NULL, *sch = NULL, *tab = NULL, *col = NULL;

	/* null pointers not allowed if arguments are identifiers */
	if (stmt->Dbc->sql_attr_metadata_id == SQL_TRUE &&
	    (SchemaName == NULL || TableName == NULL || ColumnName == NULL)) {
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

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

	if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
		if (NameLength1 > 0) {
			cat = ODBCParseOA("e", "value",
					  (const char *) CatalogName,
					  (size_t) NameLength1);
			if (cat == NULL)
				goto nomem;
		}
		if (NameLength2 > 0) {
			sch = ODBCParsePV("s", "name",
					  (const char *) SchemaName,
					  (size_t) NameLength2);
			if (sch == NULL)
				goto nomem;
		}
		if (NameLength3 > 0) {
			tab = ODBCParsePV("t", "name",
					  (const char *) TableName,
					  (size_t) NameLength3);
			if (tab == NULL)
				goto nomem;
		}
		if (NameLength4 > 0) {
			col = ODBCParsePV("c", "name",
					  (const char *) ColumnName,
					  (size_t) NameLength4);
			if (col == NULL)
				goto nomem;
		}
	} else {
		if (NameLength1 > 0) {
			cat = ODBCParseID("e", "value",
					  (const char *) CatalogName,
					  (size_t) NameLength1);
			if (cat == NULL)
				goto nomem;
		}
		if (NameLength2 > 0) {
			sch = ODBCParseID("s", "name",
					  (const char *) SchemaName,
					  (size_t) NameLength2);
			if (sch == NULL)
				goto nomem;
		}
		if (NameLength3 > 0) {
			tab = ODBCParseID("t", "name",
					  (const char *) TableName,
					  (size_t) NameLength3);
			if (tab == NULL)
				goto nomem;
		}
		if (NameLength4 > 0) {
			col = ODBCParseID("c", "name",
					  (const char *) ColumnName,
					  (size_t) NameLength4);
			if (col == NULL)
				goto nomem;
		}
	}

	/* construct the query now */
	query = malloc(6500 + (cat ? strlen(cat) : 0) +
		       (sch ? strlen(sch) : 0) + (tab ? strlen(tab) : 0) +
		       (col ? strlen(col) : 0));
	if (query == NULL)
		goto nomem;
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
		"select e.value as table_cat, "
		       "s.name as table_schem, "
		       "t.name as table_name, "
		       "c.name as column_name, "
		       "case c.type "
			    "when 'bigint' then %d "
			    "when 'blob' then %d "
			    "when 'boolean' then %d "
			    "when 'char' then %d "
			    "when 'clob' then %d "
			    "when 'date' then %d "
			    "when 'decimal' then %d "
			    "when 'double' then %d "
			    "when 'int' then %d "
			    "when 'month_interval' then "
				 "case c.type_digits "
				      "when 1 then %d "
				      "when 2 then %d "
				      "when 3 then %d "
				 "end "
			    "when 'real' then %d "
			    "when 'sec_interval' then "
				 "case c.type_digits "
				      "when 4 then %d "
				      "when 5 then %d "
				      "when 6 then %d "
				      "when 7 then %d "
				      "when 8 then %d "
				      "when 9 then %d "
				      "when 10 then %d "
				      "when 11 then %d "
				      "when 12 then %d "
				      "when 13 then %d "
				 "end "
			    "when 'smallint' then %d "
			    "when 'time' then %d "
			    "when 'timestamp' then %d "
			    "when 'timestamptz' then %d "
			    "when 'timetz' then %d "
			    "when 'tinyint' then %d "
			    "when 'varchar' then %d "
		       "end as data_type, "
		       "case c.type "
			    "when 'bigint' then 'BIGINT' "
			    "when 'blob' then 'BINARY LARGE OBJECT' "
			    "when 'boolean' then 'BOOLEAN' "
			    "when 'char' then 'CHARACTER' "
			    "when 'clob' then 'CHARACTER LARGE OBJECT' "
			    "when 'date' then 'DATE' "
			    "when 'decimal' then 'DECIMAL' "
			    "when 'double' then 'DOUBLE' "
			    "when 'int' then 'INTEGER' "
			    "when 'month_interval' then "
				 "case c.type_digits "
				      "when 1 then 'INTERVAL YEAR' "
				      "when 2 then 'INTERVAL YEAR TO MONTH' "
				      "when 3 then 'INTERVAL MONTH' "
				 "end "
			    "when 'real' then 'REAL' "
			    "when 'sec_interval' then "
				 "case c.type_digits "
				      "when 4 then 'INTERVAL DAY' "
				      "when 5 then 'INTERVAL DAY TO HOUR' "
				      "when 6 then 'INTERVAL DAY TO MINUTE' "
				      "when 7 then 'INTERVAL DAY TO SECOND' "
				      "when 8 then 'INTERVAL HOUR' "
				      "when 9 then 'INTERVAL HOUR TO MINUTE' "
				      "when 10 then 'INTERVAL HOUR TO SECOND' "
				      "when 11 then 'INTERVAL MINUTE' "
				      "when 12 then 'INTERVAL MINUTE TO SECOND' "
				      "when 13 then 'INTERVAL SECOND' "
				 "end "
			    "when 'smallint' then 'SMALLINT' "
			    "when 'time' then 'TIME' "
			    "when 'timestamp' then 'TIMESTAMP' "
			    "when 'timestamptz' then 'TIMESTAMP' "
			    "when 'timetz' then 'TIME' "
			    "when 'tinyint' then 'TINYINT' "
			    "when 'varchar' then 'VARCHAR' "
		       "end as type_name, "
		       "case c.type "
			    "when 'date' then 10 "
			    "when 'month_interval' then "
				 "case c.type_digits "
				      "when 1 then 26 "
				      "when 2 then 38 "
				      "when 3 then 27 "
				 "end "
			    "when 'sec_interval' then "
				 "case c.type_digits "
				      "when 4 then 25 "
				      "when 5 then 36 "
				      "when 6 then 41 "
				      "when 7 then 47 "
				      "when 8 then 26 "
				      "when 9 then 39 "
				      "when 10 then 45 "
				      "when 11 then 28 "
				      "when 12 then 44 "
				      "when 13 then 30 "
				 "end "
			    "when 'time' then 12 "
			    "when 'timestamp' then 23 "
			    "when 'timestamptz' then 23 "
			    "when 'timetz' then 12 "
			    "else c.type_digits "
		       "end as column_size, "
		       "case c.type "
			    "when 'bigint' then 20 "
			    "when 'char' then 2 * c.type_digits "
			    "when 'clob' then 2 * c.type_digits "
			    "when 'date' then 10 "
			    "when 'double' then 24 "
			    "when 'int' then 11 "
			    "when 'month_interval' then "
				 "case c.type_digits "
				      "when 1 then 26 "
				      "when 2 then 38 "
				      "when 3 then 27 "
				 "end "
			    "when 'real' then 14 "
			    "when 'sec_interval' then "
				 "case c.type_digits "
				      "when 4 then 25 "
				      "when 5 then 36 "
				      "when 6 then 41 "
				      "when 7 then 47 "
				      "when 8 then 26 "
				      "when 9 then 39 "
				      "when 10 then 45 "
				      "when 11 then 28 "
				      "when 12 then 44 "
				      "when 13 then 30 "
				 "end "
			    "when 'smallint' then 6 "
			    "when 'time' then 12 "
			    "when 'timestamp' then 23 "
			    "when 'timestamptz' then 23 "
			    "when 'timetz' then 12 "
			    "when 'tinyint' then 4 "
			    "when 'varchar' then 2 * c.type_digits "
			    "else c.type_digits "
		       "end as buffer_length, "
		       "case c.type "
			    "when 'bigint' then 19 "
			    "when 'decimal' then c.type_scale "
			    "when 'double' then "
				 "case when c.type_digits = 53 and c.type_scale = 0 then 15 "
				 "else c.type_digits "
				 "end "
			    "when 'int' then 10 "
			    "when 'month_interval' then 0 "
			    "when 'real' then "
				 "case when c.type_digits = 24 and c.type_scale = 0 then 7 "
				 "else c.type_digits "
				 "end "
			    "when 'sec_interval' then 0 "
			    "when 'smallint' then 5 "
			    "when 'time' then c.type_digits - 1 "
			    "when 'timestamp' then c.type_digits - 1 "
			    "when 'timestamptz' then c.type_digits - 1 "
			    "when 'timetz' then c.type_digits - 1 "
			    "when 'tinyint' then 3 "
			    "else cast(null as smallint) "
		       "end as decimal_digits, "
		       "case c.type "
			    "when 'bigint' then 2 "
			    "when 'decimal' then 10 "
			    "when 'double' then "
				 "case when c.type_digits = 53 and c.type_scale = 0 then 2 "
				 "else 10 "
				 "end "
			    "when 'int' then 2 "
			    "when 'real' then "
				 "case when c.type_digits = 24 and c.type_scale = 0 then 2 "
				 "else 10 "
				 "end "
			    "when 'smallint' then 2 "
			    "when 'tinyint' then 2 "
			    "else cast(null as smallint) "
		       "end as num_prec_radix, "
		       "case c.\"null\" "
			    "when true then cast(%d as smallint) "
			    "when false then cast(%d as smallint) "
		       "end as nullable, "
		       "cast('' as varchar(1)) as remarks, "
		       "c.\"default\" as column_def, "
		       "case c.type "
			    "when 'bigint' then %d "
			    "when 'blob' then %d "
			    "when 'boolean' then %d "
			    "when 'char' then %d "
			    "when 'clob' then %d "
			    "when 'date' then %d "
			    "when 'decimal' then %d "
			    "when 'double' then %d "
			    "when 'int' then %d "
			    "when 'month_interval' then %d "
			    "when 'real' then %d "
			    "when 'sec_interval' then %d "
			    "when 'smallint' then %d "
			    "when 'time' then %d "
			    "when 'timestamp' then %d "
			    "when 'timestamptz' then %d "
			    "when 'timetz' then %d "
			    "when 'tinyint' then %d "
			    "when 'varchar' then %d "
		       "end as sql_data_type, "
		       "case c.type "
			    "when 'date' then %d "
			    "when 'month_interval' then "
				 "case c.type_digits "
				      "when 1 then %d "
				      "when 2 then %d "
				      "when 3 then %d "
				 "end "
			    "when 'sec_interval' then "
				 "case c.type_digits "
				      "when 4 then %d "
				      "when 5 then %d "
				      "when 6 then %d "
				      "when 7 then %d "
				      "when 8 then %d "
				      "when 9 then %d "
				      "when 10 then %d "
				      "when 11 then %d "
				      "when 12 then %d "
				      "when 13 then %d "
				 "end "
			    "when 'time' then %d "
			    "when 'timestamp' then %d "
			    "when 'timestamptz' then %d "
			    "when 'timetz' then %d "
			    "else cast(null as smallint) "
		       "end as sql_datetime_sub, "
		       "case c.type "
			    "when 'char' then 2 * c.type_digits "
			    "when 'varchar' then 2 * c.type_digits "
			    "when 'clob' then 2 * c.type_digits "
			    "when 'blob' then c.type_digits "
			    "else cast(null as integer) "
		       "end as char_octet_length, "
		       "cast(c.number + 1 as integer) as ordinal_position, "
		       "case c.\"null\" "
			    "when true then cast('YES' as varchar(3)) "
			    "when false then cast('NO' as varchar(3)) "
		       "end as is_nullable "
		 "from sys.schemas s, "
		      "sys.tables t, "
		      "sys.columns c, "
		      "sys.env() e "
		 "where s.id = t.schema_id and "
		       "t.id = c.table_id and "
		       "e.name = 'gdk_dbname'",
		/* data_type: */
		SQL_BIGINT, SQL_LONGVARBINARY, SQL_BIT, SQL_WCHAR,
		SQL_WLONGVARCHAR, SQL_TYPE_DATE, SQL_DECIMAL, SQL_DOUBLE,
		SQL_INTEGER, SQL_INTERVAL_YEAR, SQL_INTERVAL_YEAR_TO_MONTH,
		SQL_INTERVAL_MONTH, SQL_REAL, SQL_INTERVAL_DAY,
		SQL_INTERVAL_DAY_TO_HOUR, SQL_INTERVAL_DAY_TO_MINUTE,
		SQL_INTERVAL_DAY_TO_SECOND, SQL_INTERVAL_HOUR,
		SQL_INTERVAL_HOUR_TO_MINUTE, SQL_INTERVAL_HOUR_TO_SECOND,
		SQL_INTERVAL_MINUTE, SQL_INTERVAL_MINUTE_TO_SECOND,
		SQL_INTERVAL_SECOND, SQL_SMALLINT, SQL_TYPE_TIME,
		SQL_TYPE_TIMESTAMP, SQL_TYPE_TIMESTAMP, SQL_TYPE_TIME,
		SQL_TINYINT, SQL_WVARCHAR,
		/* nullable: */
		SQL_NULLABLE, SQL_NO_NULLS,
		/* sql_data_type: */
		SQL_BIGINT, SQL_LONGVARBINARY, SQL_BIT, SQL_WCHAR,
		SQL_WLONGVARCHAR, SQL_DATETIME, SQL_DECIMAL, SQL_DOUBLE,
		SQL_INTEGER, SQL_INTERVAL, SQL_REAL, SQL_INTERVAL,
		SQL_SMALLINT, SQL_DATETIME, SQL_DATETIME, SQL_DATETIME,
		SQL_DATETIME, SQL_TINYINT, SQL_WVARCHAR,
		/* sql_datetime_sub: */
		SQL_CODE_DATE, SQL_CODE_YEAR, SQL_CODE_YEAR_TO_MONTH,
		SQL_CODE_MONTH, SQL_CODE_DAY, SQL_CODE_DAY_TO_HOUR,
		SQL_CODE_DAY_TO_MINUTE, SQL_CODE_DAY_TO_SECOND,
		SQL_CODE_HOUR, SQL_CODE_HOUR_TO_MINUTE,
		SQL_CODE_HOUR_TO_SECOND, SQL_CODE_MINUTE,
		SQL_CODE_MINUTE_TO_SECOND, SQL_CODE_SECOND,
		SQL_CODE_TIME, SQL_CODE_TIMESTAMP, SQL_CODE_TIMESTAMP,
		SQL_CODE_TIME);
	assert(strlen(query) < 6300);
	query_end += strlen(query_end);

	/* depending on the input parameter values we must add a
	   variable selection condition dynamically */

	/* Construct the selection condition query part */
	if (cat) {
		/* filtering requested on catalog name */
		sprintf(query_end, " and %s", cat);
		query_end += strlen(query_end);
		free(cat);
	}
	if (sch) {
		/* filtering requested on schema name */
		sprintf(query_end, " and %s", sch);
		query_end += strlen(query_end);
		free(sch);
	}
	if (tab) {
		/* filtering requested on table name */
		sprintf(query_end, " and %s", tab);
		query_end += strlen(query_end);
		free(tab);
	}
	if (col) {
		/* filtering requested on column name */
		sprintf(query_end, " and %s", col);
		query_end += strlen(query_end);
		free(col);
	}

	/* add the ordering */
	strcpy(query_end,
	       " order by table_cat, table_schem, "
	       "table_name, ordinal_position");
	query_end += strlen(query_end);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;

  nomem:
	/* note that query must be NULL when we get here */
	if (cat)
		free(cat);
	if (sch)
		free(sch);
	if (tab)
		free(tab);
	if (col)
		free(col);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
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

	return MNDBColumns(stmt,
			   CatalogName, NameLength1,
			   SchemaName, NameLength2,
			   TableName, NameLength3,
			   ColumnName, NameLength4);
}

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

	rc = MNDBColumns(stmt,
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
