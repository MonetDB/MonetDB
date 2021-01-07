/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
#include "ODBCQueries.h"

#define NCOLUMNS	18

static SQLRETURN
MNDBColumns(ODBCStmt *stmt,
	    const SQLCHAR *CatalogName,
	    SQLSMALLINT NameLength1,
	    const SQLCHAR *SchemaName,
	    SQLSMALLINT NameLength2,
	    const SQLCHAR *TableName,
	    SQLSMALLINT NameLength3,
	    const SQLCHAR *ColumnName,
	    SQLSMALLINT NameLength4)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *sch = NULL, *tab = NULL, *col = NULL;

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
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, TableName ? (char *) TableName : "",
		(int) NameLength4, ColumnName ? (char *) ColumnName : "");
#endif

	if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
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
	querylen = 6500 + strlen(stmt->Dbc->dbname) + (sch ? strlen(sch) : 0) +
		(tab ? strlen(tab) : 0) + (col ? strlen(col) : 0);
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

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

	pos += snprintf(query + pos, querylen - pos,
		"select '%s' as table_cat, "
		       "s.name as table_schem, "
		       "t.name as table_name, "
		       "c.name as column_name, "
		DATA_TYPE(c) ", "
		TYPE_NAME(c) ", "
		COLUMN_SIZE(c) ", "
		BUFFER_LENGTH(c) ", "
		DECIMAL_DIGITS(c) ", "
		NUM_PREC_RADIX(c) ", "
		       "case c.\"null\" "
			    "when true then cast(%d as smallint) "
			    "when false then cast(%d as smallint) "
		       "end as nullable, "
		       "%s as remarks, "
		       "c.\"default\" as column_def, "
		SQL_DATA_TYPE(c) ", "
		SQL_DATETIME_SUB(c) ", "
		CHAR_OCTET_LENGTH(c) ", "
		       "cast(c.number + 1 as integer) as ordinal_position, "
		       "case c.\"null\" "
			    "when true then cast('YES' as varchar(3)) "
			    "when false then cast('NO' as varchar(3)) "
		       "end as is_nullable "
		 "from sys.schemas s, "
		      "sys.tables t, "
		      "sys.columns c%s "
		 "where s.id = t.schema_id and "
		       "t.id = c.table_id",
		stmt->Dbc->dbname,
#ifdef DATA_TYPE_ARGS
		DATA_TYPE_ARGS,
#endif
#ifdef TYPE_NAME_ARGS
		TYPE_NAME_ARGS,
#endif
#ifdef COLUMN_SIZE_ARGS
		COLUMN_SIZE_ARGS,
#endif
#ifdef BUFFER_LENGTH_ARGS
		BUFFER_LENGTH_ARGS,
#endif
#ifdef DECIMAL_DIGITS_ARGS
		DECIMAL_DIGITS_ARGS,
#endif
#ifdef NUM_PREC_RADIX_ARGS
		NUM_PREC_RADIX_ARGS,
#endif
		/* nullable: */
		SQL_NULLABLE, SQL_NO_NULLS,
		/* remarks: */
		stmt->Dbc->has_comment ? "com.remark" : "cast(null as varchar(1))",
#ifdef SQL_DATA_TYPE_ARGS
		SQL_DATA_TYPE_ARGS,
#endif
#ifdef SQL_DATETIME_SUB_ARGS
		SQL_DATETIME_SUB_ARGS,
#endif
#ifdef CHAR_OCTET_LENGTH_ARGS
		CHAR_OCTET_LENGTH_ARGS,
#endif
		/* from clause: */
		stmt->Dbc->has_comment ? " left outer join sys.comments com on com.id = c.id" : "");
	assert(pos < 6300);

	/* depending on the input parameter values we must add a
	   variable selection condition dynamically */

	/* Construct the selection condition query part */
	if (NameLength1 > 0 && CatalogName != NULL) {
		/* filtering requested on catalog name */
		if (strcmp((char *) CatalogName, stmt->Dbc->dbname) != 0) {
			/* catalog name does not match the database name, so return no rows */
			pos += snprintf(query + pos, querylen - pos, " and 1=2");
		}
	}
	if (sch) {
		/* filtering requested on schema name */
		pos += snprintf(query + pos, querylen - pos, " and %s", sch);
		free(sch);
	}
	if (tab) {
		/* filtering requested on table name */
		pos += snprintf(query + pos, querylen - pos, " and %s", tab);
		free(tab);
	}
	if (col) {
		/* filtering requested on column name */
		pos += snprintf(query + pos, querylen - pos, " and %s", col);
		free(col);
	}

	/* add the ordering (exclude table_cat as it is the same for all rows) */
	pos += strcpy_len(query + pos, " order by table_schem, table_name, ordinal_position", querylen - pos);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query, (SQLINTEGER) pos);

	free(query);

	return rc;

  nomem:
	/* note that query must be NULL when we get here */
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
	ODBCLOG("SQLColumns %p", StatementHandle);
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
	ODBCLOG("SQLColumnsW %p", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(TableName, NameLength3, SQLCHAR, table,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(ColumnName, NameLength4, SQLCHAR, column,
		   addStmtError, stmt, goto bailout);

	rc = MNDBColumns(stmt,
			 catalog, SQL_NTS,
			 schema, SQL_NTS,
			 table, SQL_NTS,
			 column, SQL_NTS);

      bailout:
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
