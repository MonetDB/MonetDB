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
 * SQLProcedureColumns()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Sjoerd Mullender
 * Date  : 28 Feb 2018
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"
#include "ODBCQueries.h"


static SQLRETURN
MNDBProcedureColumns(ODBCStmt *stmt,
		     const SQLCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     const SQLCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     const SQLCHAR *ProcName,
		     SQLSMALLINT NameLength3,
		     const SQLCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *sch = NULL, *prc = NULL, *col = NULL;

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ProcName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(ColumnName, NameLength4, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, ProcName ? (char *) ProcName : "",
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
			prc = ODBCParsePV("p", "name",
					  (const char *) ProcName,
					  (size_t) NameLength3);
			if (prc == NULL)
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
			prc = ODBCParseID("p", "name",
					  (const char *) ProcName,
					  (size_t) NameLength3);
			if (prc == NULL)
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
	querylen = 6500 + strlen(stmt->Dbc->dbname) +
		(sch ? strlen(sch) : 0) + (prc ? strlen(prc) : 0) +
		(col ? strlen(col) : 0);
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

	/* SQLProcedureColumns returns a table with the following columns:
	   VARCHAR      procedure_cat
	   VARCHAR      procedure_schem
	   VARCHAR      procedure_name NOT NULL
	   VARCHAR      column_name NOT NULL
	   SMALLINT     column_type NOT NULL
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

/* see sql_catalog.h */
#define F_UNION 5
#define FUNC_LANG_SQL 2
	pos += snprintf(query + pos, querylen - pos,
		"select '%s' as procedure_cat, "
		       "s.name as procedure_schem, "
		       "p.name as procedure_name, "
		       "a.name as column_name, "
		       "case when a.inout = 1 then %d "
			    "when p.type = %d then %d "
			    "else %d "
		       "end as column_type, "
		DATA_TYPE(a) ", "
		TYPE_NAME(a) ", "
		COLUMN_SIZE(a) ", "
		BUFFER_LENGTH(a) ", "
		DECIMAL_DIGITS(a) ", "
		NUM_PREC_RADIX(a) ", "
		       "cast(%d as smallint) as nullable, "
		       "%s as remarks, "
		       "cast(null as varchar(1)) as column_def, "
		SQL_DATA_TYPE(a) ", "
		SQL_DATETIME_SUB(a) ", "
		CHAR_OCTET_LENGTH(a) ", "
		       "case when p.type = 5 and a.inout = 0 then a.number + 1 "
			    "when p.type = 5 and a.inout = 1 then a.number - x.maxout "
			    "when a.inout = 0 then 0 "
			    "else a.number + 1 "
		       "end as ordinal_position, "
		       "'' as is_nullable "
		"from sys.schemas s, "
		     "sys.functions p left outer join (select func_id, max(number) as maxout from sys.args where inout = 0 group by func_id) as x on p.id = x.func_id, "
		     "sys.args a%s "
		"where p.language >= %d and "
		      "s.id = p.schema_id and "
		      "p.id = a.func_id",
		stmt->Dbc->dbname,
		/* column_type: */
		SQL_PARAM_INPUT, F_UNION, SQL_RESULT_COL, SQL_RETURN_VALUE,
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
		SQL_NULLABLE_UNKNOWN,
		/* remarks: */
		stmt->Dbc->has_comment ? "c.remark" : "cast(null as varchar(1))",
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
		stmt->Dbc->has_comment ? " left outer join sys.comments c on c.id = a.id" : "",
		FUNC_LANG_SQL);
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
	if (prc) {
		/* filtering requested on procedure name */
		pos += snprintf(query + pos, querylen - pos, " and %s", prc);
		free(prc);
	}
	if (col) {
		/* filtering requested on column name */
		pos += snprintf(query + pos, querylen - pos, " and %s", col);
		free(col);
	}

	/* add the ordering (exclude procedure_cat as it is the same for all rows) */
	pos += strcpy_len(query + pos, " order by procedure_schem, procedure_name, column_type, ordinal_position", querylen - pos);

	/* query the MonetDB data dictionary tables */
	rc = MNDBExecDirect(stmt, (SQLCHAR *) query, (SQLINTEGER) pos);

	free(query);

	return rc;

  nomem:
	/* note that query must be NULL when we get here */
	if (sch)
		free(sch);
	if (prc)
		free(prc);
	if (col)
		free(col);
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLProcedureColumns(SQLHSTMT StatementHandle,
		    SQLCHAR *CatalogName,
		    SQLSMALLINT NameLength1,
		    SQLCHAR *SchemaName,
		    SQLSMALLINT NameLength2,
		    SQLCHAR *ProcName,
		    SQLSMALLINT NameLength3,
		    SQLCHAR *ColumnName,
		    SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedureColumns %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBProcedureColumns(stmt,
				    CatalogName, NameLength1,
				    SchemaName, NameLength2,
				    ProcName, NameLength3,
				    ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLProcedureColumnsA(SQLHSTMT StatementHandle,
		     SQLCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLCHAR *ProcName,
		     SQLSMALLINT NameLength3,
		     SQLCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	return SQLProcedureColumns(StatementHandle,
				   CatalogName, NameLength1,
				   SchemaName, NameLength2,
				   ProcName, NameLength3,
				   ColumnName, NameLength4);
}

SQLRETURN SQL_API
SQLProcedureColumnsW(SQLHSTMT StatementHandle,
		     SQLWCHAR *CatalogName,
		     SQLSMALLINT NameLength1,
		     SQLWCHAR *SchemaName,
		     SQLSMALLINT NameLength2,
		     SQLWCHAR *ProcName,
		     SQLSMALLINT NameLength3,
		     SQLWCHAR *ColumnName,
		     SQLSMALLINT NameLength4)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *proc = NULL, *column = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLProcedureColumnsW %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CatalogName, NameLength1, SQLCHAR, catalog,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(SchemaName, NameLength2, SQLCHAR, schema,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(ProcName, NameLength3, SQLCHAR, proc,
		   addStmtError, stmt, goto bailout);
	fixWcharIn(ColumnName, NameLength4, SQLCHAR, column,
		   addStmtError, stmt, goto bailout);

	rc = MNDBProcedureColumns(stmt,
				  catalog, SQL_NTS,
				  schema, SQL_NTS,
				  proc, SQL_NTS,
				  column, SQL_NTS);

      bailout:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (proc)
		free(proc);
	if (column)
		free(column);

	return rc;
}
