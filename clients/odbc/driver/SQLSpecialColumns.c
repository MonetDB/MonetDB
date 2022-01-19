/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
 * SQLSpecialColumns()
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


#ifdef ODBCDEBUG
static char *
translateIdentifierType(SQLUSMALLINT IdentifierType)
{
	switch (IdentifierType) {
	case SQL_BEST_ROWID:
		return "SQL_BEST_ROWID";
	case SQL_ROWVER:
		return "SQL_ROWVER";
	default:
		return "unknown";
	}
}

static char *
translateScope(SQLUSMALLINT Scope)
{
	/* check for valid Scope argument */
	switch (Scope) {
	case SQL_SCOPE_CURROW:
		return "SQL_SCOPE_CURROW";
	case SQL_SCOPE_TRANSACTION:
		return "SQL_SCOPE_TRANSACTION";
	case SQL_SCOPE_SESSION:
		return "SQL_SCOPE_SESSION";
	default:
		return "unknown";
	}
}

static char *
translateNullable(SQLUSMALLINT Nullable)
{
	/* check for valid Nullable argument */
	switch (Nullable) {
	case SQL_NO_NULLS:
		return "SQL_NO_NULLS";
	case SQL_NULLABLE:
		return "SQL_NULLABLE";
	default:
		return "unknown";
	}
}
#endif

static SQLRETURN
MNDBSpecialColumns(ODBCStmt *stmt,
		   SQLUSMALLINT IdentifierType,
		   const SQLCHAR *CatalogName,
		   SQLSMALLINT NameLength1,
		   const SQLCHAR *SchemaName,
		   SQLSMALLINT NameLength2,
		   const SQLCHAR *TableName,
		   SQLSMALLINT NameLength3,
		   SQLUSMALLINT Scope,
		   SQLUSMALLINT Nullable)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *sch = NULL, *tab = NULL;

	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\" %s %s\n",
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, TableName ? (char *) TableName : "",
		translateScope(Scope), translateNullable(Nullable));
#endif

	/* check for valid IdentifierType argument */
	switch (IdentifierType) {
	case SQL_BEST_ROWID:
	case SQL_ROWVER:
		break;
	default:
		/* Column type out of range */
		addStmtError(stmt, "HY097", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Scope argument */
	switch (Scope) {
	case SQL_SCOPE_CURROW:
	case SQL_SCOPE_TRANSACTION:
	case SQL_SCOPE_SESSION:
		break;
	default:
		/* Scope type out of range */
		addStmtError(stmt, "HY098", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Nullable argument */
	switch (Nullable) {
	case SQL_NO_NULLS:
	case SQL_NULLABLE:
		break;
	default:
		/* Nullable type out of range */
		addStmtError(stmt, "HY099", NULL, 0);
		return SQL_ERROR;
	}

	/* check if a valid (non null, not empty) table name is supplied */
	if (TableName == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	if (NameLength3 == 0) {
		/* Invalid string or buffer length */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	/* SQLSpecialColumns returns a table with the following columns:
	   SMALLINT     scope
	   VARCHAR      column_name NOT NULL
	   SMALLINT     data_type NOT NULL
	   VARCHAR      type_name NOT NULL
	   INTEGER      column_size
	   INTEGER      buffer_length
	   SMALLINT     decimal_digits
	   SMALLINT     pseudo_column
	 */
	if (IdentifierType == SQL_BEST_ROWID) {
		/* Select from the key table the (smallest) primary/unique key */
		if (stmt->Dbc->sql_attr_metadata_id == SQL_FALSE) {
			if (NameLength2 > 0) {
				sch = ODBCParseOA("s", "name",
						  (const char *) SchemaName,
						  (size_t) NameLength2);
				if (sch == NULL)
					goto nomem;
			}
			if (NameLength3 > 0) {
				tab = ODBCParseOA("t", "name",
						  (const char *) TableName,
						  (size_t) NameLength3);
				if (tab == NULL)
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
		}

		/* first create a string buffer (1000 extra bytes is plenty */
		querylen = 5000 + NameLength1 + NameLength2 + NameLength3;
		query = malloc(querylen);
		if (query == NULL)
			goto nomem;

		/* Note: SCOPE is SQL_SCOPE_TRANSACTION */
		/* Note: PSEUDO_COLUMN is SQL_PC_NOT_PSEUDO */
		pos += snprintf(query + pos, querylen - pos,
			"with sc as ("
			"select t.id as table_id, k.type as type, "
			       "cast(%d as smallint) as scope, "
			       "c.name as column_name, "
			DATA_TYPE(c) ", "
			TYPE_NAME(c) ", "
			COLUMN_SIZE(c) ", "
			BUFFER_LENGTH(c) ", "
			DECIMAL_DIGITS(c) ", "
			       "cast(%d as smallint) as pseudo_column "
			 "from sys.schemas s, "
			      "sys.tables t, "
			      "sys.columns c, "
			      "sys.keys k, "
			      "sys.objects kc "
			 "where s.id = t.schema_id and "
			       "t.id = c.table_id and "
			       "t.id = k.table_id and "
			       "c.name = kc.name and "
			       "kc.id = k.id and "
			       "k.type = 0",
			/* scope: */
			SQL_SCOPE_TRANSACTION,
#ifdef DATA_TYPE_ARGS
			DATA_TYPE_ARGS,
#endif
#ifdef TYPE_NAME_ARGS
			TYPE_NAME_ARGS,
#endif
#ifdef COLUMN_SIZE_ARGS
			COLUMN_SIZE_ARGS,
#endif
#ifdef BUFFER_SIZE_ARGS
			BUFFER_SIZE_ARGS,
#endif
#ifdef DECIMAL_DIGITS_ARGS
			DECIMAL_DIGITS_ARGS,
#endif
			/* pseudo_column: */
			SQL_PC_NOT_PSEUDO);
		assert(pos < 4300);
		/* TODO: improve the SQL to get the correct result:
		   - only one set of columns should be returned, also
		     when multiple primary keys are available for this
		     table.
		   - when the table has NO primary key it should
		     return the columns of a unique key (only from ONE
		     unique key which is also the best/smallest key)
		   TODO: optimize SQL:
		   - when no SchemaName is set (see above) also no
		     filtering on SCHEMA NAME and join with table
		     SCHEMAS is needed!
		 */

		/* add the selection condition */
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

		/* add an extra selection when SQL_NO_NULLS is requested */
		if (Nullable == SQL_NO_NULLS) {
			pos += strcpy_len(query + pos, " and c.\"null\" = false", querylen - pos);
		}

		pos += strcpy_len(query + pos,
		       "), "
			"tid as ("
			   "select t.id as tid "
			    "from sys._tables t, sys.keys k "
			    "where t.id = k.table_id and k.type = 0"
		       ") "
			"select sc.scope, sc.column_name, sc.data_type, "
			       "sc.type_name, sc.column_size, "
			       "sc.buffer_length, sc.decimal_digits, "
			       "sc.pseudo_column "
			"from sc "
			"where (sc.type = 0 and "
			       "sc.table_id in (select tid from tid)) or "
			      "(sc.type = 1 and "
			       "sc.table_id not in (select tid from tid))",
			querylen - pos);

		/* ordering on SCOPE not needed (since it is constant) */
	} else {
		assert(IdentifierType == SQL_ROWVER);
		/* The backend does not have such info available */
		/* create just a query which results in zero rows */
		/* Note: pseudo_column is sql_pc_unknown is 0 */
		query = strdup("select cast(null as smallint) as scope, "
				      "cast('' as varchar(1)) as column_name, "
				      "cast(1 as smallint) as data_type, "
				      "cast('char' as varchar(4)) as type_name, "
				      "cast(1 as integer) as column_size, "
				      "cast(1 as integer) as buffer_length, "
				      "cast(0 as smallint) as decimal_digits, "
				      "cast(0 as smallint) as pseudo_column "
			       "where 0 = 1");
		if (query == NULL)
			goto nomem;
		pos = strlen(query);
	}

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
	/* Memory allocation error */
	addStmtError(stmt, "HY001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLSpecialColumns(SQLHSTMT StatementHandle,
		  SQLUSMALLINT IdentifierType,
		  SQLCHAR *CatalogName,
		  SQLSMALLINT NameLength1,
		  SQLCHAR *SchemaName,
		  SQLSMALLINT NameLength2,
		  SQLCHAR *TableName,
		  SQLSMALLINT NameLength3,
		  SQLUSMALLINT Scope,
		  SQLUSMALLINT Nullable)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSpecialColumns %p %s ",
		StatementHandle,
		translateIdentifierType(IdentifierType));
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBSpecialColumns(stmt,
				  IdentifierType,
				  CatalogName, NameLength1,
				  SchemaName, NameLength2,
				  TableName, NameLength3,
				  Scope,
				  Nullable);
}

SQLRETURN SQL_API
SQLSpecialColumnsA(SQLHSTMT StatementHandle,
		   SQLUSMALLINT IdentifierType,
		   SQLCHAR *CatalogName,
		   SQLSMALLINT NameLength1,
		   SQLCHAR *SchemaName,
		   SQLSMALLINT NameLength2,
		   SQLCHAR *TableName,
		   SQLSMALLINT NameLength3,
		   SQLUSMALLINT Scope,
		   SQLUSMALLINT Nullable)
{
	return SQLSpecialColumns(StatementHandle,
				 IdentifierType,
				 CatalogName, NameLength1,
				 SchemaName, NameLength2,
				 TableName, NameLength3,
				 Scope,
				 Nullable);
}

SQLRETURN SQL_API
SQLSpecialColumnsW(SQLHSTMT StatementHandle,
		   SQLUSMALLINT IdentifierType,
		   SQLWCHAR *CatalogName,
		   SQLSMALLINT NameLength1,
		   SQLWCHAR *SchemaName,
		   SQLSMALLINT NameLength2,
		   SQLWCHAR *TableName,
		   SQLSMALLINT NameLength3,
		   SQLUSMALLINT Scope,
		   SQLUSMALLINT Nullable)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSpecialColumnsW %p %s ",
		StatementHandle,
		translateIdentifierType(IdentifierType));
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

	rc = MNDBSpecialColumns(stmt,
				IdentifierType,
				catalog, SQL_NTS,
				schema, SQL_NTS,
				table, SQL_NTS,
				Scope,
				Nullable);

      bailout:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);

	return rc;
}
