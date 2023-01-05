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
	   SMALLINT     SCOPE
	   VARCHAR      COLUMN_NAME NOT NULL
	   SMALLINT     DATA_TYPE NOT NULL
	   VARCHAR      TYPE_NAME NOT NULL
	   INTEGER      COLUMN_SIZE
	   INTEGER      BUFFER_LENGTH
	   SMALLINT     DECIMAL_DIGITS
	   SMALLINT     PSEUDO_COLUMN
	*/
	if (IdentifierType == SQL_BEST_ROWID) {
		size_t querylen;

		/* determine if we need to add a query against the tmp.* tables */
		bool inclTmpKey = (SchemaName == NULL)
				|| (SchemaName != NULL
				 && (strcmp((const char *) SchemaName, "tmp") == 0
				  || strchr((const char *) SchemaName, '%') != NULL
				  || strchr((const char *) SchemaName, '_') != NULL));

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

		/* construct the query */
		querylen = 6000 + (sch ? strlen(sch) : 0) + (tab ? strlen(tab) : 0);
		query = malloc(querylen);
		if (query == NULL)
			goto nomem;

		/* When there is a PK for the table we return the pkey columns.
		 * When there is No PK but there are multiple unique constraints, we need to pick one.
		 * In the current implementation we return the first uc (lowest sys.keys.id).
		 * When there is no PK or unique constraints and it is not a
		 * view, we return all the columns of the table.
		 *
		 * Instead of the first uc (in case of multiple) we could potentially use the uc which has
		 *  a) the least number of columns and
		 *  b) the most efficient datatype (integers) or smallest total(size in bytes).
		 * That's much more complex to do in SQL than the current implementation.
		 * The current implementation (picking first uc) is fast and
		 * gives a correct result, hence preferred.
		 */

		/* 1st cte: syskeys */
		pos += strcpy_len(query + pos,
			"with syskeys as ("
			/* all pkeys */
			"SELECT \"id\", \"table_id\" FROM \"sys\".\"keys\" WHERE \"type\" = 0 "
			"UNION ALL "
			/* and first unique constraint of a table when table has no pkey */
			"SELECT \"id\", \"table_id\" FROM \"sys\".\"keys\" WHERE \"type\" = 1 "
			"AND \"table_id\" NOT IN (select \"table_id\" from \"sys\".\"keys\" where \"type\" = 0) "
			"AND (\"table_id\", \"id\") IN (select \"table_id\", min(\"id\") from \"sys\".\"keys\" where \"type\" = 1 group by \"table_id\"))",
			querylen - pos);
		if (inclTmpKey) {
			/* we must also include the primary key or unique constraint of local temporary tables which are stored in tmp.keys */
			/* 2nd cte: tmpkeys */
			pos += strcpy_len(query + pos,
			", tmpkeys as ("
			"SELECT \"id\", \"table_id\" FROM \"tmp\".\"keys\" WHERE \"type\" = 0 "
			"UNION ALL "
			"SELECT \"id\", \"table_id\" FROM \"tmp\".\"keys\" WHERE \"type\" = 1 "
			"AND \"table_id\" NOT IN (select \"table_id\" from \"tmp\".\"keys\" where \"type\" = 0) "
			"AND (\"table_id\", \"id\") IN (select \"table_id\", min(\"id\") from \"tmp\".\"keys\" where \"type\" = 1 group by \"table_id\"))",
			querylen - pos);
		}
		/* 3rd cte: tableids */
		pos += strcpy_len(query + pos,
			", tableids as ("
			"SELECT t.\"id\" "
			"FROM \"sys\".\"tables\" t "
			"JOIN \"sys\".\"schemas\" s ON t.\"schema_id\" = s.\"id\" "
			"WHERE t.\"type\" NOT IN (1, 11)",	/* exclude all VIEWs and SYSTEM VIEWs */
			querylen - pos);
		/* add the selection condition */
		if (NameLength1 > 0 && CatalogName != NULL) {
			/* filtering requested on catalog name */
			if (strcmp((char *) CatalogName, stmt->Dbc->dbname) != 0) {
				/* catalog name does not match the database name, so return no rows */
				pos += strcpy_len(query + pos, " and 1=2", querylen - pos);
			}
		}
		if (sch) {
			/* filtering requested on schema name */
			pos += snprintf(query + pos, querylen - pos, " and %s", sch);
		}
		if (tab) {
			/* filtering requested on table name */
			pos += snprintf(query + pos, querylen - pos, " and %s", tab);
		}
		/* 4th cte: cols, this unions 2 (or 4 when inclTmpKey == true) select queries */
		pos += strcpy_len(query + pos,
			"), cols as ("
			"SELECT c.\"name\", c.\"type\", c.\"type_digits\", c.\"type_scale\", o.\"nr\" "
			"FROM syskeys k "
			"JOIN tableids t ON k.\"table_id\" = t.\"id\" "
			"JOIN \"sys\".\"objects\" o ON k.\"id\" = o.\"id\" "
			"JOIN \"sys\".\"_columns\" c ON (k.\"table_id\" = c.\"table_id\" AND o.\"name\" = c.\"name\")",
			querylen - pos);
		/* add an extra selection when SQL_NO_NULLS is requested */
		if (Nullable == SQL_NO_NULLS) {
			pos += strcpy_len(query + pos, " WHERE c.\"null\" = false", querylen - pos);
		}
		if (inclTmpKey) {
			/* we must also include the primary key or unique constraint of local temporary tables
			 * which are stored in tmp.keys, tmp.objects, tmp._tables and tmp._columns */
			pos += strcpy_len(query + pos,
			" UNION ALL "
			"SELECT c.\"name\", c.\"type\", c.\"type_digits\", c.\"type_scale\", o.\"nr\" "
			"FROM tmpkeys k "
			"JOIN tableids t ON k.\"table_id\" = t.\"id\" "
			"JOIN \"tmp\".\"objects\" o ON k.\"id\" = o.\"id\" "
			"JOIN \"tmp\".\"_columns\" c ON (k.\"table_id\" = c.\"table_id\" AND o.\"name\" = c.\"name\")",
			querylen - pos);
			/* add an extra selection when SQL_NO_NULLS is requested */
			if (Nullable == SQL_NO_NULLS) {
				pos += strcpy_len(query + pos, " WHERE c.\"null\" = false", querylen - pos);
			}
		}
		/* when there is No PK and No unique constraints, we should return all columns of the table */
		pos += strcpy_len(query + pos,
			" UNION ALL "
			"SELECT c.\"name\", c.\"type\", c.\"type_digits\", c.\"type_scale\", c.\"number\" "
			"FROM tableids t "
			"JOIN \"sys\".\"_columns\" c ON t.\"id\" = c.\"table_id\" "
			"WHERE t.\"id\" NOT IN (SELECT \"table_id\" FROM \"sys\".\"keys\" WHERE \"type\" in (0, 1))",
			querylen - pos);
		/* add an extra selection when SQL_NO_NULLS is requested */
		if (Nullable == SQL_NO_NULLS) {
			pos += strcpy_len(query + pos, " AND c.\"null\" = false", querylen - pos);
		}
		if (inclTmpKey) {
			pos += strcpy_len(query + pos,
			" UNION ALL "
			"SELECT c.\"name\", c.\"type\", c.\"type_digits\", c.\"type_scale\", c.\"number\" "
			"FROM tableids t "
			"JOIN \"tmp\".\"_columns\" c ON t.\"id\" = c.\"table_id\" "
			"WHERE t.\"id\" NOT IN (SELECT \"table_id\" FROM \"tmp\".\"keys\" WHERE \"type\" in (0, 1))",
			querylen - pos);
			/* add an extra selection when SQL_NO_NULLS is requested */
			if (Nullable == SQL_NO_NULLS) {
				pos += strcpy_len(query + pos, " AND c.\"null\" = false", querylen - pos);
			}
		}
		/* the final select query */
		/* Note: SCOPE is SQL_SCOPE_TRANSACTION */
		/* Note: PSEUDO_COLUMN is SQL_PC_NOT_PSEUDO */
		pos += snprintf(query + pos, querylen - pos,
			") SELECT "
			"cast(%d AS smallint) AS \"SCOPE\", "
			"c.\"name\" AS \"COLUMN_NAME\", "
			DATA_TYPE(c) ", "
			TYPE_NAME(c) ", "
			COLUMN_SIZE(c) ", "
			BUFFER_LENGTH(c) ", "
			DECIMAL_DIGITS(c) ", "
			"cast(%d AS smallint) AS \"PSEUDO_COLUMN\" "
			"FROM cols c "
			"ORDER BY \"SCOPE\", c.\"nr\", \"COLUMN_NAME\"",
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

		if (sch)
			free(sch);
		if (tab)
			free(tab);

		if (pos >= querylen)
			fprintf(stderr, "pos >= querylen, %zu > %zu\n", pos, querylen);
		assert(pos < querylen);
	} else {
		assert(IdentifierType == SQL_ROWVER);
		/* The backend does not have such info available */
		/* create just a query which results in zero rows */
		/* Note: PSEUDO_COLUMN is SQL_PC_UNKNOWN is 0 */
		query = strdup("select cast(null as smallint) as \"SCOPE\", "
				      "cast('' as varchar(1)) as \"COLUMN_NAME\", "
				      "cast(1 as smallint) as \"DATA_TYPE\", "
				      "cast('char' as varchar(4)) as \"TYPE_NAME\", "
				      "cast(1 as integer) as \"COLUMN_SIZE\", "
				      "cast(1 as integer) as \"BUFFER_LENGTH\", "
				      "cast(0 as smallint) as \"DECIMAL_DIGITS\", "
				      "cast(0 as smallint) as \"PSEUDO_COLUMN\" "
			       "where 0 = 1");
		if (query == NULL)
			goto nomem;
		pos = strlen(query);
	}

	/* debug: fprintf(stdout, "SQLSpecialColumns query (pos: %zu, len: %zu):\n%s\n\n", pos, strlen(query), query); */

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
