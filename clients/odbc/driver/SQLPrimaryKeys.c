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
 * SQLPrimaryKeys()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
MNDBPrimaryKeys(ODBCStmt *stmt,
		const SQLCHAR *CatalogName,
		SQLSMALLINT NameLength1,
		const SQLCHAR *SchemaName,
		SQLSMALLINT NameLength2,
		const SQLCHAR *TableName,
		SQLSMALLINT NameLength3)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	size_t querylen;
	size_t pos = 0;
	char *sch = NULL, *tab = NULL;

	/* deal with SQL_NTS and SQL_NULL_DATA */
	fixODBCstring(CatalogName, NameLength1, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(SchemaName, NameLength2, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(TableName, NameLength3, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

	/* check if a valid (non null, not empty) table name is supplied */
	if (TableName == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) NameLength1, CatalogName ? (char *) CatalogName : "",
		(int) NameLength2, SchemaName ? (char *) SchemaName : "",
		(int) NameLength3, TableName ? (char *) TableName : "");
#endif

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
	querylen = 1000 + strlen(stmt->Dbc->dbname) + (sch ? strlen(sch) : 0) +
		(tab ? strlen(tab) : 0);
	query = malloc(querylen);
	if (query == NULL)
		goto nomem;

	/* SQLPrimaryKeys returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name NOT NULL
	   VARCHAR      column_name NOT NULL
	   SMALLINT     key_seq NOT NULL
	   VARCHAR      pk_name
	 */
	pos += snprintf(query + pos, querylen - pos,
	       "select '%s' as table_cat, "
		      "s.name as table_schem, "
		      "t.name as table_name, "
		      "kc.name as column_name, "
		      "cast(kc.nr + 1 as smallint) as key_seq, "
		      "k.name as pk_name "
	       "from sys.schemas s, sys.tables t, "
		    "sys.keys k, sys.objects kc "
	       "where k.id = kc.id and "
		     "k.table_id = t.id and "
		     "t.schema_id = s.id and "
		     "k.type = 0",
		stmt->Dbc->dbname);
	assert(pos < 800);

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

	/* add the ordering */
	pos += strcpy_len(query + pos, " order by table_schem, table_name, key_seq", querylen - pos);

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
SQLPrimaryKeys(SQLHSTMT StatementHandle,
	       SQLCHAR *CatalogName,
	       SQLSMALLINT NameLength1,
	       SQLCHAR *SchemaName,
	       SQLSMALLINT NameLength2,
	       SQLCHAR *TableName,
	       SQLSMALLINT NameLength3)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrimaryKeys %p ", StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBPrimaryKeys(stmt,
			       CatalogName, NameLength1,
			       SchemaName, NameLength2,
			       TableName, NameLength3);
}

SQLRETURN SQL_API
SQLPrimaryKeysA(SQLHSTMT StatementHandle,
		SQLCHAR *CatalogName,
		SQLSMALLINT NameLength1,
		SQLCHAR *SchemaName,
		SQLSMALLINT NameLength2,
		SQLCHAR *TableName,
		SQLSMALLINT NameLength3)
{
	return SQLPrimaryKeys(StatementHandle,
			      CatalogName, NameLength1,
			      SchemaName, NameLength2,
			      TableName, NameLength3);
}

SQLRETURN SQL_API
SQLPrimaryKeysW(SQLHSTMT StatementHandle,
		SQLWCHAR *CatalogName,
		SQLSMALLINT NameLength1,
		SQLWCHAR *SchemaName,
		SQLSMALLINT NameLength2,
		SQLWCHAR *TableName,
		SQLSMALLINT NameLength3)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrimaryKeysW %p ", StatementHandle);
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

	rc = MNDBPrimaryKeys(stmt,
			     catalog, SQL_NTS,
			     schema, SQL_NTS,
			     table, SQL_NTS);

      bailout:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);

	return rc;
}
