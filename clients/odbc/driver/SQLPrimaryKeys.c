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
 * SQLPrimaryKeys()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: catalogs are not supported, we ignore any value set for
 * szCatalogName.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
SQLPrimaryKeys_(ODBCStmt *stmt,
		SQLCHAR *szCatalogName,
		SQLSMALLINT nCatalogNameLength,
		SQLCHAR *szSchemaName,
		SQLSMALLINT nSchemaNameLength,
		SQLCHAR *szTableName,
		SQLSMALLINT nTableNameLength)
{
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;	/* pointer to end of built-up query */

	/* deal with SQL_NTS and SQL_NULL_DATA */
	fixODBCstring(szCatalogName, nCatalogNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szSchemaName, nSchemaNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);
	fixODBCstring(szTableName, nTableNameLength, SQLSMALLINT, addStmtError, stmt, return SQL_ERROR);

	/* check if a valid (non null, not empty) table name is supplied */
	if (szTableName == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" \"%.*s\" \"%.*s\"\n",
		(int) nCatalogNameLength, (char *) szCatalogName,
		(int) nSchemaNameLength, (char *) szSchemaName,
		(int) nTableNameLength, (char *) szTableName);
#endif

	/* construct the query */
	query = (char *) malloc(1000 + nTableNameLength + nSchemaNameLength);
	assert(query);
	query_end = query;

	/* SQLPrimaryKeys returns a table with the following columns:
	   VARCHAR      table_cat
	   VARCHAR      table_schem
	   VARCHAR      table_name NOT NULL
	   VARCHAR      column_name NOT NULL
	   SMALLINT     key_seq NOT NULL
	   VARCHAR      pk_name
	 */
	strcpy(query_end,
	       "select "
	       "cast(null as varchar(1)) as table_cat, "
	       "s.\"name\" as table_schem, "
	       "t.\"name\" as table_name, "
	       "kc.\"name\" as column_name, "
	       "cast(kc.\"nr\" + 1 as smallint) as key_seq, "
	       "k.\"name\" as pk_name "
	       "from sys.\"schemas\" s, sys.\"tables\" t, "
	       "sys.\"keys\" k, sys.\"objects\" kc "
	       "where k.\"id\" = kc.\"id\" and "
	       "k.\"table_id\" = t.\"id\" and "
	       "t.\"schema_id\" = s.\"id\" and "
	       "k.\"type\" = 0");
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	/* search pattern is not allowed for table name so use = and not LIKE */
	sprintf(query_end, " and t.\"name\" = '%.*s'", nTableNameLength, (char*)szTableName);
	query_end += strlen(query_end);

	if (szSchemaName != NULL) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and s.\"name\" = '%.*s'", nSchemaNameLength, (char*)szSchemaName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end, " order by table_schem, table_name, key_seq");
	query_end += strlen(query_end);
	assert(query_end - query < 1000 + nTableNameLength + nSchemaNameLength);

	/* query the MonetDB data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query, (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}

SQLRETURN SQL_API
SQLPrimaryKeys(SQLHSTMT hStmt,
	       SQLCHAR *szCatalogName,
	       SQLSMALLINT nCatalogNameLength,
	       SQLCHAR *szSchemaName,
	       SQLSMALLINT nSchemaNameLength,
	       SQLCHAR *szTableName,
	       SQLSMALLINT nTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrimaryKeys " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLPrimaryKeys_(stmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szTableName, nTableNameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLPrimaryKeysA(SQLHSTMT hStmt,
		SQLCHAR *szCatalogName,
		SQLSMALLINT nCatalogNameLength,
		SQLCHAR *szSchemaName,
		SQLSMALLINT nSchemaNameLength,
		SQLCHAR *szTableName,
		SQLSMALLINT nTableNameLength)
{
	return SQLPrimaryKeys(hStmt, szCatalogName, nCatalogNameLength, szSchemaName, nSchemaNameLength, szTableName, nTableNameLength);
}

SQLRETURN SQL_API
SQLPrimaryKeysW(SQLHSTMT hStmt,
		SQLWCHAR * szCatalogName,
		SQLSMALLINT nCatalogNameLength,
		SQLWCHAR * szSchemaName,
		SQLSMALLINT nSchemaNameLength,
		SQLWCHAR * szTableName,
		SQLSMALLINT nTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc = SQL_ERROR;
	SQLCHAR *catalog = NULL, *schema = NULL, *table = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrimaryKeysW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCatalogName, nCatalogNameLength, SQLCHAR, catalog, addStmtError, stmt, goto exit);
	fixWcharIn(szSchemaName, nSchemaNameLength, SQLCHAR, schema, addStmtError, stmt, goto exit);
	fixWcharIn(szTableName, nTableNameLength, SQLCHAR, table, addStmtError, stmt, goto exit);

	rc = SQLPrimaryKeys_(stmt, catalog, SQL_NTS, schema, SQL_NTS, table, SQL_NTS);

      exit:
	if (catalog)
		free(catalog);
	if (schema)
		free(schema);
	if (table)
		free(table);

	return rc;
}
#endif /* WITH_WCHAR */
