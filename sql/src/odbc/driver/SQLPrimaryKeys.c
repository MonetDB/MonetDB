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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN
SQLPrimaryKeys(SQLHSTMT hStmt,
	       SQLCHAR *szCatalogName, SQLSMALLINT nCatalogNameLength,
	       SQLCHAR *szSchemaName, SQLSMALLINT nSchemaNameLength,
	       SQLCHAR *szTableName, SQLSMALLINT nTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;	/* pointer to end of built-up query */

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrimaryKeys\n");
#endif

	(void) szCatalogName;	/* Stefan: unused!? */
	(void) nCatalogNameLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, no query should be prepared or executed */
	if (stmt->State != INITED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* deal with SQL_NTS and SQL_NULL_DATA */
	fixODBCstring(szSchemaName, nSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szTableName, nTableNameLength, addStmtError, stmt);

	/* check if a valid (non null, not empty) table name is supplied */
	if (szTableName == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	/* construct the query */
	query = malloc(1000 + nTableNameLength + nSchemaNameLength);
	assert(query);
	query_end = query;

	strcpy(query_end,
	       "select '' as table_cat, s.name as table_schem, "
	       "t.name as table_name, c.name as column_name, "
	       "kc.ordinal_position as key_seq, "
	       "k.key_name as pk_name from schemas s, tables t, "
	       "columns c, keys k, keycolumns kc "
	       "where s.id = t.schema_id and t.id = c.table_id and "
	       "t.id = k.table_id and c.id = kc.column_id and "
	       "kc.key_id = k.key_id and k.is_primary = 1");
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	/* search pattern is not allowed for table name so use = and not LIKE */
	sprintf(query_end, " and t.name = '%.*s'",
		nTableNameLength, szTableName);
	query_end += strlen(query_end);

	if (szSchemaName != NULL) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and s.name = '%.*s'",
			nSchemaNameLength, szSchemaName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end, " order by s.name, t.name, k.key_seq");
	query_end += strlen(query_end);

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect_(hStmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
