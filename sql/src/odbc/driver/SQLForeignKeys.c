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
 * SQLForeignKeys()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: catalogs are not supported, we ignore any value set for
 * szPKCatalogName and szFKCatalogName.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN
SQLForeignKeys(SQLHSTMT hStmt,
	       SQLCHAR *szPKCatalogName, SQLSMALLINT nPKCatalogNameLength,
	       SQLCHAR *szPKSchemaName, SQLSMALLINT nPKSchemaNameLength,
	       SQLCHAR *szPKTableName, SQLSMALLINT nPKTableNameLength,
	       SQLCHAR *szFKCatalogName, SQLSMALLINT nFKCatalogNameLength,
	       SQLCHAR *szFKSchemaName, SQLSMALLINT nFKSchemaNameLength,
	       SQLCHAR *szFKTableName, SQLSMALLINT nFKTableNameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;	/* pointer to end of built-up query */

#ifdef ODBCDEBUG
	ODBCLOG("SQLForeignKeys\n");
#endif

	(void) szPKCatalogName;	/* Stefan: unused!? */
	(void) nPKCatalogNameLength;	/* Stefan: unused!? */
	(void) szFKCatalogName;	/* Stefan: unused!? */
	(void) nFKCatalogNameLength;	/* Stefan: unused!? */

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
	fixODBCstring(szPKSchemaName, nPKSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szPKTableName, nPKTableNameLength, addStmtError, stmt);
	fixODBCstring(szFKSchemaName, nFKSchemaNameLength, addStmtError, stmt);
	fixODBCstring(szFKTableName, nFKTableNameLength, addStmtError, stmt);

	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	/* first create a string buffer (1000 extra bytes is plenty:
	   we actually need under 600) */
	query = malloc(1000 + nPKSchemaNameLength + nPKTableNameLength +
		       nFKSchemaNameLength + nFKTableNameLength);
	assert(query);
	query_end = query;

	strcpy(query_end,
	       "select '' as pktable_cat, s1.name as pktable_schem, "
	       "t1.name as pktable_name, c1.name as pkcolumn_name, "
	       "'' as fktable_cat, s1.name as fktable_schem, "
	       "t1.name as fktable_name, c1.name as fkcolumn_name, "
	       "kc.ordinal_position as key_seq, "
	       "k.update_rule as update_rule, k.delete_rule as delete_rule, "
	       "k.fk_name as fk_name, k.pk_name as pk_name, "
	       "k.deferrability as deferrability from schemas s, tables t, "
	       "columns c where s.id = t.schema_id and t.id = c.table_id");
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	if (szPKSchemaName != NULL && nPKSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and s1.name = '%.*s'",
			nPKSchemaNameLength, szPKSchemaName);
		query_end += strlen(query_end);
	}

	if (szPKTableName != NULL && nPKTableNameLength > 0) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and t1.name = '%.*s'",
			nPKTableNameLength, szPKTableName);
		query_end += strlen(query_end);
	}

	if (szFKSchemaName != NULL && nFKSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and s2.name = '%.*s'",
			nFKSchemaNameLength, szFKSchemaName);
		query_end += strlen(query_end);
	}

	if (szFKTableName != NULL && nFKTableNameLength > 0) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " and t2.name = '%.*s'",
			nFKTableNameLength, szFKTableName);
		query_end += strlen(query_end);
	}


/* TODO finish the FROM and WHERE clauses */

	/* add the ordering */
	/* if szPKTableName != NULL, selection on primary key, order
	   on FK output columns, else order on PK output columns */
	sprintf(query_end, " order by %s.name, %s.name, kc.ordinal_position",
		szPKTableName != NULL ? "s2" : "s1",
		szPKTableName != NULL ? "t2" : "t1");
	query_end += strlen(query_end);

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect_(stmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
