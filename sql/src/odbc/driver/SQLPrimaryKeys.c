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
	fixODBCstring(szSchemaName, nSchemaNameLength);
	fixODBCstring(szTableName, nTableNameLength);

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
	       "SELECT '' AS TABLE_CAT, S.NAME AS TABLE_SCHEM, "
	       "T.NAME AS TABLE_NAME, C.NAME AS COLUMN_NAME, "
	       "KC.ORDINAL_POSITION AS KEY_SEQ, "
	       "K.KEY_NAME AS PK_NAME FROM SCHEMAS S, TABLES T, "
	       "COLUMNS C, KEYS K, KEYCOLUMNS KC "
	       "WHERE S.ID = T.SCHEMA_ID AND T.ID = C.TABLE_ID AND "
	       "T.ID = K.TABLE_ID AND C.ID = KC.COLUMN_ID AND "
	       "KC.KEY_ID = K.KEY_ID AND K.IS_PRIMARY = 1");
	query_end += strlen(query_end);

	/* Construct the selection condition query part */
	/* search pattern is not allowed for table name so use = and not LIKE */
	sprintf(query_end, " AND T.TABLE_NAME = '%.*s'",
		nTableNameLength, szTableName);
	query_end += strlen(query_end);

	if (szSchemaName != NULL) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		sprintf(query_end, " AND S.NAME = '%.*s'",
			nSchemaNameLength, szSchemaName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end, " ORDER BY S.NAME, T.NAME, K.KEY_SEQ");
	query_end += strlen(query_end);

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect_(hStmt, (SQLCHAR *) query,
			    (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
