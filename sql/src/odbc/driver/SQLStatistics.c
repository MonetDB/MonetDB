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
 * SQLStatistics()
 * CLI Compliance: ISO 92
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
SQLStatistics(SQLHSTMT hStmt, SQLCHAR *szCatalogName,
	      SQLSMALLINT nCatalogNameLength, SQLCHAR *szSchemaName,
	      SQLSMALLINT nSchemaNameLength, SQLCHAR *szTableName,
	      SQLSMALLINT nTableNameLength, SQLUSMALLINT nUnique,
	      SQLUSMALLINT nReserved)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;
	char *query_end = NULL;

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

	/* check for valid Unique argument */
	switch (nUnique) {
	case SQL_INDEX_ALL:
	case SQL_INDEX_UNIQUE:
		break;
	default:
		/* HY100 = Invalid Unique value */
		addStmtError(stmt, "HY100", NULL, 0);
		return SQL_ERROR;
	}

	/* check for valid Reserved argument */
	switch (nReserved) {
	case SQL_ENSURE:
	case SQL_QUICK:
		break;
	default:
		/* HY101 = Invalid Reserved value */
		addStmtError(stmt, "HY101", NULL, 0);
		return SQL_ERROR;
	}


	/* check if a valid (non null, not empty) table name is supplied */
	fixODBCstring(szTableName, nTableNameLength);
	if (szTableName == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	if (nTableNameLength == 0) {
		/* HY090 = Invalid string */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	fixODBCstring(szSchemaName, nSchemaNameLength);

	/* construct the query now */
	query = malloc(1000 + nTableNameLength + nSchemaNameLength);
	assert(query);
	query_end = query;

	/* TODO: finish the SQL query */
	strcpy(query_end,
	       "SELECT '' AS TABLE_CAT, S.NAME AS TABLE_SCHEM, "
	       "T.NAME AS TABLE_NAME, 1 AS NON_UNIQUE, "
	       "NULL AS INDEX_QUALIFIER, NULL AS INDEX_NAME, 0 AS TYPE, "
	       "NULL AS ORDINAL_POSITION, C.COLUMN_NAME AS COLUMN_NAME, "
	       "'A' AS ASC_OR_DESC, NULL AS CARDINALITY, NULL AS PAGES, "
	       "NULL AS FILTER_CONDITION FROM SCHEMAS S, TABLES T, COLUMNS C "
	       "WHERE S.ID = T.SCHEMA_ID AND T.ID = C.TABLE_ID AND "
	       "T.ID = K.TABLE_ID");
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
	strcpy(query_end,
	       " ORDER BY NON_UNIQUE, TYPE, INDEX_QUALLIFIER, INDEX_NAME, "
	       "ORDINAL_POSITION");
	query_end += strlen(query_end);

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect(hStmt, (SQLCHAR *) query,
			   (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
