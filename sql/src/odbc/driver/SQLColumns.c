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
 * Note: catalogs are not supported, we ignore any value set for szCatalogName
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


SQLRETURN
SQLColumns(SQLHSTMT hStmt, SQLCHAR *szCatalogName,
	   SQLSMALLINT nCatalogNameLength, SQLCHAR *szSchemaName,
	   SQLSMALLINT nSchemaNameLength, SQLCHAR *szTableName,
	   SQLSMALLINT nTableNameLength, SQLCHAR *szColumnName,
	   SQLSMALLINT nColumnNameLength)
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

	assert(stmt->Query == NULL);

	fixODBCstring(szSchemaName, nSchemaNameLength);
	fixODBCstring(szTableName, nTableNameLength);
	fixODBCstring(szColumnName, nColumnNameLength);

	/* construct the query now */
	query = malloc(1000 + nSchemaNameLength + nTableNameLength +
		       nColumnNameLength);
	assert(query);
	query_end = query;

	strcpy(query_end,
	       "SELECT '' AS TABLE_CAT, S.NAME AS TABLE_SCHEM, "
	       "T.NAME AS TABLE_NAME, C.NAME AS COLUMN_NAME, "
	       "C.TYPE AS DATA_TYPE, C.TYPE AS TYPE_NAME, "
	       "C.TYPE_DIGITS AS COLUMN_SIZE, C.TYPE_DIGITS AS BUFFER_LENGTH, "
	       "C.TYPE_SCALE AS DECIMAL_DIGITS, '' AS NUM_PREC_RADIX, "
	       "C.NULL AS NULLABLE, '' AS REMARKS, '' AS COLUMN_DEF, "
	       "C.TYPE AS SQL_DATA_TYPE, '' AS SQL_DATETIME_SUB, "
	       "'' AS CHAR_OCTET_LENGTH, C.NUMBER AS ORDINAL_POSITION, "
	       "C.NULL AS IS_NULLABLE FROM SCHEMAS S, TABLES T, COLUMNS C "
	       "WHERE S.ID = T.SCHEMA_ID AND T.ID = C.TABLE_ID");
	query_end += strlen(query_end);

	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	/* Construct the selection condition query part */
	if (nSchemaNameLength > 0) {
		/* filtering requested on schema name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped. Check it and may
		   be convert it. */
		sprintf(query_end, " AND S.NAME %s '%.*s'",
			memchr(szSchemaName, '%', nSchemaNameLength) ||
			memchr(szSchemaName, '_', nSchemaNameLength) ?
			"LIKE" : "=",
			nSchemaNameLength, szSchemaName);
		query_end += strlen(query_end);
	}

	if (nTableNameLength > 0) {
		/* filtering requested on table name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check it and
		   may be convert it. */
		sprintf(query_end, " AND T.NAME %s '%.*s'",
			memchr(szTableName, '%', nTableNameLength) ||
			memchr(szTableName, '_', nTableNameLength) ?
			"LIKE" : "=",
			nTableNameLength, szTableName);
		query_end += strlen(query_end);
	}

	if (nColumnNameLength > 0) {
		/* filtering requested on column name */
		/* use LIKE when it contains a wildcard '%' or a '_' */
		/* TODO: the wildcard may be escaped.  Check it and
		   may be convert it. */
		sprintf(query_end, " AND C.NAME %s '%.*s'",
			memchr(szColumnName, '%', nColumnNameLength) ||
			memchr(szColumnName, '_', nColumnNameLength) ?
			"LIKE" : "=",
			nColumnNameLength, szColumnName);
		query_end += strlen(query_end);
	}

	/* add the ordering */
	strcpy(query_end, " ORDER BY S.NAME, T.NAME, C.NUMBER");
	query_end += strlen(query_end);

	/* query the MonetDb data dictionary tables */
	rc = SQLExecDirect(hStmt, (SQLCHAR *) query,
			   (SQLINTEGER) (query_end - query));

	free(query);

	return rc;
}
