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
 * SQLTables()
 * CLI Compliance: X/Open
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


SQLRETURN
SQLTables(SQLHSTMT hStmt, SQLCHAR *szCatalogName,
	  SQLSMALLINT nCatalogNameLength, SQLCHAR *szSchemaName,
	  SQLSMALLINT nSchemaNameLength, SQLCHAR *szTableName,
	  SQLSMALLINT nTableNameLength, SQLCHAR *szTableType,
	  SQLSMALLINT nTableTypeLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char *query = NULL;

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

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(szCatalogName, nCatalogNameLength);
	fixODBCstring(szSchemaName, nSchemaNameLength);
	fixODBCstring(szTableName, nTableNameLength);
	fixODBCstring(szTableType, nTableTypeLength);

	/* Check first on the special cases */
	if (szCatalogName && strcmp(szCatalogName, SQL_ALL_CATALOGS) == 0 &&
	    szSchemaName && nSchemaNameLength == 0 &&
	    szTableName && nTableNameLength == 0) {
		/* Special case query to fetch all Catalog names. */
		/* Note: Catalogs are not supported so the result set will be empty. */
		query = strdup("SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, "
			       "'' AS TABLE_NAME, '' AS TABLE_TYPE, "
			       "'' AS REMARKS FROM SCHEMAS WHERE 0 = 1");
	} else if (szCatalogName && nCatalogNameLength == 0 &&
		   szSchemaName && strcmp(szSchemaName, SQL_ALL_SCHEMAS) == 0 &&
		   szTableName && nTableNameLength == 0) {
		/* Special case query to fetch all Schema names. */
		query = strdup("SELECT '' AS TABLE_CAT, NAME AS TABLE_SCHEM, "
			       "'' AS TABLE_NAME, '' AS TABLE_TYPE, "
			       "'' AS REMARKS FROM SCHEMAS ORDER BY NAME");
	} else if (szCatalogName && nCatalogNameLength == 0 &&
		   szSchemaName && nSchemaNameLength == 0 &&
		   szTableName && nTableNameLength == 0 &&
		   szTableType && strcmp(szTableType, SQL_ALL_TABLE_TYPES) == 0) {
		/* Special case query to fetch all Table type names. */
		query = strdup("SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, "
			       "'' AS TABLE_NAME, "
			       "DISTINCT CASE T.TYPE WHEN 1 THEN 'TABLE' "
			       "WHEN 0 THEN 'SYSTEM_TABLE' WHEN 2 THEN 'VIEW' "
			       "WHEN 3 THEN 'LOCAL TEMPORARY TABLE' "
			       "ELSE 'INTERNAL TYPE' END AS TABLE_TYPE, "
			       "'' AS REMARKS FROM TABLES ORDER BY TYPE");
		/* TODO: UNION it with all supported table types */
	} else {
		/* no special case argument values */
		char *query_end;

		/* construct the query now */
		query = malloc(1000 + nCatalogNameLength + nSchemaNameLength +
			       nTableNameLength + nTableTypeLength);
		assert(query);
		query_end = query;

		sprintf(query_end,
			"SELECT '%.*s' AS TABLE_CAT, S.NAME AS TABLE_SCHEM, "
			"T.NAME AS TABLE_NAME, "
			"CASE T.TYPE WHEN 0 THEN 'TABLE' "
			"WHEN 1 THEN 'SYSTEM_TABLE' WHEN 2 THEN 'VIEW' "
			"WHEN 3 THEN 'LOCAL TEMPORARY TABLE' "
			"ELSE 'INTERNAL TABLE TYPE' END AS TABLE_TYPE, "
			"'' AS REMARKS FROM SCHEMAS S, TABLES T "
			"WHERE S.ID = T.SCHEMA_ID",
			nCatalogNameLength, szCatalogName);
		query_end += strlen(query_end);

		/* dependent on the input parameter values we must add a
		   variable selection condition dynamically */

		/* Construct the selection condition query part */
		if (nCatalogNameLength > 0) {
			/* filtering requested on catalog name */
			/* we do not support catalog names, so ignore it */
		}

		if (nSchemaNameLength > 0) {
			/* filtering requested on schema name */
			/* use LIKE when it contains a wildcard '%' or a '_' */
			/* TODO: the wildcard may be escaped. Check it
			   and maybe convert it. */
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
			/* TODO: the wildcard may be escaped.  Check
			   it and may be convert it. */
			sprintf(query_end, " AND T.NAME %s '%.*s'",
				memchr(szTableName, '%', nTableNameLength) ||
				memchr(szTableName, '_', nTableNameLength) ?
				"LIKE" : "=",
				nTableNameLength, szTableName);
			query_end += strlen(query_end);
		}

		if (nTableTypeLength > 0) {
			/* filtering requested on table type */
			/* TODO: decompose the string szTableType into
			   separate string values (separated by
			   comma).
			   Mapped these string values to type numbers
			   (see enum table_type in catalog.h).  The
			   type numbers need to be inserted in the
			   SQL: " AND T.TYPE IN (<insert the list of
			   numbers here>)" where the comma separated
			   numbers are placed in the <insert ...here>
			   part.  Note: when there is no single type
			   number mapped use -1 so the SQL becomes: "
			   AND T.TYPE IN (-1)" This way no records
			   will be returned and the result set will be
			   empty.
			 */
		}

		/* add the ordering */
		strcpy(query_end, " ORDER BY S.NAME, T.NAME, T.TYPE");
	}

	/* query the MonetDb data dictionary tables */

	rc = SQLExecDirect(hStmt, (SQLCHAR *) query, SQL_NTS);

	free(query);

	return rc;
}
