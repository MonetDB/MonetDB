/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
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


SQLRETURN SQLTables(
	SQLHSTMT	hStmt,
	SQLCHAR *	szCatalogName,
	SQLSMALLINT	nCatalogNameLength,
	SQLCHAR *	szSchemaName,
	SQLSMALLINT	nSchemaNameLength,
	SQLCHAR *	szTableName,
	SQLSMALLINT	nTableNameLength,
	SQLCHAR *	szTableType,
	SQLSMALLINT	nTableTypeLength )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	char *	catName = NULL;
	char *	schName = NULL;
	char *	tabName = NULL;
	char *	typName = NULL;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char * query = NULL;


	if (! isValidStmt(stmt))
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
	catName = copyODBCstr2Cstr(szCatalogName, nCatalogNameLength);
	schName = copyODBCstr2Cstr(szSchemaName, nSchemaNameLength);
	tabName = copyODBCstr2Cstr(szTableName, nTableNameLength);
	typName = copyODBCstr2Cstr(szTableType, nTableTypeLength);

	/* Check first on the special cases */
	if (catName != NULL && (strcmp(catName, SQL_ALL_CATALOGS) == 0) &&
	    schName != NULL && (strcmp(schName, "") == 0) &&
	    tabName != NULL && (strcmp(tabName, "") == 0) )
	{
		/* Special case query to fetch all Catalog names. */
		/* Note: Catalogs are not supported so the result set will be empty. */
		query = strdup("SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, '' AS TABLE_TYPE, '' AS REMARKS FROM SCHEMAS WHERE 0 = 1");
	} else {
	if (catName != NULL && (strcmp(catName, "") == 0) &&
	    schName != NULL && (strcmp(schName, SQL_ALL_SCHEMAS) == 0) &&
	    tabName != NULL && (strcmp(tabName, "") == 0) )
	{
		/* Special case query to fetch all Schema names. */
		query = strdup("SELECT '' AS TABLE_CAT, NAME AS TABLE_SCHEM, '' AS TABLE_NAME, '' AS TABLE_TYPE, '' AS REMARKS FROM SCHEMAS ORDER BY NAME");
	} else {
	if (catName != NULL && (strcmp(catName, "") == 0) &&
	    schName != NULL && (strcmp(schName, "") == 0) &&
	    tabName != NULL && (strcmp(tabName, "") == 0) &&
	    typName != NULL && (strcmp(typName, SQL_ALL_TABLE_TYPES) == 0) )
	{
		/* Special case query to fetch all Table type names. */
		query = strdup("SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, DISTINCT CASE T.TYPE WHEN 1 THEN 'TABLE' WHEN 0 THEN 'SYSTEM_TABLE' WHEN 2 THEN 'VIEW' WHEN 3 THEN 'LOCAL TEMPORARY TABLE' ELSE 'INTERNAL TYPE' END AS TABLE_TYPE, '' AS REMARKS FROM TABLES ORDER BY TYPE");
		/* TODO: UNION it with all supported table types */
	}
	}
	}

	/* check if we had special case argument values */
	if (query == NULL)
	{
		/* dependent on the input parameter values we must add a
		   variable selection condition dynamically */

		/* first create a string buffer */
		char * work_str = NULL;	/* buffer pointer */
		int work_str_len = 1000;
		
		if (catName != NULL) {
			work_str_len += strlen(catName);
		}
		if (schName != NULL) {
			work_str_len += strlen(schName);
		}
		if (tabName != NULL) {
			work_str_len += strlen(tabName);
		}
		if (typName != NULL) {
			work_str_len += strlen(typName);
		}
		work_str = malloc(work_str_len);
		assert(work_str);
		strcpy(work_str, "");	/* initialize it */


		/* Construct the selection condition query part */
		if (catName != NULL && (strcmp(catName, "") != 0)) {
			/* filtering requested on catalog name */
			/* we do not support catalog names, so ignore it */
		}

		if (schName != NULL && (strcmp(schName, "") != 0)) {
			/* filtering requested on schema name */
			strcat(work_str, " AND S.NAME ");

			/* use LIKE when it contains a wildcard '%' or a '_' */
			if (strchr(schName, '%') || strchr(schName, '_')) {
				/* TODO: the wildcard may be escaped.
				   Check it and may be convert it. */
				strcat(work_str, "LIKE '");
			} else {
				strcat(work_str, "= '");
			}
			strcat(work_str, schName);
			strcat(work_str, "'");
		}

		if (tabName != NULL && (strcmp(tabName, "") != 0)) {
			/* filtering requested on table name */
			strcat(work_str, " AND T.NAME ");

			/* use LIKE when it contains a wildcard '%' or a '_' */
			if (strchr(tabName, '%') || strchr(tabName, '_')) {
				/* TODO: the wildcard may be escaped.
				   Check it and may be convert it. */
				strcat(work_str, "LIKE '");
			} else {
				strcat(work_str, "= '");
			}
			strcat(work_str, tabName);
			strcat(work_str, "'");
		}

		if (typName != NULL && (strcmp(typName, "") != 0)) {
			/* filtering requested on table type */
			/* TODO: decompose the string typName into separate string values (separated by comma).
				Mapped these string values to type numbers (see enum table_type in catalog.h).
				The type numbers need to be inserted in the SQL:
				 " AND T.TYPE IN (<insert the list of numbers here>)"
				where the comma separated numbers are placed in the <insert ...here> part.
				Note: when there is no single type number mapped use -1 so the SQL becomes:
				 " AND T.TYPE IN (-1)"
				This way no records will be returned and the result set will be empty.
			 */
		}

		/* construct the query now */
		query = malloc(1000 + strlen(work_str));
		assert(query);

		snprintf(query, 1000+ strlen(work_str), "SELECT '%s' AS TABLE_CAT, S.NAME AS TABLE_SCHEM, T.NAME AS TABLE_NAME, CASE T.TYPE WHEN 0 THEN 'TABLE' WHEN 1 THEN 'SYSTEM_TABLE' WHEN 2 THEN 'VIEW' WHEN 3 THEN 'LOCAL TEMPORARY TABLE' ELSE 'INTERNAL TABLE TYPE' END AS TABLE_TYPE, '' AS REMARKS FROM SCHEMAS S, TABLES T WHERE S.ID = T.SCHEMA_ID ", catName);

		/* add the selection condition */
		strcat(query, work_str);

		/* add the ordering */
		strcat(query, " ORDER BY S.NAME, T.NAME, T.TYPE");
		free(work_str);
	}

	/* Done with parameter values evaluation. Now free the C strings. */
	if (catName != NULL) {
		free(catName);
	}
	if (schName != NULL) {
		free(schName);
	}
	if (tabName != NULL) {
		free(tabName);
	}
	if (typName != NULL) {
		free(typName);
	}

	/* query the MonetDb data dictionary tables */
	assert(query);

	rc = SQLExecDirect(hStmt, query, SQL_NTS);

	free(query);

	return rc;
}
