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
		query = GDKstrdup("SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, '' AS TABLE_TYPE, '' AS REMARKS FROM SQL_SCHEMA WHERE 0 = 1");
	} else {
	if (catName != NULL && (strcmp(catName, "") == 0) &&
	    schName != NULL && (strcmp(schName, SQL_ALL_SCHEMAS) == 0) &&
	    tabName != NULL && (strcmp(tabName, "") == 0) )
	{
		/* Special case query to fetch all Schema names. */
		query = GDKstrdup("SELECT '' AS TABLE_CAT, SCHEMA_NAME AS TABLE_SCHEM, '' AS TABLE_NAME, '' AS TABLE_TYPE, REMARKS FROM SQL_SCHEMA ORDER BY SCHEMA_NAME");
	} else {
	if (catName != NULL && (strcmp(catName, "") == 0) &&
	    schName != NULL && (strcmp(schName, "") == 0) &&
	    tabName != NULL && (strcmp(tabName, "") == 0) &&
	    typName != NULL && (strcmp(typName, SQL_ALL_TABLE_TYPES) == 0) )
	{
		/* Special case query to fetch all Table type names. */
		query = GDKstrdup("SELECT '' AS TABLE_CAT, '' AS TABLE_SCHEM, '' AS TABLE_NAME, DISTINCT TABLE_TYPE, '' AS REMARKS FROM SQL_TABLE ORDER BY TABLE_TYPE");
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
		work_str = GDKmalloc(work_str_len);
		assert(work_str);
		strcpy(work_str, "");	/* initialize it */


		/* Construct the selection condition query part */
		if (catName != NULL && (strcmp(catName, "") != 0)) {
			/* filtering requested on catalog name */
			/* we do not support catalog names, so ignore it */
		}

		if (schName != NULL && (strcmp(schName, "") != 0)) {
			/* filtering requested on schema name */
			strcat(work_str, " AND S.SCHEMA_NAME ");

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
			strcat(work_str, " AND T.TABLE_NAME ");

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
			/* TODO: decompose the string typName into separate
				values which can be inserted in the SQL:
				" AND T.TABLE_TYPE IN ('type_1', 'type_n')"
			 */
		}

		/* construct the query now */
		query = GDKmalloc(1000 + strlen(work_str));
		assert(query);

		strcpy(query, "SELECT '' AS TABLE_CAT, S.SCHEMA_NAME AS TABLE_SCHEM, T.TABLE_NAME AS TABLE_NAME, T.TABLE_TYPE AS TABLE_TYPE, T.REMARKS FROM SQL_SCHEMA S, SQL_TABLE T WHERE T.SCHEMA_ID = S.SCHEMA_ID ");

		/* add the selection condition */
		strcat(query, work_str);

		/* add the ordering */
		strcat(query, " ORDER BY S.SCHEMA_NAME, T.TABLE_NAME, T.TABLE_TYPE");
		GDKfree(work_str);
	}

	/* Done with parameter values evaluation. Now free the C strings. */
	if (catName != NULL) {
		GDKfree(catName);
	}
	if (schName != NULL) {
		GDKfree(schName);
	}
	if (tabName != NULL) {
		GDKfree(tabName);
	}
	if (typName != NULL) {
		GDKfree(typName);
	}

	/* query the MonetDb data dictionary tables */
	assert(query);
	rc = SQLExecDirect(hStmt, query, SQL_NTS);

	GDKfree(query);

	return rc;
}
