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


SQLRETURN SQLColumns(
	SQLHSTMT	hStmt,
	SQLCHAR *	szCatalogName,
	SQLSMALLINT	nCatalogNameLength,
	SQLCHAR *	szSchemaName,
	SQLSMALLINT	nSchemaNameLength,
	SQLCHAR *	szTableName,
	SQLSMALLINT	nTableNameLength,
	SQLCHAR *	szColumnName,
	SQLSMALLINT	nColumnNameLength )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	char *	schName = NULL;
	char *	tabName = NULL;
	char *	colName = NULL;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char * query = NULL;
	char * work_str = NULL;
	int work_str_len = 1000;

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
	schName = copyODBCstr2Cstr(szSchemaName, nSchemaNameLength);
	tabName = copyODBCstr2Cstr(szTableName, nTableNameLength);
	colName = copyODBCstr2Cstr(szColumnName, nColumnNameLength);

	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	/* first create a string buffer */
	if (schName != NULL) {
		work_str_len += strlen(schName);
	}
	if (tabName != NULL) {
		work_str_len += strlen(tabName);
	}
	if (colName != NULL) {
		work_str_len += strlen(colName);
	}
	work_str = malloc(work_str_len);
	assert(work_str);
	strcpy(work_str, "");	/* initialize it */


	/* Construct the selection condition query part */
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

	if (colName != NULL && (strcmp(colName, "") != 0)) {
		/* filtering requested on column name */
		strcat(work_str, " AND C.NAME ");

		/* use LIKE when it contains a wildcard '%' or a '_' */
		if (strchr(colName, '%') || strchr(colName, '_')) {
			/* TODO: the wildcard may be escaped.
			   Check it and may be convert it. */
			strcat(work_str, "LIKE '");
		} else {
			strcat(work_str, "= '");
		}
		strcat(work_str, colName);
		strcat(work_str, "'");
	}


	/* construct the query now */
	query = malloc(1000 + strlen(work_str));
	assert(query);

	strcpy(query, "SELECT '' AS TABLE_CAT, S.NAME AS TABLE_SCHEM, T.NAME AS TABLE_NAME, C.NAME AS COLUMN_NAME, C.TYPE AS DATA_TYPE, C.TYPE AS TYPE_NAME, C.TYPE_DIGITS AS COLUMN_SIZE, C.TYPE_DIGITS AS BUFFER_LENGTH, C.TYPE_SCALE AS DECIMAL_DIGITS, '' AS NUM_PREC_RADIX, C.NULL AS NULLABLE, '' AS REMARKS, '' AS COLUMN_DEF, C.TYPE AS SQL_DATA_TYPE, '' AS SQL_DATETIME_SUB, '' AS CHAR_OCTET_LENGTH, C.NUMBER AS ORDINAL_POSITION, C.NULL AS IS_NULLABLE FROM SCHEMAS S, TABLES T, COLUMNS C WHERE S.ID = T.SCHEMA_ID AND T.ID = C.TABLE_ID");

	/* add the selection condition */
	strcat(query, work_str);

	/* add the ordering */
	strcat(query, " ORDER BY S.NAME, T.NAME, C.NUMBER");
	free(work_str);

	/* Done with parameter values evaluation. Now free the C strings. */
	if (schName != NULL) {
		free(schName);
	}
	if (tabName != NULL) {
		free(tabName);
	}
	if (colName != NULL) {
		free(colName);
	}

	/* query the MonetDb data dictionary tables */
	assert(query);
	rc = ExecDirect(hStmt, query, SQL_NTS);

	free(query);

	return rc;
}
