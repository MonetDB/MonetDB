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


SQLRETURN SQLForeignKeys(
	SQLHSTMT	hStmt,
	SQLCHAR *	szPKCatalogName,
	SQLSMALLINT	nPKCatalogNameLength,
	SQLCHAR *	szPKSchemaName,
	SQLSMALLINT	nPKSchemaNameLength,
	SQLCHAR *	szPKTableName,
	SQLSMALLINT	nPKTableNameLength,
	SQLCHAR *	szFKCatalogName,
	SQLSMALLINT	nFKCatalogNameLength,
	SQLCHAR *	szFKSchemaName,
	SQLSMALLINT	nFKSchemaNameLength,
	SQLCHAR *	szFKTableName,
	SQLSMALLINT	nFKTableNameLength )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	char *	pkschName = NULL;
	char *	pktabName = NULL;
	char *	fkschName = NULL;
	char *	fktabName = NULL;
	RETCODE rc;

	/* buffer for the constructed query to do meta data retrieval */
	char * query = NULL;
	char * work_str = NULL;
	int work_str_len = 1000;

	(void) szPKCatalogName;	/* Stefan: unused!? */
	(void) nPKCatalogNameLength;	/* Stefan: unused!? */
	(void) szFKCatalogName;	/* Stefan: unused!? */
	(void) nFKCatalogNameLength;	/* Stefan: unused!? */

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
	pkschName = copyODBCstr2Cstr(szPKSchemaName, nPKSchemaNameLength);
	pktabName = copyODBCstr2Cstr(szPKTableName, nPKTableNameLength);
	fkschName = copyODBCstr2Cstr(szFKSchemaName, nFKSchemaNameLength);
	fktabName = copyODBCstr2Cstr(szFKTableName, nFKTableNameLength);

	/* dependent on the input parameter values we must add a
	   variable selection condition dynamically */

	/* first create a string buffer */
	if (pkschName != NULL) {
		work_str_len += strlen(pkschName);
	}
	if (pktabName != NULL) {
		work_str_len += strlen(pktabName);
	}
	if (fkschName != NULL) {
		work_str_len += strlen(fkschName);
	}
	if (fktabName != NULL) {
		work_str_len += strlen(fktabName);
	}
	work_str = malloc(work_str_len);
	assert(work_str);
	strcpy(work_str, "");	/* initialize it */


	/* Construct the selection condition query part */
	if (pkschName != NULL && (strcmp(pkschName, "") != 0)) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		strcat(work_str, " AND S1.NAME = '");
		strcat(work_str, pkschName);
		strcat(work_str, "'");
	}

	if (pktabName != NULL && (strcmp(pktabName, "") != 0)) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		strcat(work_str, " AND T1.NAME = '");
		strcat(work_str, pktabName);
		strcat(work_str, "'");
	}

	if (fkschName != NULL && (strcmp(fkschName, "") != 0)) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		strcat(work_str, " AND S2.NAME = '");
		strcat(work_str, fkschName);
		strcat(work_str, "'");
	}

	if (fktabName != NULL && (strcmp(fktabName, "") != 0)) {
		/* filtering requested on table name */
		/* search pattern is not allowed so use = and not LIKE */
		strcat(work_str, " AND T2.NAME = '");
		strcat(work_str, fktabName);
		strcat(work_str, "'");
	}


	/* construct the query now */
	query = malloc(1000 + strlen(work_str));
	assert(query);

	strcpy(query, "SELECT '' AS PKTABLE_CAT, S1.NAME AS PKTABLE_SCHEM, T1.NAME AS PKTABLE_NAME, C1.NAME AS PKCOLUMN_NAME, '' AS FKTABLE_CAT, S1.NAME AS FKTABLE_SCHEM, T1.NAME AS FKTABLE_NAME, C1.NAME AS FKCOLUMN_NAME, KC.ORDINAL_POSITION AS KEY_SEQ, K.UPDATE_RULE AS UPDATE_RULE, K.DELETE_RULE AS DELETE_RULE, K.FK_NAME AS FK_NAME, K.PK_NAME AS PK_NAME, K.DEFERRABILITY AS DEFERRABILITY \
 FROM SCHEMAS S, TABLES T, _COLUMNS C WHERE S.ID = T.SCHEMA_ID AND T.ID = C.TABLE_ID");
/* TODO finish the FROM en WHERE clauses */

	/* add the selection condition */
	strcat(query, work_str);

	/* add the ordering */
	if (pktabName != NULL) {	/* selection on primary key */
		/* order on FK output columns */
		strcat(query, " ORDER BY S2.NAME, T2.NAME, KC.ORDINAL_POSITION");
	} else {
		/* order on PK output columns */
		strcat(query, " ORDER BY S1.NAME, T1.NAME, KC.ORDINAL_POSITION");
	}
	free(work_str);

	/* Done with parameter values evaluation. Now free the C strings. */
	if (pkschName != NULL) {
		free(pkschName);
	}
	if (pktabName != NULL) {
		free(pktabName);
	}
	if (fkschName != NULL) {
		free(fkschName);
	}
	if (fktabName != NULL) {
		free(fktabName);
	}

	/* query the MonetDb data dictionary tables */
	assert(query);
	rc = SQLExecDirect(hStmt, (SQLCHAR*)query, SQL_NTS);

	free(query);

	return rc;
}
