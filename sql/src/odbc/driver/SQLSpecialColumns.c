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
 * SQLSpecialColumns()
 * CLI Compliance: X/Open
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


SQLRETURN SQLSpecialColumns(
	SQLHSTMT	hStmt,
	SQLUSMALLINT	nIdentifierType,
	SQLCHAR *	szCatalogName,
	SQLSMALLINT	nCatalogNameLength,
	SQLCHAR *	szSchemaName,
	SQLSMALLINT	nSchemaNameLength,
	SQLCHAR *	szTableName,
	SQLSMALLINT	nTableNameLength,
	SQLUSMALLINT	nScope,
	SQLUSMALLINT	nNullable )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	char *	schName = NULL;
	char *	tabName = NULL;
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

	/* check for valid IdentifierType argument */
	switch (nIdentifierType) {
		case SQL_BEST_ROWID:
		case SQL_ROWVER:
			break;
		default:
			/* HY097 = Invalid identifier type */
			addStmtError(stmt, "HY097", NULL, 0);
			return SQL_ERROR;
	}

	/* check for valid Scope argument */
	switch (nScope) {
		case SQL_SCOPE_CURROW:
		case SQL_SCOPE_TRANSACTION:
		case SQL_SCOPE_SESSION:
			break;
		default:
			/* HY098 = Invalid scope type */
			addStmtError(stmt, "HY098", NULL, 0);
			return SQL_ERROR;
	}

	/* check for valid Nullable argument */
	switch (nNullable) {
		case SQL_NO_NULLS:
		case SQL_NULLABLE:
			break;
		default:
			/* HY099 = Invalid nullable type */
			addStmtError(stmt, "HY099", NULL, 0);
			return SQL_ERROR;
	}

	/* check if a valid (non null, not empty) table name is supplied */
	tabName = copyODBCstr2Cstr(szTableName, nTableNameLength);
	if (tabName == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	assert(tabName);
	if (strcmp(tabName, "") == 0) {
		free(tabName);
		/* HY090 = Invalid string */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	/* convert input string parameters to normal null terminated C strings */
	schName = copyODBCstr2Cstr(szSchemaName, nSchemaNameLength);


	/* first create a string buffer for the selection condition */
	work_str_len += strlen(tabName);
	if (schName != NULL) {
		work_str_len += strlen(schName);
	}
	work_str = malloc(work_str_len);
	assert(work_str);
	strcpy(work_str, "");	/* initialize it */


	/* Construct the selection condition query part */
	/* search pattern is not allowed for table name so use = and not LIKE */
	strcat(work_str, " AND T.TABLE_NAME = '");
	strcat(work_str, tabName);
	strcat(work_str, "'");

	if (schName != NULL && (strcmp(schName, "") != 0)) {
		/* filtering requested on schema name */
		/* search pattern is not allowed so use = and not LIKE */
		strcat(work_str, " AND S.NAME = '");
		strcat(work_str, schName);
		strcat(work_str, "'");
	}


	/* construct the query now */
	query = malloc(1000 + strlen(work_str));
	assert(query);

	if (nIdentifierType == SQL_BEST_ROWID) {
		/* Select from the key table the (smallest) primary/unique key */
		/* Note: SCOPE is SQL_SCOPE_TRANSACTION is 1 */
		/* Note: PSEUDO_COLUMN is SQL_PC_NOT_PSEUDO is 1 */
		strcpy(query, "SELECT 1 AS SCOPE, C.COLUMN_NAME AS COLUMN_NAME, C.DATA_TYPE AS DATA_TYPE, C.TYPE_NAME AS TYPE_NAME, C.COLUMN_SIZE AS COLUMN_SIZE, C.BUFFER_LENGTH AS BUFFER_LENGTH, C.DECIMAL_DIGITS AS DECIMAL_DIGITS, 1 AS PSEUDO_COLUMN FROM SCHEMAS S, TABLES T, COLUMNS C, KEYS K, KEYCOLUMNS KC WHERE S.ID = T.SCHEMA_ID AND T.ID = C.TABLE_ID AND T.ID = K.TABLE_ID AND C.ID = KC.COLUMN_ID AND KC.KEY_ID = K.KEY_ID AND K.IS_PRIMARY = 1");
		/* TODO: improve the SQL to get the correct result:
		   - only one set of columns should be returned, also when
		     multiple primary keys are available for this table.
		   - when the table has NO primary key it should return the
		     columns of a unique key (only from ONE unique key which
		     is also the best/smallest key)
		   TODO: optimize SQL:
		   - when no schName is set (see above) also no filtering on
		     SCHEMA NAME and join with table SCHEMAS is needed!
		 */

		/* add the selection condition */
		strcat(query, work_str);

		/* add an extra selection when SQL_NO_NULLS is requested */
		if (nNullable == SQL_NO_NULLS) {
			strcat(query, " AND C.IS_NULLABLE = 0");
		}

		/* no ordering needed */
	} else {
		assert(nIdentifierType == SQL_ROWVER);
		/* The backend does not have such info available */
		/* create just a query which results in zero rows */
		/* Note: PSEUDO_COLUMN is SQL_PC_UNKNOWN is 0 */
		strcpy(query, "SELECT NULL AS SCOPE, '' AS COLUMN_NAME, 1 AS DATA_TYPE, 'CHAR' AS TYPE_NAME, 1 AS COLUMN_SIZE, 1 AS BUFFER_LENGTH, 0 AS DECIMAL_DIGITS, 0 AS PSEUDO_COLUMN FROM SCHEMAS S WHERE 0 = 1");
	}

	free(work_str);

	/* Done with parameter values evaluation. Now free the C strings. */
	if (schName != NULL) {
		free(schName);
	}
	if (tabName != NULL) {
		free(tabName);
	}

	/* query the MonetDb data dictionary tables */
	assert(query);
	rc = ExecDirect(hStmt, query, SQL_NTS);

	free(query);

	return rc;
}
