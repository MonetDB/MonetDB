/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
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
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************************************
 * SQLDescribeCol()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLDescribeCol(
	SQLHSTMT	hStmt,
	SQLUSMALLINT	nCol,
	SQLCHAR *	szColName,
	SQLSMALLINT	nColNameMax,
	SQLSMALLINT *	pnColNameLength,
	SQLSMALLINT *	pnSQLDataType,
	SQLUINTEGER *	pnColSize,
	SQLSMALLINT *	pnDecDigits,
	SQLSMALLINT *	pnNullable )
{
	ODBCStmt * stmt = (ODBCStmt *) hStmt;
	ColumnHeader *	pColumnHeader = NULL;
	char *	colName = NULL;
	int	colNameLen = 0;


	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	/* and it should return a result set */
	if (stmt->Result == NULL) {
		/* 07005 = Prepared statement not a cursor specification */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}
	if (nCol < 1 || nCol > stmt->nrCols)
	{
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}

	/* OK */
	assert(stmt->Result != NULL);
	pColumnHeader = (ColumnHeader *)(stmt->Result)[nCol];
	if (pColumnHeader == NULL)
	{
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}

	colName = pColumnHeader->pszSQL_DESC_NAME;
	if (colName) {
		colNameLen = strlen(colName);
	}

	/* now copy the data */
	if (szColName) {
		if (colName) {
			strncpy(szColName, colName, nColNameMax -1);
			szColName[nColNameMax] = '\0';	/* make it null terminated */
		}
	}
	if (pnColNameLength) {
		*pnColNameLength = (colName) ? colNameLen : SQL_NULL_DATA;
	}
	if (pnSQLDataType) {
		*pnSQLDataType = pColumnHeader->nSQL_DESC_TYPE;
	}
	if (pnColSize) {
		*pnColSize = pColumnHeader->nSQL_DESC_LENGTH;
	}
	if (pnDecDigits) {
		*pnDecDigits = pColumnHeader->nSQL_DESC_SCALE;
	}
	if (pnNullable) {
		*pnNullable = pColumnHeader->nSQL_DESC_NULLABLE;
	}

	if (colNameLen >= nColNameMax) {
		/* 01004 = String data, right truncation */
		addStmtError(stmt, "01004", NULL, 0);
		return SQL_SUCCESS_WITH_INFO;
	}
	return SQL_SUCCESS;
}
