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
 * SQLDescribeCol()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLDescribeCol(SQLHSTMT hStmt, SQLUSMALLINT nCol, SQLCHAR *szColName,
	       SQLSMALLINT nColNameMax, SQLSMALLINT *pnColNameLength,
	       SQLSMALLINT *pnSQLDataType, SQLUINTEGER *pnColSize,
	       SQLSMALLINT *pnDecDigits, SQLSMALLINT *pnNullable)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	ColumnHeader *pColumnHeader = NULL;
	char *colName = NULL;
	int colNameLen = 0;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeCol\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	/* and it should return a result set */
	if (stmt->ResultCols == NULL) {
		/* 07005 = Prepared statement not a cursor specification */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}
	if (nCol < 1 || nCol > stmt->nrCols) {
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}

	/* OK */
	pColumnHeader = stmt->ResultCols + nCol;

	colName = pColumnHeader->pszSQL_DESC_NAME;
	if (colName)
		colNameLen = strlen(colName);

	/* now copy the data */
	if (szColName && colName) {
		strncpy((char *) szColName, colName, nColNameMax - 1);
		szColName[nColNameMax - 1] = 0; /* null terminate it */
	}
	if (pnColNameLength)
		*pnColNameLength = colName ? colNameLen : SQL_NULL_DATA;

	if (pnSQLDataType)
		*pnSQLDataType = pColumnHeader->nSQL_DESC_TYPE;

	if (pnColSize)
		*pnColSize = pColumnHeader->nSQL_DESC_LENGTH;

	if (pnDecDigits)
		*pnDecDigits = pColumnHeader->nSQL_DESC_SCALE;

	if (pnNullable)
		*pnNullable = pColumnHeader->nSQL_DESC_NULLABLE;

	if (colNameLen >= nColNameMax) {
		/* 01004 = String data, right truncation */
		addStmtError(stmt, "01004", NULL, 0);
		return SQL_SUCCESS_WITH_INFO;
	}
	return SQL_SUCCESS;
}
