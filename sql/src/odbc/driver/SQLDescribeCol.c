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
	ODBCDescRec *rec = NULL;
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
	if (stmt->ImplRowDescr->descRec == NULL) {
		/* 07005 = Prepared statement not a cursor specification */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}
	if (nCol < 1 || nCol > stmt->ImplRowDescr->sql_desc_count) {
		/* 07009 = Invalid descriptor index */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}

	/* OK */
	rec = stmt->ImplRowDescr->descRec + nCol;

	if (rec->sql_desc_name)
		colNameLen = strlen((char *) rec->sql_desc_name);

	/* now copy the data */
	if (szColName && rec->sql_desc_name) {
		strncpy((char *) szColName, (char *) rec->sql_desc_name, nColNameMax - 1);
		szColName[nColNameMax - 1] = 0; /* null terminate it */
	}
	if (pnColNameLength)
		*pnColNameLength = rec->sql_desc_name ? colNameLen : SQL_NULL_DATA;

	if (pnSQLDataType)
		*pnSQLDataType = rec->sql_desc_type;

	if (pnColSize)
		*pnColSize = rec->sql_desc_length;

	if (pnDecDigits)
		*pnDecDigits = rec->sql_desc_scale;

	if (pnNullable)
		*pnNullable = rec->sql_desc_nullable;

	if (colNameLen >= nColNameMax) {
		/* 01004 = String data, right truncation */
		addStmtError(stmt, "01004", NULL, 0);
		return SQL_SUCCESS_WITH_INFO;
	}
	return SQL_SUCCESS;
}
