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
#include "ODBCUtil.h"


static SQLRETURN
SQLDescribeCol_(ODBCStmt *stmt, SQLUSMALLINT nCol, SQLCHAR *szColName,
		SQLSMALLINT nColNameMax, SQLSMALLINT *pnColNameLength,
		SQLSMALLINT *pnSQLDataType, SQLUINTEGER *pnColSize,
		SQLSMALLINT *pnDecDigits, SQLSMALLINT *pnNullable)
{
	ODBCDescRec *rec = NULL;

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	/* and it should return a result set */
	if (stmt->ImplRowDescr->sql_desc_count == 0) {
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

	/* now copy the data */
	copyString(rec->sql_desc_name,
		   szColName, nColNameMax, pnColNameLength,
		   addStmtError, stmt);

	if (pnSQLDataType)
		*pnSQLDataType = rec->sql_desc_concise_type;

	/* also see SQLDescribeParam */
	if (pnColSize)
		*pnColSize = ODBCDisplaySize(rec);

	/* also see SQLDescribeParam */
	if (pnDecDigits) {
		switch (rec->sql_desc_concise_type) {
		case SQL_DECIMAL:
		case SQL_NUMERIC:
			*pnDecDigits = rec->sql_desc_scale;
			break;
		case SQL_BIT:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
			*pnDecDigits = 0;
			break;
		case SQL_TYPE_TIME:
		case SQL_TYPE_TIMESTAMP:
		case SQL_INTERVAL_SECOND:
		case SQL_INTERVAL_DAY_TO_SECOND:
		case SQL_INTERVAL_HOUR_TO_SECOND:
		case SQL_INTERVAL_MINUTE_TO_SECOND:
			*pnDecDigits = rec->sql_desc_precision;
			break;
		}
	}

	if (pnNullable)
		*pnNullable = rec->sql_desc_nullable;

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLDescribeCol(SQLHSTMT hStmt, SQLUSMALLINT nCol, SQLCHAR *szColName,
	       SQLSMALLINT nColNameMax, SQLSMALLINT *pnColNameLength,
	       SQLSMALLINT *pnSQLDataType, SQLUINTEGER *pnColSize,
	       SQLSMALLINT *pnDecDigits, SQLSMALLINT *pnNullable)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeCol\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLDescribeCol_(stmt, nCol, szColName, nColNameMax,
			       pnColNameLength, pnSQLDataType, pnColSize,
			       pnDecDigits, pnNullable);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLDescribeColW(SQLHSTMT hStmt, SQLUSMALLINT nCol, SQLWCHAR *szColName,
		SQLSMALLINT nColNameMax, SQLSMALLINT *pnColNameLength,
		SQLSMALLINT *pnSQLDataType, SQLUINTEGER *pnColSize,
		SQLSMALLINT *pnDecDigits, SQLSMALLINT *pnNullable)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLCHAR *colname;
	SQLSMALLINT n;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeColW\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	prepWcharOut(colname, nColNameMax);

	rc = SQLDescribeCol_(stmt, nCol, colname, nColNameMax * 4,
			     &n, pnSQLDataType, pnColSize,
			     pnDecDigits, pnNullable);

	fixWcharOut(rc, colname, n, szColName, nColNameMax, pnColNameLength, addStmtError, stmt);

	return rc;
}
#endif	/* WITH_WCHAR */
