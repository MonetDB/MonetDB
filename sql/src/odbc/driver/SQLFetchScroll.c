/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (thats about all I get out of it).
 * - Peter Harvey pharvey@codebydesign.com
 * 
 * This file has been modified for the MonetDB project.  See the file
 * Copyright in this directory for more information.
 */

/*****************************************************************************
 * SQLFetchScroll()
 * CLI Compliance: ISO 92
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 *****************************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLFetchScroll_(ODBCStmt *stmt, SQLSMALLINT FetchOrientation,
		SQLINTEGER FetchOffset)
{
	int LastResultRow;

	assert(stmt->hdl);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or
		   SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	if ((stmt->cursorType == SQL_CURSOR_FORWARD_ONLY ||
	     stmt->cursorScrollable == SQL_NONSCROLLABLE) &&
	    FetchOrientation != SQL_FETCH_NEXT) {
		/* Fetch type out of range */
		addStmtError(stmt, "HY106", NULL, 0);
		return SQL_ERROR;
	}

#define RowSetSize	(stmt->ApplRowDescr->sql_desc_array_size)
	LastResultRow = mapi_get_row_count(stmt->hdl);
	/* set currentRow to be the row number of the last row in the
	   result set */
	stmt->currentRow = stmt->startRow + stmt->rowSetSize;
	stmt->rowSetSize = 0;

	switch (FetchOrientation) {
	case SQL_FETCH_NEXT:
		if (stmt->currentRow >= LastResultRow)
			return SQL_NO_DATA;
		break;
	case SQL_FETCH_FIRST:
		stmt->startRow = 0;
		break;
	case SQL_FETCH_LAST:
		if (LastResultRow < RowSetSize)
			stmt->startRow = 0;
		else
			stmt->startRow = LastResultRow - RowSetSize;
		break;
	case SQL_FETCH_PRIOR:
		if (stmt->startRow == 0) {
			stmt->startRow = 0;
			return SQL_NO_DATA;
		}
		if (stmt->startRow < RowSetSize) {
			addStmtError(stmt, "01S06", NULL, 0);
			stmt->startRow = 0;
		} else
			stmt->startRow = stmt->startRow - RowSetSize;
		break;
	case SQL_FETCH_RELATIVE:
		if ((stmt->currentRow != 0 || FetchOffset <= 0) &&
		    (stmt->currentRow != LastResultRow || FetchOffset >= 0)) {
			if ((stmt->currentRow == 0 && FetchOffset <= 0) ||
			    (stmt->startRow == 0 && FetchOffset < 0) ||
			    (stmt->startRow > 0 &&
			     (int) stmt->startRow + FetchOffset < 0 &&
			     -FetchOffset > RowSetSize)) {
				stmt->startRow = 0;
				return SQL_NO_DATA;
			}
			if (stmt->startRow > 0 &&
			    (int) stmt->startRow + FetchOffset < 0) {
				stmt->startRow = 0;
				addStmtError(stmt, "01S06", NULL, 0);
				break;
			}
			if (stmt->startRow + FetchOffset >= LastResultRow ||
			    stmt->currentRow == LastResultRow) {
				stmt->startRow = LastResultRow;
				return SQL_NO_DATA;
			}
			stmt->startRow = stmt->startRow + FetchOffset;
			break;
		}
		/* fall through */
	case SQL_FETCH_ABSOLUTE:
		if (FetchOffset < 0) {
			if (-FetchOffset <= LastResultRow) {
				stmt->startRow = LastResultRow + FetchOffset;
				break;
			}
			stmt->startRow = 0;
			if (-FetchOffset > RowSetSize)
				return SQL_NO_DATA;
			addStmtError(stmt, "01S06", NULL, 0);
			break;
		}
		if (FetchOffset == 0) {
			stmt->startRow = 0;
			return SQL_NO_DATA;
		}
		if (FetchOffset > LastResultRow) {
			stmt->startRow = LastResultRow;
			return SQL_NO_DATA;
		}
		stmt->startRow = FetchOffset - 1;
		break;
	case SQL_FETCH_BOOKMARK:
		/* Optional feature not implemented */
		addStmtError(stmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	default:
		/* Fetch type out of range */
		addStmtError(stmt, "HY106", NULL, 0);
		return SQL_ERROR;
	}

	return SQLFetch_(stmt);
}

SQLRETURN
SQLFetchScroll(SQLHSTMT hStmt, SQLSMALLINT FetchOrientation,
	       SQLINTEGER FetchOffset)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLFetchScroll\n");
#endif

	if (!isValidStmt((ODBCStmt *) hStmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	return SQLFetchScroll_((ODBCStmt *) hStmt, FetchOrientation,
			       FetchOffset);
}
