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

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	assert(stmt->hdl);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or
		   SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

#define RowSetSize	(stmt->ApplRowDescr->sql_desc_array_size)
	LastResultRow = mapi_get_row_count(stmt->hdl);

	switch (FetchOrientation) {
	case SQL_FETCH_NEXT:
		if (stmt->currentRow >= LastResultRow)
			return SQL_NO_DATA;
		break;
	case SQL_FETCH_FIRST:
		stmt->currentRow = 0;
		break;
	case SQL_FETCH_LAST:
		if (LastResultRow < RowSetSize)
			stmt->currentRow = 0;
		else
			stmt->currentRow = LastResultRow - RowSetSize;
		break;
	case SQL_FETCH_PRIOR:
		if (stmt->previousRow == 0) {
			stmt->currentRow = 0;
			return SQL_NO_DATA;
		}
		if (stmt->previousRow < RowSetSize) {
			addStmtError(stmt, "01S06", NULL, 0);
			stmt->currentRow = 0;
		}
		stmt->currentRow = stmt->previousRow - RowSetSize;
		break;
	case SQL_FETCH_RELATIVE:
		if ((stmt->currentRow != 0 || FetchOffset <= 0) &&
		    (stmt->currentRow != LastResultRow || FetchOffset >= 0)) {
			if ((stmt->currentRow == 0 && FetchOffset <= 0) ||
			    (stmt->previousRow == 0 && FetchOffset < 0) ||
			    (stmt->previousRow > 0 &&
			     (int) stmt->previousRow + FetchOffset < 0 &&
			     -FetchOffset > RowSetSize)) {
				stmt->currentRow = 0;
				return SQL_NO_DATA;
			}
			if (stmt->previousRow > 0 &&
			    (int) stmt->previousRow + FetchOffset < 0) {
				stmt->currentRow = 0;
				addStmtError(stmt, "01S06", NULL, 0);
				break;
			}
			if (stmt->previousRow + FetchOffset >= LastResultRow ||
			    stmt->currentRow == LastResultRow) {
				stmt->currentRow = LastResultRow;
				return SQL_NO_DATA;
			}
			stmt->currentRow = stmt->previousRow + FetchOffset;
			break;
		}
		/* fall through */
	case SQL_FETCH_ABSOLUTE:
		if (FetchOffset < 0) {
			if (-FetchOffset <= LastResultRow) {
				stmt->currentRow = LastResultRow + FetchOffset;
				break;
			}
			stmt->currentRow = 0;
			if (-FetchOffset > RowSetSize)
				return SQL_NO_DATA;
			addStmtError(stmt, "01S06", NULL, 0);
			break;
		}
		if (FetchOffset == 0) {
			stmt->currentRow = 0;
			return SQL_NO_DATA;
		}
		if (FetchOffset > LastResultRow) {
			stmt->currentRow = LastResultRow;
			return SQL_NO_DATA;
		}
		stmt->currentRow = FetchOffset - 1;
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
	mapi_seek_row(stmt->hdl, stmt->currentRow, MAPI_SEEK_SET);
	if (mapi_error(stmt->Dbc->mid)) {
		/* Row value out of range (we assume that's the error) */
		addStmtError(stmt, "HY107", mapi_error_str(stmt->Dbc->mid), 0);
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

	return SQLFetchScroll_((ODBCStmt *) hStmt, FetchOrientation,
			       FetchOffset);
}
