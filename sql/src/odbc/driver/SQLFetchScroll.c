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
SQLFetchScroll(SQLHSTMT hStmt, SQLSMALLINT nOrientation, SQLINTEGER nOffset)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State != EXECUTED) {
		/* caller should have called SQLExecute or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	switch (nOrientation) {
	case SQL_FETCH_NEXT:
		break;
	case SQL_FETCH_FIRST:
		mapi_seek_row(stmt->Dbc->mid, 0, MAPI_SEEK_SET);
		break;
	case SQL_FETCH_LAST:
		mapi_seek_row(stmt->Dbc->mid, -1, MAPI_SEEK_END);
		break;
	case SQL_FETCH_PRIOR:
		mapi_seek_row(stmt->Dbc->mid, -1, MAPI_SEEK_CUR);
		break;
	case SQL_FETCH_ABSOLUTE:
		mapi_seek_row(stmt->Dbc->mid, nOffset - 1, MAPI_SEEK_SET);
		break;
	case SQL_FETCH_RELATIVE:
		mapi_seek_row(stmt->Dbc->mid, nOffset, MAPI_SEEK_CUR);
		break;
	default:
		/* TODO change to unkown Orientation */
		/* for now return error IM001: driver not capable */
		addStmtError(stmt, "IM001", NULL, 0);
		return SQL_ERROR;
	}
	if (mapi_error(stmt->Dbc->mid)) {
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	}
	return SQLFetch(stmt);
}
