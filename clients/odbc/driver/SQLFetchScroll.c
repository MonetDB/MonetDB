/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 *****************************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
MNDBFetchScroll(ODBCStmt *stmt,
		SQLSMALLINT FetchOrientation,
		SQLLEN FetchOffset,
		SQLUSMALLINT *RowStatusArray)
{
	assert(stmt->hdl);

	if ((stmt->cursorType == SQL_CURSOR_FORWARD_ONLY ||
	     stmt->cursorScrollable == SQL_NONSCROLLABLE) &&
	    FetchOrientation != SQL_FETCH_NEXT) {
		/* Fetch type out of range */
		addStmtError(stmt, "HY106", NULL, 0);
		return SQL_ERROR;
	}
#define RowSetSize	(stmt->ApplRowDescr->sql_desc_array_size)

	assert(stmt->startRow >= 0);
	switch (FetchOrientation) {
	case SQL_FETCH_NEXT:
		stmt->startRow += stmt->rowSetSize;
		break;
	case SQL_FETCH_FIRST:
		stmt->startRow = 0;
		break;
	case SQL_FETCH_LAST:
		if (stmt->rowcount < RowSetSize)
			stmt->startRow = 0;
		else
			stmt->startRow = stmt->rowcount - RowSetSize;
		break;
	case SQL_FETCH_PRIOR:
		if (stmt->startRow == 0) {
			/* before start */
			stmt->startRow = 0;
			stmt->rowSetSize = 0;
			stmt->State = FETCHED;
			return SQL_NO_DATA;
		}
		if (stmt->startRow < (SQLLEN) RowSetSize) {
			/* Attempt to fetch before the result set
			 * returned the first rowset */
			addStmtError(stmt, "01S06", NULL, 0);
			stmt->startRow = 0;
		} else
			stmt->startRow = stmt->startRow - RowSetSize;
		break;
	case SQL_FETCH_RELATIVE:
		if ((stmt->startRow > 0 || stmt->rowSetSize > 0 ||
		     FetchOffset <= 0) &&
		    ((SQLULEN) stmt->startRow < stmt->rowcount ||
		     FetchOffset >= 0)) {
			if ((stmt->startRow == 0 && stmt->rowSetSize == 0 &&
			     FetchOffset <= 0) ||
			    (stmt->startRow == 0 && stmt->rowSetSize > 0 &&
			     FetchOffset < 0) ||
			    (stmt->startRow > 0 &&
			     stmt->startRow + FetchOffset < 1 &&
			     (FetchOffset > (SQLLEN) RowSetSize ||
			      -FetchOffset > (SQLLEN) RowSetSize))) {
				/* before start */
				stmt->startRow = 0;
				stmt->rowSetSize = 0;
				stmt->State = FETCHED;
				return SQL_NO_DATA;
			}
			if (stmt->startRow > 0 &&
			    stmt->startRow + FetchOffset < 1 &&
			    FetchOffset <= (SQLLEN) RowSetSize &&
			    -FetchOffset <= (SQLLEN) RowSetSize) {
				/* Attempt to fetch before the result
				 * set returned the first rowset */
				addStmtError(stmt, "01S06", NULL, 0);
				stmt->startRow = 0;
				break;
			}
			if (stmt->startRow + FetchOffset >= 0 &&
			    stmt->startRow + FetchOffset < (SQLLEN) stmt->rowcount) {
				stmt->startRow += FetchOffset;
				break;
			}
			if (stmt->startRow + FetchOffset >= (SQLLEN) stmt->rowcount ||
			    (stmt->startRow >= (SQLLEN) stmt->rowcount &&
			     FetchOffset >= 0)) {
				/* after end */
				stmt->startRow = stmt->rowcount;
				stmt->rowSetSize = 0;
				stmt->State = FETCHED;
				return SQL_NO_DATA;
			}
			/* all bases should have been covered above */
			assert(0);
		}
		/* fall through */
	case SQL_FETCH_ABSOLUTE:
		if (FetchOffset < 0) {
			if ((unsigned int) -FetchOffset <= stmt->rowcount) {
				stmt->startRow = stmt->rowcount + FetchOffset;
				break;
			}
			stmt->startRow = 0;
			if ((unsigned int) -FetchOffset > RowSetSize) {
				/* before start */
				stmt->State = FETCHED;
				stmt->rowSetSize = 0;
				return SQL_NO_DATA;
			}
			/* Attempt to fetch before the result set
			   returned the first rowset */
			addStmtError(stmt, "01S06", NULL, 0);
			break;
		}
		if (FetchOffset == 0) {
			/* before start */
			stmt->startRow = 0;
			stmt->rowSetSize = 0;
			stmt->State = FETCHED;
			return SQL_NO_DATA;
		}
		if ((SQLULEN) FetchOffset > stmt->rowcount) {
			/* after end */
			stmt->startRow = stmt->rowcount;
			stmt->rowSetSize = 0;
			stmt->State = FETCHED;
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

	return MNDBFetch(stmt, RowStatusArray);
}

SQLRETURN SQL_API
SQLFetchScroll(SQLHSTMT StatementHandle,
	       SQLSMALLINT FetchOrientation,
	       SQLLEN FetchOffset)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLFetchScroll " PTRFMT " %s " LENFMT "\n",
		PTRFMTCAST StatementHandle,
		translateFetchOrientation(FetchOrientation),
		LENCAST FetchOffset);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State < EXECUTED0 || stmt->State == EXTENDEDFETCHED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	return MNDBFetchScroll(stmt, FetchOrientation, FetchOffset,
			       stmt->ImplRowDescr->sql_desc_array_status_ptr);
}
