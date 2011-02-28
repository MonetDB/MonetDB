/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
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

/********************************************************************
 * SQLSetPos()
 * CLI Compliance: ODBC
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLSetPos(SQLHSTMT hStmt,
	  SQLSETPOSIROW nRow,
	  SQLUSMALLINT nOperation,
	  SQLUSMALLINT nLockType)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetPos " PTRFMT " " ULENFMT " %d %d\n", PTRFMTCAST hStmt, ULENCAST nRow, (int) nOperation, (int) nLockType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check the parameter values */

	if (stmt->State < EXECUTED0) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State <= EXECUTED1) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	if (nRow > (SQLSETPOSIROW) stmt->rowSetSize) {
		/* Row value out of range */
		addStmtError(stmt, "HY107", NULL, 0);
		return SQL_ERROR;
	}

	if (stmt->cursorType == SQL_CURSOR_FORWARD_ONLY) {
		/* Invalid cursor position */
		addStmtError(stmt, "HY109", NULL, 0);
		return SQL_ERROR;
	}

	switch (nLockType) {
	case SQL_LOCK_NO_CHANGE:
		/* the only value that we support */
		break;
	case SQL_LOCK_EXCLUSIVE:
	case SQL_LOCK_UNLOCK:
		/* Optional feature not implemented */
		addStmtError(stmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	switch (nOperation) {
	case SQL_POSITION:
		if (nRow == 0) {
			/* Invalid cursor position */
			addStmtError(stmt, "HY109", NULL, 0);
			return SQL_ERROR;
		}
		if (mapi_seek_row(stmt->hdl, stmt->startRow + nRow - 1, MAPI_SEEK_SET) != MOK) {
			/* Invalid cursor position */
			addStmtError(stmt, "HY109", NULL, 0);
			return SQL_ERROR;
		}
		stmt->currentRow = stmt->startRow + nRow - 1;
		switch (mapi_fetch_row(stmt->hdl)) {
		case MOK:
			break;
		case MTIMEOUT:
			/* Connection timeout expired */
			addStmtError(stmt, "HYT01", NULL, 0);
			return SQL_ERROR;
		default:
			/* Invalid cursor position */
			addStmtError(stmt, "HY109", NULL, 0);
			return SQL_ERROR;
		}
		stmt->currentRow++;
		break;
	case SQL_REFRESH:
	case SQL_UPDATE:
	case SQL_DELETE:
		/* Optional feature not implemented */
		addStmtError(stmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}
