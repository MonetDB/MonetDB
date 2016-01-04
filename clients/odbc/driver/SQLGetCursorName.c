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

/**********************************************************************
 * SQLGetCursorName()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
MNDBGetCursorName(ODBCStmt *stmt,
		  SQLCHAR *CursorName,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *NameLengthPtr)
{
	(void) CursorName;
	(void) BufferLength;
	(void) NameLengthPtr;

	/* TODO: implement the requested behavior when
	 * SQLSetCursorName() is implemented */

	/* for now always return error */
	/* No cursor name available */
	addStmtError(stmt, "HY015", NULL, 0);

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLGetCursorName(SQLHSTMT StatementHandle,
		 SQLCHAR *CursorName,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *NameLengthPtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetCursorName " PTRFMT " " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST StatementHandle, PTRFMTCAST CursorName,
		(int) BufferLength, PTRFMTCAST NameLengthPtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBGetCursorName(stmt, CursorName, BufferLength, NameLengthPtr);
}

SQLRETURN SQL_API
SQLGetCursorNameA(SQLHSTMT StatementHandle,
		  SQLCHAR *CursorName,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *NameLengthPtr)
{
	return SQLGetCursorName(StatementHandle,
				CursorName,
				BufferLength,
				NameLengthPtr);
}

SQLRETURN SQL_API
SQLGetCursorNameW(SQLHSTMT StatementHandle,
		  SQLWCHAR *CursorName,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *NameLengthPtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc;
	SQLSMALLINT n;
	SQLCHAR *cursor;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetCursorNameW " PTRFMT " " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST StatementHandle, PTRFMTCAST CursorName,
		(int) BufferLength, PTRFMTCAST NameLengthPtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	rc = MNDBGetCursorName(stmt, NULL, 0, &n);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearStmtErrors(stmt);
	n++;			/* account for NUL byte */
	cursor = malloc(n);
	if (cursor == NULL) {
		/* Memory allocation error */
		addStmtError(stmt, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	rc = MNDBGetCursorName(stmt, cursor, BufferLength, &n);
	if (SQL_SUCCEEDED(rc))    {
		fixWcharOut(rc, cursor, n, CursorName, BufferLength,
			    NameLengthPtr, 1, addStmtError, stmt);
	}
	free(cursor);

	return rc;
}
