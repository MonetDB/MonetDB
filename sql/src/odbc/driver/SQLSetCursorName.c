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
 * SQLSetCursorName()
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
SQLSetCursorName_(ODBCStmt *stmt, SQLCHAR *szCursor, SQLSMALLINT nCursorLength)
{
	fixODBCstring(szCursor, nCursorLength, addStmtError, stmt);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\"\n", nCursorLength, szCursor);
#endif

	if (stmt->State >= EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: implement the requested behavior */
	/* Note: when cursor names are to be implemented the SQL parser &
	   executor must also be able to use it. */

	/* for now always return error */
	/* Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLSetCursorName(SQLHSTMT hStmt, SQLCHAR *szCursor, SQLSMALLINT nCursorLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetCursorName " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLSetCursorName_(stmt, szCursor, nCursorLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLSetCursorNameA(SQLHSTMT hStmt, SQLCHAR *szCursor, SQLSMALLINT nCursorLength)
{
	return SQLSetCursorName(hStmt, szCursor, nCursorLength);
}

SQLRETURN SQL_API
SQLSetCursorNameW(SQLHSTMT hStmt, SQLWCHAR *szCursor,
		  SQLSMALLINT nCursorLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc;
	SQLCHAR *cursor;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetCursorNameW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szCursor, nCursorLength, cursor, addStmtError, stmt, return SQL_ERROR);

	rc = SQLSetCursorName_(stmt, cursor, SQL_NTS);

	if (cursor)
		free(cursor);

	return rc;
}
#endif	/* WITH_WCHAR */
