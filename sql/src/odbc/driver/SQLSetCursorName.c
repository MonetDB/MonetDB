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


SQLRETURN
SQLSetCursorName(SQLHSTMT hStmt, SQLCHAR *szCursor, SQLSMALLINT nCursorLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetCursorName\n");
#endif

	(void) szCursor;	/* Stefan: unused!? */
	(void) nCursorLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* TODO: implement the requested behavior */
	/* Note: when cursor names are to be implemented the SQL parser &
	   executor must also be able to use it. */

	/* for now always return error: Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}
