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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLGetCursorName(SQLHSTMT hStmt, SQLCHAR *szCursor,
		 SQLSMALLINT nCursorMaxLength, SQLSMALLINT *pnCursorLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

	(void) szCursor;	/* Stefan: unused!? */
	(void) nCursorMaxLength;	/* Stefan: unused!? */
	(void) pnCursorLength;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* TODO: implement the requested behavior when SQLSetCursorName() is implemented */

	/* for now always return error: No cursor name available */
	addStmtError(stmt, "HY015", NULL, 0);

	return SQL_ERROR;
}
