/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
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
SQLGetCursorName_(ODBCStmt *stmt,
		  SQLCHAR *szCursor,
		  SQLSMALLINT nCursorMaxLength,
		  SQLSMALLINT *pnCursorLength)
{
	(void) szCursor;	/* Stefan: unused!? */
	(void) nCursorMaxLength;	/* Stefan: unused!? */
	(void) pnCursorLength;	/* Stefan: unused!? */

	/* TODO: implement the requested behavior when SQLSetCursorName() is implemented */

	/* for now always return error */
	/* No cursor name available */
	addStmtError(stmt, "HY015", NULL, 0);

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLGetCursorName(SQLHSTMT hStmt,
		 SQLCHAR *szCursor,
		 SQLSMALLINT nCursorMaxLength,
		 SQLSMALLINT *pnCursorLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetCursorName " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLGetCursorName_(stmt, szCursor, nCursorMaxLength, pnCursorLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetCursorNameA(SQLHSTMT hStmt,
		  SQLCHAR *szCursor,
		  SQLSMALLINT nCursorMaxLength,
		  SQLSMALLINT *pnCursorLength)
{
	return SQLGetCursorName(hStmt, szCursor, nCursorMaxLength, pnCursorLength);
}

SQLRETURN SQL_API
SQLGetCursorNameW(SQLHSTMT hStmt,
		  SQLWCHAR * szCursor,
		  SQLSMALLINT nCursorMaxLength,
		  SQLSMALLINT *pnCursorLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc;
	SQLSMALLINT n;
	SQLCHAR *cursor;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetCursorNameW " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	rc = SQLGetCursorName_(stmt, NULL, 0, &n);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearStmtErrors(stmt);
	n++;			/* account for NUL byte */
	cursor = malloc(n);
	rc = SQLGetCursorName_(stmt, cursor, nCursorMaxLength, &n);
	fixWcharOut(rc, cursor, n, szCursor, nCursorMaxLength, pnCursorLength, 1, addStmtError, stmt);

	return rc;
}
#endif /* WITH_WCHAR */
