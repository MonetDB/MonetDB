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
 * SQLSetCursorName()
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
SQLSetCursorName_(ODBCStmt *stmt,
		  SQLCHAR *CursorName,
		  SQLSMALLINT NameLength)
{
	fixODBCstring(CursorName, NameLength, SQLSMALLINT,
		      addStmtError, stmt, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\"\n", (int) NameLength, (char *) CursorName);
#endif

	if (stmt->State >= EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: implement the requested behavior */
	/* Note: when cursor names are to be implemented the SQL
	 * parser & executor must also be able to use it. */

	/* for now always return error */
	/* Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLSetCursorName(SQLHSTMT StatementHandle,
		 SQLCHAR *CursorName,
		 SQLSMALLINT NameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetCursorName " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLSetCursorName_(stmt, CursorName, NameLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLSetCursorNameA(SQLHSTMT StatementHandle,
		  SQLCHAR *CursorName,
		  SQLSMALLINT NameLength)
{
	return SQLSetCursorName(StatementHandle, CursorName, NameLength);
}

SQLRETURN SQL_API
SQLSetCursorNameW(SQLHSTMT StatementHandle,
		  SQLWCHAR *CursorName,
		  SQLSMALLINT NameLength)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc;
	SQLCHAR *cursor;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetCursorNameW " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(CursorName, NameLength, SQLCHAR, cursor,
		   addStmtError, stmt, return SQL_ERROR);

	rc = SQLSetCursorName_(stmt, cursor, SQL_NTS);

	if (cursor)
		free(cursor);

	return rc;
}
#endif /* WITH_WCHAR */
