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
 * SQLBulkOperations()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


#ifdef ODBCDEBUG
static char *
translateOperation(SQLSMALLINT Operation)
{
	switch (Operation) {
	case SQL_ADD:
		return "SQL_ADD";
	case SQL_UPDATE_BY_BOOKMARK:
		return "SQL_UPDATE_BY_BOOKMARK";
	case SQL_DELETE_BY_BOOKMARK:
		return "SQL_DELETE_BY_BOOKMARK";
	case SQL_FETCH_BY_BOOKMARK:
		return "SQL_FETCH_BY_BOOKMARK";
	default:
		return "invalid";
	}
}
#endif

SQLRETURN SQL_API
SQLBulkOperations(SQLHSTMT StatementHandle,
		  SQLSMALLINT Operation)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLBulkOperations " PTRFMT " %s\n",
		PTRFMTCAST StatementHandle, translateOperation(Operation));
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

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

	/* check Operation code */
	switch (Operation) {
	case SQL_ADD:
	case SQL_UPDATE_BY_BOOKMARK:
	case SQL_DELETE_BY_BOOKMARK:
	case SQL_FETCH_BY_BOOKMARK:
		break;
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: finish implementation */

	/* Driver does not support this function */
	addStmtError(stmt, "IM001", NULL, 0);
	return SQL_ERROR;
}
