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
 * SQLFreeStmt()
 * CLI Compliance: ISO 92
 *
 * Note: the option SQL_DROP is deprecated in ODBC 3.0 and replaced by
 * SQLFreeHandle(). It is provided here for old (pre ODBC 3.0) applications.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
MNDBFreeStmt(ODBCStmt *stmt,
	     SQLUSMALLINT Option)
{
	switch (Option) {
	case SQL_CLOSE:
		/* Note: this option is also called from SQLCancel()
		 * and SQLCloseCursor(), so be careful when changing
		 * the code */
		/* close cursor, discard result set, set to prepared */
		setODBCDescRecCount(stmt->ImplRowDescr, 0);
		stmt->currentRow = 0;
		stmt->startRow = 0;
		stmt->rowSetSize = 0;

		if (stmt->State == EXECUTED0)
			stmt->State = stmt->queryid >= 0 ? PREPARED0 : INITED;
		else if (stmt->State >= EXECUTED1)
			stmt->State = stmt->queryid >= 0 ? PREPARED1 : INITED;

		/* Important: do not destroy the bind parameters and columns! */
		return SQL_SUCCESS;
	case SQL_DROP:
		return ODBCFreeStmt_(stmt);
	case SQL_UNBIND:
		setODBCDescRecCount(stmt->ApplRowDescr, 0);
		return SQL_SUCCESS;
	case SQL_RESET_PARAMS:
		setODBCDescRecCount(stmt->ApplParamDescr, 0);
		setODBCDescRecCount(stmt->ImplParamDescr, 0);
		mapi_clear_params(stmt->hdl);
		return SQL_SUCCESS;
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	/* not reached */
}

#ifdef ODBCDEBUG
static char *
translateOption(SQLUSMALLINT Option)
{
	switch (Option) {
	case SQL_CLOSE:
		return "SQL_CLOSE";
	case SQL_DROP:
		return "SQL_DROP";
	case SQL_UNBIND:
		return "SQL_UNBIND";
	case SQL_RESET_PARAMS:
		return "SQL_RESET_PARAMS";
	default:
		return "unknown";
	}
}
#endif

SQLRETURN SQL_API
SQLFreeStmt(SQLHSTMT StatementHandle,
	    SQLUSMALLINT Option)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLFreeStmt " PTRFMT " %s\n",
		PTRFMTCAST StatementHandle, translateOption(Option));
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return MNDBFreeStmt((ODBCStmt *) StatementHandle, Option);
}
