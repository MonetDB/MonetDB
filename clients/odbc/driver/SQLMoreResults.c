/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * SQLMoreResults()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQL_API
SQLMoreResults(SQLHSTMT StatementHandle)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLMoreResults " PTRFMT "\n", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	if (stmt->State < EXECUTED0)
		return SQL_NO_DATA;

	switch (mapi_next_result(stmt->hdl)) {
	case MOK:
		stmt->State = stmt->queryid >= 0 ? (stmt->State == EXECUTED0 ? PREPARED0 : PREPARED1) : INITED;
		return SQL_NO_DATA;
	case MERROR:
		/* General error */
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	case MTIMEOUT:
		/* Timeout expired / Communication link failure */
		addStmtError(stmt, stmt->Dbc->sql_attr_connection_timeout ? "HYT00" : "08S01", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	default:
		return ODBCInitResult(stmt);
	}
}
