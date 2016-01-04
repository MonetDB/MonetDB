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
 * SQLGetStmtOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLGetStmtAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN SQL_API
SQLGetStmtOption(SQLHSTMT StatementHandle,
		 SQLUSMALLINT Option,
		 SQLPOINTER ValuePtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLULEN v;
	SQLRETURN r;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetStmtOption " PTRFMT " %s " PTRFMT "\n",
		PTRFMTCAST StatementHandle, translateStmtOption(Option),
		PTRFMTCAST ValuePtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* only the ODBC 1.0 and ODBC 2.0 options */
	switch (Option) {
	case SQL_ASYNC_ENABLE:
	case SQL_CONCURRENCY:
	case SQL_CURSOR_TYPE:
	case SQL_NOSCAN:
	case SQL_QUERY_TIMEOUT:
	case SQL_RETRIEVE_DATA:
	case SQL_SIMULATE_CURSOR:
	case SQL_USE_BOOKMARKS:
	case SQL_ROW_NUMBER:
		/* SQLGetStmtAttr returns 64 bit value, but we need to
		 * return 32 bit value */
		r = MNDBGetStmtAttr(stmt, Option, &v, 0, NULL);
		if (SQL_SUCCEEDED(r))
			WriteData(ValuePtr, (SQLUINTEGER) v, SQLUINTEGER);
		return r;
	case SQL_BIND_TYPE:
	case SQL_KEYSET_SIZE:
	case SQL_MAX_LENGTH:
	case SQL_MAX_ROWS:
	case SQL_ROWSET_SIZE:
/*		case SQL_GET_BOOKMARKS:	is deprecated in ODBC 3.0+ */
		/* use mapping as described in ODBC 3.0 SDK Help */
		return MNDBGetStmtAttr(stmt, Option, ValuePtr, 0, NULL);
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		break;
	}

	return SQL_ERROR;
}
