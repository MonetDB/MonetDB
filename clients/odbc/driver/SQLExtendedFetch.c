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
 * SQLExtendedFetch
 * CLI Compliance: Deprecated.
 * In ODBC 3.x, SQLExtendedFetch has been replaced by SQLFetchScroll.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN SQL_API
SQLExtendedFetch(SQLHSTMT StatementHandle,
		 SQLUSMALLINT FetchOrientation,
		 SQLLEN FetchOffset,
#ifdef BUILD_REAL_64_BIT_MODE	/* note: only defined on Debian Lenny */
		 SQLUINTEGER  *RowCountPtr,
#else
		 SQLULEN *RowCountPtr,
#endif
		 SQLUSMALLINT *RowStatusArray)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLExtendedFetch " PTRFMT " %s " LENFMT " " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST StatementHandle,
		translateFetchOrientation(FetchOrientation),
		LENCAST FetchOffset, PTRFMTCAST RowCountPtr,
		PTRFMTCAST RowStatusArray);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State < EXECUTED0 || stmt->State == FETCHED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	rc = MNDBFetchScroll(stmt, FetchOrientation, FetchOffset,
			     RowStatusArray);

	if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
		stmt->State = EXTENDEDFETCHED;

	if (SQL_SUCCEEDED(rc) && RowCountPtr) {
#ifdef BUILD_REAL_64_BIT_MODE	/* note: only defined on Debian Lenny */
		WriteValue(RowCountPtr, (SQLUINTEGER) stmt->rowSetSize);
#else
		WriteValue(RowCountPtr, (SQLULEN) stmt->rowSetSize);
#endif
	}

	return rc;
}
