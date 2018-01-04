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
 * SQLParamOptions()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetStmtAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLParamOptions(SQLHSTMT StatementHandle,
		SQLULEN RowNumber,
		SQLULEN *RowNumberPtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	RETCODE rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLParamOptions " PTRFMT " " ULENFMT "\n",
		PTRFMTCAST StatementHandle, ULENCAST RowNumber);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	rc = MNDBSetStmtAttr(stmt, SQL_ATTR_PARAMSET_SIZE,
			     (SQLPOINTER) (uintptr_t) RowNumber, 0);

	if (SQL_SUCCEEDED(rc)) {
		rc = MNDBSetStmtAttr(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
				     RowNumberPtr, 0);
	}
	return rc;
}
