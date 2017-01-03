/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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

/********************************************************************
 * SQLSetScrollOptions()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetStmtAttr()
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLSetScrollOptions(SQLHSTMT StatementHandle,
		    SQLUSMALLINT fConcurrency,
		    SQLLEN crowKeyset,
		    SQLUSMALLINT crowRowset)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetScrollOptions " PTRFMT " %u " LENFMT " %u\n",
		PTRFMTCAST StatementHandle, (unsigned int) fConcurrency,
		LENCAST crowKeyset, (unsigned int) crowRowset);
#endif

	(void) fConcurrency;	/* Stefan: unused!? */
	(void) crowKeyset;	/* Stefan: unused!? */
	(void) crowRowset;	/* Stefan: unused!? */

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	/* TODO: implement the mapping to multiple SQLSetStmtAttr() calls */
	/* See ODBC 3.5 SDK Help file for details */

	/* for now return error */
	/* Driver does not support this function */
	addStmtError((ODBCStmt *) StatementHandle, "IM001", NULL, 0);
	return SQL_ERROR;
}
