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
 * SQLCancel()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLCancel(SQLHSTMT hStmt)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLCancel\n");
#endif

	if (!isValidStmt(hStmt))
		return SQL_INVALID_HANDLE;

	return SQLFreeStmt_(hStmt, SQL_CLOSE);
}
