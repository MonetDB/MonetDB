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
 * SQLSetPos()
 * CLI Compliance: ODBC
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"		/* for isValidStmt() & addStmtError() */

SQLRETURN
SQLSetPos(SQLHSTMT hStmt, SQLUSMALLINT nRow, SQLUSMALLINT nOperation,
	  SQLUSMALLINT nLockType)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetPos\n");
#endif

	(void) nRow;		/* Stefan: unused!? */

	if (!isValidStmt(hStmt))
		return SQL_INVALID_HANDLE;

	/* check the parameter values */
	switch (nOperation) {
	case SQL_POSITION:
	case SQL_REFRESH:
	case SQL_UPDATE:
	case SQL_DELETE:
	default:
		/* return error: "Optional feature not implemented" */
		addStmtError(hStmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	}

	switch (nLockType) {
	case SQL_LOCK_NO_CHANGE:
	case SQL_LOCK_EXCLUSIVE:
	case SQL_LOCK_UNLOCK:
	default:
		/* return error: "Optional feature not implemented" */
		addStmtError(hStmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: implement the requested behavior */

	/* for now always return error */
	addStmtError(hStmt, "IM001", NULL, 0);
	return SQL_ERROR;
}
