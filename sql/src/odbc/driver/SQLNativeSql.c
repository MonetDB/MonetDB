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
 * SQLNativeSql()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLNativeSql(SQLHSTMT hStmt, SQLCHAR *szSqlStrIn, SQLINTEGER cbSqlStrIn,
	     SQLCHAR *szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSql\n");
#endif

	(void) szSqlStrIn;	/* Stefan: unused!? */
	(void) cbSqlStrIn;	/* Stefan: unused!? */
	(void) szSqlStr;	/* Stefan: unused!? */
	(void) cbSqlStrMax;	/* Stefan: unused!? */
	(void) pcbSqlStr;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* TODO: implement this function and corresponding behavior */

	/* for now return error IM001: driver not capable */
	addStmtError(stmt, "IM001", NULL, 0);

	return SQL_ERROR;
}
