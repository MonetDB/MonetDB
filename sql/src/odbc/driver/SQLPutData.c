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
 * SQLPutData()
 * CLI Compliance: ISO 92
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQL_API
SQLPutData(SQLHSTMT hStmt, SQLPOINTER pData, SQLINTEGER nLengthOrIndicator)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLPutData " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	(void) pData;		/* Stefan: unused!? */
	(void) nLengthOrIndicator;	/* Stefan: unused!? */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* TODO: implement this function and corresponding behavior */

	/* Function sequence error */
	addStmtError(stmt, "HY010", NULL, 0);

	return SQL_ERROR;
}
