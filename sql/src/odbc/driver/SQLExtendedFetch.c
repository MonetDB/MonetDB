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
 * CLI Compliance: Deprecated
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLExtendedFetch(SQLHSTMT hStmt, SQLUSMALLINT nOrientation, SQLLEN nOffset, SQLULEN * pnRowCount, SQLUSMALLINT *pRowStatusArray)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLUSMALLINT *array_status_ptr;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLExtendedFetch " PTRFMT " %d %d\n", PTRFMTCAST hStmt, nOrientation, nOffset);
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

	array_status_ptr = stmt->ApplRowDescr->sql_desc_array_status_ptr;
	stmt->ApplRowDescr->sql_desc_array_status_ptr = pRowStatusArray;

	rc = SQLFetchScroll_(stmt, nOrientation, nOffset);

	stmt->ApplRowDescr->sql_desc_array_status_ptr = array_status_ptr;

	if (SQL_SUCCEEDED(rc) || rc == SQL_NO_DATA)
		stmt->State = EXTENDEDFETCHED;

	if (SQL_SUCCEEDED(rc) && pnRowCount)
		*pnRowCount = stmt->rowSetSize;

	return rc;
}
