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
 * SQLNumResultCols()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLNumResultCols(SQLHSTMT hStmt, SQLSMALLINT *pnColumnCount)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNumResultCols\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be prepared or executed */
	if (stmt->State == INITED) {
		/* caller should have called SQLPrepare or SQLExecDirect first */
		/* HY010 = Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);

		return SQL_ERROR;
	}

	/* When the query is only prepared (via SQLPrepare) we do not have
	 * the correct nrCols value yet (this is a limitation of the current
	 * MonetDB SQL frontend implementation). */
	/* we only have a correct nrCols value when the query is executed */
	if (stmt->State != EXECUTED) {
		/* HY000 = General Error */
		addStmtError(stmt, "HY000",
			     "Cannot return the number of output columns. Query must be executed first",
			     0);
		return SQL_ERROR;
	}

	/* check output parameter */
	if (pnColumnCount == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	/* We can now set the "number of output columns" value */
	/* Note: row count can be 0 (for non SELECT queries) */
	*pnColumnCount = stmt->ImplRowDescr->sql_desc_count;

	return SQL_SUCCESS;
}
