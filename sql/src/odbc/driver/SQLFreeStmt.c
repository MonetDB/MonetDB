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
 * SQLFreeStmt()
 * CLI Compliance: ISO 92
 *
 * Note: the option SQL_DROP is deprecated in ODBC 3.0 and replaced by
 * SQLFreeHandle(). It is provided here for old (pre ODBC 3.0) applications.
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLFreeStmt(SQLHSTMT handle, SQLUSMALLINT option)
{
	ODBCStmt *stmt = (ODBCStmt *) handle;

	/* Check parameter handle */
	if (!isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	switch (option) {
	case SQL_CLOSE:
		/* Note: this option is also called from SQLCancel() and
		   SQLCloseCursor(), so be careful when changing the code */
		/* close cursor, discard result set, set to prepared */
		ODBCfreeResultCol(stmt);
		stmt->nrCols = 0;
		stmt->currentRow = 0;

		if (stmt->State == EXECUTED)
			stmt->State = PREPARED;

		/* Important: do not destroy the bind parameters and columns! */
		return SQL_SUCCESS;
	case SQL_DROP:
		return SQLFreeHandle(SQL_HANDLE_STMT, handle);
	case SQL_UNBIND:
		mapi_clear_bindings(stmt->hdl);
		ODBCfreebindcol(stmt);
		return SQL_SUCCESS;
	case SQL_RESET_PARAMS:
		mapi_clear_params(stmt->hdl);
		return SQL_SUCCESS;
	default:
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	/* not reached */
}
