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
 * SQLGetStmtOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLGetStmtAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLGetStmtOption(SQLHSTMT hStmt, SQLUSMALLINT fOption, SQLPOINTER pvParam)
{
	switch (fOption) {
		/* only the ODBC 1.0 and ODBC 2.0 options */
	case SQL_QUERY_TIMEOUT:
	case SQL_MAX_ROWS:
	case SQL_NOSCAN:
	case SQL_MAX_LENGTH:
	case SQL_ASYNC_ENABLE:
	case SQL_BIND_TYPE:
	case SQL_CURSOR_TYPE:
	case SQL_CONCURRENCY:
	case SQL_KEYSET_SIZE:
	case SQL_ROWSET_SIZE:
	case SQL_SIMULATE_CURSOR:
	case SQL_RETRIEVE_DATA:
	case SQL_USE_BOOKMARKS:
/*		case SQL_GET_BOOKMARKS:	is deprecated in ODBC 3.0+ */
	case SQL_ROW_NUMBER:
		/* use mapping as described in ODBC 3.0 SDK Help */
		return SQLGetStmtAttr_(hStmt, fOption, pvParam, 0, NULL);
	default:
	{
		ODBCStmt *stmt = (ODBCStmt *) hStmt;

		if (!isValidStmt(stmt))
			 return SQL_INVALID_HANDLE;

		clearStmtErrors(stmt);

		/* return error: Invalid option/attribute identifier */
		addStmtError(stmt, "HY092", NULL, 0);

		return SQL_ERROR;
	}
	}

	return SQL_ERROR;
}
