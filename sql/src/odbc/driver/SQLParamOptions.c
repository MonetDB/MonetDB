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

SQLRETURN
SQLParamOptions(SQLHSTMT hStmt, SQLUINTEGER nRow, SQLUINTEGER *pnRow)
{
	RETCODE rc;

	/* use mapping as described in ODBC 3 SDK Help file */
	rc = SQLSetStmtAttr(hStmt, SQL_ATTR_PARAMSET_SIZE, &nRow, 0);
	if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
		rc = SQLSetStmtAttr(hStmt, SQL_ATTR_PARAMS_PROCESSED_PTR,
				    pnRow, 0);
	}
	return rc;
}
