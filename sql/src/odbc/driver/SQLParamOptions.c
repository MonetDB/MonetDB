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
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLParamOptions(SQLHSTMT hStmt, SQLUINTEGER nRow, SQLUINTEGER *pnRow)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLParamOptions " PTRFMT " %d\n", PTRFMTCAST hStmt, nRow);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	rc = SQLSetStmtAttr_(stmt, SQL_ATTR_PARAMSET_SIZE, &nRow, 0);

	if (SQL_SUCCEEDED(rc)) {
		rc = SQLSetStmtAttr_(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, pnRow, 0);
	}
	return rc;
}
