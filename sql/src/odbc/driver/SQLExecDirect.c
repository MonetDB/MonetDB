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
 * SQLExecDirect()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLExecDirect_(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
	RETCODE rc;

	if (!isValidStmt((ODBCStmt *) hStmt))
		return SQL_INVALID_HANDLE;

	/* prepare SQL command */
	rc = SQLPrepare_(hStmt, szSqlStr, nSqlStr);
	if (rc == SQL_SUCCESS) {
		/* execute prepared statement */
		rc = SQLExecute_(hStmt);
	}

	/* Do not set errors here, they are set in SQLPrepare() and/or SQLExecute() */

	return rc;
}

SQLRETURN
SQLExecDirect(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
	return SQLExecDirect_(hStmt, szSqlStr, nSqlStr);
}
