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
#include "ODBCUtil.h"

SQLRETURN
SQLExecDirect_(ODBCStmt *stmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
	RETCODE rc;

	/* prepare SQL command */
	rc = SQLPrepare_(stmt, szSqlStr, nSqlStr);
	if (SQL_SUCCEEDED(rc)) {
		/* execute prepared statement */
		rc = SQLExecute_(stmt);
		if (rc == SQL_SUCCESS && stmt->Error)
			rc = SQL_SUCCESS_WITH_INFO;
	}
	return rc;
}

SQLRETURN SQL_API
SQLExecDirect(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLExecDirect " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt((ODBCStmt *) hStmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	return SQLExecDirect_((ODBCStmt *) hStmt, szSqlStr, nSqlStr);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLExecDirectW(SQLHSTMT hStmt, SQLWCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc;
	SQLCHAR *sql;

#ifdef ODBCDEBUG
	ODBCLOG("SQLExecDirectW " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szSqlStr, nSqlStr, sql, addStmtError, stmt, return SQL_ERROR);

	rc = SQLExecDirect_((ODBCStmt *) hStmt, sql, SQL_NTS);

	if (sql)
		free(sql);

	return rc;
}
#endif	/* WITH_WCHAR */
