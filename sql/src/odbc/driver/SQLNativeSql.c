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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


SQLRETURN
SQLNativeSql(SQLHSTMT hStmt, SQLCHAR *szSqlStrIn, SQLINTEGER cbSqlStrIn,
	     SQLCHAR *szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	char *query;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSql\n");
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixODBCstring(szSqlStrIn, cbSqlStrIn, addStmtError, stmt);

	if (szSqlStrIn == NULL) {
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	query = ODBCTranslateSQL(szSqlStrIn, (size_t) cbSqlStrIn);
	copyString(query, szSqlStr, cbSqlStrMax, pcbSqlStr,
		   addStmtError, stmt);
	free(query);

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
