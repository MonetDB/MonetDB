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


static SQLRETURN
SQLNativeSql_(ODBCStmt *stmt, SQLCHAR *szSqlStrIn, SQLINTEGER cbSqlStrIn, SQLCHAR *szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)
{
	char *query;

	fixODBCstring(szSqlStrIn, cbSqlStrIn, addStmtError, stmt);

	if (szSqlStrIn == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);

		return SQL_ERROR;
	}
#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\"\n", cbSqlStrIn, szSqlStrIn);
#endif

	query = ODBCTranslateSQL(szSqlStrIn, (size_t) cbSqlStrIn);
	copyString(query, szSqlStr, cbSqlStrMax, pcbSqlStr, addStmtError, stmt);
	free(query);

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLNativeSql(SQLHSTMT hStmt, SQLCHAR *szSqlStrIn, SQLINTEGER cbSqlStrIn, SQLCHAR *szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSql " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLNativeSql_(stmt, szSqlStrIn, cbSqlStrIn, szSqlStr, cbSqlStrMax, pcbSqlStr);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLNativeSqlA(SQLHSTMT hStmt, SQLCHAR *szSqlStrIn, SQLINTEGER cbSqlStrIn, SQLCHAR *szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)
{
	return SQLNativeSql(hStmt, szSqlStrIn, cbSqlStrIn, szSqlStr, cbSqlStrMax, pcbSqlStr);
}

SQLRETURN SQL_API
SQLNativeSqlW(SQLHSTMT hStmt, SQLWCHAR * szSqlStrIn, SQLINTEGER cbSqlStrIn, SQLWCHAR * szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLRETURN rc;
	SQLINTEGER n;
	SQLSMALLINT nn;
	SQLCHAR *sqlin, *sqlout;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSqlW " PTRFMT " ", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szSqlStrIn, cbSqlStrIn, sqlin, addStmtError, stmt, return SQL_ERROR);

	prepWcharOut(sqlout, cbSqlStrMax);

	rc = SQLNativeSql_(stmt, sqlin, SQL_NTS, sqlout, cbSqlStrMax * 4, &n);

	nn = (SQLSMALLINT) n;
	fixWcharOut(rc, sqlout, nn, szSqlStr, cbSqlStrMax, pcbSqlStr, 1, addStmtError, stmt);

	return rc;
}
#endif /* WITH_WCHAR */
