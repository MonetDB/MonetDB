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
	MapiMsg ret;
	char *query;

	if (stmt->State >= EXECUTED1 || (stmt->State == EXECUTED0 && mapi_more_results(stmt->hdl))) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);

		return SQL_ERROR;
	}

	/* check input parameter */
	if (szSqlStr == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);

		return SQL_ERROR;
	}

	fixODBCstring(szSqlStr, nSqlStr, addStmtError, stmt);
	query = ODBCTranslateSQL(szSqlStr, (size_t) nSqlStr);

	ODBCResetStmt(stmt);

#ifdef ODBCDEBUG
	ODBCLOG("SQLExecDirect: \"%s\"\n", query);
#endif
	ret = mapi_query_handle(stmt->hdl, query);

	free(query);
	if (ret != MOK) {
		/* General error */
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);

		return SQL_ERROR;
	}
	return ODBCInitResult(stmt);
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
SQLExecDirectA(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
	return SQLExecDirect(hStmt, szSqlStr, nSqlStr);
}

SQLRETURN SQL_API
SQLExecDirectW(SQLHSTMT hStmt, SQLWCHAR * szSqlStr, SQLINTEGER nSqlStr)
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
#endif /* WITH_WCHAR */
