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
 * SQLPrepare
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


void
ODBCResetStmt(ODBCStmt *stmt)
{
	SQLFreeStmt_(stmt, SQL_CLOSE);
	setODBCDescRecCount(stmt->ImplParamDescr, 0);

	if (stmt->query)
		free(stmt->query);
	stmt->query = NULL;
	stmt->State = INITED;
}

SQLRETURN
SQLPrepare_(ODBCStmt *stmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStrLength)
{
	char *query;
	MapiMsg ret;

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

	fixODBCstring(szSqlStr, nSqlStrLength, addStmtError, stmt);
	/* TODO: convert ODBC escape sequences ( {d 'value'} or {t 'value'} or
	   {ts 'value'} or {escape 'e-char'} or {oj outer-join} or
	   {fn scalar-function} etc. ) to MonetDB SQL syntax */
	query = ODBCTranslateSQL(szSqlStr, (size_t) nSqlStrLength);

	ODBCResetStmt(stmt);

	/* TODO: check (parse) the Query on correctness */
	/* count the number of parameter markers (question mark: ?) */

	/* TODO: count the number of output columns and their description */

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepare: \"%s\"\n", query);
#endif

	ret = mapi_prepare_handle(stmt->hdl, query);

	if (ret != MOK) {
		/* General error */
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);

		return SQL_ERROR;
	}

	/* update the internal state */
	stmt->query = query;
	stmt->State = PREPARED1;	/* XXX or PREPARED0, depending on query */

	return SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLPrepare(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStrLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepare " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt((ODBCStmt *) hStmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	return SQLPrepare_((ODBCStmt *) hStmt, szSqlStr, nSqlStrLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLPrepareA(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStrLength)
{
	return SQLPrepare(hStmt, szSqlStr, nSqlStrLength);
}

SQLRETURN SQL_API
SQLPrepareW(SQLHSTMT hStmt, SQLWCHAR * szSqlStr, SQLINTEGER nSqlStrLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	SQLCHAR *sql;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepareW " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(szSqlStr, nSqlStrLength, sql, addStmtError, stmt, return SQL_ERROR);

	rc = SQLPrepare_(stmt, sql, SQL_NTS);

	if (sql)
		free(sql);

	return rc;
}
#endif /* WITH_WCHAR */
