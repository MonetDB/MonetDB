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


SQLRETURN
SQLPrepare_(ODBCStmt *stmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStrLength)
{
	char *query;
	MapiMsg ret;

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should NOT be executed */
	if (stmt->State == EXECUTED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* check input parameter */
	if (szSqlStr == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	fixODBCstring(szSqlStr, nSqlStrLength, addStmtError, stmt);

	/* TODO: check (parse) the Query on correctness */
	/* TODO: convert ODBC escape sequences ( {d 'value'} or {t 'value'} or
	   {ts 'value'} or {escape 'e-char'} or {oj outer-join} or
	   {fn scalar-function} etc. ) to MonetDB SQL syntax */
	/* count the number of parameter markers (question mark: ?) */

	/* TODO: count the number of output columns and their description */

	/* we need a null-terminated string, so allocate a copy */
	query = dupODBCstring(szSqlStr, nSqlStrLength);
#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepare: \"%s\"\n", query);
#endif
	ret = mapi_prepare_handle(stmt->hdl, query);
	free(query);
	if (ret != MOK) {
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	}

	/* update the internal state */
	stmt->State = PREPARED;
	return SQL_SUCCESS;
}

SQLRETURN
SQLPrepare(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStrLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLPrepare\n");
#endif

	return SQLPrepare_((ODBCStmt *) hStmt, szSqlStr, nSqlStrLength);
}
