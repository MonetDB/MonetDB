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
SQLPrepare(SQLHSTMT hStmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStrLength)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	int params = 0;
	char *query = 0;


	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should NOT be executed */
	if (stmt->State == EXECUTED) {
		/* 24000 = Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);

		return SQL_ERROR;
	}
	assert(stmt->ResultRows == NULL);

	/* check input parameter */
	if (szSqlStr == NULL) {
		/* HY009 = Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);

		return SQL_ERROR;
	}

	if (stmt->Query != NULL) {
		/* there was already a prepared statement, free it */
		free(stmt->Query);
		stmt->Query = NULL;
	}

	/* make a duplicate of the SQL command string */
	fixODBCstring(szSqlStr, nSqlStrLength);
	stmt->Query = dupODBCstring(szSqlStr, nSqlStrLength);

	if (stmt->Query == NULL) {
		/* the value for nSqlStrLength was invalid */
		/* HY090 = Invalid string or buffer length */
		addStmtError(stmt, "HY090", NULL, 0);

		return SQL_ERROR;
	}

	/* TODO: check (parse) the Query on correctness */
	/* TODO: convert ODBC escape sequences ( {d 'value'} or {t 'value'} or
	   {ts 'value'} or {escape 'e-char'} or {oj outer-join} or
	   {fn scalar-function} etc. ) to MonetDB SQL syntax */
	/* count the number of parameter markers (question mark: ?) */

	/* should move to the parser (or a parser should be moved in here) */
	if (stmt->bindParams.size) {
		query = stmt->Query;

		while (query) {
			/* problem with strings with ?s */
			if ((query = strchr(query, '?')) != NULL)
				params++;
		}
		if (stmt->bindParams.size != params) {
			addStmtError(stmt, "HY000", NULL, 0);

			return SQL_ERROR;
		}
	}

	/* TODO: count the number of output columns and their description */

	/* update the internal state */
	stmt->State = PREPARED;

	return SQL_SUCCESS;
}
