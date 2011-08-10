/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static struct errors {
	const char *error;
	const char *msg;
} errors[] = {
	{"!syntax error", "42000"},
	{"!DROP TABLE: no such table", "42S02"},
	{"!DROP VIEW: unknown view", "42S02"},
	{"!ALTER TABLE: no such table", "42S02"},
	{"!CREATE INDEX: no such table", "42S02"},
	{"!SELECT: no such table", "42S02"},
	{"!INSERT INTO: no such table", "42S02"},
	{"!DELETE FROM: no such table", "42S02"},
	{"!UPDATE: no such table", "42S02"},
	{"!CONSTRAINT FOREIGN KEY: no such table", "42S02"},
	{NULL, NULL},		/* sentinel */
};

static SQLRETURN
ODBCExecDirect(ODBCStmt *stmt, SQLCHAR *StatementText, SQLINTEGER TextLength)
{
	char *query;
	MapiMsg ret;
	MapiHdl hdl;

	hdl = stmt->hdl;

	if (stmt->State >= EXECUTED1 ||
	    (stmt->State == EXECUTED0 && mapi_more_results(hdl))) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: convert ODBC escape sequences ( {d 'value'} or {t
	 * 'value'} or {ts 'value'} or {escape 'e-char'} or {oj
	 * outer-join} or {fn scalar-function} etc. ) to MonetDB SQL
	 * syntax */
	query = ODBCTranslateSQL(StatementText, (size_t) TextLength,
				 stmt->noScan);

	ODBCResetStmt(stmt);

#ifdef ODBCDEBUG
	ODBCLOG("SQLExecDirect: \"%s\"\n", query);
#endif

	if (stmt->next == NULL &&
	    stmt->Dbc->FirstStmt == stmt &&
	    stmt->cursorType == SQL_CURSOR_FORWARD_ONLY) {
		/* we're the only Stmt handle, and we're only going forward */
		if (stmt->Dbc->cachelimit != 10000)
			mapi_cache_limit(stmt->Dbc->mid, 10000);
		stmt->Dbc->cachelimit = 10000;
	} else {
		if (stmt->Dbc->cachelimit != 100)
			mapi_cache_limit(stmt->Dbc->mid, 100);
		stmt->Dbc->cachelimit = 100;
	}
	ret = mapi_query_handle(hdl, query);
	free(query);
	switch (ret) {
	case MOK:
		break;
	case MTIMEOUT:
		/* Communication link failure */
		addStmtError(stmt, "08S01", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	default:
		/* reuse variable for error string */
		query = mapi_result_error(hdl);
		if (query == NULL)
			query = mapi_error_str(stmt->Dbc->mid);
		if (query != NULL) {
			struct errors *e;

			for (e = errors; e->error != NULL; e++)
				if (strncmp(query, e->error, strlen(e->error)) == 0) {
					addStmtError(stmt, e->msg, query, 0);
					return SQL_ERROR;
				}
		}
		/* General error */
		addStmtError(stmt, "HY000", query, 0);
		return SQL_ERROR;
	}

	/* now get the result data and store it to our internal data
	 * structure */

	return ODBCInitResult(stmt);
}

SQLRETURN
SQLExecDirect_(ODBCStmt *stmt,
	       SQLCHAR *StatementText,
	       SQLINTEGER TextLength)
{
	SQLRETURN ret;
	SQLINTEGER i;

	/* check input parameter */
	if (StatementText == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	fixODBCstring(StatementText, TextLength, SQLINTEGER,
		      addStmtError, stmt, return SQL_ERROR);
	for (i = 0; i < TextLength; i++)
		if (StatementText[i] == '?') {
			/* query may have parameters, take the long route */
			ret = SQLPrepare_(stmt, StatementText, TextLength);
			if (ret == SQL_SUCCESS)
				ret = SQLExecute_(stmt);
			return ret;
		}

	/* no parameters, take the direct route */
	return ODBCExecDirect(stmt, StatementText, TextLength);
}

SQLRETURN SQL_API
SQLExecDirect(SQLHSTMT StatementHandle,
	      SQLCHAR *StatementText,
	      SQLINTEGER TextLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLExecDirect " PTRFMT "\n", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return SQLExecDirect_((ODBCStmt *) StatementHandle,
			      StatementText,
			      TextLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLExecDirectA(SQLHSTMT StatementHandle,
	       SQLCHAR *StatementText,
	       SQLINTEGER TextLength)
{
	return SQLExecDirect(StatementHandle, StatementText, TextLength);
}

SQLRETURN SQL_API
SQLExecDirectW(SQLHSTMT StatementHandle,
	       SQLWCHAR *StatementText,
	       SQLINTEGER TextLength)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc;
	SQLCHAR *sql;

#ifdef ODBCDEBUG
	ODBCLOG("SQLExecDirectW " PTRFMT "\n", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(StatementText, TextLength, SQLCHAR, sql,
		   addStmtError, stmt, return SQL_ERROR);

	rc = SQLExecDirect_((ODBCStmt *) StatementHandle, sql, SQL_NTS);

	if (sql)
		free(sql);

	return rc;
}
#endif /* WITH_WCHAR */
