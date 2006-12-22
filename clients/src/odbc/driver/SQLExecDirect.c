/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2006 CWI.
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
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
ODBCExecDirect(ODBCStmt *stmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
	char *query;
	MapiMsg ret;
	MapiHdl hdl;

	hdl = stmt->hdl;

	if (stmt->State >= EXECUTED1 || (stmt->State == EXECUTED0 && mapi_more_results(hdl))) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: convert ODBC escape sequences ( {d 'value'} or {t 'value'} or
	   {ts 'value'} or {escape 'e-char'} or {oj outer-join} or
	   {fn scalar-function} etc. ) to MonetDB SQL syntax */
	query = ODBCTranslateSQL(szSqlStr, (size_t) nSqlStr, stmt->noScan);

	ODBCResetStmt(stmt);

#ifdef ODBCDEBUG
	ODBCLOG("SQLExecDirect: \"%s\"\n", query);
#endif

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
		/* General error */
		addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	}

	/* now get the result data and store it to our internal data structure */

	return ODBCInitResult(stmt);
}

SQLRETURN
SQLExecDirect_(ODBCStmt *stmt, SQLCHAR *szSqlStr, SQLINTEGER nSqlStr)
{
	SQLRETURN ret;
	SQLINTEGER i;

	/* check input parameter */
	if (szSqlStr == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	fixODBCstring(szSqlStr, nSqlStr, SQLINTEGER, addStmtError, stmt);
	for (i = 0; i < nSqlStr; i++)
		if (szSqlStr[i] == '?') {
			/* query may have parameters, take the long route */
			ret = SQLPrepare_(stmt, szSqlStr, nSqlStr);
			if (ret == SQL_SUCCESS)
				ret = SQLExecute_(stmt);
			return ret;
		}

	/* no parameters, take the direct route */
	return ODBCExecDirect(stmt, szSqlStr, nSqlStr);
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

	fixWcharIn(szSqlStr, nSqlStr, SQLCHAR, sql, addStmtError, stmt, return SQL_ERROR);

	rc = SQLExecDirect_((ODBCStmt *) hStmt, sql, SQL_NTS);

	if (sql)
		free(sql);

	return rc;
}
#endif /* WITH_WCHAR */
