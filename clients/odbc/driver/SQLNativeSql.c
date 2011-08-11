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
 * SQLNativeSql()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
SQLNativeSql_(ODBCStmt *stmt,
	      SQLCHAR *InStatementText,
	      SQLINTEGER TextLength1,
	      SQLCHAR *OutStatementText,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *TextLength2Ptr)
{
	char *query;

	fixODBCstring(InStatementText, TextLength1, SQLINTEGER,
		      addStmtError, stmt, return SQL_ERROR);

	if (InStatementText == NULL) {
		/* Invalid use of null pointer */
		addStmtError(stmt, "HY009", NULL, 0);
		return SQL_ERROR;
	}
#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\"\n", (int) TextLength1, (char *) InStatementText);
#endif

	query = ODBCTranslateSQL(InStatementText, (size_t) TextLength1,
				 stmt->noScan);
	copyString(query, strlen(query), OutStatementText, BufferLength,
		   TextLength2Ptr, SQLINTEGER, addStmtError, stmt,
		   free(query); return SQL_ERROR);
	free(query);

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLNativeSql(SQLHSTMT StatementHandle,
	     SQLCHAR *InStatementText,
	     SQLINTEGER TextLength1,
	     SQLCHAR *OutStatementText,
	     SQLINTEGER BufferLength,
	     SQLINTEGER *TextLength2Ptr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSql " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLNativeSql_(stmt,
			     InStatementText,
			     TextLength1,
			     OutStatementText,
			     BufferLength,
			     TextLength2Ptr);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLNativeSqlA(SQLHSTMT StatementHandle,
	      SQLCHAR *InStatementText,
	      SQLINTEGER TextLength1,
	      SQLCHAR *OutStatementText,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *TextLength2Ptr)
{
	return SQLNativeSql(StatementHandle,
			    InStatementText,
			    TextLength1,
			    OutStatementText,
			    BufferLength,
			    TextLength2Ptr);
}

SQLRETURN SQL_API
SQLNativeSqlW(SQLHSTMT StatementHandle,
	      SQLWCHAR *InStatementText,
	      SQLINTEGER TextLength1,
	      SQLWCHAR *OutStatementText,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *TextLength2Ptr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLRETURN rc;
	SQLINTEGER n;
	SQLSMALLINT nn;
	SQLCHAR *sqlin, *sqlout;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSqlW " PTRFMT " ", PTRFMTCAST StatementHandle);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	fixWcharIn(InStatementText, TextLength1, SQLCHAR, sqlin,
		   addStmtError, stmt, return SQL_ERROR);

	rc = SQLNativeSql_(stmt, sqlin, SQL_NTS, NULL, 0, &n);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearStmtErrors(stmt);
	n++;			/* account for NUL byte */
	sqlout = malloc(n);
	rc = SQLNativeSql_(stmt, sqlin, SQL_NTS, sqlout, n, &n);
	nn = (SQLSMALLINT) n;
	fixWcharOut(rc, sqlout, nn, OutStatementText, BufferLength,
		    TextLength2Ptr, 1, addStmtError, stmt);

	return rc;
}
#endif /* WITH_WCHAR */
