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
 * Copyright August 2008-2013 MonetDB B.V.
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
SQLNativeSql_(ODBCDbc *dbc,
	      SQLCHAR *InStatementText,
	      SQLINTEGER TextLength1,
	      SQLCHAR *OutStatementText,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *TextLength2Ptr)
{
	char *query;

	fixODBCstring(InStatementText, TextLength1, SQLINTEGER,
		      addDbcError, dbc, return SQL_ERROR);

	if (InStatementText == NULL) {
		/* Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}
#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\"\n", (int) TextLength1, (char *) InStatementText);
#endif

	query = ODBCTranslateSQL(InStatementText, (size_t) TextLength1,
				 SQL_NOSCAN_OFF);
	copyString(query, strlen(query), OutStatementText, BufferLength,
		   TextLength2Ptr, SQLINTEGER, addDbcError, dbc,
		   free(query); return SQL_ERROR);
	free(query);

	return dbc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLNativeSql(SQLHDBC ConnectionHandle,
	     SQLCHAR *InStatementText,
	     SQLINTEGER TextLength1,
	     SQLCHAR *OutStatementText,
	     SQLINTEGER BufferLength,
	     SQLINTEGER *TextLength2Ptr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSql " PTRFMT " ", PTRFMTCAST ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		 return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return SQLNativeSql_(dbc,
			     InStatementText,
			     TextLength1,
			     OutStatementText,
			     BufferLength,
			     TextLength2Ptr);
}

SQLRETURN SQL_API
SQLNativeSqlA(SQLHDBC ConnectionHandle,
	      SQLCHAR *InStatementText,
	      SQLINTEGER TextLength1,
	      SQLCHAR *OutStatementText,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *TextLength2Ptr)
{
	return SQLNativeSql(ConnectionHandle,
			    InStatementText,
			    TextLength1,
			    OutStatementText,
			    BufferLength,
			    TextLength2Ptr);
}

SQLRETURN SQL_API
SQLNativeSqlW(SQLHDBC ConnectionHandle,
	      SQLWCHAR *InStatementText,
	      SQLINTEGER TextLength1,
	      SQLWCHAR *OutStatementText,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *TextLength2Ptr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;
	SQLRETURN rc;
	SQLINTEGER n;
	SQLSMALLINT nn;
	SQLCHAR *sqlin, *sqlout;

#ifdef ODBCDEBUG
	ODBCLOG("SQLNativeSqlW " PTRFMT " ", PTRFMTCAST ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		 return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(InStatementText, TextLength1, SQLCHAR, sqlin,
		   addDbcError, dbc, return SQL_ERROR);

	rc = SQLNativeSql_(dbc, sqlin, SQL_NTS, NULL, 0, &n);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearDbcErrors(dbc);
	n++;			/* account for NUL byte */
	sqlout = malloc(n);
	rc = SQLNativeSql_(dbc, sqlin, SQL_NTS, sqlout, n, &n);
	nn = (SQLSMALLINT) n;
	if (SQL_SUCCEEDED(rc)) {
		fixWcharOut(rc, sqlout, nn, OutStatementText, BufferLength,
			    TextLength2Ptr, 1, addDbcError, dbc);
	}
	free(sqlout);

	return rc;
}
