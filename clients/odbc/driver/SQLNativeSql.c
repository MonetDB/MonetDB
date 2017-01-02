/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
MNDBNativeSql(ODBCDbc *dbc,
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

	query = ODBCTranslateSQL(dbc, InStatementText, (size_t) TextLength1,
				 SQL_NOSCAN_OFF);
	if (query == NULL) {
		/* Memory allocation error */
		addDbcError(dbc, "HY001", NULL, 0);
		return SQL_ERROR;
	}
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

	return MNDBNativeSql(dbc,
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

	rc = MNDBNativeSql(dbc, sqlin, SQL_NTS, NULL, 0, &n);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearDbcErrors(dbc);
	n++;			/* account for NUL byte */
	sqlout = malloc(n);
	if (sqlout == NULL) {
		/* Memory allocation error */
		addDbcError(dbc, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	rc = MNDBNativeSql(dbc, sqlin, SQL_NTS, sqlout, n, &n);
	nn = (SQLSMALLINT) n;
	if (SQL_SUCCEEDED(rc)) {
		fixWcharOut(rc, sqlout, nn, OutStatementText, BufferLength,
			    TextLength2Ptr, 1, addDbcError, dbc);
	}
	free(sqlout);

	return rc;
}
