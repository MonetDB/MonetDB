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
 * SQLBrowseConnect()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"


static SQLRETURN
SQLBrowseConnect_(ODBCDbc *dbc, SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn,
		  SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax,
		  SQLSMALLINT *pcbConnStrOut)
{
	(void) szConnStrOut;	/* Stefan: unused!? */
	(void) cbConnStrOutMax;	/* Stefan: unused!? */
	(void) pcbConnStrOut;	/* Stefan: unused!? */

	fixODBCstring(szConnStrIn, cbConnStrIn, addDbcError, dbc);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\"\n", szConnStrIn, cbConnStrIn);
#endif

	/* check connection state, should not be connected */
	if (dbc->Connected) {
		/* Connection name in use */
		addDbcError(dbc, "08002", NULL, 0);
		return SQL_ERROR;
	}


	/* TODO: finish implementation */
	/* TODO: check szConnStrIn, parse it and retrieve the different settings */
	/* TODO: next call (an internal version of) SQLConnect() */


	/* For now just report "not supported" and return error */
	/* Driver does not support this function */
	addDbcError(dbc, "IM001", NULL, 0);
	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLBrowseConnect(SQLHDBC hDbc, SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn,
		 SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax,
		 SQLSMALLINT *pcbConnStrOut)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLBrowseConnect " PTRFMT, PTRFMTCAST hDbc);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return SQLBrowseConnect_(dbc, szConnStrIn, cbConnStrIn, szConnStrOut,
				 cbConnStrOutMax, pcbConnStrOut);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLBrowseConnectA(SQLHDBC hDbc, SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn,
		  SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax,
		  SQLSMALLINT *pcbConnStrOut)
{
	return SQLBrowseConnect(hDbc, szConnStrIn, cbConnStrIn, szConnStrOut,
				cbConnStrOutMax, pcbConnStrOut);
}

SQLRETURN SQL_API
SQLBrowseConnectW(SQLHDBC hDbc, SQLWCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn,
		  SQLWCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax,
		  SQLSMALLINT *pcbConnStrOut)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;
	SQLCHAR *in = NULL, *out;
	SQLSMALLINT n;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLBrowseConnectW " PTRFMT, PTRFMTCAST hDbc);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(szConnStrIn, cbConnStrIn, in, addDbcError, dbc, return SQL_ERROR);
	prepWcharOut(out, cbConnStrOutMax);
	rc = SQLBrowseConnect_(dbc, in, SQL_NTS, out, cbConnStrOutMax * 4, &n);
	fixWcharOut(rc, out, n, szConnStrOut, cbConnStrOutMax, pcbConnStrOut, 1, addDbcError, dbc);
	if (in)
		free(in);
	return rc;
}
#endif	/* WITH_WCHAR */
