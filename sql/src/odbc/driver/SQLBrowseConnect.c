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
	char *key, *attr;
	char *dsn, *uid, *pwd, *host;
	int port;
	SQLSMALLINT len = 0;

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

	dsn = dbc->dsn;
	uid = dbc->uid;
	pwd = dbc->pwd;
	host = dbc->host;
	port = dbc->port;

	while (ODBCGetKeyAttr(&szConnStrIn, &cbConnStrIn, &key, &attr)) {
		if (strcasecmp(key, "dsn") == 0 && dsn == NULL)
			dsn = attr;
		else if (strcasecmp(key, "uid") == 0 && uid == NULL)
			uid = attr;
		else if (strcasecmp(key, "pwd") == 0 && pwd == NULL)
			pwd = attr;
		else if (strcasecmp(key, "host") == 0 && host == NULL)
			host = attr;
		else if (strcasecmp(key, "port") == 0 && port == 0) {
			port = atoi(attr);
			free(attr);
		} else
			free(attr);
		free(key);
	}

	if (dsn != NULL && uid != NULL && pwd != NULL) {
		return SQLConnect_(dbc, dsn, SQL_NTS, uid, SQL_NTS,
				   pwd, SQL_NTS, host, port);
	}

	if (dsn == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy(szConnStrOut, "DSN={MonetDB};", cbConnStrOutMax);
		len += 14;
		szConnStrOut += 14;
		cbConnStrOutMax -= 14;
	}
	if (uid == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy(szConnStrOut, "UID:Login ID=?;", cbConnStrOutMax);
		len += 15;
		szConnStrOut += 15;
		cbConnStrOutMax -= 15;
	}
	if (pwd == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy(szConnStrOut, "PWD:Password=?;", cbConnStrOutMax);
		len += 15;
		szConnStrOut += 15;
		cbConnStrOutMax -= 15;
	}
	if (host == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy(szConnStrOut, "*HOST:Server=?;", cbConnStrOutMax);
		len += 15;
		szConnStrOut += 15;
		cbConnStrOutMax -= 15;
	}
	if (port == 0) {
		if (cbConnStrOutMax > 0)
			strncpy(szConnStrOut, "*PORT:Port=?;", cbConnStrOutMax);
		len += 13;
		szConnStrOut += 13;
		cbConnStrOutMax -= 13;
	}

	if (pcbConnStrOut)
		*pcbConnStrOut = len;

	return SQL_NEED_DATA;
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
