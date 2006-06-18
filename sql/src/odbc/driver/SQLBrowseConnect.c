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
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif


static SQLRETURN
SQLBrowseConnect_(ODBCDbc *dbc, SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn, SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)
{
	char *key, *attr;
	char *dsn, *uid, *pwd, *host;
	int port;
	SQLSMALLINT len = 0;

	fixODBCstring(szConnStrIn, cbConnStrIn, addDbcError, dbc);

#ifdef ODBCDEBUG
	ODBCLOG(" \"%.*s\"\n", (int) cbConnStrIn, (char*) szConnStrIn);
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
		return SQLConnect_(dbc, (SQLCHAR *) dsn, SQL_NTS, (SQLCHAR *) uid, SQL_NTS, (SQLCHAR *) pwd, SQL_NTS, host, port);
	}

	if (dsn == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy((char *) szConnStrOut, "DSN={MonetDB};", cbConnStrOutMax);
		len += 14;
		szConnStrOut += 14;
		cbConnStrOutMax -= 14;
	}
	if (uid == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy((char *) szConnStrOut, "UID:Login ID=?;", cbConnStrOutMax);
		len += 15;
		szConnStrOut += 15;
		cbConnStrOutMax -= 15;
	}
	if (pwd == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy((char *) szConnStrOut, "PWD:Password=?;", cbConnStrOutMax);
		len += 15;
		szConnStrOut += 15;
		cbConnStrOutMax -= 15;
	}
	if (host == NULL) {
		if (cbConnStrOutMax > 0)
			strncpy((char *) szConnStrOut, "*HOST:Server=?;", cbConnStrOutMax);
		len += 15;
		szConnStrOut += 15;
		cbConnStrOutMax -= 15;
	}
	if (port == 0) {
		if (cbConnStrOutMax > 0)
			strncpy((char *) szConnStrOut, "*PORT:Port=?;", cbConnStrOutMax);
		len += 13;
		szConnStrOut += 13;
		cbConnStrOutMax -= 13;
	}

	if (pcbConnStrOut)
		*pcbConnStrOut = len;

	return SQL_NEED_DATA;
}

SQLRETURN SQL_API
SQLBrowseConnect(SQLHDBC hDbc, SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn, SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLBrowseConnect " PTRFMT, PTRFMTCAST hDbc);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return SQLBrowseConnect_(dbc, szConnStrIn, cbConnStrIn, szConnStrOut, cbConnStrOutMax, pcbConnStrOut);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLBrowseConnectA(SQLHDBC hDbc, SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn, SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)
{
	return SQLBrowseConnect(hDbc, szConnStrIn, cbConnStrIn, szConnStrOut, cbConnStrOutMax, pcbConnStrOut);
}

SQLRETURN SQL_API
SQLBrowseConnectW(SQLHDBC hDbc, SQLWCHAR * szConnStrIn, SQLSMALLINT cbConnStrIn, SQLWCHAR * szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)
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

	fixWcharIn(szConnStrIn, cbConnStrIn, SQLCHAR, in, addDbcError, dbc, return SQL_ERROR);
	prepWcharOut(out, cbConnStrOutMax);
	rc = SQLBrowseConnect_(dbc, in, SQL_NTS, out, cbConnStrOutMax * 4, &n);
	fixWcharOut(rc, out, n, szConnStrOut, cbConnStrOutMax, pcbConnStrOut, 1, addDbcError, dbc);
	if (in)
		free(in);
	return rc;
}
#endif /* WITH_WCHAR */
