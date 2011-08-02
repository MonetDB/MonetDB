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

#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#endif

#ifndef HAVE_SQLGETPRIVATEPROFILESTRING
#define SQLGetPrivateProfileString(section,entry,default,buffer,bufferlen,filename)	(strncpy(buffer,default,bufferlen), buffer[bufferlen-1]=0, strlen(buffer))
#endif


static SQLRETURN
SQLBrowseConnect_(ODBCDbc *dbc,
		  SQLCHAR *szConnStrIn,
		  SQLSMALLINT cbConnStrIn,
		  SQLCHAR *szConnStrOut,
		  SQLSMALLINT cbConnStrOutMax,
		  SQLSMALLINT *pcbConnStrOut)
{
	char *key, *attr;
	char *dsn, *uid, *pwd, *host, *dbname;
	int port;
	SQLSMALLINT len = 0;
	char buf[256];
	int n;
	int allocated = 0;
	SQLRETURN rc;

	fixODBCstring(szConnStrIn, cbConnStrIn, SQLSMALLINT, addDbcError, dbc, return SQL_ERROR);

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
	dbname = dbc->dbname;

	while (ODBCGetKeyAttr(&szConnStrIn, &cbConnStrIn, &key, &attr)) {
		if (strcasecmp(key, "dsn") == 0 && dsn == NULL) {
			dsn = attr;
			allocated |= 1;
		} else if (strcasecmp(key, "uid") == 0 && uid == NULL) {
			uid = attr;
			allocated |= 2;
		} else if (strcasecmp(key, "pwd") == 0 && pwd == NULL) {
			pwd = attr;
			allocated |= 4;
		} else if (strcasecmp(key, "host") == 0 && host == NULL) {
			host = attr;
			allocated |= 8;
		} else if (strcasecmp(key, "port") == 0 && port == 0) {
			port = atoi(attr);
			free(attr);
		} else if (strcasecmp(key, "database") == 0 && dbname == NULL) {
			dbname = attr;
			allocated |= 16;
		} else
			free(attr);
		free(key);
	}

	if (dsn) {
		if (uid == NULL) {
			n = SQLGetPrivateProfileString(dsn, "uid", "", buf, sizeof(buf), "odbc.ini");
			if (n > 0 && buf[0]) {
				uid = strdup(buf);
				allocated |= 2;
			}
		}
		if (pwd == NULL) {
			n = SQLGetPrivateProfileString(dsn, "pwd", "", buf, sizeof(buf), "odbc.ini");
			if (n > 0 && buf[0]) {
				pwd = strdup(buf);
				allocated |= 4;
			}
		}
		if (host == NULL) {
			n = SQLGetPrivateProfileString(dsn, "host", "", buf, sizeof(buf), "odbc.ini");
			if (n > 0 && buf[0]) {
				host = strdup(buf);
				allocated |= 8;
			}
		}
		if (port == 0) {
			n = SQLGetPrivateProfileString(dsn, "port", "", buf, sizeof(buf), "odbc.ini");
			if (n > 0 && buf[0]) {
				port = atoi(buf);
			}
		}
		if (dbname == NULL) {
			n = SQLGetPrivateProfileString(dsn, "database", "", buf, sizeof(buf), "odbc.ini");
			if (n > 0 && buf[0]) {
				dbname = strdup(buf);
				allocated |= 16;
			}
		}
	}

	if (uid != NULL && pwd != NULL) {
		rc = SQLConnect_(dbc, (SQLCHAR *) dsn, SQL_NTS, (SQLCHAR *) uid, SQL_NTS, (SQLCHAR *) pwd, SQL_NTS, host, port, dbname);
	} else {
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
		if (dbname == NULL) {
			if (cbConnStrOutMax > 0)
				strncpy((char *) szConnStrOut, "*DATABASE:Database=?;", cbConnStrOutMax);
			len += 21;
			szConnStrOut += 21;
			cbConnStrOutMax -= 21;
		}

		if (pcbConnStrOut)
			*pcbConnStrOut = len;

		rc = SQL_NEED_DATA;
	}

	if (allocated & 1)
		free(dsn);
	if (allocated & 2)
		free(uid);
	if (allocated & 4)
		free(pwd);
	if (allocated & 8)
		free(host);
	if (allocated & 16)
		free(dbname);
	return rc;
}

SQLRETURN SQL_API
SQLBrowseConnect(SQLHDBC hDbc,
		 SQLCHAR *szConnStrIn,
		 SQLSMALLINT cbConnStrIn,
		 SQLCHAR *szConnStrOut,
		 SQLSMALLINT cbConnStrOutMax,
		 SQLSMALLINT *pcbConnStrOut)
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
SQLBrowseConnectA(SQLHDBC hDbc,
		  SQLCHAR *szConnStrIn,
		  SQLSMALLINT cbConnStrIn,
		  SQLCHAR *szConnStrOut,
		  SQLSMALLINT cbConnStrOutMax,
		  SQLSMALLINT *pcbConnStrOut)
{
	return SQLBrowseConnect(hDbc, szConnStrIn, cbConnStrIn, szConnStrOut, cbConnStrOutMax, pcbConnStrOut);
}

SQLRETURN SQL_API
SQLBrowseConnectW(SQLHDBC hDbc,
		  SQLWCHAR * szConnStrIn,
		  SQLSMALLINT cbConnStrIn,
		  SQLWCHAR * szConnStrOut,
		  SQLSMALLINT cbConnStrOutMax,
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

	fixWcharIn(szConnStrIn, cbConnStrIn, SQLCHAR, in, addDbcError, dbc, return SQL_ERROR);
	out = malloc(100);	/* max 80 needed */
	rc = SQLBrowseConnect_(dbc, in, SQL_NTS, out, 100, &n);
	fixWcharOut(rc, out, n, szConnStrOut, cbConnStrOutMax, pcbConnStrOut, 1, addDbcError, dbc);
	if (in)
		free(in);
	return rc;
}
#endif /* WITH_WCHAR */
