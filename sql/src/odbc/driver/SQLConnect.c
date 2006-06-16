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
 * SQLConnect()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include <monet_options.h>
#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif

SQLRETURN
SQLConnect_(ODBCDbc *dbc, SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength, SQLCHAR *szUID, SQLSMALLINT nUIDLength, SQLCHAR *szPWD, SQLSMALLINT nPWDLength, char *host, int port)
{
	SQLRETURN rc = SQL_SUCCESS;
	char *dsn = NULL;
	char *uid = NULL;
	char *pwd = NULL;
	char *schema = NULL;
	char *s;
	Mapi mid;

	/* check connection state, should not be connected */
	if (dbc->Connected) {
		/* Connection name in use */
		addDbcError(dbc, "08002", NULL, 0);
		return SQL_ERROR;
	}

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(szDataSource, nDataSourceLength, addDbcError, dbc);
	if (nDataSourceLength == 0) {
		szDataSource = (SQLCHAR *) "MonetDB";
		nDataSourceLength = strlen((char *) szDataSource);
	}
	dsn = dupODBCstring(szDataSource, (size_t) nDataSourceLength);
	/* for now we only allow the MonetDB data source */
	if (strcasecmp(dsn, "monetdb") != 0) {
		free(dsn);
		/* Data source name not found and no default driver
		   specified */
		addDbcError(dbc, "IM002", NULL, 0);
		return SQL_ERROR;
	}

	/* we need NULL-terminated strings for uid and password, so we
	   need to make copies */
	fixODBCstring(szUID, nUIDLength, addDbcError, dbc);
	if (nUIDLength == 0) {
		uid = strdup("monetdb");
	} else {
		uid = dupODBCstring(szUID, (size_t) nUIDLength);
	}
	fixODBCstring(szPWD, nPWDLength, addDbcError, dbc);
	if (nPWDLength == 0) {
		pwd = strdup("monetdb");
	} else {
		pwd = dupODBCstring(szPWD, (size_t) nPWDLength);
	}

	if (port == 0 && (s = getenv("MAPIPORT")) != NULL)
		port = atoi(s);
	if (port == 0)
		port = 50000;

	/* TODO: get and use a database name */

	/* Retrieved and checked the arguments.
	   Now try to open a connection with the server */
	if (host == NULL || *host == 0)
		host = "localhost";

#ifdef ODBCDEBUG
	ODBCLOG("SQLConnect: DSN=%s UID=%s PWD=%s host=%s port=%d\n", dsn, uid, pwd, host, port);
#endif

	/* connect to a server on host via port */
	mid = mapi_connect(host, port, uid, pwd, "sql");
	if (mid == NULL || mapi_error(mid)) {
		/* Client unable to establish connection */
		addDbcError(dbc, "08001", NULL, 0);
		rc = SQL_ERROR;
		/* clean up */
		if (mid)
			mapi_destroy(mid);
		if (uid != NULL)
			free(uid);
		if (pwd != NULL)
			free(pwd);
		if (dsn != NULL)
			free(dsn);
	} else {
		/* store internal information and clean up buffers */
		dbc->Connected = 1;
		dbc->mid = mid;
		if (dbc->dsn != NULL)
			free(dbc->dsn);
		dbc->dsn = dsn;
		if (dbc->uid != NULL)
			free(dbc->uid);
		dbc->uid = uid;
		if (dbc->pwd != NULL)
			free(dbc->pwd);
		dbc->pwd = pwd;
		if (dbc->host)
			free(dbc->host);
		dbc->host = strdup(host);
		if (dbc->DBNAME != NULL)
			free(dbc->DBNAME);
		dbc->DBNAME = schema;
		mapi_setAutocommit(mid, dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON);
	}

	return rc;
}

SQLRETURN SQL_API
SQLConnect(SQLHDBC hDbc, SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength, SQLCHAR *szUID, SQLSMALLINT nUIDLength, SQLCHAR *szPWD, SQLSMALLINT nPWDLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLConnect " PTRFMT "\n", PTRFMTCAST hDbc);
#endif

	if (!isValidDbc((ODBCDbc *) hDbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors((ODBCDbc *) hDbc);

	return SQLConnect_((ODBCDbc *) hDbc, szDataSource, nDataSourceLength, szUID, nUIDLength, szPWD, nPWDLength, NULL, 0);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLConnectA(SQLHDBC hDbc, SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength, SQLCHAR *szUID, SQLSMALLINT nUIDLength, SQLCHAR *szPWD, SQLSMALLINT nPWDLength)
{
	return SQLConnect(hDbc, szDataSource, nDataSourceLength, szUID, nUIDLength, szPWD, nPWDLength);
}

SQLRETURN SQL_API
SQLConnectW(SQLHDBC hDbc, SQLWCHAR * szDataSource, SQLSMALLINT nDataSourceLength, SQLWCHAR * szUID, SQLSMALLINT nUIDLength, SQLWCHAR * szPWD, SQLSMALLINT nPWDLength)
{
	SQLCHAR *ds = NULL, *uid = NULL, *pwd = NULL;
	SQLRETURN rc = SQL_ERROR;
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLConnectW " PTRFMT "\n", PTRFMTCAST hDbc);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(szDataSource, nDataSourceLength, SQLCHAR, ds, addDbcError, dbc, goto exit);
	fixWcharIn(szUID, nUIDLength, SQLCHAR, uid, addDbcError, dbc, goto exit);
	fixWcharIn(szPWD, nPWDLength, SQLCHAR, pwd, addDbcError, dbc, goto exit);

	rc = SQLConnect_(dbc, ds, SQL_NTS, uid, SQL_NTS, pwd, SQL_NTS, NULL, 0);

      exit:
	if (ds)
		free(ds);
	if (uid)
		free(uid);
	if (pwd)
		free(pwd);
	return rc;
}
#endif /* WITH_WCHAR */
