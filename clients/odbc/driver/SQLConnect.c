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
 * SQLConnect()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"
#include <monet_options.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>
#endif
#ifdef HAVE_TIME_H
#include <time.h>
#endif

#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#endif

#ifndef HAVE_SQLGETPRIVATEPROFILESTRING
#define SQLGetPrivateProfileString(section,entry,default,buffer,bufferlen,filename)	(strncpy(buffer,default,bufferlen), buffer[bufferlen-1]=0, strlen(buffer))
#endif

static void
set_timezone(Mapi mid)
{
#ifdef HAVE_TIMEZONE
#ifdef _MSC_VER
#define timezone _timezone
#define daylight _daylight
#define tzset _tzset
#endif
	char buf[128];
	long tzone;
	MapiHdl hdl;

	/* timezone and daylight are POSIX-defined variables */
	tzset();
	tzone = timezone - 3600 * daylight;
	if (tzone < 0)
		snprintf(buf, sizeof(buf),
			 "SET TIME ZONE INTERVAL '+%02ld:%02ld' HOUR TO MINUTE",
			 -tzone / 3600, (-tzone % 3600) / 60);
	else
		snprintf(buf, sizeof(buf),
			 "SET TIME ZONE INTERVAL '-%02ld:%02ld' HOUR TO MINUTE",
			 tzone / 3600, (tzone % 3600) / 60);
	if ((hdl = mapi_query(mid, buf)) != NULL)
		mapi_close_handle(hdl);
#else
	(void) mid;
#endif
}

SQLRETURN
SQLConnect_(ODBCDbc *dbc,
	    SQLCHAR *ServerName,
	    SQLSMALLINT NameLength1,
	    SQLCHAR *UserName,
	    SQLSMALLINT NameLength2,
	    SQLCHAR *Authentication,
	    SQLSMALLINT NameLength3,
	    char *host,
	    int port,
	    char *schema)
{
	SQLRETURN rc = SQL_SUCCESS;
	char *dsn = NULL;
	char uid[32];
	char pwd[32];
	char buf[256];
	char db[32];
	char *s;
	int n;
	Mapi mid;

	/* check connection state, should not be connected */
	if (dbc->Connected) {
		/* Connection name in use */
		addDbcError(dbc, "08002", NULL, 0);
		return SQL_ERROR;
	}

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(ServerName, NameLength1, SQLSMALLINT,
		      addDbcError, dbc, return SQL_ERROR);
	if (NameLength1 > 0)
		dsn = dupODBCstring(ServerName, (size_t) NameLength1);

	if (dsn && *dsn)
		n = SQLGetPrivateProfileString(dsn, "uid", "monetdb",
					       uid, sizeof(uid), "odbc.ini");
	else
		n = 0;
	fixODBCstring(UserName, NameLength2, SQLSMALLINT,
		      addDbcError, dbc, if (dsn) free(dsn); return SQL_ERROR);
	if (n == 0 && NameLength2 == 0) {
		if (dsn)
			free(dsn);
		/* Invalid authorization specification */
		addDbcError(dbc, "28000", NULL, 0);
		return SQL_ERROR;
	}
	if (NameLength2 > 0) {
		if ((size_t)NameLength2 >= sizeof(uid))
			NameLength2 = sizeof(uid) - 1;
		strncpy(uid, (char *) UserName, NameLength2);
		uid[NameLength2] = 0;
	}
	if (dsn && *dsn)
		n = SQLGetPrivateProfileString(dsn, "pwd", "monetdb",
					       pwd, sizeof(pwd), "odbc.ini");
	else
		n = 0;
	fixODBCstring(Authentication, NameLength3, SQLSMALLINT,
		      addDbcError, dbc, if (dsn) free(dsn); return SQL_ERROR);
	if (n == 0 && NameLength3 == 0) {
		if (dsn)
			free(dsn);
		/* Invalid authorization specification */
		addDbcError(dbc, "28000", NULL, 0);
		return SQL_ERROR;
	}
	if (NameLength3 > 0) {
		if ((size_t)NameLength3 >= sizeof(pwd))
			NameLength3 = sizeof(pwd) - 1;
		strncpy(pwd, (char *) Authentication, NameLength3);
		pwd[NameLength3] = 0;
	}

	if (schema == NULL || *schema == 0) {
		if (dsn && *dsn) {
			n = SQLGetPrivateProfileString(dsn, "database", "", db,
						       sizeof(db), "odbc.ini");
			if (n > 0)
				schema = db;
		}
	}
	if (schema && !*schema)
		schema = NULL;

	if (port == 0 && (s = getenv("MAPIPORT")) != NULL)
		port = atoi(s);
	if (port == 0 && dsn && *dsn) {
		n = SQLGetPrivateProfileString(dsn, "port", "50000",
					       buf, sizeof(buf), "odbc.ini");
		if (n > 0)
			port = atoi(buf);
	}
	if (port == 0)
		port = 50000;

	if (host == NULL || *host == 0) {
		host = "localhost";
		if (dsn && *dsn) {
			n = SQLGetPrivateProfileString(dsn, "host", "localhost",
						       buf, sizeof(buf),
						       "odbc.ini");
			if (n > 0)
				host = buf;
		}
	}

#ifdef ODBCDEBUG
	ODBCLOG("SQLConnect: DSN=%s UID=%s PWD=%s host=%s port=%d database=%s\n",
		dsn ? dsn : "(null)", uid, pwd, host, port,
		schema ? schema : "(null)");
#endif

	/* connect to a server on host via port */
	/* FIXME: use dbname/catalog from ODBC connect string/options here */
	mid = mapi_connect(host, port, uid, pwd, "sql", schema);
	if (mid == NULL || mapi_error(mid)) {
		/* Client unable to establish connection */
		addDbcError(dbc, "08001", NULL, 0);
		rc = SQL_ERROR;
		/* clean up */
		if (mid)
			mapi_destroy(mid);
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
		dbc->uid = strdup(uid);
		if (dbc->pwd != NULL)
			free(dbc->pwd);
		dbc->pwd = strdup(pwd);
		if (dbc->host)
			free(dbc->host);
		dbc->host = strdup(host);
		if (dbc->dbname != NULL)
			free(dbc->dbname);
		dbc->dbname = schema ? strdup(schema) : NULL;
		mapi_setAutocommit(mid, dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON);
		set_timezone(mid);
	}

	return rc;
}

SQLRETURN SQL_API
SQLConnect(SQLHDBC ConnectionHandle,
	   SQLCHAR *ServerName,
	   SQLSMALLINT NameLength1,
	   SQLCHAR *UserName,
	   SQLSMALLINT NameLength2,
	   SQLCHAR *Authentication,
	   SQLSMALLINT NameLength3)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLConnect " PTRFMT "\n", PTRFMTCAST ConnectionHandle);
#endif

	if (!isValidDbc((ODBCDbc *) ConnectionHandle))
		return SQL_INVALID_HANDLE;

	clearDbcErrors((ODBCDbc *) ConnectionHandle);

	return SQLConnect_((ODBCDbc *) ConnectionHandle,
			   ServerName, NameLength1,
			   UserName, NameLength2,
			   Authentication, NameLength3,
			   NULL, 0, NULL);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLConnectA(SQLHDBC ConnectionHandle,
	    SQLCHAR *ServerName,
	    SQLSMALLINT NameLength1,
	    SQLCHAR *UserName,
	    SQLSMALLINT NameLength2,
	    SQLCHAR *Authentication,
	    SQLSMALLINT NameLength3)
{
	return SQLConnect(ConnectionHandle,
			  ServerName, NameLength1,
			  UserName, NameLength2,
			  Authentication, NameLength3);
}

SQLRETURN SQL_API
SQLConnectW(SQLHDBC ConnectionHandle,
	    SQLWCHAR *ServerName,
	    SQLSMALLINT NameLength1,
	    SQLWCHAR *UserName,
	    SQLSMALLINT NameLength2,
	    SQLWCHAR *Authentication,
	    SQLSMALLINT NameLength3)
{
	SQLCHAR *ds = NULL, *uid = NULL, *pwd = NULL;
	SQLRETURN rc = SQL_ERROR;
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLConnectW " PTRFMT "\n", PTRFMTCAST ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(ServerName, NameLength1, SQLCHAR, ds,
		   addDbcError, dbc, goto exit);
	fixWcharIn(UserName, NameLength2, SQLCHAR, uid,
		   addDbcError, dbc, goto exit);
	fixWcharIn(Authentication, NameLength3, SQLCHAR, pwd,
		   addDbcError, dbc, goto exit);

	rc = SQLConnect_(dbc,
			 ds, SQL_NTS,
			 uid, SQL_NTS,
			 pwd, SQL_NTS,
			 NULL, 0, NULL);

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
