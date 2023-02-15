/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
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
#include <time.h>

#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#endif

#ifndef HAVE_SQLGETPRIVATEPROFILESTRING
#define SQLGetPrivateProfileString(section,entry,default,buffer,bufferlen,filename)	((int) strcpy_len(buffer,default,bufferlen))
#endif

static void
get_serverinfo(ODBCDbc *dbc)
{
	MapiHdl hdl;
	char *n, *v;

	if ((hdl = mapi_query(dbc->mid, "select name, value from sys.env() where name in ('monet_version', 'gdk_dbname', 'max_clients', 'raw_strings')")) == NULL)
		return;
	dbc->raw_strings = false;
	while (mapi_fetch_row(hdl)) {
		n = mapi_fetch_field(hdl, 0);
		v = mapi_fetch_field(hdl, 1);
		if (strcmp(n, "monet_version") == 0) {
			sscanf(v, "%hd.%hd.%hd",
			       &dbc->major, &dbc->minor, &dbc->patch);
		} else
		if (strcmp(n, "max_clients") == 0) {
			sscanf(v, "%hu", &dbc->maxclients);
		} else if (strcmp(n, "raw_strings") == 0) {
			dbc->raw_strings = strcmp(v, "true") == 0;
		} else {
			assert(strcmp(n, "gdk_dbname") == 0);
			assert(dbc->dbname == NULL ||
			       strcmp(dbc->dbname, v) == 0);
			if (dbc->dbname)
				free(dbc->dbname);
			dbc->dbname = strdup(v);
		}
	}
	mapi_close_handle(hdl);
	if ((hdl = mapi_query(dbc->mid, "select id from sys._tables where name = 'comments' and schema_id = (select id from sys.schemas where name = 'sys')")) == NULL)
		return;
	n = NULL;
	while (mapi_fetch_row(hdl)) {
		n = mapi_fetch_field(hdl, 0);
	}
	dbc->has_comment = n != NULL;
	mapi_close_handle(hdl);
}

SQLRETURN
MNDBConnect(ODBCDbc *dbc,
	    const SQLCHAR *ServerName,
	    SQLSMALLINT NameLength1,
	    const SQLCHAR *UserName,
	    SQLSMALLINT NameLength2,
	    const SQLCHAR *Authentication,
	    SQLSMALLINT NameLength3,
	    const char *host,
	    int port,
	    const char *dbname)
{
	SQLRETURN rc = SQL_SUCCESS;
	char *dsn = NULL;
	char uid[32];
	char pwd[32];
	char buf[256];
	char db[32];
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
	if (NameLength1 > 0) {
		dsn = dupODBCstring(ServerName, (size_t) NameLength1);
		if (dsn == NULL) {
			/* Memory allocation error */
			addDbcError(dbc, "HY001", NULL, 0);
			return SQL_ERROR;
		}
	}

#ifdef ODBCDEBUG
	if ((ODBCdebug == NULL || *ODBCdebug == 0) && dsn && *dsn) {
		char logfile[2048];
		n = SQLGetPrivateProfileString(dsn, "logfile", "",
					       logfile, sizeof(logfile),
					       "odbc.ini");
		if (n > 0) {
			free((void *) ODBCdebug); /* discard const */
#ifdef NATIVE_WIN32
			size_t attrlen = strlen(logfile);
			SQLWCHAR *wattr = malloc((attrlen + 1) * sizeof(SQLWCHAR));
			if (ODBCutf82wchar(logfile,
					   (SQLINTEGER) attrlen,
					   wattr,
					   (SQLLEN) ((attrlen + 1) * sizeof(SQLWCHAR)),
					   NULL,
					   NULL)) {
				free(wattr);
				wattr = NULL;
			}
			ODBCdebug = wattr;
#else
			ODBCdebug = strdup(logfile);
#endif
		}
	}
#endif

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

	if (dbname == NULL || *dbname == 0) {
		dbname = dbc->dbname;
	}
	if (dbname == NULL || *dbname == 0) {
		if (dsn && *dsn) {
			n = SQLGetPrivateProfileString(dsn, "database", "", db,
						       sizeof(db), "odbc.ini");
			if (n > 0)
				dbname = db;
		}
	}
	if (dbname && !*dbname)
		dbname = NULL;

#ifdef NATIVE_WIN32
	wchar_t *s;
	if (port == 0 && (s = _wgetenv(L"MAPIPORT")) != NULL)
		port = _wtoi(s);
#else
	char *s;
	if (port == 0 && (s = getenv("MAPIPORT")) != NULL)
		port = atoi(s);
#endif
	if (port == 0 && dsn && *dsn) {
		n = SQLGetPrivateProfileString(dsn, "port", MAPI_PORT_STR,
					       buf, sizeof(buf), "odbc.ini");
		if (n > 0)
			port = atoi(buf);
	}
	if (port == 0)
		port = MAPI_PORT;

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
		dbname ? dbname : "(null)");
#endif

	/* connect to a server on host via port */
	/* FIXME: use dbname from ODBC connect string/options here */
	mid = mapi_mapi(host, port, uid, pwd, "sql", dbname);
	if (mid) {
		mapi_setAutocommit(mid, dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON);
		mapi_set_size_header(mid, true);
		mapi_reconnect(mid);
	}
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
		dbc->Connected = true;
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
		if (dbname)	/* dup before dbname is freed */
			dbname = strdup(dbname);
		if (dbc->dbname != NULL)
			free(dbc->dbname);
		dbc->dbname = (char *) dbname; /* discard const */
		get_serverinfo(dbc);
		/* set timeout after we're connected */
		mapi_timeout(mid, dbc->sql_attr_connection_timeout * 1000);
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
	ODBCLOG("SQLConnect %p\n", ConnectionHandle);
#endif

	if (!isValidDbc((ODBCDbc *) ConnectionHandle))
		return SQL_INVALID_HANDLE;

	clearDbcErrors((ODBCDbc *) ConnectionHandle);

	return MNDBConnect((ODBCDbc *) ConnectionHandle,
			   ServerName, NameLength1,
			   UserName, NameLength2,
			   Authentication, NameLength3,
			   NULL, 0, NULL);
}

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
	ODBCLOG("SQLConnectW %p\n", ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(ServerName, NameLength1, SQLCHAR, ds,
		   addDbcError, dbc, goto bailout);
	fixWcharIn(UserName, NameLength2, SQLCHAR, uid,
		   addDbcError, dbc, goto bailout);
	fixWcharIn(Authentication, NameLength3, SQLCHAR, pwd,
		   addDbcError, dbc, goto bailout);

	rc = MNDBConnect(dbc,
			 ds, SQL_NTS,
			 uid, SQL_NTS,
			 pwd, SQL_NTS,
			 NULL, 0, NULL);

      bailout:
	if (ds)
		free(ds);
	if (uid)
		free(uid);
	if (pwd)
		free(pwd);
	return rc;
}
