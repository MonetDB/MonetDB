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
 * SQLDriverConnect()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strcasecmp */
#else
#include <string.h>
#endif

int
ODBCGetKeyAttr(SQLCHAR **conn, SQLSMALLINT *nconn, char **key, char **attr)
{
	SQLCHAR *p;
	size_t len;

	*key = *attr = NULL;

	p = *conn;
	if (!**conn)
		return 0;
	while (*nconn > 0 && **conn && **conn != '=' && **conn != ';') {
		(*conn)++;
		(*nconn)--;
	}
	if (*nconn == 0 || !**conn || **conn == ';')
		return 0;
	len = *conn - p;
	*key = (char *) malloc(len + 1);
	strncpy(*key, (char *) p, len);
	(*key)[len] = 0;
	(*conn)++;
	(*nconn)--;
	p = *conn;

	if (*nconn > 0 && **conn == '{' && strcasecmp(*key, "DRIVER") == 0) {
		(*conn)++;
		(*nconn)--;
		p++;
		while (*nconn > 0 && **conn && **conn != '}') {
			(*conn)++;
			(*nconn)--;
		}
		len = *conn - p;
		*attr = (char *) malloc(len + 1);
		strncpy(*attr, (char *) p, len);
		(*attr)[len] = 0;
		(*conn)++;
		(*nconn)--;
		/* should check that *nconn == 0 || **conn == ';' */
	} else {
		while (*nconn > 0 && **conn && **conn != ';') {
			(*conn)++;
			(*nconn)--;
		}
		len = *conn - p;
		*attr = (char *) malloc(len + 1);
		strncpy(*attr, (char *) p, len);
		(*attr)[len] = 0;
	}
	if (*nconn > 0 && **conn) {
		(*conn)++;
		(*nconn)--;
	}
	return 1;
}

SQLRETURN
ODBCConnectionString(SQLRETURN rc,
		     ODBCDbc *dbc,
		     SQLCHAR *OutConnectionString,
		     SQLSMALLINT BufferLength,
		     SQLSMALLINT *StringLength2Ptr,
		     const char *dsn,
		     const char *uid,
		     const char *pwd,
		     const char *host,
		     int port,
		     const char *database)
{
	int n;
#ifdef ODBCDEBUG
	SQLCHAR *buf = OutConnectionString;
	int buflen = BufferLength;
#endif

	if (OutConnectionString == NULL)
		BufferLength = -1;
	if (BufferLength > 0) {
		n = snprintf((char *) OutConnectionString, BufferLength,
			     "DSN=%s;", dsn ? dsn : "DEFAULT");
		/* some snprintf's return -1 if buffer too small */
		if (n < 0)
			n = BufferLength + 1;	/* make sure it becomes < 0 */
		BufferLength -= n;
		OutConnectionString += n;
	} else {
		BufferLength = -1;
	}
	if (uid) {
		if (BufferLength > 0) {
			n = snprintf((char *) OutConnectionString,
				     BufferLength, "UID=%s;", uid);
			if (n < 0)
				n = BufferLength + 1;
			BufferLength -= n;
			OutConnectionString += n;
		} else {
			BufferLength = -1;
		}
	}
	if (pwd) {
		if (BufferLength > 0) {
			n = snprintf((char *) OutConnectionString,
				     BufferLength, "PWD=%s;", pwd);
			if (n < 0)
				n = BufferLength + 1;
			BufferLength -= n;
			OutConnectionString += n;
		} else {
			BufferLength = -1;
		}
	}
	if (host) {
		if (BufferLength > 0) {
			n = snprintf((char *) OutConnectionString,
				     BufferLength, "HOST=%s;", host);
			if (n < 0)
				n = BufferLength + 1;
			BufferLength -= n;
			OutConnectionString += n;
		} else {
			BufferLength = -1;
		}
	}
	if (port) {
		char portbuf[10];

		if (BufferLength > 0) {
			n = snprintf((char *) OutConnectionString,
				     BufferLength, "PORT=%d;", port);
			if (n < 0)
				n = BufferLength + 1;
			BufferLength -= n;
			OutConnectionString += n;
		} else {
			BufferLength = -1;
		}
		port = snprintf(portbuf, sizeof(portbuf), "%d", port);
	}
	if (database) {
		if (BufferLength > 0) {
			n = snprintf((char *) OutConnectionString,
				     BufferLength,
				     "DATABASE=%s;", database);
			if (n < 0)
				n = BufferLength + 1;
			BufferLength -= n;
			OutConnectionString += n;
		} else {
			BufferLength = -1;
		}
	}
#ifdef ODBCDEBUG
	if (ODBCdebug && getenv("ODBCDEBUG") == NULL) {
		if (BufferLength > 0) {
			n = snprintf((char *) OutConnectionString,
				     BufferLength,
				     "LOGFILE=%s;", ODBCdebug);
			if (n < 0)
				n = BufferLength + 1;
			BufferLength -= n;
			OutConnectionString += n;
		} else {
			BufferLength = -1;
		}
	}
#endif

	/* calculate how much space was needed */
	if (StringLength2Ptr)
		*StringLength2Ptr = (int) (strlen(dsn ? dsn : "DEFAULT") + 5 +
					   (uid ? strlen(uid) + 5 : 0) +
					   (pwd ? strlen(pwd) + 5 : 0) +
					   (host ? strlen(host) + 6 : 0) +
					   (port ? port + 6 : 0) +
					   (database ? strlen(database) + 10 : 0)
#ifdef ODBCDEBUG
					   + (ODBCdebug && getenv("ODBCDEBUG") == NULL ? strlen(ODBCdebug) + 9 : 0)
#endif
			);

#ifdef ODBCDEBUG
	ODBCLOG("ConnectionString: \"%.*s\" %d\n", buf ? buflen : 6, buf ? buf : "(null)", buflen);
#endif

	/* if it didn't fit, say so */
	if (BufferLength < 0) {
		/* String data, right-truncated */
		addDbcError(dbc, "01004", NULL, 0);
		return SQL_SUCCESS_WITH_INFO;
	}
	return rc;
}

static SQLRETURN
SQLDriverConnect_(ODBCDbc *dbc,
		  SQLHWND WindowHandle,
		  SQLCHAR *InConnectionString,
		  SQLSMALLINT StringLength1,
		  SQLCHAR *OutConnectionString,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLength2Ptr,
		  SQLUSMALLINT DriverCompletion,
		  int tryOnly)
{
	char *key, *attr;
	char *dsn = 0, *uid = 0, *pwd = 0, *host = 0, *database = 0;
	int port = 0;
	SQLRETURN rc;

	(void) WindowHandle;		/* Stefan: unused!? */

	/* check connection state, should not be connected */
	if (dbc->Connected) {
		/* Connection name in use */
		addDbcError(dbc, "08002", NULL, 0);
		return SQL_ERROR;
	}
	assert(!dbc->Connected);

	fixODBCstring(InConnectionString, StringLength1, SQLSMALLINT,
		      addDbcError, dbc, return SQL_ERROR);

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" %u\n", StringLength1,
		(char *) InConnectionString, (unsigned int) DriverCompletion);
#endif

	/* check input arguments */
	switch (DriverCompletion) {
	case SQL_DRIVER_PROMPT:
	case SQL_DRIVER_COMPLETE:
	case SQL_DRIVER_COMPLETE_REQUIRED:
	case SQL_DRIVER_NOPROMPT:
		break;
	default:
		/* Invalid attribute/option identifier */
		addDbcError(dbc, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	while (ODBCGetKeyAttr(&InConnectionString, &StringLength1, &key, &attr)) {
		if (strcasecmp(key, "dsn") == 0 && dsn == NULL)
			dsn = attr;
		else if (strcasecmp(key, "uid") == 0 && uid == NULL)
			uid = attr;
		else if (strcasecmp(key, "pwd") == 0 && pwd == NULL)
			pwd = attr;
		else if (strcasecmp(key, "host") == 0 && host == NULL)
			host = attr;
		else if (strcasecmp(key, "database") == 0 && database == NULL)
			database = attr;
		else if (strcasecmp(key, "port") == 0 && port == 0) {
			port = atoi(attr);
			free(attr);
#ifdef ODBCDEBUG
		} else if (strcasecmp(key, "logfile") == 0 &&
			   getenv("ODBCDEBUG") == NULL) {
			if (ODBCdebug)
				free((void *) ODBCdebug); /* discard const */
			ODBCdebug = strdup(attr);
#endif
		} else
			free(attr);
		free(key);
	}

	if (dsn && strlen(dsn) > SQL_MAX_DSN_LENGTH) {
		/* Data source name too long */
		addDbcError(dbc, "IM010", NULL, 0);
		rc = SQL_ERROR;
	} else if (tryOnly) {
		rc = SQL_SUCCESS;
	} else {
		rc = SQLConnect_(dbc, (SQLCHAR *) dsn, SQL_NTS,
				 (SQLCHAR *) uid, SQL_NTS,
				 (SQLCHAR *) pwd, SQL_NTS,
				 host, port, database);
	}

	if (SQL_SUCCEEDED(rc)) {
		rc = ODBCConnectionString(rc, dbc, OutConnectionString,
					  BufferLength, StringLength2Ptr,
					  dsn, uid, pwd, host, port, database);
	}

	if (dsn)
		free(dsn);
	if (uid)
		free(uid);
	if (pwd)
		free(pwd);
	if (host)
		free(host);
	if (database)
		free(database);
	return rc;
}

SQLRETURN SQL_API
SQLDriverConnect(SQLHDBC ConnectionHandle,
		 SQLHWND WindowHandle,
		 SQLCHAR *InConnectionString,
		 SQLSMALLINT StringLength1,
		 SQLCHAR *OutConnectionString,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLength2Ptr,
		 SQLUSMALLINT DriverCompletion)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDriverConnect " PTRFMT " ", PTRFMTCAST ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return SQLDriverConnect_(dbc,
				 WindowHandle,
				 InConnectionString,
				 StringLength1,
				 OutConnectionString,
				 BufferLength,
				 StringLength2Ptr,
				 DriverCompletion,
				 0);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLDriverConnectA(SQLHDBC ConnectionHandle,
		  SQLHWND WindowHandle,
		  SQLCHAR *InConnectionString,
		  SQLSMALLINT StringLength1,
		  SQLCHAR *OutConnectionString,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLength2Ptr,
		  SQLUSMALLINT DriverCompletion)
{
	return SQLDriverConnect(ConnectionHandle,
				WindowHandle,
				InConnectionString,
				StringLength1,
				OutConnectionString,
				BufferLength,
				StringLength2Ptr,
				DriverCompletion);
}

SQLRETURN SQL_API
SQLDriverConnectW(SQLHDBC ConnectionHandle,
		  SQLHWND WindowHandle,
		  SQLWCHAR *InConnectionString,
		  SQLSMALLINT StringLength1,
		  SQLWCHAR *OutConnectionString,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLength2Ptr,
		  SQLUSMALLINT DriverCompletion)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;
	SQLCHAR *in = NULL, *out;
	SQLSMALLINT n;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDriverConnectW " PTRFMT " ", PTRFMTCAST ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(InConnectionString, StringLength1, SQLCHAR, in,
		   addDbcError, dbc, return SQL_ERROR);

	rc = SQLDriverConnect_(dbc, WindowHandle, in, SQL_NTS, NULL, 0, &n,
			       DriverCompletion, 1);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearDbcErrors(dbc);
	n++;			/* account for NUL byte */
	out = malloc(n);
	rc = SQLDriverConnect_(dbc, WindowHandle, in, SQL_NTS, out, n, &n,
			       DriverCompletion, 0);
	fixWcharOut(rc, out, n, OutConnectionString, BufferLength,
		    StringLength2Ptr, 1, addDbcError, dbc);
	if (in)
		free(in);
	return rc;
}
#endif /* WITH_WCHAR */
