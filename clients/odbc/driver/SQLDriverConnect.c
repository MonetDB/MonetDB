/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

/*
 * This code was created by Peter Harvey (mostly during Christmas 98/99).
 * This code is LGPL. Please ensure that this message remains in future
 * distributions and uses of this code (that's about all I get out of it).
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
#include "ODBCAttrs.h"
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strcasecmp */
#else
#include <string.h>
#endif

int
ODBCGetKeyAttr(const SQLCHAR **conn, SQLSMALLINT *nconn, char **key, char **attr)
{
	const SQLCHAR *p;
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
	if (*key == NULL)
		return -1;
	strcpy_len(*key, (char *) p, len + 1);
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
		if (*attr == NULL) {
			free(*key);
			*key = NULL;
			return -1;
		}
		strcpy_len(*attr, (char *) p, len + 1);
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
		if (*attr == NULL) {
			free(*key);
			*key = NULL;
			return -1;
		}
		strcpy_len(*attr, (char *) p, len + 1);
	}
	if (*nconn > 0 && **conn) {
		(*conn)++;
		(*nconn)--;
	}
	return 1;
}

#ifdef ODBCDEBUG
static char *
translateDriverCompletion(SQLUSMALLINT DriverCompletion)
{
	switch (DriverCompletion) {
	case SQL_DRIVER_PROMPT:
		return "SQL_DRIVER_PROMPT";
	case SQL_DRIVER_COMPLETE:
		return "SQL_DRIVER_COMPLETE";
	case SQL_DRIVER_COMPLETE_REQUIRED:
		return "SQL_DRIVER_COMPLETE_REQUIRED";
	case SQL_DRIVER_NOPROMPT:
		return "SQL_DRIVER_NOPROMPT";
	default:
		return "unknown";
	}
}
#endif

SQLRETURN
MNDBDriverConnect(ODBCDbc *dbc,
		  SQLHWND WindowHandle,
		  const SQLCHAR *InConnectionString,
		  SQLSMALLINT StringLength1,
		  SQLCHAR *OutConnectionString,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLength2Ptr,
		  SQLUSMALLINT DriverCompletion,
		  int tryOnly)
{
	(void) WindowHandle;

	SQLRETURN rc = SQL_SUCCESS;
	const char *sqlstate = NULL;
	size_t out_len;
	const char *scratch_no_alloc;

	// These will be free'd at the end label
	msettings *settings = NULL;
	char *scratch_alloc = NULL;
	char *dsn = NULL;

	/* check connection state, should not be connected */
	if (dbc->Connected) {
		sqlstate = "08002";
		goto failure;
	}

	fixODBCstring(InConnectionString, StringLength1, SQLSMALLINT,
		      addDbcError, dbc, return SQL_ERROR);

	settings = msettings_clone(dbc->settings);
	if (!settings)
		goto failure;

#ifdef ODBCDEBUG
	ODBCLOG("\"%.*s\" %s\n", StringLength1,
		(char *) InConnectionString,
		translateDriverCompletion(DriverCompletion));
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
		sqlstate = "HY092";
		goto failure;
	}

	rc = takeFromConnString(dbc, settings, InConnectionString, StringLength1, &dsn);
	if (!SQL_SUCCEEDED(rc))
		goto end;

	if (!msettings_validate(settings, &scratch_alloc)) {
		addDbcError(dbc, "HY009", scratch_alloc, 0);
		rc = SQL_ERROR;
		goto end;
	}

	// Build a connect string for the current connection and put it in the out buffer.
	scratch_alloc = buildConnectionString(dsn ? dsn : "DEFAULT", settings);
	if (!scratch_alloc)
		goto failure;
	out_len = strcpy_len((char*)OutConnectionString, scratch_alloc, BufferLength);
	if (StringLength2Ptr)
		*StringLength2Ptr = (SQLSMALLINT)out_len;
	if (out_len + 1 > (size_t)BufferLength) {
		addDbcError(dbc, "01004", NULL, 0);
		rc = SQL_SUCCESS_WITH_INFO;
	}

	if (tryOnly) {
		assert(sqlstate == NULL);
		goto end;
	}

	scratch_no_alloc = msetting_string(settings, MP_LOGFILE);
	if (*scratch_no_alloc)
		setODBCdebug(scratch_no_alloc, false);

	rc = MNDBConnectSettings(dbc, dsn, settings);
	settings = NULL; // do not free now

	// always go to end, MNDBConnectSettings has already logged any failures
	goto end;

failure:
	if (sqlstate == NULL)
		sqlstate = "HY001"; // malloc failure
	rc = SQL_ERROR;
	// fallthrough
end:
	if (sqlstate != NULL)
		addDbcError(dbc, sqlstate, NULL, 0);
	msettings_destroy(settings);
	free(scratch_alloc);
	free(dsn);
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
	ODBCLOG("SQLDriverConnect %p ", ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return MNDBDriverConnect(dbc,
				 WindowHandle,
				 InConnectionString,
				 StringLength1,
				 OutConnectionString,
				 BufferLength,
				 StringLength2Ptr,
				 DriverCompletion,
				 0);
}

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
	ODBCLOG("SQLDriverConnectW %p ", ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(InConnectionString, StringLength1, SQLCHAR, in,
		   addDbcError, dbc, return SQL_ERROR);

	rc = MNDBDriverConnect(dbc, WindowHandle, in, SQL_NTS, NULL, 0, &n,
			       DriverCompletion, 1);  // 1 = Try Only
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearDbcErrors(dbc);
	n++;			/* account for NUL byte */
	out = malloc(n);
	if (out == NULL) {
		/* Memory allocation error */
		addDbcError(dbc, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	rc = MNDBDriverConnect(dbc, WindowHandle, in, SQL_NTS, out, n, &n,
			       DriverCompletion, 0);
	if (SQL_SUCCEEDED(rc)) {
		fixWcharOut(rc, out, n, OutConnectionString, BufferLength,
			    StringLength2Ptr, 1, addDbcError, dbc);
	}
	free(out);
	if (in)
		free(in);
	return rc;
}
