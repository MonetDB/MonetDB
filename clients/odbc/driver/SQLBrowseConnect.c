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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"
#include "ODBCAttrs.h"
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* strcasecmp */
#endif

#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#endif

#ifndef HAVE_SQLGETPRIVATEPROFILESTRING
#define SQLGetPrivateProfileString(section,entry,default,buffer,bufferlen,filename)	((int) strcpy_len(buffer,default,bufferlen))
#endif

static void
suggest_settings(ODBCDbc *dbc, char **buf, size_t *pos, size_t *cap, char touched_as, const char *prefix)
{
	for (int i = 0; i < attr_setting_count; i++) {
		const struct attr_setting *entry = &attr_settings[i];
		mparm parm = entry->parm;
		if (dbc->setting_touched[(int)parm] == touched_as) {
			const char *sep = *pos > 0 ? ";" : "";
			const char *values = entry->suggest_values ? entry->suggest_values : "?";
			reallocprintf(
				buf, pos, cap,
				"%s%s%s:%s=%s",
				sep, prefix, entry->name, entry->alt_name, values);
		}
	}
}



static SQLRETURN
MNDBBrowseConnect(ODBCDbc *dbc,
		  const SQLCHAR *InConnectionString,
		  SQLSMALLINT StringLength1,
		  SQLCHAR *OutConnectionString,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLength2Ptr)
{
	SQLRETURN rc;

	rc = MNDBDriverConnect(
		dbc, NULL,
		InConnectionString, StringLength1,
		OutConnectionString, BufferLength, StringLength2Ptr,
		SQL_DRIVER_NOPROMPT,
		0
	);

	if (SQL_SUCCEEDED(rc)) {
		return rc;
	}

	// 0 = never touched, show it
	// 1 = touched, do not show it
	// 2 = show as mandatory

	if (dbc->setting_touched[MP_USER] != 1)
		dbc->setting_touched[MP_USER] = 2;
	if (dbc->setting_touched[MP_PASSWORD] != 1)
		dbc->setting_touched[MP_PASSWORD] = 2;

	// Make MP_DATABASE mandatory if monetdbd asks for it.
	for (ODBCError *err = dbc->Error; err != NULL; err = getErrorRec(err, 2)) {
		if (strcmp("08001", getSqlState(err)) != 0)
			continue;
		if (strstr(getMessage(err), "monetdbd: please specify a database") == NULL)
			continue;
		dbc->setting_touched[MP_DATABASE] = 2;
	}

	char *buf = NULL;
	size_t pos = 0;
	size_t cap = 0;
	suggest_settings(dbc, &buf, &pos, &cap, 2, "");    // mandatory first
	suggest_settings(dbc, &buf, &pos, &cap, 0, "*");   // then optional

	if (buf && pos) {
		size_t n = strcpy_len((char*)OutConnectionString, buf, BufferLength);
		if (StringLength2Ptr)
			*StringLength2Ptr = (SQLSMALLINT)n;
	}

	free(buf);
	clearDbcErrors(dbc);
	return SQL_NEED_DATA;
}

SQLRETURN SQL_API
SQLBrowseConnect(SQLHDBC ConnectionHandle,
		 SQLCHAR *InConnectionString,
		 SQLSMALLINT StringLength1,
		 SQLCHAR *OutConnectionString,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLength2Ptr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLBrowseConnect %p", ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return MNDBBrowseConnect(dbc, InConnectionString, StringLength1, OutConnectionString, BufferLength, StringLength2Ptr);
}

SQLRETURN SQL_API
SQLBrowseConnectA(SQLHDBC ConnectionHandle,
		  SQLCHAR *InConnectionString,
		  SQLSMALLINT StringLength1,
		  SQLCHAR *OutConnectionString,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLength2Ptr)
{
	return SQLBrowseConnect(ConnectionHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength, StringLength2Ptr);
}

SQLRETURN SQL_API
SQLBrowseConnectW(SQLHDBC ConnectionHandle,
		  SQLWCHAR *InConnectionString,
		  SQLSMALLINT StringLength1,
		  SQLWCHAR *OutConnectionString,
		  SQLSMALLINT BufferLength,
		  SQLSMALLINT *StringLength2Ptr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;
	SQLCHAR *in = NULL, *out;
	SQLSMALLINT n;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLBrowseConnectW %p", ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	fixWcharIn(InConnectionString, StringLength1, SQLCHAR, in,
		   addDbcError, dbc, return SQL_ERROR);
	out = malloc(2048);
	if (out == NULL) {
		/* Memory allocation error */
		addDbcError(dbc, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	rc = MNDBBrowseConnect(dbc, in, SQL_NTS, out, 2048, &n);
	if (SQL_SUCCEEDED(rc) || rc == SQL_NEED_DATA) {
		fixWcharOut(rc, out, n, OutConnectionString, BufferLength,
			    StringLength2Ptr, 1, addDbcError, dbc);
	}
	free(out);
	if (in)
		free(in);
	return rc;
}
