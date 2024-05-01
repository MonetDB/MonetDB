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
#include "msettings.h"
#include "mstring.h"

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

// Return a newly allocated NUL-terminated config value from either the argument
// or the data source. Return 'default_value' if no value can be found, NULL on
// allocation error.
//
// If non-NULL, parameter 'argument' points to an argument that may or may not
// be NUL-terminated. The length parameter 'argument_len' can either be the
// length of the argument or one of the following special values:
//
//    SQL_NULL_DATA: consider the argument NULL
//    SQL_NTS:       the argument is actually NUL-terminated
//
// Parameters 'dsn' and 'entry', if not NULL and not empty, indicate which data
// source field to look up in "odbc.ini".
static char*
getConfig(
	const void *argument, ssize_t argument_len,
	const char *dsn, const char *entry,
	const char *default_value)
{
	if (argument != NULL && argument_len != SQL_NULL_DATA) {
		// argument is present..
		if (argument_len == SQL_NTS) {
			// .. and it's already NUL-terminated
			return strdup((const char*)argument);
		} else {
			// .. but we need to create a NUL-terminated copy
			char *value = malloc(argument_len + 1);
			if (value == NULL)
				return NULL;
			memmove(value, argument, argument_len);
			value[argument_len] = '\0';
			return value;
		}
	} else if (dsn && *dsn && entry && *entry) {
		// look up in the data source
		size_t size = 1024; // should be plenty
		char *buffer = malloc(size);
		if (buffer == NULL)
			return NULL;
		int n = SQLGetPrivateProfileString(dsn, entry, "", buffer, size, "odbc.ini");
		if (n > 0) {
			// found some
			return buffer;
		} else {
			// found none
			free(buffer);
			return strdup(default_value);
		}
	} else {
		return strdup(default_value);
	}
}

// Helper function for use in MNDBConnect.
// Try to set the setting from a data source field, return false on error.
static bool
ds_setting(msettings *settings, const char *dsn, const char **err_state, const char **explanation, mparm parm, const char *entry)
{
	assert(*err_state == NULL);
	assert(*explanation == NULL);

	char *value = getConfig(NULL, 0, dsn, entry, "");
	if (value == NULL)
		return false; // allocation failed
	if (*value == '\0') {
		free(value);
		return true; // nothing to do
	}

	msettings_error err = msetting_parse(settings, parm, value);
	free(value);
	if (!err)
		return true;
	if (!msettings_malloc_failed(err)) {
		*err_state = "HY009"; // invalid argument
		*explanation = err;
	}
	return false;
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
	    const char *dbname,
	    int mapToLongVarchar)
{
	// These will be passed to addDbcError if you 'goto failure'.
	// If unset, 'goto failure' will assume an allocation error.
	const char *error_state = NULL;
	const char *error_explanation = NULL;

	// These will be free'd / destroyed at the 'end' label at the bottom of this function
	char *dsn = NULL;
	char *uid = NULL;
	char *pwd = NULL;
	char *db = NULL;
	char *hostdup = NULL;
	char *portdup = NULL;
	char *logbuf = NULL;
	Mapi mid = NULL;
	msettings *settings = NULL;

	// These do not need to be free'd
	const char *mapiport_env;

	/* check connection state, should not be connected */
	if (dbc->Connected) {
		error_state = "08002";
		goto failure;
	}

	dsn = getConfig(ServerName, NameLength1, NULL, NULL, "");
	if (dsn == NULL)
		goto failure;

#ifdef ODBCDEBUG
	if ((ODBCdebug == NULL || *ODBCdebug == 0) && dsn && *dsn) {
		char logfile[2048];
		int n = SQLGetPrivateProfileString(dsn, "logfile", "",
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

	uid = getConfig(UserName, NameLength2, dsn, "uid", "monetdb");
	if (uid == NULL)
		goto failure;
	if (*uid == '\0') {
		error_state = "28000";
		error_explanation = "user name not set";
		goto failure;
	}

	pwd = getConfig(Authentication, NameLength3, dsn, "pwd", "monetdb");
	if (pwd == NULL)
		goto failure;
	if (*pwd == '\0') {
		error_state = "28000";
		error_explanation = "password not set";
		goto failure;
	}

	// In the old code, the dbname precedence was:
	// 1. 'dbname' parameter
	// 2. existing database name
	// 3. database name from data source
	//
	// That seemed odd, so now it's
	// 1. 'dbname' parameter
	// 2. database name from data source
	// 3. existing database name
	db = getConfig(dbname, SQL_NTS, dsn, "database", dbc->dbname ? dbc->dbname : "");
	if (db == NULL)
		goto failure;

	// In the old code we had Windows-specific code that
	// ran _wgetenv(L"MAPIPORT").
	// However, even on Windows getenv() is probably fine for a variable that's
	// supposed to only hold digits.
	mapiport_env = getenv("MAPIPORT");

	// Port precedence:
	// 2. 'port' parameter
	// 1. MAPIPORT env var
	// 3. data source
	// 4. MAPI_PORT_STR ("50000")
	if (port == 0) {
		portdup = getConfig(mapiport_env, SQL_NTS, dsn, "port", MAPI_PORT_STR);
		if (portdup == NULL)
			goto failure;
		char *end;
		long longport = strtol(portdup, &end, 10);
		if (*portdup == '\0' || *end != '\0' || longport < 1 || longport > 65535) {
			error_state = "HY009"; // invalid argument
			error_explanation = mapiport_env != NULL
				? "invalid port setting in MAPIPORT environment variable"
				: "invalid port setting in data source";
			goto failure;
		}
		port = longport;
	}

	hostdup = getConfig(host, SQL_NTS, dsn, "host", "localhost");
	if (hostdup == NULL)
		goto failure;

	settings = msettings_create();
	// Move the currently known parameters into the settings object.
	if (false
		|| (error_explanation = msetting_set_string(settings, MP_DATABASE, db))
		|| (error_explanation = msetting_set_string(settings, MP_HOST, hostdup))
		|| (error_explanation = msetting_set_long(settings, MP_PORT, port))
		|| (error_explanation = msetting_set_string(settings, MP_USER, uid))
		|| (error_explanation = msetting_set_string(settings, MP_PASSWORD, pwd))
	) {
		if (msettings_malloc_failed(error_explanation))
			error_explanation = NULL; // it's a malloc failure
		else
			error_state = "HY009";   // it's otherwise invalid
		goto failure;
	}

	// The other parameters can only be set from the data source.
	// We have made a helper function for that.
	if (false
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_SOCK, "Unix Socket")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_TLS, "Encrypt")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_CERT, "Server Certificate")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_CERTHASH, "Server Certificate Hash")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_CLIENTKEY, "Client Key")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_CLIENTCERT, "Client Certificate")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_AUTOCOMMIT, "Autocommit")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_SCHEMA, "Schema")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_TIMEZONE, "Time Zone")
		|| !ds_setting(settings, dsn, &error_state, &error_explanation, MP_REPLYSIZE, "Reply Size")

	) {
		goto failure;
	}

#ifdef ODBCDEBUG
	{
		size_t pos = 0;
		size_t cap = 1024;
		reallocprintf(&logbuf, &pos, &cap, "SQLConnect: DSN=%s", dsn);
		mparm parm;
		for (int i = 0; (parm = mparm_enumerate(i)) != MP_UNKNOWN ; i++) {
			if (parm == MP_TABLE || parm == MP_TABLESCHEMA)
				continue;
			char *value = msetting_as_string(settings, parm);
			char *default_value = msetting_as_string(msettings_default, parm);
			if (!value || !default_value)
				goto failure;
			if (mparm_is_core(parm) || strcmp(value, default_value) != 0) {
				reallocprintf(&logbuf, &pos, &cap, ", %s=%s", mparm_name(parm), value);
			}
			free(value);
			free(default_value);
		}
		if (pos > cap)
			goto failure;
		ODBCLOG("%s\n", logbuf);
	}
#endif

	// Create mid and execute a bunch of commands before checking for errors.
	mid = mapi_settings(settings);
	if (mid) {
		settings = NULL; // it has moved into 'mid' and must not be freed.
		mapi_setAutocommit(mid, dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON);
		mapi_set_size_header(mid, true);
		mapi_reconnect(mid);
	}
	if (mid == NULL || mapi_error(mid)) {
		error_state = "08001";
		error_explanation = mid ? mapi_error_str(mid) : NULL;
		goto failure;
	}

	/* store internal information and clean up buffers */
	dbc->Connected = true;
	dbc->mapToLongVarchar = mapToLongVarchar;

	// Move strings into the dbc struct, clearing whatever was there
	// and leaving the original location NULL so they don't accidentally
	// get free'd.
	#define MOVE_CONF(free_, dst, src)  do { if (dst) free_(dst); dst = src; src = NULL;  } while (0)
	MOVE_CONF(mapi_destroy, dbc->mid, mid);
	MOVE_CONF(free, dbc->dsn, dsn);
	MOVE_CONF(free, dbc->uid, uid);
	MOVE_CONF(free, dbc->pwd, pwd);
	MOVE_CONF(free, dbc->host, hostdup);
	MOVE_CONF(free, dbc->dbname, db);

	get_serverinfo(dbc);
	/* set timeout after we're connected */
	mapi_timeout(dbc->mid, dbc->sql_attr_connection_timeout * 1000);

	assert(error_state == NULL);
	assert(error_explanation == NULL);
	goto end;

failure:
	if (error_state == NULL)
		error_state = "HY001";
	addDbcError(dbc, error_state, error_explanation, 0);
	// fallthrough

end:
	free(dsn);
	free(uid);
	free(pwd);
	free(db);
	free(hostdup);
	free(portdup);
	free(logbuf);
	if (mid)
		mapi_destroy(mid);
	msettings_destroy(settings);

	return error_state == NULL ? SQL_SUCCESS : SQL_ERROR;
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
			   NULL, 0, NULL, 0);
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
			 NULL, 0, NULL, 0);

      bailout:
	if (ds)
		free(ds);
	if (uid)
		free(uid);
	if (pwd)
		free(pwd);
	return rc;
}
