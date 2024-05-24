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
#include "ODBCAttrs.h"
#include <time.h>
#include "msettings.h"
#include "mstring.h"

#ifdef HAVE_ODBCINST_H
#include <odbcinst.h>
#endif

#ifndef HAVE_SQLGETPRIVATEPROFILESTRING
#define SQLGetPrivateProfileString(section,entry,default,buffer,bufferlen,filename)	((int) strcpy_len(buffer,default,bufferlen))
#endif

#define SUGGEST_BOOLEAN "{True,False}"

const struct attr_setting attr_settings[] = {
	{ "UID", "User", MP_USER },
	{ "PWD", "Password", MP_PASSWORD },
	{ "DATABASE", "Database", MP_DATABASE },
	{ "PORT", "Port", MP_PORT },
	{ "HOST", "Server", MP_HOST },
	{ "SOCK", "Unix Socket", MP_SOCK },
	{ "TLS", "Encrypt", MP_TLS, .suggest_values = SUGGEST_BOOLEAN },
	{ "CERT", "Server Certificate", MP_CERT },
	{ "CERTHASH", "Server Certificate Hash", MP_CERTHASH },
	{ "CLIENTKEY", "Client Key", MP_CLIENTKEY },
	{ "CLIENTCERT", "Client Certificate", MP_CLIENTCERT },
	{ "AUTOCOMMIT", "Autocommit", MP_AUTOCOMMIT, .suggest_values = SUGGEST_BOOLEAN },
	{ "SCHEMA", "Schema", MP_SCHEMA },
	{ "TIMEZONE", "Time Zone", MP_TIMEZONE },
	{ "REPLYSIZE", "Reply Size", MP_REPLYSIZE },
	{ "LOGFILE", "Log File", MP_LOGFILE },
	{ "LOGINTIMEOUT", "Login Timeout", MP_CONNECT_TIMEOUT},
	{ "CONNECTIONTIMEOUT", "Connection Timeout", MP_REPLY_TIMEOUT},
};

const int attr_setting_count = sizeof(attr_settings) / sizeof(attr_settings[0]);


int
attr_setting_lookup(const char *attr_name, bool allow_alt_name)
{
	for (int i = 0; i < attr_setting_count; i++) {
		const struct attr_setting *entry = &attr_settings[i];
		if (strcasecmp(attr_name, entry->name) == 0)
			return i;
		if (allow_alt_name && entry->alt_name && strcasecmp(attr_name, entry->alt_name) == 0)
			return i;
	}
	return -1;
}


static SQLRETURN
get_serverinfo(ODBCDbc *dbc)
{
	MapiHdl hdl = NULL;
	SQLRETURN rc; // intentionally uninitialized
	char *n, *v;

	if ((hdl = mapi_query(dbc->mid, "select name, value from sys.env() where name in ('monet_version', 'gdk_dbname', 'max_clients', 'raw_strings')")) == NULL)
		goto end;
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
			msetting_set_string(dbc->settings, MP_DATABASE, v);
		}
	}
	if (mapi_error(dbc->mid))
		goto end;
	mapi_close_handle(hdl);
	if ((hdl = mapi_query(dbc->mid, "select id from sys._tables where name = 'comments' and schema_id = (select id from sys.schemas where name = 'sys')")) == NULL)
		goto end;
	if (mapi_error(dbc->mid))
		goto end;
	n = NULL;
	while (mapi_fetch_row(hdl)) {
		n = mapi_fetch_field(hdl, 0);
	}
	dbc->has_comment = n != NULL;

	rc = SQL_SUCCESS;
end:
	if (mapi_error(dbc->mid)) {
		addDbcError(dbc, "08001", mapi_error_str(dbc->mid), 0);
		rc = SQL_ERROR;
	}
	mapi_close_handle(hdl);
	return rc;
}


// Ensure '*argument' is either NULL or a NUL-terminated string,
// taking into account 'argument_len' being either a proper string length
// or one of the special values SQL_NULL_DATA or SQL_NTS.
//
// Return 'true' on success and 'false' on allocation failure.
//
// If memory needs to be allocated and 'scratch' is not NULL,
// a pointer to the allocated memory will be stored in '*scratch'
// and the previous value of '*scratch' will be free'd.
//
// '*argument' is never free'd.
bool
makeNulTerminated(const SQLCHAR **argument, ssize_t argument_len, void **scratch)
{
	assert(argument != NULL);

	if (*argument == NULL || argument_len == SQL_NTS)
		return true;
	if (argument_len == SQL_NULL_DATA) {
		*argument = NULL;
		return true;
	}

	SQLCHAR *value = malloc(argument_len + 1);
	if (value == NULL)
		return false;
	memmove(value, argument, argument_len);
	value[argument_len] = '\0';

	*argument = value;
	if (scratch) {
		free(*scratch);
		*scratch = value;
	}

	return value;
}

char*
buildConnectionString(const char *dsn, const msettings *settings)
{

	size_t pos = 0;
	size_t cap = 1024;
	char *buf = malloc(cap);  // reallocprintf will deal with allocation failures
	char *sep = "";
	char *value = NULL;
	char *default_value = NULL;
	bool ok = false;

	if (dsn) {
		if (reallocprintf(&buf, &pos, &cap, "DSN=%s", dsn) < 0)
			goto end;
		sep = ";";
	}

	for (int i = 0; i < attr_setting_count; i++) {
		const struct attr_setting *entry = &attr_settings[i];
		mparm parm = entry->parm;

		if (parm == MP_IGNORE || parm == MP_TABLE || parm == MP_TABLESCHEMA)
			continue;

		free(value);
		value = msetting_as_string(settings, parm);
		if (!value)
			goto end;

		bool show_this;  // intentionally uninitialized
		if (parm == MP_USER || parm == MP_PASSWORD) {
			show_this = true;
		} else if (parm == MP_PORT && msetting_long(settings, MP_PORT) <= 0) {
			show_this = false;
		} else if (parm == MP_TLS) {
			show_this = msetting_bool(settings, MP_TLS);
		} else if (mparm_is_core(parm)) {
			show_this = true;
		} else {
			// skip if still default
			free(default_value);
			default_value = msetting_as_string(msettings_default, parm);
			if (!default_value)
				goto end;
			show_this = (strcmp(value, default_value) != 0);
		}
		if (show_this) {
			if (reallocprintf(&buf, &pos, &cap, "%s%s=%s", sep, entry->name, value) < 0)
				goto end;
			sep = ";";
		}
	}

	ok = true;

end:
	free(value);
	free(default_value);
	if (ok) {
		return buf;
	} else {
		free(buf);
		return NULL;
	}
}

static int
lookup(const char *dsn, const struct attr_setting *entry, char *buf, int bufsize)
{
	int n;
	assert(entry->name);
	n = SQLGetPrivateProfileString(dsn, entry->name, "", buf, bufsize, "odbc.ini");
	if (n > 0)
		return n;
	if (entry->alt_name)
		n = SQLGetPrivateProfileString(dsn, entry->alt_name, "", buf, bufsize, "odbc.ini");
	return n;
}

const char*
takeFromDataSource(ODBCDbc *dbc, msettings *settings, const char *dsn)
{
	char buf[1024] = { 0 };

	for (int i = 0; i < attr_setting_count; i++) {
		const struct attr_setting *entry = &attr_settings[i];
		mparm parm = entry->parm;
		int n = lookup(dsn, entry, buf, sizeof(buf));
		if (n > 0) {
			if (sizeof(buf) - n <= 1)
				return "01004"; // truncated
			const char *msg = msetting_parse(settings, parm, buf);
			if (msg != NULL)
				return msg;
			dbc->setting_touched[(int)parm] = 1;
		}
	}

	return NULL;
}

SQLRETURN
takeFromConnString(
	ODBCDbc *dbc,
	msettings *settings,
	const SQLCHAR *InConnectionString,
	SQLSMALLINT StringLength1,
	char **dsn_out)
{
	SQLRETURN rc = SQL_SUCCESS;
	const char *sqlstate = NULL;
	const char *sql_explanation = NULL;
	const SQLCHAR *cursor;
	SQLSMALLINT n;
	char *dsn = NULL, *key = NULL, *attr = NULL;

	// figure out the DSN and load its settings
	cursor = InConnectionString;
	n = StringLength1;
	while (ODBCGetKeyAttr(&cursor, &n, &key, &attr) > 0) {
		if (strcasecmp(key, "dsn") == 0) {
			dsn = attr;
			free(key);
			break;
		}
		free(key);
		free(attr);
	}
	key = NULL;
	attr = NULL;
	if (dsn) {
		if (strlen(dsn) > SQL_MAX_DSN_LENGTH)
			sqlstate = "IM010";    // Data source name too long
		else
			sqlstate = takeFromDataSource(dbc, settings, dsn);
	}
	if (sqlstate)
		goto end;

	// Override with settings from the connect string itself
	cursor = InConnectionString;
	n = StringLength1;
	while (ODBCGetKeyAttr(&cursor, &n, &key, &attr) > 0) {
		int i = attr_setting_lookup(key, true);
		if (i >= 0) {
			mparm parm = attr_settings[i].parm;
			sql_explanation = msetting_parse(settings, parm, attr);
			if (sql_explanation)
				goto end;
			dbc->setting_touched[(int)parm] = 1;
		}
		free(key);
		free(attr);
	}
	key = NULL;
	attr = NULL;

	if (dsn && dsn_out) {
		*dsn_out = dsn;
		dsn = NULL;
	}

end:
	if (sql_explanation && !sqlstate)
		sqlstate = "HY009";
	if (sqlstate) {
		addDbcError(dbc, sqlstate, sql_explanation, 0);
		rc = SQL_ERROR;
	}
	free(key);
	free(attr);
	free(dsn);
	return rc;
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
	Mapi mid = NULL;
	msettings *settings = NULL;
	void *scratch = NULL;

	// These do not need to be free'd
	const char *mapiport_env;

	// Check connection state, should not be connected
	if (dbc->Connected) {
		error_state = "08002";
		goto failure;
	}

	// Modify a copy so the original remains unchanged when we return an error
	settings = msettings_clone(dbc->settings);
	if (settings == NULL)
		goto failure;

	// ServerName is really the Data Source name
	if (!makeNulTerminated(&ServerName, NameLength1, &scratch))
		goto failure;
	if (ServerName != NULL) {
		dsn = strdup((char*)ServerName);
		if (dsn == NULL)
			goto failure;
	}

	// data source settings take precedence over existing ones
	if (dsn && *dsn) {
		error_state = takeFromDataSource(dbc, settings, dsn);
		if (error_state != NULL)
			goto failure;
	}

#ifdef ODBCDEBUG
	if (ODBCdebug == NULL || *ODBCdebug == 0) {
		const char *logfile = msetting_string(settings, MP_LOGFILE);
		if (*logfile)
			setODBCdebug(logfile, true);
	}
#endif

	// The dedicated parameters for user name, password, host, port and database name
	// override the pre-existing values and whatever came from the data source.
	// We also take the MAPIPORT environment variable into account.

	if (!makeNulTerminated(&UserName, NameLength2, &scratch))
		goto failure;
	if (UserName) {
		if (!*UserName) {
			error_state = "28000";
			error_explanation = "user name not set";
			goto failure;
		}
		error_explanation = msetting_set_string(settings, MP_USER, (char*)UserName);
		if (error_explanation != NULL)
			goto failure;
	}

	if (!makeNulTerminated(&Authentication, NameLength3, &scratch))
		goto failure;
	if (Authentication) {
		if (!*Authentication) {
			error_state = "28000";
			error_explanation = "password not set";
			goto failure;
		}
		error_explanation = msetting_set_string(settings, MP_PASSWORD, (char*)Authentication);
		if (error_explanation != NULL)
			goto failure;
	}

	if (host != NULL) {
		error_explanation = msetting_set_string(settings, MP_HOST, host);
		if (error_explanation != NULL)
			goto failure;
	}

	mapiport_env = getenv("MAPIPORT");
	if (port > 0)
		error_explanation = msetting_set_long(settings, MP_PORT, port);
	else if (mapiport_env != NULL)
		error_explanation = msetting_parse(settings, MP_PORT, mapiport_env);
	if (error_explanation != NULL)
		goto failure;

	if (dbname != NULL) {
		error_explanation = msetting_set_string(settings, MP_DATABASE, dbname);
		if (error_explanation != NULL)
			goto failure;
	}

	error_explanation = msetting_set_long(settings, MP_MAPTOLONGVARCHAR, mapToLongVarchar);
	if (error_explanation)
		goto failure;

#ifdef ODBCDEBUG
	{
		free(scratch);
		char *connstring = scratch = buildConnectionString(dsn, settings);
		if (!connstring)
			goto failure;
		ODBCLOG("SQLConnect: %s\n", connstring);
	}
#endif

	assert(error_state == NULL);
	assert(error_explanation == NULL);

	SQLRETURN ret;

	ret = MNDBConnectSettings(dbc, dsn, settings);
	settings = NULL; // must not be free'd now

	goto end;

failure:
	if (error_state == NULL) {
		if (error_explanation == NULL || msettings_malloc_failed(error_explanation))
			error_state = "HY001"; // allocation failure
		else
			error_state = "HY009"; // invalid argument
	}
	addDbcError(dbc, error_state, error_explanation, 0);
	ret = SQL_ERROR;

	// fallthrough
end:
	free(dsn);
	free(scratch);
	if (mid)
		mapi_destroy(mid);
	msettings_destroy(settings);

	return ret;
}

SQLRETURN
MNDBConnectSettings(ODBCDbc *dbc, const char *dsn, msettings *settings)
{
	SQLRETURN rc;
	msettings *clone = msettings_clone(settings);

	if (clone == NULL) {
		addDbcError(dbc, "HY001", NULL, 0);
		return SQL_ERROR;
	}

	Mapi mid = mapi_settings(settings);
	if (mid) {
		settings = NULL; // will be free'd as part of 'mid' now
		mapi_setAutocommit(mid, dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON);
		mapi_set_size_header(mid, true);
		mapi_reconnect(mid);
	}
	if (mid == NULL || mapi_error(mid)) {
		const char *error_state = "08001";
		const char *error_explanation = mid ? mapi_error_str(mid) : NULL;
		addDbcError(dbc, error_state, error_explanation, 0);
		if (mid)
			mapi_destroy(mid);
		msettings_destroy(settings);
		msettings_destroy(clone);
		return SQL_ERROR;
	}

	free(dbc->dsn);
	dbc->dsn = dsn ? strdup(dsn) : NULL;

	if (dbc->mid)
		mapi_destroy(dbc->mid);
	dbc->mid = mid;

	msettings_destroy(dbc->settings);
	dbc->settings = clone;

	dbc->mapToLongVarchar = msetting_long(dbc->settings, MP_MAPTOLONGVARCHAR);

	dbc->Connected = true;

	rc = get_serverinfo(dbc);
	if (!SQL_SUCCEEDED(rc))
		return rc;

	return SQL_SUCCESS;
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
