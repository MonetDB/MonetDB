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
SQLConnect_(SQLHDBC hDbc, SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength,
	    SQLCHAR *szUID, SQLSMALLINT nUIDLength, SQLCHAR *szPWD,
	    SQLSMALLINT nPWDLength)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;
	SQLRETURN rc = SQL_SUCCESS;
	char *dsn = NULL;
	char *uid = NULL;
	char *pwd = NULL;
	char *schema = NULL;
	int port = 0;
	char *s;
	Mapi mid;

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check connection state, should not be connected */
	if (dbc->Connected == 1) {
		/* 08002 = Connection already in use */
		addDbcError(dbc, "08002", NULL, 0);
		return SQL_ERROR;
	}
	assert(dbc->Connected == 0);

	/* convert input string parameters to normal null terminated C strings */
	fixODBCstring(szDataSource, nDataSourceLength, addDbcError, dbc);
	if (nDataSourceLength == 0) {
		szDataSource = (SQLCHAR*)"Default";
		nDataSourceLength = strlen((char*)szDataSource);
	}
	dsn = dupODBCstring(szDataSource, nDataSourceLength);
	/* for now we only allow the default data source */
	if (strcasecmp(dsn, "default") != 0) {
		free(dsn);
		addDbcError(dbc, "IM002", NULL, 0);
		return SQL_ERROR;
	}

	fixODBCstring(szUID, nUIDLength, addDbcError, dbc);
	if (nUIDLength == 0) {
		uid = strdup(mo_find_option(NULL, 0, "sql_user"));
	} else {
		uid = dupODBCstring(szUID, nUIDLength);
	}
	fixODBCstring(szPWD, nPWDLength, addDbcError, dbc);
	if (nPWDLength == 0) {
		pwd = mo_find_option(NULL, 0, "sql_passwd");
		if (pwd == NULL)
			pwd = "monetdb";
		pwd = strdup(pwd);
	} else {
		pwd = dupODBCstring(szPWD, nPWDLength);
	}

	s = mo_find_option(NULL, 0, "sql_port");
	port = atoi(s);

	/* TODO: get and use a database name */

	/* Retrieved and checked the arguments.
	   Now try to open a connection with the server */
	/* temporarily hold hostname in s */
	s = mo_find_option(NULL, 0, "host");

#ifdef ODBCDEBUG
	ODBCLOG("SQLConnect: DSN=%s UID=%s PWD=%s port=%d\n", dsn, uid, pwd, port);
#endif

	/* connect to a server on host via port */
	mid = mapi_connect(s, port, uid, pwd, "sql");
	if (mid == NULL || mapi_error(mid)) {
		/* 08001 = Client unable to establish connection */
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
		if (dbc->DSN != NULL)
			free(dbc->DSN);
		dbc->DSN = dsn;
		if (dbc->UID != NULL)
			free(dbc->UID);
		dbc->UID = uid;
		if (dbc->PWD != NULL)
			free(dbc->PWD);
		dbc->PWD = pwd;
		if (dbc->DBNAME != NULL)
			free(dbc->DBNAME);
		dbc->DBNAME = schema;
		mapi_setAutocommit(mid, dbc->autocommit);
	}

	return rc;
}

SQLRETURN
SQLConnect(SQLHDBC hDbc, SQLCHAR *szDataSource, SQLSMALLINT nDataSourceLength,
	   SQLCHAR *szUID, SQLSMALLINT nUIDLength, SQLCHAR *szPWD,
	   SQLSMALLINT nPWDLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLConnect\n");
#endif

	return SQLConnect_(hDbc, szDataSource, nDataSourceLength,
			   szUID, nUIDLength, szPWD, nPWDLength);
}
