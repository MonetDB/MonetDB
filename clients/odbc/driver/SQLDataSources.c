/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 * SQLDataSources()
 * CLI Compliance: ISO 92
 *
 * Author: Sjoerd Mullender
 * Date  : 4 sep 2003
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
MNDBDataSources(ODBCEnv *env,
		SQLUSMALLINT Direction,
		SQLCHAR *ServerName,
		SQLSMALLINT BufferLength1,
		SQLSMALLINT *NameLength1,
		SQLCHAR *Description,
		SQLSMALLINT BufferLength2,
		SQLSMALLINT *NameLength2)
{
	(void) Direction;
	(void) ServerName;
	(void) BufferLength1;
	(void) NameLength1;
	(void) Description;
	(void) BufferLength2;
	(void) NameLength2;

	if (env->sql_attr_odbc_version == 0) {
		/* Function sequence error */
		addEnvError(env, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: implement the requested behavior */

	/* Driver does not support this function */
	addEnvError(env, "IM001", NULL, 0);
	return SQL_ERROR;
}

#ifdef ODBCDEBUG
static char *
translateDirection(SQLUSMALLINT Direction)
{
	switch (Direction) {
	case SQL_FETCH_NEXT:
		return "SQL_FETCH_NEXT";
	case SQL_FETCH_FIRST:
		return "SQL_FETCH_FIRST";
	case SQL_FETCH_FIRST_USER:
		return "SQL_FETCH_FIRST_USER";
	case SQL_FETCH_FIRST_SYSTEM:
		return "SQL_FETCH_FIRST_SYSTEM";
	default:
		return "unknown";
	}
}
#endif

SQLRETURN SQL_API
SQLDataSources(SQLHENV EnvironmentHandle,
	       SQLUSMALLINT Direction,
	       SQLCHAR *ServerName,
	       SQLSMALLINT BufferLength1,
	       SQLSMALLINT *NameLength1,
	       SQLCHAR *Description,
	       SQLSMALLINT BufferLength2,
	       SQLSMALLINT *NameLength2)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDataSources " PTRFMT " %s\n",
		PTRFMTCAST EnvironmentHandle, translateDirection(Direction));
#endif

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	return MNDBDataSources(env, Direction,
			       ServerName, BufferLength1, NameLength1,
			       Description, BufferLength2, NameLength2);
}

SQLRETURN SQL_API
SQLDataSourcesA(SQLHENV EnvironmentHandle,
		SQLUSMALLINT Direction,
		SQLCHAR *ServerName,
		SQLSMALLINT BufferLength1,
		SQLSMALLINT *NameLength1,
		SQLCHAR *Description,
		SQLSMALLINT BufferLength2,
		SQLSMALLINT *NameLength2)
{
	return SQLDataSources(EnvironmentHandle, Direction,
			      ServerName, BufferLength1, NameLength1,
			      Description, BufferLength2, NameLength2);
}

SQLRETURN SQL_API
SQLDataSourcesW(SQLHENV EnvironmentHandle,
		SQLUSMALLINT Direction,
		SQLWCHAR *ServerName,
		SQLSMALLINT BufferLength1,
		SQLSMALLINT *NameLength1,
		SQLWCHAR *Description,
		SQLSMALLINT BufferLength2,
		SQLSMALLINT *NameLength2)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;
	SQLRETURN rc;
	SQLCHAR *server, *descr;
	SQLSMALLINT length1, length2;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDataSourcesW " PTRFMT " %s\n",
		PTRFMTCAST EnvironmentHandle, translateDirection(Direction));
#endif

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	server = malloc(100);
	descr = malloc(100);
	if (server == NULL || descr == NULL) {
		/* Memory allocation error */
		addEnvError(env, "HY001", NULL, 0);
		if (server)
			free(server);
		if (descr)
			free(descr);
		return SQL_ERROR;
	}

	rc = MNDBDataSources(env, Direction,
			     server, 100, &length1,
			     descr, 100, &length2);

	if (SQL_SUCCEEDED(rc)) {
		fixWcharOut(rc, server, length1,
			    ServerName, BufferLength1, NameLength1,
			    1, addEnvError, env);
		fixWcharOut(rc, descr, length2,
			    Description, BufferLength2, NameLength2,
			    1, addEnvError, env);
	}
	free(server);
	free(descr);

	return rc;
}
