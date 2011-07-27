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
SQLDataSources_(ODBCEnv *env,
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
	ODBCLOG("SQLDataSources " PTRFMT " %u\n",
		PTRFMTCAST EnvironmentHandle, (unsigned int) Direction);
#endif

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	return SQLDataSources_(env, Direction, ServerName, BufferLength1, NameLength1, Description, BufferLength2, NameLength2);
}

#ifdef WITH_WCHAR
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
	return SQLDataSources(EnvironmentHandle, Direction, ServerName, BufferLength1, NameLength1, Description, BufferLength2, NameLength2);
}

SQLRETURN SQL_API
SQLDataSourcesW(SQLHENV EnvironmentHandle,
		SQLUSMALLINT Direction,
		SQLWCHAR * ServerName,
		SQLSMALLINT BufferLength1,
		SQLSMALLINT *NameLength1,
		SQLWCHAR * Description,
		SQLSMALLINT BufferLength2,
		SQLSMALLINT *NameLength2)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;
	SQLRETURN rc;
	SQLCHAR *server, *descr;
	SQLSMALLINT length1, length2;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDataSourcesW " PTRFMT " %u\n",
		PTRFMTCAST EnvironmentHandle, (unsigned int) Direction);
#endif

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	server = malloc(100);
	descr = malloc(100);

	rc = SQLDataSources_(env, Direction, server, 100, &length1, descr, 100, &length2);

	fixWcharOut(rc, server, length1, ServerName, BufferLength1, NameLength1, 1, addEnvError, env);
	fixWcharOut(rc, descr, length2, Description, BufferLength2, NameLength2, 1, addEnvError, env);

	return rc;
}
#endif /* WITH_WCHAR */
