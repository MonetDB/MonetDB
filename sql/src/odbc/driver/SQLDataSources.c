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


SQLRETURN
SQLDataSources(SQLHENV EnvironmentHandle, SQLUSMALLINT Direction,
	       SQLCHAR *ServerName, SQLSMALLINT BufferLength1,
	       SQLSMALLINT *NameLength1, SQLCHAR *Description,
	       SQLSMALLINT BufferLength2, SQLSMALLINT *NameLength2)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDataSources\n");
#endif

	(void) Direction;	/* Stefan: unused!? */
	(void) ServerName;	/* Stefan: unused!? */
	(void) BufferLength1;	/* Stefan: unused!? */
	(void) NameLength1;	/* Stefan: unused!? */
	(void) Description;	/* Stefan: unused!? */
	(void) BufferLength2;	/* Stefan: unused!? */
	(void) NameLength2;	/* Stefan: unused!? */

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	/* TODO: implement the requested behavior */

	/* for now always return error: Driver does not support this function */
	addEnvError(env, "IM001", NULL, 0);
	return SQL_ERROR;
}
