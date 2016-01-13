/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

/**********************************************
 * ODBCEnv.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC environment structures/objects (see ODBCEnv.h).
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"

#define ODBC_ENV_MAGIC_NR  341	/* for internal sanity check only */


/*
 * Creates a new allocated ODBCEnv object and initializes it.
 *
 * Precondition: none
 * Postcondition: returns a new ODBCEnv object
 */
ODBCEnv *
newODBCEnv(void)
{
	ODBCEnv *env = (ODBCEnv *) malloc(sizeof(ODBCEnv));

	if (env == NULL)
		return NULL;

	env->Error = NULL;
	env->RetrievedErrors = 0;
	env->FirstDbc = NULL;
	env->Type = ODBC_ENV_MAGIC_NR;
	env->sql_attr_odbc_version = 0;

	return env;
}


/*
 * Check if the enviroment handle is valid.
 * Note: this function is used internally by the driver to assert legal
 * and save usage of the handle and prevent crashes as much as possible.
 *
 * Precondition: none
 * Postcondition: returns 1 if it is a valid environment handle,
 * 	returns 0 if is invalid and thus an unusable handle.
 */
int
isValidEnv(ODBCEnv *env)
{
#ifdef ODBCDEBUG
	if (!(env && env->Type == ODBC_ENV_MAGIC_NR))
		ODBCLOG("env " PTRFMT " not a valid environment handle\n", PTRFMTCAST env);
#endif
	return env && env->Type == ODBC_ENV_MAGIC_NR;
}


/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCEnv struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: env must be valid. SQLState and errMsg may be NULL.
 */
void
addEnvError(ODBCEnv *env, const char *SQLState, const char *errMsg, int nativeErrCode)
{
	ODBCError *error = NULL;

#ifdef ODBCDEBUG
	ODBCLOG("addEnvError " PTRFMT " %s %s %d\n", PTRFMTCAST env, SQLState, errMsg ? errMsg : getStandardSQLStateMsg(SQLState), nativeErrCode);
#endif
	assert(isValidEnv(env));

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	appendODBCError(&env->Error, error);
}


/*
 * Extracts an error object from the error list of this ODBCEnv struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: env and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError *
getEnvError(ODBCEnv *env)
{
	assert(isValidEnv(env));
	return env->Error;;
}


/*
 * Destroys the ODBCEnv object including its own managed data.
 *
 * Precondition: env must be valid and no ODBCDbc objects may refer to this env.
 * Postcondition: env is completely destroyed, env handle is become invalid.
 */
void
destroyODBCEnv(ODBCEnv *env)
{
	assert(isValidEnv(env));
	assert(env->FirstDbc == NULL);

	/* first set this object to invalid */
	env->Type = 0;

	deleteODBCErrorList(&env->Error);
	free((void *) env);
}
