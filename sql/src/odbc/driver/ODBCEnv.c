/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************
 * ODBCEnv.c
 *
 * Description:
 * This file contains the functions which operate on
 * ODBC environment structures/objects (see ODBCEnv.h).
 *
 * Author: Martin van Dinther
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
ODBCEnv * newODBCEnv()
{
	ODBCEnv * env = (ODBCEnv *)GDKmalloc(sizeof(ODBCEnv));
	assert(env);

	env->Error = NULL;
	env->FirstDbc = NULL;
	env->Type = ODBC_ENV_MAGIC_NR;

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
int isValidEnv(ODBCEnv * env)
{
	return (env && (env->Type == ODBC_ENV_MAGIC_NR)) ? 1 : 0;
}


/*
 * Creates and adds an error msg object to the end of the error list of
 * this ODBCEnv struct.
 * When the errMsg is NULL and the SQLState is an ISO SQLState the
 * standard ISO message text for the SQLState is used as message.
 *
 * Precondition: env must be valid. SQLState and errMsg may be NULL.
 */
void addEnvError(ODBCEnv * env, char * SQLState, char * errMsg, int nativeErrCode)
{
	ODBCError * error = NULL;

	assert(isValidEnv(env));

	error = newODBCError(SQLState, errMsg, nativeErrCode);
	if (env->Error == NULL) {
		env->Error = error;
	} else {
		appendODBCError(env->Error, error);
	}
}


/*
 * Adds an error msg object to the end of the error list of
 * this ODBCEnv struct.
 *
 * Precondition: env and error must be valid.
 */
void addEnvErrorObj(ODBCEnv * env, ODBCError * error)
{
	assert(isValidEnv(env));
	assert(error);

	if (env->Error == NULL) {
		env->Error = error;
	} else {
		appendODBCError(env->Error, error);
	}
}


/*
 * Extracts an error object from the error list of this ODBCEnv struct.
 * The error object itself is removed from the error list.
 * The caller is now responsible for freeing the error object memory.
 *
 * Precondition: env and error must be valid
 * Postcondition: returns a ODBCError object or null when no error is available.
 */
ODBCError * getEnvError(ODBCEnv * env)
{
	ODBCError * err;
	assert(isValidEnv(env));

	err = env->Error;	/* get first error */
	env->Error = (err) ? err->next : NULL;	/* set new first error */

	return err;
}


/*
 * Destroys the ODBCEnv object including its own managed data.
 *
 * Precondition: env must be valid and no ODBCDbc objects may refer to this env.
 * Postcondition: env is completely destroyed, env handle is become invalid.
 */
void destroyODBCEnv(ODBCEnv * env)
{
	assert(isValidEnv(env));
	assert(env->FirstDbc == NULL);

	/* first set this object to invalid */
	env->Type = 0;

	deleteODBCErrorList(env->Error);
	GDKfree((void *)env);
}

/*
 * Clear the contents of the ODBCEnv object.
 *
 * Precondition: env must be valid and no ODBCDbc objects may refer to this env.
 * Postcondition: env managed data is completely destroyed.
 */
void clearEnvErrors(ODBCEnv * env)
{
	assert(isValidEnv(env));
	assert(env->FirstDbc == NULL);

	deleteODBCErrorList(env->Error);
	env->Error = NULL;
}
