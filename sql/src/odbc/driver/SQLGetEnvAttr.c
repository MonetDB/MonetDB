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
 * SQLGetEnvAttr()
 * CLI Compliance: ISO 92
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"


SQLRETURN
SQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute,
	      SQLPOINTER Value, SQLINTEGER BufferLength,
	      SQLINTEGER *StringLength)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

	(void) Attribute;	/* Stefan: unused!? */
	(void) Value;		/* Stefan: unused!? */
	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLength;	/* Stefan: unused!? */

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	/* TODO: implement this function and corresponding behavior */

	/* for now return error IM001: driver not capable */
	addEnvError(env, "IM001", NULL, 0);
	return SQL_ERROR;
}
