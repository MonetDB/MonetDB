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


SQLRETURN SQL_API
SQLGetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetEnvAttr " PTRFMT " %d\n", PTRFMTCAST EnvironmentHandle, Attribute);
#endif

	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLength;	/* Stefan: unused!? */

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	switch (Attribute) {
	case SQL_ATTR_ODBC_VERSION:
		*(SQLINTEGER *) Value = env->sql_attr_odbc_version;
		break;
	case SQL_ATTR_OUTPUT_NTS:
		*(SQLINTEGER *) Value = SQL_TRUE;
		break;
	case SQL_ATTR_CONNECTION_POOLING:
		*(SQLUINTEGER *) Value = SQL_CP_OFF;
		break;
	case SQL_ATTR_CP_MATCH:
		/* TODO: implement this function and corresponding behavior */

		/* for now return error */
		/* Driver does not support this function */
		addEnvError(env, "IM001", NULL, 0);
		return SQL_ERROR;
	default:
		/* Invalid attribute/option identifier */
		addEnvError(env, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}
