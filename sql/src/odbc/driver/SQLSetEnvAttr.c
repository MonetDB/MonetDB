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
 * SQLSetEnvAttr()
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
SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute,
	      SQLPOINTER Value, SQLINTEGER StringLength)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetEnvAttr %d\n", Attribute);
#endif

	(void) StringLength;	/* Stefan: unused!? */

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	switch (Attribute) {
	case SQL_ATTR_ODBC_VERSION:
		switch ((SQLINTEGER) (ssize_t) Value) {
		case SQL_OV_ODBC3:
			env->ODBCVersion = ODBC_3;
			break;
		case SQL_OV_ODBC2:
			env->ODBCVersion = ODBC_2;
			break;
		default:
			/* HY024: Invalid attribute value */
			addEnvError(env, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		break;
	default:
		/* TODO: implement this function and corresponding behavior */

		/* for now return error IM001: driver not capable */
		addEnvError(env, "IM001", NULL, 0);
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}
