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
#include <sys/types.h> /* for ssize_t on Darwin */


SQLRETURN SQL_API
SQLSetEnvAttr(SQLHENV EnvironmentHandle, SQLINTEGER Attribute,
	      SQLPOINTER Value, SQLINTEGER StringLength)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetEnvAttr " PTRFMT " %d %lx\n",
		PTRFMTCAST EnvironmentHandle,
		Attribute, (unsigned long) (size_t) Value);
#endif

	(void) StringLength;	/* Stefan: unused!? */

	/* global attribute */
	if (Attribute == SQL_ATTR_CONNECTION_POOLING && env == NULL) {
		switch ((SQLUINTEGER) (size_t) Value) {
		case SQL_CP_OFF:
		case SQL_CP_ONE_PER_DRIVER:
		case SQL_CP_ONE_PER_HENV:
			break;
		default:
			return SQL_INVALID_HANDLE;
		}
		return SQL_SUCCESS;
	}

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	/* can only set environment attributes if no connection handle
	   has been allocated */
	if (env->FirstDbc != NULL) {
		/* Function sequence error */
		addEnvError(env, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	switch (Attribute) {
	case SQL_ATTR_ODBC_VERSION:
		switch ((SQLINTEGER) (ssize_t) Value) {
		case SQL_OV_ODBC3:
		case SQL_OV_ODBC2:
			env->sql_attr_odbc_version = (SQLINTEGER) (ssize_t) Value;
			break;
		default:
			/* Invalid attribute value */
			addEnvError(env, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_ATTR_OUTPUT_NTS:
		switch ((SQLINTEGER) (ssize_t) Value) {
		case SQL_TRUE:
			break;
		case SQL_FALSE:
			/* Optional feature not implemented */
			addEnvError(env, "HYC00", NULL, 0);
			return SQL_ERROR;
		default:
			/* Invalid attribute/option identifier */
			addEnvError(env, "HY092", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_ATTR_CP_MATCH:
		/* Optional feature not implemented */
		addEnvError(env, "HYC00", NULL, 0);
		return SQL_ERROR;
	case SQL_ATTR_CONNECTION_POOLING:
		/* not valid with non-NULL environment handle parameter */
	default:
		/* Invalid attribute/option identifier */
		addEnvError(env, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}
