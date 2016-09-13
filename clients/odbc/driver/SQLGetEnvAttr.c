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

/**********************************************************************
 * SQLGetEnvAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCUtil.h"


SQLRETURN SQL_API
SQLGetEnvAttr(SQLHENV EnvironmentHandle,
	      SQLINTEGER Attribute,
	      SQLPOINTER ValuePtr,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *StringLengthPtr)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetEnvAttr " PTRFMT " %s " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST EnvironmentHandle,
		translateEnvAttribute(Attribute),
		PTRFMTCAST ValuePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr);
#endif

	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLengthPtr;	/* Stefan: unused!? */

	if (!isValidEnv(env))
		return SQL_INVALID_HANDLE;

	clearEnvErrors(env);

	switch (Attribute) {
	case SQL_ATTR_ODBC_VERSION:
		WriteData(ValuePtr, env->sql_attr_odbc_version, SQLINTEGER);
		break;
	case SQL_ATTR_OUTPUT_NTS:
		WriteData(ValuePtr, SQL_TRUE, SQLINTEGER);
		break;
	case SQL_ATTR_CONNECTION_POOLING:
		WriteData(ValuePtr, SQL_CP_OFF, SQLUINTEGER);
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
