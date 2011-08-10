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
 * SQLGetEnvAttr()
 * CLI Compliance: ISO 92
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"


SQLRETURN SQL_API
SQLGetEnvAttr(SQLHENV EnvironmentHandle,
	      SQLINTEGER Attribute,
	      SQLPOINTER Value,
	      SQLINTEGER BufferLength,
	      SQLINTEGER *StringLength)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetEnvAttr " PTRFMT " %d\n",
		PTRFMTCAST EnvironmentHandle, (int) Attribute);
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
