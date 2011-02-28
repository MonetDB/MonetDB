/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
#include <sys/types.h>		/* for ssize_t on Darwin */


SQLRETURN SQL_API
SQLSetEnvAttr(SQLHENV EnvironmentHandle,
	      SQLINTEGER Attribute,
	      SQLPOINTER Value,
	      SQLINTEGER StringLength)
{
	ODBCEnv *env = (ODBCEnv *) EnvironmentHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetEnvAttr " PTRFMT " %d %lx\n", PTRFMTCAST EnvironmentHandle, (int) Attribute, (unsigned long) (size_t) Value);
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
