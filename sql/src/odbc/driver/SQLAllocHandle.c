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

/**********************************************************************
 * SQLAllocHandle
 * CLI compliance: ISO 92
 *
 * Note: This function also implements the deprecated ODBC functions
 * SQLAllocEnv(), SQLAllocConnect() and SQLAllocStmt()
 * Those functions are simply mapped to this function.
 * All checks and allocation is done in this function.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"

static int odbc_init = 0; 

SQLRETURN  SQLAllocHandle(
	SQLSMALLINT	nHandleType,	/* type to be allocated */
	SQLHANDLE	nInputHandle,	/* type to be allocated */
	SQLHANDLE *	pnOutputHandle )/* ptr for allocated handle struct */
{
	if (!odbc_init){
		odbc_init = 1;
		stream_init();
	}

	/* Check parameters nHandleType and nInputHandle */
	if (nInputHandle == NULL && nHandleType != SQL_HANDLE_ENV)
	{
		/* can not set an error message because the handle is NULL */
		return SQL_INVALID_HANDLE;
	}
	switch (nHandleType)
	{
		case SQL_HANDLE_ENV:
			/* there is no parent handle to test */
			break;
		case SQL_HANDLE_DBC:
		{
			ODBCEnv * env = (ODBCEnv *)nInputHandle;
			if (! isValidEnv(env))
				return SQL_INVALID_HANDLE;
			break;
		}
		case SQL_HANDLE_STMT:
		case SQL_HANDLE_DESC:
		{
			ODBCDbc * dbc = (ODBCDbc *)nInputHandle;
			if (! isValidDbc(dbc))
				return SQL_INVALID_HANDLE;
			break;
		}
		default:
		{
			/* we cannot set an error because we do not know
			   the handle type of the possible non-null handle */
			return SQL_INVALID_HANDLE;
		}
	}

	/* Check parameter pnOutputHandle */
	if (pnOutputHandle == NULL)
	{
		if (nInputHandle) {
		switch (nHandleType) {
			case SQL_HANDLE_ENV:
				/* there is no valid parent handle */
				break;
			case SQL_HANDLE_DBC:
			{
				ODBCEnv * env = (ODBCEnv *)nInputHandle;
				addEnvError(env, "HY009", NULL, 0);
				return SQL_ERROR;
			}
			case SQL_HANDLE_STMT:
			case SQL_HANDLE_DESC:
			{
				ODBCDbc * dbc = (ODBCDbc *)nInputHandle;
				addDbcError(dbc, "HY009", NULL, 0);
				return SQL_ERROR;
			}
			default:
			{
				assert(0);  /* this should not be possible */
			}
		}
		}
		return SQL_INVALID_HANDLE;
	}

	/* We are ready to do the allocation now */
	switch (nHandleType)
	{
		case SQL_HANDLE_ENV:
		{
			*pnOutputHandle = (SQLHANDLE *) newODBCEnv();
			return SQL_SUCCESS;
		}
		case SQL_HANDLE_DBC:
		{
			ODBCEnv * env = (ODBCEnv *)nInputHandle;
			assert(isValidEnv(env));
			*pnOutputHandle = (SQLHANDLE *) newODBCDbc(env);
			return SQL_SUCCESS;
		}
		case SQL_HANDLE_STMT:
		{
			ODBCDbc * dbc = (ODBCDbc *)nInputHandle;
			assert(isValidDbc(dbc));
			*pnOutputHandle = (SQLHANDLE *) newODBCStmt(dbc);
			return SQL_SUCCESS;
		}
		case SQL_HANDLE_DESC:
		{
			/* TODO: implement this handle type.
			 * For now, report an error.
			 */
			ODBCDbc * dbc = (ODBCDbc *)nInputHandle;
			assert(isValidDbc(dbc));
			addDbcError(dbc, "HYC00", NULL, 0);
			*pnOutputHandle = SQL_NULL_HDESC;
			return SQL_ERROR;
		}
		default:
		{
			assert(0);	/* this should not be possible */
		}
	}

	return SQL_INVALID_HANDLE;
}
