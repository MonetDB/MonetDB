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
 * SQLFreeHandle()
 * CLI compliance: ISO 92
 *
 * Note: This function also implements the deprecated ODBC functions
 * SQLFreeEnv(), SQLFreeConnect() and SQLFreeStmt(with option SQL_DROP)
 * Those functions are simply mapped to this function.
 * All checks are done in this function.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"


SQLRETURN FreeHandle(
	SQLSMALLINT	handleType,
	SQLHANDLE	handle )
{
	/* Check parameter handleType */
	if ( ! (handleType == SQL_HANDLE_ENV ||
	        handleType == SQL_HANDLE_DBC ||
	        handleType == SQL_HANDLE_STMT ||
	        handleType == SQL_HANDLE_DESC) )
	{
		/* can not set an error message because we do not know the handle type */
		return SQL_INVALID_HANDLE;
	}

	/* Check parameter handle */
	if (handle == NULL)
	{
		/* can not set an error message because the handle is NULL */
		return SQL_INVALID_HANDLE;
	}


	switch (handleType)
	{
		case SQL_HANDLE_ENV:
		{
			ODBCEnv * env = (ODBCEnv *)handle;
			/* check it's validity */
			if (! isValidEnv(env)) {
				return SQL_INVALID_HANDLE;
			}

			/* check if no associated connections are still active */
			if (env->FirstDbc != NULL) {
				/* There are allocated connections */
				addEnvError(env, "HY010", NULL, 0);
				return SQL_ERROR;
			}

			/* Ready to destroy the env handle */
			destroyODBCEnv(env);
			return SQL_SUCCESS;
		}
		case SQL_HANDLE_DBC:
		{
			ODBCDbc * dbc = (ODBCDbc *)handle;
			/* check it's validity */
			if (! isValidDbc(dbc)) {
				return SQL_INVALID_HANDLE;
			}

			/* check if connection is not active */
			if (dbc->Connected != 0) {
				/* should be disconnected first */
				addDbcError(dbc, "HY010", NULL, 0);
				return SQL_ERROR;
			}

			/* check if no associated statements are still active */
			if (dbc->FirstStmt != NULL) {
				/* There are allocated statements */
				/* should be closed and freed first */
				printf("No FirstStmt \n");
				addDbcError(dbc, "HY010", NULL, 0);
				return SQL_ERROR;
			}

			/* Ready to destroy the dbc handle */
			destroyODBCDbc(dbc);
			return SQL_SUCCESS;
		}
		case SQL_HANDLE_STMT:
		{
			ODBCStmt * stmt = (ODBCStmt *)handle;
			/* check it's validity */
			if (! isValidStmt(stmt)) {
				return SQL_INVALID_HANDLE;
			}

			/* check if statement is not active */
			if (stmt->State == EXECUTED) {
				/* should be closed first */
				int res = FreeStmt(stmt, SQL_CLOSE);
				if (res != SQL_SUCCESS)
					return res;
			}

			/* Ready to destroy the stmt handle */
			destroyODBCStmt(stmt);
			return SQL_SUCCESS;
		}
		case SQL_HANDLE_DESC:
		{
			/* This handle type is not supported (yet).
			 */
			return SQL_INVALID_HANDLE;
		}
		default:
			assert(0);	/* this should not be possible */
	}

	return SQL_INVALID_HANDLE;
}

SQLRETURN SQLFreeHandle(
	SQLSMALLINT	handleType,
	SQLHANDLE	handle )
{
	FreeHandle( handleType, handle);
}
