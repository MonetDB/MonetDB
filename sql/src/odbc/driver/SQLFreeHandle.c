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


SQLRETURN
SQLFreeHandle_(SQLSMALLINT handleType, SQLHANDLE handle)
{
	/* Check parameter handle */
	if (handle == NULL) {
		/* can not set an error message because the handle is NULL */
		return SQL_INVALID_HANDLE;
	}


	switch (handleType) {
	case SQL_HANDLE_ENV:
	{
		ODBCEnv *env = (ODBCEnv *) handle;

		/* check it's validity */
		if (!isValidEnv(env))
			return SQL_INVALID_HANDLE;

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
		ODBCDbc *dbc = (ODBCDbc *) handle;

		/* check it's validity */
		if (!isValidDbc(dbc))
			return SQL_INVALID_HANDLE;

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
			addDbcError(dbc, "HY010", NULL, 0);
			return SQL_ERROR;
		}

		/* Ready to destroy the dbc handle */
		destroyODBCDbc(dbc);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_STMT:
	{
		ODBCStmt *stmt = (ODBCStmt *) handle;

		/* check it's validity */
		if (!isValidStmt(stmt))
			return SQL_INVALID_HANDLE;

		/* check if statement is not active */
		if (stmt->State == EXECUTED) {
			/* should be closed first */
			SQLRETURN res = SQLFreeStmt_(stmt, SQL_CLOSE);

			if (res != SQL_SUCCESS)
				return res;
		}

		/* Ready to destroy the stmt handle */
		destroyODBCStmt(stmt);
		return SQL_SUCCESS;
	}
	case SQL_HANDLE_DESC:
	{
		ODBCDesc *desc = (ODBCDesc *) handle;
		ODBCStmt *stmt;

		/* check it's validity */
		if (!isValidDesc(desc))
			return SQL_INVALID_HANDLE;

		/* check if descriptor is implicitly allocated */
		if (desc->sql_desc_alloc_type == SQL_DESC_ALLOC_AUTO) {
			/* Invalid use of an automatically allocated descriptor handle */
			addDescError(desc, "HY017", NULL, 0);
			return SQL_ERROR;
		}

		/* all statements using this handle revert to
		   implicitly allocated descriptor handles */
		for (stmt = desc->Dbc->FirstStmt; stmt; stmt = stmt->next) {
			if (desc == stmt->ApplRowDescr)
				stmt->ApplRowDescr = stmt->AutoApplRowDescr;
			if (desc == stmt->ApplParamDescr)
				stmt->ApplParamDescr = stmt->AutoApplParamDescr;
		}

		/* Ready to destroy the desc handle */
		destroyODBCDesc(desc);
		return SQL_SUCCESS;
	}
	default:
		return SQL_INVALID_HANDLE;
	}

	/* not reached */
}

SQLRETURN
SQLFreeHandle(SQLSMALLINT handleType, SQLHANDLE handle)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLFreeHandle %d\n", handleType);
#endif

	return SQLFreeHandle_(handleType, handle);
}
