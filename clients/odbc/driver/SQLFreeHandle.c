/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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


static SQLRETURN
ODBCFreeEnv_(ODBCEnv *env)
{
	if (env->sql_attr_odbc_version == 0) {
		/* Function sequence error */
		addEnvError(env, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* check if no associated connections are still active */
	if (env->FirstDbc != NULL) {
		/* Function sequence error */
		addEnvError(env, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* Ready to destroy the env handle */
	destroyODBCEnv(env);
	return SQL_SUCCESS;
}

static SQLRETURN
ODBCFreeDbc_(ODBCDbc *dbc)
{
	/* check if connection is not active */
	if (dbc->Connected) {
		/* Function sequence error */
		addDbcError(dbc, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* check if no associated statements are still active */
	if (dbc->FirstStmt != NULL) {
		/* There are allocated statements should be closed and
		 * freed first */
		/* Function sequence error */
		addDbcError(dbc, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	/* Ready to destroy the dbc handle */
	destroyODBCDbc(dbc);
	return SQL_SUCCESS;
}

SQLRETURN
ODBCFreeStmt_(ODBCStmt *stmt)
{
	/* check if statement is not active */
	if (stmt->State >= EXECUTED0) {
		/* should be closed first */
		if (MNDBFreeStmt(stmt, SQL_CLOSE) == SQL_ERROR)
			return SQL_ERROR;
	}

	/* Ready to destroy the stmt handle */
	destroyODBCStmt(stmt);
	return SQL_SUCCESS;
}

static SQLRETURN
ODBCFreeDesc_(ODBCDesc *desc)
{
	ODBCStmt *stmt;

	/* check if descriptor is implicitly allocated */
	if (desc->sql_desc_alloc_type == SQL_DESC_ALLOC_AUTO) {
		/* Invalid use of an automatically allocated
		 * descriptor handle */
		addDescError(desc, "HY017", NULL, 0);
		return SQL_ERROR;
	}

	/* all statements using this handle revert to implicitly
	 * allocated descriptor handles */
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

SQLRETURN
MNDBFreeHandle(SQLSMALLINT HandleType,
	       SQLHANDLE Handle)
{
	/* Check parameter handle */
	if (Handle == NULL) {
		/* can not set an error message because the handle is NULL */
		return SQL_INVALID_HANDLE;
	}


	switch (HandleType) {
	case SQL_HANDLE_ENV:
	{
		ODBCEnv *env = (ODBCEnv *) Handle;

		/* check it's validity */
		if (!isValidEnv(env))
			return SQL_INVALID_HANDLE;
		clearEnvErrors(env);
		return ODBCFreeEnv_(env);
	}
	case SQL_HANDLE_DBC:
	{
		ODBCDbc *dbc = (ODBCDbc *) Handle;

		/* check it's validity */
		if (!isValidDbc(dbc))
			return SQL_INVALID_HANDLE;
		clearDbcErrors(dbc);
		return ODBCFreeDbc_(dbc);
	}
	case SQL_HANDLE_STMT:
	{
		ODBCStmt *stmt = (ODBCStmt *) Handle;

		/* check it's validity */
		if (!isValidStmt(stmt))
			 return SQL_INVALID_HANDLE;
		clearStmtErrors(stmt);
		return ODBCFreeStmt_(stmt);
	}
	case SQL_HANDLE_DESC:
	{
		ODBCDesc *desc = (ODBCDesc *) Handle;

		/* check it's validity */
		if (!isValidDesc(desc))
			return SQL_INVALID_HANDLE;
		clearDescErrors(desc);
		return ODBCFreeDesc_(desc);
	}
	default:
		return SQL_INVALID_HANDLE;
	}

	/* not reached */
}

SQLRETURN SQL_API
SQLFreeHandle(SQLSMALLINT HandleType,
	      SQLHANDLE Handle)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLFreeHandle %s " PTRFMT "\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle);
#endif

	return MNDBFreeHandle(HandleType, Handle);
}
