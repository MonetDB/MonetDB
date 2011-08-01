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
		   freed first */
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
		if (SQLFreeStmt_(stmt, SQL_CLOSE) == SQL_ERROR)
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
		   descriptor handle */
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

SQLRETURN
SQLFreeHandle_(SQLSMALLINT handleType,
	       SQLHANDLE handle)
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
		clearEnvErrors(env);
		return ODBCFreeEnv_(env);
	}
	case SQL_HANDLE_DBC:
	{
		ODBCDbc *dbc = (ODBCDbc *) handle;

		/* check it's validity */
		if (!isValidDbc(dbc))
			return SQL_INVALID_HANDLE;
		clearDbcErrors(dbc);
		return ODBCFreeDbc_(dbc);
	}
	case SQL_HANDLE_STMT:
	{
		ODBCStmt *stmt = (ODBCStmt *) handle;

		/* check it's validity */
		if (!isValidStmt(stmt))
			 return SQL_INVALID_HANDLE;
		clearStmtErrors(stmt);
		return ODBCFreeStmt_(stmt);
	}
	case SQL_HANDLE_DESC:
	{
		ODBCDesc *desc = (ODBCDesc *) handle;

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
SQLFreeHandle(SQLSMALLINT handleType,
	      SQLHANDLE handle)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLFreeHandle %s " PTRFMT "\n", handleType == SQL_HANDLE_ENV ? "Env" : handleType == SQL_HANDLE_DBC ? "Dbc" : handleType == SQL_HANDLE_STMT ? "Stmt" : "Desc", PTRFMTCAST handle);
#endif

	return SQLFreeHandle_(handleType, handle);
}
