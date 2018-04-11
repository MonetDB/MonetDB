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
 * SQLAllocHandle
 * CLI compliance: ISO 92
 *
 * Note: This function also implements the deprecated ODBC functions
 * SQLAllocEnv(), SQLAllocConnect() and SQLAllocStmt()
 * Those functions are simply mapped to this function.
 * All checks and allocation is done in this function.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"

static SQLRETURN
MNDBAllocEnv(SQLHANDLE *OutputHandlePtr)
{
	if (OutputHandlePtr == NULL) {
		return SQL_INVALID_HANDLE;
	}
	*OutputHandlePtr = (SQLHANDLE *) newODBCEnv();
#ifdef ODBCDEBUG
	ODBCLOG("new env %p\n", *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

static SQLRETURN
MNDBAllocDbc(ODBCEnv *env, SQLHANDLE *OutputHandlePtr)
{
	if (env->sql_attr_odbc_version == 0) {
		/* Function sequence error */
		addEnvError(env, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (OutputHandlePtr == NULL) {
		/* Invalid use of null pointer */
		addEnvError(env, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	*OutputHandlePtr = (SQLHANDLE *) newODBCDbc(env);
#ifdef ODBCDEBUG
	ODBCLOG("new dbc %p\n", *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN
MNDBAllocStmt(ODBCDbc *dbc, SQLHANDLE *OutputHandlePtr)
{
	if (!dbc->Connected) {
		/* Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}
	if (OutputHandlePtr == NULL) {
		/* Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	*OutputHandlePtr = (SQLHANDLE *) newODBCStmt(dbc);
#ifdef ODBCDEBUG
	ODBCLOG("new stmt %p\n", *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

static SQLRETURN
MNDBAllocDesc(ODBCDbc *dbc, SQLHANDLE *OutputHandlePtr)
{
	if (!dbc->Connected) {
		/* Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}
	if (OutputHandlePtr == NULL) {
		/* Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	*OutputHandlePtr = (SQLHANDLE *) newODBCDesc(dbc);
#ifdef ODBCDEBUG
	ODBCLOG("new desc %p\n", *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN
MNDBAllocHandle(SQLSMALLINT HandleType,
		SQLHANDLE InputHandle,
		SQLHANDLE *OutputHandlePtr)
{
	switch (HandleType) {
	case SQL_HANDLE_ENV:
		if (InputHandle != NULL)
			return SQL_INVALID_HANDLE;
		return MNDBAllocEnv(OutputHandlePtr);
	case SQL_HANDLE_DBC:
		if (!isValidEnv((ODBCEnv *) InputHandle))
			return SQL_INVALID_HANDLE;
		clearEnvErrors((ODBCEnv *) InputHandle);
		return MNDBAllocDbc((ODBCEnv *) InputHandle, OutputHandlePtr);
	case SQL_HANDLE_STMT:
		if (!isValidDbc((ODBCDbc *) InputHandle))
			return SQL_INVALID_HANDLE;
		clearDbcErrors((ODBCDbc *) InputHandle);
		return MNDBAllocStmt((ODBCDbc *) InputHandle, OutputHandlePtr);
	case SQL_HANDLE_DESC:
		if (!isValidDbc((ODBCDbc *) InputHandle))
			return SQL_INVALID_HANDLE;
		clearDbcErrors((ODBCDbc *) InputHandle);
		return MNDBAllocDesc((ODBCDbc *) InputHandle, OutputHandlePtr);
	default:
		/* we cannot set an error because we do not know
		   the handle type of the possibly non-null handle */
		return SQL_INVALID_HANDLE;
	}
}

SQLRETURN SQL_API
SQLAllocHandle(SQLSMALLINT HandleType,	/* type to be allocated */
	       SQLHANDLE InputHandle,	/* context for new handle */
	       SQLHANDLE *OutputHandlePtr) /* ptr for allocated handle struct */
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLAllocHandle %s %p\n",
		HandleType == SQL_HANDLE_ENV ? "Env" :
		    HandleType == SQL_HANDLE_DBC ? "Dbc" :
		    HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		InputHandle);
#endif

	return MNDBAllocHandle(HandleType, InputHandle, OutputHandlePtr);
}
