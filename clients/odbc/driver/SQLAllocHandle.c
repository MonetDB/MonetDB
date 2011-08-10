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
SQLAllocEnv_(SQLHANDLE *OutputHandlePtr)
{
	if (OutputHandlePtr == NULL) {
		return SQL_INVALID_HANDLE;
	}
	*OutputHandlePtr = (SQLHANDLE *) newODBCEnv();
#ifdef ODBCDEBUG
	ODBCLOG("new env " PTRFMT "\n", PTRFMTCAST *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

static SQLRETURN
SQLAllocDbc_(ODBCEnv *env, SQLHANDLE *OutputHandlePtr)
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
	ODBCLOG("new dbc " PTRFMT "\n", PTRFMTCAST *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN
SQLAllocStmt_(ODBCDbc *dbc, SQLHANDLE *OutputHandlePtr)
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
	ODBCLOG("new stmt " PTRFMT "\n", PTRFMTCAST *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

static SQLRETURN
SQLAllocDesc_(ODBCDbc *dbc, SQLHANDLE *OutputHandlePtr)
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
	ODBCLOG("new desc " PTRFMT "\n", PTRFMTCAST *OutputHandlePtr);
#endif
	return *OutputHandlePtr == NULL ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN
SQLAllocHandle_(SQLSMALLINT HandleType,
		SQLHANDLE InputHandle,
		SQLHANDLE *OutputHandlePtr)
{
	switch (HandleType) {
	case SQL_HANDLE_ENV:
		if (InputHandle != NULL)
			return SQL_INVALID_HANDLE;
		return SQLAllocEnv_(OutputHandlePtr);
	case SQL_HANDLE_DBC:
		if (!isValidEnv((ODBCEnv *) InputHandle))
			return SQL_INVALID_HANDLE;
		clearEnvErrors((ODBCEnv *) InputHandle);
		return SQLAllocDbc_((ODBCEnv *) InputHandle, OutputHandlePtr);
	case SQL_HANDLE_STMT:
		if (!isValidDbc((ODBCDbc *) InputHandle))
			return SQL_INVALID_HANDLE;
		clearDbcErrors((ODBCDbc *) InputHandle);
		return SQLAllocStmt_((ODBCDbc *) InputHandle, OutputHandlePtr);
	case SQL_HANDLE_DESC:
		if (!isValidDbc((ODBCDbc *) InputHandle))
			return SQL_INVALID_HANDLE;
		clearDbcErrors((ODBCDbc *) InputHandle);
		return SQLAllocDesc_((ODBCDbc *) InputHandle, OutputHandlePtr);
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
	ODBCLOG("SQLAllocHandle %s " PTRFMT "\n",
		HandleType == SQL_HANDLE_ENV ? "Env" :
		    HandleType == SQL_HANDLE_DBC ? "Dbc" :
		    HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST InputHandle);
#endif

	return SQLAllocHandle_(HandleType, InputHandle, OutputHandlePtr);
}
