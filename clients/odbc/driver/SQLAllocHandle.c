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
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"

static SQLRETURN
SQLAllocEnv_(SQLHANDLE *pnOutputHandle)
{
	if (pnOutputHandle == NULL) {
		return SQL_INVALID_HANDLE;
	}
	*pnOutputHandle = (SQLHANDLE *) newODBCEnv();
#ifdef ODBCDEBUG
	ODBCLOG("new env " PTRFMT "\n", PTRFMTCAST *pnOutputHandle);
#endif
	return *pnOutputHandle == NULL ? SQL_ERROR : SQL_SUCCESS;
}

static SQLRETURN
SQLAllocDbc_(ODBCEnv *env,
	     SQLHANDLE *pnOutputHandle)
{
	if (env->sql_attr_odbc_version == 0) {
		/* Function sequence error */
		addEnvError(env, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (pnOutputHandle == NULL) {
		/* Invalid use of null pointer */
		addEnvError(env, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	*pnOutputHandle = (SQLHANDLE *) newODBCDbc(env);
#ifdef ODBCDEBUG
	ODBCLOG("new dbc " PTRFMT "\n", PTRFMTCAST *pnOutputHandle);
#endif
	return *pnOutputHandle == NULL ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN
SQLAllocStmt_(ODBCDbc *dbc,
	      SQLHANDLE *pnOutputHandle)
{
	if (!dbc->Connected) {
		/* Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}
	if (pnOutputHandle == NULL) {
		/* Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	*pnOutputHandle = (SQLHANDLE *) newODBCStmt(dbc);
#ifdef ODBCDEBUG
	ODBCLOG("new stmt " PTRFMT "\n", PTRFMTCAST *pnOutputHandle);
#endif
	return *pnOutputHandle == NULL ? SQL_ERROR : SQL_SUCCESS;
}

static SQLRETURN
SQLAllocDesc_(ODBCDbc *dbc,
	      SQLHANDLE *pnOutputHandle)
{
	if (!dbc->Connected) {
		/* Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}
	if (pnOutputHandle == NULL) {
		/* Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}
	*pnOutputHandle = (SQLHANDLE *) newODBCDesc(dbc);
#ifdef ODBCDEBUG
	ODBCLOG("new desc " PTRFMT "\n", PTRFMTCAST *pnOutputHandle);
#endif
	return *pnOutputHandle == NULL ? SQL_ERROR : SQL_SUCCESS;
}

SQLRETURN
SQLAllocHandle_(SQLSMALLINT nHandleType,
		SQLHANDLE nInputHandle,
		SQLHANDLE *pnOutputHandle)
{
	switch (nHandleType) {
	case SQL_HANDLE_ENV:
		if (nInputHandle != NULL)
			return SQL_INVALID_HANDLE;
		return SQLAllocEnv_(pnOutputHandle);
	case SQL_HANDLE_DBC:
		if (!isValidEnv((ODBCEnv *) nInputHandle))
			return SQL_INVALID_HANDLE;
		clearEnvErrors((ODBCEnv *) nInputHandle);
		return SQLAllocDbc_((ODBCEnv *) nInputHandle, pnOutputHandle);
	case SQL_HANDLE_STMT:
		if (!isValidDbc((ODBCDbc *) nInputHandle))
			return SQL_INVALID_HANDLE;
		clearDbcErrors((ODBCDbc *) nInputHandle);
		return SQLAllocStmt_((ODBCDbc *) nInputHandle, pnOutputHandle);
	case SQL_HANDLE_DESC:
		if (!isValidDbc((ODBCDbc *) nInputHandle))
			return SQL_INVALID_HANDLE;
		clearDbcErrors((ODBCDbc *) nInputHandle);
		return SQLAllocDesc_((ODBCDbc *) nInputHandle, pnOutputHandle);
	default:
		/* we cannot set an error because we do not know
		   the handle type of the possibly non-null handle */
		return SQL_INVALID_HANDLE;
	}
}

SQLRETURN SQL_API
SQLAllocHandle(SQLSMALLINT nHandleType,	/* type to be allocated */
	       SQLHANDLE nInputHandle,	/* context for new handle */
	       SQLHANDLE *pnOutputHandle)
{				/* ptr for allocated handle struct */
#ifdef ODBCDEBUG
	ODBCLOG("SQLAllocHandle %s " PTRFMT "\n", nHandleType == SQL_HANDLE_ENV ? "Env" : nHandleType == SQL_HANDLE_DBC ? "Dbc" : nHandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc", PTRFMTCAST nInputHandle);
#endif

	return SQLAllocHandle_(nHandleType, nInputHandle, pnOutputHandle);
}
