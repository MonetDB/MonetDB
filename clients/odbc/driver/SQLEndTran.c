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
 * SQLEndTran()
 * CLI Compliance: ISO 92
 *
 * Note: commit or rollback all open connections on a given environment
 * handle is currently NOT supported, see TODO below.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"


SQLRETURN
SQLEndTran_(SQLSMALLINT HandleType,
	    SQLHANDLE Handle,
	    SQLSMALLINT CompletionType)
{
	ODBCEnv *env = NULL;
	ODBCDbc *dbc = NULL;
	SQLHANDLE StatementHandle;
	RETCODE rc;

	/* check parameters HandleType and Handle for validity */
	switch (HandleType) {
	case SQL_HANDLE_DBC:
		dbc = (ODBCDbc *) Handle;
		if (!isValidDbc(dbc))
			return SQL_INVALID_HANDLE;
		clearDbcErrors(dbc);
		if (!dbc->Connected) {
			/* Connection does not exist */
			addDbcError(dbc, "08003", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_HANDLE_ENV:
		env = (ODBCEnv *) Handle;
		if (!isValidEnv(env))
			return SQL_INVALID_HANDLE;
		clearEnvErrors(env);
		if (env->sql_attr_odbc_version == 0) {
			/* Function sequence error */
			addEnvError(env, "HY010", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_HANDLE_STMT:
		if (isValidStmt((ODBCStmt *) Handle)) {
			clearStmtErrors((ODBCStmt *) Handle);
			/* Invalid attribute/option identifier */
			addStmtError((ODBCStmt *) Handle, "HY092", NULL, 0);
			return SQL_ERROR;
		}
		return SQL_INVALID_HANDLE;
	case SQL_HANDLE_DESC:
		if (isValidDesc((ODBCDesc *) Handle)) {
			clearDescErrors((ODBCDesc *) Handle);
			/* Invalid attribute/option identifier */
			addDescError((ODBCDesc *) Handle, "HY092", NULL, 0);
			return SQL_ERROR;
		}
		return SQL_INVALID_HANDLE;
	default:
		return SQL_INVALID_HANDLE;
	}

	/* check parameter CompletionType */
	if (CompletionType != SQL_COMMIT && CompletionType != SQL_ROLLBACK) {
		/* Invalid transaction operation code */
		if (HandleType == SQL_HANDLE_DBC)
			addDbcError(dbc, "HY012", NULL, 0);
		else
			addEnvError(env, "HY012", NULL, 0);
		return SQL_ERROR;
	}

	if (HandleType == SQL_HANDLE_ENV) {
		RETCODE rc1 = SQL_SUCCESS;

		for (dbc = env->FirstDbc; dbc; dbc = dbc->next) {
			assert(isValidDbc(dbc));
			if (!dbc->Connected)
				continue;
			rc = SQLEndTran_(SQL_HANDLE_DBC, dbc, CompletionType);
			if (rc == SQL_ERROR)
				rc1 = SQL_ERROR;
			else if (rc == SQL_SUCCESS_WITH_INFO &&
				 rc1 != SQL_ERROR)
				rc1 = rc;
		}
		return rc1;
	}

	assert(HandleType == SQL_HANDLE_DBC);

	if (dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON) {
		/* nothing to do if in autocommit mode */
		return SQL_SUCCESS;
	}

	/* construct a statement object and excute a SQL COMMIT or ROLLBACK */
	rc = SQLAllocStmt_(dbc, &StatementHandle);
	if (SQL_SUCCEEDED(rc)) {
		ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
		rc = SQLExecDirect_(stmt,
				    CompletionType == SQL_COMMIT ? (SQLCHAR *) "commit" : (SQLCHAR *) "rollback",
				    SQL_NTS);

		if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO) {
			/* get the error/warning and post in on the
			 * dbc handle */
			SQLCHAR sqlState[SQL_SQLSTATE_SIZE + 1];
			SQLINTEGER nativeErrCode;
			SQLCHAR msgText[SQL_MAX_MESSAGE_LENGTH + 1];

			(void) SQLGetDiagRec_(SQL_HANDLE_STMT, stmt, 1,
					      sqlState, &nativeErrCode,
					      msgText, sizeof(msgText), NULL);

			addDbcError(dbc, (char *) sqlState,
				    (char *) msgText + ODBCErrorMsgPrefixLength,
				    nativeErrCode);
		}
		/* clean up the statement handle */
		SQLFreeStmt_(stmt, SQL_CLOSE);
		ODBCFreeStmt_(stmt);

		for (stmt = dbc->FirstStmt; stmt; stmt = stmt->next)
			ODBCResetStmt(stmt);
	} else {
		/* could not allocate a statement object */
		/* Memory management error */
		addDbcError(dbc, "HY013", NULL, 0);
		return SQL_ERROR;
	}

	return rc;
}

SQLRETURN SQL_API
SQLEndTran(SQLSMALLINT HandleType,
	   SQLHANDLE Handle,
	   SQLSMALLINT CompletionType)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLEndTran %s " PTRFMT " %d\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, (int) CompletionType);
#endif

	return SQLEndTran_(HandleType, Handle, CompletionType);
}
