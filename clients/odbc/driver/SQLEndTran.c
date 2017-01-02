/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
MNDBEndTran(SQLSMALLINT HandleType,
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
			rc = MNDBEndTran(SQL_HANDLE_DBC, dbc, CompletionType);
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
	rc = MNDBAllocStmt(dbc, &StatementHandle);
	if (SQL_SUCCEEDED(rc)) {
		ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
		rc = MNDBExecDirect(stmt,
				    CompletionType == SQL_COMMIT ? (SQLCHAR *) "commit" : (SQLCHAR *) "rollback",
				    SQL_NTS);

		if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO) {
			/* get the error/warning and post in on the
			 * dbc handle */
			SQLCHAR sqlState[SQL_SQLSTATE_SIZE + 1];
			SQLINTEGER nativeErrCode;
			SQLCHAR msgText[SQL_MAX_MESSAGE_LENGTH + 1];

			(void) MNDBGetDiagRec(SQL_HANDLE_STMT, stmt, 1,
					      sqlState, &nativeErrCode,
					      msgText, sizeof(msgText), NULL);

			addDbcError(dbc, (char *) sqlState,
				    (char *) msgText + ODBCErrorMsgPrefixLength,
				    nativeErrCode);
		}
		/* clean up the statement handle */
		ODBCResetStmt(stmt);
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
	ODBCLOG("SQLEndTran %s " PTRFMT " %s\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, translateCompletionType(CompletionType));
#endif

	return MNDBEndTran(HandleType, Handle, CompletionType);
}
