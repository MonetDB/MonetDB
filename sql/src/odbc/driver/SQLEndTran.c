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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"


SQLRETURN
SQLEndTran(SQLSMALLINT nHandleType, SQLHANDLE nHandle,
	   SQLSMALLINT nCompletionType)
{
	ODBCEnv *env = (ODBCEnv *) nHandle;
	ODBCDbc *dbc = (ODBCDbc *) nHandle;
	SQLHANDLE hStmt = NULL;
	RETCODE rc = SQL_ERROR;

	/* check parameters nHandleType and nHandle for validity */
	switch (nHandleType) {
	case SQL_HANDLE_DBC:
		if (!isValidDbc(dbc))
			return SQL_INVALID_HANDLE;
		clearDbcErrors(dbc);
		break;
	case SQL_HANDLE_ENV:
		if (!isValidEnv(env))
			return SQL_INVALID_HANDLE;
		clearEnvErrors(env);

		/* Currently commit/rollback of all connections within
		   this environment handle is NOT implemented. */
		/* report error HYC00 = Optional feature not implemented */
		addEnvError(env, "HYC00", NULL, 0);
		return SQL_ERROR;
	default:
		/* invalid handle type */
		/* set an error only if the handle is valid handle */
		if (isValidDbc(dbc)) {
			clearDbcErrors(dbc);
			addDbcError(dbc, "HY092", NULL, 0);
			return SQL_ERROR;
		} else {
			if (isValidEnv(env)) {
				clearEnvErrors(env);
				addEnvError(env, "HY092", NULL, 0);
				return SQL_ERROR;
			}
		}
		/* else just return error code (no msg) */
		return SQL_ERROR;
	}

	/* check parameter nCompletionType */
	if (nCompletionType != SQL_COMMIT && nCompletionType != SQL_ROLLBACK) {
		/* HY012 = invalid transaction operation code */
		if (nHandleType == SQL_HANDLE_DBC)
			addDbcError(dbc, "HY012", NULL, 0);
		else
			addEnvError(env, "HY012", NULL, 0);
		return SQL_ERROR;
	}


	/* only the case SQL_HANDLE_DBC is supported, see above */
	assert(nHandleType == SQL_HANDLE_DBC);


	/* TODO: implement the code for case: nHandleType == SQL_HANDLE_ENV */
	/* This could be done by calling SQLEndTran() for each dbc allocated
	   within the env (use env->FirstDbc), test if the dbc has an open
	   connection and then call SQLEndTran() on this dbc */


	/* construct a statement object and excute a SQL COMMIT or ROLLBACK */
	rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &hStmt);
	if (rc == SQL_SUCCESS || rc == SQL_SUCCESS_WITH_INFO) {
		ODBCStmt *stmt = (ODBCStmt *) hStmt;
		rc = SQLExecDirect(stmt,
				   (SQLCHAR *) (nCompletionType == SQL_COMMIT ?
						"COMMIT" : "ROLLBACK"),
				   SQL_NTS);
		if (rc == SQL_ERROR || rc == SQL_SUCCESS_WITH_INFO) {
			/* get the error/warning and post in on the dbc handle */
			SQLCHAR sqlState[SQL_SQLSTATE_SIZE + 1];
			SQLINTEGER nativeErrCode;
			SQLCHAR msgText[SQL_MAX_MESSAGE_LENGTH + 1];

			(void) SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1,
					     sqlState, &nativeErrCode, msgText,
					     sizeof(msgText), NULL);

			addDbcError(dbc, (char *) sqlState, (char *) msgText,
				    nativeErrCode);
		}
		/* clean up the statement handle */
		SQLFreeStmt(stmt, SQL_CLOSE);
		SQLFreeHandle(SQL_HANDLE_STMT, stmt);
	} else {
		/* could not allocated a statement object */
		addDbcError(dbc, "HY013", NULL, 0);
		return SQL_ERROR;
	}

	return rc;
}
