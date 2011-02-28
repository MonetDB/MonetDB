/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * SQLGetDiagRec()
 * CLI Compliance: ISO 92
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
#include "ODBCUtil.h"

SQLRETURN
SQLGetDiagRec_(SQLSMALLINT handleType,
	       SQLHANDLE handle,
	       SQLSMALLINT recNumber,
	       SQLCHAR *sqlState,
	       SQLINTEGER *nativeErrorPtr,
	       SQLCHAR *messageText,
	       SQLSMALLINT bufferLength,
	       SQLSMALLINT *textLengthPtr)
{
	ODBCError *err;
	SQLRETURN retCode;
	char *msg;
	SQLSMALLINT msgLen;

	switch (handleType) {
	case SQL_HANDLE_ENV:
		/* Check if this struct is still valid/alive */
		if (!isValidEnv((ODBCEnv *) handle))
			return SQL_INVALID_HANDLE;
		err = getEnvError((ODBCEnv *) handle);
		break;
	case SQL_HANDLE_DBC:
		/* Check if this struct is still valid/alive */
		if (!isValidDbc((ODBCDbc *) handle))
			return SQL_INVALID_HANDLE;
		err = getDbcError((ODBCDbc *) handle);
		break;
	case SQL_HANDLE_STMT:
		/* Check if this struct is still valid/alive */
		if (!isValidStmt((ODBCStmt *) handle))
			return SQL_INVALID_HANDLE;
		err = getStmtError((ODBCStmt *) handle);
		break;
	case SQL_HANDLE_DESC:
		/* not yet supported */
		return handle ? SQL_NO_DATA : SQL_INVALID_HANDLE;
	default:
		return SQL_INVALID_HANDLE;
	}

	/* Note: bufferLength may be 0 !! */
	if (bufferLength < 0)
		return SQL_ERROR;

	if (recNumber <= 0)
		return SQL_ERROR;

	err = getErrorRec(err, recNumber);

	/* Check the error object from the handle, it may be NULL when
	 * no (more) errors are available
	 */
	if (err == NULL)
		return SQL_NO_DATA;

	/* Now fill the output parameters where possible */
	if (sqlState) {
		char *state = getSqlState(err);

		assert(state);
		/* copy only the first SQL_SQLSTATE_SIZE (5) chars in
		 * the buffer and make it null terminated
		 */
		strncpy((char *) sqlState, state, SQL_SQLSTATE_SIZE);
		sqlState[SQL_SQLSTATE_SIZE] = 0;
	}

	if (nativeErrorPtr)
		*nativeErrorPtr = getNativeErrorCode(err);

	msg = getMessage(err);
	msgLen = msg ? (SQLSMALLINT) strlen(msg) : 0;
	retCode = SQL_SUCCESS;

	if (messageText && bufferLength > 0) {
		bufferLength--;	/* reserve space for term NULL byte */
		messageText[bufferLength] = 0;	/* write it already */

		/* first write the error message prefix text:
		 * [MonetDB][ODBC driver 1.0]; this is
		 * required by the ODBC spec and used to
		 * determine where the error originated
		 */
		if (bufferLength > 0)
			strncpy((char *) messageText, ODBCErrorMsgPrefix, bufferLength);
		bufferLength -= ODBCErrorMsgPrefixLength;
		messageText += ODBCErrorMsgPrefixLength;

		/* next append the error msg itself */
		if (msg && bufferLength > 0) {
			strncpy((char *) messageText, msg, bufferLength);
			bufferLength -= msgLen;
		}

		if (bufferLength < 0) {
			/* it didn't fit */
			retCode = SQL_SUCCESS_WITH_INFO;
		}
	} else {
		/* There is no valid messageText buffer or its
		 * buffer size is 0.  In these cases we cannot
		 * write the prefix and message.  We just set
		 * the return code.
		 */
		retCode = SQL_SUCCESS_WITH_INFO;
	}

	if (textLengthPtr)
		*textLengthPtr = (SQLSMALLINT) (msgLen + ODBCErrorMsgPrefixLength);

	return retCode;
}

SQLRETURN SQL_API
SQLGetDiagRec(SQLSMALLINT handleType,
	      SQLHANDLE handle,
	      SQLSMALLINT recNumber,
	      SQLCHAR *sqlState,
	      SQLINTEGER *nativeErrorPtr,
	      SQLCHAR *messageText,
	      SQLSMALLINT bufferLength,
	      SQLSMALLINT *textLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagRec %s " PTRFMT " %d %d\n",
		handleType == SQL_HANDLE_ENV ? "Env" : handleType == SQL_HANDLE_DBC ? "Dbc" : handleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST handle, (int) recNumber, (int) bufferLength);
#endif

	return SQLGetDiagRec_(handleType, handle, recNumber, sqlState, nativeErrorPtr, messageText, bufferLength, textLengthPtr);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetDiagRecA(SQLSMALLINT handleType,
	       SQLHANDLE handle,
	       SQLSMALLINT recNumber,
	       SQLCHAR *sqlState,
	       SQLINTEGER *nativeErrorPtr,
	       SQLCHAR *messageText,
	       SQLSMALLINT bufferLength,
	       SQLSMALLINT *textLengthPtr)
{
	return SQLGetDiagRec(handleType, handle, recNumber, sqlState, nativeErrorPtr, messageText, bufferLength, textLengthPtr);
}

SQLRETURN SQL_API
SQLGetDiagRecW(SQLSMALLINT handleType,
	       SQLHANDLE handle,
	       SQLSMALLINT recNumber,
	       SQLWCHAR * sqlState,
	       SQLINTEGER *nativeErrorPtr,
	       SQLWCHAR * messageText,
	       SQLSMALLINT bufferLength,
	       SQLSMALLINT *textLengthPtr)
{
	SQLRETURN rc;
	SQLCHAR state[6];
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagRecW %s " PTRFMT " %d %d\n",
		handleType == SQL_HANDLE_ENV ? "Env" : handleType == SQL_HANDLE_DBC ? "Dbc" : handleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST handle, (int) recNumber, (int) bufferLength);
#endif

	/* figure out how much space we need */
	rc = SQLGetDiagRec_(handleType, handle, recNumber, state, nativeErrorPtr, NULL, 0, &n);

	if (SQL_SUCCEEDED(rc)) {
		SQLCHAR *msg;

		/* then try for real */
		msg = (SQLCHAR *) malloc(n + 1);
		rc = SQLGetDiagRec_(handleType, handle, recNumber, state, nativeErrorPtr, msg, n + 1, &n);
#ifdef ODBCDEBUG
		ODBCLOG("SQLGetDiagRecW: %s\n", (char *) msg);
#endif

		if (SQL_SUCCEEDED(rc)) {
			char *e = ODBCutf82wchar(state, 5, sqlState, 6, NULL);

			if (e)
				rc = SQL_ERROR;
		}

		if (SQL_SUCCEEDED(rc)) {
			char *e = ODBCutf82wchar(msg, n, messageText, bufferLength, &n);

			if (e)
				rc = SQL_ERROR;
			if (textLengthPtr)
				*textLengthPtr = n;
		}
		free(msg);
	}

	return rc;
}
#endif /* WITH_WCHAR */
