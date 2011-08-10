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
 * SQLGetDiagRec()
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
#include "ODBCUtil.h"

SQLRETURN
SQLGetDiagRec_(SQLSMALLINT HandleType,
	       SQLHANDLE Handle,
	       SQLSMALLINT RecNumber,
	       SQLCHAR *SQLState,
	       SQLINTEGER *NativeErrorPtr,
	       SQLCHAR *MessageText,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *TextLengthPtr)
{
	ODBCError *err;
	SQLRETURN retCode;
	char *msg;
	SQLSMALLINT msgLen;

	switch (HandleType) {
	case SQL_HANDLE_ENV:
		/* Check if this struct is still valid/alive */
		if (!isValidEnv((ODBCEnv *) Handle))
			return SQL_INVALID_HANDLE;
		err = getEnvError((ODBCEnv *) Handle);
		break;
	case SQL_HANDLE_DBC:
		/* Check if this struct is still valid/alive */
		if (!isValidDbc((ODBCDbc *) Handle))
			return SQL_INVALID_HANDLE;
		err = getDbcError((ODBCDbc *) Handle);
		break;
	case SQL_HANDLE_STMT:
		/* Check if this struct is still valid/alive */
		if (!isValidStmt((ODBCStmt *) Handle))
			return SQL_INVALID_HANDLE;
		err = getStmtError((ODBCStmt *) Handle);
		break;
	case SQL_HANDLE_DESC:
		/* not yet supported */
		return Handle ? SQL_NO_DATA : SQL_INVALID_HANDLE;
	default:
		return SQL_INVALID_HANDLE;
	}

	/* Note: BufferLength may be 0 !! */
	if (BufferLength < 0)
		return SQL_ERROR;

	if (RecNumber <= 0)
		return SQL_ERROR;

	err = getErrorRec(err, RecNumber);

	/* Check the error object from the handle, it may be NULL when
	 * no (more) errors are available
	 */
	if (err == NULL)
		return SQL_NO_DATA;

	/* Now fill the output parameters where possible */
	if (SQLState) {
		char *state = getSqlState(err);

		assert(state);
		/* copy only the first SQL_SQLSTATE_SIZE (5) chars in
		 * the buffer and make it null terminated
		 */
		strncpy((char *) SQLState, state, SQL_SQLSTATE_SIZE);
		SQLState[SQL_SQLSTATE_SIZE] = 0;
	}

	if (NativeErrorPtr)
		*NativeErrorPtr = getNativeErrorCode(err);

	msg = getMessage(err);
	msgLen = msg ? (SQLSMALLINT) strlen(msg) : 0;
	retCode = SQL_SUCCESS;

	if (MessageText && BufferLength > 0) {
		BufferLength--;	/* reserve space for term NULL byte */
		MessageText[BufferLength] = 0;	/* write it already */

		/* first write the error message prefix text:
		 * [MonetDB][ODBC driver 1.0]; this is
		 * required by the ODBC spec and used to
		 * determine where the error originated
		 */
		if (BufferLength > 0)
			strncpy((char *) MessageText, ODBCErrorMsgPrefix, BufferLength);
		BufferLength -= ODBCErrorMsgPrefixLength;
		MessageText += ODBCErrorMsgPrefixLength;

		/* next append the error msg itself */
		if (msg && BufferLength > 0) {
			strncpy((char *) MessageText, msg, BufferLength);
			BufferLength -= msgLen;
		}

		if (BufferLength < 0) {
			/* it didn't fit */
			retCode = SQL_SUCCESS_WITH_INFO;
		}
	} else {
		/* There is no valid MessageText buffer or its
		 * buffer size is 0.  In these cases we cannot
		 * write the prefix and message.  We just set
		 * the return code.
		 */
		retCode = SQL_SUCCESS_WITH_INFO;
	}

	if (TextLengthPtr)
		*TextLengthPtr = (SQLSMALLINT) (msgLen + ODBCErrorMsgPrefixLength);

	return retCode;
}

SQLRETURN SQL_API
SQLGetDiagRec(SQLSMALLINT HandleType,
	      SQLHANDLE Handle,
	      SQLSMALLINT RecNumber,
	      SQLCHAR *SQLState,
	      SQLINTEGER *NativeErrorPtr,
	      SQLCHAR *MessageText,
	      SQLSMALLINT BufferLength,
	      SQLSMALLINT *TextLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagRec %s " PTRFMT " %d %d\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, (int) RecNumber, (int) BufferLength);
#endif

	return SQLGetDiagRec_(HandleType,
			      Handle,
			      RecNumber,
			      SQLState,
			      NativeErrorPtr,
			      MessageText,
			      BufferLength,
			      TextLengthPtr);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetDiagRecA(SQLSMALLINT HandleType,
	       SQLHANDLE Handle,
	       SQLSMALLINT RecNumber,
	       SQLCHAR *SQLState,
	       SQLINTEGER *NativeErrorPtr,
	       SQLCHAR *MessageText,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *TextLengthPtr)
{
	return SQLGetDiagRec(HandleType,
			     Handle,
			     RecNumber,
			     SQLState,
			     NativeErrorPtr,
			     MessageText,
			     BufferLength,
			     TextLengthPtr);
}

SQLRETURN SQL_API
SQLGetDiagRecW(SQLSMALLINT HandleType,
	       SQLHANDLE Handle,
	       SQLSMALLINT RecNumber,
	       SQLWCHAR *SQLState,
	       SQLINTEGER *NativeErrorPtr,
	       SQLWCHAR *MessageText,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *TextLengthPtr)
{
	SQLRETURN rc;
	SQLCHAR state[6];
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagRecW %s " PTRFMT " %d %d\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, (int) RecNumber, (int) BufferLength);
#endif

	/* figure out how much space we need */
	rc = SQLGetDiagRec_(HandleType, Handle, RecNumber, state,
			    NativeErrorPtr, NULL, 0, &n);

	if (SQL_SUCCEEDED(rc)) {
		SQLCHAR *msg;

		/* then try for real */
		msg = (SQLCHAR *) malloc(n + 1);
		rc = SQLGetDiagRec_(HandleType, Handle, RecNumber, state,
				    NativeErrorPtr, msg, n + 1, &n);
#ifdef ODBCDEBUG
		ODBCLOG("SQLGetDiagRecW: %s\n", (char *) msg);
#endif

		if (SQL_SUCCEEDED(rc)) {
			char *e = ODBCutf82wchar(state, 5, SQLState, 6, NULL);

			if (e)
				rc = SQL_ERROR;
		}

		if (SQL_SUCCEEDED(rc)) {
			char *e = ODBCutf82wchar(msg, n, MessageText,
						 BufferLength, &n);

			if (e)
				rc = SQL_ERROR;
			if (TextLengthPtr)
				*TextLengthPtr = n;
		}
		free(msg);
	}

	return rc;
}
#endif /* WITH_WCHAR */
