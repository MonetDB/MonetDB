/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
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

SQLRETURN GetDiagRec(
	SQLSMALLINT	handleType,	/* must contain a valid type */
	SQLHANDLE	handle,		/* must contain a valid handle */
	SQLSMALLINT	recNumber,	/* must be >= 1 */
	SQLCHAR *	sqlState,	/* may be null */
	SQLINTEGER *	nativeErrorPtr,	/* may be null */
	SQLCHAR *	messageText,	/* may be null */
	SQLSMALLINT	bufferLength,	/* must be >= 0 */
	SQLSMALLINT *	textLengthPtr )	/* may be null */
{
	ODBCEnv * env = NULL;
	ODBCDbc * dbc = NULL;
	ODBCStmt * stmt = NULL;
	ODBCError * err = NULL;
	SQLRETURN retCode = SQL_SUCCESS;

	/* input & output parameters validity checks */
	if ( ! handle )
	{
		return SQL_INVALID_HANDLE;
	}

	switch (handleType) {
		case SQL_HANDLE_ENV:
			env = handle;	/* cast the handle to the proper struct type */
			/* Check if this struct is still valid/alive */
			if ( !(isValidEnv(env)) ) {
				return SQL_INVALID_HANDLE;
			}
			err = getEnvError(env);
			break;
		case SQL_HANDLE_DBC:
			dbc = handle;	/* cast the handle to the proper struct type */
			/* Check if this struct is still valid/alive */
			if ( !(isValidDbc(dbc)) ) {
				return SQL_INVALID_HANDLE;
			}
			err = getDbcError(dbc);
			break;
		case SQL_HANDLE_STMT:
			stmt = handle;	/* cast the handle to the proper struct type */
			/* Check if this struct is still valid/alive */
			if ( !(isValidStmt(stmt)) ) {
				return SQL_INVALID_HANDLE;
			}
			err = getStmtError(stmt);
			break;
		case SQL_HANDLE_DESC:
			/* not yet supported */
			return SQL_NO_DATA;
		default:
			return SQL_INVALID_HANDLE;
	}

	if (recNumber <= 0)
	{
		return SQL_ERROR;
	}
	/* TODO: currently we do not use the recNumber yet.
	 * It needs to be implemented when the bulk data retrieval functionality
	 * (SQLFetchScroll) and/or bulk data manipulations (SQLBulkOperations) is
	 * implemented. Only then it will be possible to have multiple records,
	 * and thus will the recNumber become usefull.
	 */

	if (bufferLength < 0)
	{	/* Note: bufferLength may be 0 !! The DriverManager uses it to discard an error */
		return SQL_ERROR;
	}

	/* Check the error object from the handle, it may be NULL when no (more) errors are available */
	if (err == NULL)
	{
		return SQL_NO_DATA;
	}


	/* Now fill the output parameters where possible */
	assert(err);
	if (sqlState)
	{
		char * state = getSqlState(err);
		int stateLen = 0;
		assert(state);

		stateLen = strlen(state);
		if (stateLen <= SQL_SQLSTATE_SIZE) {	/* SQL state codes are 5 chars wide normally */
			strcpy((char *)sqlState, state);
		} else {
			/* copy only the first 5 chars in the buffer and make it null terminated */
			strncpy((char *)sqlState, state, SQL_SQLSTATE_SIZE);
			messageText[SQL_SQLSTATE_SIZE] = '\0';
		}
	}

	if (nativeErrorPtr)
	{
		*nativeErrorPtr = getNativeErrorCode(err);
	}

	{
		char * msg = getMessage(err);
		int msgLen = (msg) ? strlen(msg) : 0;
		msgLen += ODBCErrorMsgPrefixLength;	/* the total formatted error msg length */

		if (messageText && bufferLength > 0)
		{
			if (msg) {
				if (msgLen < bufferLength) {	/* bufferLength needs 1 place for null terminator */
					/* first write the error message prefix text: [MonetDB][ODBC driver 1.0] */
					/* this is required by the ODBC spec and used to determine where the error originated */
					strcpy((char *)messageText, ODBCErrorMsgPrefix);

					/* next append the error msg itself */
					strcat((char *)messageText, msg);
				} else {
					/* copy only the data that fits in the buffer and make it null terminated */
					/* use an extra tmp_str storage */
					char * tmp_str = (char *) GDKmalloc(msgLen + 1);
					strcpy((char *)messageText, ODBCErrorMsgPrefix);
					strcat((char *)messageText, msg);

					strncpy((char *)messageText, tmp_str, bufferLength - 1);
					messageText[bufferLength - 1] = '\0';
					GDKfree(tmp_str);

					retCode = SQL_SUCCESS_WITH_INFO;
				}
			} else {
				/*  No message available. Only the prefix can be copied. */
				if (ODBCErrorMsgPrefixLength < bufferLength) {
					strcpy((char *)messageText, ODBCErrorMsgPrefix);
				} else {
					/* the buffer is too small to write the complete prefix */
					strncpy((char *)messageText, ODBCErrorMsgPrefix, bufferLength - 1);
					messageText[bufferLength - 1] = '\0';
					retCode = SQL_SUCCESS_WITH_INFO;
				}
			}
		} else {
			/* There is no valid messageText buffer or its buffer size is 0.
			* In these cases we cannot write the prefix and message.
			* We just set the return code.
			*/
			retCode = SQL_SUCCESS_WITH_INFO;
		}

		if (textLengthPtr)
		{
			*textLengthPtr = (SQLSMALLINT)msgLen;
		}
	}


	/* The ODBCError object is already removed from the list */

	/* Now we have copied the error info we must remove the error object itself */
	deleteODBCError(err);

	return retCode;
}

SQLRETURN SQLGetDiagRec(
	SQLSMALLINT	handleType,	/* must contain a valid type */
	SQLHANDLE	handle,		/* must contain a valid handle */
	SQLSMALLINT	recNumber,	/* must be >= 1 */
	SQLCHAR *	sqlState,	/* may be null */
	SQLINTEGER *	nativeErrorPtr,	/* may be null */
	SQLCHAR *	messageText,	/* may be null */
	SQLSMALLINT	bufferLength,	/* must be >= 0 */
	SQLSMALLINT *	textLengthPtr )	/* may be null */
{
	return GetDiagRec( handleType, handle, recNumber, sqlState, nativeErrorPtr, messageText, bufferLength, textLengthPtr);
}
