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

SQLRETURN
SQLGetDiagRec_(SQLSMALLINT handleType,	/* must contain a valid type */
	       SQLHANDLE handle,      /* must contain a valid handle */
	       SQLSMALLINT recNumber,  /* must be >= 1 */
	       SQLCHAR *sqlState,	/* may be null */
	       SQLINTEGER *nativeErrorPtr,/* may be null */
	       SQLCHAR *messageText,	  /* may be null */
	       SQLSMALLINT bufferLength,   /* must be >= 0 */
	       SQLSMALLINT *textLengthPtr)
{				/* may be null */
	ODBCError *err;
	SQLRETURN retCode;
	char *msg;
	size_t msgLen;

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
		sqlState[SQL_SQLSTATE_SIZE] = '\0';
	}

	if (nativeErrorPtr)
		*nativeErrorPtr = getNativeErrorCode(err);

	msg = getMessage(err);
	msgLen = msg ? strlen(msg) : 0;
	retCode = SQL_SUCCESS;

	if (messageText && bufferLength > 0) {
		bufferLength--;	       /* reserve space for term NULL byte */
		messageText[bufferLength] = '\0';/* write it already */

		/* first write the error message prefix text:
		 * [MonetDB][ODBC driver 1.0]; this is
		 * required by the ODBC spec and used to
		 * determine where the error originated
		 */
		if (bufferLength > 0)
			strncpy((char *) messageText,
				ODBCErrorMsgPrefix, bufferLength);
		bufferLength -= ODBCErrorMsgPrefixLength;
		messageText += ODBCErrorMsgPrefixLength;

		/* next append the error msg itself */
		if (msg && bufferLength > 0) {
			strncpy((char *) messageText, msg,
				bufferLength);
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

SQLRETURN
SQLGetDiagRec(SQLSMALLINT handleType,	/* must contain a valid type */
	      SQLHANDLE handle,	/* must contain a valid handle */
	      SQLSMALLINT recNumber,	/* must be >= 1 */
	      SQLCHAR *sqlState,	/* may be null */
	      SQLINTEGER *nativeErrorPtr,	/* may be null */
	      SQLCHAR *messageText,	/* may be null */
	      SQLSMALLINT bufferLength,	/* must be >= 0 */
	      SQLSMALLINT *textLengthPtr)
{
	return SQLGetDiagRec_(handleType, handle, recNumber, sqlState,
			      nativeErrorPtr, messageText, bufferLength,
			      textLengthPtr);
}
