/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
 * SQLGetDiagField()
 * ODBC 3.0 API function
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"
#include "ODBCUtil.h"

#define copyDiagString(str, buf, len, lenp)				\
		do {							\
			SQLSMALLINT _l;					\
			if (len < 0)					\
				return SQL_ERROR;			\
			_l = (SQLSMALLINT) strlen(str);			\
			if (buf && len > 0)				\
				strncpy((char *) buf, str, len);	\
			if (lenp)					\
				*lenp = _l;				\
			if (buf == NULL || _l >= len)			\
				return SQL_SUCCESS_WITH_INFO;		\
		} while (0)

static SQLRETURN
MNDBGetDiagField(SQLSMALLINT HandleType,
		 SQLHANDLE Handle,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT DiagIdentifier,
		 SQLPOINTER DiagInfoPtr,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLengthPtr)
{
	ODBCError *err;
	ODBCDbc *dbc = NULL;

	/* input & output parameters validity checks */

	switch (HandleType) {
	case SQL_HANDLE_ENV:
		/* Check if this struct is still valid/alive */
		if (!isValidEnv((ODBCEnv *) Handle))
			return SQL_INVALID_HANDLE;
		err = ((ODBCEnv *) Handle)->Error;
		break;
	case SQL_HANDLE_DBC:
		/* Check if this struct is still valid/alive */
		dbc = (ODBCDbc *) Handle;
		if (!isValidDbc(dbc))
			return SQL_INVALID_HANDLE;
		err = dbc->Error;
		break;
	case SQL_HANDLE_STMT:
		/* Check if this struct is still valid/alive */
		if (!isValidStmt((ODBCStmt *) Handle))
			return SQL_INVALID_HANDLE;
		err = ((ODBCStmt *) Handle)->Error;
		dbc = ((ODBCStmt *) Handle)->Dbc;
		break;
	case SQL_HANDLE_DESC:
		/* Check if this struct is still valid/alive */
		if (!isValidDesc((ODBCDesc *) Handle))
			return SQL_INVALID_HANDLE;
		err = ((ODBCDesc *) Handle)->Error;
		dbc = ((ODBCDesc *) Handle)->Dbc;
		break;
	default:
		return SQL_INVALID_HANDLE;
	}

	/* header fields */
	switch (DiagIdentifier) {
	case SQL_DIAG_CURSOR_ROW_COUNT:		/* SQLLEN */
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLLEN *) DiagInfoPtr = (SQLLEN) ((ODBCStmt *) Handle)->rowSetSize;
		return SQL_SUCCESS;
	case SQL_DIAG_DYNAMIC_FUNCTION:		/* SQLCHAR* */
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		copyDiagString("", DiagInfoPtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:	/* SQLINTEGER */
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLINTEGER *) DiagInfoPtr = SQL_DIAG_UNKNOWN_STATEMENT;
		return SQL_SUCCESS;
	case SQL_DIAG_NUMBER:			/* SQLINTEGER */
		*(SQLINTEGER *) DiagInfoPtr = getErrorRecCount(err);
		return SQL_SUCCESS;
	case SQL_DIAG_RETURNCODE:		/* SQLRETURN */
		*(SQLRETURN *) DiagInfoPtr = SQL_SUCCESS;
		return SQL_SUCCESS;
	case SQL_DIAG_ROW_COUNT:		/* SQLLEN */
		if (HandleType != SQL_HANDLE_STMT || ((ODBCStmt *) Handle)->State < EXECUTED0)
			return SQL_ERROR;
		*(SQLLEN *) DiagInfoPtr = (SQLLEN) ((ODBCStmt *) Handle)->rowcount;
		return SQL_SUCCESS;
	}

	/* record fields */
	if (RecNumber <= 0)
		return SQL_ERROR;

	err = getErrorRec(err, RecNumber);
	if (err == NULL)
		return SQL_NO_DATA;

	switch (DiagIdentifier) {
	case SQL_DIAG_CLASS_ORIGIN:{		/* SQLCHAR* */
		char *msg = strncmp(getSqlState(err), "IM", 2) == 0 ? "ODBC 3.0" : "ISO 9075";

		copyDiagString(msg, DiagInfoPtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_COLUMN_NUMBER:		/* SQLINTEGER */
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLINTEGER *) DiagInfoPtr = SQL_COLUMN_NUMBER_UNKNOWN;
		return SQL_SUCCESS;
	case SQL_DIAG_CONNECTION_NAME:{		/* SQLCHAR* */
		char *msg = "MonetDB ODBC/Mapi";

		copyDiagString(msg, DiagInfoPtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
#if 0
/* not clear yet what to return here */
	case SQL_DIAG_MESSAGE_TEXT: {		/* SQLCHAR* */
		char msg[1024];
		snprintf(msg, sizeof(msg), "");
		copyDiagString(msg, DiagInfoPtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
#endif
	case SQL_DIAG_NATIVE:			/* SQLINTEGER */
		*(SQLINTEGER *) DiagInfoPtr = getNativeErrorCode(err);
		return SQL_SUCCESS;
	case SQL_DIAG_ROW_NUMBER:		/* SQLLEN */
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLLEN *) DiagInfoPtr = SQL_ROW_NUMBER_UNKNOWN;
		return SQL_SUCCESS;
	case SQL_DIAG_SERVER_NAME:{		/* SQLCHAR* */
		char *msg = dbc && dbc->Connected && dbc->dsn ? dbc->dsn : "";

		copyDiagString(msg, DiagInfoPtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_SQLSTATE:{		/* SQLCHAR* */
		char *msg = getSqlState(err);

		copyDiagString(msg, DiagInfoPtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_SUBCLASS_ORIGIN:{		/* SQLCHAR* */
		char *state = getSqlState(err);
		char *msg;

		if (('0' <= state[0] && state[0] <= '4') ||
		    ('A' <= state[0] && state[0] <= 'H'))
			msg = "ISO 9075"; /* defined by standard */
		else
			msg = "ODBC 3.0"; /* effectively just "IM" */

		copyDiagString(msg, DiagInfoPtr, BufferLength, StringLengthPtr);
		return SQL_SUCCESS;
	}
	}

	/* Currently no Diagnostic Fields are supported.
	   Hence we always return NO_DATA */
	return SQL_NO_DATA;
}

#ifdef ODBCDEBUG
static char *
translateDiagIdentifier(SQLSMALLINT DiagIdentifier)
{
	static char unknown[32];

	switch (DiagIdentifier) {
	case SQL_DIAG_CLASS_ORIGIN:
		return "SQL_DIAG_CLASS_ORIGIN";
	case SQL_DIAG_COLUMN_NUMBER:
		return "SQL_DIAG_COLUMN_NUMBER";
	case SQL_DIAG_CONNECTION_NAME:
		return "SQL_DIAG_CONNECTION_NAME";
	case SQL_DIAG_CURSOR_ROW_COUNT:
		return "SQL_DIAG_CURSOR_ROW_COUNT";
	case SQL_DIAG_DYNAMIC_FUNCTION:
		return "SQL_DIAG_DYNAMIC_FUNCTION";
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
		return "SQL_DIAG_DYNAMIC_FUNCTION_CODE";
	case SQL_DIAG_MESSAGE_TEXT:
		return "SQL_DIAG_MESSAGE_TEXT";
	case SQL_DIAG_NATIVE:
		return "SQL_DIAG_NATIVE";
	case SQL_DIAG_NUMBER:
		return "SQL_DIAG_NUMBER";
	case SQL_DIAG_RETURNCODE:
		return "SQL_DIAG_RETURNCODE";
	case SQL_DIAG_ROW_COUNT:
		return "SQL_DIAG_ROW_COUNT";
	case SQL_DIAG_ROW_NUMBER:
		return "SQL_DIAG_ROW_NUMBER";
	case SQL_DIAG_SERVER_NAME:
		return "SQL_DIAG_SERVER_NAME";
	case SQL_DIAG_SQLSTATE:
		return "SQL_DIAG_SQLSTATE";
	case SQL_DIAG_SUBCLASS_ORIGIN:
		return "SQL_DIAG_SUBCLASS_ORIGIN";
	default:
		snprintf(unknown, sizeof(unknown), "unknown (%d)",
			 (int) DiagIdentifier);
		return unknown;
	}
}
#endif

SQLRETURN SQL_API
SQLGetDiagField(SQLSMALLINT HandleType,
		SQLHANDLE Handle,
		SQLSMALLINT RecNumber,
		SQLSMALLINT DiagIdentifier,
		SQLPOINTER DiagInfoPtr,
		SQLSMALLINT BufferLength,
		SQLSMALLINT *StringLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagField %s " PTRFMT " %d %s " PTRFMT " %d " PTRFMT "\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, (int) RecNumber,
		translateDiagIdentifier(DiagIdentifier),
		PTRFMTCAST DiagInfoPtr,
		(int) BufferLength, PTRFMTCAST StringLengthPtr);
#endif

	return MNDBGetDiagField(HandleType,
				Handle,
				RecNumber,
				DiagIdentifier,
				DiagInfoPtr,
				BufferLength,
				StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetDiagFieldA(SQLSMALLINT HandleType,
		 SQLHANDLE Handle,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT DiagIdentifier,
		 SQLPOINTER DiagInfoPtr,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLengthPtr)
{
	return SQLGetDiagField(HandleType,
			       Handle,
			       RecNumber,
			       DiagIdentifier,
			       DiagInfoPtr,
			       BufferLength,
			       StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetDiagFieldW(SQLSMALLINT HandleType,
		 SQLHANDLE Handle,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT DiagIdentifier,
		 SQLPOINTER DiagInfoPtr,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLengthPtr)
{
	SQLRETURN rc;
	SQLPOINTER ptr = NULL;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagFieldW %s " PTRFMT " %d %s " PTRFMT " %d " PTRFMT "\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, (int) RecNumber,
		translateDiagIdentifier(DiagIdentifier),
		PTRFMTCAST DiagInfoPtr,
		(int) BufferLength, PTRFMTCAST StringLengthPtr);
#endif

	switch (DiagIdentifier) {
		/* all string attributes */
	case SQL_DIAG_DYNAMIC_FUNCTION:
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_CONNECTION_NAME:
	case SQL_DIAG_MESSAGE_TEXT:
	case SQL_DIAG_SERVER_NAME:
	case SQL_DIAG_SQLSTATE:
	case SQL_DIAG_SUBCLASS_ORIGIN:
		rc = MNDBGetDiagField(HandleType, Handle, RecNumber,
				      DiagIdentifier, NULL, 0, &n);
		if (!SQL_SUCCEEDED(rc))
			return rc;
		n++;		/* account for NUL byte */
		ptr = (SQLPOINTER) malloc(n);
		break;
	default:
		n = BufferLength;
		ptr = DiagInfoPtr;
		break;
	}

	rc = MNDBGetDiagField(HandleType, Handle, RecNumber,
			      DiagIdentifier, ptr, n, &n);
#ifdef ODBCDEBUG
	if (ptr != DiagInfoPtr)
		ODBCLOG("SQLGetDiagFieldW: %s\n", (char *) ptr);
#endif

	if (ptr != DiagInfoPtr) {
		if (SQL_SUCCEEDED(rc)) {
			char *e = ODBCutf82wchar(ptr, n, DiagInfoPtr,
						 BufferLength / 2, &n);

			if (e)
				rc = SQL_ERROR;
			if (StringLengthPtr)
				*StringLengthPtr = n * 2;
		}
		free(ptr);
	}

	return rc;
}
