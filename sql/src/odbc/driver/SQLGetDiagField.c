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
 * Portions created by CWI are Copyright (C) 1997-2007 CWI.
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
			_l = str ? (SQLSMALLINT) strlen((char *) str) : 0; \
			if (buf)					\
				strncpy((char *) buf, str ? (char *) str : "", len); \
			if (lenp)					\
				*lenp = _l;				\
			if (buf == NULL || _l >= len)			\
				return SQL_SUCCESS_WITH_INFO;		\
		} while (0)

static SQLRETURN
SQLGetDiagField_(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier, SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)
{
	ODBCError *err;

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
		if (!isValidDbc((ODBCDbc *) Handle))
			return SQL_INVALID_HANDLE;
		err = ((ODBCDbc *) Handle)->Error;
		break;
	case SQL_HANDLE_STMT:
		/* Check if this struct is still valid/alive */
		if (!isValidStmt((ODBCStmt *) Handle))
			return SQL_INVALID_HANDLE;
		err = ((ODBCStmt *) Handle)->Error;
		break;
	case SQL_HANDLE_DESC:
		/* Check if this struct is still valid/alive */
		if (!isValidDesc((ODBCDesc *) Handle))
			return SQL_INVALID_HANDLE;
		err = ((ODBCDesc *) Handle)->Error;
		break;
	default:
		return SQL_INVALID_HANDLE;
	}

	/* header fields */
	switch (DiagIdentifier) {
	case SQL_DIAG_CURSOR_ROW_COUNT:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLINTEGER *) DiagInfo = ((ODBCStmt *) Handle)->rowSetSize;
		return SQL_SUCCESS;
	case SQL_DIAG_DYNAMIC_FUNCTION:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		copyDiagString("", DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLINTEGER *) DiagInfo = SQL_DIAG_UNKNOWN_STATEMENT;
		return SQL_SUCCESS;
	case SQL_DIAG_NUMBER:
		*(SQLINTEGER *) DiagInfo = getErrorRecCount(err);
		return SQL_SUCCESS;
	case SQL_DIAG_RETURNCODE:
		*(SQLRETURN *) DiagInfo = SQL_SUCCESS;
		return SQL_SUCCESS;
	case SQL_DIAG_ROW_COUNT:
		if (HandleType != SQL_HANDLE_STMT || ((ODBCStmt *) Handle)->State < EXECUTED0)
			return SQL_ERROR;
		*(SQLINTEGER *) DiagInfo = (SQLINTEGER) ((ODBCStmt *) Handle)->rowcount;
		return SQL_SUCCESS;
	}

	/* record fields */
	if (RecNumber <= 0)
		return SQL_ERROR;

	err = getErrorRec(err, RecNumber);
	if (err == NULL)
		return SQL_NO_DATA;

	switch (DiagIdentifier) {
	case SQL_DIAG_CLASS_ORIGIN:{
		char *msg = strncmp(getSqlState(err), "IM", 2) == 0 ? "ODBC 3.0" : "ISO 9075";

		copyDiagString(msg, DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_COLUMN_NUMBER:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLINTEGER *) DiagInfo = SQL_COLUMN_NUMBER_UNKNOWN;
		return SQL_SUCCESS;
	}

	/* Currently no Diagnostic Fields are supported.
	   Hence we always return NO_DATA */
	return SQL_NO_DATA;
}

SQLRETURN SQL_API
SQLGetDiagField(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier, SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagField %s " PTRFMT "\n", HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc", PTRFMTCAST Handle);
#endif

	return SQLGetDiagField_(HandleType, Handle, RecNumber, DiagIdentifier, DiagInfo, BufferLength, StringLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetDiagFieldA(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier, SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)
{
	return SQLGetDiagField(HandleType, Handle, RecNumber, DiagIdentifier, DiagInfo, BufferLength, StringLength);
}

SQLRETURN SQL_API
SQLGetDiagFieldW(SQLSMALLINT HandleType, SQLHANDLE Handle, SQLSMALLINT RecNumber, SQLSMALLINT DiagIdentifier, SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)
{
	SQLRETURN rc;
	SQLPOINTER ptr;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagFieldW %s " PTRFMT "\n", HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc", PTRFMTCAST Handle);
#endif

	switch (DiagIdentifier) {
		/* all string attributes */
	case SQL_DIAG_DYNAMIC_FUNCTION:
	case SQL_DIAG_CLASS_ORIGIN:
		n = BufferLength * 4;
		ptr = (SQLPOINTER) malloc(n);
		break;
	default:
		n = BufferLength;
		ptr = DiagInfo;
		break;
	}

	rc = SQLGetDiagField_(HandleType, Handle, RecNumber, DiagIdentifier, ptr, n, &n);

	if (ptr !=DiagInfo) {
		if (SQL_SUCCEEDED(rc)) {
			char *e = ODBCutf82wchar(ptr, n, DiagInfo, BufferLength, &n);

			if (e)
				rc = SQL_ERROR;
			if (StringLength)
				*StringLength = n;
		}
		free(ptr);
	}

	return rc;
}
#endif /* WITH_WCHAR */
