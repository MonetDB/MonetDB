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
SQLGetDiagField_(SQLSMALLINT HandleType,
		 SQLHANDLE Handle,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT DiagIdentifier,
		 SQLPOINTER DiagInfo,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLength)
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
	case SQL_DIAG_CURSOR_ROW_COUNT:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		*(SQLLEN *) DiagInfo = (SQLLEN) ((ODBCStmt *) Handle)->rowSetSize;
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
		*(SQLLEN *) DiagInfo = (SQLLEN) ((ODBCStmt *) Handle)->rowcount;
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
	case SQL_DIAG_CONNECTION_NAME:{
		char *msg = "MonetDB ODBC/Mapi";

		copyDiagString(msg, DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_SERVER_NAME:{
		char *msg = dbc && dbc->Connected && dbc->dsn ? dbc->dsn : "";

		copyDiagString(msg, DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_SQLSTATE:{
		char *msg = getSqlState(err);

		copyDiagString(msg, DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_SUBCLASS_ORIGIN:{
		char *state = getSqlState(err);
		char *msg;

		if (strcmp(state, "01S00") == 0 ||
		    strcmp(state, "01S01") == 0 ||
		    strcmp(state, "01S02") == 0 ||
		    strcmp(state, "01S06") == 0 ||
		    strcmp(state, "01S07") == 0 ||
		    strcmp(state, "07S01") == 0 ||
		    strcmp(state, "08S01") == 0 ||
		    strcmp(state, "21S01") == 0 ||
		    strcmp(state, "21S02") == 0 ||
		    strcmp(state, "25S01") == 0 ||
		    strcmp(state, "25S02") == 0 ||
		    strcmp(state, "25S03") == 0 ||
		    strcmp(state, "42S01") == 0 ||
		    strcmp(state, "42S02") == 0 ||
		    strcmp(state, "42S11") == 0 ||
		    strcmp(state, "42S12") == 0 ||
		    strcmp(state, "42S21") == 0 ||
		    strcmp(state, "42S22") == 0 ||
		    strcmp(state, "HY095") == 0 ||
		    strcmp(state, "HY097") == 0 ||
		    strcmp(state, "HY098") == 0 ||
		    strcmp(state, "HY099") == 0 ||
		    strcmp(state, "HY100") == 0 ||
		    strcmp(state, "HY101") == 0 ||
		    strcmp(state, "HY105") == 0 ||
		    strcmp(state, "HY107") == 0 ||
		    strcmp(state, "HY109") == 0 ||
		    strcmp(state, "HY110") == 0 ||
		    strcmp(state, "HY111") == 0 ||
		    strcmp(state, "HYT00") == 0 ||
		    strcmp(state, "HYT01") == 0 ||
		    strcmp(state, "IM001") == 0 ||
		    strcmp(state, "IM002") == 0 ||
		    strcmp(state, "IM003") == 0 ||
		    strcmp(state, "IM004") == 0 ||
		    strcmp(state, "IM005") == 0 ||
		    strcmp(state, "IM006") == 0 ||
		    strcmp(state, "IM007") == 0 ||
		    strcmp(state, "IM008") == 0 ||
		    strcmp(state, "IM010") == 0 ||
		    strcmp(state, "IM011") == 0 ||
		    strcmp(state, "IM012") == 0)
			msg = "ODBC 3.0";
		else
			msg = "ISO 9075";

		copyDiagString(msg, DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	}
	}

	/* Currently no Diagnostic Fields are supported.
	   Hence we always return NO_DATA */
	return SQL_NO_DATA;
}

SQLRETURN SQL_API
SQLGetDiagField(SQLSMALLINT HandleType,
		SQLHANDLE Handle,
		SQLSMALLINT RecNumber,
		SQLSMALLINT DiagIdentifier,
		SQLPOINTER DiagInfo,
		SQLSMALLINT BufferLength,
		SQLSMALLINT *StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagField %s " PTRFMT " %d %d %d\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, (int) RecNumber, (int) DiagIdentifier,
		(int) BufferLength);
#endif

	return SQLGetDiagField_(HandleType, Handle, RecNumber, DiagIdentifier, DiagInfo, BufferLength, StringLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetDiagFieldA(SQLSMALLINT HandleType,
		 SQLHANDLE Handle,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT DiagIdentifier,
		 SQLPOINTER DiagInfo,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLength)
{
	return SQLGetDiagField(HandleType, Handle, RecNumber, DiagIdentifier, DiagInfo, BufferLength, StringLength);
}

SQLRETURN SQL_API
SQLGetDiagFieldW(SQLSMALLINT HandleType,
		 SQLHANDLE Handle,
		 SQLSMALLINT RecNumber,
		 SQLSMALLINT DiagIdentifier,
		 SQLPOINTER DiagInfo,
		 SQLSMALLINT BufferLength,
		 SQLSMALLINT *StringLength)
{
	SQLRETURN rc;
	SQLPOINTER ptr = NULL;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagFieldW %s " PTRFMT " %d %d %d\n",
		HandleType == SQL_HANDLE_ENV ? "Env" : HandleType == SQL_HANDLE_DBC ? "Dbc" : HandleType == SQL_HANDLE_STMT ? "Stmt" : "Desc",
		PTRFMTCAST Handle, (int) RecNumber, (int) DiagIdentifier,
		(int) BufferLength);
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
		rc = SQLGetDiagField_(HandleType, Handle, RecNumber, DiagIdentifier, NULL, 0, &n);
		if (!SQL_SUCCEEDED(rc))
			return rc;
		n++;		/* account for NUL byte */
		ptr = (SQLPOINTER) malloc(n);
		break;
	default:
		n = BufferLength;
		ptr = DiagInfo;
		break;
	}

	rc = SQLGetDiagField_(HandleType, Handle, RecNumber, DiagIdentifier, ptr, n, &n);
#ifdef ODBCDEBUG
	if (ptr != DiagInfo)
		ODBCLOG("SQLGetDiagFieldW: %s\n", (char *) ptr);
#endif

	if (ptr != DiagInfo) {
		if (SQL_SUCCEEDED(rc)) {
			char *e = ODBCutf82wchar(ptr, n, DiagInfo, BufferLength / 2, &n);

			if (e)
				rc = SQL_ERROR;
			if (StringLength)
				*StringLength = n * 2;
		}
		free(ptr);
	}

	return rc;
}
#endif /* WITH_WCHAR */
