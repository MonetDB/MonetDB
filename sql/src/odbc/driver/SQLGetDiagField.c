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

#define copyDiagString(str, buf, len, lenp)				\
		do {							\
			size_t _l;					\
			if (len < 0)					\
				return SQL_ERROR;			\
			_l = str ? strlen((char *) str) : 0;		\
			if (buf)					\
				strncpy((char *) buf, str ? (char *) str : "", len); \
			if (lenp)					\
				*lenp = _l;				\
			if (buf == NULL || _l >= len)			\
				return SQL_SUCCESS_WITH_INFO;		\
		} while (0)

SQLRETURN
SQLGetDiagField(SQLSMALLINT HandleType,	/* must contain a valid type */
		SQLHANDLE Handle,	/* must contain a valid Handle */
		SQLSMALLINT RecNumber,	/* must be >= 1 */
		SQLSMALLINT DiagIdentifier,	/* a valid identifier */
		SQLPOINTER DiagInfo,	/* may be null */
		SQLSMALLINT BufferLength,	/* must be >= 0 */
		SQLSMALLINT *StringLength   /* may be null */
	)
{
	ODBCError *err;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagField\n");
#endif

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
		* (SQLINTEGER *) DiagInfo = ((ODBCStmt *) Handle)->rowSetSize;
		return SQL_SUCCESS;
	case SQL_DIAG_DYNAMIC_FUNCTION:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		copyDiagString("", DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		* (SQLINTEGER *) DiagInfo = SQL_DIAG_UNKNOWN_STATEMENT;
		return SQL_SUCCESS;
	case SQL_DIAG_NUMBER:
		* (SQLINTEGER *) DiagInfo = getErrorRecCount(err);
		return SQL_SUCCESS;
	case SQL_DIAG_RETURNCODE:
		* (SQLRETURN *) DiagInfo = SQL_SUCCESS;
		return SQL_SUCCESS;
	case SQL_DIAG_ROW_COUNT:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		* (SQLINTEGER *) DiagInfo = ((ODBCStmt *) Handle)->hdl ?
			mapi_get_row_count(((ODBCStmt *) Handle)->hdl) : 0;
		return SQL_SUCCESS;
	}

	/* record fields */
	if (RecNumber <= 0)
		return SQL_ERROR;

	err = getErrorRec(err, RecNumber);
	if (err == NULL)
		return SQL_NO_DATA;

	switch (DiagIdentifier) {
	case SQL_DIAG_CLASS_ORIGIN: {
		char *msg = strncmp(getSqlState(err), "IM", 2) == 0 ?
			"ODBC 3.0" : "ISO 9075";
		copyDiagString(msg, DiagInfo, BufferLength, StringLength);
		return SQL_SUCCESS;
	}
	case SQL_DIAG_COLUMN_NUMBER:
		if (HandleType != SQL_HANDLE_STMT)
			return SQL_ERROR;
		* (SQLINTEGER *) DiagInfo = SQL_COLUMN_NUMBER_UNKNOWN;
		return SQL_SUCCESS;
	}

	/* Currently no Diagnostic Fields are supported.
	   Hence we always return NO_DATA */
	return SQL_NO_DATA;
}
