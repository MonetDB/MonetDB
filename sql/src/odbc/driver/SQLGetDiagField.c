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
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDiagField\n");
#endif

	(void) RecNumber;	/* Stefan: unused!? */
	(void) DiagIdentifier;	/* Stefan: unused!? */
	(void) DiagInfo;	/* Stefan: unused!? */
	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLength;	/* Stefan: unused!? */

	/* input & output parameters validity checks */
	if (!Handle)
		return SQL_INVALID_HANDLE;

	switch (HandleType) {
	case SQL_HANDLE_ENV:
		/* Check if this struct is still valid/alive */
		if (!isValidEnv((ODBCEnv *) Handle))
			return SQL_INVALID_HANDLE;
		break;
	case SQL_HANDLE_DBC:
		/* Check if this struct is still valid/alive */
		if (!isValidDbc((ODBCDbc *) Handle))
			return SQL_INVALID_HANDLE;
		break;
	case SQL_HANDLE_STMT:
		/* Check if this struct is still valid/alive */
		if (!isValidStmt((ODBCStmt *) Handle))
			return SQL_INVALID_HANDLE;
		break;
	case SQL_HANDLE_DESC:
		/* not yet supported */
		return SQL_NO_DATA;
	default:
		return SQL_INVALID_HANDLE;
	}

	/* Currently no Diagnostic Fields are supported.
	   Hence we always return NO_DATA */
	return SQL_NO_DATA;
}
