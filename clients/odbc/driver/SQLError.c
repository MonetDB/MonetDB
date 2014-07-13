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
 * Copyright August 2008-2014 MonetDB B.V.
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
 * SQLError()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLGetDiagRec())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN SQL_API
SQLError(SQLHENV EnvironmentHandle,
	 SQLHDBC ConnectionHandle,
	 SQLHSTMT StatementHandle,
	 SQLCHAR *SQLState,
	 SQLINTEGER *NativeErrorPtr,
	 SQLCHAR *MessageText,
	 SQLSMALLINT BufferLength,
	 SQLSMALLINT *TextLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLError " PTRFMT " " PTRFMT " " PTRFMT "\n", PTRFMTCAST EnvironmentHandle, PTRFMTCAST ConnectionHandle, PTRFMTCAST StatementHandle);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	if (StatementHandle)
		return SQLGetDiagRec_(SQL_HANDLE_STMT,
				      StatementHandle,
				      ++((ODBCStmt *) StatementHandle)->RetrievedErrors,
				      SQLState,
				      NativeErrorPtr,
				      MessageText,
				      BufferLength,
				      TextLengthPtr);
	else if (ConnectionHandle)
		return SQLGetDiagRec_(SQL_HANDLE_DBC,
				      ConnectionHandle,
				      ++((ODBCDbc *) ConnectionHandle)->RetrievedErrors,
				      SQLState,
				      NativeErrorPtr,
				      MessageText,
				      BufferLength,
				      TextLengthPtr);
	else if (EnvironmentHandle)
		return SQLGetDiagRec_(SQL_HANDLE_ENV,
				      EnvironmentHandle,
				      ++((ODBCEnv *) EnvironmentHandle)->RetrievedErrors,
				      SQLState,
				      NativeErrorPtr,
				      MessageText,
				      BufferLength,
				      TextLengthPtr);
	else
		return SQL_ERROR;
}

SQLRETURN SQL_API
SQLErrorA(SQLHENV EnvironmentHandle,
	  SQLHDBC ConnectionHandle,
	  SQLHSTMT StatementHandle,
	  SQLCHAR *SQLState,
	  SQLINTEGER *NativeErrorPtr,
	  SQLCHAR *MessageText,
	  SQLSMALLINT BufferLength,
	  SQLSMALLINT *TextLengthPtr)
{
	return SQLError(EnvironmentHandle,
			ConnectionHandle,
			StatementHandle,
			SQLState,
			NativeErrorPtr,
			MessageText,
			BufferLength,
			TextLengthPtr);
}

SQLRETURN SQL_API
SQLErrorW(SQLHENV EnvironmentHandle,
	  SQLHDBC ConnectionHandle,
	  SQLHSTMT StatementHandle,
	  SQLWCHAR *SQLState,
	  SQLINTEGER *NativeErrorPtr,
	  SQLWCHAR *MessageText,
	  SQLSMALLINT BufferLength,
	  SQLSMALLINT *TextLengthPtr)
{
	SQLCHAR state[6];
	SQLRETURN rc;
	SQLSMALLINT n;
	SQLCHAR errmsg[512];

#ifdef ODBCDEBUG
	ODBCLOG("SQLErrorW " PTRFMT " " PTRFMT " " PTRFMT "\n", PTRFMTCAST EnvironmentHandle, PTRFMTCAST ConnectionHandle, PTRFMTCAST StatementHandle);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	if (StatementHandle)
		rc = SQLGetDiagRec_(SQL_HANDLE_STMT,
				    StatementHandle,
				    ((ODBCStmt *) StatementHandle)->RetrievedErrors,
				    state, NativeErrorPtr,
				    errmsg, (SQLSMALLINT) sizeof(errmsg), &n);
	else if (ConnectionHandle)
		rc = SQLGetDiagRec_(SQL_HANDLE_DBC,
				    ConnectionHandle,
				    ((ODBCDbc *) ConnectionHandle)->RetrievedErrors,
				    state, NativeErrorPtr,
				    errmsg, (SQLSMALLINT) sizeof(errmsg), &n);
	else if (EnvironmentHandle)
		rc = SQLGetDiagRec_(SQL_HANDLE_ENV,
				    EnvironmentHandle,
				    ((ODBCEnv *) EnvironmentHandle)->RetrievedErrors,
				    state, NativeErrorPtr,
				    errmsg, (SQLSMALLINT) sizeof(errmsg), &n);
	else
		return SQL_ERROR;

	if (SQL_SUCCEEDED(rc)) {
		char *e = ODBCutf82wchar(state, 5, SQLState, 6, NULL);

		if (e)
			rc = SQL_ERROR;
	}

	if (SQL_SUCCEEDED(rc)) {
		char *e = ODBCutf82wchar(errmsg, n,
					 MessageText, BufferLength, &n);

		if (e)
			rc = SQL_ERROR;
		if (TextLengthPtr)
			*TextLengthPtr = n;
	}

	return rc;
}
