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
SQLError(SQLHENV hEnv,
	 SQLHDBC hDbc,
	 SQLHSTMT hStmt,
	 SQLCHAR *szSqlState,
	 SQLINTEGER *pfNativeError,
	 SQLCHAR *szErrorMsg,
	 SQLSMALLINT nErrorMsgMax,
	 SQLSMALLINT *pcbErrorMsg)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLError " PTRFMT " " PTRFMT " " PTRFMT "\n", PTRFMTCAST hEnv, PTRFMTCAST hDbc, PTRFMTCAST hStmt);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	return SQLGetDiagRec_(hStmt ? SQL_HANDLE_STMT : (hDbc ? SQL_HANDLE_DBC : SQL_HANDLE_ENV), hStmt ? hStmt : (hDbc ? hDbc : hEnv),
			      (hStmt ? ++((ODBCStmt *) hStmt)->RetrievedErrors : (hDbc ? ++((ODBCDbc *) hDbc)->RetrievedErrors : ++((ODBCEnv *) hEnv)->RetrievedErrors)), szSqlState, pfNativeError, szErrorMsg, nErrorMsgMax, pcbErrorMsg);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLErrorA(SQLHENV hEnv,
	  SQLHDBC hDbc,
	  SQLHSTMT hStmt,
	  SQLCHAR *szSqlState,
	  SQLINTEGER *pfNativeError,
	  SQLCHAR *szErrorMsg,
	  SQLSMALLINT nErrorMsgMax,
	  SQLSMALLINT *pcbErrorMsg)
{
	return SQLError(hEnv, hDbc, hStmt, szSqlState, pfNativeError, szErrorMsg, nErrorMsgMax, pcbErrorMsg);
}

SQLRETURN SQL_API
SQLErrorW(SQLHENV hEnv,
	  SQLHDBC hDbc,
	  SQLHSTMT hStmt,
	  SQLWCHAR * szSqlState,
	  SQLINTEGER *pfNativeError,
	  SQLWCHAR * szErrorMsg,
	  SQLSMALLINT nErrorMsgMax,
	  SQLSMALLINT *pcbErrorMsg)
{
	SQLCHAR state[6];
	SQLRETURN rc;
	SQLSMALLINT n;
	SQLCHAR *errmsg;

#ifdef ODBCDEBUG
	ODBCLOG("SQLErrorW " PTRFMT " " PTRFMT " " PTRFMT "\n", PTRFMTCAST hEnv, PTRFMTCAST hDbc, PTRFMTCAST hStmt);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	/* first try to figure out how big the buffer needs to be */
	rc = SQLGetDiagRec_(hStmt ? SQL_HANDLE_STMT : (hDbc ? SQL_HANDLE_DBC : SQL_HANDLE_ENV),
			    hStmt ? hStmt : (hDbc ? hDbc : hEnv),
			    (hStmt ? ++((ODBCStmt *) hStmt)->RetrievedErrors : (hDbc ? ++((ODBCDbc *) hDbc)->RetrievedErrors : ++((ODBCEnv *) hEnv)->RetrievedErrors)),
			    state, pfNativeError, NULL, 0, &n);

	/* and now for real */
	errmsg = (SQLCHAR *) malloc(n + 1);
	rc = SQLGetDiagRec_(hStmt ? SQL_HANDLE_STMT : (hDbc ? SQL_HANDLE_DBC : SQL_HANDLE_ENV),
			    hStmt ? hStmt : (hDbc ? hDbc : hEnv),
			    (hStmt ? ((ODBCStmt *) hStmt)->RetrievedErrors : (hDbc ? ((ODBCDbc *) hDbc)->RetrievedErrors : ((ODBCEnv *) hEnv)->RetrievedErrors)),
			    state, pfNativeError, errmsg, n + 1, &n);

	if (SQL_SUCCEEDED(rc)) {
		char *e = ODBCutf82wchar(state, 5, szSqlState, 6, NULL);

		if (e)
			rc = SQL_ERROR;
	}

	if (SQL_SUCCEEDED(rc)) {
		char *e = ODBCutf82wchar(errmsg, n, szErrorMsg, nErrorMsgMax, &n);

		if (e)
			rc = SQL_ERROR;
		if (pcbErrorMsg)
			*pcbErrorMsg = n;
	}
	free(errmsg);

	return rc;
}
#endif /* WITH_WCHAR */
