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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"

SQLRETURN
SQLError(SQLHENV hEnv, SQLHDBC hDbc, SQLHSTMT hStmt, SQLCHAR *szSqlState,
	 SQLINTEGER *pfNativeError, SQLCHAR *szErrorMsg,
	 SQLSMALLINT nErrorMsgMax, SQLSMALLINT *pcbErrorMsg)
{
	/* use mapping as described in ODBC 3 SDK Help file */
	return SQLGetDiagRec_(hStmt ? SQL_HANDLE_STMT :
			      (hDbc ? SQL_HANDLE_DBC : SQL_HANDLE_ENV),
			      hStmt ? hStmt : (hDbc ? hDbc : hEnv),
			      (hStmt ? ++((ODBCStmt *)hStmt)->RetrievedErrors :
			      (hDbc ? ++((ODBCDbc *)hDbc)->RetrievedErrors :
			       ++((ODBCEnv *)hEnv)->RetrievedErrors)),
			      szSqlState, pfNativeError, szErrorMsg,
			      nErrorMsgMax, pcbErrorMsg);
}
