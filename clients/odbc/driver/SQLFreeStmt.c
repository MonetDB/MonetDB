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
 * SQLFreeStmt()
 * CLI Compliance: ISO 92
 *
 * Note: the option SQL_DROP is deprecated in ODBC 3.0 and replaced by
 * SQLFreeHandle(). It is provided here for old (pre ODBC 3.0) applications.
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLFreeStmt_(ODBCStmt *stmt,
	     SQLUSMALLINT option)
{
	switch (option) {
	case SQL_CLOSE:
		/* Note: this option is also called from SQLCancel() and
		   SQLCloseCursor(), so be careful when changing the code */
		/* close cursor, discard result set, set to prepared */
		setODBCDescRecCount(stmt->ImplRowDescr, 0);
		stmt->currentRow = 0;
		stmt->startRow = 0;
		stmt->rowSetSize = 0;

		if (stmt->State == EXECUTED0)
			stmt->State = stmt->queryid >= 0 ? PREPARED0 : INITED;
		else if (stmt->State >= EXECUTED1)
			stmt->State = stmt->queryid >= 0 ? PREPARED1 : INITED;

		/* Important: do not destroy the bind parameters and columns! */
		return SQL_SUCCESS;
	case SQL_DROP:
		return ODBCFreeStmt_(stmt);
	case SQL_UNBIND:
		setODBCDescRecCount(stmt->ApplRowDescr, 0);
		return SQL_SUCCESS;
	case SQL_RESET_PARAMS:
		setODBCDescRecCount(stmt->ApplParamDescr, 0);
		setODBCDescRecCount(stmt->ImplParamDescr, 0);
		mapi_clear_params(stmt->hdl);
		return SQL_SUCCESS;
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	/* not reached */
}

SQLRETURN SQL_API
SQLFreeStmt(SQLHSTMT handle,
	    SQLUSMALLINT option)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLFreeStmt " PTRFMT " %u\n",
		PTRFMTCAST handle, (unsigned int) option);
#endif

	if (!isValidStmt((ODBCStmt *) handle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) handle);

	return SQLFreeStmt_((ODBCStmt *) handle, option);
}
