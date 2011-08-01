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
 * SQLParamOptions()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetStmtAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLParamOptions(SQLHSTMT hStmt,
		SQLULEN nRow,
		SQLULEN *pnRow)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	RETCODE rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLParamOptions " PTRFMT " " ULENFMT "\n",
		PTRFMTCAST hStmt, ULENCAST nRow);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	rc = SQLSetStmtAttr_(stmt, SQL_ATTR_PARAMSET_SIZE, &nRow, 0);

	if (SQL_SUCCEEDED(rc)) {
		rc = SQLSetStmtAttr_(stmt, SQL_ATTR_PARAMS_PROCESSED_PTR, pnRow, 0);
	}
	return rc;
}
