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

/********************************************************************
 * SQLSetScrollOptions()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetStmtAttr()
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLSetScrollOptions(SQLHSTMT hStmt,
		    SQLUSMALLINT fConcurrency,
		    SQLLEN crowKeyset,
		    SQLUSMALLINT crowRowset)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetScrollOptions " PTRFMT " %d " LENFMT " %d\n", PTRFMTCAST hStmt, (int) fConcurrency, LENCAST crowKeyset, (int) crowRowset);
#endif

	(void) fConcurrency;	/* Stefan: unused!? */
	(void) crowKeyset;	/* Stefan: unused!? */
	(void) crowRowset;	/* Stefan: unused!? */

	if (!isValidStmt((ODBCStmt *) hStmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	/* TODO: implement the mapping to multiple SQLSetStmtAttr() calls */
	/* See ODBC 3.5 SDK Help file for details */

	/* for now return error */
	/* Driver does not support this function */
	addStmtError((ODBCStmt *) hStmt, "IM001", NULL, 0);
	return SQL_ERROR;
}
