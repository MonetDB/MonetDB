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
 * SQLAllocStmt()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLAllocHandle())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN SQL_API
SQLAllocStmt(SQLHDBC hDbc, SQLHSTMT *phStmt)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLAllocStmt " PTRFMT "\n", PTRFMTCAST hDbc);
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	return SQLAllocHandle_(SQL_HANDLE_STMT, (SQLHANDLE) hDbc, (SQLHANDLE *) phStmt);
}
