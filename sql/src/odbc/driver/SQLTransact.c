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
 * SQLTransact()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLEndTran())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN SQL_API
SQLTransact(SQLHENV hEnv, SQLHDBC hDbc, UWORD fType)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLTransact " PTRFMT " " PTRFMT " %d\n", PTRFMTCAST hEnv, PTRFMTCAST hDbc, fType);
#endif

	/* use mapping as described in ODBC 3 SDK Help */
	if (hDbc != SQL_NULL_HDBC)
		return SQLEndTran_(SQL_HANDLE_DBC, hDbc, fType);
	else
		return SQLEndTran_(SQL_HANDLE_ENV, hEnv, fType);
}
