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
 * SQLSetConnectOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetConnectAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"

SQLRETURN
SQLSetConnectOption(SQLHDBC hDbc, SQLUSMALLINT nOption, SQLULEN vParam)
{
	/* use mapping as described in ODBC 3 SDK Help file */
	switch (nOption) {
		/* connection attributes (ODBC 1 and 2 only) */
	case SQL_ACCESS_MODE:
	case SQL_AUTOCOMMIT:
	case SQL_LOGIN_TIMEOUT:
	case SQL_ODBC_CURSORS:
	case SQL_OPT_TRACE:
	case SQL_PACKET_SIZE:
	case SQL_QUIET_MODE:
	case SQL_TRANSLATE_OPTION:
	case SQL_TXN_ISOLATION:
		/* 32 bit integer argument */
		return SQLSetConnectAttr_(hDbc, nOption, &vParam, 0);

	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		/* null terminated string argument */
		return SQLSetConnectAttr_(hDbc, nOption, &vParam, SQL_NTS);

	default:
	{			/* other options (e.g. ODBC 3) are NOT valid */
		ODBCDbc *dbc = (ODBCDbc *) hDbc;

		if (!isValidDbc(dbc))
			return SQL_INVALID_HANDLE;

		clearDbcErrors(dbc);

		/* set error: Invalid attribute/option */
		addDbcError(dbc, "HY092", NULL, 0);
		return SQL_ERROR;
	}
	}

	return SQL_ERROR;
}
