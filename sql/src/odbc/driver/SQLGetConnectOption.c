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
 * SQLGetConnectOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLGetConnectAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN
SQLGetConnectOption(SQLHDBC hDbc, SQLUSMALLINT nOption, SQLPOINTER pvParam)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetConnectOption\n");
#endif

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
		return SQLGetConnectAttr_(hDbc, nOption, pvParam, 0, NULL);

	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		/* null terminated string argument */
		return SQLGetConnectAttr_(hDbc, nOption, pvParam,
					  SQL_MAX_OPTION_STRING_LENGTH, NULL);

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
