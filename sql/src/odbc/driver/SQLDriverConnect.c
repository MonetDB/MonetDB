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
 * SQLDriverConnect()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN
SQLDriverConnect(SQLHDBC hDbc, SQLHWND hWnd, SQLCHAR *szConnStrIn,
		 SQLSMALLINT nConnStrIn, SQLCHAR *szConnStrOut,
		 SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pnConnStrOut,
		 SQLUSMALLINT nDriverCompletion)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

	(void) hWnd;		/* Stefan: unused!? */
	(void) szConnStrIn;	/* Stefan: unused!? */
	(void) nConnStrIn;	/* Stefan: unused!? */
	(void) szConnStrOut;	/* Stefan: unused!? */
	(void) cbConnStrOutMax;	/* Stefan: unused!? */
	(void) pnConnStrOut;	/* Stefan: unused!? */

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check connection state, should not be connected */
	if (dbc->Connected == 1) {
		/* 08002 = Connection already in use */
		addDbcError(dbc, "08002", NULL, 0);
		return SQL_ERROR;
	}
	assert(dbc->Connected == 0);

	/* check input arguments */
	switch (nDriverCompletion) {
	case SQL_DRIVER_PROMPT:
	case SQL_DRIVER_COMPLETE:
	case SQL_DRIVER_COMPLETE_REQUIRED:
	case SQL_DRIVER_NOPROMPT:
		break;
	default:
		/* HY092 = Invalid attribute/option identifier */
		addDbcError(dbc, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	/* TODO: finish implementation */

	/* TODO: check szConnStrIn, parse it and retrieve the different settings */
	/* TODO: next call (an internal version of) SQLConnect() */


	/* For now just report "not supported" and return error */
	/* IM001 = Driver does not support this function */
	addDbcError(dbc, "IM001", NULL, 0);
	return SQL_ERROR;
}
