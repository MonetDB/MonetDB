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
 * SQLBrowseConnect()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN
SQLBrowseConnect(SQLHDBC hDbc, SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn,
		 SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax,
		 SQLSMALLINT *pcbConnStrOut)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

	(void) szConnStrIn;	/* Stefan: unused!? */
	(void) cbConnStrIn;	/* Stefan: unused!? */
	(void) szConnStrOut;	/* Stefan: unused!? */
	(void) cbConnStrOutMax;	/* Stefan: unused!? */
	(void) pcbConnStrOut;	/* Stefan: unused!? */

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


	/* TODO: finish implementation */
	/* TODO: check szConnStrIn, parse it and retrieve the different settings */
	/* TODO: next call (an internal version of) SQLConnect() */


	/* For now just report "not supported" and return error */
	/* IM001 = Driver does not support this function */
	addDbcError(dbc, "IM001", NULL, 0);
	return SQL_ERROR;
}
