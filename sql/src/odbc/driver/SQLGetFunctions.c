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
 * SQLGetFunctions()
 * CLI Compliance: ISO 92
 *
 * Author: Sjoerd Mullender
 * Date  : 4 sep 2003
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLGetFunctions(SQLHDBC hDbc, SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

	(void) FunctionId;	/* Stefan: unused!? */
	(void) Supported;	/* Stefan: unused!? */

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* TODO: implement the requested behavior */

	/* for now always return error: Driver does not support this function */
	addDbcError(dbc, "IM001", NULL, 0);
	return SQL_ERROR;
}
