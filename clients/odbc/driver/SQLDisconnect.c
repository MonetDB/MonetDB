/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 * SQLDisconnect
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLDisconnect(SQLHDBC ConnectionHandle)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDisconnect " PTRFMT "\n", PTRFMTCAST ConnectionHandle);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check connection state, should be connected */
	if (!dbc->Connected) {
		/* Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}

	while (dbc->FirstStmt != NULL)
		if (ODBCFreeStmt_(dbc->FirstStmt) == SQL_ERROR)
			return SQL_ERROR;

	/* client waves goodbye */
	mapi_disconnect(dbc->mid);
	mapi_destroy(dbc->mid);

	dbc->mid = NULL;
	dbc->cachelimit = 0;
	dbc->Mdebug = 0;
	dbc->Connected = 0;

	return SQL_SUCCESS;
}
