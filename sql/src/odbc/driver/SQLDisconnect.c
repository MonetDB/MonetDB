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
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"

SQLRETURN
SQLDisconnect(SQLHDBC hDbc)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDisconnect\n");
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check connection state, should not be connected */
	if (dbc->Connected != 1) {
		/* 08003 = Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}
	assert(dbc->Connected == 1);

#if 0
	if (dbc->FirstStmt != NULL) {
		/* there are still active statements for this connection */
		/* 25000 = Invalid transaction state */
		addDbcError(dbc, "25000", NULL, 0);
		return SQL_ERROR;
	}
#endif

	/* Ready to close the connection and clean up */
	if (dbc->autocommit && dbc->Error == NULL)
		mapi_query(dbc->mid, "COMMIT;");

	/* client waves goodbye */
	mapi_disconnect(dbc->mid);
	mapi_destroy(dbc->mid);

	dbc->mid = NULL;
	dbc->Mdebug = 0;
	dbc->Connected = 0;

	return SQL_SUCCESS;
}
