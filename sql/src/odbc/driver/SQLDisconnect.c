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
	{
		int chars_printed;
		stream *rs = dbc->Mrs;
		stream *ws = dbc->Mws;
		int debug = dbc->Mdebug;
		char buf[BUFSIZ];

		if (dbc->autocommit && dbc->Error == NULL) {
			chars_printed = snprintf(buf, BUFSIZ, "COMMIT;\n");
			ws->write(ws, buf, chars_printed, 1);
			ws->flush(ws);

			simple_receive(rs, ws, debug);
		}

		/* client waves goodbye */
		buf[0] = EOT;
		ws->write(ws, buf, 1, 1);
		ws->flush(ws);

		rs->close(rs);
		rs->destroy(rs);

		ws->close(ws);
		ws->destroy(ws);
	}
	dbc->socket = 0;
	dbc->Mrs = NULL;
	dbc->Mws = NULL;
	dbc->Mdebug = 0;
	dbc->Connected = 0;

	return SQL_SUCCESS;
}
