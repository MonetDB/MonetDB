/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
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

SQLRETURN SQLDisconnect(SQLHDBC hDbc)
{
	ODBCDbc * dbc = (ODBCDbc *) hDbc;

	if (! isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check connection state, should not be connected */
	if (dbc->Connected != 1) {
		/* 08003 = Connection does not exist */
		addDbcError(dbc, "08003", NULL, 0);
		return SQL_ERROR;
	}
	assert(dbc->Connected == 1);

	if (dbc->FirstStmt != NULL)
	{
		/* there are still active statements for this connection */
		/* 25000 = Invalid transaction state */
		addDbcError(dbc, "25000", NULL, 0);
		return SQL_ERROR;
	}


	/* Ready to close the connection and clean up */
	{
		int	chars_printed;
		stream *	rs = dbc->Mrs;
		context *	lc = &dbc->Mlc;
		stream *	ws = lc->out;
		int	flag = 0;
		char	buf[BUFSIZ];

		chars_printed = snprintf(buf, BUFSIZ,
			"s0 := mvc_commit(myc, 0, \"\");\n");
		chars_printed += snprintf(buf + chars_printed, BUFSIZ - chars_printed,
			"result(Output, mvc_type(myc), mvc_status(myc));\n");
		ws->write(ws, buf, chars_printed, 1);
		ws->flush(ws);

		if (stream_readInt(rs, &flag)) {
			int type = 0;
			int status = 0;

			/* TODO check if flag == COMM_DONE */
			stream_readInt(rs, &type);
			stream_readInt(rs, &status);
			if (status < 0){
				/* TODO: handle this case */
			}
		}

		/* client waves goodbye */
		buf[0] = EOT;
		ws->write(ws, buf, 1, 1);
		ws->flush(ws);

		rs->close(rs);
		rs->destroy(rs);
	}
	sql_exit_context(&dbc->Mlc);
	dbc->socket = 0;
	dbc->Mrs = NULL;
	dbc->Connected = 0;

	return SQL_SUCCESS;
}
