/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at
 * http://monetdb.cwi.nl/Legal/MonetDBPL-1.0.html
 *
 * Software distributed under the License is distributed on an
 * "AS IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
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
 * 		Martin Kersten  <Martin.Kersten@cwi.nl>
 * 		Peter Boncz  <Peter.Boncz@cwi.nl>
 * 		Niels Nes  <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
 */

/**********************************************************************
 * SQLBrowseConnect()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN SQLBrowseConnect(
	SQLHDBC		hDbc,
	SQLCHAR *	szConnStrIn,
	SQLSMALLINT	cbConnStrIn,
	SQLCHAR *	szConnStrOut,
	SQLSMALLINT	cbConnStrOutMax,
	SQLSMALLINT *	pcbConnStrOut )
{
	ODBCDbc * dbc = (ODBCDbc *) hDbc;

	if (! isValidDbc(dbc))
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
