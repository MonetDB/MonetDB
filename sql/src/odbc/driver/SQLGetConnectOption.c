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
 * SQLGetConnectOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLGetConnectAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN SQLGetConnectOption(
	SQLHDBC		hDbc,
	SQLUSMALLINT	nOption,
	SQLPOINTER	pvParam )
{
	/* use mapping as described in ODBC 3 SDK Help file */
	switch (nOption)
	{
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
			return SQLGetConnectAttr(hDbc, nOption, pvParam, 0, NULL);

		case SQL_CURRENT_QUALIFIER:
		case SQL_OPT_TRACEFILE:
		case SQL_TRANSLATE_DLL:
			/* null terminated string argument */
			return SQLGetConnectAttr(hDbc, nOption, pvParam, SQL_MAX_OPTION_STRING_LENGTH, NULL);

		default:
		{	/* other options (e.g. ODBC 3) are NOT valid */
			ODBCDbc * dbc = (ODBCDbc *) hDbc;

			if (! isValidDbc(dbc))
				return SQL_INVALID_HANDLE;

			clearDbcErrors(dbc);

			/* set error: Invalid attribute/option */
			addDbcError(dbc, "HY092", NULL, 0);
			return SQL_ERROR;
		}
	}

	return SQL_ERROR;
}
