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
 * SQLGetFunctions()
 * CLI Compliance: ISO 92
 *
 * Author: Sjoerd Mullender
 * Date  : 4 sep 2003
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLGetFunctions(
	SQLHDBC hDbc,
	SQLUSMALLINT FunctionId,
	SQLUSMALLINT *Supported)
{
	ODBCDbc * dbc = (ODBCDbc *) hDbc;

	(void) FunctionId;	/* Stefan: unused!? */
	(void) Supported;	/* Stefan: unused!? */

	if (! isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* TODO: implement the requested behavior */

	/* for now always return error: Driver does not support this function */
	addDbcError(dbc, "IM001", NULL, 0);
	return SQL_ERROR;
}
