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
 * SQLGetDiagField()
 * ODBC 3.0 API function
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"
#include "ODBCDbc.h"
#include "ODBCStmt.h"
#include "ODBCError.h"

SQLRETURN SQLGetDiagField(
	SQLSMALLINT	HandleType,	/* must contain a valid type */
	SQLHANDLE	Handle,		/* must contain a valid Handle */
	SQLSMALLINT	RecNumber,	/* must be >= 1 */
	SQLSMALLINT	DiagIdentifier,	/* a valid identifier */
	SQLPOINTER	DiagInfo,	/* may be null */
	SQLSMALLINT	BufferLength,	/* must be >= 0 */
	SQLSMALLINT *	StringLength )	/* may be null */
{
	ODBCEnv * env = NULL;
	ODBCDbc * dbc = NULL;
	ODBCStmt * stmt = NULL;
	SQLRETURN retCode = SQL_SUCCESS;

	/* input & output parameters validity checks */
	if ( ! Handle )
	{
		return SQL_INVALID_HANDLE;
	}

	switch (HandleType) {
		case SQL_HANDLE_ENV:
			env = Handle;	/* cast the Handle to the proper struct type */
			/* Check if this struct is still valid/alive */
			if ( !(isValidEnv(env)) ) {
				return SQL_INVALID_HANDLE;
			}
			break;
		case SQL_HANDLE_DBC:
			dbc = Handle;	/* cast the Handle to the proper struct type */
			/* Check if this struct is still valid/alive */
			if ( !(isValidDbc(dbc)) ) {
				return SQL_INVALID_HANDLE;
			}
			break;
		case SQL_HANDLE_STMT:
			stmt = Handle;	/* cast the Handle to the proper struct type */
			/* Check if this struct is still valid/alive */
			if ( !(isValidStmt(stmt)) ) {
				return SQL_INVALID_HANDLE;
			}
			break;
		case SQL_HANDLE_DESC:
			/* not yet supported */
			return SQL_NO_DATA;
		default:
			return SQL_INVALID_HANDLE;
	}

	/* Currently no Diagnostic Fields are supported.
	   Hence we always return NO_DATA */
	return SQL_NO_DATA;
}
