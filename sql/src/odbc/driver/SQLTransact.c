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

/********************************************************************
 * SQLTransact()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLEndTran())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN SQLTransact(
	SQLHENV	hEnv,
	SQLHDBC	hDbc,
	UWORD	fType )
{
	/* use mapping as described in ODBC 3 SDK Help */
	if (hDbc != SQL_NULL_HDBC)
	{
		return SQLEndTran(SQL_HANDLE_DBC, hDbc, fType);
	} else {
		return SQLEndTran(SQL_HANDLE_ENV, hEnv, fType);
	}
}
