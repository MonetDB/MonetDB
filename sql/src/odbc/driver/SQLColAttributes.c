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
 * SQLColAttributes()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLColAttribute())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN SQLColAttributes(
	SQLHSTMT	hStmt,
	SQLUSMALLINT	nCol,
	SQLUSMALLINT	nDescType,
	SQLPOINTER	pszDesc,
	SQLSMALLINT	nDescMax,
	SQLSMALLINT *	pcbDesc,
	SQLINTEGER *	pfDesc )
{
	/* use mapping as described in ODBC 3 SDK Help file */
	return SQLColAttribute(hStmt, nCol, nDescType, pszDesc, nDescMax, pcbDesc, pfDesc);

	/* TODO: implement specials semantics for nDescTypes: SQL_COLUMN_TYPE,
	   SQL_COLUMN_NAME, SQL_COLUMN_NULLABLE and SQL_COLUMN_COUNT.
	   See ODBC 3 SDK Help file, SQLColAttributes Mapping.
	 */
}
