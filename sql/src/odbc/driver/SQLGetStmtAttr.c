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
 * SQLGetStmtAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQLGetStmtAttr(
	SQLHSTMT	hStmt,
	SQLINTEGER	Attribute,
	SQLPOINTER	Value,
	SQLINTEGER	BufferLength,
	SQLINTEGER *	StringLength )
{
	ODBCStmt * stmt = (ODBCStmt *)hStmt;

	if (! isValidStmt(stmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* TODO: check parameters: Value, BufferLength and StringLength */

	switch (Attribute)
	{
		/* TODO: implement requested behavior */
		case SQL_ATTR_APP_PARAM_DESC:
		case SQL_ATTR_APP_ROW_DESC:
		case SQL_ATTR_FETCH_BOOKMARK_PTR:
		case SQL_ATTR_IMP_PARAM_DESC:
		case SQL_ATTR_IMP_ROW_DESC:

		case SQL_ATTR_ASYNC_ENABLE:
		case SQL_ATTR_CONCURRENCY:
		case SQL_ATTR_CURSOR_SCROLLABLE:
		case SQL_ATTR_CURSOR_SENSITIVITY:
		case SQL_ATTR_CURSOR_TYPE:
		case SQL_ATTR_ENABLE_AUTO_IPD:
		case SQL_ATTR_KEYSET_SIZE:
		case SQL_ATTR_MAX_LENGTH:
		case SQL_ATTR_MAX_ROWS:
		case SQL_ATTR_METADATA_ID:
		case SQL_ATTR_NOSCAN:
		case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
		case SQL_ATTR_PARAM_BIND_TYPE:
		case SQL_ATTR_PARAM_OPERATION_PTR:
		case SQL_ATTR_PARAM_STATUS_PTR:
		case SQL_ATTR_PARAMS_PROCESSED_PTR:
		case SQL_ATTR_PARAMSET_SIZE:
		case SQL_ATTR_QUERY_TIMEOUT:
		case SQL_ATTR_RETRIEVE_DATA:
		case SQL_ATTR_ROW_ARRAY_SIZE:
		case SQL_ATTR_ROW_BIND_OFFSET_PTR:
		case SQL_ATTR_ROW_BIND_TYPE:
		case SQL_ATTR_ROW_NUMBER:
		case SQL_ATTR_ROW_OPERATION_PTR:
		case SQL_ATTR_ROW_STATUS_PTR:
		case SQL_ATTR_ROWS_FETCHED_PTR:
		case SQL_ATTR_SIMULATE_CURSOR:
		case SQL_ATTR_USE_BOOKMARKS:
			/* return error: Optional feature not supported */
			addStmtError(stmt, "HYC00", NULL, 0);
			return SQL_ERROR;
		default:
			/* return error: Invalid option/attribute identifier */
			addStmtError(stmt, "HY092", NULL, 0);
			return SQL_ERROR;
	}

	return SQL_SUCCESS;
}
