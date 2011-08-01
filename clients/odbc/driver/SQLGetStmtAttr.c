/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

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
 * SQLGetStmtAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLGetStmtAttr_(ODBCStmt *stmt,
		SQLINTEGER Attribute,
		SQLPOINTER Value,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLength)
{
	/* TODO: check parameters: Value, BufferLength and StringLength */

	switch (Attribute) {
	case SQL_ATTR_APP_PARAM_DESC:
		*(SQLHANDLE *) Value = stmt->ApplParamDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_APP_ROW_DESC:
		*(SQLHANDLE *) Value = stmt->ApplRowDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_ASYNC_ENABLE:
		*(SQLUINTEGER *) Value = SQL_ASYNC_ENABLE_OFF;
		break;
	case SQL_ATTR_CONCURRENCY:
		*(SQLUINTEGER *) Value = SQL_CONCUR_READ_ONLY;
		break;
	case SQL_ATTR_CURSOR_SCROLLABLE:
		*(SQLUINTEGER *) Value = stmt->cursorScrollable;
		break;
	case SQL_ATTR_CURSOR_SENSITIVITY:
		*(SQLUINTEGER *) Value = SQL_INSENSITIVE;
		break;
	case SQL_ATTR_CURSOR_TYPE:
		*(SQLUINTEGER *) Value = stmt->cursorType;
		break;
	case SQL_ATTR_IMP_PARAM_DESC:
		*(SQLHANDLE *) Value = stmt->ImplParamDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_IMP_ROW_DESC:
		*(SQLHANDLE *) Value = stmt->ImplRowDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_MAX_LENGTH:
		*(SQLULEN *) Value = 0;
		break;
	case SQL_ATTR_MAX_ROWS:
		*(SQLULEN *) Value = 0;
		break;
	case SQL_ATTR_NOSCAN:
		*(SQLUINTEGER *) Value = stmt->noScan;
		break;
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
		return SQLGetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_BIND_OFFSET_PTR, Value, BufferLength, StringLength);
	case SQL_ATTR_PARAM_BIND_TYPE:
		return SQLGetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_BIND_TYPE, Value, BufferLength, StringLength);
	case SQL_ATTR_PARAM_OPERATION_PTR:
		return SQLGetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, BufferLength, StringLength);
	case SQL_ATTR_PARAM_STATUS_PTR:
		return SQLGetDescField_(stmt->ImplParamDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, BufferLength, StringLength);
	case SQL_ATTR_PARAMS_PROCESSED_PTR:
		return SQLGetDescField_(stmt->ImplParamDescr, 0, SQL_DESC_ROWS_PROCESSED_PTR, Value, BufferLength, StringLength);
	case SQL_ATTR_PARAMSET_SIZE:
		return SQLGetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_ARRAY_SIZE, Value, BufferLength, StringLength);
	case SQL_ATTR_RETRIEVE_DATA:
		*(SQLUINTEGER *) Value = stmt->retrieveData;
		break;
	case SQL_ATTR_ROW_ARRAY_SIZE:
	case SQL_ROWSET_SIZE:
		return SQLGetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_ARRAY_SIZE, Value, BufferLength, StringLength);
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:
		return SQLGetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_BIND_OFFSET_PTR, Value, BufferLength, StringLength);
	case SQL_ATTR_ROW_BIND_TYPE:
		return SQLGetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_BIND_TYPE, Value, BufferLength, StringLength);
	case SQL_ATTR_ROW_NUMBER:
		if (stmt->State <= EXECUTED1) {
			/* Invalid cursor state */
			addStmtError(stmt, "24000", NULL, 0);
			return SQL_ERROR;
		}
		*(SQLULEN *) Value = (SQLULEN) stmt->currentRow;
		break;
	case SQL_ATTR_ROW_OPERATION_PTR:
		return SQLGetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, BufferLength, StringLength);
	case SQL_ATTR_ROW_STATUS_PTR:
		return SQLGetDescField_(stmt->ImplRowDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, BufferLength, StringLength);
	case SQL_ATTR_ROWS_FETCHED_PTR:
		return SQLGetDescField_(stmt->ImplRowDescr, 0, SQL_DESC_ROWS_PROCESSED_PTR, Value, BufferLength, StringLength);

		/* TODO: implement requested behavior */
	case SQL_ATTR_ENABLE_AUTO_IPD:
	case SQL_ATTR_FETCH_BOOKMARK_PTR:
	case SQL_ATTR_KEYSET_SIZE:
	case SQL_ATTR_METADATA_ID:
	case SQL_ATTR_QUERY_TIMEOUT:
	case SQL_ATTR_SIMULATE_CURSOR:
	case SQL_ATTR_USE_BOOKMARKS:
		/* Optional feature not implemented */
		addStmtError(stmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLGetStmtAttr(SQLHSTMT hStmt,
	       SQLINTEGER Attribute,
	       SQLPOINTER Value,
	       SQLINTEGER BufferLength,
	       SQLINTEGER *StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetStmtAttr " PTRFMT " %d\n",
		PTRFMTCAST hStmt, (int) Attribute);
#endif

	if (!isValidStmt((ODBCStmt *) hStmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	return SQLGetStmtAttr_((ODBCStmt *) hStmt, Attribute, Value, BufferLength, StringLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetStmtAttrA(SQLHSTMT hStmt,
		SQLINTEGER Attribute,
		SQLPOINTER Value,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLength)
{
	return SQLGetStmtAttr(hStmt, Attribute, Value, BufferLength, StringLength);
}

SQLRETURN SQL_API
SQLGetStmtAttrW(SQLHSTMT hStmt,
		SQLINTEGER Attribute,
		SQLPOINTER Value,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetStmtAttrW " PTRFMT " %d\n",
		PTRFMTCAST hStmt, (int) Attribute);
#endif

	if (!isValidStmt((ODBCStmt *) hStmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	/* there are no string-valued attributes */

	return SQLGetStmtAttr_((ODBCStmt *) hStmt, Attribute, Value, BufferLength, StringLength);
}
#endif /* WITH_WCHAR */
