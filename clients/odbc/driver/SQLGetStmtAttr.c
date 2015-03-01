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
 * Copyright August 2008-2015 MonetDB B.V.
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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLGetStmtAttr_(ODBCStmt *stmt,
		SQLINTEGER Attribute,
		SQLPOINTER ValuePtr,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLengthPtr)
{
	/* TODO: check parameters: ValuePtr, BufferLength and
	 * StringLengthPtr */

	switch (Attribute) {
	case SQL_ATTR_APP_PARAM_DESC:		/* SQLHANDLE */
		*(SQLHANDLE *) ValuePtr = stmt->ApplParamDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_APP_ROW_DESC:		/* SQLHANDLE */
		*(SQLHANDLE *) ValuePtr = stmt->ApplRowDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_ASYNC_ENABLE:		/* SQLULEN */
		/* SQL_ASYNC_ENABLE */
		*(SQLULEN *) ValuePtr = SQL_ASYNC_ENABLE_OFF;
		break;
	case SQL_ATTR_CONCURRENCY:		/* SQLULEN */
		/* SQL_CONCURRENCY */
		*(SQLULEN *) ValuePtr = SQL_CONCUR_READ_ONLY;
		break;
	case SQL_ATTR_CURSOR_SCROLLABLE:	/* SQLULEN */
		*(SQLULEN *) ValuePtr = stmt->cursorScrollable;
		break;
	case SQL_ATTR_CURSOR_SENSITIVITY:	/* SQLULEN */
		*(SQLULEN *) ValuePtr = SQL_INSENSITIVE;
		break;
	case SQL_ATTR_CURSOR_TYPE:		/* SQLULEN */
		/* SQL_CURSOR_TYPE */
		*(SQLULEN *) ValuePtr = stmt->cursorType;
		break;
	case SQL_ATTR_IMP_PARAM_DESC:		/* SQLHANDLE */
		*(SQLHANDLE *) ValuePtr = stmt->ImplParamDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_IMP_ROW_DESC:		/* SQLHANDLE */
		*(SQLHANDLE *) ValuePtr = stmt->ImplRowDescr;
		return SQL_SUCCESS;
	case SQL_ATTR_MAX_LENGTH:		/* SQLULEN */
		/* SQL_MAX_LENGTH */
		*(SQLULEN *) ValuePtr = 0;
		break;
	case SQL_ATTR_MAX_ROWS:			/* SQLULEN */
		/* SQL_MAX_ROWS */
		*(SQLULEN *) ValuePtr = 0;
		break;
	case SQL_ATTR_METADATA_ID:		/* SQLULEN */
		*(SQLULEN *) ValuePtr = stmt->Dbc->sql_attr_metadata_id;
		break;
	case SQL_ATTR_NOSCAN:			/* SQLULEN */
		/* SQL_NOSCAN */
		*(SQLULEN *) ValuePtr = stmt->noScan;
		break;
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:	/* SQLULEN* */
		return SQLGetDescField_(stmt->ApplParamDescr, 0,
					SQL_DESC_BIND_OFFSET_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAM_BIND_TYPE:		/* SQLULEN */
		/* SQL_BIND_TYPE */
		*(SQLULEN *) ValuePtr = stmt->ApplParamDescr->sql_desc_bind_type;
		break;
	case SQL_ATTR_PARAM_OPERATION_PTR:	/* SQLUSMALLINT* */
		return SQLGetDescField_(stmt->ApplParamDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAMSET_SIZE:		/* SQLULEN */
		return SQLGetDescField_(stmt->ApplParamDescr, 0,
					SQL_DESC_ARRAY_SIZE, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAMS_PROCESSED_PTR:	/* SQLULEN* */
		return SQLGetDescField_(stmt->ImplParamDescr, 0,
					SQL_DESC_ROWS_PROCESSED_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAM_STATUS_PTR:		/* SQLUSMALLINT* */
		return SQLGetDescField_(stmt->ImplParamDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_RETRIEVE_DATA:		/* SQLULEN */
		/* SQL_RETRIEVE_DATA */
		*(SQLULEN *) ValuePtr = stmt->retrieveData;
		break;
	case SQL_ATTR_ROW_ARRAY_SIZE:		/* SQLULEN */
	case SQL_ROWSET_SIZE:
		return SQLGetDescField_(stmt->ApplRowDescr, 0,
					SQL_DESC_ARRAY_SIZE, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:	/* SQLULEN* */
		return SQLGetDescField_(stmt->ApplRowDescr, 0,
					SQL_DESC_BIND_OFFSET_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROW_BIND_TYPE:		/* SQLULEN */
		*(SQLULEN *) ValuePtr = stmt->ApplRowDescr->sql_desc_bind_type;
		break;
	case SQL_ATTR_ROW_NUMBER:	     /* SQLULEN */
		if (stmt->State <= EXECUTED1) {
			/* Invalid cursor state */
			addStmtError(stmt, "24000", NULL, 0);
			return SQL_ERROR;
		}
		*(SQLULEN *) ValuePtr = (SQLULEN) stmt->currentRow;
		break;
	case SQL_ATTR_ROW_OPERATION_PTR:	/* SQLUSMALLINT* */
		return SQLGetDescField_(stmt->ApplRowDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROW_STATUS_PTR:		/* SQLUSMALLINT* */
		return SQLGetDescField_(stmt->ImplRowDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROWS_FETCHED_PTR:		/* SQLULEN* */
		return SQLGetDescField_(stmt->ImplRowDescr, 0,
					SQL_DESC_ROWS_PROCESSED_PTR, ValuePtr,
					BufferLength, StringLengthPtr);

		/* TODO: implement requested behavior */
#ifdef SQL_ATTR_ASYNC_STMT_EVENT
	case SQL_ATTR_ASYNC_EVENT:		/* SQLPOINTER */
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
	case SQL_ATTR_ASYNC_PCALLBACK:		/* SQLPOINTER */
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
	case SQL_ATTR_ASYNC_PCONTEXT:		/* SQLPOINTER */
#endif
	case SQL_ATTR_ENABLE_AUTO_IPD:		/* SQLULEN */
	case SQL_ATTR_FETCH_BOOKMARK_PTR:	/* SQLLEN* */
	case SQL_ATTR_KEYSET_SIZE:		/* SQLULEN */
		/* SQL_KEYSET_SIZE */
	case SQL_ATTR_QUERY_TIMEOUT:		/* SQLULEN */
		/* SQL_QUERY_TIMEOUT */
	case SQL_ATTR_SIMULATE_CURSOR:		/* SQLULEN */
	case SQL_ATTR_USE_BOOKMARKS:		/* SQLULEN */
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
SQLGetStmtAttr(SQLHSTMT StatementHandle,
	       SQLINTEGER Attribute,
	       SQLPOINTER ValuePtr,
	       SQLINTEGER BufferLength,
	       SQLINTEGER *StringLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetStmtAttr " PTRFMT " %s " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST StatementHandle, translateStmtAttribute(Attribute),
		PTRFMTCAST ValuePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return SQLGetStmtAttr_((ODBCStmt *) StatementHandle,
			       Attribute,
			       ValuePtr,
			       BufferLength,
			       StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetStmtAttrA(SQLHSTMT StatementHandle,
		SQLINTEGER Attribute,
		SQLPOINTER ValuePtr,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLengthPtr)
{
	return SQLGetStmtAttr(StatementHandle,
			      Attribute,
			      ValuePtr,
			      BufferLength,
			      StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetStmtAttrW(SQLHSTMT StatementHandle,
		SQLINTEGER Attribute,
		SQLPOINTER ValuePtr,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetStmtAttrW " PTRFMT " %s " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST StatementHandle, translateStmtAttribute(Attribute),
		PTRFMTCAST ValuePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	/* there are no string-valued attributes */

	return SQLGetStmtAttr_((ODBCStmt *) StatementHandle,
			       Attribute,
			       ValuePtr,
			       BufferLength,
			       StringLengthPtr);
}
