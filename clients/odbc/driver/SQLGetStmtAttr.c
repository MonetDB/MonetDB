/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "ODBCUtil.h"


SQLRETURN
MNDBGetStmtAttr(ODBCStmt *stmt,
		SQLINTEGER Attribute,
		SQLPOINTER ValuePtr,
		SQLINTEGER BufferLength,
		SQLINTEGER *StringLengthPtr)
{
	/* TODO: check parameters: ValuePtr, BufferLength and
	 * StringLengthPtr */

	switch (Attribute) {
#ifndef STATIC_CODE_ANALYSIS
	/* Coverity doesn't like the debug print in WriteData, so we
	 * hide this whole thing */
	case SQL_ATTR_APP_PARAM_DESC:		/* SQLHANDLE */
		WriteData(ValuePtr, stmt->ApplParamDescr, SQLHANDLE);
		return SQL_SUCCESS;
	case SQL_ATTR_APP_ROW_DESC:		/* SQLHANDLE */
		WriteData(ValuePtr, stmt->ApplRowDescr, SQLHANDLE);
		return SQL_SUCCESS;
#endif
	case SQL_ATTR_ASYNC_ENABLE:		/* SQLULEN */
		/* SQL_ASYNC_ENABLE */
		WriteData(ValuePtr, SQL_ASYNC_ENABLE_OFF, SQLULEN);
		break;
	case SQL_ATTR_ENABLE_AUTO_IPD:		/* SQLULEN */
		WriteData(ValuePtr, SQL_TRUE, SQLULEN);
		break;
	case SQL_ATTR_CONCURRENCY:		/* SQLULEN */
		/* SQL_CONCURRENCY */
		WriteData(ValuePtr, SQL_CONCUR_READ_ONLY, SQLULEN);
		break;
	case SQL_ATTR_CURSOR_SCROLLABLE:	/* SQLULEN */
		WriteData(ValuePtr, stmt->cursorScrollable, SQLULEN);
		break;
	case SQL_ATTR_CURSOR_SENSITIVITY:	/* SQLULEN */
		WriteData(ValuePtr, SQL_INSENSITIVE, SQLULEN);
		break;
	case SQL_ATTR_CURSOR_TYPE:		/* SQLULEN */
		/* SQL_CURSOR_TYPE */
		WriteData(ValuePtr, stmt->cursorType, SQLULEN);
		break;
#ifndef STATIC_CODE_ANALYSIS
	/* Coverity doesn't like the debug print in WriteData, so we
	 * hide this whole thing */
	case SQL_ATTR_IMP_PARAM_DESC:		/* SQLHANDLE */
		WriteData(ValuePtr, stmt->ImplParamDescr, SQLHANDLE);
		return SQL_SUCCESS;
	case SQL_ATTR_IMP_ROW_DESC:		/* SQLHANDLE */
		WriteData(ValuePtr, stmt->ImplRowDescr, SQLHANDLE);
		return SQL_SUCCESS;
#endif
	case SQL_ATTR_MAX_LENGTH:		/* SQLULEN */
		/* SQL_MAX_LENGTH */
		WriteData(ValuePtr, 0, SQLULEN);
		break;
	case SQL_ATTR_MAX_ROWS:			/* SQLULEN */
		/* SQL_MAX_ROWS */
		WriteData(ValuePtr, 0, SQLULEN);
		break;
	case SQL_ATTR_METADATA_ID:		/* SQLULEN */
		WriteData(ValuePtr, stmt->Dbc->sql_attr_metadata_id, SQLULEN);
		break;
	case SQL_ATTR_NOSCAN:			/* SQLULEN */
		/* SQL_NOSCAN */
		WriteData(ValuePtr, stmt->noScan, SQLULEN);
		break;
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:	/* SQLULEN* */
		return MNDBGetDescField(stmt->ApplParamDescr, 0,
					SQL_DESC_BIND_OFFSET_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAM_BIND_TYPE:		/* SQLULEN */
		/* SQL_BIND_TYPE */
		WriteData(ValuePtr, stmt->ApplParamDescr->sql_desc_bind_type, SQLULEN);
		break;
	case SQL_ATTR_PARAM_OPERATION_PTR:	/* SQLUSMALLINT* */
		return MNDBGetDescField(stmt->ApplParamDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAMSET_SIZE:		/* SQLULEN */
		return MNDBGetDescField(stmt->ApplParamDescr, 0,
					SQL_DESC_ARRAY_SIZE, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAMS_PROCESSED_PTR:	/* SQLULEN* */
		return MNDBGetDescField(stmt->ImplParamDescr, 0,
					SQL_DESC_ROWS_PROCESSED_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_PARAM_STATUS_PTR:		/* SQLUSMALLINT* */
		return MNDBGetDescField(stmt->ImplParamDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_QUERY_TIMEOUT:		/* SQLULEN */
		/* SQL_QUERY_TIMEOUT */
		WriteData(ValuePtr, stmt->qtimeout, SQLULEN);
		break;
	case SQL_ATTR_RETRIEVE_DATA:		/* SQLULEN */
		/* SQL_RETRIEVE_DATA */
		WriteData(ValuePtr, stmt->retrieveData, SQLULEN);
		break;
	case SQL_ATTR_ROW_ARRAY_SIZE:		/* SQLULEN */
	case SQL_ROWSET_SIZE:
		return MNDBGetDescField(stmt->ApplRowDescr, 0,
					SQL_DESC_ARRAY_SIZE, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:	/* SQLULEN* */
		return MNDBGetDescField(stmt->ApplRowDescr, 0,
					SQL_DESC_BIND_OFFSET_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROW_BIND_TYPE:		/* SQLULEN */
		WriteData(ValuePtr, stmt->ApplRowDescr->sql_desc_bind_type, SQLULEN);
		break;
	case SQL_ATTR_ROW_NUMBER:	     /* SQLULEN */
		if (stmt->State <= EXECUTED1) {
			/* Invalid cursor state */
			addStmtError(stmt, "24000", NULL, 0);
			return SQL_ERROR;
		}
		WriteData(ValuePtr, (SQLULEN) stmt->currentRow, SQLULEN);
		break;
	case SQL_ATTR_ROW_OPERATION_PTR:	/* SQLUSMALLINT* */
		return MNDBGetDescField(stmt->ApplRowDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROW_STATUS_PTR:		/* SQLUSMALLINT* */
		return MNDBGetDescField(stmt->ImplRowDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					BufferLength, StringLengthPtr);
	case SQL_ATTR_ROWS_FETCHED_PTR:		/* SQLULEN* */
		return MNDBGetDescField(stmt->ImplRowDescr, 0,
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
	case SQL_ATTR_FETCH_BOOKMARK_PTR:	/* SQLLEN* */
	case SQL_ATTR_KEYSET_SIZE:		/* SQLULEN */
		/* SQL_KEYSET_SIZE */
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
	ODBCLOG("SQLGetStmtAttr %p %s %p %d %p\n",
		StatementHandle, translateStmtAttribute(Attribute),
		ValuePtr, (int) BufferLength,
		StringLengthPtr);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return MNDBGetStmtAttr((ODBCStmt *) StatementHandle,
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
	ODBCLOG("SQLGetStmtAttrW %p %s %p %d %p\n",
		StatementHandle, translateStmtAttribute(Attribute),
		ValuePtr, (int) BufferLength,
		StringLengthPtr);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	/* there are no string-valued attributes */

	return MNDBGetStmtAttr((ODBCStmt *) StatementHandle,
			       Attribute,
			       ValuePtr,
			       BufferLength,
			       StringLengthPtr);
}
