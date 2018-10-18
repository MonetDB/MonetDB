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
 * SQLSetStmtAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
MNDBSetStmtAttr(ODBCStmt *stmt,
		SQLINTEGER Attribute,
		SQLPOINTER ValuePtr,
		SQLINTEGER StringLength)
{
	/* TODO: check parameters: ValuePtr and StringLength */

	if (Attribute == SQL_ATTR_CONCURRENCY ||
	    Attribute == SQL_ATTR_CURSOR_SCROLLABLE ||
	    Attribute == SQL_ATTR_CURSOR_SENSITIVITY ||
	    Attribute == SQL_ATTR_CURSOR_TYPE ||
	    Attribute == SQL_ATTR_USE_BOOKMARKS) {
		if (stmt->State >= EXECUTED0) {
			/* Invalid cursor state */
			addStmtError(stmt, "24000", NULL, 0);
			return SQL_ERROR;
		}
		if (stmt->State > INITED) {
			/* Attribute cannot be set now */
			addStmtError(stmt, "HY011", NULL, 0);
			return SQL_ERROR;
		}
	}

	switch (Attribute) {
#define desc ((ODBCDesc *) ValuePtr)	/* abbrev. */
	case SQL_ATTR_APP_PARAM_DESC:		/* SQLHANDLE */
		if (ValuePtr == SQL_NULL_HDESC ||
		    desc == stmt->AutoApplParamDescr) {
			stmt->ApplParamDescr = stmt->AutoApplParamDescr;
			return SQL_SUCCESS;
		}
		if (!isValidDesc(desc)) {
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		if (desc->sql_desc_alloc_type == SQL_DESC_ALLOC_AUTO) {
			/* Invalid use of an automatically allocated
			   descriptor handle */
			addStmtError(stmt, "HY017", NULL, 0);
			return SQL_ERROR;
		}
		stmt->ApplParamDescr = desc;
		break;
	case SQL_ATTR_APP_ROW_DESC:		/* SQLHANDLE */
		if (ValuePtr == SQL_NULL_HDESC ||
		    desc == stmt->AutoApplRowDescr) {
			stmt->ApplRowDescr = stmt->AutoApplRowDescr;
			return SQL_SUCCESS;
		}
		if (!isValidDesc(desc)) {
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		if (desc->sql_desc_alloc_type == SQL_DESC_ALLOC_AUTO) {
			/* Invalid use of an automatically allocated
			   descriptor handle */
			addStmtError(stmt, "HY017", NULL, 0);
			return SQL_ERROR;
		}
		stmt->ApplRowDescr = desc;
		break;
#undef desc
	case SQL_ATTR_CURSOR_SCROLLABLE:	/* SQLULEN */
		switch ((SQLULEN) (uintptr_t) ValuePtr) {
		case SQL_NONSCROLLABLE:
			stmt->cursorType = SQL_CURSOR_FORWARD_ONLY;
			break;
		case SQL_SCROLLABLE:
			stmt->cursorType = SQL_CURSOR_STATIC;
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		stmt->cursorScrollable = (SQLULEN) (uintptr_t) ValuePtr;
		break;
	case SQL_ATTR_CURSOR_TYPE:		/* SQLULEN */
		switch ((SQLULEN) (uintptr_t) ValuePtr) {
		case SQL_CURSOR_KEYSET_DRIVEN:
		case SQL_CURSOR_DYNAMIC:
			/* Option value changed */
			addStmtError(stmt, "01S02", NULL, 0);

			/* fall through */
		case SQL_CURSOR_STATIC:
			stmt->cursorScrollable = SQL_SCROLLABLE;
			stmt->cursorType = SQL_CURSOR_STATIC;
			break;
		case SQL_CURSOR_FORWARD_ONLY:
			stmt->cursorScrollable = SQL_NONSCROLLABLE;
			stmt->cursorType = SQL_CURSOR_FORWARD_ONLY;
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		break;
	case SQL_ATTR_ENABLE_AUTO_IPD:		/* SQLULEN */
		switch ((SQLULEN) (uintptr_t) ValuePtr) {
		case SQL_TRUE:
		case SQL_FALSE:
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		/* ignore value, always treat as SQL_TRUE */
		break;
	case SQL_ATTR_IMP_PARAM_DESC:		/* SQLHANDLE */
	case SQL_ATTR_IMP_ROW_DESC:		/* SQLHANDLE */
		/* Invalid use of an automatically allocated
		   descriptor handle */
		addStmtError(stmt, "HY017", NULL, 0);
		return SQL_ERROR;
	case SQL_ATTR_MAX_LENGTH:		/* SQLULEN */
	case SQL_ATTR_MAX_ROWS:			/* SQLULEN */
		if ((SQLULEN) (uintptr_t) ValuePtr != 0 &&
		    (SQLULEN) (uintptr_t) ValuePtr != 2147483647)
			addStmtError(stmt, "01S02", NULL, 0);
		break;
	case SQL_ATTR_NOSCAN:			/* SQLULEN */
		switch ((SQLULEN) (uintptr_t) ValuePtr) {
		case SQL_NOSCAN_ON:
		case SQL_NOSCAN_OFF:
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		stmt->noScan = (SQLULEN) (uintptr_t) ValuePtr;
		break;
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:	/* SQLULEN* */
		return MNDBSetDescField(stmt->ApplParamDescr, 0,
					SQL_DESC_BIND_OFFSET_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_PARAM_BIND_TYPE:		/* SQLULEN */
	{
		SQLUINTEGER v = (SQLUINTEGER) (SQLULEN) (uintptr_t) ValuePtr;
		return MNDBSetDescField(stmt->ApplParamDescr, 0,
					SQL_DESC_BIND_TYPE, (SQLPOINTER) (uintptr_t) v,
					StringLength);
	}
	case SQL_ATTR_PARAM_OPERATION_PTR:	/* SQLUSMALLINT* */
		return MNDBSetDescField(stmt->ApplParamDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_PARAM_STATUS_PTR:		/* SQLUSMALLINT* */
		return MNDBSetDescField(stmt->ImplParamDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_PARAMS_PROCESSED_PTR:	/* SQLULEN* */
		return MNDBSetDescField(stmt->ImplParamDescr, 0,
					SQL_DESC_ROWS_PROCESSED_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_PARAMSET_SIZE:		/* SQLULEN */
		return MNDBSetDescField(stmt->ApplParamDescr, 0,
					SQL_DESC_ARRAY_SIZE, ValuePtr,
					StringLength);
	case SQL_ATTR_QUERY_TIMEOUT:		/* SQLULEN */
		if ((uintptr_t) ValuePtr > 0x7FFFFFFF) {
			stmt->qtimeout = 0x7FFFFFFF;
			addStmtError(stmt, "01S02", NULL, 0);
		} else {
			stmt->qtimeout = (SQLULEN) (uintptr_t) ValuePtr;
		}
		break;
	case SQL_ATTR_RETRIEVE_DATA:		/* SQLULEN */
		switch ((SQLULEN) (uintptr_t) ValuePtr) {
		case SQL_RD_ON:
		case SQL_RD_OFF:
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		stmt->retrieveData = (SQLULEN) (uintptr_t) ValuePtr;
		break;
	case SQL_ATTR_ROW_ARRAY_SIZE:		/* SQLULEN */
	case SQL_ROWSET_SIZE:
		return MNDBSetDescField(stmt->ApplRowDescr, 0,
					SQL_DESC_ARRAY_SIZE, ValuePtr,
					StringLength);
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:	/* SQLULEN* */
		return MNDBSetDescField(stmt->ApplRowDescr, 0,
					SQL_DESC_BIND_OFFSET_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_ROW_BIND_TYPE:		/* SQLULEN */
		return MNDBSetDescField(stmt->ApplRowDescr, 0,
					SQL_DESC_BIND_TYPE, ValuePtr,
					StringLength);
	case SQL_ATTR_ROW_OPERATION_PTR:	/* SQLUSMALLINT* */
		return MNDBSetDescField(stmt->ApplRowDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_ROW_STATUS_PTR:		/* SQLUSMALLINT* */
		return MNDBSetDescField(stmt->ImplRowDescr, 0,
					SQL_DESC_ARRAY_STATUS_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_ROWS_FETCHED_PTR:		/* SQLULEN* */
		return MNDBSetDescField(stmt->ImplRowDescr, 0,
					SQL_DESC_ROWS_PROCESSED_PTR, ValuePtr,
					StringLength);
	case SQL_ATTR_METADATA_ID:		/* SQLULEN */
		switch ((SQLULEN) (uintptr_t) ValuePtr) {
		case SQL_TRUE:
		case SQL_FALSE:
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		stmt->Dbc->sql_attr_metadata_id = (SQLUINTEGER) (SQLULEN) (uintptr_t) ValuePtr;
		break;

	case SQL_ATTR_CONCURRENCY:		/* SQLULEN */
		switch ((SQLULEN) (uintptr_t) ValuePtr) {
		case SQL_CONCUR_READ_ONLY:
			/* the only value we support */
			break;
		case SQL_CONCUR_LOCK:
		case SQL_CONCUR_ROWVER:
		case SQL_CONCUR_VALUES:
			/* Optional feature not implemented */
			addStmtError(stmt, "HYC00", NULL, 0);
			return SQL_ERROR;
		}
		break;

	case SQL_ATTR_ROW_NUMBER:	     /* SQLULEN */
		/* read-only attribute */
	default:
		/* Invalid attribute/option identifier */
		addStmtError(stmt, "HY092", NULL, 0);
		return SQL_ERROR;

		/* TODO: implement requested behavior */
	case SQL_ATTR_ASYNC_ENABLE:		/* SQLULEN */
#ifdef SQL_ATTR_ASYNC_STMT_EVENT
	case SQL_ATTR_ASYNC_EVENT:		/* SQLPOINTER */
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCALLBACK
	case SQL_ATTR_ASYNC_PCALLBACK:		/* SQLPOINTER */
#endif
#ifdef SQL_ATTR_ASYNC_STMT_PCONTEXT
	case SQL_ATTR_ASYNC_PCONTEXT:		/* SQLPOINTER */
#endif
	case SQL_ATTR_CURSOR_SENSITIVITY:	/* SQLULEN */
	case SQL_ATTR_FETCH_BOOKMARK_PTR:	/* SQLLEN* */
	case SQL_ATTR_KEYSET_SIZE:		/* SQLULEN */
	case SQL_ATTR_SIMULATE_CURSOR:		/* SQLULEN */
	case SQL_ATTR_USE_BOOKMARKS:		/* SQLULEN */
		/* Optional feature not implemented */
		addStmtError(stmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	}

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLSetStmtAttr(SQLHSTMT StatementHandle,
	       SQLINTEGER Attribute,
	       SQLPOINTER ValuePtr,
	       SQLINTEGER StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetStmtAttr %p %s %p %d\n",
		StatementHandle, translateStmtAttribute(Attribute),
		ValuePtr, (int) StringLength);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	return MNDBSetStmtAttr((ODBCStmt *) StatementHandle,
			       Attribute,
			       ValuePtr,
			       StringLength);
}

SQLRETURN SQL_API
SQLSetStmtAttrW(SQLHSTMT StatementHandle,
		SQLINTEGER Attribute,
		SQLPOINTER ValuePtr,
		SQLINTEGER StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetStmtAttrW %p %s %p %d\n",
		StatementHandle, translateStmtAttribute(Attribute),
		ValuePtr, (int) StringLength);
#endif

	if (!isValidStmt((ODBCStmt *) StatementHandle))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) StatementHandle);

	/* there are no string-valued attributes */

	return MNDBSetStmtAttr((ODBCStmt *) StatementHandle,
			       Attribute,
			       ValuePtr,
			       StringLength);
}
