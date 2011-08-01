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
 * SQLSetStmtAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLSetStmtAttr_(ODBCStmt *stmt,
		SQLINTEGER Attribute,
		SQLPOINTER Value,
		SQLINTEGER StringLength)
{
	/* TODO: check parameters: Value and StringLength */

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
#define desc ((ODBCDesc *) Value)	/* abbrev. */
	case SQL_ATTR_APP_PARAM_DESC:
		if (Value == SQL_NULL_HDESC ||
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
	case SQL_ATTR_APP_ROW_DESC:
		if (Value == SQL_NULL_HDESC ||
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
	case SQL_ATTR_CURSOR_SCROLLABLE:
		switch ((SQLUINTEGER) (size_t) Value) {
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
		stmt->cursorScrollable = (SQLUINTEGER) (size_t) Value;
		break;
	case SQL_ATTR_CURSOR_TYPE:
		switch ((SQLUINTEGER) (size_t) Value) {
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
	case SQL_ATTR_IMP_PARAM_DESC:
	case SQL_ATTR_IMP_ROW_DESC:
		/* Invalid use of an automatically allocated
		   descriptor handle */
		addStmtError(stmt, "HY017", NULL, 0);
		return SQL_ERROR;
	case SQL_ATTR_NOSCAN:
		switch ((SQLUINTEGER) (size_t) Value) {
		case SQL_NOSCAN_ON:
		case SQL_NOSCAN_OFF:
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		stmt->noScan = (SQLUINTEGER) (size_t) Value;
		break;
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
		return SQLSetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_BIND_OFFSET_PTR, Value, StringLength);
	case SQL_ATTR_PARAM_BIND_TYPE:
		return SQLSetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_BIND_TYPE, Value, StringLength);
	case SQL_ATTR_PARAM_OPERATION_PTR:
		return SQLSetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, StringLength);
	case SQL_ATTR_PARAM_STATUS_PTR:
		return SQLSetDescField_(stmt->ImplParamDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, StringLength);
	case SQL_ATTR_PARAMS_PROCESSED_PTR:
		return SQLSetDescField_(stmt->ImplParamDescr, 0, SQL_DESC_ROWS_PROCESSED_PTR, Value, StringLength);
	case SQL_ATTR_PARAMSET_SIZE:
		return SQLSetDescField_(stmt->ApplParamDescr, 0, SQL_DESC_ARRAY_SIZE, Value, StringLength);
	case SQL_ATTR_RETRIEVE_DATA:
		switch ((SQLUINTEGER) (size_t) Value) {
		case SQL_RD_ON:
		case SQL_RD_OFF:
			break;
		default:
			/* Invalid attribute value */
			addStmtError(stmt, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		stmt->retrieveData = (SQLUINTEGER) (size_t) Value;
		break;
	case SQL_ATTR_ROW_ARRAY_SIZE:
	case SQL_ROWSET_SIZE:
		return SQLSetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_ARRAY_SIZE, Value, StringLength);
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:
		return SQLSetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_BIND_OFFSET_PTR, Value, StringLength);
	case SQL_ATTR_ROW_BIND_TYPE:
		return SQLSetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_BIND_TYPE, Value, StringLength);
	case SQL_ATTR_ROW_OPERATION_PTR:
		return SQLSetDescField_(stmt->ApplRowDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, StringLength);
	case SQL_ATTR_ROW_STATUS_PTR:
		return SQLSetDescField_(stmt->ImplRowDescr, 0, SQL_DESC_ARRAY_STATUS_PTR, Value, StringLength);
	case SQL_ATTR_ROWS_FETCHED_PTR:
		return SQLSetDescField_(stmt->ImplRowDescr, 0, SQL_DESC_ROWS_PROCESSED_PTR, Value, StringLength);

		/* TODO: implement requested behavior */
	case SQL_ATTR_ASYNC_ENABLE:
	case SQL_ATTR_CONCURRENCY:
	case SQL_ATTR_CURSOR_SENSITIVITY:
	case SQL_ATTR_ENABLE_AUTO_IPD:
	case SQL_ATTR_FETCH_BOOKMARK_PTR:
	case SQL_ATTR_KEYSET_SIZE:
	case SQL_ATTR_MAX_LENGTH:
	case SQL_ATTR_MAX_ROWS:
	case SQL_ATTR_METADATA_ID:
	case SQL_ATTR_QUERY_TIMEOUT:
	case SQL_ATTR_ROW_NUMBER:
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

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLSetStmtAttr(SQLHSTMT hStmt,
	       SQLINTEGER Attribute,
	       SQLPOINTER Value,
	       SQLINTEGER StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetStmtAttr " PTRFMT " %d\n",
		PTRFMTCAST hStmt, (int) Attribute);
#endif

	if (!isValidStmt((ODBCStmt *) hStmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	return SQLSetStmtAttr_((ODBCStmt *) hStmt, Attribute, Value, StringLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLSetStmtAttrW(SQLHSTMT hStmt,
		SQLINTEGER Attribute,
		SQLPOINTER Value,
		SQLINTEGER StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetStmtAttrW " PTRFMT " %d\n",
		PTRFMTCAST hStmt, (int) Attribute);
#endif

	if (!isValidStmt((ODBCStmt *) hStmt))
		return SQL_INVALID_HANDLE;

	clearStmtErrors((ODBCStmt *) hStmt);

	/* there are no string-valued attributes */

	return SQLSetStmtAttr_((ODBCStmt *) hStmt, Attribute, Value, StringLength);
}
#endif /* WITH_WCHAR */
