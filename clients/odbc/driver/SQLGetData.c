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
 * SQLGetData()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

SQLRETURN SQL_API
SQLGetData(SQLHSTMT StatementHandle,
	   SQLUSMALLINT Col_or_Param_Num,
	   SQLSMALLINT TargetType,
	   SQLPOINTER TargetValuePtr,
	   SQLLEN BufferLength,
	   SQLLEN *StrLen_or_IndPtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetData " PTRFMT " %u %d\n", PTRFMTCAST StatementHandle,
		(unsigned int) Col_or_Param_Num, (int) TargetType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	assert(stmt->Dbc);
	assert(stmt->Dbc->mid);
	assert(stmt->hdl);

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be executed */
	if (stmt->State < EXECUTED0) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State <= EXECUTED1) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->rowSetSize == 0) {
		/* SQLFetch failed */
		/* General error */
		addStmtError(stmt, "HY000", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->rowSetSize > 1 &&
	    stmt->cursorType == SQL_CURSOR_FORWARD_ONLY) {
		/* Invalid cursor position */
		addStmtError(stmt, "HY109", NULL, 0);
		return SQL_ERROR;
	}
	if (Col_or_Param_Num <= 0 ||
	    Col_or_Param_Num > stmt->ImplRowDescr->sql_desc_count) {
		/* Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

	if (Col_or_Param_Num != stmt->currentCol)
		stmt->retrieved = 0;
	stmt->currentCol = Col_or_Param_Num;

	if (TargetType == SQL_ARD_TYPE) {
		ODBCDesc *desc = stmt->ApplRowDescr;

		if (Col_or_Param_Num > desc->sql_desc_count) {
			/* Invalid descriptor index */
			addStmtError(stmt, "07009", NULL, 0);
			return SQL_ERROR;
		}
		TargetType = desc->descRec[Col_or_Param_Num].sql_desc_concise_type;
	}

	return ODBCFetch(stmt, Col_or_Param_Num, TargetType, TargetValuePtr,
			 BufferLength, StrLen_or_IndPtr, StrLen_or_IndPtr,
			 UNAFFECTED, UNAFFECTED, UNAFFECTED, 0, 0);
}
