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
 * SQLFetch()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"
#include <time.h>
#ifdef HAVE_STRINGS_H
#include <strings.h>		/* for strncasecmp */
#endif

SQLRETURN
SQLFetch_(ODBCStmt *stmt)
{
	ODBCDesc *desc;
	ODBCDescRec *rec;
	int i;
	SQLULEN row;
	SQLINTEGER offset;
	SQLUSMALLINT *statusp;

	desc = stmt->ApplRowDescr;

	stmt->retrieved = 0;
	stmt->currentCol = 0;

	stmt->startRow += stmt->rowSetSize;
	stmt->rowSetSize = 0;
	stmt->currentRow = stmt->startRow + 1;
	if (mapi_seek_row(stmt->hdl, stmt->startRow, MAPI_SEEK_SET) != MOK) {
		/* Row value out of range */
		addStmtError(stmt, "HY107", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	}

	stmt->State = FETCHED;

	statusp = desc->sql_desc_array_status_ptr;

	if (stmt->retrieveData == SQL_RD_OFF) {
		/* don't really retrieve the data, just do as if,
		   updating the SQL_DESC_ARRAY_STATUS_PTR */
		stmt->rowSetSize = desc->sql_desc_array_size;

		if (stmt->startRow + stmt->rowSetSize > (SQLLEN) stmt->rowcount)
			stmt->rowSetSize = stmt->rowcount - stmt->startRow;

		if (stmt->rowSetSize <= 0) {
			stmt->rowSetSize = 0;
			return SQL_NO_DATA;
		}
		if (statusp) {
			for (row = 0; (SQLLEN) row < stmt->rowSetSize; row++)
				*statusp++ = SQL_ROW_SUCCESS;
			for (; row < desc->sql_desc_array_size; row++)
				*statusp++ = SQL_ROW_NOROW;
		}
		return SQL_SUCCESS;
	}

	if (desc->sql_desc_bind_offset_ptr)
		offset = *desc->sql_desc_bind_offset_ptr;
	else
		offset = 0;
	for (row = 0; row < desc->sql_desc_array_size; row++) {
		if (mapi_fetch_row(stmt->hdl) == 0) {
			if (desc->sql_desc_rows_processed_ptr)
				*desc->sql_desc_rows_processed_ptr = row;
			switch (mapi_error(stmt->Dbc->mid)) {
			case MOK:
				if (row == 0)
					return SQL_NO_DATA;
				break;
			case MTIMEOUT:
				if (statusp)
					*statusp = SQL_ROW_ERROR;
				/* Communication link failure */
				addStmtError(stmt, "08S01", mapi_error_str(stmt->Dbc->mid), 0);
				return SQL_ERROR;
			default:
				if (statusp)
					*statusp = SQL_ROW_ERROR;
				/* General error */
				addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
				return SQL_ERROR;
			}
			break;
		}
		if (statusp)
			*statusp = SQL_ROW_SUCCESS;

		stmt->rowSetSize++;

		for (i = 1; i <= desc->sql_desc_count; i++) {
			rec = &desc->descRec[i];
			if (rec->sql_desc_data_ptr == NULL)
				continue;
			stmt->retrieved = 0;
			if (ODBCFetch(stmt, i,
				      rec->sql_desc_concise_type,
				      rec->sql_desc_data_ptr,
				      rec->sql_desc_octet_length,
				      rec->sql_desc_octet_length_ptr,
				      rec->sql_desc_indicator_ptr,
				      rec->sql_desc_precision,
				      rec->sql_desc_scale,
				      rec->sql_desc_datetime_interval_precision,
				      offset, row) == SQL_ERROR) {
				if (statusp)
					*statusp = SQL_ROW_SUCCESS_WITH_INFO;
			}
		}
		if (statusp)
			statusp++;
	}
	if (desc->sql_desc_rows_processed_ptr)
		*desc->sql_desc_rows_processed_ptr = (SQLULEN) stmt->rowSetSize;

	if (statusp)
		while (row++ < desc->sql_desc_array_size)
			*statusp++ = SQL_ROW_NOROW;

	if (stmt->rowSetSize > 1) {
		mapi_seek_row(stmt->hdl, stmt->startRow, MAPI_SEEK_SET);
		mapi_fetch_row(stmt->hdl);
	}

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLFetch(SQLHSTMT hStmt)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;

#ifdef ODBCDEBUG
	ODBCLOG("SQLFetch " PTRFMT "\n", PTRFMTCAST hStmt);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	assert(stmt->hdl);

	/* check statement cursor state, query should be executed */
	if (stmt->State < EXECUTED0 || stmt->State == EXTENDEDFETCHED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	return SQLFetch_(stmt);
}
