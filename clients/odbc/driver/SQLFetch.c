/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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
 * Author: Martin van Dinther, Sjoerd Mullender
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
MNDBFetch(ODBCStmt *stmt, SQLUSMALLINT *RowStatusArray)
{
	ODBCDesc *ard, *ird;
	ODBCDescRec *rec;
	int i;
	SQLULEN row;
	SQLLEN offset;

	/* stmt->startRow is the (0 based) index of the first row we
	 * stmt->need to fetch */

	ard = stmt->ApplRowDescr;
	ird = stmt->ImplRowDescr;

	stmt->retrieved = 0;
	stmt->currentCol = 0;

	stmt->rowSetSize = 0;
	stmt->currentRow = stmt->startRow + 1;
	if (mapi_seek_row(stmt->hdl, stmt->startRow, MAPI_SEEK_SET) != MOK) {
		/* Row value out of range */
		addStmtError(stmt, "HY107", mapi_error_str(stmt->Dbc->mid), 0);
		return SQL_ERROR;
	}

	stmt->State = FETCHED;

	if (stmt->retrieveData == SQL_RD_OFF) {
		/* don't really retrieve the data, just do as if,
		   updating the SQL_DESC_ARRAY_STATUS_PTR */
		stmt->rowSetSize = ard->sql_desc_array_size;

		if (stmt->startRow + stmt->rowSetSize > (SQLLEN) stmt->rowcount)
			stmt->rowSetSize = stmt->rowcount - stmt->startRow;

		if (stmt->rowSetSize <= 0) {
			stmt->rowSetSize = 0;
			return SQL_NO_DATA;
		}
		if (RowStatusArray) {
			for (row = 0; (SQLLEN) row < stmt->rowSetSize; row++) {
				WriteValue(RowStatusArray, SQL_ROW_SUCCESS);
				RowStatusArray++;
			}
			for (; row < ard->sql_desc_array_size; row++) {
				WriteValue(RowStatusArray, SQL_ROW_NOROW);
				RowStatusArray++;
			}
		}
		return SQL_SUCCESS;
	}

	if (ard->sql_desc_bind_offset_ptr)
		offset = *ard->sql_desc_bind_offset_ptr;
	else
		offset = 0;
	for (row = 0; row < ard->sql_desc_array_size; row++) {
		if (mapi_fetch_row(stmt->hdl) == 0) {
			switch (mapi_error(stmt->Dbc->mid)) {
			case MOK:
				if (row == 0)
					return SQL_NO_DATA;
				break;
			case MTIMEOUT:
				if (RowStatusArray)
					WriteValue(RowStatusArray, SQL_ROW_ERROR);
				/* Timeout expired / Communication
				 * link failure */
				addStmtError(stmt, stmt->Dbc->sql_attr_connection_timeout ? "HYT00" : "08S01", mapi_error_str(stmt->Dbc->mid), 0);
				return SQL_ERROR;
			default:
				if (RowStatusArray)
					WriteValue(RowStatusArray, SQL_ROW_ERROR);
				/* General error */
				addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
				return SQL_ERROR;
			}
			break;
		}
		if (RowStatusArray)
			WriteValue(RowStatusArray, SQL_ROW_SUCCESS);

		stmt->rowSetSize++;

		for (i = 1; i <= ird->sql_desc_count; i++)
			ird->descRec[i].already_returned = 0;

		for (i = 1; i <= ard->sql_desc_count; i++) {
			rec = &ard->descRec[i];
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
				if (RowStatusArray)
					WriteValue(RowStatusArray, SQL_ROW_SUCCESS_WITH_INFO);
			}
		}
		if (RowStatusArray)
			RowStatusArray++;
	}
	if (ird->sql_desc_rows_processed_ptr)
		*ird->sql_desc_rows_processed_ptr = (SQLULEN) stmt->rowSetSize;

	if (RowStatusArray)
		while (row++ < ard->sql_desc_array_size) {
			WriteValue(RowStatusArray, SQL_ROW_NOROW);
			RowStatusArray++;
		}

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLFetch(SQLHSTMT StatementHandle)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLFetch " PTRFMT "\n", PTRFMTCAST StatementHandle);
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

	stmt->startRow += stmt->rowSetSize;

	return MNDBFetch(stmt, stmt->ImplRowDescr->sql_desc_array_status_ptr);
}
