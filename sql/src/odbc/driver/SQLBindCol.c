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
 * SQLBindCol()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN
SQLBindCol(SQLHSTMT hStmt, SQLUSMALLINT nCol, SQLSMALLINT nTargetType,
	   SQLPOINTER pTargetValue, SQLINTEGER nTargetValueMax,
	   SQLINTEGER *pnLengthOrIndicator)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	ODBCDesc *desc;		/* Application Row Descriptor */

#ifdef ODBCDEBUG
	ODBCLOG("SQLBindCol %d %d\n", nCol, nTargetType);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	assert(stmt->Dbc);

	clearStmtErrors(stmt);

	/* check input parameters */
	/* column number 0 (Bookmark column) is not supported */
	if (nCol == 0) {
		/* HYC00 = Optional feature not implemented */
		addStmtError(stmt, "HYC00", NULL, 0);
		return SQL_ERROR;
	}
	/* For safety: limit the maximum number of columns to bind */
	if (nCol > MONETDB_MAX_BIND_COLS) {
		/* HY000 = General Error */
		addStmtError(stmt, "HY000",
			     "Maximum number of bind columns (8192) exceeded",
			     0);
		return SQL_ERROR;
	}

	desc = stmt->ApplRowDescr;
	if (pTargetValue == NULL && nCol == desc->sql_desc_count) {
		int i = desc->sql_desc_count - 1;

		while (i > 0 && desc->descRec[i].sql_desc_data_ptr == NULL)
			i--;
		setODBCDescRecCount(desc, i);
	} else {
		ODBCDescRec *rec;
		SQLRETURN rc;

		if (nCol > desc->sql_desc_count)
			setODBCDescRecCount(desc, nCol);
		rc = SQLSetDescField_(desc, nCol, SQL_DESC_CONCISE_TYPE,
				      (SQLPOINTER) (ssize_t) nTargetType, 0);
		if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO)
			return rc;
		rec = &desc->descRec[nCol];
		rec->sql_desc_octet_length = nTargetValueMax;
		rec->sql_desc_data_ptr = pTargetValue;
		rec->sql_desc_indicator_ptr = pnLengthOrIndicator;
		rec->sql_desc_octet_length_ptr = pnLengthOrIndicator;
	}

	return SQL_SUCCESS;
}
