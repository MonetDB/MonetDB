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
	OdbcOutHostVar outVar = NULL;

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

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

	switch (nTargetType) {
	case SQL_C_CHAR:
	case SQL_C_SSHORT:
	case SQL_C_USHORT:
	case SQL_C_SLONG:
	case SQL_C_ULONG:
	case SQL_C_STINYINT:
	case SQL_C_UTINYINT:
	case SQL_C_SBIGINT:
	case SQL_C_UBIGINT:
	case SQL_C_FLOAT:
	case SQL_C_DOUBLE:
	case SQL_C_TYPE_DATE:
	case SQL_C_TYPE_TIME:
	case SQL_C_TYPE_TIMESTAMP:
	case SQL_C_DEFAULT:
		/* these are supported */
		break;

	case SQL_C_BINARY:
	case SQL_C_NUMERIC:
	case SQL_C_GUID:
		/* these are NOT supported */
	default:
		/* HY003 = Invalid application buffer type */
		addStmtError(stmt, "HY003", NULL, 0);
		return SQL_ERROR;
	}

	if (pTargetValue == NULL) {
		/* the ODBC spec specifies this should be possible, 
		 *      (it unbinds a column) */
		delOdbcOutArray(&(stmt->bindCols), nCol);
		return SQL_SUCCESS;
	}

	if (nTargetValueMax <= 0 &&
	    (nTargetType == SQL_C_CHAR || nTargetType == SQL_C_BINARY ||
	     nTargetType == SQL_C_NUMERIC)) {
		/* for variable length data we need a buffer length */
		/* HY090 = Invalid string or buffer length */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}


	/* Now store the bind information in stmt struct */
	outVar = makeOdbcOutHostVar(nCol, nTargetType, pTargetValue,
				    nTargetValueMax, pnLengthOrIndicator);

	/* Note: there may already be bind information stored, in that
	   case the column is rebound, so old bind info is overwritten */
	addOdbcOutArray(&stmt->bindCols, outVar);

	return SQL_SUCCESS;
}
