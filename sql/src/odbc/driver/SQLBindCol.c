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
	int mapitype;

#ifdef ODBCDEBUG
	ODBCLOG("SQLBindCol\n");
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

	ODBCdelbindcol(stmt, nCol);

	switch (nTargetType) {
	case SQL_C_CHAR:
#ifdef SQL_C_XML
	case SQL_C_XML:
#endif
	case SQL_C_VARBOOKMARK:
		/* mapi_store_field doesn't copy the data but only the
		   pointer to the data */
		mapitype = MAPI_VARCHAR;
		pTargetValue = ODBCaddbindcol(stmt, nCol, pTargetValue, nTargetValueMax, pnLengthOrIndicator);
		break;
	case SQL_C_LONG:
	case SQL_C_SLONG:
		mapitype = MAPI_LONG;
		break;
	case SQL_C_ULONG:
		mapitype = MAPI_ULONG;
		break;
	case SQL_C_SHORT:
	case SQL_C_SSHORT:
		mapitype = MAPI_SHORT;
		break;
	case SQL_C_USHORT:
		mapitype = MAPI_USHORT;
		break;
	case SQL_C_BIT:
	case SQL_C_TINYINT:
	case SQL_C_STINYINT:
		mapitype = MAPI_TINY;
		break;
	case SQL_C_UTINYINT:
		mapitype = MAPI_UTINY;
		break;
	case SQL_C_SBIGINT:
		mapitype = MAPI_LONGLONG;
		break;
	case SQL_C_UBIGINT:
		mapitype = MAPI_ULONGLONG;
		break;
	case SQL_C_FLOAT:
		mapitype = MAPI_FLOAT;
		break;
	case SQL_C_DOUBLE:
		mapitype = MAPI_DOUBLE;
		break;
	case SQL_C_TYPE_DATE:
		mapitype = MAPI_DATE;
		break;
	case SQL_C_TYPE_TIME:
		mapitype = MAPI_TIME;
		break;
	case SQL_C_TYPE_TIMESTAMP:
		mapitype = MAPI_DATETIME;
		break;
	default:
/* TODO: finish implementation */
		/* unimplemented conversion */
		/* HY003 = Invalid application buffer type */
		addStmtError(stmt, "HY003", NULL, 0);
		return SQL_ERROR;
	}

	if (pTargetValue != NULL && nTargetValueMax <= 0 &&
	    (nTargetType == SQL_C_CHAR || nTargetType == SQL_C_BINARY ||
	     nTargetType == SQL_C_NUMERIC)) {
		/* for variable length data we need a buffer length */
		/* HY090 = Invalid string or buffer length */
		addStmtError(stmt, "HY090", NULL, 0);
		return SQL_ERROR;
	}

	mapi_bind_var(stmt->hdl, nCol - 1, mapitype, pTargetValue);

	return SQL_SUCCESS;
}
