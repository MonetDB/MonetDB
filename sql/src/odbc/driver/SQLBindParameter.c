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
 * SQLBindParameter()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Note: this function is not supported (yet), it returns an error.
 * So parametrized SQL commands are not possible!
 * TODO: implement this function and corresponding behavior in
 * SQLPrepare() and SQLExecute().
 *
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLBindParameter(SQLHSTMT hStmt, SQLUSMALLINT ParameterNumber,
		 SQLSMALLINT InputOutputType, SQLSMALLINT ValueType,
		 SQLSMALLINT ParameterType, SQLUINTEGER ColumnSize,
		 SQLSMALLINT DecimalDigits, SQLPOINTER ParameterValuePtr,
		 SQLINTEGER BufferLength, SQLINTEGER *StrLen_or_IndPtr)
{
	ODBCStmt *stmt = (ODBCStmt *) hStmt;
	MapiMsg rc = MOK;

	(void) ColumnSize;
	(void) DecimalDigits;
	(void) BufferLength; /* only used for (unimplemented) output params */

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check input parameters */
	/* For safety: limit the maximum number of columns to bind */
	if (ParameterNumber <= 0 || ParameterNumber > MONETDB_MAX_BIND_COLS) {
		/* HY000 = General Error */
		addStmtError(stmt, "HY000",
			     "Maximum number of bind columns (8192) exceeded",
			     0);
		return SQL_ERROR;
	}

	if (InputOutputType != SQL_PARAM_INPUT) {
		/* HYC00 = Optional feature not implemented */
		addStmtError(stmt, "HYC00",
			     "Output parameters are not supported", 0);
		return SQL_ERROR;
	}

	switch (ValueType) {
	case SQL_C_CHAR:
		/* note about the cast: on a system with
		   sizeof(long)==8, SQLINTEGER is typedef'ed as int,
		   otherwise as long, but on those other systems, long
		   and int are the same size, so the cast works */
		rc = mapi_param_string(stmt->hdl, ParameterNumber - 1,
				       ParameterType, ParameterValuePtr,
				       (int *) StrLen_or_IndPtr);
		break;
	case SQL_C_SSHORT:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_SHORT, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_USHORT:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_USHORT, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_SLONG:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_LONG, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_ULONG:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_ULONG, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_STINYINT:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_TINY, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_UTINYINT:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_UTINY, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_SBIGINT:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_LONGLONG, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_UBIGINT:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_ULONGLONG, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_FLOAT:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_FLOAT, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_DOUBLE:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_DOUBLE, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_TYPE_DATE:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_DATE, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_TYPE_TIME:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_TIME, ParameterType, ParameterValuePtr);
		break;
	case SQL_C_TYPE_TIMESTAMP:
		rc = mapi_param_type(stmt->hdl, ParameterNumber - 1,
				     MAPI_DATETIME, ParameterType, ParameterValuePtr);
		break;
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

	if (rc == MOK)
		return SQL_SUCCESS;

	addStmtError(stmt, "HY000", mapi_error_str(stmt->Dbc->mid), 0);
	return SQL_ERROR;
}
