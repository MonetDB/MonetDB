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
	OdbcInHostVar inVar = NULL;

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

	/* Now store the bind information in stmt struct */
	inVar = makeOdbcInHostVar(ParameterNumber, InputOutputType, ValueType,
				  ParameterType, ColumnSize, DecimalDigits,
				  ParameterValuePtr, BufferLength,
				  StrLen_or_IndPtr);

	/* Note: there may already be bind information stored, in that
	   case the column is rebound, so old bind info is overwritten */
	addOdbcInArray(&stmt->bindParams, inVar);

	return SQL_SUCCESS;
}
