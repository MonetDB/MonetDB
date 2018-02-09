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
 * SQLDescribeCol()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"


static SQLRETURN
MNDBDescribeCol(ODBCStmt *stmt,
		SQLUSMALLINT ColumnNumber,
		SQLCHAR *ColumnName,
		SQLSMALLINT BufferLength,
		SQLSMALLINT *NameLengthPtr,
		SQLSMALLINT *DataTypePtr,
		SQLULEN *ColumnSizePtr,
		SQLSMALLINT *DecimalDigitsPtr,
		SQLSMALLINT *NullablePtr)
{
	ODBCDescRec *rec = NULL;

	/* check statement cursor state, query should be executed */
	if (stmt->State == INITED) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == PREPARED0) {
		/* Prepared statement not a cursor-specification */
		addStmtError(stmt, "07005", NULL, 0);
		return SQL_ERROR;
	}
	if (stmt->State == EXECUTED0) {
		/* Invalid cursor state */
		addStmtError(stmt, "24000", NULL, 0);
		return SQL_ERROR;
	}

	if (ColumnNumber < 1 ||
	    ColumnNumber > stmt->ImplRowDescr->sql_desc_count) {
		/* Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

	rec = stmt->ImplRowDescr->descRec + ColumnNumber;

	/* now copy the data */
	copyString(rec->sql_desc_name, strlen((char *) rec->sql_desc_name),
		   ColumnName, BufferLength, NameLengthPtr, SQLSMALLINT,
		   addStmtError, stmt, return SQL_ERROR);

	if (DataTypePtr)
		*DataTypePtr = rec->sql_desc_concise_type;

	/* also see SQLDescribeParam */
	if (ColumnSizePtr) {
		*ColumnSizePtr = ODBCLength(rec, SQL_DESC_LENGTH);
		if (*ColumnSizePtr == (SQLULEN) SQL_NO_TOTAL)
			*ColumnSizePtr = 0;
	}

	/* also see SQLDescribeParam */
	if (DecimalDigitsPtr) {
		switch (rec->sql_desc_concise_type) {
		case SQL_DECIMAL:
		case SQL_NUMERIC:
			*DecimalDigitsPtr = rec->sql_desc_scale;
			break;
		case SQL_BIT:
		case SQL_TINYINT:
		case SQL_SMALLINT:
		case SQL_INTEGER:
		case SQL_BIGINT:
		case SQL_HUGEINT:
			*DecimalDigitsPtr = 0;
			break;
		case SQL_TYPE_TIME:
		case SQL_TYPE_TIMESTAMP:
		case SQL_INTERVAL_SECOND:
		case SQL_INTERVAL_DAY_TO_SECOND:
		case SQL_INTERVAL_HOUR_TO_SECOND:
		case SQL_INTERVAL_MINUTE_TO_SECOND:
			*DecimalDigitsPtr = rec->sql_desc_precision;
			break;
		default:
			*DecimalDigitsPtr = 0;
			break;
		}
	}

	if (NullablePtr)
		*NullablePtr = rec->sql_desc_nullable;

	return stmt->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLDescribeCol(SQLHSTMT StatementHandle,
	       SQLUSMALLINT ColumnNumber,
	       SQLCHAR *ColumnName,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *NameLengthPtr,
	       SQLSMALLINT *DataTypePtr,
	       SQLULEN *ColumnSizePtr,
	       SQLSMALLINT *DecimalDigitsPtr,
	       SQLSMALLINT *NullablePtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeCol %p %u %p %d %p %p %p %p %p\n",
		StatementHandle, (unsigned int) ColumnNumber,
		ColumnName, (int) BufferLength,
		NameLengthPtr, DataTypePtr,
		ColumnSizePtr, DecimalDigitsPtr,
		NullablePtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return MNDBDescribeCol(stmt,
			       ColumnNumber,
			       ColumnName,
			       BufferLength,
			       NameLengthPtr,
			       DataTypePtr,
			       ColumnSizePtr,
			       DecimalDigitsPtr,
			       NullablePtr);
}

SQLRETURN SQL_API
SQLDescribeColA(SQLHSTMT StatementHandle,
		SQLUSMALLINT ColumnNumber,
		SQLCHAR *ColumnName,
		SQLSMALLINT BufferLength,
		SQLSMALLINT *NameLengthPtr,
		SQLSMALLINT *DataTypePtr,
		SQLULEN *ColumnSizePtr,
		SQLSMALLINT *DecimalDigitsPtr,
		SQLSMALLINT *NullablePtr)
{
	return SQLDescribeCol(StatementHandle,
			      ColumnNumber,
			      ColumnName,
			      BufferLength,
			      NameLengthPtr,
			      DataTypePtr,
			      ColumnSizePtr,
			      DecimalDigitsPtr,
			      NullablePtr);
}

SQLRETURN SQL_API
SQLDescribeColW(SQLHSTMT StatementHandle,
		SQLUSMALLINT ColumnNumber,
		SQLWCHAR *ColumnName,
		SQLSMALLINT BufferLength,
		SQLSMALLINT *NameLengthPtr,
		SQLSMALLINT *DataTypePtr,
		SQLULEN *ColumnSizePtr,
		SQLSMALLINT *DecimalDigitsPtr,
		SQLSMALLINT *NullablePtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	SQLCHAR *colname;
	SQLSMALLINT n;
	SQLRETURN rc = SQL_ERROR;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeColW %p %u %p %d %p %p %p %p %p\n",
		StatementHandle, (unsigned int) ColumnNumber,
		ColumnName, (int) BufferLength,
		NameLengthPtr, DataTypePtr,
		ColumnSizePtr, DecimalDigitsPtr,
		NullablePtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	rc = MNDBDescribeCol(stmt, ColumnNumber, NULL, 0, &n, DataTypePtr,
			     ColumnSizePtr, DecimalDigitsPtr, NullablePtr);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearStmtErrors(stmt);
	n++;			/* account for NUL byte */
	colname = malloc(n);
	if (colname == NULL) {
		/* Memory allocation error */
		addStmtError(stmt, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	rc = MNDBDescribeCol(stmt,
			     ColumnNumber,
			     colname,
			     n,
			     &n,
			     DataTypePtr,
			     ColumnSizePtr,
			     DecimalDigitsPtr,
			     NullablePtr);
	if (SQL_SUCCEEDED(rc)) {
		fixWcharOut(rc, colname, n, ColumnName, BufferLength,
			    NameLengthPtr, 1, addStmtError, stmt);
	}
	free(colname);

	return rc;
}
