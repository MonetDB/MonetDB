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
SQLDescribeCol_(ODBCStmt *stmt,
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
	if (ColumnSizePtr)
		*ColumnSizePtr = ODBCLength(rec, 0);

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
	ODBCLOG("SQLDescribeCol " PTRFMT " %u\n",
		PTRFMTCAST StatementHandle, (unsigned int) ColumnNumber);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	return SQLDescribeCol_(stmt,
			       ColumnNumber,
			       ColumnName,
			       BufferLength,
			       NameLengthPtr,
			       DataTypePtr,
			       ColumnSizePtr,
			       DecimalDigitsPtr,
			       NullablePtr);
}

#ifdef WITH_WCHAR
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
	ODBCLOG("SQLDescribeColW " PTRFMT " %u\n",
		PTRFMTCAST StatementHandle, (unsigned int) ColumnNumber);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	rc = SQLDescribeCol_(stmt, ColumnNumber, NULL, 0, &n, DataTypePtr,
			     ColumnSizePtr, DecimalDigitsPtr, NullablePtr);
	if (!SQL_SUCCEEDED(rc))
		return rc;
	clearStmtErrors(stmt);
	n++;			/* account for NUL byte */
	colname = malloc(n);
	rc = SQLDescribeCol_(stmt,
			     ColumnNumber,
			     colname,
			     n,
			     &n,
			     DataTypePtr,
			     ColumnSizePtr,
			     DecimalDigitsPtr,
			     NullablePtr);
	fixWcharOut(rc, colname, n, ColumnName, BufferLength, NameLengthPtr,
		    1, addStmtError, stmt);

	return rc;
}
#endif /* WITH_WCHAR */
