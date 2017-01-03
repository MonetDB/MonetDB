/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
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
 * SQLDescribeParam()
 * CLI Compliance: ODBC (Microsoft)
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLDescribeParam(SQLHSTMT StatementHandle,
		 SQLUSMALLINT ParameterNumber,
		 SQLSMALLINT *DataTypePtr,
		 SQLULEN *ParameterSizePtr,
		 SQLSMALLINT *DecimalDigitsPtr,
		 SQLSMALLINT *NullablePtr)
{
	ODBCStmt *stmt = (ODBCStmt *) StatementHandle;
	ODBCDescRec *rec;

#ifdef ODBCDEBUG
	ODBCLOG("SQLDescribeParam " PTRFMT " %u " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST StatementHandle, (unsigned int) ParameterNumber,
		PTRFMTCAST DataTypePtr, PTRFMTCAST ParameterSizePtr,
		PTRFMTCAST DecimalDigitsPtr, PTRFMTCAST NullablePtr);
#endif

	if (!isValidStmt(stmt))
		 return SQL_INVALID_HANDLE;

	clearStmtErrors(stmt);

	/* check statement cursor state, query should be prepared or executed */
	if (stmt->State == INITED || stmt->State >= EXECUTED0) {
		/* Function sequence error */
		addStmtError(stmt, "HY010", NULL, 0);
		return SQL_ERROR;
	}

	if (ParameterNumber < 1 ||
	    ParameterNumber > stmt->ImplParamDescr->sql_desc_count) {
		/* Invalid descriptor index */
		addStmtError(stmt, "07009", NULL, 0);
		return SQL_ERROR;
	}

	rec = &stmt->ImplParamDescr->descRec[ParameterNumber];

	if (DataTypePtr)
		*DataTypePtr = rec->sql_desc_concise_type;

	if (NullablePtr)
		*NullablePtr = rec->sql_desc_nullable;

	/* also see SQLDescribeCol */
	if (ParameterSizePtr)
		*ParameterSizePtr = ODBCLength(rec, SQL_DESC_LENGTH);

	/* also see SQLDescribeCol */
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

	return SQL_SUCCESS;
}
