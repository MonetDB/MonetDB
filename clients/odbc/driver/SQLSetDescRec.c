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
 * Copyright August 2008-2014 MonetDB B.V.
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
 * SQLSetDescRec()
 * CLI Compliance: IOS 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

#ifdef ODBCDEBUG
static char *
translateSubType(SQLSMALLINT Type, SQLSMALLINT SubType)
{
	if (Type == SQL_DATETIME) {
		switch (SubType) {
		case SQL_CODE_DATE:
			return "SQL_CODE_DATE";
		case SQL_CODE_TIME:
			return "SQL_CODE_TIME";
		case SQL_CODE_TIMESTAMP:
			return "SQL_CODE_TIMESTAMP";
		default:
			return "unknown";
		}
	} else if (Type == SQL_INTERVAL) {
		switch (SubType) {
		case SQL_CODE_MONTH:
			return "SQL_CODE_MONTH";
		case SQL_CODE_YEAR:
			return "SQL_CODE_YEAR";
		case SQL_CODE_YEAR_TO_MONTH:
			return "SQL_CODE_YEAR_TO_MONTH";
		case SQL_CODE_DAY:
			return "SQL_CODE_DAY";
		case SQL_CODE_HOUR:
			return "SQL_CODE_HOUR";
		case SQL_CODE_MINUTE:
			return "SQL_CODE_MINUTE";
		case SQL_CODE_SECOND:
			return "SQL_CODE_SECOND";
		case SQL_CODE_DAY_TO_HOUR:
			return "SQL_CODE_DAY_TO_HOUR";
		case SQL_CODE_DAY_TO_MINUTE:
			return "SQL_CODE_DAY_TO_MINUTE";
		case SQL_CODE_DAY_TO_SECOND:
			return "SQL_CODE_DAY_TO_SECOND";
		case SQL_CODE_HOUR_TO_MINUTE:
			return "SQL_CODE_HOUR_TO_MINUTE";
		case SQL_CODE_HOUR_TO_SECOND:
			return "SQL_CODE_HOUR_TO_SECOND";
		case SQL_CODE_MINUTE_TO_SECOND:
			return "SQL_CODE_MINUTE_TO_SECOND";
		default:
			return "unknown";
		}
	} else
		return "unused";
}
#endif

SQLRETURN SQL_API
SQLSetDescRec(SQLHDESC DescriptorHandle,
	      SQLSMALLINT RecNumber,
	      SQLSMALLINT Type,
	      SQLSMALLINT SubType,
	      SQLLEN Length,
	      SQLSMALLINT Precision,
	      SQLSMALLINT Scale,
	      SQLPOINTER DataPtr,
	      SQLLEN *StringLengthPtr,
	      SQLLEN *IndicatorPtr)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetDescRec " PTRFMT " %d %s %s " LENFMT " %d %d\n",
		PTRFMTCAST DescriptorHandle, (int) RecNumber,
		isAD(desc) ? translateCType(Type) : translateSQLType(Type),
		translateSubType(Type, SubType), LENCAST Length,
		(int) Precision, (int) Scale);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	if (SQLSetDescField_(desc, RecNumber, SQL_DESC_TYPE,
			     (SQLPOINTER) (ssize_t) Type, 0) == SQL_ERROR)
		return SQL_ERROR;
	if ((Type == SQL_DATETIME || Type == SQL_INTERVAL) &&
	    SQLSetDescField_(desc, RecNumber, SQL_DESC_DATETIME_INTERVAL_CODE,
			     (SQLPOINTER) (ssize_t) SubType, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, RecNumber, SQL_DESC_OCTET_LENGTH,
			     (SQLPOINTER) (ssize_t) Length, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, RecNumber, SQL_DESC_PRECISION,
			     (SQLPOINTER) (ssize_t) Precision, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, RecNumber, SQL_DESC_SCALE,
			     (SQLPOINTER) (ssize_t) Scale, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, RecNumber, SQL_DESC_OCTET_LENGTH_PTR,
			     (SQLPOINTER) StringLengthPtr, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, RecNumber, SQL_DESC_INDICATOR_PTR,
			     (SQLPOINTER) IndicatorPtr, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, RecNumber, SQL_DESC_DATA_PTR,
			     (SQLPOINTER) DataPtr, 0) == SQL_ERROR)
		return SQL_ERROR;
	return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
