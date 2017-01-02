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
 * SQLSetDescRec()
 * CLI Compliance: ISO 92
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
	ODBCLOG("SQLSetDescRec " PTRFMT " %d %s %s " LENFMT " %d %d " PTRFMT " " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST DescriptorHandle, (int) RecNumber,
		isAD(desc) ? translateCType(Type) : translateSQLType(Type),
		translateSubType(Type, SubType), LENCAST Length,
		(int) Precision, (int) Scale, PTRFMTCAST DataPtr,
		PTRFMTCAST StringLengthPtr, PTRFMTCAST IndicatorPtr);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	if (MNDBSetDescField(desc, RecNumber, SQL_DESC_TYPE,
			     (SQLPOINTER) (intptr_t) Type, 0) == SQL_ERROR)
		return SQL_ERROR;
	if ((Type == SQL_DATETIME || Type == SQL_INTERVAL) &&
	    MNDBSetDescField(desc, RecNumber, SQL_DESC_DATETIME_INTERVAL_CODE,
			     (SQLPOINTER) (intptr_t) SubType, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (MNDBSetDescField(desc, RecNumber, SQL_DESC_OCTET_LENGTH,
			     (SQLPOINTER) (intptr_t) Length, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (MNDBSetDescField(desc, RecNumber, SQL_DESC_PRECISION,
			     (SQLPOINTER) (intptr_t) Precision, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (MNDBSetDescField(desc, RecNumber, SQL_DESC_SCALE,
			     (SQLPOINTER) (intptr_t) Scale, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (MNDBSetDescField(desc, RecNumber, SQL_DESC_OCTET_LENGTH_PTR,
			     (SQLPOINTER) StringLengthPtr, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (MNDBSetDescField(desc, RecNumber, SQL_DESC_INDICATOR_PTR,
			     (SQLPOINTER) IndicatorPtr, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (MNDBSetDescField(desc, RecNumber, SQL_DESC_DATA_PTR,
			     (SQLPOINTER) DataPtr, 0) == SQL_ERROR)
		return SQL_ERROR;
	return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
