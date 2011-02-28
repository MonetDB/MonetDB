/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * SQLSetDescRec()
 * CLI Compliance: IOS 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLSetDescRec(SQLHDESC hDescriptorHandle,
	      SQLSMALLINT nRecordNumber,
	      SQLSMALLINT nType,
	      SQLSMALLINT nSubType,
	      SQLLEN nLength,
	      SQLSMALLINT nPrecision,
	      SQLSMALLINT nScale,
	      SQLPOINTER pData,
	      SQLLEN *pnStringLength,
	      SQLLEN *pnIndicator)
{
	ODBCDesc *desc = (ODBCDesc *) hDescriptorHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetDescRec " PTRFMT " %d %d %d " LENFMT " %d %d\n",
		PTRFMTCAST hDescriptorHandle, (int) nRecordNumber,
		(int) nType, (int) nSubType, LENCAST nLength,
		(int) nPrecision, (int) nScale);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	if (SQLSetDescField_(desc, nRecordNumber, SQL_DESC_TYPE, (SQLPOINTER) (ssize_t) nType, 0) == SQL_ERROR)
		return SQL_ERROR;
	if ((nType == SQL_DATETIME || nType == SQL_INTERVAL) && SQLSetDescField_(desc, nRecordNumber, SQL_DESC_DATETIME_INTERVAL_CODE, (SQLPOINTER) (ssize_t) nSubType, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, nRecordNumber, SQL_DESC_OCTET_LENGTH, (SQLPOINTER) (ssize_t) nLength, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, nRecordNumber, SQL_DESC_PRECISION, (SQLPOINTER) (ssize_t) nPrecision, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, nRecordNumber, SQL_DESC_SCALE, (SQLPOINTER) (ssize_t) nScale, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, nRecordNumber, SQL_DESC_OCTET_LENGTH_PTR, (SQLPOINTER) pnStringLength, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, nRecordNumber, SQL_DESC_INDICATOR_PTR, (SQLPOINTER) pnIndicator, 0) == SQL_ERROR)
		return SQL_ERROR;
	if (SQLSetDescField_(desc, nRecordNumber, SQL_DESC_DATA_PTR, (SQLPOINTER) pData, 0) == SQL_ERROR)
		return SQL_ERROR;
	return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}
