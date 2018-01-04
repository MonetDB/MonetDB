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
 * SQLGetDescRec()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
MNDBGetDescRec(ODBCDesc *desc,
	       SQLSMALLINT RecNumber,
	       SQLCHAR *Name,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *StringLengthPtr,
	       SQLSMALLINT *TypePtr,
	       SQLSMALLINT *SubTypePtr,
	       SQLLEN *LengthPtr,
	       SQLSMALLINT *PrecisionPtr,
	       SQLSMALLINT *ScalePtr,
	       SQLSMALLINT *NullablePtr)
{
	ODBCDescRec *rec;

	if (isIRD(desc)) {
		if (desc->Stmt->State == INITED) {
			/* Function sequence error */
			addDescError(desc, "HY010", NULL, 0);
			return SQL_ERROR;
		}
		if (desc->Stmt->State == EXECUTED0) {
			/* Invalid cursor state */
			addDescError(desc, "24000", NULL, 0);
			return SQL_ERROR;
		}
		if (desc->Stmt->State == PREPARED0)
			return SQL_NO_DATA;
	}

	if (RecNumber <= 0) {
		/* Invalid descriptor index */
		addDescError(desc, "07009", NULL, 0);
		return SQL_ERROR;
	}

	if (RecNumber > desc->sql_desc_count)
		return SQL_NO_DATA;

	rec = &desc->descRec[RecNumber];

	if (TypePtr)
		*TypePtr = rec->sql_desc_type;
	if (SubTypePtr)
		*SubTypePtr = rec->sql_desc_datetime_interval_code;
	if (LengthPtr)
		*LengthPtr = rec->sql_desc_octet_length;
	if (PrecisionPtr)
		*PrecisionPtr = rec->sql_desc_precision;
	if (ScalePtr)
		*ScalePtr = rec->sql_desc_scale;
	if (NullablePtr && isID(desc))
		*NullablePtr = rec->sql_desc_nullable;

	if (isID(desc)) {
		copyString(rec->sql_desc_name,
			   strlen((char *) rec->sql_desc_name), Name,
			   BufferLength, StringLengthPtr, SQLSMALLINT,
			   addDescError, desc, return SQL_ERROR);
	}

	return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLGetDescRec(SQLHDESC DescriptorHandle,
	      SQLSMALLINT RecNumber,
	      SQLCHAR *Name,
	      SQLSMALLINT BufferLength,
	      SQLSMALLINT *StringLengthPtr,
	      SQLSMALLINT *TypePtr,
	      SQLSMALLINT *SubTypePtr,
	      SQLLEN *LengthPtr,
	      SQLSMALLINT *PrecisionPtr,
	      SQLSMALLINT *ScalePtr,
	      SQLSMALLINT *NullablePtr)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRec " PTRFMT " %d " PTRFMT " %d " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST DescriptorHandle, (int) RecNumber, PTRFMTCAST Name,
		(int) BufferLength, PTRFMTCAST StringLengthPtr,
		PTRFMTCAST TypePtr, PTRFMTCAST SubTypePtr,
		PTRFMTCAST LengthPtr, PTRFMTCAST PrecisionPtr,
		PTRFMTCAST ScalePtr, PTRFMTCAST NullablePtr);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	return MNDBGetDescRec(desc,
			      RecNumber,
			      Name,
			      BufferLength,
			      StringLengthPtr,
			      TypePtr,
			      SubTypePtr,
			      LengthPtr,
			      PrecisionPtr,
			      ScalePtr,
			      NullablePtr);
}

SQLRETURN SQL_API
SQLGetDescRecA(SQLHDESC DescriptorHandle,
	       SQLSMALLINT RecNumber,
	       SQLCHAR *Name,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *StringLengthPtr,
	       SQLSMALLINT *TypePtr,
	       SQLSMALLINT *SubTypePtr,
	       SQLLEN *LengthPtr,
	       SQLSMALLINT *PrecisionPtr,
	       SQLSMALLINT *ScalePtr,
	       SQLSMALLINT *NullablePtr)
{
	return SQLGetDescRec(DescriptorHandle,
			     RecNumber,
			     Name,
			     BufferLength,
			     StringLengthPtr,
			     TypePtr,
			     SubTypePtr,
			     LengthPtr,
			     PrecisionPtr,
			     ScalePtr,
			     NullablePtr);
}

SQLRETURN SQL_API
SQLGetDescRecW(SQLHDESC DescriptorHandle,
	       SQLSMALLINT RecNumber,
	       SQLWCHAR *Name,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *StringLengthPtr,
	       SQLSMALLINT *TypePtr,
	       SQLSMALLINT *SubTypePtr,
	       SQLLEN *LengthPtr,
	       SQLSMALLINT *PrecisionPtr,
	       SQLSMALLINT *ScalePtr,
	       SQLSMALLINT *NullablePtr)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;
	SQLRETURN rc;
	SQLCHAR *name;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRecW " PTRFMT " %d " PTRFMT " %d " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST DescriptorHandle, (int) RecNumber, PTRFMTCAST Name,
		(int) BufferLength, PTRFMTCAST StringLengthPtr,
		PTRFMTCAST TypePtr, PTRFMTCAST SubTypePtr,
		PTRFMTCAST LengthPtr, PTRFMTCAST PrecisionPtr,
		PTRFMTCAST ScalePtr, PTRFMTCAST NullablePtr);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	/* dry run: figure out how much data we'll get */
	rc = MNDBGetDescRec(desc, RecNumber, NULL, 0, &n, TypePtr, SubTypePtr,
			    LengthPtr, PrecisionPtr, ScalePtr, NullablePtr);

	/* get the data */
	name = (SQLCHAR *) malloc(n + 1);
	if (name == NULL) {
		/* Memory allocation error */
		addDescError(desc, "HY001", NULL, 0);
		return SQL_ERROR;
	}
	rc = MNDBGetDescRec(desc, RecNumber, name, n + 1, &n, TypePtr,
			    SubTypePtr, LengthPtr, PrecisionPtr, ScalePtr,
			    NullablePtr);

	if (SQL_SUCCEEDED(rc)) {
		char *e = ODBCutf82wchar(name, n, Name, BufferLength, &n);

		if (e)
			rc = SQL_ERROR;
		if (StringLengthPtr)
			*StringLengthPtr = n;
	}
	free(name);

	return rc;
}
