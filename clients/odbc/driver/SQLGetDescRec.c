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
 * SQLGetDescRec()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"
#include "ODBCUtil.h"

static SQLRETURN
SQLGetDescRec_(ODBCDesc *desc,
	       SQLSMALLINT RecordNumber,
	       SQLCHAR *Name,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *StringLength,
	       SQLSMALLINT *Type,
	       SQLSMALLINT *SubType,
	       SQLLEN *Length,
	       SQLSMALLINT *Precision,
	       SQLSMALLINT *Scale,
	       SQLSMALLINT *Nullable)
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

	if (RecordNumber <= 0) {
		/* Invalid descriptor index */
		addDescError(desc, "07009", NULL, 0);
		return SQL_ERROR;
	}

	if (RecordNumber > desc->sql_desc_count)
		return SQL_NO_DATA;

	rec = &desc->descRec[RecordNumber];

	if (Type)
		*Type = rec->sql_desc_type;
	if (SubType)
		*SubType = rec->sql_desc_datetime_interval_code;
	if (Length)
		*Length = rec->sql_desc_octet_length;
	if (Precision)
		*Precision = rec->sql_desc_precision;
	if (Scale)
		*Scale = rec->sql_desc_scale;
	if (Nullable && isID(desc))
		*Nullable = rec->sql_desc_nullable;

	if (isID(desc)) {
		copyString(rec->sql_desc_name, strlen((char *) rec->sql_desc_name), Name, BufferLength, StringLength, SQLSMALLINT, addDescError, desc, return SQL_ERROR);
	}

	return desc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLGetDescRec(SQLHDESC DescriptorHandle,
	      SQLSMALLINT RecordNumber,
	      SQLCHAR *Name,
	      SQLSMALLINT BufferLength,
	      SQLSMALLINT *StringLength,
	      SQLSMALLINT *Type,
	      SQLSMALLINT *SubType,
	      SQLLEN *Length,
	      SQLSMALLINT *Precision,
	      SQLSMALLINT *Scale,
	      SQLSMALLINT *Nullable)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRec " PTRFMT " %d\n",
		PTRFMTCAST DescriptorHandle, (int) RecordNumber);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	return SQLGetDescRec_(desc, RecordNumber, Name, BufferLength, StringLength, Type, SubType, Length, Precision, Scale, Nullable);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLGetDescRecA(SQLHDESC DescriptorHandle,
	       SQLSMALLINT RecordNumber,
	       SQLCHAR *Name,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *StringLength,
	       SQLSMALLINT *Type,
	       SQLSMALLINT *SubType,
	       SQLLEN *Length,
	       SQLSMALLINT *Precision,
	       SQLSMALLINT *Scale,
	       SQLSMALLINT *Nullable)
{
	return SQLGetDescRec(DescriptorHandle, RecordNumber, Name, BufferLength, StringLength, Type, SubType, Length, Precision, Scale, Nullable);
}

SQLRETURN SQL_API
SQLGetDescRecW(SQLHDESC DescriptorHandle,
	       SQLSMALLINT RecordNumber,
	       SQLWCHAR * Name,
	       SQLSMALLINT BufferLength,
	       SQLSMALLINT *StringLength,
	       SQLSMALLINT *Type,
	       SQLSMALLINT *SubType,
	       SQLLEN *Length,
	       SQLSMALLINT *Precision,
	       SQLSMALLINT *Scale,
	       SQLSMALLINT *Nullable)
{
	ODBCDesc *desc = (ODBCDesc *) DescriptorHandle;
	SQLRETURN rc;
	SQLCHAR *name;
	SQLSMALLINT n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRecW " PTRFMT " %d\n",
		PTRFMTCAST DescriptorHandle, (int) RecordNumber);
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	/* dry run: figure out how much data we'll get */
	rc = SQLGetDescRec_(desc, RecordNumber, NULL, 0, &n, Type, SubType, Length, Precision, Scale, Nullable);

	/* get the data */
	name = (SQLCHAR *) malloc(n + 1);
	rc = SQLGetDescRec_(desc, RecordNumber, name, n + 1, &n, Type, SubType, Length, Precision, Scale, Nullable);

	if (SQL_SUCCEEDED(rc)) {
		char *e = ODBCutf82wchar(name, n, Name, BufferLength, &n);

		if (e)
			rc = SQL_ERROR;
		if (StringLength)
			*StringLength = n;
	}
	free(name);

	return rc;
}
#endif /* WITH_WCHAR */
