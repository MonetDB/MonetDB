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
 * SQLCopyDesc()
 * CLI Compliance: ISO 92
 *
 * Note: this function is not supported (yet), it returns an error.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQL_API
SQLCopyDesc(SQLHDESC hSourceDescHandle,
	    SQLHDESC hTargetDescHandle)
{
	ODBCDesc *src = (ODBCDesc *) hSourceDescHandle;
	ODBCDesc *dst = (ODBCDesc *) hTargetDescHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLCopyDesc " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST hSourceDescHandle, PTRFMTCAST hTargetDescHandle);
#endif

	if (!isValidDesc(src))
		return SQL_INVALID_HANDLE;

	if (!isValidDesc(dst))
		return SQL_INVALID_HANDLE;

	if (isIRD(dst)) {
		/* Cannot modify an implementation row descriptor */
		addDescError(src, "HY016", NULL, 0);
		return SQL_ERROR;
	}

	clearDescErrors(src);

	if (isIRD(src)) {
		if (src->Stmt->State == INITED) {
			/* Associated statement is not prepared */
			addDescError(src, "HY007", NULL, 0);
			return SQL_ERROR;
		}
		if (src->Stmt->State == PREPARED0 || src->Stmt->State == EXECUTED0) {
			/* Invalid cursor state */
			addDescError(src, "24000", NULL, 0);
			return SQL_ERROR;
		}
	}

	/* copy sql_desc_count and allocate space for descr. records */
	setODBCDescRecCount(dst, src->sql_desc_count);

	/* don't copy sql_desc_alloc_type */
	dst->sql_desc_array_size = src->sql_desc_array_size;
	dst->sql_desc_array_status_ptr = src->sql_desc_array_status_ptr;
	dst->sql_desc_bind_offset_ptr = src->sql_desc_bind_offset_ptr;
	dst->sql_desc_bind_type = src->sql_desc_bind_type;
	dst->sql_desc_rows_processed_ptr = src->sql_desc_rows_processed_ptr;
	if (src->descRec)
		memcpy(dst->descRec, src->descRec, src->sql_desc_count * sizeof(*src->descRec));

	return SQL_SUCCESS;
}
