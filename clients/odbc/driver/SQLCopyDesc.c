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
 * SQLCopyDesc()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


SQLRETURN SQL_API
SQLCopyDesc(SQLHDESC SourceDescHandle,
	    SQLHDESC TargetDescHandle)
{
	ODBCDesc *src = (ODBCDesc *) SourceDescHandle;
	ODBCDesc *dst = (ODBCDesc *) TargetDescHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLCopyDesc %p %p\n",
		SourceDescHandle, TargetDescHandle);
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
		if (src->Stmt->State == PREPARED0 ||
		    src->Stmt->State == EXECUTED0) {
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
		memcpy(dst->descRec, src->descRec,
		       src->sql_desc_count * sizeof(*src->descRec));

	return SQL_SUCCESS;
}
