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


SQLRETURN
SQLCopyDesc(SQLHDESC hSourceDescHandle, SQLHDESC hTargetDescHandle)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLCopyDesc\n");
#endif

	if (hSourceDescHandle == SQL_NULL_HANDLE)
		return SQL_INVALID_HANDLE;

	if (hTargetDescHandle == SQL_NULL_HANDLE)
		return SQL_INVALID_HANDLE;


	/* TODO: implement this function and corresponding behavior */

	/* can not set an error msg (do not have descriptor handles yet */
	/* just return SQL_ERRO */
	return SQL_ERROR;
}
