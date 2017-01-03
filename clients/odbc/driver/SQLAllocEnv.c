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
 * SQLAllocEnv()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLAllocHandle())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCEnv.h"

SQLRETURN SQL_API
SQLAllocEnv(SQLHENV *OutputHandlePtr)
{
	SQLRETURN r;

#ifdef ODBCDEBUG
	ODBCLOG("SQLAllocEnv\n");
#endif

	/* use mapping as described in ODBC 3 SDK Help file */
	r = MNDBAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE,
			    (SQLHANDLE *) OutputHandlePtr);
	if (SQL_SUCCEEDED(r)) {
		/* ODBC 3.x applications never call this interface, so
		 * this is an ODBC 2.x application */
		((ODBCEnv *) *OutputHandlePtr)->sql_attr_odbc_version = SQL_OV_ODBC2;
	}
	return r;
}
