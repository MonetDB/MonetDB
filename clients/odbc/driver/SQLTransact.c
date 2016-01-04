/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
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

/********************************************************************
 * SQLTransact()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLEndTran())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN SQL_API
SQLTransact(SQLHENV EnvironmentHandle,
	    SQLHDBC ConnectionHandle,
	    UWORD CompletionType)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLTransact " PTRFMT " " PTRFMT " %s\n",
		PTRFMTCAST EnvironmentHandle, PTRFMTCAST ConnectionHandle,
		translateCompletionType(CompletionType));
#endif

	/* use mapping as described in ODBC 3 SDK Help */
	if (ConnectionHandle != SQL_NULL_HDBC)
		return MNDBEndTran(SQL_HANDLE_DBC,
				   ConnectionHandle,
				   CompletionType);
	else
		return MNDBEndTran(SQL_HANDLE_ENV,
				   EnvironmentHandle,
				   CompletionType);
}
