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
 * SQLSetConnectOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLSetConnectAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"

#ifdef _MSC_VER
/* can't call them by their real name with Visual Studio 12.0 since we
 * would then get a warning which we translate to an error during
 * compilation (also see ODBC.syms) */
#define SQLSetConnectOption	SQLSetConnectOption_deprecated
#define SQLSetConnectOptionA	SQLSetConnectOptionA_deprecated
#define SQLSetConnectOptionW	SQLSetConnectOptionW_deprecated
#endif

static SQLRETURN
MNDBSetConnectOption(ODBCDbc *dbc,
		     SQLUSMALLINT Option,
		     SQLULEN ValuePtr)
{
	/* use mapping as described in ODBC 3 SDK Help file */
	switch (Option) {
	/* connection attributes (ODBC 1 and 2 only) */
	case SQL_ACCESS_MODE:
	case SQL_AUTOCOMMIT:
	case SQL_LOGIN_TIMEOUT:
	case SQL_ODBC_CURSORS:
	case SQL_OPT_TRACE:
	case SQL_PACKET_SIZE:
	case SQL_TRANSLATE_OPTION:
	case SQL_TXN_ISOLATION:
		/* 32 bit integer argument */
		return MNDBSetConnectAttr(dbc, Option,
					  (SQLPOINTER) (uintptr_t) ValuePtr, 0);
	case SQL_QUIET_MODE:
		/* 32/64 bit integer argument */
		return MNDBSetConnectAttr(dbc, Option,
					  (SQLPOINTER) (uintptr_t) ValuePtr, 0);

	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		/* null terminated string argument */
		return MNDBSetConnectAttr(dbc, Option,
					  (SQLPOINTER) (uintptr_t) ValuePtr,
					  SQL_NTS);

	default:
		/* other options (e.g. ODBC 3) are NOT valid */
		/* Invalid attribute/option identifier */
		addDbcError(dbc, "HY092", NULL, 0);
		break;
	}

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLSetConnectOption(SQLHDBC ConnectionHandle,
		    SQLUSMALLINT Option,
		    SQLULEN ValuePtr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetConnectOption " PTRFMT " %s " ULENFMT "\n",
		PTRFMTCAST ConnectionHandle, translateConnectOption(Option),
		ULENCAST ValuePtr);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return MNDBSetConnectOption(dbc, Option, ValuePtr);
}

SQLRETURN SQL_API
SQLSetConnectOptionA(SQLHDBC ConnectionHandle,
		     SQLUSMALLINT Option,
		     SQLULEN ValuePtr)
{
	return SQLSetConnectOption(ConnectionHandle, Option, ValuePtr);
}

SQLRETURN SQL_API
SQLSetConnectOptionW(SQLHDBC ConnectionHandle,
		     SQLUSMALLINT Option,
		     SQLULEN ValuePtr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;
	SQLPOINTER ptr = (SQLPOINTER) (uintptr_t) ValuePtr;
	SQLULEN p;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetConnectOptionW " PTRFMT " %s " ULENFMT "\n",
		PTRFMTCAST ConnectionHandle, translateConnectOption(Option),
		ULENCAST ValuePtr);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	switch (Option) {
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
		fixWcharIn((SQLPOINTER) (uintptr_t) ValuePtr, SQL_NTS, SQLCHAR,
			   ptr, addDbcError, dbc, return SQL_ERROR);
		p = (SQLULEN) (uintptr_t) ptr;
		break;
	default:
		p = ValuePtr;
		break;
	}

	rc = MNDBSetConnectOption(dbc, Option, p);

	if (ptr &&p != ValuePtr)
		free(ptr);

	return rc;
}
