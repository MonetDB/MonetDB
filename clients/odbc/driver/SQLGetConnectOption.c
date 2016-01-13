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

/**********************************************************************
 * SQLGetConnectOption()
 * CLI Compliance: deprecated in ODBC 3.0 (replaced by SQLGetConnectAttr())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"

#ifdef _MSC_VER
/* can't call them by their real name with Visual Studio 12.0 since we
 * would then get a warning which we translate to an error during
 * compilation (also see ODBC.syms) */
#define SQLGetConnectOption	SQLGetConnectOption_deprecated
#define SQLGetConnectOptionA	SQLGetConnectOptionA_deprecated
#define SQLGetConnectOptionW	SQLGetConnectOptionW_deprecated
#endif

static SQLRETURN
MNDBGetConnectOption(ODBCDbc *dbc,
		     SQLUSMALLINT Option,
		     SQLPOINTER ValuePtr)
{
	SQLLEN v;
	SQLRETURN r;

	/* use mapping as described in ODBC 3 SDK Help file */
	switch (Option) {
		/* connection attributes (ODBC 1 and 2 only) */
	case SQL_ACCESS_MODE:
	case SQL_AUTOCOMMIT:
	case SQL_LOGIN_TIMEOUT:
	case SQL_OPT_TRACE:
	case SQL_PACKET_SIZE:
	case SQL_TRANSLATE_OPTION:
	case SQL_TXN_ISOLATION:
		/* 32 bit integer argument */
		return MNDBGetConnectAttr(dbc, Option, ValuePtr, 0, NULL);
	case SQL_ODBC_CURSORS:
		/* 32 bit integer argument, but SQLGetConnectAttr returns 64 */
		r = MNDBGetConnectAttr(dbc, Option, &v, 0, NULL);
		if (SQL_SUCCEEDED(r))
			WriteData(ValuePtr, (SQLUINTEGER) v, SQLUINTEGER);
		return r;
	case SQL_QUIET_MODE:
		/* 32/64 bit integer argument */
		return MNDBGetConnectAttr(dbc, Option, ValuePtr, 0, NULL);
	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		/* null terminated string argument */
		return MNDBGetConnectAttr(dbc, Option, ValuePtr,
					  SQL_MAX_OPTION_STRING_LENGTH, NULL);
	default:
		/* Invalid attribute/option identifier */
		addDbcError(dbc, "HY092", NULL, 0);
		break;
	}

	return SQL_ERROR;
}

SQLRETURN SQL_API
SQLGetConnectOption(SQLHDBC ConnectionHandle,
		    SQLUSMALLINT Option,
		    SQLPOINTER ValuePtr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetConnectOption " PTRFMT " %s " PTRFMT "\n",
		PTRFMTCAST ConnectionHandle, translateConnectOption(Option),
		PTRFMTCAST ValuePtr);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;
	clearDbcErrors(dbc);

	return MNDBGetConnectOption(dbc, Option, ValuePtr);
}

SQLRETURN SQL_API
SQLGetConnectOptionA(SQLHDBC ConnectionHandle,
		     SQLUSMALLINT Option,
		     SQLPOINTER ValuePtr)
{
	return SQLGetConnectOption(ConnectionHandle, Option, ValuePtr);
}

SQLRETURN SQL_API
SQLGetConnectOptionW(SQLHDBC ConnectionHandle,
		     SQLUSMALLINT Option,
		     SQLPOINTER ValuePtr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;
	SQLRETURN rc;
	SQLPOINTER ptr;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetConnectOptionW " PTRFMT " %s " PTRFMT "\n",
		PTRFMTCAST ConnectionHandle, translateConnectOption(Option),
		PTRFMTCAST ValuePtr);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	switch (Option) {
	/* all string attributes */
	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		ptr = (SQLPOINTER) malloc(SQL_MAX_OPTION_STRING_LENGTH);
		if (ptr == NULL) {
			/* Memory allocation error */
			addDbcError(dbc, "HY001", NULL, 0);
			return SQL_ERROR;
		}
		break;
	default:
		ptr = ValuePtr;
		break;
	}

	rc = MNDBGetConnectOption(dbc, Option, ptr);

	if (ptr != ValuePtr) {
		if (SQL_SUCCEEDED(rc)) {
			SQLSMALLINT n = (SQLSMALLINT) strlen((char *) ptr);
			SQLSMALLINT *nullp = NULL;

			fixWcharOut(rc, ptr, n, ValuePtr,
				    SQL_MAX_OPTION_STRING_LENGTH, nullp, 2,
				    addDbcError, dbc);
		}
		free(ptr);
	}

	return rc;
}
