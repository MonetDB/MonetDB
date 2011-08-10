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

static SQLRETURN
SQLSetConnectOption_(ODBCDbc *dbc,
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
		return SQLSetConnectAttr_(dbc, Option,
					  (SQLPOINTER) (size_t) ValuePtr, 0);
	case SQL_QUIET_MODE:
		/* 32/64 bit integer argument */
		return SQLSetConnectAttr_(dbc, Option,
					  (SQLPOINTER) (size_t) ValuePtr, 0);

	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		/* null terminated string argument */
		return SQLSetConnectAttr_(dbc, Option,
					  (SQLPOINTER) (size_t) ValuePtr,
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
	ODBCLOG("SQLSetConnectOption " PTRFMT " %u " ULENFMT "\n",
		PTRFMTCAST ConnectionHandle, (unsigned int) Option,
		ULENCAST ValuePtr);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	return SQLSetConnectOption_(dbc, Option, ValuePtr);
}

#ifdef WITH_WCHAR
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
	SQLPOINTER ptr = (SQLPOINTER) (size_t) ValuePtr;
	SQLULEN p;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetConnectOptionW " PTRFMT " %u " ULENFMT "\n",
		PTRFMTCAST ConnectionHandle, (unsigned int) Option,
		ULENCAST ValuePtr);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	switch (Option) {
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
		fixWcharIn((SQLPOINTER) (size_t) ValuePtr, SQL_NTS, SQLCHAR,
			   ptr, addDbcError, dbc, return SQL_ERROR);
		p = (SQLULEN) (size_t) ptr;
		break;
	default:
		p = ValuePtr;
		break;
	}

	rc = SQLSetConnectOption_(dbc, Option, p);

	if (ptr &&p != ValuePtr)
		free(ptr);

	return rc;
}
#endif /* WITH_WCHAR */
