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
 * SQLSetConnectAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"


SQLRETURN
SQLSetConnectAttr_(ODBCDbc *dbc,
		   SQLINTEGER Attribute,
		   SQLPOINTER ValuePtr,
		   SQLINTEGER StringLength)
{
	(void) StringLength;	/* Stefan: unused!? */

	switch (Attribute) {
	case SQL_ATTR_AUTOCOMMIT:
		switch ((SQLUINTEGER) (size_t) ValuePtr) {
		case SQL_AUTOCOMMIT_ON:
		case SQL_AUTOCOMMIT_OFF:
			dbc->sql_attr_autocommit = (SQLUINTEGER) (size_t) ValuePtr;
#ifdef ODBCDEBUG
			ODBCLOG("SQLSetConnectAttr set autocommit %s\n",
				dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON ? "on" : "off");
#endif
			if (dbc->mid)
				mapi_setAutocommit(dbc->mid, dbc->sql_attr_autocommit == SQL_AUTOCOMMIT_ON);
			break;
		default:
			/* Invalid attribute value */
			addDbcError(dbc, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		return SQL_SUCCESS;
	case SQL_ATTR_METADATA_ID:
		switch ((SQLUINTEGER) (size_t) ValuePtr) {
		case SQL_TRUE:
		case SQL_FALSE:
			dbc->sql_attr_metadata_id = (SQLUINTEGER) (size_t) ValuePtr;
#ifdef ODBCDEBUG
			ODBCLOG("SQLSetConnectAttr set metadata_id %s\n",
				dbc->sql_attr_metadata_id == SQL_TRUE ? "true" : "false");
#endif
			break;
		default:
			/* Invalid attribute value */
			addDbcError(dbc, "HY024", NULL, 0);
			return SQL_ERROR;
		}
		return SQL_SUCCESS;

		/* TODO: implement connection attribute behavior */
	case SQL_ATTR_ACCESS_MODE:
	case SQL_ATTR_ASYNC_ENABLE:
	case SQL_ATTR_CONNECTION_TIMEOUT:
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_LOGIN_TIMEOUT:
	case SQL_ATTR_ODBC_CURSORS:
	case SQL_ATTR_PACKET_SIZE:
	case SQL_ATTR_QUIET_MODE:
	case SQL_ATTR_TRACE:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
	case SQL_ATTR_TRANSLATE_OPTION:
	case SQL_ATTR_TXN_ISOLATION:
		/* Optional feature not implemented */
		addDbcError(dbc, "HYC00", NULL, 0);
		return SQL_ERROR;
	case SQL_ATTR_AUTO_IPD:	/* read-only attribute */
	case SQL_ATTR_CONNECTION_DEAD:
	default:
		/* Invalid attribute/option identifier */
		addDbcError(dbc, "HY092", NULL, 0);
		break;
	}

	return SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLSetConnectAttr(SQLHDBC ConnectionHandle,
		  SQLINTEGER Attribute,
		  SQLPOINTER ValuePtr,
		  SQLINTEGER StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetConnectAttr " PTRFMT " %d\n",
		PTRFMTCAST ConnectionHandle, (int) Attribute);
#endif

	if (!isValidDbc((ODBCDbc *) ConnectionHandle))
		return SQL_INVALID_HANDLE;

	clearDbcErrors((ODBCDbc *) ConnectionHandle);

	return SQLSetConnectAttr_((ODBCDbc *) ConnectionHandle,
				  Attribute,
				  ValuePtr,
				  StringLength);
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLSetConnectAttrA(SQLHDBC ConnectionHandle,
		   SQLINTEGER Attribute,
		   SQLPOINTER ValuePtr,
		   SQLINTEGER StringLength)
{
	return SQLSetConnectAttr(ConnectionHandle,
				 Attribute,
				 ValuePtr,
				 StringLength);
}

SQLRETURN SQL_API
SQLSetConnectAttrW(SQLHDBC ConnectionHandle,
		   SQLINTEGER Attribute,
		   SQLPOINTER ValuePtr,
		   SQLINTEGER StringLength)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;
	SQLPOINTER ptr;
	SQLINTEGER n;
	SQLRETURN rc;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetConnectAttrW " PTRFMT " %d\n",
		PTRFMTCAST ConnectionHandle, (int) Attribute);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	switch (Attribute) {
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
		if (StringLength > 0)	/* convert from bytes to characters */
			StringLength /= 2;
		fixWcharIn(ValuePtr, StringLength, SQLCHAR, ptr,
			   addDbcError, dbc, return SQL_ERROR);
		n = SQL_NTS;
		break;
	default:
		ptr = ValuePtr;
		n = StringLength;
		break;
	}

	rc = SQLSetConnectAttr_(dbc, Attribute, ptr, n);

	if (ptr && ptr != ValuePtr)
		free(ptr);

	return rc;
}
#endif /* WITH_WCHAR */
