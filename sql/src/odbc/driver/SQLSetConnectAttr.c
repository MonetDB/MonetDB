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
 * Author: Martin van Dinther
 * Date  : 30 Aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN
SQLSetConnectAttr_(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
		   SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

	(void) StringLength;	/* Stefan: unused!? */

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	if (ValuePtr == NULL) {
		/* invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	switch (Attribute) {
	case SQL_ATTR_AUTOCOMMIT:
		dbc->autocommit = 1;
		return SQL_SUCCESS;

		/* TODO: implement connection attribute behavior */
	case SQL_ATTR_ACCESS_MODE:
	case SQL_ATTR_CONNECTION_TIMEOUT:
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_DISCONNECT_BEHAVIOR:
	case SQL_ATTR_ENLIST_IN_DTC:
	case SQL_ATTR_ENLIST_IN_XA:
	case SQL_ATTR_LOGIN_TIMEOUT:
	case SQL_ATTR_ODBC_CURSORS:
	case SQL_ATTR_PACKET_SIZE:
	case SQL_ATTR_QUIET_MODE:
	case SQL_ATTR_TRACE:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
	case SQL_ATTR_TRANSLATE_OPTION:
	case SQL_ATTR_TXN_ISOLATION:
		/* set error: Optional feature not implemented */
		addDbcError(dbc, "HYC00", NULL, 0);
		return SQL_ERROR;
	default:
		/* set error: Invalid attribute/option */
		addDbcError(dbc, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}

SQLRETURN
SQLSetConnectAttr(SQLHDBC ConnectionHandle, SQLINTEGER Attribute,
		  SQLPOINTER ValuePtr, SQLINTEGER StringLength)
{
	return SQLSetConnectAttr_(ConnectionHandle, Attribute, ValuePtr,
				  StringLength);
}
