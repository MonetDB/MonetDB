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
 * SQLGetConnectAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"


SQLRETURN
SQLGetConnectAttr_(SQLHDBC hDbc, SQLINTEGER Attribute, SQLPOINTER ValuePtr,
		   SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLength;	/* Stefan: unused!? */

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check input parameters */
	if (ValuePtr == NULL) {
		/* HY009 = Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	switch (Attribute) {
	case SQL_ATTR_ACCESS_MODE:
		* (SQLUINTEGER *) ValuePtr = SQL_MODE_READ_WRITE;
		break;
	case SQL_ATTR_ASYNC_ENABLE:
		* (SQLUINTEGER *) ValuePtr = SQL_ASYNC_ENABLE_OFF;
		break;
	case SQL_ATTR_AUTOCOMMIT:
/* TODO: AUTOCOMMIT should be ON by default. This has to be changed including the behavior of SQLExecute(). */
		* (SQLUINTEGER *) ValuePtr = SQL_AUTOCOMMIT_OFF;
		break;
	case SQL_ATTR_CONNECTION_TIMEOUT:
		* (SQLUINTEGER *) ValuePtr = 0;	/* no timeout */
		break;
	case SQL_ATTR_LOGIN_TIMEOUT:
		* (SQLUINTEGER *) ValuePtr = 0;	/* no timeout */
		break;
	case SQL_ATTR_METADATA_ID:
		* (SQLUINTEGER *) ValuePtr = SQL_FALSE;
		break;
	case SQL_ATTR_ODBC_CURSORS:
		* (SQLUINTEGER *) ValuePtr = SQL_CUR_USE_IF_NEEDED;
		break;
	case SQL_ATTR_TRACE:
		* (SQLUINTEGER *) ValuePtr = SQL_OPT_TRACE_OFF;
		break;

/* TODO: implement all the other Connection Attributes */
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_DISCONNECT_BEHAVIOR:
	case SQL_ATTR_ENLIST_IN_DTC:
	case SQL_ATTR_ENLIST_IN_XA:
	case SQL_ATTR_PACKET_SIZE:
	case SQL_ATTR_QUIET_MODE:
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
SQLGetConnectAttr(SQLHDBC hDbc, SQLINTEGER Attribute, SQLPOINTER ValuePtr,
		  SQLINTEGER BufferLength, SQLINTEGER *StringLength)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetConnectAttr\n");
#endif

	return SQLGetConnectAttr_(hDbc, Attribute, ValuePtr, BufferLength,
				  StringLength);
}
