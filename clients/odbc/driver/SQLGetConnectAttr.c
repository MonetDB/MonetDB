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
 * SQLGetConnectAttr()
 * CLI Compliance: ISO 92
 *
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCDbc.h"
#include "ODBCUtil.h"


SQLRETURN
MNDBGetConnectAttr(ODBCDbc *dbc,
		   SQLINTEGER Attribute,
		   SQLPOINTER ValuePtr,
		   SQLINTEGER BufferLength,
		   SQLINTEGER *StringLengthPtr)
{
	/* check input parameters */
	if (ValuePtr == NULL) {
		/* Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	switch (Attribute) {
	case SQL_ATTR_ACCESS_MODE:		/* SQLUINTEGER */
		/* SQL_ACCESS_MODE */
		WriteData(ValuePtr, SQL_MODE_READ_WRITE, SQLUINTEGER);
		break;
	case SQL_ATTR_ASYNC_ENABLE:		/* SQLULEN */
		WriteData(ValuePtr, SQL_ASYNC_ENABLE_OFF, SQLULEN);
		break;
	case SQL_ATTR_AUTO_IPD:			/* SQLUINTEGER */
		/* TODO implement automatic filling of IPD See also
		 * SQLSetStmtAttr.c for SQL_ATTR_ENABLE_AUTO_IPD */
		WriteData(ValuePtr, SQL_FALSE, SQLUINTEGER);
		break;
	case SQL_ATTR_AUTOCOMMIT:		/* SQLUINTEGER */
		/* SQL_AUTOCOMMIT */
		WriteData(ValuePtr, dbc->sql_attr_autocommit, SQLUINTEGER);
		break;
	case SQL_ATTR_CONNECTION_DEAD:		/* SQLUINTEGER */
		WriteData(ValuePtr, dbc->mid && mapi_is_connected(dbc->mid) ? SQL_CD_FALSE : SQL_CD_TRUE, SQLUINTEGER);
		break;
	case SQL_ATTR_CONNECTION_TIMEOUT:	/* SQLUINTEGER */
		WriteData(ValuePtr, dbc->sql_attr_connection_timeout, SQLUINTEGER);
		break;
	case SQL_ATTR_LOGIN_TIMEOUT:		/* SQLUINTEGER */
		/* SQL_LOGIN_TIMEOUT */
		WriteData(ValuePtr, 0, SQLUINTEGER);	/* no timeout */
		break;
	case SQL_ATTR_METADATA_ID:		/* SQLUINTEGER */
		WriteData(ValuePtr, dbc->sql_attr_metadata_id, SQLUINTEGER);
		break;
	case SQL_ATTR_ODBC_CURSORS:		/* SQLULEN */
		/* SQL_ODBC_CURSORS */
		WriteData(ValuePtr, SQL_CUR_USE_DRIVER, SQLULEN);
		break;
	case SQL_ATTR_TRACE:			/* SQLUINTEGER */
		/* SQL_OPT_TRACE */
		WriteData(ValuePtr, SQL_OPT_TRACE_OFF, SQLUINTEGER);
		break;
	case SQL_ATTR_CURRENT_CATALOG:		/* SQLCHAR* */
		/* SQL_CURRENT_QUALIFIER */
		copyString(dbc->dbname, strlen(dbc->dbname), ValuePtr,
			   BufferLength, StringLengthPtr, SQLINTEGER,
			   addDbcError, dbc, return SQL_ERROR);
		break;
	case SQL_ATTR_TXN_ISOLATION:		/* SQLUINTEGER */
		/* SQL_TXN_ISOLATION */
		WriteData(ValuePtr, SQL_TXN_SERIALIZABLE, SQLUINTEGER);
		break;

/* TODO: implement all the other Connection Attributes */
#ifdef SQL_ATTR_ASYNC_DBC_EVENT
	case SQL_ATTR_ASYNC_DBC_EVENT:		/* SQLPOINTER */
#endif
#ifdef SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE
	case SQL_ATTR_ASYNC_DBC_FUNCTIONS_ENABLE: /* SQLUINTEGER */
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCALLBACK
	case SQL_ATTR_ASYNC_DBC_PCALLBACK:	/* SQLPOINTER */
#endif
#ifdef SQL_ATTR_ASYNC_DBC_PCONTEXT
	case SQL_ATTR_ASYNC_DBC_PCONTEXT:	/* SQLPOINTER */
#endif
#ifdef SQL_ATTR_DBC_INFO_TOKEN
	case SQL_ATTR_DBC_INFO_TOKEN:		/* SQLPOINTER */
#endif
	case SQL_ATTR_DISCONNECT_BEHAVIOR:
	case SQL_ATTR_ENLIST_IN_DTC:		/* SQLPOINTER */
	case SQL_ATTR_ENLIST_IN_XA:
	case SQL_ATTR_PACKET_SIZE:		/* SQLUINTEGER */
		/* SQL_PACKET_SIZE */
	case SQL_ATTR_QUIET_MODE:		/* HWND (SQLPOINTER) */
		/* SQL_QUIET_MODE */
	case SQL_ATTR_TRACEFILE:		/* SQLCHAR* */
		/* SQL_OPT_TRACEFILE */
	case SQL_ATTR_TRANSLATE_LIB:		/* SQLCHAR* */
		/* SQL_TRANSLATE_DLL */
	case SQL_ATTR_TRANSLATE_OPTION:		/* SQLUINTEGER */
		/* SQL_TRANSLATE_OPTION */
		/* Optional feature not implemented */
		addDbcError(dbc, "HYC00", NULL, 0);
		return SQL_ERROR;
	default:
		/* Invalid attribute/option identifier */
		addDbcError(dbc, "HY092", NULL, 0);
		return SQL_ERROR;
	}

	return dbc->Error ? SQL_SUCCESS_WITH_INFO : SQL_SUCCESS;
}

SQLRETURN SQL_API
SQLGetConnectAttr(SQLHDBC ConnectionHandle,
		  SQLINTEGER Attribute,
		  SQLPOINTER ValuePtr,
		  SQLINTEGER BufferLength,
		  SQLINTEGER *StringLengthPtr)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetConnectAttr " PTRFMT " %s " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST ConnectionHandle,
		translateConnectAttribute(Attribute),
		PTRFMTCAST ValuePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr);
#endif

	if (!isValidDbc((ODBCDbc *) ConnectionHandle))
		return SQL_INVALID_HANDLE;

	clearDbcErrors((ODBCDbc *) ConnectionHandle);

	return MNDBGetConnectAttr((ODBCDbc *) ConnectionHandle,
				  Attribute,
				  ValuePtr,
				  BufferLength,
				  StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetConnectAttrA(SQLHDBC ConnectionHandle,
		   SQLINTEGER Attribute,
		   SQLPOINTER ValuePtr,
		   SQLINTEGER BufferLength,
		   SQLINTEGER *StringLengthPtr)
{
	return SQLGetConnectAttr(ConnectionHandle,
				 Attribute,
				 ValuePtr,
				 BufferLength,
				 StringLengthPtr);
}

SQLRETURN SQL_API
SQLGetConnectAttrW(SQLHDBC ConnectionHandle,
		   SQLINTEGER Attribute,
		   SQLPOINTER ValuePtr,
		   SQLINTEGER BufferLength,
		   SQLINTEGER *StringLengthPtr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;
	SQLRETURN rc;
	SQLPOINTER ptr;
	SQLINTEGER n;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetConnectAttrW " PTRFMT " %s " PTRFMT " %d " PTRFMT "\n",
		PTRFMTCAST ConnectionHandle,
		translateConnectAttribute(Attribute),
		PTRFMTCAST ValuePtr, (int) BufferLength,
		PTRFMTCAST StringLengthPtr);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	switch (Attribute) {
	/* all string attributes */
	case SQL_ATTR_CURRENT_CATALOG:
		ptr = malloc(BufferLength);
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

	rc = MNDBGetConnectAttr(dbc, Attribute, ptr, BufferLength, &n);

	if (ptr != ValuePtr) {
		if (rc == SQL_SUCCESS_WITH_INFO) {
			clearDbcErrors(dbc);
			free(ptr);
			ptr = malloc(++n); /* add one for NULL byte */
			if (ptr == NULL) {
				/* Memory allocation error */
				addDbcError(dbc, "HY001", NULL, 0);
				return SQL_ERROR;
			}
			rc = MNDBGetConnectAttr(dbc, Attribute, ptr, n, &n);
		}
		if (SQL_SUCCEEDED(rc)) {
			SQLSMALLINT nn = (SQLSMALLINT) n;

			fixWcharOut(rc, ptr, nn, ValuePtr, BufferLength,
				    StringLengthPtr, 2, addDbcError, dbc);
		}
		free(ptr);
	} else if (StringLengthPtr)
		*StringLengthPtr = n;

	return rc;
}
