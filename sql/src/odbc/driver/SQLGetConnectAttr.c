/*
 * The contents of this file are subject to the MonetDB Public
 * License Version 1.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at 
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.0.html
 * 
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 * 
 * The Original Code is the Monet Database System.
 * 
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-2002 CWI.  
 * All Rights Reserved.
 * 
 * Contributor(s):
 * 		Martin Kersten <Martin.Kersten@cwi.nl>
 * 		Peter Boncz <Peter.Boncz@cwi.nl>
 * 		Niels Nes <Niels.Nes@cwi.nl>
 * 		Stefan Manegold  <Stefan.Manegold@cwi.nl>
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


SQLRETURN SQLGetConnectAttr(
	SQLHDBC		hDbc,
	SQLINTEGER	Attribute,
	SQLPOINTER	ValuePtr,
	SQLINTEGER	BufferLength,
	SQLINTEGER *	StringLength )
{
	ODBCDbc * dbc = (ODBCDbc *) hDbc;

	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLength;	/* Stefan: unused!? */

	if (! isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	/* check input parameters */
	if (ValuePtr == NULL) {
		/* HY009 = Invalid use of null pointer */
		addDbcError(dbc, "HY009", NULL, 0);
		return SQL_ERROR;
	}

	switch (Attribute)
	{
		case SQL_ATTR_ACCESS_MODE:
			ValuePtr = (SQLUINTEGER) SQL_MODE_READ_WRITE;
			break;
		case SQL_ATTR_ASYNC_ENABLE:
			ValuePtr = (SQLUINTEGER) SQL_ASYNC_ENABLE_OFF;
			break;
		case SQL_ATTR_AUTOCOMMIT:
/* TODO: AUTOCOMMIT should be ON by default. This has to be changed including the behavior of SQLExecute(). */
			ValuePtr = (SQLUINTEGER) SQL_AUTOCOMMIT_OFF;
			break;
		case SQL_ATTR_CONNECTION_TIMEOUT:
			ValuePtr = (SQLUINTEGER) 0;	/* no timeout */
			break;
		case SQL_ATTR_LOGIN_TIMEOUT:
			ValuePtr = (SQLUINTEGER) 0;	/* no timeout */
			break;
		case SQL_ATTR_METADATA_ID:
			ValuePtr = (SQLUINTEGER) SQL_FALSE;
			break;
		case SQL_ATTR_ODBC_CURSORS:
			ValuePtr = (SQLUINTEGER) SQL_CUR_USE_IF_NEEDED;
			break;
		case SQL_ATTR_TRACE:
			ValuePtr = (SQLUINTEGER) SQL_OPT_TRACE_OFF;
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
