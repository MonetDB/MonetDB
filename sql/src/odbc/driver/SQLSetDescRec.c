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
 * SQLSetDescRec()
 * CLI Compliance: IOS 92
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN
SQLSetDescRec(SQLHDESC hDescriptorHandle, SQLSMALLINT nRecordNumber,
	      SQLSMALLINT nType, SQLSMALLINT nSubType, SQLINTEGER nLength,
	      SQLSMALLINT nPrecision, SQLSMALLINT nScale, SQLPOINTER pData,
	      SQLINTEGER *pnStringLength, SQLINTEGER *pnIndicator)
{
	ODBCDesc *desc = (ODBCDesc *) hDescriptorHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLSetDescRec\n");
#endif

	if (!isValidDesc(desc))
		return SQL_INVALID_HANDLE;

	(void) nRecordNumber;	/* Stefan: unused!? */
	(void) nType;		/* Stefan: unused!? */
	(void) nSubType;	/* Stefan: unused!? */
	(void) nLength;		/* Stefan: unused!? */
	(void) nPrecision;	/* Stefan: unused!? */
	(void) nScale;		/* Stefan: unused!? */
	(void) pData;		/* Stefan: unused!? */
	(void) pnStringLength;	/* Stefan: unused!? */
	(void) pnIndicator;	/* Stefan: unused!? */

	/* IM001: driver not capable */
	addDescError(desc, "IM001", NULL, 0);
	return SQL_ERROR;
}
