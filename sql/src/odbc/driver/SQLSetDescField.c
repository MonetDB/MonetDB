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
 * SQLSetDescField()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN
SQLSetDescField(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
		SQLSMALLINT FieldIdentifier, SQLPOINTER Value,
		SQLINTEGER BufferLength)
{
	(void) DescriptorHandle;	/* Stefan: unused!? */
	(void) RecordNumber;	/* Stefan: unused!? */
	(void) FieldIdentifier;	/* Stefan: unused!? */
	(void) Value;		/* Stefan: unused!? */
	(void) BufferLength;	/* Stefan: unused!? */

	/* no Descriptors supported (yet) */
	/* no Descriptor handle support, so not possible to set an error */
	/* so return Invalid Handle */
	return SQL_INVALID_HANDLE;
}
