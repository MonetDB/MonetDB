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
 * SQLGetDescRec()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN
SQLGetDescRec(SQLHDESC DescriptorHandle, SQLSMALLINT RecordNumber,
	      SQLCHAR *Name, SQLSMALLINT BufferLength,
	      SQLSMALLINT *StringLength, SQLSMALLINT *Type,
	      SQLSMALLINT *SubType, SQLINTEGER *Length, SQLSMALLINT *Precision,
	      SQLSMALLINT *Scale, SQLSMALLINT *Nullable)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLGetDescRec\n");
#endif

	(void) DescriptorHandle;	/* Stefan: unused!? */
	(void) RecordNumber;	/* Stefan: unused!? */
	(void) Name;		/* Stefan: unused!? */
	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLength;	/* Stefan: unused!? */
	(void) Type;		/* Stefan: unused!? */
	(void) SubType;		/* Stefan: unused!? */
	(void) Length;		/* Stefan: unused!? */
	(void) Precision;	/* Stefan: unused!? */
	(void) Scale;		/* Stefan: unused!? */
	(void) Nullable;	/* Stefan: unused!? */

	/* We have not implemented Descriptors (yet) */
	/* Hence we can not return an error, such as "IM001: driver not capable". */
	return SQL_INVALID_HANDLE;
}
