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
 * SQLGetDescRec()
 * CLI Compliance: ISO 92
 **********************************************************************/

#include "ODBCGlobal.h"

SQLRETURN SQLGetDescRec(
	SQLHDESC	DescriptorHandle,
	SQLSMALLINT	RecordNumber,
	SQLCHAR *	Name,
	SQLSMALLINT	BufferLength,
	SQLSMALLINT *	StringLength,
	SQLSMALLINT *	Type,
	SQLSMALLINT *	SubType,
	SQLINTEGER *	Length,
	SQLSMALLINT *	Precision,
	SQLSMALLINT *	Scale,
	SQLSMALLINT *	Nullable )
{
	(void) DescriptorHandle;	/* Stefan: unused!? */
	(void) RecordNumber;	/* Stefan: unused!? */
	(void) Name;	/* Stefan: unused!? */
	(void) BufferLength;	/* Stefan: unused!? */
	(void) StringLength;	/* Stefan: unused!? */
	(void) Type;	/* Stefan: unused!? */
	(void) SubType;	/* Stefan: unused!? */
	(void) Length;	/* Stefan: unused!? */
	(void) Precision;	/* Stefan: unused!? */
	(void) Scale;	/* Stefan: unused!? */
	(void) Nullable;	/* Stefan: unused!? */

	/* We have not implemented Descriptors (yet) */
	/* Hence we can not return an error, such as "IM001: driver not capable". */
	return SQL_INVALID_HANDLE;
}
