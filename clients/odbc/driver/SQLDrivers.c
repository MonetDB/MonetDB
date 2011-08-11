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

#include "ODBCGlobal.h"
#include "ODBCEnv.h"

SQLRETURN SQL_API
SQLDrivers(SQLHENV EnvironmentHandle,
	   SQLUSMALLINT Direction,
	   SQLCHAR *DriverDescription,
	   SQLSMALLINT BufferLength1,
	   SQLSMALLINT *DescriptionLengthPtr,
	   SQLCHAR *DriverAttributes,
	   SQLSMALLINT BufferLength2,
	   SQLSMALLINT *AttributesLengthPtr)
{
	(void) Direction;
	(void) DriverDescription;
	(void) BufferLength1;
	(void) DescriptionLengthPtr;
	(void) DriverAttributes;
	(void) BufferLength2;
	(void) AttributesLengthPtr;
	addEnvError((ODBCEnv *) EnvironmentHandle, "HY000",
		    "Driver Manager only function", 0);
	return SQL_ERROR;
}

#ifdef WITH_WCHAR
SQLRETURN SQL_API
SQLDriversA(SQLHENV EnvironmentHandle,
	    SQLUSMALLINT Direction,
	    SQLCHAR *DriverDescription,
	    SQLSMALLINT BufferLength1,
	    SQLSMALLINT *DescriptionLengthPtr,
	    SQLCHAR *DriverAttributes,
	    SQLSMALLINT BufferLength2,
	    SQLSMALLINT *AttributesLengthPtr)
{
	return SQLDrivers(EnvironmentHandle,
			  Direction,
			  DriverDescription,
			  BufferLength1,
			  DescriptionLengthPtr,
			  DriverAttributes,
			  BufferLength2,
			  AttributesLengthPtr);
}

SQLRETURN SQL_API
SQLDriversW(SQLHENV EnvironmentHandle,
	    SQLUSMALLINT Direction,
	    SQLWCHAR *DriverDescription,
	    SQLSMALLINT BufferLength1,
	    SQLSMALLINT *DescriptionLengthPtr,
	    SQLWCHAR *DriverAttributes,
	    SQLSMALLINT BufferLength2,
	    SQLSMALLINT *AttributesLengthPtr)
{
	(void) Direction;
	(void) DriverDescription;
	(void) BufferLength1;
	(void) DescriptionLengthPtr;
	(void) DriverAttributes;
	(void) BufferLength2;
	(void) AttributesLengthPtr;
	addEnvError((ODBCEnv *) EnvironmentHandle, "HY000",
		    "Driver Manager only function", 0);
	return SQL_ERROR;
}
#endif /* WITH_WCHAR */
