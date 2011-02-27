/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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

/********************************************************************
 * SQLSetParam()
 * CLI Compliance: deprecated in ODCB 2.0 (replaced by SQLBindParameter())
 * Provided here for old (pre ODBC 3.0) applications and driver managers.
 *
 * Author: Martin van Dinther
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLSetParam(SQLHSTMT hStmt,
	    SQLUSMALLINT ParameterNumber,
	    SQLSMALLINT ValueType,
	    SQLSMALLINT ParameterType,
	    SQLULEN LengthPrecision,
	    SQLSMALLINT ParameterScale,
	    SQLPOINTER ParameterValue,
	    SQLLEN *StrLen_or_Ind)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetParam " PTRFMT " %u %d %d " ULENFMT " %d\n", PTRFMTCAST hStmt, (unsigned int) ParameterNumber, (int) ValueType, (int) ParameterType, ULENCAST LengthPrecision, (int) ParameterScale);
#endif

	/* map this call to SQLBindParameter as described in ODBC 3.0 SDK help */
	return SQLBindParameter_((ODBCStmt *) hStmt, ParameterNumber, SQL_PARAM_INPUT_OUTPUT, ValueType, ParameterType, LengthPrecision, ParameterScale, ParameterValue, SQL_SETPARAM_VALUE_MAX, StrLen_or_Ind);
}
