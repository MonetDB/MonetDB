/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * Author: Martin van Dinther, Sjoerd Mullender
 * Date  : 30 aug 2002
 *
 ********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"

SQLRETURN SQL_API
SQLSetParam(SQLHSTMT StatementHandle,
	    SQLUSMALLINT ParameterNumber,
	    SQLSMALLINT ValueType,
	    SQLSMALLINT ParameterType,
	    SQLULEN LengthPrecision,
	    SQLSMALLINT ParameterScale,
	    SQLPOINTER ParameterValue,
	    SQLLEN *StrLen_or_Ind)
{
#ifdef ODBCDEBUG
	ODBCLOG("SQLSetParam " PTRFMT " %u %s %s " ULENFMT " %d " PTRFMT " " PTRFMT "\n",
		PTRFMTCAST StatementHandle, (unsigned int) ParameterNumber,
		translateCType(ValueType),
		translateSQLType(ParameterType),
		ULENCAST LengthPrecision, (int) ParameterScale,
		PTRFMTCAST ParameterValue, PTRFMTCAST StrLen_or_Ind);
#endif

	/* map this call to SQLBindParameter as described in ODBC 3.0 SDK help */
	return MNDBBindParameter((ODBCStmt *) StatementHandle,
				 ParameterNumber,
				 SQL_PARAM_INPUT_OUTPUT,
				 ValueType,
				 ParameterType,
				 LengthPrecision,
				 ParameterScale,
				 ParameterValue,
				 SQL_SETPARAM_VALUE_MAX,
				 StrLen_or_Ind);
}
