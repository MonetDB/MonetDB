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

SQLRETURN
SQLSetParam(SQLHSTMT hStmt, SQLUSMALLINT ParameterNumber,
	    SQLSMALLINT ValueType, SQLSMALLINT ParameterType,
	    SQLUINTEGER LengthPrecision, SQLSMALLINT ParameterScale,
	    SQLPOINTER ParameterValue, SQLINTEGER *StrLen_or_Ind)
{
	/* map this call to SQLBindParameter as described in ODBC 3.0 SDK help */
	return SQLBindParameter_(hStmt, ParameterNumber,
				 SQL_PARAM_INPUT_OUTPUT, ValueType,
				 ParameterType, LengthPrecision,
				 ParameterScale, ParameterValue,
				 SQL_SETPARAM_VALUE_MAX, StrLen_or_Ind);
}
