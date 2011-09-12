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
 * SQLGetFunctions()
 * CLI Compliance: ISO 92
 *
 * Author: Sjoerd Mullender
 * Date  : 4 sep 2003
 *
 **********************************************************************/

#include "ODBCGlobal.h"
#include "ODBCStmt.h"


/* this table contains all functions for which SQLGetFunctions should
   return SQL_TRUE */
static UWORD FuncImplemented[] = {
	SQL_API_SQLALLOCCONNECT,
	SQL_API_SQLALLOCENV,
#ifdef SQL_API_SQLALLOCHANDLE
	SQL_API_SQLALLOCHANDLE,
#endif
	SQL_API_SQLALLOCSTMT,
	SQL_API_SQLBINDCOL,
	SQL_API_SQLBINDPARAMETER,
	SQL_API_SQLBROWSECONNECT,
	SQL_API_SQLCANCEL,
#ifdef SQL_API_SQLCLOSECURSOR
	SQL_API_SQLCLOSECURSOR,
#endif
#ifdef SQL_API_SQLCOLATTRIBUTE
	SQL_API_SQLCOLATTRIBUTE,	/* == SQL_API_SQLCOLATTRIBUTES */
#endif
	SQL_API_SQLCOLATTRIBUTES,
	SQL_API_SQLCOLUMNPRIVILEGES,
	SQL_API_SQLCOLUMNS,
	SQL_API_SQLCONNECT,
#ifdef SQL_API_SQLCOPYDESC
	SQL_API_SQLCOPYDESC,
#endif
	SQL_API_SQLDESCRIBECOL,
	SQL_API_SQLDESCRIBEPARAM,
	SQL_API_SQLDISCONNECT,
	SQL_API_SQLDRIVERCONNECT,
#ifdef SQL_API_SQLENDTRAN
	SQL_API_SQLENDTRAN,
#endif
	SQL_API_SQLERROR,
	SQL_API_SQLEXECDIRECT,
	SQL_API_SQLEXECUTE,
	SQL_API_SQLEXTENDEDFETCH,
	SQL_API_SQLFETCH,
#ifdef SQL_API_SQLFETCHSCROLL
	SQL_API_SQLFETCHSCROLL,
#endif
	SQL_API_SQLFOREIGNKEYS,
	SQL_API_SQLFREECONNECT,
	SQL_API_SQLFREEENV,
#ifdef SQL_API_SQLFREEHANDLE
	SQL_API_SQLFREEHANDLE,
#endif
	SQL_API_SQLFREESTMT,
#ifdef SQL_API_SQLGETCONNECTATTR
	SQL_API_SQLGETCONNECTATTR,
#endif
	SQL_API_SQLGETCONNECTOPTION,
	SQL_API_SQLGETDATA,
#ifdef SQL_API_SQLGETDESCFIELD
	SQL_API_SQLGETDESCFIELD,
#endif
#ifdef SQL_API_SQLGETDESCREC
	SQL_API_SQLGETDESCREC,
#endif
#ifdef SQL_API_SQLGETDIAGREC
	SQL_API_SQLGETDIAGREC,
#endif
#ifdef SQL_API_SQLGETENVATTR
	SQL_API_SQLGETENVATTR,
#endif
	SQL_API_SQLGETFUNCTIONS,
	SQL_API_SQLGETINFO,
#ifdef SQL_API_SQLGETSTMTATTR
	SQL_API_SQLGETSTMTATTR,
#endif
	SQL_API_SQLGETSTMTOPTION,
	SQL_API_SQLGETTYPEINFO,
	SQL_API_SQLMORERESULTS,
	SQL_API_SQLNATIVESQL,
	SQL_API_SQLNUMPARAMS,
	SQL_API_SQLNUMRESULTCOLS,
	SQL_API_SQLPARAMOPTIONS,
	SQL_API_SQLPREPARE,
	SQL_API_SQLPRIMARYKEYS,
	SQL_API_SQLPROCEDURES,
	SQL_API_SQLROWCOUNT,
#ifdef SQL_API_SQLSETCONNECTATTR
	SQL_API_SQLSETCONNECTATTR,
#endif
	SQL_API_SQLSETCONNECTOPTION,
#ifdef SQL_API_SQLSETDESCFIELD
	SQL_API_SQLSETDESCFIELD,
#endif
#ifdef SQL_API_SQLSETDESCREC
	SQL_API_SQLSETDESCREC,
#endif
#ifdef SQL_API_SQLSETENVATTR
	SQL_API_SQLSETENVATTR,
#endif
	SQL_API_SQLSETPARAM,
	SQL_API_SQLSETPOS,
#ifdef SQL_API_SQLSETSTMTATTR
	SQL_API_SQLSETSTMTATTR,
#endif
	SQL_API_SQLSETSTMTOPTION,
	SQL_API_SQLSPECIALCOLUMNS,
	SQL_API_SQLSTATISTICS,
	SQL_API_SQLTABLEPRIVILEGES,
	SQL_API_SQLTABLES,
	SQL_API_SQLTRANSACT,
#if 0
/* not yet implemented */
#ifdef SQL_API_SQLALLOCHANDLESTD
	SQL_API_SQLALLOCHANDLESTD,
#endif
#ifdef SQL_API_SQLBINDPARAM
	SQL_API_SQLBINDPARAM,
#endif
#ifdef SQL_API_SQLBULKOPERATIONS
	SQL_API_SQLBULKOPERATIONS,
#endif
	SQL_API_SQLDATASOURCES,
	SQL_API_SQLDRIVERS,
	SQL_API_SQLGETCURSORNAME,
#ifdef SQL_API_SQLGETDIAGFIELD
	SQL_API_SQLGETDIAGFIELD,
#endif
	SQL_API_SQLPARAMDATA,
	SQL_API_SQLPROCEDURECOLUMNS,
	SQL_API_SQLPUTDATA,
	SQL_API_SQLSETCURSORNAME,
	SQL_API_SQLSETSCROLLOPTIONS,
#endif /* not yet implemented */
};

#define NFUNCIMPLEMENTED (sizeof(FuncImplemented)/sizeof(FuncImplemented[0]))

/* this table is a bit map compatible with
   SQL_API_ODBC3_ALL_FUNCTIONS, to be initialized from the table
   above */
static UWORD FuncExistMap[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE];

SQLRETURN SQL_API
SQLGetFunctions(SQLHDBC ConnectionHandle,
		SQLUSMALLINT FunctionId,
		SQLUSMALLINT *SupportedPtr)
{
	ODBCDbc *dbc = (ODBCDbc *) ConnectionHandle;

#ifdef ODBCDEBUG
	ODBCLOG("SQLGetFunctions " PTRFMT " %u\n",
		PTRFMTCAST ConnectionHandle, (unsigned int) FunctionId);
#endif

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	if (!SQL_FUNC_EXISTS(FuncExistMap, FuncImplemented[0])) {
		/* not yet initialized, so do it now */
		UWORD *p;

		for (p = FuncImplemented; p < &FuncImplemented[NFUNCIMPLEMENTED]; p++)
			FuncExistMap[*p >> 4] |= 1 << (*p & 0xF);
	}

	if (FunctionId == SQL_API_ODBC3_ALL_FUNCTIONS) {
		memcpy(SupportedPtr, FuncExistMap,
		       SQL_API_ODBC3_ALL_FUNCTIONS_SIZE * sizeof(FuncExistMap[0]));
		return SQL_SUCCESS;
	}

	if (FunctionId < SQL_API_ODBC3_ALL_FUNCTIONS_SIZE * 16) {
		*SupportedPtr = SQL_FUNC_EXISTS(FuncExistMap, FunctionId);
		return SQL_SUCCESS;
	}

	/* Function type out of range */
	addDbcError(dbc, "HY095", NULL, 0);
	return SQL_ERROR;
}
