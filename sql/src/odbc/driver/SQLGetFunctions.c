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


SQLRETURN
SQLGetFunctions(SQLHDBC hDbc, SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported)
{
	ODBCDbc *dbc = (ODBCDbc *) hDbc;

	if (!isValidDbc(dbc))
		return SQL_INVALID_HANDLE;

	clearDbcErrors(dbc);

	switch (FunctionId) {
	case SQL_API_SQLALLOCCONNECT:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLALLOCENV:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLALLOCHANDLE
	case SQL_API_SQLALLOCHANDLE:
		*Supported = SQL_TRUE;
		break;
#endif
#ifdef SQL_API_SQLALLOCHANDLESTD
	case SQL_API_SQLALLOCHANDLESTD:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLALLOCSTMT:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLBINDCOL:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLBINDPARAM
	case SQL_API_SQLBINDPARAM:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLBINDPARAMETER:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLBROWSECONNECT:
		*Supported = SQL_FALSE;
		break;
#ifdef SQL_API_SQLBULKOPERATIONS
	case SQL_API_SQLBULKOPERATIONS:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLCANCEL:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLCLOSECURSOR
	case SQL_API_SQLCLOSECURSOR:
		*Supported = SQL_TRUE;
		break;
#endif
#if defined(SQL_API_SQLCOLATTRIBUTE) && SQL_API_SQLCOLATTRIBUTE != SQL_API_SQLCOLATTRIBUTES
	case SQL_API_SQLCOLATTRIBUTE:
		*Supported = SQL_TRUE;
		break;
#endif
	case SQL_API_SQLCOLATTRIBUTES:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLCOLUMNPRIVILEGES:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLCOLUMNS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLCONNECT:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLCOPYDESC
	case SQL_API_SQLCOPYDESC:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLDATASOURCES:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLDESCRIBECOL:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLDESCRIBEPARAM:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLDISCONNECT:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLDRIVERCONNECT:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLDRIVERS:
		*Supported = SQL_FALSE;
		break;
#ifdef SQL_API_SQLENDTRAN
	case SQL_API_SQLENDTRAN:
		*Supported = SQL_TRUE;
		break;
#endif
	case SQL_API_SQLERROR:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLEXECDIRECT:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLEXECUTE:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLEXTENDEDFETCH:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLFETCH:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLFETCHSCROLL
	case SQL_API_SQLFETCHSCROLL:
		*Supported = SQL_TRUE;
		break;
#endif
	case SQL_API_SQLFOREIGNKEYS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLFREECONNECT:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLFREEENV:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLFREEHANDLE
	case SQL_API_SQLFREEHANDLE:
		*Supported = SQL_TRUE;
		break;
#endif
	case SQL_API_SQLFREESTMT:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLGETCONNECTATTR
	case SQL_API_SQLGETCONNECTATTR:
		*Supported = SQL_TRUE;
		break;
#endif
	case SQL_API_SQLGETCONNECTOPTION:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLGETCURSORNAME:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLGETDATA:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLGETDESCFIELD
	case SQL_API_SQLGETDESCFIELD:
		*Supported = SQL_FALSE;
		break;
#endif
#ifdef SQL_API_SQLGETDESCREC
	case SQL_API_SQLGETDESCREC:
		*Supported = SQL_FALSE;
		break;
#endif
#ifdef SQL_API_SQLGETDIAGFIELD
	case SQL_API_SQLGETDIAGFIELD:
		*Supported = SQL_FALSE;
		break;
#endif
#ifdef SQL_API_SQLGETDIAGREC
	case SQL_API_SQLGETDIAGREC:
		*Supported = SQL_TRUE;
		break;
#endif
#ifdef SQL_API_SQLGETENVATTR
	case SQL_API_SQLGETENVATTR:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLGETFUNCTIONS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLGETINFO:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLGETSTMTATTR
	case SQL_API_SQLGETSTMTATTR:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLGETSTMTOPTION:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLGETTYPEINFO:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLMORERESULTS:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLNATIVESQL:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLNUMPARAMS:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLNUMRESULTCOLS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLPARAMDATA:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLPARAMOPTIONS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLPREPARE:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLPRIMARYKEYS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLPROCEDURECOLUMNS:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLPROCEDURES:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLPUTDATA:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLROWCOUNT:
		*Supported = SQL_TRUE;
		break;
#ifdef SQL_API_SQLSETCONNECTATTR
	case SQL_API_SQLSETCONNECTATTR:
		*Supported = SQL_TRUE;
		break;
#endif
	case SQL_API_SQLSETCONNECTOPTION:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLSETCURSORNAME:
		*Supported = SQL_FALSE;
		break;
#ifdef SQL_API_SQLSETDESCFIELD
	case SQL_API_SQLSETDESCFIELD:
		*Supported = SQL_FALSE;
		break;
#endif
#ifdef SQL_API_SQLSETDESCREC
	case SQL_API_SQLSETDESCREC:
		*Supported = SQL_FALSE;
		break;
#endif
#ifdef SQL_API_SQLSETENVATTR
	case SQL_API_SQLSETENVATTR:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLSETPARAM:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLSETPOS:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLSETSCROLLOPTIONS:
		*Supported = SQL_FALSE;
		break;
#ifdef SQL_API_SQLSETSTMTATTR
	case SQL_API_SQLSETSTMTATTR:
		*Supported = SQL_FALSE;
		break;
#endif
	case SQL_API_SQLSETSTMTOPTION:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLSPECIALCOLUMNS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLSTATISTICS:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLTABLEPRIVILEGES:
		*Supported = SQL_FALSE;
		break;
	case SQL_API_SQLTABLES:
		*Supported = SQL_TRUE;
		break;
	case SQL_API_SQLTRANSACT:
		*Supported = SQL_TRUE;
		break;
	default:
		/* HY095: Function type out of range */
		addDbcError(dbc, "HY095", NULL, 0);
		return SQL_ERROR;
	}

	return SQL_SUCCESS;
}
