// The contents of this file are subject to the MonetDB Public License
// Version 1.1 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at
// http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
//
// Software distributed under the License is distributed on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
// License for the specific language governing rights and limitations
// under the License.
//
// The Original Code is the MonetDB Database System.
//
// The Initial Developer of the Original Code is CWI.
// Portions created by CWI are Copyright (C) 1997-2007 CWI.
// All Rights Reserved.

%module odbc
%include "typemaps.i"
%{
#include <sqltypes.h>
#include <sqlucode.h>

#ifdef SQL_MAX_OPTION_STRING_LENGTH
#define BUFLEN ((SQL_MAX_OPTION_STRING_LENGTH) > 1024 ? (SQL_MAX_OPTION_STRING_LENGTH) : 1024)
#else
#define BUFLEN 1024
#endif

#if !defined(PY_LONG_LONG) && defined(LONG_LONG)
/* in python 2.2 it's still called LONG_LONG, but we use the newer
   name PY_LONG_LONG */
#define PY_LONG_LONG LONG_LONG
#endif

static PyObject *ErrorObject;
%}

%ignore __SQLTYPES_H;
%include "sqltypes.h"

%init %{
{
	PyObject *odbc;

	/* create the error object */
	ErrorObject = PyErr_NewException("odbc.error", NULL, NULL);
	PyDict_SetItemString(d, "error", ErrorObject);

	/* dirty trick to export the error object just created to the
	   interface module odbc */
	odbc = PyImport_ImportModule("odbc");
	if (odbc == NULL)
		Py_FatalError("can't import module odbc for _odbc");
	PyModule_AddObject(odbc, "error", ErrorObject);
	PyModule_AddObject(odbc, "SQLGetConnectAttrA",
			   PyObject_GetAttrString(m, "SQLGetConnectAttr"));
	PyModule_AddObject(odbc, "SQLGetEnvAttrA",
			   PyObject_GetAttrString(m, "SQLGetEnvAttr"));
	PyModule_AddObject(odbc, "SQLGetStmtAttrA",
			   PyObject_GetAttrString(m, "SQLGetStmtAttr"));
	PyModule_AddObject(odbc, "SQLGetConnectOptionA",
			   PyObject_GetAttrString(m, "SQLGetConnectOption"));
	PyModule_AddObject(odbc, "SQLGetDescFieldA",
			   PyObject_GetAttrString(m, "SQLGetDescField"));
	PyModule_AddObject(odbc, "SQLGetDiagFieldA",
			   PyObject_GetAttrString(m, "SQLGetDiagField"));
	PyModule_AddObject(odbc, "SQLGetInfoA",
			   PyObject_GetAttrString(m, "SQLGetInfo"));
	PyModule_AddObject(odbc, "SQLSetConnectOptionA",
			   PyObject_GetAttrString(m, "SQLSetConnectOption"));
	PyModule_AddObject(odbc, "SQLSetStmtOptionA",
			   PyObject_GetAttrString(m, "SQLSetStmtOption"));
	Py_DECREF(odbc);
}
%}

%{
/*
  helper code for SQLWCHAR parameters
 */

static PyObject *
PyUnicode_FromSqlWChar(const SQLWCHAR *s, SQLSMALLINT l)
{
	const SQLWCHAR *s1;
	Py_UNICODE *u, *u1;
	PyObject *o;

	if (s == NULL) {
		Py_INCREF(Py_None);
		return Py_None;
	}
	if (l == SQL_NTS)
		for (l = 0, s1 = s; *s1; s1++, l++)
			;
	u = PyMem_Malloc((l + 1) * sizeof(Py_UNICODE));
	for (s1 = s, u1 = u; l > 0; s1++, u1++, l--)
		*u1 = *s1;
	o = PyUnicode_FromUnicode(u, (int) (s1 - s));
	PyMem_Free(u);
	return o;
}

SQLWCHAR *
SqlWChar_FromPyUnicode(PyObject *o, int *lp)
{
	Py_UNICODE *u;
	int ul;
	SQLWCHAR *b, *s;

	if ((u = PyUnicode_AsUnicode(o)) == NULL)
		return NULL;
	ul = PyUnicode_GetSize(o);
	if ((b = s = PyMem_Malloc((ul + 1) * sizeof(SQLWCHAR))) == NULL)
		return NULL;
	while (ul-- >= 0)
		*s++ = *u++;
	if (lp)
		*lp = (int) (s - b) - 1;
	return b;
}
%}

%typemap(in, numinputs=1) (SQLWCHAR *STRING, int LENGTH) (int len) {
	if (($1 = SqlWChar_FromPyUnicode($input, &len)) == NULL) goto fail;
	$2 = ($2_type) len;
}
%typemap(argout) (SQLWCHAR *STRING, int LENGTH) {
	PyMem_Free($1);
}

/*
  output arg to return a Handle
 */
%typemap(in, numinputs=0) SQLHANDLE *OUTPUT (SQLHANDLE temp) {
	$1 = &temp;
}
%typemap(argout,fragment="t_output_helper") SQLHANDLE *OUTPUT {
	PyObject *o = SWIG_NewPointerObj((void *) *$1, $1_descriptor, 0);
	$result = t_output_helper($result, o);
}

/*
  output arg to return a 5-byte state string
  used only in SQLError and SQLGetDiagRec
*/
%typemap(in, numinputs=0) SQLCHAR *Sqlstate (SQLCHAR tempbuf[6]) {
	$1 = tempbuf;
}
%typemap(argout,fragment="t_output_helper") SQLCHAR *Sqlstate {
	PyObject *o = PyString_FromStringAndSize((char *) $1, 5);
	$result = t_output_helper($result, o);
}
// SQLErrorA, SQLGetDiagRecA
%apply SQLCHAR *Sqlstate {SQLCHAR *szSqlState};
// SQLErrorW, SQLGetDiagRecW
%typemap(in, numinputs=0) SQLWCHAR *szSqlState (SQLWCHAR tempbuf[6]) {
	$1 = tempbuf;
}
%typemap(argout,fragment="t_output_helper") SQLWCHAR *szSqlState {
	PyObject *o = PyUnicode_FromSqlWChar($1, 5);
	$result = t_output_helper($result, o);
}

/*
  Three args to return a string: OUTBUF points to a buffer of length
  BUFSIZE which is filled with *OUTSIZE characters plus a NUL byte.
  If the buffer is too small, *OUTSIZE returns the required buffer
  size.
*/
%typemap(in, numinputs=0) (char *OUTBUF, int BUFSIZE, int *OUTSIZE) (SQLCHAR msgbuf[BUFLEN], $*3_type msglen) {
	$1 = msgbuf;
	$2 = ($2_type) sizeof(msgbuf);
	$3 = &msglen;
}
%typemap(argout,fragment="t_output_helper") (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {
	PyObject *o = PyString_FromStringAndSize((char *) $1, *$3 >= $2 ? $2 - 1 : *$3);
	$result = t_output_helper($result, o);
}
%typemap(in, numinputs=0) (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) (SQLWCHAR msgbuf[BUFLEN], $*3_type msglen) {
	$1 = msgbuf;
	$2 = ($2_type) (sizeof(msgbuf) / sizeof(SQLWCHAR));
	$3 = &msglen;
}
%typemap(argout,fragment="t_output_helper") (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {
	PyObject *o = PyUnicode_FromSqlWChar($1, *$3 >= $2 ? $2 - 1 : *$3);
	$result = t_output_helper($result, o);
}

// SQLError, SQLGetDiagRec
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength)};
// SQLErrorA, SQLGetDiagRecA
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szErrorMsg, SQLSMALLINT cbErrorMsgMax, SQLSMALLINT *pcbErrorMsg)};
// SQLErrorW, SQLGetDiagRecW
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szErrorMsg, SQLSMALLINT cbErrorMsgMax, SQLSMALLINT *pcbErrorMsg)};

// SQLAllocConnect
%newobject SQLAllocConnect;
%apply SQLHANDLE *OUTPUT {SQLHDBC *ConnectionHandle};

// SQLAllocEnv
%newobject SQLAllocEnv;
%apply SQLHANDLE *OUTPUT {SQLHENV *EnvironmentHandle};

// SQLAllocHandle
#if (ODBCVER >= 0x0300)
%newobject SQLAllocHandle;
%apply SQLHANDLE *OUTPUT {SQLHANDLE *OutputHandle};
#endif

// SQLAllocStmt
%newobject SQLAllocStmt;
%apply SQLHANDLE *OUTPUT {SQLHSTMT *StatementHandle};

// SQLBrowseConnect, SQLBrowseConnectA
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn)};
// SQLBrowseConnectW
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn)};

// SQLColAttribute
%typemap(in, numinputs=1) (SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength, SQLPOINTER NumericAttribute) (char tempbuf[BUFLEN], $*4_type buflen, int temp) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = (SQLPOINTER) tempbuf;
	$3 = (SQLSMALLINT) sizeof(tempbuf);
	$4 = &buflen;
	$5 = (SQLPOINTER) &temp;
}
%typemap(argout,fragment="t_output_helper") (SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength, SQLPOINTER NumericAttribute) {
	PyObject *o;
	switch ($1) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_CONCISE_TYPE:
	case SQL_DESC_COUNT:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_LENGTH:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_NUM_PREC_RADIX:
	case SQL_DESC_OCTET_LENGTH:
	case SQL_DESC_PRECISION:
	case SQL_DESC_SCALE:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TYPE:
	case SQL_DESC_UNNAMED:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE:
		o = PyInt_FromLong((long) * (int *) $5);
		break;
	default:
		o = PyString_FromStringAndSize((char *) $2, *$4 >= $3 ? $3 - 1 : *$4);
		break;
	}
	$result = t_output_helper($result, o);
}
// SQLColAttributeA
%apply (SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength, SQLPOINTER NumericAttribute) {(SQLSMALLINT iField, SQLPOINTER pCharAttr, SQLSMALLINT cbCharAttrMax, SQLSMALLINT *pcbCharAttr, SQLPOINTER pNumAttr)};
// SQLColAttributeW
%typemap(in, numinputs=1) (SQLUSMALLINT iField, SQLPOINTER pCharAttr, SQLSMALLINT cbCharAttrMax, SQLSMALLINT *pcbCharAttr, SQLPOINTER pNumAttr) (char tempbuf[BUFLEN], $*4_type buflen, int temp) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = (SQLPOINTER) tempbuf;
	$3 = (SQLSMALLINT) sizeof(tempbuf);
	$4 = &buflen;
	$5 = (SQLPOINTER) &temp;
}
%typemap(argout,fragment="t_output_helper") (SQLUSMALLINT iField, SQLPOINTER pCharAttr, SQLSMALLINT cbCharAttrMax, SQLSMALLINT *pcbCharAttr, SQLPOINTER pNumAttr) {
	PyObject *o;
	switch ($1) {
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_CONCISE_TYPE:
	case SQL_DESC_COUNT:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_LENGTH:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_NUM_PREC_RADIX:
	case SQL_DESC_OCTET_LENGTH:
	case SQL_DESC_PRECISION:
	case SQL_DESC_SCALE:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TYPE:
	case SQL_DESC_UNNAMED:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE:
		o = PyInt_FromLong((long) * (int *) $5);
		break;
	default:
		o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, (*$4 >= $3 ? $3 - sizeof(SQLWCHAR) : *$4) / sizeof(SQLWCHAR));
		break;
	}
	$result = t_output_helper($result, o);
}

%ignore SQLColAttributes;

// SQLColumnPrivileges, SQLPrimaryKeys, SQLProcedureColumns, SQLProcedures,
// SQLTablePrivileges
// SQLColumnPrivilegesA, SQLPrimaryKeysA, SQLProcedureColumnsA, SQLProceduresA,
// SQLTablePrivilegesA, SQLColumnsA, SQLSpecialColumnsA, SQLTablesA
%apply (char *STRING, int LENGTH) {(SQLCHAR *szCatalogName, SQLSMALLINT cbCatalogName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szSchemaName, SQLSMALLINT cbSchemaName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szTableName, SQLSMALLINT cbTableName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szColumnName, SQLSMALLINT cbColumnName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szProcName, SQLSMALLINT cbProcName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szTableType, SQLSMALLINT cbTableType)};

// SQLColumns, SQLSpecialColumns, SQLStatistics, SQLTables
%apply (char *STRING, int LENGTH) {(SQLCHAR *CatalogName, SQLSMALLINT NameLength1)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *SchemaName, SQLSMALLINT NameLength2)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *TableName, SQLSMALLINT NameLength3)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *ColumnName, SQLSMALLINT NameLength4)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *TableType, SQLSMALLINT NameLength4)};

// SQLConnect
%apply (char *STRING, int LENGTH) {(SQLCHAR *ServerName, SQLSMALLINT NameLength1)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *UserName, SQLSMALLINT NameLength2)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *Authentication, SQLSMALLINT NameLength3)};

// SQLColumnPrivilegesW, SQLPrimaryKeysW, SQLProcedureColumnsW, SQLProceduresW,
// SQLTablePrivilegesW, SQLColumnsW, SQLSpecialColumnsW, SQLTablesW
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szCatalogName, SQLSMALLINT cbCatalogName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szSchemaName, SQLSMALLINT cbSchemaName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szTableName, SQLSMALLINT cbTableName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szColumnName, SQLSMALLINT cbColumnName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szProcName, SQLSMALLINT cbProcName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szTableType, SQLSMALLINT cbTableType)};
// SQLConnectA
%apply (char *STRING, int LENGTH) {(SQLCHAR *szDSN, SQLSMALLINT cbDSN)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szUID, SQLSMALLINT cbUID)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szAuthStr, SQLSMALLINT cbAuthStr)};
// SQLConnectW
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szDSN, SQLSMALLINT cbDSN)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szUID, SQLSMALLINT cbUID)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szAuthStr, SQLSMALLINT cbAuthStr)};

// SQLDataSources
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *ServerName, SQLSMALLINT BufferLength1, SQLSMALLINT *NameLength1)};
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *Description, SQLSMALLINT BufferLength2, SQLSMALLINT *NameLength2)};
// SQLDataSourcesA
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szDSN, SQLSMALLINT cbDSNMax, SQLSMALLINT *pcbDSN)};
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szDescription, SQLSMALLINT cbDescriptionMax, SQLSMALLINT *pcbDescription)};
// SQLDataSourcesW
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szDSN, SQLSMALLINT cbDSNMax, SQLSMALLINT *pcbDSN)};
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szDescription, SQLSMALLINT cbDescriptionMax, SQLSMALLINT *pcbDescription)};

// SQLDescribeCol
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *ColumnName, SQLSMALLINT BufferLength, SQLSMALLINT *NameLength)};
%apply int *OUTPUT {SQLSMALLINT *DataType};
%apply int *OUTPUT {SQLULEN *ColumnSize};
%apply int *OUTPUT {SQLSMALLINT *DecimalDigits};
%apply int *OUTPUT {SQLSMALLINT *Nullable};
// SQLDescribeColA
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szColName, SQLSMALLINT cbColNameMax, SQLSMALLINT *pcbColName)};
// SQLDescribeColW
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szColName, SQLSMALLINT cbColNameMax, SQLSMALLINT *pcbColName)};
%apply int *OUTPUT {SQLSMALLINT *pfSqlType};
%apply int *OUTPUT {SQLULEN *pcbColDef};
%apply int *OUTPUT {SQLSMALLINT *pibScale};
%apply int *OUTPUT {SQLSMALLINT *pfNullable};

// SQLDescribeParam, SQLDescribeParamA
%apply int *OUTPUT {SQLSMALLINT *pfSqlType};
%apply int *OUTPUT {SQLULEN *pcbParamDef};
%apply int *OUTPUT {SQLSMALLINT *pibScale};
%apply int *OUTPUT {SQLSMALLINT *pfNullable};

// SQLDriverConnect,  SQLDriverConnectA
%apply (char *STRING, int LENGTH) {(SQLCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn)};
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)};
// SQLDriverConnectW
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szConnStrIn, SQLSMALLINT cbConnStrIn)};
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szConnStrOut, SQLSMALLINT cbConnStrOutMax, SQLSMALLINT *pcbConnStrOut)};

// SQLDrivers, SQLDriversA
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szDriverDesc, SQLSMALLINT cbDriverDescMax, SQLSMALLINT *pcbDriverDesc)};
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szDriverAttributes, SQLSMALLINT cbDrvrAttrMax, SQLSMALLINT *pcbDrvrAttr)};
// SQLDriversW
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szDriverDesc, SQLSMALLINT cbDriverDescMax, SQLSMALLINT *pcbDriverDesc)};
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szDriverAttributes, SQLSMALLINT cbDrvrAttrMax, SQLSMALLINT *pcbDrvrAttr)};

// SQLError, SQLGetDiagRec
// SQLErrorA, SQLGetDiagRecA
%apply int *OUTPUT {SQLINTEGER *NativeError};
// SQLErrorW, SQLGetDiagRecW
%apply int *OUTPUT {SQLINTEGER *pfNativeError};

// SQLExecDirect, SQLPrepare
%apply (char *STRING, int LENGTH) {(SQLCHAR *StatementText, SQLINTEGER TextLength)};
// SQLExecDirectA, SQLPrepareA
%apply (char *STRING, int LENGTH) {(SQLCHAR *szSqlStr, SQLINTEGER cbSqlStr)};
// SQLExecDirectW, SQLPrepareW
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szSqlStr, SQLINTEGER cbSqlStr)};

// SQLExtendedFetch
%apply int *OUTPUT {SQLROWSETSIZE *pcrow};
%apply int *OUTPUT {SQLUSMALLINT *rgfRowStatus};

// SQLForeignKeys, SQLForeignKeysA
%apply (char *STRING, int LENGTH) {(SQLCHAR *szPkCatalogName, SQLSMALLINT cbPkCatalogName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szPkSchemaName, SQLSMALLINT cbPkSchemaName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szPkTableName, SQLSMALLINT cbPkTableName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szFkCatalogName, SQLSMALLINT cbFkCatalogName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szFkSchemaName, SQLSMALLINT cbFkSchemaName)};
%apply (char *STRING, int LENGTH) {(SQLCHAR *szFkTableName, SQLSMALLINT cbFkTableName)};
// SQLForeignKeysW
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szPkCatalogName, SQLSMALLINT cbPkCatalogName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szPkSchemaName, SQLSMALLINT cbPkSchemaName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szPkTableName, SQLSMALLINT cbPkTableName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szFkCatalogName, SQLSMALLINT cbFkCatalogName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szFkSchemaName, SQLSMALLINT cbFkSchemaName)};
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szFkTableName, SQLSMALLINT cbFkTableName)};

// SQLGetConnectAttr, SQLGetEnvAttr, SQLGetStmtAttr
// SQLGetConnectAttrA, SQLGetEnvAttrA, SQLGetStmtAttrA have the same
// argument names as SQLGetConnectAttrW, SQLGetEnvAttrW,
// SQLGetStmtAttrW, but behave the same as SQLGetConnectAttr,
// SQLGetEnvAttr, SQLGetStmtAttr (i.e. not unicode).  Also see the
// dirty trick in the %init section.
%ignore SQLGetConnectAttrA;
%ignore SQLGetEnvAttrA;
%ignore SQLGetStmtAttrA;
%typemap(in, numinputs=1) (SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER BufferLength, SQLINTEGER *StringLength) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_type) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLINTEGER Attribute, SQLPOINTER Value, SQLINTEGER BufferLength, SQLINTEGER *StringLength) {
	PyObject *o;
	switch ($1) {
	/* SQLGetConnectAttr values */
	case SQL_ATTR_ACCESS_MODE:
	case SQL_ATTR_ASYNC_ENABLE:
	case SQL_ATTR_AUTO_IPD:
	case SQL_ATTR_AUTOCOMMIT:
	case SQL_ATTR_CONNECTION_DEAD:
	case SQL_ATTR_CONNECTION_TIMEOUT:
	case SQL_ATTR_LOGIN_TIMEOUT:
	case SQL_ATTR_METADATA_ID:
	case SQL_ATTR_ODBC_CURSORS:
	case SQL_ATTR_PACKET_SIZE:
	case SQL_ATTR_TRACE:
	case SQL_ATTR_TRANSLATE_OPTION:	/* 32-bit value */
	case SQL_ATTR_TXN_ISOLATION: /* 32-bit value */
	/* SQLGetEnvAttr values */
	case SQL_ATTR_CONNECTION_POOLING:
	case SQL_ATTR_CP_MATCH:
	case SQL_ATTR_ODBC_VERSION:
#if SQL_ATTR_AUTO_IPD != SQL_ATTR_OUTPUT_NTS
	case SQL_ATTR_OUTPUT_NTS:
#endif
	/* SQLGetStmtAttr values */
	case SQL_ATTR_CONCURRENCY:
	case SQL_ATTR_CURSOR_SCROLLABLE:
	case SQL_ATTR_CURSOR_SENSITIVITY:
	case SQL_ATTR_CURSOR_TYPE:
	case SQL_ATTR_ENABLE_AUTO_IPD:
	case SQL_ATTR_KEYSET_SIZE:
	case SQL_ATTR_MAX_LENGTH:
	case SQL_ATTR_MAX_ROWS:
	case SQL_ATTR_NOSCAN:
	case SQL_ATTR_PARAM_BIND_TYPE:
	case SQL_ATTR_PARAMSET_SIZE:
	case SQL_ATTR_QUERY_TIMEOUT:
	case SQL_ATTR_RETRIEVE_DATA:
	case SQL_ATTR_ROW_ARRAY_SIZE:
	case SQL_ATTR_ROW_BIND_TYPE:
	case SQL_ATTR_ROW_NUMBER:
	case SQL_ATTR_SIMULATE_CURSOR:
	case SQL_ATTR_USE_BOOKMARKS:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
		/* string */
		o = PyString_FromStringAndSize((char *) $2, *$4 >= $3 ? $3 - 1 : *$4);
		break;
	case SQL_ATTR_QUIET_MODE:
	case SQL_ATTR_APP_PARAM_DESC:
	case SQL_ATTR_APP_ROW_DESC:
	case SQL_ATTR_IMP_PARAM_DESC:
	case SQL_ATTR_IMP_ROW_DESC:
		/* handle */
		if (* (SQLHANDLE *) $2)
			o = SWIG_NewPointerObj((void *) * (SQLHANDLE *) $2, $descriptor(SQLHANDLE *), 0);
		else {
			o = Py_None;
			Py_INCREF(o);
		}
		break;
	case SQL_ATTR_FETCH_BOOKMARK_PTR:
		/* pointer to binary bookmark value */
		/* TODO: implement */
		o = Py_None;
		Py_INCREF(o);
		break;
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
	case SQL_ATTR_PARAMS_PROCESSED_PTR:
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:
	case SQL_ATTR_ROWS_FETCHED_PTR:
		/* pointer to integer */
		/* TODO: implement */
		o = Py_None;
		Py_INCREF(o);
		break;
	case SQL_ATTR_PARAM_OPERATION_PTR:
	case SQL_ATTR_PARAM_STATUS_PTR:
	case SQL_ATTR_ROW_OPERATION_PTR:
	case SQL_ATTR_ROW_STATUS_PTR:
		/* pointer to array of SQLUSMALLINT */
		/* TODO: implement */
		o = Py_None;
		Py_INCREF(o);
		break;
	}
	$result = t_output_helper($result, o);
}
// SQLGetConnectAttrW, SQLGetEnvAttrW, SQLGetStmtAttrW
%typemap(in, numinputs=1) (SQLINTEGER fAttribute, SQLPOINTER rgbValue, SQLINTEGER cbValueMax, SQLINTEGER *pcbValue) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_type) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLINTEGER fAttribute, SQLPOINTER rgbValue, SQLINTEGER cbValueMax, SQLINTEGER *pcbValue) {
	PyObject *o;
	switch ($1) {
	/* SQLGetConnectAttr values */
	case SQL_ATTR_ACCESS_MODE:
	case SQL_ATTR_ASYNC_ENABLE:
	case SQL_ATTR_AUTO_IPD:
	case SQL_ATTR_AUTOCOMMIT:
	case SQL_ATTR_CONNECTION_DEAD:
	case SQL_ATTR_CONNECTION_TIMEOUT:
	case SQL_ATTR_LOGIN_TIMEOUT:
	case SQL_ATTR_METADATA_ID:
	case SQL_ATTR_ODBC_CURSORS:
	case SQL_ATTR_PACKET_SIZE:
	case SQL_ATTR_TRACE:
	case SQL_ATTR_TRANSLATE_OPTION:	/* 32-bit value */
	case SQL_ATTR_TXN_ISOLATION: /* 32-bit value */
	/* SQLGetEnvAttr values */
	case SQL_ATTR_CONNECTION_POOLING:
	case SQL_ATTR_CP_MATCH:
	case SQL_ATTR_ODBC_VERSION:
#if SQL_ATTR_AUTO_IPD != SQL_ATTR_OUTPUT_NTS
	case SQL_ATTR_OUTPUT_NTS:
#endif
	/* SQLGetStmtAttr values */
	case SQL_ATTR_CONCURRENCY:
	case SQL_ATTR_CURSOR_SCROLLABLE:
	case SQL_ATTR_CURSOR_SENSITIVITY:
	case SQL_ATTR_CURSOR_TYPE:
	case SQL_ATTR_ENABLE_AUTO_IPD:
	case SQL_ATTR_KEYSET_SIZE:
	case SQL_ATTR_MAX_LENGTH:
	case SQL_ATTR_MAX_ROWS:
	case SQL_ATTR_NOSCAN:
	case SQL_ATTR_PARAM_BIND_TYPE:
	case SQL_ATTR_PARAMSET_SIZE:
	case SQL_ATTR_QUERY_TIMEOUT:
	case SQL_ATTR_RETRIEVE_DATA:
	case SQL_ATTR_ROW_ARRAY_SIZE:
	case SQL_ATTR_ROW_BIND_TYPE:
	case SQL_ATTR_ROW_NUMBER:
	case SQL_ATTR_SIMULATE_CURSOR:
	case SQL_ATTR_USE_BOOKMARKS:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_ATTR_CURRENT_CATALOG:
	case SQL_ATTR_TRACEFILE:
	case SQL_ATTR_TRANSLATE_LIB:
		/* string */
		o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, (*$4 >= $3 ? $3 - sizeof(SQLWCHAR) : *$4) / sizeof(SQLWCHAR));
		break;
	case SQL_ATTR_QUIET_MODE:
	case SQL_ATTR_APP_PARAM_DESC:
	case SQL_ATTR_APP_ROW_DESC:
	case SQL_ATTR_IMP_PARAM_DESC:
	case SQL_ATTR_IMP_ROW_DESC:
		/* handle */
		if (* (SQLHANDLE *) $2)
			o = SWIG_NewPointerObj((void *) * (SQLHANDLE *) $2, $descriptor(SQLHANDLE *), 0);
		else {
			o = Py_None;
			Py_INCREF(o);
		}
		break;
	case SQL_ATTR_FETCH_BOOKMARK_PTR:
		/* pointer to binary bookmark value */
		/* TODO: implement */
		o = Py_None;
		Py_INCREF(o);
		break;
	case SQL_ATTR_PARAM_BIND_OFFSET_PTR:
	case SQL_ATTR_PARAMS_PROCESSED_PTR:
	case SQL_ATTR_ROW_BIND_OFFSET_PTR:
	case SQL_ATTR_ROWS_FETCHED_PTR:
		/* pointer to integer */
		/* TODO: implement */
		o = Py_None;
		Py_INCREF(o);
		break;
	case SQL_ATTR_PARAM_OPERATION_PTR:
	case SQL_ATTR_PARAM_STATUS_PTR:
	case SQL_ATTR_ROW_OPERATION_PTR:
	case SQL_ATTR_ROW_STATUS_PTR:
		/* pointer to array of SQLUSMALLINT */
		/* TODO: implement */
		o = Py_None;
		Py_INCREF(o);
		break;
	}
	$result = t_output_helper($result, o);
}

// SQLGetConnectOption, SQLGetStmtOption
%typemap(in, numinputs=1) (SQLUSMALLINT Option, SQLPOINTER Value) (char tempbuf[BUFLEN]) {
	$1 = ($1_type) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
};
%typemap(argout,fragment="t_output_helper") (SQLUSMALLINT Option, SQLPOINTER Value) {
	PyObject *o;
	switch ($1) {
	case SQL_ACCESS_MODE:
	case SQL_AUTOCOMMIT:
	case SQL_LOGIN_TIMEOUT:
	case SQL_ODBC_CURSORS:
	case SQL_OPT_TRACE:
	case SQL_PACKET_SIZE:
	case SQL_QUIET_MODE:
	case SQL_TRANSLATE_OPTION:
	case SQL_TXN_ISOLATION:
	case SQL_QUERY_TIMEOUT:
	case SQL_MAX_ROWS:
	case SQL_NOSCAN:
	case SQL_MAX_LENGTH:
	case SQL_ASYNC_ENABLE:
	case SQL_BIND_TYPE:
	case SQL_CURSOR_TYPE:
	case SQL_CONCURRENCY:
	case SQL_KEYSET_SIZE:
	case SQL_ROWSET_SIZE:
	case SQL_SIMULATE_CURSOR:
	case SQL_RETRIEVE_DATA:
	case SQL_USE_BOOKMARKS:
/*		case SQL_GET_BOOKMARKS:	is deprecated in ODBC 3.0+ */
	case SQL_ROW_NUMBER:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		/* string */
		o = PyString_FromString((char *) $2);
		break;
	}
	$result = t_output_helper($result, o);
}
// SQLGetConnectOptionW
%ignore SQLGetConnectOptionA;
%typemap(in, numinputs=1) (SQLUSMALLINT fOption, SQLPOINTER pvParam) (char tempbuf[BUFLEN]) {
	$1 = ($1_type) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
};
%typemap(argout,fragment="t_output_helper") (SQLUSMALLINT fOption, SQLPOINTER pvParam) {
	PyObject *o;
	switch ($1) {
	case SQL_ACCESS_MODE:
	case SQL_AUTOCOMMIT:
	case SQL_LOGIN_TIMEOUT:
	case SQL_ODBC_CURSORS:
	case SQL_OPT_TRACE:
	case SQL_PACKET_SIZE:
	case SQL_QUIET_MODE:
	case SQL_TRANSLATE_OPTION:
	case SQL_TXN_ISOLATION:
	case SQL_QUERY_TIMEOUT:
	case SQL_MAX_ROWS:
	case SQL_NOSCAN:
	case SQL_MAX_LENGTH:
	case SQL_ASYNC_ENABLE:
	case SQL_BIND_TYPE:
	case SQL_CURSOR_TYPE:
	case SQL_CONCURRENCY:
	case SQL_KEYSET_SIZE:
	case SQL_ROWSET_SIZE:
	case SQL_SIMULATE_CURSOR:
	case SQL_RETRIEVE_DATA:
	case SQL_USE_BOOKMARKS:
/*		case SQL_GET_BOOKMARKS:	is deprecated in ODBC 3.0+ */
	case SQL_ROW_NUMBER:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_CURRENT_QUALIFIER:
	case SQL_OPT_TRACEFILE:
	case SQL_TRANSLATE_DLL:
		/* string */
		o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, SQL_NTS);
		break;
	}
	$result = t_output_helper($result, o);
}

// SQLGetCursorName
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *CursorName, SQLSMALLINT BufferLength, SQLSMALLINT *NameLength)};
// SQLGetCursorNameA
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szCursor, SQLSMALLINT cbCursorMax, SQLSMALLINT *pcbCursor)};
// SQLGetCursorNameW
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szCursor, SQLSMALLINT cbCursorMax, SQLSMALLINT *pcbCursor)};

// SQLGetData
// known problem: if the result value doesn't fit in tempbuf, you only
// get part of the result
%typemap(in, numinputs=1) (SQLSMALLINT TargetType, SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind) ($*4_type len, SQLCHAR tempbuf[2048]) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = sizeof(tempbuf);
	$4 = &len;
}
%typemap(argout,fragment="t_output_helper") (SQLSMALLINT TargetType, SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind) {
	PyObject *o;
	if (*$4 == SQL_NULL_DATA || *$4 == SQL_NO_TOTAL) {
		o = Py_None;
		Py_INCREF(Py_None);
	} else
	switch ($1) {
	case SQL_C_CHAR:
	case SQL_C_BINARY:
#ifdef SQL_C_XML
	case SQL_C_XML:
#endif
#if SQL_C_VARBOOKMARK != SQL_C_BINARY
	case SQL_C_VARBOOKMARK:
#endif
		if (*$4 == SQL_NO_TOTAL)
			o = PyString_FromStringAndSize((char *) $2, $3 - 1);
		else
			o = PyString_FromStringAndSize((char *) $2, *$4 >= $3 ? $3 - 1 : *$4);
		break;
	case SQL_C_WCHAR:
		if (*$4 == SQL_NO_TOTAL)
			o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, ($3 - sizeof(SQLWCHAR)) / sizeof(SQLWCHAR));
		else
			o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, (*$4 >= $3 ? $3 - sizeof(SQLWCHAR) : *$4) / sizeof(SQLWCHAR));
		break;
	case SQL_C_BIT:
		o = PyBool_FromLong((long) * (SQLCHAR *) $2);
		break;
	case SQL_C_UTINYINT:
		o = PyInt_FromLong((long) * (SQLCHAR *) $2);
		break;
	case SQL_C_SHORT:
	case SQL_C_SSHORT:
		o = PyInt_FromLong((long) * (SQLSMALLINT *) $2);
		break;
	case SQL_C_USHORT:
		o = PyInt_FromLong((long) * (SQLUSMALLINT *) $2);
		break;
	case SQL_C_LONG:
	case SQL_C_SLONG:
		o = PyInt_FromLong((long) * (SQLINTEGER *) $2);
		break;
	case SQL_C_ULONG:
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_C_FLOAT:
		o = PyFloat_FromDouble((double) * (SQLREAL *) $2);
		break;
	case SQL_C_DOUBLE:
		o = PyFloat_FromDouble((double) * (SQLDOUBLE *) $2);
		break;
	case SQL_C_TINYINT:
	case SQL_C_STINYINT:
		o = PyInt_FromLong((long) * (SQLSCHAR *) $2);
		break;
	case SQL_C_SBIGINT:
		o = PyLong_FromLongLong((PY_LONG_LONG) * (SQLBIGINT *) $2);
		break;
	case SQL_C_UBIGINT:
		o = PyLong_FromLongLong((unsigned PY_LONG_LONG) * (SQLUBIGINT *) $2);
		break;
#if SQL_C_BOOKMARK != SQL_C_ULONG && SQL_C_BOOKMARK != SQL_C_UBIGINT
	case SQL_C_BOOKMARK:
		o = PyInt_FromLong((long) * (BOOKMARK *) $2);
		break;
#endif
	case SQL_C_TYPE_DATE:
		o = Py_BuildValue("(iii)", ((SQL_DATE_STRUCT *) $2)->year, ((SQL_DATE_STRUCT *) $2)->month, ((SQL_DATE_STRUCT *) $2)->day);
		break;
	case SQL_C_TYPE_TIME:
		o = Py_BuildValue("(iii)", ((SQL_TIME_STRUCT *) $2)->hour, ((SQL_TIME_STRUCT *) $2)->minute, ((SQL_TIME_STRUCT *) $2)->second);
		break;
	case SQL_C_TYPE_TIMESTAMP:
		o = Py_BuildValue("(iiiiid)", ((SQL_TIMESTAMP_STRUCT *) $2)->year, ((SQL_TIMESTAMP_STRUCT *) $2)->month, ((SQL_TIMESTAMP_STRUCT *) $2)->day, ((SQL_TIMESTAMP_STRUCT *) $2)->hour, ((SQL_TIMESTAMP_STRUCT *) $2)->minute, ((SQL_TIMESTAMP_STRUCT *) $2)->second + ((SQL_TIMESTAMP_STRUCT *) $2)->fraction / 1000000000.);
		break;
	case SQL_C_NUMERIC:
		o = Py_BuildValue("(iiis#)", ((SQL_NUMERIC_STRUCT *) $2)->precision, ((SQL_NUMERIC_STRUCT *) $2)->scale, ((SQL_NUMERIC_STRUCT *) $2)->sign, ((SQL_NUMERIC_STRUCT *) $2)->val, SQL_MAX_NUMERIC_LEN);
		break;
	case SQL_C_GUID:
		o = Py_BuildValue("(iiis#)", ((SQLGUID *) $2)->Data1, ((SQLGUID *) $2)->Data2, ((SQLGUID *) $2)->Data3, ((SQLGUID *) $2)->Data4, 8);
		break;
	case SQL_C_INTERVAL_YEAR:
	case SQL_C_INTERVAL_MONTH:
	case SQL_C_INTERVAL_YEAR_TO_MONTH:
		o = Py_BuildValue("(ii(ii))", ((SQL_INTERVAL_STRUCT *) $2)->interval_type, ((SQL_INTERVAL_STRUCT *) $2)->interval_sign, ((SQL_INTERVAL_STRUCT *) $2)->intval.year_month.year, ((SQL_INTERVAL_STRUCT *) $2)->intval.year_month.month);
		break;
	case SQL_C_INTERVAL_DAY:
	case SQL_C_INTERVAL_HOUR:
	case SQL_C_INTERVAL_MINUTE:
	case SQL_C_INTERVAL_SECOND:
	case SQL_C_INTERVAL_DAY_TO_HOUR:
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
	case SQL_C_INTERVAL_DAY_TO_SECOND:
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
	case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		o = Py_BuildValue("(ii(iiiii))", ((SQL_INTERVAL_STRUCT *) $2)->interval_type, ((SQL_INTERVAL_STRUCT *) $2)->interval_sign, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.day, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.hour, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.minute, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.second, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.fraction);
		break;
	default:
		o = Py_None;
		Py_INCREF(Py_None);
		break;
	}
	$result = t_output_helper($result, o);
}

// SQLGetDescField
%typemap(in, numinputs=1) (SQLSMALLINT FieldIdentifier, SQLPOINTER Value, SQLINTEGER BufferLength, SQLINTEGER *StringLength) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_type) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLSMALLINT FieldIdentifier, SQLPOINTER Value, SQLINTEGER BufferLength, SQLINTEGER *StringLength) {
	PyObject *o;
	switch ($1) {
	case SQL_DESC_ALLOC_TYPE:
	case SQL_DESC_COUNT:
	case SQL_DESC_CONCISE_TYPE:
	case SQL_DESC_DATETIME_INTERVAL_CODE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_PRECISION:
	case SQL_DESC_ROWVER:
	case SQL_DESC_SCALE:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TYPE:
	case SQL_DESC_UNNAMED:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE:
		/* SQLSMALLINT */
		o = PyInt_FromLong((long) * (SQLSMALLINT *) $2);
		break;
	case SQL_DESC_ARRAY_SIZE:
	case SQL_DESC_BIND_TYPE:
	case SQL_DESC_LENGTH:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_DATETIME_INTERVAL_PRECISION:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_NUM_PREC_RADIX:
	case SQL_DESC_OCTET_LENGTH:
	case SQL_DESC_PARAMETER_TYPE:
		/* SQLINTEGER */
		o = PyInt_FromLong((long) * (SQLINTEGER *) $2);
		break;
	case SQL_DESC_BASE_COLUMN_NAME:
	case SQL_DESC_BASE_TABLE_NAME:
	case SQL_DESC_CATALOG_NAME:
	case SQL_DESC_LABEL:
	case SQL_DESC_LITERAL_PREFIX:
	case SQL_DESC_LITERAL_SUFFIX:
	case SQL_DESC_LOCAL_TYPE_NAME:
	case SQL_DESC_NAME:
	case SQL_DESC_SCHEMA_NAME:
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_TYPE_NAME:
		/* string */
		o = PyString_FromStringAndSize((char *) $2, *$4 >= $3 ? $3 - 1 : *$4);
		break;
	case SQL_DESC_ARRAY_STATUS_PTR:
	case SQL_DESC_BIND_OFFSET_PTR:
	case SQL_DESC_ROWS_PROCESSED_PTR:
	case SQL_DESC_DATA_PTR:
	case SQL_DESC_INDICATOR_PTR:
	case SQL_DESC_OCTET_LENGTH_PTR:
		/* pointer */
		o = Py_None;
		Py_INCREF(Py_None);
		break;
	}
	$result = t_output_helper($result, o);
}
// SQLGetDescFieldW
%ignore SQLGetDescFieldA;
%typemap(in, numinputs=1) (SQLSMALLINT iField, SQLPOINTER rgbValue, SQLINTEGER cbValueMax, SQLINTEGER *pcbValue) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_type) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLSMALLINT iField, SQLPOINTER rgbValue, SQLINTEGER cbValueMax, SQLINTEGER *pcbValue) {
	PyObject *o;
	switch ($1) {
	case SQL_DESC_ALLOC_TYPE:
	case SQL_DESC_COUNT:
	case SQL_DESC_CONCISE_TYPE:
	case SQL_DESC_DATETIME_INTERVAL_CODE:
	case SQL_DESC_FIXED_PREC_SCALE:
	case SQL_DESC_NULLABLE:
	case SQL_DESC_PRECISION:
	case SQL_DESC_ROWVER:
	case SQL_DESC_SCALE:
	case SQL_DESC_SEARCHABLE:
	case SQL_DESC_TYPE:
	case SQL_DESC_UNNAMED:
	case SQL_DESC_UNSIGNED:
	case SQL_DESC_UPDATABLE:
		/* SQLSMALLINT */
		o = PyInt_FromLong((long) * (SQLSMALLINT *) $2);
		break;
	case SQL_DESC_ARRAY_SIZE:
	case SQL_DESC_BIND_TYPE:
	case SQL_DESC_LENGTH:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_DESC_AUTO_UNIQUE_VALUE:
	case SQL_DESC_CASE_SENSITIVE:
	case SQL_DESC_DATETIME_INTERVAL_PRECISION:
	case SQL_DESC_DISPLAY_SIZE:
	case SQL_DESC_NUM_PREC_RADIX:
	case SQL_DESC_OCTET_LENGTH:
	case SQL_DESC_PARAMETER_TYPE:
		/* SQLINTEGER */
		o = PyInt_FromLong((long) * (SQLINTEGER *) $2);
		break;
	case SQL_DESC_BASE_COLUMN_NAME:
	case SQL_DESC_BASE_TABLE_NAME:
	case SQL_DESC_CATALOG_NAME:
	case SQL_DESC_LABEL:
	case SQL_DESC_LITERAL_PREFIX:
	case SQL_DESC_LITERAL_SUFFIX:
	case SQL_DESC_LOCAL_TYPE_NAME:
	case SQL_DESC_NAME:
	case SQL_DESC_SCHEMA_NAME:
	case SQL_DESC_TABLE_NAME:
	case SQL_DESC_TYPE_NAME:
		/* string */
		o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, (*$4 >= $3 ? $3 - sizeof(SQLWCHAR) : *$4) / sizeof(SQLWCHAR));
		break;
	case SQL_DESC_ARRAY_STATUS_PTR:
	case SQL_DESC_BIND_OFFSET_PTR:
	case SQL_DESC_ROWS_PROCESSED_PTR:
	case SQL_DESC_DATA_PTR:
	case SQL_DESC_INDICATOR_PTR:
	case SQL_DESC_OCTET_LENGTH_PTR:
		/* pointer */
		o = Py_None;
		Py_INCREF(Py_None);
		break;
	}
	$result = t_output_helper($result, o);
}

// SQLGetDescRec
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *Name, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength)};
%apply int *OUTPUT { SQLSMALLINT *Type };
%apply int *OUTPUT { SQLSMALLINT *SubType };
%apply int *OUTPUT { SQLLEN *Length };
%apply int *OUTPUT { SQLSMALLINT *Precision };
%apply int *OUTPUT { SQLSMALLINT *Scale };
%apply int *OUTPUT { SQLSMALLINT *Nullable };
// SQLGetDescRecA, SQLGetDescRecW
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szName, SQLSMALLINT cbNameMax, SQLSMALLINT *pcbName)};
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szName, SQLSMALLINT cbNameMax, SQLSMALLINT *pcbName)};
%apply int *OUTPUT { SQLSMALLINT *pfType };
%apply int *OUTPUT { SQLSMALLINT *pfSubType };
%apply int *OUTPUT { SQLLEN *pLength };
%apply int *OUTPUT { SQLSMALLINT *pPrecision };
%apply int *OUTPUT { SQLSMALLINT *pScale };
%apply int *OUTPUT { SQLSMALLINT *pNullable };

// SQLGetDiagField
%typemap(in, numinputs=1) (SQLSMALLINT DiagIdentifier, SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLSMALLINT DiagIdentifier, SQLPOINTER DiagInfo, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) {
	PyObject *o;
	switch ($1) {
	case SQL_DIAG_DYNAMIC_FUNCTION:
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_CONNECTION_NAME:
	case SQL_DIAG_MESSAGE_TEXT:
	case SQL_DIAG_SERVER_NAME:
	case SQL_DIAG_SQLSTATE:
	case SQL_DIAG_SUBCLASS_ORIGIN:
		/* string */
		o = PyString_FromStringAndSize((char *) $2, *$4 >= $3 ? $3 - 1 : *$4);
		break;
	case SQL_DIAG_CURSOR_ROW_COUNT:
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
	case SQL_DIAG_NUMBER:
	case SQL_DIAG_ROW_COUNT:
	case SQL_DIAG_COLUMN_NUMBER:
	case SQL_DIAG_NATIVE:
	case SQL_DIAG_ROW_NUMBER:
		/* SQLINTEGER */
		o = PyInt_FromLong((long) * (SQLINTEGER *) $2);
		break;
	case SQL_DIAG_RETURNCODE:
		/* SQLRETURN */
		o = PyInt_FromLong((long) * (SQLRETURN *) $2);
		break;
	default:
		PyErr_SetString(ErrorObject, "bad info type");
		SWIG_fail;
	}
	$result = t_output_helper($result, o);
}
// SQLGetDiagFieldW
%ignore SQLGetDiagFieldA;
%typemap(in, numinputs=1) (SQLSMALLINT fDiagField, SQLPOINTER rgbDiagInfo, SQLSMALLINT cbDiagInfoMax, SQLSMALLINT *pcbDiagInfo) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLSMALLINT fDiagField, SQLPOINTER rgbDiagInfo, SQLSMALLINT cbDiagInfoMax, SQLSMALLINT *pcbDiagInfo) {
	PyObject *o;
	switch ($1) {
	case SQL_DIAG_DYNAMIC_FUNCTION:
	case SQL_DIAG_CLASS_ORIGIN:
	case SQL_DIAG_CONNECTION_NAME:
	case SQL_DIAG_MESSAGE_TEXT:
	case SQL_DIAG_SERVER_NAME:
	case SQL_DIAG_SQLSTATE:
	case SQL_DIAG_SUBCLASS_ORIGIN:
		/* string */
		o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, (*$4 >= $3 ? $3 - sizeof(SQLWCHAR) : *$4) / sizeof(SQLWCHAR));
		break;
	case SQL_DIAG_CURSOR_ROW_COUNT:
	case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
	case SQL_DIAG_NUMBER:
	case SQL_DIAG_ROW_COUNT:
	case SQL_DIAG_COLUMN_NUMBER:
	case SQL_DIAG_NATIVE:
	case SQL_DIAG_ROW_NUMBER:
		/* SQLINTEGER */
		o = PyInt_FromLong((long) * (SQLINTEGER *) $2);
		break;
	case SQL_DIAG_RETURNCODE:
		/* SQLRETURN */
		o = PyInt_FromLong((long) * (SQLRETURN *) $2);
		break;
	default:
		PyErr_SetString(ErrorObject, "bad info type");
		SWIG_fail;
	}
	$result = t_output_helper($result, o);
}

// SQLGetFunctions
%apply int *OUTPUT {SQLUSMALLINT *Supported};
%typemap(in, numinputs=1) (SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported) (SQLUSMALLINT tempbuf[SQL_API_ODBC3_ALL_FUNCTIONS_SIZE]) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
}
%typemap(argout,fragment="t_output_helper") (SQLUSMALLINT FunctionId, SQLUSMALLINT *Supported) {
	PyObject *o;
	int i;
	switch ($1) {
	case SQL_API_ODBC3_ALL_FUNCTIONS:
		o = PyList_New(SQL_API_ODBC3_ALL_FUNCTIONS_SIZE);
		for (i = 0; i < SQL_API_ODBC3_ALL_FUNCTIONS_SIZE; i++)
			PyList_SetItem(o, i, PyInt_FromLong((long) $2[i]));
		break;
	case SQL_API_ALL_FUNCTIONS:
		o = PyList_New(100);
		for (i = 0; i < 100; i++)
			PyList_SetItem(o, i, PyInt_FromLong((long) $2[i]));
		break;
	default:
		o = PyInt_FromLong((long) *$2);
		break;
	}
	if (PyErr_Occurred()) {
		Py_XDECREF(o);
		SWIG_fail;
	}
	$result = t_output_helper($result, o);
}

// SQLGetInfo
// InfoTypes SQL_DRIVER_HDESC and SQL_DRIVER_HSTMS use *InfoValue as
// input/output value.  This has not (yet) been implemented by this
// code.
%typemap(in, numinputs=1) (SQLUSMALLINT InfoType, SQLPOINTER InfoValue, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLUSMALLINT InfoType, SQLPOINTER InfoValue, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) {
	PyObject *o;
	switch ($1) {
	case SQL_ACCESSIBLE_PROCEDURES:
	case SQL_ACCESSIBLE_TABLES:
	case SQL_CATALOG_NAME:
	case SQL_CATALOG_NAME_SEPARATOR:
	case SQL_CATALOG_TERM:
	case SQL_COLLATION_SEQ:
	case SQL_COLUMN_ALIAS:
	case SQL_DATA_SOURCE_NAME:
	case SQL_DATA_SOURCE_READ_ONLY:
	case SQL_DATABASE_NAME:
	case SQL_DBMS_NAME:
	case SQL_DESCRIBE_PARAMETER:
	case SQL_DM_VER:
	case SQL_DRIVER_NAME:
	case SQL_DRIVER_ODBC_VER:
	case SQL_DRIVER_VER:
	case SQL_EXPRESSIONS_IN_ORDERBY:
	case SQL_IDENTIFIER_QUOTE_CHAR:
	case SQL_INTEGRITY:
	case SQL_KEYWORDS:
	case SQL_LIKE_ESCAPE_CLAUSE:
	case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
	case SQL_MULT_RESULT_SETS:
	case SQL_MULTIPLE_ACTIVE_TXN:
	case SQL_NEED_LONG_DATA_LEN:
	case SQL_ODBC_VER:
	case SQL_ORDER_BY_COLUMNS_IN_SELECT:
	case SQL_PROCEDURE_TERM:
	case SQL_PROCEDURES:
	case SQL_ROW_UPDATES:
	case SQL_SCHEMA_TERM:
	case SQL_SEARCH_PATTERN_ESCAPE:
	case SQL_SERVER_NAME:
	case SQL_SPECIAL_CHARACTERS:
	case SQL_TABLE_TERM:
	case SQL_USER_NAME:
	case SQL_XOPEN_CLI_YEAR:
		/* string */
		o = PyString_FromStringAndSize((char *) $2, *$4 >= $3 ? $3 - 1 : *$4);
		break;
	case SQL_ACTIVE_ENVIRONMENTS:
	case SQL_CATALOG_LOCATION:
	case SQL_CONCAT_NULL_BEHAVIOR:
	case SQL_CORRELATION_NAME:
	case SQL_CURSOR_COMMIT_BEHAVIOR:
	case SQL_CURSOR_ROLLBACK_BEHAVIOR:
	case SQL_FILE_USAGE:
	case SQL_GROUP_BY:
	case SQL_IDENTIFIER_CASE:
	case SQL_MAX_CATALOG_NAME_LEN:
	case SQL_MAX_COLUMN_NAME_LEN:
	case SQL_MAX_COLUMNS_IN_GROUP_BY:
	case SQL_MAX_COLUMNS_IN_INDEX:
	case SQL_MAX_COLUMNS_IN_ORDER_BY:
	case SQL_MAX_COLUMNS_IN_SELECT:
	case SQL_MAX_COLUMNS_IN_TABLE:
	case SQL_MAX_CONCURRENT_ACTIVITIES:
	case SQL_MAX_CURSOR_NAME_LEN:
	case SQL_MAX_DRIVER_CONNECTIONS:
	case SQL_MAX_IDENTIFIER_LEN:
	case SQL_MAX_PROCEDURE_NAME_LEN:
	case SQL_MAX_SCHEMA_NAME_LEN:
	case SQL_MAX_TABLE_NAME_LEN:
	case SQL_MAX_TABLES_IN_SELECT:
	case SQL_MAX_USER_NAME_LEN:
	case SQL_NON_NULLABLE_COLUMNS:
	case SQL_NULL_COLLATION:
	case SQL_QUOTED_IDENTIFIER_CASE:
	case SQL_TXN_CAPABLE:
		/* SQLUSMALLINT */
		o = PyInt_FromLong((long) * (SQLUSMALLINT *) $2);
		break;
	case SQL_AGGREGATE_FUNCTIONS:
	case SQL_ALTER_DOMAIN:
	case SQL_ALTER_TABLE:
	case SQL_ASYNC_MODE:
	case SQL_BATCH_ROW_COUNT:
	case SQL_BATCH_SUPPORT:
	case SQL_BOOKMARK_PERSISTENCE:
	case SQL_CATALOG_USAGE:
	case SQL_CONVERT_BIGINT:
	case SQL_CONVERT_BINARY:
	case SQL_CONVERT_BIT:
	case SQL_CONVERT_CHAR:
#ifdef SQL_CONVERT_GUID
	case SQL_CONVERT_GUID:
#endif
	case SQL_CONVERT_DATE:
	case SQL_CONVERT_DECIMAL:
	case SQL_CONVERT_DOUBLE:
	case SQL_CONVERT_FLOAT:
	case SQL_CONVERT_INTEGER:
	case SQL_CONVERT_INTERVAL_YEAR_MONTH:
	case SQL_CONVERT_INTERVAL_DAY_TIME:
	case SQL_CONVERT_LONGVARBINARY:
	case SQL_CONVERT_LONGVARCHAR:
	case SQL_CONVERT_NUMERIC:
	case SQL_CONVERT_REAL:
	case SQL_CONVERT_SMALLINT:
	case SQL_CONVERT_TIME:
	case SQL_CONVERT_TIMESTAMP:
	case SQL_CONVERT_TINYINT:
	case SQL_CONVERT_VARBINARY:
	case SQL_CONVERT_VARCHAR:
	case SQL_CONVERT_FUNCTIONS:
	case SQL_CREATE_ASSERTION:
	case SQL_CREATE_CHARACTER_SET:
	case SQL_CREATE_COLLATION:
	case SQL_CREATE_DOMAIN:
	case SQL_CREATE_SCHEMA:
	case SQL_CREATE_TABLE:
	case SQL_CREATE_TRANSLATION:
	case SQL_CREATE_VIEW:
#ifdef SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY
	case SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY:
#endif
	case SQL_DATETIME_LITERALS:
	case SQL_DDL_INDEX:
	case SQL_DEFAULT_TXN_ISOLATION:
	case SQL_DRIVER_HDBC:
#if 0				/* not implemented */
	case SQL_DRIVER_HENV:
	case SQL_DRIVER_HDESC:
#endif
	case SQL_DRIVER_HLIB:
	case SQL_DRIVER_HSTMT:
	case SQL_DROP_ASSERTION:
	case SQL_DROP_CHARACTER_SET:
	case SQL_DROP_COLLATION:
	case SQL_DROP_DOMAIN:
	case SQL_DROP_SCHEMA:
	case SQL_DROP_TABLE:
	case SQL_DROP_TRANSLATION:
	case SQL_DROP_VIEW:
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
	case SQL_GETDATA_EXTENSIONS:
	case SQL_INDEX_KEYWORDS:
	case SQL_INFO_SCHEMA_VIEWS:
	case SQL_INSERT_STATEMENT:
	case SQL_KEYSET_CURSOR_ATTRIBUTES1:
	case SQL_KEYSET_CURSOR_ATTRIBUTES2:
	case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
	case SQL_MAX_BINARY_LITERAL_LEN:
	case SQL_MAX_CHAR_LITERAL_LEN:
	case SQL_MAX_INDEX_SIZE:
	case SQL_MAX_ROW_SIZE:
	case SQL_MAX_STATEMENT_LEN:
	case SQL_NUMERIC_FUNCTIONS:
	case SQL_ODBC_INTERFACE_CONFORMANCE:
	case SQL_OJ_CAPABILITIES:
	case SQL_PARAM_ARRAY_ROW_COUNTS:
	case SQL_PARAM_ARRAY_SELECTS:
	case SQL_SCHEMA_USAGE:
	case SQL_SCROLL_OPTIONS:
	case SQL_SQL_CONFORMANCE:
	case SQL_SQL92_DATETIME_FUNCTIONS:
	case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
	case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
	case SQL_SQL92_GRANT:
	case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
	case SQL_SQL92_PREDICATES:
	case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
	case SQL_SQL92_REVOKE:
	case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
	case SQL_SQL92_STRING_FUNCTIONS:
	case SQL_SQL92_VALUE_EXPRESSIONS:
	case SQL_STANDARD_CLI_CONFORMANCE:
	case SQL_STATIC_CURSOR_ATTRIBUTES1:
	case SQL_STATIC_CURSOR_ATTRIBUTES2:
	case SQL_STRING_FUNCTIONS:
	case SQL_SUBQUERIES:
	case SQL_SYSTEM_FUNCTIONS:
	case SQL_TIMEDATE_ADD_INTERVALS:
	case SQL_TIMEDATE_DIFF_INTERVALS:
	case SQL_TIMEDATE_FUNCTIONS:
	case SQL_TXN_ISOLATION_OPTION:
	case SQL_UNION:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_POS_OPERATIONS:
		/* SQLINTEGER */
		o = PyInt_FromLong((long) * (SQLINTEGER *) $2);
		break;
	default:
		PyErr_SetString(ErrorObject, "bad info type");
		SWIG_fail;
	}
	$result = t_output_helper($result, o);
}
// SQLGetInfoW
// InfoTypes SQL_DRIVER_HDESC and SQL_DRIVER_HSTMS use *InfoValue as
// input/output value.  This has not (yet) been implemented by this
// code.
%ignore SQLGetInfoA;
%typemap(in, numinputs=1) (SQLUSMALLINT fInfoType, SQLPOINTER rgbInfoValue, SQLSMALLINT cbInfoValueMax, SQLSMALLINT *pcbInfoValue) (char tempbuf[BUFLEN], $*4_type len) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = len = sizeof(tempbuf);
	$4 = &len;
};
%typemap(argout,fragment="t_output_helper") (SQLUSMALLINT fInfoType, SQLPOINTER rgbInfoValue, SQLSMALLINT cbInfoValueMax, SQLSMALLINT *pcbInfoValue) {
	PyObject *o;
	switch ($1) {
	case SQL_ACCESSIBLE_PROCEDURES:
	case SQL_ACCESSIBLE_TABLES:
	case SQL_CATALOG_NAME:
	case SQL_CATALOG_NAME_SEPARATOR:
	case SQL_CATALOG_TERM:
	case SQL_COLLATION_SEQ:
	case SQL_COLUMN_ALIAS:
	case SQL_DATA_SOURCE_NAME:
	case SQL_DATA_SOURCE_READ_ONLY:
	case SQL_DATABASE_NAME:
	case SQL_DBMS_NAME:
	case SQL_DESCRIBE_PARAMETER:
	case SQL_DM_VER:
	case SQL_DRIVER_NAME:
	case SQL_DRIVER_ODBC_VER:
	case SQL_DRIVER_VER:
	case SQL_EXPRESSIONS_IN_ORDERBY:
	case SQL_IDENTIFIER_QUOTE_CHAR:
	case SQL_INTEGRITY:
	case SQL_KEYWORDS:
	case SQL_LIKE_ESCAPE_CLAUSE:
	case SQL_MAX_ROW_SIZE_INCLUDES_LONG:
	case SQL_MULT_RESULT_SETS:
	case SQL_MULTIPLE_ACTIVE_TXN:
	case SQL_NEED_LONG_DATA_LEN:
	case SQL_ODBC_VER:
	case SQL_ORDER_BY_COLUMNS_IN_SELECT:
	case SQL_PROCEDURE_TERM:
	case SQL_PROCEDURES:
	case SQL_ROW_UPDATES:
	case SQL_SCHEMA_TERM:
	case SQL_SEARCH_PATTERN_ESCAPE:
	case SQL_SERVER_NAME:
	case SQL_SPECIAL_CHARACTERS:
	case SQL_TABLE_TERM:
	case SQL_USER_NAME:
	case SQL_XOPEN_CLI_YEAR:
		/* string */
		o = PyUnicode_FromSqlWChar((SQLWCHAR *) $2, (*$4 >= $3 ? $3 - sizeof(SQLWCHAR) : *$4) / sizeof(SQLWCHAR));
		break;
	case SQL_ACTIVE_ENVIRONMENTS:
	case SQL_CATALOG_LOCATION:
	case SQL_CONCAT_NULL_BEHAVIOR:
	case SQL_CORRELATION_NAME:
	case SQL_CURSOR_COMMIT_BEHAVIOR:
	case SQL_CURSOR_ROLLBACK_BEHAVIOR:
	case SQL_FILE_USAGE:
	case SQL_GROUP_BY:
	case SQL_IDENTIFIER_CASE:
	case SQL_MAX_CATALOG_NAME_LEN:
	case SQL_MAX_COLUMN_NAME_LEN:
	case SQL_MAX_COLUMNS_IN_GROUP_BY:
	case SQL_MAX_COLUMNS_IN_INDEX:
	case SQL_MAX_COLUMNS_IN_ORDER_BY:
	case SQL_MAX_COLUMNS_IN_SELECT:
	case SQL_MAX_COLUMNS_IN_TABLE:
	case SQL_MAX_CONCURRENT_ACTIVITIES:
	case SQL_MAX_CURSOR_NAME_LEN:
	case SQL_MAX_DRIVER_CONNECTIONS:
	case SQL_MAX_IDENTIFIER_LEN:
	case SQL_MAX_PROCEDURE_NAME_LEN:
	case SQL_MAX_SCHEMA_NAME_LEN:
	case SQL_MAX_TABLE_NAME_LEN:
	case SQL_MAX_TABLES_IN_SELECT:
	case SQL_MAX_USER_NAME_LEN:
	case SQL_NON_NULLABLE_COLUMNS:
	case SQL_NULL_COLLATION:
	case SQL_QUOTED_IDENTIFIER_CASE:
	case SQL_TXN_CAPABLE:
	case SQL_FETCH_DIRECTION:
	case SQL_ODBC_API_CONFORMANCE:
	case SQL_ODBC_SQL_CONFORMANCE:
	case SQL_SCROLL_CONCURRENCY:
		/* SQLUSMALLINT */
		o = PyInt_FromLong((long) * (SQLUSMALLINT *) $2);
		break;
	case SQL_AGGREGATE_FUNCTIONS:
	case SQL_ALTER_DOMAIN:
	case SQL_ALTER_TABLE:
	case SQL_ASYNC_MODE:
	case SQL_BATCH_ROW_COUNT:
	case SQL_BATCH_SUPPORT:
	case SQL_BOOKMARK_PERSISTENCE:
	case SQL_CATALOG_USAGE:
	case SQL_CONVERT_BIGINT:
	case SQL_CONVERT_BINARY:
	case SQL_CONVERT_BIT:
	case SQL_CONVERT_CHAR:
#ifdef SQL_CONVERT_GUID
	case SQL_CONVERT_GUID:
#endif
	case SQL_CONVERT_DATE:
	case SQL_CONVERT_DECIMAL:
	case SQL_CONVERT_DOUBLE:
	case SQL_CONVERT_FLOAT:
	case SQL_CONVERT_INTEGER:
	case SQL_CONVERT_INTERVAL_YEAR_MONTH:
	case SQL_CONVERT_INTERVAL_DAY_TIME:
	case SQL_CONVERT_LONGVARBINARY:
	case SQL_CONVERT_LONGVARCHAR:
	case SQL_CONVERT_NUMERIC:
	case SQL_CONVERT_REAL:
	case SQL_CONVERT_SMALLINT:
	case SQL_CONVERT_TIME:
	case SQL_CONVERT_TIMESTAMP:
	case SQL_CONVERT_TINYINT:
	case SQL_CONVERT_VARBINARY:
	case SQL_CONVERT_VARCHAR:
	case SQL_CONVERT_FUNCTIONS:
	case SQL_CREATE_ASSERTION:
	case SQL_CREATE_CHARACTER_SET:
	case SQL_CREATE_COLLATION:
	case SQL_CREATE_DOMAIN:
	case SQL_CREATE_SCHEMA:
	case SQL_CREATE_TABLE:
	case SQL_CREATE_TRANSLATION:
	case SQL_CREATE_VIEW:
#ifdef SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY
	case SQL_CURSOR_ROLLBACK_SQL_CURSOR_SENSITIVITY:
#endif
	case SQL_DATETIME_LITERALS:
	case SQL_DDL_INDEX:
	case SQL_DEFAULT_TXN_ISOLATION:
	case SQL_DRIVER_HDBC:
#if 0				/* not implemented */
	case SQL_DRIVER_HENV:
	case SQL_DRIVER_HDESC:
#endif
	case SQL_DRIVER_HLIB:
	case SQL_DRIVER_HSTMT:
	case SQL_DROP_ASSERTION:
	case SQL_DROP_CHARACTER_SET:
	case SQL_DROP_COLLATION:
	case SQL_DROP_DOMAIN:
	case SQL_DROP_SCHEMA:
	case SQL_DROP_TABLE:
	case SQL_DROP_TRANSLATION:
	case SQL_DROP_VIEW:
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES1:
	case SQL_DYNAMIC_CURSOR_ATTRIBUTES2:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1:
	case SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2:
	case SQL_GETDATA_EXTENSIONS:
	case SQL_INDEX_KEYWORDS:
	case SQL_INFO_SCHEMA_VIEWS:
	case SQL_INSERT_STATEMENT:
	case SQL_KEYSET_CURSOR_ATTRIBUTES1:
	case SQL_KEYSET_CURSOR_ATTRIBUTES2:
	case SQL_MAX_ASYNC_CONCURRENT_STATEMENTS:
	case SQL_MAX_BINARY_LITERAL_LEN:
	case SQL_MAX_CHAR_LITERAL_LEN:
	case SQL_MAX_INDEX_SIZE:
	case SQL_MAX_ROW_SIZE:
	case SQL_MAX_STATEMENT_LEN:
	case SQL_NUMERIC_FUNCTIONS:
	case SQL_ODBC_INTERFACE_CONFORMANCE:
	case SQL_OJ_CAPABILITIES:
	case SQL_PARAM_ARRAY_ROW_COUNTS:
	case SQL_PARAM_ARRAY_SELECTS:
	case SQL_SCHEMA_USAGE:
	case SQL_SCROLL_OPTIONS:
	case SQL_SQL_CONFORMANCE:
	case SQL_SQL92_DATETIME_FUNCTIONS:
	case SQL_SQL92_FOREIGN_KEY_DELETE_RULE:
	case SQL_SQL92_FOREIGN_KEY_UPDATE_RULE:
	case SQL_SQL92_GRANT:
	case SQL_SQL92_NUMERIC_VALUE_FUNCTIONS:
	case SQL_SQL92_PREDICATES:
	case SQL_SQL92_RELATIONAL_JOIN_OPERATORS:
	case SQL_SQL92_REVOKE:
	case SQL_SQL92_ROW_VALUE_CONSTRUCTOR:
	case SQL_SQL92_STRING_FUNCTIONS:
	case SQL_SQL92_VALUE_EXPRESSIONS:
	case SQL_STANDARD_CLI_CONFORMANCE:
	case SQL_STATIC_CURSOR_ATTRIBUTES1:
	case SQL_STATIC_CURSOR_ATTRIBUTES2:
	case SQL_STRING_FUNCTIONS:
	case SQL_SUBQUERIES:
	case SQL_SYSTEM_FUNCTIONS:
	case SQL_TIMEDATE_ADD_INTERVALS:
	case SQL_TIMEDATE_DIFF_INTERVALS:
	case SQL_TIMEDATE_FUNCTIONS:
	case SQL_TXN_ISOLATION_OPTION:
	case SQL_UNION:
	case SQL_LOCK_TYPES:
	case SQL_POS_OPERATIONS:
	case SQL_POSITIONED_STATEMENTS:
		/* SQLUINTEGER */
		o = PyInt_FromLong((long) * (SQLUINTEGER *) $2);
		break;
	case SQL_STATIC_SENSITIVITY:
		/* SQLINTEGER */
		o = PyInt_FromLong((long) * (SQLINTEGER *) $2);
		break;
	default:
		PyErr_SetString(ErrorObject, "bad info type");
		SWIG_fail;
	}
	$result = t_output_helper($result, o);
}

// SQLNativeSql, SQLNativeSqlA
%apply (char *STRING, int LENGTH) {(SQLCHAR *szSqlStrIn, SQLINTEGER cbSqlStrIn)};
%apply (char *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLCHAR *szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)};
// SQLNativeSqlW
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szSqlStrIn, SQLINTEGER cbSqlStrIn)};
%apply (SQLWCHAR *OUTBUF, int BUFSIZE, int *OUTSIZE) {(SQLWCHAR *szSqlStr, SQLINTEGER cbSqlStrMax, SQLINTEGER *pcbSqlStr)};

// SQLNumParams
%apply int *OUTPUT {SQLSMALLINT *pcpar};

// SQLNumResultCols
%apply int *OUTPUT {SQLSMALLINT *ColumnCount};

%ignore SQLParamOptions;

// SQLPutData (questionable)
%apply (char *STRING, int LENGTH) {(SQLPOINTER Data, SQLLEN StrLen_or_Ind)};

// SQLRowCount
%apply int *OUTPUT {SQLLEN *RowCount};

// SQLSetConnectAttr, SQLSetEnvAttr, SQLSetStmtAttr
%typemap(in, numinputs=1) (SQLPOINTER Value, SQLINTEGER StringLength) (SQLUINTEGER u, SQLHANDLE h) {
	u = (SQLUINTEGER) PyInt_AsUnsignedLongMask($input);
	if (!PyErr_Occurred()) {
		$1 = ($1_type) (size_t) u;
		$2 = 0;
	} else {
		PyErr_Clear();
		$1 = ($1_type) PyString_AsString($input);
		if (!PyErr_Occurred()) {
			$2 = ($2_type) PyString_Size($input);
		} else {
			PyErr_Clear();
			if (SWIG_ConvertPtr($input, &h, $descriptor(SQLHANDLE *), 0) == 0) {
				$1 = ($1_type) &h;
				$2 = 0;
			} else {
				PyErr_SetString(ErrorObject, "bad value type");
				SWIG_fail;
			}
		}
	}
}
// SQLSetConnectAttrW, SQLSetEnvAttrW, SQLSetStmtAttrW
%typemap(in, numinputs=1) (SQLPOINTER rgbValue, SQLINTEGER cbValue) {
	if (PyUnicode_Check($input)) {
		int i;
		$1 = ($1_type) SqlWChar_FromPyUnicode($input, &i);
		$2 = ($2_type) i * sizeof(SQLWCHAR);
	} else {
		SQLUINTEGER u;
		u = (SQLUINTEGER) PyInt_AsUnsignedLongMask($input);
		if (!PyErr_Occurred()) {
			$1 = ($1_type) (size_t) u;
			$2 = 0;
		} else {
			SQLHANDLE h;
			PyErr_Clear();
			if (SWIG_ConvertPtr($input, &h, $descriptor(SQLHANDLE *), 0) == 0) {
				$1 = ($1_type) &h;
				$2 = 0;
			} else {
				PyErr_SetString(ErrorObject, "bad value type");
				SWIG_fail;
			}
		}
	}
}
%typemap(argout) (SQLPOINTER rgbValue, SQLINTEGER cbValue) {
	if ($2 > 0) PyMem_Free($1); /* > 0 implies unicode, i.e. malloced */
}

// SQLSetConnectOption, SQLSetStmtOption
%typemap(in, numinputs=1) (SQLULEN Value) {
	if (PyString_Check($input)) {
		$1 = ($1_type) (size_t) PyString_AsString($input);
	} else {
		SQLUINTEGER u;
		u = (SQLUINTEGER) PyInt_AsUnsignedLongMask($input);
		if (!PyErr_Occurred()) {
			$1 = ($1_type) u;
		} else {
			PyErr_Clear();
			PyErr_SetString(ErrorObject, "bad value type");
			SWIG_fail;
		}
	}
}
// SQLSetConnectOptionW, SQLSetStmtOptionW
%ignore SQLSetConnectOptionA;
%ignore SQLSetStmtOptionA;
%typemap(in, numinputs=1) (SQLULEN vParam) (SQLUINTEGER u) {
	if (PyUnicode_Check($input)) {
		$1 = ($1_type) (size_t) SqlWChar_FromPyUnicode($input, NULL);
	} else {
		SQLUINTEGER u;
		u = (SQLUINTEGER) PyInt_AsUnsignedLongMask($input);
		if (!PyErr_Occurred()) {
			$1 = ($1_type) u;
		} else {
			PyErr_Clear();
			PyErr_SetString(ErrorObject, "bad value type");
			SWIG_fail;
		}
	}
}

// SQLSetDescField, SQLSetDescFieldA, SQLSetDescFieldW
%typemap(in, numinputs=1) (SQLPOINTER Value, SQLINTEGER BufferLength) {
	if (PyUnicode_Check($input)) {
		int i;
		$1 = ($1_type) SqlWChar_FromPyUnicode($input, &i);
		$2 = ($2_type) i;
	} else {
		if (PyString_Check($input)) {
			$1 = ($1_type) PyString_AsString($input);
			$2 = PyString_Size($input);
		} else {
			SQLUINTEGER u;
			u = (SQLUINTEGER) PyInt_AsUnsignedLongMask($input);
			if (!PyErr_Occurred()) {
				$1 = ($1_type) (size_t) u;
				$2 = 0;
			} else {
				PyErr_Clear();
				PyErr_SetString(ErrorObject, "bad value type");
				SWIG_fail;
			}
		}
	}
}

// SQLSetDescRec
%typemap(in, numinputs=0) (SQLPOINTER Data) {
	$1 = NULL;
}
%typemap(in, numinputs=0) (SQLLEN *StringLength) {
	$1 = NULL;
}
%apply SQLLEN *StringLength {SQLLEN *Indicator};

// SQLSetCursorName
%apply (char *STRING, int LENGTH) {(SQLCHAR *CursorName, SQLSMALLINT NameLength)};
// SQLSetCursorNameA
%apply (char *STRING, int LENGTH) {(SQLCHAR *szCursor, SQLSMALLINT cbCursor)};
// SQLSetCursorNameW
%apply (SQLWCHAR *STRING, int LENGTH) {(SQLWCHAR *szCursor, SQLSMALLINT cbCursor)};

%ignore SQLBindCol;		/* can't be implemented in Python */
%ignore SQLBindParam;		/* not part of ODBC */
%ignore SQLBindParameter;
%ignore SQLSetParam;

/*
  Generate exception for failed call
*/
// It would be nice if we could also not return the result value
%{
#define CheckResult(res,tpe,hnd)					\
	if (res == SQL_ERROR) {				  		\
		SQLCHAR msg[256], state[6];				\
		SQLSMALLINT len;					\
		PyObject *errobj;					\
		SQLGetDiagRec(tpe, hnd, 1, state, NULL, msg, 256, &len); \
		errobj = Py_BuildValue("ss", (char *) msg, (char *) state); \
		PyErr_SetObject(ErrorObject, errobj);			\
		Py_XDECREF(errobj);					\
		return NULL;						\
	}								\
	if (res == SQL_INVALID_HANDLE) {				\
		PyErr_SetString(ErrorObject, "Invalid handle");		\
		return NULL;						\
	}
%}
%exception SQLAllocHandle {
	$action
	if (result == SQL_INVALID_HANDLE) {
		PyErr_SetString(ErrorObject, "Invalid handle");
		return NULL;
	}
	if (result == SQL_ERROR) {
		SQLCHAR msg[256], state[6];
		SQLSMALLINT len;
		PyObject *errobj;
		SQLGetDiagRec(arg1==SQL_HANDLE_DBC?SQL_HANDLE_ENV:SQL_HANDLE_DBC, arg2, 1, state, NULL, msg, 256, &len);
		errobj = Py_BuildValue("ss", (char *) msg, (char *) state);
		PyErr_SetObject(ErrorObject, errobj);
		Py_XDECREF(errobj);
		return NULL;
	}
}
%exception SQLAllocEnv {
	$action
	if (result == SQL_ERROR || result == SQL_INVALID_HANDLE) {
		PyErr_SetString(ErrorObject, "Invalid handle");
		return NULL;
	}
}
%exception SQLAllocConnect {
	$action
	CheckResult(result, SQL_HANDLE_ENV, arg1)
}
%exception SQLAllocStmt {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLBrowseConnect {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLBulkOperations {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLCancel {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLCloseCursor {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLColAttribute {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLColumnPrivileges {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLColumns {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLConnect {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLCopyDesc {
	$action
	CheckResult(result, SQL_HANDLE_DESC, arg1)
}
%exception SQLDataSources {
	$action
	CheckResult(result, SQL_HANDLE_ENV, arg1)
	if (result == SQL_NO_DATA) {
		Py_INCREF(Py_None);
		return Py_None;
	}
}
%exception SQLDescribeCol {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLDescribeParam {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLDisconnect {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLDriverConnect {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLDrivers {
	$action
	CheckResult(result, SQL_HANDLE_ENV, arg1)
}
%exception SQLEndTran {
	$action
	CheckResult(result, arg1, arg2)
}
%exception SQLExecDirect {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLExecute {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLExtendedFetch {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLFetch {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLFetchScroll {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLForeignKeys {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLFreeConnect {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLFreeEnv {
	$action
	CheckResult(result, SQL_HANDLE_ENV, arg1)
}
%exception SQLFreeHandle {
	$action
	CheckResult(result, arg1, arg2)
}
%exception SQLFreeStmt {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLGetConnectAttr {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLGetConnectOption {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLGetCursorName {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLGetData {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
	if (result == SQL_NO_DATA) {
		Py_INCREF(Py_None);
		return Py_None;
	}
}
%exception SQLGetDescField {
	$action
	CheckResult(result, SQL_HANDLE_DESC, arg1)
}
%exception SQLGetDescRec {
	$action
	CheckResult(result, SQL_HANDLE_DESC, arg1)
}
%exception SQLGetEnvAttr {
	$action
	CheckResult(result, SQL_HANDLE_ENV, arg1)
}
%exception SQLGetFunctions {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLGetInfo {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLGetStmtAttr {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLGetStmtOption {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLGetTypeInfo {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLMoreResults {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLNativeSql {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLNumParams {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLNumResultCols {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLParamData {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLParamOptions {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLPrepare {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLPrimaryKeys {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLProcedureColumns {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLProcedures {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLPutData {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLRowCount {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLSetConnectAttr {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLSetConnectOption {
	$action
	CheckResult(result, SQL_HANDLE_DBC, arg1)
}
%exception SQLSetCursorName {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLSetDescField {
	$action
	CheckResult(result, SQL_HANDLE_DESC, arg1)
}
%exception SQLSetDescRec {
	$action
	CheckResult(result, SQL_HANDLE_DESC, arg1)
}
%exception SQLSetEnvAttr {
	$action
	CheckResult(result, SQL_HANDLE_ENV, arg1)
}
%exception SQLSetPos {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLSetScrollOptions {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLSetStmtAttr {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLSetStmtOption {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLSpecialColumns {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLStatistics {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLTablePrivileges {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLTables {
	$action
	CheckResult(result, SQL_HANDLE_STMT, arg1)
}
%exception SQLTransact {
	$action
	if (arg1 == Py_None) {
		CheckResult(result, SQL_HANDLE_ENV, arg2)
	} else {
		CheckResult(result, SQL_HANDLE_DBC, arg1)
	}
}

%ignore __SQL_H;
%include "sql.h"

%ignore __SQLEXT_H;
%ignore SQLAllocHandleStd;
%ignore FireVSDebugEvent;
%ignore ODBC_VS_FLAG_RETCODE;
%ignore ODBC_VS_FLAG_STOP;
%ignore ODBC_VS_FLAG_UNICODE_ARG;
%ignore ODBC_VS_FLAG_UNICODE_COR;
%ignore TraceCloseLogFile;
%ignore TRACE_ON;
%ignore TraceOpenLogFile;
%ignore TraceReturn;
%ignore TraceVersion;
%ignore TRACE_VERSION;
%ignore TraceVSControl;
%ignore TRACE_VS_EVENT_ON;

%include "sqlext.h"

%ignore __SQLUCODE_H;
%ignore SQLSetStmtOptionA;
%ignore SQLGetStmtOptionA;
%ignore SQLPrepareA;
%ignore SQLDescribeParamA;
%include "sqlucode.h"
