%module odbc
%include "typemaps.i"
%{
#include <sqltypes.h>
#include <sqlucode.h>
%}

%ignore __SQLTYPES_H;
%include "/usr/include/sqltypes.h"

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
// SQLAllocConnect
%apply SQLHANDLE *OUTPUT {SQLHDBC *ConnectionHandle};
// SQLAllocEnv
%apply SQLHANDLE *OUTPUT {SQLHENV *EnvironmentHandle};
#if (ODBCVER >= 0x0300)
// SQLAllocHandle
%apply SQLHANDLE *OUTPUT {SQLHANDLE *OutputHandle};
#endif
// SQLAllocStmt
%apply SQLHANDLE *OUTPUT {SQLHSTMT *StatementHandle};

/*
  output arg to return a 5-byte state string
  used only in SQLError and SQLGetDiagRec
*/
%typemap(in, numinputs=0) SQLCHAR *Sqlstate (SQLCHAR tempbuf[6]) {
	$1 = tempbuf;
}
%typemap(argout,fragment="t_output_helper") SQLCHAR *Sqlstate {
	PyObject *o = PyString_FromStringAndSize($1, 5);
	$result = t_output_helper($result, o);
}

// SQLError, SQLGetDiagRec
%apply int *OUTPUT {SQLINTEGER *NativeError};
// SQLDescribeCol
%apply int *OUTPUT {SQLSMALLINT *DataType};
%apply int *OUTPUT {SQLULEN *ColumnSize};
%apply int *OUTPUT {SQLSMALLINT *DecimalDigits};
%apply int *OUTPUT {SQLSMALLINT *Nullable};

/*
  three args to return a string: MessageText points to a buffer of
  length BufferLength which is filled with *TextLength characters plus
  a NUL byte.  If the buffer is too small, *TextLength returns the
  required value.
  The arg names are for SQLError and SQLGetDiagRec, but the pattern
  occurs in other functions as well.
*/
%typemap(in, numinputs=0) (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) (SQLCHAR msgbuf[1024], SQLSMALLINT msglen) {
	$1 = msgbuf;
	$2 = (SQLSMALLINT) sizeof(msgbuf);
	$3 = &msglen;
}
%typemap(argout,fragment="t_output_helper") (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
	PyObject *o = PyString_FromStringAndSize($1, *$3 >= $2 ? $2 - 1 : *$3);
	$result = t_output_helper($result, o);
}
// SQLDataSources
%apply (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) { (SQLCHAR *ServerName, SQLSMALLINT BufferLength1, SQLSMALLINT *NameLength1) };
%apply (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) { (SQLCHAR *Description, SQLSMALLINT BufferLength2, SQLSMALLINT *NameLength2) };
// SQLDescribeCol
%apply (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) { (SQLCHAR *ColumnName, SQLSMALLINT BufferLength, SQLSMALLINT *NameLength) };
// SQLGetCursorName
%apply (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) { (SQLCHAR *CursorName, SQLSMALLINT BufferLength, SQLSMALLINT *NameLength) };
// SQLGetDescRec
%apply (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) { (SQLCHAR *Name, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) };

// SQLColumns, SQLSpecialColumns, SQLStatistics, SQLTables
%apply (char *STRING, int LENGTH) { (SQLCHAR *CatalogName, SQLSMALLINT NameLength1) };
%apply (char *STRING, int LENGTH) { (SQLCHAR *SchemaName, SQLSMALLINT NameLength2) };
%apply (char *STRING, int LENGTH) { (SQLCHAR *TableName, SQLSMALLINT NameLength3) };
%apply (char *STRING, int LENGTH) { (SQLCHAR *ColumnName, SQLSMALLINT NameLength4) };
%apply (char *STRING, int LENGTH) { (SQLCHAR *TableType, SQLSMALLINT NameLength4) };

// SQLConnect
%apply (char *STRING, int LENGTH) { (SQLCHAR *ServerName, SQLSMALLINT NameLength1) };
%apply (char *STRING, int LENGTH) { (SQLCHAR *UserName, SQLSMALLINT NameLength2) };
%apply (char *STRING, int LENGTH) { (SQLCHAR *Authentication, SQLSMALLINT NameLength3) };

// SQLExecDirect, SQLPrepare
%apply (char *STRING, int LENGTH) { (SQLCHAR *StatementText, SQLINTEGER TextLength) };

// SQLSetCursorName
%apply (char *STRING, int LENGTH) { (SQLCHAR *CursorName, SQLSMALLINT NameLength) };

// SQLNumResultCols
%apply int *OUTPUT {SQLSMALLINT *ColumnCount};

// SQLRowCount
%apply int *OUTPUT {SQLLEN *RowCount};

/*
  SQLColAttribute is weird
*/
%typemap(in, numinputs=1) (SQLUSMALLINT FieldIdentifier, SQLPOINTER CharacterAttribute, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength, SQLPOINTER NumericAttribute) (char tempbuf[1024], SQLSMALLINT buflen, int temp)
	"$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = (SQLPOINTER) tempbuf;
	$3 = (SQLSMALLINT) sizeof(tempbuf);
	$4 = &buflen;
	$5 = (SQLPOINTER) &temp;";
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
		o = PyInt_FromLong((long) $5);
		break;
	default:
		o = PyString_FromStringAndSize((char *) $2, *$4 >= $3 ? $3 - 1 : *$4);
		break;
	}
	$result = t_output_helper($result, o);
}

/*
  Generate exception for failed call
*/
// It would be nice if we could also not return the result value
// %exception {
//     $action
//     if (result < 0) {
// 	PyErr_SetString(PyExc_RuntimeError, "error");
// 	return NULL;
//     }
// }

%ignore __SQL_H;
%include "/usr/include/sql.h"

%ignore __SQLEXT_H;
%ignore SQLAllocHandleStd;
%ignore FireVSDebugEvent;
%ignore ODBC_VS_FLAG_RETCODE;
%ignore ODBC_VS_FLAG_STOP;
%ignore ODBC_VS_FLAG_UNICODE_ARG;
%ignore ODBC_VS_FLAG_UNICODE_COR;
%ignore SQLDrivers;
%ignore TraceCloseLogFile;
%ignore TRACE_ON;
%ignore TraceOpenLogFile;
%ignore TraceReturn;
%ignore TraceVersion;
%ignore TRACE_VERSION;
%ignore TraceVSControl;
%ignore TRACE_VS_EVENT_ON;

%include "/usr/include/sqlext.h"
