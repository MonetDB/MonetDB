%module odbc
%include "typemaps.i"
%{
#include <sqltypes.h>
#include <sqlucode.h>
%}

%include "/usr/include/sqltypes.h"

%typemap(in, numinputs=0) SQLHANDLE *OUTPUT (SQLHANDLE temp) {
   $1 = &temp;
}
%typemap(argout,fragment="t_output_helper") SQLHANDLE *OUTPUT {
   PyObject *o = SWIG_NewPointerObj((void *) *$1, $1_descriptor, 0);
   $result = t_output_helper($result, o);
}
%apply SQLHANDLE *OUTPUT {SQLHDBC *ConnectionHandle};
%apply SQLHANDLE *OUTPUT {SQLHENV *EnvironmentHandle};
%apply SQLHANDLE *OUTPUT {SQLHANDLE *OutputHandle};
%apply SQLHANDLE *OUTPUT {SQLHSTMT *StatementHandle};

// SQLError, SQLGetDiagRec
%typemap(in, numinputs=0) SQLCHAR *Sqlstate (SQLCHAR tempbuf[6]) {
	$1 = tempbuf;
}
%typemap(argout,fragment="t_output_helper") SQLCHAR *Sqlstate {
	PyObject *o = PyString_FromStringAndSize($1, 5);
	$result = t_output_helper($result, o);
}
%apply int *OUTPUT {SQLINTEGER *NativeError};
%typemap(in, numinputs=0) (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) (SQLCHAR msgbuf[1024], SQLSMALLINT msglen) {
	$1 = msgbuf;
	$2 = (SQLSMALLINT) sizeof(msgbuf);
	$3 = &msglen;
}
%typemap(argout,fragment="t_output_helper") (SQLCHAR *MessageText, SQLSMALLINT BufferLength, SQLSMALLINT *TextLength) {
	PyObject *o = PyString_FromStringAndSize($1, *$3 >= $2 ? $2 - 1 : *$3);
	$result = t_output_helper($result, o);
}

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

// It would be nice if we could also not return the result value
// %exception {
//     $action
//     if (result < 0) {
// 	PyErr_SetString(PyExc_RuntimeError, "error");
// 	return NULL;
//     }
// }

%include "/usr/include/sql.h"
