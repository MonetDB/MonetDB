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
%newobject SQLAllocConnect;
%apply SQLHANDLE *OUTPUT {SQLHDBC *ConnectionHandle};
// SQLAllocEnv
%newobject SQLAllocEnv;
%apply SQLHANDLE *OUTPUT {SQLHENV *EnvironmentHandle};
#if (ODBCVER >= 0x0300)
// SQLAllocHandle
%newobject SQLAllocHandle;
%apply SQLHANDLE *OUTPUT {SQLHANDLE *OutputHandle};
#endif
// SQLAllocStmt
%newobject SQLAllocStmt;
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

// SQLColAttribute
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

// SQLGetData
%typemap(in, numinputs=1) (SQLSMALLINT TargetType, SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind) (SQLLEN len, SQLCHAR tempbuf[2048]) {
	$1 = ($1_ltype) PyInt_AsLong($input);
	if (PyErr_Occurred()) SWIG_fail;
	$2 = tempbuf;
	$3 = sizeof(tempbuf);
	$4 = &len;
}
%typemap(argout,fragment="t_output_helper") (SQLSMALLINT TargetType, SQLPOINTER TargetValue, SQLLEN BufferLength, SQLLEN *StrLen_or_Ind) {
	PyObject *o;
	if (*$4 == SQL_NULL_DATA || *$4 == SQL_NO_DATA) {
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
	case SQL_C_BIT:
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
		o = PyInt_FromLong((long) * (SQLREAL *) $2);
		break;
	case SQL_C_DOUBLE:
		o = PyInt_FromLong((long) * (SQLDOUBLE *) $2);
		break;
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
	case SQL_C_INTERVAL_DAY_TO_HOUR:
	case SQL_C_INTERVAL_DAY_TO_MINUTE:
	case SQL_C_INTERVAL_DAY_TO_SECOND:
	case SQL_C_INTERVAL_HOUR_TO_MINUTE:
	case SQL_C_INTERVAL_HOUR_TO_SECOND:
	case SQL_C_INTERVAL_MINUTE_TO_SECOND:
		o = Py_BuildValue("(ii(iiid))", ((SQL_INTERVAL_STRUCT *) $2)->interval_type, ((SQL_INTERVAL_STRUCT *) $2)->interval_sign, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.day, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.hour, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.minute, ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.second + ((SQL_INTERVAL_STRUCT *) $2)->intval.day_second.fraction / 1000000000.);
		break;
	default:
		o = Py_None;
		Py_INCREF(Py_None);
		break;
	}
	$result = t_output_helper($result, o);
}

// SQLGetInfo
// InfoTypes SQL_DRIVER_HDESC and SQL_DRIVER_HSTMS use *InfoValue as
// input/output value.  This has not (yet) been implemented by this
// code.
%typemap(in, numinputs=1) (SQLUSMALLINT InfoType, SQLPOINTER InfoValue, SQLSMALLINT BufferLength, SQLSMALLINT *StringLength) (char tempbuf[1024], SQLSMALLINT len) {
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
		PyErr_SetString(PyExc_ValueError, "bad info type");
		SWIG_fail;
	}
	$result = t_output_helper($result, o);
}

%ignore SQLBindCol;		/* can't be implemented in Python */

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
