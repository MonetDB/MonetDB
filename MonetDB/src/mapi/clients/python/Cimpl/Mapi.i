%module MapiLib
%include "typemaps.i"
%include "exception.i"

%{
#include "Mapi.h"
%}

// don't care for the guard symbol
%ignore _MAPI_H_INCLUDED;

// unimplementable in Python
%ignore mapi_bind;
%ignore mapi_bind_var;
%ignore mapi_bind_numeric;
%ignore mapi_clear_bindings;
%ignore mapi_param;
%ignore mapi_param_type;
%ignore mapi_param_numeric;
%ignore mapi_param_string;
%ignore mapi_clear_params;
%ignore mapi_store_field;
%ignore mapi_quote;
%ignore mapi_unquote;

%apply (char *STRING, int LENGTH) {(const char *cmd, size_t size)}; 

%typemap(in) (char **val) {
	int i, n;
	if (!PyList_Check($input)) {
		PyErr_SetString(PyExc_ValueError, "Expecting a list");
		return NULL;
	}
	n = PyList_Size($input);
	$1 = (char **) malloc((n + 1) * sizeof(char *));
	for (i = 0; i < n; i++) {
		PyObject *s = PyList_GetItem($input, i);
		if (!PyString_Check(s)) {
			free($1);
			PyErr_SetString(PyExc_ValueError, "List items must be strings");
			return NULL;
		}
		$1[i] = PyString_AsString(s);
	}
	$1[i] = 0;
}
%typemap(freearg) (char **val) {
	if ($1)
		free($1);
}

%typemap(in) (FILE *fd) {
	if (!PyFile_Check($input)) {
		PyErr_SetString(PyExc_ValueError, "Expecting a file");
		return NULL;
	}
	$1 = PyFile_AsFile($input);
}

// options string arg, i.e. arg can be a string or NULL (in Python: None).
%typemap(in,parse="z") char *OPTSTRING "";

%apply char *OPTSTRING {char *lang};
%apply char *OPTSTRING {char *password};
%apply char *OPTSTRING {char *username};
%apply char *OPTSTRING {char *host};

%apply char *OPTSTRING {char *cmd};

// %typemap(out) MapiMsg {
// 	switch ($1) {
// 	case MOK:
// 		$result = Py_None;
// 		Py_INCREF(Py_None);
// 		break;
// 	case MERROR:
// 		SWIG_exception(SWIG_RuntimeError, arg1?mapi_error_str(arg1):"function returned MERROR");
// 		break;
// 	case MTIMEOUT:
// 		SWIG_exception(SWIG_IOError, "function returned MTIMEOUT");
// 		break;
// 	default:
// 		$result = PyLong_FromLong((long) $1);
// 		break;
// 	}
// }

%typemap(out) char * {
	if ($1) {
		$result = PyString_FromString($1);
	} else {
		$result = Py_None;
		Py_INCREF(Py_None);
	}
}

%typemap(out) char ** {
	if ($1) {
		int i;
		for (i = 0; $1[i]; i++)
			;
		$result = PyList_New(i);
		if (!$result) SWIG_fail;
		for (i = 0; $1[i]; i++) {
			PyList_SetItem($result, i, PyString_FromString($1[i]));
		}
		if (PyErr_Occurred()) {
			Py_DECREF($result);
			$result = 0;
			SWIG_fail;
		}
	} else {
		$result = Py_None;
		Py_INCREF(Py_None);
	}
}

%include "Mapi.h"
