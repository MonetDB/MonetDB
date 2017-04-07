/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file simply includes standard MonetDB and Python headers
 * Because this needs to be done in a specific order, and this needs to happen
 * in multiple places
 * We simplify our lives by only having to include this header.
 */

#ifndef _PYHEADER_H_
#define _PYHEADER_H_

#include "mal.h"
#include "mal_stack.h"
#include "mal_linker.h"
#include "gdk_atoms.h"
#include "gdk_utils.h"
#include "gdk_posix.h"
#include "gdk.h"
#include "sql_catalog.h"
#include "sql_scenario.h"
#include "sql_cast.h"
#include "sql_execute.h"
#include "sql_storage.h"

#include "undef.h"

// Python library
#undef _GNU_SOURCE
#undef _XOPEN_SOURCE
#undef _POSIX_C_SOURCE
#ifdef _DEBUG
#undef _DEBUG
#include <Python.h>
#define _DEBUG
#else
#include <Python.h>
#endif

// Numpy Library
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#ifdef __INTEL_COMPILER
// Intel compiler complains about trailing comma's in numpy source code,
#pragma warning(disable : 271)
#endif
#include <numpy/arrayobject.h>
#include <numpy/npy_common.h>

// DLL Export Flags
#ifdef WIN32
#ifndef LIBPYAPI
#define pyapi_export extern __declspec(dllimport)
#else
#define pyapi_export extern __declspec(dllexport)
#endif
#else
#define pyapi_export extern
#endif

// Fixes for Python 2 <> Python 3
#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#define PyString_FromString PyUnicode_FromString
#define PyString_Check PyUnicode_Check
#define PyString_CheckExact PyUnicode_CheckExact
#define PyString_AsString PyUnicode_AsUTF8
#define PyString_AS_STRING PyUnicode_AsUTF8
#define PyString_FromStringAndSize PyUnicode_FromStringAndSize
#define PyInt_FromLong PyLong_FromLong
#define PyInt_Check PyLong_Check
#define PythonUnicodeType char

#else
#define PythonUnicodeType Py_UNICODE
#endif

#if defined(WIN32) && !defined(HAVE_EMBEDDED)
// On Windows we need to dynamically load any SQL functions we use
// For embedded, this is not necessary because we create one large shared object
#define CREATE_SQL_FUNCTION_PTR(retval, fcnname)                               \
	typedef retval (*fcnname##_ptr_tpe)();                                     \
	fcnname##_ptr_tpe fcnname##_ptr = NULL;

#define LOAD_SQL_FUNCTION_PTR(fcnname)                                         \
	fcnname##_ptr =                                                            \
		(fcnname##_ptr_tpe)getAddress(NULL, "lib_sql.dll", #fcnname, 0);       \
	if (fcnname##_ptr == NULL) {                                               \
		msg = createException(MAL, "pyapi.eval", "Failed to load function %s", \
							  #fcnname);                                       \
	}
#else
#define CREATE_SQL_FUNCTION_PTR(retval, fcnname)                               \
	typedef retval (*fcnname##_ptr_tpe)();                                     \
	fcnname##_ptr_tpe fcnname##_ptr = (fcnname##_ptr_tpe)fcnname;

#define LOAD_SQL_FUNCTION_PTR(fcnname) (void)fcnname
#endif

#define utf8string_minlength 256

#endif /* _PYHEADER_H_ */
