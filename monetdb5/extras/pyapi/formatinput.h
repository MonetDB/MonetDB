/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of functions for formatting the 
 */

#ifndef _PY_FORMAT_INPUT_LIB_
#define _PY_FORMAT_INPUT_LIB_

#include <stddef.h>

#undef _GNU_SOURCE
#undef _XOPEN_SOURCE
#undef _POSIX_C_SOURCE
#include <Python.h>
 
extern PyObject *marshal_loads;

char* FormatCode(char* code, char **args, size_t argcount, size_t tabwidth, PyObject **code_object, char **return_message);

#endif /* _PY_FORMAT_INPUT_LIB_ */
