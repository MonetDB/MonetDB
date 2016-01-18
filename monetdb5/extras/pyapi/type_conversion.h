/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of helper functions for converting between types, mainly used to convert from an object from a numpy array to the type requested by the BAT.
 */

#ifndef _TYPE_CONVERSION_
#define _TYPE_CONVERSION_

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "monetdb_config.h"
#include "mal.h"
#include "mal_stack.h"
#include "mal_linker.h"
#include "gdk_utils.h"
#include "gdk.h"

#undef _GNU_SOURCE
#undef _XOPEN_SOURCE
#undef _POSIX_C_SOURCE
#include <Python.h>



//! Copies the string of size up to max_size from the source to the destination, returns FALSE if "source" is not a legal ASCII string (i.e. a character is >= 128)
bool string_copy(char * source, char* dest, size_t max_size);
#ifdef HAVE_HGE
//! Converts a hge to a string and writes it into the string "str"
int hge_to_string(char *str, hge );
//! Converts a base-10 string to a hge value
bool str_to_hge(char *ptr, size_t maxsize, hge *value);
//! Converts a base-10 utf32-encoded string to a hge value
bool unicode_to_hge(Py_UNICODE *utf32, size_t maxsize, hge *value);
//! Converts a PyObject to a hge value
bool pyobject_to_hge(PyObject **ptr, size_t maxsize, hge *value);
//! Create a PyLongObject from a hge integer
PyObject *PyLong_FromHge(hge h);
#endif


//using macros, create a number of str_to_<type>, unicode_to_<type> and pyobject_to_<type> functions (we are Java now)
#define CONVERSION_FUNCTION_HEADER_FACTORY(tpe)          \
    bool str_to_##tpe(char *ptr, size_t maxsize, tpe *value);          \
    bool unicode_to_##tpe(Py_UNICODE *ptr, size_t maxsize, tpe *value);                  \
    bool pyobject_to_##tpe(PyObject **ptr, size_t maxsize, tpe *value);                  \

CONVERSION_FUNCTION_HEADER_FACTORY(bte)
CONVERSION_FUNCTION_HEADER_FACTORY(wrd)
CONVERSION_FUNCTION_HEADER_FACTORY(oid)
CONVERSION_FUNCTION_HEADER_FACTORY(bit)
CONVERSION_FUNCTION_HEADER_FACTORY(sht)
CONVERSION_FUNCTION_HEADER_FACTORY(int)
CONVERSION_FUNCTION_HEADER_FACTORY(lng)
CONVERSION_FUNCTION_HEADER_FACTORY(flt)
CONVERSION_FUNCTION_HEADER_FACTORY(dbl)

#endif /* _TYPE_CONVERSION_ */
