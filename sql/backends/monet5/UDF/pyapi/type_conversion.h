/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of helper functions for converting between types,
 * mainly used to convert from an object from a numpy array to the type
 * requested by the BAT.
 */

#ifndef _TYPE_CONVERSION_
#define _TYPE_CONVERSION_

#include <stdint.h>
#include <stddef.h>

#include "pyheader.h"

//! Copies the string of size up to max_size from the source to the destination,
//! returns FALSE if "source" is not a legal ASCII string (i.e. a character is
//! >= 128)
bool string_copy(char *source, char *dest, size_t max_size, bool allow_unicode);
#ifdef HAVE_HGE
//! Converts a hge to a string and writes it into the string "str"
int hge_to_string(char *str, hge);
//! Converts a base-10 string to a hge value
str str_to_hge(char *ptr, size_t maxsize, hge *value);
//! Converts a base-10 utf32-encoded string to a hge value
str unicode_to_hge(Py_UNICODE *utf32, size_t maxsize, hge *value);
//! Converts a PyObject to a hge value
str pyobject_to_hge(PyObject **ptr, size_t maxsize, hge *value);
//! Create a PyLongObject from a hge integer
PyObject *PyLong_FromHge(hge h);
#endif
//! Returns the minimum size needed when this python object is converted to a
//! string
size_t pyobject_get_size(PyObject *obj);
//! Converts a PyObject to a str; the output string will be a newly allocated
//! string (if *value == NULL) or stored in *value (if *value != NULL)
str pyobject_to_str(PyObject **ptr, size_t maxsize, str *value);
//! Converts a PyObject to a blob
str pyobject_to_blob(PyObject **ptr, size_t maxsize, blob **value);

//using macros, create a number of str_to_<type>, unicode_to_<type> and pyobject_to_<type> functions (we are Java now)
#define CONVERSION_FUNCTION_HEADER_FACTORY(tpe)          \
    str str_to_##tpe(char *ptr, size_t maxsize, tpe *value);          \
    str unicode_to_##tpe(Py_UNICODE *ptr, size_t maxsize, tpe *value);                  \
    str pyobject_to_##tpe(PyObject **ptr, size_t maxsize, tpe *value);

CONVERSION_FUNCTION_HEADER_FACTORY(bte)
CONVERSION_FUNCTION_HEADER_FACTORY(oid)
CONVERSION_FUNCTION_HEADER_FACTORY(bit)
CONVERSION_FUNCTION_HEADER_FACTORY(sht)
CONVERSION_FUNCTION_HEADER_FACTORY(int)
CONVERSION_FUNCTION_HEADER_FACTORY(lng)
CONVERSION_FUNCTION_HEADER_FACTORY(flt)
CONVERSION_FUNCTION_HEADER_FACTORY(dbl)

void _typeconversion_init(void);

#endif /* _TYPE_CONVERSION_ */
