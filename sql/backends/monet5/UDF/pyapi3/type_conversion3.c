/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "type_conversion.h"
#include "unicode.h"

#if PY_MINOR_VERSION >= 11
#include <cpython/longintrepr.h>
#else
#include <longintrepr.h>
#endif

bool pyapi3_string_copy(const char *source, char *dest, size_t max_size, bool allow_unicode)
{
	size_t i;
	for (i = 0; i < max_size; i++) {
		dest[i] = source[i];
		if (dest[i] == 0)
			return TRUE;
		if (!allow_unicode && source[i] & 0x80)
			return FALSE;
	}
	dest[max_size] = '\0';
	return TRUE;
}

#ifdef HAVE_HGE
int hge_to_string(char *str, hge x)
{
	size_t len = 256; /* assume str is large enough */
	hgeToStr(&str, &len, &x, false);
	return TRUE;
}

PyObject *PyLong_FromHge(hge h)
{
	PyLongObject *z;
	size_t size = 0;
	hge shift = h >= 0 ? h : -h;
	hge prev = shift;
	int i;
	while (shift > 0) {
		size++;
		shift = shift >> PyLong_SHIFT;
	}
	z = _PyLong_New(size);
	for (i = size - 1; i >= 0; i--) {
		digit result = (digit)(prev >> (PyLong_SHIFT * i));
		prev = prev - ((prev >> (PyLong_SHIFT * i)) << (PyLong_SHIFT * i));
		z->ob_digit[i] = result;
	}
	if (h < 0) {
#ifdef Py_SET_SIZE
		Py_SET_SIZE(z, -Py_SIZE(z));
#else
		Py_SIZE(z) = -(Py_SIZE(z));
#endif
	}
	return (PyObject *)z;
}
#endif

size_t pyobject_get_size(PyObject *obj)
{
	size_t size = 256;

	if (PyByteArray_CheckExact(obj)) {
		size = Py_SIZE(obj); // Normal strings are 1 byte per character
	} else if (PyUnicode_CheckExact(obj)) {
		size = Py_SIZE(obj) * 4; // UTF32 is 4 bytes per character
	}
	return size;
}

str pyobject_to_date(PyObject **ptr, size_t maxsize, date *value) {
	str msg = MAL_SUCCEED;

	if (ptr == NULL || *ptr == NULL) {
		msg = createException(MAL, "pyapi3.eval", "Invalid PyObject.");
		goto wrapup;
	}

	(void) maxsize;

	USE_DATETIME_API;
	if(PyDate_Check(*ptr)) {
		*value = date_create(PyDateTime_GET_YEAR(*ptr),
							 PyDateTime_GET_MONTH(*ptr),
							 PyDateTime_GET_DAY(*ptr));
	}
	else {
		msg = createException(MAL, "pyapi3.eval", "Invalid PyDate object.");
	}

 wrapup:
	return msg;
}

str pyobject_to_daytime(PyObject **ptr, size_t maxsize, daytime *value) {
	str msg = MAL_SUCCEED;

	if (ptr == NULL || *ptr == NULL) {
		msg = createException(MAL, "pyapi3.eval", "Invalid PyObject.");
		goto wrapup;
	}

	(void) maxsize;

	USE_DATETIME_API;
	if(PyTime_Check(*ptr)) {
		*value = daytime_create(PyDateTime_TIME_GET_HOUR(*ptr),
								PyDateTime_TIME_GET_MINUTE(*ptr),
								PyDateTime_TIME_GET_SECOND(*ptr),
								PyDateTime_TIME_GET_MICROSECOND(*ptr));
	}
	else {
		msg = createException(MAL, "pyapi3.eval", "Invalid PyTime object.");
	}

 wrapup:
	return msg;
}

str pyobject_to_timestamp(PyObject **ptr, size_t maxsize, timestamp *value) {
	str msg = MAL_SUCCEED;

	if (ptr == NULL || *ptr == NULL) {
		msg = createException(MAL, "pyapi3.eval", "Invalid PyObject.");
		goto wrapup;
	}

	(void) maxsize;

	USE_DATETIME_API;
	if(PyDateTime_Check(*ptr)) {
		date dt = date_create(PyDateTime_GET_YEAR(*ptr),
							  PyDateTime_GET_MONTH(*ptr),
							  PyDateTime_GET_DAY(*ptr));
		daytime dtm = daytime_create(PyDateTime_DATE_GET_HOUR(*ptr),
									 PyDateTime_DATE_GET_MINUTE(*ptr),
									 PyDateTime_DATE_GET_SECOND(*ptr),
									 PyDateTime_DATE_GET_MICROSECOND(*ptr));
		*value = timestamp_create(dt, dtm);
	}
	else {
		msg = createException(MAL, "pyapi3.eval", "Invalid PyDateTime object.");
	}

 wrapup:
	return msg;
}

str pyobject_to_blob(PyObject **ptr, size_t maxsize, blob **value) {
	size_t size;
	char* bytes_data;
	PyObject *obj;
	str msg = MAL_SUCCEED;
	if (ptr == NULL || *ptr == NULL) {
		msg = createException(MAL, "pyapi3.eval", "Invalid PyObject.");
		goto wrapup;
	}
	obj = *ptr;

	(void)maxsize;
	if (PyByteArray_CheckExact(obj)) {
		size = PyByteArray_Size(obj);
		bytes_data = ((PyByteArrayObject *)obj)->ob_bytes;
	} else {
		msg = createException(
			MAL, "pyapi3.eval",
			"Unrecognized Python object. Could not convert to blob.\n");
		goto wrapup;
	}

	*value = GDKmalloc(sizeof(blob) + size + 1);
	if (!*value) {
		msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto wrapup;
	}
	(*value)->nitems = size;
	memcpy((*value)->data, bytes_data, size);
wrapup:
	return msg;
}

str pyobject_to_str(PyObject **ptr, size_t maxsize, str *value)
{
	PyObject *obj;
	str msg = MAL_SUCCEED;
	str utf8_string = NULL;
	size_t len = 0;

	if (ptr == NULL || *ptr == NULL) {
		msg = createException(MAL, "pyapi3.eval", SQLSTATE(PY000) "Invalid PyObject.");
		goto wrapup;
	}
	obj = *ptr;

	utf8_string = *value;
	if (!utf8_string) {
		utf8_string = (str)malloc(len = (pyobject_get_size(obj) + 1));
		if (!utf8_string) {
			msg = createException(MAL, "pyapi3.eval",
								  SQLSTATE(HY013) MAL_MALLOC_FAIL "python string");
			goto wrapup;
		}
		*value = utf8_string;
	} else {
		len = maxsize;
	}

	if (PyByteArray_CheckExact(obj)) {
		char *str = ((PyByteArrayObject *)obj)->ob_bytes;
		if (!pyapi3_string_copy(str, utf8_string, len-1, false)) {
			msg = createException(MAL, "pyapi3.eval",
				  SQLSTATE(PY000) "Invalid string encoding used. Please return "
						  "a regular ASCII string, or a Numpy_Unicode "
						  "object.\n");
			goto wrapup;
		}
	} else if (PyUnicode_CheckExact(obj)) {
		const char *str = PyUnicode_AsUTF8(obj);
		if (!pyapi3_string_copy(str, utf8_string, len-1, true)) {
			msg = createException(MAL, "pyapi3.eval",
				  SQLSTATE(PY000) "Invalid string encoding used. Please return "
						  "a regular ASCII string, or a Numpy_Unicode "
						  "object.\n");
			goto wrapup;
		}
	} else if (PyBool_Check(obj) || PyLong_Check(obj) || PyFloat_Check(obj)) {
#ifdef HAVE_HGE
		hge h;
		pyobject_to_hge(&obj, 0, &h);
		hge_to_string(utf8_string, h);
#else
		lng h;
		pyobject_to_lng(&obj, 0, &h);
		snprintf(utf8_string, utf8string_minlength, LLFMT, h);
#endif
	} else {
		msg = createException(
			MAL, "pyapi3.eval",
			SQLSTATE(PY000) "Unrecognized Python object. Could not convert to NPY_UNICODE.\n");
		goto wrapup;
	}
wrapup:
	return msg;
}

#define STRING_TO_NUMBER_FACTORY(tpe)                                          \
	str str_to_##tpe(const char *ptr, size_t maxsize, tpe *value)              \
	{                                                                          \
		size_t len = sizeof(tpe);                                              \
		char buf[256];                                                         \
		if (maxsize > 0) {                                                     \
			if (maxsize >= sizeof(buf))                                        \
				maxsize = sizeof(buf) - 1;                                     \
			strncpy(buf, ptr, maxsize);                                        \
			buf[maxsize] = 0;                                                  \
			if (strlen(buf) >= sizeof(buf) - 1)                                \
				return GDKstrdup("string too long to convert.");               \
			ptr = buf;                                                         \
		}                                                                      \
		if (BATatoms[TYPE_##tpe].atomFromStr(ptr, &len, (void **)&value, false) < 0) \
			return GDKstrdup("Error converting string.");                      \
		return MAL_SUCCEED;                                                    \
	}

str str_to_date(const char *ptr, size_t maxsize, date *value)
{
	(void)ptr;
	(void)maxsize;
	(void)value;

	return GDKstrdup("Implicit conversion of string to date is not allowed.");
}

str unicode_to_date(Py_UNICODE *ptr, size_t maxsize, date *value)
{
	(void)ptr;
	(void)maxsize;
	(void)value;

	return GDKstrdup("Implicit conversion of string to date is not allowed.");
}

str str_to_daytime(const char *ptr, size_t maxsize, daytime *value)
{
	(void)ptr;
	(void)maxsize;
	(void)value;

	return GDKstrdup("Implicit conversion of string to daytime is not allowed.");
}

str unicode_to_daytime(Py_UNICODE *ptr, size_t maxsize, daytime *value)
{
	(void)ptr;
	(void)maxsize;
	(void)value;

	return GDKstrdup("Implicit conversion of string to daytime is not allowed.");
}

str str_to_timestamp(const char *ptr, size_t maxsize, timestamp *value)
{
	(void)ptr;
	(void)maxsize;
	(void)value;

	return GDKstrdup("Implicit conversion of string to timestamp is not allowed.");
}

str unicode_to_timestamp(Py_UNICODE *ptr, size_t maxsize, timestamp *value)
{
	(void)ptr;
	(void)maxsize;
	(void)value;

	return GDKstrdup("Implicit conversion of string to timestamp is not allowed.");
}


#define PY_TO_(type, inttpe)						\
str pyobject_to_##type(PyObject **pyobj, size_t maxsize, type *value)	\
{									\
	PyObject *ptr = *pyobj;						\
	str retval = MAL_SUCCEED;						\
	(void) maxsize;							\
	if (PyLong_CheckExact(ptr)) {					\
		PyLongObject *p = (PyLongObject*) ptr;				\
		inttpe h = 0;							\
		inttpe prev = 0;						\
		Py_ssize_t i = Py_SIZE(p);						\
		int sign = i < 0 ? -1 : 1;					\
		i *= sign;							\
		while (--i >= 0) {						\
			prev = h; (void)prev;					\
			h = (h << PyLong_SHIFT) + p->ob_digit[i];			\
			if ((h >> PyLong_SHIFT) != prev) {				\
				return GDKstrdup("Overflow when converting value.");	\
			}								\
		}								\
		*value = (type)(h * sign);					\
	} else if (PyBool_Check(ptr)) {					\
		*value = ptr == Py_True ? (type) 1 : (type) 0;			\
	} else if (PyFloat_CheckExact(ptr)) {				\
		*value = isnan(((PyFloatObject*)ptr)->ob_fval) ? type##_nil : (type) ((PyFloatObject*)ptr)->ob_fval; \
	} else if (PyUnicode_CheckExact(ptr)) {				\
		return str_to_##type(PyUnicode_AsUTF8(ptr), 0, value);		\
	}  else if (PyByteArray_CheckExact(ptr)) {				\
		return str_to_##type(((PyByteArrayObject*)ptr)->ob_bytes, 0, value); \
	}  else if (ptr == Py_None) {					\
		*value = type##_nil;						\
	}									\
	return retval;							\
}

#define CONVERSION_FUNCTION_FACTORY(tpe, inttpe)            \
	STRING_TO_NUMBER_FACTORY(tpe)                   \
	str unicode_to_##tpe(Py_UNICODE *ptr, size_t maxsize, tpe *value)   \
	{                                   \
		char utf8[1024];                        \
	if (maxsize == 0)                       \
			maxsize = utf32_strlen(ptr);                \
	if (maxsize > 255)                      \
			maxsize = 255;                      \
		unicode_to_utf8(0, maxsize, utf8, ptr);             \
		return str_to_##tpe(utf8, 0, value);                \
	}                                   \
	PY_TO_(tpe, inttpe);

CONVERSION_FUNCTION_FACTORY(bte, bte)
CONVERSION_FUNCTION_FACTORY(oid, oid)
CONVERSION_FUNCTION_FACTORY(bit, bit)
CONVERSION_FUNCTION_FACTORY(sht, sht)
CONVERSION_FUNCTION_FACTORY(int, int)
CONVERSION_FUNCTION_FACTORY(lng, lng)
CONVERSION_FUNCTION_FACTORY(flt, lng)

#ifdef HAVE_HGE
CONVERSION_FUNCTION_FACTORY(hge, hge)
CONVERSION_FUNCTION_FACTORY(dbl, hge)
#else
CONVERSION_FUNCTION_FACTORY(dbl, lng)
#endif

void _typeconversion_init(void) { _import_array(); }
