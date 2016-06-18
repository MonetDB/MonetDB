/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "type_conversion.h"
#include "unicode.h"

#include <longintrepr.h>

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#endif

bool string_copy(char * source, char* dest, size_t max_size, bool allow_unicode)
{
    size_t i;
    for(i = 0; i < max_size; i++)
    {
        dest[i] = source[i];
        if (dest[i] == 0) return TRUE;
        if (!allow_unicode && (*(unsigned char*)&source[i]) >= 128) return FALSE;
    }
    dest[max_size] = '\0';
    return TRUE;
}

#ifdef HAVE_HGE
int hge_to_string(char * str, hge x)
{
    int i = 0;
    size_t size = 1;
    hge cpy = x > 0 ? x : -x;
    while(cpy > 0) {
        cpy /= 10;
        size++;
    }
    if (x < 0) size++;
    if (x < 0)
    {
        x *= -1;
        str[0] = '-';
    }
    str[size - 1] = '\0';
    i = size - 1;
    while(x > 0)
    {
        int v = x % 10;
        i--;
        if (i < 0) return FALSE;
        if (v == 0)       str[i] = '0';
        else if (v == 1)  str[i] = '1';
        else if (v == 2)  str[i] = '2';
        else if (v == 3)  str[i] = '3';
        else if (v == 4)  str[i] = '4';
        else if (v == 5)  str[i] = '5';
        else if (v == 6)  str[i] = '6';
        else if (v == 7)  str[i] = '7';
        else if (v == 8)  str[i] = '8';
        else if (v == 9)  str[i] = '9';
        x = x / 10;
    }

    return TRUE;
}

PyObject *PyLong_FromHge(hge h)
{
    PyLongObject *z;
    size_t size = 0;
    hge shift = h >= 0 ? h : -h;
    hge prev = shift;
    int i;
    while(shift > 0) {
        size++;
        shift = shift >> PyLong_SHIFT;
    }
    z = _PyLong_New(size);
    for(i = size - 1; i >= 0; i--) {
        digit result = (digit)(prev >> (PyLong_SHIFT * i));
        prev = prev - ((prev >> (PyLong_SHIFT * i)) << (PyLong_SHIFT * i));
        z->ob_digit[i] = result;
    }
    if (h < 0) Py_SIZE(z) = -(Py_SIZE(z));
    return (PyObject*) z;
}
#endif


size_t pyobject_get_size(PyObject *obj) {
    size_t size = 256;
    if (PyString_CheckExact(obj) || PyByteArray_CheckExact(obj)) {
        size = Py_SIZE(obj);     //Normal strings are 1 string per character
    } else if (PyUnicode_CheckExact(obj)) {
        size = Py_SIZE(obj) * 4; //UTF32 is 4 bytes per character
    }
    return size;
}

str pyobject_to_str(PyObject **ptr, size_t maxsize, str *value) {
    PyObject *obj;
    str msg = MAL_SUCCEED;
    str utf8_string = NULL;

    (void) maxsize;

    if (ptr == NULL || *ptr == NULL) {
        msg = createException(MAL, "pyapi.eval", "Invalid PyObject.");
        goto wrapup;
    }
    obj = *ptr;

    utf8_string = *value;
    if (!utf8_string) {
        utf8_string = (str) malloc(pyobject_get_size(obj) * sizeof(char));
        if (!utf8_string) {
            msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL"python string");
            goto wrapup;
        }
        *value = utf8_string;
    }

#ifndef IS_PY3K
    if (PyString_CheckExact(obj)) {
        char *str = ((PyStringObject*)obj)->ob_sval;
        if (!string_copy(str, utf8_string, strlen(str) + 1, false)) {
            msg = createException(MAL, "pyapi.eval", "Invalid string encoding used. Please return a regular ASCII string, or a Numpy_Unicode object.\n");
            goto wrapup;
        }
    } else
#endif
    if (PyByteArray_CheckExact(obj)) {
        char *str = ((PyByteArrayObject*)obj)->ob_bytes;
        if (!string_copy(str, utf8_string, strlen(str) + 1, false)) {
            msg = createException(MAL, "pyapi.eval", "Invalid string encoding used. Please return a regular ASCII string, or a Numpy_Unicode object.\n");
            goto wrapup;
        }
    } else if (PyUnicode_CheckExact(obj)) {
#ifndef IS_PY3K
        Py_UNICODE *str = (Py_UNICODE*)((PyUnicodeObject*)obj)->str;
#if Py_UNICODE_SIZE >= 4
        utf32_to_utf8(0, ((PyUnicodeObject*)obj)->length, utf8_string, str);
#else
        ucs2_to_utf8(0, ((PyUnicodeObject*)obj)->length, utf8_string, str);
#endif
#else
        char *str = PyUnicode_AsUTF8(obj);
        if (!string_copy(str, utf8_string, strlen(str) + 1, true)) {
            msg = createException(MAL, "pyapi.eval", "Invalid string encoding used. Please return a regular ASCII string, or a Numpy_Unicode object.\n");
            goto wrapup;
        }
#endif
    } else if (PyBool_Check(obj) || PyLong_Check(obj) || PyInt_Check(obj) || PyFloat_Check(obj)) {
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
        msg = createException(MAL, "pyapi.eval", "Unrecognized Python object. Could not convert to NPY_UNICODE.\n");
        goto wrapup;
    }
wrapup:
    return msg;
}

#ifndef IS_PY3K
#define PY_TO_(type, inttpe)                                                                                      \
str pyobject_to_##type(PyObject **pyobj, size_t maxsize, type *value)                                             \
{                                                                                                                 \
    PyObject *ptr = *pyobj;                                                                                       \
    str retval = MAL_SUCCEED;                                                                                     \
    (void) maxsize;                                                                                               \
    if (PyLong_CheckExact(ptr)) {                                                                                 \
        PyLongObject *p = (PyLongObject*) ptr;                                                                    \
        inttpe h = 0;                                                                                             \
        inttpe prev = 0;                                                                                          \
        ssize_t i = Py_SIZE(p);                                                                                   \
        int sign = i < 0 ? -1 : 1;                                                                                \
        i *= sign;                                                                                                \
        while (--i >= 0) {                                                                                        \
            prev = h; (void)prev;                                                                                 \
            h = (h << PyLong_SHIFT) + p->ob_digit[i];                                                             \
            if ((h >> PyLong_SHIFT) != prev) {                                                                    \
                return GDKstrdup("Overflow when converting value.");                                              \
            }                                                                                                     \
        }                                                                                                         \
        *value = (type)(h * sign);                                                                                \
    } else if (PyInt_CheckExact(ptr) || PyBool_Check(ptr)) {                                                      \
        *value = (type)((PyIntObject*)ptr)->ob_ival;                                                              \
    } else if (PyFloat_CheckExact(ptr)) {                                                                         \
        *value = (type) ((PyFloatObject*)ptr)->ob_fval;                                                           \
    } else if (PyString_CheckExact(ptr)) {                                                                        \
        return str_to_##type(((PyStringObject*)ptr)->ob_sval, -1, value);                                         \
    }  else if (PyByteArray_CheckExact(ptr)) {                                                                    \
        return str_to_##type(((PyByteArrayObject*)ptr)->ob_bytes, -1, value);                                     \
    } else if (PyUnicode_CheckExact(ptr)) {                                                                       \
        return unicode_to_##type(((PyUnicodeObject*)ptr)->str, -1, value);                                        \
    } else if (ptr == Py_None) {                                                                                  \
        *value = type##_nil;                                                                                      \
    }                                                                                                             \
    return retval;                                                                                                \
}
#define CONVERSION_FUNCTION_FACTORY(tpe, inttpe)              \
    str str_to_##tpe(char *ptr, size_t maxsize, tpe *value) \
    { \
        ssize_t i = maxsize - 1; \
        tpe factor = 1; \
        if (i < 0) i = strlen(ptr) - 1; \
        *value = 0;  \
        for( ; i >= 0; i--) \
        { \
            switch(ptr[i]) \
            { \
                case '0': break; \
                case '1': *value += factor; break; \
                case '2': *value += 2 * factor; break; \
                case '3': *value += 3 * factor; break; \
                case '4': *value += 4 * factor; break; \
                case '5': *value += 5 * factor; break; \
                case '6': *value += 6 * factor; break; \
                case '7': *value += 7 * factor; break; \
                case '8': *value += 8 * factor; break; \
                case '9': *value += 9 * factor; break; \
                case '-': *value *= -1; break; \
                case '.': \
                case ',': *value /= factor; factor = 1; continue; \
                case '\0': continue; \
                default: \
                { \
                    return "Error converting string."; \
                } \
            } \
            factor *= 10; \
        }  \
        return MAL_SUCCEED; \
    } \
    str unicode_to_##tpe(Py_UNICODE *ptr, size_t maxsize, tpe *value) \
    {                                                              \
        char utf8[255];                                            \
        unicode_to_utf8(0, 255, utf8, ptr);                          \
        return str_to_##tpe(utf8, maxsize, value);                 \
    }                                                              \
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
#else
#define PY_TO_(type, inttpe)                                                                                      \
str pyobject_to_##type(PyObject **pyobj, size_t maxsize, type *value)                                             \
{                                                                                                                 \
    PyObject *ptr = *pyobj;                                                                                       \
    str retval = MAL_SUCCEED;                                                                                     \
    (void) maxsize;                                                                                               \
    if (PyLong_CheckExact(ptr)) {                                                                                 \
        PyLongObject *p = (PyLongObject*) ptr;                                                                    \
        inttpe h = 0;                                                                                             \
        inttpe prev = 0;                                                                                          \
        int i = Py_SIZE(p);                                                                                       \
        int sign = i < 0 ? -1 : 1;                                                                                \
        i *= sign;                                                                                                \
        while (--i >= 0) {                                                                                        \
            prev = h; (void)prev;                                                                                 \
            h = (h << PyLong_SHIFT) + p->ob_digit[i];                                                             \
            if ((h >> PyLong_SHIFT) != prev) {                                                                    \
                return GDKstrdup("Overflow when converting value.");                                              \
            }                                                                                                     \
        }                                                                                                         \
        *value = (type)(h * sign);                                                                                \
    } else if (PyBool_Check(ptr)) {                                                                               \
        *value = ptr == Py_True ? 1 : 0;                                                                          \
    } else if (PyFloat_CheckExact(ptr)) {                                                                         \
        *value = (type) ((PyFloatObject*)ptr)->ob_fval;                                                           \
    } else if (PyUnicode_CheckExact(ptr)) {                                                                       \
        return str_to_##type(PyUnicode_AsUTF8(ptr), -1, value);                                                   \
    }  else if (PyByteArray_CheckExact(ptr)) {                                                                    \
        return str_to_##type(((PyByteArrayObject*)ptr)->ob_bytes, -1, value);                                     \
    }  else if (ptr == Py_None) {                                                                                 \
        *value = type##_nil;                                                                                      \
    }                                                                                                             \
    return retval;                                                                                                \
}
#define CONVERSION_FUNCTION_FACTORY(tpe, inttpe)              \
    str str_to_##tpe(char *ptr, size_t maxsize, tpe *value) \
    { \
        int i = maxsize - 1; \
        tpe factor = 1; \
        if (i < 0) i = strlen(ptr) - 1; \
        *value = 0;  \
        for( ; i >= 0; i--) \
        { \
            switch(ptr[i]) \
            { \
                case '0': break; \
                case '1': *value += factor; break; \
                case '2': *value += 2 * factor; break; \
                case '3': *value += 3 * factor; break; \
                case '4': *value += 4 * factor; break; \
                case '5': *value += 5 * factor; break; \
                case '6': *value += 6 * factor; break; \
                case '7': *value += 7 * factor; break; \
                case '8': *value += 8 * factor; break; \
                case '9': *value += 9 * factor; break; \
                case '-': *value *= -1; break; \
                case '.': \
                case ',': *value /= factor; factor = 1; continue; \
                case '\0': continue; \
                default: \
                { \
                    return "Error converting string."; \
                } \
            } \
            factor *= 10; \
        }  \
        return MAL_SUCCEED; \
    } \
    str unicode_to_##tpe(char *ptr, size_t maxsize, tpe *value) { return str_to_##tpe(ptr, maxsize, value); }\
    PY_TO_(tpe, inttpe);

CONVERSION_FUNCTION_FACTORY(bte, bte)
CONVERSION_FUNCTION_FACTORY(oid, oid)
CONVERSION_FUNCTION_FACTORY(bit, bit)
CONVERSION_FUNCTION_FACTORY(sht, sht)
CONVERSION_FUNCTION_FACTORY(int, int)
CONVERSION_FUNCTION_FACTORY(lng, lng)
CONVERSION_FUNCTION_FACTORY(flt, lng)
CONVERSION_FUNCTION_FACTORY(dbl, lng)

#ifdef HAVE_HGE
CONVERSION_FUNCTION_FACTORY(hge, hge)
#endif
#endif

void _typeconversion_init(void) {
    import_array();
}
