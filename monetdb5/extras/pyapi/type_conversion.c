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


bool string_copy(char * source, char* dest, size_t max_size)
{
    size_t i;
    for(i = 0; i < max_size; i++)
    {
        dest[i] = source[i];
        if (dest[i] == 0) return TRUE;
        if ((*(unsigned char*)&source[i]) >= 128) return FALSE;
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

#define PY_TO_(type, inttpe)                                                                                             \
bool pyobject_to_##type(PyObject **pyobj, size_t maxsize, type *value)                                                                    \
{                                                                                                                \
    PyObject *ptr = *pyobj; \
    (void) maxsize; \
    if (PyLong_CheckExact(ptr)) {                                                                                     \
        PyLongObject *p = (PyLongObject*) ptr;                                                                   \
        inttpe h = 0;                                                                                              \
        inttpe prev = 0;                                                                                           \
        int i = Py_SIZE(p);                                                                                      \
        int sign = i < 0 ? -1 : 1;                                                                               \
        i *= sign;                                                                                               \
        while (--i >= 0) {                                                                                       \
            prev = h; (void)prev;                                                                                \
            h = (h << PyLong_SHIFT) + p->ob_digit[i];                                                            \
            if ((h >> PyLong_SHIFT) != prev) {                                                                   \
                printf("Overflow!\n");                                                                           \
                return false;                                                                                    \
            }                                                                                                    \
        }                                                                                                        \
        *value = (type)(h * sign);                                                                                       \
        return true;                                                                                             \
    } else if (PyInt_CheckExact(ptr) || PyBool_Check(ptr)) {                                                          \
        *value = (type)((PyIntObject*)ptr)->ob_ival;                                                             \
        return true;                                                                                             \
    } else if (PyFloat_CheckExact(ptr)) {                                                                             \
        *value = (type) ((PyFloatObject*)ptr)->ob_fval;                                                          \
        return true;                                                                                             \
    } else if (PyString_CheckExact(ptr)) {                                                                            \
        return str_to_##type(((PyStringObject*)ptr)->ob_sval, -1, value);     \
    }  else if (PyByteArray_CheckExact(ptr)) {                                                                        \
        return str_to_##type(((PyByteArrayObject*)ptr)->ob_bytes, -1, value);\
    } else if (PyUnicode_CheckExact(ptr)) {                                                                           \
        return unicode_to_##type(((PyUnicodeObject*)ptr)->str, -1, value);                                             \
    }                                                                                                            \
    return false;                                                                                                \
}

#define CONVERSION_FUNCTION_FACTORY(tpe, inttpe)              \
    bool str_to_##tpe(char *ptr, size_t maxsize, tpe *value) \
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
                    return false; \
                } \
            } \
            factor *= 10; \
        }  \
        return true; \
    } \
    bool unicode_to_##tpe(Py_UNICODE *ptr, size_t maxsize, tpe *value) \
    {                                                              \
        char utf8[255];                                            \
        utf32_to_utf8(0, 255, utf8, ptr);                          \
        return str_to_##tpe(utf8, maxsize, value);                 \
    }                                                              \
    PY_TO_(tpe, inttpe);

CONVERSION_FUNCTION_FACTORY(bte, bte)
CONVERSION_FUNCTION_FACTORY(wrd, wrd)
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
