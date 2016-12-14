/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains the conversion functions to go from BAT <> NumPy Array
 */

#ifndef _PYCONVERSION_LIB_
#define _PYCONVERSION_LIB_

#include "pyheader.h"

typedef struct {
    PyObject *numpy_array;              //PyArrayObject* with data (can be NULL, as long as array_data is set)
    PyObject *numpy_mask;               //PyArrayObject* with mask (NULL if there is no mask)
    void *array_data;                   //void* pointer to data
    bool *mask_data;                    //bool* pointer to mask data
    size_t count;                       //amount of return elements
    size_t memory_size;                 //memory size of each element
    lng mmap_id;
    int result_type;                    //result type as NPY_<TYPE>
    bool multidimensional;              //whether or not the result is multidimensional
} PyReturn;

typedef struct {
    void *dataptr;                      //pointer to input data
    BAT *bat;                           //pointer to input BAT
    int bat_type;                       //BAT type as TYPE_<type>
    sql_subtype *sql_subtype;           //SQL typename (for _column_types)
    size_t count;                       //amount of elements in BAT
    bool scalar;                        //True if the input is a scalar (in this case, BAT* is NULL)
    PyObject *result;                   //Converted PyObject, probably shouldn't be here
} PyInput;

//! Create a Numpy Array Object from a PyInput structure containing a BAT
pyapi_export PyObject *PyArrayObject_FromBAT(PyInput *input_bat, size_t start, size_t end, char **return_message, bool copy);
//! Creates a Null Mask from a BAT (a Numpy Boolean Array of equal length to the BAT, where NULLMASK[i] = True if BAT[i] is NULL, and False otherwise)
pyapi_export PyObject *PyNullMask_FromBAT(BAT *b, size_t start, size_t end);
//! Creates a Numpy Array object from an PyInput structure containing a scalar
pyapi_export PyObject *PyArrayObject_FromScalar(PyInput* input_scalar, char **return_message);
//! Creates a Numpy Masked Array  from an PyInput structure containing a BAT (essentially just combines PyArrayObject_FromBAT and PyNullMask_FromBAT)
pyapi_export PyObject *PyMaskedArray_FromBAT(PyInput *inp, size_t t_start, size_t t_end, char **return_message, bool copy);
//! Test if a PyDict object can be converted to the expected set of return columns, by checking if the correct keys are in the dictionary and if the values in the keys can be converted to single columns (to a single numpy array)
pyapi_export PyObject *PyDict_CheckForConversion(PyObject *pResult, int expected_columns, char **retcol_names, char **return_message);
//! Test if a specific PyObject can be converted to a set of <expected_columns> BATs (or just check if they can be converted to any number of BATs if expected_columns is smaller than 0)
pyapi_export PyObject *PyObject_CheckForConversion(PyObject *pResult, int expected_columns, int *actual_columns, char **return_message);
//! Preprocess a PyObject (that is the result of PyObject_CheckForConversion), pyreturn_values must be an array of PyReturn structs of size column_count
pyapi_export bool PyObject_PreprocessObject(PyObject *pResult, PyReturn *pyreturn_values, int column_count, char **return_message);
//! Create a BAT from the i'th PyReturn struct (filled by PyObject_PreprocessObject), with bat_type set to the expected BAT Type (set this to PyType_ToBat(ret->result_type) if there is no expected type), seqbase should be set to 0 unless you know what you're doing
pyapi_export BAT *PyObject_ConvertToBAT(PyReturn *ret, sql_subtype *type, int bat_type, int index, oid seqbase, char **return_message, bool copy);
//! Returns the size of the Python object when converted
pyapi_export ssize_t PyType_Size(PyObject *obj);
//! Populate a PyReturn object from a Python object
pyapi_export str PyObject_GetReturnValues(PyObject *obj, PyReturn *return_value);
//! Returns whether or not a BAT type is of a standard type (string or numeric), or an extended type (e.g. blob, timestamp, etc)
pyapi_export bit IsStandardBATType(int type);
//! Returns whether or not a special conversion is possible for the SQL type (if this returns 0, string conversion is used instead)
pyapi_export bit ConvertableSQLType(sql_subtype *sql_subtype);
//! Convert a BAT of a non-standard type (SQL type) to a standard type (numeric or string)
pyapi_export str ConvertFromSQLType(BAT *b, sql_subtype *sql_subtype, BAT **ret_bat, int *ret_type);
//! Convert a BAT of a standard type (numeric or string) to a BAT of the specified SQL type
pyapi_export str ConvertToSQLType(Client cntxt, BAT *b, sql_subtype *sql_subtype, BAT **ret_bat, int *ret_type);

str _conversion_init(void);

#endif /* _PYCONVERSION_LIB_ */
