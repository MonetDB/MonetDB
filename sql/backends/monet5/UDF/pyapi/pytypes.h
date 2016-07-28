/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of helper functions for Python and Numpy types
 */

#ifndef _PYTYPE_LIB_
#define _PYTYPE_LIB_

#include <stdint.h>
#include <stddef.h>

#include "pyapi.h"

// This describes return values, used in multiprocessing to tell the main process the size of the shared memory to allocate
struct _ReturnBatDescr
{
    int npy_type;                        //npy type
    size_t element_size;                 //element size in bytes
    size_t bat_count;                    //number of elements in bat
    size_t bat_size;                     //bat size in bytes
    size_t bat_start;                    //start position of bat
    bool has_mask;                       //if the return value has a mask or not
};
#define ReturnBatDescr struct _ReturnBatDescr

struct _PyReturn{
    PyObject *numpy_array;              //PyArrayObject* with data (can be NULL, as long as array_data is set)
    PyObject *numpy_mask;               //PyArrayObject* with mask (NULL if there is no mask)
    void *array_data;                   //void* pointer to data
    bool *mask_data;                    //bool* pointer to mask data
    size_t count;                       //amount of return elements
    size_t memory_size;                 //memory size of each element
    lng mmap_id;
    int result_type;                    //result type as NPY_<TYPE>
    bool multidimensional;              //whether or not the result is multidimensional
};
#define PyReturn struct _PyReturn

struct _PyInput{
    void *dataptr;                      //pointer to input data
    BAT *bat;                           //pointer to input BAT
    int bat_type;                       //BAT type as TYPE_<type>
    sql_subtype *sql_subtype;           //SQL typename (for _column_types)
    size_t count;                       //amount of elements in BAT
    bool scalar;                        //True if the input is a scalar (in this case, BAT* is NULL)
    PyObject *result;                   //Converted PyObject, probably shouldn't be here
};
#define PyInput struct _PyInput

struct _QueryStruct{
    bool pending_query;
    char query[8192];
    int nr_cols;
    int mmapid;
    size_t memsize;
};
#define QueryStruct struct _QueryStruct

//! Returns true if a NPY_#type is an integral type, and false otherwise
pyapi_export bool PyType_IsInteger(int);
//! Returns true if a NPY_#type is a float type, and false otherwise
pyapi_export bool PyType_IsFloat(int);
//! Returns true if a NPY_#type is a double type, and false otherwise
pyapi_export bool PyType_IsDouble(int);
//! Formats NPY_#type as a String (so NPY_INT => "INT"), for usage in error reporting and warnings
pyapi_export char *PyType_Format(int);
//! Returns true if a PyObject is a scalar type ('scalars' in this context means numeric or string types)
pyapi_export bool PyType_IsPyScalar(PyObject *object);
//! Returns true if the PyObject is of type numpy.ndarray, and false otherwise
pyapi_export bool PyType_IsNumpyArray(PyObject *object);
//! Returns true if the PyObject is of type numpy.ma.core.MaskedArray, and false otherwise
pyapi_export bool PyType_IsNumpyMaskedArray(PyObject *object);
//! Returns true if the PyObject is of type pandas.core.frame.DataFrame, and false otherwise
pyapi_export bool PyType_IsPandasDataFrame(PyObject *object);
//! Returns true if the PyObject is of type lazyarray, and false otherwise
pyapi_export bool PyType_IsLazyArray(PyObject *object);
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
pyapi_export char *BatType_Format(int);

pyapi_export int PyType_ToBat(int);
pyapi_export int BatType_ToPyType(int);

pyapi_export bool Python_ObtainGIL(void);
pyapi_export bool Python_ReleaseGIL(bool);

#define bte_TO_PYSCALAR(value) PyInt_FromLong(value)
#define bit_TO_PYSCALAR(value) PyInt_FromLong(value)
#define sht_TO_PYSCALAR(value) PyInt_FromLong(value)
#define int_TO_PYSCALAR(value) PyInt_FromLong(value)
#define lng_TO_PYSCALAR(value) PyLong_FromLongLong(value)
#define flt_TO_PYSCALAR(value) PyFloat_FromDouble(value)
#define dbl_TO_PYSCALAR(value) PyFloat_FromDouble(value)

// A simple #define that converts a numeric TYPE_<mtpe> value to a Python scalar
#define SCALAR_TO_PYSCALAR(mtpe, value) mtpe##_TO_PYSCALAR(value)


void _pytypes_init(void);

#endif /* _PYTYPE_LIB_ */
