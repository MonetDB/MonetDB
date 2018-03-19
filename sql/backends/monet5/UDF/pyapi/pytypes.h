		/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * This file contains a number of helper functions for Python and Numpy types
 */

#ifndef _PYTYPE_LIB_
#define _PYTYPE_LIB_

#include <stdint.h>
#include <stddef.h>

#include "pyheader.h"

// This describes return values, used in multiprocessing to tell the main
// process the size of the shared memory to allocate
typedef struct {
	int npy_type;		 // npy type
	size_t element_size; // element size in bytes
	size_t bat_count;	// number of elements in bat
	size_t bat_size;	 // bat size in bytes
	size_t bat_start;	// start position of bat
	bool has_mask;		 // if the return value has a mask or not
} ReturnBatDescr;

//! Returns true if a NPY_#type is an integral type, and false otherwise
pyapi_export bool PyType_IsInteger(int);
//! Returns true if a NPY_#type is a float type, and false otherwise
pyapi_export bool PyType_IsFloat(int);
//! Returns true if a NPY_#type is a double type, and false otherwise
pyapi_export bool PyType_IsDouble(int);
//! Formats NPY_#type as a String (so NPY_INT => "INT"), for usage in error
//! reporting and warnings
pyapi_export char *PyType_Format(int);
//! Returns true if a PyObject is a scalar type ('scalars' in this context means
//! numeric or string types)
pyapi_export bool PyType_IsPyScalar(PyObject *object);
//! Returns true if the PyObject is of type numpy.ndarray, and false otherwise
pyapi_export bool PyType_IsNumpyArray(PyObject *object);
//! Returns true if the PyObject is of type numpy.ma.core.MaskedArray, and false
//! otherwise
pyapi_export bool PyType_IsNumpyMaskedArray(PyObject *object);
//! Returns true if the PyObject is of type pandas.core.frame.DataFrame, and
//! false otherwise
pyapi_export bool PyType_IsPandasDataFrame(PyObject *object);
//! Returns true if the PyObject is of type lazyarray, and false otherwise
pyapi_export bool PyType_IsLazyArray(PyObject *object);
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
