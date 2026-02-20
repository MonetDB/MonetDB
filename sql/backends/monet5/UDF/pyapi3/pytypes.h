/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
extern bool PyType_IsInteger(int)
	__attribute__((__visibility__("hidden")));
//! Returns true if a NPY_#type is a float type, and false otherwise
extern bool PyType_IsFloat(int)
	__attribute__((__visibility__("hidden")));
//! Returns true if a NPY_#type is a double type, and false otherwise
extern bool PyType_IsDouble(int)
	__attribute__((__visibility__("hidden")));
//! Formats NPY_#type as a String (so NPY_INT => "INT"), for usage in error
//! reporting and warnings
extern char *PyType_Format(int)
	__attribute__((__visibility__("hidden")));
//! Returns true if a PyObject is a scalar type ('scalars' in this context means
//! numeric or string types)
extern bool PyType_IsPyScalar(PyObject *object)
	__attribute__((__visibility__("hidden")));
//! Returns true if the PyObject is of type numpy.ndarray, and false otherwise
extern bool PyType_IsNumpyArray(PyObject *object)
	__attribute__((__visibility__("hidden")));
//! Returns true if the PyObject is of type numpy.ma.core.MaskedArray, and false
//! otherwise
extern bool PyType_IsNumpyMaskedArray(PyObject *object)
	__attribute__((__visibility__("hidden")));
//! Returns true if the PyObject is of type pandas.core.frame.DataFrame, and
//! false otherwise
extern bool PyType_IsPandasDataFrame(PyObject *object)
	__attribute__((__visibility__("hidden")));
//! Returns true if the PyObject is of type lazyarray, and false otherwise
extern bool PyType_IsLazyArray(PyObject *object)
	__attribute__((__visibility__("hidden")));
extern char *BatType_Format(int)
	__attribute__((__visibility__("hidden")));

extern int PyType_ToBat(int)
	__attribute__((__visibility__("hidden")));
extern int BatType_ToPyType(int)
	__attribute__((__visibility__("hidden")));

extern bool Python_ObtainGIL(void)
	__attribute__((__visibility__("hidden")));
extern bool Python_ReleaseGIL(bool)
	__attribute__((__visibility__("hidden")));

#define bte_TO_PYSCALAR(value) PyLong_FromLong(value)
#define bit_TO_PYSCALAR(value) PyLong_FromLong(value)
#define sht_TO_PYSCALAR(value) PyLong_FromLong(value)
#define int_TO_PYSCALAR(value) PyLong_FromLong(value)
#define lng_TO_PYSCALAR(value) PyLong_FromLongLong(value)
#define flt_TO_PYSCALAR(value) PyFloat_FromDouble(value)
#define dbl_TO_PYSCALAR(value) PyFloat_FromDouble(value)

// A simple #define that converts a numeric TYPE_<mtpe> value to a Python scalar
#define SCALAR_TO_PYSCALAR(mtpe, value) mtpe##_TO_PYSCALAR(value)

extern void _pytypes_init(void)
	__attribute__((__visibility__("hidden")));

#endif /* _PYTYPE_LIB_ */
