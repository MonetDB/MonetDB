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
 * This file contains the conversion functions to go from BAT <> NumPy Array
 */

#ifndef _PYCONVERSION_LIB_
#define _PYCONVERSION_LIB_

#include "pyheader.h"

typedef struct {
	PyObject *numpy_array; // PyArrayObject* with data (can be NULL, as long as
						   // array_data is set)
	PyObject *numpy_mask; // PyArrayObject* with mask (NULL if there is no mask)
	void *array_data;	 // void* pointer to data
	bool *mask_data;	  // bool* pointer to mask data
	size_t array_size;
	size_t count;		  // amount of return elements
	size_t memory_size;   // memory size of each element
	lng mmap_id;
	int result_type;	   // result type as NPY_<TYPE>
	bool multidimensional; // whether or not the result is multidimensional
} PyReturn;

typedef struct {
	void *dataptr;			  // pointer to input data
	BAT *bat;				  // pointer to input BAT
	BAT *conv_bat;			  // converted input BAT
	int bat_type;			  // BAT type as TYPE_<type>
	sql_subtype *sql_subtype; // SQL typename (for _column_types)
	size_t count;			  // amount of elements in BAT
	bool scalar; // True if the input is a scalar (in this case, BAT* is NULL)
	PyObject *result; // Converted PyObject, probably shouldn't be here
} PyInput;

//! Create a Numpy Array Object from a PyInput structure containing a BAT
extern PyObject *PyArrayObject_FromBAT(allocator *, Client ctx, PyInput *input_bat, size_t start,
											 size_t end, char **return_message,
											 bool copy)
	__attribute__((__visibility__("hidden")));
//! Creates a Null Mask from a BAT (a Numpy Boolean Array of equal length to the
//! BAT, where NULLMASK[i] = True if BAT[i] is NULL, and False otherwise)
extern PyObject *PyNullMask_FromBAT(BAT *b, size_t start, size_t end)
	__attribute__((__visibility__("hidden")));
//! Creates a Numpy Array object from an PyInput structure containing a scalar
extern PyObject *PyArrayObject_FromScalar(PyInput *input_scalar,
												char **return_message)
	__attribute__((__visibility__("hidden")));
//! Creates a Numpy Masked Array  from an PyInput structure containing a BAT
//! (essentially just combines PyArrayObject_FromBAT and PyNullMask_FromBAT)
extern PyObject *PyMaskedArray_FromBAT(allocator *, Client ctx, PyInput *inp, size_t t_start,
											 size_t t_end,
											 char **return_message, bool copy)
	__attribute__((__visibility__("hidden")));
//! Test if a PyDict object can be converted to the expected set of return
//! columns, by checking if the correct keys are in the dictionary and if the
//! values in the keys can be converted to single columns (to a single numpy
//! array)
extern PyObject *PyDict_CheckForConversion(PyObject *pResult,
												 int expected_columns,
												 char **retcol_names,
												 char **return_message)
	__attribute__((__visibility__("hidden")));
//! Test if a specific PyObject can be converted to a set of <expected_columns>
//! BATs (or just check if they can be converted to any number of BATs if
//! expected_columns is smaller than 0)
extern PyObject *PyObject_CheckForConversion(PyObject *pResult,
												   int expected_columns,
												   int *actual_columns,
												   char **return_message)
	__attribute__((__visibility__("hidden")));
//! Preprocess a PyObject (that is the result of PyObject_CheckForConversion),
//! pyreturn_values must be an array of PyReturn structs of size column_count
extern bool PyObject_PreprocessObject(PyObject *pResult,
											PyReturn *pyreturn_values,
											int column_count,
											char **return_message)
	__attribute__((__visibility__("hidden")));
//! Create a BAT from the i'th PyReturn struct (filled by
//! PyObject_PreprocessObject), with bat_type set to the expected BAT Type (set
//! this to PyType_ToBat(ret->result_type) if there is no expected type),
//! seqbase should be set to 0 unless you know what you're doing
extern BAT *PyObject_ConvertToBAT(allocator *, Client ctx, PyReturn *ret, sql_subtype *type,
										int bat_type, int index, oid seqbase,
										char **return_message, bool copy)
	__attribute__((__visibility__("hidden")));
//! Returns the size of the Python object when converted
extern ssize_t PyType_Size(PyObject *obj)
	__attribute__((__visibility__("hidden")));
//! Populate a PyReturn object from a Python object
extern str PyObject_GetReturnValues(PyObject *obj,
										  PyReturn *return_value)
	__attribute__((__visibility__("hidden")));
//! Returns whether or not a BAT type is of a standard type (string or numeric),
//! or an extended type (e.g. blob, timestamp, etc)
extern bit IsStandardBATType(int type)
	__attribute__((__visibility__("hidden")));
//! Returns whether or not a special conversion is possible for the SQL type (if
//! this returns 0, string conversion is used instead)
extern bit ConvertableSQLType(sql_subtype *sql_subtype)
	__attribute__((__visibility__("hidden")));
//! Convert a BAT of a non-standard type (SQL type) to a standard type (numeric
//! or string)
extern str ConvertFromSQLType(allocator *, Client ctx, BAT *b, sql_subtype *sql_subtype,
									BAT **ret_bat, int *ret_type)
	__attribute__((__visibility__("hidden")));
//! Convert a BAT of a standard type (numeric or string) to a BAT of the
//! specified SQL type
extern str ConvertToSQLType(Client cntxt, BAT *b,
								  sql_subtype *sql_subtype, BAT **ret_bat,
								  int *ret_type)
	__attribute__((__visibility__("hidden")));

extern str _conversion_init(void)
	__attribute__((__visibility__("hidden")));

#endif /* _PYCONVERSION_LIB_ */
