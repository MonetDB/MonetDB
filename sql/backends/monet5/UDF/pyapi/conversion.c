/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "conversion.h"
#include "convert_loops.h"
#include "pytypes.h"
#include "type_conversion.h"
#include "unicode.h"
#include "blob.h"
#ifndef HAVE_EMBEDDED
#include "gdk_interprocess.h"
#endif

CREATE_SQL_FUNCTION_PTR(str, batbte_dec2_dbl);
CREATE_SQL_FUNCTION_PTR(str, batsht_dec2_dbl);
CREATE_SQL_FUNCTION_PTR(str, batint_dec2_dbl);
CREATE_SQL_FUNCTION_PTR(str, batlng_dec2_dbl);
#ifdef HAVE_HGE
CREATE_SQL_FUNCTION_PTR(str, bathge_dec2_dbl);
#endif
CREATE_SQL_FUNCTION_PTR(str, batstr_2time_timestamp);
CREATE_SQL_FUNCTION_PTR(str, batstr_2time_daytime);
CREATE_SQL_FUNCTION_PTR(str, batstr_2_date);
CREATE_SQL_FUNCTION_PTR(str, batdbl_num2dec_lng);

//! Wrapper to get eclass of SQL type
int GetSQLType(sql_subtype *sql_subtype);

static bool IsBlobType(int type)
{
	return type == TYPE_blob || type == TYPE_sqlblob;
}

PyObject *PyArrayObject_FromScalar(PyInput *inp, char **return_message)
{
	PyObject *vararray = NULL;
	char *msg = NULL;
	assert(inp->scalar); // input has to be a scalar

	switch (inp->bat_type) {
		case TYPE_bit:
			vararray = PyInt_FromLong((long)(*(bit *)inp->dataptr));
			break;
		case TYPE_bte:
			vararray = PyInt_FromLong((long)(*(bte *)inp->dataptr));
			break;
		case TYPE_sht:
			vararray = PyInt_FromLong((long)(*(sht *)inp->dataptr));
			break;
		case TYPE_int:
			vararray = PyInt_FromLong((long)(*(int *)inp->dataptr));
			break;
		case TYPE_lng:
			vararray = PyLong_FromLongLong((*(lng *)inp->dataptr));
			break;
		case TYPE_flt:
			vararray = PyFloat_FromDouble((double)(*(flt *)inp->dataptr));
			break;
		case TYPE_dbl:
			vararray = PyFloat_FromDouble((double)(*(dbl *)inp->dataptr));
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			vararray = PyLong_FromHge(*((hge *)inp->dataptr));
			break;
#endif
		case TYPE_str:
			vararray = PyUnicode_FromString(*((char **)inp->dataptr));
			break;
		default:
			msg = createException(MAL, "pyapi.eval",
								  "SQLSTATE PY000 !""Unsupported scalar type %i.", inp->bat_type);
			goto wrapup;
	}
	if (vararray == NULL) {
		msg = createException(MAL, "pyapi.eval", "SQLSTATE PY000 !""Something went wrong "
												 "converting the MonetDB "
												 "scalar to a Python scalar.");
		goto wrapup;
	}
wrapup:
	*return_message = msg;
	return vararray;
}

PyObject *PyMaskedArray_FromBAT(PyInput *inp, size_t t_start, size_t t_end,
								char **return_message, bool copy)
{
	BAT *b = inp->bat;
	char *msg;
	PyObject *vararray;

	vararray = PyArrayObject_FromBAT(inp, t_start, t_end, return_message, copy);
	if (vararray == NULL) {
		return NULL;
	}
	// To deal with null values, we use the numpy masked array structure
	// The masked array structure is an object with two arrays of equal size, a
	// data array and a mask array
	// The mask array is a boolean array that has the value 'True' when the
	// element is NULL, and 'False' otherwise
	// if we know for sure that the BAT has no NULL values, we can skip the construction
	// of this masked array. Otherwise, we create it.
	if (!(b->tnil == 0 && b->tnonil == 1)) {
		PyObject *mask;
		PyObject *mafunc = PyObject_GetAttrString(
			PyImport_Import(PyString_FromString("numpy.ma")), "masked_array");
		PyObject *maargs;
		PyObject *nullmask = PyNullMask_FromBAT(b, t_start, t_end);

		if (!nullmask) {
			msg = createException(MAL, "pyapi.eval", "Failed to create mask for some reason");
			goto wrapup;
		} else if (nullmask == Py_None) {
			maargs = PyTuple_New(1);
			PyTuple_SetItem(maargs, 0, vararray);
		} else {
			maargs = PyTuple_New(2);
			PyTuple_SetItem(maargs, 0, vararray);
			PyTuple_SetItem(maargs, 1, (PyObject *)nullmask);
		}

		// Now we will actually construct the mask by calling the masked array
		// constructor
		mask = PyObject_CallObject(mafunc, maargs);
		if (!mask) {
			msg = createException(MAL, "pyapi.eval", "SQLSTATE PY000 !""Failed to create mask");
			goto wrapup;
		}
		Py_DECREF(maargs);
		Py_DECREF(mafunc);

		vararray = mask;
	}
	return vararray;
wrapup:
	*return_message = msg;
	return NULL;
}

PyObject *PyArrayObject_FromBAT(PyInput *inp, size_t t_start, size_t t_end,
								char **return_message, bool copy)
{
	// This variable will hold the converted Python object
	PyObject *vararray = NULL;
	char *msg;
	size_t j = 0;
	BUN p = 0, q = 0;
	BATiter li;
	BAT *b = inp->bat;
	npy_intp elements[1] = {t_end - t_start};

	assert(!inp->scalar); // input has to be a BAT

	if (!b) {
		// No BAT was found, we can't do anything in this case
		msg = createException(MAL, "pyapi.eval", "SQLSTATE HY001 !"MAL_MALLOC_FAIL " bat missing");
		goto wrapup;
	}

	if (!IsBlobType(inp->bat_type) &&
		(!IsStandardBATType(inp->bat_type) ||
		 ConvertableSQLType(inp->sql_subtype))) { // if the sql type is set, we
												  // have to do some conversion
		if (inp->scalar) {
			// FIXME: scalar SQL types
			msg = createException(
				MAL, "pyapi.eval",
				"SQLSTATE PY000 !""Scalar SQL types haven't been implemented yet... sorry");
			goto wrapup;
		} else {
			BAT *ret_bat = NULL;
			msg = ConvertFromSQLType(inp->bat, inp->sql_subtype, &ret_bat,
									 &inp->bat_type);
			if (msg != MAL_SUCCEED) {
				msg = createException(MAL, "pyapi.eval",
									  "SQLSTATE PY000 !""Failed to convert BAT.");
				goto wrapup;
			}
			BBPunfix(inp->bat->batCacheid);
			inp->bat = ret_bat;
		}
	}

	b = inp->bat;

	if (IsBlobType(inp->bat_type)) {
		PyObject **data;
		li = bat_iterator(b);
		vararray = PyArray_EMPTY(1, elements, NPY_OBJECT, 0);
		data = PyArray_DATA((PyArrayObject *)vararray);
		BATloop(b, p, q)
		{
			blob *t = (blob *)BUNtail(li, p);
			if (t->nitems == ~(size_t)0) {
				data[p] = Py_None;
				Py_INCREF(Py_None);
			} else {
				data[p] = PyByteArray_FromStringAndSize(t->data, t->nitems);
			}
		}
	} else {
		switch (inp->bat_type) {
			case TYPE_bit:
				BAT_TO_NP(b, bit, NPY_INT8);
				break;
			case TYPE_bte:
				BAT_TO_NP(b, bte, NPY_INT8);
				break;
			case TYPE_sht:
				BAT_TO_NP(b, sht, NPY_INT16);
				break;
			case TYPE_int:
				BAT_TO_NP(b, int, NPY_INT32);
				break;
			case TYPE_lng:
				BAT_TO_NP(b, lng, NPY_INT64);
				break;
			case TYPE_flt:
				BAT_TO_NP(b, flt, NPY_FLOAT32);
				break;
			case TYPE_dbl:
				BAT_TO_NP(b, dbl, NPY_FLOAT64);
				break;
			case TYPE_str: {
				bool unicode = false;
				li = bat_iterator(b);
				// create a NPY_OBJECT array object
				vararray = PyArray_New(&PyArray_Type, 1, elements, NPY_OBJECT,
									   NULL, NULL, 0, 0, NULL);

				BATloop(b, p, q)
				{
					char *t = (char *)BUNtail(li, p);
					for (; *t != 0; t++) {
						if (*t & 0x80) {
							unicode = true;
							break;
						}
					}
					if (unicode) {
						break;
					}
				}

				{
					PyObject **data =
						((PyObject **)PyArray_DATA((PyArrayObject *)vararray));
					PyObject *obj;
					j = 0;
					if (unicode) {
						if (GDK_ELIMDOUBLES(b->tvheap)) {
							PyObject **pyptrs =
								GDKzalloc(b->tvheap->free * sizeof(PyObject *));
							if (!pyptrs) {
								msg = createException(MAL, "pyapi.eval",
													  "SQLSTATE HY001 !"MAL_MALLOC_FAIL
													  " PyObject strings.");
								goto wrapup;
							}
							BATloop(b, p, q)
							{
								const char *t = (const char *)BUNtail(li, p);
								ptrdiff_t offset = t - b->tvheap->base;
								if (!pyptrs[offset]) {
									if (strcmp(t, str_nil) == 0) {
										// str_nil isn't a valid UTF-8 character
										// (it's 0x80), so we can't decode it as
										// UTF-8 (it will throw an error)
										pyptrs[offset] =
											PyUnicode_FromString("-");
									} else {
										// otherwise we can just decode the
										// string as UTF-8
										pyptrs[offset] =
											PyUnicode_FromString(t);
									}
									if (!pyptrs[offset]) {
										msg = createException(
											MAL, "pyapi.eval",
											"SQLSTATE PY000 !""Failed to create string.");
										goto wrapup;
									}
								} else {
									Py_INCREF(pyptrs[offset]);
								}
								data[j++] = pyptrs[offset];
							}
							GDKfree(pyptrs);
						} else {
							BATloop(b, p, q)
							{
								char *t = (char *)BUNtail(li, p);
								if (strcmp(t, str_nil) == 0) {
									// str_nil isn't a valid UTF-8 character
									// (it's 0x80), so we can't decode it as
									// UTF-8 (it will throw an error)
									obj = PyUnicode_FromString("-");
								} else {
									// otherwise we can just decode the string
									// as UTF-8
									obj = PyUnicode_FromString(t);
								}

								if (obj == NULL) {
									msg = createException(
										MAL, "pyapi.eval",
										"SQLSTATE PY000 !""Failed to create string.");
									goto wrapup;
								}
								data[j++] = obj;
							}
						}
					} else {
						/* special case where we exploit the
						 * duplicate-eliminated string heap */
						if (GDK_ELIMDOUBLES(b->tvheap)) {
							PyObject **pyptrs =
								GDKzalloc(b->tvheap->free * sizeof(PyObject *));
							if (!pyptrs) {
								msg = createException(MAL, "pyapi.eval",
													  "SQLSTATE HY001 !"MAL_MALLOC_FAIL
													  " PyObject strings.");
								goto wrapup;
							}
							BATloop(b, p, q)
							{
								const char *t = (const char *)BUNtail(li, p);
								ptrdiff_t offset = t - b->tvheap->base;
								if (!pyptrs[offset]) {
									pyptrs[offset] = PyString_FromString(t);
								} else {
									Py_INCREF(pyptrs[offset]);
								}
								data[j++] = pyptrs[offset];
							}
							GDKfree(pyptrs);
						} else {
							BATloop(b, p, q)
							{
								char *t = (char *)BUNtail(li, p);
								obj = PyString_FromString(t);
								if (obj == NULL) {
									msg = createException(
										MAL, "pyapi.eval",
										"SQLSTATE PY000 !""Failed to create string.");
									goto wrapup;
								}
								data[j++] = obj;
							}
						}
					}
				}
			} break;
#ifdef HAVE_HGE
			case TYPE_hge: {
				li = bat_iterator(b);
				// create a NPY_FLOAT64 array to hold the huge type
				vararray = PyArray_New(&PyArray_Type, 1,
									   (npy_intp[1]){t_end - t_start},
									   NPY_FLOAT64, NULL, NULL, 0, 0, NULL);

				j = 0;
				{
					dbl *data = (dbl *)PyArray_DATA((PyArrayObject *)vararray);
					BATloop(b, p, q)
					{
						const hge *t = (const hge *)BUNtail(li, p);
						data[j++] = (dbl)*t;
					}
				}
				break;
			}
#endif
			default:
				if (!inp->sql_subtype || !inp->sql_subtype->type) {
					msg = createException(MAL, "pyapi.eval",
										  "SQLSTATE PY000 !""unknown argument type");
				} else {
					msg = createException(MAL, "pyapi.eval",
										  "SQLSTATE PY000 !""Unsupported SQL Type: %s",
										  inp->sql_subtype->type->sqlname);
				}
				goto wrapup;
		}
	}

	if (vararray == NULL) {
		msg = createException(MAL, "pyapi.eval",
							  "SQLSTATE PY000 !""Failed to convert BAT to Numpy array.");
		goto wrapup;
	}
	return vararray;
wrapup:
	*return_message = msg;
	return NULL;
}

#define CreateNullMask(tpe)                                                    \
	{                                                                          \
		tpe *bat_ptr = (tpe *)b->theap.base;                                   \
		for (j = 0; j < count; j++) {                                          \
			mask_data[j] = bat_ptr[j] == tpe##_nil;                            \
			found_nil = found_nil || mask_data[j];                             \
		}                                                                      \
	}

PyObject *PyNullMask_FromBAT(BAT *b, size_t t_start, size_t t_end)
{
	// We will now construct the Masked array, we start by setting everything to
	// False
	size_t count = t_end - t_start;
	npy_intp elements[1] = {count};
	PyArrayObject *nullmask =
		(PyArrayObject *)PyArray_EMPTY(1, elements, NPY_BOOL, 0);
	const void *nil = ATOMnilptr(b->ttype);
	size_t j;
	bool found_nil = false;
	BATiter bi = bat_iterator(b);
	bool *mask_data = (bool *)PyArray_DATA(nullmask);

	switch (ATOMstorage(getBatType(b->ttype))) {
		case TYPE_bit:
			CreateNullMask(bit);
			break;
		case TYPE_bte:
			CreateNullMask(bte);
			break;
		case TYPE_sht:
			CreateNullMask(sht);
			break;
		case TYPE_int:
			CreateNullMask(int);
			break;
		case TYPE_lng:
			CreateNullMask(lng);
			break;
		case TYPE_flt:
			CreateNullMask(flt);
			break;
		case TYPE_dbl:
			CreateNullMask(dbl);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			CreateNullMask(hge);
			break;
#endif
		default: {
			int (*atomcmp)(const void *, const void *) = ATOMcompare(b->ttype);
			for (j = 0; j < count; j++) {
				mask_data[j] = (*atomcmp)(BUNtail(bi, (BUN)(j)), nil) == 0;
				found_nil = found_nil || mask_data[j];
			}
		}
	}

	if (!found_nil) {
		Py_DECREF(nullmask);
		Py_RETURN_NONE;
	}

	return (PyObject *)nullmask;
}

PyObject *PyDict_CheckForConversion(PyObject *pResult, int expected_columns,
									char **retcol_names, char **return_message)
{
	char *msg = MAL_SUCCEED;
	PyObject *result = PyList_New(expected_columns),
			 *keys = PyDict_Keys(pResult);
	int i;

	for (i = 0; i < expected_columns; i++) {
		PyObject *object = PyDict_GetItemString(pResult, retcol_names[i]);
		if (object == NULL) {
			msg =
				createException(MAL, "pyapi.eval",
								"SQLSTATE PY000 !""Expected a return value with name \"%s\", but "
								"this key was not present in the dictionary.",
								retcol_names[i]);
			goto wrapup;
		}
		Py_INCREF(object);
		object = PyObject_CheckForConversion(object, 1, NULL, return_message);
		if (object == NULL) {
			msg = createException(
				MAL, "pyapi.eval",
				"SQLSTATE PY000 !""Error converting dict return value \"%s\": %s.",
				retcol_names[i], *return_message);
			GDKfree(*return_message);
			goto wrapup;
		}
		if (PyList_CheckExact(object)) {
			PyObject *item = PyList_GetItem(object, 0);
			PyList_SetItem(result, i, item);
			Py_INCREF(item);
			Py_DECREF(object);
		} else {
			msg = createException(MAL, "pyapi.eval", "SQLSTATE PY000 !""Why is this not a list?");
			goto wrapup;
		}
	}
	Py_DECREF(keys);
	Py_DECREF(pResult);
	// Py_INCREF(result);
	return result;
wrapup:
	*return_message = msg;
	Py_DECREF(result);
	Py_DECREF(keys);
	Py_DECREF(pResult);
	return NULL;
}

PyObject *PyObject_CheckForConversion(PyObject *pResult, int expected_columns,
									  int *actual_columns,
									  char **return_message)
{
	char *msg;
	int columns = 0;
	if (pResult) {
		PyObject *pColO = NULL;
		if (PyType_IsPandasDataFrame(pResult)) {
			// the result object is a Pandas data frame
			// we can convert the pandas data frame to a numpy array by simply
			// accessing the "values" field (as pandas dataframes are numpy
			// arrays internally)
			pResult = PyObject_GetAttrString(pResult, "values");
			if (pResult == NULL) {
				msg = createException(MAL, "pyapi.eval",
									  "SQLSTATE PY000 !""Invalid Pandas data frame.");
				goto wrapup;
			}
			// we transpose the values field so it's aligned correctly for our
			// purposes
			pResult = PyObject_GetAttrString(pResult, "T");
			if (pResult == NULL) {
				msg = createException(MAL, "pyapi.eval",
									  "SQLSTATE PY000 !""Invalid Pandas data frame.");
				goto wrapup;
			}
		}

		if (PyType_IsPyScalar(
				pResult)) { // check if the return object is a scalar
			if (expected_columns == 1 || expected_columns <= 0) {
				// if we only expect a single return value, we can accept
				// scalars by converting it into an array holding an array
				// holding the element (i.e. [[pResult]])
				PyObject *list = PyList_New(1);
				PyList_SetItem(list, 0, pResult);
				pResult = list;

				list = PyList_New(1);
				PyList_SetItem(list, 0, pResult);
				pResult = list;

				columns = 1;
			} else {
				// the result object is a scalar, yet we expect more than one
				// return value. We can only convert the result into a list with
				// a single element, so the output is necessarily wrong.
				msg = createException(
					MAL, "pyapi.eval",
					"SQLSTATE PY000 !""A single scalar was returned, yet we expect a list of %d "
					"columns. We can only convert a single scalar into a "
					"single column, thus the result is invalid.",
					expected_columns);
				goto wrapup;
			}
		} else {
			// if it is not a scalar, we check if it is a single array
			bool IsSingleArray = TRUE;
			PyObject *data = pResult;
			if (PyType_IsNumpyMaskedArray(data)) {
				data = PyObject_GetAttrString(pResult, "data");
				if (data == NULL) {
					msg = createException(MAL, "pyapi.eval",
										  "SQLSTATE PY000 !""Invalid masked array.");
					goto wrapup;
				}
			}
			if (PyType_IsNumpyArray(data)) {
				if (PyArray_NDIM((PyArrayObject *)data) != 1) {
					IsSingleArray = FALSE;
				} else {
					pColO = PyArray_GETITEM(
						(PyArrayObject *)data,
						PyArray_GETPTR1((PyArrayObject *)data, 0));
					IsSingleArray = PyType_IsPyScalar(pColO);
				}
			} else if (PyList_Check(data)) {
				pColO = PyList_GetItem(data, 0);
				IsSingleArray = PyType_IsPyScalar(pColO);
			} else if (!PyType_IsNumpyMaskedArray(data)) {
				// it is neither a python array, numpy array or numpy masked
				// array, thus the result is unsupported! Throw an exception!
				msg = createException(
					MAL, "pyapi.eval",
					"SQLSTATE PY000 !""Unsupported result object. Expected either a list, "
					"dictionary, a numpy array, a numpy masked array or a "
					"pandas data frame, but received an object of type \"%s\"",
					PyString_AsString(PyObject_Str(PyObject_Type(data))));
				goto wrapup;
			}

			if (IsSingleArray) {
				if (expected_columns == 1 || expected_columns <= 0) {
					// if we only expect a single return value, we can accept a
					// single array by converting it into an array holding an
					// array holding the element (i.e. [pResult])
					PyObject *list = PyList_New(1);
					PyList_SetItem(list, 0, pResult);
					pResult = list;

					columns = 1;
				} else {
					// the result object is a single array, yet we expect more
					// than one return value. We can only convert the result
					// into a list with a single array, so the output is
					// necessarily wrong.
					msg = createException(MAL, "pyapi.eval",
										  "SQLSTATE PY000 !""A single array was returned, yet we "
										  "expect a list of %d columns. The "
										  "result is invalid.",
										  expected_columns);
					goto wrapup;
				}
			} else {
				// the return value is an array of arrays, all we need to do is
				// check if it is the correct size
				int results = 0;
				if (PyList_Check(data))
					results = (int)PyList_Size(data);
				else
					results = (int)PyArray_DIMS((PyArrayObject *)data)[0];
				columns = results;
				if (results != expected_columns && expected_columns > 0) {
					// wrong return size, we expect pci->retc arrays
					msg = createException(MAL, "pyapi.eval",
										  "SQLSTATE PY000 !""An array of size %d was returned, "
										  "yet we expect a list of %d columns. "
										  "The result is invalid.",
										  results, expected_columns);
					goto wrapup;
				}
			}
		}
	} else {
		msg = createException(
			MAL, "pyapi.eval",
			"SQLSTATE PY000 !""Invalid result object. No result object could be generated.");
		goto wrapup;
	}

	if (actual_columns != NULL)
		*actual_columns = columns;
	return pResult;
wrapup:
	if (actual_columns != NULL)
		*actual_columns = columns;
	*return_message = msg;
	return NULL;
}

str PyObject_GetReturnValues(PyObject *obj, PyReturn *ret)
{
	PyObject *pMask = NULL;
	str msg = MAL_SUCCEED;
	// If it isn't we need to convert pColO to the expected Numpy Array type
	ret->numpy_array = PyArray_FromAny(
		obj, NULL, 1, 1, NPY_ARRAY_CARRAY | NPY_ARRAY_FORCECAST, NULL);
	if (ret->numpy_array == NULL) {
		msg = createException(
			MAL, "pyapi.eval",
			"SQLSTATE PY000 !""Could not create a Numpy array from the return type.\n");
		goto wrapup;
	}

	ret->result_type =
		PyArray_DESCR((PyArrayObject *)ret->numpy_array)
			->type_num; // We read the result type from the resulting array
	ret->memory_size = PyArray_DESCR((PyArrayObject *)ret->numpy_array)->elsize;
	ret->count = PyArray_DIMS((PyArrayObject *)ret->numpy_array)[0];
	ret->array_data = PyArray_DATA((PyArrayObject *)ret->numpy_array);
	ret->mask_data = NULL;
	ret->numpy_mask = NULL;
	// If pColO is a Masked array, we convert the mask to a NPY_BOOL numpy array
	if (PyObject_HasAttrString(obj, "mask")) {
		pMask = PyObject_GetAttrString(obj, "mask");
		if (pMask != NULL) {
			ret->numpy_mask =
				PyArray_FromAny(pMask, PyArray_DescrFromType(NPY_BOOL), 1, 1,
								NPY_ARRAY_CARRAY, NULL);
			if (ret->numpy_mask == NULL ||
				PyArray_DIMS((PyArrayObject *)ret->numpy_mask)[0] !=
					(int)ret->count) {
				PyErr_Clear();
				pMask = NULL;
				ret->numpy_mask = NULL;
			}
		}
	}
	if (ret->numpy_mask != NULL)
		ret->mask_data = PyArray_DATA((PyArrayObject *)ret->numpy_mask);
wrapup:
	return msg;
}

bool PyObject_PreprocessObject(PyObject *pResult, PyReturn *pyreturn_values,
							   int column_count, char **return_message)
{
	int i;
	char *msg;
	for (i = 0; i < column_count; i++) {
		// Refers to the current Numpy mask (if it exists)
		PyObject *pMask = NULL;
		// Refers to the current Numpy array
		PyObject *pColO = NULL;
		// This is the PyReturn header information for the current return value,
		// we will fill this now
		PyReturn *ret = &pyreturn_values[i];

		ret->multidimensional = FALSE;
		// There are three possibilities (we have ensured this right after
		// executing the Python call by calling PyObject_CheckForConversion)
		// 1: The top level result object is a PyList or Numpy Array containing
		// pci->retc Numpy Arrays
		// 2: The top level result object is a (pci->retc x N) dimensional Numpy
		// Array [Multidimensional]
		// 3: The top level result object is a (pci->retc x N) dimensional Numpy
		// Masked Array [Multidimensional]
		if (PyList_Check(pResult)) {
			// If it is a PyList, we simply get the i'th Numpy array from the
			// PyList
			pColO = PyList_GetItem(pResult, i);
		} else {
			// If it isn't, the result object is either a Nump Masked Array or a
			// Numpy Array
			PyObject *data = pResult;
			if (PyType_IsNumpyMaskedArray(data)) {
				data = PyObject_GetAttrString(
					pResult, "data"); // If it is a Masked array, the data is
									  // stored in the masked_array.data
									  // attribute
				pMask = PyObject_GetAttrString(pResult, "mask");
			}

			// We can either have a multidimensional numpy array, or a single
			// dimensional numpy array
			if (PyArray_NDIM((PyArrayObject *)data) != 1) {
				// If it is a multidimensional numpy array, we have to convert
				// the i'th dimension to a NUMPY array object
				ret->multidimensional = TRUE;
				ret->result_type =
					PyArray_DESCR((PyArrayObject *)data)->type_num;
			} else {
				// If it is a single dimensional Numpy array, we get the i'th
				// Numpy array from the Numpy Array
				pColO =
					PyArray_GETITEM((PyArrayObject *)data,
									PyArray_GETPTR1((PyArrayObject *)data, i));
			}
		}

		// Now we have to do some preprocessing on the data
		if (ret->multidimensional) {
			// If it is a multidimensional Numpy array, we don't need to do any
			// conversion, we can just do some pointers
			ret->count = PyArray_DIMS((PyArrayObject *)pResult)[1];
			ret->numpy_array = pResult;
			ret->numpy_mask = pMask;
			ret->array_data = PyArray_DATA((PyArrayObject *)ret->numpy_array);
			if (ret->numpy_mask != NULL)
				ret->mask_data = PyArray_DATA((PyArrayObject *)ret->numpy_mask);
			ret->memory_size =
				PyArray_DESCR((PyArrayObject *)ret->numpy_array)->elsize;
		} else {
			msg = PyObject_GetReturnValues(pColO, ret);
			if (msg != MAL_SUCCEED) {
				goto wrapup;
			}
		}
	}
	return TRUE;
wrapup:
	*return_message = msg;
	return FALSE;
}

BAT *PyObject_ConvertToBAT(PyReturn *ret, sql_subtype *type, int bat_type,
						   int i, oid seqbase, char **return_message, bool copy)
{
	BAT *b = NULL;
	size_t index_offset = 0;
	char *msg;
	size_t iu;

	if (ret->multidimensional)
		index_offset = i;

	switch (GetSQLType(type)) {
		case EC_TIMESTAMP:
		case EC_TIME:
		case EC_DATE:
			bat_type = TYPE_str;
			break;
		case EC_DEC:
			bat_type = TYPE_dbl;
			break;
		default:
			break;
	}

	if (IsBlobType(bat_type)) {
		bool *mask = NULL;
		char *data = NULL;
		blob *ele_blob;
		size_t blob_fixed_size = -1;
		if (ret->result_type == NPY_OBJECT) {
			// FIXME: check for byte array/or pickle object to string
			msg = createException(MAL, "pyapi.eval",
								  "SQLSTATE PY000 !""Python object to BLOB not supported yet.");
			goto wrapup;
		}
		if (ret->mask_data != NULL) {
			mask = (bool *)ret->mask_data;
		}
		if (ret->array_data == NULL) {
			msg = createException(MAL, "pyapi.eval",
								  "SQLSTATE PY000 !""No return value stored in the structure.");
			goto wrapup;
		}
		data = (char *)ret->array_data;
		data += (index_offset * ret->count) * ret->memory_size;
		blob_fixed_size = ret->memory_size;
		b = COLnew(seqbase, TYPE_sqlblob, (BUN)ret->count, TRANSIENT);
		b->tnil = 0;
		b->tnonil = 1;
		b->tkey = 0;
		b->tsorted = 0;
		b->trevsorted = 0;
		for (iu = 0; iu < ret->count; iu++) {
			size_t blob_len = 0;
			if (mask && mask[iu]) {
				ele_blob = (blob *)GDKmalloc(offsetof(blob, data));
				ele_blob->nitems = ~(size_t)0;
			} else {
				if (blob_fixed_size > 0) {
					blob_len = blob_fixed_size;
				} else {
					assert(0);
				}
				ele_blob = GDKmalloc(blobsize(blob_len));
				ele_blob->nitems = blob_len;
				memcpy(ele_blob->data, data, blob_len);
			}
			if (BUNappend(b, ele_blob, FALSE) != GDK_SUCCEED) {
				goto bunins_failed;
			}
			GDKfree(ele_blob);
			data += ret->memory_size;
		}
		BATsetcount(b, (BUN)ret->count);
		BATsettrivprop(b);
	} else {
		switch (bat_type) {
			case TYPE_bit:
				NP_CREATE_BAT(b, bit);
				break;
			case TYPE_bte:
				NP_CREATE_BAT(b, bte);
				break;
			case TYPE_sht:
				NP_CREATE_BAT(b, sht);
				break;
			case TYPE_int:
				NP_CREATE_BAT(b, int);
				break;
			case TYPE_oid:
				NP_CREATE_BAT(b, oid);
				break;
			case TYPE_lng:
				NP_CREATE_BAT(b, lng);
				break;
			case TYPE_flt:
				NP_CREATE_BAT(b, flt);
				break;
			case TYPE_dbl:
				NP_CREATE_BAT(b, dbl);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				NP_CREATE_BAT(b, hge);
				break;
#endif
			case TYPE_str: {
				bool *mask = NULL;
				char *data = NULL;
				char *utf8_string = NULL;
				if (ret->mask_data != NULL) {
					mask = (bool *)ret->mask_data;
				}
				if (ret->array_data == NULL) {
					msg = createException(
						MAL, "pyapi.eval",
						"SQLSTATE PY000 !""No return value stored in the structure.  n");
					goto wrapup;
				}
				data = (char *)ret->array_data;

				if (ret->result_type != NPY_OBJECT) {
					utf8_string =
						GDKzalloc(utf8string_minlength + ret->memory_size + 1);
					utf8_string[utf8string_minlength + ret->memory_size] = '\0';
				}

				b = COLnew(seqbase, TYPE_str, (BUN)ret->count, TRANSIENT);
				b->tnil = 0;
				b->tnonil = 1;
				b->tkey = 0;
				b->tsorted = 0;
				b->trevsorted = 0;
				NP_INSERT_STRING_BAT(b);
				if (utf8_string)
					GDKfree(utf8_string);
				BATsetcount(b, (BUN)ret->count);
				BATsettrivprop(b);
				break;
			}
			default:
				msg = createException(MAL, "pyapi.eval",
									  "SQLSTATE PY000 !""Unrecognized BAT type %s.\n",
									  BatType_Format(bat_type));
				goto wrapup;
		}
	}
	if (ConvertableSQLType(type)) {
		BAT *result;
		msg = ConvertToSQLType(NULL, b, type, &result, &bat_type);
		if (msg != MAL_SUCCEED) {
			goto wrapup;
		}
		b = result;
	}

	return b;
bunins_failed:
	BBPunfix(b->batCacheid);
	msg = createException(MAL, "pyapi.eval", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);
wrapup:
	*return_message = msg;
	return NULL;
}

bit ConvertableSQLType(sql_subtype *sql_subtype)
{
	switch (GetSQLType(sql_subtype)) {
		case EC_DATE:
		case EC_TIME:
		case EC_TIMESTAMP:
		case EC_DEC:
			return 1;
	}
	return 0;
}

int GetSQLType(sql_subtype *sql_subtype)
{
	if (!sql_subtype)
		return -1;
	if (!sql_subtype->type)
		return -1;
	return sql_subtype->type->eclass;
}

str ConvertFromSQLType(BAT *b, sql_subtype *sql_subtype, BAT **ret_bat,
					   int *ret_type)
{
	str res = MAL_SUCCEED;
	int conv_type;

	assert(sql_subtype);
	assert(sql_subtype->type);

	switch (sql_subtype->type->eclass) {
		case EC_DATE:
		case EC_TIME:
		case EC_TIMESTAMP:
			conv_type = TYPE_str;
			break;
		case EC_DEC:
			conv_type = TYPE_dbl;
			break;
		default:
			conv_type = TYPE_str;
	}

	if (conv_type == TYPE_str) {
		BATiter li = bat_iterator(b);
		BUN p = 0, q = 0;
		char *result = NULL;
		int length = 0;
		int (*strConversion)(str *, int *, const void *) =
			BATatoms[b->ttype].atomToStr;
		*ret_bat = COLnew(0, TYPE_str, 0, TRANSIENT);
		*ret_type = conv_type;
		if (!(*ret_bat)) {
			return createException(MAL, "pyapi.eval",
								   "SQLSTATE HY001 !"MAL_MALLOC_FAIL " string conversion BAT.");
		}
		BATloop(b, p, q)
		{
			void *element = (void *)BUNtail(li, p);
			if (strConversion(&result, &length, element) == 0) {
				return createException(MAL, "pyapi.eval",
									   "SQLSTATE PY000 !""Failed to convert element to string.");
			}
			if (BUNappend(*ret_bat, result, FALSE) != GDK_SUCCEED) {
				BBPunfix((*ret_bat)->batCacheid);
				throw(MAL, "pyapi.eval", "SQLSTATE HY001 !"MAL_MALLOC_FAIL);
			}
		}
		if (result) {
			GDKfree(result);
		}
		return res;
	} else if (conv_type == TYPE_dbl) {
		int bat_type = ATOMstorage(b->ttype);
		int hpos = sql_subtype->scale;
		bat result = 0;
		// decimal values can be stored in various numeric fields, so check the
		// numeric field and convert the one it's actually stored in
		switch (bat_type) {
			case TYPE_bte:
				res = (*batbte_dec2_dbl_ptr)(&result, &hpos, &b->batCacheid);
				break;
			case TYPE_sht:
				res = (*batsht_dec2_dbl_ptr)(&result, &hpos, &b->batCacheid);
				break;
			case TYPE_int:
				res = (*batint_dec2_dbl_ptr)(&result, &hpos, &b->batCacheid);
				break;
			case TYPE_lng:
				res = (*batlng_dec2_dbl_ptr)(&result, &hpos, &b->batCacheid);
				break;
#ifdef HAVE_HGE
			case TYPE_hge:
				res = (*bathge_dec2_dbl_ptr)(&result, &hpos, &b->batCacheid);
				break;
#endif
			default:
				return createException(MAL, "pyapi.eval",
									   "Unsupported decimal storage type.");
		}
		if (res == MAL_SUCCEED) {
			*ret_bat = BATdescriptor(result);
			*ret_type = TYPE_dbl;
		} else {
			*ret_bat = NULL;
		}
		return res;
	}
	return createException(MAL, "pyapi.eval", "Unrecognized conv type.");
}

str ConvertToSQLType(Client cntxt, BAT *b, sql_subtype *sql_subtype,
					 BAT **ret_bat, int *ret_type)
{
	str res = MAL_SUCCEED;
	bat result_bat = 0;
	int digits = sql_subtype->digits;
	int scale = sql_subtype->scale;
	(void)cntxt;

	assert(sql_subtype);
	assert(sql_subtype->type);

	switch (sql_subtype->type->eclass) {
		case EC_TIMESTAMP:
			res = (*batstr_2time_timestamp_ptr)(&result_bat, &b->batCacheid,
												&digits);
			break;
		case EC_TIME:
			res = (*batstr_2time_daytime_ptr)(&result_bat, &b->batCacheid,
											  &digits);
			break;
		case EC_DATE:
			res = (*batstr_2_date_ptr)(&result_bat, &b->batCacheid);
			break;
		case EC_DEC:
			res = (*batdbl_num2dec_lng_ptr)(&result_bat, &b->batCacheid,
											&digits, &scale);
			break;
		default:
			return createException(
				MAL, "pyapi.eval",
				"Convert To SQL Type: Unrecognized SQL type %s (%d).",
				sql_subtype->type->sqlname, sql_subtype->type->eclass);
	}
	if (res == MAL_SUCCEED) {
		*ret_bat = BATdescriptor(result_bat);
		*ret_type = (*ret_bat)->ttype;
	}

	return res;
}

ssize_t PyType_Size(PyObject *obj)
{
	if (PyType_IsPyScalar(obj)) {
		return 1;
	}
	if (PyArray_Check(obj)) {
		return PyArray_Size(obj);
	}
	if (PyList_Check(obj)) {
		return Py_SIZE(obj);
	}
	return -1;
}

bit IsStandardBATType(int type)
{
	switch (type) {
		case TYPE_bit:
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_oid:
		case TYPE_lng:
		case TYPE_flt:
		case TYPE_dbl:
#ifdef HAVE_HGE
		case TYPE_hge:
#endif
		case TYPE_str:
			return 1;
		default:
			return 0;
	}
}

static void conversion_import_array(void) { _import_array(); }

str _conversion_init(void)
{
	str msg = MAL_SUCCEED;
	conversion_import_array();

#ifndef HAVE_EMBEDDED
	LOAD_SQL_FUNCTION_PTR(batbte_dec2_dbl);
	LOAD_SQL_FUNCTION_PTR(batsht_dec2_dbl);
	LOAD_SQL_FUNCTION_PTR(batint_dec2_dbl);
	LOAD_SQL_FUNCTION_PTR(batlng_dec2_dbl);
#ifdef HAVE_HGE
	LOAD_SQL_FUNCTION_PTR(bathge_dec2_dbl);
#endif
	LOAD_SQL_FUNCTION_PTR(batstr_2time_timestamp);
	LOAD_SQL_FUNCTION_PTR(batstr_2time_daytime);
	LOAD_SQL_FUNCTION_PTR(batstr_2_date);
	LOAD_SQL_FUNCTION_PTR(batdbl_num2dec_lng);
#endif
	return msg;
}
