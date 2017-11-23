/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * M. Raasveldt
 * Conversion loops used to convert from BAT <> NumPy Array
 * these are in a separate header because they are used in multiple places
 */

#define BAT_TO_NP(bat, mtpe, nptpe)                                            \
	if (copy) {                                                                \
		vararray = PyArray_EMPTY(1, elements, nptpe, 0);                       \
		memcpy(PyArray_DATA((PyArrayObject *)vararray), Tloc(bat, 0),          \
			   sizeof(mtpe) * (t_end - t_start));                              \
	} else {                                                                   \
		vararray =                                                             \
			PyArray_New(&PyArray_Type, 1, elements, nptpe, NULL,               \
						&((mtpe *)Tloc(bat, 0))[t_start], 0,                   \
						NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);       \
	}

// This #define creates a new BAT with the internal data and mask from a Numpy
// array, without copying the data
// 'bat' is a BAT* pointer, which will contain the new BAT. TYPE_'mtpe' is the
// BAT type, and 'batstore' is the heap storage type of the BAT (this should be
// STORE_CMEM or STORE_SHARED)
#define nancheck_flt(bat)                                                      \
	do {                                                                       \
		for (iu = 0; iu < ret->count; iu++) {                                  \
			if (isnan(((flt *)data)[index_offset * ret->count + iu])) {        \
				((flt *)data)[index_offset * ret->count + iu] = flt_nil;       \
				bat->tnil = 1;                                                 \
			}                                                                  \
		}                                                                      \
		bat->tnonil = !bat->tnil;                                              \
	} while (0)
#define nancheck_dbl(bat)                                                      \
	do {                                                                       \
		for (iu = 0; iu < ret->count; iu++) {                                  \
			if (isnan(((dbl *)data)[index_offset * ret->count + iu])) {        \
				((dbl *)data)[index_offset * ret->count + iu] = dbl_nil;       \
				bat->tnil = 1;                                                 \
			}                                                                  \
		}                                                                      \
		bat->tnonil = !bat->tnil;                                              \
	} while (0)
#define nancheck_bit(bat) ((void)0)
#define nancheck_bte(bat) ((void)0)
#define nancheck_sht(bat) ((void)0)
#define nancheck_int(bat) ((void)0)
#define nancheck_lng(bat) ((void)0)
#define nancheck_hge(bat) ((void)0) /* not used if no HAVE_HGE */
#define nancheck_oid(bat) ((void)0)
#if defined(HAVE_FORK) && !defined(HAVE_EMBEDDED)
#define CREATE_BAT_ZEROCOPY(bat, mtpe, batstore)                               \
	{                                                                          \
		bat = COLnew(seqbase, TYPE_##mtpe, 0, TRANSIENT);                      \
		if (bat == NULL) {                                                     \
		msg = createException(MAL, "pyapi.eval", SQLSTATE(PY000) "Cannot create column");     \
			goto wrapup;                                                       \
		}                                                                      \
		bat->tnil = 0;                                                         \
		bat->tnonil = 1;                                                       \
		bat->tkey = 0;                                                         \
		bat->tsorted = 0;                                                      \
		bat->trevsorted = 0;                                                   \
		/*Change nil values to the proper values, if they exist*/              \
		if (mask != NULL) {                                                    \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask[index_offset * ret->count + iu] == TRUE) {            \
					(*(mtpe *)(&data[(index_offset * ret->count + iu) *        \
									 ret->memory_size])) = mtpe##_nil;         \
					bat->tnil = 1;                                             \
				}                                                              \
			}                                                                  \
			bat->tnonil = 1 - bat->tnil;                                       \
		} else {                                                               \
			bat->tnil = 0;                                                     \
			bat->tnonil = 0;                                                   \
			nancheck_##mtpe(bat);                                              \
		}                                                                      \
                                                                               \
		/*When we create a BAT a small part of memory is allocated, free it*/  \
		GDKfree(bat->theap.base);                                              \
		bat->theap.base =                                                      \
			&data[(index_offset * ret->count) * ret->memory_size];             \
		bat->theap.size = ret->count * ret->memory_size;                       \
		bat->theap.free =                                                      \
			bat->theap.size; /*There are no free places in the array*/         \
		/*If index_offset > 0, we are mapping part of a multidimensional       \
		 * array.*/                                                            \
		/*The entire array will be cleared when the part with index_offset=0   \
		 * is freed*/                                                          \
		/*So we set this part of the mapping to 'NOWN'*/                       \
		if (index_offset > 0)                                                  \
			bat->theap.storage = STORE_NOWN;                                   \
		else {                                                                 \
			bat->theap.storage = batstore;                                     \
			if (batstore == STORE_MMAPABS) {                                   \
				/* If we are taking data from a MMAP file, set the filename to \
				 * the absolute path */                                        \
				char address[100];                                             \
				GDKmmapfile(address, sizeof(address), ret->mmap_id);           \
				bat->theap.filename =                                          \
					GDKfilepath(NOFARM, BATDIR, address, "tmp");               \
				ret->mmap_id = -1;                                             \
			}                                                                  \
		}                                                                      \
		bat->theap.newstorage = STORE_MEM;                                     \
		bat->batCount = ret->count;                                            \
		bat->batCapacity = ret->count;                                         \
		bat->batCopiedtodisk = false;                                          \
		/*Take over the data from the numpy array*/                            \
		if (ret->numpy_array != NULL)                                          \
			PyArray_CLEARFLAGS((PyArrayObject *)ret->numpy_array,              \
							   NPY_ARRAY_OWNDATA);                             \
	}
#else
#define CREATE_BAT_ZEROCOPY(bat, mtpe, batstore)                               \
	{                                                                          \
		bat = COLnew(seqbase, TYPE_##mtpe, 0, TRANSIENT);                      \
		if (bat == NULL) {                                                     \
			msg = createException(MAL, "pyapi.eval", SQLSTATE(PY000) "Cannot create column");     \
			goto wrapup;                                                       \
		}                                                                      \
		bat->tnil = 0;                                                         \
		bat->tnonil = 1;                                                       \
		bat->tkey = 0;                                                         \
		bat->tsorted = 0;                                                      \
		bat->trevsorted = 0;                                                   \
		/*Change nil values to the proper values, if they exist*/              \
		if (mask != NULL) {                                                    \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask[index_offset * ret->count + iu] == TRUE) {            \
					(*(mtpe *)(&data[(index_offset * ret->count + iu) *        \
									 ret->memory_size])) = mtpe##_nil;         \
					bat->tnil = 1;                                             \
				}                                                              \
			}                                                                  \
			bat->tnonil = 1 - bat->tnil;                                       \
		} else {                                                               \
			bat->tnil = 0;                                                     \
			bat->tnonil = 0;                                                   \
			nancheck_##mtpe(bat);                                              \
		}                                                                      \
		/*When we create a BAT a small part of memory is allocated, free it*/  \
		GDKfree(bat->theap.base);                                              \
		bat->theap.base =                                                      \
			&data[(index_offset * ret->count) * ret->memory_size];             \
		bat->theap.size = ret->count * ret->memory_size;                       \
		bat->theap.free =                                                      \
			bat->theap.size; /*There are no free places in the array*/         \
		/*If index_offset > 0, we are mapping part of a multidimensional       \
		 * array.*/                                                            \
		/*The entire array will be cleared when the part with index_offset=0   \
		 * is freed*/                                                          \
		/*So we set this part of the mapping to 'NOWN'*/                       \
		if (index_offset > 0)                                                  \
			bat->theap.storage = STORE_NOWN;                                   \
		else {                                                                 \
			bat->theap.storage = batstore;                                     \
		}                                                                      \
		bat->theap.newstorage = STORE_MEM;                                     \
		bat->batCount = (BUN)ret->count;                                       \
		bat->batCapacity = (BUN)ret->count;                                    \
		bat->batCopiedtodisk = false;                                          \
		/*Take over the data from the numpy array*/                            \
		if (ret->numpy_array != NULL)                                          \
			PyArray_CLEARFLAGS((PyArrayObject *)ret->numpy_array,              \
							   NPY_ARRAY_OWNDATA);                             \
	}
#endif

// This #define converts a Numpy Array to a BAT by copying the internal data to
// the BAT. It assumes the BAT 'bat' is already created with the proper size.
// This should only be used with integer data that can be cast. It assumes the
// Numpy Array has an internal array of type 'mtpe_from', and the BAT has an
// internal array of type 'mtpe_to'.
// it then does the cast by simply doing BAT[i] = (mtpe_to)
// ((mtpe_from*)NUMPY_ARRAY[i]), which only works if both mtpe_to and mtpe_from
// are integers
#define NP_COL_BAT_LOOP(bat, mtpe_to, mtpe_from, index)                        \
	{                                                                          \
		if (mask == NULL) {                                                    \
			for (iu = 0; iu < ret->count; iu++) {                              \
				((mtpe_to *)Tloc(bat, 0))[index + iu] = (mtpe_to)(             \
					*(mtpe_from *)(&data[(index_offset * ret->count + iu) *    \
										 ret->memory_size]));                  \
			}                                                                  \
		} else {                                                               \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask[index_offset * ret->count + iu] == TRUE) {            \
					bat->tnil = 1;                                             \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = mtpe_to##_nil;     \
				} else {                                                       \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = (mtpe_to)(*(       \
						mtpe_from *)(&data[(index_offset * ret->count + iu) *  \
										   ret->memory_size]));                \
				}                                                              \
			}                                                                  \
		}                                                                      \
	}
#define NP_COL_BAT_LOOPF(bat, mtpe_to, mtpe_from, index)                       \
	{                                                                          \
		if (mask == NULL) {                                                    \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (isnan((                                                    \
						(mtpe_from *)data)[index_offset * ret->count + iu])) { \
					bat->tnil = 1;                                             \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = mtpe_to##_nil;     \
				} else {                                                       \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = (mtpe_to)(         \
						(mtpe_from *)data)[index_offset * ret->count + iu];    \
				}                                                              \
			}                                                                  \
		} else {                                                               \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask[index_offset * ret->count + iu] == TRUE ||            \
					isnan((                                                    \
						(mtpe_from *)data)[index_offset * ret->count + iu])) { \
					bat->tnil = 1;                                             \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = mtpe_to##_nil;     \
				} else {                                                       \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = (mtpe_to)(*(       \
						mtpe_from *)(&data[(index_offset * ret->count + iu) *  \
										   ret->memory_size]));                \
				}                                                              \
			}                                                                  \
		}                                                                      \
	}

// This #define converts a Numpy Array to a BAT by copying the internal data to
// the BAT. It converts the data from the Numpy Array to the BAT using a
// function
// This function has to have the prototype 'bool function(void *data, size_t
// memory_size, mtpe_to *resulting_value)', and either return False (if
// conversion fails)
//  or write the value into the 'resulting_value' pointer. This is used
//  convertring strings/unicodes/python objects to numeric values.
#define NP_COL_BAT_LOOP_FUNC(bat, mtpe_to, func, ptrtpe, index)                \
	{                                                                          \
		mtpe_to value;                                                         \
		if (mask == NULL) {                                                    \
			for (iu = 0; iu < ret->count; iu++) {                              \
				msg = func((ptrtpe *)&data[(index_offset * ret->count + iu) *  \
										   ret->memory_size],                  \
						   ret->memory_size, &value);                          \
				if (msg != MAL_SUCCEED) {                                      \
					goto wrapup;                                               \
				}                                                              \
				((mtpe_to *)Tloc(bat, 0))[index + iu] = value;                 \
				if (bat->tnil == 0)                                            \
					bat->tnil = is_##mtpe_to##_nil(value);                     \
			}                                                                  \
		} else {                                                               \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask[index_offset * ret->count + iu] == TRUE) {            \
					bat->tnil = 1;                                             \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = mtpe_to##_nil;     \
				} else {                                                       \
					msg = func(                                                \
						(ptrtpe *)&data[(index_offset * ret->count + iu) *     \
										ret->memory_size],                     \
						ret->memory_size, &value);                             \
					if (msg != MAL_SUCCEED) {                                  \
						goto wrapup;                                           \
					}                                                          \
					((mtpe_to *)Tloc(bat, 0))[index + iu] = value;             \
					if (bat->tnil == 0)                                        \
						bat->tnil = is_##mtpe_to##_nil(value);                 \
				}                                                              \
			}                                                                  \
		}                                                                      \
	}


static gdk_return
convert_and_append(BAT* b, const char* text, bit force) {
	if (b->ttype == TYPE_str) {
		return BUNappend(b, text, force);	
	} else if (text == str_nil) {
		return BUNappend(b, BATatoms[b->ttype].atomNull, force);
	} else {
		void* element = NULL;
		size_t len = 0;
		gdk_return ret;

		BATatoms[b->ttype].atomFromStr(text, &len, &element);
		ret = BUNappend(b, element, force);
		GDKfree(element);
		return ret;
	}
}

// This #define is for converting a numeric numpy array into a string BAT.
// 'conv' is a function that turns a numeric value of type 'mtpe' to a char*
// array.
#define NP_COL_BAT_STR_LOOP(bat, mtpe, fmt)                                    \
	if (mask == NULL) {                                                        \
		for (iu = 0; iu < ret->count; iu++) {                                  \
			snprintf(utf8_string, utf8string_minlength, fmt,                   \
					 *((mtpe *)&data[(index_offset * ret->count + iu) *        \
									 ret->memory_size]));                      \
			if (convert_and_append(bat, utf8_string, FALSE) != GDK_SUCCEED) {           \
				msg =                                                          \
					createException(MAL, "pyapi.eval", SQLSTATE(PY000) "BUNappend failed.\n"); \
				goto wrapup;                                                   \
			}                                                                  \
		}                                                                      \
	} else {                                                                   \
		for (iu = 0; iu < ret->count; iu++) {                                  \
			if (mask[index_offset * ret->count + iu] == TRUE) {                \
				bat->tnil = 1;                                                 \
				if (convert_and_append(bat, str_nil, FALSE) != GDK_SUCCEED) {           \
					msg = createException(MAL, "pyapi.eval",                   \
										  SQLSTATE(PY000) "BUNappend failed.\n");              \
					goto wrapup;                                               \
				}                                                              \
			} else {                                                           \
				snprintf(utf8_string, utf8string_minlength, fmt,               \
						 *((mtpe *)&data[(index_offset * ret->count + iu) *    \
										 ret->memory_size]));                  \
				if (convert_and_append(bat, utf8_string, FALSE) != GDK_SUCCEED) {       \
					msg = createException(MAL, "pyapi.eval",                   \
										  SQLSTATE(PY000) "BUNappend failed.\n");              \
					goto wrapup;                                               \
				}                                                              \
			}                                                                  \
		}                                                                      \
	}

#define NP_INSERT_BAT(bat, mtpe, index)                                        \
	{                                                                          \
		switch (ret->result_type) {                                            \
			case NPY_BOOL:                                                     \
				NP_COL_BAT_LOOP(bat, mtpe, char, index);                       \
				break;                                                         \
			case NPY_BYTE:                                                     \
				NP_COL_BAT_LOOP(bat, mtpe, char, index);                       \
				break;                                                         \
			case NPY_SHORT:                                                    \
				NP_COL_BAT_LOOP(bat, mtpe, short, index);                      \
				break;                                                         \
			case NPY_INT:                                                      \
				NP_COL_BAT_LOOP(bat, mtpe, int, index);                        \
				break;                                                         \
			case NPY_LONG:                                                     \
				NP_COL_BAT_LOOP(bat, mtpe, long, index);                       \
				break;                                                         \
			case NPY_LONGLONG:                                                 \
				NP_COL_BAT_LOOP(bat, mtpe, long long, index);                  \
				break;                                                         \
			case NPY_UBYTE:                                                    \
				NP_COL_BAT_LOOP(bat, mtpe, unsigned char, index);              \
				break;                                                         \
			case NPY_USHORT:                                                   \
				NP_COL_BAT_LOOP(bat, mtpe, unsigned short, index);             \
				break;                                                         \
			case NPY_UINT:                                                     \
				NP_COL_BAT_LOOP(bat, mtpe, unsigned int, index);               \
				break;                                                         \
			case NPY_ULONG:                                                    \
				NP_COL_BAT_LOOP(bat, mtpe, unsigned long, index);              \
				break;                                                         \
			case NPY_ULONGLONG:                                                \
				NP_COL_BAT_LOOP(bat, mtpe, unsigned long long, index);         \
				break;                                                         \
			case NPY_FLOAT16:                                                  \
			case NPY_FLOAT:                                                    \
				NP_COL_BAT_LOOPF(bat, mtpe, float, index);                     \
				break;                                                         \
			case NPY_DOUBLE:                                                   \
				NP_COL_BAT_LOOPF(bat, mtpe, double, index);                    \
				break;                                                         \
			case NPY_LONGDOUBLE:                                               \
				NP_COL_BAT_LOOPF(bat, mtpe, long double, index);               \
				break;                                                         \
			case NPY_STRING:                                                   \
				NP_COL_BAT_LOOP_FUNC(bat, mtpe, str_to_##mtpe, char, index);   \
				break;                                                         \
			case NPY_UNICODE:                                                  \
				NP_COL_BAT_LOOP_FUNC(bat, mtpe, unicode_to_##mtpe,             \
									 PythonUnicodeType, index);                \
				break;                                                         \
			case NPY_OBJECT:                                                   \
				NP_COL_BAT_LOOP_FUNC(bat, mtpe, pyobject_to_##mtpe,            \
									 PyObject *, index);                       \
				break;                                                         \
			default:                                                           \
				msg = createException(                                         \
					MAL, "pyapi.eval",                                         \
					SQLSTATE(PY000) "Unrecognized type. Could not convert to %s.\n",           \
					BatType_Format(TYPE_##mtpe));                              \
				goto wrapup;                                                   \
		}                                                                      \
		bat->tnonil = 1 - bat->tnil;                                           \
	}

#define NP_INSERT_STRING_BAT(b)                                                \
	switch (ret->result_type) {                                                \
		case NPY_BOOL:                                                         \
			NP_COL_BAT_STR_LOOP(b, bit, "%hhd");                               \
			break;                                                             \
		case NPY_BYTE:                                                         \
			NP_COL_BAT_STR_LOOP(b, bte, "%hhd");                               \
			break;                                                             \
		case NPY_SHORT:                                                        \
			NP_COL_BAT_STR_LOOP(b, sht, "%hd");                                \
			break;                                                             \
		case NPY_INT:                                                          \
			NP_COL_BAT_STR_LOOP(b, int, "%d");                                 \
			break;                                                             \
		case NPY_LONG:                                                         \
			NP_COL_BAT_STR_LOOP(b, long, "%ld");                               \
			break;                                                             \
		case NPY_LONGLONG:                                                     \
			NP_COL_BAT_STR_LOOP(b, lng, LLFMT);                                \
			break;                                                             \
		case NPY_UBYTE:                                                        \
			NP_COL_BAT_STR_LOOP(b, unsigned char, "%hhu");                     \
			break;                                                             \
		case NPY_USHORT:                                                       \
			NP_COL_BAT_STR_LOOP(b, unsigned short, "%hu");                     \
			break;                                                             \
		case NPY_UINT:                                                         \
			NP_COL_BAT_STR_LOOP(b, unsigned int, "%u");                        \
			break;                                                             \
		case NPY_ULONG:                                                        \
			NP_COL_BAT_STR_LOOP(b, unsigned long, "%lu");                      \
			break;                                                             \
		case NPY_ULONGLONG:                                                    \
			NP_COL_BAT_STR_LOOP(b, ulng, ULLFMT);                              \
			break;                                                             \
		case NPY_FLOAT16:                                                      \
		case NPY_FLOAT:                                                        \
			NP_COL_BAT_STR_LOOP(b, flt, "%f");                                 \
			break;                                                             \
		case NPY_DOUBLE:                                                       \
		case NPY_LONGDOUBLE:                                                   \
			NP_COL_BAT_STR_LOOP(b, dbl, "%lf");                                \
			break;                                                             \
		case NPY_STRING:                                                       \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask != NULL &&                                            \
					(mask[index_offset * ret->count + iu]) == TRUE) {          \
					b->tnil = 1;                                               \
					if (convert_and_append(b, str_nil, FALSE) != GDK_SUCCEED) {         \
						msg = createException(MAL, "pyapi.eval",               \
											  SQLSTATE(PY000) "BUNappend failed.\n");          \
						goto wrapup;                                           \
					}                                                          \
				} else {                                                       \
					if (!string_copy(&data[(index_offset * ret->count + iu) *  \
										   ret->memory_size],                  \
									 utf8_string, ret->memory_size, false)) {  \
						msg = createException(MAL, "pyapi.eval",               \
											  SQLSTATE(PY000) "Invalid string encoding used. " \
											  "Please return a regular ASCII " \
											  "string, or a Numpy_Unicode "    \
											  "object.\n");                    \
						goto wrapup;                                           \
					}                                                          \
					if (convert_and_append(b, utf8_string, FALSE) != GDK_SUCCEED) {     \
						msg = createException(MAL, "pyapi.eval",               \
											  SQLSTATE(PY000) "BUNappend failed.\n");          \
						goto wrapup;                                           \
					}                                                          \
				}                                                              \
			}                                                                  \
			break;                                                             \
		case NPY_UNICODE:                                                      \
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask != NULL &&                                            \
					(mask[index_offset * ret->count + iu]) == TRUE) {          \
					b->tnil = 1;                                               \
					if (convert_and_append(b, str_nil, FALSE) != GDK_SUCCEED) {         \
						msg = createException(MAL, "pyapi.eval",               \
											  SQLSTATE(PY000) "BUNappend failed.\n");          \
						goto wrapup;                                           \
					}                                                          \
				} else {                                                       \
					utf32_to_utf8(                                             \
						0, ret->memory_size / 4, utf8_string,                  \
						(const Py_UNICODE                                      \
							 *)(&data[(index_offset * ret->count + iu) *       \
									  ret->memory_size]));                     \
					if (convert_and_append(b, utf8_string, FALSE) != GDK_SUCCEED) {     \
						msg = createException(MAL, "pyapi.eval",               \
											  SQLSTATE(PY000) "BUNappend failed.\n");          \
						goto wrapup;                                           \
					}                                                          \
				}                                                              \
			}                                                                  \
			break;                                                             \
		case NPY_OBJECT: {                                                     \
			/* The resulting array is an array of pointers to various python   \
			 * objects */                                                      \
			/* Because the python objects can be of any size, we need to       \
			 * allocate a different size utf8_string for every object */       \
			/* we will first loop over all the objects to get the maximum size \
			 * needed, so we only need to do one allocation */                 \
			size_t utf8_size = utf8string_minlength;                           \
			for (iu = 0; iu < ret->count; iu++) {                              \
				size_t size = utf8string_minlength;                            \
				PyObject *obj;                                                 \
				if (mask != NULL &&                                            \
					(mask[index_offset * ret->count + iu]) == TRUE)            \
					continue;                                                  \
				obj = *((PyObject **)&data[(index_offset * ret->count + iu) *  \
										   ret->memory_size]);                 \
				size = pyobject_get_size(obj);                                 \
				if (size > utf8_size)                                          \
					utf8_size = size;                                          \
			}                                                                  \
			utf8_string = GDKzalloc(utf8_size);                                \
			if (utf8_string == NULL) {			\
				msg = createException(MAL, "pyapi.eval", SQLSTATE(HY001) MAL_MALLOC_FAIL); \
				goto wrapup;				\
			}						\
			for (iu = 0; iu < ret->count; iu++) {                              \
				if (mask != NULL &&                                            \
					(mask[index_offset * ret->count + iu]) == TRUE) {          \
					b->tnil = 1;                                               \
					if (convert_and_append(b, str_nil, FALSE) != GDK_SUCCEED) {         \
						msg = createException(MAL, "pyapi.eval",               \
											  SQLSTATE(PY000) "BUNappend failed.\n");          \
						goto wrapup;                                           \
					}                                                          \
				} else {                                                       \
					/* we try to handle as many types as possible */           \
					pyobject_to_str(                                           \
						((PyObject **)&data[(index_offset * ret->count + iu) * \
											ret->memory_size]),                \
						utf8_size, &utf8_string);                              \
					if (convert_and_append(b, utf8_string, FALSE) != GDK_SUCCEED) {     \
						msg = createException(MAL, "pyapi.eval",               \
											  SQLSTATE(PY000) "BUNappend failed.\n");          \
						goto wrapup;                                           \
					}                                                          \
				}                                                              \
			}                                                                  \
			break;                                                             \
		}                                                                      \
		default:                                                               \
			msg = createException(                                             \
				MAL, "pyapi.eval",                                             \
				SQLSTATE(PY000) "Unrecognized type. Could not convert to NPY_UNICODE.\n");     \
			goto wrapup;                                                       \
	}                                                                          \
	b->tnonil = 1 - b->tnil;

#ifdef HAVE_HGE
#define NOT_HGE(mtpe) TYPE_##mtpe != TYPE_hge
#else
#define NOT_HGE(mtpe) true
#endif

// This very big #define combines all the previous #defines for one big #define
// that is responsible for converting a Numpy array (described in the PyReturn
// object 'ret')
// to a BAT of type 'mtpe'. This should only be used for numeric BATs (but can
// be used for any Numpy Array). The resulting BAT will be stored in 'bat'.
#define NP_CREATE_BAT(bat, mtpe)                                               \
	{                                                                          \
		bool *mask = NULL;                                                     \
		char *data = NULL;                                                     \
		if (ret->mask_data != NULL) {                                          \
			mask = (bool *)ret->mask_data;                                     \
		}                                                                      \
		if (ret->array_data == NULL) {                                         \
			msg =                                                              \
				createException(MAL, "pyapi.eval",                             \
								SQLSTATE(PY000) "No return value stored in the structure.\n"); \
			goto wrapup;                                                       \
		}                                                                      \
		data = (char *)ret->array_data;                                        \
		if (!copy && ret->count > 0 &&                                         \
			TYPE_##mtpe == PyType_ToBat(ret->result_type) &&                   \
			(ret->count * ret->memory_size < BUN_MAX) &&                       \
			(ret->numpy_array == NULL ||                                       \
			 PyArray_FLAGS((PyArrayObject *)ret->numpy_array) &                \
				 NPY_ARRAY_OWNDATA)) {                                         \
			/*We can only create a direct map if the numpy array type and      \
			 * target BAT type*/                                               \
			/*are identical, otherwise we have to do a conversion.*/           \
			if (ret->numpy_array == NULL) {                                    \
				CREATE_BAT_ZEROCOPY(bat, mtpe, STORE_MMAPABS);                 \
				ret->array_data = NULL;                                        \
			} else {                                                           \
				CREATE_BAT_ZEROCOPY(bat, mtpe, STORE_CMEM);                    \
			}                                                                  \
		} else {                                                               \
			bat = COLnew(seqbase, TYPE_##mtpe, (BUN)ret->count, TRANSIENT);    \
			if (bat == NULL) {                                                 \
				msg = createException(MAL, "pyapi.eval", SQLSTATE(PY000) "Cannot create column"); \
				goto wrapup;                                                   \
			}                                                                  \
			bat->tkey = 0;                                                     \
			bat->tsorted = 0;                                                  \
			bat->trevsorted = 0;                                               \
			NP_INSERT_BAT(bat, mtpe, 0);                                       \
			if (!mask) {                                                       \
				bat->tnil = 0;                                                 \
				bat->tnonil = 0;                                               \
			}                                                                  \
			BATsetcount(bat, (BUN)ret->count);                                 \
			BATsettrivprop(bat);                                               \
		}                                                                      \
	}
