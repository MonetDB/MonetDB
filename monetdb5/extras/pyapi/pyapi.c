/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

#include "pyapi.h"
#include "connection.h"

#include "unicode.h"
#include "pytypes.h"
#include "interprocess.h"
#include "type_conversion.h"
#include "formatinput.h"

#ifdef HAVE_FORK
// These libraries are used for PYTHON_MAP when forking is enabled [to start new processes and wait on them]
#include <sys/types.h>
#include <sys/wait.h>
#endif

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#define PyString_FromString PyUnicode_FromString
#define PyString_Check PyUnicode_Check
#define PyString_CheckExact PyUnicode_CheckExact
#define PyString_AsString PyUnicode_AsUTF8
#define PyString_AS_STRING PyUnicode_AsUTF8
#define PyInt_FromLong PyLong_FromLong
#define PyInt_Check PyLong_Check
#define PythonUnicodeType char
#else
#define PythonUnicodeType Py_UNICODE

#endif

const char* pyapi_enableflag = "embedded_py";
const char* verbose_enableflag = "enable_pyverbose";
const char* warning_enableflag = "enable_pywarnings";
const char* debug_enableflag = "enable_pydebug";
#ifdef _PYAPI_VERBOSE_
static bool option_verbose;
#endif
#ifdef _PYAPI_DEBUG_
static bool option_debug;
#endif
#ifdef _PYAPI_WARNINGS_
bool option_warning;
#endif

static PyObject *marshal_module = NULL;
PyObject *marshal_loads = NULL;

const int utf8string_minlength = 256;

int PyAPIEnabled(void) {
    return (GDKgetenv_istrue(pyapi_enableflag)
            || GDKgetenv_isyes(pyapi_enableflag));
}

struct _AggrParams{
    PyInput **pyinput_values;
    void ****split_bats;
    size_t **group_counts;
    str **args;
    PyObject **connection;
    PyObject **function;
    PyObject **column_types_dict;
    PyObject **result_objects;
    str *pycall;
    str msg;
    size_t base;
    size_t additional_columns;
    size_t named_columns;
    size_t columns;
    size_t group_count;
    size_t group_start;
    size_t group_end;
    MT_Id thread;
};
#define AggrParams struct _AggrParams
static void ComputeParallelAggregation(AggrParams *p);

static char* FunctionBasePath(void);
static char* FunctionBasePath(void) {
    char *basepath = GDKgetenv("function_basepath");
    if (basepath == NULL) {
        basepath = getenv("HOME");
    }
    if (basepath == NULL) {
        basepath = "";
    }
    return basepath;
}

CREATE_SQL_FUNCTION_PTR(str,batbte_dec2_dbl,(bat*, int*, bat*));
CREATE_SQL_FUNCTION_PTR(str,batsht_dec2_dbl,(bat*, int*, bat*));
CREATE_SQL_FUNCTION_PTR(str,batint_dec2_dbl,(bat*, int*, bat*));
CREATE_SQL_FUNCTION_PTR(str,batlng_dec2_dbl,(bat*, int*, bat*));
CREATE_SQL_FUNCTION_PTR(str,bathge_dec2_dbl,(bat*, int*, bat*));
CREATE_SQL_FUNCTION_PTR(str,batstr_2time_timestamp,(bat*, bat*, int*));
CREATE_SQL_FUNCTION_PTR(str,batstr_2time_daytime,(bat*, bat*, int*));
CREATE_SQL_FUNCTION_PTR(str,batstr_2_date,(bat*, bat*));
CREATE_SQL_FUNCTION_PTR(str,batdbl_num2dec_lng,(bat*, bat*, int*,int*));
CREATE_SQL_FUNCTION_PTR(str,SQLbatstr_cast,(Client, MalBlkPtr, MalStkPtr, InstrPtr));

static MT_Lock pyapiLock;
static MT_Lock queryLock;
static int pyapiInitialized = FALSE;

#ifdef HAVE_FORK
static bool python_call_active = false;
#endif

#ifdef WIN32
static bool enable_zerocopy_input = true;
static bool enable_zerocopy_output = false;
#else
static bool enable_zerocopy_input = true;
static bool enable_zerocopy_output = true;
#endif

#define BAT_TO_NP(bat, mtpe, nptpe)                                                                                                 \
        if (copy) {                                                                                                                 \
            vararray = PyArray_EMPTY(1, elements, nptpe, 0);                        \
            memcpy(PyArray_DATA((PyArrayObject*)vararray), Tloc(bat, BUNfirst(bat)), sizeof(mtpe) * (t_end - t_start));             \
        } else {                                                                                                                    \
            vararray = PyArray_New(&PyArray_Type, 1, elements,                                               \
            nptpe, NULL, &((mtpe*) Tloc(bat, BUNfirst(bat)))[t_start], 0,                                                           \
            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);                                                                        \
        }

// This #define creates a new BAT with the internal data and mask from a Numpy array, without copying the data
// 'bat' is a BAT* pointer, which will contain the new BAT. TYPE_'mtpe' is the BAT type, and 'batstore' is the heap storage type of the BAT (this should be STORE_CMEM or STORE_SHARED)
#ifdef HAVE_FORK
#define CREATE_BAT_ZEROCOPY(bat, mtpe, batstore) {                                                                      \
        bat = BATnew(TYPE_void, TYPE_##mtpe, 0, TRANSIENT);                                                             \
        BATseqbase(bat, seqbase); bat->T->nil = 0; bat->T->nonil = 1;                                                   \
        bat->tkey = 0; bat->tsorted = 0; bat->trevsorted = 0;                                                           \
        /*Change nil values to the proper values, if they exist*/                                                       \
        if (mask != NULL)                                                                                               \
        {                                                                                                               \
            for (iu = 0; iu < ret->count; iu++)                                                                         \
            {                                                                                                           \
                if (mask[index_offset * ret->count + iu] == TRUE)                                                       \
                {                                                                                                       \
                    (*(mtpe*)(&data[(index_offset * ret->count + iu) * ret->memory_size])) = mtpe##_nil;                \
                    bat->T->nil = 1;                                                                                    \
                }                                                                                                       \
            }                                                                                                           \
        }                                                                                                               \
        bat->T->nonil = 1 - bat->T->nil;                                                                                \
        /*When we create a BAT a small part of memory is allocated, free it*/                                           \
        GDKfree(bat->T->heap.base);                                                                                     \
        bat->T->heap.base = &data[(index_offset * ret->count) * ret->memory_size];                                      \
        bat->T->heap.size = ret->count * ret->memory_size;                                                              \
        bat->T->heap.free = bat->T->heap.size;  /*There are no free places in the array*/                               \
        /*If index_offset > 0, we are mapping part of a multidimensional array.*/                                       \
        /*The entire array will be cleared when the part with index_offset=0 is freed*/                                 \
        /*So we set this part of the mapping to 'NOWN'*/                                                                \
        if (index_offset > 0) bat->T->heap.storage = STORE_NOWN;                                                        \
        else {                                                                                                          \
            bat->T->heap.storage = batstore;                                                                            \
            if (batstore == STORE_MMAPABS) {                                                                            \
                /* If we are taking data from a MMAP file, set the filename to the absolute path */                     \
                char address[999];                                                                                      \
                snprintf_mmap_file(address, 999, ret->mmap_id);                                                         \
                bat->T->heap.filename = GDKfilepath(NOFARM, BATDIR, address, "tmp");                                    \
                ret->mmap_id = -1;                                                                                      \
            }                                                                                                           \
        }                                                                                                               \
        bat->T->heap.newstorage = STORE_MEM;                                                                            \
        bat->S->count = ret->count;                                                                                     \
        bat->S->capacity = ret->count;                                                                                  \
        bat->S->copiedtodisk = false;                                                                                   \
        /*Take over the data from the numpy array*/                                                                     \
        if (ret->numpy_array != NULL) PyArray_CLEARFLAGS((PyArrayObject*)ret->numpy_array, NPY_ARRAY_OWNDATA);          \
    }
#else
#define CREATE_BAT_ZEROCOPY(bat, mtpe, batstore) {                                                                      \
        bat = BATnew(TYPE_void, TYPE_##mtpe, 0, TRANSIENT);                                                             \
        BATseqbase(bat, seqbase); bat->T->nil = 0; bat->T->nonil = 1;                                                   \
        bat->tkey = 0; bat->tsorted = 0; bat->trevsorted = 0;                                                           \
        /*Change nil values to the proper values, if they exist*/                                                       \
        if (mask != NULL)                                                                                               \
        {                                                                                                               \
            for (iu = 0; iu < ret->count; iu++)                                                                         \
            {                                                                                                           \
                if (mask[index_offset * ret->count + iu] == TRUE)                                                       \
                {                                                                                                       \
                    (*(mtpe*)(&data[(index_offset * ret->count + iu) * ret->memory_size])) = mtpe##_nil;                \
                    bat->T->nil = 1;                                                                                    \
                }                                                                                                       \
            }                                                                                                           \
        }                                                                                                               \
        bat->T->nonil = 1 - bat->T->nil;                                                                                \
        /*When we create a BAT a small part of memory is allocated, free it*/                                           \
        GDKfree(bat->T->heap.base);                                                                                     \
        bat->T->heap.base = &data[(index_offset * ret->count) * ret->memory_size];                                      \
        bat->T->heap.size = ret->count * ret->memory_size;                                                              \
        bat->T->heap.free = bat->T->heap.size;  /*There are no free places in the array*/                               \
        /*If index_offset > 0, we are mapping part of a multidimensional array.*/                                       \
        /*The entire array will be cleared when the part with index_offset=0 is freed*/                                 \
        /*So we set this part of the mapping to 'NOWN'*/                                                                \
        if (index_offset > 0) bat->T->heap.storage = STORE_NOWN;                                                        \
        else {                                                                                                          \
            bat->T->heap.storage = batstore;                                                                            \
        }                                                                                                               \
        bat->T->heap.newstorage = STORE_MEM;                                                                            \
        bat->S->count = (BUN) ret->count;                                                                                     \
        bat->S->capacity = (BUN) ret->count;                                                                                  \
        bat->S->copiedtodisk = false;                                                                                   \
        /*Take over the data from the numpy array*/                                                                     \
        if (ret->numpy_array != NULL) PyArray_CLEARFLAGS((PyArrayObject*)ret->numpy_array, NPY_ARRAY_OWNDATA);          \
    }
#endif

// This #define converts a Numpy Array to a BAT by copying the internal data to the BAT. It assumes the BAT 'bat' is already created with the proper size.
// This should only be used with integer data that can be cast. It assumes the Numpy Array has an internal array of type 'mtpe_from', and the BAT has an internal array of type 'mtpe_to'.
// it then does the cast by simply doing BAT[i] = (mtpe_to) ((mtpe_from*)NUMPY_ARRAY[i]), which only works if both mtpe_to and mtpe_from are integers
#define NP_COL_BAT_LOOP(bat, mtpe_to, mtpe_from) {                                                                                               \
    if (mask == NULL)                                                                                                                            \
    {                                                                                                                                            \
        for (iu = 0; iu < ret->count; iu++)                                                                                                      \
        {                                                                                                                                        \
            ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[iu] = (mtpe_to)(*(mtpe_from*)(&data[(index_offset * ret->count + iu) * ret->memory_size]));    \
        }                                                                                                                                        \
    }                                                                                                                                            \
    else                                                                                                                                         \
    {                                                                                                                                            \
        for (iu = 0; iu < ret->count; iu++)                                                                                                      \
        {                                                                                                                                        \
            if (mask[index_offset * ret->count + iu] == TRUE)                                                                                    \
            {                                                                                                                                    \
                bat->T->nil = 1;                                                                                                                 \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[iu] = mtpe_to##_nil;                                                                       \
            }                                                                                                                                    \
            else                                                                                                                                 \
            {                                                                                                                                    \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[iu] = (mtpe_to)(*(mtpe_from*)(&data[(index_offset * ret->count + iu) * ret->memory_size]));\
            }                                                                                                                                    \
        }                                                                                                                                        \
    } }

// This #define converts a Numpy Array to a BAT by copying the internal data to the BAT. It converts the data from the Numpy Array to the BAT using a function
// This function has to have the prototype 'bool function(void *data, size_t memory_size, mtpe_to *resulting_value)', and either return False (if conversion fails)
//  or write the value into the 'resulting_value' pointer. This is used convertring strings/unicodes/python objects to numeric values.
#define NP_COL_BAT_LOOP_FUNC(bat, mtpe_to, func, ptrtpe) {                                                                                                    \
    mtpe_to value;                                                                                                                                    \
    if (mask == NULL)                                                                                                                                 \
    {                                                                                                                                                 \
        for (iu = 0; iu < ret->count; iu++)                                                                                                           \
        {                                                                                                                                             \
            msg = func((ptrtpe*)&data[(index_offset * ret->count + iu) * ret->memory_size], ret->memory_size, &value);                                \
            if (msg != MAL_SUCCEED) {                                                                                                                 \
                goto wrapup;                                                                                                                          \
            }                                                                                                                                         \
            ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[iu] = value;                                                                                        \
        }                                                                                                                                             \
    }                                                                                                                                                 \
    else                                                                                                                                              \
    {                                                                                                                                                 \
        for (iu = 0; iu < ret->count; iu++)                                                                                                           \
        {                                                                                                                                             \
            if (mask[index_offset * ret->count + iu] == TRUE)                                                                                         \
            {                                                                                                                                         \
                bat->T->nil = 1;                                                                                                                      \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[iu] = mtpe_to##_nil;                                                                            \
            }                                                                                                                                         \
            else                                                                                                                                      \
            {                                                                                                                                         \
                msg = func((ptrtpe*)&data[(index_offset * ret->count + iu) * ret->memory_size], ret->memory_size, &value);                            \
                if (msg != MAL_SUCCEED) {                                                                                                             \
                    goto wrapup;                                                                                                                      \
                }                                                                                                                                     \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[iu] = value;                                                                                    \
            }                                                                                                                                         \
        }                                                                                                                                             \
    } }


// This #define is for converting a numeric numpy array into a string BAT. 'conv' is a function that turns a numeric value of type 'mtpe' to a char* array.
#define NP_COL_BAT_STR_LOOP(bat, mtpe, fmt)                                                                                                           \
    if (mask == NULL)                                                                                                                                 \
    {                                                                                                                                                 \
        for (iu = 0; iu < ret->count; iu++)                                                                                                           \
        {                                                                                                                                             \
            snprintf(utf8_string, utf8string_minlength, fmt, *((mtpe*)&data[(index_offset * ret->count + iu) * ret->memory_size]));                   \
            BUNappend(bat, utf8_string, FALSE);                                                                                                       \
        }                                                                                                                                             \
    }                                                                                                                                                 \
    else                                                                                                                                              \
    {                                                                                                                                                 \
        for (iu = 0; iu < ret->count; iu++)                                                                                                           \
        {                                                                                                                                             \
            if (mask[index_offset * ret->count + iu] == TRUE)                                                                                         \
            {                                                                                                                                         \
                bat->T->nil = 1;                                                                                                                      \
                BUNappend(b, str_nil, FALSE);                                                                                                         \
            }                                                                                                                                         \
            else                                                                                                                                      \
            {                                                                                                                                         \
                snprintf(utf8_string, utf8string_minlength, fmt, *((mtpe*)&data[(index_offset * ret->count + iu) * ret->memory_size]));               \
                BUNappend(bat, utf8_string, FALSE);                                                                                                   \
            }                                                                                                                                         \
        }                                                                                                                                             \
    }

// This is here so we can remove the option_zerocopyoutput from the zero copy conditionals if testing is disabled

#ifdef HAVE_HGE
#define NOT_HGE(mtpe) TYPE_##mtpe != TYPE_hge
#else
#define NOT_HGE(mtpe) true
#endif

// This very big #define combines all the previous #defines for one big #define that is responsible for converting a Numpy array (described in the PyReturn object 'ret')
// to a BAT of type 'mtpe'. This should only be used for numeric BATs (but can be used for any Numpy Array). The resulting BAT will be stored in 'bat'.
#define NP_CREATE_BAT(bat, mtpe) {                                                                                                                             \
        bool *mask = NULL;                                                                                                                                     \
        char *data = NULL;                                                                                                                                     \
        if (ret->mask_data != NULL) {                                                                                                                          \
            mask = (bool*) ret->mask_data;                                                                                                                     \
        }                                                                                                                                                      \
        if (ret->array_data == NULL) {                                                                                                                         \
            msg = createException(MAL, "pyapi.eval", "No return value stored in the structure.\n");                                                            \
            goto wrapup;                                                                                                                                       \
        }                                                                                                                                                      \
        data = (char*) ret->array_data;                                                                                                                        \
        if (!copy && ret->count > 0 && TYPE_##mtpe == PyType_ToBat(ret->result_type) && (ret->count * ret->memory_size < BUN_MAX) &&           \
             (ret->numpy_array == NULL || PyArray_FLAGS((PyArrayObject*)ret->numpy_array) & NPY_ARRAY_OWNDATA)) {                      \
            /*We can only create a direct map if the numpy array type and target BAT type*/                                                                    \
            /*are identical, otherwise we have to do a conversion.*/                                                                                           \
            if (ret->numpy_array == NULL) {                                                                                                                    \
                VERBOSE_MESSAGE("- Zero copy (Map)!\n");                                                                                                       \
                CREATE_BAT_ZEROCOPY(bat, mtpe, STORE_MMAPABS);                                                                                                 \
                ret->array_data = NULL;                                                                                                                        \
            } else {                                                                                                                                           \
                VERBOSE_MESSAGE("- Zero copy!\n");                                                                                                             \
                CREATE_BAT_ZEROCOPY(bat, mtpe, STORE_CMEM);                                                                                                    \
            }                                                                                                                                                  \
        } else {                                                                                                                                               \
            bat = BATnew(TYPE_void, TYPE_##mtpe, (BUN) ret->count, TRANSIENT);                                                                                       \
            BATseqbase(bat, seqbase); bat->T->nil = 0; bat->T->nonil = 1;                                                                                      \
            if (NOT_HGE(mtpe) && TYPE_##mtpe != PyType_ToBat(ret->result_type)) WARNING_MESSAGE("!PERFORMANCE WARNING: You are returning a Numpy Array of type %s, which has to be converted to a BAT of type %s. If you return a Numpy\
Array of type %s no copying will be needed.\n", PyType_Format(ret->result_type), BatType_Format(TYPE_##mtpe), PyType_Format(BatType_ToPyType(TYPE_##mtpe)));   \
            bat->tkey = 0; bat->tsorted = 0; bat->trevsorted = 0;                                                                                              \
            switch(ret->result_type)                                                                                                                           \
            {                                                                                                                                                  \
                case NPY_BOOL:       NP_COL_BAT_LOOP(bat, mtpe, char); break;                                                                                   \
                case NPY_BYTE:       NP_COL_BAT_LOOP(bat, mtpe, char); break;                                                                                   \
                case NPY_SHORT:      NP_COL_BAT_LOOP(bat, mtpe, short); break;                                                                                   \
                case NPY_INT:        NP_COL_BAT_LOOP(bat, mtpe, int); break;                                                                                   \
                case NPY_LONG:       NP_COL_BAT_LOOP(bat, mtpe, long); break;                                                                                   \
                case NPY_LONGLONG:   NP_COL_BAT_LOOP(bat, mtpe, long long); break;                                                                                   \
                case NPY_UBYTE:      NP_COL_BAT_LOOP(bat, mtpe, unsigned char); break;                                                                         \
                case NPY_USHORT:     NP_COL_BAT_LOOP(bat, mtpe, unsigned short); break;                                                                        \
                case NPY_UINT:       NP_COL_BAT_LOOP(bat, mtpe, unsigned int); break;                                                                          \
                case NPY_ULONG:      NP_COL_BAT_LOOP(bat, mtpe, unsigned long); break;                                                                         \
                case NPY_ULONGLONG:  NP_COL_BAT_LOOP(bat, mtpe, unsigned long long); break;                                                                         \
                case NPY_FLOAT16:                                                                                                                              \
                case NPY_FLOAT:      NP_COL_BAT_LOOP(bat, mtpe, float); break;                                                                                   \
                case NPY_DOUBLE:     NP_COL_BAT_LOOP(bat, mtpe, double); break;                                                                                   \
                case NPY_LONGDOUBLE: NP_COL_BAT_LOOP(bat, mtpe, long double); break;                                                                                   \
                case NPY_STRING:     NP_COL_BAT_LOOP_FUNC(bat, mtpe, str_to_##mtpe, char); break;                                                                    \
                case NPY_UNICODE:    NP_COL_BAT_LOOP_FUNC(bat, mtpe, unicode_to_##mtpe, PythonUnicodeType); break;                                                                \
                case NPY_OBJECT:     NP_COL_BAT_LOOP_FUNC(bat, mtpe, pyobject_to_##mtpe, PyObject*); break;                                                               \
                default:                                                                                                                                       \
                    msg = createException(MAL, "pyapi.eval", "Unrecognized type. Could not convert to %s.\n", BatType_Format(TYPE_##mtpe));                    \
                    goto wrapup;                                                                                                                               \
            }                                                                                                                                                  \
            bat->T->nonil = 1 - bat->T->nil;                                                                                                                   \
            BATsetcount(bat, (BUN) ret->count);                                                                                                                      \
            BATsettrivprop(bat);                                                                                                                               \
        }                                                                                                                                                      \
    }

#define NP_SPLIT_BAT(tpe) {                                                       \
    tpe ***ptr = (tpe***)split_bats;                                              \
    size_t *temp_indices;                                                         \
    tpe *batcontent = (tpe*)basevals;                                             \
    /* allocate space for split BAT */                                            \
    for(group_it = 0; group_it < group_count; group_it++) {                       \
        ptr[group_it][i] = GDKzalloc(group_counts[group_it] * sizeof(tpe));       \
    }                                                                             \
    /*iterate over the elements of the current BAT*/                              \
    temp_indices = GDKzalloc(sizeof(lng) * group_count);                          \
    for(element_it = 0; element_it < elements; element_it++) {                    \
        /*group of current element*/                                              \
        oid group = aggr_group_arr[element_it];                                   \
        /*append current element to proper group*/                                \
        ptr[group][i][temp_indices[group]++] = batcontent[element_it];            \
    }                                                                             \
    GDKfree(temp_indices);                                                        \
} 

str
PyAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped, bit mapped);

str
PyAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    return PyAPIeval(cntxt, mb, stk, pci, 0, 0);
}

str
PyAPIevalStdMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    return PyAPIeval(cntxt, mb, stk, pci, 0, 1);
}

str
PyAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    return PyAPIeval(cntxt, mb, stk, pci, 1, 0);
}

str
PyAPIevalAggrMap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    return PyAPIeval(cntxt, mb, stk, pci, 1, 1);
}

static char *PyError_CreateException(char *error_text, char *pycall);

int GetSQLType(sql_subtype *sql_subtype);
bit ConvertableSQLType(sql_subtype *sql_subtype);
str ConvertFromSQLType(Client cntxt, BAT *b, sql_subtype *sql_subtype, BAT **ret_bat, int *ret_type);
str ConvertToSQLType(Client cntxt, BAT *b, sql_subtype *sql_subtype, BAT **ret_bat, int *ret_type);

//! The main PyAPI function, this function does everything PyAPI related
//! It takes as argument a bunch of input BATs, a python function, and outputs a number of BATs
//! This function follows the following pipeline
//! [PARSE_CODE] Step 1: It parses the Python source code and corrects any wrong indentation, or converts the source code into a PyCodeObject if the source code is encoded as such
//! [CONVERT_BAT] Step 2: It converts the input BATs into Numpy Arrays
//! [EXECUTE_CODE] Step 3: It executes the Python code using the Numpy arrays as arguments
//! [RETURN_VALUES] Step 4: It collects the return values and converts them back into BATs
//! If 'mapped' is set to True, it will fork a separate process at [FORK_PROCESS] that executes Step 1-3, the process will then write the return values into memory mapped files and exit, then Step 4 is executed by the main process
str PyAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped, bit mapped) {
    sql_func * sqlfun;
    str exprStr;

    const int additional_columns = 3;
    int i = 1, ai = 0;
    char* pycall = NULL;
    str *args;
    char *msg = MAL_SUCCEED;
    BAT *b = NULL;
    node * argnode;
    int seengrp = FALSE;
    PyObject *pArgs = NULL, *pColumns = NULL, *pColumnTypes = NULL, *pConnection, *pResult = NULL; // this is going to be the parameter tuple
    PyObject *code_object = NULL;
    PyReturn *pyreturn_values = NULL;
    PyInput *pyinput_values = NULL;
    oid seqbase = 0;
#ifdef HAVE_FORK
    char *mmap_ptr;
    QueryStruct *query_ptr = NULL;
    int query_sem = -1;
    int mmap_id = -1;
    size_t memory_size = 0;
    bool child_process = false;
    bool holds_gil = !mapped;
    void **mmap_ptrs = NULL;
    size_t *mmap_sizes = NULL;
#endif
    bit varres;
    int retcols;
    bool gstate = 0;
    int unnamedArgs = 0;
    bit parallel_aggregation = grouped && mapped;
    int argcount = pci->argc;

    mapped = 0;

#ifndef HAVE_FORK
    (void) mapped;
#endif

    if (!PyAPIEnabled()) {
        throw(MAL, "pyapi.eval",
              "Embedded Python has not been enabled. Start server with --set %s=true",
              pyapi_enableflag);
    }

    if (!pyapiInitialized) {
        throw(MAL, "pyapi.eval",
              "Embedded Python is enabled but an error was thrown during initialization.");
    }

    sqlfun = *(sql_func**) getArgReference(stk, pci, pci->retc);
    exprStr = *getArgReference_str(stk, pci, pci->retc + 1);
    varres = sqlfun ? sqlfun->varres : 0;
    retcols = !varres ? pci->retc : -1;

    VERBOSE_MESSAGE("PyAPI Start\n");

    args = (str*) GDKzalloc(pci->argc * sizeof(str));
    pyreturn_values = GDKzalloc(pci->retc * sizeof(PyReturn));
    if (args == NULL || pyreturn_values == NULL) {
        throw(MAL, "pyapi.eval", MAL_MALLOC_FAIL" arguments.");
    }

    if ((pci->argc - (pci->retc + 2)) * sizeof(PyInput) > 0) {
        pyinput_values = GDKzalloc((pci->argc - (pci->retc + 2)) * sizeof(PyInput));

        if (pyinput_values == NULL) {
            GDKfree(args); GDKfree(pyreturn_values);
            throw(MAL, "pyapi.eval", MAL_MALLOC_FAIL" input values.");
        }
    }

    // Analyse the SQL_Func structure to get the parameter names
    if (sqlfun != NULL && sqlfun->ops->cnt > 0) {
        unnamedArgs = pci->retc + 2;
        argnode = sqlfun->ops->h;
        while (argnode) {
            char* argname = ((sql_arg*) argnode->data)->name;
            args[unnamedArgs++] = GDKstrdup(argname);
            argnode = argnode->next;
        }
        if (parallel_aggregation && unnamedArgs < pci->argc) {
            argcount = unnamedArgs;
        } else {
            parallel_aggregation = 0;
        }
    } else {
        parallel_aggregation = 0;
    }

    // We name all the unknown arguments, if grouping is enabled the first unknown argument that is the group variable, we name this 'aggr_group'
    for (i = pci->retc + 2; i < argcount; i++) {
        if (args[i] == NULL) {
            if (!seengrp && grouped) {
                args[i] = GDKstrdup("aggr_group");
                seengrp = TRUE;
            } else {
                char argbuf[64];
                snprintf(argbuf, sizeof(argbuf), "arg%i", i - pci->retc - 1);
                args[i] = GDKstrdup(argbuf);
            }
        }
    }

    // Construct PyInput objects, we do this before any multiprocessing because there is some locking going on in there, and locking + forking = bad idea (a thread can fork while another process is in the lock, which means we can get stuck permanently)
    argnode = sqlfun && sqlfun->ops->cnt > 0 ? sqlfun->ops->h : NULL;
    for (i = pci->retc + 2; i < argcount; i++) {
        PyInput *inp = &pyinput_values[i - (pci->retc + 2)];
        if (!isaBatType(getArgType(mb,pci,i))) {
            inp->scalar = true;
            inp->bat_type = getArgType(mb, pci, i);
            inp->count = 1;
            if (inp->bat_type == TYPE_str) {
                inp->dataptr = getArgReference_str(stk, pci, i);
            }
            else {
                inp->dataptr = getArgReference(stk, pci, i);
            }
        } else {
            b = BATdescriptor(*getArgReference_bat(stk, pci, i));
            if (b == NULL) {
                msg = createException(MAL, "pyapi.eval", "The BAT passed to the function (argument #%d) is NULL.\n", i - (pci->retc + 2) + 1);
                goto wrapup;
            }
            seqbase = b->H->seq;
            inp->count = BATcount(b);
            inp->bat_type = ATOMstorage(getColumnType(getArgType(mb,pci,i)));
            inp->bat = b;
        }
        if (argnode) {
            inp->sql_subtype = &((sql_arg*)argnode->data)->type;

            if (ConvertableSQLType(inp->sql_subtype)) { // if the sql type is set, we have to do some conversion
                if (inp->scalar) {
                    // todo: scalar SQL types
                    msg = PyError_CreateException("Scalar SQL types haven't been implemented yet... sorry", NULL);
                    goto wrapup;
                } else {
                    BAT *ret_bat = NULL;
                    msg = ConvertFromSQLType(cntxt, inp->bat, inp->sql_subtype, &ret_bat, &inp->bat_type);
                    if (msg != MAL_SUCCEED) {
                        goto wrapup;
                    }
                    inp->bat = ret_bat;
                }
            }
            b = inp->bat;
            argnode = argnode->next;
        }
    }

#ifdef HAVE_FORK
    if (!mapped) {
        MT_lock_set(&pyapiLock);
        if (python_call_active) {
            mapped = true;
            holds_gil = false;
        }
        else {
            python_call_active = true;
        }
        MT_lock_unset(&pyapiLock);
    }
#endif

#ifdef HAVE_FORK
    /*[FORK_PROCESS]*/
    if (mapped)
    {
        lng pid;
        //we need 3 + pci->retc * 2 shared memory spaces
        //the first is for the header information
        //the second for query struct information
        //the third is for query results
        //the remaining pci->retc * 2 is one for each return BAT, and one for each return mask array
        int mmap_count = 4 + pci->retc * 2;

        //create initial shared memory
        MT_lock_set(&pyapiLock);
        mmap_id = get_unique_id(mmap_count); 
        MT_lock_unset(&pyapiLock);

        mmap_ptrs = GDKzalloc(mmap_count * sizeof(void*));
        mmap_sizes = GDKzalloc(mmap_count * sizeof(size_t));
        if (mmap_ptrs == NULL || mmap_sizes == NULL) {
            msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL" mmap values.");
            goto wrapup;
        }

        VERBOSE_MESSAGE("Creating multiple processes.\n");

        memory_size = pci->retc * sizeof(ReturnBatDescr); //the memory size for the header files, each process has one per return value

        VERBOSE_MESSAGE("Initializing shared memory.\n");

        assert(memory_size > 0);
        //create the shared memory for the header
        MT_lock_set(&pyapiLock);
        msg = init_mmap_memory(mmap_id, 0, memory_size, &mmap_ptrs, &mmap_sizes, NULL);
        MT_lock_unset(&pyapiLock);
        if (msg != MAL_SUCCEED) {
            goto wrapup;
        }
        mmap_ptr = mmap_ptrs[0];

        //create the cross-process semaphore used for signaling queries
        //we need two semaphores
        //the main process waits on the first one (exiting when a query is requested or the child process is done)
        //the forked process waits for the second one when it requests a query (waiting for the result of the query)
        msg = create_process_semaphore(mmap_id, 2, &query_sem);
        if (msg != MAL_SUCCEED) {
            goto wrapup;
        }

        //create the shared memory space for queries
        MT_lock_set(&pyapiLock);
        msg = init_mmap_memory(mmap_id, 1, sizeof(QueryStruct), &mmap_ptrs, &mmap_sizes, NULL);
        MT_lock_unset(&pyapiLock);
        if (msg != MAL_SUCCEED) {
            goto wrapup;
        }
        query_ptr = mmap_ptrs[1];
        query_ptr->pending_query = false;
        query_ptr->query[0] = '\0';
        query_ptr->mmapid = -1;
        query_ptr->memsize = 0;

        VERBOSE_MESSAGE("Waiting to fork.\n");
        //fork
        MT_lock_set(&pyapiLock);
        gstate = Python_ObtainGIL(); // we need the GIL before forking, otherwise it can get stuck in the forked child
        VERBOSE_MESSAGE("Start forking.\n");
        if ((pid = fork()) < 0)
        {
            msg = createException(MAL, "pyapi.eval", "Failed to fork process");
            MT_lock_unset(&pyapiLock);

            goto wrapup;
        }
        else if (pid == 0)
        {
            child_process = true;
            query_ptr = NULL;
            msg = init_mmap_memory(mmap_id, 1, sizeof(QueryStruct), NULL, NULL, (char**)&query_ptr);
            if (msg != MAL_SUCCEED) {
                goto wrapup;
            }
        } else {
            gstate = Python_ReleaseGIL(gstate);
        }
        if (!child_process) {
            //main process
            int status;
            bool success = true;
            bool sem_success = false;
            pid_t retcode = 0;

            // release the GIL in the main process
            MT_lock_unset(&pyapiLock);

            while (true) {
                //wait for the child to finish
                //note that we use a timeout here in case the child crashes for some reason
                //in this case the semaphore value is never increased, so we would be stuck otherwise
                msg = change_semaphore_value_timeout(query_sem, 0, -1, 100, &sem_success); 
                if (msg != MAL_SUCCEED){
                    goto wrapup;
                }
                if (sem_success) 
                {
                    if (query_ptr->pending_query) {
                        // we have to handle a query for the forked process
                        res_table *output = NULL;
                        // we only perform one query at a time, otherwise bad things happen
                        // todo: we might only have to limit this to 'one query per client', rather than 'one query per pyapi'
                        MT_lock_set(&queryLock);
                        // execute the query
                        msg = _connection_query(cntxt, query_ptr->query , &output);
                        if (msg != MAL_SUCCEED) {
                            MT_lock_unset(&queryLock);
                            change_semaphore_value(query_sem, 1, 1); // free the forked process so it can exit in case of failure
                            goto wrapup;
                        }
                        MT_lock_unset(&queryLock);

                        query_ptr->memsize = 0;
                        query_ptr->nr_cols = 0;
                        query_ptr->mmapid = -1;

                        if (output != NULL && output->nr_cols > 0) 
                        {
                            // copy the return values into shared memory if there are any
                            size_t size = 0;
                            size_t position = 0;
                            char *result_ptr;

                            for (i = 0; i < output->nr_cols; i++) {
                                res_col col = output->cols[i];
                                BAT *b = BATdescriptor(col.b);
                                sql_subtype *subtype = &col.type;

                                // if the sql type is set, we have to do some conversion
                                // we do this before sending the BATs to the other process, otherwise there are complaints about not being able to find a BATdescriptor
                                if (ConvertableSQLType(subtype)) {
                                    BAT *ret_bat = NULL;
                                    int ret_type;
                                    msg = ConvertFromSQLType(cntxt, b, subtype, &ret_bat, &ret_type);
                                    if (msg != MAL_SUCCEED) {
                                        goto wrapup;
                                    }
                                    output->cols[i].b = ret_bat->batCacheid;
                                }
                                BBPunfix(col.b);
                            }

                            // first obtain the total size of the shared memory region
                            // the region is structured as [COLNAME][BAT][COLREC][BATREC][DATA]([VHEAP][VHEAPDATA])
                            for (i = 0; i < output->nr_cols; i++) {
                                res_col col = output->cols[i];
                                BAT* b = BATdescriptor(col.b);
                                size_t batsize = b->T->width * BATcount(b);
                                char *colname = col.name;

                                size += strlen(colname) + 1;                                          //[COLNAME]
                                size += sizeof(BAT);                                                  //[BAT]
                                size += sizeof(COLrec);                                               //[COLrec]
                                size += sizeof(BATrec);                                               //[BATrec]
                                size += batsize;                                                      //[DATA]
                                
                                if (b->T->vheap != NULL) {
                                    size += sizeof(Heap);                                             //[VHEAP]
                                    size += b->T->vheap->size;                                        //[VHEAPDATA]
                                }
                                BBPunfix(b->batCacheid);
                            }

                            query_ptr->memsize = size;
                            query_ptr->nr_cols = output->nr_cols;

                            // create the actual shared memory region
                            MT_lock_set(&pyapiLock);
                            query_ptr->mmapid = get_unique_id(1); 
                            MT_lock_unset(&pyapiLock);

                            msg = init_mmap_memory(query_ptr->mmapid, 0, size, NULL, NULL, &result_ptr);

                            if (msg != MAL_SUCCEED) {
                                _connection_cleanup_result(output);
                                change_semaphore_value(query_sem, 1, 1);
                                goto wrapup;
                            }

                            // copy the data into the shared memory region
                            for (i = 0; i < output->nr_cols; i++) {
                                res_col col = output->cols[i];
                                BAT* b = BATdescriptor(col.b);
                                char *colname = col.name;
                                size_t batsize = b->T->width * BATcount(b);

                                //[COLNAME]
                                memcpy(result_ptr + position, colname, strlen(colname) + 1); 
                                position += strlen(colname) + 1;
                                //[BAT]
                                memcpy(result_ptr + position, b, sizeof(BAT)); 
                                position += sizeof(BAT);
                                //[COLREC]
                                memcpy(result_ptr + position, b->T, sizeof(COLrec)); 
                                position += sizeof(COLrec);
                                //[BATREC]
                                memcpy(result_ptr + position, b->S, sizeof(BATrec)); 
                                position += sizeof(BATrec);
                                //[DATA]
                                memcpy(result_ptr + position, Tloc(b, BUNfirst(b)), batsize);
                                position += batsize;
                                if (b->T->vheap != NULL) {
                                    //[VHEAP]
                                    memcpy(result_ptr + position, b->T->vheap, sizeof(Heap));
                                    position += sizeof(Heap);
                                    //[VHEAPDATA]
                                    memcpy(result_ptr + position, b->T->vheap->base, b->T->vheap->size);
                                    position += b->T->vheap->size;
                                }
                                BBPunfix(b->batCacheid);
                            }
                            //detach the main process from this piece of shared memory so the child process can delete it
                            _connection_cleanup_result(output);
                        }
                        //signal that we are finished processing this query
                        query_ptr->pending_query = false;
                        //after putting the return values in shared memory return control to the other process
                        change_semaphore_value(query_sem, 1, 1);
                        continue;
                    } else {
                        break;
                    }
                }
                retcode = waitpid(pid, &status, WNOHANG);
                if (retcode > 0) break; //we have successfully waited for the child to exit
                if (retcode < 0) {
                    // error message
                    char *err = strerror(errno);
                    sem_success = 0;
                    errno = 0;
                    msg = createException(MAL, "waitpid", "Error calling waitpid(%llu, &status, WNOHANG): %s", pid, err);
                    break;
                }
            }
            if (sem_success)
                waitpid(pid, &status, 0);

            if (status != 0)
                success = false;

            if (!success)
            {
                //a child failed, get the error message from the child
                ReturnBatDescr *descr = &(((ReturnBatDescr*)mmap_ptr)[0]);
                char *err_ptr;

                if (descr->bat_size == 0) {
                    msg = createException(MAL, "pyapi.eval", "Failure in child process with unknown error.");
                } else {
                    msg = init_mmap_memory(mmap_id, 3, descr->bat_size, &mmap_ptrs, &mmap_sizes, &err_ptr);
                    if (msg == MAL_SUCCEED) {
                        msg = createException(MAL, "pyapi.eval", "%s", err_ptr);
                    }
                }
                goto wrapup;
            }
            VERBOSE_MESSAGE("Finished waiting for child process.\n");

            //collect return values
            for(i = 0; i < pci->retc; i++)
            {
                PyReturn *ret = &pyreturn_values[i];
                ReturnBatDescr *descr = &(((ReturnBatDescr*)mmap_ptr)[i]);
                size_t total_size = 0;
                bool has_mask = false;
                ret->count = 0;
                ret->mmap_id = mmap_id + i + 3;
                ret->memory_size = 0;
                ret->result_type = 0;

                ret->count = descr->bat_count;
                total_size = descr->bat_size;

                ret->memory_size = descr->element_size;
                ret->result_type = descr->npy_type;
                has_mask = has_mask || descr->has_mask;

                //get the shared memory address for this return value
                VERBOSE_MESSAGE("Parent requesting memory at id %d of size %zu\n", mmap_id + (i + 3), total_size);

                assert(total_size > 0);
                MT_lock_set(&pyapiLock);
                msg = init_mmap_memory(mmap_id, i + 3, total_size, &mmap_ptrs, &mmap_sizes, NULL);
                MT_lock_unset(&pyapiLock);
                if (msg != MAL_SUCCEED) {
                    goto wrapup;
                }
                ret->array_data = mmap_ptrs[i + 3];
                ret->mask_data = NULL;
                ret->numpy_array = NULL;
                ret->numpy_mask = NULL;
                ret->multidimensional = FALSE;
                if (has_mask)
                {
                    size_t mask_size = ret->count * sizeof(bool);

                    assert(mask_size > 0);
                    MT_lock_set(&pyapiLock);
                    msg = init_mmap_memory(mmap_id, pci->retc + (i + 3), mask_size, &mmap_ptrs, &mmap_sizes, NULL);
                    MT_lock_unset(&pyapiLock);
                    if (msg != MAL_SUCCEED) {
                        goto wrapup;
                    }
                    ret->mask_data = mmap_ptrs[pci->retc + (i + 3)];
                }
            }
            msg = MAL_SUCCEED;

            goto returnvalues;
        }
    }
#endif

    //After this point we will execute Python Code, so we need to acquire the GIL
    if (!mapped) { 
        gstate = Python_ObtainGIL();
    }

    if (sqlfun) {
        // Check if exprStr references to a file path or if it contains the Python code itself
        // There is no easy way to check, so the rule is if it starts with '/' it is always a file path,
        // Otherwise it's a (relative) file path only if it ends with '.py'
        size_t length = strlen(exprStr);
        if (exprStr[0] == '/' || (exprStr[length - 3] == '.' && exprStr[length - 2] == 'p' && exprStr[length - 1] == 'y')) {
            FILE *fp;
            char address[1000];
            struct stat buffer;
            size_t length;
            if (exprStr[0] == '/') { 
                // absolute path
                snprintf(address, 1000, "%s", exprStr);
            } else {
                // relative path
                snprintf(address, 1000, "%s/%s", FunctionBasePath(), exprStr);
            }
            if (stat(address, &buffer) < 0) { 
                msg = createException(MAL, "pyapi.eval", "Could not find Python source file \"%s\".", address);
                goto wrapup;
            }
            fp = fopen(address, "r");
            if (fp == NULL) {
                msg = createException(MAL, "pyapi.eval", "Could not open Python source file \"%s\".", address);
                goto wrapup;
            }
            fseek(fp, 0, SEEK_END);
            length = ftell(fp);
            fseek(fp, 0, SEEK_SET);
            exprStr = GDKzalloc(length + 1);
            if (exprStr == NULL) {
                msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL" function body string.");
                goto wrapup;
            }
            if (fread(exprStr, 1, length, fp) != length) {
                msg = createException(MAL, "pyapi.eval", "Failed to read from file \"%s\".", address);
                goto wrapup;
            }
            fclose(fp);
        }
    }

    /*[PARSE_CODE]*/
    VERBOSE_MESSAGE("Formatting python code.\n");
    pycall = FormatCode(exprStr, args, argcount, 4, &code_object, &msg);
    if (pycall == NULL && code_object == NULL) {
        if (msg == NULL) { msg = createException(MAL, "pyapi.eval", "Error while parsing Python code."); }
        goto wrapup;
    }

    /*[CONVERT_BAT]*/
    VERBOSE_MESSAGE("Loading data from the database into Python.\n");

    // Now we will do the input handling (aka converting the input BATs to numpy arrays)
    // We will put the python arrays in a PyTuple object, we will use this PyTuple object as the set of arguments to call the Python function
    pArgs = PyTuple_New(argcount - (pci->retc + 2) + (code_object == NULL ? additional_columns : 0));
    pColumns = PyDict_New();
    pColumnTypes = PyDict_New();
#ifdef HAVE_FORK
    pConnection = Py_Connection_Create(cntxt, mapped, query_ptr, query_sem);
#else
    pConnection = Py_Connection_Create(cntxt, 0, 0, 0);
#endif

    // Now we will loop over the input BATs and convert them to python objects
    for (i = pci->retc + 2; i < argcount; i++) {
        PyObject *result_array;
         // t_start and t_end hold the part of the BAT we will convert to a Numpy array, by default these hold the entire BAT [0 - BATcount(b)]
        size_t t_start = 0, t_end = pyinput_values[i - (pci->retc + 2)].count;

        // There are two possibilities, either the input is a BAT, or the input is a scalar
        // If the input is a scalar we will convert it to a python scalar
        // If the input is a BAT, we will convert it to a numpy array
        if (pyinput_values[i - (pci->retc + 2)].scalar) {
            result_array = PyArrayObject_FromScalar(&pyinput_values[i - (pci->retc + 2)], &msg);
        } else {
            result_array = PyMaskedArray_FromBAT(&pyinput_values[i - (pci->retc + 2)], t_start, t_end, &msg, !enable_zerocopy_input);
        }
        if (result_array == NULL) {
            if (msg == MAL_SUCCEED) {
                msg = createException(MAL, "pyapi.eval", "Failed to create Numpy Array from BAT.");
            }
            goto wrapup;
        }
        if (code_object == NULL) {
            PyObject *arg_type = PyString_FromString(BatType_Format(pyinput_values[i - (pci->retc + 2)].bat_type));
            PyDict_SetItemString(pColumns, args[i], result_array);
            PyDict_SetItemString(pColumnTypes, args[i], arg_type);
            Py_DECREF(arg_type);
        }
        pyinput_values[i - (pci->retc + 2)].result = result_array;
        PyTuple_SetItem(pArgs, ai++, result_array);
    }
    if (code_object == NULL) {
        PyTuple_SetItem(pArgs, ai++, pColumns);
        PyTuple_SetItem(pArgs, ai++, pColumnTypes);
        PyTuple_SetItem(pArgs, ai++, pConnection);
    }

    /*[EXECUTE_CODE]*/
    VERBOSE_MESSAGE("Executing python code.\n");

    // Now it is time to actually execute the python code
    {
        PyObject *pFunc, *pModule, *v, *d;

        // First we will load the main module, this is required
        pModule = PyImport_AddModule("__main__");
        if (!pModule) {
            msg = PyError_CreateException("Failed to load module", NULL);
            goto wrapup;
        }

        // Now we will add the UDF to the main module
        d = PyModule_GetDict(pModule);
        if (code_object == NULL) {
            v = PyRun_StringFlags(pycall, Py_file_input, d, d, NULL);
            if (v == NULL) {
                msg = PyError_CreateException("Could not parse Python code", pycall);
                goto wrapup;
            }
            Py_DECREF(v);

            // Now we need to obtain a pointer to the function, the function is called "pyfun"
            pFunc = PyObject_GetAttrString(pModule, "pyfun");
            if (!pFunc || !PyCallable_Check(pFunc)) {
                msg = PyError_CreateException("Failed to load function", NULL);
                goto wrapup;
            }
        } else {
            pFunc = PyFunction_New(code_object, d);
            if (!pFunc || !PyCallable_Check(pFunc)) {
                msg = PyError_CreateException("Failed to load function", NULL);
                goto wrapup;
            }
        }


        if (parallel_aggregation) {
            // parallel aggregation, we run the function once for every group in parallel
            BAT *aggr_group = NULL, *group_first_occurrence = NULL;
            size_t group_count, elements, element_it, group_it;
            size_t *group_counts = NULL;
            oid *aggr_group_arr = NULL;
            void ***split_bats = NULL;
            int named_columns = unnamedArgs - (pci->retc + 2);
            PyObject *aggr_result;

            // release the GIL
            gstate = Python_ReleaseGIL(gstate);

            // the first unnamed argument has the group numbers for every row
            aggr_group = BATdescriptor(*getArgReference_bat(stk, pci, unnamedArgs));
            // the second unnamed argument has the first occurrence of every group number, we just use this to get the total amount of groups quickly
            group_first_occurrence = BATdescriptor(*getArgReference_bat(stk, pci, unnamedArgs + 1));
            group_count = BATcount(group_first_occurrence);
            BBPunfix(group_first_occurrence->batCacheid);
            elements = BATcount(aggr_group); // get the amount of groups

            // now we count, for every group, how many elements it has
            group_counts = GDKzalloc(group_count * sizeof(size_t));
            if (group_counts == NULL) {
                msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL" group count array.");
                goto aggrwrapup;
            }

            aggr_group_arr = (oid*) aggr_group->T->heap.base;
            for(element_it = 0; element_it < elements; element_it++) {
                group_counts[aggr_group_arr[element_it]]++;
            }

            //now perform the actual splitting of the data, first construct room for splits for every group
            // elements are structured as follows: 
            // split_bats [groupnr] [columnnr] [elementnr]
            split_bats = GDKzalloc(group_count * sizeof(void*));
            for(group_it = 0; group_it < group_count; group_it++) {
                split_bats[group_it] = GDKzalloc(sizeof(void*) * named_columns);
            }

            // now split the columns one by one
            for(i = 0; i < named_columns; i++) {
                PyInput input = pyinput_values[i];
                void *basevals = input.bat->T->heap.base;

                if (!input.scalar) {
                    switch(input.bat_type) {
                        case TYPE_bit:
                            NP_SPLIT_BAT(bit);
                            break;
                        case TYPE_bte:
                            NP_SPLIT_BAT(bte);
                            break;
                        case TYPE_sht:
                            NP_SPLIT_BAT(sht);
                            break;
                        case TYPE_int:
                            NP_SPLIT_BAT(int);
                            break;
                        case TYPE_oid:
                            NP_SPLIT_BAT(oid);
                            break;
                        case TYPE_lng:
                            NP_SPLIT_BAT(lng);
                            break;
                        case TYPE_flt:
                            NP_SPLIT_BAT(flt);
                            break;
                        case TYPE_dbl:
                            NP_SPLIT_BAT(dbl);
                            break;
                    #ifdef HAVE_HGE
                        case TYPE_hge:
                            basevals = PyArray_BYTES((PyArrayObject*)input.result);
                            NP_SPLIT_BAT(dbl);
                            break;
                    #endif
                        case TYPE_str:
                        {
                            PyObject ****ptr = (PyObject****)split_bats;
                            size_t *temp_indices;
                            PyObject **batcontent = (PyObject**)PyArray_DATA((PyArrayObject*)input.result);
                            // allocate space for split BAT
                            for(group_it = 0; group_it < group_count; group_it++) {
                                ptr[group_it][i] = GDKzalloc(group_counts[group_it] * sizeof(PyObject*));
                            }
                            // iterate over the elements of the current BAT
                            temp_indices = GDKzalloc(sizeof(PyObject*) * group_count);
                            for(element_it = 0; element_it < elements; element_it++) {
                                //group of current element
                                oid group = aggr_group_arr[element_it]; 
                                //append current element to proper group
                                ptr[group][i][temp_indices[group]++] = batcontent[element_it];
                            }
                            GDKfree(temp_indices);
                            break;
                        }
                        default:
                            msg = createException(MAL, "pyapi.eval", "Unrecognized BAT type %s", BatType_Format(input.bat_type));
                            goto aggrwrapup;
                            break;
                    }
                }
            }

            {
                int res = 0;
                size_t threads = 8; //GDKgetenv("gdk_nr_threads");
                size_t thread_it;
                size_t result_it;
                AggrParams *parameters;
                PyObject **results;
                double current = 0.0;
                double increment;

                // if there are less groups than threads, limit threads to amount of groups
                threads = group_count < threads ? group_count : threads; 

                increment = (double) group_count / (double) threads;
                // start running the threads
                parameters = GDKzalloc(threads * sizeof(AggrParams));
                results = GDKzalloc(group_count * sizeof(PyObject*));
                for(thread_it = 0; thread_it < threads; thread_it++) {
                    AggrParams *params = &parameters[thread_it];
                    params->named_columns = named_columns;
                    params->additional_columns = additional_columns;
                    params->group_count = group_count;
                    params->group_counts = &group_counts;
                    params->pyinput_values = &pyinput_values;
                    params->column_types_dict = &pColumnTypes;
                    params->split_bats = &split_bats;
                    params->base = pci->retc + 2;
                    params->function = &pFunc;
                    params->connection = &pConnection;
                    params->pycall = &pycall;
                    params->group_start = (size_t)floor(current);
                    params->group_end = (size_t)floor(current += increment);
                    params->args = &args;
                    params->msg = NULL;
                    params->result_objects = results;
                    res = MT_create_thread(&params->thread, (void (*)(void *))&ComputeParallelAggregation, params, MT_THR_JOINABLE);
                    if (res != 0) {
                        msg = createException(MAL, "pyapi.eval", "Failed to start thread.");
                        goto aggrwrapup;
                    }
                }
                for(thread_it = 0; thread_it < threads; thread_it++) {
                    AggrParams params = parameters[thread_it];
                    int res = MT_join_thread(params.thread);
                    if (res != 0) {
                        msg = createException(MAL, "pyapi.eval", "Failed to join thread.");
                        goto aggrwrapup;
                    }
                }

                for(thread_it = 0; thread_it < threads; thread_it++) {
                    AggrParams params = parameters[thread_it];
                    if (results[thread_it] == NULL || params.msg != NULL) {
                        msg = params.msg;
                        goto wrapup;
                    }
                }

                // we need the GIL again to group the parameters
                gstate = Python_ObtainGIL();

                aggr_result = PyList_New(group_count);
                for(result_it = 0; result_it < group_count; result_it++) {
                    PyList_SetItem(aggr_result, result_it, results[result_it]);
                }
                GDKfree(parameters);
                GDKfree(results);
            }
            pResult = PyList_New(1);
            PyList_SetItem(pResult, 0, aggr_result);

aggrwrapup:
            if (group_counts != NULL) {
                GDKfree(group_counts);
            }
            if (split_bats != NULL) {
                for(group_it = 0; group_it < group_count; group_it++) {
                    if (split_bats[group_it] != NULL) {
                        for(i = 0; i < named_columns; i++) {
                            if (split_bats[group_it][i] != NULL) {
                                GDKfree(split_bats[group_it][i]);
                            }
                        }
                        GDKfree(split_bats[group_it]);
                    }
                }
                GDKfree(split_bats);
            }
            if (aggr_group != NULL) {
                BBPunfix(aggr_group->batCacheid);
            }
            if (msg != MAL_SUCCEED) {
                goto wrapup;
            }
        } else {
            // The function has been successfully created/compiled, all that remains is to actually call the function
            pResult = PyObject_CallObject(pFunc, pArgs);
        }


        Py_DECREF(pFunc);
        Py_DECREF(pArgs);

        if (PyErr_Occurred()) {
            msg = PyError_CreateException("Python exception", pycall);
            if (code_object == NULL) { PyRun_SimpleString("del pyfun"); }
            goto wrapup;
        }

        //if (code_object == NULL) { PyRun_SimpleString("del pyfun"); }

        if (PyDict_Check(pResult)) { // Handle dictionary returns
            // For dictionary returns we need to map each of the (key,value) pairs to the proper return value
            // We first analyze the SQL Function structure for a list of return value names
            char **retnames = NULL;
            if (!varres)
            {
                if (sqlfun != NULL) {
                    retnames = GDKzalloc(sizeof(char*) * sqlfun->res->cnt);
                    argnode = sqlfun->res->h;
                    for(i = 0; i < sqlfun->res->cnt; i++) {
                        retnames[i] = ((sql_arg*)argnode->data)->name;
                        argnode = argnode->next;
                    }
                } else {
                    msg = createException(MAL, "pyapi.eval", "Return value is a dictionary, but there is no sql function object, so we don't know the return value names and mapping cannot be done.");
                    goto wrapup;
                }
            } else {
                // If there are a variable number of return types, we take the column names from the dictionary
                PyObject *keys = PyDict_Keys(pResult);
                retcols = (int)PyList_Size(keys);
                retnames = GDKzalloc(sizeof(char*) * retcols);
                for(i = 0; i < retcols; i++) {
                    PyObject *colname = PyList_GetItem(keys, i);
                    if (!PyString_CheckExact(colname)) {
                        msg = createException(MAL, "pyapi.eval", "Expected a string key in the dictionary, but received an object of type %s", colname->ob_type->tp_name);
                        goto wrapup;
                    }
#ifndef IS_PY3K
                    retnames[i] = ((PyStringObject*)colname)->ob_sval;
#else
                    retnames[i] = PyUnicode_AsUTF8(colname);
#endif
                }
            }
            pResult = PyDict_CheckForConversion(pResult, retcols, retnames, &msg);
            if (retnames != NULL) GDKfree(retnames);
        } else if (varres) {
            msg = createException(MAL, "pyapi.eval", "Expected a variable number return values, but the return type was not a dictionary. We require the return type to be a dictionary for column naming purposes.");
            goto wrapup;
        }
        else {
            // Now we need to do some error checking on the result object, because the result object has to have the correct type/size
            // We will also do some converting of result objects to a common type (such as scalar -> [[scalar]])
            pResult = PyObject_CheckForConversion(pResult, retcols, NULL, &msg);
        }
        if (pResult == NULL) {
            goto wrapup;
        }
    }
    VERBOSE_MESSAGE("Collecting return values.\n");

    if (varres) {
        GDKfree(pyreturn_values);
        pyreturn_values = GDKzalloc(retcols * sizeof(PyReturn));
    }

    // Now we have executed the Python function, we have to collect the return values and convert them to BATs
    // We will first collect header information about the Python return objects and extract the underlying C arrays
    // We will store this header information in a PyReturn object

    // The reason we are doing this as a separate step is because this preprocessing requires us to call the Python API
    // Whereas the actual returning does not require us to call the Python API
    // This means we can do the actual returning without holding the GIL
    if (!PyObject_PreprocessObject(pResult, pyreturn_values, retcols, &msg)) {
        goto wrapup;
    }


#ifdef HAVE_FORK
    /*[FORKED]*/
    // This is where the child process stops executing
    // We have successfully executed the Python function and converted the result object to a C array
    // Now all that is left is to copy the C array to shared memory so the main process can read it and return it
    if (mapped && child_process) {
        char *mmap_ptr;
        ReturnBatDescr *ptr;

        // First we will fill in the header information, we will need to get a pointer to the header data first
        // The main process has already created the header data for the child process
        VERBOSE_MESSAGE("Getting shared memory.\n");
        msg = init_mmap_memory(mmap_id, 0, memory_size, &mmap_ptrs, &mmap_sizes, &mmap_ptr);
        if (msg != MAL_SUCCEED) {
            goto wrapup;
        }

        VERBOSE_MESSAGE("Writing headers.\n");

        // Now we will write data about our result (memory size, type, number of elements) to the header
        ptr = (ReturnBatDescr*)mmap_ptr;
        for (i = 0; i < retcols; i++)
        {
            PyReturn *ret = &pyreturn_values[i];
            ReturnBatDescr *descr = &ptr[i];

            if (ret->result_type == NPY_OBJECT) {
                // We can't deal with NPY_OBJECT arrays, because these are 'arrays of pointers', so we can't just copy the content of the array into shared memory
                // So if we're dealing with a NPY_OBJECT array, we convert them to a Numpy Array of type NPY_<TYPE> that corresponds with the desired BAT type
                // WARNING: Because we could be converting to a NPY_STRING or NPY_UNICODE array (if the desired type is TYPE_str or TYPE_hge), this means that memory usage can explode
                //   because NPY_STRING/NPY_UNICODE arrays are 2D string arrays with fixed string length (so if there's one very large string the size explodes quickly)
                //   if someone has some problem with memory size exploding when using PYTHON_MAP but it being fine in regular PYTHON this is probably the issue
                int bat_type = getColumnType(getArgType(mb,pci,i));
                PyObject *new_array = PyArray_FromAny(ret->numpy_array, PyArray_DescrFromType(BatType_ToPyType(bat_type)), 1, 1, NPY_ARRAY_CARRAY | NPY_ARRAY_FORCECAST, NULL);
                if (new_array == NULL) {
                    msg = createException(MAL, "pyapi.eval", "Could not convert the returned NPY_OBJECT array to the desired array of type %s.\n", BatType_Format(bat_type));
                    goto wrapup;
                }
                Py_DECREF(ret->numpy_array); //do we really care about cleaning this up, considering this only happens in a separate process that will be exited soon anyway?
                ret->numpy_array = new_array;
                ret->result_type = PyArray_DESCR((PyArrayObject*)ret->numpy_array)->type_num;
                ret->memory_size = PyArray_DESCR((PyArrayObject*)ret->numpy_array)->elsize;
                ret->count = PyArray_DIMS((PyArrayObject*)ret->numpy_array)[0];
                ret->array_data = PyArray_DATA((PyArrayObject*)ret->numpy_array);
            }

            descr->npy_type = ret->result_type;
            descr->element_size =   ret->memory_size;
            descr->bat_count = ret->count;
            descr->bat_size = ret->memory_size * ret->count;
            descr->has_mask = ret->mask_data != NULL;

            if (ret->count > 0)
            {
                int memory_size = ret->memory_size * ret->count;
                char *mem_ptr;
                //now create shared memory for the return value and copy the actual values
                assert(memory_size > 0);
                if (init_mmap_memory(mmap_id, i + 3, memory_size, &mmap_ptrs, &mmap_sizes, NULL) != MAL_SUCCEED)
                {
                    msg = createException(MAL, "pyapi.eval", "Failed to allocate shared memory for returning data.\n");
                    goto wrapup;
                }
                mem_ptr = mmap_ptrs[i + 3];
                assert(mem_ptr);
                memcpy(mem_ptr, PyArray_DATA((PyArrayObject*)ret->numpy_array), memory_size);

                if (descr->has_mask)
                {
                    bool *mask_ptr;
                    int mask_size = ret->count * sizeof(bool);
                    assert(mask_size > 0);
                    if (init_mmap_memory(mmap_id, retcols + (i + 3), mask_size, &mmap_ptrs, &mmap_sizes, NULL) != MAL_SUCCEED) //create a memory space for the mask
                    {
                        msg = createException(MAL, "pyapi.eval", "Failed to allocate shared memory for returning mask.\n");
                        goto wrapup;
                    }
                    mask_ptr = mmap_ptrs[retcols + i + 3];
                    assert(mask_ptr);
                    memcpy(mask_ptr, ret->mask_data, mask_size);
                }
            }
        }
        //now free the main process from the semaphore
        msg = change_semaphore_value(query_sem, 0, 1);
        if (msg != MAL_SUCCEED)
            goto wrapup;
        // Exit child process without an error code
        exit(0);
    }
#endif
    // We are done executing Python code (aside from cleanup), so we can release the GIL
    gstate = Python_ReleaseGIL(gstate);

#ifdef HAVE_FORK // This goto is only used for multiprocessing, if HAVE_FORK is set to 0 this is unused
returnvalues:
#endif
    /*[RETURN_VALUES]*/
    VERBOSE_MESSAGE("Returning values.\n");

    argnode = sqlfun && sqlfun->res ? sqlfun->res->h : NULL;
    for (i = 0; i < retcols; i++)
    {
        PyReturn *ret = &pyreturn_values[i];
        int bat_type = TYPE_any;
        sql_subtype *sql_subtype = argnode ? &((sql_arg*)argnode->data)->type : NULL;
        if (!varres) {
            bat_type = getColumnType(getArgType(mb,pci,i));

            if (bat_type == TYPE_any || bat_type == TYPE_void) {
                bat_type = PyType_ToBat(ret->result_type);
                getArgType(mb,pci,i) = bat_type;
            }
        } else {
            bat_type = PyType_ToBat(ret->result_type);
        }

        b = PyObject_ConvertToBAT(ret, sql_subtype, bat_type, i, seqbase, &msg, !enable_zerocopy_output);
        if (b == NULL) {
            goto wrapup;
        }

        if (isaBatType(getArgType(mb,pci,i)))
        {
            *getArgReference_bat(stk, pci, i) = b->batCacheid;
            BBPkeepref(b->batCacheid);
        }
        else
        { // single value return, only for non-grouped aggregations
            VALinit(&stk->stk[pci->argv[i]], bat_type, Tloc(b, BUNfirst(b)));
        }
        if (argnode) {
            argnode = argnode->next;
        }
        msg = MAL_SUCCEED;
    }
wrapup:

#ifdef HAVE_FORK
    if (mapped && child_process)
    {
        // If we get here, something went wrong in a child process
        char *error_mem, *tmp_msg;
        ReturnBatDescr *ptr;

        // Now we exit the program with an error code
        VERBOSE_MESSAGE("Failure in child process: %s\n", msg);
        tmp_msg = change_semaphore_value(query_sem, 0, 1);
        if (tmp_msg != MAL_SUCCEED) {
            VERBOSE_MESSAGE("Failed to increase value of semaphore in child process: %s\n", tmp_msg);
            exit(1);
        }

        assert(memory_size > 0);
        tmp_msg = init_mmap_memory(mmap_id, 0, memory_size, &mmap_ptrs, &mmap_sizes, NULL);
        if (tmp_msg != MAL_SUCCEED) {
            VERBOSE_MESSAGE("Failed to get shared memory in child process: %s\n", tmp_msg);
            exit(1);
        }

        // To indicate that we failed, we will write information to our header
        ptr = (ReturnBatDescr*)mmap_ptrs[0];
        for (i = 0; i < retcols; i++) {
            ReturnBatDescr *descr = &ptr[i];
            // We will write descr->npy_type to -1, so other processes can see that we failed
            descr->npy_type = -1;
            // We will write the memory size of our error message to the bat_size, so the main process can access the shared memory
            descr->bat_size = (strlen(msg) + 1) * sizeof(char);
        }

        // Now create the shared memory to write our error message to
        // We can simply use the slot mmap_id + 3, even though this is normally used for query return values
        // This is because, if the process fails, no values will be returned
        tmp_msg = init_mmap_memory(mmap_id, 3, (strlen(msg) + 1) * sizeof(char), NULL, NULL, &error_mem);
        if (tmp_msg != MAL_SUCCEED) {
            VERBOSE_MESSAGE("Failed to create shared memory in child process: %s\n", tmp_msg);
            exit(1);
        }
        strcpy(error_mem, msg);
        exit(1);
    }
#endif

    VERBOSE_MESSAGE("Cleaning up.\n");

#ifdef HAVE_FORK
    if (holds_gil){
        MT_lock_set(&pyapiLock);
        python_call_active = false;
        MT_lock_unset(&pyapiLock);
    }

    if (mapped)
    {
        for(i = 0; i < retcols; i++) {
            PyReturn *ret = &pyreturn_values[i];
            if (ret->mmap_id < 0) {
                // if we directly give the mmap file to a BAT, don't delete the MMAP file
                mmap_ptrs[i + 3] = NULL;
            }
        }
        for(i = 0; i < 3 + pci->retc * 2; i++) {
            if (mmap_ptrs[i] != NULL) {
                release_mmap_memory(mmap_ptrs[i], mmap_sizes[i], mmap_id + i);
            }
        }
        if (query_sem > 0)
            release_process_semaphore(query_sem);
    }
#endif
    // Actual cleanup
    // Cleanup input BATs
    for (i = pci->retc + 2; i < pci->argc; i++)
    {
        PyInput *inp = &pyinput_values[i - (pci->retc + 2)];
        if (inp->bat != NULL) BBPunfix(inp->bat->batCacheid);
    }
    if (pResult != NULL && gstate == 0) {
        //if there is a pResult here, we are running single threaded (LANGUAGE PYTHON),
        //thus we need to free python objects, thus we need to obtain the GIL
        gstate = Python_ObtainGIL();
    }
    for (i = 0; i < retcols; i++) {
        PyReturn *ret = &pyreturn_values[i];
        // First clean up any return values
        if (!ret->multidimensional) {
            // Clean up numpy arrays, if they are there
            if (ret->numpy_array != NULL) {
                Py_DECREF(ret->numpy_array);
            }
            if (ret->numpy_mask != NULL) {
                Py_DECREF(ret->numpy_mask);
            }
        }
    }
    if (pResult != NULL) {
        Py_DECREF(pResult);
    }
    if (gstate != 0) {
        gstate = Python_ReleaseGIL(gstate);
    }

    // Now release some GDK memory we allocated for strings and input values
    GDKfree(pyreturn_values);
    GDKfree(pyinput_values);
    for (i = 0; i < pci->argc; i++)
        if (args[i] != NULL)
            GDKfree(args[i]);
    GDKfree(args);
    GDKfree(pycall);

    VERBOSE_MESSAGE("Finished cleaning up.\n");
    return msg;
}

str
 PyAPIprelude(void *ret) {
    (void) ret;
#ifndef _EMBEDDED_MONETDB_MONETDB_LIB_
    MT_lock_init(&pyapiLock, "pyapi_lock");
    MT_lock_init(&queryLock, "query_lock");
    if (PyAPIEnabled()) {
        MT_lock_set(&pyapiLock);
        if (!pyapiInitialized) {
            str msg = MAL_SUCCEED;
            Py_Initialize();
            if (PyRun_SimpleString("import numpy") != 0 || _import_array() < 0) {
                return PyError_CreateException("Failed to initialize embedded python", NULL);
            }
            msg = _connection_init();
            marshal_module = PyImport_Import(PyString_FromString("marshal"));
            if (marshal_module == NULL) {
                return createException(MAL, "pyapi.eval", "Failed to load Marshal module.");
            }
            marshal_loads = PyObject_GetAttrString(marshal_module, "loads");
            if (marshal_loads == NULL) {
                return createException(MAL, "pyapi.eval", "Failed to load function \"loads\" from Marshal module.");
            }
            PyEval_SaveThread();
            LOAD_SQL_FUNCTION_PTR(batbte_dec2_dbl, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(batsht_dec2_dbl, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(batint_dec2_dbl, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(batlng_dec2_dbl, "lib_sql.dll");
#ifdef HAVE_HGE
            LOAD_SQL_FUNCTION_PTR(bathge_dec2_dbl, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(bathge_dec2_dbl, "lib_sql.dll");
#endif
            LOAD_SQL_FUNCTION_PTR(batstr_2time_timestamp, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(batstr_2time_daytime, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(batstr_2_date, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(batdbl_num2dec_lng, "lib_sql.dll");
            LOAD_SQL_FUNCTION_PTR(SQLbatstr_cast, "lib_sql.dll");
            if (msg != MAL_SUCCEED) {
                MT_lock_unset(&pyapiLock);
                return msg;
            }
            pyapiInitialized++;
        }
        MT_lock_unset(&pyapiLock);
        fprintf(stdout, "# MonetDB/Python module loaded\n");
    }
#else
    if (!pyapiInitialized) {
        char* iar = NULL;
        import_array1(iar);
        pyapiInitialized++;
        marshal_module = PyImport_Import(PyString_FromString("marshal"));
        if (marshal_module == NULL) {
            return createException(MAL, "pyapi.eval", "Failed to load Marshal module.");
        }
        marshal_loads = PyObject_GetAttrString(marshal_module, "loads");
        if (marshal_loads == NULL) {
            return createException(MAL, "pyapi.eval", "Failed to load function \"loads\" from Marshal module.");
        }
    }
#endif
#ifdef _PYAPI_VERBOSE_
    option_verbose = GDKgetenv_isyes(verbose_enableflag) || GDKgetenv_istrue(verbose_enableflag);
#endif
#ifdef _PYAPI_DEBUG_
    option_debug = GDKgetenv_isyes(debug_enableflag) || GDKgetenv_istrue(debug_enableflag);
    (void) option_debug;
#endif
#ifdef _PYAPI_WARNINGS_
    option_warning = GDKgetenv_isyes(warning_enableflag) || GDKgetenv_istrue(warning_enableflag);
#endif
    return MAL_SUCCEED;
}

//Returns true if the type of [object] is a scalar (i.e. numeric scalar or string, basically "not an array but a single value")
bool PyType_IsPyScalar(PyObject *object)
{
    if (object == NULL) return false;
    return (PyArray_CheckScalar(object) || PyInt_Check(object) || PyFloat_Check(object) || PyLong_Check(object) || PyString_Check(object) || PyBool_Check(object) || PyUnicode_Check(object) || PyByteArray_Check(object)
#ifdef IS_PY3K   
        || PyBytes_Check(object)
#endif
        );
}


static char *PyError_CreateException(char *error_text, char *pycall)
{
    PyObject *py_error_type = NULL, *py_error_value = NULL, *py_error_traceback = NULL;
    char *py_error_string = NULL;
    lng line_number = -1;

    PyErr_Fetch(&py_error_type, &py_error_value, &py_error_traceback);
    if (py_error_value) {
        PyObject *error;
        PyErr_NormalizeException(&py_error_type, &py_error_value, &py_error_traceback);
        error = PyObject_Str(py_error_value);

        py_error_string = PyString_AS_STRING(error);
        Py_XDECREF(error);
        if (pycall != NULL && strlen(pycall) > 0) {
            if (py_error_traceback == NULL) {
                //no traceback info, this means we are dealing with a parsing error
                //line information should be in the error message
                sscanf(py_error_string, "%*[^0-9]"LLFMT, &line_number);
                if (line_number < 0) goto finally;
            } else {
                line_number = ((PyTracebackObject*)py_error_traceback)->tb_lineno;
            }

            // Now only display the line numbers around the error message, we display 5 lines around the error message
            {
                char linenr[32];
                size_t nrpos, pos, i, j;
                char lineinformation[5000]; //we only support 5000 characters for 5 lines of the program, should be enough
                nrpos = 0; // Current line number
                pos = 0; //Current position in the lineinformation result array
                for(i = 0; i < strlen(pycall); i++) {
                    if (pycall[i] == '\n' || i == 0) {
                        // Check if we have arrived at a new line, if we have increment the line count
                        nrpos++;
                        // Now check if we should display this line
                        if (nrpos >= ((size_t)line_number - 2) && nrpos <= ((size_t)line_number + 2) && pos < 4997) {
                            // We shouldn't put a newline on the first line we encounter, only on subsequent lines
                            if (nrpos > ((size_t)line_number - 2)) lineinformation[pos++] = '\n';
                            if ((size_t)line_number == nrpos) {
                                // If this line is the 'error' line, add an arrow before it, otherwise just add spaces
                                lineinformation[pos++] = '>';
                                lineinformation[pos++] = ' ';
                            } else {
                                lineinformation[pos++] = ' ';
                                lineinformation[pos++] = ' ';
                            }
                            snprintf(linenr, 32, SZFMT, nrpos);
                            for(j = 0; j < strlen(linenr); j++) {
                                lineinformation[pos++] = linenr[j];
                            }
                            lineinformation[pos++] = '.';
                            lineinformation[pos++] = ' ';
                        }
                    }
                    if (pycall[i] != '\n' && nrpos >= (size_t)line_number - 2 && nrpos <= (size_t)line_number + 2 && pos < 4999) {
                        // If we are on a line number that we have to display, copy the text from this line for display
                        lineinformation[pos++] = pycall[i];
                    }
                }
                lineinformation[pos] = '\0';
                return createException(MAL, "pyapi.eval", "%s\n%s\n%s", error_text, lineinformation, py_error_string);
            }
        }
    }
    else {
        py_error_string = "";
    }
finally:
    if (pycall == NULL) return createException(MAL, "pyapi.eval", "%s\n%s", error_text, py_error_string);
    return createException(MAL, "pyapi.eval", "%s\n%s\n%s", error_text, pycall, py_error_string);
}

PyObject *PyArrayObject_FromScalar(PyInput* inp, char **return_message)
{
    PyObject *vararray = NULL;
    char *msg = NULL;
    assert(inp->scalar); //input has to be a scalar
    VERBOSE_MESSAGE("- Loading a scalar of type %s (%i)", BatType_Format(inp->bat_type), inp->bat_type);

    switch(inp->bat_type)
    {
        case TYPE_bit:
            vararray = PyInt_FromLong((long)(*(bit*)inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %ld]\n", (long)(*(bit*)inp->dataptr));
            break;
        case TYPE_bte:
            vararray = PyInt_FromLong((long)(*(bte*)inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %ld]\n", (long)(*(bte*)inp->dataptr));
            break;
        case TYPE_sht:
            vararray = PyInt_FromLong((long)(*(sht*)inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %ld]\n", (long)(*(sht*)inp->dataptr));
            break;
        case TYPE_int:
            vararray = PyInt_FromLong((long)(*(int*)inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %ld]\n", (long)(*(int*)inp->dataptr));
            break;
        case TYPE_lng:
            vararray = PyLong_FromLong((long)(*(lng*)inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %ld]\n", (long)(*(lng*)inp->dataptr));
            break;
        case TYPE_flt:
            vararray = PyFloat_FromDouble((double)(*(flt*)inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %lf]\n", (double)(*(flt*)inp->dataptr));
            break;
        case TYPE_dbl:
            vararray = PyFloat_FromDouble((double)(*(dbl*)inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %lf]\n", (double)(*(dbl*)inp->dataptr));
            break;
#ifdef HAVE_HGE
        case TYPE_hge:
            vararray = PyLong_FromHge(*((hge *) inp->dataptr));
            VERBOSE_MESSAGE(" [Value: Huge]\n");
            break;
#endif
        case TYPE_str:
            vararray = PyUnicode_FromString(*((char**) inp->dataptr));
            VERBOSE_MESSAGE(" [Value: %s]\n", *((char**) inp->dataptr));
            break;
        default:
            VERBOSE_MESSAGE(" [Value: Unknown]\n");
            msg = createException(MAL, "pyapi.eval", "Unsupported scalar type %i.", inp->bat_type);
            goto wrapup;
    }
    if (vararray == NULL)
    {
        msg = createException(MAL, "pyapi.eval", "Something went wrong converting the MonetDB scalar to a Python scalar.");
        goto wrapup;
    }
wrapup:
    *return_message = msg;
    return vararray;
}

PyObject *PyMaskedArray_FromBAT(PyInput *inp, size_t t_start, size_t t_end, char **return_message, bool copy)
{
    BAT *b = inp->bat;
    char *msg;
    PyObject *vararray;

    vararray = PyArrayObject_FromBAT(inp, t_start, t_end, return_message, copy);
    if (vararray == NULL) {
        return NULL;
    }
    // To deal with null values, we use the numpy masked array structure
    // The masked array structure is an object with two arrays of equal size, a data array and a mask array
    // The mask array is a boolean array that has the value 'True' when the element is NULL, and 'False' otherwise
    // If the BAT has Null values, we construct this masked array
    if (!(b->T->nil == 0 && b->T->nonil == 1))
    {
        PyObject *mask;
        PyObject *mafunc = PyObject_GetAttrString(PyImport_Import(PyString_FromString("numpy.ma")), "masked_array");
        PyObject *maargs;
        PyObject *nullmask = PyNullMask_FromBAT(b, t_start, t_end);

        if (nullmask == Py_None) {
            maargs = PyTuple_New(1);
            PyTuple_SetItem(maargs, 0, vararray);
        } else {
            maargs = PyTuple_New(2);
            PyTuple_SetItem(maargs, 0, vararray);
            PyTuple_SetItem(maargs, 1, (PyObject*) nullmask);
        }

        // Now we will actually construct the mask by calling the masked array constructor
        mask = PyObject_CallObject(mafunc, maargs);
        if (!mask) {
            msg = PyError_CreateException("Failed to create mask", NULL);
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

PyObject *PyArrayObject_FromBAT(PyInput *inp, size_t t_start, size_t t_end, char **return_message, bool copy)
{
    // This variable will hold the converted Python object
    PyObject *vararray = NULL;
    char *msg;
    size_t j = 0;
    BUN p = 0, q = 0;
    BATiter li;
    BAT *b = inp->bat;
    npy_intp elements[1] = { t_end-t_start };

    assert(!inp->scalar); //input has to be a BAT

    if (b == NULL)
    {
        // No BAT was found, we can't do anything in this case
        msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL" bat.");
        goto wrapup;
    }

    VERBOSE_MESSAGE("- Loading a BAT of type %s (%d) [Size: %zu]\n", BatType_Format(inp->bat_type), inp->bat_type, inp->count);

    switch (inp->bat_type) {
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
    case TYPE_str:
        {
            bool unicode = false;
            li = bat_iterator(b);
            //create a NPY_OBJECT array object
            vararray = PyArray_New(
                &PyArray_Type,
                1,
                elements,
                NPY_OBJECT,
                NULL,
                NULL,
                0,
                0,
                NULL);

            BATloop(b, p, q) {
                char *t = (char *) BUNtail(li, p);
                for(; *t != 0; t++) {
                    if (*t < 0) {
                        unicode = true;
                        break;
                    }
                }
                if (unicode) {
                    break;
                }
            }

            {
                PyObject **data = ((PyObject**)PyArray_DATA((PyArrayObject*)vararray));
                PyObject *obj;
                j = 0;
                if (unicode) {                    
                    if (GDK_ELIMDOUBLES(b->T->vheap)) {
                        PyObject** pyptrs = GDKzalloc(b->T->vheap->free * sizeof(PyObject*));
                        if (!pyptrs) {
                            msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL" PyObject strings.");
                            goto wrapup;
                        }
                        BATloop(b, p, q) {
                            const char *t = (const char *) BUNtail(li, p);
                            ptrdiff_t offset = t - b->T->vheap->base;
                            if (!pyptrs[offset]) {
                                if (strcmp(t, str_nil) == 0) {
                                     //str_nil isn't a valid UTF-8 character (it's 0x80), so we can't decode it as UTF-8 (it will throw an error)
                                    pyptrs[offset] = PyUnicode_FromString("-");
                                } else {
                                    //otherwise we can just decode the string as UTF-8
                                    pyptrs[offset] = PyUnicode_FromString(t);
                                }
                                if (!pyptrs[offset]) {
                                    msg = createException(MAL, "pyapi.eval", "Failed to create string.");
                                    goto wrapup;
                                }
                            } else {
                                Py_INCREF(pyptrs[offset]);
                            }
                            data[j++] = pyptrs[offset];
                        }
                        GDKfree(pyptrs);
                    }
                    else {
                    BATloop(b, p, q) {
                            char *t = (char *) BUNtail(li, p);
                            if (strcmp(t, str_nil) == 0) {
                                 //str_nil isn't a valid UTF-8 character (it's 0x80), so we can't decode it as UTF-8 (it will throw an error)
                                obj = PyUnicode_FromString("-");
                            } else {
                                //otherwise we can just decode the string as UTF-8
                                obj = PyUnicode_FromString(t);
                            }

                            if (obj == NULL) {
                                msg = createException(MAL, "pyapi.eval", "Failed to create string.");
                                goto wrapup;
                            }
                            data[j++] = obj;
                        }
                    }
                } else {
                    /* special case where we exploit the duplicate-eliminated string heap */
                    if (GDK_ELIMDOUBLES(b->T->vheap)) {
                        PyObject** pyptrs = GDKzalloc(b->T->vheap->free * sizeof(PyObject*));
                        if (!pyptrs) {
                            msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL" PyObject strings.");
                            goto wrapup;
                        }
                        BATloop(b, p, q) {
                            const char *t = (const char *) BUNtail(li, p);
                            ptrdiff_t offset = t - b->T->vheap->base;
                            if (!pyptrs[offset]) {
                                pyptrs[offset] = PyString_FromString(t);
                            } else {
                                Py_INCREF(pyptrs[offset]);
                            }
                            data[j++] = pyptrs[offset];
                        }
                        GDKfree(pyptrs);
                    }
                    else {
                        BATloop(b, p, q) {
                            char *t = (char *) BUNtail(li, p);
                            obj = PyString_FromString(t);
                            if (obj == NULL) {
                                msg = createException(MAL, "pyapi.eval", "Failed to create string.");
                                goto wrapup;
                            }
                            data[j++] = obj;
                        }
                    }
                }
            }
        }
        break;
#ifdef HAVE_HGE
    case TYPE_hge:
    {
        WARNING_MESSAGE("!ACCURACY WARNING: Type \"hge\" (128 bit) is unsupported by Numpy. The numbers are instead converted to float64, which results in loss of accuracy.\n");
        li = bat_iterator(b);
        //create a NPY_FLOAT64 array to hold the huge type
        vararray = PyArray_New(
            &PyArray_Type,
            1,
            (npy_intp[1]) { t_end - t_start },
            NPY_FLOAT64,
            NULL,
            NULL,
            0,
            0,
            NULL);

        j = 0;
        {
            dbl *data = (dbl*)PyArray_DATA((PyArrayObject*)vararray);
            BATloop(b, p, q) {
                const hge *t = (const hge *) BUNtail(li, p);
                data[j++] = (dbl) *t;
            }
        }
        break;
    }
#endif
    default:
        if (!inp->sql_subtype || !inp->sql_subtype->type) {
            msg = createException(MAL, "pyapi.eval", "unknown argument type");
        } else {
            msg = createException(MAL, "pyapi.eval", "Unsupported SQL Type: %s", inp->sql_subtype->type->sqlname);
        }
        goto wrapup;
    }
    if (vararray == NULL) {
        msg = PyError_CreateException("Failed to convert BAT to Numpy array.", NULL);
        goto wrapup;
    }
    return vararray;
wrapup:
    *return_message = msg;
    return NULL;
}

#define CreateNullMask(tpe) {                                       \
    tpe *bat_ptr = (tpe*)b->T->heap.base;                           \
    for(j = 0; j < count; j++) {                                    \
        mask_data[j] = bat_ptr[j] == tpe##_nil;                     \
        found_nil = found_nil || mask_data[j];                      \
    } }

PyObject *PyNullMask_FromBAT(BAT *b, size_t t_start, size_t t_end)
{
    // We will now construct the Masked array, we start by setting everything to False
    size_t count = t_end - t_start;
    npy_intp elements[1] = { count };
    PyArrayObject* nullmask = (PyArrayObject*) PyArray_EMPTY(1, elements, NPY_BOOL, 0);
    const void *nil = ATOMnilptr(b->ttype);
    size_t j;
    bool found_nil = false;
    BATiter bi = bat_iterator(b);
    bool *mask_data = (bool*)PyArray_DATA(nullmask);

    switch(ATOMstorage(getColumnType(b->T->type)))
    {
        case TYPE_bit: CreateNullMask(bit); break;
        case TYPE_bte: CreateNullMask(bte); break;
        case TYPE_sht: CreateNullMask(sht); break;
        case TYPE_int: CreateNullMask(int); break;
        case TYPE_lng: CreateNullMask(lng); break;
        case TYPE_flt: CreateNullMask(flt); break;
        case TYPE_dbl: CreateNullMask(dbl); break;
#ifdef HAVE_HGE
        case TYPE_hge: CreateNullMask(hge); break;
#endif
        case TYPE_str:
        {
            int (*atomcmp)(const void *, const void *) = ATOMcompare(b->ttype);
            for (j = 0; j < count; j++) {
                mask_data[j] = (*atomcmp)(BUNtail(bi, (BUN)(BUNfirst(b) + j)), nil) == 0;
                found_nil = found_nil || mask_data[j];
            }
            break;
        }
        default:
            //todo: do something with the error?
            return NULL;
    }

    if (!found_nil) {
        Py_DECREF(nullmask);
        Py_RETURN_NONE;
    }

    return (PyObject*)nullmask;
}


PyObject *PyDict_CheckForConversion(PyObject *pResult, int expected_columns, char **retcol_names, char **return_message)
{
    char *msg = MAL_SUCCEED;
    PyObject *result = PyList_New(expected_columns), *keys = PyDict_Keys(pResult);
    int i;

    if (PyList_Size(keys) != expected_columns) {
#ifdef _PYAPI_WARNINGS_
        if (PyList_Size(keys) > expected_columns) {
            WARNING_MESSAGE("WARNING: Expected %d return values, but a dictionary with "SSZFMT" values was returned instead.\n", expected_columns, PyList_Size(keys));
        }
#endif
    }

    for(i = 0; i < expected_columns; i++) {
        PyObject *object = PyDict_GetItemString(pResult, retcol_names[i]);
        if (object == NULL) {
            msg = createException(MAL, "pyapi.eval", "Expected a return value with name \"%s\", but this key was not present in the dictionary.", retcol_names[i]);
            goto wrapup;
        }
        Py_INCREF(object);
        object = PyObject_CheckForConversion(object, 1, NULL, return_message);
        if (object == NULL) {
            msg = createException(MAL, "pyapi.eval", "Error converting dict return value \"%s\": %s.", retcol_names[i], *return_message);
            GDKfree(*return_message);
            goto wrapup;
        }
        if (PyList_CheckExact(object)) {
            PyObject *item = PyList_GetItem(object, 0);
            PyList_SetItem(result, i, item);
            Py_INCREF(item);
            Py_DECREF(object);
        } else {
            msg = createException(MAL, "pyapi.eval", "Why is this not a list?");
            goto wrapup;
        }
    }
    Py_DECREF(keys);
    Py_DECREF(pResult);
    //Py_INCREF(result);
    return result;
wrapup:
    *return_message = msg;
    Py_DECREF(result);
    Py_DECREF(keys);
    Py_DECREF(pResult);
    return NULL;
}

PyObject *PyObject_CheckForConversion(PyObject *pResult, int expected_columns, int *actual_columns, char **return_message)
{
    char *msg;
    int columns = 0;
    if (pResult) {
        PyObject * pColO = NULL;
        if (PyType_IsPandasDataFrame(pResult)) {
            //the result object is a Pandas data frame
            //we can convert the pandas data frame to a numpy array by simply accessing the "values" field (as pandas dataframes are numpy arrays internally)
            pResult = PyObject_GetAttrString(pResult, "values");
            if (pResult == NULL) {
                msg = createException(MAL, "pyapi.eval", "Invalid Pandas data frame.");
                goto wrapup;
            }
            //we transpose the values field so it's aligned correctly for our purposes
            pResult = PyObject_GetAttrString(pResult, "T");
            if (pResult == NULL) {
                msg = createException(MAL, "pyapi.eval", "Invalid Pandas data frame.");
                goto wrapup;
            }
        }

        if (PyType_IsPyScalar(pResult)) { //check if the return object is a scalar
            if (expected_columns == 1 || expected_columns <= 0)  {
                //if we only expect a single return value, we can accept scalars by converting it into an array holding an array holding the element (i.e. [[pResult]])
                PyObject *list = PyList_New(1);
                PyList_SetItem(list, 0, pResult);
                pResult = list;

                list = PyList_New(1);
                PyList_SetItem(list, 0, pResult);
                pResult = list;

                columns = 1;
            }
            else {
                //the result object is a scalar, yet we expect more than one return value. We can only convert the result into a list with a single element, so the output is necessarily wrong.
                msg = createException(MAL, "pyapi.eval", "A single scalar was returned, yet we expect a list of %d columns. We can only convert a single scalar into a single column, thus the result is invalid.", expected_columns);
                goto wrapup;
            }
        }
        else {
            //if it is not a scalar, we check if it is a single array
            bool IsSingleArray = TRUE;
            PyObject *data = pResult;
            if (PyType_IsNumpyMaskedArray(data)) {
                data = PyObject_GetAttrString(pResult, "data");
                if (data == NULL) {
                    msg = createException(MAL, "pyapi.eval", "Invalid masked array.");
                    goto wrapup;
                }
            }
            if (PyType_IsNumpyArray(data)) {
                if (PyArray_NDIM((PyArrayObject*)data) != 1) {
                    IsSingleArray = FALSE;
                }
                else {
                    pColO = PyArray_GETITEM((PyArrayObject*)data, PyArray_GETPTR1((PyArrayObject*)data, 0));
                    IsSingleArray = PyType_IsPyScalar(pColO);
                }
            }
            else if (PyList_Check(data)) {
                pColO = PyList_GetItem(data, 0);
                IsSingleArray = PyType_IsPyScalar(pColO);
            } else if (!PyType_IsNumpyMaskedArray(data)) {
                //it is neither a python array, numpy array or numpy masked array, thus the result is unsupported! Throw an exception!
                msg = createException(MAL, "pyapi.eval", "Unsupported result object. Expected either a list, dictionary, a numpy array, a numpy masked array or a pandas data frame, but received an object of type \"%s\"", PyString_AsString(PyObject_Str(PyObject_Type(data))));
                goto wrapup;
            }

            if (IsSingleArray) {
                if (expected_columns == 1 || expected_columns <= 0) {
                    //if we only expect a single return value, we can accept a single array by converting it into an array holding an array holding the element (i.e. [pResult])
                    PyObject *list = PyList_New(1);
                    PyList_SetItem(list, 0, pResult);
                    pResult = list;

                    columns = 1;
                }
                else {
                    //the result object is a single array, yet we expect more than one return value. We can only convert the result into a list with a single array, so the output is necessarily wrong.
                    msg = createException(MAL, "pyapi.eval", "A single array was returned, yet we expect a list of %d columns. The result is invalid.", expected_columns);
                    goto wrapup;
                }
            }
            else {
                //the return value is an array of arrays, all we need to do is check if it is the correct size
                int results = 0;
                if (PyList_Check(data)) results = (int)PyList_Size(data);
                else results = (int)PyArray_DIMS((PyArrayObject*)data)[0];
                columns = results;
                if (results != expected_columns && expected_columns > 0) {
                    //wrong return size, we expect pci->retc arrays
                    msg = createException(MAL, "pyapi.eval", "An array of size %d was returned, yet we expect a list of %d columns. The result is invalid.", results, expected_columns);
                    goto wrapup;
                }
            }
        }
    } else {
        msg = createException(MAL, "pyapi.eval", "Invalid result object. No result object could be generated.");
        goto wrapup;
    }

    if (actual_columns != NULL) *actual_columns = columns;
    return pResult;
wrapup:
    if (actual_columns != NULL) *actual_columns = columns;
    *return_message = msg;
    return NULL;
}


bool PyObject_PreprocessObject(PyObject *pResult, PyReturn *pyreturn_values, int column_count, char **return_message)
{
    int i;
    char *msg;
    for (i = 0; i < column_count; i++) {
        // Refers to the current Numpy mask (if it exists)
        PyObject *pMask = NULL;
        // Refers to the current Numpy array
        PyObject * pColO = NULL;
        // This is the PyReturn header information for the current return value, we will fill this now
        PyReturn *ret = &pyreturn_values[i];

        ret->multidimensional = FALSE;
        // There are three possibilities (we have ensured this right after executing the Python call by calling PyObject_CheckForConversion)
        // 1: The top level result object is a PyList or Numpy Array containing pci->retc Numpy Arrays
        // 2: The top level result object is a (pci->retc x N) dimensional Numpy Array [Multidimensional]
        // 3: The top level result object is a (pci->retc x N) dimensional Numpy Masked Array [Multidimensional]
        if (PyList_Check(pResult)) {
            // If it is a PyList, we simply get the i'th Numpy array from the PyList
            pColO = PyList_GetItem(pResult, i);
        }
        else {
            // If it isn't, the result object is either a Nump Masked Array or a Numpy Array
            PyObject *data = pResult;
            if (PyType_IsNumpyMaskedArray(data)) {
                data = PyObject_GetAttrString(pResult, "data"); // If it is a Masked array, the data is stored in the masked_array.data attribute
                pMask = PyObject_GetAttrString(pResult, "mask");
            }

            // We can either have a multidimensional numpy array, or a single dimensional numpy array
            if (PyArray_NDIM((PyArrayObject*)data) != 1) {
                // If it is a multidimensional numpy array, we have to convert the i'th dimension to a NUMPY array object
                ret->multidimensional = TRUE;
                ret->result_type = PyArray_DESCR((PyArrayObject*)data)->type_num;
            }
            else {
                // If it is a single dimensional Numpy array, we get the i'th Numpy array from the Numpy Array
                pColO = PyArray_GETITEM((PyArrayObject*)data, PyArray_GETPTR1((PyArrayObject*)data, i));
            }
        }

        // Now we have to do some preprocessing on the data
        if (ret->multidimensional) {
            // If it is a multidimensional Numpy array, we don't need to do any conversion, we can just do some pointers
            ret->count = PyArray_DIMS((PyArrayObject*)pResult)[1];
            ret->numpy_array = pResult;
            ret->numpy_mask = pMask;
            ret->array_data = PyArray_DATA((PyArrayObject*)ret->numpy_array);
            if (ret->numpy_mask != NULL) ret->mask_data = PyArray_DATA((PyArrayObject*)ret->numpy_mask);
            ret->memory_size = PyArray_DESCR((PyArrayObject*)ret->numpy_array)->elsize;
        }
        else {
            // If it isn't we need to convert pColO to the expected Numpy Array type
            ret->numpy_array = PyArray_FromAny(pColO, NULL, 1, 1, NPY_ARRAY_CARRAY | NPY_ARRAY_FORCECAST, NULL);
            if (ret->numpy_array == NULL) {
                msg = createException(MAL, "pyapi.eval", "Could not create a Numpy array from the return type.\n");
                goto wrapup;
            }

            ret->result_type = PyArray_DESCR((PyArrayObject*)ret->numpy_array)->type_num; // We read the result type from the resulting array
            ret->memory_size = PyArray_DESCR((PyArrayObject*)ret->numpy_array)->elsize;
            ret->count = PyArray_DIMS((PyArrayObject*)ret->numpy_array)[0];
            ret->array_data = PyArray_DATA((PyArrayObject*)ret->numpy_array);
            // If pColO is a Masked array, we convert the mask to a NPY_BOOL numpy array
            if (PyObject_HasAttrString(pColO, "mask")) {
                pMask = PyObject_GetAttrString(pColO, "mask");
                if (pMask != NULL) {
                    ret->numpy_mask = PyArray_FromAny(pMask, PyArray_DescrFromType(NPY_BOOL), 1, 1,  NPY_ARRAY_CARRAY, NULL);
                    if (ret->numpy_mask == NULL || PyArray_DIMS((PyArrayObject*)ret->numpy_mask)[0] != (int)ret->count)
                    {
                        PyErr_Clear();
                        pMask = NULL;
                        ret->numpy_mask = NULL;
                    }
                }
            }
            if (ret->numpy_mask != NULL) ret->mask_data = PyArray_DATA((PyArrayObject*)ret->numpy_mask);
        }
    }
    return TRUE;
wrapup:
    *return_message = msg;
    return FALSE;
}

BAT *PyObject_ConvertToBAT(PyReturn *ret, sql_subtype *type, int bat_type, int i, oid seqbase, char **return_message, bool copy)
{
    BAT *b = NULL;
    size_t index_offset = 0;
    char *msg;
    size_t iu;

    if (ret->multidimensional) index_offset = i;

    switch(GetSQLType(type))
    {
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

    VERBOSE_MESSAGE("- Returning a Numpy Array of type %s of size %zu and storing it in a BAT of type %s\n", PyType_Format(ret->result_type), ret->count,  BatType_Format(bat_type));
    switch (bat_type)
    {
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
    case TYPE_str:
        {
            bool *mask = NULL;
            char *data = NULL;
            char *utf8_string = NULL;
            if (ret->mask_data != NULL)
            {
                mask = (bool*)ret->mask_data;
            }
            if (ret->array_data == NULL)
            {
                msg = createException(MAL, "pyapi.eval", "No return value stored in the structure.  n");
                goto wrapup;
            }
            data = (char*) ret->array_data;

            if (ret->result_type != NPY_OBJECT) {
                utf8_string = GDKzalloc(utf8string_minlength + ret->memory_size + 1);
                utf8_string[utf8string_minlength + ret->memory_size] = '\0';
            }

            b = BATnew(TYPE_void, TYPE_str, (BUN) ret->count, TRANSIENT);
            BATseqbase(b, seqbase); b->T->nil = 0; b->T->nonil = 1;
            b->tkey = 0; b->tsorted = 0; b->trevsorted = 0;
            VERBOSE_MESSAGE("- Collecting return values of type %s.\n", PyType_Format(ret->result_type));
            switch(ret->result_type)
            {
                case NPY_BOOL:      NP_COL_BAT_STR_LOOP(b, bit, "%hhd"); break;
                case NPY_BYTE:      NP_COL_BAT_STR_LOOP(b, bte, "%hhd"); break;
                case NPY_SHORT:     NP_COL_BAT_STR_LOOP(b, sht, "%hd"); break;
                case NPY_INT:       NP_COL_BAT_STR_LOOP(b, int, "%d"); break;
                case NPY_LONG:      NP_COL_BAT_STR_LOOP(b, long, "%ld"); break;
                case NPY_LONGLONG:  NP_COL_BAT_STR_LOOP(b, lng, LLFMT); break;
                case NPY_UBYTE:     NP_COL_BAT_STR_LOOP(b, unsigned char, "%hhu"); break;
                case NPY_USHORT:    NP_COL_BAT_STR_LOOP(b, unsigned short, "%hu"); break;
                case NPY_UINT:      NP_COL_BAT_STR_LOOP(b, unsigned int, "%u"); break;
                case NPY_ULONG:     NP_COL_BAT_STR_LOOP(b, unsigned long, "%lu"); break;
                case NPY_ULONGLONG: NP_COL_BAT_STR_LOOP(b, unsigned long long, ULLFMT); break;
                case NPY_FLOAT16:
                case NPY_FLOAT:     NP_COL_BAT_STR_LOOP(b, flt, "%f"); break;
                case NPY_DOUBLE:
                case NPY_LONGDOUBLE: NP_COL_BAT_STR_LOOP(b, dbl, "%lf"); break;
                case NPY_STRING:
                    for (iu = 0; iu < ret->count; iu++) {
                        if (mask != NULL && (mask[index_offset * ret->count + iu]) == TRUE) {
                            b->T->nil = 1;
                            BUNappend(b, str_nil, FALSE);
                        }  else {
                            if (!string_copy(&data[(index_offset * ret->count + iu) * ret->memory_size], utf8_string, ret->memory_size, true)) {
                                msg = createException(MAL, "pyapi.eval", "Invalid string encoding used. Please return a regular ASCII string, or a Numpy_Unicode object.\n");
                                goto wrapup;
                            }
                            BUNappend(b, utf8_string, FALSE);
                        }
                    }
                    break;
                case NPY_UNICODE:
                    for (iu = 0; iu < ret->count; iu++) {
                        if (mask != NULL && (mask[index_offset * ret->count + iu]) == TRUE) {
                            b->T->nil = 1;
                            BUNappend(b, str_nil, FALSE);
                        }  else {
                            utf32_to_utf8(0, ret->memory_size / 4, utf8_string, (const Py_UNICODE*)(&data[(index_offset * ret->count + iu) * ret->memory_size]));
                            BUNappend(b, utf8_string, FALSE);
                        }
                    }
                    break;
                case NPY_OBJECT:
                {
                    //The resulting array is an array of pointers to various python objects
                    //Because the python objects can be of any size, we need to allocate a different size utf8_string for every object
                    //we will first loop over all the objects to get the maximum size needed, so we only need to do one allocation
                    size_t utf8_size = utf8string_minlength;
                    for (iu = 0; iu < ret->count; iu++) {
                        size_t size = utf8string_minlength;
                        PyObject *obj;
                        if (mask != NULL && (mask[index_offset * ret->count + iu]) == TRUE) continue;
                        obj = *((PyObject**) &data[(index_offset * ret->count + iu) * ret->memory_size]);
                        if (PyString_CheckExact(obj) || PyByteArray_CheckExact(obj)) {
                            size = Py_SIZE(obj);     //Normal strings are 1 string per character
                        } else if (PyUnicode_CheckExact(obj)) {
                            size = Py_SIZE(obj) * 4; //UTF32 is 4 bytes per character
                        }
                        if (size > utf8_size) utf8_size = size;
                    }
                    utf8_string = GDKzalloc(utf8_size);
                    for (iu = 0; iu < ret->count; iu++) {
                        if (mask != NULL && (mask[index_offset * ret->count + iu]) == TRUE) {
                            b->T->nil = 1;
                            BUNappend(b, str_nil, FALSE);
                        } else {
                            //we try to handle as many types as possible
                            PyObject *obj = *((PyObject**) &data[(index_offset * ret->count + iu) * ret->memory_size]);
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
                            BUNappend(b, utf8_string, FALSE);
                        }
                    }
                    break;
                }
                default:
                    msg = createException(MAL, "pyapi.eval", "Unrecognized type. Could not convert to NPY_UNICODE.\n");
                    goto wrapup;
            }
            GDKfree(utf8_string);

            b->T->nonil = 1 - b->T->nil;
            BATsetcount(b, (BUN) ret->count);
            BATsettrivprop(b);
            break;
        }
    default:
        msg = createException(MAL, "pyapi.eval", "Unrecognized BAT type %s.\n", BatType_Format(bat_type));
        goto wrapup;
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
wrapup:
    *return_message = msg;
    return NULL;
}

bit ConvertableSQLType(sql_subtype *sql_subtype) {
    switch(GetSQLType(sql_subtype)) {
        case EC_DATE:
        case EC_TIME:
        case EC_TIMESTAMP:
        case EC_DEC:
            return 1;
    }
    return 0;
}

int GetSQLType(sql_subtype *sql_subtype) {
    if (!sql_subtype) return -1;
    if (!sql_subtype->type) return -1;
    return sql_subtype->type->eclass;
}

str ConvertFromSQLType(Client cntxt, BAT *b, sql_subtype *sql_subtype, BAT **ret_bat,  int *ret_type)
{
    str res = MAL_SUCCEED;
    int conv_type; 

    assert(sql_subtype);
    assert(sql_subtype->type);

    switch(sql_subtype->type->eclass)
    {
        case EC_DATE:
        case EC_TIME:
        case EC_TIMESTAMP:
            conv_type = TYPE_str;
            break;
        case EC_DEC:
            conv_type = TYPE_dbl;
            break;
        default: 
            return createException(MAL, "pyapi.eval", "Convert From SQL Type: Unrecognized SQL type %s (%d).", sql_subtype->type->sqlname, sql_subtype->type->eclass);
    }

    if (conv_type == TYPE_str)
    {
        // maybe there's a more elegant way for obj->str conversion than calling this MAL function? probably not
        int eclass = sql_subtype->type->eclass;
        int d1 = 0;
        int s1 = 0;
        int has_tz = 0;
        bat bid = b->batCacheid;
        int digits = 0;

        int i;
        int nvar = 7; // variables we need to fill in
        MalBlkRecord mb;
        MalStack*     stk = NULL;
        InstrRecord*  pci = NULL;

        switch(eclass)
        {
            case EC_DATE:
                d1 = 0;
                break;
            case EC_TIME:
                d1 = 1;
                break;
            case EC_TIMESTAMP:
                d1 = 7;
                break;
            default: 
                break;
        }

        // very black MAL magic below
        stk = GDKmalloc(sizeof(MalStack) + nvar * sizeof(ValRecord));
        pci = GDKmalloc(sizeof(InstrRecord) + nvar * sizeof(int));
        assert(stk != NULL && pci != NULL); // cough, cough
        for (i = 0; i < nvar; i++) {
            pci->argv[i] = i;
        }

        stk->stk[0].vtype = TYPE_bat;
        stk->stk[1].val.ival = eclass;
        stk->stk[1].vtype = TYPE_int;
        stk->stk[2].val.ival = d1;
        stk->stk[2].vtype = TYPE_int;
        stk->stk[3].val.ival = s1;
        stk->stk[3].vtype = TYPE_int;
        stk->stk[4].val.ival = has_tz;
        stk->stk[4].vtype = TYPE_int;
        stk->stk[5].val.bval = bid;
        stk->stk[5].vtype = TYPE_bat;
        stk->stk[6].val.ival = digits;
        stk->stk[6].vtype = TYPE_int;

        res = (*SQLbatstr_cast_ptr)(cntxt, &mb, stk, pci);

        if (res == MAL_SUCCEED) {
            *ret_bat = BATdescriptor(stk->stk[0].val.bval);
            *ret_type = TYPE_str;
        } else {
            *ret_bat = NULL;
        }
        
        GDKfree(stk);
        GDKfree(pci);
        return res;
    } 
    else if (conv_type == TYPE_dbl)
    {
        int bat_type = ATOMstorage(b->T->type);
        int hpos = sql_subtype->scale; //this value isn't right, it's always 3. todo: find the right scale value (i.e. where the decimal point is)
        bat result = 0;
        //decimal values can be stored in various numeric fields, so check the numeric field and convert the one it's actually stored in
        switch(bat_type) 
        {
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
                return createException(MAL, "pyapi.eval", "Unsupported decimal storage type.");
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

str 
ConvertToSQLType(Client cntxt, BAT *b, sql_subtype *sql_subtype, BAT **ret_bat, int *ret_type)
{
    str res = MAL_SUCCEED;
    bat result_bat = 0;
    int digits = sql_subtype->digits;
    int scale = sql_subtype->scale;
    (void) cntxt;

    assert(sql_subtype);
    assert(sql_subtype->type);

    switch(sql_subtype->type->eclass)
    {
        case EC_TIMESTAMP:
            res = (*batstr_2time_timestamp_ptr)(&result_bat, &b->batCacheid, &digits);
            break;
        case EC_TIME:
            res = (*batstr_2time_daytime_ptr)(&result_bat, &b->batCacheid, &digits);
            break;
        case EC_DATE:
            res = (*batstr_2_date_ptr)(&result_bat, &b->batCacheid);
            break;
        case EC_DEC:
            res = (*batdbl_num2dec_lng_ptr)(&result_bat, &b->batCacheid, &digits, &scale);
            break;
        default: 
            return createException(MAL, "pyapi.eval", "Convert To SQL Type: Unrecognized SQL type %s (%d).", sql_subtype->type->sqlname, sql_subtype->type->eclass);
    }
    if (res == MAL_SUCCEED) {
        *ret_bat = BATdescriptor(result_bat);
        *ret_type = (*ret_bat)->T->type;
    }

    return res;
}

static 
void ComputeParallelAggregation(AggrParams *p)
{
    int i;
    size_t group_it, ai;
    bool gstate = 0;
    //now perform the actual aggregation
    //we perform one aggregation per group

    //we need the GIL to execute the functions
    gstate = Python_ObtainGIL();
    for(group_it = p->group_start; group_it < p->group_end; group_it++) {
        // we first have to construct new 
        PyObject *pArgsPartial = PyTuple_New(p->named_columns + p->additional_columns);
        PyObject *pColumnsPartial = PyDict_New();
        PyObject *result;
        size_t group_elements = (*p->group_counts)[group_it];
        ai = 0;
        // iterate over columns
        for(i = 0; i < (int)p->named_columns; i++) {
            PyObject *vararray = NULL;
            PyInput input = (*p->pyinput_values)[i];
            if (input.scalar) {
                // scalar not handled yet
                vararray = input.result;
            } else {
                npy_intp elements[1] = { group_elements };
                switch(input.bat_type) {
                    case TYPE_bte:
                        vararray = PyArray_New(&PyArray_Type, 1, 
                            elements, 
                            NPY_INT8, 
                            NULL, ((bte***)(*p->split_bats))[group_it][i], 0, 
                            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
                        break;
                    case TYPE_sht:
                        vararray = PyArray_New(&PyArray_Type, 1, 
                            elements, 
                            NPY_INT16, 
                            NULL, ((sht***)(*p->split_bats))[group_it][i], 0, 
                            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
                        break;
                    case TYPE_int:
                        vararray = PyArray_New(&PyArray_Type, 1, 
                            elements, 
                            NPY_INT32, 
                            NULL, ((int***)(*p->split_bats))[group_it][i], 0, 
                            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
                        break;
                    case TYPE_lng:
                        vararray = PyArray_New(&PyArray_Type, 1, 
                            elements, 
                            NPY_INT64, 
                            NULL, ((lng***)(*p->split_bats))[group_it][i], 0, 
                            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
                        break;
                    case TYPE_flt:
                        vararray = PyArray_New(&PyArray_Type, 1, 
                            elements, 
                            NPY_FLOAT32, 
                            NULL, ((flt***)(*p->split_bats))[group_it][i], 0, 
                            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
                        break;
                #ifdef HAVE_HGE
                    case TYPE_hge:
                #endif
                    case TYPE_dbl:
                        vararray = PyArray_New(&PyArray_Type, 1, 
                            elements, 
                            NPY_FLOAT64, 
                            NULL, ((dbl***)(*p->split_bats))[group_it][i], 0, 
                            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
                        break;
                    case TYPE_str:
                        vararray = PyArray_New(&PyArray_Type, 1, 
                            elements, 
                            NPY_OBJECT, 
                            NULL, ((PyObject****)(*p->split_bats))[group_it][i], 0, 
                            NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
                        break;
                }

                if (vararray == NULL) {
                    p->msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL" to create NumPy array.");
                    goto wrapup;
                }
            }
            // fill in _columns array
            PyDict_SetItemString(pColumnsPartial, (*p->args)[p->base + i], vararray);

            PyTuple_SetItem(pArgsPartial, ai++, vararray);
        }

        // additional parameters
        PyTuple_SetItem(pArgsPartial, ai++, pColumnsPartial);
        PyTuple_SetItem(pArgsPartial, ai++, *p->column_types_dict); Py_INCREF(*p->column_types_dict);
        PyTuple_SetItem(pArgsPartial, ai++, *p->connection); Py_INCREF(*p->connection);

        // call the aggregation function
        result = PyObject_CallObject(*p->function, pArgsPartial);
        Py_DECREF(pArgsPartial);

        if (result == NULL) {
            p->msg = PyError_CreateException("Python exception", *p->pycall);
            goto wrapup;
        }
        // gather results
        p->result_objects[group_it] = result;
    }
    //release the GIL again   
wrapup: 
    gstate = Python_ReleaseGIL(gstate);
}

bool PyType_IsNumpyArray(PyObject *object)
{
    return PyArray_CheckExact(object);
}

bool Python_ObtainGIL(void)
{
    PyGILState_STATE gstate = PyGILState_Ensure();
    return gstate == PyGILState_LOCKED ? 0 : 1;
}

bool Python_ReleaseGIL(bool state)
{
    PyGILState_STATE gstate = state == 0 ? PyGILState_LOCKED : PyGILState_UNLOCKED;
    PyGILState_Release(gstate);
    return 0;
}

void* lookup_function(char *func, char* library) {
    void *dl, *fun;
    dl = mdlopen(library, RTLD_NOW | RTLD_GLOBAL);
    if (dl == NULL) {
        return NULL;
    }
    fun = dlsym(dl, func);
    dlclose(dl);
    return fun;
}

