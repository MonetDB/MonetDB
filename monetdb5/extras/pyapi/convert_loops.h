
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
        bat->S->count = (BUN) ret->count;                                                                               \
        bat->S->capacity = (BUN) ret->count;                                                                            \
        bat->S->copiedtodisk = false;                                                                                   \
        /*Take over the data from the numpy array*/                                                                     \
        if (ret->numpy_array != NULL) PyArray_CLEARFLAGS((PyArrayObject*)ret->numpy_array, NPY_ARRAY_OWNDATA);          \
    }
#endif

// This #define converts a Numpy Array to a BAT by copying the internal data to the BAT. It assumes the BAT 'bat' is already created with the proper size.
// This should only be used with integer data that can be cast. It assumes the Numpy Array has an internal array of type 'mtpe_from', and the BAT has an internal array of type 'mtpe_to'.
// it then does the cast by simply doing BAT[i] = (mtpe_to) ((mtpe_from*)NUMPY_ARRAY[i]), which only works if both mtpe_to and mtpe_from are integers
#define NP_COL_BAT_LOOP(bat, mtpe_to, mtpe_from,index) {                                                                                   \
    if (mask == NULL)                                                                                                                            \
    {                                                                                                                                            \
        for (iu = 0; iu < ret->count; iu++)                                                                                                      \
        {                                                                                                                                        \
            ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[index + iu] = (mtpe_to)(*(mtpe_from*)(&data[(index_offset * ret->count + iu) * ret->memory_size]));    \
        }                                                                                                                                        \
    }                                                                                                                                            \
    else                                                                                                                                         \
    {                                                                                                                                            \
        for (iu = 0; iu < ret->count; iu++)                                                                                                      \
        {                                                                                                                                        \
            if (mask[index_offset * ret->count + iu] == TRUE)                                                                                    \
            {                                                                                                                                    \
                bat->T->nil = 1;                                                                                                                 \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[index + iu] = mtpe_to##_nil;                                                               \
            }                                                                                                                                    \
            else                                                                                                                                 \
            {                                                                                                                                    \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[index + iu] = (mtpe_to)(*(mtpe_from*)(&data[(index_offset * ret->count + iu) * ret->memory_size]));\
            }                                                                                                                                    \
        }                                                                                                                                        \
    } }

// This #define converts a Numpy Array to a BAT by copying the internal data to the BAT. It converts the data from the Numpy Array to the BAT using a function
// This function has to have the prototype 'bool function(void *data, size_t memory_size, mtpe_to *resulting_value)', and either return False (if conversion fails)
//  or write the value into the 'resulting_value' pointer. This is used convertring strings/unicodes/python objects to numeric values.
#define NP_COL_BAT_LOOP_FUNC(bat, mtpe_to, func, ptrtpe,index) {                                                                                                    \
    mtpe_to value;                                                                                                                                    \
    if (mask == NULL)                                                                                                                                 \
    {                                                                                                                                                 \
        for (iu = 0; iu < ret->count; iu++)                                                                                                           \
        {                                                                                                                                             \
            msg = func((ptrtpe*)&data[(index_offset * ret->count + iu) * ret->memory_size], ret->memory_size, &value);                                \
            if (msg != MAL_SUCCEED) {                                                                                                                 \
                goto wrapup;                                                                                                                          \
            }                                                                                                                                         \
            ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[index + iu] = value;                                                                                \
        }                                                                                                                                             \
    }                                                                                                                                                 \
    else                                                                                                                                              \
    {                                                                                                                                                 \
        for (iu = 0; iu < ret->count; iu++)                                                                                                           \
        {                                                                                                                                             \
            if (mask[index_offset * ret->count + iu] == TRUE)                                                                                         \
            {                                                                                                                                         \
                bat->T->nil = 1;                                                                                                                      \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[index + iu] = mtpe_to##_nil;                                                                    \
            }                                                                                                                                         \
            else                                                                                                                                      \
            {                                                                                                                                         \
                msg = func((ptrtpe*)&data[(index_offset * ret->count + iu) * ret->memory_size], ret->memory_size, &value);                            \
                if (msg != MAL_SUCCEED) {                                                                                                             \
                    goto wrapup;                                                                                                                      \
                }                                                                                                                                     \
                ((mtpe_to*) Tloc(bat, BUNfirst(bat)))[index + iu] = value;                                                                            \
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

#define NP_INSERT_BAT(bat, mtpe, index)  {                                                                                                              \
    switch(ret->result_type)                                                                                                                            \
    {                                                                                                                                                   \
        case NPY_BOOL:       NP_COL_BAT_LOOP(bat, mtpe, char, index); break;                                                                            \
        case NPY_BYTE:       NP_COL_BAT_LOOP(bat, mtpe, char, index); break;                                                                            \
        case NPY_SHORT:      NP_COL_BAT_LOOP(bat, mtpe, short, index); break;                                                                           \
        case NPY_INT:        NP_COL_BAT_LOOP(bat, mtpe, int, index); break;                                                                             \
        case NPY_LONG:       NP_COL_BAT_LOOP(bat, mtpe, long, index); break;                                                                            \
        case NPY_LONGLONG:   NP_COL_BAT_LOOP(bat, mtpe, long long, index); break;                                                                       \
        case NPY_UBYTE:      NP_COL_BAT_LOOP(bat, mtpe, unsigned char, index); break;                                                                   \
        case NPY_USHORT:     NP_COL_BAT_LOOP(bat, mtpe, unsigned short, index); break;                                                                  \
        case NPY_UINT:       NP_COL_BAT_LOOP(bat, mtpe, unsigned int, index); break;                                                                    \
        case NPY_ULONG:      NP_COL_BAT_LOOP(bat, mtpe, unsigned long, index); break;                                                                   \
        case NPY_ULONGLONG:  NP_COL_BAT_LOOP(bat, mtpe, unsigned long long, index); break;                                                              \
        case NPY_FLOAT16:                                                                                                                               \
        case NPY_FLOAT:      NP_COL_BAT_LOOP(bat, mtpe, float, index); break;                                                                           \
        case NPY_DOUBLE:     NP_COL_BAT_LOOP(bat, mtpe, double, index); break;                                                                          \
        case NPY_LONGDOUBLE: NP_COL_BAT_LOOP(bat, mtpe, long double, index); break;                                                                     \
        case NPY_STRING:     NP_COL_BAT_LOOP_FUNC(bat, mtpe, str_to_##mtpe, char, index); break;                                                        \
        case NPY_UNICODE:    NP_COL_BAT_LOOP_FUNC(bat, mtpe, unicode_to_##mtpe, PythonUnicodeType, index); break;                                       \
        case NPY_OBJECT:     NP_COL_BAT_LOOP_FUNC(bat, mtpe, pyobject_to_##mtpe, PyObject*, index); break;                                              \
        default:                                                                                                                                        \
            msg = createException(MAL, "pyapi.eval", "Unrecognized type. Could not convert to %s.\n", BatType_Format(TYPE_##mtpe));                     \
            goto wrapup;                                                                                                                                \
    }                                                                                                                                                   \
    bat->T->nonil = 1 - bat->T->nil;  }                                                                                                                 \

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
            NP_INSERT_BAT(bat, mtpe, 0);                                                                                                                   \
            BATsetcount(bat, (BUN) ret->count);                                                                                                                 \
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
