
#include "emit.h"
#include "type_conversion.h"
#include "interprocess.h"

#if PY_MAJOR_VERSION >= 3
#define IS_PY3K
#define PyString_CheckExact PyUnicode_CheckExact
#define PyString_FromString PyUnicode_FromString
#endif

#define scalar_convert(tpe) {\
    tpe val = (tpe) tpe##_nil; msg = pyobject_to_##tpe(&dictEntry, 42, &val); \
    BUNappend(self->cols[i].b, &val, 0); \
    if (msg != MAL_SUCCEED) { \
        PyErr_Format(PyExc_TypeError, "Conversion Failed: %s", msg); \
        return NULL; \
    }}

static PyObject *
_emit_emit(Py_EmitObject *self, PyObject *args) {
    size_t i, ai;
    ssize_t el_count = -1;
    str msg = MAL_SUCCEED;
    (void) self;
    if (!PyDict_Check(args)) {
        // complain
        PyErr_SetString(PyExc_TypeError, "need dict");
        return NULL;
    }

    for (i = 0; i < self->ncols; i++) {
        PyObject *dictEntry = PyDict_GetItemString(args, self->cols[i].name);
        ssize_t this_size = 1;
        if (dictEntry) {
            if (!PyType_IsPyScalar(dictEntry)) {
                this_size = Py_SIZE(dictEntry);
            }
            if (el_count < 0) el_count = this_size;
            else {
                if (el_count != this_size) {
                    PyErr_SetString(PyExc_TypeError, "need same length values");
                    // todo: better error message!
                    return NULL;
                }
            }
        }
    }
    if (el_count < 1) {
        PyErr_SetString(PyExc_TypeError, "need at least some values");
        // todo: better error message!
        return NULL;
    }

    // TODO: check for dict entries not matched by any column and complain if present


    for (i = 0; i < self->ncols; i++) {
        PyObject *dictEntry = PyDict_GetItemString(args, self->cols[i].name);
        if (dictEntry) {
            if (PyType_IsPyScalar(dictEntry)) {
                switch (self->cols[i].b->T->type)
                    {
                    case TYPE_bit:
                        scalar_convert(bit);
                        break;
                    case TYPE_bte:
                        scalar_convert(bte);
                        break;
                    case TYPE_sht:
                        scalar_convert(sht);
                        break;
                    case TYPE_int:
                        scalar_convert(int);
                        break;
                    case TYPE_oid:
                        scalar_convert(oid);
                        break;
                    case TYPE_lng:
                        scalar_convert(lng);
                        break;
                    case TYPE_flt:
                        scalar_convert(flt);
                        break;
                    case TYPE_dbl:
                        scalar_convert(dbl);
                        break;
                #ifdef HAVE_HGE
                    case TYPE_hge:
                        scalar_convert(hge);
                        break;
                #endif
                    case TYPE_str:
                    {
                        str val = NULL;
                        msg = pyobject_to_str(&dictEntry, 42, &val);
                        BUNappend(self->cols[i].b, val, 0);
                        if (val) {
                            free(val);
                        }
                        if (msg != MAL_SUCCEED) {
                            PyErr_Format(PyExc_TypeError, "Conversion Failed: %s", msg);
                            return NULL;
                        }
                    }
                        break;
                    default:
                        break;
                        // complain
                }
            } else {
                // TODO: handle dicts with array values
            }
        } else {
            for (ai = 0; ai < (size_t) el_count; ai++) {
                BUNappend(self->cols[i].b, ATOMnil(self->cols[i].b->T->type), 0);
            }
        }
    }
    self->nvals += el_count;
    Py_RETURN_NONE;
}


static PyMethodDef _emitObject_methods[] = {
    {"emit", (PyCFunction)_emit_emit, METH_O,"emit(dictionary) -> returns parsed values for table insertion"},
    {NULL,NULL,0,NULL}  /* Sentinel */
};

PyTypeObject Py_EmitType = {
    PyObject_HEAD_INIT(NULL)
    0,
    "monetdb._emit",
    sizeof(Py_EmitObject),
    0,
    0,                                          /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    (hashfunc)PyObject_HashNotImplemented,      /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                         /* tp_flags */
    "Value Emitter",                    /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    _emitObject_methods,                  /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    0,                                          /* tp_init */
    PyType_GenericAlloc,                        /* tp_alloc */
    PyType_GenericNew,                          /* tp_new */
    PyObject_Del,                               /* tp_free */
    0,
    0,
    0,
    0,
    0,
    0,
    0,
    0
#ifdef IS_PY3K
    ,0
#endif
};



PyObject *Py_Emit_Create(EmitCol *cols, size_t ncols)
{
    register Py_EmitObject *op;

    op = (Py_EmitObject *)PyObject_MALLOC(sizeof(Py_EmitObject));
    if (op == NULL)
        return PyErr_NoMemory();
    PyObject_Init((PyObject*)op, &Py_EmitType);
    op->cols = cols;
    op->ncols = ncols;
    op->nvals = 0;
    return (PyObject*) op;
}

str _emit_init(void)
{
    _import_array();
    if (PyType_Ready(&Py_EmitType) < 0)
        return createException(MAL, "pyapi.eval", "Failed to initialize emit type.");
    return MAL_SUCCEED;
}
