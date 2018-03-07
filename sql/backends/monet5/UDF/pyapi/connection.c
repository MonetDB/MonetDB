/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "pyapi.h"
#include "conversion.h"
#include "connection.h"
#include "type_conversion.h"
#include "gdk_interprocess.h"

CREATE_SQL_FUNCTION_PTR(void, SQLdestroyResult);
CREATE_SQL_FUNCTION_PTR(str, SQLstatementIntern);
CREATE_SQL_FUNCTION_PTR(str, create_table_from_emit);
CREATE_SQL_FUNCTION_PTR(str, append_to_table_from_emit);

static PyObject *_connection_execute(Py_ConnectionObject *self, PyObject *args)
{
	char *query = NULL;
#ifndef IS_PY3K
	if (PyUnicode_CheckExact(args)) {
		PyObject* str = PyUnicode_AsUTF8String(args);
		if (!str) {
			PyErr_Format(PyExc_Exception, "Unicode failure.");
			return NULL;
		}
		query = GDKstrdup(((PyStringObject *)str)->ob_sval);
		Py_DECREF(str);
	} else
#endif
	if (PyString_CheckExact(args)) {
#ifndef IS_PY3K
		query = GDKstrdup(((PyStringObject *)args)->ob_sval);
#else
		query = GDKstrdup(PyUnicode_AsUTF8(args));
#endif
	} else {
		PyErr_Format(PyExc_TypeError,
					 "expected a query string, but got an object of type %s",
					 Py_TYPE(args)->tp_name);
		return NULL;
	}
	if (!query) {
		PyErr_Format(PyExc_Exception, "%s", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return NULL;
	}
	if (!self->mapped || option_disable_fork) {
		// This is not a mapped process, so we can just directly execute the
		// query here
		PyObject *result;
		res_table *output = NULL;
		char *res = NULL;
//Py_BEGIN_ALLOW_THREADS;
		res = _connection_query(self->cntxt, query, &output);
//Py_END_ALLOW_THREADS;
		GDKfree(query);
		if (res != MAL_SUCCEED) {
			PyErr_Format(PyExc_Exception, "SQL Query Failed: %s",
						 (res ? getExceptionMessage(res) : "<no error>"));
			return NULL;
		}

		result = PyDict_New();
		if (output && output->nr_cols > 0) {
			PyInput input;
			PyObject *numpy_array;
			int i;
			for (i = 0; i < output->nr_cols; i++) {
				res_col col = output->cols[i];
				BAT *b = BATdescriptor(col.b);

				if (b == NULL) {
					PyErr_Format(PyExc_Exception, "Internal error: could not retrieve bat");
					return NULL;
				}

				input.bat = b;
				input.count = BATcount(b);
				input.bat_type = getBatType(b->ttype);
				input.scalar = false;
				input.sql_subtype = &col.type;

				numpy_array =
					PyMaskedArray_FromBAT(&input, 0, input.count, &res, true);
				if (!numpy_array) {
					_connection_cleanup_result(output);
					BBPunfix(b->batCacheid);
					PyErr_Format(PyExc_Exception, "SQL Query Failed: %s",
								 (res ? getExceptionMessage(res) : "<no error>"));
					return NULL;
				}
				PyDict_SetItem(result,
							   PyString_FromString(output->cols[i].name),
							   numpy_array);
				Py_DECREF(numpy_array);
				BBPunfix(input.bat->batCacheid);
			}
			_connection_cleanup_result(output);
			return result;
		} else {
			Py_RETURN_NONE;
		}
	} else {
		PyErr_Format(PyExc_Exception, "Loopback queries are not supported in parallel.");
		return NULL;
	}
}

static PyMethodDef _connectionObject_methods[] = {
	{"execute", (PyCFunction)_connection_execute, METH_O,
	 "execute(query) -> executes a SQL query on the database in the current "
	 "client context"},
	{NULL, NULL, 0, NULL} /* Sentinel */
};

PyTypeObject Py_ConnectionType = {
	_PyObject_EXTRA_INIT
// in python3 they use structs within structs to represent this information, and
// many compilers throw warnings if you don't use separate braces
// to initialize these separate structs. However, in Python2, they use #defines
// to put this information in, so we have these nice #ifdefs
#ifdef IS_PY3K
	{{
#endif
		 1, NULL
#ifdef IS_PY3K
	 }
#endif
	 ,
	 0
#ifdef IS_PY3K
	}
#endif
	,
	"monetdb._connection", sizeof(Py_ConnectionObject), 0, 0, /* tp_dealloc */
	0,														  /* tp_print */
	0,														  /* tp_getattr */
	0,														  /* tp_setattr */
	0,														  /* tp_compare */
	0,														  /* tp_repr */
	0,														  /* tp_as_number */
	0,									   /* tp_as_sequence */
	0,									   /* tp_as_mapping */
	(hashfunc)PyObject_HashNotImplemented, /* tp_hash */
	0,									   /* tp_call */
	0,									   /* tp_str */
	0,									   /* tp_getattro */
	0,									   /* tp_setattro */
	0,									   /* tp_as_buffer */
	Py_TPFLAGS_DEFAULT,					   /* tp_flags */
	"Connection to MonetDB",			   /* tp_doc */
	0,									   /* tp_traverse */
	0,									   /* tp_clear */
	0,									   /* tp_richcompare */
	0,									   /* tp_weaklistoffset */
	0,									   /* tp_iter */
	0,									   /* tp_iternext */
	_connectionObject_methods,			   /* tp_methods */
	0,									   /* tp_members */
	0,									   /* tp_getset */
	0,									   /* tp_base */
	0,									   /* tp_dict */
	0,									   /* tp_descr_get */
	0,									   /* tp_descr_set */
	0,									   /* tp_dictoffset */
	0,									   /* tp_init */
	PyType_GenericAlloc,				   /* tp_alloc */
	PyType_GenericNew,					   /* tp_new */
	PyObject_Del,						   /* tp_free */
	0, 0, 0, 0, 0, 0, 0, 0
#ifdef IS_PY3K
	,
	0
#endif
};

void _connection_cleanup_result(void *output)
{
	(*SQLdestroyResult_ptr)((res_table *)output);
}

str _connection_query(Client cntxt, char *query, res_table **result)
{
	str res = MAL_SUCCEED;
	res = (*SQLstatementIntern_ptr)(cntxt, &query, "name", 1, 0, result);
	return res;
}

str _connection_create_table(Client cntxt, char *sname, char *tname,
							 sql_emit_col *columns, size_t ncols)
{
	return (*create_table_from_emit_ptr)(cntxt, sname, tname, columns, ncols);
}

str _connection_append_to_table(Client cntxt, char *sname, char *tname,
							 sql_emit_col *columns, size_t ncols)
{
	return (*append_to_table_from_emit_ptr)(cntxt, sname, tname, columns, ncols);
}

PyObject *Py_Connection_Create(Client cntxt, bit mapped, QueryStruct *query_ptr,
							   int query_sem)
{
	register Py_ConnectionObject *op;

	op = (Py_ConnectionObject *)PyObject_MALLOC(sizeof(Py_ConnectionObject));
	if (op == NULL)
		return PyErr_NoMemory();
	PyObject_Init((PyObject *)op, &Py_ConnectionType);

	op->cntxt = cntxt;
	op->mapped = mapped;
	op->query_ptr = query_ptr;
	op->query_sem = query_sem;

	return (PyObject *)op;
}

static void _connection_import_array(void) { _import_array(); }

str _connection_init(void)
{
	str msg = MAL_SUCCEED;
	_connection_import_array();

	LOAD_SQL_FUNCTION_PTR(SQLdestroyResult);
	LOAD_SQL_FUNCTION_PTR(SQLstatementIntern);
	LOAD_SQL_FUNCTION_PTR(create_table_from_emit);
	LOAD_SQL_FUNCTION_PTR(append_to_table_from_emit);

	if (msg != MAL_SUCCEED) {
		return msg;
	}

	if (PyType_Ready(&Py_ConnectionType) < 0)
		return createException(MAL, "pyapi.eval",
				       SQLSTATE(PY000) "Failed to initialize connection type.");
	return msg;
}
