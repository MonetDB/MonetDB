/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "pyapi.h"
#include "conversion.h"
#include "connection.h"
#include "emit.h"

#include "unicode.h"
#include "pytypes.h"
#include "gdk_interprocess.h"
#include "type_conversion.h"
#include "formatinput.h"

static void _loader_import_array(void) { _import_array(); }

str _loader_init(void)
{
	str msg = MAL_SUCCEED;
	_loader_import_array();
	msg = _emit_init();
	if (msg != MAL_SUCCEED) {
		return msg;
	}

	if (PyType_Ready(&Py_ConnectionType) < 0)
		return createException(MAL, "pyapi.eval",
							   "Failed to initialize loader functions.");
	return msg;
}

static int
pyapi_list_length(list *l)
{
	if (l)
		return l->cnt;
	return 0;
}

str 
PYFUNCNAME(PyAPIevalLoader)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
    sql_func * sqlfun;
    sql_subfunc * sqlmorefun;
    str exprStr;

	const int additional_columns = 2;
	int i = 1, ai = 0;
	char *pycall = NULL;
	str *args = NULL;
	char *msg = MAL_SUCCEED;
	node *argnode, *n, *n2;
	PyObject *pArgs = NULL, *pEmit = NULL,
			 *pConnection; // this is going to be the parameter tuple
	PyObject *code_object = NULL;
	sql_emit_col *cols = NULL;
	bool gstate = 0;
	int unnamedArgs = 0;
	int argcount = pci->argc;
	bool create_table = false;
	BUN nval = 0;
	int ncols = 0;

	char *loader_additional_args[] = {"_emit", "_conn"};

    if (!PYFUNCNAME(PyAPIInitialized())) {
        throw(MAL, "pyapi.eval",
              "Embedded Python is enabled but an error was thrown during initialization.");
    }
    sqlmorefun = *(sql_subfunc**) getArgReference(stk, pci, pci->retc);
    sqlfun = sqlmorefun->func;
    exprStr = *getArgReference_str(stk, pci, pci->retc + 1);

	args = (str *)GDKzalloc(pci->argc * sizeof(str));
	if (!args) {
		throw(MAL, "pyapi.eval", MAL_MALLOC_FAIL " arguments.");
	}

	// Analyse the SQL_Func structure to get the parameter names
	if (sqlfun != NULL && sqlfun->ops->cnt > 0) {
		unnamedArgs = pci->retc + 2;
		argnode = sqlfun->ops->h;
		while (argnode) {
			char *argname = ((sql_arg *)argnode->data)->name;
			args[unnamedArgs++] = GDKstrdup(argname);
			argnode = argnode->next;
		}
	}

	// We name all the unknown arguments
	for (i = pci->retc + 2; i < argcount; i++) {
		if (!args[i]) {
			char argbuf[64];
			snprintf(argbuf, sizeof(argbuf), "arg%i", i - pci->retc - 1);
			args[i] = GDKstrdup(argbuf);
		}
	}
	gstate = Python_ObtainGIL();

	pArgs = PyTuple_New(argcount - pci->retc - 2 + additional_columns);
	if (!pArgs) {
		msg = createException(MAL, "pyapi.eval_loader",
							  MAL_MALLOC_FAIL "python object");
		goto wrapup;
	}

	ai = 0;
	argnode = sqlfun && sqlfun->ops->cnt > 0 ? sqlfun->ops->h : NULL;
	for (i = pci->retc + 2; i < argcount; i++) {
		PyInput inp;
		PyObject *val = NULL;
		inp.bat = NULL;
		inp.sql_subtype = NULL;

		if (!isaBatType(getArgType(mb, pci, i))) {
			inp.scalar = true;
			inp.bat_type = getArgType(mb, pci, i);
			inp.count = 1;
			if (inp.bat_type == TYPE_str) {
				inp.dataptr = getArgReference_str(stk, pci, i);
			} else {
				inp.dataptr = getArgReference(stk, pci, i);
			}
			val = PyArrayObject_FromScalar(&inp, &msg);
		} else {
			BAT* b = BATdescriptor(*getArgReference_bat(stk, pci, i));
			if (b == NULL) {
				msg = createException(
					MAL, "pyapi.eval",
					"The BAT passed to the function (argument #%d) is NULL.\n",
					i - (pci->retc + 2) + 1);
				goto wrapup;
			}
			inp.scalar = false;
			inp.count = BATcount(b);
			inp.bat_type = ATOMstorage(getBatType(getArgType(mb, pci, i)));
			inp.bat = b;

			val = PyMaskedArray_FromBAT(
				&inp, 0, inp.count, &msg,
				false);
			BBPunfix(inp.bat->batCacheid);
		}
		if (msg != MAL_SUCCEED) {
			goto wrapup;
		}
		if (PyTuple_SetItem(pArgs, ai++, val) != 0) {
			msg =
				createException(MAL, "pyapi.eval_loader",
								"Failed to set tuple (this shouldn't happen).");
			goto wrapup;
		}
		// TODO deal with sql types
	}

	getArg(pci, 0) = TYPE_void;
	if (sqlmorefun->colnames) {
		n = sqlmorefun->colnames->h;
		n2 = sqlmorefun->coltypes->h;
		ncols = pyapi_list_length(sqlmorefun->colnames);
		if (ncols == 0) {
			msg = createException(MAL, "pyapi.eval_loader",
								  "No columns supplied.");
			goto wrapup;
		}
		cols = GDKzalloc(sizeof(sql_emit_col) * ncols);
		if (!cols) {
			msg = createException(MAL, "pyapi.eval_loader",
								  MAL_MALLOC_FAIL "column list");
			goto wrapup;
		}
		assert(pyapi_list_length(sqlmorefun->colnames) == pyapi_list_length(sqlmorefun->coltypes));
		i = 0;
		while (n) {
			sql_subtype* tpe = (sql_subtype*) n2->data;
			cols[i].name = GDKstrdup(*((char **)n->data));
			n = n->next;
			cols[i].b =
				COLnew(0, tpe->type->localtype, 0, TRANSIENT);
			n2 = n2->next;
			cols[i].b->tnil = 0;
			cols[i].b->tnonil = 0;
			i++;
		}
	} else {
		// set the return value to the correct type to prevent MAL layers from
		// complaining
		cols = NULL;
		ncols = 0;
		create_table = true;
	}

	pConnection = Py_Connection_Create(cntxt, 0, 0, 0);
	pEmit = PyEmit_Create(cols, ncols);
	if (!pConnection || !pEmit) {
		msg = createException(MAL, "pyapi.eval_loader",
							  MAL_MALLOC_FAIL "python object");
		goto wrapup;
	}

	PyTuple_SetItem(pArgs, ai++, pEmit);
	PyTuple_SetItem(pArgs, ai++, pConnection);

	pycall = FormatCode(exprStr, args, argcount, 4, &code_object, &msg,
						loader_additional_args, additional_columns);
	if (!pycall && !code_object) {
		if (msg == MAL_SUCCEED) {
			msg = createException(MAL, "pyapi.eval_loader",
								  "Error while parsing Python code.");
		}
		goto wrapup;
	}

	{
		PyObject *pFunc, *pModule, *v, *d, *ret;

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
				msg = PyError_CreateException("Could not parse Python code",
											  pycall);
				goto wrapup;
			}
			Py_DECREF(v);

			// Now we need to obtain a pointer to the function, the function is
			// called "pyfun"
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
		ret = PyObject_CallObject(pFunc, pArgs);

		if (PyErr_Occurred()) {
			Py_DECREF(pFunc);
			msg = PyError_CreateException("Python exception", pycall);
			if (code_object == NULL) {
				PyRun_SimpleString("del pyfun");
			}
			goto wrapup;
		}

		if (ret != Py_None) {
			if (PyEmit_Emit((PyEmitObject *)pEmit, ret) == NULL) {
				Py_DECREF(pFunc);
				msg = PyError_CreateException("Python exception", pycall);
				goto wrapup;
			}
		}

		cols = ((PyEmitObject *)pEmit)->cols;
		nval = ((PyEmitObject *)pEmit)->nvals;
		ncols = (int)((PyEmitObject *)pEmit)->ncols;
		Py_DECREF(pFunc);
		Py_DECREF(pArgs);
		pArgs = NULL;

		if (ncols == 0) {
			msg = createException(MAL, "pyapi.eval_loader",
								  "No elements emitted by the loader.");
			goto wrapup;
		}
	}

	gstate = Python_ReleaseGIL(gstate);

	for (i = 0; i < ncols; i++) {
		BAT *b = cols[i].b;
		BATsetcount(b, nval);
		b->tkey = 0;
		b->tsorted = 0;
		b->trevsorted = 0;
	}
	if (!create_table) {
		msg = _connection_append_to_table(cntxt, sqlmorefun->sname,
									   sqlmorefun->tname, cols, ncols);
		goto wrapup;
	} else {
		msg = _connection_create_table(cntxt, sqlmorefun->sname,
									   sqlmorefun->tname, cols, ncols);
		goto wrapup;
	}

wrapup:
	if (cols) {
		for (i = 0; i < ncols; i++) {
			if (cols[i].b) {
				BBPunfix(cols[i].b->batCacheid);
			}
			if (cols[i].name) {
				GDKfree(cols[i].name);
			}
		}
		GDKfree(cols);
	}
	if (gstate) {
		if (pArgs) {
			Py_DECREF(pArgs);
		}
		gstate = Python_ReleaseGIL(gstate);
	}
	if (pycall)
		GDKfree(pycall);
	if (args) {
		for (i = pci->retc + 2; i < argcount; i++) {
			if (args[i]) {
				GDKfree(args[i]);
			}
		}
		GDKfree(args);
	}
	return (msg);
}
