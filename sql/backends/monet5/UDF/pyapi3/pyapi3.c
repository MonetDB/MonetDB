/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "pyapi.h"
#include "connection.h"
#include "mutils.h"

#include "unicode.h"
#include "pytypes.h"
#include "type_conversion.h"
#include "formatinput.h"
#include "conversion.h"

static PyObject *marshal_module = NULL;
PyObject *marshal_loads = NULL;

static str CreateEmptyReturn(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
							  size_t retcols, oid seqbase);

static const char *FunctionBasePath(char *buf, size_t len)
{
	const char *basepath = GDKgetenv("function_basepath");
#ifdef NATIVE_WIN32
	if (basepath == NULL) {
		const wchar_t *home = _wgetenv(L"HOME");
		if (home) {
			char *path = wchartoutf8(home);
			if (path) {
				strcpy_len(buf, path, len);
				free(path);
				basepath = buf;
			}
		}
	}
#else
	/* not used except on Windows */
	(void) buf;
	(void) len;
	if (basepath == NULL) {
		basepath = getenv("HOME");
	}
#endif
	if (basepath == NULL) {
		basepath = "";
	}
	return basepath;
}

static MT_Lock pyapiLock = MT_LOCK_INITIALIZER(pyapiLock);
static bool pyapiInitialized = false;
static PyDateTime_CAPI *PYAPI3_DateTimeAPI;

bool PYAPI3PyAPIInitialized(void) {
	return pyapiInitialized;
}

PyDateTime_CAPI *get_DateTimeAPI(void) {
	return PYAPI3_DateTimeAPI;
}

void init_DateTimeAPI(void) {
	PyDateTime_IMPORT;
	PYAPI3_DateTimeAPI = PyDateTimeAPI;
}

#ifdef WIN32
static bool enable_zerocopy_input = true;
static bool enable_zerocopy_output = false;
#else
static bool enable_zerocopy_input = true;
static bool enable_zerocopy_output = true;
#endif

static str
PyAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool grouped);

str
PYAPI3PyAPIevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return PyAPIeval(cntxt, mb, stk, pci, false);
}

str
PYAPI3PyAPIevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return PyAPIeval(cntxt, mb, stk, pci, true);
}

#define NP_SPLIT_BAT(tpe)                                                      \
	{                                                                          \
		tpe ***ptr = (tpe ***)split_bats;                                      \
		size_t *temp_indices;                                                  \
		tpe *batcontent = (tpe *)basevals;                                     \
		/* allocate space for split BAT */                                     \
		for (group_it = 0; group_it < group_count; group_it++) {               \
			ptr[group_it][i] =                                                 \
				GDKzalloc(group_counts[group_it] * sizeof(tpe));               \
		}                                                                      \
		/*iterate over the elements of the current BAT*/                       \
		temp_indices = GDKzalloc(sizeof(lng) * group_count);                   \
		if (BATtvoid(aggr_group)) {                                            \
			for (element_it = 0; element_it < elements; element_it++) {        \
				/*append current element to proper group*/                     \
				ptr[element_it][i][temp_indices[element_it]++] =               \
					batcontent[element_it];                                    \
			}                                                                  \
		} else {                                                               \
			for (element_it = 0; element_it < elements; element_it++) {        \
				/*group of current element*/                                   \
				oid group = aggr_group_arr[element_it];                        \
				/*append current element to proper group*/                     \
				ptr[group][i][temp_indices[group]++] = batcontent[element_it]; \
			}                                                                  \
		}                                                                      \
		GDKfree(temp_indices);                                                 \
	}

//! The main PyAPI function, this function does everything PyAPI related
//! It takes as argument a bunch of input BATs, a python function, and outputs a
//! number of BATs
//! This function follows the following pipeline
//! [PARSE_CODE] Step 1: It parses the Python source code and corrects any wrong
//! indentation, or converts the source code into a PyCodeObject if the source
//! code is encoded as such
//! [CONVERT_BAT] Step 2: It converts the input BATs into Numpy Arrays
//! [EXECUTE_CODE] Step 3: It executes the Python code using the Numpy arrays as arguments
//! [RETURN_VALUES] Step 4: It collects the return values and converts them back into BATs
static str PyAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool grouped) {
	sql_func * sqlfun = NULL;
	str exprStr = NULL;

	const int additional_columns = 3;
	int i = 1, ai = 0;
	char *pycall = NULL;
	str *args;
	char *msg = MAL_SUCCEED;
	BAT *b = NULL;
	node *argnode = NULL;
	int seengrp = FALSE;
	PyObject *pArgs = NULL, *pColumns = NULL, *pColumnTypes = NULL,
			 *pConnection,
			 *pResult = NULL; // this is going to be the parameter tuple
	PyObject *code_object = NULL;
	PyReturn *pyreturn_values = NULL;
	PyInput *pyinput_values = NULL;
	oid seqbase = 0;
	bit varres;
	int retcols;
	bool gstate = 0;
	int unnamedArgs = 0;
	bool freeexprStr = false;
	int argcount = pci->argc;

	char *eval_additional_args[] = {"_columns", "_column_types", "_conn"};

	if (!pyapiInitialized) {
		throw(MAL, "pyapi3.eval", SQLSTATE(PY000) "Embedded Python is enabled but an error was "
								 "thrown during initialization.");
	}

	// If the first input argument is of type lng, this is a cardinality-only bulk operation.
	int has_card_arg = 0;
	BUN card; // cardinality of non-bat inputs
	if (getArgType(mb, pci, pci->retc) == TYPE_lng) {
		has_card_arg=1;
		card = (BUN) *getArgReference_lng(stk, pci, pci->retc);
	} else {
		has_card_arg=0;
		card = 1;
	}

	sqlfun = *(sql_func **)getArgReference(stk, pci, pci->retc + has_card_arg);
	exprStr = *getArgReference_str(stk, pci, pci->retc + 1 + has_card_arg);
	varres = sqlfun ? sqlfun->varres : 0;
	retcols = !varres ? pci->retc : -1;

	args = (str *)GDKzalloc(pci->argc * sizeof(str));
	pyreturn_values = GDKzalloc(pci->retc * sizeof(PyReturn));
	if (args == NULL || pyreturn_values == NULL) {
		throw(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL " arguments.");
	}

	if ((pci->argc - (pci->retc + 2 + has_card_arg)) * sizeof(PyInput) > 0) {
		pyinput_values = GDKzalloc((pci->argc - (pci->retc + 2 + has_card_arg)) * sizeof(PyInput));

		if (pyinput_values == NULL) {
			GDKfree(args);
			GDKfree(pyreturn_values);
			throw(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL " input values.");
		}
	}

	// Analyse the SQL_Func structure to get the parameter names
	if (sqlfun != NULL && sqlfun->ops->cnt > 0) {
		unnamedArgs = pci->retc + 2 + has_card_arg;
		argnode = sqlfun->ops->h;
		while (argnode) {
			char *argname = ((sql_arg *)argnode->data)->name;
			args[unnamedArgs++] = GDKstrdup(argname);
			argnode = argnode->next;
		}
	}

	// We name all the unknown arguments, if grouping is enabled the first
	// unknown argument that is the group variable, we name this 'aggr_group'
	for (i = pci->retc + 2 + has_card_arg; i < argcount; i++) {
		if (args[i] == NULL) {
			if (!seengrp && grouped) {
				args[i] = GDKstrdup("aggr_group");
				seengrp = TRUE;
			} else {
				char argbuf[64];
				snprintf(argbuf, sizeof(argbuf), "arg%i", i - pci->retc - (1 + has_card_arg));
				args[i] = GDKstrdup(argbuf);
			}
		}
	}

	// Construct PyInput objects
	argnode = sqlfun && sqlfun->ops->cnt > 0 ? sqlfun->ops->h : NULL;
	for (i = pci->retc + 2 + has_card_arg; i < argcount; i++) {
		PyInput *inp = &pyinput_values[i - (pci->retc + 2 + has_card_arg)];
		if (!isaBatType(getArgType(mb, pci, i))) {
			inp->scalar = true;
			inp->bat_type = getArgType(mb, pci, i);
			inp->count = 1;

			if (!has_card_arg) {
				if (inp->bat_type == TYPE_str) {
					inp->dataptr = getArgReference_str(stk, pci, i);
				} else {
					inp->dataptr = getArgReference(stk, pci, i);
				}
			}
			else {
				const ValRecord *v = &stk->stk[getArg(pci, i)];
				b = BATconstant(0, v->vtype, VALptr(v), card, TRANSIENT);
				if (b == NULL) {
					msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto wrapup;
				}
				inp->count = BATcount(b);
				inp->bat_type = b->ttype;
				inp->bat = b;
			}
		} else {
			b = BATdescriptor(*getArgReference_bat(stk, pci, i));
			if (b == NULL) {
				msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto wrapup;
			}
			seqbase = b->hseqbase;
			inp->count = BATcount(b);
			inp->bat_type = b->ttype;
			inp->bat = b;
			if (inp->count == 0) {
				// one of the input BATs is empty, don't execute the function at
				// all
				// just return empty BATs
				msg = CreateEmptyReturn(mb, stk, pci, retcols, seqbase);
				goto wrapup;
			}
		}
		if (argnode) {
			inp->sql_subtype = &((sql_arg *)argnode->data)->type;
			argnode = argnode->next;
		}
	}

	// After this point we will execute Python Code, so we need to acquire the
	// GIL
	gstate = Python_ObtainGIL();

	if (sqlfun) {
		// Check if exprStr references to a file path or if it contains the
		// Python code itself
		// There is no easy way to check, so the rule is if it starts with '/'
		// it is always a file path,
		// Otherwise it's a (relative) file path only if it ends with '.py'
		size_t length = strlen(exprStr);
		if (exprStr[0] == '/' ||
			(exprStr[length - 3] == '.' && exprStr[length - 2] == 'p' &&
			 exprStr[length - 1] == 'y')) {
			FILE *fp;
			char address[1000];
			struct stat buffer;
			ssize_t length;
			if (exprStr[0] == '/') {
				// absolute path
				snprintf(address, 1000, "%s", exprStr);
			} else {
				// relative path
				snprintf(address, 1000, "%s/%s", FunctionBasePath((char[256]){0}, 256), exprStr);
			}
			if (MT_stat(address, &buffer) < 0) {
				msg = createException(
					MAL, "pyapi3.eval",
					SQLSTATE(PY000) "Could not find Python source file \"%s\".", address);
				goto wrapup;
			}
			fp = fopen(address, "r");
			if (fp == NULL) {
				msg = createException(
					MAL, "pyapi3.eval",
					SQLSTATE(PY000) "Could not open Python source file \"%s\".", address);
				goto wrapup;
			}
			if(fseek(fp, 0, SEEK_END) == -1) {
				msg = createException(
						MAL, "pyapi3.eval",
						SQLSTATE(PY000) "Failed to set file pointer on Python source file \"%s\".", address);
				goto wrapup;
			}
			if((length = ftell(fp)) == -1) {
				msg = createException(
						MAL, "pyapi3.eval",
						SQLSTATE(PY000) "Failed to set file pointer on Python source file \"%s\".", address);
				goto wrapup;
			}
			if(fseek(fp, 0, SEEK_SET) == -1) {
				msg = createException(
						MAL, "pyapi3.eval",
						SQLSTATE(PY000) "Failed to set file pointer on Python source file \"%s\".", address);
				goto wrapup;
			}
			exprStr = GDKzalloc(length + 1);
			if (exprStr == NULL) {
				msg = createException(MAL, "pyapi3.eval",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL " function body string.");
				goto wrapup;
			}
			freeexprStr = true;
			if (fread(exprStr, 1, (size_t) length, fp) != (size_t) length) {
				msg = createException(MAL, "pyapi3.eval",
									  SQLSTATE(PY000) "Failed to read from file \"%s\".",
									  address);
				goto wrapup;
			}
			fclose(fp);
		}
	}

	/*[PARSE_CODE]*/
	pycall = FormatCode(exprStr, args, argcount, 4, &code_object, &msg,
						eval_additional_args, additional_columns);
	if (pycall == NULL && code_object == NULL) {
		if (msg == NULL) {
			msg = createException(MAL, "pyapi3.eval",
								  SQLSTATE(PY000) "Error while parsing Python code.");
		}
		goto wrapup;
	}

	/*[CONVERT_BAT]*/
	// Now we will do the input handling (aka converting the input BATs to numpy
	// arrays)
	// We will put the python arrays in a PyTuple object, we will use this
	// PyTuple object as the set of arguments to call the Python function
	pArgs = PyTuple_New(argcount - (pci->retc + 2 + has_card_arg) +
						(code_object == NULL ? additional_columns : 0));
	pColumns = PyDict_New();
	pColumnTypes = PyDict_New();
	pConnection = Py_Connection_Create(cntxt, 0, 0);

	// Now we will loop over the input BATs and convert them to python objects
	for (i = pci->retc + 2 + has_card_arg; i < argcount; i++) {
		PyObject *result_array;
		// t_start and t_end hold the part of the BAT we will convert to a Numpy
		// array, by default these hold the entire BAT [0 - BATcount(b)]
		size_t t_start = 0, t_end = pyinput_values[i - (pci->retc + 2 + has_card_arg)].count;

		// There are two possibilities, either the input is a BAT, or the input
		// is a scalar
		// If the input is a scalar we will convert it to a python scalar
		// If the input is a BAT, we will convert it to a numpy array
		if (pyinput_values[i - (pci->retc + 2 + has_card_arg)].scalar) {
			result_array = PyArrayObject_FromScalar(
				&pyinput_values[i - (pci->retc + 2 + has_card_arg)], &msg);
		} else {
			int type = pyinput_values[i - (pci->retc + 2 + has_card_arg)].bat_type;
			result_array = PyMaskedArray_FromBAT(
				&pyinput_values[i - (pci->retc + 2 + has_card_arg)], t_start, t_end, &msg,
				!enable_zerocopy_input && type != TYPE_void);
		}
		if (result_array == NULL) {
			if (msg == MAL_SUCCEED) {
				msg = createException(MAL, "pyapi3.eval",
									  SQLSTATE(PY000) "Failed to create Numpy Array from BAT.");
			}
			goto wrapup;
		}
		if (code_object == NULL) {
			PyObject *arg_type = PyUnicode_FromString(
				BatType_Format(pyinput_values[i - (pci->retc + 2 + has_card_arg)].bat_type));
			PyDict_SetItemString(pColumns, args[i], result_array);
			PyDict_SetItemString(pColumnTypes, args[i], arg_type);
			Py_DECREF(arg_type);
		}
		pyinput_values[i - (pci->retc + 2 + has_card_arg)].result = result_array;
		PyTuple_SetItem(pArgs, ai++, result_array);
	}
	if (code_object == NULL) {
		PyTuple_SetItem(pArgs, ai++, pColumns);
		PyTuple_SetItem(pArgs, ai++, pColumnTypes);
		PyTuple_SetItem(pArgs, ai++, pConnection);
	}

	/*[EXECUTE_CODE]*/
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
			v = PyRun_StringFlags(pycall, Py_file_input, d, NULL, NULL);
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

		// The function has been successfully created/compiled, all that
		// remains is to actually call the function
		pResult = PyObject_CallObject(pFunc, pArgs);

		Py_DECREF(pFunc);
		Py_DECREF(pArgs);

		if (PyErr_Occurred()) {
			msg = PyError_CreateException("Python exception", pycall);
			if (code_object == NULL) {
				PyRun_SimpleString("del pyfun");
			}
			goto wrapup;
		}

		// if (code_object == NULL) { PyRun_SimpleString("del pyfun"); }

		if (PyDict_Check(pResult)) { // Handle dictionary returns
			// For dictionary returns we need to map each of the (key,value)
			// pairs to the proper return value
			// We first analyze the SQL Function structure for a list of return
			// value names
			char **retnames = NULL;
			if (!varres) {
				if (sqlfun != NULL) {
					retnames = GDKzalloc(sizeof(char *) * sqlfun->res->cnt);
					argnode = sqlfun->res->h;
					for (i = 0; i < sqlfun->res->cnt; i++) {
						retnames[i] = ((sql_arg *)argnode->data)->name;
						argnode = argnode->next;
					}
				} else {
					msg = createException(MAL, "pyapi3.eval",
										  SQLSTATE(PY000) "Return value is a dictionary, but "
										  "there is no sql function object, so "
										  "we don't know the return value "
										  "names and mapping cannot be done.");
					goto wrapup;
				}
			} else {
				// If there are a variable number of return types, we take the
				// column names from the dictionary
				PyObject *keys = PyDict_Keys(pResult);
				retcols = (int)PyList_Size(keys);
				retnames = GDKzalloc(sizeof(char *) * retcols);
				for (i = 0; i < retcols; i++) {
					PyObject *colname = PyList_GetItem(keys, i);
					if (!PyUnicode_CheckExact(colname)) {
						msg = createException(MAL, "pyapi3.eval",
											  SQLSTATE(PY000) "Expected a string key in the "
											  "dictionary, but received an "
											  "object of type %s",
											  colname->ob_type->tp_name);
						goto wrapup;
					}
					retnames[i] = (char *) PyUnicode_AsUTF8(colname);
				}
				Py_DECREF(keys);
			}
			pResult = PyDict_CheckForConversion(pResult, retcols, retnames, &msg);
			if (retnames != NULL)
				GDKfree(retnames);
		} else if (varres) {
			msg = createException(MAL, "pyapi3.eval",
								  SQLSTATE(PY000) "Expected a variable number return values, "
								  "but the return type was not a dictionary. "
								  "We require the return type to be a "
								  "dictionary for column naming purposes.");
			goto wrapup;
		} else {
			// Now we need to do some error checking on the result object,
			// because the result object has to have the correct type/size
			// We will also do some converting of result objects to a common
			// type (such as scalar -> [[scalar]])
			pResult = PyObject_CheckForConversion(pResult, retcols, NULL, &msg);
		}
		if (pResult == NULL) {
			goto wrapup;
		}
	}

	if (varres) {
		GDKfree(pyreturn_values);
		pyreturn_values = GDKzalloc(retcols * sizeof(PyReturn));
	}

	// Now we have executed the Python function, we have to collect the return
	// values and convert them to BATs
	// We will first collect header information about the Python return objects
	// and extract the underlying C arrays
	// We will store this header information in a PyReturn object

	// The reason we are doing this as a separate step is because this
	// preprocessing requires us to call the Python API
	// Whereas the actual returning does not require us to call the Python API
	// This means we can do the actual returning without holding the GIL
	if (!PyObject_PreprocessObject(pResult, pyreturn_values, retcols, &msg)) {
		goto wrapup;
	}

	// We are done executing Python code (aside from cleanup), so we can release
	// the GIL
	gstate = Python_ReleaseGIL(gstate);

	/*[RETURN_VALUES]*/
	argnode = sqlfun && sqlfun->res ? sqlfun->res->h : NULL;
	for (i = 0; i < retcols; i++) {
		PyReturn *ret = &pyreturn_values[i];
		int bat_type = TYPE_any;
		sql_subtype *sql_subtype = argnode ? &((sql_arg *)argnode->data)->type : NULL;
		if (!varres) {
			bat_type = getBatType(getArgType(mb, pci, i));

			if (bat_type == TYPE_any || bat_type == TYPE_void) {
				bat_type = PyType_ToBat(ret->result_type);
				getArgType(mb, pci, i) = bat_type;
			}
		} else {
			bat_type = PyType_ToBat(ret->result_type);
		}

		b = PyObject_ConvertToBAT(ret, sql_subtype, bat_type, i, seqbase, &msg,
								  !enable_zerocopy_output);
		if (b == NULL) {
			goto wrapup;
		}

		msg = MAL_SUCCEED;
		if (isaBatType(getArgType(mb, pci, i))) {
			*getArgReference_bat(stk, pci, i) = b->batCacheid;
			BBPkeepref(b);
		} else { // single value return, only for non-grouped aggregations
			BATiter li = bat_iterator(b);
			if (bat_type != TYPE_str) {
				if (VALinit(&stk->stk[pci->argv[i]], bat_type, li.base) ==
					NULL)
					msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			} else {
				if (VALinit(&stk->stk[pci->argv[i]], bat_type,
							BUNtail(li, 0)) == NULL)
					msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			bat_iterator_end(&li);
			BBPunfix(b->batCacheid);
			b = NULL;
			if (msg != MAL_SUCCEED)
				goto wrapup;
		}
		if (argnode) {
			argnode = argnode->next;
		}
	}
wrapup:

	// Actual cleanup
	// Cleanup input BATs
	for (i = pci->retc + 2 + has_card_arg; i < pci->argc; i++) {
		PyInput *inp = &pyinput_values[i - (pci->retc + 2 + has_card_arg)];
		BBPreclaim(inp->bat);
		BBPreclaim(inp->conv_bat); /* delayed free */
	}
	if (pResult != NULL && gstate == 0) {
		// if there is a pResult here, we are running single threaded (LANGUAGE
		// PYTHON),
		// thus we need to free python objects, thus we need to obtain the GIL
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
		if (args[i])
			GDKfree(args[i]);
	GDKfree(args);
	GDKfree(pycall);
	if (freeexprStr)
		GDKfree(exprStr);

	return msg;
}

#ifdef _MSC_VER
#define wcsdup _wcsdup
#endif
static str
PYAPI3PyAPIprelude(void) {
	MT_lock_set(&pyapiLock);
	if (!pyapiInitialized) {
		wchar_t* program = L"mserver5";
		wchar_t* argv[] = { program, NULL };
		str msg = MAL_SUCCEED;
		PyObject *tmp;

		static_assert(PY_MAJOR_VERSION == 3, "Python 3.X required");
#if PY_MINOR_VERSION >= 11
		/* introduced in 3.8, we use it for 3.11 and later
		 * on Windows, this code does not work with 3.10, it needs more
		 * complex initialization */
		PyStatus status;
		PyConfig config;

		PyConfig_InitIsolatedConfig(&config);
		status = PyConfig_SetArgv(&config, 1, argv);
		if (!PyStatus_Exception(status))
			status = PyConfig_Read(&config);
		if (!PyStatus_Exception(status))
			status = Py_InitializeFromConfig(&config);
		PyConfig_Clear(&config);
		if (PyStatus_Exception(status)) {
			MT_lock_unset(&pyapiLock);
			throw(MAL, "pyapi3.eval",
				  SQLSTATE(PY000) "Python initialization failed: %s: %s",
				  status.func ? status.func : "PYAPI3PyAPIprelude",
				  status.err_msg ? status.err_msg : "");
		}
#else
		/* PySys_SetArgvEx deprecated in 3.11 */
		Py_InitializeEx(0);
		PySys_SetArgvEx(1, argv, 0);
#endif

		_import_array();
		msg = _connection_init();
		if (msg != MAL_SUCCEED) {
			MT_lock_unset(&pyapiLock);
			return msg;
		}
		msg = _conversion_init();
		if (msg != MAL_SUCCEED) {
			MT_lock_unset(&pyapiLock);
			return msg;
		}
		_pytypes_init();
		_loader_init();
		tmp = PyUnicode_FromString("marshal");
		marshal_module = PyImport_Import(tmp);
		init_DateTimeAPI();
		Py_DECREF(tmp);
		if (marshal_module == NULL) {
			MT_lock_unset(&pyapiLock);
			return createException(MAL, "pyapi3.eval", SQLSTATE(PY000) "Failed to load Marshal module.");
		}
		marshal_loads = PyObject_GetAttrString(marshal_module, "loads");
		if (marshal_loads == NULL) {
			MT_lock_unset(&pyapiLock);
			return createException(MAL, "pyapi3.eval", SQLSTATE(PY000) "Failed to load function \"loads\" from Marshal module.");
		}
		if (PyRun_SimpleString("import numpy") != 0) {
			msg = PyError_CreateException("Failed to initialize embedded python", NULL);
			MT_lock_unset(&pyapiLock);
			return msg;
		}
		PyEval_SaveThread();
		if (msg != MAL_SUCCEED) {
			MT_lock_unset(&pyapiLock);
			return msg;
		}
		pyapiInitialized = true;
		fprintf(stdout, "# MonetDB/Python%d module loaded\n", 3);
	}
	MT_lock_unset(&pyapiLock);
	return MAL_SUCCEED;
}

char *PyError_CreateException(char *error_text, char *pycall)
{
	PyObject *py_error_type = NULL, *py_error_value = NULL,
			 *py_error_traceback = NULL;
	const char *py_error_string = NULL;
	lng line_number = -1;

	PyErr_Fetch(&py_error_type, &py_error_value, &py_error_traceback);
	if (py_error_value) {
		PyObject *error;
		PyErr_NormalizeException(&py_error_type, &py_error_value,
								 &py_error_traceback);
		error = PyObject_Str(py_error_value);

		py_error_string = PyUnicode_AsUTF8(error);
		Py_XDECREF(error);
		if (pycall != NULL && strlen(pycall) > 0) {
			if (py_error_traceback == NULL) {
				// no traceback info, this means we are dealing with a parsing
				// error
				// line information should be in the error message
				sscanf(py_error_string, "%*[^0-9]" LLSCN, &line_number);
				if (line_number < 0)
					goto finally;
			} else {
				line_number =
					((PyTracebackObject *)py_error_traceback)->tb_lineno;
			}

			// Now only display the line numbers around the error message, we
			// display 5 lines around the error message
			{
				char linenr[32];
				size_t nrpos, pos, i, j;
				char lineinformation[5000]; // we only support 5000 characters
											// for 5 lines of the program,
											// should be enough
				nrpos = 0; // Current line number
				pos = 0; // Current position in the lineinformation result array
				for (i = 0; i < strlen(pycall); i++) {
					if (pycall[i] == '\n' || i == 0) {
						// Check if we have arrived at a new line, if we have
						// increment the line count
						nrpos++;
						// Now check if we should display this line
						if (nrpos >= ((size_t)line_number - 2) &&
							nrpos <= ((size_t)line_number + 2) && pos < 4997) {
							// We shouldn't put a newline on the first line we
							// encounter, only on subsequent lines
							if (nrpos > ((size_t)line_number - 2))
								lineinformation[pos++] = '\n';
							if ((size_t)line_number == nrpos) {
								// If this line is the 'error' line, add an
								// arrow before it, otherwise just add spaces
								lineinformation[pos++] = '>';
								lineinformation[pos++] = ' ';
							} else {
								lineinformation[pos++] = ' ';
								lineinformation[pos++] = ' ';
							}
							snprintf(linenr, 32, "%zu", nrpos);
							for (j = 0; j < strlen(linenr); j++) {
								lineinformation[pos++] = linenr[j];
							}
							lineinformation[pos++] = '.';
							lineinformation[pos++] = ' ';
						}
					}
					if (pycall[i] != '\n' && nrpos >= (size_t)line_number - 2 &&
						nrpos <= (size_t)line_number + 2 && pos < 4999) {
						// If we are on a line number that we have to display,
						// copy the text from this line for display
						lineinformation[pos++] = pycall[i];
					}
				}
				lineinformation[pos] = '\0';
				return createException(MAL, "pyapi3.eval",  SQLSTATE(PY000) "%s\n%s\n%s",
									   error_text, lineinformation,
									   py_error_string);
			}
		}
	} else {
		py_error_string = "";
	}
finally:
	if (pycall == NULL)
		return createException(MAL, "pyapi3.eval", SQLSTATE(PY000) "%s\n%s", error_text,
							   py_error_string);
	return createException(MAL, "pyapi3.eval", SQLSTATE(PY000) "%s\n%s\n%s", error_text, pycall,
						   py_error_string);
}

static str CreateEmptyReturn(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
							 size_t retcols, oid seqbase)
{
	str msg = MAL_SUCCEED;
	void **res = GDKzalloc(retcols * sizeof(void*));

	if (!res) {
		msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}

	for (size_t i = 0; i < retcols; i++) {
		if (isaBatType(getArgType(mb, pci, i))) {
			BAT *b = COLnew(seqbase, getBatType(getArgType(mb, pci, i)), 0, TRANSIENT);
			if (!b) {
				msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
				goto bailout;
			}
			((BAT**)res)[i] = b;
		} else { // single value return, only for non-grouped aggregations
			// return NULL to conform to SQL aggregates
			int tpe = getArgType(mb, pci, i);
			if (!VALinit(&stk->stk[pci->argv[i]], tpe, ATOMnilptr(tpe))) {
				msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
				goto bailout;
			}
			((ValPtr*)res)[i] = &stk->stk[pci->argv[i]];
		}
	}

bailout:
	if (res) {
		for (size_t i = 0; i < retcols; i++) {
			if (isaBatType(getArgType(mb, pci, i))) {
				BAT *b = ((BAT**)res)[i];

				if (b && msg) {
					BBPreclaim(b);
				} else if (b) {
					*getArgReference_bat(stk, pci, i) = b->batCacheid;
					BBPkeepref(b);
				}
			} else if (msg) {
				ValPtr pt = ((ValPtr*)res)[i];

				if (pt)
					VALclear(pt);
			}
		}
		GDKfree(res);
	}
	return msg;
}

static str
PyAPI3prelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
    (void)cntxt; (void)mb; (void)stk; (void)pci;
	return PYAPI3PyAPIprelude();
}

static str
PyAPI3epilogue(void *ret)
{
    (void)ret;
	MT_lock_set(&pyapiLock);
	if (pyapiInitialized) {
		PyGILState_STATE gstate;
		gstate = PyGILState_Ensure();

		/* now exit/cleanup */
		if (0) Py_FinalizeEx();
		(void)gstate;
	}
	MT_lock_unset(&pyapiLock);
    return MAL_SUCCEED;
}

#include "mel.h"
static mel_func pyapi3_init_funcs[] = {
 pattern("pyapi3", "eval", PYAPI3PyAPIevalStd, true, "Execute a simple Python script returning a single value", args(1,3, argany("",1),arg("fptr",ptr),arg("expr",str))),
 pattern("pyapi3", "eval", PYAPI3PyAPIevalStd, true, "Execute a simple Python script value", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("pyapi3", "subeval_aggr", PYAPI3PyAPIevalAggr, true, "grouped aggregates through Python", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("pyapi3", "eval_aggr", PYAPI3PyAPIevalAggr, true, "grouped aggregates through Python", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("pyapi3", "eval_loader", PYAPI3PyAPIevalLoader, true, "loader functions through Python", args(1,3, varargany("",0),arg("fptr",ptr),arg("expr",str))),
 pattern("pyapi3", "eval_loader", PYAPI3PyAPIevalLoader, true, "loader functions through Python", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("batpyapi3", "eval", PYAPI3PyAPIevalStd, true, "Execute a simple Python script value", args(1,4, batvarargany("",0),arg("fptr", ptr), arg("expr",str),varargany("arg",0))),
 pattern("batpyapi3", "eval", PYAPI3PyAPIevalStd, true, "Execute a simple Python script value", args(1,4, batargany("",1),arg("card", lng), arg("fptr",ptr),arg("expr",str))),
 pattern("batpyapi3", "subeval_aggr", PYAPI3PyAPIevalAggr, true, "grouped aggregates through Python", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("batpyapi3", "eval_aggr", PYAPI3PyAPIevalAggr, true, "grouped aggregates through Python", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("batpyapi3", "eval_loader", PYAPI3PyAPIevalLoader, true, "loader functions through Python", args(1,3, varargany("",0),arg("fptr",ptr),arg("expr",str))),
 pattern("batpyapi3", "eval_loader", PYAPI3PyAPIevalLoader, true, "loader functions through Python", args(1,4, varargany("",0),arg("fptr",ptr),arg("expr",str),varargany("arg",0))),
 pattern("pyapi3", "prelude", PyAPI3prelude, false, "", noargs),
 command("pyapi3", "epilogue", PyAPI3epilogue, false, "", noargs),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pyapi3_mal)
{ mal_module("pyapi3", NULL, pyapi3_init_funcs); }
