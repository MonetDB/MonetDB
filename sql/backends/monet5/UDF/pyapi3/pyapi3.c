/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "pyapi.h"
#include "connection.h"

#include "unicode.h"
#include "pytypes.h"
#include "type_conversion.h"
#include "formatinput.h"
#include "conversion.h"
#include "gdk_interprocess.h"

#ifdef HAVE_FORK
// These libraries are used for PYTHON_MAP when forking is enabled [to start new
// processes and wait on them]
#include <sys/types.h>
#include <sys/wait.h>
#endif

const char *fork_disableflag = "disable_fork";
bool option_disable_fork = false;

static PyObject *marshal_module = NULL;
PyObject *marshal_loads = NULL;

typedef struct _AggrParams{
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
} AggrParams;

static void ComputeParallelAggregation(AggrParams *p);
static void CreateEmptyReturn(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
							  size_t retcols, oid seqbase);

static const char *FunctionBasePath(void)
{
	const char *basepath = GDKgetenv("function_basepath");
	if (basepath == NULL) {
		basepath = getenv("HOME");
	}
	if (basepath == NULL) {
		basepath = "";
	}
	return basepath;
}

static MT_Lock pyapiLock = MT_LOCK_INITIALIZER("pyapiLock");
static bool pyapiInitialized = false;

bool PYFUNCNAME(PyAPIInitialized)(void) {
	return pyapiInitialized;
}

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

static str
PyAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool grouped, bool mapped);

str
PYFUNCNAME(PyAPIevalStd)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return PyAPIeval(cntxt, mb, stk, pci, false, false);
}

str
PYFUNCNAME(PyAPIevalStdMap)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return PyAPIeval(cntxt, mb, stk, pci, false, true);
}

str
PYFUNCNAME(PyAPIevalAggr)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return PyAPIeval(cntxt, mb, stk, pci, true, false);
}

str
PYFUNCNAME(PyAPIevalAggrMap)(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return PyAPIeval(cntxt, mb, stk, pci, true, true);
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
		for (element_it = 0; element_it < elements; element_it++) {            \
			/*group of current element*/                                       \
			oid group = aggr_group_arr[element_it];                            \
			/*append current element to proper group*/                         \
			ptr[group][i][temp_indices[group]++] = batcontent[element_it];     \
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
//! If 'mapped' is set to True, it will fork a separate process at [FORK_PROCESS] that executes Step 1-3, the process will then write the return values into memory mapped files and exit, then Step 4 is executed by the main process
static str PyAPIeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bool grouped, bool mapped) {
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
	bool allow_loopback = !mapped;
	bit varres;
	int retcols;
	bool gstate = 0;
	int unnamedArgs = 0;
	bool parallel_aggregation = grouped && mapped;
	int argcount = pci->argc;

	char *eval_additional_args[] = {"_columns", "_column_types", "_conn"};

	mapped = false;

#ifndef HAVE_FORK
	(void)mapped;
#endif

	if (!pyapiInitialized) {
		throw(MAL, "pyapi3.eval", SQLSTATE(PY000) "Embedded Python is enabled but an error was "
								 "thrown during initialization.");
	}
	if (!grouped) {
		sql_subfunc *sqlmorefun =
			(*(sql_subfunc **)getArgReference(stk, pci, pci->retc));
		if (sqlmorefun) {
			sqlfun = sqlmorefun->func;
		}
	} else {
		sqlfun = *(sql_func **)getArgReference(stk, pci, pci->retc);
	}
	exprStr = *getArgReference_str(stk, pci, pci->retc + 1);
	varres = sqlfun ? sqlfun->varres : 0;
	retcols = !varres ? pci->retc : -1;

	args = (str *)GDKzalloc(pci->argc * sizeof(str));
	pyreturn_values = GDKzalloc(pci->retc * sizeof(PyReturn));
	if (args == NULL || pyreturn_values == NULL) {
		throw(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL " arguments.");
	}

	if ((pci->argc - (pci->retc + 2)) * sizeof(PyInput) > 0) {
		pyinput_values =
			GDKzalloc((pci->argc - (pci->retc + 2)) * sizeof(PyInput));

		if (pyinput_values == NULL) {
			GDKfree(args);
			GDKfree(pyreturn_values);
			throw(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL " input values.");
		}
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
		if (parallel_aggregation && unnamedArgs < pci->argc) {
			argcount = unnamedArgs;
		} else {
			parallel_aggregation = false;
		}
	} else {
		parallel_aggregation = false;
	}

	// We name all the unknown arguments, if grouping is enabled the first
	// unknown argument that is the group variable, we name this 'aggr_group'
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

	// Construct PyInput objects, we do this before any multiprocessing because
	// there is some locking going on in there, and locking + forking = bad idea
	// (a thread can fork while another process is in the lock, which means we
	// can get stuck permanently)
	argnode = sqlfun && sqlfun->ops->cnt > 0 ? sqlfun->ops->h : NULL;
	for (i = pci->retc + 2; i < argcount; i++) {
		PyInput *inp = &pyinput_values[i - (pci->retc + 2)];
		if (!isaBatType(getArgType(mb, pci, i))) {
			inp->scalar = true;
			inp->bat_type = getArgType(mb, pci, i);
			inp->count = 1;
			if (inp->bat_type == TYPE_str) {
				inp->dataptr = getArgReference_str(stk, pci, i);
			} else {
				inp->dataptr = getArgReference(stk, pci, i);
			}
		} else {
			b = BATdescriptor(*getArgReference_bat(stk, pci, i));
			if (b == NULL) {
				msg = createException(
					MAL, "pyapi3.eval",
					SQLSTATE(PY000) "The BAT passed to the function (argument #%d) is NULL.\n",
					i - (pci->retc + 2) + 1);
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
				CreateEmptyReturn(mb, stk, pci, retcols, seqbase);
				goto wrapup;
			}
		}
		if (argnode) {
			inp->sql_subtype = &((sql_arg *)argnode->data)->type;
			argnode = argnode->next;
		}
	}

#ifdef HAVE_FORK
	if (!option_disable_fork) {
		if (!mapped && !parallel_aggregation) {
			MT_lock_set(&pyapiLock);
			if (python_call_active) {
				mapped = true;
				holds_gil = false;
			} else {
				python_call_active = true;
				holds_gil = true;
			}
			MT_lock_unset(&pyapiLock);
		}
	} else {
		mapped = false;
		holds_gil = true;
	}
#endif

#ifdef HAVE_FORK
	/*[FORK_PROCESS]*/
	if (mapped) {
		lng pid;
		// we need 3 + pci->retc * 2 shared memory spaces
		// the first is for the header information
		// the second for query struct information
		// the third is for query results
		// the remaining pci->retc * 2 is one for each return BAT, and one for
		// each return mask array
		int mmap_count = 4 + pci->retc * 2;

		// create initial shared memory
		mmap_id = GDKuniqueid(mmap_count);

		mmap_ptrs = GDKzalloc(mmap_count * sizeof(void *));
		mmap_sizes = GDKzalloc(mmap_count * sizeof(size_t));
		if (mmap_ptrs == NULL || mmap_sizes == NULL) {
			msg = createException(MAL, "pyapi3.eval",
								  SQLSTATE(HY013) MAL_MALLOC_FAIL " mmap values.");
			goto wrapup;
		}

		memory_size =
			pci->retc * sizeof(ReturnBatDescr); // the memory size for the
												// header files, each process
												// has one per return value

		assert(memory_size > 0);
		// create the shared memory for the header
		MT_lock_set(&pyapiLock);
		mmap_ptrs[0] = GDKinitmmap(mmap_id + 0, memory_size, &mmap_sizes[0]);
		MT_lock_unset(&pyapiLock);
		if (mmap_ptrs[0] == NULL) {
			msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
			goto wrapup;
		}
		mmap_ptr = mmap_ptrs[0];

		// create the cross-process semaphore used for signaling queries
		// we need two semaphores
		// the main process waits on the first one (exiting when a query is
		// requested or the child process is done)
		// the forked process waits for the second one when it requests a query
		// (waiting for the result of the query)
		if (GDKcreatesem(mmap_id, 2, &query_sem) != GDK_SUCCEED) {
			msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
			goto wrapup;
		}

		// create the shared memory space for queries
		MT_lock_set(&pyapiLock);
		mmap_ptrs[1] = GDKinitmmap(mmap_id + 1, sizeof(QueryStruct),
						 &mmap_sizes[1]);
		MT_lock_unset(&pyapiLock);
		if (mmap_ptrs[1] == NULL) {
			msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
			goto wrapup;
		}
		query_ptr = mmap_ptrs[1];
		query_ptr->pending_query = false;
		query_ptr->query[0] = '\0';
		query_ptr->mmapid = -1;
		query_ptr->memsize = 0;

		// fork
		MT_lock_set(&pyapiLock);
		gstate = Python_ObtainGIL(); // we need the GIL before forking,
									 // otherwise it can get stuck in the forked
									 // child
		if ((pid = fork()) < 0) {
			msg = createException(MAL, "pyapi3.eval", SQLSTATE(PY000) "Failed to fork process");
			MT_lock_unset(&pyapiLock);

			goto wrapup;
		} else if (pid == 0) {
			child_process = true;
			query_ptr = NULL;
			if ((query_ptr = GDKinitmmap(mmap_id + 1, sizeof(QueryStruct),
										 NULL)) == NULL) {
				msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
				goto wrapup;
			}
		} else {
			gstate = Python_ReleaseGIL(gstate);
		}
		if (!child_process) {
			// main process
			int status;
			bool success = true;
			bool sem_success = false;
			pid_t retcode = 0;

			// release the GIL in the main process
			MT_lock_unset(&pyapiLock);

			while (true) {
				// wait for the child to finish
				// note that we use a timeout here in case the child crashes for
				// some reason
				// in this case the semaphore value is never increased, so we
				// would be stuck otherwise
				if (GDKchangesemval_timeout(query_sem, 0, -1, 100, &sem_success) != GDK_SUCCEED) {
					msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
					goto wrapup;
				}
				if (sem_success) {
					break;
				}
				retcode = waitpid(pid, &status, WNOHANG);
				if (retcode > 0)
					break; // we have successfully waited for the child to exit
				if (retcode < 0) {
					// error message
					const char *err = GDKstrerror(errno, (char[128]){0}, 128);
					sem_success = 0;
					errno = 0;
					msg = createException(
						MAL, "waitpid",
						SQLSTATE(PY000) "Error calling waitpid(" LLFMT ", &status, WNOHANG): %s",
						pid, err);
					break;
				}
			}
			if (sem_success)
				waitpid(pid, &status, 0);

			if (status != 0)
				success = false;

			if (!success) {
				// a child failed, get the error message from the child
				ReturnBatDescr *descr = &(((ReturnBatDescr *)mmap_ptr)[0]);

				if (descr->bat_size == 0) {
					msg = createException(
						MAL, "pyapi3.eval",
						SQLSTATE(PY000) "Failure in child process with unknown error.");
				} else if ((mmap_ptrs[3] = GDKinitmmap(mmap_id + 3, descr->bat_size,
													   &mmap_sizes[3])) != NULL) {
					msg = createException(MAL, "pyapi3.eval", SQLSTATE(PY000) "%s",
										  (char *)mmap_ptrs[3]);
				} else {
					msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
				}
				goto wrapup;
			}

			// collect return values
			for (i = 0; i < pci->retc; i++) {
				PyReturn *ret = &pyreturn_values[i];
				ReturnBatDescr *descr = &(((ReturnBatDescr *)mmap_ptr)[i]);
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

				// get the shared memory address for this return value
				assert(total_size > 0);
				MT_lock_set(&pyapiLock);
				mmap_ptrs[i + 3] = GDKinitmmap(mmap_id + i + 3, total_size,
											   &mmap_sizes[i + 3]);
				MT_lock_unset(&pyapiLock);
				if (mmap_ptrs[i + 3] == NULL) {
					msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
					goto wrapup;
				}
				ret->array_data = mmap_ptrs[i + 3];
				ret->mask_data = NULL;
				ret->numpy_array = NULL;
				ret->numpy_mask = NULL;
				ret->multidimensional = FALSE;
				if (has_mask) {
					size_t mask_size = ret->count * sizeof(bool);

					assert(mask_size > 0);
					MT_lock_set(&pyapiLock);
					mmap_ptrs[pci->retc + (i + 3)] = GDKinitmmap(
						mmap_id + pci->retc + (i + 3), mask_size,
						&mmap_sizes[pci->retc + (i + 3)]);
					MT_lock_unset(&pyapiLock);
					if (mmap_ptrs[pci->retc + (i + 3)] == NULL) {
						msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
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

	// After this point we will execute Python Code, so we need to acquire the
	// GIL
	if (!mapped) {
		gstate = Python_ObtainGIL();
	}

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
				snprintf(address, 1000, "%s/%s", FunctionBasePath(), exprStr);
			}
			if (stat(address, &buffer) < 0) {
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
	pArgs = PyTuple_New(argcount - (pci->retc + 2) +
						(code_object == NULL ? additional_columns : 0));
	pColumns = PyDict_New();
	pColumnTypes = PyDict_New();
#ifdef HAVE_FORK
	pConnection = Py_Connection_Create(cntxt, !allow_loopback, query_ptr, query_sem);
#else
	pConnection = Py_Connection_Create(cntxt, !allow_loopback, 0, 0);
#endif

	// Now we will loop over the input BATs and convert them to python objects
	for (i = pci->retc + 2; i < argcount; i++) {
		PyObject *result_array;
		// t_start and t_end hold the part of the BAT we will convert to a Numpy
		// array, by default these hold the entire BAT [0 - BATcount(b)]
		size_t t_start = 0, t_end = pyinput_values[i - (pci->retc + 2)].count;

		// There are two possibilities, either the input is a BAT, or the input
		// is a scalar
		// If the input is a scalar we will convert it to a python scalar
		// If the input is a BAT, we will convert it to a numpy array
		if (pyinput_values[i - (pci->retc + 2)].scalar) {
			result_array = PyArrayObject_FromScalar(
				&pyinput_values[i - (pci->retc + 2)], &msg);
		} else {
			int type = pyinput_values[i - (pci->retc + 2)].bat_type;
			result_array = PyMaskedArray_FromBAT(
				&pyinput_values[i - (pci->retc + 2)], t_start, t_end, &msg,
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
			PyObject *arg_type = PyString_FromString(
				BatType_Format(pyinput_values[i - (pci->retc + 2)].bat_type));
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

		if (parallel_aggregation) {
			// parallel aggregation, we run the function once for every group in
			// parallel
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
			aggr_group =
				BATdescriptor(*getArgReference_bat(stk, pci, unnamedArgs));
			// the second unnamed argument has the first occurrence of every
			// group number, we just use this to get the total amount of groups
			// quickly
			group_first_occurrence =
				BATdescriptor(*getArgReference_bat(stk, pci, unnamedArgs + 1));
			group_count = BATcount(group_first_occurrence);
			BBPunfix(group_first_occurrence->batCacheid);
			elements = BATcount(aggr_group); // get the amount of groups

			// now we count, for every group, how many elements it has
			group_counts = GDKzalloc(group_count * sizeof(size_t));
			if (group_counts == NULL) {
				msg = createException(MAL, "pyapi3.eval",
									  SQLSTATE(HY013) MAL_MALLOC_FAIL " group count array.");
				goto aggrwrapup;
			}

			aggr_group_arr = (oid *)aggr_group->theap.base;
			for (element_it = 0; element_it < elements; element_it++) {
				group_counts[aggr_group_arr[element_it]]++;
			}

			// now perform the actual splitting of the data, first construct
			// room for splits for every group
			// elements are structured as follows:
			// split_bats [groupnr] [columnnr] [elementnr]
			split_bats = GDKzalloc(group_count * sizeof(void *));
			for (group_it = 0; group_it < group_count; group_it++) {
				split_bats[group_it] =
					GDKzalloc(sizeof(void *) * named_columns);
			}

			// now split the columns one by one
			for (i = 0; i < named_columns; i++) {
				PyInput input = pyinput_values[i];
				void *basevals = input.bat->theap.base;

				if (!input.scalar) {
					switch (input.bat_type) {
						case TYPE_void:
							NP_SPLIT_BAT(oid);
							break;
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
							basevals =
								PyArray_BYTES((PyArrayObject *)input.result);
							NP_SPLIT_BAT(dbl);
							break;
#endif
						case TYPE_str: {
							PyObject ****ptr = (PyObject ****)split_bats;
							size_t *temp_indices;
							PyObject **batcontent = (PyObject **)PyArray_DATA(
								(PyArrayObject *)input.result);
							// allocate space for split BAT
							for (group_it = 0; group_it < group_count;
								 group_it++) {
								ptr[group_it][i] =
									GDKzalloc(group_counts[group_it] *
											  sizeof(PyObject *));
							}
							// iterate over the elements of the current BAT
							temp_indices =
								GDKzalloc(sizeof(PyObject *) * group_count);
							for (element_it = 0; element_it < elements;
								 element_it++) {
								// group of current element
								oid group = aggr_group_arr[element_it];
								// append current element to proper group
								ptr[group][i][temp_indices[group]++] =
									batcontent[element_it];
							}
							GDKfree(temp_indices);
							break;
						}
						default:
							msg = createException(
								MAL, "pyapi3.eval", SQLSTATE(PY000) "Unrecognized BAT type %s",
								BatType_Format(input.bat_type));
							goto aggrwrapup;
							break;
					}
				}
			}

			{
				int res = 0;
				size_t threads = 8; // GDKgetenv("gdk_nr_threads");
				size_t thread_it;
				size_t result_it;
				AggrParams *parameters;
				PyObject **results;
				double current = 0.0;
				double increment;

				// if there are less groups than threads, limit threads to
				// amount of groups
				threads = group_count < threads ? group_count : threads;

				increment = (double)group_count / (double)threads;
				// start running the threads
				parameters = GDKzalloc(threads * sizeof(AggrParams));
				results = GDKzalloc(group_count * sizeof(PyObject *));
				for (thread_it = 0; thread_it < threads; thread_it++) {
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
					res = MT_create_thread(&params->thread,
										   (void (*)(void *)) &
											   ComputeParallelAggregation,
										   params, MT_THR_JOINABLE,
										   "pyapi_par_aggr");
					if (res != 0) {
						msg = createException(MAL, "pyapi3.eval",
											  SQLSTATE(PY000) "Failed to start thread.");
						goto aggrwrapup;
					}
				}
				for (thread_it = 0; thread_it < threads; thread_it++) {
					AggrParams params = parameters[thread_it];
					int res = MT_join_thread(params.thread);
					if (res != 0) {
						msg = createException(MAL, "pyapi3.eval",
											  "Failed to join thread.");
						goto aggrwrapup;
					}
				}

				for (thread_it = 0; thread_it < threads; thread_it++) {
					AggrParams params = parameters[thread_it];
					if (results[thread_it] == NULL || params.msg != NULL) {
						msg = params.msg;
						goto wrapup;
					}
				}

				// we need the GIL again to group the parameters
				gstate = Python_ObtainGIL();

				aggr_result = PyList_New(group_count);
				for (result_it = 0; result_it < group_count; result_it++) {
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
				for (group_it = 0; group_it < group_count; group_it++) {
					if (split_bats[group_it] != NULL) {
						for (i = 0; i < named_columns; i++) {
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
			// The function has been successfully created/compiled, all that
			// remains is to actually call the function
			pResult = PyObject_CallObject(pFunc, pArgs);
		}

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
					if (!PyString_CheckExact(colname)) {
						msg = createException(MAL, "pyapi3.eval",
											  SQLSTATE(PY000) "Expected a string key in the "
											  "dictionary, but received an "
											  "object of type %s",
											  colname->ob_type->tp_name);
						goto wrapup;
					}
#ifndef IS_PY3K
					retnames[i] = ((PyStringObject *)colname)->ob_sval;
#else
					retnames[i] = (char *) PyUnicode_AsUTF8(colname);
#endif
				}
			}
			pResult =
				PyDict_CheckForConversion(pResult, retcols, retnames, &msg);
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

#ifdef HAVE_FORK
	/*[FORKED]*/
	// This is where the child process stops executing
	// We have successfully executed the Python function and converted the
	// result object to a C array
	// Now all that is left is to copy the C array to shared memory so the main
	// process can read it and return it
	if (mapped && child_process) {
		ReturnBatDescr *ptr;

		// First we will fill in the header information, we will need to get a
		// pointer to the header data first
		// The main process has already created the header data for the child
		// process
		if ((mmap_ptrs[0] = GDKinitmmap(mmap_id + 0, memory_size, &mmap_sizes[0])) == NULL) {
			msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
			goto wrapup;
		}

		// Now we will write data about our result (memory size, type, number of
		// elements) to the header
		ptr = (ReturnBatDescr *)mmap_ptrs[0];
		for (i = 0; i < retcols; i++) {
			PyReturn *ret = &pyreturn_values[i];
			ReturnBatDescr *descr = &ptr[i];

			if (ret->result_type == NPY_OBJECT) {
				// We can't deal with NPY_OBJECT arrays, because these are
				// 'arrays of pointers', so we can't just copy the content of
				// the array into shared memory
				// So if we're dealing with a NPY_OBJECT array, we convert them
				// to a Numpy Array of type NPY_<TYPE> that corresponds with the
				// desired BAT type
				// WARNING: Because we could be converting to a NPY_STRING or
				// NPY_UNICODE array (if the desired type is TYPE_str or
				// TYPE_hge), this means that memory usage can explode
				//   because NPY_STRING/NPY_UNICODE arrays are 2D string arrays
				//   with fixed string length (so if there's one very large
				//   string the size explodes quickly)
				//   if someone has some problem with memory size exploding when
				//   using PYTHON_MAP but it being fine in regular PYTHON this
				//   is probably the issue
				PyInput *inp = &pyinput_values[i - (pci->retc + 2)];
				int bat_type = inp->bat_type;
				PyObject *new_array = PyArray_FromAny(
					ret->numpy_array,
					PyArray_DescrFromType(BatType_ToPyType(bat_type)), 1, 1,
					NPY_ARRAY_CARRAY | NPY_ARRAY_FORCECAST, NULL);
				if (new_array == NULL) {
					msg = createException(MAL, "pyapi3.eval",
										  SQLSTATE(PY000) "Could not convert the returned "
										  "NPY_OBJECT array to the desired "
										  "array of type %s.\n",
										  BatType_Format(bat_type));
					goto wrapup;
				}
				Py_DECREF(ret->numpy_array); // do we really care about cleaning
											 // this up, considering this only
											 // happens in a separate process
											 // that will be exited soon anyway?
				ret->numpy_array = new_array;
				ret->result_type =
					PyArray_DESCR((PyArrayObject *)ret->numpy_array)->type_num;
				ret->memory_size =
					PyArray_DESCR((PyArrayObject *)ret->numpy_array)->elsize;
				ret->count = PyArray_DIMS((PyArrayObject *)ret->numpy_array)[0];
				ret->array_data =
					PyArray_DATA((PyArrayObject *)ret->numpy_array);
			}

			descr->npy_type = ret->result_type;
			descr->element_size = ret->memory_size;
			descr->bat_count = ret->count;
			descr->bat_size = ret->memory_size * ret->count;
			descr->has_mask = ret->mask_data != NULL;

			if (ret->count > 0) {
				int memory_size = ret->memory_size * ret->count;
				char *mem_ptr;
				// now create shared memory for the return value and copy the
				// actual values
				assert(memory_size > 0);
				if ((mmap_ptrs[i + 3] = GDKinitmmap(mmap_id + i + 3, memory_size,
													&mmap_sizes[i + 3])) == NULL) {
					msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
					goto wrapup;
				}
				mem_ptr = mmap_ptrs[i + 3];
				assert(mem_ptr);
				memcpy(mem_ptr, PyArray_DATA((PyArrayObject *)ret->numpy_array),
					   memory_size);

				if (descr->has_mask) {
					bool *mask_ptr;
					int mask_size = ret->count * sizeof(bool);
					assert(mask_size > 0);
					// create a memory space for the mask
					if ((mmap_ptrs[retcols + (i + 3)] = GDKinitmmap(
							 mmap_id + retcols + (i + 3), mask_size,
							 &mmap_sizes[retcols + (i + 3)])) == NULL) {
						msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
						goto wrapup;
					}
					mask_ptr = mmap_ptrs[retcols + i + 3];
					assert(mask_ptr);
					memcpy(mask_ptr, ret->mask_data, mask_size);
				}
			}
		}
		// now free the main process from the semaphore
		if (GDKchangesemval(query_sem, 0, 1) != GDK_SUCCEED) {
			msg = createException(MAL, "pyapi3.eval", GDK_EXCEPTION);
			goto wrapup;
		}
		// Exit child process without an error code
		exit(0);
	}
#endif
	// We are done executing Python code (aside from cleanup), so we can release
	// the GIL
	gstate = Python_ReleaseGIL(gstate);

#ifdef HAVE_FORK // This goto is only used for multiprocessing, if HAVE_FORK is
				 // set to 0 this is unused
returnvalues:
#endif
	/*[RETURN_VALUES]*/
	argnode = sqlfun && sqlfun->res ? sqlfun->res->h : NULL;
	for (i = 0; i < retcols; i++) {
		PyReturn *ret = &pyreturn_values[i];
		int bat_type = TYPE_any;
		sql_subtype *sql_subtype =
			argnode ? &((sql_arg *)argnode->data)->type : NULL;
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
			BBPkeepref(b->batCacheid);
		} else { // single value return, only for non-grouped aggregations
			if (bat_type != TYPE_str) {
				if (VALinit(&stk->stk[pci->argv[i]], bat_type, Tloc(b, 0)) ==
					NULL)
					msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			} else {
				BATiter li = bat_iterator(b);
				if (VALinit(&stk->stk[pci->argv[i]], bat_type,
							BUNtail(li, 0)) == NULL)
					msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		if (argnode) {
			argnode = argnode->next;
		}
	}
wrapup:

#ifdef HAVE_FORK
	if (mapped && child_process) {
		// If we get here, something went wrong in a child process
		char *error_mem;
		ReturnBatDescr *ptr;

		// Now we exit the program with an error code
		if (GDKchangesemval(query_sem, 0, 1) != GDK_SUCCEED) {
			exit(1);
		}

		assert(memory_size > 0);
		if ((mmap_ptrs[0] = GDKinitmmap(mmap_id + 0, memory_size, &mmap_sizes[0])) == NULL) {
			exit(1);
		}

		// To indicate that we failed, we will write information to our header
		ptr = (ReturnBatDescr *)mmap_ptrs[0];
		for (i = 0; i < retcols; i++) {
			ReturnBatDescr *descr = &ptr[i];
			// We will write descr->npy_type to -1, so other processes can see
			// that we failed
			descr->npy_type = -1;
			// We will write the memory size of our error message to the
			// bat_size, so the main process can access the shared memory
			descr->bat_size = (strlen(msg) + 1) * sizeof(char);
		}

		// Now create the shared memory to write our error message to
		// We can simply use the slot mmap_id + 3, even though this is normally
		// used for query return values
		// This is because, if the process fails, no values will be returned
		if ((error_mem = GDKinitmmap(mmap_id + 3,
									 (strlen(msg) + 1) * sizeof(char),
									 NULL)) == NULL) {
			exit(1);
		}
		strcpy(error_mem, msg);
		exit(1);
	}
#endif

#ifdef HAVE_FORK
	if (holds_gil) {
		MT_lock_set(&pyapiLock);
		python_call_active = false;
		MT_lock_unset(&pyapiLock);
	}

	if (mapped) {
		for (i = 0; i < retcols; i++) {
			PyReturn *ret = &pyreturn_values[i];
			if (ret->mmap_id < 0) {
				// if we directly give the mmap file to a BAT, don't delete the
				// MMAP file
				mmap_ptrs[i + 3] = NULL;
			}
		}
		for (i = 0; i < 3 + pci->retc * 2; i++) {
			if (mmap_ptrs[i] != NULL) {
				GDKreleasemmap(mmap_ptrs[i], mmap_sizes[i], mmap_id + i);
			}
		}
		if (mmap_ptrs)
			GDKfree(mmap_ptrs);
		if (mmap_sizes)
			GDKfree(mmap_sizes);
		if (query_sem > 0) {
			GDKreleasesem(query_sem);
		}
	}
#endif
	// Actual cleanup
	// Cleanup input BATs
	for (i = pci->retc + 2; i < pci->argc; i++) {
		PyInput *inp = &pyinput_values[i - (pci->retc + 2)];
		if (inp->bat != NULL)
			BBPunfix(inp->bat->batCacheid);
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

	return msg;
}

str
PYFUNCNAME(PyAPIprelude)(void *ret) {
	(void) ret;
	MT_lock_set(&pyapiLock);
	if (!pyapiInitialized) {
#ifdef IS_PY3K
		wchar_t* program = Py_DecodeLocale("mserver5", NULL);
		wchar_t* argv[] = { program };
#else
		char* argv[] = {"mserver5"};
#endif
		str msg = MAL_SUCCEED;
		PyObject *tmp;
		Py_Initialize();
		PySys_SetArgvEx(1, argv, 0);
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
		tmp = PyString_FromString("marshal");
		marshal_module = PyImport_Import(tmp);
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
		fprintf(stdout, "# MonetDB/Python%d module loaded\n",
#ifdef IS_PY3K
			3
#else
			2
#endif
		);
	}
	MT_lock_unset(&pyapiLock);
	option_disable_fork = GDKgetenv_istrue(fork_disableflag) || GDKgetenv_isyes(fork_disableflag);
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

		py_error_string = PyString_AS_STRING(error);
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

static void ComputeParallelAggregation(AggrParams *p)
{
	int i;
	size_t group_it, ai;
	bool gstate = 0;
	// now perform the actual aggregation
	// we perform one aggregation per group

	// we need the GIL to execute the functions
	gstate = Python_ObtainGIL();
	for (group_it = p->group_start; group_it < p->group_end; group_it++) {
		// we first have to construct new
		PyObject *pArgsPartial =
			PyTuple_New(p->named_columns + p->additional_columns);
		PyObject *pColumnsPartial = PyDict_New();
		PyObject *result;
		size_t group_elements = (*p->group_counts)[group_it];
		ai = 0;
		// iterate over columns
		for (i = 0; i < (int)p->named_columns; i++) {
			PyObject *vararray = NULL;
			PyInput input = (*p->pyinput_values)[i];
			if (input.scalar) {
				// scalar not handled yet
				vararray = input.result;
			} else {
				npy_intp elements[1] = {group_elements};
				switch (input.bat_type) {
					case TYPE_void:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, 
#if SIZEOF_OID == SIZEOF_INT
							NPY_UINT
#else
							NPY_ULONGLONG
#endif
							,
							NULL,
							((oid ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_oid:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, 
#if SIZEOF_OID == SIZEOF_INT
							NPY_UINT32
#else
							NPY_UINT64
#endif
							,
							NULL,
							((oid ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_bit:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_BOOL, NULL,
							((bit ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_bte:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_INT8, NULL,
							((bte ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_sht:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_INT16, NULL,
							((sht ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_int:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_INT32, NULL,
							((int ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_lng:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_INT64, NULL,
							((lng ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_flt:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_FLOAT32, NULL,
							((flt ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
#ifdef HAVE_HGE
					case TYPE_hge:
#endif
					case TYPE_dbl:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_FLOAT64, NULL,
							((dbl ***)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
					case TYPE_str:
						vararray = PyArray_New(
							&PyArray_Type, 1, elements, NPY_OBJECT, NULL,
							((PyObject ****)(*p->split_bats))[group_it][i], 0,
							NPY_ARRAY_CARRAY || !NPY_ARRAY_WRITEABLE, NULL);
						break;
				}

				if (vararray == NULL) {
					p->msg = createException(MAL, "pyapi3.eval", SQLSTATE(HY013) MAL_MALLOC_FAIL
											 " to create NumPy array.");
					goto wrapup;
				}
			}
			// fill in _columns array
			PyDict_SetItemString(pColumnsPartial, (*p->args)[p->base + i],
								 vararray);

			PyTuple_SetItem(pArgsPartial, ai++, vararray);
		}

		// additional parameters
		PyTuple_SetItem(pArgsPartial, ai++, pColumnsPartial);
		PyTuple_SetItem(pArgsPartial, ai++, *p->column_types_dict);
		Py_INCREF(*p->column_types_dict);
		PyTuple_SetItem(pArgsPartial, ai++, *p->connection);
		Py_INCREF(*p->connection);

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
// release the GIL again
wrapup:
	gstate = Python_ReleaseGIL(gstate);
}

static void CreateEmptyReturn(MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
							  size_t retcols, oid seqbase)
{
	size_t i;
	for (i = 0; i < retcols; i++) {
		int bat_type = getBatType(getArgType(mb, pci, i));
		BAT *b = COLnew(seqbase, bat_type, 0, TRANSIENT);
		if (isaBatType(getArgType(mb, pci, i))) {
			*getArgReference_bat(stk, pci, i) = b->batCacheid;
			BBPkeepref(b->batCacheid);
		} else { // single value return, only for non-grouped aggregations
			VALinit(&stk->stk[pci->argv[i]], bat_type, Tloc(b, 0));
		}
	}
}
