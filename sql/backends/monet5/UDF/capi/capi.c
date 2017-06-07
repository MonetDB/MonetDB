/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "capi.h"
#include "cheader.h"
#include "cheader.text.h"

static str
CUDFeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped);

str CUDFevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return CUDFeval(cntxt, mb, stk, pci, 0);
}

str CUDFevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {
	return CUDFeval(cntxt, mb, stk, pci, 1);
}

str CUDFprelude(void *ret) {
	(void) ret;
	return MAL_SUCCEED;
}

typedef char* (*jitted_function)(void** inputs, void** outputs, malloc_function_ptr malloc);

static bool
WriteDataToFile(FILE* f, const void* data, size_t data_size) {
	fwrite(data, data_size, 1, f);
	return (!ferror(f));
}

static bool
WriteTextToFile(FILE* f, const char* data) {
	return WriteDataToFile(f, data, strlen(data));
}

#define ATTEMPT_TO_WRITE_TO_FILE(f, data) \
	if (!WriteTextToFile(f, data)) { \
		errno = 0; \
		msg = createException(MAL, "cudf.eval", "Write error."); \
		goto wrapup; \
	}

#define GENERATE_BAT_INPUT(b, tpe) {\
	struct cudf_data_struct_##tpe* bat_data = GDKmalloc(sizeof(struct cudf_data_struct_##tpe)); \
	if (!bat_data) { \
		goto wrapup; \
	} \
	inputs[index] = bat_data; \
	bat_data->count = BATcount(b); \
	bat_data->data = (tpe*) Tloc(b, 0); \
	bat_data->null_value = tpe##_nil;\
}

#define GENERATE_BAT_OUTPUT(tpe) {\
	struct cudf_data_struct_##tpe* bat_data = GDKmalloc(sizeof(struct cudf_data_struct_##tpe)); \
	if (!bat_data) { \
		goto wrapup; \
	} \
	outputs[index] = bat_data; \
	bat_data->count = 0; \
	bat_data->data = NULL; \
	bat_data->null_value = tpe##_nil;\
}

#define GENERATE_SCALAR_INPUT(tpe) \
	inputs[index] = GDKmalloc(sizeof(tpe)); \
	if (!inputs[index]) { \
		goto wrapup; \
	} \
	*((tpe*)inputs[index]) = *((tpe*)getArgReference(stk, pci, i));


#define JIT_COMPILER_NAME "clang"

static size_t GetTypeCount(int type, void* struct_ptr);
static void* GetTypeData(int type, void* struct_ptr);
static const char* GetTypeDefinition(int type);
static const char* GetTypeName(int type);

static str
CUDFeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci, bit grouped) {
	sql_func * sqlfun = NULL;
	str exprStr = *getArgReference_str(stk, pci, pci->retc + 1);

	int i = 0;
	char argbuf[64];
	char buf[BUFSIZ];
	char fname[BUFSIZ];
	char libname[BUFSIZ];
	char error_buf[BUFSIZ];
	char total_error_buf[8192];
	size_t error_buffer_position = 0;
	str *args = NULL;
	str *output_names = NULL;
	char *msg = MAL_SUCCEED;
	char* funcname = "yet_another_c_function";
	node * argnode;
	int seengrp = FALSE;
	FILE *f = NULL;
	void* handle = NULL;
	jitted_function func;

	FILE *compiler = NULL;
	int compiler_return_code;

	void** inputs = NULL;
	size_t input_count = 0;
	void** outputs = NULL;
	size_t output_count = 0;
	BAT** input_bats = NULL;

	const char* compilation_flags = "-g";


	if (!grouped) {
		sql_subfunc *sqlmorefun = (*(sql_subfunc**) getArgReference(stk, pci, pci->retc));
		if (sqlmorefun) sqlfun = (*(sql_subfunc**) getArgReference(stk, pci, pci->retc))->func;
	} else {
		sqlfun = *(sql_func**) getArgReference(stk, pci, pci->retc);
	}

	args = (str*) GDKzalloc(sizeof(str) * pci->argc);
	output_names = pci->retc > 0 ? (str*) GDKzalloc(sizeof(str) * pci->retc) : NULL;
	if (!args || !output_names) {
		throw(MAL, "cudf.eval", MAL_MALLOC_FAIL);
	}

	// retrieve the argument names from the sqlfun structure
	// first argument after the return contains the pointer to the sql_func structure
	if (sqlfun != NULL) {
		// retrieve the argument names (inputs)
		if (sqlfun->ops->cnt > 0) {
			int carg = pci->retc + 2;
			argnode = sqlfun->ops->h;
			while (argnode) {
				char* argname = ((sql_arg*) argnode->data)->name;
				args[carg] = GDKstrdup(argname);
				if (!args[carg]) {
					msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
					goto wrapup;
				}
				carg++;
				argnode = argnode->next;
			}
		}
		// retrieve the output names
		argnode = sqlfun->res->h;
		for (i = 0; i < sqlfun->res->cnt; i++) {
			output_names[i] = GDKstrdup(((sql_arg *)argnode->data)->name);
			argnode = argnode->next;
		}
	}
	// name unnamed outputs
	for(i = 0; i < pci->retc; i++) {
		if (!output_names[i]) {
			if (pci->retc > 1) {
				snprintf(argbuf, sizeof(argbuf), "output%i", i);
			} else {
				// just call it "output" if there is only one
				snprintf(argbuf, sizeof(argbuf), "output");
			}
			output_names[i] = GDKstrdup(argbuf);
		}
	}
	// the first unknown argument is the group, we don't really care for the rest.
	for (i = pci->retc + 2; i < pci->argc; i++) {
		if (args[i] == NULL) {
			if (!seengrp && grouped) {
				args[i] = GDKstrdup("aggr_group");
				if (!args[i]) {
					msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
					goto wrapup;
				}
				seengrp = TRUE;
			} else {
				snprintf(argbuf, sizeof(argbuf), "arg%i", i - pci->retc - 1);
				args[i] = GDKstrdup(argbuf);
				if (!args[i]) {
					msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
					goto wrapup;
				}
			}
		}
	}

	// name of the source file and lib file that will be generated
	snprintf(fname, BUFSIZ, "%s.c", funcname);
	snprintf(libname, BUFSIZ, "lib%s%s", funcname, SO_EXT);

	// first generate the source file
	f = fopen(fname, "w+");
	if (!f) {
		errno = 0;
		msg = createException(MAL, "cudf.eval", "Failed to open file for JIT compilation.");
		goto wrapup;
	}

	// Include some standard C headers first
	ATTEMPT_TO_WRITE_TO_FILE(f, "#include <stdio.h>\n");
	ATTEMPT_TO_WRITE_TO_FILE(f, "#include <stdlib.h>\n");
	// we include "cheader.h", but not directly to avoid having to deal with headers, etc...
	// Instead it is embedded in a string (loaded from "cheader.text.h")
	// this file contains the structures used for input/output arguments
	ATTEMPT_TO_WRITE_TO_FILE(f, cheader_header_text);
	// some monetdb-style typedefs to make it easier
	ATTEMPT_TO_WRITE_TO_FILE(f, "typedef signed char bte;\n");
	ATTEMPT_TO_WRITE_TO_FILE(f, "typedef short sht;\n");
	ATTEMPT_TO_WRITE_TO_FILE(f, "typedef long long lng;\n");
	ATTEMPT_TO_WRITE_TO_FILE(f, "typedef float flt;\n");
	ATTEMPT_TO_WRITE_TO_FILE(f, "typedef double dbl;\n");
	ATTEMPT_TO_WRITE_TO_FILE(f, "typedef char* str;\n");
	// create the actual function
	ATTEMPT_TO_WRITE_TO_FILE(f, "\nchar* ");
	ATTEMPT_TO_WRITE_TO_FILE(f, funcname);
	ATTEMPT_TO_WRITE_TO_FILE(f, "(void** __inputs, void** __outputs, malloc_function_ptr __malloc) {\n");

	const char* struct_prefix = "struct cudf_data_struct_";
	// now we convert the input arguments from void** to the proper input/output of the function
	// first convert the input
	// FIXME: deal with SQL types
	for (i = pci->retc + 2; i < pci->argc; i++) {
		if (!isaBatType(getArgType(mb, pci, i))) {
			// scalar input
			int scalar_type = getArgType(mb, pci, i);
			const char* tpe = GetTypeDefinition(scalar_type);
			assert(tpe);
			if (tpe) {
				snprintf(buf, sizeof(buf), "%s %s = *((%s*)__inputs[%d]);\n", tpe, args[i], tpe, i - (pci->retc + 2));
				ATTEMPT_TO_WRITE_TO_FILE(f, buf);
			}
		} else {
			int bat_type = ATOMstorage(getBatType(getArgType(mb, pci, i)));
			const char* tpe = GetTypeName(bat_type);
			assert(tpe);
			if (tpe) {
				snprintf(buf, sizeof(buf), "%s%s %s = *((%s%s*)__inputs[%d]);\n", struct_prefix, tpe, args[i], struct_prefix, tpe, i - (pci->retc + 2));
				ATTEMPT_TO_WRITE_TO_FILE(f, buf);
			}
		}
	}
	// output types
	// FIXME: deal with SQL types
	for (i = 0; i < pci->retc; i++) {
		int bat_type = getBatType(getArgType(mb, pci, i));
		const char* tpe = GetTypeName(bat_type);
		assert(tpe);
		if (tpe) {
			snprintf(buf, sizeof(buf), "%s%s* %s = ((%s%s*)__outputs[%d]);\n", struct_prefix, tpe, output_names[i], struct_prefix, tpe, i);
			ATTEMPT_TO_WRITE_TO_FILE(f, buf);
		}
	}


	ATTEMPT_TO_WRITE_TO_FILE(f, "\n");
	// write the actual user defined code into the file
	ATTEMPT_TO_WRITE_TO_FILE(f, exprStr);

	ATTEMPT_TO_WRITE_TO_FILE(f, "\nreturn 0;\n}\n");

	fclose(f);
	f = NULL;

	// now it's time to try to compile the code
	// we use popen to capture any error output
	snprintf(buf, sizeof(buf), "%s -c -fPIC %s %s.c -o %s.o 2>&1 >/dev/null", JIT_COMPILER_NAME, compilation_flags, funcname, funcname);
	compiler = popen(buf, "r");
	if (!compiler) {
		goto wrapup;
	}
	while (fgets(error_buf, sizeof(error_buf) - 1, compiler)) {
		size_t error_size = strlen(error_buf);
		snprintf(total_error_buf + error_buffer_position, sizeof(total_error_buf) - error_buffer_position - 1,
			"%s", error_buf);
		error_buffer_position += error_size;
	}

	compiler_return_code = pclose(compiler);
	compiler = NULL;

	if (compiler_return_code != 0) {
		// failure in compiling the code
		// report the failure to the user
		msg = createException(MAL, "cudf.eval", "Failed to compile C UDF:\n%s", total_error_buf);
		goto wrapup;
	}
	snprintf(buf, sizeof(buf), "%s %s.o -shared -o %s", JIT_COMPILER_NAME, funcname, libname);
	compiler = popen(buf, "r");
	if (!compiler) {
		goto wrapup;
	}
	while (fgets(error_buf, sizeof(error_buf) - 1, compiler)) {
		size_t error_size = strlen(error_buf);
		snprintf(total_error_buf + error_buffer_position, sizeof(total_error_buf) - error_buffer_position - 1,
			"%s", error_buf);
		error_buffer_position += error_size;
	}

	compiler_return_code = pclose(compiler);
	compiler = NULL;

	if (compiler_return_code != 0) {
		// failure in compiler
		msg = createException(MAL, "cudf.eval", "Failed to link C UDF:\n%s", total_error_buf);
		goto wrapup;
	}

	handle = dlopen(libname, RTLD_LAZY);
	if (!handle) {
		msg = createException(MAL, "cudf.eval", "Failed to open shared library: %s.", dlerror());
		goto wrapup;
	}
	func = (jitted_function) dlsym(handle, funcname);
	if (!func) {
		msg = createException(MAL, "cudf.eval", "Failed to load function from library: %s.", dlerror());
		goto wrapup;
	}

	// now create the actual input/output parameters from the input bats
	input_count = pci->argc - (pci->retc + 2);
	output_count = pci->retc;

	input_bats = GDKzalloc(sizeof(BAT*) * input_count);
	inputs = GDKzalloc(sizeof(void*) * input_count);
	outputs = GDKzalloc(sizeof(void*) * output_count);
	if (!inputs || !outputs || !input_bats) {
		goto wrapup;
	}
	// create the inputs
	for (i = pci->retc + 2; i < pci->argc; i++) {
		size_t index = i - (pci->retc + 2);
		if (!isaBatType(getArgType(mb, pci, i))) {
			// deal with scalar input
			int scalar_type = getArgType(mb, pci, i);
			switch(scalar_type) {
				case TYPE_bit:
					GENERATE_SCALAR_INPUT(bit);
					break;
				case TYPE_bte:
					GENERATE_SCALAR_INPUT(bte);
					break;
				case TYPE_sht:
					GENERATE_SCALAR_INPUT(sht);
					break;
				case TYPE_int:
					GENERATE_SCALAR_INPUT(int);
					break;
				case TYPE_oid:
					GENERATE_SCALAR_INPUT(oid);
					break;
				case TYPE_lng:
					GENERATE_SCALAR_INPUT(lng);
					break;
				case TYPE_flt:
					GENERATE_SCALAR_INPUT(flt);
					break;
				case TYPE_dbl:
					GENERATE_SCALAR_INPUT(dbl);
					break;
				case TYPE_str:
					inputs[index] = GDKmalloc(sizeof(str));
					if (!inputs[index]) {
						goto wrapup;
					}
					*((str*)inputs[index]) = *((str*)getArgReference_str(stk, pci, i));
					break;
				default:
					assert(0);
					goto wrapup;
			}
		} else {
			// deal with BAT input
			int bat_type = ATOMstorage(getBatType(getArgType(mb, pci, i)));
			input_bats[index] = BATdescriptor(*getArgReference_bat(stk, pci, i));
			switch(bat_type) {
				case TYPE_bit:
					GENERATE_BAT_INPUT(input_bats[index], bte);
					break;
				case TYPE_bte:
					GENERATE_BAT_INPUT(input_bats[index], bte);
					break;
				case TYPE_sht:
					GENERATE_BAT_INPUT(input_bats[index], sht);
					break;
				case TYPE_int:
					GENERATE_BAT_INPUT(input_bats[index], int);
					break;
				case TYPE_oid:
					assert(0);
					//GENERATE_BAT_INPUT(input_bats[index], oid);
					break;
				case TYPE_lng:
					GENERATE_BAT_INPUT(input_bats[index], lng);
					break;
				case TYPE_flt:
					GENERATE_BAT_INPUT(input_bats[index], flt);
					break;
				case TYPE_dbl:
					GENERATE_BAT_INPUT(input_bats[index], dbl);
					break;
				case TYPE_str:
					assert(0);
					// FIXME: strings
					break;
				default:
					assert(0);
					goto wrapup;
			}
		}
	}
	// output types
	// FIXME: deal with SQL types
	for (i = 0; i < output_count; i++) {
		size_t index = i;
		int bat_type = getBatType(getArgType(mb, pci, i));
		switch(bat_type) {
			case TYPE_bit:
				GENERATE_BAT_OUTPUT(bte);
				break;
			case TYPE_bte:
				GENERATE_BAT_OUTPUT(bte);
				break;
			case TYPE_sht:
				GENERATE_BAT_OUTPUT(sht);
				break;
			case TYPE_int:
				GENERATE_BAT_OUTPUT(int);
				break;
			case TYPE_oid:
				assert(0);
				//GENERATE_BAT_OUTPUT(oid);
				break;
			case TYPE_lng:
				GENERATE_BAT_OUTPUT(lng);
				break;
			case TYPE_flt:
				GENERATE_BAT_OUTPUT(flt);
				break;
			case TYPE_dbl:
				GENERATE_BAT_OUTPUT(dbl);
				break;
			case TYPE_str:
				assert(0);
				// FIXME: strings
				break;
			default:
				assert(0);
				goto wrapup;
		}
	}

	// call the actual jitted function
	msg = func(inputs, outputs, GDKmalloc);
	if (msg) {
		// failure in function
		msg = createException(MAL, "cudf.eval", msg);
		goto wrapup;
	}


	// FIXME: deal with strings
	// FIXME: deal with SQL types
	for (i = 0; i < pci->retc; i++) {
		int bat_type = getBatType(getArgType(mb, pci, i));
		// create bats, BBPkeepref, etc...
		void* data = GetTypeData(bat_type, outputs[i]);
		size_t count = GetTypeCount(bat_type, outputs[i]);
		BAT* b = COLnew(0, bat_type, count, TRANSIENT);
		if (!b) {
			msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
			goto wrapup;
		}
		b->tnil = 0;
		b->tnonil = 0;
		b->tkey = 0;
		b->tsorted = 0;
		b->trevsorted = 0;
		// free the current (initial) storage
		GDKfree(b->theap.base);
		b->theap.base = data;
		b->theap.size = count * b->twidth;
		b->theap.free = b->theap.size;
		b->theap.storage = STORE_MEM;
		b->theap.newstorage = STORE_MEM;
		b->batCount = (BUN)count;
		b->batCapacity = (BUN)count;
		b->batCopiedtodisk = false;

		GDKfree(outputs[i]);
		outputs[i] = NULL;

		*getArgReference_bat(stk, pci, i) = b->batCacheid;
		BBPkeepref(b->batCacheid);
	}

/*
		if (isaBatType(getArgType(mb, pci, i))) {
			*getArgReference_bat(stk, pci, i) = b->batCacheid;
			BBPkeepref(b->batCacheid);
		} else { // single value return, only for non-grouped aggregations
			if (bat_type != TYPE_str) {
				if (VALinit(&stk->stk[pci->argv[i]], bat_type, Tloc(b, 0)) ==
					NULL)
					msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL);
			} else {
				BATiter li = bat_iterator(b);
				if (VALinit(&stk->stk[pci->argv[i]], bat_type,
							BUNtail(li, 0)) == NULL)
					msg = createException(MAL, "pyapi.eval", MAL_MALLOC_FAIL);
			}
		}
		*/


wrapup:
	if (args) {
		for(i = 0; i < pci->argc; i++) {
			if (args[i]) {
				GDKfree(args[i]);
			}
		}
		GDKfree(args);
	}
	if (output_names) {
		for(i = 0; i < pci->retc; i++) {
			if (output_names[i]) {
				GDKfree(output_names[i]);
			}
		}
		GDKfree(output_names);
	}
	if (inputs) {
		for(i = 0; i < input_count; i++) {
			if (inputs[i]) {
				GDKfree(inputs[i]);
			}
		}
		GDKfree(inputs);
	}
	if (outputs) {
		for(i = 0; i < output_count; i++) {
			int bat_type = getBatType(getArgType(mb, pci, i));
			if (outputs[i]) {
				void* data = GetTypeData(bat_type, outputs[i]);
				if (data) {
					GDKfree(data);
				}
				GDKfree(outputs[i]);
			}
		}
		GDKfree(outputs);
	}
	if (f) {
		fclose(f);
	}
	if (handle) {
		dlclose(handle);
	}
	if (compiler) {
		pclose(compiler);
	}
	return msg;
}


static const char* GetTypeDefinition(int type) {
	const char* tpe = NULL;
	switch(type) {
		case TYPE_bit:
			tpe = "bool";
			break;
		case TYPE_bte:
			tpe = "signed char";
			break;
		case TYPE_sht:
			tpe = "short";
			break;
		case TYPE_int:
			tpe = "int";
			break;
		case TYPE_oid:
			tpe = "long long";
			break;
		case TYPE_lng:
			tpe = "long long";
			break;
		case TYPE_flt:
			tpe = "float";
			break;
		case TYPE_dbl:
			tpe = "double";
			break;
		case TYPE_str:
			tpe = "char*";
			break;
	}
	return tpe;
}

static const char* GetTypeName(int type) {
	const char* tpe = NULL;
	switch(type) {
		case TYPE_bit:
		case TYPE_bte:
			tpe = "bte";
			break;
		case TYPE_sht:
			tpe = "short";
			break;
		case TYPE_int:
			tpe = "int";
			break;
		case TYPE_oid:
			tpe = "oid";
			break;
		case TYPE_lng:
			tpe = "lng";
			break;
		case TYPE_flt:
			tpe = "flt";
			break;
		case TYPE_dbl:
			tpe = "dbl";
			break;
		case TYPE_str:
			tpe = "str";
			break;
	}
	return tpe;
}

void* GetTypeData(int type, void* struct_ptr) {
	void* data = NULL;
	switch(type) {
		case TYPE_bit:
		case TYPE_bte:
			data = ((struct cudf_data_struct_bte*)struct_ptr)->data;
			break;
		case TYPE_sht:
			data = ((struct cudf_data_struct_sht*)struct_ptr)->data;
			break;
		case TYPE_int:
			data = ((struct cudf_data_struct_int*)struct_ptr)->data;
			break;
		case TYPE_oid:
			assert(0);
			break;
		case TYPE_lng:
			data = ((struct cudf_data_struct_lng*)struct_ptr)->data;
			break;
		case TYPE_flt:
			data = ((struct cudf_data_struct_flt*)struct_ptr)->data;
			break;
		case TYPE_dbl:
			data = ((struct cudf_data_struct_dbl*)struct_ptr)->data;
			break;
		case TYPE_str:
			assert(0);
			break;
	}
	return data;
}

size_t GetTypeCount(int type, void* struct_ptr) {
	size_t count = 0;
	switch(type) {
		case TYPE_bit:
		case TYPE_bte:
			count = ((struct cudf_data_struct_bte*)struct_ptr)->count;
			break;
		case TYPE_sht:
			count = ((struct cudf_data_struct_sht*)struct_ptr)->count;
			break;
		case TYPE_int:
			count = ((struct cudf_data_struct_int*)struct_ptr)->count;
			break;
		case TYPE_oid:
			assert(0);
			break;
		case TYPE_lng:
			count = ((struct cudf_data_struct_lng*)struct_ptr)->count;
			break;
		case TYPE_flt:
			count = ((struct cudf_data_struct_flt*)struct_ptr)->count;
			break;
		case TYPE_dbl:
			count = ((struct cudf_data_struct_dbl*)struct_ptr)->count;
			break;
		case TYPE_str:
			assert(0);
			break;
	}
	return count;
}
