/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "capi.h"
#include "cheader.h"
#include "cheader.text.h"

#include "mtime.h"
#include "blob.h"

#include <setjmp.h>
#include <signal.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic ignored "-Wclobbered"
#endif

struct _allocated_region;
typedef struct _allocated_region {
	struct _allocated_region *next;
} allocated_region;

struct _mprotected_region;
typedef struct _mprotected_region {
	void *addr;
	size_t len;

	struct _mprotected_region *next;
} mprotected_region;

static char *mprotect_region(void *addr, size_t len,
							 mprotected_region **regions);
static char *clear_mprotect(void *addr, size_t len);

static allocated_region *allocated_regions[THREADS];
static jmp_buf jump_buffer[THREADS];

typedef char *(*jitted_function)(void **inputs, void **outputs,
								 malloc_function_ptr malloc);

struct _cached_functions;
typedef struct _cached_functions {
	jitted_function function;
	BUN expression_hash;
	char *parameters;
	void *dll_handle;
	struct _cached_functions *next;
} cached_functions;

#define FUNCTION_CACHE_SIZE 128

static cached_functions *function_cache[FUNCTION_CACHE_SIZE];
static MT_Lock cache_lock;
static int cudf_initialized = 0;

static str CUDFeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
					bit grouped);

str CUDFevalStd(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return CUDFeval(cntxt, mb, stk, pci, 0);
}

str CUDFevalAggr(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	return CUDFeval(cntxt, mb, stk, pci, 1);
}

str CUDFprelude(void *ret)
{
	(void)ret;
	if (!cudf_initialized) {
		MT_lock_init(&cache_lock, "cache_lock");
		cudf_initialized = true;
	}
	return MAL_SUCCEED;
}

static bool WriteDataToFile(FILE *f, const void *data, size_t data_size)
{
	fwrite(data, data_size, 1, f);
	return (!ferror(f));
}

static bool WriteTextToFile(FILE *f, const char *data)
{
	return WriteDataToFile(f, data, strlen(data));
}

static void handler(int sig, siginfo_t *si, void *unused)
{
	int tid = THRgettid();

	(void)sig;
	(void)si;
	(void)unused;

	longjmp(jump_buffer[tid], 1);
}

static bool can_mprotect_region(void* addr) {
	int pagesize = getpagesize();
	void* page_begin = (void *)((size_t)addr - (size_t)addr % pagesize);
	return page_begin == addr;
}

static char *mprotect_region(void *addr, size_t len,
							 mprotected_region **regions)
{
	mprotected_region *region;
	if (len == 0)
		return NULL;

	assert(can_mprotect_region(addr));

	region = GDKmalloc(sizeof(mprotected_region));
	if (!region) {
		return MAL_MALLOC_FAIL;
	}
	region->addr = addr;
	region->len = len;
	region->next = *regions;
	*regions = region;
	return NULL;
}

static char *clear_mprotect(void *addr, size_t len)
{
	if (!addr)
		return NULL;

	if (mprotect(addr, len, PROT_READ | PROT_WRITE) < 0) {
		return strerror(errno);
	}
	return NULL;
}

#define ATTEMPT_TO_WRITE_TO_FILE(f, data)                                      \
	if (!WriteTextToFile(f, data)) {                                           \
		errno = 0;                                                             \
		msg = createException(MAL, "cudf.eval", "Write error.");               \
		goto wrapup;                                                           \
	}

#define ATTEMPT_TO_WRITE_DATA_TO_FILE(f, data, size)                           \
	if (!WriteDataToFile(f, data, size)) {                                     \
		errno = 0;                                                             \
		msg = createException(MAL, "cudf.eval", "Write error.");               \
		goto wrapup;                                                           \
	}

static void *jump_GDK_malloc(size_t size)
{
	void *ptr = GDKmalloc(size);
	if (!ptr) {
		longjmp(jump_buffer[THRgettid()], 2);
	}
	return ptr;
}

static void *add_allocated_region(void *ptr)
{
	allocated_region *region;
	int tid = THRgettid();
	region = (allocated_region *)ptr;
	region->next = allocated_regions[tid];
	allocated_regions[tid] = region;
	return (char *)ptr + sizeof(allocated_region);
}

static void *wrapped_GDK_malloc(size_t size)
{
	void *ptr = jump_GDK_malloc(size + sizeof(allocated_region));
	return add_allocated_region(ptr);
}

static void *wrapped_GDK_malloc_nojump(size_t size)
{
	void *ptr = GDKmalloc(size + sizeof(allocated_region));
	if (!ptr) {
		return NULL;
	}
	return add_allocated_region(ptr);
}

static void *wrapped_GDK_zalloc_nojump(size_t size)
{
	void *ptr = GDKzalloc(size + sizeof(allocated_region));
	if (!ptr) {
		return NULL;
	}
	return add_allocated_region(ptr);
}

#define GENERATE_NUMERIC_FUNCTIONS(type, tpename) \
	static void tpename##_initialize(struct cudf_data_struct_##tpename *self,  \
									 size_t count)                             \
	{                                                                          \
		BAT* b;                                                                \
		if (self->bat) {                                                       \
			BBPunfix(((BAT*)self->bat)->batCacheid);                           \
			self->bat = NULL;                                                  \
		}                                                                      \
		b = COLnew(0, TYPE_##tpename, count, TRANSIENT);                       \
		if (!b) {                                                              \
			longjmp(jump_buffer[THRgettid()], 2);                              \
		}                                                                      \
		self->bat = (void*) b;                                                 \
		self->count = count;                                                   \
		self->data = (type*) b->theap.base;                                    \
		BATsetcount(b, count);                                                 \
	}                                                                          \
	static int tpename##_is_null(type value) { return value == tpename##_nil; }


#define GENERATE_BASE_HEADERS(type, tpename)                                   \
	static int tpename##_is_null(type value);                                  \
	static void tpename##_initialize(struct cudf_data_struct_##tpename *self,  \
									 size_t count)                             \
	{                                                                          \
		self->count = count;                                                   \
		self->data = jump_GDK_malloc(count * sizeof(self->null_value));        \
	}

#define GENERATE_BASE_FUNCTIONS(tpe, tpename)                                  \
	GENERATE_BASE_HEADERS(tpe, tpename);                                       \
	static int tpename##_is_null(tpe value) { return value == tpename##_nil; }

GENERATE_NUMERIC_FUNCTIONS(bit, bit);
GENERATE_NUMERIC_FUNCTIONS(bte, bte);
GENERATE_NUMERIC_FUNCTIONS(sht, sht);
GENERATE_NUMERIC_FUNCTIONS(int, int);
GENERATE_NUMERIC_FUNCTIONS(lng, lng);
GENERATE_NUMERIC_FUNCTIONS(flt, flt);
GENERATE_NUMERIC_FUNCTIONS(dbl, dbl);
GENERATE_NUMERIC_FUNCTIONS(oid, oid);

GENERATE_BASE_HEADERS(char *, str);
GENERATE_BASE_HEADERS(cudf_data_date, date);
GENERATE_BASE_HEADERS(cudf_data_time, time);
GENERATE_BASE_HEADERS(cudf_data_timestamp, timestamp);
static int blob_is_null(cudf_data_blob value);
static void blob_initialize(struct cudf_data_struct_blob *self,
								 size_t count);

#define GENERATE_BAT_INPUT_BASE(tpe)                                           \
	struct cudf_data_struct_##tpe *bat_data =                                  \
		GDKzalloc(sizeof(struct cudf_data_struct_##tpe));                      \
	if (!bat_data) {                                                           \
		msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);              \
		goto wrapup;                                                           \
	}                                                                          \
	inputs[index] = bat_data;                                                  \
	bat_data->is_null = tpe##_is_null;                                         \
	bat_data->scale =                                                          \
		argnode ? pow(10, ((sql_arg *)argnode->data)->type.scale) : 1;         \
	bat_data->bat = NULL;                                                      \
	bat_data->initialize = (void (*)(void *, size_t))tpe##_initialize;

#define GENERATE_BAT_INPUT(b, tpe)                                             \
	{                                                                          \
		char *mprotect_retval;                                                 \
		GENERATE_BAT_INPUT_BASE(tpe);                                          \
		bat_data->count = BATcount(b);                                         \
		bat_data->null_value = tpe##_nil;                                      \
		if (b->tdense) {                                                       \
			size_t it = 0;                                                     \
			tpe val = b->T.seq;                                                \
			/* bat is dense, materialize it */                                 \
			bat_data->data = wrapped_GDK_malloc_nojump(                        \
				bat_data->count * sizeof(bat_data->null_value));               \
			if (!bat_data->data) {                                             \
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);      \
				goto wrapup;                                                   \
			}                                                                  \
			for (it = 0; it < bat_data->count; it++) {                         \
				bat_data->data[it] = val++;                                    \
			}                                                                  \
		} else if (can_mprotect_region(Tloc(b, 0))) {                          \
			bat_data->data = (tpe *)Tloc(b, 0);                                \
			mprotect_retval = mprotect_region(                                 \
				bat_data->data,                                                \
				bat_data->count * sizeof(bat_data->null_value), &regions);     \
			if (mprotect_retval) {                                             \
				msg = createException(MAL, "cudf.eval",                        \
									  "Failed to mprotect region: %s",         \
									  mprotect_retval);                        \
				goto wrapup;                                                   \
			}                                                                  \
		} else {                                                               \
			/* cannot mprotect bat region, copy data */                        \
			bat_data->data = wrapped_GDK_malloc_nojump(                        \
				bat_data->count * sizeof(bat_data->null_value));               \
			if (!bat_data->data) {                                             \
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);      \
				goto wrapup;                                                   \
			}                                                                  \
			memcpy(bat_data->data, Tloc(b, 0),                                 \
				bat_data->count * sizeof(bat_data->null_value));                \
		}                                                                      \
	}

#define GENERATE_BAT_OUTPUT_BASE(tpe)                                          \
	struct cudf_data_struct_##tpe *bat_data =                                  \
		GDKzalloc(sizeof(struct cudf_data_struct_##tpe));                      \
	if (!bat_data) {                                                           \
		msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);              \
		goto wrapup;                                                           \
	}                                                                          \
	outputs[index] = bat_data;                                                 \
	bat_data->count = 0;                                                       \
	bat_data->data = NULL;                                                     \
	bat_data->is_null = tpe##_is_null;                                         \
	bat_data->scale =                                                          \
		argnode ? pow(10, ((sql_arg *)argnode->data)->type.scale) : 1;         \
	bat_data->initialize = (void (*)(void *, size_t))tpe##_initialize;

#define GENERATE_BAT_OUTPUT(tpe)                                               \
	{                                                                          \
		GENERATE_BAT_OUTPUT_BASE(tpe);                                         \
		bat_data->null_value = (tpe)tpe##_nil;                                 \
	}

#define GENERATE_SCALAR_INPUT(tpe)                                             \
	inputs[index] = GDKmalloc(sizeof(tpe));                                    \
	if (!inputs[index]) {                                                      \
		goto wrapup;                                                           \
	}                                                                          \
	*((tpe *)inputs[index]) = *((tpe *)getArgReference(stk, pci, i));

const char *debug_flag = "capi_use_debug";
const char *cc_flag = "capi_cc";
const char *cpp_flag = "capi_cpp";

const char *cflags_pragma = "#pragma CFLAGS ";
const char *ldflags_pragma = "#pragma LDFLAGS ";

#define JIT_COMPILER_NAME "cc"
#define JIT_CPP_COMPILER_NAME "c++"

static size_t GetTypeCount(int type, void *struct_ptr);
static void *GetTypeData(int type, void *struct_ptr);
static void *GetTypeBat(int type, void *struct_ptr);
static const char *GetTypeName(int type);

static void data_from_date(date d, cudf_data_date *ptr);
static date date_from_data(cudf_data_date *ptr);
static void data_from_time(daytime d, cudf_data_time *ptr);
static daytime time_from_data(cudf_data_time *ptr);
static void data_from_timestamp(timestamp d, cudf_data_timestamp *ptr);
static timestamp timestamp_from_data(cudf_data_timestamp *ptr);

static char valid_path_characters[] = "abcdefghijklmnopqrstuvwxyz";

static str CUDFeval(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci,
					bit grouped)
{
	sql_func *sqlfun = NULL;
	bit use_cpp = *((bit *)getArgReference(stk, pci, pci->retc + 1));
	str exprStr = *getArgReference_str(stk, pci, pci->retc + 2);

	const int ARG_OFFSET = 3;

	size_t i = 0, j = 0;
	char argbuf[64];
	char buf[8192];
	char fname[BUFSIZ];
	char oname[BUFSIZ];
	char libname[BUFSIZ];
	char error_buf[BUFSIZ];
	char total_error_buf[8192];
	size_t error_buffer_position = 0;
	str *args = NULL;
	str *output_names = NULL;
	char *msg = MAL_SUCCEED;
	node *argnode;
	int seengrp = FALSE;
	FILE *f = NULL;
	void *handle = NULL;
	jitted_function volatile func = NULL;
	int ret;

	FILE *compiler = NULL;
	int compiler_return_code;

	void ** volatile inputs = NULL;
	size_t volatile input_count = 0;
	void ** volatile outputs = NULL;
	size_t volatile output_count = 0;
	BAT ** volatile input_bats = NULL;
	mprotected_region *regions = NULL, *region_iter = NULL;

	lng initial_output_count = -1;

	struct sigaction sa, oldsa, oldsb;
	sigset_t signal_set;

#ifdef NDEBUG
	int debug_build =
		GDKgetenv_istrue(debug_flag) || GDKgetenv_isyes(debug_flag);
#else
	int debug_build = true;
#endif
	char* extra_cflags = NULL;
	char* extra_ldflags = NULL;


	const char *compilation_flags = debug_build ? "-g -O0" : "-O2";
	const char *c_compiler =
		use_cpp ? (GDKgetenv(cpp_flag) ? GDKgetenv(cpp_flag)
									   : JIT_CPP_COMPILER_NAME)
				: (GDKgetenv(cc_flag) ? GDKgetenv(cc_flag) : JIT_COMPILER_NAME);

	const char *struct_prefix = "struct cudf_data_struct_";
	const char *funcname;

	BUN expression_hash = 0, funcname_hash = 0;
	cached_functions *cached_function;
	char* volatile function_parameters = NULL;
	int tid = THRgettid();
	size_t input_size = 0;
	bit non_grouped_aggregate = 0;

	size_t index = 0;
	int bat_type = 0;
	const char* tpe = NULL;

	size_t volatile extra_inputs = 0;

	(void)cntxt;

	allocated_regions[tid] = NULL;

	// we need to be able to catch segfaults and bus errors
	// so we can work with mprotect to prevent UDFs from changing
	// the input data

	// we remove them from the pthread_sigmask
	(void)sigemptyset(&signal_set);
	(void)sigaddset(&signal_set, SIGSEGV);
	(void)sigaddset(&signal_set, SIGBUS);
	(void)pthread_sigmask(SIG_UNBLOCK, &signal_set, NULL);

	memset(&sa, 0, sizeof(sa));

	if (!grouped) {
		sql_subfunc *sqlmorefun =
			(*(sql_subfunc **)getArgReference(stk, pci, pci->retc));
		if (sqlmorefun)
			sqlfun =
				(*(sql_subfunc **)getArgReference(stk, pci, pci->retc))->func;
	} else {
		sqlfun = *(sql_func **)getArgReference(stk, pci, pci->retc);
	}

	funcname = sqlfun ? sqlfun->base.name : "yet_another_c_function";

	args = (str *)GDKzalloc(sizeof(str) * pci->argc);
	output_names = (str *)GDKzalloc(sizeof(str) * pci->argc);
	if (!args || !output_names) {
		throw(MAL, "cudf.eval", MAL_MALLOC_FAIL);
	}

	// retrieve the argument names from the sqlfun structure
	// first argument after the return contains the pointer to the sql_func
	// structure
	if (sqlfun != NULL) {
		// retrieve the argument names (inputs)
		if (sqlfun->ops->cnt > 0) {
			int carg = pci->retc + ARG_OFFSET;
			argnode = sqlfun->ops->h;
			while (argnode) {
				char *argname = ((sql_arg *)argnode->data)->name;
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
		for (i = 0; i < (size_t)sqlfun->res->cnt; i++) {
			output_names[i] = GDKstrdup(((sql_arg *)argnode->data)->name);
			argnode = argnode->next;
		}
	}

	// name unnamed outputs
	for (i = 0; i < (size_t)pci->retc; i++) {
		if (!output_names[i]) {
			if (pci->retc > 1) {
				snprintf(argbuf, sizeof(argbuf), "output%zu", i);
			} else {
				// just call it "output" if there is only one
				snprintf(argbuf, sizeof(argbuf), "output");
			}
			output_names[i] = GDKstrdup(argbuf);
		}
	}
	// the first unknown argument is the group, we don't really care for the
	// rest.
	for (i = pci->retc + ARG_OFFSET; i < (size_t)pci->argc; i++) {
		if (args[i] == NULL) {
			if (!seengrp && grouped) {
				args[i] = GDKstrdup("aggr_group");
				if (!args[i]) {
					msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
					goto wrapup;
				}
				seengrp = TRUE;
			} else {
				snprintf(argbuf, sizeof(argbuf), "arg%zu", i - pci->retc - 1);
				args[i] = GDKstrdup(argbuf);
				if (!args[i]) {
					msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
					goto wrapup;
				}
			}
		}
	}
	// non-grouped aggregates don't have the group list
	// to allow users to write code for both grouped and non-grouped aggregates
	// we create an "aggr_group" BAT for non-grouped aggregates
	non_grouped_aggregate = grouped && !seengrp;

	input_count = pci->argc - (pci->retc + ARG_OFFSET);
	output_count = pci->retc;

	// begin the compilation phase
	// first look up if we have already compiled this function
	expression_hash = 0;
	GDK_STRHASH(exprStr, expression_hash);
	GDK_STRHASH(funcname, funcname_hash);
	funcname_hash = funcname_hash % FUNCTION_CACHE_SIZE;
	j = 0;
	for (i = 0; i < (size_t)pci->argc; i++) {
		if (args[i]) {
			j += strlen(args[i]);
		}
		if (output_names[i]) {
			j += strlen(output_names[i]);
		}
	}

	function_parameters =
		GDKzalloc((j + input_count + output_count + 1) * sizeof(char));
	if (!function_parameters) {
		msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
		goto wrapup;
	}
	for (i = 0; i < input_count; i++) {
		if (!isaBatType(getArgType(mb, pci, i))) {
			function_parameters[i] = getArgType(mb, pci, i);
		} else {
			function_parameters[i] = getBatType(getArgType(mb, pci, i));
		}
	}
	for (i = 0; i < output_count; i++) {
		if (!isaBatType(getArgType(mb, pci, i))) {
			function_parameters[input_count + i] = getArgType(mb, pci, i);
		} else {
			function_parameters[input_count + i] =
				getBatType(getArgType(mb, pci, i));
		}
	}
	j = input_count + output_count;
	for (i = 0; i < (size_t)pci->argc; i++) {
		if (args[i]) {
			size_t len = strlen(args[i]);
			memcpy(function_parameters + j, args[i], len);
			j += len;
		}
		if (output_names[i]) {
			size_t len = strlen(output_names[i]);
			memcpy(function_parameters + j, output_names[i], len);
			j += len;
		}
	}

	MT_lock_set(&cache_lock);
	cached_function = function_cache[funcname_hash];
	while (cached_function) {
		if (cached_function->expression_hash == expression_hash &&
			strcmp(cached_function->parameters, function_parameters) == 0) {
			// this function matches our compiled function
			// in both source code and parameters
			// use the already compiled function instead of recompiling
			func = cached_function->function;
			break;
		}
		cached_function = cached_function->next;
	}
	MT_lock_unset(&cache_lock);

	if (!func) {
		// function was not found in the cache
		// we have to compile it

		// first generate the names	of the files
		// we place the temporary files in the DELDIR directory
		// because this will be removed again upon server startup
		const int RANDOM_NAME_SIZE = 32;
		char *path = NULL;
		const char *prefix = TEMPDIR_NAME DIR_SEP_STR;
		size_t prefix_size = strlen(prefix);
		char *deldirpath;

		memcpy(buf, prefix, sizeof(char) * strlen(prefix));
		// generate a random 32-character name for the temporary files
		for (i = prefix_size; i < prefix_size + RANDOM_NAME_SIZE; i++) {
			buf[i] = valid_path_characters[rand() %
										   (sizeof(valid_path_characters) - 1)];
		}
		buf[i] = '\0';
		path = GDKfilepath(0, BATDIR, buf, "c");
		if (!path) {
			msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
			goto wrapup;
		}
		strcpy(fname, path);
		strcpy(oname, fname);
		oname[strlen(oname) - 1] = 'o';
		GDKfree(path);

		memmove(buf + strlen(SO_PREFIX) + prefix_size, buf + prefix_size,
				i + 1 - prefix_size);
		memcpy(buf + prefix_size, SO_PREFIX, sizeof(char) * strlen(SO_PREFIX));
		path =
			GDKfilepath(0, BATDIR, buf, SO_EXT[0] == '.' ? SO_EXT + 1 : SO_EXT);
		if (!path) {
			msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
			goto wrapup;
		}
		strcpy(libname, path);
		GDKfree(path);

		// if DELDIR directory does not exist, create it
		deldirpath = GDKfilepath(0, NULL, TEMPDIR, NULL);
		if (!deldirpath) {
			msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
			goto wrapup;
		}
		if (mkdir(deldirpath, 0755) < 0 && errno != EEXIST) {
			msg = createException(MAL, "cudf.eval",
								  "cannot create directory %s\n", deldirpath);
			goto wrapup;
		}
		GDKfree(deldirpath);

		// now generate the source file
		f = fopen(fname, "w+");
		if (!f) {
			msg = createException(MAL, "cudf.eval",
								  "Failed to open file for JIT compilation: %s",
								  strerror(errno));
			errno = 0;
			goto wrapup;
		}

		// include some standard C headers first
		ATTEMPT_TO_WRITE_TO_FILE(f, "#include <stdio.h>\n");
		ATTEMPT_TO_WRITE_TO_FILE(f, "#include <stdlib.h>\n");
		// we include "cheader.h", but not directly to avoid having to deal with
		// headers, etc...
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
		ATTEMPT_TO_WRITE_TO_FILE(f, "typedef size_t oid;\n");
		// now we search exprStr for any preprocessor directives (#)
		// we move these to the top of the file
		// this allows the user to normally #include files
		{
			int preprocessor_start = 0;
			bool is_preprocessor_directive = false;
			bool new_line = false;
			for (i = 0; i < strlen(exprStr); i++) {
				if (exprStr[i] == '\n') {
					if (is_preprocessor_directive) {
						// the previous line was a preprocessor directive
						// first check if it is one of our special preprocessor directives
						if (i - preprocessor_start >= strlen(cflags_pragma) && 
							memcmp(exprStr + preprocessor_start, cflags_pragma, strlen(cflags_pragma)) == 0) {
							size_t cflags_characters = (i - preprocessor_start) - strlen(cflags_pragma);
							if (cflags_characters > 0 && !extra_cflags) {
								extra_cflags = GDKzalloc(cflags_characters + 1);
								if (extra_cflags) {
									memcpy(extra_cflags, exprStr + preprocessor_start + strlen(cflags_pragma), cflags_characters);
								}
							}
						} else if (i - preprocessor_start >= strlen(ldflags_pragma) && 
							memcmp(exprStr + preprocessor_start, ldflags_pragma, strlen(ldflags_pragma)) == 0) {
							size_t ldflags_characters = (i - preprocessor_start) - strlen(ldflags_pragma);
							if (ldflags_characters > 0 && !extra_ldflags) {
								extra_ldflags = GDKzalloc(ldflags_characters + 1);
								if (extra_ldflags) {
									memcpy(extra_ldflags, exprStr + preprocessor_start + strlen(ldflags_pragma), ldflags_characters);
								}
							}
						} else {
							// regular preprocessor directive: write it to the file
							ATTEMPT_TO_WRITE_DATA_TO_FILE(f, exprStr +
																 preprocessor_start,
														  i - preprocessor_start);
							ATTEMPT_TO_WRITE_TO_FILE(f, "\n");
						}
						// now overwrite the preprocessor directive in the
						// expression string with spaces
						for (j = preprocessor_start; j < i; j++) {
							exprStr[j] = ' ';
						}
					}
					is_preprocessor_directive = false;
					new_line = true;
				} else if (exprStr[i] == ' ' || exprStr[i] == '\t') {
					// skip any spaces
					continue;
				} else if (new_line) {
					if (exprStr[i] == '#') {
						preprocessor_start = i;
						is_preprocessor_directive = true;
					}
					new_line = false;
				}
			}
		}

		// create the actual function
		if (use_cpp) {
			// avoid name wrangling if we are compiling C++ code
			ATTEMPT_TO_WRITE_TO_FILE(f, "\nextern \"C\"");
		}
		ATTEMPT_TO_WRITE_TO_FILE(f, "\nchar* ");
		ATTEMPT_TO_WRITE_TO_FILE(f, funcname);
		ATTEMPT_TO_WRITE_TO_FILE(f, "(void** __inputs, void** __outputs, "
									"malloc_function_ptr malloc) {\n");

		// now we convert the input arguments from void** to the proper
		// input/output
		// of the function
		// first convert the input
		for (i = pci->retc + ARG_OFFSET; i < (size_t)pci->argc; i++) {
			bat_type = !isaBatType(getArgType(mb, pci, i))
							   ? getArgType(mb, pci, i)
							   : getBatType(getArgType(mb, pci, i));
			tpe = GetTypeName(bat_type);
			assert(tpe);
			if (tpe) {
				snprintf(buf, sizeof(buf),
						 "\t%s%s %s = *((%s%s*)__inputs[%zu]);\n", struct_prefix,
						 tpe, args[i], struct_prefix, tpe,
						 i - (pci->retc + ARG_OFFSET));
				ATTEMPT_TO_WRITE_TO_FILE(f, buf);
			}
		}
		if (non_grouped_aggregate) {
			// manually add "aggr_group" for non-grouped aggregates
			bat_type = TYPE_oid;
			tpe = GetTypeName(bat_type);
			assert(tpe);
			if (tpe) {
				snprintf(buf, sizeof(buf),
						 "\t%s%s %s = *((%s%s*)__inputs[%zu]);\n", struct_prefix,
						 tpe, "aggr_group", struct_prefix, tpe, input_count);
				ATTEMPT_TO_WRITE_TO_FILE(f, buf);
			}
		}
		// output types
		for (i = 0; i < (size_t)pci->retc; i++) {
			bat_type = getBatType(getArgType(mb, pci, i));
			tpe = GetTypeName(bat_type);
			assert(tpe);
			if (tpe) {
				snprintf(buf, sizeof(buf),
						 "\t%s%s* %s = ((%s%s*)__outputs[%zu]);\n", struct_prefix,
						 tpe, output_names[i], struct_prefix, tpe, i);
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
		snprintf(buf, sizeof(buf), "%s %s -c -fPIC %s %s -o %s 2>&1 >/dev/null",
				 c_compiler, extra_cflags ? extra_cflags : "", compilation_flags, fname, oname);
		compiler = popen(buf, "r");
		if (!compiler) {
			msg = createException(MAL, "cudf.eval", "Failed popen");
			goto wrapup;
		}
		// read the error stream into the error buffer until the compiler is
		// done
		while (fgets(error_buf, sizeof(error_buf) - 1, compiler)) {
			size_t error_size = strlen(error_buf);
			snprintf(total_error_buf + error_buffer_position,
					 sizeof(total_error_buf) - error_buffer_position - 1, "%s",
					 error_buf);
			error_buffer_position += error_size;
			if (error_buffer_position >= sizeof(total_error_buf)) break;
		}

		compiler_return_code = pclose(compiler);
		compiler = NULL;

		if (compiler_return_code != 0) {
			// failure in compiling the code
			// report the failure to the user
			msg = createException(MAL, "cudf.eval",
								  "Failed to compile C UDF:\n%s",
								  total_error_buf);
			goto wrapup;
		}

		error_buffer_position = 0;
		error_buf[0] = '\0';

		snprintf(buf, sizeof(buf), "%s %s %s -shared -o %s 2>&1 >/dev/null", c_compiler, 
			extra_ldflags ? extra_ldflags : "", oname, libname);
		compiler = popen(buf, "r");
		if (!compiler) {
			msg = createException(MAL, "cudf.eval", "Failed popen");
			goto wrapup;
		}
		while (fgets(error_buf, sizeof(error_buf) - 1, compiler)) {
			size_t error_size = strlen(error_buf);
			snprintf(total_error_buf + error_buffer_position,
					 sizeof(total_error_buf) - error_buffer_position - 1, "%s",
					 error_buf);
			error_buffer_position += error_size;
			if (error_buffer_position >= sizeof(total_error_buf)) break;
		}

		compiler_return_code = pclose(compiler);
		compiler = NULL;

		if (compiler_return_code != 0) {
			// failure in compiler
			msg = createException(MAL, "cudf.eval", "Failed to link C UDF.\n%s",
								  total_error_buf);
			goto wrapup;
		}

		handle = dlopen(libname, RTLD_LAZY);
		if (!handle) {
			msg = createException(MAL, "cudf.eval",
								  "Failed to open shared library: %s.",
								  dlerror());
			goto wrapup;
		}
		func = (jitted_function)dlsym(handle, funcname);
		if (!func) {
			msg = createException(MAL, "cudf.eval",
								  "Failed to load function from library: %s.",
								  dlerror());
			goto wrapup;
		}
		// now that we have compiled this function
		// store it in our function cache
		{
			cached_functions *new_entry = GDKmalloc(sizeof(cached_functions));
			if (!new_entry) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			new_entry->function = func;
			new_entry->expression_hash = expression_hash;
			new_entry->parameters = function_parameters;
			new_entry->dll_handle = handle;
			function_parameters = NULL;
			handle = NULL;
			MT_lock_set(&cache_lock);
			new_entry->next = function_cache[funcname_hash];
			function_cache[funcname_hash] = new_entry;
			MT_lock_unset(&cache_lock);
		}
	}
	if (input_count > 0) {
		// add "aggr_group" for non-grouped aggregates
		extra_inputs = non_grouped_aggregate ? 1 : 0;
		input_bats = GDKzalloc(sizeof(BAT *) * (input_count + extra_inputs));
		inputs = GDKzalloc(sizeof(void *) * (input_count + extra_inputs));
		if (!inputs || !input_bats) {
			msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
			goto wrapup;
		}
	}
	if (output_count > 0) {
		outputs = GDKzalloc(sizeof(void *) * output_count);
		if (!outputs) {
			msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
			goto wrapup;
		}
	}
	// create the inputs
	argnode = sqlfun ? sqlfun->ops->h : NULL;
	for (i = pci->retc + ARG_OFFSET; i < (size_t)pci->argc; i++) {
		index = i - (pci->retc + ARG_OFFSET);
		bat_type = getArgType(mb, pci, i);
		if (!isaBatType(bat_type)) {
			// scalar input
			// create a temporary BAT
			input_bats[index] = COLnew(0, bat_type, 1, TRANSIENT);
			if (!input_bats[index]) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			if (BUNappend(input_bats[index], getArgReference(stk, pci, i),
						  FALSE) != GDK_SUCCEED) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
		} else {
			// deal with BAT input
			bat_type = getBatType(getArgType(mb, pci, i));
			input_bats[index] =
				BATdescriptor(*getArgReference_bat(stk, pci, i));
		}

		if (bat_type == TYPE_bit) {
			GENERATE_BAT_INPUT(input_bats[index], bit);
		} else if (bat_type == TYPE_bte) {
			GENERATE_BAT_INPUT(input_bats[index], bte);
		} else if (bat_type == TYPE_sht) {
			GENERATE_BAT_INPUT(input_bats[index], sht);
		} else if (bat_type == TYPE_int) {
			GENERATE_BAT_INPUT(input_bats[index], int);
		} else if (bat_type == TYPE_oid) {
			GENERATE_BAT_INPUT(input_bats[index], oid);
		} else if (bat_type == TYPE_lng) {
			GENERATE_BAT_INPUT(input_bats[index], lng);
		} else if (bat_type == TYPE_flt) {
			GENERATE_BAT_INPUT(input_bats[index], flt);
		} else if (bat_type == TYPE_dbl) {
			GENERATE_BAT_INPUT(input_bats[index], dbl);
		} else if (bat_type == TYPE_str) {
			BATiter li;
			BUN p = 0, q = 0;
			bool can_mprotect_varheap = false;
			str mprotect_retval;
			GENERATE_BAT_INPUT_BASE(str);
			bat_data->count = BATcount(input_bats[index]);
			bat_data->data = GDKmalloc(sizeof(char *) * bat_data->count);
			bat_data->null_value = NULL;
			if (!bat_data->data) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			j = 0;

			// check if we can mprotect the varheap
			// if we can't mprotect, copy the strings instead
			assert(input_bats[index]->tvheap);
			can_mprotect_varheap = can_mprotect_region(input_bats[index]->tvheap->base);

			li = bat_iterator(input_bats[index]);
			BATloop(input_bats[index], p, q)
			{
				char *t = (char *)BUNtail(li, p);
				if (strcmp(t, str_nil) == 0) {
					bat_data->data[j] = NULL;
				} else {
					if (can_mprotect_varheap) {
						bat_data->data[j] = t;
					} else {
						bat_data->data[j] = wrapped_GDK_malloc_nojump(strlen(t) + 1);
						if (!bat_data->data[j]) {
							msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
							goto wrapup;
						}
						strcpy(bat_data->data[j], t);
					}
				}
				j++;
			}
			if (can_mprotect_varheap) {
				// mprotect the varheap of the BAT to prevent modification of input strings
				mprotect_retval =
					mprotect_region(input_bats[index]->tvheap->base,
									input_bats[index]->tvheap->size, &regions);
				if (mprotect_retval) {
					msg = createException(MAL, "cudf.eval",
										  "Failed to mprotect region: %s",
										  mprotect_retval);
					goto wrapup;
				}
			}
		} else if (bat_type == TYPE_date) {
			date *baseptr;
			GENERATE_BAT_INPUT_BASE(date);
			bat_data->count = BATcount(input_bats[index]);
			bat_data->data =
				GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}

			baseptr = (date *)Tloc(input_bats[index], 0);
			for (j = 0; j < bat_data->count; j++) {
				data_from_date(baseptr[j], bat_data->data + j);
			}
			data_from_date(date_nil, &bat_data->null_value);
		} else if (bat_type == TYPE_daytime) {
			daytime *baseptr;
			GENERATE_BAT_INPUT_BASE(time);
			bat_data->count = BATcount(input_bats[index]);
			bat_data->data =
				GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}

			baseptr = (daytime *)Tloc(input_bats[index], 0);
			for (j = 0; j < bat_data->count; j++) {
				data_from_time(baseptr[j], bat_data->data + j);
			}
			data_from_time(daytime_nil, &bat_data->null_value);
		} else if (bat_type == TYPE_timestamp) {
			timestamp *baseptr;
			GENERATE_BAT_INPUT_BASE(timestamp);
			bat_data->count = BATcount(input_bats[index]);
			bat_data->data =
				GDKmalloc(sizeof(bat_data->null_value) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}

			baseptr = (timestamp *)Tloc(input_bats[index], 0);
			for (j = 0; j < bat_data->count; j++) {
				data_from_timestamp(baseptr[j], bat_data->data + j);
			}
			data_from_timestamp(*timestamp_nil, &bat_data->null_value);
		} else if (bat_type == TYPE_blob || bat_type == TYPE_sqlblob) {
			BATiter li;
			BUN p = 0, q = 0;
			str mprotect_retval;
			bool can_mprotect_varheap = false;
			GENERATE_BAT_INPUT_BASE(blob);
			bat_data->count = BATcount(input_bats[index]);
			bat_data->data =
				GDKmalloc(sizeof(cudf_data_blob) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			j = 0;

			// check if we can mprotect the varheap
			// if we can't mprotect, copy the strings instead
			assert(input_bats[index]->tvheap);
			can_mprotect_varheap = can_mprotect_region(input_bats[index]->tvheap->base);

			li = bat_iterator(input_bats[index]);
			BATloop(input_bats[index], p, q)
			{
				blob *t = (blob *)BUNtail(li, p);
				if (t->nitems == ~(size_t)0) {
					bat_data->data[j].size = 0;
					bat_data->data[j].data = NULL;
				} else {
					bat_data->data[j].size = t->nitems;
					if (can_mprotect_varheap) {
						bat_data->data[j].data = &t->data[0];
					} else {
						bat_data->data[j].data = wrapped_GDK_malloc_nojump(t->nitems);
						if (!bat_data->data[j].data) {
							msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
							goto wrapup;
						}
						memcpy(bat_data->data[j].data, &t->data[0], t->nitems);
					}
				}
				j++;
			}
			bat_data->null_value.size = 0;
			bat_data->null_value.data = NULL;
			if (can_mprotect_varheap) {
				// for blob columns, mprotect the varheap of the BAT
				mprotect_retval =
					mprotect_region(input_bats[index]->tvheap->base,
									input_bats[index]->tvheap->size, &regions);
				if (mprotect_retval) {
					msg = createException(MAL, "cudf.eval",
										  "Failed to mprotect region: %s",
										  mprotect_retval);
					goto wrapup;
				}
			}
		} else {
			// unsupported type: convert to string
			BATiter li;
			BUN p = 0, q = 0;
			GENERATE_BAT_INPUT_BASE(str);
			bat_data->count = BATcount(input_bats[index]);
			bat_data->null_value = NULL;
			bat_data->data = GDKzalloc(sizeof(char *) * bat_data->count);
			if (!bat_data->data) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			j = 0;

			li = bat_iterator(input_bats[index]);
			BATloop(input_bats[index], p, q)
			{
				void *t = BUNtail(li, p);
				if (BATatoms[bat_type].atomCmp(
						t, BATatoms[bat_type].atomNull) == 0) {
					bat_data->data[j] = NULL;
				} else {
					char *result = NULL;
					size_t length = 0;
					if (BATatoms[bat_type].atomToStr(&result, &length, t) ==
						0) {
						msg = createException(
							MAL, "cudf.eval",
							"Failed to convert element to string");
						goto wrapup;
					}
					bat_data->data[j] = result;
				}
				j++;
			}
		}
		input_size = BATcount(input_bats[index]) > input_size
						 ? BATcount(input_bats[index])
						 : input_size;
		argnode = argnode ? argnode->next : NULL;
	}

	index = input_count;
	if (non_grouped_aggregate) {
		GENERATE_BAT_INPUT_BASE(oid);
		bat_data->count = input_size;
		bat_data->null_value = oid_nil;
		bat_data->data =
			wrapped_GDK_zalloc_nojump(bat_data->count * sizeof(bat_data->null_value));
		if (!bat_data->data) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
		}
	}

	argnode = sqlfun ? sqlfun->res->h : NULL;
	// output types
	for (i = 0; i < output_count; i++) {
		index = i;
		bat_type = getBatType(getArgType(mb, pci, i));
		if (bat_type == TYPE_bit) {
			GENERATE_BAT_OUTPUT(bit);
		} else if (bat_type == TYPE_bte) {
			GENERATE_BAT_OUTPUT(bte);
		} else if (bat_type == TYPE_sht) {
			GENERATE_BAT_OUTPUT(sht);
		} else if (bat_type == TYPE_int) {
			GENERATE_BAT_OUTPUT(int);
		} else if (bat_type == TYPE_oid) {
			GENERATE_BAT_OUTPUT(oid);
		} else if (bat_type == TYPE_lng) {
			GENERATE_BAT_OUTPUT(lng);
		} else if (bat_type == TYPE_flt) {
			GENERATE_BAT_OUTPUT(flt);
		} else if (bat_type == TYPE_dbl) {
			GENERATE_BAT_OUTPUT(dbl);
		} else if (bat_type == TYPE_str) {
			GENERATE_BAT_OUTPUT_BASE(str);
			bat_data->null_value = NULL;
		} else if (bat_type == TYPE_date) {
			GENERATE_BAT_OUTPUT_BASE(date);
			data_from_date(date_nil, &bat_data->null_value);
		} else if (bat_type == TYPE_daytime) {
			GENERATE_BAT_OUTPUT_BASE(time);
			data_from_time(daytime_nil, &bat_data->null_value);
		} else if (bat_type == TYPE_timestamp) {
			GENERATE_BAT_OUTPUT_BASE(timestamp);
			data_from_timestamp(*timestamp_nil, &bat_data->null_value);
		} else if (bat_type == TYPE_blob || bat_type == TYPE_sqlblob) {
			GENERATE_BAT_OUTPUT_BASE(blob);
			bat_data->null_value.size = 0;
			bat_data->null_value.data = NULL;
		} else {
			// unsupported type, convert from string output
			GENERATE_BAT_OUTPUT_BASE(str);
			bat_data->null_value = NULL;
		}
		argnode = argnode ? argnode->next : NULL;
	}

	// set up a longjmp point
	// this longjmp point is used for some error handling in the C function
	// such as failed mallocs
	ret = setjmp(jump_buffer[tid]);
	if (ret < 0) {
		// error value
		msg = createException(MAL, "cudf.eval", "Failed setjmp: %s",
							  strerror(errno));
		errno = 0;
		goto wrapup;
	} else if (ret > 0) {
		if (ret == 1) {
			msg = createException(MAL, "cudf.eval", "Attempting to write to "
													"the input or triggered a "
													"segfault/bus error");
		} else if (ret == 2) {
			msg = createException(MAL, "cudf.eval",
								  "Malloc failure in internal function!");
		} else {
			// we jumped here
			msg = createException(MAL, "cudf.eval", "We longjumped here "
													"because of an error, but "
													"we don't know which!");
		}
		goto wrapup;
	}

	// set up the signal handler for catching segfaults
	memset(&sa, 0, sizeof(sa));
	sa.sa_flags = SA_SIGINFO;
	sigfillset(&sa.sa_mask);
	sa.sa_sigaction = handler;
	if (sigaction(SIGSEGV, &sa, &oldsa) == -1 ||
		sigaction(SIGBUS, &sa, &oldsb) == -1) {
		msg = createException(MAL, "cudf.eval",
							  "Failed to set signal handler: %s",
							  strerror(errno));
		errno = 0;
		goto wrapup;
	}

	// actually mprotect the regions now that the signal handlers are set
	region_iter = regions;
	while (region_iter) {
		if (mprotect(region_iter->addr, region_iter->len, PROT_READ) < 0) {
			goto wrapup;
		}
		region_iter = region_iter->next;
	}

	// call the actual jitted function
	msg = func(inputs, outputs, wrapped_GDK_malloc);

	// clear any mprotected regions
	while (regions) {
		mprotected_region *next = regions->next;
		clear_mprotect(regions->addr, regions->len);
		GDKfree(regions);
		regions = next;
	}

	// clear the signal handlers
	if (sigaction(SIGSEGV, &oldsa, NULL) == -1 ||
		sigaction(SIGBUS, &oldsb, NULL) == -1) {
		msg = createException(MAL, "cudf.eval",
							  "Failed to unset signal handler: %s",
							  strerror(errno));
		errno = 0;
		goto wrapup;
	}
	memset(&sa, 0, sizeof(sa));

	if (msg) {
		// failure in function
		msg = createException(MAL, "cudf.eval", "%s", msg);
		goto wrapup;
	}

	// create the output bats from the returned results
	for (i = 0; i < (size_t)pci->retc; i++) {
		size_t count;
		void *data;
		BAT *b;
		bat_type = getBatType(getArgType(mb, pci, i));
		
		if (!outputs[i]) {
			msg = createException(MAL, "cudf.eval", "No data returned.");
			goto wrapup;
		}
		count = GetTypeCount(bat_type, outputs[i]);
		data = GetTypeData(bat_type, outputs[i]);
		if (!data) {
			msg = createException(MAL, "cudf.eval", "No data returned.");
			goto wrapup;
		}
		if (initial_output_count < 0) {
			initial_output_count = count;
		} else if ((size_t)initial_output_count != count) {
			msg = createException(MAL, "cudf.eval",
								  "Data has different cardinalities.");
			goto wrapup;
		}
		if (bat_type == TYPE_bit || bat_type == TYPE_bte ||
			bat_type == TYPE_sht || bat_type == TYPE_int ||
			bat_type == TYPE_oid || bat_type == TYPE_lng ||
			bat_type == TYPE_flt || bat_type == TYPE_dbl) {
			b = GetTypeBat(bat_type, outputs[i]);
			if (!b) {
				msg = createException(MAL, "cudf.eval", "Output column was not properly initialized.");
				goto wrapup;
			}
		} else {
			assert(GetTypeBat(bat_type, outputs[i]) == NULL);
			b = COLnew(0, bat_type, count, TRANSIENT);
			if (!b) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			if (bat_type == TYPE_date) {
				date *baseptr = (date *)Tloc(b, 0);
				cudf_data_date *source_base = (cudf_data_date *)data;
				for (j = 0; j < count; j++) {
					baseptr[j] = date_from_data(source_base + j);
				}
				BATsetcount(b, count);
				GDKfree(data);
			} else if (bat_type == TYPE_daytime) {
				daytime *baseptr = (daytime *)Tloc(b, 0);
				cudf_data_time *source_base = (cudf_data_time *)data;
				for (j = 0; j < count; j++) {
					baseptr[j] = time_from_data(source_base + j);
				}
				BATsetcount(b, count);
				GDKfree(data);
			} else if (bat_type == TYPE_timestamp) {
				timestamp *baseptr = (timestamp *)Tloc(b, 0);
				cudf_data_timestamp *source_base = (cudf_data_timestamp *)data;
				for (j = 0; j < count; j++) {
					baseptr[j] = timestamp_from_data(source_base + j);
				}
				BATsetcount(b, count);
				GDKfree(data);
			} else if (bat_type == TYPE_str) {
				char **source_base = (char **)data;
				for (j = 0; j < count; j++) {
					const char *ptr = source_base[j];
					if (!ptr) {
						ptr = str_nil;
					}
					if (BUNappend(b, ptr, FALSE) != GDK_SUCCEED) {
						msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
						goto wrapup;
					}
				}
				GDKfree(data);
			} else if (bat_type == TYPE_blob || bat_type == TYPE_sqlblob) {
				cudf_data_blob *source_base = (cudf_data_blob *)data;
				blob *current_blob = NULL;
				size_t current_blob_maxsize = 0;
				for (j = 0; j < count; j++) {
					const cudf_data_blob blob = source_base[j];

					if (!current_blob || current_blob_maxsize < blob.size) {
						if (current_blob) {
							GDKfree(current_blob);
						}
						current_blob_maxsize = blob.size;
						current_blob = GDKmalloc(sizeof(size_t) + blob.size);
						if (!current_blob) {
							msg =
								createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
							goto wrapup;
						}
					}

					if (!blob.data) {
						current_blob->nitems = ~(size_t)0;
					} else {
						current_blob->nitems = blob.size;
						memcpy(&current_blob->data[0], blob.data, blob.size);
					}

					if (BUNappend(b, current_blob, FALSE) != GDK_SUCCEED) {
						if (current_blob) {
							GDKfree(current_blob);
						}
						msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
						goto wrapup;
					}
				}
				if (current_blob) {
					GDKfree(current_blob);
				}
				GDKfree(data);
			} else {
				char **source_base = (char **)data;
				size_t len = 0;
				void *element = NULL;
				for (j = 0; j < count; j++) {
					const char *ptr = source_base[j];
					const void *appended_element;
					if (!ptr || strcmp(ptr, str_nil) == 0) {
						appended_element = (void *)BATatoms[bat_type].atomNull;
					} else {
						if (BATatoms[bat_type].atomFromStr(ptr, &len, &element) ==
							0) {
							msg = createException(MAL, "cudf.eval",
												  "Failed to convert output "
												  "element from string: %s",
												  ptr);
							goto wrapup;
						}
						appended_element = element;
					}
					if (BUNappend(b, appended_element, FALSE) != GDK_SUCCEED) {
						if (element) {
							GDKfree(element);
						}
						msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
						goto wrapup;
					}
				}
				if (element) {
					GDKfree(element);
				}
				GDKfree(data);
			}
		}
		b->tnil = 0;
		b->tnonil = 0;
		b->tkey = 0;
		b->tsorted = 0;
		b->trevsorted = 0;

		// free the output value right now to prevent the internal data from
		// being freed later
		// as the internal data is now part of the bat we just created
		GDKfree(outputs[i]);
		outputs[i] = NULL;

		// return the BAT from the function
		if (isaBatType(getArgType(mb, pci, i))) {
			*getArgReference_bat(stk, pci, i) = b->batCacheid;
			BBPkeepref(b->batCacheid);
		} else {
			// single value return, only for non-grouped aggregations
			BATiter li = bat_iterator(b);
			if (VALinit(&stk->stk[pci->argv[i]], bat_type, BUNtail(li, 0)) ==
				NULL) {
				msg = createException(MAL, "cudf.eval", MAL_MALLOC_FAIL);
				goto wrapup;
			}
			BBPunfix(b->batCacheid);
		}
	}

wrapup:
	// cleanup
	// remove the signal handler, if any was set
	if (sa.sa_sigaction) {
		sigaction(SIGSEGV, &oldsa, NULL);
		sigaction(SIGBUS, &oldsb, NULL);

		memset(&sa, 0, sizeof(sa));
	}
	// clear any mprotected regions
	while (regions) {
		mprotected_region *next = regions->next;
		clear_mprotect(regions->addr, regions->len);
		GDKfree(regions);
		regions = next;
	}
	while (allocated_regions[tid]) {
		allocated_region *next = allocated_regions[tid]->next;
		GDKfree(allocated_regions[tid]);
		allocated_regions[tid] = next;
	}
	// block segfaults and bus errors again after we exit
	(void)pthread_sigmask(SIG_BLOCK, &signal_set, NULL);
	// argument names (input)
	if (args) {
		for (i = 0; i < (size_t)pci->argc; i++) {
			if (args[i]) {
				GDKfree(args[i]);
			}
		}
		GDKfree(args);
	}
	// output names
	if (output_names) {
		for (i = 0; i < (size_t)pci->retc; i++) {
			if (output_names[i]) {
				GDKfree(output_names[i]);
			}
		}
		GDKfree(output_names);
	}
	if (input_bats) {
		for(i = 0; i < input_count + extra_inputs; i++) {
			if (input_bats[i]) {
				BBPunfix(input_bats[i]->batCacheid);
			}
		}
		GDKfree(input_bats);
	}
	// input data
	if (inputs) {
		for (i = 0; i < (size_t)input_count + extra_inputs; i++) {
			if (inputs[i]) {
				if (isaBatType(getArgType(mb, pci, i))) {
					bat_type = getBatType(getArgType(mb, pci, i));
					if (bat_type == TYPE_str || bat_type == TYPE_date ||
						bat_type == TYPE_daytime ||
						bat_type == TYPE_timestamp || bat_type == TYPE_blob ||
						bat_type == TYPE_sqlblob) {
						// have to free input data
						void *data = GetTypeData(bat_type, inputs[i]);
						if (data) {
							GDKfree(data);
						}
					} else if (bat_type > TYPE_str) {
						// this type was converted to individually malloced
						// strings
						// we have to free all the individual strings
						char **data = (char **)GetTypeData(bat_type, inputs[i]);
						size_t count = GetTypeCount(bat_type, inputs[i]);
						for (j = 0; j < count; j++) {
							if (data[j]) {
								GDKfree(data[j]);
							}
						}
						if (data) {
							GDKfree(data);
						}
					}
				}
				GDKfree(inputs[i]);
			}
		}
		GDKfree(inputs);
	}
	// output data
	if (outputs) {
		for (i = 0; i < (size_t)output_count; i++) {
			bat_type = isaBatType(getArgType(mb, pci, i))
							   ? getBatType(getArgType(mb, pci, i))
							   : getArgType(mb, pci, i);
			if (outputs[i]) {
				void* b = GetTypeBat(bat_type, outputs[i]);
				if (b) {
					BBPunfix(((BAT*)b)->batCacheid);
				} else {
					void *data = GetTypeData(bat_type, outputs[i]);
					if (data) {
						GDKfree(data);
					}
				}
				GDKfree(outputs[i]);
			}
		}
		GDKfree(outputs);
	}
	if (function_parameters) {
		GDKfree(function_parameters);
	}
	// close the file handle
	if (f) {
		fclose(f);
	}
	// close the dll
	if (handle) {
		dlclose(handle);
	}
	// close the compiler stream
	if (compiler) {
		pclose(compiler);
	}
	if (extra_cflags) {
		GDKfree(extra_cflags);
	}
	if (extra_ldflags) {
		GDKfree(extra_ldflags);
	}
	return msg;
}

static const char *GetTypeName(int type)
{
	const char *tpe = NULL;
	if (type == TYPE_bit || type == TYPE_bte) {
		tpe = "bte";
	} else if (type == TYPE_sht) {
		tpe = "short";
	} else if (type == TYPE_int) {
		tpe = "int";
	} else if (type == TYPE_oid) {
		tpe = "oid";
	} else if (type == TYPE_lng) {
		tpe = "lng";
	} else if (type == TYPE_flt) {
		tpe = "flt";
	} else if (type == TYPE_dbl) {
		tpe = "dbl";
	} else if (type == TYPE_str) {
		tpe = "str";
	} else if (type == TYPE_date) {
		tpe = "date";
	} else if (type == TYPE_daytime) {
		tpe = "time";
	} else if (type == TYPE_timestamp) {
		tpe = "timestamp";
	} else if (type == TYPE_blob || type == TYPE_sqlblob) {
		tpe = "blob";
	} else {
		// unsupported type: string
		tpe = "str";
	}
	return tpe;
}

void *GetTypeData(int type, void *struct_ptr)
{
	void *data = NULL;

	if (type == TYPE_bit || type == TYPE_bte) {
		data = ((struct cudf_data_struct_bte *)struct_ptr)->data;
	} else if (type == TYPE_sht) {
		data = ((struct cudf_data_struct_sht *)struct_ptr)->data;
	} else if (type == TYPE_int) {
		data = ((struct cudf_data_struct_int *)struct_ptr)->data;
	} else if (type == TYPE_oid) {
		data = ((struct cudf_data_struct_oid *)struct_ptr)->data;
	} else if (type == TYPE_lng) {
		data = ((struct cudf_data_struct_lng *)struct_ptr)->data;
	} else if (type == TYPE_flt) {
		data = ((struct cudf_data_struct_flt *)struct_ptr)->data;
	} else if (type == TYPE_dbl) {
		data = ((struct cudf_data_struct_dbl *)struct_ptr)->data;
	} else if (type == TYPE_str) {
		data = ((struct cudf_data_struct_str *)struct_ptr)->data;
	} else if (type == TYPE_date) {
		data = ((struct cudf_data_struct_date *)struct_ptr)->data;
	} else if (type == TYPE_daytime) {
		data = ((struct cudf_data_struct_time *)struct_ptr)->data;
	} else if (type == TYPE_timestamp) {
		data = ((struct cudf_data_struct_timestamp *)struct_ptr)->data;
	} else if (type == TYPE_blob || type == TYPE_sqlblob) {
		data = ((struct cudf_data_struct_blob *)struct_ptr)->data;
	} else {
		// unsupported type: string
		data = ((struct cudf_data_struct_str *)struct_ptr)->data;
	}
	return data;
}

void *GetTypeBat(int type, void *struct_ptr)
{
	void *bat = NULL;

	if (type == TYPE_bit || type == TYPE_bte) {
		bat = ((struct cudf_data_struct_bte *)struct_ptr)->bat;
	} else if (type == TYPE_sht) {
		bat = ((struct cudf_data_struct_sht *)struct_ptr)->bat;
	} else if (type == TYPE_int) {
		bat = ((struct cudf_data_struct_int *)struct_ptr)->bat;
	} else if (type == TYPE_oid) {
		bat = ((struct cudf_data_struct_oid *)struct_ptr)->bat;
	} else if (type == TYPE_lng) {
		bat = ((struct cudf_data_struct_lng *)struct_ptr)->bat;
	} else if (type == TYPE_flt) {
		bat = ((struct cudf_data_struct_flt *)struct_ptr)->bat;
	} else if (type == TYPE_dbl) {
		bat = ((struct cudf_data_struct_dbl *)struct_ptr)->bat;
	} else if (type == TYPE_str) {
		bat = ((struct cudf_data_struct_str *)struct_ptr)->bat;
	} else if (type == TYPE_date) {
		bat = ((struct cudf_data_struct_date *)struct_ptr)->bat;
	} else if (type == TYPE_daytime) {
		bat = ((struct cudf_data_struct_time *)struct_ptr)->bat;
	} else if (type == TYPE_timestamp) {
		bat = ((struct cudf_data_struct_timestamp *)struct_ptr)->bat;
	} else if (type == TYPE_blob || type == TYPE_sqlblob) {
		bat = ((struct cudf_data_struct_blob *)struct_ptr)->bat;
	} else {
		// unsupported type: string
		bat = ((struct cudf_data_struct_str *)struct_ptr)->bat;
	}
	return bat;
}

size_t GetTypeCount(int type, void *struct_ptr)
{
	size_t count = 0;
	if (type == TYPE_bit || type == TYPE_bte) {
		count = ((struct cudf_data_struct_bte *)struct_ptr)->count;
	} else if (type == TYPE_sht) {
		count = ((struct cudf_data_struct_sht *)struct_ptr)->count;
	} else if (type == TYPE_int) {
		count = ((struct cudf_data_struct_int *)struct_ptr)->count;
	} else if (type == TYPE_oid) {
		count = ((struct cudf_data_struct_oid *)struct_ptr)->count;
	} else if (type == TYPE_lng) {
		count = ((struct cudf_data_struct_lng *)struct_ptr)->count;
	} else if (type == TYPE_flt) {
		count = ((struct cudf_data_struct_flt *)struct_ptr)->count;
	} else if (type == TYPE_dbl) {
		count = ((struct cudf_data_struct_dbl *)struct_ptr)->count;
	} else if (type == TYPE_str) {
		count = ((struct cudf_data_struct_str *)struct_ptr)->count;
	} else if (type == TYPE_date) {
		count = ((struct cudf_data_struct_date *)struct_ptr)->count;
	} else if (type == TYPE_daytime) {
		count = ((struct cudf_data_struct_time *)struct_ptr)->count;
	} else if (type == TYPE_timestamp) {
		count = ((struct cudf_data_struct_timestamp *)struct_ptr)->count;
	} else if (type == TYPE_blob || type == TYPE_sqlblob) {
		count = ((struct cudf_data_struct_blob *)struct_ptr)->count;
	} else {
		// unsupported type: string
		count = ((struct cudf_data_struct_str *)struct_ptr)->count;
	}
	return count;
}

void data_from_date(date d, cudf_data_date *ptr)
{
	int day, month, year;
	MTIMEfromdate(d, &day, &month, &year);
	ptr->day = day;
	ptr->month = month;
	ptr->year = year;
}

date date_from_data(cudf_data_date *ptr)
{
	return MTIMEtodate(ptr->day, ptr->month, ptr->year);
}

void data_from_time(daytime d, cudf_data_time *ptr)
{
	int hour, min, sec, msec;
	MTIMEfromtime(d, &hour, &min, &sec, &msec);
	ptr->hours = hour;
	ptr->minutes = min;
	ptr->seconds = sec;
	ptr->ms = msec;
}

daytime time_from_data(cudf_data_time *ptr)
{
	return MTIMEtotime(ptr->hours, ptr->minutes, ptr->seconds, ptr->ms);
}

void data_from_timestamp(timestamp d, cudf_data_timestamp *ptr)
{
	data_from_date(d.payload.p_days, &ptr->date);
	data_from_time(d.payload.p_msecs, &ptr->time);
}

timestamp timestamp_from_data(cudf_data_timestamp *ptr)
{
	timestamp d;
	d.payload.p_days = date_from_data(&ptr->date);
	d.payload.p_msecs = time_from_data(&ptr->time);
	return d;
}

int date_is_null(cudf_data_date value)
{
	cudf_data_date null_value;
	data_from_date(date_nil, &null_value);
	return value.year == null_value.year && value.month == null_value.month &&
		   value.day == null_value.day;
}

int time_is_null(cudf_data_time value)
{
	cudf_data_time null_value;
	data_from_time(daytime_nil, &null_value);
	return value.hours == null_value.hours &&
		   value.minutes == null_value.minutes &&
		   value.seconds == null_value.seconds && value.ms == null_value.ms;
}

int timestamp_is_null(cudf_data_timestamp value)
{
	return ts_isnil(timestamp_from_data(&value));
}

int str_is_null(char *value) { return value == NULL; }

int blob_is_null(cudf_data_blob value) { return value.data == NULL; }

void blob_initialize(struct cudf_data_struct_blob *self,
								 size_t count) {
	self->count = count;
	self->data = jump_GDK_malloc(count * sizeof(self->null_value));
	memset(self->data, 0, count * sizeof(self->null_value));
}
