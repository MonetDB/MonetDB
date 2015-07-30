/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2008-2015 MonetDB B.V.
 */

/*
 * H. Muehleisen, M. Raasveldt
 * Inverse RAPI
 */

#include "embedded.h"

#include "monetdb_config.h"
#include "monet_options.h"
#include "mal.h"
#include "mal_client.h"
#include "mal_linker.h"
#include "msabaoth.h"
#include "sql_scenario.h"

typedef str (*SQLstatementIntern_ptr_tpe)(Client, str*, str, bit, bit, res_table**);
SQLstatementIntern_ptr_tpe SQLstatementIntern_ptr = NULL;
typedef void (*res_table_destroy_ptr_tpe)(res_table *t);
res_table_destroy_ptr_tpe res_table_destroy_ptr = NULL;

static bit monetdb_embedded_initialized = 0;
static MT_Lock monetdb_embedded_lock;

static void* lookup_function(char* lib, char* func) {
	void *dl, *fun;
	dl = mdlopen(lib, RTLD_NOW | RTLD_GLOBAL);
	if (dl == NULL) {
		return NULL;
	}
	fun = dlsym(dl, func);
	dlclose(dl);
	return fun;
}

int monetdb_startup(char* dir, char silent) {
	opt *set = NULL;
	int setlen = 0;
	int retval = -1;
	void* res = NULL;
	char mod_path[1000];

	MT_lock_init(&monetdb_embedded_lock, "monetdb_embedded_lock");
	MT_lock_set(&monetdb_embedded_lock, "monetdb.startup");
	if (monetdb_embedded_initialized) goto cleanup;

	setlen = mo_builtin_settings(&set);
	setlen = mo_add_option(&set, setlen, opt_cmdline, "gdk_dbpath", dir);
	if (GDKinit(set, setlen) == 0)  goto cleanup;

	snprintf(mod_path, 1000, "%s/../lib/monetdb5", BINDIR);
	GDKsetenv("monet_mod_path", mod_path);
	GDKsetenv("mapi_disable", "true");
	GDKsetenv("max_clients", "0");

	if (silent) THRdata[0] = stream_blackhole_create();
	msab_dbpathinit(GDKgetenv("gdk_dbpath"));
	if (mal_init() != 0) goto cleanup;
	if (silent) mal_clients[0].fdout = THRdata[0];

	// This dynamically looks up functions, because the library containing them is loaded at runtime.
	SQLstatementIntern_ptr = (SQLstatementIntern_ptr_tpe) lookup_function("lib_sql",  "SQLstatementIntern");
	res_table_destroy_ptr  = (res_table_destroy_ptr_tpe)  lookup_function("libstore", "res_table_destroy");
	if (SQLstatementIntern_ptr == NULL || res_table_destroy_ptr == NULL) goto cleanup;

	monetdb_embedded_initialized = true;
	// sanity check, run a SQL query
	if (monetdb_query("SELECT * FROM tables;", res) < 0) {
		monetdb_embedded_initialized = false;
		goto cleanup;
	}
	retval = 0;

cleanup:
	mo_free_options(set, setlen);
	MT_lock_unset(&monetdb_embedded_lock, "monetdb.startup");
	return retval;
}

// TODO: This does stop working on the first failing query, do something about this
char* monetdb_query(char* query, void** result) {
	str res;
	Client c = &mal_clients[0];
	if (!monetdb_embedded_initialized) {
		fprintf(stderr, "Embedded MonetDB is not started.\n");
		return NULL;
	}
	res = (*SQLstatementIntern_ptr)(c, &query, "name", 1, 0, (res_table **) result);
	return res;
}

void monetdb_cleanup_result(void* output) {
	(*res_table_destroy_ptr)((res_table*) output);
}

#define BAT_TO_INTSXP(bat,tpe,retsxp)						\
	do {													\
		tpe v;	size_t j;									\
		retsxp = PROTECT(NEW_INTEGER(BATcount(bat)));		\
		for (j = 0; j < BATcount(bat); j++) {				\
			v = ((tpe*) Tloc(bat, BUNfirst(bat)))[j];		\
			if ( v == tpe##_nil)							\
				INTEGER_POINTER(retsxp)[j] = 	NA_INTEGER; \
			else											\
				INTEGER_POINTER(retsxp)[j] = 	(int)v;		\
		}													\
	} while (0)

#define BAT_TO_REALSXP(bat,tpe,retsxp)						\
	do {													\
		tpe v; size_t j;									\
		retsxp = PROTECT(NEW_NUMERIC(BATcount(bat)));		\
		for (j = 0; j < BATcount(bat); j++) {				\
			v = ((tpe*) Tloc(bat, BUNfirst(bat)))[j];		\
			if ( v == tpe##_nil)							\
				NUMERIC_POINTER(retsxp)[j] = 	NA_REAL;	\
			else											\
				NUMERIC_POINTER(retsxp)[j] = 	(double)v;	\
		}													\
	} while (0)


SEXP monetdb_query_R(SEXP query) {
	res_table* output = NULL;
	char* err = monetdb_query((char*)CHAR(STRING_ELT(query, 0)), (void**)&output);
	if (err != NULL) { // there was an error
		return ScalarString(mkCharCE(err, CE_UTF8));
	}
	if (output && output->nr_cols > 0) {
		int i;
		SEXP retlist, names, varvalue = R_NilValue;
		retlist = PROTECT(allocVector(VECSXP, output->nr_cols));
		names = PROTECT(NEW_STRING(output->nr_cols));

		for (i = 0; i < output->nr_cols; i++) {
			res_col col = output->cols[i];
			BAT* b = BATdescriptor(col.b);
			SET_STRING_ELT(names, i, mkCharCE(output->cols[i].name, CE_UTF8));

			switch (ATOMstorage(getColumnType(b->T->type))) {
				case TYPE_bte:
					BAT_TO_INTSXP(b, bte, varvalue);
					break;
				case TYPE_sht:
					BAT_TO_INTSXP(b, sht, varvalue);
					break;
				case TYPE_int:
					BAT_TO_INTSXP(b, int, varvalue);
					break;
				case TYPE_flt:
					BAT_TO_REALSXP(b, flt, varvalue);
					break;
				case TYPE_dbl:
					BAT_TO_REALSXP(b, dbl, varvalue);
					break;
				case TYPE_lng: /* R's integers are stored as int, so we cannot be sure long will fit */
					BAT_TO_REALSXP(b, lng, varvalue);
					break;
				case TYPE_str: { // there is only one string type, thus no macro here
					BUN p = 0, q = 0, j = 0;
					BATiter li;
					li = bat_iterator(b);
					varvalue = PROTECT(NEW_STRING(BATcount(b)));
					BATloop(b, p, q) {
						const char *t = (const char *) BUNtail(li, p);
						if (ATOMcmp(TYPE_str, t, str_nil) == 0) {
							SET_STRING_ELT(varvalue, j, NA_STRING);
						} else {
							SET_STRING_ELT(varvalue, j, mkCharCE(t, CE_UTF8));
						}
						j++;
					}
				} 	break;
				default:
					// no clue what type to consider
					fprintf(stderr, "unknown argument type");
					return NULL;
			}

			SET_VECTOR_ELT(retlist, i, varvalue);
		}
		SET_NAMES(retlist, names);
		UNPROTECT(output->nr_cols + 2);

		monetdb_cleanup_result(output);
		return retlist;
	}
	return ScalarLogical(1);
}

SEXP monetdb_startup_R(SEXP dirsexp, SEXP silentsexp) {
	if (!IS_CHARACTER(dirsexp) || !IS_LOGICAL(silentsexp)) {
		return ScalarInteger(-1);
	}
	const char* dir = CHAR(STRING_ELT(dirsexp, 0));
	char silent = LOGICAL(silentsexp)[0];
	int res = monetdb_startup((char*) dir, silent);
	return ScalarInteger(res);
}
