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
typedef str (*SQLautocommit_ptr_tpe)(Client, mvc*);
SQLautocommit_ptr_tpe SQLautocommit_ptr = NULL;
typedef str (*SQLinitClient_ptr_tpe)(Client);
SQLinitClient_ptr_tpe SQLinitClient_ptr = NULL;
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
	if (GDKinit(set, setlen) == 0) {
		retval = -2;
		goto cleanup;
	}

	snprintf(mod_path, 1000, "%s/../lib/monetdb5", BINDIR);
	GDKsetenv("monet_mod_path", mod_path);
	GDKsetenv("mapi_disable", "true");
	GDKsetenv("max_clients", "0");

	if (silent) THRdata[0] = stream_blackhole_create();
	msab_dbpathinit(GDKgetenv("gdk_dbpath"));
	if (mal_init() != 0) {
		retval = -3;
		goto cleanup;
	}
	if (silent) mal_clients[0].fdout = THRdata[0];

	// This dynamically looks up functions, because the library containing them is loaded at runtime.
	SQLstatementIntern_ptr = (SQLstatementIntern_ptr_tpe) lookup_function("lib_sql",  "SQLstatementIntern");
	SQLautocommit_ptr = (SQLautocommit_ptr_tpe) lookup_function("lib_sql",  "SQLautocommit");
	SQLinitClient_ptr = (SQLinitClient_ptr_tpe) lookup_function("lib_sql",  "SQLinitClient");
	res_table_destroy_ptr  = (res_table_destroy_ptr_tpe)  lookup_function("libstore", "res_table_destroy");
	if (SQLstatementIntern_ptr == NULL || SQLautocommit_ptr == NULL ||
			SQLinitClient_ptr == NULL || res_table_destroy_ptr == NULL) {
		retval = -4;
		goto cleanup;
	}
	// call this, otherwise c->sqlcontext is empty
	(*SQLinitClient_ptr)(&mal_clients[0]);
	((backend *) mal_clients[0].sqlcontext)->mvc->session->auto_commit = 1;
	monetdb_embedded_initialized = true;
	// sanity check, run a SQL query
	if (monetdb_query("SELECT * FROM tables;", res) != NULL) {
		monetdb_embedded_initialized = false;
		retval = -5;
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
	str res = MAL_SUCCEED;
	Client c = &mal_clients[0];
	mvc* m = ((backend *) c->sqlcontext)->mvc;
	if (!monetdb_embedded_initialized) {
		fprintf(stderr, "Embedded MonetDB is not started.\n");
		return NULL;
	}

	while (*query == ' ' || *query == '\t') query++;
	if (strncasecmp(query, "START", 5) == 0) { // START TRANSACTION
		m->session->auto_commit = 0;
	}
	else if (strncasecmp(query, "ROLLBACK", 8) == 0) {
		m->session->status = -1;
		m->session->auto_commit = 1;
	}
	else if (strncasecmp(query, "COMMIT", 6) == 0) {
		m->session->auto_commit = 1;
	}
	else if (strncasecmp(query, "SHIBBOLEET", 10) == 0) {
		res = GDKstrdup("\x46\x6f\x72\x20\x69\x6d\x6d\x65\x64\x69\x61\x74\x65\x20\x74\x65\x63\x68\x6e\x69\x63\x61\x6c\x20\x73\x75\x70\x70\x6f\x72\x74\x20\x63\x61\x6c\x6c\x20\x2b\x33\x31\x20\x32\x30\x20\x35\x39\x32\x20\x34\x30\x33\x39");
	}
	else if (m->session->status < 0 && m->session->auto_commit ==0){
		res = GDKstrdup("Current transaction is aborted (please ROLLBACK)");
	} else {
		res = (*SQLstatementIntern_ptr)(c, &query, "name", 1, 0, (res_table **) result);
	}

	(*SQLautocommit_ptr)(c, m);
	return res;
}

char* monetdb_append(char* schema, char* table, append_data *data, int ncols) {
	int i;
	MalBlkRecord mb;
	MalStack     stk;
	InstrRecord  pci;
	str res = MAL_SUCCEED;

	assert(table != NULL && append_data != NULL && ncols > 0);
	for (i = 0; i < 6; i++) {
		pci.argv[i] = i;
	}

	stk.stk[2].val.sval = schema;
	stk.stk[3].val.sval = table;

	for (i=0; i <ncols; i++) {
		append_data ad = data[i];
		stk.stk[4].val.sval = ad.colname;
		stk.stk[5].vtype = TYPE_bat;
		stk.stk[5].val.bval = ad.batid;

		res = mvc_append_wrap(&mal_clients[0], mb, stk, pci);
		if (res != NULL) {
			break;
		}
	}
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


#define SXP_TO_BAT(tpe,access_fun,na_check)								\
	do {																\
		tpe *p, prev = tpe##_nil;										\
		b = BATnew(TYPE_void, TYPE_##tpe, cnt, TRANSIENT);				\
		BATseqbase(b, 0); b->T->nil = 0; b->T->nonil = 1; b->tkey = 0;	\
		b->tsorted = 1; b->trevsorted = 1;								\
		p = (tpe*) Tloc(b, BUNfirst(b));								\
		for( j =0; j< (int) cnt; j++, p++){								\
			*p = (tpe) access_fun(ret_col)[j];							\
			if (na_check){ b->T->nil = 1; 	b->T->nonil = 0; 	*p= tpe##_nil;} \
			if (j > 0){													\
				if ( *p > prev && b->trevsorted){						\
					b->trevsorted = 0;									\
					if (*p != prev +1) b->tdense = 0;					\
				} else													\
					if ( *p < prev && b->tsorted){						\
						b->tsorted = 0;									\
						b->tdense = 0;									\
					}													\
			}															\
			prev = *p;													\
		}																\
		BATsetcount(b,cnt);												\
		BATsettrivprop(b);												\
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
	const char* dir = NULL;
	char silent = 0;
	int res = 0;
	if (!IS_CHARACTER(dirsexp) || !IS_LOGICAL(silentsexp)) {
		return ScalarInteger(-1);
	}
	dir = CHAR(STRING_ELT(dirsexp, 0));
	silent = LOGICAL(silentsexp)[0];
	res = monetdb_startup((char*) dir, silent);
	return ScalarInteger(res);
}

str monetdb_get_columns(char* schema_name, char *table_name, int *column_count, char ***column_names, int **column_types) {
	Client c = &mal_clients[0];
	mvc *m;
	sql_schema *s;
	sql_table *t;
	char *msg = MAL_SUCCEED;
	int columns;
	int i;

	assert(column_count != NULL && column_names != NULL && column_types != NULL);

	if ((msg = getSQLContext(c, NULL, &m, NULL)) != NULL)
		return msg;

	s = mvc_bind_schema(m, schema_name);
	if (s == NULL)
		msg = createException(MAL, "embedded", "Missing schema!");
	t = mvc_bind_table(m, s, table_name);
	if (t == NULL)
		msg = createException(MAL, "embedded", "Could not find table %s", table_name);

	columns = t->columns.set->cnt;
	*column_count = columns;
	*column_names = GDKzalloc(sizeof(char*) * columns);
	*column_types = GDKzalloc(sizeof(int) * columns);

	if (*column_names == NULL || *column_types == NULL) {
		return MAL_MALLOC_FAIL;
	}

	for(i = 0; i < columns; i++) {
		int acol = ((sql_column*)t->columns.set->h->data)[i].colnr;
		*column_names[acol] = ((sql_base*)t->columns.set->h->data)[i].name;
		*column_types[acol] = ((sql_column*)t->columns.set->h->data)[i].type.type->localtype;
	}
	return msg;
}

SEXP monetdb_append_R(SEXP schemasexp, SEXP namesexp, SEXP tabledatasexp) {
	const char *schema = NULL, *name = NULL;
	str msg;
	int ncols, nrows, i, j;
	BAT *b;
	BUN cnt;
	append_data *ad = NULL;
	int t_column_count;
	char** t_column_names = NULL;
	int* t_column_types = NULL;

	if (!IS_CHARACTER(schemasexp) || !IS_CHARACTER(namesexp)) {
		return ScalarInteger(-1);
	}
	schema = CHAR(STRING_ELT(schemasexp, 0));
	name = CHAR(STRING_ELT(namesexp, 0));

	ncols = LENGTH(tabledatasexp);
	nrows = LENGTH(VECTOR_ELT(tabledatasexp, 0));

	msg = monetdb_get_columns(schema, name, &t_column_count, &t_column_names, &t_column_types);
	if (msg != MAL_SUCCEED)
		goto wrapup;

	if (t_column_count != ncols) {
		msg = GDKstrdup("Unequal number of columns"); // TODO: add counts here
		goto wrapup;
	}

	ad = GDKmalloc(ncols * sizeof(append_data));

	for (i = 0; i < ncols; i++) {
		SEXP ret_col = VECTOR_ELT(tabledatasexp, i);
		int bat_type = t_column_types[i];
		cnt = (BUN) nrows;

		// hand over the vector into a BAT
		switch (bat_type) {
		case TYPE_int: {
			if (!IS_INTEGER(ret_col)) {
				msg =
					createException(MAL, "rapi.eval",
									"wrong R column type for column %d, expected INTeger, got %s.",
									i, rtypename(TYPEOF(ret_col)));
				goto wrapup;
			}
			SXP_TO_BAT(int, INTEGER_POINTER, *p==NA_INTEGER);
			break;
		}
		}
		ad[i].colname = t_column_names[i];
		ad[i].batid = b->batCacheid;
	}

	monetdb_append(schema, name, ad, ncols);
	wrapup:
		if (t_column_names != NULL) {
			GDKfree(t_column_names);
		}
		if (t_column_types != NULL) {
			GDKfree(t_column_types);
		}
		return ScalarString(mkCharCE(msg, CE_UTF8));
}

