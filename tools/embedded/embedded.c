#include <stdio.h>
#include "monetdb_config.h"

#include "embedded.h"
#include <stdio.h>
#include <errno.h>
#include <string.h> /* strerror */
#include <locale.h>
#include "monet_options.h"
#include "mal.h"
#include "mal_session.h"
#include "mal_import.h"
#include "mal_client.h"
#include "mal_function.h"
#include "mal_authorize.h"
#include "mal_linker.h"

#include "mutils.h"
#include "msabaoth.h"
#include "sql_catalog.h"
#include "sql_execute.h"

typedef str (*SQLstatementIntern_ptr_tpe)(Client, str*, str, bit, bit, res_table**);
SQLstatementIntern_ptr_tpe SQLstatementIntern_ptr = NULL;
typedef void (*res_table_destroy_ptr_tpe)(res_table *t);
res_table_destroy_ptr_tpe res_table_destroy_ptr = NULL;


int monetdb_startup(char* dir) {
	opt *set = NULL;
	int setlen = 0;
	void *dl;

	//GDKsetenv("gdk_dbpath", dir);
	//BBPaddfarm(dir, (1 << PERSISTENT) | (1 << TRANSIENT));
	setlen = mo_builtin_settings(&set);
	if (GDKinit(set, setlen) == 0) {
		return -1;
	}
	msab_dbpathinit(dir);

	// TODO: we should be able to set this correctly from R
	GDKsetenv("monet_mod_path", "/Users/hannes/Library/R/3.2/library/MonetDB/install/lib/monetdb5/");
	GDKsetenv("mapi_disable", "true");

	if (mal_init() != 0) {
		return -2;
	}

	// This dynamically looks up the SQLstatementIntern function, because the library containing it is loaded at runtime.
	dl = mdlopen("lib_sql", RTLD_NOW | RTLD_GLOBAL);
	if (dl == NULL) {
		return -3;
	}
	SQLstatementIntern_ptr = (SQLstatementIntern_ptr_tpe) dlsym(dl, "SQLstatementIntern");
	dlclose(dl);

	dl = mdlopen("libstore", RTLD_NOW | RTLD_GLOBAL);
	if (dl == NULL) {
		return -3;
	}
	res_table_destroy_ptr = (res_table_destroy_ptr_tpe) dlsym(dl, "res_table_destroy");
	dlclose(dl);

	return 0;
}

int monetdb_shutdown(void){
	return 42;
}

void* monetdb_query(char* query) {
	str res;
	res_table* output = NULL;
	Client c = &mal_clients[0];
	res = (*SQLstatementIntern_ptr)(c, &query, "name", 1, 0, &output);
	if (output) {
		return output;
	}
	if (res != MAL_SUCCEED){
		fprintf(stderr, "sql err: %s\n", res);
	}
	return NULL;
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
	SEXP retlist = R_NilValue;
	res_table* output = monetdb_query((char*)CHAR(STRING_ELT(query, 0)));
	SEXP varvalue = R_NilValue;
	if (output && output->nr_cols > 0) {
		int i;
		retlist = PROTECT(allocVector(VECSXP, output->nr_cols));

		for (i = 0; i < output->nr_cols; i++) {
			res_col col = output->cols[i];
			BAT* b = BATdescriptor(col.b);

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
					// TODO: cleanup result set
					return NULL;
			}

			SET_VECTOR_ELT(retlist, i, varvalue);
			// TODO: create a separate names vector and set names (names in cols struct)
		}
		UNPROTECT(output->nr_cols + 1);
		monetdb_cleanup_result(output);
	}
	return retlist;

}

SEXP monetdb_startup_R(SEXP dirsexp) {
	const char* dir = CHAR(STRING_ELT(dirsexp, 0));
	int res = monetdb_startup((char*) dir);
	SEXP retsxp = PROTECT(NEW_INTEGER(1));
	INTEGER_POINTER(retsxp)[0] = res;
	UNPROTECT(1);
	return retsxp;
}
