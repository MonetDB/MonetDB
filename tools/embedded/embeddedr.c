#include "monetdb_config.h"

#ifdef HAVE_EMBEDDED_R
#include "embeddedr.h"
#include "R_ext/Random.h"
#include "monet_options.h"
#include "mal.h"
#include "mmath.h"
#include "mal_client.h"
#include "mal_linker.h"
#include "msabaoth.h"
#include "sql_scenario.h"
#include "gdk_utils.h"

int embedded_r_rand(void) {
	int ret;
	ret = (int) (unif_rand() * RAND_MAX);
	return ret;
}


/* we need the BAT-SEXP-BAT conversion in two places, here and in RAPI */
#include "converters.c.h"

SEXP monetdb_query_R(SEXP connsexp, SEXP querysexp, SEXP executesexp, SEXP resultconvertsexp) {
	res_table* output = NULL;
	char* err = NULL;
	GetRNGstate();
	err = monetdb_query(R_ExternalPtrAddr(connsexp),
			(char*)CHAR(STRING_ELT(querysexp, 0)), LOGICAL(executesexp)[0], (void**)&output);
	if (err) { // there was an error
		PutRNGstate();
		return ScalarString(mkCharCE(err, CE_UTF8));
	}
	if (output && output->nr_cols > 0) {
		int i, ncols = output->nr_cols;
		SEXP retlist, names, varvalue = R_NilValue;
		retlist = PROTECT(allocVector(VECSXP, ncols));
		names = PROTECT(NEW_STRING(ncols));
		SET_ATTR(retlist, install("__rows"),
			Rf_ScalarReal(BATcount(BATdescriptor(output->cols[0].b))));
		for (i = 0; i < ncols; i++) {
			BAT* b = BATdescriptor(output->cols[i].b);
			if (!LOGICAL(resultconvertsexp)[0]) {
				BATsetcount(b, 0); // hehe
			}
			if (!(varvalue = bat_to_sexp(b))) {
				UNPROTECT(i + 3);
				PutRNGstate();
				return ScalarString(mkCharCE("Conversion error", CE_UTF8));
			}
			SET_STRING_ELT(names, i, mkCharCE(output->cols[i].name, CE_UTF8));
			SET_VECTOR_ELT(retlist, i, varvalue);
		}
		monetdb_cleanup_result(R_ExternalPtrAddr(connsexp), output);
		SET_NAMES(retlist, names);
		UNPROTECT(ncols + 2);
		PutRNGstate();
		return retlist;
	}
	PutRNGstate();
	return ScalarLogical(1);
}

SEXP monetdb_startup_R(SEXP dbdirsexp, SEXP silentsexp, SEXP sequentialsexp) {
	char* res = NULL;

	if (monetdb_embedded_initialized) {
		return ScalarLogical(0);
	}

#if defined(WIN32) && !defined(_WIN64)
	Rf_warning("MonetDBLite running in a 32-Bit Windows. This is not recommended.");
#endif
	GetRNGstate();
	res = monetdb_startup((char*) CHAR(STRING_ELT(dbdirsexp, 0)),
		LOGICAL(silentsexp)[0], LOGICAL(sequentialsexp)[0]);
	PutRNGstate();
	if (!res) {
		return ScalarLogical(1);
	}  else {
		return ScalarString(mkCharCE(res, CE_UTF8));
	}
}


SEXP monetdb_append_R(SEXP connsexp, SEXP schemasexp, SEXP namesexp, SEXP tabledatasexp) {
	const char *schema = NULL, *name = NULL;
	str msg;
	int col_ct, i;
	BAT *b = NULL;
	append_data *ad = NULL;
	int t_column_count;
	char** t_column_names = NULL;
	int* t_column_types = NULL;

	if (!IS_CHARACTER(schemasexp) || !IS_CHARACTER(namesexp)) {
		return ScalarInteger(-1);
	}
	GetRNGstate();
	schema = CHAR(STRING_ELT(schemasexp, 0));
	name = CHAR(STRING_ELT(namesexp, 0));
	col_ct = LENGTH(tabledatasexp);

	msg = monetdb_get_columns(R_ExternalPtrAddr(connsexp), schema, name, &t_column_count, &t_column_names, &t_column_types);
	if (msg != MAL_SUCCEED)
		goto wrapup;

	if (t_column_count != col_ct) {
		msg = GDKstrdup("Unequal number of columns"); // TODO: add counts here
		goto wrapup;
	}

	ad = GDKmalloc(col_ct * sizeof(append_data));
	assert(ad);

	for (i = 0; i < col_ct; i++) {
		SEXP ret_col = VECTOR_ELT(tabledatasexp, i);
		int bat_type = t_column_types[i];
		b = sexp_to_bat(ret_col, bat_type);
		if (b == NULL) {
			msg = createException(MAL, "embedded", "Could not convert column %i %s to type %i ", i, t_column_names[i], bat_type);
			goto wrapup;
		}
		ad[i].colname = t_column_names[i];
		ad[i].batid = b->batCacheid;
	}

	msg = monetdb_append(R_ExternalPtrAddr(connsexp), schema, name, ad, col_ct);

	wrapup:
		PutRNGstate();
		if (t_column_names) {
			GDKfree(t_column_names);
		}
		if (t_column_types) {
			GDKfree(t_column_types);
		}
		if (!msg) {
			return ScalarLogical(1);
		}
		return ScalarString(mkCharCE(msg, CE_UTF8));
}


SEXP monetdb_connect_R(void) {
	void* llconn = monetdb_connect();
	SEXP conn = NULL;
	if (!llconn) {
		error("Could not create connection.");
	}
	conn = PROTECT(R_MakeExternalPtr(llconn, R_NilValue, R_NilValue));
	R_RegisterCFinalizer(conn, (void (*)(SEXP)) monetdb_disconnect_R);
	UNPROTECT(1);
	return conn;
}

SEXP monetdb_disconnect_R(SEXP connsexp) {
	void* addr = R_ExternalPtrAddr(connsexp);
	if (addr) {
		monetdb_disconnect(addr);
		R_ClearExternalPtr(connsexp);
	}
	return R_NilValue;
}

SEXP monetdb_shutdown_R(void) {
	monetdb_shutdown();
	return R_NilValue;
}
#endif
