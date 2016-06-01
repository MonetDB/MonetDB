/* Ghetto programming warning. */

#include "monetdb_config.h"
#include "mal.h"
#include "mal_stack.h"
#include "mal_linker.h"
#include "gdk_utils.h"
#include "gdk.h"
#include "mmath.h"
#include "sql_catalog.h"
#include "sql_execute.h"
#include "fwf.h"

str fwf_load(mvc *m, char* schema, char* table, char* filename, BAT *widths, char padding) {
	FILE *f;
	size_t fsize;
    struct stat stb;
    char* fptr, *fcur = NULL;
    size_t i;
    size_t width_sum = 0;
	sql_schema *s;
	sql_table *t;
	node *n;
	str msg = MAL_SUCCEED;
	size_t ncol;
	size_t approx_nrows;
	size_t row = 0;
	BAT** appends;
	char* line = NULL;
	int *widths_ptr = (int*) widths->T->heap.base;

	s = mvc_bind_schema(m, schema);
	if (s == NULL)
		throw(SQL, "sql.append", "Schema missing");
	t = mvc_bind_table(m, s, table);
	if (t == NULL)
		throw(SQL, "sql.append", "Table missing");

	f = fopen(filename, "r");
	if (fstat(fileno(f), &stb) != 0) {
		msg = createException(MAL, "fwf.load", "Could not stat file");
		goto cleanup;
    }
    fsize = (size_t) stb.st_size;
    fptr = GDKmmap(filename, MMAP_READ, fsize);
	if (!fptr) {
		msg = createException(MAL, "fwf.load", "Could not map file");
		goto cleanup;
	}

	ncol = t->columns.set->cnt;
	if (BATcount(widths) != ncol) {
		msg = createException(MAL, "fwf.load", "Incorrect number of widths supplied");
		goto cleanup;
	}

	appends = malloc(sizeof(BAT*)*ncol);
	if (!appends) {
		msg = createException(MAL, "fwf.load", MAL_MALLOC_FAIL);
		goto cleanup;
	}

	for (i = 0; i < ncol; i++) {
		width_sum += widths_ptr[i];
	}
	line = GDKmalloc(width_sum + ncol + 1);
	if (!line) {
		msg = createException(MAL, "fwf.load", MAL_MALLOC_FAIL);
		goto cleanup;
	}

	approx_nrows = fsize/width_sum;
	i = 0;
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		appends[i] = BATnew(TYPE_void, c->type.type->localtype, approx_nrows, TRANSIENT) ;
		if (!appends[i]) {
			msg = createException(MAL, "fwf.load", MAL_MALLOC_FAIL);
			goto cleanup;
		}
		i++;
	}

	for (fcur = fptr; fcur < fptr + fsize - width_sum;) {
		for (i=0; i < ncol; i++) {
			char* val_start = line;
			char* val_end = line + widths_ptr[i] - 1;
			int type = appends[i]->T->type;
			strncpy(val_start, fcur, widths_ptr[i]);
			while (*val_start == padding) val_start++;
			while (*val_end == padding) val_end--;
			val_end[1] = '\0';

			if (type == TYPE_str) {
				 BUNappend(appends[i], &val_start, 0);
			} else {
				char* dst = appends[i]->T->heap.base + BATatoms[type].size * row;
				if ((*BATatoms[type].atomFromStr) (val_start, (int*) &BATatoms[type].size, (void**) &dst) < 0) {
					msg = createException(MAL, "fwf.load", "Conversion error");
					goto cleanup;
				}
			}
			fcur += widths_ptr[i];
		}
		row++;
		while (*fcur != '\n' && fcur < fptr + fsize - width_sum) fcur++;
		fcur++;
	}

	if (!m->session->active) mvc_trans(m);
	i = 0;
	for (n = t->columns.set->h; n; n = n->next) {
		BATsetcount(appends[i], row);
		store_funcs.append_col(m->session->tr, n->data, &appends[i]->batCacheid, TYPE_bat);
		i++;
	}
	sqlcleanup(m, 0);

cleanup:
	// TODO this leaks like an SR71

	return msg;
}

str fwf_load_wrap(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci) {

	mvc *m = NULL;
	str msg = MAL_SUCCEED;
	str sname = *getArgReference_str(stk, pci, 1);
	str tname = *getArgReference_str(stk, pci, 2);
	str fname = *getArgReference_str(stk, pci, 3);
	bat batid = *getArgReference_bat(stk, pci, 4);
	bte padding = *getArgReference_bte(stk, pci, 5);

	if ((msg = getSQLContext(cntxt, mb, &m, NULL)) != NULL)
		return msg;
	if ((msg = checkSQLContext(cntxt)) != NULL)
		return msg;

	msg = fwf_load(m, sname, tname, fname, BATdescriptor(batid), padding);
	return msg;
}

