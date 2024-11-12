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
#include "rel_file_loader.h"
#include "rel_exp.h"

#include "mal_instruction.h"
#include "mal_interpreter.h"
#include "mal_parser.h"
#include "mal_builder.h"
#include "mal_namespace.h"
#include "mal_exception.h"
#include "mal_linker.h"
#include "mal_backend.h"
#include "sql_types.h"
#include "rel_bin.h"
#include "mal_pipelines.h"
#include "pipeline.h"
#include "sql_pp_statement.h"
#include "bin_partition.h"

#include <unistd.h>
#include <glob.h>

#include <pqc_reader.h>

static sql_subtype *
pqc_find_subtype(mvc *sql, const pqc_schema_element *pse)
{
	sql_subtype *tpe = SA_ZNEW(sql->sa, sql_subtype);
	if (!tpe)
		return NULL;

	switch(pse->type) {
		case stringtype:
			if (!sql_find_subtype(tpe, "varchar", pse->precision, 0))
				return NULL;
			return tpe;
		case enumtype:
			if (pse->precision <= 8 && sql_find_subtype(tpe, "tinyint", pse->precision, 0))
				return tpe;
			break;
		case inttype:
			if (pse->size == 8 && sql_find_subtype(tpe, "tinyint", pse->precision, 0))
				return tpe;
			if (pse->size == 16 && sql_find_subtype(tpe, "smallint", pse->precision, 0))
				return tpe;
			if (pse->size == 32 && sql_find_subtype(tpe, "int", pse->precision, 0))
				return tpe;
			if (pse->size == 64 && sql_find_subtype(tpe, "bigint", pse->precision, 0))
				return tpe;
			break;
		case floattype:
			if (pse->precision == 32 && sql_find_subtype(tpe, "real", 0, 0))
				return tpe;
			if (pse->precision == 64 && sql_find_subtype(tpe, "double", 0, 0))
				return tpe;
			break;
		case datetype:
			if (pse->precision == 32 && sql_find_subtype(tpe, "date", pse->precision, 0))
				return tpe;
			break;
		case timetype:
			if (pse->precision == 64 && sql_find_subtype(tpe, "time", pse->precision, 0))
				return tpe;
			break;
		case timestamptype:
			if (pse->precision <= 6 && sql_find_subtype(tpe, "timestamp", pse->precision, 0))
				return tpe;
			break;
		case decimaltype:
			if (pse->size == 32) {
				if (sql_find_subtype(tpe, "decimal", 9, pse->scale)) {
					tpe->digits = pse->precision;
					return tpe;
				}
			} else if (pse->size == 64) {
				if (sql_find_subtype(tpe, "decimal", 18, pse->scale)) {
					tpe->digits = pse->precision;
					return tpe;
				}
			}
            break;
		case listtype:
			if (sql_find_subtype(tpe, "oid", 0, 0))
				return tpe;
            break;
		default:
			return NULL;
	}
	return NULL;
}

static int
pqc_find_localtype(const pqc_schema_element *pse)
{
	switch(pse->type) {
		case stringtype:
			return TYPE_str;
		case enumtype:
			if (pse->precision <= 8)
				return TYPE_bte;
			break;
		case inttype:
			if (pse->size == 8)
				return TYPE_bte;
			if (pse->size == 16)
				return TYPE_sht;
			if (pse->size == 32)
				return TYPE_int;
			if (pse->size == 64)
				return TYPE_lng;
			break;
		case floattype:
			if (pse->precision == 32)
				return TYPE_flt;
			if (pse->precision == 64)
				return TYPE_dbl;
			break;
		case datetype:
			if (pse->precision == 32)
				return TYPE_date;
			break;
		case timetype:
			if (pse->precision == 64)
				return TYPE_timestamp;
			break;
		case timestamptype:
			if (pse->precision <= 6)
				return TYPE_timestamp;
			break;
		case decimaltype:
			if (pse->size == 32)
				return TYPE_int;
			if (pse->size == 64)
				return TYPE_lng;
			break;
		case listtype:
			return TYPE_oid;
		default:
			return TYPE_void;
	}
	return TYPE_void;
}

static str
pqc_relation(mvc *sql, sql_subfunc *f, char *filename, list *res_exps, char *tname, lng *est)
{
	pqc_file *pq = NULL;

	if (est)
		*est = 1;
	if (filename && strchr(filename, '*')) {
		glob_t pglob = {};
		if (glob(filename, GLOB_ERR, NULL, &pglob) < 0)
			throw(SQL, SQLSTATE(42000), "parquet" "Could not open parquet file %s", filename);
		if (pglob.gl_pathc)
			filename = sa_strdup(sql->sa, pglob.gl_pathv[0]);
		if (est)
			*est = pglob.gl_pathc;
		globfree(&pglob);
	}
	if (pqc_open(&pq, filename) < 0)
		throw(SQL, SQLSTATE(42000), "parquet" "Could not open parquet file %s", filename);

	if (pqc_read_schema(pq) < 0) {
		pqc_close(pq);
		throw(SQL, SQLSTATE(42000), "parquet" "Could not read parquet file %s schema data", filename);
	}

	pqc_filemetadata *fmd = pqc_get_filemetadata(pq);
	if (est && fmd)
		*est *= fmd->nrows;
	int nr = 0;
	const pqc_schema_element *pse = pqc_get_schema_elements(pq, &nr);
	if (pse) {
		if (0)
		if (pse->nchildren != (nr-1)) {
			pqc_close(pq);
			throw(SQL, SQLSTATE(42000), "parquet" "Data in file %s is not tabular", filename);
		}
		f->tname = tname;
		if (0)
		for(int i = 1; i < nr; i++ ) {
			const pqc_schema_element *e = pse+i;

			if (e->nchildren || e->repetition == 2) {
				char *nme = e->name?sa_strdup(sql->ta, e->name):NULL;
				pqc_close(pq);
				throw(SQL, SQLSTATE(42000), "parquet" "Data in file %s is not tabular (column %s has repetition)", filename, nme);
			}
		}
		list *types = sa_list(sql->sa), *names = sa_list(sql->sa);
		for(int i = 1; i < nr; i++) {
			const pqc_schema_element *e = pse+i;
			sql_subtype *t = (e->type)?pqc_find_subtype(sql, e):NULL;

			if (e->type && !t) {
				int tpe = e->type;
				char *nme = e->name?sa_strdup(sql->ta, e->name):NULL;
				pqc_close(pq);
				throw(SQL, SQLSTATE(42000), "parquet: " "Data type (%d) not supported for column %s", tpe, nme);
			}
			if (!t)
				t = sql_bind_localtype("oid");
			list_append(types, t);
			char *name = NULL;
			if (e->name) {
				if (i == 14)
				name = mkLower(sa_strdup(sql->sa, "e1"));
				else
				name = mkLower(sa_strdup(sql->sa, e->name));
			} else {
				char buff[25];
				snprintf(buff, 25, "name_%i", i);
				name = sa_strdup(sql->sa, buff);
			}
			list_append(names, name);
			sql_exp *ne = exp_column(sql->sa, tname, name, t, CARD_MULTI, 1, 0, 0);
			set_basecol(ne);
			ne->alias.label = -(sql->nid++);
			if (!e->type || e->type == listtype)
				set_intern(ne);
			list_append(res_exps, ne);
			if (e->precision && *est > (1L<<e->precision)) {
				prop *p = ne->p = prop_create(sql->sa, PROP_NUNIQUES, ne->p);
				p->value.dval = 1L << e->precision;
			}
			//printf("name %s %d(%d,%d) %s\n", e->name, e->type, e->precision, e->scale, e->repetition==0?"NOT NULL":e->repetition==2?"NESTED":"");
		}
		f->res = types;
		f->coltypes = types;
		f->colnames = names;
	}
	pqc_close(pq);
	return NULL;
}

typedef struct pqc_creader {
	Sink sink;
	pqc_file *b;
	pqc_filemetadata *fmd;
	lng nrows;
	int ncols;
	int nrworkers;
	int firstcol;		/* needed for synchronisation initially -1 */
	char *done;
	pqc_reader_t **c;	/* column reader per column */
} pqc_creader;
#define PARQUET_SINK 43

static void
pqcc_destroy(void *sink)
{
	pqc_creader *r = (pqc_creader*)sink;
	assert(r->sink.type == PARQUET_SINK);
	/* for each r->readers */
	for(int i = 0; i < r->ncols; i++)
		if (r->c[i])
			pqc_reader_destroy(r->c[i]);
	GDKfree(r->c);
	if (r->b)
		pqc_close(r->b);
	GDKfree(r->done);
	GDKfree(r);
}

static int
pqcc_done(void *sink, int wid)
{
	pqc_creader *r = (pqc_creader*)sink;
	assert(r->sink.type == PARQUET_SINK);
	if (r->done[wid])
		return 1;
	return 0;
}

static pqc_creader *
pqcc_create(pqc_file *pq, pqc_filemetadata *fmd, lng nrows)
{
	pqc_creader *r = (pqc_creader*)GDKzalloc(sizeof(pqc_creader));

	r->sink.destroy = &pqcc_destroy;
	r->sink.done = &pqcc_done;
	r->sink.type = PARQUET_SINK;
	r->b = pq;
	r->fmd = fmd;
	r->nrows = nrows;
	r->ncols = fmd->rowgroups->ncolumnchunks;
	r->firstcol = -1;
	r->done = NULL;
	return r;
}

typedef struct pqc_mcreader {
	Sink sink;
	glob_t glob;
	lng nrows;
	int nrworkers;
	ATOMIC_TYPE cnt;
	char *done;
	pqc_creader **c;	/* reader per worker */
} pqc_mcreader;
#define MPARQUET_SINK 44

static void
pqcmc_destroy(void *sink)
{
	pqc_mcreader *r = (pqc_mcreader*)sink;

	if (r->glob.gl_pathc)
		globfree(&r->glob);
	assert(r->sink.type == MPARQUET_SINK);
	GDKfree(r->c);
	GDKfree(r->done);
	GDKfree(r);
}

static int
pqcmc_done(void *sink, int wid)
{
	pqc_mcreader *r = (pqc_mcreader*)sink;
	assert(r->sink.type == MPARQUET_SINK);
	if (r->c && r->c[wid] && r->c[wid]->done[0]) {
			pqcc_destroy(r->c[wid]);
			r->c[wid] = NULL;
	}
	if (r->done[wid])
		return 1;
	return 0;
}

static pqc_mcreader *
pqcmc_create(glob_t *glob, lng nrows)
{
	pqc_mcreader *r = (pqc_mcreader*)GDKzalloc(sizeof(pqc_mcreader));

	if(!r) {
		globfree(glob);
		return NULL;
	}
	r->sink.destroy = &pqcmc_destroy;
	r->sink.done = &pqcmc_done;
	r->sink.type = MPARQUET_SINK;
	r->nrworkers = 1;
	r->glob = *glob;
	r->nrows = nrows;
	r->done = NULL;
	r->c = NULL;
	ATOMIC_INIT(&r->cnt, 0);
	return r;
}

#define FILE_READER_VECTORSIZE (16*1024*16)
//(16*1024)

static str
PARQUETread_large(BAT **R, pqc_creader *r, int colno, Pipeline *p, int wnr)
{
	ssize_t sz = FILE_READER_VECTORSIZE;

	if (r->nrows < sz)
		sz = r->nrows;

	if (!r->c) {
		pipeline_lock(p);
		if (!r->c) {
			r->nrworkers = p->p->nr_workers;
			r->done = GDKzalloc( sizeof(char*) * r->nrworkers );
			r->firstcol = colno;
			r->c = GDKzalloc( sizeof(pqc_reader_t*) * r->ncols);
		}
		pipeline_unlock(p);
	}
	const pqc_schema_element *pse = r->fmd->elements+colno+1;
	int localtype = pqc_find_localtype(pse);

	if (!r->c[pse->ccnr]) {
		pipeline_lock(p);
		if (!r->c[pse->ccnr])
			r->c[pse->ccnr] = pqc_reader(NULL, pqc_dup(r->b), r->nrworkers, r->fmd, colno, r->nrows, ATOMnilptr(localtype));
		pipeline_unlock(p);
	}
	pqc_mark_chunk(r->c[pse->ccnr], r->nrworkers, wnr, sz);

	BAT *rb = NULL;

	if (pse->type == stringtype) { /* remove vector from interface */
		int ssize = 0, dict = 0;
		if (pqc_read_chunk(r->c[pse->ccnr], wnr, NULL, NULL, sz, &ssize, &dict) < 0) {
			throw (SQL, "parquet.read", SQLSTATE(HY002) "Error reading parquet file");
		}
		if (ssize < 256)
			dict = 1;
		else if (ssize < 64*1024)
			dict = 2;
		else
			dict = 4;
		/* prepare heap */
		rb = COLnew2(0, localtype, sz, TRANSIENT, dict);
		if (!rb) {
			throw(SQL, "parquet.read",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		Heap *h = rb->tvheap;
		BUN size = GDK_STRHASHTABLE * sizeof(stridx_t) + ssize/* * GDK_VARALIGN*/;
		if (h->storage == STORE_INVALID) {
			if (HEAPalloc(h, size, 1) != GDK_SUCCEED) {
				BBPreclaim(rb);
				throw(SQL, "parquet.read",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
		}
		assert(h->size >= size);
		h->free = GDK_STRHASHTABLE * sizeof(stridx_t);
		h->dirty = true;
#ifdef NDEBUG
		memset(h->base, 0, h->free);
#else
		/* fill should solve initialization problems within valgrind */
		memset(h->base, 0, h->size);
#endif
		rb->tascii = true; /* tobe fixed */
		int offset = 0;
		if (dict == 4)
			offset = h->free;
		ssize_t rsz = 0, tsz = 0;
		if ((rsz = pqc_read_chunk(r->c[pse->ccnr], wnr, rb->theap->base, ((char*)rb->tvheap->base)+rb->tvheap->free, sz, &offset, &dict)) < 0) {
			BBPreclaim(rb);
			throw (SQL, "parquet.read", SQLSTATE(HY002) "Error reading parquet file");
		}
		h->free += ssize;
		tsz += rsz;
		while(tsz < sz) {
			dict = 0;
			if (rsz == 0)
				break;
			if (pqc_read_chunk(r->c[pse->ccnr], wnr, NULL, NULL, sz, &ssize, &dict) < 0) {
				throw (SQL, "parquet.read", SQLSTATE(HY002) "Error reading parquet file");
			}
			if (dict) {
				assert(0); /* only one dict */
			}
			Heap *h = rb->tvheap;
			BUN size = h->free + (ssize /** GDK_VARALIGN*/);
			if (HEAPextend(h, size, 1) != GDK_SUCCEED) {
				BBPreclaim(rb);
				throw(SQL, "parquet.read",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			/* realloc (TODO handle extending the width) */
			assert(h->size >= size);
			dict = rb->twidth;
			offset = h->free;
			if ((rsz = pqc_read_chunk(r->c[pse->ccnr], wnr, ((char*)rb->theap->base)+(tsz*dict), ((char*)rb->tvheap->base)+rb->tvheap->free, sz, &offset, &dict)) < 0) {
				BBPreclaim(rb);
				throw (SQL, "parquet.read", SQLSTATE(HY002) "Error reading parquet file");
			}
			if (rsz == 0)
				break;
			tsz += rsz;
			h->free += ssize;
		}
		sz = tsz;
	} else { /* fixed sized */
		rb = COLnew(0, localtype, sz, TRANSIENT);
		if (!rb) {
			throw(SQL, "parquet.read",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		ssize_t rsz = 0, tsz = 0;
		if ((rsz = pqc_read_chunk(r->c[pse->ccnr], wnr, rb->theap->base, NULL, sz, NULL, NULL)) < 0) {
			BBPreclaim(rb);
			throw (SQL, "parquet.read", SQLSTATE(HY002) "Error reading parquet file");
		}
		tsz += rsz;
		while(tsz < sz) {
			if (rsz == 0)
				break;
			if ((rsz = pqc_read_chunk(r->c[pse->ccnr], wnr, ((char*)rb->theap->base)+(tsz*rb->twidth), NULL, sz, NULL, NULL)) < 0) {
				BBPreclaim(rb);
				throw (SQL, "parquet.read", SQLSTATE(HY002) "Error reading parquet file");
			}
			if (rsz == 0)
				break;
			tsz += rsz;
		}
		sz = tsz;
	}
	if (sz) {
		BATsetcount(rb, sz);
		BATnegateprops(rb);
	}
	if (!sz && r->firstcol == colno)
		r->done[wnr] = 1;
	*R = rb;
	return NULL;
}

static str
PARQUETread_multi(BAT **R, BAT *b, int colno, Pipeline *p)
{
	assert(b->T.sink->type == MPARQUET_SINK);
	pqc_mcreader *r = (pqc_mcreader*)b->T.sink;
	assert(r);

	int wnr = p->wid;
	if (!r->c) {
		pipeline_lock(p);
		if (!r->c) {
			r->nrworkers = p->p->nr_workers;
			r->done = GDKzalloc( sizeof(char*) * r->nrworkers );
			r->c = GDKzalloc( sizeof(pqc_creader*) * r->nrworkers);
		}
		pipeline_unlock(p);
	}
	if (!r->c[wnr]) {
		pipeline_lock(p);
		if (!r->c[wnr]) {
			size_t x = ATOMIC_INC(&r->cnt);
			if (x >= r->glob.gl_pathc) {
				r->done[wnr] = 1;
				pipeline_unlock(p);
				return 0;
			}
			char *f = r->glob.gl_pathv[x];
			pqc_file *pq = NULL;
			lng nrows = r->nrows;

			if (pqc_open(&pq, f) < 0) {
				pipeline_unlock(p);
				throw(SQL, "parquet.open",  SQLSTATE(HY013) "Failed to open file '%s'", f);
			}
			/* read all filemetadata including chunks */
			if (pqc_read_filemetadata(pq) < 0) {
				pipeline_unlock(p);
				pqc_close(pq);
				throw(SQL, "parquet.open",  SQLSTATE(HY013) "Failed to read metadata for file '%s'", f);
			}
			pqc_filemetadata *fmd = pqc_get_filemetadata(pq);

			if (nrows < 0)
				nrows = fmd->nrows;
			if (fmd->nrows > nrows)
				fmd->nrows = nrows;
			r->c[wnr] = pqcc_create(pq, fmd, nrows);
			/* change into single reader */
			r->c[wnr]->nrworkers = 1;
			r->c[wnr]->done = GDKzalloc( sizeof(char*) );
			r->c[wnr]->firstcol = colno;
			r->c[wnr]->c = GDKzalloc( sizeof(pqc_reader_t*) * r->c[wnr]->ncols);
		}
		pipeline_unlock(p);
	}
	return PARQUETread_large(R, r->c[wnr], colno, p, 0);
}

static str
PARQUETread(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)mb;
	(void)cntxt;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat pqb = *getArgReference_bat(stk, pci, pci->retc + 0);
	int colno = *getArgReference_int(stk, pci, pci->retc + 1);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, pci->retc + 2);
	char *msg = NULL;

	BAT *b = BATdescriptor(pqb), *rb = NULL;
	if (!b)
		throw (SQL, "parquet.read", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if (b->T.sink->type == PARQUET_SINK) {
		assert(b->T.sink->type == PARQUET_SINK);
		pqc_creader *r = (pqc_creader*)b->T.sink;
		assert(r);

		msg = PARQUETread_large(&rb, r, colno, p, p->wid);
	} else {
		msg = PARQUETread_multi(&rb, b, colno, p);
	}
	BBPreclaim(b);
	if (!msg) {
		if (!rb)
			rb = COLnew(0, stk->stk[pci->argv[0]].vtype, 0, TRANSIENT);
		if (rb) {
			*res = rb->batCacheid;
			BBPkeepref(rb);
		}
	}
	return msg;
}

/* parquet.open
 *
 * open parquet file
 * read filemeta data
 * create bat with sink (pqc_creader object)
 */
static str
PARQUETopen(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	(void)mb;
	str msg = MAL_SUCCEED;
	bat *res = getArgReference_bat(stk, pci, 0);
	char *f = *(str*)getArgReference(stk, pci, pci->retc);
	lng nrows = *getArgReference_lng(stk, pci, pci->retc + 1);
	pqc_file *pq = NULL;

	BAT *b = COLnew(0, TYPE_oid, 1024, TRANSIENT);
	if (!b)
		throw(SQL, "parquet.open",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (f && strchr(f, '*')) {
		glob_t pglob = {};
		if (glob(f, GLOB_ERR, NULL, &pglob) < 0) {
			BBPreclaim(b);
			throw(SQL, SQLSTATE(42000), "parquet" "Could not open parquet file %s", f);
		}
		b->T.sink = (Sink*)pqcmc_create(&pglob, nrows);
	} else {
		if (pqc_open(&pq, f) < 0) {
			BBPreclaim(b);
			throw(SQL, "parquet.open",  SQLSTATE(HY013) "Failed to open file '%s'", f);
		}
		/* read all filemetadata including chunks */
		if (pqc_read_filemetadata(pq) < 0) {
			BBPreclaim(b);
			pqc_close(pq);
			throw(SQL, "parquet.open",  SQLSTATE(HY013) "Failed to read metadata for file '%s'", f);
		}
		pqc_filemetadata *fmd = pqc_get_filemetadata(pq);

		if (nrows < 0)
			nrows = fmd->nrows;
		if (fmd->nrows > nrows)
			fmd->nrows = nrows;

		b->T.sink = (Sink*)pqcc_create(pq, fmd, nrows);
	}

	if (!b->T.sink) {
		BBPreclaim(b);
		throw(SQL, "parquet.open",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	*res = (b->batCacheid);
	BBPkeepref(b);
	return msg;
}

static void *
pqc_load(void *BE, sql_subfunc *f, char *filename, sql_exp *topn)
{
	backend *be = BE;
	lng nrows = -1;

	if (topn) {
		lng v = (topn->type == e_atom)?((atom*)topn->l)->data.val.lval:0;
		nrows = v;
	}
	/* pre-pipeline steps */

	/* bat b = parquet.open(file, nrows); */
	InstrPtr q = newStmt(be->mb, "parquet", "open");
	q = pushStr(be->mb, q, filename);
	q = pushLng(be->mb, q, nrows);
	pushInstruction(be->mb, q);
	int pf = getDestVar(q);

	/* start pipeline */
	pp_cleanup(be, pf); /* cleanup at end of pipeline block */

    assert(!be->pp);
    // START LOOP
    set_pipeline(be, stmt_pp_start_generator(be));
	be->source = pf;

	/* for each col create file_loader stmt */
	node *n, *m;
	int i;
	list *l = sa_list(be->mvc->sa);

	for (i=0, n = f->res->h, m = f->colnames->h; n && m; i++, n = n->next, m = m->next) {
		sql_subtype *st = n->data;
		stmt *s = stmt_none(be);
		char *column_name = m->data;

		InstrPtr q = newStmt(be->mb, "parquet", "read");
		setVarType(be->mb, getArg(q, 0), newBatType(st->type->localtype));
		q = pushArgument(be->mb, q, pf);
		q = pushInt(be->mb, q, i);
		q = pushArgument(be->mb, q, be->pipeline);
		pushInstruction(be->mb, q);

		s->nr = getDestVar(q);
		s->nrcols = 1;
		s->q = q;
		s->op4.typeval = *st;
		s = stmt_alias(be, s, i+1, "bla", column_name);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static str
PARQUETprelude(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt; (void)mb; (void)stk; (void)pci;

	fl_register("parquet", &pqc_relation, &pqc_load);
	return MAL_SUCCEED;
}

static str
PARQUETepilogue(void *ret)
{
	fl_unregister("parquet");
	(void)ret;
	return MAL_SUCCEED;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func parquet_init_funcs[] = {
	pattern("parquet", "prelude", PARQUETprelude, false, "", noargs),
	command("parquet", "epilogue", PARQUETepilogue, false, "", noargs),
    pattern("parquet", "open", PARQUETopen, true, "Create resource for shared reading from parquet file", args(1, 3, batarg("", oid), arg("f", str), arg("nrows", lng))),
    pattern("parquet", "read", PARQUETread, false, "read part of parquet file", args(1, 4, batargany("", 1), batarg("p", oid), arg("colno", int), arg("pipeline", ptr))),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_parquet_mal)
{ mal_module("parquet", NULL, parquet_init_funcs); }

