/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#include "monetdb_config.h"
#include "pqc_filemetadata.h"
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
#ifdef HAVE_GLOB_H
#include <glob.h>
#endif

#include <pqc_reader.h>

static char*
str_physical_type(PhysicalType type)
{
	switch(type) {
		case PT_BOOLEAN:
			return "BOOLEAN";
		case PT_INT32:
			return "INT32";
		case PT_INT64:
			return "INT64";
		case PT_INT96:
			return "INT96";
		case PT_FLOAT:
			return "FLOAT";
		case PT_DOUBLE:
			return "DOUBLE";
		case PT_BYTE_ARRAY:
			return "BYTE_ARRAY";
		case PT_FIXED_LEN_BYTE_ARRAY:
			return "FIXED_LEN_BYTE_ARRAY";
		default:
			return "";
	}
}

static char*
str_logical_type(logicaltype type)
{
	switch(type) {
		case stringtype:
			return "STRING";
		case maptype:
			return "MAP";
		case listtype:
			return "LIST";
		case enumtype:
			return "ENUM";
		case decimaltype:
			return "DECIMAL";
		case datetype:
			return "DATE";
		case timetype:
			return "TIME";
		case timestamptype:
			return "TIMESTAMP";
		case intervaltype:
			return "INTERVAL";
		case inttype:
			return "INTEGER";
		case nulltype:
			return "UNKNOWN";
		case jsontype:
			return "JSON";
		case bsontype:
			return "BSON";
		case uuidtype:
			return "UUID";
		case float16type:
			return "FLOAT16";
		case varianttype:
			return "VARIANT";
		default:
			return "";
	}
}

static char*
str_converted_type(convertedtype type)
{
	switch(type) {
		case CT_UTF8:
			return "UTF8";
		case CT_MAP:
			return "MAP";
		case CT_MAP_KEY_VALUE:
			return "MAP_KEY_VALUE";
		case CT_LIST:
			return "LIST";
		case CT_ENUM:
			return "ENUM";
		case CT_DECIMAL:
			return "DECIMAL";
		case CT_DATE:
			return "DATE";
		case CT_TIME_MILLIS:
			return "TIME_MILLIS";
		case CT_TIME_MICROS:
			return "TIME_MICROS";
		case CT_TIMESTAMP_MILLIS:
			return "TIMESTAMP_MILLIS";
		case CT_TIMESTAMP_MICROS:
			return "TIMESTAMP_MICROS";
		case CT_UINT_8:
			return "UINT_8";
		case CT_UINT_16:
			return "UINT_16";
		case CT_UINT_32:
			return "UINT_32";
		case CT_UINT_64:
			return "UINT_64";
		case CT_INT_8:
			return "INT_8";
		case CT_INT_16:
			return "INT_16";
		case CT_INT_32:
			return "INT_32";
		case CT_INT_64:
			return "INT_64";
		case CT_JSON:
			return "JSON";
		case CT_BSON:
			return "BSON";
		case CT_INTERVAL:
			return "INTERVAL";
		default:
			return "";
	}
}

static char*
str_repetition_type(FieldRepetitionType type)
{
	switch(type) {
		case FRT_REQUIRED:
			return "REQUIRED";
		case FRT_OPTIONAL:
			return "OPTIONAL";
		case FRT_REPEATED:
			return "REPEATED";
		default:
			return "";
	}
}

static char*
str_compression_codec(CompressionCodec type)
{
	switch(type) {
		case CC_UNCOMPRESSED:
			return "UNCOMPRESSED";
		case CC_SNAPPY:
			return "SNAPPY";
		case CC_GZIP:
			return "GZIP";
		case CC_LZO:
			return "LZO";
		case CC_BROTLI:
			return "BROTLY";
		case CC_LZ4:
			return "LZ4";
		case CC_ZSTD:
			return "ZSTD";
		case CC_LZ4_RAW:
			return "LZ4_RAW";
		default:
			return "";
	}
}

static char*
str_encoding(Encoding e)
{
	switch(e) {
		case PLAIN:
			return "PLAIN";
		case PLAIN_DICTIONARY:
			return "PLAIN_DICTIONARY";
		case RLE:
			return "RLE";
		case BIT_PACKED:
			return "BIT_PACKED";
		case DELTA_BINARY_PACKED:
			return "DELTA_BINARY_PACKED";
		case DELTA_LENGTH_BYTE_ARRAY:
			return "DELTA_LENGTH_BYTE_ARRAY";
		case RLE_DICTIONARY:
			return "RLE_DICTIONARY";
		case BYTE_STREAM_SPLIT:
			return "BYTE_STREAM_SPLIT";
		default:
			return "";
	}
}

static const char*
get_valid_utf8_or_empty(const char *s)
{
	if (s != NULL && checkUTF8(s, NULL))
		return s;
	return "";
}

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
			if (pse->size == 0) { /* byte array */
				if (sql_find_subtype(tpe, "decimal", pse->precision, pse->scale)) {
					tpe->digits = pse->precision;
					return tpe;
				}
			} else if (pse->size == 32) {
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
			if (pse->size == 0) {
				return TYPE_sht;
			}
			if (pse->size == 32)
				return TYPE_int;
			if (pse->size == 64)
				return TYPE_lng;
			assert(0);
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
#ifdef HAVE_GLOB_H
	if (filename && strchr(filename, '*')) {
		glob_t pglob = {};
		if (glob(filename, GLOB_ERR, NULL, &pglob) < 0)
			throw(SQL, SQLSTATE(42000), "parquet" "Could not open parquet file %s", filename);
		if (pglob.gl_pathc)
			filename = ma_strdup(sql->sa, pglob.gl_pathv[0]);
		if (est)
			*est = pglob.gl_pathc;
		globfree(&pglob);
	}
#endif
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
	allocator *ma = MT_thread_getallocator();
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
				char *nme = e->name?ma_strdup(ma, e->name):NULL;
				pqc_close(pq);
				throw(SQL, SQLSTATE(42000), "parquet" "Data in file %s is not tabular (column %s has repetition)", filename, nme);
			}
		}
		list *types = sa_list(sql->sa), *names = sa_list(sql->sa);
		for(int i = 1; i < nr; i++) {
			const pqc_schema_element *e = pse+i;
			sql_subtype *t = (e->type!=LT_UNKNOWN)?pqc_find_subtype(sql, e):NULL;

			if (e->type != LT_UNKNOWN && !t) {
				int tpe = e->type;
				char *nme = e->name?ma_strdup(ma, e->name):NULL;
				pqc_close(pq);
				throw(SQL, SQLSTATE(42000), "parquet: " "Data type (%s) not supported for column %s", str_logical_type(tpe), nme);
			}
			if (!t)
				t = sql_fetch_localtype(TYPE_oid);
			list_append(types, t);
			char *name = NULL;
			if (e->name) {
				if (i == 14)
				name = mkLower(ma_strdup(sql->sa, "e1"));
				else
				name = mkLower(ma_strdup(sql->sa, e->name));
			} else {
				char buff[25];
				snprintf(buff, 25, "name_%i", i);
				name = ma_strdup(sql->sa, buff);
			}
			list_append(names, name);
			sql_exp *ne = exp_column(sql->sa, tname, name, t, CARD_MULTI, 1, 0, 0);
			set_basecol(ne);
			ne->alias.label = -(sql->nid++);
			if (e->type == LT_UNKNOWN || e->type == listtype)
				set_intern(ne);
			list_append(res_exps, ne);
			if (e->precision && *est > ((lng) 1 << e->precision)) {
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
pqcc_destroy(pqc_creader *r)
{
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
pqcc_done(pqc_creader *r, int wid, int nr_workers, bool redo)
{
	(void)redo;
	(void)nr_workers;
	assert(r->sink.type == PARQUET_SINK);
	if (r->done && r->done[wid])
		return 1;
	return 0;
}

static pqc_creader *
pqcc_create(pqc_file *pq, pqc_filemetadata *fmd, lng nrows)
{
	pqc_creader *r = (pqc_creader*)GDKzalloc(sizeof(pqc_creader));

	r->sink.destroy = (sink_destroy)&pqcc_destroy;
	r->sink.done = (sink_done)&pqcc_done;
	r->sink.type = PARQUET_SINK;
	r->b = pq;
	r->fmd = fmd;
	r->nrows = nrows;
	r->ncols = fmd->rowgroups->ncolumnchunks;
	r->firstcol = -1;
	r->done = NULL;
	return r;
}

#ifdef HAVE_GLOB_H
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
pqcmc_destroy(pqc_mcreader *r)
{
#ifdef HAVE_GLOB_H
	if (r->glob.gl_pathc)
		globfree(&r->glob);
#endif
	assert(r->sink.type == MPARQUET_SINK);
	GDKfree(r->c);
	GDKfree(r->done);
	GDKfree(r);
}

static int
pqcmc_done(pqc_mcreader *r, int wid, int nr_workers, bool redo)
{
	(void)redo;
	(void)nr_workers;
	assert(r->sink.type == MPARQUET_SINK);
	if (r->c && r->c[wid] && r->c[wid]->done[0]) {
			pqcc_destroy(r->c[wid]);
			r->c[wid] = NULL;
	}
	if (r->done && r->done[wid])
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
	r->sink.destroy = (sink_destroy)&pqcmc_destroy;
	r->sink.done = (sink_done)&pqcmc_done;
	r->sink.type = MPARQUET_SINK;
	r->nrworkers = 1;
	r->glob = *glob;
	r->nrows = nrows;
	r->done = NULL;
	r->c = NULL;
	ATOMIC_INIT(&r->cnt, 0);
	return r;
}
#endif

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
			if ((rsz = pqc_read_chunk(r->c[pse->ccnr], wnr, ((char*)rb->theap->base)+(tsz*dict), ((char*)rb->tvheap->base)+rb->tvheap->free, sz-tsz, &offset, &dict)) < 0) {
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
			if ((rsz = pqc_read_chunk(r->c[pse->ccnr], wnr, ((char*)rb->theap->base)+(tsz*rb->twidth), NULL, sz-tsz, NULL, NULL)) < 0) {
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

#ifdef HAVE_GLOB_H
static str
PARQUETread_multi(BAT **R, BAT *b, int colno, Pipeline *p)
{
	assert(b->tsink->type == MPARQUET_SINK);
	pqc_mcreader *r = (pqc_mcreader*)b->tsink;
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
#endif

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
	if (b->tsink->type == PARQUET_SINK) {
		assert(b->tsink->type == PARQUET_SINK);
		pqc_creader *r = (pqc_creader*)b->tsink;
		assert(r);

		msg = PARQUETread_large(&rb, r, colno, p, p->wid);
	} else {
#ifdef HAVE_GLOB_H
		msg = PARQUETread_multi(&rb, b, colno, p);
#else
		assert(0);
#endif
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

#ifdef HAVE_GLOB_H
	if (f && strchr(f, '*')) {
		glob_t pglob = {};
		if (glob(f, GLOB_ERR, NULL, &pglob) < 0) {
			BBPreclaim(b);
			throw(SQL, SQLSTATE(42000), "parquet" "Could not open parquet file %s", f);
		}
		b->tsink = (Sink*)pqcmc_create(&pglob, nrows);
	} else
#endif
	{
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

		b->tsink = (Sink*)pqcc_create(pq, fmd, nrows);
	}

	if (!b->tsink) {
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

	if (be->pp) {
		stmt_concat_add_source(be);
	} else {
		/* start pipeline */
		pp_cleanup(be, pf); /* cleanup at end of pipeline block */
		// START LOOP
		set_pipeline(be, stmt_pp_start_generator(be, pf, false));
		be->need_pipeline = false;
	}

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

static str
PARQUETschema(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt; (void) mb;
	char *msg = NULL;
	char *fname = *(str*)getArgReference(stk, pci, pci->retc);
	pqc_file *pq = NULL;

	if (pqc_open(&pq, fname) < 0) {
		throw(SQL, "parquet.schema",  SQLSTATE(HY013) "Failed to open file '%s'", fname);
	}
	if (pqc_read_schema(pq) < 0) {
		pqc_close(pq);
		throw(SQL, "parquet.schema",  SQLSTATE(HY013) "Failed to read metadata for file '%s'", fname);
	}

	BAT *name = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *path = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *physical_type = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *length = COLnew(0, TYPE_int, 0, TRANSIENT);
	BAT *repetition_type = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *num_children = COLnew(0, TYPE_int, 0, TRANSIENT);
	BAT *converted_type = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *logical_type = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *precision = COLnew(0, TYPE_int, 0, TRANSIENT);
	BAT *scale = COLnew(0, TYPE_int, 0, TRANSIENT);

	if (name == NULL || path == NULL || physical_type == NULL
		   	|| length == NULL || repetition_type == NULL
		   	|| num_children == NULL || converted_type == NULL
		   	|| logical_type == NULL || precision == NULL
		   	|| scale == NULL) {
		msg = createException(SQL, "parquet.schema", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	pqc_filemetadata *fmd = pqc_get_filemetadata(pq);
	for (int i = 0; i < fmd->nelements; i++) {
		pqc_schema_element e = fmd->elements[i];
		int nchildren = e.nchildren;
		// only leaf nodes have physical_type defined
		char *phy_type = nchildren > 0 ? "" : str_physical_type(e.physical_type);
		// only leaf nodes have converted_type defined
		char *conv_type = nchildren > 0 ? "" : str_converted_type(e.converted_type);
		// only leaf nodes have logical_type defined
		char *logic_type = nchildren > 0 ? "" : str_logical_type(e.type);
		char *rep_type = str_repetition_type(e.repetition);
		uint32_t type_length = e.type_length;
		char *cpath = ""; // TODO

		if (BUNappend(name, e.name, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(path, cpath, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(physical_type, phy_type, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(length, &type_length, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(repetition_type, rep_type, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(num_children, &nchildren, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(converted_type, conv_type, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(logical_type, logic_type, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(precision, &e.repetition, false) != GDK_SUCCEED)
			goto bailout;
		if (BUNappend(scale, &e.scale, false) != GDK_SUCCEED)
			goto bailout;
	}

	bat *name_id = getArgReference_bat(stk, pci, 0);
	*name_id = name->batCacheid;
	BBPkeepref(name);
	bat *path_id = getArgReference_bat(stk, pci, 1);
	*path_id = path->batCacheid;
	BBPkeepref(path);
	bat *pt_id = getArgReference_bat(stk, pci, 2);
	*pt_id = physical_type->batCacheid;
	BBPkeepref(physical_type);
	bat *length_id = getArgReference_bat(stk, pci, 3);
	*length_id = length->batCacheid;
	BBPkeepref(length);
	bat *rt_id = getArgReference_bat(stk, pci, 4);
	*rt_id = repetition_type->batCacheid;
	BBPkeepref(repetition_type);
	bat *nchildren_id = getArgReference_bat(stk, pci, 5);
	*nchildren_id = num_children->batCacheid;
	BBPkeepref(num_children);
	bat *ct_id = getArgReference_bat(stk, pci, 6);
	*ct_id = converted_type->batCacheid;
	BBPkeepref(converted_type);
	bat *lt_id = getArgReference_bat(stk, pci, 7);
	*lt_id = logical_type->batCacheid;
	BBPkeepref(logical_type);
	bat *precision_id = getArgReference_bat(stk, pci, 8);
	*precision_id = precision->batCacheid;
	BBPkeepref(precision);
	bat *scale_id = getArgReference_bat(stk, pci, 9);
	*scale_id = scale->batCacheid;
	BBPkeepref(scale);
	return MAL_SUCCEED;
bailout:
	if(pq)
		pqc_close(pq);
	BBPreclaim(name);
	BBPreclaim(path);
	BBPreclaim(physical_type);
	BBPreclaim(length);
	BBPreclaim(repetition_type);
	BBPreclaim(num_children);
	BBPreclaim(converted_type);
	BBPreclaim(logical_type);
	BBPreclaim(precision);
	BBPreclaim(scale);
	return msg;
}

static str
PARQUETfile_metadata(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt; (void) mb;
	char *msg = NULL;
	char *fname = *(str*)getArgReference(stk, pci, pci->retc);
	pqc_file *pq = NULL;

	if (pqc_open(&pq, fname) < 0) {
		throw(SQL, "parquet.file_metadata",  SQLSTATE(HY013) "Failed to open file '%s'", fname);
	}
	if (pqc_read_filemetadata(pq) < 0) {
		pqc_close(pq);
		throw(SQL, "parquet.file_metadata",  SQLSTATE(HY013) "Failed to read file metadata for file '%s'", fname);
	}
	BAT *created_by = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *format_version = COLnew(0, TYPE_int, 0, TRANSIENT);
	BAT *num_columns = COLnew(0, TYPE_int, 0, TRANSIENT);
	BAT *num_row_groups = COLnew(0, TYPE_int, 0, TRANSIENT);
	BAT *num_rows = COLnew(0, TYPE_int, 0, TRANSIENT);
	BAT *encryption_algorithm = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *footer_signing_key_metadata = COLnew(0, TYPE_str, 0, TRANSIENT);

	if (created_by == NULL || format_version == NULL || num_columns == NULL
		   	|| num_row_groups == NULL || num_rows == NULL
		   	|| encryption_algorithm == NULL || footer_signing_key_metadata == NULL) {
		msg = createException(SQL, "parquet.file_metadata", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	pqc_filemetadata *fmd = pqc_get_filemetadata(pq);
	int ncolumns = fmd->nelements - 1; // ?
	const char *cby = get_valid_utf8_or_empty(fmd->created_by);
	const char *algorithm = get_valid_utf8_or_empty(fmd->encryption_algorithm);
	const char *fskm = get_valid_utf8_or_empty(fmd->footer_signing_key_metadata);

	if (BUNappend(created_by, cby, false) != GDK_SUCCEED)
		goto bailout;
	if (BUNappend(format_version, &fmd->version, false) != GDK_SUCCEED)
		goto bailout;
	if (BUNappend(num_columns, &ncolumns, false) != GDK_SUCCEED)
		goto bailout;
	if (BUNappend(num_row_groups, &fmd->nrowgroups, false) != GDK_SUCCEED)
		goto bailout;
	if (BUNappend(num_rows, &fmd->nrows, false) != GDK_SUCCEED)
		goto bailout;
	if (BUNappend(encryption_algorithm, algorithm, false) != GDK_SUCCEED)
		goto bailout;
	if (BUNappend(footer_signing_key_metadata, fskm, false) != GDK_SUCCEED)
		goto bailout;

	bat *created_by_id = getArgReference_bat(stk, pci, 0);
	*created_by_id = created_by->batCacheid;
	BBPkeepref(created_by);
	bat *format_version_id = getArgReference_bat(stk, pci, 1);
	*format_version_id = format_version->batCacheid;
	BBPkeepref(format_version);
	bat *num_columns_id = getArgReference_bat(stk, pci, 2);
	*num_columns_id = num_columns->batCacheid;
	BBPkeepref(num_columns);
	bat *num_row_groups_id = getArgReference_bat(stk, pci, 3);
	*num_row_groups_id = num_row_groups->batCacheid;
	BBPkeepref(num_row_groups);
	bat *num_rows_id = getArgReference_bat(stk, pci, 4);
	*num_rows_id = num_rows->batCacheid;
	BBPkeepref(num_rows);
	bat *encryption_algorithm_id = getArgReference_bat(stk, pci, 5);
	*encryption_algorithm_id = encryption_algorithm->batCacheid;
	BBPkeepref(encryption_algorithm);
	bat *footer_signing_key_metadata_id = getArgReference_bat(stk, pci, 6);
	*footer_signing_key_metadata_id = footer_signing_key_metadata->batCacheid;
	BBPkeepref(footer_signing_key_metadata);
	return MAL_SUCCEED;
bailout:
	if(pq)
		pqc_close(pq);
	BBPreclaim(created_by);
	BBPreclaim(format_version);
	BBPreclaim(num_columns);
	BBPreclaim(num_row_groups);
	BBPreclaim(num_rows);
	BBPreclaim(encryption_algorithm);
	BBPreclaim(footer_signing_key_metadata);
	return msg;
}

static str
PARQUETmetadata(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt; (void) mb;
	char *msg = NULL;
	char *fname = *(str*)getArgReference(stk, pci, pci->retc);
	pqc_file *pq = NULL;

	if (pqc_open(&pq, fname) < 0) {
		throw(SQL, "parquet.metadata",  SQLSTATE(HY013) "Failed to open file '%s'", fname);
	}
	if (pqc_read_filemetadata(pq) < 0) {
		pqc_close(pq);
		throw(SQL, "parquet.metadata",  SQLSTATE(HY013) "Failed to read file metadata for file '%s'", fname);
	}

	BAT *row_group_id = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *row_group_num_rows = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *row_group_num_columns = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *row_group_bytes = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *column_id = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *file_offset = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *num_values = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *path_in_schema = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *type = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *stats_min = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *stats_max = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *stats_null_count = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *stats_distinct_count = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *stats_min_value = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *stats_max_value = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *compression = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *encodings = COLnew(0, TYPE_str, 0, TRANSIENT);
	BAT *index_page_offset = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *dictionary_page_offset = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *data_page_offset = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *total_compressed_size = COLnew(0, TYPE_lng, 0, TRANSIENT);
	BAT *total_uncompressed_size = COLnew(0, TYPE_lng, 0, TRANSIENT);

	if (row_group_id == NULL || row_group_num_rows == NULL || row_group_num_columns == NULL
		   	|| row_group_bytes == NULL || column_id == NULL
		   	|| file_offset == NULL || num_values == NULL
		   	|| path_in_schema == NULL || type == NULL
		   	|| stats_min == NULL || stats_max == NULL
		   	|| stats_null_count == NULL || stats_distinct_count == NULL
		   	|| stats_min_value == NULL || stats_max_value == NULL
		   	|| compression == NULL || encodings == NULL
		   	|| index_page_offset == NULL || dictionary_page_offset == NULL
		   	|| data_page_offset == NULL || total_compressed_size == NULL
		   	|| total_uncompressed_size == NULL) {
		msg = createException(SQL, "parquet.metadata", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto bailout;
	}
	pqc_filemetadata *fmd = pqc_get_filemetadata(pq);
	for (int i = 0; i < fmd->nrowgroups; i++) {
		pqc_row_group row_group = fmd->rowgroups[i];
		for (int j = 0; j < row_group.ncolumnchunks; j++) {
			pqc_columnchunk column_chunk = row_group.columnchunks[j];
			pqc_stat stats = column_chunk.stat;
			if (BUNappend(row_group_id, &row_group.ordinal, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(row_group_num_rows, &row_group.num_rows, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(row_group_num_columns, &row_group.ncolumnchunks, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(row_group_bytes, &row_group.total_byte_size, false) != GDK_SUCCEED)
				goto bailout;
			//
			if (BUNappend(column_id, &j, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(file_offset, &column_chunk.file_offset, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(num_values, &column_chunk.num_values, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(path_in_schema, get_valid_utf8_or_empty(column_chunk.path_in_schema), false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(type, str_physical_type(column_chunk.type), false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(stats_min, get_valid_utf8_or_empty(stats.min_string), false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(stats_max, get_valid_utf8_or_empty(stats.max_string), false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(stats_null_count, &stats.null_count, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(stats_distinct_count, &stats.distinct_count, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(stats_min_value, get_valid_utf8_or_empty(stats.min_value), false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(stats_max_value, get_valid_utf8_or_empty(stats.max_value), false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(compression, str_compression_codec(column_chunk.codec), false) != GDK_SUCCEED)
				goto bailout;
			// should be enough to hold all concatenated encodings strings
			char encodings_str[200] = "";
			if (column_chunk.num_encodings ) {
				uint32_t char_cnt = 0;
				for (uint32_t k = 0; k < column_chunk.num_encodings && char_cnt < 200; k++) {
					char *next = str_encoding(column_chunk.encodings[k]);
					char_cnt += strlen(next);
					strcat(encodings_str, next);
					if (k < column_chunk.num_encodings - 1) {
						strcat(encodings_str, ", ");
						char_cnt += 2;
					}
				}
			}
			if (BUNappend(encodings, encodings_str, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(index_page_offset, &column_chunk.index_page_offset, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(dictionary_page_offset, &column_chunk.dictionary_page_offset, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(data_page_offset, &column_chunk.data_page_offset, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(total_compressed_size, &column_chunk.total_compressed_size, false) != GDK_SUCCEED)
				goto bailout;
			if (BUNappend(total_uncompressed_size, &column_chunk.total_uncompressed_size, false) != GDK_SUCCEED)
				goto bailout;
		}
	}

	bat *row_group_id_id = getArgReference_bat(stk, pci, 0);
	*row_group_id_id = row_group_id->batCacheid;
	BBPkeepref(row_group_id);
	bat *row_group_num_rows_id = getArgReference_bat(stk, pci, 1);
	*row_group_num_rows_id = row_group_num_rows->batCacheid;
	BBPkeepref(row_group_num_rows);
	bat *row_group_num_columns_id = getArgReference_bat(stk, pci, 2);
	*row_group_num_columns_id = row_group_num_columns->batCacheid;
	BBPkeepref(row_group_num_columns);
	bat *row_group_bytes_id = getArgReference_bat(stk, pci, 3);
	*row_group_bytes_id = row_group_bytes->batCacheid;
	BBPkeepref(row_group_bytes);
	bat *column_id_id = getArgReference_bat(stk, pci, 4);
	*column_id_id = column_id->batCacheid;
	BBPkeepref(column_id);
	bat *file_offset_id = getArgReference_bat(stk, pci, 5);
	*file_offset_id = file_offset->batCacheid;
	BBPkeepref(file_offset);
	bat *num_values_id = getArgReference_bat(stk, pci, 6);
	*num_values_id = num_values->batCacheid;
	BBPkeepref(num_values);
	bat *path_in_schema_id = getArgReference_bat(stk, pci, 7);
	*path_in_schema_id = path_in_schema->batCacheid;
	BBPkeepref(path_in_schema);
	bat *type_id = getArgReference_bat(stk, pci, 8);
	*type_id = type->batCacheid;
	BBPkeepref(type);
	bat *stats_min_id = getArgReference_bat(stk, pci, 9);
	*stats_min_id = stats_min->batCacheid;
	BBPkeepref(stats_min);
	bat *stats_max_id = getArgReference_bat(stk, pci, 10);
	*stats_max_id = stats_max->batCacheid;
	BBPkeepref(stats_max);
	bat *stats_null_count_id = getArgReference_bat(stk, pci, 11);
	*stats_null_count_id = stats_null_count->batCacheid;
	BBPkeepref(stats_null_count);
	bat *stats_distinct_count_id = getArgReference_bat(stk, pci, 12);
	*stats_distinct_count_id = stats_distinct_count->batCacheid;
	BBPkeepref(stats_distinct_count);
	bat *stats_min_value_id = getArgReference_bat(stk, pci, 13);
	*stats_min_value_id = stats_min_value->batCacheid;
	BBPkeepref(stats_min_value);
	bat *stats_max_value_id = getArgReference_bat(stk, pci, 14);
	*stats_max_value_id = stats_max_value->batCacheid;
	BBPkeepref(stats_max_value);
	bat *compression_id = getArgReference_bat(stk, pci, 15);
	*compression_id = compression->batCacheid;
	BBPkeepref(compression);
	bat *encodings_id = getArgReference_bat(stk, pci, 16);
	*encodings_id = encodings->batCacheid;
	BBPkeepref(encodings);
	bat *index_page_offset_id = getArgReference_bat(stk, pci, 17);
	*index_page_offset_id = index_page_offset->batCacheid;
	BBPkeepref(index_page_offset);
	bat *dictionary_page_offset_id = getArgReference_bat(stk, pci, 18);
	*dictionary_page_offset_id = dictionary_page_offset->batCacheid;
	BBPkeepref(dictionary_page_offset);
	bat *data_page_offset_id = getArgReference_bat(stk, pci, 19);
	*data_page_offset_id = data_page_offset->batCacheid;
	BBPkeepref(data_page_offset);
	bat *total_compressed_size_id = getArgReference_bat(stk, pci, 20);
	*total_compressed_size_id = total_compressed_size->batCacheid;
	BBPkeepref(total_compressed_size);
	bat *total_uncompressed_size_id = getArgReference_bat(stk, pci, 21);
	*total_uncompressed_size_id = total_uncompressed_size->batCacheid;
	BBPkeepref(total_uncompressed_size);

	return MAL_SUCCEED;
bailout:
	if(pq)
		pqc_close(pq);
	BBPreclaim(row_group_id);
	BBPreclaim(row_group_num_rows);
	BBPreclaim(row_group_num_columns);
	BBPreclaim(row_group_bytes);
	BBPreclaim(column_id);
	BBPreclaim(file_offset);
	BBPreclaim(num_values);
	BBPreclaim(path_in_schema);
	BBPreclaim(type);
	BBPreclaim(stats_min);
	BBPreclaim(stats_max);
	BBPreclaim(stats_null_count);
	BBPreclaim(stats_distinct_count);
	BBPreclaim(stats_min_value);
	BBPreclaim(stats_max_value);
	BBPreclaim(compression);
	BBPreclaim(encodings);
	BBPreclaim(index_page_offset);
	BBPreclaim(dictionary_page_offset);
	BBPreclaim(data_page_offset);
	BBPreclaim(total_compressed_size);
	BBPreclaim(total_uncompressed_size);

	return msg;
}

#include "sql_scenario.h"
#include "mel.h"

static mel_func parquet_init_funcs[] = {
	pattern("parquet", "prelude", PARQUETprelude, false, "", noargs),
	command("parquet", "epilogue", PARQUETepilogue, false, "", noargs),
    pattern("parquet", "open", PARQUETopen, true, "Create resource for shared reading from parquet file", args(1, 3, batarg("", oid), arg("f", str), arg("nrows", lng))),
    pattern("parquet", "read", PARQUETread, false, "read part of parquet file", args(1, 4, batargany("", 1), batarg("p", oid), arg("colno", int), arg("pipeline", ptr))),
	pattern("parquet", "schema", PARQUETschema, false, "Read parquet schema",
		   	args(10,11,
			   	batarg("name", str),
			   	batarg("path", str),
			   	batarg("physical_type", str),
			   	batarg("length", lng),
			   	batarg("repetition_type", str),
			   	batarg("num_children", lng),
			   	batarg("converted_type", str),
			   	batarg("logical_type", str),
			   	batarg("precision", lng),
			   	batarg("scale", lng),
			   	arg("fname",str))),
	pattern("parquet", "file_metadata", PARQUETfile_metadata, false, "Read parquet file metadata",
		   	args(7,8,
			   	batarg("created_by", str),
			   	batarg("format_version", lng),
			   	batarg("num_columns", lng),
			   	batarg("num_row_groups", lng),
			   	batarg("num_rows", lng),
			   	batarg("encryption_algorithm", str),
			   	batarg("footer_signing_key_metadata", str),
			   	arg("fname",str))),
	pattern("parquet", "metadata", PARQUETmetadata, false, "Read parquet metadata",
		   	args(22,23,
			   	batarg("row_group_id", lng),
			   	batarg("row_group_num_rows", lng),
			   	batarg("row_group_num_columns", lng),
			   	batarg("row_group_bytes", lng),
			   	batarg("column_id", lng),
			   	batarg("file_offset", lng),
			   	batarg("num_values", lng),
			   	batarg("path_in_schema", str),
			   	batarg("type", str),
			   	batarg("stats_min", str),
			   	batarg("stats_max", str),
			   	batarg("stats_null_count", lng),
			   	batarg("stats_distinct_count", lng),
			   	batarg("stats_min_value", str),
			   	batarg("stats_max_value", str),
			   	batarg("compression", str),
			   	batarg("encodings", str),
			   	batarg("index_page_offset", lng),
			   	batarg("dictionary_page_offset", lng),
			   	batarg("date_page_offset", lng),
			   	batarg("total_compressed_size", lng),
			   	batarg("total_uncompressed_size", lng),
			   	arg("fname",str))),
{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_parquet_mal)
{ mal_module("parquet", NULL, parquet_init_funcs); }
