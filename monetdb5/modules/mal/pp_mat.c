/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

/*
 * TODO: add some description about these new pipeline structures
 *
 * check mat into partitioned set.
 */
#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_instruction.h"
#include "mal_pipelines.h"
#include "pp_mat.h"
#include "pp_hash.h"
#include "pipeline.h"

typedef struct part_t {
	Sink s;
	int nr;
	lng *curpos;
	MT_Lock l;
} part_t;

BAT *
pack_mat(BAT *b)
{
	mat_t *mp = (mat_t*)b->tsink;
	BUN cap = 0;

	for (int i = 0; i<mp->nr; i++)
		cap += BATcount(mp->bat[i]);
	BAT *bn = COLnew(0, b->ttype, cap, TRANSIENT);
	if (!bn)
		return NULL;
	for (int i = 0; i<mp->nr; i++) {
		if (BATappend(bn, mp->bat[i], NULL, false) != GDK_SUCCEED) {
			BBPunfix(bn->batCacheid);
			return NULL;
		}
	}
	return bn;
}

static void
mat_destroy( mat_t *m )
{
	for(int i = 0; i<m->nr; i++) {
		BBPreclaim(m->bat[i]);
	}
	GDKfree(m->bat);
	GDKfree(m->part);
	GDKfree(m->subpart);
	GDKfree(m);
}

static void
part_destroy( part_t *p )
{
	GDKfree(p->curpos);
	GDKfree(p);
}

static void
mat_activate(mat_t *mt)
{
	MT_rwlock_rdlock(&mt->rwlock);
}

static void
mat_deactivate(mat_t *mt)
{
	MT_rwlock_rdunlock(&mt->rwlock);
}

static str
MATnew(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void)cntxt;
	bat *mid = getArgReference_bat(stk, p, 0);
	int tt = getArgType(mb, p, 1);
	int nr = *getArgReference_int(stk, p, 2);
	int hashsize = (p->argc >= 4)?*getArgReference_int(stk, p, 3):0;
	bat *pid = (p->argc == 5)?getArgReference_bat(stk, p, 4):NULL;

	mat_t *mat = (mat_t*)GDKmalloc(sizeof(mat_t));
	if (!mat)
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	mat->nr = nr;
	mat->bat = (BAT**)GDKzalloc(nr * sizeof(BAT*));
	mat->part = NULL;
	mat->subpart = NULL;
	mat->slicesize = 100000; /* todo should be set from code generation */
	MT_rwlock_init(&mat->rwlock, "mat.new");
	if (!mat->bat) {
		GDKfree(mat);
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	mat->s.destroy = (sink_destroy)&mat_destroy;
	mat->s.type = MAT_SINK;

	BAT *matb = COLnew(0, tt, 1, TRANSIENT);
	if (!matb) {
		GDKfree(mat->bat);
		GDKfree(mat);
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BATnegateprops(matb);
	int i = 0;
	mat_t *pmat = NULL;
	if (pid) {
		BAT *p = BATdescriptor(*pid);
		if (!p) {
			mat_destroy(mat);
			BBPunfix(matb->batCacheid);
			throw(MAL, "mat.new", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		pmat = (mat_t*)p->tsink;
		BBPreclaim(p);
	}
	for (i = 0; i<mat->nr; i++ ) {
		BAT *b = COLnew(0, tt, 100000 /* need estimate? */, TRANSIENT);
		mat->bat[i] = b;
		if (!b)
			break;
		BATnegateprops(b);
		if (hashsize)
			b->tsink = (Sink*)ht_create(tt, (size_t)(hashsize*1.2*2.1), pmat?(hash_table*)pmat->bat[i]->tsink:NULL);
	}
	if (i < mat->nr) {
		mat_destroy(mat);
		BBPunfix(matb->batCacheid);
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	matb->tsink = (Sink*)mat;
	*mid = matb->batCacheid;
	BBPkeepref(matb);
	return MAL_SUCCEED;
}

static str
PARTnew(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void)cntxt;
	(void)mb;
	bat *pid = getArgReference_bat(stk, p, 0);
	int nr = *getArgReference_int(stk, p, 1);

	part_t *part = (part_t*)GDKmalloc(sizeof(part_t));
	if (!part)
		throw(MAL, "part.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	part->nr = nr;
	part->curpos = (lng*)GDKzalloc(nr * sizeof(lng));
	if (!part->curpos) {
		GDKfree(part);
		throw(MAL, "part.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	MT_lock_init(&part->l, "partition");
	part->s.destroy = (sink_destroy)&part_destroy;
	part->s.type = PART_SINK;

	BAT *partb = COLnew(0, TYPE_oid, 100000 /* need estimate? */, TRANSIENT);
	if (!partb) {
		GDKfree(part->curpos);
		GDKfree(part);
		throw(MAL, "part.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	partb->tsink = (Sink*)part;
	*pid = partb->batCacheid;
	BBPkeepref(partb);
	return MAL_SUCCEED;
}

static str
PARTprefixsum(Client ctx, bat *pos, const bat *gid, lng *max )
{
	(void)ctx;
	BAT *g = BATdescriptor(*gid);
	if (!g)
		throw(MAL, "part.prefixsum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	BUN n = 0, i;

	n = *max;
	BAT *p = COLnew(0, TYPE_lng, n, TRANSIENT);
	if (!p) {
		BBPunfix(g->batCacheid);
		throw(MAL, "part.prefixsum", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	lng *cnts = (lng*)Tloc(p, 0);
	for(i=0; i<n; i++)
		cnts[i]=0;
	BATsetcount(p, n);

	n = BATcount(g);
	lng *id = (lng*)Tloc(g, 0);
	for(i=0; i<n; i++)
		cnts[id[i]]++;
	*pos = p->batCacheid;
	BBPunfix(g->batCacheid);
	BATnegateprops(p);
	BBPkeepref(p);
	return MAL_SUCCEED;
}

static str
PARTpartition(Client ctx, bat *pos, const bat *part, const bat *glen )
{
	(void)ctx;
	BAT *p = BATdescriptor(*part);
	BAT *g = BATdescriptor(*glen);
	if (!p || !g) {
		BBPreclaim(p);
		BBPreclaim(g);
		throw(MAL, "part.partition", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	part_t *pt = (part_t*)p->tsink;
	assert(pt->s.type == PART_SINK);
	assert(pt->nr == (int)BATcount(g));
	BAT *posb = COLnew(0, TYPE_lng, pt->nr, TRANSIENT);
	if (!posb) {
		BBPunfix(p->batCacheid);
		BBPunfix(g->batCacheid);
		throw(MAL, "part.partition", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	lng *pp = (lng*)Tloc(posb, 0);
	lng *gp = (lng*)Tloc(g, 0);
	MT_lock_set(&pt->l);
	for(int i = 0; i < pt->nr; i++) {
		pp[i] = pt->curpos[i];
		pt->curpos[i] += gp[i];
		//printf("%p[%d]=%ld\n", pt, i, pt->curpos[i]);
	}
	MT_lock_unset(&pt->l);
	BATsetcount(posb, pt->nr);
	*pos = posb->batCacheid;
	BBPunfix(p->batCacheid);
	BBPunfix(g->batCacheid);
	BATnegateprops(posb);
	BBPkeepref(posb);
	return MAL_SUCCEED;
}

#define mat_project(T)										\
	{														\
		T **cp = (T**)GDKzalloc(mt->nr * sizeof(T*));		\
		if (cp) {											\
			bool extend = false;							\
			for(int i = 0; i<mt->nr && !extend; i++) {					\
				if (BATcapacity(mt->bat[i]) < (BUN)(curpos[i]+lp[i]))	\
					extend = true;										\
			}												\
			if (extend) {									\
				mat_deactivate(mt);							\
				MT_rwlock_wrlock(&mt->rwlock);				\
				for(int i = 0; i<mt->nr; i++) {				\
					if (BATcapacity(mt->bat[i]) < (BUN)(curpos[i]+lp[i])) {		\
						if (BATextend(mt->bat[i], curpos[i]+lp[i]) != GDK_SUCCEED) {	\
							err = createException(MAL, "mat.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
							break;							\
						}									\
					}										\
				}											\
				MT_rwlock_wrunlock(&mt->rwlock);			\
				mat_activate(mt);							\
			}												\
			if (!ATOMvarsized(d->ttype))					\
				MT_lock_set(&m->theaplock);					\
			for(int i = 0; i<mt->nr; i++) {					\
				if (BATcount(mt->bat[i]) < (BUN)(curpos[i]+lp[i]))		\
					BATsetcount(mt->bat[i], curpos[i]+lp[i]);		\
				cp[i] = (T*)Tloc(mt->bat[i], 0);			\
			}												\
			if (!ATOMvarsized(d->ttype))					\
				MT_lock_unset(&m->theaplock);				\
			if (err == NULL) {								\
				T *dp = (T*)Tloc(d, 0);						\
				for(BUN i = 0; i<BATcount(d); i++) {		\
					lng g = grp[i];							\
					cp[g][curpos[g]] = dp[i];				\
					curpos[g]++;							\
				}											\
			}												\
			GDKfree(cp);									\
		} else {										\
			err = createException(MAL, "mat.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}												\
	}													\
	break

#define mat_project_() \
	{	\
		for(int i = 0; i<mt->nr; i++) {										\
			if (BATcapacity(mt->bat[i]) < (BUN)(curpos[i]+lp[i])) {						\
				if (BATextend(mt->bat[i], curpos[i]+lp[i]) != GDK_SUCCEED) {				\
					err = createException(MAL, "mat.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
					break;										\
				}											\
			}												\
			if (BATcount(mt->bat[i]) < (BUN)(curpos[i]+lp[i]))						\
				BATsetcount(mt->bat[i], curpos[i]+lp[i]);						\
			mt->bat[i]->ttype = TYPE_fstr; \
		}													\
		if (err == NULL) {											\
			BATiter di = bat_iterator(d); \
			BUN cnt = BATcount(d); \
			TIMEOUT_LOOP_IDX_DECL(i, cnt, qry_ctx) { \
				lng g = grp[i];						\
				if (tfastins_nocheckVAR( mt->bat[g], curpos[g], BUNtvar(&di, i)) != GDK_SUCCEED) { \
					err = createException(MAL, "pp algebra.projection", MAL_MALLOC_FAIL); \
					goto error; \
				} \
				if (err) \
					TIMEOUT_LOOP_BREAK; \
				curpos[g]++; \
			} \
			bat_iterator_end(&di); 	\
		} \
		for(int i = 0; i<mt->nr; i++) {	  \
			mt->bat[i]->ttype = TYPE_str; \
		}								  \
	}

/*
 * ToDo concurrent insert of var-types. Use a single atomic operation for claiming space in the heap.
 * use read/write locks for resize of vheap.
 */

static str
MATproject(Client ctx, bat *mat, const bat *pos, const bat *lid, const bat *gid, const bat *data )
{
	(void)ctx;
	str err = NULL;
	BAT *m = BATdescriptor(*mat);
	BAT *p = BATdescriptor(*pos);
	BAT *l = BATdescriptor(*lid);
	BAT *g = BATdescriptor(*gid);
	BAT *d = BATdescriptor(*data);
	lng *curpos = NULL;
	if (!m || !p || !l || !g || !d) {
		err = createException(MAL, "mat.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto error;
	}
	curpos = GDKmalloc(sizeof(lng) * BATcount(p));
	for (BUN i = 0; i < BATcount(p); i++)
		curpos[i] = *(lng*)Tloc(p, i);
	lng *lp = (lng*)Tloc(l, 0);
	lng *grp = (lng*)Tloc(g, 0);
	mat_t *mt = (mat_t*)m->tsink;
	assert(mt->s.type == MAT_SINK);
	assert(mt->nr == (int)BATcount(p));
	assert(mt->nr == (int)BATcount(l));
	assert(BATcount(g) == BATcount(d));

	bool local_storage = false;
	if (ATOMvarsized(d->ttype))
		MT_lock_set(&m->theaplock);
	else
		mat_activate(mt);
	if (BATcount(d)) {
		BAT *r = mt->bat[0];
		assert(r);
		bool hcnt = 0;
		for (int i = 0; i < mt->nr && !hcnt; i++)
			hcnt = BATcount(mt->bat[i]) > 0;
		if (ATOMvarsized(r->ttype) && !hcnt && r->tvheap->parentid == r->batCacheid) {
			for (int i = 0; i < mt->nr; i++) {
				if (mt->bat[i]->twidth != d->twidth) {
					int m = d->twidth / mt->bat[i]->twidth;
					mt->bat[i]->twidth = d->twidth;
					mt->bat[i]->tshift = d->tshift;
					mt->bat[i]->batCapacity /= m;
				}
				BATswap_heaps(mt->bat[i], d, NULL);
			}
		} else if (ATOMvarsized(r->ttype) && hcnt && r->tvheap->parentid == r->batCacheid) {
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && hcnt && r->tvheap->parentid != r->batCacheid &&
			(r->tvheap->parentid != d->tvheap->parentid || (!VIEWvtparent(d) || BBP_desc(VIEWvtparent(d))->batRestricted != BAT_READ))) {
			for (int i = 0; i < mt->nr; i++) {
				BAT *r = mt->bat[i];
				if (unshare_varsized_heap(r) != GDK_SUCCEED) {
					MT_lock_unset(&m->theaplock);
					err = createException(MAL, "mat.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto error;
				}
			}
			local_storage = true;
		}
		if (ATOMvarsized(r->ttype) && !local_storage)
			mat_activate(mt);
		QryCtx *qry_ctx = MT_thread_get_qry_ctx();
                qry_ctx = qry_ctx ? qry_ctx : &(QryCtx) {.endtime = 0};
		if (!local_storage) {
			switch(d->twidth) {
			case 1:
				mat_project(bte);
			case 2:
				mat_project(sht);
			case 4:
				mat_project(int);
			case 8:
				mat_project(lng);
#ifdef HAVE_HGE
			case 16:
				mat_project(hge);
#endif
			default:
				err = createException(MAL, "mat.project", SQLSTATE(HY002) "invalid BAT width");
			}
		} else if (d->ttype == TYPE_str) {
			mat_project_();
		}
		TIMEOUT_CHECK(qry_ctx, err = createException(SQL, "pp algebra.projection", RUNTIME_QRY_TIMEOUT));
		if (ATOMvarsized(r->ttype) && !local_storage)
			mat_deactivate(mt);
	}
	if (ATOMvarsized(d->ttype))
		MT_lock_unset(&m->theaplock);
	else
		mat_deactivate(mt);
	if (err)
		goto error;
	GDKfree(curpos);
	BBPkeepref(m);
	BBPunfix(p->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(d->batCacheid);
	return MAL_SUCCEED;
error:
	GDKfree(curpos);
	BBPreclaim(m);
	BBPreclaim(p);
	BBPreclaim(l);
	BBPreclaim(g);
	return err;
}

/* todo split fetching for reading and writing. Such that the reading phase marks the
 * bat as read only */
static str
MATfetch(Client ctx, bat *res, const bat *mat, const int *i )
{
	(void)ctx;
	BAT *m = BATdescriptor(*mat);
	if (!m)
		throw(MAL, "mat.fetch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	mat_t *mt = (mat_t*)m->tsink;
	assert(mt->s.type == MAT_SINK);
	assert(*i < mt->nr);
	BAT *b = mt->bat[*i];
	BATnegateprops(b);
	BBPunfix(m->batCacheid);
	BBPretain(*res = b->batCacheid);
	return MAL_SUCCEED;
}

static str
MATfetch_slices(Client ctx, bat *res, const bat *mat, const int *i, const int *S )
{
	int s = *S;
	(void)ctx;
	BAT *m = BATdescriptor(*mat);
	if (!m)
		throw(MAL, "mat.fetch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	mat_t *mt = (mat_t*)m->tsink;
	assert(mt->s.type == MAT_SINK);
	assert(*i < mt->nr);
	mt->bat[*i] = BATsetaccess(mt->bat[*i], BAT_READ);
	BAT *b = mt->bat[*i];
	BATnegateprops(b);
	BAT *v = BATslice(b, s*mt->slicesize, (s+1)*mt->slicesize); /* later dynamic sizes */
	BBPunfix(m->batCacheid);
	*res = v->batCacheid;
	BBPkeepref(v);
	return MAL_SUCCEED;
}

static str
MATadd(Client ctx, bat *mat, const bat *bid, const int *i )
{
	(void)ctx;
	BAT *b = BATdescriptor(*bid);
	BAT *m = BATdescriptor(*mat);
	if (!b || !m) {
		if (b) BBPunfix(b->batCacheid);
		throw(MAL, "mat.add", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	mat_t *mt = (mat_t*)m->tsink;
	assert(mt->s.type == MAT_SINK);
	assert(*i < mt->nr);
	if (mt->bat[*i])
		BBPunfix(mt->bat[*i]->batCacheid);
	mt->bat[*i] = b;
	BBPkeepref(m);
	return MAL_SUCCEED;
}

static str
MATnr_parts(Client ctx, int *nr, const bat *mat, const int *slicesize)
{
	(void)ctx;
	BUN sz = *slicesize;
	BAT *m = BATdescriptor(*mat);
	if (!m)
		throw(MAL, "mat.nr_parts", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	mat_t *mt = (mat_t*)m->tsink;
	assert(mt->s.type == MAT_SINK);
	int n = 0;
	for(int i = 0; i< mt->nr; i++) {
		n += (int)((BATcount(mt->bat[i]) + sz - 1)/sz);
		mt->bat[i] = BATsetaccess(mt->bat[i], BAT_READ);
	}
	mt->nr_parts = n;
	mt->part = (int*)GDKmalloc(sizeof(int) * n);
	mt->subpart = (int*)GDKmalloc(sizeof(int) * n);
	mt->slicesize = *slicesize;
	if (!mt->part || !mt->subpart)
		throw(MAL, "mat.nr_parts", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	for(int i = 0, k = 0; i<mt->nr; i++) {
		int nr = (int)((BATcount(mt->bat[i]) + sz - 1)/sz);
		for(int j = 0; j < nr; j++, k++) {
			mt->part[k] = i;
			mt->subpart[k] = j;
		}
	}
	BBPreclaim(m);
	*nr = n;
	return MAL_SUCCEED;
}

static str
MATcounters_get(Client ctx, int *partid, int *sliceid, const bat *mat, const int *partnr)
{
	(void)ctx;
	BAT *m = BATdescriptor(*mat);
	if (!m)
		throw(MAL, "mat.counters_get", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	mat_t *mt = (mat_t*)m->tsink;
	assert(mt->s.type == MAT_SINK);
	if (*partnr > mt->nr_parts || *partnr < 0)
		throw(MAL, "mat.counters_get", SQLSTATE(HY002) "partnr out of range");
	*partid = mt->part[*partnr];
	*sliceid = mt->subpart[*partnr];
	BBPreclaim(m);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func pp_mat_init_funcs[] = {
 pattern("mat", "new", MATnew, false, "Create mat for partitioning", args(1,3, batargany("mat",1),argany("tt",1),arg("nr",int))),
 pattern("mat", "new", MATnew, false, "Create mat for partitioning", args(1,4, batargany("mat",1),argany("tt",1),arg("nr",int), arg("hashsize", int))),
 pattern("mat", "new", MATnew, false, "Create mat for partitioning", args(1,5, batargany("mat",1),argany("tt",1),arg("nr",int), arg("hashsize", int), batargany("parent",2))),
 pattern("part", "new", PARTnew, false, "Create part for partitioning", args(1,2, batarg("mat",oid),arg("nr",int))),
 command("part", "prefixsum", PARTprefixsum, false, "Count per group id", args(1,3, batarg("pos",lng),batarg("gid",lng),arg("max",lng))),
 command("part", "partition", PARTpartition, false, "Claim result positions for the given group lengths, returns first pos of each group", args(1,3, batarg("pos",lng),batarg("part",oid),batarg("grouplen",lng))),
 command("mat", "project", MATproject, false, "project over the partitions", args(1,5, batargany("mat",1),batarg("pos",lng), batarg("lid", lng), batarg("gid", lng), batargany("data",1))),
 command("mat", "fetch", MATfetch, false, "return i-th bat from mat", args(1,3, batargany("res",1),batargany("mat",1), arg("i", int))),
 command("mat", "fetch", MATfetch_slices, false, "return i-th bat from mat", args(1,4, batargany("res",1),batargany("mat",1), arg("i", int), arg("slice_id", int))),
 command("mat", "add", MATadd, false, "add i-th bat to mat", args(1,3, batargany("mat",1),batargany("b",1), arg("i", int))),
 command("mat", "nr_parts", MATnr_parts, false, "Return number of mat parts, where each mat element could be sliced in parts", args(1, 3, arg("nr",int), batargany("mat", 1), arg("slice_size", int))),
 command("mat", "counters_get", MATcounters_get, false, "Return part and slice id", args(2, 4, arg("partid", int), arg("sliceid", int), batargany("mat", 1), arg("partnr", int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pp_mat_mal)
{ mal_module("pp_mat", NULL, pp_mat_init_funcs); }
