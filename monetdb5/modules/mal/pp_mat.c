/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

/*
 * TODO: add some description about these new pipeline structures
 */
#include "monetdb_config.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_instruction.h"
#include "pp_mat.h"

typedef struct part_t {
	Sink s;
	int nr;
	lng *curpos;
	MT_Lock l;
} part_t;

BAT *
pack_mat(BAT *b)
{
	mat_t *mp = (mat_t*)b->T.sink;
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
	GDKfree(m);
}

static void
part_destroy( part_t *p )
{
	GDKfree(p->curpos);
	GDKfree(p);
}

static str
MATnew(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	(void)cntxt;
	bat *mid = getArgReference_bat(stk, p, 0);
	int tt = getArgType(mb, p, 1);
	int nr = *getArgReference_int(stk, p, 2);

	mat_t *mat = (mat_t*)GDKmalloc(sizeof(mat_t));
	if (!mat)
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	mat->nr = nr;
	mat->bat = (BAT**)GDKzalloc(nr * sizeof(BAT*));
	if (!mat->bat) {
		GDKfree(mat);
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	mat->s.destroy = (sink_destroy)&mat_destroy;
	mat->s.type = MAT_SINK;

	BAT *matb = COLnew(0, tt, 100000 /* need estimate? */, TRANSIENT);
	if (!matb) {
		GDKfree(mat->bat);
		GDKfree(mat);
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	BATnegateprops(matb);
	int i = 0;
	for (i = 0; i<mat->nr; i++ ) {
		BAT *b = COLnew(0, tt, 100000 /* need estimate? */, TRANSIENT);
		mat->bat[i] = b;
		if (!b)
			break;
		BATnegateprops(b);
	}
	if (i < mat->nr) {
		mat_destroy(mat);
		BBPunfix(matb->batCacheid);
		throw(MAL, "mat.new", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	matb->T.sink = (Sink*)mat;
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

	partb->T.sink = (Sink*)part;
	*pid = partb->batCacheid;
	BBPkeepref(partb);
	return MAL_SUCCEED;
}

static str
PARTprefixsum( bat *pos, const bat *gid, lng *max )
{
	BAT *g = BATdescriptor(*gid);
	if (!g)
		throw(MAL, "part.prefixsum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	BUN n = 0, i;

	/* get max from gid */
	/*
	if (!BATmax(g, &n))
		throw(MAL, "part.prefixsum", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		*/
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
PARTpartition( bat *pos, const bat *part, const bat *glen )
{
	BAT *p = BATdescriptor(*part);
	BAT *g = BATdescriptor(*glen);
	if (!p || !g) {
		BBPreclaim(p);
		BBPreclaim(g);
		throw(MAL, "part.partition", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	part_t *pt = (part_t*)p->T.sink;
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

#define mat_project(T)																					\
		{																								\
			T **cp = (T**)GDKzalloc(mt->nr * sizeof(T*));												\
			if (cp) {																					\
				for(int i = 0; i<mt->nr; i++) {															\
					if (BATcapacity(mt->bat[i]) < (BUN)(curpos[i]+lp[i])) {								\
						if (BATextend(mt->bat[i], curpos[i]+lp[i]) != GDK_SUCCEED) {					\
							err = createException(MAL, "mat.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
							break;																		\
						}																				\
					}																					\
					if (BATcount(mt->bat[i]) < (BUN)(curpos[i]+lp[i]))									\
						BATsetcount(mt->bat[i], curpos[i]+lp[i]);										\
					cp[i] = (T*)Tloc(mt->bat[i], 0);													\
				}																						\
				if (err == NULL) {																		\
					T *dp = (T*)Tloc(d, 0);																\
					for(BUN i = 0; i<BATcount(d); i++) {												\
						int g = grp[i];																	\
						cp[g][curpos[g]] = dp[i];														\
						curpos[g]++;																	\
					}																					\
				}																						\
				GDKfree(cp);																			\
			} else {																					\
				err = createException(MAL, "mat.project", SQLSTATE(HY013) MAL_MALLOC_FAIL);				\
			}																							\
		}																								\
		break

static str
MATproject( bat *mat, const bat *pos, const bat *lid, const bat *gid, const bat *data )
{
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
	mat_t *mt = (mat_t*)m->T.sink;
	assert(mt->s.type == MAT_SINK);
	assert(mt->nr == (int)BATcount(p));
	assert(mt->nr == (int)BATcount(l));
	assert(BATcount(g) == BATcount(d));

	MT_lock_set(&m->theaplock);
	if (BATcount(d)) {
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
	}
	MT_lock_unset(&m->theaplock);
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

static str
MATfetch( bat *res, const bat *mat, const int *i )
{
	BAT *m = BATdescriptor(*mat);
	if (!m)
		throw(MAL, "mat.fetch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	mat_t *mt = (mat_t*)m->T.sink;
	assert(mt->s.type == MAT_SINK);
	assert(*i < mt->nr);
	BAT *b = mt->bat[*i];
	BBPunfix(m->batCacheid);
	BBPretain(*res = b->batCacheid);
	return MAL_SUCCEED;
}

static str
MATadd( bat *mat, const bat *bid, const int *i )
{
	BAT *b = BATdescriptor(*bid);
	BAT *m = BATdescriptor(*mat);
	if (!b || !m) {
		if (b) BBPunfix(b->batCacheid);
		throw(MAL, "mat.add", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	mat_t *mt = (mat_t*)m->T.sink;
	assert(mt->s.type == MAT_SINK);
	assert(*i < mt->nr);
	if (mt->bat[*i])
		BBPunfix(mt->bat[*i]->batCacheid);
	mt->bat[*i] = b;
	BBPkeepref(m);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func pp_mat_init_funcs[] = {
 pattern("mat", "new", MATnew, false, "Create mat for partitioning", args(1,3, batargany("mat",1),argany("tt",1),arg("nr",int))),
 pattern("part", "new", PARTnew, false, "Create part for partitioning", args(1,2, batarg("mat",oid),arg("nr",int))),
 command("part", "prefixsum", PARTprefixsum, false, "Count per group id", args(1,3, batarg("pos",lng),batarg("gid",lng),arg("max",lng))),
 command("part", "partition", PARTpartition, false, "Claim result positions for the given group lengths, returns first pos of each group", args(1,3, batarg("pos",lng),batarg("part",oid),batarg("grouplen",lng))),
 command("mat", "project", MATproject, false, "project over the partitions", args(1,5, batargany("mat",1),batarg("pos",lng), batarg("lid", lng), batarg("gid", lng), batargany("data",1))),
 command("mat", "fetch", MATfetch, false, "return i-th bat from mat", args(1,3, batargany("res",1),batargany("mat",1), arg("i", int))),
 command("mat", "add", MATadd, false, "add i-th bat to mat", args(1,3, batargany("mat",1),batargany("b",1), arg("i", int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mat_mal)
{ mal_module("pp_mat", NULL, pp_mat_init_funcs); }
