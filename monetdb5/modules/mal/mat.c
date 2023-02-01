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
 * Martin Kersten
 * Multiple association tables
 * A MAT is a convenient way to deal represent horizontal fragmented
 * tables. It combines the definitions of several, type compatible
 * BATs under a single name.
 * It is produced by the mitosis optimizer and the operations
 * are the target of the mergetable optimizer.
 *
 * The MAT is materialized when the operations
 * can not deal with the components individually,
 * or the incremental operation is not supported.
 * Normally all mat.new() operations are removed by the
 * mergetable optimizer.
 * In case a mat.new() is retained in the code, then it will
 * behave as a mat.pack();
 *
 * The primitives below are chosen to accomodate the SQL
 * front-end to produce reasonable efficient code.
 */
#include "monetdb_config.h"
#include "mal_resolve.h"
#include "mal_exception.h"
#include "mal_interpreter.h"

#define PART_SINK 4
#define MAT_SINK 5

typedef struct mat_t {
	Sink s;
	int nr;
	BAT **bat;
} mat_t;

typedef struct part_t {
	Sink s;
	int nr;
	lng *curpos;
	MT_Lock l;
} part_t;

/*
 * The pack is an ordinary multi BAT insert. Oid synchronistion
 * between pieces should be ensured by the code generators.
 * The pack operation could be quite expensive, because it
 * may create a really large BAT.
 * The slice over a mat helps to avoid constructing intermediates
 * that are subsequently reduced.
 * Contrary to most operations, NIL arguments are skipped and
 * do not produce RUNTIME_OBJECT_MISSING.
 */
static BAT *
pack_mat(BAT *b)
{
	printf("pack mat\n");
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

static str
MATpackInternal(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i;
	bat *ret = getArgReference_bat(stk,p,0);
	BAT *b, *bn = NULL;
	BUN cap = 0;
	int tt = TYPE_any;
	int rt = getArgType(mb, p, 0), unmask = 0;
	(void) cntxt;

	for (i = 1; i < p->argc; i++) {
		bat bid = stk->stk[getArg(p,i)].val.bval;
		b = BBPquickdesc(bid);
		mat_t *mp = (mat_t*)b->T.sink;
		if (mp && mp->s.type == MAT_SINK) {
			bn = pack_mat(b);
			if (bn == NULL)
				throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);
			*ret = bn->batCacheid;
			BBPkeepref(bn);
			return MAL_SUCCEED;
		}

		if( b ){
			if (tt == TYPE_any)
				tt = b->ttype;
			if ((tt != TYPE_void && b->ttype != TYPE_void && b->ttype != TYPE_msk) && tt != b->ttype)
				throw(MAL, "mat.pack", "incompatible arguments");
			cap += BATcount(b);
		}
	}
	if (tt == TYPE_any){
		*ret = bat_nil;
		return MAL_SUCCEED;
	}

	if (tt == TYPE_msk && rt == newBatType(TYPE_oid)) {
		tt = TYPE_oid;
		unmask = 1;
	}
	bn = COLnew(0, tt, cap, TRANSIENT);
	if (bn == NULL)
		throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 1; i < p->argc; i++) {
		if (!(b = BATdescriptor(stk->stk[getArg(p,i)].val.ival))) {
			BBPreclaim(bn);
			throw(MAL, "mat.pack", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if ((unmask && b->ttype == TYPE_msk) || mask_cand(b)) {
			BAT *ob = b;
			b = BATunmask(b);
			BBPunfix(ob->batCacheid);
			if (!b) {
				BBPreclaim(bn);
				throw(MAL, "mat.pack", GDK_EXCEPTION);
			}
		}
		if (BATcount(bn) == 0) {
			BAThseqbase(bn, b->hseqbase);
			BATtseqbase(bn, b->tseqbase);
		}
		if (BATappend(bn, b, NULL, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", GDK_EXCEPTION);
		}
		BBPunfix(b->batCacheid);
	}
	if (bn->tnil && bn->tnonil) {
		BBPreclaim(bn);
		throw(MAL, "mat.pack", "INTERNAL ERROR" "bn->tnil or  bn->tnonil fails ");
	}
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

/*
 * Enable incremental packing. The SQL front-end requires
 * fixed oid sequences.
 */
static str
MATpackIncrement(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	bat *ret = getArgReference_bat(stk,p,0);
	int	pieces;
	BAT *b, *bb, *bn;
	size_t newsize;

	(void) cntxt;
	b = BATdescriptor( stk->stk[getArg(p,1)].val.ival);
	if ( b == NULL)
		throw(MAL, "mat.pack", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	if ( getArgType(mb,p,2) == TYPE_int){
		/* first step, estimate with some slack */
		pieces = stk->stk[getArg(p,2)].val.ival;
		int tt = ATOMtype(b->ttype);
		if (b->ttype == TYPE_msk)
			tt = TYPE_oid;
		bn = COLnew(b->hseqbase, tt, (BUN)(1.2 * BATcount(b) * pieces), TRANSIENT);
		if (bn == NULL) {
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		/* allocate enough space for the vheap, but not for strings,
		 * since BATappend does clever things for strings */
		if ( b->tvheap && bn->tvheap && ATOMstorage(b->ttype) != TYPE_str){
			newsize =  b->tvheap->size * pieces;
			if (HEAPextend(bn->tvheap, newsize, true) != GDK_SUCCEED) {
				BBPunfix(b->batCacheid);
				BBPreclaim(bn);
				throw(MAL, "mat.pack", GDK_EXCEPTION);
			}
		}
		BATtseqbase(bn, b->tseqbase);
		if (b->ttype == TYPE_msk || mask_cand(b)) {
			BAT *ob = b;
			b = BATunmask(b);
			BBPunfix(ob->batCacheid);
			if (!b) {
				BBPreclaim(bn);
				throw(MAL, "mat.pack", GDK_EXCEPTION);
			}
		}
		if (BATappend(bn, b, NULL, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", GDK_EXCEPTION);
		}
		bn->unused = (pieces-1); /* misuse "unused" field */
		BBPunfix(b->batCacheid);
		if (bn->tnil && bn->tnonil) {
			BBPreclaim(bn);
			throw(MAL, "mat.pack", "INTERNAL ERROR" " bn->tnil %d bn->tnonil %d", bn->tnil, bn->tnonil);
		}
		*ret = bn->batCacheid;
		BBPretain(bn->batCacheid);
		BBPunfix(bn->batCacheid);
	} else {
		/* remaining steps */
		if (!(bb = BATdescriptor(stk->stk[getArg(p,2)].val.ival))) {
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		}
		if (bb->ttype == TYPE_msk || mask_cand(bb)) {
			BAT *obb = bb;
			bb = BATunmask(bb);
			BBPunfix(obb->batCacheid);
			if (!bb) {
				BBPunfix(b->batCacheid);
				throw(MAL, "mat.pack", GDK_EXCEPTION);
			}
		}
		if (BATcount(b) == 0) {
			BAThseqbase(b, bb->hseqbase);
			BATtseqbase(b, bb->tseqbase);
		}
		if (BATappend(b, bb, NULL, false) != GDK_SUCCEED) {
			BBPunfix(bb->batCacheid);
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", GDK_EXCEPTION);
		}
		BBPunfix(bb->batCacheid);
		b->unused--;
		if (b->unused == 0 && (b = BATsetaccess(b, BAT_READ)) == NULL)
			throw(MAL, "mat.pack", GDK_EXCEPTION);
		if (b->tnil && b->tnonil) {
			BBPunfix(b->batCacheid);
			throw(MAL, "mat.pack", "INTERNAL ERROR" " b->tnil or  b->tnonil fails ");
		}
		*ret = b->batCacheid;
		BBPretain(b->batCacheid);
		BBPunfix(b->batCacheid);
	}
	return MAL_SUCCEED;
}

static str
MATpack(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	return MATpackInternal(cntxt,mb,stk,p);
}

static str
MATpackValues(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr p)
{
	int i, type, first = 1;
	bat *ret;
	BAT *bn;

	(void) cntxt;
	type = getArgType(mb,p,first);
	bn = COLnew(0, type, p->argc, TRANSIENT);
	if( bn == NULL)
		throw(MAL, "mat.pack", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (ATOMextern(type)) {
		for(i = first; i < p->argc; i++)
			if (BUNappend(bn, stk->stk[getArg(p,i)].val.pval, false) != GDK_SUCCEED)
				goto bailout;
	} else {
		for(i = first; i < p->argc; i++)
			if (BUNappend(bn, getArgReference(stk, p, i), false) != GDK_SUCCEED)
				goto bailout;
	}
	ret= getArgReference_bat(stk,p,0);
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
  bailout:
	BBPreclaim(bn);
	throw(MAL, "mat.pack", GDK_EXCEPTION);
}

static void
mat_destroy( mat_t *m )
{
	for(int i = 0; i<m->nr; i++) {
		if (m->bat[i])
			BBPunfix(m->bat[i]->batCacheid);
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

	if (mat) {
		mat->nr = nr;
		mat->bat = (BAT**)GDKzalloc(nr * sizeof(BAT*));
		if (!mat->bat)
			mat = NULL;
		mat->s.destroy = (sink_destroy)&mat_destroy;
		mat->s.type = MAT_SINK;
	}
	BAT *matb = COLnew(0, tt, 100000 /* need estimate? */, TRANSIENT);
	if (!matb || !mat) {
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

	if (part) {
		part->nr = nr;
		part->curpos = (lng*)GDKzalloc(nr * sizeof(lng));
		MT_lock_init(&part->l, "partition");
		if (!part->curpos)
			part = NULL;
		part->s.destroy = (sink_destroy)&part_destroy;
		part->s.type = PART_SINK;
	}
	BAT *partb = COLnew(0, TYPE_oid, 100000 /* need estimate? */, TRANSIENT);
	if (!partb || !part) {
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
	BBPkeepref(p);
	return MAL_SUCCEED;
}

static str
PARTpartition( bat *pos, const bat *part, const bat *glen )
{
	BAT *p = BATdescriptor(*part);
	BAT *g = BATdescriptor(*glen);
	if (!p || !g) {
		if (p) BBPunfix(p->batCacheid);
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
	BBPkeepref(posb);
	return MAL_SUCCEED;
}

#define mat_project(T)										\
		{													\
			int rc = GDK_SUCCEED;							\
			T **cp = (T**)GDKzalloc(mt->nr * sizeof(T*));	\
			for(int i = 0; i<mt->nr; i++) {					\
				if (BATcapacity(mt->bat[i]) < (BUN)(curpos[i]+lp[i]))	\
					rc = BATextend(mt->bat[i], curpos[i]+lp[i]);		\
				if (rc != GDK_SUCCEED)						\
					break;									\
				if (BATcount(mt->bat[i]) < (BUN)(curpos[i]+lp[i]))		\
					BATsetcount(mt->bat[i], curpos[i]+lp[i]);			\
				cp[i] = (T*)Tloc(mt->bat[i], 0);			\
			}												\
			if (cp && rc == GDK_SUCCEED) {					\
				T *dp = (T*)Tloc(d, 0);						\
				for(BUN i = 0; i<BATcount(d); i++) { 		\
					int g = grp[i];							\
					cp[g][curpos[g]] = dp[i];				\
					curpos[g]++;							\
			    }											\
			}												\
			GDKfree(cp);									\
		}													\
		break

static str
MATproject( bat *mat, const bat *pos, const bat *lid, const bat *gid, const bat *data )
{
	BAT *m = BATdescriptor(*mat);
	BAT *p = BATdescriptor(*pos);
	BAT *l = BATdescriptor(*lid);
	BAT *g = BATdescriptor(*gid);
	BAT *d = BATdescriptor(*data);
	if (!m || !p || !l || !g || !d) {
		if (m) BBPunfix(m->batCacheid);
		if (p) BBPunfix(p->batCacheid);
		if (l) BBPunfix(l->batCacheid);
		if (g) BBPunfix(g->batCacheid);
		throw(MAL, "mat.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	lng *curpos = (lng*)Tloc(p, 0);
	lng *lp = (lng*)Tloc(l, 0);
	lng *grp = (lng*)Tloc(g, 0);
	mat_t *mt = (mat_t*)m->T.sink;
	assert(mt->s.type == MAT_SINK);
	assert(mt->nr == (int)BATcount(p));
	assert(mt->nr == (int)BATcount(l));
	assert(BATcount(g) == BATcount(d));

	MT_lock_set(&m->theaplock);
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
		printf("error\n");
	}
	MT_lock_unset(&m->theaplock);
	BBPkeepref(m);
	BBPunfix(p->batCacheid);
	BBPunfix(l->batCacheid);
	BBPunfix(g->batCacheid);
	BBPunfix(d->batCacheid);
	return MAL_SUCCEED;
}

static str
MATfetch( bat *res, const bat *mat, const int *i )
{
	BAT *m = BATdescriptor(*mat);
	if (!m)
		throw(MAL, "mat.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
		throw(MAL, "mat.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
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
mel_func mat_init_funcs[] = {
 pattern("mat", "new", MATpack, false, "Define a Merge Association Table (MAT). Fall back to the pack operation\nwhen this is called ", args(1,2, batargany("",2),batvarargany("b",2))),
 pattern("bat", "pack", MATpackValues, false, "Materialize the values into a BAT. Avoiding a clash with mat.pack() in mergetable", args(1,2, batargany("",2),varargany("",2))),
 pattern("mat", "pack", MATpackValues, false, "Materialize the MAT (of values) into a BAT", args(1,2, batargany("",2),varargany("",2))),
 pattern("mat", "pack", MATpack, false, "Materialize the MAT into a BAT", args(1,2, batargany("",2),batvarargany("b",2))),
 pattern("mat", "packIncrement", MATpackIncrement, false, "Prepare incremental mat pack", args(1,3, batargany("",2),batargany("b",2),arg("pieces",int))),
 pattern("mat", "packIncrement", MATpackIncrement, false, "Prepare incremental mat pack", args(1,3, batargany("",2),batargany("b",2),batargany("c",2))),
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
{ mal_module("mat", NULL, mat_init_funcs); }
