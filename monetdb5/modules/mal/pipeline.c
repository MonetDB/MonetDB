/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "algebra.h"

static str
PPcounter(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	int *res = getArgReference_int(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);

    *res = ATOMIC_INC(&p->counter);
	(void)cntxt; (void)mb;
	return MAL_SUCCEED;
}

#define sum(a,b) a+b
#define min(a,b) a<b?a:b
#define max(a,b) a>b?a:b

#define aggr(T,f)  \
	if (type == TYPE_##T) {								\
		T val = *getArgReference_##T(stk, pci, 2);		\
		if (val != T##_nil && BATcount(b)) {			\
			T *t = Tloc(b, 0);							\
			if (t[0] == T##_nil) {						\
				t[0] = val;								\
			} else										\
				t[0] = f(t[0], val);					\
			b->tnil = false;							\
			b->tnonil = true;							\
		} else if (BATcount(b) == 0) {					\
			if (BUNappend(b, &val, true) != GDK_SUCCEED)\
				err = createException(SQL, "aggr.sum",	\
					SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		}												\
	}

static str
LOCKEDAGGRsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "aggr.sum",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(hge,sum);
		aggr(lng,sum);
		aggr(int,sum);
		aggr(sht,sum);
		aggr(bte,sum);
		aggr(flt,sum);
		aggr(dbl,sum);
		if (!err)
			BBPkeepref(b->batCacheid);
		else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.sum",	"Result is not initialized");
	}
	MT_lock_unset(&p->l);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LOCKEDAGGRmin(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "aggr.min",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(hge,min);
		aggr(lng,min);
		aggr(int,min);
		aggr(sht,min);
		aggr(bte,min);
		aggr(flt,min);
		aggr(dbl,min);
		if (!err)
			BBPkeepref(b->batCacheid);
		else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.min",	"Result is not initialized");
	}
	MT_lock_unset(&p->l);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LOCKEDAGGRmax(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	bat *res = getArgReference_bat(stk, pci, 0);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 1);
	int type = getArgType(mb, pci, 2);
	str err = NULL;

	if (type != TYPE_hge && type != TYPE_lng && type != TYPE_int && type != TYPE_sht && type != TYPE_bte &&
			type != TYPE_flt && type != TYPE_dbl)
			return createException(SQL, "aggr.max",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(hge,max);
		aggr(lng,max);
		aggr(int,max);
		aggr(sht,max);
		aggr(bte,max);
		aggr(flt,min);
		aggr(dbl,min);
		if (!err)
			BBPkeepref(b->batCacheid);
		else
			BBPunfix(b->batCacheid);
	} else {
			err = createException(SQL, "aggr.max",	"Result is not initialized");
	}
	MT_lock_unset(&p->l);
	if (err)
		return err;
	(void)cntxt;
	return MAL_SUCCEED;
}

static str
LALGprojection(bat *result, const ptr *h, const bat *lid, const bat *rid)
{
	Pipeline *p = (Pipeline*)*h;
	str res;

	MT_lock_set(&p->l);
	res = ALGprojection(result, lid, rid);
	MT_lock_unset(&p->l);
	return res;
}

#define unique_2_naive(Type) \
		if (tt == TYPE_##Type) { \
            Type *bp = Tloc(b, 0);        \
			for(BUN i = 0; i<cnt; i++) { \
				BUN p;					 \
				bool fnd = 0;			 \
                HASHloop_##Type(ui, ui.b->thash, p, bp+i) { \
					fnd = 1;			 \
					break;				 \
				}						 \
				if (!fnd) {				 \
					if (BUNappend(u, bp+i, true) != GDK_SUCCEED) { \
						err = 1;							\
						break;								\
					}									    \
					ui.base = u->T.heap->base;				\
					gp[r++] = b->hseqbase + i;			    \
				}											\
			}												\
		}

#define unique_2_lock(Type) \
		if (tt == TYPE_##Type) { \
			Hash *hs = u->thash; \
            Type *bp = Tloc(b, 0); \
            Type *up = Tloc(u, 0); \
			BUN cur = BATcount(u); \
			\
			for(BUN i = 0; i<cnt; i++) { \
				bool fnd = 0; \
				BUN hb, prb = HASHprobe(hs, bp+i); \
                for (hb = HASHget(hs, prb); \
                    hb != BUN_NONE; \
                    hb = HASHgetlink(hs, hb)) { \
                    if (bp[i] == up[hb]) { \
						fnd = 1; \
                        break; \
					} \
                } \
				if (!fnd) { \
					BUN p = cur; \
					up[cur++] = bp[i]; \
                    HASHputlink(hs, p, HASHget(hs, prb)); \
                    HASHput(hs, prb, p); \
					gp[r++] = b->hseqbase + i; \
				} \
			} \
			BATsetcount(u, cur); \
		}

#if 0
static str
LALGunique(bat *gid, bat *uid, const ptr *h, bat *bid, bat *sid)
{
	Pipeline *p = (Pipeline*)*h;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *b = BATdescriptor(*bid);
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;

	if (u) {
		MT_lock_set(&p->l);
		BUN cnt = BATcount(b);

		BAT *g = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int err = 0, tt = b->ttype;
		oid *gp = Tloc(g, 0);
		BUN r = 0;
		BUN ucap = BATcapacity(u), ucnt = BATcount(u);

		if ((ucap - ucnt) < cnt) {
			if (BATextend(u, (ucap - ucnt) + cnt ) != GDK_SUCCEED)
				err = 1;
		}
        if (!err && BAThash(u) == GDK_SUCCEED) {
			unique(bte)
			unique(sht)
			unique(int)
			unique(lng)
			unique(hge)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BATsetcount(g, r);
			/* props */
			BBPkeepref(*uid = u->batCacheid);
			BBPkeepref(*gid = g->batCacheid);
		}
		MT_lock_unset(&p->l);
	}
	return MAL_SUCCEED;
}

#endif


#define GIDBITS 63
typedef lng gid;

typedef ATOMIC_TYPE hash_key_t;

#define HT_MIN_SIZE 1024*64
#define HT_MAX_SIZE 1024*1024*1024

typedef struct uhash_table {
        int type;
        int width;

        void *vals;			/* hash(ed) values */
        hash_key_t *gids;   /* chain of gids (k, ie mark used/-k mark used and value filled) */

        size_t size;
        gid mask;
} uhash_table;

static unsigned int
log_base2(unsigned int n)
{
        unsigned int l ;

        for (l = 0; n; l++) {
                n >>= 1 ;
        }
        return l ;
}

static uhash_table *
_ht_init( uhash_table *h )
{
        if (h->gids == NULL) {
                h->vals = (char*)GDKmalloc(h->size * (size_t)h->width);
                h->gids = (hash_key_t*)GDKzalloc(sizeof(hash_key_t)* h->size);
        }
        return h;
}

static uhash_table *
_ht_create( int type, int size)
{
        uhash_table *h = (uhash_table*)GDKzalloc(sizeof(uhash_table));
        int bits = log_base2(size-1);

        if (bits >= GIDBITS)
                bits = GIDBITS-1;
        h->size = (gid)1<<bits;
        h->mask = h->size-1;
        h->type = type;
        h->width = ATOMsize(type);
        return _ht_init(h);
}

static uhash_table *
ht_create(int type, int size)
{
        if (size < HT_MIN_SIZE)
                size = HT_MIN_SIZE;
        if (size > HT_MAX_SIZE)
                size = HT_MAX_SIZE;
        return _ht_create(type, size);
}


#define _hash_bit(X)  ((unsigned int)X)
#define _hash_bte(X)  ((unsigned int)X)
#define _hash_sht(X)  ((unsigned int)X)
#define _hash_int(X)  ((((unsigned int)X)>>7)^(((unsigned int)X)>>13)^(((unsigned int)X)>>21)^((unsigned int)X))
#define _hash_lng(X)  ((((ulng)X)>>7)^(((ulng)X)>>13)^(((ulng)X)>>21)^(((ulng)X)>>31)^(((ulng)X)>>38)^(((ulng)X)>>46)^(((ulng)X)>>56)^((ulng)X))

#define _mix_hge(X)      (((hge) (X) >> 7) ^     \
                         ((hge) (X) >> 13) ^    \
                         ((hge) (X) >> 21) ^    \
                         ((hge) (X) >> 31) ^    \
                         ((hge) (X) >> 38) ^    \
                         ((hge) (X) >> 46) ^    \
                         ((hge) (X) >> 56) ^    \
                         ((hge) (X) >> 65) ^    \
                         ((hge) (X) >> 70) ^    \
                         ((hge) (X) >> 78) ^    \
                         ((hge) (X) >> 85) ^    \
                         ((hge) (X) >> 90) ^    \
                         ((hge) (X) >> 98) ^    \
                         ((hge) (X) >> 107) ^   \
                         ((hge) (X) >> 116) ^   \
                         (hge) (X))
#define _hash_hge(X)  (_hash_lng(((lng)X) ^ _hash_lng((lng)(X>>64))))
//#define hash_hge(X)  ((lng)_mix_hge(X))
#define _hash_flt(X)  (_hash_int(X))
#define _hash_dbl(X)  (_hash_lng(X))
#define _hash_gid(X)  (_hash_lng(X))



static str
UHASHnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *res = getArgReference_bat(s, p, 0);
	int tt = getArgType(m, p, 1);
	int size = *getArgReference_int(s, p, 2);

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	b->T.ht = (void*)ht_create(tt, size*1.1);
	BBPkeepref(*res = b->batCacheid);
	return MAL_SUCCEED;
}

#define unique(Type) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(bp[i])&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && vals[k] != bp[i];) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, (k<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
				continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

#define funique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && vals[k] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, (k<<1))) { \
					vals[k] = bp[i]; \
					new = 1; \
					g = ATOMIC_INC(h->gids+k); \
				} \
				if ((g&1) == 0) \
				continue; \
				fnd = 1; \
			} \
			if (new) \
			gp[r++] = b->hseqbase + i; \
		} \
	}

static str
LALGunique(bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid)
{
	//Pipeline *p = (Pipeline*)*H;
	(void)H;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *b = BATdescriptor(*bid);
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;

	uhash_table *h = (uhash_table*)u->T.ht;
	assert(h);
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *g = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int err = 0, tt = b->ttype;
		oid *gp = Tloc(g, 0);
		BUN r = 0;

		if (!err) {
			unique(bte)
			unique(sht)
			unique(int)
			unique(lng)
			unique(hge)
			funique(flt, int)
			funique(dbl, lng)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BATsetcount(g, r);
			/* props */
			BBPkeepref(*uid = u->batCacheid);
			BBPkeepref(*rid = g->batCacheid);
		}
	}
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func pipeline_init_funcs[] = {
 pattern("pipeline", "counter", PPcounter, true, "return next atomic number [0..n>", args(1,2, arg("", int), arg("pipeline", ptr))),
 pattern("lockedaggr", "sum", LOCKEDAGGRsum, true, "sum values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "min", LOCKEDAGGRmin, true, "min values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "max", LOCKEDAGGRmax, true, "max values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 command("lockedalgebra", "projection", LALGprojection, false, "Project left input onto right input.", args(1,4, batargany("",3), arg("pipeline", ptr), batarg("left",oid),batargany("right",3))),
 command("algebra", "unique", LALGunique, false, "Project left input onto right input.", args(2,5, batarg("gid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid))),
 pattern("hash", "new", UHASHnew, false, "", args(1,3, batargany("",1),argany("tt",1),arg("size",int))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pipeline", NULL, pipeline_init_funcs); }
