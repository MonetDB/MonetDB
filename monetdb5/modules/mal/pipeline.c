/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_atoms.h"
#include "gdk_time.h"
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

#define getArgReference_date(stk, pci, nr) (date*)getArgReference(stk, pci, nr)
#define getArgReference_daytime(stk, pci, nr) (daytime*)getArgReference(stk, pci, nr)
#define getArgReference_timestamp(stk, pci, nr) (timestamp*)getArgReference(stk, pci, nr)

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
				err = createException(SQL, "aggr.##f",	\
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
		type != TYPE_flt && type != TYPE_dbl &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp)
			return createException(SQL, "aggr.min",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(date,min);
		aggr(daytime,min);
		aggr(timestamp,min);
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
		type != TYPE_flt && type != TYPE_dbl &&
		type != TYPE_date && type != TYPE_daytime && type != TYPE_timestamp)
			return createException(SQL, "aggr.max",	"Wrong input type (%d)", type);

	MT_lock_set(&p->l);
	if (*res) {
		BAT *b = BATdescriptor(*res);

		aggr(date,max);
		aggr(daytime,max);
		aggr(timestamp,max);
		aggr(hge,max);
		aggr(lng,max);
		aggr(int,max);
		aggr(sht,max);
		aggr(bte,max);
		aggr(flt,max);
		aggr(dbl,max);
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

#define GIDBITS 63
typedef lng gid;

typedef ATOMIC_TYPE hash_key_t;

#define HT_MIN_SIZE 1024*64
#define HT_MAX_SIZE 1024*1024*1024

typedef int (*fcmp)(void *v1, void *v2);
typedef lng (*fhsh)(void *v);

static int
str_cmp(str s1, str s2)
{
	return strcmp(s1,s2);
}

static lng
str_hsh(str v)
{
	lng key = 1;

	if (v) {
		for(;*v; v++) {
			key += ((lng)(*v));
			key += (key<<10);
			key ^= (key>>6);
		}
		key += (key << 3);
		key ^= (key >> 11);
		key += (key << 15);
	}
	return key;
}

typedef struct hash_table {
		Sink s;
        int type;
        int width;
		fcmp cmp;
		fhsh hsh;

        void *vals;			/* hash(ed) values */
        hash_key_t *gids;   /* chain of gids (k, ie mark used/-k mark used and value filled) */
		gid *pgids;			/* id of the parent hash */

		struct hash_table *p;	/* parent hash */
        size_t last;
        size_t size;
        gid mask;
} hash_table;

static unsigned int
log_base2(unsigned int n)
{
        unsigned int l ;

        for (l = 0; n; l++) {
                n >>= 1 ;
        }
        return l ;
}

#define HASH_SINK 1

static hash_table *
_ht_init( hash_table *h )
{
        if (h->gids == NULL) {
                h->vals = (char*)GDKmalloc(h->size * (size_t)h->width);
                h->gids = (hash_key_t*)GDKzalloc(sizeof(hash_key_t)* h->size);
				if (h->p) {
					assert(h->s.type == HASH_SINK);
					h->pgids = (gid*)GDKzalloc(sizeof(gid)* h->size);
				}
        }
        return h;
}

static void
ht_destroy(hash_table *ht)
{
	if (ht->vals)
		GDKfree(ht->vals);
	if (ht->gids)
		GDKfree((void*)ht->gids);
	if (ht->pgids)
		GDKfree(ht->pgids);
	GDKfree(ht);
}

static hash_table *
_ht_create( int type, int size, hash_table *p)
{
        hash_table *h = (hash_table*)GDKzalloc(sizeof(hash_table));
        int bits = log_base2(size-1);

		h->s.destroy = (sink_destroy)&ht_destroy;
		h->s.type = HASH_SINK;
        if (bits >= GIDBITS)
                bits = GIDBITS-1;
        h->size = (gid)1<<bits;
        h->mask = h->size-1;
        h->type = type;
        h->width = ATOMsize(type);
		h->last = 0;
		h->p = p;
		if (type == TYPE_str) {
			h->cmp = (fcmp)str_cmp;
			h->hsh = (fhsh)str_hsh;
		}
        return _ht_init(h);
}

static hash_table *
ht_create(int type, int size, hash_table *p)
{
        if (size < HT_MIN_SIZE)
                size = HT_MIN_SIZE;
        if (size > HT_MAX_SIZE)
                size = HT_MAX_SIZE;
        return _ht_create(type, size, p);
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
#define ROT64(x, y)  ((x << y) | (x >> (64 - y)))
#define combine(X,Y)  (X^Y)

//(_hash_lng(ROT64(X, 3) ^ ROT64((lng)Y, 17)))

static str
UHASHnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *res = getArgReference_bat(s, p, 0);
	int tt = getArgType(m, p, 1);
	int size = *getArgReference_int(s, p, 2);
	hash_table *parent = NULL;
	if (p->argc == 4) {
		bat pid = *getArgReference_bat(s, p, 3);
		BAT *p = BATdescriptor(pid);
		parent = (hash_table*)p->T.sink;
		BBPunfix(p->batCacheid);
	}

	BAT *b = COLnew(0, tt, 0, TRANSIENT);
	b->T.sink = (Sink*)ht_create(tt, size*1.2*2.1, parent);
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
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
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
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
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

#define aunique(Type) \
	if (tt == TYPE_##Type) { \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				Type bpi = BUNtvar(bi, i); \
				gid k = (gid)h->hsh(bpi)&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (h->cmp(vals[k], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bpi; \
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
		bat_iterator_end(&bi); \
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

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
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
			aunique(str)
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

#define gunique(Type) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(bp[i]))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || vals[k] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
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

#define gfunique(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				gid k = (gid)combine(p[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[k])) && vals[k] != bp[i]));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bp[i]; \
					pgids[k] = p[i]; \
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

#define gaunique(Type) \
	if (tt == TYPE_##Type) { \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool new = 0, fnd = 0; \
			\
			for(; !fnd; ) { \
				Type bpi = BUNtvar(bi, i); \
				gid k = (gid)combine(p[i], h->hsh(bpi))&h->mask; \
				gid g = ATOMIC_GET(h->gids+k); \
				for(;g&1 && (pgids[k] != p[i] || h->cmp(vals[k], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g && ATOMIC_CAS(h->gids+k, &expected, ((k+1)<<1))) { \
					vals[k] = bpi; \
					pgids[k] = p[i]; \
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
		bat_iterator_end(&bi); \
	}

static str
LALGgroup_unique(bat *rid, bat *uid, const ptr *H, bat *bid, bat *sid, bat *Gid)
{
	//Pipeline *p = (Pipeline*)*H;
	(void)H;
	assert(*uid && !is_bat_nil(*uid));
	BAT *u = BATdescriptor(*uid);
	BAT *G = BATdescriptor(*Gid);
	BAT *b = BATdescriptor(*bid);
	assert(is_bat_nil(*sid)); /* no cands jet */
	(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *ng = COLnew(0, TYPE_oid, cnt, TRANSIENT);

		/* probably need bat resize and create hash */
		int err = 0, tt = b->ttype;
		oid *gp = Tloc(ng, 0);
		gid *p = Tloc(G, 0);
		gid *pgids = h->pgids;
		BUN r = 0;

		if (!err) {
			gunique(bte)
			gunique(sht)
			gunique(int)
			gunique(lng)
			gunique(hge)
			gfunique(flt, int)
			gfunique(dbl, lng)
			gaunique(str)
		}
		if (!err) {
			BBPunfix(G->batCacheid);
			BBPunfix(b->batCacheid);
			BATsetcount(ng, r);
			/* props */
			BBPkeepref(*uid = u->batCacheid);
			BBPkeepref(*rid = ng->batCacheid);
		}
	}
	return MAL_SUCCEED;
}

#define PRE_CLAIM 4
#define group(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)_hash_##Type(bp[i])&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && vals[g] != bp[i];) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define fgroup(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)_hash_##Type(*(((BaseType*)bp)+i))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define agroup(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)str_hsh(bpi)&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (vals[g] && h->cmp(vals[g], bpi) != 0);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}


static str
LALGgroup(bat *rid, bat *uid, const ptr *H, bat *bid/*, bat *sid*/)
{
	//Pipeline *p = (Pipeline*)*H;
	(void)H;
	/* private or not */
	bool private = (!*uid || is_bat_nil(*uid));

	BAT *u, *b = BATdescriptor(*bid);
	if (private && !*uid) { /* create but how big ??? */
		u = COLnew(b->hseqbase, b->ttype, 0, TRANSIENT);
		u->T.sink = (Sink*)ht_create(b->ttype, 1, NULL);
	} else {
		u = BATdescriptor(*uid);
	}
	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);

		int err = 0, tt = b->ttype;
		oid *gp = Tloc(g, 0);

		if (!err) {
			group(bte)
			group(sht)
			group(int)
			group(lng)
			group(hge)
			fgroup(flt, int)
			fgroup(dbl, lng)
			agroup(str)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BATsetcount(g, cnt);
			/* props */
			gid last = ATOMIC_GET(&h->last);
			/* pass max id */
			g->T.maxval = last;
			g->tkey = FALSE;
			/* this will cause a double free */
			/* TODO instead we should solve the max val propagation */
			//h->s.destroy = NULL;
			//g->T.sink = h;
			BBPkeepref(*uid = u->batCacheid);
			BBPkeepref(*rid = g->batCacheid);
		}
	}
	return MAL_SUCCEED;
}

#define derive(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)combine(p[i], _hash_##Type(bp[i]))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != p[i] || vals[g] != bp[i]);) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = p[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define fderive(Type, BaseType) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		Type *bp = Tloc(b, 0); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			gid k = (gid)combine(p[i], _hash_##Type(*(((BaseType*)bp)+i)))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != p[i] || (!(is_##Type##_nil(bp[i]) && is_##Type##_nil(vals[g])) && vals[g] != bp[i]));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bp[i]; \
					pgids[g] = p[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
	}

#define aderive(Type) \
	if (tt == TYPE_##Type) { \
		int slots = 0; \
		gid slot = 0; \
		BATiter bi = bat_iterator(b); \
		Type *vals = h->vals; \
		\
		for(BUN i = 0; i<cnt; i++) { \
			bool fnd = 0; \
			Type bpi = (void *) ((bi).vh->base+BUNtvaroff(bi,i)); \
			gid k = (gid)combine(p[i], str_hsh(bpi))&h->mask; \
			gid g = 0; \
			\
			for(; !fnd; ) { \
				g = ATOMIC_GET(h->gids+k); \
				for(;g && (pgids[g] != p[i] || (vals[g] && h->cmp(vals[g], bpi) != 0));) { \
					k++; \
					k &= h->mask; \
					g = ATOMIC_GET(h->gids+k); \
				} \
				if (!g) { \
					if (slots == 0) { \
						slots = PRE_CLAIM; \
						slot = ATOMIC_ADD(&h->last, PRE_CLAIM); \
					} \
					slots--; \
					g = ++slot; \
					vals[g] = bpi; \
					pgids[g] = p[i]; \
					if (!ATOMIC_CAS(h->gids+k, &expected, g)) \
						continue; \
				} \
				fnd = 1; \
			} \
			gp[i] = g-1; \
		} \
		bat_iterator_end(&bi); \
	}


static str
LALGderive(bat *rid, bat *uid, const ptr *H, bat *Gid, bat *bid /*, bat *sid*/)
{
	//Pipeline *p = (Pipeline*)*H;
	(void)H;
	bool private = (!*uid || is_bat_nil(*uid));
	BAT *u, *b = BATdescriptor(*bid);
	BAT *G = BATdescriptor(*Gid);
	if (private && !*uid) { /* create but how big ??? */
		u = COLnew(b->hseqbase, b->ttype, 0, TRANSIENT);
		u->T.sink = (Sink*)ht_create(b->ttype, 1, (hash_table*)G->T.sink);
	} else {
		u = BATdescriptor(*uid);
	}
	//assert(is_bat_nil(*sid)); /* no cands jet */
	//(void)sid;

	hash_table *h = (hash_table*)u->T.sink;
	assert(h && h->s.type == HASH_SINK);
	if (h) {
		ATOMIC_BASE_TYPE expected = 0;
		BUN cnt = BATcount(b);

		BAT *g = COLnew(b->hseqbase, TYPE_oid, cnt, TRANSIENT);

		int err = 0, tt = b->ttype;
		oid *gp = Tloc(g, 0);
		gid *p = Tloc(G, 0);
		gid *pgids = h->pgids;

		if (!err) {
			derive(bte)
			derive(sht)
			derive(int)
			derive(lng)
			derive(hge)
			fderive(flt, int)
			fderive(dbl, lng)
			aderive(str)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(G->batCacheid);
			BATsetcount(g, cnt);
			/* props */
			gid last = ATOMIC_GET(&h->last);
			/* pass max id */
			g->T.maxval = last;
			g->tkey = FALSE;
			BBPkeepref(*uid = u->batCacheid);
			BBPkeepref(*rid = g->batCacheid);
		}
	}
	return MAL_SUCCEED;
}

#define project(Type) \
	if (ATOMstorage(tt) == TYPE_##Type) { \
		Type *v = Tloc(b, 0); \
		Type *o = Tloc(r, 0); \
		for(BUN i = 0; i<cnt; i++) { \
			o[gp[i]] = v[i]; \
		} \
	}

#define aproject(Type,w,Toff) \
	if (ATOMstorage(tt) == TYPE_##Type && b->twidth == w) { \
		Toff *v = Tloc(b, 0); \
		Toff *o = Tloc(r, 0); \
		for(BUN i = 0; i<cnt; i++) { \
			o[gp[i]] = v[i]; \
		} \
	}

/* result := algebra.projections(groupid, input)  */
/* this (possibly) overwrites the values, therefor for expensive (var) types we only write offsets (ie use the heap from
 * the parent) */
static str
LALGproject(bat *rid, bat *gid, bat *bid, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	oid max = g->T.maxval;
	int err = 0;

	/* probably need bat resize and create hash */
	if (*rid)
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	/* probably want to use a per 'r' lock, but the r->theaplock is blocking this on BATextend and BATsetcount */
	if (!private)
		MT_lock_set(&p->l);
	if (r) {
		if (r->ttype == TYPE_str && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
		   	if (r->twidth < b->twidth) {
				int m = b->twidth / r->twidth;
				r->twidth = b->twidth;
				r->tshift = b->tshift;
				r->batCapacity /= m;
			}

			HEAPdecref(r->tvheap, r->tvheap->parentid == r->batCacheid);
			HEAPincref(b->tvheap);
			r->tvheap = b->tvheap;
			BBPshare(b->tvheap->parentid);
			r->batDirtydesc = true;
		}
	} else {
		if (b->ttype == TYPE_str) {
			r = COLnew2(0, b->ttype, max, TRANSIENT, b->twidth);
			HEAPdecref(r->tvheap, r->tvheap->parentid == r->batCacheid);
			HEAPincref(b->tvheap);
			r->tvheap = b->tvheap;
			BBPshare(b->tvheap->parentid);
			r->batDirtydesc = true;
		} else {
			r = COLnew(0, b->ttype, max, TRANSIENT);
		}
		r->T.private_bat = 1;
	}
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
	}

	/* get max id from gid */
	if (!err) {
		BUN cnt = BATcount(b);

		int err = 0, tt = b->ttype;
		oid *gp = Tloc(g, 0);

		if (!err) {
			project(bte)
			project(sht)
			project(int)
			project(lng)
			project(hge)
			project(flt)
			project(dbl)
			aproject(str,1,bte)
			aproject(str,2,sht)
			aproject(str,4,int)
			aproject(str,8,lng)
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			BATsetcount(r, max);
			BBPkeepref(*rid = r->batCacheid);
		}
	}
	if (err)
	printf(" error \n");
	if (!private)
		MT_lock_unset(&p->l);
	return MAL_SUCCEED;
}

static str
LALGcountstar(bat *rid, bat *gid, const ptr *H, bat *pid)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *r = NULL;
	int err = 0;

	if (*rid)
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		MT_lock_set(&p->l);

	BAT *pg = BATdescriptor(*pid);
	oid max = pg->T.maxval;
	BBPunfix(pg->batCacheid);

	if (!r) {
		r = COLnew(0, TYPE_lng, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
		memset(Tloc(r, cnt), 0, sizeof(lng)*(sz-cnt));
	} else if (cnt == 0) {
		BUN sz = BATcapacity(r);
		memset(Tloc(r, 0), 0, sizeof(lng)*sz);
	}

	if (!err) {
		BUN cnt = BATcount(g);

		int err = 0;

		if (!err) {
			oid *v = Tloc(g, 0);
			lng *o = Tloc(r, 0);
			for(BUN i = 0; i<cnt; i++)
				o[v[i]]++;
		}
		if (!err) {
			BBPunfix(g->batCacheid);
			BATsetcount(r, max);
			r->trevsorted = r->tsorted = FALSE;
			r->tkey = FALSE;
			BBPkeepref(*rid = r->batCacheid);
		}
	}
	if (!private)
		MT_lock_unset(&p->l);
	return MAL_SUCCEED;
}

static str
LALGcount(bat *rid, bat *gid, bat *bid, bit *nonil, const ptr *H, bat *pid)
{
	(void)bid;
	(void)nonil;
	return LALGcountstar(rid, gid, H, pid);
}

#define gsum(OutType, InType) \
	if (tt == TYPE_##InType && ot == TYPE_##OutType) { \
			InType *in = Tloc(b, 0); \
			OutType *o = Tloc(r, 0); \
			for(BUN i = 0; i<cnt; i++) \
				o[grp[i]] += in[i]; \
	}

static str
//LALGsum(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
LALGsum(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void)cntxt;
	bat *rid = getArgReference_bat(stk, pci, 0);
	bat *gid = getArgReference_bat(stk, pci, 1);
	bat *bid = getArgReference_bat(stk, pci, 2);
	Pipeline *p = (Pipeline*)*getArgReference_ptr(stk, pci, 3); /* last arg should move to first argument .. */
	bat *pid = getArgReference_bat(stk, pci, 4);

	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0;

	if (*rid)
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		MT_lock_set(&p->l);

	BAT *pg = BATdescriptor(*pid);
	oid max = pg->T.maxval;
	BBPunfix(pg->batCacheid);

	if (!r) {
		int tt = getBatType(getArgType(mb, pci, 0));
		r = COLnew(b->hseqbase, tt, max, TRANSIENT);
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
		memset(Tloc(r, cnt), 0, r->twidth*(sz-cnt));
	} else if (cnt == 0) {
		BUN sz = BATcapacity(r);
		memset(Tloc(r, 0), 0, r->twidth*sz);
	}

	if (!err) {
		BUN cnt = BATcount(g);
		int tt = b->ttype, ot = r->ttype;

		if (!err) {
			oid *grp = Tloc(g, 0);

			gsum(bte,bte);
			gsum(sht,bte);
			gsum(sht,sht);
			gsum(int,bte);
			gsum(int,sht);
			gsum(int,int);
			gsum(lng,bte);
			gsum(lng,sht);
			gsum(lng,int);
			gsum(lng,lng);
			gsum(hge,bte);
			gsum(hge,sht);
			gsum(hge,int);
			gsum(hge,lng);
			gsum(hge,hge);
			gsum(flt,flt);
			gsum(dbl,flt);
			gsum(dbl,dbl);
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			BATsetcount(r, max);
			r->trevsorted = r->tsorted = FALSE;
			r->tkey = FALSE;
			BBPkeepref(*rid = r->batCacheid);
		}
	}
	if (!private)
		MT_lock_unset(&p->l);
	return MAL_SUCCEED;
}

#define gfunc(Type, f) \
	if (ATOMstorage(tt) == TYPE_##Type) { \
			Type *in = Tloc(b, 0); \
			Type *o = Tloc(r, 0); \
			for(BUN i = 0; i<cnt; i++) \
				o[grp[i]] = f(o[grp[i]], in[i]); \
	}

/* fron now assume shared heap */
#define vamin(cmp, o, opos, op, in, i, bp) \
	if (!o[opos] || cmp(op+o[opos], bp+in[i]) > 0) \
		o[opos] = in[i];
#define vamax(cmp, o, opos, op, in, i, bp) \
	if (!o[opos] || cmp(op+o[opos], bp+in[i]) < 0) \
		o[opos] = in[i];

#define gafunc(f) \
	if (ATOMextern(tt) && ATOMvarsized(tt)) { \
			BATiter bi = bat_iterator(b); \
			BATiter ri = bat_iterator(r); \
			char *bp = bi.vh->base; \
			char *op = ri.vh->base; \
		    int (*cmp)(const void *v1,const void *v2) = ATOMcompare(tt); \
			if (b->twidth == 1) { \
				bp += GDK_VAROFFSET; \
				op += GDK_VAROFFSET; \
				uint8_t *in = Tloc(b, 0); \
				uint8_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp); \
			} else if (b->twidth == 2) { \
				bp += GDK_VAROFFSET; \
				op += GDK_VAROFFSET; \
				uint16_t *in = Tloc(b, 0); \
				uint16_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp); \
			} else if (b->twidth == 4) { \
				uint32_t *in = Tloc(b, 0); \
				uint32_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp); \
			} else if (b->twidth == 8) { \
				var_t *in = Tloc(b, 0); \
				var_t *o = Tloc(r, 0); \
				for(BUN i = 0; i<cnt; i++) \
					f(cmp, o, grp[i], op, in, i, bp); \
			} \
			bat_iterator_end(&bi); \
			bat_iterator_end(&ri); \
	}

static str
LALGmin(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	Pipeline *p = (Pipeline*)H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0, tt = b->ttype;

	if (*rid)
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		MT_lock_set(&p->l);

	BAT *pg = BATdescriptor(*pid);
	oid max = pg->T.maxval;
	BBPunfix(pg->batCacheid);

	if (r) {
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
		   	if (r->twidth < b->twidth) {
				int m = b->twidth / r->twidth;
				r->twidth = b->twidth;
				r->tshift = b->tshift;
				r->batCapacity /= m;
			}

			//int parentid = b->tvheap->parentid;
			assert (VIEWvtparent(b));
			HEAPdecref(r->tvheap, r->tvheap->parentid == r->batCacheid);
			HEAPincref(b->tvheap);
			r->tvheap = b->tvheap;
			BBPshare(b->tvheap->parentid);
			r->batDirtydesc = true;
		}
	} else {
		if (b->ttype == TYPE_str) {
			r = COLnew2(0, b->ttype, max, TRANSIENT, b->twidth);
			HEAPdecref(r->tvheap, r->tvheap->parentid == r->batCacheid);
			HEAPincref(b->tvheap);
			r->tvheap = b->tvheap;
			BBPshare(b->tvheap->parentid);
			r->batDirtydesc = true;
		} else {
			r = COLnew(0, b->ttype, max, TRANSIENT);
		}
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
		/* todo correctly initialize */
		memset(Tloc(r, cnt), 0, r->twidth*(sz-cnt));
	} else if (cnt == 0) {
		BUN sz = BATcapacity(r);
		/* todo correctly initialize */
		memset(Tloc(r, 0), 0, r->twidth*sz);
	}
	assert(b->twidth == r->twidth);

	if (!err) {
		BUN cnt = BATcount(g);

		if (!err) {
			oid *grp = Tloc(g, 0);

			gfunc(bte,min);
			gfunc(sht,min);
			gfunc(int,min);
			gfunc(lng,min);
			gfunc(hge,min);
			gfunc(flt,min);
			gfunc(dbl,min);
			gafunc(vamin);
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			BATsetcount(r, max);
			r->trevsorted = r->tsorted = FALSE;
			r->tkey = FALSE;
			BBPkeepref(*rid = r->batCacheid);
		}
	}
	if (!private)
		MT_lock_unset(&p->l);
	return MAL_SUCCEED;
}

static str
LALGmax(bat *rid, bat *gid, bat *bid, const ptr *H, bat *pid)
{
	Pipeline *p = (Pipeline*)H; /* last arg should move to first argument .. */
	BAT *g = BATdescriptor(*gid);
	BAT *b = BATdescriptor(*bid);
	BAT *r = NULL;
	int err = 0, tt = b->ttype;

	if (*rid)
		r = BATdescriptor(*rid);
	bool private = (!r || r->T.private_bat);

	if (!private)
		MT_lock_set(&p->l);

	BAT *pg = BATdescriptor(*pid);
	oid max = pg->T.maxval;
	BBPunfix(pg->batCacheid);

	if (r) {
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
		   	if (r->twidth < b->twidth) {
				int m = b->twidth / r->twidth;
				r->twidth = b->twidth;
				r->tshift = b->tshift;
				r->batCapacity /= m;
			}

			printf("view parent %d\n", VIEWvtparent(b));
			assert (VIEWvtparent(b));
			HEAPdecref(r->tvheap, r->tvheap->parentid == r->batCacheid);
			HEAPincref(b->tvheap);
			r->tvheap = b->tvheap;
			BBPshare(b->tvheap->parentid);
			r->batDirtydesc = true;
		}
	} else {
		if (b->ttype == TYPE_str) {
			r = COLnew2(0, b->ttype, max, TRANSIENT, b->twidth);
			HEAPdecref(r->tvheap, r->tvheap->parentid == r->batCacheid);
			HEAPincref(b->tvheap);
			r->tvheap = b->tvheap;
			BBPshare(b->tvheap->parentid);
			r->batDirtydesc = true;
		} else {
			r = COLnew(0, b->ttype, max, TRANSIENT);
		}
		r->T.private_bat = 1;
	}
	BUN cnt = BATcount(r);
	if (BATcapacity(r) < max) {
		BUN sz = max*2;
		if (BATextend(r, sz) != GDK_SUCCEED)
			err = 1;
		/* todo correctly initialize */
		memset(Tloc(r, cnt), 0, r->twidth*(sz-cnt));
	} else if (cnt == 0) {
		BUN sz = BATcapacity(r);
		/* todo correctly initialize */
		memset(Tloc(r, 0), 0, r->twidth*sz);
	}
	assert(b->twidth == r->twidth);

	if (!err) {
		BUN cnt = BATcount(g);

		if (!err) {
			oid *grp = Tloc(g, 0);

			gfunc(bte,max);
			gfunc(sht,max);
			gfunc(int,max);
			gfunc(lng,max);
			gfunc(hge,max);
			gfunc(flt,max);
			gfunc(dbl,max);
			gafunc(vamax);
		}
		if (!err) {
			BBPunfix(b->batCacheid);
			BBPunfix(g->batCacheid);
			BATsetcount(r, max);
			r->trevsorted = r->tsorted = FALSE;
			r->tkey = FALSE;
			BBPkeepref(*rid = r->batCacheid);
		}
	}
	if (!private)
		MT_lock_unset(&p->l);
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func pipeline_init_funcs[] = {
 pattern("pipeline", "counter", PPcounter, true, "return next atomic number [0..n>", args(1,2, arg("", int), arg("pipeline", ptr))),
 pattern("lockedaggr", "sum", LOCKEDAGGRsum, true, "sum values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "min", LOCKEDAGGRmin, true, "min values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 pattern("lockedaggr", "max", LOCKEDAGGRmax, true, "max values into bat (bat has value, update), using the bat lock", args(1,3, sharedbatargany("", 1), arg("pipeline", ptr), argany("val", 1))),
 command("lockedalgebra", "projection", LALGprojection, false, "Project left input onto right input.", args(1,4, batargany("",3), arg("pipeline", ptr), batarg("left",oid),batargany("right",3))),
 command("algebra", "unique", LALGunique, false, "Unique rows.", args(2,5, batarg("gid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid))),
 command("algebra", "unique", LALGgroup_unique, false, "Unique per group rows.", args(2,6, batarg("ngid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",3), batarg("s",oid), batarg("gid",oid))),
 command("group", "group", LALGgroup, false, "Group input.", args(2,4, batarg("gid", oid), batargany("",3), arg("pipeline", ptr), batargany("b",4))),
 command("group", "group", LALGderive, false, "Sub Group input.", args(2,5, batarg("gid", oid), batargany("",3), arg("pipeline", ptr), batarg("pgid", oid), batargany("b",4))),
 command("algebra", "projection", LALGproject, false, "Project.", args(1,4, batargany("",1), batarg("gid", oid), batargany("b",1), arg("pipeline", ptr))),
 command("aggr", "count", LALGcount, false, "Project.", args(1,6, batarg("",lng), batarg("gid", oid), batargany("", 1), arg("nonil", bit), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "count", LALGcountstar, false, "count per group.", args(1,4, batarg("",lng), batarg("gid", oid), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("aggr", "sum", LALGsum, false, "sum per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 2), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "min", LALGmin, false, "Min per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 command("aggr", "max", LALGmax, false, "Max per group.", args(1,5, batargany("",1), batarg("gid", oid), batargany("", 1), arg("pipeline", ptr), batarg("pid", oid))),
 pattern("hash", "new", UHASHnew, false, "", args(1,3, batargany("",1),argany("tt",1),arg("size",int))),
 pattern("hash", "new", UHASHnew, false, "", args(1,4, batargany("",1),argany("tt",1),arg("size",int), batargany("p",2))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_pipeline_mal)
{ mal_module("pipeline", NULL, pipeline_init_funcs); }
