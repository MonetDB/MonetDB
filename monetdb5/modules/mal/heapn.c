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

/* todo move into GDK ! */
static void
BATnegateprops(BAT *b)
{
    /* disable all properties here */
    b->tnonil = false;
    b->tnil = false;
    if (b->ttype) {
        b->tsorted = false;
        b->trevsorted = false;
        b->tnosorted = 0;
        b->tnorevsorted = 0;
    }
    b->tseqbase = oid_nil;
    b->tkey = false;
    b->tnokey[0] = 0;
    b->tnokey[1] = 0;
    b->tmaxpos = b->tminpos = BUN_NONE;
}

#define pipeline_lock(p) MT_lock_set(&p->p->l)
#define pipeline_unlock(p) MT_lock_unset(&p->p->l)

#define pipeline_lock1(r) MT_lock_set(&r->batIdxLock)
#define pipeline_unlock1(r) MT_lock_unset(&r->batIdxLock)

#define pipeline_lock2(r) MT_lock_set(&r->theaplock)
#define pipeline_unlock2(r) MT_lock_unset(&r->theaplock)
/*
 * Min/Max processing using heap structure
 * ie array implementation
 */
typedef int (*fcmp)(void *v1, void *v2, void *hp);
typedef int (*fcmp2)(void *v1, void *v2);
typedef void *(*fnil)();
typedef void *mallocator;
extern void ma_destroy(mallocator* ma);
extern mallocator * ma_create(void);
extern void * ma_alloc( mallocator *ma, size_t sz );
typedef lng gid;
typedef lng sel_t;

typedef struct subheap {
	fcmp cmp;		/* cmp function for complex types */
	fcmp2 cmp2;		/* cmp function for complex types */
	fnil nilptr;		/* is nil */

	void *vals;		/* array values */
	void *ivals;		/* input pointer */
	BAT *in;
	BATiter bi;
	struct subheap *sub;	/* for multi attribute topn, next value array and function ptrs */
	int type;
	int width;
	bool min;	/* or max heap */
	bool var;
	bool external;
} subheap;

typedef struct heapn {
	Sink s;
	size_t size;
	size_t used;

	mallocator *ma;	/* allocator for variable data */

	bool shared;		/* PRIVATE or SHARED */
	struct subheap *sub;	/* for multi attribute topn, next value array and function ptrs */
} heapn;

extern heapn *heapn_create( int size, bool shared);
extern heapn *heapn_subheap( heapn *hp, int type, bool min /* or max */, bool nulls_last);
extern heapn *heapn_done( heapn *hp ); /* done creating subheaps */

static heapn *_heap_create( int size, bool shared );
static subheap *subheap_create( heapn *hp, int type, bool min /* or max */, bool nulls_last);

#define HEAP_INIT(h) if (h->sub->vals == NULL) _heap_init(h);

static void
_heap_init( heapn *hp )
{
        //printf("heap_init(%c) %ld\n", hp->shared?'S':'L', hp->size);
        for (subheap *sh = hp->sub; sh && !sh->vals; sh = sh->sub) {
                sh->vals = (char*)GDKzalloc(hp->size*sh->width);
        }
}

static int
subheap_down( subheap *sh, size_t q, size_t l)
{
	char *vals = sh->vals;
	int cmp = 0;
	if (sh->var) {
		void **nvals = (void**)vals;
 		cmp = sh->cmp(nvals[q], nvals[l], sh);
	} else
		cmp = sh->cmp(vals+(q*sh->width), vals+(l*sh->width), sh);

	if (!cmp && sh->sub)
		q = subheap_down(sh->sub, q, l);
	else if (sh->min && cmp > 0)
		q = l;
	else if (!sh->min && cmp < 0)
		q = l;
	return q;
}

static int
subheap_up( subheap *sh, size_t q, size_t p)
{
	char *vals = sh->vals;
	int cmp = 0;
	if (sh->var) {
		void **nvals = (void**)vals;
 		cmp = sh->cmp(nvals[q], nvals[p], sh);
	} else
		cmp = sh->cmp(vals+(q*sh->width), vals+(p*sh->width), sh);

	if (!cmp && sh->sub)
		q = subheap_up(sh->sub, q, p);
	else if (sh->min && cmp < 0)
		q = p;
	else if (!sh->min && cmp > 0)
		q = p;
	return q;
}

static bool
subheap_newroot( subheap *sh, size_t p)
{
	char *vals = sh->vals;
	char *ivals = sh->ivals;
	int cmp = 0;
	if (sh->var) {
		void *val = BUNtvar(sh->bi, p);
		void **nvals = (void**)vals;
 		cmp = sh->cmp(nvals[0], val, sh);
	} else
 		cmp = sh->cmp(vals, ivals+(p*sh->width), sh);
	bool newroot = false;

	if (!cmp && sh->sub)
		newroot = subheap_newroot(sh->sub, p);
	else if (sh->min && cmp < 0)
		newroot = true;
	else if (!sh->min && cmp > 0)
		newroot = true;
	return newroot;
}

static void
subheap_swap( subheap *sh, size_t p, size_t q)
{
	switch(sh->width) {
	case 1: {
		int8_t *vals = sh->vals;
		int8_t v = vals[p];
		vals[p] = vals[q];
		vals[q] = v;
	} break;
	case 2: {
		int16_t *vals = sh->vals;
		int16_t v = vals[p];
		vals[p] = vals[q];
		vals[q] = v;
	} break;
	case 4: {
		int32_t *vals = sh->vals;
		int32_t v = vals[p];
		vals[p] = vals[q];
		vals[q] = v;
	} break;
	case 8: {
		int64_t *vals = sh->vals;
		int64_t v = vals[p];
		vals[p] = vals[q];
		vals[q] = v;
	} break;
#ifdef HAVE_HGE
	case 16: {
		hge *vals = sh->vals;
		hge v = vals[p];
		vals[p] = vals[q];
		vals[q] = v;
	} break;
#endif
	}
	if (sh->sub)
		subheap_swap(sh->sub, p, q);
}

static void
subheap_del( subheap *sh, size_t pos)
{
	subheap_swap(sh, 0, pos);
	if (sh->sub)
		subheap_del(sh->sub, pos);
}

static void
subheap_ins( subheap *sh, size_t pos, size_t dst)
{
	if (sh->var) {
			void *val = BUNtvar(sh->bi, pos);
			void **vals = sh->vals;
			vals[dst] = val;
	} else {
		switch(sh->width) {
		case 1: {
			int8_t *ivals = sh->ivals;
			int8_t *vals = sh->vals;
			vals[dst] = ivals[pos];
		} break;
		case 2: {
			int16_t *ivals = sh->ivals;
			int16_t *vals = sh->vals;
			vals[dst] = ivals[pos];
		} break;
		case 4: {
			int32_t *ivals = sh->ivals;
			int32_t *vals = sh->vals;
			vals[dst] = ivals[pos];
		} break;
		case 8: {
			int64_t *ivals = sh->ivals;
			int64_t *vals = sh->vals;
			vals[dst] = ivals[pos];
		} break;
	#ifdef HAVE_HGE
		case 16: {
			hge *ivals = sh->ivals;
			hge *vals = sh->vals;
			vals[dst] = ivals[pos];
		} break;
	#endif
		}
	}
	if (sh->sub)
		subheap_ins(sh->sub, pos, dst);
}

static int
heap_down_any( heapn *hp, int p)
{
	subheap *sh = hp->sub;
	void **vals = sh->vals;
	int l = p*2+1, r = p*2+2, q = p;

	if (l < (int)hp->used) {
		int cmp = sh->cmp(vals[q], vals[l], sh);

		if (!cmp && sh->sub)
			q = subheap_down(sh->sub, q, l);
		else if (sh->min && cmp > 0)
			q = l;
		else if (!sh->min && cmp < 0)
			q = l;
	}
	if (r < (int)hp->used) {
		int cmp = sh->cmp(vals[q], vals[r], sh);

		if (!cmp && sh->sub)
			q = subheap_down(sh->sub, q, r);
		else if (sh->min && cmp > 0)
			q = r;
		else if (!sh->min && cmp < 0)
			q = r;
	}
	if (p != q) {
		void *v = vals[p];

		vals[p] = vals[q];
		vals[q] = v;
		if (sh->sub)
			subheap_swap(sh->sub, p, q);
		return heap_down_any(hp, q);
	}
	return p+1;
}

static int
heap_up_any( heapn *hp, size_t p)
{
	subheap *sh = hp->sub;
	if (p == 0)
		return p+1;
	void **vals = sh->vals;
	size_t q = (p-1)/2;
	int cmp = sh->cmp(vals[q], vals[p], sh);

	if (!cmp && sh->sub)
		q = subheap_up(sh->sub, q, p);
	else if (sh->min && cmp < 0)
		q = p;
	else if (!sh->min && cmp > 0)
		q = p;

	if (p != q) {
		void *v = vals[p];

		vals[p] = vals[q];
		vals[q] = v;
		if (sh->sub)
			subheap_swap(sh->sub, p, q);
		if (q == 0)
			return q+1;
		return heap_up_any(hp, q);
	}
	p++;
	return p;
}

static gid
heap_del_any( heapn *hp)
{
	subheap *sh = hp->sub;
	void **vals = sh->vals;

	hp->used--;
	vals[0] = vals[hp->used];
	if (sh->sub)
		subheap_del(sh->sub, hp->used);
	return heap_down_any( hp, 0);
}

static gid
heap_ins_any( heapn *hp, size_t pos)
{
	subheap *sh = hp->sub;
	void *val = BUNtvar(sh->bi, pos);
	void **vals = sh->vals;
	size_t p = hp->used;

	assert(p < hp->size);
	/* TODO !! later do a ma_copy */
	vals[p] = val;
	if (sh->sub)
		subheap_ins( sh->sub, pos, p);
	hp->used++;
	return heap_up_any(hp, p);
}

#define any_min_op(cmp) cmp < 0
#define any_max_op(cmp) cmp > 0

#define heap_type(T) 						\
								\
static int							\
heap_down_##T( heapn *hp, size_t p)				\
{								\
	subheap *sh = hp->sub;					\
	T *vals = sh->vals;					\
	size_t l = p*2+1, r = p*2+2, q = p;			\
								\
	if (l < hp->used) {					\
		T cmp = (vals[q] - vals[l]); 			\
								\
		if (!cmp && sh->sub)				\
			q = subheap_down(sh->sub, q, l);	\
		else if (sh->min && cmp > 0) 			\
			q = l;					\
		else if (!sh->min && cmp < 0) 			\
			q = l;					\
	}							\
	if (r < hp->used) {					\
		T cmp = (vals[q] - vals[r]); 			\
								\
		if (!cmp && sh->sub)				\
			q = subheap_down(sh->sub, q, r);	\
		else if (sh->min && cmp > 0) 			\
			q = r;					\
		else if (!sh->min && cmp < 0) 			\
			q = r;					\
	}							\
	if (p != q) {						\
		T v = vals[p]; 					\
								\
		vals[p] = vals[q];				\
		vals[q] = v;					\
		if (sh->sub)					\
			subheap_swap(sh->sub, p, q);		\
		return heap_down_##T(hp, q);			\
	}							\
	return p+1;						\
}								\
								\
static int							\
heap_up_##T( heapn *hp, size_t p)				\
{								\
	subheap *sh = hp->sub;					\
	if (p == 0)						\
		return p+1;					\
	T *vals = sh->vals;					\
	size_t q = (p-1)/2;					\
	T cmp = (vals[q] - vals[p]); 				\
								\
	if (!cmp && sh->sub)					\
		q = subheap_up(sh->sub, q, p);			\
	if (sh->min && cmp < 0) 				\
		q = p;						\
	else if (!sh->min && cmp > 0) 				\
		q = p;						\
								\
	if (p != q) {						\
		T v = vals[p];					\
								\
		vals[p] = vals[q];				\
		vals[q] = v;					\
		if (sh->sub)					\
			subheap_swap(sh->sub, p, q);		\
		if (q == 0)					\
			return q+1;				\
		return heap_up_##T(hp, q);			\
	}							\
	p++;							\
	return p;						\
}								\
								\
static gid							\
heap_del_##T( heapn *hp) 					\
{								\
	subheap *sh = hp->sub;					\
	T *vals = sh->vals;					\
								\
	hp->used--;						\
	vals[0] = vals[hp->used];				\
	if (sh->sub)						\
		subheap_del(sh->sub, hp->used);			\
	return heap_down_##T( hp, 0);				\
}								\
								\
static gid 							\
heap_ins_##T( heapn *hp, size_t pos)				\
{								\
	subheap *sh = hp->sub;					\
	T *ivals = sh->ivals;					\
	T val = ivals[pos];					\
	T *vals = sh->vals;					\
	size_t p = hp->used;					\
								\
	assert(p < hp->size);					\
	vals[p] = val;						\
	if (sh->sub)						\
		subheap_ins( sh->sub, pos, p);			\
	hp->used++;						\
	return heap_up_##T(hp, p);				\
}						\
						\
static int					\
topn_##T( size_t n, oid *sel, oid *del, oid *ins, heapn *hp)			\
{								\
	size_t i = 0, j = 0;					\
								\
	subheap *sh = hp->sub;					\
	T *hpvals = sh->vals;					\
								\
	T *vals = sh->ivals;					\
	if (hp->used < hp->size) {						\
		for(i=0; i<n && hp->used < hp->size ; i++) {			\
			sel[i] = i;						\
			ins[i] = heap_ins_##T(hp, i); 				\
			del[i] = hp->size+1;				\
		}					\
		j = i;					\
	}						\
	if (sh->min) {					\
		for(; i<n; i++) {						\
			if (hpvals[0] < vals[i] || (sh->sub && hpvals[0] == vals[i] && subheap_newroot(sh->sub, i))) {	\
				sel[j] = i;					\
				del[j] = heap_del_##T(hp);			\
				ins[j] = heap_ins_##T(hp, i);			\
				j++;						\
			}				\
		}					\
	} else {					\
		for(; i<n; i++) {						\
			if (hpvals[0] > vals[i] || (sh->sub && hpvals[0] == vals[i] && subheap_newroot(sh->sub, i))) {	\
				sel[j] = i;					\
				del[j] = heap_del_##T(hp);			\
				ins[j] = heap_ins_##T(hp, i);			\
				j++;						\
			}				\
		}					\
	}						\
	return j;					\
}							\

/* create functions */
heap_type(bte)
heap_type(sht)
heap_type(int)
heap_type(lng)
heap_type(hge)
heap_type(flt)
heap_type(dbl)

/* The 2 have 2 compare functions one for nil as largest value (default) and one for nil as smallest value */

#define heap_type_cmp(T)						\
static int										\
T##_cmp##_nsmall( T *v1, T *v2, void *sh)		\
{												\
	(void)sh;									\
	return (is_##T##_nil(*v1)?(!is_##T##_nil(*v2)?-1:0):(is_##T##_nil(*v2)?1:(*v1<*v2?-1:((*v1==*v2)?0:1))));	\
}												\
												\
static int										\
T##_cmp( T *v1, T *v2, void *sh )				\
{												\
	(void)sh;									\
	return (is_##T##_nil(*v1)?(!is_##T##_nil(*v2)?1:0):(is_##T##_nil(*v2)?-1:(*v1<*v2?-1:((*v1==*v2)?0:1))));	\
}

heap_type_cmp(bte)
heap_type_cmp(sht)
heap_type_cmp(int)
heap_type_cmp(lng)
heap_type_cmp(hge)
heap_type_cmp(flt)
heap_type_cmp(dbl)

#define HEAP_SINK 3

static void
subheap_destroy( subheap *h)
{
	if (h->sub)
		subheap_destroy(h->sub);
	if (h->vals)
		GDKfree(h->vals);
	GDKfree(h);
}

static void
heap_destroy( heapn *h)
{
	if (!h)
		return;
	if (h->sub)
		subheap_destroy(h->sub);
	if (h->ma)
		ma_destroy(h->ma);
	GDKfree(h);
}

static BAT *
HEAPnew_topn( MalStkPtr s, InstrPtr p, heapn *hp, lng n, BAT *b, bit min, bit nulls_last)
{
	subheap_create(hp, b->ttype, min, nulls_last);
	for(int cur = 9; cur < p->argc; cur+=3) {
		bat *in = getArgReference_bat(s, p, cur);
		min = *getArgReference_bit(s, p, cur+1);
		nulls_last = *getArgReference_bit(s, p, cur+2);
		BAT *b = BATdescriptor(*in);
		if (!b) {
			heap_destroy(hp);
			return NULL;
		}
		subheap_create(hp, b->ttype, min, nulls_last);
		BBPunfix(b->batCacheid);
	}
	heapn_done(hp);
	b = COLnew(0, TYPE_oid, n, TRANSIENT);
	if (b)
		b->T.sink = (Sink*)hp;
	else
		heap_destroy(hp);
	return b;
}

/* convert into using BATsss */
static str
/* PATTERN HEAPtopn( bat *sel, bat *del, bat *ins, bat *HP, lng *sz, bat *in, bit *min, bit *nulls_last, ...) */
HEAPtopn(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt; (void)m;

	bat *sel = getArgReference_bat(s, p, 0);
	bat *del = getArgReference_bat(s, p, 1);
	bat *ins = getArgReference_bat(s, p, 2);
	bat *HP = getArgReference_bat(s, p, 3);
	Pipeline *pp = *(Pipeline**)getArgReference_ptr(s, p, 5);
	bat *in = getArgReference_bat(s, p, 6);
	bit min = *getArgReference_bit(s, p, 7);
	bit nulls_last = *getArgReference_bit(s, p, 8);
	bool private = (!*HP || is_bat_nil(*HP));
	BAT *hps, *b = BATdescriptor(*in);

	if (!b)
		return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (private) {
		lng n = *getArgReference_lng(s, p, 4);
		heapn *hp = heapn_create(n, 0);
		hps = HEAPnew_topn(s, p, hp, n, b, min, nulls_last);
		if (!hps) {
			BBPunfix(b->batCacheid);
			return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
       	hps->T.private_bat = 1;
		*HP = hps->batCacheid;
	} else {
		hps = BATdescriptor(*HP);
	}
	private = hps->T.private_bat;
	heapn *hp = (heapn*)hps->T.sink;
	assert(hp && hp->s.type == HEAP_SINK);
	HEAP_INIT(hp);
	subheap *sh = hp->sub;

	if (!private) {
		while(hps->unused != pp->wid) MT_sleep_ms(10);
	}

	if (!private)
		pipeline_lock1(hps);
	/* lock ? */

	void **hpvals = sh->vals;
	sh->in = b;
	if (!sh->var)
		sh->ivals = Tloc(sh->in, 0);
	else
		sh->bi = bat_iterator(sh->in);
	size_t cnt = BATcount(sh->in), i = 0, j = 0;

	BAT *S = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	BAT *D = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	BAT *I = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	oid *sp = Tloc(S, 0);
	oid *dp = Tloc(D, 0);
	oid *ip = Tloc(I, 0);
	subheap *nsh = NULL;
	int cur = 9;
	for(nsh = sh->sub; cur < p->argc; nsh = nsh->sub, cur += 3) {
		bat *in = getArgReference_bat(s, p, cur);
		nsh->in = BATdescriptor(*in);
		if (!nsh->in) {
			if (!private)
				hps->unused++;
			return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (!nsh->var)
			nsh->ivals = Tloc(nsh->in, 0);
		else
			nsh->bi = bat_iterator(nsh->in);
		assert(cnt == BATcount(nsh->in) && nsh->min == *getArgReference_bit(s, p, cur+1));
	}
	if (ATOMstorage(b->ttype) == TYPE_bte) {
		j = topn_bte(cnt, sp, dp, ip, hp);
	} else if (ATOMstorage(b->ttype) == TYPE_sht) {
		j = topn_sht(cnt, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_int) {
		j = topn_int(cnt, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_lng) {
		j = topn_lng(cnt, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_hge) {
		j = topn_hge(cnt, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_flt) {
		j = topn_flt(cnt, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_dbl) {
		j = topn_dbl(cnt, sp, dp, ip, hp);
	/* TODO add date, daytime and timestamp */
	} else {
		if (hp->used < hp->size) {
			for(i=0; i<cnt && hp->used < hp->size ; i++) {
				sp[i] = i;
				ip[i] = heap_ins_any(hp, i);
				dp[i] = hp->size+1;
			}
			j = i;
		}
		if (sh->min) {
			for(; i<cnt; i++) {
				int cmp = sh->cmp(hpvals[0], BUNtvar(sh->bi, i), sh);
				if (cmp < 0 || (sh->sub && !cmp && subheap_newroot(sh->sub, i))) {
					sp[j] = i;
					dp[j] = heap_del_any(hp);
					ip[j] = heap_ins_any(hp, i);
					j++;
				}
			}
   		} else {
			for(; i<cnt; i++) {
				int cmp = sh->cmp(hpvals[0], BUNtvar(sh->bi, i), sh);
				if (cmp > 0 || (sh->sub && !cmp && subheap_newroot(sh->sub, i))) {
					sp[j] = i;
					dp[j] = heap_del_any(hp);
					ip[j] = heap_ins_any(hp, i);
					j++;
				}
			}
		}
	}
	if (!private)
		pipeline_lock2(hps);
	for(nsh = sh; nsh; nsh = nsh->sub) {
		BBPunfix(nsh->in->batCacheid);
		if (nsh->var)
			bat_iterator_end(&nsh->bi);
	}
	BATsetcount(S, j);
	BATsetcount(D, j);
	BATsetcount(I, j);
	BATnegateprops(S);
	BATnegateprops(D);
	BATnegateprops(I);
	*sel = S->batCacheid;
	*del = D->batCacheid;
	*ins = I->batCacheid;
	BBPkeepref(S);
	BBPkeepref(D);
	BBPkeepref(I);
	if (!private)
		pipeline_unlock2(hps);
	if (!private)
		pipeline_unlock1(hps);
	BBPkeepref(hps);
	if (!private)
		hps->unused++;
	return MAL_SUCCEED;
}

static heapn *
_heap_create( int size, bool shared )
{
	heapn *h = (heapn*)GDKzalloc(sizeof(heapn));

	h->s.destroy = (sink_destroy)heap_destroy;
	h->s.type = HEAP_SINK;
	h->shared = shared;

	h->size = size;
	h->used = 0;
	return h;
}

static int
strCmp_nsmall(char *v1, char *v2, void *sh)
{
	(void)sh;
	if (strNil(v1)) {
		if (!strNil(v2))
			return -1;
		else
			return 0;
	} else if (strNil(v2)) {
		return 1;
	}
	return strCmp(v1, v2);
}

static int
strCmp_nlarge(char *v1, char *v2, void *sh)
{
	(void)sh;
	return strCmp(v1, v2);
}

static int
any_cmp_nsmall( void *v1, void *v2, void *SH)
{
	subheap *sh = SH;
	if (sh->cmp2(v1, sh->nilptr()) == 0) {
		if (sh->cmp2(v2, sh->nilptr()) != 0)
			return -1;
		else
			return 0;
	} else if (sh->cmp2(v2, sh->nilptr()) == 0) {
		return 1;
	}
	return sh->cmp2(v1, v2);
}

static int
any_cmp_nlarge( void *v1, void *v2, void *SH)
{
	subheap *sh = SH;
	return sh->cmp2(v1, v2);
}

static subheap *
subheap_create( heapn *hp, int type, bool min /* or max */, bool nulls_last)
{
	subheap *sh = (subheap*)GDKzalloc(sizeof(subheap));
	sh->type = type;
	sh->width = ATOMsize(type);
	sh->var = ATOMvarsized(type);
	if (type == TYPE_str) {
		if ((nulls_last && min) || (!nulls_last && !min)) {
			sh->cmp = (fcmp)strCmp_nsmall;
		} else {
			sh->cmp = (fcmp)strCmp_nlarge;
		}
	}
	if (!sh->cmp) {
		if ((nulls_last && min) || (!nulls_last && !min)) {
			switch(sh->width) {
			case 1:
				sh->cmp = (fcmp)&bte_cmp_nsmall;
				break;
			case 2:
				sh->cmp = (fcmp)&sht_cmp_nsmall;
				break;
			case 4:
				sh->cmp = (fcmp)&int_cmp_nsmall;
				if (type == TYPE_flt)
					sh->cmp = (fcmp)&flt_cmp_nsmall;
				break;
			case 8:
				sh->cmp = (fcmp)&lng_cmp_nsmall;
				if (type == TYPE_dbl)
					sh->cmp = (fcmp)&dbl_cmp_nsmall;
				break;
	#ifdef HAVE_HGE
			case 16:
				sh->cmp = (fcmp)&hge_cmp_nsmall;
				break;
	#endif
			}
		} else {
			switch(sh->width) {
			case 1:
				sh->cmp = (fcmp)&bte_cmp;
				break;
			case 2:
				sh->cmp = (fcmp)&sht_cmp;
				break;
			case 4:
				sh->cmp = (fcmp)&int_cmp;
				if (type == TYPE_flt)
					sh->cmp = (fcmp)&flt_cmp;
				break;
			case 8:
				sh->cmp = (fcmp)&lng_cmp;
				if (type == TYPE_dbl)
					sh->cmp = (fcmp)&dbl_cmp;
				break;
	#ifdef HAVE_HGE
			case 16:
				sh->cmp = (fcmp)&hge_cmp;
				break;
	#endif
			}
		}
	}
	if (!sh->cmp) {
		sh->nilptr = (fnil)ATOMnilptr(type);
		sh->cmp2 = (fcmp2)ATOMcompare(type);
		if ((nulls_last && min) || (!nulls_last && !min))
			sh->cmp = (fcmp)any_cmp_nsmall;
		else
			sh->cmp = (fcmp)any_cmp_nlarge;
	}
	sh->min = min;

	if (!hp->sub)
		hp->sub = sh;
	else {
		subheap *s = hp->sub;
		for(; s->sub; s = s->sub)  ;
		s->sub = sh;
	}
	return sh;
}

heapn *
heapn_create( int size, bool shared )
{
	return _heap_create( size, shared);
}

heapn *
heapn_subheap( heapn *hp, int type, bool min /* or max */, bool nulls_last)
{
	(void)subheap_create(hp, type, min, nulls_last);
	return hp;
}

heapn *
heapn_done( heapn *hp )
{
	/* should only be called once all subheaps are added */
	if (hp->shared)
		_heap_init(hp);
	return hp;
}

static BAT *
HEAPnew_new( MalBlkPtr m, MalStkPtr s, InstrPtr p, heapn *hp, lng n, int tt,  bit min, bit nulls_last)
{
	subheap_create(hp, tt, min, nulls_last);
	for(int cur = 5; cur < p->argc; cur+=3) {
		tt = getArgType(m, p, cur);
		min = *getArgReference_bit(s, p, cur+1);
		nulls_last = *getArgReference_bit(s, p, cur+2);
		subheap_create(hp, tt, min, nulls_last);
	}
	heapn_done(hp);
	BAT *b = COLnew(0, TYPE_oid, n, TRANSIENT);
	if (b)
		b->T.sink = (Sink*)hp;
	else
		heap_destroy(hp);
	return b;
}

static str
/* PATTERN HEAPnew( bat *HP, lng *N, ptr *type, bit *min, ...) */
HEAPnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;

	bat *HP = getArgReference_bat(s, p, 0);
	lng n = *getArgReference_lng(s, p, 1);
	int tt = getArgType(m, p, 2);
	bit min = *getArgReference_bit(s, p, 3);
	bit nulls_last = *getArgReference_bit(s, p, 4);

	heapn *hp = heapn_create( (int)n, 1);
	BAT *b = NULL;

	if (!hp) {
		heap_destroy(hp);
		return createException(SQL, "heapn.new",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	b = HEAPnew_new(m, s, p, hp, n, tt, min, nulls_last);
	if (!b)
		return createException(SQL, "heapn.new",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	*HP = b->batCacheid;
	BBPkeepref(b);
	return MAL_SUCCEED;
}

extern int BATupgrade(BAT *r, BAT *b, bool locked);
extern void BATswap_heaps(BAT *u, BAT *b, Pipeline *p);

#define project(T) \
	if (ATOMstorage(tt) == TYPE_##T) { \
		T *ri = Tloc(r,0);		\
		T *bi = Tloc(b,0);		\
							\
		for(; i<cnt; i++) {		\
			if (ii[i] != 0) {	\
				gid d = di[i];	\
				gid j = ii[i], p = size-1;		\
												\
				assert(j>=0);					\
				if (j<0)						\
					j=-j;						\
				assert(d>=0);					\
				if ((BUN)d > size) {					\
					p = hpcnt++;					\
				/* repeat the heap_del, del first, reinsert last */		\
				} else if (d > 0) {											\
					/* move up del -> 0 */								\
					T oval = ri[size-1];								\
																		\
					/* move up the heap all values up until pos i */	\
					d--;												\
					while (d) {											\
						T v = ri[d];									\
																		\
						ri[d] = oval;									\
						oval = v;										\
						d = (d-1)/2;									\
					}													\
					ri[0] = oval;										\
				}														\
				/* move up the heap all values from size-1 till j */	\
				j--;													\
				while (p>j) {											\
					int q = (p-1)/2;									\
					ri[p] = ri[q];										\
					p = q;												\
					}													\
				assert(p == j);											\
				ri[j] = bi[si[i]];										\
			}															\
		}																\
	}

#define aproject(T,w,Toff) \
	if (ATOMstorage(tt) == TYPE_##T && b->twidth == w) { \
		Toff *bi = Tloc(b, 0); \
		Toff *ri = Tloc(r, 0); \
							\
		for(; i<cnt; i++) {		\
			if (ii[i] != 0) {	\
				gid d = di[i];	\
				gid j = ii[i], p = size-1;		\
												\
				assert(j>=0);					\
				if (j<0)						\
					j=-j;						\
				assert(d>=0);					\
				if ((BUN)d > size) {				\
					p = hpcnt++;					\
				/* repeat the heap_del, del first, reinsert last */		\
				} else if (d > 0) {										\
					/* move up del -> 0 */								\
					Toff oval = ri[size-1];								\
																		\
					/* move up the heap all values up until pos i */	\
					d--;												\
					while (d) {											\
						Toff v = ri[d];									\
																		\
						ri[d] = oval;									\
						oval = v;										\
						d = (d-1)/2;									\
					}													\
					ri[0] = oval;										\
				}														\
				/* move up the heap all values from size-1 till j */	\
				j--;													\
				while (p>j) {											\
					int q = (p-1)/2;									\
					ri[p] = ri[q];										\
					p = q;												\
				}														\
				assert(p == j);											\
				ri[j] = bi[si[i]];										\
			}															\
		}																\
	}

#define project_del(Toff) \
		Toff *ri = Tloc(r, 0); \
		/* move up del -> 0 */								\
		Toff oval = ri[size-1];								\
																		\
		/* move up the heap all values up until pos i */	\
		d--;												\
		while (d) {											\
			Toff v = ri[d];									\
															\
			ri[d] = oval;									\
			oval = v;										\
			d = (d-1)/2;									\
		}													\
		ri[0] = oval;

#define project_ins(Toff) \
		Toff *ri = Tloc(r, 0); \
		while (p>j) {											\
			int q = (p-1)/2;									\
			ri[p] = ri[q];										\
			p = q;												\
		}														\

#define aproject_(T) \
	if (ATOMstorage(tt) == TYPE_##T) { \
		BATiter bi = bat_iterator(b); \
		int w = r->twidth; \
		for(; i<cnt; i++) {		\
			if (ii[i] != 0) {	\
				gid d = di[i];	\
				gid j = ii[i], p = size-1;		\
												\
				assert(j>=0);					\
				if (j<0)						\
					j=-j;						\
				assert(d>=0);					\
				if ((BUN)d > size) {					\
					p = hpcnt++;					\
				/* repeat the heap_del, del first, reinsert last */		\
				} else if (d > 0) {										\
					if (w == 1) {										\
						project_del(uint8_t);							\
					} else if (w == 2) {								\
						project_del(uint16_t);							\
					} else if (w == 4) {								\
						project_del(uint32_t);							\
					} else {											\
						project_del(var_t);								\
					}													\
				}														\
				/* move up the heap all values from size-1 till j */	\
				j--;													\
				if (w == 1) {											\
					project_ins(uint8_t);								\
				}else if (w == 2) {										\
					project_ins(uint16_t);								\
				}else if (w == 4) {										\
					project_ins(uint32_t);								\
				} else {												\
					project_ins(var_t);									\
				}														\
				assert(p == j);											\
				BUN cnd = si[i];											\
				if (tfastins_nocheckVAR(r, j, BUNtvar(bi, cnd)) != GDK_SUCCEED) \
					err = 1; \
			}															\
		}																\
		bat_iterator_end(&bi); \
	}

static str
HEAPproject(bat *rid, bat *cand, bat *del, bat *ins, bat *in, lng *n, const ptr *H)
{
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *r = NULL, *s = BATdescriptor(*cand), *d = BATdescriptor(*del), *i = BATdescriptor(*ins), *b = BATdescriptor(*in);
	int err = 0;

	if (!s || !d || !i || !b) {
		if (s)
			BBPunfix(s->batCacheid);
		if (d)
			BBPunfix(d->batCacheid);
		if (i)
			BBPunfix(i->batCacheid);
		assert(!b);
		throw(MAL, "heapn.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (*rid && !is_bat_nil(*rid)) {
		r = BATdescriptor(*rid);
		err = (!r);
	}

	int tt = b->ttype;
	bool private = (!r || r->T.private_bat), local_storage = false;

	if (!private) {
		while(r->unused != p->wid) MT_sleep_ms(10);
	}

	if (!err && r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = 1;
		} else if (ATOMvarsized(r->ttype) && ((BATcount(r) && r->tvheap->parentid == r->batCacheid) ||
				(!VIEWvtparent(b) || BBP_cache(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			assert(r->tvheap->parentid == r->batCacheid);
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
		}
	} else if (!err && !r) {
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(tt) && VIEWvtparent(b) && BBP_cache(VIEWvtparent(b))->batRestricted == BAT_READ) {
			uint16_t width = b->twidth;
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, tt, *n, TRANSIENT, width);
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			local_storage = true;
			r = COLnew2(0, tt, *n, TRANSIENT, b->twidth);
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED)
				err = 1;
		}
		assert(private);
		r->T.private_bat = 1;
	}
	if (!private)
		pipeline_lock1(r);

	if (!err) {
		oid *si = Tloc(s, 0);
		oid *di = Tloc(d, 0);
		oid *ii = Tloc(i, 0);
		size_t i = 0;
		BUN size = *n;
		BUN cnt = BATcount(s), hpcnt = BATcount(r);

		project(bte)
		project(sht)
		project(int)
		project(lng)
		project(hge)
		project(flt)
		project(dbl)
		if (local_storage) {
			aproject_(str)
		} else {
			aproject(str,1,uint8_t)
			aproject(str,2,uint16_t)
			aproject(str,4,uint32_t)
			aproject(str,8,var_t)
		}
		if (!err) {
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < hpcnt) {
				BATsetcount(r, hpcnt);
				assert(hpcnt <= size);
			}
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
		}
	}

	if (!private)
		pipeline_unlock1(r);
	BBPunfix(s->batCacheid);
	BBPunfix(d->batCacheid);
	BBPunfix(i->batCacheid);
	BBPunfix(b->batCacheid);
	if (err)
		throw(MAL, "heapn.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	*rid = r->batCacheid;
	BBPkeepref(r);
	if (!private)
		r->unused++;
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func heapn_init_funcs[] = {
    pattern("heapn", "new", HEAPnew, false, "return new heap sink", args(1,3,
				batarg("sink",oid),
				arg("n",lng),
				vararg("in",any)/*,
				vararg("min", bit),
				vararg("nulls_last", bit)
				*/
				)
		   ),
	pattern("heapn", "topn", HEAPtopn, false, "Return a candidate list with the trail of changes of the heap encoded in the deleted/inserted rows", args(4,6,
				batarg("sel",oid),
				batarg("del",oid),
				batarg("ins",oid),
				sharedbatarg("heap",oid),
				arg("N", lng),
				vararg("in",any)/*,
				vararg("min", bit),
				vararg("nulls_last", bit)
				*/
				)
			),
	command("heapn", "projection", HEAPproject, false, "Project.", args(1,7,
				batargany("",1),
				batarg("sel", oid),
				batarg("del", oid),
				batarg("ins", oid),
				batargany("b",1),
				arg("n", lng),
				arg("pipeline", ptr)
				)
			),
	{ .imp=NULL }
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_heapn_mal)
{ mal_module("heapn", NULL, heapn_init_funcs); }
