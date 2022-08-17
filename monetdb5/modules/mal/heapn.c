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
typedef int (*fcmp)(void *v1, void *v2);
typedef void *mallocator;
extern void ma_destroy(mallocator* ma);
extern mallocator * ma_create(void);
extern void * ma_alloc( mallocator *ma, size_t sz );
typedef lng gid;
typedef lng sel_t;

static int
str_cmp(str s1, str s2)
{
    return strcmp(s1,s2);
}

typedef struct subheap {
	fcmp cmp;		/* cmp function for complex types */
	//lenfptr length;		/* length function for complex types */

	void *vals;		/* array values */
	void *ivals;		/* input pointer */
	BAT *in;
	struct subheap *sub;	/* for multi attribute topn, next value array and function ptrs */
	int type;
	int width;
	bool min;	/* or max heap */
	bool var;
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
extern heapn *heapn_subheap( heapn *hp, int type, char min /* or max */);
extern heapn *heapn_done( heapn *hp ); /* done creating subheaps */

static heapn *_heap_create( int size, bool shared );
static subheap *subheap_create( heapn *hp, int type, char min /* or max */);

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
 		cmp = sh->cmp(nvals[q], nvals[l]);
	} else
		cmp = sh->cmp(vals+(q*sh->width), vals+(l*sh->width));

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
 		cmp = sh->cmp(nvals[q], nvals[p]);
	} else
		cmp = sh->cmp(vals+(q*sh->width), vals+(p*sh->width));

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
		void **nvals = (void**)vals;
		void **nivals = (void**)ivals;
 		cmp = sh->cmp(nvals[0], nivals[p]);
	} else
 		cmp = sh->cmp(vals, ivals+(p*sh->width));
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
		int cmp = sh->cmp(vals[q], vals[l]);

		if (!cmp && sh->sub)
			q = subheap_down(sh->sub, q, l);
		else if (sh->min && cmp > 0)
			q = l;
		else if (!sh->min && cmp < 0)
			q = l;
	}
	if (r < (int)hp->used) {
		int cmp = sh->cmp(vals[q], vals[r]);

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
		return p;
	void **vals = sh->vals;
	size_t q = (p-1)/2;
	int cmp = sh->cmp(vals[q], vals[p]);

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
	void **ivals = sh->ivals;
	void *val = ivals[pos];
	void **vals = sh->vals;
	size_t p = hp->used;

	assert(p < hp->size);
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
			del[i] = -hp->used;					\
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

#define heap_type_cmp(T)				\
static int						\
T##_cmp( T *v1, T *v2 )					\
{							\
	return *v1-*v2;					\
}

heap_type_cmp(bte)
heap_type_cmp(sht)
heap_type_cmp(int)
heap_type_cmp(lng)
heap_type_cmp(hge)

#define HEAP_SINK 3

/* convert into using BATsss */
static str
//topn_any( size_t *SZ, sel_t *sel, gid *del, gid *ins, heapn *hp, vector *in, ...)
topn_any( bat *sel, bat *del, bat *ins, bat *HP, lng *sz, bat *in, ...)
{
	va_list va;
	bool private = (!*HP || is_bat_nil(*HP));
	BAT *hps, *b = BATdescriptor(*in);

	if (!b)
		return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (private) {
		hps = COLnew(b->hseqbase, b->ttype?b->ttype:TYPE_oid, 0, TRANSIENT);
        	hps->T.sink = (Sink*)heapn_create(*sz, 0);
        	hps->T.private_bat = 1;
	} else {
		hps = BATdescriptor(*HP);
	}
	private = hps->T.private_bat;
	heapn *hp = (heapn*)hps->T.sink;
	assert(hp && hp->s.type == HEAP_SINK);
	HEAP_INIT(hp);
	subheap *sh = hp->sub;

	if (!private)
		pipeline_lock1(hps);
	/* lock ? */

	void **hpvals = sh->vals;
	va_start(va, in);
	sh->in = b;
	if (!sh->var)
		sh->ivals = Tloc(sh->in, 0);
	void **vals = sh->ivals;
	size_t n = BATcount(sh->in), i = 0, j = 0;

	BAT *S = COLnew(0, TYPE_oid, n, TRANSIENT);
	BAT *D = COLnew(0, TYPE_oid, n, TRANSIENT);
	BAT *I = COLnew(0, TYPE_oid, n, TRANSIENT);
	oid *sp = Tloc(S, 0);
	oid *dp = Tloc(S, 0);
	oid *ip = Tloc(S, 0);
	subheap *nsh = NULL;
	for(nsh = sh->sub; (in = va_arg(va,bat*)) != NULL && nsh; nsh = nsh->sub) {
		nsh->in = BATdescriptor(*in);
		if (!nsh->in)
			return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		if (!nsh->var)
			nsh->ivals = Tloc(nsh->in, 0);
		assert(n == BATcount(nsh->in));
	}
	assert(nsh == NULL && in == NULL);
	va_end(va);
	if (b->ttype == TYPE_bte) {
		j = topn_bte(n, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_sht) {
		j = topn_sht(n, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_int) {
		j = topn_int(n, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_lng) {
		j = topn_lng(n, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_hge) {
		j = topn_hge(n, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_flt) {
		j = topn_flt(n, sp, dp, ip, hp);
	} else if (b->ttype == TYPE_dbl) {
		j = topn_dbl(n, sp, dp, ip, hp);
	} else {
		if (hp->used < hp->size) {
			for(i=0; i<n && hp->used < hp->size ; i++) {
				sp[i] = i;
				ip[i] = heap_ins_any(hp, i);
				dp[i] = -hp->used;
			}
			j = i;
		}
		if (sh->min) {
			for(; i<n; i++) {
				/* todo do proper var bat lookups */
				int cmp = sh->cmp(hpvals[0], vals[i]);
				if (cmp < 0 || (sh->sub && !cmp && subheap_newroot(sh->sub, i))) {
					sp[j] = i;
					dp[j] = heap_del_any(hp);
					ip[j] = heap_ins_any(hp, i);
					j++;
				}
			}
   		} else {
			for(; i<n; i++) {
				int cmp = sh->cmp(hpvals[0], vals[i]);
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
	for(nsh = sh; nsh; nsh = nsh->sub)
		BBPunfix(nsh->in->batCacheid);
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
	return MAL_SUCCEED;
}

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
	if (h->sub)
		subheap_destroy(h->sub);
	if (h->ma)
		ma_destroy(h->ma);
	GDKfree(h);
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

static subheap *
subheap_create( heapn *hp, int type, char min /* or max */)
{
	subheap *sh = (subheap*)GDKzalloc(sizeof(subheap));
	sh->type = type;
	sh->width = ATOMsize(type);
	if (type == TYPE_str) {
		sh->cmp = (fcmp)str_cmp;
	}
	if (!sh->cmp) {
		switch(sh->width) {
		case 1:
			sh->cmp = (fcmp)&bte_cmp;
			break;
		case 2:
			sh->cmp = (fcmp)&sht_cmp;
			break;
		case 4:
			sh->cmp = (fcmp)&int_cmp;
			break;
		case 8:
			sh->cmp = (fcmp)&lng_cmp;
			break;
#ifdef HAVE_HGE
		case 16:
			sh->cmp = (fcmp)&hge_cmp;
			break;
#endif
		}
	}
	//sh->length = l;
	sh->var = (type == TYPE_str); // TODO: handle more var types
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
heapn_subheap( heapn *hp, int type, char min /* or max */)
{
	(void)subheap_create(hp, type, min);
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

#include "mel.h"
static mel_func heapn_init_funcs[] = {
	command("heapn", "topn", topn_any, false, "Return a candidate list with the trail of changes of the heap encoded in the deleted/inserted rows", args(3,6,
				batarg("sel",oid),
				batarg("del",oid),
				batarg("ins",oid),
				batarg("heap",oid),
				arg("N", lng),
				batvarargany("in",1)
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
