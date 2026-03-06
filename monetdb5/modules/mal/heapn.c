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
#include "gdk.h"
#include "gdk_atoms.h"
#include "gdk_time.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include "mal_pipelines.h"
#include "pipeline.h"
#include "algebra.h"

/*
 * Min/Max processing using heap structure
 * ie array implementation
 */
typedef int (*fcmp)(const void *v1, const void *v2, const void *hp);
typedef int (*fcmp2)(const void *v1, const void *v2);
typedef void *(*fnil)();
typedef lng gid;

typedef struct subheap {
	fcmp cmp;		/* cmp function for complex types */
	fcmp2 cmp2;		/* cmp function for complex types */
	fnil nilptr;		/* is nil */

	BAT *vb;
	BATiter vbi;
	void *vals;		/* array values */
	void *ivals;		/* input pointer */
	BAT *in;
	BATiter bi;
	struct subheap *sub;	/* for multi attribute topn, next value array and function ptrs */
	int type;
	int width;
	bool min;	/* or max heap */
	bool nlarge;
	bool var;
	bool external;
} subheap;

typedef struct heapn {
	Sink s;
	size_t size;
	size_t used;
	bool full;
	ATOMIC_TYPE counter;

	size_t gsize;
	oid *pos;
	BAT *ginb;
	oid *gi;
	BAT *grpb;
	oid *grp;
	BAT *usedb;
	int *useda;
	BAT *fullb;
	bte *fulla;
	bool grouped;
	bool shared;		/* PRIVATE or SHARED */
	struct subheap *sub;	/* for multi attribute topn, next value array and function ptrs */
} heapn;

static heapn *heapn_create( int size, bool shared, bool grouped);
static heapn *heapn_done( heapn *hp ); /* done creating subheaps */

static heapn *_heap_create( int size, bool shared, bool grouped );
static subheap *subheap_create( heapn *hp, int type, bool min /* or max */, bool nulls_last);

static heapn *
_heap_init( heapn *hp )
{
	//printf("heap_init(%c) %ld\n", hp->shared?'S':'L', hp->size);
	if (hp->grouped) {
		hp->gsize = 256;
		hp->grpb = COLnew(0, TYPE_oid, hp->size * hp->gsize, TRANSIENT);
		if (!hp->grpb)
			return NULL;
		hp->grp = Tloc(hp->grpb, 0);

		hp->usedb = COLnew(0, TYPE_int, hp->gsize, TRANSIENT);
		if (!hp->usedb)
			return NULL;
		hp->useda = Tloc(hp->usedb, 0);
		memset(hp->useda, 0, hp->gsize*sizeof(int));

		hp->fullb = COLnew(0, TYPE_bte, hp->gsize, TRANSIENT);
		if (!hp->fullb)
			return NULL;
		hp->fulla = Tloc(hp->fullb, 0);
		memset(hp->fulla, 0, hp->gsize);
	}
	for (subheap *sh = hp->sub; sh && !sh->vb; sh = sh->sub) {
		sh->vb = COLnew(0, sh->type, hp->grouped?hp->gsize*hp->size:hp->size, TRANSIENT);
		if (!sh->vb)
			return NULL;
		if (sh->var)
			sh->vbi = bat_iterator(sh->vb);
		sh->vals = Tloc(sh->vb, 0);
	}
	return hp;
}

static int
subheap_down( heapn *hp, subheap *sh, size_t q, size_t l)
{
	char *vals = sh->vals;
	int cmp = 0;
	if (sh->var) {
 		cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[q]), BUNtvar(&sh->vbi, hp->pos[l]), sh);
	} else
		cmp = sh->cmp(vals+(hp->pos[q]*sh->width), vals+(hp->pos[l]*sh->width), sh);

	if (!cmp && sh->sub)
		q = subheap_down(hp, sh->sub, q, l);
	else if (sh->min && cmp > 0)
		q = l;
	else if (!sh->min && cmp < 0)
		q = l;
	return (int)q;
}

static int
subheap_up( heapn *hp, subheap *sh, size_t q, size_t p)
{
	int cmp = 0;
	char *vals = sh->vals;
	if (sh->var) {
 		cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[q]), BUNtvar(&sh->vbi, hp->pos[p]), sh);
	} else
		cmp = sh->cmp(vals+(hp->pos[q]*sh->width), vals+(hp->pos[p]*sh->width), sh);

	if (!cmp && sh->sub)
		q = subheap_up(hp, sh->sub, q, p);
	else if (sh->min && cmp < 0)
		q = p;
	else if (!sh->min && cmp > 0)
		q = p;
	return (int)q;
}

static int
gsubheap_down( heapn *hp, subheap *sh, oid g, size_t q, size_t l)
{
	oid *pos = hp->pos + g * hp->size;
	int cmp = 0;
	char *vals = sh->vals;
	if (sh->var) {
 		cmp = sh->cmp(BUNtvar(&sh->vbi, pos[q]), BUNtvar(&sh->vbi, pos[l]), sh);
	} else
		cmp = sh->cmp(vals+(pos[q]*sh->width), vals+(pos[l]*sh->width), sh);

	if (!cmp && sh->sub)
		q = gsubheap_down(hp, sh->sub, g, q, l);
	else if (sh->min && cmp > 0)
		q = l;
	else if (!sh->min && cmp < 0)
		q = l;
	return (int)q;
}

static int
gsubheap_up( heapn *hp, subheap *sh, oid g, size_t q, size_t p)
{
	oid *pos = hp->pos + g * hp->size;
	char *vals = sh->vals;
	int cmp = 0;
	if (sh->var) {
 		cmp = sh->cmp(BUNtvar(&sh->vbi, pos[q]), BUNtvar(&sh->vbi, pos[p]), sh);
	} else
		cmp = sh->cmp(vals+(pos[q]*sh->width), vals+(pos[p]*sh->width), sh);

	if (!cmp && sh->sub)
		q = gsubheap_up(hp, sh->sub, g, q, p);
	else if (sh->min && cmp < 0)
		q = p;
	else if (!sh->min && cmp > 0)
		q = p;
	return (int)q;
}


static bool
subheap_newroot( heapn *hp, subheap *sh, size_t p)
{
	char *vals = sh->vals;
	char *ivals = sh->ivals;
	int cmp = 0;
	if (sh->var) {
		const void *val = BUNtvar(&sh->bi, p);
 		cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[0]), val, sh);
	} else
 		cmp = sh->cmp(vals+(hp->pos[0]*sh->width), ivals+(p*sh->width), sh);
	bool newroot = false;

	if (!cmp && sh->sub)
		newroot = subheap_newroot(hp, sh->sub, p);
	else if (sh->min && cmp < 0)
		newroot = true;
	else if (!sh->min && cmp > 0)
		newroot = true;
	return newroot;
}

static int
subheap_ins( subheap *sh, size_t pos, size_t dst)
{
	if (sh->var) {
			const void *val = BUNtvar(&sh->bi, pos);

			if (dst == BATcount(sh->vb)) {
				if (BUNappend(sh->vb, val, true) != GDK_SUCCEED)
					return -1;
			} else if (BUNreplace(sh->vb, dst, val, true) != GDK_SUCCEED)
				return -2;
			bat_iterator_end(&sh->vbi);
			sh->vbi = bat_iterator(sh->vb);
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
		return subheap_ins(sh->sub, pos, dst);
	return 0;
}

static int
heap_down_any( heapn *hp, int p)
{
	subheap *sh = hp->sub;
	int l = p*2+1, r = p*2+2, q = p;

	if (l < (int)hp->used) {
		int cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[q]), BUNtvar(&sh->vbi, hp->pos[l]), sh);

		if (!cmp && sh->sub)
			q = subheap_down(hp, sh->sub, q, l);
		else if (sh->min && cmp > 0)
			q = l;
		else if (!sh->min && cmp < 0)
			q = l;
	}
	if (r < (int)hp->used) {
		int cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[q]), BUNtvar(&sh->vbi, hp->pos[r]), sh);

		if (!cmp && sh->sub)
			q = subheap_down(hp, sh->sub, q, r);
		else if (sh->min && cmp > 0)
			q = r;
		else if (!sh->min && cmp < 0)
			q = r;
	}
	if (p != q) {
		oid vpos = hp->pos[p];

		hp->pos[p] = hp->pos[q];
		hp->pos[q] = vpos;
		return heap_down_any(hp, q);
	}
	return (int)(p+1);
}

static int
heap_up_any( heapn *hp, size_t p)
{
	subheap *sh = hp->sub;
	if (p == 0)
		return (int)(p+1);
	size_t q = (p-1)/2;
	/* todo get real bat var atom offsets */
	int cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[q]), BUNtvar(&sh->vbi, hp->pos[p]), sh);

	if (!cmp && sh->sub)
		q = subheap_up(hp, sh->sub, q, p);
	else if (sh->min && cmp < 0)
		q = p;
	else if (!sh->min && cmp > 0)
		q = p;

	if (p != q) {
		oid vpos = hp->pos[p];

		hp->pos[p] = hp->pos[q];
		hp->pos[q] = vpos;
		if (q == 0)
			return (int)(q+1);
		return heap_up_any(hp, q);
	}
	p++;
	return (int)p;
}

static gid
heap_del_any( heapn *hp)
{
	hp->used--;
	oid vpos = hp->pos[0];

	hp->pos[0] = hp->pos[hp->used];
	hp->pos[hp->used] = vpos;
	(void) heap_down_any( hp, 0);
	return vpos;
}

static gid
heap_ins_any( heapn *hp, size_t pos, int *err)
{
	subheap *sh = hp->sub;
	const void *val = BUNtvar(&sh->bi, pos);
	size_t p = hp->used;
	if (!hp->full)
		hp->pos[p] = p;
	oid vpos = hp->pos[p];
	if (vpos == BATcount(sh->vb)) {
		if (BUNappend(sh->vb, val, true) != GDK_SUCCEED)
			*err = 1;
	} else if (BUNreplace(sh->vb, vpos, val, true) != GDK_SUCCEED)
		*err = 2;
	bat_iterator_end(&sh->vbi);
	sh->vbi = bat_iterator(sh->vb);
	assert(p < hp->size);
	if (sh->sub)
		*err = subheap_ins( sh->sub, pos, vpos);
	hp->used++;
	hp->full = hp->used == hp->size;
	(void) heap_up_any(hp, p);
	return vpos;
}

static bool
gsubheap_newroot( heapn *hp, subheap *sh, gid g, size_t p)
{
	oid *pos = hp->pos + g * hp->size;
	char *vals = sh->vals;
	char *ivals = sh->ivals;
	int cmp = 0;
	if (sh->var) {
		const void *val = BUNtvar(&sh->bi, p);
 		cmp = sh->cmp(BUNtvar(&sh->vbi, pos[0]), val, sh);
	} else
 		cmp = sh->cmp(vals+(pos[0]*sh->width), ivals+(p*sh->width), sh);
	bool newroot = false;

	if (!cmp && sh->sub)
		newroot = gsubheap_newroot(hp, sh->sub, g, p);
	else if (sh->min && cmp < 0)
		newroot = true;
	else if (!sh->min && cmp > 0)
		newroot = true;
	return newroot;
}

static int
gheap_up_any( heapn *hp, gid g, size_t p)
{
	subheap *sh = hp->sub;
	oid *pos = hp->pos + g * hp->size;  \
	if (p == 0)
		return (int)(p+1);
	size_t q = (p-1)/2;
	/* todo get real bat var atom offsets */
	int cmp = sh->cmp(BUNtvar(&sh->vbi, pos[q]), BUNtvar(&sh->vbi, pos[p]), sh);

	if (!cmp && sh->sub)
		q = gsubheap_up(hp, sh->sub, g, q, p);
	else if (sh->min && cmp < 0)
		q = p;
	else if (!sh->min && cmp > 0)
		q = p;

	if (p != q) {
		oid vpos = pos[p];

		pos[p] = pos[q];
		pos[q] = vpos;
		if (q == 0)
			return (int)(q+1);
		return gheap_up_any(hp, g, q);
	}
	p++;
	return (int)p;
}

static int
gheap_down_any( heapn *hp, gid g, int p)
{
	oid *pos = hp->pos + g * hp->size;	\
	subheap *sh = hp->sub;
	int l = p*2+1, r = p*2+2, q = p;

	if (l < (int)hp->used) {
		int cmp = sh->cmp(BUNtvar(&sh->vbi, pos[q]), BUNtvar(&sh->vbi, pos[l]), sh);

		if (!cmp && sh->sub)
			q = gsubheap_down(hp, sh->sub, g, q, l);
		else if (sh->min && cmp > 0)
			q = l;
		else if (!sh->min && cmp < 0)
			q = l;
	}
	if (r < (int)hp->used) {
		int cmp = sh->cmp(BUNtvar(&sh->vbi, pos[q]), BUNtvar(&sh->vbi, pos[r]), sh);

		if (!cmp && sh->sub)
			q = gsubheap_down(hp, sh->sub, g, q, r);
		else if (sh->min && cmp > 0)
			q = r;
		else if (!sh->min && cmp < 0)
			q = r;
	}
	if (p != q) {
		oid vpos = pos[p];

		pos[p] = pos[q];
		pos[q] = vpos;
		return gheap_down_any(hp, g, q);
	}
	return p+1;
}

static gid
gheap_del_any( heapn *hp, gid g)
{
	hp->useda[g]--;
	oid *pos = hp->pos + g * hp->size;	\
	oid vpos = pos[0];

	pos[0] = pos[hp->useda[g]];
	pos[hp->useda[g]] = vpos;
	(void) gheap_down_any( hp, g, 0);
	return vpos;
}

static gid
gheap_ins_any( heapn *hp, size_t pos, int *err)
{
	gid g = hp->gi[pos];
	size_t p = hp->useda[g];
	if (!hp->fulla[g])
		hp->pos[g*hp->size + p] = g*hp->size + p;
	oid vpos = hp->pos[g*hp->size + p];
	assert(p < hp->size);

	hp->grp[vpos] = g;
	subheap *sh = hp->sub;
	if (sh) {
		const void *val = BUNtvar(&sh->bi, pos);
		/* fill in empty slots */
		if (vpos > BATcount(sh->vb)) {
			for(BUN j = BATcount(sh->vb); j < vpos; j++)
				if (BUNappend(sh->vb, ATOMnilptr(sh->vb->ttype), true) != GDK_SUCCEED)
						*err = 3;
		}
		if (BUNreplace(sh->vb, vpos, val, true) != GDK_SUCCEED)
			*err = 4;
		bat_iterator_end(&sh->vbi);
		sh->vbi = bat_iterator(sh->vb);
		if (sh->sub)
			*err = subheap_ins( sh->sub, pos, vpos);
	}
	hp->useda[g]++;
	hp->fulla[g] = (hp->useda[g] == (int)hp->size);
	if (sh)
		(void) gheap_up_any(hp, g, p);
	return vpos;
}

static int heap_up_lng( heapn *hp, size_t p);

static gid
heap_ins_void( heapn *hp, size_t pos, oid val, int *err)
{
	subheap *sh = hp->sub;
	oid *vals = sh->vals;
	size_t p = hp->used;
	if (!hp->full)
		hp->pos[p] = p;
	oid vpos = hp->pos[p];

	assert(p < hp->size);
	vals[vpos] = val;
	if (sh->sub)
		*err = subheap_ins( sh->sub, pos, vpos);
	hp->used++;
	hp->full = hp->used == hp->size;
	(void) heap_up_lng(hp, p);
	return vpos;
}

#define any_min_op(cmp) cmp < 0
#define any_max_op(cmp) cmp > 0

/* The 2 have 2 compare functions one for nil as largest value (default) and one for nil as smallest value */

#define type_cmp(T,l,r) (is_##T##_nil(l)?(!is_##T##_nil(r)?1:0):(is_##T##_nil(r)?-1:(l<r?-1:((l==r)?0:1))))

#define type_cmp_nsmall(T,l,r) (is_##T##_nil(l)?(!is_##T##_nil(r)?-1:0):(is_##T##_nil(r)?1:(l<r?-1:((l==r)?0:1))))

#define heap_type_cmp(T)												\
static int																\
T##_cmp##_nsmall( const T *v1, const T *v2, const void *sh)				\
{																		\
	(void)sh;															\
	return (is_##T##_nil(*v1)?(!is_##T##_nil(*v2)?-1:0):(is_##T##_nil(*v2)?1:(*v1<*v2?-1:((*v1==*v2)?0:1)))); \
}																		\
																		\
static int																\
T##_cmp( const T *v1, const T *v2, const void *sh )						\
{																		\
	(void)sh;															\
	return (is_##T##_nil(*v1)?(!is_##T##_nil(*v2)?1:0):(is_##T##_nil(*v2)?-1:(*v1<*v2?-1:((*v1==*v2)?0:1)))); \
}

heap_type_cmp(bte)
heap_type_cmp(sht)
heap_type_cmp(int)
heap_type_cmp(lng)
#ifdef HAVE_HGE
heap_type_cmp(hge)
#endif
heap_type_cmp(flt)
heap_type_cmp(dbl)


#define heap_type(T) 					\
										\
static int								\
heap_down_##T( heapn *hp, size_t p)		\
{										\
	subheap *sh = hp->sub;				\
	T *vals = sh->vals;					\
	size_t l = p*2+1, r = p*2+2, q = p;	\
										\
	if (l < hp->used) {					\
		T cmp = sh->nlarge?type_cmp(T, vals[hp->pos[q]] , vals[hp->pos[l]]):type_cmp_nsmall(T, vals[hp->pos[q]], vals[hp->pos[l]]); 			\
										\
		if (!cmp && sh->sub)			\
			q = subheap_down(hp, sh->sub, q, l);	\
		else if (sh->min && cmp > 0) 	\
			q = l;						\
		else if (!sh->min && cmp < 0) 	\
			q = l;						\
	}									\
	if (r < hp->used) {					\
		T cmp = sh->nlarge?type_cmp(T, vals[hp->pos[q]] , vals[hp->pos[r]]):type_cmp_nsmall(T, vals[hp->pos[q]], vals[hp->pos[r]]); 			\
										\
		if (!cmp && sh->sub)			\
			q = subheap_down(hp, sh->sub, q, r);	\
		else if (sh->min && cmp > 0) 	\
			q = r;						\
		else if (!sh->min && cmp < 0) 	\
			q = r;						\
	}									\
	if (p != q) {						\
		oid vpos = hp->pos[p];			\
										\
		hp->pos[p] = hp->pos[q];		\
		hp->pos[q] = vpos;				\
		return heap_down_##T(hp, q);	\
	}									\
	return (int)(p+1);							\
}										\
										\
static int								\
heap_up_##T( heapn *hp, size_t p)		\
{										\
	subheap *sh = hp->sub;				\
	if (p == 0)							\
		return (int)(p+1);						\
	T *vals = sh->vals;					\
	size_t q = (p-1)/2;					\
	T cmp = sh->nlarge?type_cmp(T, vals[hp->pos[q]] , vals[hp->pos[p]]):type_cmp_nsmall(T, vals[hp->pos[q]], vals[hp->pos[p]]); 			\
										\
	if (!cmp && sh->sub)				\
		q = subheap_up(hp, sh->sub, q, p);	\
	if (sh->min && cmp < 0) 			\
		q = p;							\
	else if (!sh->min && cmp > 0) 		\
		q = p;							\
										\
	if (p != q) {						\
		oid vpos = hp->pos[p];			\
										\
		hp->pos[p] = hp->pos[q];		\
		hp->pos[q] = vpos;				\
		if (q == 0)						\
			return (int)(q+1);					\
		return heap_up_##T(hp, q);		\
	}									\
	p++;								\
	return (int)p;							\
}										\
										\
static gid								\
heap_del_##T( heapn *hp) 				\
{										\
	hp->used--;							\
	oid vpos = hp->pos[0];				\
										\
	hp->pos[0] = hp->pos[hp->used];		\
	hp->pos[hp->used] = vpos;			\
	(void) heap_down_##T( hp, 0);		\
	return vpos;						\
}										\
										\
static gid								\
heap_ins_##T( heapn *hp, size_t pos, int *err)	\
{										\
	subheap *sh = hp->sub;				\
	T *ivals = sh->ivals;				\
	T val = ivals[pos];					\
	T *vals = sh->vals;					\
	size_t p = hp->used;				\
	if (!hp->full)						\
		hp->pos[p] = p;					\
	oid vpos = hp->pos[p];				\
										\
	assert(p < hp->size);				\
	vals[vpos] = val;					\
	if (sh->sub)						\
		*err = subheap_ins( sh->sub, pos, vpos);	\
	hp->used++;							\
	hp->full = hp->used == hp->size;	\
	(void) heap_up_##T(hp, p);			\
	return vpos;						\
}						\
\
static int								\
gheap_down_##T( heapn *hp, gid g, size_t p)		\
{										\
	subheap *sh = hp->sub;				\
	oid *pos = hp->pos + g * hp->size;  \
	T *vals = sh->vals;					\
	size_t l = p*2+1, r = p*2+2, q = p;	\
										\
	if (l < (size_t)hp->useda[g]) {					\
		T cmp = sh->nlarge?type_cmp(T, vals[pos[q]] , vals[pos[l]]):type_cmp_nsmall(T, vals[pos[q]], vals[pos[l]]); 			\
										\
		if (!cmp && sh->sub)			\
			q = gsubheap_down(hp, sh->sub, g, q, l);	\
		else if (sh->min && cmp > 0) 	\
			q = l;						\
		else if (!sh->min && cmp < 0) 	\
			q = l;						\
	}									\
	if (r < (size_t)hp->useda[g]) {					\
		T cmp = sh->nlarge?type_cmp(T, vals[pos[q]] , vals[pos[r]]):type_cmp_nsmall(T, vals[pos[q]], vals[pos[r]]); 			\
										\
		if (!cmp && sh->sub)			\
			q = gsubheap_down(hp, sh->sub, g, q, r);	\
		else if (sh->min && cmp > 0) 	\
			q = r;						\
		else if (!sh->min && cmp < 0) 	\
			q = r;						\
	}									\
	if (p != q) {						\
		oid vpos = pos[p];			\
										\
		pos[p] = pos[q];		\
		pos[q] = vpos;				\
		return gheap_down_##T(hp, g, q);	\
	}									\
	return (int)(p+1);							\
}										\
										\
static int								\
gheap_up_##T( heapn *hp, gid g, size_t p)		\
{										\
	subheap *sh = hp->sub;				\
	oid *pos = hp->pos + g * hp->size;  \
	if (p == 0)							\
		return (int)(p+1);						\
	T *vals = sh->vals;					\
	size_t q = (p-1)/2;					\
	T cmp = sh->nlarge?type_cmp(T, vals[pos[q]] , vals[pos[p]]):type_cmp_nsmall(T, vals[pos[q]], vals[pos[p]]); 			\
										\
	if (!cmp && sh->sub)				\
		q = gsubheap_up(hp, sh->sub, g, q, p);	\
	if (sh->min && cmp < 0) 			\
		q = p;							\
	else if (!sh->min && cmp > 0) 		\
		q = p;							\
										\
	if (p != q) {						\
		oid vpos = pos[p];				\
										\
		pos[p] = pos[q];				\
		pos[q] = vpos;					\
		if (q == 0)						\
			return (int)(q+1);					\
		return gheap_up_##T(hp, g, q);	\
	}									\
	p++;								\
	return (int)p;							\
}										\
										\
static gid								\
gheap_del_##T( heapn *hp, gid g) 		\
{										\
	hp->useda[g]--;						\
	oid *pos = hp->pos + g * hp->size;	\
	oid vpos = pos[0];					\
										\
	pos[0] = pos[hp->useda[g]];			\
	pos[hp->useda[g]] = vpos;			\
	(void) gheap_down_##T( hp, g, 0);	\
	return vpos;						\
}										\
										\
static gid								\
gheap_ins_##T( heapn *hp, size_t pos, int *err)	\
{										\
	gid g = hp->gi[pos];				\
	size_t p = hp->useda[g];			\
	if (!hp->fulla[g])					\
		hp->pos[g*hp->size + p] = g*hp->size + p;					\
	oid vpos = hp->pos[g*hp->size + p];	\
	assert(p < hp->size);				\
	\
	hp->grp[vpos] = g;					\
	subheap *sh = hp->sub;				\
	if (sh) {							\
		T *ivals = sh->ivals;			\
		T val = ivals[pos];				\
		T *vals = sh->vals;				\
										\
		vals[vpos] = val;				\
		if (sh->sub)					\
			*err = subheap_ins( sh->sub, pos, vpos);	\
	}									\
	hp->useda[g]++;						\
	hp->fulla[g] = (hp->useda[g] == (int)hp->size);	\
	if (sh)	\
		(void) gheap_up_##T(hp, g, p);			\
	return vpos;						\
}						\
						\
static int				\
topn_##T( size_t n, oid *pos, oid *sl, heapn *hp, int *err)			\
{							\
	size_t i = 0, j = 0;	\
							\
	subheap *sh = hp->sub;	\
	T *hpvals = sh->vals;	\
							\
	T *vals = sh->ivals;	\
	if (hp->used < hp->size) {						\
		for(i=0; i<n && hp->used < hp->size ; i++) {			\
			pos[i] = heap_ins_##T(hp, i, err);		\
			sl[i] = i;		\
		}					\
		j = i;				\
	}						\
	if (sh->nlarge) {		\
		if (sh->min) {		\
			for(; i<n; i++) {						\
				int c = type_cmp(T, hpvals[hp->pos[0]], vals[i]);	\
				if (c < 0 || (sh->sub && c == 0 && subheap_newroot(hp, sh->sub, i))) {	\
					pos[j] = heap_del_##T(hp);			\
					pos[j] = heap_ins_##T(hp, i, err);		\
					sl[j] = i;		\
					j++;	\
				}			\
			}				\
		} else {			\
			for(; i<n; i++) {						\
				int c = type_cmp(T, hpvals[hp->pos[0]], vals[i]);	\
				if (c > 0 || (sh->sub && c == 0 && subheap_newroot(hp, sh->sub, i))) {	\
					pos[j] = heap_del_##T(hp);			\
					pos[j] = heap_ins_##T(hp, i, err);		\
					sl[j] = i;		\
					j++;	\
				}			\
			}				\
		}					\
	} else if (sh->min) {	\
		for(; i<n; i++) {	\
			int c = type_cmp_nsmall(T, hpvals[hp->pos[0]], vals[i]);	\
			if (c < 0 || (sh->sub && c == 0 && subheap_newroot(hp, sh->sub, i))) {	\
				pos[j] = heap_del_##T(hp);			\
				pos[j] = heap_ins_##T(hp, i, err);		\
				sl[j] = i;		\
				j++;		\
			}				\
		}					\
	} else {				\
		for(; i<n; i++) {	\
			int c = type_cmp_nsmall(T, hpvals[hp->pos[0]], vals[i]);	\
			if (c > 0 || (sh->sub && c == 0 && subheap_newroot(hp, sh->sub, i))) {	\
				pos[j] = heap_del_##T(hp);		\
				pos[j] = heap_ins_##T(hp, i, err);	\
				sl[j] = i;		\
				j++;		\
			}				\
		}					\
	}						\
	return j;				\
}							\
							\
static int				\
topn_grouped_##T( size_t n, oid *pos, oid *sl, heapn *hp, int *err)			\
{							\
	size_t i = 0, j = 0;	\
							\
	subheap *sh = hp->sub;	\
	if (!sh) {				\
		if (hp->used < hp->size) {	\
			for(i=0; i<n; i++) {	\
				if (hp->useda[hp->gi[i]] < (int)hp->size) { \
					pos[i] = gheap_ins_##T(hp, i, err);	\
					sl[i] = i;		\
				}				\
			}					\
			j = i;				\
		}						\
		return j;			\
	}						\
	T *hpvals = sh->vals;	\
	T *vals = sh->ivals;	\
	\
	if (sh->nlarge) {		\
		if (sh->min) {		\
			for(; i<n; i++) {						\
				if (hp->useda[hp->gi[i]] < (int)hp->size) { \
					pos[j] = gheap_ins_##T(hp, i, err);		\
					sl[j] = i;	\
					j++;	\
				} else { \
					oid *hppos = hp->pos + hp->gi[i] * hp->size; \
					int c = type_cmp(T, hpvals[hppos[0]], vals[i]);	\
					if (c < 0 || (sh->sub && c == 0 && gsubheap_newroot(hp, sh->sub, hp->gi[i], i))) {	\
						pos[j] = gheap_del_##T(hp, hp->gi[i]);			\
						pos[j] = gheap_ins_##T(hp, i, err);		\
						sl[j] = i;		\
						j++;	\
					}			\
				}			\
			}				\
		} else {			\
			for(; i<n; i++) {						\
				if (hp->useda[hp->gi[i]] < (int)hp->size) { \
					pos[j] = gheap_ins_##T(hp, i, err);		\
					sl[j] = i;	\
					j++;	\
				} else { \
					oid *hppos = hp->pos + hp->gi[i] * hp->size; \
					int c = type_cmp(T, hpvals[hppos[0]], vals[i]);	\
					if (c > 0 || (sh->sub && c == 0 && gsubheap_newroot(hp, sh->sub, hp->gi[i], i))) {	\
						pos[j] = gheap_del_##T(hp, hp->gi[i]);			\
						pos[j] = gheap_ins_##T(hp, i, err);		\
						sl[j] = i;		\
						j++;	\
					}			\
				}			\
			}				\
		}					\
	} else if (sh->min) {	\
		for(; i<n; i++) {	\
			if (hp->useda[hp->gi[i]] < (int)hp->size) { \
				pos[j] = gheap_ins_##T(hp, i, err);		\
				sl[j] = i;	\
				j++;		\
			} else { \
				oid *hppos = hp->pos + hp->gi[i] * hp->size; \
				int c = type_cmp_nsmall(T, hpvals[hppos[0]], vals[i]);	\
				if (c < 0 || (sh->sub && c == 0 && gsubheap_newroot(hp, sh->sub, hp->gi[i], i))) {	\
					pos[j] = gheap_del_##T(hp, hp->gi[i]);			\
					pos[j] = gheap_ins_##T(hp, i, err);		\
					sl[j] = i;		\
					j++;		\
				}				\
			}				\
		}					\
	} else {				\
		for(; i<n; i++) {	\
			if (hp->useda[hp->gi[i]] < (int)hp->size) { \
				pos[j] = gheap_ins_##T(hp, i, err);		\
				sl[j] = i;	\
				j++;		\
			} else { \
				oid *hppos = hp->pos + hp->gi[i] * hp->size; \
				int c = type_cmp_nsmall(T, hpvals[hppos[0]], vals[i]);	\
				if (c > 0 || (sh->sub && c == 0 && gsubheap_newroot(hp, sh->sub, hp->gi[i], i))) {	\
					pos[j] = gheap_del_##T(hp, hp->gi[i]);		\
					pos[j] = gheap_ins_##T(hp, i, err);	\
					sl[j] = i;		\
					j++;		\
				}				\
			}				\
		}					\
	}						\
	return j;				\
}							\
							\



/* create functions */
heap_type(bte)
heap_type(sht)
heap_type(int)
heap_type(lng)
#ifdef HAVE_HGE
heap_type(hge)
#endif
heap_type(flt)
heap_type(dbl)

static void
subheap_destroy( subheap *h)
{
	if (h->sub)
		subheap_destroy(h->sub);
	if (h->var)
		bat_iterator_end(&h->vbi);
	if (h->vb)
		BBPreclaim(h->vb);
	GDKfree(h);
}

static void
heap_destroy( heapn *h)
{
	if (!h)
		return;
	if (h->sub)
		subheap_destroy(h->sub);
	if (h->grouped) {
		BBPreclaim(h->grpb);
		BBPreclaim(h->usedb);
	}
	GDKfree(h);
}

static BAT *
HEAPnew_topn( MalStkPtr s, InstrPtr p, int args, heapn *hp, lng n, BAT *b, bit min, bit nulls_last)
{
	if (b)
		subheap_create(hp, b->ttype, min, nulls_last);
	for(int cur = args; cur < p->argc; cur+=3) {
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
	b = COLnew(0, TYPE_oid, hp->grouped?n*256:n, TRANSIENT);
	if (b)
		b->tsink = (Sink*)hp;
	else
		heap_destroy(hp);
	return b;
}

static int					\
topn_grouped( size_t n, oid *pos, oid *sl, heapn *hp, int *err) \
{							\
	size_t i = 0, j = 0;	\
							\
	if (hp->used < hp->size) {	\
		for(i=0; i<n; i++) {\
			if (hp->useda[hp->gi[i]] < (int)hp->size) { \
				pos[j] = gheap_ins_lng(hp, i, err);	\
				sl[j++] = i;	\
			}				\
		}					\
	}						\
	return j;				\
}							\

static int
topn_void( size_t n, oid *pos, oid *sl, heapn *hp, int *err)
{
	size_t i = 0, j = 0;

	assert(hp->ginb == NULL);
	subheap *sh = hp->sub;
	oid *hpvals = sh->vals;
	oid val = sh->bi.tseq;

	if (hp->used < hp->size) {
		for(i=0; i<n && hp->used < hp->size ; i++) {
			pos[i] = heap_ins_void(hp, i, val+i, err);
			sl[i] = i;
		}
		j = i;
	}
	if (sh->min) {
		for(; i<n; i++) {
			int c = (int)(hpvals[hp->pos[0]] - val + i);
			if (c < 0 || (sh->sub && c == 0 && subheap_newroot(hp, sh->sub, i))) {
				pos[j] = heap_del_lng(hp);
				pos[j] = heap_ins_void(hp, i, val+i, err);
				sl[j] = i;
				j++;
			}
		}
	} else {
		for(; i<n; i++) {
			int c = (int)(hpvals[hp->pos[0]] - val + i);
			if (c > 0 || (sh->sub && c == 0 && subheap_newroot(hp, sh->sub, i))) {
				pos[j] = heap_del_lng(hp);
				pos[j] = heap_ins_void(hp, i, val+i, err);
				sl[j] = i;
				j++;
			}
		}
	}
	return (int)j;
}

static heapn *
_heap_create( int size, bool shared, bool grouped )
{
	heapn *h = (heapn*)GDKzalloc(sizeof(heapn));

	h->s.destroy = (sink_destroy)heap_destroy;
	h->s.type = HEAP_SINK;
	h->shared = shared;
	h->grouped = grouped;
	h->size = size;
	h->used = 0;
	ATOMIC_INIT(&h->counter, -1);
	return h;
}

static int
strCmp_nsmall(char *v1, char *v2, void *sh)
{
	(void)sh;
	return strCmp(v1, v2);
}

static int
strCmp_nlarge(char *v1, char *v2, void *sh)
{
	(void)sh;
	if (strNil(v1)) {
		if (!strNil(v2))
			return 1;
		else
			return 0;
	} else if (strNil(v2)) {
		return -1;
	}
	return strCmp(v1, v2);
}

static int
any_cmp_nsmall( void *v1, void *v2, void *SH)
{
	subheap *sh = SH;
	return sh->cmp2(v1, v2);
}

static int
any_cmp_nlarge( void *v1, void *v2, void *SH)
{
	subheap *sh = SH;
	if (sh->cmp2(v1, sh->nilptr()) == 0) {
		if (sh->cmp2(v2, sh->nilptr()) != 0)
			return 1;
		else
			return 0;
	} else if (sh->cmp2(v2, sh->nilptr()) == 0) {
		return -1;
	}
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
			sh->nlarge = true;
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

static heapn *
heapn_create( int size, bool shared, bool grouped )
{
	return _heap_create( size, shared, grouped);
}

static heapn *
heapn_done( heapn *hp )
{
	/* should only be called once all subheaps are added */
	if (hp->shared && !_heap_init(hp))
		return NULL;
	return hp;
}

static BAT *
HEAPnew_new( MalBlkPtr m, MalStkPtr s, InstrPtr p, heapn *hp, lng n, int tt,  bit min, bit nulls_last)
{
	if (tt >= 0)
		subheap_create(hp, tt, min, nulls_last);
	for(int cur = 6; cur < p->argc; cur+=3) {
		tt = getArgType(m, p, cur);
		min = *getArgReference_bit(s, p, cur+1);
		nulls_last = *getArgReference_bit(s, p, cur+2);
		subheap_create(hp, tt, min, nulls_last);
	}
	if (!heapn_done(hp)) {
		heap_destroy(hp);
		return NULL;
	}
	BAT *b = COLnew(0, TYPE_oid, hp->grouped?(n*256):n, TRANSIENT);
	if (b)
		b->tsink = (Sink*)hp;
	else
		heap_destroy(hp);
	return b;
}

static str
/* PATTERN HEAPnew( bat *HP, lng *N, bit *grouped, ptr *type, bit *min, ...) */
HEAPnew(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr p)
{
	(void)cntxt;
	bat *HP = getArgReference_bat(s, p, 0);
	lng n = *getArgReference_lng(s, p, 1);
	bit grouped = *getArgReference_bit(s, p, 2);

	heapn *hp = heapn_create( (int)n, 1, grouped);

	if (!hp) {
		heap_destroy(hp);
		return createException(SQL, "heapn.new",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	int tt = (p->argc>3)?getArgType(m, p, 3):-1;
	bit min = (p->argc>3)?*getArgReference_bit(s, p, 4):false;
	bit nulls_last = (p->argc>3)?*getArgReference_bit(s, p, 5):false;
	BAT *b = HEAPnew_new(m, s, p, hp, n, tt, min, nulls_last);
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
		for(; i<cnt; i++)		\
			ri[pi[i]] = bi[si[i]];	\
	}

#define aproject(T,w,Toff) \
	if (ATOMstorage(tt) == TYPE_##T && b->twidth == w) { \
		Toff *bi = Tloc(b, 0);	\
		Toff *ri = Tloc(r, 0);	\
								\
		for(; i<cnt; i++)		\
			ri[pi[i]] = bi[si[i]];	\
	}

#define aproject_(T) \
	if (ATOMstorage(tt) == TYPE_##T) { \
		BATiter bi = bat_iterator(b); \
		for(; i<cnt && !err; i++) {		\
			char *v = BUNtvar(&bi, si[i]); \
			if (pi[i] > BATcount(r)) { \
				for(BUN j = BATcount(r); j < pi[i]; j++)  \
					if (BUNappend(r, ATOMnilptr(r->ttype), true) != GDK_SUCCEED) \
						err = 1; \
			} \
			if (BUNreplace(r, pi[i], v, true) != GDK_SUCCEED) \
					err = 1; \
		} \
		bat_iterator_end(&bi); \
	}

static str
HEAPproject(Client ctx, bat *rid, bat *pos, bat *sel, bat *in, const ptr *H)
{
	(void)ctx;
	Pipeline *p = (Pipeline*)*H; /* last arg should move to first argument .. */
	BAT *r = NULL, *P = BATdescriptor(*pos), *S = BATdescriptor(*sel), *b = BATdescriptor(*in);
	int err = 0;
    char *errmsg = NULL;

	if (!P || !S || !b) {
		if (P)
			BBPunfix(P->batCacheid);
		if (S)
			BBPunfix(S->batCacheid);
		assert(!b);
		throw(MAL, "heapn.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	if (*rid && !is_bat_nil(*rid)) {
		r = BATdescriptor(*rid);
		err = (!r);
	}

	int tt = b->ttype;
	bool private = (!r || r->tprivate_bat), local_storage = false;

	if (!private) {
		while(r->unused != P->unused && !ATOMIC_PTR_GET(&p->p->error)) MT_sleep_ms(10);
	}

	BUN size = P->tmaxval;
	if (!err && r && BATcount(b)) {
		MT_lock_set(&r->theaplock);
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid && r->twidth < b->twidth && BATupgrade(r, b, true)) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			err = 1;
		} else if (ATOMvarsized(r->ttype) && r->tvheap->parentid == r->batCacheid && (BATcount(r) ||
				(!VIEWvtparent(b) || BBP_desc(VIEWvtparent(b))->batRestricted != BAT_READ))) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && r->tvheap->parentid != r->batCacheid &&
				r->tvheap->parentid != b->tvheap->parentid) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			if (unshare_varsized_heap(r) != GDK_SUCCEED)
				err = 1;
			local_storage = true;
		} else if (ATOMvarsized(r->ttype) && BATcount(r) == 0 && r->tvheap->parentid == r->batCacheid) {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
			BATswap_heaps(r, b, p);
			assert(r->twidth == b->twidth);
		} else {
			MT_lock_unset(&b->theaplock);
			MT_lock_unset(&r->theaplock);
		}
	} else if (!err && !r) {
		MT_lock_set(&b->theaplock);
		if (ATOMvarsized(tt) && VIEWvtparent(b) && BBP_desc(VIEWvtparent(b))->batRestricted == BAT_READ) {
			MT_lock_unset(&b->theaplock);
			r = COLnew2(0, tt, size, TRANSIENT, b->twidth);
			if (r == NULL) {
				err = 1;
				goto error;
			}
			BATswap_heaps(r, b, p);
		} else {
			MT_lock_unset(&b->theaplock);
			local_storage = true;
			r = COLnew2(0, tt, size, TRANSIENT, b->twidth);
			if (r == NULL) {
				err = 1;
				goto error;
			}
			if (r->tvheap && r->tvheap->base == NULL &&
				ATOMheap(r->ttype, r->tvheap, r->batCapacity) != GDK_SUCCEED) {
				err = 1;
				goto error;
			}
		}
		assert(private);
		r->tprivate_bat = 1;
	}
	if (BATcapacity(r) < size) {
		if (BATextend(r, size) != GDK_SUCCEED) {
            errmsg = createException(MAL, "heapn.project", MAL_MALLOC_FAIL);
			err = 1;
            goto error;
        }
	}

	//if (!private)
		//pipeline_lock1(r);

	if (!err) {
		oid *pi = Tloc(P, 0);
		oid *si = Tloc(S, 0);
		size_t i = 0;
		BUN cnt = BATcount(P);

		project(bte)
		project(sht)
		project(int)
		project(lng)
#ifdef HAVE_HGE
		project(hge)
#endif
		project(flt)
		project(dbl)
		if (local_storage) {
	//		aproject_(str)
	if (ATOMstorage(tt) == TYPE_str) { \
		BATiter bi = bat_iterator(b); \
		for(; i<cnt && !err; i++) {		\
			const char *v = BUNtvar(&bi, si[i]); \
			if (pi[i] > BATcount(r)) { \
				for(BUN j = BATcount(r); j < pi[i]; j++)  \
					if (BUNappend(r, ATOMnilptr(r->ttype), true) != GDK_SUCCEED) \
						err = 1; \
			} \
			if (pi[i] == BATcount(r)) { \
				if (BUNappend(r, v, true) != GDK_SUCCEED) \
					err = 1; \
			} else if (BUNreplace(r, pi[i], v, true) != GDK_SUCCEED) \
				err = 1; \
		} \
		bat_iterator_end(&bi); \
	}
		} else {
			aproject(str,1,uint8_t)
			aproject(str,2,uint16_t)
			aproject(str,4,uint32_t)
			aproject(str,8,var_t)
		}
		if (!err) {
			if (!private)
				pipeline_lock2(r);
			if (BATcount(r) < size)
				BATsetcount(r, size);
			BATnegateprops(r);
			if (!private)
				pipeline_unlock2(r);
		}
	}

error:
	//if (!private)
		//pipeline_unlock1(r);
	BBPunfix(P->batCacheid);
	BBPunfix(b->batCacheid);
	if (errmsg)
		return errmsg;
	if (err)
		throw(MAL, "heapn.project", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	*rid = r->batCacheid;
	BBPkeepref(r);
	if (!private)
		r->unused++;
	return MAL_SUCCEED;
}

static str
HEAPtopn(Client cntxt, MalBlkPtr m, MalStkPtr s, InstrPtr pci)
{
	(void)cntxt; (void)m;

	bool grouped = ((pci->argc - 5)%4);
	int retc = 0;
	int nr_cols = (pci->argc - (grouped?2:0) - 5)/4;
	assert((nr_cols * 4 + (grouped?2:0) + 5) == pci->argc);
	bat *pos = getArgReference_bat(s, pci, retc++);
	bat *sel = getArgReference_bat(s, pci, retc++);
	bat *rgrp = (grouped)?getArgReference_bat(s, pci, retc++):NULL;
	bat *HP = getArgReference_bat(s, pci, retc + nr_cols);

	bool private = (!*HP || is_bat_nil(*HP));

	assert(retc + nr_cols + 1 == pci->retc);
	lng n = *getArgReference_lng(s, pci, pci->retc + 0);
	Pipeline *pp = *(Pipeline**)getArgReference_ptr(s, pci, pci->retc + 1);
	int args = pci->retc + 2 + (grouped?1:0);
	bat *in = nr_cols?getArgReference_bat(s, pci, args++) : NULL;
	BAT *hps, *b = nr_cols?BATdescriptor(*in):NULL;
	BAT *gps = NULL;

	if (nr_cols && !b)
		return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	if (grouped) {
		bat *in = getArgReference_bat(s, pci, pci->retc + 2);
		gps = BATdescriptor(*in);
		if (!gps) {
			BBPreclaim(b);
			return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	if (nr_cols && !b) {
		if (gps) BBPreclaim(gps);
		return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	bit min = (nr_cols)?*getArgReference_bit(s, pci, args++):false;
	bit nulls_last = (nr_cols)?*getArgReference_bit(s, pci, args++):false;

	if (private) {
		if (n < 0) {
			BBPunfix(b->batCacheid);
			if (gps) BBPreclaim(gps);
			throw(MAL, "heap.topn", ILLEGAL_ARGUMENT);
		}
		heapn *hp = heapn_create((int)n, 0, gps?true:false);
		hps = HEAPnew_topn(s, pci, args, hp, n, b, min, nulls_last);
		if (!hps) {
			BBPunfix(b->batCacheid);
			if (gps) BBPreclaim(gps);
			return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		hps->tprivate_bat = 1;
		*HP = hps->batCacheid;
	} else {
		hps = BATdescriptor(*HP);
	}
	private = hps->tprivate_bat;
	heapn *hp = (heapn*)hps->tsink;
	assert(hp && hp->s.type == HEAP_SINK);
	if (((hp->sub && hp->sub->vb == NULL) || (hp->grouped && hp->grpb == NULL)) && !_heap_init(hp)) {
		BBPreclaim(b);
		BBPreclaim(gps);
		return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	subheap *sh = hp->sub;
	int nxt = (int)ATOMIC_INC(&hp->counter);

	if (!private) {
		while(hps->unused != nxt && !ATOMIC_PTR_GET(&pp->p->error)) MT_sleep_ms(10);
	}

	if (!private)
		pipeline_lock1(hps);

	hp->pos = Tloc(hps, 0);
	if (sh) {
		sh->in = b;
		if (!sh->var)
			sh->ivals = Tloc(sh->in, 0);
		else
			sh->bi = bat_iterator(sh->in);
	}
	if (gps) {
		hp->ginb = gps;
		if (hp->ginb)
			hp->gi = Tloc(gps, 0);
		if (gps->tmaxval > hp->gsize) {
			size_t os = hp->gsize;
			hp->gsize = gps->tmaxval;

			if (BATextend(hps, hp->gsize * hp->size) != GDK_SUCCEED) {
				assert(0); /* clean up needed */
				return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			hp->pos = Tloc(hps, 0);

			if (BATextend(hp->grpb, hp->gsize * hp->size) != GDK_SUCCEED) {
				assert(0); /* clean up needed */
				return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			hp->grp = Tloc(hp->grpb, 0);
			if (BATextend(hp->usedb, hp->gsize) != GDK_SUCCEED) {
				assert(0); /* clean up needed */
				return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			hp->useda = Tloc(hp->usedb, 0);
			memset(hp->useda + os, 0, (hp->gsize-os)*sizeof(int));
			if (BATextend(hp->fullb, hp->gsize) != GDK_SUCCEED) {
				assert(0); /* clean up needed */
				return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			hp->fulla = Tloc(hp->fullb, 0);
			memset(hp->fulla + os, 0, (hp->gsize-os));
			for(subheap *nsh = sh; nsh; nsh = nsh->sub) {
				if (BATextend(nsh->vb, hp->gsize * hp->size) != GDK_SUCCEED) {
					assert(0); /* clean up needed */
					return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
				if (!nsh->var) {
					nsh->vals = Tloc(nsh->vb, 0);
				} else {
					bat_iterator_end(&nsh->vbi);
					nsh->vbi = bat_iterator(nsh->vb);
				}
			}
		}
	}
	size_t cnt = sh?BATcount(sh->in):BATcount(gps), i = 0, j = 0;

	BAT *P = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	BAT *S = COLnew(0, TYPE_oid, cnt, TRANSIENT);
	if (!P || !S) {
		BBPreclaim(P);
		BBPreclaim(S);
		return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	P->unused = hps->unused;
	oid *p = Tloc(P, 0);
	oid *sl = Tloc(S, 0);
	subheap *nsh = NULL;
	int cur = pci->retc+2+ (hp->grouped?1:0) + 3, err = 0;
	if (sh) {
		for(nsh = sh->sub; cur < pci->argc; nsh = nsh->sub, cur += 3) {
			bat *in = getArgReference_bat(s, pci, cur);
			nsh->in = BATdescriptor(*in);
			if (!nsh->in) {
				if (!private) {
					ATOMIC_INC(&hps->unused);
					pipeline_unlock1(hps);
				}
				for(nsh = sh; nsh; nsh = nsh->sub) {
					if (nsh->in) {
						BBPunfix(nsh->in->batCacheid);
						if (nsh->var)
							bat_iterator_end(&nsh->bi);
					}
					nsh->in = NULL;
				}
				return createException(SQL, "heapn.topn",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
			}
			if (!nsh->var && nsh->in->ttype)
				nsh->ivals = Tloc(nsh->in, 0);
			else
				nsh->bi = bat_iterator(nsh->in);
			assert(cnt == BATcount(nsh->in) && nsh->min == *getArgReference_bit(s, pci, cur+1));
		}
	}
	if (!b && gps) {
		j = topn_grouped(cnt, p, sl, hp, &err);
	} else if (!b->ttype) {
			j = topn_void(cnt, p, sl, hp, &err);
	} else if (ATOMstorage(b->ttype) == TYPE_bte) {
		if (gps)
			j = topn_grouped_bte(cnt, p, sl, hp, &err);
		else
			j = topn_bte(cnt, p, sl, hp, &err);
	} else if (ATOMstorage(b->ttype) == TYPE_sht) {
		if (gps)
			j = topn_grouped_sht(cnt, p, sl, hp, &err);
		else
			j = topn_sht(cnt, p, sl, hp, &err);
	} else if (b->ttype == TYPE_int) {
		if (gps)
			j = topn_grouped_int(cnt, p, sl, hp, &err);
		else
			j = topn_int(cnt, p, sl, hp, &err);
	} else if (b->ttype == TYPE_date) {
		if (gps)
			j = topn_grouped_int(cnt, p, sl, hp, &err);
		else
			j = topn_int(cnt, p, sl, hp, &err);
	} else if (b->ttype == TYPE_lng) {
		if (gps)
			j = topn_grouped_lng(cnt, p, sl, hp, &err);
		else
			j = topn_lng(cnt, p, sl, hp, &err);
	} else if (b->ttype == TYPE_daytime) {
		if (gps)
			j = topn_grouped_lng(cnt, p, sl, hp, &err);
		else
			j = topn_lng(cnt, p, sl, hp, &err);
	} else if (b->ttype == TYPE_timestamp) {
		if (gps)
			j = topn_grouped_lng(cnt, p, sl, hp, &err);
		else
			j = topn_lng(cnt, p, sl, hp, &err);
#ifdef HAVE_HGE
	} else if (b->ttype == TYPE_hge) {
		if (gps)
			j = topn_grouped_hge(cnt, p, sl, hp, &err);
		else
			j = topn_hge(cnt, p, sl, hp, &err);
#endif
	} else if (b->ttype == TYPE_flt) {
		if (gps)
			j = topn_grouped_flt(cnt, p, sl, hp, &err);
		else
			j = topn_flt(cnt, p, sl, hp, &err);
	} else if (b->ttype == TYPE_dbl) {
		if (gps)
			j = topn_grouped_dbl(cnt, p, sl, hp, &err);
		else
			j = topn_dbl(cnt, p, sl, hp, &err);
	/* TODO add date, daytime and timestamp */
	} else {
		if (hp->grouped) {
			if (sh->min) {
				for(; i<cnt; i++) {
					if (hp->useda[hp->gi[i]] < (int)hp->size) {
						p[j] = gheap_ins_any(hp, i, &err);
						sl[j] = i;
						j++;
					} else {
						oid *hppos = hp->pos + hp->gi[i] * hp->size;
						int cmp = sh->cmp(BUNtvar(&sh->vbi, hppos[0]), BUNtvar(&sh->bi, i), sh);
						if (cmp < 0 || (sh->sub && !cmp && gsubheap_newroot(hp, sh->sub, hp->gi[i], i))) {
							p[j] = gheap_del_any(hp, hp->gi[i]);
							p[j] = gheap_ins_any(hp, i, &err);
							sl[j] = i;
							j++;
						}
					}
				}
			} else {
				for(; i<cnt; i++) {
					if (hp->useda[hp->gi[i]] < (int)hp->size) {
						p[j] = gheap_ins_any(hp, i, &err);
						sl[j] = i;
						j++;
					} else {
						oid *hppos = hp->pos + hp->gi[i] * hp->size;
						int cmp = sh->cmp(BUNtvar(&sh->vbi, hppos[0]), BUNtvar(&sh->bi, i), sh);
						if (cmp > 0 || (sh->sub && !cmp && gsubheap_newroot(hp, sh->sub, hp->gi[i], i))) {
							p[j] = gheap_del_any(hp, hp->gi[i]);
							p[j] = gheap_ins_any(hp, i, &err);
							sl[j] = i;
							j++;
						}
					}
				}
			}
		} else {
			if (hp->used < hp->size) {
				for(i=0; i<cnt && hp->used < hp->size ; i++) {
					p[i] = heap_ins_any(hp, i, &err);
					sl[i] = i;
				}
				j = i;
			}
			if (sh->min) {
				for(; i<cnt; i++) {
					int cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[0]), BUNtvar(&sh->bi, i), sh);
					if (cmp < 0 || (sh->sub && !cmp && subheap_newroot(hp, sh->sub, i))) {
						p[j] = heap_del_any(hp);
						p[j] = heap_ins_any(hp, i, &err);
						sl[j] = i;
						j++;
					}
				}
			} else {
				for(; i<cnt; i++) {
					int cmp = sh->cmp(BUNtvar(&sh->vbi, hp->pos[0]), BUNtvar(&sh->bi, i), sh);
					if (cmp > 0 || (sh->sub && !cmp && subheap_newroot(hp, sh->sub, i))) {
						p[j] = heap_del_any(hp);
						p[j] = heap_ins_any(hp, i, &err);
						sl[j] = i;
						j++;
					}
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
		bat *res = getArgReference_bat(s, pci, retc++);
		*res = nsh->vb->batCacheid;
		BATsetcount(nsh->vb, hp->grouped?hp->size*gps->tmaxval:hp->used);
		BATnegateprops(nsh->vb);
		BBPretain(*res);
		nsh->in = NULL;
	}
	if (rgrp) {
		hp->grpb->tmaxval = hp->ginb->tmaxval;
		BATsetcount(hp->grpb, hp->grpb->tmaxval * hp->size);
		BATnegateprops(hp->grpb);
		*rgrp = hp->grpb->batCacheid;
		BBPretain(*rgrp);
		BBPunfix(hp->ginb->batCacheid);
		hp->ginb = NULL;
	}
	P->tmaxval = hp->grouped?hp->size*gps->tmaxval:hp->used; /* for grouped it should be n*nrgrps */
	BATsetcount(P, j);
	BATnegateprops(P);
	*pos = P->batCacheid;
	BBPkeepref(P);
	BATsetcount(S, j);
	BATnegateprops(S);
	*sel = S->batCacheid;
	BBPkeepref(S);
	if (!private)
		pipeline_unlock2(hps);
	if (!private)
		hps->unused++;
	if (!private)
		pipeline_unlock1(hps);
	BBPkeepref(hps);
	if (err)
		return createException(SQL, "heapn.topn",  SQLSTATE(HY013) "insert failed");
	return MAL_SUCCEED;
}

#define order(T)	\
	if (hp->grouped) { \
		BUN j = 0; \
		for(size_t g = 0; g < hp->gsize; g++) { \
			oid *pos = hp->pos + g * hp->size;  \
			j += hp->useda[g];					\
			for(int i = 0; hp->useda[g]; i++) { \
				rp[j-(i+1)] = pos[0];			\
				(void) gheap_del_##T(hp, g);	\
			}									\
		}										\
		BATsetcount(r, j);						\
	} else {									\
		for(int i = 0; hp->used; i++) {			\
			rp[used-i] = hp->pos[0];			\
			(void) heap_del_##T(hp);			\
		}										\
	}

static str
HEAPorder(Client ctx, bat *rid, bat *hb)
{
	(void)ctx;
	BAT *r = NULL, *hpb = BATdescriptor(*hb);

	if (!hpb)
		throw(MAL, "heapn.order", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	heapn *hp = (heapn*)hpb->tsink;
	r = COLnew(0, TYPE_oid, hp->grouped?hp->gsize*hp->size:hp->size, TRANSIENT);
	if (!r) {
		BBPreclaim(hpb);
		throw(MAL, "heapn.order",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	oid *rp = Tloc(r, 0);
	BATsetcount(r, hp->used);
	BATnegateprops(r);

	size_t used = hp->used-1;
	if (!hp->sub || (!hp->shared && hp->grouped)) {
		BUN j = 0;
		for(size_t g = 0; g < hp->gsize; g++) {
			oid *pos = hp->pos + g * hp->size;
			for (int i = 0; i < hp->useda[g]; i++)
				rp[j++] = pos[i];
		}
		BATsetcount(r, j);
	} else if (!hp->sub->vb->ttype) {
		order(lng);
	} else if (ATOMstorage(hp->sub->vb->ttype) == TYPE_bte) {
		order(bte);
	} else if (ATOMstorage(hp->sub->vb->ttype) == TYPE_sht) {
		order(sht);
	} else if (hp->sub->vb->ttype == TYPE_int) {
		order(int);
	} else if (hp->sub->vb->ttype == TYPE_date) {
		order(int);
	} else if (hp->sub->vb->ttype == TYPE_lng) {
		order(lng);
	} else if (hp->sub->vb->ttype == TYPE_daytime) {
		order(lng);
	} else if (hp->sub->vb->ttype == TYPE_timestamp) {
		order(lng);
#ifdef HAVE_HGE
	} else if (hp->sub->vb->ttype == TYPE_hge) {
		order(hge);
#endif
	} else if (hp->sub->vb->ttype == TYPE_flt) {
		order(flt);
	} else if (hp->sub->vb->ttype == TYPE_dbl) {
		order(dbl);
	} else {
		if (hp->grouped) {
			BUN j = 0;
			for(size_t g = 0; g < hp->gsize; g++) {
				oid *pos = hp->pos + g * hp->size;
				j += hp->useda[g];
				for(int i = 0; hp->useda[g]; i++) {
					rp[j-(i+1)] = pos[0];
					(void) gheap_del_any(hp, g);
				}
			}
			BATsetcount(r, j);
		} else {
			for(int i = 0; hp->used; i++) {
				rp[used-i] = hp->pos[0];
				(void) heap_del_any(hp);
			}
		}
	}
	*rid = r->batCacheid;
	BBPkeepref(r);
	return MAL_SUCCEED;
}

static str
HEAPgroups(Client ctx, bat *rid, bat *pid, bat *gid)
{
	(void)ctx;
	BAT *r = NULL, *p = BATdescriptor(*pid), *g = BATdescriptor(*gid);

	if (!p || !g) {
		BBPreclaim(p);
		throw(MAL, "heapn.groups", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	r = BATproject(p, g);
	BBPreclaim(p);
	if (!r) {
		BBPreclaim(g);
		throw(MAL, "heapn.groups",  SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	r->tmaxval = g->tmaxval;
	BBPreclaim(g);
	BBPkeepref(r);
	*rid = r->batCacheid;
	return MAL_SUCCEED;
}

#include "mel.h"
static mel_func heapn_init_funcs[] = {
    pattern("heapn", "new", HEAPnew, false, "return new heap sink, simple topn no order by cols", args(1,3,
				batarg("sink",oid),
				arg("n",lng),
				arg("grouped",bit)
				)
		   ),
    pattern("heapn", "new", HEAPnew, false, "return new heap sink", args(1,4,
				batarg("sink",oid),
				arg("n",lng),
				arg("grouped",bit),
				vararg("in",any)/*,
				vararg("min", bit),
				vararg("nulls_last", bit)
				*/
				)
		   ),
	pattern("heapn", "topn", HEAPtopn, false, "Return table with heap based topn, and positions and candidate for any projections (without order by, with groups)", args(3,5,
				batarg("pos", oid),
				batarg("sel", oid),
				/*sharedbatarg("rgrp", oid),*/
				sharedbatarg("heap", oid),
				arg("n",lng),
				arg("pipeline", ptr)
				)
			),
	pattern("heapn", "topn", HEAPtopn, false, "Return table with heap based topn, and positions and candidate for any projections (without order by, with groups)", args(3,6,
				batarg("pos", oid),
				batarg("sel", oid),
				/*sharedbatarg("rgrp", oid),*/
				sharedbatarg("heap", oid),
				arg("n",lng),
				arg("pipeline", ptr),
				batarg("grp", oid)
				)
			),
	pattern("heapn", "topn", HEAPtopn, false, "Return table with heap based topn, and positions and candidate for any projections", args(3,6,
				batarg("pos", oid),
				batarg("sel", oid),
				sharedbatvararg("res", any),
				//sharedbatarg("heap", oid),
				arg("n",lng),
				arg("pipeline", ptr),
				vararg("in", any)/*,
				vararg("min", bit),
				vararg("nulls_last", bit)
				*/
				)
			),
	pattern("heapn", "topn", HEAPtopn, false, "Return table with heap based topn, and positions and candidate for any projections", args(3,6,
				batarg("pos", oid),
				batarg("sel", oid),
				sharedbatvararg("res", any),
				//sharedbatarg("heap", oid),
				arg("n",lng),
				arg("pipeline", ptr),
				//batarg("grp", oid),
				vararg("in", any)/*,
				vararg("min", bit),
				vararg("nulls_last", bit)
				*/
				)
			),
	command("heapn", "projection", HEAPproject, false, "Project.", args(1,5,
				batargany("",1),
				batarg("pos", oid),
				batarg("sel", oid),
				batargany("b",1),
				arg("pipeline", ptr)
				)
			),
	command("heapn", "order", HEAPorder, false, "Order.", args(1,2,
				batarg("pos", oid),
				batarg("heap",oid)
				)
			),
	command("heapn", "groups", HEAPgroups, false, "Project groups.", args(1,3,
				batarg("rgrp", oid),
				batarg("pos", oid),
				batarg("grp", oid)
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
