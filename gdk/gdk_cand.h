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

#ifndef _GDK_CAND_H_
#define _GDK_CAND_H_

/* candidates by design are ordered oid lists, besides native oid bats
 * there are
 *	void bats for dense oid lists,
 *	negative oid lists
 *	masked oid lists
 */

#define CAND_NEGOID 0
#define CAND_MSK 1

typedef struct {
	uint64_t
		type:1,
//		mask:1,
		firstbit:48;
} ccand_t;

#define CCAND(b)	((ccand_t *) (b)->tvheap->base)
#define complex_cand(b)	((b)->ttype == TYPE_void && (b)->tvheap != NULL)
#define negoid_cand(b)	(complex_cand(b) && CCAND(b)->type == CAND_NEGOID)
#define mask_cand(b)	(complex_cand(b) && CCAND(b)->type == CAND_MSK)
#define ccand_first(b)	((b)->tvheap->base + sizeof(ccand_t))
#define ccand_free(b)	((b)->tvheap->free - sizeof(ccand_t))

enum cand_type {
	cand_dense,	/* simple dense BAT, i.e. no look ups */
	cand_materialized, /* simple materialized OID list */
	cand_except,	/* list of exceptions in vheap */
	cand_mask,	/* bitmask (TYPE_msk) bat as candidate list */
};

struct canditer {
	BAT *s;			/* candidate BAT the iterator is based on */
	union {
		struct {	/* for all except cand_mask */
			const oid *oids; /* candidate or exceptions for non-dense */
			BUN offset;	/* how much of candidate list BAT we skipped */
			oid add;	/* value to add because of exceptions seen */
		};
		struct {	/* only for cand_mask */
			const uint32_t *mask; /* bitmask */
			BUN nextmsk;
			oid mskoff;
			uint8_t nextbit;
			uint8_t firstbit;
			uint8_t lastbit;
		};
	};
	oid seq;		/* first candidate */
	oid hseq;		/* hseqbase from s/b for first candidate */
	BUN nvals;		/* number of values in .oids/.mask */
	BUN ncand;		/* number of candidates */
	BUN next;		/* next BUN to return value for */
	enum cand_type tpe;
};

/* iterate CI->ncand times using an anonymous index variable, and
 * evaluating the loop count only once */
#define CAND_LOOP(CI)	for (BUN CCTR = 0, CREPS = (CI)->ncand; CCTR < CREPS; CCTR++)
/* iterate CI->ncand times using the given index variable, and
 * evaluating the loop count only once */
#define CAND_LOOP_IDX(CI, IDX)	for (BUN CREPS = (IDX = 0, (CI)->ncand); IDX < CREPS; IDX++)

/* returns the position of the lowest order bit in x, i.e. the
 * smallest n such that (x & (1<<n)) != 0; must not be called with 0 */
__attribute__((__const__))
static inline int
candmask_lobit(uint32_t x)
{
	assert(x != 0);
#ifdef __has_builtin
#if __has_builtin(__builtin_ctz)
	return __builtin_ctz(x) /* ffs(x) - 1 */;
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
#if defined(_MSC_VER)
	unsigned long idx;
	if (_BitScanForward(&idx, x))
		return (int) idx;
	return -1;
#else
	/* use binary search for the lowest set bit */
	int n = 1;
	if ((x & 0x0000FFFF) == 0) { n += 16; x >>= 16; }
	if ((x & 0x000000FF) == 0) { n +=  8; x >>=  8; }
	if ((x & 0x0000000F) == 0) { n +=  4; x >>=  4; }
	if ((x & 0x00000003) == 0) { n +=  2; x >>=  2; }
	return n - (x & 1);
#endif
#endif
#undef BUILTIN_USED
}

/* population count: count number of 1 bits in a value */
__attribute__((__const__))
static inline uint32_t
candmask_pop(uint32_t x)
{
#ifdef __has_builtin
#if __has_builtin(__builtin_popcount)
	return (uint32_t) __builtin_popcount(x);
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
#if defined(_MSC_VER)
	return (uint32_t) __popcnt((unsigned int) (x));
#else
	/* divide and conquer implementation (the two versions are
	 * essentially equivalent, but the first version is written a
	 * bit smarter) */
#if 1
	x -= (x >> 1) & ~0U/3 /* 0x55555555 */; /* 3-1=2; 2-1=1; 1-0=1; 0-0=0 */
	x = (x & ~0U/5) + ((x >> 2) & ~0U/5) /* 0x33333333 */;
	x = (x + (x >> 4)) & ~0UL/0x11 /* 0x0F0F0F0F */;
	x = (x + (x >> 8)) & ~0UL/0x101 /* 0x00FF00FF */;
	x = (x + (x >> 16)) & 0xFFFF /* ~0UL/0x10001 */;
#else
	x = (x & 0x55555555) + ((x >>  1) & 0x55555555);
	x = (x & 0x33333333) + ((x >>  2) & 0x33333333);
	x = (x & 0x0F0F0F0F) + ((x >>  4) & 0x0F0F0F0F);
	x = (x & 0x00FF00FF) + ((x >>  8) & 0x00FF00FF);
	x = (x & 0x0000FFFF) + ((x >> 16) & 0x0000FFFF);
#endif
	return x;
#endif
#endif
#undef BUILTIN_USED
}

#define canditer_next_dense(ci)		((ci)->seq + (ci)->next++)
static inline oid
canditer_next(struct canditer *ci)
{
	oid o;
	if (ci->next == ci->ncand)
		return oid_nil;
	switch (ci->tpe) {
	case cand_dense:
		return canditer_next_dense(ci);
	case cand_materialized:
		assert(ci->next < ci->nvals);
		return ci->oids[ci->next++];
	case cand_except:
		o = ci->seq + ci->add + ci->next++;
		while (ci->add < ci->nvals && o == ci->oids[ci->add]) {
			ci->add++;
			o++;
		}
		return o;
	case cand_mask:
		while ((ci->mask[ci->nextmsk] >> ci->nextbit) == 0) {
			ci->nextmsk++;
			ci->nextbit = 0;
		}
		ci->nextbit += candmask_lobit(ci->mask[ci->nextmsk] >> ci->nextbit);
		o = ci->mskoff + ci->nextmsk * 32 + ci->nextbit;
		if (++ci->nextbit == 32) {
			ci->nextbit = 0;
			ci->nextmsk++;
		}
		ci->next++;
		return o;
	default:
		MT_UNREACHABLE();
	}
}

gdk_export void canditer_init(struct canditer *ci, BAT *b, BAT *s)
	__attribute__((__access__(write_only, 1)));
gdk_export oid canditer_peek(const struct canditer *ci)
	__attribute__((__pure__));
gdk_export oid canditer_last(const struct canditer *ci)
	__attribute__((__pure__));
gdk_export oid canditer_prev(struct canditer *ci);
gdk_export oid canditer_peekprev(const struct canditer *ci)
	__attribute__((__pure__));
gdk_export oid canditer_idx(const struct canditer *ci, BUN p)
	__attribute__((__pure__));
#define canditer_idx_dense(ci, p) ((p >= (ci)->ncand)?oid_nil:((ci)->seq + p))
gdk_export void canditer_setidx(struct canditer *ci, BUN p);
gdk_export void canditer_reset(struct canditer *ci);

__attribute__((__pure__))
static inline BUN
canditer_search_dense(const struct canditer *ci, oid o, bool next)
{
	if (o < ci->seq)
		return next ? 0 : BUN_NONE;
	else if (o >= ci->seq + ci->ncand)
		return next ? ci->ncand : BUN_NONE;
	else
		return o - ci->seq;
}
gdk_export BUN canditer_search(const struct canditer *ci, oid o, bool next)
	__attribute__((__pure__));

__attribute__((__pure__))
static inline bool
canditer_contains(const struct canditer *ci, oid o)
{
	if (ci->tpe == cand_mask) {
		if (o < ci->mskoff)
			return false;
		o -= ci->mskoff;
		BUN p = o / 32;
		if (p >= ci->nvals)
			return false;
		o %= 32;
		if (p == ci->nvals - 1 && o >= ci->lastbit)
			return false;
		return ci->mask[p] & (1U << o);
	}
	return canditer_search(ci, o, false) != BUN_NONE;
}
gdk_export oid canditer_mask_next(const struct canditer *ci, oid o, bool next)
	__attribute__((__pure__));

gdk_export BAT *canditer_slice(const struct canditer *ci, BUN lo, BUN hi);
gdk_export BAT *canditer_sliceval(const struct canditer *ci, oid lo, oid hi);
gdk_export BAT *canditer_slice2(const struct canditer *ci, BUN lo1, BUN hi1, BUN lo2, BUN hi2);
gdk_export BAT *canditer_slice2val(const struct canditer *ci, oid lo1, oid hi1, oid lo2, oid hi2);

gdk_export BAT *BATnegcands(oid tseq, BUN nr, BAT *odels);
gdk_export BAT *BATmaskedcands(oid hseq, BUN nr, BAT *masked, bool selected);
gdk_export BAT *BATunmask(BAT *b);

gdk_export BAT *BATmergecand(BAT *a, BAT *b);
gdk_export BAT *BATintersectcand(BAT *a, BAT *b);
gdk_export BAT *BATdiffcand(BAT *a, BAT *b);

#endif	/* _GDK_CAND_H_ */
