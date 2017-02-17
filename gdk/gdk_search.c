/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

/*
 * @f gdk_search
 *
 */
/*
 * @a M. L. Kersten, P. Boncz, N. Nes
 *
 * @* Search Accelerators
 *
 * What sets BATs apart from normal arrays is their built-in ability
 * to search on both dimensions of the binary association.  The
 * easiest way to implement this is simply walk to the whole table and
 * compare against each element.  This method is of course highly
 * inefficient, much better performance can be obtained if the BATs
 * use some kind of index method to speed up searching.
 *
 * While index methods speed up searching they also have
 * disadvantages.  In the first place extra storage is needed for the
 * index. Second, insertion of data or removing old data requires
 * updating of the index structure, which takes extra time.
 *
 * This means there is a need for both indexed and non-indexed BAT,
 * the first to be used when little or no searching is needed, the
 * second to be used when searching is predominant. Also, there is no
 * best index method for all cases, different methods have different
 * storage needs and different performance. Thus, multiple index
 * methods are provided, each suited to particular types of usage.
 *
 * For query-dominant environments it pays to build a search
 * accelerator.  The main problems to be solved are:
 *
 * - avoidance of excessive storage requirements, and
 * - limited maintenance overhead.
 *
 * The idea that query intensive tasks need many different index
 * methods has been proven invalid. The current direction is multiple
 * copies of data, which can than be sorted or clustered.
 *
 * The BAT library automatically decides when an index becomes cost
 * effective.
 *
 * In situations where an index is expected, a call is made to
 * BAThash.  This operation check for indexing on the header.
 *
 * Interface Declarations
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

#define SORTfndloop(TYPE, CMP, BUNtail, POS)				\
	do {								\
		while (lo < hi) {					\
			cur = mid = (lo + hi) >> 1;			\
			cmp = CMP(BUNtail(bi, POS), v, TYPE);		\
			if (cmp < 0) {					\
				lo = ++mid;				\
				cur++;					\
			} else if (cmp > 0) {				\
				hi = mid;				\
			} else {					\
				break;					\
			}						\
		}							\
	} while (0)

enum find_which {
	FIND_FIRST,
	FIND_ANY,
	FIND_LAST
};

static BUN
SORTfndwhich(BAT *b, const void *v, enum find_which which, int use_orderidx)
{
	BUN lo, hi, mid;
	int cmp;
	BUN cur;
	const oid *o = NULL;
	BATiter bi;
	BUN diff, end;
	int tp;

	if (b == NULL ||
	    (!b->tsorted && !b->trevsorted &&
	     (!use_orderidx || !BATcheckorderidx(b))))
		return BUN_NONE;

	lo = 0;
	hi = BUNlast(b);

	if (BATtdense(b)) {
		/* no need for binary search on dense column */
		if (*(const oid *) v < b->tseqbase ||
		    *(const oid *) v == oid_nil)
			return which == FIND_ANY ? BUN_NONE : lo;
		if (*(const oid *) v >= b->tseqbase + BATcount(b))
			return which == FIND_ANY ? BUN_NONE : hi;
		cur = (BUN) (*(const oid *) v - b->tseqbase) + lo;
		return cur + (which == FIND_LAST);
	}
	if (b->ttype == TYPE_void) {
		assert(b->tseqbase == oid_nil);
		switch (which) {
		case FIND_FIRST:
			if (*(const oid *) v == oid_nil)
				return lo;
		case FIND_LAST:
			return hi;
		default:
			if (lo < hi && *(const oid *) v == oid_nil)
				return lo;
			return BUN_NONE;
		}
	}
	cmp = 1;
	cur = BUN_NONE;
	bi = bat_iterator(b);
	/* only use storage type if comparison functions are equal */
	tp = ATOMbasetype(b->ttype);

	if (use_orderidx) {
		if (b->torderidx == NULL ||
		    b->torderidx->base == NULL) {
			GDKerror("ORDERfindwhich: order idx not found\n");
			return BUN_NONE;
		}
		o = (const oid *) b->torderidx->base + ORDERIDXOFF;
		lo = 0;
		hi = BATcount(b);
	}

	switch (which) {
	case FIND_FIRST:
		end = lo;
		if (lo >= hi ||
		    (use_orderidx ?
		     (atom_GE(BUNtail(bi, (o[lo]&BUN_UNMSK) - b->hseqbase), v, b->ttype)) :
		     (b->tsorted ? atom_GE(BUNtail(bi, lo), v, b->ttype) : atom_LE(BUNtail(bi, lo), v, b->ttype)))) {
			/* shortcut: if BAT is empty or first (and
			 * hence all) tail value is >= v (if sorted)
			 * or <= v (if revsorted), we're done */
			return lo;
		}
		break;
	case FIND_LAST:
		end = hi;
		if (lo >= hi ||
		    (use_orderidx ?
		     (atom_LE(BUNtail(bi, (o[hi - 1]&BUN_UNMSK) - b->hseqbase), v, b->ttype)) :
		     (b->tsorted ? atom_LE(BUNtail(bi, hi - 1), v, b->ttype) : atom_GE(BUNtail(bi, hi - 1), v, b->ttype)))) {
			/* shortcut: if BAT is empty or last (and
			 * hence all) tail value is <= v (if sorted)
			 * or >= v (if revsorted), we're done */
			return hi;
		}
		break;
	default: /* case FIND_ANY -- stupid compiler */
		end = 0;	/* not used in this case */
		if (lo >= hi) {
			/* empty BAT: value not found */
			return BUN_NONE;
		}
		break;
	}

	if (b->tsorted) {
		switch (tp) {
		case TYPE_bte:
			SORTfndloop(bte, simple_CMP, BUNtloc, cur);
			break;
		case TYPE_sht:
			SORTfndloop(sht, simple_CMP, BUNtloc, cur);
			break;
		case TYPE_int:
			SORTfndloop(int, simple_CMP, BUNtloc, cur);
			break;
		case TYPE_lng:
			SORTfndloop(lng, simple_CMP, BUNtloc, cur);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SORTfndloop(hge, simple_CMP, BUNtloc, cur);
			break;
#endif
		case TYPE_flt:
			SORTfndloop(flt, simple_CMP, BUNtloc, cur);
			break;
		case TYPE_dbl:
			SORTfndloop(dbl, simple_CMP, BUNtloc, cur);
			break;
		default:
			if (b->tvarsized)
				SORTfndloop(b->ttype, atom_CMP, BUNtvar, cur);
			else
				SORTfndloop(b->ttype, atom_CMP, BUNtloc, cur);
			break;
		}
	} else if (b->trevsorted) {
		switch (tp) {
		case TYPE_bte:
			SORTfndloop(bte, -simple_CMP, BUNtloc, cur);
			break;
		case TYPE_sht:
			SORTfndloop(sht, -simple_CMP, BUNtloc, cur);
			break;
		case TYPE_int:
			SORTfndloop(int, -simple_CMP, BUNtloc, cur);
			break;
		case TYPE_lng:
			SORTfndloop(lng, -simple_CMP, BUNtloc, cur);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SORTfndloop(hge, -simple_CMP, BUNtloc, cur);
			break;
#endif
		case TYPE_flt:
			SORTfndloop(flt, -simple_CMP, BUNtloc, cur);
			break;
		case TYPE_dbl:
			SORTfndloop(dbl, -simple_CMP, BUNtloc, cur);
			break;
		default:
			if (b->tvarsized)
				SORTfndloop(b->ttype, -atom_CMP, BUNtvar, cur);
			else
				SORTfndloop(b->ttype, -atom_CMP, BUNtloc, cur);
			break;
		}
	} else {
		assert(use_orderidx);
		switch (tp) {
		case TYPE_bte:
			SORTfndloop(bte, simple_CMP, BUNtloc, (o[cur]&BUN_UNMSK) - b->hseqbase);
			break;
		case TYPE_sht:
			SORTfndloop(sht, simple_CMP, BUNtloc, (o[cur]&BUN_UNMSK) - b->hseqbase);
			break;
		case TYPE_int:
			SORTfndloop(int, simple_CMP, BUNtloc, (o[cur]&BUN_UNMSK) - b->hseqbase);
			break;
		case TYPE_lng:
			SORTfndloop(lng, simple_CMP, BUNtloc, (o[cur]&BUN_UNMSK) - b->hseqbase);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			SORTfndloop(hge, simple_CMP, BUNtloc, (o[cur]&BUN_UNMSK) - b->hseqbase);
			break;
#endif
		case TYPE_flt:
			SORTfndloop(flt, simple_CMP, BUNtloc, (o[cur]&BUN_UNMSK) - b->hseqbase);
			break;
		case TYPE_dbl:
			SORTfndloop(dbl, simple_CMP, BUNtloc, (o[cur]&BUN_UNMSK) - b->hseqbase);
			break;
		default:
			assert(0);
			break;
		}
	}

	switch (which) {
	case FIND_FIRST:
		if (cmp == 0 && !b->tkey) {
			/* shift over multiple equals */
			if (use_orderidx) {
				while (--cur >= end && !(o[cur]&BUN_MSK)) {
					;
				}
				cur++;
			} else {
				for (diff = cur - end; diff; diff >>= 1) {
					while (cur >= end + diff &&
					       atom_EQ(BUNtail(bi, cur - diff), v, b->ttype))
						cur -= diff;
				}
			}
		}
		break;
	case FIND_LAST:
		if (cmp == 0 && !b->tkey) {
			/* shift over multiple equals */
			if (use_orderidx) {
				while (cur < end && !(o[cur]&BUN_MSK)) {
					cur++;
				}
			} else {
				for (diff = (end - cur) >> 1; diff; diff >>= 1) {
					while (cur + diff < end &&
					       atom_EQ(BUNtail(bi, cur + diff), v, b->ttype)) {
						cur += diff;
					}
				}
			}
		}
		cur += (cmp == 0);
		break;
	default: /* case FIND_ANY -- stupid compiler */
		if (cmp) {
			/* not found */
			cur = BUN_NONE;
		}
		break;
	}

	return cur;
}

/* Return the BUN of any tail value in b that is equal to v; if no
 * match is found, return BUN_NONE.  b must be sorted (reverse or
 * forward). */
BUN
SORTfnd(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_ANY, 0);
}

/* use orderidx, returns BUN on order index */
BUN
ORDERfnd(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_ANY, 1);
}

/* Return the BUN of the first (lowest numbered) tail value that is
 * equal to v; if no match is found, return the BUN of the next higher
 * value in b.  b must be sorted (reverse or forward). */
BUN
SORTfndfirst(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_FIRST, 0);
}

/* use orderidx, returns BUN on order index */
BUN
ORDERfndfirst(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_FIRST, 1);
}

/* Return the BUN of the first (lowest numbered) tail value beyond v.
 * b must be sorted (reverse or forward). */
BUN
SORTfndlast(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_LAST, 0);
}

/* use orderidx, returns BUN on order index */
BUN
ORDERfndlast(BAT *b, const void *v)
{
	return SORTfndwhich(b, v, FIND_LAST, 1);
}
