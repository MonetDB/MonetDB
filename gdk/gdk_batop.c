/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * (c) M. L. Kersten, P. Boncz, S. Manegold, N. Nes, K.S. Mullender
 * Common BAT Operations
 * We factor out all possible overhead by inlining code.  This
 * includes the macros BUNhead and BUNtail, which do a test to see
 * whether the atom resides in the buns or in a variable storage
 * heap.
 */
#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

gdk_return
unshare_varsized_heap(BAT *b)
{
	if (ATOMvarsized(b->ttype) &&
	    b->tvheap->parentid != b->batCacheid) {
		Heap *h = GDKmalloc(sizeof(Heap));
		if (h == NULL)
			return GDK_FAIL;
		MT_thread_setalgorithm("unshare vheap");
		*h = (Heap) {
			.parentid = b->batCacheid,
			.farmid = BBPselectfarm(b->batRole, TYPE_str, varheap),
		};
		strconcat_len(h->filename, sizeof(h->filename),
			      BBP_physical(b->batCacheid), ".theap", NULL);
		if (HEAPcopy(h, b->tvheap, 0) != GDK_SUCCEED) {
			HEAPfree(h, true);
			GDKfree(h);
			return GDK_FAIL;
		}
		ATOMIC_INIT(&h->refs, 1);
		MT_lock_set(&b->theaplock);
		int parent = b->tvheap->parentid;
		HEAPdecref(b->tvheap, false);
		b->tvheap = h;
		MT_lock_unset(&b->theaplock);
		BBPunshare(parent);
		BBPunfix(parent);
	}
	return GDK_SUCCEED;
}

/* We try to be clever when appending one string bat to another.
 * First of all, we try to actually share the string heap so that we
 * don't need an extra copy, and if that can't be done, we see whether
 * it makes sense to just quickly copy the whole string heap instead
 * of inserting individual strings.  See the comments in the code for
 * more information. */
static gdk_return
insert_string_bat(BAT *b, BAT *n, struct canditer *ci, bool force, bool mayshare, lng timeoffset)
{
	BATiter ni;		/* iterator */
	size_t toff = ~(size_t) 0;	/* tail offset */
	BUN p, r;		/* loop variables */
	const void *tp = NULL;	/* tail value pointer */
	unsigned char tbv;	/* tail value-as-bte */
	unsigned short tsv;	/* tail value-as-sht */
#if SIZEOF_VAR_T == 8
	unsigned int tiv;	/* tail value-as-int */
#endif
	var_t v;		/* value */
	size_t off;		/* offset within n's string heap */
	BUN cnt = ci->ncand;
	BUN oldcnt = BATcount(b);

	assert(b->ttype == TYPE_str);
	assert(b->tbaseoff == 0);
	assert(b->theap->parentid == b->batCacheid);
	/* only transient bats can use some other bat's string heap */
	assert(b->batRole == TRANSIENT || b->tvheap->parentid == b->batCacheid);
	if (cnt == 0)
		return GDK_SUCCEED;
	ni = bat_iterator(n);

	if (b->tvheap == ni.vh) {
		/* vheaps are already shared, continue doing so: we just
		 * need to append the offsets */
		toff = 0;
		MT_thread_setalgorithm("shared vheap");
	} else if (mayshare && b->batRole == TRANSIENT && oldcnt == 0) {
		/* we can share the vheaps, so we then only need to
		 * append the offsets */
		MT_lock_set(&b->theaplock);
		if (b->tvheap->parentid != b->batCacheid) {
			BBPunshare(b->tvheap->parentid);
			BBPunfix(b->tvheap->parentid);
		}
		HEAPdecref(b->tvheap, b->tvheap->parentid == b->batCacheid);
		HEAPincref(ni.vh);
		b->tvheap = ni.vh;
		BBPshare(ni.vh->parentid);
		MT_lock_unset(&b->theaplock);
		toff = 0;
		MT_thread_setalgorithm("share vheap");
	} else {
		/* no heap sharing, so also make sure the heap isn't
		 * shared currently (we're not allowed to write in
		 * another bat's heap) */
		if (b->tvheap->parentid != b->batCacheid &&
		    unshare_varsized_heap(b) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
		if (oldcnt == 0 || (!GDK_ELIMDOUBLES(b->tvheap) &&
				    !GDK_ELIMDOUBLES(ni.vh))) {
			/* we'll consider copying the string heap completely
			 *
			 * we first estimate how much space the string heap
			 * should occupy, given the number of rows we need to
			 * insert, then, if that is way smaller than the actual
			 * space occupied, we will skip the copy and just insert
			 * one by one */
			size_t len = 0;
			for (int i = 0; i < 1024; i++) {
				p = (BUN) (((double) rand() / RAND_MAX) * (cnt - 1));
				p = canditer_idx(ci, p) - n->hseqbase;
				len += strlen(BUNtvar(ni, p)) + 1;
			}
			len = (len + 512) / 1024; /* rounded average length */
			r = (GDK_ELIMLIMIT - GDK_STRHASHSIZE) / (len + 12);
			/* r is estimate of number of strings in
			 * double-eliminated area */
			if (r < ci->ncand)
				len = GDK_ELIMLIMIT + (ci->ncand - r) * len;
			else
				len = GDK_STRHASHSIZE + ci->ncand * (len + 12);
			/* len is total estimated expected size of vheap */

			if (len > ni.vhfree / 2) {
				/* we copy the string heap, perhaps appending */
				if (oldcnt == 0) {
					toff = 0;
					MT_thread_setalgorithm("copy vheap");
				} else {
					toff = (b->tvheap->free + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
					MT_thread_setalgorithm("append vheap");
				}

				if (HEAPgrow(&b->theaplock, &b->tvheap, toff + ni.vh->size, force) != GDK_SUCCEED) {
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
				memcpy(b->tvheap->base + toff, ni.vh->base, ni.vhfree);
				b->tvheap->free = toff + ni.vhfree;
				b->tvheap->dirty = true;
			}
		}
	}
	/* if toff has the initial value of ~0, we insert strings
	 * individually, otherwise we only copy (insert) offsets */
	if (toff == ~(size_t) 0)
		v = GDK_VAROFFSET;
	else
		v = b->tvheap->free - 1;

	/* make sure there is (vertical) space in the offset heap, we
	 * may also widen thanks to v, set above */
	if (GDKupgradevarheap(b, v, oldcnt + cnt < b->batCapacity ? b->batCapacity : oldcnt + cnt, b->batCount) != GDK_SUCCEED) {
		bat_iterator_end(&ni);
		return GDK_FAIL;
	}

	if (toff == 0 && ni.width == b->twidth && ci->tpe == cand_dense) {
		/* we don't need to do any translation of offset
		 * values, so we can use fast memcpy */
		MT_thread_setalgorithm("memcpy offsets");
		memcpy(Tloc(b, BUNlast(b)), (const char *) ni.base + ((ci->seq - n->hseqbase) << ni.shift), cnt << ni.shift);
	} else if (toff != ~(size_t) 0) {
		/* we don't need to insert any actual strings since we
		 * have already made sure that they are all in b's
		 * string heap at known locations (namely the offset
		 * in n added to toff), so insert offsets from n after
		 * adding toff into b */
		/* note the use of the "restrict" qualifier here: all
		 * four pointers below point to the same value, but
		 * only one of them will actually be used, hence we
		 * still obey the rule for restrict-qualified
		 * pointers */
		const unsigned char *restrict tbp = (const unsigned char *) ni.base;
		const unsigned short *restrict tsp = (const unsigned short *) ni.base;
#if SIZEOF_VAR_T == 8
		const unsigned int *restrict tip = (const unsigned int *) ni.base;
#endif
		const var_t *restrict tvp = (const var_t *) ni.base;

		switch (b->twidth) {
		case 1:
			tp = &tbv;
			break;
		case 2:
			tp = &tsv;
			break;
#if SIZEOF_VAR_T == 8
		case 4:
			tp = &tiv;
			break;
		case 8:
			tp = &v;
			break;
#else
		case 4:
			tp = &v;
			break;
#endif
		default:
			assert(0);
		}
		MT_thread_setalgorithm("copy offset values");
		r = b->batCount;
		TIMEOUT_LOOP(cnt, timeoffset) {
			p = canditer_next(ci) - n->hseqbase;
			switch (ni.width) {
			case 1:
				v = (var_t) tbp[p] + GDK_VAROFFSET;
				break;
			case 2:
				v = (var_t) tsp[p] + GDK_VAROFFSET;
				break;
#if SIZEOF_VAR_T == 8
			case 4:
				v = (var_t) tip[p];
				break;
#endif
			default:
				v = tvp[p];
				break;
			}
			v = (var_t) ((size_t) v + toff);
			assert(v >= GDK_VAROFFSET);
			assert((size_t) v < b->tvheap->free);
			switch (b->twidth) {
			case 1:
				assert(v - GDK_VAROFFSET < ((var_t) 1 << 8));
				((uint8_t *) b->theap->base)[r++] = (uint8_t) (v - GDK_VAROFFSET);
				break;
			case 2:
				assert(v - GDK_VAROFFSET < ((var_t) 1 << 16));
				((uint16_t *) b->theap->base)[r++] = (uint16_t) (v - GDK_VAROFFSET);
				break;
#if SIZEOF_VAR_T == 8
			case 4:
				assert(v < ((var_t) 1 << 32));
				((uint32_t *) b->theap->base)[r++] = (uint32_t) v;
				break;
#endif
			default:
				((var_t *) b->theap->base)[r++] = v;
				break;
			}
		}
	} else if (b->tvheap->free < ni.vhfree / 2 ||
		   GDK_ELIMDOUBLES(b->tvheap)) {
		/* if b's string heap is much smaller than n's string
		 * heap, don't bother checking whether n's string
		 * values occur in b's string heap; also, if b is
		 * (still) fully double eliminated, we must continue
		 * to use the double elimination mechanism */
		r = b->batCount;
		oid hseq = n->hseqbase;
		MT_thread_setalgorithm("insert string values");
		TIMEOUT_LOOP(cnt, timeoffset) {
			p = canditer_next(ci) - hseq;
			tp = BUNtvar(ni, p);
			if (tfastins_nocheckVAR(b, r, tp) != GDK_SUCCEED) {
				bat_iterator_end(&ni);
				return GDK_FAIL;
			}
			r++;
		}
	} else {
		/* Insert values from n individually into b; however,
		 * we check whether there is a string in b's string
		 * heap at the same offset as the string is in n's
		 * string heap (in case b's string heap is a copy of
		 * n's).  If this is the case, we just copy the
		 * offset, otherwise we insert normally.  */
		r = b->batCount;
		MT_thread_setalgorithm("insert string values with check");
		TIMEOUT_LOOP(cnt, timeoffset) {
			p = canditer_next(ci) - n->hseqbase;
			off = BUNtvaroff(ni, p); /* the offset */
			tp = ni.vh->base + off; /* the string */
			if (off < b->tvheap->free &&
			    strcmp(b->tvheap->base + off, tp) == 0) {
				/* we found the string at the same
				 * offset in b's string heap as it was
				 * in n's string heap, so we don't
				 * have to insert a new string into b:
				 * we can just copy the offset */
				v = (var_t) off;
				switch (b->twidth) {
				case 1:
					assert(v - GDK_VAROFFSET < ((var_t) 1 << 8));
					((uint8_t *) b->theap->base)[r] = (unsigned char) (v - GDK_VAROFFSET);
					break;
				case 2:
					assert(v - GDK_VAROFFSET < ((var_t) 1 << 16));
					((uint16_t *) b->theap->base)[r] = (unsigned short) (v - GDK_VAROFFSET);
					break;
#if SIZEOF_VAR_T == 8
				case 4:
					assert(v < ((var_t) 1 << 32));
					((uint32_t *) b->theap->base)[r] = (unsigned int) v;
					break;
#endif
				default:
					((var_t *) b->theap->base)[r] = v;
					break;
				}
			} else {
				if (tfastins_nocheckVAR(b, r, tp) != GDK_SUCCEED) {
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
			}
			r++;
		}
	}
	bat_iterator_end(&ni);
	TIMEOUT_CHECK(timeoffset, TIMEOUT_HANDLER(GDK_FAIL));
	MT_lock_set(&b->theaplock);
	BATsetcount(b, oldcnt + ci->ncand);
	assert(b->batCapacity >= b->batCount);
	MT_lock_unset(&b->theaplock);
	/* maintain hash */
	MT_rwlock_wrlock(&b->thashlock);
	for (r = oldcnt, cnt = BATcount(b); b->thash && r < cnt; r++) {
		HASHappend_locked(b, r, b->tvheap->base + VarHeapVal(Tloc(b, 0), r, b->twidth));
	}
	MT_rwlock_wrunlock(&b->thashlock);
	return GDK_SUCCEED;
}

static gdk_return
append_varsized_bat(BAT *b, BAT *n, struct canditer *ci, bool mayshare)
{
	BATiter ni;
	BUN cnt = ci->ncand, r;
	oid hseq = n->hseqbase;

	/* only transient bats can use some other bat's vheap */
	assert(b->batRole == TRANSIENT || b->tvheap->parentid == b->batCacheid);
	/* make sure the bats use var_t */
	assert(b->twidth == n->twidth);
	assert(b->twidth == SIZEOF_VAR_T);
	if (cnt == 0)
		return GDK_SUCCEED;
	if (cnt > BATcapacity(b) - BATcount(b)) {
		/* if needed space exceeds a normal growth extend just
		 * with what's needed */
		BUN ncap = BATcount(b) + cnt;
		BUN grows = BATgrows(b);

		if (ncap > grows)
			grows = ncap;
		if (BATextend(b, grows) != GDK_SUCCEED)
			return GDK_FAIL;
	}
	ni = bat_iterator(n);
	if (mayshare &&
	    BATcount(b) == 0 &&
	    b->batRole == TRANSIENT &&
	    n->batRestricted == BAT_READ &&
	    b->tvheap != ni.vh) {
		/* if b is still empty, in the transient farm, and n
		 * is read-only, we replace b's vheap with a reference
		 * to n's */
		MT_lock_set(&b->theaplock);
		if (b->tvheap->parentid != b->batCacheid) {
			BBPunshare(b->tvheap->parentid);
			BBPunfix(b->tvheap->parentid);
		}
		BBPshare(ni.vh->parentid);
		HEAPdecref(b->tvheap, true);
		HEAPincref(ni.vh);
		b->tvheap = ni.vh;
		MT_lock_unset(&b->theaplock);
	}
	if (b->tvheap == ni.vh) {
		/* if b and n use the same vheap, we only need to copy
		 * the offsets from n to b */
		if (ci->tpe == cand_dense) {
			/* fast memcpy since we copy a consecutive
			 * chunk of memory */
			memcpy(Tloc(b, BUNlast(b)),
			       (const var_t *) ni.base + (ci->seq - hseq),
			       cnt << b->tshift);
		} else {
			var_t *restrict dst = (var_t *) Tloc(b, BUNlast(b));
			const var_t *restrict src = (const var_t *) ni.base;
			while (cnt > 0) {
				cnt--;
				*dst++ = src[canditer_next(ci) - hseq];
			}
		}
		MT_lock_set(&b->theaplock);
		BATsetcount(b, BATcount(b) + ci->ncand);
		MT_lock_unset(&b->theaplock);
		/* maintain hash table */
		MT_rwlock_wrlock(&b->thashlock);
		for (BUN i = BATcount(b) - ci->ncand;
		     b->thash && i < BATcount(b);
		     i++) {
			HASHappend_locked(b, i, b->tvheap->base + *(var_t *) Tloc(b, i));
		}
		MT_rwlock_wrunlock(&b->thashlock);
		bat_iterator_end(&ni);
		return GDK_SUCCEED;
	}
	/* b and n do not share their vheap, so we need to copy data */
	if (b->tvheap->parentid != b->batCacheid) {
		/* if b shares its vheap with some other bat, unshare it */
		Heap *h = GDKmalloc(sizeof(Heap));
		if (h == NULL) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
		*h = (Heap) {
			.parentid = b->batCacheid,
			.farmid = BBPselectfarm(b->batRole, b->ttype, varheap),
		};
		strconcat_len(h->filename, sizeof(h->filename),
			      BBP_physical(b->batCacheid), ".theap", NULL);
		if (HEAPcopy(h, b->tvheap, 0) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			HEAPfree(h, true);
			GDKfree(h);
			return GDK_FAIL;
		}
		bat parid = b->tvheap->parentid;
		BBPunshare(parid);
		MT_lock_set(&b->theaplock);
		HEAPdecref(b->tvheap, false);
		ATOMIC_INIT(&h->refs, 1);
		b->tvheap = h;
		MT_lock_unset(&b->theaplock);
		BBPunfix(parid);
	}
	if (BATcount(b) == 0 && BATatoms[b->ttype].atomFix == NULL &&
	    ci->tpe == cand_dense && ci->ncand == ni.count) {
		/* just copy the heaps */
		if (HEAPgrow(&b->theaplock, &b->tvheap, ni.vhfree, false) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
		memcpy(b->theap->base, ni.base, ni.hfree);
		memcpy(b->tvheap->base, ni.vh->base, ni.vhfree);
		b->theap->free = ni.hfree;
		b->tvheap->free = ni.vhfree;
		BATsetcount(b, ni.count);
		b->tnil = n->tnil;
		b->tnonil = n->tnonil;
		b->tsorted = n->tsorted;
		b->tnosorted = n->tnosorted;
		b->trevsorted = n->trevsorted;
		b->tnorevsorted = n->tnorevsorted;
		b->tkey = n->tkey;
		b->tnokey[0] = n->tnokey[0];
		b->tnokey[1] = n->tnokey[1];
		b->tminpos = n->tminpos;
		b->tmaxpos = n->tmaxpos;
		b->tunique_est = n->tunique_est;
		bat_iterator_end(&ni);
		return GDK_SUCCEED;
	}
	/* copy data from n to b */
	r = BUNlast(b);
	for (BUN i = 0; i < cnt; i++) {
		BUN p = canditer_next(ci) - hseq;
		const void *t = BUNtvar(ni, p);
		if (tfastins_nocheckVAR(b, r, t) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
		r++;
	}
	MT_rwlock_wrlock(&b->thashlock);
	if (b->thash) {
		r -= cnt;
		BATiter bi = bat_iterator_nolock(b);
		for (BUN i = 0; i < cnt; i++) {
			const void *t = BUNtvar(bi, r);
			HASHappend_locked(b, r, t);
			r++;
		}
	}
	MT_rwlock_wrunlock(&b->thashlock);
	MT_lock_set(&b->theaplock);
	BATsetcount(b, r);
	MT_lock_unset(&b->theaplock);
	bat_iterator_end(&ni);
	return GDK_SUCCEED;
}

static gdk_return
append_msk_bat(BAT *b, BAT *n, struct canditer *ci)
{
	if (ci->ncand == 0)
		return GDK_SUCCEED;
	if (BATextend(b, BATcount(b) + ci->ncand) != GDK_SUCCEED)
		return GDK_FAIL;

	MT_lock_set(&b->theaplock);

	uint32_t boff = b->batCount % 32;
	uint32_t *bp = (uint32_t *) b->theap->base + b->batCount / 32;
	b->batCount += ci->ncand;
	b->theap->dirty = true;
	b->theap->free = ((b->batCount + 31) / 32) * 4;
	BATiter ni = bat_iterator(n);
	if (ci->tpe == cand_dense) {
		const uint32_t *np;
		uint32_t noff, mask;
		BUN cnt;
		noff = (ci->seq - n->hseqbase) % 32;
		cnt = ci->ncand;
		np = (const uint32_t *) ni.base + (ci->seq - n->hseqbase) / 32;
		if (boff == noff) {
			/* words of b and n are aligned, so we don't
			 * need to shift bits around */
			if (boff + cnt <= 32) {
				/* all new bits within one word */
				if (cnt == 32) {
					*bp = *np;
				} else {
					mask = ((1U << cnt) - 1) << boff;
					*bp &= ~mask;
					*bp |= *np & mask;
				}
			} else {
				/* multiple words of b are affected */
				if (boff != 0) {
					/* first fill up the rest of the first
					 * word */
					mask = ~0U << boff;
					*bp &= ~mask;
					*bp++ |= *np++ & mask;
					cnt -= 32 - boff;
				}
				if (cnt >= 32) {
					/* copy an integral number of words fast */
					BUN nw = cnt / 32;
					memcpy(bp, np, nw*sizeof(int));
					bp += nw;
					np += nw;
					cnt %= 32;
				}
				if (cnt > 0) {
					/* do the left over bits */
					mask = (1U << cnt) - 1;
					*bp = *np & mask;
				}
			}
		} else if (boff > noff) {
			if (boff + cnt <= 32) {
				/* we only need to copy bits from a
				 * single word of n to a single word
				 * of b */
				/* boff > 0, so cnt < 32, hence the
				 * shift is ok */
				mask = (1U << cnt) - 1;
				*bp &= ~(mask << boff);
				*bp |= (*np & (mask << noff)) << (boff - noff);
			} else {
				/* first fill the rest of the last partial
				 * word of b, so that's 32-boff bits */
				mask = (1U << (32 - boff)) - 1;
				*bp &= ~(mask << boff);
				*bp++ |= (*np & (mask << noff)) << (boff - noff);
				cnt -= 32 - boff;

				/* set boff and noff to the amount we need to
				 * shift bits in consecutive words of n around
				 * to fit into the next word of b; set mask to
				 * the mask of the bottom bits of n that fit
				 * in a word of b (and the complement are the
				 * top bits that go to another word of b) */
				boff -= noff;
				noff = 32 - boff;
				mask = (1U << noff) - 1;
				while (cnt >= 32) {
					*bp = (*np++ & ~mask) >> noff;
					*bp++ |= (*np & mask) << boff;
					cnt -= 32;
				}
				if (cnt > noff) {
					/* the last bits come from two words
					 * in n */
					*bp = (*np++ & ~mask) >> noff;
					cnt -= noff;
					mask = (1U << cnt) - 1;
					*bp++ |= (*np & mask) << boff;
				} else if (cnt > 0) {
					/* the last bits come from a single
					 * word in n */
					mask = ((1U << cnt) - 1) << noff;
					*bp = (*np & mask) >> noff;
				}
			}
		} else {
			/* boff < noff */
			if (noff + cnt <= 32) {
				/* only need part of the first word of n */
				mask = (1U << cnt) - 1;
				*bp &= ~(mask << boff);
				*bp |= (*np & (mask << noff)) >> (noff - boff);
			} else if (boff + cnt <= 32) {
				/* only need to fill a single word of
				 * b, but from two of n */
				if (cnt < 32)
					*bp &= ~(((1U << cnt) - 1) << boff);
				else
					*bp = 0;
				mask = ~((1U << noff) - 1);
				*bp |= (*np++ & mask) >> (noff - boff);
				cnt -= 32 - noff;
				mask = (1U << cnt) - 1;
				*bp |= (*np & mask) << (32 - noff);
			} else {
				if (boff > 0) {
					/* fill the rest of the first word of b */
					cnt -= 32 - boff;
					*bp &= (1U << boff) - 1;
					mask = ~((1U << noff) - 1);
					noff -= boff;
					boff = 32 - noff;
					*bp |= (*np++ & mask) >> noff;
					*bp |= (*np & ((1U << noff) - 1)) << boff;
				} else {
					boff = 32 - noff;
				}
				mask = (1U << noff) - 1;
				while (cnt >= 32) {
					*bp = (*np++ & ~mask) >> noff;
					*bp++ |= (*np & mask) << boff;
					cnt -= 32;
				}
				if (cnt > 0) {
					*bp = (*np++ & ~mask) >> noff;
					if (cnt > noff)
						*bp++ |= (*np & mask) << boff;
				}
			}
		}
	} else {
		oid o;
		uint32_t v = boff > 0 ? *bp & ((1U << boff) - 1) : 0;
		do {
			for (uint32_t i = boff; i < 32; i++) {
				o = canditer_next(ci);
				if (is_oid_nil(o))
					break;
				o -= n->hseqbase;
				v |= (uint32_t) Tmskval(&ni, o - n->hseqbase) << i;
			}
			*bp++ = v;
			v = 0;
			boff = 0;
		} while (!is_oid_nil(o));
	}
	bat_iterator_end(&ni);
	MT_lock_unset(&b->theaplock);
	return GDK_SUCCEED;
}

/* Append the contents of BAT n (subject to the optional candidate
 * list s) to BAT b.  If b is empty, b will get the seqbase of s if it
 * was passed in, and else the seqbase of n. */
gdk_return
BATappend2(BAT *b, BAT *n, BAT *s, bool force, bool mayshare)
{
	struct canditer ci;
	BUN cnt;
	BUN r;
	oid hseq = n->hseqbase;
	char buf[64];
	lng t0 = 0;

	if (b == NULL || n == NULL || BATcount(n) == 0) {
		return GDK_SUCCEED;
	}
	assert(b->theap->parentid == b->batCacheid);

	TRC_DEBUG_IF(ALGO) {
		t0 = GDKusec();
		snprintf(buf, sizeof(buf), ALGOBATFMT, ALGOBATPAR(b));
	}

	ALIGNapp(b, force, GDK_FAIL);

	if (ATOMstorage(ATOMtype(b->ttype)) != ATOMstorage(ATOMtype(n->ttype))) {
		GDKerror("Incompatible operands.\n");
		return GDK_FAIL;
	}

	if (BATttype(b) != BATttype(n) &&
	    ATOMtype(b->ttype) != ATOMtype(n->ttype)) {
		TRC_DEBUG(CHECK_, "Interpreting %s as %s.\n",
			  ATOMname(BATttype(n)), ATOMname(BATttype(b)));
	}

	lng timeoffset = 0;
	QryCtx *qry_ctx = MT_thread_get_qry_ctx();
	if (qry_ctx != NULL) {
		timeoffset = (qry_ctx->starttime && qry_ctx->querytimeout) ? (qry_ctx->starttime + qry_ctx->querytimeout) : 0;
	}

	BATiter ni = bat_iterator(n);

	cnt = canditer_init(&ci, n, s);
	if (cnt == 0) {
		goto doreturn;
	}

	if (BUNlast(b) + cnt > BUN_MAX) {
		bat_iterator_end(&ni);
		GDKerror("combined BATs too large\n");
		return GDK_FAIL;
	}

	if (b->hseqbase + BATcount(b) + cnt >= GDK_oid_max) {
		bat_iterator_end(&ni);
		GDKerror("overflow of head value\n");
		return GDK_FAIL;
	}

	IMPSdestroy(b);		/* imprints do not support updates yet */
	OIDXdestroy(b);
	STRMPdestroy(b);	/* TODO: use STRMPappendBitString */

	MT_lock_set(&b->theaplock);

	if (BATcount(b) == 0 || b->tmaxpos != BUN_NONE) {
		if (ni.maxpos != BUN_NONE) {
			BATiter bi = bat_iterator_nolock(b);
			if (BATcount(b) == 0 || ATOMcmp(b->ttype, BUNtail(bi, b->tmaxpos), BUNtail(ni, ni.maxpos)) < 0) {
				if (s == NULL) {
					b->tmaxpos = BATcount(b) + ni.maxpos;
				} else {
					b->tmaxpos = BUN_NONE;
				}
			}
		} else {
			b->tmaxpos = BUN_NONE;
		}
	}
	if (BATcount(b) == 0 || b->tminpos != BUN_NONE) {
		if (ni.minpos != BUN_NONE) {
			BATiter bi = bat_iterator_nolock(b);
			if (BATcount(b) == 0 || ATOMcmp(b->ttype, BUNtail(bi, b->tminpos), BUNtail(ni, ni.minpos)) > 0) {
				if (s == NULL) {
					b->tminpos = BATcount(b) + ni.minpos;
				} else {
					b->tminpos = BUN_NONE;
				}
			}
		} else {
			b->tminpos = BUN_NONE;
		}
	}
	if (cnt > BATcount(b) / GDK_UNIQUE_ESTIMATE_KEEP_FRACTION) {
		b->tunique_est = 0;
	}
	MT_lock_unset(&b->theaplock);
	/* load hash so that we can maintain it */
	(void) BATcheckhash(b);

	if (b->ttype == TYPE_void) {
		/* b does not have storage, keep it that way if we can */
		HASHdestroy(b);	/* we're not maintaining the hash here */
		MT_lock_set(&b->theaplock);
		if (BATtdense(n) && ci.tpe == cand_dense &&
		    (BATcount(b) == 0 ||
		     (BATtdense(b) &&
		      b->tseqbase + BATcount(b) == n->tseqbase + ci.seq - hseq))) {
			/* n is also dense and consecutive with b */
			if (BATcount(b) == 0)
				BATtseqbase(b, n->tseqbase + ci.seq - hseq);
			BATsetcount(b, BATcount(b) + cnt);
			MT_lock_unset(&b->theaplock);
			goto doreturn;
		}
		if ((BATcount(b) == 0 || is_oid_nil(b->tseqbase)) &&
		    ni.type == TYPE_void && is_oid_nil(n->tseqbase)) {
			/* both b and n are void/nil */
			BATtseqbase(b, oid_nil);
			BATsetcount(b, BATcount(b) + cnt);
			MT_lock_unset(&b->theaplock);
			goto doreturn;
		}
		/* we need to materialize b; allocate enough capacity */
		b->batCapacity = BATcount(b) + cnt;
		MT_lock_unset(&b->theaplock);
		if (BATmaterialize(b) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
	}

	r = BUNlast(b);

	/* property setting */
	MT_lock_set(&b->theaplock);
	if (BATcount(b) == 0) {
		b->tsorted = n->tsorted;
		b->trevsorted = n->trevsorted;
		b->tseqbase = oid_nil;
		b->tnonil = n->tnonil;
		b->tnil = n->tnil && cnt == BATcount(n);
		if (ci.tpe == cand_dense) {
			b->tnosorted = ci.seq - hseq <= n->tnosorted && n->tnosorted < ci.seq + cnt - hseq ? n->tnosorted + hseq - ci.seq : 0;
			b->tnorevsorted = ci.seq - hseq <= n->tnorevsorted && n->tnorevsorted < ci.seq + cnt - hseq ? n->tnorevsorted + hseq - ci.seq : 0;
			if (BATtdense(n)) {
				b->tseqbase = n->tseqbase + ci.seq - hseq;
			}
		} else {
			b->tnosorted = 0;
			b->tnorevsorted = 0;
		}
		b->tkey = n->tkey;
		if (cnt == BATcount(n)) {
			b->tnokey[0] = n->tnokey[0];
			b->tnokey[1] = n->tnokey[1];
		} else {
			b->tnokey[0] = b->tnokey[1] = 0;
		}
	} else {
		BUN last = r - 1;
		BATiter bi = bat_iterator_nolock(b);
		int xx = ATOMcmp(b->ttype,
				 BUNtail(ni, ci.seq - hseq),
				 BUNtail(bi, last));
		if (BATtordered(b) && (!BATtordered(n) || xx < 0)) {
			b->tsorted = false;
			b->tnosorted = 0;
			b->tseqbase = oid_nil;
		}
		if (BATtrevordered(b) &&
		    (!BATtrevordered(n) || xx > 0)) {
			b->trevsorted = false;
			b->tnorevsorted = 0;
		}
		if (b->tkey &&
		    (!(BATtordered(b) || BATtrevordered(b)) ||
		     !n->tkey || xx == 0)) {
			BATkey(b, false);
		}
		if (b->ttype != TYPE_void && b->tsorted && BATtdense(b) &&
		    (!BATtdense(n) ||
		     ci.tpe != cand_dense ||
		     1 + *(oid *) BUNtloc(bi, last) != BUNtoid(n, ci.seq - hseq))) {
			b->tseqbase = oid_nil;
		}
		b->tnonil &= n->tnonil;
		b->tnil |= n->tnil && cnt == ni.count;
	}
	MT_lock_unset(&b->theaplock);
	if (b->ttype == TYPE_str) {
		if (insert_string_bat(b, n, &ci, force, mayshare, timeoffset) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
	} else if (ATOMvarsized(b->ttype)) {
		if (append_varsized_bat(b, n, &ci, mayshare) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
	} else if (ATOMstorage(b->ttype) == TYPE_msk) {
		if (append_msk_bat(b, n, &ci) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
	} else {
		if (cnt > BATcapacity(b) - BATcount(b)) {
			/* if needed space exceeds a normal growth
			 * extend just with what's needed */
			BUN ncap = BATcount(b) + cnt;
			BUN grows = BATgrows(b);

			if (ncap > grows)
				grows = ncap;
			if (BATextend(b, grows) != GDK_SUCCEED) {
				bat_iterator_end(&ni);
				return GDK_FAIL;
			}
		}
		MT_rwlock_wrlock(&b->thashlock);
		if (BATatoms[b->ttype].atomFix == NULL &&
		    b->ttype != TYPE_void &&
		    ni.type != TYPE_void &&
		    ci.tpe == cand_dense) {
			/* use fast memcpy if we can */
			memcpy(Tloc(b, BUNlast(b)),
			       (const char *) ni.base + ((ci.seq - hseq) << ni.shift),
			       cnt << ni.shift);
			for (BUN i = 0; b->thash && i < cnt; i++) {
				HASHappend_locked(b, r, Tloc(b, r));
				r++;
			}
		} else {
			TIMEOUT_LOOP(cnt, timeoffset) {
				BUN p = canditer_next(&ci) - hseq;
				const void *t = BUNtail(ni, p);
				if (tfastins_nocheck(b, r, t) != GDK_SUCCEED) {
					MT_rwlock_wrunlock(&b->thashlock);
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
				if (b->thash)
					HASHappend_locked(b, r, t);
				r++;
			}
			TIMEOUT_CHECK(timeoffset, GOTO_LABEL_TIMEOUT_HANDLER(bailout));
		}
		MT_rwlock_wrunlock(&b->thashlock);
		MT_lock_set(&b->theaplock);
		BATsetcount(b, b->batCount + ci.ncand);
		MT_lock_unset(&b->theaplock);
	}

  doreturn:
	bat_iterator_end(&ni);
	TRC_DEBUG(ALGO, "b=%s,n=" ALGOBATFMT ",s=" ALGOOPTBATFMT
		  " -> " ALGOBATFMT " (" LLFMT " usec)\n",
		  buf, ALGOBATPAR(n), ALGOOPTBATPAR(s), ALGOBATPAR(b),
		  GDKusec() - t0);

	return GDK_SUCCEED;
  bailout:
	MT_rwlock_wrunlock(&b->thashlock);
	bat_iterator_end(&ni);
	return GDK_FAIL;
}

gdk_return
BATappend(BAT *b, BAT *n, BAT *s, bool force)
{
	return BATappend2(b, n, s, force, true);
}

gdk_return
BATdel(BAT *b, BAT *d)
{
	gdk_return (*unfix) (const void *) = BATatoms[b->ttype].atomUnfix;
	void (*atmdel) (Heap *, var_t *) = BATatoms[b->ttype].atomDel;
	BATiter bi = bat_iterator_nolock(b);

	assert(ATOMtype(d->ttype) == TYPE_oid);
	assert(d->tsorted);
	assert(d->tkey);
	if (BATcount(d) == 0)
		return GDK_SUCCEED;
	IMPSdestroy(b);
	OIDXdestroy(b);
	HASHdestroy(b);
	PROPdestroy(b);
	STRMPdestroy(b);
	if (BATtdense(d)) {
		oid o = d->tseqbase;
		BUN c = BATcount(d);

		if (o + c <= b->hseqbase)
			return GDK_SUCCEED;
		if (o < b->hseqbase) {
			c -= b->hseqbase - o;
			o = b->hseqbase;
		}
		if (o - b->hseqbase < b->batInserted) {
			GDKerror("cannot delete committed values\n");
			return GDK_FAIL;
		}
		if (o + c > b->hseqbase + BATcount(b))
			c = b->hseqbase + BATcount(b) - o;
		if (c == 0)
			return GDK_SUCCEED;
		if (unfix || atmdel) {
			BUN p = o - b->hseqbase;
			BUN q = p + c;
			while (p < q) {
				if (unfix && (*unfix)(BUNtail(bi, p)) != GDK_SUCCEED)
					return GDK_FAIL;
				if (atmdel)
					(*atmdel)(b->tvheap, (var_t *) BUNtloc(bi, p));
				p++;
			}
		}
		if (BATtdense(b) && BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
		if (o + c < b->hseqbase + BATcount(b)) {
			o -= b->hseqbase;
			if (ATOMstorage(b->ttype) == TYPE_msk) {
				BUN n = BATcount(b) - (o + c);
				/* not very efficient, but first see
				 * how much this is used */
				for (BUN i = 0; i < n; i++)
					mskSetVal(b, o + i,
						  mskGetVal(b, o + c + i));
			} else {
				memmove(Tloc(b, o),
					Tloc(b, o + c),
					Tsize(b) * (BATcount(b) - (o + c)));
			}
			b->theap->dirty = true;
			// o += b->hseqbase; // if this were to be used again
		}
		MT_lock_set(&b->theaplock);
		b->batCount -= c;
		MT_lock_unset(&b->theaplock);
	} else {
		BATiter di = bat_iterator(d);
		const oid *o = (const oid *) di.base;
		const oid *s;
		BUN c = di.count;
		BUN nd = 0;
		BUN pos;
		char *p = NULL;

		if (o[c - 1] <= b->hseqbase) {
			bat_iterator_end(&di);
			return GDK_SUCCEED;
		}
		while (*o < b->hseqbase) {
			o++;
			c--;
		}
		if (*o - b->hseqbase < b->batInserted) {
			bat_iterator_end(&di);
			GDKerror("cannot delete committed values\n");
			return GDK_FAIL;
		}
		if (BATtdense(b) && BATmaterialize(b) != GDK_SUCCEED) {
			bat_iterator_end(&di);
			return GDK_FAIL;
		}
		s = o;
		pos = *o - b->hseqbase;
		if (ATOMstorage(b->ttype) != TYPE_msk)
			p = Tloc(b, pos);
		while (c > 0 && *o < b->hseqbase + BATcount(b)) {
			size_t n;
			if (unfix)
				(*unfix)(BUNtail(bi, *o - b->hseqbase));
			if (atmdel)
				(*atmdel)(b->tvheap, (var_t *) BUNtloc(bi, *o - b->hseqbase));
			o++;
			c--;
			nd++;
			if (c == 0 || *o - b->hseqbase >= BATcount(b))
				n = b->hseqbase + BATcount(b) - o[-1] - 1;
			else if ((oid) (o - s) < *o - *s)
				n = o[0] - o[-1] - 1;
			else
				n = 0;
			if (n > 0) {
				if (ATOMstorage(b->ttype) == TYPE_msk) {
					BUN opos = o[-1] + 1 - b->hseqbase;
					/* not very efficient, but
					 * first see how much this is
					 * used */
					for (BUN i = 0; i < n; i++) {
						mskSetVal(b, pos + i,
							  mskGetVal(b, opos + i));
					}
					pos += n;
				} else {
					n *= Tsize(b);
					memmove(p,
						Tloc(b, o[-1] + 1 - b->hseqbase),
						n);
					p += n;
				}
				s = o;
			}
		}
		bat_iterator_end(&di);
		MT_lock_set(&b->theaplock);
		b->theap->dirty = true;
		b->batCount -= nd;
		MT_lock_unset(&b->theaplock);
	}
	if (b->batCount <= 1) {
		/* some trivial properties */
		b->tkey = true;
		b->tsorted = b->trevsorted = true;
		if (b->batCount == 0) {
			b->tnil = false;
			b->tnonil = true;
		}
	}
	/* not sure about these anymore */
	MT_lock_set(&b->theaplock);
	b->tnosorted = b->tnorevsorted = 0;
	b->tnokey[0] = b->tnokey[1] = 0;
	b->tminpos = BUN_NONE;
	b->tmaxpos = BUN_NONE;
	b->tunique_est = 0.0;
	MT_lock_unset(&b->theaplock);

	return GDK_SUCCEED;
}

/*
 * Replace all values in b with values from n whose location is given by
 * the oid in either p or positions.
 * If positions is used, autoincr specifies whether it is the first of a
 * dense range of positions or whether it is a full-blown array of
 * position.
 * If mayappend is set, the position in p/positions may refer to
 * locations beyond the end of b.
 */
static gdk_return
BATappend_or_update(BAT *b, BAT *p, const oid *positions, BAT *n,
		    bool mayappend, bool autoincr, bool force)
{
	lng t0 = GDKusec();
	oid pos = oid_nil;

	if (b == NULL || b->ttype == TYPE_void || n == NULL) {
		return GDK_SUCCEED;
	}
	/* either p or positions */
	assert((p == NULL) != (positions == NULL));
	if (p != NULL) {
		if (BATcount(p) != BATcount(n)) {
			GDKerror("update BATs not the same size\n");
			return GDK_FAIL;
		}
		if (ATOMtype(p->ttype) != TYPE_oid) {
			GDKerror("positions BAT not type OID\n");
			return GDK_FAIL;
		}
		if (BATtdense(p)) {
			pos = p->tseqbase;
			positions = &pos;
			autoincr = true;
			p = NULL;
		} else if (p->ttype != TYPE_void) {
			positions = (const oid *) Tloc(p, 0);
			autoincr = false;
		} else {
			autoincr = false;
		}
	} else if (autoincr) {
		pos = *positions;
	}
	if (BATcount(n) == 0) {
		return GDK_SUCCEED;
	}
	if (!force && (b->batRestricted != BAT_WRITE || b->batSharecnt > 0)) {
		GDKerror("access denied to %s, aborting.\n", BATgetId(b));
		return GDK_FAIL;
	}

	OIDXdestroy(b);
	IMPSdestroy(b);
	STRMPdestroy(b);
	/* load hash so that we can maintain it */
	(void) BATcheckhash(b);
	BATiter ni = bat_iterator(n);
	MT_lock_set(&b->theaplock);
	if (ni.count > BATcount(b) / GDK_UNIQUE_ESTIMATE_KEEP_FRACTION) {
		b->tunique_est = 0;
	}

	BATiter bi = bat_iterator_nolock(b);

	b->tsorted = b->trevsorted = false;
	b->tnosorted = b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = b->tnokey[1] = 0;

	int (*atomcmp)(const void *, const void *) = ATOMcompare(b->ttype);
	const void *nil = ATOMnilptr(b->ttype);
	oid hseqend = b->hseqbase + BATcount(b);

	MT_lock_unset(&b->theaplock);

	bool anynil = false;
	bool locked = false;

	if (b->tvarsized) {
		for (BUN i = 0; i < ni.count; i++) {
			oid updid;
			if (positions) {
				updid = autoincr ? pos++ : *positions++;
			} else {
				updid = BUNtoid(p, i);
			}

			if (updid < b->hseqbase ||
			    (!mayappend && updid >= hseqend)) {
				GDKerror("id out of range\n");
				goto bailout;
			}
			updid -= b->hseqbase;
			if (!force && updid < b->batInserted) {
				GDKerror("updating committed value\n");
				goto bailout;
			}

			const void *new = BUNtvar(ni, i);

			if (updid >= BATcount(b)) {
				assert(mayappend);
				if (locked) {
					MT_rwlock_wrunlock(&b->thashlock);
					locked = false;
				}
				if (b->tminpos != bi.minpos ||
				    b->tmaxpos != bi.maxpos) {
					MT_lock_set(&b->theaplock);
					b->tminpos = bi.minpos;
					b->tmaxpos = bi.maxpos;
					MT_lock_unset(&b->theaplock);
				}
				if (BATcount(b) < updid &&
				    BUNappendmulti(b, NULL, (BUN) (updid - BATcount(b)), force) != GDK_SUCCEED) {
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
				if (BUNappend(b, new, force) != GDK_SUCCEED) {
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
				bi = bat_iterator_nolock(b);
				continue;
			}

			const void *old = BUNtvar(bi, updid);

			if (atomcmp(old, new) == 0) {
				/* replacing with the same value:
				 * nothing to do */
				continue;
			}

			bool isnil = atomcmp(new, nil) == 0;
			anynil |= isnil;
			if (b->tnil &&
			    !anynil &&
			    atomcmp(old, nil) == 0) {
				/* if old value is nil and no new
				 * value is, we're not sure anymore
				 * about the nil property, so we must
				 * clear it */
				b->tnil = false;
			}
			b->tnonil &= !isnil;
			b->tnil |= isnil;
			if (bi.maxpos != BUN_NONE) {
				if (!isnil &&
				    atomcmp(BUNtvar(bi, bi.maxpos), new) < 0) {
					/* new value is larger than
					 * previous largest */
					bi.maxpos = updid;
				} else if (atomcmp(BUNtvar(bi, bi.maxpos), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * largest and new value is
					 * smaller, so we don't know
					 * anymore which is the
					 * largest */
					bi.maxpos = BUN_NONE;
				}
			}
			if (bi.minpos != BUN_NONE) {
				if (!isnil &&
				    atomcmp(BUNtvar(bi, bi.minpos), new) > 0) {
					/* new value is smaller than
					 * previous smallest */
					bi.minpos = updid;
				} else if (atomcmp(BUNtvar(bi, bi.minpos), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * smallest and new value is
					 * larger, so we don't know
					 * anymore which is the
					 * smallest */
					bi.minpos = BUN_NONE;
				}
			}
			if (!locked) {
				MT_rwlock_wrlock(&b->thashlock);
				locked = true;
			}
			HASHdelete_locked(b, updid, old);

			var_t d;
			switch (b->twidth) {
			default: /* only three or four cases possible */
				d = (var_t) ((uint8_t *) b->theap->base)[updid] + GDK_VAROFFSET;
				break;
			case 2:
				d = (var_t) ((uint16_t *) b->theap->base)[updid] + GDK_VAROFFSET;
				break;
			case 4:
				d = (var_t) ((uint32_t *) b->theap->base)[updid];
				break;
#if SIZEOF_VAR_T == 8
			case 8:
				d = (var_t) ((uint64_t *) b->theap->base)[updid];
				break;
#endif
			}
			if (ATOMreplaceVAR(b, &d, new) != GDK_SUCCEED) {
				goto bailout;
			}
			if (b->twidth < SIZEOF_VAR_T &&
			    (b->twidth <= 2 ? d - GDK_VAROFFSET : d) >= ((size_t) 1 << (8 << b->tshift))) {
				/* doesn't fit in current heap, upgrade
				 * it, can't keep hashlock while doing
				 * so */
				MT_rwlock_wrunlock(&b->thashlock);
				locked = false;
				if (GDKupgradevarheap(b, d, 0, MAX(updid, b->batCount)) != GDK_SUCCEED) {
					goto bailout;
				}
				MT_rwlock_wrlock(&b->thashlock);
				locked = true;
			}
			/* in case ATOMreplaceVAR and/or
			 * GDKupgradevarheap replaces a heap, we need to
			 * reinitialize the iterator */
			{
				/* save and restore minpos/maxpos */
				BUN minpos = bi.minpos;
				BUN maxpos = bi.maxpos;
				bi = bat_iterator_nolock(b);
				bi.minpos = minpos;
				bi.maxpos = maxpos;
			}
			switch (b->twidth) {
			case 1:
				((uint8_t *) b->theap->base)[updid] = (uint8_t) (d - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) b->theap->base)[updid] = (uint16_t) (d - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) b->theap->base)[updid] = (uint32_t) d;
				break;
#if SIZEOF_VAR_T == 8
			case 8:
				((uint64_t *) b->theap->base)[updid] = (uint64_t) d;
				break;
#endif
			}
			HASHinsert_locked(b, updid, new);

		}
		if (locked) {
			MT_rwlock_wrunlock(&b->thashlock);
			locked = false;
		}
		MT_lock_set(&b->theaplock);
		b->tvheap->dirty = true;
		MT_lock_unset(&b->theaplock);
	} else if (ATOMstorage(b->ttype) == TYPE_msk) {
		HASHdestroy(b);	/* hash doesn't make sense for msk */
		for (BUN i = 0; i < ni.count; i++) {
			oid updid;
			if (positions) {
				updid = autoincr ? pos++ : *positions++;
			} else {
				updid = BUNtoid(p, i);
			}

			if (updid < b->hseqbase ||
			    (!mayappend && updid >= hseqend)) {
				GDKerror("id out of range\n");
				bat_iterator_end(&ni);
				return GDK_FAIL;
			}
			updid -= b->hseqbase;
			if (!force && updid < b->batInserted) {
				GDKerror("updating committed value\n");
				bat_iterator_end(&ni);
				return GDK_FAIL;
			}
			if (updid >= BATcount(b)) {
				assert(mayappend);
				if (BATcount(b) < updid &&
				    BUNappendmulti(b, NULL, (BUN) (updid - BATcount(b)), force) != GDK_SUCCEED) {
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
				if (BUNappend(b, BUNtmsk(ni, i), force) != GDK_SUCCEED) {
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
				continue;
			}
			mskSetVal(b, updid, Tmskval(&ni, i));
		}
	} else if (autoincr) {
		if (pos < b->hseqbase ||
		    (!mayappend && pos + ni.count > hseqend)) {
			GDKerror("id out of range\n");
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}
		pos -= b->hseqbase;
		if (!force && pos < b->batInserted) {
			GDKerror("updating committed value\n");
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}

		if (pos >= BATcount(b)) {
			assert(mayappend);
			bat_iterator_end(&ni);
			if (BATcount(b) < pos &&
			    BUNappendmulti(b, NULL, (BUN) (pos - BATcount(b)), force) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
			return BATappend(b, n, NULL, force);
		}
		if (pos + ni.count > BATcount(b) &&
		    BUNappendmulti(b, NULL, (BUN) (pos + ni.count - BATcount(b)), force) != GDK_SUCCEED) {
			bat_iterator_end(&ni);
			return GDK_FAIL;
		}

		/* we copy all of n, so if there are nils in n we get
		 * nils in b (and else we don't know) */
		b->tnil = n->tnil;
		/* we may not copy over all of b, so we only know that
		 * there are no nils in b afterward if there weren't
		 * any in either b or n to begin with */
		b->tnonil &= n->tnonil;
		/* if there is no hash, we don't start the loop, if
		 * there is only a persisted hash, it will get destroyed
		 * in the first iteration, after which there is no hash
		 * and the loop ends */
		MT_rwlock_wrlock(&b->thashlock);
		locked = true;
		for (BUN i = pos, j = pos + ni.count; i < j && b->thash; i++)
			HASHdelete_locked(b, i, Tloc(b, i));
		if (n->ttype == TYPE_void) {
			assert(b->ttype == TYPE_oid);
			oid *o = Tloc(b, pos);
			if (is_oid_nil(ni.tseq)) {
				/* we may or may not overwrite the old
				 * min/max values */
				bi.minpos = BUN_NONE;
				bi.maxpos = BUN_NONE;
				for (BUN i = 0, j = ni.count; i < j; i++)
					o[i] = oid_nil;
				b->tnil = true;
			} else {
				oid v = ni.tseq;
				/* we know min/max of n, so we know
				 * the new min/max of b if those of n
				 * are smaller/larger than the old */
				if (bi.minpos != BUN_NONE) {
					if (v <= BUNtoid(b, bi.minpos))
						bi.minpos = pos;
					else if (pos <= bi.minpos && bi.minpos < pos + ni.count)
						bi.minpos = BUN_NONE;
				}
				if (complex_cand(n)) {
					for (BUN i = 0, j = ni.count; i < j; i++)
						o[i] = *(oid *)Tpos(&ni, i);
					/* last value */
					v = o[ni.count - 1];
				} else {
					for (BUN i = 0, j = ni.count; i < j; i++)
						o[i] = v++;
					/* last value added (not one beyond) */
					v--;
				}
				if (bi.maxpos != BUN_NONE) {
					if (v >= BUNtoid(b, bi.maxpos))
						bi.maxpos = pos + ni.count - 1;
					else if (pos <= bi.maxpos && bi.maxpos < pos + ni.count)
						bi.maxpos = BUN_NONE;
				}
			}
		} else {
			/* if the extremes of n are at least as
			 * extreme as those of b, we can replace b's
			 * min/max, else we don't know what b's new
			 * min/max are*/
			if (bi.minpos != BUN_NONE && n->tminpos != BUN_NONE &&
			    atomcmp(BUNtloc(bi, bi.minpos), BUNtail(ni, n->tminpos)) >= 0) {
				bi.minpos = pos + n->tminpos;
			} else {
				bi.minpos = BUN_NONE;
			}
			if (bi.maxpos != BUN_NONE && n->tmaxpos != BUN_NONE &&
			    atomcmp(BUNtloc(bi, bi.maxpos), BUNtail(ni, n->tmaxpos)) <= 0) {
				bi.maxpos = pos + n->tmaxpos;
			} else {
				bi.maxpos = BUN_NONE;
			}
			memcpy(Tloc(b, pos), ni.base,
			       ni.count << b->tshift);
		}
		/* either we have a hash that was updated above, or we
		 * have no hash; we cannot have the case where there is
		 * only a persisted (unloaded) hash since it would have
		 * been destroyed above */
		if (b->thash != NULL) {
			for (BUN i = pos, j = pos + ni.count; i < j; i++)
				HASHinsert_locked(b, i, Tloc(b, i));
		}
		MT_rwlock_wrunlock(&b->thashlock);
		locked = false;
		if (ni.count == BATcount(b)) {
			/* if we replaced all values of b by values
			 * from n, we can also copy the min/max
			 * properties */
			bi.minpos = n->tminpos;
			bi.maxpos = n->tmaxpos;
			if (BATtdense(n)) {
				/* replaced all of b with a dense sequence */
				BATtseqbase(b, ni.tseq);
			}
		}
	} else {
		for (BUN i = 0; i < ni.count; i++) {
			oid updid;
			if (positions) {
				/* assert(!autoincr) */
				updid = *positions++;
			} else {
				updid = BUNtoid(p, i);
			}

			if (updid < b->hseqbase ||
			    (!mayappend && updid >= hseqend)) {
				GDKerror("id out of range\n");
				goto bailout;
			}
			updid -= b->hseqbase;
			if (!force && updid < b->batInserted) {
				GDKerror("updating committed value\n");
				goto bailout;
			}

			const void *new = BUNtloc(ni, i);

			if (updid >= BATcount(b)) {
				assert(mayappend);
				if (locked) {
					MT_rwlock_wrunlock(&b->thashlock);
					locked = false;
				}
				if (b->tminpos != bi.minpos ||
				    b->tmaxpos != bi.maxpos) {
					MT_lock_set(&b->theaplock);
					b->tminpos = bi.minpos;
					b->tmaxpos = bi.maxpos;
					MT_lock_unset(&b->theaplock);
				}
				if (BATcount(b) < updid &&
				    BUNappendmulti(b, NULL, (BUN) (updid - BATcount(b)), force) != GDK_SUCCEED) {
					goto bailout;
				}
				if (BUNappend(b, new, force) != GDK_SUCCEED) {
					bat_iterator_end(&ni);
					return GDK_FAIL;
				}
				bi = bat_iterator_nolock(b);
				continue;
			}

			const void *old = BUNtloc(bi, updid);
			bool isnil = atomcmp(new, nil) == 0;
			anynil |= isnil;
			if (b->tnil &&
			    !anynil &&
			    atomcmp(old, nil) == 0) {
				/* if old value is nil and no new
				 * value is, we're not sure anymore
				 * about the nil property, so we must
				 * clear it */
				b->tnil = false;
			}
			b->tnonil &= !isnil;
			b->tnil |= isnil;
			if (bi.maxpos != BUN_NONE) {
				if (!isnil &&
				    atomcmp(BUNtloc(bi, bi.maxpos), new) < 0) {
					/* new value is larger than
					 * previous largest */
					bi.maxpos = updid;
				} else if (atomcmp(BUNtloc(bi, bi.maxpos), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * largest and new value is
					 * smaller, so we don't know
					 * anymore which is the
					 * largest */
					bi.maxpos = BUN_NONE;
				}
			}
			if (bi.minpos != BUN_NONE) {
				if (!isnil &&
				    atomcmp(BUNtloc(bi, bi.minpos), new) > 0) {
					/* new value is smaller than
					 * previous smallest */
					bi.minpos = updid;
				} else if (atomcmp(BUNtloc(bi, bi.minpos), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * smallest and new value is
					 * larger, so we don't know
					 * anymore which is the
					 * smallest */
					bi.minpos = BUN_NONE;
				}
			}

			if (!locked) {
				MT_rwlock_wrlock(&b->thashlock);
				locked = true;
			}
			HASHdelete_locked(b, updid, old);
			switch (b->twidth) {
			case 1:
				((bte *) b->theap->base)[updid] = * (bte *) new;
				break;
			case 2:
				((sht *) b->theap->base)[updid] = * (sht *) new;
				break;
			case 4:
				((int *) b->theap->base)[updid] = * (int *) new;
				break;
			case 8:
				((lng *) b->theap->base)[updid] = * (lng *) new;
				break;
			case 16:
#ifdef HAVE_HGE
				((hge *) b->theap->base)[updid] = * (hge *) new;
#else
				((uuid *) b->theap->base)[updid] = * (uuid *) new;
#endif
				break;
			default:
				memcpy(BUNtloc(bi, updid), new, ATOMsize(b->ttype));
				break;
			}
			HASHinsert_locked(b, updid, new);
		}
		if (locked) {
			MT_rwlock_wrunlock(&b->thashlock);
			locked = false;
		}
	}
	bat_iterator_end(&ni);
	MT_lock_set(&b->theaplock);
	b->tminpos = bi.minpos;
	b->tmaxpos = bi.maxpos;
	b->theap->dirty = true;
	MT_lock_unset(&b->theaplock);
	TRC_DEBUG(ALGO,
		  "BATreplace(" ALGOBATFMT "," ALGOOPTBATFMT "," ALGOBATFMT ") " LLFMT " usec\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(p), ALGOBATPAR(n),
		  GDKusec() - t0);
	return GDK_SUCCEED;

  bailout:
	bat_iterator_end(&ni);
	if (locked) {
		Hash *h = b->thash;
		b->thash = NULL;
		MT_rwlock_wrunlock(&b->thashlock);
		doHASHdestroy(b, h);
	}
	return GDK_FAIL;
}

/* replace values from b at locations specified in p with values in n */
gdk_return
BATreplace(BAT *b, BAT *p, BAT *n, bool force)
{
	return BATappend_or_update(b, p, NULL, n, false, false, force);
}

/* like BATreplace, but p may specify locations beyond the end of b */
gdk_return
BATupdate(BAT *b, BAT *p, BAT *n, bool force)
{
	return BATappend_or_update(b, p, NULL, n, true, false, force);
}

/* like BATreplace, but the positions are given by an array of oid values */
gdk_return
BATreplacepos(BAT *b, const oid *positions, BAT *n, bool autoincr, bool force)
{
	return BATappend_or_update(b, NULL, positions, n, false, autoincr, force);
}

/* like BATreplace, but the positions are given by an array of oid
 * values, and they may specify locations beyond the end of b */
gdk_return
BATupdatepos(BAT *b, const oid *positions, BAT *n, bool autoincr, bool force)
{
	return BATappend_or_update(b, NULL, positions, n, true, autoincr, force);
}

/*
 *  BAT Selections
 * The BAT selectors are among the most heavily used operators.
 * Their efficient implementation is therefore mandatory.
 *
 * BAT slice
 * This function returns a horizontal slice from a BAT. It optimizes
 * execution by avoiding to copy when the BAT is memory mapped (in
 * this case, an independent submap is created) or else when it is
 * read-only, then a VIEW bat is created as a result.
 *
 * If a new copy has to be created, this function takes care to
 * preserve void-columns (in this case, the seqbase has to be
 * recomputed in the result).
 *
 * The selected range is excluding the high value.
 */
BAT *
BATslice(BAT *b, BUN l, BUN h)
{
	BUN low = l;
	BAT *bn = NULL;
	BATiter bni;
	oid foid;		/* first oid value if oid column */

	BATcheck(b, NULL);
	if (h > BATcount(b))
		h = BATcount(b);
	if (h < l)
		h = l;

	if (l > BUN_MAX || h > BUN_MAX) {
		GDKerror("boundary out of range\n");
		goto doreturn;
	}

	if (complex_cand(b)) {
		/* slicing a candidate list with exceptions */
		struct canditer ci;
		canditer_init(&ci, NULL, b);
		if (b->hseqbase + l >= ci.hseq) {
			l = b->hseqbase + l - ci.hseq;
			h = b->hseqbase + h - ci.hseq;
		} else {
			l = 0;
			if (b->hseqbase + h >= ci.hseq)
				h = b->hseqbase + h - ci.hseq;
			else
				h = 0;
		}
		bn = canditer_slice(&ci, l, h);
		goto doreturn;
	}
	/* If the source BAT is readonly, then we can obtain a VIEW
	 * that just reuses the memory of the source. */
	if (ATOMstorage(b->ttype) == TYPE_msk) {
		/* forget about slices for bit masks: we can't deal
		 * with difference in alignment, so we'll just make a
		 * copy */
		bn = COLnew((oid) (b->hseqbase + low), b->ttype, h - l, TRANSIENT);
		/* we use BATappend with a candidate list to easily
		 * copy the part of b that we need */
		BAT *s = BATdense(0, (oid) (b->hseqbase + low), h - l);
		if (bn == NULL ||
		    s == NULL ||
		    BATappend(bn, b, s, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			BBPreclaim(s);
			bn = NULL;
			goto doreturn;
		}
		BBPunfix(s->batCacheid);
		goto doreturn;
	} else if (b->batRestricted == BAT_READ &&
	    (!VIEWtparent(b) ||
	     BBP_cache(VIEWtparent(b))->batRestricted == BAT_READ)) {
		bn = VIEWcreate(b->hseqbase + low, b);
		if (bn == NULL)
			goto doreturn;
		VIEWbounds(b, bn, l, h);
	} else {
		/* create a new BAT and put everything into it */
		BUN p = l;
		BUN q = h;

		bn = COLnew((oid) (b->hseqbase + low), BATtdense(b) ? TYPE_void : b->ttype, h - l, TRANSIENT);
		if (bn == NULL)
			goto doreturn;

		if (bn->ttype == TYPE_void ||
		    (!bn->tvarsized &&
		     BATatoms[bn->ttype].atomPut == NULL &&
		     BATatoms[bn->ttype].atomFix == NULL)) {
			if (bn->ttype) {
				BATiter bi = bat_iterator(b);
				memcpy(Tloc(bn, 0), (const char *) bi.base + (p << bi.shift),
				       (q - p) << bn->tshift);
				bat_iterator_end(&bi);
				bn->theap->dirty = true;
			}
			BATsetcount(bn, h - l);
		} else {
			BATiter bi = bat_iterator(b);
			for (; p < q; p++) {
				if (bunfastapp(bn, BUNtail(bi, p)) != GDK_SUCCEED) {
					bat_iterator_end(&bi);
					BBPreclaim(bn);
					bn = NULL;
					goto doreturn;
				}
			}
			bat_iterator_end(&bi);
		}
		bn->theap->dirty = true;
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		bn->tkey = b->tkey;
		bn->tnonil = b->tnonil;
		if (b->tnosorted > l && b->tnosorted < h)
			bn->tnosorted = b->tnosorted - l;
		else
			bn->tnosorted = 0;
		if (b->tnorevsorted > l && b->tnorevsorted < h)
			bn->tnorevsorted = b->tnorevsorted - l;
		else
			bn->tnorevsorted = 0;
		if (b->tnokey[0] >= l && b->tnokey[0] < h &&
		    b->tnokey[1] >= l && b->tnokey[1] < h &&
		    b->tnokey[0] != b->tnokey[1]) {
			bn->tnokey[0] = b->tnokey[0] - l;
			bn->tnokey[1] = b->tnokey[1] - l;
		} else {
			bn->tnokey[0] = bn->tnokey[1] = 0;
		}
	}
	bn->tnonil = b->tnonil || bn->batCount == 0;
	bn->tnil = false;	/* we just don't know */
	bn->tnosorted = 0;
	bn->tnokey[0] = bn->tnokey[1] = 0;
	bni = bat_iterator_nolock(bn);
	if (BATtdense(b)) {
		BATtseqbase(bn, (oid) (b->tseqbase + low));
	} else if (bn->ttype == TYPE_oid) {
		if (BATcount(bn) == 0) {
			BATtseqbase(bn, 0);
		} else if (!is_oid_nil((foid = *(oid *) BUNtloc(bni, 0))) &&
			   (BATcount(bn) == 1 ||
			    (bn->tkey &&
			     bn->tsorted &&
			     foid + BATcount(bn) - 1 == *(oid *) BUNtloc(bni, BUNlast(bn) - 1)))) {
			BATtseqbase(bn, foid);
		}
	}
	if (bn->batCount <= 1) {
		bn->tsorted = ATOMlinear(b->ttype);
		bn->trevsorted = ATOMlinear(b->ttype);
		BATkey(bn, true);
	} else {
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		BATkey(bn, BATtkey(b));
	}
  doreturn:
	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",lo=" BUNFMT ",hi=" BUNFMT " -> "
		  ALGOOPTBATFMT "\n",
		  ALGOBATPAR(b), l, h, ALGOOPTBATPAR(bn));
	return bn;
}

#define BAT_ORDERED(TPE)						\
	do {								\
		const TPE *restrict vals = Tloc(b, 0);			\
		for (BUN q = BUNlast(b), p = 1; p < q; p++) {		\
			if (vals[p - 1] > vals[p]) {			\
				MT_lock_set(&b->theaplock);		\
				b->tnosorted = p;			\
				MT_lock_unset(&b->theaplock);		\
				TRC_DEBUG(ALGO, "Fixed nosorted(" BUNFMT ") for " ALGOBATFMT " (" LLFMT " usec)\n", p, ALGOBATPAR(b), GDKusec() - t0); \
				goto doreturn;				\
			} else if (vals[p - 1] < vals[p]) {		\
				MT_lock_set(&b->theaplock);		\
				if (!b->trevsorted && b->tnorevsorted == 0) { \
					b->tnorevsorted = p;		\
					TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for " ALGOBATFMT "\n", p, ALGOBATPAR(b)); \
				}					\
				MT_lock_unset(&b->theaplock);		\
			} else if (!b->tkey && b->tnokey[1] == 0) {	\
				MT_lock_set(&b->theaplock);		\
				b->tnokey[0] = p - 1;			\
				b->tnokey[1] = p;			\
				MT_lock_unset(&b->theaplock);		\
				TRC_DEBUG(ALGO, "Fixed nokey(" BUNFMT "," BUNFMT") for " ALGOBATFMT "\n", p - 1, p, ALGOBATPAR(b)); \
			}						\
		}							\
	} while (0)

#define BAT_ORDERED_FP(TPE)						\
	do {								\
		const TPE *restrict vals = Tloc(b, 0);			\
		TPE prev = vals[0];					\
		bool prevnil = is_##TPE##_nil(prev);			\
		for (BUN q = BUNlast(b), p = 1; p < q; p++) {		\
			TPE next = vals[p];				\
			int cmp = prevnil ? -!(prevnil = is_##TPE##_nil(next)) : (prevnil = is_##TPE##_nil(next)) ? 1 : (prev > next) - (prev < next); \
			prev = next;					\
			if (cmp > 0) {					\
				MT_lock_set(&b->theaplock);		\
				b->tnosorted = p;			\
				MT_lock_unset(&b->theaplock);		\
				TRC_DEBUG(ALGO, "Fixed nosorted(" BUNFMT ") for " ALGOBATFMT " (" LLFMT " usec)\n", p, ALGOBATPAR(b), GDKusec() - t0); \
				goto doreturn;				\
			} else if (cmp < 0) {				\
				MT_lock_set(&b->theaplock);		\
				if (!b->trevsorted && b->tnorevsorted == 0) { \
					b->tnorevsorted = p;		\
					TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for " ALGOBATFMT "\n", p, ALGOBATPAR(b)); \
				}					\
				MT_lock_unset(&b->theaplock);		\
			} else if (!b->tkey && b->tnokey[1] == 0) {	\
				MT_lock_set(&b->theaplock);		\
				b->tnokey[0] = p - 1;			\
				b->tnokey[1] = p;			\
				MT_lock_unset(&b->theaplock);		\
				TRC_DEBUG(ALGO, "Fixed nokey(" BUNFMT "," BUNFMT") for " ALGOBATFMT "\n", p - 1, p, ALGOBATPAR(b)); \
			}						\
		}							\
	} while (0)

/* Return whether the BAT is ordered or not.  If we don't know, invest
 * in a scan and record the results in the bat descriptor.  If during
 * the scan we happen to find evidence that the BAT is not reverse
 * sorted, we record the location.  */
bool
BATordered(BAT *b)
{
	lng t0 = GDKusec();

	if (b->ttype == TYPE_void || b->tsorted || BATcount(b) == 0)
		return true;
	if (b->tnosorted > 0 || !ATOMlinear(b->ttype))
		return false;

	/* In order that multiple threads don't scan the same BAT at the
	 * same time (happens a lot with mitosis/mergetable), we use a
	 * lock.  We reuse the batIdxLock lock for this, not because this
	 * scanning interferes with heap reference counting, but because
	 * it's there, and not so likely to be used at the same time. */
	MT_lock_set(&b->batIdxLock);
	BATiter bi = bat_iterator_nolock(b);
	if (!b->tsorted && b->tnosorted == 0) {
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte:
			BAT_ORDERED(bte);
			break;
		case TYPE_sht:
			BAT_ORDERED(sht);
			break;
		case TYPE_int:
			BAT_ORDERED(int);
			break;
		case TYPE_lng:
			BAT_ORDERED(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BAT_ORDERED(hge);
			break;
#endif
		case TYPE_flt:
			BAT_ORDERED_FP(flt);
			break;
		case TYPE_dbl:
			BAT_ORDERED_FP(dbl);
			break;
		case TYPE_str:
			for (BUN q = BUNlast(b), p = 1; p < q; p++) {
				int c;
				const char *p1 = BUNtail(bi, p - 1);
				const char *p2 = BUNtail(bi, p);
				if (p1 == p2)
					c = 0;
				else if (p1[0] == '\200') {
					if (p2[0] == '\200')
						c = 0;
					else
						c = -1;
				} else if (p2[0] == '\200')
					c = 1;
				else
					c = strcmp(p1, p2);
				if (c > 0) {
					MT_lock_set(&b->theaplock);
					b->tnosorted = p;
					MT_lock_unset(&b->theaplock);
					TRC_DEBUG(ALGO, "Fixed nosorted(" BUNFMT ") for " ALGOBATFMT " (" LLFMT " usec)\n", p, ALGOBATPAR(b), GDKusec() - t0);
					goto doreturn;
				} else if (c < 0) {
					MT_lock_set(&b->theaplock);
					assert(!b->trevsorted);
					if (b->tnorevsorted == 0) {
						b->tnorevsorted = p;
						TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for " ALGOBATFMT "\n", p, ALGOBATPAR(b));
					}
					MT_lock_unset(&b->theaplock);
				} else if (b->tnokey[1] == 0) {
					MT_lock_set(&b->theaplock);
					assert(!b->tkey);
					b->tnokey[0] = p - 1;
					b->tnokey[1] = p;
					MT_lock_unset(&b->theaplock);
					TRC_DEBUG(ALGO, "Fixed nokey(" BUNFMT "," BUNFMT") for " ALGOBATFMT "\n", p - 1, p, ALGOBATPAR(b));
				}
			}
			break;
		default: {
			int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
			for (BUN q = BUNlast(b), p = 1; p < q; p++) {
				int c;
				if ((c = cmpf(BUNtail(bi, p - 1), BUNtail(bi, p))) > 0) {
					MT_lock_set(&b->theaplock);
					b->tnosorted = p;
					MT_lock_unset(&b->theaplock);
					TRC_DEBUG(ALGO, "Fixed nosorted(" BUNFMT ") for " ALGOBATFMT " (" LLFMT " usec)\n", p, ALGOBATPAR(b), GDKusec() - t0);
					goto doreturn;
				} else if (c < 0) {
					MT_lock_set(&b->theaplock);
					if (!b->trevsorted && b->tnorevsorted == 0) {
						b->tnorevsorted = p;
						TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for " ALGOBATFMT "\n", p, ALGOBATPAR(b));
					}
					MT_lock_unset(&b->theaplock);
				} else if (!b->tkey && b->tnokey[1] == 0) {
					MT_lock_set(&b->theaplock);
					b->tnokey[0] = p - 1;
					b->tnokey[1] = p;
					MT_lock_unset(&b->theaplock);
					TRC_DEBUG(ALGO, "Fixed nokey(" BUNFMT "," BUNFMT") for " ALGOBATFMT "\n", p - 1, p, ALGOBATPAR(b));
				}
			}
			break;
		}
		}
		/* we only get here if we completed the scan; note that
		 * if we didn't record evidence about *reverse*
		 * sortedness, we know that the BAT is also reverse
		 * sorted; similarly, if we didn't record evidence about
		 * keyness, we know the BAT is key */
		MT_lock_set(&b->theaplock);
		b->tsorted = true;
		TRC_DEBUG(ALGO, "Fixed sorted for " ALGOBATFMT " (" LLFMT " usec)\n", ALGOBATPAR(b), GDKusec() - t0);
		if (!b->trevsorted && b->tnorevsorted == 0) {
			b->trevsorted = true;
			TRC_DEBUG(ALGO, "Fixed revsorted for " ALGOBATFMT "\n", ALGOBATPAR(b));
		}
		if (!b->tkey && b->tnokey[1] == 0) {
			b->tkey = true;
			TRC_DEBUG(ALGO, "Fixed key for " ALGOBATFMT "\n", ALGOBATPAR(b));
		}
		MT_lock_unset(&b->theaplock);
	}
  doreturn:
	MT_lock_unset(&b->batIdxLock);
	return b->tsorted;
}

#define BAT_REVORDERED(TPE)						\
	do {								\
		const TPE *restrict vals = Tloc(b, 0);			\
		for (BUN q = BUNlast(b), p = 1; p < q; p++) {		\
			if (vals[p - 1] < vals[p]) {			\
				b->tnorevsorted = p;			\
				TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for " ALGOBATFMT " (" LLFMT " usec)\n", p, ALGOBATPAR(b), GDKusec() - t0); \
				goto doreturn;				\
			}						\
		}							\
	} while (0)

#define BAT_REVORDERED_FP(TPE)						\
	do {								\
		const TPE *restrict vals = Tloc(b, 0);			\
		for (BUN q = BUNlast(b), p = 1; p < q; p++) {		\
			TPE prev = vals[p - 1], next = vals[p];		\
			int cmp = is_flt_nil(prev) ? -!is_flt_nil(next) : is_flt_nil(next) ? 1 : (prev > next) - (prev < next);	\
			if (cmp < 0) {					\
				b->tnorevsorted = p;			\
				TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for " ALGOBATFMT " (" LLFMT " usec)\n", p, ALGOBATPAR(b), GDKusec() - t0); \
				goto doreturn;				\
			}						\
		}							\
	} while (0)

/* Return whether the BAT is reverse ordered or not.  If we don't
 * know, invest in a scan and record the results in the bat
 * descriptor.  */
bool
BATordered_rev(BAT *b)
{
	lng t0 = GDKusec();

	if (b == NULL || !ATOMlinear(b->ttype))
		return false;
	if (BATcount(b) <= 1 || b->trevsorted)
		return true;
	if (b->ttype == TYPE_void)
		return is_oid_nil(b->tseqbase);
	if (BATtdense(b) || b->tnorevsorted > 0)
		return false;
	MT_lock_set(&b->batIdxLock);
	BATiter bi = bat_iterator_nolock(b);
	if (!b->trevsorted && b->tnorevsorted == 0) {
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_bte:
			BAT_REVORDERED(bte);
			break;
		case TYPE_sht:
			BAT_REVORDERED(sht);
			break;
		case TYPE_int:
			BAT_REVORDERED(int);
			break;
		case TYPE_lng:
			BAT_REVORDERED(lng);
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			BAT_REVORDERED(hge);
			break;
#endif
		case TYPE_flt:
			BAT_REVORDERED_FP(flt);
			break;
		case TYPE_dbl:
			BAT_REVORDERED_FP(dbl);
			break;
		default: {
			int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
			for (BUN q = BUNlast(b), p = 1; p < q; p++) {
				if (cmpf(BUNtail(bi, p - 1), BUNtail(bi, p)) < 0) {
					b->tnorevsorted = p;
					TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for " ALGOBATFMT " (" LLFMT " usec)\n", p, ALGOBATPAR(b), GDKusec() - t0);
					goto doreturn;
				}
			}
			break;
		}
		}
		b->trevsorted = true;
		TRC_DEBUG(ALGO, "Fixed revsorted for " ALGOBATFMT " (" LLFMT " usec)\n", ALGOBATPAR(b), GDKusec() - t0);
	}
  doreturn:
	MT_lock_unset(&b->batIdxLock);
	return b->trevsorted;
}

/* figure out which sort function is to be called
 * stable sort can produce an error (not enough memory available),
 * "quick" sort does not produce errors */
static gdk_return
do_sort(void *restrict h, void *restrict t, const void *restrict base,
	size_t n, int hs, int ts, int tpe, bool reverse, bool nilslast,
	bool stable)
{
	if (n <= 1)		/* trivially sorted */
		return GDK_SUCCEED;
	if (stable) {
		if (reverse)
			return GDKssort_rev(h, t, base, n, hs, ts, tpe);
		else
			return GDKssort(h, t, base, n, hs, ts, tpe);
	} else {
		GDKqsort(h, t, base, n, hs, ts, tpe, reverse, nilslast);
	}
	return GDK_SUCCEED;
}

/* Sort the bat b according to both o and g.  The stable and reverse
 * parameters indicate whether the sort should be stable or descending
 * respectively.  The parameter b is required, o and g are optional
 * (i.e., they may be NULL).
 *
 * A sorted copy is returned through the sorted parameter, the new
 * ordering is returned through the order parameter, group information
 * is returned through the groups parameter.  All three output
 * parameters may be NULL.  If they're all NULL, this function does
 * nothing.
 *
 * If o is specified, it is used to first rearrange b according to the
 * order specified in o, after which b is sorted taking g into
 * account.
 *
 * If g is specified, it indicates groups which should be individually
 * ordered.  Each row of consecutive equal values in g indicates a
 * group which is sorted according to stable and reverse.  g is used
 * after the order in b was rearranged according to o.
 *
 * The outputs order and groups can be used in subsequent calls to
 * this function.  This can be used if multiple BATs need to be sorted
 * together.  The BATs should then be sorted in order of significance,
 * and each following call should use the original unordered BAT plus
 * the order and groups bat from the previous call.  In this case, the
 * sorted BATs are not of much use, so the sorted output parameter
 * does not need to be specified.
 * Apart from error checking and maintaining reference counts, sorting
 * three columns (col1, col2, col3) could look like this with the
 * sorted results in (col1s, col2s, col3s):
 *	BATsort(&col1s, &ord1, &grp1, col1, NULL, NULL, false, false, false);
 *	BATsort(&col2s, &ord2, &grp2, col2, ord1, grp1, false, false, false);
 *	BATsort(&col3s,  NULL,  NULL, col3, ord2, grp2, false, false, false);
 * Note that the "reverse" parameter can be different for each call.
 */
gdk_return
BATsort(BAT **sorted, BAT **order, BAT **groups,
	BAT *b, BAT *o, BAT *g, bool reverse, bool nilslast, bool stable)
{
	BAT *bn = NULL, *on = NULL, *gn = NULL, *pb = NULL;
	oid *restrict grps, *restrict ords, prev;
	BUN p, q, r;
	lng t0 = GDKusec();
	bool mkorderidx, orderidxlock = false;
	Heap *oidxh = NULL;

	/* we haven't implemented NILs as largest value for stable
	 * sort, so NILs come first for ascending and last for
	 * descending */
	assert(!stable || reverse == nilslast);

	if (b == NULL) {
		GDKerror("b must exist\n");
		return GDK_FAIL;
	}
	if (stable && reverse != nilslast) {
		GDKerror("stable sort cannot have reverse != nilslast\n");
		return GDK_FAIL;
	}
	if (!ATOMlinear(b->ttype)) {
		GDKerror("type %s cannot be sorted\n", ATOMname(b->ttype));
		return GDK_FAIL;
	}
	if (b->ttype == TYPE_void) {
		if (!b->tsorted) {
			b->tsorted = true;
		}
		if (b->trevsorted != (is_oid_nil(b->tseqbase) || b->batCount <= 1)) {
			b->trevsorted = !b->trevsorted;
		}
		if (b->tkey != (!is_oid_nil(b->tseqbase) || b->batCount <= 1)) {
			b->tkey = !b->tkey;
		}
	} else if (b->batCount <= 1) {
		if (!b->tsorted || !b->trevsorted) {
			b->tsorted = b->trevsorted = true;
		}
	}
	if (o != NULL &&
	    (ATOMtype(o->ttype) != TYPE_oid || /* oid tail */
	     BATcount(o) != BATcount(b) ||     /* same size as b */
	     (o->ttype == TYPE_void &&	       /* no nil tail */
	      BATcount(o) != 0 &&
	      is_oid_nil(o->tseqbase)))) {
		GDKerror("o must have type oid and same size as b\n");
		return GDK_FAIL;
	}
	if (g != NULL &&
	    (ATOMtype(g->ttype) != TYPE_oid || /* oid tail */
	     !g->tsorted ||		       /* sorted */
	     BATcount(g) != BATcount(b) ||     /* same size as b */
	     (g->ttype == TYPE_void &&	       /* no nil tail */
	      BATcount(g) != 0 &&
	      is_oid_nil(g->tseqbase)))) {
		GDKerror("g must have type oid, sorted on the tail, "
			 "and same size as b\n");
		return GDK_FAIL;
	}
	if (sorted == NULL && order == NULL) {
		/* no place to put result, so we're done quickly */
		GDKerror("no place to put the result.\n");
		return GDK_FAIL;
	}
	if (g == NULL && !stable) {
		/* pre-ordering doesn't make sense if we're not
		 * subsorting and the sort is not stable */
		o = NULL;
	}
	if (b->tnonil) {
		/* if there are no nils, placement of nils doesn't
		 * matter, so set nilslast such that ordered bits can
		 * be used */
		nilslast = reverse;
	}
	if (BATcount(b) <= 1 ||
	    (reverse == nilslast &&
	     (reverse ? BATtrevordered(b) : BATtordered(b)) &&
	     o == NULL && g == NULL &&
	     (groups == NULL || BATtkey(b) ||
	      (reverse ? BATtordered(b) : BATtrevordered(b))))) {
		/* trivially (sub)sorted, and either we don't need to
		 * return group information, or we can trivially
		 * deduce the groups */
		if (sorted) {
			bn = COLcopy(b, b->ttype, false, TRANSIENT);
			if (bn == NULL)
				goto error;
			*sorted = bn;
		}
		if (order) {
			on = BATdense(b->hseqbase, b->hseqbase, BATcount(b));
			if (on == NULL)
				goto error;
			*order = on;
		}
		if (groups) {
			if (BATtkey(b)) {
				/* singleton groups */
				gn = BATdense(0, 0, BATcount(b));
				if (gn == NULL)
					goto error;
			} else {
				/* single group */
				const oid *o = 0;
				assert(BATcount(b) == 1 ||
				       (BATtordered(b) && BATtrevordered(b)));
				gn = BATconstant(0, TYPE_oid, &o, BATcount(b), TRANSIENT);
				if (gn == NULL)
					goto error;
			}
			*groups = gn;
		}
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",o="
			  ALGOOPTBATFMT ",g=" ALGOOPTBATFMT
			  ",reverse=%d,nilslast=%d,stable=%d) = ("
			  ALGOOPTBATFMT "," ALGOOPTBATFMT ","
			  ALGOOPTBATFMT " -- trivial (" LLFMT
			  " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(o),
			  ALGOOPTBATPAR(g), reverse, nilslast, stable,
			  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn),
			  ALGOOPTBATPAR(on), GDKusec() - t0);
		return GDK_SUCCEED;
	}
	if (VIEWtparent(b)) {
		pb = BBP_cache(VIEWtparent(b));
		if (b->tbaseoff != pb->tbaseoff ||
		    BATcount(b) != BATcount(pb) ||
		    b->hseqbase != pb->hseqbase ||
		    BATatoms[b->ttype].atomCmp != BATatoms[pb->ttype].atomCmp)
			pb = NULL;
	} else {
		pb = b;
	}
	/* when we will create an order index if it doesn't already exist */
	mkorderidx = (g == NULL && !reverse && !nilslast && pb != NULL && (order || !pb->batTransient));
	if (g == NULL && !reverse && !nilslast && pb != NULL) {
		(void) BATcheckorderidx(pb);
		MT_lock_set(&pb->batIdxLock);
		if (pb->torderidx) {
			if (!stable || ((oid *) pb->torderidx->base)[2]) {
				/* we can use the order index */
				oidxh = pb->torderidx;
				HEAPincref(oidxh);
			}
			mkorderidx = false;
		} else if (b != pb) {
			/* don't build orderidx on parent bat */
			mkorderidx = false;
		} else if (mkorderidx) {
			/* keep lock when going to create */
			orderidxlock = true;
		}
		if (!orderidxlock)
			MT_lock_unset(&pb->batIdxLock);
	}
	if (g == NULL && o == NULL && !reverse && !nilslast && oidxh != NULL) {
		/* there is an order index that we can use */
		on = COLnew(pb->hseqbase, TYPE_oid, BATcount(pb), TRANSIENT);
		if (on == NULL)
			goto error;
		memcpy(Tloc(on, 0), (oid *) oidxh->base + ORDERIDXOFF, BATcount(pb) * sizeof(oid));
		BATsetcount(on, BATcount(b));
		HEAPdecref(oidxh, false);
		oidxh = NULL;
		on->tkey = true;
		on->tnil = false;
		on->tnonil = true;
		on->tsorted = on->trevsorted = false;
		on->tseqbase = oid_nil;
		if (sorted || groups) {
			bn = BATproject(on, b);
			if (bn == NULL)
				goto error;
			bn->tsorted = true;
			if (groups) {
				if (BATgroup_internal(groups, NULL, NULL, bn, NULL, g, NULL, NULL, true) != GDK_SUCCEED)
					goto error;
				if (sorted &&
				    (*groups)->tkey &&
				    g == NULL) {
					/* if new groups bat is key
					 * and since there is no input
					 * groups bat, we know the
					 * result bat is key */
					bn->tkey = true;
				}
			}
			if (sorted)
				*sorted = bn;
			else {
				BBPunfix(bn->batCacheid);
				bn = NULL;
			}
		}
		if (order)
			*order = on;
		else {
			BBPunfix(on->batCacheid);
			on = NULL;
		}
		TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",o="
			  ALGOOPTBATFMT ",g=" ALGOOPTBATFMT
			  ",reverse=%d,nilslast=%d,stable=%d) = ("
			  ALGOOPTBATFMT "," ALGOOPTBATFMT ","
			  ALGOOPTBATFMT " -- orderidx (" LLFMT
			  " usec)\n",
			  ALGOBATPAR(b), ALGOOPTBATPAR(o),
			  ALGOOPTBATPAR(g), reverse, nilslast, stable,
			  ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn),
			  ALGOOPTBATPAR(on), GDKusec() - t0);
		return GDK_SUCCEED;
	} else if (oidxh) {
		HEAPdecref(oidxh, false);
		oidxh = NULL;
	}
	if (o) {
		bn = BATproject(o, b);
		if (bn == NULL)
			goto error;
		if (bn->ttype == TYPE_void || isVIEW(bn)) {
			BAT *b2 = COLcopy(bn, ATOMtype(bn->ttype), true, TRANSIENT);
			BBPunfix(bn->batCacheid);
			bn = b2;
		}
		pb = NULL;
	} else {
		bn = COLcopy(b, b->ttype, true, TRANSIENT);
	}
	if (bn == NULL)
		goto error;
	if (order) {
		/* prepare order bat */
		if (o) {
			/* make copy of input so that we can refine it;
			 * copy can be read-only if we take the shortcut
			 * below in the case g is "key" */
			on = COLcopy(o, TYPE_oid,
				     g == NULL ||
				     !(g->tkey || g->ttype == TYPE_void),
				     TRANSIENT);
			if (on == NULL)
				goto error;
			BAThseqbase(on, b->hseqbase);
			on->tminpos = BUN_NONE;
			on->tmaxpos = BUN_NONE;
		} else {
			/* create new order */
			on = COLnew(b->hseqbase, TYPE_oid, BATcount(bn), TRANSIENT);
			if (on == NULL)
				goto error;
			ords = (oid *) Tloc(on, 0);
			for (p = 0, q = BATcount(bn); p < q; p++)
				ords[p] = p + b->hseqbase;
			BATsetcount(on, BATcount(bn));
			on->tkey = true;
			on->tnil = false;
			on->tnonil = true;
		}
		/* COLcopy above can create TYPE_void */
		if (on->ttype != TYPE_void) {
			on->tsorted = on->trevsorted = false; /* it won't be sorted */
			on->tseqbase = oid_nil;	/* and hence not dense */
			on->tnosorted = on->tnorevsorted = 0;
		}
		*order = on;
		ords = (oid *) Tloc(on, 0);
	} else {
		ords = NULL;
	}
	if (g) {
		if (g->tkey || g->ttype == TYPE_void) {
			/* if g is "key", all groups are size 1, so no
			 * subsorting needed */
			if (sorted) {
				*sorted = bn;
			} else {
				BBPunfix(bn->batCacheid);
				bn = NULL;
			}
			if (order) {
				*order = on;
				if (o) {
					/* we can inherit sortedness
					 * after all */
					on->tsorted = o->tsorted;
					on->trevsorted = o->trevsorted;
					if (o->tnosorted)
						on->tnosorted = o->tnosorted;
					if (o->tnorevsorted)
						on->tnorevsorted = o->tnorevsorted;
				} else {
					/* we didn't rearrange, so
					 * still sorted */
					on->tsorted = true;
					on->trevsorted = false;
				}
				if (BATcount(on) <= 1) {
					on->tsorted = true;
					on->trevsorted = true;
				}
			}
			if (groups) {
				gn = COLcopy(g, g->ttype, false, TRANSIENT);
				if (gn == NULL)
					goto error;
				*groups = gn;
			}
			TRC_DEBUG(ALGO, "b=" ALGOBATFMT
				  ",o=" ALGOOPTBATFMT ",g=" ALGOBATFMT
				  ",reverse=%d,nilslast=%d,stable=%d"
				  ") = (" ALGOOPTBATFMT ","
				  ALGOOPTBATFMT "," ALGOOPTBATFMT
				  " -- key group (" LLFMT " usec)\n",
				  ALGOBATPAR(b), ALGOOPTBATPAR(o),
				  ALGOBATPAR(g), reverse, nilslast,
				  stable, ALGOOPTBATPAR(bn),
				  ALGOOPTBATPAR(gn), ALGOOPTBATPAR(on),
				  GDKusec() - t0);
			return GDK_SUCCEED;
		}
		assert(g->ttype == TYPE_oid);
		grps = (oid *) Tloc(g, 0);
		prev = grps[0];
		if (BATmaterialize(bn) != GDK_SUCCEED)
			goto error;
		for (r = 0, p = 1, q = BATcount(g); p < q; p++) {
			if (grps[p] != prev) {
				/* sub sort [r,p) */
				if (do_sort(Tloc(bn, r),
					    ords ? ords + r : NULL,
					    bn->tvheap ? bn->tvheap->base : NULL,
					    p - r, Tsize(bn), ords ? sizeof(oid) : 0,
					    bn->ttype, reverse, nilslast, stable) != GDK_SUCCEED)
					goto error;
				r = p;
				prev = grps[p];
			}
		}
		/* sub sort [r,q) */
		if (do_sort(Tloc(bn, r),
			    ords ? ords + r : NULL,
			    bn->tvheap ? bn->tvheap->base : NULL,
			    p - r, Tsize(bn), ords ? sizeof(oid) : 0,
			    bn->ttype, reverse, nilslast, stable) != GDK_SUCCEED)
			goto error;
		/* if single group (r==0) the result is (rev)sorted,
		 * otherwise (maybe) not */
		bn->tsorted = r == 0 && !reverse && !nilslast;
		bn->trevsorted = r == 0 && reverse && nilslast;
	} else {
		Heap *m = NULL;
		/* only invest in creating an order index if the BAT
		 * is persistent */
		if (mkorderidx) {
			assert(orderidxlock);
			if ((m = createOIDXheap(pb, stable)) != NULL &&
			    ords == NULL) {
				ords = (oid *) m->base + ORDERIDXOFF;
				if (o && o->ttype != TYPE_void)
					memcpy(ords, Tloc(o, 0), BATcount(o) * sizeof(oid));
				else if (o)
					for (p = 0, q = BATcount(o); p < q; p++)
						ords[p] = p + o->tseqbase;
				else
					for (p = 0, q = BATcount(b); p < q; p++)
						ords[p] = p + b->hseqbase;
			}
		}
		if ((reverse != nilslast ||
		     (reverse ? !bn->trevsorted : !bn->tsorted)) &&
		    (BATmaterialize(bn) != GDK_SUCCEED ||
		     do_sort(Tloc(bn, 0),
			     ords,
			     bn->tvheap ? bn->tvheap->base : NULL,
			     BATcount(bn), Tsize(bn), ords ? sizeof(oid) : 0,
			     bn->ttype, reverse, nilslast, stable) != GDK_SUCCEED)) {
			if (m != NULL) {
				HEAPfree(m, true);
				GDKfree(m);
			}
			goto error;
		}
		bn->tsorted = !reverse && !nilslast;
		bn->trevsorted = reverse && nilslast;
		if (m != NULL) {
			assert(orderidxlock);
			if (pb->torderidx == NULL) {
				if (ords != (oid *) m->base + ORDERIDXOFF) {
					memcpy((oid *) m->base + ORDERIDXOFF,
					       ords,
					       BATcount(pb) * sizeof(oid));
				}
				ATOMIC_INIT(&m->refs, 1);
				pb->torderidx = m;
				persistOIDX(pb);
			} else {
				HEAPfree(m, true);
				GDKfree(m);
			}
		}
	}
	if (orderidxlock) {
		MT_lock_unset(&pb->batIdxLock);
		orderidxlock = false;
	}
	bn->theap->dirty = true;
	bn->tnosorted = 0;
	bn->tnorevsorted = 0;
	bn->tnokey[0] = bn->tnokey[1] = 0;
	bn->tminpos = BUN_NONE;
	bn->tmaxpos = BUN_NONE;
	if (groups) {
		if (BATgroup_internal(groups, NULL, NULL, bn, NULL, g, NULL, NULL, true) != GDK_SUCCEED)
			goto error;
		if ((*groups)->tkey &&
		    (g == NULL || (g->tsorted && g->trevsorted))) {
			/* if new groups bat is key and the input
			 * group bat has a single value (both sorted
			 * and revsorted), we know the result bat is
			 * key */
			bn->tkey = true;
		}
	}

	if (sorted)
		*sorted = bn;
	else {
		BBPunfix(bn->batCacheid);
		bn = NULL;
	}

	TRC_DEBUG(ALGO, "b=" ALGOBATFMT ",o=" ALGOOPTBATFMT
		  ",g=" ALGOOPTBATFMT ",reverse=%d,nilslast=%d,"
		  "stable=%d) = (" ALGOOPTBATFMT "," ALGOOPTBATFMT ","
		  ALGOOPTBATFMT " -- %ssort (" LLFMT " usec)\n",
		  ALGOBATPAR(b), ALGOOPTBATPAR(o), ALGOOPTBATPAR(g),
		  reverse, nilslast, stable, ALGOOPTBATPAR(bn),
		  ALGOOPTBATPAR(gn), ALGOOPTBATPAR(on),
		  g ? "grouped " : "", GDKusec() - t0);
	return GDK_SUCCEED;

  error:
	if (orderidxlock)
		MT_lock_unset(&pb->batIdxLock);
	if (oidxh)
		HEAPdecref(oidxh, false);
	if (bn)
		BBPunfix(bn->batCacheid);
	BBPreclaim(on);
	if (sorted)
		*sorted = NULL;
	if (order)
		*order = NULL;
	if (groups)
		*groups = NULL;
	return GDK_FAIL;
}

/* return a new BAT of length n with seqbase hseq, and the constant v
 * in the tail */
BAT *
BATconstant(oid hseq, int tailtype, const void *v, BUN n, role_t role)
{
	BAT *bn;
	void *restrict p;
	BUN i;
	lng t0 = 0;

	TRC_DEBUG_IF(ALGO) t0 = GDKusec();
	if (v == NULL)
		return NULL;
	bn = COLnew(hseq, tailtype, n, role);
	if (bn != NULL && n > 0) {
		p = Tloc(bn, 0);
		switch (ATOMstorage(tailtype)) {
		case TYPE_void:
			v = &oid_nil;
			BATtseqbase(bn, oid_nil);
			break;
		case TYPE_msk:
			if (*(msk*)v) {
				memset(p, 0xFF, 4 * ((n + 31) / 32));
				if (n & 31) {
					uint32_t *m = p;
					m[n / 32] &= (1U << (n % 32)) - 1;
				}
			} else
				memset(p, 0x00, 4 * ((n + 31) / 32));
			break;
		case TYPE_bte:
			memset(p, *(bte*)v, n);
			break;
		case TYPE_sht:
			for (i = 0; i < n; i++)
				((sht *) p)[i] = *(sht *) v;
			break;
		case TYPE_int:
		case TYPE_flt:
			assert(sizeof(int) == sizeof(flt));
			for (i = 0; i < n; i++)
				((int *) p)[i] = *(int *) v;
			break;
		case TYPE_lng:
		case TYPE_dbl:
			assert(sizeof(lng) == sizeof(dbl));
			for (i = 0; i < n; i++)
				((lng *) p)[i] = *(lng *) v;
			break;
#ifdef HAVE_HGE
		case TYPE_hge:
			for (i = 0; i < n; i++)
				((hge *) p)[i] = *(hge *) v;
			break;
#endif
		case TYPE_uuid:
			for (i = 0; i < n; i++)
				((uuid *) p)[i] = *(uuid *) v;
			break;
		case TYPE_str:
			/* insert the first value, then just copy the
			 * offset lots of times */
			if (tfastins_nocheck(bn, 0, v) != GDK_SUCCEED) {
				BBPreclaim(bn);
				return NULL;
			}
			char val[sizeof(var_t)];
			memcpy(val, Tloc(bn, 0), bn->twidth);
			if (bn->twidth == 1 && n > 1) {
				/* single byte value: we have a
				 * function for that */
				memset(Tloc(bn, 1), val[0], n - 1);
			} else {
				char *p = Tloc(bn, 0);
				for (i = 1; i < n; i++) {
					p += bn->twidth;
					memcpy(p, val, bn->twidth);
				}
			}
			break;
		default:
			for (i = 0; i < n; i++)
				if (tfastins_nocheck(bn, i, v) != GDK_SUCCEED) {
					BBPreclaim(bn);
					return NULL;
				}
			break;
		}
		bn->theap->dirty = true;
		bn->tnil = n >= 1 && ATOMnilptr(tailtype) && (*ATOMcompare(tailtype))(v, ATOMnilptr(tailtype)) == 0;
		BATsetcount(bn, n);
		bn->tsorted = bn->trevsorted = ATOMlinear(tailtype);
		bn->tnonil = !bn->tnil;
		bn->tkey = BATcount(bn) <= 1;
	}
	TRC_DEBUG(ALGO, "-> " ALGOOPTBATFMT " " LLFMT "usec\n",
		  ALGOOPTBATPAR(bn), GDKusec() - t0);
	return bn;
}

/*
 * BAT Aggregates
 *
 * We retain the size() and card() aggregate results in the column
 * descriptor.  We would like to have such functionality in an
 * extensible way for many aggregates, for DD (1) we do not want to
 * change the binary BAT format on disk and (2) aggr and size are the
 * most relevant aggregates.
 *
 * It is all hacked into the aggr[3] records; three adjacent integers
 * that were left over in the column record. We refer to these as if
 * it where an int aggr[3] array.  The below routines set and retrieve
 * the aggregate values from the tail of the BAT, as many
 * aggregate-manipulating BAT functions work on tail.
 *
 * The rules are as follows: aggr[0] contains the alignment ID of the
 * column (if set i.e. nonzero).  Hence, if this value is nonzero and
 * equal to b->talign, the precomputed aggregate values in
 * aggr[GDK_AGGR_SIZE] and aggr[GDK_AGGR_CARD] hold. However, only one
 * of them may be set at the time. This is encoded by the value
 * int_nil, which cannot occur in these two aggregates.
 *
 * This was now extended to record the property whether we know there
 * is a nil value present by mis-using the highest bits of both
 * GDK_AGGR_SIZE and GDK_AGGR_CARD.
 */

void
PROPdestroy_nolock(BAT *b)
{
	PROPrec *p = b->tprops;
	PROPrec *n;

	b->tprops = NULL;
	while (p) {
		n = p->next;
		VALclear(&p->v);
		GDKfree(p);
		p = n;
	}
}

void
PROPdestroy(BAT *b)
{
	MT_lock_set(&b->theaplock);
	PROPdestroy_nolock(b);
	MT_lock_unset(&b->theaplock);
}

ValPtr
BATgetprop_nolock(BAT *b, enum prop_t idx)
{
	PROPrec *p;

	p = b->tprops;
	while (p && p->id != idx)
		p = p->next;
	return p ? &p->v : NULL;
}

void
BATrmprop_nolock(BAT *b, enum prop_t idx)
{
	PROPrec *prop = b->tprops, *prev = NULL;

	while (prop) {
		if (prop->id == idx) {
			if (prev)
				prev->next = prop->next;
			else
				b->tprops = prop->next;
			VALclear(&prop->v);
			GDKfree(prop);
			return;
		}
		prev = prop;
		prop = prop->next;
	}
}

ValPtr
BATsetprop_nolock(BAT *b, enum prop_t idx, int type, const void *v)
{
	PROPrec *p;

	p = b->tprops;
	while (p && p->id != idx)
		p = p->next;
	if (p == NULL) {
		if ((p = GDKmalloc(sizeof(PROPrec))) == NULL) {
			/* properties are hints, so if we can't create
			 * one we ignore the error */
			GDKclrerr();
			return NULL;
		}
		p->id = idx;
		p->next = b->tprops;
		p->v.vtype = 0;
		b->tprops = p;
	} else {
		VALclear(&p->v);
	}
	if (VALinit(&p->v, type, v) == NULL) {
		/* failed to initialize, so remove property */
		BATrmprop_nolock(b, idx);
		GDKclrerr();
		p = NULL;
	}
	return p ? &p->v : NULL;
}

ValPtr
BATgetprop_try(BAT *b, enum prop_t idx)
{
	ValPtr p = NULL;
	if (MT_lock_try(&b->theaplock)) {
		p = BATgetprop_nolock(b, idx);
		MT_lock_unset(&b->theaplock);
	}
	return p;
}

ValPtr
BATgetprop(BAT *b, enum prop_t idx)
{
	ValPtr p;

	MT_lock_set(&b->theaplock);
	p = BATgetprop_nolock(b, idx);
	MT_lock_unset(&b->theaplock);
	return p;
}

ValPtr
BATsetprop(BAT *b, enum prop_t idx, int type, const void *v)
{
	ValPtr p;
	MT_lock_set(&b->theaplock);
	p = BATsetprop_nolock(b, idx, type, v);
	MT_lock_unset(&b->theaplock);
	return p;
}

void
BATrmprop(BAT *b, enum prop_t idx)
{
	MT_lock_set(&b->theaplock);
	BATrmprop_nolock(b, idx);
	MT_lock_unset(&b->theaplock);
}

/*
 * The BATcount_no_nil function counts all BUN in a BAT that have a
 * non-nil tail value.
 */
BUN
BATcount_no_nil(BAT *b, BAT *s)
{
	BUN cnt = 0;
	BUN i, n;
	const void *restrict p, *restrict nil;
	const char *restrict base;
	int t;
	int (*cmp)(const void *, const void *);
	struct canditer ci;
	oid hseq;

	BATcheck(b, 0);

	hseq = b->hseqbase;
	n = canditer_init(&ci, b, s);
	if (b->tnonil)
		return n;
	BATiter bi = bat_iterator(b);
	p = bi.base;
	t = ATOMbasetype(b->ttype);
	switch (t) {
	case TYPE_void:
		cnt = n * BATtdense(b);
		break;
	case TYPE_msk:
		cnt = n;
		break;
	case TYPE_bte:
		for (i = 0; i < n; i++)
			cnt += !is_bte_nil(((const bte *) p)[canditer_next(&ci) - hseq]);
		break;
	case TYPE_sht:
		for (i = 0; i < n; i++)
			cnt += !is_sht_nil(((const sht *) p)[canditer_next(&ci) - hseq]);
		break;
	case TYPE_int:
		for (i = 0; i < n; i++)
			cnt += !is_int_nil(((const int *) p)[canditer_next(&ci) - hseq]);
		break;
	case TYPE_lng:
		for (i = 0; i < n; i++)
			cnt += !is_lng_nil(((const lng *) p)[canditer_next(&ci) - hseq]);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		for (i = 0; i < n; i++)
			cnt += !is_hge_nil(((const hge *) p)[canditer_next(&ci) - hseq]);
		break;
#endif
	case TYPE_flt:
		for (i = 0; i < n; i++)
			cnt += !is_flt_nil(((const flt *) p)[canditer_next(&ci) - hseq]);
		break;
	case TYPE_dbl:
		for (i = 0; i < n; i++)
			cnt += !is_dbl_nil(((const dbl *) p)[canditer_next(&ci) - hseq]);
		break;
	case TYPE_uuid:
		for (i = 0; i < n; i++)
			cnt += !is_uuid_nil(((const uuid *) p)[canditer_next(&ci) - hseq]);
		break;
	case TYPE_str:
		base = bi.vh->base;
		switch (bi.width) {
		case 1:
			for (i = 0; i < n; i++)
				cnt += base[(var_t) ((const unsigned char *) p)[canditer_next(&ci) - hseq] + GDK_VAROFFSET] != '\200';
			break;
		case 2:
			for (i = 0; i < n; i++)
				cnt += base[(var_t) ((const unsigned short *) p)[canditer_next(&ci) - hseq] + GDK_VAROFFSET] != '\200';
			break;
#if SIZEOF_VAR_T != SIZEOF_INT
		case 4:
			for (i = 0; i < n; i++)
				cnt += base[(var_t) ((const unsigned int *) p)[canditer_next(&ci) - hseq]] != '\200';
			break;
#endif
		default:
			for (i = 0; i < n; i++)
				cnt += base[((const var_t *) p)[canditer_next(&ci) - hseq]] != '\200';
			break;
		}
		break;
	default:
		nil = ATOMnilptr(t);
		cmp = ATOMcompare(t);
		if (nil == NULL) {
			cnt = n;
		} else if (b->tvarsized) {
			base = b->tvheap->base;
			for (i = 0; i < n; i++)
				cnt += (*cmp)(nil, base + ((const var_t *) p)[canditer_next(&ci) - hseq]) != 0;
		} else {
			for (i = 0, n += i; i < n; i++)
				cnt += (*cmp)(BUNtloc(bi, canditer_next(&ci) - hseq), nil) != 0;
		}
		break;
	}
	if (cnt == bi.count) {
		MT_lock_set(&b->theaplock);
		if (cnt == BATcount(b) && bi.h == b->theap) {
			/* we learned something */
			b->tnonil = true;
			assert(!b->tnil);
			b->tnil = false;
		}
		MT_lock_unset(&b->theaplock);
		bat pbid = VIEWtparent(b);
		if (pbid) {
			BAT *pb = BBP_cache(pbid);
			MT_lock_set(&pb->theaplock);
			if (cnt == BATcount(pb) &&
			    bi.h == pb->theap &&
			    !pb->tnonil) {
				pb->tnonil = true;
				assert(!pb->tnil);
				pb->tnil = false;
			}
			MT_lock_unset(&pb->theaplock);
		}
	}
	bat_iterator_end(&bi);
	return cnt;
}
