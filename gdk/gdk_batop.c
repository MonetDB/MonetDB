/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
#include "gdk_cand.h"

gdk_return
unshare_string_heap(BAT *b)
{
	assert(b->batCacheid > 0);
	if (b->ttype == TYPE_str &&
	    b->tvheap->parentid != b->batCacheid) {
		Heap *h = GDKzalloc(sizeof(Heap));
		if (h == NULL)
			return GDK_FAIL;
		h->parentid = b->batCacheid;
		h->farmid = BBPselectfarm(b->batRole, TYPE_str, varheap);
		if (b->tvheap->filename) {
			char *nme = BBP_physical(b->batCacheid);
			h->filename = GDKfilepath(NOFARM, NULL, nme, "theap");
			if (h->filename == NULL) {
				GDKfree(h);
				return GDK_FAIL;
			}
		}
		if (HEAPcopy(h, b->tvheap) != GDK_SUCCEED) {
			HEAPfree(h, 1);
			GDKfree(h);
			return GDK_FAIL;
		}
		BBPunshare(b->tvheap->parentid);
		b->tvheap = h;
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
insert_string_bat(BAT *b, BAT *n, BAT *s, int force)
{
	BATiter ni;		/* iterator */
	size_t toff = ~(size_t) 0;	/* tail offset */
	BUN p, r;		/* loop variables */
	const void *tp;		/* tail value pointer */
	unsigned char tbv;	/* tail value-as-bte */
	unsigned short tsv;	/* tail value-as-sht */
#if SIZEOF_VAR_T == 8
	unsigned int tiv;	/* tail value-as-int */
#endif
	var_t v;		/* value */
	size_t off;		/* offset within n's string heap */
	BUN start, end, cnt;
	const oid *restrict cand = NULL, *candend = NULL;

	assert(b->ttype == TYPE_str);
	/* only transient bats can use some other bat's string heap */
	assert(b->batRole == TRANSIENT || b->tvheap->parentid == b->batCacheid);
	if (n->batCount == 0 || (s && s->batCount == 0))
		return GDK_SUCCEED;
	ni = bat_iterator(n);
	tp = NULL;
	CANDINIT(n, s, start, end, cnt, cand, candend);
	cnt = cand ? (BUN) (candend - cand) : end - start;
	if (cnt == 0)
		return GDK_SUCCEED;
	if ((!GDK_ELIMDOUBLES(b->tvheap) || b->batCount == 0) &&
	    !GDK_ELIMDOUBLES(n->tvheap) &&
	    b->tvheap->hashash == n->tvheap->hashash) {
		if (b->batRole == TRANSIENT || b->tvheap == n->tvheap) {
			/* If b is in the transient farm (i.e. b will
			 * never become persistent), we try some
			 * clever tricks to avoid copying:
			 * - if b is empty, we just let it share the
                         *   string heap with n;
			 * - otherwise, if b's string heap and n's
                         *   string heap are the same (i.e. shared),
                         *   we leave it that way (this includes the
                         *   case that b is persistent and n shares
                         *   its string heap with b);
			 * - otherwise, if b shares its string heap
                         *   with some other bat, we materialize it
                         *   and we will have to copy strings.
			 */
			bat bid = b->batCacheid;

			/* if cand != NULL, there is no wholesale
			 * copying of n's offset heap, but we may
			 * still be able to share the string heap */
			if (b->batCount == 0 &&
			    b->tvheap != n->tvheap &&
			    cand == NULL) {
				if (b->tvheap->parentid != bid) {
					BBPunshare(b->tvheap->parentid);
				} else {
					HEAPfree(b->tvheap, 1);
					GDKfree(b->tvheap);
				}
				BBPshare(n->tvheap->parentid);
				b->tvheap = n->tvheap;
				toff = 0;
			} else if (b->tvheap->parentid == n->tvheap->parentid &&
				   cand == NULL) {
				toff = 0;
			} else if (b->tvheap->parentid != bid &&
				   unshare_string_heap(b) != GDK_SUCCEED) {
				return GDK_FAIL;
			}
		}
		if (toff == ~(size_t) 0 && cnt > 1024) {
			/* If b and n aren't sharing their string
			 * heaps, we try to determine whether to copy
			 * n's whole string heap to the end of b's, or
			 * whether we will insert each string from n
			 * individually.  We do this by testing a
			 * sample of n's strings and extrapolating
			 * from that sample whether n uses a
			 * significant part of its string heap for its
			 * strings (i.e. whether there are many unused
			 * strings in n's string heap).  If n doesn't
			 * have many strings in the first place, we
			 * skip this and just insert them all
			 * individually.  We also check whether a
			 * significant number of n's strings happen to
			 * have the same offset in b.  In the latter
			 * case we also want to insert strings
			 * individually, but reusing the string in b's
			 * string heap. */
			int match = 0, i;
			size_t len = b->tvheap->hashash ? 1024 * EXTRALEN : 0;
			for (i = 0; i < 1024; i++) {
				p = (BUN) (((double) rand() / RAND_MAX) * (cnt - 1));
				if (cand)
					p = cand[p] - n->hseqbase;
				else
					p += start;
				off = BUNtvaroff(ni, p);
				if (off < b->tvheap->free &&
				    strcmp(b->tvheap->base + off, n->tvheap->base + off) == 0 &&
				    (!b->tvheap->hashash ||
				     ((BUN *) (b->tvheap->base + off))[-1] == (n->tvheap->hashash ? ((BUN *) (n->tvheap->base + off))[-1] : strHash(n->tvheap->base + off))))
					match++;
				len += (strlen(n->tvheap->base + off) + 8) & ~7;
			}
			if (match < 768 && (size_t) (BATcount(n) * (double) len / 1024) >= n->tvheap->free / 2) {
				/* append string heaps */
				toff = b->batCount == 0 ? 0 : b->tvheap->free;
				/* make sure we get alignment right */
				toff = (toff + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
				/* if in "force" mode, the heap may be
				 * shared when memory mapped */
				if (HEAPextend(b->tvheap, toff + n->tvheap->size, force) != GDK_SUCCEED) {
					toff = ~(size_t) 0;
					goto bunins_failed;
				}
				memcpy(b->tvheap->base + toff, n->tvheap->base, n->tvheap->free);
				b->tvheap->free = toff + n->tvheap->free;
				if (toff > 0) {
					/* flush double-elimination
					 * hash table */
					memset(b->tvheap->base, 0,
					       GDK_STRHASHSIZE);
				}
			}
		}
		if (toff != ~(size_t) 0) {
			/* we only have to copy the offsets from n to
			 * b, possibly with an offset (if toff != 0),
			 * so set up some variables and set up b's
			 * tail so that it looks like it's a fixed
			 * size column.  Of course, we must make sure
			 * first that the width of b's offset heap can
			 * accommodate all values. */
			if (b->twidth < SIZEOF_VAR_T &&
			    ((size_t) 1 << 8 * b->twidth) <= (b->twidth <= 2 ? b->tvheap->size - GDK_VAROFFSET : b->tvheap->size)) {
				/* offsets aren't going to fit, so
				 * widen offset heap */
				if (GDKupgradevarheap(b, (var_t) b->tvheap->size, 0, force) != GDK_SUCCEED) {
					toff = ~(size_t) 0;
					goto bunins_failed;
				}
			}
		}
	} else if (unshare_string_heap(b) != GDK_SUCCEED)
		return GDK_FAIL;
	if (toff == 0 && n->twidth == b->twidth && cand == NULL) {
		/* we don't need to do any translation of offset
		 * values, so we can use fast memcpy */
		memcpy(Tloc(b, BUNlast(b)), Tloc(n, start),
		       cnt * n->twidth);
		BATsetcount(b, BATcount(b) + cnt);
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
		const unsigned char *restrict tbp = (const unsigned char *) Tloc(n, 0);
		const unsigned short *restrict tsp = (const unsigned short *) Tloc(n, 0);
#if SIZEOF_VAR_T == 8
		const unsigned int *restrict tip = (const unsigned int *) Tloc(n, 0);
#endif
		const var_t *restrict tvp = (const var_t *) Tloc(n, 0);

		switch (b->twidth) {
		case 1:
			b->ttype = TYPE_bte;
			tp = &tbv;
			break;
		case 2:
			b->ttype = TYPE_sht;
			tp = &tsv;
			break;
#if SIZEOF_VAR_T == 8
		case 4:
			b->ttype = TYPE_int;
			tp = &tiv;
			break;
		case 8:
			b->ttype = TYPE_lng;
			tp = &v;
			break;
#else
		case 4:
			b->ttype = TYPE_int;
			tp = &v;
			break;
#endif
		default:
			assert(0);
		}
		b->tvarsized = 0;
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				p = *cand++ - n->hseqbase;
			} else {
				p = start++;
			}
			if (p >= end)
				break;
			switch (n->twidth) {
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
				tbv = (unsigned char) (v - GDK_VAROFFSET);
				break;
			case 2:
				assert(v - GDK_VAROFFSET < ((var_t) 1 << 16));
				tsv = (unsigned short) (v - GDK_VAROFFSET);
				break;
#if SIZEOF_VAR_T == 8
			case 4:
				assert(v < ((var_t) 1 << 32));
				tiv = (unsigned int) v;
				break;
#endif
			default:
				break;
			}
			bunfastapp(b, tp);
		}
		b->tvarsized = 1;
		b->ttype = TYPE_str;
	} else if (b->tvheap->free < n->tvheap->free / 2 ||
		   GDK_ELIMDOUBLES(b->tvheap)) {
		/* if b's string heap is much smaller than n's string
		 * heap, don't bother checking whether n's string
		 * values occur in b's string heap; also, if b is
		 * (still) fully double eliminated, we must continue
		 * to use the double elimination mechanism */
		r = BUNlast(b);
		if (cand) {
			oid hseq = n->hseqbase;
			while (cand < candend) {
				tp = BUNtvar(ni, *cand - hseq);
				bunfastapp(b, tp);
				HASHins(b, r, tp);
				r++;
				cand++;
			}
		} else {
			while (start < end) {
				tp = BUNtvar(ni, start);
				bunfastapp(b, tp);
				HASHins(b, r, tp);
				r++;
				start++;
			}
		}
	} else {
		/* Insert values from n individually into b; however,
		 * we check whether there is a string in b's string
		 * heap at the same offset as the string is in n's
		 * string heap (in case b's string heap is a copy of
		 * n's).  If this is the case, we just copy the
		 * offset, otherwise we insert normally.  */
		r = BUNlast(b);
		for (;;) {
			if (cand) {
				if (cand == candend)
					break;
				p = *cand++ - n->hseqbase;
			} else {
				p = start++;
			}
			if (p >= end)
				break;
			off = BUNtvaroff(ni, p); /* the offset */
			tp = n->tvheap->base + off; /* the string */
			if (off < b->tvheap->free &&
			    strcmp(b->tvheap->base + off, tp) == 0 &&
			    (!b->tvheap->hashash ||
			     ((BUN *) (b->tvheap->base + off))[-1] == (n->tvheap->hashash ? ((BUN *) tp)[-1] : strHash(tp)))) {
				/* we found the string at the same
				 * offset in b's string heap as it was
				 * in n's string heap, so we don't
				 * have to insert a new string into b:
				 * we can just copy the offset */
				v = (var_t) off;
				if (b->twidth < SIZEOF_VAR_T &&
				    ((size_t) 1 << 8 * b->twidth) <= (b->twidth <= 2 ? v - GDK_VAROFFSET : v)) {
					/* offset isn't going to fit,
					 * so widen offset heap */
					if (GDKupgradevarheap(b, v, 0, force) != GDK_SUCCEED) {
						goto bunins_failed;
					}
				}
				switch (b->twidth) {
				case 1:
					assert(v - GDK_VAROFFSET < ((var_t) 1 << 8));
					*(unsigned char *)Tloc(b, BUNlast(b)) = (unsigned char) (v - GDK_VAROFFSET);
					b->theap.free += 1;
					break;
				case 2:
					assert(v - GDK_VAROFFSET < ((var_t) 1 << 16));
					*(unsigned short *)Tloc(b, BUNlast(b)) = (unsigned short) (v - GDK_VAROFFSET);
					b->theap.free += 2;
					break;
#if SIZEOF_VAR_T == 8
				case 4:
					assert(v < ((var_t) 1 << 32));
					*(unsigned int *)Tloc(b, BUNlast(b)) = (unsigned int) v;
					b->theap.free += 4;
					break;
#endif
				default:
					*(var_t *)Tloc(b, BUNlast(b)) = v;
					b->theap.free += SIZEOF_VAR_T;
					break;
				}
				b->batCount++;
			} else {
				bunfastapp(b, tp);
			}
			HASHins(b, r, tp);
			r++;
		}
	}
	return GDK_SUCCEED;
      bunins_failed:
	b->tvarsized = 1;
	b->ttype = TYPE_str;
	return GDK_FAIL;
}

static gdk_return
append_varsized_bat(BAT *b, BAT *n, BAT *s)
{
	BATiter ni;
	BUN start, end, cnt, r;
	const oid *restrict cand = NULL, *candend = NULL;

	/* only transient bats can use some other bat's vheap */
	assert(b->batRole == TRANSIENT || b->tvheap->parentid == b->batCacheid);
	/* make sure the bats use var_t */
	assert(b->twidth == n->twidth);
	assert(b->twidth == SIZEOF_VAR_T);
	if (n->batCount == 0 || (s && s->batCount == 0))
		return GDK_SUCCEED;
	CANDINIT(n, s, start, end, cnt, cand, candend);
	cnt = cand ? (BUN) (candend - cand) : end - start;
	if (cnt == 0)
		return GDK_SUCCEED;
	if (BATcount(b) == 0 &&
	    b->batRole == TRANSIENT &&
	    n->batRestricted == BAT_READ &&
	    b->tvheap != n->tvheap) {
		/* if b is still empty, in the transient farm, and n
		 * is read-only, we replace b's vheap with a reference
		 * to n's */
		if (b->tvheap->parentid != b->batCacheid) {
			BBPunshare(b->tvheap->parentid);
		} else {
			HEAPfree(b->tvheap, 1);
			GDKfree(b->tvheap);
		}
		BBPshare(n->tvheap->parentid);
		b->tvheap = n->tvheap;
	}
	if (b->tvheap == n->tvheap) {
		/* if b and n use the same vheap, we only need to copy
		 * the offsets from n to b */
		HASHdestroy(b);	/* not maintaining, so destroy it */
		if (cand == NULL) {
			/* fast memcpy since we copy a consecutive
			 * chunk of memory */
			memcpy(Tloc(b, BUNlast(b)),
			       Tloc(n, start),
			       cnt * b->twidth);
		} else {
			var_t *restrict dst = (var_t *) Tloc(b, BUNlast(b));
			oid hseq = n->hseqbase;
			const var_t *restrict src = (const var_t *) Tloc(n, 0);
			while (cand < candend)
				*dst++ = src[*cand++ - hseq];
		}
		BATsetcount(b, BATcount(b) + cnt);
		return GDK_SUCCEED;
	}
	/* b and n do not share their vheap, so we need to copy data */
	if (b->tvheap->parentid != b->batCacheid) {
		/* if b shares its vheap with some other bat, unshare it */
		Heap *h = GDKzalloc(sizeof(Heap));
		if (h == NULL)
			return GDK_FAIL;
		h->parentid = b->batCacheid;
		h->farmid = BBPselectfarm(b->batRole, b->ttype, varheap);
		if (b->tvheap->filename) {
			const char *nme = BBP_physical(b->batCacheid);
			h->filename = GDKfilepath(NOFARM, NULL, nme, "theap");
			if (h->filename == NULL) {
				GDKfree(h);
				return GDK_FAIL;
			}
		}
		if (HEAPcopy(h, b->tvheap) != GDK_SUCCEED) {
			HEAPfree(h, 1);
			GDKfree(h);
			return GDK_FAIL;
		}
		BBPunshare(b->tvheap->parentid);
		b->tvheap = h;
	}
	/* copy data from n to b */
	ni = bat_iterator(n);
	r = BUNlast(b);
	if (cand) {
		oid hseq = n->hseqbase;
		while (cand < candend) {
			const void *t = BUNtvar(ni, *cand - hseq);
			bunfastapp_nocheck(b, r, t, Tsize(b));
			HASHins(b, r, t);
			r++;
			cand++;
		}
	} else {
		while (start < end) {
			const void *t = BUNtvar(ni, start);
			bunfastapp_nocheck(b, r, t, Tsize(b));
			HASHins(b, r, t);
			r++;
			start++;
		}
	}
	return GDK_SUCCEED;

      bunins_failed:
	if (b->tunique)
		BBPunfix(s->batCacheid);
	return GDK_FAIL;
}

/* Append the contents of BAT n (subject to the optional candidate
 * list s) to BAT b.  If b is empty, b will get the seqbase of s if it
 * was passed in, and else the seqbase of n. */
gdk_return
BATappend(BAT *b, BAT *n, BAT *s, bit force)
{
	BUN start, end, cnt;
	BUN r;
	const oid *restrict cand = NULL, *candend = NULL;

	if (b == NULL || n == NULL || (cnt = BATcount(n)) == 0) {
		return GDK_SUCCEED;
	}
	assert(b->batCacheid > 0);
	assert(b->theap.parentid == 0);

	ALIGNapp(b, "BATappend", force, GDK_FAIL);

	if (ATOMstorage(ATOMtype(b->ttype)) != ATOMstorage(ATOMtype(n->ttype))) {
		GDKerror("Incompatible operands.\n");
		return GDK_FAIL;
	}
	CHECKDEBUG {
		if (BATttype(b) != BATttype(n) &&
		    ATOMtype(b->ttype) != ATOMtype(n->ttype)) {
			fprintf(stderr,"#Interpreting %s as %s.\n",
				ATOMname(BATttype(n)), ATOMname(BATttype(b)));
		}
	}

	if (BATcount(b) == 0)
		BAThseqbase(b, s ? s->hseqbase : n->hseqbase);

	if (b->tunique) {
		/* if b has the unique bit set, only insert values
		 * from n that don't already occur in b, and make sure
		 * we don't insert any duplicates either; we do this
		 * by calculating a subset of n that complies with
		 * this */
		BAT *d;

		d = BATdiff(n, b, s, NULL, 1, BUN_NONE);
		if (d == NULL)
			return GDK_FAIL;
		s = BATunique(n, d);
		BBPunfix(d->batCacheid);
		if (s == NULL)
			return GDK_FAIL;
		if (BATcount(s) == 0) {
			/* no new values in subset of n */
			BBPunfix(s->batCacheid);
			return GDK_SUCCEED;
		}
	}

	CANDINIT(n, s, start, end, cnt, cand, candend);
	if (start == end) {
		assert(!b->tunique);
		return GDK_SUCCEED;
	}
	/* fix cnt to be number of values we're goind to add to b */
	if (cand)
		cnt = (BUN) (candend - cand);
	else
		cnt = end - start;

	if (BUNlast(b) + cnt > BUN_MAX) {
		if (b->tunique)
			BBPunfix(s->batCacheid);
		GDKerror("BATappend: combined BATs too large\n");
		return GDK_FAIL;
	}

	if (b->hseqbase + BATcount(b) + cnt >= GDK_oid_max) {
		if (b->tunique)
			BBPunfix(s->batCacheid);
		GDKerror("BATappend: overflow of head value\n");
		return GDK_FAIL;
	}

	b->batDirty = 1;

	if (cnt > BATcapacity(b) - BUNlast(b)) {
		/* if needed space exceeds a normal growth extend just
		 * with what's needed */
		BUN ncap = BUNlast(b) + cnt;
		BUN grows = BATgrows(b);

		if (ncap > grows)
			grows = ncap;
		if (BATextend(b, grows) != GDK_SUCCEED)
			goto bunins_failed;
	}

	IMPSdestroy(b);		/* imprints do not support updates yet */
	OIDXdestroy(b);
	PROPdestroy(b->tprops);
	b->tprops = NULL;
	if (b->thash == (Hash *) 1 || BATcount(b) == 0) {
		/* don't bother first loading the hash to then change
		 * it, or updating the hash if we replace the heap */
		HASHdestroy(b);
	}

	if (b->ttype == TYPE_void) {
		if (BATtdense(n) && cand == NULL) {
			/* append two void,void bats */
			oid f = n->tseqbase + start;

			if (n->ttype != TYPE_void)
				f = *(oid *) Tloc(n, start);

			if (BATcount(b) == 0 && f != oid_nil)
				BATtseqbase(b, f);
			if (BATcount(b) + b->tseqbase == f) {
				BATsetcount(b, BATcount(b) + cnt);
				if (b->tunique)
					BBPunfix(s->batCacheid);
				return GDK_SUCCEED;
			}
		}

		/* we need to materialize the tail */
		if (BATmaterialize(b) != GDK_SUCCEED) {
			if (b->tunique)
				BBPunfix(s->batCacheid);
			return GDK_FAIL;
		}
	}

	/* if growing too much, remove the hash, else we maintain it */
	if (BATcheckhash(b) && (2 * b->thash->mask) < (BATcount(b) + cnt)) {
		HASHdestroy(b);
	}

	r = BUNlast(b);

	if (BATcount(b) == 0) {
		BATiter ni = bat_iterator(n);

		b->tsorted = n->tsorted;
		b->trevsorted = n->trevsorted;
		b->tdense = n->tdense && cand == NULL;
		b->tnonil = n->tnonil;
		b->tnil = n->tnil && cnt == BATcount(n);
		b->tseqbase = oid_nil;
		if (cand == NULL) {
			b->tnosorted = start <= n->tnosorted && n->tnosorted < end ? n->tnosorted - start : 0;
			b->tnorevsorted = start <= n->tnorevsorted && n->tnorevsorted < end ? n->tnorevsorted - start : 0;
			b->tnodense = start <= n->tnodense && n->tnodense < end ? n->tnodense - start : 0;
			if (n->tdense && n->ttype == TYPE_oid)
				b->tseqbase = *(oid *) BUNtail(ni, start);
			else if (n->ttype == TYPE_void &&
				 n->tseqbase != oid_nil)
				b->tseqbase = n->tseqbase + start;
		} else {
			b->tnosorted = 0;
			b->tnorevsorted = 0;
		}
		/* if tunique, uniqueness is guaranteed above */
		b->tkey = n->tkey | b->tunique;
		if (!b->tunique && cnt == BATcount(n)) {
			b->tnokey[0] = n->tnokey[0];
			b->tnokey[1] = n->tnokey[1];
		} else {
			b->tnokey[0] = b->tnokey[1] = 0;
		}
	} else {
		BUN last = r - 1;
		BATiter ni = bat_iterator(n);
		BATiter bi = bat_iterator(b);
		int xx = ATOMcmp(b->ttype, BUNtail(ni, start), BUNtail(bi, last));
		if (BATtordered(b) && (!BATtordered(n) || xx < 0)) {
			b->tsorted = FALSE;
			b->tnosorted = 0;
			if (b->tdense) {
				b->tdense = FALSE;
				b->tnodense = r;
			}
		}
		if (BATtrevordered(b) &&
		    (!BATtrevordered(n) || xx > 0)) {
			b->trevsorted = FALSE;
			b->tnorevsorted = 0;
		}
		if (!b->tunique && /* uniqueness is guaranteed above */
		    b->tkey &&
		    (!(BATtordered(b) || BATtrevordered(b)) ||
		     !n->tkey || xx == 0)) {
			BATkey(b, FALSE);
		}
		if (b->ttype != TYPE_void && b->tsorted && b->tdense &&
		    (BATtdense(n) == 0 ||
		     cand != NULL ||
		     1 + *(oid *) BUNtloc(bi, last) != *(oid *) BUNtail(ni, start))) {
			b->tdense = FALSE;
			b->tnodense = cand ? 0 : r;
		}
		b->tnonil &= n->tnonil;
		b->tnil |= n->tnil && cnt == BATcount(n);
	}
	if (b->ttype == TYPE_str) {
		if (insert_string_bat(b, n, s, force) != GDK_SUCCEED) {
			if (b->tunique)
				BBPunfix(s->batCacheid);
			return GDK_FAIL;
		}
	} else if (ATOMvarsized(b->ttype)) {
		if (append_varsized_bat(b, n, s) != GDK_SUCCEED) {
			if (b->tunique)
				BBPunfix(s->batCacheid);
			return GDK_FAIL;
		}
	} else {
		if (BATatoms[b->ttype].atomFix == NULL &&
		    b->ttype != TYPE_void &&
		    n->ttype != TYPE_void &&
		    cand == NULL) {
			/* use fast memcpy if we can, but then we
			 * can't maintain the hash */
			HASHdestroy(b);
			memcpy(Tloc(b, BUNlast(b)),
			       Tloc(n, start),
			       cnt * Tsize(n));
			BATsetcount(b, BATcount(b) + cnt);
		} else {
			BATiter ni = bat_iterator(n);

			if (cand) {
				oid hseq = n->hseqbase;
				while (cand < candend) {
					const void *t = BUNtail(ni, *cand - hseq);
					bunfastapp_nocheck(b, r, t, Tsize(b));
					HASHins(b, r, t);
					r++;
					cand++;
				}
			} else {
				while (start < end) {
					const void *t = BUNtail(ni, start);
					bunfastapp_nocheck(b, r, t, Tsize(b));
					HASHins(b, r, t);
					r++;
					start++;
				}
			}
		}
	}
	if (b->tunique)
		BBPunfix(s->batCacheid);
	return GDK_SUCCEED;
      bunins_failed:
	if (b->tunique)
		BBPunfix(s->batCacheid);
	return GDK_FAIL;
}

gdk_return
BATdel(BAT *b, BAT *d)
{
	int (*unfix) (const void *) = BATatoms[b->ttype].atomUnfix;
	void (*atmdel) (Heap *, var_t *) = BATatoms[b->ttype].atomDel;
	BATiter bi = bat_iterator(b);

	assert(ATOMtype(d->ttype) == TYPE_oid);
	assert(d->tsorted);
	assert(d->tkey);
	if (BATcount(d) == 0)
		return GDK_SUCCEED;
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
			GDKerror("BATdelete: cannot delete committed values\n");
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
				if (unfix)
					(*unfix)(BUNtail(bi, p));
				if (atmdel)
					(*atmdel)(b->tvheap, (var_t *) BUNtloc(bi, p));
				p++;
			}
		}
		if (BATtdense(b) && BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
		if (o + c < b->hseqbase + BATcount(b)) {
			memmove(Tloc(b, o - b->hseqbase),
				Tloc(b, o + c - b->hseqbase),
				Tsize(b) * (BATcount(b) - (o + c - b->hseqbase)));
		}
		b->batCount -= c;
	} else {
		const oid *o = (const oid *) Tloc(d, 0);
		const oid *s;
		BUN c = BATcount(d);
		BUN nd = 0;
		char *p;

		if (o[c - 1] <= b->hseqbase)
			return GDK_SUCCEED;
		while (*o < b->hseqbase) {
			o++;
			c--;
		}
		if (*o - b->hseqbase < b->batInserted) {
			GDKerror("BATdelete: cannot delete committed values\n");
			return GDK_FAIL;
		}
		if (BATtdense(b) && BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
		s = o;
		p = Tloc(b, *o - b->hseqbase);
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
				n *= Tsize(b);
				memmove(p,
					Tloc(b, o[-1] + 1 - b->hseqbase),
					n);
				p += n;
				s = o;
			}
		}
		b->batCount -= nd;
	}
	if (b->batCount <= 1) {
		/* some trivial properties */
		b->tkey = 1;
		b->tsorted = b->trevsorted = 1;
		if (b->batCount == 0) {
			b->tnil = 0;
			b->tnonil = 1;
		}
	}
	/* not sure about these anymore */
	b->tnosorted = b->tnorevsorted = 0;
	b->tnokey[0] = b->tnokey[1] = 0;
	PROPdestroy(b->tprops);
	b->tprops = NULL;

	return GDK_SUCCEED;
}

/*
 * The last in this series is a BATreplace, which replaces all the
 * buns mentioned.
 */
gdk_return
BATreplace(BAT *b, BAT *p, BAT *n, bit force)
{
	if (b == NULL || p == NULL || n == NULL || BATcount(n) == 0) {
		return GDK_SUCCEED;
	}
	if (void_replace_bat(b, p, n, force) == BUN_NONE)
		return GDK_FAIL;
	return GDK_SUCCEED;
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
 * NOTE new semantics, the selected range is excluding the high value.
 */
BAT *
BATslice(BAT *b, BUN l, BUN h)
{
	BUN low = l;
	BAT *bn;
	BATiter bni, bi = bat_iterator(b);
	oid foid;		/* first oid value if oid column */

	BATcheck(b, "BATslice", NULL);
	if (h > BATcount(b))
		h = BATcount(b);
	if (h < l)
		h = l;

	if (l > BUN_MAX || h > BUN_MAX) {
		GDKerror("BATslice: boundary out of range\n");
		return NULL;
	}

	/* If the source BAT is readonly, then we can obtain a VIEW
	 * that just reuses the memory of the source. */
	if (BAThrestricted(b) == BAT_READ && BATtrestricted(b) == BAT_READ) {
		bn = VIEWcreate_(b->hseqbase + low, b, TRUE);
		if (bn == NULL)
			return NULL;
		VIEWbounds(b, bn, l, h);
	} else {
		/* create a new BAT and put everything into it */
		BUN p = l;
		BUN q = h;

		bn = COLnew((oid) (b->hseqbase + low), BATtdense(b) ? TYPE_void : b->ttype, h - l, TRANSIENT);
		if (bn == NULL) {
			return bn;
		}
		if (bn->ttype == TYPE_void ||
		    (!bn->tvarsized &&
		     BATatoms[bn->ttype].atomPut == NULL &&
		     BATatoms[bn->ttype].atomFix == NULL)) {
			if (bn->ttype)
				memcpy(Tloc(bn, 0), Tloc(b, p),
				       (q - p) * Tsize(bn));
			BATsetcount(bn, h - l);
		} else {
			for (; p < q; p++) {
				bunfastapp(bn, BUNtail(bi, p));
			}
		}
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
		if (b->tnodense > l && b->tnodense < h)
			bn->tnodense = b->tnodense - l;
		else
			bn->tnodense = 0;
		if (b->tnokey[0] >= l && b->tnokey[0] < h &&
		    b->tnokey[1] >= l && b->tnokey[1] < h &&
		    b->tnokey[0] != b->tnokey[1]) {
			bn->tnokey[0] = b->tnokey[0] - l;
			bn->tnokey[1] = b->tnokey[1] - l;
		} else {
			bn->tnokey[0] = bn->tnokey[1] = 0;
		}
	}
	bni = bat_iterator(bn);
	if (BATtdense(b)) {
		bn->tdense = TRUE;
		BATtseqbase(bn, (oid) (b->tseqbase + low));
	} else if (bn->tkey && bn->ttype == TYPE_oid) {
		if (BATcount(bn) == 0) {
			bn->tdense = TRUE;
			BATtseqbase(bn, 0);
		} else if (bn->tsorted &&
			   (foid = *(oid *) BUNtloc(bni, 0)) != oid_nil &&
			   foid + BATcount(bn) - 1 == *(oid *) BUNtloc(bni, BUNlast(bn) - 1)) {
			bn->tdense = TRUE;
			BATtseqbase(bn, *(oid *) BUNtloc(bni, 0));
		}
	}
	if (bn->batCount <= 1) {
		bn->tsorted = ATOMlinear(b->ttype);
		bn->trevsorted = ATOMlinear(b->ttype);
		BATkey(bn, 1);
	} else {
		bn->tsorted = b->tsorted;
		bn->trevsorted = b->trevsorted;
		BATkey(bn, BATtkey(b));
	}
	bn->tnonil = b->tnonil || bn->batCount == 0;
	bn->tnil = 0;		/* we just don't know */
	bn->tnosorted = 0;
	bn->tnodense = 0;
	bn->tnokey[0] = bn->tnokey[1] = 0;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/* Return whether the BAT has all unique values or not.  It we don't
 * know, invest in a proper check and record the results in the bat
 * descriptor.  */
int
BATkeyed(BAT *b)
{
	lng t0 = GDKusec();
	BATiter bi = bat_iterator(b);
	int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
	BUN p, q, hb;
	Hash *hs = NULL;

	if (b->ttype == TYPE_void)
		return b->tseqbase != oid_nil || BATcount(b) <= 1;
	if (BATcount(b) <= 1)
		return 1;
	if (b->twidth < SIZEOF_BUN &&
	    BATcount(b) > (BUN) 1 << (8 * b->twidth)) {
		/* more rows than possible bit combinations in the atom */
		assert(!b->tkey);
		return 0;
	}

	/* In order that multiple threads don't scan the same BAT at
	 * the same time (happens a lot with mitosis/mergetable), we
	 * use a lock.  We reuse the hash lock for this, not because
	 * this scanning interferes with hashes, but because it's
	 * there, and not so likely to be used at the same time. */
	MT_lock_set(&GDKhashLock(b->batCacheid));
	b->batDirtydesc = 1;
	if (!b->tkey && b->tnokey[0] == 0 && b->tnokey[1] == 0) {
		if (b->tsorted || b->trevsorted) {
			const void *prev = BUNtail(bi, 0);
			const void *cur;
			for (q = BUNlast(b), p = 1; p < q; p++) {
				cur = BUNtail(bi, p);
				if ((*cmpf)(prev, cur) == 0) {
					b->tnokey[0] = p - 1;
					b->tnokey[1] = p;
					ALGODEBUG fprintf(stderr, "#BATkeyed: fixed nokey(" BUNFMT "," BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p - 1, p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				}
				prev = cur;
			}
			/* we completed the scan: no duplicates */
			b->tkey = 1;
		} else if (BATcheckhash(b) ||
			   (b->batPersistence == PERSISTENT &&
			    BAThash(b, 0) == GDK_SUCCEED)
#ifndef DISABLE_PARENT_HASH
			   || (VIEWtparent(b) != 0 &&
			       BATcheckhash(BBPdescriptor(VIEWtparent(b))))
#endif
			) {
			/* we already have a hash table on b, or b is
			 * persistent and we could create a hash
			 * table, or b is a view on a bat that already
			 * has a hash table */
			BUN lo = 0;

			hs = b->thash;
#ifndef DISABLE_PARENT_HASH
			if (b->thash == NULL && VIEWtparent(b) != 0) {
				BAT *b2 = BBPdescriptor(VIEWtparent(b));
				lo = (BUN) ((b->theap.base - b2->theap.base) >> b->tshift);
				hs = b2->thash;
			}
#endif
			for (q = BUNlast(b), p = 0; p < q; p++) {
				const void *v = BUNtail(bi, p);
				for (hb = HASHgetlink(hs, p + lo);
				     hb != HASHnil(hs) && hb >= lo;
				     hb = HASHgetlink(hs, hb)) {
					assert(hb < p + lo);
					if ((*cmpf)(v, BUNtail(bi, hb - lo)) == 0) {
						b->tnokey[0] = hb - lo;
						b->tnokey[1] = p;
					ALGODEBUG fprintf(stderr, "#BATkeyed: fixed nokey(" BUNFMT "," BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", hb - lo, p, BATgetId(b), BATcount(b), GDKusec() - t0);
						goto doreturn;
					}
				}
			}
			/* we completed the scan: no duplicates */
			b->tkey = 1;
		} else {
			const char *nme;
			size_t nmelen;
			BUN prb;
			BUN mask;
			char *ext = NULL;
			Heap *hp = NULL;

			GDKclrerr(); /* not interested in BAThash errors */
			nme = BBP_physical(b->batCacheid);
			nmelen = strlen(nme);
			if (ATOMbasetype(b->ttype) == TYPE_bte) {
				mask = (BUN) 1 << 8;
				cmpf = NULL; /* no compare needed, "hash" is perfect */
			} else if (ATOMbasetype(b->ttype) == TYPE_sht) {
				mask = (BUN) 1 << 16;
				cmpf = NULL; /* no compare needed, "hash" is perfect */
			} else {
				mask = HASHmask(b->batCount);
				if (mask < ((BUN) 1 << 16))
					mask = (BUN) 1 << 16;
			}
			if ((hp = GDKzalloc(sizeof(Heap))) == NULL ||
			    (hp->filename = GDKmalloc(nmelen + 30)) == NULL ||
			    snprintf(hp->filename, nmelen + 30,
				     "%s.hash" SZFMT, nme, MT_getpid()) < 0 ||
			    (ext = GDKstrdup(hp->filename + nmelen + 1)) == NULL ||
			    (hs = HASHnew(hp, b->ttype, BUNlast(b), mask, BUN_NONE)) == NULL) {
				if (hp) {
					if (hp->filename)
						GDKfree(hp->filename);
					GDKfree(hp);
				}
				GDKfree(ext);
				/* err on the side of caution: not keyed */
				goto doreturn;
			}
			for (q = BUNlast(b), p = 0; p < q; p++) {
				const void *v = BUNtail(bi, p);
				prb = HASHprobe(hs, v);
				for (hb = HASHget(hs, prb);
				     hb != HASHnil(hs);
				     hb = HASHgetlink(hs, hb)) {
					if (cmpf == NULL ||
					    (*cmpf)(v, BUNtail(bi, hb)) == 0) {
						b->tnokey[0] = hb;
						b->tnokey[1] = p;
						ALGODEBUG fprintf(stderr, "#BATkeyed: fixed nokey(" BUNFMT "," BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", hb, p, BATgetId(b), BATcount(b), GDKusec() - t0);
						goto doreturn_free;
					}
				}
				/* enter into hash table */
				HASHputlink(hs, p, HASHget(hs, prb));
				HASHput(hs, prb, p);
			}
		  doreturn_free:
			HEAPfree(hp, 1);
			GDKfree(hp);
			GDKfree(hs);
			GDKfree(ext);
			if (p == q) {
				/* we completed the complete scan: no
				 * duplicates */
				b->tkey = 1;
			}
		}
	}
  doreturn:
	MT_lock_unset(&GDKhashLock(b->batCacheid));
	return b->tkey;
}

/* Return whether the BAT is ordered or not.  If we don't know, invest
 * in a scan and record the results in the bat descriptor.  If during
 * the scan we happen to find evidence that the BAT is not reverse
 * sorted, we record the location.  */
int
BATordered(BAT *b)
{
	lng t0 = 0;

	ALGODEBUG t0 = GDKusec();

	if (b->ttype == TYPE_void)
		return 1;
	/* In order that multiple threads don't scan the same BAT at
	 * the same time (happens a lot with mitosis/mergetable), we
	 * use a lock.  We reuse the hash lock for this, not because
	 * this scanning interferes with hashes, but because it's
	 * there, and not so likely to be used at the same time. */
	MT_lock_set(&GDKhashLock(b->batCacheid));
	if (!b->tsorted && b->tnosorted == 0) {
		BATiter bi = bat_iterator(b);
		int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
		BUN p, q;
		b->batDirtydesc = 1;
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_int: {
			const int *iptr = (const int *) Tloc(b, 0);
			for (q = BUNlast(b), p = 1; p < q; p++) {
				if (iptr[p - 1] > iptr[p]) {
					b->tnosorted = p;
					ALGODEBUG fprintf(stderr, "#BATordered: fixed nosorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				} else if (!b->trevsorted &&
					   b->tnorevsorted == 0 &&
					   iptr[p - 1] < iptr[p]) {
					b->tnorevsorted = p;
					ALGODEBUG fprintf(stderr, "#BATordered: fixed norevsorted(" BUNFMT ") for %s#" BUNFMT "\n", p, BATgetId(b), BATcount(b));
				}
			}
			break;
		}
		case TYPE_lng: {
			const lng *lptr = (const lng *) Tloc(b, 0);
			for (q = BUNlast(b), p = 1; p < q; p++) {
				if (lptr[p - 1] > lptr[p]) {
					b->tnosorted = p;
					ALGODEBUG fprintf(stderr, "#BATordered: fixed nosorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				} else if (!b->trevsorted &&
					   b->tnorevsorted == 0 &&
					   lptr[p - 1] < lptr[p]) {
					b->tnorevsorted = p;
					ALGODEBUG fprintf(stderr, "#BATordered: fixed norevsorted(" BUNFMT ") for %s#" BUNFMT "\n", p, BATgetId(b), BATcount(b));
				}
			}
			break;
		}
		default:
			for (q = BUNlast(b), p = 1; p < q; p++) {
				int c;
				if ((c = cmpf(BUNtail(bi, p - 1), BUNtail(bi, p))) > 0) {
					b->tnosorted = p;
					ALGODEBUG fprintf(stderr, "#BATordered: fixed nosorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				} else if (!b->trevsorted &&
					   b->tnorevsorted == 0 &&
					   c < 0) {
					b->tnorevsorted = p;
					ALGODEBUG fprintf(stderr, "#BATordered: fixed norevsorted(" BUNFMT ") for %s#" BUNFMT "\n", p, BATgetId(b), BATcount(b));
				}
			}
			break;
		}
		/* we only get here if we completed the scan; note
		 * that if we didn't record evidence about *reverse*
		 * sortedness, we know that the BAT is also reverse
		 * sorted */
		b->tsorted = 1;
		ALGODEBUG fprintf(stderr, "#BATordered: fixed sorted for %s#" BUNFMT " (" LLFMT " usec)\n", BATgetId(b), BATcount(b), GDKusec() - t0);
		if (!b->trevsorted && b->tnorevsorted == 0) {
			b->trevsorted = 1;
			ALGODEBUG fprintf(stderr, "#BATordered: fixed revsorted for %s#" BUNFMT "\n", BATgetId(b), BATcount(b));
		}
	}
  doreturn:
	MT_lock_unset(&GDKhashLock(b->batCacheid));
	return b->tsorted;
}

/* Return whether the BAT is reverse ordered or not.  If we don't
 * know, invest in a scan and record the results in the bat
 * descriptor.  */
int
BATordered_rev(BAT *b)
{
	lng t0 = 0;

	ALGODEBUG t0 = GDKusec();

	if (b == NULL)
		return 0;
	if (b->ttype == TYPE_void)
		return b->tseqbase == oid_nil;
	MT_lock_set(&GDKhashLock(b->batCacheid));
	if (!b->trevsorted && b->tnorevsorted == 0) {
		BATiter bi = bat_iterator(b);
		int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
		BUN p, q;
		b->batDirtydesc = 1;
		for (q = BUNlast(b), p = 1; p < q; p++) {
			if (cmpf(BUNtail(bi, p - 1), BUNtail(bi, p)) < 0) {
				b->tnorevsorted = p;
				ALGODEBUG fprintf(stderr, "#BATordered_rev: fixed norevsorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
				goto doreturn;
			}
		}
		b->trevsorted = 1;
		ALGODEBUG fprintf(stderr, "#BATordered_rev: fixed revsorted for %s#" BUNFMT " (" LLFMT " usec)\n", BATgetId(b), BATcount(b), GDKusec() - t0);
	}
  doreturn:
	MT_lock_unset(&GDKhashLock(b->batCacheid));
	return b->trevsorted;
}

/* figure out which sort function is to be called
 * stable sort can produce an error (not enough memory available),
 * "quick" sort does not produce errors */
static gdk_return
do_sort(void *h, void *t, const void *base, size_t n, int hs, int ts, int tpe,
	int reverse, int stable)
{
	if (n <= 1)		/* trivially sorted */
		return GDK_SUCCEED;
	if (reverse) {
		if (stable) {
			return GDKssort_rev(h, t, base, n, hs, ts, tpe);
		} else {
			GDKqsort_rev(h, t, base, n, hs, ts, tpe);
		}
	} else {
		if (stable) {
			return GDKssort(h, t, base, n, hs, ts, tpe);
		} else {
			GDKqsort(h, t, base, n, hs, ts, tpe);
		}
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
 *	BATsort(&col1s, &ord1, &grp1, col1, NULL, NULL, 0, 0);
 *	BATsort(&col2s, &ord2, &grp2, col2, ord1, grp1, 0, 0);
 *	BATsort(&col3s,  NULL,  NULL, col3, ord2, grp2, 0, 0);
 * Note that the "reverse" parameter can be different for each call.
 */
gdk_return
BATsort(BAT **sorted, BAT **order, BAT **groups,
	   BAT *b, BAT *o, BAT *g, int reverse, int stable)
{
	BAT *bn = NULL, *on = NULL, *gn, *pb = NULL;
	oid *restrict grps, *restrict ords, prev;
	BUN p, q, r;

	if (b == NULL) {
		GDKerror("BATsort: b must exist\n");
		return GDK_FAIL;
	}
	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATsort: type %s cannot be sorted\n",
			 ATOMname(b->ttype));
		return GDK_FAIL;
	}
	if (o != NULL &&
	    (ATOMtype(o->ttype) != TYPE_oid || /* oid tail */
	     BATcount(o) != BATcount(b) ||     /* same size as b */
	     (o->ttype == TYPE_void &&	       /* no nil tail */
	      BATcount(o) != 0 &&
	      o->tseqbase == oid_nil))) {
		GDKerror("BATsort: o must have type oid and same size as b\n");
		return GDK_FAIL;
	}
	if (g != NULL &&
	    (ATOMtype(g->ttype) != TYPE_oid || /* oid tail */
	     !g->tsorted ||		       /* sorted */
	     BATcount(o) != BATcount(b) ||     /* same size as b */
	     (g->ttype == TYPE_void &&	       /* no nil tail */
	      BATcount(g) != 0 &&
	      g->tseqbase == oid_nil))) {
		GDKerror("BATsort: g must have type oid, sorted on the tail, and same size as b\n");
		return GDK_FAIL;
	}
	assert(reverse == 0 || reverse == 1);
	assert(stable == 0 || stable == 1);
	if (sorted == NULL && order == NULL && groups == NULL) {
		/* no place to put result, so we're done quickly */
		return GDK_SUCCEED;
	}
	if (g == NULL && !stable) {
		/* pre-ordering doesn't make sense if we're not
		 * subsorting and the sort is not stable */
		o = NULL;
	}
	if (BATcount(b) <= 1 ||
	    ((reverse ? BATtrevordered(b) : BATtordered(b)) &&
	     o == NULL && g == NULL &&
	     (groups == NULL || BATtkey(b) ||
	      (reverse ? BATtordered(b) : BATtrevordered(b))))) {
		/* trivially (sub)sorted, and either we don't need to
		 * return group information, or we can trivially
		 * deduce the groups */
		if (sorted) {
			bn = COLcopy(b, b->ttype, 0, TRANSIENT);
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
		return GDK_SUCCEED;
	}
	if (VIEWtparent(b)) {
		pb = BBPdescriptor(VIEWtparent(b));
		if (b->theap.base != pb->theap.base ||
		    BATcount(b) != BATcount(pb) ||
		    b->hseqbase != pb->hseqbase ||
		    BATatoms[b->ttype].atomCmp != BATatoms[pb->ttype].atomCmp)
			pb = NULL;
	} else {
		pb = b;
	}
	if (g == NULL && o == NULL && !reverse &&
	    pb != NULL && BATcheckorderidx(pb) &&
	    /* if we want a stable sort, the order index must be
	     * stable, if we don't want stable, we don't care */
	    (!stable || ((oid *) pb->torderidx->base)[2])) {
		/* there is an order index that we can use */
		on = COLnew(pb->hseqbase, TYPE_oid, BATcount(pb), TRANSIENT);
		if (on == NULL)
			goto error;
		memcpy(Tloc(on, 0), (oid *) pb->torderidx->base + ORDERIDXOFF, BATcount(pb) * sizeof(oid));
		BATsetcount(on, BATcount(b));
		on->tkey = 1;
		on->tnil = 0;
		on->tnonil = 1;
		on->tsorted = on->trevsorted = 0;
		on->tdense = 0;
		if (sorted || groups) {
			bn = BATproject(on, b);
			if (bn == NULL)
				goto error;
			bn->tsorted = 1;
			if (groups) {
				if (BATgroup_internal(groups, NULL, NULL, bn, NULL, g, NULL, NULL, 1) != GDK_SUCCEED)
					goto error;
				if (sorted &&
				    (*groups)->tkey &&
				    g == NULL) {
					/* if new groups bat is key
					 * and since there is no input
					 * groups bat, we know the
					 * result bat is key */
					bn->tkey = 1;
				}
			}
			if (sorted)
				*sorted = bn;
			else
				BBPunfix(bn->batCacheid);
		}
		if (order)
			*order = on;
		else
			BBPunfix(on->batCacheid);
		return GDK_SUCCEED;
	}
	if (o) {
		bn = BATproject(o, b);
		if (bn == NULL)
			goto error;
		if (bn->ttype == TYPE_void || isVIEW(bn)) {
			b = COLcopy(bn, ATOMtype(bn->ttype), TRUE, TRANSIENT);
			BBPunfix(bn->batCacheid);
			bn = b;
		}
		pb = NULL;
	} else {
		bn = COLcopy(b, b->ttype, TRUE, TRANSIENT);
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
		} else {
			/* create new order */
			on = COLnew(b->hseqbase, TYPE_oid, BATcount(bn), TRANSIENT);
			if (on == NULL)
				goto error;
			ords = (oid *) Tloc(on, 0);
			for (p = 0, q = BATcount(bn); p < q; p++)
				ords[p] = p + b->hseqbase;
			BATsetcount(on, BATcount(bn));
			on->tkey = 1;
			on->tnil = 0;
			on->tnonil = 1;
		}
		on->tsorted = on->trevsorted = 0; /* it won't be sorted */
		on->tdense = 0;			  /* and hence not dense */
		on->tnosorted = on->tnorevsorted = on->tnodense = 0;
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
					on->tsorted = 1;
					on->trevsorted = 0;
				}
				if (BATcount(on) <= 1) {
					on->tsorted = 1;
					on->trevsorted = 1;
				}
			}
			if (groups) {
				gn = COLcopy(g, g->ttype, 0, TRANSIENT);
				if (gn == NULL)
					goto error;
				*groups = gn;
			}
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
					    bn->ttype, reverse, stable) != GDK_SUCCEED)
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
			    bn->ttype, reverse, stable) != GDK_SUCCEED)
			goto error;
		/* if single group (r==0) the result is (rev)sorted,
		 * otherwise (maybe) not */
		bn->tsorted = r == 0 && !reverse;
		bn->trevsorted = r == 0 && reverse;
	} else {
		Heap *m = NULL;
		/* only invest in creating an order index if the BAT
		 * is persistent */
		if (!reverse &&
		    pb != NULL &&
		    (ords != NULL || pb->batPersistence == PERSISTENT) &&
		    (m = createOIDXheap(pb, stable)) != NULL) {
			if (ords == NULL) {
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
		if (b->ttype == TYPE_void) {
			b->tsorted = 1;
			b->trevsorted = b->tseqbase == oid_nil || b->batCount <= 1;
			b->tkey = b->tseqbase != oid_nil;
		} else if (b->batCount <= 1) {
			b->tsorted = b->trevsorted = 1;
		}
		if (!(reverse ? bn->trevsorted : bn->tsorted) &&
		    (BATmaterialize(bn) != GDK_SUCCEED ||
		     do_sort(Tloc(bn, 0),
			     ords,
			     bn->tvheap ? bn->tvheap->base : NULL,
			     BATcount(bn), Tsize(bn), ords ? sizeof(oid) : 0,
			     bn->ttype, reverse, stable) != GDK_SUCCEED)) {
			if (m != NULL) {
				HEAPfree(m, 1);
				GDKfree(m);
			}
			goto error;
		}
		bn->tsorted = !reverse;
		bn->trevsorted = reverse;
		if (m != NULL) {
			MT_lock_set(&GDKhashLock(pb->batCacheid));
			if (pb->torderidx == NULL) {
				pb->batDirtydesc = TRUE;
				pb->torderidx = m;
				if (ords != (oid *) m->base + ORDERIDXOFF) {
					memcpy((oid *) m->base + ORDERIDXOFF,
					       ords,
					       BATcount(pb) * sizeof(oid));
				}
				persistOIDX(pb);
			} else {
				HEAPfree(m, 1);
				GDKfree(m);
			}
			MT_lock_unset(&GDKhashLock(pb->batCacheid));
		}
	}
	bn->tnosorted = 0;
	bn->tnorevsorted = 0;
	if (groups) {
		if (BATgroup_internal(groups, NULL, NULL, bn, NULL, g, NULL, NULL, 1) != GDK_SUCCEED)
			goto error;
		if ((*groups)->tkey &&
		    (g == NULL || (g->tsorted && g->trevsorted))) {
			/* if new groups bat is key and the input
			 * group bat has a single value (both sorted
			 * and revsorted), we know the result bat is
			 * key */
			bn->tkey = 1;
		}
	}

	if (sorted)
		*sorted = bn;
	else
		BBPunfix(bn->batCacheid);

	return GDK_SUCCEED;

  error:
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
BATconstant(oid hseq, int tailtype, const void *v, BUN n, int role)
{
	BAT *bn;
	void *restrict p;
	BUN i;

	if (v == NULL)
		return NULL;
	bn = COLnew(hseq, tailtype, n, role);
	if (bn == NULL)
		return NULL;
	p = Tloc(bn, 0);
	switch (ATOMstorage(tailtype)) {
	case TYPE_void:
		v = &oid_nil;
		BATtseqbase(bn, oid_nil);
		break;
	case TYPE_bte:
		for (i = 0; i < n; i++)
			((bte *) p)[i] = *(bte *) v;
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
	default:
		for (i = 0, n += i; i < n; i++)
			tfastins_nocheck(bn, i, v, Tsize(bn));
		break;
	}
	bn->tnil = n >= 1 && (*ATOMcompare(tailtype))(v, ATOMnilptr(tailtype)) == 0;
	BATsetcount(bn, n);
	bn->tsorted = 1;
	bn->trevsorted = 1;
	bn->tnonil = !bn->tnil;
	bn->tkey = BATcount(bn) <= 1;
	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
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
PROPdestroy(PROPrec *p)
{
	PROPrec *n;

	while (p) {
		n = p->next;
		if (p->v.vtype == TYPE_str)
			GDKfree(p->v.val.sval);
		GDKfree(p);
		p = n;
	}
}

PROPrec *
BATgetprop(BAT *b, int idx)
{
	PROPrec *p = b->tprops;

	while (p) {
		if (p->id == idx)
			return p;
		p = p->next;
	}
	return NULL;
}

void
BATsetprop(BAT *b, int idx, int type, void *v)
{
	ValRecord vr;
	PROPrec *p = BATgetprop(b, idx);

	if (p == NULL &&
	    (p = (PROPrec *) GDKmalloc(sizeof(PROPrec))) != NULL) {
		p->id = idx;
		p->next = b->tprops;
		p->v.vtype = 0;
		b->tprops = p;
	}
	if (p) {
		VALset(&vr, type, v);
		VALcopy(&p->v, &vr);
		b->batDirtydesc = TRUE;
	}
}


/*
 * The BATcount_no_nil function counts all BUN in a BAT that have a
 * non-nil tail value.
 */
BUN
BATcount_no_nil(BAT *b)
{
	BUN cnt = 0;
	BUN i, n;
	const void *restrict p, *restrict nil;
	const char *restrict base;
	int t;
	int (*cmp)(const void *, const void *);

	BATcheck(b, "BATcnt", 0);
	n = BATcount(b);
	if (b->tnonil)
		return n;
	p = Tloc(b, 0);
	t = ATOMbasetype(b->ttype);
	switch (t) {
	case TYPE_void:
		cnt = b->tseqbase == oid_nil ? 0 : n;
		break;
	case TYPE_bte:
		for (i = 0; i < n; i++)
			cnt += ((const bte *) p)[i] != bte_nil;
		break;
	case TYPE_sht:
		for (i = 0; i < n; i++)
			cnt += ((const sht *) p)[i] != sht_nil;
		break;
	case TYPE_int:
		for (i = 0; i < n; i++)
			cnt += ((const int *) p)[i] != int_nil;
		break;
	case TYPE_lng:
		for (i = 0; i < n; i++)
			cnt += ((const lng *) p)[i] != lng_nil;
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		for (i = 0; i < n; i++)
			cnt += ((const hge *) p)[i] != hge_nil;
		break;
#endif
	case TYPE_flt:
		for (i = 0; i < n; i++)
			cnt += ((const flt *) p)[i] != flt_nil;
		break;
	case TYPE_dbl:
		for (i = 0; i < n; i++)
			cnt += ((const dbl *) p)[i] != dbl_nil;
		break;
	case TYPE_str:
		base = b->tvheap->base;
		switch (b->twidth) {
		case 1:
			for (i = 0; i < n; i++)
				cnt += base[(var_t) ((const unsigned char *) p)[i] + GDK_VAROFFSET] != '\200';
			break;
		case 2:
			for (i = 0; i < n; i++)
				cnt += base[(var_t) ((const unsigned short *) p)[i] + GDK_VAROFFSET] != '\200';
			break;
#if SIZEOF_VAR_T != SIZEOF_INT
		case 4:
			for (i = 0; i < n; i++)
				cnt += base[(var_t) ((const unsigned int *) p)[i]] != '\200';
			break;
#endif
		default:
			for (i = 0; i < n; i++)
				cnt += base[((const var_t *) p)[i]] != '\200';
			break;
		}
		break;
	default:
		nil = ATOMnilptr(t);
		cmp = ATOMcompare(t);
		if (b->tvarsized) {
			base = b->tvheap->base;
			for (i = 0; i < n; i++)
				cnt += (*cmp)(nil, base + ((const var_t *) p)[i]) != 0;
		} else {
			for (i = 0, n += i; i < n; i++)
				cnt += (*cmp)(Tloc(b, i), nil) != 0;
		}
		break;
	}
	if (cnt == BATcount(b)) {
		/* we learned something */
		b->tnonil = 1;
		assert(b->tnil == 0);
		b->tnil = 0;
	}
	return cnt;
}

/* create a new, dense candidate list with values from `first' up to,
 * but not including, `last' */
static BAT *
newdensecand(oid first, oid last)
{
	if (last < first)
		first = last = 0; /* empty range */
	return BATdense(0, first, last - first);
}

/* merge two candidate lists and produce a new one
 *
 * candidate lists are VOID-headed BATs with an OID tail which is
 * sorted and unique.
 */
BAT *
BATmergecand(BAT *a, BAT *b)
{
	BAT *bn;
	const oid *restrict ap, *restrict bp, *ape, *bpe;
	oid *restrict p, i;
	oid af, al, bf, bl;
	BATiter ai, bi;
	bit ad, bd;

	BATcheck(a, "BATmergecand", NULL);
	BATcheck(b, "BATmergecand", NULL);
	assert(ATOMtype(a->ttype) == TYPE_oid);
	assert(ATOMtype(b->ttype) == TYPE_oid);
	assert(BATcount(a) <= 1 || a->tsorted);
	assert(BATcount(b) <= 1 || b->tsorted);
	assert(BATcount(a) <= 1 || a->tkey);
	assert(BATcount(b) <= 1 || b->tkey);
	assert(a->tnonil);
	assert(b->tnonil);

	/* we can return a if b is empty (and v.v.) */
	if (BATcount(a) == 0) {
		return COLcopy(b, b->ttype, 0, TRANSIENT);
	}
	if (BATcount(b) == 0) {
		return COLcopy(a, a->ttype, 0, TRANSIENT);
	}
	/* we can return a if a fully covers b (and v.v) */
	ai = bat_iterator(a);
	bi = bat_iterator(b);
	af = *(oid*) BUNtail(ai, 0);
	bf = *(oid*) BUNtail(bi, 0);
	al = *(oid*) BUNtail(ai, BUNlast(a) - 1);
	bl = *(oid*) BUNtail(bi, BUNlast(b) - 1);
	ad = (af + BATcount(a) - 1 == al); /* i.e., dense */
	bd = (bf + BATcount(b) - 1 == bl); /* i.e., dense */
	if (ad && bd) {
		/* both are dense */
		if (af <= bf && bf <= al + 1) {
			/* partial overlap starting with a, or b is
			 * smack bang after a */
			return newdensecand(af, al < bl ? bl + 1 : al + 1);
		}
		if (bf <= af && af <= bl + 1) {
			/* partial overlap starting with b, or a is
			 * smack bang after b */
			return newdensecand(bf, al < bl ? bl + 1 : al + 1);
		}
	}
	if (ad && af <= bf && al >= bl) {
		return newdensecand(af, al + 1);
	}
	if (bd && bf <= af && bl >= al) {
		return newdensecand(bf, bl + 1);
	}

	bn = COLnew(0, TYPE_oid, BATcount(a) + BATcount(b), TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = (oid *) Tloc(bn, 0);
	if (a->ttype == TYPE_void && b->ttype == TYPE_void) {
		/* both lists are VOID */
		if (a->tseqbase > b->tseqbase) {
			BAT *t = a;

			a = b;
			b = t;
		}
		/* a->tseqbase <= b->tseqbase */
		for (i = a->tseqbase; i < a->tseqbase + BATcount(a); i++)
			*p++ = i;
		for (i = MAX(b->tseqbase, i);
		     i < b->tseqbase + BATcount(b);
		     i++)
			*p++ = i;
	} else if (a->ttype == TYPE_void || b->ttype == TYPE_void) {
		if (b->ttype == TYPE_void) {
			BAT *t = a;

			a = b;
			b = t;
		}
		/* a->ttype == TYPE_void, b->ttype == TYPE_oid */
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (bp < bpe && *bp < a->tseqbase)
			*p++ = *bp++;
		for (i = a->tseqbase; i < a->tseqbase + BATcount(a); i++)
			*p++ = i;
		while (bp < bpe && *bp < i)
			bp++;
		while (bp < bpe)
			*p++ = *bp++;
	} else {
		/* a->ttype == TYPE_oid, b->ttype == TYPE_oid */
		ap = (const oid *) Tloc(a, 0);
		ape = ap + BATcount(a);
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (ap < ape && bp < bpe) {
			if (*ap < *bp)
				*p++ = *ap++;
			else if (*ap > *bp)
				*p++ = *bp++;
			else {
				*p++ = *ap++;
				bp++;
			}
		}
		while (ap < ape)
			*p++ = *ap++;
		while (bp < bpe)
			*p++ = *bp++;
	}

	/* properties */
	BATsetcount(bn, (BUN) (p - (oid *) Tloc(bn, 0)));
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tsorted = 1;
	bn->tkey = 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	return virtualize(bn);
}

/* intersect two candidate lists and produce a new one
 *
 * candidate lists are VOID-headed BATs with an OID tail which is
 * sorted and unique.
 */
BAT *
BATintersectcand(BAT *a, BAT *b)
{
	BAT *bn;
	const oid *restrict ap, *restrict bp, *ape, *bpe;
	oid *restrict p;
	oid af, al, bf, bl;
	BATiter ai, bi;

	BATcheck(a, "BATintersectcand", NULL);
	BATcheck(b, "BATintersectcand", NULL);
	assert(ATOMtype(a->ttype) == TYPE_oid);
	assert(ATOMtype(b->ttype) == TYPE_oid);
	assert(a->tsorted);
	assert(b->tsorted);
	assert(a->tkey);
	assert(b->tkey);
	assert(a->tnonil);
	assert(b->tnonil);

	if (BATcount(a) == 0 || BATcount(b) == 0) {
		return newdensecand(0, 0);
	}

	ai = bat_iterator(a);
	bi = bat_iterator(b);
	af = *(oid*) BUNtail(ai, 0);
	bf = *(oid*) BUNtail(bi, 0);
	al = *(oid*) BUNtail(ai, BUNlast(a) - 1);
	bl = *(oid*) BUNtail(bi, BUNlast(b) - 1);

	if ((af + BATcount(a) - 1 == al) && (bf + BATcount(b) - 1 == bl)) {
		/* both lists are VOID */
		return newdensecand(MAX(af, bf), MIN(al, bl) + 1);
	}

	bn = COLnew(0, TYPE_oid, MIN(BATcount(a), BATcount(b)), TRANSIENT);
	if (bn == NULL)
		return NULL;
	p = (oid *) Tloc(bn, 0);
	if (a->ttype == TYPE_void || b->ttype == TYPE_void) {
		if (b->ttype == TYPE_void) {
			BAT *t = a;

			a = b;
			b = t;
		}
		/* a->ttype == TYPE_void, b->ttype == TYPE_oid */
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (bp < bpe && *bp < a->tseqbase)
			bp++;
		while (bp < bpe && *bp < a->tseqbase + BATcount(a))
			*p++ = *bp++;
	} else {
		/* a->ttype == TYPE_oid, b->ttype == TYPE_oid */
		ap = (const oid *) Tloc(a, 0);
		ape = ap + BATcount(a);
		bp = (const oid *) Tloc(b, 0);
		bpe = bp + BATcount(b);
		while (ap < ape && bp < bpe) {
			if (*ap < *bp)
				ap++;
			else if (*ap > *bp)
				bp++;
			else {
				*p++ = *ap++;
				bp++;
			}
		}
	}

	/* properties */
	BATsetcount(bn, (BUN) (p - (oid *) Tloc(bn, 0)));
	bn->trevsorted = BATcount(bn) <= 1;
	bn->tsorted = 1;
	bn->tkey = 1;
	bn->tnil = 0;
	bn->tnonil = 1;
	return virtualize(bn);
}
