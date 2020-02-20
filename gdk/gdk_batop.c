/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
		strconcat_len(h->filename, sizeof(h->filename),
			      BBP_physical(b->batCacheid), ".theap", NULL);
		if (HEAPcopy(h, b->tvheap) != GDK_SUCCEED) {
			HEAPfree(h, true);
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
#ifdef STATIC_CODE_ANALYSIS
#define rand()		0
#endif

static gdk_return
insert_string_bat(BAT *b, BAT *n, BAT *s, bool force)
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
	struct canditer ci;
	BUN cnt;
	BUN oldcnt = BATcount(b);

	assert(b->ttype == TYPE_str);
	/* only transient bats can use some other bat's string heap */
	assert(b->batRole == TRANSIENT || b->tvheap->parentid == b->batCacheid);
	if (n->batCount == 0 || (s && s->batCount == 0))
		return GDK_SUCCEED;
	ni = bat_iterator(n);
	tp = NULL;
	cnt = canditer_init(&ci, n, s);
	if (cnt == 0)
		return GDK_SUCCEED;
	if ((!GDK_ELIMDOUBLES(b->tvheap) || oldcnt == 0) &&
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

			/* if candidates are not dense, there is no
			 * wholesale copying of n's offset heap, but
			 * we may still be able to share the string
			 * heap */
			if (oldcnt == 0 &&
			    b->tvheap != n->tvheap &&
			    ci.tpe == cand_dense) {
				if (b->tvheap->parentid != bid) {
					BBPunshare(b->tvheap->parentid);
				} else {
					HEAPfree(b->tvheap, true);
					GDKfree(b->tvheap);
				}
				BBPshare(n->tvheap->parentid);
				b->tvheap = n->tvheap;
				b->batDirtydesc = true;
				toff = 0;
			} else if (b->tvheap->parentid == n->tvheap->parentid &&
				   ci.tpe == cand_dense) {
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
				p = canditer_idx(&ci, p) - n->hseqbase;
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
				toff = oldcnt == 0 ? 0 : b->tvheap->free;
				/* make sure we get alignment right */
				toff = (toff + GDK_VARALIGN - 1) & ~(GDK_VARALIGN - 1);
				/* if in "force" mode, the heap may be
				 * shared when memory mapped */
				if (HEAPextend(b->tvheap, toff + n->tvheap->size, force) != GDK_SUCCEED) {
					toff = ~(size_t) 0;
					return GDK_FAIL;
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
				if (GDKupgradevarheap(b, (var_t) b->tvheap->size, false, force) != GDK_SUCCEED) {
					toff = ~(size_t) 0;
					return GDK_FAIL;
				}
			}
		}
	} else if (unshare_string_heap(b) != GDK_SUCCEED)
		return GDK_FAIL;
	if (toff == 0 && n->twidth == b->twidth && ci.tpe == cand_dense) {
		/* we don't need to do any translation of offset
		 * values, so we can use fast memcpy */
		memcpy(Tloc(b, BUNlast(b)), Tloc(n, ci.seq - n->hseqbase), cnt << n->tshift);
		BATsetcount(b, oldcnt + cnt);
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
		b->tvarsized = false;
		while (cnt > 0) {
			cnt--;
			p = canditer_next(&ci) - n->hseqbase;
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
			if (BUNlast(b) >= BATcapacity(b)) {
				if (BATcount(b) == BUN_MAX) {
					GDKerror("bunfastapp: too many elements to accomodate (" BUNFMT ")\n", BUN_MAX);
					goto bunins_failed;
				}
				if (BATextend(b, BATgrows(b)) != GDK_SUCCEED)
					goto bunins_failed;
			}
			switch (b->twidth) {
			case 1:
				assert(v - GDK_VAROFFSET < ((var_t) 1 << 8));
				((uint8_t *) b->theap.base)[b->batCount++] = (uint8_t) (v - GDK_VAROFFSET);
				b->theap.free += 1;
				break;
			case 2:
				assert(v - GDK_VAROFFSET < ((var_t) 1 << 16));
				((uint16_t *) b->theap.base)[b->batCount++] = (uint16_t) (v - GDK_VAROFFSET);
				b->theap.free += 2;
				break;
#if SIZEOF_VAR_T == 8
			case 4:
				assert(v < ((var_t) 1 << 32));
				((uint32_t *) b->theap.base)[b->batCount++] = (uint32_t) v;
				b->theap.free += 4;
				break;
#endif
			default:
				((var_t *) b->theap.base)[b->batCount++] = v;
				b->theap.free += sizeof(var_t);
				break;
			}
		}
		b->tvarsized = true;
		b->ttype = TYPE_str;
	} else if (b->tvheap->free < n->tvheap->free / 2 ||
		   GDK_ELIMDOUBLES(b->tvheap)) {
		/* if b's string heap is much smaller than n's string
		 * heap, don't bother checking whether n's string
		 * values occur in b's string heap; also, if b is
		 * (still) fully double eliminated, we must continue
		 * to use the double elimination mechanism */
		r = BUNlast(b);
		oid hseq = n->hseqbase;
		while (cnt > 0) {
			cnt--;
			p = canditer_next(&ci) - hseq;
			tp = BUNtvar(ni, p);
			if (bunfastappVAR(b, tp) != GDK_SUCCEED)
				goto bunins_failed;
			r++;
		}
	} else {
		/* Insert values from n individually into b; however,
		 * we check whether there is a string in b's string
		 * heap at the same offset as the string is in n's
		 * string heap (in case b's string heap is a copy of
		 * n's).  If this is the case, we just copy the
		 * offset, otherwise we insert normally.  */
		r = BUNlast(b);
		while (cnt > 0) {
			cnt--;
			p = canditer_next(&ci) - n->hseqbase;
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
					if (GDKupgradevarheap(b, v, false, force) != GDK_SUCCEED) {
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
				if (bunfastappVAR(b, tp) != GDK_SUCCEED)
					goto bunins_failed;
			}
			r++;
		}
	}
	b->theap.dirty = true;
	/* maintain hash */
	for (r = oldcnt, cnt = BATcount(b); b->thash && r < cnt; r++) {
		HASHins(b, r, Tbase(b) + VarHeapVal(b->theap.base, r, b->twidth));
	}
	return GDK_SUCCEED;
      bunins_failed:
	b->tvarsized = true;
	b->ttype = TYPE_str;
	return GDK_FAIL;
}

static gdk_return
append_varsized_bat(BAT *b, BAT *n, BAT *s)
{
	BATiter ni;
	struct canditer ci;
	BUN cnt, r;
	oid hseq = n->hseqbase;

	/* only transient bats can use some other bat's vheap */
	assert(b->batRole == TRANSIENT || b->tvheap->parentid == b->batCacheid);
	/* make sure the bats use var_t */
	assert(b->twidth == n->twidth);
	assert(b->twidth == SIZEOF_VAR_T);
	if (n->batCount == 0 || (s && s->batCount == 0))
		return GDK_SUCCEED;
	cnt = canditer_init(&ci, n, s);
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
			HEAPfree(b->tvheap, true);
			GDKfree(b->tvheap);
		}
		BBPshare(n->tvheap->parentid);
		b->tvheap = n->tvheap;
		b->batDirtydesc = true;
	}
	if (b->tvheap == n->tvheap) {
		/* if b and n use the same vheap, we only need to copy
		 * the offsets from n to b */
		if (ci.tpe == cand_dense) {
			/* fast memcpy since we copy a consecutive
			 * chunk of memory */
			memcpy(Tloc(b, BUNlast(b)),
			       Tloc(n, ci.seq - hseq),
			       cnt << b->tshift);
		} else {
			var_t *restrict dst = (var_t *) Tloc(b, BUNlast(b));
			const var_t *restrict src = (const var_t *) Tloc(n, 0);
			while (cnt > 0) {
				cnt--;
				*dst++ = src[canditer_next(&ci) - hseq];
			}
		}
		b->theap.dirty = true;
		BATsetcount(b, BATcount(b) + ci.ncand);
		/* maintain hash table */
		for (BUN i = BATcount(b) - ci.ncand;
		     b->thash && i < BATcount(b);
		     i++) {
			HASHins(b, i, b->tvheap->base + ((var_t *) b->theap.base)[i]);
		}
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
		strconcat_len(h->filename, sizeof(h->filename),
			      BBP_physical(b->batCacheid), ".theap", NULL);
		if (HEAPcopy(h, b->tvheap) != GDK_SUCCEED) {
			HEAPfree(h, true);
			GDKfree(h);
			return GDK_FAIL;
		}
		BBPunshare(b->tvheap->parentid);
		b->tvheap = h;
	}
	/* copy data from n to b */
	ni = bat_iterator(n);
	r = BUNlast(b);
	while (cnt > 0) {
		cnt--;
		BUN p = canditer_next(&ci) - hseq;
		const void *t = BUNtvar(ni, p);
		if (bunfastapp_nocheckVAR(b, r, t, Tsize(b)) != GDK_SUCCEED)
			return GDK_FAIL;
		if (b->thash)
			HASHins(b, r, t);
		r++;
	}
	b->theap.dirty = true;
	return GDK_SUCCEED;
}

/* Append the contents of BAT n (subject to the optional candidate
 * list s) to BAT b.  If b is empty, b will get the seqbase of s if it
 * was passed in, and else the seqbase of n. */
gdk_return
BATappend(BAT *b, BAT *n, BAT *s, bool force)
{
	struct canditer ci;
	BUN cnt;
	BUN r;
	PROPrec *prop, *nprop;
	oid hseq = n->hseqbase;

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

	if (BATttype(b) != BATttype(n) &&
		ATOMtype(b->ttype) != ATOMtype(n->ttype)) {
			TRC_DEBUG(CHECK_, "Interpreting %s as %s.\n",
									ATOMname(BATttype(n)), ATOMname(BATttype(b)));
		}

	cnt = canditer_init(&ci, n, s);
	if (cnt == 0) {
		return GDK_SUCCEED;
	}

	if (BUNlast(b) + cnt > BUN_MAX) {
		GDKerror("BATappend: combined BATs too large\n");
		return GDK_FAIL;
	}

	if (b->hseqbase + BATcount(b) + cnt >= GDK_oid_max) {
		GDKerror("BATappend: overflow of head value\n");
		return GDK_FAIL;
	}

	b->batDirtydesc = true;

	IMPSdestroy(b);		/* imprints do not support updates yet */
	OIDXdestroy(b);
	if ((prop = BATgetprop(b, GDK_MAX_VALUE)) != NULL) {
		if ((nprop = BATgetprop(n, GDK_MAX_VALUE)) != NULL) {
			if (ATOMcmp(b->ttype, VALptr(&prop->v), VALptr(&nprop->v)) < 0) {
				if (s == NULL)
					BATsetprop(b, GDK_MAX_VALUE, b->ttype, VALptr(&nprop->v));
				else
					BATrmprop(b, GDK_MAX_VALUE);
			}
		} else {
			BATrmprop(b, GDK_MAX_VALUE);
		}
	}
	if ((prop = BATgetprop(b, GDK_MIN_VALUE)) != NULL) {
		if ((nprop = BATgetprop(n, GDK_MIN_VALUE)) != NULL) {
			if (ATOMcmp(b->ttype, VALptr(&prop->v), VALptr(&nprop->v)) > 0) {
				if (s == NULL)
					BATsetprop(b, GDK_MIN_VALUE, b->ttype, VALptr(&nprop->v));
				else
					BATrmprop(b, GDK_MIN_VALUE);
			}
		} else {
			BATrmprop(b, GDK_MIN_VALUE);
		}
	}
	BATrmprop(b, GDK_NUNIQUE);
#if 0		/* enable if we have more properties than just min/max */
	do {
		for (prop = b->tprops; prop; prop = prop->next)
			if (prop->id != GDK_MAX_VALUE &&
			    prop->id != GDK_MIN_VALUE &&
			    prop->id != GDK_HASH_BUCKETS) {
				BATrmprop(b, prop->id);
				break;
			}
	} while (prop);
#endif
	/* load hash so that we can maintain it */
	(void) BATcheckhash(b);

	if (b->ttype == TYPE_void) {
		/* b does not have storage, keep it that way if we can */
		HASHdestroy(b);	/* we're not maintaining the hash here */
		if (BATtdense(n) && ci.tpe == cand_dense &&
		    (BATcount(b) == 0 ||
		     (BATtdense(b) &&
		      b->tseqbase + BATcount(b) == n->tseqbase + ci.seq - hseq))) {
			/* n is also dense and consecutive with b */
			if (BATcount(b) == 0)
				BATtseqbase(b, n->tseqbase + ci.seq - hseq);
			BATsetcount(b, BATcount(b) + cnt);
			return GDK_SUCCEED;
		}
		if ((BATcount(b) == 0 || is_oid_nil(b->tseqbase)) &&
		    n->ttype == TYPE_void && is_oid_nil(n->tseqbase)) {
			/* both b and n are void/nil */
			BATtseqbase(b, oid_nil);
			BATsetcount(b, BATcount(b) + cnt);
			return GDK_SUCCEED;
		}
		/* we need to materialize b; allocate enough capacity */
		b->batCapacity = BATcount(b) + cnt;
		if (BATmaterialize(b) != GDK_SUCCEED)
			return GDK_FAIL;
	} else if (cnt > BATcapacity(b) - BATcount(b)) {
		/* if needed space exceeds a normal growth extend just
		 * with what's needed */
		BUN ncap = BATcount(b) + cnt;
		BUN grows = BATgrows(b);

		if (ncap > grows)
			grows = ncap;
		if (BATextend(b, grows) != GDK_SUCCEED)
			return GDK_FAIL;
	}

	r = BUNlast(b);

	if (BATcount(b) == 0) {
		b->tsorted = n->tsorted;
		b->trevsorted = n->trevsorted;
		b->tseqbase = oid_nil;
		b->tnonil = n->tnonil;
		b->tnil = n->tnil && cnt == BATcount(n);
		b->tseqbase = oid_nil;
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
		BATiter ni = bat_iterator(n);
		BATiter bi = bat_iterator(b);
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
		b->tnil |= n->tnil && cnt == BATcount(n);
	}
	if (b->ttype == TYPE_str) {
		if (insert_string_bat(b, n, s, force) != GDK_SUCCEED) {
			return GDK_FAIL;
		}
	} else if (ATOMvarsized(b->ttype)) {
		if (append_varsized_bat(b, n, s) != GDK_SUCCEED) {
			return GDK_FAIL;
		}
	} else {
		if (BATatoms[b->ttype].atomFix == NULL &&
		    b->ttype != TYPE_void &&
		    n->ttype != TYPE_void &&
		    ci.tpe == cand_dense) {
			/* use fast memcpy if we can */
			memcpy(Tloc(b, BUNlast(b)),
			       Tloc(n, ci.seq - hseq),
			       cnt * Tsize(n));
			for (BUN i = 0; b->thash && i < cnt; i++) {
				HASHins(b, r, b->theap.base + r * b->twidth);
				r++;
			}
			BATsetcount(b, BATcount(b) + cnt);
		} else {
			BATiter ni = bat_iterator(n);

			while (cnt > 0) {
				cnt--;
				BUN p = canditer_next(&ci) - hseq;
				const void *t = BUNtail(ni, p);
				if (bunfastapp_nocheck(b, r, t, Tsize(b)) != GDK_SUCCEED)
					return GDK_FAIL;
				if (b->thash)
					HASHins(b, r, t);
				r++;
			}
		}
		b->theap.dirty = true;
	}
	if (b->thash)
		BATsetprop(b, GDK_NUNIQUE, TYPE_oid, &(oid){b->thash->nunique});
	return GDK_SUCCEED;
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
		b->tkey = true;
		b->tsorted = b->trevsorted = true;
		if (b->batCount == 0) {
			b->tnil = false;
			b->tnonil = true;
		}
	}
	/* not sure about these anymore */
	b->tnosorted = b->tnorevsorted = 0;
	b->tnokey[0] = b->tnokey[1] = 0;
	PROPdestroy(b);

	return GDK_SUCCEED;
}

/*
 * The last in this series is a BATreplace, which replaces all the
 * buns mentioned.
 */
gdk_return
BATreplace(BAT *b, BAT *p, BAT *n, bool force)
{
	lng t0 = GDKusec();

	if (b == NULL || b->ttype == TYPE_void || p == NULL || n == NULL) {
		return GDK_SUCCEED;
	}
	if (BATcount(p) != BATcount(n)) {
		GDKerror("BATreplace: update BATs not the same size\n");
		return GDK_FAIL;
	}
	if (ATOMtype(p->ttype) != TYPE_oid) {
		GDKerror("BATreplace: positions BAT not type OID\n");
		return GDK_FAIL;
	}
	if (BATcount(n) == 0) {
		return GDK_SUCCEED;
	}
	if (!force && (b->batRestricted != BAT_WRITE || b->batSharecnt > 0)) {
		GDKerror("BATreplace: access denied to %s, aborting.\n",
			 BATgetId(b));
		return GDK_FAIL;
	}

	HASHdestroy(b);
	OIDXdestroy(b);
	IMPSdestroy(b);
	BATrmprop(b, GDK_NUNIQUE);

	b->tsorted = b->trevsorted = false;
	b->tnosorted = b->tnorevsorted = 0;
	b->tseqbase = oid_nil;
	b->tkey = false;
	b->tnokey[0] = b->tnokey[1] = 0;

	const PROPrec *maxprop = BATgetprop(b, GDK_MAX_VALUE);
	const PROPrec *minprop = BATgetprop(b, GDK_MIN_VALUE);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(b->ttype);
	const void *nil = ATOMnilptr(b->ttype);
	oid hseqend = b->hseqbase + BATcount(b);
	BATiter bi = bat_iterator(b);
	BATiter ni = bat_iterator(n);
	bool anynil = false;

	b->theap.dirty = true;
	if (b->tvarsized) {
		b->tvheap->dirty = true;
		for (BUN i = 0, j = BATcount(p); i < j; i++) {
			oid updid = BUNtoid(p, i);

			if (updid < b->hseqbase || updid >= hseqend) {
				GDKerror("BATreplace: id out of range\n");
				return GDK_FAIL;
			}
			updid -= b->hseqbase;
			if (!force && updid < b->batInserted) {
				GDKerror("BATreplace: updating committed value\n");
				return GDK_FAIL;
			}

			const void *old = BUNtvar(bi, updid);
			const void *new = BUNtvar(ni, i);
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
			if (maxprop) {
				if (!isnil &&
				    atomcmp(VALptr(&maxprop->v), new) < 0) {
					/* new value is larger than
					 * previous largest */
					BATsetprop(b, GDK_MAX_VALUE, b->ttype, new);
					maxprop = BATgetprop(b, GDK_MAX_VALUE);
				} else if (atomcmp(VALptr(&maxprop->v), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * largest and new value is
					 * smaller, so we don't know
					 * anymore which is the
					 * largest */
					BATrmprop(b, GDK_MAX_VALUE);
					maxprop = NULL;
				}
			}
			if (minprop) {
				if (!isnil &&
				    atomcmp(VALptr(&minprop->v), new) > 0) {
					/* new value is smaller than
					 * previous smallest */
					BATsetprop(b, GDK_MIN_VALUE, b->ttype, new);
					minprop = BATgetprop(b, GDK_MIN_VALUE);
				} else if (atomcmp(VALptr(&minprop->v), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * smallest and new value is
					 * larger, so we don't know
					 * anymore which is the
					 * smallest */
					BATrmprop(b, GDK_MIN_VALUE);
					minprop = NULL;
				}
			}

			var_t d;
			switch (b->twidth) {
			default: /* only three of four cases possible */
				d = (var_t) ((uint8_t *) b->theap.base)[updid] + GDK_VAROFFSET;
				break;
			case 2:
				d = (var_t) ((uint16_t *) b->theap.base)[updid] + GDK_VAROFFSET;
				break;
			case 4:
				d = (var_t) ((uint32_t *) b->theap.base)[updid];
				break;
#if SIZEOF_VAR_T == 8
			case 8:
				d = (var_t) ((uint64_t *) b->theap.base)[updid];
				break;
#endif
			}
			if (ATOMreplaceVAR(b->ttype, b->tvheap, &d, new) != GDK_SUCCEED)
				return GDK_FAIL;
			if (b->twidth < SIZEOF_VAR_T &&
			    (b->twidth <= 2 ? d - GDK_VAROFFSET : d) >= ((size_t) 1 << (8 * b->twidth))) {
				/* doesn't fit in current heap, upgrade it */
				if (GDKupgradevarheap(b, d, false, b->batRestricted == BAT_READ) != GDK_SUCCEED)
					return GDK_FAIL;
			}
			switch (b->twidth) {
			case 1:
				((uint8_t *) b->theap.base)[updid] = (uint8_t) (d - GDK_VAROFFSET);
				break;
			case 2:
				((uint16_t *) b->theap.base)[updid] = (uint16_t) (d - GDK_VAROFFSET);
				break;
			case 4:
				((uint32_t *) b->theap.base)[updid] = (uint32_t) d;
				break;
#if SIZEOF_VAR_T == 8
			case 8:
				((uint64_t *) b->theap.base)[updid] = (uint64_t) d;
				break;
#endif
			}
		}
	} else if (BATtdense(p)) {
		oid updid = BUNtoid(p, 0);

		if (updid < b->hseqbase || updid + BATcount(p) > hseqend) {
			GDKerror("BATreplace: id out of range\n");
			return GDK_FAIL;
		}
		updid -= b->hseqbase;
		if (!force && updid < b->batInserted) {
			GDKerror("BATreplace: updating committed value\n");
			return GDK_FAIL;
		}

		/* we copy all of n, so if there are nils in n we get
		 * nils in b (and else we don't know) */
		b->tnil = n->tnil;
		/* we may not copy over all of b, so we only know that
		 * there are no nils in b afterward if there weren't
		 * any in either b or n to begin with */
		b->tnonil &= n->tnonil;
		if (n->ttype == TYPE_void) {
			assert(b->ttype == TYPE_oid);
			oid *o = Tloc(b, updid);
			if (is_oid_nil(n->tseqbase)) {
				/* we may or may not overwrite the old
				 * min/max values */
				BATrmprop(b, GDK_MAX_VALUE);
				BATrmprop(b, GDK_MIN_VALUE);
				for (BUN i = 0, j = BATcount(p); i < j; i++)
					o[i] = oid_nil;
				b->tnil = true;
			} else {
				oid v = n->tseqbase;
				/* we know min/max of n, so we know
				 * the new min/max of b if those of n
				 * are smaller/larger than the old */
				if (minprop && v <= minprop->v.val.oval)
					BATsetprop(b, GDK_MIN_VALUE, TYPE_oid, &v);
				else
					BATrmprop(b, GDK_MIN_VALUE);
				for (BUN i = 0, j = BATcount(p); i < j; i++)
					o[i] = v++;
				if (maxprop && --v >= maxprop->v.val.oval)
					BATsetprop(b, GDK_MAX_VALUE, TYPE_oid, &v);
				else
					BATrmprop(b, GDK_MAX_VALUE);
			}
		} else {
			/* if the extremes of n are at least as
			 * extreme as those of b, we can replace b's
			 * min/max, else we don't know what b's new
			 * min/max are*/
			PROPrec *prop;
			if (maxprop != NULL &&
			    (prop = BATgetprop(n, GDK_MAX_VALUE)) != NULL &&
			    atomcmp(VALptr(&maxprop->v), VALptr(&prop->v)) <= 0)
				BATsetprop(b, GDK_MAX_VALUE, b->ttype, VALptr(&prop->v));
			else
				BATrmprop(b, GDK_MAX_VALUE);
			if (minprop != NULL &&
			    (prop = BATgetprop(n, GDK_MIN_VALUE)) != NULL &&
			    atomcmp(VALptr(&minprop->v), VALptr(&prop->v)) >= 0)
				BATsetprop(b, GDK_MIN_VALUE, b->ttype, VALptr(&prop->v));
			else
				BATrmprop(b, GDK_MIN_VALUE);
			memcpy(Tloc(b, updid), Tloc(n, 0),
			       BATcount(p) * b->twidth);
		}
		if (BATcount(p) == BATcount(b)) {
			/* if we replaced all values of b by values
			 * from n, we can also copy the min/max
			 * properties */
			if ((minprop = BATgetprop(n, GDK_MIN_VALUE)) != NULL)
				BATsetprop(b, GDK_MIN_VALUE, b->ttype, VALptr(&minprop->v));
			if ((maxprop = BATgetprop(n, GDK_MAX_VALUE)) != NULL)
				BATsetprop(b, GDK_MAX_VALUE, b->ttype, VALptr(&maxprop->v));
			if (BATtdense(n)) {
				/* replaced all of b with a dense sequence */
				BATtseqbase(b, n->tseqbase);
			}
		}
	} else {
		for (BUN i = 0, j = BATcount(p); i < j; i++) {
			oid updid = BUNtoid(p, i);

			if (updid < b->hseqbase || updid >= hseqend) {
				GDKerror("BATreplace: id out of range\n");
				return GDK_FAIL;
			}
			updid -= b->hseqbase;
			if (!force && updid < b->batInserted) {
				GDKerror("BATreplace: updating committed value\n");
				return GDK_FAIL;
			}

			const void *old = BUNtloc(bi, updid);
			const void *new = BUNtail(ni, i);
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
			if (maxprop) {
				if (!isnil &&
				    atomcmp(VALptr(&maxprop->v), new) < 0) {
					/* new value is larger than
					 * previous largest */
					BATsetprop(b, GDK_MAX_VALUE, b->ttype, new);
					maxprop = BATgetprop(b, GDK_MAX_VALUE);
				} else if (atomcmp(VALptr(&maxprop->v), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * largest and new value is
					 * smaller, so we don't know
					 * anymore which is the
					 * largest */
					BATrmprop(b, GDK_MAX_VALUE);
					maxprop = NULL;
				}
			}
			if (minprop) {
				if (!isnil &&
				    atomcmp(VALptr(&minprop->v), new) > 0) {
					/* new value is smaller than
					 * previous smallest */
					BATsetprop(b, GDK_MIN_VALUE, b->ttype, new);
					minprop = BATgetprop(b, GDK_MIN_VALUE);
				} else if (atomcmp(VALptr(&minprop->v), old) == 0 &&
					   atomcmp(new, old) != 0) {
					/* old value is equal to
					 * smallest and new value is
					 * larger, so we don't know
					 * anymore which is the
					 * smallest */
					BATrmprop(b, GDK_MIN_VALUE);
					minprop = NULL;
				}
			}

			switch (b->twidth) {
			case 1:
				((bte *) b->theap.base)[updid] = * (bte *) new;
				break;
			case 2:
				((sht *) b->theap.base)[updid] = * (sht *) new;
				break;
			case 4:
				((int *) b->theap.base)[updid] = * (int *) new;
				break;
			case 8:
				((lng *) b->theap.base)[updid] = * (lng *) new;
				break;
#ifdef HAVE_HGE
			case 16:
				((hge *) b->theap.base)[updid] = * (hge *) new;
				break;
#endif
			default:
				memcpy(BUNtloc(bi, updid), new, ATOMsize(b->ttype));
				break;
			}
		}
	}
	TRC_DEBUG(ALGO,
		  "BATreplace(" ALGOBATFMT "," ALGOBATFMT "," ALGOBATFMT ") " LLFMT " usec\n",
		  ALGOBATPAR(b), ALGOBATPAR(p), ALGOBATPAR(n),
		  GDKusec() - t0);
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

	if (b->ttype == TYPE_void && b->tvheap != NULL) {
		/* slicing a candidate list with exceptions */
		struct canditer ci;
		canditer_init(&ci, NULL, b);
		return canditer_slice(&ci, l, h);
	}
	/* If the source BAT is readonly, then we can obtain a VIEW
	 * that just reuses the memory of the source. */
	if (b->batRestricted == BAT_READ &&
	    (!VIEWtparent(b) ||
	     BBP_cache(VIEWtparent(b))->batRestricted == BAT_READ)) {
		bn = VIEWcreate(b->hseqbase + low, b);
		if (bn == NULL)
			return NULL;
		VIEWbounds(b, bn, l, h);
	} else {
		/* create a new BAT and put everything into it */
		BUN p = l;
		BUN q = h;

		bn = COLnew((oid) (b->hseqbase + low), BATtdense(b) ? TYPE_void : b->ttype, h - l, TRANSIENT);
		if (bn == NULL)
			return NULL;

		if (bn->ttype == TYPE_void ||
		    (!bn->tvarsized &&
		     BATatoms[bn->ttype].atomPut == NULL &&
		     BATatoms[bn->ttype].atomFix == NULL)) {
			if (bn->ttype) {
				memcpy(Tloc(bn, 0), Tloc(b, p),
				       (q - p) * Tsize(bn));
				bn->theap.dirty = true;
			}
			BATsetcount(bn, h - l);
		} else {
			for (; p < q; p++) {
				if (bunfastapp(bn, BUNtail(bi, p)) != GDK_SUCCEED) {
					BBPreclaim(bn);
					return NULL;
				}
			}
		}
		bn->theap.dirty = true;
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
	bni = bat_iterator(bn);
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
	TRC_DEBUG(ALGO, "BATslice(" ALGOBATFMT "," BUNFMT "," BUNFMT ")"
			  	"=" ALGOBATFMT "\n",
			  	ALGOBATPAR(b), l, h, ALGOBATPAR(bn));
	return bn;
}

/* Return whether the BAT has all unique values or not.  It we don't
 * know, invest in a proper check and record the results in the bat
 * descriptor.  */
bool
BATkeyed(BAT *b)
{
	lng t0 = GDKusec();
	BATiter bi = bat_iterator(b);
	int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
	BUN p, q, hb;
	Hash *hs = NULL;

	if (b->ttype == TYPE_void)
		return BATtdense(b) || BATcount(b) <= 1;
	if (BATcount(b) <= 1)
		return true;
	if (b->twidth < SIZEOF_BUN &&
	    BATcount(b) > (BUN) 1 << (8 * b->twidth)) {
		/* more rows than possible bit combinations in the atom */
		assert(!b->tkey);
		return false;
	}

	b->batDirtydesc = true;
	if (!b->tkey && b->tnokey[0] == 0 && b->tnokey[1] == 0) {
		if (b->tsorted || b->trevsorted) {
			const void *prev = BUNtail(bi, 0);
			const void *cur;
			for (q = BUNlast(b), p = 1; p < q; p++) {
				cur = BUNtail(bi, p);
				if ((*cmpf)(prev, cur) == 0) {
					b->tnokey[0] = p - 1;
					b->tnokey[1] = p;
					TRC_DEBUG(ALGO, "Fixed nokey(" BUNFMT "," BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p - 1, p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				}
				prev = cur;
			}
			/* we completed the scan: no duplicates */
			b->tkey = true;
		} else if (BATcheckhash(b) ||
			   (!b->batTransient &&
			    BAThash(b) == GDK_SUCCEED) ||
			   (VIEWtparent(b) != 0 &&
			    BATcheckhash(BBPdescriptor(VIEWtparent(b))))) {
			/* we already have a hash table on b, or b is
			 * persistent and we could create a hash
			 * table, or b is a view on a bat that already
			 * has a hash table */
			BUN lo = 0;

			hs = b->thash;
			if (hs == NULL && VIEWtparent(b) != 0) {
				BAT *b2 = BBPdescriptor(VIEWtparent(b));
				lo = (BUN) ((b->theap.base - b2->theap.base) >> b->tshift);
				hs = b2->thash;
			}
			for (q = BUNlast(b), p = 0; p < q; p++) {
				const void *v = BUNtail(bi, p);
				for (hb = HASHgetlink(hs, p + lo);
				     hb != HASHnil(hs) && hb >= lo;
				     hb = HASHgetlink(hs, hb)) {
					assert(hb < p + lo);
					if ((*cmpf)(v, BUNtail(bi, hb - lo)) == 0) {
						b->tnokey[0] = hb - lo;
						b->tnokey[1] = p;
						TRC_DEBUG(ALGO, "Fixed nokey(" BUNFMT "," BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", hb - lo, p, BATgetId(b), BATcount(b), GDKusec() - t0);
						goto doreturn;
					}
				}
			}
			/* we completed the scan: no duplicates */
			b->tkey = true;
		} else {
			const char *nme;
			BUN prb;
			BUN mask;

			GDKclrerr(); /* not interested in BAThash errors */
			nme = BBP_physical(b->batCacheid);
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
			if ((hs = GDKzalloc(sizeof(Hash))) == NULL)
				goto doreturn;
			if (snprintf(hs->heaplink.filename, sizeof(hs->heaplink.filename), "%s.thshkeyl%x", nme, THRgettid()) >= (int) sizeof(hs->heaplink.filename) ||
			    snprintf(hs->heapbckt.filename, sizeof(hs->heapbckt.filename), "%s.thshkeyb%x", nme, THRgettid()) >= (int) sizeof(hs->heapbckt.filename) ||
			    HASHnew(hs, b->ttype, BUNlast(b), mask, BUN_NONE, false) != GDK_SUCCEED) {
				GDKfree(hs);
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
						TRC_DEBUG(ALGO, "Fixed nokey(" BUNFMT "," BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", hb, p, BATgetId(b), BATcount(b), GDKusec() - t0);
						goto doreturn_free;
					}
				}
				/* enter into hash table */
				HASHputlink(hs, p, HASHget(hs, prb));
				HASHput(hs, prb, p);
			}
		  doreturn_free:
			HEAPfree(&hs->heaplink, true);
			HEAPfree(&hs->heapbckt, true);
			GDKfree(hs);
			if (p == q) {
				/* we completed the complete scan: no
				 * duplicates */
				b->tkey = true;
			}
		}
	}
  doreturn:
	return b->tkey;
}

/* Return whether the BAT is ordered or not.  If we don't know, invest
 * in a scan and record the results in the bat descriptor.  If during
 * the scan we happen to find evidence that the BAT is not reverse
 * sorted, we record the location.  */
bool
BATordered(BAT *b)
{
	lng t0 = GDKusec();

	if (b->ttype == TYPE_void || b->tsorted)
		return true;
	if (b->tnosorted > 0)
		return false;

	/* In order that multiple threads don't scan the same BAT at
	 * the same time (happens a lot with mitosis/mergetable), we
	 * use a lock.  We reuse the hash lock for this, not because
	 * this scanning interferes with hashes, but because it's
	 * there, and not so likely to be used at the same time. */
	MT_lock_set(&b->batIdxLock);
	if (!b->tsorted && b->tnosorted == 0) {
		BATiter bi = bat_iterator(b);
		int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
		BUN p, q;
		b->batDirtydesc = true;
		switch (ATOMbasetype(b->ttype)) {
		case TYPE_int: {
			const int *iptr = (const int *) Tloc(b, 0);
			for (q = BUNlast(b), p = 1; p < q; p++) {
				if (iptr[p - 1] > iptr[p]) {
					b->tnosorted = p;
					TRC_DEBUG(ALGO, "Fixed nosorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				} else if (!b->trevsorted &&
					   b->tnorevsorted == 0 &&
					   iptr[p - 1] < iptr[p]) {
					b->tnorevsorted = p;
					TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for %s#" BUNFMT "\n", p, BATgetId(b), BATcount(b));
				}
			}
			break;
		}
		case TYPE_lng: {
			const lng *lptr = (const lng *) Tloc(b, 0);
			for (q = BUNlast(b), p = 1; p < q; p++) {
				if (lptr[p - 1] > lptr[p]) {
					b->tnosorted = p;
					TRC_DEBUG(ALGO, "Fixed nosorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				} else if (!b->trevsorted &&
					   b->tnorevsorted == 0 &&
					   lptr[p - 1] < lptr[p]) {
					b->tnorevsorted = p;
					TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for %s#" BUNFMT "\n", p, BATgetId(b), BATcount(b));
				}
			}
			break;
		}
		default:
			for (q = BUNlast(b), p = 1; p < q; p++) {
				int c;
				if ((c = cmpf(BUNtail(bi, p - 1), BUNtail(bi, p))) > 0) {
					b->tnosorted = p;
					TRC_DEBUG(ALGO, "Fixed nosorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
					goto doreturn;
				} else if (!b->trevsorted &&
					   b->tnorevsorted == 0 &&
					   c < 0) {
					b->tnorevsorted = p;
					TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for %s#" BUNFMT "\n", p, BATgetId(b), BATcount(b));
				}
			}
			break;
		}
		/* we only get here if we completed the scan; note
		 * that if we didn't record evidence about *reverse*
		 * sortedness, we know that the BAT is also reverse
		 * sorted */
		b->tsorted = true;
		TRC_DEBUG(ALGO, "Fixed sorted for %s#" BUNFMT " (" LLFMT " usec)\n", BATgetId(b), BATcount(b), GDKusec() - t0);
		if (!b->trevsorted && b->tnorevsorted == 0) {
			b->trevsorted = true;
			TRC_DEBUG(ALGO, "Fixed revsorted for %s#" BUNFMT "\n", BATgetId(b), BATcount(b));
		}
	}
  doreturn:
	MT_lock_unset(&b->batIdxLock);
	return b->tsorted;
}

/* Return whether the BAT is reverse ordered or not.  If we don't
 * know, invest in a scan and record the results in the bat
 * descriptor.  */
bool
BATordered_rev(BAT *b)
{
	lng t0 = GDKusec();

	if (b == NULL)
		return false;
	if (BATcount(b) <= 1 || b->trevsorted)
		return true;
	if (b->ttype == TYPE_void)
		return is_oid_nil(b->tseqbase);
	if (BATtdense(b) || b->tnorevsorted > 0)
		return false;
	MT_lock_set(&b->batIdxLock);
	if (!b->trevsorted && b->tnorevsorted == 0) {
		BATiter bi = bat_iterator(b);
		int (*cmpf)(const void *, const void *) = ATOMcompare(b->ttype);
		BUN p, q;
		b->batDirtydesc = true;
		for (q = BUNlast(b), p = 1; p < q; p++) {
			if (cmpf(BUNtail(bi, p - 1), BUNtail(bi, p)) < 0) {
				b->tnorevsorted = p;
				TRC_DEBUG(ALGO, "Fixed norevsorted(" BUNFMT ") for %s#" BUNFMT " (" LLFMT " usec)\n", p, BATgetId(b), BATcount(b), GDKusec() - t0);
				goto doreturn;
			}
		}
		b->trevsorted = true;
		TRC_DEBUG(ALGO, "Fixed revsorted for %s#" BUNFMT " (" LLFMT " usec)\n", BATgetId(b), BATcount(b), GDKusec() - t0);
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

	/* we haven't implemented NILs as largest value for stable
	 * sort, so NILs come first for ascending and last for
	 * descending */
	assert(!stable || reverse == nilslast);

	if (b == NULL) {
		GDKerror("BATsort: b must exist\n");
		return GDK_FAIL;
	}
	if (stable && reverse != nilslast) {
		GDKerror("BATsort: stable sort cannot have "
			 "reverse != nilslast\n");
		return GDK_FAIL;
	}
	if (!ATOMlinear(b->ttype)) {
		GDKerror("BATsort: type %s cannot be sorted\n",
			 ATOMname(b->ttype));
		return GDK_FAIL;
	}
	if (b->ttype == TYPE_void) {
		if (!b->tsorted) {
			b->tsorted = true;
			b->batDirtydesc = true;
		}
		if (b->trevsorted != is_oid_nil(b->tseqbase) || b->batCount <= 1) {
			b->trevsorted = !b->trevsorted;
			b->batDirtydesc = true;
		}
		if (b->tkey != !is_oid_nil(b->tseqbase)) {
			b->tkey = !b->tkey;
			b->batDirtydesc = true;
		}
	} else if (b->batCount <= 1) {
		if (!b->tsorted || !b->trevsorted) {
			b->tsorted = b->trevsorted = true;
			b->batDirtydesc = true;
		}
	}
	if (o != NULL &&
	    (ATOMtype(o->ttype) != TYPE_oid || /* oid tail */
	     BATcount(o) != BATcount(b) ||     /* same size as b */
	     (o->ttype == TYPE_void &&	       /* no nil tail */
	      BATcount(o) != 0 &&
	      is_oid_nil(o->tseqbase)))) {
		GDKerror("BATsort: o must have type oid and same size as b\n");
		return GDK_FAIL;
	}
	if (g != NULL &&
	    (ATOMtype(g->ttype) != TYPE_oid || /* oid tail */
	     !g->tsorted ||		       /* sorted */
	     BATcount(o) != BATcount(b) ||     /* same size as b */
	     (g->ttype == TYPE_void &&	       /* no nil tail */
	      BATcount(g) != 0 &&
	      is_oid_nil(g->tseqbase)))) {
		GDKerror("BATsort: g must have type oid, sorted on the tail, "
			 "and same size as b\n");
		return GDK_FAIL;
	}
	if (sorted == NULL && order == NULL) {
		/* no place to put result, so we're done quickly */
		GDKerror("BATsort: no place to put the result.\n");
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
		TRC_DEBUG(ALGO, "BATsort(b=" ALGOBATFMT ",o="
					ALGOOPTBATFMT ",g=" ALGOOPTBATFMT
					",reverse=%d,nilslast=%d,stable=%d) = ("
					ALGOOPTBATFMT "," ALGOOPTBATFMT ","
					ALGOOPTBATFMT ") -- trivial (" LLFMT
					" usec)\n",
					ALGOBATPAR(b), ALGOOPTBATPAR(o),
					ALGOOPTBATPAR(g), reverse, nilslast, stable,
					ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn),
					ALGOOPTBATPAR(on), GDKusec() - t0);
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
	/* when we will create an order index if it doesn't already exist */
	mkorderidx = (g == NULL && !reverse && !nilslast && pb != NULL && (order || !pb->batTransient));
	if (g == NULL && !reverse && !nilslast &&
	    pb != NULL && !BATcheckorderidx(pb)) {
		MT_lock_set(&pb->batIdxLock);
		if (pb->torderidx == NULL) {
			/* no index created while waiting for lock */
			if (mkorderidx) /* keep lock when going to create */
				orderidxlock = true;
		} else {
			/* no need to create an index: it already exists */
			mkorderidx = false;
		}
		if (!orderidxlock)
			MT_lock_unset(&pb->batIdxLock);
	} else {
		mkorderidx = false;
	}
	if (g == NULL && o == NULL && !reverse && !nilslast &&
	    pb != NULL && pb->torderidx != NULL &&
	    /* if we want a stable sort, the order index must be
	     * stable, if we don't want stable, we don't care */
	    (!stable || ((oid *) pb->torderidx->base)[2])) {
		/* there is an order index that we can use */
		on = COLnew(pb->hseqbase, TYPE_oid, BATcount(pb), TRANSIENT);
		if (on == NULL)
			goto error;
		memcpy(Tloc(on, 0), (oid *) pb->torderidx->base + ORDERIDXOFF, BATcount(pb) * sizeof(oid));
		BATsetcount(on, BATcount(b));
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
		TRC_DEBUG(ALGO, "BATsort(b=" ALGOBATFMT ",o="
					ALGOOPTBATFMT ",g=" ALGOOPTBATFMT
					",reverse=%d,nilslast=%d,stable=%d) = ("
					ALGOOPTBATFMT "," ALGOOPTBATFMT ","
					ALGOOPTBATFMT ") -- orderidx (" LLFMT
					" usec)\n",
					ALGOBATPAR(b), ALGOOPTBATPAR(o),
					ALGOOPTBATPAR(g), reverse, nilslast, stable,
					ALGOOPTBATPAR(bn), ALGOOPTBATPAR(gn),
					ALGOOPTBATPAR(on), GDKusec() - t0);
		return GDK_SUCCEED;
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
			TRC_DEBUG(ALGO, "BATsort(b=" ALGOBATFMT
						",o=" ALGOOPTBATFMT ",g=" ALGOBATFMT
						",reverse=%d,nilslast=%d,stable=%d"
						") = (" ALGOOPTBATFMT ","
						ALGOOPTBATFMT "," ALGOOPTBATFMT
						") -- key group (" LLFMT " usec)\n",
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
			if (orderidxlock)
				MT_lock_unset(&pb->batIdxLock);
			goto error;
		}
		bn->tsorted = !reverse && !nilslast;
		bn->trevsorted = reverse && nilslast;
		if (m != NULL) {
			assert(orderidxlock);
			if (pb->torderidx == NULL) {
				pb->batDirtydesc = true;
				if (ords != (oid *) m->base + ORDERIDXOFF) {
					memcpy((oid *) m->base + ORDERIDXOFF,
					       ords,
					       BATcount(pb) * sizeof(oid));
				}
				pb->torderidx = m;
				persistOIDX(pb);
			} else {
				HEAPfree(m, true);
				GDKfree(m);
			}
		}
	}
	if (orderidxlock)
		MT_lock_unset(&pb->batIdxLock);
	bn->theap.dirty = true;
	bn->tnosorted = 0;
	bn->tnorevsorted = 0;
	bn->tnokey[0] = bn->tnokey[1] = 0;
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

	TRC_DEBUG(ALGO, "BATsort(b=" ALGOBATFMT ",o=" ALGOOPTBATFMT
				",g=" ALGOOPTBATFMT ",reverse=%d,nilslast=%d,"
				"stable=%d) = (" ALGOOPTBATFMT "," ALGOOPTBATFMT ","
				ALGOOPTBATFMT ") -- %ssort (" LLFMT " usec)\n",
				ALGOBATPAR(b), ALGOOPTBATPAR(o), ALGOOPTBATPAR(g),
				reverse, nilslast, stable, ALGOOPTBATPAR(bn),
				ALGOOPTBATPAR(gn), ALGOOPTBATPAR(on),
				g ? "grouped " : "", GDKusec() - t0);
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
BATconstant(oid hseq, int tailtype, const void *v, BUN n, role_t role)
{
	BAT *bn;
	void *restrict p;
	BUN i;
	lng t0 = GDKusec();

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
				if (tfastins_nocheck(bn, i, v, Tsize(bn)) != GDK_SUCCEED) {
					BBPreclaim(bn);
					return NULL;
				}
			break;
		}
		bn->theap.dirty = true;
		bn->tnil = n >= 1 && (*ATOMcompare(tailtype))(v, ATOMnilptr(tailtype)) == 0;
		BATsetcount(bn, n);
		bn->tsorted = true;
		bn->trevsorted = true;
		bn->tnonil = !bn->tnil;
		bn->tkey = BATcount(bn) <= 1;
	}
	TRC_DEBUG(ALGO, "%s()=" ALGOOPTBATFMT
				" (" LLFMT "usec)\n",
				__func__,
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
PROPdestroy(BAT *b)
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

PROPrec *
BATgetprop_nolock(BAT *b, enum prop_t idx)
{
	PROPrec *p;

	p = b->tprops;
	while (p && p->id != idx)
		p = p->next;
	return p;
}

static void
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

void
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
			return;
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
	}
	b->batDirtydesc = true;
}

PROPrec *
BATgetprop(BAT *b, enum prop_t idx)
{
	PROPrec *p;

	MT_lock_set(&b->batIdxLock);
	p = BATgetprop_nolock(b, idx);
	MT_lock_unset(&b->batIdxLock);
	return p;
}

void
BATsetprop(BAT *b, enum prop_t idx, int type, const void *v)
{
	MT_lock_set(&b->batIdxLock);
	BATsetprop_nolock(b, idx, type, v);
	MT_lock_unset(&b->batIdxLock);
}

void
BATrmprop(BAT *b, enum prop_t idx)
{
	MT_lock_set(&b->batIdxLock);
	BATrmprop_nolock(b, idx);
	MT_lock_unset(&b->batIdxLock);
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
		cnt = n * BATtdense(b);
		break;
	case TYPE_bte:
		for (i = 0; i < n; i++)
			cnt += !is_bte_nil(((const bte *) p)[i]);
		break;
	case TYPE_sht:
		for (i = 0; i < n; i++)
			cnt += !is_sht_nil(((const sht *) p)[i]);
		break;
	case TYPE_int:
		for (i = 0; i < n; i++)
			cnt += !is_int_nil(((const int *) p)[i]);
		break;
	case TYPE_lng:
		for (i = 0; i < n; i++)
			cnt += !is_lng_nil(((const lng *) p)[i]);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		for (i = 0; i < n; i++)
			cnt += !is_hge_nil(((const hge *) p)[i]);
		break;
#endif
	case TYPE_flt:
		for (i = 0; i < n; i++)
			cnt += !is_flt_nil(((const flt *) p)[i]);
		break;
	case TYPE_dbl:
		for (i = 0; i < n; i++)
			cnt += !is_dbl_nil(((const dbl *) p)[i]);
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
		b->tnonil = true;
		assert(!b->tnil);
		b->tnil = false;
	}
	return cnt;
}
