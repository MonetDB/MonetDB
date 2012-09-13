/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"

static BAT *
newempty(void)
{
	BAT *bn = BATnew(TYPE_void, TYPE_void, 0);
	BATseqbase(bn, 0);
	BATseqbase(BATmirror(bn), 0);
	return bn;
}

static BAT *
BATslice2(BAT *b, BUN l1, BUN h1, BUN l2, BUN h2)
{
	BUN p, q;
	BAT *bn;
	BATiter bi = bat_iterator(b);
	int tt = b->ttype;

	BATcheck(b, "BATslice");
	if (h2 > BATcount(b))
		h2 = BATcount(b);
	if (h1 < l1)
		h1 = l1;
	if (h2 < l2)
		h2 = l2;
	l1 += BUNfirst(b);
	l2 += BUNfirst(b);
	h1 += BUNfirst(b);
	h2 += BUNfirst(b);

	if (l1 > BUN_MAX || l2 > BUN_MAX || h1 > BUN_MAX || h2 > BUN_MAX) {
		GDKerror("BATslice2: boundary out of range\n");
		return NULL;
	}

	if (tt == TYPE_void && b->T->seq != oid_nil)
		tt = TYPE_oid;
	bn = BATnew(ATOMtype(b->htype), tt, h1 - l1 + h2 - l2);
	if (bn == NULL)
		return bn;
	for (p = (BUN) l1, q = (BUN) h1; p < q; p++) {
		bunfastins(bn, BUNhead(bi, p), BUNtail(bi, p));
	}
	for (p = (BUN) l2, q = (BUN) h2; p < q; p++) {
		bunfastins(bn, BUNhead(bi, p), BUNtail(bi, p));
	}
	bn->hsorted = BAThordered(b);
	bn->tsorted = BATtordered(b);
	bn->hrevsorted = BAThrevordered(b);
	bn->trevsorted = BATtrevordered(b);
	BATkey(bn, BAThkey(b));
	BATkey(BATmirror(bn), BATtkey(b));
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	if (bn->hkey && bn->htype == TYPE_oid) {
		if (BATcount(bn) == 0) {
			bn->hdense = TRUE;
			BATseqbase(bn, 0);
		}
	}
	if (bn->tkey && bn->ttype == TYPE_oid) {
		if (BATcount(bn) == 0) {
			bn->tdense = TRUE;
			BATseqbase(BATmirror(bn), 0);
		}
	}
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/*
 * @-  Value Selections
 * The string search is optimized for the degenerated case that th =
 * tl, and double elimination in the string heap.
 *
 * We allow value selections on the nil atom. This is formally not
 * correct, as in MIL (nil = nil) != true.  However, we do need an
 * implementation for selecting nil (in MIL, this is done through is
 * the "isnil" predicate). So we implement it here.
 */
static BAT *
BAT_hashselect(BAT *b, BAT *s, BAT *bn, const void *tl)
{
	BATiter bi;
	BUN i;
	oid o;
	oid off;

	assert(bn->htype == TYPE_void);
	assert(bn->ttype == TYPE_oid);
	assert(BAThdense(b));
	off = b->hseqbase - b->U->first;
	b = BATmirror(b);	/* BATprepareHash works on HEAD column */
	if (BATprepareHash(b)) {
		BBPreclaim(bn);
		return NULL;
	}
	bi = bat_iterator(b);
	if (s) {
		assert(s->tsorted);
		s = BATmirror(s); /* SORTfnd works on HEAD column */
		HASHloop(bi, b->H->hash, i, tl) {
			o = (oid) i + off;
			if (SORTfnd(s, &o) != BUN_NONE)
				bunfastins(bn, NULL, &o);
		}
	} else {
		HASHloop(bi, b->H->hash, i, tl) {
			o = (oid) i + off;
			bunfastins(bn, NULL, &o);
		}
	}
	bn->tkey = 1;
	bn->tdense = bn->tsorted = bn->trevsorted = bn->U->count <= 1;
	if (bn->U->count == 1)
		bn->tseqbase =  * (oid *) Tloc(bn, BUNfirst(bn));
	/* temporarily set head to nil so that BATorder doesn't materialize */
	bn->hseqbase = oid_nil;
	bn->hkey = 0;
	bn->hsorted = bn->hrevsorted = 1;
	bn = BATmirror(BATorder(BATmirror(bn)));
	bn->hseqbase = 0;
	bn->hkey = 1;
	bn->hsorted = 1;
	bn->hrevsorted = bn->U->count <= 1;
	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/* scan select loop with candidates */
#define candscanloop(TEST)						\
	do {								\
		ALGODEBUG fprintf(stderr,				\
			    "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): " \
			    "scanselect %s\n", BATgetId(b), BATcount(b), \
			    s ? BATgetId(s) : "NULL", anti, #TEST);	\
		while (p < q) {						\
			o = *candlist++;				\
			r = (BUN) (o - off);				\
			v = BUNtail(bi, r);				\
			if (TEST)					\
				bunfastins(bn, NULL, &o);		\
			p++;						\
		}							\
	} while (0)

/* scan select loop without candidates */
#define scanloop(TEST)							\
	do {								\
		ALGODEBUG fprintf(stderr,				\
			    "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): " \
			    "scanselect %s\n", BATgetId(b), BATcount(b), \
			    s ? BATgetId(s) : "NULL", anti, #TEST);	\
		while (p < q) {						\
			v = BUNtail(bi, p);				\
			if (TEST) {					\
				o = (oid) p + off;			\
				bunfastins(bn, NULL, &o);		\
			}						\
			p++;						\
		}							\
	} while (0)

static BAT *
BAT_scanselect(BAT *b, BAT *s, BAT *bn, const void *tl, const void *th,
	       int li, int hi, int equi, int anti, int lval, int hval)
{
	BATiter bi = bat_iterator(b);
	int (*cmp)(const void *, const void *);
	BUN p, q;
	oid o, off;
	const void *nil, *v;
	int c;

	assert(b != NULL);
	assert(bn != NULL);
	assert(bn->htype == TYPE_void);
	assert(bn->ttype == TYPE_oid);
	assert(anti == 0 || anti == 1);
	assert(!lval || tl != NULL);
	assert(!hval || th != NULL);

	cmp = BATatoms[b->ttype].atomCmp;

	assert(!lval || !hval || (*cmp)(tl, th) <= 0);

	nil = b->T->nonil ? NULL : ATOMnilptr(b->ttype);
	off = b->hseqbase - BUNfirst(b);

	if (s && !BATtdense(s)) {
		const oid *candlist;
		BUN r;

		assert(s->tsorted);
		assert(s->tkey);
		/* setup candscanloop loop vars to only iterate over
		 * part of s that has values that are in range of b */
		o = b->hseqbase + BATcount(b);
		q = SORTfndfirst(s, &o);
		p = SORTfndfirst(s, &b->hseqbase);
		/* should we return an error if p > BUNfirst(s) || q <
		 * BUNlast(s) (i.e. s not fully used)? */
		candlist = (const oid *) Tloc(s, p);
		if (equi) {
			assert(li && hi);
			assert(!anti);
			candscanloop((*cmp)(tl, v) == 0);
		} else if (anti) {
			candscanloop((nil == NULL || (*cmp)(v, nil) != 0) &&
				     ((lval &&
				       ((c = (*cmp)(tl, v)) > 0 ||
					(!li && c == 0))) ||
				      (hval &&
				       ((c = (*cmp)(th, v)) < 0 ||
					(!hi && c == 0)))));
		} else {
			candscanloop((nil == NULL || (*cmp)(v, nil) != 0) &&
				     ((!lval ||
				       (c = cmp(tl, v)) < 0 ||
				       (li && c == 0)) &&
				      (!hval ||
				       (c = cmp(th, v)) > 0 ||
				       (hi && c == 0))));
		}
	} else {
		if (s) {
			assert(BATtdense(s));
			p = (BUN) s->tseqbase;
			q = p + BATcount(s);
			if ((oid) p < b->hseqbase)
				p = b->hseqbase;
			if ((oid) q > b->hseqbase + BATcount(b))
				q = b->hseqbase + BATcount(b);
			p += BUNfirst(b);
			q += BUNfirst(b);
		} else {
			p = BUNfirst(b);
			q = BUNlast(b);
		}
		if (equi) {
			assert(li && hi);
			assert(!anti);
			scanloop((*cmp)(tl, v) == 0);
		} else if (anti) {
			scanloop((nil == NULL || (*cmp)(v, nil) != 0) &&
				 ((lval &&
				   ((c = (*cmp)(tl, v)) > 0 ||
				    (!li && c == 0))) ||
				  (hval &&
				   ((c = (*cmp)(th, v)) < 0 ||
				    (!hi && c == 0)))));
		} else {
			scanloop((nil == NULL || (*cmp)(v, nil) != 0) &&
				 ((!lval ||
				   (c = cmp(tl, v)) < 0 ||
				   (li && c == 0)) &&
				  (!hval ||
				   (c = cmp(th, v)) > 0 ||
				   (hi && c == 0))));
		}
	}
	bn->tsorted = 1;
	bn->trevsorted = bn->U->count <= 1;
	bn->tkey = 1;
	bn->tdense = bn->U->count <= 1;
	if (bn->U->count == 1)
		bn->tseqbase =  * (oid *) Tloc(bn, BUNfirst(bn));
	bn->hsorted = 1;
	bn->hdense = 1;
	bn->hseqbase = 0;
	bn->hkey = 1;
	bn->hrevsorted = bn->U->count <= 1;

	return bn;

  bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

/* generic range select
 *
 * Return a dense-headed BAT with the OID values of b in the tail for
 * qualifying tuples.  The return BAT is sorted on the tail value
 * (i.e. in the same order as the input BAT).
 *
 * If s[dense,OID] is specified, its tail column is a list of
 * candidates.  s should be sorted on the tail value.
 *
 * tl may not be NULL, li, hi, and anti must be either 0 or 1.
 *
 * If th is NULL, hi is ignored.
 *
 * If anti is 0, qualifying tuples are those whose tail value is
 * between tl and th.  If li or hi is 1, the respective boundary is
 * inclusive, otherwise exclusive.  If th is NULL it is taken to be
 * equal to tl, turning this into an equi- or point-select.  Note that
 * for a point select to return anything, li (and hi if th was not
 * NULL) must be 1.  There is a special case if tl is nil and th is
 * NULL.  This is the only way to select for nil values.
 *
 * If anti is 1, the result is the complement of what the result would
 * be if anti were 0, except that nils are filtered out.
 *
 * In brief:
 * - if tl==nil and th==NULL and anti==0, return all nils (only way to
 *   get nils);
 * - it tl==nil and th==nil, return all but nils;
 * - if tl==nil and th!=NULL, no lower bound;
 * - if th==NULL or tl==th, point (equi) select;
 * - if th==nil, no upper bound
 */
BAT *
BATsubselect(BAT *b, BAT *s, const void *tl, const void *th, int li, int hi, int anti)
{
	int hval, lval, equi, t, lnil;
	const void *nil;
	BAT *bn;
	BUN estimate;

	BATcheck(b, "BATsubselect");
	BATcheck(tl, "BATsubselect: tl value required");

	assert(BAThdense(b));
	assert(s == NULL || BAThdense(s));
	assert(s == NULL || s->ttype == TYPE_oid || s->ttype == TYPE_void);
	assert(hi == 0 || hi == 1);
	assert(li == 0 || li == 1);
	assert(anti == 0 || anti == 1);

	if ((li != 0 && li != 1) ||
	    (hi != 0 && hi != 1) ||
	    (anti != 0 && anti != 1)) {
		GDKerror("BATsubselect: invalid arguments: "
			 "li, hi, anti must be 0 or 1\n");
		return NULL;
	}
	if (!BAThdense(b)) {
		GDKerror("BATsubselect: invalid argument: "
			 "b must have a dense head.\n");
		return NULL;
	}
	if (s && !BATtordered(s)) {
		GDKerror("BATsubselect: invalid argument: "
			 "s must be sorted.\n");
		return NULL;
	}

	if (b->U->count == 0 || (s && s->U->count == 0)) {
		/* trivially empty result */
		ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): trivially empty\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
		return newempty();
	}

	t = b->ttype;
	nil = ATOMnilptr(t);
	lnil = ATOMcmp(t, tl, nil) == 0; /* low value = nil? */
	lval = !lnil || th == NULL;	 /* low value used for comparison */
	equi = th == NULL || (lval && ATOMcmp(t, tl, th) == 0); /* point select? */
	if (equi) {
		assert(lval);
		if (th == NULL)
			hi = li;
		th = tl;
		hval = 1;
	} else {
		hval = ATOMcmp(t, th, nil) != 0;
	}
	if (anti) {
		if (lval != hval) {
			/* one of the end points is nil and the other
			 * isn't: swap sub-ranges */
			const void *tv;
			int ti;
			assert(!equi);
			ti = li;
			li = hi;
			hi = ti;
			tv = tl;
			tl = th;
			th = tv;
			ti = lval;
			lval = hval;
			hval = ti;
			lnil = ATOMcmp(t, tl, nil) == 0;
			anti = 0;
			ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): anti: switch ranges\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
		} else if (!lval && !hval) {
			/* antiselect for nil-nil range: all non-nil
			 * values are in range; we must return all
			 * other non-nil values, i.e. nothing */
			ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): anti: nil-nil range, nonil\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
			return newempty();
		} else if (equi && lnil) {
			/* antiselect for nil value: turn into range
			 * select for nil-nil range (i.e. everything
			 * but nil) */
			equi = 0;
			anti = 0;
			lval = 0;
			hval = 0;
			ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): anti-nil\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
		} else {
			equi = 0;
		}
	}

	assert(!equi || (lval && hval)); /* if equi set, then so are both lval and hval */

	if (hval && ((equi && !(li && hi)) || ATOMcmp(t, tl, th) > 0)) {
		/* empty range */
		ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): empty range\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
		return newempty();
	}
	if (equi && lnil && b->T->nonil) {
		/* return all nils, but there aren't any */
		ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): equi-nil, nonil\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
		return newempty();
	}

	if (!equi && !lval && !hval && lnil && b->T->nonil) {
		/* return all non-nils from a BAT that doesn't have
		 * any: i.e. return everything */
		ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): everything, nonil\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
		if (s) {
			return BATcopy(s, TYPE_void, s->ttype, 0);
		} else {
			return BATmirror(BATmark(b, 0));
		}
	}

	if (b->tsorted || b->trevsorted) {
		BAT *v;
		BUN low = 0;
		BUN high = b->U->count;

		if (BATtdense(b)) {
			/* positional */
			/* we expect nonil to be set, in which case we
			 * already know that we're not dealing with a
			 * nil equiselect (dealt with above) */
			assert(b->T->nonil);
			assert(b->tsorted);
			ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): dense\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
			if (hval) {
				oid h = * (oid *) th + hi;

				if (h > b->tseqbase)
					h -= b->tseqbase;
				else
					h = 0;
				if ((BUN) h < high)
					high = (BUN) h;
			}
			if (lval) {
				oid l = * (oid *) tl + !li;

				if (l > b->tseqbase)
					l -= b->tseqbase;
				else
					l = 0;
				if ((BUN) l > low)
					low = (BUN) l;
			}
		} else if (b->tsorted) {
			ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): sorted\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
			if (lval) {
				if (li)
					low = SORTfndfirst(b, tl);
				else
					low = SORTfndlast(b, tl);
			} else {
				/* skip over nils at start of column */
				low = SORTfndlast(b, nil);
			}
			low -= BUNfirst(b);
			if (hval) {
				if (hi)
					high = SORTfndlast(b, th);
				else
					high = SORTfndfirst(b, th);
				high -= BUNfirst(b);
			}
		} else {
			assert(b->trevsorted);
			ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): reverse sorted\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
			if (lval) {
				if (li)
					high = SORTfndlast(b, tl);
				else
					high = SORTfndfirst(b, tl);
			} else {
				/* skip over nils at end of column */
				high = SORTfndfirst(b, nil);
			}
			high -= BUNfirst(b);
			if (hval) {
				if (hi)
					low = SORTfndfirst(b, th);
				else
					low = SORTfndlast(b, th);
				low -= BUNfirst(b);
			}
		}
		if (anti) {
			BUN first = SORTfndlast(b, nil);
			/* match: [first..low) + [high..count) */
			if (s) {
				oid o = (oid) first;
				first = SORTfndfirst(s, &o);
				o = (oid) low;
				low = SORTfndfirst(s, &o);
				o = (oid) high;
				high = SORTfndfirst(s, &o);
				v = VIEWhead(BATmirror(s));
			} else {
				v = VIEWhead(b); /* [oid,nil] */
			}
			bn = BATslice2(v, first, low, high, BUNlast(v));
		} else {
			/* match: [low..high) */
			if (s) {
				oid o = (oid) low;
				low = SORTfndfirst(s, &o);
				o = (oid) high;
				high = SORTfndfirst(s, &o);
				v = VIEWhead(BATmirror(s));
			} else {
				v = VIEWhead(b); /* [oid,nil] */
			}
			bn = BATslice(v, low, high);
		}
		BBPunfix(v->batCacheid);
		bn = BATmirror(bn);
		bn->hseqbase = 0;
		bn->hkey = 1;
		bn->hsorted = 1;
		bn->hrevsorted = bn->U->count <= 1;
		bn->H->nonil = 1;
		bn->H->nil = 0;
		return bn;
	}

	if (b->tkey) {
		estimate = 1;
	} else if (s) {
		estimate = BATcount(s);
	} else if (BATcount(b) <= 100000) {
		estimate = BATguess(b);
	} else {
		BAT *tmp1 = BATmirror(BATmark(BATmirror(b), oid_nil));
		estimate = 0;
		if (tmp1) {
			BAT *tmp2 = BATsample(tmp1, 128);
			if (tmp2) {
				BAT *tmp3;
				BATseqbase(tmp2, 0);
				tmp3 = BATsubselect(tmp2, NULL, tl, th, li, hi, anti);
				if (tmp3) {
					estimate = (BUN) ((lng) BATcount(tmp3) * BATcount(b) / 100);
					BBPreclaim(tmp3);
				}
				BBPreclaim(tmp2);
			}
			BBPreclaim(tmp1);
		}
	}

	bn = BATnew(TYPE_void, TYPE_oid, estimate);
	if (bn == NULL)
		return NULL;

	if (equi &&
	    (b->T->hash ||
	     (b->batPersistence == PERSISTENT &&
	      (size_t) ATOMsize(b->ttype) > sizeof(BUN) / 4 &&
	      estimate < BATcount(b) / 100 &&
	      BATcount(b) * (ATOMsize(b->ttype) + 2 * sizeof(BUN)) < GDK_mem_maxsize / 2))) {
		ALGODEBUG fprintf(stderr, "#BATsubselect(b=%s#"BUNFMT",s=%s,anti=%d): hash select\n", BATgetId(b), BATcount(b), s ? BATgetId(s) : "NULL", anti);
		bn = BAT_hashselect(b, s, bn, tl);
	} else {
		bn = BAT_scanselect(b, s, bn, tl, th, li, hi, equi, anti, lval, hval);
	}

	return bn;
}

static BAT *
BAT_select_(BAT *b, const void *tl, const void *th, bit li, bit hi, bit tail, bit anti)
{
	BAT *bn;
	BAT *bn1 = NULL;
	BAT *map;
	BAT *b1;

	BATcheck(b, "BAT_select_");
	/* b is a [any_1,any_2] BAT */
	if (!BAThdense(b)) {
		ALGODEBUG fprintf(stderr, "#BAT_select_(b=%s#"BUNFMT",tail=%d): make map\n", BATgetId(b), BATcount(b), tail);
		map = BATmirror(BATmark(b, 0)); /* [dense,any_1] */
		b1 = BATmirror(BATmark(BATmirror(b), 0)); /* dense,any_2] */
	} else {
		ALGODEBUG fprintf(stderr, "#BAT_select_(b=%s#"BUNFMT",tail=%d): dense head\n", BATgetId(b), BATcount(b), tail);
		map = NULL;
		b1 = b;		/* [dense,any_2] (any_1==dense) */
	}
	/* b1 is a [dense,any_2] BAT, map (if set) is a [dense,any_1] BAT */
	bn = BATsubselect(b1, NULL, tl, th, li, hi, anti);
	if (bn == NULL)
		goto error;
	/* bn is a [dense,oid] BAT */
	if (tail) {
		/* we want to return a [any_1,any_2] subset of b */
		if (map) {
			bn1 = BATleftfetchjoin(bn, map, BATcount(bn));
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense,any_1] */
			BBPunfix(map->batCacheid);
			map = BATmirror(bn1);
			/* map is [any_1,dense] */
			bn1 = BATleftfetchjoin(bn, b1, BATcount(bn));
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense,any_2] */
			BBPunfix(b1->batCacheid);
			b1 = NULL;
			BBPunfix(bn->batCacheid);
			bn = BATleftfetchjoin(map, bn1, BATcount(map));
			if (bn == NULL)
				goto error;
			/* bn is [any_1,any_2] */
			BBPunfix(map->batCacheid);
			BBPunfix(bn1->batCacheid);
			map = bn1 = NULL;
		} else {
			/* b was [dense,any_2] */
			bn1 = VIEWcombine(BATmirror(bn));
			/* bn1 is [oid,oid] */
			BBPunfix(bn->batCacheid);
			bn = BATleftfetchjoin(bn1, b, BATcount(bn1));
			if (bn == NULL)
				goto error;
			/* bn is [oid,any_2] */
			BBPunfix(bn1->batCacheid);
			bn1 = NULL;
		}
		if (th == NULL && !anti && BATcount(bn) > 0 &&
		    ATOMcmp(b->ttype, tl, ATOMnilptr(b->ttype)) == 0) {
			/* this was the only way to get nils, so we
			 * have nils if there are any values at all */
			bn->T->nil = 1;
		} else {
			/* we can't have nils */
			bn->T->nonil = 1;
		}
	} else {
		/* we want to return a [any_1,nil] BAT */
		if (map) {
			BBPunfix(b1->batCacheid);
			b1 = NULL;
			bn1 = BATleftfetchjoin(bn, map, BATcount(bn));
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense,any_1] */
			BBPunfix(map->batCacheid);
			BBPunfix(bn->batCacheid);
			bn = bn1;
			map = bn1 = NULL;
		}
		BATseqbase(bn, oid_nil);
		/* bn is [nil,any_1] */
		bn = BATmirror(bn);
		/* bn is [any_1,nil] */
	}
	return bn;

  error:
	if (map)
		BBPunfix(map->batCacheid);
	if (b1 && b1 != b)
		BBPunfix(b1->batCacheid);
	if (bn1)
		BBPunfix(bn1->batCacheid);
	if (bn)
		BBPunfix(bn->batCacheid);
	return NULL;
}

BAT *
BATselect_(BAT *b, const void *h, const void *t, bit li, bit hi)
{
	return BAT_select_(b, h, t, li, hi, TRUE, FALSE);
}

BAT *
BATuselect_(BAT *b, const void *h, const void *t, bit li, bit hi)
{
	return BAT_select_(b, h, t, li, hi, FALSE, FALSE);
}

BAT *
BATantiuselect_(BAT *b, const void *h, const void *t, bit li, bit hi)
{
	return BAT_select_(b, h, t, li, hi, FALSE, TRUE);
}

BAT *
BATselect(BAT *b, const void *h, const void *t)
{
	return BATselect_(b, h, t, TRUE, TRUE);
}

BAT *
BATuselect(BAT *b, const void *h, const void *t)
{
	return BATuselect_(b, h, t, TRUE, TRUE);
}

BAT *
BATthetasubselect(BAT *b, BAT *s, const void *val, const char *op)
{
	const void *nil;

	BATcheck(b, "BATthetasubselect");
	BATcheck(val, "BATthetasubselect");
	BATcheck(op, "BATthetasubselect");

	if (op[0] == '=' && ((op[1] == '=' && op[2] == 0) || op[2] == 0)) {
		/* "=" or "==" */
		return BATsubselect(b, s, val, NULL, 1, 1, 0);
	}
	nil = ATOMnilptr(b->ttype);
	if (op[0] == '<') {
		if (op[1] == 0) {
			/* "<" */
			return BATsubselect(b, s, nil, val, 0, 0, 0);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* "<=" */
			return BATsubselect(b, s, nil, val, 0, 1, 0);
		}
	}
	if (op[0] == '>') {
		if (op[1] == 0) {
			/* ">" */
			return BATsubselect(b, s, val, nil, 0, 0, 0);
		}
		if (op[1] == '=' && op[2] == 0) {
			/* ">=" */
			return BATsubselect(b, s, val, nil, 1, 0, 0);
		}
	}
	GDKerror("BATthetasubselect: unknown operator.\n");
	return NULL;
}
