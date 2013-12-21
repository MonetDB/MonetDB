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
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
 */

/*
 * @f gdk_setop
 *
 */
/*
 * @a Peter Boncz
 *
 * @* Set Operations
 * Set operations are provided in two series:
 * @itemize
 * @item
 * k-@emph{operand}, which look only at the head column.
 * @item
 * s-@emph{operand} series, that look at the whole BUN.
 * @end itemize
 *
 * Operands provided are:
 * @itemize
 * @item [s,k]unique
 * produces a copy of the bat, with double elimination
 * @item [s,k]union
 * produces a bat union.
 * @item [s,k]diff
 * produces bat difference.
 * @item [s,k]intersection
 * produce bat intersection.
 * @end itemize
 *
 * Implementations typically take two forms: if the input relation(s)
 * is/are ordered, a merge-algorithm is used. Otherwise, hash-indices
 * are produced on demand for the hash-based versions.
 *
 * The @emph{[k,s]intersect(l,r)} operations result in all BUNs of
 * @emph{l} that are also in @emph{r}. They do not do
 * double-elimination over the @emph{l} BUNs.
 *
 * The @emph{[k,s]diff(l,r)} operations result in all BUNs of @emph{l}
 * that are not in @emph{r}. They do not do double-elimination over
 * the @emph{l} BUNs.
 *
 * The @emph{[k,s]union(l,r)} operations result in all BUNs of
 * @emph{l}, plus all BUNs of @emph{r} that are not in @emph{l}. They
 * do not do double-elimination over the @emph{l} nor @emph{r} BUNs.
 *
 * Operations with double-elimination can be formed by performing
 * @emph{[k,s]unique(l)} on their operands.
 *
 * The @emph{kintersect(l,r)} is used also as implementation for the
 * @emph{semijoin()}.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_search.h"

#define HITk(t1,t2)		TRUE
#define HITs(t1,t2)		((*cmp)(t1,t2) == 0)
#define EQUALs(t1,t2)		((*cmp)(t1,t2) == 0 && (*cmp)(t1,tnil))
#define EQUALk(t1,t2)		TRUE

#define HITintersect(h,t)       bunfastins(bn,h,t)
#define HITdiff(h,t)
#define MISSintersect(h,t)
#define MISSdiff(h,t)           bunfastins(bn,h,t)

#define HITintersect_nocheck(h,t)       bunfastins_nocheck(bn,BUNlast(bn),h,t,Hsize(bn),Tsize(bn))
#define HITdiff_nocheck(h,t)
#define MISSintersect_nocheck(h,t)
#define MISSdiff_nocheck(h,t)           bunfastins_nocheck(bn,BUNlast(bn),h,t,Hsize(bn),Tsize(bn))

#define DHITintersect(h,t)       bnh[o] = *(h); bnt[o++] = t;
#define DHITdiff(h,t)
#define DMISSintersect(h,t)
#define DMISSdiff(h,t)           bnh[o] = *(h); bnt[o++] = t;

#define ENDintersect(h,t)
#define ENDdiff(h,t)            for(;p1<q1;p1++) bunfastins(bn,h,t)

/*
 * @+ Double Elimination
 * Comes in two flavors: looking at one column, or at two at-a-time.
 * Implementation is either merge- or hash-based.
 */
#define mergeelim(a1,a2,a3,a4,a5)					\
	do {								\
		BATloop(b, p, q) {					\
			ptr h = BUNh##a2(bi,p);				\
			ptr t = BUNt##a3(bi,p);				\
									\
			for (r = p + 1; r < q && a4 == 0; r++) {	\
				if (HIT##a1(t, BUNt##a3(bi, r)))	\
					goto next##a2##a3##a5;		\
			}						\
			bunfastins(bn, h, t);				\
		  next##a2##a3##a5:;					\
		}							\
	} while (0)
#define hashelim(a1,a2,a3,a4)						\
	do {								\
		zz = BUNfirst(bn);					\
		if (!bn->H->hash) {					\
			if (BAThash(bn, BATcapacity(bn)) == NULL) {	\
				BBPreclaim(bn);				\
				return NULL;				\
			}						\
		}							\
		BATloop(b, p, q) {					\
			ptr h = BUNh##a2(bi, p);			\
			ptr t = BUNt##a3(bi, p);			\
			int ins = 1;					\
			BUN yy;						\
									\
			if (BATprepareHash(bn)) {			\
				BBPreclaim(bn);				\
				return NULL;				\
			}						\
			HASHloop##a4(bni, bn->H->hash, yy, h) {		\
				if (HIT##a1(t, BUNt##a3(bni, yy))) {	\
					ins = 0;			\
					break;				\
				}					\
			}						\
			if (ins) {					\
				bunfastins(bn, h, t);			\
				if (bn->H->hash)			\
					HASHins##a4(bn->H->hash, zz, h); \
				zz++;					\
			}						\
		}							\
	} while (0)
#define elim(a1,a2,a3,a4)						\
	{								\
		int (*cmp)(const void *, const void *) = BATatoms[b->ttype].atomCmp; \
		BUN zz;							\
		BUN p, q, r;						\
									\
		if (BAThordered(b)) {					\
			ALGODEBUG fprintf(stderr, "#BATins_%sunique: BAThordered(b)\n", #a1); \
			ALGODEBUG fprintf(stderr, "#BATins_%sunique: mergeelim\n", #a1); \
			if (b->tvarsized) {				\
				mergeelim(a1,a2,var,a4,a3);		\
			} else {					\
				mergeelim(a1,a2,loc,a4,a3);		\
			}						\
		} else if (b->tvarsized) {				\
			ALGODEBUG fprintf(stderr, "#BATins_%sunique: hashelim\n", #a1); \
			hashelim(a1,a2,var,a3);				\
		} else {						\
			ALGODEBUG fprintf(stderr, "#BATins_%sunique: hashelim\n", #a1); \
			hashelim(a1,a2,loc,a3);				\
		}							\
		(void) cmp;						\
	}
#define elim_doubles(a1)						\
	do {								\
		int tpe = ATOMtype(b->htype);				\
		if (tpe != ATOMstorage(tpe) &&				\
		    ATOMnilptr(ATOMstorage(tpe)) == ATOMnilptr(tpe) &&	\
		    BATatoms[ATOMstorage(tpe)].atomCmp == BATatoms[tpe].atomCmp) \
			tpe = ATOMstorage(tpe);				\
		switch (tpe) {						\
		case TYPE_bte:						\
			elim(a1,loc,_bte,simple_CMP(h,BUNhloc(bi,r),bte)); \
			break;						\
		case TYPE_sht:						\
			elim(a1,loc,_sht,simple_CMP(h,BUNhloc(bi,r),sht)); \
			break;						\
		case TYPE_int:						\
			elim(a1,loc,_int,simple_CMP(h,BUNhloc(bi,r),int)); \
			break;						\
		case TYPE_flt:						\
			elim(a1,loc,_flt,simple_CMP(h,BUNhloc(bi,r),flt)); \
			break;						\
		case TYPE_dbl:						\
			elim(a1,loc,_dbl,simple_CMP(h,BUNhloc(bi,r),dbl)); \
			break;						\
		case TYPE_lng:						\
			elim(a1,loc,_lng,simple_CMP(h,BUNhloc(bi,r),lng)); \
			break;						\
		case TYPE_str:						\
			if (b->H->vheap->hashash) {			\
				elim(a1,var,_str_hv,GDK_STRCMP(h,BUNhvar(bi,r))); \
				break;					\
			}						\
			/* fall through */				\
		default: {						\
			int (*merge)(const void *, const void *) = BATatoms[b->htype].atomCmp; \
									\
			if (b->hvarsized) {				\
				elim(a1,var,var,((*merge)(h,BUNhvar(bi,r)))); \
			} else {					\
				elim(a1,loc,loc,((*merge)(h,BUNhloc(bi,r)))); \
			}						\
			break;						\
		}							\
		}							\
	} while (0)

static BAT *
BATins_kunique(BAT *bn, BAT *b)
{
	bit unique = FALSE;
	BATiter bi = bat_iterator(b);
	BATiter bni = bat_iterator(bn);

	BATcheck(b, "BATins_kunique: src BAT required");
	BATcheck(bn, "BATins_kunique: dst BAT required");
	unique = (BATcount(bn) == 0);
	elim_doubles(k);
	if (unique && bn->hkey == FALSE) {
		/* we inserted unique head-values into an empty BAT;
		   hence, the resulting BAT's head is (now) unique/key ... */
		BATkey(bn, TRUE);
	}
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}

static BAT *
BATins_sunique(BAT *bn, BAT *b)
{
	bit unique = FALSE;
	BUN fst1, fst2, last1, last2;
	BATiter bi = bat_iterator(b);
	BATiter bni = bat_iterator(bn);

	BATcheck(b, "BATins_sunique: src BAT required");
	BATcheck(bn, "BATins_sunique: dst BAT required");

	unique = (BATcount(bn) == 0);

	fst1 = BUNfirst(bn);
	fst2 = BUNfirst(b);

	last1 = (BUNlast(bn) - 1);
	last2 = (BUNlast(b) - 1);

	if (BATcount(b) &&
	    BAThordered(b) &&
	    ATOMcmp(b->htype, BUNhead(bi, fst2), BUNhead(bi, last2)) == 0 &&
	    (BATcount(bn) == 0 ||
	     (ATOMcmp(bn->htype, BUNhead(bni, fst1), BUNhead(bi, fst2)) == 0 &&
	      BAThordered(bn) &&
	      ATOMcmp(bn->htype, BUNhead(bni, fst1), BUNhead(bni, last1)) == 0))) {
		ALGODEBUG fprintf(stderr, "#BATins_sunique: BATins_kunique(BATmirror(bn), BATmirror(b))\n");
		return BATins_kunique(BATmirror(bn), BATmirror(b));
	}
	if (BATcount(b) &&
	    BATtordered(b) &&
	    ATOMcmp(b->ttype, BUNtail(bi, fst2), BUNtail(bi, last2)) == 0 &&
	    (BATcount(bn) == 0 ||
	     (ATOMcmp(bn->ttype, BUNtail(bni, fst1), BUNtail(bi, fst2)) == 0 &&
	      BATtordered(bn) &&
	      ATOMcmp(bn->ttype, BUNtail(bni, fst1), BUNtail(bni, last1)) == 0))) {
		ALGODEBUG fprintf(stderr, "#BATins_sunique: BATins_kunique(bn, b)\n");
		return BATins_kunique(bn, b);
	}
	if (BATtordered(b) && ATOMstorage(b->ttype) < TYPE_str) {
		bni.b = bn = BATmirror(bn);
		bi.b = b = BATmirror(b);
	}

	elim_doubles(s);
	if (unique && bn->batSet == FALSE) {
		/* we inserted unique BUNs into an empty BAT;
		   hence, the resulting BAT is (now) unique/set ... */
		BATset(bn, TRUE);
	}
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	return bn;
      bunins_failed:
	BBPreclaim(bn);
	return NULL;
}


/*
 * @- Unique
 * The routine BATsunique removes duplicate BUNs,
 * The routine BATkunique removes duplicate head entries.
 */
BAT *
BATkunique(BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATkunique");

	if (b->hkey) {
		bn = BATcopy(b, b->htype, b->ttype, FALSE);
		if (bn == NULL)
			return NULL;
	} else {
		BUN cnt = BATcount(b);

		if (cnt > 10000) {
			BAT *tmp2 = NULL, *tmp1, *tmp0 = VIEWhead_(b, BAT_WRITE);

			if (tmp0) {
				tmp1 = BATsample(tmp0, 1000);
				if (tmp1) {
					tmp2 = BATkunique(tmp1);
					if (tmp2) {
						cnt = (BUN) ((((lng) BATcount(tmp2)) * cnt) / 900);
						BBPreclaim(tmp2);
					}
					BBPreclaim(tmp1);
				}
				BBPreclaim(tmp0);
			}
			if (tmp2 == NULL)
				return NULL;
		}
		bn = BATnew(BAThtype(b), BATttype(b), cnt);
		if (bn == NULL || BATins_kunique(bn, b) == NULL)
			return NULL;
	}

	/* property management */
	if (b->halign == 0) {
		b->halign = OIDnew(1);
	}
	BATkey(BATmirror(bn), BATtkey(b));
	bn->hsorted = BAThordered(b);
	bn->hrevsorted = BAThrevordered(b);
	bn->tsorted = BATtordered(b);
	bn->trevsorted = BATtrevordered(b);
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	if (BATcount(bn) == BATcount(b)) {
		ALIGNset(bn, b);
	}
	BATkey(bn, TRUE);	/* this we accomplished */
	return bn;
}

BAT *
BATsunique(BAT *b)
{
	BAT *bn;

	BATcheck(b, "BATsunique");

	if (b->hkey || b->tkey || b->batSet) {
		bn = BATcopy(b, b->htype, b->ttype, FALSE);
	} else {
		BUN cnt = BATcount(b);

		if (cnt > 10000) {
			BAT *tmp2 = NULL, *tmp1 = BATsample(b, 1000);

			if (tmp1) {
				tmp2 = BATkunique(tmp1);
				if (tmp2) {
					cnt = BATcount(tmp2) * (cnt / 1000);
					BBPreclaim(tmp2);
				}
				BBPreclaim(tmp1);
			}
			if (tmp2 == NULL)
				return NULL;
		}
		bn = BATnew(BAThtype(b), BATttype(b), cnt);
		if (bn == NULL || BATins_sunique(bn, b) == NULL)
			return NULL;
	}

	/* property management */
	BATkey(bn, BAThkey(b));
	BATkey(BATmirror(bn), BATtkey(b));
	bn->hsorted = BAThordered(b);
	bn->hrevsorted = BAThrevordered(b);
	bn->tsorted = BATtordered(b);
	bn->trevsorted = BATtrevordered(b);
	bn->H->nonil = b->H->nonil;
	bn->T->nonil = b->T->nonil;
	if (BATcount(bn) == BATcount(b)) {
		ALIGNset(bn, b);
	}
	BATset(bn, TRUE);	/* this we accomplished */
	return bn;
}

/*
 * @+ Difference and Intersect
 * Difference and Intersection are handled together. For each routine
 * there are two versions: BATkdiff(l,r) and
 * BATkintersect(l,r) (which look at the head column only), versus
 * BATsdiff(l,r) and BATsintersect(l,r) (looking at both
 * columns).  TODO synced/key case..
 */
#define mergecheck(a1,a2,a3,a4,a5)					\
	do {								\
		BUN p1 = BUNfirst(l), p2 = BUNfirst(r);			\
		BUN q1 = BUNlast(l),  q2 = BUNlast(r);			\
		BATiter li = bat_iterator(l);				\
		BATiter ri = bat_iterator(r);				\
									\
		ALGODEBUG fprintf(stderr,				\
				  "#BATins_%s%s: mergecheck[%s, %s, %s, %s, %s];\n", \
				  #a1, #a2, #a1, #a2, #a3, #a4, #a5);	\
		if (p2 < q2)						\
			BATloop(l, p1, q1) {				\
				ptr  h = BUNh##a2(li, p1);		\
				ptr  t = BUNtail(li, p1);		\
				ptr h2 = BUNh##a2(ri, p2);		\
				int c;					\
				while ((c = a4) > 0) {			\
					if ((++p2) >= q2)		\
						goto end##a2##a3;	\
					h2 = BUNh##a2(ri, p2);		\
				}					\
				if (c == 0) {				\
					h2 = hnil;			\
					if (a4) { /* check for not-nil (nils don't match anyway) */ \
						BUN pb = p2;		\
						int done = FALSE;	\
									\
						while (!done) {		\
							if (EQUAL##a5(t, BUNtail(ri, pb))) { \
								HIT##a1(h, t); \
								done = TRUE; \
							} else {	\
								if ((++pb) >= q2) { \
									MISS##a1(h, t);	\
									done = TRUE; \
								} else { \
									h2 = BUNh##a2(ri, pb); \
									if (a4) { \
										MISS##a1(h, t);	\
										done = TRUE; \
									} \
								}	\
							}		\
						}			\
						continue;		\
					}				\
				}					\
				MISS##a1(h, t);				\
			}						\
	  end##a2##a3:;							\
		END##a1(BUNh##a2(li, p1), BUNtail(li, p1));		\
	} while (0)
#define hashcheck(a1,a2,a3,a4,a5,a6)					\
	do {								\
		BUN p1, q1;						\
		int ins;						\
		BUN s2;							\
		ptr h, t, h2 = hnil;					\
		BATiter li = bat_iterator(l);				\
		BATiter ri = bat_iterator(r);				\
									\
		ALGODEBUG fprintf(stderr, "#BATins_%s%s: hashcheck[%s, %s, %s, %s, %s5];\n", #a1, #a2, #a1, #a2, #a3, #a4, #a5); \
		if (BATprepareHash(r)) {				\
			goto bunins_failed;				\
		}							\
		BATloop(l, p1, q1) {					\
			h = BUNh##a2(li, p1);				\
			t = BUNtail(li, p1);				\
			ins = TRUE;					\
			if (a6) /* check for not-nil (nils don't match anyway) */ \
				HASHloop##a4(ri, r->H->hash, s2, h) {	\
					if (EQUAL##a5(t, BUNtail(ri, s2))) { \
						HIT##a1(h, t);		\
						ins = FALSE;		\
						break;			\
					}				\
				}					\
			if (!ins)					\
				continue;				\
			MISS##a1(h, t);					\
		}							\
		(void)h2; /* in some cases the a6 check doesn't use the h2 */ \
	} while (0)

#define DIRECT_MAX 256

#define bte_EQ(x,y) simple_EQ(x,y,bte)
#define sht_EQ(x,y) simple_EQ(x,y,sht)
#define int_EQ(x,y) simple_EQ(x,y,int)
#define lng_EQ(x,y) simple_EQ(x,y,lng)
#define flt_EQ(x,y) simple_EQ(x,y,flt)
#define dbl_EQ(x,y) simple_EQ(x,y,dbl)

/* later add version for l void tail, remove general tail values then */
#define directcheck(a1,a2,a3,a4,a5,a6,a7)				\
	do {								\
		BUN p1, q1;						\
		int i;							\
		ptr h, h2 = hnil;					\
		BATiter li = bat_iterator(l);				\
		BATiter ri = bat_iterator(r);				\
		sht d[DIRECT_MAX];					\
		Hash hs, *H = &hs;					\
		int collision = 0;					\
									\
		H -> mask = DIRECT_MAX-1;				\
		H -> type = BAThtype(l);				\
									\
		ALGODEBUG fprintf(stderr, "#BATins_%s%s: directcheck[%s, %s, %s, %s, %s];\n", #a1, #a2, #a1, #a2, #a3, #a4, #a5); \
									\
		assert(l->htype == r->htype && r->htype != TYPE_void);	\
									\
		memset(d, 0, sizeof(d));				\
		BATloop(r, p1, q1) {					\
			h = BUNh##a2(ri,p1);				\
			i = (int) hash_##a4(H, h);			\
			/* collision or check for not-nil (nils don't match anyway) */ \
			if (d[i] != 0 || !(a6)) {			\
				collision = 1;				\
				break;					\
			}						\
			d[i] = ((sht)p1)+1;				\
		}							\
		if (collision) {					\
			hashcheck(a1,a2,a3,_##a4,a5,a6);		\
		} else {						\
			if (!l->ttype && l->tseqbase != oid_nil) {	\
				oid b = l->tseqbase, *t = &b;		\
				a4 *h = (a4*)BUNhloc(li, BUNfirst(l));	\
				a4 *rh = (a4*)BUNhloc(ri, 0);		\
				a4 *bnh;				\
				oid *bnt;				\
				BUN o = BUNfirst(bn);			\
									\
				ALGODEBUG fprintf(stderr, "#BATins_%s%s: directcheck[%s, %s, %s, _%s, %s][void tail]; " BUNFMT " " BUNFMT "\n", #a1, #a2, #a1, #a2, #a3, #a4, #a5, BATcount(l), BATcount(r)); \
				p1 = 0;					\
				q1 = BATcount(l);			\
				while(p1 < q1) {			\
					BUN r1;				\
					if (p1 + 1 > BATcapacity(bn)){	\
						BATsetcount(bn, o);	\
						if (BATextend(bn, BATgrows(bn)) == NULL) \
							goto bunins_failed; \
					}				\
					r1 = p1 + BATcapacity(bn) - BUNlast(bn); \
					if (r1 > q1) r1 = q1;		\
					bnh = (a4*)Hloc(bn,0);		\
					bnt = (oid*)Tloc(bn,0);		\
					for (; p1<r1; p1++, b++){	\
						i = (int) hash_##a4(H, h+p1); \
						if (d[i] != 0 && a7(h+p1, rh+d[i]-1) &&	\
						    EQUAL##a5(t, BUNtail(ri, d[i]-1))) { \
							DHIT##a1(h+p1, b); \
						} else {		\
							DMISS##a1(h+p1, b); \
						}			\
					}				\
				}					\
				BATsetcount(bn, o);			\
				(void)t;				\
			} else {					\
				a4 *h = (a4*)BUNhloc(li, 0);		\
				a4 *rh = (a4*)BUNhloc(ri, 0);		\
									\
				ALGODEBUG fprintf(stderr, "#BATins_%s%s: directcheck[%s, %s, %s, _%s, %s]; " BUNFMT " " BUNFMT "\n", #a1, #a2, #a1, #a2, #a3, #a4, #a5, BATcount(l), BATcount(r)); \
				p1 = BUNfirst(l);			\
				q1 = BUNlast(l);			\
				while(p1 < q1) {			\
					BUN r1;				\
					if (BUNlast(bn) + 1 > BATcapacity(bn)){	\
						if (BATextend(bn, BATcapacity(bn)+65536) == NULL) \
							goto bunins_failed; \
					}				\
					r1 = p1 + BATcapacity(bn) - BUNlast(bn); \
					if (r1 > q1) r1 = q1;		\
					for (; p1<r1; p1++) {		\
						i = (int) hash_##a4(H, h+p1); \
						if (d[i] != 0 && a7(h+p1, rh+d[i]-1) &&	\
						    EQUAL##a5(BUNtail(li,p1), BUNtail(ri, d[i]-1))) { \
							HIT##a1##_nocheck(h+p1, BUNtail(li, p1));	\
						} else {		\
							MISS##a1##_nocheck(h+p1, BUNtail(li, p1)); \
						}			\
					}				\
				}					\
			}						\
		}							\
		(void)h2; /* in some cases the a6 check doesn't use the h2 */ \
	} while (0)

#define checkall(a1,a2,a3,a4,a5)					\
	do {								\
		if (BAThdense(l)) {					\
			hashcheck(a1,pos,a2,a3,a5,TRUE);		\
		} else if (hash) {					\
			if (l->htype == TYPE_str && l->H->vheap->hashash) { \
				hashcheck(a1,a2,a2,_str_hv,a5,a4);	\
			} else {					\
				hashcheck(a1,a2,a2,a3,a5,a4);		\
			}						\
		} else {						\
			mergecheck(a1,a2,a3,a4,a5);			\
		}							\
	} while (0)

#define check(a1,a2,a3,a4,a5,a6)					\
	do {								\
		if (BAThdense(l)) {					\
			hashcheck(a1,pos,a2,_##a3,a5,TRUE);		\
		} else if (hash) {					\
			if (BATcount(r) < DIRECT_MAX) {			\
				directcheck(a1,a2,a2,a3,a5,a4,a6);	\
			} else {					\
				hashcheck(a1,a2,a2,_##a3,a5,a4);	\
			}						\
		} else {						\
			mergecheck(a1,a2,_##a3,a4,a5);			\
		}							\
	} while (0)

#define FLIPs								\
	else {								\
		int flip = BATtordered(l) & BATtordered(r);		\
									\
		if (flip) {						\
			hash = FALSE;					\
		} else {						\
			flip = r->H->hash == NULL && r->T->hash != NULL; \
		}							\
		if (flip) {						\
			r = BATmirror(r);				\
			l = BATmirror(l);				\
			bn = BATmirror(bn);				\
		}							\
	}
#define FLIPk

#define batcheck(a1,a2)							\
static BAT*								\
BATins_##a1##a2(BAT *bn, BAT *l, BAT *r)				\
{									\
	int hash = TRUE, (*cmp)(const void *, const void *), (*merge)(const void *, const void *) = NULL; \
	ptr hnil, tnil;							\
	BAT *b = bn;							\
									\
	/* determine how to do the intersect */				\
	if (BAThordered(l) & BAThordered(r)) {				\
		hash = FALSE;						\
	} FLIP##a1							\
									\
	merge = BATatoms[l->htype].atomCmp;				\
	cmp = BATatoms[l->ttype].atomCmp;				\
	hnil = ATOMnilptr(l->htype);					\
	tnil = ATOMnilptr(l->ttype);					\
	(void) cmp;							\
	(void) tnil;							\
	(void) hnil;							\
									\
	if (BAThdense(r)) {						\
		/* voidcheck */						\
		BATiter li = bat_iterator(l);				\
		BATiter ri = bat_iterator(r);				\
		BUN p1 = BUNfirst(r), q1 = BUNlast(r);			\
		oid rl = * (oid *) BUNhead(ri, p1);			\
		oid rh = rl + BATcount(r);				\
		ptr h, t = NULL, t2 = NULL;				\
									\
		(void) t2;						\
									\
		ALGODEBUG fprintf(stderr,				\
				  "#BATins_%s%s: voidcheck[%s, %s];\n", \
				  #a1, #a2, #a1, #a2);			\
		if (BAThdense(l)) {					\
			oid ll = * (oid *) BUNhead(li, (p1 = BUNfirst(l))); \
			oid lh = ll + BATcount(l);			\
			BUN hit_start = (q1 = BUNlast(l)), hit_end = q1, w = BUNfirst(r); \
			BUN off = p1;					\
									\
			h = (ptr) &ll;					\
									\
			if (rl >= ll && rl < lh) {			\
				hit_start = off + (rl - ll);		\
			} else if (rl < ll && rh > ll) {		\
				hit_start = p1;				\
				w += (ll - rl);				\
			}						\
			if (rh >= ll && rh < lh) {			\
				hit_end = off + (rh - ll);		\
			}						\
			while(p1 < hit_start) {				\
				t = BUNtail(li, p1);			\
				MISS##a2(h, t);				\
				ll++;					\
				p1++;					\
			}						\
			while(p1 < hit_end) {				\
				t = BUNtail(li, p1);			\
				t2 = BUNtail(ri, w);			\
				if (EQUAL##a1(t, t2)) {			\
					HIT##a2(h, t);			\
				} else {				\
					MISS##a2(h, t);			\
				}					\
				ll++;					\
				p1++;					\
				w++;					\
			}						\
			while (p1 < q1) {				\
				t = BUNtail(li, p1);			\
				MISS##a2(h, t);				\
				ll++;					\
				p1++;					\
			}						\
		} else {						\
			BUN off = p1;					\
									\
			BATloop(l, p1, q1) {				\
				oid o = * (oid *) BUNhloc(li, p1);	\
									\
				h = (ptr) &o;				\
				t = BUNtail(li, p1);			\
									\
				if (o >= rl && o < rh) {		\
					BUN w = off + (o - rl);		\
									\
					t2 = BUNtail(ri, w);		\
					if (EQUAL##a1(t, t2)) {		\
						HIT##a2(h, t);		\
						continue;		\
					}				\
				}					\
				MISS##a2(h, t);				\
			}						\
		}							\
	} else {							\
		int tpe = ATOMtype(r->htype);				\
		if (tpe != ATOMstorage(tpe) &&				\
		    ATOMnilptr(ATOMstorage(tpe)) == ATOMnilptr(tpe) &&	\
		    BATatoms[ATOMstorage(tpe)].atomCmp == BATatoms[tpe].atomCmp) \
			tpe = ATOMstorage(tpe);				\
		switch(tpe) {						\
		case TYPE_bte:						\
			check(a2,loc,bte,simple_CMP(h,h2,bte),a1,bte_EQ); \
			break;						\
		case TYPE_sht:						\
			check(a2,loc,sht,simple_CMP(h,h2,sht),a1,sht_EQ); \
			break;						\
		case TYPE_int:						\
			check(a2,loc,int,simple_CMP(h,h2,int),a1,int_EQ); \
			break;						\
		case TYPE_flt:						\
			check(a2,loc,flt,simple_CMP(h,h2,flt),a1,flt_EQ); \
			break;						\
		case TYPE_dbl:						\
			check(a2,loc,dbl,simple_CMP(h,h2,dbl),a1,dbl_EQ); \
			break;						\
		case TYPE_lng:						\
			check(a2,loc,lng,simple_CMP(h,h2,lng),a1,lng_EQ); \
			break;						\
		default:						\
			if (r->hvarsized) {				\
				checkall(a2,var,var,((*merge)(h,h2)),a1); \
			} else {					\
				checkall(a2,loc,loc,((*merge)(h,h2)),a1); \
			}						\
			break;						\
		}							\
	}								\
	return b;							\
  bunins_failed:							\
	BBPreclaim(b);							\
	return NULL;							\
}

batcheck(s,intersect)
batcheck(s,diff)
batcheck(k,intersect)
batcheck(k,diff)


static BAT *
diff_intersect(BAT *l, BAT *r, int diff, int set)
{
	BUN smaller;
	BAT *bn;

	ERRORcheck(l == NULL, "diff_intersect: left is null");
	ERRORcheck(r == NULL, "diff_intersect: right is null");
	ERRORcheck(TYPEerror(BAThtype(l), BAThtype(r)), "diff_intersect: incompatible head-types");
	if (set)
		ERRORcheck(TYPEerror(BATttype(l), BATttype(r)), "diff_intersect: incompatible tail-types");

	if (BATcount(r) == 0) {
		return diff ? BATcopy(l, l->htype, l->ttype, FALSE) : BATclone(l, 10);
	} else if (BATcount(l) == 0) {
		return BATclone(l, 10);
	}
	smaller = BATcount(l);
	if (!diff && BATcount(r) < smaller)
		smaller = BATcount(r);
	bn = BATnew(BAThtype(l), BATttype(l), MAX(smaller,BATTINY));
	if (bn == NULL)
		return NULL;

	/* fill result bat bn */
	if (set) {
		if (diff) {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_sdiff(bn, l, r);\n");
			bn = BATins_sdiff(bn, l, r);
		} else {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_sintersect(bn, l, r);\n");
			bn = BATins_sintersect(bn, l, r);
		}
	} else {
		if (diff) {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_kdiff(bn, l, r);\n");
			bn = BATins_kdiff(bn, l, r);
		} else {
			ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_kintersect(bn, l, r);\n");
			bn = BATins_kintersect(bn, l, r);
		}
	}
	if (bn == NULL)
		return NULL;

	/* propagate alignment info */
	if (BATcount(bn) == BATcount(l)) {
		ALIGNset(bn, l);
	}
	if (!diff &&
	    BAThordered(l) & BAThordered(r) &&
	    l->hkey &&
	    BATcount(bn) == BATcount(r)) {
		ALIGNsetH(bn, r);
	}
	bn->hsorted = BAThordered(l);
	bn->hrevsorted = BAThrevordered(l);
	bn->tsorted = BATtordered(l);
	bn->trevsorted = BATtrevordered(l);
	if (BATcount(bn)) {
		BATkey(bn, BAThkey(l));
		BATkey(BATmirror(bn), BATtkey(l));
	} else {
		BATkey(bn, TRUE);
		BATkey(BATmirror(bn), TRUE);
	}
	bn->H->nonil = l->H->nonil;
	bn->T->nonil = l->T->nonil;
	return bn;
}

BAT *
BATsdiff(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 1, 1);
}

BAT *
BATsintersect(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 0, 1);
}

BAT *
BATkdiff(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 1, 0);
}

BAT *
BATkintersect(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 0, 0);
}

/*
 * @+ Union
 * Union also consists of two versions: BATkunion(l,r), which
 * unites with double elimination over the head column only, and
 * BATsunion(l,r), that looks at both columns. Their
 * implementation uses the s/kdiff() and s/kunique() code for efficient
 * double elimination.
 */

static BAT *
BATunion(BAT *l, BAT *r, int set)
{
	int hdisjunct, tdisjunct;
	BAT *bn, *b;
	BUN p,q;
	BATiter li, ri;
	int ht, tt;

	BATcompatible(l, r);
	if (BATcount(l) == 0) {
		b = l;
		l = r;
		r = b;
	}
	if (BATcount(r) == 0) {
		return BATcopy(l, l->htype, l->ttype, FALSE);
	}

	b = NULL;
	li = bat_iterator(l);
 	ri = bat_iterator(r);
	hdisjunct = BAThordered(r) & BAThordered(l) &&
		    ATOMcmp(l->htype, BUNhead(li, BUNlast(l) - 1), BUNhead(ri, BUNfirst(r))) < 0;
	tdisjunct = BATtordered(r) & BATtordered(l) &&
		    ATOMcmp(l->ttype, BUNtail(li, BUNlast(l) - 1), BUNtail(ri, BUNfirst(r))) < 0;

	if (!hdisjunct) {
		b = r;
		ri.b = r = set ? BATsdiff(r, l) : BATkdiff(r, l);
		if (r == NULL) {
			return NULL;
		}
	}

	if (BATcount(r) == 0) {
		if (b)
			BBPreclaim(r);
		return BATcopy(l, l->htype, l->ttype, FALSE);
	}

	ht = l->htype;
	tt = l->ttype;
	if (ht == TYPE_void && l->hseqbase != oid_nil)
		ht = TYPE_oid;
	if (tt == TYPE_void && l->tseqbase != oid_nil)
		tt = TYPE_oid;
	bn = BATcopy(l, ht, tt, TRUE);
	if (bn == NULL) {
		if (b)
			BBPreclaim(r);
		return NULL;
	}
	BATloop(r, p, q) {
		bunfastins(bn, BUNhead(ri, p), BUNtail(ri, p));
	}
	if (!BAThdense(l) || !BAThdense(r) ||
	    * (oid *) BUNhead(li, BUNlast(l) - 1) + 1 != * (oid *) BUNhead(ri, BUNfirst(r))) {
		bn->hseqbase = oid_nil;
		bn->hdense = 0;
	}
	if (!BATtdense(l) || !BATtdense(r) ||
	    * (oid *) BUNtail(li, BUNlast(l) - 1) + 1 != * (oid *) BUNtail(ri, BUNfirst(r))) {
		bn->tseqbase = oid_nil;
		bn->tdense = 0;
	}
	bn->H->nonil = l->H->nonil & r->H->nonil;
	bn->T->nonil = l->T->nonil & r->T->nonil;
	bn->H->nil = l->H->nil | r->H->nil;
	bn->T->nil = l->T->nil | r->T->nil;
	if (b) {
		BBPreclaim(r);
		r = b;
	}
	HASHdestroy(bn);

	bn->hsorted = hdisjunct;
	bn->hrevsorted = 0;
	bn->tsorted = tdisjunct;
	bn->trevsorted = 0;
	bn->talign = bn->halign = 0;
	if (!r->hkey)
		BATkey(bn, FALSE);
	if (set && bn->hkey && hdisjunct == FALSE)
		BATkey(bn, FALSE);
	BATkey(BATmirror(bn), tdisjunct && BATtkey(l) && BATtkey(r));

	return bn;
  bunins_failed:
	BBPreclaim(bn);
	if (b)
		BBPreclaim(r);
	return NULL;
}

BAT *
BATsunion(BAT *l, BAT *r)
{
	return BATunion(l, r, 1);
}

BAT *
BATkunion(BAT *l, BAT *r)
{
	return BATunion(l, r, 0);
}
