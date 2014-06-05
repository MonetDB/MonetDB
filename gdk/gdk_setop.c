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
 * Copyright August 2008-2014 MonetDB B.V.
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
 * @item kunique
 * produces a copy of the bat, with double elimination
 * @item kunion
 * produces a bat union.
 * @item kdiff
 * produces bat difference.
 * @item kintersection
 * produce bat intersection.
 * @end itemize
 *
 * Implementations typically take two forms: if the input relation(s)
 * is/are ordered, a merge-algorithm is used. Otherwise, hash-indices
 * are produced on demand for the hash-based versions.
 *
 * The @emph{kintersect(l,r)} operations result in all BUNs of
 * @emph{l} that are also in @emph{r}. They do not do
 * double-elimination over the @emph{l} BUNs.
 *
 * The @emph{kdiff(l,r)} operations result in all BUNs of @emph{l}
 * that are not in @emph{r}. They do not do double-elimination over
 * the @emph{l} BUNs.
 *
 * The @emph{kunion(l,r)} operations result in all BUNs of
 * @emph{l}, plus all BUNs of @emph{r} that are not in @emph{l}. They
 * do not do double-elimination over the @emph{l} nor @emph{r} BUNs.
 *
 * Operations with double-elimination can be formed by performing
 * @emph{kunique(l)} on their operands.
 *
 * The @emph{kintersect(l,r)} is used also as implementation for the
 * @emph{semijoin()}.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_private.h"
#include "gdk_search.h"

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

/*
 * @- Unique
 * The routine BATkunique removes duplicate head entries.
 */
BAT *
BATkunique(BAT *b)
{
	BAT *bn;
	BAT *bn1 = NULL;
	BAT *map;
	BAT *b1;

	BATcheck(b, "BATkunique");

	if (b->hkey) {
		bn = BATcopy(b, b->htype, b->ttype, FALSE);
	} else {
		b = BATmirror(b);	/* work on tail instead of head */
		/* b is a [any_1,any_2] BAT */
		if (!BAThdense(b)) {
			map = BATmirror(BATmark(b, 0)); /* [dense1,any_1] */
			b1 = BATmirror(BATmark(BATmirror(b), 0)); /* [dense1,any_2] */
		} else {
			map = NULL;
			b1 = b;		/* [dense1,any_2] (any_1==dense1) */
		}
		bn = BATsubunique(b1, NULL);
		if (bn == NULL)
			goto error;
		/* bn is a [dense2,oid1] BAT with oid1 a subset of dense1 */
		/* we want to return a [any_1,any_2] subset of b */
		if (map) {
			bn1 = BATproject(bn, map);
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense2,any_1] */
			BBPunfix(map->batCacheid);
			map = BATmirror(bn1);
			/* map is [any_1,dense2] */
			bn1 = BATproject(bn, b1);
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense2,any_2] */
			BBPunfix(b1->batCacheid);
			b1 = NULL;
			BBPunfix(bn->batCacheid);
			bn = VIEWcreate(map, bn1);
			if (bn == NULL)
				goto error;
			/* bn is [any_1,any_2] */
			BBPunfix(bn1->batCacheid);
			BBPunfix(map->batCacheid);
			bn1 = map = NULL;
		} else {
			bn1 = BATproject(bn, b);
			if (bn1 == NULL)
				goto error;
			/* bn1 is [dense2,any_2] */
			/* bn was [dense2,any_1] since b was hdense */
			b1 = VIEWcreate(BATmirror(bn), bn1);
			if (b1 == NULL)
				goto error;
			/* b1 is [any_1,any_2] */
			BBPunfix(bn->batCacheid);
			bn = b1;
			b1 = NULL;
			BBPunfix(bn1->batCacheid);
			bn1 = NULL;
		}
		bn = BATmirror(bn);
	}
	BATkey(bn, TRUE);

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

/*
 * @+ Difference and Intersect
 * Difference and Intersection are handled together.  BATkdiff(l,r)
 * and BATkintersect(l,r)
 */
#define mergecheck(a1,a2,a3,a4)						\
	do {								\
		BUN p1 = BUNfirst(l), p2 = BUNfirst(r);			\
		BUN q1 = BUNlast(l),  q2 = BUNlast(r);			\
		BATiter li = bat_iterator(l);				\
		BATiter ri = bat_iterator(r);				\
									\
		ALGODEBUG fprintf(stderr,				\
				  "#BATins_%s%s: mergecheck[%s, %s, %s, %s, k];\n", \
				  #a1, #a2, #a1, #a2, #a3, #a4);	\
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
						HIT##a1(h, t);		\
						continue;		\
					}				\
				}					\
				MISS##a1(h, t);				\
			}						\
	  end##a2##a3:;							\
		END##a1(BUNh##a2(li, p1), BUNtail(li, p1));		\
	} while (0)

#define hashcheck(a1,a2,a3,a4,a5)					\
	do {								\
		BUN p1, q1;						\
		int ins;						\
		BUN s2;							\
		ptr h, t, h2 = hnil;					\
		BATiter li = bat_iterator(l);				\
		BATiter ri = bat_iterator(r);				\
									\
		ALGODEBUG fprintf(stderr, "#BATins_%s%s: hashcheck[%s, %s, %s, %s, k];\n", #a1, #a2, #a1, #a2, #a3, #a4); \
		if (BATprepareHash(r)) {				\
			goto bunins_failed;				\
		}							\
		BATloop(l, p1, q1) {					\
			h = BUNh##a2(li, p1);				\
			t = BUNtail(li, p1);				\
			ins = TRUE;					\
			if (a5) /* check for not-nil (nils don't match anyway) */ \
				HASHloop##a4(ri, r->H->hash, s2, h) {	\
					HIT##a1(h, t);			\
					ins = FALSE;			\
					break;				\
				}					\
			if (!ins)					\
				continue;				\
			MISS##a1(h, t);					\
		}							\
		(void)h2; /* in some cases the a5 check doesn't use the h2 */ \
	} while (0)

#define DIRECT_MAX 256

#define bte_EQ(x,y) simple_EQ(x,y,bte)
#define sht_EQ(x,y) simple_EQ(x,y,sht)
#define int_EQ(x,y) simple_EQ(x,y,int)
#define lng_EQ(x,y) simple_EQ(x,y,lng)
#ifdef HAVE_HGE
#define hge_EQ(x,y) simple_EQ(x,y,hge)
#endif
#define flt_EQ(x,y) simple_EQ(x,y,flt)
#define dbl_EQ(x,y) simple_EQ(x,y,dbl)

/* later add version for l void tail, remove general tail values then */
#define directcheck(a1,a2,a3,a4,a5,a6)					\
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
		ALGODEBUG fprintf(stderr, "#BATins_%s%s: directcheck[%s, %s, %s, %s, k];\n", #a1, #a2, #a1, #a2, #a3, #a4); \
									\
		assert(l->htype == r->htype && r->htype != TYPE_void);	\
									\
		memset(d, 0, sizeof(d));				\
		BATloop(r, p1, q1) {					\
			h = BUNh##a2(ri,p1);				\
			i = (int) hash_##a4(H, h);			\
			/* collision or check for not-nil (nils don't match anyway) */ \
			if (d[i] != 0 || !(a5)) {			\
				collision = 1;				\
				break;					\
			}						\
			d[i] = ((sht)p1)+1;				\
		}							\
		if (collision) {					\
			hashcheck(a1,a2,a3,_##a4,a5);			\
		} else {						\
			if (!l->ttype && l->tseqbase != oid_nil) {	\
				oid b = l->tseqbase, *t = &b;		\
				a4 *h = (a4*)BUNhloc(li, BUNfirst(l));	\
				a4 *rh = (a4*)BUNhloc(ri, 0);		\
				a4 *bnh;				\
				oid *bnt;				\
				BUN o = BUNfirst(bn);			\
									\
				ALGODEBUG fprintf(stderr, "#BATins_%s%s: directcheck[%s, %s, %s, _%s, k][void tail]; " BUNFMT " " BUNFMT "\n", #a1, #a2, #a1, #a2, #a3, #a4, BATcount(l), BATcount(r)); \
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
						if (d[i] != 0 && a6(h+p1, rh+d[i]-1)) { \
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
				ALGODEBUG fprintf(stderr, "#BATins_%s%s: directcheck[%s, %s, %s, _%s, k]; " BUNFMT " " BUNFMT "\n", #a1, #a2, #a1, #a2, #a3, #a4, BATcount(l), BATcount(r)); \
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
						if (d[i] != 0 && a6(h+p1, rh+d[i]-1)) { \
							HIT##a1##_nocheck(h+p1, BUNtail(li, p1)); \
						} else {		\
							MISS##a1##_nocheck(h+p1, BUNtail(li, p1)); \
						}			\
					}				\
				}					\
			}						\
		}							\
		(void)h2; /* in some cases the a5 check doesn't use the h2 */ \
	} while (0)

#define checkall(a1,a2,a3,a4)						\
	do {								\
		if (BAThdense(l)) {					\
			hashcheck(a1,pos,a2,a3,TRUE);			\
		} else if (hash) {					\
			if (l->htype == TYPE_str && l->H->vheap->hashash) { \
				hashcheck(a1,a2,a2,_str_hv,a4);		\
			} else {					\
				hashcheck(a1,a2,a2,a3,a4);		\
			}						\
		} else {						\
			mergecheck(a1,a2,a3,a4);			\
		}							\
	} while (0)

#define check(a1,a2,a3,a4,a5)					\
	do {							\
		if (BAThdense(l)) {				\
			hashcheck(a1,pos,a2,_##a3,TRUE);	\
		} else if (hash) {				\
			if (BATcount(r) < DIRECT_MAX) {		\
				directcheck(a1,a2,a2,a3,a4,a5);	\
			} else {				\
				hashcheck(a1,a2,a2,_##a3,a4);	\
			}					\
		} else {					\
			mergecheck(a1,a2,_##a3,a4);		\
		}						\
	} while (0)

#ifdef HAVE_HGE
#define batcheck_hge(a1)						\
		case TYPE_hge:						\
			check(a1,loc,hge,simple_CMP(h,h2,hge),hge_EQ);	\
			break
#else
#define batcheck_hge(a1)
#endif
#define batcheck(a1)							\
static BAT*								\
BATins_k##a1(BAT *bn, BAT *l, BAT *r)					\
{									\
	int hash = TRUE, (*cmp)(const void *, const void *), (*merge)(const void *, const void *) = NULL; \
	ptr hnil, tnil;							\
	BAT *b = bn;							\
									\
	/* determine how to do the intersect */				\
	if (BAThordered(l) & BAThordered(r)) {				\
		hash = FALSE;						\
	}								\
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
				  "#BATins_k%s: voidcheck[k, %s];\n",	\
				  #a1, #a1);				\
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
				MISS##a1(h, t);				\
				ll++;					\
				p1++;					\
			}						\
			while(p1 < hit_end) {				\
				t = BUNtail(li, p1);			\
				t2 = BUNtail(ri, w);			\
				HIT##a1(h, t);				\
				ll++;					\
				p1++;					\
				w++;					\
			}						\
			while (p1 < q1) {				\
				t = BUNtail(li, p1);			\
				MISS##a1(h, t);				\
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
					HIT##a1(h, t);			\
					continue;			\
				}					\
				MISS##a1(h, t);				\
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
			check(a1,loc,bte,simple_CMP(h,h2,bte),bte_EQ);	\
			break;						\
		case TYPE_sht:						\
			check(a1,loc,sht,simple_CMP(h,h2,sht),sht_EQ);	\
			break;						\
		case TYPE_int:						\
			check(a1,loc,int,simple_CMP(h,h2,int),int_EQ);	\
			break;						\
		case TYPE_flt:						\
			check(a1,loc,flt,simple_CMP(h,h2,flt),flt_EQ);	\
			break;						\
		case TYPE_dbl:						\
			check(a1,loc,dbl,simple_CMP(h,h2,dbl),dbl_EQ);	\
			break;						\
		case TYPE_lng:						\
			check(a1,loc,lng,simple_CMP(h,h2,lng),lng_EQ);	\
			break;						\
		batcheck_hge(a1);					\
		default:						\
			if (r->hvarsized) {				\
				checkall(a1,var,var,((*merge)(h,h2)));	\
			} else {					\
				checkall(a1,loc,loc,((*merge)(h,h2)));	\
			}						\
			break;						\
		}							\
	}								\
	return b;							\
  bunins_failed:							\
	BBPreclaim(b);							\
	return NULL;							\
}

batcheck(intersect)
batcheck(diff)


static BAT *
diff_intersect(BAT *l, BAT *r, int diff)
{
	BUN smaller;
	BAT *bn;

	ERRORcheck(l == NULL, "diff_intersect: left is null");
	ERRORcheck(r == NULL, "diff_intersect: right is null");
	ERRORcheck(TYPEerror(BAThtype(l), BAThtype(r)), "diff_intersect: incompatible head-types");

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
	if (diff) {
		ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_kdiff(bn, l, r);\n");
		bn = BATins_kdiff(bn, l, r);
	} else {
		ALGODEBUG fprintf(stderr, "#diff_intersect: BATins_kintersect(bn, l, r);\n");
		bn = BATins_kintersect(bn, l, r);
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
BATkdiff(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 1);
}

BAT *
BATkintersect(BAT *l, BAT *r)
{
	return diff_intersect(l, r, 0);
}

/*
 * @+ Union
 * Union consists of one version: BATkunion(l,r), which unites
 * with double elimination over the head column only. The
 * implementation uses the kdiff() and kunique() code for
 * efficient double elimination.
 */
BAT *
BATkunion(BAT *l, BAT *r)
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
		ri.b = r = BATkdiff(r, l);
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
	BATkey(BATmirror(bn), tdisjunct && BATtkey(l) && BATtkey(r));

	return bn;
  bunins_failed:
	BBPreclaim(bn);
	if (b)
		BBPreclaim(r);
	return NULL;
}
