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

#include <monetdb_config.h>
#include <mal_exception.h>
#include <gdk_cand.h>
#include <gdk_atoms.h>
#include <string.h>

#define M 1000000
#if 0
#define GZ 128
#define CHAR_MAP(s) (s&127)
#else
#define GZ 64
#define CHAR_MAP(s) (s&63)
#endif
#define SZ_1GRAM GZ
#define SZ_2GRAM (GZ*GZ)
#define SZ_3GRAM (GZ*GZ*GZ)
#define SZ_4GRAM ((size_t)GZ*GZ*GZ*GZ)

#define hist_1gram sht_hist_1gram
#define hist_2gram sht_hist_2gram
#define hist_3gram sht_hist_3gram
#define NGsignature NGsignature_sht
#define NGand NGand_sht
#define NGandselect NGandselect_sht
#define NGRAM_TYPE sht
#define NGRAM_TYPEID TYPE_sht
#define NGRAM_TYPENIL sht_nil
#define NGRAM_CST
#define NGRAM_BITS 15
#include "ngram.h"

#undef hist_1gram
#undef hist_2gram
#undef hist_3gram
#undef NGsignature
#undef NGand
#undef NGandselect
#undef NGRAM_TYPE
#undef NGRAM_TYPEID
#undef NGRAM_TYPENIL
#undef NGRAM_CST
#undef NGRAM_BITS

#define hist_1gram int_hist_1gram
#define hist_2gram int_hist_2gram
#define hist_3gram int_hist_3gram
#define NGsignature NGsignature_int
#define NGand NGand_int
#define NGandselect NGandselect_int
#define NGRAM_TYPE int
#define NGRAM_TYPEID TYPE_int
#define NGRAM_TYPENIL int_nil
#define NGRAM_CST
#define NGRAM_BITS 31
#include "ngram.h"

#undef hist_1gram
#undef hist_2gram
#undef hist_3gram
#undef NGsignature
#undef NGand
#undef NGandselect
#undef NGRAM_TYPE
#undef NGRAM_TYPEID
#undef NGRAM_TYPENIL
#undef NGRAM_CST
#undef NGRAM_BITS

#define hist_1gram lng_hist_1gram
#define hist_2gram lng_hist_2gram
#define hist_3gram lng_hist_3gram
#define NGsignature NGsignature_lng
#define NGand NGand_lng
#define NGandselect NGandselect_lng
#define NGRAM_TYPE lng
#define NGRAM_TYPEID TYPE_lng
#define NGRAM_TYPENIL lng_nil
#define NGRAM_CST(v) LL_CONSTANT(v)
#define NGRAM_BITS 63
#include "ngram.h"

#undef hist_1gram
#undef hist_2gram
#undef hist_3gram
#undef NGsignature
#undef NGand
#undef NGandselect
#undef NGRAM_TYPE
#undef NGRAM_TYPEID
#undef NGRAM_TYPENIL
#undef NGRAM_CST
#undef NGRAM_BITS

#define hist_1gram hge_hist_1gram
#define hist_2gram hge_hist_2gram
#define hist_3gram hge_hist_3gram
#define NGsignature NGsignature_hge
#define NGand NGand_hge
#define NGandselect NGandselect_hge
#define NGRAM_TYPE hge
#define NGRAM_TYPEID TYPE_hge
#define NGRAM_TYPENIL hge_nil
#define NGRAM_CST(v) ((hge)LL_CONSTANT(v))
#define NGRAM_BITS 127
#include "ngram.h"

#undef hist_1gram
#undef hist_2gram
#undef hist_3gram
#undef NGsignature
#undef NGand
#undef NGandselect
#undef NGRAM_TYPE
#undef NGRAM_TYPEID
#undef NGRAM_TYPENIL
#undef NGRAM_CST
#undef NGRAM_BITS

static str
NGandjoin_intern(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)L;
	(void)R;
	(void)sigs;
	(void)needle;
	(void)lc;
	(void)rc;
	(void)nil_matches;
	(void)estimate;
	(void)anti;
	return MAL_SUCCEED;
}

static str
NGandjoin1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGandjoin_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGandjoin(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGandjoin_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}

static inline int
popcount64(uint64_t x)
{
#if defined(__GNUC__)
    return (uint32_t) __builtin_popcountll(x);
#elif defined(_MSC_VER)
    return (uint32_t) __popcnt64(x);
#else
    x = (x & 0x5555555555555555ULL) + ((x >> 1) & 0x5555555555555555ULL);
    x = (x & 0x3333333333333333ULL) + ((x >> 2) & 0x3333333333333333ULL);
    x = (x & 0x0F0F0F0F0F0F0F0FULL) + ((x >> 4) & 0x0F0F0F0F0F0F0F0FULL);
    return (x * 0x0101010101010101ULL) >> 56;
#endif
}

static str
NGpopcnt(int *cnt, lng *v)
{
	*cnt = popcount64(*v);
	return MAL_SUCCEED;
}

static str
NGsignature_dummy( str *sig, str *str, int *n)
{
	(void)sig;
	(void)str;
	(void)n;
	throw(MAL, "ngram.signature", "no scalar version\n");
}

static char *
gor_lng(lng *res, const bat *bid)
{
	BAT *b;
	lng val = 0;
	BUN nval = 0;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "gram.gor", RUNTIME_OBJECT_MISSING);

	const lng *vals = (const lng *) Tloc(b, 0);
	for (BUN i = 0, n = BATcount(b); i < n; i++) {
		if (is_lng_nil(vals[i]))
			continue; /* nils are ignored */
		if (vals[i] == 0) {
			/* any value zero is easy: result is zero */
			BBPunfix(b->batCacheid);
			*res = 0;
			return MAL_SUCCEED;
		}
		if (vals[i] < 0) {
			val |= -vals[i];
		} else {
			val |= vals[i];
		}
		nval++;		/* count non-nil values */
	}
	BBPunfix(b->batCacheid);
	if (nval == 0) {
		/* if there are no non-nil values, the result is nil */
		*res = lng_nil;
	} else {
		*res = val;
	}
	return MAL_SUCCEED;
}

static char *
subgrouped_gor_cand_lng(bat *retval, const bat *bid, const bat *gid,
			const bat *eid, const bat *sid,
			const bit *skip_nils)
{
	BAT *b, *bn;		/* these two are always assigned */
	BAT *g = NULL;		/* these three are optional and may not ... */
	BAT *e = NULL;		/* ... be assigned to below, ... */
	BAT *s = NULL;		/* ... so we initialize them here */

	/* we ignore these two inputs */
	(void) skip_nils;

	/* the bat we're supposed to be working on (bid) is not
	 * optional, but the others are, so we test whether the bat id
	 * is not nil, and if it isn't, whether we can find the BAT
	 * descriptor */
	if ((b = BATdescriptor(*bid)) == NULL ||
	    (gid && !is_bat_nil(*gid) && (g = BATdescriptor(*gid)) == NULL) ||
	    (eid && !is_bat_nil(*eid) && (e = BATdescriptor(*eid)) == NULL) ||
	    (sid && !is_bat_nil(*sid) && (s = BATdescriptor(*sid)) == NULL)) {
		if (b)
			BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "ngram.gor", RUNTIME_OBJECT_MISSING);
	}

	oid min, max;		/* min and max group id */
	BUN ngrp;	/* number of groups, number of candidates */
	struct canditer ci;	/* candidate list iterator */
	const char *err;	/* error message */
	err = BATgroupaggrinit(b, g, e, s, &min, &max, &ngrp, &ci);
	if (err != NULL) {
		BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "ngram.gor", "%s\n", err);
	}

	/* create a result BAT and initialize it with all zeros */
	bn = BATconstant(min, TYPE_lng, &(lng){0}, ngrp, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		if (g)
			BBPunfix(g->batCacheid);
		if (e)
			BBPunfix(e->batCacheid);
		if (s)
			BBPunfix(s->batCacheid);
		throw(MAL, "ngram.gor", "%s\n", GDK_EXCEPTION);
	}

	/* We are going to change the contents of the newly created BAT.
	 * We are allowed to do that because we are the "owner" and no
	 * other thread can have a reference to this BAT yet.  First we
	 * set the properties.  We don't know whether the result is
	 * sorted or whether there are any duplicate values, so we must
	 * set these properties to false. */
	bn->tsorted = false;
	bn->trevsorted = false;
	bn->tkey = false;
	/* we may change these two below */
	bn->tnil = false;
	bn->tnonil = true;

	/* In general we need to distinguish two cases per group: there
	 * are no non-nil values, then the result should be nil; there
	 * are non-nil values, then the result should be the aggregate.
	 * If there is an error (e.g. overflow), there should not be any
	 * result (error return).  We shouldn't get any overflow errors,
	 * so this doesn't apply here. */
	const lng *vals = (const lng *) Tloc(b, 0);
	const oid *gids = NULL;
	if (g && !BATtdense(g))
		gids= (const oid *) Tloc(g, 0);
	lng *gor = (lng *) Tloc(bn, 0); /* results */
	for (BUN i = 0; i < ci.ncand; i++) {
		oid o = canditer_next(&ci); /* id of candidate */
		BUN p = o - b->hseqbase;    /* BUN (index) of candidate */
		lng v = vals[p];	    /* the actual value */
		oid grp = gids ? gids[p] : g ? min + (oid) p : 0; /* group id */
		if (grp >= min && grp <= max) { /* extra check */
			grp -= min;		/* zero based access */
			if (is_lng_nil(v))
				continue; /* skip nils (no matter skip_nils) */
			/* non zero and non NULL value */
			gor[grp] |= v;
		}
	}

	BBPunfix(b->batCacheid);
	if (g)
		BBPunfix(g->batCacheid);
	if (e)
		BBPunfix(e->batCacheid);
	if (s)
		BBPunfix(s->batCacheid);

	*retval = bn->batCacheid;
	BBPkeepref(bn);
	return MAL_SUCCEED;
}

static char *
subgrouped_gor_lng(bat *retval, const bat *bid, const bat *gid,
		    const bat *eid, const bit *skip_nils)
{
	return subgrouped_gor_cand_lng(retval, bid, gid, eid, NULL, skip_nils);
}

static char *
grouped_gor_lng(bat *retval, const bat *bid, const bat *gid, const bat *eid)
{
	return subgrouped_gor_cand_lng(retval, bid, gid, eid, NULL, &(bit){1});
}

static str
NGcx(bit *r, str *h, str *needle)
{
	*r = strstr(*h, *needle) != NULL;
	return MAL_SUCCEED;
}

static str
NGcxselect(bat *R, bat *H, bat *C, str *Needle, bit *anti)
{
	(void)R;
	(void)H;
	(void)C;
	(void)Needle;
	(void)anti;
	return MAL_SUCCEED;
}

static inline unsigned long long
prof_clock(void)
{
        unsigned long long tsc;

#if ((((defined(__GNUC__) && (defined(__powerpc__) || defined(__ppc__))) || (defined(__MWERKS__) && defined(macintosh)))) || (defined(__IBM_GCC_ASM) && (defined(__powerpc__) || defined(__ppc__))))
        return getticks();
#elif defined(__x86_64__) && !defined(__PGI)
        unsigned long long a, d;
        __asm__ __volatile__("rdtsc" : "=a" (a), "=d" (d));
        tsc = ((unsigned long)a) | (((unsigned long)d)<<32);
#elif defined(__i386__) && !defined(__PGI)
        __asm__ __volatile__("rdtsc" : "=A" (tsc));
#elif !defined(__ia64__)
        tsc = (unsigned long long) clock();
#elif defined(__GNUC__)
        __asm__ __volatile__("mov %0=ar.itc" : "=r"(tsc) :: "memory");
#else
        tsc = (unsigned long long)_ _getReg(_IA64_REG_AR_ITC);
#endif /* defined(i386) */

        return tsc;
}

#if 0
#define NGRAM_TYPE int
#define NGRAM_TYPEID TYPE_int
#define NGRAM_TYPENIL int_nil
#define NGRAM_CST(v) (v)
#define NGRAM_BITS 31

//#define NGRAM_TYPE lng
//#define NGRAM_TYPEID TYPE_lng
//#define NGRAM_TYPENIL lng_nil
//#define NGRAM_CST(v) LL_CONSTANT(v)
//#define NGRAM_BITS 63
#else
#define NGRAM_TYPE hge
#define NGRAM_TYPEID TYPE_hge
#define NGRAM_TYPENIL hge_nil
#define NGRAM_CST(v) ((hge)LL_CONSTANT(v))
#define NGRAM_BITS 127
#endif
#define NGRAM_MULTIPLE 16

typedef struct {
	NGRAM_TYPE max, min;
	unsigned int *h;
	unsigned int *pos;
	unsigned int *rid;
	NGRAM_TYPE *idx;
	NGRAM_TYPE *sigs;
} Ngrams;

static void
ngrams_destroy(Ngrams *n)
{
	if (n) {
		GDKfree(n->h);
		GDKfree(n->idx);
		GDKfree(n->pos);
		GDKfree(n->rid);
		GDKfree(n->sigs);
	}
	GDKfree(n);
}

static Ngrams *
ngrams_create(BAT *b, size_t ngramsize)
{
	Ngrams *n = NULL;
	size_t sz = BATcount(b);

	n = (Ngrams*)GDKmalloc(sizeof(Ngrams));
	if (n) {
		n->h = (unsigned int*)GDKmalloc(ngramsize*sizeof(int));
		n->pos=(unsigned int*)GDKzalloc(ngramsize*sizeof(int));
		n->rid=(unsigned int*)GDKmalloc(NGRAM_MULTIPLE* sz * sizeof(int));

		n->idx = (NGRAM_TYPE*)GDKmalloc(ngramsize*sizeof(NGRAM_TYPE));
		n->sigs=(NGRAM_TYPE*)GDKmalloc(sz * sizeof(NGRAM_TYPE));
		n->max = -1;
		n->min = -1;
	}
	if (!n || !n->h || !n->idx || !n->pos || !n->rid || !n->sigs) {
		ngrams_destroy(n);
		return NULL;
	}
	return n;
}

static int
ngrams_init_1gram(Ngrams *n, BAT *b)
{
	BUN cnt = BATcount(b);
	NGRAM_TYPE *h = (NGRAM_TYPE *)GDKzalloc(SZ_1GRAM*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_1GRAM*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			for(; *s; s++) {
				h[CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(int i=0; i<SZ_1GRAM; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, SZ_1GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=SZ_1GRAM-1; i>=0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], min = hist[i];
	/* printf("max %d, first min %d nr of larger %d sum %d, cnt %d\n", (int)hist[0], (int)min, i, sum, cnt); */
	n->max = max;
	n->min = min;
	for(int i=0; i<SZ_1GRAM && hist[i] > 0; i++) {
		unsigned int x=id[i];
		idx[x] = NGRAM_CST(1)<<bc;
		assert(idx[x] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		/* char *os = s; */
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0]) { /* too short skipped */
			for(; *s; s++) {
				int k = CHAR_MAP(*s);
				sig |= idx[k];
				if (n->h[k] <= n->min) {
					if (n->pos[k] == 0) {
						n->pos[k] = pos;
						pos += n->h[k];
						n->h[k] = 0;
					}
					/* deduplicate */
					if (n->h[k] == 0 || n->rid[n->pos[k] + n->h[k] - 1] != i) {
						n->rid[n->pos[k] + n->h[k]] = i;
						n->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
NGc1join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)nil_matches;
	(void)estimate;
	BAT *h = BATdescriptor(*H);
	BAT *n = BATdescriptor(*N);

	if (lc && !is_bat_nil(*lc))
		assert(0);
	if (rc && !is_bat_nil(*rc))
		assert(0);

	if (*anti)
		throw(MAL, "gram.c1", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c1", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
		printf("todo fall back to select \n");
	}

	Ngrams *ngi = ngrams_create(h, SZ_1GRAM);
	if (ngi && ngrams_init_1gram(ngi, h) == 0) {
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		int ncnt = 0, ncnt1 = 0, ncnt2 = 0, ncnt3 = 0, ncnt4 = 0, ncnt5 = 0;
		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0]) {
				NGRAM_TYPE min = ngi->max;
				unsigned int min_pos = 0;
				for(; *s; s++) {
					unsigned int k = CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_pos = k; /* encoded min ngram */
					}
				}
				ncnt++;
				if (min <= ngi->min) {
					unsigned int rr = ngi->pos[min_pos];
					int hcnt = ngi->h[min_pos];
					ncnt1++;
					//printf("list used %d pos %d,%d, %s\n", hcnt, rr, min_pos, os);
					for(int k = 0; k<hcnt; k++, rr++) {
						unsigned int hr = ngi->rid[rr];
						if (((ngi->sigs[hr] & sig) == sig)) {
							char *hs = BUNtail(hi, hr);
							ncnt3++;
							if (strstr(hs, os) != NULL) {
								*ol++ = hr;
								*or++ = (oid)i;
							}
						}
					}
				} else {
					unsigned int hcnt = BATcount(h);
					ncnt2++;
					for(unsigned int k = 0; k<hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							ncnt4++;
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
				}
				//printf("%d %d\n", min_pos, (int)min);
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) { /* skipped */
				unsigned int hcnt = BATcount(h);
				ncnt++;
				for(unsigned int k = 0; k<hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
		/* printf("max %d\n", nmax); */
		bat_iterator_end(&ni);
		bat_iterator_end(&hi);
		BBPreclaim(h);
		BBPreclaim(n);
		BATsetcount(l, ol - (oid*)Tloc(l, 0));
		BATsetcount(r, ol - (oid*)Tloc(l, 0));
		*L = l->batCacheid;
		*R = r->batCacheid;
		BBPkeepref(l);
		BBPkeepref(r);
		printf("%d, %d, %d, %d, %d, %d, %d\n", ncnt, ncnt1, ncnt2, ncnt3, ncnt4, ncnt5, (int)ngi->min);
		ngrams_destroy(ngi);
		return MAL_SUCCEED;
	}
	BBPreclaim(h);
	BBPreclaim(n);
	throw(MAL, "gram.c1", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
NGc1join1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc1join_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGc1join(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc1join_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}
static int
ngrams_init_2gram(Ngrams *n, BAT *b)
{
	lng t0 = prof_clock(), t1, t2;
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ] = (NGRAM_TYPE (*)[GZ])GDKzalloc(SZ_2GRAM*sizeof(NGRAM_TYPE)),
		*hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_2GRAM*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; p=CHAR_MAP(*s), s++) {
				h[p][CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);
	t0 = prof_clock()-t0;

	int bc = 0;

	t1 = prof_clock();
	for(int i=0; i<SZ_2GRAM; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, SZ_2GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=SZ_2GRAM-1; i>=0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], min = hist[i];
	printf("max %d, first min %d nr of larger %d sum %d, cnt %d\n", (int)hist[0], (int)min, i, (int)sum, (int)cnt);
	n->max = max;
	n->min = min;
	for(int i=0; i<SZ_2GRAM && hist[i] > 0; i++) {
		int y=(id[i]/GZ)%GZ, z=id[i]%GZ;
		idx[y*GZ+z] = NGRAM_CST(1)<<bc;
		assert(idx[y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}
	t1 = prof_clock()-t1;

	t2 = prof_clock();
	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		/* char *os = s; */
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1]) { /* too short skipped */
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; p=CHAR_MAP(*s), s++) {
				int k = p*GZ+CHAR_MAP(*s);
				sig |= idx[k];
				if (n->h[k] <= n->min) {
					if (n->pos[k] == 0) {
						n->pos[k] = pos;
						pos += n->h[k];
						n->h[k] = 0;
					}
					/* deduplicate */
					if (n->h[k] == 0 || n->rid[n->pos[k] + n->h[k] - 1] != i) {
						n->rid[n->pos[k] + n->h[k]] = i;
						n->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	bat_iterator_end(&bi);
	t2 = prof_clock()-t2;
	printf("times %ld, %ld, %ld\n", t0/M, t1/M, t2/M);

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
NGc2join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)nil_matches;
	(void)estimate;
	BAT *h = BATdescriptor(*H);
	BAT *n = BATdescriptor(*N);
	lng t0 = prof_clock();

	if (lc && !is_bat_nil(*lc))
		assert(0);
	if (rc && !is_bat_nil(*rc))
		assert(0);

	if (*anti)
		throw(MAL, "gram.c2", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c2", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
		printf("todo fall back to select \n");
	}

	Ngrams *ngi = ngrams_create(h, SZ_2GRAM);
	if (ngi && ngrams_init_2gram(ngi, h) == 0) {
		t0 = prof_clock()-t0;
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		int ncnt = 0, ncnt1 = 0, ncnt2 = 0, ncnt3 = 0, ncnt4 = 0, ncnt5 = 0;
		lng t1 = 0, t2 = 0, t3 = 0, t4 = 0;
	        lng ncnt6 = 0;
		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0] && s[1]) { /* skipped */
				NGRAM_TYPE min = ngi->min+1;
				unsigned int min_pos = 0, min_posp = 0;
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; p=CHAR_MAP(*s), s++) {
					unsigned int k = p*GZ+CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_posp = min_pos;
						min_pos = k; /* encoded min ngram */
					}
				}
				ncnt++;
				if (min_pos) {
					if (min_posp && min_pos != min_posp && ngi->h[min_posp] < 16 * ngi->h[min_pos]) {
						lng t = prof_clock();
						unsigned int rr = ngi->pos[min_pos], rc = ngi->h[min_pos], er = rr + rc;
						unsigned int rl = ngi->pos[min_posp], lc = ngi->h[min_posp], el = rl + lc;
						ncnt1++;
						ncnt6+=rc;
						//printf("list used %d pos %d,%d, %s\n", rc, rr, min_pos, os);
						while (rr < er && rl < el) {
							unsigned int hr = ngi->rid[rr];
							unsigned int hl = ngi->rid[rl];
							if (hr == hl) {
								if (((ngi->sigs[hr] & sig) == sig)) {
									char *hs = BUNtail(hi, hr);
									ncnt3++;
									if (strstr(hs, os) != NULL) {
										*ol++ = hr;
										*or++ = (oid)i;
									}
								}
								rr++;
								rl++;
							} else if (hr < hl) {
								rr++;
							} else {
								rl++;
							}
						}
						t4 += (prof_clock()-t);
					} else {
						lng t = prof_clock();
						unsigned int rr = ngi->pos[min_pos], rc = ngi->h[min_pos], er = rr + rc;
						ncnt1++;
						ncnt6+=rc;
						//printf("list used %d pos %d,%d, %s\n", rc, rr, min_pos, os);
						for(; rr < er; rr++) {
							unsigned int hr = ngi->rid[rr];
							if (((ngi->sigs[hr] & sig) == sig)) {
								char *hs = BUNtail(hi, hr);
								ncnt3++;
								if (strstr(hs, os) != NULL) {
									*ol++ = hr;
									*or++ = (oid)i;
								}
							}
						}
						t1 += (prof_clock()-t);
					}
				} else {
					lng t = prof_clock();
					unsigned int hcnt = BATcount(h);
					ncnt2++;
					for(unsigned int k = 0; k<hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							ncnt4++;
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
					t2 += (prof_clock()-t);
				}
				//printf("%d %d\n", min_pos, (int)min);
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) {
				lng t = prof_clock();
				unsigned int hcnt = BATcount(h);
				ncnt++;
				printf("os %s\n", os);
				for(unsigned int k = 0; k<hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
				t3 += (prof_clock()-t);
			}
		}
		/* printf("max %d\n", nmax); */
		bat_iterator_end(&ni);
		bat_iterator_end(&hi);
		BBPreclaim(h);
		BBPreclaim(n);
		BATsetcount(l, ol - (oid*)Tloc(l, 0));
		BATsetcount(r, ol - (oid*)Tloc(l, 0));
		*L = l->batCacheid;
		*R = r->batCacheid;
		BBPkeepref(l);
		BBPkeepref(r);
		printf("%d, %d, %d, %d, %d, %d, %ld, %d\n", ncnt, ncnt1, ncnt2, ncnt3, ncnt4, ncnt5, ncnt6, (int)ngi->min);
		printf("times %ld, %ld, %ld, %ld, %ld\n", t0/M, t1/M, t2/M, t3/M, t4/M);
		ngrams_destroy(ngi);
		return MAL_SUCCEED;
	}
	BBPreclaim(h);
	BBPreclaim(n);
	throw(MAL, "gram.c2", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
NGc2join1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc2join_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGc2join(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc2join_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}

static int
ngrams_init_3gram(Ngrams *n, BAT *b)
{
	lng t0 = prof_clock(), t1, t2;
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ][GZ] = (NGRAM_TYPE (*)[GZ][GZ])GDKzalloc(SZ_3GRAM*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_3GRAM*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char o = CHAR_MAP(*s++);
			if (!*s)
			       continue;
			unsigned char p = CHAR_MAP(*s++), q;
			for(; *s; o=p, p=q, s++) {
				q = CHAR_MAP(*s);
				h[o][p][q]++;
			}
		}
	}
	bat_iterator_end(&bi);
	t0 = prof_clock()-t0;

	int bc = 0;

	t1 = prof_clock();
	for(int i=0; i<SZ_3GRAM; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, SZ_3GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=SZ_3GRAM-1; i>=0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], min = hist[i];
	/* printf("max %d, first min %d nr of larger %d sum %d, cnt %d\n", (int)hist[0], (int)min, i, sum, cnt); */
	n->max = max;
	n->min = min;
	for(int i=0; i<SZ_3GRAM && hist[i] > 0; i++) {
		unsigned int x=id[i]/(GZ*GZ), y=(id[i]/GZ)%GZ,
			     z=id[i]%GZ;
		idx[x*GZ*GZ+y*GZ+z] = NGRAM_CST(1)<<bc;
		assert(idx[x*GZ*GZ+y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
		//printf("%d\n", hist[i]);
	}
	t1 = prof_clock()-t1;

	t2 = prof_clock();
	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		/* char *os = s; */
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1] && s[2]) { /* too short skipped */
			unsigned char o = CHAR_MAP(*s++), q;
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; o=p, p=q, s++) {
				q = CHAR_MAP(*s);
				int k = o*GZ*GZ+p*GZ+q;
				sig |= idx[k];
				if (n->h[k] <= n->min) {
					if (n->pos[k] == 0) {
						n->pos[k] = pos;
						pos += n->h[k];
						n->h[k] = 0;
					}
					/* deduplicate */
					if (n->h[k] == 0 || n->rid[n->pos[k] + n->h[k] - 1] != i) {
						n->rid[n->pos[k] + n->h[k]] = i;
						n->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}
	bat_iterator_end(&bi);
	t2 = prof_clock()-t2;
	printf("times %ld, %ld, %ld\n", t0/M, t1/M, t2/M);

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
NGc3join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)nil_matches;
	(void)estimate;
	BAT *h = BATdescriptor(*H);
	BAT *n = BATdescriptor(*N);
	lng t0 = prof_clock();

	if (lc && !is_bat_nil(*lc))
		assert(0);
	if (rc && !is_bat_nil(*rc))
		assert(0);

	if (*anti)
		throw(MAL, "gram.c3", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c3", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
		printf("todo fall back to select \n");
	}

	Ngrams *ngi = ngrams_create(h, SZ_3GRAM);
	if (ngi && ngrams_init_3gram(ngi, h) == 0) {
		t0 = prof_clock()-t0;
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		int ncnt = 0, ncnt1 = 0, ncnt2 = 0, ncnt3 = 0, ncnt4 = 0, ncnt5 = 0;
		lng t1 = 0, t2 = 0, t3 = 0, t4 = 0;
	        lng ncnt6 = 0;
		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0] && s[1] && s[2]) { /* skipped */
				NGRAM_TYPE min = ngi->min+1;
				unsigned int min_pos = 0, min_posp = 0;
				unsigned char pp = CHAR_MAP(*s++);
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; pp=p, p=CHAR_MAP(*s), s++) {
					unsigned int k = pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_posp = min_pos;
						min_pos = k; /* encoded min ngram */
					}
				}
				ncnt++;
				if (min_pos) {
					if (min_posp && min_pos != min_posp && ngi->h[min_posp] < 16 * ngi->h[min_pos]) {
						lng t = prof_clock();
						unsigned int rr = ngi->pos[min_pos], rc = ngi->h[min_pos], er = rr + rc;
						unsigned int rl = ngi->pos[min_posp], lc = ngi->h[min_posp], el = rl + lc;
						ncnt1++;
						ncnt6+=rc;
						//printf("list used %d pos %d,%d, %s\n", rc, rr, min_pos, os);
						while (rr < er && rl < el) {
							unsigned int hr = ngi->rid[rr];
							unsigned int hl = ngi->rid[rl];
							if (hr == hl) {
								if (((ngi->sigs[hr] & sig) == sig)) {
									char *hs = BUNtail(hi, hr);
									ncnt3++;
									if (strstr(hs, os) != NULL) {
										*ol++ = hr;
										*or++ = (oid)i;
									}
								}
								rr++;
								rl++;
							} else if (hr < hl) {
								rr++;
							} else {
								rl++;
							}
						}
						t4 += (prof_clock()-t);
					} else {
						lng t = prof_clock();
						unsigned int rr = ngi->pos[min_pos], rc = ngi->h[min_pos], er = rr + rc;
						ncnt1++;
						ncnt6+=rc;
						//printf("list used %d pos %d,%d, %s\n", rc, rr, min_pos, os);
						for(; rr < er; rr++) {
							unsigned int hr = ngi->rid[rr];
							if (((ngi->sigs[hr] & sig) == sig)) {
								char *hs = BUNtail(hi, hr);
								ncnt3++;
								if (strstr(hs, os) != NULL) {
									*ol++ = hr;
									*or++ = (oid)i;
								}
							}
						}
						t1 += (prof_clock()-t);
					}
				} else {
					lng t = prof_clock();
					unsigned int hcnt = BATcount(h);
					ncnt2++;
					for(unsigned int k = 0; k<hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							ncnt4++;
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
					t2 += (prof_clock()-t);
				}
				//printf("%d %d\n", min_pos, (int)min);
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) { /* skipped */
				lng t = prof_clock();
				unsigned int hcnt = BATcount(h);
				ncnt++;
				for(unsigned int k = 0; k<hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
				t3 += (prof_clock()-t);
			}
		}
		/* printf("max %d\n", nmax); */
		bat_iterator_end(&ni);
		bat_iterator_end(&hi);
		BBPreclaim(h);
		BBPreclaim(n);
		BATsetcount(l, ol - (oid*)Tloc(l, 0));
		BATsetcount(r, ol - (oid*)Tloc(l, 0));
		*L = l->batCacheid;
		*R = r->batCacheid;
		BBPkeepref(l);
		BBPkeepref(r);
		printf("%d, %d, %d, %d, %d, %d, %ld, %d\n", ncnt, ncnt1, ncnt2, ncnt3, ncnt4, ncnt5, ncnt6, (int)ngi->min);
		printf("times %ld, %ld, %ld, %ld, %ld\n", t0/M, t1/M, t2/M, t3/M, t4/M);
		ngrams_destroy(ngi);
		return MAL_SUCCEED;
	}
	BBPreclaim(h);
	BBPreclaim(n);
	throw(MAL, "gram.c3", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
NGc3join1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc3join_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGc3join(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc3join_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}

static int
ngrams_init_4gram(Ngrams *n, BAT *b)
{
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ][GZ][GZ] = (NGRAM_TYPE (*)[GZ][GZ][GZ])GDKzalloc(SZ_4GRAM*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(SZ_4GRAM*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char ppp = CHAR_MAP(*s++);
			if (!*s)
			       continue;
			unsigned char pp = CHAR_MAP(*s++);
			if (!*s)
			       continue;
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; ppp=pp, pp=p, p=CHAR_MAP(*s), s++) {
				h[ppp][pp][p][CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(size_t i=0; i<SZ_4GRAM; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, SZ_4GRAM, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=SZ_4GRAM-1; i>=0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], min = hist[i];
    /* 	printf("max %d, first min %d nr of larger %d sum %d, cnt %d\n", (int)hist[0], (int)min, i, sum, cnt); */
	n->max = max;
	n->min = min;
	for(size_t i=0; i<SZ_4GRAM && hist[i] > 0; i++) {
		unsigned int x=id[i]/(GZ*GZ*GZ), y=(id[i]/GZ*GZ)%GZ, z=id[i]/GZ%GZ, zz=id[i]%GZ;
		idx[x*GZ*GZ*GZ+y*GZ*GZ+z*GZ+zz] = NGRAM_CST(1)<<bc;
		assert(idx[x*GZ*GZ*GZ+y*GZ*GZ+z*GZ+zz] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
			char *s = BUNtail(bi,i);
			/* char *os = s; */
			NGRAM_TYPE sig = 0;
			if (!strNil(s) && s[0] && s[1] && s[2] && s[3]) { /* too short skipped */
				unsigned char ppp = CHAR_MAP(*s++);
				unsigned char pp = CHAR_MAP(*s++);
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; ppp=pp, pp=p, p=CHAR_MAP(*s), s++) {
					int k = ppp*GZ*GZ*GZ+pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
					sig |= idx[k];
					if (n->h[k] <= n->min) {
						if (n->pos[k] == 0) {
							n->pos[k] = pos;
							pos += n->h[k];
							n->h[k] = 0;
						}
						/* deduplicate */
						if (n->h[k] == 0 || n->rid[n->pos[k] + n->h[k] - 1] != i) {
							n->rid[n->pos[k] + n->h[k]] = i;
							n->h[k]++;
						}
					}
				}
				*sp = sig;
			} else {
				*sp = NGRAM_TYPENIL;
			}
			sp++;
	}
	bat_iterator_end(&bi);

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
NGc4join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	(void)nil_matches;
	(void)estimate;
	BAT *h = BATdescriptor(*H);
	BAT *n = BATdescriptor(*N);

	if (lc && !is_bat_nil(*lc))
		assert(0);
	if (rc && !is_bat_nil(*rc))
		assert(0);

	if (*anti)
		throw(MAL, "gram.c4", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c4", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
		printf("todo fall back to select \n");
	}

	Ngrams *ngi = ngrams_create(h, SZ_4GRAM);
	if (ngi && ngrams_init_4gram(ngi, h) == 0) {
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		int ncnt = 0, ncnt1 = 0, ncnt2 = 0, ncnt3 = 0, ncnt4 = 0, ncnt5 = 0;
	        lng ncnt6 = 0;
		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0] && s[1] && s[2] && s[3]) { /* skipped */
				NGRAM_TYPE min = ngi->max;
				unsigned int min_pos = 0;
				unsigned char ppp = CHAR_MAP(*s++);
				unsigned char pp = CHAR_MAP(*s++);
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; ppp=pp, pp=p, p=CHAR_MAP(*s), s++) {
					unsigned int k = ppp*GZ*GZ*GZ+pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_pos = k; /* encoded min ngram */
					}
				}
				ncnt++;
				if (min <= ngi->min) {
					unsigned int rr = ngi->pos[min_pos];
					int hcnt = ngi->h[min_pos];
					ncnt1++;
					ncnt6+=hcnt;
					//printf("list used %d pos %d,%d, %s\n", hcnt, rr, min_pos, os);
					for(int k = 0; k<hcnt; k++, rr++) {
						unsigned int hr = ngi->rid[rr];
						if (((ngi->sigs[hr] & sig) == sig)) {
							char *hs = BUNtail(hi, hr);
							ncnt3++;
							if (strstr(hs, os) != NULL) {
								*ol++ = hr;
								*or++ = (oid)i;
							}
						}
					}
				} else {
					unsigned int hcnt = BATcount(h);
					ncnt2++;
					for(unsigned int k = 0; k<hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							ncnt4++;
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
				}
				//printf("%d %d\n", min_pos, (int)min);
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) { /* skipped */
				unsigned int hcnt = BATcount(h);
				ncnt++;
				for(unsigned int k = 0; k<hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
	/* 	printf("max %d\n", nmax); */
		bat_iterator_end(&ni);
		bat_iterator_end(&hi);
		BBPreclaim(h);
		BBPreclaim(n);
		BATsetcount(l, ol - (oid*)Tloc(l, 0));
		BATsetcount(r, ol - (oid*)Tloc(l, 0));
		*L = l->batCacheid;
		*R = r->batCacheid;
		BBPkeepref(l);
		BBPkeepref(r);
		printf("%d, %d, %d, %d, %d, %d, %ld, %d\n", ncnt, ncnt1, ncnt2, ncnt3, ncnt4, ncnt5, ncnt6, (int)ngi->min);
		ngrams_destroy(ngi);
		return MAL_SUCCEED;
	}
	BBPreclaim(h);
	BBPreclaim(n);
	throw(MAL, "gram.c4", SQLSTATE(HY013) MAL_MALLOC_FAIL);
}

static str
NGc4join1(bat *L, bat *sigs, bat *needle, bat *lc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc4join_intern(L, NULL, sigs, needle, lc, NULL, nil_matches, estimate, anti);
}

static str
NGc4join(bat *L, bat *R, bat *sigs, bat *needle, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
{
	return NGc4join_intern(L, R, sigs, needle, lc, rc, nil_matches, estimate, anti);
}

#include "mel.h"
/* static char ngram_sql[] = "CREATE FUNCTION signature(val STRING, n INTEGER) RETURNS BIGINT EXTERNAL NAME ngram.signature;"; */

static mel_func ngram_init_funcs[] = {
	command("ngram", "signature_sht", NGsignature_dummy, false,
		"Return a signature for the given string",
		args(1,3, arg("signature",sht), arg("string",str), arg("n",sht))),
	command("batngram", "signature_sht", NGsignature_sht, false,
		"Return a signature for each string in the bat 'b'",
		args(1,3, batarg("signature",sht), batarg("b",str), arg("n",sht))),
	command("ngram", "and", NGand_sht, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("sig", sht), arg("needle", sht))),
	command("ngram", "andselect", NGandselect_sht, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("sigs", sht), batarg("s", oid), arg("needle", sht), arg("anti", bit))),

	command("ngram", "signature_int", NGsignature_dummy, false,
		"Return a signature for the given string",
		args(1,3, arg("signature",int), arg("string",str), arg("n",int))),
	command("batngram", "signature_int", NGsignature_int, false,
		"Return a signature for each string in the bat 'b'",
		args(1,3, batarg("signature",int), batarg("b",str), arg("n",int))),
	command("ngram", "and", NGand_int, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("sig", int), arg("needle", int))),
	command("ngram", "andselect", NGandselect_int, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("sigs", int), batarg("s", oid), arg("needle", int), arg("anti", bit))),

	command("ngram", "popcnt", NGpopcnt, false,
		"count number of bits set",
		args(1, 2, arg("res", int), arg("sig", lng))),

	command("ngram", "signature_lng", NGsignature_dummy, false,
		"Return a signature for the given string",
		args(1,3, arg("signature",lng), arg("string",str), arg("n",int))),
	command("batngram", "signature_lng", NGsignature_lng, false,
		"Return a signature for each string in the bat 'b'",
		args(1,3, batarg("signature",lng), batarg("b",str), arg("n",int))),
	command("ngram", "and", NGand_lng, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("sig", lng), arg("needle", lng))),
	command("ngram", "andselect", NGandselect_lng, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("sigs", lng), batarg("s", oid), arg("needle", lng), arg("anti", bit))),

	command("ngram", "andjoin", NGandjoin1, false,
		"predicate if value and needle equal needle",
		args(1, 8, batarg("l", oid), batarg("sigs", lng), batarg("needle", lng), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "andjoin", NGandjoin, false,
		"predicate if value and needle equal needle",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("sigs", lng), batarg("needle", lng), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	command("ngram", "signature_hge", NGsignature_dummy, false,
		"Return a signature for the given string",
		args(1,3, arg("signature",hge), arg("string",str), arg("n",int))),
	command("batngram", "signature_hge", NGsignature_hge, false,
		"Return a signature for each string in the bat 'b'",
		args(1,3, batarg("signature",hge), batarg("b",str), arg("n",int))),
	command("ngram", "and", NGand_hge, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("sig", hge), arg("needle", hge))),
	command("ngram", "andselect", NGandselect_hge, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("sigs", hge), batarg("s", oid), arg("needle", hge), arg("anti", bit))),

	command("ngram", "gor", gor_lng, false,
		"aggregate signatures",
		args(1,2, arg("",lng), batarg("b",lng))),
	command("ngram", "gor", grouped_gor_lng, false,
		"aggregate signatures",
		args(1,4, batarg("",lng), batarg("b",lng), batarg("g",oid), batargany("e",1))),
	command("ngram", "subgor", subgrouped_gor_lng, false,
		"aggregate signatures",
		args(1,5, batarg("",lng), batarg("b",lng), batarg("g",oid), batargany("e",1), arg("skip_nils",bit))),
	command("ngram", "subgor", subgrouped_gor_cand_lng, false,
		"aggregate signatures",
		args(1,6, batarg("",lng), batarg("b",lng), batarg("g",oid), batargany("e",1), batarg("s",oid), arg("skip_nils",bit))),

	command("ngram", "c1", NGcx, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngram", "c1select", NGcxselect, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngram", "c1join", NGc1join1, false,
		"predicate if value and needle equal needle (using 1gram)",
		args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "c1join", NGc1join, false,
		"predicate if value and needle equal needle (using 1gram)",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	command("ngram", "c2", NGcx, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngram", "c2select", NGcxselect, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngram", "c2join", NGc2join1, false,
		"predicate if value and needle equal needle (using 2gram)",
		args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "c2join", NGc2join, false,
		"predicate if value and needle equal needle (using 2gram)",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	command("ngram", "c3", NGcx, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngram", "c3select", NGcxselect, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngram", "c3join", NGc3join1, false,
		"predicate if value and needle equal needle (using 3gram)",
		args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "c3join", NGc3join, false,
		"predicate if value and needle equal needle (using 3gram)",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	command("ngram", "c4", NGcx, false,
		"predicate if value and needle equal needle",
		args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngram", "c4select", NGcxselect, false,
		"predicate if value and needle equal needle",
		args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngram", "c4join", NGc4join1, false,
		"predicate if value and needle equal needle (using 3gram)",
		args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngram", "c4join", NGc4join, false,
		"predicate if value and needle equal needle (using 3gram)",
		args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	{ .imp=NULL }		/* sentinel */
};
#include "mal_import.h"
/* #include "sql_import.h" */
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_ngram)
{
	mal_module("ngram", NULL, ngram_init_funcs);
	/* sql_register("ngram", ngram_sql); */
}
