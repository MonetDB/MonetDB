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

#include "monetdb_config.h"
#include "mal_interpreter.h"
#include "mal_exception.h"
#include "string.h"
#include "str.h"

#if HAVE_HGE
#define GZ 128
#define CHAR_MAP(s) (s&127)
#else
#define GZ 64
#define CHAR_MAP(s) (s&63)
#endif

#define UNIGRAM_SZ GZ
#define BIGRAM_SZ (GZ*GZ)
#define TRIGRAM_SZ (GZ*GZ*GZ)

#if HAVE_HGE
#define NGRAM_TYPE hge
#define NGRAM_TYPEID TYPE_hge
#define NGRAM_TYPENIL hge_nil
#define NGRAM_CST(v) ((hge)LL_CONSTANT(v))
#define NGRAM_BITS 127
#else
#define NGRAM_TYPE lng
#define NGRAM_TYPEID TYPE_lng
#define NGRAM_TYPENIL lng_nil
#define NGRAM_CST(v) LL_CONSTANT(v)
#define NGRAM_BITS 63
#endif

#define NGRAM_MULTIPLE 16

#define SET_EMPTY_BAT_PROPS(B)					\
	do {										\
		B->tnil = false;						\
		B->tnonil = true;						\
		B->tkey = true;							\
		B->tsorted = true;						\
		B->trevsorted = true;					\
		B->tseqbase = 0;						\
	} while (0)

static inline void
BBPreclaim_n(int nargs, ...)
{
	va_list valist;
	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		BBPreclaim(b);
	}
	va_end(valist);
}

typedef struct {
	NGRAM_TYPE *idx;
	NGRAM_TYPE *sigs;
	NGRAM_TYPE max, min;
	unsigned int *h;
	unsigned int *pos;
	unsigned int *rid;
} Ngrams;

static void
ngrams_destroy(Ngrams *ng)
{
	if (ng) {
		GDKfree(ng->h);
		GDKfree(ng->idx);
		GDKfree(ng->pos);
		GDKfree(ng->rid);
		GDKfree(ng->sigs);
	}
	GDKfree(ng);
}

static Ngrams *
ngrams_create_old(BAT *b, size_t ngramsize)
{
	Ngrams *n = NULL;
	size_t sz = BATcount(b);

	n = (Ngrams*)GDKmalloc(sizeof(Ngrams));
	if (n) {
		n->h = (unsigned int*)GDKmalloc(ngramsize*sizeof(int));
		n->pos = (unsigned int*)GDKzalloc(ngramsize*sizeof(int));
		n->rid = (unsigned int*)GDKmalloc(NGRAM_MULTIPLE* sz * sizeof(int));
		n->idx = (NGRAM_TYPE*)GDKmalloc(ngramsize*sizeof(NGRAM_TYPE));
		n->sigs = (NGRAM_TYPE*)GDKmalloc(sz * sizeof(NGRAM_TYPE));
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
	NGRAM_TYPE *h = (NGRAM_TYPE *)GDKzalloc(UNIGRAM_SZ*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(UNIGRAM_SZ*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			for(; *s; s++) {
				h[CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(int i=0; i<UNIGRAM_SZ; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, UNIGRAM_SZ, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=UNIGRAM_SZ-1; i>=0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	n->max = max;
	n->min = small;

	for(int i=0; i<UNIGRAM_SZ && hist[i] > 0; i++) {
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
		const char *s = BUNtail(bi, i);
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
					int done =  (n->h[k] > 0 && n->rid[n->pos[k] + n->h[k]-1] == i);
					if (!done) {
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

	Ngrams *ngi = ngrams_create_old(h, UNIGRAM_SZ);
	if (ngi && ngrams_init_1gram(ngi, h) == 0) { /* TODO add locks and only create ngram once for full (parent bat) */
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
			const char *s = BUNtail(ni,i), *os = s;
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
					for(size_t k = 0; k < hcnt; k++) {
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
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) { /* skipped */
				unsigned int hcnt = BATcount(h);
				ncnt++;
				for(size_t k = 0; k < hcnt; k++) {
					char *hs = BUNtail(hi, k);
					ncnt5++;
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
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
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ] = (NGRAM_TYPE (*)[GZ])GDKzalloc(BIGRAM_SZ*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(BIGRAM_SZ*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; p=CHAR_MAP(*s), s++) {
				h[p][CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(int i=0; i<BIGRAM_SZ; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];
	}
	GDKqsort(h, id, NULL, BIGRAM_SZ, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=BIGRAM_SZ-1; i>=0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	n->max = max;
	n->min = small;
	for(int i=0; i<BIGRAM_SZ && hist[i] > 0; i++) {
		int y=(id[i]/GZ)%GZ, z=id[i]%GZ;
		idx[y*GZ+z] = NGRAM_CST(1)<<bc;
		assert(idx[y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi, i);
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
					int done =  (n->h[k] > 0 && n->rid[n->pos[k] + n->h[k]-1] == i);
					if (!done) {
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
NGc2join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
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
		throw(MAL, "gram.c2", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c2", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
	}

	Ngrams *ng = ngrams_create_old(h, BIGRAM_SZ);
	if (ng && ngrams_init_2gram(ng, h) == 0) {
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			const char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0] && s[1]) { /* skipped */
				NGRAM_TYPE min = ng->max;
				unsigned int min_pos = 0;
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; p=CHAR_MAP(*s), s++) {
					unsigned int k = p*GZ+CHAR_MAP(*s);
					sig |= ng->idx[k];
					if (ng->h[k] < min) {
						min = ng->h[k];
						min_pos = k; /* encoded min ngram */
					}
				}
				if (min <= ng->min) {
					unsigned int rr = ng->pos[min_pos];
					int hcnt = ng->h[min_pos];
					for(int k = 0; k<hcnt; k++, rr++) {
						unsigned int hr = ng->rid[rr];
						if (((ng->sigs[hr] & sig) == sig)) {
							char *hs = BUNtail(hi, hr);
							if (strstr(hs, os) != NULL) {
								*ol++ = hr;
								*or++ = (oid)i;
							}
						}
					}
				} else {
					unsigned int hcnt = BATcount(h);
					for(size_t k = 0; k < hcnt; k++) {
						if (((ng->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
				}
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) {
				unsigned int hcnt = BATcount(h);
				for(size_t k = 0; k < hcnt; k++) {
					char *hs = BUNtail(hi, k);
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
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
		ngrams_destroy(ng);
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
	BUN cnt = BATcount(b);
	NGRAM_TYPE (*h)[GZ][GZ] = (NGRAM_TYPE (*)[GZ][GZ])GDKzalloc(TRIGRAM_SZ*sizeof(NGRAM_TYPE)), *hist = (NGRAM_TYPE*)h, sum = 0;
	int *id = (int*)GDKmalloc(TRIGRAM_SZ*sizeof(int)), i;
	NGRAM_TYPE *idx = n->idx;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	BATiter bi = bat_iterator(b);
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi,i);
		if (!strNil(s) && *s) { /* skipped */
			unsigned char pp = CHAR_MAP(*s++);
			if (!*s)
			       continue;
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp=p, p=CHAR_MAP(*s), s++) {
				h[pp][p][CHAR_MAP(*s)]++;
			}
		}
	}
	bat_iterator_end(&bi);

	int bc = 0;

	for(int i=0; i<TRIGRAM_SZ; i++) {
		id[i] = i;
		idx[i] = 0;
		n->h[i] = (unsigned int)hist[i];  /* TODO check for overflow ? */
	}
	GDKqsort(h, id, NULL, TRIGRAM_SZ, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i=TRIGRAM_SZ-1; i>=0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE*cnt)-1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	n->max = max;
	n->min = small;
	for(int i=0; i<TRIGRAM_SZ && hist[i] > 0; i++) {
		unsigned int x=id[i]/(GZ*GZ), y=(id[i]/GZ)%GZ,
			     z=id[i]%GZ;
		idx[x*GZ*GZ+y*GZ+z] = NGRAM_CST(1)<<bc;
		assert(idx[x*GZ*GZ+y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	bi = bat_iterator(b);
	NGRAM_TYPE *sp = n->sigs;
	unsigned int pos = 1;
	for(BUN i=0; i<cnt; i++) {
		const char *s = BUNtail(bi, i);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1] && s[2]) { /* too short skipped */
			unsigned char pp = CHAR_MAP(*s++);
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp=p, p=CHAR_MAP(*s), s++) {
				int k = pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
				sig |= idx[k];
				if (n->h[k] <= n->min) {
					if (n->pos[k] == 0) {
						n->pos[k] = pos;
						pos += n->h[k];
						n->h[k] = 0;
					}
					/* deduplicate */
					int done =  (n->h[k] > 0 && n->rid[n->pos[k] + n->h[k]-1] == i);
					if (!done) {
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
NGc3join_intern(bat *L, bat *R, bat *H, bat *N, bat *lc, bat *rc, bit *nil_matches, lng *estimate, bit *anti)
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
		throw(MAL, "gram.c3", "No anti contains yet\n");
	if (!h || !n) {
		BBPreclaim(h);
		BBPreclaim(n);
		throw(MAL, "gram.c3", RUNTIME_OBJECT_MISSING);
	}

	if (BATcount(n) < 10) {
	}

	Ngrams *ngi = ngrams_create_old(h, TRIGRAM_SZ);
	if (ngi && ngrams_init_3gram(ngi, h) == 0) { /* TODO add locks and only create ngram once for full (parent bat) */
		BUN cnt = BATcount(h);
		/* create L/R */
		BAT *l = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);
		BAT *r = COLnew(0, TYPE_oid, 10*cnt, TRANSIENT);

		BATiter ni = bat_iterator(n);
		BATiter hi = bat_iterator(h);
		NGRAM_TYPE nmax = 0;
		oid *ol = Tloc(l, 0), *el = ol + 10*cnt;
		oid *or = Tloc(r, 0);
		cnt = BATcount(n);
		/* if needed grow */
		for(BUN i = 0; i<cnt; i++) {
			const char *s = BUNtail(ni,i), *os = s;
			NGRAM_TYPE sig = 0;

			if ((ol+1000) > el)
				break;
			if (!strNil(s) && s[0] && s[1] && s[2]) { /* skipped */
				NGRAM_TYPE min = ngi->max;
				unsigned int min_pos = 0;
				unsigned char pp = CHAR_MAP(*s++);
				unsigned char p = CHAR_MAP(*s++);
				for(; *s; pp=p, p=CHAR_MAP(*s), s++) {
					unsigned int k = pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
					sig |= ngi->idx[k];
					if (ngi->h[k] < min) {
						min = ngi->h[k];
						min_pos = k; /* encoded min ngram */
					}
				}
				if (min <= ngi->min) {
					unsigned int rr = ngi->pos[min_pos];
					unsigned int hcnt = ngi->h[min_pos]; /* no unsigned */
					for(size_t k = 0; k < hcnt; k++, rr++) {
						unsigned int hr = ngi->rid[rr];
						if (((ngi->sigs[hr] & sig) == sig)) {
							char *hs = BUNtail(hi, hr);
							if (strstr(hs, os) != NULL) {
								*ol++ = hr;
								*or++ = (oid)i;
							}
						}
					}
				} else {
					unsigned int hcnt = BATcount(h);
					for(size_t k = 0; k < hcnt; k++) {
						if (((ngi->sigs[k] & sig) == sig)) {
							char *hs = BUNtail(hi, k);
							if (strstr(hs, os) != NULL) {
								*ol++ = k;
								*or++ = (oid)i;
							}
						}
					}
				}
				if (min > nmax)
					nmax = min;
			} else if (!strNil(s)) { /* skipped */
				unsigned int hcnt = BATcount(h);
				for(size_t k = 0; k < hcnt; k++) {
					char *hs = BUNtail(hi, k);
					if (strstr(hs, os) != NULL) {
						*ol++ = k;
						*or++ = (oid)i;
					}
				}
			}
		}
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

static Ngrams *
ngrams_create(size_t b_sz, size_t ng_sz)
{
	Ngrams *ng = GDKmalloc(sizeof(Ngrams));
	if (ng) {
		ng->idx = GDKmalloc(ng_sz * sizeof(NGRAM_TYPE));
		ng->sigs = GDKmalloc(b_sz * sizeof(NGRAM_TYPE));
		ng->h = GDKmalloc(ng_sz * sizeof(unsigned int));
		ng->pos = GDKzalloc(ng_sz * sizeof(unsigned int));
		ng->rid = GDKmalloc(NGRAM_MULTIPLE * b_sz * sizeof(unsigned int));
	}
	if (!ng || !ng->h || !ng->idx || !ng->pos || !ng->rid || !ng->sigs) {
		ngrams_destroy(ng);
		return NULL;
	}
	return ng;
}

static str
ngram_choice(const bat *NG, bte *ngram, const char *fname)
{
	BAT *ng = NULL;
	if ((ng = BATdescriptor(*NG)) == NULL)
		throw(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);

	BATiter bi = bat_iterator(ng);
	if (bi.count != 1) {
		bat_iterator_end(&bi);
		BBPreclaim(ng);
		if (bi.count < 1)
			throw(MAL, fname, SQLSTATE(42000) "Empty bat\n");
		else
			throw(MAL, fname, SQLSTATE(42000) "Single value bat expected\n");
	}
	*ngram = *(bte *) BUNtloc(bi, 0);
	bat_iterator_end(&bi);
	BBPreclaim(ng);
	return MAL_SUCCEED;
}

static int
init_unigram_idx(Ngrams *ng, BATiter *bi, size_t b_cnt)
{
	NGRAM_TYPE *h = GDKzalloc(UNIGRAM_SZ * sizeof(NGRAM_TYPE)),
		*hist = h, sum = 0;
	NGRAM_TYPE *idx = ng->idx;
	int *id = GDKmalloc(UNIGRAM_SZ*sizeof(int)), i;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	for(size_t j = 0; j < b_cnt; j++) {
		const char *s = BUNtail(*bi, j);
		if (!strNil(s) && *s) {
			for(; *s; s++) {
				h[CHAR_MAP(*s)]++;
			}
		}
	}

	int bc = 0;

	for(size_t j = 0; j < UNIGRAM_SZ; j++) {
		id[j] = j;
		idx[j] = 0;
		ng->h[j] = (unsigned int)hist[j];
	}
	GDKqsort(h, id, NULL, UNIGRAM_SZ, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for(i = UNIGRAM_SZ - 1; i >= 0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE * b_cnt) - 1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	ng->max = max;
	ng->min = small;

	for(size_t j = 0; j < UNIGRAM_SZ && hist[j] > 0; j++) {
		unsigned int x = id[j];
		idx[x] = NGRAM_CST(1) << bc;
		assert(idx[x] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	NGRAM_TYPE *sp = ng->sigs;
	unsigned int pos = 1;
	for(size_t j = 0; j < b_cnt; j++) {
		const char *s = BUNtail(*bi, j);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0]) {
			for(; *s; s++) {
				int k = CHAR_MAP(*s);
				sig |= idx[k];
				if (ng->h[k] <= ng->min) {
					if (ng->pos[k] == 0) {
						ng->pos[k] = pos;
						pos += ng->h[k];
						ng->h[k] = 0;
					}
					/* deduplicate */
					int done =  (ng->h[k] > 0 && ng->rid[ng->pos[k] + ng->h[k]-1] == j);
					if (!done) {
						ng->rid[ng->pos[k] + ng->h[k]] = j;
						ng->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static int
init_bigram_idx(Ngrams *ng, BATiter *bi, size_t b_cnt)
{
	NGRAM_TYPE (*h)[GZ] = (NGRAM_TYPE (*)[GZ])GDKzalloc(BIGRAM_SZ * sizeof(NGRAM_TYPE)),
		*hist = (NGRAM_TYPE*)h, sum = 0;
	NGRAM_TYPE *idx = ng->idx;
	int *id = GDKmalloc(BIGRAM_SZ * sizeof(int)), i;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	for (size_t j = 0; j < b_cnt; j++) {
		const char *s = BUNtail(*bi, j);
		if (!strNil(s) && *s) {
			unsigned char p = CHAR_MAP(*s++);
			for (; *s; p = CHAR_MAP(*s), s++) {
				h[p][CHAR_MAP(*s)]++;
			}
		}
	}

	int bc = 0;

	for(size_t j = 0; j < BIGRAM_SZ; j++) {
		id[j] = j;
		idx[j] = 0;
		ng->h[j] = (unsigned int)hist[j];
	}
	GDKqsort(h, id, NULL, BIGRAM_SZ, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for (i = BIGRAM_SZ - 1; i >= 0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE * b_cnt) - 1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for(; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	ng->max = max;
	ng->min = small;

	for (size_t j = 0; j < BIGRAM_SZ && hist[j] > 0; j++) {
		int y = (id[j]/GZ) % GZ, z = id[j] % GZ;
		idx[y*GZ+z] = NGRAM_CST(1) << bc;
		assert(idx[y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	NGRAM_TYPE *sp = ng->sigs;
	unsigned int pos = 1;
	for(size_t j = 0; j < b_cnt; j++) {
		const char *s = BUNtail(*bi, j);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1]) {
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; p = CHAR_MAP(*s), s++) {
				int k = p*GZ+CHAR_MAP(*s);
				sig |= idx[k];
				if (ng->h[k] <= ng->min) {
					if (ng->pos[k] == 0) {
						ng->pos[k] = pos;
						pos += ng->h[k];
						ng->h[k] = 0;
					}
					/* deduplicate */
					int done =  (ng->h[k] > 0 && ng->rid[ng->pos[k] + ng->h[k]-1] == j);
					if (!done) {
						ng->rid[ng->pos[k] + ng->h[k]] = j;
						ng->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static int
init_trigram_idx(Ngrams *ng, BATiter *bi, size_t b_cnt)
{
	NGRAM_TYPE (*h)[GZ][GZ] = (NGRAM_TYPE (*)[GZ][GZ])GDKzalloc(TRIGRAM_SZ * sizeof(NGRAM_TYPE)),
		*hist = (NGRAM_TYPE*)h, sum = 0;
	NGRAM_TYPE *idx = ng->idx;
	int *id = (int*)GDKmalloc(TRIGRAM_SZ*sizeof(int)), i;

	if (!h || !id) {
		GDKfree(h);
		GDKfree(id);
		return -1;
	}

	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi,i);
		if (!strNil(s) && *s) {
			unsigned char pp = CHAR_MAP(*s++);
			if (!*s)
				continue;
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp = p, p = CHAR_MAP(*s), s++)
				h[pp][p][CHAR_MAP(*s)]++;
		}
	}

	int bc = 0;

	for (size_t j = 0; j < TRIGRAM_SZ; j++) {
		id[j] = j;
		idx[j] = 0;
		ng->h[j] = (unsigned int)hist[j];  /* TODO check for overflow ? */
	}
	GDKqsort(h, id, NULL, TRIGRAM_SZ, sizeof(NGRAM_TYPE), sizeof(int), NGRAM_TYPEID, true, false);
	for (i = TRIGRAM_SZ - 1; i >= 0; i--) {
		if ((sum + hist[i]) >= (NGRAM_MULTIPLE * b_cnt) - 1)
			break;
		sum += hist[i];
	}
	NGRAM_TYPE larger_cnt = hist[i];
	for (; hist[i] == larger_cnt; i++)
		;
	NGRAM_TYPE max = hist[0], small = hist[i];
	ng->max = max;
	ng->min = small;

	for (size_t j = 0; j < TRIGRAM_SZ && hist[j] > 0; j++) {
		unsigned int x = id[j]/(GZ*GZ), y = (id[j]/GZ)%GZ,
			z = id[j]%GZ;
		idx[x*GZ*GZ+y*GZ+z] = NGRAM_CST(1)<<bc;
		assert(idx[x*GZ*GZ+y*GZ+z] > 0);
		bc++;
		bc %= NGRAM_BITS;
	}

	NGRAM_TYPE *sp = ng->sigs;
	unsigned int pos = 1;
	for (size_t j = 0; j < b_cnt; j++) {
		const char *s = BUNtail(*bi, j);
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1] && s[2]) {
			unsigned char pp = CHAR_MAP(*s++);
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp = p, p = CHAR_MAP(*s), s++) {
				int k = pp*GZ*GZ+p*GZ+CHAR_MAP(*s);
				sig |= idx[k];
				if (ng->h[k] <= ng->min) {
					if (ng->pos[k] == 0) {
						ng->pos[k] = pos;
						pos += ng->h[k];
						ng->h[k] = 0;
					}
					/* deduplicate */
					int done =  (ng->h[k] > 0 && ng->rid[ng->pos[k] + ng->h[k]-1] == j);
					if (!done) {
						ng->rid[ng->pos[k] + ng->h[k]] = j;
						ng->h[k]++;
					}
				}
			}
			*sp = sig;
		} else {
			*sp = NGRAM_TYPENIL;
		}
		sp++;
	}

	GDKfree(h);
	GDKfree(id);
	return 0;
}

static str
join_unigram(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			 size_t l_cnt, size_t r_cnt,
			 int (*str_cmp)(const char *, const char *, int))
{
	Ngrams *ng = ngrams_create(l_cnt, UNIGRAM_SZ);

	if (!ng)
		throw(MAL, "join_unigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (init_unigram_idx(ng, li, l_cnt) != 0)
		throw(MAL, "join_unigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	NGRAM_TYPE nmax = 0;
	oid *ol = Tloc(rl, 0), *el = ol + 10 * l_cnt;
	oid *or = Tloc(rr, 0);

	/* if needed grow */
	for(size_t i = 0; i < r_cnt; i++) {
		const char *s = BUNtail(*ri, i), *os = s;
		NGRAM_TYPE sig = 0;

		if ((ol + 1000) > el)
			break;
		if (!strNil(s) && s[0]) {
			NGRAM_TYPE min = ng->max;
			unsigned int min_pos = 0;
			for(; *s; s++) {
				unsigned int k = CHAR_MAP(*s);
				sig |= ng->idx[k];
				if (ng->h[k] < min) {
					min = ng->h[k];
					min_pos = k; /* encoded min ngram */
				}
			}
			if (min <= ng->min) {
				unsigned int rr = ng->pos[min_pos];
				int hcnt = ng->h[min_pos];
				for(int k = 0; k<hcnt; k++, rr++) {
					unsigned int hr = ng->rid[rr];
					if (((ng->sigs[hr] & sig) == sig)) {
						char *hs = BUNtail(*li, hr);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*ol++ = hr;
							*or++ = (oid)i;
						}
					}
				}
			} else {
				for(size_t k = 0; k < l_cnt; k++) {
					if (((ng->sigs[k] & sig) == sig)) {
						char *hs = BUNtail(*li, k);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*ol++ = k;
							*or++ = (oid)i;
						}
					}
				}
			}
			if (min > nmax)
				nmax = min;
		} else if (!strNil(s)) {
			for(size_t k = 0; k < l_cnt; k++) {
				char *hs = BUNtail(*li, k);
				if (str_cmp(hs, os, str_strlen(os)) == 0) {
					*ol++ = k;
					*or++ = (oid)i;
				}
			}
		}
	}

	BATsetcount(rl, ol - (oid*)Tloc(rl, 0));
	BATsetcount(rr, ol - (oid*)Tloc(rl, 0));
	ngrams_destroy(ng);
	return MAL_SUCCEED;
}

static str
join_bigram(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			 size_t l_cnt, size_t r_cnt,
			 int (*str_cmp)(const char *, const char *, int))
{
	Ngrams *ng = ngrams_create(l_cnt, BIGRAM_SZ);

	if (!ng)
		throw(MAL, "join_bigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (init_bigram_idx(ng, li, l_cnt) != 0)
		throw(MAL, "join_bigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	NGRAM_TYPE nmax = 0;
	oid *ol = Tloc(rl, 0), *el = ol + 10 * l_cnt;
	oid *or = Tloc(rr, 0);

	/* if needed grow */
	for(size_t i = 0; i < r_cnt; i++) {
		const char *s = BUNtail(*ri, i), *os = s;
		NGRAM_TYPE sig = 0;

		if ((ol + 1000) > el)
			break;
		if (!strNil(s) && s[0] && s[1]) { /* skipped */
			NGRAM_TYPE min = ng->max;
			unsigned int min_pos = 0;
			unsigned char p = CHAR_MAP(*s++);
			for (; *s; p = CHAR_MAP(*s), s++) {
				unsigned int k = p * GZ + CHAR_MAP(*s);
				sig |= ng->idx[k];
				if (ng->h[k] < min) {
					min = ng->h[k];
					min_pos = k; /* encoded min ngram */
				}
			}
			if (min <= ng->min) {
				unsigned int rr = ng->pos[min_pos];
				int hcnt = ng->h[min_pos];
				for (int k = 0; k < hcnt; k++, rr++) {
					unsigned int hr = ng->rid[rr];
					if (((ng->sigs[hr] & sig) == sig)) {
						char *hs = BUNtail(*li, hr);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*ol++ = hr;
							*or++ = (oid)i;
						}
					}
				}
			} else {
				for (size_t k = 0; k < l_cnt; k++) {
					if (((ng->sigs[k] & sig) == sig)) {
						char *hs = BUNtail(*li, k);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*ol++ = k;
							*or++ = (oid)i;
						}
					}
				}
			}
			if (min > nmax)
				nmax = min;
		} else if (!strNil(s)) {
			for (size_t k = 0; k < l_cnt; k++) {
				char *hs = BUNtail(*li, k);
				if (str_cmp(hs, os, str_strlen(os)) == 0) {
					*ol++ = k;
					*or++ = (oid)i;
				}
			}
		}
	}

	BATsetcount(rl, ol - (oid*)Tloc(rl, 0));
	BATsetcount(rr, ol - (oid*)Tloc(rl, 0));
	ngrams_destroy(ng);
	return MAL_SUCCEED;
}

static str
join_trigram(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			 size_t l_cnt, size_t r_cnt,
			 int (*str_cmp)(const char *, const char *, int))
{
	Ngrams *ng = ngrams_create(l_cnt, TRIGRAM_SZ);

	if (!ng)
		throw(MAL, "join_trigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (init_trigram_idx(ng, li, l_cnt) != 0)
		throw(MAL, "join_trigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	NGRAM_TYPE nmax = 0;
	oid *ol = Tloc(rl, 0), *el = ol + 10 * l_cnt;
	oid *or = Tloc(rr, 0);

	/* if needed grow */
	for(size_t i = 0; i < r_cnt; i++) {
		const char *s = BUNtail(*ri, i), *os = s;
		NGRAM_TYPE sig = 0;

		if ((ol + 1000) > el)
			break;
		if (!strNil(s) && s[0] && s[1] && s[2]) {
			NGRAM_TYPE min = ng->max;
			unsigned int min_pos = 0;
			unsigned char pp = CHAR_MAP(*s++);
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp = p, p = CHAR_MAP(*s), s++) {
				unsigned int k = pp *GZ * GZ + p * GZ + CHAR_MAP(*s);
				sig |= ng->idx[k];
				if (ng->h[k] < min) {
					min = ng->h[k];
					min_pos = k; /* encoded min ngram */
				}
			}
			if (min <= ng->min) {
				unsigned int rr = ng->pos[min_pos];
				unsigned int hcnt = ng->h[min_pos]; /* no unsigned */
				for (size_t k = 0; k < hcnt; k++, rr++) {
					unsigned int hr = ng->rid[rr];
					if (((ng->sigs[hr] & sig) == sig)) {
						char *hs = BUNtail(*li, hr);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*ol++ = hr;
							*or++ = (oid)i;
						}
					}
				}
			} else {
				for (size_t k = 0; k < l_cnt; k++) {
					if (((ng->sigs[k] & sig) == sig)) {
						char *hs = BUNtail(*li, k);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*ol++ = k;
							*or++ = (oid)i;
						}
					}
				}
			}
			if (min > nmax)
				nmax = min;
		} else if (!strNil(s)) {
			for (size_t k = 0; k < l_cnt; k++) {
				char *hs = BUNtail(*li, k);
				if (str_cmp(hs, os, str_strlen(os)) == 0) {
					*ol++ = k;
					*or++ = (oid)i;
				}
			}
		}
	}

	BATsetcount(rl, ol - (oid*)Tloc(rl, 0));
	BATsetcount(rr, ol - (oid*)Tloc(rl, 0));
	ngrams_destroy(ng);
	return MAL_SUCCEED;
}

static str
NGjoin(MalStkPtr stk, InstrPtr pci,
	   int (*str_cmp)(const char *, const char *, int),
	   const char *fname)
{
	str msg = MAL_SUCCEED;
	bat *RL = getArgReference(stk, pci, 0);
	bat *RR = pci->retc == 1 ? 0 : getArgReference(stk, pci, 1);
	int offset = pci->retc == 1 ? 1 : 2;
	bat *L = getArgReference(stk, pci, offset++);
	bat *R = getArgReference(stk, pci, offset++);
	bat *NG = pci->argc - pci->retc == 7 ? NULL : getArgReference(stk, pci, offset++);
	bat *CL = getArgReference(stk, pci, offset++);
	bat *CR = getArgReference(stk, pci, offset++);
	bat *nil_matches = getArgReference(stk, pci, offset++);
	bat *estimates = getArgReference(stk, pci, offset++);
	bit *anti = getArgReference(stk, pci, offset++);
	BAT *rl = NULL, *rr = NULL, *l = NULL, *r = NULL;
	(void)nil_matches;
	(void)estimates;

	if (*anti)
		throw(MAL, fname, "No anti join yet\n");

	bte ngram = 2;
	if (NG && (msg = ngram_choice(NG, &ngram, fname)))
		return msg;

	if (!(l = BATdescriptor(*L)) || !(r = BATdescriptor(*R))) {
		BBPreclaim_n(2, l, r);
		throw(MAL, fname, SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	/* not candidate lists for now */
	(void) CL;
	(void) CR;

	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	size_t l_cnt = BATcount(l);
	size_t r_cnt = BATcount(r);

	/* TODO: if r_cnt < 10 then fall back to select */

	rl = COLnew(0, TYPE_oid, l_cnt, TRANSIENT);
	if (RR)
		rr = COLnew(0, TYPE_oid, l_cnt, TRANSIENT);

	if (!rl || (RR && !rr)) {
		bat_iterator_end(&li);
		bat_iterator_end(&ri);
		BBPreclaim_n(4, l, r, rl, rr);
		throw(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	SET_EMPTY_BAT_PROPS(rl);
	if (RR)
		SET_EMPTY_BAT_PROPS(rr);

	switch(ngram) {
	case 1:
		msg = join_unigram(rl, rr, &li, &ri, l_cnt, r_cnt, str_cmp);
		break;
	case 2:
		msg = join_bigram(rl, rr, &li, &ri, l_cnt, r_cnt, str_cmp);
		break;
	case 3:
		msg = join_trigram(rl, rr, &li, &ri, l_cnt, r_cnt, str_cmp);
		break;
	default:
		bat_iterator_end(&li);
		bat_iterator_end(&ri);
		BBPreclaim_n(4, l, r, rl, rr);
		throw(MAL, fname, SQLSTATE(42000) "Only uni, bi or trigrams available.");
	}

	if (!msg) {
		*RL = rl->batCacheid;
		BBPkeepref(rl);
		if (RR) {
			*RR = rr->batCacheid;
			BBPkeepref(rr);
		}
	} else {
		BBPreclaim_n(2, rl, rr);
	}

	bat_iterator_end(&li);
	bat_iterator_end(&ri);
	BBPreclaim_n(2, l, r);
	return msg;
}

static str
NGstartswith(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;

	bit *r = getArgReference_bit(stk, pci, 0);
	const char *s1 = *getArgReference_str(stk, pci, 1);
	const char *s2 = *getArgReference_str(stk, pci, 2);

	if (strNil(s1) || strNil(s2)) {
		*r = bit_nil;
	} else {
		int s2_len = str_strlen(s2);
		*r = str_is_prefix(s1, s2, s2_len) == 0;
	}
	return MAL_SUCCEED;
}

static str
NGendswith(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;

	bit *r = getArgReference_bit(stk, pci, 0);
	const char *s1 = *getArgReference_str(stk, pci, 1);
	const char *s2 = *getArgReference_str(stk, pci, 2);

	if (strNil(s1) || strNil(s2)) {
		*r = bit_nil;
	} else {
		int s2_len = str_strlen(s2);
		*r = str_is_suffix(s1, s2, s2_len) == 0;
	}
	return MAL_SUCCEED;
}

static str
NGcontains(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;

	bit *r = getArgReference_bit(stk, pci, 0);
	const char *s1 = *getArgReference_str(stk, pci, 1);
	const char *s2 = *getArgReference_str(stk, pci, 2);

	if (strNil(s1) || strNil(s2)) {
		*r = bit_nil;
	} else {
		int s2_len = str_strlen(s2);
		*r = str_contains(s1, s2, s2_len) == 0;
	}
	return MAL_SUCCEED;
}

static str
NGselect(bat *R, bat *H, bat *C, str *Needle, bit *anti)
{
	(void)R;
	(void)H;
	(void)C;
	(void)Needle;
	(void)anti;
	return MAL_SUCCEED;
}

static str
NGstartswithjoin(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;
	return NGjoin(stk, pci, str_is_prefix, "ng.startswithjoin");
}

static str
NGendswithjoin(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;
	return NGjoin(stk, pci, str_is_suffix, "ng.endswithjoin");
}

static str
NGcontainsjoin(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;
	return NGjoin(stk, pci, str_contains, "ng.contains_join");
}

#include "mel.h"
static mel_func ngrams_init_functions[] = {

	pattern("ngrams", "c1", NGcontains, false,
			"predicate if value and needle equal needle",
			args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngrams", "c1select", NGselect, false,
			"predicate if value and needle equal needle",
			args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngrams", "c1join", NGc1join1, false,
			"predicate if value and needle equal needle (using 1gram)",
			args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngrams", "c1join", NGc1join, false,
			"predicate if value and needle equal needle (using 1gram)",
			args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	pattern("ngrams", "c2", NGcontains, false,
			"predicate if value and needle equal needle",
			args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngrams", "c2select", NGselect, false,
			"predicate if value and needle equal needle",
			args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngrams", "c2join", NGc2join1, false,
			"predicate if value and needle equal needle (using 2gram)",
			args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngrams", "c2join", NGc2join, false,
			"predicate if value and needle equal needle (using 2gram)",
			args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),

	pattern("ngrams", "c3", NGcontains, false,
			"predicate if value and needle equal needle",
			args(1, 3, arg("res", bit), arg("h", str), arg("needle", str))),
	command("ngrams", "c3select", NGselect, false,
			"predicate if value and needle equal needle",
			args(1, 5, batarg("res", oid), batarg("h", str), batarg("s", oid), arg("needle", str), arg("anti", bit))),
	command("ngrams", "c3join", NGc3join1, false,
			"predicate if value and needle equal needle (using 3gram)",
			args(1, 8, batarg("l", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),
	command("ngrams", "c3join", NGc3join, false,
			"predicate if value and needle equal needle (using 3gram)",
			args(2, 9, batarg("l", oid), batarg("r", oid), batarg("h", str), batarg("needle", str), batarg("lc", oid), batarg("rc", oid), arg("nil_matches",bit), arg("estimate",lng), arg("anti", bit))),



	pattern("ngrams", "startswith", NGstartswith, false, "",
			args(1, 3, arg("rl", bit), arg("l", str), arg("r", str))),
	pattern("ngrams", "startswith", NGstartswith, false, "",
			args(1, 4, arg("rl", bit), arg("l", str), arg("r", str), arg("ngram", bte))),
	pattern("ngrams", "endswith", NGendswith, false, "",
			args(1, 3, arg("rl", bit), arg("l", str), arg("r", str))),
	pattern("ngrams", "endswith", NGendswith, false, "",
			args(1, 4, arg("rl", bit), arg("l", str), arg("r", str), arg("ngram", bte))),
	pattern("ngrams", "contains", NGcontains, false, "",
			args(1, 3, arg("rl", bit), arg("l", str), arg("r", str))),
	pattern("ngrams", "contains", NGcontains, false, "",
			args(1, 4, arg("rl", bit), arg("l", str), arg("r", str), arg("ngram", bte))),

	/* pattern("ngrams", "startswithselect", NGstartswithselect, false, "", */
	/* 		args(1, 5, batarg("rl", oid), batarg("l", str), batarg("cl", oid), arg("r", str), arg("anti", bit))), */
	/* pattern("ngrams", "startswithselect", NGstartswithselect, false, "", */
	/* 		args(1, 6, batarg("rl", oid), batarg("l", str), batarg("cl", oid), arg("r", str), arg("ngram", bte), arg("anti", bit))), */

	/* pattern("ngrams", "endswithselect", NGendswithselect, false, "", */
	/* 		args(1, 5, batarg("rl", oid), batarg("l", str), batarg("cl", oid), arg("r", str), arg("anti", bit))), */
	/* pattern("ngrams", "endswithselect", NGendswithselect, false, "", */
	/* 		args(1, 6, batarg("rl", oid), batarg("l", str), batarg("cl", oid), arg("r", str), arg("ngram", bte), arg("anti", bit))), */

	/* pattern("ngrams", "containsselect", NGcontainsselect, false, "", */
	/* 		args(1, 5, batarg("rl", oid), batarg("l", str), batarg("cl", oid), arg("r", str), arg("anti", bit))), */
	/* pattern("ngrams", "containsselect", NGcontainsselect, false, "", */
	/* 		args(1, 6, batarg("rl", oid), batarg("l", str), batarg("cl", oid), arg("r", str), arg("ngram", bte), arg("anti", bit))), */

	pattern("ngrams", "startswithjoin", NGstartswithjoin, false, "",
			args(1, 8, batarg("rl", oid), batarg("l", str), batarg("r", str), batarg("cl",oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "startswithjoin", NGstartswithjoin, false, "",
			args(1, 9, batarg("rl", oid), batarg("l", str), batarg("r", str), batarg("ngram", bte), batarg("cl",oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "startswithjoin", NGstartswithjoin, false, "",
			args(2, 9, batarg("rl", oid), batarg("rr", oid), batarg("l", str), batarg("r", str), batarg("cl", oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "startswithjoin", NGstartswithjoin, false, "",
			args(2, 10, batarg("rl", oid), batarg("rr", oid), batarg("l", str), batarg("r", str), batarg("ngram", bte), batarg("cl", oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "endswithjoin", NGendswithjoin, false, "",
			args(1, 8, batarg("rl", oid), batarg("l", str), batarg("r", str), batarg("cl",oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "endswithjoin", NGendswithjoin, false, "",
			args(1, 9, batarg("rl", oid), batarg("l", str), batarg("r", str), batarg("ngram", batbte), batarg("cl",oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "endswithjoin", NGendswithjoin, false, "",
			args(2, 9, batarg("rl", oid), batarg("rr", oid), batarg("l", str), batarg("r", str), batarg("cl", oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "endswithjoin", NGendswithjoin, false, "",
			args(2, 10, batarg("rl", oid), batarg("rr", oid), batarg("l", str), batarg("r", str), batarg("ngram", bte), batarg("cl", oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "containsjoin", NGcontainsjoin, false, "",
			args(1, 8, batarg("rl", oid), batarg("l", str), batarg("r", str), batarg("cl",oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "containsjoin", NGcontainsjoin, false, "",
			args(1, 9, batarg("rl", oid), batarg("l", str), batarg("r", str), batarg("ngram", bte), batarg("cl",oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "containsjoin", NGcontainsjoin, false, "",
			args(2, 9, batarg("rl", oid), batarg("rr", oid), batarg("l", str), batarg("r", str), batarg("cl", oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	pattern("ngrams", "containsjoin", NGcontainsjoin, false, "",
			args(2, 10, batarg("rl", oid), batarg("rr", oid), batarg("l", str), batarg("r", str), batarg("ngram", bte), batarg("cl", oid), batarg("cr", oid), arg("nil_matches", bit), arg("estimate", lng), arg("anti", bit))),
	{ .imp=NULL }		/* sentinel */
};

#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_ngrams)
{
	mal_module("ngrams", NULL, ngrams_init_functions);
}
