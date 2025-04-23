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
#include "gdk.h"
#include "mal_exception.h"
#include "str.h"

#ifdef HAVE_HGE
#define NGRAM_TYPE hge
#define NGRAM_TYPENIL hge_nil
#define NGRAM_CST(v) ((hge)LL_CONSTANT(v))
#define NGRAM_BITS 127
#define CHARMAP(s) (s & NGRAM_BITS)
#define SZ 128
#else
#define NGRAM_TYPE lng
#define NGRAM_TYPEID TYPE_lng
#define NGRAM_TYPENIL lng_nil
#define NGRAM_CST(v) LL_CONSTANT(v)
#define NGRAM_BITS 63
#define CHARMAP(s) (s & NGRAM_BITS)
#define SZ 64
#endif

#define BIGRAM_SZ (SZ * SZ)
#define NGRAM_MULTIPLE 16
#define TOKEN1(s) (*s)
#define TOKEN2(s) (*(s + 1))
#define BIGRAM(s) (TOKEN1(s) && TOKEN2(s))

#define ENC_TOKEN1(t) CHARMAP(*t)
#define ENC_TOKEN2(t) CHARMAP(*(t + 1))

#undef VALUE
#undef APPEND
#define VALUE(s, x)  (s##vars + VarHeapVal(s##vals, (x), s##i->width))
#define APPEND(b, o) (((oid *) b->theap->base)[b->batCount++] = (o))

typedef struct {
	NGRAM_TYPE *idx;
	NGRAM_TYPE *sigs;
	unsigned *histogram;
	unsigned min, max;
	unsigned *lists;
	unsigned *rids;
} Ngrams;

static void
ngrams_destroy(Ngrams *ng)
{
	if (ng) {
		GDKfree(ng->idx);
		GDKfree(ng->sigs);
		GDKfree(ng->histogram);
		GDKfree(ng->lists);
		GDKfree(ng->rids);
	}
	GDKfree(ng);
}

static Ngrams *
ngrams_create(size_t cnt, size_t ng_sz)
{
	Ngrams *ng = GDKmalloc(sizeof(Ngrams));
	if (ng) {
		ng->idx  = GDKmalloc(ng_sz * sizeof(NGRAM_TYPE));
		ng->sigs = GDKmalloc(cnt * sizeof(NGRAM_TYPE));
		ng->histogram = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->lists  = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->rids  = GDKmalloc(2 * NGRAM_MULTIPLE * cnt * sizeof(unsigned));
	}
	if (!ng || !ng->idx || !ng->sigs || !ng->histogram || !ng->lists || !ng->rids) {
		ngrams_destroy(ng);
		return NULL;
	}
	return ng;
}

static str
init_bigram_idx(Ngrams *ng, BATiter *bi, struct canditer *bci, QryCtx *qry_ctx)
{
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	unsigned (*h_tmp)[SZ] = GDKzalloc(BIGRAM_SZ * sizeof(unsigned));
	unsigned *h_tmp_ptr = (unsigned *) h_tmp;
	unsigned *map = GDKmalloc(BIGRAM_SZ * sizeof(unsigned));
	unsigned int k = 1;

	if (!h_tmp || !map) {
		GDKfree(h_tmp);
		GDKfree(map);
		throw(MAL, "init_bigram_idx", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	oid bbase = bi->b->hseqbase, ob;
	const char *bvars = bi->vh->base, *bvals = bi->base;

	canditer_reset(bci);
	TIMEOUT_LOOP(bci->ncand, qry_ctx) {
		ob = canditer_next(bci);
		const char *s = VALUE(b, ob - bbase);
		if (!strNil(s))
			for ( ; BIGRAM(s); s++)
				h_tmp[ENC_TOKEN1(s)][ENC_TOKEN2(s)]++;
	}

	for (size_t i = 0; i < BIGRAM_SZ; i++) {
		map[i] = i;
		idx[i] = lists[i] = 0;
		h[i] = h_tmp_ptr[i];
	}

	GDKqsort(h_tmp, map, NULL, BIGRAM_SZ,
			 sizeof(unsigned), sizeof(unsigned), TYPE_int, true, false);

	unsigned j = BIGRAM_SZ - 1, sum = 0;
	for ( ; j; j--) {
		sum += h_tmp_ptr[j];
		if ((sum + h_tmp_ptr[j]) >= NGRAM_MULTIPLE * bci->ncand - 1)
			break;
	}
	ng->max = h_tmp_ptr[0];
	ng->min = h_tmp_ptr[j];

	int n = 0;
	for (size_t i = 0; i < BIGRAM_SZ && h_tmp_ptr[i] > 0; i++) {
		idx[map[i]] = NGRAM_CST(1) << n++;
		n %= NGRAM_BITS;
	}

	canditer_reset(bci);
	TIMEOUT_LOOP(bci->ncand, qry_ctx) {
		ob = canditer_next(bci);
		const char *s = VALUE(b, ob - bbase);
		if (!strNil(s) && BIGRAM(s)) {
			NGRAM_TYPE sig = 0;
			for ( ; BIGRAM(s); s++) {
				unsigned bigram = ENC_TOKEN1(s)*SZ + ENC_TOKEN2(s);
				sig |= idx[bigram];
				if (h[bigram] <= ng->min) {
					if (lists[bigram] == 0) {
						lists[bigram] = k;
						k += h[bigram];
						h[bigram] = 0;
					}
					int done = (h[bigram] > 0 &&
								rids[lists[bigram] + h[bigram] - 1] == ob - bbase);
					if (!done) {
						rids[lists[bigram] + h[bigram]] = ob - bbase;
						h[bigram]++;
					}
				}
			}
			*sigs = sig;
		} else {
			*sigs = NGRAM_TYPENIL;
		}
		sigs++;
	}

	GDKfree(h_tmp);
	GDKfree(map);
	return MAL_SUCCEED;
}

/* static str */
/* bigram_strselect(BAT *rl, BATiter *li, struct canditer *lci, const char *r, */
/* 				 int (*str_cmp)(const char *, const char *, int), QryCtx *qry_ctx) */
/* { */
/* 	str msg = MAL_SUCCEED; */
/* 	Ngrams *ng = ngrams_create(lci->ncand, BIGRAM_SZ); */
/* 	if (!ng) */
/* 		throw(MAL, "select_bigram", SQLSTATE(HY013) MAL_MALLOC_FAIL); */

/* 	NGRAM_TYPE *idx = ng->idx; */
/* 	NGRAM_TYPE *sigs = ng->sigs; */
/* 	unsigned *h = ng->histogram; */
/* 	unsigned *lists = ng->lists; */
/* 	unsigned *rids = ng->rids; */
/* 	oid lbase = li->b->hseqbase, ol; */
/* 	const char *lvars = li->vh->base, *lvals = li->base; */
/* 	const char *rs = r, *rs_iter = r; */

/* 	if (strNil(rs)) */
/* 		return msg; */

/* 	if (strlen(rs) < 2) { */
/* 		canditer_reset(lci); */
/* 		TIMEOUT_LOOP(lci->ncand, qry_ctx) { */
/* 			ol = canditer_next(lci); */
/* 			const char *ls = VALUE(l, ol - lbase); */
/* 			if (!strNil(ls) && str_cmp(ls, rs, str_strlen(rs)) == 0) */
/* 				APPEND(rl, ol); */
/* 		} */
/* 	} */

/* 	msg = init_bigram_idx(ng, li, lci, qry_ctx); */
/* 	if (msg) { */
/* 		ngrams_destroy(ng); */
/* 		return msg; */
/* 	} */

/* 	NGRAM_TYPE sig = 0; */
/* 	unsigned min = ng->max, min_pos = 0; */
/* 	for ( ; BIGRAM(rs_iter); rs_iter++) { */
/* 		unsigned bigram = ENC_TOKEN1(rs_iter)*SZ + ENC_TOKEN2(rs_iter); */
/* 		sig |= idx[bigram]; */
/* 		if (h[bigram] < min) { */
/* 			min = h[bigram]; */
/* 			min_pos = bigram; */
/* 		} */
/* 	} */

/* 	if (min <= ng->min) { */
/* 		unsigned list = lists[min_pos], list_cnt = h[min_pos]; */
/* 		for (size_t i = 0; i < list_cnt; i++, list++) { */
/* 			unsigned ol = rids[list]; */
/* 			if ((sigs[ol] & sig) == sig) { */
/* 				const char *ls = VALUE(l, ol); */
/* 				if (str_cmp(ls, rs, str_strlen(rs)) == 0) */
/* 					APPEND(rl, ol + lbase); */
/* 			} */
/* 		} */
/* 	} else { */
/* 		canditer_reset(lci); */
/* 		TIMEOUT_LOOP(lci->ncand, qry_ctx) { */
/* 			ol = canditer_next(lci); */
/* 			if ((sigs[ol - lbase] & sig) == sig) { */
/* 				const char *ls = VALUE(l, ol - lbase); */
/* 				if (str_cmp(ls, rs, str_strlen(rs)) == 0) */
/* 					APPEND(rl, ol); */
/* 			} */
/* 		} */
/* 	} */

/* 	BATsetcount(rl, BATcount(rl)); */
/* 	ngrams_destroy(ng); */
/* 	return msg; */
/* } */

static str
bigram_strjoin(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			   struct canditer *lci, struct canditer *rci,
			   int (*str_cmp)(const char *, const char *, int),
			   const char *fname, QryCtx *qry_ctx)
{
	str msg = MAL_SUCCEED;

	Ngrams *ng = ngrams_create(lci->ncand, BIGRAM_SZ);
	if (!ng)
		throw(MAL, fname, MAL_MALLOC_FAIL);

	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	size_t new_cap;

	msg = init_bigram_idx(ng, li, lci, qry_ctx);
	if (msg) {
		ngrams_destroy(ng);
		return msg;
	}

	oid lbase = li->b->hseqbase, rbase = ri->b->hseqbase, or, ol;
	const char *lvars = li->vh->base, *rvars = ri->vh->base,
		*lvals = li->base, *rvals = ri->base;

	lng t0 = 0;
	TRC_DEBUG_IF(ALGO) t0 = GDKusec();

	canditer_reset(lci);
	TIMEOUT_LOOP(rci->ncand, qry_ctx) {
		or = canditer_next(rci);
		const char *rs = VALUE(r, or - rbase), *rs_iter = rs;
		if (strNil(rs))
			continue;
		if (strlen(rs) < 2) {
			canditer_reset(lci);
			TIMEOUT_LOOP(lci->ncand, qry_ctx) {
				ol = canditer_next(lci);
				const char *ls = VALUE(l, ol - lbase);
				if (!strNil(ls)) {
					if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
						APPEND(rl, ol);
						if (rr) APPEND(rr, or);
						if (BATcount(rl) == BATcapacity(rl)) {
							new_cap = BATgrows(rl);
							if (BATextend(rl, new_cap) != GDK_SUCCEED ||
								(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
								ngrams_destroy(ng);
								throw(MAL, fname, GDK_EXCEPTION);
							}
						}
					}
				}
			}
		} else  if (BIGRAM(rs)) {
			NGRAM_TYPE sig = 0;
			unsigned min = ng->max, min_pos = 0;
			for ( ; BIGRAM(rs_iter); rs_iter++) {
				unsigned bigram = ENC_TOKEN1(rs_iter)*SZ + ENC_TOKEN2(rs_iter);
				sig |= idx[bigram];
				if (h[bigram] < min) {
					min = h[bigram];
					min_pos = bigram;
				}
			}
			if (min <= ng->min) {
				unsigned list = lists[min_pos], list_cnt = h[min_pos];
				for (size_t i = 0; i < list_cnt; i++, list++) {
					unsigned ol = rids[list];
					if ((sigs[ol] & sig) == sig) {
						const char *ls = VALUE(l, ol);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							APPEND(rl, ol + lbase);
							if (rr) APPEND(rr, or);
							if (BATcount(rl) == BATcapacity(rl)) {
								new_cap = BATgrows(rl);
								if (BATextend(rl, new_cap) != GDK_SUCCEED ||
									(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
									ngrams_destroy(ng);
									throw(MAL, fname, GDK_EXCEPTION);
								}
							}
						}
					}
				}
			} else {
				canditer_reset(lci);
				TIMEOUT_LOOP(lci->ncand, qry_ctx) {
					ol = canditer_next(lci);
					if ((sigs[ol - lbase] & sig) == sig) {
						const char *ls = VALUE(l, ol - lbase);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							APPEND(rl, ol);
							if (rr) APPEND(rr, or);
							if (BATcount(rl) == BATcapacity(rl)) {
								new_cap = BATgrows(rl);
								if (BATextend(rl, new_cap) != GDK_SUCCEED ||
									(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
									ngrams_destroy(ng);
									throw(MAL, fname, GDK_EXCEPTION);
								}
							}
						}
					}
				}
			}
		}
	}

	BATsetcount(rl, BATcount(rl));
	if (rr) BATsetcount(rr, BATcount(rr));

	if (BATcount(rl) > 0) {
		BATnegateprops(rl);
		if (rr) BATnegateprops(rr);
	}

	ngrams_destroy(ng);

	TRC_DEBUG(ALGO, "(%s, %s, l=%s #%zu [%s], r=%s #%zu [%s], cl=%s #%zu, cr=%s #%zu, time=%ld)\n",
			  fname, "bigram_strjoin",
			  BATgetId(li->b), li->count, ATOMname(li->b->ttype),
			  BATgetId(ri->b), ri->count, ATOMname(ri->b->ttype),
			  lci ? BATgetId(lci->s) : "NULL", lci ? lci->ncand : 0,
			  rci ? BATgetId(rci->s) : "NULL", rci ? rci->ncand : 0,
			  GDKusec() - t0);

	return msg;
}
