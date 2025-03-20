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
#include "ngrams.h"
#include "mal_interpreter.h"
#include "mal_exception.h"
#include "string.h"
#include "str.h"

static inline int
is_prefix(const char *s1, const char *s2, int s2_len)
{
	return strncmp(s1, s2, s2_len);
}

static inline int
is_suffix(const char *s1, const char *s2, int s2_len)
{
	return strcmp(s1 + strlen(s1) - s2_len, s2);
}

static inline int
is_contains(const char *s1, const char *s2, int s2_len)
{
	(void) s2_len;
	return strstr(s1, s2) == NULL;
}

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
ngrams_create(size_t b_cnt, size_t ng_sz)
{
	Ngrams *ng = GDKmalloc(sizeof(Ngrams));
	if (ng) {
		ng->idx  = GDKmalloc(ng_sz * sizeof(NGRAM_TYPE));
		ng->sigs = GDKmalloc(b_cnt * sizeof(NGRAM_TYPE));
		ng->histogram = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->lists  = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->rids  = GDKmalloc(2 * NGRAM_MULTIPLE * b_cnt * sizeof(unsigned));
	}
	if (!ng || !ng->idx || !ng->sigs || !ng->histogram || !ng->lists || !ng->rids) {
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
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	unsigned *h_tmp = GDKzalloc(UNIGRAM_SZ * sizeof(unsigned));
	unsigned *map = GDKmalloc(UNIGRAM_SZ * sizeof(unsigned));

	if (!h_tmp || !map) {
		GDKfree(h_tmp);
		GDKfree(map);
		return -1;
	}

	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s))
			for (; UNIGRAM(s); s++)
				h_tmp[ENC_TOKEN1(s)]++;
	}

	for(size_t i = 0; i < UNIGRAM_SZ; i++) {
		map[i] = i;
		idx[i] = lists[i] = 0;
		h[i] = h_tmp[i];
	}

	GDKqsort(h_tmp, map, NULL, UNIGRAM_SZ,
			 sizeof(unsigned), sizeof(unsigned), TYPE_int, true, false);

	unsigned j = UNIGRAM_SZ - 1, sum = 0;
	for (; j; j--) {
		sum += h_tmp[j];
		if (sum + h_tmp[j] >= NGRAM_MULTIPLE * b_cnt - 1)
			break;
	}
	ng->max = h_tmp[0];
	ng->min = h_tmp[j];

	int n = 0;
	for(size_t i = 0; i < UNIGRAM_SZ && h_tmp[i] > 0; i++) {
		unsigned x = map[i];
		idx[x] = NGRAM_CST(1) << n++;
		n %= NGRAM_BITS;
	}

	unsigned k = 1;
	for(size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && UNIGRAM(s)) {
			NGRAM_TYPE sig = 0;
			for(; UNIGRAM(s); s++) {
				unsigned unigram = ENC_TOKEN1(s);
				sig |= idx[unigram];
				if (h[unigram] <= ng->min) {
					if (lists[unigram] == 0) {
						lists[unigram] = k;
						k += h[unigram];
						h[unigram] = 0;
					}
					int done = (h[unigram] > 0 && rids[lists[unigram] + h[unigram] - 1] == i);
					if (!done) {
						rids[lists[unigram] + h[unigram]] = i;
						h[unigram]++;
					}
				}
			}
			*sigs = sig;
		} else if (!strNil(s)) {
			*sigs = 1;
		} else {
			*sigs = NGRAM_TYPENIL;
		}
		sigs++;
	}

	GDKfree(h_tmp);
	GDKfree(map);
	return 0;
}

static int
init_bigram_idx(Ngrams *ng, BATiter *bi, size_t b_cnt)
{
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	unsigned (*h_tmp)[SZ] = GDKzalloc(BIGRAM_SZ * sizeof(unsigned));
	unsigned *h_tmp_ptr = (unsigned *) h_tmp;
	unsigned *map = GDKmalloc(BIGRAM_SZ * sizeof(unsigned));

	if (!h_tmp || !map) {
		GDKfree(h_tmp);
		GDKfree(map);
		return -1;
	}

	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s))
			for (; BIGRAM(s); s++)
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
	for (; j; j--) {
		sum += h_tmp_ptr[j];
		if ((sum + h_tmp_ptr[j]) >= NGRAM_MULTIPLE * b_cnt - 1)
			break;
	}
	ng->max = h_tmp_ptr[0];
	ng->min = h_tmp_ptr[j];

	int n = 0;
	for (size_t i = 0; i < BIGRAM_SZ && h_tmp_ptr[i] > 0; i++) {
		/* unsigned x = (map[i] / SZ) % SZ, y = map[i] % SZ; */
		/* idx[x*SZ + y] = NGRAM_CST(1) << n; */
		/* n++; */
		/* n %= NGRAM_BITS; */
		/* assert(x*SZ + y == map[i]); */
		idx[map[i]] = NGRAM_CST(1) << n++;
		n %= NGRAM_BITS;
	}

	unsigned int k = 1;
	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && BIGRAM(s)) {
			NGRAM_TYPE sig = 0;
			for (; BIGRAM(s); s++) {
				unsigned bigram = ENC_TOKEN1(s)*SZ + ENC_TOKEN2(s);
				sig |= idx[bigram];
				if (h[bigram] <= ng->min) {
					if (lists[bigram] == 0) {
						lists[bigram] = k;
						k += h[bigram];
						h[bigram] = 0;
					}
					int done = (h[bigram] > 0 && rids[lists[bigram] + h[bigram] - 1] == i);
					if (!done) {
						rids[lists[bigram] + h[bigram]] = i;
						h[bigram]++;
					}
				}
			}
			*sigs = sig;
		/* } else if (!strNil(s)) { */
		/* 	*sigs = 1; */
		} else {
			*sigs = NGRAM_TYPENIL;
		}
		sigs++;
	}

	GDKfree(h_tmp);
	GDKfree(map);
	return 0;
}

static int
init_trigram_idx(Ngrams *ng, BATiter *bi, size_t b_cnt)
{
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	unsigned (*h_tmp)[SZ][SZ] = GDKzalloc(TRIGRAM_SZ * sizeof(unsigned));
	unsigned *h_tmp_ptr = (unsigned *) h_tmp;
	unsigned *map = GDKmalloc(TRIGRAM_SZ * sizeof(unsigned));

	if (!h_tmp || !map) {
		GDKfree(h_tmp);
		GDKfree(map);
		return -1;
	}

	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s))
			for (; TRIGRAM(s); s++)
				h_tmp[ENC_TOKEN1(s)][ENC_TOKEN2(s)][ENC_TOKEN3(s)]++;
	}

	for (size_t i = 0; i < TRIGRAM_SZ; i++) {
		map[i] = i;
		idx[i] = lists[i] = 0;
		h[i] = h_tmp_ptr[i];
	}

	GDKqsort(h_tmp, map, NULL, TRIGRAM_SZ,
			 sizeof(unsigned), sizeof(unsigned), TYPE_int, true, false);

	unsigned j = TRIGRAM_SZ - 1, sum = 0;
	for (; j; j--) {
		sum += h_tmp_ptr[j];
		if ((sum + h_tmp_ptr[j]) >= NGRAM_MULTIPLE * b_cnt - 1)
			break;
	}
	ng->max = h_tmp_ptr[0];
	ng->min = h_tmp_ptr[j];

	int n = 0;
	for (size_t i = 0; i < TRIGRAM_SZ && h_tmp_ptr[i] > 0; i++) {
		idx[map[i]] = NGRAM_CST(1) << n++;
		n %= NGRAM_BITS;
	}

	unsigned k = 1;
	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && s[0] && s[1] && s[2]) {
			NGRAM_TYPE sig = 0;
			for(; TRIGRAM(s); s++) {
				unsigned trigram = ENC_TOKEN1(s)*SZ*SZ + ENC_TOKEN2(s)*SZ + ENC_TOKEN3(s);
				sig |= idx[trigram];
				if (h[trigram] <= ng->min) {
					if (lists[trigram] == 0) {
						lists[trigram] = k;
						k += h[trigram];
						h[trigram] = 0;
					}
					int done =  (h[trigram] > 0 && rids[lists[trigram] + h[trigram]-1] == i);
					if (!done) {
						rids[lists[trigram] + h[trigram]] = i;
						h[trigram]++;
					}
				}
			}
			*sigs = sig;
		} else if (!strNil(s)) {
			*sigs = 1;
		} else {
			*sigs = NGRAM_TYPENIL;
		}
		sigs++;
	}

	GDKfree(h_tmp);
	GDKfree(map);
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

	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	size_t new_cap;
	oid *orl = NULL, *orr = NULL, l_hseqbase = li->b->hseqbase;

	orl = Tloc(rl, 0);
	if (rr) orr = Tloc(rr, 0);

	if (init_unigram_idx(ng, li, l_cnt) != 0) {
		ngrams_destroy(ng);
		throw(MAL, "join_unigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for (size_t j = 0; j < r_cnt; j++) {
		const char *rs = BUNtail(*ri, j), *rs_iter = rs;
		NGRAM_TYPE sig = 0;
		if (!strNil(rs) && UNIGRAM(rs)) {
			unsigned min = ng->max, min_pos = 0;
			for (; UNIGRAM(rs_iter); rs_iter++) {
				unsigned unigram = ENC_TOKEN1(rs_iter);
				sig |= idx[unigram];
				if (h[unigram] < min) {
					min = h[unigram];
					min_pos = unigram;
				}
			}
			if (min <= ng->min) {
				unsigned list = lists[min_pos], list_cnt = h[min_pos];
				for (size_t i = 0; i < list_cnt; i++, list++) {
					unsigned ol = rids[list];
					if ((sigs[ol] & sig) == sig) {
						const char *ls = BUNtail(*li, ol);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							*orl++ = ol + l_hseqbase;
							if (rr) *orr++ = j;
						}
						if (*orl == BATcapacity(rl)) {
							new_cap = BATgrows(rl);
							if (BATextend(rl, new_cap) != GDK_SUCCEED ||
								(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
								ngrams_destroy(ng);
								throw(MAL, "join_unigram", GDK_EXCEPTION);
							}
						}
					}
				}
			} else {
				for (size_t i = 0; i < l_cnt; i++) {
					if ((sigs[i] & sig) == sig) {
						const char *ls = BUNtail(*li, i);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							*orl++ = i + l_hseqbase;
							if (rr) *orr++ = j;
						}
						if (*orl == BATcapacity(rl)) {
							new_cap = BATgrows(rl);
							if (BATextend(rl, new_cap) != GDK_SUCCEED ||
								(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
								ngrams_destroy(ng);
								throw(MAL, "join_unigram", GDK_EXCEPTION);
							}
						}
					}
				}
			}
		} else if (!strNil(rs)) {
			for (size_t i = 0; i < l_cnt; i++) {
				const char *ls = BUNtail(*li, i);
				if (!strNil(ls)) {
					*orl++ = i + li->b->hseqbase;
					if (rr) *orr++ = j;
				}
				if (*orl == BATcapacity(rl)) {
					new_cap = BATgrows(rl);
					if (BATextend(rl, new_cap) != GDK_SUCCEED ||
						(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
						ngrams_destroy(ng);
						throw(MAL, "join_unigram", GDK_EXCEPTION);
					}
				}
			}
		}
	}

	BATsetcount(rl, orl - (oid *)Tloc(rl, 0));
	if (rr) BATsetcount(rr, orl - (oid *)Tloc(rl, 0));
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

	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	size_t new_cap;
	oid *orl = NULL, *orr = NULL, l_hseqbase = li->b->hseqbase;

	orl = Tloc(rl, 0);
	if (rr) orr = Tloc(rr, 0);

	if (init_bigram_idx(ng, li, l_cnt) != 0) {
		ngrams_destroy(ng);
		throw(MAL, "join_bigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for(size_t j = 0; j < r_cnt; j++) {
		const char *rs = BUNtail(*ri, j), *rs_iter = rs;
		NGRAM_TYPE sig = 0;
		if (!strNil(rs) && BIGRAM(rs)) {
			unsigned min = ng->max, min_pos = 0;
			for (; BIGRAM(rs_iter); rs_iter++) {
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
						const char *ls = BUNtail(*li, ol);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							*orl++ = ol + l_hseqbase;
							if (rr) *orr++ = j;
						}
						if (*orl == BATcapacity(rl)) {
							new_cap = BATgrows(rl);
							if (BATextend(rl, new_cap) != GDK_SUCCEED ||
								(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
								ngrams_destroy(ng);
								throw(MAL, "join_bigram", GDK_EXCEPTION);
							}
						}
					}
				}
			} else {
				for (size_t i = 0; i < l_cnt; i++) {
					if (((sigs[i] & sig) == sig)) {
						const char *ls = BUNtail(*li, i);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							*orl++ = i + l_hseqbase;
							if (rr) *orr++ = j;
						}
						if (*orl == BATcapacity(rl)) {
							new_cap = BATgrows(rl);
							if (BATextend(rl, new_cap) != GDK_SUCCEED ||
								(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
								ngrams_destroy(ng);
								throw(MAL, "join_bigram", GDK_EXCEPTION);
							}
						}
					}
				}
			}
		} else if (!strNil(rs)) {
			for (size_t i = 0; i < l_cnt; i++) {
				const char *ls = BUNtail(*li, i);
				if (!strNil(ls)) {
					if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
						*orl++ = i + l_hseqbase;
						if (rr) *orr++ = j;
					}
					if (*orl == BATcapacity(rl)) {
						new_cap = BATgrows(rl);
						if (BATextend(rl, new_cap) != GDK_SUCCEED ||
							(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
							ngrams_destroy(ng);
							throw(MAL, "join_trigram", GDK_EXCEPTION);
						}
					}
				}
			}
		}
	}

	BATsetcount(rl, orl - (oid *)Tloc(rl, 0));
	if (rr) BATsetcount(rr, orl - (oid *)Tloc(rl, 0));
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

	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->histogram;
	unsigned *lists = ng->lists;
	unsigned *rids = ng->rids;
	size_t new_cap;
	oid *orl = NULL, *orr = NULL, l_hseqbase = li->b->hseqbase;

	orl = Tloc(rl, 0);
	if (rr) orr = Tloc(rr, 0);

	if (init_trigram_idx(ng, li, l_cnt) != 0) {
		ngrams_destroy(ng);
		throw(MAL, "join_trigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	for(size_t j = 0; j < r_cnt; j++) {
		const char *rs = BUNtail(*ri, j), *rs_iter = rs;
		NGRAM_TYPE sig = 0;
		if (!strNil(rs_iter) && TRIGRAM(rs)) {
			unsigned min = ng->max, min_pos = 0;
			for(; TRIGRAM(rs_iter); rs_iter++) {
				unsigned trigram = ENC_TOKEN1(rs_iter)*SZ*SZ + ENC_TOKEN2(rs_iter)*SZ + ENC_TOKEN3(rs_iter);
				sig |= idx[trigram];
				if (h[trigram] < min) {
					min = h[trigram];
					min_pos = trigram;
				}
			}
			if (min <= ng->min) {
				unsigned list = lists[min_pos], list_cnt = h[min_pos];
				for (size_t i = 0; i < list_cnt; i++, list++) {
					unsigned ol = rids[list];
					if ((sigs[ol] & sig) == sig) {
						char *ls = BUNtail(*li, ol);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							*orl++ = ol + l_hseqbase;
							if (rr) *orr++ = j;
						}
					}
					if (*orl == BATcapacity(rl)) {
						new_cap = BATgrows(rl);
						if (BATextend(rl, new_cap) != GDK_SUCCEED ||
							(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
							ngrams_destroy(ng);
							throw(MAL, "join_trigram", GDK_EXCEPTION);
						}
					}
				}
			} else {
				for (size_t i = 0; i < l_cnt; i++) {
					if ((sigs[i] & sig) == sig) {
						char *ls = BUNtail(*li, i);
						if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
							*orl++ = i + l_hseqbase;
							if (rr) *orr++ = j;
						}
						if (*orl == BATcapacity(rl)) {
							new_cap = BATgrows(rl);
							if (BATextend(rl, new_cap) != GDK_SUCCEED ||
								(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
								ngrams_destroy(ng);
								throw(MAL, "join_trigram", GDK_EXCEPTION);
							}
						}
					}
				}
			}
		} else if (!strNil(rs)) {
			for (size_t i = 0; i < l_cnt; i++) {
				const char *ls = BUNtail(*li, i);
				if (!strNil(ls)) {
					if (str_cmp(ls, rs, str_strlen(rs)) == 0) {
						*orl++ = i + l_hseqbase;
						if (rr) *orr++ = j;
					}
					if (*orl == BATcapacity(rl)) {
						new_cap = BATgrows(rl);
						if (BATextend(rl, new_cap) != GDK_SUCCEED ||
							(rr && BATextend(rr, new_cap) != GDK_SUCCEED)) {
							ngrams_destroy(ng);
							throw(MAL, "join_trigram", GDK_EXCEPTION);
						}
					}
				}
			}
		}
	}

	BATsetcount(rl, orl - (oid *)Tloc(rl, 0));
	if (rr) BATsetcount(rr, orl - (oid *)Tloc(rl, 0));
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

	if ((CL && !is_bat_nil(*CL)) || (CR && !is_bat_nil(*CR)))
		assert(0);

	BATiter li = bat_iterator(l);
	BATiter ri = bat_iterator(r);

	size_t l_cnt = BATcount(l);
	size_t r_cnt = BATcount(r);

	rl = COLnew(0, TYPE_oid, l_cnt, TRANSIENT);
	if (RR)
		rr = COLnew(0, TYPE_oid, l_cnt, TRANSIENT);

	if (!rl || (RR && !rr)) {
		bat_iterator_end(&li);
		bat_iterator_end(&ri);
		BBPreclaim_n(4, l, r, rl, rr);
		throw(MAL, fname, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

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

	bat_iterator_end(&li);
	bat_iterator_end(&ri);

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
		*r = is_prefix(s1, s2, s2_len) == 0;
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
		*r = is_suffix(s1, s2, s2_len) == 0;
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
		*r = is_contains(s1, s2, s2_len) == 0;
	}
	return MAL_SUCCEED;
}

/* static str */
/* NGselect(bat *R, bat *H, bat *C, str *Needle, bit *anti) */
/* { */
/* 	(void)R; */
/* 	(void)H; */
/* 	(void)C; */
/* 	(void)Needle; */
/* 	(void)anti; */
/* 	return MAL_SUCCEED; */
/* } */

static str
NGstartswithjoin(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;
	return NGjoin(stk, pci, is_prefix, "ng.startswithjoin");
}

static str
NGendswithjoin(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;
	return NGjoin(stk, pci, is_suffix, "ng.endswithjoin");
}

static str
NGcontainsjoin(Client c, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) c;
	(void) mb;
	return NGjoin(stk, pci, is_contains, "ng.containsjoin");
}

#include "mel.h"
static mel_func ngrams_init_functions[] = {

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
