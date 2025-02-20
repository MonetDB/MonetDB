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
		GDKfree(ng->h);
		GDKfree(ng->pos);
		GDKfree(ng->rid);
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
		ng->h    = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->pos  = GDKmalloc(ng_sz * sizeof(unsigned));
		ng->rid  = GDKmalloc(NGRAM_MULTIPLE * b_cnt * sizeof(unsigned));
	}
	if (!ng || !ng->idx || !ng->sigs || !ng->h || !ng->pos || !ng->rid) {
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
	unsigned *h = ng->h;
	unsigned *pos = ng->pos;
	unsigned *rid = ng->rid;
	unsigned *h_tmp = GDKzalloc(UNIGRAM_SZ * sizeof(unsigned));
	unsigned *map = GDKmalloc(UNIGRAM_SZ * sizeof(unsigned));

	if (!h_tmp || !map) {
		GDKfree(h_tmp);
		GDKfree(map);
		return -1;
	}

	for(size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && *s)
			for(const char *c = s; *c; c++)
				h_tmp[CHAR_MAP(*c)]++;
	}

	for(size_t i = 0; i < UNIGRAM_SZ; i++) {
		map[i] = i;
		idx[i] = pos[i] = 0;
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

	int n_shift = 0;
	for(size_t i = 0; i < UNIGRAM_SZ && h[i] > 0; i++) {
		unsigned x = map[i];
		idx[x] = NGRAM_CST(1) << n_shift;
		n_shift++;
		n_shift %= NGRAM_BITS;
	}

	unsigned p = 1;
	for(size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && s[0]) {
			NGRAM_TYPE sig = 0;
			for(; *s; s++) {
				unsigned c = CHAR_MAP(*s);
				sig |= idx[c];
				if (h[c] <= ng->min) {
					if (pos[c] == 0) {
						pos[c] = p;
						p += h[c];
						h[c] = 0;
					}
					int done =  (h[c] > 0 && rid[pos[c] + h[c] - 1] == i);
					if (!done) {
						rid[pos[c] + h[c]] = i;
						h[c]++;
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
	return 0;
}

static int
init_bigram_idx(Ngrams *ng, BATiter *bi, size_t b_cnt)
{
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->h;
	unsigned *pos = ng->pos;
	unsigned *rid = ng->rid;
	unsigned (*h_tmp)[GZ] = GDKzalloc(BIGRAM_SZ * sizeof(unsigned));
	unsigned *h_tmp_ptr = (unsigned *) h_tmp;
	unsigned *map = GDKmalloc(BIGRAM_SZ * sizeof(unsigned));

	if (!h_tmp || !map) {
		GDKfree(h_tmp);
		GDKfree(map);
		return -1;
	}

	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && *s) {
			unsigned char p = CHAR_MAP(*s++);
			for (; *s; p = CHAR_MAP(*s), s++)
				h_tmp[p][CHAR_MAP(*s)]++;
		}
	}

	for (size_t i = 0; i < BIGRAM_SZ; i++) {
		map[i] = i;
		idx[i] = pos[i] = 0;
		ng->h[i] = h_tmp_ptr[i];
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

	int n_shift = 0;
	for (size_t i = 0; i < BIGRAM_SZ && h_tmp_ptr[i] > 0; i++) {
		unsigned x = (map[i] / GZ) % GZ;
		unsigned y = map[i] % GZ;
		idx[x * GZ + y] = NGRAM_CST(1) << n_shift;
		n_shift++;
		n_shift %= NGRAM_BITS;
	}

	unsigned int p = 1;
	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && s[0] && s[1]) {
			NGRAM_TYPE sig = 0;
			unsigned c = CHAR_MAP(*s++);
			for (; *s; c = CHAR_MAP(*s), s++) {
				int k = c * GZ + CHAR_MAP(*s);
				sig |= idx[k];
				if (h[k] <= ng->min) {
					if (pos[k] == 0) {
						pos[k] = p;
						p += h[k];
						h[k] = 0;
					}
					int done =  (h[k] > 0 && rid[pos[k] + h[k] -1] == i);
					if (!done) {
						rid[pos[k] + h[k]] = i;
						h[k]++;
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
	return 0;
}

static int
init_trigram_idx(Ngrams *ng, BATiter *bi, size_t b_cnt)
{
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->h;
	unsigned *pos = ng->pos;
	unsigned *rid = ng->rid;
	unsigned (*h_tmp)[GZ][GZ] = GDKzalloc(TRIGRAM_SZ * sizeof(unsigned));
	unsigned *h_tmp_ptr = (unsigned *) h_tmp;
	unsigned *map = GDKmalloc(TRIGRAM_SZ * sizeof(unsigned));

	if (!h_tmp || !map) {
		GDKfree(h_tmp);
		GDKfree(map);
		return -1;
	}

	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && *s) {
			unsigned char pp = CHAR_MAP(*s++);
			if (!*s)
				continue;
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp = p, p = CHAR_MAP(*s), s++)
				h_tmp[pp][p][CHAR_MAP(*s)]++;
		}
	}

	for (size_t j = 0; j < TRIGRAM_SZ; j++) {
		map[j] = j;
		idx[j] = pos[j] = 0;
		ng->h[j] = h_tmp_ptr[j];
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

	int n_shift = 0;
	for (size_t j = 0; j < TRIGRAM_SZ && h_tmp_ptr[j] > 0; j++) {
		unsigned x = map[j]/(GZ*GZ);
		unsigned y = (map[j]/GZ)%GZ;
		unsigned z = map[j]%GZ;
		idx[x*GZ*GZ+y*GZ+z] = NGRAM_CST(1) << n_shift;
		n_shift++;
		n_shift %= NGRAM_BITS;
	}


	unsigned p = 1;
	for (size_t i = 0; i < b_cnt; i++) {
		const char *s = BUNtail(*bi, i);
		if (!strNil(s) && s[0] && s[1] && s[2]) {
			NGRAM_TYPE sig = 0;
			unsigned cc = CHAR_MAP(*s++);
			unsigned c = CHAR_MAP(*s++);
			for(; *s; cc = c, c = CHAR_MAP(*s), s++) {
				int k = cc * GZ * GZ +c * GZ + CHAR_MAP(*s);
				sig |= idx[k];
				if (h[k] <= ng->min) {
					if (pos[k] == 0) {
						pos[k] = p;
						p += h[k];
						h[k] = 0;
					}
					int done =  (h[k] > 0 && rid[pos[k] + h[k]-1] == i);
					if (!done) {
						rid[pos[k] + h[k]] = i;
						h[k]++;
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
	return 0;
}

static str
join_unigram(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			 size_t l_cnt, size_t r_cnt,
			 int (*str_cmp)(const char *, const char *, int))
{
	(void) str_cmp;
	Ngrams *ng = ngrams_create(l_cnt, UNIGRAM_SZ);
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->h;
	unsigned *pos = ng->pos;
	unsigned *rid = ng->rid;

	if (!ng)
		throw(MAL, "join_unigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (init_unigram_idx(ng, li, l_cnt) != 0)
		throw(MAL, "join_unigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	unsigned nmax = 0;
	oid *o_rl = Tloc(rl, 0);
	oid *o_rr = Tloc(rr, 0);

	for (size_t j = 0; j < r_cnt; j++) {
		const char *rs = BUNtail(*ri, j), *rs_c = rs;
		NGRAM_TYPE sig = 0;
		if (!strNil(rs) && rs[0]) {
			unsigned min = ng->max;
			unsigned min_pos = 0;
			for (; *rs_c; rs_c++) {
				unsigned d = CHAR_MAP(*rs_c);
				sig |= idx[d];
				if (h[d] < min) {
					min = h[d];
					min_pos = d;
				}
			}
			if (min <= ng->min) {
				unsigned rrr = pos[min_pos];
				unsigned lcnt = h[min_pos];
				for(size_t i = 0; i < lcnt; i++, rrr++) {
					unsigned hr = rid[rrr];
					if (((sigs[hr] & sig) == sig)) {
						char *ls = BUNtail(*li, hr);
						/* if (str_cmp(ls, rs, str_strlen(rs)) == 0) { */
						if (strstr(ls, rs) != NULL) {
							*o_rl++ = hr;
							*o_rr++ = j;
						}
					}
				}
			} else {
				for (size_t i = 0; i < l_cnt; i++) {
					if (((sigs[i] & sig) == sig)) {
						char *ls = BUNtail(*li, i);
						/* if (str_cmp(ls, rs, str_strlen(rs)) == 0) { */
						if (strstr(ls, rs) != NULL) {
							*o_rl++ = i;
							*o_rr++ = j;
						}
					}
				}
			}
			if (min > nmax)
				nmax = min;
		} else if (!strNil(rs)) {
			for (size_t i = 0; i < l_cnt; i++) {
				const char *ls = BUNtail(*li, i);
				/* if (str_cmp(ls, rs, str_strlen(rs)) == 0) { */
				if (strstr(ls, rs) != NULL) {
					*o_rl++ = i;
					*o_rr++ = j;
				}
			}
		}
	}

	BATsetcount(rl, o_rl - (oid *)Tloc(rl, 0));
	BATsetcount(rr, o_rl - (oid *)Tloc(rl, 0));
	ngrams_destroy(ng);
	return MAL_SUCCEED;
}

static str
join_bigram(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			 size_t l_cnt, size_t r_cnt,
			 int (*str_cmp)(const char *, const char *, int))
{
	Ngrams *ng = ngrams_create(l_cnt, BIGRAM_SZ);
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->h;
	unsigned *pos = ng->pos;
	unsigned *rid = ng->rid;

	if (!ng)
		throw(MAL, "join_bigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (init_bigram_idx(ng, li, l_cnt) != 0)
		throw(MAL, "join_bigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	unsigned nmax = 0;
	oid *o_rl = Tloc(rl, 0);
	oid *o_rr = Tloc(rr, 0);

	for(size_t j = 0; j < r_cnt; j++) {
		const char *s = BUNtail(*ri, j), *os = s;
		NGRAM_TYPE sig = 0;
		if (!strNil(s) && s[0] && s[1]) { /* skipped */
			unsigned min = ng->max;
			unsigned min_pos = 0;
			unsigned char p = CHAR_MAP(*s++);
			for (; *s; p = CHAR_MAP(*s), s++) {
				unsigned k = p * GZ + CHAR_MAP(*s);
				sig |= idx[k];
				if (h[k] < min) {
					min = h[k];
					min_pos = k; /* encoded min ngram */
				}
			}
			if (min <= ng->min) {
				unsigned rrr = pos[min_pos];
				int lcnt = h[min_pos];
				for (int i = 0; i < lcnt; i++, rrr++) {
					unsigned hr = rid[rrr];
					if (((sigs[hr] & sig) == sig)) {
						char *hs = BUNtail(*li, hr);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*o_rl++ = hr;
							*o_rr++ = j;
						}
					}
				}
			} else {
				for (size_t i = 0; i < l_cnt; i++) {
					if (((sigs[i] & sig) == sig)) {
						char *hs = BUNtail(*li, i);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*o_rl++ = i;
							*o_rr++ = j;
						}
					}
				}
			}
			if (min > nmax)
				nmax = min;
		} else if (!strNil(s)) {
			for (size_t i = 0; i < l_cnt; i++) {
				char *hs = BUNtail(*li, i);
				if (str_cmp(hs, os, str_strlen(os)) == 0) {
					*o_rl++ = i;
					*o_rr++ = j;
				}
			}
		}
	}

	BATsetcount(rl, o_rl - (oid *)Tloc(rl, 0));
	BATsetcount(rr, o_rl - (oid *)Tloc(rl, 0));
	ngrams_destroy(ng);
	return MAL_SUCCEED;
}

static str
join_trigram(BAT *rl, BAT *rr, BATiter *li, BATiter *ri,
			 size_t l_cnt, size_t r_cnt,
			 int (*str_cmp)(const char *, const char *, int))
{
	Ngrams *ng = ngrams_create(l_cnt, TRIGRAM_SZ);
	NGRAM_TYPE *idx = ng->idx;
	NGRAM_TYPE *sigs = ng->sigs;
	unsigned *h = ng->h;
	unsigned *pos = ng->pos;
	unsigned *rid = ng->rid;

	if (!ng)
		throw(MAL, "join_trigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (init_trigram_idx(ng, li, l_cnt) != 0)
		throw(MAL, "join_trigram", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	unsigned nmax = 0;
	oid *o_rl = Tloc(rl, 0);
	oid *o_rr = Tloc(rr, 0);

	for(size_t i = 0; i < r_cnt; i++) {
		const char *s = BUNtail(*ri, i), *os = s;
		NGRAM_TYPE sig = 0;

		if (!strNil(s) && s[0] && s[1] && s[2]) {
			unsigned min = ng->max;
			unsigned min_pos = 0;
			unsigned char pp = CHAR_MAP(*s++);
			unsigned char p = CHAR_MAP(*s++);
			for(; *s; pp = p, p = CHAR_MAP(*s), s++) {
				unsigned k = pp *GZ * GZ + p * GZ + CHAR_MAP(*s);
				sig |= idx[k];
				if (h[k] < min) {
					min = h[k];
					min_pos = k; /* encoded min ngram */
				}
			}
			if (min <= ng->min) {
				unsigned rrr = pos[min_pos];
				unsigned hcnt = h[min_pos]; /* no unsigned */
				for (size_t k = 0; k < hcnt; k++, rrr++) {
					unsigned int hr = rid[rrr];
					if (((sigs[hr] & sig) == sig)) {
						char *hs = BUNtail(*li, hr);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*o_rl++ = hr;
							*o_rr++ = (oid)i;
						}
					}
				}
			} else {
				for (size_t k = 0; k < l_cnt; k++) {
					if (((sigs[k] & sig) == sig)) {
						char *hs = BUNtail(*li, k);
						if (str_cmp(hs, os, str_strlen(os)) == 0) {
							*o_rl++ = k;
							*o_rr++ = (oid)i;
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
					*o_rl++ = k;
					*o_rr++ = (oid)i;
				}
			}
		}
	}

	BATsetcount(rl, o_rl - (oid *)Tloc(rl, 0));
	BATsetcount(rr, o_rl - (oid *)Tloc(rl, 0));
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
