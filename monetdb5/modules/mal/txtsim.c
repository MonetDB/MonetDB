/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "str.h"
#include "mal.h"
#include "mal_exception.h"
#include "mal_interpreter.h"
#include <string.h>
#include <limits.h>

#define MIN3(X,Y,Z)   (MIN(MIN((X),(Y)),(Z)))
#define MAX3(X,Y,Z)   (MAX(MAX((X),(Y)),(Z)))
#define MIN4(W,X,Y,Z) (MIN(MIN(MIN(W,X),Y),Z))

static void
reclaim_bats(int nargs, ...)
{
	va_list valist;

	va_start(valist, nargs);
	for (int i = 0; i < nargs; i++) {
		BAT *b = va_arg(valist, BAT *);
		BBPreclaim(b);
	}
	va_end(valist);
}

static inline int *
damerau_get_cellpointer(int *pOrigin, int col, int row, int nCols)
{
	return pOrigin + col + (row * (nCols + 1));
}

static inline int
damerau_getat(int *pOrigin, int col, int row, int nCols)
{
	int *pCell;
	pCell = damerau_get_cellpointer(pOrigin, col, row, nCols);
	return *pCell;
}

static inline void
damerau_putat(int *pOrigin, int col, int row, int nCols, int x)
{
	int *pCell;
	pCell = damerau_get_cellpointer(pOrigin, col, row, nCols);
	*pCell = x;
}

static str
dameraulevenshtein(int *res, str *S, str *T, int insdel_cost, int replace_cost,
				   int transpose_cost)
{
	char *s = *S;
	char *t = *T;
	int *d;						/* pointer to matrix */
	int n;						/* length of s */
	int m;						/* length of t */
	int i;						/* iterates through s */
	int j;						/* iterates through t */
	char s_i;					/* ith character of s */
	char t_j;					/* jth character of t */
	int cost;					/* cost */
	int cell;					/* contents of target cell */
	int above;					/* contents of cell immediately above */
	int left;					/* contents of cell immediately to left */
	int diag;					/* contents of cell immediately above and to left */
	int sz;						/* number of cells in matrix */
	int diag2 = 0, cost2 = 0;

	if (strNil(*S) || strNil(*T)) {
		*res = int_nil;
		return MAL_SUCCEED;
	}

	/* Step 1 */
	n = (int) strlen(s);		/* 64bit: assume strings are less than 2 GB */
	m = (int) strlen(t);
	if (n == 0) {
		*res = m;
		return MAL_SUCCEED;
	}
	if (m == 0) {
		*res = n;
		return MAL_SUCCEED;
	}
	sz = (n + 1) * (m + 1) * sizeof(int);
	d = (int *) GDKmalloc(sz);
	if (d == NULL)
		throw(MAL, "levenshtein", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* Step 2 */
	for (i = 0; i <= n; i++) {
		damerau_putat(d, i, 0, n, i);
	}
	for (j = 0; j <= m; j++) {
		damerau_putat(d, 0, j, n, j);
	}
	/* Step 3 */
	for (i = 1; i <= n; i++) {
		s_i = s[i - 1];
		/* Step 4 */
		for (j = 1; j <= m; j++) {
			t_j = t[j - 1];
			/* Step 5 */
			if (s_i == t_j) {
				cost = 0;
			} else {
				cost = replace_cost;
			}
			/* Step 6 */
			above = damerau_getat(d, i - 1, j, n);
			left = damerau_getat(d, i, j - 1, n);
			diag = damerau_getat(d, i - 1, j - 1, n);
			if (j >= 2 && i >= 2) {
				/* NEW: detect transpositions */
				diag2 = damerau_getat(d, i - 2, j - 2, n);
				if (s[i - 2] == t[j - 1] && s[i - 1] == t[j - 2]) {
					cost2 = transpose_cost;
				} else {
					cost2 = 2;
				}
				cell = MIN4(above + insdel_cost, left + insdel_cost,
							diag + cost, diag2 + cost2);
			} else {
				cell = MIN3(above + insdel_cost, left + insdel_cost,
							diag + cost);
			}
			damerau_putat(d, i, j, n, cell);
		}
	}
	/* Step 7 */
	*res = damerau_getat(d, n, m, n);
	GDKfree(d);
	return MAL_SUCCEED;
}

static str
TXTSIMdameraulevenshtein1(int *result, str *s, str *t)
{
	return dameraulevenshtein(result, s, t, 1, 1, 2);
}

static str
TXTSIMdameraulevenshtein2(int *result, str *s, str *t)
{
	return dameraulevenshtein(result, s, t, 1, 1, 1);
}

static str
TXTSIMdameraulevenshtein(Client cntxt, MalBlkPtr mb, MalStkPtr stk,
						 InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	int *res = getArgReference_int(stk, pci, 0);
	str *X = getArgReference_str(stk, pci, 1),
		*Y = getArgReference_str(stk, pci, 2);
	int insdel_cost, replace_cost, transpose_cost;

	assert(pci->argc == 3 || pci->argc == 6);

	if (pci->argc == 3) {
		insdel_cost = 1;
		replace_cost = 1;
		transpose_cost = 2;
	} else {
		insdel_cost = *getArgReference_int(stk, pci, 3);
		replace_cost = *getArgReference_int(stk, pci, 4);
		transpose_cost = *getArgReference_int(stk, pci, 5);
	}

	return dameraulevenshtein(res, X, Y, insdel_cost, replace_cost,
							  transpose_cost);
}

static inline str
levenshtein(int *res, const str *X, const str *Y, int insdel_cost,
			int replace_cost, int max)
{
	str x = *X, y = *Y, x_iter = x;
	unsigned int xlen, ylen, i = 0, j = 0;
	unsigned int last_diagonal, old_diagonal;
	int cx, cy;
	unsigned int *column, min;

	if (strNil(*X) || strNil(*Y)) {
		*res = int_nil;
		return MAL_SUCCEED;
	}

	xlen = UTF8_strlen(x);
	ylen = UTF8_strlen(y);

	if (xlen == ylen && (strcmp(x, y) == 0))
		return MAL_SUCCEED;

	column = GDKmalloc((xlen + 1) * sizeof(unsigned int));
	if (column == NULL)
		throw(MAL, "levenshtein", MAL_MALLOC_FAIL);

	for (i = 1; i <= xlen; i++)
		column[i] = i;

	for (j = 1; j <= ylen; j++) {
		column[0] = j;
		min = INT_MAX;
		x_iter = x;
		UTF8_GETCHAR(cy, y);
		for (i = 1, last_diagonal = j - 1; i <= xlen; i++) {
			UTF8_GETCHAR(cx, x_iter);
			old_diagonal = column[i];
			column[i] = MIN3(column[i] + insdel_cost,
							 column[i - 1] + insdel_cost,
							 last_diagonal + (cx == cy ? 0 : replace_cost));
			last_diagonal = old_diagonal;
			if (last_diagonal < min)
				min = last_diagonal;
		}
		if (max != -1 && min > (unsigned int) max) {
			*res = INT_MAX;
			GDKfree(column);
			return MAL_SUCCEED;
		}
		(*x)++;
	}

	*res = column[xlen];
	GDKfree(column);
	return MAL_SUCCEED;
  illegal:
	/* UTF8_GETCHAR bail */
	GDKfree(column);
	throw(MAL, "txtsim.levenshtein", "Illegal unicode code point");
}

/* Levenshtein OP but with column externaly allocated */
static inline int
levenshtein2(const str X, const str Y, const size_t xlen, const size_t ylen,
			 unsigned int *column, const int insdel_cost,
			 const int replace_cost, const int max)
{
	str x = X, y = Y;
	str x_iter = x;
	unsigned int i = 0, j = 0, min;;
	unsigned int last_diagonal, old_diagonal;
	int cx, cy;

	if (strNil(x) || strNil(y))
		return int_nil;

	if (xlen == ylen && (strcmp(x, y) == 0))
		return 0;

	for (i = 1; i <= xlen; i++)
		column[i] = i;

	for (j = 1; j <= ylen; j++) {
		column[0] = j;
		min = INT_MAX;
		x_iter = x;
		UTF8_GETCHAR(cy, y);
		for (i = 1, last_diagonal = j - 1; i <= xlen; i++) {
			UTF8_GETCHAR(cx, x_iter);
			old_diagonal = column[i];
			column[i] = MIN3(column[i] + insdel_cost,
							 column[i - 1] + insdel_cost,
							 last_diagonal + (cx == cy ? 0 : replace_cost));
			last_diagonal = old_diagonal;
			if (last_diagonal < min)
				min = last_diagonal;
		}
		if (max != -1 && min > (unsigned int) max)
			return INT_MAX;
		(*x)++;
	}
	return column[xlen];
  illegal:
	/* UTF8_GETCHAR bail */
	return INT_MAX;
}

static str
TXTSIMlevenshtein(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	int *res = getArgReference_int(stk, pci, 0);
	str *X = getArgReference_str(stk, pci, 1),
		*Y = getArgReference_str(stk, pci, 2);
	int insdel_cost, replace_cost;

	if (pci->argc == 3) {
		insdel_cost = 1;
		replace_cost = 1;
	} else if (pci->argc == 5 || pci->argc == 6) {
		insdel_cost = *getArgReference_int(stk, pci, 3);
		replace_cost = *getArgReference_int(stk, pci, 4);
		/* Backwards compatibility purposes */
		if (pci->argc == 6) {
			int transposition_cost = *getArgReference_int(stk, pci, 5);
			return dameraulevenshtein(res, X, Y, insdel_cost, replace_cost,
									  transposition_cost);
		}
	} else {
		throw(MAL, "txtsim.levenshtein", RUNTIME_SIGNATURE_MISSING);
	}

	return levenshtein(res, X, Y, insdel_cost, replace_cost, -1);;
}

static str
TXTSIMmaxlevenshtein(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	int *res = getArgReference_int(stk, pci, 0);
	const str *X = getArgReference_str(stk, pci, 1),
		*Y = getArgReference_str(stk, pci, 2);
	const int *k = getArgReference_int(stk, pci, 3);
	int insdel_cost, replace_cost;

	if (pci->argc == 4) {
		insdel_cost = 1;
		replace_cost = 1;
	} else if (pci->argc == 6) {
		insdel_cost = *getArgReference_int(stk, pci, 4);
		replace_cost = *getArgReference_int(stk, pci, 5);
	} else {
		throw(MAL, "txtsim.maxlevenshtein", RUNTIME_SIGNATURE_MISSING);
	}

	return levenshtein(res, X, Y, insdel_cost, replace_cost, *k);
}

static str
BATTXTSIMmaxlevenshtein(Client cntxt, MalBlkPtr mb, MalStkPtr stk, InstrPtr pci)
{
	(void) cntxt;
	(void) mb;
	bat *res = getArgReference_bat(stk, pci, 0);
	bat *lid = getArgReference_bat(stk, pci, 1);
	bat *rid = getArgReference_bat(stk, pci, 2);
	int *k = getArgReference_int(stk, pci, 3);
	int insdel_cost = pci->argc == 6 ? *getArgReference_int(stk, pci, 4) : 1,
			replace_cost = pci->argc == 6 ? *getArgReference_int(stk, pci, 5) : 1;
	BAT *left = NULL, *right = NULL, *bn = NULL;
	BUN p, q;
	BATiter li, ri;
	unsigned int *buffer = NULL;
	str lv, rv, msg = MAL_SUCCEED;
	size_t llen = 0, rlen = 0, maxlen = 0;
	int d;
	bit v;

	if ((left = BATdescriptor(*lid)) == NULL) {
		msg = createException(MAL, "battxtsim.maxlevenshtein",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}
	if ((right = BATdescriptor(*rid)) == NULL) {
		msg = createException(MAL, "battxtsim.maxlevenshtein",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}
	if (BATcount(left) != BATcount(right)) {
		msg = createException(MAL, "battxtsim.maxlevenshtein",
							  "Columns must be aligned");
		goto exit;
	}
	if ((bn = COLnew(0, TYPE_bit, BATcount(left), TRANSIENT)) == NULL) {
		msg = createException(MAL, "battxtsim.maxlevenshtein",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}

	li = bat_iterator(left);
	ri = bat_iterator(right);
	BATloop(left, p, q) {
		lv = (str) BUNtail(li, p);
		rv = (str) BUNtail(ri, p);
		llen = UTF8_strlen(lv);
		rlen = UTF8_strlen(rv);
		if (abs((int) llen - (int) rlen) > (int) *k)
			v = false;
		else {
			if (llen > maxlen) {
				maxlen = llen;
				unsigned int *tmp = GDKrealloc(buffer, (maxlen + 1) * sizeof(unsigned int));
				if (tmp == NULL) {
					bat_iterator_end(&li);
					bat_iterator_end(&ri);
					msg = createException(MAL, "battxtsim.maxlevenshtein",
										  SQLSTATE(HY013) MAL_MALLOC_FAIL);
					goto exit;
				}
				buffer = tmp;
			}
			d = levenshtein2(lv, rv, llen, rlen, buffer, insdel_cost,
							 replace_cost, (int) *k);
			v = (bit) (d <= (int) *k);
		}
		if (BUNappend(bn, (const void *) &v, false) != GDK_SUCCEED) {
			bat_iterator_end(&li);
			bat_iterator_end(&ri);
			msg = createException(MAL, "battxtsim.maxlevenshtein",
								  "BUNappend failed");
			goto exit;
		}
	}
	bat_iterator_end(&li);
	bat_iterator_end(&ri);

	*res = bn->batCacheid;
	BBPkeepref(bn);
  exit:
	GDKfree(buffer);
	BBPreclaim(left);
	BBPreclaim(right);
	if (msg != MAL_SUCCEED)
		BBPreclaim(bn);
	return msg;
}

#define JARO_WINKLER_SCALING_FACTOR 0.1
#define JARO_WINKLER_PREFIX_LEN 4

typedef struct {
	size_t matches;				/* accumulator for number of matches for this item */
	BUN o;						/* position in the BAT */
	str val;					/* string value */
	int *cp_sequence;			/* string as array of Unicode codepoints */
	int len;					/* string length in characters (multi-byte characters count as 1) */
	int cp_seq_len;				/* string length in bytes */
	uint64_t abm;				/* 64bit alphabet bitmap */
	int abm_popcount;			/* hamming weight of abm */
} str_item;

static inline int
popcount64(uint64_t x)
{
	x = (x & 0x5555555555555555ULL) + ((x >> 1) & 0x5555555555555555ULL);
	x = (x & 0x3333333333333333ULL) + ((x >> 2) & 0x3333333333333333ULL);
	x = (x & 0x0F0F0F0F0F0F0F0FULL) + ((x >> 4) & 0x0F0F0F0F0F0F0F0FULL);
	return (x * 0x0101010101010101ULL) >> 56;
}

static int
str_item_lenrev_cmp(const void *a, const void *b)
{
	return ((int) ((str_item *) b)->len - (int) ((str_item *) a)->len);
}

static str
str_2_codepointseq(str_item *s)
{
	str p = s->val;
	int c;

	s->cp_sequence = GDKmalloc(s->len * sizeof(int));
	if (s->cp_sequence == NULL)
		throw(MAL, "str_2_byteseq", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (int i = 0; i < s->len; i++) {
		UTF8_GETCHAR(c, p);
		if (c == 0)
			break;
		s->cp_sequence[i] = c;
	}
	return MAL_SUCCEED;
  illegal:
	throw(MAL, "str_2_byteseq", SQLSTATE(42000) "Illegal unicode code point");
}

static void
str_alphabet_bitmap(str_item *s)
{
	int i;

	s->abm = 0ULL;

	for (i = 0; i < s->len; i++)
		s->abm |= 1ULL << (s->cp_sequence[i] % 64);

	s->abm_popcount = popcount64(s->abm);
}

static inline double
jarowinkler_lp(const str_item *a, const str_item *b)
{
	unsigned int l;

	/* calculate common string prefix up to prefixlen chars */
	l = 0;
	for (int i = 0; i < MIN3(a->len, b->len, JARO_WINKLER_PREFIX_LEN); i++)
		l += (a->cp_sequence[i] == b->cp_sequence[i]);

	return (double) l *JARO_WINKLER_SCALING_FACTOR;
}

static inline double
jarowinkler(const str_item *x, const str_item *y, double lp, int *x_flags,
			int *y_flags)
{
	int xlen = x->len, ylen = y->len;
	int range = MAX(0, MAX(xlen, ylen) / 2 - 1);
	int *x1 = x->cp_sequence, *x2 = y->cp_sequence;
	int m = 0, t = 0;
	int i, j, l;
	double dw;

	if (!xlen || !ylen)
		return 0.0;
	for (i = 0; i < xlen; i++)
		x_flags[i] = 0;
	for (i = 0; i < ylen; i++)
		y_flags[i] = 0;
	/* matching chars */
	for (i = 0; i < ylen; i++) {
		for (j = MAX(i - range, 0), l = MIN(i + range + 1, xlen); j < l; j++) {
			if (x2[i] == x1[j] && !x_flags[j]) {
				x_flags[j] = 1;
				y_flags[i] = 1;
				m++;
				break;
			}
		}
	}
	if (!m)
		return 0.0;
	/* char transpositions */
	l = 0;
	for (i = 0; i < ylen; i++) {
		if (y_flags[i] == 1) {
			for (j = l; j < xlen; j++) {
				if (x_flags[j] == 1) {
					l = j + 1;
					break;
				}
			}
			if (x2[i] != x1[j])
				t++;
		}
	}
	t /= 2;

	/* jaro similarity */
	dw = (((double) m / xlen) + ((double) m / ylen) +
		  ((double) (m - t) / m)) / 3.0;
	/* calculate common string prefix up to prefixlen chars */
	if (lp == -1)
		lp = jarowinkler_lp(x, y);
	/* jaro-winkler similarity */
	dw = dw + (lp * (1 - dw));
	return dw;
}

static str
TXTSIMjarowinkler(dbl *res, str *x, str *y)
{
	int *x_flags = NULL, *y_flags = NULL;
	str_item xi = { 0 }, yi = { 0 };
	str msg = MAL_SUCCEED;

	xi.val = *x;
	xi.len = UTF8_strlen(*x);
	if ((msg = str_2_codepointseq(&xi)) != MAL_SUCCEED)
		goto bailout;

	yi.val = *y;
	yi.len = UTF8_strlen(*y);
	if ((msg = str_2_codepointseq(&yi)) != MAL_SUCCEED)
		goto bailout;

	x_flags = GDKmalloc(xi.len * sizeof(int));
	y_flags = GDKmalloc(yi.len * sizeof(int));

	if (x_flags == NULL || y_flags == NULL)
		goto bailout;

	*res = jarowinkler(&xi, &yi, -1, x_flags, y_flags);

  bailout:
	GDKfree(x_flags);
	GDKfree(y_flags);
	GDKfree(xi.cp_sequence);
	GDKfree(yi.cp_sequence);
	return msg;
}

static str
TXTSIMminjarowinkler(bit *res, str *x, str *y, const dbl *threshold)
{
	str msg = MAL_SUCCEED;
	double s = 1;

	msg = TXTSIMjarowinkler(&s, x, y);
	if (msg != MAL_SUCCEED)
		throw(MAL, "txt.minjarowinkler", OPERATION_FAILED);

	*res = (s > *threshold);
	return MAL_SUCCEED;
}

#define VALUE(s, x) (s##vars + VarHeapVal(s##vals, (x), s##width))
#define APPEND(b, o) (((oid *) b->theap->base)[b->batCount++] = (o))

#define PREP_BAT_STRITEM(B, CI, SI)									\
		do {															\
			for (n = 0; n < CI.ncand; n++) {							\
				SI[n].matches = 0;										\
				SI[n].o = canditer_next(&CI);							\
				SI[n].val = (str) VALUE(B, SI[n].o - B->hseqbase);		\
				SI[n].cp_sequence = NULL;								\
				SI[n].len = UTF8_strlen(SI[n].val);					\
				SI[n].cp_seq_len = str_strlen(SI[n].val);				\
				if ((msg = str_2_codepointseq(&SI[n])) != MAL_SUCCEED)	\
					goto exit;											\
				str_alphabet_bitmap(&SI[n]);							\
			}															\
		} while (false)

#define FINALIZE_BATS(L, R, LCI, RCI, LSI, RSI)	\
		do {										\
			assert(BATcount(L) == BATcount(R));	\
			BATsetcount(L, BATcount(L));			\
			BATsetcount(R, BATcount(R));			\
			for (n = 0; n < LCI.ncand; n++) {		\
				if (LSI[n].matches > 1) {			\
					L->tkey = false;				\
					break;							\
				}									\
			}										\
			if (n == LCI.ncand) {					\
				L->tkey = true;					\
			}										\
			for (n = 0; n < RCI.ncand; n++) {		\
				if (RSI[n].matches > 1) {			\
					R->tkey = false;				\
					break;							\
				}									\
			}										\
			if (n == RCI.ncand) {					\
				R->tkey = true;					\
			}										\
			BATordered(L);							\
			BATordered(R);							\
			L->theap->dirty |= BATcount(L) > 0;	\
			R->theap->dirty |= BATcount(R) > 0;	\
		} while (false)

static inline int
maxlevenshtein_extcol_stritem(const str_item *si1, const str_item *si2,
							  unsigned int *column, const int k)
{
	unsigned int lastdiag, olddiag;
	int c1, c2, x, y;
	unsigned int min;
	int s1len = si1->len, s2len = si2->len;
	int *s1 = si1->cp_sequence, *s2 = si2->cp_sequence;
	/* first test if the strings are equal */
	if (s1len == s2len) {
		for (x = 0; x < s1len; x++)
			if (s1[x] != s2[x])
				break;
		if (x == s1len)
			return 0;
	}
	for (y = 1; y <= s1len; y++)
		column[y] = y;
	for (x = 1; x <= s2len; x++) {
		c2 = s2[x - 1];
		column[0] = x;
		min = INT_MAX;
		for (y = 1, lastdiag = x - 1; y <= s1len; y++) {
			olddiag = column[y];
			c1 = s1[y - 1];
			column[y] = MIN3(column[y] + 1, column[y - 1] + 1, lastdiag + (c1 == c2 ? 0 : 1));
			lastdiag = olddiag;
			if (lastdiag < min)
				min = lastdiag;
		}
		if (min > (unsigned int) k)
			return INT_MAX;
	}
	return column[s1len];
}

static str
maxlevenshteinjoin(BAT **r1, BAT **r2, BAT *l, BAT *r, BAT *sl, BAT *sr, int k)
{
	BAT *r1t = NULL, *r2t = NULL;
	BUN n;
	struct canditer lci, rci;
	const char *lvals, *rvals, *lvars, *rvars;;
	int lwidth, rwidth, d;
	str_item *lsi = NULL, *rsi = NULL;
	str msg = MAL_SUCCEED;
	unsigned int *buffer = NULL;

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	canditer_init(&lci, l, sl);
	canditer_init(&rci, r, sr);

	if (lci.ncand == 0 || rci.ncand == 0)
		goto exit;

	lvals = (const char *) Tloc(l, 0);
	rvals = (const char *) Tloc(r, 0);
	lvars = l->tvheap->base;
	rvars = r->tvheap->base;
	lwidth = l->twidth;
	rwidth = r->twidth;

	if ((r1t = COLnew(0, TYPE_oid, lci.ncand, TRANSIENT)) == NULL ||
		(r2t = COLnew(0, TYPE_oid, rci.ncand, TRANSIENT)) == NULL) {
		reclaim_bats(2, r1t, r2t);
		msg = createException(MAL, "txtsim.maxlevenshteinjoin",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}

	r1t->tsorted = r1t->trevsorted = false;
	r2t->tsorted = r2t->trevsorted = false;

	if ((lsi = GDKmalloc(lci.ncand * sizeof(str_item))) == NULL ||
		(rsi = GDKmalloc(rci.ncand * sizeof(str_item))) == NULL) {
		reclaim_bats(2, r1t, r2t);
		msg = createException(MAL, "txtsim.maxlevenshteinjoin",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}

	PREP_BAT_STRITEM(l, lci, lsi);
	PREP_BAT_STRITEM(r, rci, rsi);
	qsort(lsi, lci.ncand, sizeof(str_item), str_item_lenrev_cmp);
	qsort(rsi, rci.ncand, sizeof(str_item), str_item_lenrev_cmp);

	if ((buffer = GDKmalloc((lsi[0].len + 1) * sizeof(int))) == NULL) {
		msg = createException(MAL, "txtsim.maxlevenshteinjoin",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}

	/* join loop */
	for (BUN lstart = 0, rstart = 0; rstart < rci.ncand; rstart++) {
		for (n = lstart; n < lci.ncand; n++) {
			/* first and cheapest filter */
			if ((lsi[n].len) > k + rsi[rstart].len) {
				lstart++;
				continue;		/* no possible matches yet for this r */
			} else if (rsi[rstart].len > k + lsi[n].len) {
				break;			/* no more possible matches from this r */
			}
			/* filter by comparing alphabet bitmaps (imprecise but fast filter) */
			d = MAX(lsi[n].abm_popcount,
					rsi[rstart].abm_popcount) -
					popcount64(lsi[n].abm & rsi[rstart].abm);
			if (d > k)
				continue;
			/* final and most expensive test: Levenshtein distance */
			d = maxlevenshtein_extcol_stritem(&lsi[n], &rsi[rstart], buffer,
											  (const unsigned int) k);
			if (d > k)
				continue;
			/* The match test succeeded */
			lsi[n].matches++;
			rsi[rstart].matches++;
			if (bunfastappTYPE(oid, r1t, &(lsi[n].o)) != GDK_SUCCEED) {
				reclaim_bats(2, r1t, r2t);
				msg = createException(MAL, "txtsim.maxlevenshteinjoin",
									  OPERATION_FAILED "Failed bun append");
				goto exit;
			}
			if (bunfastappTYPE(oid, r2t, &(rsi[rstart].o)) != GDK_SUCCEED) {
				reclaim_bats(2, r1t, r2t);
				msg = createException(MAL, "txtsim.maxlevenshteinjoin",
									  OPERATION_FAILED "Failed bun append");
				goto exit;
			}
		}
	}

	FINALIZE_BATS(r1t, r2t, lci, rci, lsi, rsi);
	*r1 = r1t;
	*r2 = r2t;

  exit:
	for (n = 0; lsi && n < lci.ncand; n++)
		GDKfree(lsi[n].cp_sequence);
	for (n = 0; rsi && n < rci.ncand; n++)
		GDKfree(rsi[n].cp_sequence);
	GDKfree(lsi);
	GDKfree(rsi);
	GDKfree(buffer);
	return msg;
}

static str
TXTSIMmaxlevenshteinjoin(bat *r1, bat *r2, const bat *lid, const bat *rid,
						 const bat *kid, const bat *slid, const bat *srid,
						 const bit *nil_matches, const lng *estimate,
						 const bit *anti)
{
	(void) nil_matches;
	(void) estimate;
	(void) anti;

	BAT *bleft = NULL, *bright = NULL, *bk = NULL,
			*bcandleft = NULL, *bcandright = NULL, *r1t = NULL, *r2t = NULL;
	int k = 0;
	str msg = MAL_SUCCEED;

	if ((bleft = BATdescriptor(*lid)) == NULL ||
		(bright = BATdescriptor(*rid)) == NULL ||
		(bk = BATdescriptor(*kid)) == NULL) {
		msg = createException(MAL, "txtsim.maxlevenshteinjoin",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}

	if ((*slid != bat_nil && (bcandleft = BATdescriptor(*slid)) == NULL) ||
		(*srid != bat_nil && (bcandright = BATdescriptor(*srid)) == NULL)) {
		msg = createException(MAL, "txtsim.maxlevenshteinjoin",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}

	if (BATcount(bk) > 0) {
		BATiter ki = bat_iterator(bk);
		k = *(int *) BUNtloc(ki, 0);
		bat_iterator_end(&ki);
	}

	if ((msg = maxlevenshteinjoin(&r1t, &r2t, bleft, bright, bcandleft, bcandright, k)) != MAL_SUCCEED)
		goto exit;

	*r1 = r1t->batCacheid;
	*r2 = r2t->batCacheid;
	BBPkeepref(r1t);
	BBPkeepref(r2t);

  exit:
	reclaim_bats(5, bleft, bright, bcandleft, bcandright, bk);
	if (msg != MAL_SUCCEED)
		reclaim_bats(2, r1t, r2t);
	return msg;
}

static inline void
jarowinkler_rangebounds(int *lb, int *ub, const str_item *a, const double lp,
						const double threshold)
{
	*lb = (int) floor(3.0 * a->len * (threshold - lp) / (1.0 - lp) -
					  (2.0 * a->len));
	*ub = (int) ceil(a->len / ((3.0 * (threshold - lp) / (1.0 - lp)) - 2.0));
}

/* version with given lp and m, and t = 0*/
static inline double
jarowinkler_lp_m_t0(const str_item *lsi, const str_item *rsi, double lp, int m)
{
	double dw;
	/* Jaro similarity */
	dw = (((double) m / lsi->len) + ((double) m / rsi->len) + 1.0) / 3.0;
	/* Jaro-Winkler similarity */
	dw = dw + (lp * (1 - dw));
	return dw;
}

static str
minjarowinklerjoin(BAT **r1, BAT **r2, BAT *l, BAT *r, BAT *sl, BAT *sr,
				   const dbl threshold)
{
	BAT *r1t = NULL, *r2t = NULL;
	BUN n;
	struct canditer lci, rci;
	const char *lvals, *rvals, *lvars, *rvars;
	int lwidth, rwidth, lb = 0, ub = 0, m = -1, *x_flags = NULL, *y_flags = NULL;
	str_item *ssl = NULL, *ssr = NULL, shortest;
	str msg = MAL_SUCCEED;
	const bool sliding_window_allowed = threshold > (2.01 + JARO_WINKLER_PREFIX_LEN * JARO_WINKLER_SCALING_FACTOR) / 3.0;
	double s, lp = 0;

	assert(ATOMtype(l->ttype) == ATOMtype(r->ttype));
	assert(ATOMtype(l->ttype) == TYPE_str);

	canditer_init(&lci, l, sl);
	canditer_init(&rci, r, sr);

	if (lci.ncand == 0 || rci.ncand == 0)
		goto exit;

	lvals = (const char *) Tloc(l, 0);
	rvals = (const char *) Tloc(r, 0);
	assert(r->ttype);
	lvars = l->tvheap->base;
	rvars = r->tvheap->base;
	lwidth = l->twidth;
	rwidth = r->twidth;

	if ((r1t = COLnew(0, TYPE_oid, lci.ncand, TRANSIENT)) == NULL ||
		(r2t = COLnew(0, TYPE_oid, rci.ncand, TRANSIENT)) == NULL) {
		reclaim_bats(2, r1t, r2t);
		msg = createException(MAL, "txtsim.minjarowinklerjoin",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}

	r1t->tsorted = r1t->trevsorted = false;
	r2t->tsorted = r2t->trevsorted = false;

	if ((ssl = GDKmalloc(lci.ncand * sizeof(str_item))) == NULL ||
		(ssr = GDKmalloc(rci.ncand * sizeof(str_item))) == NULL) {
		reclaim_bats(2, r1t, r2t);
		msg = createException(MAL, "txtsim.maxlevenshteinjoin",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}

	PREP_BAT_STRITEM(l, lci, ssl);
	PREP_BAT_STRITEM(r, rci, ssr);
	qsort(ssl, lci.ncand, sizeof(str_item), str_item_lenrev_cmp);
	qsort(ssr, rci.ncand, sizeof(str_item), str_item_lenrev_cmp);

	if ((x_flags = GDKmalloc(ssl[0].len * sizeof(int))) == NULL ||
		(y_flags = GDKmalloc(ssr[0].len * sizeof(int))) == NULL) {
		msg = createException(MAL, "txtsim.minjarowinklerjoin",
							  SQLSTATE(HY013) MAL_MALLOC_FAIL);
		goto exit;
	}
	// lp used for filters. Use -1 for actual JW (forces to compute actual lp)
	lp = JARO_WINKLER_PREFIX_LEN * JARO_WINKLER_SCALING_FACTOR;

	/* join loop */
	for (BUN lstart = 0, rstart = 0; rstart < rci.ncand; rstart++) {
		if (sliding_window_allowed)
			jarowinkler_rangebounds(&lb, &ub, &ssr[rstart], lp, threshold);
		for (n = lstart; n < lci.ncand; n++) {
			/* Update sliding window */
			/* This is the first and cheapest filter */
			if (sliding_window_allowed) {
				if (ssl[n].len > ub) {	/* no possible matches yet for this r */
					lstart++;
					continue;
				}
				if (ssl[n].len < lb) {	/* no more possible matches from this r */
					break;
				}
			}
			/* filter by comparing alphabet bitmaps */
			/* find the best possible m: the length of the shorter string
			 * minus the number of characters that surely cannot match */
			shortest = ssl[n].len < ssr[rstart].len ? ssl[n] : ssr[rstart];
			m = shortest.len - popcount64(shortest.abm -
										  (ssl[n].abm & ssr[rstart].abm));
			/* equivalent to:
			   m = shortest.len - popcount64(shortest.abm & (~(ssl[n].abm & ssr[rstart].abm))); */
			s = jarowinkler_lp_m_t0(&ssl[n], &ssr[rstart], lp, m);
			if (s < threshold) {
				continue;
			}
			/* final and most expensive test: Jaro-Winkler similarity */
			s = jarowinkler(&ssl[n], &ssr[rstart], -1, x_flags, y_flags);
			if (s < threshold) {
				continue;
			}
			/* The match test succeeded */
			ssl[n].matches++;
			ssr[rstart].matches++;
			if (bunfastappTYPE(oid, r1t, &(ssl[n].o)) != GDK_SUCCEED) {
				reclaim_bats(2, r1t, r2t);
				msg = createException(MAL, "txtsim.maxlevenshteinjoin",
									  OPERATION_FAILED "Failed bun append");
				goto exit;
			}
			if (bunfastappTYPE(oid, r2t, &(ssr[rstart].o)) != GDK_SUCCEED) {
				reclaim_bats(2, r1t, r2t);
				msg = createException(MAL, "txtsim.maxlevenshteinjoin",
									  OPERATION_FAILED "Failed bun append");
				goto exit;
			}
		}
	}

	FINALIZE_BATS(r1t, r2t, lci, rci, ssl, ssr);
	*r1 = r1t;
	*r2 = r2t;

  exit:
	if (ssl)
		for (n = 0; n < lci.ncand; n++)
			GDKfree(ssl[n].cp_sequence);
	if (ssr)
		for (n = 0; n < rci.ncand; n++)
			GDKfree(ssr[n].cp_sequence);
	GDKfree(x_flags);
	GDKfree(y_flags);
	GDKfree(ssl);
	GDKfree(ssr);
	return msg;
}

static str
TXTSIMminjarowinklerjoin(bat *r1, bat *r2, const bat *lid, const bat *rid,
						 const bat *thresholdid, const bat *slid,
						 const bat *srid, const bit *nil_matches,
						 const lng *estimate, const bit *anti)
{
	(void) nil_matches;
	(void) estimate;
	(void) anti;

	BAT *bleft = NULL, *bright = NULL,
			*bcandleft = NULL, *bcandright = NULL, *bthreshold = NULL,
			*r1t = NULL, *r2t = NULL;
	dbl threshold = 1;
	str msg = MAL_SUCCEED;

	if ((bleft = BATdescriptor(*lid)) == NULL ||
		(bright = BATdescriptor(*rid)) == NULL ||
		(bthreshold = BATdescriptor(*thresholdid)) == NULL) {
		msg = createException(MAL, "txtsim.minjarowinklerjoin",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}

	if ((*slid != bat_nil && (bcandleft = BATdescriptor(*slid)) == NULL) ||
		(*srid != bat_nil && (bcandright = BATdescriptor(*srid)) == NULL)) {
		msg = createException(MAL, "txtsim.minjarowinklerjoin",
							  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
		goto exit;
	}

	if (BATcount(bthreshold) > 0) {
		BATiter thresholdi = bat_iterator(bthreshold);
		threshold = *(dbl *) BUNtail(thresholdi, 0);
		bat_iterator_end(&thresholdi);
	}

	if ((msg = minjarowinklerjoin(&r1t, &r2t, bleft, bright, bcandleft, bcandright, threshold)) != MAL_SUCCEED)
		goto exit;

	*r1 = r1t->batCacheid;
	*r2 = r2t->batCacheid;
	BBPkeepref(r1t);
	BBPkeepref(r2t);

  exit:
	reclaim_bats(5, bleft, bright, bcandleft, bcandright, bthreshold);
	if (msg != MAL_SUCCEED)
		reclaim_bats(2, r1t, r2t);
	return msg;
}

#define SoundexLen 4			/* length of a soundex code */
#define SoundexKey "Z000"		/* default key for soundex code */

/* set letter values */
static const int Code[] = { 0, 1, 2, 3, 0, 1,
	2, 0, 0, 2, 2, 4,
	5, 5, 0, 1, 2, 6,
	2, 3, 0, 1, 0, 2, 0, 2
};

#define RETURN_NIL_IF(b,t)												\
	if (b) {															\
		if (ATOMextern(t)) {											\
			*(ptr*) res = (ptr) ATOMnil(t);							\
			if ( *(ptr *) res == NULL)									\
				throw(MAL,"txtsim", SQLSTATE(HY013) MAL_MALLOC_FAIL);	\
		} else {														\
			memcpy(res, ATOMnilptr(t), ATOMsize(t));					\
		}																\
		return MAL_SUCCEED;											\
	}

static inline char
SCode(unsigned char c)
{
	if (c == 95)
		return (2);				/* german sz */
	return (Code[toupper(c) - 'A']);
}

static str
soundex_code(const char *Name, char *Key)
{
	char LastLetter;
	int Index;

	for (const char *p = Name; *p; p++)
		if ((*p & 0x80) != 0)
			throw(MAL, "soundex",
				  SQLSTATE(42000)
				  "Soundex function not available for non ASCII strings");

	/* set default key */
	strcpy(Key, SoundexKey);

	/* keep first letter */
	Key[0] = *Name;
	if (!isupper((unsigned char) (Key[0])))
		Key[0] = toupper(Key[0]);

	LastLetter = *Name;
	if (!*Name)
		return MAL_SUCCEED;
	Name++;

	/* scan rest of string */
	for (Index = 1; (Index <SoundexLen) &&*Name; Name++) {
		/* use only letters */
		if (isalpha((unsigned char) (*Name))) {
			/* ignore duplicate successive chars */
			if (LastLetter != *Name) {
				/* new LastLetter */
				LastLetter = *Name;

				/* ignore letters with code 0 */
				if (SCode(*Name) != 0) {
					Key[Index] = '0' + SCode(*Name);
					Index ++;
				}
			}
		}
	}
	return MAL_SUCCEED;
}

static str
soundex(str *res, str *Name)
{
	str msg = MAL_SUCCEED;

	GDKfree(*res);
	RETURN_NIL_IF(strNil(*Name), TYPE_str);

	*res = (str) GDKmalloc(sizeof(char) * (SoundexLen + 1));
	if (*res == NULL)
		throw(MAL, "soundex", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	/* calculate Key for Name */
	if ((msg = soundex_code(*Name, *res))) {
		GDKfree(*res);
		*res = NULL;
		return msg;
	}
	return msg;
}

static str
stringdiff(int *res, str *s1, str *s2)
{
	str r = MAL_SUCCEED;
	char *S1 = NULL, *S2 = NULL;

	r = soundex(&S1, s1);
	if (r != MAL_SUCCEED)
		return r;
	r = soundex(&S2, s2);
	if (r != MAL_SUCCEED) {
		GDKfree(S1);
		return r;
	}
	r = TXTSIMdameraulevenshtein1(res, &S1, &S2);
	GDKfree(S1);
	GDKfree(S2);
	return r;
}

/******************************
 * QGRAMNORMALIZE
 *
 * This function 'normalizes' a string so valid q-grams can  be made of it:
 * All characters are transformed to uppercase, and all characters
 * which are not letters or digits are stripped to a single space.
 *
 * qgramnormalize("Hallo, allemaal!").print(); --> "HALLO ALLEMAAL"
 * qgramnormalize(" '' t ' est").print(); --> [ "T EST" ]
 *
 *****************************/
static str
qgram_normalize(str *res, str *Input)
{
	char *input = *Input;
	int i, j = 0;
	char c, last = ' ';

	GDKfree(*res);
	RETURN_NIL_IF(strNil(input), TYPE_str);
	*res = (str) GDKmalloc(sizeof(char) * (strlen(input) + 1));	/* normalized strings are never longer than original */
	if (*res == NULL)
		throw(MAL, "txtsim.qgramnormalize", SQLSTATE(HY013) MAL_MALLOC_FAIL);

	for (i = 0; input[i]; i++) {
		c = toupper(input[i]);
		if (!(('A' <= c && c <= 'Z') || isdigit((unsigned char) c)))
			c = ' ';
		if (c != ' ' || last != ' ') {
			(*res)[j++] = c;
		}
		last = c;
	}
	(*res)[j] = 0;
	/* strip final whitespace */
	while (j > 0 && (*res)[--j] == ' ')
		(*res)[j] = 0;

	return MAL_SUCCEED;
}

static str
qgram_selfjoin(bat *res1, bat *res2, bat *qid, bat *bid, bat *pid, bat *lid,
			   flt *c, int *k)
{
	BAT *qgram, *id, *pos, *len;
	BUN n;
	BUN i, j;
	BAT *bn, *bn2;
	oid *qbuf;
	int *ibuf;
	int *pbuf;
	int *lbuf;
	str msg = MAL_SUCCEED;

	qgram = BATdescriptor(*qid);
	id = BATdescriptor(*bid);
	pos = BATdescriptor(*pid);
	len = BATdescriptor(*lid);
	if (qgram == NULL || id == NULL || pos == NULL || len == NULL) {
		BBPreclaim(qgram);
		BBPreclaim(id);
		BBPreclaim(pos);
		BBPreclaim(len);
		throw(MAL, "txtsim.qgramselfjoin",
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}

	BATiter qgrami = bat_iterator(qgram);
	BATiter idi = bat_iterator(id);
	BATiter posi = bat_iterator(pos);
	BATiter leni = bat_iterator(len);
	if (qgrami.type != TYPE_oid)
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": tail of BAT qgram must be oid");
	else if (idi.type != TYPE_int)
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": tail of BAT id must be int");
	else if (posi.type != TYPE_int)
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": tail of BAT pos must be int");
	else if (leni.type != TYPE_int)
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": tail of BAT len must be int");
	if (msg) {
		bat_iterator_end(&qgrami);
		bat_iterator_end(&idi);
		bat_iterator_end(&posi);
		bat_iterator_end(&leni);
		BBPunfix(qgram->batCacheid);
		BBPunfix(id->batCacheid);
		BBPunfix(pos->batCacheid);
		BBPunfix(len->batCacheid);
		return msg;
	}

	n = BATcount(qgram);

	/* if (BATcount(qgram)>1 && !qgrami.sorted) throw(MAL, "txtsim.qgramselfjoin", SEMANTIC_TYPE_MISMATCH); */

	if (!ALIGNsynced(qgram, id))
		 msg = createException(MAL, "txtsim.qgramselfjoin",
							   SEMANTIC_TYPE_MISMATCH
							   ": qgram and id are not synced");

	else if (!ALIGNsynced(qgram, pos))
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": qgram and pos are not synced");
	else if (!ALIGNsynced(qgram, len))
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": qgram and len are not synced");

	else if (qgrami.width != ATOMsize(qgrami.type))
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": qgram is not a true void bat");
	else if (idi.width != ATOMsize(idi.type))
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": id is not a true void bat");

	else if (posi.width != ATOMsize(posi.type))
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": pos is not a true void bat");
	else if (leni.width != ATOMsize(leni.type))
		msg = createException(MAL, "txtsim.qgramselfjoin",
							  SEMANTIC_TYPE_MISMATCH
							  ": len is not a true void bat");
	if (msg) {
		bat_iterator_end(&qgrami);
		bat_iterator_end(&idi);
		bat_iterator_end(&posi);
		bat_iterator_end(&leni);
		BBPunfix(qgram->batCacheid);
		BBPunfix(id->batCacheid);
		BBPunfix(pos->batCacheid);
		BBPunfix(len->batCacheid);
		return msg;
	}

	bn = COLnew(0, TYPE_int, n, TRANSIENT);
	bn2 = COLnew(0, TYPE_int, n, TRANSIENT);
	if (bn == NULL || bn2 == NULL) {
		bat_iterator_end(&qgrami);
		bat_iterator_end(&idi);
		bat_iterator_end(&posi);
		bat_iterator_end(&leni);
		BBPreclaim(bn);
		BBPreclaim(bn2);
		BBPunfix(qgram->batCacheid);
		BBPunfix(id->batCacheid);
		BBPunfix(pos->batCacheid);
		BBPunfix(len->batCacheid);
		throw(MAL, "txtsim.qgramselfjoin", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	qbuf = (oid *) qgrami.base;
	ibuf = (int *) idi.base;
	pbuf = (int *) posi.base;
	lbuf = (int *) leni.base;
	for (i = 0; i < n - 1; i++) {
		for (j = i + 1;
			 (j < n && qbuf[j] == qbuf[i]
			  && pbuf[j] <= (pbuf[i] + (*k + *c * MIN(lbuf[i], lbuf[j]))));
			 j++) {
			if (ibuf[i] != ibuf[j]
				&& abs(lbuf[i] - lbuf[j]) <= (*k + *c * MIN(lbuf[i], lbuf[j]))) {
				if (BUNappend(bn, ibuf + i, false) != GDK_SUCCEED
					|| BUNappend(bn2, ibuf + j, false) != GDK_SUCCEED) {
					bat_iterator_end(&qgrami);
					bat_iterator_end(&idi);
					bat_iterator_end(&posi);
					bat_iterator_end(&leni);
					BBPunfix(qgram->batCacheid);
					BBPunfix(id->batCacheid);
					BBPunfix(pos->batCacheid);
					BBPunfix(len->batCacheid);
					BBPreclaim(bn);
					BBPreclaim(bn2);
					throw(MAL, "txtsim.qgramselfjoin",
						  SQLSTATE(HY013) MAL_MALLOC_FAIL);
				}
			}
		}
	}
	bat_iterator_end(&qgrami);
	bat_iterator_end(&idi);
	bat_iterator_end(&posi);
	bat_iterator_end(&leni);

	BBPunfix(qgram->batCacheid);
	BBPunfix(id->batCacheid);
	BBPunfix(pos->batCacheid);
	BBPunfix(len->batCacheid);

	*res1 = bn->batCacheid;
	BBPkeepref(bn);
	*res2 = bn2->batCacheid;
	BBPkeepref(bn2);

	return MAL_SUCCEED;
}

/* copy up to utf8len UTF-8 encoded characters from src to buf
 * stop early if buf (size given by bufsize) is too small, or if src runs out
 * return number of UTF-8 characters copied (excluding NUL)
 * close with NUL if enough space */
static size_t
utf8strncpy(char *buf, size_t bufsize, const char *src, size_t utf8len)
{
	size_t cnt = 0;

	while (utf8len != 0 && *src != 0 && bufsize != 0) {
		bufsize--;
		utf8len--;
		cnt++;
		if (((*buf++ = *src++) & 0x80) != 0) {
			while ((*src & 0xC0) == 0x80 && bufsize != 0) {
				*buf++ = *src++;
				bufsize--;
			}
		}
	}
	if (bufsize != 0)
		*buf = 0;
	return cnt;
}

static str
str_2_qgrams(bat *ret, str *val)
{
	BAT *bn;
	size_t i, len = strlen(*val) + 5;
	str s = GDKmalloc(len);
	char qgram[4 * 6 + 1];		/* 4 UTF-8 code points plus NULL byte */

	if (s == NULL)
		throw(MAL, "txtsim.str2qgram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	strcpy(s, "##");
	strcpy(s + 2, *val);
	strcpy(s + len - 3, "$$");
	bn = COLnew(0, TYPE_str, (BUN) strlen(*val), TRANSIENT);
	if (bn == NULL) {
		GDKfree(s);
		throw(MAL, "txtsim.str2qgram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	i = 0;
	while (s[i]) {
		if (utf8strncpy(qgram, sizeof(qgram), s + i, 4) < 4)
			break;
		if (BUNappend(bn, qgram, false) != GDK_SUCCEED) {
			BBPreclaim(bn);
			GDKfree(s);
			throw(MAL, "txtsim.str2qgram", SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if ((s[i++] & 0xC0) == 0xC0) {
			while ((s[i] & 0xC0) == 0x80)
				i++;
		}
	}
	*ret = bn->batCacheid;
	BBPkeepref(bn);
	GDKfree(s);
	return MAL_SUCCEED;
}

#include "mel.h"
mel_func txtsim_init_funcs[] = {
	pattern("txtsim", "dameraulevenshtein", TXTSIMdameraulevenshtein, false, "Calculates Damerau-Levenshtein distance between two strings, operation costs (ins/del = 1, replacement = 1, transposition = 2)", args(1,3,arg("",int),arg("x",str),arg("y",str))),
	pattern("txtsim", "dameraulevenshtein", TXTSIMdameraulevenshtein, false, "Calculates Damerau-Levenshtein distance between two strings, variable operation costs (ins/del, replacement, transposition)", args(1,6,arg("",int),arg("x",str),arg("y",str),arg("insdel_cost",int),arg("replace_cost",int),arg("transpose_cost",int))),
	command("txtsim", "editdistance", TXTSIMdameraulevenshtein1, false, "Alias for Damerau-Levenshtein(str,str), insdel cost = 1, replace cost = 1 and transpose = 2", args(1,3, arg("",int),arg("s",str),arg("t",str))),
	command("txtsim", "editdistance2", TXTSIMdameraulevenshtein2, false, "Alias for Damerau-Levenshtein(str,str), insdel cost = 1, replace cost = 1 and transpose = 1", args(1,3, arg("",int),arg("s",str),arg("t",str))),
	pattern("txtsim", "levenshtein", TXTSIMlevenshtein, false, "Calculates Levenshtein distance between two strings, operation costs (ins/del = 1, replacement = 1)", args(1,3,arg("",int),arg("s",str),arg("t",str))),
	pattern("txtsim", "levenshtein", TXTSIMlevenshtein, false, "Calculates Levenshtein distance between two strings, variable operation costs (ins/del, replacement)", args(1,5,arg("",int),arg("x",str),arg("y",str),arg("insdel_cost",int),arg("replace_cost",int))),
	pattern("txtsim", "levenshtein", TXTSIMlevenshtein, false, "(Backwards compatibility purposes) Calculates Damerau-Levenshtein distance between two strings, variable operation costs (ins/del, replacement, transposition)", args(1,6,arg("",int),arg("x",str),arg("y",str),arg("insdel_cost",int),arg("replace_cost",int),arg("transpose_cost",int))),
	pattern("txtsim", "maxlevenshtein", TXTSIMmaxlevenshtein, false, "Levenshtein distance with basic costs but up to a MAX", args(1, 4, arg("",int), arg("l",str),arg("r",str),arg("k",int))),
	pattern("txtsim", "maxlevenshtein", TXTSIMmaxlevenshtein, false, "Levenshtein distance with variable costs but up to a MAX", args(1, 6, arg("",int), arg("l",str),arg("r",str),arg("k",int),arg("insdel_cost",int),arg("replace_cost",int))),
	pattern("battxtsim", "maxlevenshtein", BATTXTSIMmaxlevenshtein, false, "Same as maxlevenshtein but for BATS", args(1, 4, batarg("",bit), batarg("l",str),batarg("r",str),arg("k",int))),
	pattern("battxtsim", "maxlevenshtein", BATTXTSIMmaxlevenshtein, false, "Same as maxlevenshtein but for BATS", args(1, 6, batarg("",bit), batarg("l",str),batarg("r",str),arg("k",int),arg("insdel_cost",int),arg("replace_cost",int))),
	command("txtsim", "maxlevenshteinjoin", TXTSIMmaxlevenshteinjoin, false, "", args(2,10, batarg("",oid),batarg("",oid),batarg("l",str),batarg("r",str),batarg("k",int),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	command("txtsim", "soundex", soundex, false, "Soundex function for phonetic matching", args(1,2, arg("",str),arg("name",str))),
	command("txtsim", "stringdiff", stringdiff, false, "Calculate the soundexed editdistance", args(1,3, arg("",int),arg("s1",str),arg("s2",str))),
	command("txtsim", "qgramnormalize", qgram_normalize, false, "'Normalizes' strings (eg. toUpper and replaces non-alphanumerics with one space", args(1,2, arg("",str),arg("input",str))),
	command("txtsim", "qgramselfjoin", qgram_selfjoin, false, "QGram self-join on ordered(!) qgram tables and sub-ordered q-gram positions", args(2,8, batarg("",int),batarg("",int),batarg("qgram",oid),batarg("id",oid),batarg("pos",int),batarg("len",int),arg("c",flt),arg("k",int))),
	command("txtsim", "str2qgrams", str_2_qgrams, false, "Break the string into 4-grams", args(1,2, batarg("",str),arg("s",str))),
	command("txtsim", "jarowinkler", TXTSIMjarowinkler, false, "Calculate Jaro Winkler similarity", args(1,3, arg("",dbl),arg("x",str),arg("y",str))),
	command("txtsim", "minjarowinkler", TXTSIMminjarowinkler, false, "", args(1, 4, arg("",bit), arg("l",str),arg("r",str),arg("threshold",dbl))),
	command("txtsim", "minjarowinklerjoin", TXTSIMminjarowinklerjoin, false, "", args(2, 10, batarg("",oid),batarg("",oid), batarg("l",str),batarg("r",str),batarg("threshold",dbl),batarg("sl",oid),batarg("sr",oid),arg("nil_matches",bit),arg("estimate",lng),arg("anti",bit))),
	{ .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_txtsim_mal)
{ mal_module("txtsim", NULL, txtsim_init_funcs); }
