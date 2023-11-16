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

#define ANALYTICAL_MIN_MAX_CALC_FIXED_UNBOUNDED_TILL_CURRENT_ROW(TPE, MIN_MAX) \
	do {								\
		TPE curval = TPE##_nil;				\
		for (; k < i;) {					\
			j = k;						\
			do {						\
				if (!is_##TPE##_nil(bp[k])) {		\
					if (is_##TPE##_nil(curval))	\
						curval = bp[k];	\
					else				\
						curval = MIN_MAX(bp[k], curval); \
				}					\
				k++;					\
			} while (k < i && !op[k]);			\
			for (; j < k; j++)				\
				rb[j] = curval;			\
			has_nils |= is_##TPE##_nil(curval);		\
		}							\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_CURRENT_ROW_TILL_UNBOUNDED(TPE, MIN_MAX) \
	do {								\
		TPE curval = TPE##_nil;				\
		l = i - 1;						\
		for (j = l; ; j--) {					\
			if (!is_##TPE##_nil(bp[j])) {			\
				if (is_##TPE##_nil(curval))		\
					curval = bp[j];		\
				else					\
					curval = MIN_MAX(bp[j], curval);\
			}						\
			if (op[j] || j == k) {				\
				for (; ; l--) {			\
					rb[l] = curval;		\
					if (l == j)			\
						break;			\
				}					\
				has_nils |= is_##TPE##_nil(curval);	\
				if (j == k)				\
					break;				\
				l = j - 1;				\
			}						\
		}							\
		k = i;							\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_ALL_ROWS(TPE, MIN_MAX)		\
	do {								\
		TPE curval = TPE##_nil;				\
		for (j = k; j < i; j++) {				\
			TPE v = bp[j];					\
			if (!is_##TPE##_nil(v)) {			\
				if (is_##TPE##_nil(curval))		\
					curval = v;			\
				else					\
					curval = MIN_MAX(v, curval);	\
			}						\
		}							\
		for (; k < i; k++)					\
			rb[k] = curval;				\
		has_nils |= is_##TPE##_nil(curval);			\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_CURRENT_ROW(TPE, MIN_MAX) \
	do {							\
		for (; k < i; k++) {				\
			TPE v = bp[k];				\
			rb[k] = v;				\
			has_nils |= is_##TPE##_nil(v);		\
		}						\
	} while (0)

#define INIT_AGGREGATE_MIN_MAX_FIXED(TPE, MIN_MAX, NOTHING)	\
	do {							\
		computed = TPE##_nil;				\
	} while (0)

#define COMPUTE_LEVEL0_MIN_MAX_FIXED(X, TPE, MIN_MAX, NOTHING)	\
	do {							\
		computed = bp[j + X];				\
	} while (0)

#define COMPUTE_LEVELN_MIN_MAX_FIXED(VAL, TPE, MIN_MAX, NOTHING)	\
	do {								\
		if (!is_##TPE##_nil(VAL)) {				\
			if (is_##TPE##_nil(computed))			\
				computed = VAL;			\
			else						\
				computed = MIN_MAX(computed, VAL);	\
		}							\
	} while (0)

#define FINALIZE_AGGREGATE_MIN_MAX_FIXED(TPE, MIN_MAX, NOTHING) \
	do {							\
		rb[k] = computed;				\
		has_nils |= is_##TPE##_nil(computed);		\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_FIXED_OTHERS(TPE, MIN_MAX)		\
	do {								\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(TPE), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(TPE, ncount, INIT_AGGREGATE_MIN_MAX_FIXED, COMPUTE_LEVEL0_MIN_MAX_FIXED, COMPUTE_LEVELN_MIN_MAX_FIXED, TPE, MIN_MAX, NOTHING); \
		for (; k < i; k++)					\
			compute_on_segment_tree(TPE, start[k] - j, end[k] - j, INIT_AGGREGATE_MIN_MAX_FIXED, COMPUTE_LEVELN_MIN_MAX_FIXED, FINALIZE_AGGREGATE_MIN_MAX_FIXED, TPE, MIN_MAX, NOTHING); \
		j = k;							\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_OTHERS_UNBOUNDED_TILL_CURRENT_ROW(GT_LT) \
	do {								\
		const void *curval = nil;				\
		if (ATOMvarsized(tpe)) {				\
			for (; k < i;) {				\
				j = k;					\
				do {					\
					const void *next = BUNtvar(bi, k); \
					if (atomcmp(next, nil) != 0) {	\
						if (atomcmp(curval, nil) == 0) \
							curval = next;	\
						else			\
							curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
					}				\
					k++;				\
				} while (k < i && !op[k]);		\
				for (; j < k; j++)			\
					if ((res = tfastins_nocheckVAR(r, j, curval)) != GDK_SUCCEED) \
						goto cleanup;		\
				has_nils |= atomcmp(curval, nil) == 0;	\
			}						\
		} else {						\
			for (; k < i;) {				\
				j = k;					\
				do {					\
					const void *next = BUNtloc(bi, k); \
					if (atomcmp(next, nil) != 0) {	\
						if (atomcmp(curval, nil) == 0) \
							curval = next;	\
						else			\
							curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
					}				\
					k++;				\
				} while (k < i && !op[k]);		\
				for (; j < k; j++) {			\
					memcpy(rcast, curval, width);	\
					rcast += width;		\
				}					\
				has_nils |= atomcmp(curval, nil) == 0;	\
			}						\
		}							\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_OTHERS_CURRENT_ROW_TILL_UNBOUNDED(GT_LT)\
	do {								\
		const void *curval = nil;				\
		l = i - 1;						\
		if (ATOMvarsized(tpe)) {				\
			for (j = l; ; j--) {				\
				const void *next = BUNtvar(bi, j);	\
				if (atomcmp(next, nil) != 0) {		\
					if (atomcmp(curval, nil) == 0)	\
						curval = next;		\
					else				\
						curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
				}					\
				if (op[j] || j == k) {			\
					for (; ; l--) {		\
						if ((res = tfastins_nocheckVAR(r, l, curval)) != GDK_SUCCEED) \
							goto cleanup;	\
						if (l == j)		\
							break;		\
					}				\
					has_nils |= atomcmp(curval, nil) == 0; \
					if (j == k)			\
						break;			\
					l = j - 1;			\
				}					\
			}						\
		} else {						\
			for (j = l; ; j--) {				\
				const void *next = BUNtloc(bi, j);	\
				if (atomcmp(next, nil) != 0) {		\
					if (atomcmp(curval, nil) == 0)	\
						curval = next;		\
					else				\
						curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
				}					\
				if (op[j] || j == k) {			\
					BUN x = l * width;		\
					for (; ; l--) {			\
						memcpy(rcast + x, curval, width); \
						x -= width;		\
						if (l == j)		\
							break;		\
					}				\
					has_nils |= atomcmp(curval, nil) == 0; \
					if (j == k)			\
						break;			\
					l = j - 1;			\
				}					\
			}						\
		}							\
		k = i;							\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_OTHERS_ALL_ROWS(GT_LT)			\
	do {								\
		const void *curval = (void*) nil;			\
		if (ATOMvarsized(tpe)) {				\
			for (j = k; j < i; j++) {			\
				const void *next = BUNtvar(bi, j);	\
				if (atomcmp(next, nil) != 0) {		\
					if (atomcmp(curval, nil) == 0)	\
						curval = next;		\
					else				\
						curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
				}					\
			}						\
			for (; k < i; k++)				\
				if ((res = tfastins_nocheckVAR(r, k, curval)) != GDK_SUCCEED) \
					goto cleanup;			\
		} else {						\
			for (j = k; j < i; j++) {			\
				const void *next = BUNtloc(bi, j);	\
				if (atomcmp(next, nil) != 0) {		\
					if (atomcmp(curval, nil) == 0)	\
						curval = next;		\
					else				\
						curval = atomcmp(next, curval) GT_LT 0 ? curval : next; \
				}					\
			}						\
			for (; k < i; k++) {				\
				memcpy(rcast, curval, width);		\
				rcast += width;			\
			}						\
		}							\
		has_nils |= atomcmp(curval, nil) == 0;			\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_OTHERS_CURRENT_ROW(GT_LT)		\
	do {								\
		if (ATOMvarsized(tpe)) {				\
			for (; k < i; k++) {				\
				const void *next = BUNtvar(bi, k);	\
				if ((res = tfastins_nocheckVAR(r, k, next)) != GDK_SUCCEED) \
					goto cleanup;			\
				has_nils |= atomcmp(next, nil) == 0;	\
			}						\
		} else {						\
			for (; k < i; k++) {				\
				const void *next = BUNtloc(bi, k);	\
				memcpy(rcast, next, width);		\
				rcast += width;			\
				has_nils |= atomcmp(next, nil) == 0;	\
			}						\
		}							\
	} while (0)

#define INIT_AGGREGATE_MIN_MAX_OTHERS(GT_LT, NOTHING1, NOTHING2)	\
	do {								\
		computed = (void*) nil;				\
	} while (0)

#define COMPUTE_LEVEL0_MIN_MAX_OTHERS(X, GT_LT, NOTHING1, NOTHING2)	\
	do {								\
		computed = BUNtail(bi, j + X);				\
	} while (0)

#define COMPUTE_LEVELN_MIN_MAX_OTHERS(VAL, GT_LT, NOTHING1, NOTHING2)	\
	do {								\
		if (atomcmp(VAL, nil) != 0) {				\
			if (atomcmp(computed, nil) == 0)		\
				computed = VAL;			\
			else						\
				computed = atomcmp(VAL, computed) GT_LT 0 ? computed : VAL; \
		}							\
	} while (0)

#define FINALIZE_AGGREGATE_MIN_MAX_OTHERS(GT_LT, NOTHING1, NOTHING2)	\
	do {								\
		if (ATOMvarsized(tpe)) {				\
			if ((res = tfastins_nocheckVAR(r, k, computed)) != GDK_SUCCEED) \
				goto cleanup;				\
		} else {						\
			memcpy(rcast, computed, width);		\
			rcast += width;				\
		}							\
		has_nils |= atomcmp(computed, nil) == 0;		\
	} while (0)

#define ANALYTICAL_MIN_MAX_CALC_OTHERS_OTHERS(GT_LT)			\
	do {								\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(void*), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(void*, ncount, INIT_AGGREGATE_MIN_MAX_OTHERS, COMPUTE_LEVEL0_MIN_MAX_OTHERS, COMPUTE_LEVELN_MIN_MAX_OTHERS, GT_LT, NOTHING, NOTHING); \
		for (; k < i; k++)					\
			compute_on_segment_tree(void*, start[k] - j, end[k] - j, INIT_AGGREGATE_MIN_MAX_OTHERS, COMPUTE_LEVELN_MIN_MAX_OTHERS, FINALIZE_AGGREGATE_MIN_MAX_OTHERS, GT_LT, NOTHING, NOTHING); \
		j = k;							\
	} while (0)

#define ANALYTICAL_MIN_MAX_PARTITIONS(TPE, MIN_MAX, IMP)		\
	do {								\
		TPE *restrict bp = (TPE*)bi.base, *rb = (TPE*)Tloc(r, 0); \
		if (p) {						\
			while (i < cnt) {				\
				if (np[i]) {				\
				minmaxfixed##TPE##IMP:			\
					ANALYTICAL_MIN_MAX_CALC_FIXED_##IMP(TPE, MIN_MAX); \
				}					\
				if (!last)				\
					i++;				\
			}						\
		}							\
		if (!last) { /* hack to reduce code explosion, there's no need to duplicate the code to iterate each partition */ \
			last = true;					\
			i = cnt;					\
			goto minmaxfixed##TPE##IMP;			\
		}							\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_MIN_MAX_LIMIT(MIN_MAX, IMP)			\
	case TYPE_hge:						\
	ANALYTICAL_MIN_MAX_PARTITIONS(hge, MIN_MAX, IMP);	\
	break;
#else
#define ANALYTICAL_MIN_MAX_LIMIT(MIN_MAX, IMP)
#endif

#define ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, IMP)		\
	do {								\
		switch (ATOMbasetype(tpe)) {				\
		case TYPE_bte:						\
			ANALYTICAL_MIN_MAX_PARTITIONS(bte, MIN_MAX, IMP); \
			break;						\
		case TYPE_sht:						\
			ANALYTICAL_MIN_MAX_PARTITIONS(sht, MIN_MAX, IMP); \
			break;						\
		case TYPE_int:						\
			ANALYTICAL_MIN_MAX_PARTITIONS(int, MIN_MAX, IMP); \
			break;						\
		case TYPE_lng:						\
			ANALYTICAL_MIN_MAX_PARTITIONS(lng, MIN_MAX, IMP); \
			break;						\
			ANALYTICAL_MIN_MAX_LIMIT(MIN_MAX, IMP)		\
		case TYPE_flt:						\
			ANALYTICAL_MIN_MAX_PARTITIONS(flt, MIN_MAX, IMP); \
			break;						\
		case TYPE_dbl:						\
			ANALYTICAL_MIN_MAX_PARTITIONS(dbl, MIN_MAX, IMP); \
			break;						\
		default: {						\
			if (p) {					\
				while (i < cnt) {			\
					if (np[i]) {			\
					minmaxvarsized##IMP:		\
						ANALYTICAL_MIN_MAX_CALC_OTHERS_##IMP(GT_LT); \
					}				\
					if (!last)			\
						i++;			\
				}					\
			}						\
			if (!last) {					\
				last = true;				\
				i = cnt;				\
				goto minmaxvarsized##IMP;		\
			}						\
		}							\
		}							\
	} while (0)

#define GDKANALYTIC(OP, MIN_MAX, GT_LT) OP

gdk_return
GDKANALYTIC(OP, MIN_MAX, GT_LT)(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type)
{
	BATiter pi = bat_iterator(p);
	BATiter oi = bat_iterator(o);
	BATiter bi = bat_iterator(b);
	BATiter si = bat_iterator(s);
	BATiter ei = bat_iterator(e);
	bool has_nils = false, last = false;
	oid i = 0, j = 0, k = 0, l = 0, cnt = BATcount(b), *restrict start = si.base, *restrict end = ei.base,
		*levels_offset = NULL, nlevels = 0;
	bit *np = pi.base, *op = oi.base;
	const void *nil = ATOMnilptr(tpe);
	int (*atomcmp)(const void *, const void *) = ATOMcompare(tpe);
	void *segment_tree = NULL;
	gdk_return res = GDK_SUCCEED;
	uint16_t width = r->twidth;
	uint8_t *restrict rcast = (uint8_t *) Tloc(r, 0);
	BAT *st = NULL;

	if (cnt > 0) {
		switch (frame_type) {
		case 3: /* unbounded until current row */
			ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, UNBOUNDED_TILL_CURRENT_ROW); \
                        break;
		case 4: /* current row until unbounded */
			ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, CURRENT_ROW_TILL_UNBOUNDED); \
                        break;
		case 5: /* all rows */
			ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, ALL_ROWS);
			break;
		case 6: /* current row */
			ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, CURRENT_ROW);
			break;
		default:
			if (!(st = GDKinitialize_segment_tree())) {
				res = GDK_FAIL;
				goto cleanup;
			}
			ANALYTICAL_MIN_MAX_BRANCHES(MIN_MAX, GT_LT, OTHERS);
			break;
		}
	}

	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
 cleanup:
	bat_iterator_end(&pi);
	bat_iterator_end(&oi);
	bat_iterator_end(&bi);
	bat_iterator_end(&si);
	bat_iterator_end(&ei);
	BBPreclaim(st);
	return res;
}
