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
#include "gdk_analytic.h"
#include "gdk_calc_private.h"

#ifdef HAVE_HGE
#define LNG_HGE         hge
#define GDK_LNG_HGE_max GDK_hge_max
#define LNG_HGE_nil     hge_nil
#else
#define LNG_HGE         lng
#define GDK_LNG_HGE_max GDK_lng_max
#define LNG_HGE_nil     lng_nil
#endif

/* average on integers */
#define ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, ARG)		\
	if (!is_##TPE##_nil(ARG)) {					\
		ADD_WITH_CHECK(ARG, sum, LNG_HGE, sum, GDK_LNG_HGE_max, goto avg_overflow##TPE##IMP); \
		/* count only when no overflow occurs */		\
		n++;							\
	}

#define ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP)		\
			if (0) {				\
avg_overflow##TPE##IMP:						\
				assert(n > 0);			\
				if (sum >= 0) {			\
					a = (TPE) (sum / n);	\
					rr = (lng) (sum % n);	\
				} else {			\
					sum = -sum;		\
					a = - (TPE) (sum / n);	\
					rr = (lng) (sum % n);	\
					if (r) {		\
						a--;		\
						rr = n - rr;	\
					}			\
				}

#define ANALYTICAL_AVG_IMP_NUM_UNBOUNDED_TILL_CURRENT_ROW(TPE, IMP)	\
	do {								\
		TPE a = 0;						\
		dbl curval = dbl_nil;					\
		for (; k < i;) {					\
			j = k;						\
			do {						\
				ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, bp[k]) \
				ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP) \
					while (k < i && !op[k]) {	\
						TPE v = bp[k++];	\
						if (is_##TPE##_nil(v))	\
							continue;	\
						AVERAGE_ITER(TPE, v, a, rr, n);	\
					}				\
					curval = a + (dbl) rr / n;	\
					goto calc_done##TPE##IMP;	\
				}					\
				k++;					\
			} while (k < i && !op[k]);			\
			curval = n > 0 ? (dbl) sum / n : dbl_nil;	\
calc_done##TPE##IMP:							\
			for (; j < k; j++)				\
				rb[j] = curval;				\
			has_nils |= (n == 0);				\
		}							\
		n = 0;							\
		sum = 0;						\
	} while (0)

#define ANALYTICAL_AVG_IMP_NUM_CURRENT_ROW_TILL_UNBOUNDED(TPE, IMP)	\
	do {								\
		TPE a = 0;						\
		dbl curval = dbl_nil;					\
		l = i - 1;						\
		for (j = l; ; j--) {					\
			ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, bp[j]) \
			ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP)	\
				while (!(op[j] || j == k)) {		\
					TPE v = bp[j--];		\
					if (is_##TPE##_nil(v))		\
						continue;		\
					AVERAGE_ITER(TPE, v, a, rr, n);	\
				}					\
				curval = a + (dbl) rr / n;		\
				goto calc_done##TPE##IMP;		\
			}						\
			if (op[j] || j == k) {				\
				curval = n > 0 ? (dbl) sum / n : dbl_nil; \
calc_done##TPE##IMP:							\
				for (; ; l--) {				\
					rb[l] = curval;			\
					if (l == j)			\
						break;			\
				}					\
				has_nils |= (n == 0);			\
				if (j == k)				\
					break;				\
				l = j - 1;				\
			}						\
		}							\
		n = 0;							\
		sum = 0;						\
		k = i;							\
	} while (0)

#define ANALYTICAL_AVG_IMP_NUM_ALL_ROWS(TPE, IMP)			\
	do {								\
		TPE a = 0;						\
		for (; j < i; j++) {					\
			TPE v = bp[j];					\
			ANALYTICAL_AVERAGE_CALC_NUM_STEP1(TPE, IMP, v)	\
			ANALYTICAL_AVERAGE_CALC_NUM_STEP2(TPE, IMP)	\
				for (; j < i; j++) {			\
					v = bp[j];			\
					if (is_##TPE##_nil(v))		\
						continue;		\
					AVERAGE_ITER(TPE, v, a, rr, n);	\
				}					\
				curval = a + (dbl) rr / n;		\
				goto calc_done##TPE##IMP;		\
			}						\
		}							\
		curval = n > 0 ? (dbl) sum / n : dbl_nil;		\
calc_done##TPE##IMP:							\
		for (; k < i; k++)					\
			rb[k] = curval;					\
		has_nils |= (n == 0);					\
		n = 0;							\
		sum = 0;						\
	} while (0)

#define ANALYTICAL_AVG_IMP_NUM_CURRENT_ROW(TPE, IMP)	\
	do {						\
		for (; k < i; k++) {			\
			TPE v = bp[k];			\
			if (is_##TPE##_nil(v)) {	\
				rb[k] = dbl_nil;	\
				has_nils = true;	\
			} else	{			\
				rb[k] = (dbl) v;	\
			}				\
		}					\
	} while (0)

#define avg_num_deltas(TPE) typedef struct avg_num_deltas##TPE { TPE a; lng n; lng rr;} avg_num_deltas##TPE;
avg_num_deltas(bte)
avg_num_deltas(sht)
avg_num_deltas(int)
avg_num_deltas(lng)

#define INIT_AGGREGATE_AVG_NUM(TPE, NOTHING1, NOTHING2) \
	do {						\
		computed = (avg_num_deltas##TPE) {0};	\
	} while (0)
#define COMPUTE_LEVEL0_AVG_NUM(X, TPE, NOTHING1, NOTHING2)		\
	do {								\
		TPE v = bp[j + X];					\
		computed = is_##TPE##_nil(v) ? (avg_num_deltas##TPE){0} : (avg_num_deltas##TPE) {.a = v, .n = 1}; \
	} while (0)
#define COMPUTE_LEVELN_AVG_NUM(VAL, TPE, NOTHING1, NOTHING2)		\
	do {								\
		if (VAL.n)						\
			AVERAGE_ITER(TPE, VAL.a, computed.a, computed.rr, computed.n); \
	} while (0)
#define FINALIZE_AGGREGATE_AVG_NUM(TPE, NOTHING1, NOTHING2)		\
	do {								\
		if (computed.n == 0) {					\
			rb[k] = dbl_nil;				\
			has_nils = true;				\
		} else {						\
			rb[k] = computed.a + (dbl) computed.rr / computed.n; \
		}							\
	} while (0)
#define ANALYTICAL_AVG_IMP_NUM_OTHERS(TPE, IMP)				\
	do {								\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(avg_num_deltas##TPE), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(avg_num_deltas##TPE, ncount, INIT_AGGREGATE_AVG_NUM, COMPUTE_LEVEL0_AVG_NUM, COMPUTE_LEVELN_AVG_NUM, TPE, NOTHING, NOTHING); \
		for (; k < i; k++)					\
			compute_on_segment_tree(avg_num_deltas##TPE, start[k] - j, end[k] - j, INIT_AGGREGATE_AVG_NUM, COMPUTE_LEVELN_AVG_NUM, FINALIZE_AGGREGATE_AVG_NUM, TPE, NOTHING, NOTHING); \
		j = k;							\
	} while (0)

/* average on floating-points */
#define ANALYTICAL_AVG_IMP_FP_UNBOUNDED_TILL_CURRENT_ROW(TPE, IMP)	\
	do {								\
		TPE a = 0;						\
		dbl curval = dbl_nil;					\
		for (; k < i;) {					\
			j = k;						\
			do {						\
				if (!is_##TPE##_nil(bp[k]))		\
					AVERAGE_ITER_FLOAT(TPE, bp[k], a, n); \
				k++;					\
			} while (k < i && !op[k]);			\
			if (n > 0)					\
				curval = a;				\
			else						\
				has_nils = true;			\
			for (; j < k; j++)				\
				rb[j] = curval;				\
		}							\
		n = 0;							\
	} while (0)

#define ANALYTICAL_AVG_IMP_FP_CURRENT_ROW_TILL_UNBOUNDED(TPE, IMP)	\
	do {								\
		TPE a = 0;						\
		dbl curval = dbl_nil;					\
		l = i - 1;						\
		for (j = l; ; j--) {					\
			if (!is_##TPE##_nil(bp[j]))			\
				AVERAGE_ITER_FLOAT(TPE, bp[j], a, n);	\
			if (op[j] || j == k) {				\
				for (; ; l--) {				\
					rb[l] = curval;			\
					if (l == j)			\
						break;			\
				}					\
				has_nils |= is_##TPE##_nil(curval);	\
				if (j == k)				\
					break;				\
				l = j - 1;				\
			}						\
		}							\
		n = 0;							\
		k = i;							\
	} while (0)

#define ANALYTICAL_AVG_IMP_FP_ALL_ROWS(TPE, IMP)			\
	do {								\
		TPE a = 0;						\
		dbl curval = dbl_nil;					\
		for (; j < i; j++) {					\
			TPE v = bp[j];					\
			if (!is_##TPE##_nil(v))				\
				AVERAGE_ITER_FLOAT(TPE, v, a, n);	\
		}							\
		if (n > 0)						\
			curval = a;					\
		else							\
			has_nils = true;				\
		for (; k < i; k++)					\
			rb[k] = curval;					\
		n = 0;							\
	} while (0)

#define ANALYTICAL_AVG_IMP_FP_CURRENT_ROW(TPE, IMP)	 ANALYTICAL_AVG_IMP_NUM_CURRENT_ROW(TPE, IMP)

#define avg_fp_deltas(TPE) typedef struct avg_fp_deltas_##TPE {TPE a; lng n;} avg_fp_deltas_##TPE;
avg_fp_deltas(flt)
avg_fp_deltas(dbl)

#define INIT_AGGREGATE_AVG_FP(TPE, NOTHING1, NOTHING2)	\
	do {						\
		computed = (avg_fp_deltas_##TPE) {0};	\
	} while (0)
#define COMPUTE_LEVEL0_AVG_FP(X, TPE, NOTHING1, NOTHING2)		\
	do {								\
		TPE v = bp[j + X];					\
		computed = is_##TPE##_nil(v) ? (avg_fp_deltas_##TPE) {0} : (avg_fp_deltas_##TPE) {.n = 1, .a = v}; \
	} while (0)
#define COMPUTE_LEVELN_AVG_FP(VAL, TPE, NOTHING1, NOTHING2)		\
	do {								\
		if (VAL.n)						\
			AVERAGE_ITER_FLOAT(TPE, VAL.a, computed.a, computed.n); \
	} while (0)
#define FINALIZE_AGGREGATE_AVG_FP(TPE, NOTHING1, NOTHING2)	\
	do {							\
		if (computed.n == 0) {				\
			rb[k] = dbl_nil;			\
			has_nils = true;			\
		} else {					\
			rb[k] = computed.a;			\
		}						\
	} while (0)
#define ANALYTICAL_AVG_IMP_FP_OTHERS(TPE, IMP)				\
	do {								\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(avg_fp_deltas_##TPE), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(avg_fp_deltas_##TPE, ncount, INIT_AGGREGATE_AVG_FP, COMPUTE_LEVEL0_AVG_FP, COMPUTE_LEVELN_AVG_FP, TPE, NOTHING, NOTHING); \
		for (; k < i; k++)					\
			compute_on_segment_tree(avg_fp_deltas_##TPE, start[k] - j, end[k] - j, INIT_AGGREGATE_AVG_FP, COMPUTE_LEVELN_AVG_FP, FINALIZE_AGGREGATE_AVG_FP, TPE, NOTHING, NOTHING); \
		j = k;							\
	} while (0)

#define ANALYTICAL_AVG_PARTITIONS(TPE, IMP, REAL_IMP)		\
	do {							\
		TPE *restrict bp = (TPE*)bi.base;		\
		if (p) {					\
			while (i < cnt) {			\
				if (np[i]) 	{		\
avg##TPE##IMP:							\
					REAL_IMP(TPE, IMP);	\
				}				\
				if (!last)			\
					i++;			\
			}					\
		}						\
		if (!last) {					\
			last = true;				\
			i = cnt;				\
			goto avg##TPE##IMP;			\
		}						\
	} while (0)

#ifdef HAVE_HGE
avg_num_deltas(hge)
#define ANALYTICAL_AVG_LIMIT(IMP)					\
	case TYPE_hge:							\
		ANALYTICAL_AVG_PARTITIONS(hge, IMP, ANALYTICAL_AVG_IMP_NUM_##IMP); \
		break;
#else
#define ANALYTICAL_AVG_LIMIT(IMP)
#endif

#define ANALYTICAL_AVG_BRANCHES(IMP)					\
	do {								\
		switch (tpe) {						\
		case TYPE_bte:						\
			ANALYTICAL_AVG_PARTITIONS(bte, IMP, ANALYTICAL_AVG_IMP_NUM_##IMP); \
			break;						\
		case TYPE_sht:						\
			ANALYTICAL_AVG_PARTITIONS(sht, IMP, ANALYTICAL_AVG_IMP_NUM_##IMP); \
			break;						\
		case TYPE_int:						\
			ANALYTICAL_AVG_PARTITIONS(int, IMP, ANALYTICAL_AVG_IMP_NUM_##IMP); \
			break;						\
		case TYPE_lng:						\
			ANALYTICAL_AVG_PARTITIONS(lng, IMP, ANALYTICAL_AVG_IMP_NUM_##IMP); \
			break;						\
		ANALYTICAL_AVG_LIMIT(IMP)				\
		case TYPE_flt:						\
			ANALYTICAL_AVG_PARTITIONS(flt, IMP, ANALYTICAL_AVG_IMP_FP_##IMP); \
			break;						\
		case TYPE_dbl:						\
			ANALYTICAL_AVG_PARTITIONS(dbl, IMP, ANALYTICAL_AVG_IMP_FP_##IMP); \
			break;						\
		default:						\
			goto nosupport;					\
		}							\
	} while (0)

gdk_return
GDKanalyticalavg(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type)
{
	BATiter pi = bat_iterator(p);
	BATiter oi = bat_iterator(o);
	BATiter bi = bat_iterator(b);
	BATiter si = bat_iterator(s);
	BATiter ei = bat_iterator(e);
	bool has_nils = false, last = false;
	oid i = 0, j = 0, k = 0, l = 0, cnt = BATcount(b), *restrict start = si.base, *restrict end = ei.base,
		*levels_offset = NULL, nlevels = 0;
	lng n = 0, rr = 0;
	dbl *rb = (dbl *) Tloc(r, 0), curval = dbl_nil;
	bit *np = pi.base, *op = oi.base;
	void *segment_tree = NULL;
	gdk_return res = GDK_SUCCEED;
#ifdef HAVE_HGE
	hge sum = 0;
#else
	lng sum = 0;
#endif
	BAT *st = NULL;

	if (cnt > 0) {
		switch (frame_type) {
		case 3: /* unbounded until current row */	{
			ANALYTICAL_AVG_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW);
		} break;
		case 4: /* current row until unbounded */	{
			ANALYTICAL_AVG_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED);
		} break;
		case 5: /* all rows */	{
			ANALYTICAL_AVG_BRANCHES(ALL_ROWS);
		} break;
		case 6: /* current row */ {
			ANALYTICAL_AVG_BRANCHES(CURRENT_ROW);
		} break;
		default: {
			if (!(st = GDKinitialize_segment_tree())) {
				res = GDK_FAIL;
				goto cleanup;
			}
			ANALYTICAL_AVG_BRANCHES(OTHERS);
		}
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
nosupport:
	GDKerror("42000!average of type %s to dbl unsupported.\n", ATOMname(tpe));
	res = GDK_FAIL;
	goto cleanup;
}

#ifdef TRUNCATE_NUMBERS
#define ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avg, rem, ncnt)	\
	do {							\
		if (rem > 0 && avg < 0)				\
			avg++;					\
	} while(0)
#else
#define ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avg, rem, ncnt)	\
	do {							\
		if (rem > 0) {					\
			if (avg < 0) {				\
				if (2*rem > ncnt)		\
					avg++;			\
			} else {				\
				if (2*rem >= ncnt)		\
					avg++;			\
			}					\
		}						\
	} while(0)
#endif

#define ANALYTICAL_AVG_INT_UNBOUNDED_TILL_CURRENT_ROW(TPE)		\
	do {								\
		TPE avg = 0;						\
		for (; k < i;) {					\
			j = k;						\
			do {						\
				if (!is_##TPE##_nil(bp[k]))		\
					AVERAGE_ITER(TPE, bp[k], avg, rem, ncnt); \
				k++;					\
			} while (k < i && !op[k]);			\
			if (ncnt == 0) {				\
				has_nils = true;			\
				for (; j < k; j++)			\
					rb[j] = TPE##_nil;		\
			} else {					\
				TPE avgfinal = avg;			\
				ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avgfinal, rem, ncnt); \
				for (; j < k; j++)			\
					rb[j] = avgfinal;		\
			}						\
		}							\
		rem = 0;						\
		ncnt = 0;						\
	} while (0)

#define ANALYTICAL_AVG_INT_CURRENT_ROW_TILL_UNBOUNDED(TPE)		\
	do {								\
		TPE avg = 0;						\
		l = i - 1;						\
		for (j = l; ; j--) {					\
			if (!is_##TPE##_nil(bp[j]))			\
				AVERAGE_ITER(TPE, bp[j], avg, rem, ncnt); \
			if (op[j] || j == k) {				\
				if (ncnt == 0) {			\
					has_nils = true;		\
					for (; ; l--) {			\
						rb[l] = TPE##_nil;	\
						if (l == j)		\
							break;		\
					}				\
				} else {				\
					TPE avgfinal = avg;		\
					ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avgfinal, rem, ncnt); \
					for (; ; l--) {			\
						rb[l] = avgfinal;	\
						if (l == j)		\
							break;		\
					}				\
				}					\
				if (j == k)				\
					break;				\
				l = j - 1;				\
			}						\
		}							\
		rem = 0;						\
		ncnt = 0;						\
		k = i;							\
	} while (0)

#define ANALYTICAL_AVG_INT_ALL_ROWS(TPE)				\
	do {								\
		TPE avg = 0;						\
		for (; j < i; j++) {					\
			TPE v = bp[j];					\
			if (!is_##TPE##_nil(v))				\
				AVERAGE_ITER(TPE, v, avg, rem, ncnt);	\
		}							\
		if (ncnt == 0) {					\
			for (; k < i; k++)				\
				rb[k] = TPE##_nil;			\
			has_nils = true;				\
		} else {						\
			ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(avg, rem, ncnt); \
			for (; k < i; k++)				\
				rb[k] = avg;				\
		}							\
		rem = 0;						\
		ncnt = 0;						\
	} while (0)

#define ANALYTICAL_AVG_INT_CURRENT_ROW(TPE)		\
	do {						\
		for (; k < i; k++) {			\
			TPE v = bp[k];			\
			rb[k] = bp[k];			\
			has_nils |= is_##TPE##_nil(v);	\
		}					\
	} while (0)

#define avg_int_deltas(TPE) typedef struct avg_int_deltas_##TPE { TPE avg; lng rem, ncnt;} avg_int_deltas_##TPE;
avg_int_deltas(bte)
avg_int_deltas(sht)
avg_int_deltas(int)
avg_int_deltas(lng)

#define INIT_AGGREGATE_AVG_INT(TPE, NOTHING1, NOTHING2) \
	do {						\
		computed = (avg_int_deltas_##TPE) {0};	\
	} while (0)
#define COMPUTE_LEVEL0_AVG_INT(X, TPE, NOTHING1, NOTHING2)		\
	do {								\
		TPE v = bp[j + X];					\
		computed = is_##TPE##_nil(v) ? (avg_int_deltas_##TPE) {0} : (avg_int_deltas_##TPE) {.avg = v, .ncnt = 1}; \
	} while (0)
#define COMPUTE_LEVELN_AVG_INT(VAL, TPE, NOTHING1, NOTHING2)		\
	do {								\
		if (VAL.ncnt)						\
			AVERAGE_ITER(TPE, VAL.avg, computed.avg, computed.rem, computed.ncnt); \
	} while (0)
#define FINALIZE_AGGREGATE_AVG_INT(TPE, NOTHING1, NOTHING2)		\
	do {								\
		if (computed.ncnt == 0) {				\
			has_nils = true;				\
			rb[k] = TPE##_nil;				\
		} else {						\
			ANALYTICAL_AVERAGE_INT_CALC_FINALIZE(computed.avg, computed.rem, computed.ncnt); \
			rb[k] = computed.avg;				\
		}							\
	} while (0)
#define ANALYTICAL_AVG_INT_OTHERS(TPE)					\
	do {								\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(avg_int_deltas_##TPE), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(avg_int_deltas_##TPE, ncount, INIT_AGGREGATE_AVG_INT, COMPUTE_LEVEL0_AVG_INT, COMPUTE_LEVELN_AVG_INT, TPE, NOTHING, NOTHING); \
		for (; k < i; k++)					\
			compute_on_segment_tree(avg_int_deltas_##TPE, start[k] - j, end[k] - j, INIT_AGGREGATE_AVG_INT, COMPUTE_LEVELN_AVG_INT, FINALIZE_AGGREGATE_AVG_INT, TPE, NOTHING, NOTHING); \
		j = k;							\
	} while (0)

#define ANALYTICAL_AVG_INT_PARTITIONS(TPE, IMP)				\
	do {								\
		TPE *restrict bp = (TPE*)bi.base, *rb = (TPE *) Tloc(r, 0); \
		if (p) {						\
			while (i < cnt) {				\
				if (np[i]) 	{			\
avg##TPE##IMP:								\
					IMP(TPE);			\
				}					\
				if (!last)				\
					i++;				\
			}						\
		}							\
		if (!last) {						\
			last = true;					\
			i = cnt;					\
			goto avg##TPE##IMP;				\
		}							\
	} while (0)

#ifdef HAVE_HGE
avg_int_deltas(hge)
#define ANALYTICAL_AVG_INT_LIMIT(IMP)					\
	case TYPE_hge:							\
		ANALYTICAL_AVG_INT_PARTITIONS(hge, ANALYTICAL_AVG_INT_##IMP); \
		break;
#else
#define ANALYTICAL_AVG_INT_LIMIT(IMP)
#endif

#define ANALYTICAL_AVG_INT_BRANCHES(IMP)				\
	do {								\
		switch (tpe) {						\
		case TYPE_bte:						\
			ANALYTICAL_AVG_INT_PARTITIONS(bte, ANALYTICAL_AVG_INT_##IMP); \
			break;						\
		case TYPE_sht:						\
			ANALYTICAL_AVG_INT_PARTITIONS(sht, ANALYTICAL_AVG_INT_##IMP); \
			break;						\
		case TYPE_int:						\
			ANALYTICAL_AVG_INT_PARTITIONS(int, ANALYTICAL_AVG_INT_##IMP); \
			break;						\
		case TYPE_lng:						\
			ANALYTICAL_AVG_INT_PARTITIONS(lng, ANALYTICAL_AVG_INT_##IMP); \
			break;						\
		ANALYTICAL_AVG_INT_LIMIT(IMP)				\
		default:						\
			goto nosupport;					\
		}							\
	} while (0)

gdk_return
GDKanalyticalavginteger(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type)
{
	BATiter pi = bat_iterator(p);
	BATiter oi = bat_iterator(o);
	BATiter bi = bat_iterator(b);
	BATiter si = bat_iterator(s);
	BATiter ei = bat_iterator(e);
	bool has_nils = false, last = false;
	oid i = 0, j = 0, k = 0, l = 0, cnt = BATcount(b), *restrict start = si.base, *restrict end = ei.base,
		*levels_offset = NULL, nlevels = 0;
	lng rem = 0, ncnt = 0;
	bit *np = pi.base, *op = oi.base;
	void *segment_tree = NULL;
	gdk_return res = GDK_SUCCEED;
	BAT *st = NULL;

	if (cnt > 0) {
		switch (frame_type) {
		case 3: /* unbounded until current row */	{
			ANALYTICAL_AVG_INT_BRANCHES(UNBOUNDED_TILL_CURRENT_ROW);
		} break;
		case 4: /* current row until unbounded */	{
			ANALYTICAL_AVG_INT_BRANCHES(CURRENT_ROW_TILL_UNBOUNDED);
		} break;
		case 5: /* all rows */	{
			ANALYTICAL_AVG_INT_BRANCHES(ALL_ROWS);
		} break;
		case 6: /* current row */ {
			ANALYTICAL_AVG_INT_BRANCHES(CURRENT_ROW);
		} break;
		default: {
			if (!(st = GDKinitialize_segment_tree())) {
				res = GDK_FAIL;
				goto cleanup;
			}
			ANALYTICAL_AVG_INT_BRANCHES(OTHERS);
		}
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
nosupport:
	GDKerror("42000!average of type %s to %s unsupported.\n", ATOMname(tpe), ATOMname(tpe));
	res = GDK_FAIL;
	goto cleanup;
}

#define ANALYTICAL_STDEV_VARIANCE_UNBOUNDED_TILL_CURRENT_ROW(TPE, SAMPLE, OP) \
	do {								\
		TPE *restrict bp = (TPE*)bi.base;			\
		for (; k < i;) {					\
			j = k;						\
			do {						\
				TPE v = bp[k];				\
				if (!is_##TPE##_nil(v))	 {		\
					n++;				\
					delta = (dbl) v - mean;		\
					mean += delta / n;		\
					m2 += delta * ((dbl) v - mean);	\
				}					\
				k++;					\
			} while (k < i && !op[k]);			\
			if (isinf(m2)) {				\
				goto overflow;				\
			} else if (n > SAMPLE) {			\
				for (; j < k; j++)			\
					rb[j] = OP;			\
			} else {					\
				for (; j < k; j++)			\
					rb[j] = dbl_nil;		\
				has_nils = true;			\
			}						\
		}							\
		n = 0;							\
		mean = 0;						\
		m2 = 0;							\
	} while (0)

#define ANALYTICAL_STDEV_VARIANCE_CURRENT_ROW_TILL_UNBOUNDED(TPE, SAMPLE, OP) \
	do {								\
		TPE *restrict bp = (TPE*)bi.base;			\
		l = i - 1;						\
		for (j = l; ; j--) {					\
			TPE v = bp[j];					\
			if (!is_##TPE##_nil(v))	{			\
				n++;					\
				delta = (dbl) v - mean;			\
				mean += delta / n;			\
				m2 += delta * ((dbl) v - mean);		\
			}						\
			if (op[j] || j == k) {				\
				if (isinf(m2)) {			\
					goto overflow;			\
				} else if (n > SAMPLE) {		\
					for (; ; l--) {			\
						rb[l] = OP;		\
						if (l == j)		\
							break;		\
					}				\
				} else {				\
					for (; ; l--) {			\
						rb[l] = dbl_nil;	\
						if (l == j)		\
							break;		\
					}				\
					has_nils = true;		\
				}					\
				if (j == k)				\
					break;				\
				l = j - 1;				\
			}						\
		}							\
		n = 0;							\
		mean = 0;						\
		m2 = 0;							\
		k = i;							\
	} while (0)

#define ANALYTICAL_STDEV_VARIANCE_ALL_ROWS(TPE, SAMPLE, OP)	\
	do {							\
		TPE *restrict bp = (TPE*)bi.base;		\
		for (; j < i; j++) {				\
			TPE v = bp[j];				\
			if (is_##TPE##_nil(v))			\
				continue;			\
			n++;					\
			delta = (dbl) v - mean;			\
			mean += delta / n;			\
			m2 += delta * ((dbl) v - mean);		\
		}						\
		if (isinf(m2)) {				\
			goto overflow;				\
		} else if (n > SAMPLE) {			\
			for (; k < i; k++)			\
				rb[k] = OP;			\
		} else {					\
			for (; k < i; k++)			\
				rb[k] = dbl_nil;		\
			has_nils = true;			\
		}						\
		n = 0;						\
		mean = 0;					\
		m2 = 0;						\
	} while (0)

#define ANALYTICAL_STDEV_VARIANCE_CURRENT_ROW(TPE, SAMPLE, OP)	\
	do {							\
		for (; k < i; k++)				\
			rb[k] = SAMPLE == 1 ? dbl_nil : 0;	\
		has_nils = is_dbl_nil(rb[k - 1]);		\
	} while (0)

typedef struct stdev_var_deltas {
	BUN n;
	dbl mean, delta, m2;
} stdev_var_deltas;

#define INIT_AGGREGATE_STDEV_VARIANCE(TPE, SAMPLE, OP)	\
	do {						\
		computed = (stdev_var_deltas) {0};	\
	} while (0)
#define COMPUTE_LEVEL0_STDEV_VARIANCE(X, TPE, SAMPLE, OP)		\
	do {								\
		TPE v = bp[j + X];					\
		computed = is_##TPE##_nil(v) ? (stdev_var_deltas) {0} : (stdev_var_deltas) {.n = 1, .mean = (dbl)v, .delta = (dbl)v}; \
	} while (0)
#define COMPUTE_LEVELN_STDEV_VARIANCE(VAL, TPE, SAMPLE, OP)		\
	do {								\
		if (VAL.n) {						\
			computed.n++;					\
			computed.delta = VAL.delta - computed.mean;	\
			computed.mean += computed.delta / computed.n;	\
			computed.m2 += computed.delta * (VAL.delta - computed.mean); \
		}							\
	} while (0)
#define FINALIZE_AGGREGATE_STDEV_VARIANCE(TPE, SAMPLE, OP)	\
	do {							\
		dbl m2 = computed.m2;				\
		BUN n = computed.n;				\
		if (isinf(m2)) {				\
			goto overflow;				\
		} else if (n > SAMPLE) {			\
			rb[k] = OP;				\
		} else {					\
			rb[k] = dbl_nil;			\
			has_nils = true;			\
		}						\
	} while (0)
#define ANALYTICAL_STDEV_VARIANCE_OTHERS(TPE, SAMPLE, OP)		\
	do {								\
		TPE *restrict bp = (TPE*)bi.base;			\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(stdev_var_deltas), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(stdev_var_deltas, ncount, INIT_AGGREGATE_STDEV_VARIANCE, COMPUTE_LEVEL0_STDEV_VARIANCE, COMPUTE_LEVELN_STDEV_VARIANCE, TPE, SAMPLE, OP); \
		for (; k < i; k++)					\
			compute_on_segment_tree(stdev_var_deltas, start[k] - j, end[k] - j, INIT_AGGREGATE_STDEV_VARIANCE, COMPUTE_LEVELN_STDEV_VARIANCE, FINALIZE_AGGREGATE_STDEV_VARIANCE, TPE, SAMPLE, OP); \
		j = k;							\
	} while (0)

#define ANALYTICAL_STATISTICS_PARTITIONS(TPE, SAMPLE, OP, IMP)	\
	do {							\
		if (p) {					\
			while (i < cnt) {			\
				if (np[i]) 	{		\
statistics##TPE##IMP:						\
					IMP(TPE, SAMPLE, OP);	\
				}				\
				if (!last)			\
					i++;			\
			}					\
		}						\
		if (!last) {					\
			last = true;				\
			i = cnt;				\
			goto statistics##TPE##IMP;		\
		}						\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_STATISTICS_LIMIT(IMP, SAMPLE, OP)			\
	case TYPE_hge:							\
		ANALYTICAL_STATISTICS_PARTITIONS(hge, SAMPLE, OP, ANALYTICAL_##IMP); \
		break;
#else
#define ANALYTICAL_STATISTICS_LIMIT(IMP, SAMPLE, OP)
#endif

#define ANALYTICAL_STATISTICS_BRANCHES(IMP, SAMPLE, OP)			\
	do {								\
		switch (tpe) {						\
		case TYPE_bte:						\
			ANALYTICAL_STATISTICS_PARTITIONS(bte, SAMPLE, OP, ANALYTICAL_##IMP); \
			break;						\
		case TYPE_sht:						\
			ANALYTICAL_STATISTICS_PARTITIONS(sht, SAMPLE, OP, ANALYTICAL_##IMP); \
			break;						\
		case TYPE_int:						\
			ANALYTICAL_STATISTICS_PARTITIONS(int, SAMPLE, OP, ANALYTICAL_##IMP); \
			break;						\
		case TYPE_lng:						\
			ANALYTICAL_STATISTICS_PARTITIONS(lng, SAMPLE, OP, ANALYTICAL_##IMP); \
			break;						\
		case TYPE_flt:						\
			ANALYTICAL_STATISTICS_PARTITIONS(flt, SAMPLE, OP, ANALYTICAL_##IMP); \
			break;						\
		case TYPE_dbl:						\
			ANALYTICAL_STATISTICS_PARTITIONS(dbl, SAMPLE, OP, ANALYTICAL_##IMP); \
			break;						\
		ANALYTICAL_STATISTICS_LIMIT(IMP, SAMPLE, OP)		\
		default:						\
			goto nosupport;					\
		}							\
	} while (0)

#define GDK_ANALYTICAL_STDEV_VARIANCE(NAME, SAMPLE, OP, DESC)		\
gdk_return								\
GDKanalytical_##NAME(BAT *r, BAT *p, BAT *o, BAT *b, BAT *s, BAT *e, int tpe, int frame_type) \
{									\
	BATiter pi = bat_iterator(p);					\
	BATiter oi = bat_iterator(o);					\
	BATiter bi = bat_iterator(b);					\
	BATiter si = bat_iterator(s);					\
	BATiter ei = bat_iterator(e);					\
	bool has_nils = false, last = false;				\
	oid i = 0, j = 0, k = 0, l = 0, cnt = BATcount(b), *restrict start = si.base, *restrict end = ei.base, \
		*levels_offset = NULL, nlevels = 0;			\
	lng n = 0;							\
	bit *np = pi.base, *op = oi.base;				\
	dbl *rb = (dbl *) Tloc(r, 0), mean = 0, m2 = 0, delta;		\
	void *segment_tree = NULL;					\
	gdk_return res = GDK_SUCCEED;					\
	BAT *st = NULL;							\
									\
	if (cnt > 0) {							\
		switch (frame_type) {					\
		case 3: /* unbounded until current row */	{	\
			ANALYTICAL_STATISTICS_BRANCHES(STDEV_VARIANCE_UNBOUNDED_TILL_CURRENT_ROW, SAMPLE, OP); \
		} break;						\
		case 4: /* current row until unbounded */	{	\
			ANALYTICAL_STATISTICS_BRANCHES(STDEV_VARIANCE_CURRENT_ROW_TILL_UNBOUNDED, SAMPLE, OP); \
		} break;						\
		case 5: /* all rows */	{				\
			ANALYTICAL_STATISTICS_BRANCHES(STDEV_VARIANCE_ALL_ROWS, SAMPLE, OP); \
		} break;						\
		case 6: /* current row */ {				\
			ANALYTICAL_STATISTICS_BRANCHES(STDEV_VARIANCE_CURRENT_ROW, SAMPLE, OP);	\
		} break;						\
		default: {						\
			if (!(st = GDKinitialize_segment_tree())) {	\
				res = GDK_FAIL;				\
				goto cleanup;				\
			}						\
			ANALYTICAL_STATISTICS_BRANCHES(STDEV_VARIANCE_OTHERS, SAMPLE, OP); \
		}							\
		}							\
	}								\
									\
	BATsetcount(r, cnt);						\
	r->tnonil = !has_nils;						\
	r->tnil = has_nils;						\
	goto cleanup; /* all these gotos seem confusing but it cleans up the ending of the operator */ \
overflow:								\
	GDKerror("22003!overflow in calculation.\n");			\
	res = GDK_FAIL;							\
cleanup:								\
	bat_iterator_end(&pi);						\
	bat_iterator_end(&oi);						\
	bat_iterator_end(&bi);						\
	bat_iterator_end(&si);						\
	bat_iterator_end(&ei);						\
	BBPreclaim(st);							\
	return res;							\
nosupport:								\
	GDKerror("42000!%s of type %s unsupported.\n", DESC, ATOMname(tpe)); \
	res = GDK_FAIL;							\
	goto cleanup;							\
}

GDK_ANALYTICAL_STDEV_VARIANCE(stddev_samp, 1, sqrt(m2 / (n - 1)), "standard deviation")
GDK_ANALYTICAL_STDEV_VARIANCE(stddev_pop, 0, sqrt(m2 / n), "standard deviation")
GDK_ANALYTICAL_STDEV_VARIANCE(variance_samp, 1, m2 / (n - 1), "variance")
GDK_ANALYTICAL_STDEV_VARIANCE(variance_pop, 0, m2 / n, "variance")

#define ANALYTICAL_COVARIANCE_UNBOUNDED_TILL_CURRENT_ROW(TPE, SAMPLE, OP) \
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		for (; k < i;) {					\
			j = k;						\
			do {						\
				TPE v1 = bp1[k], v2 = bp2[k];		\
				if (!is_##TPE##_nil(v1) && !is_##TPE##_nil(v2))	{ \
					n++;				\
					delta1 = (dbl) v1 - mean1;	\
					mean1 += delta1 / n;		\
					delta2 = (dbl) v2 - mean2;	\
					mean2 += delta2 / n;		\
					m2 += delta1 * ((dbl) v2 - mean2); \
				}					\
				k++;					\
			} while (k < i && !op[k]);			\
			if (isinf(m2))					\
				goto overflow;				\
			if (n > SAMPLE) {				\
				for (; j < k; j++)			\
					rb[j] = OP;			\
			} else {					\
				for (; j < k; j++)			\
					rb[j] = dbl_nil;		\
				has_nils = true;			\
			}						\
		}							\
		n = 0;							\
		mean1 = 0;						\
		mean2 = 0;						\
		m2 = 0;							\
	} while (0)

#define ANALYTICAL_COVARIANCE_CURRENT_ROW_TILL_UNBOUNDED(TPE, SAMPLE, OP) \
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		l = i - 1;						\
		for (j = l; ; j--) {					\
			TPE v1 = bp1[j], v2 = bp2[j];			\
			if (!is_##TPE##_nil(v1) && !is_##TPE##_nil(v2))	{ \
				n++;					\
				delta1 = (dbl) v1 - mean1;		\
				mean1 += delta1 / n;			\
				delta2 = (dbl) v2 - mean2;		\
				mean2 += delta2 / n;			\
				m2 += delta1 * ((dbl) v2 - mean2);	\
			}						\
			if (op[j] || j == k) {				\
				if (isinf(m2))				\
					goto overflow;			\
				if (n > SAMPLE) {			\
					for (; ; l--) {			\
						rb[l] = OP;		\
						if (l == j)		\
							break;		\
					}				\
				} else {				\
					for (; ; l--) {			\
						rb[l] = dbl_nil;	\
						if (l == j)		\
							break;		\
					}				\
					has_nils = true;		\
				}					\
				if (j == k)				\
					break;				\
				l = j - 1;				\
			}						\
		}							\
		n = 0;							\
		mean1 = 0;						\
		mean2 = 0;						\
		m2 = 0;							\
		k = i;							\
	} while (0)

#define ANALYTICAL_COVARIANCE_ALL_ROWS(TPE, SAMPLE, OP)			\
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		for (; j < i; j++) {					\
			TPE v1 = bp1[j], v2 = bp2[j];			\
			if (!is_##TPE##_nil(v1) && !is_##TPE##_nil(v2))	{ \
				n++;					\
				delta1 = (dbl) v1 - mean1;		\
				mean1 += delta1 / n;			\
				delta2 = (dbl) v2 - mean2;		\
				mean2 += delta2 / n;			\
				m2 += delta1 * ((dbl) v2 - mean2);	\
			}						\
		}							\
		if (isinf(m2))						\
			goto overflow;					\
		if (n > SAMPLE) {					\
			for (; k < i; k++)				\
				rb[k] = OP;				\
		} else {						\
			for (; k < i; k++)				\
				rb[k] = dbl_nil;			\
			has_nils = true;				\
		}							\
		n = 0;							\
		mean1 = 0;						\
		mean2 = 0;						\
		m2 = 0;							\
	} while (0)

#define ANALYTICAL_COVARIANCE_CURRENT_ROW(TPE, SAMPLE, OP)	\
	do {							\
		for (; k < i; k++)				\
			rb[k] = SAMPLE == 1 ? dbl_nil : 0;	\
		has_nils = is_dbl_nil(rb[k - 1]);		\
	} while (0)

typedef struct covariance_deltas {
	BUN n;
	dbl mean1, mean2, delta1, delta2, m2;
} covariance_deltas;

#define INIT_AGGREGATE_COVARIANCE(TPE, SAMPLE, OP)	\
	do {						\
		computed = (covariance_deltas) {0};	\
	} while (0)
#define COMPUTE_LEVEL0_COVARIANCE(X, TPE, SAMPLE, OP)			\
	do {								\
		TPE v1 = bp1[j + X], v2 = bp2[j + X];			\
		computed = is_##TPE##_nil(v1) || is_##TPE##_nil(v2) ? (covariance_deltas) {0} \
															: (covariance_deltas) {.n = 1, .mean1 = (dbl)v1, .mean2 = (dbl)v2, .delta1 = (dbl)v1, .delta2 = (dbl)v2}; \
	} while (0)
#define COMPUTE_LEVELN_COVARIANCE(VAL, TPE, SAMPLE, OP)			\
	do {								\
		if (VAL.n) {						\
			computed.n++;					\
			computed.delta1 = VAL.delta1 - computed.mean1;	\
			computed.mean1 += computed.delta1 / computed.n;	\
			computed.delta2 = VAL.delta2 - computed.mean2;	\
			computed.mean2 += computed.delta2 / computed.n;	\
			computed.m2 += computed.delta1 * (VAL.delta2 - computed.mean2);	\
		}							\
	} while (0)
#define FINALIZE_AGGREGATE_COVARIANCE(TPE, SAMPLE, OP) FINALIZE_AGGREGATE_STDEV_VARIANCE(TPE, SAMPLE, OP)
#define ANALYTICAL_COVARIANCE_OTHERS(TPE, SAMPLE, OP)			\
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(covariance_deltas), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(covariance_deltas, ncount, INIT_AGGREGATE_COVARIANCE, COMPUTE_LEVEL0_COVARIANCE, COMPUTE_LEVELN_COVARIANCE, TPE, SAMPLE, OP); \
		for (; k < i; k++)					\
			compute_on_segment_tree(covariance_deltas, start[k] - j, end[k] - j, INIT_AGGREGATE_COVARIANCE, COMPUTE_LEVELN_COVARIANCE, FINALIZE_AGGREGATE_COVARIANCE, TPE, SAMPLE, OP); \
		j = k;							\
	} while (0)

#define GDK_ANALYTICAL_COVARIANCE(NAME, SAMPLE, OP)			\
gdk_return								\
GDKanalytical_##NAME(BAT *r, BAT *p, BAT *o, BAT *b1, BAT *b2, BAT *s, BAT *e, int tpe, int frame_type) \
{									\
	BATiter pi = bat_iterator(p);					\
	BATiter oi = bat_iterator(o);					\
	BATiter b1i = bat_iterator(b1);					\
	BATiter b2i = bat_iterator(b2);					\
	BATiter si = bat_iterator(s);					\
	BATiter ei = bat_iterator(e);					\
	bool has_nils = false, last = false;				\
	oid i = 0, j = 0, k = 0, l = 0, cnt = BATcount(b1), *restrict start = si.base, *restrict end = ei.base,	\
		*levels_offset = NULL, nlevels = 0;			\
	lng n = 0;							\
	bit *np = pi.base, *op = oi.base;				\
	dbl *rb = (dbl *) Tloc(r, 0), mean1 = 0, mean2 = 0, m2 = 0, delta1, delta2; \
	void *segment_tree = NULL;					\
	gdk_return res = GDK_SUCCEED;					\
	BAT *st = NULL;							\
									\
	if (cnt > 0) {							\
		switch (frame_type) {					\
		case 3: /* unbounded until current row */	{	\
			ANALYTICAL_STATISTICS_BRANCHES(COVARIANCE_UNBOUNDED_TILL_CURRENT_ROW, SAMPLE, OP); \
		} break;						\
		case 4: /* current row until unbounded */	{	\
			ANALYTICAL_STATISTICS_BRANCHES(COVARIANCE_CURRENT_ROW_TILL_UNBOUNDED, SAMPLE, OP); \
		} break;						\
		case 5: /* all rows */	{				\
			ANALYTICAL_STATISTICS_BRANCHES(COVARIANCE_ALL_ROWS, SAMPLE, OP); \
		} break;						\
		case 6: /* current row */ {				\
			ANALYTICAL_STATISTICS_BRANCHES(COVARIANCE_CURRENT_ROW, SAMPLE, OP); \
		} break;						\
		default: {						\
			if (!(st = GDKinitialize_segment_tree())) {	\
				res = GDK_FAIL;				\
				goto cleanup;				\
			}						\
			ANALYTICAL_STATISTICS_BRANCHES(COVARIANCE_OTHERS, SAMPLE, OP); \
		}							\
		}							\
	}								\
									\
	BATsetcount(r, cnt);						\
	r->tnonil = !has_nils;						\
	r->tnil = has_nils;						\
	goto cleanup; /* all these gotos seem confusing but it cleans up the ending of the operator */ \
overflow:								\
	GDKerror("22003!overflow in calculation.\n");			\
	res = GDK_FAIL;							\
cleanup:								\
	bat_iterator_end(&pi);						\
	bat_iterator_end(&oi);						\
	bat_iterator_end(&b1i);						\
	bat_iterator_end(&b2i);						\
	bat_iterator_end(&si);						\
	bat_iterator_end(&ei);						\
	BBPreclaim(st);							\
	return res;							\
nosupport:								\
	GDKerror("42000!covariance of type %s unsupported.\n", ATOMname(tpe)); \
	res = GDK_FAIL;							\
	goto cleanup;							\
}

GDK_ANALYTICAL_COVARIANCE(covariance_samp, 1, m2 / (n - 1))
GDK_ANALYTICAL_COVARIANCE(covariance_pop, 0, m2 / n)

#define ANALYTICAL_CORRELATION_UNBOUNDED_TILL_CURRENT_ROW(TPE, SAMPLE, OP)	/* SAMPLE and OP not used */ \
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		for (; k < i;) {					\
			j = k;						\
			do {						\
				TPE v1 = bp1[k], v2 = bp2[k];		\
				if (!is_##TPE##_nil(v1) && !is_##TPE##_nil(v2))	{ \
					n++;				\
					delta1 = (dbl) v1 - mean1;	\
					mean1 += delta1 / n;		\
					delta2 = (dbl) v2 - mean2;	\
					mean2 += delta2 / n;		\
					aux = (dbl) v2 - mean2;		\
					up += delta1 * aux;		\
					down1 += delta1 * ((dbl) v1 - mean1); \
					down2 += delta2 * aux;		\
				}					\
				k++;					\
			} while (k < i && !op[k]);			\
			if (isinf(up) || isinf(down1) || isinf(down2))	\
				goto overflow;				\
			if (n != 0 && down1 != 0 && down2 != 0) {	\
				rr = (up / n) / (sqrt(down1 / n) * sqrt(down2 / n)); \
				assert(!is_dbl_nil(rr));		\
			} else {					\
				rr = dbl_nil;				\
				has_nils = true;			\
			}						\
			for (; j < k; j++)				\
				rb[j] = rr;				\
		}							\
		n = 0;							\
		mean1 = 0;						\
		mean2 = 0;						\
		up = 0;							\
		down1 = 0;						\
		down2 = 0;						\
	} while (0)

#define ANALYTICAL_CORRELATION_CURRENT_ROW_TILL_UNBOUNDED(TPE, SAMPLE, OP)	/* SAMPLE and OP not used */ \
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		l = i - 1;						\
		for (j = l; ; j--) {					\
			TPE v1 = bp1[j], v2 = bp2[j];			\
			if (!is_##TPE##_nil(v1) && !is_##TPE##_nil(v2))	{ \
				n++;					\
				delta1 = (dbl) v1 - mean1;		\
				mean1 += delta1 / n;			\
				delta2 = (dbl) v2 - mean2;		\
				mean2 += delta2 / n;			\
				aux = (dbl) v2 - mean2;			\
				up += delta1 * aux;			\
				down1 += delta1 * ((dbl) v1 - mean1);	\
				down2 += delta2 * aux;			\
			}						\
			if (op[j] || j == k) {				\
				if (isinf(up) || isinf(down1) || isinf(down2)) \
					goto overflow;			\
				if (n != 0 && down1 != 0 && down2 != 0) { \
					rr = (up / n) / (sqrt(down1 / n) * sqrt(down2 / n)); \
					assert(!is_dbl_nil(rr));	\
				} else {				\
					rr = dbl_nil;			\
					has_nils = true;		\
				}					\
				for (; ; l--) {				\
					rb[l] = rr;			\
					if (l == j)			\
						break;			\
				}					\
				if (j == k)				\
					break;				\
				l = j - 1;				\
			}						\
		}							\
		n = 0;							\
		mean1 = 0;						\
		mean2 = 0;						\
		up = 0;							\
		down1 = 0;						\
		down2 = 0;						\
		k = i;							\
	} while (0)

#define ANALYTICAL_CORRELATION_ALL_ROWS(TPE, SAMPLE, OP)	/* SAMPLE and OP not used */ \
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		for (; j < i; j++) {					\
			TPE v1 = bp1[j], v2 = bp2[j];			\
			if (!is_##TPE##_nil(v1) && !is_##TPE##_nil(v2))	{ \
				n++;					\
				delta1 = (dbl) v1 - mean1;		\
				mean1 += delta1 / n;			\
				delta2 = (dbl) v2 - mean2;		\
				mean2 += delta2 / n;			\
				aux = (dbl) v2 - mean2;			\
				up += delta1 * aux;			\
				down1 += delta1 * ((dbl) v1 - mean1);	\
				down2 += delta2 * aux;			\
			}						\
		}							\
		if (isinf(up) || isinf(down1) || isinf(down2))		\
			goto overflow;					\
		if (n != 0 && down1 != 0 && down2 != 0) {		\
			rr = (up / n) / (sqrt(down1 / n) * sqrt(down2 / n)); \
			assert(!is_dbl_nil(rr));			\
		} else {						\
			rr = dbl_nil;					\
			has_nils = true;				\
		}							\
		for (; k < i ; k++)					\
			rb[k] = rr;					\
		n = 0;							\
		mean1 = 0;						\
		mean2 = 0;						\
		up = 0;							\
		down1 = 0;						\
		down2 = 0;						\
	} while (0)

#define ANALYTICAL_CORRELATION_CURRENT_ROW(TPE, SAMPLE, OP)	/* SAMPLE and OP not used */ \
	do {								\
		for (; k < i; k++)					\
			rb[k] = dbl_nil;				\
		has_nils = true;					\
	} while (0)


typedef struct correlation_deltas {
	BUN n;
	dbl mean1, mean2, delta1, delta2, up, down1, down2;
} correlation_deltas;

#define INIT_AGGREGATE_CORRELATION(TPE, SAMPLE, OP)	\
	do {						\
		computed = (correlation_deltas) {0};	\
	} while (0)
#define COMPUTE_LEVEL0_CORRELATION(X, TPE, SAMPLE, OP)			\
	do {								\
		TPE v1 = bp1[j + X], v2 = bp2[j + X];			\
		computed = is_##TPE##_nil(v1) || is_##TPE##_nil(v2) ? (correlation_deltas) {0} \
															: (correlation_deltas) {.n = 1, .mean1 = (dbl)v1, .mean2 = (dbl)v2, .delta1 = (dbl)v1, .delta2 = (dbl)v2}; \
	} while (0)
#define COMPUTE_LEVELN_CORRELATION(VAL, TPE, SAMPLE, OP)		\
	do {								\
		if (VAL.n) {						\
			computed.n++;					\
			computed.delta1 = VAL.delta1 - computed.mean1;	\
			computed.mean1 += computed.delta1 / computed.n;	\
			computed.delta2 = VAL.delta2 - computed.mean2;	\
			computed.mean2 += computed.delta2 / computed.n;	\
			dbl aux = VAL.delta2 - computed.mean2;		\
			computed.up += computed.delta1 * aux;		\
			computed.down1 += computed.delta1 * (VAL.delta1 - computed.mean1); \
			computed.down2 += computed.delta2 * aux;	\
		}							\
	} while (0)
#define FINALIZE_AGGREGATE_CORRELATION(TPE, SAMPLE, OP)			\
	do {								\
		n = computed.n;						\
		up = computed.up;					\
		down1 = computed.down1;					\
		down2 = computed.down2;					\
		if (isinf(up) || isinf(down1) || isinf(down2))		\
			goto overflow;					\
		if (n != 0 && down1 != 0 && down2 != 0) {		\
			rr = (up / n) / (sqrt(down1 / n) * sqrt(down2 / n)); \
			assert(!is_dbl_nil(rr));			\
		} else {						\
			rr = dbl_nil;					\
			has_nils = true;				\
		}							\
		rb[k] = rr;						\
	} while (0)
#define ANALYTICAL_CORRELATION_OTHERS(TPE, SAMPLE, OP) 	/* SAMPLE and OP not used */ \
	do {								\
		TPE *bp1 = (TPE*)b1i.base, *bp2 = (TPE*)b2i.base;	\
		oid ncount = i - k;					\
		if ((res = GDKrebuild_segment_tree(ncount, sizeof(correlation_deltas), st, &segment_tree, &levels_offset, &nlevels)) != GDK_SUCCEED) \
			goto cleanup;					\
		populate_segment_tree(correlation_deltas, ncount, INIT_AGGREGATE_CORRELATION, COMPUTE_LEVEL0_CORRELATION, COMPUTE_LEVELN_CORRELATION, TPE, SAMPLE, OP); \
		for (; k < i; k++)					\
			compute_on_segment_tree(correlation_deltas, start[k] - j, end[k] - j, INIT_AGGREGATE_CORRELATION, COMPUTE_LEVELN_CORRELATION, FINALIZE_AGGREGATE_CORRELATION, TPE, SAMPLE, OP); \
		j = k;							\
	} while (0)

gdk_return
GDKanalytical_correlation(BAT *r, BAT *p, BAT *o, BAT *b1, BAT *b2, BAT *s, BAT *e, int tpe, int frame_type)
{
	bool has_nils = false, last = false;
	BATiter pi = bat_iterator(p);
	BATiter oi = bat_iterator(o);
	BATiter b1i = bat_iterator(b1);
	BATiter b2i = bat_iterator(b2);
	BATiter si = bat_iterator(s);
	BATiter ei = bat_iterator(e);
	oid i = 0, j = 0, k = 0, l = 0, cnt = BATcount(b1),
		*levels_offset = NULL, nlevels = 0;
	const oid *restrict start = si.base, *restrict end = ei.base;
	lng n = 0;
	const bit *np = pi.base, *op = oi.base;
	dbl *rb = (dbl *) Tloc(r, 0), mean1 = 0, mean2 = 0, up = 0, down1 = 0, down2 = 0, delta1, delta2, aux, rr;
	void *segment_tree = NULL;
	gdk_return res = GDK_SUCCEED;
	BAT *st = NULL;

	if (cnt > 0) {
		switch (frame_type) {
		case 3: /* unbounded until current row */	{
			ANALYTICAL_STATISTICS_BRANCHES(CORRELATION_UNBOUNDED_TILL_CURRENT_ROW, ;, ;);
		} break;
		case 4: /* current row until unbounded */	{
			ANALYTICAL_STATISTICS_BRANCHES(CORRELATION_CURRENT_ROW_TILL_UNBOUNDED, ;, ;);
		} break;
		case 5: /* all rows */	{
			ANALYTICAL_STATISTICS_BRANCHES(CORRELATION_ALL_ROWS, ;, ;);
		} break;
		case 6: /* current row */ {
			ANALYTICAL_STATISTICS_BRANCHES(CORRELATION_CURRENT_ROW, ;, ;);
		} break;
		default: {
			if (!(st = GDKinitialize_segment_tree())) {
				res = GDK_FAIL;
				goto cleanup;
			}
			ANALYTICAL_STATISTICS_BRANCHES(CORRELATION_OTHERS, ;, ;);
		}
		}
	}

	BATsetcount(r, cnt);
	r->tnonil = !has_nils;
	r->tnil = has_nils;
	goto cleanup; /* all these gotos seem confusing but it cleans up the ending of the operator */
overflow:
	GDKerror("22003!overflow in calculation.\n");
	res = GDK_FAIL;
cleanup:
	bat_iterator_end(&pi);
	bat_iterator_end(&oi);
	bat_iterator_end(&b1i);
	bat_iterator_end(&b2i);
	bat_iterator_end(&si);
	bat_iterator_end(&ei);
	BBPreclaim(st);
	return res;
  nosupport:
	GDKerror("42000!correlation of type %s unsupported.\n", ATOMname(tpe));
	res = GDK_FAIL;
	goto cleanup;
}
