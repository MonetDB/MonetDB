/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_analytic.h"
#include "gdk_time.h"
#include "gdk_calc_private.h"

#define ANALYTICAL_DIFF_IMP(TPE)				\
	do {							\
		TPE *restrict bp = (TPE*)Tloc(b, 0), prev = bp[0];		\
		if (np) {					\
			for (; i < cnt; i++) {	\
				TPE next = bp[i]; \
				if (next != prev) {		\
					rb[i] = TRUE;		\
					prev = next;		\
				} else {	\
					rb[i] = np[i];			\
				}			\
			}					\
		} else if (npbit) {					\
			for (; i < cnt; i++) {	\
				TPE next = bp[i]; \
				if (next != prev) {		\
					rb[i] = TRUE;		\
					prev = next;		\
				} else {	\
					rb[i] = npb;			\
				}			\
			}					\
		} else {					\
			for (; i < cnt; i++) {		\
				TPE next = bp[i]; \
				if (next == prev) {		\
					rb[i] = FALSE;		\
				} else {			\
					rb[i] = TRUE;		\
					prev = next;		\
				}				\
			}				\
		}				\
	} while (0)

/* We use NaN for floating point null values, which always output false on equality tests */
#define ANALYTICAL_DIFF_FLOAT_IMP(TPE)					\
	do {								\
		TPE *restrict bp = (TPE*)Tloc(b, 0), prev = bp[0];		\
		if (np) {						\
			for (; i < cnt; i++) {		\
				TPE next = bp[i]; \
				if (next != prev && (!is_##TPE##_nil(next) || !is_##TPE##_nil(prev))) { \
					rb[i] = TRUE;			\
					prev = next;			\
				} else {	\
					rb[i] = np[i];			\
				}			\
			}						\
		} else if (npbit) {						\
			for (; i < cnt; i++) {		\
				TPE next = bp[i]; \
				if (next != prev && (!is_##TPE##_nil(next) || !is_##TPE##_nil(prev))) { \
					rb[i] = TRUE;			\
					prev = next;			\
				} else {	\
					rb[i] = npb;			\
				}			\
			}						\
		} else {						\
			for (; i < cnt; i++) {			\
				TPE next = bp[i]; \
				if (next == prev || (is_##TPE##_nil(next) && is_##TPE##_nil(prev))) { \
					rb[i] = FALSE;			\
				} else {				\
					rb[i] = TRUE;			\
					prev = next;		\
				}				\
			}				\
		}				\
	} while (0)

gdk_return
GDKanalyticaldiff(BAT *r, BAT *b, BAT *p, const bit *restrict npbit, int tpe)
{
	BUN i = 0, cnt = BATcount(b);
	bit *restrict rb = (bit *) Tloc(r, 0), *restrict np = p ? (bit *) Tloc(p, 0) : NULL, npb = npbit ? *npbit : 0;

	switch (ATOMbasetype(tpe)) {
	case TYPE_bte:
		ANALYTICAL_DIFF_IMP(bte);
		break;
	case TYPE_sht:
		ANALYTICAL_DIFF_IMP(sht);
		break;
	case TYPE_int:
		ANALYTICAL_DIFF_IMP(int);
		break;
	case TYPE_lng:
		ANALYTICAL_DIFF_IMP(lng);
		break;
#ifdef HAVE_HGE
	case TYPE_hge:
		ANALYTICAL_DIFF_IMP(hge);
		break;
#endif
	case TYPE_flt: {
		if (b->tnonil) {
			ANALYTICAL_DIFF_IMP(flt);
		} else { /* Because of NaN values, use this path */
			ANALYTICAL_DIFF_FLOAT_IMP(flt);
		}
	} break;
	case TYPE_dbl: {
		if (b->tnonil) {
			ANALYTICAL_DIFF_IMP(dbl);
		} else { /* Because of NaN values, use this path */
			ANALYTICAL_DIFF_FLOAT_IMP(dbl);
		}
	} break;
	default:{
		BATiter it = bat_iterator(b);
		ptr v = BUNtail(it, 0), next;
		int (*atomcmp) (const void *, const void *) = ATOMcompare(tpe);
		if (np) {
			for (i = 0; i < cnt; i++) {
				rb[i] = np[i];
				next = BUNtail(it, i);
				if (atomcmp(v, next) != 0) {
					rb[i] = TRUE;
					v = next;
				}
			}
		} else if (npbit) {
			for (i = 0; i < cnt; i++) {
				rb[i] = npb;
				next = BUNtail(it, i);
				if (atomcmp(v, next) != 0) {
					rb[i] = TRUE;
					v = next;
				}
			}
		} else {
			for (i = 0; i < cnt; i++) {
				next = BUNtail(it, i);
				if (atomcmp(v, next) != 0) {
					rb[i] = TRUE;
					v = next;
				} else {
					rb[i] = FALSE;
				}
			}
		}
	}
	}
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return GDK_SUCCEED;
}

#define ANALYTICAL_WINDOW_BOUNDS_ROWS_PRECEDING(TPE, LIMIT, UPCAST)			\
	do {								\
		j = k;							\
		for (; k < i; k++) {				\
			TPE olimit = LIMIT;	\
			if (is_##TPE##_nil(olimit) || olimit < 0)	\
				goto invalid_bound;	\
			oid rlimit = UPCAST;	\
			rb[k] = rlimit > k - j ? j : k - rlimit + second_half; \
		}							\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_ROWS_FOLLOWING(TPE, LIMIT, UPCAST)			\
	do {								\
		for (; k < i; k++) {				\
			TPE olimit = LIMIT;	\
			if (is_##TPE##_nil(olimit) || olimit < 0)	\
				goto invalid_bound;	\
			oid rlimit = UPCAST + second_half; \
			rb[k] = rlimit > i - k ? i : k + rlimit; \
		}							\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(IMP, CARD, TPE, LIMIT, UPCAST)	\
	do {								\
		if (p) {						\
			for (; i < cnt; i++) {			\
				if (np[i]) 	{		\
rows##TPE##IMP##CARD: \
					ANALYTICAL_WINDOW_BOUNDS_ROWS##IMP(TPE, LIMIT, UPCAST);	\
				} \
			}						\
		} 		\
		if (!last) { \
			last = true; \
			i = cnt; \
			goto rows##TPE##IMP##CARD; \
		} \
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_GROUPS_PRECEDING(TPE, LIMIT, UPCAST)			\
	do {								\
		oid m = k;						\
		for (; k < i; k++) {		\
			TPE olimit = LIMIT;	\
			if (is_##TPE##_nil(olimit) || olimit < 0)	\
				goto invalid_bound;	\
			oid rlimit = UPCAST;	\
			for (j = k; ; j--) {		\
				if (bp[j]) {		\
					if (rlimit == 0)		\
						break;		\
					rlimit--;		\
				}				\
				if (j == m)		\
					break;		\
			}				\
			rb[k] = j;		\
		}					\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_GROUPS_FOLLOWING(TPE, LIMIT, UPCAST)			\
	do {								\
		for (; k < i; k++) {		\
			TPE olimit = LIMIT;	\
			if (is_##TPE##_nil(olimit) || olimit < 0)	\
				goto invalid_bound;	\
			oid rlimit = UPCAST;	\
			for (j = k + 1; j < i; j++) {	\
				if (bp[j]) {		\
					if (rlimit == 0)		\
						break;		\
					rlimit--;		\
				}		\
			}		\
			rb[k] = j;		\
		}		\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(IMP, CARD, TPE, LIMIT, UPCAST)	\
	do {								\
		if (p) {						\
			for (; i < cnt; i++) {			\
				if (np[i]) 	{		\
groups##TPE##IMP##CARD: \
					ANALYTICAL_WINDOW_BOUNDS_GROUPS##IMP(TPE, LIMIT, UPCAST);	\
				} \
			}						\
		}				\
		if (!last) { \
			last = true; \
			i = cnt; \
			goto groups##TPE##IMP##CARD; \
		} \
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_RANGE_PRECEDING(TPE1, LIMIT, TPE2) \
	do {								\
		oid m = k;				\
		TPE1 v, calc;						\
		if (b->tnonil) {					\
			for (; k < i; k++) {			\
				TPE2 olimit = LIMIT;	\
				if (is_##TPE2##_nil(olimit) || olimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];				\
				for (j = k; ; j--) {			\
					SUB_WITH_CHECK(v, bp[j], TPE1, calc, GDK_##TPE1##_max, goto calc_overflow); \
					if (ABSOLUTE(calc) > olimit) { \
						j++; \
						break;			\
					} \
					if (j == m)			\
						break;			\
				}					\
				rb[k] = j;	\
			}						\
		} else {						\
			for (; k < i; k++) {			\
				TPE2 olimit = LIMIT;	\
				if (is_##TPE2##_nil(olimit) || olimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];				\
				if (is_##TPE1##_nil(v)) {		\
					for (j = k; ; j--) {		\
						if (!is_##TPE1##_nil(bp[j])) {	\
							j++;	\
							break;			\
						}	\
						if (j == m)			\
							break;			\
					}			\
					rb[k] = j;	\
				} else {				\
					for (j = k; ; j--) {		\
						if (is_##TPE1##_nil(bp[j])) { \
							j++;	\
							break;		\
						}	\
						SUB_WITH_CHECK(v, bp[j], TPE1, calc, GDK_##TPE1##_max, goto calc_overflow); \
						if (ABSOLUTE(calc) > olimit) { \
							j++; \
							break;			\
						} \
						if (j == m)	\
							break;			\
					}				\
					rb[k] = j;	\
				}					\
			}						\
		}							\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_RANGE_FOLLOWING(TPE1, LIMIT, TPE2) \
	do {								\
		TPE1 v, calc;						\
		if (b->tnonil) {					\
			for (; k < i; k++) {			\
				TPE2 olimit = LIMIT;	\
				if (is_##TPE2##_nil(olimit) || olimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];				\
				for (j = k + 1; j < i; j++) {		\
					SUB_WITH_CHECK(v, bp[j], TPE1, calc, GDK_##TPE1##_max, goto calc_overflow); \
					if (ABSOLUTE(calc) > olimit) \
						break;			\
				}					\
				rb[k] = j;				\
			}						\
		} else {						\
			for (; k < i; k++) {			\
				TPE2 olimit = LIMIT;	\
				if (is_##TPE2##_nil(olimit) || olimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];				\
				if (is_##TPE1##_nil(v)) {		\
					for (j =k + 1; j < i; j++) {	\
						if (!is_##TPE1##_nil(bp[j])) \
							break;		\
					}				\
				} else {				\
					for (j = k + 1; j < i; j++) {	\
						if (is_##TPE1##_nil(bp[j])) \
							break;		\
						SUB_WITH_CHECK(v, bp[j], TPE1, calc, GDK_##TPE1##_max, goto calc_overflow); \
						if (ABSOLUTE(calc) > olimit) \
							break;		\
					}				\
				}					\
				rb[k] = j;				\
			}						\
		}							\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(TPE1, IMP, CARD, LIMIT, TPE2)	\
	do {								\
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0);			\
		if (np) {						\
			for (; i < cnt; i++) {			\
				if (np[i]) 	{	\
range##TPE1##TPE2##IMP##CARD: \
					IMP(TPE1, LIMIT, TPE2);		\
				} \
			}						\
		} 	\
		if (!last) { \
			last = true; \
			i = cnt; \
			goto range##TPE1##TPE2##IMP##CARD; \
		} \
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(IMP, CARD, LIMIT, TPE2)	\
	do {								\
		switch (tp1) {						\
		case TYPE_bte:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(bte, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, TPE2); \
			break;						\
		case TYPE_sht:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(sht, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, TPE2); \
			break;						\
		case TYPE_int:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(int, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, TPE2); \
			break;						\
		case TYPE_lng:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(lng, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, TPE2); \
			break;						\
		default:						\
			goto type_not_supported;	\
		}							\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(IMP, CARD, LIMIT)		\
	do {								\
		switch (tp1) {						\
			case TYPE_flt:					\
				ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(flt, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, flt); \
				break;					\
			default:					\
				goto type_not_supported;		\
		}							\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(IMP, CARD, LIMIT)		\
	do {								\
		switch (tp1) {						\
			case TYPE_dbl:					\
				ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(dbl, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, dbl); \
				break;					\
			default:					\
				goto type_not_supported;		\
		}							\
	} while (0)

#ifdef HAVE_HGE
#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(IMP, CARD, LIMIT)		\
	do {								\
		switch (tp1) {						\
		case TYPE_bte:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(bte, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, hge); \
			break;						\
		case TYPE_sht:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(sht, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, hge); \
			break;						\
		case TYPE_int:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(int, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, hge); \
			break;						\
		case TYPE_lng:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(lng, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, hge); \
			break;						\
		case TYPE_hge:						\
			ANALYTICAL_WINDOW_BOUNDS_CALC_NUM(hge, ANALYTICAL_WINDOW_BOUNDS_RANGE##IMP, CARD, LIMIT, hge); \
			break;						\
		default:						\
			goto type_not_supported;	\
		}							\
	} while (0)
#endif

#define date_sub_month(D,M)			date_add_month(D,-(M))
#define timestamp_sub_month(T,M)	timestamp_add_month(T,-(M))

#define daytime_add_msec(D,M)		daytime_add_usec(D, 1000*(M))
#define daytime_sub_msec(D,M)		daytime_add_usec(D, -1000*(M))
#define date_add_msec(D,M)			date_add_day(D,(int) ((M)/(24*60*60*1000)))
#define date_sub_msec(D,M)			date_add_day(D,(int) (-(M)/(24*60*60*1000)))
#define timestamp_add_msec(T,M)		timestamp_add_usec(T, (M)*1000)
#define timestamp_sub_msec(T,M)		timestamp_add_usec(T, -(M)*1000)

#define ANALYTICAL_WINDOW_BOUNDS_RANGE_MTIME_PRECEDING(TPE1, LIMIT, TPE2, SUB, ADD) \
	do {																\
		oid m = k;									\
		TPE1 v, vmin, vmax;												\
		if (b->tnonil) {												\
			for (; k < i; k++) {									\
				TPE2 rlimit = LIMIT;	\
				if (is_##TPE1##_nil(rlimit) || rlimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];												\
				vmin = SUB(v, rlimit);									\
				vmax = ADD(v, rlimit);									\
				for (j=k; ; j--) {										\
					if ((!is_##TPE1##_nil(vmin) && bp[j] < vmin) || (!is_##TPE1##_nil(vmax) && bp[j] > vmax)) { \
						j++; \
						break;			\
					} \
					if (j == m)	\
						break;			\
				} \
				rb[k] = j;	\
			}															\
		} else {														\
			for (; k < i; k++) {									\
				TPE2 rlimit = LIMIT;	\
				if (is_##TPE1##_nil(rlimit) || rlimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];												\
				if (is_##TPE1##_nil(v)) {								\
					for (j=k; ; j--) {									\
						if (!is_##TPE1##_nil(bp[j])) { \
							j++; \
							break;			\
						} \
						if (j == m)	\
							break;			\
					} \
					rb[k] = j;	\
				} else {												\
					vmin = SUB(v, rlimit);								\
					vmax = ADD(v, rlimit);								\
					for (j=k; ; j--) {									\
						if (is_##TPE1##_nil(bp[j])) { \
							j++; \
							break;			\
						}						\
						if ((!is_##TPE1##_nil(vmin) && bp[j] < vmin) || (!is_##TPE1##_nil(vmax) && bp[j] > vmax)) {	\
							j++; 				\
							break;		\
						} \
						if (j == m)	\
							break;			\
					} \
					rb[k] = j;													\
				} 					\
			} 				\
		}															\
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_RANGE_MTIME_FOLLOWING(TPE1, LIMIT, TPE2, SUB, ADD) \
	do {																\
		TPE1 v, vmin, vmax;												\
		if (b->tnonil) {												\
			for (; k < i; k++) {									\
				TPE2 rlimit = LIMIT;	\
				if (is_##TPE1##_nil(rlimit) || rlimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];												\
				vmin = SUB(v, rlimit);									\
				vmax = ADD(v, rlimit);									\
				for (j=k+1; j<i; j++)									\
					if ((!is_##TPE1##_nil(vmin) && bp[j] < vmin) ||	(!is_##TPE1##_nil(vmax) && bp[j] > vmax))		\
						break;											\
				rb[k] = j;												\
			}															\
		} else {														\
			for (; k < i; k++) {									\
				TPE2 rlimit = LIMIT;	\
				if (is_##TPE1##_nil(rlimit) || rlimit < 0)	\
					goto invalid_bound;	\
				v = bp[k];												\
				if (is_##TPE1##_nil(v)) {								\
					for (j=k+1; j<i; j++) 								\
						if (!is_##TPE1##_nil(bp[j]))					\
							break;										\
				} else {												\
					vmin = SUB(v, rlimit);								\
					vmax = ADD(v, rlimit);								\
					for (j=k+1; j<i; j++) {								\
						if (is_##TPE1##_nil(bp[j]) || (!is_##TPE1##_nil(vmin) && bp[j] < vmin) || (!is_##TPE1##_nil(vmax) && bp[j] > vmax))	\
							break;										\
					}													\
				}														\
				rb[k] = j;												\
			}															\
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_CALC_MTIME(TPE1, IMP, CARD, LIMIT, TPE2, SUB, ADD) \
	do { \
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0); \
		if (p) {						\
			for (; i < cnt; i++) {			\
				if (np[i]) 	{		\
rangemtime##TPE1##TPE2##IMP##CARD: \
					IMP(TPE1, LIMIT, TPE2, SUB, ADD); \
				} \
			}						\
		}		\
		if (!last) { \
			last = true; \
			i = cnt; \
			goto rangemtime##TPE1##TPE2##IMP##CARD; \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(IMP, CARD, LIMIT) \
	do { \
		switch (tp1) {	\
		case TYPE_date:		\
			ANALYTICAL_WINDOW_BOUNDS_CALC_MTIME(date, ANALYTICAL_WINDOW_BOUNDS_RANGE_MTIME##IMP, CARD, LIMIT, int, date_sub_month, date_add_month); \
			break;		\
		case TYPE_timestamp:	\
			ANALYTICAL_WINDOW_BOUNDS_CALC_MTIME(timestamp, ANALYTICAL_WINDOW_BOUNDS_RANGE_MTIME##IMP, CARD, LIMIT, int, timestamp_sub_month, timestamp_add_month); \
			break;	\
		default:	\
			goto type_not_supported;	\
		}		\
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(IMP, CARD, LIMIT) \
	do { \
		switch (tp1) {	\
		case TYPE_date:		\
			ANALYTICAL_WINDOW_BOUNDS_CALC_MTIME(date, ANALYTICAL_WINDOW_BOUNDS_RANGE_MTIME##IMP, CARD, LIMIT, lng, date_sub_msec, date_add_msec); \
			break;		\
		case TYPE_daytime:		\
			ANALYTICAL_WINDOW_BOUNDS_CALC_MTIME(daytime, ANALYTICAL_WINDOW_BOUNDS_RANGE_MTIME##IMP, CARD, LIMIT, lng, daytime_sub_msec, daytime_add_msec); \
			break;		\
		case TYPE_timestamp:		\
			ANALYTICAL_WINDOW_BOUNDS_CALC_MTIME(timestamp, ANALYTICAL_WINDOW_BOUNDS_RANGE_MTIME##IMP, CARD, LIMIT, lng, timestamp_sub_msec, timestamp_add_msec); \
			break;	\
		default:		\
			goto type_not_supported;	\
		}	\
	} while(0)

static gdk_return
GDKanalyticalallbounds(BAT *r, BAT *b, BAT *p, bool preceding)
{
	oid *restrict rb = (oid *) Tloc(r, 0), i = 0, k = 0, j = 0, cnt = BATcount(b);
	bit *restrict np = p ? (bit *) Tloc(p, 0) : NULL;

	if (preceding) {
		if (np) {
			for (; i < cnt; i++) {
				if (np[i]) {
					j = k;
					for (; k < i; k++)
						rb[k] = j;
				}
			}
		}
		i = cnt;
		j = k;
		for (; k < i; k++)
			rb[k] = j;
	} else {	/* following */
		if (np) {
			for (; i < cnt; i++) {
				if (np[i]) {
					for (; k < i; k++)
						rb[k] = i;
				}
			}
		}
		i = cnt;
		for (; k < i; k++)
			rb[k] = i;
	}

	BATsetcount(r, cnt);
	r->tnonil = false;
	r->tnil = false;
	return GDK_SUCCEED;
}

#define ANALYTICAL_WINDOW_BOUNDS_PEERS_FIXED_PRECEDING(TPE, NAN_CHECK)	\
	do {								\
		TPE prev = bp[k];		\
		l = j;	\
		for (; k < i; k++) {	\
			TPE next = bp[k]; \
			if (next != prev NAN_CHECK) { 	\
				for ( ; j < k ; j++)\
					rb[j] = l; \
				l = j;	\
				prev = next; \
			}	\
		}	\
		for ( ; j < k ; j++) \
			rb[j] = l; \
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_PEERS_FIXED_FOLLOWING(TPE, NAN_CHECK)	\
	do {								\
		TPE prev = bp[k];		\
		for (; k < i; k++) {	\
			TPE next = bp[k]; \
			if (next != prev NAN_CHECK) { 	\
				l += k - j; \
				for ( ; j < k ; j++) \
					rb[j] = l;	\
				prev = next; \
			}	\
		}	\
		l += k - j; \
		for ( ; j < k ; j++) \
			rb[j] = l;	\
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(IMP, TPE, NAN_CHECK)	\
	do {								\
		TPE *restrict bp = (TPE*)Tloc(b, 0);	\
		if (p) {						\
			for (; i < cnt; i++) {			\
				if (np[i]) 	{		\
peers##TPE##IMP: \
					ANALYTICAL_WINDOW_BOUNDS_PEERS_FIXED##IMP(TPE, NAN_CHECK);	\
				} \
			}						\
		} 		\
		if (!last) { \
			last = true; \
			i = cnt; \
			goto peers##TPE##IMP; \
		} \
	} while (0)

#define NO_NAN_CHECK /* nulls match on this operator */

static gdk_return
GDKanalyticalpeers(BAT *r, BAT *b, BAT *p, bool preceding) /* used in range when the limit is 0, ie match peer rows */
{
	oid *restrict rb = (oid *) Tloc(r, 0), i = 0, k = 0, j = 0, l = 0, cnt = BATcount(b);
	bit *restrict np = p ? (bit *) Tloc(p, 0) : NULL;
	bool last = false;

	switch (ATOMbasetype(b->ttype)) {
	case TYPE_bte: {
		if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_PRECEDING, bte, NO_NAN_CHECK);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_FOLLOWING, bte, NO_NAN_CHECK);
		}
	} break;
	case TYPE_sht: {
		if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_PRECEDING, sht, NO_NAN_CHECK);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_FOLLOWING, sht, NO_NAN_CHECK);
		}
	} break;
	case TYPE_int: {
		if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_PRECEDING, int, NO_NAN_CHECK);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_FOLLOWING, int, NO_NAN_CHECK);
		}
	} break;
	case TYPE_lng: {
		if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_PRECEDING, lng, NO_NAN_CHECK);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_FOLLOWING, lng, NO_NAN_CHECK);
		}
	} break;
#ifdef HAVE_HGE
	case TYPE_hge: {
		if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_PRECEDING, hge, NO_NAN_CHECK);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_FOLLOWING, hge, NO_NAN_CHECK);
		}
	} break;
#endif
	case TYPE_flt: {
		if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_PRECEDING, flt, && (!is_flt_nil(next) || !is_flt_nil(prev)));
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_FOLLOWING, flt, && (!is_flt_nil(next) || !is_flt_nil(prev)));
		}
	} break;
	case TYPE_dbl: {
		if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_PRECEDING, dbl, && (!is_dbl_nil(next) || !is_dbl_nil(prev)));
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_PEERS(_FOLLOWING, dbl, && (!is_dbl_nil(next) || !is_dbl_nil(prev)));
		}
	} break;
	default: {
		BATiter it = bat_iterator(b);
		ptr prev, next;
		int (*atomcmp) (const void *, const void *) = ATOMcompare(b->ttype);

		if (preceding) {
			if (p) {
				for (; i < cnt; i++) {
					if (np[i]) {
						prev = BUNtail(it, k);
						l = j;
						for (; k < i; k++) {
							next = BUNtail(it, k);
							if (atomcmp(prev, next) != 0) {
								for ( ; j < k ; j++)
									rb[j] = l;
								l = j;
								prev = next;
							}
						}
						for ( ; j < k ; j++)
							rb[j] = l;
					}
				}
			}
			i = cnt;
			prev = BUNtail(it, k);
			l = j;
			for (; k < i; k++) {
				next = BUNtail(it, k);
				if (atomcmp(prev, next) != 0) {
					for ( ; j < k ; j++)
						rb[j] = l;
					l = j;
					prev = next;
				}
			}
			for ( ; j < k ; j++)
				rb[j] = l;
		} else {
			if (p) {
				for (; i < cnt; i++) {
					if (np[i]) {
						prev = BUNtail(it, k);
						for (; k < i; k++) {
							next = BUNtail(it, k);
							if (atomcmp(prev, next) != 0) {
								l += k - j;
								for ( ; j < k ; j++)
									rb[j] = l;
								prev = next;
							}
						}
						l += k - j;
						for ( ; j < k ; j++)
							rb[j] = l;
					}
				}
			}
			i = cnt;
			prev = BUNtail(it, k);
			for (; k < i; k++) {
				next = BUNtail(it, k);
				if (atomcmp(prev, next) != 0) {
					l += k - j;
					for ( ; j < k ; j++)
						rb[j] = l;
					prev = next;
				}
			}
			l += k - j;
			for ( ; j < k ; j++)
				rb[j] = l;
		}
	}
	}

	BATsetcount(r, cnt);
	r->tnonil = false;
	r->tnil = false;
	return GDK_SUCCEED;
}

static gdk_return
GDKanalyticalrowbounds(BAT *r, BAT *b, BAT *p, BAT *l, const void *restrict bound, int tp2, bool preceding, oid second_half)
{
	oid cnt = BATcount(b), nils = 0, i = 0, k = 0, j = 0;
	bit *restrict np = p ? (bit *) Tloc(p, 0) : NULL;
	oid *restrict rb = (oid *) Tloc(r, 0);
	bool last = false;

	if (l) {		/* dynamic bounds */
		if (l->tnil)
			goto invalid_bound;
		switch (tp2) {
		case TYPE_bte:{
			bte *restrict limit = (bte *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, MULTI, bte, limit[k], (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, MULTI, bte, limit[k], (oid) olimit);
			}
			break;
		}
		case TYPE_sht:{
			sht *restrict limit = (sht *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, MULTI, sht, limit[k], (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, MULTI, sht, limit[k], (oid) olimit);
			}
			break;
		}
		case TYPE_int:{
			int *restrict limit = (int *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, MULTI, int, limit[k], (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, MULTI, int, limit[k], (oid) olimit);
			}
			break;
		}
		case TYPE_lng:{
			lng *restrict limit = (lng *) Tloc(l, 0);
			if (preceding) {
#if SIZEOF_OID == SIZEOF_INT
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, MULTI, lng, limit[k], (olimit > (lng) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
#else
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, MULTI, lng, limit[k], (oid) olimit);
#endif
			} else {
#if SIZEOF_OID == SIZEOF_INT
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, MULTI, lng, limit[k], (olimit > (lng) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
#else
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, MULTI, lng, limit[k], (oid) olimit);
#endif
			}
			break;
		}
#ifdef HAVE_HGE
		case TYPE_hge:{
			hge *restrict limit = (hge *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, MULTI, hge, limit[k], (olimit > (hge) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, MULTI, hge, limit[k], (olimit > (hge) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
			}
			break;
		}
#endif
		default:
			goto bound_not_supported;
		}
	} else {	/* static bounds, all the limits are cast to the word size */
		lng limit;
		switch (tp2) {
		case TYPE_bte:
			limit = is_bte_nil(*(bte *) bound) ? lng_nil : (lng) *(bte *) bound;
			break;
		case TYPE_sht:
			limit = is_sht_nil(*(sht *) bound) ? lng_nil : (lng) *(sht *) bound;
			break;
		case TYPE_int:
			limit = is_int_nil(*(int *) bound) ? lng_nil : (lng) *(int *) bound;
			break;
		case TYPE_lng: {
#if SIZEOF_OID == SIZEOF_INT
			lng nval = *(lng *) bound;
			limit = is_lng_nil(nval) ? lng_nil : (nval > (lng) GDK_oid_max) ? GDK_lng_max : (lng) nval;
#else
			limit = (lng) (*(lng *) bound);
#endif
		} break;
#ifdef HAVE_HGE
		case TYPE_hge: {
			hge nval = *(hge *) bound;
			limit = is_hge_nil(nval) ? lng_nil : (nval > (hge) GDK_lng_max) ? GDK_lng_max : (lng) nval;
			break;
		}
#endif
		default:
			goto bound_not_supported;
		}
		if (limit == GDK_lng_max) {
			return GDKanalyticalallbounds(r, b, p, preceding);
		} else if (is_lng_nil(limit) || limit < 0) { /* this check is needed if the input is empty */
			goto invalid_bound;
		} else if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_PRECEDING, SINGLE, lng, limit, (oid) olimit);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_ROWS(_FOLLOWING, SINGLE, lng, limit, (oid) olimit);
		}
	}

	BATsetcount(r, cnt);
	r->tnonil = (nils == 0);
	r->tnil = (nils > 0);
	return GDK_SUCCEED;
      bound_not_supported:
	GDKerror("42000!rows frame bound type %s not supported.\n", ATOMname(tp2));
	return GDK_FAIL;
      invalid_bound:
	GDKerror("42000!row frame bound must be non negative and non null.\n");
	return GDK_FAIL;
}

static gdk_return
GDKanalyticalrangebounds(BAT *r, BAT *b, BAT *p, BAT *l, const void *restrict bound, int tp1, int tp2, bool preceding)
{
	oid cnt = BATcount(b), nils = 0, i = 0, k = 0, j = 0, *restrict rb = (oid *) Tloc(r, 0);
	bit *restrict np = p ? (bit *) Tloc(p, 0) : NULL;
	int abort_on_error = 1;
	bool last = false;

	if ((tp1 == TYPE_daytime || tp1 == TYPE_date || tp1 == TYPE_timestamp) && tp2 != TYPE_int && tp2 != TYPE_lng)
		goto bound_not_supported;

	if (l) {		/* dynamic bounds */
		if (l->tnil)
			goto invalid_bound;
		switch (tp2) {
		case TYPE_bte:{
			bte *restrict limit = (bte *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, MULTI, limit[k], bte);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, MULTI, limit[k], bte);
			}
			break;
		}
		case TYPE_sht:{
			sht *restrict limit = (sht *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, MULTI, limit[k], sht);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, MULTI, limit[k], sht);
			}
			break;
		}
		case TYPE_int:{
			int *restrict limit = (int *) Tloc(l, 0);
			if (tp1 == TYPE_daytime || tp1 == TYPE_date || tp1 == TYPE_timestamp) {
				if (preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_PRECEDING, MULTI, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_FOLLOWING, MULTI, limit[k]);
				}
			} else if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, MULTI, limit[k], int);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, MULTI, limit[k], int);
			}
			break;
		}
		case TYPE_lng:{
			lng *restrict limit = (lng *) Tloc(l, 0);
			if (tp1 == TYPE_daytime || tp1 == TYPE_date || tp1 == TYPE_timestamp) {
				if (preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_PRECEDING, MULTI, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_FOLLOWING, MULTI, limit[k]);
				}
			} else if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, MULTI, limit[k], lng);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, MULTI, limit[k], lng);
			}
			break;
		}
		case TYPE_flt:{
			flt *restrict limit = (flt *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_PRECEDING, MULTI, limit[k]);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_FOLLOWING, MULTI, limit[k]);
			}
			break;
		}
		case TYPE_dbl:{
			dbl *restrict limit = (dbl *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_PRECEDING, MULTI, limit[k]);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_FOLLOWING, MULTI, limit[k]);
			}
			break;
		}
#ifdef HAVE_HGE
		case TYPE_hge:{
			hge *restrict limit = (hge *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_PRECEDING, MULTI, limit[k]);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_FOLLOWING, MULTI, limit[k]);
			}
			break;
		}
#endif
		default:
			goto bound_not_supported;
		}
	} else {		/* static bounds */
		switch (tp2) {
		case TYPE_bte:
		case TYPE_sht:
		case TYPE_int:
		case TYPE_lng:{
			lng limit = 0;
			int int_limit = 0;
			switch (tp2) {
			case TYPE_bte:{
				bte ll = (*(bte *) bound);
				if (ll == GDK_bte_max)	/* UNBOUNDED PRECEDING and UNBOUNDED FOLLOWING cases, avoid overflow */
					return GDKanalyticalallbounds(r, b, p, preceding);
				if (ll == 0)
					return GDKanalyticalpeers(r, b, p, preceding);
				limit = is_bte_nil(ll) ? lng_nil : (lng) ll;
				break;
			}
			case TYPE_sht:{
				sht ll = (*(sht *) bound);
				if (ll == GDK_sht_max)
					return GDKanalyticalallbounds(r, b, p, preceding);
				if (ll == 0)
					return GDKanalyticalpeers(r, b, p, preceding);
				limit = is_sht_nil(ll) ? lng_nil : (lng) ll;
				break;
			}
			case TYPE_int:{
				int_limit = (*(int *) bound);
				if (int_limit == GDK_int_max)
					return GDKanalyticalallbounds(r, b, p, preceding);
				if (int_limit == 0)
					return GDKanalyticalpeers(r, b, p, preceding);
				limit = is_int_nil(int_limit) ? lng_nil : (lng) int_limit;
				break;
			}
			case TYPE_lng:{
				limit = (*(lng *) bound);
				if (limit == GDK_lng_max)
					return GDKanalyticalallbounds(r, b, p, preceding);
				if (limit == 0)
					return GDKanalyticalpeers(r, b, p, preceding);
				break;
			}
			default:
				assert(0);
			}
			if (is_lng_nil(limit) || limit < 0 || is_int_nil(int_limit) || int_limit < 0) {
				goto invalid_bound;
			} else if (tp1 == TYPE_daytime || tp1 == TYPE_date || tp1 == TYPE_timestamp) {
				if (tp2 == TYPE_int) {
					if (preceding) {
						ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_PRECEDING, SINGLE, int_limit);
					} else {
						ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_FOLLOWING, SINGLE, int_limit);
					}
				} else {
					if (preceding) {
						ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_PRECEDING, SINGLE, limit);
					} else {
						ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_FOLLOWING, SINGLE, limit);
					}
				}
			} else if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_PRECEDING, SINGLE, limit, lng);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_NUM(_FOLLOWING, SINGLE, limit, lng);
			}
			break;
		}
		case TYPE_flt:{
			flt limit = (*(flt *) bound);
			if (is_flt_nil(limit) || limit < 0) {
				goto invalid_bound;
			} else if (limit == GDK_flt_max) {
				return GDKanalyticalallbounds(r, b, p, preceding);
			} else if (limit == 0) {
				return GDKanalyticalpeers(r, b, p, preceding);
			} else if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_PRECEDING, SINGLE, limit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_FLT(_FOLLOWING, SINGLE, limit);
			}
			break;
		}
		case TYPE_dbl:{
			dbl limit = (*(dbl *) bound);
			if (is_dbl_nil(limit) || limit < 0) {
				goto invalid_bound;
			} else if (limit == GDK_dbl_max) {
				return GDKanalyticalallbounds(r, b, p, preceding);
			} else if (limit == 0) {
				return GDKanalyticalpeers(r, b, p, preceding);
			} else if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_PRECEDING, SINGLE, limit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_DBL(_FOLLOWING, SINGLE, limit);
			}
			break;
		}
#ifdef HAVE_HGE
		case TYPE_hge:{
			hge limit = (*(hge *) bound);
			if (is_hge_nil(limit) || limit < 0) {
				goto invalid_bound;
			} else if (limit == GDK_hge_max) {
				return GDKanalyticalallbounds(r, b, p, preceding);
			} else if (limit == 0) {
				return GDKanalyticalpeers(r, b, p, preceding);
			} else if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_PRECEDING, SINGLE, limit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_HGE(_FOLLOWING, SINGLE, limit);
			}
			break;
		}
#endif
		default:
			goto bound_not_supported;
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = (nils == 0);
	r->tnil = (nils > 0);
	return GDK_SUCCEED;
      bound_not_supported:
	GDKerror("42000!range frame bound type %s not supported.\n", ATOMname(tp2));
	return GDK_FAIL;
      type_not_supported:
	GDKerror("42000!type %s not supported for %s frame bound type.\n", ATOMname(tp1), ATOMname(tp2));
	return GDK_FAIL;
      calc_overflow:
	GDKerror("22003!overflow in calculation.\n");
	return GDK_FAIL;
      invalid_bound:
	GDKerror("42000!range frame bound must be non negative and non null.\n");
	return GDK_FAIL;
}

static gdk_return
GDKanalyticalgroupsbounds(BAT *r, BAT *b, BAT *p, BAT *l, const void *restrict bound, int tp2, bool preceding)
{
	oid cnt = BATcount(b), *restrict rb = (oid *) Tloc(r, 0), i = 0, k = 0, j = 0;
	bit *restrict np = p ? (bit*)Tloc(p, 0) : NULL, *restrict bp = (bit*) Tloc(b, 0);
	bool last = false;

	if (b->ttype != TYPE_bit) {
		GDKerror("42000!groups frame bound type must be of type bit.\n");
		return GDK_FAIL;
	}

	if (l) {		/* dynamic bounds */
		if (l->tnil)
			goto invalid_bound;
		switch (tp2) {
		case TYPE_bte:{
			bte *restrict limit = (bte *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, MULTI, bte, limit[k], (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, MULTI, bte, limit[k], (oid) olimit);
			}
			break;
		}
		case TYPE_sht:{
			sht *restrict limit = (sht *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, MULTI, sht, limit[k], (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, MULTI, sht, limit[k], (oid) olimit);
			}
			break;
		}
		case TYPE_int:{
			int *restrict limit = (int *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, MULTI, int, limit[k], (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, MULTI, int, limit[k], (oid) olimit);
			}
			break;
		}
		case TYPE_lng:{
			lng *restrict limit = (lng *) Tloc(l, 0);
			if (preceding) {
#if SIZEOF_OID == SIZEOF_INT
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, MULTI, lng, limit[k], (olimit > (lng) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
#else
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, MULTI, lng, limit[k], (oid) olimit);
#endif
			} else {
#if SIZEOF_OID == SIZEOF_INT
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, MULTI, lng, limit[k], (olimit > (lng) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
#else
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, MULTI, lng, limit[k], (oid) olimit);
#endif
			}
			break;
		}
#ifdef HAVE_HGE
		case TYPE_hge:{
			hge *restrict limit = (hge *) Tloc(l, 0);
			if (preceding) {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, MULTI, hge, limit[k], (olimit > (hge) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
			} else {
				ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, MULTI, hge, limit[k], (olimit > (hge) GDK_oid_max) ? GDK_oid_max : (oid) olimit);
			}
			break;
		}
#endif
		default:
			goto bound_not_supported;
		}
	} else {	/* static bounds, all the limits are cast to the word size */
		lng limit;
		switch (tp2) {
		case TYPE_bte:
			limit = is_bte_nil(*(bte *) bound) ? lng_nil : (lng) *(bte *) bound;
			break;
		case TYPE_sht:
			limit = is_sht_nil(*(sht *) bound) ? lng_nil : (lng) *(sht *) bound;
			break;
		case TYPE_int:
			limit = is_int_nil(*(int *) bound) ? lng_nil : (lng) *(int *) bound;
			break;
		case TYPE_lng: {
#if SIZEOF_OID == SIZEOF_INT
			lng nval = *(lng *) bound;
			limit = is_lng_nil(nval) ? lng_nil : (nval > (lng) GDK_oid_max) ? GDK_lng_max : (lng) nval;
#else
			limit = (lng) (*(lng *) bound);
#endif
		} break;
#ifdef HAVE_HGE
		case TYPE_hge: {
			hge nval = *(hge *) bound;
			limit = is_hge_nil(nval) ? lng_nil : (nval > (hge) GDK_lng_max) ? GDK_lng_max : (lng) nval;
			break;
		}
#endif
		default:
			goto bound_not_supported;
		}
		if (limit == GDK_lng_max) {
			return GDKanalyticalallbounds(r, b, p, preceding);
		} else if (is_lng_nil(limit) || limit < 0) { /* this check is needed if the input is empty */
			goto invalid_bound;
		} else if (preceding) {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_PRECEDING, SINGLE, lng, limit, (oid) olimit);
		} else {
			ANALYTICAL_WINDOW_BOUNDS_BRANCHES_GROUPS(_FOLLOWING, SINGLE, lng, limit, (oid) olimit);
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = true;
	r->tnil = false;
	return GDK_SUCCEED;
      bound_not_supported:
	GDKerror("42000!groups frame bound type %s not supported.\n", ATOMname(tp2));
	return GDK_FAIL;
      invalid_bound:
	GDKerror("42000!groups frame bound must be non negative and non null.\n");
	return GDK_FAIL;
}

gdk_return
GDKanalyticalwindowbounds(BAT *r, BAT *b, BAT *p, BAT *l, const void *restrict bound, int tp1, int tp2, int unit, bool preceding, oid second_half)
{
	assert((l && !bound) || (!l && bound));

	switch (unit) {
	case 0:
		return GDKanalyticalrowbounds(r, b, p, l, bound, tp2, preceding, second_half);
	case 1:
		return GDKanalyticalrangebounds(r, b, p, l, bound, tp1, tp2, preceding);
	case 2:
		return GDKanalyticalgroupsbounds(r, b, p, l, bound, tp2, preceding);
	default:
		assert(0);
	}
	GDKerror("42000!unit type %d not supported (this is a bug).\n", unit);
	return GDK_FAIL;
}
