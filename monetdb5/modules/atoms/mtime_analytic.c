/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * @t Temporal analytic module
 * @a Pedro Ferreira
 * @v 1.0
 *
 * Ranges calculation for temporal types for SQL windowing functions.
 */

#include "monetdb_config.h"
#include "mtime.h"
#include "mtime_private.h"

#define MABSOLUTE(X) ((X) < 0 ? -(X) : (X))

#define DATE_ADD_MONTH(D,M)	date_addmonth(D,M)
#define DATE_SUB_MONTH(D,M)	date_addmonth(D,-(M))
#define TIMESTAMP_ADD_MONTH(T,M)	mktimestamp(date_addmonth(ts_date(T),M),ts_time(T))
#define TIMESTAMP_SUB_MONTH(T,M)	mktimestamp(date_addmonth(ts_date(T),-(M)),ts_time(T))

#define DAYTIME_ADD_MSEC(D,M)	(D + M)
#define DAYTIME_SUB_MSEC(D,M)	(D - M)
#define DATE_ADD_MSEC(D,M)		date_add(D,(M)/(24*60*60*1000))
#define DATE_SUB_MSEC(D,M)		date_add(D,-(M)/(24*60*60*1000))
#define TIMESTAMP_ADD_MSEC(T,M)	timestamp_add(T, (M)*1000)
#define TIMESTAMP_SUB_MSEC(T,M)	timestamp_add(T, -(M)*1000)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME_PRECEDING(TPE1, LIMIT, TPE2, SUB, ADD) \
	do {																\
		lng m = k - 1;													\
		TPE1 v, vmin, vmax;												\
		TPE2 rlimit;													\
		for(; k<i; k++, rb++) {											\
			rlimit = (TPE2) LIMIT;										\
			v = bp[k];													\
			if(is_##TPE1##_nil(v)) {									\
				for(j=k; ; j--) {										\
					if(!is_##TPE1##_nil(bp[j]))							\
						break;											\
				}														\
			} else {													\
				vmin = SUB(v, rlimit);									\
				vmax = ADD(v, rlimit);									\
				for(j=k; ; j--) {										\
					if(j == m)											\
						break;											\
					if(is_##TPE1##_nil(bp[j]))							\
						break;											\
					if (bp[j] <= vmin || bp[j] >= vmax)					\
						break;											\
				}														\
			}															\
			j++;														\
			*rb = j;													\
		}																\
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME_FOLLOWING(TPE1, LIMIT, TPE2, SUB, ADD) \
	do {																\
		TPE1 v, vmin, vmax;												\
		TPE2 rlimit;													\
		for(; k<i; k++, rb++) {											\
			rlimit = (TPE2) LIMIT;										\
			v = bp[k];													\
			if(is_##TPE1##_nil(v)) {									\
				for(j=k+1; j<i; j++) {									\
					if(!is_##TPE1##_nil(bp[j]))							\
						break;											\
				}														\
			} else {													\
				vmin = SUB(v, rlimit);									\
				vmax = ADD(v, rlimit);									\
				for(j=k+1; j<i; j++) {									\
					if(is_##TPE1##_nil(bp[j]))							\
						break;											\
					if (bp[j] <= vmin || bp[j] >= vmax)					\
						break;											\
				}														\
			}															\
			*rb = j;													\
		}																\
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(TPE1, IMP, LIMIT, TPE2, SUB, ADD) \
	do { \
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0); \
		if(np) { \
			nend += cnt; \
			for(; np<nend; np++) { \
				if (*np) { \
					i += (np - pnp); \
					IMP(TPE1, LIMIT, TPE2, SUB, ADD); \
					pnp = np; \
				} \
			} \
			i += (np - pnp); \
			IMP(TPE1, LIMIT, TPE2, SUB, ADD); \
		} else { \
			i += (lng) cnt; \
			IMP(TPE1, LIMIT, TPE2, SUB, ADD); \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(IMP, LIMIT) \
	do { \
		if(tp1 == TYPE_date) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(date, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, int, DATE_SUB_MONTH, DATE_ADD_MONTH); \
		} else if(tp1 == TYPE_timestamp) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(timestamp, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, int, TIMESTAMP_SUB_MONTH, TIMESTAMP_ADD_MONTH); \
		} else { \
			goto type_not_supported; \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(IMP, LIMIT) \
	do { \
		if(tp1 == TYPE_daytime) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(daytime, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, lng, DAYTIME_SUB_MSEC, DAYTIME_ADD_MSEC); \
		} else if(tp1 == TYPE_date) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(date, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, lng, DATE_SUB_MSEC, DATE_ADD_MSEC); \
		} else if(tp1 == TYPE_timestamp) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(timestamp, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, lng, TIMESTAMP_SUB_MSEC, TIMESTAMP_ADD_MSEC); \
		} else { \
			goto type_not_supported; \
		} \
	} while(0)

str //VERY IMPORTANT -> the FRAME_ALL case shall never fall here, as well as FRAME_GROUPS and FRAME_ROWS
MTIMEanalyticalrangebounds(BAT *r, BAT *b, BAT *p, BAT *l, const void* restrict bound, int tp1, int tp2,
						   bool preceding, lng first_half)
{
	BUN cnt = BATcount(b), nils = 0;
	lng *restrict rb = (lng*) Tloc(r, 0), i = 0, k = 0, j = 0;
	bit *np = p ? (bit*) Tloc(p, 0) : NULL, *pnp = np, *nend = np;
	str msg = MAL_SUCCEED;

	(void) first_half;
	assert((l && !bound) || (!l && bound));

	if (l) { /* dynamic bounds */
		switch(tp2) {
			case TYPE_int: { //month_interval (not available for daytime type)
				int *restrict limit = (int*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_FOLLOWING, limit[k]);
				}
			} break;
			case TYPE_lng: { //sec_interval, which are milliseconds
				lng *restrict limit = (lng*) Tloc(l, 0);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_PRECEDING, limit[k]);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_FOLLOWING, limit[k]);
				}
			} break;
			default:
				goto bound_not_supported;
		}
	} else { /* static bounds */
		switch(tp2) {
			case TYPE_int: {
				int limit = (*(int*)bound);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_PRECEDING, limit);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(_FOLLOWING, limit);
				}
			} break;
			case TYPE_lng: {
				lng limit = (*(lng*)bound);
				if(preceding) {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_PRECEDING, limit);
				} else {
					ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(_FOLLOWING, limit);
				}
			} break;
			default:
				goto bound_not_supported;
		}
	}
	BATsetcount(r, cnt);
	r->tnonil = (nils == 0);
	r->tnil = (nils > 0);
	return msg;
bound_not_supported:
	throw(MAL, "mtime.analyticalrangebounds", SQLSTATE(42000) "range frame bound type %s not supported.\n", ATOMname(tp2));
type_not_supported:
	throw(MAL, "mtime.analyticalrangebounds", SQLSTATE(42000) "type %s not supported for %s frame bound type.\n", ATOMname(tp1), ATOMname(tp2));
}
