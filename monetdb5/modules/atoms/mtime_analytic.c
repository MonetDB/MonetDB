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

#define MTIME_SUB_WITH_CHECK(lft, rgt, TYPE, dst, min, max, on_overflow) \
	do { \
		if ((rgt) < 1) { \
			if ((max) + (rgt) < (lft)) \
				on_overflow; \
			else \
				(dst) = (TYPE) (lft) - (rgt); \
		} else { \
			if ((min) + (rgt) > (lft)) \
				on_overflow; \
			else \
				(dst) = (TYPE) (lft) - (rgt); \
		} \
	} while (0)

#define MABSOLUTE(X) ((X) < 0 ? -(X) : (X))

#define DATE_RANGE_MONTH_DIFF(X,Y,R) \
	do { \
		MTIME_SUB_WITH_CHECK(X, Y, date, R, DATE_MIN, DATE_MAX, goto calc_overflow); \
		R = MABSOLUTE(R); \
		R /= 30; /* days in a month */ \
		R += (X != Y); /* in a '0' month interval, the rows don't belong to the same frame if the difference is less than one month */ \
	} while (0)

#define TIMESTAMP_RANGE_MONTH_DIFF(X,Y,R) \
	do { \
		MTIME_SUB_WITH_CHECK(X.days, Y.days, date, R, DATE_MIN, DATE_MAX, goto calc_overflow); \
		R = MABSOLUTE(R); \
		R /= 30; /* days in a month */ \
		R += (X.days != Y.days); /* same reason as above */ \
	} while (0)

#define DAYTIME_RANGE_SEC_DIFF(X,Y,R) \
	do { \
		R = MABSOLUTE(X - Y); /* never overflows */ \
	} while (0)

#define DATE_RANGE_SEC_DIFF(X,Y,R) \
	do { \
		MTIME_SUB_WITH_CHECK(X, Y, date, R, DATE_MIN, DATE_MAX, goto calc_overflow); \
		R = MABSOLUTE(R); \
		R *= 86400000; /* days in milliseconds */ \
	} while (0)

#define TIMESTAMP_RANGE_SEC_DIFF(X,Y,R) \
	do { \
		MTIME_SUB_WITH_CHECK(X.days, Y.days, date, R, DATE_MIN, DATE_MAX, goto calc_overflow); \
		R = MABSOLUTE(R); \
		R *= 86400000; /* days in milliseconds */ \
		R += MABSOLUTE(X.msecs - Y.msecs); /* never overflows */ \
	} while (0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME_PRECEDING(TPE1, LIMIT, TPE2, CMP) \
	do {																\
		lng m = k - 1;													\
		TPE1 v;															\
		TPE2 rlimit, calc;												\
		for(; k<i; k++, rb++) {											\
			rlimit = (TPE2) LIMIT;										\
			v = bp[k];													\
			if(is_##TPE1##_nil(v)) {									\
				for(j=k; ; j--) {										\
					if(!is_##TPE1##_nil(bp[j]))							\
						break;											\
				}														\
			} else {													\
				for(j=k; ; j--) {										\
					if(j == m)											\
						break;											\
					if(is_##TPE1##_nil(bp[j]))							\
						break;											\
					CMP(v, bp[j], calc);								\
					if(calc > rlimit)									\
						break;											\
				}														\
			}															\
			j++;														\
			*rb = j;													\
		}																\
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME_FOLLOWING(TPE1, LIMIT, TPE2, CMP) \
	do {																\
		TPE1 v;															\
		TPE2 rlimit, calc;												\
		for(; k<i; k++, rb++) {											\
			rlimit = (TPE2) LIMIT;										\
			v = bp[k];													\
			if(is_##TPE1##_nil(v)) {									\
				for(j=k+1; j<i; j++) {									\
					if(!is_##TPE1##_nil(bp[j]))							\
						break;											\
				}														\
			} else {													\
				for(j=k+1; j<i; j++) {									\
					if(is_##TPE1##_nil(bp[j]))							\
						break;											\
					CMP(v, bp[j], calc);								\
					if(calc > rlimit)									\
						break;											\
				}														\
			}															\
			*rb = j;													\
		}																\
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(TPE1, IMP, LIMIT, TPE2, CMP) \
	do { \
		TPE1 *restrict bp = (TPE1*)Tloc(b, 0); \
		if(np) { \
			nend += cnt; \
			for(; np<nend; np++) { \
				if (*np) { \
					i += (np - pnp); \
					IMP(TPE1, LIMIT, TPE2, CMP); \
					pnp = np; \
				} \
			} \
			i += (np - pnp); \
			IMP(TPE1, LIMIT, TPE2, CMP); \
		} else { \
			i += (lng) cnt; \
			IMP(TPE1, LIMIT, TPE2, CMP); \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_MONTH_INTERVAL(IMP, LIMIT) \
	do { \
		if(tp1 == TYPE_date) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(date, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, int, DATE_RANGE_MONTH_DIFF); \
		} else if(tp1 == TYPE_timestamp) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(timestamp, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, int, TIMESTAMP_RANGE_MONTH_DIFF); \
		} else { \
			goto type_not_supported; \
		} \
	} while(0)

#define ANALYTICAL_WINDOW_BOUNDS_BRANCHES_RANGE_MTIME_SEC_INTERVAL(IMP, LIMIT) \
	do { \
		if(tp1 == TYPE_daytime) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(daytime, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, lng, DAYTIME_RANGE_SEC_DIFF); \
		} else if(tp1 == TYPE_date) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(date, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, lng, DATE_RANGE_SEC_DIFF); \
		} else if(tp1 == TYPE_timestamp) { \
			ANALYTICAL_WINDOW_BOUNDS_CALC_FIXED_MTIME(timestamp, ANALYTICAL_WINDOW_BOUNDS_FIXED_RANGE_MTIME##IMP, LIMIT, lng, TIMESTAMP_RANGE_SEC_DIFF); \
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
calc_overflow:
	throw(MAL, "mtime.analyticalrangebounds", SQLSTATE(22003) "overflow in calculation.\n");
}
