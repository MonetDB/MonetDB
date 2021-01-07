/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "mal_instruction.h"

#define do_date_trunc(val, DIVISOR)					\
	timestamp_create(timestamp_date(val),				\
			 (timestamp_daytime(val) / (DIVISOR)) * (DIVISOR))

#define date_trunc_time_loop(NAME, DIVISOR)				\
	do {								\
		if  ( strcasecmp(*scale, NAME) == 0){			\
			for( ; lo < hi; lo++)				\
				if (is_timestamp_nil(bt[lo])) {		\
					dt[lo] = timestamp_nil;		\
				} else {                 		\
					dt[lo] = do_date_trunc(bt[lo], DIVISOR); \
				}					\
		}							\
	} while (0)

static inline bool
truncate_check(const char *scale)
{
	return
		strcasecmp(scale, "millennium") == 0 ||
		strcasecmp(scale, "century") == 0  ||
		strcasecmp(scale, "decade") == 0 ||
		strcasecmp(scale, "year") == 0 ||
		strcasecmp(scale, "quarter" ) == 0 ||
		strcasecmp(scale, "month") == 0 ||
		strcasecmp(scale, "week") == 0 ||
		strcasecmp(scale, "day") == 0  ||
		strcasecmp(scale, "hour") == 0 ||
		strcasecmp(scale, "minute") == 0 ||
		strcasecmp(scale, "second") == 0 ||
		strcasecmp(scale, "milliseconds") == 0 ||
		strcasecmp(scale, "microseconds") == 0;
}

str
bat_date_trunc(bat *res, const str *scale, const bat *bid)
{
	BAT *b, *bn;
	oid lo, hi;
	const timestamp *bt;
	timestamp *dt;
	char *msg = NULL;
	date days;

	if ( truncate_check(*scale) == 0)
		throw(SQL, "batcalc.truncate_timestamp", SQLSTATE(HY005) "Improper directive ");

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.truncate_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bn = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.truncate", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	bt = (const timestamp *) Tloc(b, 0);
	dt = (timestamp *) Tloc(bn, 0);

	lo = 0;
	hi = lo + BATcount(b);

	date_trunc_time_loop("microseconds", 1);
	date_trunc_time_loop("milliseconds", 1000);
	date_trunc_time_loop("second", 1000000);
	date_trunc_time_loop("minute", 1000000 * 60);
	date_trunc_time_loop("hour", LL_CONSTANT(1000000) * 60 * 60);

	if  ( strcasecmp(*scale, "day") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(days);
			}
	}

	if  ( strcasecmp(*scale, "week") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(date_add_day(days, 1 - date_dayofweek(days)));
			}
	}

	if  ( strcasecmp(*scale, "month") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(
					date_create(date_year(days),
						    date_month(days),
						    1));
			}
	}

	if  ( strcasecmp(*scale, "quarter") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(
					date_create(date_year(days),
						    ((date_month(days) - 1) / 3) * 3 + 1,
						    1));
			}
	}

	if  ( strcasecmp(*scale, "year") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(date_create(date_year(days), 1, 1));
			}
	}

	if  ( strcasecmp(*scale, "decade") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(date_create((date_year(days) / 10) * 10, 1, 1));
			}
	}

	if  ( strcasecmp(*scale, "century") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(date_create((date_year(days) / 100) * 100, 1, 1));
			}
	}

	if  ( strcasecmp(*scale, "millennium") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = timestamp_nil;
			} else {
				days = timestamp_date(bt[lo]);
				dt[lo] = timestamp_fromdate(date_create((date_year(days) / 1000) * 1000, 1, 1));
			}
	}

	BATsetcount(bn, (BUN) lo);
	/* we can inherit most properties */
	bn->tnonil = b->tnonil;
	bn->tnil = b->tnil;
	bn->tsorted = b->tsorted;
	bn->trevsorted = b->trevsorted;
	bn->tkey = false;	/* can't be sure */
	BBPkeepref(*res = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return msg;
}

#define date_trunc_single_time(NAME, DIVISOR)			\
	do {							\
		if  ( strcasecmp(*scale, NAME) == 0){		\
			*dt = do_date_trunc(*bt, DIVISOR);	\
		}						\
	} while (0)

str
date_trunc(timestamp *dt, const str *scale, const timestamp *bt)
{
	str msg = MAL_SUCCEED;
	date days;

	if (truncate_check(*scale) == 0)
		throw(SQL, "sql.truncate", SQLSTATE(HY013) "Improper directive ");

	if (is_timestamp_nil(*bt)) {
		*dt = timestamp_nil;
		return MAL_SUCCEED;
	}

	date_trunc_single_time("microseconds", 1);
	date_trunc_single_time("milliseconds", 1000);
	date_trunc_single_time("second", 1000000);
	date_trunc_single_time("minute", 1000000 * 60);
	date_trunc_single_time("hour", LL_CONSTANT(1000000) * 60 * 60);

	if  ( strcasecmp(*scale, "day") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(days);
	}

	if  ( strcasecmp(*scale, "week") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(date_add_day(days, 1 - date_dayofweek(days)));
	}

	if  ( strcasecmp(*scale, "month") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(date_create(date_year(days), date_month(days), 1));
	}

	if  ( strcasecmp(*scale, "quarter") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(date_create(date_year(days), ((date_month(days) - 1) / 3) * 3 + 1, 1));
	}

	if  ( strcasecmp(*scale, "year") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(date_create(date_year(days), 1, 1));
	}

	if  ( strcasecmp(*scale, "decade") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(date_create((date_year(days) / 10) * 10, 1, 1));
	}

	if  ( strcasecmp(*scale, "century") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(date_create((date_year(days) / 100) * 100, 1, 1));
	}

	if  ( strcasecmp(*scale, "millennium") == 0){
		days = timestamp_date(*bt);
		*dt = timestamp_fromdate(date_create((date_year(days) / 1000) * 1000, 1, 1));
	}
	return msg;
}
