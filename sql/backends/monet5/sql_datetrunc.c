/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql.h"
#include "mal_instruction.h"

#define date_trunc_time_loop(NAME, DIVISOR)				\
	do {								\
		if  ( strcasecmp(*scale, NAME) == 0){			\
			for( ; lo < hi; lo++)				\
				if (is_timestamp_nil(bt[lo])) {		\
					dt[lo] = *timestamp_nil;	\
				} else {                 		\
					ts = bt[0];			\
					ts.msecs = (ts.msecs / (DIVISOR)) * (DIVISOR); \
					dt[lo] = ts;			\
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
	timestamp ts;
	int dow, y, m, d, one = 1;

	if ( truncate_check(*scale) == 0)
		throw(SQL, "batcalc.truncate_timestamp", SQLSTATE(HY005) "Improper directive ");

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(SQL, "batcalc.truncate_timestamp", SQLSTATE(HY005) "Cannot access column descriptor");
	}
	bn = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(SQL, "sql.truncate", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}

	bt = (const timestamp *) Tloc(b, 0);
	dt = (timestamp *) Tloc(bn, 0);

	lo = 0;
	hi = lo + BATcount(b);

	date_trunc_time_loop("microseconds", 1);
	date_trunc_time_loop("milliseconds", 1);
	date_trunc_time_loop("second", 1000);
	date_trunc_time_loop("minute", 1000 * 60);
	date_trunc_time_loop("hour", 1000 * 60 * 24);

	if  ( strcasecmp(*scale, "day") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				dt[lo] = ts;
			}
	}

	if  ( strcasecmp(*scale, "week") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				MTIMEdate_extract_dayofweek(&dow, &ts.days);
				d =  d - dow - 1;
				MTIMEdate_create(&ts.days, &y, &m, &d);
				dt[lo] = ts;
			}
	}

	if  ( strcasecmp(*scale, "month") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				MTIMEdate_create(&ts.days, &y, &m, &one);
				dt[lo] = ts;
			}
	}

	if  ( strcasecmp(*scale, "quarter") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				m = m/4 + 1;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;
			}
	}

	if  ( strcasecmp(*scale, "year") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;
			}
	}

	if  ( strcasecmp(*scale, "decade") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				y = (y /10 ) *10;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;
			}
	}

	if  ( strcasecmp(*scale, "century") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				y = (y /100 ) *100;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;
			}
	}

	if  ( strcasecmp(*scale, "millennium") == 0){
		for( ; lo < hi; lo++)
			if (is_timestamp_nil(bt[lo])) {
				dt[lo] = *timestamp_nil;
			} else {
				ts = bt[lo];
				ts.msecs = 0;
				MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
				y = (y /1000 ) *1000;
				MTIMEdate_create(&ts.days, &y, &one, &one);
				dt[lo] = ts;
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

#define date_trunc_single_time(NAME, DIVISOR)				\
	do {								\
		if  ( strcasecmp(*scale, NAME) == 0){			\
			ts = *bt;					\
			ts.msecs = (ts.msecs / (DIVISOR)) * (DIVISOR);	\
			*dt = ts;					\
		}							\
	} while (0)

str
date_trunc(timestamp *dt, const str *scale, const timestamp *bt)
{
	str msg = MAL_SUCCEED;
	timestamp ts;
	int dow, y, m, d, one = 1;

	if (truncate_check(*scale) == 0)
		throw(SQL, "sql.truncate", SQLSTATE(HY001) "Improper directive ");

	if (is_timestamp_nil(*bt)) {
		*dt = *timestamp_nil;
		return MAL_SUCCEED;
	}

	date_trunc_single_time("microseconds", 1);
	date_trunc_single_time("milliseconds", 1);
	date_trunc_single_time("second", 1000);
	date_trunc_single_time("minute", 1000 * 60);
	date_trunc_single_time("hour", 1000 * 60 * 24);

	if  ( strcasecmp(*scale, "day") == 0){
		ts = *bt;
		ts.msecs = 0;
		*dt = ts;
	}

	if  ( strcasecmp(*scale, "week") == 0){
		ts = *bt;
		ts.msecs = 0;
		MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
		MTIMEdate_extract_dayofweek(&dow, &ts.days);
		d =  d - dow - 1;
		MTIMEdate_create(&ts.days, &y, &m, &d);
		*dt = ts;
	}

	if  ( strcasecmp(*scale, "month") == 0){
		ts = *bt;
		ts.msecs = 0;
		MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
		MTIMEdate_create(&ts.days, &y, &m, &one);
		*dt = ts;
	}

	if  ( strcasecmp(*scale, "quarter") == 0){
		ts = *bt;
		ts.msecs = 0;
		MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
		m = m/4 + 1;
		MTIMEdate_create(&ts.days, &y, &one, &one);
		*dt = ts;
	}

	if  ( strcasecmp(*scale, "year") == 0){
		ts = *bt;
		ts.msecs = 0;
		MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
		MTIMEdate_create(&ts.days, &y, &one, &one);
		*dt = ts;
	}

	if  ( strcasecmp(*scale, "decade") == 0){
		ts = *bt;
		ts.msecs = 0;
		MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
		y = (y /10 ) *10;
		MTIMEdate_create(&ts.days, &y, &one, &one);
		*dt = ts;
	}

	if  ( strcasecmp(*scale, "century") == 0){
		ts = *bt;
		ts.msecs = 0;
		MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
		y = (y /100 ) *100 +1;
		MTIMEdate_create(&ts.days, &y, &one, &one);
		*dt = ts;
	}

	if  ( strcasecmp(*scale, "millennium") == 0){
		ts = *bt;
		ts.msecs = 0;
		MTIMEdate_extract_ymd(&y, &m, &d, &ts.days);
		y = (y /1000 ) *1000 +1;
		MTIMEdate_create(&ts.days, &y, &one, &one);
		*dt = ts;
	}
	return msg;
}
