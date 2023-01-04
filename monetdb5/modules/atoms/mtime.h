/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#ifndef __MTIME_H__
#define __MTIME_H__

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_time.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

/* TODO change dayint again into an int instead of lng */
static inline lng
date_diff_imp(const date d1, const date d2)
{
	int diff = date_diff(d1, d2);
	return is_int_nil(diff) ? lng_nil : (lng) diff * (lng) (24*60*60*1000);
}

static inline daytime
time_sub_msec_interval(const daytime t, const lng ms)
{
	if (is_lng_nil(ms))
		return daytime_nil;
	return daytime_add_usec_modulo(t, -ms * 1000);
}

static inline daytime
time_add_msec_interval(const daytime t, const lng ms)
{
	if (is_lng_nil(ms))
		return daytime_nil;
	return daytime_add_usec_modulo(t, ms * 1000);
}

static inline str
date_sub_msec_interval(date *ret, date d, lng ms)
{
	if (is_date_nil(d) || is_lng_nil(ms)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	if (is_date_nil((*ret = date_add_day(d, (int) (-ms / (24*60*60*1000))))))
		throw(MAL, "mtime.date_sub_msec_interval", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
date_add_msec_interval(date *ret, date d, lng ms)
{
	if (is_date_nil(d) || is_lng_nil(ms)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	if (is_date_nil((*ret = date_add_day(d, (int) (ms / (24*60*60*1000))))))
		throw(MAL, "mtime.date_add_msec_interval", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
timestamp_sub_msec_interval(timestamp *ret, timestamp ts, lng ms)
{
	if (is_timestamp_nil(ts) || is_lng_nil(ms)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	if (is_timestamp_nil((*ret = timestamp_add_usec(ts, -ms * 1000))))
		throw(MAL, "mtime.timestamp_sub_msec_interval", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
timestamp_sub_month_interval(timestamp *ret, timestamp ts, int m)
{
	if (is_timestamp_nil(ts) || is_int_nil(m)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	if (is_timestamp_nil((*ret = timestamp_add_month(ts, -m))))
		throw(MAL, "mtime.timestamp_sub_month_interval", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
timestamp_add_month_interval(timestamp *ret, timestamp ts, int m)
{
	if (is_timestamp_nil(ts) || is_int_nil(m)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	if (is_timestamp_nil((*ret = timestamp_add_month(ts, m))))
		throw(MAL, "mtime.timestamp_add_month_interval", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
timestamp_add_msec_interval(timestamp *ret, timestamp ts, lng ms)
{
	if (is_timestamp_nil(ts) || is_lng_nil(ms)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	if (is_timestamp_nil((*ret = timestamp_add_usec(ts, ms * 1000))))
		throw(MAL, "mtime.timestamp_add_msec_interval", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
date_submonths(date *ret, date d, int m)
{
	if (is_date_nil(d) || is_int_nil(m)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	if (is_date_nil((*ret = date_add_month(d, -m))))
		throw(MAL, "mtime.date_submonths", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
date_addmonths(date *ret, date d, int m)
{
	if (is_date_nil(d) || is_int_nil(m)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	if (is_date_nil((*ret = date_add_month(d, m))))
		throw(MAL, "mtime.date_addmonths", SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

#define date_to_msec_since_epoch(t) is_date_nil(t) ? lng_nil : (timestamp_diff(timestamp_create(t, daytime_create(0, 0, 0, 0)), unixepoch) / 1000)
#define daytime_to_msec_since_epoch(t) daytime_diff(t, daytime_create(0, 0, 0, 0))

static inline lng
TSDIFF(timestamp t1, timestamp t2)
{
	lng diff = timestamp_diff(t1, t2);
	if (!is_lng_nil(diff)) {
#ifndef TRUNCATE_NUMBERS
		if (diff < 0)
			diff = -((-diff + 500) / 1000);
		else
			diff = (diff + 500) / 1000;
#else
		diff /= 1000;
#endif
	}
	return diff;
}

static inline int
timestamp_century(const timestamp t)
{
	if (is_timestamp_nil(t))
		return int_nil;
	int y = date_year(timestamp_date(t));
	if (y > 0)
		return (y - 1) / 100 + 1;
	else
		return -((-y - 1) / 100 + 1);
}
#define timestamp_decade(t) is_timestamp_nil(t) ? int_nil : date_year(timestamp_date(t)) / 10
#define timestamp_year(t) date_year(timestamp_date(t))
#define timestamp_quarter(t) is_timestamp_nil(t) ? int_nil : (date_month(timestamp_date(t)) - 1) / 3 + 1
#define timestamp_month(t) date_month(timestamp_date(t))
#define timestamp_day(t) date_day(timestamp_date(t))
#define timestamp_hours(t) daytime_hour(timestamp_daytime(t))
#define timestamp_minutes(t) daytime_min(timestamp_daytime(t))
#define timestamp_extract_usecond(ts)	daytime_sec_usec(timestamp_daytime(ts))
#define timestamp_to_msec_since_epoch(t) is_timestamp_nil(t) ? lng_nil : (timestamp_diff(t, unixepoch) / 1000)

#define sql_year(m) is_int_nil(m) ? int_nil : m / 12
#define sql_month(m) is_int_nil(m) ? int_nil : m % 12
#define sql_day(m) is_lng_nil(m) ? lng_nil : m / (24*60*60*1000)
#define sql_hours(m) is_lng_nil(m) ? int_nil : (int) ((m % (24*60*60*1000)) / (60*60*1000))
#define sql_minutes(m) is_lng_nil(m) ? int_nil : (int) ((m % (60*60*1000)) / (60*1000))
#define sql_seconds(m) is_lng_nil(m) ? int_nil : (int) ((m % (60*1000)) / 1000)
#define msec_since_epoch(ts)	ts

#endif /* __MTIME_H__ */
