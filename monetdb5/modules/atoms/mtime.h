/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
 */

#ifndef __MTIME_H__
#define __MTIME_H__

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_time.h"
#include "mal_interpreter.h"
#include "mal_exception.h"

/* TODO change dayint again into an int instead of lng */
__attribute__((__const__))
static inline lng
date_diff_imp(const date d1, const date d2)
{
	int diff = date_diff(d1, d2);
	return is_int_nil(diff) ? lng_nil : (lng) diff *(lng) (24 * 60 * 60 * 1000);
}

__attribute__((__const__))
static inline daytime
time_sub_msec_interval(const daytime t, const lng ms)
{
	if (is_lng_nil(ms))
		return daytime_nil;
	return daytime_add_usec_modulo(t, -ms * 1000);
}

__attribute__((__const__))
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
	if (is_date_nil((*ret = date_add_day(d, (int) (-ms / (24 * 60 * 60 * 1000))))))
		throw(MAL, "mtime.date_sub_msec_interval",
			  SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

static inline str
date_add_msec_interval(date *ret, date d, lng ms)
{
	if (is_date_nil(d) || is_lng_nil(ms)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	if (is_date_nil((*ret = date_add_day(d, (int) (ms / (24 * 60 * 60 * 1000))))))
		throw(MAL, "mtime.date_add_msec_interval",
			  SQLSTATE(22003) "overflow in calculation");
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
		throw(MAL, "mtime.timestamp_sub_msec_interval",
			  SQLSTATE(22003) "overflow in calculation");
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
		throw(MAL, "mtime.timestamp_sub_month_interval",
			  SQLSTATE(22003) "overflow in calculation");
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
		throw(MAL, "mtime.timestamp_add_month_interval",
			  SQLSTATE(22003) "overflow in calculation");
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
		throw(MAL, "mtime.timestamp_add_msec_interval",
			  SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}


static inline str
odbc_timestamp_add_msec_interval_time(timestamp *ret, daytime t, lng ms)
{
	date today = timestamp_date(timestamp_current());
	timestamp ts = timestamp_create(today, t);
	if (is_timestamp_nil((*ret = timestamp_add_usec(ts, ms * 1000))))
		throw(MAL, "mtime.odbc_timestamp_add_msec_interval_time",
			  SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}


static inline str
odbc_timestamp_add_month_interval_time(timestamp *ret, daytime t, int m)
{
	date today = timestamp_date(timestamp_current());
	timestamp ts = timestamp_create(today, t);
	if (is_timestamp_nil((*ret = timestamp_add_month(ts, m))))
		throw(MAL, "mtime.odbc_timestamp_add_month_interval_time",
			  SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}


static inline str
odbc_timestamp_add_msec_interval_date(timestamp *ret, date d, lng ms)
{
	timestamp ts = timestamp_fromdate(d);
	if (is_timestamp_nil((*ret = timestamp_add_usec(ts, ms * 1000))))
		throw(MAL, "mtime.odbc_timestamp_add_msec_interval_date",
			  SQLSTATE(22003) "overflow in calculation");
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
		throw(MAL, "mtime.date_submonths",
			  SQLSTATE(22003) "overflow in calculation");
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
		throw(MAL, "mtime.date_addmonths",
			  SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

__attribute__((__const__))
static inline lng
date_to_msec_since_epoch(date t)
{
	return is_date_nil(t)
		? lng_nil
		: (timestamp_diff(timestamp_create(t, daytime_create(0, 0, 0, 0)),
						  unixepoch)
		   / 1000);
}

__attribute__((__const__))
static inline lng
daytime_to_msec_since_epoch(daytime t)
{
	return daytime_diff(t, daytime_create(0, 0, 0, 0));
}

#ifdef TRUNCATE_NUMBERS
#define DIVIDE(v, div, TYPE)	(is_##TYPE##_nil(v) ? (v) : (v) / (div))
#else
#define DIVIDE(v, div, TYPE)	(is_##TYPE##_nil(v)						\
								 ? (v)									\
								 : ((v) < 0								\
									? (-(TYPE) (((u##TYPE) -(v)			\
												 + ((u##TYPE) (div) >> 1)) \
												/ (div)))				\
									: ((TYPE) (((u##TYPE) (v)			\
												+ ((u##TYPE) (div) >> 1)) \
											   / (div)))))
#endif

__attribute__((__const__))
static inline lng
timestamp_diff_msec(timestamp t1, timestamp t2)
{
	lng diff = timestamp_diff(t1, t2);
	return DIVIDE(diff, 1000, lng);
}

__attribute__((__const__))
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

__attribute__((__const__))
static inline int
timestamp_decade(timestamp t)
{
	return is_timestamp_nil(t) ? int_nil : date_year(timestamp_date(t)) / 10;
}

__attribute__((__const__))
static inline int
timestamp_year(timestamp t)
{
	return date_year(timestamp_date(t));
}

__attribute__((__const__))
static inline int
timestamp_quarter(timestamp t)
{
	return is_timestamp_nil(t) ? bte_nil : (date_month(timestamp_date(t)) - 1) / 3 + 1;
}

__attribute__((__const__))
static inline int
timestamp_month(timestamp t)
{
	return date_month(timestamp_date(t));
}

__attribute__((__const__))
static inline int
timestamp_day(timestamp t)
{
	return date_day(timestamp_date(t));
}

__attribute__((__const__))
static inline int
timestamp_hours(timestamp t)
{
	return daytime_hour(timestamp_daytime(t));
}

__attribute__((__const__))
static inline int
timestamp_minutes(timestamp t)
{
	return daytime_min(timestamp_daytime(t));
}

__attribute__((__const__))
static inline int
timestamp_extract_usecond(timestamp ts)
{
	return daytime_sec_usec(timestamp_daytime(ts));
}

__attribute__((__const__))
static inline lng
timestamp_to_msec_since_epoch(timestamp t)
{
	return is_timestamp_nil(t) ? lng_nil : (timestamp_diff(t, unixepoch) / 1000);
}

__attribute__((__const__))
static inline int
sql_year(int m)
{
	return is_int_nil(m) ? int_nil : m / 12;
}

__attribute__((__const__))
static inline int
sql_month(int m)
{
	return is_int_nil(m) ? int_nil : m % 12;
}

__attribute__((__const__))
static inline lng
sql_day(lng m)
{
	return is_lng_nil(m) ? lng_nil : m / (24*60*60*1000);
}

__attribute__((__const__))
static inline int
sql_hours(lng m)
{
	return is_lng_nil(m) ? int_nil : (int) ((m % (24*60*60*1000)) / (60*60*1000));
}

__attribute__((__const__))
static inline int
sql_minutes(lng m)
{
	return is_lng_nil(m) ? int_nil : (int) ((m % (60*60*1000)) / (60*1000));
}

__attribute__((__const__))
static inline int
sql_seconds(lng m)
{
	return is_lng_nil(m) ? int_nil : (int) ((m % (60*1000)) / 1000);
}

__attribute__((__const__))
static inline lng
msec_since_epoch(lng ts)
{
	return ts;
}

#endif /* __MTIME_H__ */
