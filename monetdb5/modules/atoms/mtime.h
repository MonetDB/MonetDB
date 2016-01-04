/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

/*
 * @* Implementation
 *
 * @+ Atoms
 *
 * @- date
 * Internally, we store date as the (possibly negative) number of days
 * since the start of the calendar. Oddly, since I (later) learned
 * that the year 0 did no exist, this defines the start of the
 * calendar to be Jan 1 of the year -1 (in other words, a -- positive
 * -- year component of a date is equal to the number of years that
 * have passed since the start of the calendar).
 */
#ifndef _MONETTIME_H_
#define _MONETTIME_H_

#include <gdk.h>
#include "mal.h"
#include "mal_exception.h"

#ifdef WIN32
#if !defined(LIBMAL) && !defined(LIBATOMS) && !defined(LIBKERNEL) && !defined(LIBMAL) && !defined(LIBOPTIMIZER) && !defined(LIBSCHEDULER) && !defined(LIBMONETDB5)
#define mtime_export extern __declspec(dllimport)
#else
#define mtime_export extern __declspec(dllexport)
#endif
#else
#define mtime_export extern
#endif

#ifdef TIME_WITH_SYS_TIME
# include <sys/time.h>
# include <time.h>
#else
# ifdef HAVE_SYS_TIME_H
#  include <sys/time.h>
# else
#  include <time.h>
# endif
#endif

typedef int date;
#define date_nil		((date) int_nil)
#define date_isnil(X)	((X) == date_nil)

/*
 * @- daytime
 * Daytime values are also stored as the number of milliseconds that
 * passed since the start of the day (i.e. midnight).
 */
typedef int daytime;
#define daytime_nil ((daytime) int_nil)
#define daytime_isnil(X) ((X) == daytime_nil)

/*
 * @- timestamp
 * Timestamp is implemented as a record that contains a date and a time (GMT).
 */
typedef union {
	lng alignment;
	struct {
#ifndef WORDS_BIGENDIAN
		daytime p_msecs;
		date p_days;
#else
		date p_days;
		daytime p_msecs;
#endif
	} payload;
} timestamp;
#define msecs payload.p_msecs
#define days payload.p_days

/*
 * @- rule
 * rules are used to define the start and end of DST. It uses the 25
 * lower bits of an int.
 */
typedef union {
	struct {
		unsigned int month:4,	/* values: [1..12] */
		 minutes:11,			/* values: [0:1439] */
		 day:6,					/* values: [-31..-1,1..31] */
		 weekday:4,				/* values: [-7..-1,1..7] */
		 empty:7;				/* rule uses just 32-7=25 bits */
	} s;
	int asint;					/* the same, seen as single value */
} rule;

/*
 * @- tzone
 * A tzone consists of an offset and two DST rules, all crammed into one lng.
 */
typedef struct {
	/* we had this as bit fields in one unsigned long long, but native
	 * sun CC does not eat that.  */
	unsigned int dst:1, off1:6, dst_start:25;
	unsigned int off2:7, dst_end:25;
} tzone;

mtime_export tzone tzone_local;
mtime_export timestamp *timestamp_nil;

#define timestamp_isnil(X) ts_isnil(X)
#define tz_isnil(z)   (get_offset(&(z)) == get_offset(tzone_nil))
#define ts_isnil(t)   ((t).days == timestamp_nil->days && (t).msecs == timestamp_nil->msecs)

mtime_export int daytime_tz_fromstr(const char *buf, int *len, daytime **ret);
mtime_export int timestamp_tz_fromstr(const char *buf, int *len, timestamp **ret);
mtime_export str MTIMEcurrent_timestamp(timestamp *t);
mtime_export str MTIMEcurrent_date(date *d);
mtime_export str MTIMEcurrent_time(daytime *t);
mtime_export int timestamp_tz_tostr(str *buf, int *len, const timestamp *val, const tzone *timezone);
mtime_export str MTIMEnil2date(date *ret, const void *src);
mtime_export str MTIMEdate2date(date *ret, const date *src);
mtime_export str MTIMEdaytime2daytime(daytime *ret, const daytime *src);
mtime_export str MTIMEsecs2daytime(daytime *ret, const lng *src);
mtime_export str MTIMEsecs2daytime_bulk(bat *ret, bat *bid);
mtime_export str MTIMEtimestamp2timestamp(timestamp *ret, const timestamp *src);
mtime_export str MTIMEprelude(void *ret);
mtime_export str MTIMEepilogue(void *ret);
mtime_export str MTIMEsynonyms(void *ret, const bit *allow);
mtime_export str MTIMEtimezone(tzone *z, const char * const *name);
mtime_export str MTIMElocal_timezone(lng *res);
mtime_export str MTIMEtzone_set_local(void *res, const tzone *z);
mtime_export str MTIMEtzone_get_local(tzone *z);
mtime_export str MTIMEmonth_from_str(int *ret, const char * const *month);
mtime_export str MTIMEmonth_to_str(str *ret, const int *month);
mtime_export str MTIMEday_from_str(int *ret, const char * const *day);
mtime_export str MTIMEday_to_str(str *ret, const int *day);
mtime_export str MTIMEdate_date(date *d, const date *s);
mtime_export str MTIMEdate_fromstr(date *ret, const char * const *s);
mtime_export str MTIMEdate_create(date *ret, const int *year, const int *month, const int *day);
mtime_export str MTIMEdaytime_create(daytime *ret, const int *hour, const int *min, const int *sec, const int *msec);
mtime_export str MTIMEtimestamp_fromstr(timestamp *ret, const char * const *d);
mtime_export str MTIMEtimestamp_create(timestamp *ret, const date *d, const daytime *t, const tzone *z);
mtime_export str MTIMEtimestamp_create_default(timestamp *ret, const date *d, const daytime *t);
mtime_export str MTIMEtimestamp_create_from_date(timestamp *ret, const date *d);
mtime_export str MTIMEtimestamp_create_from_date_bulk(bat *ret, bat *bid);
mtime_export str MTIMEdate_extract_year(int *ret, const date *v);
mtime_export str MTIMEdate_extract_month(int *ret, const date *v);
mtime_export str MTIMEdate_extract_day(int *ret, const date *v);
mtime_export str MTIMEdate_extract_dayofyear(int *ret, const date *v);
mtime_export str MTIMEdate_extract_weekofyear(int *ret, const date *v);
mtime_export str MTIMEdate_extract_dayofweek(int *ret, const date *v);
mtime_export str MTIMEdaytime_extract_hours(int *ret, const daytime *v);
mtime_export str MTIMEdaytime_extract_minutes(int *ret, const daytime *v);
mtime_export str MTIMEdaytime_extract_seconds(int *ret, const daytime *v);
mtime_export str MTIMEdaytime_extract_sql_seconds(int *ret, const daytime *v);
mtime_export str MTIMEdaytime_extract_milliseconds(int *ret, const daytime *v);
mtime_export str MTIMEtimestamp_extract_daytime(daytime *ret, const timestamp *t, const tzone *z);
mtime_export str MTIMEtimestamp_extract_daytime_default(daytime *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_extract_daytime_default_bulk(bat *ret, bat *bid);
mtime_export str MTIMEtimestamp_extract_date(date *ret, const timestamp *t, const tzone *z);
mtime_export str MTIMEtimestamp_extract_date_default(date *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_extract_date_default_bulk(bat *ret, bat *bid);
mtime_export str MTIMEdate_addyears(date *ret, const date *v, const int *delta);
mtime_export str MTIMEdate_adddays(date *ret, const date *v, const int *delta);
mtime_export str MTIMEdate_addmonths(date *ret, const date *v, const int *delta);
mtime_export str MTIMEdate_submonths(date *ret, const date *v, const int *delta);
mtime_export str MTIMEdate_diff(int *ret, const date *v1, const date *v2);
mtime_export str MTIMEdate_diff_bulk(bat *ret, const bat *bid1, const bat *bid2);
mtime_export str MTIMEtimestamp_add(timestamp *ret, const timestamp *v, const lng *msec);
mtime_export str MTIMEdaytime_diff(lng *ret, const daytime *v1, const daytime *v2);
mtime_export str MTIMEtimestamp_diff(lng *ret, const timestamp *v1, const timestamp *v2);
mtime_export str MTIMEtimestamp_diff_bulk(bat *ret, const bat *bid1, const bat *bid2);
mtime_export str MTIMEtimestamp_inside_dst(bit *ret, const timestamp *p, const tzone *z);

mtime_export str MTIMEtimestamp_year(int *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_month(int *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_day(int *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_hours(int *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_minutes(int *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_seconds(int *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_sql_seconds(int *ret, const timestamp *t);
mtime_export str MTIMEtimestamp_milliseconds(int *ret, const timestamp *t);
mtime_export str MTIMEsql_year(int *ret, const int *t);
mtime_export str MTIMEsql_month(int *ret, const int *t);
mtime_export str MTIMEsql_day(lng *ret, const lng *t);
mtime_export str MTIMEsql_hours(int *ret, const lng *t);
mtime_export str MTIMEsql_minutes(int *ret, const lng *t);
mtime_export str MTIMEsql_seconds(int *ret, const lng *t);

mtime_export str MTIMErule_fromstr(rule *ret, const char * const *s);
mtime_export str MTIMErule_create(rule *ret, const int *month, const int *day, const int *weekday, const int *minutes);
mtime_export str MTIMEtzone_create_dst(tzone *ret, const int *minutes, const rule *start, const rule *end);
mtime_export str MTIMEtzone_create(tzone *ret, const int *minutes);
mtime_export str MTIMEtzone_create_lng(tzone *ret, const lng *minutes);
mtime_export str MTIMErule_extract_month(int *ret, const rule *r);
mtime_export str MTIMErule_extract_day(int *ret, const rule *r);
mtime_export str MTIMErule_extract_weekday(int *ret, const rule *r);
mtime_export str MTIMErule_extract_minutes(int *ret, const rule *r);
mtime_export str MTIMEtzone_extract_start(rule *ret, const tzone *t);
mtime_export str MTIMEtzone_extract_end(rule *ret, const tzone *t);
mtime_export str MTIMEtzone_extract_minutes(int *ret, const tzone *t);
mtime_export str MTIMEdate_sub_sec_interval_wrap(date *ret, const date *t, const int *sec);
mtime_export str MTIMEdate_sub_msec_interval_lng_wrap(date *ret, const date *t, const lng *msec);
mtime_export str MTIMEdate_add_sec_interval_wrap(date *ret, const date *t, const int *sec);
mtime_export str MTIMEdate_add_msec_interval_lng_wrap(date *ret, const date *t, const lng *msec);
mtime_export str MTIMEtimestamp_sub_msec_interval_lng_wrap(timestamp *ret, const timestamp *t, const lng *msec);
mtime_export str MTIMEtimestamp_sub_month_interval_wrap(timestamp *ret, const timestamp *t, const int *months);
mtime_export str MTIMEtimestamp_add_month_interval_wrap(timestamp *ret, const timestamp *t, const int *months);
mtime_export str MTIMEtimestamp_sub_month_interval_lng_wrap(timestamp *ret, const timestamp *t, const lng *months);
mtime_export str MTIMEtimestamp_add_month_interval_lng_wrap(timestamp *ret, const timestamp *t, const lng *months);
mtime_export str MTIMEtime_sub_msec_interval_wrap(daytime *ret, const daytime *t, const lng *msec);
mtime_export str MTIMEtime_add_msec_interval_wrap(daytime *ret, const daytime *t, const lng *msec);
mtime_export str MTIMEcompute_rule_foryear(date *ret, const rule *val, const int *year);
mtime_export str MTIMEtzone_tostr(str *s, const tzone *ret);
mtime_export str MTIMEtzone_fromstr(tzone *ret, const char * const *s);
mtime_export str MTIMEdaytime_fromstr(daytime *ret, const char * const *s);
mtime_export str MTIMEmsecs(lng *ret, const int *d, const int *h, const int *m, const int *s, const int *ms);
mtime_export str MTIMEmsec(lng *r);
mtime_export str MTIMEdaytime1(daytime *ret, const int *h);
mtime_export str MTIMEdaytime2(daytime *ret, const int *h, const int *m);
mtime_export str MTIMEdaytime3(daytime *ret, const int *h, const int *m, const int *s);
mtime_export str MTIMEunix_epoch(timestamp *ret);
mtime_export str MTIMEepoch2int(int *res, const timestamp *ts);
mtime_export str MTIMEtimestamp(timestamp *ret, const int *sec);
mtime_export str MTIMEtimestamplng(timestamp *ret, const lng *sec);
mtime_export str MTIMEtimestamp_bulk(bat *ret, bat *bid);
mtime_export str MTIMEtimestamp_lng(timestamp *ret, const lng *msec);
mtime_export str MTIMEtimestamp_lng_bulk(bat *ret, bat *bid);
mtime_export str MTIMEruleDef0(rule *ret, const int *m, const int *d, const int *w, const int *h, const int *mint);
mtime_export str MTIMEruleDef1(rule *ret, const int *m, const char * const *dnme, const int *w, const int *h, const int *mint);
mtime_export str MTIMEruleDef2(rule *ret, const int *m, const char * const *dnme, const int *w, const int *mint);
mtime_export int date_fromstr(const char *buf, int *len, date **d);
mtime_export int date_tostr(str *buf, int *len, const date *val);
mtime_export int daytime_fromstr(const char *buf, int *len, daytime **ret);
mtime_export int daytime_tostr(str *buf, int *len, const daytime *val);
mtime_export int timestamp_fromstr(const char *buf, int *len, timestamp **ret);
mtime_export int timestamp_tostr(str *buf, int *len, const timestamp *val);
mtime_export int tzone_tostr(str *buf, int *len, const tzone *z);
mtime_export int rule_fromstr(const char *buf, int *len, rule **d);
mtime_export int rule_tostr(str *buf, int *len, const rule *r);
mtime_export int tzone_fromstr(const char *buf, int *len, tzone **d);

mtime_export str MTIMEstr_to_date(date *d, const char * const *s, const char * const *format);
mtime_export str MTIMEdate_to_str(str *s, const date *d, const char * const *format);
mtime_export str MTIMEstr_to_time(daytime *d, const char * const *s, const char * const *format);
mtime_export str MTIMEtime_to_str(str *s, const daytime *d, const char * const *format);
mtime_export str MTIMEstr_to_timestamp(timestamp *d, const char * const *s, const char * const *format);
mtime_export str MTIMEtimestamp_to_str(str *s, const timestamp *d, const char * const *format);

mtime_export str MTIMEdate_extract_year_bulk(bat *ret, const bat *bid);

mtime_export str MTIMEdate_extract_month_bulk(bat *ret, const bat *bid);

mtime_export str MTIMEdate_extract_day_bulk(bat *ret, const bat *bid);

mtime_export str MTIMEdaytime_extract_hours_bulk(bat *ret, const bat *bid);

mtime_export str MTIMEdaytime_extract_minutes_bulk(bat *ret, const bat *bid);

mtime_export str MTIMEdaytime_extract_seconds_bulk(bat *ret, const bat *bid);

mtime_export str MTIMEdaytime_extract_sql_seconds_bulk(bat *ret, const bat *bid);

mtime_export str MTIMEdaytime_extract_milliseconds_bulk(bat *ret, const bat *bid);

mtime_export int TYPE_date;
mtime_export int TYPE_daytime;
mtime_export int TYPE_timestamp;
mtime_export int TYPE_tzone;
mtime_export int TYPE_rule;

#endif /* _MONETTIME_H_ */
