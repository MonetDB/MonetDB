/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2013 MonetDB B.V.
 * All Rights Reserved.
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
typedef struct {
#ifndef WORDS_BIGENDIAN
	daytime msecs;
	date days;
#else
	date days;
	daytime msecs;
#endif
} timestamp;

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

mtime_export void fromdate(int n, int *d, int *m, int *y);
mtime_export void fromtime(int n, int *hour, int *min, int *sec, int *msec);
mtime_export int daytime_tz_fromstr(str buf, int *len, daytime **ret);
mtime_export str MTIMEcurrent_timestamp(timestamp *t);
mtime_export str MTIMEcurrent_date(date *d);
mtime_export str MTIMEcurrent_time(daytime *t);
mtime_export int timestamp_tostr(str *buf, int *len, timestamp *val);
mtime_export int timestamp_tz_tostr(str *buf, int *len, timestamp *val, tzone *timezone);
mtime_export str MTIMEnil2date(date *ret, int *src);
mtime_export str MTIMEdate2date(date *ret, date *src);
mtime_export str MTIMEdaytime2daytime(daytime *ret, daytime *src);
mtime_export str MTIMEsecs2daytime(daytime *ret, lng *src);
mtime_export str MTIMEtimestamp2timestamp(timestamp *ret, timestamp *src);
mtime_export str MTIMErule_fromstr(rule *ret, str *s);
mtime_export str MTIMEprelude(void);
mtime_export str MTIMEepilogue(void);
mtime_export str MTIMEsynonyms(bit *allow);
mtime_export str MTIMEoldduration(int *ndays, str *s);
mtime_export str MTIMEolddate(date *d, str *buf);
mtime_export str MTIMEtimezone(tzone *z, str *name);
mtime_export str MTIMElocal_timezone(lng *res);
mtime_export str MTIMEtzone_set_local(int res, tzone *z);
mtime_export str MTIMEtzone_get_local(tzone *z);
mtime_export str MTIMEmonth_from_str(int *ret, str *month);
mtime_export str MTIMEmonth_to_str(str *ret, int *month);
mtime_export str MTIMEday_from_str(int *ret, str *day);
mtime_export str MTIMEday_to_str(str *ret, int *day);
mtime_export str MTIMEdate_date(date *d, date *s);
mtime_export str MTIMEdate_tostr(str *ret, date *d);
mtime_export str MTIMEdate_fromstr(date *ret, str *s);
mtime_export str MTIMEdate_create(date *ret, int *year, int *month, int *day);
mtime_export str MTIMEdaytime_tostr(str *ret, daytime *d);
mtime_export str MTIMEdaytime_create(daytime *ret, int *hour, int *min, int *sec, int *msec);
mtime_export str MTIMEtimestamp_fromstr(timestamp *ret, str *d);
mtime_export str MTIMEtimestamp_timestamp(timestamp *d, timestamp *s);
mtime_export str MTIMEtimestamp_create(timestamp *ret, date *d, daytime *t, tzone *z);
mtime_export str MTIMEtimestamp_create_default(timestamp *ret, date *d, daytime *t);
mtime_export str MTIMEtimestamp_create_from_date(timestamp *ret, date *d);
mtime_export str MTIMEdate_extract_year(int *ret, date *v);
mtime_export str MTIMEdate_extract_month(int *ret, date *v);
mtime_export str MTIMEdate_extract_day(int *ret, date *v);
mtime_export str MTIMEdate_extract_dayofyear(int *ret, date *v);
mtime_export str MTIMEdate_extract_weekofyear(int *ret, date *v);
mtime_export str MTIMEdate_extract_dayofweek(int *ret, date *v);
mtime_export str MTIMEdaytime_extract_hours(int *ret, daytime *v);
mtime_export str MTIMEdaytime_extract_minutes(int *ret, daytime *v);
mtime_export str MTIMEdaytime_extract_seconds(int *ret, daytime *v);
mtime_export str MTIMEdaytime_extract_sql_seconds(int *ret, daytime *v);
mtime_export str MTIMEdaytime_extract_milliseconds(int *ret, daytime *v);
mtime_export str MTIMEtimestamp_extract_daytime(daytime *ret, timestamp *t, tzone *z);
mtime_export str MTIMEtimestamp_extract_daytime_default(daytime *ret, timestamp *t);
mtime_export str MTIMEtimestamp_extract_date(date *ret, timestamp *t, tzone *z);
mtime_export str MTIMEtimestamp_extract_date_default(date *ret, timestamp *t);
mtime_export str MTIMEdate_addyears(date *ret, date *v, int *delta);
mtime_export str MTIMEdate_adddays(date *ret, date *v, int *delta);
mtime_export str MTIMEdate_addmonths(date *ret, date *v, int *delta);
mtime_export str MTIMEdate_diff(int *ret, date *v1, date *v2);
mtime_export str MTIMEdate_diff_bulk(bat *ret, bat *bid1, bat *bid2);
mtime_export str MTIMEtimestamp_add(timestamp *ret, timestamp *v, lng *msecs);
mtime_export str MTIMEtimestamp_diff(lng *ret, timestamp *v1, timestamp *v2);
mtime_export str MTIMEtimestamp_diff_bulk(bat *ret, bat *bid1, bat *bid2);
mtime_export str MTIMEtimestamp_inside_dst(bit *ret, timestamp *p, tzone *z);

mtime_export str MTIMEtimestamp_year(int *ret, timestamp *t);
mtime_export str MTIMEtimestamp_month(int *ret, timestamp *t);
mtime_export str MTIMEtimestamp_day(int *ret, timestamp *t);
mtime_export str MTIMEtimestamp_hours(int *ret, timestamp *t);
mtime_export str MTIMEtimestamp_minutes(int *ret, timestamp *t);
mtime_export str MTIMEtimestamp_seconds(int *ret, timestamp *t);
mtime_export str MTIMEtimestamp_sql_seconds(int *ret, timestamp *t);
mtime_export str MTIMEtimestamp_milliseconds(int *ret, timestamp *t);
mtime_export str MTIMEsql_year(int *ret, int *t);
mtime_export str MTIMEsql_month(int *ret, int *t);
mtime_export str MTIMEsql_day(lng *ret, lng *t);
mtime_export str MTIMEsql_hours(int *ret, lng *t);
mtime_export str MTIMEsql_minutes(int *ret, lng *t);
mtime_export str MTIMEsql_seconds(int *ret, lng *t);

mtime_export str MTIMEtimestamp_LT(bit *retval, timestamp *val1, timestamp *val2);
mtime_export str MTIMEtimestamp_LE(bit *retval, timestamp *val1, timestamp *val2);
mtime_export str MTIMEtimestamp_GT(bit *retval, timestamp *val1, timestamp *val2);
mtime_export str MTIMEtimestamp_GE(bit *retval, timestamp *val1, timestamp *val2);
mtime_export str MTIMErule_tostr(str *s, rule *r);
mtime_export str MTIMErule_fromstr(rule *ret, str *s);
mtime_export str MTIMErule_create(rule *ret, int *month, int *day, int *weekday, int *minutes);
mtime_export str MTIMEtzone_create_dst(tzone *ret, int *minutes, rule *start, rule *end);
mtime_export str MTIMEtzone_create(tzone *ret, int *minutes);
mtime_export str MTIMEtzone_isnil(bit *retval, tzone *val);
mtime_export str MTIMErule_extract_month(int *ret, rule *r);
mtime_export str MTIMErule_extract_day(int *ret, rule *r);
mtime_export str MTIMErule_extract_weekday(int *ret, rule *r);
mtime_export str MTIMErule_extract_minutes(int *ret, rule *r);
mtime_export str MTIMEtzone_extract_start(rule *ret, tzone *t);
mtime_export str MTIMEtzone_extract_end(rule *ret, tzone *t);
mtime_export str MTIMEtzone_extract_minutes(int *ret, tzone *t);
mtime_export str MTIMEdate_sub_sec_interval_wrap(date *ret, date *t, int *sec);
mtime_export str MTIMEdate_sub_msec_interval_lng_wrap(date *ret, date *t, lng *msec);
mtime_export str MTIMEdate_add_sec_interval_wrap(date *ret, date *t, int *sec);
mtime_export str MTIMEdate_add_msec_interval_lng_wrap(date *ret, date *t, lng *msec);
mtime_export str MTIMEtimestamp_sub_msec_interval_lng_wrap(timestamp *ret, timestamp *t, lng *msec);
mtime_export str MTIMEtimestamp_sub_month_interval_wrap(timestamp *ret, timestamp *t, int *months);
mtime_export str MTIMEtimestamp_add_month_interval_wrap(timestamp *ret, timestamp *t, int *months);
mtime_export str MTIMEtime_sub_msec_interval_wrap(daytime *ret, daytime *t, lng *msec);
mtime_export str MTIMEtime_add_msec_interval_wrap(daytime *ret, daytime *t, lng *msec);
mtime_export str MTIMEcompute_rule_foryear(date *ret, rule *val, int *year);
mtime_export str MTIMEtzone_tostr(str *s, tzone *ret);
mtime_export str MTIMEtzone_fromstr(tzone *ret, str *s);
mtime_export str MTIMEdaytime_fromstr(daytime *ret, str *s);
mtime_export str MTIMEmsecs(lng *ret, int *d, int *h, int *m, int *s, int *ms);
mtime_export str MTIMEmsec(lng *r);
mtime_export str MTIMEdaytime1(daytime *ret, int *h);
mtime_export str MTIMEdaytime2(daytime *ret, int *h, int *m);
mtime_export str MTIMEdaytime3(daytime *ret, int *h, int *m, int *s);
mtime_export str MTIMEunix_epoch(timestamp *ret);
mtime_export str MTIMEepoch(timestamp *ret);
mtime_export str MTIMEepoch2int(int *res, timestamp *ts);
mtime_export str MTIMEtimestamp(timestamp *ret, int *sec);
mtime_export str MTIMEtimestamp_lng(timestamp *ret, lng *msecs);
mtime_export str MTIMEruleDef0(rule *ret, int *m, int *d, int *w, int *h, int *mint);
mtime_export str MTIMEruleDef1(rule *ret, int *m, str *dnme, int *w, int *h, int *mint);
mtime_export str MTIMEruleDef2(rule *ret, int *m, str *dnme, int *w, int *mint);
mtime_export str MTIMEcurrent_timestamp(timestamp *t);
mtime_export str MTIMEcurrent_date(date *d);
mtime_export str MTIMEcurrent_time(daytime *t);
mtime_export int date_fromstr(str buf, int *len, date **d);
mtime_export int date_tostr(str *buf, int *len, date *val);
mtime_export int daytime_fromstr(str buf, int *len, daytime **ret);
mtime_export int daytime_tostr(str *buf, int *len, daytime *val);
mtime_export int timestamp_fromstr(str buf, int *len, timestamp **ret);
mtime_export int timestamp_tostr(str *buf, int *len, timestamp *val);
mtime_export int tzone_fromstr(str buf, int *len, tzone **d);
mtime_export int tzone_tostr(str *buf, int *len, tzone *z);
mtime_export int rule_fromstr(str buf, int *len, rule **d);
mtime_export int rule_tostr(str *buf, int *len, rule *r);
mtime_export int tzone_fromstr(str buf, int *len, tzone **d);

mtime_export str MTIMEstrptime(date *d, str *s, str *format);
mtime_export str MTIMEstrftime(str *s, date *d, str *format);

mtime_export str MTIMEdate_extract_year_bulk(int *ret, int *bid);

mtime_export str MTIMEdate_extract_month_bulk(int *ret, int *bid);

mtime_export str MTIMEdate_extract_day_bulk(int *ret, int *bid);

mtime_export str MTIMEdaytime_extract_hours_bulk(int *ret, int *bid);

mtime_export str MTIMEdaytime_extract_minutes_bulk(int *ret, int *bid);

mtime_export str MTIMEdaytime_extract_seconds_bulk(int *ret, int *bid);

mtime_export str MTIMEdaytime_extract_sql_seconds_bulk(int *ret, int *bid);

mtime_export str MTIMEdaytime_extract_milliseconds_bulk(int *ret, int *bid);

mtime_export int TYPE_date;
mtime_export int TYPE_daytime;
mtime_export int TYPE_timestamp;
mtime_export int TYPE_tzone;
mtime_export int TYPE_rule;

#endif /* _MONETTIME_H_ */
