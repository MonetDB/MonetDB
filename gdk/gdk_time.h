/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _GDK_TIME_H_
#define _GDK_TIME_H_

#include <time.h>			/* for time_t */

typedef int date;			/* we use 26 bits out of 32 */
#define date_nil			((date) {int_nil})
#define is_date_nil(x)			((x) == date_nil)

typedef lng daytime;			/* we use 37 bits out of 64 */
#define daytime_nil			((daytime) {lng_nil})
#define is_daytime_nil(x)		((x) == daytime_nil)

typedef lng timestamp;			/* we use 26+37=63 bits out of 64 */
#define timestamp_nil			((timestamp) {lng_nil})
#define is_timestamp_nil(x)		((x) == timestamp_nil)

/* functions to manipulate date, daytime, and timestamp values */
#define HOUR_USEC	(60*60*LL_CONSTANT(1000000)) /* usec in an hour */
#define DAY_USEC	(24*HOUR_USEC) /* usec in a day */

gdk_export const timestamp unixepoch;

gdk_export date date_create(int year, int month, int day)
	__attribute__((__const__));
gdk_export date date_add_day(date dt, int days)
	__attribute__((__const__));
gdk_export date date_add_month(date dt, int months)
	__attribute__((__const__));
#define date_add_year(d, y)		date_add_month(d, (y) * 12)
gdk_export int date_diff(date d1, date d2)
	__attribute__((__const__));
gdk_export int date_dayofweek(date dt) /* Monday=1, Sunday=7 */
	__attribute__((__const__));
gdk_export int date_weekofyear(date dt)
	__attribute__((__const__));
gdk_export int date_usweekofyear(date dt)
	__attribute__((__const__));
gdk_export int date_dayofyear(date dt)
	__attribute__((__const__));
gdk_export int date_century(date dt)
	__attribute__((__const__));
gdk_export int date_decade(date dt)
	__attribute__((__const__));
gdk_export int date_year(date dt)
	__attribute__((__const__));
gdk_export int date_quarter(date dt)
	__attribute__((__const__));
gdk_export int date_month(date dt)
	__attribute__((__const__));
gdk_export int date_day(date dt)
	__attribute__((__const__));

gdk_export daytime daytime_create(int hour, int minute, int second, int usec)
	__attribute__((__const__));
gdk_export lng daytime_diff(daytime d1, daytime d2)
	__attribute__((__const__));
gdk_export daytime daytime_add_usec(daytime tm, lng usec)
	__attribute__((__const__));
#define daytime_add_sec(t, s)	daytime_add_usec(t, (s)*LL_CONSTANT(1000000))
#define daytime_add_min(t, m)			daytime_add_sec(t, (m) * 60)
#define daytime_add_hour(t, h)			daytime_add_min(t, (h) * 60)

/* add to daytime, but wrap around (modulo a full day) */
gdk_export daytime daytime_add_usec_modulo(daytime tm, lng usec)
	__attribute__((__const__));
#define daytime_add_sec_modulo(t, s)	daytime_add_usec_modulo(t, (s)*LL_CONSTANT(1000000))
#define daytime_add_min_modulo(t, m)	daytime_add_sec_modulo(t, (m) * 60)
#define daytime_add_hour_modulo(t, h)	daytime_add_min_modulo(t, (h) * 60)
gdk_export int daytime_hour(daytime tm)
	__attribute__((__const__));
gdk_export int daytime_min(daytime tm)
	__attribute__((__const__));
gdk_export int daytime_sec(daytime tm)
	__attribute__((__const__));
gdk_export int daytime_usec(daytime tm)
	__attribute__((__const__));
gdk_export int daytime_sec_usec(daytime tm)
	__attribute__((__const__));

gdk_export timestamp timestamp_fromtime(time_t timeval)
	__attribute__((__const__));
gdk_export timestamp timestamp_fromusec(lng usec)
	__attribute__((__const__));
gdk_export timestamp timestamp_fromdate(date dt)
	__attribute__((__const__));
gdk_export timestamp timestamp_create(date dt, daytime tm)
	__attribute__((__const__));
gdk_export timestamp timestamp_current(void);
gdk_export timestamp timestamp_add_usec(timestamp t, lng usec)
	__attribute__((__const__));
#define timestamp_add_sec(t, s)	timestamp_add_usec(t, (s)*LL_CONSTANT(1000000))
#define timestamp_add_min(t, m)		timestamp_add_sec(t, (m) * 60)
#define timestamp_add_hour(t, h)	timestamp_add_min(t, (h) * 60)
#define timestamp_add_day(t, d)		timestamp_add_hour(t, (d) * 24)
gdk_export timestamp timestamp_add_month(timestamp t, int m)
	__attribute__((__const__));
#define timestamp_add_year(t, y)	timestamp_add_month(t, (y) * 12)
gdk_export date timestamp_date(timestamp t)
	__attribute__((__const__));
gdk_export daytime timestamp_daytime(timestamp t)
	__attribute__((__const__));
gdk_export lng timestamp_diff(timestamp t1, timestamp t2)
	__attribute__((__const__));

/* interfaces for GDK level atoms date, daytime, and timestamp */
gdk_export ssize_t date_fromstr(const char *buf, size_t *len, date **d, bool external);
gdk_export ssize_t date_tostr(str *buf, size_t *len, const date *val, bool external);
gdk_export ssize_t daytime_tz_fromstr(const char *buf, size_t *len, daytime **d, bool external);
gdk_export ssize_t daytime_fromstr(const char *buf, size_t *len, daytime **d, bool external);
gdk_export ssize_t daytime_precision_tostr(str *buf, size_t *len, const daytime dt, int precision, bool external);
gdk_export ssize_t daytime_tostr(str *buf, size_t *len, const daytime *val, bool external);
gdk_export ssize_t timestamp_fromstr(const char *buf, size_t *len, timestamp **d, bool external);
gdk_export ssize_t timestamp_tz_fromstr(const char *buf, size_t *len, timestamp **ret, bool external);
gdk_export ssize_t timestamp_tostr(str *buf, size_t *len, const timestamp *val, bool external);
gdk_export ssize_t timestamp_precision_tostr(str *buf, size_t *len, timestamp val, int precision, bool external);

#endif	/* _GDK_TIME_H_ */
