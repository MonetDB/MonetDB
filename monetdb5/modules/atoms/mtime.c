/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/* In this file we implement three new types with supporting code.
 * The types are:
 *
 * - daytime - representing a time-of-day between 00:00:00 (included)
 *   and 24:00:00 (not included);
 * - date - representing a date between the year -4712 and 170050;
 * - timestamp - a combination of date and daytime, representing an
 *   exact point in time.
 *
 * Dates, both in the date and the timestamp types, are represented in
 * the so-called proleptic Gregorian calendar, that is to say, the
 * Gregorian calendar (which is in common use today) is extended
 * backwards.  See e.g.
 * <https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar>.
 *
 * Times, both in the daytime and the timestamp types, are recorded
 * with microsecond precision.
 *
 * Times and timestamps are all in UTC.  Conversion from the system
 * time zone where appropriate is done automatically.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_time.h"
#include "mtime.h"
#include "mal_exception.h"

#ifndef HAVE_STRPTIME
extern char *strptime(const char *, const char *, struct tm *);
#endif

/* interfaces callable from MAL, not used from any C code */
mal_export str MTIMEcurrent_date(date *ret);
mal_export str MTIMEcurrent_time(daytime *ret);
mal_export str MTIMEcurrent_timestamp(timestamp *ret);
mal_export str MTIMEdate_sub_msec_interval(date *ret, const date *d, const lng *ms);
mal_export str MTIMEdate_add_msec_interval(date *ret, const date *d, const lng *ms);
mal_export str MTIMEtimestamp_sub_msec_interval(timestamp *ret, const timestamp *t, const lng *ms);
mal_export str MTIMEtimestamp_add_msec_interval(timestamp *ret, const timestamp *t, const lng *ms);
mal_export str MTIMEtimestamp_sub_month_interval(timestamp *ret, const timestamp *t, const int *m);
mal_export str MTIMEtimestamp_add_month_interval(timestamp *ret, const timestamp *t, const int *m);
mal_export str MTIMEtime_sub_msec_interval(daytime *ret, const daytime *t, const lng *ms);
mal_export str MTIMEtime_add_msec_interval(daytime *ret, const daytime *t, const lng *ms);
mal_export str MTIMEdaytime_diff_msec(lng *ret, const daytime *t1, const daytime *t2);
mal_export str MTIMEdate_submonths(date *ret, const date *d, const int *m);
mal_export str MTIMEdate_addmonths(date *ret, const date *d, const int *m);
mal_export str MTIMEdate_extract_dayofyear(int *ret, const date *d);
mal_export str MTIMEdate_extract_weekofyear(int *ret, const date *d);
mal_export str MTIMEdate_extract_dayofweek(int *ret, const date *d);
mal_export str MTIMEtimestamp_century(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_decade(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_year(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_quarter(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_month(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_day(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_hours(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_minutes(int *ret, const timestamp *t);
mal_export str MTIMEsql_year(int *ret, const int *months);
mal_export str MTIMEsql_month(int *ret, const int *months);
mal_export str MTIMEsql_day(lng *ret, const lng *msecs);
mal_export str MTIMEsql_hours(int *ret, const lng *msecs);
mal_export str MTIMEsql_minutes(int *ret, const lng *msecs);
mal_export str MTIMEsql_seconds(int *ret, const lng *msecs);

mal_export str MTIMEdate_fromstr(date *ret, const char *const *s);
mal_export str MTIMEdate_date(date *dst, const date *src);
mal_export str MTIMEtimestamp_fromstr(timestamp *ret, const char *const *s);
mal_export str MTIMEtimestamp_timestamp(timestamp *dst, const timestamp *src);
mal_export str MTIMEseconds_since_epoch(int *ret, const timestamp *t);
mal_export str MTIMEdaytime_fromstr(daytime *ret, const char *const *s);
mal_export str MTIMEdaytime_daytime(daytime *dst, const daytime *src);
mal_export str MTIMEdaytime_fromseconds(daytime *ret, const lng *secs);
mal_export str MTIMEdaytime_fromseconds_bulk(bat *ret, bat *bid);
mal_export str MTIMElocal_timezone_msec(lng *ret);
mal_export str MTIMEstr_to_date(date *ret, const char *const *s, const char *const *format);
mal_export str MTIMEdate_to_str(str *ret, const date *d, const char *const *format);
mal_export str MTIMEstr_to_time(daytime *ret, const char *const *s, const char *const *format);
mal_export str MTIMEtime_to_str(str *ret, const daytime *d, const char *const *format);
mal_export str MTIMEstr_to_timestamp(timestamp *ret, const char *const *s, const char *const *format);
mal_export str MTIMEtimestamp_to_str(str *ret, const timestamp *d, const char *const *format);

str
MTIMEcurrent_date(date *ret)
{
	*ret = timestamp_date(timestamp_current());
	return MAL_SUCCEED;
}

str
MTIMEcurrent_time(daytime *ret)
{
	*ret = timestamp_daytime(timestamp_current());
	return MAL_SUCCEED;
}

str
MTIMEcurrent_timestamp(timestamp *ret)
{
	*ret = timestamp_current();
	return MAL_SUCCEED;
}

#define COPYFLAGS	do { bn->tsorted = b->tsorted; bn->trevsorted = b->trevsorted; } while (0)
#define SETFLAGS	do { bn->tsorted = bn->trevsorted = n < 2; } while (0)
#define func1(NAME, NAMEBULK, MALFUNC, INTYPE, OUTYPE, FUNC, SETFLAGS)	\
mal_export str NAME(OUTYPE *ret, const INTYPE *src);					\
mal_export str NAMEBULK(bat *ret, const bat *bid);						\
str																		\
NAME(OUTYPE *ret, const INTYPE *src)									\
{																		\
	*ret = FUNC(*src);													\
	return MAL_SUCCEED;													\
}																		\
str																		\
NAMEBULK(bat *ret, const bat *bid)										\
{																		\
	BAT *b, *bn;														\
	BUN n;																\
	const INTYPE *src;													\
	OUTYPE *dst;														\
																		\
	if ((b = BATdescriptor(*bid)) == NULL)								\
		throw(MAL, "batmtime." MALFUNC,									\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
	n = BATcount(b);													\
	if ((bn = COLnew(b->hseqbase, TYPE_##OUTYPE, n, TRANSIENT)) == NULL) { \
		BBPunfix(b->batCacheid);										\
		throw(MAL, "batmtime." MALFUNC, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
	}																	\
	src = Tloc(b, 0);													\
	dst = Tloc(bn, 0);													\
	for (BUN i = 0; i < n; i++) {										\
		dst[i] = FUNC(src[i]);											\
	}																	\
	bn->tnonil = b->tnonil;												\
	bn->tnil = b->tnil;													\
	BATsetcount(bn, n);													\
	SETFLAGS;															\
	bn->tkey = false;													\
	BBPunfix(b->batCacheid);											\
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}

#define func2(NAME, NAMEBULK, MALFUNC, INTYPE1, INTYPE2, OUTTYPE, FUNC)	\
mal_export str NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2); \
mal_export str NAMEBULK(bat *ret, const bat *bid1, const bat *bid2);	\
str																		\
NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2)				\
{																		\
	*ret = FUNC(*v1, *v2);												\
	return MAL_SUCCEED;													\
}																		\
str																		\
NAMEBULK(bat *ret, const bat *bid1, const bat *bid2)					\
{																		\
	BAT *b1, *b2, *bn;													\
	BUN n;																\
	const INTYPE1 *src1;												\
	const INTYPE2 *src2;												\
	OUTTYPE *dst;														\
																		\
	b1 = BATdescriptor(*bid1);											\
	b2 = BATdescriptor(*bid2);											\
	if (b1 == NULL || b2 == NULL) {										\
		if (b1)															\
			BBPunfix(b1->batCacheid);									\
		if (b2)															\
			BBPunfix(b2->batCacheid);									\
		throw(MAL, "batmtime." MALFUNC,									\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
	}																	\
	n = BATcount(b1);													\
	if (n != BATcount(b2)) {											\
		BBPunfix(b1->batCacheid);										\
		BBPunfix(b2->batCacheid);										\
		throw(MAL, "batmtime." MALFUNC, "inputs not the same size");	\
	}																	\
	if ((bn = COLnew(b1->hseqbase, TYPE_##OUTTYPE, n, TRANSIENT)) == NULL) { \
		BBPunfix(b1->batCacheid);										\
		BBPunfix(b2->batCacheid);										\
		throw(MAL, "batmtime." MALFUNC, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
	}																	\
	src1 = Tloc(b1, 0);													\
	src2 = Tloc(b2, 0);													\
	dst = Tloc(bn, 0);													\
	for (BUN i = 0; i < n; i++) {										\
		dst[i] = FUNC(src1[i], src2[i]);								\
	}																	\
	bn->tnonil = b1->tnonil & b2->tnonil;								\
	bn->tnil = b1->tnil | b2->tnil;										\
	BATsetcount(bn, n);													\
	bn->tsorted = n < 2;												\
	bn->trevsorted = n < 2;												\
	bn->tkey = false;													\
	BBPunfix(b1->batCacheid);											\
	BBPunfix(b2->batCacheid);											\
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}

func2(MTIMEdate_diff, MTIMEdate_diff_bulk, "diff", date, date, int, date_diff)

#define func2chk(NAME, NAMEBULK, MALFUNC, INTYPE1, INTYPE2, OUTTYPE, FUNC)	\
mal_export str NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2); \
mal_export str NAMEBULK(bat *ret, const bat *bid1, const bat *bid2);	\
str																		\
NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2)				\
{																		\
	if (is_##INTYPE1##_nil(*v1) || is_##INTYPE2##_nil(*v2))				\
		*ret = OUTTYPE##_nil;											\
	else {																\
		*ret = FUNC(*v1, *v2);											\
		if (is_##OUTTYPE##_nil(*ret))									\
			throw(MAL, "mtime." MALFUNC,								\
				  SQLSTATE(22003) "overflow in calculation");			\
	}																	\
	return MAL_SUCCEED;													\
}																		\
str																		\
NAMEBULK(bat *ret, const bat *bid1, const bat *bid2)					\
{																		\
	BAT *b1, *b2, *bn;													\
	BUN n;																\
	const INTYPE1 *src1;												\
	const INTYPE2 *src2;												\
	OUTTYPE *dst;														\
																		\
	b1 = BATdescriptor(*bid1);											\
	b2 = BATdescriptor(*bid2);											\
	if (b1 == NULL || b2 == NULL) {										\
		if (b1)															\
			BBPunfix(b1->batCacheid);									\
		if (b2)															\
			BBPunfix(b2->batCacheid);									\
		throw(MAL, "batmtime." MALFUNC,									\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
	}																	\
	n = BATcount(b1);													\
	if (n != BATcount(b2)) {											\
		BBPunfix(b1->batCacheid);										\
		BBPunfix(b2->batCacheid);										\
		throw(MAL, "batmtime." MALFUNC, "inputs not the same size");	\
	}																	\
	if ((bn = COLnew(b1->hseqbase, TYPE_##OUTTYPE, n, TRANSIENT)) == NULL) { \
		BBPunfix(b1->batCacheid);										\
		BBPunfix(b2->batCacheid);										\
		throw(MAL, "batmtime." MALFUNC, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
	}																	\
	src1 = Tloc(b1, 0);													\
	src2 = Tloc(b2, 0);													\
	dst = Tloc(bn, 0);													\
	bn->tnil = false;													\
	for (BUN i = 0; i < n; i++) {										\
		if (is_##INTYPE1##_nil(src1[i]) || is_##INTYPE2##_nil(src2[i])) { \
			dst[i] = OUTTYPE##_nil;										\
			bn->tnil = true;											\
		} else {														\
			dst[i] = FUNC(src1[i], src2[i]);							\
			if (is_##OUTTYPE##_nil(dst[i])) {							\
				BBPunfix(b1->batCacheid);								\
				BBPunfix(b2->batCacheid);								\
				BBPreclaim(bn);											\
				throw(MAL, "batmtime." MALFUNC,							\
					  SQLSTATE(22003) "overflow in calculation");		\
			}															\
		}																\
	}																	\
	bn->tnonil = !bn->tnil;												\
	BATsetcount(bn, n);													\
	bn->tsorted = n < 2;												\
	bn->trevsorted = n < 2;												\
	bn->tkey = false;													\
	BBPunfix(b1->batCacheid);											\
	BBPunfix(b2->batCacheid);											\
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}

str
MTIMEdate_sub_msec_interval(date *ret, const date *d, const lng *ms)
{
	if (is_date_nil(*d) || is_lng_nil(*ms))
		*ret = date_nil;
	else {
		*ret = date_add_day(*d, (int) (-*ms / (24*60*60*1000)));
		if (is_date_nil(*ret))
			throw(MAL, "mtime.date_sub_msec_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEdate_add_msec_interval(date *ret, const date *d, const lng *ms)
{
	if (is_date_nil(*d) || is_lng_nil(*ms))
		*ret = date_nil;
	else {
		*ret = date_add_day(*d, (int) (*ms / (24*60*60*1000)));
		if (is_date_nil(*ret))
			throw(MAL, "mtime.date_add_msec_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

#define TSSUBMS(ts, ms)		timestamp_add_usec((ts), -(ms) * 1000)
#define TSADDMS(ts, ms)		timestamp_add_usec((ts), (ms) * 1000)
func2chk(MTIMEtimestamp_sub_msec_interval, MTIMEtimestamp_sub_msec_interval_bulk, "timestamp_sub_msec_interval", timestamp, lng, timestamp, TSSUBMS)
func2chk(MTIMEtimestamp_add_msec_interval, MTIMEtimestamp_add_msec_interval_bulk, "timestamp_add_msec_interval", timestamp, lng, timestamp, TSADDMS)

str
MTIMEtimestamp_sub_month_interval(timestamp *ret, const timestamp *t, const int *m)
{
	if (is_timestamp_nil(*t) || is_int_nil(*m))
		*ret = timestamp_nil;
	else {
		*ret = timestamp_add_month(*t, -*m);
		if (is_timestamp_nil(*ret))
			throw(MAL, "mtime.timestamp_sub_month_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_add_month_interval(timestamp *ret, const timestamp *t, const int *m)
{
	if (is_timestamp_nil(*t) || is_int_nil(*m))
		*ret = timestamp_nil;
	else {
		*ret = timestamp_add_month(*t, *m);
		if (is_timestamp_nil(*ret))
			throw(MAL, "mtime.timestamp_add_month_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEtime_sub_msec_interval(daytime *ret, const daytime *t, const lng *ms)
{
	if (is_daytime_nil(*t) || is_lng_nil(*ms))
		*ret = daytime_nil;
	else {
		*ret = daytime_add_usec_modulo(*t, -*ms * 1000);
	}
	return MAL_SUCCEED;
}

str
MTIMEtime_add_msec_interval(daytime *ret, const daytime *t, const lng *ms)
{
	if (is_daytime_nil(*t) || is_lng_nil(*ms))
		*ret = daytime_nil;
	else {
		*ret = daytime_add_usec_modulo(*t, *ms * 1000);
	}
	return MAL_SUCCEED;
}

str
MTIMEdaytime_diff_msec(lng *ret, const daytime *t1, const daytime *t2)
{
	if (is_daytime_nil(*t1) || is_daytime_nil(*t2))
		*ret = lng_nil;
	else
		*ret = (*t1 - *t2) / 1000;
	return MAL_SUCCEED;
}

str
MTIMEdate_submonths(date *ret, const date *d, const int *m)
{
	if (is_date_nil(*d) || is_int_nil(*m))
		*ret = date_nil;
	else {
		*ret = date_add_month(*d, -*m);
		if (is_date_nil(*ret))
			throw(MAL, "mtime.date_sub_month_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEdate_addmonths(date *ret, const date *d, const int *m)
{
	if (is_date_nil(*d) || is_int_nil(*m))
		*ret = date_nil;
	else {
		*ret = date_add_month(*d, *m);
		if (is_date_nil(*ret))
			throw(MAL, "mtime.date_sub_month_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

func1(MTIMEdate_extract_century, MTIMEdate_extract_century_bulk, "century", date, int, date_century, COPYFLAGS)
func1(MTIMEdate_extract_decade, MTIMEdate_extract_decade_bulk, "decade", date, int, date_decade, COPYFLAGS)
func1(MTIMEdate_extract_year, MTIMEdate_extract_year_bulk, "year", date, int, date_year, COPYFLAGS)
func1(MTIMEdate_extract_quarter, MTIMEdate_extract_quarter_bulk, "quarter", date, int, date_quarter, SETFLAGS)
func1(MTIMEdate_extract_month, MTIMEdate_extract_month_bulk, "month", date, int, date_month, SETFLAGS)
func1(MTIMEdate_extract_day, MTIMEdate_extract_day_bulk, "day", date, int, date_day, SETFLAGS)
func1(MTIMEdaytime_extract_hours, MTIMEdaytime_extract_hours_bulk, "hours", daytime, int, daytime_hour, COPYFLAGS)
func1(MTIMEdaytime_extract_minutes, MTIMEdaytime_extract_minutes_bulk, "minutes", daytime, int, daytime_min, SETFLAGS)
func1(MTIMEdaytime_extract_sql_seconds, MTIMEdaytime_extract_sql_seconds_bulk, "seconds", daytime, int, daytime_sec_usec, SETFLAGS)

str
MTIMEdate_extract_dayofyear(int *ret, const date *d)
{
	*ret = date_dayofyear(*d);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_weekofyear(int *ret, const date *d)
{
	*ret = date_weekofyear(*d);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_dayofweek(int *ret, const date *d)
{
	*ret = date_dayofweek(*d);
	return MAL_SUCCEED;
}

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
func2(MTIMEtimestamp_diff_msec, MTIMEtimestamp_diff_msec_bulk, "diff", timestamp, timestamp, lng, TSDIFF)

str
MTIMEtimestamp_century(int *ret, const timestamp *t)
{
	if (is_timestamp_nil(*t)) {
		*ret = int_nil;
	} else {
		int y = date_year(timestamp_date(*t));
		if (y > 0)
			*ret = (y - 1) / 100 + 1;
		else
			*ret = -((-y - 1) / 100 + 1);
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_decade(int *ret, const timestamp *t)
{
	if (is_timestamp_nil(*t)) {
		*ret = int_nil;
	} else {
		*ret = date_year(timestamp_date(*t)) / 10;
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_year(int *ret, const timestamp *t)
{
	*ret = date_year(timestamp_date(*t));
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_quarter(int *ret, const timestamp *t)
{
	*ret = is_timestamp_nil(*t) ? int_nil : (date_month(timestamp_date(*t)) - 1) / 3 + 1;
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_month(int *ret, const timestamp *t)
{
	*ret = date_month(timestamp_date(*t));
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_day(int *ret, const timestamp *t)
{
	*ret = date_day(timestamp_date(*t));
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_hours(int *ret, const timestamp *t)
{
	*ret = daytime_hour(timestamp_daytime(*t));
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_minutes(int *ret, const timestamp *t)
{
	*ret = daytime_min(timestamp_daytime(*t));
	return MAL_SUCCEED;
}

#define timestamp_extract_usecond(ts)	daytime_sec_usec(timestamp_daytime(ts))
func1(MTIMEtimestamp_sql_seconds, MTIMEtimestamp_sql_seconds_bulk, "sql_seconds", timestamp, int, timestamp_extract_usecond, SETFLAGS)

str
MTIMEsql_year(int *ret, const int *months)
{
	*ret = is_int_nil(*months) ? int_nil : *months / 12;
	return MAL_SUCCEED;
}

str
MTIMEsql_month(int *ret, const int *months)
{
	*ret = is_int_nil(*months) ? int_nil : *months % 12;
	return MAL_SUCCEED;
}

str
MTIMEsql_day(lng *ret, const lng *msecs)
{
	*ret = is_lng_nil(*msecs) ? lng_nil : *msecs / (24*60*60*1000);
	return MAL_SUCCEED;
}

str
MTIMEsql_hours(int *ret, const lng *msecs)
{
	*ret = is_lng_nil(*msecs) ? int_nil : (int) ((*msecs % (24*60*60*1000)) / (60*60*1000));
	return MAL_SUCCEED;
}

str
MTIMEsql_minutes(int *ret, const lng *msecs)
{
	*ret = is_lng_nil(*msecs) ? int_nil : (int) ((*msecs % (60*60*1000)) / (60*1000));
	return MAL_SUCCEED;
}

str
MTIMEsql_seconds(int *ret, const lng *msecs)
{
	*ret = is_lng_nil(*msecs) ? int_nil : (int) ((*msecs % (60*1000)) / 1000);
	return MAL_SUCCEED;
}

str
MTIMEdate_fromstr(date *ret, const char *const *s)
{
	if (date_fromstr(*s, &(size_t){sizeof(date)}, &ret, true) < 0)
		throw(MAL, "cald.date", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
MTIMEdate_date(date *dst, const date *src)
{
	*dst = *src;
	return MAL_SUCCEED;
}

func1(MTIMEtimestamp_extract_date, MTIMEtimestamp_extract_date_bulk, "date", timestamp, date, timestamp_date, COPYFLAGS)

str
MTIMEtimestamp_fromstr(timestamp *ret, const char *const *s)
{
	if (timestamp_fromstr(*s, &(size_t){sizeof(timestamp)}, &ret, true) < 0)
		throw(MAL, "calc.timestamp", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_timestamp(timestamp *dst, const timestamp *src)
{
	*dst = *src;
	return MAL_SUCCEED;
}

#define mkts(dt)	timestamp_create(dt, daytime_create(0, 0, 0, 0))
func1(MTIMEtimestamp_fromdate, MTIMEtimestamp_fromdate_bulk, "timestamp", date, timestamp, mkts, COPYFLAGS)

str
MTIMEseconds_since_epoch(int *ret, const timestamp *t)
{
	lng df = timestamp_diff(*t, unixepoch);
	*ret = is_lng_nil(df) ? int_nil : (int) (df / 1000000);
	return MAL_SUCCEED;
}

#define mktsfromsec(sec)	(is_int_nil(sec) ?							\
							 timestamp_nil :							\
							 timestamp_add_usec(unixepoch,				\
												(sec) * LL_CONSTANT(1000000)))
#define mktsfrommsec(msec)	(is_lng_nil(msec) ?							\
							 timestamp_nil :							\
							 timestamp_add_usec(unixepoch,				\
												(msec) * LL_CONSTANT(1000)))
func1(MTIMEtimestamp_fromsecond, MTIMEtimestamp_fromsecond_bulk, "timestamp", int, timestamp, mktsfromsec, COPYFLAGS)
func1(MTIMEtimestamp_frommsec, MTIMEtimestamp_frommsec_bulk, "timestamp", lng, timestamp, mktsfrommsec, COPYFLAGS)

str
MTIMEdaytime_fromstr(daytime *ret, const char *const *s)
{
	if (daytime_fromstr(*s, &(size_t){sizeof(daytime)}, &ret, true) < 0)
		throw(MAL, "calc.daytime", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_daytime(daytime *dst, const daytime *src)
{
	*dst = *src;
	return MAL_SUCCEED;
}

str
MTIMEdaytime_fromseconds(daytime *ret, const lng *secs)
{
	if (is_lng_nil(*secs))
		*ret = daytime_nil;
	else if (*secs < 0 || *secs >= 24*60*60)
		throw(MAL, "calc.daytime", SQLSTATE(42000) ILLEGAL_ARGUMENT);
	else
		*ret = (daytime) (*secs * 1000000);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_fromseconds_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const lng *s;
	daytime *d;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.daytime", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_daytime, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.daytime", SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	s = Tloc(b, 0);
	d = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_lng_nil(s[i])) {
			bn->tnil = true;
			d[i] = daytime_nil;
		} else if (s[i] < 0 || s[i] >= 24*60*60) {
			BBPunfix(b->batCacheid);
			BBPreclaim(bn);
			throw(MAL, "batcalc.daytime", SQLSTATE(42000) ILLEGAL_ARGUMENT);
		} else {
			d[i] = (daytime) (s[i] * 1000000);
		}
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = b->tsorted;
	bn->trevsorted = b->trevsorted;
	bn->tkey = false;
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

func1(MTIMEtimestamp_extract_daytime, MTIMEtimestamp_extract_daytime_bulk, "daytime", timestamp, daytime, timestamp_daytime, SETFLAGS)

/* return current system time zone offset in seconds East of Greenwich */
static int
local_timezone(int *isdstp)
{
	int tzone = 0;
	int isdst = -1;

#if defined(_MSC_VER)
	DYNAMIC_TIME_ZONE_INFORMATION tzinf;

	/* documentation says: UTC = localtime + Bias (in minutes),
	 * but experimentation during DST period says, UTC = localtime
	 * + Bias + DaylightBias, and presumably during non DST
	 * period, UTC = localtime + Bias */
	switch (GetDynamicTimeZoneInformation(&tzinf)) {
	case TIME_ZONE_ID_STANDARD:	/* using standard time */
	case TIME_ZONE_ID_UNKNOWN:	/* no daylight saving time in this zone */
		isdst = 0;
		tzone = -(int) tzinf.Bias * 60;
		break;
	case TIME_ZONE_ID_DAYLIGHT:	/* using daylight saving time */
		isdst = 1;
		tzone = -(int) (tzinf.Bias + tzinf.DaylightBias) * 60;
		break;
	default:			/* aka TIME_ZONE_ID_INVALID */
		/* call failed, we don't know the time zone */
		tzone = 0;
		break;
	}
#elif defined(HAVE_STRUCT_TM_TM_ZONE)
	time_t t;
	struct tm tm = (struct tm) {0};

	if ((t = time(NULL)) != (time_t) -1 && localtime_r(&t, &tm)) {
		tzone = (int) tm.tm_gmtoff;
		isdst = tm.tm_isdst;
	}
#else
	time_t t;
	struct tm tm = (struct tm) {0};

	if ((t = time(NULL)) != (time_t) -1 && gmtime_r(&t, &tm)) {
		timestamp lt, gt;
		gt = timestamp_create(date_create(tm.tm_year + 1900,
										  tm.tm_mon + 1,
										  tm.tm_mday),
							  daytime_create(tm.tm_hour,
											 tm.tm_min,
											 tm.tm_sec == 60 ? 59 : tm.tm_sec,
											 0));
		if (localtime_r(&t, &tm)) {
			isdst = tm.tm_isdst;
			lt = timestamp_create(date_create(tm.tm_year + 1900,
											  tm.tm_mon + 1,
											  tm.tm_mday),
								  daytime_create(tm.tm_hour,
												 tm.tm_min,
												 tm.tm_sec == 60 ? 59 : tm.tm_sec,
												 0));
			tzone = (int) (timestamp_diff(lt, gt) / 1000000);
		}
	}
#endif
	if (isdstp)
		*isdstp = isdst;
	return tzone;
}

str
MTIMElocal_timezone_msec(lng *ret)
{
	int tzone = local_timezone(NULL);
	*ret = (lng) tzone * 1000;
	return MAL_SUCCEED;
}

static str
timestamp_to_str(str *ret, const timestamp *d, const char *const *format,
				 const char *type, const char *malfunc)
{
	char buf[512];
	date dt;
	daytime t;
	struct tm tm;

	if (is_timestamp_nil(*d) || strNil(*format)) {
		*ret = GDKstrdup(str_nil);
		if (*ret == NULL)
			throw(MAL, malfunc, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	dt = timestamp_date(*d);
	t = timestamp_daytime(*d);
	tm = (struct tm) {
		.tm_year = date_year(dt) - 1900,
		.tm_mon = date_month(dt) - 1,
		.tm_mday = date_day(dt),
		.tm_wday = date_dayofweek(dt) % 7,
		.tm_yday = date_dayofyear(dt) - 1,
		.tm_hour = daytime_hour(t),
		.tm_min = daytime_min(t),
		.tm_sec = daytime_sec(t),
	};
	if (strftime(buf, sizeof(buf), *format, &tm) == 0)
		throw(MAL, malfunc, "cannot convert %s", type);
	*ret = GDKstrdup(buf);
	if (*ret == NULL)
		throw(MAL, malfunc, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

static str
str_to_timestamp(timestamp *ret, const char *const *s, const char *const *format, const char *type, const char *malfunc)
{
	struct tm tm = (struct tm) {0};
	time_t t;

	if (strNil(*s) || strNil(*format)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	t = time(NULL);
	localtime_r(&t, &tm);
	tm.tm_sec = tm.tm_min = tm.tm_hour = 0;
	tm.tm_isdst = -1;
	if (strptime(*s, *format, &tm) == NULL)
		throw(MAL, malfunc,
			  "format '%s', doesn't match %s '%s'", *format, type, *s);
	*ret = timestamp_create(date_create(tm.tm_year + 1900,
										tm.tm_mon + 1,
										tm.tm_mday),
							daytime_create(tm.tm_hour,
										   tm.tm_min,
										   tm.tm_sec == 60 ? 59 : tm.tm_sec,
										   0));
	/* if strptime filled in DST information (tm_isdst >= 0), then the
	 * time is in system local time and we convert to GMT by
	 * subtracting the time zone offset, else we don't touch the time
	 * returned because it is assumed to already be in GMT */
	if (tm.tm_isdst >= 0) {
		int isdst = 0;
		int tz = local_timezone(&isdst);
		/* if strptime's information doesn't square with our own
		 * information about having or not having DST, we compensate
		 * an hour */
		if (tm.tm_isdst > 0 && isdst == 0) {
			tz += 3600;
		} else if (tm.tm_isdst == 0 && isdst > 0) {
			tz -= 3600;
		}

		*ret = timestamp_add_usec(*ret, -tz * LL_CONSTANT(1000000));
	}
	if (is_timestamp_nil(*ret))
		throw(MAL, malfunc, "bad %s '%s'", type, *s);
	return MAL_SUCCEED;
}

str
MTIMEstr_to_date(date *ret, const char *const *s, const char *const *format)
{
	timestamp ts;
	str msg = str_to_timestamp(&ts, s, format, "date", "mtime.str_to_date");
	if (msg != MAL_SUCCEED)
		return msg;
	*ret = timestamp_date(ts);
	return MAL_SUCCEED;
}

str
MTIMEdate_to_str(str *ret, const date *d, const char *const *format)
{
	timestamp ts = timestamp_create(*d, timestamp_daytime(timestamp_current()));
	return timestamp_to_str(ret, &ts, format, "date", "mtime.date_to_str");
}

str
MTIMEstr_to_time(daytime *ret, const char *const *s, const char *const *format)
{
	timestamp ts;
	str msg = str_to_timestamp(&ts, s, format, "time", "mtime.str_to_time");
	if (msg != MAL_SUCCEED)
		return msg;
	*ret = timestamp_daytime(ts);
	return MAL_SUCCEED;
}

str
MTIMEtime_to_str(str *ret, const daytime *d, const char *const *format)
{
	timestamp ts = timestamp_create(timestamp_date(timestamp_current()), *d);
	return timestamp_to_str(ret, &ts, format, "time", "mtime.time_to_str");
}

str
MTIMEstr_to_timestamp(timestamp *ret, const char *const *s, const char *const *format)
{
	return str_to_timestamp(ret, s, format, "timestamp", "mtime.str_to_timestamp");
}

str
MTIMEtimestamp_to_str(str *ret, const timestamp *d, const char *const *format)
{
	return timestamp_to_str(ret, d, format,
							"timestamp", "mtime.timestamp_to_str");
}
