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
mal_export str MTIMElocal_timezone_msec(lng *ret);

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

#define is_str_nil strNil

#define COPYFLAGS	do { bn->tsorted = b->tsorted; bn->trevsorted = b->trevsorted; } while (0)
#define SETFLAGS	do { bn->tsorted = bn->trevsorted = n < 2; } while (0)
#define func1(NAME, NAMEBULK, MALFUNC, INTYPE, OUTYPE, FUNC, SETFLAGS, FUNC_CALL)	\
mal_export str NAME(OUTYPE *ret, const INTYPE *src);					\
mal_export str NAMEBULK(bat *ret, const bat *bid);						\
str																		\
NAME(OUTYPE *ret, const INTYPE *src)									\
{																		\
	str msg = MAL_SUCCEED; 												\
	do {																\
		FUNC_CALL(FUNC, (*ret), *src);									\
	} while (0);														\
	return msg;															\
}																		\
str																		\
NAMEBULK(bat *ret, const bat *bid)										\
{																		\
	BAT *b = NULL, *bn = NULL;											\
	BUN n;																\
	const INTYPE *src;													\
	OUTYPE *dst;														\
	str msg = MAL_SUCCEED; 												\
																		\
	if ((b = BATdescriptor(*bid)) == NULL)	{							\
		msg = createException(MAL, "batmtime." MALFUNC,					\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
		goto bailout;													\
	}																	\
	n = BATcount(b);													\
	if ((bn = COLnew(b->hseqbase, TYPE_##OUTYPE, n, TRANSIENT)) == NULL) { \
		msg = createException(MAL, "batmtime." MALFUNC, 				\
			  SQLSTATE(HY013) MAL_MALLOC_FAIL); 						\
		goto bailout;													\
	}																	\
	src = Tloc(b, 0);													\
	dst = Tloc(bn, 0);													\
	for (BUN i = 0; i < n; i++) {										\
		FUNC_CALL(FUNC, (dst[i]), src[i]);								\
	}																	\
	bn->tnonil = b->tnonil;												\
	bn->tnil = b->tnil;													\
	BATsetcount(bn, n);													\
	SETFLAGS;															\
	bn->tkey = false;													\
bailout: 																\
	if (b)																\
		BBPunfix(b->batCacheid);										\
	if (msg && bn)														\
		BBPreclaim(bn);													\
	else if (bn) 														\
		BBPkeepref(*ret = bn->batCacheid);								\
	return msg;															\
}

#define func1_noexcept(FUNC, RET, PARAM) RET = FUNC(PARAM)
#define func1_except(FUNC, RET, PARAM) msg = FUNC(&RET, PARAM); if (msg) break;

#define func2(NAME, NAMEBULK, MALFUNC, INTYPE1, INTYPE2, OUTTYPE, FUNC, FUNC_CALL)	\
mal_export str NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2); \
mal_export str NAMEBULK(bat *ret, const bat *bid1, const bat *bid2);	\
mal_export str NAMEBULK##_p1(bat *ret, const INTYPE1 *src1, const bat *bid2);	\
mal_export str NAMEBULK##_p2(bat *ret, const bat *bid1, const INTYPE2 *src2);	\
str																		\
NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2)				\
{																		\
	str msg = MAL_SUCCEED; 												\
	do {																\
		FUNC_CALL(FUNC, (*ret), *v1, *v2);								\
	} while (0);														\
	return msg;															\
}																		\
str																		\
NAMEBULK(bat *ret, const bat *bid1, const bat *bid2)					\
{																		\
	BAT *b1 = NULL, *b2 = NULL, *bn = NULL;								\
	BUN n;																\
	const INTYPE1 *src1;												\
	const INTYPE2 *src2;												\
	OUTTYPE *dst;														\
	str msg = MAL_SUCCEED; 												\
																		\
	b1 = BATdescriptor(*bid1);											\
	b2 = BATdescriptor(*bid2);											\
	if (b1 == NULL || b2 == NULL) {										\
		msg = createException(MAL, "batmtime." MALFUNC,					\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
		goto bailout;													\
	}																	\
	n = BATcount(b1);													\
	if (n != BATcount(b2)) {											\
		msg = createException(MAL, "batmtime." MALFUNC, 				\
			  "inputs not the same size");								\
		goto bailout;													\
	}																	\
	if ((bn = COLnew(b1->hseqbase, TYPE_##OUTTYPE, n, TRANSIENT)) == NULL) { \
		msg = createException(MAL, "batmtime." MALFUNC, 				\
			  SQLSTATE(HY013) MAL_MALLOC_FAIL); 						\
		goto bailout;													\
	}																	\
	src1 = Tloc(b1, 0);													\
	src2 = Tloc(b2, 0);													\
	dst = Tloc(bn, 0);													\
	for (BUN i = 0; i < n; i++) {										\
		FUNC_CALL(FUNC, (dst[i]), src1[i], src2[i]);					\
	}																	\
	bn->tnonil = b1->tnonil & b2->tnonil;								\
	bn->tnil = b1->tnil | b2->tnil;										\
	BATsetcount(bn, n);													\
	bn->tsorted = n < 2;												\
	bn->trevsorted = n < 2;												\
	bn->tkey = false;													\
bailout: 																\
	if (b1)																\
		BBPunfix(b1->batCacheid);										\
	if (b2) 															\
		BBPunfix(b2->batCacheid);										\
	if (msg && bn)														\
		BBPreclaim(bn);													\
	else if (bn) 														\
		BBPkeepref(*ret = bn->batCacheid);								\
	return msg;															\
}																		\
str																		\
NAMEBULK##_p1(bat *ret, const INTYPE1 *src1, const bat *bid2)			\
{																		\
	BAT *b2 = NULL, *bn = NULL;											\
	BUN n;																\
	const INTYPE2 *src2;												\
	OUTTYPE *dst;														\
	str msg = MAL_SUCCEED; 												\
																		\
	if ((b2 = BATdescriptor(*bid2)) == NULL) {							\
		msg = createException(MAL, "batmtime." MALFUNC,					\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
		goto bailout;													\
	}																	\
	n = BATcount(b2);													\
	if ((bn = COLnew(b2->hseqbase, TYPE_##OUTTYPE, n, TRANSIENT)) == NULL) { \
		msg = createException(MAL, "batmtime." MALFUNC, 				\
			  SQLSTATE(HY013) MAL_MALLOC_FAIL); 						\
		goto bailout;													\
	}																	\
	src2 = Tloc(b2, 0);													\
	dst = Tloc(bn, 0);													\
	for (BUN i = 0; i < n; i++) {										\
		FUNC_CALL(FUNC, (dst[i]), (*src1), src2[i]);					\
	}																	\
	bn->tnonil = !is_##INTYPE1##_nil(*src1) && b2->tnonil;				\
	bn->tnil = is_##INTYPE1##_nil(*src1) || b2->tnil;					\
	BATsetcount(bn, n);													\
	bn->tsorted = n < 2;												\
	bn->trevsorted = n < 2;												\
	bn->tkey = false;													\
bailout: 																\
	if (b2) 															\
		BBPunfix(b2->batCacheid);										\
	if (msg && bn)														\
		BBPreclaim(bn);													\
	else if (bn) 														\
		BBPkeepref(*ret = bn->batCacheid);								\
	return msg;															\
}																		\
str																		\
NAMEBULK##_p2(bat *ret, const bat *bid1, const INTYPE2 *src2)			\
{																		\
	BAT *b1 = NULL, *bn = NULL;											\
	BUN n;																\
	const INTYPE1 *src1;												\
	OUTTYPE *dst;														\
	str msg = MAL_SUCCEED; 												\
																		\
	if ((b1 = BATdescriptor(*bid1)) == NULL) {							\
		msg = createException(MAL, "batmtime." MALFUNC,					\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
		goto bailout;													\
	}																	\
	n = BATcount(b1);													\
	if ((bn = COLnew(b1->hseqbase, TYPE_##OUTTYPE, n, TRANSIENT)) == NULL) { \
		msg = createException(MAL, "batmtime." MALFUNC, 				\
			  SQLSTATE(HY013) MAL_MALLOC_FAIL); 						\
		goto bailout;													\
	}																	\
	src1 = Tloc(b1, 0);													\
	dst = Tloc(bn, 0);													\
	for (BUN i = 0; i < n; i++) {										\
		FUNC_CALL(FUNC, (dst[i]), src1[i], (*src2));					\
	}																	\
	bn->tnonil = b1->tnonil && !is_##INTYPE2##_nil(*src2);				\
	bn->tnil = b1->tnil || is_##INTYPE2##_nil(*src2);					\
	BATsetcount(bn, n);													\
	bn->tsorted = n < 2;												\
	bn->trevsorted = n < 2;												\
	bn->tkey = false;													\
bailout: 																\
	if (b1) 															\
		BBPunfix(b1->batCacheid);										\
	if (msg && bn)														\
		BBPreclaim(bn);													\
	else if (bn) 														\
		BBPkeepref(*ret = bn->batCacheid);								\
	return msg;															\
}																		\

#define func2_noexcept(FUNC, RET, PARAM1, PARAM2) RET = FUNC(PARAM1, PARAM2)
#define func2_except(FUNC, RET, PARAM1, PARAM2) msg = FUNC(&RET, PARAM1, PARAM2); if (msg) break;

func2(MTIMEdate_diff, MTIMEdate_diff_bulk, "diff", date, date, int, date_diff, func2_noexcept)
func2(MTIMEdaytime_diff_msec, MTIMEdaytime_diff_msec_bulk, "diff", daytime, daytime, lng, daytime_diff, func2_noexcept)

#define func2chk(NAME, NAMEBULK, MALFUNC, INTYPE1, INTYPE2, OUTTYPE, FUNC)	\
mal_export str NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2); \
mal_export str NAMEBULK(bat *ret, const bat *bid1, const bat *bid2);	\
mal_export str NAMEBULK##_p1(bat *ret, const INTYPE1 *v1, const bat *bid2);	\
mal_export str NAMEBULK##_p2(bat *ret, const bat *bid1, const INTYPE2 *v2);	\
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
}																		\
str																		\
NAMEBULK##_p1(bat *ret, const INTYPE1 *v1, const bat *bid2)				\
{																		\
	BAT *b2, *bn;														\
	BUN n;																\
	const INTYPE2 *src2;												\
	OUTTYPE *dst;														\
																		\
	if ((b2 = BATdescriptor(*bid2)) == NULL) {							\
		throw(MAL, "batmtime." MALFUNC,									\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
	}																	\
	n = BATcount(b2);													\
	if ((bn = COLnew(b2->hseqbase, TYPE_##OUTTYPE, n, TRANSIENT)) == NULL) { \
		BBPunfix(b2->batCacheid);										\
		throw(MAL, "batmtime." MALFUNC, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
	}																	\
	src2 = Tloc(b2, 0);													\
	dst = Tloc(bn, 0);													\
	bn->tnil = false;													\
	for (BUN i = 0; i < n; i++) {										\
		if (is_##INTYPE1##_nil(*v1) || is_##INTYPE2##_nil(src2[i])) {	\
			dst[i] = OUTTYPE##_nil;										\
			bn->tnil = true;											\
		} else {														\
			dst[i] = FUNC((*v1), src2[i]);								\
			if (is_##OUTTYPE##_nil(dst[i])) {							\
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
	BBPunfix(b2->batCacheid);											\
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}																		\
str																		\
NAMEBULK##_p2(bat *ret, const bat *bid1, const INTYPE2 *v2)				\
{																		\
	BAT *b1, *bn;														\
	BUN n;																\
	const INTYPE1 *src1;												\
	OUTTYPE *dst;														\
																		\
	if ((b1 = BATdescriptor(*bid1)) == NULL) {							\
		throw(MAL, "batmtime." MALFUNC,									\
			  SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);					\
	}																	\
	n = BATcount(b1);													\
	if ((bn = COLnew(b1->hseqbase, TYPE_##OUTTYPE, n, TRANSIENT)) == NULL) { \
		BBPunfix(b1->batCacheid);										\
		throw(MAL, "batmtime." MALFUNC, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
	}																	\
	src1 = Tloc(b1, 0);													\
	dst = Tloc(bn, 0);													\
	bn->tnil = false;													\
	for (BUN i = 0; i < n; i++) {										\
		if (is_##INTYPE1##_nil(src1[i]) || is_##INTYPE2##_nil(*v2)) {	\
			dst[i] = OUTTYPE##_nil;										\
			bn->tnil = true;											\
		} else {														\
			dst[i] = FUNC(src1[i], (*v2));								\
			if (is_##OUTTYPE##_nil(dst[i])) {							\
				BBPunfix(b1->batCacheid);								\
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
	BBPkeepref(*ret = bn->batCacheid);									\
	return MAL_SUCCEED;													\
}
#define date_sub_msec_interval(d, ms) date_add_day(d, (int) (-ms / (24*60*60*1000)))
#define date_add_msec_interval(d, ms) date_add_day(d, (int) (ms / (24*60*60*1000)))
func2chk(MTIMEdate_sub_msec_interval, MTIMEdate_sub_msec_interval_bulk, "date_sub_msec_interval", date, lng, date, date_sub_msec_interval)
func2chk(MTIMEdate_add_msec_interval, MTIMEdate_add_msec_interval_bulk, "date_add_msec_interval", date, lng, date, date_add_msec_interval)

#define TSSUBMS(ts, ms)		timestamp_add_usec((ts), -(ms) * 1000)
#define TSADDMS(ts, ms)		timestamp_add_usec((ts), (ms) * 1000)
func2chk(MTIMEtimestamp_sub_msec_interval, MTIMEtimestamp_sub_msec_interval_bulk, "timestamp_sub_msec_interval", timestamp, lng, timestamp, TSSUBMS)
func2chk(MTIMEtimestamp_add_msec_interval, MTIMEtimestamp_add_msec_interval_bulk, "timestamp_add_msec_interval", timestamp, lng, timestamp, TSADDMS)

#define timestamp_sub_month_interval(d, m) timestamp_add_month(d, -m)
#define timestamp_add_month_interval(d, m) timestamp_add_month(d, m)
func2chk(MTIMEtimestamp_sub_month_interval, MTIMEtimestamp_sub_month_interval_bulk, "timestamp_sub_month_interval", timestamp, int, timestamp, timestamp_sub_month_interval)
func2chk(MTIMEtimestamp_add_month_interval, MTIMEtimestamp_add_month_interval_bulk, "timestamp_add_month_interval", timestamp, int, timestamp, timestamp_add_month_interval)

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
func2(MTIMEtime_sub_msec_interval, MTIMEtime_sub_msec_interval_bulk, "time_sub_msec_interval", daytime, lng, daytime, time_sub_msec_interval, func2_noexcept)
func2(MTIMEtime_add_msec_interval, MTIMEtime_add_msec_interval_bulk, "time_add_msec_interval", daytime, lng, daytime, time_add_msec_interval, func2_noexcept)

#define date_submonths(d, m) date_add_month(d, -m)
#define date_addmonths(d, m) date_add_month(d, m)
func2chk(MTIMEdate_submonths, MTIMEdate_submonths_bulk, "date_submonths", date, int, date, date_submonths)
func2chk(MTIMEdate_addmonths, MTIMEdate_addmonths_bulk, "date_addmonths", date, int, date, date_addmonths)

func1(MTIMEdate_extract_century, MTIMEdate_extract_century_bulk, "date_century", date, int, date_century, COPYFLAGS, func1_noexcept)
func1(MTIMEdate_extract_decade, MTIMEdate_extract_decade_bulk, "date_decade", date, int, date_decade, COPYFLAGS, func1_noexcept)
func1(MTIMEdate_extract_year, MTIMEdate_extract_year_bulk, "date_year", date, int, date_year, COPYFLAGS, func1_noexcept)
func1(MTIMEdate_extract_quarter, MTIMEdate_extract_quarter_bulk, "date_quarter", date, int, date_quarter, SETFLAGS, func1_noexcept)
func1(MTIMEdate_extract_month, MTIMEdate_extract_month_bulk, "date_month", date, int, date_month, SETFLAGS, func1_noexcept)
func1(MTIMEdate_extract_day, MTIMEdate_extract_day_bulk, "date_day", date, int, date_day, SETFLAGS, func1_noexcept)
func1(MTIMEdate_extract_dayofyear, MTIMEdate_extract_dayofyear_bulk, "date_dayofyear", date, int, date_dayofyear, SETFLAGS, func1_noexcept)
func1(MTIMEdate_extract_weekofyear, MTIMEdate_extract_weekofyear_bulk, "date_weekofyear", date, int, date_weekofyear, SETFLAGS, func1_noexcept)
func1(MTIMEdate_extract_dayofweek, MTIMEdate_extract_dayofweek_bulk, "date_dayofweek", date, int, date_dayofweek, SETFLAGS, func1_noexcept)
func1(MTIMEdaytime_extract_hours, MTIMEdaytime_extract_hours_bulk, "daytime_hour", daytime, int, daytime_hour, COPYFLAGS, func1_noexcept)
func1(MTIMEdaytime_extract_minutes, MTIMEdaytime_extract_minutes_bulk, "daytime_minutes", daytime, int, daytime_min, SETFLAGS, func1_noexcept)
func1(MTIMEdaytime_extract_sql_seconds, MTIMEdaytime_extract_sql_seconds_bulk, "daytime_seconds", daytime, int, daytime_sec_usec, SETFLAGS, func1_noexcept)

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
func2(MTIMEtimestamp_diff_msec, MTIMEtimestamp_diff_msec_bulk, "diff", timestamp, timestamp, lng, TSDIFF, func2_noexcept)

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
func1(MTIMEtimestamp_century, MTIMEtimestamp_century_bulk, "timestamp_century", timestamp, int, timestamp_century, COPYFLAGS, func1_noexcept)
func1(MTIMEtimestamp_decade, MTIMEtimestamp_decade_bulk, "timestamp_decade", timestamp, int, timestamp_decade, COPYFLAGS, func1_noexcept)
func1(MTIMEtimestamp_year, MTIMEtimestamp_year_bulk, "timestamp_year", timestamp, int, timestamp_year, COPYFLAGS, func1_noexcept)
func1(MTIMEtimestamp_quarter, MTIMEtimestamp_quarter_bulk, "timestamp_quarter", timestamp, int, timestamp_quarter, SETFLAGS, func1_noexcept)
func1(MTIMEtimestamp_month, MTIMEtimestamp_month_bulk, "timestamp_month", timestamp, int, timestamp_month, SETFLAGS, func1_noexcept)
func1(MTIMEtimestamp_day, MTIMEtimestamp_day_bulk, "timestamp_day", timestamp, int, timestamp_day, SETFLAGS, func1_noexcept)
func1(MTIMEtimestamp_hours, MTIMEtimestamp_hours_bulk, "timestamp_hours", timestamp, int, timestamp_hours, SETFLAGS, func1_noexcept)
func1(MTIMEtimestamp_minutes, MTIMEtimestamp_minutes_bulk, "timestamp_minutes", timestamp, int, timestamp_minutes, SETFLAGS, func1_noexcept)
func1(MTIMEtimestamp_sql_seconds, MTIMEtimestamp_sql_seconds_bulk, "sql_seconds", timestamp, int, timestamp_extract_usecond, SETFLAGS, func1_noexcept)

#define sql_year(m) is_int_nil(m) ? int_nil : m / 12
#define sql_month(m) is_int_nil(m) ? int_nil : m % 12
#define sql_day(m) is_lng_nil(m) ? lng_nil : m / (24*60*60*1000)
#define sql_hours(m) is_lng_nil(m) ? int_nil : (int) ((m % (24*60*60*1000)) / (60*60*1000))
#define sql_minutes(m) is_lng_nil(m) ? int_nil : (int) ((m % (60*60*1000)) / (60*1000))
#define sql_seconds(m) is_lng_nil(m) ? int_nil : (int) ((m % (60*1000)) / 1000)
func1(MTIMEsql_year, MTIMEsql_year_bulk, "sql_year", int, int, sql_year, COPYFLAGS, func1_noexcept)
func1(MTIMEsql_month, MTIMEsql_month_bulk, "sql_month", int, int, sql_month, SETFLAGS, func1_noexcept)
func1(MTIMEsql_day, MTIMEsql_day_bulk, "sql_day", lng, lng, sql_day, COPYFLAGS, func1_noexcept)
func1(MTIMEsql_hours, MTIMEsql_hours_bulk, "sql_hours", lng, int, sql_hours, SETFLAGS, func1_noexcept)
func1(MTIMEsql_minutes, MTIMEsql_minutes_bulk, "sql_minutes", lng, int, sql_minutes, SETFLAGS, func1_noexcept)
func1(MTIMEsql_seconds, MTIMEsql_seconds_bulk, "sql_seconds", lng, int, sql_seconds, SETFLAGS, func1_noexcept)

static inline str
date_fromstr_func(date *ret, str s)
{
	if (date_fromstr(s, &(size_t){sizeof(date)}, &ret, true) < 0)
		throw(MAL, "mtime.date_fromstr", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
func1(MTIMEdate_fromstr, MTIMEdate_fromstr_bulk, "date_fromstr", str, date, date_fromstr_func, SETFLAGS, func1_except)

#define date_date(m) m
func1(MTIMEdate_date, MTIMEdate_date_bulk, "date_date", date, date, date_date, COPYFLAGS, func1_noexcept)

func1(MTIMEtimestamp_extract_date, MTIMEtimestamp_extract_date_bulk, "date", timestamp, date, timestamp_date, COPYFLAGS, func1_noexcept)

static inline str
timestamp_fromstr_func(timestamp *ret, str s)
{
	if (timestamp_fromstr(s, &(size_t){sizeof(timestamp)}, &ret, true) < 0)
		throw(MAL, "mtime.timestamp_fromstr", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
func1(MTIMEtimestamp_fromstr, MTIMEtimestamp_fromstr_bulk, "timestamp_fromstr", str, timestamp, timestamp_fromstr_func, SETFLAGS, func1_except)

#define timestamp_timestamp(m) m
func1(MTIMEtimestamp_timestamp, MTIMEtimestamp_timestamp_bulk, "timestamp_timestamp", timestamp, timestamp, timestamp_timestamp, COPYFLAGS, func1_noexcept)

#define mkts(dt)	timestamp_create(dt, daytime_create(0, 0, 0, 0))
func1(MTIMEtimestamp_fromdate, MTIMEtimestamp_fromdate_bulk, "timestamp_fromdate", date, timestamp, mkts, COPYFLAGS, func1_noexcept)

#define seconds_since_epoch(t) is_timestamp_nil(t) ? int_nil : (int) (timestamp_diff(t, unixepoch) / 1000000);
func1(MTIMEseconds_since_epoch, MTIMEseconds_since_epoch_bulk, "seconds_since_epoch", timestamp, int, seconds_since_epoch, COPYFLAGS, func1_noexcept)

#define mktsfromsec(sec)	(is_int_nil(sec) ?							\
							 timestamp_nil :							\
							 timestamp_add_usec(unixepoch,				\
												(sec) * LL_CONSTANT(1000000)))
#define mktsfrommsec(msec)	(is_lng_nil(msec) ?							\
							 timestamp_nil :							\
							 timestamp_add_usec(unixepoch,				\
												(msec) * LL_CONSTANT(1000)))
func1(MTIMEtimestamp_fromsecond, MTIMEtimestamp_fromsecond_bulk, "timestamp_fromsecond", int, timestamp, mktsfromsec, COPYFLAGS, func1_noexcept)
func1(MTIMEtimestamp_frommsec, MTIMEtimestamp_frommsec_bulk, "timestamp_frommsec", lng, timestamp, mktsfrommsec, COPYFLAGS, func1_noexcept)

static inline str
daytime_fromstr_func(daytime *ret, str s)
{
	if (daytime_fromstr(s, &(size_t){sizeof(daytime)}, &ret, true) < 0)
		throw(MAL, "mtime.daytime_fromstr", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
func1(MTIMEdaytime_fromstr, MTIMEdaytime_fromstr_bulk, "daytime_fromstr", str, daytime, daytime_fromstr_func, SETFLAGS, func1_except)

#define daytime_daytime(m) m
func1(MTIMEdaytime_daytime, MTIMEdaytime_daytime_bulk, "daytime_daytime", daytime, daytime, daytime_daytime, COPYFLAGS, func1_noexcept)

static inline str
daytime_fromseconds(daytime *ret, const lng secs)
{
	if (is_lng_nil(secs))
		*ret = daytime_nil;
	else if (secs < 0 || secs >= 24*60*60)
		throw(MAL, "mtime.daytime_fromseconds", SQLSTATE(42000) ILLEGAL_ARGUMENT);
	else
		*ret = (daytime) (secs * 1000000);
	return MAL_SUCCEED;
}
func1(MTIMEdaytime_fromseconds, MTIMEdaytime_fromseconds_bulk, "daytime_fromseconds", const lng, daytime, daytime_fromseconds, COPYFLAGS, func1_except)

func1(MTIMEtimestamp_extract_daytime, MTIMEtimestamp_extract_daytime_bulk, "timestamp_extract_daytime", timestamp, daytime, timestamp_daytime, SETFLAGS, func1_noexcept)

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
timestamp_to_str(str *ret, const timestamp *d, str *format,
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
str_to_timestamp(timestamp *ret, str *s, str *format, const char *type, const char *malfunc)
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

static inline str
str_to_date(date *ret, str s, str format)
{
	str msg = MAL_SUCCEED;
	timestamp ts;
	if ((msg = str_to_timestamp(&ts, &s, &format, "date", "mtime.str_to_date")) != MAL_SUCCEED)
		return msg;
	*ret = timestamp_date(ts);
	return MAL_SUCCEED;
}
func2(MTIMEstr_to_date, MTIMEstr_to_date_bulk, "str_to_date", str, str, date, str_to_date, func2_except)

static inline str
date_to_str(str *ret, date d, str format)
{
	timestamp ts = timestamp_create(d, timestamp_daytime(timestamp_current()));
	return timestamp_to_str(ret, &ts, &format, "date", "mtime.date_to_str");
}
func2(MTIMEdate_to_str, MTIMEdate_to_str_bulk, "date_to_str", date, str, str, date_to_str, func2_except)

static inline str
str_to_time(daytime *ret, str s, str format)
{
	str msg = MAL_SUCCEED;
	timestamp ts;
	if ((msg = str_to_timestamp(&ts, &s, &format, "time", "mtime.str_to_time")) != MAL_SUCCEED)
		return msg;
	*ret = timestamp_daytime(ts);
	return MAL_SUCCEED;
}
func2(MTIMEstr_to_time, MTIMEstr_to_time_bulk, "str_to_time", str, str, daytime, str_to_time, func2_except)

static inline str
time_to_str(str *ret, daytime d, str format)
{
	timestamp ts = timestamp_create(timestamp_date(timestamp_current()), d);
	return timestamp_to_str(ret, &ts, &format, "time", "mtime.time_to_str");
}
func2(MTIMEtime_to_str, MTIMEtime_to_str_bulk, "time_to_str", daytime, str, str, time_to_str, func2_except)

static inline str
str_to_timestamp_func(timestamp *ret, str s, str format)
{
	return str_to_timestamp(ret, &s, &format, "timestamp", "mtime.str_to_timestamp");
}
func2(MTIMEstr_to_timestamp, MTIMEstr_to_timestamp_bulk, "str_to_timestamp", str, str, timestamp, str_to_timestamp_func, func2_except)

static inline str
timestamp_to_str_func(str *ret, timestamp d, str format)
{
	return timestamp_to_str(ret, &d, &format, "timestamp", "mtime.timestamp_to_str");
}
func2(MTIMEtimestamp_to_str, MTIMEtimestamp_to_str_bulk, "timestamp_to_str", timestamp, str, str, timestamp_to_str_func, func2_except)
