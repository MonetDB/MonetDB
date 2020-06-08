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


#define DEC_VAR_R(TYPE, VAR) TYPE *restrict VAR

#define DEC_VAR(TYPE, VAR) TYPE * VAR

#define DEC_ITER(TYPE, VAR) BATiter VAR


#define INIT_VAR(VAR, VAR_BAT) VAR = Tloc(VAR_BAT, 0)

#define APPEND_VAR(MALFUNC) dst[i] = res;

#define GET_NEXT_VAR(VAR) VAR[i]


#define INIT_ITER(VAR, VAR_BAT) VAR = bat_iterator(VAR_BAT)

#define APPEND_STR(MALFUNC) \
	if (BUNappend(bn, res, false) != GDK_SUCCEED) { \
		msg = createException(SQL, "batmtime." MALFUNC, SQLSTATE(HY013) MAL_MALLOC_FAIL); \
		break; \
	}

#define GET_NEXT_ITER(VAR) BUNtvar(VAR, i)


#define DEC_NOTHING(A, B) ;

#define COPYFLAGS	do { bn->tsorted = b1->tsorted; bn->trevsorted = b1->trevsorted; } while (0)
#define SETFLAGS	do { bn->tsorted = bn->trevsorted = n < 2; } while (0)
#define func1(NAME, NAMEBULK, MALFUNC, INTYPE, OUTTYPE, FUNC, SETFLAGS, FUNC_CALL, DEC_SRC, DEC_OUTPUT, \
			  INIT_SRC, INIT_OUTPUT, GET_NEXT_SRC)	\
mal_export str NAME(OUTTYPE *ret, const INTYPE *src);					\
mal_export str NAMEBULK(bat *ret, const bat *bid);						\
str																		\
NAME(OUTTYPE *ret, const INTYPE *src)									\
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
	BAT *b1 = NULL, *bn = NULL;											\
	BUN n;																\
	DEC_SRC(INTYPE, src1); 												\
	DEC_OUTPUT(OUTTYPE, dst);											\
	str msg = MAL_SUCCEED; 												\
																		\
	if ((b1 = BATdescriptor(*bid)) == NULL)	{							\
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
	INIT_SRC(src1, b1);													\
	INIT_OUTPUT(dst, bn);												\
	for (BUN i = 0; i < n; i++) { 										\
		FUNC_CALL(FUNC, (dst[i]), (GET_NEXT_SRC(src1)));				\
	}																	\
	bn->tnonil = b1->tnonil;											\
	bn->tnil = b1->tnil;												\
	BATsetcount(bn, n);													\
	SETFLAGS;															\
	bn->tkey = false;													\
bailout: 																\
	if (b1)																\
		BBPunfix(b1->batCacheid);										\
	if (msg && bn)														\
		BBPreclaim(bn);													\
	else if (bn) 														\
		BBPkeepref(*ret = bn->batCacheid);								\
	return msg;															\
}

#define func1_noexcept(FUNC, RET, PARAM) RET = FUNC(PARAM)
#define func1_except(FUNC, RET, PARAM) msg = FUNC(&RET, PARAM); if (msg) break;

#define func2(NAME, NAMEBULK, MALFUNC, INTYPE1, INTYPE2, OUTTYPE, FUNC, FUNC_CALL, DEC_INPUT1, DEC_INPUT2, DEC_SRC1, DEC_SRC2, DEC_OUTPUT, \
			 INIT_SRC1, INIT_SRC2, INIT_OUTPUT, GET_NEXT_SRC1, GET_NEXT_SRC2, APPEND_NEXT)	\
mal_export str NAME(OUTTYPE *ret, const INTYPE1 *v1, const INTYPE2 *v2); \
mal_export str NAMEBULK(bat *ret, const bat *bid1, const bat *bid2);	\
mal_export str NAMEBULK##_p1(bat *ret, const DEC_INPUT1(INTYPE1, src1), const bat *bid2);	\
mal_export str NAMEBULK##_p2(bat *ret, const bat *bid1, const DEC_INPUT2(INTYPE2, src2));	\
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
	DEC_SRC1(INTYPE1, src1); 											\
	DEC_SRC2(INTYPE2, src2); 											\
	DEC_OUTPUT(OUTTYPE, dst);											\
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
	INIT_SRC1(src1, b1);												\
	INIT_SRC2(src2, b2);												\
	INIT_OUTPUT(dst, bn);												\
	for (BUN i = 0; i < n; i++) { 										\
		OUTTYPE res; 													\
		FUNC_CALL(FUNC, (res), (GET_NEXT_SRC1(src1)), (GET_NEXT_SRC2(src2)));	\
		APPEND_NEXT(MALFUNC); 											\
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
NAMEBULK##_p1(bat *ret, const DEC_INPUT1(INTYPE1, src1), const bat *bid2)	\
{																		\
	BAT *b2 = NULL, *bn = NULL;											\
	BUN n;																\
	DEC_SRC2(INTYPE2, src2); 											\
	DEC_OUTPUT(OUTTYPE, dst);											\
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
	INIT_SRC2(src2, b2);												\
	INIT_OUTPUT(dst, bn);												\
	for (BUN i = 0; i < n; i++) { 										\
		OUTTYPE res; 													\
		FUNC_CALL(FUNC, (res), *src1, (GET_NEXT_SRC2(src2)));			\
		APPEND_NEXT(MALFUNC); 											\
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
NAMEBULK##_p2(bat *ret, const bat *bid1, const DEC_INPUT2(INTYPE2, src2))	\
{																		\
	BAT *b1 = NULL, *bn = NULL;											\
	BUN n;																\
	DEC_SRC1(INTYPE1, src1);											\
	DEC_OUTPUT(OUTTYPE, dst);											\
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
	INIT_SRC1(src1, b1);												\
	INIT_OUTPUT(dst, bn);												\
	for (BUN i = 0; i < n; i++) { 										\
		OUTTYPE res; 													\
		FUNC_CALL(FUNC, (res), (GET_NEXT_SRC1(src1)), *src2);			\
		APPEND_NEXT(MALFUNC); 											\
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

func2(MTIMEdate_diff, MTIMEdate_diff_bulk, "diff", date, date, int, date_diff, func2_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)
func2(MTIMEdaytime_diff_msec, MTIMEdaytime_diff_msec_bulk, "diff", daytime, daytime, lng, daytime_diff, func2_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)

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
func2(MTIMEdate_sub_msec_interval, MTIMEdate_sub_msec_interval_bulk, "date_sub_msec_interval", date, lng, date, date_sub_msec_interval, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)
func2(MTIMEdate_add_msec_interval, MTIMEdate_add_msec_interval_bulk, "date_add_msec_interval", date, lng, date, date_add_msec_interval, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)

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
func2(MTIMEtimestamp_sub_msec_interval, MTIMEtimestamp_sub_msec_interval_bulk, "timestamp_sub_msec_interval", timestamp, lng, timestamp, timestamp_sub_msec_interval, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)
func2(MTIMEtimestamp_add_msec_interval, MTIMEtimestamp_add_msec_interval_bulk, "timestamp_add_msec_interval", timestamp, lng, timestamp, timestamp_add_msec_interval, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)

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
func2(MTIMEtimestamp_sub_month_interval, MTIMEtimestamp_sub_month_interval_bulk, "timestamp_sub_month_interval", timestamp, int, timestamp, timestamp_sub_month_interval, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)
func2(MTIMEtimestamp_add_month_interval, MTIMEtimestamp_add_month_interval_bulk, "timestamp_add_month_interval", timestamp, int, timestamp, timestamp_add_month_interval, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)

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
func2(MTIMEtime_sub_msec_interval, MTIMEtime_sub_msec_interval_bulk, "time_sub_msec_interval", daytime, lng, daytime, time_sub_msec_interval, func2_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)
func2(MTIMEtime_add_msec_interval, MTIMEtime_add_msec_interval_bulk, "time_add_msec_interval", daytime, lng, daytime, time_add_msec_interval, func2_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)

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
func2(MTIMEdate_submonths, MTIMEdate_submonths_bulk, "date_submonths", date, int, date, date_submonths, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)
func2(MTIMEdate_addmonths, MTIMEdate_addmonths_bulk, "date_addmonths", date, int, date, date_addmonths, func2_except, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)

func1(MTIMEdate_extract_century, MTIMEdate_extract_century_bulk, "date_century", date, int, date_century, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_decade, MTIMEdate_extract_decade_bulk, "date_decade", date, int, date_decade, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_year, MTIMEdate_extract_year_bulk, "date_year", date, int, date_year, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_quarter, MTIMEdate_extract_quarter_bulk, "date_quarter", date, int, date_quarter, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_month, MTIMEdate_extract_month_bulk, "date_month", date, int, date_month, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_day, MTIMEdate_extract_day_bulk, "date_day", date, int, date_day, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_dayofyear, MTIMEdate_extract_dayofyear_bulk, "date_dayofyear", date, int, date_dayofyear, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_weekofyear, MTIMEdate_extract_weekofyear_bulk, "date_weekofyear", date, int, date_weekofyear, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdate_extract_dayofweek, MTIMEdate_extract_dayofweek_bulk, "date_dayofweek", date, int, date_dayofweek, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdaytime_extract_hours, MTIMEdaytime_extract_hours_bulk, "daytime_hour", daytime, int, daytime_hour, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdaytime_extract_minutes, MTIMEdaytime_extract_minutes_bulk, "daytime_minutes", daytime, int, daytime_min, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEdaytime_extract_sql_seconds, MTIMEdaytime_extract_sql_seconds_bulk, "daytime_seconds", daytime, int, daytime_sec_usec, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

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
func2(MTIMEtimestamp_diff_msec, MTIMEtimestamp_diff_msec_bulk, "diff", timestamp, timestamp, lng, TSDIFF, func2_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, INIT_VAR, GET_NEXT_VAR, GET_NEXT_VAR, APPEND_VAR)

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
func1(MTIMEtimestamp_century, MTIMEtimestamp_century_bulk, "timestamp_century", timestamp, int, timestamp_century, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_decade, MTIMEtimestamp_decade_bulk, "timestamp_decade", timestamp, int, timestamp_decade, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_year, MTIMEtimestamp_year_bulk, "timestamp_year", timestamp, int, timestamp_year, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_quarter, MTIMEtimestamp_quarter_bulk, "timestamp_quarter", timestamp, int, timestamp_quarter, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_month, MTIMEtimestamp_month_bulk, "timestamp_month", timestamp, int, timestamp_month, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_day, MTIMEtimestamp_day_bulk, "timestamp_day", timestamp, int, timestamp_day, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_hours, MTIMEtimestamp_hours_bulk, "timestamp_hours", timestamp, int, timestamp_hours, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_minutes, MTIMEtimestamp_minutes_bulk, "timestamp_minutes", timestamp, int, timestamp_minutes, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_sql_seconds, MTIMEtimestamp_sql_seconds_bulk, "sql_seconds", timestamp, int, timestamp_extract_usecond, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

#define sql_year(m) is_int_nil(m) ? int_nil : m / 12
#define sql_month(m) is_int_nil(m) ? int_nil : m % 12
#define sql_day(m) is_lng_nil(m) ? lng_nil : m / (24*60*60*1000)
#define sql_hours(m) is_lng_nil(m) ? int_nil : (int) ((m % (24*60*60*1000)) / (60*60*1000))
#define sql_minutes(m) is_lng_nil(m) ? int_nil : (int) ((m % (60*60*1000)) / (60*1000))
#define sql_seconds(m) is_lng_nil(m) ? int_nil : (int) ((m % (60*1000)) / 1000)
func1(MTIMEsql_year, MTIMEsql_year_bulk, "sql_year", int, int, sql_year, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEsql_month, MTIMEsql_month_bulk, "sql_month", int, int, sql_month, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEsql_day, MTIMEsql_day_bulk, "sql_day", lng, lng, sql_day, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEsql_hours, MTIMEsql_hours_bulk, "sql_hours", lng, int, sql_hours, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEsql_minutes, MTIMEsql_minutes_bulk, "sql_minutes", lng, int, sql_minutes, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEsql_seconds, MTIMEsql_seconds_bulk, "sql_seconds", lng, int, sql_seconds, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

static inline str
date_fromstr_func(date *ret, str s)
{
	if (date_fromstr(s, &(size_t){sizeof(date)}, &ret, true) < 0)
		throw(MAL, "mtime.date_fromstr", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
func1(MTIMEdate_fromstr, MTIMEdate_fromstr_bulk, "date_fromstr", str, date, date_fromstr_func, SETFLAGS, func1_except, \
	  DEC_ITER, DEC_VAR_R, INIT_ITER, INIT_VAR, GET_NEXT_ITER)

#define date_date(m) m
func1(MTIMEdate_date, MTIMEdate_date_bulk, "date_date", date, date, date_date, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

func1(MTIMEtimestamp_extract_date, MTIMEtimestamp_extract_date_bulk, "date", timestamp, date, timestamp_date, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

static inline str
timestamp_fromstr_func(timestamp *ret, str s)
{
	if (timestamp_fromstr(s, &(size_t){sizeof(timestamp)}, &ret, true) < 0)
		throw(MAL, "mtime.timestamp_fromstr", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
func1(MTIMEtimestamp_fromstr, MTIMEtimestamp_fromstr_bulk, "timestamp_fromstr", str, timestamp, timestamp_fromstr_func, SETFLAGS, func1_except, \
	  DEC_ITER, DEC_VAR_R, INIT_ITER, INIT_VAR, GET_NEXT_ITER)

#define timestamp_timestamp(m) m
func1(MTIMEtimestamp_timestamp, MTIMEtimestamp_timestamp_bulk, "timestamp_timestamp", timestamp, timestamp, timestamp_timestamp, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

#define mkts(dt)	timestamp_create(dt, daytime_create(0, 0, 0, 0))
func1(MTIMEtimestamp_fromdate, MTIMEtimestamp_fromdate_bulk, "timestamp_fromdate", date, timestamp, mkts, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

#define seconds_since_epoch(t) is_timestamp_nil(t) ? int_nil : (int) (timestamp_diff(t, unixepoch) / 1000000);
func1(MTIMEseconds_since_epoch, MTIMEseconds_since_epoch_bulk, "seconds_since_epoch", timestamp, int, seconds_since_epoch, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

#define mktsfromsec(sec)	(is_int_nil(sec) ?							\
							 timestamp_nil :							\
							 timestamp_add_usec(unixepoch,				\
												(sec) * LL_CONSTANT(1000000)))
#define mktsfrommsec(msec)	(is_lng_nil(msec) ?							\
							 timestamp_nil :							\
							 timestamp_add_usec(unixepoch,				\
												(msec) * LL_CONSTANT(1000)))
func1(MTIMEtimestamp_fromsecond, MTIMEtimestamp_fromsecond_bulk, "timestamp_fromsecond", int, timestamp, mktsfromsec, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)
func1(MTIMEtimestamp_frommsec, MTIMEtimestamp_frommsec_bulk, "timestamp_frommsec", lng, timestamp, mktsfrommsec, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

static inline str
daytime_fromstr_func(daytime *ret, str s)
{
	if (daytime_fromstr(s, &(size_t){sizeof(daytime)}, &ret, true) < 0)
		throw(MAL, "mtime.daytime_fromstr", GDK_EXCEPTION);
	return MAL_SUCCEED;
}
func1(MTIMEdaytime_fromstr, MTIMEdaytime_fromstr_bulk, "daytime_fromstr", str, daytime, daytime_fromstr_func, SETFLAGS, func1_except, \
	  DEC_ITER, DEC_VAR_R, INIT_ITER, INIT_VAR, GET_NEXT_ITER)

#define daytime_daytime(m) m
func1(MTIMEdaytime_daytime, MTIMEdaytime_daytime_bulk, "daytime_daytime", daytime, daytime, daytime_daytime, COPYFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

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
func1(MTIMEdaytime_fromseconds, MTIMEdaytime_fromseconds_bulk, "daytime_fromseconds", lng, daytime, daytime_fromseconds, COPYFLAGS, func1_except, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

func1(MTIMEtimestamp_extract_daytime, MTIMEtimestamp_extract_daytime_bulk, "timestamp_extract_daytime", timestamp, daytime, timestamp_daytime, SETFLAGS, func1_noexcept, \
	  DEC_VAR_R, DEC_VAR_R, INIT_VAR, INIT_VAR, GET_NEXT_VAR)

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
func2(MTIMEstr_to_date, MTIMEstr_to_date_bulk, "str_to_date", str, str, date, str_to_date, func2_except, \
	  DEC_VAR, DEC_VAR, DEC_ITER, DEC_ITER, DEC_VAR_R, INIT_ITER, INIT_ITER, INIT_VAR, GET_NEXT_ITER, GET_NEXT_ITER, APPEND_VAR)

static inline str
date_to_str(str *ret, date d, str format)
{
	timestamp ts = timestamp_create(d, timestamp_daytime(timestamp_current()));
	return timestamp_to_str(ret, &ts, &format, "date", "mtime.date_to_str");
}
func2(MTIMEdate_to_str, MTIMEdate_to_str_bulk, "date_to_str", date, str, str, date_to_str, func2_except, \
	  DEC_VAR_R, DEC_VAR, DEC_VAR, DEC_ITER, DEC_NOTHING, INIT_VAR, INIT_ITER, DEC_NOTHING, GET_NEXT_VAR, GET_NEXT_ITER, APPEND_STR)

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
func2(MTIMEstr_to_time, MTIMEstr_to_time_bulk, "str_to_time", str, str, daytime, str_to_time, func2_except, \
	  DEC_VAR, DEC_VAR, DEC_ITER, DEC_ITER, DEC_VAR_R, INIT_ITER, INIT_ITER, INIT_VAR, GET_NEXT_ITER, GET_NEXT_ITER, APPEND_VAR)

static inline str
time_to_str(str *ret, daytime d, str format)
{
	timestamp ts = timestamp_create(timestamp_date(timestamp_current()), d);
	return timestamp_to_str(ret, &ts, &format, "time", "mtime.time_to_str");
}
func2(MTIMEtime_to_str, MTIMEtime_to_str_bulk, "time_to_str", daytime, str, str, time_to_str, func2_except, \
	  DEC_VAR_R, DEC_VAR, DEC_VAR, DEC_ITER, DEC_NOTHING, INIT_VAR, INIT_ITER, DEC_NOTHING, GET_NEXT_VAR, GET_NEXT_ITER, APPEND_STR)

static inline str
str_to_timestamp_func(timestamp *ret, str s, str format)
{
	return str_to_timestamp(ret, &s, &format, "timestamp", "mtime.str_to_timestamp");
}
func2(MTIMEstr_to_timestamp, MTIMEstr_to_timestamp_bulk, "str_to_timestamp", str, str, timestamp, str_to_timestamp_func, func2_except, \
	  DEC_VAR, DEC_VAR, DEC_ITER, DEC_ITER, DEC_VAR_R, INIT_ITER, INIT_ITER, INIT_VAR, GET_NEXT_ITER, GET_NEXT_ITER, APPEND_VAR)

static inline str
timestamp_to_str_func(str *ret, timestamp d, str format)
{
	return timestamp_to_str(ret, &d, &format, "timestamp", "mtime.timestamp_to_str");
}
func2(MTIMEtimestamp_to_str, MTIMEtimestamp_to_str_bulk, "timestamp_to_str", timestamp, str, str, timestamp_to_str_func, func2_except, \
	  DEC_VAR_R, DEC_VAR, DEC_VAR, DEC_ITER, DEC_NOTHING, INIT_VAR, INIT_ITER, DEC_NOTHING, GET_NEXT_VAR, GET_NEXT_ITER, APPEND_STR)

#include "mel.h"
static mel_func mtime_init_funcs[] = {
 command("mtime", "epoch", MTIMEseconds_since_epoch, false, "unix-time (epoch) support: seconds since epoch", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "epoch", MTIMEseconds_since_epoch_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "epoch", MTIMEtimestamp_fromsecond, false, "convert seconds since epoch into a timestamp", args(1,2, arg("",timestamp),arg("t",int))),
 command("batmtime", "epoch", MTIMEtimestamp_fromsecond_bulk, false, "", args(1,2, batarg("",timestamp),batarg("t",int))),
 command("mtime", "epoch", MTIMEtimestamp_frommsec, false, "convert milli seconds since epoch into a timestamp", args(1,2, arg("",timestamp),arg("t",lng))),
 command("batmtime", "epoch", MTIMEtimestamp_frommsec_bulk, false, "", args(1,2, batarg("",timestamp),batarg("t",lng))),
 command("mtime", "date_sub_msec_interval", MTIMEdate_sub_msec_interval, false, "", args(1,3, arg("",date),arg("t",date),arg("ms",lng))),
 command("batmtime", "date_sub_msec_interval", MTIMEdate_sub_msec_interval_bulk, false, "", args(1,3, batarg("",date),batarg("t",date),batarg("ms",lng))),
 command("batmtime", "date_sub_msec_interval", MTIMEdate_sub_msec_interval_bulk_p1, false, "", args(1,3, batarg("",date),arg("t",date),batarg("ms",lng))),
 command("batmtime", "date_sub_msec_interval", MTIMEdate_sub_msec_interval_bulk_p2, false, "", args(1,3, batarg("",date),batarg("t",date),arg("ms",lng))),
 command("mtime", "date_add_msec_interval", MTIMEdate_add_msec_interval, false, "", args(1,3, arg("",date),arg("t",date),arg("ms",lng))),
 command("batmtime", "date_add_msec_interval", MTIMEdate_add_msec_interval_bulk, false, "", args(1,3, batarg("",date),batarg("t",date),batarg("ms",lng))),
 command("batmtime", "date_add_msec_interval", MTIMEdate_add_msec_interval_bulk_p1, false, "", args(1,3, batarg("",date),arg("t",date),batarg("ms",lng))),
 command("batmtime", "date_add_msec_interval", MTIMEdate_add_msec_interval_bulk_p2, false, "", args(1,3, batarg("",date),batarg("t",date),arg("ms",lng))),
 command("mtime", "timestamp_sub_msec_interval", MTIMEtimestamp_sub_msec_interval, false, "", args(1,3, arg("",timestamp),arg("t",timestamp),arg("ms",lng))),
 command("batmtime", "timestamp_sub_msec_interval", MTIMEtimestamp_sub_msec_interval_bulk, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),batarg("ms",lng))),
 command("batmtime", "timestamp_sub_msec_interval", MTIMEtimestamp_sub_msec_interval_bulk_p1, false, "", args(1,3, batarg("",timestamp),arg("t",timestamp),batarg("ms",lng))),
 command("batmtime", "timestamp_sub_msec_interval", MTIMEtimestamp_sub_msec_interval_bulk_p2, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),arg("ms",lng))),
 command("mtime", "timestamp_add_msec_interval", MTIMEtimestamp_add_msec_interval, false, "", args(1,3, arg("",timestamp),arg("t",timestamp),arg("ms",lng))),
 command("batmtime", "timestamp_add_msec_interval", MTIMEtimestamp_add_msec_interval_bulk, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),batarg("ms",lng))),
 command("batmtime", "timestamp_add_msec_interval", MTIMEtimestamp_add_msec_interval_bulk_p1, false, "", args(1,3, batarg("",timestamp),arg("t",timestamp),batarg("ms",lng))),
 command("batmtime", "timestamp_add_msec_interval", MTIMEtimestamp_add_msec_interval_bulk_p2, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),arg("ms",lng))),
 command("mtime", "timestamp_sub_month_interval", MTIMEtimestamp_sub_month_interval, false, "Subtract months from a timestamp", args(1,3, arg("",timestamp),arg("t",timestamp),arg("s",int))),
 command("batmtime", "timestamp_sub_month_interval", MTIMEtimestamp_sub_month_interval_bulk, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),batarg("s",int))),
 command("batmtime", "timestamp_sub_month_interval", MTIMEtimestamp_sub_month_interval_bulk_p1, false, "", args(1,3, batarg("",timestamp),arg("t",timestamp),batarg("s",int))),
 command("batmtime", "timestamp_sub_month_interval", MTIMEtimestamp_sub_month_interval_bulk_p2, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),arg("s",int))),
 command("mtime", "timestamp_add_month_interval", MTIMEtimestamp_add_month_interval, false, "Add months to a timestamp", args(1,3, arg("",timestamp),arg("t",timestamp),arg("s",int))),
 command("batmtime", "timestamp_add_month_interval", MTIMEtimestamp_add_month_interval_bulk, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),batarg("s",int))),
 command("batmtime", "timestamp_add_month_interval", MTIMEtimestamp_add_month_interval_bulk_p1, false, "", args(1,3, batarg("",timestamp),arg("t",timestamp),batarg("s",int))),
 command("batmtime", "timestamp_add_month_interval", MTIMEtimestamp_add_month_interval_bulk_p2, false, "", args(1,3, batarg("",timestamp),batarg("t",timestamp),arg("s",int))),
 command("mtime", "time_sub_msec_interval", MTIMEtime_sub_msec_interval, false, "Subtract seconds from a time", args(1,3, arg("",daytime),arg("t",daytime),arg("ms",lng))),
 command("batmtime", "time_sub_msec_interval", MTIMEtime_sub_msec_interval_bulk, false, "", args(1,3, batarg("",daytime),batarg("t",daytime),batarg("ms",lng))),
 command("batmtime", "time_sub_msec_interval", MTIMEtime_sub_msec_interval_bulk_p1, false, "", args(1,3, batarg("",daytime),arg("t",daytime),batarg("ms",lng))),
 command("batmtime", "time_sub_msec_interval", MTIMEtime_sub_msec_interval_bulk_p2, false, "", args(1,3, batarg("",daytime),batarg("t",daytime),arg("ms",lng))),
 command("mtime", "time_add_msec_interval", MTIMEtime_add_msec_interval, false, "Add seconds to a time", args(1,3, arg("",daytime),arg("t",daytime),arg("ms",lng))),
 command("batmtime", "time_add_msec_interval", MTIMEtime_add_msec_interval_bulk, false, "", args(1,3, batarg("",daytime),batarg("t",daytime),batarg("ms",lng))),
 command("batmtime", "time_add_msec_interval", MTIMEtime_add_msec_interval_bulk_p1, false, "", args(1,3, batarg("",daytime),arg("t",daytime),batarg("ms",lng))),
 command("batmtime", "time_add_msec_interval", MTIMEtime_add_msec_interval_bulk_p2, false, "", args(1,3, batarg("",daytime),batarg("t",daytime),arg("ms",lng))),
 command("mtime", "diff", MTIMEdaytime_diff_msec, false, "returns the number of msec between 'val1' and 'val2'.", args(1,3, arg("",lng),arg("val1",daytime),arg("val2",daytime))),
 command("batmtime", "diff", MTIMEdaytime_diff_msec_bulk, false, "", args(1,3, batarg("",lng),batarg("val1",daytime),batarg("val2",daytime))),
 command("batmtime", "diff", MTIMEdaytime_diff_msec_bulk_p1, false, "", args(1,3, batarg("",lng),arg("val1",daytime),batarg("val2",daytime))),
 command("batmtime", "diff", MTIMEdaytime_diff_msec_bulk_p2, false, "", args(1,3, batarg("",lng),batarg("val1",daytime),arg("val2",daytime))),
 command("mtime", "date_sub_month_interval", MTIMEdate_submonths, false, "Subtract months from a date", args(1,3, arg("",date),arg("t",date),arg("months",int))),
 command("batmtime", "date_sub_month_interval", MTIMEdate_submonths_bulk, false, "", args(1,3, batarg("",date),batarg("t",date),batarg("months",int))),
 command("batmtime", "date_sub_month_interval", MTIMEdate_submonths_bulk_p1, false, "", args(1,3, batarg("",date),arg("t",date),batarg("months",int))),
 command("batmtime", "date_sub_month_interval", MTIMEdate_submonths_bulk_p2, false, "", args(1,3, batarg("",date),batarg("t",date),arg("months",int))),
 command("mtime", "local_timezone", MTIMElocal_timezone_msec, false, "get the local timezone in seconds", args(1,1, arg("",lng))),
 command("mtime", "century", MTIMEdate_extract_century, false, "extracts century from date.", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "century", MTIMEdate_extract_century_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "decade", MTIMEdate_extract_decade, false, "extracts decade from date.", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "decade", MTIMEdate_extract_decade_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "year", MTIMEdate_extract_year, false, "extracts year from date.", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "year", MTIMEdate_extract_year_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "quarter", MTIMEdate_extract_quarter, false, "extracts quarter from date", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "quarter", MTIMEdate_extract_quarter_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "month", MTIMEdate_extract_month, false, "extracts month from date", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "month", MTIMEdate_extract_month_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "day", MTIMEdate_extract_day, false, "extracts day from date ", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "day", MTIMEdate_extract_day_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "hours", MTIMEdaytime_extract_hours, false, "extracts hour from daytime", args(1,2, arg("",int),arg("h",daytime))),
 command("batmtime", "hours", MTIMEdaytime_extract_hours_bulk, false, "", args(1,2, batarg("",int),batarg("d",daytime))),
 command("mtime", "minutes", MTIMEdaytime_extract_minutes, false, "extracts minutes from daytime", args(1,2, arg("",int),arg("d",daytime))),
 command("batmtime", "minutes", MTIMEdaytime_extract_minutes_bulk, false, "", args(1,2, batarg("",int),batarg("d",daytime))),
 command("mtime", "sql_seconds", MTIMEdaytime_extract_sql_seconds, false, "extracts seconds (with fractional milliseconds) from daytime", args(1,2, arg("",int),arg("d",daytime))),
 command("batmtime", "sql_seconds", MTIMEdaytime_extract_sql_seconds_bulk, false, "", args(1,2, batarg("",int),batarg("d",daytime))),
 command("mtime", "addmonths", MTIMEdate_addmonths, false, "returns the date after a number of\nmonths (possibly negative).", args(1,3, arg("",date),arg("value",date),arg("months",int))),
 command("batmtime", "addmonths", MTIMEdate_addmonths_bulk, false, "", args(1,3, batarg("",date),batarg("value",date),batarg("months",int))),
 command("batmtime", "addmonths", MTIMEdate_addmonths_bulk_p1, false, "", args(1,3, batarg("",date),arg("value",date),batarg("months",int))),
 command("batmtime", "addmonths", MTIMEdate_addmonths_bulk_p2, false, "", args(1,3, batarg("",date),batarg("value",date),arg("months",int))),
 command("mtime", "diff", MTIMEdate_diff, false, "returns the number of days\nbetween 'val1' and 'val2'.", args(1,3, arg("",int),arg("val1",date),arg("val2",date))),
 command("batmtime", "diff", MTIMEdate_diff_bulk, false, "", args(1,3, batarg("",int),batarg("val1",date),batarg("val2",date))),
 command("batmtime", "diff", MTIMEdate_diff_bulk_p1, false, "", args(1,3, batarg("",int),arg("val1",date),batarg("val2",date))),
 command("batmtime", "diff", MTIMEdate_diff_bulk_p2, false, "", args(1,3, batarg("",int),batarg("val1",date),arg("val2",date))),
 command("mtime", "dayofyear", MTIMEdate_extract_dayofyear, false, "Returns N where d is the Nth day\nof the year (january 1 returns 1)", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "dayofyear", MTIMEdate_extract_dayofyear_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "weekofyear", MTIMEdate_extract_weekofyear, false, "Returns the week number in the year.", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "weekofyear", MTIMEdate_extract_weekofyear_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "dayofweek", MTIMEdate_extract_dayofweek, false, "Returns the current day of the week\nwhere 1=monday, .., 7=sunday", args(1,2, arg("",int),arg("d",date))),
 command("batmtime", "dayofweek", MTIMEdate_extract_dayofweek_bulk, false, "", args(1,2, batarg("",int),batarg("d",date))),
 command("mtime", "diff", MTIMEtimestamp_diff_msec, false, "returns the number of milliseconds\nbetween 'val1' and 'val2'.", args(1,3, arg("",lng),arg("val1",timestamp),arg("val2",timestamp))),
 command("batmtime", "diff", MTIMEtimestamp_diff_msec_bulk, false, "", args(1,3, batarg("",lng),batarg("val1",timestamp),batarg("val2",timestamp))),
 command("batmtime", "diff", MTIMEtimestamp_diff_msec_bulk_p1, false, "", args(1,3, batarg("",lng),arg("val1",timestamp),batarg("val2",timestamp))),
 command("batmtime", "diff", MTIMEtimestamp_diff_msec_bulk_p2, false, "", args(1,3, batarg("",lng),batarg("val1",timestamp),arg("val2",timestamp))),
 command("mtime", "str_to_date", MTIMEstr_to_date, false, "create a date from the string, using the specified format (see man strptime)", args(1,3, arg("",date),arg("s",str),arg("format",str))),
 command("batmtime", "str_to_date", MTIMEstr_to_date_bulk, false, "", args(1,3, batarg("",date),batarg("s",str),batarg("format",str))),
 command("batmtime", "str_to_date", MTIMEstr_to_date_bulk_p1, false, "", args(1,3, batarg("",date),arg("s",str),batarg("format",str))),
 command("batmtime", "str_to_date", MTIMEstr_to_date_bulk_p2, false, "", args(1,3, batarg("",date),batarg("s",str),arg("format",str))),
 command("mtime", "date_to_str", MTIMEdate_to_str, false, "create a string from the date, using the specified format (see man strftime)", args(1,3, arg("",str),arg("d",date),arg("format",str))),
 command("batmtime", "date_to_str", MTIMEdate_to_str_bulk, false, "", args(1,3, batarg("",str),batarg("d",str),batarg("format",str))),
 command("batmtime", "date_to_str", MTIMEdate_to_str_bulk_p1, false, "", args(1,3, batarg("",str),arg("d",date),batarg("format",str))),
 command("batmtime", "date_to_str", MTIMEdate_to_str_bulk_p2, false, "", args(1,3, batarg("",str),batarg("d",date),arg("format",str))),
 command("mtime", "str_to_time", MTIMEstr_to_time, false, "create a time from the string, using the specified format (see man strptime)", args(1,3, arg("",daytime),arg("s",str),arg("format",str))),
 command("batmtime", "str_to_time", MTIMEstr_to_time_bulk, false, "", args(1,3, batarg("",daytime),batarg("s",str),batarg("format",str))),
 command("batmtime", "str_to_time", MTIMEstr_to_time_bulk_p1, false, "", args(1,3, batarg("",daytime),arg("s",str),batarg("format",str))),
 command("batmtime", "str_to_time", MTIMEstr_to_time_bulk_p2, false, "", args(1,3, batarg("",daytime),batarg("s",str),arg("format",str))),
 command("mtime", "time_to_str", MTIMEtime_to_str, false, "create a string from the time, using the specified format (see man strftime)", args(1,3, arg("",str),arg("d",daytime),arg("format",str))),
 command("batmtime", "time_to_str", MTIMEtime_to_str_bulk, false, "", args(1,3, batarg("",str),batarg("d",daytime),batarg("format",str))),
 command("batmtime", "time_to_str", MTIMEtime_to_str_bulk_p1, false, "", args(1,3, batarg("",str),arg("d",daytime),batarg("format",str))),
 command("batmtime", "time_to_str", MTIMEtime_to_str_bulk_p2, false, "", args(1,3, batarg("",str),batarg("d",daytime),arg("format",str))),
 command("mtime", "str_to_timestamp", MTIMEstr_to_timestamp, false, "create a timestamp from the string, using the specified format (see man strptime)", args(1,3, arg("",timestamp),arg("s",str),arg("format",str))),
 command("batmtime", "str_to_timestamp", MTIMEstr_to_timestamp_bulk, false, "", args(1,3, batarg("",timestamp),batarg("d",str),batarg("format",str))),
 command("batmtime", "str_to_timestamp", MTIMEstr_to_timestamp_bulk_p1, false, "", args(1,3, batarg("",timestamp),arg("s",str),batarg("format",str))),
 command("batmtime", "str_to_timestamp", MTIMEstr_to_timestamp_bulk_p2, false, "", args(1,3, batarg("",timestamp),batarg("s",str),arg("format",str))),
 command("mtime", "timestamp_to_str", MTIMEtimestamp_to_str, false, "create a string from the time, using the specified format (see man strftime)", args(1,3, arg("",str),arg("d",timestamp),arg("format",str))),
 command("batmtime", "timestamp_to_str", MTIMEtimestamp_to_str_bulk, false, "", args(1,3, batarg("",str),batarg("d",timestamp),batarg("format",str))),
 command("batmtime", "timestamp_to_str", MTIMEtimestamp_to_str_bulk_p1, false, "", args(1,3, batarg("",str),arg("d",timestamp),batarg("format",str))),
 command("batmtime", "timestamp_to_str", MTIMEtimestamp_to_str_bulk_p2, false, "", args(1,3, batarg("",str),batarg("d",timestamp),arg("format",str))),
 command("mtime", "current_timestamp", MTIMEcurrent_timestamp, false, "", args(1,1, arg("",timestamp))),
 command("mtime", "current_date", MTIMEcurrent_date, false, "", args(1,1, arg("",date))),
 command("mtime", "current_time", MTIMEcurrent_time, false, "", args(1,1, arg("",daytime))),
 command("mtime", "century", MTIMEtimestamp_century, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "century", MTIMEtimestamp_century_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "decade", MTIMEtimestamp_decade, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "decade", MTIMEtimestamp_decade_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "year", MTIMEtimestamp_year, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "year", MTIMEtimestamp_year_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "quarter", MTIMEtimestamp_quarter, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "quarter", MTIMEtimestamp_quarter_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "month", MTIMEtimestamp_month, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "month", MTIMEtimestamp_month_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "day", MTIMEtimestamp_day, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "day", MTIMEtimestamp_day_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "hours", MTIMEtimestamp_hours, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "hours", MTIMEtimestamp_hours_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "minutes", MTIMEtimestamp_minutes, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "minutes", MTIMEtimestamp_minutes_bulk, false, "", args(1,2, batarg("",int),batarg("t",timestamp))),
 command("mtime", "sql_seconds", MTIMEtimestamp_sql_seconds, false, "", args(1,2, arg("",int),arg("t",timestamp))),
 command("batmtime", "sql_seconds", MTIMEtimestamp_sql_seconds_bulk, false, "", args(1,2, batarg("",int),batarg("d",timestamp))),
 command("mtime", "year", MTIMEsql_year, false, "", args(1,2, arg("",int),arg("months",int))),
 command("batmtime", "year", MTIMEsql_year_bulk, false, "", args(1,2, batarg("",int),batarg("months",int))),
 command("mtime", "month", MTIMEsql_month, false, "", args(1,2, arg("",int),arg("months",int))),
 command("batmtime", "month", MTIMEsql_month_bulk, false, "", args(1,2, batarg("",int),batarg("months",int))),
 command("mtime", "day", MTIMEsql_day, false, "", args(1,2, arg("",lng),arg("msecs",lng))),
 command("batmtime", "day", MTIMEsql_day_bulk, false, "", args(1,2, batarg("",lng),batarg("msecs",lng))),
 command("mtime", "hours", MTIMEsql_hours, false, "", args(1,2, arg("",int),arg("msecs",lng))),
 command("batmtime", "hours", MTIMEsql_hours_bulk, false, "", args(1,2, batarg("",int),batarg("msecs",lng))),
 command("mtime", "minutes", MTIMEsql_minutes, false, "", args(1,2, arg("",int),arg("msecs",lng))),
 command("batmtime", "minutes", MTIMEsql_minutes_bulk, false, "", args(1,2, batarg("",int),batarg("msecs",lng))),
 command("mtime", "seconds", MTIMEsql_seconds, false, "", args(1,2, arg("",int),arg("msecs",lng))),
 command("batmtime", "seconds", MTIMEsql_seconds_bulk, false, "", args(1,2, batarg("",int),batarg("msecs",lng))),
 command("calc", "date", MTIMEdate_fromstr, false, "", args(1,2, arg("",date),arg("s",str))),
 command("calc", "date", MTIMEdate_date, false, "", args(1,2, arg("",date),arg("d",date))),
 command("calc", "date", MTIMEtimestamp_extract_date, false, "", args(1,2, arg("",date),arg("t",timestamp))),
 command("calc", "timestamp", MTIMEtimestamp_fromstr, false, "", args(1,2, arg("",timestamp),arg("s",str))),
 command("calc", "timestamp", MTIMEtimestamp_timestamp, false, "", args(1,2, arg("",timestamp),arg("t",timestamp))),
 command("calc", "timestamp", MTIMEtimestamp_fromdate, false, "", args(1,2, arg("",timestamp),arg("d",date))),
 command("calc", "timestamp", MTIMEtimestamp_fromsecond, false, "", args(1,2, arg("",timestamp),arg("secs",int))),
 command("calc", "timestamp", MTIMEtimestamp_frommsec, false, "", args(1,2, arg("",timestamp),arg("msecs",lng))),
 command("calc", "daytime", MTIMEdaytime_fromstr, false, "", args(1,2, arg("",daytime),arg("s",str))),
 command("calc", "daytime", MTIMEdaytime_daytime, false, "", args(1,2, arg("",daytime),arg("d",daytime))),
 command("calc", "daytime", MTIMEdaytime_fromseconds, false, "", args(1,2, arg("",daytime),arg("s",lng))),
 command("calc", "daytime", MTIMEtimestamp_extract_daytime, false, "", args(1,2, arg("",daytime),arg("t",timestamp))),
 command("batcalc", "date", MTIMEdate_fromstr_bulk, false, "", args(1,2, batarg("",date),batarg("s",str))),
 command("batcalc", "date", MTIMEdate_date_bulk, false, "", args(1,2, batarg("",date),batarg("d",date))),
 command("batcalc", "date", MTIMEtimestamp_extract_date_bulk, false, "", args(1,2, batarg("",date),batarg("t",timestamp))),
 command("batcalc", "timestamp", MTIMEtimestamp_fromstr_bulk, false, "", args(1,2, batarg("",timestamp),batarg("s",str))),
 command("batcalc", "timestamp", MTIMEtimestamp_timestamp_bulk, false, "", args(1,2, batarg("",timestamp),batarg("t",timestamp))),
 command("batcalc", "timestamp", MTIMEtimestamp_fromdate_bulk, false, "", args(1,2, batarg("",timestamp),batarg("d",date))),
 command("batcalc", "timestamp", MTIMEtimestamp_fromsecond_bulk, false, "", args(1,2, batarg("",timestamp),batarg("secs",int))),
 command("batcalc", "timestamp", MTIMEtimestamp_frommsec_bulk, false, "", args(1,2, batarg("",timestamp),batarg("msecs",lng))),
 command("batcalc", "daytime", MTIMEdaytime_fromstr_bulk, false, "", args(1,2, batarg("",daytime),batarg("s",str))),
 command("batcalc", "daytime", MTIMEdaytime_daytime_bulk, false, "", args(1,2, batarg("",daytime),batarg("d",daytime))),
 command("batcalc", "daytime", MTIMEdaytime_fromseconds_bulk, false, "", args(1,2, batarg("",daytime),batarg("s",lng))),
 command("batcalc", "daytime", MTIMEtimestamp_extract_daytime_bulk, false, "", args(1,2, batarg("",daytime),batarg("t",timestamp))),
 { .imp=NULL }
};
#include "mal_import.h"
#ifdef _MSC_VER
#undef read
#pragma section(".CRT$XCU",read)
#endif
LIB_STARTUP_FUNC(init_mtime_mal)
{ mal_module("mtime", NULL, mtime_init_funcs); }
