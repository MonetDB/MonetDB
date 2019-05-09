/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#define YEAR_OFFSET		(-YEAR_MIN)
#define DTDAY_WIDTH		5		/* 1..28/29/30/31, depending on month */
#define DTDAY_SHIFT		0
#define DTMONTH_WIDTH	21		/* enough for 174762 years */
#define DTMONTH_SHIFT	(DTDAY_WIDTH+DTDAY_SHIFT)
#define mkdate(y, m, d)	(((((y) + YEAR_OFFSET) * 12 + (m) - 1) << DTMONTH_SHIFT) \
						 | ((d) << DTDAY_SHIFT))
#define date_day(dt)	(((dt) >> DTDAY_SHIFT) & ((1 << DTDAY_WIDTH) - 1))
#define date_month(dt)	((((dt) >> DTMONTH_SHIFT) & ((1 << DTMONTH_WIDTH) - 1)) % 12 + 1)
#define date_year(dt)	((((dt) >> DTMONTH_SHIFT) & ((1 << DTMONTH_WIDTH) - 1)) / 12 - YEAR_OFFSET)

#define mkdaytime(h,m,s,u)	(((((daytime) (h) * 60 + (m)) * 60) + (s)) * LL_CONSTANT(1000000) + (u))

#define TSTIME_WIDTH	37		/* [0..24*60*60*1000000) */
#define TSTIME_SHIFT	0
#define TSDATE_WIDTH	(DTDAY_WIDTH+DTMONTH_WIDTH)
#define TSDATE_SHIFT	(TSTIME_SHIFT+TSTIME_WIDTH)
#define ts_time(ts)		((daytime) (((ts) >> TSTIME_SHIFT) & ((LL_CONSTANT(1) << TSTIME_WIDTH) - 1)))
#define ts_date(ts)		((date) (((ts) >> TSDATE_SHIFT) & ((1 << TSDATE_WIDTH) - 1)))
#define mktimestamp(d, t)	((timestamp) (((uint64_t) (d) << TSDATE_SHIFT) | \
										  ((uint64_t) (t) << TSTIME_SHIFT)))

extern date DATE_MAX, DATE_MIN;		/* often used dates; computed once */
extern date date_add(date dt, int days);
extern date date_addmonth(date dt, int months);
