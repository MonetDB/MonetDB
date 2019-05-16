/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
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
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "mtime.h"
#include "mal_exception.h"

#ifndef HAVE_STRPTIME
extern char *strptime(const char *, const char *, struct tm *);
#endif

#define YEAR_MIN		(-4712)	/* 4713 BC */
#define YEAR_MAX		(YEAR_MIN+(1<<21)/12)

#define YEAR_OFFSET		(-YEAR_MIN)
#define DTDAY_WIDTH		5		/* 1..28/29/30/31, depending on month */
#define DTDAY_SHIFT		0
#define DTMONTH_WIDTH	21		/* enough for 174762 years */
#define DTMONTH_SHIFT	(DTDAY_WIDTH+DTDAY_SHIFT)
#define isdate(y, m, d)	((m) > 0 && (m) <= 12 && (d) > 0 && (y) >= YEAR_MIN && (y) <= YEAR_MAX && (d) <= monthdays(y, m))
#define mkdate(y, m, d)	(((((y) + YEAR_OFFSET) * 12 + (m) - 1) << DTMONTH_SHIFT) \
						 | ((d) << DTDAY_SHIFT))
#define date_extract_day(dt)	(((dt) >> DTDAY_SHIFT) & ((1 << DTDAY_WIDTH) - 1))
#define date_extract_month(dt)	((((dt) >> DTMONTH_SHIFT) & ((1 << DTMONTH_WIDTH) - 1)) % 12 + 1)
#define date_extract_year(dt)	((((dt) >> DTMONTH_SHIFT) & ((1 << DTMONTH_WIDTH) - 1)) / 12 - YEAR_OFFSET)

#define istime(h,m,s,u)	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) <= 60 && (u) >= 0 && (u) < 1000000)
#define mkdaytime(h,m,s,u)	(((((daytime) (h) * 60 + (m)) * 60) + (s)) * LL_CONSTANT(1000000) + (u))

#define TSTIME_WIDTH	37		/* [0..24*60*60*1000000) */
#define TSTIME_SHIFT	0
#define TSDATE_WIDTH	(DTDAY_WIDTH+DTMONTH_WIDTH)
#define TSDATE_SHIFT	(TSTIME_SHIFT+TSTIME_WIDTH)
#define ts_time(ts)		((daytime) (((uint64_t) (ts) >> TSTIME_SHIFT) & ((LL_CONSTANT(1) << TSTIME_WIDTH) - 1)))
#define ts_date(ts)		((date) (((uint64_t) (ts) >> TSDATE_SHIFT) & ((1 << TSDATE_WIDTH) - 1)))
#define mktimestamp(d, t)	((timestamp) (((uint64_t) (d) << TSDATE_SHIFT) | \
										  ((uint64_t) (t) << TSTIME_SHIFT)))

#define unixepoch		mktimestamp(mkdate(1970, 1, 1), mkdaytime(0, 0, 0, 0))

static const int leapdays[13] = { /* days per month in leap year */
	0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};
static const int cumdays[13] = { /* cumulative days in non leap year */
	0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
};
#define isleapyear(y)		((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))
#define monthdays(y, m)		((m) != 2 ? leapdays[m] : 28 + isleapyear(y))

int TYPE_date;
int TYPE_daytime;
int TYPE_timestamp;

date
date_create(int year, int month, int day)
{
	return isdate(year, month, day) ? mkdate(year, month, day) : date_nil;
}

int
date_year(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	return date_extract_year(dt);
}

int
date_month(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	return date_extract_month(dt);
}

int
date_day(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	return date_extract_day(dt);
}

date
date_add_day(date dt, int days)
{
	if (is_date_nil(dt) || is_int_nil(days))
		return date_nil;

	if (abs(days) >= 1 << (DTDAY_WIDTH + DTMONTH_WIDTH))
		return date_nil;		/* overflow for sure */

	int y = date_extract_year(dt);
	int m = date_extract_month(dt);
	int d = date_extract_day(dt);

	d += days;
	while (d <= 0) {
		if (--m == 0) {
			m = 12;
			if (--y < YEAR_MIN)
				return date_nil;
		}
		d += monthdays(y, m);
	}
	while (d > monthdays(y, m)) {
		d -= monthdays(y, m);
		if (++m > 12) {
			m = 1;
			if (++y > YEAR_MAX)
				return date_nil;
		}
	}
	return mkdate(y, m, d);
}

date
date_add_month(date dt, int months)
{
	if (is_date_nil(dt) || is_int_nil(months))
		return date_nil;

	if (abs(months) >= 1 << DTMONTH_WIDTH)
		return date_nil;		/* overflow for sure */

	int y = date_extract_year(dt);
	int m = date_extract_month(dt);
	int d = date_extract_day(dt);
	m += months;
	if (m <= 0) {
		y -= (12 - m) / 12;
		if (y < YEAR_MIN)
			return date_nil;
		m = 12 - (-m % 12);
	} else if (m > 12) {
		y += (m - 1) / 12;
		if (y > YEAR_MAX)
			return date_nil;
		m = (m - 1) % 12 + 1;
	}
	if (d > monthdays(y, m)) {
		d -= monthdays(y, m);
		if (++m > 12) {
			m = 1;
			if (++y > YEAR_MAX)
				return date_nil;
		}
	}
	return mkdate(y, m, d);
}

/* count days (including leap days) since some time before YEAR_MIN */
#define CNT_OFF		(((YEAR_OFFSET+399)/400)*400)
static inline int
date_countdays(date dt)
{
	static_assert(CNT_OFF % 400 == 0, /* for leapyear function to work */
				  "CNT_OFF must be multiple of 400");
	assert(!is_date_nil(dt));
	int y = date_extract_year(dt);
	int m = date_extract_month(dt);
	int y1 = y + CNT_OFF - 1;
	return date_extract_day(dt)
		+ (y+CNT_OFF)*365 + y1/4 - y1/100 + y1/400
		+ cumdays[m-1] + (m > 2 && isleapyear(y));
}

/* return the difference in days between the two dates */
int
date_diff(date d1, date d2)
{
	if (is_date_nil(d1) || is_date_nil(d2))
		return int_nil;
	return date_countdays(d1) - date_countdays(d2);
}

/* 21 April 2019 is a Sunday, we can calculate the offset for the
 * day-of-week calculation below from this fact */
#define DOW_OFF (7 - (((21 + (2019+CNT_OFF)*365 + (2019+CNT_OFF-1)/4 - (2019+CNT_OFF-1)/100 + (2019+CNT_OFF-1)/400 + 90) % 7) + 1))
/* return day of week of given date; Monday = 1, Sunday = 7 */
int
date_dayofweek(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	/* calculate number of days since the start of the year -CNT_OFF */
	int d = date_countdays(dt);
	/* then simply take the remainder from 7 and convert to correct
	 * weekday */
	return (d + DOW_OFF) % 7 + 1;
}

/* week 1 is the week (Monday to Sunday) in which January 4 falls; if
 * January 1 to 3 fall in the week before the 4th, they are in the
 * last week of the previous year; the last days of the year may fall
 * in week 1 of the following year */
int
date_weekofyear(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	int y = date_extract_year(dt);
	int m = date_extract_month(dt);
	int d = date_extract_day(dt);
	int cnt1 = date_countdays(mkdate(y, 1, 4));
	int wd1 = (cnt1 + DOW_OFF) % 7 + 1; /* date_dayofweek(mkdate(y, 1, 4)) */
	int cnt2 = date_countdays(dt);
	int wd2 = (cnt2 + DOW_OFF) % 7 + 1; /* date_dayofweek(dt) */
	if (wd2 > wd1 && m == 1 && d < 4) {
		/* last week of previous year */
		cnt1 = date_countdays(mkdate(y - 1, 1, 4));
		wd1 = (cnt1 + DOW_OFF) % 7 + 1; /* date_dayofweek(mkdate(y-1, 1, 4)) */
	} else if (m == 12 && wd2 + 31 - d < 4)
		return 1;
	if (wd2 < wd1)
		cnt2 += 6;
	return (cnt2 - cnt1) / 7 + 1;
}

int
date_dayofyear(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	int m = date_extract_month(dt);
	return date_extract_day(dt) + cumdays[m-1]
		+ (m > 2 && isleapyear(date_extract_year(dt)));
}

daytime
daytime_create(int hour, int min, int sec, int usec)
{
	return istime(hour, min, sec, usec) ? mkdaytime(hour, min, sec, usec) : daytime_nil;
}

int
daytime_hour(daytime tm)
{
	if (is_daytime_nil(tm))
		return int_nil;
	return (int) (tm / HOUR_USEC);
}

int
daytime_min(daytime tm)
{
	if (is_daytime_nil(tm))
		return int_nil;
	return (int) ((tm / 60000000) % 60);
}

int
daytime_sec(daytime tm)
{
	if (is_daytime_nil(tm))
		return int_nil;
	return (int) ((tm / 1000000) % 60);
}

int
daytime_usec(daytime tm)
{
	if (is_daytime_nil(tm))
		return int_nil;
	return (int) (tm % 1000000);
}

int
daytime_sec_usec(daytime tm)
{
	if (is_daytime_nil(tm))
		return int_nil;
	return (int) (tm % 60000000);
}

daytime
daytime_add_usec(daytime t, lng usec)
{
	if (is_daytime_nil(t) || is_lng_nil(usec))
		return daytime_nil;
	if (llabs(usec) >= DAY_USEC)
		return daytime_nil;		/* overflow for sure */
	t += usec;
	if (t < 0 || t >= DAY_USEC)
		return daytime_nil;
	return t;
}

daytime
daytime_add_usec_modulo(daytime t, lng usec)
{
	if (is_daytime_nil(t) || is_lng_nil(usec))
		return daytime_nil;
	/* if usec < 0, usec%DAY_USEC < 0 */
	t += usec % DAY_USEC;
	if (t < 0)
		t += DAY_USEC;
	else if (t >= DAY_USEC)
		t -= DAY_USEC;
	return t;
}

#if !defined(HAVE_GMTIME_R) || !defined(HAVE_LOCALTIME_R)
static MT_Lock timelock = MT_LOCK_INITIALIZER("timelock");
#endif

/* convert a value returned by the system time() function to a timestamp */
timestamp
timestamp_fromtime(time_t timeval)
{
	struct tm tm, *tmp;
	date d;
	daytime t;

#ifdef HAVE_GMTIME_R
	if ((tmp = gmtime_r(&timeval, &tm)) == NULL)
		return timestamp_nil;
#else
	MT_lock_set(&timelock);
	if ((tmp = gmtime(&timeval)) == NULL) {
		MT_lock_unset(&timelock);
		return timestamp_nil;
	}
	tm = *tmp;					/* copy as quickly as possible */
	tmp = &tm;
	MT_lock_unset(&timelock);
#endif
	if (tmp->tm_sec >= 60)
		tmp->tm_sec = 59;			/* ignore leap seconds */
	d = date_create(tmp->tm_year + 1900, tmp->tm_mon + 1, tmp->tm_mday);
	t = daytime_create(tmp->tm_hour, tmp->tm_min, tmp->tm_sec, 0);
	if (is_date_nil(d) || is_daytime_nil(t))
		return timestamp_nil;
	return mktimestamp(d, t);
}

/* convert a value returned by GDKusec() to a timestamp */
timestamp
timestamp_fromusec(lng usec)
{
	if (is_lng_nil(usec))
		return timestamp_nil;
	return timestamp_add_usec(unixepoch, usec);
}

timestamp
timestamp_fromdate(date dt)
{
	if (is_date_nil(dt))
		return timestamp_nil;
	return mktimestamp(dt, mkdaytime(0, 0, 0, 0));
}

timestamp
timestamp_create(date dt, daytime tm)
{
	if (is_date_nil(dt) || is_daytime_nil(tm))
		return timestamp_nil;
	return mktimestamp(dt, tm);
}

timestamp
timestamp_current(void)
{
#if defined(_MSC_VER)
	FILETIME ft;
	ULARGE_INTEGER l;
	GetSystemTimeAsFileTime(&ft);
	l.LowPart = ft.dwLowDateTime;
	l.HighPart = ft.dwHighDateTime;
	return timestamp_add_usec(mktimestamp(mkdate(1601, 1, 1),
										  mkdaytime(0, 0, 0, 0)),
							  (lng) (l.QuadPart / 10));
#elif defined(HAVE_CLOCK_GETTIME)
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	return timestamp_add_usec(unixepoch,
							  ts.tv_sec * LL_CONSTANT(1000000)
							  + ts.tv_nsec / 1000);
#elif defined(HAVE_GETTIMEOFDAY)
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return timestamp_add_usec(unixepoch,
							  tv.tv_sec * LL_CONSTANT(1000000) + tv.tv_usec);
#else
	return timestamp_add_usec(unixepoch,
							  (lng) time(NULL) * LL_CONSTANT(1000000));
#endif
}

timestamp
timestamp_add_usec(timestamp t, lng usec)
{
	if (is_timestamp_nil(t) || is_lng_nil(usec))
		return timestamp_nil;

	daytime tm = ts_time(t);
	date dt = ts_date(t);

	tm += usec;
	if (tm < 0) {
		int add = (int) ((DAY_USEC - 1 - tm) / DAY_USEC);
		tm += add * DAY_USEC;
		dt = date_add_day(dt, -add);
	} else if (tm >= DAY_USEC) {
		dt = date_add_day(dt, (int) (tm / DAY_USEC));
		tm %= DAY_USEC;
	}
	if (is_date_nil(dt))
		return timestamp_nil;
	return mktimestamp(dt, tm);
}

timestamp
timestamp_add_month(timestamp t, int m)
{
	if (is_timestamp_nil(t) || is_int_nil(m))
		return timestamp_nil;

	daytime tm = ts_time(t);
	date dt = ts_date(t);

	dt = date_add_month(dt, m);
	if (is_date_nil(dt))
		return timestamp_nil;
	return mktimestamp(dt, tm);
}

date
timestamp_date(timestamp t)
{
	if (is_timestamp_nil(t))
		return date_nil;
	return ts_date(t);
}

daytime
timestamp_daytime(timestamp t)
{
	if (is_timestamp_nil(t))
		return daytime_nil;
	return ts_time(t);
}

lng
timestamp_diff(timestamp t1, timestamp t2)
{
	if (is_timestamp_nil(t1) || is_timestamp_nil(t2))
		return lng_nil;
	return ts_time(t1) - ts_time(t2) + DAY_USEC * date_diff(ts_date(t1), ts_date(t2));
}

/* GDK level atom functions with some helpers */
static ssize_t
fleximatch(const char *s, const char *pat, size_t min)
{
	size_t hit;
	bool spacy = false;

	if (min == 0) {
		min = (int) strlen(pat);	/* default minimum required hits */
	}
	for (hit = 0; *pat; hit++) {
		if (tolower((unsigned char) s[hit]) != (unsigned char) *pat) {
			if (GDKisspace(s[hit]) && spacy) {
				min++;
				continue;		/* extra spaces */
			}
			break;
		}
		spacy = GDKisspace(*pat);
		pat++;
	}
	return (hit >= min) ? hit : 0;
}

static ssize_t
parse_substr(int *ret, const char *s, size_t min, const char *list[], int size)
{
	ssize_t j = 0;
	int i = 0;

	*ret = int_nil;
	while (++i <= size) {
		if ((j = fleximatch(s, list[i], min)) > 0) {
			*ret = i;
			break;
		}
	}
	return j;
}

static const char *MONTHS[13] = {
	NULL, "january", "february", "march", "april", "may", "june",
	"july", "august", "september", "october", "november", "december"
};

static ssize_t
parse_date(const char *buf, date *d, bool external)
{
	int day = 0, month = int_nil;
	int year = 0;
	bool yearneg, yearlast = false;
	ssize_t pos = 0;
	int sep;

	*d = date_nil;
	if (strcmp(buf, str_nil) == 0)
		return 1;
	if (external && strncmp(buf, "nil", 3) == 0)
		return 3;
	if ((yearneg = (buf[0] == '-')))
		buf++;
	if (!yearneg && !GDKisdigit(buf[0])) {
#ifdef HAVE_SYNONYMS
		if (!synonyms) {
			GDKerror("Syntax error in date.\n");
			return -1;
		}
#endif
		yearlast = true;
		sep = ' ';
	} else {
		for (pos = 0; GDKisdigit(buf[pos]); pos++) {
			year = (buf[pos] - '0') + year * 10;
			if (year > YEAR_MAX)
				break;
		}
		sep = (unsigned char) buf[pos++];
#ifdef HAVE_SYNONYMS
		if (!synonyms && sep != '-') {
			GDKerror("Syntax error in date.\n");
			return -1;
		}
#endif
		sep = tolower(sep);
		if (sep >= 'a' && sep <= 'z') {
			sep = 0;
		} else if (sep == ' ') {
			while (buf[pos] == ' ')
				pos++;
		} else if (sep != '-' && sep != '/' && sep != '\\') {
			GDKerror("Syntax error in date.\n");
			return -1;
		}
	}
	if (GDKisdigit(buf[pos])) {
		month = buf[pos++] - '0';
		if (GDKisdigit(buf[pos])) {
			month = (buf[pos++] - '0') + month * 10;
		}
#ifdef HAVE_SYNONYMS
	} else if (!synonyms) {
		GDKerror("Syntax error in date.\n");
		return -1;
#endif
	} else {
		pos += parse_substr(&month, buf + pos, 3, MONTHS, 12);
	}
	if (is_int_nil(month) || (sep && buf[pos++] != sep)) {
		GDKerror("Syntax error in date.\n");
		return -1;
	}
	if (sep == ' ') {
		while (buf[pos] == ' ')
			pos++;
	}
	if (!GDKisdigit(buf[pos])) {
		GDKerror("Syntax error in date.\n");
		return -1;
	}
	while (GDKisdigit(buf[pos])) {
		day = (buf[pos++] - '0') + day * 10;
		if (day > 31)
			break;
	}
	if (yearlast && (buf[pos] == ',' || buf[pos] == ' ')) {
		while (buf[++pos] == ' ')
			;
		if (buf[pos] == '-') {
			yearneg = true;
			pos++;
		}
		while (GDKisdigit(buf[pos])) {
			year = (buf[pos++] - '0') + year * 10;
			if (year > YEAR_MAX)
				break;
		}
	}
	/* handle semantic error here (returns nil in that case) */
	*d = date_create(yearneg ? -year : year, month, day);
	if (is_date_nil(*d)) {
		GDKerror("Semantic error in date.\n");
		return -1;
	}
	return pos + yearneg;
}

ssize_t
date_fromstr(const char *buf, size_t *len, date **d, bool external)
{
	if (*len < sizeof(date) || *d == NULL) {
		GDKfree(*d);
		*d = (date *) GDKmalloc(*len = sizeof(date));
		if( *d == NULL)
			return -1;
	}
	return parse_date(buf, *d, external);
}

ssize_t
date_tostr(str *buf, size_t *len, const date *val, bool external)
{
	/* 15 bytes is more than enough */
	if (*len < 15 || *buf == NULL) {
		GDKfree(*buf);
		*buf = GDKmalloc(15);
		if( *buf == NULL)
			return -1;
		*len = 15;
	}
	if (is_date_nil(*val)) {
		if (external) {
			strcpy(*buf, "nil");
			return 3;
		}
		strcpy(*buf, str_nil);
		return 1;
	}
	return (ssize_t) snprintf(*buf, *len, "%d-%02d-%02d",
							  date_extract_year(*val), date_extract_month(*val),
							  date_extract_day(*val));
}

static ssize_t
parse_daytime(const char *buf, daytime *dt, bool external)
{
	int hour, min, sec = 0, usec = 0;
	ssize_t pos = 0;

	*dt = daytime_nil;
	if (strcmp(buf, str_nil) == 0)
		return 1;
	if (external && strncmp(buf, "nil", 3) == 0)
		return 3;
	if (!GDKisdigit(buf[pos])) {
		GDKerror("Syntax error in time.\n");
		return -1;
	}
	for (hour = 0; GDKisdigit(buf[pos]); pos++) {
		if (hour <= 24)
			hour = (buf[pos] - '0') + hour * 10;
	}
	if ((buf[pos++] != ':') || !GDKisdigit(buf[pos])) {
		GDKerror("Syntax error in time.\n");
		return -1;
	}
	for (min = 0; GDKisdigit(buf[pos]); pos++) {
		if (min <= 60)
			min = (buf[pos] - '0') + min * 10;
	}
	if ((buf[pos] == ':') && GDKisdigit(buf[pos + 1])) {
		for (pos++, sec = 0; GDKisdigit(buf[pos]); pos++) {
			if (sec <= 60)
				sec = (buf[pos] - '0') + sec * 10;
		}
		if ((buf[pos] == '.' || (
#ifdef HAVE_SYNONYMS
				 synonyms &&
#endif
				 buf[pos] == ':')) &&
			GDKisdigit(buf[pos + 1])) {
			int i;
			pos++;
			for (i = 0; i < 6; i++) {
				usec *= 10;
				if (GDKisdigit(buf[pos])) {
					usec += buf[pos] - '0';
					pos++;
				}
			}
#ifndef TRUNCATE_NUMBERS
			if (GDKisdigit(buf[pos]) && buf[pos] >= '5') {
				/* round the value */
				if (++usec == 1000000) {
					usec = 0;
					if (++sec == 60) {
						sec = 0;
						if (++min == 60) {
							min = 0;
							if (++hour == 24) {
								/* forget about rounding if it doesn't fit */
								hour = 23;
								min = 59;
								sec = 59;
								usec = 999999;
							}
						}
					}
				}
			}
#endif
			while (GDKisdigit(buf[pos]))
				pos++;
		}
	}
	/* handle semantic error here (returns nil in that case) */
	*dt = daytime_create(hour, min, sec, usec);
	if (is_daytime_nil(*dt)) {
		GDKerror("Semantic error in time.\n");
		return -1;
	}
	return pos;
}

ssize_t
daytime_fromstr(const char *buf, size_t *len, daytime **ret, bool external)
{
	if (*len < sizeof(daytime) || *ret == NULL) {
		GDKfree(*ret);
		*ret = (daytime *) GDKmalloc(*len = sizeof(daytime));
		if (*ret == NULL)
			return -1;
	}
	return parse_daytime(buf, *ret, external);
}

ssize_t
daytime_tz_fromstr(const char *buf, size_t *len, daytime **ret, bool external)
{
	const char *s = buf;
	ssize_t pos = daytime_fromstr(s, len, ret, external);
	daytime val;
	int offset = 0;

	if (pos < 0 || is_daytime_nil(**ret))
		return pos;

	s = buf + pos;
	pos = 0;
	while (GDKisspace(*s))
		s++;
	/* in case of gmt we need to add the time zone */
	if (fleximatch(s, "gmt", 0) == 3) {
		s += 3;
	}
	if ((s[0] == '-' || s[0] == '+') &&
		GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
		((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
		offset = (((s[1] - '0') * 10 + (s[2] - '0')) * 60 + (s[pos] - '0') * 10 + (s[pos + 1] - '0')) * 60;
		pos += 2;
		if (s[0] != '-')
			offset = -offset;
		s += pos;
	}
	val = **ret + (lng) offset * 1000000;
	if (val < 0)
		**ret = DAY_USEC + val;
	else if (val >= DAY_USEC)
		**ret = val - DAY_USEC;
	else
		**ret = val;
	return (ssize_t) (s - buf);
}

ssize_t
daytime_precision_tostr(str *buf, size_t *len, const daytime dt,
						int precision, bool external)
{
	int hour, min, sec, usec;

	if (precision < 0)
		precision = 0;
	if (*len < 10 + (size_t) precision || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 10 + (size_t) precision);
		if( *buf == NULL)
			return -1;
	}
	if (is_daytime_nil(dt)) {
		if (external) {
			strcpy(*buf, "nil");
			return 3;
		}
		strcpy(*buf, str_nil);
		return 1;
	}
	usec = (int) (dt % 1000000);
	sec = (int) (dt / 1000000);
	hour = sec / 3600;
	min = (sec % 3600) / 60;
	sec %= 60;

	if (precision == 0)
		return snprintf(*buf, *len, "%02d:%02d:%02d", hour, min, sec);
	else if (precision < 6) {
		for (int i = 0; i < precision; i++)
			usec /= 10;
		return snprintf(*buf, *len, "%02d:%02d:%02d.%0*d", hour, min, sec, precision, usec);
	} else {
		ssize_t l = snprintf(*buf, *len, "%02d:%02d:%02d.%06d", hour, min, sec, usec);
		while (precision > 6) {
			precision--;
			(*buf)[l++] = '0';
		}
		(*buf)[l] = '\0';
		return l;
	}
}

ssize_t
daytime_tostr(str *buf, size_t *len, const daytime *val, bool external)
{
	return daytime_precision_tostr(buf, len, *val, 6, external);
}

ssize_t
timestamp_fromstr(const char *buf, size_t *len, timestamp **ret, bool external)
{
	const char *s = buf;
	ssize_t pos;
	date dt;
	daytime tm;

	if (*len < sizeof(timestamp) || *ret == NULL) {
		GDKfree(*ret);
		*ret = (timestamp *) GDKmalloc(*len = sizeof(timestamp));
		if (*ret == NULL)
			return -1;
	}
	pos = parse_date(buf, &dt, external);
	if (pos < 0)
		return pos;
	if (is_date_nil(dt)) {
		**ret = timestamp_nil;
		return pos;
	}
	s += pos;
	if (*s == '@' || *s == ' ' || *s == '-' || *s == 'T') {
		while (*++s == ' ')
			;
		pos = parse_daytime(s, &tm, external);
		if (pos < 0)
			return pos;
		s += pos;
		if (is_daytime_nil(tm)) {
			**ret = timestamp_nil;
			return (ssize_t) (s - buf);
		}
	} else if (*s) {
		tm = daytime_nil;
	} else {
		tm = mkdaytime(0, 0, 0, 0);
	}
	if (is_date_nil(dt) || is_daytime_nil(tm)) {
		**ret = timestamp_nil;
	} else {
		lng offset = 0;

		**ret = mktimestamp(dt, tm);
		while (GDKisspace(*s))
			s++;
		/* in case of gmt we need to add the time zone */
		if (fleximatch(s, "gmt", 0) == 3) {
			s += 3;
		}
		if ((s[0] == '-' || s[0] == '+') &&
			GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
			((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
			offset = (((s[1] - '0') * LL_CONSTANT(10) + (s[2] - '0')) * LL_CONSTANT(60) + (s[pos] - '0') * LL_CONSTANT(10) + (s[pos + 1] - '0')) * LL_CONSTANT(60000000);
			pos += 2;
			if (s[0] != '-')
				offset = -offset;
			s += pos;
		}
		**ret = timestamp_add_usec(**ret, offset);
	}
	return (ssize_t) (s - buf);
}

ssize_t
timestamp_tz_fromstr(const char *buf, size_t *len, timestamp **ret, bool external)
{
	const char *s = buf;
	ssize_t pos = timestamp_fromstr(s, len, ret, external);
	lng offset = 0;

	if (pos < 0 || is_timestamp_nil(**ret))
		return pos;

	s = buf + pos;
	pos = 0;
	while (GDKisspace(*s))
		s++;
	/* incase of gmt we need to add the time zone */
	if (fleximatch(s, "gmt", 0) == 3) {
		s += 3;
	}
	if ((s[0] == '-' || s[0] == '+') &&
		GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
		((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
		offset = (((s[1] - '0') * LL_CONSTANT(10) + (s[2] - '0')) * LL_CONSTANT(60) + (s[pos] - '0') * LL_CONSTANT(10) + (s[pos + 1] - '0')) * LL_CONSTANT(60000000);
		pos += 2;
		if (s[0] != '-')
			offset = -offset;
		s += pos;
	}
	**ret = timestamp_add_usec(**ret, offset);
	return (ssize_t) (s - buf);
}

ssize_t
timestamp_precision_tostr(str *buf, size_t *len, timestamp val, int precision, bool external)
{
	ssize_t len1, len2;
	size_t big = 128;
	char buf1[128], buf2[128], *s = *buf, *s1 = buf1, *s2 = buf2;
	date dt;
	daytime tm;

	if (is_timestamp_nil(val)) {
		if (*len < 4 || *buf == NULL) {
			GDKfree(*buf);
			*buf = GDKmalloc(*len = 4);
			if( *buf == NULL)
				return -1;
		}
		if (external) {
			strcpy(*buf, "nil");
			return 3;
		}
		strcpy(*buf, str_nil);
		return 1;
	}

	dt = ts_date(val);
	tm = ts_time(val);
	len1 = date_tostr(&s1, &big, &dt, false);
	len2 = daytime_precision_tostr(&s2, &big, tm, precision, false);
	if (len1 < 0 || len2 < 0)
		return -1;

	if (*len < 2 + (size_t) len1 + (size_t) len2 || *buf == NULL) {
		GDKfree(*buf);
		*buf = GDKmalloc(*len = (size_t) len1 + (size_t) len2 + 2);
		if( *buf == NULL)
			return -1;
	}
	s = *buf;
	strcpy(s, buf1);
	s += len1;
	*s++ = ' ';
	strcpy(s, buf2);
	s += len2;
	return (ssize_t) (s - *buf);
}

ssize_t
timestamp_tostr(str *buf, size_t *len, const timestamp *val, bool external)
{
	return timestamp_precision_tostr(buf, len, *val, 6, external);
}

/* interfaces callable from MAL, not used from any C code */
mal_export str MTIMEprelude(void *ret);
mal_export str MTIMEcurrent_date(date *ret);
mal_export str MTIMEcurrent_time(daytime *ret);
mal_export str MTIMEcurrent_timestamp(timestamp *ret);
mal_export str MTIMEdate_diff(int *ret, const date *v1, const date *v2);
mal_export str MTIMEdate_diff_bulk(bat *ret, bat *bid1, bat *bid2);
mal_export str MTIMEdate_sub_msec_interval(date *ret, const date *d, const lng *ms);
mal_export str MTIMEdate_add_msec_interval(date *ret, const date *d, const lng *ms);
mal_export str MTIMEtimestamp_sub_msec_interval(timestamp *ret, const timestamp *t, const lng *ms);
mal_export str MTIMEtimestamp_add_msec_interval(timestamp *ret, const timestamp *t, const lng *ms);
mal_export str MTIMEtimestamp_sub_month_interval(timestamp *ret, const timestamp *t, const int *m);
mal_export str MTIMEtimestamp_add_month_interval(timestamp *ret, const timestamp *t, const int *m);
mal_export str MTIMEtime_sub_msec_interval(daytime *ret, const daytime *t, const lng *ms);
mal_export str MTIMEtime_add_msec_interval(daytime *ret, const daytime *t, const lng *ms);
mal_export str MTIMEdaytime_diff_msec(lng *ret, const daytime *t1, const daytime *t2);
mal_export str MTIMEtimestamp_diff_msec_bulk(bat *ret, bat *bid1, bat *bid2);
mal_export str MTIMEdate_submonths(date *ret, const date *d, const int *m);
mal_export str MTIMEdate_addmonths(date *ret, const date *d, const int *m);
mal_export str MTIMEdate_extract_century(int *ret, const date *d);
mal_export str MTIMEdate_extract_century_bulk(bat *ret, bat *bid);
mal_export str MTIMEdate_extract_decade(int *ret, const date *d);
mal_export str MTIMEdate_extract_decade_bulk(bat *ret, bat *bid);
mal_export str MTIMEdate_extract_year(int *ret, const date *d);
mal_export str MTIMEdate_extract_year_bulk(bat *ret, bat *bid);
mal_export str MTIMEdate_extract_quarter(int *ret, const date *d);
mal_export str MTIMEdate_extract_quarter_bulk(bat *ret, bat *bid);
mal_export str MTIMEdate_extract_month(int *ret, const date *d);
mal_export str MTIMEdate_extract_month_bulk(bat *ret, bat *bid);
mal_export str MTIMEdate_extract_day(int *ret, const date *d);
mal_export str MTIMEdate_extract_day_bulk(bat *ret, bat *bid);
mal_export str MTIMEdaytime_extract_hours(int *ret, const daytime *t);
mal_export str MTIMEdaytime_extract_hours_bulk(bat *ret, bat *bid);
mal_export str MTIMEdaytime_extract_minutes(int *ret, const daytime *t);
mal_export str MTIMEdaytime_extract_minutes_bulk(bat *ret, bat *bid);
mal_export str MTIMEdaytime_extract_sql_seconds(int *ret, const daytime *t);
mal_export str MTIMEdaytime_extract_sql_seconds_bulk(bat *ret, bat *bid);
mal_export str MTIMEdate_extract_dayofyear(int *ret, const date *d);
mal_export str MTIMEdate_extract_weekofyear(int *ret, const date *d);
mal_export str MTIMEdate_extract_dayofweek(int *ret, const date *d);
mal_export str MTIMEtimestamp_diff_msec(lng *ret, const timestamp *t1, const timestamp *t2);
mal_export str MTIMEtimestamp_century(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_decade(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_year(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_quarter(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_month(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_day(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_hours(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_minutes(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_sql_seconds(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_sql_seconds_bulk(bat *ret, bat *bid);
mal_export str MTIMEsql_year(int *ret, const int *months);
mal_export str MTIMEsql_month(int *ret, const int *months);
mal_export str MTIMEsql_day(lng *ret, const lng *msecs);
mal_export str MTIMEsql_hours(int *ret, const lng *msecs);
mal_export str MTIMEsql_minutes(int *ret, const lng *msecs);
mal_export str MTIMEsql_seconds(int *ret, const lng *msecs);

mal_export str MTIMEdate_fromstr(date *ret, const char *const *s);
mal_export str MTIMEdate_date(date *dst, const date *src);
mal_export str MTIMEtimestamp_extract_date(date *ret, const timestamp *t);
mal_export str MTIMEtimestamp_extract_date_bulk(bat *ret, bat *bid);
mal_export str MTIMEtimestamp_fromstr(timestamp *ret, const char *const *s);
mal_export str MTIMEtimestamp_timestamp(timestamp *dst, const timestamp *src);
mal_export str MTIMEtimestamp_fromdate(timestamp *ret, const date *d);
mal_export str MTIMEtimestamp_fromdate_bulk(bat *ret, bat *bid);
mal_export str MTIMEseconds_since_epoch(int *ret, const timestamp *t);
mal_export str MTIMEtimestamp_fromsecond(timestamp *ret, const int *secs);
mal_export str MTIMEtimestamp_fromsecond_bulk(bat *ret, bat *bid);
mal_export str MTIMEtimestamp_frommsec(timestamp *ret, const lng *msecs);
mal_export str MTIMEtimestamp_frommsec_bulk(bat *ret, bat *bid);
mal_export str MTIMEdaytime_fromstr(daytime *ret, const char *const *s);
mal_export str MTIMEdaytime_daytime(daytime *dst, const daytime *src);
mal_export str MTIMEdaytime_fromseconds(daytime *ret, const lng *secs);
mal_export str MTIMEdaytime_fromseconds_bulk(bat *ret, bat *bid);
mal_export str MTIMEtimestamp_extract_daytime(daytime *ret, const timestamp *t);
mal_export str MTIMEtimestamp_extract_daytime_bulk(bat *ret, bat *bid);
mal_export str MTIMElocal_timezone_msec(lng *ret);
mal_export str MTIMEstr_to_date(date *ret, const char *const *s, const char *const *format);
mal_export str MTIMEdate_to_str(str *ret, const date *d, const char *const *format);
mal_export str MTIMEstr_to_time(daytime *ret, const char *const *s, const char *const *format);
mal_export str MTIMEtime_to_str(str *ret, const daytime *d, const char *const *format);
mal_export str MTIMEstr_to_timestamp(timestamp *ret, const char *const *s, const char *const *format);
mal_export str MTIMEtimestamp_to_str(str *ret, const timestamp *d, const char *const *format);

str
MTIMEprelude(void *ret)
{
	(void) ret;

	TYPE_date = ATOMindex("date");
	TYPE_daytime = ATOMindex("daytime");
	TYPE_timestamp = ATOMindex("timestamp");
	return MAL_SUCCEED;
}

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

str
MTIMEdate_diff(int *ret, const date *v1, const date *v2)
{
	*ret = date_diff(*v1, *v2);
	return MAL_SUCCEED;
}

str
MTIMEdate_diff_bulk(bat *ret, bat *bid1, bat *bid2)
{
	BAT *b1, *b2, *bn;
	BUN n;
	const date *d1, *d2;
	int *df;

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	if (b1 == NULL || b2 == NULL) {
		if (b1)
			BBPunfix(b1->batCacheid);
		if (b2)
			BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	n = BATcount(b1);
	if (n != BATcount(b2)) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", "inputs not the same size");
	}
	if ((bn = COLnew(b1->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d1 = Tloc(b1, 0);
	d2 = Tloc(b2, 0);
	df = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		df[i] = date_diff(d1[i], d2[i]);
		bn->tnil |= is_int_nil(df[i]);
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
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

str
MTIMEtimestamp_sub_msec_interval(timestamp *ret, const timestamp *t, const lng *ms)
{
	if (is_timestamp_nil(*t) || is_lng_nil(*ms))
		*ret = timestamp_nil;
	else {
		*ret = timestamp_add_usec(*t, -*ms * 1000);
		if (is_timestamp_nil(*ret))
			throw(MAL, "mtime.timestamp_sub_msec_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_add_msec_interval(timestamp *ret, const timestamp *t, const lng *ms)
{
	if (is_timestamp_nil(*t) || is_lng_nil(*ms))
		*ret = timestamp_nil;
	else {
		*ret = timestamp_add_usec(*t, *ms * 1000);
		if (is_timestamp_nil(*ret))
			throw(MAL, "mtime.timestamp_add_msec_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

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

str
MTIMEdate_extract_century(int *ret, const date *d)
{
	if (is_date_nil(*d)) {
		*ret = int_nil;
	} else {
		int y = date_extract_year(*d);
		if (y > 0)
			*ret = (y - 1) / 100 + 1;
		else
			*ret = -((-y - 1) / 100 + 1);
	}
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_century_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const date *d;
	int *y;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.century", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.century", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	y = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_date_nil(d[i])) {
			y[i] = int_nil;
			bn->tnil |= is_int_nil(y[i]);
		} else {
			y[i] = date_extract_year(d[i]);
			if (y[i] > 0)
				y[i] = (y[i] - 1) / 100 + 1;
			else
				y[i] = -((-y[i] - 1) / 100 + 1);
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

str
MTIMEdate_extract_decade(int *ret, const date *d)
{
	if (is_date_nil(*d)) {
		*ret = int_nil;
	} else {
		*ret = date_extract_year(*d) / 10;
	}
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_decade_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const date *d;
	int *y;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.decade", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.decade", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	y = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_date_nil(d[i])) {
			y[i] = int_nil;
			bn->tnil |= is_int_nil(y[i]);
		} else {
			y[i] = date_extract_year(d[i]) / 10;
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

str
MTIMEdate_extract_year(int *ret, const date *d)
{
	*ret = date_year(*d);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_year_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const date *d;
	int *y;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.year", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.year", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	y = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		y[i] = date_year(d[i]);
		bn->tnil |= is_int_nil(y[i]);
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

str
MTIMEdate_extract_quarter(int *ret, const date *d)
{
	*ret = is_date_nil(*d) ? int_nil : (date_extract_month(*d) - 1) / 3 + 1;
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_quarter_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const date *d;
	int *q;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.quarter", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.quarter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	q = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_date_nil(d[i])) {
			bn->tnil = true;
			q[i] = int_nil;
		} else {
			q[i] = (date_extract_month(d[i]) - 1) / 3 + 1;
		}
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_month(int *ret, const date *d)
{
	*ret = date_month(*d);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_month_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const date *d;
	int *m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.month", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.month", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	m = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		m[i] = date_month(d[i]);
		bn->tnil |= is_int_nil(m[i]);
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_day(int *ret, const date *d)
{
	*ret = date_day(*d);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_day_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const date *d;
	int *m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.day", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.day", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	m = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		m[i] = date_day(d[i]);
		bn->tnil |= is_int_nil(m[i]);
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_hours(int *ret, const daytime *t)
{
	*ret = daytime_hour(*t);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_hours_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const daytime *d;
	int *m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.hours", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.hours", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	m = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		m[i] = daytime_hour(d[i]);
		bn->tnil |= is_int_nil(m[i]);
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

str
MTIMEdaytime_extract_minutes(int *ret, const daytime *t)
{
	*ret = daytime_min(*t);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_minutes_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const daytime *d;
	int *m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.minutes", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.minutes", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	m = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		m[i] = daytime_min(d[i]);
		bn->tnil |= is_int_nil(m[i]);
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_sql_seconds(int *ret, const daytime *t)
{
	*ret = is_daytime_nil(*t) ? int_nil : (int) (*t % 60000000);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_sql_seconds_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const daytime *d;
	int *m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.sql_seconds", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.sql_seconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	m = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_daytime_nil(d[i])) {
			m[i] = int_nil;
			bn->tnil = true;
		} else {
			m[i] = (int) (d[i] % 60000000);
		}
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

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

str
MTIMEtimestamp_diff_msec(lng *ret, const timestamp *t1, const timestamp *t2)
{
	*ret = timestamp_diff(*t1, *t2);
	if (!is_lng_nil(*ret)) {
#ifndef TRUNCATE_NUMBERS
		if (*ret < 0)
			*ret = -((-*ret + 500) / 1000);
		else
			*ret = (*ret + 500) / 1000;
#else
		*ret /= 1000;
#endif
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_diff_msec_bulk(bat *ret, bat *bid1, bat *bid2)
{
	BAT *b1, *b2, *bn;
	BUN n;
	const timestamp *d1, *d2;
	lng *df;

	b1 = BATdescriptor(*bid1);
	b2 = BATdescriptor(*bid2);
	if (b1 == NULL || b2 == NULL) {
		if (b1)
			BBPunfix(b1->batCacheid);
		if (b2)
			BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	n = BATcount(b1);
	if (n != BATcount(b2)) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", "inputs not the same size");
	}
	if ((bn = COLnew(b1->hseqbase, TYPE_lng, n, TRANSIENT)) == NULL) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d1 = Tloc(b1, 0);
	d2 = Tloc(b2, 0);
	df = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		df[i] = timestamp_diff(d1[i], d2[i]);
		if (is_lng_nil(df[i]))
			bn->tnil = true;
		else {
#ifndef TRUNCATE_NUMBERS
			if (df[i] < 0)
				df[i] = -((-df[i] + 500) / 1000);
			else
				df[i] = (df[i] + 500) / 1000;
#else
			df[i] /= 1000;
#endif
		}
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b1->batCacheid);
	BBPunfix(b2->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_century(int *ret, const timestamp *t)
{
	if (is_timestamp_nil(*t)) {
		*ret = int_nil;
	} else {
		int y = date_extract_year(ts_date(*t));
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
		*ret = date_extract_year(ts_date(*t)) / 10;
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
	*ret = is_timestamp_nil(*t) ? int_nil : (date_extract_month(ts_date(*t)) - 1) / 3 + 1;
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

str
MTIMEtimestamp_sql_seconds(int *ret, const timestamp *t)
{
	*ret = is_timestamp_nil(*t) ? int_nil : (int) (ts_time(*t) % 60000000);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_sql_seconds_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const timestamp *t;
	int *m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.sql_seconds", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.sql_seconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = Tloc(b, 0);
	m = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_timestamp_nil(t[i])) {
			m[i] = int_nil;
			bn->tnil = true;
		} else {
			m[i] = (int) (ts_time(t[i]) % 60000000);
		}
	}
	bn->tnonil = !bn->tnil;
	BATsetcount(bn, n);
	bn->tsorted = n < 2;
	bn->trevsorted = n < 2;
	bn->tkey = false;
	BBPunfix(b->batCacheid);
	BBPkeepref(*ret = bn->batCacheid);
	return MAL_SUCCEED;
}

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

str
MTIMEtimestamp_extract_date(date *ret, const timestamp *t)
{
	*ret = timestamp_date(*t);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_extract_date_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const timestamp *t;
	date *d;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.date", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_date, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.date", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = Tloc(b, 0);
	d = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		d[i] = timestamp_date(t[i]);
		bn->tnil |= is_int_nil(d[i]);
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

str
MTIMEtimestamp_fromdate(timestamp *ret, const date *d)
{
	*ret = timestamp_fromdate(*d);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_fromdate_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const date *d;
	timestamp *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = Tloc(b, 0);
	t = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		t[i] = timestamp_fromdate(d[i]);
		bn->tnil |= is_timestamp_nil(t[i]);
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

str
MTIMEseconds_since_epoch(int *ret, const timestamp *t)
{
	lng df = timestamp_diff(*t, unixepoch);
	*ret = is_lng_nil(df) ? int_nil : (int) (df / 1000000);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_fromsecond(timestamp *ret, const int *secs)
{
	*ret = is_int_nil(*secs) ? timestamp_nil : timestamp_add_usec(unixepoch, *secs * LL_CONSTANT(1000000));
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_fromsecond_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const int *s;
	timestamp *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	s = Tloc(b, 0);
	t = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_int_nil(s[i])) {
			t[i] = timestamp_nil;
			bn->tnil = true;
		} else {
			t[i] = timestamp_add_usec(unixepoch, s[i] * LL_CONSTANT(1000000));
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

str
MTIMEtimestamp_frommsec(timestamp *ret, const lng *msecs)
{
	*ret = is_lng_nil(*msecs) ? timestamp_nil : timestamp_add_usec(unixepoch, *msecs * 1000);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_frommsec_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const lng *ms;
	timestamp *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	ms = Tloc(b, 0);
	t = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_lng_nil(ms[i])) {
			t[i] = timestamp_nil;
			bn->tnil = true;
		} else {
			t[i] = timestamp_add_usec(unixepoch, ms[i] * 1000);
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
		throw(MAL, "batcalc.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

str
MTIMEtimestamp_extract_daytime(daytime *ret, const timestamp *t)
{
	*ret = timestamp_daytime(*t);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_extract_daytime_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	BUN n;
	const timestamp *t;
	daytime *d;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.daytime", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_daytime, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = Tloc(b, 0);
	d = Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		d[i] = timestamp_daytime(t[i]);
		bn->tnil |= is_int_nil(d[i]);
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

str
MTIMElocal_timezone_msec(lng *ret)
{
	int tzone;

#if defined(_MSC_VER)
	DYNAMIC_TIME_ZONE_INFORMATION tzinf;

	/* documentation says: UTC = localtime + Bias (in minutes),
	 * but experimentation during DST period says, UTC = localtime
	 * + Bias + DaylightBias, and presumably during non DST
	 * period, UTC = localtime + Bias */
	switch (GetDynamicTimeZoneInformation(&tzinf)) {
	case TIME_ZONE_ID_STANDARD:
	case TIME_ZONE_ID_UNKNOWN:
		tzone = -(int) tzinf.Bias * 60;
		break;
	case TIME_ZONE_ID_DAYLIGHT:
		tzone = -(int) (tzinf.Bias + tzinf.DaylightBias) * 60;
		break;
	default:
		/* call failed, we don't know the time zone */
		tzone = 0;
		break;
	}
#elif defined(HAVE_STRUCT_TM_TM_ZONE)
	time_t t;
	struct tm *tmp;

	t = time(NULL);
	tmp = localtime(&t);
	tzone = (int) tmp->tm_gmtoff;
#else
	time_t t;
	timestamp lt, gt;
	struct tm tm, *tmp;

	t = time(NULL);
#ifdef HAVE_GMTIME_R
	tmp = gmtime_r(&t, &tm);
#else
	MT_lock_set(&timelock);
	tmp = gmtime(&t);
	tm = *tmp;
	tmp = &tm;
	MT_lock_unset(&timelock);
#endif
	gt = mktimestamp(mkdate(tmp->tm_year + 1900,
							tmp->tm_mon + 1,
							tmp->tm_mday),
					 mkdaytime(tmp->tm_hour,
							   tmp->tm_min,
							   tmp->tm_sec == 60 ? 59 : tmp->tm_sec,
							   0));
#ifdef HAVE_LOCALTIME_R
	tmp = localtime_r(&t, &tm);
#else
	MT_lock_set(&timelock);
	tmp = localtime(&t);
	tm = *tmp;
	tmp = &tm;
	MT_lock_unset(&timelock);
#endif
	lt = mktimestamp(mkdate(tmp->tm_year + 1900,
							tmp->tm_mon + 1,
							tmp->tm_mday),
					 mkdaytime(tmp->tm_hour,
							   tmp->tm_min,
							   tmp->tm_sec == 60 ? 59 : tmp->tm_sec,
							   0));
	tzone = (int) (timestamp_diff(lt, gt) / 1000000);
#endif
	*ret = tzone * 1000;
	return MAL_SUCCEED;
}

str
MTIMEstr_to_date(date *ret, const char *const *s, const char *const *format)
{
	struct tm tm;

	if (GDK_STRNIL(*s) || GDK_STRNIL(*format)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	if (strptime(*s, *format, &tm) == NULL)
		throw(MAL, "mtime.str_to_date", "format '%s', doesn't match date '%s'",
			  *format, *s);
	*ret = mkdate(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
	if (is_date_nil(*ret))
		throw(MAL, "mtime.str_to_date", "bad date '%s'", *s);
	return MAL_SUCCEED;
}

str
MTIMEdate_to_str(str *ret, const date *d, const char *const *format)
{
	char buf[512];
	struct tm tm;

	if (is_date_nil(*d) || GDK_STRNIL(*format)) {
		*ret = GDKstrdup(str_nil);
		if (*ret == NULL)
			throw(MAL, "mtime.date_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	tm = (struct tm) {
		.tm_year = date_extract_year(*d) - 1900,
		.tm_mon = date_extract_month(*d) - 1,
		.tm_mday = date_extract_day(*d),
		.tm_isdst = -1,
	};
	if (mktime(&tm) == (time_t) -1)
		throw(MAL, "mtime.date_to_str", "cannot convert date");
	if (strftime(buf, sizeof(buf), *format, &tm) == 0)
		throw(MAL, "mtime.date_to_str", "cannot convert date");
	*ret = GDKstrdup(buf);
	if (*ret == NULL)
		throw(MAL, "mtime.date_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
MTIMEstr_to_time(daytime *ret, const char *const *s, const char *const *format)
{
	struct tm tm;

	if (GDK_STRNIL(*s) || GDK_STRNIL(*format)) {
		*ret = daytime_nil;
		return MAL_SUCCEED;
	}
	if (strptime(*s, *format, &tm) == NULL)
		throw(MAL, "mtime.str_to_time", "format '%s', doesn't match time '%s'",
			  *format, *s);
	*ret = mkdaytime(tm.tm_hour, tm.tm_min, tm.tm_sec == 60 ? 59 : tm.tm_sec, 0);
	if (is_daytime_nil(*ret))
		throw(MAL, "mtime.str_to_time", "bad time '%s'", *s);
	return MAL_SUCCEED;
}

str
MTIMEtime_to_str(str *ret, const daytime *d, const char *const *format)
{
	char buf[512];
	daytime dt = *d;
	struct tm tm;

	if (is_daytime_nil(dt) || GDK_STRNIL(*format)) {
		*ret = GDKstrdup(str_nil);
		if (*ret == NULL)
			throw(MAL, "mtime.time_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	time_t now = time(NULL);
	/* fill in current date in struct tm */
#ifdef HAVE_LOCALTIME_R
	localtime_r(&now, &tm);
#else
	MT_lock_set(&timelock);
	tm = *localtime(&now);
	MT_lock_unset(&timelock);
#endif
	/* replace time with requested time */
	dt /= 1000000;
	tm.tm_sec = dt % 60;
	dt /= 60;
	tm.tm_min = dt % 60;
	dt /= 60;
	tm.tm_hour = (int) dt;
	if (mktime(&tm) == (time_t) -1)
		throw(MAL, "mtime.time_to_str", "cannot convert time");
	if (strftime(buf, sizeof(buf), *format, &tm) == 0)
		throw(MAL, "mtime.time_to_str", "cannot convert time");
	*ret = GDKstrdup(buf);
	if (*ret == NULL)
		throw(MAL, "mtime.time_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
MTIMEstr_to_timestamp(timestamp *ret, const char *const *s, const char *const *format)
{
	struct tm tm = (struct tm) {0};

	if (GDK_STRNIL(*s) || GDK_STRNIL(*format)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	if (strptime(*s, *format, &tm) == NULL)
		throw(MAL, "mtime.str_to_timestamp",
			  "format '%s', doesn't match timestamp '%s'", *format, *s);
	*ret = mktimestamp(mkdate(tm.tm_year + 1900,
							  tm.tm_mon + 1,
							  tm.tm_mday),
					   mkdaytime(tm.tm_hour,
								 tm.tm_min,
								 tm.tm_sec == 60 ? 59 : tm.tm_sec,
								 0));
	if (is_timestamp_nil(*ret))
		throw(MAL, "mtime.str_to_timestamp", "bad timestamp '%s'", *s);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_to_str(str *ret, const timestamp *d, const char *const *format)
{
	char buf[512];
	date dt;
	daytime t;
	struct tm tm;

	if (is_timestamp_nil(*d) || GDK_STRNIL(*format)) {
		*ret = GDKstrdup(str_nil);
		if (*ret == NULL)
			throw(MAL, "mtime.timestamp_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	dt = ts_date(*d);
	t = ts_time(*d);
	tm = (struct tm) {
		.tm_year = date_extract_year(dt) - 1900,
		.tm_mon = date_extract_month(dt) - 1,
		.tm_mday = date_extract_day(dt),
		.tm_isdst = -1,
	};
	t /= 1000000;
	tm.tm_sec = t % 60;
	t /= 60;
	tm.tm_min = t % 60;
	t /= 60;
	tm.tm_hour = (int) t;
	if (mktime(&tm) == (time_t) -1)
		throw(MAL, "mtime.timestamp_to_str", "cannot convert timestamp");
	if (strftime(buf, sizeof(buf), *format, &tm) == 0)
		throw(MAL, "mtime.timestamp_to_str", "cannot convert timestamp");
	*ret = GDKstrdup(buf);
	if (*ret == NULL)
		throw(MAL, "mtime.timestamp_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}
