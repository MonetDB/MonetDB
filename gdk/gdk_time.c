/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "gdk.h"
#include "gdk_time.h"

#define YEAR_MIN		(-4712)	/* 4713 BC */

#define YEAR_OFFSET		(-YEAR_MIN)
#define DTDAY_WIDTH		5		/* 1..28/29/30/31, depending on month/year */
#define DTDAY_SHIFT		0
#define DTMONTH_WIDTH	21		/* enough for 174761 years (and 8 months) */
#define DTMONTH_SHIFT	(DTDAY_WIDTH+DTDAY_SHIFT)

#define YEAR_MAX		(YEAR_MIN+(1<<DTMONTH_WIDTH)/12-1)

#define isdate(y, m, d)	((m) > 0 && (m) <= 12 && (d) > 0 && (y) >= YEAR_MIN && (y) <= YEAR_MAX && (d) <= monthdays(y, m))
#define mkdate(y, m, d)	((date) (((uint32_t) (((y) + YEAR_OFFSET) * 12 + (m) - 1) << DTMONTH_SHIFT) \
				 | ((uint32_t) (d) << DTDAY_SHIFT)))
#define date_extract_day(dt)	((int) (((uint32_t) (dt) >> DTDAY_SHIFT) & ((1 << DTDAY_WIDTH) - 1)))
#define date_extract_month(dt)	((int) ((((uint32_t) (dt) >> DTMONTH_SHIFT) & ((1 << DTMONTH_WIDTH) - 1)) % 12 + 1))
#define date_extract_year(dt)	((int) ((((uint32_t) (dt) >> DTMONTH_SHIFT) & ((1 << DTMONTH_WIDTH) - 1)) / 12 - YEAR_OFFSET))

#define istime(h,m,s,u)	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) <= 60 && (u) >= 0 && (u) < 1000000)
#define mkdaytime(h,m,s,u)	(((((daytime) (h) * 60 + (m)) * 60) + (s)) * LL_CONSTANT(1000000) + (u))

#define daytime_extract_hour(tm)	((int) (tm / HOUR_USEC))
#define daytime_extract_minute(tm)	((int) ((tm / 60000000) % 60))
#define daytime_extract_usecond(tm)	((int) (tm % 60000000)) /* includes seconds */

#define TSTIME_WIDTH	37		/* [0..24*60*60*1000000) */
#define TSTIME_SHIFT	0
#define TSDATE_WIDTH	(DTDAY_WIDTH+DTMONTH_WIDTH)
#define TSDATE_SHIFT	(TSTIME_SHIFT+TSTIME_WIDTH)
#define ts_time(ts)		((daytime) (((uint64_t) (ts) >> TSTIME_SHIFT) & ((LL_CONSTANT(1) << TSTIME_WIDTH) - 1)))
#define ts_date(ts)		((date) (((uint64_t) (ts) >> TSDATE_SHIFT) & ((1 << TSDATE_WIDTH) - 1)))
#define mktimestamp(d, t)	((timestamp) (((uint64_t) (d) << TSDATE_SHIFT) | \
					      ((uint64_t) (t) << TSTIME_SHIFT)))

static const int leapdays[13] = { /* days per month in leap year */
	0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};
static const int cumdays[13] = { /* cumulative days in non leap year */
	0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
};
#define isleapyear(y)		((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))
#define monthdays(y, m)		(leapdays[m] - ((m) == 2 && !isleapyear(y)))

const timestamp unixepoch = mktimestamp(mkdate(1970, 1, 1), mkdaytime(0, 0, 0, 0));

date
date_create(int year, int month, int day)
{
	/* note that isdate returns false if any argument is nil */
	return isdate(year, month, day) ? mkdate(year, month, day) : date_nil;
}

int
date_century(date dt)
{
	int yr;
	if (is_date_nil(dt))
		return int_nil;
	yr = date_extract_year(dt);
	if (yr > 0)
		return (yr - 1) / 100 + 1;
	else
		return -((-yr - 1) / 100 + 1);
}

int
date_decade(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	return date_extract_year(dt) / 10;
}

int
date_year(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	return date_extract_year(dt);
}

int
date_quarter(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	return (date_extract_month(dt) - 1) / 3 + 1;
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

/* In the US they have to do it differently, of course.
 * Week 1 is the week (Sunday to Saturday) in which January 1 falls */
int
date_usweekofyear(date dt)
{
	if (is_date_nil(dt))
		return int_nil;
	int y = date_extract_year(dt);
	int m = date_extract_month(dt);
	/* day of year (date_dayofyear without nil check) */
	int doy = date_extract_day(dt) + cumdays[m-1]
		+ (m > 2 && isleapyear(y));
	int jan1 = mkdate(y, 1, 1);
	int jan1days = date_countdays(jan1);
	int jan1dow = (jan1days + DOW_OFF + 1) % 7; /* Sunday=0, Saturday=6 */
	return (doy + jan1dow - 1) / 7 + 1;
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

/* return the difference in milliseconds between the two daytimes */
lng
daytime_diff(daytime d1, daytime d2)
{
	if (is_daytime_nil(d1) || is_daytime_nil(d2))
		return lng_nil;
	return (d1 - d2) / 1000;
}

int
daytime_hour(daytime tm)
{
	if (is_daytime_nil(tm))
		return int_nil;
	return daytime_extract_hour(tm);
}

int
daytime_min(daytime tm)
{
	if (is_daytime_nil(tm))
		return int_nil;
	return daytime_extract_minute(tm);
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
	return daytime_extract_usecond(tm);
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

/* convert a value returned by the system time() function to a timestamp */
timestamp
timestamp_fromtime(time_t timeval)
{
	struct tm tm = (struct tm) {0};
	date d;
	daytime t;

	if (timeval == (time_t) -1 || gmtime_r(&timeval, &tm) == NULL)
		return timestamp_nil;
	if (tm.tm_sec >= 60)
		tm.tm_sec = 59;			/* ignore leap seconds */
	d = date_create(tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
	t = daytime_create(tm.tm_hour, tm.tm_min, tm.tm_sec, 0);
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
	return timestamp_add_usec(mktimestamp(mkdate(1970, 1, 1),
					      mkdaytime(0, 0, 0, 0)),
				  usec);
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

/* return the current time in UTC */
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
	return timestamp_add_usec(timestamp_fromtime(ts.tv_sec),
				  (lng) (ts.tv_nsec / 1000));
#elif defined(HAVE_GETTIMEOFDAY)
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return timestamp_add_usec(timestamp_fromtime(tv.tv_sec), (lng) tv.tv_usec);
#else
	return timestamp_fromtime(time(NULL));
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
	if (strNil(buf))
		return 1;
	if (external && strncmp(buf, "nil", 3) == 0)
		return 3;
	if ((yearneg = (buf[0] == '-')))
		buf++;
	if (!yearneg && !GDKisdigit(buf[0])) {
		yearlast = true;
		sep = ' ';
	} else {
		for (pos = 0; GDKisdigit(buf[pos]); pos++) {
			year = (buf[pos] - '0') + year * 10;
			if (year > YEAR_MAX)
				break;
		}
		sep = (unsigned char) buf[pos++];
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
	/* handle semantic error here */
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

static ssize_t
do_date_tostr(char *buf, size_t len, const date *val, bool external)
{
	assert(len >= 15);
	if (is_date_nil(*val)) {
		if (external) {
			strcpy(buf, "nil");
			return 3;
		}
		strcpy(buf, str_nil);
		return 1;
	}
	return (ssize_t) snprintf(buf, len, "%d-%02d-%02d",
				  date_extract_year(*val),
				  date_extract_month(*val),
				  date_extract_day(*val));
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
	return do_date_tostr(*buf, *len, val, external);
}

static ssize_t
parse_daytime(const char *buf, daytime *dt, bool external)
{
	unsigned int hour, min, sec = 0, usec = 0;
	int n1, n2;
	ssize_t pos = 0;

	*dt = daytime_nil;
	if (strNil(buf))
		return 1;
	if (external && strncmp(buf, "nil", 3) == 0)
		return 3;
	/* accept plenty (6) of digits, but the range is still limited */
	switch (sscanf(buf, "%6u:%6u%n:%6u%n", &hour, &min, &n1, &sec, &n2)) {
	default:
		GDKerror("Syntax error in time.\n");
		return -1;
	case 2:
		/* read hour and min, but not sec */
		if (hour >= 24 || min >= 60) {
			GDKerror("Syntax error in time.\n");
			return -1;
		}
		pos += n1;
		break;
	case 3:
		/* read hour, min, and sec */
		if (hour >= 24 || min >= 60 || sec > 60) {
			GDKerror("Syntax error in time.\n");
			return -1;
		}
		pos += n2;
		if (buf[pos] == '.' && GDKisdigit(buf[pos+1])) {
			if (sscanf(buf + pos + 1, "%7u%n", &usec, &n1) < 1) {
				/* cannot happen: buf[pos+1] is a digit */
				GDKerror("Syntax error in time.\n");
				return -1;
			}
			pos += n1 + 1;
			while (n1 < 6) {
				usec *= 10;
				n1++;
			}
			if (n1 == 7) {
#ifdef TRUNCATE_NUMBERS
				usec /= 10;
#else
				usec = (usec + 5) / 10;
				if (usec == 1000000) {
					usec = 0;
					if (++sec == 60) {
						sec = 0;
						if (++min == 60) {
							min = 0;
							if (++hour == 24) {
								hour = 23;
								min = 59;
								sec = 59;
								usec = 999999;
							}
						}
					}
				}
#endif
			}
			/* ignore excess digits */
			while (GDKisdigit(buf[pos]))
				pos++;
		}
		break;
	}
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
	ssize_t pos;
	daytime val;
	int offset = 0;

	pos = daytime_fromstr(s, len, ret, external);
	if (pos < 0 || is_daytime_nil(**ret))
		return pos;

	s = buf + pos;
	pos = 0;
	while (GDKisspace(*s))
		s++;
	/* for GMT we need to add the time zone */
	if (fleximatch(s, "gmt", 0) == 3) {
		s += 3;
	}
	if ((s[0] == '-' || s[0] == '+') &&
	    GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
	    ((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
		offset = (((s[1] - '0') * 10 + (s[2] - '0')) * 60 + (s[pos] - '0') * 10 + (s[pos + 1] - '0')) * 60;
		pos += 2;
		if (s[0] == '+')
			offset = -offset;	/* East of Greenwich */
		s += pos;
	}
	/* convert to UTC */
	val = **ret + offset * LL_CONSTANT(1000000);
	if (val < 0)
		val += DAY_USEC;
	else if (val >= DAY_USEC)
		val -= DAY_USEC;
	/* and return */
	**ret = val;
	return (ssize_t) (s - buf);
}

static ssize_t
do_daytime_precision_tostr(char *buf, size_t len, const daytime dt,
			   int precision, bool external)
{
	int hour, min, sec, usec;

	if (precision < 0)
		precision = 0;
	if (len < 10 + (size_t) precision) {
		return -1;
	}
	if (is_daytime_nil(dt)) {
		if (external) {
			strcpy(buf, "nil");
			return 3;
		}
		strcpy(buf, str_nil);
		return 1;
	}
	usec = (int) (dt % 1000000);
	sec = (int) (dt / 1000000);
	hour = sec / 3600;
	min = (sec % 3600) / 60;
	sec %= 60;

	if (precision == 0)
		return snprintf(buf, len, "%02d:%02d:%02d", hour, min, sec);
	else if (precision < 6) {
		for (int i = 6; i > precision; i--)
			usec /= 10;
		return snprintf(buf, len, "%02d:%02d:%02d.%0*d", hour, min, sec, precision, usec);
	} else {
		ssize_t l = snprintf(buf, len, "%02d:%02d:%02d.%06d", hour, min, sec, usec);
		while (precision > 6) {
			precision--;
			buf[l++] = '0';
		}
		buf[l] = '\0';
		return l;
	}
}

ssize_t
daytime_precision_tostr(str *buf, size_t *len, const daytime dt,
			int precision, bool external)
{
	if (precision < 0)
		precision = 0;
	if (*len < 10 + (size_t) precision || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 10 + (size_t) precision);
		if( *buf == NULL)
			return -1;
	}
	return do_daytime_precision_tostr(*buf, *len, dt, precision, external);
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
	return (ssize_t) (s - buf);
}

ssize_t
timestamp_precision_tostr(str *buf, size_t *len, timestamp val, int precision, bool external)
{
	ssize_t len1, len2;
	char buf1[128], buf2[128];
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
	len1 = do_date_tostr(buf1, sizeof(buf1), &dt, false);
	len2 = do_daytime_precision_tostr(buf2, sizeof(buf2), tm,
					  precision, false);
	if (len1 < 0 || len2 < 0)
		return -1;

	if (*len < 2 + (size_t) len1 + (size_t) len2 || *buf == NULL) {
		GDKfree(*buf);
		*buf = GDKmalloc(*len = (size_t) len1 + (size_t) len2 + 2);
		if( *buf == NULL)
			return -1;
	}
	return (ssize_t) strconcat_len(*buf, *len, buf1, " ", buf2, NULL);
}

ssize_t
timestamp_tostr(str *buf, size_t *len, const timestamp *val, bool external)
{
	return timestamp_precision_tostr(buf, len, *val, 6, external);
}
