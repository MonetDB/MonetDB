/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

/*
 * @t New Temporal Module
 * @a Peter Boncz, Martin van Dinther
 * @v 1.0
 *
 * Temporal Module
 * The goal of this module is to provide adequate functionality for
 * storing and manipulated time-related data. The minimum requirement
 * is that data can easily be imported from all common commercial
 * RDBMS products.
 *
 * This module supersedes the 'temporal' module that has a number of
 * conceptual problems and hard-to-solve bugs that stem from these
 * problems.
 *
 * The starting point of this module are SQL 92 and the ODBC
 * time-related data types.  Also, some functionalities have been
 * imported from the time classes of the Java standard library.
 *
 * This module introduces four basic types and operations on them:
 * @table @samp
 * @item date
 * a @samp{date} in the proleptic Gregorian calendar, e.g. 1999-JAN-31
 *
 * @item daytime
 * a time of day to the detail of microseconds, e.g. 23:59:59:000000
 *
 * @item timestamp
 * a combination of date and time, indicating an exact point in
 * time (GMT). GMT is the time at the Greenwich meridian without a
 * daylight savings time (DST) regime. Absence of DST means that hours
 * are consecutive (no jumps) which makes it easy to perform time
 * difference calculations.
 *
 * Limitations
 * The valid ranges of the various data types are as follows:
 *
 * @table @samp
 * @item min and max year
 * The maximum and minimum dates and timestamps that can be stored are
 * in the years 5,867,411 and -5,867,411, respectively.
 *
 * @item valid dates
 * Fall in a valid year, and have a month and day that is valid in
 * that year. The first day in the year is January 1, the last
 * December 31. Months with 31 days are January, March, May, July,
 * August, October, and December, while April, June, September and
 * November have 30 days. February has 28 days, expect in a leap year,
 * when it has 29. A leap year is a year that is an exact multiple of
 * 4. Years that are a multiple of 100 but not of 400 are an
 * exception; they are no leap years.
 *
 * @item valid daytime
 * The smallest daytime is 00:00:00:000000 and the largest 23:59:59:999999
 * (the hours in a daytime range between [0,23], minutes and seconds
 * between [0,59] and milliseconds between [0:999]).  Daytime
 * identifies a valid time-of-day, not an amount of time (for denoting
 * amounts of time, or time differences, we use here concepts like
 * "number of days" or "number of milliseconds" denoted by some value
 * of a standard integer type).
 *
 * @item valid timestamp
 * is formed by a combination of a valid date and valid daytime.
 * @item difference in days
 * For difference calculations between dates (in numbers of days) we
 * use signed integers (the @i{int} Monet type), hence the valid range
 * for difference calculations is between -2147483648 and 2147483647
 * days (which corresponds to roughly -5,867,411 and 5,867,411 years).
 * @item difference in usecs
 * For difference between timestamps (in numbers of milliseconds) we
 * use 64-bit longs (the @i{lng} Monet type).  These are large
 * integers of maximally 19 digits, which therefore impose a limit of
 * about 106,000,000,000 years on the maximum time difference used in
 * computations.
 * @end table
 *
 * There are also conceptual limitations that are inherent to the time
 * system itself:
 * @table @samp
 * @item Gregorian calendar
 * The basics of the Gregorian calendar stem from the time of Julius
 * Caesar, when the concept of a solar year as consisting of 365.25
 * days (365 days plus once in 4 years one extra day) was
 * introduced. However, this Julian Calendar, made a year 11 minutes
 * long, which subsequently accumulated over the ages, causing a shift
 * in seasons. In medieval times this was noticed, and in 1582 Pope
 * Gregory XIII issued a decree, skipped 11 days. This measure was not
 * adopted in the whole of Europe immediately, however.  For this
 * reason, there were many regions in Europe that upheld different
 * dates.
 *
 * It was only on @b{September 14, 1752} that some consensus was
 * reached and more countries joined the Gregorian Calendar, which
 * also was last modified at that time. The modifications were
 * twofold: first, 12 more days were skipped. Second, it was
 * determined that the year starts on January 1 (in England, for
 * instance, it had been starting on March 25).
 *
 * Other parts of the world have adopted the Gregorian Calendar even
 * later.
 *
 * This module implements the Gregorian Calendar in all its
 * regularity. This means that values before the year 1752 probably do
 * not correspond with the dates that people really used in times
 * before that (what they did use, however, was very vague anyway, as
 * explained above). In solar terms, however, this calendar is
 * reasonably accurate (see the "correction seconds" note below).
 *
 * @item correction seconds
 * Once every such hundred years, a correction second is added on new
 * year's night.  As I do not know the rule, and this rule would
 * seriously complicate this module (as then the duration of a day,
 * which is now the fixed number of 24*60*60*1000 milliseconds,
 * becomes parametrized by the date), it is not implemented. Hence
 * these seconds are lost, so time difference calculations in
 * milliseconds (rather than in days) have a small error if the time
 * difference spans many hundreds of years.
 * @end table
 *
 * Time/date comparison
 */

#include "monetdb_config.h"
#include "mtime.h"
#include "mtime_private.h"

#ifndef HAVE_STRPTIME
extern char *strptime(const char *, const char *, struct tm *);
#endif

static const char *MONTHS[13] = {
	NULL, "january", "february", "march", "april", "may", "june",
	"july", "august", "september", "october", "november", "december"
};

static const char *DAYS[8] = {
	NULL, "monday", "tuesday", "wednesday", "thursday",
	"friday", "saturday", "sunday"
};
static int LEAPDAYS[13] = {
	0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};
static int CUMDAYS[13] = {
	0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
};

#define HOUR_USEC		(60*60*LL_CONSTANT(1000000)) /* usec in an hour */
#define DAY_USEC		(24*HOUR_USEC) /* usec in a day */

date DATE_MAX, DATE_MIN;		/* often used dates; computed once */

#define monthdays(y, m)	((m) != 2 ? LEAPDAYS[m] : 28 + ISLEAPYEAR(y))

#define isdate(y, m, d)	((m) > 0 && (m) <= 12 && (d) > 0 && (y) >= YEAR_MIN && (y) <= YEAR_MAX && (d) <= monthdays(y, m))
#define istime(h,m,s,u)	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) <= 60 && (u) >= 0 && (u) < 1000000)
#define LOWER(c)		((c) >= 'A' && (c) <= 'Z' ? (c) + 'a' - 'A' : (c))

/*
 * auxiliary functions
 */

int TYPE_date;
int TYPE_daytime;
int TYPE_timestamp;

static bool synonyms = true;

#define ISLEAPYEAR(y)		((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))

const timestamp unixepoch = mktimestamp(mkdate(1970, 1, 1),
										mkdaytime(0, 0, 0, 0));

static inline date
todate(int year, int month, int day)
{
	return isdate(year, month, day) ? mkdate(year, month, day) : date_nil;
}

static inline void
fromdate(date n, int *y, int *m, int *d)
{
	if (is_date_nil(n)) {
		if (d)
			*d = int_nil;
		if (m)
			*m = int_nil;
		if (y)
			*y = int_nil;
	} else {
		if (d)
			*d = date_day(n);
		if (m)
			*m = date_month(n);
		if (y)
			*y = date_year(n);
	}
}

static daytime
totime(int hour, int min, int sec, int usec)
{
	return istime(hour, min, sec, usec) ? mkdaytime(hour, min, sec, usec) : daytime_nil;
}

static void
fromtime(daytime n, int *hour, int *min, int *sec, int *usec)
{
	int h, m, s, u;

	if (!is_daytime_nil(n)) {
		u = (int) (n % 1000000);
		n /= 1000000;
		s = (int) (n % 60);
		n /= 60;
		m = (int) (n % 60);
		n /= 60;
		h = (int) n;			/* < 24 */
	} else {
		h = m = s = u = int_nil;
	}
	if (hour)
		*hour = h;
	if (min)
		*min = m;
	if (sec)
		*sec = s;
	if (usec)
		*usec = u;
}

/* matches regardless of case and extra spaces */
static int
fleximatch(const char *s, const char *pat, int min)
{
	int hit, spacy = 0;

	if (min == 0) {
		min = (int) strlen(pat);	/* default mininum required hits */
	}
	for (hit = 0; *pat; s++, hit++) {
		if (LOWER(*s) != *pat) {
			if (GDKisspace(*s) && spacy) {
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

static int
parse_substr(int *ret, const char *s, int min, const char *list[], int size)
{
	int j = 0, i = 0;

	*ret = int_nil;
	while (++i <= size) {
		if ((j = fleximatch(s, list[i], min)) > 0) {
			*ret = i;
			break;
		}
	}
	return j;
}

/* Monday = 1, Sunday = 7 */

/* 21 April 2019 is a Sunday, we can calculate the offset for the
 * day-of-week calculation below from this fact */
#define CNT_OFF		(((YEAR_OFFSET+399)/400)*400)
static inline int
date_countdays(date v)
{
	int y = date_year(v);
	int m = date_month(v);
	int y1 = y + CNT_OFF - 1;
	return date_day(v) + (y+CNT_OFF)*365 + y1/4 - y1/100 + y1/400 + CUMDAYS[m-1] + (m > 2 && ISLEAPYEAR(y));
}
	
#define DOW_OFF (7 - (((21 + (2019+CNT_OFF)*365 + (2019+CNT_OFF-1)/4 - (2019+CNT_OFF-1)/100 + (2019+CNT_OFF-1)/400 + 90) % 7) + 1))
static inline int
date_dayofweek(date v)
{
	/* calculate number of days since the start of the year -CNT_OFF */
	static_assert(CNT_OFF % 400 == 0, /* for leapyear function to work */
				  "CNT_OFF must be multiple of 400");
	int d = date_countdays(v);
	/* then simply take the remainder from 7 and convert to correct
	 * weekday */
	return (d + DOW_OFF) % 7 + 1;
}

static inline int
date_diff(date v1, date v2)
{
	return date_countdays(v1) - date_countdays(v2);
}

date
date_add(date dt, int days)
{
	int y = date_year(dt);
	int m = date_month(dt);
	int d = date_day(dt);
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
date_addmonth(date dt, int months)
{
	int y = date_year(dt);
	int m = date_month(dt);
	int d = date_day(dt);
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
	return mkdate(y, m, d);
}

static inline lng
timestamp_diff(timestamp v1, timestamp v2)
{
	return ts_time(v1) - ts_time(v2) + DAY_USEC * date_diff(ts_date(v1), ts_date(v2));
}

timestamp
timestamp_add(timestamp t, lng us)
{
	daytime tm = ts_time(t);
	date dt = ts_date(t);
	tm += us;
	if (tm < 0) {
		dt = date_add(dt, -(int) ((DAY_USEC - tm) / DAY_USEC));
		tm = -tm % DAY_USEC;
	} else if (tm >= DAY_USEC) {
		dt = date_add(dt, (int) (tm / DAY_USEC));
		tm %= DAY_USEC;
	}
	if (is_date_nil(dt))
		return timestamp_nil;
	return mktimestamp(dt, tm);
}

/*
 * ADT implementations
 */
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
		if (!synonyms) {
			GDKerror("Syntax error in date.\n");
			return -1;
		}
		yearlast = true;
		sep = ' ';
	} else {
		for (pos = 0; GDKisdigit(buf[pos]); pos++) {
			year = (buf[pos] - '0') + year * 10;
			if (year > YEAR_MAX)
				break;
		}
		sep = buf[pos++];
		if (!synonyms && sep != '-') {
			GDKerror("Syntax error in date.\n");
			return -1;
		}
		sep = LOWER(sep);
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
	} else if (!synonyms) {
		GDKerror("Syntax error in date.\n");
		return -1;
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
	if (yearlast && buf[pos] == ',') {
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
	*d = todate(yearneg ? -year : year, month, day);
	if (is_date_nil(*d)) {
		GDKerror("Semantic error in date.\n");
		return -1;
	}
	return pos;
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
	/* longest possible string: "-5867411-01-01" i.e. 14 chars
	   without NUL (see definition of YEAR_MIN/YEAR_MAX above) */
	if (*len < 15 || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 15);
		if( *buf == NULL)
			return -1;
	}
	if (is_date_nil(*val)) {
		if (external) {
			strcpy(*buf, "nil");
			return 3;
		}
		strcpy(*buf, str_nil);
		return 1;
	}
	snprintf(*buf, 15, "%d-%02d-%02d",
			 date_year(*val), date_month(*val), date_day(*val));
	return (ssize_t) strlen(*buf);
}

/*
 * @- daytime
 */
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
		if ((buf[pos] == '.' || (synonyms && buf[pos] == ':')) &&
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
	*dt = totime(hour, min, sec, usec);
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
	daytime mtime = mkdaytime(24, 0, 0, 0);

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
		**ret = mtime + val;
	else if (val >= mtime)
		**ret = val - mtime;
	else
		**ret = val;
	return (ssize_t) (s - buf);
}

ssize_t
daytime_tostr(str *buf, size_t *len, const daytime *val, bool external)
{
	int hour, min, sec, usec;

	fromtime(*val, &hour, &min, &sec, &usec);
	if (*len < 16 || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 16);
		if( *buf == NULL)
			return -1;
	}
	if (is_daytime_nil(*val) || !istime(hour, min, sec, usec)) {
		if (external) {
			strcpy(*buf, "nil");
			return 3;
		}
		strcpy(*buf, str_nil);
		return 1;
	}
	return snprintf(*buf, 16, "%02d:%02d:%02d.%06d", hour, min, sec, usec);
}

/*
 * @- timestamp
 */
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
		**ret = timestamp_add(**ret, offset);
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
	**ret = timestamp_add(**ret, offset);
	return (ssize_t) (s - buf);
}


ssize_t
timestamp_tostr(str *buf, size_t *len, const timestamp *val, bool external)
{
	ssize_t len1, len2;
	size_t big = 128;
	char buf1[128], buf2[128], *s = *buf, *s1 = buf1, *s2 = buf2;
	date dt;
	daytime tm;

	if (is_timestamp_nil(*val)) {
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

	dt = ts_date(*val);
	tm = ts_time(*val);
	len1 = date_tostr(&s1, &big, &dt, false);
	len2 = daytime_tostr(&s2, &big, &tm, false);
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

/*
 * operator implementations
 */
static daytime
daytime_add(daytime v, lng usec)
{
	v += usec;
	if (v < 0 || v >= DAY_USEC)
		return daytime_nil;
	return v;
}

/* returns the timestamp that comes 'milliseconds' after 'value'. */
str
MTIMEtimestamp_add(timestamp *ret, const timestamp *v, const lng *msec)
{
	if (!is_timestamp_nil(*v) && !is_lng_nil(*msec)) {
		timestamp t = timestamp_add(*v, *msec * 1000);
		if (is_timestamp_nil(t))
			throw(MAL, "time.add", SQLSTATE(22003) "overflow in calculation");
		*ret = t;
	} else {
		*ret = timestamp_nil;
	}
	return MAL_SUCCEED;
}

/*
 * Include BAT macros
 */
#include "mal.h"
#include "mal_exception.h"

str
MTIMEnil2date(date *ret, const void *src)
{
	(void) src;
	*ret = date_nil;
	return MAL_SUCCEED;
}

str
MTIMEdate2date(date *ret, const date *src)
{
	*ret = *src;
	return MAL_SUCCEED;
}

str
MTIMEdaytime2daytime(daytime *ret, const daytime *src)
{
	*ret = *src;
	return MAL_SUCCEED;
}

str
MTIMEtimestamp2timestamp(timestamp *ret, const timestamp *src)
{
	*ret = *src;
	return MAL_SUCCEED;
}

void MTIMEreset(void) {
}

str
MTIMEprelude(void *ret)
{
	(void) ret;

	TYPE_date = ATOMindex("date");
	TYPE_daytime = ATOMindex("daytime");
	TYPE_timestamp = ATOMindex("timestamp");

	MONTHS[0] = (str) str_nil;
	DAYS[0] = (str) str_nil;
	LEAPDAYS[0] = int_nil;
	DATE_MAX = todate(YEAR_MAX, 12, 31);
	DATE_MIN = todate(YEAR_MIN, 1, 1);

	return MAL_SUCCEED;
}

str
MTIMEepilogue(void *ret)
{
	(void) ret;
	MTIMEreset();
	return MAL_SUCCEED;
}

str
MTIMEsynonyms(void *ret, const bit *allow)
{
	(void) ret;
	if (!is_bit_nil(*allow))
		synonyms = *allow;
	return MAL_SUCCEED;
}

/* Returns month number [1-12] from a string (or nil if does not match any). */
str
MTIMEmonth_from_str(int *ret, const char * const *month)
{
	parse_substr(ret, *month, 3, MONTHS, 12);
	return MAL_SUCCEED;
}

/* Returns month name from a number between [1-7], str(nil) otherwise. */
str
MTIMEmonth_to_str(str *ret, const int *month)
{
	*ret = GDKstrdup(MONTHS[(*month < 1 || *month > 12) ? 0 : *month]);
	if (*ret == NULL)
		throw(MAL, "mtime.month_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

/* Returns number of day [1-7] from a string (or nil if does not match any). */
str
MTIMEday_from_str(int *ret, const char * const *day)
{
	if (strcmp(*day, str_nil) == 0)
		*ret = int_nil;
	else
		parse_substr(ret, *day, 3, DAYS, 7);
	return MAL_SUCCEED;
}

/* Returns day name from a number between [1-7], str(nil) otherwise. */
str
MTIMEday_to_str(str *ret, const int *day)
{
	*ret = GDKstrdup(DAYS[(*day < 1 || *day > 7) ? 0 : *day]);
	if (*ret == NULL)
		throw(MAL, "mtime.day_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	return MAL_SUCCEED;
}

str
MTIMEdate_date(date *d, const date *s)
{
	*d = *s;
	return MAL_SUCCEED;
}

str
MTIMEdate_fromstr(date *ret, const char * const *s)
{
	size_t len = sizeof(date);

	if (strcmp(*s, "nil") == 0) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	if (date_fromstr(*s, &len, &ret, false) < 0)
		throw(MAL, "mtime.date", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/* creates a date from (day,month,year) parameters */
str
MTIMEdate_create(date *ret, const int *year, const int *month, const int *day)
{
	*ret = todate(*year, *month, *day);
	return MAL_SUCCEED;
}

/* creates a daytime from (hours,minutes,seconds,milliseconds) parameters */
str
MTIMEdaytime_create(daytime *ret, const int *hour, const int *min, const int *sec, const int *msec)
{
	*ret = totime(*hour, *min, *sec, *msec * 1000);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_fromstr(timestamp *ret, const char * const *d)
{
	size_t len = sizeof(timestamp);

	if (strcmp(*d, "nil") == 0) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	if (timestamp_fromstr(*d, &len, &ret, false) < 0)
		throw(MAL, "mtime.timestamp", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/* creates a timestamp from (date,daytime) parameters */
str
MTIMEtimestamp_create(timestamp *ret, const date *d, const daytime *t)
{
	if (is_date_nil(*d) || is_daytime_nil(*t)) {
		*ret = timestamp_nil;
	} else {
		*ret = mktimestamp(*d, *t);
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_create_from_date(timestamp *ret, const date *d)
{
	daytime t = totime(0, 0, 0, 0);
	return MTIMEtimestamp_create(ret, d, &t);
}

str
MTIMEtimestamp_create_from_date_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	timestamp *t;
	const date *d;
	const daytime dt = totime(0, 0, 0, 0);
	BUN n;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = (const date *) Tloc(b, 0);
	t = (timestamp *) Tloc(bn, 0);
	bn->tnil = false;
	for (n = BATcount(b); n > 0; n--, t++, d++) {
		if (is_date_nil(*d)) {
			*t = timestamp_nil;
			bn->tnil = true;
		} else {
			*t = mktimestamp(*d, dt);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->tsorted = b->tsorted || BATcount(bn) <= 1;
	bn->trevsorted = b->trevsorted || BATcount(bn) <= 1;
	bn->tnonil = !bn->tnil;
	BBPunfix(b->batCacheid);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

/* extracts year from date (value between -5867411 and +5867411). */
str
MTIMEdate_extract_year(int *ret, const date *v)
{
	if (is_date_nil(*v)) {
		*ret = int_nil;
	} else {
		*ret = date_year(*v);
	}
	return MAL_SUCCEED;
}

/* extracts quarter from date (value between 1 and 4) */
str
MTIMEdate_extract_quarter(int *ret, const date *v)
{
	if (is_date_nil(*v)) {
		*ret = int_nil;
	} else {
		*ret = (date_month(*v) - 1) / 4 + 1;
	}
	return MAL_SUCCEED;
}

/* extracts month from date (value between 1 and 12) */
str
MTIMEdate_extract_month(int *ret, const date *v)
{
	if (is_date_nil(*v)) {
		*ret = int_nil;
	} else {
		*ret = date_month(*v);
	}
	return MAL_SUCCEED;
}

/* extracts day from date (value between 1 and 31)*/
str
MTIMEdate_extract_day(int *ret, const date *v)
{
	if (is_date_nil(*v)) {
		*ret = int_nil;
	} else {
		*ret = date_day(*v);
	}
	return MAL_SUCCEED;
}

/* Returns N where d is the Nth day of the year (january 1 returns 1). */
str
MTIMEdate_extract_dayofyear(int *ret, const date *v)
{
	if (is_date_nil(*v)) {
		*ret = int_nil;
	} else {
		*ret = date_countdays(*v) - date_countdays(mkdate(date_year(*v), 1, 1)) + 1;
	}
	return MAL_SUCCEED;
}

/* Returns the week number */
str
MTIMEdate_extract_weekofyear(int *ret, const date *v)
{
	if (is_date_nil(*v)) {
		*ret = int_nil;
	} else {
		int y = date_year(*v);
		int m = date_month(*v);
		int d = date_day(*v);
		int wd1 = date_dayofweek(mkdate(y, 1, 4));
		int wd2 = date_dayofweek(*v);
		int cnt1, cnt2;
		if (m == 1 && d < 4 && wd2 > wd1) {
			/* last week of previous y */
			cnt1 = date_countdays(mkdate(y - 1, 1, 4));
			wd1 = date_dayofweek(mkdate(y - 1, 1, 4));
		} else {
			cnt1 = date_countdays(mkdate(y, 1, 4));
		}
		cnt2 = date_countdays(*v);
		if (wd2 < wd1)
			cnt2 += 6;
		*ret = (cnt2 - cnt1) / 7 + 1;
	}
	return MAL_SUCCEED;
}

/* Returns the current day  of the week where 1=monday, .., 7=sunday */
str
MTIMEdate_extract_dayofweek(int *ret, const date *v)
{
	if (is_date_nil(*v)) {
		*ret = int_nil;
	} else {
		*ret = date_dayofweek(*v);
	}
	return MAL_SUCCEED;
}

/* extracts hour from daytime (value between 0 and 23) */
str
MTIMEdaytime_extract_hours(int *ret, const daytime *v)
{
	if (is_daytime_nil(*v)) {
		*ret = int_nil;
	} else {
		fromtime(*v, ret, NULL, NULL, NULL);
	}
	return MAL_SUCCEED;
}

/* extracts minutes from daytime (value between 0 and 59) */
str
MTIMEdaytime_extract_minutes(int *ret, const daytime *v)
{
	if (is_daytime_nil(*v)) {
		*ret = int_nil;
	} else {
		fromtime(*v, NULL, ret, NULL, NULL);
	}
	return MAL_SUCCEED;
}

/* extracts seconds from daytime (value between 0 and 59) */
str
MTIMEdaytime_extract_seconds(int *ret, const daytime *v)
{
	if (is_daytime_nil(*v)) {
		*ret = int_nil;
	} else {
		fromtime(*v, NULL, NULL, ret, NULL);
	}
	return MAL_SUCCEED;
}

/* extracts (milli) seconds from daytime (value between 0 and 59999) */
str
MTIMEdaytime_extract_sql_seconds(int *ret, const daytime *v)
{
	if (is_daytime_nil(*v)) {
		*ret = int_nil;
	} else {
		int sec, usec;
		fromtime(*v, NULL, NULL, &sec, &usec);
		*ret = sec * 1000 + usec / 1000;
	}
	return MAL_SUCCEED;
}

/* extracts milliseconds from daytime (value between 0 and 999) */
str
MTIMEdaytime_extract_milliseconds(int *ret, const daytime *v)
{
	if (is_daytime_nil(*v)) {
		*ret = int_nil;
	} else {
		int usec;
		fromtime(*v, NULL, NULL, NULL, &usec);
		*ret = usec / 1000;
	}
	return MAL_SUCCEED;
}

/* extracts daytime from timestamp */
str
MTIMEtimestamp_extract_daytime(daytime *ret, const timestamp *t)
{
	if (is_timestamp_nil(*t)) {
		*ret = daytime_nil;
	} else {
		*ret = ts_time(*t);
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_extract_daytime_bulk(bat *ret, bat *bid)
{
	BAT *b = BATdescriptor(*bid);
	BAT *bn;
	const timestamp *t;
	daytime *dt;
	BUN n;

	if (b == NULL)
		throw(MAL, "batcalc.daytime", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bn = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = (const timestamp *) Tloc(b, 0);
	dt = (daytime *) Tloc(bn, 0);
	bn->tnil = false;
	n = BATcount(b);
	for (BUN i = 0; i < n; i++) {
		if (is_timestamp_nil(t[i])) {
			dt[i] = daytime_nil;
			bn->tnil = true;
		} else {
			dt[i] = ts_time(t[i]);
		}
	}
	BATsetcount(bn, n);
	bn->tsorted = b->tsorted || n <= 1;
	bn->trevsorted = b->trevsorted || n <= 1;
	bn->tnonil = !bn->tnil;
	BBPunfix(b->batCacheid);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

/* extracts date from timestamp */
str
MTIMEtimestamp_extract_date(date *ret, const timestamp *t)
{
	if (is_timestamp_nil(*t)) {
		*ret = date_nil;
	} else {
		*ret = ts_date(*t);
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_extract_date_bulk(bat *ret, bat *bid)
{
	BAT *b = BATdescriptor(*bid);
	BAT *bn;
	const timestamp *t;
	date *d;
	BUN n;

	if (b == NULL)
		throw(MAL, "batcalc.date", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bn = COLnew(b->hseqbase, TYPE_date, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.date", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = (const timestamp *) Tloc(b, 0);
	d = (date *) Tloc(bn, 0);
	bn->tnil = false;
	n = BATcount(b);
	for (BUN i = 0; i < n; i++) {
		if (is_timestamp_nil(t[i])) {
			d[i] = date_nil;
			bn->tnil = true;
		} else {
			d[i] = ts_date(t[i]);
		}
	}
	BATsetcount(bn, n);
	bn->tsorted = b->tsorted || n <= 1;
	bn->trevsorted = b->trevsorted || n <= 1;
	bn->tnonil = !bn->tnil;
	BBPunfix(b->batCacheid);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

/* returns the date that comes a number of years after 'v' (or before
 * iff *delta < 0). */
str
MTIMEdate_addyears(date *ret, const date *v, const int *delta)
{
	if (is_date_nil(*v) || is_int_nil(*delta)) {
		*ret = date_nil;
	} else {
		int y = date_year(*v);
		int m = date_month(*v);
		int d = date_day(*v);
		y += *delta;
		if (y < YEAR_MIN || y > YEAR_MAX)
			throw(MAL, "mtime.addyears",
				  SQLSTATE(22003) "overflow in calculation");
		*ret = mkdate(y, m, d);
	}
	return MAL_SUCCEED;
}

/* returns the date that comes a number of day after 'v' (or before
 * iff *delta < 0). */
str
MTIMEdate_adddays(date *ret, const date *v, const int *delta)
{
	if (is_date_nil(*v) || is_int_nil(*delta)) {
		*ret = date_nil;
	} else {
		*ret = date_add(*v, *delta);
		if (is_date_nil(*ret))
			throw(MAL, "mtime.adddays",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

/* returns the date that comes a number of months after 'v' (or before
 * if *delta < 0). */
str
MTIMEdate_addmonths(date *ret, const date *v, const int *delta)
{
	if (is_date_nil(*v) || is_int_nil(*delta)) {
		*ret = date_nil;
	} else {
		*ret = date_addmonth(*v, *delta);
		if (is_date_nil(*ret))
			throw(MAL, "mtime.adddays",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEdate_submonths(date *ret, const date *v, const int *delta)
{
	if (is_int_nil(*delta)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	int mindelta = -(*delta);
	return MTIMEdate_addmonths(ret, v, &mindelta);
}

/* returns the number of days between 'val1' and 'val2'. */
str
MTIMEdate_diff(int *ret, const date *v1, const date *v2)
{
	if (is_date_nil(*v1) || is_date_nil(*v2))
		*ret = int_nil;
	else
		*ret = date_diff(*v1, *v2);
	return MAL_SUCCEED;
}

str
MTIMEdate_diff_bulk(bat *ret, const bat *bid1, const bat *bid2)
{
	BAT *b1, *b2, *bn;
	const date *t1, *t2;
	int *tn;
	BUN i, n;

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
	bn = COLnew(b1->hseqbase, TYPE_int, BATcount(b1), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t1 = (const date *) Tloc(b1, 0);
	t2 = (const date *) Tloc(b2, 0);
	tn = (int *) Tloc(bn, 0);
	bn->tnonil = true;
	bn->tnil = false;
	for (i = 0; i < n; i++) {
		if (is_date_nil(t1[i]) || is_date_nil(t2[i])) {
			tn[i] = int_nil;
			bn->tnonil = false;
			bn->tnil = true;
		} else {
			tn[i] = date_diff(t1[i], t2[i]);
		}
	}
	BBPunfix(b2->batCacheid);
	BATsetcount(bn, (BUN) (tn - (int *) Tloc(bn, 0)));
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	BBPunfix(b1->batCacheid);
	BBPkeepref(bn->batCacheid);
	*ret = bn->batCacheid;
	return MAL_SUCCEED;
}

str
MTIMEdaytime_diff(lng *ret, const daytime *v1, const daytime *v2)
{
	if (is_daytime_nil(*v1) || is_daytime_nil(*v2)) {
		*ret = lng_nil;
	} else {
		*ret = (lng) (*v1 - *v2);
	}
	return MAL_SUCCEED;
}

/* returns the number of milliseconds between 'val1' and 'val2'. */
str
MTIMEtimestamp_diff(lng *ret, const timestamp *v1, const timestamp *v2)
{
	if (is_timestamp_nil(*v1) || is_timestamp_nil(*v2)) {
		*ret = lng_nil;
	} else {
		*ret = timestamp_diff(*v1, *v2) / 1000;
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_diff_bulk(bat *ret, const bat *bid1, const bat *bid2)
{
	BAT *b1, *b2, *bn;
	const timestamp *t1, *t2;
	lng *tn;
	BUN i, n;

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
	bn = COLnew(b1->hseqbase, TYPE_lng, BATcount(b1), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b1->batCacheid);
		BBPunfix(b2->batCacheid);
		throw(MAL, "batmtime.diff", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t1 = (const timestamp *) Tloc(b1, 0);
	t2 = (const timestamp *) Tloc(b2, 0);
	tn = (lng *) Tloc(bn, 0);
	bn->tnonil = true;
	bn->tnil = false;
	for (i = 0; i < n; i++) {
		if (is_timestamp_nil(t1[i]) || is_timestamp_nil(t2[i])) {
			tn[i] = lng_nil;
			bn->tnonil = false;
			bn->tnil = true;
		} else {
			tn[i] = timestamp_diff(t1[i], t2[i]) / 1000;
		}
	}
	BBPunfix(b2->batCacheid);
	BATsetcount(bn, (BUN) (tn - (lng *) Tloc(bn, 0)));
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	BBPunfix(b1->batCacheid);
	BBPkeepref(bn->batCacheid);
	*ret = bn->batCacheid;
	return MAL_SUCCEED;
}

str
MTIMEdate_sub_sec_interval_wrap(date *ret, const date *t, const int *sec)
{
	int delta;

	if (is_int_nil(*sec) || is_date_nil(*t)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	delta = -(int) (*sec / 86400);		/* / truncates toward zero */
	return MTIMEdate_adddays(ret, t, &delta);
}

str
MTIMEdate_sub_msec_interval_lng_wrap(date *ret, const date *t, const lng *msec)
{
	int delta;

	if (is_lng_nil(*msec) || is_date_nil(*t)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	delta = -(int) (*msec / 86400000);	/* / truncates toward zero */
	return MTIMEdate_adddays(ret, t, &delta);
}

str
MTIMEdate_add_sec_interval_wrap(date *ret, const date *t, const int *sec)
{
	int delta;

	if (is_int_nil(*sec) || is_date_nil(*t)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	delta = (int) (*sec / 86400);		/* / truncates toward zero */
	return MTIMEdate_adddays(ret, t, &delta);
}

str
MTIMEdate_add_msec_interval_lng_wrap(date *ret, const date *t, const lng *msec)
{
	int delta;

	if (is_lng_nil(*msec) || is_date_nil(*t)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	delta = (int) (*msec / 86400000);	/* / truncates toward zero */
	return MTIMEdate_adddays(ret, t, &delta);
}

str
MTIMEtimestamp_sub_msec_interval_lng_wrap(timestamp *ret, const timestamp *t, const lng *msec)
{
	if (is_lng_nil(*msec) || is_date_nil(*t)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	*ret = timestamp_add(*t, -*msec * 1000);
	if (is_timestamp_nil(*ret))
		throw(MAL, "mtime.date_sub_msec_interval",
			  SQLSTATE(22003) "overflow in calculation");
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_add_month_interval_wrap(timestamp *ret, const timestamp *v, const int *months)
{
	if (is_timestamp_nil(*v) || is_int_nil(*months)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	date dt = ts_date(*v);
	daytime tm = ts_time(*v);
	dt = date_addmonth(dt, *months);
	if (is_date_nil(dt))
		throw(MAL, "mtime.timestamp_add_month_interval",
			  SQLSTATE(22003) "overflow in calculation");
	*ret = mktimestamp(dt, tm);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_add_month_interval_lng_wrap(timestamp *ret, const timestamp *v, const lng *months)
{
	daytime t;
	date d;
	int m;
	MTIMEtimestamp_extract_daytime(&t, v);
	MTIMEtimestamp_extract_date(&d, v);
	if (*months > (YEAR_MAX*12))
		throw(MAL, "mtime.timestamp_sub_interval", "too many months");
	m = (int)*months;
	MTIMEdate_addmonths(&d, &d, &m);
	return MTIMEtimestamp_create(ret, &d, &t);
}

str
MTIMEtimestamp_sub_month_interval_wrap(timestamp *ret, const timestamp *v, const int *months)
{
	daytime t;
	date d;
	int m = -*months;
	MTIMEtimestamp_extract_daytime(&t, v);
	MTIMEtimestamp_extract_date(&d, v);
	MTIMEdate_addmonths(&d, &d, &m);
	return MTIMEtimestamp_create(ret, &d, &t);
}

str
MTIMEtimestamp_sub_month_interval_lng_wrap(timestamp *ret, const timestamp *v, const lng *months)
{
	daytime t;
	date d;
	int m;
	MTIMEtimestamp_extract_daytime(&t, v);
	MTIMEtimestamp_extract_date(&d, v);
	if (*months > (YEAR_MAX*12))
		throw(MAL, "mtime.timestamp_sub_interval", "too many months");
	m = -(int)*months;
	MTIMEdate_addmonths(&d, &d, &m);
	return MTIMEtimestamp_create(ret, &d, &t);
}


str
MTIMEtime_add_msec_interval_wrap(daytime *ret, const daytime *t, const lng *mseconds)
{
	if (is_daytime_nil(*t) || is_lng_nil(*mseconds))
		*ret = daytime_nil;
	else {
		*ret = daytime_add(*t, *mseconds * 1000);
		if (is_daytime_nil(*ret))
			throw(MAL, "mtime.time_add_msec_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEtime_sub_msec_interval_wrap(daytime *ret, const daytime *t, const lng *mseconds)
{
	if (is_daytime_nil(*t) || is_lng_nil(*mseconds))
		*ret = daytime_nil;
	else {
		*ret = daytime_add(*t, -*mseconds * 1000);
		if (is_daytime_nil(*ret))
			throw(MAL, "mtime.time_sub_msec_interval",
				  SQLSTATE(22003) "overflow in calculation");
	}
	return MAL_SUCCEED;
}

str
MTIMEdaytime_fromstr(daytime *ret, const char * const *s)
{
	size_t len = sizeof(daytime);

	if (strcmp(*s, "nil") == 0) {
		*ret = daytime_nil;
		return MAL_SUCCEED;
	}
	if (daytime_fromstr(*s, &len, &ret, false) < 0)
		throw(MAL, "mtime.daytime", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/*
 * The utilities from Monet V4
 */
str
MTIMEmsecs(lng *ret, const int *d, const int *h, const int *m, const int *s, const int *ms)
{
	if (is_int_nil(*d) || is_int_nil(*h) || is_int_nil(*m) ||
		is_int_nil(*s) || is_int_nil(*ms))
		*ret = lng_nil;
	else
		*ret = ((lng) *ms) + 1000 * (*s + 60 * (*m + 60 * (*h + 24 * *d)));
	return MAL_SUCCEED;
}

str
MTIMEdaytime1(daytime *ret, const int *h)
{
	int m = 0, s = 0, ms = 0;

	return MTIMEdaytime_create(ret, h, &m, &s, &ms);
}

str
MTIMEsecs2daytime(daytime *ret, const lng *s)
{
	*ret = is_lng_nil(*s) ||
			*s > GDK_int_max / 1000 ||
			*s < GDK_int_min / 1000 ?
		daytime_nil : (daytime) (*s * 1000);
	return MAL_SUCCEED;
}

str
MTIMEsecs2daytime_bulk(bat *ret, bat *bid)
{
	BAT *b = BATdescriptor(*bid);
	BAT *bn;
	const lng *s;
	daytime *dt;
	BUN n;

	if (b == NULL)
		throw(MAL, "batcalc.daytime", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bn = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	s = (const lng *) Tloc(b, 0);
	dt = (daytime *) Tloc(bn, 0);
	bn->tnil = false;
	for (n = BATcount(b); n > 0; n--, s++, dt++) {
		if (is_lng_nil(*s) ||
			*s > GDK_int_max / 1000 ||
			*s < GDK_int_min / 1000) {
			*dt = daytime_nil;
			bn->tnil = true;
		} else {
			*dt = (daytime) (*s * 1000);
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->tsorted = b->tsorted || BATcount(bn) <= 1;
	bn->trevsorted = b->trevsorted || BATcount(bn) <= 1;
	bn->tnonil = !bn->tnil;
	BBPunfix(b->batCacheid);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
MTIMEdaytime2(daytime *ret, const int *h, const int *m)
{
	int s = 0, ms = 0;

	return MTIMEdaytime_create(ret, h, m, &s, &ms);
}

str
MTIMEdaytime3(daytime *ret, const int *h, const int *m, const int *s)
{
	int ms = 0;

	return MTIMEdaytime_create(ret, h, m, s, &ms);
}

str
MTIMEunix_epoch(timestamp *ret)
{
	*ret = unixepoch;
	return MAL_SUCCEED;
}

str
MTIMEepoch2int(int *ret, const timestamp *t)
{
	lng v;

	if (is_timestamp_nil(*t)) {
		*ret = int_nil;
		return MAL_SUCCEED;
	}
	v = timestamp_diff(*t, unixepoch) / 1000000;
	if (v > (lng) GDK_int_max || v < (lng) GDK_int_min)
		throw(MAL, "mtime.epoch", "22003!epoch value too large");
	else
		*ret = (int) v;
	return MAL_SUCCEED;
}

str
MTIMEepoch2lng(lng *ret, const timestamp *t)
{
	*ret = timestamp_diff(*t, unixepoch) / 1000;
	return MAL_SUCCEED;
}

str
MTIMEepoch_bulk(bat *ret, bat *bid)
{
	const timestamp *t;
	lng *tn;
	str msg = MAL_SUCCEED;
	BAT *b, *bn;
	BUN i, n;

	if ((b = BATdescriptor(*bid)) == NULL) {
		throw(MAL, "batcalc.epoch", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	}
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_lng, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.epoch", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = (const timestamp *) Tloc(b, 0);
	tn = (lng *) Tloc(bn, 0);
	bn->tnonil = true;
	b->tnil = false;
	for (i = 0; i < n; i++) {
		if (is_timestamp_nil(t[i])) {
			tn[i] = lng_nil;
			bn->tnonil = false;
			bn->tnil = true;
		} else {
			tn[i] = timestamp_diff(t[i], unixepoch) / 1000;
		}
	}
	BBPunfix(b->batCacheid);
	BATsetcount(bn, n);
	bn->tsorted = n <= 1;
	bn->trevsorted = n <= 1;
	BBPkeepref(bn->batCacheid);
	*ret = bn->batCacheid;
	return msg;
}

str
MTIMEtimestamp(timestamp *ret, const int *sec)
{
	if (is_int_nil(*sec)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	*ret = timestamp_add(unixepoch, *sec * LL_CONSTANT(1000000));
	if (is_timestamp_nil(*ret))
		throw(MAL, "mtime.timestamp", "22003!value too large");
	return MAL_SUCCEED;
}

str
MTIMEtimestamplng(timestamp *ret, const lng *sec)
{
	if (is_lng_nil(*sec)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	*ret = timestamp_add(unixepoch, *sec * LL_CONSTANT(1000000));
	if (is_timestamp_nil(*ret))
		throw(MAL, "mtime.epoch", "22003!value too large");
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	timestamp *t;
	const int *s;
	BUN n;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	s = (const int *) Tloc(b, 0);
	t = (timestamp *) Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_int_nil(s[i])) {
			t[i] = timestamp_nil;
			bn->tnil = true;
		} else {				
			t[i] = timestamp_add(unixepoch, s[i] * LL_CONSTANT(1000000));
			if (is_timestamp_nil(t[i])) {
				BBPreclaim(bn);
				throw(MAL, "batcalc.timestamp", "22003!value too large");
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->tsorted = b->tsorted || BATcount(bn) <= 1;
	bn->trevsorted = b->trevsorted || BATcount(bn) <= 1;
	bn->tnonil = !bn->tnil;
	BBPunfix(b->batCacheid);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_lng(timestamp *ret, const lng *msec)
{
	if (is_lng_nil(*msec)) {
		*ret = timestamp_nil;
		return MAL_SUCCEED;
	}
	*ret = timestamp_add(unixepoch, *msec * LL_CONSTANT(1000));
	if (is_timestamp_nil(*ret))
		throw(MAL, "mtime.timestamp", "22003!value too large");
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_lng_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	timestamp *t;
	const lng *ms;
	BUN n;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	n = BATcount(b);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, n, TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	ms = (const lng *) Tloc(b, 0);
	t = (timestamp *) Tloc(bn, 0);
	bn->tnil = false;
	for (BUN i = 0; i < n; i++) {
		if (is_lng_nil(ms[i])) {
			t[i] = timestamp_nil;
			bn->tnil = true;
		} else {				
			t[i] = timestamp_add(unixepoch, ms[i] * LL_CONSTANT(1000));
			if (is_timestamp_nil(t[i])) {
				BBPreclaim(bn);
				throw(MAL, "batcalc.timestamp", "22003!value too large");
			}
		}
	}
	BATsetcount(bn, BATcount(b));
	bn->tsorted = b->tsorted || BATcount(bn) <= 1;
	bn->trevsorted = b->trevsorted || BATcount(bn) <= 1;
	bn->tnonil = !bn->tnil;
	BBPunfix(b->batCacheid);
	*ret = bn->batCacheid;
	BBPkeepref(*ret);
	return MAL_SUCCEED;
}

str
MTIMEcurrent_timestamp(timestamp *ret)
{
#if defined(HAVE_GETSYSTEMTIMEASFILETIME)
	FILETIME ft;
	ULARGE_INTEGER l;
	GetSystemTimeAsFileTime(&ft);
	l.LowPart = ft.dwLowDateTime;
	l.HighPart = ft.dwHighDateTime;
	*ret = timestamp_add(mktimestamp(mkdate(1601, 1, 1),
									 mkdaytime(0, 0, 0, 0)),
						 (lng) (l.QuadPart / 10));
#elif defined(HAVE_CLOCK_GETTIME)
	struct timespec ts;
	clock_gettime(CLOCK_REALTIME, &ts);
	*ret = timestamp_add(unixepoch, ts.tv_sec * LL_CONSTANT(1000000) + ts.tv_nsec / 1000);
#elif defined(HAVE_GETTIMEOFDAY)
	struct timeval tv;
	gettimeofday(&tv, NULL);
	*ret = timestamp_add(unixepoch, tv.tv_sec * LL_CONSTANT(1000000) + tv.tv_usec);
#else
	*ret = timestamp_add(unixepoch, (lng) time(NULL) * LL_CONSTANT(1000000));
#endif
	if (is_timestamp_nil(*ret))
		throw(MAL, "mtime.epoch", "22003!value too large");
	return MAL_SUCCEED;
}

str
MTIMEcurrent_date(date *d)
{
	timestamp stamp;
	str e;

	if ((e = MTIMEcurrent_timestamp(&stamp)) != MAL_SUCCEED)
		return e;
	return MTIMEtimestamp_extract_date(d, &stamp);
}

str
MTIMEcurrent_time(daytime *t)
{
	timestamp stamp;
	str e;

	if ((e = MTIMEcurrent_timestamp(&stamp)) != MAL_SUCCEED)
		return e;
	return MTIMEtimestamp_extract_daytime(t, &stamp);
}

/* more SQL extraction utilities */
str
MTIMEtimestamp_year(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_year(ret, &d);
}

str
MTIMEtimestamp_quarter(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_quarter(ret, &d);
}

str
MTIMEtimestamp_month(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_month(ret, &d);
}


str
MTIMEtimestamp_day(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_day(ret, &d);
}

str
MTIMEtimestamp_hours(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_hours(ret, &d);
}

str
MTIMEtimestamp_minutes(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_minutes(ret, &d);
}

str
MTIMEtimestamp_seconds(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_seconds(ret, &d);
}

str
MTIMEtimestamp_sql_seconds(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_sql_seconds(ret, &d);
}

str
MTIMEtimestamp_milliseconds(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_milliseconds(ret, &d);
}

str
MTIMEsql_year(int *ret, const int *t)
{
	if (is_int_nil(*t))
		*ret = int_nil;
	else
		*ret = *t / 12;
	return MAL_SUCCEED;
}

str
MTIMEsql_month(int *ret, const int *t)
{
	if (is_int_nil(*t))
		*ret = int_nil;
	else
		*ret = *t % 12;
	return MAL_SUCCEED;
}

str
MTIMEsql_day(lng *ret, const lng *t)
{
	if (is_lng_nil(*t))
		*ret = lng_nil;
	else
		*ret = *t / 86400000;
	return MAL_SUCCEED;
}

str
MTIMEsql_hours(int *ret, const lng *t)
{
	if (is_lng_nil(*t))
		*ret = int_nil;
	else
		*ret = (int) ((*t % 86400000) / 3600000);
	return MAL_SUCCEED;
}

str
MTIMEsql_minutes(int *ret, const lng *t)
{
	if (is_lng_nil(*t))
		*ret = int_nil;
	else
		*ret = (int) ((*t % 3600000) / 60000);
	return MAL_SUCCEED;
}

str
MTIMEsql_seconds(int *ret, const lng *t)
{
	if (is_lng_nil(*t))
		*ret = int_nil;
	else
		*ret = (int) ((*t % 60000) / 1000);
	return MAL_SUCCEED;
}

/*
 * The BAT equivalents for these functions provide
 * speed.
 */

str
MTIMEmsec(lng *r)
{
	lng t = GDKusec();
	*r = t / 1000;
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_year_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i,n;
	int *y;
	const date *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.year", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.year", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const date *) Tloc(b, 0);
	y = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_date_nil(*t)) {
			*y = int_nil;
			bn->tnil = true;
		} else {
			MTIMEdate_extract_year(y, t);
			if (is_int_nil(*y)) {
				bn->tnil = true;
			}
		}
		y++;
		t++;
	}

	BATsetcount(bn, (BUN) (y - (int *) Tloc(bn, 0)));

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_quarter_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i,n;
	int *q;
	const date *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.quarter", SQLSTATE(HY005) "Cannot access descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.quarter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const date *) Tloc(b, 0);
	q = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_date_nil(*t)) {
			*q = int_nil;
			bn->tnil = true;
		} else {
			MTIMEdate_extract_quarter(q, t);
			if (is_int_nil(*q)) {
				bn->tnil = true;
			}
		}
		q++;
		t++;
	}
	BATsetcount(bn, (BUN) (q - (int *) Tloc(bn, 0)));

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_month_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i,n;
	int *m;
	const date *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.year", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.month", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const date *) Tloc(b, 0);
	m = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_date_nil(*t)) {
			*m = int_nil;
			bn->tnil = true;
		} else {
			MTIMEdate_extract_month(m, t);
			if (is_int_nil(*m)) {
				bn->tnil = true;
			}
		}
		m++;
		t++;
	}
	BATsetcount(bn, (BUN) (m - (int *) Tloc(bn, 0)));

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdate_extract_day_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i,n;
	int *d;
	const date *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.day", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.day", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const date *) Tloc(b, 0);
	d = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_date_nil(*t)) {
			*d = int_nil;
			bn->tnil = true;
		} else {
			MTIMEdate_extract_day(d, t);
			if (is_int_nil(*d)) {
				bn->tnil = true;
			}
		}
		d++;
		t++;
	}

	BATsetcount(bn, (BUN) (d - (int *) Tloc(bn, 0)));

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_hours_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i, n;
	int *h;
	const daytime *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.hours", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.hours", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const daytime *) Tloc(b, 0);
	h = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_daytime_nil(t[i])) {
			h[i] = int_nil;
			bn->tnil = true;
		} else {
			fromtime(t[i], &h[i], NULL, NULL, NULL);
		}
	}
	BATsetcount(bn, n);

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_minutes_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i, n;
	int *m;
	const daytime *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.minutes", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.minutes", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const daytime *) Tloc(b, 0);
	m = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_daytime_nil(t[i])) {
			m[i] = int_nil;
			bn->tnil = true;
		} else {
			fromtime(t[i], NULL, &m[i], NULL, NULL);
		}
	}
	BATsetcount(bn, n);

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_seconds_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i, n;
	int *s;
	const daytime *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.seconds", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.seconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const daytime *) Tloc(b, 0);
	s = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_daytime_nil(t[i])) {
			s[i] = int_nil;
			bn->tnil = true;
		} else {
			fromtime(t[i], NULL, NULL, &s[i], NULL);
		}
	}
	BATsetcount(bn, n);

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_sql_seconds_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i, n;
	int *s;
	const daytime *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.seconds", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.seconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const daytime *) Tloc(b, 0);
	s = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_daytime_nil(t[i])) {
			s[i] = int_nil;
			bn->tnil = true;
		} else {
			int sec, usec;
			fromtime(t[i], NULL, NULL, &sec, &usec);
			s[i] = sec * 1000 + usec / 1000;
		}
	}
	BATsetcount(bn, n);

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEdaytime_extract_milliseconds_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i, n;
	int *ms;
	const daytime *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.seconds", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.seconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = true;
	bn->tnil = false;

	t = (const daytime *) Tloc(b, 0);
	ms = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (is_daytime_nil(t[i])) {
			ms[i] = int_nil;
			bn->tnil = true;
		} else {
			int usec;
			fromtime(t[i], NULL, NULL, NULL, &usec);
			ms[i] = usec / 1000;
		}
	}
	BATsetcount(bn, n);

	bn->tnonil = !bn->tnil;
	bn->tsorted = BATcount(bn) < 2;
	bn->trevsorted = BATcount(bn) < 2;

	BBPkeepref(*ret = bn->batCacheid);
	BBPunfix(b->batCacheid);
	return MAL_SUCCEED;
}

str
MTIMEstr_to_date(date *d, const char * const *s, const char * const *format)
{
	struct tm t;

	if (strcmp(*s, str_nil) == 0 || strcmp(*format, str_nil) == 0) {
		*d = date_nil;
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	if (strptime(*s, *format, &t) == NULL)
		throw(MAL, "mtime.str_to_date", "format '%s', doesn't match date '%s'\n", *format, *s);
	*d = todate(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
	return MAL_SUCCEED;
}

str
MTIMEdate_to_str(str *s, const date *d, const char * const *format)
{
	struct tm t;
	char buf[512];
	size_t sz;
	int mon, year;

	if (is_date_nil(*d) || strcmp(*format, str_nil) == 0) {
		*s = GDKstrdup(str_nil);
		if (*s == NULL)
			throw(MAL, "mtime.date_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	fromdate(*d, &year, &mon, &t.tm_mday);
	t.tm_mon = mon - 1;
	t.tm_year = year - 1900;
	t.tm_isdst = -1;
	(void)mktime(&t); /* corrects the tm_wday etc */
	if ((sz = strftime(buf, sizeof(buf), *format, &t)) == 0)
		throw(MAL, "mtime.date_to_str", "failed to convert date to string using format '%s'\n", *format);
	*s = GDKmalloc(sz + 1);
	if (*s == NULL)
		throw(MAL, "mtime.date_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	strncpy(*s, buf, sz + 1);
	return MAL_SUCCEED;
}

str
MTIMEstr_to_time(daytime *d, const char * const *s, const char * const *format)
{
	struct tm t;

	if (strcmp(*s, str_nil) == 0 || strcmp(*format, str_nil) == 0) {
		*d = daytime_nil;
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	if (strptime(*s, *format, &t) == NULL)
		throw(MAL, "mtime.str_to_time", "format '%s', doesn't match time '%s'\n", *format, *s);
	*d = totime(t.tm_hour, t.tm_min, t.tm_sec, 0);
	return MAL_SUCCEED;
}

str
MTIMEtime_to_str(str *s, const daytime *d, const char * const *format)
{
	struct tm t;
	char buf[512];
	size_t sz;

	if (is_daytime_nil(*d) || strcmp(*format, str_nil) == 0) {
		*s = GDKstrdup(str_nil);
		if (*s == NULL)
			throw(MAL, "mtime.time_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	fromtime(*d, &t.tm_hour, &t.tm_min, &t.tm_sec, NULL);
	t.tm_isdst = -1;
	(void)mktime(&t); /* corrects the tm_wday etc */
	if ((sz = strftime(buf, sizeof(buf), *format, &t)) == 0)
		throw(MAL, "mtime.time_to_str", "failed to convert time to string using format '%s'\n", *format);
	*s = GDKmalloc(sz + 1);
	if (*s == NULL)
		throw(MAL, "mtime.time_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	strncpy(*s, buf, sz + 1);
	return MAL_SUCCEED;
}

str
MTIMEstr_to_timestamp(timestamp *ts, const char * const *s, const char * const *format)
{
	struct tm t;

	if (strcmp(*s, str_nil) == 0 || strcmp(*format, str_nil) == 0) {
		*ts = timestamp_nil;
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	if (strptime(*s, *format, &t) == NULL)
		throw(MAL, "mtime.str_to_timestamp", "format '%s', doesn't match timestamp '%s'\n", *format, *s);
	*ts = mktimestamp(mkdate(t.tm_year + 1900, t.tm_mon + 1, t.tm_mday),
					  mkdaytime(t.tm_hour, t.tm_min, t.tm_sec, 0));
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_to_str(str *s, const timestamp *ts, const char * const *format)
{
	struct tm t;
	char buf[512];
	size_t sz;
	int mon, year;

	if (is_timestamp_nil(*ts) || strcmp(*format, str_nil) == 0) {
		*s = GDKstrdup(str_nil);
		if (*s == NULL)
			throw(MAL, "mtime.timestamp_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	fromdate(ts_date(*ts), &year, &mon, &t.tm_mday);
	t.tm_mon = mon - 1;
	t.tm_year = year - 1900;
	fromtime(ts_time(*ts), &t.tm_hour, &t.tm_min, &t.tm_sec, NULL);
	t.tm_isdst = -1;
	(void)mktime(&t); /* corrects the tm_wday etc */
	if ((sz = strftime(buf, sizeof(buf), *format, &t)) == 0)
		throw(MAL, "mtime.timestamp_to_str", "failed to convert timestampt to string using format '%s'\n", *format);
	*s = GDKmalloc(sz + 1);
	if (*s == NULL)
		throw(MAL, "mtime.timestamp_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	strncpy(*s, buf, sz + 1);
	return MAL_SUCCEED;
}


date MTIMEtodate(int year, int month, int day) {
	return todate(year, month, day);
}

void MTIMEfromdate(date n, int *y, int *m, int *d) {
	fromdate(n, y, m, d);
}

daytime MTIMEtotime(int hour, int min, int sec, int msec) {
	return totime(hour, min, sec, msec * 1000);
}

void MTIMEfromtime(daytime n, int *hour, int *min, int *sec, int *msec) {
	int usec;
	fromtime(n, hour, min, sec, &usec);
	if (msec && !is_int_nil(usec))
		*msec = usec / 1000;
}

timestamp MTIMEtotimestamp(int year, int month, int day, int hour, int min, int sec, int msec)
{
	date d = todate(year, month, day);
	daytime t = totime(hour, min, sec, msec * 1000);
	if (is_date_nil(d) || is_daytime_nil(t))
		return timestamp_nil;
	return mktimestamp(d, t);
}

void MTIMEfromtimestamp(timestamp t, int *Y, int *M, int *D, int *h, int *m, int *s, int *ms)
{
	int us;
	if (is_timestamp_nil(t)) {
		fromdate(date_nil, Y, M, D);
		fromtime(daytime_nil, h, m, s, &us);
	} else {
		fromdate(ts_date(t), Y, M, D);
		fromtime(ts_time(t), h, m, s, &us);
		if (ms)
			*ms = us / 1000;
	}
}
