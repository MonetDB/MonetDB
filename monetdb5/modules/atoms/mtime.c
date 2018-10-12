/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
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
 * a @samp{date} in the Gregorian calendar, e.g. 1999-JAN-31
 *
 * @item daytime
 * a time of day to the detail of milliseconds, e.g. 23:59:59:000
 *
 * @item timestamp
 * a combination of date and time, indicating an exact point in
 *
 * time (GMT). GMT is the time at the Greenwich meridian without a
 * daylight savings time (DST) regime. Absence of DST means that hours
 * are consecutive (no jumps) which makes it easy to perform time
 * difference calculations.
 *
 * @item timezone
 * the local time is often different from GMT (even at Greenwich in
 * summer, as the UK also has DST). Therefore, whenever a timestamp is
 * composed from a local daytime and date, a timezone should be
 * specified in order to translate the local daytime to GMT (and vice
 * versa if a timestamp is to be decomposed in a local date and
 * daytime).
 *
 * @item rule
 * There is an additional atom @samp{rule} that is used to define when
 * daylight savings time in a timezone starts and ends. We provide
 * predefined timezone objects for a number of timezones below (see
 * the init script of this module).  Also, there is one timezone
 * called the local @samp{timezone}, which can be set to one global
 * value in a running Monet server, that is used if the timezone
 * parameter is omitted from a command that needs it (if not set, the
 * default value of the local timezone is plain GMT).
 * @end table
 *
 * Limitations
 * The valid ranges of the various data types are as follows:
 *
 * @table @samp
 * @item min and max year
 * The maximum and minimum dates and timestamps that can be stored are
 * in the years 5,867,411 and -5,867,411, respectively. Interestingly,
 * the year 0 is not a valid year. The year before 1 is called -1.
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
 * The smallest daytime is 00:00:00:000 and the largest 23:59:59:999
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
 * @item difference in msecs
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
 * @item timezones
 * The basic timezone regime was established on @b{November 1, 1884}
 * in the @emph{International Meridian Conference} held in Greenwich
 * (UK). Before that, a different time held in almost any city. The
 * conference established 24 different time zones defined by regular
 * longitude intervals that all differed by one hour. Not for long it
 * was that national and political interest started to erode this
 * nicely regular system.  Timezones now often follow country borders,
 * and some regions (like the Guinea areas in Latin America) have
 * times that differ with a 15 minute grain from GMT rather than an
 * hour or even half-an-hour grain.
 *
 * An extra complication became the introduction of daylight saving
 * time (DST), which causes a time jump in spring, when the clock is
 * skips one hour and in autumn, when the clock is set back one hour
 * (so in a one hour span, the same times occur twice).  The DST
 * regime is a purely political decision made on a country-by-country
 * basis. Countries in the same timezone can have different DST
 * regimes. Even worse, some countries have DST in some years, and not
 * in other years.
 *
 * To avoid confusion, this module stores absolute points of time in
 * GMT only (GMT does not have a DST regime). When storing local times
 * in the database, or retrieving local times from absolute
 * timestamps, a correct timezone object should be used for the
 * conversion.
 *
 * Applications that do not make correct use of timezones, will
 * produce irregular results on e.g. time difference calculations.
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
 * TODO: we cannot handle well changes in the timezone rules (e.g.,
 * DST only exists since 40 years, and some countries make frequent
 * changes to the DST policy). To accommodate this we should make
 * timezone_local a function with a year parameter. The tool should
 * maintain and access the timezone database stored in two bats
 * [str,timezone],[str,year].  Lookup of the correct timezone would be
 * dynamic in this structure. The timezone_setlocal would just set the
 * string name of the timezone.
 *
 * Time/date comparison
 */

#include "monetdb_config.h"
#include "mtime.h"

#ifndef HAVE_STRPTIME
extern char *strptime(const char *, const char *, struct tm *);
#endif


#define get_rule(r)	((r).s.weekday | ((r).s.day<<6) | ((r).s.minutes<<10) | ((r).s.month<<21))
#define set_rule(r,i)							\
	do {										\
		(r).asint = int_nil;					\
		(r).s.weekday = (i)&15;					\
		(r).s.day = ((i)&(63<<6))>>6;			\
		(r).s.minutes = ((i)&(2047<<10))>>10;	\
		(r).s.month = ((i)&(15<<21))>>21;		\
	} while (0)

/* phony zero values, used to get negative numbers from unsigned
 * sub-integers in rule */
#define WEEKDAY_ZERO	8
#define DAY_ZERO	32
#define OFFSET_ZERO	4096

/* as the offset field got split in two, we need macros to get and set them */
#define get_offset(z)	(((int) (((z)->off1 << 7) + (z)->off2)) - OFFSET_ZERO)
#define set_offset(z,i)	do { (z)->off1 = (((i)+OFFSET_ZERO)&8064) >> 7; (z)->off2 = ((i)+OFFSET_ZERO)&127; } while (0)

tzone tzone_local;

static const char *MONTHS[13] = {
	NULL, "january", "february", "march", "april", "may", "june",
	"july", "august", "september", "october", "november", "december"
};

static const char *DAYS[8] = {
	NULL, "monday", "tuesday", "wednesday", "thursday",
	"friday", "saturday", "sunday"
};
static const char *COUNT1[7] = {
	NULL, "first", "second", "third", "fourth", "fifth", "last"
};
static const char *COUNT2[7] = {
	NULL, "1st", "2nd", "3rd", "4th", "5th", "last"
};
static int LEAPDAYS[13] = {
	0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31
};
static int CUMDAYS[13] = {
	0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
};
static int CUMLEAPDAYS[13] = {
	0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366
};

static date DATE_MAX, DATE_MIN;		/* often used dates; computed once */

#define YEAR_MIN		(-YEAR_MAX)
#define MONTHDAYS(m,y)	((m) != 2 ? LEAPDAYS[m] : leapyear(y) ? 29 : 28)
#define YEARDAYS(y)		(leapyear(y) ? 366 : 365)
#define DATE(d,m,y)		((m) > 0 && (m) <= 12 && (d) > 0 && (y) != 0 && (y) >= YEAR_MIN && (y) <= YEAR_MAX && (d) <= MONTHDAYS(m, y))
#define TIME(h,m,s,x)	((h) >= 0 && (h) < 24 && (m) >= 0 && (m) < 60 && (s) >= 0 && (s) <= 60 && (x) >= 0 && (x) < 1000)
#define LOWER(c)		((c) >= 'A' && (c) <= 'Z' ? (c) + 'a' - 'A' : (c))

/*
 * auxiliary functions
 */

static union {
	timestamp ts;
	lng nilval;
} ts_nil;
static union {
	tzone tz;
	lng nilval;
} tz_nil;
timestamp *timestamp_nil = NULL;
static tzone *tzone_nil = NULL;

int TYPE_date;
int TYPE_daytime;
int TYPE_timestamp;
int TYPE_tzone;
int TYPE_rule;

static int synonyms = TRUE;

#define leapyear(y)		((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))

static int
leapyears(int year)
{
	/* count the 4-fold years that passed since jan-1-0 */
	int y4 = year / 4;

	/* count the 100-fold years */
	int y100 = year / 100;

	/* count the 400-fold years */
	int y400 = year / 400;

	return y4 + y400 - y100 + (year >= 0);	/* may be negative */
}

static date
todate(int day, int month, int year)
{
	date n = date_nil;

	if (DATE(day, month, year)) {
		if (year < 0)
			year++;				/* HACK: hide year 0 */
		n = (date) (day - 1);
		if (month > 2 && leapyear(year))
			n++;
		n += CUMDAYS[month - 1];
		/* current year does not count as leapyear */
		n += 365 * year + leapyears(year >= 0 ? year - 1 : year);
	}
	return n;
}

static void
fromdate(date n, int *d, int *m, int *y)
{
	int day, month, year;

	if (date_isnil(n)) {
		if (d)
			*d = int_nil;
		if (m)
			*m = int_nil;
		if (y)
			*y = int_nil;
		return;
	}
	year = n / 365;
	day = (n - year * 365) - leapyears(year >= 0 ? year - 1 : year);
	if (n < 0) {
		year--;
		while (day >= 0) {
			year++;
			day -= YEARDAYS(year);
		}
		day = YEARDAYS(year) + day;
	} else {
		while (day < 0) {
			year--;
			day += YEARDAYS(year);
		}
	}
	if (d == 0 && m == 0) {
		if (y)
			*y = (year <= 0) ? year - 1 : year;	/* HACK: hide year 0 */
		return;
	}

	day++;
	if (leapyear(year)) {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMLEAPDAYS[month - 1] && day <= CUMLEAPDAYS[month]) {
				if (m)
					*m = month;
				if (d == 0)
					return;
				break;
			}
		day -= CUMLEAPDAYS[month - 1];
	} else {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMDAYS[month - 1] && day <= CUMDAYS[month]) {
				if (m)
					*m = month;
				if (d == 0)
					return;
				break;
			}
		day -= CUMDAYS[month - 1];
	}
	if (d)
		*d = day;
	if (m)
		*m = month;
	if (y)
		*y = (year <= 0) ? year - 1 : year;	/* HACK: hide year 0 */
}

static daytime
totime(int hour, int min, int sec, int msec)
{
	if (TIME(hour, min, sec, msec)) {
		return (daytime) (((((hour * 60) + min) * 60) + sec) * 1000 + msec);
	}
	return daytime_nil;
}

static void
fromtime(daytime n, int *hour, int *min, int *sec, int *msec)
{
	int h, m, s, ms;

	if (!daytime_isnil(n)) {
		h = n / 3600000;
		n -= h * 3600000;
		m = n / 60000;
		n -= m * 60000;
		s = n / 1000;
		n -= s * 1000;
		ms = n;
	} else {
		h = m = s = ms = int_nil;
	}
	if (hour)
		*hour = h;
	if (min)
		*min = m;
	if (sec)
		*sec = s;
	if (msec)
		*msec = ms;
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
static int
date_dayofweek(date v)
{
	/* note, v can be negative, so v%7 is in the range -6...6
	 * v==0 is Saturday, so should result in return value 6 */
	return (v % 7 + 12) % 7 + 1;
}

#define SKIP_DAYS(d, w, i)						\
	do {										\
		d += i;									\
		w = (w + i) % 7;						\
		if (w <= 0)								\
			w += 7;								\
	} while (0)

static date
compute_rule(const rule *val, int y)
{
	int m = val->s.month, cnt = abs(val->s.day - DAY_ZERO);
	date d = todate(1, m, y);
	int dayofweek = date_dayofweek(d);
	int w = abs(val->s.weekday - WEEKDAY_ZERO);

	if (val->s.weekday == WEEKDAY_ZERO || w == WEEKDAY_ZERO) {
		/* cnt-th of month */
		d += cnt - 1;
	} else if (val->s.day > DAY_ZERO) {
		if (val->s.weekday < WEEKDAY_ZERO) {
			/* first weekday on or after cnt-th of month */
			SKIP_DAYS(d, dayofweek, cnt - 1);
			cnt = 1;
		}						/* ELSE cnt-th weekday of month */
		while (dayofweek != w || --cnt > 0) {
			if (++dayofweek == WEEKDAY_ZERO)
				dayofweek = 1;
			d++;
		}
	} else {
		if (val->s.weekday > WEEKDAY_ZERO) {
			/* cnt-last weekday from end of month */
			SKIP_DAYS(d, dayofweek, MONTHDAYS(m, y) - 1);
		} else {
			/* first weekday on or before cnt-th of month */
			SKIP_DAYS(d, dayofweek, cnt - 1);
			cnt = 1;
		}
		while (dayofweek != w || --cnt > 0) {
			if (--dayofweek == 0)
				dayofweek = 7;
			d--;
		}
	}
	return d;
}

#define BEFORE(d1, m1, d2, m2) ((d1) < (d2) || ((d1) == (d2) && (m1) <= (m2)))

static int
timestamp_inside(timestamp *ret, const timestamp *t, const tzone *z, lng offset)
{
	/* starts with GMT time t, and returns whether it is in the DST for z */
	lng add = (offset != (lng) 0) ? offset : (get_offset(z)) * (lng) 60000;
	int start_days, start_msecs, end_days, end_msecs, year;
	rule start, end;

	MTIMEtimestamp_add(ret, t, &add);

	if (ts_isnil(*ret) || z->dst == 0) {
		return 0;
	}
	set_rule(start, z->dst_start);
	set_rule(end, z->dst_end);

	start_msecs = start.s.minutes * 60000;
	end_msecs = end.s.minutes * 60000;

	fromdate(ret->days, NULL, NULL, &year);
	start_days = compute_rule(&start, year);
	end_days = compute_rule(&end, year);

	return BEFORE(start_days, start_msecs, end_days, end_msecs) ?
		(BEFORE(start_days, start_msecs, ret->days, ret->msecs) &&
		 BEFORE(ret->days, ret->msecs, end_days, end_msecs)) :
		(BEFORE(start_days, start_msecs, ret->days, ret->msecs) ||
		 BEFORE(ret->days, ret->msecs, end_days, end_msecs));
}

/*
 * ADT implementations
 */
ssize_t
date_fromstr(const char *buf, size_t *len, date **d)
{
	int day = 0, month = int_nil;
	int year = 0, yearneg = (buf[0] == '-'), yearlast = 0;
	ssize_t pos = 0;
	int sep;

	if (*len < sizeof(date) || *d == NULL) {
		GDKfree(*d);
		*d = (date *) GDKmalloc(*len = sizeof(date));
		if( *d == NULL)
			return -1;
	}
	**d = date_nil;
	if (strcmp(buf, str_nil) == 0)
		return 1;
	if (yearneg == 0 && !GDKisdigit(buf[0])) {
		if (!synonyms) {
			GDKerror("Syntax error in date.\n");
			return -1;
		}
		yearlast = 1;
		sep = ' ';
	} else {
		for (pos = yearneg; GDKisdigit(buf[pos]); pos++) {
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
			yearneg = 1;
			pos++;
		}
		while (GDKisdigit(buf[pos])) {
			year = (buf[pos++] - '0') + year * 10;
			if (year > YEAR_MAX)
				break;
		}
	}
	/* handle semantic error here (returns nil in that case) */
	**d = todate(day, month, yearneg ? -year : year);
	if (date_isnil(**d)) {
		GDKerror("Semantic error in date.\n");
		return -1;
	}
	return pos;
}

ssize_t
date_tostr(str *buf, size_t *len, const date *val)
{
	int day, month, year;

	fromdate(*val, &day, &month, &year);
	/* longest possible string: "-5867411-01-01" i.e. 14 chars
	   without NUL (see definition of YEAR_MIN/YEAR_MAX above) */
	if (*len < 15 || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 15);
		if( *buf == NULL)
			return -1;
	}
	if (date_isnil(*val) || !DATE(day, month, year)) {
		strcpy(*buf, "nil");
		return 3;
	}
	sprintf(*buf, "%d-%02d-%02d", year, month, day);
	return (ssize_t) strlen(*buf);
}

/*
 * @- daytime
 */
ssize_t
daytime_fromstr(const char *buf, size_t *len, daytime **ret)
{
	int hour, min, sec = 0, msec = 0;
	ssize_t pos = 0;

	if (*len < sizeof(daytime) || *ret == NULL) {
		GDKfree(*ret);
		*ret = (daytime *) GDKmalloc(*len = sizeof(daytime));
		if (*ret == NULL)
			return -1;
	}
	**ret = daytime_nil;
	if (strcmp(buf, str_nil) == 0)
		return 1;
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
			for (i = 0; i < 3; i++) {
				msec *= 10;
				if (GDKisdigit(buf[pos])) {
					msec += buf[pos] - '0';
					pos++;
				}
			}
#ifndef TRUNCATE_NUMBERS
			if (GDKisdigit(buf[pos]) && buf[pos] >= '5') {
				/* round the value */
				if (++msec == 1000) {
					msec = 0;
					if (++sec == 60) {
						sec = 0;
						if (++min == 60) {
							min = 0;
							if (++hour == 24) {
								/* forget about rounding if it doesn't fit */
								hour = 23;
								min = 59;
								sec = 59;
								msec = 999;
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
	**ret = totime(hour, min, sec, msec);
	if (daytime_isnil(**ret)) {
		GDKerror("Semantic error in time.\n");
		return -1;
	}
	return pos;
}

ssize_t
daytime_tz_fromstr(const char *buf, size_t *len, daytime **ret)
{
	const char *s = buf;
	ssize_t pos = daytime_fromstr(s, len, ret);
	lng val, offset = 0;
	daytime mtime = 24 * 60 * 60 * 1000;

	if (pos < 0 || daytime_isnil(**ret))
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
		offset = (((s[1] - '0') * (lng) 10 + (s[2] - '0')) * (lng) 60 + (s[pos] - '0') * (lng) 10 + (s[pos + 1] - '0')) * (lng) 60000;
		pos += 2;
		if (s[0] != '-')
			offset = -offset;
		s += pos;
	} else {
		/* if no tzone is specified; work with the local */
		offset = get_offset(&tzone_local) * (lng) -60000;
	}
	val = **ret + offset;
	if (val < 0)
		val = mtime + val;
	if (val >= mtime)
		val = val - mtime;
	**ret = (daytime) val;
	return (ssize_t) (s - buf);
}

ssize_t
daytime_tostr(str *buf, size_t *len, const daytime *val)
{
	int hour, min, sec, msec;

	fromtime(*val, &hour, &min, &sec, &msec);
	if (*len < 12 || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 13);
		if( *buf == NULL)
			return -1;
	}
	if (daytime_isnil(*val) || !TIME(hour, min, sec, msec)) {
		strcpy(*buf, "nil");
		return 3;
	}
	return sprintf(*buf, "%02d:%02d:%02d.%03d", hour, min, sec, msec);
}

/*
 * @- timestamp
 */
ssize_t
timestamp_fromstr(const char *buf, size_t *len, timestamp **ret)
{
	const char *s = buf;
	ssize_t pos;
	date *d;
	daytime *t;

	if (*len < sizeof(timestamp) || *ret == NULL) {
		GDKfree(*ret);
		*ret = (timestamp *) GDKmalloc(*len = sizeof(timestamp));
		if( *ret == NULL)
			return -1;
	}
	d = &(*ret)->days;
	t = &(*ret)->msecs;
	(*ret)->msecs = 0;
	pos = date_fromstr(buf, len, &d);
	if (pos < 0)
		return pos;
	if (date_isnil(*d)) {
		**ret = *timestamp_nil;
		return pos;
	}
	s += pos;
	if (*s == '@' || *s == ' ' || *s == '-' || *s == 'T') {
		while (*++s == ' ')
			;
		pos = daytime_fromstr(s, len, &t);
		if (pos < 0)
			return pos;
		s += pos;
		if (daytime_isnil(*t)) {
			**ret = *timestamp_nil;
			return (ssize_t) (s - buf);
		}
	} else if (*s) {
		(*ret)->msecs = daytime_nil;
	}
	if (date_isnil((*ret)->days) || daytime_isnil((*ret)->msecs)) {
		**ret = *timestamp_nil;
	} else {
		lng offset = 0;

		while (GDKisspace(*s))
			s++;
		/* in case of gmt we need to add the time zone */
		if (fleximatch(s, "gmt", 0) == 3) {
			s += 3;
		}
		if ((s[0] == '-' || s[0] == '+') &&
			GDKisdigit(s[1]) && GDKisdigit(s[2]) && GDKisdigit(s[pos = 4]) &&
			((s[3] == ':' && GDKisdigit(s[5])) || GDKisdigit(s[pos = 3]))) {
			offset = (((s[1] - '0') * (lng) 10 + (s[2] - '0')) * (lng) 60 + (s[pos] - '0') * (lng) 10 + (s[pos + 1] - '0')) * (lng) 60000;
			pos += 2;
			if (s[0] != '-')
				offset = -offset;
			s += pos;
		} else {
			/* if no tzone is specified; work with the local */
			timestamp tmp = **ret;

			offset = get_offset(&tzone_local) * (lng) -60000;
			if (timestamp_inside(&tmp, &tmp, &tzone_local, (lng) -3600000)) {
				**ret = tmp;
			}
		}
		MTIMEtimestamp_add(*ret, *ret, &offset);
	}
	return (ssize_t) (s - buf);
}

ssize_t
timestamp_tz_fromstr(const char *buf, size_t *len, timestamp **ret)
{
	const char *s = buf;
	ssize_t pos = timestamp_fromstr(s, len, ret);
	lng offset = 0;

	if (pos < 0 || *ret == timestamp_nil)
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
		offset = (((s[1] - '0') * (lng) 10 + (s[2] - '0')) * (lng) 60 + (s[pos] - '0') * (lng) 10 + (s[pos + 1] - '0')) * (lng) 60000;
		pos += 2;
		if (s[0] != '-')
			offset = -offset;
		s += pos;
	} else {
		/* if no tzone is specified; work with the local */
		offset = get_offset(&tzone_local) * (lng) -60000;
	}
	MTIMEtimestamp_add(*ret, *ret, &offset);
	return (ssize_t) (s - buf);
}


ssize_t
timestamp_tz_tostr(str *buf, size_t *len, const timestamp *val, const tzone *timezone)
{
	ssize_t len1, len2;
	size_t big = 128;
	char buf1[128], buf2[128], *s = *buf, *s1 = buf1, *s2 = buf2;
	if (timezone != NULL) {
		/* int off = get_offset(timezone); */
		timestamp tmp = *val;

		if (!ts_isnil(tmp) && timestamp_inside(&tmp, val, timezone, (lng) 0)) {
			lng add = (lng) 3600000;

			MTIMEtimestamp_add(&tmp, &tmp, &add);
			/* off += 60; */
		}
		len1 = date_tostr(&s1, &big, &tmp.days);
		len2 = daytime_tostr(&s2, &big, &tmp.msecs);
		if (len1 < 0 || len2 < 0)
			return -1;

		if (*len < 2 + (size_t) len1 + (size_t) len2 || *buf == NULL) {
			GDKfree(*buf);
			*buf = GDKmalloc(*len = (size_t) len1 + (size_t) len2 + 2);
			if( *buf == NULL)
				return -1;
		}
		s = *buf;
		if (ts_isnil(tmp)) {
			strcpy(s, "nil");
			return 3;
		}
		strcpy(s, buf1);
		s += len1;
		*s++ = ' ';
		strcpy(s, buf2);
		s += len2;
		/* omit GMT distance in order not to confuse the confused user
		   strcpy(s, "GMT"); s += 3;
		   if (off) {
		   *s++ = (off>=0)?'+':'-';
		   sprintf(s, "%02d%02d", abs(off)/60, abs(off)%60);
		   s += 4;
		   }
		 */
	}
	return (ssize_t) (s - *buf);
}

ssize_t
timestamp_tostr(str *buf, size_t *len, const timestamp *val)
{
	return timestamp_tz_tostr(buf, len, val, &tzone_local);
}

static const char *
count1(int i)
{
	static char buf[16];

	if (i <= 0) {
		return "(illegal number)";
	} else if (i < 6) {
		return COUNT1[i];
	}
	sprintf(buf, "%dth", i);
	return buf;
}

/*
 * @- rule
 */
ssize_t
rule_tostr(str *buf, size_t *len, const rule *r)
{
	int hours = r->s.minutes / 60;
	int minutes = r->s.minutes % 60;

	if (*len < 64 || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 64);
		if( *buf == NULL)
			return -1;
	}
	if (is_int_nil(r->asint)) {
		strcpy(*buf, "nil");
	} else if (r->s.weekday == WEEKDAY_ZERO) {
		sprintf(*buf, "%s %d@%02d:%02d",
				MONTHS[r->s.month], r->s.day - DAY_ZERO, hours, minutes);
	} else if (r->s.weekday > WEEKDAY_ZERO && r->s.day > DAY_ZERO) {
		sprintf(*buf, "%s %s from start of %s@%02d:%02d",
				count1(r->s.day - DAY_ZERO), DAYS[r->s.weekday - WEEKDAY_ZERO],
				MONTHS[r->s.month], hours, minutes);
	} else if (r->s.weekday > WEEKDAY_ZERO && r->s.day < DAY_ZERO) {
		sprintf(*buf, "%s %s from end of %s@%02d:%02d",
				count1(DAY_ZERO - r->s.day), DAYS[r->s.weekday - WEEKDAY_ZERO],
				MONTHS[r->s.month], hours, minutes);
	} else if (r->s.day > DAY_ZERO) {
		sprintf(*buf, "first %s on or after %s %d@%02d:%02d",
				DAYS[WEEKDAY_ZERO - r->s.weekday], MONTHS[r->s.month],
				r->s.day - DAY_ZERO, hours, minutes);
	} else {
		sprintf(*buf, "last %s on or before %s %d@%02d:%02d",
				DAYS[WEEKDAY_ZERO - r->s.weekday], MONTHS[r->s.month],
				DAY_ZERO - r->s.day, hours, minutes);
	}
	return (ssize_t) strlen(*buf);
}

ssize_t
rule_fromstr(const char *buf, size_t *len, rule **d)
{
	int day = 0, month = 0, weekday = 0, hours = 0, minutes = 0;
	int neg_day = 0, neg_weekday = 0, pos;
	const char *cur = buf;

	if (*len < (int) sizeof(rule) || *d == NULL) {
		GDKfree(*d);
		*d = (rule *) GDKmalloc(*len = sizeof(rule));
		if( *d == NULL)
			return -1;
	}
	(*d)->asint = int_nil;
	if (strcmp(buf, str_nil) == 0)
		return 1;

	/* start parsing something like "first", "second", .. etc */
	pos = parse_substr(&day, cur, 0, COUNT1, 6);
	if (pos == 0) {
		pos = parse_substr(&day, cur, 0, COUNT2, 6);
	}
	if (pos && cur[pos++] == ' ') {
		/* now we must see a weekday */
		cur += pos;
		cur += parse_substr(&weekday, cur, 3, DAYS, 7);
		if (is_int_nil(weekday)) {
			return 0;			/* syntax error */
		}
		pos = fleximatch(cur, " from start of ", 0);
		if (pos == 0) {
			pos = fleximatch(cur, " from end of ", 0);
			if (pos)
				neg_day = 1;
		}
		if (pos && day < 6) {
			/* RULE 1+2: X-th weekday from start/end of month */
			pos = parse_substr(&month, cur += pos, 3, MONTHS, 12);
		} else if (day == 1) {
			/* RULE 3: first weekday on or after-th of month */
			pos = fleximatch(cur, " on or after ", 0);
			neg_weekday = 1;
			day = int_nil;		/* re-read below */
		} else if (day == 6) {
			/* RULE 4: last weekday on or before X-th of month */
			pos = fleximatch(cur, " on or before ", 0);
			neg_weekday = neg_day = 1;
			day = int_nil;		/* re-read below */
		}
		if (pos == 0) {
			GDKerror("Syntax error in timezone rule.\n");
			return -1;
		}
		cur += pos;
	}
	if (is_int_nil(day)) {
		/* RULE 5:  X-th of month */
		cur += parse_substr(&month, cur, 3, MONTHS, 12);
		if (is_int_nil(month) || *cur++ != ' ' || !GDKisdigit(*cur)) {
			GDKerror("Syntax error in timezone rule.\n");
			return -1;
		}
		day = 0;
		while (GDKisdigit(*cur) && day < 31) {
			day = (*(cur++) - '0') + day * 10;
		}
	}

	/* parse hours:minutes */
	if (*cur++ != '@' || !GDKisdigit(*cur)) {
		GDKerror("Syntax error in timezone rule.\n");
		return -1;
	}
	while (GDKisdigit(*cur) && hours < 24) {
		hours = (*(cur++) - '0') + hours * 10;
	}
	if (*cur++ != ':' || !GDKisdigit(*cur)) {
		GDKerror("Syntax error in timezone rule.\n");
		return -1;
	}
	while (GDKisdigit(*cur) && minutes < 60) {
		minutes = (*(cur++) - '0') + minutes * 10;
	}

	/* assign if semantically ok */
	if (day >= 1 && day <= LEAPDAYS[month] &&
		hours >= 0 && hours < 60 &&
		minutes >= 0 && minutes < 60) {
		(*d)->s.month = month;
		(*d)->s.weekday = WEEKDAY_ZERO + (neg_weekday ? -weekday : weekday);
		(*d)->s.day = DAY_ZERO + (neg_day ? -day : day);
		(*d)->s.minutes = hours * 60 + minutes;
	}
	return (ssize_t) (cur - buf);
}

/*
 * @- tzone
 */
ssize_t
tzone_fromstr(const char *buf, size_t *len, tzone **d)
{
	int hours = 0, minutes = 0, neg_offset = 0;
	ssize_t pos = 0;
	rule r1, *rp1 = &r1, r2, *rp2 = &r2;
	const char *cur = buf;

	rp1->asint = rp2->asint = 0;
	if (*len < (int) sizeof(tzone) || *d == NULL) {
		GDKfree(*d);
		*d = (tzone *) GDKmalloc(*len = sizeof(tzone));
		if( *d == NULL)
			return -1;
	}
	**d = *tzone_nil;
	if (strcmp(buf, str_nil) == 0)
		return 1;

	/* syntax checks */
	if (fleximatch(cur, "gmt", 0) == 0) {
		GDKerror("Syntax error in timezone.\n");
		return -1;
	}
	cur += 3;
	if (*cur == '-' || *cur == '+') {
		const char *bak = cur + 1;

		neg_offset = (*cur++ == '-');
		if (!GDKisdigit(*cur)) {
			GDKerror("Syntax error in timezone.\n");
			return -1;
		}
		while (GDKisdigit(*cur) && hours < 9999) {
			hours = (*(cur++) - '0') + hours * 10;
		}
		if (*cur == ':' && GDKisdigit(cur[1])) {
			cur++;
			do {
				minutes = (*(cur++) - '0') + minutes * 10;
			} while (GDKisdigit(*cur) && minutes < 60);
		} else if (*cur != ':' && (cur - bak) == 4) {
			minutes = hours % 100;
			hours = hours / 100;
		} else {
			GDKerror("Syntax error in timezone.\n");
			return -1;
		}
	}
	if (fleximatch(cur, "-dst[", 0)) {
		pos = rule_fromstr(cur += 5, len, &rp1);
		if (pos < 0)
			return pos;
		if (is_int_nil(rp1->asint)) {
			**d = *tzone_nil;
			return (ssize_t) (cur + pos - buf);;
		}
		if (cur[pos++] != ',') {
			GDKerror("Syntax error in timezone.\n");
			return -1;
		}
		pos = rule_fromstr(cur += pos, len, &rp2);
		if (pos < 0)
			return pos;
		if (is_int_nil(rp2->asint)) {
			**d = *tzone_nil;
			return (ssize_t) (cur + pos - buf);;
		}
		if (cur[pos++] != ']') {
			GDKerror("Syntax error in timezone.\n");
			return -1;
		}
		cur += pos;
	}
	/* semantic check */
	if (hours < 24 && minutes < 60 &&
		!is_int_nil(rp1->asint) && !is_int_nil(rp2->asint)) {
		minutes += hours * 60;
		set_offset(*d, neg_offset ? -minutes : minutes);
		if (pos) {
			(*d)->dst = TRUE;
			(*d)->dst_start = get_rule(r1);
			(*d)->dst_end = get_rule(r2);
		} else {
			(*d)->dst = FALSE;
		}
	}
	return (ssize_t) (cur - buf);
}

ssize_t
tzone_tostr(str *buf, size_t *len, const tzone *z)
{
	str s;

	if (*len < 160 || *buf == NULL) {
		GDKfree(*buf);
		*buf = (str) GDKmalloc(*len = 160);
		if( *buf == NULL)
			return -1;
	}
	s = *buf;
	if (tz_isnil(*z)) {
		strcpy(s, "nil");
		s += 3;
	} else {
		rule dst_start, dst_end;
		int mins = get_offset(z);

		set_rule(dst_start, z->dst_start);
		set_rule(dst_end, z->dst_end);

		if (z->dst)
			*s++ = '"';
		strcpy(s, "GMT");
		s += 3;
		if (mins > 0) {
			sprintf(s, "+%02d:%02d", mins / 60, mins % 60);
			s += 6;
		} else if (mins < 0) {
			sprintf(s, "-%02d:%02d", (-mins) / 60, (-mins) % 60);
			s += 6;
		}
		if (z->dst) {
			ssize_t l;
			strcpy(s, "-DST[");
			s += 5;
			l = rule_tostr(&s, len, &dst_start);
			if (l < 0)
				return -1;
			s += l;
			*s++ = ',';
			l = rule_tostr(&s, len, &dst_end);
			if (l < 0)
				return -1;
			s += l;
			*s++ = ']';
			*s++ = '"';
			*s = 0;
		}
	}
	return (ssize_t) (s - *buf);
}

/*
 * operator implementations
 */
static str
tzone_set_local(const tzone *z)
{
	if (tz_isnil(*z))
		throw(MAL, "mtime.timezone_local", "cannot set timezone to nil");
	tzone_local = *z;
	return MAL_SUCCEED;
}

static str
daytime_add(daytime *ret, const daytime *v, const lng *msec)
{
	if (daytime_isnil(*v)) {
		*ret = int_nil;
	} else {
		*ret = *v + (daytime) (*msec);
	}
	return MAL_SUCCEED;
}

/* returns the timestamp that comes 'milliseconds' after 'value'. */
str
MTIMEtimestamp_add(timestamp *ret, const timestamp *v, const lng *msec)
{
	if (!ts_isnil(*v) && !is_lng_nil(*msec)) {
		int day = (int) (*msec / (24 * 60 * 60 * 1000));

		ret->msecs = (int) (v->msecs + (*msec - ((lng) day) * (24 * 60 * 60 * 1000)));
		ret->days = v->days;
		if (ret->msecs >= (24 * 60 * 60 * 1000)) {
			day++;
			ret->msecs -= (24 * 60 * 60 * 1000);
		} else if (ret->msecs < 0) {
			day--;
			ret->msecs += (24 * 60 * 60 * 1000);
		}
		if (day) {
			MTIMEdate_adddays(&ret->days, &ret->days, &day);
			if (is_int_nil(ret->days)) {
				*ret = *timestamp_nil;
			}
		}
	} else {
		*ret = *timestamp_nil;
	}
	return MAL_SUCCEED;
}

union lng_tzone {
	lng lval;
	tzone tzval;
};

/*
 * Wrapper
 * The Monet V5 API interface is defined here
 */
#define TIMEZONES(X1, X2)												\
	do {																\
		str err;														\
		ticks = (X2);													\
		if ((err = MTIMEtzone_create(&ltz.tzval, &ticks)) != MAL_SUCCEED) \
			return err;													\
		vr.val.lval = ltz.lval;											\
		if (BUNappend(tzbatnme, (X1), false) != GDK_SUCCEED ||			\
			BUNappend(tzbatdef, &vr.val.lval, false) != GDK_SUCCEED)	\
			goto bailout;												\
	} while (0)

#define TIMEZONES2(X1, X2, X3, X4)										\
	do {																\
		str err;														\
		ticks = (X2);													\
		if ((err = MTIMEtzone_create_dst(&ltz.tzval, &ticks, &(X3), &(X4))) != MAL_SUCCEED) \
			return err;													\
		vr.val.lval = ltz.lval;											\
		if (BUNappend(tzbatnme, (X1), false) != GDK_SUCCEED ||			\
			BUNappend(tzbatdef, &vr.val.lval, false) != GDK_SUCCEED)	\
			goto bailout;												\
	} while (0)

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

static BAT *timezone_name = NULL;
static BAT *timezone_def = NULL;

void MTIMEreset(void) {
	timezone_name = NULL;
	timezone_def = NULL;
}

str
MTIMEprelude(void *ret)
{
	const char *msg = NULL;
	char *err;
	ValRecord vr;
	int ticks;
	union lng_tzone ltz;
	rule RULE_MAR, RULE_OCT;
	const char *s1 = "first sunday from end of march@02:00";
	const char *s2 = "first sunday from end of october@02:00";
	tzone tz;
	BAT *tzbatnme;
	BAT *tzbatdef;

	(void) ret;
	ts_nil.nilval = lng_nil;
	tz_nil.nilval = lng_nil;

	timestamp_nil = &ts_nil.ts;
	tzone_nil = &tz_nil.tz;

	TYPE_date = ATOMindex("date");
	TYPE_daytime = ATOMindex("daytime");
	TYPE_timestamp = ATOMindex("timestamp");
	TYPE_tzone = ATOMindex("timezone");
	TYPE_rule = ATOMindex("rule");

	MONTHS[0] = (str) str_nil;
	DAYS[0] = (str) str_nil;
	LEAPDAYS[0] = int_nil;
	DATE_MAX = todate(31, 12, YEAR_MAX);
	DATE_MIN = todate(1, 1, YEAR_MIN);
	tzone_local.dst = 0;
	set_offset(&tzone_local, 0);

	tz = *tzone_nil;			/* to ensure initialized variables */

	/* if it was already filled we can skip initialization */
	if( timezone_name )
		return MAL_SUCCEED;
	tzbatnme = COLnew(0, TYPE_str, 30, TRANSIENT);
	tzbatdef = COLnew(0, ATOMindex("timezone"), 30, TRANSIENT);

	if (tzbatnme == NULL || tzbatdef == NULL) {
		BBPreclaim(tzbatnme);
		BBPreclaim(tzbatdef);
		throw(MAL, "time.prelude", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	if (BBPrename(tzbatnme->batCacheid, "timezone_name") != 0 ||
		BBPrename(tzbatdef->batCacheid, "timezone_def") != 0)
		throw(MAL, "time.prelude", GDK_EXCEPTION);
	timezone_name = tzbatnme;
	timezone_def = tzbatdef;

/* perhaps add the following to the global kvstore
* 	timezone_name
* 	timezone_def
*/
	vr.vtype = ATOMindex("timezone");
	TIMEZONES("Wake Island", 12 * 60);
	TIMEZONES("Melbourne/Australia", 11 * 60);
	TIMEZONES("Brisbane/Australia", 10 * 60);
	TIMEZONES("Japan", 9 * 60);
	TIMEZONES("Singapore", 8 * 60);
	TIMEZONES("Thailand", 7 * 60);
	TIMEZONES("Pakistan", 5 * 60);
	TIMEZONES("United Arab Emirates", 4 * 60);
	TIMEZONES("GMT", 0 * 0);
	TIMEZONES("Azore Islands", -1 * 60);
	TIMEZONES("Hawaii/USA", -10 * 60);
	TIMEZONES("American Samoa", -11 * 60);
	if ((err = MTIMErule_fromstr(&RULE_MAR, &s1)) != MAL_SUCCEED ||
		(err = MTIMErule_fromstr(&RULE_OCT, &s2)) != MAL_SUCCEED)
		return err;
	TIMEZONES2("Kazakhstan", 6 * 60, RULE_MAR, RULE_OCT);
	TIMEZONES2("Moscow/Russia", 3 * 60, RULE_MAR, RULE_OCT);
	TIMEZONES2("East/Europe", 2 * 60, RULE_MAR, RULE_OCT);
	TIMEZONES2("West/Europe", 1 * 60, RULE_MAR, RULE_OCT);
	TIMEZONES2("UK", 0 * 0, RULE_MAR, RULE_OCT);
	TIMEZONES2("Eastern/Brazil", -2 * 60, RULE_OCT, RULE_MAR);
	TIMEZONES2("Western/Brazil", -3 * 60, RULE_OCT, RULE_MAR);
	TIMEZONES2("Andes/Brazil", -4 * 60, RULE_OCT, RULE_MAR);
	TIMEZONES2("East/USA", -5 * 60, RULE_MAR, RULE_OCT);
	TIMEZONES2("Central/USA", -6 * 60, RULE_MAR, RULE_OCT);
	TIMEZONES2("Mountain/USA", -7 * 60, RULE_MAR, RULE_OCT);
	TIMEZONES2("Alaska/USA", -9 * 60, RULE_MAR, RULE_OCT);
	msg = "West/Europe";
	return MTIMEtimezone(&tz, &msg);
  bailout:
	throw(MAL, "mtime.prelude", SQLSTATE(HY001) MAL_MALLOC_FAIL);
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

str
MTIMEtimezone(tzone *ret, const char * const *name)
{
	BUN p;
	str s;
	tzone *z;
	BATiter tzi;

	if ((p = BUNfnd(timezone_name, *name)) == BUN_NONE)
		throw(MAL, "mtime.setTimezone", "unknown timezone");
	tzi = bat_iterator(timezone_def);
	z = (tzone *) BUNtail(tzi, p);
	if ((s = tzone_set_local(z)) != MAL_SUCCEED)
		return s;
	*ret = *z;
	return MAL_SUCCEED;
}

str
MTIMEtzone_set_local(void *res, const tzone *z)
{
	(void) res;					/* fool compilers */
	return tzone_set_local(z);
}

str
MTIMEtzone_get_local(tzone *z)
{
	*z = tzone_local;
	return MAL_SUCCEED;
}

str
MTIMElocal_timezone(lng *res)
{
	tzone z;

	MTIMEtzone_get_local(&z);
	*res = get_offset(&z);
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
	if (date_fromstr(*s, &len, &ret) < 0)
		throw(MAL, "mtime.date", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/* creates a date from (day,month,year) parameters */
str
MTIMEdate_create(date *ret, const int *year, const int *month, const int *day)
{
	*ret = todate(*day, *month, *year);
	return MAL_SUCCEED;
}

/* creates a daytime from (hours,minutes,seconds,milliseconds) parameters */
str
MTIMEdaytime_create(daytime *ret, const int *hour, const int *min, const int *sec, const int *msec)
{
	*ret = totime(*hour, *min, *sec, *msec);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_fromstr(timestamp *ret, const char * const *d)
{
	size_t len = sizeof(timestamp);

	if (strcmp(*d, "nil") == 0) {
		ret->msecs = daytime_nil;
		ret->days = date_nil;
		return MAL_SUCCEED;
	}
	if (timestamp_fromstr(*d, &len, &ret) < 0)
		throw(MAL, "mtime.timestamp", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/* creates a timestamp from (date,daytime) parameters */
str
MTIMEtimestamp_create(timestamp *ret, const date *d, const daytime *t, const tzone *z)
{
	if (date_isnil(*d) || daytime_isnil(*t) || tz_isnil(*z)) {
		*ret = *timestamp_nil;
	} else {
		lng add = get_offset(z) * (lng) -60000;

		ret->days = *d;
		ret->msecs = *t;
		if (z->dst) {
			timestamp tmp;

			if (timestamp_inside(&tmp, ret, z, (lng) -3600000)) {
				*ret = tmp;
			}
		}
		MTIMEtimestamp_add(ret, ret, &add);
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_create_default(timestamp *ret, const date *d, const daytime *t)
{
	return MTIMEtimestamp_create(ret, d, t, &tzone_local);
}

str
MTIMEtimestamp_create_from_date(timestamp *ret, const date *d)
{
	daytime t = totime(0, 0, 0, 0);
	return MTIMEtimestamp_create(ret, d, &t, &tzone_local);
}

str
MTIMEtimestamp_create_from_date_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	timestamp *t, tmp;
	const date *d;
	const daytime dt = totime(0, 0, 0, 0);
	BUN n;
	lng add = get_offset(&tzone_local) * (lng) -60000;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	d = (const date *) Tloc(b, 0);
	t = (timestamp *) Tloc(bn, 0);
	bn->tnil = 0;
	for (n = BATcount(b); n > 0; n--, t++, d++) {
		if (date_isnil(*d)) {
			*t = *timestamp_nil;
			bn->tnil = 1;
		} else {
			t->days = *d;
			t->msecs = dt;
			if (tzone_local.dst &&
				timestamp_inside(&tmp, t, &tzone_local, (lng) -3600000))
				*t = tmp;
			MTIMEtimestamp_add(t, t, &add);
			if (ts_isnil(*t))
				bn->tnil = 1;
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
	if (date_isnil(*v)) {
		*ret = int_nil;
	} else {
		fromdate(*v, NULL, NULL, ret);
	}
	return MAL_SUCCEED;
}

/* extracts quarter from date (value between 1 and 4) */
str
MTIMEdate_extract_quarter(int *ret, const date *v)
{
	if (date_isnil(*v)) {
		*ret = int_nil;
	} else {
		int mnd = 0;
		fromdate(*v, NULL, &mnd, NULL);
		if (mnd <= 6) {
			*ret = (mnd <= 3) ? 1 : 2;
		} else {
			*ret = (mnd <= 9) ? 3 : 4;
		}
	}
	return MAL_SUCCEED;
}

/* extracts month from date (value between 1 and 12) */
str
MTIMEdate_extract_month(int *ret, const date *v)
{
	if (date_isnil(*v)) {
		*ret = int_nil;
	} else {
		fromdate(*v, NULL, ret, NULL);
	}
	return MAL_SUCCEED;
}

/* extracts day from date (value between 1 and 31)*/
str
MTIMEdate_extract_day(int *ret, const date *v)
{
	if (date_isnil(*v)) {
		*ret = int_nil;
	} else {
		fromdate(*v, ret, NULL, NULL);
	}
	return MAL_SUCCEED;
}

/* Returns N where d is the Nth day of the year (january 1 returns 1). */
str
MTIMEdate_extract_dayofyear(int *ret, const date *v)
{
	if (date_isnil(*v)) {
		*ret = int_nil;
	} else {
		int year;

		fromdate(*v, NULL, NULL, &year);
		*ret = (int) (1 + *v - todate(1, 1, year));
	}
	return MAL_SUCCEED;
}

/* Returns the week number */
str
MTIMEdate_extract_weekofyear(int *ret, const date *v)
{
	if (date_isnil(*v)) {
		*ret = int_nil;
	} else {
		int year;
		date thd;
		date thd1;

		/* find the Thursday in the same week as the given date */
		thd = *v + 4 - date_dayofweek(*v);
		/* extract the year (may be different from year of the given date!) */
		fromdate(thd, NULL, NULL, &year);
		/* find January 4 of that year */
		thd1 = todate(4, 1, year);
		/* find the Thursday of the week in which January 4 falls */
		thd1 += 4 - date_dayofweek(thd1);
		/* now calculate the week number */
		*ret = (int) ((thd - thd1) / 7) + 1;
	}
	return MAL_SUCCEED;
}

/* Returns the current day  of the week where 1=monday, .., 7=sunday */
str
MTIMEdate_extract_dayofweek(int *ret, const date *v)
{
	if (date_isnil(*v)) {
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
	if (daytime_isnil(*v)) {
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
	if (daytime_isnil(*v)) {
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
	if (daytime_isnil(*v)) {
		*ret = int_nil;
	} else {
		fromtime(*v, NULL, NULL, ret, NULL);
	}
	return MAL_SUCCEED;
}

/* extracts (milli) seconds from daytime (value between 0 and 59000) */
str
MTIMEdaytime_extract_sql_seconds(int *ret, const daytime *v)
{
	int sec, milli;

	if (daytime_isnil(*v)) {
		*ret = int_nil;
	} else {
		fromtime(*v, NULL, NULL, &sec, &milli);
		*ret = sec * 1000 + milli;
	}
	return MAL_SUCCEED;
}

/* extracts milliseconds from daytime (value between 0 and 999) */
str
MTIMEdaytime_extract_milliseconds(int *ret, const daytime *v)
{
	if (daytime_isnil(*v)) {
		*ret = int_nil;
	} else {
		fromtime(*v, NULL, NULL, NULL, ret);
	}
	return MAL_SUCCEED;
}

/* extracts daytime from timestamp */
str
MTIMEtimestamp_extract_daytime(daytime *ret, const timestamp *t, const tzone *z)
{
	if (ts_isnil(*t) || tz_isnil(*z)) {
		*ret = daytime_nil;
	} else {
		timestamp tmp;

		if (timestamp_inside(&tmp, t, z, (lng) 0)) {
			lng add = (lng) 3600000;

			MTIMEtimestamp_add(&tmp, &tmp, &add);
		}
		if (ts_isnil(tmp)) {
			*ret = daytime_nil;
		} else {
			*ret = tmp.msecs;
		}
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_extract_daytime_default(daytime *ret, const timestamp *t)
{
	return MTIMEtimestamp_extract_daytime(ret, t, &tzone_local);
}

str
MTIMEtimestamp_extract_daytime_default_bulk(bat *ret, bat *bid)
{
	BAT *b = BATdescriptor(*bid);
	BAT *bn;
	const timestamp *t;
	daytime *dt;
	BUN n;
	timestamp tmp;
	lng add = (lng) 3600000;	/* one hour */

	if (b == NULL)
		throw(MAL, "batcalc.daytime", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bn = COLnew(b->hseqbase, TYPE_daytime, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.daytime", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = (const timestamp *) Tloc(b, 0);
	dt = (daytime *) Tloc(bn, 0);
	bn->tnil = 0;
	for (n = BATcount(b); n > 0; n--, t++, dt++) {
		if (ts_isnil(*t)) {
			*dt = daytime_nil;
			bn->tnil = 1;
		} else {
			if (timestamp_inside(&tmp, t, &tzone_local, (lng) 0))
				MTIMEtimestamp_add(&tmp, &tmp, &add);
			if (ts_isnil(tmp)) {
				*dt = daytime_nil;
				bn->tnil = 1;
			} else {
				*dt = tmp.msecs;
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

/* extracts date from timestamp */
str
MTIMEtimestamp_extract_date(date *ret, const timestamp *t, const tzone *z)
{
	if (ts_isnil(*t) || tz_isnil(*z)) {
		*ret = date_nil;
	} else {
		timestamp tmp;

		if (timestamp_inside(&tmp, t, z, (lng) 0)) {
			lng add = (lng) 3600000;

			MTIMEtimestamp_add(&tmp, &tmp, &add);
		}
		if (ts_isnil(tmp)) {
			*ret = date_nil;
		} else {
			*ret = tmp.days;
		}
	}
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_extract_date_default(date *ret, const timestamp *t)
{
	return MTIMEtimestamp_extract_date(ret, t, &tzone_local);
}

str
MTIMEtimestamp_extract_date_default_bulk(bat *ret, bat *bid)
{
	BAT *b = BATdescriptor(*bid);
	BAT *bn;
	const timestamp *t;
	date *d;
	BUN n;
	timestamp tmp;
	lng add = (lng) 3600000;	/* one hour */

	if (b == NULL)
		throw(MAL, "batcalc.date", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	bn = COLnew(b->hseqbase, TYPE_date, BATcount(b), TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.date", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	t = (const timestamp *) Tloc(b, 0);
	d = (date *) Tloc(bn, 0);
	bn->tnil = 0;
	for (n = BATcount(b); n > 0; n--, t++, d++) {
		if (ts_isnil(*t)) {
			*d = date_nil;
			bn->tnil = 1;
		} else {
			if (timestamp_inside(&tmp, t, &tzone_local, (lng) 0))
				MTIMEtimestamp_add(&tmp, &tmp, &add);
			if (ts_isnil(tmp)) {
				*d = date_nil;
				bn->tnil = 1;
			} else {
				*d = tmp.days;
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

/* returns the date that comes a number of years after 'v' (or before
 * iff *delta < 0). */
str
MTIMEdate_addyears(date *ret, const date *v, const int *delta)
{
	if (date_isnil(*v) || is_int_nil(*delta)) {
		*ret = date_nil;
	} else {
		int d, m, y, x, z = *delta;

		fromdate(*v, &d, &m, &y);
		if (m >= 3) {
			y++;
		}
		*ret = *v;
		while (z > 0) {
			x = YEARDAYS(y);
			MTIMEdate_adddays(ret, ret, &x);
			z--;
			y++;
		}
		while (z < 0) {
			z++;
			y--;
			x = -YEARDAYS(y);
			MTIMEdate_adddays(ret, ret, &x);
		}
	}
	return MAL_SUCCEED;
}

/* returns the date that comes a number of day after 'v' (or before
 * iff *delta < 0). */
str
MTIMEdate_adddays(date *ret, const date *v, const int *delta)
{
	lng min = DATE_MIN, max = DATE_MAX;
	lng cur = (lng) *v, inc = *delta;

	if (is_int_nil(cur) || is_int_nil(inc) || (inc > 0 && (max - cur) < inc) || (inc < 0 && (min - cur) > inc)) {
		*ret = date_nil;
	} else {
		*ret = *v + *delta;
	}
	return MAL_SUCCEED;
}

/* returns the date that comes a number of months after 'v' (or before
 * if *delta < 0). */
str
MTIMEdate_addmonths(date *ret, const date *v, const int *delta)
{
	if (date_isnil(*v) || is_int_nil(*delta)) {
		*ret = date_nil;
	} else {
		int d, m, y, x, z = *delta;

		fromdate(*v, &d, &m, &y);
		*ret = *v;
		while (z > 0) {
			z--;
			x = MONTHDAYS(m, y);
			if (++m == 13) {
				m = 1;
				y++;
			}
			MTIMEdate_adddays(ret, ret, &x);
		}
		while (z < 0) {
			z++;
			if (--m == 0) {
				m = 12;
				y--;
			}
			x = -MONTHDAYS(m, y);
			MTIMEdate_adddays(ret, ret, &x);
		}
	}
	return MAL_SUCCEED;
}

str
MTIMEdate_submonths(date *ret, const date *v, const int *delta)
{
	int mindelta = -(*delta);
	return MTIMEdate_addmonths(ret, v, &mindelta);
}

/* returns the number of days between 'val1' and 'val2'. */
str
MTIMEdate_diff(int *ret, const date *v1, const date *v2)
{
	if (date_isnil(*v1) || date_isnil(*v2)) {
		*ret = int_nil;
	} else {
		*ret = (int) (*v1 - *v2);
	}
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
	bn->tnonil = 1;
	bn->tnil = 0;
	for (i = 0; i < n; i++) {
		if (date_isnil(*t1) || date_isnil(*t2)) {
			*tn = int_nil;
			bn->tnonil = 0;
			bn->tnil = 1;
		} else {
			*tn = (int) (*t1 - *t2);
		}
		t1++;
		t2++;
		tn++;
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
	if (daytime_isnil(*v1) || daytime_isnil(*v2)) {
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
	if (ts_isnil(*v1) || ts_isnil(*v2)) {
		*ret = lng_nil;
	} else {
		*ret = ((lng) (v1->days - v2->days)) * ((lng) 24 * 60 * 60 * 1000) + ((lng) (v1->msecs - v2->msecs));
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
	bn->tnonil = 1;
	bn->tnil = 0;
	for (i = 0; i < n; i++) {
		if (ts_isnil(*t1) || ts_isnil(*t2)) {
			*tn = lng_nil;
			bn->tnonil = 0;
			bn->tnil = 1;
		} else {
			*tn = ((lng) (t1->days - t2->days)) * ((lng) 24 * 60 * 60 * 1000) + ((lng) (t1->msecs - t2->msecs));
		}
		t1++;
		t2++;
		tn++;
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

/* return whether DST holds in the tzone at a certain point of time. */
str
MTIMEtimestamp_inside_dst(bit *ret, const timestamp *p, const tzone *z)
{
	*ret = FALSE;

	if (tz_isnil(*z)) {
		*ret = bit_nil;
	} else if (z->dst) {
		timestamp tmp;

		if (timestamp_inside(&tmp, p, z, (lng) 0)) {
			*ret = TRUE;
		}
	}
	return MAL_SUCCEED;
}

str
MTIMErule_fromstr(rule *ret, const char * const *s)
{
	size_t len = sizeof(rule);

	if (strcmp(*s, "nil") == 0) {
		ret->asint = int_nil;
		return MAL_SUCCEED;
	}
	if (rule_fromstr(*s, &len, &ret) < 0)
		throw(MAL, "mtime.rule", GDK_EXCEPTION);
	return MAL_SUCCEED;
}

/* create a DST start/end date rule. */
str
MTIMErule_create(rule *ret, const int *month, const int *day, const int *weekday, const int *minutes)
{
	ret->asint = int_nil;
	if (!is_int_nil(*month) && *month >= 1 && *month <= 12 &&
		!is_int_nil(*weekday) && abs(*weekday) <= 7 &&
		!is_int_nil(*minutes) && *minutes >= 0 && *minutes < 24 * 60 &&
		!is_int_nil(*day) && abs(*day) >= 1 && abs(*day) <= LEAPDAYS[*month] &&
		(*weekday || *day > 0)) {
		ret->s.month = *month;
		ret->s.day = DAY_ZERO + *day;
		ret->s.weekday = WEEKDAY_ZERO + *weekday;
		ret->s.minutes = *minutes;
	}
	return MAL_SUCCEED;
}

/* create a tzone as a simple hour difference from GMT. */
str
MTIMEtzone_create_dst(tzone *ret, const int *minutes, const rule *start, const rule *end)
{
	*ret = *tzone_nil;
	if (!is_int_nil(*minutes) && abs(*minutes) < 24 * 60 &&
		!is_int_nil(start->asint) && !is_int_nil(end->asint)) {
		set_offset(ret, *minutes);
		ret->dst = TRUE;
		ret->dst_start = get_rule(*start);
		ret->dst_end = get_rule(*end);
	}
	return MAL_SUCCEED;
}

/* create a tzone as an hour difference from GMT and a DST. */
str
MTIMEtzone_create(tzone *ret, const int *minutes)
{
	*ret = *tzone_nil;
	if (!is_int_nil(*minutes) && abs(*minutes) < 24 * 60) {
		set_offset(ret, *minutes);
		ret->dst = FALSE;
	}
	return MAL_SUCCEED;
}

str
MTIMEtzone_create_lng(tzone *ret, const lng *minutes)
{
	*ret = *tzone_nil;
	if (!is_lng_nil(*minutes) && *minutes < 24 * 60 && -*minutes < 24 * 60) {
		set_offset(ret, (int) *minutes);
		ret->dst = FALSE;
	}
	return MAL_SUCCEED;
}

/* extract month from rule. */
str
MTIMErule_extract_month(int *ret, const rule *r)
{
	*ret = (is_int_nil(r->asint)) ? int_nil : r->s.month;
	return MAL_SUCCEED;
}

/* extract day from rule. */
str
MTIMErule_extract_day(int *ret, const rule *r)
{
	*ret = (is_int_nil(r->asint)) ? int_nil : r->s.day - DAY_ZERO;
	return MAL_SUCCEED;
}

/* extract weekday from rule. */
str
MTIMErule_extract_weekday(int *ret, const rule *r)
{
	*ret = (is_int_nil(r->asint)) ? int_nil : r->s.weekday - WEEKDAY_ZERO;
	return MAL_SUCCEED;
}

/* extract minutes from rule. */
str
MTIMErule_extract_minutes(int *ret, const rule *r)
{
	*ret = (is_int_nil(r->asint)) ? int_nil : r->s.minutes;
	return MAL_SUCCEED;
}

/* extract rule that determines start of DST from tzone. */
str
MTIMEtzone_extract_start(rule *ret, const tzone *t)
{
	if (tz_isnil(*t) || !t->dst) {
		ret->asint = int_nil;
	} else {
		set_rule(*ret, t->dst_start);
	}
	return MAL_SUCCEED;
}

/* extract rule that determines end of DST from tzone. */
str
MTIMEtzone_extract_end(rule *ret, const tzone *t)
{
	if (tz_isnil(*t) || !t->dst) {
		ret->asint = int_nil;
	} else {
		set_rule(*ret, t->dst_end);
	}
	return MAL_SUCCEED;
}

/* extract number of minutes that tzone is offset wrt GMT. */
str
MTIMEtzone_extract_minutes(int *ret, const tzone *t)
{
	*ret = (tz_isnil(*t)) ? int_nil : get_offset(t);
	return MAL_SUCCEED;
}

str
MTIMEdate_sub_sec_interval_wrap(date *ret, const date *t, const int *sec)
{
	int delta;

	if (is_int_nil(*sec) || date_isnil(*t)) {
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

	if (is_lng_nil(*msec) || date_isnil(*t)) {
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

	if (is_int_nil(*sec) || date_isnil(*t)) {
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

	if (is_lng_nil(*msec) || date_isnil(*t)) {
		*ret = date_nil;
		return MAL_SUCCEED;
	}
	delta = (int) (*msec / 86400000);	/* / truncates toward zero */
	return MTIMEdate_adddays(ret, t, &delta);
}

str
MTIMEtimestamp_sub_msec_interval_lng_wrap(timestamp *ret, const timestamp *t, const lng *msec)
{
	lng Msec = -*msec;
	return MTIMEtimestamp_add(ret, t, &Msec);
}

str
MTIMEtimestamp_add_month_interval_wrap(timestamp *ret, const timestamp *v, const int *months)
{
	daytime t;
	date d;
	MTIMEtimestamp_extract_daytime(&t, v, &tzone_local);
	MTIMEtimestamp_extract_date(&d, v, &tzone_local);
	MTIMEdate_addmonths(&d, &d, months);
	return MTIMEtimestamp_create(ret, &d, &t, &tzone_local);
}

str
MTIMEtimestamp_add_month_interval_lng_wrap(timestamp *ret, const timestamp *v, const lng *months)
{
	daytime t;
	date d;
	int m;
	MTIMEtimestamp_extract_daytime(&t, v, &tzone_local);
	MTIMEtimestamp_extract_date(&d, v, &tzone_local);
	if (*months > (YEAR_MAX*12))
		throw(MAL, "mtime.timestamp_sub_interval", "to many months");
	m = (int)*months;
	MTIMEdate_addmonths(&d, &d, &m);
	return MTIMEtimestamp_create(ret, &d, &t, &tzone_local);
}

str
MTIMEtimestamp_sub_month_interval_wrap(timestamp *ret, const timestamp *v, const int *months)
{
	daytime t;
	date d;
	int m = -*months;
	MTIMEtimestamp_extract_daytime(&t, v, &tzone_local);
	MTIMEtimestamp_extract_date(&d, v, &tzone_local);
	MTIMEdate_addmonths(&d, &d, &m);
	return MTIMEtimestamp_create(ret, &d, &t, &tzone_local);
}

str
MTIMEtimestamp_sub_month_interval_lng_wrap(timestamp *ret, const timestamp *v, const lng *months)
{
	daytime t;
	date d;
	int m;
	MTIMEtimestamp_extract_daytime(&t, v, &tzone_local);
	MTIMEtimestamp_extract_date(&d, v, &tzone_local);
	if (*months > (YEAR_MAX*12))
		throw(MAL, "mtime.timestamp_sub_interval", "to many months");
	m = -(int)*months;
	MTIMEdate_addmonths(&d, &d, &m);
	return MTIMEtimestamp_create(ret, &d, &t, &tzone_local);
}


str
MTIMEtime_add_msec_interval_wrap(daytime *ret, const daytime *t, const lng *mseconds)
{
	lng s = *mseconds;
	return daytime_add(ret, t, &s);
}

str
MTIMEtime_sub_msec_interval_wrap(daytime *ret, const daytime *t, const lng *mseconds)
{
	lng s = -*mseconds;
	return daytime_add(ret, t, &s);
}

/* compute the date from a rule in a certain year. */
str
MTIMEcompute_rule_foryear(date *ret, const rule *val, const int *year)
{
	if (is_int_nil(*(int *) val) || *year < YEAR_MIN || *year > YEAR_MAX) {
		*ret = date_nil;
	} else {
		*ret = compute_rule(val, *year);
	}
	return MAL_SUCCEED;
}

str
MTIMEtzone_tostr(str *s, const tzone *ret)
{
	char *s1 = NULL;
	size_t len = 0;

	if (tzone_tostr(&s1, &len, ret) < 0) {
		GDKfree(s1);
		throw(MAL, "mtime,str", GDK_EXCEPTION);
	}
	*s = s1;
	return MAL_SUCCEED;
}

str
MTIMEtzone_fromstr(tzone *ret, const char * const *s)
{
	size_t len = sizeof(tzone);

	if (strcmp(*s, "nil") == 0) {
		*ret = *tzone_nil;
		return MAL_SUCCEED;
	}
	if (tzone_fromstr(*s, &len, &ret) < 0)
		throw(MAL, "mtime.timezone", GDK_EXCEPTION);
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
	if (daytime_fromstr(*s, &len, &ret) < 0)
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
	bn->tnil = 0;
	for (n = BATcount(b); n > 0; n--, s++, dt++) {
		if (is_lng_nil(*s) ||
			*s > GDK_int_max / 1000 ||
			*s < GDK_int_min / 1000) {
			*dt = daytime_nil;
			bn->tnil = 1;
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
	date d0 = todate(1, 1, 1970);
	int zero = 0;
	const char *s = "GMT";
	daytime d1;
	tzone d2;
	str e;

	if ((e = MTIMEdaytime_create(&d1, &zero, &zero, &zero, &zero)) != MAL_SUCCEED)
		return e;
	if ((e = MTIMEtzone_fromstr(&d2, &s)) != MAL_SUCCEED)
		return e;
	return MTIMEtimestamp_create(ret, &d0, &d1, &d2);
}

str
MTIMEepoch2int(int *ret, const timestamp *t)
{
	timestamp e;
	lng v;
	str err;

	if ((err = MTIMEunix_epoch(&e)) != MAL_SUCCEED)
		return err;
	if ((err = MTIMEtimestamp_diff(&v, t, &e)) != MAL_SUCCEED)
		return err;
	if (is_lng_nil(v))
		*ret = int_nil;
	else if ((v/1000) > GDK_int_max || (v/1000) < GDK_int_min)
		throw(MAL, "mtime.epoch", "22003!epoch value too large");
	else
		*ret = (int) (v / 1000);
	return MAL_SUCCEED;
}

str
MTIMEepoch2lng(lng *ret, const timestamp *t)
{
	timestamp e;
	lng v;
	str err;

	if ((err = MTIMEunix_epoch(&e)) != MAL_SUCCEED)
		return err;
	if ((err = MTIMEtimestamp_diff(&v, t, &e)) != MAL_SUCCEED)
		return err;
	if (is_lng_nil(v))
		*ret = int_nil;
	else
		*ret = v;
	return MAL_SUCCEED;
}

str
MTIMEepoch_bulk(bat *ret, bat *bid)
{
	timestamp epoch;
	const timestamp *t;
	lng *tn;
	str msg = MAL_SUCCEED;
	BAT *b, *bn;
	BUN i, n;

	if ((msg = MTIMEunix_epoch(&epoch)) != MAL_SUCCEED)
		return msg;
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
	bn->tnonil = 1;
	b->tnil = 0;
	for (i = 0; i < n; i++) {
		if (ts_isnil(*t)) {
			*tn = lng_nil;
			bn->tnonil = 0;
			bn->tnil = 1;
		} else {
			*tn = ((lng) (t->days - epoch.days)) * ((lng) 24 * 60 * 60 * 1000) + ((lng) (t->msecs - epoch.msecs));
		}
		t++;
		tn++;
	}
	BBPunfix(b->batCacheid);
	BATsetcount(bn, (BUN) (tn - (lng *) Tloc(bn, 0)));
	bn->tsorted = BATcount(bn) <= 1;
	bn->trevsorted = BATcount(bn) <= 1;
	BBPkeepref(bn->batCacheid);
	*ret = bn->batCacheid;
	return msg;
}

str
MTIMEtimestamp(timestamp *ret, const int *sec)
{
	timestamp t;
	lng l;
	str e;

	if (is_int_nil(*sec)) {
		*ret = *timestamp_nil;
		return MAL_SUCCEED;
	}
	if ((e = MTIMEunix_epoch(&t)) != MAL_SUCCEED)
		return e;
	l = ((lng) *sec) * 1000;
	return MTIMEtimestamp_add(ret, &t, &l);
}

str
MTIMEtimestamplng(timestamp *ret, const lng *sec)
{
	timestamp t;
	lng l;
	str e;

	if (is_lng_nil(*sec)) {
		*ret = *timestamp_nil;
		return MAL_SUCCEED;
	}
	if ((e = MTIMEunix_epoch(&t)) != MAL_SUCCEED)
		return e;
	l = ((lng) *sec);
	return MTIMEtimestamp_add(ret, &t, &l);
}

str
MTIMEtimestamp_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	timestamp *t;
	timestamp e;
	const int *s;
	lng ms;
	BUN n;
	str msg;

	if ((msg = MTIMEunix_epoch(&e)) != MAL_SUCCEED)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	s = (const int *) Tloc(b, 0);
	t = (timestamp *) Tloc(bn, 0);
	bn->tnil = 0;
	for (n = BATcount(b); n > 0; n--, t++, s++) {
		if (is_int_nil(*s)) {
			*t = *timestamp_nil;
			bn->tnil = 1;
		} else {
			ms = ((lng)*s) * 1000;
			if ((msg = MTIMEtimestamp_add(t, &e, &ms)) != MAL_SUCCEED) {
				BBPreclaim(bn);
				BBPunfix(b->batCacheid);
				return msg;
			}
			if (ts_isnil(*t))
				bn->tnil = 1;
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
	timestamp t;
	lng l = *msec;
	str e;

	if ((e = MTIMEunix_epoch(&t)) != MAL_SUCCEED)
		return e;
	return MTIMEtimestamp_add(ret, &t, &l);
}

str
MTIMEtimestamp_lng_bulk(bat *ret, bat *bid)
{
	BAT *b, *bn;
	timestamp *t;
	timestamp e;
	const lng *ms;
	BUN n;
	str msg;

	if ((msg = MTIMEunix_epoch(&e)) != MAL_SUCCEED)
		return msg;
	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY002) RUNTIME_OBJECT_MISSING);
	if ((bn = COLnew(b->hseqbase, TYPE_timestamp, BATcount(b), TRANSIENT)) == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batcalc.timestamp", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	ms = (const lng *) Tloc(b, 0);
	t = (timestamp *) Tloc(bn, 0);
	bn->tnil = 0;
	for (n = BATcount(b); n > 0; n--, t++, ms++) {
		if (is_lng_nil(*ms)) {
			*t = *timestamp_nil;
			bn->tnil = 1;
		} else {
			if ((msg = MTIMEtimestamp_add(t, &e, ms)) != MAL_SUCCEED) {
				BBPreclaim(bn);
				BBPunfix(b->batCacheid);
				return msg;
			}
			if (ts_isnil(*t))
				bn->tnil = 1;
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
MTIMEruleDef0(rule *ret, const int *m, const int *d, const int *w, const int *h, const int *mint)
{
	int d0 = 60 * *h;
	int d1 = d0 + *mint;

	return MTIMErule_create(ret, m, d, w, &d1);
}

str
MTIMEruleDef1(rule *ret, const int *m, const char * const *dnme, const int *w, const int *h, const int *mint)
{
	int d;
	int d0 = 60 * *h;
	int d1 = d0 + *mint;
	str e;

	if ((e = MTIMEday_from_str(&d, dnme)) != MAL_SUCCEED)
		return e;
	return MTIMErule_create(ret, m, &d, w, &d1);
}

str
MTIMEruleDef2(rule *ret, const int *m, const char * const *dnme, const int *w, const int *mint)
{
	int d;
	str e;

	if ((e = MTIMEday_from_str(&d, dnme)) != MAL_SUCCEED)
		return e;
	return MTIMErule_create(ret, m, &d, w, mint);
}

str
MTIMEcurrent_timestamp(timestamp *ret)
{
	timestamp ts;
	lng t = ((lng) time(0)) * 1000;
	str e;

	/* convert number of seconds into a timestamp */
	if ((e = MTIMEunix_epoch(&ts)) == MAL_SUCCEED)
		e = MTIMEtimestamp_add(ret, &ts, &t);
	return e;
}

str
MTIMEcurrent_date(date *d)
{
	timestamp stamp;
	str e;

	if ((e = MTIMEcurrent_timestamp(&stamp)) != MAL_SUCCEED)
		return e;
	return MTIMEtimestamp_extract_date_default(d, &stamp);
}

str
MTIMEcurrent_time(daytime *t)
{
	timestamp stamp;
	str e;

	if ((e = MTIMEcurrent_timestamp(&stamp)) != MAL_SUCCEED)
		return e;
	return MTIMEtimestamp_extract_daytime_default(t, &stamp);
}

/* more SQL extraction utilities */
str
MTIMEtimestamp_year(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_year(ret, &d);
}

str
MTIMEtimestamp_quarter(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_quarter(ret, &d);
}

str
MTIMEtimestamp_month(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_month(ret, &d);
}


str
MTIMEtimestamp_day(int *ret, const timestamp *t)
{
	date d;
	str e;

	if ((e = MTIMEtimestamp_extract_date(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdate_extract_day(ret, &d);
}

str
MTIMEtimestamp_hours(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_hours(ret, &d);
}

str
MTIMEtimestamp_minutes(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_minutes(ret, &d);
}

str
MTIMEtimestamp_seconds(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_seconds(ret, &d);
}

str
MTIMEtimestamp_sql_seconds(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t, &tzone_local)) != MAL_SUCCEED)
		return e;
	return MTIMEdaytime_extract_sql_seconds(ret, &d);
}

str
MTIMEtimestamp_milliseconds(int *ret, const timestamp *t)
{
	daytime d;
	str e;

	if ((e = MTIMEtimestamp_extract_daytime(&d, t, &tzone_local)) != MAL_SUCCEED)
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
#ifdef HAVE_GETTIMEOFDAY
	struct timeval tp;

	gettimeofday(&tp, NULL);
	*r = ((lng) (tp.tv_sec)) * LL_CONSTANT(1000) + (lng) tp.tv_usec / LL_CONSTANT(1000);
#else
#ifdef HAVE_FTIME
	struct timeb tb;

	ftime(&tb);
	*r = ((lng) (tb.time)) * LL_CONSTANT(1000) + ((lng) tb.millitm);
#endif
#endif
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
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	y = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*y = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdate_extract_year(y, t);
			if (is_int_nil(*y)) {
				bn->tnil = 1;
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
		throw(MAL, "batmtime.quarter", "Cannot access descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.quarter", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	q = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*q = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdate_extract_quarter(q, t);
			if (is_int_nil(*q)) {
				bn->tnil = 1;
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
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	m = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*m = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdate_extract_month(m, t);
			if (is_int_nil(*m)) {
				bn->tnil = 1;
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
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	d = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*d = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdate_extract_day(d, t);
			if (is_int_nil(*d)) {
				bn->tnil = 1;
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
	const date *t;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.hourse", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.hours", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	h = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*h = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdaytime_extract_hours(h, t);
			if (is_int_nil(*h)) {
				bn->tnil = 1;
			}
		}
		h++;
		t++;
	}
	BATsetcount(bn, (BUN) (h - (int *) Tloc(bn, 0)));

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
	const date *t;
	int *m;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.minutes", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.minutes", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	m = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*m = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdaytime_extract_minutes(m, t);
			if (is_int_nil(*m)) {
				bn->tnil = 1;
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
MTIMEdaytime_extract_seconds_bulk(bat *ret, const bat *bid)
{
	BAT *b, *bn;
	BUN i, n;
	const date *t;
	int *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.seconds", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.seconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	s = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*s = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdaytime_extract_seconds(s, t);
			if (is_int_nil(*s)) {
				bn->tnil = 1;
			}
		}
		s++;
		t++;
	}
	BATsetcount(bn, (BUN) (s - (int *) Tloc(bn, 0)));

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
	const date *t;
	int *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.sql_seconds", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.sql_seconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	s = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*s = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdaytime_extract_sql_seconds(s, t);
			if (is_int_nil(*s)) {
				bn->tnil = 1;
			}
		}
		s++;
		t++;
	}

	BATsetcount(bn, (BUN) (s - (int *) Tloc(bn, 0)));

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
	const date *t;
	int *s;

	if ((b = BATdescriptor(*bid)) == NULL)
		throw(MAL, "batmtime.milliseconds", SQLSTATE(HY005) "Cannot access column descriptor");
	n = BATcount(b);

	bn = COLnew(b->hseqbase, TYPE_int, n, TRANSIENT);
	if (bn == NULL) {
		BBPunfix(b->batCacheid);
		throw(MAL, "batmtime.milliseconds", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	}
	bn->tnonil = 1;
	bn->tnil = 0;

	t = (const date *) Tloc(b, 0);
	s = (int *) Tloc(bn, 0);
	for (i = 0; i < n; i++) {
		if (date_isnil(*t)) {
			*s = int_nil;
			bn->tnil = 1;
		} else {
			MTIMEdaytime_extract_milliseconds(s, t);
			if (is_int_nil(*s)) {
				bn->tnil = 1;
			}
		}
		s++;
		t++;
	}
	BATsetcount(bn, (BUN) (s - (int *) Tloc(bn, 0)));

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
	*d = todate(t.tm_mday, t.tm_mon + 1, t.tm_year + 1900);
	return MAL_SUCCEED;
}

str
MTIMEdate_to_str(str *s, const date *d, const char * const *format)
{
	struct tm t;
	char buf[512];
	size_t sz;
	int mon, year;

	if (date_isnil(*d) || strcmp(*format, str_nil) == 0) {
		*s = GDKstrdup(str_nil);
		if (*s == NULL)
			throw(MAL, "mtime.date_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	fromdate(*d, &t.tm_mday, &mon, &year);
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
	int msec;

	if (daytime_isnil(*d) || strcmp(*format, str_nil) == 0) {
		*s = GDKstrdup(str_nil);
		if (*s == NULL)
			throw(MAL, "mtime.time_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	fromtime(*d, &t.tm_hour, &t.tm_min, &t.tm_sec, &msec);
	(void)msec;
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
		*ts = *timestamp_nil;
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	if (strptime(*s, *format, &t) == NULL)
		throw(MAL, "mtime.str_to_timestamp", "format '%s', doesn't match timestamp '%s'\n", *format, *s);
	ts->days = todate(t.tm_mday, t.tm_mon + 1, t.tm_year + 1900);
	ts->msecs = totime(t.tm_hour, t.tm_min, t.tm_sec, 0);
	return MAL_SUCCEED;
}

str
MTIMEtimestamp_to_str(str *s, const timestamp *ts, const char * const *format)
{
	struct tm t;
	char buf[512];
	size_t sz;
	int mon, year, msec;

	if (timestamp_isnil(*ts) || strcmp(*format, str_nil) == 0) {
		*s = GDKstrdup(str_nil);
		if (*s == NULL)
			throw(MAL, "mtime.timestamp_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
		return MAL_SUCCEED;
	}
	t = (struct tm) {0};
	fromdate(ts->days, &t.tm_mday, &mon, &year);
	t.tm_mon = mon - 1;
	t.tm_year = year - 1900;
	fromtime(ts->msecs, &t.tm_hour, &t.tm_min, &t.tm_sec, &msec);
	t.tm_isdst = -1;
	(void)mktime(&t); /* corrects the tm_wday etc */
	(void)msec;
	if ((sz = strftime(buf, sizeof(buf), *format, &t)) == 0)
		throw(MAL, "mtime.timestamp_to_str", "failed to convert timestampt to string using format '%s'\n", *format);
	*s = GDKmalloc(sz + 1);
	if (*s == NULL)
		throw(MAL, "mtime.timestamp_to_str", SQLSTATE(HY001) MAL_MALLOC_FAIL);
	strncpy(*s, buf, sz + 1);
	return MAL_SUCCEED;
}


date MTIMEtodate(int day, int month, int year) {
	return todate(day, month, year);
}

void MTIMEfromdate(date n, int *d, int *m, int *y) {
	fromdate(n, d, m, y);
}

daytime MTIMEtotime(int hour, int min, int sec, int msec) {
	return totime(hour, min, sec, msec);
}

void MTIMEfromtime(daytime n, int *hour, int *min, int *sec, int *msec) {
	fromtime(n, hour, min, sec, msec);
}
