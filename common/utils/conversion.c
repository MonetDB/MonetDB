
#include "conversion.h"
#include <string.h>

typedef signed char bit;
typedef signed char bte;
typedef short sht;
typedef size_t oid;
typedef float flt;
typedef double dbl;
typedef char *str;
typedef void *ptr;

#if SIZEOF_INT==8
#	define LL_CONSTANT(val)	(val)
#elif SIZEOF_LONG==8
#	define LL_CONSTANT(val)	(val##L)
#elif defined(HAVE_LONG_LONG)
#	define LL_CONSTANT(val)	(val##LL)
#elif defined(HAVE___INT64)
#	define LL_CONSTANT(val)	(val##i64)
#endif

#include <limits.h>		/* for *_MIN and *_MAX */
#include <float.h>		/* for FLT_MAX and DBL_MAX */
#ifndef LLONG_MAX
#ifdef LONGLONG_MAX
#define LLONG_MAX LONGLONG_MAX
#define LLONG_MIN LONGLONG_MIN
#else
#define LLONG_MAX LL_CONSTANT(9223372036854775807)
#define LLONG_MIN (-LL_CONSTANT(9223372036854775807) - LL_CONSTANT(1))
#endif
#endif

#define NULL_STRING "nil"

int
conversion_bit_to_string(char *dst, int len, const signed char *src, signed char null_value)
{
	if (len < 6) return -1;

	if (*src == null_value)
		return snprintf(dst, len, NULL_STRING);
	if (*src)
		return snprintf(dst, len, "true");
	return snprintf(dst, len, "false");
}


#define atomtostr(TYPE, FMT, FMTCAST)			\
int	                         \
conversion_##TYPE##_to_string(char *dst, int len, const TYPE *src, TYPE nullvalue)	\
{							\
	if (len < TYPE##Strlen) return -1;			\
	if (*src == nullvalue) {			\
		return snprintf(dst, len, NULL_STRING);	\
	}						\
	return snprintf(dst, len, FMT, FMTCAST *src);	\
}

atomtostr(bte, "%hhd", )
atomtostr(sht, "%hd", )
atomtostr(int, "%d", )
atomtostr(lng, LLFMT, )
atomtostr(ptr, PTRFMT, PTRFMTCAST)

int conversion_dbl_to_string(char *dst, int len, const double *src, double null_value) {
	int i;

	if (len < dblStrlen) return -1;
	if (*src == null_value) {
		return snprintf(dst, len, NULL_STRING);
	}
	for (i = 4; i < 18; i++) {
		snprintf(dst, len, "%.*g", i, *src);
		if (strtod(dst, NULL) == *src)
			break;
	}
	return (int) strlen(dst);
}

int
conversion_flt_to_string(char *dst, int len, const float *src, float null_value) {
	int i;

	if (len < fltStrlen) return -1;
	if (*src == null_value) {
		return snprintf(dst, len, NULL_STRING);
	}
	for (i = 4; i < 10; i++) {
		snprintf(dst, len, "%.*g", i, *src);
#ifdef HAVE_STRTOF
		if (strtof(dst, NULL) == *src)
			break;
#else
		if ((float) strtod(dst, NULL) == *src)
			break;
#endif
	}
	return (int) strlen(dst);
}

#ifdef HAVE_HGE
#ifdef WIN32
#define HGE_LL018FMT "%018I64d"
#else
#define HGE_LL018FMT "%018lld"
#endif
#define HGE_LL18DIGITS LL_CONSTANT(1000000000000000000)
#define HGE_ABS(a) (((a) < 0) ? -(a) : (a))
int
conversion_hge_to_string(char *dst, int len, const hge *src, hge null_value)
{
	if (len < hgeStrlen) return -1;
	if (*src == null_value) {
		strncpy(dst, NULL_STRING, len);
		return 3;
	}
	if ((hge) LLONG_MIN < *src && *src <= (hge) LLONG_MAX) {
		lng s = (lng) *src;
		return conversion_lng_to_string(dst, len, &s, LLONG_MIN);
	} else {
		hge s = *src / HGE_LL18DIGITS;
		int l = conversion_hge_to_string(dst, len, &s, null_value);
		snprintf(dst + l, len - l, HGE_LL018FMT, (lng) HGE_ABS(*src % HGE_LL18DIGITS));
		return (int) strlen(dst);
	}
}
#endif


#define DEC_TOSTR(TYPE,TYPESTRLEN)							\
	do {								\
		char buf[64];						\
		TYPE v = *(const TYPE *) value;				\
		int cur = 63, i, done = 0;				\
		int neg = v < 0;					\
		int l;							\
		if (buflen < TYPESTRLEN) return -1; \
		if (v == *((TYPE*)null_value)) {					\
			strcpy(buffer, "NULL");				\
			return 4;					\
		}							\
		if (v<0)						\
			v = -v;						\
		buf[cur--] = 0;						\
		if (scale){						\
			for (i=0; i<scale; i++) {			\
				buf[cur--] = (char) (v%10 + '0');	\
				v /= 10;				\
			}						\
			buf[cur--] = '.';				\
		}							\
		while (v) {						\
			buf[cur--] = (char ) (v%10 + '0');		\
			v /= 10;					\
			done = 1;					\
		}							\
		if (!done)						\
			buf[cur--] = '0';				\
		if (neg)						\
			buf[cur--] = '-';				\
		l = (64-cur-1);						\
		strcpy(buffer, buf+cur+1);				\
		return l - 1;						\
	} while (0)

int 
conversion_decimal_to_string(const void *value, char *buffer, int buflen, int scale, int typelen, const void *null_value) {
	/* support dec map to bte, sht, int and lng */
	if (typelen == 1) {
		DEC_TOSTR(bte, bteStrlen + 1);
	} else if (typelen == 2) {
		DEC_TOSTR(sht, shtStrlen + 1);
	} else if (typelen == 4) {
		DEC_TOSTR(int, intStrlen + 1);
	} else if (typelen == 8) {
		DEC_TOSTR(lng, lngStrlen + 1);
#ifdef HAVE_HGE
	} else if (typelen == 16) {
		DEC_TOSTR(hge, hgeStrlen + 1);
#endif
	} else {
		return -1;
		//GDKerror("Decimal cannot be mapped to %s\n", ATOMname(type));
	}
	return 0;
}

// date and daytime
// part of the code here is copied from `mtime.c`

#define leapyear(y)		((y) % 4 == 0 && ((y) % 100 != 0 || (y) % 400 == 0))
#define YEARDAYS(y)		(leapyear(y) ? 366 : 365)

static int CUMDAYS[13] = {
	0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365
};
static int CUMLEAPDAYS[13] = {
	0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366
};

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

int
conversion_date_to_string(char *dst, int len, const int *src, int null_value) {
	int day, month, year;
	if (len < dateStrlen) return -1;
	if (*src == null_value) {
		strcpy(dst, NULL_STRING);
		return 3;
	}

	year = *src / 365;
	day = (*src - year * 365) - leapyears(year >= 0 ? year - 1 : year);
	if (*src < 0) {
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

	day++;
	if (leapyear(year)) {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMLEAPDAYS[month - 1] && day <= CUMLEAPDAYS[month]) {
				break;
			}
		day -= CUMLEAPDAYS[month - 1];
	} else {
		for (month = day / 31 == 0 ? 1 : day / 31; month <= 12; month++)
			if (day > CUMDAYS[month - 1] && day <= CUMDAYS[month]) {
				break;
			}
		day -= CUMDAYS[month - 1];
	}
	year = (year <= 0) ? year - 1 : year; // hide year 0
	// YYYY-MM-DD
	sprintf(dst, "%d-%02d-%02d", year, month, day);
	return (int) strlen(dst);
}

int 
conversion_time_to_string(char *dst, int len, const int *src, int null_value, int digits, int timezone_diff) {
	int sec, min, hour, ms;
	int time = *src;
	int mtime = 24 * 60 * 60 * 1000;
	int res = 0;
	if (len < daytimeStrlen) return -1;
	if (*src == null_value) {
		strcpy(dst, NULL_STRING);
		return 3;
	}
	// account for the timezone of the client
	time += timezone_diff;
	// time has to be between 00:00 and 24:00
	if (time < 0)
		time = mtime + time;
	if (time > mtime)
		time = time - mtime;

	hour = time / 3600000;
	time -= hour * 3600000;
	min = time / 60000;
	time -= min * 60000;
	sec = time / 1000;
	time -= sec * 1000;
	ms = time;
	if ((res = sprintf(dst, "%02d:%02d:%02d.%03d000", hour, min, sec, ms)) < 0) {
		return res;
	}
	digits--;
	if (digits == 0) digits = -1;
	// adjust displayed precision based on the digits
	dst[9 + digits] = '\0';
	return 9 + digits;
}

static int days_between_zero_and_epoch = 719528;


static int
conversion_epoch_optional_tz_to_string(char *dst, int len, const lng *src, lng null_value, int include_timezone, int timezone_diff) {
	int ms, sec, min, hour;
	int days = 0;
	lng time = *src;
	int offset;

	if (*src == null_value) {
		strcpy(dst, NULL_STRING);
		return 3;
	}
	// account for the timezone of the client
	time += timezone_diff;

	ms = time % 1000 * 1000;
	time /= 1000;
	sec = time % 60;
	time /= 60;
	min = time % 60;
	time /= 60;
	hour = time % 24;
	time /= 24;
	// we know the amount of days since epoch, just add the days between 0000-01-01 and epoch 
	// then we can use our conversion_date_to_string function
	days = (int)(time + days_between_zero_and_epoch);

	offset = conversion_date_to_string(dst, len, &days, -2147483647);
	if (offset < 0) return -1;
	if (include_timezone) {
		int diff_hour, diff_min;
		int original_diff = timezone_diff;
		
		diff_hour = timezone_diff / 3600000;
		timezone_diff -= diff_hour * 3600000;
		diff_min = timezone_diff / 60000;
		return snprintf(dst + offset, len - offset, " %02d:%02d:%02d.%06d%s%02d:%02d", hour, min, sec, ms, original_diff >= 0 ? "+" : "", diff_hour, diff_min);
	}
	return snprintf(dst + offset, len - offset, " %02d:%02d:%02d.%06d", hour, min, sec, ms);
}

int
conversion_epoch_to_string(char *dst, int len, const lng *src, lng null_value) {
	return conversion_epoch_optional_tz_to_string(dst, len, src, null_value, 0, 0);
}

int 
conversion_epoch_tz_to_string(char *dst, int len, const lng *src, lng null_value, int timezone_diff) {
	return conversion_epoch_optional_tz_to_string(dst, len, src, null_value, 1, timezone_diff);
}

static char hexit[] = "0123456789ABCDEF";

int
conversion_blob_to_string(char *dst, int len, const char *blobdata, size_t nitems)
{
	char *s;
	size_t i;
	int expectedlen;

	if (nitems == ~(size_t) 0)
		expectedlen = 4;
	else
		expectedlen = 24 + (nitems * 3);

	if (len < expectedlen) return -1;

	if (nitems == ~(size_t) 0) {
		strcpy(dst, NULL_STRING);
		return 3;
	}

	strcpy(dst, "\0");
	s = dst;

	for (i = 0; i < nitems; i++) {
		int val = (blobdata[i] >> 4) & 15;

		*s++ = hexit[val];
		val = blobdata[i] & 15;
		*s++ = hexit[val];
	}
	*s = '\0';
	return (int) (s - dst); /* 64bit: check for overflow */
}

