/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_datetime.h"
#include "sql_string.h"

int
parse_interval_qualifier(mvc *sql, struct dlist *pers, int *sk, int *ek, int *sp, int *ep)
{
	*sk = iyear;
	*ek = isec;

	if (pers) {
		dlist *s = pers->h->data.lval;

		assert(s->h->type == type_int);
		*ek = *sk = s->h->data.i_val;
		*ep = *sp = s->h->next->data.i_val;

		if (dlist_length(pers) == 2) {
			dlist *e = pers->h->next->data.lval;

			assert(e->h->type == type_int);
			*ek = e->h->data.i_val;
			*ep = e->h->next->data.i_val;
		}
	}
	if (*sk > *ek) {
		snprintf(sql->errstr, ERRSIZE, _("End interval field is larger than the start field\n"));
		return -1;
	}
	if ((*sk == iyear || *sk == imonth) && *ek > imonth) {
		snprintf(sql->errstr, ERRSIZE, _("Correct interval ranges are year-month or day-seconds\n"));
		return -1;
	}
	if (*sk == iyear || *sk == imonth)
		return 0;
	return 1;
}

lng
qualifier2multiplier( int sk )
{
	lng mul = 1;

	switch (sk) {
	case iyear:
		mul *= 12;
		/* fall through */
	case imonth:
		break;
	case iday:
		mul *= 24;
		/* fall through */
	case ihour:
		mul *= 60;
		/* fall through */
	case imin:
		mul *= 60000;
		/* fall through */
	case isec:
		break;
	default:
		return -1;
	}
	return mul;
}

static int
parse_interval_(mvc *sql, lng sign, const char *str, int sk, int ek, int sp, int ep, lng *i)
{
	char *n = NULL, sep = ':';
	lng val = 0, mul;
	int type;

	if (*str == '-') {
		sign *= -1;
		str++;
	}
	mul = sign;

	switch (sk) {
	case iyear:
		mul *= 12;
		/* fall through */
	case imonth:
		sep = '-';
		type = 0;
		break;
	case iday:
		mul *= 24;
		sep = ' ';
		/* fall through */
	case ihour:
		mul *= 60;
		/* fall through */
	case imin:
		mul *= 60000;
		/* fall through */
	case isec:
		type = 1;
		break;
	default:
		if (sql)
			snprintf(sql->errstr, ERRSIZE, _("Internal error: parse_interval: bad value for sk (%d)\n"), sk);
		return -1;
	}

	val = strtoll(str, &n, 10);
	if (!n)
		return -1;
	if (sk == isec) {
		lng msec = 0;
		val *= 1000;
		if (n && n[0] == '.') {
			char *nn;
			msec = strtol(n+1, &nn, 10);
			if (msec && nn) {
				ptrdiff_t d = nn-(n+1);
				for( ;d<3; d++)
					msec *= 10;
				for( ;d>3; d--)
					msec /= 10;
				n = nn;
			}
		}
		val += msec;
	}
	switch (sk) {
	case imonth:
		if (val >= 12) {
			snprintf(sql->errstr, ERRSIZE, _("Overflow detected in months (" LLFMT ")\n"), val);
			return -1;
		}
		break;
	case ihour:
		if (val >= 24) {
			snprintf(sql->errstr, ERRSIZE, _("Overflow detected in hours (" LLFMT ")\n"), val);
			return -1;
		}
		break;
	case imin:
		if (val >= 60) {
			snprintf(sql->errstr, ERRSIZE, _("Overflow detected in minutes (" LLFMT ")\n"), val);
			return -1;
		}
		break;
	case isec:
		if (val >= 60000) {
			snprintf(sql->errstr, ERRSIZE, _("Overflow detected in seconds (" LLFMT ")\n"), val);
			return -1;
		}
		break;
	}
	val *= mul;
	*i += val;
	if (ek != sk) {
		if (*n != sep) {
			if (sql)
				snprintf(sql->errstr, ERRSIZE, _("Interval field seperator \'%c\' missing\n"), sep);
			return -1;
		}
		return parse_interval_(sql, sign, n + 1, sk + 1, ek, sp, ep, i);
	} else {
		return type;
	}
}

#define MABS(a) (((a) < 0) ? -(a) : (a))

int
parse_interval(mvc *sql, lng sign, const char *str, int sk, int ek, int sp, int ep, lng *i)
{
	char *n = NULL, sep = ':';
	lng val = 0, mul, msec = 0;
	int type;

	if (*str == '-') {
		sign *= -1;
		str++;
	}
	mul = sign;

	switch (sk) {
	case iyear:
		mul *= 12;
		/* fall through */
	case imonth:
		sep = '-';
		type = 0;
		break;
	case iday:
		mul *= 24;
		sep = ' ';
		/* fall through */
	case ihour:
		mul *= 60;
		/* fall through */
	case imin:
		mul *= 60000;
		/* fall through */
	case isec:
		type = 1;
		break;
	default:
		if (sql)
			snprintf(sql->errstr, ERRSIZE, _("Internal error: parse_interval: bad value for sk (%d)\n"), sk);
		return -1;
	}

	val = strtoll(str, &n, 10);
	if (!n)
		return -1;
	if (sk == isec) {
		if (n && n[0] == '.') {
			char *nn;
			msec = strtol(n+1, &nn, 10);
			if (msec && nn) {
				ptrdiff_t d = nn-(n+1);
				for( ;d<3; d++)
					msec *= 10;
				for( ;d>3; d--)
					msec /= 10;
				n = nn;
			}
		}
	}
	switch (sk) {
	case iyear:
	case imonth:
		if (val > (lng) GDK_int_max / MABS(mul)) {
			if (sql)
				snprintf(sql->errstr, ERRSIZE, _("Overflow\n"));
			return -1;
		}
		break;
	case iday:
	case ihour:
	case imin:
		if (val > GDK_lng_max / MABS(mul)) {
			if (sql)
				snprintf(sql->errstr, ERRSIZE, _("Overflow\n"));
			return -1;
		}
		break;
	case isec:
		if (val > GDK_lng_max / 1000 / MABS(mul) || (val == GDK_lng_max / 1000 / MABS(mul) && msec > GDK_lng_max % 1000)) {
			if (sql)
				snprintf(sql->errstr, ERRSIZE, _("Overflow\n"));
			return -1;
		}
		val *= 1000;
		val += msec;
		break;
	default:
		assert(0);
	}
	val *= mul;
	*i += val;
	if (ek != sk) {
		if (*n != sep) {
			if (sql)
				snprintf(sql->errstr, ERRSIZE, _("Interval field seperator \'%c\' missing\n"), sep);
			return -1;
		}
		return parse_interval_(sql, sign, n + 1, sk + 1, ek, sp, ep, i);
	} else {
		if (!n || *n) {
			if (sql)
				snprintf(sql->errstr, ERRSIZE, _("Interval type miss match '%s'\n"), (!n)?"":n);
			return -1;
		}
		return type;
	}
}

int interval_from_str(const char *str, int d, int p, lng *val)
{
	int sk = digits2sk(d);
	int ek = digits2ek(d);
	*val = 0;
	return parse_interval(NULL, 1, str, sk, ek, p, p, val);
}

char *
datetime_field(itype f)
{
	switch (f) {
	default:
	case icentury:
		return "century";
	case idecade:
		return "decade";
	case iyear:
		return "year";
	case imonth:
		return "month";
	case iday:
		return "day";
	case ihour:
		return "hour";
	case imin:
		return "minute";
	case isec:
		return "second";
	case iquarter:
		return "quarter";
	case iweek:
		return "week";
	case idow:
		return "dayofweek";
	case idoy:
		return "dayofyear";
	case iepoch:
		return "epoch_ms";
	}
}

int inttype2digits( int sk, int ek )
{
	switch(sk) {
	case iyear:
		if(ek == iyear)
			return 1;
		return 2;
	case iquarter:
	case imonth:
		return 3;
	case iweek:
	case iday:
		switch(ek) {
		case iweek:
		case iday:
			return 4;
		case ihour:
			return 5;
		case imin:
			return 6;
		default:
			return 7;
		}
	case ihour:
		switch(ek) {
		case ihour:
			return 8;
		case imin:
			return 9;
		default:
			return 10;
		}
	case imin:
		if(ek == imin)
			return 11;
		return 12;
	case isec:
		return 13;
	}
	return 1;
}

int digits2sk( int digits)
{
	int sk = iyear;

	if (digits > 2)
		sk = imonth;
	if (digits > 3)
		sk = iday;
	if (digits > 7)
		sk = ihour;
	if (digits > 10)
		sk = imin;
	if (digits > 12)
		sk = isec;
	return sk;
}

int digits2ek( int digits)
{
	int ek = iyear;

	if (digits == 2 || digits == 3)
		ek = imonth;
	if (digits == 4)
		ek = iday;
	if (digits == 5 || digits == 8)
		ek = ihour;
	if (digits == 6 || digits == 9 || digits == 11)
		ek = imin;
	if (digits == 7 || digits == 10 || digits == 12 || digits == 13)
		ek = isec;
	return ek;
}


static int
parse_time(const char* val,
	   	unsigned int* hr,
	   	unsigned int* mn,
	   	unsigned int* sc,
	   	unsigned long* fr,
	   	unsigned int* pr)
{
	int n;
	const char* p = val;
	if (sscanf(p, "%u:%u:%u%n", hr, mn, sc, &n) >= 3) {
		p += n;
		if (*p == '.') {
			char* e;
			p++;
			*fr = strtoul(p, &e, 10);
			if (e > p)
				*pr = (unsigned int) (e - p);
		}
	}
	return -1;
}


static int
parse_timestamp(const char* val,
	   	unsigned int* yr,
	   	unsigned int* mt,
	   	unsigned int* dy,
	   	unsigned int* hr,
	   	unsigned int* mn,
	   	unsigned int* sc,
	   	unsigned long* fr,
	   	unsigned int* pr)
{
	int n;
	const char* p = val;
	if (sscanf(p, "%u-%u-%u %u:%u:%u%n",
			   	yr, mt, dy, hr, mn, sc, &n) >= 6) {
		p += n;
		if (*p == '.') {
			char* e;
			p++;
			*fr = strtoul(p, &e, 10);
			if (e > p)
				*pr = (unsigned int) (e - p);
		}
	}
	return -1;
}

unsigned int
get_time_precision(const char* val)
{
	unsigned int hr;
	unsigned int mn;
	unsigned int sc;
	unsigned long fr;
	unsigned int pr = 0;
	parse_time(val, &hr, &mn, &sc, &fr, &pr);
	return pr;
}

unsigned int
get_timestamp_precision(const char* val)
{
	unsigned int yr;
	unsigned int mt;
	unsigned int dy;
	unsigned int hr;
	unsigned int mn;
	unsigned int sc;
	unsigned long fr;
	unsigned int pr = 0;
	parse_timestamp(val, &yr, &mt, &dy, &hr, &mn, &sc, &fr, &pr);
	return pr;
}


int
process_odbc_interval(mvc *sql, itype interval, int val, sql_subtype *t, lng *i)
{
	assert(sql);
	lng mul = 1;
	int d = inttype2digits(interval, interval);
	switch (interval) {
		case iyear:
			mul *= 12;
			break;
		case iquarter:
			mul *= 3;
			break;
		case imonth:
			break;
		case iweek:
			mul *= 7;
			/* fall through */
		case iday:
			mul *= 24;
			/* fall through */
		case ihour:
			mul *= 60;
			/* fall through */
		case imin:
			mul *= 60;
			/* fall through */
		case isec:
			mul *= 1000;
			break;
		case insec:
			d = 5;
			break;
		default:
			snprintf(sql->errstr, ERRSIZE, _("Internal error: bad interval qualifier (%d)\n"), interval);
			return -1;
	}

	// check for overflow
	if (((lng) abs(val) * mul) > GDK_lng_max) {
		snprintf(sql->errstr, ERRSIZE, _("Overflow\n"));
		return -1;
	}
	// compute value month or sec interval
	*i += val * mul;

	int r = 0;
	if (d < 4){
		r = sql_find_subtype(t, "month_interval", d, 0);
	} else if (d == 4) {
		r = sql_find_subtype(t, "day_interval", d, 0);
	} else {
		r = sql_find_subtype(t, "sec_interval", d, 0);
	}
	if (!r)
		return -1;
	return 0;
}
