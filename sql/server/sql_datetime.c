/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
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
parse_interval_(mvc *sql, lng sign, char *str, int sk, int ek, int sp, int ep, lng *i)
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
parse_interval(mvc *sql, lng sign, char *str, int sk, int ek, int sp, int ep, lng *i)
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

int interval_from_str(char *str, int d, int p, lng *val)
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
	case imonth:
		return 3;
	case iday:
		switch(ek) {
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

