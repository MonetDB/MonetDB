/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"

#include "sql_decimal.h"


DEC_TPE
decimal_from_str(const char *dec, int* digits, int* scale, int* has_errors)
{

#ifdef HAVE_HGE
    const hge max0 = GDK_hge_max / 10, max1 = GDK_hge_max % 10;
#else
    const lng max0 = GDK_lng_max / 10, max1 = GDK_lng_max % 10;
#endif

	assert(digits);
	assert(scale);
	assert(has_errors);

	DEC_TPE res = 0;
	*has_errors = 0;

	int _digits	= 0;
	int _scale	= 0;

// preceding whitespace:
	int neg = 0;
	while(isspace((unsigned char) *dec))
		dec++;

// optional sign:
	if (*dec == '-') {
		neg = 1;
		dec++;
	} else if (*dec == '+') {
		dec++;
	}

// optional fractional separator first opportunity
	if (*dec == '.') {  // case: (+|-).456
fractional_sep_first_opp:
		dec++;
		goto trailing_digits;
	}

// preceding_digits:
	if (!isdigit((unsigned char) *dec)) {
		*has_errors = 1;
		goto end_state;
	}
	while (*dec == '0' || *dec == '_'){
		// skip leading zeros in preceding digits, e.g. '0004563.1234' => '4563.1234'
		dec++;
		if (*dec == '.') {
			_digits = 1; // case: 0.xyz the zero. the single preceding zero counts for one digit by convention.
			goto fractional_sep_first_opp;
		}
	}
	for (; *dec && (*dec == '_' || isdigit((unsigned char) *dec)); dec++) {
		if (*dec == '_')
			continue;
		if (res > max0 || (res == max0 && *dec - '0' > max1)) {
			*has_errors = 1;
			return 0;
		}
		res *= 10;
		res += *dec - '0';
		_digits++;
	}

// optional fractional separator second opportunity
	if (*dec == '.')	// case: (+|-)123.(456)
		dec++;
	else					// case:  (+|-)123
		goto trailing_whitespace;

trailing_digits:
	if (!isdigit((unsigned char) *dec))
		goto trailing_whitespace;
	for (; *dec && (*dec == '_' || isdigit((unsigned char) *dec)); dec++) {
		if (*dec == '_')
			continue;
		if (res > max0 || (res == max0 && *dec - '0' > max1)) {
			*has_errors = 1;
			return 0;
		}
		res *= 10;
		res += *dec - '0';
		_scale++;
	}
	_digits += _scale;

trailing_whitespace:
	while(isspace((unsigned char) *dec))
		dec++;

end_state:
	/* When the string cannot be parsed up to and including the null terminator,
	 * the string is an invalid decimal representation. */
	if (*dec != 0)
		*has_errors = 1;

	*digits = _digits;
	*scale = _scale;

	if (neg)
		return -res;
	else
		return res;
}

char *
#ifdef HAVE_HGE
decimal_to_str(allocator *sa, hge v, sql_subtype *t)
#else
decimal_to_str(allocator *sa, lng v, sql_subtype *t)
#endif
{
	char buf[64];
	unsigned int scale = t->scale, i;
	int cur = 63, neg = (v<0), done = 0;

	if (v<0) v = -v;

	buf[cur--] = 0;
	if (scale){
		for (i=0; i<scale; i++) {
			buf[cur--] = (char) (v%10 + '0');
			v /= 10;
		}
		buf[cur--] = '.';
	}
	while (v) {
		buf[cur--] = (char ) (v%10 + '0');
		v /= 10;
		done = 1;
	}
	if (!done)
		buf[cur--] = '0';
	if (neg)
		buf[cur--] = '-';
	assert(cur >= -1);
	return sa_strdup(sa, buf+cur+1);
}

unsigned int
#ifdef HAVE_HGE
decimal_digits(hge val)
#else
decimal_digits(lng val)
#endif

{
	if (val < 0)
		val = -val;
	unsigned int digits = 1;
	while (val >= 10) {
		val /= 10;
		digits++;
	}
	return digits;
}

#ifdef HAVE_HGE
extern hge
#else
extern lng
#endif
scale2value(int scale)
{
#ifdef HAVE_HGE
	hge val = 1;
#else
	lng val = 1;
#endif

	if (scale < 0)
		scale = -scale;
	for (; scale; scale--) {
		val = val * 10;
	}
	return val;
}

unsigned int
#ifdef HAVE_HGE
number_bits(hge val)
#else
number_bits(lng val)
#endif
{
	if (val < 0)
		val = -val;
	unsigned bits = 0;
#ifdef HAVE_HGE
	hge m = (hge)1 << bits;
	for( ; (val & ~m) > m; bits++)
		m = (hge)1 << bits;
#else
	lng m = (lng)1 << bits;
	for( ; (val & ~m) > m; bits++)
		m = ((lng)1) << bits;
#endif
	if (!bits)
		bits = 1;
	return bits;
}
