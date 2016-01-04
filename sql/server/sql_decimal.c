/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "sql_decimal.h"

#ifdef HAVE_HGE
hge
#else
lng
#endif
decimal_from_str(char *dec, char **end)
{
#ifdef HAVE_HGE
	hge res = 0;
#else
	lng res = 0;
#endif
	int neg = 0;

	while(isspace(*dec))
		dec++;
	if (*dec == '-') {
		neg = 1;
		dec++;
	}
	if (*dec == '+') {
		neg = 0;
		dec++;
	}
	for (; *dec && ((*dec >= '0' && *dec <= '9') || *dec == '.'); dec++) {
		if (*dec != '.') {
			res *= 10;
			res += *dec - '0';
		}
	}
	while(isspace(*dec))
		dec++;
	if (end)
		*end = dec;
	if (neg)
		return -res;
	else
		return res;
}

char *
#ifdef HAVE_HGE
decimal_to_str(hge v, sql_subtype *t) 
#else
decimal_to_str(lng v, sql_subtype *t) 
#endif
{
	char buf[64];
	int scale = t->scale, cur = 63, neg = (v<0), i, done = 0;

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
	return _STRDUP(buf+cur+1);
}

