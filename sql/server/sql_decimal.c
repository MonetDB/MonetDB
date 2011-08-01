/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */

#include "monetdb_config.h"

#include "sql_decimal.h"

lng
decimal_from_str(char *dec)
{
	lng res = 0;
	int neg = 0;

	if (*dec == '-') {
		neg = 1;
		dec++;
	}
	for (; *dec; dec++) {
		if (*dec != '.') {
			res *= 10;
			res += *dec - '0';
		}
	}
	if (neg)
		return -res;
	else
		return res;
}

char *
decimal_to_str(lng v, sql_subtype *t) 
{
	char buf[32];
	int scale = t->scale, cur = 31, neg = (v<0)?1:0, i, done = 0;

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
	return _strdup(buf+cur+1);
}

