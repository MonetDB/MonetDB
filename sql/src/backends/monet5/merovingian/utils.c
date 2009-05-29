/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
 * Copyright August 2008-2009 MonetDB B.V.
 * All Rights Reserved.
 */

/**
 * utils
 * Fabian Groffen
 * Shared utility functions between merovingian and monetdb
 */

#include "utils.h"
#include "sql_config.h"
#include <stdio.h> /* fprintf, fgets */
#include <string.h> /* memcpy */
#include <gdk.h> /* GDKmalloc */

/**
 * Returns a GDKmalloced copy of s, with the first occurrence of
 * "${prefix}" replaced by prefix.  If s is NULL, this value returns
 * also NULL.
 */
inline char *
replacePrefix(char *s, char *prefix)
{
	char *p;
	char buf[1024];

	/* replace first occurence of ${prefix}, return a modified copy */
	p = strstr(s, "${prefix}");
	if (p != NULL) {
		memcpy(buf, s, p - s);
		memcpy(buf + (p - s), prefix, strlen(prefix));
		memcpy(buf + (p - s) + strlen(prefix), s + (p - s) + 9,
				strlen(s) - 9 - (p - s) + 1);
	} else {
		memcpy(buf, s, strlen(s) + 1);
	}
	
	return(GDKstrdup(buf));
}

/**
 * Parses the given file stream matching the keys from list.  If a match
 * is found, the value is set in list->value.  Values are GDKmalloced.
 */
inline void
readConfFile(confkeyval *list, FILE *cnf) {
	char buf[1024];
	confkeyval *t;
	size_t len;

	while (fgets(buf, 1024, cnf) != NULL) {
		/* eliminate fgets' newline */
		buf[strlen(buf) - 1] = '\0';
		for (t = list; t->key != NULL; t++) {
			len = strlen(t->key);
			if (*buf && strncmp(buf, t->key, len) == 0 && buf[len] == '=') {
				if (t->val != NULL)
					GDKfree(t->val);
				t->val = GDKstrdup(buf + len + 1);
			}
		}
	}
}

/**
 * Fills the array pointed to by buf with a human representation of t.
 * The argument longness represents the number of units to print
 * starting from the biggest unit that has a non-zero value for t.
 */
inline void
secondsToString(char *buf, time_t t, int longness)
{
	time_t p;
	size_t i = 0;

	p = 1 * 60 * 60 * 24 * 7;
	if (t > p) {
		i += sprintf(buf, "%dw", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}
	p /= 7;
	if (t > p) {
		i += sprintf(buf, "%dd", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}
	p /= 24;
	if (t > p) {
		i += sprintf(buf + i, "%dh", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}
	p /= 60;
	if (t > p) {
		i += sprintf(buf + i, "%dm", (int)(t / p));
		t -= (t / p) * p;
		if (--longness == 0)
			return;
		buf[i++] = ' ';
	}

	/* t must be < 60 */
	if (--longness == 0 || !(i > 0 && t == 0)) {
		sprintf(buf + i, "%ds", (int)(t));
	} else {
		buf[--i] = '\0';
	}
}

/* vim:set ts=4 sw=4 noexpandtab: */
