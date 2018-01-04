/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_remote.h"

#define mapi_prefix "mapi:monetdb://"

int
mapiuri_valid( const char *uri)
{
	int i = 0, l = 0;
	const char *p = uri;

	if (strncmp(p, "mapi:monetdb://", strlen(mapi_prefix)))
		return 0;
	/* optional host (todo limit to valid hostnames ??) */
	p += strlen(mapi_prefix);
	for(; *p; p++) {
		if (*p == ':')
			break;
		if (*p == '/')
			break;
	}
	if (!p)
		return 0;
	if (*p == ':') {
		char *x;
		int i = strtol(p+1, &x, 10);

		if (!x || i < 0 || i >= 64*1024)
			return 0;
		p = x;
	}
	if (*p != '/')
		return 0;
	p++; 
	/* now find at most 2 '/'s, with some string inbetween */
	for(; *p; p++, l++) {
		if (*p == '/') {
			if (l == 0) /* no string inbetween */
				return 0;
			if (i == 2) /* 3 parts (ie database/schema/table) */
				return 0;
			i++;
			l=0;
		}
	}
	if (i == 0 && l == 0) /* missing database name */
		return 0;
	return 1;
}

/* assume valid uri's next functions */

/* mapiuri_uri prefix including database name */
const char *
mapiuri_uri( const char *uri, sql_allocator *sa)
{
	const char *p = uri, *b = uri, *e;

	p = strchr(p, '/')+1;
	p++;
	e = p = strchr(p, '/');
	e = strchr(p+1, '/');
	if (e)
		return sa_strndup(sa, b, e - b);
	else 
		return sa_strdup(sa, b);
}

const char *
mapiuri_database( const char *uri, sql_allocator *sa)
{
	const char *p = uri, *b, *e;

	p = strchr(p, '/')+1;
	p++;
	b = p = strchr(p, '/')+1;
	e = strchr(p, '/');

	if (e) {
		return sa_strndup(sa, b, e - b);
	} else {
		return sa_strdup(sa, b);
	}
}

const char *
mapiuri_schema( const char *uri, sql_allocator *sa, const char *fallback)
{
	const char *p = uri, *b, *e;

	p = strchr(p, '/')+1;
	p = strchr(p+1, '/');
	p = strchr(p+1, '/');
	if (!p)
		return fallback;
 	b = ++p;
	e = strchr(p, '/');

	if (e) {
		return sa_strndup(sa, b, e - b);
	} else {
		return sa_strdup(sa, b);
	}
}

const char *
mapiuri_table( const char *uri, sql_allocator *sa, const char *fallback)
{
	const char *p = uri, *b;

	p = strchr(p, '/')+1;
	p = strchr(p+1, '/');
	p = strchr(p+1, '/');
	if (!p)
		return fallback;
	p = strchr(p+1, '/');
	if (!p)
		return fallback;
 	b = ++p;
	return sa_strdup(sa, b);
}
