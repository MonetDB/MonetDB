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

#ifndef _REL_PROP_H_
#define _REL_PROP_H_

typedef enum rel_prop {
	PROP_COUNT,     /* Number of expect rows for the relation */
	PROP_NUNIQUES,  /* Estimated number of distinct rows for the expression */
	PROP_MIN,       /* min value if available */
	PROP_MAX,       /* max value if available */
	PROP_JOINIDX,   /* could use join idx */
	PROP_HASHIDX,   /* is hash idx */
	PROP_HASHCOL,   /* could use hash idx */
	PROP_REMOTE,    /* uri for remote execution */
	PROP_USED,      /* number of times exp is used */
	PROP_GROUPINGS  /* used by ROLLUP/CUBE/GROUPING SETS, value contains the list of sets */
} rel_prop;

typedef struct prop {
	rel_prop kind;  /* kind of property */
	sqlid id;		/* optional id of object involved */
	union {
		BUN lval; /* property with simple counts */
		dbl dval; /* property with estimate */
		void *pval; /* property value */
	} value;
	struct prop *p; /* some relations may have many properties, which are kept in a chain list */
} prop;

/* for REMOTE prop we need to keep a list with tids and uris for the remote tables */
typedef struct tid_uri {
	sqlid id;
	const char* uri;
} tid_uri;

sql_export prop * prop_create( allocator *sa, rel_prop kind, prop *pre );
extern prop * prop_copy( allocator *sa, prop *p);
extern prop * prop_remove( prop *plist, prop *p);
extern prop * find_prop( prop *p, rel_prop kind);
extern void * find_prop_and_get(prop *p, rel_prop kind);
extern const char * propkind2string( prop *p);
extern char * propvalue2string(allocator *sa, prop *p);

#endif /* _REL_PROP_H_ */
