/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_PROP_H_
#define _REL_PROP_H_

typedef enum rel_prop {
	PROP_COUNT,
	PROP_JOINIDX,   /* could use join idx */
	PROP_HASHIDX,   /* is hash idx */
	PROP_SORTIDX,   /* is sorted */
	PROP_HASHCOL,   /* could use hash idx */
	PROP_FETCH,     /* fetchjoin */
	PROP_REMOTE,    /* uri for remote execution */
	PROP_USED,      /* number of times exp is used */
	PROP_DISTRIBUTE, /* used by merge tables when sql.affectedRows is the sum of several insert/update/delete statements */
	PROP_GROUPINGS  /* used by ROLLUP/CUBE/GROUPING SETS, value contains the list of sets */
} rel_prop;

typedef struct prop {
	rel_prop kind;  /* kind of property */
	void *value;    /* property value */
	struct prop *p; /* some relations may have many properties, which are kept in a chain list */
} prop;

extern prop * prop_create( sql_allocator *sa, rel_prop kind, prop *pre );
extern prop * prop_copy( sql_allocator *sa, prop *p);
extern prop * prop_remove( prop *plist, prop *p);
extern prop * find_prop( prop *p, rel_prop kind);
extern const char * propkind2string( prop *p);
extern char * propvalue2string(sql_allocator *sa, prop *p);

#endif /* _REL_PROP_H_ */
