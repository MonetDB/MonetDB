/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#ifndef _REL_PROP_H_
#define _REL_PROP_H_

typedef struct prop {
	int kind;	/* kind of property */
	void *value;	/* property value */
	struct prop *p;	/* some relations may have many properties, 
			   which are kept in a chain list */
} prop;

#define PROP_COUNT	0
#define PROP_JOINIDX	1	/* could use join idx */
#define PROP_HASHIDX	2	/* is hash idx */
#define PROP_SORTIDX	3	/* is sorted */
#define PROP_HASHCOL	4	/* could use hash idx */
#define PROP_FETCH	5	/* fetchjoin */
#define PROP_REMOTE     6	/* uri for remote execution */
#define PROP_USED	10	/* number of times exp is used */

extern prop * prop_create( sql_allocator *sa, int kind, prop *pre );
extern prop * prop_copy( sql_allocator *sa, prop *p);
extern prop * prop_remove( prop *plist, prop *p);
extern prop * find_prop( prop *p, int kind);
extern const char * propkind2string( prop *p);
extern char * propvalue2string( prop *p);

#endif /* _REL_PROP_H_ */

