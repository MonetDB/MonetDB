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
 * Copyright August 2008-2014 MonetDB B.V.
 * All Rights Reserved.
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

