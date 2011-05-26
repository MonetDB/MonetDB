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
 * Copyright August 2008-2011 MonetDB B.V.
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
#define PROP_HASHIDX	2	/* could use hash idx */
#define PROP_SORTIDX	3	/* could use sortedness */
#define PROP_USED	10	/* number of times exp is used */

#define prop_list() 	list_create((fdestroy)&prop_destroy)

extern prop * prop_create( sql_allocator *sa, int kind, prop *pre );
extern prop * prop_copy( sql_allocator *sa, prop *p);
extern prop * find_prop( prop *p, int kind);

#endif /* _REL_PROP_H_ */

