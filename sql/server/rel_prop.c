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

#include <monetdb_config.h>
#include "rel_semantic.h"
#include "rel_prop.h"

prop *
prop_create( sql_allocator *sa, int kind, prop *pre )
{
	prop *p = SA_NEW(sa, prop);
	
	p->kind = kind;
	p->value = 0;
	p->p = pre;
	return p;
}

prop *
prop_copy( sql_allocator *sa, prop *p )
{
	prop *np = NULL;

	for(; p; p = p->p) {
		np = prop_create(sa, p->kind, np);
		np->value = p->value;
	}
	return np;
}

prop *
find_prop( prop *p, int kind)
{
	while(p) {
		if (p->kind == kind)
			return p;
		p = p->p;
	}
	return p;
}

char *
propkind2string( prop *p)
{
	switch(p->kind) {
#define PT(TYPE) case PROP_##TYPE : return #TYPE
		PT(COUNT);
		PT(JOINIDX);
		PT(HASHIDX);
		PT(SORTIDX);
		PT(FETCH);
		PT(USED);
	}
	return "UNKNOWN";
}

char *
propvalue2string( prop *p)
{
	char buf [BUFSIZ];

	if (p->value) {
		switch(p->kind) {
		case PROP_JOINIDX: {
					   sql_idx *i = p->value;

					   snprintf(buf, BUFSIZ, "%s.%s.%s", i->t->s->base.name, i->t->base.name, i->base.name);
					   return _strdup(buf);
				   }
			break;
		default:
			break;
		}
	}
	return "";
}
