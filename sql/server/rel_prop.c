/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_relation.h"
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
prop_remove( prop *plist, prop *p )
{
	prop *op = plist;

	if (plist == p)
		return p->p;
	for(; op; op = op->p) {
		if (op->p == p) {
			op->p = p->p;
			break;
		}
	}
	return plist;
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

const char *
propkind2string( prop *p)
{
	switch(p->kind) {
#define PT(TYPE) case PROP_##TYPE : return #TYPE
		PT(COUNT);
		PT(JOINIDX);
		PT(HASHIDX);
		PT(SORTIDX);
		PT(HASHCOL);
		PT(FETCH);
		PT(REMOTE);
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
			   return _STRDUP(buf);
			}
		case PROP_REMOTE: {
			   char *uri = p->value;

			   return _STRDUP(uri);
			}
		default:
			break;
		}
	}
	return _STRDUP("");
}
