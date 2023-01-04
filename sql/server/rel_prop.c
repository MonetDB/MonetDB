/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_relation.h"
#include "rel_prop.h"
#include "sql_string.h"
#include "sql_atom.h"

prop *
prop_create( sql_allocator *sa, rel_prop kind, prop *pre )
{
	prop *p = SA_NEW(sa, prop);

	*p = (prop) {
		.kind = kind,
		.p = pre,
	};
	if (kind == PROP_NUNIQUES)
		p->value.dval = 0.0; /* floating point compatibility */
	return p;
}

prop *
prop_copy( sql_allocator *sa, prop *p )
{
	prop *np = NULL;

	for(; p; p = p->p) {
		np = prop_create(sa, p->kind, np);
		switch (p->kind) {
		case PROP_COUNT:
			np->value.lval = p->value.lval;
			break;
		case PROP_NUNIQUES:
			np->value.dval = p->value.dval;
			break;
		default:
			np->value.pval = p->value.pval;
		}
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
find_prop( prop *p, rel_prop kind)
{
	while(p) {
		if (p->kind == kind)
			return p;
		p = p->p;
	}
	return p;
}

void *
find_prop_and_get(prop *p, rel_prop kind)
{
	prop *found = find_prop(p, kind);

	/* this doesn't work with numbers yet */
	assert(kind != PROP_COUNT && kind != PROP_NUNIQUES);
	return found ? found->value.pval : NULL;
}

#ifdef MIN
#undef MIN
#endif

#ifdef MAX
#undef MAX
#endif

const char *
propkind2string( prop *p)
{
	switch(p->kind) {
#define PT(TYPE) case PROP_##TYPE : return #TYPE
		PT(COUNT);
		PT(NUNIQUES);
		PT(JOINIDX);
		PT(HASHIDX);
		PT(HASHCOL);
		PT(REMOTE);
		PT(USED);
		PT(GROUPINGS);
		PT(MIN);
		PT(MAX);
	}
	return "UNKNOWN";
}

char *
propvalue2string(sql_allocator *sa, prop *p)
{
	char buf [BUFSIZ];

	switch(p->kind) {
	case PROP_COUNT: {
		snprintf(buf, BUFSIZ, BUNFMT, p->value.lval);
		return sa_strdup(sa, buf);
	}
	case PROP_NUNIQUES: {
		snprintf(buf, BUFSIZ, "%f", p->value.dval);
		return sa_strdup(sa, buf);
	}
	case PROP_JOINIDX: {
		sql_idx *i = p->value.pval;

		if (i) {
			snprintf(buf, BUFSIZ, "\"%s\".\"%s\".\"%s\"", sql_escape_ident(sa, i->t->s->base.name),
					 sql_escape_ident(sa, i->t->base.name), sql_escape_ident(sa, i->base.name));
			return sa_strdup(sa, buf);
		}
	} break;
	case PROP_REMOTE: {
		char *uri = p->value.pval;

		if (uri)
			return sa_strdup(sa, uri);
	} break;
	case PROP_MIN:
	case PROP_MAX: {
		atom *a = p->value.pval;
		char *res = NULL;

		if (a->isnull) {
			res = sa_strdup(sa, "\"NULL\"");
		} else {
			char *s = ATOMformat(a->data.vtype, VALptr(&a->data));
			if (s && *s == '"') {
				res = sa_strdup(sa, s);
			} else if (s) {
				res = sa_alloc(sa, strlen(s) + 3);
				stpcpy(stpcpy(stpcpy(res, "\""), s), "\"");
			}
			GDKfree(s);
		}
		return res;
	}
	default:
		break;
	}
	return sa_strdup(sa, "");
}
