/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _REL_STATISTICS_H_
#define _REL_STATISTICS_H_

#include "sql_relation.h"
#include "sql_mvc.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"

typedef void (*lookup_function) (mvc*, sql_exp*);

struct function_properties {
	const char *name;
	lookup_function func;
};

#define atom_max(X,Y) atom_cmp(X, Y) > 0 ? X : Y
#define atom_min(X,Y) atom_cmp(X, Y) > 0 ? Y : X

static inline void
set_property(mvc *sql, sql_exp *e, rel_prop kind, atom *val)
{
	prop *found = find_prop(e->p, kind);
	if (found) {
		found->value = val;
	} else {
		prop *p = e->p = prop_create(sql->sa, kind, e->p);
		p->value = val;
	}
}

static inline void
set_min_property(mvc *sql, sql_exp *e, atom *val)
{
	prop *found = find_prop(e->p, PROP_MIN);
	if (found) {
		found->value = atom_max(found->value, val);
	} else {
		prop *p = e->p = prop_create(sql->sa, PROP_MIN, e->p);
		p->value = val;
	}
}

static inline void
set_max_property(mvc *sql, sql_exp *e, atom *val)
{
	prop *found = find_prop(e->p, PROP_MAX);
	if (found) {
		found->value = atom_min(found->value, val);
	} else {
		prop *p = e->p = prop_create(sql->sa, PROP_MAX, e->p);
		p->value = val;
	}
}

extern sql_hash *sql_functions_lookup;
extern void initialize_sql_functions_lookup(sql_allocator *sa);
extern sql_rel *rel_statistics(mvc *sql, sql_rel *rel);

#endif /*_REL_STATISTICS_H_*/
