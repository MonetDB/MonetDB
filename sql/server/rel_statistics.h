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

static inline atom *
statistics_atom_max(mvc *sql, atom *v1, atom *v2)
{
	sql_subtype super;
	atom *cast1 = v1, *cast2 = v2;

	supertype(&super, &v1->tpe, &v2->tpe);
	if (subtype_cmp(&v1->tpe, &super) != 0) {
		cast1 = atom_dup(sql->sa, v1);
		if (!atom_cast(sql->sa, cast1, &super))
			return NULL;
	}
	if (subtype_cmp(&v2->tpe, &super) != 0) {
		cast2 = atom_dup(sql->sa, v2);
		if (!atom_cast(sql->sa, cast2, &super))
			return NULL;
	}
	return atom_cmp(cast1, cast2) > 0 ? cast1 : cast2;
}

static inline atom *
statistics_atom_min(mvc *sql, atom *v1, atom *v2)
{
	sql_subtype super;
	atom *cast1 = v1, *cast2 = v2;

	supertype(&super, &v1->tpe, &v2->tpe);
	if (subtype_cmp(&v1->tpe, &super) != 0) {
		cast1 = atom_dup(sql->sa, v1);
		if (!atom_cast(sql->sa, cast1, &super))
			return NULL;
	}
	if (subtype_cmp(&v2->tpe, &super) != 0) {
		cast2 = atom_dup(sql->sa, v2);
		if (!atom_cast(sql->sa, cast2, &super))
			return NULL;
	}
	return atom_cmp(cast1, cast2) > 0 ? cast2 : cast1;
}

static inline void
set_property(mvc *sql, sql_exp *e, rel_prop kind, atom *val)
{
	sql_subtype *tpe = exp_subtype(e);
	prop *found = find_prop(e->p, kind);

	if (subtype_cmp(&val->tpe, tpe) != 0) { /* make sure it's the same type */
		val = atom_dup(sql->sa, val);
		if (!atom_cast(sql->sa, val, tpe))
			return;
	}
	if (found) {
		found->value = val;
	} else {
		prop *p = e->p = prop_create(sql->sa, kind, e->p);
		p->value = val;
	}
}

extern sql_hash *sql_functions_lookup;
extern void initialize_sql_functions_lookup(sql_allocator *sa);
extern sql_rel *rel_get_statistics(visitor *v, sql_rel *rel);

#endif /*_REL_STATISTICS_H_*/
