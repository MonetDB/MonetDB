/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

static inline void
set_max_of_values(mvc *sql, sql_exp *e, rel_prop kind, ValPtr lval, ValPtr rval)
{
	prop *p;
	ValRecord res;

	VARcalcgt(&res, lval, rval);
	p = e->p = prop_create(sql->sa, kind, e->p);
	p->value = res.val.btval == 1 ? lval : rval;
}

static inline void
set_min_of_values(mvc *sql, sql_exp *e, rel_prop kind, ValPtr lval, ValPtr rval)
{
	prop *p;
	ValRecord res;

	VARcalcgt(&res, lval, rval);
	p = e->p = prop_create(sql->sa, kind, e->p);
	p->value = res.val.btval == 1 ? rval : lval;
}

static inline void
set_property(mvc *sql, sql_exp *e, rel_prop kind, ValPtr val)
{
	prop *p = e->p = prop_create(sql->sa, kind, e->p);
	p->value = val;
}

extern sql_hash *sql_functions_lookup;
extern void initialize_sql_functions_lookup(sql_allocator *sa);
extern sql_rel *rel_statistics(mvc *sql, sql_rel *rel);

#endif /*_REL_STATISTICS_H_*/
