
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_statistics.h"
#include "rel_optimizer.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_rewriter.h"
#include "sql_mvc.h"

sql_hash *sql_functions_lookup = NULL;

#define sql_binop_propagate_trivial(FUNC) \
static void \
sql_##FUNC##_propagate_statistics(mvc *sql, sql_exp *e) \
{ \
	list *l = e->l; \
	sql_exp *first = l->h->data, *second = l->h->next->data; \
	ValPtr lval, rval; \
	if ((lval = find_prop_and_get(first->p, PROP_MAX)) && (rval = find_prop_and_get(second->p, PROP_MAX))) { \
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord)); \
		res->vtype = lval->vtype; \
		if (VARcalc##FUNC(res, lval, rval, true) == GDK_SUCCEED) { \
			copy_property(sql, e, PROP_MAX, res); \
		} else { \
			GDKclrerr(); \
			atom *a = atom_max_value(sql->sa, exp_subtype(first)); \
			copy_property(sql, e, PROP_MAX, &a->data); \
		} \
	} \
	if ((lval = find_prop_and_get(first->p, PROP_MIN)) && (rval = find_prop_and_get(second->p, PROP_MIN))) { \
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord)); \
		res->vtype = lval->vtype; \
		if (VARcalc##FUNC(res, lval, rval, true) == GDK_SUCCEED) { \
			copy_property(sql, e, PROP_MIN, res); \
		} else { \
			GDKclrerr(); \
			atom *a = atom_max_value(sql->sa, exp_subtype(first)); \
			copy_property(sql, e, PROP_MIN, &a->data); \
		} \
	} \
}

sql_binop_propagate_trivial(add)
sql_binop_propagate_trivial(mul)

static struct function_properties functions_list[2] = {
	{"sql_add", &sql_add_propagate_statistics},
	{"sql_mul", &sql_mul_propagate_statistics}
};

void
initialize_sql_functions_lookup(sql_allocator *sa)
{
	int nentries = sizeof(functions_list) / sizeof(functions_list[0]);

	sql_functions_lookup = hash_new(sa, nentries, (fkeyvalue)&hash_key);
	for (int i = 0; i < nentries ; i++) {
		int key = hash_key(functions_list[i].name);

		hash_add(sql_functions_lookup, key, &(functions_list[i]));
	}
}
