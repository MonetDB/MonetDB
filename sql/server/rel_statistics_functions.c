
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

static void
sql_add_propagate_statistics(mvc *sql, sql_exp *e)
{
	list *l = e->l;
	sql_exp *first = l->h->data, *second = l->h->next->data;
	ValPtr lval, rval;

	if ((lval = find_prop_and_get(first->p, PROP_MAX)) && (rval = find_prop_and_get(second->p, PROP_MAX))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		res->vtype = lval->vtype;
		if (VARcalcadd(res, lval, rval, true) == GDK_SUCCEED) {
			copy_property(sql, e, PROP_MAX, res);
		} else {
			GDKclrerr(); /* if the min/max pair overflows, then disable */
		}
	}
	if ((lval = find_prop_and_get(first->p, PROP_MIN)) && (rval = find_prop_and_get(second->p, PROP_MIN))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		res->vtype = lval->vtype;
		if (VARcalcadd(res, lval, rval, true) == GDK_SUCCEED) {
			copy_property(sql, e, PROP_MIN, res);
		} else {
			GDKclrerr();
		}
	}
}

static void
sql_mul_propagate_statistics(mvc *sql, sql_exp *e)
{
	list *l = e->l;
	sql_exp *first = l->h->data, *second = l->h->next->data;
	ValPtr lval, rval;
	ValRecord zero, verify;

	if ((lval = find_prop_and_get(first->p, PROP_MAX)) && (rval = find_prop_and_get(second->p, PROP_MAX))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		res->vtype = lval->vtype;
		if (VARcalcmul(res, lval, rval, true) == GDK_SUCCEED) { /* only propagate '*' min/max when both sides have the same sign ie a * b >= 0 */
			VALinit(&zero, res->vtype, &(int){0});
			VARcalcge(&verify, res, &zero);
			if (verify.val.btval == 1)
				copy_property(sql, e, PROP_MAX, res);
		} else {
			GDKclrerr(); /* if the min/max pair overflows, then disable */
		}
	}
	if ((lval = find_prop_and_get(first->p, PROP_MIN)) && (rval = find_prop_and_get(second->p, PROP_MIN))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		res->vtype = lval->vtype;
		if (VARcalcmul(res, lval, rval, true) == GDK_SUCCEED) {
			VALinit(&zero, res->vtype, &(int){0});
			VARcalcge(&verify, res, &zero);
			if (verify.val.btval == 1)
				copy_property(sql, e, PROP_MIN, res);
		} else {
			GDKclrerr();
		}
	}
}

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
