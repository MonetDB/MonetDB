
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
		res->vtype = lval->vtype > rval->vtype ? lval->vtype : rval->vtype;
		if (VARcalcadd(res, lval, rval, true) == GDK_SUCCEED) {
			set_property(sql, e, PROP_MAX, res);
		} else {
			GDKclrerr(); /* if the min/max pair overflows, then don't propagate */
		}
	}
	if ((lval = find_prop_and_get(first->p, PROP_MIN)) && (rval = find_prop_and_get(second->p, PROP_MIN))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		res->vtype = lval->vtype > rval->vtype ? lval->vtype : rval->vtype;
		if (VARcalcadd(res, lval, rval, true) == GDK_SUCCEED) {
			set_property(sql, e, PROP_MIN, res);
		} else {
			GDKclrerr();
		}
	}
}

#define mul_and_sub_propagate(FUNC) \
static void \
sql_##FUNC##_propagate_statistics(mvc *sql, sql_exp *e) \
{ \
	list *l = e->l; \
	sql_exp *first = l->h->data, *second = l->h->next->data; \
	ValPtr lmax, rmax, lmin, rmin; \
 \
	if ((lmax = find_prop_and_get(first->p, PROP_MAX)) && (rmax = find_prop_and_get(second->p, PROP_MAX)) && \
		(lmin = find_prop_and_get(first->p, PROP_MIN)) && (rmin = find_prop_and_get(second->p, PROP_MIN))) { \
		ValPtr res1 = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord)), res2 = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord)); \
		gdk_return code1, code2; \
 \
		res1->vtype = res2->vtype = lmax->vtype > rmax->vtype ? lmax->vtype : rmax->vtype; \
		code1 = VARcalc##FUNC(res1, lmax, rmax, true); \
		GDKclrerr(); \
		code2 = VARcalc##FUNC(res2, lmin, rmin, true); \
		GDKclrerr(); \
 \
		if (code1 == GDK_SUCCEED && code2 == GDK_SUCCEED) { /* if the min/max pair overflows, then don't propagate */ \
			ValRecord zero1 = (ValRecord) {.vtype = lmax->vtype,}, zero2 = (ValRecord) {.vtype = rmax->vtype,}, zero3 = (ValRecord) {.vtype = lmin->vtype,}, \
					  zero4 = (ValRecord) {.vtype = rmin->vtype,}, verify1, verify2, verify3, verify4; \
 \
			VARcalcge(&verify1, lmax, &zero1); \
			VARcalcge(&verify2, rmax, &zero2); \
			VARcalcge(&verify3, lmin, &zero3); \
			VARcalcge(&verify4, rmin, &zero4); \
 \
			if (verify1.val.btval == 1 && verify2.val.btval == 1 && verify3.val.btval == 1 && verify4.val.btval == 1) { /* if all positive then propagate */ \
				set_property(sql, e, PROP_MAX, res1); \
				set_property(sql, e, PROP_MIN, res2); \
			} else if (verify1.val.btval == 0 && verify2.val.btval == 0 && verify3.val.btval == 0 && verify4.val.btval == 0) { /* if all negative propagate by swapping min and max */ \
				set_property(sql, e, PROP_MIN, res1); \
				set_property(sql, e, PROP_MAX, res2); \
			} \
		} \
	} \
}

mul_and_sub_propagate(sub)
mul_and_sub_propagate(mul)

static void
sql_div_propagate_statistics(mvc *sql, sql_exp *e)
{
	list *l = e->l;
	sql_exp *first = l->h->data, *second = l->h->next->data;
	ValPtr lmax, rmax, lmin, rmin;

	if ((lmax = find_prop_and_get(first->p, PROP_MAX)) && (rmax = find_prop_and_get(second->p, PROP_MAX)) &&
		(lmin = find_prop_and_get(first->p, PROP_MIN)) && (rmin = find_prop_and_get(second->p, PROP_MIN))) {
		ValPtr res1 = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord)), res2 = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		gdk_return code1, code2;

		res1->vtype = res2->vtype = lmax->vtype > rmax->vtype ? lmax->vtype : rmax->vtype;
		code1 = VARcalcdiv(res1, lmax, rmin, true);
		GDKclrerr();
		code2 = VARcalcdiv(res2, lmin, rmax, true);
		GDKclrerr();

		if (code1 == GDK_SUCCEED && code2 == GDK_SUCCEED) { /* if the min/max pair overflows, then don't propagate */
			ValRecord zero1 = (ValRecord) {.vtype = lmax->vtype,}, zero2 = (ValRecord) {.vtype = rmax->vtype,}, zero3 = (ValRecord) {.vtype = lmin->vtype,},
					  zero4 = (ValRecord) {.vtype = rmin->vtype,}, verify1, verify2, verify3, verify4;

			VARcalcge(&verify1, lmax, &zero1);
			VARcalcge(&verify2, rmax, &zero2);
			VARcalcge(&verify3, lmin, &zero3);
			VARcalcge(&verify4, rmin, &zero4);

			if (verify1.val.btval == 1 && verify2.val.btval == 1 && verify3.val.btval == 1 && verify4.val.btval == 1) { /* if all positive then propagate */
				set_property(sql, e, PROP_MAX, res1);
				set_property(sql, e, PROP_MIN, res2);
			} else if (verify1.val.btval == 0 && verify2.val.btval == 0 && verify3.val.btval == 0 && verify4.val.btval == 0) { /* if all negative propagate by swapping min and max */
				set_property(sql, e, PROP_MIN, res1);
				set_property(sql, e, PROP_MAX, res2);
			}
		}
	}
}

static void
sql_extend_min_max(mvc *sql, sql_exp *e, sql_exp *first, sql_exp *second)
{
	ValPtr lval, rval;

	if ((lval = find_prop_and_get(first->p, PROP_MAX)) && (rval = find_prop_and_get(second->p, PROP_MAX))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		ValRecord verify1;

		VARcalcgt(&verify1, lval, rval);
		VALcopy(res, verify1.val.btval == 1 ? lval : rval);
		set_property(sql, e, PROP_MAX, res);
	}
	if ((lval = find_prop_and_get(first->p, PROP_MIN)) && (rval = find_prop_and_get(second->p, PROP_MIN))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		ValRecord verify1;

		VARcalcgt(&verify1, lval, rval);
		VALcopy(res, verify1.val.btval == 1 ? rval : lval);
		set_property(sql, e, PROP_MIN, res);
	}
}

static void
sql_least_greatest_propagate_statistics(mvc *sql, sql_exp *e)
{
	list *l = e->l;
	sql_extend_min_max(sql, e, l->h->data, l->h->next->data);
}

static void
sql_ifthenelse_propagate_statistics(mvc *sql, sql_exp *e)
{
	list *l = e->l;
	sql_extend_min_max(sql, e, l->h->next->data, l->h->next->next->data);
}

static void
sql_nullif_propagate_statistics(mvc *sql, sql_exp *e)
{
	list *l = e->l;
	sql_exp *first = l->h->data;
	ValPtr lval;

	if ((lval = find_prop_and_get(first->p, PROP_MAX))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		VALcopy(res, lval);
		set_property(sql, e, PROP_MAX, res);
	}
	if ((lval = find_prop_and_get(first->p, PROP_MIN))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		VALcopy(res, lval);
		set_property(sql, e, PROP_MIN, res);
	}
}

static struct function_properties functions_list[10] = {
	{"sql_add", &sql_add_propagate_statistics},
	{"sql_sub", &sql_sub_propagate_statistics},
	{"sql_mul", &sql_mul_propagate_statistics},
	{"sql_div", &sql_div_propagate_statistics},
	{"sql_min", &sql_least_greatest_propagate_statistics},
	{"sql_max", &sql_least_greatest_propagate_statistics},
	{"least", &sql_least_greatest_propagate_statistics},
	{"greatest", &sql_least_greatest_propagate_statistics},
	{"ifthenelse", &sql_ifthenelse_propagate_statistics},
	{"nullif", &sql_nullif_propagate_statistics}
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
