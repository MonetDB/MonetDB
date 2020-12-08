
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
copy_property(mvc *sql, sql_exp *e, rel_prop kind, ValPtr val)
{
	prop *p = e->p = prop_create(sql->sa, kind, e->p);
	p->value = val;
}

static sql_hash *sql_functions_lookup = NULL;

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
			GDKclrerr();
			atom *a = atom_max_value(sql->sa, exp_subtype(first));
			copy_property(sql, e, PROP_MAX, &a->data);
		}
	}
	if ((lval = find_prop_and_get(first->p, PROP_MIN)) && (rval = find_prop_and_get(second->p, PROP_MIN))) {
		ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
		res->vtype = lval->vtype;
		if (VARcalcadd(res, lval, rval, true) == GDK_SUCCEED) {
			copy_property(sql, e, PROP_MIN, res);
		} else {
			GDKclrerr();
			atom *a = atom_max_value(sql->sa, exp_subtype(first));
			copy_property(sql, e, PROP_MIN, &a->data);
		}
	}
}

typedef void (*lookup_function) (mvc*, sql_exp*);

static struct function_properties {
	const char *name;
	lookup_function func;
} functions_list[1] = {
	{"sql_add", &sql_add_propagate_statistics}
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

static bool
exps_have_or(list *exps)
{
	for (node *n = exps->h ; n ; n = n->next) {
		sql_exp *e = n->data;
		assert(e->type == e_cmp);
		if (e->flag == cmp_or)
			return true;
	}
	return false;
}

static sql_exp *
comparison_find_column(sql_exp *input, sql_exp *e)
{
	switch (input->type) {
		case e_convert: /* if the conversion is for a different SQL class, the min and max cannot be converted */
			if (((sql_subtype*)exp_fromtype(input))->type->eclass == ((sql_subtype*)exp_totype(input))->type->eclass)
				return comparison_find_column(input->l, e);
			return NULL;
		case e_column:
			return exp_match(e, input) ? input : NULL;
		default:
			return NULL;
	}
}

static sql_exp *
rel_propagate_column_ref_statistics(mvc *sql, sql_rel *rel, sql_exp *e)
{
	ValPtr lval, rval;
	sql_exp *ne = NULL;

	assert(e->type == e_column);
	if (rel) {
		switch(rel->op) {
		case op_left:
		case op_right:
		case op_full:
		case op_join:
		case op_select:
		/* case op_anti: later */
		case op_semi: {
			if (!list_empty(rel->exps) && !exps_have_or(rel->exps)) { /* if there's an or, the MIN and MAX get difficult to propagate */
				for (node *n = rel->exps->h ; n && !ne; n = n->next) {
					sql_exp *comp = n->data;

					switch (comp->flag) {
					case cmp_equal: {
						sql_exp *le = comp->l, *re = comp->r, *rne = NULL;

						if ((ne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e))) {
							if (is_outerjoin(rel->op)) {
								if ((lval = find_prop_and_get(ne ? ne->p : rne->p, PROP_MAX)))
									copy_property(sql, e, PROP_MAX, lval);
								if ((lval = find_prop_and_get(ne ? ne->p : rne->p, PROP_MIN)))
									copy_property(sql, e, PROP_MIN, lval);
							} else {
								if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX)))
									set_min_of_values(sql, e, PROP_MAX, lval, rval); /* for equality reduce */
								if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN)))
									set_max_of_values(sql, e, PROP_MIN, lval, rval);
							}
						}
						ne = ne ? ne : rne;
					} break;
					case cmp_notequal: {
						sql_exp *le = comp->l, *re = comp->r, *rne = NULL;

						if ((ne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e))) {
							if (is_outerjoin(rel->op)) {
								if ((lval = find_prop_and_get(ne ? ne->p : rne->p, PROP_MAX)))
									copy_property(sql, e, PROP_MAX, lval);
								if ((lval = find_prop_and_get(ne ? ne->p : rne->p, PROP_MIN)))
									copy_property(sql, e, PROP_MIN, lval);
							} else {
								if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX)))
									set_max_of_values(sql, e, PROP_MAX, lval, rval); /* for inequality expand */
								if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN)))
									set_min_of_values(sql, e, PROP_MIN, lval, rval);
							}
						}
						ne = ne ? ne : rne;
					} break;
					case cmp_gt:
					case cmp_gte: {
						sql_exp *le = comp->l, *re = comp->r, *rne = NULL;

						assert(!comp->f);
						if ((ne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e))) {
							if (is_outerjoin(rel->op)) {
								if ((lval = find_prop_and_get(ne ? ne->p : rne->p, PROP_MAX)))
									copy_property(sql, e, PROP_MAX, lval);
								if ((lval = find_prop_and_get(ne ? ne->p : rne->p, PROP_MIN)))
									copy_property(sql, e, PROP_MIN, lval);
							} else if (ne) {
								if ((lval = find_prop_and_get(le->p, PROP_MAX)))
									copy_property(sql, e, PROP_MAX, lval);
								if ((rval = find_prop_and_get(re->p, PROP_MAX)))
									copy_property(sql, e, PROP_MIN, rval);
							} else {
								if ((lval = find_prop_and_get(le->p, PROP_MIN)))
									copy_property(sql, e, PROP_MAX, lval);
								if ((rval = find_prop_and_get(re->p, PROP_MIN)))
									copy_property(sql, e, PROP_MIN, rval);
							}
						}
						ne = ne ? ne : rne;
					} break;
					case cmp_lt:
					case cmp_lte: {
						sql_exp *le = comp->l, *re = comp->r, *fe = comp->f, *rne = NULL, *fne = NULL;

						if ((ne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e)) || (fe && (fne = comparison_find_column(fe, e)))) {
							if (is_outerjoin(rel->op)) {
								if ((lval = find_prop_and_get(ne ? ne->p : rne ? rne->p : fne->p, PROP_MAX)))
									copy_property(sql, e, PROP_MAX, lval);
								if ((lval = find_prop_and_get(ne ? ne->p : rne ? rne->p : fne->p, PROP_MIN)))
									copy_property(sql, e, PROP_MIN, lval);
							} else if (ne) {
								if (fe) { /* range case */
									if ((lval = find_prop_and_get(fe->p, PROP_MIN)))
										copy_property(sql, e, PROP_MAX, lval);
									if ((rval = find_prop_and_get(re->p, PROP_MAX)))
										copy_property(sql, e, PROP_MIN, rval);
								} else {
									if ((lval = find_prop_and_get(re->p, PROP_MIN)))
										copy_property(sql, e, PROP_MAX, lval);
									if ((rval = find_prop_and_get(le->p, PROP_MIN)))
										copy_property(sql, e, PROP_MIN, rval);
								}
							} else if (rne) {
								if (fe) { /* range case */
									if ((lval = find_prop_and_get(re->p, PROP_MIN)))
										copy_property(sql, e, PROP_MAX, lval);
									if ((rval = find_prop_and_get(le->p, PROP_MIN)))
										copy_property(sql, e, PROP_MIN, rval);
								} else {
									if ((lval = find_prop_and_get(re->p, PROP_MAX)))
										copy_property(sql, e, PROP_MAX, lval);
									if ((rval = find_prop_and_get(le->p, PROP_MAX)))
										copy_property(sql, e, PROP_MIN, rval);
								}
							} else { /* range case */
								if ((lval = find_prop_and_get(fe->p, PROP_MAX)))
									copy_property(sql, e, PROP_MAX, lval);
								if ((rval = find_prop_and_get(le->p, PROP_MAX)))
									copy_property(sql, e, PROP_MIN, rval);
							}
						}
						ne = ne ? ne : rne ? rne : fne;
					} break;
					default: /* Maybe later I can do cmp_in and cmp_notin */
						break;
					}
				}
			}
			if (ne && !find_prop(e->p, PROP_MAX) && !find_prop(e->p, PROP_MIN)) /* ne was found, but the properties could not be propagated */
				ne = NULL;
			if (!ne)
				ne = rel_propagate_column_ref_statistics(sql, rel->l, e);
			if (!ne && is_join(rel->op))
				ne = rel_propagate_column_ref_statistics(sql, rel->r, e);
		} break;
		/* case op_table: later */
		case op_basetable: {
			if (e->l && (ne = exps_bind_column2(rel->exps, e->l, e->r, NULL))) {
				if ((lval = find_prop_and_get(ne->p, PROP_MAX)))
					copy_property(sql, e, PROP_MAX, lval);
				if ((lval = find_prop_and_get(ne->p, PROP_MIN)))
					copy_property(sql, e, PROP_MIN, lval);
			}
		} break;
		case op_union:
		case op_except:
		case op_inter:
		case op_project:
		case op_groupby: {
			ne = e->l ? exps_bind_column2(rel->exps, e->l, e->r, NULL) : exps_bind_column(rel->exps, e->r, NULL, NULL, 1);
			if (ne) {
				if ((lval = find_prop_and_get(ne->p, PROP_MAX)))
					copy_property(sql, e, PROP_MAX, lval);
				if ((lval = find_prop_and_get(ne->p, PROP_MIN)))
					copy_property(sql, e, PROP_MIN, lval);
			}
		} break;
		default: /* if there is a topN or sample relation in between, then the MIN and MAX values are lost */
			break;
		}
	}
	return ne;
}

static sql_exp *
rel_basetable_get_statistics(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	mvc *sql = v->sql;
	sql_column *c = NULL;

	(void)depth;
	if ((c = name_find_column(rel, e->l, e->r, -2, NULL))) {
		ValPtr min = NULL, max = NULL;

		if (has_nil(e) && mvc_has_no_nil(sql, c))
			set_has_no_nil(e);

		if ((max = mvc_has_max_value(sql, c))) {
			prop *p = e->p = prop_create(sql->sa, PROP_MAX, e->p);
			p->value = max;
		}
		if ((min = mvc_has_min_value(sql, c))) {
			prop *p = e->p = prop_create(sql->sa, PROP_MIN, e->p);
			p->value = min;
		}
	}
	return e;
}

static void
rel_set_get_statistics(mvc *sql, sql_rel *rel, sql_exp *e, int i)
{
	sql_exp *le = list_fetch(((sql_rel*)(rel->l))->exps, i);
	sql_exp *re = list_fetch(((sql_rel*)(rel->r))->exps, i);
	ValPtr lval, rval;

	assert(le && e);
	if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX))) {
		if (rel->op == op_union)
			set_max_of_values(sql, e, PROP_MAX, lval, rval); /* for union the new max will be the max of the two */
		else if (rel->op == op_inter)
			set_min_of_values(sql, e, PROP_MAX, lval, rval); /* for intersect the new max will be the min of the two */
		else
			copy_property(sql, e, PROP_MAX, lval);
	}
	if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN))) {
		if (rel->op == op_union)
			set_min_of_values(sql, e, PROP_MIN, lval, rval); /* for union the new min will be the min of the two */
		else if (rel->op == op_inter)
			set_max_of_values(sql, e, PROP_MIN, lval, rval); /* for intersect the new min will be the max of the two */
		else
			copy_property(sql, e, PROP_MIN, lval);
	}
}

static sql_exp *
rel_propagate_statistics(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	mvc *sql = v->sql;
	ValPtr lval;

	(void) depth;
	switch(e->type) {
	case e_column: {
		switch (rel->op) {
		case op_join:
		case op_left:
		case op_right:
		case op_full: {
			sql_exp *found = rel_propagate_column_ref_statistics(sql, rel->l, e);
			if (!found)
				(void) rel_propagate_column_ref_statistics(sql, rel->r, e);
		} break;
		case op_semi:
		case op_select:
		case op_project:
		case op_groupby:
			(void) rel_propagate_column_ref_statistics(sql, rel->l, e);
			break;
		case op_insert:
		case op_update:
		case op_delete:
			(void) rel_propagate_column_ref_statistics(sql, rel->r, e);
			break;
		default:
			break;
		}
	} break;
	case e_convert: {
		sql_subtype *from = exp_fromtype(e), *to = exp_totype(e);

		if (from->type->eclass == to->type->eclass) {
			sql_exp *l = e->l;
			if ((lval = find_prop_and_get(l->p, PROP_MAX))) {
				if (EC_NUMBER(from->type->eclass)) {
					ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
					VALcopy(res, lval);
					if (VALconvert(to->type->localtype, res))
						copy_property(sql, e, PROP_MAX, res);
				} else {
					copy_property(sql, e, PROP_MAX, lval);
				}
			}
			if ((lval = find_prop_and_get(l->p, PROP_MIN))) {
				if (EC_NUMBER(from->type->eclass)) {
					ValPtr res = (ValPtr) sa_zalloc(sql->sa, sizeof(ValRecord));
					VALcopy(res, lval);
					if (VALconvert(to->type->localtype, res))
						copy_property(sql, e, PROP_MIN, res);
				} else {
					copy_property(sql, e, PROP_MIN, lval);
				}
			}
		}
	} break;
	case e_aggr:
	case e_func: {
		sql_subfunc *f = e->f;

		if (!f->func->s) {
			int key = hash_key(f->func->base.name); /* Using hash lookup */
			sql_hash_e *he = sql_functions_lookup->buckets[key&(sql_functions_lookup->size-1)];
			lookup_function look = NULL;

			for (; he && !look; he = he->chain) {
				struct function_properties* fp = (struct function_properties*) he->value;

				if (!strcmp(f->func->base.name, fp->name))
					look = fp->func;
			}
			if (look)
				look(sql, e);
		}
	} break;
	case e_atom:
		if (e->l) {
			atom *a = (atom*) e->l;
			if (!a->isnull) {
				copy_property(sql, e, PROP_MAX, &a->data);
				copy_property(sql, e, PROP_MIN, &a->data);
			}
		} else if (e->f) {
			list *vals = (list *) e->f;
			sql_exp *first = vals->h ? vals->h->data : NULL;
			ValPtr max = NULL, min = NULL; /* all child values must have a valid min/max */

			if (first) {
				max = ((lval = find_prop_and_get(first->p, PROP_MAX))) ? lval : NULL;
				min = ((lval = find_prop_and_get(first->p, PROP_MIN))) ? lval : NULL;
			}

			for (node *n = vals->h ? vals->h->next : NULL ; n && min && max; n = n->next) {
				sql_exp *ee = n->data;
				ValRecord res;

				if (max) {
					if ((lval = find_prop_and_get(ee->p, PROP_MAX))) {
						VARcalcgt(&res, lval, max);
						max = res.val.btval == 1 ? lval : max;
					} else {
						max = NULL;
					}
				}
				if (min) {
					if ((lval = find_prop_and_get(ee->p, PROP_MIN))) {
						VARcalcgt(&res, min, lval);
						min = res.val.btval == 1 ? lval : min;
					} else {
						min = NULL;
					}
				}
			}

			if (max)
				copy_property(sql, e, PROP_MAX, max);
			if (min)
				copy_property(sql, e, PROP_MIN, min);
		}
		break;
	case e_cmp: /* propagating min and max of booleans is not very worth it */
	case e_psm:
		break;
	}
	return e;
}

static sql_rel *
rel_get_statistics(visitor *v, sql_rel *rel)
{
	switch(rel->op){
	case op_basetable:
		rel->exps = exps_exp_visitor_bottomup(v, rel, rel->exps, 0, &rel_basetable_get_statistics, false);
		break;
	case op_union:
	case op_inter:
	case op_except: {
		int i = 0;
		for (node *n = rel->exps->h ; n ; n = n->next) {
			rel_set_get_statistics(v->sql, rel, n->data, i);
			i++;
		}
	} break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_select:
	case op_project:
	case op_groupby:
	case op_insert:
	case op_update:
	case op_delete:
		rel->exps = exps_exp_visitor_bottomup(v, rel, rel->exps, 0, &rel_propagate_statistics, false);
		if (is_simple_project(rel->op) && !list_empty(rel->r))
			rel->r = exps_exp_visitor_bottomup(v, rel, rel->r, 0, &rel_propagate_statistics, false);
		break;
	/*These relations are less important for now
	case op_anti:
	case op_ddl:
	case op_table:
	case op_truncate:
	case op_topn:
	case op_sample:*/
	default:
		break;
	}

	return rel;
}

sql_rel *
rel_statistics(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &rel_get_statistics);
	return rel;
}
