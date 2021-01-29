/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_statistics.h"
#include "rel_optimizer.h"
#include "rel_rewriter.h"

static sql_exp *
comparison_find_column(sql_exp *input, sql_exp *e)
{
	switch (input->type) {
		case e_convert:
			return comparison_find_column(input->l, e) ? input : NULL;
		case e_column:
			return exp_match(e, input) ? input : NULL;
		default:
			return NULL;
	}
}

static sql_exp *
rel_propagate_column_ref_statistics(mvc *sql, sql_rel *rel, sql_exp *e)
{
	assert(e->type == e_column);
	if (rel) {
		switch(rel->op) {
		case op_left:
		case op_right:
		case op_full:
		case op_join:
		case op_select:
		case op_anti:
		case op_semi: {
			bool found_without_semantics = false, found_left = false, found_right = false;

			/* propagate from the bottom first */
			if (rel_propagate_column_ref_statistics(sql, rel->l, e))
				found_left = true;
			if (!found_left && is_join(rel->op) && rel_propagate_column_ref_statistics(sql, rel->r, e))
				found_right = true;

			if (!found_left && !found_right)
				return NULL;
			if (!list_empty(rel->exps) && rel->op != op_anti) { /* if there's an or, the MIN and MAX get difficult to propagate */
				for (node *n = rel->exps->h ; n ; n = n->next) {
					sql_exp *comp = n->data, *le = comp->l, *lne = NULL, *re = comp->r, *rne = NULL, *fe = comp->f, *fne = NULL;

					if (comp->type == e_cmp) {
						int flag = comp->flag & ~CMP_BETWEEN;

						if (is_theta_exp(flag) && ((lne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e)) || (fe && (fne = comparison_find_column(fe, e))))) {
							atom *lval_min = find_prop_and_get(le->p, PROP_MIN), *lval_max = find_prop_and_get(le->p, PROP_MAX), *rval_min = find_prop_and_get(re->p, PROP_MIN),
								 *rval_max = find_prop_and_get(re->p, PROP_MAX), *fval_min = fe ? find_prop_and_get(fe->p, PROP_MIN) : NULL, *fval_max = fe ? find_prop_and_get(fe->p, PROP_MAX) : NULL;

							found_without_semantics |= !comp->semantics;
							if (is_full(rel->op) || (is_left(rel->op) && found_left) || (is_right(rel->op) && found_right)) /* on outer joins, min and max cannot be propagated on some cases */
								continue;
							/* if (end2 >= start1 && start2 <= end1) then the 2 intervals are intersected */
							if (fe && lval_min && lval_max) { /* range case, the middle expression must intersect the other two */
								int int1 = rval_min && rval_max && atom_cmp(rval_max, lval_min) >= 0 && atom_cmp(rval_min, lval_max) <= 0,
									int2 = fval_min && fval_max && atom_cmp(fval_max, lval_min) >= 0 && atom_cmp(fval_min, lval_max) <= 0,
									symmetric = comp->flag & CMP_SYMMETRIC;

								if (comp->anti || (!symmetric && atom_cmp(fval_min, rval_max) < 0)) /* for asymmetric case the re range must be after the fe range */
									continue;
								if (lne && int1 && int2) {
									if (symmetric) {
										prop *p1 = find_prop(e->p, PROP_MIN), *p2 = find_prop(e->p, PROP_MAX);
										atom *nmin = statistics_atom_min(sql, rval_min, fval_min), *nmax = statistics_atom_max(sql, rval_max, fval_max);
										/* min is max from le and (min from re and fe min) */
										set_property(sql, e, PROP_MIN, p1 ? statistics_atom_max(sql, nmin, p1->value) : nmin);
										/* max is min from le and (max from re and fe max) */
										set_property(sql, e, PROP_MAX, p2 ? statistics_atom_min(sql, nmax, p2->value) : nmax);
									} else {
										prop *p1 = find_prop(e->p, PROP_MIN), *p2 = find_prop(e->p, PROP_MAX);
										/* min is max from le and re min */
										set_property(sql, e, PROP_MIN, p1 ? statistics_atom_max(sql, rval_min, p1->value) : rval_min);
										/* max is min from le and fe max */
										set_property(sql, e, PROP_MAX, p2 ? statistics_atom_min(sql, fval_max, p2->value) : fval_max);
									}
								} else if (rne) {
									if (symmetric && int1 && int2) { /* min is max from le and (min from re and fe min) */
										prop *p = find_prop(e->p, PROP_MIN);
										atom *nmin = p ? statistics_atom_min(sql, p->value, fval_min) : fval_min;
										set_property(sql, e, PROP_MIN, statistics_atom_max(sql, nmin, lval_min));
									} else if (int1) { /* min is max from le and re min */
										prop *p = find_prop(e->p, PROP_MIN);
										set_property(sql, e, PROP_MIN, p ? statistics_atom_max(sql, lval_min, p->value) : lval_min);
									}
								} else if (fne) {
									if (symmetric && int1 && int2) { /* max is min from le and (max from re and fe max) */
										prop *p = find_prop(e->p, PROP_MAX);
										atom *nmax = p ? statistics_atom_max(sql, p->value, rval_max) : rval_max;
										set_property(sql, e, PROP_MAX, p ? statistics_atom_min(sql, nmax, lval_max) : nmax);
									} else if (int2) { /* max is min from le and fe max */
										prop *p = find_prop(e->p, PROP_MAX);
										set_property(sql, e, PROP_MAX, p ? statistics_atom_min(sql, lval_max, p->value) : lval_max);
									}
								}
							} else if (lval_min && lval_max && rval_min && rval_max && atom_cmp(rval_max, lval_min) >= 0 && atom_cmp(rval_min, lval_max) <= 0) {
								/* both min and max must be set and the intervals must overlap */
								switch (comp->flag) {
								case cmp_equal: { /* for equality reduce */
									set_property(sql, e, PROP_MAX, comp->anti ? statistics_atom_max(sql, lval_max, rval_max) : statistics_atom_min(sql, lval_max, rval_max));
									set_property(sql, e, PROP_MIN, comp->anti ? statistics_atom_min(sql, lval_min, rval_min) : statistics_atom_max(sql, lval_min, rval_min));
								} break;
								case cmp_notequal: { /* for inequality expand */
									set_property(sql, e, PROP_MAX, comp->anti ? statistics_atom_min(sql, lval_max, rval_max) : statistics_atom_max(sql, lval_max, rval_max));
									set_property(sql, e, PROP_MIN, comp->anti ? statistics_atom_max(sql, lval_min, rval_min) : statistics_atom_min(sql, lval_min, rval_min));
								} break;
								case cmp_gt:
								case cmp_gte: {
									if (!comp->anti && lne) { /* min is max from both min */
										prop *p = find_prop(e->p, PROP_MIN);
										set_property(sql, e, PROP_MIN, p ? statistics_atom_max(sql, rval_min, p->value) : rval_min);
									} else if (!comp->anti) { /* max is min from both max */
										prop *p = find_prop(e->p, PROP_MAX);
										set_property(sql, e, PROP_MAX, p ? statistics_atom_min(sql, lval_max, p->value) : lval_max);
									}
								} break;
								case cmp_lt:
								case cmp_lte: {
									if (!comp->anti && lne) { /* max is min from both max */
										prop *p = find_prop(e->p, PROP_MAX);
										set_property(sql, e, PROP_MAX, p ? statistics_atom_min(sql, rval_max, p->value) : rval_max);
									} else if (!comp->anti) { /* min is max from both min */
										prop *p = find_prop(e->p, PROP_MIN);
										set_property(sql, e, PROP_MIN, p ? statistics_atom_max(sql, lval_min, p->value) : lval_min);
									}
								} break;
								default: /* Maybe later I can do cmp_in and cmp_notin */
									break;
								}
							}
						}
					}
				}
			}
			if (is_full(rel->op) || (is_left(rel->op) && found_right) || (is_right(rel->op) && found_left))
				set_has_nil(e);
			if (!is_outerjoin(rel->op) && found_without_semantics) /* at an outer join, null values pass */
				set_has_no_nil(e);
			return e;
		}
		case op_table:
		case op_basetable:
		case op_union:
		case op_except:
		case op_inter:
		case op_project:
		case op_groupby: {
			sql_exp *found;
			atom *fval;
			if ((found = rel_find_exp(rel, e)) && rel->op != op_table) {
				if ((fval = find_prop_and_get(found->p, PROP_MAX)))
					set_property(sql, e, PROP_MAX, fval);
				if ((fval = find_prop_and_get(found->p, PROP_MIN)))
					set_property(sql, e, PROP_MIN, fval);
				if (!has_nil(found))
					set_has_no_nil(e);
				return e;
			}
			return NULL;
		}
		case op_topn:
		case op_sample:
			 return rel_propagate_column_ref_statistics(sql, rel->l, e);
		default:
			break;
		}
	}
	return NULL;
}

static atom *
atom_from_valptr( sql_allocator *sa, sql_subtype *tpe, ValPtr pt)
{
	atom *a = SA_ZNEW(sa, atom);

	a->tpe = *tpe;
	a->data.vtype = tpe->type->localtype;
	if (ATOMstorage(a->data.vtype) == TYPE_str) {
		if (VALisnil(pt)) {
			VALset(&a->data, a->data.vtype, (ptr) ATOMnilptr(a->data.vtype));
		} else {
			a->data.val.sval = sa_strdup(sa, pt->val.sval);
			a->data.len = strlen(a->data.val.sval);
		}
	} else {
		VALcopy(&a->data, pt);
	}
	a->isnull = VALisnil(&a->data);
	return a;
}

static sql_exp *
rel_basetable_get_statistics(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	mvc *sql = v->sql;
	sql_column *c = NULL;

	(void)depth;
	if ((c = name_find_column(rel, exp_relname(e), exp_name(e), -2, NULL))) {
		ValPtr min = NULL, max = NULL;

		if (has_nil(e) && mvc_has_no_nil(sql, c))
			set_has_no_nil(e);
 
		if (EC_NUMBER(c->type.type->eclass) || EC_VARCHAR(c->type.type->eclass) || EC_TEMP_NOFRAC(c->type.type->eclass) || c->type.type->eclass == EC_DATE) {
			if ((max = mvc_has_max_value(sql, c))) {
				prop *p = e->p = prop_create(sql->sa, PROP_MAX, e->p);
				p->value = atom_from_valptr(sql->sa, &c->type, max);
			}
			if ((min = mvc_has_min_value(sql, c))) {
				prop *p = e->p = prop_create(sql->sa, PROP_MIN, e->p);
				p->value = atom_from_valptr(sql->sa, &c->type, min);
			}
		}
	}
	return e;
}

static void
rel_setop_get_statistics(mvc *sql, sql_rel *rel, list *lexps, list *rexps, sql_exp *e, int i)
{
	sql_exp *le = list_fetch(lexps, i), *re = list_fetch(rexps, i);
	atom *lval, *rval;

	assert(le && re && e);
	if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX))) {
		if (is_union(rel->op))
			set_property(sql, e, PROP_MAX, statistics_atom_max(sql, lval, rval)); /* for union the new max will be the max of the two */
		else if (is_inter(rel->op))
			set_property(sql, e, PROP_MAX, statistics_atom_min(sql, lval, rval)); /* for intersect the new max will be the min of the two */
		else /* except */
			set_property(sql, e, PROP_MAX, lval);
	}
	if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN))) {
		if (is_union(rel->op))
			set_property(sql, e, PROP_MIN, statistics_atom_min(sql, lval, rval)); /* for union the new min will be the min of the two */
		else if (is_inter(rel->op))
			set_property(sql, e, PROP_MIN, statistics_atom_max(sql, lval, rval)); /* for intersect the new min will be the max of the two */
		else /* except */
			set_property(sql, e, PROP_MIN, lval);
	}

	if (is_union(rel->op)) {
		if (!has_nil(le) && !has_nil(re))
			set_has_no_nil(e);
	} else if (is_inter(rel->op)) {
		if (!has_nil(le) || !has_nil(re))
			set_has_no_nil(e);
	} else {
		assert(is_except(rel->op));
		if (!has_nil(le))
			set_has_no_nil(e);
	}
}

static sql_exp *
rel_propagate_statistics(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	mvc *sql = v->sql;
	atom *lval;

	(void) depth;
	switch(e->type) {
	case e_column: {
		switch (rel->op) {
		case op_join:
		case op_left:
		case op_right:
		case op_full:
		case op_semi:
		case op_anti: {
			sql_exp *found = rel_propagate_column_ref_statistics(sql, rel->l, e);
			if (!found)
				(void) rel_propagate_column_ref_statistics(sql, rel->r, e);
		} break;
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
		sql_subtype *to = exp_totype(e), *from = exp_fromtype(e);
		sql_exp *l = e->l;

		if (EC_NUMBER(from->type->eclass) && EC_NUMBER(to->type->eclass)) {
			if ((lval = find_prop_and_get(l->p, PROP_MAX))) {
				atom *res = atom_dup(sql->sa, lval);
				if (atom_cast(sql->sa, res, to))
					set_property(sql, e, PROP_MAX, res);
			}
			if ((lval = find_prop_and_get(l->p, PROP_MIN))) {
				atom *res = atom_dup(sql->sa, lval);
				if (atom_cast(sql->sa, res, to))
					set_property(sql, e, PROP_MIN, res);
			}
		}
		if (!has_nil(l))
			set_has_no_nil(e);
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
		if (!e->semantics && e->l && !have_nil(e->l))
			set_has_no_nil(e);
	} break;
	case e_atom: {
		if (e->l) {
			atom *a = (atom*) e->l;
			if (!a->isnull) {
				set_property(sql, e, PROP_MAX, a);
				set_property(sql, e, PROP_MIN, a);
			}
		} else if (e->f) {
			list *vals = (list *) e->f;
			sql_exp *first = vals->h ? vals->h->data : NULL;
			atom *max = NULL, *min = NULL; /* all child values must have a valid min/max */

			if (first) {
				max = ((lval = find_prop_and_get(first->p, PROP_MAX))) ? lval : NULL;
				min = ((lval = find_prop_and_get(first->p, PROP_MIN))) ? lval : NULL;
			}

			for (node *n = vals->h ? vals->h->next : NULL ; n ; n = n->next) {
				sql_exp *ee = n->data;

				if (max) {
					if ((lval = find_prop_and_get(ee->p, PROP_MAX))) {
						max = atom_cmp(lval, max) > 0 ? lval : max;
					} else {
						max = NULL;
					}
				}
				if (min) {
					if ((lval = find_prop_and_get(ee->p, PROP_MIN))) {
						min = atom_cmp(min, lval) > 0 ? lval : min;
					} else {
						min = NULL;
					}
				}
			}

			if (max)
				set_property(sql, e, PROP_MAX, max);
			if (min)
				set_property(sql, e, PROP_MIN, min);
		}
	} break;
	case e_cmp:
		/* propagating min and max of booleans is not very worth it */
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			if (!have_nil(e->l) && !have_nil(e->r))
				set_has_no_nil(e);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_exp *le = e->l;
			if (!has_nil(le) && !have_nil(e->r))
				set_has_no_nil(e);
		} else {
			sql_exp *le = e->l, *re = e->r, *fe = e->f;
			if (!has_nil(le) && !has_nil(re) && (!e->f || !has_nil(fe)))
				set_has_no_nil(e);
		}
		break;
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
		sql_rel *l = rel->l, *r = rel->r;

		while (is_sample(l->op) || is_topn(l->op)) /* skip topN and sample relations in the middle */
			l = l->l;
		while (is_sample(r->op) || is_topn(r->op))
			r = r->l;
		/* if it's not a projection, then project and propagate statistics */
		if (!is_project(l->op) && !is_base(l->op)) {
			l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 0, 1));
			l->exps = exps_exp_visitor_bottomup(v, l, l->exps, 0, &rel_propagate_statistics, false);
		}
		if (!is_project(r->op) && !is_base(r->op)) {
			r = rel_project(v->sql->sa, r, rel_projections(v->sql, r, NULL, 0, 1));
			r->exps = exps_exp_visitor_bottomup(v, r, r->exps, 0, &rel_propagate_statistics, false);
		}

		for (node *n = rel->exps->h ; n ; n = n->next) {
			rel_setop_get_statistics(v->sql, rel, l->exps, r->exps, n->data, i);
			i++;
		}
	} break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_select:
	case op_project:
	case op_groupby:
	case op_ddl:
		rel->exps = exps_exp_visitor_bottomup(v, rel, rel->exps, 0, &rel_propagate_statistics, false);
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && !list_empty(rel->r))
			rel->r = exps_exp_visitor_bottomup(v, rel, rel->r, 0, &rel_propagate_statistics, false);
		break;
	/*These relations are less important for now
	case op_table:
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
	case op_topn:
	case op_sample:*/
	default:
		break;
	}

	return rel;
}

static sql_rel *
rel_simplify_count(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;

	if (is_groupby(rel->op)) {
		int ncountstar = 0;

		/* Convert count(no null) into count(*) */
		for (node *n = rel->exps->h; n ; n = n->next) {
			sql_exp *e = n->data;

			if (exp_aggr_is_count(e) && !need_distinct(e)) {
				if (list_length(e->l) == 0) {
					ncountstar++;
				} else if (list_length(e->l) == 1 && !has_nil((sql_exp*)((list*)e->l)->h->data)) {
					sql_subfunc *cf = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
					sql_exp *ne = exp_aggr(sql->sa, NULL, cf, 0, 0, e->card, 0);
					if (exp_name(e))
						exp_prop_alias(sql->sa, ne, e);
					n->data = ne;
					ncountstar++;
					v->changes++;
				}
			}
		}
		/* With multiple count(*), use exp_ref to reduce the number of calls to this aggregate */
		if (ncountstar > 1) {
			sql_exp *count_star = NULL;
			for (node *n = rel->exps->h; n ; n = n->next) {
				sql_exp *e = n->data;

				if (exp_aggr_is_count(e) && !need_distinct(e) && list_length(e->l) == 0) {
					if (!count_star) {
						count_star = e;
					} else {
						sql_exp *ne = exp_ref(sql, count_star);
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						n->data = ne;
					}
				}
			}
		}
	}
	return rel;
}

static sql_exp * /* Remove predicates always false from min/max values */
rel_prune_predicates(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void) rel;
	(void) depth;
	if (e->type == e_cmp && (is_theta_exp(e->flag) || e->f)) {
		sql_exp *le = e->l, *re = e->r, *fe = e->f;
		atom *lval_min = find_prop_and_get(le->p, PROP_MIN), *lval_max = find_prop_and_get(le->p, PROP_MAX),
			 *rval_min = find_prop_and_get(re->p, PROP_MIN), *rval_max = find_prop_and_get(re->p, PROP_MAX);
		bool always_false = false, always_true = false;

		if (fe && !(e->flag & CMP_SYMMETRIC)) {
			atom *fval_min = find_prop_and_get(fe->p, PROP_MIN), *fval_max = find_prop_and_get(fe->p, PROP_MAX);
			comp_type lower = range2lcompare(e->flag), higher = range2rcompare(e->flag);
			int not_int1 = rval_min && lval_max && !rval_min->isnull && !lval_max->isnull && /* the middle and left intervals don't overlap */
				(!e->anti && (lower == cmp_gte ? atom_cmp(rval_min, lval_max) > 0 : atom_cmp(rval_min, lval_max) >= 0)),
				not_int2 = lval_min && fval_max && !lval_min->isnull && !fval_max->isnull && /* the middle and right intervals don't overlap */
				(!e->anti && (higher == cmp_lte ? atom_cmp(lval_min, fval_max) > 0 : atom_cmp(lval_min, fval_max) >= 0)),
				not_int3 = rval_min && fval_max && !rval_min->isnull && !fval_max->isnull && /* the left interval is after the right one */
				(!e->anti && (atom_cmp(rval_min, fval_max) > 0));

			always_false |= not_int1 || not_int2 || not_int3;
			/* for anti the middle must be before the left or after the right or the right after the left, for the other the middle must be always between the left and right intervals */
			always_true |= exp_is_not_null(le) && exp_is_not_null(re) && exp_is_not_null(fe) &&
				lval_min && lval_max && rval_min && rval_max && fval_min && fval_max && !lval_min->isnull && !lval_max->isnull && !rval_min->isnull && !rval_max->isnull && !fval_min->isnull && !fval_max->isnull &&
				(e->anti ? ((lower == cmp_gte ? atom_cmp(rval_min, lval_max) > 0 : atom_cmp(rval_min, lval_max) >= 0) || (higher == cmp_lte ? atom_cmp(lval_min, fval_max) > 0 : atom_cmp(lval_min, fval_max) >= 0) || atom_cmp(rval_min, fval_max) > 0) :
				((lower == cmp_gte ? atom_cmp(lval_min, rval_max) >= 0 : atom_cmp(lval_min, rval_max) > 0) && (higher == cmp_lte ? atom_cmp(fval_min, lval_max) >= 0 : atom_cmp(fval_min, lval_max) > 0)));
		} else {
			switch (e->flag) {
			case cmp_equal:
				if (lval_min && lval_max && rval_min && rval_max && !lval_min->isnull && !lval_max->isnull && !rval_min->isnull && !rval_max->isnull)
					always_false |= (!e->anti && (atom_cmp(rval_max, lval_min) < 0 || atom_cmp(rval_min, lval_max) > 0)) || (e->anti && atom_cmp(lval_min, rval_min) == 0 && atom_cmp(lval_max, rval_max) <= 0);
				if (is_semantics(e))
					always_false |= is_semantics(e) ?
						e->anti ? (exp_is_null(le) && exp_is_null(re)) || (exp_is_not_null(le) && exp_is_not_null(re)) : (exp_is_not_null(le) && exp_is_null(re)) || (exp_is_null(le) && exp_is_not_null(re)) :
						e->anti ? exp_is_not_null(le) && exp_is_not_null(re) : (exp_is_null(le) && exp_is_null(re)) || (exp_is_not_null(le) && exp_is_null(re)) || (exp_is_null(le) && exp_is_not_null(re));
				break;
			case cmp_gt:
				if (lval_max && rval_min && !lval_max->isnull && !rval_min->isnull)
					always_false |= e->anti ? atom_cmp(lval_max, rval_min) > 0 : atom_cmp(lval_max, rval_min) <= 0;
				if (lval_min && rval_max && !lval_min->isnull && !rval_max->isnull)
					always_true |= exp_is_not_null(le) && exp_is_not_null(re) && (e->anti ? atom_cmp(lval_min, rval_max) <= 0 : atom_cmp(lval_min, rval_max) > 0);
				break;
			case cmp_gte:
				if (lval_max && rval_min && !lval_max->isnull && !rval_min->isnull)
					always_false |= e->anti ? atom_cmp(lval_max, rval_min) >= 0 : atom_cmp(lval_max, rval_min) < 0;
				if (lval_min && rval_max && !lval_min->isnull && !rval_max->isnull)
					always_true |= exp_is_not_null(le) && exp_is_not_null(re) && (e->anti ? atom_cmp(lval_min, rval_max) < 0 : atom_cmp(lval_min, rval_max) >= 0);
				break;
			case cmp_lt:
				if (lval_min && rval_max && !lval_min->isnull && !rval_max->isnull)
					always_false |= e->anti ? atom_cmp(lval_min, rval_max) < 0 : atom_cmp(lval_min, rval_max) >= 0;
				if (lval_max && rval_min && !lval_max->isnull && !rval_min->isnull)
					always_true |= exp_is_not_null(le) && exp_is_not_null(re) && (e->anti ? atom_cmp(lval_max, rval_min) >= 0 : atom_cmp(lval_max, rval_min) < 0);
				break;
			case cmp_lte:
				if (lval_min && rval_max && !lval_min->isnull && !rval_max->isnull)
					always_false |= e->anti ? atom_cmp(lval_min, rval_max) <= 0 : atom_cmp(lval_min, rval_max) > 0;
				if (lval_max && rval_min && !lval_max->isnull && !rval_min->isnull)
					always_true |= exp_is_not_null(le) && exp_is_not_null(re) && (e->anti ? atom_cmp(lval_max, rval_min) > 0 : atom_cmp(lval_max, rval_min) <= 0);
				break;
			default: /* Maybe later I can do cmp_in and cmp_notin */
				break;
			}
		}
		assert(!always_false || !always_true);
		if (always_false || always_true) {
			sql_exp *ne = exp_atom_bool(v->sql->sa, always_true);
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, ne, e);
			e = ne;
			v->changes++;
		}
	}
	return e;
}

sql_rel *
rel_statistics(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql, .value_based_opt = 1, .storage_based_opt = 1 }, ev = { .sql = sql, .value_based_opt = 1, .storage_based_opt = 1 };
	global_props gp = (global_props) {.cnt = {0},};
	rel_properties(sql, &gp, rel);

	rel = rel_visitor_bottomup(&v, rel, &rel_get_statistics);
	if (gp.cnt[op_groupby])
		rel = rel_visitor_bottomup(&v, rel, &rel_simplify_count);
	if (gp.cnt[op_select] || gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full] || gp.cnt[op_anti] || gp.cnt[op_join] || gp.cnt[op_semi]) {
		int cur_changes = v.changes;
		rel = rel_exp_visitor_bottomup(&v, rel, &rel_prune_predicates, false);
		if (v.changes > cur_changes) { /* there were changes by rel_prune_predicates, run rewrite_simplify */
			rel = rel_visitor_bottomup(&ev, rel, &rewrite_simplify);
			if (gp.cnt[op_select])
				rel = rel_visitor_bottomup(&ev, rel, &rel_remove_empty_select);
			rel = rel_dce(sql, rel);
		}
	}

	return rel;
}
