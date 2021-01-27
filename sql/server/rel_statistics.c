
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
	atom *lval, *rval;
	sql_exp *found = NULL;
	prop *p;

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
			bool found_with_semantics = false, found_left = false, found_right = false;

			/* propagate from the bottom first */
			if (rel_propagate_column_ref_statistics(sql, rel->l, e))
				found_left = true;
			if (!found_left && is_join(rel->op) && rel_propagate_column_ref_statistics(sql, rel->r, e))
				found_right = true;

			assert(found_left || found_right);
			if (!list_empty(rel->exps) && rel->op != op_anti) { /* if there's an or, the MIN and MAX get difficult to propagate */
				for (node *n = rel->exps->h ; n ; n = n->next) {
					sql_exp *comp = n->data;

					if (comp->type == e_cmp) {
						int flag = comp->flag & ~CMP_BETWEEN;

						switch (flag) {
						case cmp_equal: {
							sql_exp *le = comp->l, *lne = NULL, *re = comp->r, *rne = NULL;

							if ((lne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e))) {
								if (e->semantics)
									found_with_semantics = true;
								if (!is_outerjoin(rel->op)) {
									if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX)))
										set_property(sql, e, PROP_MAX, e->anti ? atom_max(lval, rval) : atom_min(lval, rval)); /* for equality reduce */
									if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN)))
										set_property(sql, e, PROP_MIN, e->anti ? atom_min(lval, rval) : atom_max(lval, rval));
								}
							}
							found = found ? found : lne ? lne : rne;
						} break;
						case cmp_notequal: {
							sql_exp *le = comp->l, *lne = NULL, *re = comp->r, *rne = NULL;

							if ((lne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e))) {
								if (e->semantics)
									found_with_semantics = true;
								if (!is_outerjoin(rel->op)) {
									if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX)))
										set_property(sql, e, PROP_MAX, e->anti ? atom_min(lval, rval) : atom_max(lval, rval)); /* for inequality expand */
									if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN)))
										set_property(sql, e, PROP_MIN, e->anti ? atom_max(lval, rval) : atom_min(lval, rval));
								}
							}
							found = found ? found : lne ? lne : rne;
						} break;
						case cmp_gt:
						case cmp_gte: {
							sql_exp *le = comp->l, *lne = NULL, *re = comp->r, *rne = NULL, *fe = comp->f, *fne = NULL;

							if ((lne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e)) || (fe && (fne = comparison_find_column(fe, e)))) {
								if (e->semantics)
									found_with_semantics = true;
								if (!is_outerjoin(rel->op)) {
									if (lne) {
										if (fe) { /* range case */
											if (comp->flag & CMP_SYMMETRIC) {
												if ((lval = find_prop_and_get(re->p, PROP_MIN)) && (rval = find_prop_and_get(fe->p, PROP_MIN))) {
													atom *nmin = atom_min(lval, rval);
													p = find_prop(e->p, PROP_MAX);
													set_property(sql, e, PROP_MAX, p ? atom_max(nmin, p->value) : nmin);
												}
												if ((lval = find_prop_and_get(re->p, PROP_MAX)) && (rval = find_prop_and_get(fe->p, PROP_MAX))) {
													atom *nmax = atom_max(lval, rval);
													p = find_prop(e->p, PROP_MIN);
													set_property(sql, e, PROP_MIN, p ? atom_min(nmax, p->value) : nmax);
												}
											} else {
												if ((lval = find_prop_and_get(fe->p, PROP_MIN)))
													set_max_property(sql, e, lval);
												if ((rval = find_prop_and_get(re->p, PROP_MAX)))
													set_min_property(sql, e, rval);
											}
										} else {
											if ((rval = find_prop_and_get(re->p, PROP_MAX)))
												set_min_property(sql, e, rval);
										}
									} else if (rne) {
										if (fe) { /* range case */
											if (comp->flag & CMP_SYMMETRIC) {
												if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(fe->p, PROP_MIN))) {
													atom *nmin = atom_min(lval, rval);
													p = find_prop(e->p, PROP_MAX);
													set_property(sql, e, PROP_MAX, p ? atom_max(nmin, p->value) : nmin);
												}
												if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(fe->p, PROP_MAX))) {
													atom *nmax = atom_max(lval, rval);
													p = find_prop(e->p, PROP_MIN);
													set_property(sql, e, PROP_MIN, p ? atom_min(nmax, p->value) : nmax);
												}
											} else {
												if ((rval = find_prop_and_get(le->p, PROP_MIN)))
													set_max_property(sql, e, rval);
											}
										} else {
											if ((lval = find_prop_and_get(le->p, PROP_MIN)))
												set_max_property(sql, e, lval);
										}
									} else { /* range case */
										assert(fe);
										if (comp->flag & CMP_SYMMETRIC) {
											if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN))) {
												atom *nmin = atom_min(lval, rval);
												p = find_prop(e->p, PROP_MAX);
												set_property(sql, e, PROP_MAX, p ? atom_max(nmin, p->value) : nmin);
											}
											if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX))) {
												atom *nmax = atom_max(lval, rval);
												p = find_prop(e->p, PROP_MIN);
												set_property(sql, e, PROP_MIN, p ? atom_min(nmax, p->value) : nmax);
											}
										} else {
											if ((rval = find_prop_and_get(le->p, PROP_MAX)))
												set_min_property(sql, e, rval);
										}
									}
								}
							}
							found = found ? found : lne ? lne : rne ? rne : fne;
						} break;
						case cmp_lt:
						case cmp_lte: {
							sql_exp *le = comp->l, *lne = NULL, *re = comp->r, *rne = NULL;

							assert(!comp->f);
							if ((lne = comparison_find_column(le, e)) || (rne = comparison_find_column(re, e))) {
								if (e->semantics)
									found_with_semantics = true;
								if (!is_outerjoin(rel->op)) {
									if (lne) {
										if ((lval = find_prop_and_get(re->p, PROP_MIN)))
											set_max_property(sql, e, lval);
									} else {
										if ((rval = find_prop_and_get(le->p, PROP_MAX)))
											set_min_property(sql, e, rval);
									}
								}
							}
							found = found ? found : lne ? lne : rne;
						} break;
						default: /* Maybe later I can do cmp_in and cmp_notin */
							break;
						}
					}
				}
			}
			if (found) {
				/* if semantics flag was found, null values will pass */
				if (is_full(rel->op) || (is_left(rel->op) && found_right) || (is_right(rel->op) && found_left) || (has_nil(e) && found_with_semantics))
					set_has_nil(e);
				else if (!has_nil(e) || !is_outerjoin(rel->op)) /* at an outer join, null values pass */
					set_has_no_nil(e);
			}
			return e;
		}
		case op_table:
		case op_basetable:
		case op_union:
		case op_except:
		case op_inter:
		case op_project:
		case op_groupby:
			if ((found = rel_find_exp(rel, e)) && rel->op != op_table) {
				if ((lval = find_prop_and_get(found->p, PROP_MAX)))
					set_property(sql, e, PROP_MAX, lval);
				if ((lval = find_prop_and_get(found->p, PROP_MIN)))
					set_property(sql, e, PROP_MIN, lval);
				if (!has_nil(found))
					set_has_no_nil(e);
			}
			return e;
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
rel_setop_get_statistics(mvc *sql, sql_rel *rel, sql_exp *e, int i)
{
	sql_exp *le = list_fetch(((sql_rel*)(rel->l))->exps, i);
	sql_exp *re = list_fetch(((sql_rel*)(rel->r))->exps, i);
	atom *lval, *rval;

	assert(le && e);
	if ((lval = find_prop_and_get(le->p, PROP_MAX)) && (rval = find_prop_and_get(re->p, PROP_MAX))) {
		if (is_union(rel->op))
			set_property(sql, e, PROP_MAX, atom_max(lval, rval)); /* for union the new max will be the max of the two */
		else if (is_inter(rel->op))
			set_property(sql, e, PROP_MAX, atom_min(lval, rval)); /* for intersect the new max will be the min of the two */
		else /* except */
			set_property(sql, e, PROP_MAX, lval);
	}
	if ((lval = find_prop_and_get(le->p, PROP_MIN)) && (rval = find_prop_and_get(re->p, PROP_MIN))) {
		if (is_union(rel->op))
			set_property(sql, e, PROP_MIN, atom_min(lval, rval)); /* for union the new min will be the min of the two */
		else if (is_inter(rel->op))
			set_property(sql, e, PROP_MIN, atom_max(lval, rval)); /* for intersect the new min will be the max of the two */
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
		for (node *n = rel->exps->h ; n ; n = n->next) {
			rel_setop_get_statistics(v->sql, rel, n->data, i);
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
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && !list_empty(rel->r))
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

sql_rel *
rel_statistics(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };
	global_props gp = (global_props) {.cnt = {0},};
	rel_properties(sql, &gp, rel);

	rel = rel_visitor_bottomup(&v, rel, &rel_get_statistics);
	if (gp.cnt[op_groupby])
		rel = rel_visitor_bottomup(&v, rel, &rel_simplify_count);
	return rel;
}
