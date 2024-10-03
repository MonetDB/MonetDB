/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_physical.h"
#include "rel_optimizer_private.h"
#include "rel_rewriter.h"
#include "rel_exp.h"
#include "rel_rel.h"

static sql_subfunc *
find_func( mvc *sql, char *name, list *exps )
{
	list * l = sa_list(sql->sa);
	node *n;

	for(n = exps->h; n; n = n->next)
		append(l, exp_subtype(n->data));
	return sql_bind_func_(sql, "sys", name, l, F_FUNC, false, true);

}

static sql_exp *
rel_find_aggr_exp(mvc *sql, sql_rel *rel, list *exps, sql_exp *e, char *name)
{
 	list *ea = e->l;
	sql_exp *a = NULL, *eae;
	node *n;

	(void)rel;
	if (list_length(ea) != 1)
		return NULL;
	eae = ea->h->data;
	if (eae->type != e_column)
		return NULL;
	for( n = exps->h; n; n = n->next) {
		a = n->data;

		if (a->type == e_aggr) {
			sql_subfunc *af = a->f;
			list *aa = a->l;

			/* TODO handle distinct and no-nil etc ! */
			if (strcmp(af->func->base.name, name) == 0 &&
				/* TODO handle count (has no args!!) */
			    aa && list_length(aa) == 1) {
				sql_exp *aae = aa->h->data;

				if (eae->type == e_column &&
				    ((!aae->l && !eae->l) ||
				    (aae->l && eae->l &&
				    strcmp(aae->l, eae->l) == 0)) &&
				    (aae->r && eae->r &&
				    strcmp(aae->r, eae->r) == 0))
					return exp_ref(sql, a);
			}
		}
	}
	return NULL;
}

static sql_exp *
find_aggr_exp(mvc *sql, list *exps, char *name)
{
	node *n;

	for( n = exps->h; n; n = n->next) {
		sql_exp *a = n->data;

		if (a->type == e_aggr) {
			sql_subfunc *af = a->f;

			if (strcmp(af->func->base.name, name) == 0)
				return exp_ref(sql, a);
		}
	}
	return NULL;
}

/* Filter out the 0's introducted by the parallel group by.
 * Must be done before using the values. */
static sql_rel *
rel_count_gt_zero(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op) && rel->parallel) {
		list *exps, *gbe;

		gbe = rel->r;
		if (!gbe || list_empty(gbe) || is_rewrite_gt_zero_used(rel->used))
			return rel;
		/* introduce select * from l where cnt > 0 */
		/* find count */
		if (list_empty(rel->exps)) /* no result expressions, just project the extends */
			rel->exps = rel_projections(sql, rel, NULL, 1, 1);
		exps = rel_projections(sql, rel, NULL, 1, 1);
		sql_exp *e = find_aggr_exp(sql, rel->exps, "count"), *ea = e;
		if (e && e->type == e_column)
			ea = exps_find_exp(rel->exps, e);
		if (!ea || !list_empty(ea->l)) {
			sql_subfunc *cf = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR, true, true);

			e = exp_aggr(sql->sa, NULL, cf, 0, 0, CARD_AGGR, 0);

			exp_label(sql->sa, e, ++sql->label);
			append(rel->exps, e);
			e = exp_ref(sql, e);
		}
		rel->used |= rewrite_gt_zero_used;
		e = exp_compare(sql->sa, e, exp_atom_lng(sql->sa, 0), cmp_notequal);
		rel = rel_select(sql->sa, rel, e);
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
		rel = rel_project(sql->sa, rel, exps);
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
	}
	return rel;
}


/* rewrite avg into sum/count */
static sql_rel *
rel_avg_rewrite(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op) && rel->parallel) {
		list *pexps, *nexps = new_exp_list(sql->sa), *avgs = new_exp_list(sql->sa);
		list *aexps = new_exp_list(sql->sa); /* alias list */
		node *m, *n;

		if (mvc_debug_on(sql, 64)) /* disable rewriter with sql_debug=64 */
			return rel;

		/* Find all avg's */
		for (m = rel->exps->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					sql_subtype *rt = exp_subtype(e);
					sql_subtype *it = first_arg_subtype(e);
					if ((EC_APPNUM(rt->type->eclass) && !EC_APPNUM(it->type->eclass)) || /* always rewrite floating point average */
						(rt->type->localtype > it->type->localtype)) {	/* always rewrite if result type is large enough */
						append(avgs, e);
						continue;
					}
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column &&
			   (!list_find_exp(rel->r, e) &&
			    !rel_find_exp(rel->l, e)))
				append(aexps, e);
			else
				append(nexps, e);
		}
		if (!list_length(avgs))
			return rel;

		/* For each avg, find count and sum */
		for (m = avgs->h; m; m = m->next) {
			list *args;
			sql_exp *avg = m->data, *navg, *cond, *cnt_d;
			sql_exp *cnt = rel_find_aggr_exp(sql, rel, nexps, avg, "count");
			sql_exp *sum = rel_find_aggr_exp(sql, rel, nexps, avg, "sum");
			sql_subfunc *div, *ifthen, *cmp;
			list *l = avg->l;
			sql_subtype *avg_input_t = exp_subtype(l->h->data);


			/* create nsum/cnt exp */
			if (!cnt) {
				sql_subfunc *cf = sql_bind_func_(sql, "sys", "count", append(sa_list(sql->sa), avg_input_t), F_AGGR, false, true);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), cf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				append(nexps, e);
				cnt = exp_ref(sql, e);
			}
			if (!sum) {
				sql_subfunc *sf = sql_bind_func_(sql, "sys", "sum", append(sa_list(sql->sa), avg_input_t), F_AGGR, false, true);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), sf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				append(nexps, e);
				sum = exp_ref(sql, e);
			}
			cnt_d = cnt;

			sql_subtype *avg_t = exp_subtype(avg);
			sql_subtype *dbl_t = sql_bind_localtype("dbl");
			if (subtype_cmp(avg_t, dbl_t) == 0 || EC_INTERVAL(avg_t->type->eclass)) {
				/* check for count = 0 (or move into funcs) */
				args = new_exp_list(sql->sa);
				append(args, cnt);
				append(args, exp_atom_lng(sql->sa, 0));
				cmp = find_func(sql, "=", args);
				assert(cmp);
				cond = exp_op(sql->sa, args, cmp);

				args = new_exp_list(sql->sa);
				append(args, cond);
				append(args, exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(cnt_d), NULL, 0)));
				/* TODO only ifthenelse if value column may have nil's*/
				append(args, cnt_d);
				ifthen = find_func(sql, "ifthenelse", args);
				assert(ifthen);
				cnt_d = exp_op(sql->sa, args, ifthen);

				if (subtype_cmp(avg_t, dbl_t) == 0) {
					cnt_d = exp_convert(sql, cnt, exp_subtype(cnt), dbl_t);
					sum = exp_convert(sql, sum, exp_subtype(sum), dbl_t);
				}

				args = new_exp_list(sql->sa);

				sql_subtype *st = exp_subtype(sum);
				sql_subtype *ct = exp_subtype(cnt_d);
				/* convert sum flt -> dbl */
				if (st->type->eclass == EC_FLT && ct->type->eclass == EC_FLT && st->type->localtype < ct->type->localtype) {
					sum = exp_convert(sql, sum, st, ct);
				} else if (st->type->eclass == EC_FLT) {
					if (ct->type != st->type) {
						sql_subtype *dbl_t = sql_bind_localtype("dbl");
						if (ct->type->eclass != EC_FLT || st->type == dbl_t->type)
							cnt_d = exp_convert(sql, cnt_d, exp_subtype(cnt_d), st);
					}
				}
				append(args, sum);
				append(args, cnt_d);
				div = find_func(sql, "sql_div", args);
				assert(div);
				navg = exp_op(sql->sa, args, div);
			} else {
				args = sa_list(sql->sa);
				append(args, sum);
				append(args, cnt_d);
				div = find_func(sql, "num_div", args);
				assert(div);
				navg = exp_op(sql->sa, args, div);
			}

			if (subtype_cmp(exp_subtype(avg), exp_subtype(navg)) != 0)
				navg = exp_convert(sql, navg, exp_subtype(navg), exp_subtype(avg));

			exp_prop_alias(sql->sa, navg, avg);
			assert(navg);
			m->data = navg;
		}
		pexps = new_exp_list(sql->sa);
		for (m = rel->exps->h, n = avgs->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					sql_subtype *rt = exp_subtype(e);
					sql_subtype *it = first_arg_subtype(e);
					if ((EC_APPNUM(rt->type->eclass) && !EC_APPNUM(it->type->eclass)) || /* always rewrite floating point average */
						(rt->type->localtype > it->type->localtype)) {	/* always rewrite if result type is large enough */
						sql_exp *avg = n->data;

						append(pexps, avg);
						n = n->next;
						continue;
					}
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column && !rel_find_exp(rel->l, e))
				append(pexps, e);
			else
				append(pexps, exp_ref(sql, e));
		}
		sql_rel *nrel = rel_groupby(sql, rel_dup(rel->l), rel->r);
		nrel->parallel = rel->parallel;
		nrel->partition = rel->partition;
		nrel->spb = rel->spb;
		set_count_prop(v->sql->sa, nrel, get_rel_count(rel));
		rel_destroy(rel);
		nrel->exps = nexps;
		rel = rel_project(sql->sa, nrel, pexps);
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
		set_processed(rel);
		v->changes++;
	}
	return rel;
}

#define IS_ORDER_BASED_AGGR(name) (strcmp((name), "quantile") == 0 || strcmp((name), "quantile_avg") == 0 || \
                                   strcmp((name), "median") == 0 || strcmp((name), "median_avg") == 0)

static sql_rel *
rel_add_orderby(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op)) {
		if (rel->exps && !rel->r) { /* find quantiles */
			sql_exp *obe = NULL, *oberef = NULL;
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (is_aggr(e->type)) {
					sql_subfunc *af = e->f;
					list *aa = e->l;

					/* for now we only handle one sort order */
					if (IS_ORDER_BASED_AGGR(af->func->base.name) && aa && list_length(aa) == 2) {
						sql_exp *nobe = aa->h->data;
						if (nobe && !obe) {
							sql_rel *l = rel->l = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rel->l, NULL, 1, 1));
							obe = nobe;
							oberef = nobe;
							if (l) {
								if (!is_alias(nobe->type)) {
									oberef = nobe = exp_label(v->sql->sa, exp_copy(v->sql, nobe), ++v->sql->label);
									append(l->exps, nobe);
								}
								set_nulls_first(nobe);
								set_ascending(nobe);
								aa->h->data = exp_ref(v->sql, nobe);
								list *o = l->r = sa_list(v->sql->sa);
								if (o)
									append(o, nobe);
							}
						} else if (exp_match_exp(nobe, obe)) {
							aa->h->data = exp_ref(v->sql, oberef);
						}
					}
				}
			}
			return rel;
		}
	}
	return rel;
}

static sql_rel *
rel_add_project(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	switch (rel->op) {
	case op_basetable:
	case op_table:
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi:
	case op_anti:

	case op_union:
	case op_inter:
	case op_except:
		rel->l = rel_add_project(sql, rel->l);
		rel->r = rel_add_project(sql, rel->r);
		if (is_join(rel->op) && !rel_is_ref(rel))
			rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		rel->l = rel_add_project(sql, rel->l);
		if (is_select(rel->op) && !rel_is_ref(rel))
			rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
		break;
	case op_ddl:
		rel->l = rel_add_project(sql, rel->l);
		if (rel->r)
			rel->r = rel_add_project(sql, rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
	case op_merge:
		rel->r = rel_add_project(sql, rel->r);
		break;
	case op_munion:
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			n->data = rel_add_project(sql, n->data);
	}

	if (0 && rel_is_ref(rel) && !is_project(rel->op)) {
		/* ughly inplace */
		sql_rel *n = rel_create(sql->sa);

		*n = *rel;
		n->ref.refcnt = 1;
		rel->op = op_project;
		rel->l = n;
		rel->r = NULL;
		rel->exps = rel_projections(sql, n, NULL, 1, 1);
	}
	return rel;
}

static sql_exp *
exp_timezone(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)depth;
	(void)rel;
	if (e && e->type == e_func) {
		list *l = e->l;
		sql_subfunc *f = e->f;
		const char *fname = f->func->base.name;
		if (list_length(l) == 2) {
		   if (strcmp(fname, "timestamp_to_str") == 0 || strcmp(fname, "time_to_str") == 0) {
                sql_exp *e = l->h->data;
                sql_subtype *t = exp_subtype(e);
                if (t->type->eclass == EC_TIMESTAMP_TZ || t->type->eclass == EC_TIME_TZ) {
                    sql_exp *offset = exp_atom_lng(v->sql->sa, v->sql->timezone);
                    list_append(l, offset);
                }
            } else if (strcmp(fname, "str_to_timestamp") == 0 || strcmp(fname, "str_to_time") == 0 || strcmp(fname, "str_to_date") == 0) {
                sql_exp *offset = exp_atom_lng(v->sql->sa, v->sql->timezone);
                list_append(l, offset);
            }
		}
	}
	return e;
}

sql_rel *
rel_physical(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &rel_add_orderby);
	rel = rel_visitor_bottomup(&v, rel, &rel_avg_rewrite);
	rel = rel_visitor_bottomup(&v, rel, &rel_count_gt_zero);
	rel = rel_add_project(sql, rel);
	rel = rel_dce(&v, NULL, rel);
	rel = rel_exp_visitor_topdown(&v, rel, &exp_timezone, true);

#ifdef HAVE_HGE
	if (rel && sql->no_int128) {
		sql_rel *r = rel;
		if (is_topn(r->op))
				r = r->l;
		if (r && is_project(r->op) && !list_empty(r->exps)) {
			for (node *n = r->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_subtype(e)->type->localtype == TYPE_hge) /* down cast */
					e = n->data = exp_convert(sql, e, exp_subtype(e), sql_bind_localtype("lng"));
			}
		}
	}
#endif
	return rel;
}
