/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_physical.h"
#include "rel_exp.h"
#include "rel_rel.h"

static sql_subfunc *
find_func( mvc *sql, char *name, list *exps )
{
	list * l = sa_list(sql->sa);
	node *n;

	for(n = exps->h; n; n = n->next)
		append(l, exp_subtype(n->data));
	return sql_bind_func_(sql, "sys", name, l, F_FUNC);
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

static sql_rel *
rel_count_gt_zero(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op)) {
		list *exps, *gbe;
		sql_exp *e = NULL;

		gbe = rel->r;
		if (!gbe || list_empty(gbe))
			return rel;
		/* introduce select * from l where cnt > 0 */
		/* find count */
		exps = rel_projections(sql, rel, NULL, 1, 1);
		e = find_aggr_exp(sql, rel->exps, "count");
		if (!e) {
			sql_subfunc *cf = sql_bind_func_(sql, "sys", "count", NULL, F_AGGR);

			e = exp_aggr(sql->sa, NULL, cf, 0, 0, CARD_AGGR, 0);
			exp_label(sql->sa, e, ++sql->label);
			append(rel->exps, e);
			e = exp_column(sql->sa, NULL, exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
		}
		e = exp_compare(sql->sa, e, exp_atom_lng(sql->sa, 0), cmp_notequal);
		rel = rel_select(sql->sa, rel, e);
		rel = rel_project(sql->sa, rel, exps);
	}
	return rel;
}


/* TODO for count we need remove useless 'converts' etc */
/* rewrite avg into sum/count */
static sql_rel *
rel_avg_rewrite(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op)) {
		list *pexps, *nexps = new_exp_list(sql->sa), *avgs = new_exp_list(sql->sa);
		list *aexps = new_exp_list(sql->sa); /* alias list */
		node *m, *n;

		/* Find all avg's */
		for (m = rel->exps->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					append(avgs, e);
					continue;
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
			sql_subtype *dbl_t;
			const char *rname = NULL, *name = NULL;

			rname = exp_relname(avg);
			name = exp_name(avg);
			if (!cnt) {
				list *l = avg->l;
				sql_subfunc *cf = sql_bind_func_(sql, "sys", "count", append(sa_list(sql->sa),exp_subtype(l->h->data)), F_AGGR);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), cf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				//exp_label(sql->sa, e, ++sql->label);
				append(nexps, e);
				cnt = exp_ref(sql, e);
			}
			if (!sum) {
				list *l = avg->l;
				sql_subfunc *sf = sql_bind_func_(sql, "sys", "sum", append(sa_list(sql->sa), exp_subtype(l->h->data)), F_AGGR);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), sf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				//exp_label(sql->sa, e, ++sql->label);
				append(nexps, e);
				sum = exp_ref(sql, e);
			}
			/* create new sum/cnt exp */

			/* For now we always convert to dbl */
			/* TODO fix this conversion could be done after sum! */
			//dbl_t = sql_bind_localtype("dbl");
			//cnt_d = exp_convert(sql->sa, cnt, exp_subtype(cnt), dbl_t);
			cnt_d = cnt;
			//sum = exp_convert(sql->sa, sum, exp_subtype(sum), dbl_t);

			args = new_exp_list(sql->sa);
			append(args, cnt);
			append(args, exp_atom_lng(sql->sa, 0));
			cmp = find_func(sql, "=", args);
			assert(cmp);
			cond = exp_op(sql->sa, args, cmp);

			args = new_exp_list(sql->sa);
			append(args, cond);
			//append(args, exp_atom(sql->sa, atom_general(sql->sa, dbl_t, NULL)));
			append(args, exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(cnt_d), NULL)));
			/* TODO only ifthenelse if value column may have nil's*/
			append(args, cnt_d);
			ifthen = find_func(sql, "ifthenelse", args);
			assert(ifthen);
			cnt_d = exp_op(sql->sa, args, ifthen);

			args = new_exp_list(sql->sa);

			sql_subtype *st = exp_subtype(sum);
			sql_subtype *ct = exp_subtype(cnt_d);
			/* convert sum flt -> dbl */
			if (st->type->eclass == EC_FLT && ct->type->eclass == EC_FLT && st->type->localtype < ct->type->localtype) {
				sum = exp_convert(sql->sa, sum, st, ct);
			} else if (st->type->eclass == EC_FLT) {
				if (ct->type != st->type) {
					dbl_t = sql_bind_localtype("dbl");
					if (ct->type->eclass != EC_FLT || st->type == dbl_t->type)
						cnt_d = exp_convert(sql->sa, cnt_d, exp_subtype(cnt_d), st);
				}
			}
			append(args, sum);
			append(args, cnt_d);
			div = find_func(sql, "sql_div", args);
			assert(div);
			navg = exp_op(sql->sa, args, div);

			if (subtype_cmp(exp_subtype(avg), exp_subtype(navg)) != 0)
				navg = exp_convert(sql->sa, navg, exp_subtype(navg), exp_subtype(avg));

			exp_setname(sql->sa, navg, rname, name );
			m->data = navg;
		}
		pexps = new_exp_list(sql->sa);
		for (m = rel->exps->h, n = avgs->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					sql_exp *avg = n->data;

					append(pexps, avg);
					n = n->next;
					continue;
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column && !rel_find_exp(rel->l, e))
				append(pexps, e);
			else
				append(pexps, exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e)));
		}
		sql_rel *nrel = rel_groupby(sql, rel_dup(rel->l), rel->r);
		rel_destroy(rel);
		nrel->exps = nexps;
		rel = rel_project(sql->sa, nrel, pexps);
		set_processed(rel);
		v->changes++;
	}
	return rel;
}


sql_rel *
rel_physical(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &rel_avg_rewrite);
	rel = rel_visitor_bottomup(&v, rel, &rel_count_gt_zero); /* the select > 0 should be done before using the values */
	return rel;
}
