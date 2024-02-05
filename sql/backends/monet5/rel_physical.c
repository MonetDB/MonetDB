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
	rel = rel_exp_visitor_topdown(&v, rel, &exp_timezone, true);
	return rel;
}
