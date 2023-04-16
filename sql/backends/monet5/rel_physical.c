/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
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
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (is_aggr(e->type)) {
					sql_subfunc *af = e->f;
                        		list *aa = e->l;

					/* for now we only handle one sort order */
                        		if (IS_ORDER_BASED_AGGR(af->func->base.name) && aa && list_length(aa) == 2) {
						sql_exp *obe = aa->h->data;
						if (obe) { 
							sql_rel *l = rel->l = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rel->l, NULL, 1, 1));
							if (l) {
								if (!is_alias(obe->type)) {
									append(l->exps, obe);
									obe = exp_label(v->sql->sa, obe, ++v->sql->label);
									aa->h->data = exp_ref(v->sql, obe);
								}
								list *o = l->r = sa_list(v->sql->sa);
								if (o)
									append(o, obe);
							}
						}
						return rel;
					}
				}
			}
		}
	}
	return rel;
}

sql_rel *
rel_physical(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &rel_add_orderby);
	return rel;
}
