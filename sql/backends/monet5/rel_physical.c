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

sql_rel *
rel_physical(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &rel_add_orderby);
	return rel;
}
