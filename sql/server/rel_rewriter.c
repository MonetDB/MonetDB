/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_rewriter.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "mal_errors.h" /* for SQLSTATE() */

/* simplify expressions, such as not(not(x)) */
/* exp visitor */

#define is_not_anyequal(sf) (strcmp((sf)->func->base.name, "sql_not_anyequal") == 0)

static list *
exps_simplify_exp(visitor *v, list *exps)
{
	if (list_empty(exps))
		return exps;

	int needed = 0;
	for (node *n=exps->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;

		needed = (exp_is_true(e) || exp_is_false(e) || (is_compare(e->type) && e->flag == cmp_or));
	}
	/* if there's only one expression and it is false, we have to keep it */
	if (list_length(exps) == 1 && exp_is_false(exps->h->data))
		return exps;
	if (needed) {
		list *nexps = sa_list(v->sql->sa);
		for (node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* TRUE or X -> TRUE
		 	* FALSE or X -> X */
			if (is_compare(e->type) && e->flag == cmp_or) {
				list *l = e->l = exps_simplify_exp(v, e->l);
				list *r = e->r = exps_simplify_exp(v, e->r);

				if (list_length(l) == 1) {
					sql_exp *ie = l->h->data;

					if (exp_is_true(ie)) {
						v->changes++;
						continue;
					} else if (exp_is_false(ie)) {
						v->changes++;
						nexps = list_merge(nexps, r, (fdup)NULL);
						continue;
					}
				} else if (list_length(l) == 0) { /* left is true */
					v->changes++;
					continue;
				}
				if (list_length(r) == 1) {
					sql_exp *ie = r->h->data;

					if (exp_is_true(ie)) {
						v->changes++;
						continue;
					} else if (exp_is_false(ie)) {
						nexps = list_merge(nexps, l, (fdup)NULL);
						v->changes++;
						continue;
					}
				} else if (list_length(r) == 0) { /* right is true */
					v->changes++;
					continue;
				}
			}
			/* TRUE and X -> X */
			if (exp_is_true(e)) {
				v->changes++;
				continue;
			/* FALSE and X -> FALSE */
			} else if (exp_is_false(e)) {
				v->changes++;
				return append(sa_list(v->sql->sa), e);
			} else {
				append(nexps, e);
			}
		}
		return nexps;
	}
	return exps;
}

sql_exp *
rewrite_simplify_exp(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	if (!e)
		return e;

	v->changes = 0;
	(void)rel; (void)depth;

	sql_subfunc *sf = e->f;
	if (is_func(e->type) && list_length(e->l) == 1 && is_not_func(sf)) {
		list *args = e->l;
		sql_exp *ie = args->h->data;

		if (!ie)
			return e;

		sql_subfunc *sf = ie->f;
		if (is_func(ie->type) && list_length(ie->l) == 1 && is_not_func(sf)) {
			args = ie->l;

			ie = args->h->data;
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, ie, e);
			v->changes++;
			return ie;
		}
		if (is_func(ie->type) && list_length(ie->l) == 2 && is_not_anyequal(sf)) {
			args = ie->l;

			sql_exp *l = args->h->data;
			sql_exp *vals = args->h->next->data;

			ie = exp_in_func(v->sql, l, vals, 1, 0);
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, ie, e);
			v->changes++;
			return ie;
		}
		/* TRUE or X -> TRUE
		 * FALSE or X -> X */
		if (is_compare(e->type) && e->flag == cmp_or) {
			list *l = e->l = exps_simplify_exp(v, e->l);
			list *r = e->r = exps_simplify_exp(v, e->r);

			if (list_length(l) == 1) {
				sql_exp *ie = l->h->data;

				if (exp_is_true(ie)) {
					v->changes++;
					return ie;
				} else if (exp_is_false(ie) && list_length(r) == 1) {
					v->changes++;
					return r->h->data;
				}
			} else if (list_length(l) == 0) { /* left is true */
				v->changes++;
				return exp_atom_bool(v->sql->sa, 1);
			}
			if (list_length(r) == 1) {
				sql_exp *ie = r->h->data;

				if (exp_is_true(ie)) {
					v->changes++;
					return ie;
				} else if (exp_is_false(ie) && list_length(l) == 1) {
					v->changes++;
					return l->h->data;
				}
			} else if (list_length(r) == 0) { /* right is true */
				v->changes++;
				return exp_atom_bool(v->sql->sa, 1);
			}
		}
	}
	return e;
}

sql_rel *
rewrite_simplify(visitor *v, sql_rel *rel)
{
	if (!rel)
		return rel;

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		rel->exps = exps_simplify_exp(v, rel->exps);
		/* At a select or inner join relation if the single expression is false, eliminate the inner relations with a dummy projection */
		if (v->value_based_opt && list_length(rel->exps) == 1 && exp_is_false(rel->exps->h->data)) {
			if ((is_select(rel->op) || (is_innerjoin(rel->op) && !rel_is_ref(rel->r))) && rel->card > CARD_ATOM && !rel_is_ref(rel->l)) {
				list *nexps = sa_list(v->sql->sa), *toconvert = rel_projections(v->sql, rel->l, NULL, 1, 1);
				if (is_innerjoin(rel->op))
					toconvert = list_merge(toconvert, rel_projections(v->sql, rel->r, NULL, 1, 1), NULL);

				for (node *n = toconvert->h ; n ; n = n->next) {
					sql_exp *e = n->data, *a = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL));
					exp_prop_alias(v->sql->sa, a, e);
					list_append(nexps, a);
				}
				rel_destroy(rel->l);
				if (is_innerjoin(rel->op)) {
					rel_destroy(rel->r);
					rel->r = NULL;
					rel->op = op_select;
				}
				rel->l = rel_project(v->sql->sa, NULL, nexps);
				rel->card = CARD_ATOM;
				v->changes++;
			}
		}
	}
	if (is_join(rel->op) && list_empty(rel->exps))
		rel->exps = NULL; /* crossproduct */
	return try_remove_empty_select(v, rel);
}

sql_rel *
rewrite_reset_used(visitor *v, sql_rel *rel)
{
	(void) v;
	rel->used = 0;
	return rel;
}
