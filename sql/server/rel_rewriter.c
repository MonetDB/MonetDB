/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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

		needed = (exp_is_true(v->sql, e) || exp_is_false(v->sql, e) || (is_compare(e->type) && e->flag == cmp_or));
	}
	/* if there's only one expression and it is false, we have to keep it */
	if (list_length(exps) == 1 && exp_is_false(v->sql, exps->h->data))
		return exps;
	if (needed) {
		list *nexps = sa_list(v->sql->sa);
		v->sql->caching = 0;
		for (node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* TRUE or X -> TRUE
		 	* FALSE or X -> X */
			if (is_compare(e->type) && e->flag == cmp_or) {
				list *l = e->l = exps_simplify_exp(v, e->l);
				list *r = e->r = exps_simplify_exp(v, e->r);

				if (list_length(l) == 1) {
					sql_exp *ie = l->h->data;

					if (exp_is_true(v->sql, ie)) {
						v->changes++;
						continue;
					} else if (exp_is_false(v->sql, ie)) {
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

					if (exp_is_true(v->sql, ie)) {
						v->changes++;
						continue;
					} else if (exp_is_false(v->sql, ie)) {
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
			if (exp_is_true(v->sql, e)) {
				v->changes++;
				continue;
			/* FALSE and X -> FALSE */
			} else if (exp_is_false(v->sql, e)) {
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

			v->sql->caching = 0;
			if (list_length(l) == 1) {
				sql_exp *ie = l->h->data;

				if (exp_is_true(v->sql, ie)) {
					v->changes++;
					return ie;
				} else if (exp_is_false(v->sql, ie) && list_length(r) == 1) {
					v->changes++;
					return r->h->data;
				}
			} else if (list_length(l) == 0) { /* left is true */
				v->changes++;
				return exp_atom_bool(v->sql->sa, 1);
			}
			if (list_length(r) == 1) {
				sql_exp *ie = r->h->data;

				if (exp_is_true(v->sql, ie)) {
					v->changes++;
					return ie;
				} else if (exp_is_false(v->sql, ie) && list_length(l) == 1) {
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

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps))
		rel->exps = exps_simplify_exp(v, rel->exps);
	return rel;
}

sql_rel *
rel_remove_empty_select(visitor *v, sql_rel *rel)
{
	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op) || is_project(rel->op) || is_topn(rel->op) || is_sample(rel->op)) && rel->l) {
		sql_rel *l = rel->l;
		if (is_select(l->op) && !(rel_is_ref(l)) && list_empty(l->exps)) {
			rel->l = l->l;
			l->l = NULL;
			rel_destroy(l);
			v->changes++;
		}
	}
	if ((is_join(rel->op) || is_semi(rel->op) || is_set(rel->op)) && rel->r) {
		sql_rel *r = rel->r;
		if (is_select(r->op) && !(rel_is_ref(r)) && list_empty(r->exps)) {
			rel->r = r->l;
			r->l = NULL;
			rel_destroy(r);
			v->changes++;
		}
	}
	if (is_join(rel->op) && list_empty(rel->exps))
		rel->exps = NULL; /* crossproduct */
	return rel;
}

sql_rel *
rewrite_reset_used(visitor *v, sql_rel *rel)
{
	(void) v;
	rel->used = 0;
	return rel;
}
