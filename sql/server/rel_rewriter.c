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

#define is_null(sf) (strcmp(sf->func->base.name, "isnull") == 0) 
#define is_not_func(sf) (strcmp(sf->func->base.name, "not") == 0) 
#define is_not_anyequal(sf) (strcmp(sf->func->base.name, "sql_not_anyequal") == 0)

static list *
exps_simplify_exp(mvc *sql, list *exps, int *changes)
{
	if (list_empty(exps))
		return exps;

	int needed = 0;
	for (node *n=exps->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;

		needed = (exp_is_true(sql, e) || exp_is_false(sql, e) || (is_compare(e->type) && e->flag == cmp_or)); 
	}
	if (needed) {
		list *nexps = sa_list(sql->sa);
		sql->caching = 0;
		for (node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;
	
			/* TRUE or X -> TRUE
		 	* FALSE or X -> X */
			if (is_compare(e->type) && e->flag == cmp_or) {
				list *l = e->l = exps_simplify_exp(sql, e->l, changes);
				list *r = e->r = exps_simplify_exp(sql, e->r, changes); 

				if (list_length(l) == 1) {
					sql_exp *ie = l->h->data; 
	
					if (exp_is_true(sql, ie)) {
						(*changes)++;
						continue;
					} else if (exp_is_false(sql, ie)) {
						(*changes)++;
						nexps = list_merge(nexps, r, (fdup)NULL);
						continue;
					}
				} else if (list_length(l) == 0) { /* left is true */
					(*changes)++;
					continue;
				}
				if (list_length(r) == 1) {
					sql_exp *ie = r->h->data; 
	
					if (exp_is_true(sql, ie)) {
						(*changes)++;
						continue;
					} else if (exp_is_false(sql, ie)) {
						nexps = list_merge(nexps, l, (fdup)NULL);
						(*changes)++;
						continue;
					}
				} else if (list_length(r) == 0) { /* right is true */
					(*changes)++;
					continue;
				}
			}
			/* TRUE and X -> X */
			if (exp_is_true(sql, e)) {
				(*changes)++;
				continue;
			/* FALSE and X -> FALSE */
			} else if (exp_is_false(sql, e)) {
				(*changes)++;
				return append(sa_list(sql->sa), e);
			} else {
				append(nexps, e);
			}
		}
		return nexps;
	}
	return exps;
}

sql_exp *
rewrite_simplify_exp(mvc *sql, sql_rel *rel, sql_exp *e, int depth, int *changes)
{
	if (!e)
		return e;

	*changes = 0;
	(void)sql; (void)rel; (void)depth; (void) changes;

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
				exp_prop_alias(sql->sa, ie, e);
			(*changes)++;
			return ie;
		}
		if (is_func(ie->type) && list_length(ie->l) == 2 && is_not_anyequal(sf)) {
			args = ie->l;

			sql_exp *l = args->h->data;
			sql_exp *vals = args->h->next->data;

			ie = exp_in_func(sql, l, vals, 1, 0);
			if (exp_name(e))
				exp_prop_alias(sql->sa, ie, e);
			(*changes)++;
			return ie;
		}
		/* TRUE or X -> TRUE
		 * FALSE or X -> X */
		if (is_compare(e->type) && e->flag == cmp_or) {
			list *l = e->l = exps_simplify_exp(sql, e->l, changes);
			list *r = e->r = exps_simplify_exp(sql, e->r, changes); 

			sql->caching = 0;
			if (list_length(l) == 1) {
				sql_exp *ie = l->h->data; 

				if (exp_is_true(sql, ie)) {
					(*changes)++;
					return ie;
				} else if (exp_is_false(sql, ie) && list_length(r) == 1) {
					(*changes)++;
					return r->h->data;
				}
			} else if (list_length(l) == 0) { /* left is true */
				(*changes)++;
				return exp_atom_bool(sql->sa, 1);
			}
			if (list_length(r) == 1) {
				sql_exp *ie = r->h->data; 

				if (exp_is_true(sql, ie)) {
					(*changes)++;
					return ie;
				} else if (exp_is_false(sql, ie) && list_length(l) == 1) {
					(*changes)++;
					return l->h->data;
				}
			} else if (list_length(r) == 0) { /* right is true */
				(*changes)++;
				return exp_atom_bool(sql->sa, 1);
			}
		}
	}
	return e;
}

sql_rel *
rewrite_simplify(mvc *sql, sql_rel *rel, int *changes)
{
	if (!rel)
		return rel;

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps))
		rel->exps = exps_simplify_exp(sql, rel->exps, changes);
	return rel;
}

sql_rel *
rel_remove_empty_select(mvc *sql, sql_rel *rel, int *changes)
{
	(void)sql;

	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op) || is_project(rel->op) || is_topn(rel->op) || is_sample(rel->op)) && rel->l) {
		sql_rel *l = rel->l;
		if (is_select(l->op) && !(rel_is_ref(l)) && list_empty(l->exps)) {
			rel->l = l->l;
			l->l = NULL;
			rel_destroy(l);
			(*changes)++;
		} 
	}
	if ((is_join(rel->op) || is_semi(rel->op) || is_set(rel->op)) && rel->r) {
		sql_rel *r = rel->r;
		if (is_select(r->op) && !(rel_is_ref(r)) && list_empty(r->exps)) {
			rel->r = r->l;
			r->l = NULL;
			rel_destroy(r);
			(*changes)++;
		}
	} 
	if (is_join(rel->op) && list_empty(rel->exps)) 
		rel->exps = NULL; /* crossproduct */
	return rel;
}
