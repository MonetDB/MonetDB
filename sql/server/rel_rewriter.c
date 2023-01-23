/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2023 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_rewriter.h"
#include "rel_exp.h"
#include "rel_dump.h"
#include "rel_basetable.h"

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

static sql_exp *
exp_exists(mvc *sql, sql_exp *le, int exists)
{
	sql_subfunc *exists_func = NULL;

	if (!(exists_func = sql_bind_func(sql, "sys", exists ? "sql_exists" : "sql_not_exists", exp_subtype(le), NULL, F_FUNC, true)))
		return sql_error(sql, 02, SQLSTATE(42000) "exist operator on type %s missing", exp_subtype(le) ? exp_subtype(le)->type->base.name : "unknown");
	sql_exp *res = exp_unop(sql->sa, le, exists_func);
	set_has_no_nil(res);
	return res;
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

			if (!(ie = exp_in_func(v->sql, l, vals, 1, 0)))
				return NULL;
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
	if (is_compare(e->type) && e->flag == cmp_equal) { /* predicate_func = TRUE */
		sql_exp *l = e->l, *r = e->r;
		if (is_func(l->type) && exp_is_true(r) && (is_anyequal_func(((sql_subfunc*)l->f)) || is_exists_func(((sql_subfunc*)l->f))))
			return l;
		if (is_func(l->type) && exp_is_false(r) && (is_anyequal_func(((sql_subfunc*)l->f)) || is_exists_func(((sql_subfunc*)l->f)))) {
			sql_subfunc *sf = l->f;
			if (is_anyequal_func(sf))
				return exp_in_func(v->sql, ((list*)l->l)->h->data, ((list*)l->l)->h->next->data, !is_anyequal(sf), 0);
			if (is_exists_func(sf))
				return exp_exists(v->sql, ((list*)l->l)->h->data, !is_exists(sf));
			return l;
		}
	}
	return e;
}

sql_rel *
rewrite_simplify(visitor *v, uint8_t cycle, bool value_based_opt, sql_rel *rel)
{
	if (!rel)
		return rel;

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		int changes = v->changes;
		rel->exps = exps_simplify_exp(v, rel->exps);
		/* At a select or inner join relation if the single expression is false, eliminate the inner relations with a dummy projection */
		if (value_based_opt && (v->changes > changes || cycle == 0) && (is_select(rel->op) || is_innerjoin(rel->op)) &&
			!is_single(rel) && list_length(rel->exps) == 1 && (exp_is_false(rel->exps->h->data) || exp_is_null(rel->exps->h->data))) {
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
			/* make sure the single expression is false, so the generate NULL values won't match */
			rel->exps->h->data = exp_atom_bool(v->sql->sa, 0);
			rel->l = rel_project(v->sql->sa, NULL, nexps);
			set_count_prop(v->sql->sa, rel->l, 1);
			set_count_prop(v->sql->sa, rel, 0);
			rel->card = CARD_ATOM;
			v->changes++;
		}
	}
	if (is_join(rel->op) && list_empty(rel->exps))
		rel->exps = NULL; /* crossproduct */
	return try_remove_empty_select(v, rel);
}

int
find_member_pos(list *l, sql_table *t)
{
	int i = 0;
	if (l) {
		for (node *n = l->h; n ; n = n->next, i++) {
			sql_part *pt = n->data;
			if (pt->member == t->base.id)
				return i;
		}
	}
	return -1;
}

/* The important task of the relational optimizer is to optimize the
   join order.

   The current implementation chooses the join order based on
   select counts, ie if one of the join sides has been reduced using
   a select this join is choosen over one without such selections.
 */

/* currently we only find simple column expressions */
sql_column *
name_find_column( sql_rel *rel, const char *rname, const char *name, int pnr, sql_rel **bt )
{
	sql_exp *alias = NULL;
	sql_column *c = NULL;

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (rel->exps) {
			sql_exp *e;

			if (rname)
				e = exps_bind_column2(rel->exps, rname, name, NULL);
			else
				e = exps_bind_column(rel->exps, name, NULL, NULL, 0);
			if (!e || e->type != e_column)
				return NULL;
			if (e->l)
				rname = e->l;
			name = e->r;
		}
		if (rname && strcmp(t->base.name, rname) != 0)
			return NULL;
		sql_table *mt = rel_base_get_mergetable(rel);
		if (ol_length(t->columns)) {
			for (node *cn = ol_first_node(t->columns); cn; cn = cn->next) {
				sql_column *c = cn->data;
				if (strcmp(c->base.name, name) == 0) {
					if (bt)
						*bt = rel;
					if (pnr < 0 || (mt &&
						find_member_pos(mt->members, c->t) == pnr))
						return c;
				}
			}
		}
		if (name[0] == '%' && ol_length(t->idxs)) {
			for (node *cn = ol_first_node(t->idxs); cn; cn = cn->next) {
				sql_idx *i = cn->data;
				if (strcmp(i->base.name, name+1 /* skip % */) == 0) {
					if (bt)
						*bt = rel;
					if (pnr < 0 || (mt &&
						find_member_pos(mt->members, i->t) == pnr)) {
						sql_kc *c = i->columns->h->data;
						return c->c;
					}
				}
			}
		}
		break;
	}
	case op_table:
		/* table func */
		return NULL;
	case op_ddl:
		if (is_updateble(rel))
			return name_find_column( rel->l, rname, name, pnr, bt);
		return NULL;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		/* first right (possible subquery) */
		c = name_find_column( rel->r, rname, name, pnr, bt);
		/* fall through */
	case op_semi:
	case op_anti:
		if (!c)
			c = name_find_column( rel->l, rname, name, pnr, bt);
		if (!c && !list_empty(rel->attr)) {
			if (rname)
				alias = exps_bind_column2(rel->attr, rname, name, NULL);
			else
				alias = exps_bind_column(rel->attr, name, NULL, NULL, 1);
		}
		return c;
	case op_select:
	case op_topn:
	case op_sample:
		return name_find_column( rel->l, rname, name, pnr, bt);
	case op_union:
	case op_inter:
	case op_except:

		if (pnr >= 0 || pnr == -2) {
			/* first right (possible subquery) */
			c = name_find_column( rel->r, rname, name, pnr, bt);
			if (!c)
				c = name_find_column( rel->l, rname, name, pnr, bt);
			return c;
		}
		return NULL;

	case op_project:
	case op_groupby:
		if (!rel->exps)
			break;
		if (rname)
			alias = exps_bind_column2(rel->exps, rname, name, NULL);
		else
			alias = exps_bind_column(rel->exps, name, NULL, NULL, 1);
		if (is_groupby(rel->op) && alias && alias->type == e_column && !list_empty(rel->r)) {
			if (alias->l)
				alias = exps_bind_column2(rel->r, alias->l, alias->r, NULL);
			else
				alias = exps_bind_column(rel->r, alias->r, NULL, NULL, 1);
		}
		if (is_groupby(rel->op) && !alias && rel->l) {
			/* Group by column not found as alias in projection
			 * list, fall back to check plain input columns */
			return name_find_column( rel->l, rname, name, pnr, bt);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
	case op_merge:
		break;
	}
	if (alias && !is_join(rel->op)) { /* we found an expression with the correct name, but
			we need sql_columns */
		if (rel->l && alias->type == e_column) /* real alias */
			return name_find_column(rel->l, alias->l, alias->r, pnr, bt);
	}
	return NULL;
}

sql_column *
exp_find_column( sql_rel *rel, sql_exp *exp, int pnr)
{
	if (exp->type == e_column)
		return name_find_column(rel, exp->l, exp->r, pnr, NULL);
	return NULL;
}

int
exp_joins_rels(sql_exp *e, list *rels)
{
	sql_rel *l = NULL, *r = NULL;

	assert (e->type == e_cmp);

	if (e->flag == cmp_or) {
		l = NULL;
	} else if (e->flag == cmp_filter) {
		list *ll = e->l;
		list *lr = e->r;

		l = find_rel(rels, ll->h->data);
		r = find_rel(rels, lr->h->data);
	} else if (e->flag == cmp_in || e->flag == cmp_notin) {
		list *lr = e->r;

		l = find_rel(rels, e->l);
		if (lr && lr->h)
			r = find_rel(rels, lr->h->data);
	} else {
		l = find_rel(rels, e->l);
		r = find_rel(rels, e->r);
	}

	if (l && r)
		return 0;
	return -1;
}

static int
rel_is_unique(sql_rel *rel)
{
	switch(rel->op) {
	case op_semi:
	case op_anti:
	case op_inter:
	case op_except:
	case op_topn:
	case op_sample:
		return rel_is_unique(rel->l);
	case op_table:
	case op_basetable:
		return 1;
	default:
		return 0;
	}
}

int
kc_column_cmp(sql_kc *kc, sql_column *c)
{
	/* return on equality */
	return !(c == kc->c);
}

/* WARNING exps_unique doesn't check for duplicate NULL values */
int
exps_unique(mvc *sql, sql_rel *rel, list *exps)
{
	int nr = 0, need_check = 0;
	sql_ukey *k = NULL;

	if (list_empty(exps))
		return 0;
	for(node *n = exps->h; n ; n = n->next) {
		sql_exp *e = n->data;
		prop *p;

		if (!is_unique(e)) { /* ignore unique columns */
			need_check++;
			if (!k && (p = find_prop(e->p, PROP_HASHCOL))) /* at the moment, use only one k */
				k = p->value.pval;
		}
	}
	if (!need_check) /* all have unique property return */
		return 1;
	if (!k || list_length(k->k.columns) != need_check)
		return 0;
	if (rel) {
		char *matched = SA_ZNEW_ARRAY(sql->sa, char, list_length(k->k.columns));
		fcmp cmp = (fcmp)&kc_column_cmp;
		for(node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_column *c;
			node *m;

			if (is_unique(e))
				continue;
			if ((c = exp_find_column(rel, e, -2)) != NULL && (m = list_find(k->k.columns, c, cmp)) != NULL) {
				int pos = list_position(k->k.columns, m->data);
				if (!matched[pos])
					nr++;
				matched[pos] = 1;
			}
		}
		if (nr == list_length(k->k.columns))
			return rel_is_unique(rel);
	}
	return 0;
}

BUN
get_rel_count(sql_rel *rel)
{
	prop *found = find_prop(rel->p, PROP_COUNT);
	return found ? found->value.lval : BUN_NONE;
}

void
set_count_prop(sql_allocator *sa, sql_rel *rel, BUN val)
{
	if (val != BUN_NONE) {
		prop *found = find_prop(rel->p, PROP_COUNT);

		if (found) {
			found->value.lval = val;
		} else {
			prop *p = rel->p = prop_create(sa, PROP_COUNT, rel->p);
			p->value.lval = val;
		}
	}
}
