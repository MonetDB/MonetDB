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
#include "rel_basetable.h"
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

static void psm_exps_properties(mvc *sql, global_props *gp, list *exps);

static void
psm_exp_properties(mvc *sql, global_props *gp, sql_exp *e)
{
	/* only functions need fix up */
	switch(e->type) {
	case e_column:
		break;
	case e_atom:
		if (e->f)
			psm_exps_properties(sql, gp, e->f);
		break;
	case e_convert:
		psm_exp_properties(sql, gp, e->l);
		break;
	case e_aggr:
	case e_func:
		psm_exps_properties(sql, gp, e->l);
		assert(!e->r);
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			psm_exps_properties(sql, gp, e->l);
			psm_exps_properties(sql, gp, e->r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			psm_exp_properties(sql, gp, e->l);
			psm_exps_properties(sql, gp, e->r);
		} else {
			psm_exp_properties(sql, gp, e->l);
			psm_exp_properties(sql, gp, e->r);
			if (e->f)
				psm_exp_properties(sql, gp, e->f);
		}
		break;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			psm_exp_properties(sql, gp, e->l);
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			psm_exp_properties(sql, gp, e->l);
			psm_exps_properties(sql, gp, e->r);
			if (e->flag == PSM_IF && e->f)
				psm_exps_properties(sql, gp, e->f);
		} else if (e->flag & PSM_REL && e->l) {
			rel_properties(sql, gp, e->l);
		}
		break;
	}
}

static void
psm_exps_properties(mvc *sql, global_props *gp, list *exps)
{
	node *n;

	if (!exps)
		return;
	for (n = exps->h; n; n = n->next)
		psm_exp_properties(sql, gp, n->data);
}

void
rel_properties(mvc *sql, global_props *gp, sql_rel *rel)
{
	if (!rel)
		return;

	gp->cnt[(int)rel->op]++;
	switch (rel->op) {
	case op_basetable:
	case op_table: {
		if (!find_prop(rel->p, PROP_COUNT))
			rel->p = prop_create(sql->sa, PROP_COUNT, rel->p);
		if (is_basetable(rel->op)) {
			sql_table *t = (sql_table *) rel->l;
			sql_part *pt;

			/* If the plan has a merge table or a child of one, then rel_merge_table_rewrite has to run */
			gp->has_mergetable |= (isMergeTable(t) || (t->s && t->s->parts && (pt = partition_find_part(sql->session->tr, t, NULL))));
		}
		if (rel->op == op_table && rel->l && rel->flag != TRIGGER_WRAPPER)
			rel_properties(sql, gp, rel->l);
	} break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi:
	case op_anti:

	case op_union:
	case op_inter:
	case op_except:

	case op_insert:
	case op_update:
	case op_delete:
		if (rel->l)
			rel_properties(sql, gp, rel->l);
		if (rel->r)
			rel_properties(sql, gp, rel->r);
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
	case op_truncate:
		if (rel->l)
			rel_properties(sql, gp, rel->l);
		break;
	case op_ddl:
		if (rel->flag == ddl_psm && rel->exps)
			psm_exps_properties(sql, gp, rel->exps);
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_properties(sql, gp, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_properties(sql, gp, rel->l);
			if (rel->r)
				rel_properties(sql, gp, rel->r);
		}
		break;
	}
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
void *
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
		if (name && !t)
			return rel_base_get_mergetable(rel);
		if (rname && strcmp(t->base.name, rname) != 0)
			return NULL;
		node *cn;
		sql_table *mt = rel_base_get_mergetable(rel);
		for (cn = ol_first_node(t->columns); cn; cn = cn->next) {
			sql_column *c = cn->data;
			if (strcmp(c->base.name, name) == 0) {
				if (bt)
					*bt = rel;
				if (pnr < 0 || (mt &&
					find_member_pos(mt->members, c->t) == pnr))
					return c;
			}
		}
		if (t->idxs)
		for (cn = ol_first_node(t->idxs); cn; cn = cn->next) {
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
		if (is_groupby(rel->op) && alias && alias->type == e_column && rel->r) {
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
		break;
	}
	if (alias) { /* we found an expression with the correct name, but
			we need sql_columns */
		if (rel->l && alias->type == e_column) /* real alias */
			return name_find_column(rel->l, alias->l, alias->r, pnr, bt);
	}
	return NULL;
}
