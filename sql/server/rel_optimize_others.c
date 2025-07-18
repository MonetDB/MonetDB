/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_optimizer.h"
#include "rel_optimizer_private.h"
#include "rel_optimizer.h"
#include "rel_exp.h"
#include "rel_select.h"

static void
rel_no_rename_exps( list *exps )
{
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		exp_setalias(e, e->alias.label, e->l, e->r);
	}
	list_hash_clear(exps);
}

/* positional renames ! */
/* get the names (aka aliases) from the src_exps list and rename the dest_exps list */
void
rel_rename_exps( mvc *sql, list *src_exps, list *dest_exps)
{
	node *n, *m;

	(void)sql;
	/* check if a column uses an alias earlier in the list */

	assert(list_length(src_exps) <= list_length(dest_exps));
	for (n = src_exps->h, m = dest_exps->h; n && m; n = n->next, m = m->next) {
		sql_exp *s = n->data;
		sql_exp *d = m->data;
		const char *rname = exp_relname(s);

		exp_setalias(d, s->alias.label, rname, exp_name(s));
	}
	list_hash_clear(dest_exps);
}

sql_rel *
rel_find_ref( sql_rel *r)
{
	while (!rel_is_ref(r) && r->l &&
	      (is_project(r->op) || is_select(r->op) /*|| is_join(r->op)*/))
		r = r->l;
	if (rel_is_ref(r))
		return r;
	return NULL;
}

/* merge projection */

/* push an expression through a projection.
 * The result should again be used in a projection.
 */
static list *
exps_push_down_prj(mvc *sql, list *exps, sql_rel *f, sql_rel *t, bool keepalias)
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	for(n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data, *narg = NULL;

		narg = exp_push_down_prj(sql, arg, f, t);
		if (!narg)
			return NULL;
		narg = exp_propagate(sql->sa, narg, arg);
		if (!keepalias && narg->type == e_column)
			exp_setalias(narg, arg->alias.label, narg->l, narg->r);
		append(nl, narg);
	}
	return nl;
}

sql_exp *
exp_push_down_prj(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t)
{
	sql_exp *ne = NULL, *l = NULL, *r = NULL, *r2 = NULL;

	assert(is_project(f->op));

	switch(e->type) {
	case e_column:
		assert(e->nid);
		ne = exps_bind_nid(f->exps, e->nid);
		if (!ne || (ne->type != e_column && (ne->type != e_atom || ne->f)))
			return NULL;
		while (ne && (has_label(ne) || is_selfref(ne) /*inside this list */) && is_simple_project(f->op) && ne->type == e_column) {
			sql_exp *oe = e, *one = ne;

			e = ne;
			ne = NULL;
			if (e->nid)
				ne = exps_bind_nid(f->exps, e->nid);
			if (ne && ne != one && list_position(f->exps, ne) >= list_position(f->exps, one))
				ne = NULL;
			if (!ne || ne == one) {
				ne = one;
				e = oe;
				break;
			}
			if (ne->type != e_column && (ne->type != e_atom || ne->f))
				return NULL;
		}
		/* possibly a groupby/project column is renamed */
		if (is_groupby(f->op) && !list_empty(f->r) && ne->type == e_column) {
			sql_exp *gbe = NULL;
			if (ne->nid)
				gbe = exps_bind_nid(f->exps, ne->nid);
			ne = gbe;
			if (!ne || (ne->type != e_column && (ne->type != e_atom || ne->f)))
				return NULL;
		}
		return exp_copy(sql, ne);
	case e_cmp:
		if (e->flag == cmp_filter) {
			list *l = NULL, *r = NULL;

			if (!(l = exps_push_down_prj(sql, e->l, f, t, true)) || !(r = exps_push_down_prj(sql, e->r, f, t, true)))
				return NULL;
			ne = exp_filter(sql->sa, l, r, e->f, is_anti(e));
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			list *l = NULL;

			if (!(l = exps_push_down_prj(sql, e->l, f, t, true)))
				return NULL;
			if (e->flag == cmp_con)
				ne = exp_conjunctive(sql->sa, l);
			else
				ne = exp_disjunctive(sql->sa, l);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *r = NULL;

			if (!(l = exp_push_down_prj(sql, e->l, f, t)) || !(r = exps_push_down_prj(sql, e->r, f, t, true)))
				return NULL;
			ne = exp_in(sql->sa, l, r, e->flag);
		} else {
			if (!(l = exp_push_down_prj(sql, e->l, f, t)) || !(r = exp_push_down_prj(sql, e->r, f, t)) || (e->f && !(r2 = exp_push_down_prj(sql, e->f, f, t))))
				return NULL;
			if (e->f) {
				ne = exp_compare2(sql->sa, l, r, r2, e->flag, is_symmetric(e));
			} else {
				ne = exp_compare(sql->sa, l, r, e->flag);
			}
		}
		if (!ne)
			return NULL;
		return exp_propagate(sql->sa, ne, e);
	case e_convert:
		if (!(l = exp_push_down_prj(sql, e->l, f, t)))
			return NULL;
		ne = exp_convert(sql, l, exp_fromtype(e), exp_totype(e));
		return exp_propagate(sql->sa, ne, e);
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;
		sql_exp *ne = NULL;

		if (e->type == e_func && exp_unsafe(e, false, false))
			return NULL;
		if (!list_empty(l)) {
			nl = exps_push_down_prj(sql, l, f, t, false);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			ne = exp_op(sql->sa, nl, e->f);
		else
			ne = exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
		return exp_propagate(sql->sa, ne, e);
	}
	case e_atom: {
		list *l = e->f, *nl = NULL;

		if (!list_empty(l)) {
			nl = exps_push_down_prj(sql, l, f, t, false);
			if (!nl)
				return NULL;
			ne = exp_values(sql->sa, nl);
		} else {
			ne = exp_copy(sql, e);
		}
		return exp_propagate(sql->sa, ne, e);
	}
	case e_psm:
		if (e->type == e_atom && e->f) /* value list */
			return NULL;
		return e;
	}
	return NULL;
}

atom *
exp_flatten(mvc *sql, bool value_based_opt, sql_exp *e)
{
	if (e->type == e_atom) {
		return value_based_opt ? exp_value(sql, e) : (atom *) e->l;
	} else if (e->type == e_convert) {
		atom *v = exp_flatten(sql, value_based_opt, e->l);

		if (v)
			return atom_cast(sql->sa, v, exp_subtype(e));
	} else if (e->type == e_func) {
		sql_subfunc *f = e->f;
		list *l = e->l;
		sql_arg *res = (f->func->res)?(f->func->res->h->data):NULL;

		/* TODO handle date + x months */
		if (!f->func->s && strcmp(f->func->base.name, "sql_add") == 0 && list_length(l) == 2 && res && EC_NUMBER(res->type.type->eclass)) {
			atom *l1 = exp_flatten(sql, value_based_opt, l->h->data);
			atom *l2 = exp_flatten(sql, value_based_opt, l->h->next->data);
			if (l1 && l2)
				return atom_add(sql->sa, l1, l2);
		} else if (!f->func->s && strcmp(f->func->base.name, "sql_sub") == 0 && list_length(l) == 2 && res && EC_NUMBER(res->type.type->eclass)) {
			atom *l1 = exp_flatten(sql, value_based_opt, l->h->data);
			atom *l2 = exp_flatten(sql, value_based_opt, l->h->next->data);
			if (l1 && l2)
				return atom_sub(sql->sa, l1, l2);
		}
	}
	return NULL;
}

int
exp_range_overlap(atom *min, atom *max, atom *emin, atom *emax, bool min_exclusive, bool max_exclusive)
{
	if (!min || !max || !emin || !emax || min->isnull || max->isnull || emin->isnull || emax->isnull)
		return 0;

	if ((!min_exclusive && VALcmp(&(emax->data), &(min->data)) < 0) || (min_exclusive && VALcmp(&(emax->data), &(min->data)) <= 0))
		return 0;
	if ((!max_exclusive && VALcmp(&(emin->data), &(max->data)) > 0) || (max_exclusive && VALcmp(&(emin->data), &(max->data)) >= 0))
		return 0;
	return 1;
}


/* if local_proj is >= -1, the current expression is from the same projection
   if local_proj is -1, then we don't care about self references (eg used to check for order by exps) */
static int exp_mark_used(sql_rel *subrel, sql_exp *e, int local_proj);

static int
exps_mark_used(sql_rel *subrel, list *l, int local_proj)
{
	int nr = 0;
	if (list_empty(l))
		return nr;

	for (node *n = l->h; n != NULL; n = n->next)
		nr += exp_mark_used(subrel, n->data, local_proj);
	return nr;
}

/* mark all expression related to this nid */
static int
exps_mark_all_used(list *exps, int nid, int local_proj)
{
	if (!list_empty(exps)) {
		int i = 0;
		for(node *n = exps->h; n; n = n->next, i++) {
			sql_exp *e = n->data;

			if (e->alias.label == nid) {
				if (local_proj <= -1 || i < local_proj) {
					if (local_proj < 0 || e->nid != e->alias.label) {
						e->used = 1;
						return 1;
					}
				}
			}
			/*
			if (e->f && e->type == e_column && (local_proj <= -1 || i < local_proj)) {
				if (exps_mark_all_used(e->f, nid, -2)) {
					e->used = 1;
					return 1;
				}
			}
			*/
		}
	}
	return 0;
}

static int
rel_mark_all_used(sql_rel *r, int nid, int local_proj)
{
	if (is_project(r->op) || (is_base(r->op) && r->exps))
		return exps_mark_all_used(r->exps, nid, local_proj);
	if (is_select(r->op) || is_semi(r->op))
		return rel_mark_all_used(r->l, nid, local_proj);
	if (is_join(r->op)) {
		if (r->l && rel_mark_all_used(r->l, nid, local_proj))
			return 1;
		else if (r->r)
			return rel_mark_all_used(r->r, nid, local_proj);
	}
	return 0;
}

static int
exp_mark_used(sql_rel *subrel, sql_exp *e, int local_proj)
{
	int nr = 0;
	sql_exp *ne = NULL;

	switch(e->type) {
	case e_column:
		if (e->nid && subrel && subrel->exps && rel_mark_all_used(subrel, e->nid, local_proj))
			nr++;
		else
			ne = rel_find_exp(subrel, e);
		/* if looking in the same projection, make sure 'ne' is projected before the searched column */
		if (ne && local_proj > -1 && list_position(subrel->exps, ne) >= local_proj)
			ne = NULL;
		break;
	case e_convert:
		return exp_mark_used(subrel, e->l, local_proj);
	case e_aggr:
	case e_func: {
		if (e->l)
			nr += exps_mark_used(subrel, e->l, local_proj);
		if (e->r) {
			list *r = e->r;
			list *obes = r->h->data;
			nr += exps_mark_used(subrel, obes, local_proj);
			if (r->h->next) {
				list *exps = r->h->next->data;
				nr += exps_mark_used(subrel, exps, local_proj);
			}
		}
		break;
	}
	case e_cmp:
		if (e->flag == cmp_filter) {
			nr += exps_mark_used(subrel, e->l, local_proj);
			nr += exps_mark_used(subrel, e->r, local_proj);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			nr += exps_mark_used(subrel, e->l, local_proj);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			nr += exp_mark_used(subrel, e->l, local_proj);
			nr += exps_mark_used(subrel, e->r, local_proj);
		} else {
			nr += exp_mark_used(subrel, e->l, local_proj);
			nr += exp_mark_used(subrel, e->r, local_proj);
			if (e->f)
				nr += exp_mark_used(subrel, e->f, local_proj);
		}
		break;
	case e_atom:
		/* atoms are used in e_cmp */
		e->used = 1;
		/* return 0 as constants may require a full column ! */
		if (e->f)
			nr += exps_mark_used(subrel, e->f, local_proj);
		return nr;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			nr += exp_mark_used(subrel, e->l, local_proj);
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			nr += exp_mark_used(subrel, e->l, local_proj);
			nr += exps_mark_used(subrel, e->r, local_proj);
			if (e->flag == PSM_IF && e->f)
				nr += exps_mark_used(subrel, e->f, local_proj);
		}
		e->used = 1;
		break;
	}
	if (ne && e != ne) {
		if (local_proj == -2 || ne->type != e_column || (has_label(ne) || (ne->alias.rname && ne->alias.rname[0] == '%')) || (subrel->l && !is_munion(subrel->op) && !rel_find_exp(subrel->l, e)))
			ne->used = 1;
		return ne->used;
	}
	return nr;
}

static void
positional_exps_mark_used( sql_rel *rel, sql_rel *subrel )
{
	assert(rel->exps);

	if ((is_topn(subrel->op) || is_sample(subrel->op)) && subrel->l)
		subrel = subrel->l;
	/* everything is used within the set operation */
	if (rel->exps && subrel->exps) {
		node *m;
		for (m=subrel->exps->h; m; m = m->next) {
			sql_exp *se = m->data;

			se->used = 1;
		}
	}
}

static void
rel_exps_mark_used(allocator *sa, sql_rel *rel, sql_rel *subrel)
{
	int nr = 0;

	if (rel->l && rel->r && (is_simple_project(rel->op) || is_groupby(rel->op))) {
		list *l = rel->r;
		node *n;

		for (n=l->h; n; n = n->next) {
			sql_exp *e = n->data;

			e->used = 1;
			exp_mark_used(rel->l, e, -1);
		}
	}
	if (rel->attr) {
		for (node *n = rel->attr->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type != e_aggr) /* keep all group by's */
				e->used = 1;
			if (e->used)
				nr += exp_mark_used(subrel, e, -2);
		}
	}
	if (rel->exps) {
		node *n;
		int len = list_length(rel->exps), i;
		sql_exp **exps = SA_NEW_ARRAY(sa, sql_exp*, len);

		for (n=rel->exps->h, i = 0; n; n = n->next, i++) {
			sql_exp *e = exps[i] = n->data;

			nr += e->used;
		}

		if (!nr && is_project(rel->op) && len > 0) /* project at least one column if exists */
			exps[0]->used = 1;

		for (i = len-1; i >= 0; i--) {
			sql_exp *e = exps[i];

			if (!is_project(rel->op) || (e->used && !is_set(rel->op))) {
				if (is_project(rel->op))
					nr += exp_mark_used(rel, e, i);
				nr += exp_mark_used(subrel, e, -2);
			}
		}
	}
	/* for count/rank we need at least one column */
	if (!nr && subrel && (is_project(subrel->op) || is_base(subrel->op)) && !list_empty(subrel->exps) &&
		(is_simple_project(rel->op) && project_unsafe(rel, false))) {
		sql_exp *e = subrel->exps->h->data;
		e->used = 1;
	}
	if (rel->r && (is_simple_project(rel->op) || is_groupby(rel->op))) {
		list *l = rel->r;
		node *n;

		for (n=l->h; n; n = n->next) {
			sql_exp *e = n->data;

			e->used = 1;
			/* possibly project/groupby uses columns from the inner */
			exp_mark_used(subrel, e, -2);
		}
	}
}

static void exps_used(list *l);

static void
exp_used(sql_exp *e)
{
	if (e) {
		e->used = 1;

		switch (e->type) {
		case e_convert:
			exp_used(e->l);
			break;
		case e_func:
		case e_aggr:
			exps_used(e->l);
			break;
		case e_cmp:
			if (e->flag == cmp_filter) {
				exps_used(e->l);
				exps_used(e->r);
			} else if (e->flag == cmp_con || e->flag == cmp_dis) {
				exps_used(e->l);
			} else if (e->flag == cmp_in || e->flag == cmp_notin) {
				exp_used(e->l);
				exps_used(e->r);
			} else {
				exp_used(e->l);
				exp_used(e->r);
				if (e->f)
					exp_used(e->f);
			}
			break;
		default:
			break;
		}
	}
}

static void
exps_used(list *l)
{
	if (l) {
		for (node *n = l->h; n; n = n->next)
			exp_used(n->data);
	}
}

static void
rel_used(sql_rel *rel)
{
	if (!rel)
		return;
	if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op) || is_modify(rel->op)) {
		rel_used(rel->l);
		rel_used(rel->r);
	} else if (rel->op == op_munion) {
		list *l = rel->l;
		for(node *n = l->h; n; n = n->next)
			rel_used(n->data);
	} else if (is_topn(rel->op) || is_select(rel->op) || is_sample(rel->op)) {
		rel_used(rel->l);
		rel = rel->l;
	} else if (is_ddl(rel->op)) {
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			rel_used(rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel_used(rel->l);
			rel_used(rel->r);
		} else if (rel->flag == ddl_psm) {
			exps_used(rel->exps);
		}
	} else if (rel->op == op_table) {
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			rel_used(rel->l);
		exp_used(rel->r);
	}
	if (rel && rel->exps) {
		exps_used(rel->exps);
		if (rel->r && (is_simple_project(rel->op) || is_groupby(rel->op)))
			exps_used(rel->r);
	}
}

static void
rel_mark_used(mvc *sql, sql_rel *rel, int proj)
{
	if (proj && (need_distinct(rel)))
		rel_used(rel);

	switch(rel->op) {
	case op_basetable:
	case op_truncate:
	case op_insert:
		break;

	case op_table:

		if (rel->l && rel->flag != TRIGGER_WRAPPER) {
			rel_used(rel);
			if (rel->r)
				exp_mark_used(rel->l, rel->r, -2);
			rel_mark_used(sql, rel->l, proj);
		}
		break;

	case op_topn:
	case op_sample:
		if (proj) {
			rel = rel ->l;
			rel_mark_used(sql, rel, proj);
			break;
		}
		/* fall through */
	case op_project:
	case op_groupby:
		if (proj && rel->l) {
			rel_exps_mark_used(sql->sa, rel, rel->l);
			rel_mark_used(sql, rel->l, 0);
		} else if (proj) {
			rel_exps_mark_used(sql->sa, rel, NULL);
		}
		break;
	case op_update:
	case op_delete:
		if (proj && rel->r) {
			rel_used(rel);
			sql_rel *r = rel->r;

			if (!list_empty(r->exps)) {
				for (node *n = r->exps->h; n; n = n->next) {
					sql_exp *e = n->data;
					const char *nname = exp_name(e);

					if (nname && nname[0] == '%' && strcmp(nname, TID) == 0) { /* TID is used */
						e->used = 1;
						break;
					}
				}
			}
			rel_exps_mark_used(sql->sa, rel, rel->r);
			rel_mark_used(sql, rel->r, 0);
		}
		break;

	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_mark_used(sql, rel->l, 0);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_mark_used(sql, rel->l, 0);
			if (rel->r)
				rel_mark_used(sql, rel->r, 0);
		}
		break;

	case op_select:
		if (rel->l) {
			rel_exps_mark_used(sql->sa, rel, rel->l);
			rel_mark_used(sql, rel->l, 0);
		}
		break;

	case op_inter:
	case op_except:
		/* For now we mark all union expression as used */

		/* Later we should (in case of union all) remove unused
		 * columns from the projection.
		 *
		 * Project part of union is based on column position.
		 */
		if (proj && (need_distinct(rel) || !rel->exps)) {
			rel_used(rel);
			if (!rel->exps) {
				rel_used(rel->l);
				rel_used(rel->r);
			}
			rel_mark_used(sql, rel->l, 0);
			rel_mark_used(sql, rel->r, 0);
		} else if (proj && !need_distinct(rel)) {
			sql_rel *l = rel->l;

			positional_exps_mark_used(rel, l);
			rel_exps_mark_used(sql->sa, rel, l);
			rel_mark_used(sql, rel->l, 0);
			/* based on child check set expression list */
			if (is_project(l->op) && need_distinct(l))
				positional_exps_mark_used(l, rel);
			positional_exps_mark_used(rel, rel->r);
			rel_exps_mark_used(sql->sa, rel, rel->r);
			rel_mark_used(sql, rel->r, 0);
		}
		break;

	case op_munion:
		assert(rel->l);
		if (proj && (need_distinct(rel) || !rel->exps)) {
			rel_used(rel);
			if (!rel->exps) {
				for (node *n = ((list*)rel->l)->h; n; n = n->next) {
					rel_used(n->data);
					rel_mark_used(sql, n->data, 0);
				}
			}
		} else if (proj && !need_distinct(rel)) {
			bool first = true;
			for (node *n = ((list*)rel->l)->h; n; n = n->next) {
				sql_rel *l = n->data;

				positional_exps_mark_used(rel, l);
				rel_exps_mark_used(sql->sa, rel, l);
				rel_mark_used(sql, l, 0);
				/* based on child check set expression list */
				if (first && is_project(l->op) && need_distinct(l))
					positional_exps_mark_used(l, rel);
				first = false;
			}
		}
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
		rel_exps_mark_used(sql->sa, rel, rel->l);
		rel_exps_mark_used(sql->sa, rel, rel->r);
		rel_mark_used(sql, rel->l, 0);
		rel_mark_used(sql, rel->r, 0);
		break;
	}
}

static sql_rel *rel_dce_sub(mvc *sql, sql_rel *rel);

static sql_rel *
rel_remove_unused(mvc *sql, sql_rel *rel)
{
	int needed = 0;

	if (!rel)
		return rel;

	switch(rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isReplicaTable(t)) /* TODO fix rewriting in rel_distribute.c */
			return rel;
	}
	/* fall through */
	case op_table:
		if (rel->exps && (rel->op != op_table || !IS_TABLE_PROD_FUNC(rel->flag))) {
			for(node *n=rel->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!e->used)
					needed = 1;
			}

			if (!needed)
				return rel;

			for(node *n=rel->exps->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data;

				/* at least one (needed for crossproducts, count(*), rank() and single value projections) !, handled by rel_exps_mark_used */
				if (!e->used && list_length(rel->exps) > 1)
					list_remove_node(rel->exps, NULL, n);
				n = next;
			}
		}
		if (rel->op == op_table && (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION))
			rel->l = rel_remove_unused(sql, rel->l);
		return rel;

	case op_topn:
	case op_sample:

		if (rel->l)
			rel->l = rel_remove_unused(sql, rel->l);
		return rel;

	case op_project:
	case op_groupby:

		if (/*rel->l &&*/ rel->exps) {
			for(node *n=rel->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!e->used)
					needed = 1;
			}
			if (!needed)
				return rel;

			for(node *n=rel->exps->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data;

				/* at least one (needed for crossproducts, count(*), rank() and single value projections) */
				if (!e->used && list_length(rel->exps) > 1)
					list_remove_node(rel->exps, NULL, n);
				n = next;
			}
		}
		return rel;

	case op_join:
	case op_left:
	case op_right:
	case op_full:
		if (list_length(rel->attr) > 1) {
			for(node *n=rel->attr->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!e->used)
					needed = 1;
			}
			if (!needed)
				return rel;

			for(node *n=rel->attr->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data;

				if (!e->used)
					list_remove_node(rel->attr, NULL, n);
				n = next;
			}
		}
		return rel;

	case op_inter:
	case op_except:
	case op_munion:

	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:

	case op_select:

	case op_semi:
	case op_anti:
		return rel;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel->l = rel_remove_unused(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel->l = rel_remove_unused(sql, rel->l);
			if (rel->r)
				rel->r = rel_remove_unused(sql, rel->r);
		}
		return rel;
	}
	return rel;
}

static void
rel_dce_refs(mvc *sql, sql_rel *rel, list *refs)
{
	if (!rel || (rel_is_ref(rel) && list_find(refs, rel, NULL)))
		return ;

	switch(rel->op) {
	case op_table:
	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:
	case op_select:

		if (rel->l && (rel->op != op_table || rel->flag != TRIGGER_WRAPPER))
			rel_dce_refs(sql, rel->l, refs);
		break;

	case op_basetable:
	case op_insert:
	case op_truncate:
		break;

	case op_update:
	case op_delete:

		if (rel->r)
			rel_dce_refs(sql, rel->r, refs);
		break;

	case op_inter:
	case op_except:
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:

		if (rel->l)
			rel_dce_refs(sql, rel->l, refs);
		if (rel->r)
			rel_dce_refs(sql, rel->r, refs);
		break;
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			rel_dce_refs(sql, n->data, refs);
		break;
	case op_ddl:

		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_dce_refs(sql, rel->l, refs);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_dce_refs(sql, rel->l, refs);
			if (rel->r)
				rel_dce_refs(sql, rel->r, refs);
		} break;
	}

	if (rel_is_ref(rel) && !list_find(refs, rel, NULL))
		list_prepend(refs, rel);
}

static sql_rel *
rel_dce_down(mvc *sql, sql_rel *rel, int skip_proj)
{
	if (!rel)
		return rel;

	if (!skip_proj && rel_is_ref(rel))
		return rel;

	switch(rel->op) {
	case op_basetable:
	case op_table:

		if (skip_proj && rel->l && rel->op == op_table && rel->flag != TRIGGER_WRAPPER)
			rel->l = rel_dce_down(sql, rel->l, 0);
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		/* fall through */

	case op_truncate:
		return rel;

	case op_insert:
		rel_used(rel->r);
		rel_dce_sub(sql, rel->r);
		return rel;

	case op_update:
	case op_delete:

		if (skip_proj && rel->r)
			rel->r = rel_dce_down(sql, rel->r, 0);
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		return rel;

	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:

		if (skip_proj && rel->l)
			rel->l = rel_dce_down(sql, rel->l, is_topn(rel->op) || is_sample(rel->op));
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		return rel;

	case op_inter:
	case op_except:
		if (skip_proj) {
			if (rel->l)
				rel->l = rel_dce_down(sql, rel->l, 0);
			if (rel->r)
				rel->r = rel_dce_down(sql, rel->r, 0);
		}
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		return rel;

	case op_munion:
		if (skip_proj) {
			for (node *n = ((list*)rel->l)->h; n; n = n->next)
				n->data = rel_dce_down(sql, n->data, 0);
		}
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		return rel;
	case op_select:
		if (rel->l)
			rel->l = rel_dce_down(sql, rel->l, 0);
		return rel;

	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
		if (rel->l)
			rel->l = rel_dce_down(sql, rel->l, 0);
		if (rel->r)
			rel->r = rel_dce_down(sql, rel->r, 0);
		if (!skip_proj && !list_empty(rel->attr))
			rel_dce_sub(sql, rel);
		return rel;

	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel->l = rel_dce_down(sql, rel->l, 0);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel->l = rel_dce_down(sql, rel->l, 0);
			if (rel->r)
				rel->r = rel_dce_down(sql, rel->r, 0);
		}
		return rel;
	}
	return rel;
}

/* DCE
 *
 * Based on top relation expressions mark sub expressions as used.
 * Then recurse down until the projections. Clean them up and repeat.
 */

static sql_rel *
rel_dce_sub(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	/*
	 * Mark used up until the next project
	 * For setops we need to first mark, then remove
	 * because of positional dependency
	 */
	rel_mark_used(sql, rel, 1);
	rel = rel_remove_unused(sql, rel);
	rel_dce_down(sql, rel, 1);
	return rel;
}

/*
sql_rel *
rel_deadcode_elimination(mvc *sql, sql_rel *rel)
{
	rel_used(rel);
	return rel_dce_sub(sql, rel);
}
*/

/* add projects under set ops */
static sql_rel *
rel_add_projects(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	switch(rel->op) {
	case op_basetable:
	case op_truncate:
		return rel;
	case op_insert:
	case op_update:
	case op_delete:
		if (rel->r)
			rel->r = rel_add_projects(sql, rel->r);
		return rel;
	case op_inter:
	case op_except:
		/* We can only reduce the list of expressions of an set op
		 * if the projection under it can also be reduced.
		 */
		if (rel->l) {
			sql_rel *l = rel->l;

			if (!is_project(l->op) && !need_distinct(rel))
				l = rel_project(sql->sa, l, rel_projections(sql, l, NULL, 1, 1));
			rel->l = rel_add_projects(sql, l);
		}
		if (rel->r) {
			sql_rel *r = rel->r;

			if (!is_project(r->op) && !need_distinct(rel))
				r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
			rel->r = rel_add_projects(sql, r);
		}
		return rel;
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next) {
			sql_rel* r = n->data;
			if (!is_project(r->op) && !need_distinct(rel))
				r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
			r = rel_add_projects(sql, r);
			n->data = r;
		}
		return rel;
	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:
	case op_select:
	case op_table:
		if (rel->l && (rel->op != op_table || rel->flag != TRIGGER_WRAPPER))
			rel->l = rel_add_projects(sql, rel->l);
		return rel;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
		if (rel->l)
			rel->l = rel_add_projects(sql, rel->l);
		if (rel->r)
			rel->r = rel_add_projects(sql, rel->r);
		return rel;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel->l = rel_add_projects(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel->l = rel_add_projects(sql, rel->l);
			if (rel->r)
				rel->r = rel_add_projects(sql, rel->r);
		}
		return rel;
	}
	return rel;
}

static sql_rel *
rel_dce_(mvc *sql, sql_rel *rel)
{
	list *refs = sa_list(sql->sa);

	rel_dce_refs(sql, rel, refs);
	if (refs) {
		for(node *n = refs->h; n; n = n->next) {
			sql_rel *i = n->data;

			while (!rel_is_ref(i) && i->l && !is_base(i->op))
				i = i->l;
			if (i)
				rel_used(i);
		}
	}
	rel = rel_add_projects(sql, rel);
	rel_used(rel);
	rel_dce_sub(sql, rel);
	return rel;
}


/* Remove unused expressions */
static sql_rel *
rel_dce(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_dce_(v->sql, rel);
}

/* keep export for other projects */
sql_rel *
rel_deadcode_elimination(mvc *sql, sql_rel *rel)
{
	return rel_dce_(sql, rel);
}

run_optimizer
bind_dce(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_cycle == 0 && gp->opt_level == 1 && (flag & dce) ? rel_dce : NULL;
}


static int
topn_sample_safe_exps( list *exps, bool nil_limit )
{
	/* Limit only expression lists are always save */
	if (list_length(exps) == 1)
		return 1;
	for (node *n = exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (!e || e->type != e_atom || (!nil_limit && exp_is_null(e)))
			return 0;
	}
	return 1;
}

static list *
sum_limit_offset(mvc *sql, sql_rel *rel)
{
	/* for sample we always propagate, or if the expression list only consists of a limit expression, we copy it */
	if (is_sample(rel->op) || list_length(rel->exps) == 1)
		return exps_copy(sql, rel->exps);
	assert(list_length(rel->exps) == 2);
	sql_subtype *lng = sql_fetch_localtype(TYPE_lng);
	sql_exp *add = rel_binop_(sql, NULL, exp_copy(sql, rel->exps->h->data), exp_copy(sql, rel->exps->h->next->data), "sys", "sql_add", card_value, true);
	/* for remote plans, make sure the output type is a bigint */
	if (subtype_cmp(lng, exp_subtype(add)) != 0)
		add = exp_convert(sql, add, exp_subtype(add), lng);
	return list_append(sa_list(sql->sa), add);
}

/*
 * Push TopN (only LIMIT, no ORDER BY) down through projections underneath crossproduct, i.e.,
 *
 *     topn(                          topn(
 *         project(                       project(
 *             crossproduct(                  crossproduct(
 *                 L,           =>                topn( L )[ n ],
 *                 R                              topn( R )[ n ]
 *             )                              )
 *         )[ Cs ]*                       )[ Cs ]*
 *     )[ n ]                         )[ n ]
 *
 *  (TODO: in case of n==1 we can omit the original top-level TopN)
 *
 * also push topn under (non reordering) projections.
 */
static sql_rel *
rel_push_topn_and_sample_down_(visitor *v, sql_rel *rel)
{
	sql_rel *rp = NULL, *r = rel->l, *rpp = NULL;

	if (is_topn(rel->op) && !rel_is_ref(rel) &&
			r && r->op == op_table && r->flag != TRIGGER_WRAPPER && !rel_is_ref(r) && r->r) {
		sql_exp *op = r->r;
		sql_subfunc *f = op->f;

		if (is_func(op->type) && strcmp(f->func->base.name, "file_loader") == 0 && !sql_func_mod(f->func)[0] && !sql_func_imp(f->func)[0]) {
			/* push limit, to arguments of file_loader */
			list *args = op->l;
			if (list_length(args) == 2) {
				sql_exp *topN = rel->exps->h->data;
				sql_exp *offset = rel->exps->h->next? rel->exps->h->next->data:NULL;
				atom *topn = topN->l;
				if (offset) {
						atom *b1 = (atom *)offset->l, *c = atom_add(v->sql->sa, b1, topn);

						if (!c)
							return rel;
						if (atom_cmp(c, topn) < 0) /* overflow */
							return rel;
						topn = c;
				}
				append(args, exp_atom(v->sql->sa, topn));
				v->changes++;
			}
		}
	}

	if ((is_topn(rel->op) || is_sample(rel->op)) && topn_sample_safe_exps(rel->exps, true)) {
		sql_rel *(*func) (allocator *, sql_rel *, list *) = is_topn(rel->op) ? rel_topn : rel_sample;

		/* nested topN relations */
		if (r && is_topn(rel->op) && is_topn(r->op) && !rel_is_ref(r)) {
			sql_exp *topN1 = rel->exps->h->data, *topN2 = r->exps->h->data;
			sql_exp *offset1 = list_length(rel->exps) > 1 ? rel->exps->h->next->data : NULL;
			sql_exp *offset2 = list_length(r->exps) > 1 ? r->exps->h->next->data : NULL;

			if (topN1->l && topN2->l && (!offset1 || offset1->l) && (!offset2 || offset2->l)) { /* no parameters */
				bool changed = false;

				if ((!offset1 || (offset1->type == e_atom && offset1->l)) && (!offset2 || (offset2->type == e_atom && offset2->l))) { /* only atoms */
					if (!offset1 && offset2) {
						list_append(rel->exps, exp_copy(v->sql, offset2));
						changed = true;
					} else if (offset1 && offset2) { /* sum offsets */
						atom *b1 = (atom *)offset1->l, *b2 = (atom *)offset2->l, *c = atom_add(v->sql->sa, b1, b2);

						if (!c) /* error, don't apply optimization, WARNING because of this the offset optimization must come before the limit one */
							return rel;
						if (atom_cmp(c, b2) < 0) /* overflow */
							c = atom_int(v->sql->sa, sql_fetch_localtype(TYPE_lng), GDK_lng_max);
						offset1->l = c;
						changed = true;
					}
				}

				if (topN1->type == e_atom && topN1->l && topN2->type == e_atom && topN2->l) { /* only atoms */
					atom *a1 = (atom *)topN1->l, *a2 = (atom *)topN2->l;

					if (!a2->isnull && (a1->isnull || atom_cmp(a1, a2) >= 0)) { /* topN1 is not set or is larger than topN2 */
						rel->exps->h->data = exp_copy(v->sql, topN2);
						changed = true;
					}
				}

				if (changed) {
					rel->l = r->l;
					r->l = NULL;
					rel_destroy(r);
					v->changes++;
					return rel;
				}
			}
		}

		if (r && is_simple_project(r->op) && (need_distinct(r) || project_unsafe(r, 1)))
			return rel;

		/* push topn/sample under projections */
		if (!rel_is_ref(rel) && r && is_simple_project(r->op) && !need_distinct(r) && !rel_is_ref(r) && r->l && list_empty(r->r)) {
			sql_rel *x = r, *px = x;

			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r)) {
				px = x;
				x = x->l;
			}
			/* only push topn once */
			if (x && x->op == rel->op)
				return rel;

			rel->l = x;
			px->l = rel;
			rel = r;
			v->changes++;
			return rel;
		}

		if (!topn_sample_safe_exps(rel->exps, false))
			return rel;

		/* duplicate topn/sample direct under crossproduct */
		if (r && !rel_is_ref(r) && r->l && r->r && r->op == op_join && list_empty(r->exps)) {
			sql_rel *u = r, *x;
			sql_rel *ul = u->l;
			sql_rel *ur = u->r;
			bool changed = false;

			x = ul;
			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
				x = x->l;
			if (x && x->op != rel->op) { /* only push topn once */
				ul = func(v->sql->sa, ul, sum_limit_offset(v->sql, rel));
				set_processed(ul);
				u->l = ul;
				changed = true;
			}

			x = ur;
			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
				x = x->l;
			if (x && x->op != rel->op) { /* only push topn once */
				ur = func(v->sql->sa, ur, sum_limit_offset(v->sql, rel));
				set_processed(ur);
				u->r = ur;
				changed = true;
			}

			if (changed)
				v->changes++;
			return rel;
		}

		/* duplicate topn/sample direct under union */
		if (r && !rel_is_ref(r) && r->l && r->r && is_munion(r->op) && r->exps) {
			list *rels = r->l;
			bool changed = false;
			for(node *n = rels->h; n; n = n->next) {
				sql_rel *ue = n->data, *x;

				x = ue;
				while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
					x = x->l;
				if (x && x->op != rel->op) { /* only push topn once */
					ue = func(v->sql->sa, ue, sum_limit_offset(v->sql, rel));
					set_processed(ue);
					n->data = ue;
					changed = true;
				}
			}
			if (changed)
				v->changes++;
			return rel;
		}

		/* duplicate topn/sample + [ project-order ] under union */
		if (r && !rp)
			rp = r->l;
		if (r && r->exps && is_simple_project(r->op) && !rel_is_ref(r) && !list_empty(r->r) && r->l && is_munion(rp->op)) {
			list *rels = rp->l;

			/* only push topn/sample once */
			for(node *n = rels->h; n; n = n->next) {
				sql_rel *ue = n->data, *x;
				x = ue;
				while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
					x = x->l;
				if (x && x->op == rel->op)
					return rel;
			}

			sql_rel *u = rp, *ou = u, *up = NULL;
			list *rcopy = NULL;

			rcopy = exps_copy(v->sql, r->r);
			for (node *n = rcopy->h ; n ; n = n->next) {
				sql_exp *e = n->data;
				set_descending(e); /* remove ordering properties for projected columns */
				set_nulls_first(e);
			}

			list *nrels = sa_list(v->sql->sa);
			for(node *n = rels->h; n; n = n->next) {
				sql_rel *ue = n->data;

				ue = rel_dup(ue);
				if (!is_project(ue->op))
					ue = rel_project(v->sql->sa, ue,
						rel_projections(v->sql, ue, NULL, 1, 1));
				rel_rename_exps(v->sql, u->exps, ue->exps);

				/* introduce projects under the set */
				ue = rel_project(v->sql->sa, ue, NULL);
				ue->exps = exps_copy(v->sql, r->exps);
				/* possibly add order by column */
				ue->exps = list_distinct(list_merge(ue->exps, exps_copy(v->sql, rcopy), NULL), (fcmp) exp_equal, (fdup) NULL);
				ue->nrcols = list_length(ue->exps);
				ue->r = exps_copy(v->sql, r->r);
				set_processed(ue);
				ue = func(v->sql->sa, ue, sum_limit_offset(v->sql, rel));
				set_processed(ue);

				append(nrels, ue);
			}

			u = rel_setop_n_ary(v->sql->sa, nrels, op_munion);
			u->exps = exps_alias(v->sql, r->exps);
			u->nrcols = list_length(u->exps);
			set_processed(u);
			/* possibly add order by column */
			u->exps = list_distinct(list_merge(u->exps, rcopy, NULL), (fcmp) exp_equal, (fdup) NULL);

			if (need_distinct(r)) {
				for(node *n = nrels->h; n; n = n->next) {
					sql_rel *ue = n->data;
					set_distinct(ue);
				}
			}

			/* zap names */
			rel_no_rename_exps(u->exps);
			rel_destroy(ou);

			up = rel_project(v->sql->sa, u, exps_alias(v->sql, r->exps));
			up->r = r->r;
			r->l = NULL;

			if (need_distinct(r))
				set_distinct(up);

			rel_destroy(r);
			rel->l = up;
			v->changes++;
			return rel;
		}
		/* a  left outer join b order by a.* limit L, can be copied into a */
		/* topn ( project (orderby)( optional project ( left ())
		 * rel    r                                     rp */
		if (r && !rp)
			rp = r->l;
		if (r && rp && is_simple_project(rp->op) && !rp->r && rp->l)
			rpp = rp->l;
		if (r && r->exps && is_simple_project(r->op) && !rel_is_ref(r) && r->r && r->l && ((!rpp && is_left(rp->op)) ||
				(rpp && is_left(rpp->op)))) {
			sql_rel *lj = rpp?rpp:rp;
			sql_rel *l = lj->l;
			list *obes = r->r, *nobes = sa_list(v->sql->sa);
			int fnd = 1;
			for (node *n = obes->h; n && fnd; n = n->next) {
				sql_exp *obe = n->data;
				int part = is_partitioning(obe);
				int asc = is_ascending(obe);
				int nl = nulls_last(obe);
				/* only simple rename expressions */
				sql_exp *pe = exps_find_exp(r->exps, obe);
				if (pe && rpp)
					pe = exps_find_exp(rp->exps, pe);
				if (pe)
					pe = rel_find_exp(l, pe);
				if (pe && pe->type == e_column) {
					if (exp_is_atom(pe))
						return rel;
					pe = exp_ref(v->sql, pe);
					if (part)
						set_partitioning(pe);
					if (asc)
						set_ascending(pe);
					if (nl)
						set_nulls_last(pe);
					append(nobes, pe);
				}
				else
					fnd = 0;
			}
			if (fnd && ((is_topn(rel->op) && !is_topn(l->op)) || (is_sample(rel->op) && !is_sample(l->op)))) {
				/* inject topn */
				/* Todo add order by */
				sql_rel *ob = lj->l = rel_project(v->sql->sa, lj->l, rel_projections(v->sql, lj->l, NULL, 1, 1));
				ob->r = nobes;
				lj->l = func(v->sql->sa, lj->l, sum_limit_offset(v->sql, rel));
				v->changes++;
				return rel;
			}
		}
	}
	return rel;
}

static sql_rel *
rel_push_topn_and_sample_down(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_push_topn_and_sample_down_);
}

run_optimizer
bind_push_topn_and_sample_down(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_topn] || gp->cnt[op_sample]) &&
		   (flag & push_topn_and_sample_down) ? rel_push_topn_and_sample_down : NULL;
}
