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
#include "rel_optimizer_private.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_select.h"
#include "rel_rewriter.h"

static int
exp_is_rename(sql_exp *e)
{
	return (e->type == e_column);
}

static int
exp_is_useless_rename(sql_exp *e)
{
	return (e->type == e_column && e->alias.label == e->nid);
}

static list *
rel_used_projections(mvc *sql, list *exps, list *users)
{
	list *nexps = sa_list(sql->sa);
	bool *used = SA_ZNEW_ARRAY(sql->ta, bool, list_length(exps));
	int i = 0;

	for(node *n = users->h; n; n = n->next) {
		sql_exp *e = n->data, *ne = NULL;
		assert(e->nid && exps_bind_nid(exps, e->nid));
		if (e->nid && (ne = exps_bind_nid(exps, e->nid))) {
			used[list_position(exps, ne)] = 1;
		}
	}
	for(node *n = exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		if (is_intern(e) || used[i])
			append(nexps, e);
	}
	return nexps;
}

/* move projects down with the goal op removing them completely (ie push renames/reduced lists into basetable)
 * for some cases we can directly remove iff renames rename into same alias
 * */
static sql_rel *
rel_push_project_down_(visitor *v, sql_rel *rel)
{
	/* for now only push down renames */
	if (v->depth > 1 && is_simple_project(rel->op) && !need_distinct(rel) && !rel_is_ref(rel) && rel->l && !rel->r &&
			v->parent &&
			!is_modify(v->parent->op) && !is_topn(v->parent->op) && !is_sample(v->parent->op) &&
			!is_ddl(v->parent->op) && !is_set(v->parent->op) && !is_munion(v->parent->op) &&
			list_check_prop_all(rel->exps, (prop_check_func)&exp_is_rename)) {
		sql_rel *l = rel->l;

		if (rel_is_ref(l))
			return rel;
		if (is_basetable(l->op)) {
			if (list_check_prop_all(rel->exps, (prop_check_func)&exp_is_useless_rename)) {
				/* TODO reduce list (those in the project + internal) */
				rel->l = NULL;
				l->exps = rel_used_projections(v->sql, l->exps, rel->exps);
				rel_destroy(rel);
				v->changes++;
				return l;
			}
			return rel;
		} else if (list_check_prop_all(rel->exps, (prop_check_func)&exp_is_useless_rename)) {
			if ((is_project(l->op) && list_length(l->exps) == list_length(rel->exps)) ||
				((v->parent && is_project(v->parent->op)) &&
				 (is_mset(l->op) || is_set(l->op) || is_select(l->op) || is_join(l->op) || is_semi(l->op) || is_topn(l->op) || is_sample(l->op)))) {
				rel->l = NULL;
				rel_destroy(rel);
				v->changes++;
				return l;
			}
		}
	}
	/* ToDo handle useful renames, ie new relation name and unique set of attribute names (could reduce set of * attributes) */
	/* handle both useless and useful with project [ group by ] */
	return rel;
}

static sql_rel *
rel_push_project_down(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_push_project_down_);
}

run_optimizer
bind_push_project_down(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (flag & push_project_down) &&
		   (gp->cnt[op_project] || gp->cnt[op_groupby]) ? rel_push_project_down : NULL;
}


static bool exp_shares_exps(sql_exp *e, list *shared, uint64_t *uses);

static bool
exps_shares_exps(list *exps, list *shared, uint64_t *uses)
{
	if (!exps || !shared)
		return false;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (exp_shares_exps(e, shared, uses))
			return true;
	}
	return false;
}

static bool
exp_shares_exps(sql_exp *e, list *shared, uint64_t *uses)
{
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_filter)
			return exps_shares_exps(e->l, shared, uses) || exps_shares_exps(e->r, shared, uses);
		else if (e->flag == cmp_con || e->flag == cmp_dis)
			return exps_shares_exps(e->l, shared, uses);
		else if (e->flag == cmp_in || e->flag == cmp_notin)
			return exp_shares_exps(e->l, shared, uses) || exps_shares_exps(e->r, shared, uses);
		else
			return exp_shares_exps(e->l, shared, uses) || exp_shares_exps(e->r, shared, uses) || (e->f && exp_shares_exps(e->f, shared, uses));
	case e_atom:
		if (e->f)
			return exps_shares_exps(e->f, shared, uses);
		return false;
	case e_column:
		{
			sql_exp *ne = NULL;
			assert(e->nid);
			if (e->nid)
				ne = exps_bind_nid(shared, e->nid);
			if (!ne)
				return false;
			if (ne->type != e_column) {
				int i = list_position(shared, ne);
				if (i < 0)
					return false;
				uint64_t used = (uint64_t) 1 << i;
				if (used & *uses)
					return true;
				*uses |= used;
				return false;
			}
			/* todo better check nested */
			if (ne != e && !ne->nid)
				return true;
			if (ne != e && (list_position(shared, e) < 0 || list_position(shared, e) > list_position(shared, ne)))
				/* maybe ne refers to a local complex exp */
				return exp_shares_exps(ne, shared, uses);
			return false;
		}
	case e_convert:
		return exp_shares_exps(e->l, shared, uses);
	case e_aggr:
	case e_func:
		return exps_shares_exps(e->l, shared, uses);
	case e_psm:
		assert(0);  /* not in projection list */
	}
	return false;
}

static bool
exps_share_expensive_exp(list *exps, list *shared )
{
	uint64_t uses = 0;

	if (list_empty(exps) || list_empty(shared))
		return false;
	if (list_length(shared) > 64)
		return true;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (exp_shares_exps(e, shared, &uses))
			return true;
	}
	return false;
}

static bool ambigious_ref( list *exps, sql_exp *e);
static bool
ambigious_refs( list *exps, list *refs)
{
	if (list_empty(refs))
		return false;
	for(node *n=refs->h; n; n = n->next) {
		if (ambigious_ref(exps, n->data))
			return true;
	}
	return false;
}

static bool
ambigious_ref( list *exps, sql_exp *e)
{
	sql_exp *ne = NULL;

	if (e->type == e_column) {
		assert(e->nid);
		if (e->nid)
			ne = exps_bind_nid(exps, e->nid);
		if (ne && e != ne)
			return true;
	}
	if (e->type == e_func)
		return ambigious_refs(exps, e->l);
	return false;
}

/* merge 2 projects into the lower one */
static sql_rel *
rel_merge_projects_(visitor *v, sql_rel *rel)
{
	list *exps = rel->exps;
	sql_rel *prj = rel->l;
	node *n;

	if (rel->op == op_project &&
	    prj && prj->op == op_project && !(rel_is_ref(prj)) && list_empty(prj->r)) {
		int all = 1;

		if (project_unsafe(rel, false) || project_unsafe(prj, false) || exps_share_expensive_exp(rel->exps, prj->exps))
			return rel;

		/* here we try to fix aliases */
		list *nexps = NULL;
		/* for each exp check if we can rename it */
		for (n = exps->h; n && all; n = n->next) {
			sql_exp *e = n->data, *ne = NULL;

			/* We do not handle expressions pointing back in the list */
			/*
			if (ambigious_ref(exps, e)) {
				all = 0;
				break;
			}
			*/
			ne = exp_push_down_prj(v->sql, e, prj, prj->l);
			/* check if the referred alias name isn't used twice */
			/*
			if (ne && ambigious_ref(nexps, ne)) {
				all = 0;
				break;
			}
			*/
			if (ne) {
				if (exp_name(e))
					exp_prop_alias(v->sql->sa, ne, e);
				if (!nexps)
					nexps = new_exp_list(v->sql->sa);
				list_append(nexps, ne);
			} else {
				all = 0;
			}
		}
		if (all) {
			rel->exps = nexps;
			/* we can now remove the intermediate project */
			/* push order by expressions */
			if (!list_empty(rel->r)) {
				list *nr = new_exp_list(v->sql->sa), *res = rel->r;
				for (n = res->h; n; n = n->next) {
					sql_exp *e = n->data, *ne = NULL;

					ne = exp_push_down_prj(v->sql, e, prj, prj->l);
					if (ne) {
						if (exp_name(e))
							exp_prop_alias(v->sql->sa, ne, e);
						list_append(nr, ne);
					} else {
						all = 0;
					}
				}
				if (all) {
					rel->r = nr;
				} else {
					/* leave as is */
					rel->exps = exps;
					return rel;
				}
			}
			rel->l = prj->l;
			prj->l = NULL;
			rel_destroy(prj);
			v->changes++;
			return rel_merge_projects_(v, rel);
		} else {
			/* leave as is */
			rel->exps = exps;
		}
		return rel;
	}
	return rel;
}

static sql_rel *
rel_merge_projects(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_merge_projects_);
}

run_optimizer
bind_merge_projects(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (flag & merge_projects) &&
		   (gp->cnt[op_project] || gp->cnt[op_groupby]) ? rel_merge_projects : NULL;
}


static sql_exp *split_aggr_and_project(mvc *sql, list *aexps, sql_exp *e);

static void
list_split_aggr_and_project(mvc *sql, list *aexps, list *exps)
{
	if (list_empty(exps))
		return ;
	for(node *n = exps->h; n; n = n->next)
		n->data = split_aggr_and_project(sql, aexps, n->data);
}

static sql_exp *
split_aggr_and_project(mvc *sql, list *aexps, sql_exp *e)
{
	switch(e->type) {
	case e_aggr:
		/* add to the aggrs */
		if (!exp_name(e))
			exp_label(sql->sa, e, ++sql->label);
		list_append(aexps, e);
		return exp_ref(sql, e);
	case e_cmp:
		/* e_cmp's shouldn't exist in an aggr expression list */
		assert(0);
	case e_convert:
		e->l = split_aggr_and_project(sql, aexps, e->l);
		return e;
	case e_func:
		list_split_aggr_and_project(sql, aexps, e->l);
		return e;
	case e_atom:
	case e_column: /* constants and columns shouldn't be rewritten */
	case e_psm:
		return e;
	}
	return NULL;
}

/* exp_rename */
static sql_exp * exp_rename(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

static list *
exps_rename(mvc *sql, list *l, sql_rel *f, sql_rel *t)
{
	if (list_empty(l))
		return l;
	for (node *n=l->h; n; n=n->next)
		n->data = exp_rename(sql, n->data, f, t);
	return l;
}

/* exp_rename */
static sql_exp *
exp_rename(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t)
{
	sql_exp *ne = NULL;

	assert(is_project(f->op));

	switch(e->type) {
	case e_column:
		assert(e->nid);
		ne = exps_bind_nid(f->exps, e->nid);
		if (!ne)
			return e;
		sql_exp *oe = e;
		e = NULL;
		if (ne && ne->nid)
			e = rel_find_exp(t, ne);
		if (!e) {
			sql->session->status = 0;
			sql->errstr[0] = 0;
			if (exp_is_atom(ne))
				return ne;
			return oe;
		}
		ne = exp_ref(sql, e);
		if (oe)
			exp_propagate(sql->sa, ne, oe);
		return ne;
	case e_cmp:
		if (e->flag == cmp_filter) {
			e->l = exps_rename(sql, e->l, f, t);
			e->r = exps_rename(sql, e->r, f, t);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			e->l = exps_rename(sql, e->l, f, t);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = exp_rename(sql, e->l, f, t);
			e->r = exps_rename(sql, e->r, f, t);
		} else {
			e->l = exp_rename(sql, e->l, f, t);
			e->r = exp_rename(sql, e->r, f, t);
			if (e->f)
				e->f = exp_rename(sql, e->f, f, t);
		}
		break;
	case e_convert:
		e->l = exp_rename(sql, e->l, f, t);
		break;
	case e_aggr:
	case e_func:
		e->l = exps_rename(sql, e->l, f, t);
		break;
	case e_atom:
		e->f = exps_rename(sql, e->f, f, t);
		break;
	case e_psm:
		break;
	}
	return e;
}

static int
exp_match_exp_cmp( sql_exp *e1, sql_exp *e2)
{
	if (exp_match_exp(e1,e2))
		return 0;
	return -1;
}

/* Pushing projects up the tree. Done very early in the optimizer.
 * Makes later steps easier.
 */
extern void _rel_print(mvc *sql, sql_rel *rel);
static sql_rel *
rel_push_project_up_(visitor *v, sql_rel *rel)
{
	if (is_simple_project(rel->op) && rel->l && !rel_is_ref(rel)) {
		sql_rel *l = rel->l;
		if (is_simple_project(l->op))
			return rel_merge_projects_(v, rel);
		/* find equal column references, later cases are rewritten into references back to the first
		 * project () [ i.i L1, i.i L2 ] -> project() [ i.i L1, L1 L2 ] */
		if (list_length(rel->exps) > 1) {
			list *exps = rel->exps;
			node *n = exps->h;
			bool needed = false;
			for(n = n->next; n && !needed; n = n->next) {
				sql_exp *e = n->data;
				if (e->type == e_column && !is_selfref(e)) {
					for(node *m = exps->h; m && m != n && !needed; m = m->next) {
						sql_exp *h = m->data;
						if (exp_match_exp(h,e))
							needed = true;
					}
				}
			}
			if (needed) {
				rel->exps = sa_list(v->sql->sa);
				node *n = exps->h;
				list_append(rel->exps, n->data);
				for(n = n->next; n; n = n->next) {
					sql_exp *e = n->data;
					if (e->type == e_column && !is_selfref(e)) {
						node *m = list_find(rel->exps, e, (fcmp)&exp_match_exp_cmp);
						if (m) {
							sql_exp *me = m->data;
							if (me->alias.label != me->nid) {
								sql_exp *ne = exp_ref(v->sql, m->data);
								exp_setalias(ne, e->alias.label, exp_relname(e), exp_name(e));
								exp_propagate(v->sql->sa, ne, e);
								set_selfref(ne);
								e = ne;
							}
						}
					}
					list_append(rel->exps, e);
				}
			}
		}
	}

	/* project/project cleanup is done later */
	if (is_join(rel->op) || is_select(rel->op)) {
		node *n;
		list *exps = NULL, *l_exps, *r_exps;
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		sql_rel *t;
		int nlexps = 0, i = 0;

		/* Don't rewrite refs, non projections or constant or
		   order by projections  */
		if (!l || rel_is_ref(l) || is_topn(l->op) || is_sample(l->op) ||
		   (is_join(rel->op) && !list_empty(rel->attr)) ||
		   (is_join(rel->op) && (!r || rel_is_ref(r))) ||
		   (is_select(rel->op) && l->op != op_project) ||
		   (is_join(rel->op) && ((l->op != op_project && r->op != op_project) || is_topn(r->op) || is_sample(r->op))) ||
		  ((l->op == op_project && (!l->l || l->r || project_unsafe(l, is_select(rel->op)))) ||
		   (is_join(rel->op) && (r->op == op_project && (!r->l || r->r || project_unsafe(r, false))))))
			return rel;

		if (l->op == op_project && l->l) {
			for (n = l->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (!(is_column(e->type) && exp_is_atom(e) && !(is_right(rel->op) || is_full(rel->op))) &&
					!(e->type == e_column && !has_label(e)))
						return rel;
			}
		}
		if (is_join(rel->op) && r->op == op_project && r->l) {
			for (n = r->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (!(is_column(e->type) && exp_is_atom(e) && !(is_right(rel->op) || is_full(rel->op))) &&
					!(e->type == e_column && !has_label(e)))
						return rel;
			}
		}

		if (l->op == op_project && l->l) {
			/* Go through the list of project expressions.
			   Check if they can be pushed up, ie are they not
			   changing or introducing any columns used
			   by the upper operator. */

			exps = new_exp_list(v->sql->sa);
			for (n = l->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				/* we cannot rewrite projection with atomic values from outer joins */
				if (is_column(e->type) && exp_is_atom(e) && !(is_right(rel->op) || is_full(rel->op))) {
					list_append(exps, e);
				} else if (e->type == e_column) {
					if (has_label(e))
						return rel;
					list_append(exps, e);
				} else {
					return rel;
				}
			}
		} else {
			exps = rel_projections(v->sql, l, NULL, 1, 1);
		}
		nlexps = list_length(exps);
		/* also handle right hand of join */
		if (is_join(rel->op) && r->op == op_project && r->l && list_empty(rel->attr)) {
			/* Here we also check all expressions of r like above
			   but also we need to check for ambiguous names. */

			for (n = r->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				/* we cannot rewrite projection with atomic values from outer joins */
				if (is_column(e->type) && exp_is_atom(e) && !(is_left(rel->op) || is_full(rel->op))) {
					list_append(exps, e);
				} else if (e->type == e_column) {
					if (has_label(e))
						return rel;
					list_append(exps, e);
				} else {
					return rel;
				}
			}
		} else if (is_join(rel->op) && list_empty(rel->attr)) {
			list *r_exps = rel_projections(v->sql, r, NULL, 1, 1);
			list_merge(exps, r_exps, (fdup)NULL);
		}
		if (!list_empty(rel->attr))
			append(exps, exp_ref(v->sql, rel->attr->h->data));
		/* Here we should check for ambiguous names ? */
		if (is_join(rel->op) && r && list_empty(rel->attr)) {
			t = (l->op == op_project && l->l)?l->l:l;
			l_exps = rel_projections(v->sql, t, NULL, 1, 1);
			/* conflict with old right expressions */
			t = (r->op == op_project && r->l)?r->l:r;
			r_exps = rel_projections(v->sql, t, NULL, 1, 1);
			/* conflict with new right expressions */
			for(n = l_exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_is_atom(e))
					continue;
				if (e->nid && exps_bind_nid(r_exps, e->nid))
					return rel;
				if (e->alias.label && exps_bind_nid(r_exps, e->alias.label))
					return rel;
			}
			/* conflict with new left expressions */
			for(n = r_exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_is_atom(e))
					continue;
				if (e->nid && exps_bind_nid(l_exps, e->nid))
					return rel;
				if (e->alias.label && exps_bind_nid(l_exps, e->alias.label))
					return rel;
			}
		}

		/* rename operator expressions */
		if (l->op == op_project) {
			/* rewrite rel from rel->l into rel->l->l */
			if (rel->exps) {
				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *ne;

					ne = exp_rename(v->sql, e, l, l->l);
					assert(ne);
					if (ne != e && exp_name(e))
						exp_propagate(v->sql->sa, ne, e);
					n->data = ne;
				}
			}
			rel->l = l->l;
			l->l = NULL;
			rel_destroy(l);
		}
		if (is_join(rel->op) && r->op == op_project && list_empty(rel->attr)) {
			/* rewrite rel from rel->r into rel->r->l */
			if (rel->exps) {
				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *ne;

					ne = exp_rename(v->sql, e, r, r->l);
					assert(ne);
					if (ne != e && exp_name(e))
						exp_propagate(v->sql->sa, ne, e);
					n->data = ne;
				}
			}
			rel->r = r->l;
			r->l = NULL;
			rel_destroy(r);
		}
		/* Done, ie introduce new project */
		exps_fix_card(exps, rel->card);
		/* Fix nil flag */
		if (!list_empty(exps)) {
			for (n = exps->h ; n && i < nlexps ; n = n->next, i++) {
				sql_exp *e = n->data;

				if (is_right(rel->op) || is_full(rel->op))
					set_has_nil(e);
				set_not_unique(e);
			}
			for (; n ; n = n->next) {
				sql_exp *e = n->data;

				if (is_left(rel->op) || is_full(rel->op))
					set_has_nil(e);
				set_not_unique(e);
			}
		}
		v->changes++;
		return rel_inplace_project(v->sql->sa, rel, NULL, exps);
	}
	if (is_groupby(rel->op) && !rel_is_ref(rel) && rel->exps && list_length(rel->exps) > 1) {
		node *n;
		int fnd = 0;
		list *aexps, *pexps;

		/* check if some are expressions aren't e_aggr */
		for (n = rel->exps->h; n && !fnd; n = n->next) {
			sql_exp *e = n->data;

			if (e->type != e_aggr && e->type != e_column && e->type != e_atom && e->card > CARD_ATOM) {
				fnd = 1;
			}
		}
		/* only aggr, no rewrite needed */
		if (!fnd)
			return rel;

		aexps = sa_list(v->sql->sa);
		pexps = sa_list(v->sql->sa);
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne = NULL;

			switch (e->type) {
			case e_atom: /* move over to the projection */
				list_append(pexps, e);
				break;
			case e_func:
				list_append(pexps, e);
				list_split_aggr_and_project(v->sql, aexps, e->l);
				break;
			case e_convert:
				list_append(pexps, e);
				e->l = split_aggr_and_project(v->sql, aexps, e->l);
				break;
			default: /* simple alias */
				list_append(aexps, e);
				ne = exp_ref(v->sql, e);
				list_append(pexps, ne);
				break;
			}
		}
		v->changes++;
		rel->exps = aexps;
		return rel_inplace_project( v->sql->sa, rel, NULL, pexps);
	}
	return rel;
}

static sql_rel *
rel_push_project_up(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_push_project_up_);
}

run_optimizer
bind_push_project_up(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (flag & push_project_up) &&
		   gp->cnt[op_project] ? rel_push_project_up : NULL;
}


static void split_exps(mvc *sql, list *exps, sql_rel *rel);

static int
exp_refers_cmp( sql_exp *e1, sql_exp *e2)
{
	if (exp_refers(e1,e2))
		return 0;
	return -1;
}

sql_exp *
add_exp_too_project(mvc *sql, sql_exp *e, sql_rel *rel)
{
	node *n = list_find(rel->exps, e, (fcmp)&exp_match_exp_cmp);

	/* if not matching we may refer to an older expression */
	if (!n)
		n = list_find(rel->exps, e, (fcmp)&exp_refers_cmp);
	if (!n) {
		exp_label(sql->sa, e, ++sql->label);
		append(rel->exps, e);
	} else {
		sql_exp *ne = n->data;

		if (rel && rel->l) {
			if (ne && ne->alias.label && rel_find_nid(rel->l, ne->alias.label)) {
				exp_label(sql->sa, e, ++sql->label);
				append(rel->exps, e);
				ne = e;
			}
		}
		e = ne;
	}
	e = exp_ref(sql, e);
	return e;
}

static void
add_exps_too_project(mvc *sql, list *exps, sql_rel *rel)
{
	node *n;

	if (!exps)
		return;
	for(n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type != e_column && !exp_is_atom(e))
			n->data = add_exp_too_project(sql, e, rel);
	}
}

static sql_exp *
split_exp(mvc *sql, sql_exp *e, sql_rel *rel)
{
	if (exp_is_atom(e))
		return e;
	switch(e->type) {
	case e_column:
		return e;
	case e_convert:
		e->l = split_exp(sql, e->l, rel);
		return e;
	case e_aggr:
	case e_func:
		if (!is_analytic(e) && !exp_has_sideeffect(e)) {
			sql_subfunc *f = e->f;
			if (e->type == e_func && !f->func->s && is_caselike_func(f) /*is_ifthenelse_func(f)*/) {
				return e;
			} else {
				split_exps(sql, e->l, rel);
				add_exps_too_project(sql, e->l, rel);
			}
		}
		return e;
	case e_cmp:
		if (e->flag == cmp_filter) {
			split_exps(sql, e->l, rel);
			split_exps(sql, e->r, rel);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			split_exps(sql, e->l, rel);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = split_exp(sql, e->l, rel);
			split_exps(sql, e->r, rel);
		} else {
			e->l = split_exp(sql, e->l, rel);
			e->r = split_exp(sql, e->r, rel);
			if (e->f)
				e->f = split_exp(sql, e->f, rel);
		}
		return e;
	case e_atom:
	case e_psm:
		return e;
	}
	return e;
}

static void
split_exps(mvc *sql, list *exps, sql_rel *rel)
{
	if (list_empty(exps))
		return;
	for(node *n=exps->h; n; n = n->next){
		sql_exp *e = n->data;

		e = split_exp(sql, e, rel);
		n->data = e;
	}
}

sql_rel *
rel_split_project_(visitor *v, sql_rel *rel, int top)
{
	if (mvc_highwater(v->sql))
		return rel;

	if (!rel)
		return NULL;
	if (is_project(rel->op) && list_length(rel->exps) && (is_groupby(rel->op) || rel->l) && !need_distinct(rel) && !is_single(rel)) {
		list *exps = rel->exps;
		node *n;
		int funcs = 0;
		sql_rel *nrel;

		/* are there functions */
		for (n=exps->h; n && !funcs; n = n->next) {
			sql_exp *e = n->data;

			funcs = exp_has_func(e);
		}
		/* introduce extra project */
		if (funcs && rel->op != op_project) {
			nrel = rel_project(v->sql->sa, rel->l,
				rel_projections(v->sql, rel->l, NULL, 1, 1));
			rel->l = nrel;
			/* recursively split all functions and add those to the projection list */
			split_exps(v->sql, rel->exps, nrel);
			if (nrel->l && !(nrel->l = rel_split_project_(v, nrel->l, (is_topn(rel->op)||is_sample(rel->op))?top:0)))
				return NULL;
			return rel;
		} else if (funcs && !top && list_empty(rel->r)) {
			/* projects can have columns point back into the expression list, ie
			 * create a new list including the split expressions */
			node *n;
			list *exps = rel->exps;

			rel->exps = sa_list(v->sql->sa);
			for (n=exps->h; n; n = n->next)
				append(rel->exps, split_exp(v->sql, n->data, rel));
		} else if (funcs && top && rel_is_ref(rel) && list_empty(rel->r)) {
			/* inplace */
			list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
			sql_rel *l = rel_project(v->sql->sa, rel->l, NULL);
			rel->l = l;
			l->exps = rel->exps;
			set_processed(l);
			rel->exps = exps;
		}
	}
	if (is_mset(rel->op) || is_set(rel->op) || is_basetable(rel->op))
		return rel;
	if (rel->l && (rel->op != op_table || rel->flag != TRIGGER_WRAPPER)) {
		rel->l = rel_split_project_(v, rel->l, (is_topn(rel->op)||is_sample(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
		if (!rel->l)
			return NULL;
	}
	if ((is_join(rel->op) || is_semi(rel->op)) && rel->r) {
		rel->r = rel_split_project_(v, rel->r, (is_topn(rel->op)||is_sample(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
		if (!rel->r)
			return NULL;
	}
	return rel;
}

static sql_rel *
rel_split_project(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_split_project_(v, rel, 1);
}

run_optimizer
bind_split_project(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_cycle == 0 && gp->opt_level == 1 && (flag & split_project) &&
		   (gp->cnt[op_project] || gp->cnt[op_groupby]) ? rel_split_project : NULL;
}


static sql_rel *
rel_project_reduce_casts(visitor *v, global_props *gp, sql_rel *rel)
{
	if (!rel)
		return NULL;
	(void) gp;
	if (gp->opt_level == 1 && v->value_based_opt && is_simple_project(rel->op) && list_length(rel->exps)) {
		list *exps = rel->exps;
		node *n;

		for (n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e && e->type == e_func) {
				sql_subfunc *f = e->f;
				sql_subtype *res = f->res->h->data;

				if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && res->scale > 0) {
					list *args = e->l;
					sql_exp *h = args->h->data;
					sql_exp *t = args->t->data;
					atom *ha = exp_value(v->sql, h), *ta = exp_value(v->sql, t);

					if (ha || ta) {
						atom *a = ha ? ha : ta;
						atom *na = reduce_scale(v->sql, a);

						if (na && na != a) {
							int rs = a->tpe.scale - na->tpe.scale;
							res->scale -= rs;
							if (ha) {
								h->r = NULL;
								h->l = na;
							} else {
								t->r = NULL;
								t->l = na;
							}
							v->changes++;
						}
					}
				}
			}
		}
	}
	return rel;
}

run_optimizer
bind_project_reduce_casts(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->cnt[op_project] && (flag & project_reduce_casts) ? rel_project_reduce_casts : NULL;
}

static sql_column *
exp_find_column_( sql_rel *rel, sql_exp *exp, int pnr, sql_rel **bt )
{
	if (exp->type == e_column)
		return name_find_column(rel, exp->l, exp->r, pnr, bt);
	return NULL;
}

static int
rel_part_nr( sql_rel *rel, sql_exp *e )
{
	sql_column *c = NULL;
	sql_rel *bt = NULL;
	assert(e->type == e_cmp);

	c = exp_find_column_(rel, e->l, -1, &bt);
	if (!c)
		c = exp_find_column_(rel, e->r, -1, &bt);
	if (!c && e->f)
		c = exp_find_column_(rel, e->f, -1, &bt);
	if (!c || !bt || !rel_base_get_mergetable(bt))
		return -1;
	sql_table *pp = c->t;
	sql_table *mt = rel_base_get_mergetable(bt);
	return find_member_pos(mt->members, pp);
}

static int
rel_uses_part_nr( sql_rel *rel, sql_exp *e, int pnr )
{
	sql_column *c = NULL;
	sql_rel *bt = NULL;
	assert(e->type == e_cmp);

	/*
	 * following case fails.
	 *
	 * semijoin( A1, union [A1, A2] )
	 * The union will never return proper column (from A2).
	 * ie need different solution (probably pass pnr).
	 */
	c = exp_find_column_(rel, e->l, pnr, &bt);
	if (!c)
		c = exp_find_column_(rel, e->r, pnr, &bt);
	if (c && bt && rel_base_get_mergetable(bt)) {
		sql_table *pp = c->t;
		sql_table *mt = rel_base_get_mergetable(bt);
		if (find_member_pos(mt->members, pp) == pnr)
			return 1;
	}
	/* for projects we may need to do a rename! */
	if (is_project(rel->op) || is_topn(rel->op) || is_sample(rel->op))
		return rel_uses_part_nr( rel->l, e, pnr);

	if (is_munion(rel->op)) {
		list *l = rel->l;
		if (rel_uses_part_nr( l->h->data, e, pnr))
			return 1;
	} else if (is_join(rel->op) || is_semi(rel->op)) {
		if (rel_uses_part_nr( rel->l, e, pnr))
			return 1;
		if (!is_semi(rel->op) && rel_uses_part_nr( rel->r, e, pnr))
			return 1;
	}
	return 0;
}

static sql_column *
exp_is_pkey(sql_rel *rel, sql_exp *e)
{
	if (find_prop(e->p, PROP_HASHCOL)) { /* aligned PKEY JOIN */
		fcmp cmp = (fcmp)&kc_column_cmp;
		sql_column *c = exp_find_column(rel, e, -2);

		if (c && c->t->pkey && list_find(c->t->pkey->k.columns, c, cmp) != NULL)
			return c;
	}
	return NULL;
}

static sql_exp *
rel_is_join_on_pkey(sql_rel *rel, bool pk_fk) /* pk_fk is used to verify is a join on pk-fk */
{
	if (!rel || !rel->exps)
		return NULL;
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *je = n->data;

		if (je->type == e_cmp && je->flag == cmp_equal &&
			(exp_is_pkey(rel, je->l) || exp_is_pkey(rel, je->r)) &&
			(!pk_fk || find_prop(je->p, PROP_JOINIDX)))
			return je;
	}
	return NULL;
}

static int
exps_has_predicate( list *l )
{
	node *n;

	for( n = l->h; n; n = n->next){
		sql_exp *e = n->data;

		if (e->card <= CARD_ATOM)
			return 1;
	}
	return 0;
}

static sql_rel *
rel_find_select( sql_rel *r)
{
	while (!is_select(r->op) && r->l && is_project(r->op))
		r = r->l;
	if (is_select(r->op))
		return r;
	return NULL;
}

static bool
rels_share_rel(list *l)
{
	sql_rel *ref = NULL;
	for (node *n = l->h; n; n = n->next) {
		sql_rel *r = n->data;
		if(!ref) {
			if ((ref = rel_find_ref(r)) == NULL)
				return false;
		} else if (ref != rel_find_ref(r)) {
				return false;
		}
	}
	return true;
}

static inline sql_rel *
rel_merge_munion(visitor *v, sql_rel *rel)
{
	if (is_munion(rel->op) && rels_share_rel(rel->l)) {
		list *rels = rel->l, *nrels = NULL;
		sql_rel *cur = NULL, *curs = NULL;

		/* Find selects and try to merge */
		for(node *n = rels->h; n; n = n->next) {
			sql_rel *r = n->data;
			sql_rel *s = rel_find_select(r);

			if (!s)
				return rel;
			if (cur) {
				if (s != r->l || curs->l != s->l || !rel_is_ref(s->l) ||
					/* for now only union(project*(select(R),project*(select(R))) */
				   !s->exps || !curs->exps || exps_has_predicate(s->exps) || exps_has_predicate(curs->exps)) {
					if (nrels)
						append(nrels, r);
					else
						return rel;
				}

				/* merge, ie. add 'or exp' */
				curs->exps = append(new_exp_list(v->sql->sa), exp_disjunctive(v->sql->sa, list_merge(cur->exps, s->exps, (fdup)NULL)));
				if (!nrels) {
					nrels = sa_list(v->sql->sa);
					append(nrels, cur);
				}
			} else {
				if (s != r->l || !rel_is_ref(s->l)) {
					if (nrels) {
						append(nrels, r);
					} else {
						return rel;
					}
				}
				cur = r;
				curs = s;
			}
		}
		if (nrels) {
			v->changes++;
			rel->l = nrels;
		}
		return rel;
	}
	return rel;
}

static sql_rel *
rel_optimize_munions_bottomup_(visitor *v, sql_rel *rel)
{
	rel = rel_merge_munion(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_munions_bottomup(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_optimize_munions_bottomup_);
}

run_optimizer
bind_optimize_unions_bottomup(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && gp->cnt[op_munion] && (flag & optimize_unions_bottomup)
		   ? rel_optimize_munions_bottomup : NULL;
}


static inline sql_rel *
rel_project_cse(visitor *v, sql_rel *rel)
{
	if (is_project(rel->op) && rel->exps) {
		node *n, *m;
		list *nexps;
		int needed = 0;

		for (n=rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e1 = n->data;

			if (e1->type != e_column && !exp_is_atom(e1) && exp_name(e1)) {
				for (m=n->next; m && !needed; m = m->next){
					sql_exp *e2 = m->data;

					if (exp_name(e2) && exp_match_exp(e1, e2))
						needed = 1;
				}
			}
		}

		if (!needed)
			return rel;

		nexps = new_exp_list(v->sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e1 = n->data;

			if (e1->type != e_column && !exp_is_atom(e1) && exp_name(e1)) {
				for (m=nexps->h; m; m = m->next){
					sql_exp *e2 = m->data;

					if (exp_name(e2) && exp_match_exp(e1, e2)) {
						assert(e2->alias.label);
						sql_exp *ne = exp_ref(v->sql, e2);

						ne = exp_propagate(v->sql->sa, ne, e1);
						assert(!e1->ref);
						exp_setalias(ne, e1->alias.label, exp_relname(e1), exp_name(e1));
						set_selfref(ne);
						assert(ne->nid);
						e1 = ne;
						break;
					}
				}
			}
			append(nexps, e1);
		}
		rel->exps = nexps;
	}
	return rel;
}

static int exp_is_const_op(sql_exp *exp, sql_exp *tope, sql_rel *expr);

static int
exps_are_const_op(list *exps, sql_exp *tope, sql_rel *expr)
{
	int ok = 1;

	if (list_empty(exps))
		return 1;
	for (node *n = exps->h; n && ok; n = n->next)
		ok &= exp_is_const_op(n->data, tope, expr);
	return ok;
}

static int
exp_is_const_op(sql_exp *exp, sql_exp *tope, sql_rel *expr)
{
	switch (exp->type) {
	case e_atom:
		return exp->f ? 0 : 1;
	case e_convert:
		return exp_is_const_op(exp->l, tope, expr);
	case e_func:
	case e_aggr: {
		sql_subfunc *f = exp->f;
		if (f->func->side_effect || IS_ANALYTIC(f->func))
			return 0;
		return exps_are_const_op(exp->l, tope, expr);
	}
	case e_cmp:
		if (exp->flag == cmp_filter)
			return exps_are_const_op(exp->l, tope, expr) && exps_are_const_op(exp->r, tope, expr);
		if (exp->flag == cmp_con || exp->flag == cmp_dis)
			return exps_are_const_op(exp->l, tope, expr);
		if (exp->flag == cmp_in || exp->flag == cmp_notin)
			return exp_is_const_op(exp->l, tope, expr) && exps_are_const_op(exp->r, tope, expr);
		return exp_is_const_op(exp->l, tope, expr) && exp_is_const_op(exp->r, tope, expr) && (!exp->f || exp_is_const_op(exp->f, tope, expr));
	case e_column: {
		if (is_simple_project(expr->op) || is_groupby(expr->op)) {
			/* in a simple projection, self-references may occur */
			sql_exp *nexp = exps_bind_nid(expr->exps, exp->nid);
			if (nexp && list_position(expr->exps, nexp) < list_position(expr->exps, tope))
				return exp_is_const_op(nexp, exp, expr);
		}
		return 0;
	}
	default:
		return 0;
	}
}

static sql_exp *
rel_groupby_add_count_star(mvc *sql, sql_rel *rel, sql_exp *count_star_exp, bool *count_added)
{
	if (count_star_exp)
		return count_star_exp;
	if (!list_empty(rel->exps)) {
		for (node *n=rel->exps->h; n ; n = n->next) {
			sql_exp *e = n->data;

			if (exp_aggr_is_count(e) && !need_distinct(e) && list_length(e->l) == 0)
				return e;
		}
	}
	sql_subfunc *cf = sql_bind_func(sql, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
	*count_added = true;
	return rel_groupby_add_aggr(sql, rel, exp_aggr(sql->sa, NULL, cf, 0, 0, rel->card, 0));
}

/* optimize sum(x + 12) into sum(x) + 12*count(*) */
static inline sql_rel *
rel_simplify_sum(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !list_empty(rel->exps)) {
		sql_rel *upper = NULL, *groupby = rel, *l = groupby->l;
		sql_exp *count_star_exp = NULL;
		bool count_added = false;

		for (node *n=groupby->exps->h; n ; n = n->next) {
			sql_exp *e = n->data;
			list *el = e->l;
			sql_subfunc *sf = e->f;

			if (e->type == e_aggr && !need_distinct(e) && sf->func->type == F_AGGR && !sf->func->s && !strcmp(sf->func->base.name, "sum")) {
				sql_rel *expr = groupby;
				sql_exp *exp = (sql_exp*) el->h->data, *oexp = exp;

				while (is_numeric_upcast(exp))
					exp = exp->l;
				/* we want to find a +/- call, so expect them to appear only on simple projections */
				while (exp && exp->type == e_column && (is_simple_project(expr->op) || is_groupby(expr->op)) && expr->l) {
					sql_rel *nexpr = NULL;
					sql_exp *nexp = rel_find_exp_and_corresponding_rel(l, exp, false, &nexpr, NULL);

					/* break when it loops on the same relation */
					if (nexpr == expr && list_position(expr->exps, nexp) >= list_position(expr->exps, exp))
						break;
					expr = nexpr;
					exp = oexp = nexp;
					while (exp && is_numeric_upcast(exp))
						exp = exp->l;
				}

				list *expl = exp ? exp->l : NULL;
				sql_subfunc *expf = exp ? exp->f : NULL;
				/* found a candidate function */
				if (exp && exp->type == e_func && expf->func->type == F_FUNC && !expf->func->s &&
					(!strcmp(expf->func->base.name, "sql_sub") || !strcmp(expf->func->base.name, "sql_add"))) {
					sql_exp *e1 = (sql_exp*) expl->h->data, *e2 = (sql_exp*) expl->h->next->data;
					int e1ok = exp_is_const_op(e1, oexp, expr), e2ok = exp_is_const_op(e2, oexp, expr);

					if ((!e1ok && e2ok) || (e1ok && !e2ok)) {
						sql_exp *ocol = e1ok ? e2 : e1, *constant = e1ok ? e1 : e2, *mul, *colref, *naggr, *newop, *col = ocol, *match;
						bool add_col = true, prepend = false;

						/* if 'col' is a projection from the under relation, then use it */
						while (is_numeric_upcast(col))
							col = col->l;
						if (col->type == e_column) {
							sql_exp *colf = exps_find_exp(l->exps, col);

							/* col is already found in the inner relation. Also look for a new reference for col, eg sql_add(col, 1), 1 as col */
							if (colf && list_position(l->exps, colf) < list_position(l->exps, oexp)) {
								add_col = false;
							} else if (!colf && is_simple_project(l->op) && list_empty(l->r) && !rel_is_ref(l) && !need_distinct(l)) {
								prepend = true;
								add_col = false;
							} else if (!colf && (is_simple_project(l->op) || is_groupby(l->op)))  {
								/* on these scenarios the new column expression will be ordered/(grouped for distinct) or create potential ambiguity (multiple ref), so skip */
								continue;
							}
						} else if ((is_simple_project(l->op) && (!list_empty(l->r) || rel_is_ref(l) || need_distinct(l))) || is_groupby(l->op)) {
							continue;
						}

						/* add count star */
						count_star_exp = rel_groupby_add_count_star(v->sql, groupby, count_star_exp, &count_added);
						/* multiply constant by count star */
						if (!(mul = rel_binop_(v->sql, NULL, constant, exp_ref(v->sql, count_star_exp), "sys", "sql_mul", card_value, true))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = '\0';
							continue;
						}
						if (!has_label(mul))
							exp_label(v->sql->sa, mul, ++v->sql->label);

						colref = exp_ref(v->sql, ocol);
						if (add_col) /* if 'col' will be added, then make sure it has an unique label */
							exp_label(v->sql->sa, colref, ++v->sql->label);

						/* 'oexp' contains the type for the input for the 'sum' aggregate */
						if (!(colref = exp_check_type(v->sql, exp_subtype(oexp), groupby, colref, type_equal))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = '\0';
							continue;
						}
						/* update sum to use the column side only */
						sql_subfunc *a = sql_bind_func(v->sql, "sys", "sum", exp_subtype(colref), NULL, F_AGGR, true, true);
						if (!a)
							continue;
						naggr = exp_aggr(v->sql->sa, list_append(sa_list(v->sql->sa), colref), a, need_distinct(e), need_no_nil(e), groupby->card, has_nil(e));
						if ((match = exps_any_match(groupby->exps, naggr)) && list_position(groupby->exps, match) < list_position(groupby->exps, e)) { /* found a matching aggregate, use it */
							naggr = exp_ref(v->sql, match);
							exp_label(v->sql->sa, naggr, ++v->sql->label);
						} else if (!has_label(naggr)) { /* otherwise use the new one */
							exp_label(v->sql->sa, naggr, ++v->sql->label);
						}

						/* generate addition/subtraction. subtraction is not commutative, so keep original order! */
						if (!(newop = rel_binop_(v->sql, NULL, e1 == constant ? mul : exp_ref(v->sql, naggr), e1 == constant ? exp_ref(v->sql, naggr) : mul, "sys", expf->func->base.name, card_value, true))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = '\0';
							continue;
						}
						if (!(newop = exp_check_type(v->sql, exp_subtype(e), groupby, newop, type_equal))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = '\0';
							continue;
						}

						/* a column reference can be prepended to the inner relation, add it after all the check type calls succeed */
						if (prepend)
							list_prepend(l->exps, col);

						/* the new generate function calls are valid, update relations */
						/* we need a new relation for the multiplication and addition/subtraction */
						if (!upper) {
							/* be careful with relations with more than 1 reference, so do in-place replacement */
							list *projs = rel_projections(v->sql, rel, NULL, 1, 1);
							sql_rel *nrel = rel_groupby(v->sql, rel->l, NULL);
							nrel->exps = rel->exps;
							nrel->r = rel->r;
							nrel->card = rel->card;
							nrel->nrcols = list_length(rel->exps);
							set_processed(nrel);
							rel_dup(rel->l);
							upper = rel = rel_inplace_project(v->sql->sa, rel, nrel, projs);
							rel->card = exps_card(projs);
							groupby = nrel; /* update pointers :) */
							l = groupby->l;
						}
						for (node *n = upper->exps->h ; n ; ) {
							node *next = n->next;
							sql_exp *re = n->data;

							/* remove the old reference to the aggregate because we will use the addition/subtraction instead,
							   as well as the count star if count_added */
							if (exp_refers(e, re) || (count_added && exp_refers(count_star_exp, re)))
								list_remove_node(upper->exps, NULL, n);
							n = next;
						}

						/* update sum aggregate with new aggregate or reference to an existing one */
						n->data = naggr;
						list_hash_clear(groupby->exps);

						/* add column reference with new label, if 'col' was not found */
						if (add_col) {
							if (!is_simple_project(l->op) || !list_empty(l->r) || rel_is_ref(l) || need_distinct(l))
								groupby->l = l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
							list_append(l->exps, ocol);
						}

						/* propagate alias and add new expression */
						exp_prop_alias(v->sql->sa, newop, e);
						list_append(upper->exps, newop);
						v->changes++;
					}
				}
			}
		}
	}
	return rel;
}

/* optimize group by x+1,(y-2)*3,2-z into group by x,y,z */
static inline sql_rel *
rel_simplify_groupby_columns(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !list_empty(rel->r)) {
		sql_rel *l = rel->l;

		for (node *n=((list*)rel->r)->h; n ; n = n->next) {
			sql_exp *e = n->data;
			e->used = 0; /* we need to use this flag, clean it first */
		}
		for (node *n=((list*)rel->r)->h; n ; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_column) {
				bool searching = true;
				sql_rel *efrel = NULL;
				sql_exp *exp = rel_find_exp_and_corresponding_rel(l, e, false, &efrel, NULL), *col = NULL, *tope = exp;

				while (searching && !col) {
					sql_exp *exp_col = exp;

					if (exp && is_numeric_upcast(exp))
						exp = exp->l;
					if (exp && exp->type == e_func) {
						list *el = exp->l;
						sql_subfunc *sf = exp->f;
						/* At the moment look only at injective math functions */
						if (sf->func->type == F_FUNC && !sf->func->s &&
							(!strcmp(sf->func->base.name, "sql_sub") || !strcmp(sf->func->base.name, "sql_add") || !strcmp(sf->func->base.name, "sql_mul"))) {
							sql_exp *e1 = (sql_exp*) el->h->data, *e2 = (sql_exp*) el->h->next->data;
							/* the optimization cannot be done if side-effect calls (e.g. rand()) are present */
							int e1ok = exp_is_atom(e1) && !exp_unsafe(e1, true, false) && !exp_has_sideeffect(e1), e2ok = exp_is_atom(e2) && !exp_unsafe(e2, true, false) && !exp_has_sideeffect(e2);

							if ((!e1ok && e2ok) || (e1ok && !e2ok)) {
								sql_exp *c = e1ok ? e2 : e1;
								bool done = false;
								exp_col = exps_find_exp(efrel->exps, c);
								if (exp_col)
									c = exp_col;

								while (!done) {
									if (is_numeric_upcast(c))
										c = c->l;
									if (c->type == e_column) {
										if (is_simple_project(efrel->op) || is_groupby(efrel->op)) {
											/* in a simple projection, self-references may occur */
											sql_exp *nc = exps_find_exp(efrel->exps, c);
											if (nc && list_position(efrel->exps, nc) < list_position(efrel->exps, exp_col)) {
												exp_col = c;
												c = nc;
												continue;
											}
										}
										col = c; /* 'c' is a column reference from the left relation */
										done = true;
									} else {
										exp = c; /* maybe a nested function call, let's continue searching */
										done = true;
									}
								}
							} else {
								searching = false;
							}
						} else {
							searching = false;
						}
					} else {
						searching = false;
					}
				}
				if (col) { /* a column reference was found */
					const char *rname = exp_relname(e), *name = exp_name(e);

					/* the grouping column has an alias, we have to keep it */
					if ((rname && name && (strcmp(rname, e->l) != 0 || strcmp(name, e->r) != 0)) || (!rname && name && strcmp(name, e->r) != 0)) {
						if (!has_label(e)) /* dangerous to merge, skip it */
							continue;
						if (!is_simple_project(l->op) || !list_empty(l->r) || rel_is_ref(l) || need_distinct(l))
							rel->l = l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
						list_append(l->exps, e);
						n->data = e = exp_ref(v->sql, e);
						list_hash_clear(rel->r);
					}

					sql_exp *f = exps_find_exp(rel->r, col);

					if (f && list_position(rel->r, f) < list_position(rel->r, e)) { /* if already present, remove it */
						e->used = 1;
					} else {
						/* Use an unique reference to the column found. If there's another grouping column label pointing into it,
						   rel_groupby_cse will hopefully remove it */
						sql_exp *colf = exps_find_exp(l->exps, col);

						/* col is already found in the inner relation. Also look for a new reference for col, eg sql_add(col, 1), 1 as col */
						if (colf && list_position(l->exps, colf) < list_position(l->exps, tope)) {
							n->data = exp_ref(v->sql, col);
						} else if (!colf && is_simple_project(l->op) && list_empty(l->r) && !rel_is_ref(l) && !need_distinct(l)) { /* trivial case, it can be added */
							sql_exp *ne = col;
							list_prepend(l->exps, ne);
							n->data = exp_ref(v->sql, ne);
						} else if (!colf && (is_simple_project(l->op) || is_groupby(l->op)))  {
							/* on these scenarios the new column expression will be ordered/(grouped for distinct) or create potential ambiguity (multiple ref), so skip */
							continue;
						} else {
							sql_exp *ne = exp_ref(v->sql, col);

							if (colf) /* a col reference is already there, add a new label */
								exp_label(v->sql->sa, ne, ++v->sql->label);
							if (!is_simple_project(l->op) || !list_empty(l->r) || rel_is_ref(l) || need_distinct(l))
								rel->l = l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
							list_append(l->exps, ne);
							n->data = exp_ref(v->sql, ne);
						}
						list_hash_clear(rel->r);
					}
					v->changes++;
				}
			}
		}
		for (node *n=((list*)rel->r)->h; n ; ) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (e->used) /* remove unnecessary grouping columns */
				list_remove_node(rel->r, NULL, n);
			n = next;
		}
	}
	return rel;
}

/* remove identical grouping columns */
static inline sql_rel *
rel_groupby_cse(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !list_empty(rel->r)) {
		sql_rel *l = rel->l;

		/* for every group expression e1 */
		for (node *n=((list*)rel->r)->h; n ; n = n->next) {
			sql_exp *e1 = n->data;
			/* it's good to examine the same expression in the subrelation e.g. in case it's an alias */
			/* TODO maybe cover more cases? Here I only look at the left relation */
			sql_exp *e1_sub = e1->type == e_column ?
					    e1->nid ? exps_bind_nid(l->exps, e1->nid) : exps_find_exp(l->exps, e1) : NULL;

			/* for every other group expression */
			for (node *m=n->next; m; m = m->next) {
				sql_exp *e2 = m->data;
				sql_exp *e2_sub = e2->type == e_column ?
						e2->nid ? exps_bind_nid(l->exps, e2->nid) : exps_find_exp(l->exps, e2) : NULL;

				/* check if the expression are the same */
				if (exp_match_exp(e1, e2) || exp_refers(e1, e2) || (e1_sub && e2_sub && (exp_match_exp(e1_sub, e2_sub) || exp_refers(e1_sub, e2_sub)))) {

					/* use e2 from rel->exps instead of e2 from the rel->r as it can have an alias from the higher rel */
					sql_exp *e2_in_exps = exps_uses_nid(rel->exps, e2->alias.label);
					assert(e2_in_exps);

					/* same as e2 */
					sql_exp *e1_in_exps = exps_uses_nid(rel->exps, e1->alias.label);
					if (!e1_in_exps)
						continue;

					/* write e2 as an e1 alias since the expressions are the same */
					sql_exp* e2_as_e1_alias = exp_copy(v->sql, e1_in_exps);
					/* NOTE: it is important to get the rname (exp->l) and name (exp->r) from e2 IN the exps
					 * (e2_in_exps), and not from e2, since it could carry an alias from the higher rel */
					exp_setalias(e2_as_e1_alias, e2_as_e1_alias->alias.label, e2_in_exps->l, e2_in_exps->r);
					e2_as_e1_alias->alias.label = e2->alias.label;

					/* replace e2 with e2_as_e1_alias in expressions list */
					node *e2_exps_node = list_find(rel->exps, e2_in_exps, NULL);
					list_append_before(rel->exps, e2_exps_node, e2_as_e1_alias);
					list_remove_node(rel->exps, NULL, e2_exps_node);

					/* finally remove e2 from the groups' list (->r) since it's redundant */
					node *e2_r_node = list_find(rel->r, e2, NULL);
					list_remove_node(rel->r, NULL, e2_r_node);

					v->changes++;
				}
			}
		}
	}
	return rel;
}

sql_exp *list_exps_uses_exp(list *exps, const char *rname, const char *name);

static sql_exp*
exp_uses_exp(sql_exp *e, const char *rname, const char *name)
{
	sql_exp *res = NULL;

	switch (e->type) {
		case e_psm:
			break;
		case e_atom: {
			if (e->f)
				return list_exps_uses_exp(e->f, rname, name);
		} break;
		case e_convert:
			return exp_uses_exp(e->l, rname, name);
		case e_column: {
			if (e->l && rname && strcmp(e->l, rname) == 0 &&
				e->r && name && strcmp(e->r, name) == 0)
				return e;
			if (!e->l && !rname &&
				e->r && name && strcmp(e->r, name) == 0)
				return e;
		} break;
		case e_func:
		case e_aggr: {
			if (e->l)
				return list_exps_uses_exp(e->l, rname, name);
		} 	break;
		case e_cmp: {
			if (e->flag == cmp_con || e->flag == cmp_dis) {
				return list_exps_uses_exp(e->l, rname, name);
			} else if (e->flag == cmp_in || e->flag == cmp_notin) {
				if ((res = exp_uses_exp(e->l, rname, name)))
					return res;
				return list_exps_uses_exp(e->r, rname, name);
			} else if (e->flag == cmp_filter) {
				if ((res = list_exps_uses_exp(e->l, rname, name)))
					return res;
				return list_exps_uses_exp(e->r, rname, name);
			} else {
				if ((res = exp_uses_exp(e->l, rname, name)))
					return res;
				if ((res = exp_uses_exp(e->r, rname, name)))
					return res;
				if (e->f)
					return exp_uses_exp(e->f, rname, name);
			}
		} break;
	}
	return NULL;
}

sql_exp *
list_exps_uses_exp(list *exps, const char *rname, const char *name)
{
	sql_exp *res = NULL;

	if (!exps)
		return NULL;
	for (node *n = exps->h; n && !res; n = n->next) {
		sql_exp *e = n->data;
		res = exp_uses_exp(e, rname, name);
	}
	return res;
}

/* find in the list of expression an expression which uses e */
sql_exp *
exps_uses_exp(list *exps, sql_exp *e)
{
	return list_exps_uses_exp(exps, exp_relname(e), exp_name(e));
}
/*
 * Rewrite aggregations over munion all.
 *	groupby ([ union all (a, b, c) ], [gbe], [ count, sum ] )
 *
 * into
 * 	groupby ([ union all( groupby( a, [gbe], [ count, sum] ),
 * 	                      groupby( b, [gbe], [ count, sum] ),
 * 	                      groupby( c, [gbe], [ count, sum] ) ],
 * 			 [gbe], [sum, sum] )
 */
static inline bool
rel_has_groupby(sql_rel *r)
{
	if (r) {
		if (is_groupby(r->op))
			return true;
		if (is_simple_project(r->op))
			return rel_has_groupby(r->l);
	}
	return false;
}

static inline sql_rel *
rel_push_aggr_down_n_arry(visitor *v, sql_rel *rel)
{
	sql_rel *g = rel;
	sql_rel *u = rel->l, *ou = u;
	sql_rel *r = NULL;
	list *rgbe = NULL, *gbe = NULL, *exps = NULL;
	node *n, *m;

	// TODO why?
	if (u->op == op_project && !need_distinct(u))
		u = u->l;

	if (is_recursive(u))
		return rel;

	/* make sure we don't create group by on group by's */
	for (node *n = ((list*)u->l)->h; n; n = n->next) {
		r = n->data;
		if (rel_has_groupby(r))
			return rel;
	}

	/* distinct should be done over the full result */
	for (n = g->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subfunc *af = e->f;

		if (e->type == e_atom ||
			e->type == e_func ||
		   (e->type == e_aggr &&
		   ((strcmp(af->func->base.name, "sum") &&
			 strcmp(af->func->base.name, "count") &&
			 strcmp(af->func->base.name, "min") &&
			 strcmp(af->func->base.name, "max")) ||
		   need_distinct(e))))
			return rel;
	}

	list *nl = sa_list(v->sql->sa);
	for (node *n = ((list*)u->l)->h; n; n = n->next) {
		r = rel_dup(n->data);
		//n->data = NULL; /* clean list as we steal the relation r, stealing is needed else (with multiple references) double project cleanup fails */
		if (!is_project(r->op))
			r = rel_project(v->sql->sa, r,
				            rel_projections(v->sql, r, NULL, 1, 1));
		rel_rename_exps(v->sql, u->exps, r->exps);
		if (u != ou) {
			bool isproject = is_project(r->op);
			r = rel_project(v->sql->sa, r, NULL);
			r->exps = exps_copy(v->sql, ou->exps);
			rel_rename_exps(v->sql, ou->exps, r->exps);
			set_processed(r);
			if (isproject)
				r = rel_push_project_down_(v, r); /* cleanup any double projects */
		}
		if (g->r && list_length(g->r) > 0) {
			list *gbe = g->r;
			rgbe = exps_copy(v->sql, gbe);
		}
		r = rel_groupby(v->sql, r, NULL);
		r->r = rgbe;
		r->nrcols = g->nrcols;
		r->card = g->card;
		r->exps = exps_copy(v->sql, g->exps);
		r->nrcols = list_length(r->exps);
		set_processed(r);

		assert(r);
		append(nl, r);
	}

	/* group by on primary keys which define the partitioning scheme
	 * don't need a finalizing group by */
	/* how to check if a partition is based on some primary key ?
	 * */
	if (!list_empty(rel->r)) {
		for (node *n = ((list*)rel->r)->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_column *c = NULL;

			if ((c = exp_is_pkey(rel, e)) && partition_find_part(v->sql->session->tr, c->t, NULL)) {
				/* check if key is partition key */
				v->changes++;
				return rel_inplace_setop_n_ary(v->sql, rel, nl, op_munion,
											   rel_projections(v->sql, rel, NULL, 1, 1));
			}
		}
	}

	if (!list_empty(rel->r)) {
		list *ogbe = rel->r;

		gbe = new_exp_list(v->sql->sa);
		for (n = ogbe->h; n; n = n->next) {
			sql_exp *e = n->data, *ne;

			/* group by in aggregation list */
			ne = exps_uses_exp( rel->exps, e);
			if (ne) {
				sql_rel *first_munion_rel = nl->h->data;
				ne = list_find_exp(first_munion_rel->exps, ne);
			}
			if (!ne) {
				/* e only in the u1,u2,...un->r (group by list) */
				for (node *n = nl->h; n; n = n->next) {
					ne = exp_ref(v->sql, e);
					list_append(((sql_rel*)n->data)->exps, ne);
				}
			}
			assert(ne);
			ne = exp_ref(v->sql, ne);
			append(gbe, ne);
		}
	}

	u = rel_setop_n_ary(v->sql->sa, nl, op_munion);
	rel_setop_n_ary_set_exps(v->sql, u,
			                 rel_projections(v->sql, nl->h->data, NULL, 1, 1), false);
	set_processed(u);

	exps = new_exp_list(v->sql->sa);
	for (n = u->exps->h, m = rel->exps->h; n && m; n = n->next, m = m->next) {
		sql_exp *ne, *e = n->data, *oa = m->data;

		if (oa->type == e_aggr) {
			sql_subfunc *f = oa->f;
			int cnt = exp_aggr_is_count(oa);
			sql_subfunc *a = sql_bind_func(v->sql, "sys", (cnt)?"sum":f->func->base.name, exp_subtype(e), NULL, F_AGGR, true, true);

			assert(a);
			/* munion of aggr result may have nils
			 * because sum/count of empty set */
			set_has_nil(e);
			e = exp_ref(v->sql, e);
			ne = exp_aggr1(v->sql->sa, e, a, need_distinct(e), 1, e->card, 1);
		} else {
			ne = exp_copy(v->sql, oa);
		}
		exp_setalias(ne, oa->alias.label, exp_find_rel_name(oa), exp_name(oa));
		append(exps, ne);
	}
	v->changes++;
	return rel_inplace_groupby(rel, u, gbe, exps);
}

/*
 * Rewrite aggregations over union all.
 *	groupby ([ union all (a, b) ], [gbe], [ count, sum ] )
 *
 * into
 * 	groupby ( [ union all( groupby( a, [gbe], [ count, sum] ), [ groupby( b, [gbe], [ count, sum] )) , [gbe], [sum, sum] )
 */
static inline sql_rel *
rel_push_aggr_down(visitor *v, sql_rel *rel)
{
	if (rel->op == op_groupby && rel->l) {
		sql_rel *u = rel->l, *ou = u;
		sql_rel *g = rel;
		sql_rel *ul = u->l;
		sql_rel *ur = u->r;
		node *n, *m;
		list *lgbe = NULL, *rgbe = NULL, *gbe = NULL, *exps = NULL;

		if (u->op == op_project && !need_distinct(u))
			u = u->l;

		if (!u || !(is_munion(u->op)) || need_distinct(u) || is_single(u) || !u->exps || rel_is_ref(u))
			return rel;

		if (is_munion(u->op))
			return rel_push_aggr_down_n_arry(v, rel);

		ul = u->l;
		ur = u->r;

		/* make sure we don't create group by on group by's */
		if (ul->op == op_groupby || ur->op == op_groupby)
			return rel;

		/* distinct should be done over the full result */
		for (n = g->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *af = e->f;

			if (e->type == e_atom ||
			    e->type == e_func ||
			   (e->type == e_aggr &&
			   ((strcmp(af->func->base.name, "sum") &&
			     strcmp(af->func->base.name, "count") &&
			     strcmp(af->func->base.name, "min") &&
			     strcmp(af->func->base.name, "max")) ||
			   need_distinct(e))))
				return rel;
		}

		ul = rel_dup(ul);
		ur = rel_dup(ur);
		if (!is_project(ul->op))
			ul = rel_project(v->sql->sa, ul,
				rel_projections(v->sql, ul, NULL, 1, 1));
		if (!is_project(ur->op))
			ur = rel_project(v->sql->sa, ur,
				rel_projections(v->sql, ur, NULL, 1, 1));
		rel_rename_exps(v->sql, u->exps, ul->exps);
		rel_rename_exps(v->sql, u->exps, ur->exps);
		if (u != ou) {
			ul = rel_project(v->sql->sa, ul, NULL);
			ul->exps = exps_copy(v->sql, ou->exps);
			rel_rename_exps(v->sql, ou->exps, ul->exps);
			set_processed(ul);
			ur = rel_project(v->sql->sa, ur, NULL);
			ur->exps = exps_copy(v->sql, ou->exps);
			rel_rename_exps(v->sql, ou->exps, ur->exps);
			set_processed(ur);
		}

		if (g->r && list_length(g->r) > 0) {
			list *gbe = g->r;

			lgbe = exps_copy(v->sql, gbe);
			rgbe = exps_copy(v->sql, gbe);
		}
		ul = rel_groupby(v->sql, ul, NULL);
		ul->r = lgbe;
		ul->nrcols = g->nrcols;
		ul->card = g->card;
		ul->exps = exps_copy(v->sql, g->exps);
		ul->nrcols = list_length(ul->exps);
		set_processed(ul);

		ur = rel_groupby(v->sql, ur, NULL);
		ur->r = rgbe;
		ur->nrcols = g->nrcols;
		ur->card = g->card;
		ur->exps = exps_copy(v->sql, g->exps);
		ur->nrcols = list_length(ur->exps);
		set_processed(ur);

		/* group by on primary keys which define the partitioning scheme
		 * don't need a finalizing group by */
		/* how to check if a partition is based on some primary key ?
		 * */
		if (!list_empty(rel->r)) {
			for (node *n = ((list*)rel->r)->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_column *c = NULL;

				if ((c = exp_is_pkey(rel, e)) && partition_find_part(v->sql->session->tr, c->t, NULL)) {
					/* check if key is partition key */
					v->changes++;
					return rel_inplace_setop_n_ary(v->sql, rel,
							append(append(sa_list(v->sql->sa), ul), ur), op_munion,
											 rel_projections(v->sql, rel, NULL, 1, 1));
				}
			}
		}

		if (!list_empty(rel->r)) {
			list *ogbe = rel->r;

			gbe = new_exp_list(v->sql->sa);
			for (n = ogbe->h; n; n = n->next) {
				sql_exp *e = n->data, *ne;

				/* group by in aggregation list */
				ne = exps_uses_exp( rel->exps, e);
				if (ne)
					ne = list_find_exp( ul->exps, ne);
				if (!ne) {
					/* e only in the ul/ur->r (group by list) */
					ne = exp_ref(v->sql, e);
					list_append(ul->exps, ne);
					ne = exp_ref(v->sql, e);
					list_append(ur->exps, ne);
				}
				assert(ne);
				ne = exp_ref(v->sql, ne);
				append(gbe, ne);
			}
		}

		u = rel_setop_n_ary(v->sql->sa, append(append(sa_list(v->sql->sa), ul), ur), op_munion);
		rel_setop_n_ary_set_exps(v->sql, u, rel_projections(v->sql, ul, NULL, 1, 1), false);
		set_processed(u);

		exps = new_exp_list(v->sql->sa);
		for (n = u->exps->h, m = rel->exps->h; n && m; n = n->next, m = m->next) {
			sql_exp *ne, *e = n->data, *oa = m->data;

			if (oa->type == e_aggr) {
				sql_subfunc *f = oa->f;
				int cnt = exp_aggr_is_count(oa);
				sql_subfunc *a = sql_bind_func(v->sql, "sys", (cnt)?"sum":f->func->base.name, exp_subtype(e), NULL, F_AGGR, true, true);

				assert(a);
				/* union of aggr result may have nils
			   	 * because sum/count of empty set */
				set_has_nil(e);
				e = exp_ref(v->sql, e);
				ne = exp_aggr1(v->sql->sa, e, a, need_distinct(e), 1, e->card, 1);
			} else {
				ne = exp_copy(v->sql, oa);
			}
			exp_setalias(ne, oa->alias.label, exp_find_rel_name(oa), exp_name(oa));
			append(exps, ne);
		}
		v->changes++;
		return rel_inplace_groupby( rel, u, gbe, exps);
	}
	return rel;
}

/*
 * More general
 * 	groupby(
 * 	 [ outer ] join(
 * 	    project(
 * 	      table(A) [ c1, c2, .. ]
 * 	    ) [ c1, c2, identity(c2) as I, .. ],
 * 	    table(B) [ c1, c2, .. ]
 * 	  ) [ A.c1 = B.c1 ]
 * 	) [ I ] [ a1, a2, .. ]
 *
 * ->
 *
 * 	[ outer ] join(
 * 	  project(
 * 	    table(A) [ c1, c2, .. ]
 * 	  ) [ c1, c2, .. ],
 * 	  groupby (
 * 	    table(B) [ c1, c2, .. ]
 * 	  ) [ B.c1 ] [ a1, a2, .. ]
 * 	) [ A.c1 = B.c1 ]
 */
static sql_rel *
gen_push_groupby_down(mvc *sql, sql_rel *rel, int *changes)
{
	sql_rel *j = rel->l;
	list *gbe = rel->r;

	if (rel->op == op_groupby && list_length(gbe) == 1 && j->op == op_join){
		sql_rel *jl = j->l, *jr = j->r, *cr, *cl;
		sql_exp *gb = gbe->h->data, *e;
		node *n;
		int left = 1;
		list *aggrs, *aliases, *gbe;

		if (!is_identity(gb, jl) && !is_identity(gb, jr))
			return rel;
		if (jl->op == op_project &&
		    (e = list_find_exp( jl->exps, gb)) != NULL &&
		     find_prop(e->p, PROP_HASHCOL) != NULL) {
			left = 0;
			cr = jr;
			cl = jl;
		} else if (jr->op == op_project &&
		    (e = list_find_exp( jr->exps, gb)) != NULL &&
		     find_prop(e->p, PROP_HASHCOL) != NULL) {
			left = 1;
			cr = jl;
			cl = jr;
		} else {
			return rel;
		}

		if ((left && is_base(jl->op)) || (!left && is_base(jr->op))||
		    (left && is_select(jl->op)) || (!left && is_select(jr->op))
		    || rel_is_join_on_pkey(j, false))
			return rel;

		/* only add aggr (based on left/right), and repeat the group by column */
		aggrs = sa_list(sql->sa);
		aliases = sa_list(sql->sa);
		if (rel->exps) for (n = rel->exps->h; n; n = n->next) {
			sql_exp *ce = n->data;

			if (exp_is_atom(ce))
				list_append(aliases, ce);
			else if (ce->type == e_column) {
				if (rel_has_exp(cl, ce, false) == 0) /* collect aliases outside groupby */
					list_append(aliases, ce);
				else
					list_append(aggrs, ce);
			} else if (ce->type == e_aggr) {
				list *args = ce->l;

				/* check args are part of left/right */
				if (!list_empty(args) && rel_has_exps(cl, args, false) == 0)
					return rel;
				if (rel->op != op_join && exp_aggr_is_count(ce))
					ce->p = prop_create(sql->sa, PROP_COUNT, ce->p);
				list_append(aggrs, ce);
			}
		}
		/* TODO move any column expressions (aliases) into the project list */

		/* find gb in left or right and should be unique */
		gbe = sa_list(sql->sa);
		/* push groupby to right, group on join exps */
		if (j->exps) for (n = j->exps->h; n; n = n->next) {
			sql_exp *ce = n->data, *l = ce->l, *r = ce->r, *e;

			/* get left/right hand of e_cmp */
			assert(ce->type == e_cmp);
			if (ce->flag == cmp_equal && is_alias(l->type) && is_alias(r->type) &&
				(((e = rel_find_exp(cr, l)) && rel_find_exp(cl, r)) ||
				 ((e = rel_find_exp(cr, r)) && rel_find_exp(cl, l)))) {
				e = exp_ref(sql, e);
				list_append(gbe, e);
			} else {
				return rel;
			}
		}
		if (!left)
			cr = j->r = rel_groupby(sql, cr, gbe);
		else
			cr = j->l = rel_groupby(sql, cr, gbe);
		cr->exps = list_merge(cr->exps, aggrs, (fdup)NULL);
		set_processed(cr);
		if (!is_project(cl->op))
			cl = rel_project(sql->sa, cl,
				rel_projections(sql, cl, NULL, 1, 1));
		cl->exps = list_merge(cl->exps, aliases, (fdup)NULL);
		set_processed(cl);
		if (!left)
			j->l = cl;
		else
			j->r = cl;
		rel -> l = NULL;
		rel_destroy(rel);

		if (list_empty(cr->exps) && list_empty(j->exps)) { /* remove crossproduct */
			sql_rel *r = cl;
			if (!left)
				j->l = NULL;
			else
				j->r = NULL;
			rel_destroy(j);
			j = r;
		}
		(*changes)++;
		return j;
	}
	return rel;
}

/*
 * Rewrite group(project(join(A,Dict)[a.i==dict.i])[...dict.n])[dict.n][ ... dict.n ]
 * into
 * 	project(join(groupby (A)[a.i],[a.i]), Dict)[a.i==dict.i])[dict.n]
 *
 */
static inline sql_rel *
rel_push_groupby_down(visitor *v, sql_rel *rel)
{
	sql_rel *p = rel->l;
	list *gbe = rel->r;

	if (rel->op == op_groupby && gbe && p && is_join(p->op))
		return gen_push_groupby_down(v->sql, rel, &v->changes);
	if (rel->op == op_groupby && gbe && p && p->op == op_project) {
		sql_rel *j = p->l;
		sql_rel *jl, *jr;
		node *n;

		if (!j || j->op != op_join || list_length(j->exps) != 1)
			return gen_push_groupby_down(v->sql, rel, &v->changes);
		jl = j->l;
		jr = j->r;

		/* check if jr is a dict with index and var still used */
		if (jr->op != op_basetable || jr->l || !jr->r || list_length(jr->exps) != 2)
			return gen_push_groupby_down(v->sql, rel, &v->changes);

		/* check if group by is done on dict column */
		for(n = gbe->h; n; n = n->next) {
			sql_exp *ge = n->data, *pe = NULL, *e = NULL;

			/* find group by exp in project, then in dict */
			pe = rel_find_exp(p, ge);
			if (pe) /* find project exp in right hand of join, ie dict */
				e = rel_find_exp(jr, pe);
			if (pe && e) {  /* Rewrite: join with dict after the group by */
				list *pexps = rel_projections(v->sql, rel, NULL, 1, 1), *npexps;
				node *m;
				sql_exp *ne = j->exps->h->data; /* join exp */
				p->l = jl;	/* Project now only on the left side of the join */

				ne = ne->l; 	/* The left side of the compare is the index of the left */

				/* find ge reference in new projection list */
				npexps = sa_list(v->sql->sa);
				for (m = pexps->h; m; m = m->next) {
					sql_exp *a = m->data;

					if (exp_refers(ge, a)) {
						sql_exp *sc = jr->exps->t->data;
						sql_exp *e = exp_ref(v->sql, sc);
						if (exp_name(a))
							exp_prop_alias(v->sql->sa, e, a);
						a = e;
					}
					append(npexps, a);
				}

				/* find ge in aggr list */
				for (m = rel->exps->h; m; m = m->next) {
					sql_exp *a = m->data;

					if (exp_match_exp(a, ge) || exp_refers(ge, a)) {
						a = exp_ref(v->sql, ne);
						if (exp_name(ne))
							exp_prop_alias(v->sql->sa, a, ne);
						m->data = a;
					}
				}

				/* change alias pe, ie project out the index  */
				pe->l = (void*)exp_relname(ne);
				pe->r = (void*)exp_name(ne);
				if (exp_name(ne))
					exp_prop_alias(v->sql->sa, pe, ne);

				/* change alias ge */
				ge->l = (void*)exp_relname(pe);
				ge->r = (void*)exp_name(pe);
				if (exp_name(pe))
					exp_prop_alias(v->sql->sa, ge, pe);

				/* zap both project and groupby name hash tables (as we changed names above) */
				list_hash_clear(rel->exps);
				list_hash_clear((list*)rel->r);
				list_hash_clear(p->exps);

				/* add join */
				j->l = rel;
				rel = rel_project(v->sql->sa, j, npexps);
				v->changes++;
			}
		}
	}
	return rel;
}

/* reduce group by expressions based on pkey info
 *
 * The reduced group by and (derived) aggr expressions are restored via
 * extra (new) aggregate columns.
 */
static inline sql_rel *
rel_reduce_groupby_exps(visitor *v, sql_rel *rel)
{
	list *gbe = rel->r;

	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel) && list_length(gbe)) {
		node *n, *m;
		int k, j, i, ngbe = list_length(gbe);
		int8_t *scores = SA_NEW_ARRAY(v->sql->ta, int8_t, ngbe);
		sql_column *c;
		sql_table **tbls = SA_NEW_ARRAY(v->sql->ta, sql_table*, ngbe);
		sql_rel **bts = SA_NEW_ARRAY(v->sql->ta, sql_rel*, ngbe), *bt = NULL;

		gbe = rel->r;
		for (k = 0, i = 0, n = gbe->h; n; n = n->next, k++) {
			sql_exp *e = n->data;

			c = exp_find_column_(rel, e, -2, &bt);
			if (c) {
				for(j = 0; j < i; j++)
					if (c->t == tbls[j] && bts[j] == bt)
						break;
				tbls[j] = c->t;
				bts[j] = bt;
				i += (j == i);
			}
		}
		if (i) { /* forall tables find pkey and
				remove useless other columns */
			/* TODO also remove group by columns which are related to
			 * the other columns using a foreign-key join (n->1), ie 1
			 * on the to be removed side.
			 */
			for(j = 0; j < i; j++) {
				int l, nr = 0, cnr = 0;

				k = list_length(gbe);
				memset(scores, 0, list_length(gbe));
				if (tbls[j]->pkey) {
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						fcmp cmp = (fcmp)&kc_column_cmp;
						sql_exp *e = n->data;

						c = exp_find_column_(rel, e, -2, &bt);
						if (c && c->t == tbls[j] && bts[j] == bt &&
						    list_find(tbls[j]->pkey->k.columns, c, cmp) != NULL) {
							scores[l] = 1;
							nr ++;
						} else if (c && c->t == tbls[j] && bts[j] == bt) {
							/* Okay we can cleanup a group by column */
							scores[l] = -1;
							cnr ++;
						}
					}
				}
				if (nr) {
					int all = (list_length(tbls[j]->pkey->k.columns) == nr);
					sql_kc *kc = tbls[j]->pkey->k.columns->h->data;

					c = kc->c;
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						/* pkey based group by */
						if (scores[l] == 1 && ((all ||
						   /* first of key */
						   (c == exp_find_column(rel, e, -2))) && !find_prop(e->p, PROP_HASHCOL)))
							e->p = prop_create(v->sql->sa, PROP_HASHCOL, e->p);
					}
					for (m = rel->exps->h; m; m = m->next ){
						sql_exp *e = m->data;

						for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
							sql_exp *gb = n->data;

							/* pkey based group by */
							if (scores[l] == 1 && exp_match_exp(e,gb) && find_prop(gb->p, PROP_HASHCOL) && !find_prop(e->p, PROP_HASHCOL)) {
								e->p = prop_create(v->sql->sa, PROP_HASHCOL, e->p);
								break;
							}

						}
					}
				}
				if (cnr && nr && list_length(tbls[j]->pkey->k.columns) == nr) {
					list *ngbe = new_exp_list(v->sql->sa);

					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						/* keep the group by columns which form a primary key
						 * of this table. And those unrelated to this table. */
						if (scores[l] != -1)
							append(ngbe, e);
					}
					rel->r = ngbe;
					/* rewrite gbe and aggr, in the aggr list */
					if (0)
					for (m = rel->exps->h; m; m = m->next ){
						sql_exp *e = m->data;
						int fnd = 0;

						for (l = 0, n = gbe->h; l < k && n && !fnd; l++, n = n->next) {
							sql_exp *gb = n->data;

							if (scores[l] == -1 && exp_refers(gb, e)) {
								/*
								sql_exp *rs = exp_column(v->sql->sa, gb->l?gb->l:exp_relname(gb), gb->r?gb->r:exp_name(gb), exp_subtype(gb), rel->card, has_nil(gb), is_unique(gb), is_intern(gb));
								exp_setalias(rs, e->alias.label, exp_find_rel_name(e), exp_name(e));
								e = rs;
								*/
								assert(e->alias.label == e->nid);
								fnd = 1;
							}
						}
						m->data = e;
					}
					/* new reduced aggr expression list */
					assert(list_length(rel->exps)>0);
					/* only one reduction at a time */
					list_hash_clear(rel->exps);
					v->changes++;
					return rel;
				}
				gbe = rel->r;
			}
		}
	}
	/* remove constants from group by list */
	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel)) {
		int i;
		node *n;

		for (i = 0, n = gbe->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (exp_is_atom(e))
				i++;
		}
		if (i) {
			list *ngbe = new_exp_list(v->sql->sa);
			list *dgbe = new_exp_list(v->sql->sa);

			for (n = gbe->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (!exp_is_atom(e))
					append(ngbe, e);
				/* we need at least one gbe */
				else if (!n->next && list_empty(ngbe))
					append(ngbe, e);
				else
					append(dgbe, e);
			}
			rel->r = ngbe;
			if (!list_empty(dgbe)) {
				/* use atom's directly in the aggr expr list */

				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *ne = NULL;

					if (e->type == e_column) {
						if (e->nid)
							ne = exps_bind_nid(dgbe, e->nid);
						if (ne) {
							ne = exp_copy(v->sql, ne);
							exp_prop_alias(v->sql->sa, ne, e);
							e = ne;
						}
					}
					n->data = e;
				}
				list_hash_clear(rel->exps);
				v->changes++;
			}
		}
	}
	return rel;
}

/* if all arguments to a distinct aggregate are unique, remove 'distinct' property */
static inline sql_rel *
rel_distinct_aggregate_on_unique_values(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !list_empty(rel->exps)) {
		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *exp = (sql_exp*) n->data;

			if (exp->type == e_aggr && need_distinct(exp)) {
				bool all_unique = true;
				list *l = exp->l;

				for (node *m = l->h; m && all_unique; m = m->next) {
					sql_exp *arg = (sql_exp*) m->data;

					all_unique &= arg->type == e_column && is_unique(arg) && (!is_semantics(exp) || !has_nil(arg));
				}
				if (!all_unique && exps_card(l) > CARD_ATOM)
					all_unique = exps_unique(v->sql, rel, l) && (!is_semantics(exp) || !have_nil(l));
				if (all_unique) {
					set_nodistinct(exp);
					v->changes++;
				}
			}
		}
	}
	return rel;
}

static inline sql_rel *
rel_remove_const_aggr(visitor *v, sql_rel *rel)
{
	if(!rel) {
		return rel;
	}

	list *exps = rel->exps;

	if(rel->op != op_groupby || list_empty(exps)) {
		return rel;
	}

	if(!list_empty(rel->r)) {
		/* in the general case in an expression of an aggregate over
		 * a constant can be rewritten as just the const e.g.
		 *   aggr(const) -> const
		 */

		for(node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if(e->type != e_aggr) {
				continue;
			}

			sql_func *j = ((sql_subfunc *)e->f)->func;

			/* some aggregates with const values can only be eliminated
			 * under certain circumstances e.g.
			 *   sum(NULL)   -> NULL, sum(0)  -> 0
			 *   prod(NULL)  -> NULL, prod(1) -> 1
			 *   count(NULL) -> 0
			 */
			int sum = strcmp(j->base.name, "sum") == 0,
				prd = strcmp(j->base.name, "prod") == 0,
				cnt = strcmp(j->base.name, "count") == 0;

			if(!j->s && j->system == 1) {
				list *se = e->l;

				if(se == NULL) {
					continue;
				}

				for(node *m = se->h; m; m = m->next) {
					sql_exp *w = m->data;

					if(w->type == e_atom && w->card == CARD_ATOM) {
						atom *wa = w->l;

						if(sum && !(wa->isnull || atom_is_zero(wa))) {
							continue;
						}

						if(prd && !(wa->isnull || atom_is_one(wa))) {
							continue;
						}

						if(cnt) {
							if(wa->isnull) {
								list_remove_node(se, NULL, m);

								w=exp_atom_lng(v->sql->sa, 0);
								list_append(se, w);
							}
							else {
								continue;
							}
						}

						exp_setalias(w,e->alias.label,e->alias.rname,e->alias.name);

						n->data = w;
						v->changes++;
					} else {
						break;
					}
				}
			}
		}
	}

	/*
	 * Below code replaces GROUP BY with PROJECT in some cases;
	 * Triggers on...
	 * select 1 having true; select 42 from foo group by x; select n from foo group by rollup(n);
	*/

	if (!rel_is_ref(rel)) {
		int needed = 0;
		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *exp = (sql_exp*) n->data;

			if (exp_is_atom(exp) && exp->type != e_aggr)
				needed++;
		}
		if (needed) {
			if (!list_empty(rel->r)) {
				int atoms = 0;
				/* corner case, all grouping columns are atoms */
				for (node *n = ((list*)rel->r)->h; n; n = n->next) {
					sql_exp *exp = (sql_exp*) n->data;

					if (exp_is_atom(exp))
						atoms++;
				}
				/* possible edge case, never triggers in coverage tests */
				if (atoms == list_length(rel->r)) {
					list *nexps = sa_list(v->sql->sa);
					for (node *n = rel->exps->h; n; ) {
						node *next = n->next;
						sql_exp *e = (sql_exp*) n->data;

						/* remove references to constant group by columns */
						if (e->type == e_column) {
							sql_exp *found = NULL;
							found = exps_bind_nid(rel->r, e->nid);
							if (found) {
								list_append(nexps, found);
								list_remove_node(rel->exps, NULL, n);
							}
						}
						n = next;
					}
					rel->r = NULL; /* transform it into a global aggregate */
					rel->exps = list_merge(nexps, rel->exps, (fdup) NULL); /* add grouping columns back as projections */
					/* global aggregates may return 1 row, so filter it based on the count */
					sql_subfunc *cf = sql_bind_func(v->sql, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
					sql_exp *count = exp_aggr(v->sql->sa, NULL, cf, 0, 1, CARD_ATOM, 0);
					count = rel_groupby_add_aggr(v->sql, rel, count);
					sql_exp *cp = exp_compare(v->sql->sa, exp_ref(v->sql, count), exp_atom(v->sql->sa, atom_int(v->sql->sa, exp_subtype(count), 0)), cmp_notequal);
					rel = rel_select(v->sql->sa, rel, cp);
					set_processed(rel);
					return rel;
				}
			} else if (list_length(rel->exps) == needed) { /* all are const */
				sql_rel *ll = rel->l;
				rel->op = op_project;
				/* TODO check if l->l == const, else change that */
				if (ll && ll->l) {
					rel_destroy(ll);
					rel->l = rel_project_exp(v->sql, exp_atom_bool(v->sql->sa, 1));
				}
				return rel;
			}
			sql_rel *nrel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
			for (node *n = nrel->exps->h; n; n = n->next) {
				sql_exp *exp = (sql_exp*) n->data;
				if (exp->type == e_column) {
					sql_exp *e = rel_find_exp(rel, exp);

					if (e && exp_is_atom(e) && e->type == e_atom) {
						sql_exp *ne = exp_copy(v->sql, e);
						assert(ne->alias.label);
						exp_setalias(ne, ne->alias.label, exp_find_rel_name(exp), exp_name(exp));
						n->data = ne;
						v->changes++;
					}
				}
			}
			list *nl = sa_list(v->sql->sa);
			for (node *n = rel->exps->h; n; n = n->next) {
				sql_exp *exp = (sql_exp*) n->data;

				if (!exp_is_atom(exp) || exp->type != e_atom)
					append(nl, exp);
			}
			rel->exps = nl;
			return nrel;
		}
	}

	return rel;
}

#if 0
static sql_rel *
rel_groupby_distinct2(visitor *v, sql_rel *rel)
{
	list *ngbes = sa_list(v->sql->sa), *gbes, *naggrs = sa_list(v->sql->sa), *aggrs = sa_list(v->sql->sa);
	sql_rel *l;
	node *n;

	gbes = rel->r;
	if (!gbes)
		return rel;

	/* check if each aggr is, rewritable (max,min,sum,count)
	 *  			  and only has one argument */
	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subfunc *af = e->f;

		if (e->type == e_aggr &&
		   (strcmp(af->func->base.name, "sum") &&
		     strcmp(af->func->base.name, "count") &&
		     strcmp(af->func->base.name, "min") &&
		     strcmp(af->func->base.name, "max")))
			return rel;
	}

	for (n = gbes->h; n; n = n->next) {
		sql_exp *e = n->data;

		e = exp_column(v->sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
		append(ngbes, e);
	}

	/* 1 for each aggr(distinct v) add the attribute expression v to gbes and aggrs list
	 * 2 for each aggr(z) add aggr_phase2('z') to the naggrs list
	 * 3 for each group by col, add also to the naggrs list
	 * */
	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_aggr && need_distinct(e)) { /* 1 */
			/* need column expression */
			list *args = e->l;
			sql_exp *v = args->h->data;
			append(gbes, v);
			if (!exp_name(v))
				exp_label(v->sql->sa, v, ++v->sql->label);
			v = exp_column(v->sql->sa, exp_find_rel_name(v), exp_name(v), exp_subtype(v), v->card, has_nil(v), is_unique(v), is_intern(v));
			append(aggrs, v);
			v = exp_aggr1(v->sql->sa, v, e->f, need_distinct(e), 1, e->card, 1);
			exp_setalias(v, e->alias.label, exp_find_rel_name(e), exp_name(e));
			append(naggrs, v);
		} else if (e->type == e_aggr && !need_distinct(e)) {
			sql_exp *v;
			sql_subfunc *f = e->f;
			int cnt = exp_aggr_is_count(e);
			sql_subfunc *a = sql_bind_func(v->sql, "sys", (cnt)?"sum":f->func->base.name, exp_subtype(e), NULL, F_AGGR, true, true);

			append(aggrs, e);
			if (!exp_name(e))
				exp_label(v->sql->sa, e, ++v->sql->label);
			set_has_nil(e);
			v = exp_column(v->sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
			v = exp_aggr1(v->sql->sa, v, a, 0, 1, e->card, 1);
			if (cnt)
				set_zero_if_empty(v);
			exp_setalias(v, e->alias.label, exp_find_rel_name(e), exp_name(e));
			append(naggrs, v);
		} else { /* group by col */
			if (list_find_exp(gbes, e) || !list_find_exp(naggrs, e)) {
				append(aggrs, e);

				e = exp_column(v->sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
			}
			append(naggrs, e);
		}
	}

	l = rel->l = rel_groupby(v->sql, rel->l, gbes);
	l->exps = aggrs;
	rel->r = ngbes;
	rel->exps = naggrs;
	v->changes++;
	return rel;
}
#endif

/* Rewrite group by expressions with distinct
 *
 * ie select a, count(distinct b) from c where ... groupby a;
 * No other aggregations should be present
 *
 * Rewrite the more general case, good for parallel execution
 *
 * groupby(R) [e,f] [ aggr1 a distinct, aggr2 b distinct, aggr3 c, aggr4 d]
 *
 * into
 *
 * groupby(
 * 	groupby(R) [e,f,a,b] [ a, b, aggr3 c, aggr4 d]
 * ) [e,f]( aggr1 a distinct, aggr2 b distinct, aggr3_phase2 c, aggr4_phase2 d)
 */
static inline sql_rel *
rel_groupby_distinct(visitor *v, sql_rel *rel)
{
	node *n;

	if (is_groupby(rel->op)) {
		sql_rel *l = rel->l;
		if (!l || is_groupby(l->op))
			return rel;
	}
	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel)) {
		int nr = 0, anr = 0;
		list *gbe, *ngbe, *arg, *exps, *nexps;
		sql_exp *distinct = NULL, *darg, *found;
		sql_rel *l = NULL;

		for (n=rel->exps->h; n && nr <= 2; n = n->next) {
			sql_exp *e = n->data;
			if (need_distinct(e)) {
				distinct = n->data;
				nr++;
				if (e->r) /* distinct and order by */
					nr++;
			}
			anr += is_aggr(e->type);
		}
		if (nr < 1 || distinct->type != e_aggr)
			return rel;
		if (nr > 1 || anr > nr)
			return rel;//rel_groupby_distinct2(v, rel);
		arg = distinct->l;
		if (list_length(arg) != 1 || list_length(rel->r) + nr != list_length(rel->exps))
			return rel;

		gbe = rel->r;
		ngbe = sa_list(v->sql->sa);
		exps = sa_list(v->sql->sa);
		nexps = sa_list(v->sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (e != distinct) {
				if (e->type == e_aggr) { /* copy the arguments to the aggregate */
					list *args = e->l;
					if (args) {
						for (node *n = args->h ; n ; n = n->next) {
							sql_exp *e = n->data;
							list_append(ngbe, exp_copy(v->sql, e));
							list_append(exps, exp_copy(v->sql, e));
						}
					}
				} else {
					e = exp_ref(v->sql, e);
					append(ngbe, e);
					append(exps, e);
				}
				if (e->type == e_aggr) /* aggregates must be copied */
					e = exp_copy(v->sql, e);
				else
					e = exp_ref(v->sql, e);
				append(nexps, e);
			}
		}

		darg = arg->h->data;
		if ((found = exps_find_exp(exps, darg)) == NULL) { /* not already in the groups projection list */
			if ((found = exps_find_exp(gbe, darg))) { /* first find if the aggregate argument already exists in the grouping list */
				darg = exp_ref(v->sql, found);
			} else {
				list_append(gbe, darg = exp_copy(v->sql, darg));
				exp_label(v->sql->sa, darg, ++v->sql->label);
				darg = exp_ref(v->sql, darg);
			}
			list_append(exps, darg);
			darg = exp_ref(v->sql, darg);
		} else {
			darg = exp_ref(v->sql, found);
		}
		arg->h->data = darg;
		l = rel->l = rel_groupby(v->sql, rel->l, gbe);
		l->exps = exps;
		set_processed(l);
		rel->r = ngbe;
		rel->exps = nexps;
		set_nodistinct(distinct);
		append(nexps, distinct);
		v->changes++;
	}
	return rel;
}

/*
 * Push Count inside crossjoin down, and multiply the results
 *
 *     project (                                project(
 *          group by (                               crossproduct (
 *		crossproduct(                             project (
 *		     L,			 =>                    group by (
 *		     R                                              L
 *		) [ ] [ count NOT NULL ]                       ) [ ] [ count NOT NULL ]
 *          )                                             ),
 *     ) [ NOT NULL ]                                     project (
 *                                                              group by (
 *                                                                  R
 *                                                              ) [ ] [ count NOT NULL ]
 *                                                        )
 *                                                   ) [ sql_mul(.., .. NOT NULL) ]
 *                                              )
 */
static inline sql_rel *
rel_push_count_down(visitor *v, sql_rel *rel)
{
	sql_rel *r = rel->l;

	if (is_groupby(rel->op) && !rel_is_ref(rel) && list_empty(rel->r) &&
		r && !r->exps && r->op == op_join && !(rel_is_ref(r)) &&
		/* currently only single count aggregation is handled, no other projects or aggregation */
		list_length(rel->exps) == 1 && exp_aggr_is_count(rel->exps->h->data)) {
		sql_exp *nce, *oce, *cnt1 = NULL, *cnt2 = NULL;
		sql_rel *gbl = NULL, *gbr = NULL;	/* Group By */
		sql_rel *cp = NULL;					/* Cross Product */
		sql_rel *srel;

		oce = rel->exps->h->data;
		if (oce->l) /* we only handle COUNT(*) */
			return rel;

		srel = r->l;
		{
			sql_subfunc *cf = sql_bind_func(v->sql, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
			sql_exp *e = exp_aggr(v->sql->sa, NULL, cf, need_distinct(oce), need_no_nil(oce), oce->card, 0);

			exp_label(v->sql->sa, e, ++v->sql->label);
			cnt1 = exp_ref(v->sql, e);
			gbl = rel_groupby(v->sql, rel_dup(srel), NULL);
			set_processed(gbl);
			rel_groupby_add_aggr(v->sql, gbl, e);
		}

		srel = r->r;
		{
			sql_subfunc *cf = sql_bind_func(v->sql, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
			sql_exp *e = exp_aggr(v->sql->sa, NULL, cf, need_distinct(oce), need_no_nil(oce), oce->card, 0);

			exp_label(v->sql->sa, e, ++v->sql->label);
			cnt2 = exp_ref(v->sql, e);
			gbr = rel_groupby(v->sql, rel_dup(srel), NULL);
			set_processed(gbr);
			rel_groupby_add_aggr(v->sql, gbr, e);
		}

		cp = rel_crossproduct(v->sql->sa, gbl, gbr, op_join);
		set_processed(cp);

		if (!(nce = rel_binop_(v->sql, NULL, cnt1, cnt2, "sys", "sql_mul", card_value, true))) {
			v->sql->session->status = 0;
			v->sql->errstr[0] = '\0';
			return rel; /* error, fallback to original expression */
		}
		/* because of remote plans, make sure "sql_mul" returns bigint. The cardinality is atomic, so no major performance penalty */
		if (subtype_cmp(exp_subtype(oce), exp_subtype(nce)) != 0)
			nce = exp_convert(v->sql, nce, exp_subtype(nce), exp_subtype(oce));
		if (exp_name(oce))
			exp_prop_alias(v->sql->sa, nce, oce);

		rel_destroy(rel);
		rel = rel_project(v->sql->sa, cp, append(new_exp_list(v->sql->sa), nce));
		set_processed(rel);

		v->changes++;
	}

	return rel;
}

static inline sql_rel *
rel_basecount(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !rel_is_ref(rel) && rel->l && list_empty(rel->r) &&
		list_length(rel->exps) == 1 && exp_aggr_is_count(rel->exps->h->data)) {
		sql_rel *bt = rel->l;
		sql_exp *e = rel->exps->h->data;
		if (is_basetable(bt->op) && list_empty(e->l)) { /* count(*) */
			/* change into select cnt('schema','table') */
			sql_table *t = bt->l;
			/* I need to get the declared table's frame number to make this work correctly for those */
			if (!isTable(t) || isDeclaredTable(t))
				return rel;
			sql_subfunc *cf = sql_bind_func(v->sql, "sys", "cnt", sql_fetch_localtype(TYPE_str), sql_fetch_localtype(TYPE_str), F_FUNC, true, true);
			list *exps = sa_list(v->sql->sa);
			append(exps, exp_atom_str(v->sql->sa, t->s->base.name, sql_fetch_localtype(TYPE_str)));
			append(exps, exp_atom_str(v->sql->sa, t->base.name, sql_fetch_localtype(TYPE_str)));
			sql_exp *ne = exp_op(v->sql->sa, exps, cf);

			ne = exp_propagate(v->sql->sa, ne, e);
			exp_setalias(ne, e->alias.label, exp_find_rel_name(e), exp_name(e));
			rel_destroy(rel);
			rel = rel_project(v->sql->sa, NULL, append(sa_list(v->sql->sa), ne));
			v->changes++;
		}
	}
	return rel;
}

static inline sql_rel *
rel_simplify_count(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !list_empty(rel->exps)) {
		mvc *sql = v->sql;
		int ncountstar = 0;

		/* Convert count(no null) into count(*) */
		for (node *n = rel->exps->h; n ; n = n->next) {
			sql_exp *e = n->data;

			if (exp_aggr_is_count(e) && !need_distinct(e)) {
				if (list_length(e->l) == 0) {
					ncountstar++;
				} else if (list_length(e->l) == 1 && !has_nil((sql_exp*)((list*)e->l)->h->data)) {
					sql_subfunc *cf = sql_bind_func(sql, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
					sql_exp *ne = exp_aggr(sql->sa, NULL, cf, 0, 0, e->card, 0);
					if (exp_name(e))
						exp_prop_alias(sql->sa, ne, e);
					n->data = ne;
					ncountstar++;
					v->changes++;
				}
			}
		}
		/* With multiple count(*), use exp_ref to reduce the number of calls to this aggregate */
		if (ncountstar > 1) {
			sql_exp *count_star = NULL;
			sql_rel *nrel = rel_project(v->sql->sa, rel, NULL);
			list *aexps = sa_list(v->sql->sa), *nexps = sa_list(v->sql->sa);
			nrel->exps = nexps;
			for (node *n = rel->exps->h; n ; n = n->next) {
				sql_exp *e = n->data;

				if (exp_aggr_is_count(e) && !need_distinct(e) && list_length(e->l) == 0) {
					if (!count_star) {
						count_star = e;
						append(aexps, e);
						append(nexps, exp_ref(sql, e));
					} else {
						sql_exp *ne = exp_ref(sql, count_star);

						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						v->changes++;
						append(nexps, ne);
					}
				} else {
					append(aexps, e);
					append(nexps, exp_ref(sql, e));
				}
			}
			rel->exps = aexps;
			return nrel;
		}
	}
	return rel;
}

static sql_rel *
rel_gengroupjoin(visitor *v, sql_rel *rel)
{
	sql_rel *j = rel->l;
	list *gbes = rel->r;

	int nr = 0;
	for(node *n = gbes->h; n; n = n->next) {
		sql_exp *gbe = n->data;
		for(node *m = j->exps->h; m; m = m->next) {
			sql_exp *je = m->data;
			if (je->type != e_cmp || je->flag != cmp_equal)
				return rel;
			/* check if its a join exp (ie not a selection) */
			if (!( (!rel_has_exp(j->l, je->l, false) && !rel_has_exp(j->r, je->r, false)) ||
				   (!rel_has_exp(j->l, je->r, false) && !rel_has_exp(j->r, je->l, false))))
				return rel;
			if (exp_match(je->l, gbe)) {
				nr++;
			} else if (exp_match(je->r, gbe)) {
				nr++;
			}
		}
	}
	if (nr == list_length(gbes)) {
		printf("#group by converted\n");
		j = rel_dup(j);
		j->attr = rel->exps;
		v->changes++;
		rel_destroy(rel);
		return j;
	}
	return rel;
}

static sql_rel *
rel_groupjoin(visitor *v, sql_rel *rel)
{
	if (!rel || rel_is_ref(rel) || !is_groupby(rel->op) || list_empty(rel->r))
		return rel;

	sql_rel *j = rel->l;
	//if (!j || rel_is_ref(j) || (j->op != op_join && j->op != op_left))
	if (!j || rel_is_ref(j) || j->op != op_join || list_length(rel->exps) > 1 /* only join because left joins aren't optimized yet (TODO), only length 1 as implementation of groupjoins is missing */ || !list_empty(rel->attr))
		return rel;
	/* check group by exps == equi join exps */
	list *gbes = rel->r;
	if (list_length(gbes) != list_length(j->exps))
		return rel;

	if (is_left(j->op))
		/* left joins aren't optmized jet */
		return rel;

	if (0 && (list_length(rel->exps) > 1 || list_empty(rel->attr)))
		return rel_gengroupjoin(v, rel);;

	int nr = 0;
	for(node *n = gbes->h; n; n = n->next) {
		sql_exp *gbe = n->data;
		for(node *m = j->exps->h; m; m = m->next) {
			sql_exp *je = m->data;
			if (je->type != e_cmp || je->flag != cmp_equal)
				return rel;
			/* check if its a join exp (ie not a selection) */
			if (!( (!rel_has_exp(j->l, je->l, false) && !rel_has_exp(j->r, je->r, false)) ||
				   (!rel_has_exp(j->l, je->r, false) && !rel_has_exp(j->r, je->l, false))))
				return rel;
			if (exp_match(je->l, gbe)) {
				nr++;
			} else if (exp_match(je->r, gbe)) {
				nr++;
			}
		}
	}
	if (nr == list_length(gbes)) {
		// printf("#group by converted\n");
		j = rel_dup(j);
		j->attr = rel->exps;
		v->changes++;
		rel_destroy(rel);
		return j;
	}
	return rel;
}

/* select k1 from bla where k1 = const -> select const from bla where k1 = const */
static sql_rel *
rel_project_select_exp(visitor *v, sql_rel *rel)
{
	if (is_simple_project(rel->op) && rel->exps && rel->l) {
		sql_rel *l = rel->l;
		if (is_select(l->op) && l->exps) {
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *col = n->data;
				if (col->type == e_column) {
					for(node *m = l->exps->h; m; m = m->next) {
						sql_exp *cmp = m->data;
						if (cmp->type == e_cmp && cmp->flag == cmp_equal && !is_anti(cmp) && !is_semantics(cmp) && exp_is_atom(cmp->r)) {
							sql_exp *l = cmp->l;
							if(l->type == e_column && col->nid == l->nid) {
								/* replace column with the constant */
								sql_exp *e = n->data = exp_copy(v->sql, cmp->r);
								exp_setalias(e, col->alias.label, exp_relname(col), exp_name(col));
								exp_propagate(v->sql->sa, e, col);
								list_hash_clear(rel->exps);
							}
						}
					}
				}
			}
		}
	}
	return rel;
}

static sql_rel *
rel_optimize_projections_(visitor *v, sql_rel *rel)
{
	rel = rel_project_cse(v, rel);
	rel = rel_project_select_exp(v, rel);

	if (!rel || !is_groupby(rel->op))
		return rel;

	rel = rel_remove_const_aggr(v, rel);

	if (v->value_based_opt) {
		rel = rel_simplify_sum(v, rel);
		rel = rel_simplify_groupby_columns(v, rel);
	}
	rel = rel_groupby_cse(v, rel);
	rel = rel_push_aggr_down(v, rel);
	rel = rel_push_groupby_down(v, rel);
	rel = rel_reduce_groupby_exps(v, rel);
	rel = rel_distinct_aggregate_on_unique_values(v, rel);
	rel = rel_groupby_distinct(v, rel);
	rel = rel_push_count_down(v, rel);

	/* only when value_based_opt is on, ie not for dependency resolution */
	if (v->value_based_opt) {
		rel = rel_simplify_count(v, rel);
		rel = rel_basecount(v, rel);

		rel = rel_groupjoin(v, rel);
	}
	return rel;
}

static sql_rel *
rel_optimize_projections(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_optimize_projections_);
}

run_optimizer
bind_optimize_projections(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_groupby] || gp->cnt[op_project] || gp->cnt[op_munion]
		   || gp->cnt[op_inter] || gp->cnt[op_except]) && (flag & optimize_projections) ? rel_optimize_projections : NULL;
}


static bool
exps_have_selfref(list *exps)
{
	bool selfref = false;
	if (list_empty(exps))
		return false;
	for(node *n = exps->h; n && !selfref; n = n->next) {
		sql_exp *e = n->data;
		selfref |= is_selfref(e);
	}
	return selfref;
}

static inline sql_rel *
rel_push_project_down_union(visitor *v, sql_rel *rel)
{
	/* first remove distinct if already unique */
	if (rel->op == op_project && need_distinct(rel) && rel->exps && exps_unique(v->sql, rel, rel->exps) && !have_nil(rel->exps)) {
		set_nodistinct(rel);
		if (exps_card(rel->exps) <= CARD_ATOM && rel->card > CARD_ATOM) { /* if the projection just contains constants, then no topN is needed */
			sql_rel *nl = rel->l = rel_topn(v->sql->sa, rel->l, append(sa_list(v->sql->sa), exp_atom_lng(v->sql->sa, 1)));
			set_processed(nl);
		}
		v->changes++;
	}

	if (rel->op == op_project && rel->l && rel->exps && list_empty(rel->r)) {
		int need_distinct = need_distinct(rel);
		sql_rel *u = rel->l;
		sql_rel *p = rel;

		if (!u || !(is_munion(u->op)) || need_distinct(u) || is_recursive(u) || !u->exps || rel_is_ref(u) || project_unsafe(rel, false))
			return rel;

		sql_rel *r;

		if (is_recursive(u))
			return rel;

		/* don't push project down union of single values */
		for (node *n = ((list*)u->l)->h; n; n = n->next) {
			r = n->data;
			// TODO: does this check make sense?
			if (is_project(r->op) && !r->l)
				return rel;
		}

		for (node *n = ((list*)u->l)->h; n; n = n->next) {
			r = rel_dup(n->data);

			/* introduce projection around each operand if needed */
			if (!is_project(r->op) || exps_have_selfref(r->exps))
				r = rel_project(v->sql->sa, r,
						rel_projections(v->sql, r, NULL, 1, 1));
			/* check if we need distinct */
			need_distinct &=
				(!exps_unique(v->sql, r, r->exps) || have_nil(r->exps));
			rel_rename_exps(v->sql, u->exps, r->exps);

			rel_destroy(n->data);
			n->data = r;
		}

		/* once we have checked for need_distinct in every rel we can
		 * introduce the projects under the munion which are gonna be
		 * copies of the single project above munion */
		for (node *n = ((list*)u->l)->h; n; n = n->next) {
			r = rel_dup(n->data);

			r = rel_project(v->sql->sa, r, NULL);
			if (need_distinct)
				set_distinct(r);
			r->exps = exps_copy(v->sql, p->exps);
			set_processed(r);

			rel_destroy(n->data);
			n->data = r;
		}

		/* turn the project-munion on top into munion. incr operand
		 * rels count to make sure that they are not deleted by the
		 * subsequent rel_inplace_setop_n_ary */
		for (node *n = ((list*)u->l)->h; n; n = n->next)
			rel_dup(n->data);
		rel = rel_inplace_setop_n_ary(v->sql, rel, u->l, op_munion,
				rel_projections(v->sql, rel, NULL, 1, 1));
		if (need_distinct)
			set_distinct(rel);
		if (is_single(u))
			set_single(rel);

		v->changes++;

		/* if any operand has two project above then squash them */
		for (node *n = ((list*)u->l)->h; n; n = n->next) {
			r = rel_dup(n->data);
			r = rel_merge_projects_(v, r);
			rel_destroy(n->data);
			n->data = r;
		}

		return rel;
	}
	return rel;
}

static inline sql_rel *
rel_merge_unions(visitor *v, sql_rel *rel)
{
	/* stacked munion flattening e.g.
	 * munion( munion(a, b, c), munion(d, e)) => munion(a,b,c,d,e)
	 */
	if (rel && is_munion(rel->op) && !is_recursive(rel)) {
		list *l = rel->l;
		for(node *n = l->h; n; ) {
			node *next = n->next;
			sql_rel *c = n->data;
			if (is_munion(c->op)) {
				c = rel_dup(c);
				list_remove_node(l, NULL, n);
				l = list_merge(l, c->l, (fdup)NULL);
				c->l = NULL;
				rel_destroy(c);
				if (!next)
					next = l->h;
				v->changes++;
			}
			n = next;
		}
		rel->l = l;
	}
	return rel;
}


/*
 * Push (semi)joins down unions, this is basically for merge tables, where
 * we know that the fk-indices are split over two clustered merge tables.
 */
static inline sql_rel *
rel_push_join_down_munion(visitor *v, sql_rel *rel)
{
	if ((is_join(rel->op) && !is_outerjoin(rel->op) && !is_single(rel)) || is_semi(rel->op)) {
		sql_rel *l = rel->l, *r = rel->r, *ol = l, *or = r;
		list *exps = rel->exps, *attr = rel->attr;
		sql_exp *je = NULL;

		if (is_recursive(l) || is_recursive(r))
			return rel;
		/* we would like to optimize in place reference rels which point
		 * to replica tables and let the replica optimizer handle those
		 * later. otherwise we miss the push join down optimization due
		 * to the rel_is_ref bailout
		 */
		if (rel_is_ref(l) && is_basetable(l->op) && l->l && isReplicaTable((sql_table*)l->l)) {
			rel->l = rel_copy(v->sql, l, true);
			rel_destroy(l);
		}
		if (rel_is_ref(r) && is_basetable(r->op) && r->l && isReplicaTable((sql_table*)r->l)) {
			rel->r = rel_copy(v->sql, r, true);
			rel_destroy(r);
		}

		// TODO: do we need to check if it's l/r are refs?
		if (!l || !r || need_distinct(l) || need_distinct(r) || rel_is_ref(l) || rel_is_ref(r))
			return rel;
		if (l->op == op_project)
			l = l->l;
		if (r->op == op_project)
			r = r->l;

		/* both sides only if we have a join index ASSUMING pkey-fkey are aligned */
		// TODO: we could also check if the join cols are (not) unique
		bool aligned_pk_fk = true;
		if (!l || !r || (is_munion(l->op) && is_munion(r->op) &&
			!(je = rel_is_join_on_pkey(rel, aligned_pk_fk))))
			return rel;

		// TODO: why? bailout for union semijoin without pkey joins expressions
		if (is_semi(rel->op) && is_munion(l->op) && !je)
			return rel;

		/* if both sides are munions we assume that they will have the same number of children */
		if (is_munion(l->op) && is_munion(r->op) && list_length(l->l) != list_length(r->l))
			return rel;

		if (is_munion(l->op) && !need_distinct(l) && !is_single(l) && !is_recursive(l) &&
		   !is_munion(r->op)){
			/* join(munion(a,b,c), d) -> munion(join(a,d), join(b,d), join(c,d)) */
			list *js = sa_list(v->sql->sa);
			for (node *n = ((list*)l->l)->h; n; n = n->next) {
				sql_rel *pc = rel_dup(n->data);
				if (!is_project(pc->op))
					pc = rel_project(v->sql->sa, pc, rel_projections(v->sql, pc, NULL, 1, 1));
				rel_rename_exps(v->sql, l->exps, pc->exps);
				if (l != ol) {
					pc = rel_project(v->sql->sa, pc, NULL);
					pc->exps = exps_copy(v->sql, ol->exps);
					set_processed(pc);
				}
				pc = rel_crossproduct(v->sql->sa, pc, rel_dup(or), rel->op);
				pc->exps = exps_copy(v->sql, exps);
				pc->attr = exps_copy(v->sql, attr);
				set_processed(pc);
				pc = rel_project(v->sql->sa, pc, rel_projections(v->sql, pc, NULL, 1, 1));
				js = append(js, pc);
			}
			v->changes++;
			return rel_inplace_setop_n_ary(v->sql, rel, js, op_munion,
					                       rel_projections(v->sql, rel, NULL, 1, 1));
		} else if (is_munion(l->op) && !need_distinct(l) && !is_single(l) && !is_recursive(l) &&
			       is_munion(r->op) && !need_distinct(r) && !is_single(r) && !is_recursive(r) &&
			       je) {
			/* join(munion(a,b,c), munion(d,e,f)) -> munion(join(a,d), join(b,e), join(c,f)) */
			list *cps = sa_list(v->sql->sa);
			/* create pairwise joins between left and right parts. assume eq num of parts (see earlier bailout) */
			for (node *n = ((list*)l->l)->h, *m=((list*)r->l)->h; n && m; n = n->next, m = m->next) {
				/* left part */
				sql_rel *lp = rel_dup(n->data);
				if (!is_project(lp->op))
					lp = rel_project(v->sql->sa, lp, rel_projections(v->sql, lp, NULL, 1, 1));
				rel_rename_exps(v->sql, l->exps, lp->exps);
				if (l != ol) {
					lp = rel_project(v->sql->sa, lp, NULL);
					lp->exps = exps_copy(v->sql, ol->exps);
					set_processed(lp);
				}
				/* right part */
				sql_rel *rp = rel_dup(m->data);
				if (!is_project(rp->op))
					rp = rel_project(v->sql->sa, rp, rel_projections(v->sql, rp, NULL, 1, 1));
				rel_rename_exps(v->sql, r->exps, rp->exps);
				if (r != or) {
					rp = rel_project(v->sql->sa, rp, NULL);
					rp->exps = exps_copy(v->sql, or->exps);
					set_processed(rp);
				}
				/* combine them */
				sql_rel *cp = rel_crossproduct(v->sql->sa, lp, rp, rel->op);
				cp->exps = exps_copy(v->sql, exps);
				cp->attr = exps_copy(v->sql, attr);
				set_processed(cp);
				cp = rel_project(v->sql->sa, cp, rel_projections(v->sql, cp, NULL, 1, 1));
				cps = append(cps, cp);
			}
			v->changes++;
			return rel_inplace_setop_n_ary(v->sql, rel, cps, op_munion,
										   rel_projections(v->sql, rel, NULL, 1, 1));
		} else if (!is_munion(l->op) &&
			        is_munion(r->op) && !need_distinct(r) && !is_single(r) && !is_recursive(r) &&
			       !is_semi(rel->op)) {
			/* join(a, munion(b,c,d)) -> munion(join(a,b), join(a,c), join(a,d)) */
			list *js = sa_list(v->sql->sa);
			for (node *n = ((list*)r->l)->h; n; n = n->next) {
				sql_rel *pc = rel_dup(n->data);
				if (!is_project(pc->op))
					pc = rel_project(v->sql->sa, pc, rel_projections(v->sql, pc, NULL, 1, 1));
				rel_rename_exps(v->sql, r->exps, pc->exps);
				if (r != or) {
					pc = rel_project(v->sql->sa, pc, NULL);
					pc->exps = exps_copy(v->sql, or->exps);
					set_processed(pc);
				}
				pc = rel_crossproduct(v->sql->sa, rel_dup(ol), pc, rel->op);
				pc->exps = exps_copy(v->sql, exps);
				pc->attr = exps_copy(v->sql, attr);
				set_processed(pc);
				pc = rel_project(v->sql->sa, pc, rel_projections(v->sql, pc, NULL, 1, 1));
				js = append(js, pc);
			}
			v->changes++;
			return rel_inplace_setop_n_ary(v->sql, rel, js, op_munion,
					                       rel_projections(v->sql, rel, NULL, 1, 1));
		} else if (!is_munion(l->op) &&
			        is_munion(r->op) && !need_distinct(r) && !is_single(r) && !is_recursive(r) &&
			        is_semi(rel->op) && je) {
			/* {semi}join ( A1, munion (B, A2a, C, A2b)) [A1.partkey = A2.partkey] ->
			 * {semi}join ( A1, munion (A2a, A2b))
			 * (ie some parts of an n-th munion operand)
			 *
			 * How to detect that a relation isn't matching?
			 * 		partitioning is currently done only on pkey/fkey's
			 * 		ie only matching per part if join is on pkey/fkey (parts)
			 * 		and part numbers should match.
			 * */
			int lpnr = rel_part_nr(l, je);
			if (lpnr < 0)
				return rel;

			list *ups = sa_list(v->sql->sa);
			for (node *n = ((list*)r->l)->h; n; n = n->next) {
				if (rel_uses_part_nr(n->data, je, lpnr)) {
					sql_rel *pc = rel_dup(n->data);
					ups = append(ups, pc);
				}
			}
			v->changes++;
			return rel_inplace_setop_n_ary(v->sql, r, ups, op_munion,
					                              rel_projections(v->sql, rel, NULL, 1, 1));
		}
	}
	return rel;
}

static sql_rel *
rel_optimize_unions_topdown_(visitor *v, sql_rel *rel)
{
	rel = rel_push_project_down_union(v, rel);
	rel = rel_merge_unions(v, rel);
	rel = rel_push_join_down_munion(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_unions_topdown(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_optimize_unions_topdown_);
}

run_optimizer
bind_optimize_unions_topdown(visitor *v, global_props *gp)
{
	(void) v;
	return gp->opt_level == 1 && gp->cnt[op_munion] ? rel_optimize_unions_topdown : NULL;
}


static sql_column *
is_fk_column_of_pk(mvc *sql, sql_rel *rel, sql_column *pkc, sql_exp *e) /* test if e is a foreign key column for the pk on pkc */
{
	sql_trans *tr = sql->session->tr;
	sql_column *c = exp_find_column(rel, e, -2);

	if (c) {
		sql_table *t = c->t;

		for (node *n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *li = n->data;

			if (li->type == join_idx) {
				for (node *m = li->columns->h ; m ; m = m->next) {
					sql_kc *fkc = m->data;

					if (strcmp(fkc->c->base.name, c->base.name) == 0) { /* same fkey column */
						sql_key *fkey = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)li->key)->rkey);

						if (strcmp(fkey->t->base.name, pkc->t->base.name) == 0) { /* to same pk table */
							for (node *o = fkey->columns->h ; o ; o = n->next) {
								sql_kc *kc = m->data;

								if (strcmp(kc->c->base.name, pkc->base.name) == 0) /* to same pk table column */
									return c;
							}
						}
					}
				}
			}
		}
	}
	return NULL;
}

static bool
has_no_selectivity(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return true;

	switch(rel->op){
	case op_basetable:
	case op_truncate:
	case op_table:
		return true;
	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:
		return has_no_selectivity(sql, rel->l);
	case op_ddl:
	case op_insert:
	case op_update:
	case op_delete:
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_inter:
	case op_except:
	case op_munion:
	case op_select:
		return false;
	}
	return true;
}

static sql_rel *
rel_distinct_project2groupby_(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l;

	/* rewrite distinct project (table) [ constant ] -> project [ constant ] */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    exps_card(rel->exps) <= CARD_ATOM) {
		set_nodistinct(rel);
		if (rel->card > CARD_ATOM) { /* if the projection just contains constants, then no topN is needed */
			sql_rel *nl = rel->l = rel_topn(v->sql->sa, rel->l, append(sa_list(v->sql->sa), exp_atom_lng(v->sql->sa, 1)));
			set_processed(nl);
		}
		v->changes++;
	}

	/* rewrite distinct project [ pk ] ( select ( table ) [ e op val ])
	 * into project [ pk ] ( select/semijoin ( table )  */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    (l->op == op_select || l->op == op_semi) && exps_unique(v->sql, rel, rel->exps) &&
		(!have_semantics(l->exps) || !have_nil(rel->exps))) {
		set_nodistinct(rel);
		v->changes++;
	}

	/* rewrite distinct project ( join(p,f) [ p.pk = f.fk ] ) [ p.pk ]
	 * 	into project( (semi)join(p,f) [ p.pk = f.fk ] ) [ p.pk ] */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    l && (is_select(l->op) || l->op == op_join) && rel_is_join_on_pkey(l, true) /* [ pk == fk ] */) {
		sql_exp *found = NULL, *pk = NULL, *fk = NULL;
		bool all_exps_atoms = true;
		sql_column *pkc = NULL;

		for (node *m = l->exps->h ; m ; m = m->next) { /* find a primary key join */
			sql_exp *je = (sql_exp *) m->data;
			sql_exp *le = je->l, *re = je->r;

			if (!find_prop(je->p, PROP_JOINIDX)) /* must be a pk-fk join expression */
				continue;

			if ((pkc = exp_is_pkey(l, le))) { /* le is the primary key */
				all_exps_atoms = true;

				for (node *n = rel->exps->h; n && all_exps_atoms; n = n->next) {
					sql_exp *e = (sql_exp *) n->data;

					if (exp_match(e, le) || exp_refers(e, le))
						found = e;
					else if (e->card > CARD_ATOM)
						all_exps_atoms = false;
				}
				pk = le;
				fk = re;
			}
			if (!found && (pkc = exp_is_pkey(l, re))) { /* re is the primary key */
				all_exps_atoms = true;

				for (node *n = rel->exps->h; n && all_exps_atoms; n = n->next) {
					sql_exp *e = (sql_exp *) n->data;

					if (exp_match(e, re) || exp_refers(e, re))
						found = e;
					else if (e->card > CARD_ATOM)
						all_exps_atoms = false;
				}
				pk = re;
				fk = le;
			}
		}

		if (all_exps_atoms && found) { /* rel must have the same primary key on the projection list */
			/* if the foreign key has no selectivity, the join can be removed */
			if (!(rel_is_ref(l)) && ((rel_find_exp(l->l, fk) && is_fk_column_of_pk(v->sql, l->l, pkc, fk) && has_no_selectivity(v->sql, l->l)) ||
				(l->r && rel_find_exp(l->r, fk) && is_fk_column_of_pk(v->sql, l->r, pkc, fk) && has_no_selectivity(v->sql, l->r)))) {
				sql_rel *side = (rel_find_exp(l->l, pk) != NULL)?l->l:l->r;

				rel->l = rel_dup(side);
				rel_destroy(l);
				v->changes++;
				set_nodistinct(rel);
				return rel;
			}
			/* if the join has no multiple references it can be re-written into a semijoin */
			if (l->op == op_join && !(rel_is_ref(l)) && list_length(rel->exps) == 1) { /* other expressions may come from the other side */
				if (l->r && rel_find_exp(l->r, pk)) {
					sql_rel *temp = l->l;
					l->l = l->r;
					l->r = temp;

					l->op = op_semi;
				} else if (rel_find_exp(l->l, pk)) {
					l->op = op_semi;
				}
			}
			v->changes++;
			set_nodistinct(rel);
			return rel;
		}
	}
	/* rewrite distinct project [ gbe ] ( select ( groupby [ gbe ] [ gbe, e ] )[ e op val ])
	 * into project [ gbe ] ( select ( group etc ) */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ &&
	    need_distinct(rel) && l->op == op_select){
		sql_rel *g = l->l;
		if (is_groupby(g->op)) {
			list *used = sa_list(v->sql->sa);
			list *gbe = g->r;
			node *n;
			int fnd = 1;

			for (n = rel->exps->h; n && fnd; n = n->next) {
				sql_exp *e = n->data;

				if (e->card > CARD_ATOM) {
					/* find e in gbe */
					sql_exp *ne = list_find_exp(g->exps, e);

					if (ne)
						ne = list_find_exp( gbe, ne);
					if (ne && !list_find_exp(used, ne)) {
						fnd++;
						list_append(used, ne);
					}
					if (!ne)
						fnd = 0;
				}
			}
			if (fnd == (list_length(gbe)+1)) {
				v->changes++;
				set_nodistinct(rel);
			}
		}
	}
	if (rel->op == op_project && rel->l &&
	    need_distinct(rel) && exps_card(rel->exps) > CARD_ATOM) {
		node *n;
		list *exps = new_exp_list(v->sql->sa), *gbe = new_exp_list(v->sql->sa);
		list *obe = rel->r; /* we need to read the ordering later */

		if (obe) {
			int fnd = 0;

			for(n = obe->h; n && !fnd; n = n->next) {
				sql_exp *e = n->data;

				if (e->type != e_column)
					fnd = 1;
				else if (exps_bind_nid(rel->exps, e->nid) == NULL)
					fnd = 1;
			}
			if (fnd)
				return rel;
		}
		rel->l = rel_project(v->sql->sa, rel->l, rel->exps);

		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne;

			set_nodistinct(e);
			ne = exp_ref(v->sql, e);
			if (e->card > CARD_ATOM && !list_find_exp(gbe, ne)) { /* no need to group by on constants, or the same column multiple times */
				append(gbe, ne);
				ne = exp_ref(v->sql, ne);
			}
			append(exps, ne);
		}
		rel->op = op_groupby;
		rel->exps = exps;
		rel->r = gbe;
		set_nodistinct(rel);
		if (obe) {
			/* add order again */
			rel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
			rel->r = obe;
		}
		v->changes++;
	}
	return rel;
}

static sql_rel *
rel_distinct_project2groupby(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_distinct_project2groupby_);
}

run_optimizer
bind_distinct_project2groupby(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_cycle == 0 && gp->opt_level == 1 && gp->needs_distinct && gp->cnt[op_project] &&
		   (flag & distinct_project2groupby)? rel_distinct_project2groupby : NULL;
}
