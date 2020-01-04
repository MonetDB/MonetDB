/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

/*#define DEBUG*/

#include "monetdb_config.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "rel_prop.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_select.h"
#include "mal_errors.h" /* for SQLSTATE() */
 
static void
exp_set_freevar(mvc *sql, sql_exp *e, sql_rel *r)
{
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			exps_set_freevar(sql, e->l, r);
			exps_set_freevar(sql, e->r, r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			exp_set_freevar(sql, e->l, r);
			exps_set_freevar(sql, e->r, r);
		} else {
			exp_set_freevar(sql, e->l, r);
			exp_set_freevar(sql, e->r, r);
			if (e->f)
				exp_set_freevar(sql, e->f, r);
		}
		break;
	case e_convert: 
		exp_set_freevar(sql, e->l, r);
		break;
	case e_func: 
	case e_aggr: 
		if (e->l) 
			exps_set_freevar(sql, e->l, r);
		break;
	case e_column: 
		if ((e->l && rel_bind_column2(sql, r, e->l, e->r, 0)) ||
		    (!e->l && rel_bind_column(sql, r, e->r, 0, 1)))
			return;
		set_freevar(e, 0);
		break;
	case e_atom: 
	case e_psm: 
		break;
	}
}

void
exps_set_freevar(mvc *sql, list *exps, sql_rel *r)
{
	node *n;

	if (list_empty(exps))
		return;	
	for(n = exps->h; n; n = n->next)
		exp_set_freevar(sql, n->data, r);
}

/* check if the set is distinct for the set of free variables */
static int
is_distinct_set(mvc *sql, sql_rel *rel, list *ad)
{
	int distinct = 0;
	if (ad && exps_unique(sql, rel, ad ))
		return 1;
	if (ad && is_groupby(rel->op) && exp_match_list(rel->r, ad))
		return 1;
	distinct = need_distinct(rel);
	if (is_project(rel->op) && rel->l && !distinct)
		distinct = is_distinct_set(sql, rel->l, ad);
	return distinct;
}	

int
exp_has_freevar(mvc *sql, sql_exp *e)
{
	if (THRhighwater()) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}

	if (is_freevar(e))
		return 1;
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			return (exps_have_freevar(sql, e->l) || exps_have_freevar(sql, e->r));
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			return (exp_has_freevar(sql, e->l) || exps_have_freevar(sql, e->r));
		} else {
			return (exp_has_freevar(sql, e->l) || exp_has_freevar(sql, e->r) || 
			    (e->f && exp_has_freevar(sql, e->f)));
		}
		break;
	case e_convert: 
		return exp_has_freevar(sql, e->l);
	case e_func: 
	case e_aggr: 
		if (e->l) 
			return exps_have_freevar(sql, e->l);
		/* fall through */
	case e_psm: 
		if (exp_is_rel(e))
			return rel_has_freevar(sql, e->l);
		break;
	case e_column: 
	case e_atom: 
	default:
		return 0;
	}
	return 0;
}

int
exps_have_freevar(mvc *sql, list *exps)
{
	node *n;

	if (THRhighwater()) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}
	if (!exps)
		return 0;
	for(n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (exp_has_freevar(sql, e))
			return 1;
	}
	return 0;
}

int
rel_has_freevar(mvc *sql, sql_rel *rel)
{
	if (THRhighwater()) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}

	if (is_basetable(rel->op)) {
		return 0;
	} else if (is_base(rel->op)) {
		return exps_have_freevar(sql, rel->exps) ||
			(rel->l && rel_has_freevar(sql, rel->l));
	} else if (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op)) {
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->r && exps_have_freevar(sql, rel->r)) 
			return 1;
		return exps_have_freevar(sql, rel->exps) ||
			(rel->l && rel_has_freevar(sql, rel->l));
	} else if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op) || is_modify(rel->op)) {
		return exps_have_freevar(sql, rel->exps) ||
			rel_has_freevar(sql, rel->l) || rel_has_freevar(sql, rel->r);
	}
	return 0;
}

static int 
freevar_equal( sql_exp *e1, sql_exp *e2)
{
	assert(e1 && e2 && e1->freevar && e2->freevar);
	if (e1 == e2)
		return 0;
	if (e1->type != e_column || e2->type != e_column)
		return -1;
	if (e1->l && e2->l && strcmp(e1->l, e2->l) == 0)
		return strcmp(e1->r, e2->r);
	if (!e1->l && !e2->l)
		return strcmp(e1->r, e2->r);
	return -1;
}

static list *
merge_freevar(list *l, list *r)
{
	if (!l)
		return r;
	if (!r)
		return l;
	return list_distinct(list_merge(l, r, (fdup)NULL), (fcmp)freevar_equal, (fdup)NULL);
}

static list * exps_freevar(mvc *sql, list *exps);
static list * rel_freevar(mvc *sql, sql_rel *rel);

static list *
exp_freevar(mvc *sql, sql_exp *e)
{
	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch(e->type) {
	case e_column:
		if (e->freevar)
			return append(sa_list(sql->sa), e);
		break;
	case e_convert:
		return exp_freevar(sql, e->l);
	case e_aggr:
	case e_func:
		if (e->l)
			return exps_freevar(sql, e->l);
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			list *l = exps_freevar(sql, e->l);
			list *r = exps_freevar(sql, e->r);
			return merge_freevar(l, r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *l = exp_freevar(sql, e->l);
			list *r = exps_freevar(sql, e->r);
			return merge_freevar(l, r);
		} else {
			list *l = exp_freevar(sql, e->l);
			list *r = exp_freevar(sql, e->r);
			l = merge_freevar(l, r);
			if (e->f) {
				r = exp_freevar(sql, e->f);
				return merge_freevar(l, r);
			}
			return l;
		}
		break;
	case e_psm:
		if (exp_is_rel(e))
			if (rel_has_freevar(sql, e->l))
				return rel_freevar(sql, e->l);
		return NULL;
	case e_atom:
	default:
		return NULL;
	}
	return NULL;
}

static list *
exps_freevar(mvc *sql, list *exps)
{
	node *n;
	list *c = NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!exps)
		return NULL;
	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		list *var = exp_freevar(sql, e);

		c = merge_freevar(c,var);
	}
	return c;
}

static list *
rel_freevar(mvc *sql, sql_rel *rel)
{
	list *lexps = NULL, *rexps = NULL, *exps = NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!rel)
		return NULL;
	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = exps_freevar(sql, rel->exps);
		lexps = rel_freevar(sql, rel->l);
		rexps = rel_freevar(sql, rel->r);
		lexps = merge_freevar(lexps, rexps);
		exps = merge_freevar(exps, lexps);
		return exps;

	case op_basetable:
		return NULL;
	case op_table: {
		sql_exp *call = rel->r;
		if (rel->flag != 2 && rel->l)
			lexps = rel_freevar(sql, rel->l);
		exps = (rel->flag != 2 && call)?exps_freevar(sql, call->l):NULL;
		return merge_freevar(exps, lexps);
	}
	case op_union:
	case op_except:
	case op_inter:
		exps = exps_freevar(sql, rel->exps);
		lexps = rel_freevar(sql, rel->l);
		rexps = rel_freevar(sql, rel->r);
		lexps = merge_freevar(lexps, rexps);
		exps = merge_freevar(exps, lexps);
		return exps;
	case op_ddl:
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
	case op_sample:

	case op_groupby:
	case op_project:
		exps = exps_freevar(sql, rel->exps);
		lexps = rel_freevar(sql, rel->l);
		if (rel->r) {
			if (is_groupby(rel->op))
				rexps = exps_freevar(sql, rel->r);
			else
				rexps = rel_freevar(sql, rel->r);
			lexps = merge_freevar(lexps, rexps);
		}
		exps = merge_freevar(exps, lexps);
		return exps;
	default:
		return NULL;
	}

}

static list *
rel_dependent_var(mvc *sql, sql_rel *l, sql_rel *r)
{
	list *res = NULL;

	if (rel_has_freevar(sql, r)){
		list *freevar = rel_freevar(sql, r);
		if (freevar) {
			node *n;
			list *boundvar = rel_projections(sql, l, NULL, 1, 0);

			for(n = freevar->h; n; n = n->next) {
				sql_exp *e = n->data, *ne = NULL;
				/* each freevar should be an e_column */
				if (e->l) {
					ne = exps_bind_column2(boundvar, e->l, e->r );
				} else {
					ne = exps_bind_column(boundvar, e->r, NULL, 1);
				}
				if (ne) {
					if (!res)
						res = sa_list(sql->sa);
					append(res, ne);
				}
			}
		}
	}
	return res;
}

/*
 * try to bind any freevar in the expression e
 */
static void
rel_bind_var(mvc *sql, sql_rel *rel, sql_exp *e)
{
	list *fvs = exp_freevar(sql, e);

	if (fvs) {
		node *n;

		for(n = fvs->h; n; n=n->next) { 
			sql_exp *e = n->data;

			if (e->freevar && (exp_is_atom(e) || rel_find_exp(rel,e))) 
				reset_freevar(e);
		}
	}
}

static sql_exp * push_up_project_exp(mvc *sql, sql_rel *rel, sql_exp *e);

static list *
push_up_project_exps(mvc *sql, sql_rel *rel, list *exps)
{
	node *n;

	if (!exps)
		return exps;

	for(n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;

		n->data = push_up_project_exp(sql, rel, e);
	}
	list_hash_clear(exps);
	return exps;
}

static sql_exp *
push_up_project_exp(mvc *sql, sql_rel *rel, sql_exp *e)
{
	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			e->l = push_up_project_exps(sql, rel, e->l);
			e->r = push_up_project_exps(sql, rel, e->r);
			return e;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = push_up_project_exp(sql, rel, e->l);
			e->r = push_up_project_exps(sql, rel, e->r);
			return e;
		} else {
			e->l = push_up_project_exp(sql, rel, e->l);
			e->r = push_up_project_exp(sql, rel, e->r);
			if (e->f)
				e->f = push_up_project_exp(sql, rel, e->f);
		}
		break;
	case e_convert: 
		e->l = push_up_project_exp(sql, rel, e->l);
		break;
	case e_func: 
	case e_aggr: 
		if (e->l) 
			e->l = push_up_project_exps(sql, rel, e->l);
		break;
	case e_column: 
		{
			sql_exp *ne;

			/* include project or just lookup */	
			if (e->l) {
				ne = exps_bind_column2(rel->exps, e->l, e->r );
			} else {
				ne = exps_bind_column(rel->exps, e->r, NULL, 1);
			}
			if (ne) {
				if (ne->type == e_column) {
					/* deref alias */
					e->l = ne->l;
					e->r = ne->r;
				} else {
					return push_up_project_exp(sql, rel, ne);
				}
			}
		} break;	
	case e_atom: 
	case e_psm: 
		break;
	}
	return e;
}

static sql_exp *exp_rewrite(mvc *sql, sql_rel *rel, sql_exp *e, list *ad);

static list *
exps_rewrite(mvc *sql, sql_rel *rel, list *exps, list *ad)
{
	list *nexps;
	node *n;

	if (list_empty(exps))
		return exps;
	nexps = sa_list(sql->sa);
	for(n=exps->h; n; n = n->next)
		append(nexps, exp_rewrite(sql, rel, n->data, ad));
	return nexps;
}

/* recursively rewrite some functions */
static sql_exp *
exp_rewrite(mvc *sql, sql_rel *rel, sql_exp *e, list *ad)
{
	sql_subfunc *sf;

	if (e->type == e_convert) { 
		e->l = exp_rewrite(sql, rel, e->l, ad);
		return e;
	}
	if (e->type != e_func)
		return e;
	e->l = exps_rewrite(sql, rel, e->l, ad);
	sf = e->f;
	/* window functions need to be run per freevars */
	if (sf->func->type == F_ANALYTIC && list_length(sf->func->ops) > 2) {
		sql_subtype *bt = sql_bind_localtype("bit");
		node *d;
		list *rankopargs = e->l;
		/* window_bound has partition/orderby as first argument (before normal expressions), others as second (and have a boolean placeholder) */
		int is_wb = (strcmp(sf->func->base.name, "window_bound") == 0);
		node *n = (is_wb)?rankopargs->h:rankopargs->h->next;
		sql_exp *pe = n->data;

		/* if pe is window_bound function skip */
		if (pe->type == e_func) {
			sf = pe->f;
			if (strcmp(sf->func->base.name, "window_bound") == 0)
				return e;
		}
		/* find partition expression in rankfunc */
		/* diff function */
		if (exp_is_atom(pe) || (is_wb && (pe->type != e_func || strcmp(sf->func->base.name, "diff") != 0)))
			pe = NULL;
		else
			is_wb = 0;
		for(d=ad->h; d; d=d->next) {
			sql_subfunc *df;
			sql_exp *e = d->data;
			list *args = sa_list(sql->sa);
			if (pe) { 
				df = sql_bind_func(sql->sa, NULL, "diff", bt, exp_subtype(e), F_ANALYTIC);
				append(args, pe);
			} else {
				df = sql_bind_func(sql->sa, NULL, "diff", exp_subtype(e), NULL, F_ANALYTIC);
			}
			assert(df);
			append(args, e);
			pe = exp_op(sql->sa, args, df);
		}
		if (is_wb)
			e->l = list_prepend(rankopargs, pe);
		else
			n->data = pe;
	}
	return e;
}

static sql_exp *
rel_reduce2one_exp(mvc *sql, sql_rel *sq)
{
	sql_exp *e = NULL;
	
	if (list_empty(sq->exps))
		return NULL;
	if (list_length(sq->exps) == 1)
		return sq->exps->t->data;
	for(node *n = sq->exps->h; n && !e; n = n->next) {
		sql_exp *t = n->data;

		if (!is_freevar(t))
			e = t;
	}
	if (!e)
		e = sq->exps->t->data;
	sq->exps = append(sa_list(sql->sa), e);
	return e;
}

static sql_exp *
rel_bound_exp(mvc *sql, sql_rel *rel )
{
	while (rel->l) {
		rel = rel->l;
		if (is_base(rel->op) || is_project(rel->op))
			break;
	}

	if (rel) {
		node *n;
		for(n = rel->exps->h; n; n = n->next){
			sql_exp *e = n->data;

			if (exp_is_atom(e))
				return e;
			if (!is_freevar(e))
				return exp_ref(sql->sa, e);
		}
	}
	if (rel && is_project(rel->op)) /* add dummy expression */
		return rel_project_add_exp(sql, rel, exp_atom_bool(sql->sa, 1));
	return NULL;
}

/*
 * join j was just rewriten, but some join expressions may now 
 * be too low in de relation rel. These need to move up.
 * */
static void
move_join_exps(mvc *sql, sql_rel *j, sql_rel *rel)
{
	node *n;
	list *exps = rel->exps;
	
	if (!exps)
		return;
	rel->exps = sa_list(sql->sa);
	if (!j->exps)
		j->exps = sa_list(sql->sa);
	for(n = exps->h; n; n = n->next){
		sql_exp *e = n->data;

		if (rel_find_exp(rel, e)) {
			if (exp_has_freevar(sql, e))
				rel_bind_var(sql, rel->l, e);
			append(rel->exps, e);
		} else {
			if (exp_has_freevar(sql, e))
				rel_bind_var(sql, j->l, e);
			append(j->exps, e);
		}
	}
}

static sql_rel *
rel_general_unnest(mvc *sql, sql_rel *rel, list *ad)
{
	/* current unnest only possible for equality joins, <, <> etc needs more work */
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel) && ad) {
		list *fd;
		node *n, *m;
		int nr;

		sql_rel *l = rel->l, *r = rel->r, *inner_r;
		/* rewrite T1 dependent join T2 -> T1 join D dependent join T2, where the T1/D join adds (equality) predicates (for the Domain (ad)) and D is are the distinct(projected(ad) from T1)  */
		sql_rel *D = rel_project(sql->sa, rel_dup(l), exps_copy(sql, ad));
		set_distinct(D);

		r = rel_crossproduct(sql->sa, D, r, rel->op);
		r->op = op_join;
		move_join_exps(sql, rel, r);
		set_dependent(r);
		inner_r = r;
		r = rel_project(sql->sa, r, (is_semi(inner_r->op))?sa_list(sql->sa):rel_projections(sql, r->r, NULL, 1, 1));

		if (!is_semi(inner_r->op))  { /* skip the free vars */
			list *exps = sa_list(sql->sa);

			for(node *n=r->exps->h; n; n = n->next) {
				sql_exp *e = n->data, *ne = NULL;

				if (e->l) {
					ne = exps_bind_column2(ad, e->l, e->r );
				} else {
					ne = exps_bind_column(ad, e->r, NULL, 1);
				}
				if (!ne)
					append(exps,e);
			}
			r->exps = exps;
		}

		/* append ad + rename */
		nr = sql->label+1;
		sql->label += list_length(ad);
		fd = exps_label(sql->sa, exps_copy(sql, ad), nr);
		for (n = ad->h, m = fd->h; n && m; n = n->next, m = m->next) {
			sql_exp *l = n->data, *r = m->data, *e;

			l = exp_ref(sql->sa, l);
			r = exp_ref(sql->sa, r);
			e = exp_compare(sql->sa, l, r, (is_outerjoin(rel->op)|is_semi(rel->op))?cmp_equal_nil:cmp_equal);
			if (!rel->exps)
				rel->exps = sa_list(sql->sa);
			append(rel->exps, e);
		}
		list_merge(r->exps, fd, (fdup)NULL);
		rel->r = r;
		reset_dependent(rel);
		return rel;
	}
	return rel;
}

static sql_rel *
push_up_project(mvc *sql, sql_rel *rel, list *ad) 
{
	sql_rel *r = rel->r;

	if (rel_is_ref(r)) {
		sql_rel *nr = rel_project(sql->sa, rel_dup(r->l), exps_copy(sql, r->exps));
		rel_destroy(r);
		rel->r = r = nr;
	}

	/* input rel is dependent outerjoin with on the right a project, we first try to push inner side expressions down (because these cannot be pushed up) */ 
	if (rel && is_outerjoin(rel->op) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		/* find constant expressions and move these down */
		if (r && r->op == op_project) {
			node *n;
			list *nexps = NULL;
			list *cexps = NULL;
			sql_rel *l = r->l;

			if (l && is_select(l->op) && !rel_is_ref(l)) {
				for(n=r->exps->h; n; n=n->next) {
					sql_exp *e = n->data;

					if (exp_is_atom(e) || rel_find_exp(l,e)) { /* move down */
						if (!cexps)
							cexps = sa_list(sql->sa);
						append(cexps, e);
					} else {
						if (!nexps)
							nexps = sa_list(sql->sa);
						append(nexps, e);
					}
				}
				if (cexps) {
					sql_rel *p = l->l = rel_project( sql->sa, l->l, 
						rel_projections(sql, l->l, NULL, 1, 1));
					p->exps = list_merge(p->exps, cexps, (fdup)NULL);
					if (list_empty(nexps)) {
						rel->r = l; /* remove empty project */
					} else {	
						for (n = cexps->h; n; n = n->next) { /* add pushed down renamed expressions */
							sql_exp *e = n->data;
							append(nexps, exp_ref(sql->sa, e));
						}
						r->exps = nexps;
					}
				}
			}
		}
	}
	/* input rel is dependent join with on the right a project */ 
	if (rel && is_join(rel->op) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		if (r && r->op == op_project) {
			sql_exp *id = NULL;
			node *m;
			/* move project up, ie all attributes of left + the old expression list */
			sql_rel *n = rel_project( sql->sa, (r->l)?rel:rel->l, 
					rel_projections(sql, rel->l, NULL, 1, 1));

			/* only pass bound variables */
			if (is_left(rel->op) && exps_have_freevar(sql, r->exps)) {
				id = rel_bound_exp(sql, r);
				id = rel_project_add_exp(sql, n, id);
			}
			for (m=r->exps->h; m; m = m->next) {
				sql_exp *e = m->data;

				if (!e->freevar || exp_name(e)) { /* only skip full freevars */
					if (exp_has_freevar(sql, e)) {
						rel_bind_var(sql, rel->l, e);
						if (is_left(rel->op)) { /* add ifthenelse */
							/* need bound var from r */
							/* if id is NULL then NULL else e */
							sql_exp *ne = rel_unop_(sql, NULL, exp_copy(sql, id), NULL, "isnull", card_value);
							set_has_no_nil(ne);
							ne = rel_nop_(sql, NULL, ne, exp_null(sql->sa, exp_subtype(e)), e, NULL, NULL, "ifthenelse", card_value);
							exp_prop_alias(sql->sa, ne, e);
							e = ne;
						}
					}
				}
				if (r->l)
					e = exp_rewrite(sql, r->l, e, ad);
				append(n->exps, e);
			}
			if (r->r) {
				list *exps = r->r, *oexps = n->r = sa_list(sql->sa);

				for (m=exps->h; m; m = m->next) {
					sql_exp *e = m->data;

					if (!e->freevar || exp_name(e)) { /* only skip full freevars */
						if (exp_has_freevar(sql, e)) 
							rel_bind_var(sql, rel->l, e);
					}
					append(oexps, e);
				}
			}
			/* remove old project */
			rel->r = r->l;
			r->l = NULL;
                        rel_destroy(r);
			return n;
		}
	}
	/* a dependent semi/anti join with a project on the right side, could be removed */
	if (rel && is_semi(rel->op) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		/* merge project expressions into the join expressions  */
		rel->exps = push_up_project_exps(sql, r, rel->exps);

		if (r && r->op == op_project && r->l) {
			/* remove old project */
			rel->r = rel_dup(r->l);
			rel_destroy(r);
			return rel;
		} else if (r && r->op == op_project) {
			/* remove freevars from projection */
			list *exps = r->exps, *nexps = sa_list(sql->sa);
			node *m;

			for (m=exps->h; m; m = m->next) {
				sql_exp *e = m->data;

				if (!exp_has_freevar(sql, e))
					append(nexps, e);
			}
			if (list_empty(nexps)) {
				assert(!r->l);
				/* remove old project and change outer into select */
				rel->r = NULL;
				rel_destroy(r);
				rel->op = op_select;
				return rel;
			}
			r->exps = nexps;
		}
	}
	return rel;
}

static sql_rel *
push_up_topn(mvc *sql, sql_rel *rel) 
{
	/* a dependent semi/anti join with a project on the right side, could be removed */
	if (rel && (is_semi(rel->op) || is_join(rel->op)) && is_dependent(rel)) {
		sql_rel *r = rel->r;

		if (r && r->op == op_topn) {
			/* remove old topn */
			rel->r = rel_dup(r->l);
			rel = rel_topn(sql->sa, rel, r->exps);
			rel_destroy(r);
			return rel;
		}
	}
	return rel;
}

static sql_rel *
push_up_select(mvc *sql, sql_rel *rel, list *ad) 
{
	sql_rel *d = rel->l;
	sql_rel *r = rel->r;
	int inner = 0;

	if (rel && is_dependent(rel) && r && r->op == op_select) {
		sql_rel *rl = r->l;

		if (rl && rel_has_freevar(sql, rl)) {
			list *inner_ad = rel_dependent_var(sql, d, rl);

			inner = !list_empty(inner_ad);
		}
	}
	if (inner && is_left(rel->op) && !need_distinct(d)) 
		return rel_general_unnest(sql, rel, ad);
	/* input rel is dependent join with on the right a select */ 
	if ((!inner || is_semi(rel->op)) && rel && is_dependent(rel)) {
		sql_rel *r = rel->r;

		if (r && r->op == op_select) { /* move into join */
			node *n;

			for (n=r->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				e = exp_copy(sql, e);
				if (exp_has_freevar(sql, e)) 
					rel_bind_var(sql, rel->l, e);
				rel_join_add_exp(sql->sa, rel, e);
			}
			/* remove select */
			rel->r = rel_dup(r->l);
			rel_destroy(r);
			if (!inner)
				reset_dependent(rel);
		}
	} else if (rel && is_join(rel->op) && is_dependent(rel)) {
		int cp = rel_is_ref(r);
		sql_rel *r = rel->r;
		list *exps = r->exps;

		/* remove select */
		rel->r = rel_dup(r->l);
		rel = rel_select(sql->sa, rel, NULL);
		rel->exps = !cp?exps:exps_copy(sql, exps);
		rel_destroy(r);
	}
	return rel;
}

static int
exps_is_constant( list *exps )
{
	sql_exp *e;

	if (!exps || list_empty(exps))
		return 1;
	if (list_length(exps) > 1)
		return 0;
	e = exps->h->data;
	return exp_is_atom(e);
}

static int
exp_is_count(sql_exp *e, sql_rel *rel) 
{
	if (!e || !rel)
		return 0;
	if (is_alias(e->type) && is_project(rel->op)) {
		sql_exp *ne = rel_find_exp(rel->l, e);
		return exp_is_count(ne, rel->l);
	}
	if (is_convert(e->type))
		return exp_is_count(e->l, rel);
	if (is_aggr(e->type) && exp_aggr_is_count(e)) 
		return 1;
	return 0;
}

static sql_rel *
push_up_groupby(mvc *sql, sql_rel *rel, list *ad) 
{
	/* input rel is dependent join with on the right a groupby */ 
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *l = rel->l, *r = rel->r;

		/* left of rel should be a set */ 
		if (l && is_distinct_set(sql, l, ad) && r && r->op == op_groupby) {
			list *sexps, *jexps, *a = rel_projections(sql, rel->l, NULL, 1, 1);
			node *n;
			sql_exp *id = NULL;

			/* move groupby up, ie add attributes of left + the old expression list */

			if (l && list_length(a) > 1 && !need_distinct(l)) { /* add identity call only if there's more than one column in the groupby */
				rel->l = rel_add_identity(sql, l, &id); /* add identity call for group by */
				assert(id);
			}
		
			assert(rel->op != op_anti);
			if (rel->op == op_semi && !need_distinct(l))
				rel->op = op_join;

			for (n = r->exps->h; n; n = n->next ) {
				sql_exp *e = n->data;

				/* count_nil(* or constant) -> count(t.TID) */
				if (exp_is_count(e, r) && (!e->l || exps_is_constant(e->l))) {
					sql_exp *col;
					sql_rel *p = r->l; /* ugh */

					if (!is_project(p->op))
						r->l = p = rel_project(sql->sa, p, rel_projections(sql, p, NULL, 1, 1));
					col = p->exps->t->data;
					if (strcmp(exp_name(col), TID) != 0) {
						col = exp_ref(sql->sa, col);
						col = exp_unop(sql->sa, col, sql_bind_func(sql->sa, NULL, "identity", exp_subtype(col), NULL, F_FUNC));
						col = exp_label(sql->sa, col, ++sql->label);
						append(p->exps, col);
					}
					col = exp_ref(sql->sa, col);
					append(e->l=sa_list(sql->sa), col);
					set_no_nil(e);
				}
				if (exp_has_freevar(sql, e)) 
					rel_bind_var(sql, rel->l, e);
			}
			r->exps = list_merge(r->exps, a, (fdup)NULL);
			if (!r->r) {
				if (id)
					r->r = list_append(sa_list(sql->sa), exp_ref(sql->sa, id));
				else
					r->r = exps_copy(sql, a);
			} else {
				if (id)
					list_append(r->r, exp_ref(sql->sa, id));
				else
					r->r = list_distinct(list_merge(r->r, exps_copy(sql, a), (fdup)NULL), (fcmp)exp_equal, (fdup)NULL);
			}

			if (!r->l) {
				r->l = rel->l;
				rel->l = NULL;
				rel->r = NULL;
				rel_destroy(rel);
				/* merge (distinct) projects / group by (over the same group by cols) */
				while (r->l && exps_have_freevar(sql, r->exps)) {
					sql_rel *l = r->l;

					if (!is_project(l->op))
						break;
					if (l->op == op_project && need_distinct(l)) { /* TODO: check if group by exps and distinct list are equal */
						r->l = rel_dup(l->l);
						rel_destroy(l);
					}
					if (l->op == op_groupby) { /* TODO: check if group by exps and distinct list are equal */
						/* add aggr exps of r too l, replace r by l */ 
						node *n;
						for(n = r->exps->h; n; n = n->next) {
							sql_exp *e = n->data;

							if (e->type == e_aggr)
								append(l->exps, e);
							if (exp_has_freevar(sql, e)) 
								rel_bind_var(sql, l, e);
						}
						r->l = NULL;
						rel_destroy(r);
						r = l;
					}
				}
				return r;
			} else {
				rel->r = r->l; 
				r->l = rel;
			}
			/* check if a join expression needs to be moved above the group by (into a select) */
			sexps = sa_list(sql->sa);
			jexps = sa_list(sql->sa);
			if (rel->exps) {
				for (n = rel->exps->h; n; n = n->next ) {
					sql_exp *e = n->data;
	
					if (rel_find_exp(rel, e)) {
						append(jexps, e);
					} else {
						append(sexps, e);
					}
				}
			}
			rel->exps = jexps;
			if (list_length(sexps)) {
				r = rel_select(sql->sa, r, NULL);
				r->exps = sexps;
			}
			return r;
		}
	}
	return rel;
}

static sql_rel *
push_up_select_l(mvc *sql, sql_rel *rel) 
{
	(void)sql;
	/* input rel is dependent join with on the right a project */ 
	if (rel && (is_join(rel->op) || is_semi(rel->op))) {
		sql_rel *l = rel->l;

		if (is_select(l->op) && rel_has_freevar(sql, l) && !rel_is_ref(l) ) {
			/* push up select (above join) */
			rel->l = l->l;
			l->l = rel;
			return l;
		}
	}
	return rel;
}

static sql_rel *
push_up_join(mvc *sql, sql_rel *rel, list *ad) 
{
	/* input rel is dependent join with on the right a project */ 
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *d = rel->l, *j = rel->r;

		/* left of rel should be a set */ 
		if (d && is_distinct_set(sql, d, ad) && j && (is_join(j->op) || is_semi(j->op))) {
			sql_rel *jl = j->l, *jr = j->r;
			/* op_join if F(jl) intersect A(D) = empty -> jl join (D djoin jr) 
			 * 	      F(jr) intersect A(D) = empty -> (D djoin jl) join jr
			 * 	 else (D djoin jl) natural join (D djoin jr)
			 *
			 * */
			list *rd = NULL, *ld = NULL; 

			if (is_semi(j->op) && is_select(jl->op) && rel_has_freevar(sql, jl) && !rel_is_ref(jl)) {
				rel->r = j = push_up_select_l(sql, j);
				return rel; /* ie try again */
			}
			rd = (j->op != op_full && j->op != op_right)?rel_dependent_var(sql, d, jr):(list*)1;
			ld = ((j->op == op_join || j->op == op_right))?rel_dependent_var(sql, d, jl):(list*)1;

			if (ld && rd) {
				node *m;
				sql_rel *n, *nr, *nj;
				list *inner_exps = exps_copy(sql, j->exps);
				list *outer_exps = exps_copy(sql, rel->exps);

				rel->r = rel_dup(jl);
				rel->exps = sa_list(sql->sa);
				nj = rel_crossproduct(sql->sa, rel_dup(d), rel_dup(jr), j->op);
				rel_destroy(j);
				j = nj;
				set_dependent(j);
				n = rel_crossproduct(sql->sa, rel, j, j->op);
				n->exps = outer_exps;
				if (!n->exps)
					n->exps = inner_exps;
				else
					n->exps = list_merge(n->exps, inner_exps, (fdup)NULL);
				j->op = rel->op;
				if (is_semi(rel->op)) {
					j->op = op_left;
					rel->op = op_left;
				}
				n->l = rel_project(sql->sa, n->l, rel_projections(sql, n->l, NULL, 1, 1));
				nr = n->r;
				nr = n->r = rel_project(sql->sa, n->r, is_semi(nr->op)?sa_list(sql->sa):rel_projections(sql, nr->r, NULL, 1, 1));
				/* add nr->l exps with labels */ 
				/* create jexps */
				if (!n->exps)
					n->exps = sa_list(sql->sa);
				for (m = d->exps->h; m; m = m->next) { 
					sql_exp *e = m->data, *pe, *je;

					pe = exp_ref(sql->sa, e);
					pe = exp_label(sql->sa, pe, ++sql->label);
					append(nr->exps, pe);
					pe = exp_ref(sql->sa, pe);
					e = exp_ref(sql->sa, e);
					je = exp_compare(sql->sa, e, pe, cmp_equal_nil);
					append(n->exps, je);
				}
				return n;
			}

			if (!rd) {
				rel->r = rel_dup(jl);
				sql_rel *nj = rel_crossproduct(sql->sa, rel, rel_dup(jr), j->op);
				nj->exps = exps_copy(sql, j->exps);
				rel_destroy(j);
				j = nj; 
				if (is_semi(rel->op)) {
				//assert(!is_semi(rel->op));
					rel->op = op_left;
				}
				move_join_exps(sql, j, rel);
				return j;
			}
			if (!ld) {
				rel->r = rel_dup(jr);
				sql_rel *nj = rel_crossproduct(sql->sa, rel_dup(jl), rel, j->op);
				nj->exps = exps_copy(sql, j->exps);
				rel_destroy(j);
				j = nj; 
				if (is_semi(rel->op)) {
				//assert(!is_semi(rel->op));
					rel->op = op_left;
				}
				move_join_exps(sql, j, rel);
				return j;
			}
			assert(0);
			return rel;
		}
	}
	return rel;
}

static sql_rel *
push_up_set(mvc *sql, sql_rel *rel, list *ad) 
{
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *d = rel->l, *s = rel->r;

		/* left of rel should be a set */ 
		if (d && is_distinct_set(sql, d, ad) && s && is_set(s->op)) {
			list *sexps;
			node *m;
			sql_rel *sl = s->l, *sr = s->r, *n;

			/* D djoin (sl setop sr) -> (D djoin sl) setop (D djoin sr) */
			rel->r = sl;
			n = rel_crossproduct(sql->sa, rel_dup(d), sr, rel->op);
			set_dependent(n);
			s->l = rel;
			s->r = n;
			sexps = sa_list(sql->sa);
			for (m = d->exps->h; m; m = m->next) { 
				sql_exp *e = m->data, *pe;

				pe = exp_ref(sql->sa, e);
				append(sexps, pe);
			}
			s->exps = list_merge(sexps, s->exps, (fdup)NULL);
			/* add projections to inner parts of the union */
			s->l = rel_project(sql->sa, s->l, rel_projections(sql, s->l, NULL, 1, 1));
			s->r = rel_project(sql->sa, s->r, rel_projections(sql, s->r, NULL, 1, 1));
			return s;
		}
	}
	return rel;
}

static sql_rel *
push_up_table(mvc *sql, sql_rel *rel, list *ad) 
{
	(void)sql;
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		sql_rel *d = rel->l, *tf = rel->r;

		/* for now just push d into function */
		if (d && is_distinct_set(sql, d, ad) && tf && is_base(tf->op)) {
			if (tf->l) {
				sql_rel *l = tf->l;

				assert(!l->l);
				l->l = rel_dup(d);
			} else {
				tf->l = rel_dup(d);
			}
			return rel;
		}
	}
	return rel;
}

/* reintroduce selects, for freevar's of other dependent joins */
static sql_rel *
push_down_select(mvc *sql, sql_rel *rel)
{
	if (!list_empty(rel->exps)) {
		node *n;
		list *jexps = sa_list(sql->sa);
		list *sexps = sa_list(sql->sa);
		sql_rel *d = rel->l;

		for(n=rel->exps->h; n; n=n->next) {
			sql_exp *e = n->data;
			list *v = exp_freevar(sql, e);
			int found = 1;

			if (v) {
				node *m;
				for(m=v->h; m && found; m=m->next) {
					sql_exp *fv = m->data;

					found = (rel_find_exp(d, fv) != NULL);
				}
			}
			if (found) {
				append(jexps, e);
			} else {
				append(sexps, e);
			}
		}	
		if (!list_empty(sexps)) {
			sql_rel *r;

			rel->exps = jexps;
			r = rel->r = rel_select(sql->sa, rel->r, NULL);
			r->exps = sexps;
		}
	}
	return rel;
}

static sql_rel *
rel_unnest_dependent(mvc *sql, sql_rel *rel)
{
	sql_rel *nrel = rel;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	/* current unnest only possible for equality joins, <, <> etc needs more work */
	if (rel && (is_join(rel->op) || is_semi(rel->op)) && is_dependent(rel)) {
		/* howto find out the left is a set */
		sql_rel *l, *r;

		l = rel->l;
		r = rel->r;

		if (rel_has_freevar(sql, l))
			rel->l = rel_unnest_dependent(sql, rel->l);

		if (!rel_has_freevar(sql, r)) {
			reset_dependent(rel);
			/* reintroduce selects, for freevar's of other dependent joins */
			return push_down_select(sql, rel);
		}

		/* try to push dependent join down */
		if (rel_has_freevar(sql, r)) {
			list *ad = rel_dependent_var(sql, rel->l, rel->r);

			if (r && is_simple_project(r->op) && (!exps_have_freevar(sql, r->exps) || is_distinct_set(sql, l, ad))) {
				rel = push_up_project(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_topn(r->op)) {
				rel = push_up_topn(sql, rel);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_select(r->op) && ad) {
				rel = push_up_select(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_groupby(r->op) && need_distinct(l) /*&& is_distinct_set(sql, l, ad)*/) { 
				rel = push_up_groupby(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && (is_join(r->op) || is_semi(r->op)) && is_distinct_set(sql, l, ad)) {
				rel = push_up_join(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_set(r->op) && (!is_left(rel->op) && is_distinct_set(sql, l, ad))) {
				rel = push_up_set(sql, rel, ad);
				return rel_unnest_dependent(sql, rel);
			}

			if (r && is_base(r->op) && is_distinct_set(sql, l, ad)) { /* TODO table functions need dependent implementation */
				rel = push_up_table(sql, rel, ad);
				return rel; 
			}

			/* fallback */
			if (ad != NULL)
				rel = rel_general_unnest(sql, rel, ad);

			/* no dependent variables */
			reset_dependent(rel);
			rel->r = rel_unnest_dependent(sql, rel->r);
		} else {
			rel->l = rel_unnest_dependent(sql, rel->l);
			rel->r = rel_unnest_dependent(sql, rel->r);
		}
	} else {
		if (rel && (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op)))
			rel->l = rel_unnest_dependent(sql, rel->l);
		else if (rel && (is_join(rel->op) || is_semi(rel->op) ||  is_set(rel->op) || is_modify(rel->op) || is_ddl(rel->op))) {
			rel->l = rel_unnest_dependent(sql, rel->l);
			rel->r = rel_unnest_dependent(sql, rel->r);
		}
	}
	return nrel;
}

static sql_rel *
_rel_unnest(mvc *sql, sql_rel *rel)
{
	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
	if (!rel)
		return rel;

	switch (rel->op) {
	case op_basetable:
	case op_table:
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->l = _rel_unnest(sql, rel->l);
		rel->r = _rel_unnest(sql, rel->r);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = _rel_unnest(sql, rel->l);
		break;
	case op_ddl:
		rel->l = _rel_unnest(sql, rel->l);
		if (rel->r)
			rel->r = _rel_unnest(sql, rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		rel->l = _rel_unnest(sql, rel->l);
		rel->r = _rel_unnest(sql, rel->r);
		break;
	}
	if (is_dependent(rel)) 
		rel = rel_unnest_dependent(sql, rel);
	return rel;
}

static void
rel_reset_subquery(sql_rel *rel)
{
	if (!rel)
		return;

	rel->subquery = 0;
	switch(rel->op){
	case op_basetable:
	case op_table:
		break;
	case op_ddl:
		rel_reset_subquery(rel->l);
		if (rel->r)
			rel_reset_subquery(rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		if (rel->l)
			rel_reset_subquery(rel->l);
		if (rel->r)
			rel_reset_subquery(rel->r);
		break;
	case op_select:
	case op_topn:
	case op_sample:

	case op_project:
	case op_groupby:
		if (rel->l)
			rel_reset_subquery(rel->l);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:

	case op_union:
	case op_inter:
	case op_except:
		if (rel->l)
			rel_reset_subquery(rel->l);
		if (rel->r)
			rel_reset_subquery(rel->r);
	}

}

static sql_exp *
rewrite_inner(mvc *sql, sql_rel *rel, sql_rel *inner, operator_type op)
{
	sql_rel *d = NULL;

	if (!is_project(inner->op)) 
		inner = rel_project(sql->sa, inner, rel_projections(sql, inner, NULL, 1, 1));

	if (is_join(rel->op)){ /* TODO handle set operators etc */ 
		d = rel->r = rel_crossproduct(sql->sa, rel->r, inner, op);
	} else if (is_project(rel->op)){ /* projection -> op_left */
		if (rel->l) {
			d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op_left);
		} else {
			d = rel->l = inner;
		}
	} else {
		d = rel->l = rel_crossproduct(sql->sa, rel->l, inner, op);
	}
	if (d && rel_has_freevar(sql, inner)) {
		list *dv = rel_dependent_var(sql, d, inner);
		list *fv = rel_freevar(sql, inner);
		/* check if the inner depends on the new join (d) or one leve up */
		if (list_length(dv))
			set_dependent(d);
		if (list_length(fv) != list_length(dv))
			set_dependent(rel);
	}
	return inner->exps->t->data;
}

static sql_exp *
rewrite_exp_rel(mvc *sql, sql_rel *rel, sql_exp *e, int depth)
{
	(void)depth;
	if (exp_has_rel(e) && !is_ddl(rel->op)) {
		sql_exp *ne = rewrite_inner(sql, rel, exp_rel_get_rel(sql->sa, e), op_join);

		if (!ne)
			return ne;
		if (exp_is_rel(e)) {
			ne = exp_ref(sql->sa, ne);
			if (exp_name(e))
				exp_prop_alias(sql->sa, ne, e);
			if (!exp_name(ne))
				ne = exp_label(sql->sa, ne, ++sql->label);
			e = ne;
		} else {
			e = exp_rel_update_exp(sql->sa, e);
		}
	}
	if (exp_is_rel(e) && is_ddl(rel->op))
		e->l = rel_exp_visitor(sql, e->l, &rewrite_exp_rel);
	return e;
}

#define is_not_func(sf) (strcmp(sf->func->base.name, "not") == 0) 
#define is_not_anyequal(sf) (strcmp(sf->func->base.name, "sql_not_anyequal") == 0)

/* simplify expressions, such as not(not(x)) */
/* exp visitor */

static list *
exps_simplify_exp(mvc *sql, list *exps)
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
				list *l = e->l = exps_simplify_exp(sql, e->l);
				list *r = e->r = exps_simplify_exp(sql, e->r); 

				if (list_length(l) == 1) {
					sql_exp *ie = l->h->data; 
	
					if (exp_is_true(sql, ie)) {
						continue;
					} else if (exp_is_false(sql, ie)) {
						nexps = list_merge(nexps, r, (fdup)NULL);
						continue;
					}
				} else if (list_length(l) == 0) { /* left is true */
					continue;
				}
				if (list_length(r) == 1) {
					sql_exp *ie = r->h->data; 
	
					if (exp_is_true(sql, ie))
						continue;
					else if (exp_is_false(sql, ie)) {
						nexps = list_merge(nexps, l, (fdup)NULL);
						continue;
					}
				} else if (list_length(r) == 0) { /* right is true */
					continue;
				}
			}
			/* TRUE and X -> X */
			if (exp_is_true(sql, e)) {
				continue;
			/* FALSE and X -> FALSE */
			} else if (exp_is_false(sql, e)) {
				return append(sa_list(sql->sa), e);
			} else {
				append(nexps, e);
			}
		}
		return nexps;
	}
	return exps;
}

static sql_exp *
rewrite_simplify_exp(mvc *sql, sql_rel *rel, sql_exp *e, int depth)
{
	if (!e)
		return e;

	(void)sql; (void)rel; (void)depth;

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
			return ie;
		}
		if (is_func(ie->type) && list_length(ie->l) == 2 && is_not_anyequal(sf)) {
			args = ie->l;

			sql_exp *l = args->h->data;
			sql_exp *vals = args->h->next->data;

			ie = exp_in_func(sql, l, vals, 1, 0);
			if (exp_name(e))
				exp_prop_alias(sql->sa, ie, e);
			return ie;
		}
		/* TRUE or X -> TRUE
		 * FALSE or X -> X */
		if (is_compare(e->type) && e->flag == cmp_or) {
			list *l = e->l = exps_simplify_exp(sql, e->l);
			list *r = e->r = exps_simplify_exp(sql, e->r); 

			sql->caching = 0;
			if (list_length(l) == 1) {
				sql_exp *ie = l->h->data; 

				if (exp_is_true(sql, ie))
					return ie;
				else if (exp_is_false(sql, ie) && list_length(r) == 1)
					return r->h->data;
			} else if (list_length(l) == 0) { /* left is true */
				return exp_atom_bool(sql->sa, 1);
			}
			if (list_length(r) == 1) {
				sql_exp *ie = r->h->data; 

				if (exp_is_true(sql, ie))
					return ie;
				else if (exp_is_false(sql, ie) && list_length(l) == 1)
					return l->h->data;
			} else if (list_length(r) == 0) { /* right is true */
				return exp_atom_bool(sql->sa, 1);
			}
		}
	}
	return e;
}

static sql_rel *
rewrite_simplify(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;

	if ((is_select(rel->op) || is_join(rel->op)) && !list_empty(rel->exps))
		rel->exps = exps_simplify_exp(sql, rel->exps);
	return rel;
}

/* add an dummy true projection column */
static sql_rel *
rewrite_empty_project(mvc *sql, sql_rel *rel)
{
	if (is_simple_project(rel->op) && list_empty(rel->exps))
		append(rel->exps, exp_atom_bool(sql->sa, 1));
	if (is_groupby(rel->op) && list_empty(rel->exps))
		append(rel->exps, exp_atom_bool(sql->sa, 1));
	return rel;
}

static list*
aggrs_split_args(mvc *sql, list *aggrs, list *exps, int is_groupby_list) 
{
	if (list_empty(aggrs))
		return aggrs;
	for (node *n=aggrs->h; n; n = n->next) {
		sql_exp *a = n->data;

		if (is_func(a->type) && !is_groupby_list)
			continue;
		if (!is_aggr(a->type)) {
			sql_exp *e1 = a, *found = NULL;

			for (node *nn = exps->h; nn && !found; nn = nn->next) {
				sql_exp *e2 = nn->data;
		
				if (!exp_equal(e1, e2))
					found = e2;
			}
			if (!found) {
				if (!exp_name(e1))
					e1 = exp_label(sql->sa, e1, ++sql->label);
				append(exps, e1);
			} else {
				e1 = found;
			}
			e1 = exp_ref(sql->sa, e1);
			n->data = e1; /* replace by reference */
			continue;
		}
		list *args = a->l;

		if (!list_empty(args)) {
			for (node *an = args->h; an; an = an->next) {
				sql_exp *e1 = an->data, *found = NULL, *eo = e1;
				/* we keep converts as they reuse names of inner columns */
				int convert = is_convert(e1->type);

				if (convert)
					e1 = e1->l;
				for (node *nn = exps->h; nn && !found; nn = nn->next) {
					sql_exp *e2 = nn->data;
		
					if (!exp_equal(e1, e2))
						found = e2;
				}
				if (!found) {
					if (!exp_name(e1))
						e1 = exp_label(sql->sa, e1, ++sql->label);
					append(exps, e1);
				} else {
					e1 = found;
				}
				e1 = exp_ref(sql->sa, e1);
				/* replace by reference */
				if (convert) 
					eo->l = e1;
				else
					an->data = e1; 
			}
		}
	}
	return aggrs;
}

static int
exps_complex(list *exps) 
{
	if (list_empty(exps))
		return 0;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type != e_column && e->type != e_atom)
			return 1;
	}
	return 0;
}

static int
aggrs_complex(list *exps) 
{
	if (list_empty(exps))
		return 0;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type != e_column) {
			if (e->type == e_aggr) {
				if (exps_complex(e->l))
					return 1;
			}
		}
	}
	return 0;
}

/* simplify aggregates, ie push functions under the groupby relation */
/* rel visitor */
static sql_rel *
rewrite_aggregates(mvc *sql, sql_rel *rel)
{
	if (is_groupby(rel->op) && (exps_complex(rel->r) || aggrs_complex(rel->exps))) {
		list *exps = sa_list(sql->sa);

		rel->r = aggrs_split_args(sql, rel->r, exps, 1);
		rel->exps = aggrs_split_args(sql, rel->exps, exps, 0);
		rel->l = rel_project(sql->sa, rel->l, exps);
		return rel;
	}
	return rel;
}

/* remove or expressions with subqueries */
static sql_rel *
rewrite_or_exp(mvc *sql, sql_rel *rel)
{
	if ((is_select(rel->op) || is_join(rel->op)) && !list_empty(rel->exps)) {
		for(node *n=rel->exps->h; n; n=n->next) {
			sql_exp *e = n->data;

			if (is_compare(e->type) && e->flag == cmp_or) {
				/* check for exp_is_rel */
				if (exps_have_rel_exp(e->l) || exps_have_rel_exp(e->r)) {
					/* rewrite into setop */
					sql_rel *l = rel; 
					sql_rel *r = rel_dup(rel);
					list *exps = rel_projections(sql, rel, NULL, 1, 1);

					list_remove_node(rel->exps, n); /* remove or expression */

					l = rel_select(sql->sa, l, NULL);
					l->exps = e->l;
					l = rewrite_or_exp(sql, l);
					r = rel_select(sql->sa, r, NULL);
					r->exps = e->r;
					r = rewrite_or_exp(sql, r);

					list *ls = rel_projections(sql, rel, NULL, 1, 1);
					list *rs = rel_projections(sql, rel, NULL, 1, 1);
					rel = rel_setop_check_types(sql, l, r, ls, rs, op_union);
					rel = rel_distinct(rel);
					rel->exps = exps;
					return rel;
				}
			}	
		}
	}
	return rel;
}

/* exp visitor */
static sql_exp *
rewrite_rank(mvc *sql, sql_rel *rel, sql_exp *e, int depth)
{
	sql_rel *rell = NULL;
	int needed = 0;

	if (e->type != e_func || !e->r /* e->r means window function */)
		return e;

	(void)depth;
	/* ranks/window functions only exist in the projection */
	assert(is_simple_project(rel->op));
	list *l = e->l, *r = e->r, *gbe = r->h->data, *obe = r->h->next->data; 

	needed = (gbe || obe);
	for (node *n = l->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;
		needed = e->ref;
	}

	if (needed) {
		rell = rel->l = rel_project(sql->sa, rel->l, rel_projections(sql, rel->l, NULL, 1, 1));
		for (node *n = l->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->ref) {
				e->ref = 0;
				append(rell->exps, e); 
				n->data = exp_ref(sql->sa, e);
			}
		}
	}
	if (gbe || obe) {
		if (gbe && obe) {
			gbe = list_merge(sa_list(sql->sa), gbe, (fdup)NULL); /* make sure the p->r is a different list than the gbe list */
			for(node *n = obe->h ; n ; n = n->next) {
				sql_exp *e1 = n->data;
				bool found = false;

				for(node *nn = gbe->h ; nn && !found ; nn = nn->next) {
					sql_exp *e2 = nn->data;
					/* the partition expression order should be the same as the one in the order by clause (if it's in there as well) */
					if (exp_match(e1, e2)) {
						if (is_ascending(e1))
							set_ascending(e2);
						else
							set_descending(e2);
						if (nulls_last(e1))
							set_nulls_last(e2);
						else
							set_nulls_first(e2);
						found = true;
					}
				}
				if(!found)
					append(gbe, e1);
			}
		} else if (obe) {
			for(node *n = obe->h ; n ; n = n->next) {
				sql_exp *oe = n->data;
				if (!exps_find_exp(rell->exps, oe)) {
					sql_exp *ne = exp_ref(sql->sa, oe);

					if (is_ascending(oe))
						set_ascending(ne);
					if (nulls_last(oe))
						set_nulls_last(ne);
					/* disable sorting info (ie back too defaults) */
					set_descending(oe);
					set_nulls_first(oe);
					n->data = ne;
					append(rell->exps, oe);
				}
			}
			gbe = obe;
		}
		rell->r = gbe;
		rel->l = rell;

		/* mark as normal (analytic) function now */
		e->r = NULL;

		/* add project with rank */
		rell = rel->l = rel_project(sql->sa, rel->l, rel_projections(sql, rell->l, NULL, 1, 1));
		/* move rank down add ref */
		if (!exp_name(e))
			e = exp_label(sql->sa, e, ++sql->label);
		append(rell->exps, e); 
		e = exp_ref(sql->sa, e);
	} else {
		/* mark as normal (analytic) function now */
		e->r = NULL;
	}
	return e;
}

#define is_anyequal_func(sf) (strcmp(sf->func->base.name, "sql_anyequal") == 0 || strcmp(sf->func->base.name, "sql_not_anyequal") == 0)
#define is_anyequal(sf) (strcmp(sf->func->base.name, "sql_anyequal") == 0)

static sql_rel *
rel_union_exps(mvc *sql, sql_exp **l, list *vals, int is_tuple)
{
	sql_rel *u = NULL;
	list *exps = NULL;

	for(node *n=vals->h; n; n = n->next) {
		sql_exp *ve = n->data, *r;
		sql_rel *sq = NULL;

		if (exp_has_rel(ve)) 
			sq = exp_rel_get_rel(sql->sa, ve); /* get subquery */
		else
			sq = rel_project(sql->sa, NULL, append(sa_list(sql->sa), ve));
		/* TODO merge expressions (could x+ cast(y) where y is result of a sub query) */
		r = sq->exps->t->data;
		if (!is_tuple && rel_convert_types(sql, NULL, NULL, l, &r, 1, type_equal) < 0)
			return NULL;
		sq->exps->t->data = r;
		if (!u) {
			u = sq;
			exps = rel_projections(sql, sq, NULL, 1/*keep names */, 1);
		} else {
			u = rel_setop(sql->sa, u, sq, op_union);
			u->exps = exps;
			exps = rel_projections(sql, sq, NULL, 1/*keep names */, 1);
		}
	}
	return u;
}

static sql_exp *
exp_in_project(mvc *sql, sql_exp **l, list *vals, int anyequal)
{
	sql_exp *e = NULL;

	for(node *n=vals->h; n; n = n->next) {
		sql_exp *r = n->data, *ne;

		if (rel_convert_types(sql, NULL, NULL, l, &r, 1, type_equal_no_any) < 0)
			return NULL;
		if (anyequal)
			ne = rel_binop_(sql, NULL, *l, r, NULL, "=", card_value);
		else
			ne = rel_binop_(sql, NULL, *l, r, NULL, "<>", card_value);
		if (!e) {
			e = ne;
		} else if (anyequal) {
			e = rel_binop_(sql, NULL, e, ne, NULL, "or", card_value);
		} else {
			e = rel_binop_(sql, NULL, e, ne, NULL, "and", card_value);
		}
	}
	return e;
}

static sql_exp *
exp_in_compare(mvc *sql, sql_exp **l, list *vals, int anyequal) 
{
	int vals_only = 1;

	for(node *n=vals->h; n; n = n->next) {
		sql_exp *r = n->data;

		if (rel_convert_types(sql, NULL, NULL, l, &r, 1, type_equal_no_any) < 0)
			return NULL;
		n->data = r;
		if (!exp_is_atom(r)) 
			vals_only = 0;
	}
	if (vals_only)
		return exp_in(sql->sa, *l, vals, anyequal?cmp_in:cmp_notin);

	*l = exp_in_project(sql, l, vals, anyequal); 
	return exp_compare(sql->sa, *l, exp_atom_bool(sql->sa, 1), cmp_equal);
}

/* exp visitor */
static sql_exp *
rewrite_anyequal(mvc *sql, sql_rel *rel, sql_exp *e, int depth)
{
	sql_subfunc *sf;
	if (e->type != e_func)
		return e;

	sf = e->f;
	if (is_anyequal_func(sf) && !list_empty(e->l)) {
		list *l = e->l;

		if (list_length(l) == 2) { /* input is a set */

			sql_exp *ile = l->h->data, *le, *re = l->h->next->data;
			sql_rel *lsq = NULL, *rsq = NULL;
			int is_tuple = 0;

			/* possibly this is already done ? */
			if (exp_has_rel(ile)) 
				lsq = exp_rel_get_rel(sql->sa, ile); /* get subquery */

			if (lsq)
				le = lsq->exps->t->data;
			else
				le = ile; 

			if (is_values(le)) /* exp_values */
				is_tuple = 1;

			/* re should be a values list */
			if (!is_tuple && is_values(re) && !exps_have_rel_exp(re->f)) { /* exp_values */
				list *vals = re->f;

				if (depth == 0 && is_select(rel->op))
					return exp_in_compare(sql, &le, vals, is_anyequal(sf));
				else
					return exp_in_project(sql, &le, vals, is_anyequal(sf));
			}

			if (is_atom(re->type) && re->f) { /* exp_values */
				/* flatten using unions */
				rsq = rel_union_exps(sql, &le, re->f, is_tuple);
				re = rsq->exps->t->data;

				if (!is_tuple && !is_freevar(re)) {
					re = exp_label(sql->sa, re, ++sql->label); /* unique name */
					list_hash_clear(rsq->exps);
					re = exp_ref(sql->sa, re);
				} else if (has_label(re)) {
					re = exp_ref(sql->sa, re);
				}
			}

			if (is_project(rel->op) || depth > 0) {
				list *exps = NULL;
				sql_exp *rid, *lid;
				sql_rel *sq = lsq;

				assert(!is_tuple);
				rsq = rel_add_identity2(sql, rsq, &rid);
				rid = exp_ref(sql->sa, rid);

				if (!lsq)
					lsq = rel->l;
				if (!lsq) { /* single row */
					lid = exp_atom_lng(sql->sa, 1);
				} else {
					exps = rel_projections(sql, lsq, NULL, 1/*keep names */, 1);
					lsq = rel_add_identity(sql, lsq, &lid);
					if (!sq)
						rel->l = lsq;
					lid = exp_ref(sql->sa, lid);
				}

				if (sq)
					(void)rewrite_inner(sql, rel, lsq, op_join);
				if (rsq) 
					(void)rewrite_inner(sql, rel, rsq, op_join);
	
				lsq = rel->l = rel_groupby(sql, rel->l, exp2list(sql->sa, lid)); 
				if (exps)
					lsq->exps = exps; 

				sql_subaggr *ea = sql_bind_aggr(sql->sa, sql->session->schema, is_anyequal(sf)?"anyequal":"allnotequal", exp_subtype(re));
				sql_exp *a = exp_aggr1(sql->sa, le, ea, 0, 0, CARD_AGGR, has_nil(le));
				append(a->l, re);
				append(a->l, rid);
				le = rel_groupby_add_aggr(sql, lsq, a);
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			} else {
				if (lsq)
					(void)rewrite_inner(sql, rel, lsq, op_join);
				if (rsq) 
					(void)rewrite_inner(sql, rel, rsq, !is_tuple?op_join:is_anyequal(sf)?op_semi:op_anti);
				if (is_tuple) {
					list *t = le->f;
					list *l = sa_list(sql->sa);
					list *r = sa_list(sql->sa);

					/* find out list of right expression */
					if (list_length(t) != list_length(rsq->exps))
						return NULL;
					for (node *n = t->h, *m = rsq->exps->h; n && m; n = n->next, m = m->next ) {
						sql_exp *le = n->data;
						sql_exp *re = m->data;

						append(l, le);
						re = exp_ref(sql->sa, re);
						append(r, re);
					}
					return exp_in_func(sql, exp_values(sql->sa, l), exp_values(sql->sa, r), is_anyequal(sf), 1);
				} else {
					if (exp_has_freevar(sql, le))
						rel_bind_var(sql, rel, le);
					return exp_in_func(sql, le, re, is_anyequal(sf), 0);
				}
			}
		}
	}
	return e;
}

static const char *
compare_aggr_op( char *compare, int quantifier) 
{
	if (quantifier == 0)
		return "zero_or_one";
	switch(compare[0]) {
	case '<':
		if (compare[1] == '>')
			return "all";
		return "min";
	case '>':
		return "max";
	default:
		return "all";
	}
}
/* exp visitor */
/* rewrite compare expressions including quantifiers any and all */
static sql_exp *
rewrite_compare(mvc *sql, sql_rel *rel, sql_exp *e, int depth)
{
	sql_subfunc *sf;
	if (e->type != e_func || is_ddl(rel->op))
		return e;

	sf = e->f;
	if (is_compare_func(sf) && !list_empty(e->l)) {
		list *l = e->l;

		/* TODO handle range expressions */
		if (list_length(l) == 2) { /* input is a set */
			char *op = sf->func->base.name;

			sql_exp *ile = l->h->data, *le, *re = l->h->next->data, *rnull = NULL;
			sql_rel *lsq = NULL, *rsq = NULL;
			int is_tuple = 0; /* TODO add this feature, ie select (1,2) = (1,2) etc */
					 /* cleanup tuple handling by introducing an expression for tuples */
			int quantifier = e->flag;

			/* possibly this is already done ? */
			if (exp_has_rel(ile)) 
				lsq = exp_rel_get_rel(sql->sa, ile); /* get subquery */

			if (lsq)
				le = exp_rel_update_exp(sql->sa, ile);
			else
				le = ile; 

			if (exp_has_rel(re)) 
				rsq = exp_rel_get_rel(sql->sa, re); /* get subquery */
			if (rsq) {
				re = rsq->exps->t->data;
				re = exp_ref(sql->sa, re);
			}

			if (is_values(le)) /* exp_values */
				is_tuple = 1;

			/* re should be a values list */
			if (!is_tuple && exp_is_atom(le) && exp_is_atom(re)) {
				e->flag = 0; /* remove quantifier */

				if (rel_convert_types(sql, NULL, NULL, &le, &re, 1, type_equal_no_any) < 0)
					return NULL;
				le = exp_compare_func(sql, le, re, NULL, op, 0);
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
			if (!is_tuple && is_values(re) && !exps_have_rel_exp(re->f)) { /* exp_values */
				list *vals = re->f;

				assert(0);
				if (depth == 0 && is_select(rel->op))
					return exp_in_compare(sql, &le, vals, is_anyequal(sf));
				else
					return exp_in_project(sql, &le, vals, is_anyequal(sf));
			}

			if (is_values(re)) { /* exp_values */
				/* flatten using unions */
				rsq = rel_union_exps(sql, &le, re->f, is_tuple);
				re = rsq->exps->t->data;

				if (!is_tuple) {
					re = exp_label(sql->sa, re, ++sql->label); /* unique name */
					list_hash_clear(rsq->exps);
					re = exp_ref(sql->sa, re);
				}
			}

			int is_cnt = 0;
			if (rsq)
				is_cnt = exp_is_count(re, rsq);
			if (is_project(rel->op) || depth > 0 || quantifier || is_cnt) {
				sql_rel *sq = lsq;

				assert(!is_tuple);

				if (!lsq)
					lsq = rel->l;
				if (sq) 
					(void)rewrite_inner(sql, rel, sq, op_join);
	
				if (quantifier) { 
					sql_subaggr *a;

					rsq = rel_groupby(sql, rsq, NULL); 
					a = sql_bind_aggr(sql->sa, NULL, "null", exp_subtype(re));
					rnull = exp_aggr1(sql->sa, re, a, 0, 1, CARD_AGGR, has_nil(re));
					rnull = rel_groupby_add_aggr(sql, rsq, rnull);

					if (is_notequal_func(sf))
						op = "=";
					if (op[0] == '<') {
						a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"max":"min", exp_subtype(re));
					} else if (op[0] == '>') {
						a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"min":"max", exp_subtype(re));
					} else /* (op[0] == '=')*/ /* only = ALL */ {
						a = sql_bind_aggr(sql->sa, sql->session->schema, "all", exp_subtype(re));
						is_cnt = 1;
					}
					re = exp_aggr1(sql->sa, re, a, 0, 1, CARD_AGGR, has_nil(re));
					re = rel_groupby_add_aggr(sql, rsq, re);
				} else if (rsq && exp_card(re) > CARD_ATOM) { 
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, NULL, compare_aggr_op(op, quantifier), exp_subtype(re));
	
					rsq = rel_groupby(sql, rsq, NULL);
	
					re = exp_aggr1(sql->sa, re, zero_or_one, 0, 0, CARD_AGGR, has_nil(re));
					re = rel_groupby_add_aggr(sql, rsq, re);
				}
				if (rsq) 
					(void)rewrite_inner(sql, rel, rsq, is_cnt?op_left:op_join);

				if (rel_convert_types(sql, NULL, NULL, &le, &re, 1, type_equal) < 0)
					return NULL;
				if (rnull) { /* complex compare operator */
					sql_exp *lnull = rel_unop_(sql, rel, le, NULL, "isnull", card_value);
					set_has_no_nil(lnull);
					le = exp_compare_func(sql, le, re, NULL, op, 0);
					le = rel_nop_(sql, rel, le, lnull, rnull, NULL, NULL, (quantifier==1)?"any":"all", card_value);
					if (is_notequal_func(sf))
						le = rel_unop_(sql, rel, le, NULL, "not", card_value);
				} else if (is_project(rel->op) || depth) {
					le = exp_compare_func(sql, le, re, NULL, op, 0);
				} else {
					return exp_compare(sql->sa, le, re, compare_str2type(op));
				}
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			} else {
				if (lsq) 
					(void)rewrite_inner(sql, rel, lsq, op_join);
				if (rsq) 
					(void)rewrite_inner(sql, rel, rsq, !is_tuple?op_join:is_anyequal(sf)?op_semi:op_anti);
				if (is_tuple) {
					list *t = le->f;
					list *l = sa_list(sql->sa);
					list *r = sa_list(sql->sa);

					/* find out list of right expression */
					if (list_length(t) != list_length(rsq->exps))
						return NULL;
					for (node *n = t->h, *m = rsq->exps->h; n && m; n = n->next, m = m->next ) {
						sql_exp *le = n->data;
						sql_exp *re = m->data;

						append(l, le);
						append(r, re);
					}
					return exp_compare(sql->sa, exp_values(sql->sa, l), exp_values(sql->sa, r), compare_str2type(op));
				} else {
					if (exp_has_freevar(sql, le))
						rel_bind_var(sql, rel, le);
					if (rel_convert_types(sql, NULL, NULL, &le, &re, 1, type_equal) < 0)
						return NULL;
					return exp_compare(sql->sa, le, re, compare_str2type(op));
				}
			}
		}
	}
	return e;
}


static sql_rel *
rewrite_join2semi(mvc *sql, sql_rel *rel)
{
	if (is_select(rel->op) && !list_empty(rel->exps)) {
		sql_rel *j = rel->l;
		int needed=0;

		if (!j || (!is_join(j->op) && !is_semi(j->op)) || !list_empty(j->exps))
			return rel;
		/* if needed first push select exps down under the join */
		for(node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			if (is_func(e->type) && exp_card(e) > CARD_ATOM && is_anyequal_func(sf) && rel_has_all_exps(j->l, e->l))
				needed = 1;
		}
		if (needed) {
			list *exps = sa_list(sql->sa), *jexps = sa_list(sql->sa);
			sql_rel *l = j->l = rel_select(sql->sa, j->l, NULL);

			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_subfunc *sf = e->f;

				if (is_func(e->type) && exp_card(e) > CARD_ATOM && is_anyequal_func(sf) && rel_has_all_exps(j->l, e->l))
					append(exps, e);
				else
					append(jexps, e);
			}
			rel->exps = jexps;
			l->exps = exps;
			j->l = rewrite_join2semi(sql, j->l);
		}

		needed = 0;
		for(node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			if (is_func(e->type) && is_anyequal_func(sf)) 
				needed = 1;
		}
		if (!needed)
			return rel;
		list *exps = sa_list(sql->sa);
		if (!j->exps)
			j->exps = sa_list(sql->sa);
		for(node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;

			if (is_func(e->type) && is_anyequal_func(sf)) {
				list *args = e->l;
				sql_exp *l, *r;

				assert(list_length(args)==2);
				l = args->h->data;
				r = args->h->next->data;
				j->op = (is_anyequal(sf))?op_semi:op_anti;

				if (is_values(l)) {
					assert(is_values(r));
					list *ll = l->f, *rl = r->f;
					for(node *n=ll->h, *m=rl->h; n && m; n=n->next, m=m->next) {
						e = exp_compare(sql->sa, n->data, m->data, j->op == op_semi?mark_in:mark_notin);
						append(j->exps, e);
					}
				} else {
					e = exp_compare(sql->sa, l, r, j->op == op_semi?mark_in:mark_notin);
					append(j->exps, e);
				}
			} else {
				append(exps, e);
			}
		}
		rel->exps = exps;
	}
	return rel;
}

#define is_exists_func(sf) (strcmp(sf->func->base.name, "sql_exists") == 0 || strcmp(sf->func->base.name, "sql_not_exists") == 0)
#define is_exists(sf) (strcmp(sf->func->base.name, "sql_exists") == 0)

static sql_exp *
exp_exist(mvc *sql, sql_exp *le, sql_exp *ne, int exists)
{
	sql_subfunc *exists_func = NULL;
			
	if (exists)
		exists_func = sql_bind_func(sql->sa, sql->session->schema, "sql_exists", exp_subtype(le), NULL, F_FUNC);
	else
		exists_func = sql_bind_func(sql->sa, sql->session->schema, "sql_not_exists", exp_subtype(le), NULL, F_FUNC);

	if (!exists_func) 
		return sql_error(sql, 02, SQLSTATE(42000) "exist operator on type %s missing", exp_subtype(le)->type->sqlname);
	if (ne) { /* correlated case */
		ne = rel_unop_(sql, NULL, ne, NULL, "isnull", card_value);
		set_has_no_nil(ne);
		le = rel_nop_(sql, NULL, ne, exp_atom_bool(sql->sa, !exists), exp_atom_bool(sql->sa, exists), NULL, NULL, "ifthenelse", card_value);
		return le;
	} else {
		return exp_unop(sql->sa, le, exists_func);
	}
}

/* exp visitor */
static sql_exp *
rewrite_exists(mvc *sql, sql_rel *rel, sql_exp *e, int depth)
{
	sql_subfunc *sf;
	if (e->type != e_func)
		return e;

	sf = e->f;
	if (is_exists_func(sf) && !list_empty(e->l)) {
		list *l = e->l;

		if (list_length(l) == 1) { /* exp_values */
			sql_exp *ne = NULL, *ie = l->h->data, *le;
			sql_rel *sq = NULL;

			if (!exp_is_rel(ie)) /* already fine */
				return e;

			sq = exp_rel_get_rel(sql->sa, ie); /* get subquery */

			le = rel_reduce2one_exp(sql, sq);
			if (!exp_name(le))
				le = exp_label(sql->sa, le, ++sql->label);
			le = exp_ref(sql->sa, le);
			if (is_project(rel->op) && is_freevar(le)) {
				sql_exp *re, *jc, *null;

				re = rel_bound_exp(sql, sq);
				re = rel_project_add_exp(sql, sq, re);
				jc = rel_unop_(sql, NULL, re, NULL, "isnull", card_value);
				set_has_no_nil(jc);
				null = exp_null(sql->sa, exp_subtype(le));
				le = rel_nop_(sql, NULL, jc, null, le, NULL, NULL, "ifthenelse", card_value);
			}

			if (is_project(rel->op) || depth > 0) {
				sql_subaggr *ea = NULL;
				sq = rel_groupby(sql, sq, NULL);

				if (exp_is_rel(ie))
					ie->l = sq;
				ea = sql_bind_aggr(sql->sa, sql->session->schema, is_exists(sf)?"exist":"not_exist", exp_subtype(le));
				le = exp_aggr1(sql->sa, le, ea, 0, 0, CARD_AGGR, has_nil(le));
				le = rel_groupby_add_aggr(sql, sq, le);
				if (rel_has_freevar(sql, sq))
					ne = le;

				if (exp_has_rel(ie)) 
					(void)rewrite_exp_rel(sql, rel, ie, depth);

				if (is_project(rel->op) && rel_has_freevar(sql, sq))
					le = exp_exist(sql, le, ne, is_exists(sf));
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
			} else { /* rewrite into semi/anti join */
				(void)rewrite_inner(sql, rel, sq, is_exists(sf)?op_semi:op_anti);
				return exp_atom_bool(sql->sa, 1);
			}
			return le;
		}
	}
	return e;
}

#define is_ifthenelse_func(sf) (strcmp(sf->func->base.name, "ifthenelse") == 0)
#define is_isnull_func(sf) (strcmp(sf->func->base.name, "isnull") == 0)

/* exp visitor */
static sql_exp *
rewrite_ifthenelse(mvc *sql, sql_rel *rel, sql_exp *e, int depth)
{
	(void)depth;
	/* for ifthenelse and rank flatten referenced inner expressions */
	if (e->ref) {
		sql_rel *r = rel->l = rel_project(sql->sa, rel->l, rel_projections(sql, rel->l, NULL, 1, 1));

		e->ref = 0;
		append(r->exps, e);
		return exp_ref(sql->sa, e);
	}

	sql_subfunc *sf;
	if (e->type != e_func)
		return e;

	sf = e->f;
	if (is_ifthenelse_func(sf) && !list_empty(e->l)) {
		list *l = e->l;
		sql_exp *cond = l->h->data; 
		sql_subfunc *nf = cond->f;

		if (has_nil(cond) && (cond->type != e_func || !is_isnull_func(nf))) {
			/* add is null */
			sql_exp *condnil = rel_unop_(sql, rel, cond, NULL, "isnull", card_value);

			set_has_no_nil(condnil);
			cond = exp_copy(sql, cond);
			cond = rel_nop_(sql, rel, condnil, exp_atom_bool(sql->sa, 0), cond, NULL, NULL, "ifthenelse", card_value);
			l->h->data = cond;
		}
	}
	return e;
}

static list *
rewrite_compare_exps(mvc *sql, list *exps) 
{
	if (list_empty(exps))
		return exps;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (!is_compare(e->type)) {
			n->data = e = exp_compare(sql->sa, e, exp_atom_bool(sql->sa, 1), cmp_equal);
		}
		if (is_compare(e->type) && e->flag == cmp_or) {
			e->l = rewrite_compare_exps(sql, e->l);
			e->r = rewrite_compare_exps(sql, e->r);
		}
	}
	list_hash_clear(exps);
	return exps;
}

/* add an dummy true projection column */
static sql_rel *
rewrite_compare_exp(mvc *sql, sql_rel *rel)
{
	if ((is_select(rel->op) || is_join(rel->op)) && !list_empty(rel->exps))
		rel->exps = rewrite_compare_exps(sql, rel->exps);
	return rel;
}

static sql_rel *
rewrite_remove_xp_project(mvc *sql, sql_rel *rel)
{
	(void)sql;
	if (rel->op == op_join && list_empty(rel->exps)) {
		sql_rel *r = rel->r;

		if (is_simple_project(r->op) && r->l) {
			sql_rel *rl = r->l;

			if (is_simple_project(rl->op) && !rl->l && list_length(rl->exps) == 1) {
				sql_exp *t = rl->exps->h->data;

				if (is_atom(t->type) && !exp_name(t)) { /* atom with out alias cannot be used later */
					rel = rel_project(sql->sa, rel->l, rel_projections(sql, rel->l, NULL, 1, 1));
					list_merge(rel->exps, r->exps, (fdup)NULL);
				}
			}
		}
	}
	return rel;
}

static sql_rel *
rewrite_remove_xp(mvc *sql, sql_rel *rel)
{
	(void)sql;
	if (rel->op == op_join && list_empty(rel->exps)) {
		sql_rel *r = rel->r;

		if (is_simple_project(r->op) && !r->l && list_length(r->exps) == 1) {
			sql_exp *t = r->exps->h->data;

			if (is_atom(t->type) && !exp_name(t)) /* atom with out alias cannot be used later */
				return rel->l;
		}
	}
	return rel;
}

/* rel visitor */
static sql_rel *
rewrite_fix_count(mvc *sql, sql_rel *rel)
{
	if (rel->op == op_left) {
		int changes = 0;
		sql_rel *r = rel->r;
		/* TODO create an exp iterator */
		list *rexps = rel_projections(sql, r, NULL, 1, 1), *exps;

		for(node *n = rexps->h; n; n=n->next) {
			sql_exp *e = n->data, *ne;

			if (exp_is_count(e, r)) {
				const char *rname = exp_relname(e), *name = exp_name(e);
				/* rewrite count in subquery */
				list *args, *targs;
				sql_subfunc *isnil = sql_bind_func(sql->sa, NULL, "isnull", exp_subtype(e), NULL, F_FUNC), *ifthen;

				changes = 1;
				ne = exp_unop(sql->sa, e, isnil);
				set_has_no_nil(ne);
				targs = sa_list(sql->sa);
				append(targs, sql_bind_localtype("bit"));
				append(targs, exp_subtype(e));
				append(targs, exp_subtype(e));
				ifthen = sql_bind_func_(sql->sa, NULL, "ifthenelse", targs, F_FUNC);
				args = sa_list(sql->sa);
				append(args, ne);
				append(args, exp_atom(sql->sa, atom_zero_value(sql->sa, exp_subtype(e))));
				append(args, e);
				e = exp_op(sql->sa, args, ifthen);
				exp_setname(sql->sa, e, rname, name);
				n->data = e;
			}
		}
		if (changes) { /* add project */
			exps = list_merge(rel_projections(sql, rel->l, NULL, 1, 1), rexps, (fdup)NULL);
			rel = rel_project(sql->sa, rel, exps);
		}
	}
	return rel;
}

static sql_rel *
rewrite_groupings(mvc *sql, sql_rel *rel)
{
	prop *found;

	if (rel->op == op_groupby) {
		/* ROLLUP, CUBE, GROUPING SETS cases */
		if ((found = find_prop(rel->p, PROP_GROUPINGS))) {
			list *sets = (list*) found->value;
			sql_rel *unions = NULL;

			for (node *n = sets->h ; n ; n = n->next) {
				sql_rel *nrel;
				list *l = (list*) n->data, *exps = sa_list(sql->sa), *pexps = sa_list(sql->sa);

				l = list_flaten(l);
				nrel = rel_groupby(sql, unions ? rel_dup(rel->l) : rel->l, l);

				for (node *m = rel->exps->h ; m ; m = m->next) {
					sql_exp *e = (sql_exp*) m->data, *ne = NULL;
					sql_subaggr *agr = (sql_subaggr*) e->f;

					if (e->type == e_aggr && !agr->aggr->s && !strcmp(agr->aggr->base.name, "grouping")) {
						/* replace grouping aggregate calls with constants */
						sql_subtype tpe = ((sql_arg*) agr->aggr->res->h->data)->type;
						list *groups = (list*) e->l;
						atom *a = atom_int(sql->sa, &tpe, 0);
#ifdef HAVE_HGE
						hge counter = (hge) list_length(groups) - 1;
#else
						lng counter = (lng) list_length(groups) - 1;
#endif
						assert(groups && list_length(groups) > 0);

						for (node *nn = groups->h ; nn ; nn = nn->next) {
							sql_exp *exp = (sql_exp*) nn->data;
							if (!exps_find_exp(l, exp)) {
								switch (ATOMstorage(a->data.vtype)) {
									case TYPE_bte:
										a->data.val.btval += (bte) (1 << counter);
										break;
									case TYPE_sht:
										a->data.val.shval += (sht) (1 << counter);
										break;
									case TYPE_int:
										a->data.val.ival += (int) (1 << counter);
										break;
									case TYPE_lng:
										a->data.val.lval += (lng) (1 << counter);
										break;
#ifdef HAVE_HGE
									case TYPE_hge:
										a->data.val.hval += (hge) (1 << counter);
										break;
#endif
									default:
										assert(0);
								}
							}
							counter--;
						}

						ne = exp_atom(sql->sa, a);
						exp_setname(sql->sa, ne, e->alias.rname, e->alias.name);
					} else if (e->type == e_column && !exps_find_exp(l, e) && !has_label(e)) { 
						/* do not include in the output of the group by, but add to the project as null */
						ne = exp_atom(sql->sa, atom_null_value(sql->sa, &(e->tpe)));
						exp_setname(sql->sa, ne, e->alias.rname, e->alias.name);
					} else {
						ne = exp_ref(sql->sa, e);
						append(exps, e);
					}
					append(pexps, ne);
				}
				nrel->exps = exps;
				nrel = rel_project(sql->sa, nrel, pexps);

				if (!unions)
					unions = nrel;
				else {
					unions = rel_setop(sql->sa, unions, nrel, op_union);
					unions->exps = rel_projections(sql, rel, NULL, 1, 1);
					set_processed(unions);
				}
				if (!unions)
					return unions;
			}
			return unions;
		} else {
			bool found_grouping = false;
			for (node *n = rel->exps->h ; n ; n = n->next) {
				sql_exp *e = (sql_exp*) n->data;
				sql_subaggr *agr = (sql_subaggr*) e->f;

				if (e->type == e_aggr && !agr->aggr->s && !strcmp(agr->aggr->base.name, "grouping")) {
					found_grouping = true;
					break;
				}
			}
			if (found_grouping) {
				/* replace grouping calls with constants of value 0 */
				sql_rel *nrel = rel_groupby(sql, rel->l, rel->r);
				list *exps = sa_list(sql->sa), *pexps = sa_list(sql->sa);
				sql_subtype *bt = sql_bind_localtype("bte");

				for (node *n = rel->exps->h ; n ; n = n->next) {
					sql_exp *e = (sql_exp*) n->data, *ne;
					sql_subaggr *agr = (sql_subaggr*) e->f;

					if (e->type == e_aggr && !agr->aggr->s && !strcmp(agr->aggr->base.name, "grouping")) {
						ne = exp_atom(sql->sa, atom_int(sql->sa, bt, 0));
						exp_setname(sql->sa, ne, e->alias.rname, e->alias.name);
					} else {
						ne = exp_ref(sql->sa, e);
						append(exps, e);
					}
					append(pexps, ne);
				}
				nrel->exps = exps;
				return rel_project(sql->sa, nrel, pexps);
			}
		}
	}
	return rel;
}

sql_rel *
rel_unnest(mvc *sql, sql_rel *rel)
{
	rel_reset_subquery(rel);
	rel = rel_exp_visitor(sql, rel, &rewrite_simplify_exp);
	rel = rel_visitor(sql, rel, &rewrite_simplify);
	rel = rel_visitor(sql, rel, &rewrite_aggregates);
	rel = rel_visitor(sql, rel, &rewrite_or_exp);
	rel = rel_exp_visitor(sql, rel, &rewrite_rank);
	rel = rel_exp_visitor(sql, rel, &rewrite_anyequal);
	rel = rel_exp_visitor(sql, rel, &rewrite_exists);
	rel = rel_exp_visitor(sql, rel, &rewrite_compare);
	rel = rel_exp_visitor(sql, rel, &rewrite_ifthenelse);	/* add isnull handling */
	rel = rel_exp_visitor(sql, rel, &rewrite_exp_rel);
	rel = rel_visitor(sql, rel, &rewrite_join2semi);	/* where possible convert anyequal functions into marks */
	rel = rel_visitor(sql, rel, &rewrite_compare_exp);	/* only allow for e_cmp in selects and  handling */
	rel = rel_visitor(sql, rel, &rewrite_remove_xp_project);	/* remove crossproducts with project ( project [ atom ] ) [ etc ] */
	rel = _rel_unnest(sql, rel);
	rel = rel_visitor(sql, rel, &rewrite_fix_count);	/* fix count inside a left join (adds a project (if (cnt IS null) then (0) else (cnt)) */
	rel = rel_visitor(sql, rel, &rewrite_remove_xp);	/* remove crossproducts with project [ atom ] */
	rel = rel_visitor(sql, rel, &rewrite_groupings);	/* transform group combinations into union of group relations */
	rel = rel_visitor(sql, rel, &rewrite_empty_project);
	return rel;
}
