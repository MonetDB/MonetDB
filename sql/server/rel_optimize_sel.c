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
#include "rel_planner.h"
#include "rel_exp.h"
#include "rel_select.h"
#include "rel_rewriter.h"

/* Split_select optimizer splits case statements in select expressions. This is a step needed for cse */
static void select_split_exps(mvc *sql, list *exps, sql_rel *rel);

static sql_exp *
select_split_exp(mvc *sql, sql_exp *e, sql_rel *rel)
{
	switch(e->type) {
	case e_column:
		return e;
	case e_convert:
		e->l = select_split_exp(sql, e->l, rel);
		return e;
	case e_aggr:
	case e_func:
		if (!is_analytic(e) && !exp_has_sideeffect(e)) {
			sql_subfunc *f = e->f;
			if (e->type == e_func && !f->func->s && is_caselike_func(f) /*is_ifthenelse_func(f)*/)
				return add_exp_too_project(sql, e, rel);
		}
		return e;
	case e_cmp:
		if (e->flag == cmp_filter) {
			select_split_exps(sql, e->l, rel);
			select_split_exps(sql, e->r, rel);
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			select_split_exps(sql, e->l, rel);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = select_split_exp(sql, e->l, rel);
			select_split_exps(sql, e->r, rel);
		} else {
			e->l = select_split_exp(sql, e->l, rel);
			e->r = select_split_exp(sql, e->r, rel);
			if (e->f)
				e->f = select_split_exp(sql, e->f, rel);
		}
		return e;
	case e_atom:
	case e_psm:
		return e;
	}
	return e;
}

static void
select_split_exps(mvc *sql, list *exps, sql_rel *rel)
{
	node *n;

	if (!exps)
		return;
	for(n=exps->h; n; n = n->next){
		sql_exp *e = n->data;

		e = select_split_exp(sql, e, rel);
		n->data = e;
	}
}

static sql_rel *
rel_split_select_(visitor *v, sql_rel *rel)
{
	if (!rel || !is_select(rel->op) || list_empty(rel->exps) || !rel->l || mvc_highwater(v->sql))
		return rel;

	bool funcs = false;

	/* are there functions */
	for (node *n = rel->exps->h; n && !funcs; n = n->next)
		funcs = exp_has_func(n->data);

	/* introduce extra project */
	if (funcs) {
		sql_rel *nrel = rel_project(v->sql->sa, rel->l,
			rel_projections(v->sql, rel->l, NULL, 1, 1));
		if (!nrel || !nrel->exps)
			return NULL;
		rel->l = nrel;
		/* recursively split all functions and add those to the projection list */
		select_split_exps(v->sql, rel->exps, nrel);
		return rel;
	}
	return rel;
}

static sql_rel *
rel_split_select(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_split_select_);
}

run_optimizer
bind_split_select(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_cycle == 0 && gp->opt_level == 1 && (flag & split_select)
		   && gp->cnt[op_select] ? rel_split_select : NULL;
}


/*
 * Remove a redundant join
 *
 * join (L, Distinct Project(join(L,P) [ p.key == l.lkey]) [p.key]) [ p.key == l.lkey]
 * =>
 * join(L, P) [p.key==l.lkey]
 */
static sql_rel *
rel_remove_redundant_join_(visitor *v, sql_rel *rel)
{
	if ((is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		sql_rel *l = rel->l, *r = rel->r, *b, *p = NULL, *j;

		if (is_basetable(l->op) && is_simple_project(r->op) && need_distinct(r)) {
			b = l;
			p = r;
			j = p->l;
		} else if (is_basetable(r->op) && is_simple_project(l->op) && need_distinct(l)) {
			b = r;
			p = l;
			j = p->l;
		}
		if (!p || !j || j->op != rel->op)
			return rel;
		/* j must have b->l (ie table) */
		sql_rel *jl = j->l, *jr = j->r;
		if ((is_basetable(jl->op) && jl->l == b->l) ||
		    (is_basetable(jr->op) && jr->l == b->l)) {
			int left = 0;
			if (is_basetable(jl->op) && jl->l == b->l)
				left = 1;
			if (!list_empty(p->exps)) {
				for (node *n=p->exps->h; n; n = n->next) { /* all exps of 'p' must be bound to the opposite side */
					sql_exp *e = n->data;

					if (!rel_rebind_exp(v->sql, left ? jr : jl, e))
						return rel;
				}
			}
			if (exp_match_list(j->exps, rel->exps)) {
				p->l = (left)?rel_dup(jr):rel_dup(jl);
				rel_destroy(j);
				set_nodistinct(p);
				v->changes++;
				return rel;
			}
		}
	}
	return rel;
}

static sql_rel *
rel_remove_redundant_join(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_bottomup(v, rel, &rel_remove_redundant_join_); /* this optimizer has to run before rel_first_level_optimizations */
}

run_optimizer
bind_remove_redundant_join(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_cycle == 0 && gp->opt_level == 1 && (gp->cnt[op_left] || gp->cnt[op_right]
		   || gp->cnt[op_full] || gp->cnt[op_join] || gp->cnt[op_semi] || gp->cnt[op_anti]) &&
		   (flag & remove_redundant_join) ? rel_remove_redundant_join : NULL;
}


static list *
exp_merge_range(visitor *v, sql_rel *rel, list *exps)
{
	node *n, *m;
	for (n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_exp *le = e->l;
		sql_exp *re = e->r;

		/* handle the conjuctive lists */
		if (e->type == e_cmp && e->flag == cmp_con && !is_anti(e)) {
			e->l = exp_merge_range(v, rel, e->l);
		/* only look for gt, gte, lte, lt */
		} else if (n->next &&
		    e->type == e_cmp && e->flag < cmp_equal && !e->f &&
		    re->card == CARD_ATOM && !is_anti(e)) {
			for (m=n->next; m; m = m->next) {
				sql_exp *f = m->data;
				sql_exp *lf = f->l;
				sql_exp *rf = f->r;
				int c_le = is_numeric_upcast(le), c_lf = is_numeric_upcast(lf);

				if (f->type == e_cmp && f->flag < cmp_equal && !f->f &&
				    rf->card == CARD_ATOM && !is_anti(f) &&
				    exp_match_exp(c_le?le->l:le, c_lf?lf->l:lf)) {
					sql_exp *ne;
					int swap = 0, lt = 0, gt = 0;
					sql_subtype super;
					/* for now only   c1 <[=] x <[=] c2 */

					swap = lt = (e->flag == cmp_lt || e->flag == cmp_lte);
					gt = !lt;

					if (gt &&
					   (f->flag == cmp_gt ||
					    f->flag == cmp_gte))
						continue;
					if (lt &&
					   (f->flag == cmp_lt ||
					    f->flag == cmp_lte))
						continue;

					cmp_supertype(&super, exp_subtype(le), exp_subtype(lf));
					if (!(rf = exp_check_type(v->sql, &super, rel, rf, type_equal)) ||
						!(le = exp_check_type(v->sql, &super, rel, le, type_equal)) ||
						!(re = exp_check_type(v->sql, &super, rel, re, type_equal))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = 0;
							continue;
						}
					if (!swap)
						ne = exp_compare2(v->sql->sa, le, re, rf, compare2range(e->flag, f->flag), 0);
					else
						ne = exp_compare2(v->sql->sa, le, rf, re, compare2range(f->flag, e->flag), 0);

					list_remove_data(exps, NULL, e);
					list_remove_data(exps, NULL, f);
					list_append(exps, ne);
					v->changes++;
					return exp_merge_range(v, rel, exps);
				}
			}
		} else if (n->next &&
			   e->type == e_cmp && e->flag < cmp_equal && !e->f &&
		    	   re->card > CARD_ATOM && !is_anti(e)) {
			for (m=n->next; m; m = m->next) {
				sql_exp *f = m->data;
				sql_exp *lf = f->l;
				sql_exp *rf = f->r;

				if (f->type == e_cmp && f->flag < cmp_equal && !f->f  &&
				    rf->card > CARD_ATOM && !is_anti(f)) {
					sql_exp *ne, *t;
					int swap = 0, lt = 0, gt = 0;
					comp_type ef = (comp_type) e->flag, ff = (comp_type) f->flag;
					int c_re = is_numeric_upcast(re), c_rf = is_numeric_upcast(rf);
					int c_le = is_numeric_upcast(le), c_lf = is_numeric_upcast(lf), c;
					sql_subtype super;

					/* both swapped ? */
					if (exp_match_exp(c_re?re->l:re, c_rf?rf->l:rf)) {
						t = re;
						re = le;
						le = t;
						c = c_re; c_re = c_le; c_le = c;
						ef = swap_compare(ef);
						t = rf;
						rf = lf;
						lf = t;
						c = c_rf; c_rf = c_lf; c_lf = c;
						ff = swap_compare(ff);
					}

					/* is left swapped ? */
					if (exp_match_exp(c_re?re->l:re, c_lf?lf->l:lf)) {
						t = re;
						re = le;
						le = t;
						c = c_re; c_re = c_le; c_le = c;
						ef = swap_compare(ef);
					}

					/* is right swapped ? */
					if (exp_match_exp(c_le?le->l:le, c_rf?rf->l:rf)) {
						t = rf;
						rf = lf;
						lf = t;
						c = c_rf; c_rf = c_lf; c_lf = c;
						ff = swap_compare(ff);
					}

					if (!exp_match_exp(c_le?le->l:le, c_lf?lf->l:lf))
						continue;

					/* for now only   c1 <[=] x <[=] c2 */
					swap = lt = (ef == cmp_lt || ef == cmp_lte);
					gt = !lt;

					if (gt && (ff == cmp_gt || ff == cmp_gte))
						continue;
					if (lt && (ff == cmp_lt || ff == cmp_lte))
						continue;

					cmp_supertype(&super, exp_subtype(le), exp_subtype(lf));
					if (!(rf = exp_check_type(v->sql, &super, rel, rf, type_equal)) ||
						!(le = exp_check_type(v->sql, &super, rel, le, type_equal)) ||
						!(re = exp_check_type(v->sql, &super, rel, re, type_equal))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = 0;
							continue;
						}
					if (!swap)
						ne = exp_compare2(v->sql->sa, le, re, rf, compare2range(ef, ff), 0);
					else
						ne = exp_compare2(v->sql->sa, le, rf, re, compare2range(ff, ef), 0);

					list_remove_data(exps, NULL, e);
					list_remove_data(exps, NULL, f);
					list_append(exps, ne);
					v->changes++;
					return exp_merge_range(v, rel, exps);
				}
			}
		}
	}
	return exps;
}

#define TRIVIAL_NOT_EQUAL_CMP(e) \
	((e)->type == e_cmp && (e)->flag == cmp_notequal && !is_anti((e)) && !is_semantics((e)) && ((sql_exp*)(e)->l)->card != CARD_ATOM && ((sql_exp*)(e)->r)->card == CARD_ATOM)

static list *
merge_notequal(mvc *sql, list *exps, int *changes)
{
	list *inequality_groups = NULL, *nexps = NULL;
	int needed = 0;

	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (TRIVIAL_NOT_EQUAL_CMP(e)) {
			bool appended = false;

			if (inequality_groups) {
				for (node *m = inequality_groups->h; m && !appended; m = m->next) {
					list *next = m->data;
					sql_exp *first = (sql_exp*) next->h->data;

					if (exp_match(first->l, e->l)) {
						list_append(next, e);
						appended = true;
					}
				}
			}
			if (!appended) {
				if (!inequality_groups)
					inequality_groups = new_exp_list(sql->sa);
				list_append(inequality_groups, list_append(new_exp_list(sql->sa), e));
			}
		}
	}

	if (inequality_groups) { /* if one list of inequalities has more than one entry, then the re-write is needed */
		for (node *n = inequality_groups->h; n; n = n->next) {
			list *next = n->data;

			if (list_length(next) > 1)
				needed = 1;
		}
	}

	if (needed) {
		nexps = new_exp_list(sql->sa);
		for (node *n = inequality_groups->h; n; n = n->next) {
			list *next = n->data;
			sql_exp *first = (sql_exp*) next->h->data;

			if (list_length(next) > 1) {
				list *notin = new_exp_list(sql->sa);

				for (node *m = next->h; m; m = m->next) {
					sql_exp *e = m->data;
					list_append(notin, e->r);
				}
				list_append(nexps, exp_in(sql->sa, first->l, notin, cmp_notin));
			} else {
				list_append(nexps, first);
			}
		}

		for (node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (!TRIVIAL_NOT_EQUAL_CMP(e))
				list_append(nexps, e);
		}
		(*changes)++;
	} else {
		nexps = exps;
	}

	return nexps;
}

static int
exps_cse_dis( visitor *v, list *oexps, sql_exp *de)
{
	node *n, *m, *o;
	list *dis = de->l;

	if (list_length(dis) <= 1) {
		append(oexps, de);
		return 0;
	}
	for (n = dis->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (e->type == e_cmp && e->flag == cmp_con)
			e->l = merge_notequal(v->sql, e->l, &v->changes); /* x <> 1 and x <> 2 => x not in (1, 2)*/
	}

	int matches = 0, lpos = 0, rc = 1, rpos = 0, changes = 0;
	int *matchedpos = SA_ZNEW_ARRAY(v->sql->ta, int, list_length(dis));
	sql_exp *fe = dis->h->data;
	if (fe->type != e_cmp || fe->flag != cmp_con) {
		append(oexps, de);
		return 0;
	}
	list *ll = fe->l;
	for (m = ll->h; m; ) {
		sql_exp *le = m->data;
		for (n = dis->h->next, matches = 0, rc = 1; n; n = n->next, rc++) {
			sql_exp *me = n->data;

			if (me->type != e_cmp || me->flag != cmp_con) {
			   append(oexps, de);
			   return 0;
			}
			list *rl = me->l;
			for (o = rl->h, rpos = 0; o; o = o->next, rpos++) {
				sql_exp *re = o->data;
				if (exp_match_exp(le,re)) {
					matchedpos[rc] = rpos;
					matches++;
					break;
				}
			}
		}
		if ((matches+1) == rc) {
			append(oexps, le);
			matchedpos[0] = lpos;
			int pos = 0;
			for (node *n = dis->h; n; n = n->next, pos++) {
				sql_exp *e = n->data;
				list *l = e->l;
				node *r = list_fetch_node(l, matchedpos[pos]);
				list_remove_node(l, NULL, r);
				changes++;
				m = ll->h;
				lpos = 0;
			}
		}
		m = m->next;
	   	lpos++;
	}
	//if (changes) {
		/* todo check for empty lists */
	append(oexps, de);
	return changes;
}

static inline int
exp_col_key(sql_exp *e)
{
	return e->nid ? e->nid : e->alias.label;
}

static inline int
exp_cmp_eq_unique_id(sql_exp *e)
{
	return exp_col_key(e->l);
}

static inline int
exp_multi_col_key(list *l)
{
	int k = exp_col_key(l->h->data);
	for (node *n = l->h->next; n; n = n->next) {
		k <<= 4;
		k ^= exp_col_key(n->data);
	}
	return k;
}

typedef struct exp_eq_col_values {
	/* we need ->first in order to remove it from the list of cmp_eq exps
	 * in case that we find another occurrence (with a different value)
	 */
	sql_exp* first;
	sql_exp* col; /* column */
	list *vs;     /* list of values */
} eq_cv;

typedef struct exp_eq_multi_col_values {
	/* we need ->first in order to remove it from the list of multi col
	 * cmp_eq exps in case that we find another occurrence (with different values)
	 */
	list *first;
	list *cols;  /* list of col exps */
	list *lvs;   /* list of lists of values */
} eq_mcv;

static bool
detect_col_cmp_eqs(mvc *sql, list *eqs, sql_hash *eqh)
{
	bool col_multivalue_cmp_eq = false;
	for (node *n = eqs->h; n; n = n->next ) {
		sql_exp *e = n->data;
		sql_exp *le = e->l, *re = e->r;

		/* find the le in the hash and append the re in the hash value (ea->list) */
		bool found = false;

		int key = eqh->key(le);
		sql_hash_e *he = eqh->buckets[key&(eqh->size-1)];

		for (;he && !found; he = he->chain) {
			eq_cv *cv = he->value;
			if (!exp_equal(le, cv->col)) {
				cv->vs = append(cv->vs, re);
				found = col_multivalue_cmp_eq = true;
				/* remove this and the previous (->first) occurrence (if exists) from eqs */
				if (cv->first) {
					list_remove_data(eqs, NULL, cv->first);
					cv->first = NULL;
				}
				list_remove_node(eqs, NULL, n);
			}
		}

		if (!found) {
			eq_cv *cv = SA_NEW(sql->sa, eq_cv);
			cv->first = e;
			cv->vs = sa_list(sql->sa);
			cv->vs = append(cv->vs, re);
			cv->col = le;

			hash_add(eqh, key, cv);
		}
	}
	return col_multivalue_cmp_eq;
}

static bool
detect_multicol_cmp_eqs(mvc *sql, list *mce_ands, sql_hash *meqh)
{
	/* we get as input a list of AND associated expressions (hence the entries are lists themselves)
	 * we need to detect cmp_eq-only AND-associated expressions with the same columns so we can
	 * group together their values
	 * e.g. [[n = 1, m = 10], [m = 20, k = 100, l = 3000], [m = 20, n = 2]] has
	 *      - (m,k,l) group with a single value (20, 100, 3000)
	 *      - (n,k) group with two values (1, 10) and (2, 20)
	 * at the end we return true only if we have at least a group of columns with more than a single value
	 * e.g. in this example (n,k)
	 */
	bool multi_multivalue_cmp_eq = false;
	for (node *n = mce_ands->h; n; n = n->next) {
		list *l = n->data;

		/* sort the list of the cmp_eq expressions based on the col exp
		 * NOTE: from now on we only work with the sorted list, sl */
		list *sl = list_sort(l, (fkeyvalue)&exp_cmp_eq_unique_id, NULL);
		list_append_before(mce_ands, n, sl);
		list_remove_node(mce_ands, NULL, n);

		/* find the eq exp in the hash and append the values */
		bool found = false;

		int key = meqh->key(sl);
		sql_hash_e *he = meqh->buckets[key&(meqh->size-1)];

		for (;he && !found; he = he->chain) {
			/* compare the values of the hash_entry with the cols under cmp_eq from the list */
			bool same_cols = true;
			eq_mcv *mcv = he->value;
			for (node *m = sl->h, *k = mcv->cols->h; m && k && same_cols; m = m->next, k = k->next) {
				sql_exp *col_exp = ((sql_exp*)m->data)->l;
				if (exp_equal(col_exp, k->data))
					same_cols = false;
			}
			if (same_cols) {
				/* we found the same multi cmp_eq exp in mce_ands list multiple times! */
				found = multi_multivalue_cmp_eq = true;
				/* gather all the values of the list and add them to the hash entry */
				list *atms = sa_list(sql->sa);
				for (node *m = sl->h; m; m = m->next)
					atms = append(atms, ((sql_exp*)m->data)->r);
				mcv->lvs = append(mcv->lvs, atms);
				/* remove this and the previous occurrence (which means that's the first time
				 * that we found the *same* multi cmp_eq exp)
				 */
				if (mcv->first) {
					list_remove_data(mce_ands, NULL, mcv->first);
					mcv->first = NULL;
				}
				list_remove_data(mce_ands, NULL, sl);
			}
		}

		if (!found) {
			eq_mcv *mcv = SA_NEW(sql->sa, eq_mcv);
			mcv->first = sl;
			mcv->cols = sa_list(sql->sa);
			for (node *m = sl->h; m; m = m->next)
				mcv->cols = append(mcv->cols, ((sql_exp*)m->data)->l);
			/* for the list of values (atoms) create a list and append it to the lvs list */
			list *atms = sa_list(sql->sa);
			for (node *m = sl->h; m; m = m->next)
				atms = append(atms, ((sql_exp*)m->data)->r);
			mcv->lvs = sa_list(sql->sa);
			mcv->lvs = append(mcv->lvs, atms);

			hash_add(meqh, key, mcv);
		}
	}
	return multi_multivalue_cmp_eq;
}

static void
exp_or_chain_groups(list *exps, list **gen_ands, list **mce_ands, list **eqs, list **noneq)
{
	/* identify the groups
	 * 1. gen_ands: lists of generic expressions (their inner association is AND)
	 * 2. mce_ands: lists of multi_colum cmp_eq ONLY expressions (same^^^)
	 * 3. eqs: equality expressions
	 * 4. neq: non equality col expressions
	 *
	 * return true if there is an exp with more than one cmp_eq
	 */
    bool eq_only = true;
    for (node *n = exps->h; n && eq_only; n = n->next) {
        sql_exp *e = n->data;
        sql_exp *le = e->l, *re = e->r;
        eq_only &= (e->type == e_cmp && e->flag == cmp_equal &&
                    le->card != CARD_ATOM && is_column(le->type) &&
                    re->card == CARD_ATOM && !is_semantics(e));
    }

	if (list_length(exps) > 1) {
		if (eq_only)
			*mce_ands = append(*mce_ands, exps);
		else
			*gen_ands = append(*gen_ands, exps);
	} else if (list_length(exps) == 1) {
		sql_exp *se = exps->h->data;

		if (eq_only) {
			*eqs = append(*eqs, se);
		} else {
			*noneq = append(*noneq, se);
		}
	}
}

static list *
generate_single_col_cmp_in(mvc *sql, sql_hash *eqh)
{
	/* from single col cmp_eq with multiple atoms in the hash generate
	 * "e_col in (val0, val1, ...)" (see detect_col_cmp_eqs())
	 */
	list *ins = new_exp_list(sql->sa);
	for (int i = 0; i < eqh->size; i++) {
		sql_hash_e *he = eqh->buckets[i];

		while (he) {
			eq_cv *cv = he->value;
			/* NOTE: cmp_eq expressions with a single entry are still in eqs */
			if (list_length(cv->vs) > 1)
				ins = append(ins, exp_in(sql->sa, cv->col, cv->vs, cmp_in));
			he = he->chain;
		}
	}
	return ins;
}

static list *
generate_multi_col_cmp_in(mvc *sql, sql_hash *meqh)
{
	/* from multivalue cmp_eq with multiple lists of atoms in the hash generate
	 * "(col1, col2, ...) in [(val10, val20, ...), (val11, val21, ...), ... ]"
	 * (see detect_multicol_cmp_eqs())
	 */
	list *ins = new_exp_list(sql->sa);
	for (int i = 0; i < meqh->size; i++) {
		sql_hash_e *he = meqh->buckets[i];
		while (he) {
			eq_mcv *mcv = he->value;
			/* NOTE: multivalue cmp_eq expressions with a single entry are still in mce_ands */
			if (list_length(mcv->lvs) > 1) {
				sql_exp *mc = exp_label(sql->sa, exp_values(sql->sa, mcv->cols), ++sql->label);
				for (node *a = mcv->lvs->h; a; a = a->next)
					a->data = exp_values(sql->sa, a->data);
				ins = append(ins, exp_in(sql->sa, mc, mcv->lvs, cmp_in));
			}
			he = he->chain;
		}
	}
	return ins;
}

static list *
merge_ors(mvc *sql, list *exps, int *changes)
{
	sql_hash *eqh = NULL, *meqh = NULL;
	list *eqs = NULL, *neq = NULL, *gen_ands = NULL, *mce_ands = NULL, *ins = NULL, *mins = NULL;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_dis && !is_anti(e)) {
			list *el = e->l;

			/* NOTE: gen_ands and mce_ands are both a list of lists since the AND association
			 *       between expressions is expressed with a list
			 *       e.g. [[e1, e2], [e3, e4, e5]] semantically translates
			 *         to [(e1 AND e2), (e3 AND  e4 AND e5)]
			 *       those (internal) AND list can be then used to
			 *       reconstructed an OR tree [[e1, e2], [e3, e4, e5]] =>
			 *       (([e1, e2] OR [e3, e4, e5]) OR <whatever-else> )
			 *       gen_ands includes general expressions associated with AND
			 *       mce_ands includes only cmp_eq expressions associated with AND
			 */
			gen_ands = new_exp_list(sql->sa);
			mce_ands = new_exp_list(sql->sa);
			eqs = new_exp_list(sql->sa);
			neq = new_exp_list(sql->sa);

			for(node *n = el->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (e->type == e_cmp && e->flag == cmp_con && !is_anti(e)) {
					exp_or_chain_groups(e->l, &gen_ands, &mce_ands, &eqs, &neq);
				} else if (e->type == e_cmp && e->flag == cmp_dis && !is_anti(e)) {
					node *p = el->h;
					for( ; p->next != n; p = p->next)
						;
					list_remove_node(el, NULL, n);
					list_merge(el, e->l, NULL);
					n = p;
				} else {
					list *l = append(sa_list(sql->sa), e);
					exp_or_chain_groups(l, &gen_ands, &mce_ands, &eqs, &neq);
				}
			}

			/* detect col cmp_eq exps with multiple values */
			bool col_multival = false;
			if (list_length(eqs) > 1) {
				eqh = hash_new(sql->sa, 32, (fkeyvalue)&exp_col_key);
				col_multival = detect_col_cmp_eqs(sql, eqs, eqh);
			}

			/* detect mutli-col cmp_eq exps with multiple (lists of) values */
			bool multicol_multival = false;
			if (list_length(mce_ands) > 1) {
				meqh = hash_new(sql->sa, 32, (fkeyvalue)&exp_multi_col_key);
				multicol_multival = detect_multicol_cmp_eqs(sql, mce_ands, meqh);
			}

			if (!col_multival && !multicol_multival)
				continue;

			if (col_multival)
				ins = generate_single_col_cmp_in(sql, eqh);

			if (multicol_multival)
				mins = generate_multi_col_cmp_in(sql, meqh);

			/* create the new OR (disjunctive) expression */
			list *new = sa_list(sql->sa);

			if (ins)
				list_merge(new, ins, NULL);
			if (mins)
				list_merge(new, mins, NULL);

			if (list_length(eqs))
				list_merge(new, eqs, NULL);
			if (list_length(neq))
				list_merge(new, neq, NULL);

			if (list_length(mce_ands)) {
				for (node *i = mce_ands->h; i; i = i->next, (*changes)++) {
					list *cl = append(sa_list(sql->sa), exp_conjunctive(sql->sa, i->data));
					list_merge(new, cl, NULL);
				}
			}
			if (list_length(gen_ands)) {
				for (node *a = gen_ands->h; a; a = a->next, (*changes)++) {
					list *gl = append(sa_list(sql->sa), exp_conjunctive(sql->sa, a->data));
					list_merge(new, gl, NULL);
				}
			}

			list_remove_node(exps, NULL, n);
			exps = append(exps, exp_disjunctive(sql->sa, new));
		}
	}
	return exps;
}

int
is_numeric_upcast(sql_exp *e)
{
	if (is_convert(e->type)) {
		sql_subtype *f = exp_fromtype(e);
		sql_subtype *t = exp_totype(e);

		if (f->type->eclass == t->type->eclass && EC_COMPUTE(f->type->eclass)) {
			if (f->type->localtype < t->type->localtype)
				return 1;
		}
	}
	return 0;
}

/* optimize (a = b) or (a is null and b is null) -> a = b with null semantics */
static sql_exp *
try_rewrite_equal_or_is_null(mvc *sql, sql_rel *rel, sql_exp *or, sql_exp *cmp, list *l2)
{
	bool valid = true, first_is_null_found = false, second_is_null_found = false;
	sql_exp *first = cmp->l, *second = cmp->r;

	if (is_compare(cmp->type) && !is_anti(cmp) && !cmp->f && cmp->flag == cmp_equal) {
		int fupcast = is_numeric_upcast(first), supcast = is_numeric_upcast(second);
		for(node *n = l2->h ; n && valid; n = n->next) {
			sql_exp *e = n->data, *l = e->l, *r = e->r;

			if (is_compare(e->type) && e->flag == cmp_equal && !e->f &&
					!is_anti(e) && is_semantics(e)) {
				int lupcast = is_numeric_upcast(l);
				int rupcast = is_numeric_upcast(r);
				sql_exp *rr = rupcast ? r->l : r;

				if (rr->type == e_atom && rr->l && atom_null(rr->l)) {
					if (exp_match_exp(fupcast?first->l:first, lupcast?l->l:l))
						first_is_null_found = true;
					else if (exp_match_exp(supcast?second->l:second, lupcast?l->l:l))
						second_is_null_found = true;
					else
						valid = false;
				} else {
					valid = false;
				}
			} else {
				valid = false;
			}
		}
		if (valid && first_is_null_found && second_is_null_found) {
			sql_subtype super;

			cmp_supertype(&super, exp_subtype(first), exp_subtype(second)); /* first and second must have the same type */
			if (!(first = exp_check_type(sql, &super, rel, first, type_equal)) ||
					!(second = exp_check_type(sql, &super, rel, second, type_equal))) {
				sql->session->status = 0;
				sql->errstr[0] = 0;
				return or;
			}
			sql_exp *res = exp_compare(sql->sa, first, second, cmp->flag);
			set_semantics(res);
			if (exp_name(or))
				exp_prop_alias(sql->sa, res, or);
			return res;
		}
	}
	return or;
}

static list *
merge_cmp_or_null(mvc *sql, sql_rel *rel, list *exps, int *changes)
{
	for (node *n = exps->h; n ; n = n->next) {
		sql_exp *e = n->data;

		if (is_compare(e->type) && e->flag == cmp_dis && !is_anti(e)) {
			list *l = e->l;
			if (list_length(l) == 2) {
				sql_exp *h = l->h->data;
				sql_exp *t = l->h->next->data;
				if(is_compare(h->type) && h->flag == cmp_equal &&
				   is_compare(t->type) && t->flag == cmp_con) {
					sql_exp *ne = try_rewrite_equal_or_is_null(sql, rel, e, h, t->l);
					if (ne != e) {
						(*changes)++;
						n->data = ne;
					}
				} else if(is_compare(t->type) && t->flag == cmp_equal &&
				   is_compare(h->type) && h->flag == cmp_con) {
					sql_exp *ne = try_rewrite_equal_or_is_null(sql, rel, e, t, h->l);
					if (ne != e) {
						(*changes)++;
						n->data = ne;
					}
				}
			}
		}
	}
	return exps;
}

static list *
cleanup_equal_exps(mvc *sql, sql_rel *rel, list *exps, int *changes)
{
	if (list_length(exps) <= 1)
		return exps;
	if (is_join(rel->op)) {
		for(node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (e->type == e_cmp && !e->f && e->flag <= cmp_notequal &&
				!rel_find_exp(rel->l, e->l) && !rel_find_exp(rel->r, e->r) &&
				!find_prop(e->p, PROP_HASHCOL) && !find_prop(e->p, PROP_JOINIDX)) {
				exp_swap(e);
			}
		}
	}
	bool needed = false;
	for(node *n = exps->h; !needed && n; n = n->next) {
		for (node *m = n->next; !needed && m; m = m->next) {
			if (exp_match_exp_semantics(n->data, m->data, true))
				needed = true;
		}
	}
	if (needed) {
		list *nexps = sa_list(sql->sa);

		for(node *n = exps->h; n; n = n->next) {
			bool done = false;
			for (node *m = exps->h; m && !done; m = m->next) {
				if (n != m && exp_match_exp_semantics(n->data, m->data, false)) {
					sql_exp *e1 = n->data, *e2 = m->data;
					if ((is_any(e1) || is_semantics(e1)) || (!is_any(e2) && !is_semantics(e2))) {
						append(nexps, e1);
						if ((!is_any(e2) && !is_semantics(e2)) && is_left(rel->op) && list_length(rel->attr) == 1) {
							/* nil is false */
							sql_exp *m = rel->attr->h->data;
							if (exp_is_atom(m))
								set_no_nil(m);
						}
					}
					done = true;
				}
			}
			if (!done)
				append(nexps, n->data);
		}
		return nexps;
	}
	(void)changes;
	return exps;
}

static inline sql_rel *
rel_select_cse(visitor *v, sql_rel *rel)
{
	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && rel->exps) /* cleanup equal expressions */
		rel->exps = cleanup_equal_exps(v->sql, rel, rel->exps, &v->changes); /* (a = b) and (a += b) */

	if (is_select(rel->op) && rel->exps)
		rel->exps = merge_ors(v->sql, rel->exps, &v->changes);

	if (is_select(rel->op) && rel->exps)
		rel->exps = merge_notequal(v->sql, rel->exps, &v->changes); /* x <> 1 and x <> 2 => x not in (1, 2)*/

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && rel->exps)
		rel->exps = merge_cmp_or_null(v->sql, rel, rel->exps, &v->changes); /* (a = b) or (a is null and b is null) -> a = b with null semantics */

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && rel->exps) {
		node *n;
		list *nexps;
		int needed = 0;

		for (n=rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_dis && !is_anti(e))
				needed = 1;
		}
		if (!needed)
			return rel;
		nexps = new_exp_list(v->sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_dis && !is_anti(e)) {
				/* split the common expressions */
				v->changes += exps_cse_dis(v, nexps, e);
			} else {
				append(nexps, e);
			}
		}
		rel->exps = nexps;
	}
	return rel;
}

static sql_exp *
exp_or2in( mvc *sql, sql_exp *le, sql_exp *re)
{
	if (is_anti(le) || is_anti(re) || is_symmetric(re))
		return NULL;
	if (le->flag == cmp_equal && re->flag == cmp_equal) {
		list *exps = new_exp_list(sql->sa);

		append(exps, le->r);
		append(exps, re->r);
		return exp_in(sql->sa, le->l, exps, cmp_in);
	} else if (le->flag == cmp_equal && re->flag == cmp_in){
		list *exps = new_exp_list(sql->sa);

		append(exps, le->r);
		list_merge(exps, re->r, NULL);
		return exp_in(sql->sa, le->l, exps, cmp_in);
	} else if (le->flag == cmp_in && re->flag == cmp_equal){
		list *exps = new_exp_list(sql->sa);

		list_merge(exps, le->r, NULL);
		append(exps, re->r);
		return exp_in(sql->sa, le->l, exps, cmp_in);
	} else if (le->flag == cmp_in && re->flag == cmp_in){
		list *exps = new_exp_list(sql->sa);

		list_merge(exps, le->r, NULL);
		list_merge(exps, re->r, NULL);
		return exp_in(sql->sa, le->l, exps, cmp_in);
	}
	return NULL;
}

/* merge related sub expressions
 *
 * ie   (x = a and y > 1 and y < 5) or
 *      (x = c and y > 1 and y < 10) or
 *      (x = e and y > 1 and y < 20)
 * ->
 *     ((x = a and y > 1 and y < 5) or
 *      (x = c and y > 1 and y < 10) or
 *      (x = e and y > 1 and y < 20)) and
 *     	 x in (a,c,e) and
 *     	 y > 1 and y < 20
 *
 * for single expression or's we can do better
 *		x in (a, b, c) or x in (d, e, f)
 *		->
 *		x in (a, b, c, d, e, f)
 * */
static inline sql_rel *
rel_merge_select_rse(visitor *v, sql_rel *rel)
{
	/* only execute once per select */
	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && rel->exps && !is_rel_merge_select_rse_used(rel->used)) {
		list *nexps = new_exp_list(v->sql->sa);

		for (node *n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			bool changed = false;
			if (e->type == e_cmp && e->flag == cmp_dis && !is_anti(e) && !is_semantics(e)) {
				list *exps = e->l;
				for(node *n = exps->h; n; n = n->next) {
					sql_exp *e1 = n->data;

					bool merged = false;
					if (!is_semantics(e1) && !is_anti(e1)) {
						/* no merges, ie don't change e, all merged into one new expression, some merged, rewrite e into new cmp_dis */
						node *p = n;
						for (node *m = n->next; m; p = m, m = m->next) {
							sql_exp *e2 = m->data;
							sql_exp *fnd = NULL;
							if (!is_semantics(e2) && !is_anti(e2) && exps_match_col_exps(e1, e2) && (fnd = exp_or2in(v->sql, e1, e2)) != NULL) {
								changed = merged = true;
								e1 = fnd;
								/* remove e2 from list */
								p->next = m->next;
								m = p;
								exps->cnt--;
							}
						}
						if (merged) /* replace e1 */
							n->data = e1;
					}
				}
				if (changed)
					v->changes++;
				if (list_length(exps) == 1)
					append(nexps, exps->h->data);
				else
					append(nexps, e);
			} else {
				append(nexps, e);
			}
		}
		rel->exps = nexps;
		rel->used |= rel_merge_select_rse_used;
	}
	return rel;
}

/* pack optimizers into a single function call to avoid iterations in the AST */
static sql_rel *
rel_optimize_select_and_joins_bottomup_(visitor *v, sql_rel *rel)
{
	if (!rel || (!is_join(rel->op) && !is_semi(rel->op) && !is_select(rel->op)) || list_empty(rel->exps))
		return rel;
	uint8_t cycle = *(uint8_t*) v->data;

	rel->exps = exp_merge_range(v, rel, rel->exps);
	rel = rel_select_cse(v, rel);
	if (cycle == 1)
		rel = rel_merge_select_rse(v, rel);
	rel = rewrite_simplify(v, cycle, v->value_based_opt, rel);
	return rel;
}

static sql_rel *
rel_optimize_select_and_joins_bottomup(visitor *v, global_props *gp, sql_rel *rel)
{
	v->data = &gp->opt_cycle;
	rel = rel_visitor_bottomup(v, rel, &rel_optimize_select_and_joins_bottomup_);
	v->data = gp;
	return rel;
}

run_optimizer
bind_optimize_select_and_joins_bottomup(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right] ||
		   gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti] ||
		   gp->cnt[op_select]) && (flag & optimize_select_and_joins_bottomup) ? rel_optimize_select_and_joins_bottomup : NULL;
}


static inline sql_rel *
rel_push_join_exps_down(visitor *v, sql_rel *rel)
{
	/* push select exps part of join expressions down */
	/* TODO CHECK WHY not semi enabled */
	if ((is_innerjoin(rel->op) || is_left(rel->op) || is_right(rel->op) /*|| is_semi(rel->op)*/) && !list_empty(rel->exps)) {
		int left = is_innerjoin(rel->op) || is_right(rel->op) || rel->op == op_semi;
		int right = is_innerjoin(rel->op) || is_left(rel->op) || is_semi(rel->op);
		sql_rel *jl = rel->l, *ojl = jl, *jr = rel->r, *ojr = jr;

		set_processed(jl);
		set_processed(jr);
		for (node *n = rel->exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (left && rel_rebind_exp(v->sql, jl, e) && !is_any(e)) { /* select expressions on left */
				if (!is_select(jl->op) || rel_is_ref(jl))
					rel->l = jl = rel_select(v->sql->sa, jl, NULL);
				rel_select_add_exp(v->sql->sa, jl, e);
				list_remove_node(rel->exps, NULL, n);
				v->changes++;
			} else if (right && rel_rebind_exp(v->sql, jr, e) && !is_any(e)) { /* select expressions on right */
				if (!is_select(jr->op) || rel_is_ref(jr))
					rel->r = jr = rel_select(v->sql->sa, jr, NULL);
				rel_select_add_exp(v->sql->sa, jr, e);
				list_remove_node(rel->exps, NULL, n);
				v->changes++;
			}
			n = next;
		}
		if (ojl != jl)
			set_processed(jl);
		if (ojr != jr)
			set_processed(jr);
	}
	return rel;
}

static inline bool
is_non_trivial_select_applied_to_outer_join(sql_rel *rel)
{
	return is_select(rel->op) && rel->exps && is_outerjoin(((sql_rel*) rel->l)->op);
}

extern list *list_append_before(list *l, node *n, void *data);

static void
replace_column_references_with_nulls_2(mvc *sql, sql_rel *inner_join_side, sql_exp* e);

static void
replace_column_references_with_nulls_1(mvc *sql, sql_rel *inner_join_side, list* exps) {
	if (list_empty(exps))
		return;
	for(node* n = exps->h; n; n=n->next) {
		sql_exp* e = n->data;
		replace_column_references_with_nulls_2(sql, inner_join_side, e);
	}
}

static void
replace_column_references_with_nulls_2(mvc *sql, sql_rel *inner_join_side, sql_exp* e) {
	if (e == NULL) {
		return;
	}

	switch (e->type) {
	case e_column:
		if (rel_find_exp_and_corresponding_rel(inner_join_side, e, true, NULL, NULL)) {
			e->type = e_atom;
			e->l = atom_general(sql->sa, &e->tpe, NULL, 0);
			e->r = e->f = NULL;
		}
		break;
	case e_cmp:
		switch (e->flag) {
		case cmp_gt:
		case cmp_gte:
		case cmp_lte:
		case cmp_lt:
		case cmp_equal:
		case cmp_notequal:
		{
			sql_exp* l = e->l;
			sql_exp* r = e->r;
			sql_exp* f = e->f;

			replace_column_references_with_nulls_2(sql, inner_join_side, l);
			replace_column_references_with_nulls_2(sql, inner_join_side, r);
			replace_column_references_with_nulls_2(sql, inner_join_side, f);
			break;
		}
		case cmp_filter:
		{
			list* l = e->l;
			list* r = e->r;
			replace_column_references_with_nulls_1(sql, inner_join_side, l);
			replace_column_references_with_nulls_1(sql, inner_join_side, r);
			break;
		}
		case cmp_con:
		case cmp_dis:
		{
			list* l = e->l;
			replace_column_references_with_nulls_1(sql, inner_join_side, l);
			break;
		}
		case cmp_in:
		case cmp_notin:
		{
			sql_exp* l = e->l;
			list* r = e->r;
			replace_column_references_with_nulls_2(sql, inner_join_side, l);
			replace_column_references_with_nulls_1(sql, inner_join_side, r);
			break;
		}
		default:
			break;
		}
		break;
	case e_func:
	{
		list* l = e->l;
		replace_column_references_with_nulls_1(sql, inner_join_side, l);
		break;
	}
	case e_convert:
	{
		sql_exp* l = e->l;
		replace_column_references_with_nulls_2(sql, inner_join_side, l);
		break;
	}
	default:
		break;
	}
}

static sql_rel *
out2inner(visitor *v, sql_rel* sel, sql_rel* join, sql_rel* inner_join_side, operator_type new_type) {

	/* handle inner_join relations with a simple select */
	if (is_select(inner_join_side->op) && inner_join_side->l)
		inner_join_side = inner_join_side->l;

	list* select_predicates = exps_copy(v->sql, sel->exps);

	for(node* n = select_predicates->h; n; n=n->next) {
		sql_exp* e = n->data;
		replace_column_references_with_nulls_2(v->sql, inner_join_side, e);

		if (exp_is_false(e)) {
			join->op = new_type;
			v->changes++;
			break;
		}
	}

	return sel;
}

static inline sql_rel *
rel_out2inner(visitor *v, sql_rel *rel) {

	if (!is_non_trivial_select_applied_to_outer_join(rel)) {
		// Nothing to do here.
		return rel;
	}

	sql_rel* join = (sql_rel*) rel->l;

	if (rel_is_ref(join)) {
		/* Do not alter a multi-referenced join relation.
			* This is problematic (e.g. in the case of the plan of a merge statement)
			* basically because there are no guarantees on the other container relations.
			* In particular there is no guarantee that the other referencing relations are
			* select relations with null-rejacting predicates on the inner join side.
			*/
		return rel;
	}

	sql_rel* inner_join_side;
	if (is_left(join->op)) {
		inner_join_side = join->r;
		return out2inner(v, rel, join, inner_join_side, op_join);
	}
	else if (is_right(join->op)) {
		inner_join_side = join->l;
		return out2inner(v, rel, join, inner_join_side, op_join);
	}
	else /*full outer join*/ {
		// First check if left side can degenerate from full outer join to just right outer join.
		inner_join_side = join->r;
		rel = out2inner(v, rel, join, inner_join_side, op_right);
		/* Now test if the right side can degenerate to
			* a normal inner join or a left outer join
			* depending on the result of previous call to out2inner.
			*/

		inner_join_side = join->l;
		return out2inner(v, rel, join, inner_join_side, is_right(join->op)? op_join: op_left);
	}
}

static bool
exps_uses_any(list *exps, list *l)
{
	bool uses_any = false;

	if (list_empty(exps) || list_empty(l))
		return false;
	for (node *n = l->h; n && !uses_any; n = n->next) {
		sql_exp *e = n->data;
		uses_any |= list_exps_uses_exp(exps, exp_relname(e), exp_name(e)) != NULL;
	}

	return uses_any;
}

/* TODO At the moment I have to disable the new join2semi because the join order optimizer doesn't take semi-joins into account,
so plans get deteriorated if more joins are optimized into semi-joins. Later I will review the join order with semi-joins and hopefully,
I will be able to re-enable the new join2semi. */
#if 0
#define NO_EXP_FOUND 0
#define FOUND_WITH_DUPLICATES 1
#define MAY_HAVE_DUPLICATE_NULLS 2
#define ALL_VALUES_DISTINCT 3

static int
find_projection_for_join2semi(sql_rel *rel, sql_exp *jc)
{
	sql_rel *res = NULL;
	sql_exp *e = NULL;
	bool underjoin = false;

	if ((e = rel_find_exp_and_corresponding_rel(rel, jc, &res, &underjoin))) {
		if (underjoin || e->type != e_column)
			return FOUND_WITH_DUPLICATES;
		/* if just one groupby column is projected or the relation needs distinct values and one column is projected or is a primary key, it will be distinct */
		if (is_unique(e) ||
			(is_groupby(res->op) && list_length(res->r) == 1 && exps_find_exp(res->r, e)) ||
			((is_project(res->op) || is_base(res->op)) && ((need_distinct(res) && list_length(res->exps) == 1) || res->card < CARD_AGGR)))
			return has_nil(e) ? MAY_HAVE_DUPLICATE_NULLS : ALL_VALUES_DISTINCT;
		return FOUND_WITH_DUPLICATES;
	}
	return NO_EXP_FOUND;
}

static int
subrel_uses_exp_outside_subrel(visitor *v, sql_rel *rel, list *l, sql_rel *j)
{
	if (rel == j)
		return 0;
	if (mvc_highwater(v->sql))
		return 1;
	switch(rel->op){
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		return exps_uses_any(rel->exps, l) ||
			subrel_uses_exp_outside_subrel(v, rel->l, l, j) || subrel_uses_exp_outside_subrel(v, rel->r, l, j);
	case op_semi:
	case op_anti:
	case op_select:
		return exps_uses_any(rel->exps, l) ||
			subrel_uses_exp_outside_subrel(v, rel->l, l, j);
	case op_project:
	case op_groupby:
		return exps_uses_any(rel->exps, l) || exps_uses_any(rel->r, l);
	case op_basetable:
	case op_table:
	case op_except:
	case op_inter:
		return exps_uses_any(rel->exps, l);
	case op_topn:
	case op_sample:
		return subrel_uses_exp_outside_subrel(v, rel->l, l, j);
	default:
		return 1;
	}
}

static int
projrel_uses_exp_outside_subrel(visitor *v, sql_rel *rel, list *l, sql_rel *j)
{
	/* test if projecting relation uses any of the join expressions */
	assert((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l);
	return exps_uses_any(rel->exps, l) || exps_uses_any(rel->r, l) || subrel_uses_exp_outside_subrel(v, rel->l, l, j);
}

static sql_rel *
rewrite_joins2semi(visitor *v, sql_rel *proj, sql_rel *rel)
{
	/* generalize possibility : we need the visitor 'step' here */
	if (rel_is_ref(rel) || mvc_highwater(v->sql)) /* if the join has multiple references, it's dangerous to convert it into a semijoin */
		return rel;
	if (is_innerjoin(rel->op) && !list_empty(rel->exps)) {
		sql_rel *l = rel->l, *r = rel->r;
		bool left_unique = true, right_unique = true;

		/* these relations don't project anything, so skip them */
		while (is_topn(l->op) || is_sample(l->op) || is_select(l->op) || is_semi(l->op))
			l = l->l;
		/* joins will expand values, so don't search on those */
		if (!is_base(l->op) && !is_project(l->op))
			left_unique = false;
		while (is_topn(r->op) || is_sample(r->op) || is_select(r->op) || is_semi(r->op))
			r = r->l;
		if (!is_base(r->op) && !is_project(r->op))
			right_unique = false;
		/* if all columns used in equi-joins from one of the sides are unique, the join can be rewritten into a semijoin */
		for (node *n=rel->exps->h; n && (left_unique || right_unique); n = n->next) {
			sql_exp *e = n->data, *el = e->l, *er = e->r;

			if (!is_compare(e->type) || e->flag != cmp_equal || exp_has_func(el) || exp_has_func(er)) {
				left_unique = right_unique = false;
			} else {
				int found = 0;

				if (left_unique && (found = find_projection_for_join2semi(l, el)) > NO_EXP_FOUND)
					left_unique &= (found == ALL_VALUES_DISTINCT || (found == MAY_HAVE_DUPLICATE_NULLS && (!is_semantics(e) || !has_nil(er))));
				if (left_unique && (found = find_projection_for_join2semi(l, er)) > NO_EXP_FOUND)
					left_unique &= (found == ALL_VALUES_DISTINCT || (found == MAY_HAVE_DUPLICATE_NULLS && (!is_semantics(e) || !has_nil(el))));
				if (right_unique && (found = find_projection_for_join2semi(r, el)) > NO_EXP_FOUND)
					right_unique &= (found == ALL_VALUES_DISTINCT || (found == MAY_HAVE_DUPLICATE_NULLS && (!is_semantics(e) || !has_nil(er))));
				if (right_unique && (found = find_projection_for_join2semi(r, er)) > NO_EXP_FOUND)
					right_unique &= (found == ALL_VALUES_DISTINCT || (found == MAY_HAVE_DUPLICATE_NULLS && (!is_semantics(e) || !has_nil(el))));
			}
		}

		/* now we need to check relation's expressions are not used */
		if (left_unique && !projrel_uses_exp_outside_subrel(v, proj, l->exps, rel)) {
			sql_rel *tmp = rel->r;
			rel->r = rel->l;
			rel->l = tmp;
			rel->op = op_semi;
			v->changes++;
		} else if (right_unique && !projrel_uses_exp_outside_subrel(v, proj, r->exps, rel)) {
			rel->op = op_semi;
			v->changes++;
		}
	}
	if (is_join(rel->op)) {
		rel->l = rewrite_joins2semi(v, proj, rel->l);
		rel->r = rewrite_joins2semi(v, proj, rel->r);
	} else if (is_topn(rel->op) || is_sample(rel->op) || is_select(rel->op) || is_semi(rel->op)) {
		rel->l = rewrite_joins2semi(v, proj, rel->l);
	}
	return rel;
}

static inline sql_rel *
rel_join2semijoin(visitor *v, sql_rel *rel)
{
	if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l)
		rel->l = rewrite_joins2semi(v, rel, rel->l);
	return rel;
}
#endif

#define NO_PROJECTION_FOUND 0
#define MAY_HAVE_DUPLICATE_NULLS 1
#define ALL_VALUES_DISTINCT 2

static int
find_projection_for_join2semi(sql_rel *rel)
{
	if (is_simple_project(rel->op) || is_groupby(rel->op) || is_inter(rel->op) || is_except(rel->op) || is_base(rel->op) || (is_munion(rel->op) && need_distinct(rel))) {
		if (rel->card < CARD_AGGR) /* const or groupby without group by exps */
			return ALL_VALUES_DISTINCT;
		if (list_length(rel->exps) == 1) {
			sql_exp *e = rel->exps->h->data;
			/* a single group by column in the projection list from a group by relation is guaranteed to be unique, but not an aggregate */
			if (e->type == e_column) {
				sql_rel *res = NULL;
				sql_exp *found = NULL;
				bool underjoin = false;

				/* if just one groupby column is projected or the relation needs distinct values and one column is projected or is a primary key, it will be distinct */
				if ((is_groupby(rel->op) && list_length(rel->r) == 1 && exps_find_exp(rel->r, e)) || (need_distinct(rel) && list_length(rel->exps) == 1))
					return ALL_VALUES_DISTINCT;
				if (is_unique(e))
					return has_nil(e) ? MAY_HAVE_DUPLICATE_NULLS : ALL_VALUES_DISTINCT;

				if ((is_simple_project(rel->op) || is_groupby(rel->op) || is_inter(rel->op) || is_except(rel->op)) &&
					(found = rel_find_exp_and_corresponding_rel(rel->l, e, false, &res, &underjoin)) && !underjoin) { /* grouping column on inner relation */
					if (need_distinct(res) && list_length(res->exps) == 1)
						return ALL_VALUES_DISTINCT;
					if (is_unique(found))
						return has_nil(e) ? MAY_HAVE_DUPLICATE_NULLS : ALL_VALUES_DISTINCT;
					if (found->type == e_column && found->card <= CARD_AGGR) {
						if (!is_groupby(res->op) && list_length(res->exps) != 1)
							return NO_PROJECTION_FOUND;
						for (node *n = res->exps->h ; n ; n = n->next) { /* must be the single column in the group by expression list */
							sql_exp *e = n->data;
							if (e != found && e->type == e_column)
								return NO_PROJECTION_FOUND;
						}
						return ALL_VALUES_DISTINCT;
					}
				}
			}
		}
	}
	return NO_PROJECTION_FOUND;
}

static sql_rel *
find_candidate_join2semi(visitor *v, sql_rel *rel, bool *swap)
{
	/* generalize possibility : we need the visitor 'step' here */
	if (rel_is_ref(rel)) /* if the join has multiple references, it's dangerous to convert it into a semijoin */
		return NULL;
	if (rel->op == op_join && !list_empty(rel->exps) && list_empty(rel->attr)) {
		sql_rel *l = rel->l, *r = rel->r;
		int foundr = NO_PROJECTION_FOUND, foundl = NO_PROJECTION_FOUND, found = NO_PROJECTION_FOUND;
		bool ok = false;

		foundr = find_projection_for_join2semi(r);
		if (foundr < ALL_VALUES_DISTINCT)
			foundl = find_projection_for_join2semi(l);
		if (foundr && foundr > foundl) {
			*swap = false;
			found = foundr;
		} else if (foundl) {
			*swap = true;
			found = foundl;
		}

		if (found > NO_PROJECTION_FOUND) {
			/* if all join expressions can be pushed down or have function calls, then it cannot be rewritten into a semijoin */
			for (node *n=rel->exps->h; n && !ok; n = n->next) {
				sql_exp *e = n->data;

				ok |= e->type == e_cmp && e->flag == cmp_equal && !exp_has_func(e) && !rel_rebind_exp(v->sql, l, e) && !rel_rebind_exp(v->sql, r, e) &&
					(found == ALL_VALUES_DISTINCT || !is_semantics(e) || !has_nil((sql_exp *)e->l) || !has_nil((sql_exp *)e->r));
			}
		}

		if (ok)
			return rel;
	}
	if (is_join(rel->op) || is_semi(rel->op)) {
		sql_rel *c;

		if ((c=find_candidate_join2semi(v, rel->l, swap)) != NULL ||
		    (c=find_candidate_join2semi(v, rel->r, swap)) != NULL)
			if (list_empty(c->attr))
				return c;
	}
	if (is_topn(rel->op) || is_sample(rel->op))
		return find_candidate_join2semi(v, rel->l, swap);
	return NULL;
}

static int
subrel_uses_exp_outside_subrel(sql_rel *rel, list *l, sql_rel *c)
{
	if (rel == c)
		return 0;
	/* for subrel only expect joins (later possibly selects) */
	if (is_join(rel->op) || is_semi(rel->op)) {
		if (exps_uses_any(rel->exps, l))
			return 1;
		if (subrel_uses_exp_outside_subrel(rel->l, l, c) ||
		    subrel_uses_exp_outside_subrel(rel->r, l, c))
			return 1;
	}
	if (is_topn(rel->op) || is_sample(rel->op))
		return subrel_uses_exp_outside_subrel(rel->l, l, c);
	return 0;
}

static int
rel_uses_exp_outside_subrel(sql_rel *rel, list *l, sql_rel *c)
{
	/* for now we only expect sub relations of type project, selects (rel) or join/semi */
	if (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op)) {
		if (!list_empty(rel->exps) && exps_uses_any(rel->exps, l))
			return 1;
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && !list_empty(rel->r) && exps_uses_any(rel->r, l))
			return 1;
		if (rel->l)
			return subrel_uses_exp_outside_subrel(rel->l, l, c);
	}
	if (is_topn(rel->op) || is_sample(rel->op))
		return subrel_uses_exp_outside_subrel(rel->l, l, c);
	return 1;
}

static inline sql_rel *
rel_join2semijoin(visitor *v, sql_rel *rel)
{
	if (!rel_is_ref(rel) && is_simple_project(rel->op) && need_distinct(rel) && rel->l) {
		sql_rel *l = rel->l;

		if (!rel_is_ref(l) && l->op == op_join && rel_has_all_exps(l->l, rel->exps)) {
			l->op = op_semi;
			v->changes++;
			return rel;
		}
	}
	if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l) {
		bool swap = false;
		sql_rel *l = rel->l;
		sql_rel *c = find_candidate_join2semi(v, l, &swap);

		if (c) {
			/* 'p' is a project */
			sql_rel *p = swap ? c->l : c->r;

			/* now we need to check if ce is only used at the level of c */
			if (!rel_uses_exp_outside_subrel(rel, p->exps, c)) {
				c->op = op_semi;
				if (swap) {
					sql_rel *tmp = c->r;
					c->r = c->l;
					c->l = tmp;
				}
				v->changes++;
			}
		}
	}
	return rel;
}

static inline sql_rel *
rel_push_join_down_outer(visitor *v, sql_rel *rel)
{
	if (is_join(rel->op) && !is_outerjoin(rel->op) && !is_single(rel) && !list_empty(rel->exps) && !rel_is_ref(rel)) {
		sql_rel *l = rel->l, *r = rel->r;

		if (is_left(r->op) && (is_select(l->op) || (is_join(l->op) && !is_outerjoin(l->op))) && !rel_is_ref(l) &&
				!rel_is_ref(r)) {
			sql_rel *rl = r->l;
			sql_rel *rr = r->r;
			if (rel_is_ref(rl) || rel_is_ref(rr))
				return rel;
			/* join exps should only include l and r.l */
			list *njexps = sa_list(v->sql->sa);
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *je = n->data;

				assert(je->type == e_cmp);
				if (je->f)
					return rel;
				if ((rel_find_exp(l, je->l) && rel_find_exp(rl, je->r)) || (rel_find_exp(l, je->r) && rel_find_exp(rl, je->l))) {
					list_append(njexps, je);
				} else {
					return rel;
				}
			}
			sql_rel *nl = rel_crossproduct(v->sql->sa, rel_dup(l), rl, rel->op);
			r->l = nl;
			nl->exps = njexps;
			nl->attr = rel->attr;
			rel->attr = NULL;
			set_processed(nl);
			rel_dup(r);
			rel_destroy(rel);
			rel = r;
			v->changes++;
		}
	}
	return rel;
}

static sql_rel *
rel_optimize_joins_(visitor *v, sql_rel *rel)
{
	rel = rel_push_join_exps_down(v, rel);
	rel = rel_out2inner(v, rel);
	rel = rel_join2semijoin(v, rel);
	rel = rel_push_join_down_outer(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_joins(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_optimize_joins_);
}

run_optimizer
bind_optimize_joins(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right]
		   || gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti]) && (flag & optimize_joins) ? rel_optimize_joins : NULL;
}


static sql_rel *rel_join_order_(visitor *v, sql_rel *rel);

static void
get_relations(visitor *v, sql_rel *rel, list *rels)
{
	if (list_empty(rel->attr) && !rel_is_ref(rel) && rel->op == op_join && rel->exps == NULL) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;

		get_relations(v, l, rels);
		get_relations(v, r, rels);
		rel->l = NULL;
		rel->r = NULL;
		rel_destroy(rel);
	} else {
		rel = rel_join_order_(v, rel);
		append(rels, rel);
	}
}

static void
get_inner_relations(mvc *sql, sql_rel *rel, list *rels)
{
	if (!rel_is_ref(rel) && is_join(rel->op)) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;

		get_inner_relations(sql, l, rels);
		get_inner_relations(sql, r, rels);
	} else {
		append(rels, rel);
	}
}

static int
exp_count(int *cnt, sql_exp *e)
{
	int flag;
	if (!e)
		return 0;
	if (find_prop(e->p, PROP_JOINIDX))
		*cnt += 100;
	if (find_prop(e->p, PROP_HASHCOL))
		*cnt += 100;
	if (find_prop(e->p, PROP_HASHIDX))
		*cnt += 100;
	switch(e->type) {
	case e_cmp:
		if (!is_complex_exp(e->flag)) {
			exp_count(cnt, e->l);
			exp_count(cnt, e->r);
			if (e->f)
				exp_count(cnt, e->f);
		}
 		flag = e->flag;
		switch (flag) {
		case cmp_equal:
			*cnt += 90;
			return 90;
		case cmp_notequal:
			*cnt += 7;
			return 7;
		case cmp_gt:
		case cmp_gte:
		case cmp_lt:
		case cmp_lte:
			*cnt += 6;
			if (e->l) {
				sql_exp *l = e->l;
				sql_subtype *t = exp_subtype(l);
				if (EC_TEMP(t->type->eclass)) /* give preference to temporal ranges */
					*cnt += 90;
			}
			if (e->f){ /* range */
				*cnt += 6;
				return 12;
			}
			return 6;
		case cmp_filter:
			if (exps_card(e->r) > CARD_AGGR) {
				/* filters for joins are special */
				*cnt += 1000;
				return 1000;
			}
			*cnt += 2;
			return 2;
		case cmp_con:
		case cmp_dis: {
						  /*
			list *l = e->l;
			int c = 9 - 10*list_length(l);
			*cnt += c;
			return c;
			*/
			*cnt += 3;
			return 3;
		}
		case cmp_in:
		case cmp_notin: {
			list *l = e->r;
			int c = 9 - 10*list_length(l);
			*cnt += c;
			return c;
		}
		default:
			return 0;
		}
	case e_column:
		*cnt += 20;
		return 20;
	case e_atom:
		*cnt += 10;
		return 10;
	case e_func:
		/* functions are more expensive, depending on the number of columns involved. */
		if (e->card == CARD_ATOM)
			return 0;
		*cnt -= 5*list_length(e->l);
		return 5*list_length(e->l);
	case e_convert:
		/* functions are more expensive, depending on the number of columns involved. */
		if (e->card == CARD_ATOM)
			return 0;
		/* fall through */
	default:
		*cnt -= 5;
		return -5;
	}
}

int
exp_keyvalue(sql_exp *e)
{
	int cnt = 0;
	exp_count(&cnt, e);
	return cnt;
}

static sql_exp *
joinexp_col(sql_exp *e, sql_rel *r)
{
	if (e->type == e_cmp) {
		if (rel_has_exp(r, e->l, false) >= 0)
			return e->l;
		return e->r;
	}
	assert(0);
	return NULL;
}

static sql_column *
table_colexp(sql_exp *e, sql_rel *r)
{
	sql_table *t = r->l;

	if (e->type == e_column) {
		const char *name = exp_name(e);
		node *cn;

		if (r->exps) { /* use alias */
			for (cn = r->exps->h; cn; cn = cn->next) {
				sql_exp *ce = cn->data;
				if (strcmp(exp_name(ce), name) == 0) {
					name = ce->r;
					break;
				}
			}
		}
		for (cn = ol_first_node(t->columns); cn; cn = cn->next) {
			sql_column *c = cn->data;
			if (strcmp(c->base.name, name) == 0)
				return c;
		}
	}
	return NULL;
}

static list *
matching_joins(allocator *sa, list *rels, list *exps, sql_exp *je)
{
	sql_rel *l, *r;

	assert (je->type == e_cmp);

	l = find_rel(rels, je->l);
	r = find_rel(rels, je->r);
	if (l && r) {
		list *res;
		list *n_rels = sa_list(sa);

		append(n_rels, l);
		append(n_rels, r);
		res = list_select(exps, n_rels, (fcmp) &exp_joins_rels, (fdup)NULL);
		return res;
	}
	return sa_list(sa);
}

static int
sql_column_kc_cmp(sql_column *c, sql_kc *kc)
{
	/* return on equality */
	return (c->colnr - kc->c->colnr);
}

static sql_idx *
find_fk_index(mvc *sql, sql_table *l, list *lcols, sql_table *r, list *rcols)
{
	sql_trans *tr = sql->session->tr;

	if (l->idxs) {
		node *in;
		for (in = ol_first_node(l->idxs); in; in = in->next){
			sql_idx *li = in->data;
			if (li->type == join_idx) {
				sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)li->key)->rkey);
				fcmp cmp = (fcmp)&sql_column_kc_cmp;

				if (rk->t == r &&
					list_match(lcols, li->columns, cmp) == 0 &&
					list_match(rcols, rk->columns, cmp) == 0) {
					return li;
				}
			}
		}
	}
	return NULL;
}

static sql_rel *
find_basetable( sql_rel *r)
{
	if (!r)
		return NULL;
	switch(r->op) {
	case op_basetable:
		if (!r->l)
			return NULL;
		return r;
	case op_semi:
	case op_anti:
	case op_project:
	case op_select:
	case op_topn:
	case op_sample:
		return find_basetable(r->l);
	default:
		return NULL;
	}
}

static int
exps_count(list *exps)
{
	node *n;
	int cnt = 0;

	if (!exps)
		return 0;
	for (n = exps->h; n; n=n->next)
		exp_count(&cnt, n->data);
	return cnt;
}

static list *
order_join_expressions(mvc *sql, list *dje, list *rels)
{
	node *n;
	int cnt = list_length(dje);

	if (cnt <= 1)
		return dje;

	list *res = sa_list(sql->sa);
	int i, *keys = SA_NEW_ARRAY(sql->ta, int, cnt);
	void **data = SA_NEW_ARRAY(sql->ta, void*, cnt);

	for (n = dje->h, i = 0; n; n = n->next, i++) {
		sql_exp *e = n->data;

		keys[i] = exp_keyvalue(e);
		/* add some weight for the selections */
		if (e->type == e_cmp && !is_complex_exp(e->flag)) {
			sql_rel *l = find_rel(rels, e->l);
			sql_rel *r = find_rel(rels, e->r);

			if (l && is_select(l->op) && l->exps)
				keys[i] += list_length(l->exps)*10 + exps_count(l->exps);
			if (r && is_select(r->op) && r->exps)
				keys[i] += list_length(r->exps)*10 + exps_count(r->exps);
		}
		data[i] = n->data;
	}
	/* sort descending */
	GDKqsort(keys, data, NULL, cnt, sizeof(int), sizeof(void *), TYPE_int, true, true);
	for(i=0; i<cnt; i++) {
		list_append(res, data[i]);
	}
	return res;
}

static int
find_join_rels(list **L, list **R, list *exps, list *rels)
{
	node *n;

	*L = sa_list(exps->sa);
	*R = sa_list(exps->sa);
	if (!exps || list_length(exps) <= 1)
		return -1;
	for(n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_rel *l = NULL, *r = NULL;

		if (!is_complex_exp(e->flag)){
			l = find_rel(rels, e->l);
			r = find_rel(rels, e->r);
		}
		if (l<r) {
			list_append(*L, l);
			list_append(*R, r);
		} else {
			list_append(*L, r);
			list_append(*R, l);
		}
	}
	return 0;
}

static list *
distinct_join_exps(list *aje, list *lrels, list *rrels)
{
	node *n, *m, *o, *p;
	int len = list_length(aje), i, j;
	char *used = SA_ZNEW_ARRAY(aje->sa, char, len);
	list *res = sa_list(aje->sa);

	assert(len == list_length(lrels));
	for(n = lrels->h, m = rrels->h, j = 0; n && m;
	    n = n->next, m = m->next, j++) {
		if (n->data && m->data)
		for(o = n->next, p = m->next, i = j+1; o && p;
		    o = o->next, p = p->next, i++) {
			if (o->data == n->data && p->data == m->data)
				used[i] = 1;
		}
	}
	for (i = 0, n = aje->h; i < len; n = n->next, i++) {
		if (!used[i])
			list_append(res, n->data);
	}
	return res;
}

static list *
find_fk( mvc *sql, list *rels, list *exps)
{
	node *djn;
	list *sdje, *aje, *dje;
	list *lrels, *rrels;

	/* first find the distinct join expressions */
	aje = list_select(exps, rels, (fcmp) &exp_is_join, (fdup)NULL);
	/* add left/right relation */
	if (find_join_rels(&lrels, &rrels, aje, rels) < 0)
		dje = aje;
	else
		dje = distinct_join_exps(aje, lrels, rrels);
	for(djn=dje->h; djn; djn = djn->next) {
		/* equal join expressions */
		sql_idx *idx = NULL;
		sql_exp *je = djn->data, *le = je->l, *re = je->r;

		if (is_complex_exp(je->flag))
			break;
		if (!find_prop(je->p, PROP_JOINIDX)) {
			int swapped = 0;
			list *aaje = matching_joins(sql->sa, rels, aje, je);
			list *eje = list_select(aaje, (void*)1, (fcmp) &exp_is_eqjoin, (fdup)NULL);
			sql_rel *lr = find_rel(rels, le), *olr = lr;
			sql_rel *rr = find_rel(rels, re), *orr = rr;
			sql_rel *bt = NULL;
			char *iname;

			sql_table *l, *r;
			list *lexps = list_map(eje, lr, (fmap) &joinexp_col);
			list *rexps = list_map(eje, rr, (fmap) &joinexp_col);
			list *lcols, *rcols;

			lr = find_basetable(lr);
			rr = find_basetable(rr);
			if (!lr || !rr)
				continue;
			l = lr->l;
			r = rr->l;
			lcols = list_map(lexps, lr, (fmap) &table_colexp);
			rcols = list_map(rexps, rr, (fmap) &table_colexp);
			lcols->destroy = NULL;
			rcols->destroy = NULL;
			if (list_length(lcols) != list_length(rcols))
				continue;

			idx = find_fk_index(sql, l, lcols, r, rcols);
			if (!idx) {
				idx = find_fk_index(sql, r, rcols, l, lcols);
				swapped = 1;
			}

			if (idx && (iname = sa_strconcat( sql->sa, "%", idx->base.name)) != NULL &&
				   ((!swapped && name_find_column(olr, NULL, iname, -2, &bt) == NULL) ||
			            ( swapped && name_find_column(orr, NULL, iname, -2, &bt) == NULL)))
				idx = NULL;

			if (idx) {
				prop *p;
				node *n;
				sql_exp *t = NULL, *i = NULL;

				if (list_length(lcols) > 1 || !mvc_debug_on(sql, 512)) {

					/* Add join between idx and TID */
					if (swapped) {
						sql_exp *s = je->l, *l = je->r;

						t = rel_find_column(sql, olr, s->l, TID);
						i = rel_find_column(sql, orr, l->l, iname);
						if (!t || !i)
							continue;
						t->p = NULL;
						i->p = NULL;
						je = exp_compare(sql->sa, i, t, cmp_equal);
					} else {
						sql_exp *s = je->r, *l = je->l;

						t = rel_find_column(sql, orr, s->l, TID);
						i = rel_find_column(sql, olr, l->l, iname);
						if (!t || !i)
							continue;
						t->p = NULL;
						i->p = NULL;
						je = exp_compare(sql->sa, i, t, cmp_equal);
					}

					/* Remove all join expressions */
					for (n = eje->h; n; n = n->next)
						list_remove_data(exps, NULL, n->data);
					append(exps, je);
					djn->data = je;
				} else if (swapped) { /* else keep je for single column expressions */
					je = exp_compare(sql->sa, je->r, je->l, cmp_equal);
					/* Remove all join expressions */
					for (n = eje->h; n; n = n->next)
						list_remove_data(exps, NULL, n->data);
					append(exps, je);
					djn->data = je;
				}
				je->p = p = prop_create(sql->sa, PROP_JOINIDX, je->p);
				p->value.pval = idx;
			}
		}
	}

	/* sort expressions on weighted number of reducing operators */
	sdje = order_join_expressions(sql, dje, rels);
	return sdje;
}

static int
exp_find_one_rel( sql_rel **rels, int nr, sql_exp *e)
{
	int fnd = 0;

	for(int i = 1; i<=nr; i++) {
		if (rel_has_exp(rels[i], e, false) == 0) {
			if (fnd)
				return 0;
			fnd = i;
		}
	}
	return fnd;
}

static int
exps_find_one_rel( sql_rel **rels, int nr, list *exps)
{
	int fnd = 0;

	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (exp_is_atom(e))
			continue;
		int nfnd = exp_find_one_rel(rels, nr, n->data);
		if (nfnd != fnd && fnd)
			return 0;
		fnd = nfnd;
		if (!fnd)
			return 0;
	}
	return fnd;
}

/* TODO move popcount and popcount64 into gdk_*.h, used in gdk_cand, strimps and here */
static inline int
popcount64(uint64_t x)
{
#ifdef __has_builtin
#if __has_builtin(__builtin_popcountll)
	return (uint32_t) __builtin_popcountll(x);
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
#if defined(_MSC_VER)
#if SIZEOF_OID == 4
	/* no __popcnt64 on 32 bit Windows */
	return (int) (__popcnt((uint32_t) x) + __popcnt((uint32_t) (x >> 32)));
#else
	return (uint32_t) __popcnt64(x);
#endif
#else
	x = (x & UINT64_C(0x5555555555555555)) + ((x >> 1) & UINT64_C(0x5555555555555555));
	x = (x & UINT64_C(0x3333333333333333)) + ((x >> 2) & UINT64_C(0x3333333333333333));
	x = (x & UINT64_C(0x0F0F0F0F0F0F0F0F)) + ((x >> 4) & UINT64_C(0x0F0F0F0F0F0F0F0F));
	return (x * UINT64_C(0x0101010101010101)) >> 56;
#endif
#endif
#undef BUILTIN_USED
}

static sql_rel *
order_joins(visitor *v, list *rels, list *exps)
{
	sql_rel *top = NULL, *l = NULL, *r = NULL, *f = NULL;
	sql_exp *cje;
	node *djn;
	list *sdje, *n_rels = NULL;
	int fnd = 0;
	unsigned int rsingle;
	int direct = 1;

	/* find foreign keys and reorder the expressions on reducing quality */
	sdje = find_fk(v->sql, rels, exps);

	for(djn = sdje->h; djn; djn = djn->next ) {
		sql_exp *e = djn->data;
		list_remove_data(exps, NULL, e);
	}
	if (list_length(rels) > 2 && mvc_debug_on(v->sql, 256)) {
		top =  rel_planner(v->sql, rels, sdje, exps);
		return top;
	}

	int nr_exps = list_length(sdje), nr_rels = list_length(rels), ci = 1;
	if (nr_rels > 64) {
		direct = 0;
		n_rels = sa_list(v->sql->ta);
	}
	sql_rel **rels_a = SA_NEW_ARRAY(v->sql->ta, sql_rel*, nr_rels+1); /* don't use slot 0 */
	rels_a[0] = NULL;
	for (node *n = rels->h; n; n = n->next, ci++) {
		rels_a[ci] = n->data;
	}
	ulng *h = SA_NEW_ARRAY(v->sql->ta, ulng, nr_exps), rel_mask = 0;	/* bit field (for > 64 its an imprint) */
	uint16_t *r1 = SA_NEW_ARRAY(v->sql->ta, uint16_t, nr_exps);
	uint16_t *r2 = SA_NEW_ARRAY(v->sql->ta, uint16_t, nr_exps);
	/* change r3 into rest list's */
	int *r3 = SA_NEW_ARRAY(v->sql->ta, int, nr_exps);

	ci = 0;
	for (node *n = sdje->h; n; n = n->next, ci++) {
		sql_exp *cje = n->data;

		h[ci] = r1[ci] = r2[ci] = r3[ci] = 0;
		if (cje->type == e_cmp) {
			cje->tmp = ci;
			r1[ci] = cje->flag == cmp_filter ? exps_find_one_rel(rels_a, nr_rels, cje->l) : exp_find_one_rel(rels_a, nr_rels, cje->l);
			r2[ci] = cje->flag == cmp_filter ? exps_find_one_rel(rels_a, nr_rels, cje->r) : exp_find_one_rel(rels_a, nr_rels, cje->r);
			if (r1[ci])
				h[ci] |= ((ulng)1)<<((r1[ci]-1)%64);
			if (r2[ci])
				h[ci] |= ((ulng)1)<<((r2[ci]-1)%64);
			if (cje->f && cje->flag != cmp_filter) {
				r3[ci] = exp_find_one_rel(rels_a, nr_rels, cje->f);
				if (r3[ci] == r2[ci] || r3[ci] == r1[ci])
					r3[ci] = 0;
				if (r3[ci])
					h[ci] |= ((ulng)1)<<((r3[ci]-1)%64);
			}
		}
	}
	/* open problem, some expressions use more than 2 relations */
	/* For example a.x = b.y * c.z; */
	if (list_length(rels) >= 2 && sdje->h) {
		for (node *n = sdje->h; n && (!l || !r); n = n->next, ci++) {
			cje = n->data;

			if (n->next && r3[cje->tmp])
				continue;

			/* complex expressions may touch multiple base tables
			 * Should be pushed up to extra selection.
			 * */
			if (0 && popcount64(h[cje->tmp]) > 2)
				assert(0);
			/* find the involved relations */
			if (cje->type == e_cmp) {
				l = rels_a[r1[cje->tmp]];
				r = rels_a[r2[cje->tmp]];
				if (l && r)
					rel_mask |= h[cje->tmp];
			}
		}
		cje->tmp = 0;

		if (l && r && l != r)
			list_remove_data(sdje, NULL, cje);
	}

	if (l && r && l != r) {
		list_remove_data(rels, NULL, l);
		list_remove_data(rels, NULL, r);
		if (!direct) {
			list_append(n_rels, l);
			list_append(n_rels, r);
		}

		/* Create a relation between l and r. Since the calling
		   functions rewrote the join tree, into a list of expressions
		   and a list of (simple) relations, there are no outer joins
		   involved, we can simply do a crossproduct here.
		   */
		rsingle = is_single(r);
		reset_single(r);
		top = rel_crossproduct(v->sql->sa, l, r, op_join);
		if (rsingle)
			set_single(r);
		rel_join_add_exp(v->sql->sa, top, cje);

		/* all other join expressions on these 2 relations */
		for (node *en = exps->h; en; ) {
			node *next = en->next;
			sql_exp *e = en->data;
			if (rel_rebind_exp(v->sql, top, e)) {
				rel_join_add_exp(v->sql->sa, top, e);
				list_remove_data(exps, NULL, e);
			}
			en = next;
		}
		fnd = 1;
	}
	/* build join tree using the ordered list */
	while(list_length(sdje) && fnd) {
		fnd = 0;
		/* find the first expression which could be added */
		for(djn = sdje->h; djn && !fnd && rels->h; djn = (!fnd)?djn->next:NULL) {
			node *en;
			l = r = f = NULL;
			int needs3 = 0;

			cje = djn->data;
			if ((h[cje->tmp] & rel_mask) > 0) {
				if (rel_mask & (((ulng)1)<<((r1[cje->tmp]-1)%64)))
					l = rels_a[r1[cje->tmp]];
				if (rel_mask & (((ulng)1)<<((r2[cje->tmp]-1)%64)))
					r = rels_a[r2[cje->tmp]];
				if (cje->f && r3[cje->tmp]) {
					needs3 = 1;
					if (rel_mask & (((ulng)1)<<((r3[cje->tmp]-1)%64)))
						f = rels_a[r3[cje->tmp]];
				}
			}
			if (!direct) { /* check if at least one side in n_rels */
				if (l && !list_find(n_rels, l, NULL))
					l = NULL;
				if (r && !list_find(n_rels, r, NULL))
					r = NULL;
				if (f && !list_find(n_rels, f, NULL))
					f = NULL;
			}

			if ((!needs3 && l && r) || (needs3 && l && r && f)) {
				assert(0);
				/* create a selection on the current */
				rel_join_add_exp(v->sql->sa, top, cje);
				fnd = 1;
			} else if ((!needs3 && (l || r)) || (needs3 && (l || r || f))) {
				sql_rel *nr[2]= {NULL, NULL};
				rel_mask |= h[cje->tmp];
				int i = 0;
				if (!l)
					nr[i++] = rels_a[r1[cje->tmp]];
				if (!r)
					nr[i++] = rels_a[r2[cje->tmp]];
				if (needs3 && !f)
					nr[i++] = rels_a[r3[cje->tmp]];
				if (!nr[0]) {
					fnd = 1; /* not really, but this bails out */
					list_remove_data(sdje, NULL, cje); /* handle later as select */
					if (!list_find(exps, cje, NULL))
						append(exps, cje);
					continue;
				}
				/* remove the expression from the lists */
				list_remove_data(sdje, NULL, cje);

				list_remove_data(rels, NULL, nr[0]);
				if (!direct)
					append(n_rels, nr[0]);
				if (i > 1 && nr[1]) {
					list_remove_data(rels, NULL, nr[1]);
					if (!direct)
						append(n_rels, nr[1]);
				}

				/* create a join using the current expression */
				rsingle = is_single(nr[0]);
				reset_single(nr[0]);
				top = rel_crossproduct(v->sql->sa, top, nr[0], op_join);
				if (rsingle)
					set_single(nr[0]);
				if (i > 1 && nr[1]) {
					rsingle = is_single(nr[1]);
					reset_single(nr[1]);
					top = rel_crossproduct(v->sql->sa, top, nr[1], op_join);
					if (rsingle)
						set_single(nr[1]);
				}
				rel_join_add_exp(v->sql->sa, top, cje);

				/* all join expressions on these tables */
				for (en = exps->h; en; ) {
					node *next = en->next;
					sql_exp *e = en->data;
					if (rel_rebind_exp(v->sql, top, e)) {
						rel_join_add_exp(v->sql->sa, top, e);
						list_remove_data(exps, NULL, e);
					}
					en = next;
				}
				/* Remove other joins on the current 'n_rels'
				   set in the distinct list too */
				for (en = sdje->h; en; ) {
					node *next = en->next;
					sql_exp *e = en->data;
					if ((direct && ((e->flag <= cmp_notequal && (h[e->tmp] & rel_mask) == h[e->tmp] && h[e->tmp]) || (e->flag > cmp_notequal && rel_rebind_exp(v->sql, top, e))))  ||
					    (!direct && rel_rebind_exp(v->sql, top, e))) {
						rel_join_add_exp(v->sql->sa, top, e);
						list_remove_data(sdje, NULL, en->data);
					}
					en = next;
				}
				fnd = 1;
			}
		}
	}
	if (list_length(rels)) { /* more relations */
		node *n;
		for(n=rels->h; n; n = n->next) {
			sql_rel *nr = n->data;

			if (top) {
				rsingle = is_single(nr);
				reset_single(nr);
				top = rel_crossproduct(v->sql->sa, top, nr, op_join);
				if (rsingle)
					set_single(nr);
			} else
				top = nr;
		}
	}
	if (list_length(sdje)) {
		if (list_empty(exps))
			exps = sdje;
		else
			exps = list_merge(exps, sdje, (fdup)NULL);
	}
	if (list_length(exps)) { /* more expressions (add selects) */
		top = rel_select(v->sql->sa, top, NULL);
		for(node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (exp_is_join_exp(e) == 0) {
				sql_rel *nr = NULL;
				if (is_theta_exp(e->flag)) {
					nr = rel_push_join(v->sql, top->l, e->l, e->r, e->f, e, 0);
				} else if (e->flag == cmp_filter) {
					sql_exp *l = exps_find_one_multi_exp(e->l), *r = exps_find_one_multi_exp(e->r);
					if (l && r)
						nr = rel_push_join(v->sql, top->l, l, r, NULL, e, 0);
				}
				if (!nr)
					rel_join_add_exp(v->sql->sa, top->l, e);
			} else
				rel_select_add_exp(v->sql->sa, top, e);
		}
		if (list_empty(top->exps)) { /* empty select */
			sql_rel *l = top->l;
			top->l = NULL;
			rel_destroy(top);
			top = l;
		}
	}
	return top;
}

static int
rel_neg_in_size(sql_rel *r)
{
	if (is_munion(r->op) && r->nrcols == 0) {
		list *l = r->l;
		int n = 0;
		for(node *m = l->h; m; m = m->next) {
			sql_rel *i = m->data;
			n += rel_neg_in_size(i);
		}
		return -1 + n;
	}
	if (is_project(r->op) && r->nrcols == 0)
		return -1;
	return 0;
}

static void _rel_destroy(void *dummy, sql_rel *rel)
{
	(void)dummy;
	rel_destroy(rel);
}

static list *
push_in_join_down(mvc *sql, list *rels, list *exps)
{
	node *n;
	int restart = 1;
	list *nrels;

	/* we should sort these first, ie small in's before large one's */
	nrels = list_sort(rels, (fkeyvalue)&rel_neg_in_size, (fdup)&rel_dup);

	/* we need to cleanup, the new refs ! */
	rels->destroy = (fdestroy)_rel_destroy;
	list_destroy(rels);
	rels = nrels;

	/* one of the rels should be a op_union with nrcols == 0 */
	while (restart) {
		for (n = rels->h; n; n = n->next) {
			sql_rel *r = n->data;

			restart = 0;
			if (is_project(r->op) && r->nrcols == 0) {
				/* next step find expression on this relation */
				node *m;
				sql_rel *l = NULL;
				sql_exp *je = NULL;

				for(m = exps->h; !je && m; m = m->next) {
					sql_exp *e = m->data;

					if (e->type == e_cmp && e->flag == cmp_equal) {
						/* in values are on
							the right of the join */
						if (rel_has_exp(r, e->r, false) >= 0)
							je = e;
					}
				}
				/* with this expression find other relation */
				if (je && (l = find_rel(rels, je->l)) != NULL) {
					unsigned int rsingle = is_single(r);
					reset_single(r);
					sql_rel *nr = rel_crossproduct(sql->sa, l, r, op_join);
					if (rsingle)
						set_single(r);
					rel_join_add_exp(sql->sa, nr, je);
					list_append(rels, nr);
					list_remove_data(rels, NULL, l);
					list_remove_data(rels, NULL, r);
					list_remove_data(exps, NULL, je);
					restart = 1;
					break;
				}

			}
		}
	}
	return rels;
}

static sql_rel *
remove_blocking_selects( mvc *sql, sql_rel *p, sql_rel *rel)
{
	if (rel_is_ref(rel))
		return rel;

	switch(rel->op) {
	case op_select:
		if (p) {
			sql_rel *l = rel->l;
			if (!is_join(l->op))
				return rel;
			if (!list_empty(rel->exps)) {
				for(node *n = rel->exps->h; n; ) {
					node *nn = n->next;
					sql_exp *e = n->data;

					if (e->type == e_cmp && e->flag == cmp_dis) {
						if (!p->exps)
							p->exps = sa_list(sql->sa);
						append(p->exps, e);
						list_remove_node(rel->exps, NULL, n);
					}
					n = nn;
				}
				if (list_empty(rel->exps)) {
					sql_rel *l = rel->l;
					rel->l = NULL;
					rel_destroy(rel);
					return remove_blocking_selects( sql, p, l);
				}
			}
		}
		return rel;
	case op_join:
		rel->l = remove_blocking_selects(sql, rel, rel->l);
		rel->r = remove_blocking_selects(sql, rel, rel->r);
		return rel;
	default:
		return rel;
	}
}

static list *
push_up_join_exps( mvc *sql, sql_rel *rel)
{
	if (rel_is_ref(rel))
		return NULL;

	switch(rel->op) {
	case op_join: {
		sql_rel *rl = rel->l;
		sql_rel *rr = rel->r;
		list *l, *r;

		if (rel_is_ref(rl) && rel_is_ref(rr)) {
			l = rel->exps;
			rel->exps = NULL;
			return l;
		}
		l = push_up_join_exps(sql, rl);
		r = push_up_join_exps(sql, rr);
		if (l && r) {
			l = list_merge(l, r, (fdup)NULL);
			r = NULL;
		} else if (!l) {
			l = r;
			r = NULL;
		}
		if (rel->exps) {
			if (l && !r)
				r = l;
			l = list_merge(rel->exps, r, (fdup)NULL);
		}
		rel->exps = NULL;
		return l;
	}
	default:
		return NULL;
	}
}

static sql_rel *
reorder_join(visitor *v, sql_rel *rel)
{
	list *exps, *rels;

	if (is_innerjoin(rel->op) && !is_single(rel) && !rel_is_ref(rel) && list_empty(rel->attr)) {
		if (list_empty(rel->exps)) {
			sql_rel *l = rel->l, *r = rel->r;
			if (!is_innerjoin(l->op) && !is_innerjoin(r->op))
				return rel;
		}
		/* remove blocking selects */
		rel = remove_blocking_selects(v->sql, NULL, rel);

		rel->exps = push_up_join_exps(v->sql, rel);
	}

	if (!is_innerjoin(rel->op) || is_single(rel) || rel_is_ref(rel) || list_empty(rel->exps) || !list_empty(rel->attr)) {
		if (!list_empty(rel->exps)) { /* cannot add join idxs to cross products */
			exps = rel->exps;
			rel->exps = NULL; /* should be all crosstables by now */
			rels = sa_list(v->sql->ta);
			/* try to use an join index also for outer joins */
			get_inner_relations(v->sql, rel, rels);
			int cnt = list_length(exps);
			rel->exps = find_fk(v->sql, rels, exps);
			if (list_length(rel->exps) != cnt)
				rel->exps = order_join_expressions(v->sql, exps, rels);
		}
		rel->l = rel_join_order_(v, rel->l);
		rel->r = rel_join_order_(v, rel->r);
	} else {
		exps = rel->exps;
		rel->exps = NULL; /* should be all crosstables by now */
		rels = sa_list(v->sql->ta);
		get_relations(v, rel, rels);
		if (list_length(rels) > 1) {
			rels = push_in_join_down(v->sql, rels, exps);
			rel = order_joins(v, rels, exps);
		} else {
			rel->exps = exps;
		}
	}
	return rel;
}

static sql_rel *
rel_join_order_(visitor *v, sql_rel *rel)
{
	if (!rel)
		return rel;

	switch (rel->op) {
	case op_basetable:
		break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			rel->l = rel_join_order_(v, rel->l);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		break;

	case op_semi:
	case op_anti:

	case op_inter:
	case op_except:
		rel->l = rel_join_order_(v, rel->l);
		rel->r = rel_join_order_(v, rel->r);
		break;
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			n->data = rel_join_order_(v, n->data);
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		rel->l = rel_join_order_(v, rel->l);
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			rel->l = rel_join_order_(v, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel->l = rel_join_order_(v, rel->l);
			rel->r = rel_join_order_(v, rel->r);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rel_join_order_(v, rel->r);
		break;
	case op_truncate:
		break;
	}
	if (is_join(rel->op))
		rel = reorder_join(v, rel);
	return rel;
}

static sql_rel *
rel_join_order(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	sql_rel *r = rel_join_order_(v, rel);
	sa_reset(v->sql->ta);
	return r;
}

run_optimizer
bind_join_order(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && gp->opt_cycle < 10 && !gp->cnt[op_update] && (gp->cnt[op_join] || gp->cnt[op_left] ||
		   gp->cnt[op_right] || gp->cnt[op_full]) && (flag & join_order) ? rel_join_order : NULL;
}

/* this join order is to be done once after statistics are gathered */
run_optimizer
bind_join_order2(visitor *v, global_props *gp)
{
	/*int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && !gp->has_special_modify && !gp->cnt[op_update] && (gp->cnt[op_join] || gp->cnt[op_left] ||
		   gp->cnt[op_right] || gp->cnt[op_full]) && (flag & join_order) ? rel_join_order : NULL;*/
	/* TODO we have to propagate count statistics here */
	(void) v;
	(void) gp;
	return NULL;
}


static int
is_identity_of(sql_exp *e, sql_rel *l)
{
	if (e->type != e_cmp)
		return 0;
	if (!is_identity(e->l, l) || !is_identity(e->r, l) || (e->f && !is_identity(e->f, l)))
		return 0;
	return 1;
}

static inline sql_rel *
rel_rewrite_semijoin(visitor *v, sql_rel *rel)
{
	assert(is_semi(rel->op));
	{
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		sql_rel *rl = (r->l)?r->l:NULL;
		int on_identity = 1;

		if (!rel->exps || list_length(rel->exps) != 1 || !is_identity_of(rel->exps->h->data, l))
			on_identity = 0;

		/* rewrite {semi,anti}join (A, join(A,B)) into {semi,anti}join (A,B)
		 * and     {semi,anti}join (A, join(B,A)) into {semi,anti}join (A,B)
		 * Where the semi/anti join is done using the identity */
		if (on_identity && l->ref.refcnt == 2 && ((is_join(r->op) && (l == r->l || l == r->r)) ||
		   (is_project(r->op) && rl && is_join(rl->op) && (l == rl->l || l == rl->r)))){
			sql_rel *or = r;

			if (is_project(r->op))
				r = rl;

			if (l == r->r)
				rel->r = rel_dup(r->l);
			else
				rel->r = rel_dup(r->r);

			rel->exps = r->exps;
			r->exps = NULL;
			rel->attr = r->attr;
			r->attr = NULL;
			rel_destroy(or);
			v->changes++;
		}
	}
	{
		sql_rel *l = rel->l, *rl = NULL;
		sql_rel *r = rel->r, *or = r;

		if (r)
			rl = r->l;
		if (r && is_project(r->op)) {
			r = rl;
			if (r)
				rl = r->l;
		}

		/* More general case is (join reduction)
   		   {semi,anti}join (A, join(A,B) [A.c1 == B.c1]) [ A.c1 == B.c1 ]
		   into {semi,anti}join (A,B) [ A.c1 == B.c1 ]

		   for semijoin also A.c1 == B.k1 ] [ A.c1 == B.k2 ] could be rewritten
		 */
		if (l && r && rl &&
		    is_basetable(l->op) && is_basetable(rl->op) &&
		    is_join(r->op) && l->l == rl->l)
		{
			node *n, *m;
			list *exps;

			if (!rel->exps || !r->exps ||
		       	    list_length(rel->exps) != list_length(r->exps))
				return rel;
			exps = new_exp_list(v->sql->sa);

			/* are the join conditions equal */
			for (n = rel->exps->h, m = r->exps->h;
			     n && m; n = n->next, m = m->next)
			{
				sql_exp *le = NULL, *oe = n->data;
				sql_exp *re = NULL, *ne = m->data;
				sql_column *cl;
				int equal = 0;

				if (oe->type != e_cmp || ne->type != e_cmp ||
				    oe->flag != cmp_equal ||
				    ne->flag != cmp_equal || is_anti(oe) || is_anti(ne))
					return rel;

				if ((cl = exp_find_column(rel->l, oe->l, -2)) != NULL) {
					le = oe->l;
					re = oe->r;
				} else if ((cl = exp_find_column(rel->l, oe->r, -2)) != NULL) {
					le = oe->r;
					re = oe->l;
				} else
					return rel;

				if (exp_find_column(rl, ne->l, -2) == cl) {
					sql_exp *e = (or != r)?rel_find_exp(or, re):re;

					if (e)
						equal = exp_match_exp(ne->r, e);
					if (!e || !equal)
						return rel;
					re = ne->r;
				} else if (exp_find_column(rl, ne->r, -2) == cl) {
					sql_exp *e = (or != r)?rel_find_exp(or, re):re;

					if (e)
						equal = exp_match_exp(ne->l, e);
					if (!e || !equal)
						return rel;
					re = ne->l;
				} else
					return rel;

				ne = exp_compare(v->sql->sa, le, re, cmp_equal);
				append(exps, ne);
			}

			rel->r = rel_dup(r->r);
			rel->exps = exps;
			rel_destroy(or);
			v->changes++;
		}
	}
	return rel;
}

/*
 * Push semijoins down, pushes the semijoin through a join.
 *
 * semijoin( join(A, B) [ A.x == B.y ], C ) [ A.z == C.c ]
 * ->
 * join( semijoin(A, C) [ A.z == C.c ], B ) [ A.x == B.y ]
 *
 * also push simple expressions of a semijoin down if they only
 * involve the left sided of the semijoin.
 *
 * in some cases the other way is useful, ie push join down
 * semijoin. When the join reduces (ie when there are selects on it).
 *
 * At the moment, we only flag changes by this optimizer on the first level of optimization
 */
static inline sql_rel *
rel_push_semijoin_down_or_up(visitor *v, sql_rel *rel)
{
	uint8_t cycle = *(uint8_t*) v->data;

	if (rel->op == op_join && rel->exps && rel->l) {
		sql_rel *l = rel->l, *r = rel->r;

		if (is_semi(l->op) && !rel_is_ref(l) && is_select(r->op) && !rel_is_ref(r)) {
			rel->l = l->l;
			l->l = rel;
			if (cycle <= 0)
				v->changes++;
			return l;
		}
	}
	/* also case with 2 joins */
	/* join ( join ( semijoin(), table), select (table)); */
	if (rel->op == op_join && rel->exps && rel->l) {
		sql_rel *l = rel->l, *r = rel->r;
		sql_rel *ll;

		if (is_join(l->op) && !rel_is_ref(l) && is_select(r->op) && !rel_is_ref(r)) {
			ll = l->l;
			if (is_semi(ll->op) && !rel_is_ref(ll)) {
				l->l = ll->l;
				ll->l = rel;
				if (cycle <= 0)
					v->changes++;
				return ll;
			}
		}
	}
	/* first push down the expressions involving only A */
	if (rel->op == op_semi && rel->exps && rel->l) {
		sql_rel *jl = rel->l, *ojl = jl;

		set_processed(jl);
		for (node *n = rel->exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (n != rel->exps->h && e->type == e_cmp && rel_rebind_exp(v->sql, jl, e)) {
				if (!is_select(jl->op) || rel_is_ref(jl))
					rel->l = jl = rel_select(v->sql->sa, jl, NULL);
				rel_select_add_exp(v->sql->sa, jl, e);
				list_remove_node(rel->exps, NULL, n);
				if (cycle <= 0)
					v->changes++;
			}
			n = next;
		}
		if (ojl != jl)
			set_processed(jl);
	}
	if (rel->op == op_semi && rel->exps && rel->l) {
		operator_type op = rel->op, lop;
		node *n;
		sql_rel *l = rel->l, *ll = NULL, *lr = NULL;
		sql_rel *r = rel->r;
		list *exps = rel->exps, *nsexps, *njexps, *nsattr, *njattr;
		int left = 1, right = 1;

		/* handle project
		if (l->op == op_project && !need_distinct(l))
			l = l->l;
		*/

		if (!is_join(l->op) || is_full(l->op) || rel_is_ref(l) || is_single(l))
			return rel;

		lop = l->op;
		ll = l->l;
		lr = l->r;

		/* check which side is used and other exps are atoms or from right of semijoin */
		for(n = exps->h; n; n = n->next) {
			sql_exp *sje = n->data;

			if (sje->type != e_cmp || is_complex_exp(sje->flag))
				return rel;
			/* sje->l from ll and sje->r/f from semijoin r ||
			 * sje->l from semijoin r and sje->r/f from ll ||
			 * sje->l from lr and sje->r/f from semijoin r ||
			 * sje->l from semijoin r and sje->r/f from lr */
			if (left &&
			   ((rel_rebind_exp(v->sql, ll, sje->l) && rel_rebind_exp(v->sql, rel->r, sje->r) && (!sje->f || rel_rebind_exp(v->sql, rel->r, sje->f))) ||
			    (rel_rebind_exp(v->sql, rel->r, sje->l) && rel_rebind_exp(v->sql, ll, sje->r) && (!sje->f || rel_rebind_exp(v->sql, ll, sje->f)))))
				right = 0;
			else
				left = 0;
			if (right &&
			   ((rel_rebind_exp(v->sql, lr, sje->l) && rel_rebind_exp(v->sql, rel->r, sje->r) && (!sje->f || rel_rebind_exp(v->sql, rel->r, sje->f))) ||
			    (rel_rebind_exp(v->sql, rel->r, sje->l) && rel_rebind_exp(v->sql, lr, sje->r) && (!sje->f || rel_rebind_exp(v->sql, lr, sje->f)))))
				left = 0;
			else
				right = 0;
			if (!right && !left)
				return rel;
		}
		if (left && is_right(lop))
			return rel;
		if (right && is_left(lop))
			return rel;
		nsexps = exps_copy(v->sql, rel->exps);
		nsattr = exps_copy(v->sql, rel->attr);
		njexps = exps_copy(v->sql, l->exps);
		njattr = exps_copy(v->sql, l->attr);
		if (left)
			l = rel_crossproduct(v->sql->sa, rel_dup(ll), rel_dup(r), op);
		else
			l = rel_crossproduct(v->sql->sa, rel_dup(lr), rel_dup(r), op);
		l->exps = nsexps;
		l->attr = nsattr;
		set_processed(l);
		if (left)
			l = rel_crossproduct(v->sql->sa, l, rel_dup(lr), lop);
		else
			l = rel_crossproduct(v->sql->sa, rel_dup(ll), l, lop);
		l->exps = njexps;
		l->attr = njattr;
		set_processed(l);
		rel_destroy(rel);
		rel = l;
		if (cycle <= 0)
			v->changes++;
	}
	return rel;
}

/* antijoin(a, union(b,c)) -> antijoin(antijoin(a,b), c) */
static inline sql_rel *
rel_rewrite_antijoin(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l;
	sql_rel *r = rel->r;

	assert(rel->op == op_anti);
	if (l && !rel_is_ref(l) && r && !rel_is_ref(r) && is_munion(r->op) && !is_single(r)) {
		list *rels = r->l;
		if (list_length(rels) != 2)
			return rel;
		sql_rel *rl = rel_dup(rels->h->data), *nl;
		sql_rel *rr = rel_dup(rels->h->next->data);

		if (!is_project(rl->op))
			rl = rel_project(v->sql->sa, rl,
				rel_projections(v->sql, rl, NULL, 1, 1));
		if (!is_project(rr->op))
			rr = rel_project(v->sql->sa, rr,
				rel_projections(v->sql, rr, NULL, 1, 1));
		rel_rename_exps(v->sql, r->exps, rl->exps);
		rel_rename_exps(v->sql, r->exps, rr->exps);

		nl = rel_crossproduct(v->sql->sa, rel->l, rl, op_anti);
		nl->exps = exps_copy(v->sql, rel->exps);
		nl->attr = exps_copy(v->sql, rel->attr);
		set_processed(nl);
		rel->l = nl;
		rel->r = rr;
		rel_destroy(r);
		v->changes++;
		return rel;
	}
	return rel;
}

static sql_rel *
rel_optimize_semi_and_anti_(visitor *v, sql_rel *rel)
{
	/* rewrite semijoin (A, join(A,B)) into semijoin (A,B) */
	if (rel && is_semi(rel->op))
		rel = rel_rewrite_semijoin(v, rel);
	/* push semijoin through join */
	if (rel && (is_semi(rel->op) || is_innerjoin(rel->op)))
		rel = rel_push_semijoin_down_or_up(v, rel);
	/* antijoin(a, union(b,c)) -> antijoin(antijoin(a,b), c) */
	if (rel && rel->op == op_anti)
		rel = rel_rewrite_antijoin(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_semi_and_anti(visitor *v, global_props *gp, sql_rel *rel)
{
	v->data = &gp->opt_cycle;
	rel = rel_visitor_bottomup(v, rel, &rel_optimize_semi_and_anti_);
	v->data = gp;
	return rel;
}

run_optimizer
bind_optimize_semi_and_anti(visitor *v, global_props *gp)
{
	/* Important -> Re-write semijoins after rel_join_order */
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right]
		   || gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti]) && (flag & optimize_semi_and_anti) ? rel_optimize_semi_and_anti : NULL;
}


static sql_rel *
rel_semijoin_use_fk(visitor *v, sql_rel *rel)
{
	if (is_semi(rel->op) && rel->exps) {
		list *exps = rel->exps;
		list *rels = sa_list(v->sql->sa);

		rel->exps = NULL;
		append(rels, rel->l);
		append(rels, rel->r);
		(void) find_fk( v->sql, rels, exps);

		rel->exps = exps;
	}
	return rel;
}

/*
 * Push {semi}joins down, pushes the joins through group by expressions.
 * When the join is on the group by columns, we can push the joins left
 * under the group by. This should only be done, iff the new semijoin would
 * reduce the input table to the groupby. So there should be a reduction
 * (selection) on the table A and this should be propagated to the groupby via
 * for example a primary key.
 *
 * {semi}join( A, groupby( B ) [gbe][aggrs] ) [ gbe == A.x ]
 * ->
 * {semi}join( A, groupby( semijoin(B,A) [gbe == A.x] ) [gbe][aggrs] ) [ gbe == A.x ]
 */
static inline sql_rel *
rel_push_join_down(visitor *v, sql_rel *rel)
{
	if (!rel_is_ref(rel) && ((is_left(rel->op) || rel->op == op_join || is_semi(rel->op)) && rel->l && rel->exps)) {
		sql_rel *gb = rel->r, *ogb = gb, *l = NULL, *rell = rel->l;

		if (is_simple_project(gb->op) && !rel_is_ref(gb))
			gb = gb->l;

		if (rel_is_ref(rell) || !gb || rel_is_ref(gb))
			return rel;

		if (is_groupby(gb->op) && gb->r && list_length(gb->r)) {
			list *exps = rel->exps, *jes = new_exp_list(v->sql->sa), *gbes = gb->r;
			node *n, *m;
			/* find out if all group by expressions are used in the join */
			for(n = gbes->h; n; n = n->next) {
				sql_exp *gbe = n->data;
				int fnd = 0;
				const char *rname = NULL, *name = NULL;

				/* project in between, ie find alias */
				/* first find expression in expression list */
				gbe = exps_uses_exp( gb->exps, gbe);
				if (!gbe)
					continue;
				if (ogb != gb)
					gbe = exps_uses_exp( ogb->exps, gbe);
				if (gbe) {
					rname = exp_find_rel_name(gbe);
					name = exp_name(gbe);
				}

				if (!name)
					return rel;

				for (m = exps->h; m && !fnd; m = m->next) {
					sql_exp *je = m->data;

					if (je->card >= CARD_ATOM && je->type == e_cmp &&
					    !is_complex_exp(je->flag)) {
						/* expect right expression to match */
						sql_exp *r = je->r;

						if (r == 0 || r->type != e_column)
							continue;
						if (r->l && rname && strcmp(r->l, rname) == 0 && strcmp(r->r, name)==0) {
							fnd = 1;
						} else if (!r->l && !rname  && strcmp(r->r, name)==0) {
							fnd = 1;
						}
						if (fnd) {
							sql_exp *le = je->l;
							sql_exp *re = exp_push_down_prj(v->sql, r, gb, gb->l);
							if (!re || (list_length(jes) == 0 && !find_prop(le->p, PROP_HASHCOL))) {
								fnd = 0;
							} else {
								int anti = is_anti(je), semantics = is_semantics(je);

								je = exp_compare(v->sql->sa, le, re, je->flag);
								if (anti) set_anti(je);
								if (semantics) set_semantics(je);
								list_append(jes, je);
							}
						}
					}
				}
				if (!fnd)
					return rel;
			}
			l = rel_dup(rel->l);

			/* push join's left side (as semijoin) down group by */
			l = gb->l = rel_crossproduct(v->sql->sa, gb->l, l, op_semi);
			l->exps = jes;
			set_processed(l);
			v->changes++;
			return rel;
		}
	}
	return rel;
}

static bool
check_projection_on_foreignside(sql_rel *r, list *pexps, int fk_left)
{
	/* projection columns from the foreign side */
	if (list_empty(pexps))
		return true;
	for (node *n = pexps->h; n; n = n->next) {
		sql_exp *pe = n->data;

		if (pe && is_atom(pe->type))
			continue;
		if (pe && !is_alias(pe->type))
			return false;
		/* check for columns from the pk side, then keep the join with the pk */
		if ((fk_left && rel_find_exp(r->r, pe)) || (!fk_left && rel_find_exp(r->l, pe)))
			return false;
	}
	return true;
}

static sql_rel *
rel_simplify_project_fk_join(mvc *sql, sql_rel *r, list *pexps, list *orderexps, int *changes)
{
	sql_rel *rl = r->l, *rr = r->r, *nr = NULL;
	sql_exp *je, *le, *nje, *re;
	int fk_left = 1;

	/* check for foreign key join */
	if (list_length(r->exps) != 1 || !list_empty(r->attr))
		return r;
	if (!(je = exps_find_prop(r->exps, PROP_JOINIDX)) || je->flag != cmp_equal)
		return r;
	/* je->l == foreign expression, je->r == primary expression */
	if (rel_find_exp(r->l, je->l)) {
		fk_left = 1;
	} else if (rel_find_exp(r->r, je->l)) {
		fk_left = 0;
	} else { /* not found */
		return r;
	}

	/* primary side must be a full table */
	if ((fk_left && (!is_left(r->op) && !is_full(r->op)) && !is_basetable(rr->op)) ||
		(!fk_left && (!is_right(r->op) && !is_full(r->op)) && !is_basetable(rl->op)))
		return r;

	if (!check_projection_on_foreignside(r, pexps, fk_left) || !check_projection_on_foreignside(r, orderexps, fk_left))
		return r;

	/* rewrite, ie remove pkey side if possible */
	le = (sql_exp*)je->l, re = (sql_exp*)je->l;

	/* both have NULL and there are semantics, the join cannot be removed */
	if (is_semantics(je) && has_nil(le) && has_nil(re))
		return r;

	(*changes)++;
	/* if the foreign key column doesn't have NULL values, then return it */
	if (!has_nil(le) || is_full(r->op) || (fk_left && is_left(r->op)) || (!fk_left && is_right(r->op))) {
		/* if ->attr, introduce group by on index */
		if (fk_left) {
			nr = rel_dup(r->l);
		} else {
			nr = rel_dup(r->r);
		}
		if (!list_empty(r->attr)) {
			nr = rel_groupby(sql, nr, NULL);
			if (nr) {
				// printf("# introduced groupby  \n");
				nr->r = append(sa_list(sql->sa), le);
				nr->exps = r->attr;
			}
		}
		return nr;
	}

	/* remove NULL values, ie generate a select not null */
	nje = exp_compare(sql->sa, exp_ref(sql, le), exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(le), NULL, 0)), cmp_equal);
	set_anti(nje);
	set_has_no_nil(nje);
	set_semantics(nje);
	if (fk_left) {
		nr = rel_dup(r->l);
	} else {
		nr = rel_dup(r->r);
	}
	nr = rel_select(sql->sa, nr, nje);
	set_processed(nr);
	return nr;
}

static sql_rel *
rel_simplify_count_fk_join(mvc *sql, sql_rel *r, list *gexps, list *gcols, int *changes)
{
	sql_rel *rl = r->l, *rr = r->r, *nr = NULL;
	sql_exp *je, *le, *nje, *re, *oce;
	int fk_left = 1;

	/* check for foreign key join */
	if (list_length(r->exps) != 1)
		return r;
	if (!(je = exps_find_prop(r->exps, PROP_JOINIDX)) || je->flag != cmp_equal)
		return r;
	/* je->l == foreign expression, je->r == primary expression */
	if (rel_find_exp(r->l, je->l)) {
		fk_left = 1;
	} else if (rel_find_exp(r->r, je->l)) {
		fk_left = 0;
	} else { /* not found */
		return r;
	}

	oce = gexps->h->data;
	if (oce->l) /* we only handle COUNT(*) */
		return r;

	/* primary side must be a full table */
	if ((fk_left && (!is_left(r->op) && !is_full(r->op)) && !is_basetable(rr->op)) ||
		(!fk_left && (!is_right(r->op) && !is_full(r->op)) && !is_basetable(rl->op)))
		return r;

	if (fk_left && is_join(rl->op) && !rel_is_ref(rl)) {
		r->l = rel_simplify_count_fk_join(sql, rl, gexps, gcols, changes);
		if (rl != r->l)
			rel_destroy(rl);
	}
	if (!fk_left && is_join(rr->op) && !rel_is_ref(rr)) {
		r->r = rel_simplify_count_fk_join(sql, rr, gexps, gcols, changes);
		if (rr != r->r)
			rel_destroy(rr);
	}

	if (!check_projection_on_foreignside(r, gcols, fk_left))
		return r;

	/* rewrite, ie remove pkey side if possible */
	le = (sql_exp*)je->l, re = (sql_exp*)je->l;

	/* both have NULL and there are semantics, the join cannot be removed */
	if (is_semantics(je) && has_nil(le) && has_nil(re))
		return r;

	(*changes)++;
	/* if the foreign key column doesn't have NULL values, then return it */
	if (!has_nil(le) || is_full(r->op) || (fk_left && is_left(r->op)) || (!fk_left && is_right(r->op))) {
		if (fk_left) {
			nr = rel_dup(r->l);
		} else {
			nr = rel_dup(r->r);
		}
		return nr;
	}

	/* remove NULL values, ie generate a select not null */
	nje = exp_compare(sql->sa, exp_ref(sql, le), exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(le), NULL, 0)), cmp_equal);
	set_anti(nje);
	set_has_no_nil(nje);
	set_semantics(nje);
	if (fk_left) {
		nr = rel_dup(r->l);
	} else {
		nr = rel_dup(r->r);
	}
	nr = rel_select(sql->sa, nr, nje);
	set_processed(nr);
	return nr;
}

/*
 * Handle (left/right/outer/natural) join fk-pk rewrites
 *   1 group by ( fk-pk-join () ) [ count(*) ] -> group by ( fk )
 *   2 project ( fk-pk-join () ) [ fk-column ] -> project (fk table)[ fk-column ]
 *   3 project ( fk1-pk1-join( fk2-pk2-join()) [ fk-column, pk1 column ] -> project (fk1-pk1-join)[ fk-column, pk1 column ]
 */
static inline sql_rel *
rel_simplify_fk_joins(visitor *v, sql_rel *rel)
{
	sql_rel *r = NULL;

	if (is_simple_project(rel->op))
		r = rel->l;

	while (is_simple_project(rel->op) && r && list_length(r->exps) == 1 && (is_join(r->op) || r->op == op_semi) && !(rel_is_ref(r))) {
		sql_rel *or = r;

		r = rel_simplify_project_fk_join(v->sql, r, rel->exps, rel->r, &v->changes);
		if (r == or)
			return rel;
		rel_destroy(rel->l);
		rel->l = r;
	}

	if (!is_groupby(rel->op))
		return rel;

	r = rel->l;
	while(r && is_simple_project(r->op))
		r = r->l;

	while (is_groupby(rel->op) && !rel_is_ref(rel) && r && (is_join(r->op) || r->op == op_semi) && list_length(r->exps) == 1 && !(rel_is_ref(r)) &&
		   /* currently only single count aggregation is handled, no other projects or aggregation */
		   list_length(rel->exps) == 1 && exp_aggr_is_count(rel->exps->h->data)) {
		sql_rel *or = r;

		r = rel_simplify_count_fk_join(v->sql, r, rel->exps, rel->r, &v->changes);
		if (r == or)
			return rel;
		rel_destroy(rel->l);
		rel->l = r;
	}
	return rel;
}

/*
 * Gets the column expressions of a diff function and adds them to "columns".
 * The diff function has two possible argument types: either a sql_exp representing a column
 * or a sql_exp representing another diff function, therefore this function is recursive.
 */
static void
get_diff_function_columns(sql_exp *diffExp, list *columns) {
	list *args = diffExp->l;

	for (node *arg = args->h; arg; arg = arg->next) {
		sql_exp *exp = arg->data;

		// diff function
		if (exp->type == e_func) {
			get_diff_function_columns(exp, columns);
		}
		// column
		else {
			list_append(columns, exp);
		}
	}
}

/*
 * Builds a list of aggregation key columns to be used by the select push down algorithm, namely for
 * window functions. Returns NULL if the window function does not partition by any column
 */
static list *
get_partition_by_key_columns(allocator *sa, sql_rel *r) {
	for (node* n = r->exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_func) {
			sql_subfunc *f = e->f;

			// aggregation function
			if (!strcmp(f->func->base.name, "rank")) {
				list* rankArguments = e->l;
				// the partition key is the second argument
				sql_exp *partitionExp = rankArguments->h->next->data;

				// check if the key contains any columns, i.e., is a diff function
				if (partitionExp->type == e_func) {
					// get columns to list
					list *aggColumns = sa_list(sa);
					get_diff_function_columns(partitionExp, aggColumns);
					return aggColumns;
				}
				// the function has no aggregation columns (e_atom of boolean)
				else {
					return NULL;
				}

			}
		}
	}
	return NULL;
}

/*
static bool
rank_exp_has_partition_key(sql_exp *e)
{
	if (e->type == e_func) {
		sql_subfunc *f = e->f;

		if (f->func->type == F_ANALYTIC) {
			list *args = e->l;

			if (list_length(args) >= 2) { // the partition key is the second argument
				return true;
			}
		}
	}
	return false;
}
*/

/*
 * Checks if a filter column is also used as an aggregation key, so it can be later safely pushed down.
 */
static int
filter_column_in_partition_by_columns(sql_exp *column, list *keyColumns)
{
	/* check if it is a column or an e_convert, and get the actual column if it is the latter */
	if (column->type == e_convert) {
		column = column->l;
	}

	char *tableName = column->l;
	char *columnName = column->r;

	for (node *n = keyColumns->h; n; n = n->next) {
		sql_exp *keyCol = n->data;
		char *keyColTableName = keyCol->l;
		char *keyColColumnName = keyCol->r;

		if (!strcmp(tableName, keyColTableName) && !strcmp(columnName, keyColColumnName)) {
			/* match */
			return 1;
		}
	}

	/* no matches found */
	return 0;
}

/*
 * Push select down, pushes the selects through (simple) projections. Also
 * it cleans up the projections which become useless.
 *
 * WARNING - Make sure to call try_remove_empty_select macro before returning so we ensure
 * possible generated empty selects won't never be generated
 */
static sql_rel *
rel_push_select_down(visitor *v, sql_rel *rel)
{
	list *exps = NULL;
	sql_rel *r = NULL;
	node *n;

	if (rel_is_ref(rel)) {
		if (is_select(rel->op) && !list_empty(rel->exps)) {
			/* add inplace empty select */
			sql_rel *l = rel_select(v->sql->sa, rel->l, NULL);

			l->exps = rel->exps;
			set_processed(l);
			rel->exps = NULL;
			rel->l = l;
			v->changes++;
		}
		return rel;
	}

	/* don't make changes for empty selects */
	if (is_select(rel->op) && list_empty(rel->exps))
		return try_remove_empty_select(v, rel);

	/* merge 2 selects */
	r = rel->l;
	if (is_select(rel->op) && r && r->exps && is_select(r->op) && !(rel_is_ref(r)) && !exps_have_func(rel->exps)) {
		(void)list_merge(r->exps, rel->exps, (fdup)NULL);
		rel->l = NULL;
		rel_destroy(rel);
		v->changes++;
		return try_remove_empty_select(v, r);
	}
	/*
	 * Push select through semi/anti join
	 * 	select (semi(A,B)) == semi(select(A), B)
	 */
	if (is_select(rel->op) && r && is_semi(r->op) && !(rel_is_ref(r))) {
		rel->l = r->l;
		r->l = rel;
		v->changes++;
		/*
		 * if A has 2 references (ie used on both sides of
		 * the semi join), we also push the select into A.
		 */
		if (rel_is_ref(rel->l) && rel->l == rel_find_ref(r->r)){
			sql_rel *lx = rel->l;
			sql_rel *rx = r->r;
			if (lx->ref.refcnt == 2 && !rel_is_ref(rx)) {
				while (rx->l && !rel_is_ref(rx->l) &&
	      			       (is_project(rx->op) ||
					is_select(rx->op) ||
					is_join(rx->op)))
						rx = rx->l;
				/* probably we need to introduce a project */
				rel_destroy(rel->l);
				lx = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
				r->l = lx;
				rx->l = rel_dup(lx);
			}
		}
		return r;
	}
	exps = rel->exps;

	/* push select through join */
	if (is_select(rel->op) && r && is_join(r->op) && !rel_is_ref(r) && !is_single(r)){
		sql_rel *jl = r->l, *ojl = jl, *jr = r->r, *ojr = jr;
		int left = r->op == op_join || r->op == op_left;
		int right = r->op == op_join || r->op == op_right;

		if (r->op == op_full)
			return rel;

		/* introduce selects under the join (if needed) */
		set_processed(jl);
		set_processed(jr);
		for (n = exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (!exp_unsafe(e, false, true)) {
				if (left && rel_rebind_exp(v->sql, jl, e)) {
					if (!is_select(jl->op) || rel_is_ref(jl))
						r->l = jl = rel_select(v->sql->sa, jl, NULL);
					rel_select_add_exp(v->sql->sa, jl, e);
					list_remove_node(exps, NULL, n);
					v->changes++;
				} else if (right && rel_rebind_exp(v->sql, jr, e)) {
					if (!is_select(jr->op) || rel_is_ref(jr))
						r->r = jr = rel_select(v->sql->sa, jr, NULL);
					rel_select_add_exp(v->sql->sa, jr, e);
					list_remove_node(exps, NULL, n);
					v->changes++;
				}
			}
			n = next;
		}
		if (ojl != jl)
			set_processed(jl);
		if (ojr != jr)
			set_processed(jr);
	}

	/* merge select and cross product ? */
	if (is_select(rel->op) && r && r->op == op_join && !rel_is_ref(r) && !is_single(r) && !exps_have_unsafe(exps, false, true)) {
		for (n = exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (exp_is_join(e, NULL) == 0) {
				if (!r->exps)
					r->exps = sa_list(v->sql->sa);
				append(r->exps, e);
				list_remove_node(exps, NULL, n);
				v->changes++;
			}
			n = next;
		}
	}

	if (is_select(rel->op) && r && (is_simple_project(r->op) || (is_groupby(r->op) && !list_empty(r->r))) && !rel_is_ref(r) && !is_single(r)){
		sql_rel *pl = r->l, *opl = pl;
		/* we cannot push through window functions (for safety I disabled projects over DDL too) */
		if (pl && pl->op != op_ddl && !exps_have_unsafe(r->exps, false, false)) {
			/* introduce selects under the project (if needed) */
			set_processed(pl);
			if (!pl->exps)
				pl->exps = sa_list(v->sql->sa);
			for (n = exps->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data, *ne = NULL;

				/* can we move it down */
				if (e->type == e_cmp && (ne = exp_push_down_prj(v->sql, e, r, pl)) && ne != e) {
					if (!(is_select(pl->op) && is_join(pl->op) && is_semi(pl->op)) || rel_is_ref(pl))
						r->l = pl = rel_select(v->sql->sa, pl, NULL);
					rel_select_add_exp(v->sql->sa, pl, ne);
					list_remove_node(exps, NULL, n);
					v->changes++;
				}
				n = next;
			}
			if (opl != pl)
				set_processed(pl);
		}

		/* push filters if they match the partition by key on a window function */
		else if (pl && pl->op != op_ddl && exps_have_unsafe(r->exps, false, false)) {
			set_processed(pl);
			/* list of partition by key columns */
			list *keyColumns = get_partition_by_key_columns(v->sql->sa, r);

			/* partition by keys found, check if any filter matches them */
			if (keyColumns) {
				for (n = exps->h; n;) {
					node *next = n->next;
					sql_exp *e = n->data, *ne = NULL;

					if (e->type == e_cmp) {
						/* simple comparison filter */
						if (e->flag == cmp_gt || e->flag == cmp_gte || e->flag == cmp_lte || e->flag == cmp_lt
							|| e->flag == cmp_equal || e->flag == cmp_notequal || e->flag == cmp_in || e->flag == cmp_notin
							|| (e->flag == cmp_filter && ((list*)e->l)->cnt == 1)) {
							sql_exp* column;
							/* the column in 'like' filters is stored inside a list */
							if (e->flag == cmp_filter) {
								column = ((list*)e->l)->h->data;
							} else {
								column = e->l;
							}

							/* check if the expression matches any partition by key, meaning we can
							   try to safely push it down */
							if (filter_column_in_partition_by_columns(column, keyColumns)) {
								ne = exp_push_down_prj(v->sql, e, r, pl);

								/* can we move it down */
								if (ne && ne != e && pl->exps) {
									if (!is_select(pl->op) || rel_is_ref(pl))
										r->l = pl = rel_select(v->sql->sa, pl, NULL);
									rel_select_add_exp(v->sql->sa, pl, ne);
									list_remove_node(exps, NULL, n);
									v->changes++;
								}
							}
						}
					}
					n = next;
				}

				/* cleanup list */
				list_destroy(keyColumns);
			}
			/* also push (rewrite) limits on output of row_number/(*)rank like window functions */
			if (is_simple_project(r->op) /*&& is_simple_project(pl->op)*/) { /* possible window functions */
				for (n = exps->h; n; n = n->next) {
					sql_exp *e = n->data;

					if (e->type == e_cmp && (e->flag == cmp_lt || e->flag == cmp_lte) && exp_is_atom(e->r)) { /* simple limit */
						sql_exp *ranke = rel_find_exp(r, e->l);

						if (ranke && ranke->type == e_func) {
							sql_subfunc *rankf = ranke->f;
							if (rankf->func->type == F_ANALYTIC) { /* rank functions cannot have a frame */
								// For now only for rank/row_number without partition by
								sql_rel *tn = NULL;
							   	if (strcmp(rankf->func->base.name, "rank") == 0 && is_simple_project(pl->op) && pl->r /* &&
										!rank_exp_has_partition_key(ranke)*/) {
									tn = r->l = rel_topn(v->sql->sa, r->l, append(sa_list(v->sql->sa), e->r));
									tn->grouped = 1;
									v->changes++;
									break;
								}
							   	if (strcmp(rankf->func->base.name, "row_number") == 0 && list_empty(r->r) && !is_topn(pl->op) /*&&
										!rank_exp_has_partition_key(ranke)*/) {
									tn = r->l = rel_topn(v->sql->sa, r->l, append(sa_list(v->sql->sa), e->r));
									tn->grouped = 1;
									v->changes++;
									break;
								}
							}
						}
					}
				}
			}
		}
	}

	/* try push select under set relation */
	if (is_select(rel->op) && r && is_set(r->op) && !list_empty(r->exps) && !rel_is_ref(r) && !is_single(r) && !list_empty(exps)) {
		sql_rel *u = r, *ul = u->l, *ur = u->r;

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

		/* introduce selects under the set */
		ul = rel_select(v->sql->sa, ul, NULL);
		ul->exps = exps_copy(v->sql, exps);
		set_processed(ul);
		ur = rel_select(v->sql->sa, ur, NULL);
		ur->exps = exps_copy(v->sql, exps);
		set_processed(ur);

		rel = rel_inplace_setop(v->sql, rel, ul, ur, u->op, rel_projections(v->sql, rel, NULL, 1, 1));
		if (need_distinct(u))
			set_distinct(rel);
		v->changes++;
	}
	if (is_select(rel->op) && r && is_munion(r->op) && !is_recursive(r) && !list_empty(r->exps) && !rel_is_ref(r) && !is_single(r) && !list_empty(exps)) {
		sql_rel *u = r;
		list *rels = u->l, *nrels = sa_list(v->sql->sa);
		for(node *n = rels->h; n; n = n->next) {
			sql_rel *ul = n->data;
			ul = rel_dup(ul);
			if (!is_project(ul->op) || rel_is_ref(ul))
				ul = rel_project(v->sql->sa, ul,
					rel_projections(v->sql, ul, NULL, 1, 1));
			rel_rename_exps(v->sql, u->exps, ul->exps);

			/* introduce selects under the set */
			ul = rel_select(v->sql->sa, ul, NULL);
			ul->exps = exps_copy(v->sql, exps);
			set_processed(ul);
			nrels = append(nrels, ul);
		}

		rel = rel_inplace_setop_n_ary(v->sql, rel, nrels, u->op, rel_projections(v->sql, rel, NULL, 1, 1));
		if (need_distinct(u))
			set_distinct(rel);
		if (is_recursive(u))
			set_recursive(rel);
		v->changes++;
	}

	return try_remove_empty_select(v, rel);
}

static int
index_exp(sql_exp *e, sql_idx *i)
{
	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		switch(i->type) {
		case hash_idx:
		case oph_idx:
			if (e->flag == cmp_equal)
				return 0;
			/* fall through */
		case join_idx:
		default:
			return -1;
		}
	}
	return -1;
}

/* find column for the select/join expression */
static sql_column *
sjexp_col(sql_exp *e, sql_rel *r)
{
	sql_column *res = NULL;

	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		res = exp_find_column(r, e->l, -2);
		if (!res)
			res = exp_find_column(r, e->r, -2);
	}
	return res;
}

static sql_idx *
find_index(allocator *sa, sql_rel *rel, sql_rel *sub, list **EXPS)
{
	node *n;

	/* any (partial) match of the expressions with the index columns */
	/* Depending on the index type we may need full matches and only
	   limited number of cmp types (hash only equality etc) */
	/* Depending on the index type we should (in the rel_bin) generate
	   more code, ie for spatial index add post filter etc, for hash
	   compute hash value and use index */

	if (sub->exps && rel->exps)
	for(n = sub->exps->h; n; n = n->next) {
		prop *p;
		sql_exp *e = n->data;

		if ((p = find_prop(e->p, PROP_HASHIDX)) != NULL) {
			list *exps, *cols;
			sql_idx *i = p->value.pval;
			fcmp cmp = (fcmp)&sql_column_kc_cmp;

			/* join indices are only interesting for joins */
			if (i->type == join_idx || list_length(i->columns) <= 1)
				continue;
			/* based on the index type, find qualifying exps */
			exps = list_select(rel->exps, i, (fcmp) &index_exp, (fdup)NULL);
			if (list_empty(exps))
				continue;
			/* now we obtain the columns, move into sql_column_kc_cmp! */
			cols = list_map(exps, sub, (fmap) &sjexp_col);

			/* TODO check that at most 2 relations are involved */

			/* Match the index columns with the expression columns.
			   TODO, Allow partial matches ! */
			if (list_match(cols, i->columns, cmp) == 0) {
				/* re-order exps in index order */
				node *n, *m;
				list *es = sa_list(sa);

				for(n = i->columns->h; n; n = n->next) {
					int i = 0;
					for(m = cols->h; m; m = m->next, i++) {
						if (cmp(m->data, n->data) == 0){
							sql_exp *e = list_fetch(exps, i);
							list_append(es, e);
							break;
						}
					}
				}
				/* fix the destroy function */
				cols->destroy = NULL;
				*EXPS = es;
				e->used = 1;
				return i;
			}
			cols->destroy = NULL;
		}
	}
	return NULL;
}

static inline sql_rel *
rel_use_index(visitor *v, sql_rel *rel)
{
	list *exps = NULL;
	sql_idx *i = find_index(v->sql->sa, rel, rel->l, &exps);
	int left = 1;

	assert(is_select(rel->op) || is_join(rel->op));
	if (!i && is_join(rel->op)) {
		left = 0;
		i = find_index(v->sql->sa, rel, rel->r, &exps);
	}

	if (i) {
		prop *p;
		node *n;
		int single_table = 1;
		sql_exp *re = NULL;

		for( n = exps->h; n && single_table; n = n->next) {
			sql_exp *e = n->data, *nre = e->l;

			if (!is_compare(e->type) || is_anti(e) || e->flag != cmp_equal)
				return rel;
			if (is_join(rel->op) && ((left && !rel_find_exp(rel->l, nre)) || (!left && rel_find_exp(rel->r, nre))))
				nre = e->r;
			single_table = (!re || (exp_relname(nre) && exp_relname(re) && strcmp(exp_relname(nre), exp_relname(re)) == 0));
			re = nre;
		}
		if (single_table) { /* add PROP_HASHCOL to all column exps */
			for( n = exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				/* swapped ? */
				if (is_join(rel->op) && ((left && !rel_find_exp(rel->l, e->l)) || (!left && !rel_find_exp(rel->r, e->l)))) {
					exp_swap(e);
				}
				p = find_prop(e->p, PROP_HASHCOL);
				if (!p)
					e->p = p = prop_create(v->sql->sa, PROP_HASHCOL, e->p);
				p->value.pval = i;
			}
		}
		/* add the remaining exps to the new exp list */
		if (list_length(rel->exps) > list_length(exps)) {
			for( n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (!list_find(exps, e, (fcmp)&exp_cmp))
					list_append(exps, e);
			}
		}
		rel->exps = exps;
	}
	return rel;
}

static sql_rel *
rel_select_leftgroup_2_semi(visitor *v, sql_rel *rel)
{
	if (rel_is_ref(rel) || !is_select(rel->op) || list_empty(rel->exps))
		return rel;
	sql_rel *l = rel->l;

	if (!l || rel_is_ref(l) || !is_left(l->op) || list_empty(l->attr))
		return rel;

	for(node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && !is_semantics(e) && !e->f) {
			list *attrs = l->attr;
			sql_exp *a = attrs->h->data;

			if (exps_find_exp(l->attr, e->l) && exp_is_true(e->r) && e->flag == cmp_equal /*&& exp_is_true(a)*/) {
				// printf("# optimize select leftgroup -> semi\n");
				if (!list_empty(l->exps)) {
					for(node *m = l->exps->h; m; m = m->next) {
						sql_exp *j = m->data;
						reset_any(j);
					}
				}
				l->attr = NULL;
				l->op = exp_is_true(a)?op_semi:op_anti;
				list_remove_node(rel->exps, NULL, n);
				rel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
				list_append(rel->exps, attrs->h->data);
				v->changes++;
				return rel;
			}
		}
	}
	return rel;
}

static sql_rel *
rel_optimize_select_and_joins_topdown_(visitor *v, sql_rel *rel)
{
	/* push_join_down introduces semijoins */
	uint8_t cycle = *(uint8_t*) v->data;
	if (cycle <= 0) {
		rel = rel_semijoin_use_fk(v, rel);
		rel = rel_push_join_down(v, rel);
	}

	rel = rel_simplify_fk_joins(v, rel);
	rel = rel_push_select_down(v, rel);
	rel = rel_select_leftgroup_2_semi(v, rel);
	if (rel && rel->l && (is_select(rel->op) || is_join(rel->op)))
		rel = rel_use_index(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_select_and_joins_topdown(visitor *v, global_props *gp, sql_rel *rel)
{
	v->data = &gp->opt_cycle;
	rel = rel_visitor_topdown(v, rel, &rel_optimize_select_and_joins_topdown_);
	v->data = gp;
	return rel;
}

run_optimizer
bind_optimize_select_and_joins_topdown(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right]
		   || gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti] ||
		   gp->cnt[op_select]) && (flag & optimize_select_and_joins_topdown) ? rel_optimize_select_and_joins_topdown : NULL;
}


static int
can_push_func(sql_exp *e, sql_rel *rel, int *must, int depth)
{
	switch(e->type) {
	case e_cmp: {
		sql_exp *l = e->l, *r = e->r, *f = e->f;

		/* don't push down functions inside attribute joins */
		if (e->flag == cmp_con || e->flag == cmp_dis || e->flag == cmp_in || e->flag == cmp_notin || e->flag == cmp_filter || (is_join(rel->op) && is_any(e)))
			return 0;
		if (depth > 0) { /* for comparisons under the top ones, they become functions */
			int lmust = 0;
			int res = can_push_func(l, rel, &lmust, depth + 1) && can_push_func(r, rel, &lmust, depth + 1) &&
					(!f || can_push_func(f, rel, &lmust, depth + 1));
			if (res && !lmust)
				return 1;
			(*must) |= lmust;
			return res;
		} else {
			int mustl = 0, mustr = 0, mustf = 0;
			return ((l->type == e_column || can_push_func(l, rel, &mustl, depth + 1)) && (*must = mustl)) ||
					((r->type == e_column || can_push_func(r, rel, &mustr, depth + 1)) && (*must = mustr)) ||
					((f && (f->type == e_column || can_push_func(f, rel, &mustf, depth + 1)) && (*must = mustf)));
		}
	}
	case e_convert:
		return can_push_func(e->l, rel, must, depth + 1);
	case e_aggr:
	case e_func: {
		list *l = e->l;
		int res = 1, lmust = 0;

		if (exp_unsafe(e, false, false))
			return 0;
		if (l) for (node *n = l->h; n && res; n = n->next)
			res &= can_push_func(n->data, rel, &lmust, depth + 1);
		if (res && !lmust)
			return 1;
		(*must) |= lmust;
		return res;
	}
	case e_column:
		if (rel && !rel_find_exp(rel, e))
			return 0;
		(*must) = 1;
		/* fall through */
	default:
		return 1;
	}
}

static int
exps_can_push_func(list *exps, sql_rel *rel)
{
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		int mustl = 0, mustr = 0;

		if ((is_joinop(rel->op) || is_select(rel->op)) && ((can_push_func(e, rel->l, &mustl, 0) && mustl)))
			return 1;
		if (is_joinop(rel->op) && can_push_func(e, rel->r, &mustr, 0) && mustr)
			return 1;
	}
	return 0;
}

static int
exp_needs_push_down(sql_rel *rel, sql_exp *e)
{
	switch(e->type) {
	case e_cmp:
		/* don't push down functions inside attribute joins */
		if (e->flag == cmp_con || e->flag == cmp_dis || e->flag == cmp_in || e->flag == cmp_notin || e->flag == cmp_filter || (is_join(rel->op) && is_any(e)))
			return 0;
		return exp_needs_push_down(rel, e->l) || exp_needs_push_down(rel, e->r) || (e->f && exp_needs_push_down(rel, e->f));
	case e_convert:
		return exp_needs_push_down(rel, e->l);
	case e_aggr:
	case e_func:
		if (!e->l || exps_are_atoms(e->l))
			return 0;
		return 1;
	case e_atom:
		if (!e->f || exps_are_atoms(e->f))
			return 0;
		return 1;
	case e_column:
	default:
		return 0;
	}
}

static int
exps_need_push_down(sql_rel *rel, list *exps )
{
	for(node *n = exps->h; n; n = n->next)
		if (exp_needs_push_down(rel, n->data))
			return 1;
	return 0;
}

static sql_exp *exp_push_single_func_down(visitor *v, sql_rel *rel, sql_rel *ol, sql_rel *or, sql_exp *e, int depth);

static list *
exps_push_single_func_down(visitor *v, sql_rel *rel, sql_rel *ol, sql_rel *or, list *exps, int depth)
{
	if (mvc_highwater(v->sql))
		return exps;

	for (node *n = exps->h; n; n = n->next)
		if ((n->data = exp_push_single_func_down(v, rel, ol, or, n->data, depth)) == NULL)
			return NULL;
	return exps;
}

static sql_exp *
exp_push_single_func_down(visitor *v, sql_rel *rel, sql_rel *ol, sql_rel *or, sql_exp *e, int depth)
{
	if (mvc_highwater(v->sql))
		return e;

	switch(e->type) {
	case e_cmp: {
		if (e->flag == cmp_filter) {
			if ((e->l = exps_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
				return NULL;
			if ((e->r = exps_push_single_func_down(v, rel, ol, or, e->r, depth + 1)) == NULL)
				return NULL;
		} else if (e->flag == cmp_con || e->flag == cmp_dis) {
			if ((e->l = exps_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
				return NULL;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if ((e->l = exp_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
				return NULL;
			if ((e->r = exps_push_single_func_down(v, rel, ol, or, e->r, depth + 1)) == NULL)
				return NULL;
		} else {
			if ((e->l = exp_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
				return NULL;
			if ((e->r = exp_push_single_func_down(v, rel, ol, or, e->r, depth + 1)) == NULL)
				return NULL;
			if (e->f && (e->f = exp_push_single_func_down(v, rel, ol, or, e->f, depth + 1)) == NULL)
				return NULL;
		}
	} break;
	case e_convert:
		if ((e->l = exp_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
			return NULL;
		break;
	case e_aggr:
	case e_func: {
		sql_rel *l = rel->l, *r = rel->r;
		int must = 0, mustl = 0, mustr = 0;

		if (exp_unsafe(e, false, false))
			return e;
		if (!e->l || exps_are_atoms(e->l))
			return e;
		if ((is_joinop(rel->op) && ((can_push_func(e, l, &mustl, depth + 1) && mustl) || (can_push_func(e, r, &mustr, depth + 1) && mustr))) ||
			(is_select(rel->op) && can_push_func(e, l, &must, depth + 1) && must)) {
			exp_label(v->sql->sa, e, ++v->sql->label);
			/* we need a full projection, group by's and unions cannot be extended with more expressions */
			if (mustr) {
				if (r == or) /* don't project twice */
					rel->r = r = rel_project(v->sql->sa, r, rel_projections(v->sql, r, NULL, 1, 1));
				list_append(r->exps, e);
			} else {
				if (l == ol) /* don't project twice */
					rel->l = l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
				list_append(l->exps, e);
			}
			e = exp_ref(v->sql, e);
			v->changes++;
		}
	} break;
	case e_atom: {
		if (e->f && (e->f = exps_push_single_func_down(v, rel, ol, or, e->f, depth + 1)) == NULL)
			return NULL;
	} break;
	case e_column:
	case e_psm:
		break;
	}
	return e;
}

static inline sql_rel *
rel_push_func_down(visitor *v, sql_rel *rel)
{
	if ((is_select(rel->op) || is_joinop(rel->op)) && rel->l && rel->exps && !(rel_is_ref(rel))) {
		int changes = v->changes;
		sql_rel *l = rel->l, *r = rel->r;

		/* only push down when is useful */
		if ((is_select(rel->op) && list_length(rel->exps) <= 1) || rel_is_ref(l) || (is_joinop(rel->op) && rel_is_ref(r)))
			return rel;
		if (exps_can_push_func(rel->exps, rel) && exps_need_push_down(rel, rel->exps) && !exps_push_single_func_down(v, rel, l, r, rel->exps, 0))
			return NULL;
		if (v->changes > changes) /* once we get a better join order, we can try to remove this projection */
			return rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
	}
	if (is_simple_project(rel->op) && rel->l && rel->exps) {
		sql_rel *pl = rel->l;

		if (is_joinop(pl->op) && exps_can_push_func(rel->exps, rel)) {
			sql_rel *l = pl->l, *r = pl->r, *ol = l, *or = r;

			for (node *n = rel->exps->h; n; ) {
				node *next = n->next;
				sql_exp *e = n->data;
				int mustl = 0, mustr = 0;

				if ((can_push_func(e, l, &mustl, 0) && mustl) || (can_push_func(e, r, &mustr, 0) && mustr)) {
					if (mustl) {
						if (l == ol) /* don't project twice */
							pl->l = l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
						list_append(l->exps, e);
						list_remove_node(rel->exps, NULL, n);
						v->changes++;
					} else {
						if (r == or) /* don't project twice */
							pl->r = r = rel_project(v->sql->sa, r, rel_projections(v->sql, r, NULL, 1, 1));
						list_append(r->exps, e);
						list_remove_node(rel->exps, NULL, n);
						v->changes++;
					}
				}
				n = next;
			}
		}
	}
	return rel;
}

static sql_rel *
rel_push_func_and_select_down_(visitor *v, sql_rel *rel)
{
	if (rel)
		rel = rel_push_func_down(v, rel);
	if (rel)
		rel = rel_push_select_down(v, rel);
	return rel;
}

static sql_rel *
rel_push_func_and_select_down(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	return rel_visitor_topdown(v, rel, &rel_push_func_and_select_down_);
}

run_optimizer
bind_push_func_and_select_down(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right]
			|| gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti] || gp->cnt[op_select])
			&& (flag & push_func_and_select_down) ? rel_push_func_and_select_down : NULL;
}
