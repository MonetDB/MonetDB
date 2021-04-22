/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_unnest.h"
#include "sql_semantic.h"
#include "sql_mvc.h"
#include "rel_rewriter.h"

void
rel_set_exps(sql_rel *rel, list *exps)
{
	rel->exps = exps;
	rel->nrcols = list_length(exps);
}

/* some projections results are order dependend (row_number etc) */
int
project_unsafe(sql_rel *rel, int allow_identity)
{
	sql_rel *sub = rel->l;
	node *n;

	if (need_distinct(rel) || rel->r /* order by */)
		return 1;
	if (!rel->exps)
		return 0;
	/* projects without sub and projects around ddl's cannot be changed */
	if (!sub || (sub && sub->op == op_ddl))
		return 1;
	for(n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data, *ne;

		/* aggr func in project ! */
		if (exp_unsafe(e, allow_identity))
			return 1;
		ne = rel_find_exp(rel, e);
		if (ne && ne != e) /* no self referencing */
			return 1;
	}
	return 0;
}

/* we don't name relations directly, but sometimes we need the relation
   name. So we look it up in the first expression

   we should clean up (remove) this function.
 */
const char *
rel_name( sql_rel *r )
{
	if (is_basetable(r->op))
		return rel_base_name(r);
	if (!is_project(r->op) && !is_base(r->op) && r->l)
		return rel_name(r->l);
	if (r->exps && list_length(r->exps)) {
		sql_exp *e = r->exps->h->data;
		if (exp_relname(e))
			return exp_relname(e);
		if (e->type == e_column)
			return e->l;
	}
	return NULL;
}

sql_rel *
rel_distinct(sql_rel *l)
{
	set_distinct(l);
	return l;
}

sql_rel *
rel_dup(sql_rel *r)
{
	sql_ref_inc(&r->ref);
	return r;
}

static void
rel_destroy_(sql_rel *rel)
{
	if (!rel)
		return;
	if (is_join(rel->op) ||
	    is_semi(rel->op) ||
	    is_select(rel->op) ||
	    is_set(rel->op) ||
	    is_topn(rel->op) ||
		is_sample(rel->op)) {
		if (rel->l)
			rel_destroy(rel->l);
		if (rel->r)
			rel_destroy(rel->r);
	} else if (is_simple_project(rel->op) || is_groupby(rel->op)) {
		if (rel->l)
			rel_destroy(rel->l);
	} else if (is_modify(rel->op)) {
		if (rel->r)
			rel_destroy(rel->r);
	}
}

void
rel_destroy(sql_rel *rel)
{
	if (!rel)
		return;
	if (sql_ref_dec(&rel->ref) > 0)
		return;
	rel_destroy_(rel);
}

sql_rel*
rel_create(sql_allocator *sa)
{
	sql_rel *r = SA_NEW(sa, sql_rel);
	if(!r)
		return NULL;

	*r = (sql_rel) {
		.card = CARD_ATOM,
	};
	sql_ref_init(&r->ref);
	return r;
}

sql_rel *
rel_copy(mvc *sql, sql_rel *i, int deep)
{
	sql_rel *rel;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	rel = rel_create(sql->sa);
	if (!rel)
		return NULL;

	rel->l = NULL;
	rel->r = NULL;
	rel->card = i->card;
	rel->flag = i->flag;

	switch(i->op) {
	case op_basetable:
		rel->l = i->l;
		break;
	case op_table:
		rel->l = i->l;
		rel->r = i->r;
		break;
	case op_project:
	case op_groupby:
		if (i->l)
			rel->l = rel_copy(sql, i->l, deep);
		if (i->r) {
			if (!deep) {
				rel->r = list_dup(i->r, (fdup) NULL);
			} else {
				rel->r = exps_copy(sql, i->r);
			}
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (i->l)
				rel->l = rel_copy(sql, i->l, deep);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (i->l)
				rel->l = rel_copy(sql, i->l, deep);
			if (i->r)
				rel->r = rel_copy(sql, i->r, deep);
		}
		break;
	case op_select:
	case op_topn:
	case op_sample:
	case op_truncate:
		if (i->l)
			rel->l = rel_copy(sql, i->l, deep);
		break;
	case op_insert:
	case op_update:
	case op_delete:

	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:

	case op_union:
	case op_inter:
	case op_except:
		if (i->l)
			rel->l = rel_copy(sql, i->l, deep);
		if (i->r)
			rel->r = rel_copy(sql, i->r, deep);
		break;
	}
	rel->op = i->op;
	rel->exps = (!i->exps)?NULL:deep?exps_copy(sql, i->exps):list_dup(i->exps, (fdup)NULL);
	return rel;
}

sql_rel *
rel_select_copy(sql_allocator *sa, sql_rel *l, list *exps)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;

	rel->l = l;
	rel->r = NULL;
	rel->op = op_select;
	rel->exps = exps?list_dup(exps, (fdup)NULL):NULL;
	rel->card = CARD_ATOM; /* no relation */
	if (l) {
		rel->card = l->card;
		rel->nrcols = l->nrcols;
	}
	return rel;
}

sql_exp *
rel_bind_column( mvc *sql, sql_rel *rel, const char *cname, int f, int no_tname)
{
	int ambiguous = 0, multi = 0;

	if (!rel)
		return NULL;
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if ((is_project(rel->op) || is_base(rel->op))) {
		sql_exp *e = NULL;

		if (is_base(rel->op) && !rel->exps)
			return rel_base_bind_column(sql, rel, cname, no_tname);
		if (!list_empty(rel->exps)) {
			e = exps_bind_column(rel->exps, cname, &ambiguous, &multi, no_tname);
			if (ambiguous || multi)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", cname);
			if (!e && is_groupby(rel->op) && rel->r) {
				e = exps_bind_alias(rel->r, NULL, cname);
				if (e) {
					e = exps_bind_column(rel->r, cname, &ambiguous, &multi, no_tname);
					if (ambiguous || multi)
						return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", cname);
					return e;
				}
			}
		}
		if (!e && (is_sql_sel(f) || is_sql_having(f) || !f) && is_groupby(rel->op) && rel->r) {
			e = exps_bind_column(rel->r, cname, &ambiguous, &multi, no_tname);
			if (ambiguous || multi)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", cname);
			if (e) {
				e = exp_ref(sql, e);
				e->card = rel->card;
				return e;
			}
		}
		if (e)
			return exp_alias_or_copy(sql, exp_relname(e), cname, rel, e);
	}
	if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l) {
		if (!is_processed(rel))
			return rel_bind_column(sql, rel->l, cname, f, no_tname);
	} else if (is_set(rel->op)) {
		assert(is_processed(rel));
		return NULL;
	} else if (is_join(rel->op)) {
		sql_exp *e1 = rel_bind_column(sql, rel->l, cname, f, no_tname), *e2 = NULL;

		if (e1 && (is_right(rel->op) || is_full(rel->op)))
			set_has_nil(e1);
		if (!e1 || !is_freevar(e1)) {
			e2 = rel_bind_column(sql, rel->r, cname, f, no_tname);
			if (e2 && (is_left(rel->op) || is_full(rel->op)))
				set_has_nil(e2);
			if (e1 && e2 && !is_dependent(rel))
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", cname);
		}
		return e1 ? e1 : e2;
	} else if (is_semi(rel->op) ||
		   is_select(rel->op) ||
		   is_topn(rel->op) ||
		   is_sample(rel->op)) {
		if (rel->l)
			return rel_bind_column(sql, rel->l, cname, f, no_tname);
	}
	return NULL;
}

sql_exp *
rel_bind_column2( mvc *sql, sql_rel *rel, const char *tname, const char *cname, int f)
{
	int ambiguous = 0, multi = 0;

	if (!rel)
		return NULL;
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if ((is_project(rel->op) || is_base(rel->op))) {
		sql_exp *e = NULL;

		if (is_basetable(rel->op) && !rel->exps)
			return rel_base_bind_column2(sql, rel, tname, cname);
		/* in case of orderby we should also lookup the column in group by list (and use existing references) */
		if (!list_empty(rel->exps)) {
			e = exps_bind_column2(rel->exps, tname, cname, &multi);
			if (multi)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s.%s' ambiguous",
								 tname, cname);
			if (!e && is_groupby(rel->op) && rel->r) {
				e = exps_bind_alias(rel->r, tname, cname);
				if (e) {
					const char *rname = exp_relname(e), *nname = exp_name(e);
					if (rname)
						e = exps_bind_column2(rel->exps, rname, nname, &multi);
					else
						e = exps_bind_column(rel->exps, nname, &ambiguous, &multi, 0);
					if (ambiguous || multi)
						return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s%s%s' ambiguous",
										 rname ? rname : "", rname ? "." : "", nname);
					if (e)
						return e;
				}
			}
		}
		if (!e && (is_sql_sel(f) || is_sql_having(f) || !f) && is_groupby(rel->op) && rel->r) {
			e = exps_bind_column2(rel->r, tname, cname, &multi);
			if (multi)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s.%s' ambiguous",
								 tname, cname);
			if (e) {
				e = exp_ref(sql, e);
				e->card = rel->card;
				return e;
			}
		}
		if (e)
			return exp_alias_or_copy(sql, tname, cname, rel, e);
	}
	if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l) {
		if (!is_processed(rel))
			return rel_bind_column2(sql, rel->l, tname, cname, f);
	} else if (is_set(rel->op)) {
		assert(is_processed(rel));
		return NULL;
	} else if (is_join(rel->op)) {
		sql_exp *e = rel_bind_column2(sql, rel->l, tname, cname, f);

		if (e && (is_right(rel->op) || is_full(rel->op)))
			set_has_nil(e);
		if (!e) {
			e = rel_bind_column2(sql, rel->r, tname, cname, f);
			if (e && (is_left(rel->op) || is_full(rel->op)))
				set_has_nil(e);
		}
		return e;
	} else if (is_semi(rel->op) ||
		   is_select(rel->op) ||
		   is_topn(rel->op) ||
		   is_sample(rel->op)) {
		if (rel->l)
			return rel_bind_column2(sql, rel->l, tname, cname, f);
	}
	return NULL;
}

sql_exp *
rel_first_column(mvc *sql, sql_rel *r)
{
	if (is_simple_project(r->op))
		return r->exps->h->data;

	list *exps = rel_projections(sql, r, NULL, 1, 1);

	if (!list_empty(exps))
		return exps->h->data;

	return NULL;
}

sql_rel *
rel_inplace_setop(mvc *sql, sql_rel *rel, sql_rel *l, sql_rel *r, operator_type setop, list *exps)
{
	rel_destroy_(rel);
	rel->l = l;
	rel->r = r;
	rel->op = setop;
	rel->card = CARD_MULTI;
	rel->flag = 0;
	rel_setop_set_exps(sql, rel, exps);
	set_processed(rel);
	return rel;
}

sql_rel *
rel_inplace_project(sql_allocator *sa, sql_rel *rel, sql_rel *l, list *e)
{
	if (!l) {
		l = rel_create(sa);
		if(!l)
			return NULL;

		*l = *rel;
		l->ref.refcnt = 1;
	} else {
		rel_destroy_(rel);
	}
	set_processed(rel);

	rel->l = l;
	rel->r = NULL;
	rel->op = op_project;
	rel->exps = e;
	rel->card = CARD_MULTI;
	rel->flag = 0;
	if (l) {
		rel->nrcols = l->nrcols;
		assert (exps_card(rel->exps) <= rel->card);
	}
	return rel;
}

sql_rel *
rel_inplace_groupby(sql_rel *rel, sql_rel *l, list *groupbyexps, list *exps )
{
	rel_destroy_(rel);
	rel->card = CARD_ATOM;
	if (groupbyexps)
		rel->card = CARD_AGGR;
	rel->l = l;
	rel->r = groupbyexps;
	rel->exps = exps;
	rel->nrcols = l->nrcols;
	rel->op = op_groupby;
	rel->flag = 0;
	set_processed(rel);
	return rel;
}

sql_rel *
rel_setop(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type setop)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;
	rel->l = l;
	rel->r = r;
	rel->op = setop;
	rel->exps = NULL;
	rel->card = is_union(setop) ? CARD_MULTI : l->card;
	assert(l->nrcols == r->nrcols);
	rel->nrcols = l->nrcols;
	return rel;
}

sql_rel *
rel_setop_check_types(mvc *sql, sql_rel *l, sql_rel *r, list *ls, list *rs, operator_type op)
{
	list *nls = new_exp_list(sql->sa);
	list *nrs = new_exp_list(sql->sa);
	node *n, *m;

	if(!nls || !nrs)
		return NULL;

	for (n = ls->h, m = rs->h; n && m; n = n->next, m = m->next) {
		sql_exp *le = n->data;
		sql_exp *re = m->data;

		if ((rel_convert_types(sql, l, r, &le, &re, 1, type_set) < 0))
			return NULL;
		append(nls, le);
		append(nrs, re);
	}
	l = rel_project(sql->sa, l, nls);
	r = rel_project(sql->sa, r, nrs);
	set_processed(l);
	set_processed(r);
	return rel_setop(sql->sa, l, r, op);
}

void
rel_setop_set_exps(mvc *sql, sql_rel *rel, list *exps)
{
	sql_rel *l = rel->l, *r = rel->r;
	list *lexps = l->exps, *rexps = r->exps;

	if (!is_project(l->op))
		lexps = rel_projections(sql, l, NULL, 0, 1);
	if (!is_project(r->op))
		rexps = rel_projections(sql, r, NULL, 0, 1);

	assert(is_set(rel->op) && list_length(lexps) == list_length(rexps) && list_length(exps) == list_length(lexps));

	for (node *n = exps->h, *m = lexps->h, *o = rexps->h ; m && n && o ; n = n->next, m = m->next,o = o->next) {
		sql_exp *e = n->data, *f = m->data, *g = o->data;

		if (is_union(rel->op)) { /* propagate set_has_no_nil only if it's applicable to both sides of the union*/
			if (has_nil(f) || has_nil(g))
				set_has_nil(e);
			else
				set_has_no_nil(e);
			e->p = NULL; /* remove all the properties on unions */
			e->card = MAX(f->card, g->card);
		} else
			e->card = f->card;
	}
	rel->nrcols = l->nrcols;
	rel->exps = exps;
}

sql_rel *
rel_crossproduct(sql_allocator *sa, sql_rel *l, sql_rel *r, operator_type join)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;

	rel->l = l;
	rel->r = r;
	rel->op = join;
	rel->exps = NULL;
	rel->card = CARD_MULTI;
	rel->nrcols = l->nrcols + r->nrcols;
	rel->single = r->single;
	if (r->single)
		reset_single(r);
	return rel;
}

sql_exp *
rel_is_constant(sql_rel **R, sql_exp *e)
{
	sql_rel *rel = *R;

	if (rel && rel->op == op_project && list_length(rel->exps) == 1 &&
	    !rel->l && !rel->r && !rel_is_ref(rel) && e->type == e_column) {
		sql_exp *ne = rel_find_exp(rel, e);
		if (ne) {
			rel_destroy(rel);
			*R = NULL;
			return ne;
		}
	}
	return e;
}

sql_rel *
rel_topn(sql_allocator *sa, sql_rel *l, list *exps )
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;

	rel->l = l;
	rel->r = NULL;
	rel->op = op_topn;
	rel->exps = exps;
	rel->card = l->card;
	rel->nrcols = l->nrcols;
	return rel;
}

sql_rel *
rel_sample(sql_allocator *sa, sql_rel *l, list *exps )
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;

	rel->l = l;
	rel->r = NULL;
	rel->op = op_sample;
	rel->exps = exps;
	rel->card = l->card;
	rel->nrcols = l->nrcols;
	return rel;
}

sql_rel *
rel_label( mvc *sql, sql_rel *r, int all)
{
	int nr = ++sql->label;
	char tname[16], *tnme;
	char cname[16], *cnme = NULL;

	tnme = number2name(tname, sizeof(tname), nr);
	if (!is_project(r->op)) {
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
		set_processed(r);
	}
	if (is_project(r->op) && r->exps) {
		node *ne = r->exps->h;

		list_hash_clear(r->exps);
		for (; ne; ne = ne->next) {
			sql_exp *e = ne->data;

			if (!e->freevar) {
				if (all) {
					nr = ++sql->label;
					cnme = number2name(cname, sizeof(cname), nr);
				}
				exp_setname(sql->sa, e, tnme, cnme );
			}
		}
	}
	/* op_projects can have a order by list */
	if (r->op == op_project && r->r) {
		list *exps = r->r;
		node *ne = exps->h;

		list_hash_clear(exps);
		for (; ne; ne = ne->next) {
			if (all) {
				nr = ++sql->label;
				cnme = number2name(cname, sizeof(cname), nr);
			}
			exp_setname(sql->sa, ne->data, tnme, cnme );
		}
	}
	return r;
}

sql_exp *
rel_project_add_exp( mvc *sql, sql_rel *rel, sql_exp *e)
{
	assert(is_project(rel->op));

	if (!exp_name(e))
		exp_label(sql->sa, e, ++sql->label);
	if (is_simple_project(rel->op)) {
		sql_rel *l = rel->l;
		if (!rel->exps)
			rel->exps = new_exp_list(sql->sa);
		if (l && is_groupby(l->op) && exp_card(e) <= CARD_ATOM && list_empty(l->exps))
			e = rel_project_add_exp(sql, l, e);
		if (e->card > rel->card)
			rel->card = e->card;
		append(rel->exps, e);
		rel->nrcols++;
	} else if (is_groupby(rel->op)) {
		return rel_groupby_add_aggr(sql, rel, e);
	}
	e = exp_ref(sql, e);
	return e;
}

sql_rel *
rel_select_add_exp(sql_allocator *sa, sql_rel *l, sql_exp *e)
{
	if ((l->op != op_select && !is_outerjoin(l->op)) || rel_is_ref(l))
		return rel_select(sa, l, e);

/* 	allow during AST->relational for bool expresssions as well
	if (e->type != e_cmp && e->card > CARD_ATOM) {
		sql_exp *t = exp_atom_bool(sa, 1);
		e = exp_compare(sa, e, t, cmp_equal);
	}
*/
	if (!l->exps)
		l->exps = new_exp_list(sa);
	append(l->exps, e);
	return l;
}

void
rel_join_add_exp( sql_allocator *sa, sql_rel *rel, sql_exp *e)
{
	assert(is_join(rel->op) || is_semi(rel->op) || is_select(rel->op));

	if (!rel->exps)
		rel->exps = new_exp_list(sa);
	append(rel->exps, e);
	if (e->card > rel->card)
		rel->card = e->card;
}

sql_exp *
rel_groupby_add_aggr(mvc *sql, sql_rel *rel, sql_exp *e)
{
	sql_exp *m = NULL, *ne;

	if (list_empty(rel->r))
		rel->card = e->card = CARD_ATOM;

	if ((m=exps_any_match(rel->exps, e)) == NULL) {
		if (!exp_name(e))
			exp_label(sql->sa, e, ++sql->label);
		append(rel->exps, e);
		rel->nrcols++;
		m = e;
	}
	ne = exp_ref(sql, m);
	return ne;
}

sql_rel *
rel_select(sql_allocator *sa, sql_rel *l, sql_exp *e)
{
	sql_rel *rel;

	if (l && is_outerjoin(l->op) && !is_processed(l)) {
		if (e) {
			if (!l->exps)
				l->exps = new_exp_list(sa);
			append(l->exps, e);
		}
		return l;
	}

	if (l && is_select(l->op) && !rel_is_ref(l)) { /* refine old select */
		if (e)
			rel_select_add_exp(sa, l, e);
		return l;
	}
	rel = rel_create(sa);
	if(!rel)
		return NULL;

	rel->l = l;
	rel->r = NULL;
	rel->op = op_select;
	rel->exps = new_exp_list(sa);
	if (e)
		rel_select_add_exp(sa, rel, e);
	rel->card = CARD_ATOM; /* no relation */
	if (l) {
		rel->card = l->card;
		rel->nrcols = l->nrcols;
		if (is_single(l))
			set_single(rel);
	}
	return rel;
}

sql_rel *
rel_groupby(mvc *sql, sql_rel *l, list *groupbyexps )
{
	sql_rel *rel = rel_create(sql->sa);
	list *aggrs = new_exp_list(sql->sa);
	node *en;
	if(!rel || !aggrs) {
		rel_destroy(rel);
		return NULL;
	}

	rel->card = CARD_ATOM;
	/* reduce duplicates in groupbyexps */
	if (groupbyexps && list_length(groupbyexps) > 1) {
		list *gexps = sa_list(sql->sa);

		for (en = groupbyexps->h; en; en = en->next) {
			sql_exp *e = en->data;

			if (!exps_any_match_same_or_no_alias(gexps, e))
				append(gexps, e);
		}
		groupbyexps = gexps;
	}

	if (groupbyexps) {
		rel->card = CARD_AGGR;
		for (en = groupbyexps->h; en; en = en->next) {
			sql_exp *e = en->data, *ne;

			/* after the group by the cardinality reduces */
			e->card = MIN(e->card, rel->card); /* if the column is an atom, the cardinality should not change */
			ne = exp_ref(sql, e);
			ne = exp_propagate(sql->sa, ne, e);
			append(aggrs, ne);
		}
	}
	rel->l = l;
	rel->r = groupbyexps;
	rel->exps = aggrs;
	rel->nrcols = aggrs?list_length(aggrs):0;
	rel->op = op_groupby;
	rel->grouped = 1;
	return rel;
}

sql_rel *
rel_project(sql_allocator *sa, sql_rel *l, list *e)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;

	rel->l = l;
	rel->r = NULL;
	rel->op = op_project;
	rel->exps = e;
	rel->card = exps_card(e);
	if (l) {
		rel->card = l->card;
		if (e)
			rel->nrcols = list_length(e);
		else
			rel->nrcols = l->nrcols;
		rel->single = l->single;
	}
	if (e && !list_empty(e)) {
		set_processed(rel);
		rel->nrcols = list_length(e);
	}
	return rel;
}

sql_rel*
rel_project_exp(sql_allocator *sa, sql_exp *e)
{
	sql_rel *rel = rel_project(sa, NULL, append(new_exp_list(sa), e));

	return rel;
}

sql_rel *
rel_exception(sql_allocator *sa, sql_rel *l, sql_rel *r, list *exps)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;
	rel->l = l;
	rel->r = r;
	rel->exps = exps;
	rel->op = op_ddl;
	rel->flag = ddl_exception;
	return rel;
}

sql_rel *
rel_relational_func(sql_allocator *sa, sql_rel *l, list *exps)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;

	rel->flag = 1;
	rel->l = l;
	rel->op = op_table;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = list_length(exps);
	return rel;
}

sql_rel *
rel_table_func(sql_allocator *sa, sql_rel *l, sql_exp *f, list *exps, int kind)
{
	sql_rel *rel = rel_create(sa);
	if(!rel)
		return NULL;

	assert(kind > 0);
	rel->flag = kind;
	rel->l = l; /* relation before call */
	rel->r = f; /* expression (table func call) */
	rel->op = op_table;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = list_length(exps);
	return rel;
}

static void
exps_has_nil(list *exps)
{
	node *m;

	for (m = exps->h; m; m = m->next) {
		sql_exp *e = m->data;

		set_has_nil(e);
	}
}

list *
_rel_projections(mvc *sql, sql_rel *rel, const char *tname, int settname, int intern, int basecol /* basecol only */ )
{
	list *lexps, *rexps, *exps;

	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return new_exp_list(sql->sa);

	if (!tname && is_basetable(rel->op) && !is_processed(rel))
		rel_base_use_all( sql, rel);

	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = _rel_projections(sql, rel->l, tname, settname, intern, basecol);
		if (rel->op == op_full || rel->op == op_right)
			exps_has_nil(exps);
		rexps = _rel_projections(sql, rel->r, tname, settname, intern, basecol);
		if (rel->op == op_full || rel->op == op_left)
			exps_has_nil(rexps);
		exps = list_merge( exps, rexps, (fdup)NULL);
		return exps;
	case op_groupby:
		if (list_empty(rel->exps) && rel->r) {
			list *r = rel->r;
			int label = 0;

			if (!settname)
				label = ++sql->label;
			exps = new_exp_list(sql->sa);
			for (node *en = r->h; en; en = en->next) {
				sql_exp *e = en->data;

				if (basecol && !is_basecol(e))
					continue;
				if (intern || !is_intern(e)) {
					append(exps, e = exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
					if (!settname) /* noname use alias */
						exp_setrelname(sql->sa, e, label);

				}
			}
			return exps;
		}
		/* fall through */
	case op_project:
	case op_basetable:
	case op_table:

	case op_union:
	case op_except:
	case op_inter:
		if (is_basetable(rel->op) && !rel->exps)
			return rel_base_projection(sql, rel, intern);
		if (rel->exps) {
			int label = 0;

			if (!settname)
				label = ++sql->label;
			exps = new_exp_list(sql->sa);
			for (node *en = rel->exps->h; en; en = en->next) {
				sql_exp *e = en->data;

				if (basecol && !is_basecol(e))
					continue;
				if (intern || !is_intern(e)) {
					append(exps, e = exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
					if (!settname) /* noname use alias */
						exp_setrelname(sql->sa, e, label);

				}
			}
			return exps;
		}
		lexps = _rel_projections(sql, rel->l, tname, settname, intern, basecol);
		rexps = _rel_projections(sql, rel->r, tname, settname, intern, basecol);
		exps = sa_list(sql->sa);
		if (lexps && rexps && exps) {
			int label = 0;

			if (!settname)
				label = ++sql->label;
			for (node *en = lexps->h, *ren = rexps->h; en && ren; en = en->next, ren = ren->next) {
				sql_exp *e = en->data;
				e->card = rel->card;
				if (!settname) /* noname use alias */
					exp_setrelname(sql->sa, e, label);
				append(exps, e);
			}
		}
		return exps;
	case op_ddl:
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
	case op_sample:
		return _rel_projections(sql, rel->l, tname, settname, intern, basecol);
	default:
		return NULL;
	}
}

list *
rel_projections(mvc *sql, sql_rel *rel, const char *tname, int settname, int intern)
{
	assert(tname == NULL);
	return _rel_projections(sql, rel, tname, settname, intern, 0);
}

/* find the path to the relation containing the base of the expression
	(e_column), in most cases this means go down the join tree and
	find the base column.
 */
static int
rel_bind_path_(mvc *sql, sql_rel *rel, sql_exp *e, list *path )
{
	int found = 0;

	if (mvc_highwater(sql)) {
		sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}

	switch (rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		/* first right (possible subquery) */
		found = rel_bind_path_(sql, rel->r, e, path);
		if (!found)
			found = rel_bind_path_(sql, rel->l, e, path);
		break;
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
	case op_sample:
		found = rel_bind_path_(sql, rel->l, e, path);
		break;

	case op_basetable:
		if (e->l)
			found = (rel_base_bind_column2_(rel, e->l, e->r) != NULL);
		else
			found = (rel_base_bind_column_(rel, e->r) != NULL);
		break;
	case op_union:
	case op_inter:
	case op_except:
		if (!rel->exps) {
			found = rel_bind_path_(sql, rel->l, e, path);
			assert(0);
			break;
		}
		/* fall through */
	case op_groupby:
	case op_project:
	case op_table:
		if (!rel->exps)
			break;
		if (!found && e->l && exps_bind_column2(rel->exps, e->l, e->r, NULL))
			found = 1;
		if (!found && !e->l && exps_bind_column(rel->exps, e->r, NULL, NULL, 1))
			found = 1;
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		break;
	case op_ddl:
		break;
	}
	if (found)
		list_prepend(path, rel);
	return found;
}

static list *
rel_bind_path(mvc *sql, sql_rel *rel, sql_exp *e, list *path)
{
	if (!path)
		return NULL;

	if (e->type == e_convert)
		path = rel_bind_path(sql, rel, e->l, path);
	else if (e->type == e_column) {
		if (rel) {
			if (!rel_bind_path_(sql, rel, e, path)) {
				/* something is wrong */
				return NULL;
			}
		}
		return path;
	}
	/* default the top relation */
	append(path, rel);
	return path;
}

static sql_rel *
rel_select_push_exp_down(mvc *sql, sql_rel *rel, sql_exp *e)
{
	sql_rel *r = rel->l, *jl = r->l, *jr = r->r;
	int left = r->op == op_join || r->op == op_left;
	int right = r->op == op_join || r->op == op_right;
	int done = 0;

	assert(is_select(rel->op));
	if (!is_full(r->op) && !is_single(r)) {
		if (left && rel_rebind_exp(sql, jl, e)) {
			done = 1;
			r->l = jl = rel_select_add_exp(sql->sa, jl, e);
		} else if (right && rel_rebind_exp(sql, jr, e)) {
			done = 1;
			r->r = jr = rel_select_add_exp(sql->sa, jr, e);
		}
	}
	if (!done)
		rel_select_add_exp(sql->sa, rel, e);
	return rel;
}

/* ls is the left expression of the select, rs is a simple atom, e is the
   select expression.
 */
sql_rel *
rel_push_select(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *e, int f)
{
	list *l = rel_bind_path(sql, rel, ls, sa_list(sql->sa));
	node *n;
	sql_rel *lrel = NULL, *p = NULL;

	if (!l)
		return NULL;
	if (is_sql_or(f)) /* expression has no clear parent relation, so filter current with it */
		return rel_select(sql->sa, rel, e);

	for (n = l->h; n; n = n->next ) {
		lrel = n->data;

		if (rel_is_ref(lrel))
			break;

		/* push down as long as the operators allow this */
		if (!is_select(lrel->op) &&
		    !(is_semi(lrel->op) && !rel_is_ref(lrel->l)) &&
		    lrel->op != op_join &&
		    lrel->op != op_left)
			break;
		/* pushing through left head of a left join is allowed */
		if (lrel->op == op_left && (!n->next || lrel->l != n->next->data))
			break;
		p = lrel;
	}
	if (!lrel)
		return NULL;
	if (p && is_select(p->op) && !rel_is_ref(p)) { /* refine old select */
		p = rel_select_push_exp_down(sql, p, e);
	} else {
		sql_rel *n = rel_select(sql->sa, lrel, e);

		if (p && p != lrel) {
			assert(p->op == op_join || p->op == op_left || is_semi(p->op));
			if (p->l == lrel) {
				p->l = n;
			} else {
				p->r = n;
			}
		} else {
			if (rel != lrel)
				assert(0);
			rel = n;
		}
	}
	return rel;
}

/* ls and rs are the left and right expression of the join, e is the
   join expression.
 */
sql_rel *
rel_push_join(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, sql_exp *e, int f)
{
	list *l = rel_bind_path(sql, rel, ls, sa_list(sql->sa));
	list *r = rel_bind_path(sql, rel, rs, sa_list(sql->sa));
	list *r2 = NULL;
	node *ln, *rn;
	sql_rel *lrel = NULL, *rrel = NULL, *rrel2 = NULL, *p = NULL;

	if (rs2)
		r2 = rel_bind_path(sql, rel, rs2, sa_list(sql->sa));
	if (!l || !r || (rs2 && !r2))
		return NULL;

	if (is_sql_or(f))
		return rel_push_select(sql, rel, ls, e, f);

	p = rel;
	if (r2) {
		node *rn2;

		for (ln = l->h, rn = r->h, rn2 = r2->h; ln && rn && rn2; ln = ln->next, rn = rn->next, rn2 = rn2->next ) {
			lrel = ln->data;
			rrel = rn->data;
			rrel2 = rn2->data;

			if (rel_is_ref(lrel) || rel_is_ref(rrel) || rel_is_ref(rrel2) || is_processed(lrel) || is_processed(rrel))
				break;

			/* push down as long as the operators allow this
				and the relation is equal.
			*/
			if (lrel != rrel || lrel != rrel2 ||
				(!is_select(lrel->op) &&
				 !(is_semi(lrel->op) && !rel_is_ref(lrel->l)) &&
				 lrel->op != op_join &&
				 lrel->op != op_left))
				break;
			/* pushing through left head of a left join is allowed */
			if (lrel->op == op_left && (!ln->next || lrel->l != ln->next->data))
				break;
			p = lrel;
		}
	} else {
		for (ln = l->h, rn = r->h; ln && rn; ln = ln->next, rn = rn->next ) {
			lrel = ln->data;
			rrel = rn->data;

			if (rel_is_ref(lrel) || rel_is_ref(rrel) || is_processed(lrel) || is_processed(rrel))
				break;

			/* push down as long as the operators allow this
				and the relation is equal.
			*/
			if (lrel != rrel ||
				(!is_select(lrel->op) &&
				 !(is_semi(lrel->op) && !rel_is_ref(lrel->l)) &&
				 lrel->op != op_join &&
				 lrel->op != op_left))
				break;
			/* pushing through left head of a left join is allowed */
			if (lrel->op == op_left && (!ln->next || lrel->l != ln->next->data))
				break;
			p = lrel;
		}
	}
	if (!lrel || !rrel || (r2 && !rrel2))
		return NULL;

	/* filter on columns of this relation */
	if ((lrel == rrel && (!r2 || lrel == rrel2) && lrel->op != op_join) || rel_is_ref(p)) {
		if (is_select(lrel->op) && !rel_is_ref(lrel)) {
			lrel = rel_select_push_exp_down(sql, lrel, e);
		} else if (p && is_select(p->op) && !rel_is_ref(p)) {
			p = rel_select_push_exp_down(sql, p, e);
		} else {
			sql_rel *n = rel_select(sql->sa, lrel, e);

			if (p && p != lrel) {
				if (p->l == lrel)
					p->l = n;
				else
					p->r = n;
			} else {
				rel = n;
			}
		}
		return rel;
	}

	rel_join_add_exp( sql->sa, p, e);
	return rel;
}

sql_rel *
rel_or(mvc *sql, sql_rel *rel, sql_rel *l, sql_rel *r, list *oexps, list *lexps, list *rexps)
{
	sql_rel *ll = l->l, *rl = r->l;
	list *ls, *rs;

	assert(!lexps || l == r);
	if (l == r && lexps) { /* merge both lists */
		sql_exp *e = exp_or(sql->sa, lexps, rexps, 0);
		list *nl = oexps?oexps:new_exp_list(sql->sa);

		rel_destroy(r);
		append(nl, e);
		if (is_outerjoin(l->op) && is_processed(l))
			l = rel_select(sql->sa, l, NULL);
		l->exps = nl;
		return l;
	}

	/* favor or expressions over union */
	if (l->op == r->op && is_select(l->op) &&
	    ll == rl && ll == rel && !rel_is_ref(l) && !rel_is_ref(r)) {
		sql_exp *e = exp_or(sql->sa, l->exps, r->exps, 0);
		list *nl = new_exp_list(sql->sa);

		rel_destroy(r);
		append(nl, e);
		l->exps = nl;

		/* merge and expressions */
		ll = l->l;
		while (ll && is_select(ll->op) && !rel_is_ref(ll)) {
			list_merge(l->exps, ll->exps, (fdup)NULL);
			l->l = ll->l;
			ll->l = NULL;
			rel_destroy(ll);
			ll = l->l;
		}
		return l;
	}

	if (rel) {
		ls = rel_projections(sql, rel, NULL, 1, 1);
		rs = rel_projections(sql, rel, NULL, 1, 1);
	} else {
		ls = rel_projections(sql, l, NULL, 1, 1);
		rs = rel_projections(sql, r, NULL, 1, 1);
	}
	set_processed(l);
	set_processed(r);
	rel = rel_setop_check_types(sql, l, r, ls, rs, op_union);
	if (!rel)
		return NULL;
	rel_setop_set_exps(sql, rel, rel_projections(sql, rel, NULL, 1, 1));
	set_processed(rel);
	rel->nrcols = list_length(rel->exps);
	rel = rel_distinct(rel);
	if (!rel)
		return NULL;
	if (exps_card(l->exps) <= CARD_AGGR &&
	    exps_card(r->exps) <= CARD_AGGR)
	{
		rel->card = exps_card(l->exps);
		exps_fix_card( rel->exps, rel->card);
	}
	return rel;
}

sql_table *
rel_ddl_table_get(sql_rel *r)
{
	if (r->flag == ddl_alter_table || r->flag == ddl_create_table || r->flag == ddl_create_view) {
		sql_exp *e = r->exps->t->data;
		atom *a = e->l;

		return a->data.val.pval;
	}
	return NULL;
}

sql_rel *
rel_ddl_basetable_get(sql_rel *r)
{
	if (r->flag == ddl_alter_table || r->flag == ddl_create_table || r->flag == ddl_create_view) {
		return r->l;
	}
	return NULL;
}

static sql_exp *
exps_find_identity(list *exps, sql_rel *p)
{
	node *n;

	for (n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (is_identity(e, p))
			return e;
	}
	return NULL;
}

static sql_rel *
_rel_add_identity(mvc *sql, sql_rel *rel, sql_exp **exp)
{
	list *exps = rel_projections(sql, rel, NULL, 1, 1);
	sql_exp *e;

	if (list_empty(exps)) {
		*exp = NULL;
		return rel;
	}
	if (!is_simple_project(rel->op) || !list_empty(rel->r))
		rel = rel_project(sql->sa, rel, exps);
	e = rel->exps->h->data;
	e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), rel->card, has_nil(e), is_intern(e));
	e = exp_unop(sql->sa, e, sql_bind_func(sql, "sys", "identity", exp_subtype(e), NULL, F_FUNC));
	set_intern(e);
	e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
	*exp = exp_label(sql->sa, e, ++sql->label);
	(void) rel_project_add_exp(sql, rel, e);
	return rel;
}

sql_rel *
rel_add_identity(mvc *sql, sql_rel *rel, sql_exp **exp)
{
	if (rel && is_project(rel->op) && (*exp = exps_find_identity(rel->exps, rel->l)) != NULL)
		return rel;
	return _rel_add_identity(sql, rel, exp);
}

sql_rel *
rel_add_identity2(mvc *sql, sql_rel *rel, sql_exp **exp)
{
	sql_rel *l = rel, *p = rel;

	if (rel && is_project(rel->op) && (*exp = exps_find_identity(rel->exps, rel->l)) != NULL)
		return rel;
	while(l && !is_set(l->op) && rel_has_freevar(sql, l) && l->l) {
		p = l;
		l = l->l;
	}
	if (l != p) {
		sql_rel *o = rel;
		sql_exp *id;

		p->l = _rel_add_identity(sql, l, exp);
		l = p->l;
		id = exp_ref(sql, *exp);
		while (o && o != l) {
			*exp = id;
			if (is_project(o->op))
				rel_project_add_exp(sql, o, id);
			o = o->l;
		}
		return rel;
	}
	return _rel_add_identity(sql, rel, exp);
}

sql_exp *
rel_find_column( sql_allocator *sa, sql_rel *rel, const char *tname, const char *cname )
{
	if (!rel)
		return NULL;

	if (rel->exps && (is_project(rel->op) || is_base(rel->op))) {
		int ambiguous = 0, multi = 0;
		sql_exp *e = exps_bind_column2(rel->exps, tname, cname, &multi);
		if (!e && cname[0] == '%' && !tname)
			e = exps_bind_column(rel->exps, cname, &ambiguous, &multi, 0);
		if (e && !ambiguous && !multi)
			return exp_alias(sa, exp_relname(e), exp_name(e), exp_relname(e), cname, exp_subtype(e), e->card, has_nil(e), is_intern(e));
	}
	if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l) {
		if (!is_processed(rel))
			return rel_find_column(sa, rel->l, tname, cname);
	} else if (is_set(rel->op)) {
		assert(is_processed(rel));
		return NULL;
	} else if (is_join(rel->op)) {
		sql_exp *e = rel_find_column(sa, rel->l, tname, cname);

		if (e && (is_right(rel->op) || is_full(rel->op)))
			set_has_nil(e);
		if (!e) {
			e = rel_find_column(sa, rel->r, tname, cname);
			if (e && (is_left(rel->op) || is_full(rel->op)))
				set_has_nil(e);
		}
		return e;
	} else if (is_semi(rel->op) ||
		   is_select(rel->op) ||
		   is_topn(rel->op) ||
		   is_sample(rel->op)) {
		if (rel->l)
			return rel_find_column(sa, rel->l, tname, cname);
	}
	return NULL;
}

int
rel_in_rel(sql_rel *super, sql_rel *sub)
{
	if (!super)
		return 0;
	if (super == sub)
		return 1;
	if (is_join(super->op) || is_semi(super->op) || is_set(super->op) || is_modify(super->op) || is_ddl(super->op))
		return rel_in_rel(super->l, sub) || rel_in_rel(super->r, sub);
	if (is_select(super->op) || is_simple_project(super->op) || is_groupby(super->op) || is_topn(super->op) || is_sample(super->op))
		return rel_in_rel(super->l, sub);
	return 0;
}

sql_rel*
rel_parent(sql_rel *rel)
{
	if (rel->l && (is_project(rel->op) || is_topn(rel->op) || is_sample(rel->op))) {
		sql_rel *l = rel->l;
		if (is_project(l->op))
			return l;
	}
	return rel;
}

sql_exp *
lastexp(sql_rel *rel)
{
	if (!is_processed(rel) || is_topn(rel->op) || is_sample(rel->op))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	assert(is_project(rel->op));
	return rel->exps->t->data;
}

sql_rel *
rel_return_zero_or_one(mvc *sql, sql_rel *rel, exp_kind ek)
{
	if (ek.card < card_set && rel->card > CARD_ATOM) {
		list *exps = rel->exps;

		assert (is_simple_project(rel->op) || is_set(rel->op));
		rel = rel_groupby(sql, rel, NULL);
		for(node *n = exps->h; n; n=n->next) {
			sql_exp *e = n->data;
			if (!has_label(e))
				exp_label(sql->sa, e, ++sql->label);
			sql_subtype *t = exp_subtype(e); /* parameters don't have a type defined, for those use 'void' one */
			sql_subfunc *zero_or_one = sql_bind_func(sql, "sys", "zero_or_one", t ? t : sql_bind_localtype("void"), NULL, F_AGGR);

			e = exp_ref(sql, e);
			e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, has_nil(e));
			(void)rel_groupby_add_aggr(sql, rel, e);
		}
	}
	return rel;
}

sql_rel *
rel_zero_or_one(mvc *sql, sql_rel *rel, exp_kind ek)
{
	if (is_topn(rel->op) || is_sample(rel->op))
		rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 0));
	if (ek.card < card_set && rel->card > CARD_ATOM) {
		assert (is_simple_project(rel->op) || is_set(rel->op));

		list *exps = rel->exps;
		for(node *n = exps->h; n; n=n->next) {
			sql_exp *e = n->data;
			if (!has_label(e))
				exp_label(sql->sa, e, ++sql->label);
		}
		set_single(rel);
	} else {
		sql_exp *e = lastexp(rel);
		if (!has_label(e))
			exp_label(sql->sa, e, ++sql->label);
	}
	return rel;
}

static sql_rel *
refs_find_rel(list *refs, sql_rel *rel)
{
	node *n;

	for(n=refs->h; n; n = n->next->next) {
		sql_rel *ref = n->data;
		sql_rel *s = n->next->data;

		if (rel == ref)
			return s;
	}
	return NULL;
}

static int exp_deps(mvc *sql, sql_exp *e, list *refs, list *l);

static int
exps_deps(mvc *sql, list *exps, list *refs, list *l)
{

	for(node *n = exps->h; n; n = n->next)
		if (exp_deps(sql, n->data, refs, l) != 0)
			return -1;
	return 0;
}

static int
id_cmp(sqlid *id1, sqlid *id2)
{
	if (*id1 == *id2)
		return 0;
	return -1;
}

static list *
cond_append(list *l, sqlid *id)
{
	if (*id >= FUNC_OIDS && !list_find(l, id, (fcmp) &id_cmp))
		 list_append(l, id);
	return l;
}

static int rel_deps(mvc *sql, sql_rel *r, list *refs, list *l);

static int
exp_deps(mvc *sql, sql_exp *e, list *refs, list *l)
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return -1;
	}

	switch(e->type) {
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			return exp_deps(sql, e->l, refs, l);
		} else if (e->flag & PSM_VAR) {
			return 0;
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			if (exp_deps(sql, e->l, refs, l) != 0 || exps_deps(sql, e->r, refs, l) != 0)
				return -1;
			if (e->flag & PSM_IF && e->f)
				return exps_deps(sql, e->f, refs, l);
		} else if (e->flag & PSM_REL) {
			sql_rel *rel = e->l;
			return rel_deps(sql, rel, refs, l);
		}
		break;
	case e_atom:
		if (e->f && exp_deps(sql, e->f, refs, l) != 0)
			return -1;
		break;
	case e_column:
		break;
	case e_convert:
		return exp_deps(sql, e->l, refs, l);
	case e_func: {
		sql_subfunc *f = e->f;

		if (e->l && exps_deps(sql, e->l, refs, l) != 0)
			return -1;
		cond_append(l, &f->func->base.id);
		if (e->l && list_length(e->l) == 2 && strcmp(f->func->base.name, "next_value_for") == 0) {
			/* add dependency on seq nr */
			list *nl = e->l;
			sql_exp *schname = nl->h->data, *seqname = nl->t->data;
			char *sch_name = is_atom(schname->type) && schname->l ? ((atom*)schname->l)->data.val.sval : NULL;
			char *seq_name = is_atom(seqname->type) && seqname->l ? ((atom*)seqname->l)->data.val.sval : NULL;

			if (sch_name && seq_name) {
				sql_schema *sche = mvc_bind_schema(sql, sch_name);
				if (sche) {
					sql_sequence *seq = find_sql_sequence(sql->session->tr, sche, seq_name);
					if (seq)
						cond_append(l, &seq->base.id);
				}
			}
		}
	} break;
	case e_aggr: {
		sql_subfunc *a = e->f;

		if (e->l && exps_deps(sql, e->l, refs, l) != 0)
			return -1;
		cond_append(l, &a->func->base.id);
	} break;
	case e_cmp: {
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			if (e->flag == cmp_filter) {
				sql_subfunc *f = e->f;
				cond_append(l, &f->func->base.id);
			}
			if (exps_deps(sql, e->l, refs, l) != 0 ||
				exps_deps(sql, e->r, refs, l) != 0)
				return -1;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if (exp_deps(sql, e->l, refs, l) != 0 ||
				exps_deps(sql, e->r, refs, l) != 0)
				return -1;
		} else {
			if (exp_deps(sql, e->l, refs, l) != 0 ||
				exp_deps(sql, e->r, refs, l) != 0)
				return -1;
			if (e->f)
				return exp_deps(sql, e->f, refs, l);
		}
	}	break;
	}
	return 0;
}

static int
rel_deps(mvc *sql, sql_rel *r, list *refs, list *l)
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return -1;
	}

	if (!r)
		return 0;

	if (rel_is_ref(r) && refs_find_rel(refs, r)) /* allready handled */
		return 0;
	switch (r->op) {
	case op_basetable: {
		sql_table *t = r->l;
		sql_column *c = r->r;

		if (!t && c)
			t = c->t;

		cond_append(l, &t->base.id);
		/* find all used columns */
		for (node *en = r->exps->h; en; en = en->next) {
			sql_exp *exp = en->data;
			const char *oname = exp->r;

			assert(!is_func(exp->type));
			if (oname[0] == '%' && strcmp(oname, TID) == 0) {
				continue;
			} else if (oname[0] == '%') {
				sql_idx *i = find_sql_idx(t, oname+1);
				cond_append(l, &i->base.id);
			} else {
				sql_column *c = find_sql_column(t, oname);
				cond_append(l, &c->base.id);
			}
		}
	} break;
	case op_table: {
		if ((IS_TABLE_PROD_FUNC(r->flag) || r->flag == TABLE_FROM_RELATION) && r->r) { /* table producing function, excluding rel_relational_func cases */
			sql_exp *op = r->r;
			sql_subfunc *f = op->f;
			cond_append(l, &f->func->base.id);
		}
	} break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_union:
	case op_except:
	case op_inter:

	case op_insert:
	case op_update:
	case op_delete:
		if (rel_deps(sql, r->l, refs, l) != 0 ||
			rel_deps(sql, r->r, refs, l) != 0)
			return -1;
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
	case op_truncate:
		if (rel_deps(sql, r->l, refs, l) != 0)
			return -1;
		break;
	case op_ddl:
		if (r->flag == ddl_output || r->flag == ddl_create_seq || r->flag == ddl_alter_seq || r->flag == ddl_alter_table || r->flag == ddl_create_table || r->flag == ddl_create_view) {
			if (rel_deps(sql, r->l, refs, l) != 0)
				return -1;
		} else if (r->flag == ddl_list || r->flag == ddl_exception) {
			if (rel_deps(sql, r->l, refs, l) != 0 ||
				rel_deps(sql, r->r, refs, l) != 0)
				return -1;
		}
		break;
	}
	if (!is_base(r->op) && r->exps) {
		if (exps_deps(sql, r->exps, refs, l) != 0)
			return -1;
	}
	if ((is_simple_project(r->op) || is_groupby(r->op)) && r->r) {
		if (exps_deps(sql, r->r, refs, l) != 0)
			return -1;
	}
	if (rel_is_ref(r)) {
		list_append(refs, r);
		list_append(refs, l);
	}
	return 0;
}

list *
rel_dependencies(mvc *sql, sql_rel *r)
{
	list *refs = sa_list(sql->sa);
	list *l = sa_list(sql->sa);

	if (rel_deps(sql, r, refs, l) != 0)
		return NULL;
	return l;
}

static list *exps_exp_visitor(visitor *v, sql_rel *rel, list *exps, int depth, exp_rewrite_fptr exp_rewriter, bool topdown, bool relations_topdown);

static inline list *
exps_exps_exp_visitor(visitor *v, sql_rel *rel, list *lists, int depth, exp_rewrite_fptr exp_rewriter, bool topdown, bool relations_topdown)
{
	node *n;

	if (list_empty(lists))
		return lists;
	for (n = lists->h; n; n = n->next) {
		if (n->data && (n->data = exps_exp_visitor(v, rel, n->data, depth, exp_rewriter, topdown, relations_topdown)) == NULL)
			return NULL;
	}
	return lists;
}

static sql_rel *rel_exp_visitor(visitor *v, sql_rel *rel, exp_rewrite_fptr exp_rewriter, bool topdown, bool relations_topdown);

static sql_exp *
exp_visitor(visitor *v, sql_rel *rel, sql_exp *e, int depth, exp_rewrite_fptr exp_rewriter, bool topdown, bool relations_topdown, bool *changed)
{
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	assert(e);
	if (topdown) {
		int changes = v->changes;
		if (!(e = exp_rewriter(v, rel, e, depth)))
			return NULL;
		*changed |= v->changes > changes;
	}

	switch(e->type) {
	case e_column:
		break;
	case e_convert:
		if  ((e->l = exp_visitor(v, rel, e->l, depth+1, exp_rewriter, topdown, relations_topdown, changed)) == NULL)
			return NULL;
		break;
	case e_aggr:
	case e_func:
		if (e->r) /* rewrite rank -r is list of lists */
			if ((e->r = exps_exps_exp_visitor(v, rel, e->r, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		if (e->l)
			if ((e->l = exps_exp_visitor(v, rel, e->l, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			if ((e->l = exps_exp_visitor(v, rel, e->l, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
			if ((e->r = exps_exp_visitor(v, rel, e->r, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if ((e->l = exp_visitor(v, rel, e->l, depth+1, exp_rewriter, topdown, relations_topdown, changed)) == NULL)
				return NULL;
			if ((e->r = exps_exp_visitor(v, rel, e->r, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		} else {
			if ((e->l = exp_visitor(v, rel, e->l, depth+1, exp_rewriter, topdown, relations_topdown, changed)) == NULL)
				return NULL;
			if ((e->r = exp_visitor(v, rel, e->r, depth+1, exp_rewriter, topdown, relations_topdown, changed)) == NULL)
				return NULL;
			if (e->f && (e->f = exp_visitor(v, rel, e->f, depth+1, exp_rewriter, topdown, relations_topdown, changed)) == NULL)
				return NULL;
		}
		break;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			if ((e->l = exp_visitor(v, rel, e->l, depth+1, exp_rewriter, topdown, relations_topdown, changed)) == NULL)
				return NULL;
		} else if (e->flag & PSM_VAR) {
			return e;
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			if ((e->l = exp_visitor(v, rel, e->l, depth+1, exp_rewriter, topdown, relations_topdown, changed)) == NULL)
				return NULL;
			if ((e->r = exps_exp_visitor(v, rel, e->r, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
			if (e->flag == PSM_IF && e->f && (e->f = exps_exp_visitor(v, rel, e->f, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		} else if (e->flag & PSM_REL) {
			if ((e->l = rel_exp_visitor(v, e->l, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		}
		break;
	case e_atom:
		if (e->f)
			if ((e->f = exps_exp_visitor(v, rel, e->f, depth+1, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		break;
	}
	if (!topdown) {
		int changes = v->changes;
		if (!(e = exp_rewriter(v, rel, e, depth)))
			return NULL;
		*changed |= v->changes > changes;
	}
	return e;
}

static list *
exps_exp_visitor(visitor *v, sql_rel *rel, list *exps, int depth, exp_rewrite_fptr exp_rewriter, bool topdown, bool relations_topdown)
{
	bool changed = false;
	if (list_empty(exps))
		return exps;
	for (node *n = exps->h; n; n = n->next)
		if (n->data && (n->data = exp_visitor(v, rel, n->data, depth, exp_rewriter, topdown, relations_topdown, &changed)) == NULL)
			return NULL;
	if (changed && depth == 0) /* only level 0 exps use hash, so remove only on those */
		list_hash_clear(exps);
	return exps;
}

static inline sql_rel *
rel_exp_visitor(visitor *v, sql_rel *rel, exp_rewrite_fptr exp_rewriter, bool topdown, bool relations_topdown)
{
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return rel;

	if (relations_topdown) {
		if (rel->exps && (rel->exps = exps_exp_visitor(v, rel, rel->exps, 0, exp_rewriter, topdown, relations_topdown)) == NULL)
			return NULL;
		if ((is_groupby(rel->op) || is_simple_project(rel->op)) && rel->r && (rel->r = exps_exp_visitor(v, rel, rel->r, 0, exp_rewriter, topdown, relations_topdown)) == NULL)
			return NULL;
	}

	switch(rel->op){
	case op_basetable:
		break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) {
			if (rel->l)
				if ((rel->l = rel_exp_visitor(v, rel->l, exp_rewriter, topdown, relations_topdown)) == NULL)
					return NULL;
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				if ((rel->l = rel_exp_visitor(v, rel->l, exp_rewriter, topdown, relations_topdown)) == NULL)
					return NULL;
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				if ((rel->l = rel_exp_visitor(v, rel->l, exp_rewriter, topdown, relations_topdown)) == NULL)
					return NULL;
			if (rel->r)
				if ((rel->r = rel_exp_visitor(v, rel->r, exp_rewriter, topdown, relations_topdown)) == NULL)
					return NULL;
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:

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
			if ((rel->l = rel_exp_visitor(v, rel->l, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		if (rel->r)
			if ((rel->r = rel_exp_visitor(v, rel->r, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		break;
	case op_select:
	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:
	case op_truncate:
		if (rel->l)
			if ((rel->l = rel_exp_visitor(v, rel->l, exp_rewriter, topdown, relations_topdown)) == NULL)
				return NULL;
		break;
	}

	if (!relations_topdown) {
		if (rel->exps && (rel->exps = exps_exp_visitor(v, rel, rel->exps, 0, exp_rewriter, topdown, relations_topdown)) == NULL)
			return NULL;
		if ((is_groupby(rel->op) || is_simple_project(rel->op)) && rel->r && (rel->r = exps_exp_visitor(v, rel, rel->r, 0, exp_rewriter, topdown, relations_topdown)) == NULL)
			return NULL;
	}

	return rel;
}

sql_rel *
rel_exp_visitor_topdown(visitor *v, sql_rel *rel, exp_rewrite_fptr exp_rewriter, bool relations_topdown)
{
	return rel_exp_visitor(v, rel, exp_rewriter, true, relations_topdown);
}

sql_rel *
rel_exp_visitor_bottomup(visitor *v, sql_rel *rel, exp_rewrite_fptr exp_rewriter, bool relations_topdown)
{
	return rel_exp_visitor(v, rel, exp_rewriter, false, relations_topdown);
}

static list *exps_rel_visitor(visitor *v, list *exps, rel_rewrite_fptr rel_rewriter, bool topdown);

static sql_exp *
exp_rel_visitor(visitor *v, sql_exp *e, rel_rewrite_fptr rel_rewriter, bool topdown)
{
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	assert(e);
	switch(e->type) {
	case e_column:
		break;
	case e_convert:
		if ((e->l = exp_rel_visitor(v, e->l, rel_rewriter, topdown)) == NULL)
			return NULL;
		break;
	case e_aggr:
	case e_func:
		if (e->r) /* rewrite rank */
			if ((e->r = exps_rel_visitor(v, e->r, rel_rewriter, topdown)) == NULL)
				return NULL;
		if (e->l)
			if ((e->l = exps_rel_visitor(v, e->l, rel_rewriter, topdown)) == NULL)
				return NULL;
		break;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			if ((e->l = exps_rel_visitor(v, e->l, rel_rewriter, topdown)) == NULL)
				return NULL;
			if ((e->r = exps_rel_visitor(v, e->r, rel_rewriter, topdown)) == NULL)
				return NULL;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if ((e->l = exp_rel_visitor(v, e->l, rel_rewriter, topdown)) == NULL)
				return NULL;
			if ((e->r = exps_rel_visitor(v, e->r, rel_rewriter, topdown)) == NULL)
				return NULL;
		} else {
			if ((e->l = exp_rel_visitor(v, e->l, rel_rewriter, topdown)) == NULL)
				return NULL;
			if ((e->r = exp_rel_visitor(v, e->r, rel_rewriter, topdown)) == NULL)
				return NULL;
			if (e->f && (e->f = exp_rel_visitor(v, e->f, rel_rewriter, topdown)) == NULL)
				return NULL;
		}
		break;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			if ((e->l = exp_rel_visitor(v, e->l, rel_rewriter, topdown)) == NULL)
				return NULL;
		} else if (e->flag & PSM_VAR) {
			return e;
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			if ((e->l = exp_rel_visitor(v, e->l, rel_rewriter, topdown)) == NULL)
				return NULL;
			if ((e->r = exps_rel_visitor(v, e->r, rel_rewriter, topdown)) == NULL)
				return NULL;
			if (e->flag == PSM_IF && e->f && (e->f = exps_rel_visitor(v, e->f, rel_rewriter, topdown)) == NULL)
				return NULL;
		} else if (e->flag & PSM_REL) {
			sql_rel *(*func)(visitor *, sql_rel *, rel_rewrite_fptr) = topdown ? rel_visitor_topdown : rel_visitor_bottomup;
			if ((e->l = func(v, e->l, rel_rewriter)) == NULL)
				return NULL;
		}
		break;
	case e_atom:
		if (e->f)
			if ((e->f = exps_rel_visitor(v, e->f, rel_rewriter, topdown)) == NULL)
				return NULL;
		break;
	}
	return e;
}

static list *
exps_rel_visitor(visitor *v, list *exps, rel_rewrite_fptr rel_rewriter, bool topdown)
{
	if (list_empty(exps))
		return exps;
	for (node *n = exps->h; n; n = n->next)
		if (n->data && (n->data = exp_rel_visitor(v, n->data, rel_rewriter, topdown)) == NULL)
			return NULL;
	return exps;
}

static inline sql_rel *
do_rel_visitor(visitor *v, sql_rel *rel, rel_rewrite_fptr rel_rewriter, bool topdown)
{
	if (rel->exps && (rel->exps = exps_rel_visitor(v, rel->exps, rel_rewriter, topdown)) == NULL)
		return NULL;
	if ((is_groupby(rel->op) || is_simple_project(rel->op)) && rel->r && (rel->r = exps_rel_visitor(v, rel->r, rel_rewriter, topdown)) == NULL)
		return NULL;
	int changes = v->changes;
	rel = rel_rewriter(v, rel);
	if (rel && rel->exps && v->changes > changes) {
		list_hash_clear(rel->exps);
		if ((is_groupby(rel->op) || is_simple_project(rel->op)) && rel->r)
			list_hash_clear(rel->r);
	}
	return rel;
}

static inline sql_rel *
rel_visitor(visitor *v, sql_rel *rel, rel_rewrite_fptr rel_rewriter, bool topdown)
{
	sql_rel *parent = v->parent;

	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return NULL;

	if (topdown && !(rel = do_rel_visitor(v, rel, rel_rewriter, true)))
		return NULL;

	sql_rel *(*func)(visitor *, sql_rel *, rel_rewrite_fptr) = topdown ? rel_visitor_topdown : rel_visitor_bottomup;

	v->parent = rel;
	switch(rel->op){
	case op_basetable:
		break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) {
			if (rel->l)
				if ((rel->l = func(v, rel->l, rel_rewriter)) == NULL)
					return NULL;
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				if ((rel->l = func(v, rel->l, rel_rewriter)) == NULL)
					return NULL;
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				if ((rel->l = func(v, rel->l, rel_rewriter)) == NULL)
					return NULL;
			if (rel->r)
				if ((rel->r = func(v, rel->r, rel_rewriter)) == NULL)
					return NULL;
		} else if (rel->flag == ddl_psm) {
			if ((rel->exps = exps_rel_visitor(v, rel->exps, rel_rewriter, topdown)) == NULL)
				return NULL;
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:

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
			if ((rel->l = func(v, rel->l, rel_rewriter)) == NULL)
				return NULL;
		if (rel->r)
			if ((rel->r = func(v, rel->r, rel_rewriter)) == NULL)
				return NULL;
		break;
	case op_select:
	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:
	case op_truncate:
		if (rel->l)
			if ((rel->l = func(v, rel->l, rel_rewriter)) == NULL)
				return NULL;
		break;
	}
	v->parent = parent;

	if (!topdown)
		rel = do_rel_visitor(v, rel, rel_rewriter, false);
	return rel;
}

sql_rel *
rel_visitor_topdown(visitor *v, sql_rel *rel, rel_rewrite_fptr rel_rewriter)
{
	v->depth++;
	rel = rel_visitor(v, rel, rel_rewriter, true);
	v->depth--;
	return rel;
}

sql_rel *
rel_visitor_bottomup(visitor *v, sql_rel *rel, rel_rewrite_fptr rel_rewriter)
{
	v->depth++;
	rel = rel_visitor(v, rel, rel_rewriter, false);
	v->depth--;
	return rel;
}

list *
exps_exp_visitor_topdown(visitor *v, sql_rel *rel, list *exps, int depth, exp_rewrite_fptr exp_rewriter, bool relations_topdown)
{
	return exps_exp_visitor(v, rel, exps, depth, exp_rewriter, true, relations_topdown);
}

list *
exps_exp_visitor_bottomup(visitor *v, sql_rel *rel, list *exps, int depth, exp_rewrite_fptr exp_rewriter, bool relations_topdown)
{
	return exps_exp_visitor(v, rel, exps, depth, exp_rewriter, false, relations_topdown);
}

static bool
exps_rebind_exp(mvc *sql, sql_rel *rel, list *exps)
{
	bool ok = true;

	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return false;
	}

	if (list_empty(exps))
		return true;
	for (node *n = exps->h; n && ok; n = n->next)
		ok &= rel_rebind_exp(sql, rel, n->data);
	return ok;
}

bool
rel_rebind_exp(mvc *sql, sql_rel *rel, sql_exp *e)
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return false;
	}

	switch (e->type) {
	case e_convert:
		return rel_rebind_exp(sql, rel, e->l);
	case e_aggr:
	case e_func:
		return exps_rebind_exp(sql, rel, e->l);
	case e_cmp:
		if (e->flag == cmp_in || e->flag == cmp_notin)
			return rel_rebind_exp(sql, rel, e->l) && exps_rebind_exp(sql, rel, e->r);
		if (e->flag == cmp_or || e->flag == cmp_filter)
			return exps_rebind_exp(sql, rel, e->l) && exps_rebind_exp(sql, rel, e->r);
		return rel_rebind_exp(sql, rel, e->l) && rel_rebind_exp(sql, rel, e->r) && (!e->f || rel_rebind_exp(sql, rel, e->f));
	case e_column:
		return rel_find_exp(rel, e) != NULL;
	case e_atom:
		return exps_rebind_exp(sql, rel, e->f);
	case e_psm:
		return true;
	}
	return true;
}

static sql_exp *
_exp_freevar_offset(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)rel; (void)depth;
	/* visitor will handle recursion, ie only need to check columns here */
	int vf = is_freevar(e);
	if (v->changes < vf)
		v->changes=vf;
	return e;
}

int
exp_freevar_offset(mvc *sql, sql_exp *e)
{
	bool changed = false;
	visitor v = { .sql = sql };

	(void) changed;
	exp_visitor(&v, NULL, e, 0, &_exp_freevar_offset, true, true, &changed);
	/* freevar offset is passed via changes */
	return (v.changes);
}
