/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2019 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_select.h"
#include "sql_tokens.h"
#include "sql_semantic.h"	/* TODO this dependency should be removed, move
				   the dependent code into sql_mvc */
#include "sql_privileges.h"
#include "sql_env.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_xml.h"
#include "rel_dump.h"
#include "rel_prop.h"
#include "rel_psm.h"
#include "rel_schema.h"
#include "rel_unnest.h"
#include "rel_remote.h"
#include "rel_sequence.h"
#ifdef HAVE_HGE
#include "mal.h"		/* for have_hge */
#endif

#define VALUE_FUNC(f) (f->func->type == F_FUNC || f->func->type == F_FILT)
#define check_card(card,f) ((card == card_none && !f->res) || (CARD_VALUE(card) && f->res && VALUE_FUNC(f)) || card == card_loader || (card == card_relation && f->func->type == F_UNION))

/* return all expressions, with table name == tname */
static list *
rel_table_projections( mvc *sql, sql_rel *rel, char *tname, int level )
{
	list *exps;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "query too complex: running out of stack space");

	if (!rel)
		return NULL;

	if (!tname) {
		if (is_project(rel->op) && rel->l)
			return _rel_projections(sql, rel->l, NULL, 1, 0, 1);
		else
			return NULL;
		/* return rel_projections(sql, rel, NULL, 1, 0); */
	}

	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = rel_table_projections( sql, rel->l, tname, level+1);
		if (exps)
			return exps;
		return rel_table_projections( sql, rel->r, tname, level+1);
	case op_semi:
	case op_anti:
	case op_select:
		return rel_table_projections( sql, rel->l, tname, level+1);

	case op_topn:
	case op_sample:
	case op_groupby:
	case op_union:
	case op_except:
	case op_inter:
	case op_project:
		if (!is_processed(rel) && level == 0)
			return rel_table_projections( sql, rel->l, tname, level+1);
		/* fall through */
	case op_table:
	case op_basetable:
		if (rel->exps) {
			node *en;

			exps = new_exp_list(sql->sa);
			for (en = rel->exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				/* first check alias */
				if (is_basecol(e) && exp_relname(e) && strcmp(exp_relname(e), tname) == 0)
					append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
				if (is_basecol(e) && !exp_relname(e) && e->l && strcmp(e->l, tname) == 0)
					append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
			}
			if (exps && list_length(exps))
				return exps;
		}
		/* fall through */
	default:
		return NULL;
	}
}

static sql_rel*
rel_parent( sql_rel *rel )
{
	if (rel->l && (is_project(rel->op) || rel->op == op_topn || rel->op == op_sample)) {
		sql_rel *l = rel->l;
		if (is_project(l->op))
			return l;
	}
	return rel;
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
	return NULL;
}

static sql_exp *
lastexp(sql_rel *rel) 
{
	if (!is_processed(rel) || is_topn(rel->op) || is_sample(rel->op))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	assert(is_project(rel->op));
	return rel->exps->t->data;
}

static sql_exp *
_rel_lastexp(mvc *sql, sql_rel *rel )
{
	sql_exp *e;

	if (!is_processed(rel) || is_topn(rel->op) || is_sample(rel->op))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	assert(is_project(rel->op));
	e = rel->exps->t->data;
	return exp_ref(sql->sa, e);
}

static sql_exp *
rel_lastexp(mvc *sql, sql_rel *rel )
{
	sql_exp *e;

	if (!is_processed(rel) || is_topn(rel->op))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	if (rel->op == op_project) {
		MT_lock_set(&rel->exps->ht_lock);
		rel->exps->ht = NULL;
		MT_lock_unset(&rel->exps->ht_lock);
		return exp_alias_or_copy(sql, NULL, NULL, rel, rel->exps->t->data);
	}
	assert(is_project(rel->op));
	e = rel->exps->t->data;
	return exp_ref(sql->sa, e);
}

static sql_exp *
rel_find_lastexp(sql_rel *rel )
{
	if (!is_processed(rel))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	return rel->exps->t->data;
}

static sql_rel*
rel_project2groupby(mvc *sql, sql_rel *g)
{
	if (g->op == op_project) {
		node *en;
		
		reset_processed(g);
		if (!g->exps)
			g->exps = new_exp_list(sql->sa);
		for (en = g->exps->h; en; en = en->next) {
			sql_exp *e = en->data;

			if (e->card > CARD_ATOM) {
				if (e->type == e_column && e->r) {
					return sql_error(sql, 02, SQLSTATE(42000) "Cannot use non GROUP BY column '%s' in query results without an aggregate function", (char *) e->r);
				} else {
					return sql_error(sql, 02, SQLSTATE(42000) "Cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
		}
		g->card = CARD_ATOM; /* no groupby expressions */
		g->op = op_groupby;
		g->r = new_exp_list(sql->sa); /* add empty groupby column list */
		g = rel_project(sql->sa, g, rel_projections(sql, g, NULL, 1, 1));
		reset_processed(g);
		return g;
	}
	return NULL;
}

static sql_rel*
revert_project2groupby(sql_rel *p)
{
	/* change too recusive find project/groupby */
	if (p && p->op == op_project) {
		sql_rel *g = p->l;

		if (g->op == op_groupby) {
			sql_rel *l = g->l;
			g->r = NULL;
			g->op = op_project;
			if (l)
				g->card = l->card;
			return g;
		}
	}
	return p;
}

static sql_rel *
rel_orderby(mvc *sql, sql_rel *l)
{
	sql_rel *rel = rel_create(sql->sa);
	if(!rel)
		return NULL;

	assert(l->op == op_project && !l->r);
	rel->l = l;
	rel->r = NULL;
	rel->op = op_project;	
	rel->exps = rel_projections(sql, l, NULL, 1, 0);
	rel->card = l->card;
	rel->nrcols = l->nrcols;
	return rel;
}

/* forward refs */
static sql_rel * rel_setquery(sql_query *query, sql_rel *rel, symbol *sq);
static sql_rel * rel_joinquery(sql_query *query, sql_rel *rel, symbol *sq);
static sql_rel * rel_crossquery(sql_query *query, sql_rel *rel, symbol *q);
static sql_rel * rel_unionjoinquery(sql_query *query, sql_rel *rel, symbol *sq);

static sql_rel *
rel_table_optname(mvc *sql, sql_rel *sq, symbol *optname)
{
	sql_rel *osq = sq;
	node *ne;

	if (optname && optname->token == SQL_NAME) {
		dlist *columnrefs = NULL;
		char *tname = optname->data.lval->h->data.sval;
		list *l = sa_list(sql->sa);

		columnrefs = optname->data.lval->h->next->data.lval;
		if (is_topn(sq->op) || (is_project(sq->op) && sq->r)) {
			sq = rel_project(sql->sa, sq, rel_projections(sql, sq, NULL, 1, 1));
			osq = sq;
		}
		if (columnrefs && dlist_length(columnrefs) > sq->nrcols)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: The number of aliases is longer than the number of columns (%d>%d)", dlist_length(columnrefs), sq->nrcols);
		if (columnrefs && sq->exps) {
			dnode *d = columnrefs->h;

			ne = sq->exps->h;
			MT_lock_set(&sq->exps->ht_lock);
			sq->exps->ht = NULL;
			MT_lock_unset(&sq->exps->ht_lock);
			for (; d && ne; d = d->next, ne = ne->next) {
				sql_exp *e = ne->data;

				if (exps_bind_column2(l, tname, d->data.sval))
					return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: Duplicate column name '%s.%s'", tname, d->data.sval);
				exp_setname(sql->sa, e, tname, d->data.sval );
				if (!is_intern(e))
					set_basecol(e);
				append(l, e);
			}
		}
		if (!columnrefs && sq->exps) {
			ne = sq->exps->h;
			for (; ne; ne = ne->next) {
				sql_exp *e = ne->data;

				if (exp_name(e) && exps_bind_column2(l, tname, exp_name(e)))
					return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: Duplicate column name '%s.%s'", tname, exp_name(e));
				noninternexp_setname(sql->sa, e, tname, NULL );
				if (!is_intern(e))
					set_basecol(e);
				append(l, e);
			}
		}
	} else {
		if (!is_project(sq->op) || is_topn(sq->op) || (is_project(sq->op) && sq->r)) {
			sq = rel_project(sql->sa, sq, rel_projections(sql, sq, NULL, 1, 1));
			osq = sq;
		}
		for (ne = osq->exps->h; ne; ne = ne->next) {
			sql_exp *e = ne->data;

			if (!is_intern(e))
				set_basecol(e);
		}
	}
	return osq;
}

static sql_rel *
rel_subquery_optname(sql_query *query, sql_rel *rel, symbol *ast)
{
	mvc *sql = query->sql;
	SelectNode *sn = (SelectNode *) ast;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = rel_subquery(query, rel, ast, ek);

	assert(ast->token == SQL_SELECT);
	if (!sq)
		return NULL;

	return rel_table_optname(sql, sq, sn->name);
}

sql_rel *
rel_with_query(sql_query *query, symbol *q ) 
{
	mvc *sql = query->sql;
	dnode *d = q->data.lval->h;
	symbol *next = d->next->data.sym;
	sql_rel *rel;

	if(!stack_push_frame(sql, "WITH"))
		return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
	/* first handle all with's (ie inlined views) */
	for (d = d->data.lval->h; d; d = d->next) {
		symbol *sym = d->data.sym;
		dnode *dn = sym->data.lval->h;
		char *name = qname_table(dn->data.lval);
		sql_rel *nrel;

		if (frame_find_var(sql, name)) {
			stack_pop_frame(sql);
			return sql_error(sql, 01, SQLSTATE(42000) "Variable '%s' already declared", name);
		}
		nrel = rel_semantic(query, sym);
		if (!nrel) {  
			stack_pop_frame(sql);
			return NULL;
		}
		if(!stack_push_rel_view(sql, name, nrel)) {
			stack_pop_frame(sql);
			return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		}
		if (!is_project(nrel->op)) {
			if (is_topn(nrel->op) || is_sample(nrel->op)) {
				nrel = rel_project(sql->sa, nrel, rel_projections(sql, nrel, NULL, 1, 1));
			} else {
				stack_pop_frame(sql);
				return NULL;
			}
		}
		assert(is_project(nrel->op));
		if (is_project(nrel->op) && nrel->exps) {
			node *ne = nrel->exps->h;

			for (; ne; ne = ne->next) {
				sql_exp *e = ne->data;

				noninternexp_setname(sql->sa, e, name, NULL );
				if (!is_intern(e))
					set_basecol(e);
			}
		}
	}
	rel = rel_semantic(query, next);
	stack_pop_frame(sql);
	return rel;
}

static sql_rel *
query_exp_optname(sql_query *query, sql_rel *r, symbol *q)
{
	mvc *sql = query->sql;
	switch (q->token) {
	case SQL_WITH:
	{
		sql_rel *tq = rel_with_query(query, q);

		if (!tq)
			return NULL;
		if (q->data.lval->t->type == type_symbol)
			return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
		return tq;
	}
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
	{
		sql_rel *tq = rel_setquery(query, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_JOIN:
	{
		sql_rel *tq = rel_joinquery(query, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_CROSS:
	{
		sql_rel *tq = rel_crossquery(query, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_UNIONJOIN:
	{
		sql_rel *tq = rel_unionjoinquery(query, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	default:
		(void) sql_error(sql, 02, SQLSTATE(42000) "case %d %s\n", q->token, token2string(q->token));
	}
	return NULL;
}

static sql_subfunc *
bind_func_(mvc *sql, sql_schema *s, char *fname, list *ops, sql_ftype type )
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && 
	    list_cmp(sql->forward->ops, ops, (fcmp)&arg_subtype_cmp) == 0 &&
	    execute_priv(sql, sql->forward)) 
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
	sf = sql_bind_func_(sql->sa, s, fname, ops, type);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
bind_func(mvc *sql, sql_schema *s, char *fname, sql_subtype *t1, sql_subtype *t2, sql_ftype type )
{
	sql_subfunc *sf = NULL;

	if (t1 == NULL)
		return NULL;
	if (sql->forward) {
		if (execute_priv(sql, sql->forward) &&
		    strcmp(fname, sql->forward->base.name) == 0 && 
		   ((!t1 && list_length(sql->forward->ops) == 0) || 
		    (!t2 && list_length(sql->forward->ops) == 1 && subtype_cmp(sql->forward->ops->h->data, t1) == 0) ||
		    (list_length(sql->forward->ops) == 2 && 
		     	subtype_cmp(sql->forward->ops->h->data, t1) == 0 &&
		     	subtype_cmp(sql->forward->ops->h->next->data, t2) == 0))) {
			return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
		}
	}
	sf = sql_bind_func(sql->sa, s, fname, t1, t2, type);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
bind_member_func(mvc *sql, sql_schema *s, char *fname, sql_subtype *t, int nrargs, sql_subfunc *prev)
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && 
		list_length(sql->forward->ops) == nrargs && is_subtype(t, &((sql_arg *) sql->forward->ops->h->data)->type) && execute_priv(sql, sql->forward)) 
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, t);
	sf = sql_bind_member(sql->sa, s, fname, t, nrargs, prev);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
find_func(mvc *sql, sql_schema *s, char *fname, int len, sql_ftype type, sql_subfunc *prev )
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && list_length(sql->forward->ops) == len && execute_priv(sql, sql->forward)) 
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
	sf = sql_find_func(sql->sa, s, fname, len, type, prev);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static int
score_func( sql_subfunc *sf, list *tl) 
{
	int score = 0;
	node *n, *m;

	/* todo varargs */
	for (n = sf->func->ops->h, m = tl->h; n && m; n = n->next, m = m->next){
		sql_arg *a = n->data;
		sql_subtype *t = m->data;

		if (a->type.type->eclass == EC_ANY)
			score += 100;
		else if (is_subtype(t, &a->type))
			score += t->type->localtype * 20;
		/* same class over converting to other class */
		else if (t->type->eclass == a->type.type->eclass &&
			t->type->localtype <= a->type.type->localtype)
			score += a->type.type->localtype * 4;
		/* make sure we rewrite decimals to float/doubles */
		else if (t->type->eclass == EC_DEC &&
		         a->type.type->eclass == EC_FLT)
			score += a->type.type->localtype * 2;
	}
	return score;
}

static sql_exp *
find_table_function_type(mvc *sql, sql_schema *s, char *fname, list *exps, list *tl, sql_ftype type, sql_subfunc **sf)
{
	sql_exp *e = NULL;
	*sf = bind_func_(sql, s, fname, tl, type);

	if (*sf) {
		e = exp_op(sql->sa, exps, *sf);
	} else if (list_length(tl)) { 
		int len, match = 0;
		list *funcs = sql_find_funcs(sql->sa, s, fname, list_length(tl), type); 
		if (!funcs)
			return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		len = list_length(funcs);
		if (len > 1) {
			int i, score = 0; 
			node *n;

			for (i = 0, n = funcs->h; i<len; i++, n = n->next) {
				int cscore = score_func(n->data, tl);
				if (cscore > score) {
					score = cscore;
					match = i;
				}
			}
		}
		if (list_empty(funcs))
			return NULL;

		*sf = list_fetch(funcs, match);
		if ((*sf)->func->vararg) {
			e = exp_op(sql->sa, exps, *sf);
		} else {
			node *n, *m;
			list *nexps = new_exp_list(sql->sa);
			sql_subtype *atp = NULL;
			sql_arg *aa = NULL;

			/* find largest any type argument */ 
			for (n = exps->h, m = (*sf)->func->ops->h; n && m; n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;

				if (!aa && a->type.type->eclass == EC_ANY) {
					atp = &e->tpe;
					aa = a;
				}
				if (aa && a->type.type->eclass == EC_ANY &&
				    e->tpe.type->localtype > atp->type->localtype){
					atp = &e->tpe;
					aa = a;
				}
			}
			for (n = exps->h, m = (*sf)->func->ops->h; n && m; n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;
				sql_subtype *ntp = &a->type;

				if (a->type.type->eclass == EC_ANY)
					ntp = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
				e = rel_check_type(sql, ntp, NULL, e, type_equal);
				if (!e) {
					nexps = NULL;
					break;
				}
				if (e->card > CARD_ATOM) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e));

					e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, 0);
				}
				append(nexps, e);
			}
			e = NULL;
			if (nexps) 
				e = exp_op(sql->sa, nexps, *sf);
		}
	}
	return e;
}

static sql_exp*
find_table_function(mvc *sql, sql_schema *s, char *fname, list *exps, list *tl)
{
	sql_subfunc* sf = NULL;
	return find_table_function_type(sql, s, fname, exps, tl, F_UNION, &sf);
}

static sql_rel *
rel_named_table_function(sql_query *query, sql_rel *rel, symbol *ast, int lateral)
{
	mvc *sql = query->sql;
	list *exps = NULL, *tl;
	node *m;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = NULL, *outer = NULL;
	sql_exp *e = NULL;
	sql_subfunc *sf = NULL;
	symbol *sym = ast->data.lval->h->data.sym;
	dnode *l = sym->data.lval->h;
	char *tname = NULL;
	char *fname = qname_fname(l->data.lval); 
	char *sname = qname_schema(l->data.lval);
	node *en;
	sql_schema *s = sql->session->schema;

	tl = sa_list(sql->sa);
	exps = new_exp_list(sql->sa);
	if (l->next) { /* table call with subquery */
		if (l->next->type == type_symbol && l->next->data.sym->token == SQL_SELECT) {
			if (l->next->next != NULL)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: '%s' requires a single sub query", fname);
			sq = rel_subquery(query, NULL, l->next->data.sym, ek);
		} else if (l->next->type == type_symbol || l->next->type == type_list) {
			dnode *n;
			exp_kind iek = {type_value, card_column, TRUE};
			list *exps = sa_list (sql->sa);

			if (l->next->type == type_symbol)
				n = l->next;
			else 
				n = l->next->data.lval->h;
			for ( ; n; n = n->next) {
				sql_exp *e = rel_value_exp(query, &outer, n->data.sym, sql_sel, iek);

				if (!e)
					return NULL;
				append(exps, e);
			}
			sq = rel_project(sql->sa, NULL, exps);
			if (lateral && outer) {
				sq = rel_crossproduct(sql->sa, sq, outer, op_join);
				set_dependent(sq);
			}
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		if (!sq || (!lateral && outer))
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
		for (en = sq->exps->h; en; en = en->next) {
			sql_exp *e = en->data;

			append(exps, e=exp_alias_or_copy(sql, tname, exp_name(e), NULL, e));
			append(tl, exp_subtype(e));
		}
	}

	if (sname)
		s = mvc_bind_schema(sql, sname);
	e = find_table_function(sql, s, fname, exps, tl);
	if (!e)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
	rel = sq;

	if (ast->data.lval->h->next->data.sym)
		tname = ast->data.lval->h->next->data.sym->data.lval->h->data.sval;
	else
		tname = make_label(sql->sa, ++sql->label);

	/* column or table function */
	sf = e->f;
	if (e->type != e_func || sf->func->type != F_UNION) {
		(void) sql_error(sql, 02, SQLSTATE(42000) "SELECT: '%s' does not return a table", exp_func_name(e));
		return NULL;
	}

	if (sql->emode == m_prepare && rel && rel->exps && sql->params) {
		int i = 0;
		/* for prepared statements set possible missing parameter SQL types from */
		for (m = sf->func->ops->h; m; m = m->next, i++) {
			sql_arg *func_arg = m->data;
			sql_exp *proj_parameter = (sql_exp*) list_fetch(rel->exps, i);
			if(proj_parameter) {
				sql_arg *prep_arg = (sql_arg*) list_fetch(sql->params, proj_parameter->flag);
				if(prep_arg && !prep_arg->type.type)
					prep_arg->type = func_arg->type;
			}
		}
	}

	/* for each column add table.column name */
	exps = new_exp_list(sql->sa);
	for (m = sf->func->res->h; m; m = m->next) {
		sql_arg *a = m->data;
		sql_exp *e = exp_column(sql->sa, tname, a->name, &a->type, CARD_MULTI, 1, 0);

		set_basecol(e);
		append(exps, e);
	}
	rel = rel_table_func(sql->sa, rel, e, exps, (sq != NULL));
	if (ast->data.lval->h->next->data.sym && ast->data.lval->h->next->data.sym->data.lval->h->next->data.lval)
		rel = rel_table_optname(sql, rel, ast->data.lval->h->next->data.sym);
	return rel;
}

static sql_exp *
rel_op_(mvc *sql, sql_schema *s, char *fname, exp_kind ek)
{
	sql_subfunc *f = NULL;
	sql_ftype type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:
		   ((ek.card == card_relation)?F_UNION:F_FUNC));

	f = sql_bind_func(sql->sa, s, fname, NULL, NULL, type);
	if (f && check_card(ek.card, f)) {
		return exp_op(sql->sa, NULL, f);
	} else {
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
	}
}

static sql_rel *
rel_values(sql_query *query, symbol *tableref)
{
	mvc *sql = query->sql;
	sql_rel *r = NULL;
	dlist *rowlist = tableref->data.lval;
	symbol *optname = rowlist->t->data.sym;
	dnode *o;
	node *m;
	list *exps = sa_list(sql->sa);
	exp_kind ek = {type_value, card_value, TRUE};

	for (o = rowlist->h; o; o = o->next) {
		dlist *values = o->data.lval;

		/* When performing sub-queries, the relation name appears under a SQL_NAME symbol at the end of the list */
		if (o->type == type_symbol && o->data.sym->token == SQL_NAME)
			break;

		if (!list_empty(exps) && list_length(exps) != dlist_length(values)) {
			return sql_error(sql, 02, SQLSTATE(42000) "VALUES: number of columns doesn't match between rows");
		} else {
			dnode *n;

			if (list_empty(exps)) {
				for (n = values->h; n; n = n->next) {
					sql_exp *vals = exp_values(sql->sa, sa_list(sql->sa));

					exp_label(sql->sa, vals, ++sql->label);
					list_append(exps, vals);
				}
			}
			for (n = values->h, m = exps->h; n && m; n = n->next, m = m->next) {
				sql_exp *vals = m->data;
				list *vals_list = vals->f;
				sql_exp *e = rel_value_exp(query, NULL, n->data.sym, sql_sel, ek);
				if (!e) 
					return NULL;
				list_append(vals_list, e);
			}
		}
	}
	/* loop to check types */
	for (m = exps->h; m; m = m->next) {
		node *n;
		sql_exp *vals = m->data;
		list *vals_list = vals->f;
		list *nexps = sa_list(sql->sa);
		sql_subtype *tpe = exp_subtype(vals_list->h->data);

		if (tpe)
			vals->tpe = *tpe;

		/* first get super type */
		for (n = vals_list->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subtype super, *ttpe;

			/* if the expression is a parameter set its type */
			if (tpe && e->type == e_atom && !e->l && !e->r && !e->f && !e->tpe.type) {
				if (set_type_param(sql, tpe, e->flag) == 0)
					e->tpe = *tpe;
				else
					return NULL;
			}
			ttpe = exp_subtype(e);
			if (tpe && ttpe) {
				supertype(&super, tpe, ttpe);
				vals->tpe = super;
				tpe = &vals->tpe;
			} else {
				tpe = ttpe;
			}
		}
		if (!tpe)
			continue;
		/* if the expression is a parameter set its type */
		for (n = vals_list->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (e->type == e_atom && !e->l && !e->r && !e->f && !e->tpe.type) {
				if (set_type_param(sql, tpe, e->flag) == 0)
					e->tpe = *tpe;
				else
					return NULL;
			}
		}
		vals->tpe = *tpe;
		for (n = vals_list->h; n; n = n->next) {
			sql_exp *e = n->data;
			e = rel_check_type(sql, &vals->tpe, NULL, e, type_equal);
			if (!e)
				return NULL;
			append(nexps, e); 
		}
		vals->f = nexps;
	}
	r = rel_project(sql->sa, NULL, exps);
	r->nrcols = list_length(exps);
	return rel_table_optname(sql, r, optname);
}

static int
check_is_lateral(symbol *tableref) 
{
	if (tableref->token == SQL_NAME || tableref->token == SQL_TABLE) {
		if (dlist_length(tableref->data.lval) == 3)
			return tableref->data.lval->h->next->next->data.i_val;
		return 0;
	} else if (tableref->token == SQL_VALUES) {
		return 0;
	} else if (tableref->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *) tableref;
		return sn->lateral;
	} else {
		return 0;
	}
}

sql_rel *
table_ref(sql_query *query, sql_rel *rel, symbol *tableref, int lateral)
{
	mvc *sql = query->sql;
	char *tname = NULL;
	sql_table *t = NULL;
	sql_rel *res = NULL;

	if (tableref->token == SQL_NAME) {
		dlist *name = tableref->data.lval->h->data.lval;
		sql_rel *temp_table = NULL;
		char *sname = qname_schema(name);
		sql_schema *s = NULL;

		tname = qname_table(name);

		if (dlist_length(name) > 2)
			return sql_error(sql, 02, SQLSTATE(3F000) "SELECT: only a schema and table name expected");

		if (sname && !(s=mvc_bind_schema(sql,sname)))
			return sql_error(sql, 02, SQLSTATE(3F000) "SELECT: no such schema '%s'", sname);
		if (!t && !sname) {
			t = stack_find_table(sql, tname);
			if (!t) 
				temp_table = stack_find_rel_view(sql, tname);
		}
		if (!t && !temp_table) {
			if (!s)
				s = cur_schema(sql);
			t = mvc_bind_table(sql, s, tname);
			if (!t && !sname) {
				s = tmp_schema(sql);
				t = mvc_bind_table(sql, s, tname);
			}
		}
		if (!t && !temp_table) {
			return sql_error(sql, 02, SQLSTATE(42S02) "SELECT: no such table '%s'", tname);
		} else if (!temp_table && !table_privs(sql, t, PRIV_SELECT)) {
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: access denied for %s to table '%s.%s'", stack_get_string(sql, "current_user"), s->base.name, tname);
		}
		if (tableref->data.lval->h->next->data.sym) {	/* AS */
			tname = tableref->data.lval->h->next->data.sym->data.lval->h->data.sval;
		}
		if (temp_table && !t) {
			node *n;
			list *exps = rel_projections(sql, temp_table, NULL, 1, 1);

			temp_table = rel_project(sql->sa, temp_table, exps);
			for (n = exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				noninternexp_setname(sql->sa, e, tname, NULL);
				set_basecol(e);
			}
			return temp_table;
		} else if (isView(t)) {
			/* instantiate base view */
			node *n,*m;
			sql_rel *rel;

			if (sql->emode == m_deps)
				rel = rel_basetable(sql, t, tname);
			else
				rel = rel_parse(sql, t->s, t->query, m_instantiate);

			if (!rel)
				return NULL;

			/* Rename columns of the rel_parse relation */
			if (sql->emode != m_deps) {
				rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
				if (!rel)
					return NULL;
				set_processed(rel);
				for (n = t->columns.set->h, m = rel->exps->h; n && m; n = n->next, m = m->next) {
					sql_column *c = n->data;
					sql_exp *e = m->data;

					exp_setname(sql->sa, e, tname, c->base.name);
					set_basecol(e);
				}
			}
			return rel;
		}
		if ((isMergeTable(t) || isReplicaTable(t)) && list_empty(t->members.set))
			return sql_error(sql, 02, SQLSTATE(42000) "MERGE or REPLICA TABLE should have at least one table associated");

		res = rel_basetable(sql, t, tname);
		if (tableref->data.lval->h->next->data.sym && tableref->data.lval->h->next->data.sym->data.lval->h->next->data.lval) /* AS with column aliases */
			res = rel_table_optname(sql, res, tableref->data.lval->h->next->data.sym);
		return res;
	} else if (tableref->token == SQL_VALUES) {
		return rel_values(query, tableref);
	} else if (tableref->token == SQL_TABLE) {
		return rel_named_table_function(query, rel, tableref, lateral);
	} else if (tableref->token == SQL_SELECT) {
		return rel_subquery_optname(query, rel, tableref);
	} else {
		return query_exp_optname(query, rel, tableref);
	}
}

static sql_exp *
rel_var_ref(mvc *sql, char *name, int at)
{
	if (stack_find_var(sql, name)) {
		sql_subtype *tpe = stack_find_type(sql, name);
		int frame = stack_find_frame(sql, name);

		return exp_param(sql->sa, name, tpe, frame);
	} else if (at) {
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: '@""%s' unknown", name);
	} else {
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: identifier '%s' unknown", name);
	}
}

static sql_exp *
exps_get_exp(list *exps, int nth)
{
	node *n = NULL;
	int i = 0;

	if (exps)
		for (n=exps->h, i=1; n && i<nth; n=n->next, i++)
			;
	if (n && i == nth)
		return n->data;
	return NULL;
}

static sql_exp *
rel_column_ref(sql_query *query, sql_rel **rel, symbol *column_r, int f)
{
	mvc *sql = query->sql;
	sql_exp *exp = NULL;
	dlist *l = NULL;

	assert((column_r->token == SQL_COLUMN || column_r->token == SQL_IDENT) && column_r->type == type_list);
	l = column_r->data.lval;

	if (dlist_length(l) == 1 && l->h->type == type_int) {
		int nr = l->h->data.i_val;
		atom *a;
		if ((a = sql_bind_arg(sql, nr)) != NULL) {
			if (EC_TEMP_FRAC(atom_type(a)->type->eclass)) {
				/* fix fraction */
				sql_subtype *st = atom_type(a), t;
				int digits = st->digits;
				sql_exp *e;

				sql_find_subtype(&t, st->type->sqlname, digits, 0);

				st->digits = 3;
				e = exp_atom_ref(sql->sa, nr, st);

				return exp_convert(sql->sa, e, st, &t); 
			} else {
				return exp_atom_ref(sql->sa, nr, atom_type(a));
			}
		}
		return NULL;
	} else if (dlist_length(l) == 1) {
		char *name = l->h->data.sval;
		sql_arg *a = sql_bind_param(sql, name);
		int var = stack_find_var(sql, name);

		if (!exp && rel && *rel)
			exp = rel_bind_column(sql, *rel, name, f);
		if (!exp && query && query_has_outer(query)) {
			int i;
			sql_rel *outer;

			for (i=0; !exp && (outer = query_fetch_outer(query,i)); i++) {
				exp = rel_bind_column(sql, outer, name, f);
				if (!exp && is_sql_having(f) && is_groupby(outer->op))
					exp = rel_bind_column(sql, outer->l, name, f);
			}
			if (exp && outer && outer->card <= CARD_AGGR && exp->card > CARD_AGGR && !is_sql_aggr(f))
				return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
			if (exp) { 
				set_freevar(exp);
				exp->card = CARD_ATOM;
			}
		}
		if (exp) {
			if (var || a)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", name);
		} else if (a) {
			if (var)
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: identifier '%s' ambiguous", name);
			exp = exp_param(sql->sa, a->name, &a->type, 0);
		}
		if (!exp && var) {
			sql_rel *r = stack_find_rel_var(sql, name);
			if (r) {
				*rel = r;
				return exp_rel(sql, r);
			}
			return rel_var_ref(sql, name, 0);
		}
		if (!exp && !var) {
			if (rel && *rel && (*rel)->card <= CARD_AGGR && !is_sql_aggr(f) && (is_sql_sel(f) || is_sql_having(f))) {
				sql_rel *gb = *rel;

				while (gb->l && !is_groupby(gb->op))
					gb = gb->l;
				if (gb && is_select(gb->op)) /* check for having clause generated selection */
					gb = gb->l;
				if (gb && gb->l && rel_bind_column(sql, gb->l, name, f)) 
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
			}
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: identifier '%s' unknown", name);
		}
		if (exp && rel && *rel && (*rel)->card <= CARD_AGGR && exp->card > CARD_AGGR && is_sql_sel(f) && !is_sql_aggr(f))
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
	} else if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;

		if (!exp && rel && *rel)
			exp = rel_bind_column2(sql, *rel, tname, cname, f);
		if (!exp && query && query_has_outer(query)) {
			int i;
			sql_rel *outer;

			for (i=0; !exp && (outer = query_fetch_outer(query,i)); i++) {
				exp = rel_bind_column2(sql, outer, tname, cname, f);
				if (!exp && is_sql_having(f) && is_groupby(outer->op))
					exp = rel_bind_column2(sql, outer->l, tname, cname, f);
			}
			if (exp && outer && outer->card <= CARD_AGGR && exp->card > CARD_AGGR && !is_sql_aggr(f))
				return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
			if (exp) {
				set_freevar(exp);
				exp->card = CARD_ATOM;
			}
		}

		/* some views are just in the stack,
		   like before and after updates views */
		if (rel && !exp && sql->use_views) {
			sql_rel *v = stack_find_rel_view(sql, tname);

			if (v) {
				if (*rel)
					*rel = rel_crossproduct(sql->sa, *rel, v, op_join);
				else
					*rel = v;
				exp = rel_bind_column2(sql, *rel, tname, cname, f);
			}
		}
		if (!exp) {
			if (rel && *rel && (*rel)->card <= CARD_AGGR && !is_sql_aggr(f) && (is_sql_sel(f) || is_sql_having(f))) {
				sql_rel *gb = *rel;

				while (gb->l && !is_groupby(gb->op) && is_project(gb->op))
					gb = gb->l;
				if (gb && is_select(gb->op)) /* check for having clause generated selection */
					gb = gb->l;
				if (gb && is_groupby(gb->op) && gb->l && rel_bind_column2(sql, gb->l, tname, cname, f))
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
			}
			return sql_error(sql, 02, SQLSTATE(42S22) "SELECT: no such column '%s.%s'", tname, cname);
		}
		if (exp && rel && *rel && (*rel)->card <= CARD_AGGR && exp->card > CARD_AGGR && is_sql_sel(f) && !is_sql_aggr(f))
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
	} else if (dlist_length(l) >= 3) {
		return sql_error(sql, 02, SQLSTATE(42000) "TODO: column names of level >= 3");
	}
	return exp;
}

#ifdef HAVE_HGE
static hge
#else
static lng
#endif
scale2value(int scale)
{
#ifdef HAVE_HGE
	hge val = 1;
#else
	lng val = 1;
#endif

	if (scale < 0)
		scale = -scale;
	for (; scale; scale--) {
		val = val * 10;
	}
	return val;
}

static sql_exp *
exp_fix_scale(mvc *sql, sql_subtype *ct, sql_exp *e, int both, int always)
{
	sql_subtype *et = exp_subtype(e);

	if (ct->type->scale == SCALE_FIX && et->type->scale == SCALE_FIX) {
		int scale_diff = ((int) ct->scale - (int) et->scale);

		if (scale_diff) {
			sql_subtype *it = sql_bind_localtype(et->type->base.name);
			sql_subfunc *c = NULL;

			if (scale_diff < 0) {
				if (!both)
					return e;
				c = sql_bind_func(sql->sa, sql->session->schema, "scale_down", et, it, F_FUNC);
			} else {
				c = sql_bind_func(sql->sa, sql->session->schema, "scale_up", et, it, F_FUNC);
			}
			if (c) {
#ifdef HAVE_HGE
				hge val = scale2value(scale_diff);
#else
				lng val = scale2value(scale_diff);
#endif
				atom *a = atom_int(sql->sa, it, val);
				sql_subtype *res = c->res->h->data;

				res->scale = (et->scale + scale_diff);
				return exp_binop(sql->sa, e, exp_atom(sql->sa, a), c);
			}
		}
	} else if (always && et->scale) {	/* scale down */
		int scale_diff = -(int) et->scale;
		sql_subtype *it = sql_bind_localtype(et->type->base.name);
		sql_subfunc *c = sql_bind_func(sql->sa, sql->session->schema, "scale_down", et, it, F_FUNC);

		if (c) {
#ifdef HAVE_HGE
			hge val = scale2value(scale_diff);
#else
			lng val = scale2value(scale_diff);
#endif
			atom *a = atom_int(sql->sa, it, val);
			sql_subtype *res = c->res->h->data;

			res->scale = 0;
			return exp_binop(sql->sa, e, exp_atom(sql->sa, a), c);
		} else {
			printf("scale_down missing (%s)\n", et->type->base.name);
		}
	}
	return e;
}

static int
rel_set_type_param(mvc *sql, sql_subtype *type, sql_rel *rel, sql_exp *rel_exp, int upcast)
{
	if (!type || !rel_exp || (rel_exp->type != e_atom && rel_exp->type != e_column))
		return -1;

	/* use largest numeric types */
	if (upcast && type->type->eclass == EC_NUM) 
#ifdef HAVE_HGE
		type = sql_bind_localtype(have_hge ? "hge" : "lng");
#else
		type = sql_bind_localtype("lng");
#endif
	if (upcast && type->type->eclass == EC_FLT) 
		type = sql_bind_localtype("dbl");

	if ((rel_exp->type == e_atom || rel_exp->type == e_column) && (rel_exp->l || rel_exp->r || rel_exp->f)) {
		/* it's not a parameter set possible parameters bellow */
		const char *relname = exp_relname(rel_exp), *expname = exp_name(rel_exp);
		return rel_set_type_recurse(sql, type, rel, &relname, &expname);
	} else if (set_type_param(sql, type, rel_exp->flag) == 0) {
		rel_exp->tpe = *type;
		return 0;
	}
	return -1;
}

/* try to do an in-place conversion
 *
 * in-place conversion is only possible if the exp is a variable.
 * This is only done to be able to map more cached queries onto the same
 * interface.
 */

static void
convert_atom(atom *a, sql_subtype *rt)
{
	if (atom_null(a)) {
		if (a->data.vtype != rt->type->localtype) {
			const void *p;

			a->data.vtype = rt->type->localtype;
			p = ATOMnilptr(a->data.vtype);
			VALset(&a->data, a->data.vtype, (ptr) p);
		}
	}
	a->tpe = *rt;
}

static sql_exp *
exp_convert_inplace(mvc *sql, sql_subtype *t, sql_exp *exp)
{
	atom *a;

	/* exclude named variables and variable lists */
	if (exp->type != e_atom || exp->r /* named */ || exp->f /* list */ || !exp->l /* not direct atom */) 
		return NULL;

	a = exp->l;
	if (t->scale && t->type->eclass != EC_FLT)
		return NULL;

	if (a && atom_cast(sql->sa, a, t)) {
		convert_atom(a, t);
		exp->tpe = *t;
		return exp;
	}
	return NULL;
}

static sql_exp *
rel_numeric_supertype(mvc *sql, sql_exp *e )
{
	sql_subtype *tp = exp_subtype(e);

	if (tp->type->eclass == EC_DEC) {
		sql_subtype *dtp = sql_bind_localtype("dbl");

		return rel_check_type(sql, dtp, NULL, e, type_cast);
	}
	if (tp->type->eclass == EC_NUM) {
#ifdef HAVE_HGE
		sql_subtype *ltp = sql_bind_localtype(have_hge ? "hge" : "lng");
#else
		sql_subtype *ltp = sql_bind_localtype("lng");
#endif

		return rel_check_type(sql, ltp, NULL, e, type_cast);
	}
	return e;
}

sql_exp *
rel_check_type(mvc *sql, sql_subtype *t, sql_rel *rel, sql_exp *exp, int tpe)
{
	int err = 0;
	sql_exp* nexp = NULL;
	sql_subtype *fromtype = exp_subtype(exp);

	if ((!fromtype || !fromtype->type) && rel_set_type_param(sql, t, rel, exp, 0) == 0)
		return exp;

	/* first try cheap internal (in-place) conversions ! */
	if ((nexp = exp_convert_inplace(sql, t, exp)) != NULL)
		return nexp;

	if (fromtype && subtype_cmp(t, fromtype) != 0) {
		int c = sql_type_convert(fromtype->type->eclass, t->type->eclass);
		if (!c ||
		   (c == 2 && tpe == type_set) || (c == 3 && tpe != type_cast)){
			err = 1;
		} else {
			exp = exp_convert(sql->sa, exp, fromtype, t);
		}
	}
	if (err) {
		sql_exp *res = sql_error( sql, 03, SQLSTATE(42000) "types %s(%u,%u) and %s(%u,%u) are not equal%s%s%s",
			fromtype->type->sqlname,
			fromtype->digits,
			fromtype->scale,
			t->type->sqlname,
			t->digits,
			t->scale,
			(exp->type == e_column ? " for column '" : ""),
			(exp->type == e_column ? exp_name(exp) : ""),
			(exp->type == e_column ? "'" : "")
		);
		return res;
	}
	return exp;
}

static sql_exp *
exp_scale_algebra(mvc *sql, sql_subfunc *f, sql_rel *rel, sql_exp *l, sql_exp *r)
{
	sql_subtype *lt = exp_subtype(l);
	sql_subtype *rt = exp_subtype(r);

	if (lt->type->scale == SCALE_FIX && rt->scale &&
		strcmp(f->func->imp, "/") == 0) {
		sql_subtype *res = f->res->h->data;
		int scale, digits, digL, scaleL;
		sql_subtype nlt;

		/* scale fixing may require a larger type ! */
		scaleL = (lt->scale < 3) ? 3 : lt->scale;
		scale = scaleL;
		scaleL += rt->scale;
		digL = lt->digits + (scaleL - lt->scale);
		digits = (digL > (int)rt->digits) ? digL : (int)rt->digits;

		/* HACK alert: digits should be less than max */
#ifdef HAVE_HGE
		if (have_hge) {
			if (res->type->radix == 10 && digits > 39)
				digits = 39;
			if (res->type->radix == 2 && digits > 128)
				digits = 128;
		} else
#endif
		{
			if (res->type->radix == 10 && digits > 19)
				digits = 19;
			if (res->type->radix == 2 && digits > 64)
				digits = 64;
		}

		sql_find_subtype(&nlt, lt->type->sqlname, digL, scaleL);
		l = rel_check_type( sql, &nlt, rel, l, type_equal);

		sql_find_subtype(res, lt->type->sqlname, digits, scale);
	}
	return l;
}

int
rel_convert_types(mvc *sql, sql_rel *ll, sql_rel *rr, sql_exp **L, sql_exp **R, int scale_fixing, int tpe)
{
	sql_exp *ls = *L;
	sql_exp *rs = *R;
	sql_subtype *lt = exp_subtype(ls);
	sql_subtype *rt = exp_subtype(rs);

	if (!rt && !lt) {
		sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");
		return -1;
	}
	if (rt && (!lt || !lt->type))
		 return rel_set_type_param(sql, rt, ll, ls, 0);
	if (lt && (!rt || !rt->type))
		 return rel_set_type_param(sql, lt, rr, rs, 0);

	if (rt && lt) {
		sql_subtype *i = lt;
		sql_subtype *r = rt;

		if (subtype_cmp(lt, rt) != 0 || (tpe == type_equal_no_any && (lt->type->localtype==0 || rt->type->localtype==0))) {
			sql_subtype super;

			supertype(&super, r, i);
			if (scale_fixing) {
				/* convert ls to super type */
				ls = rel_check_type(sql, &super, ll, ls, tpe);
				/* convert rs to super type */
				rs = rel_check_type(sql, &super, rr, rs, tpe);
			} else {
				/* convert ls to super type */
				super.scale = lt->scale;
				ls = rel_check_type(sql, &super, ll, ls, tpe);
				/* convert rs to super type */
				super.scale = rt->scale;
				rs = rel_check_type(sql, &super, rr, rs, tpe);
			}
		}
		*L = ls;
		*R = rs;
		if (!ls || !rs) {
			return -1;
		}
		return 0;
	}
	return -1;
}

static comp_type
compare_str2type( char *compare_op)
{
	comp_type type = cmp_filter;

	if (compare_op[0] == '=') {
		type = cmp_equal;
	} else if (compare_op[0] == '<') {
		type = cmp_lt;
		if (compare_op[1] != '\0') {
			if (compare_op[1] == '>')
				type = cmp_notequal;
			else if (compare_op[1] == '=')
				type = cmp_lte;
		}
	} else if (compare_op[0] == '>') {
		type = cmp_gt;
		if (compare_op[1] != '\0')
			if (compare_op[1] == '=')
				type = cmp_gte;
	}
	return type;
}

static sql_rel *
rel_filter(mvc *sql, sql_rel *rel, list *l, list *r, char *sname, char *filter_op, int anti )
{
	node *n;
	sql_exp *L = l->h->data, *R = r->h->data, *e = NULL;
	sql_subfunc *f = NULL;
	sql_schema *s = sql->session->schema;
	list *tl, *exps;

	exps = sa_list(sql->sa);
	tl = sa_list(sql->sa);
	for (n = l->h; n; n = n->next){
		sql_exp *e = n->data;

		list_append(exps, e);
		list_append(tl, exp_subtype(e));
	}
	for (n = r->h; n; n = n->next){
		sql_exp *e = n->data;

		list_append(exps, e);
		list_append(tl, exp_subtype(e));
	}
	if (sname)
		s = mvc_bind_schema(sql, sname);
	/* find filter function */
	f = sql_bind_func_(sql->sa, s, filter_op, tl, F_FILT);

	if (!f) 
		f = find_func(sql, s, filter_op, list_length(exps), F_FILT, NULL);
	if (f) {
		node *n,*m = f->func->ops->h;
		list *nexps = sa_list(sql->sa);

		for(n=l->h; m && n; m = m->next, n = n->next) {
			sql_arg *a = m->data;
			sql_exp *e = n->data;

			e = rel_check_type(sql, &a->type, rel, e, type_equal);
			if (!e)
				return NULL;
			list_append(nexps, e);
		}
		l = nexps;
		nexps = sa_list(sql->sa);
		for(n=r->h; m && n; m = m->next, n = n->next) {
			sql_arg *a = m->data;
			sql_exp *e = n->data;

			e = rel_check_type(sql, &a->type, rel, e, type_equal);
			if (!e)
				return NULL;
			list_append(nexps, e);
		}
		r = nexps;
	}
	if (!f) {
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such FILTER function '%s'", filter_op);
		return NULL;
	}
	e = exp_filter(sql->sa, l, r, f, anti);

	/* atom or row => select */
	if (exps_card(l) > rel->card) {
		sql_exp *ls = l->h->data;
		if (exp_name(ls))
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ls));
		else
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (exps_card(r) > rel->card) {
		sql_exp *rs = l->h->data;
		if (exp_name(rs))
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(rs));
		else
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (exps_card(r) <= CARD_ATOM && exps_are_atoms(r)) {
		if (exps_card(l) == exps_card(r) || rel->processed)  /* bin compare op */
			return rel_select(sql->sa, rel, e);

		if (/*is_semi(rel->op) ||*/ is_outerjoin(rel->op)) {
			if ((is_left(rel->op) || is_full(rel->op)) && rel_find_exp(rel->l, l->h->data)) {
				rel_join_add_exp(sql->sa, rel, e);
				return rel;
			} else if ((is_right(rel->op) || is_full(rel->op)) && rel_find_exp(rel->r, l->h->data)) {
				rel_join_add_exp(sql->sa, rel, e);
				return rel;
			}
			if (is_left(rel->op) && rel_find_exp(rel->r, l->h->data)) {
				rel->r = rel_push_select(sql, rel->r, L, e);
				return rel;
			} else if (is_right(rel->op) && rel_find_exp(rel->l, l->h->data)) {
				rel->l = rel_push_select(sql, rel->l, L, e);
				return rel;
			}
		}
		/* push select into the given relation */
		return rel_push_select(sql, rel, L, e);
	} else { /* join */
		sql_rel *r;
		if (/*is_semi(rel->op) ||*/ (is_outerjoin(rel->op) && !is_processed((rel)))) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		}
		/* push join into the given relation */
		if ((r = rel_push_join(sql, rel, L, R, NULL, e)) != NULL)
			return r;
		rel_join_add_exp(sql->sa, rel, e);
		return rel;
	}
}

static sql_rel *
rel_filter_exp_(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, char *filter_op, int anti )
{
	list *l = sa_list(sql->sa);
	list *r = sa_list(sql->sa);

	list_append(l, ls);
	list_append(r, rs);
	if (rs2)
		list_append(r, rs2);
	return rel_filter(sql, rel, l, r, "sys", filter_op, anti);
}

static sql_rel *
rel_compare_exp_(sql_query *query, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, int type, int anti, int quantifier)
{
	mvc *sql = query->sql;
	sql_exp *L = ls, *R = rs, *e = NULL;

	if (quantifier) {
		sql_exp *nl = rel_unop_(query, rel, rs, NULL, "isnull", card_value);
		e = rel_nop_(query, rel, rs, nl, rs2, NULL, NULL, (quantifier==1)?"any":"all", card_value);
		e = exp_compare(sql->sa, e, exp_atom_bool(sql->sa, 1), type_equal);
		if (anti) {
			anti = 0;
			e = rel_unop_(query, rel, e, NULL, "not", card_value);
		}
	} else if (!rs2) {
		if (ls->card < rs->card) {
			sql_exp *swap = ls;
	
			ls = rs;
			rs = swap;

			swap = L;
			L = R;
			R = swap;

			type = (int)swap_compare((comp_type)type);
		}
		if (!exp_subtype(ls) && !exp_subtype(rs))
			return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");
		if (rel_convert_types(sql, rel, rel, &ls, &rs, 1, type_equal_no_any) < 0)
			return NULL;
		e = exp_compare(sql->sa, ls, rs, type);
	} else {
		if ((rs = rel_check_type(sql, exp_subtype(ls), rel, rs, type_equal)) == NULL ||
	   	    (rs2 && (rs2 = rel_check_type(sql, exp_subtype(ls), rel, rs2, type_equal)) == NULL))
			return NULL;
		e = exp_compare2(sql->sa, ls, rs, rs2, type);
	}
	if (anti)
		set_anti(e);

	if (!rel && query_has_outer(query)) {
		/* for now only top of stack */
		return rel_select(sql->sa, query_fetch_outer(query, 0), e);
	}

	/* atom or row => select */
	if (ls->card > rel->card) {
		if (exp_name(ls))
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ls));
		else
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card > rel->card || (rs2 && rs2->card > rel->card)) {
		if (exp_name(rs))
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(rs));
		else
			return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card <= CARD_ATOM && (exp_is_atom(rs) || exp_has_freevar(sql, rs)) &&
	   (!rs2 || (rs2->card <= CARD_ATOM && (exp_is_atom(rs2) || exp_has_freevar(sql, rs2))))) {
		if ((ls->card == rs->card && !rs2) || rel->processed)  /* bin compare op */
			return rel_select(sql->sa, rel, e);

		if (/*is_semi(rel->op) ||*/ is_outerjoin(rel->op)) {
			if ((is_left(rel->op) || is_full(rel->op)) && rel_find_exp(rel->l, ls)) {
				rel_join_add_exp(sql->sa, rel, e);
				return rel;
			} else if ((is_right(rel->op) || is_full(rel->op)) && rel_find_exp(rel->r, ls)) {
				rel_join_add_exp(sql->sa, rel, e);
				return rel;
			}
			if (is_semi(rel->op)) {
				rel_join_add_exp(sql->sa, rel, e);
				return rel;
			}
			if (is_left(rel->op) && rel_find_exp(rel->r, ls)) {
				rel->r = rel_push_select(sql, rel->r, L, e);
				return rel;
			} else if (is_right(rel->op) && rel_find_exp(rel->l, ls)) {
				rel->l = rel_push_select(sql, rel->l, L, e);
				return rel;
			}
		}
		/* push select into the given relation */
		return rel_push_select(sql, rel, L, e);
	} else { /* join */
		sql_rel *r;
		if (/*is_semi(rel->op) ||*/ (is_outerjoin(rel->op) && !is_processed((rel)))) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		}
		/* push join into the given relation */
		if ((r = rel_push_join(sql, rel, L, R, rs2, e)) != NULL)
			return r;
		rel_join_add_exp(sql->sa, rel, e);
		return rel;
	}
}

static sql_rel *
rel_compare_exp(sql_query *query, sql_rel *rel, sql_exp *ls, sql_exp *rs, char *compare_op, sql_exp *esc, int reduce,
				int quantifier, int need_not)
{
	mvc *sql = query->sql;
	comp_type type = cmp_equal;

	if (!ls || !rs)
		return NULL;

	if (!quantifier && ((!rel && !query_has_outer(query)) || !reduce)) {
		/* TODO to handle filters here */
		sql_exp *e;

		if (rel_convert_types(sql, rel, rel, &ls, &rs, 1, type_equal) < 0)
			return NULL;
		e = rel_binop_(query, rel, ls, rs, NULL, compare_op, card_value);

		if (!e)
			return NULL;
		if (!reduce) {
			if (rel->op == op_project) {
				append(rel->exps, e);
			} else {
				list *exps = new_exp_list(sql->sa);

				append(exps, e);
				return rel_project(sql->sa, rel, exps);
			}
		} else {
			return rel_select(sql->sa, rel, e);
		}
	}
	if (quantifier) 
		rs = rel_binop_(query, rel, ls, rs, NULL, compare_op, card_value);
	type = compare_str2type(compare_op);
	if (type == cmp_filter) 
		return rel_filter_exp_(sql, rel, ls, rs, esc, compare_op, 0);
	return rel_compare_exp_(query, rel, ls, rs, esc, type, need_not, quantifier);
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

static sql_rel *
rel_compare(sql_query *query, sql_rel *rel, symbol *sc, symbol *lo, symbol *ro, symbol *ro2,
		char *compare_op, int f, exp_kind k, int quantifier)
{
	mvc *sql = query->sql;
	sql_exp *rs = NULL, *rs2 = NULL, *ls;
	comp_type cmp_type = compare_str2type(compare_op);
	exp_kind ek = {type_value, card_column, FALSE};
	int need_not = 0;

	if ((quantifier == 1 && cmp_type == cmp_equal) ||
	    (quantifier == 2 && cmp_type == cmp_notequal)) {
		dnode *n = sc->data.lval->h;
		dlist *dl = dlist_create(sql->sa);
		/* map into IN/NOT IN */
		sc->token = cmp_type==cmp_equal?SQL_IN:SQL_NOT_IN;
		n->next->type = type_list;
		n->next->data.lval = dl;
		n->next->next->next = NULL; /* remove quantifier */
		dl->h = n->next->next;
		n->next->next = NULL; /* (remove comparison) moved righthand side */ 
		return rel_logical_exp(query, rel, sc, f);
	}	
	/* <> ANY -> NOT (= ALL) */
	if (quantifier == 1 && cmp_type == cmp_notequal) {
		need_not = 1;
		quantifier = 2;
		cmp_type = cmp_equal;
		compare_op[0] = '=';
		compare_op[1] = 0;
	}

	if (!ro2 && lo->token == SQL_SELECT) { /* swap subquery to the right hand side */
		symbol *tmp = lo;

		lo = ro;
		ro = tmp;

		if (compare_op[0] == '>')
			compare_op[0] = '<';
		else if (compare_op[0] == '<')
			compare_op[0] = '>';
		cmp_type = swap_compare(cmp_type);
	}

	ls = rel_value_exp(query, &rel, lo, f, ek);
	if (!ls)
		return NULL;
	if (ls && rel && exp_has_freevar(sql, ls) && (is_sql_sel(f) || is_sql_having(f))) {
		ls = rel_project_add_exp(sql, rel, ls);
	}
	if (ro->token != SQL_SELECT) {
		rs = rel_value_exp(query, &rel, ro, f, ek);
		if (ro2) {
			rs2 = rel_value_exp(query, &rel, ro2, f, ek);
			if (!rs2)
				return NULL;
		}
	} else {
		/* first try without current relation, too see if there
		   are correlations with the outer relation */
		sql_rel *r = rel_subquery(query, NULL, ro, ek);

		/* NOT handled filter case */
		if (ro2)
			return NULL;
		if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = 0;
			query_push_outer(query, rel);
			r = rel_subquery(query, NULL, ro, ek);
			query_pop_outer(query);

			/* get inner queries result value, ie
			   get last expression of r */
			/*
			if (r && list_length(r->exps) != 2) 
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column\n");
				*/
			if (r) {
				rs = rel_lastexp(sql, r);

				if (r->card <= CARD_ATOM) {
					quantifier = 0; /* single value, ie. any/all -> simple compare operator */

				} else if (r->card > CARD_ATOM) {
					/* if single value (independed of relations), rewrite */
					if (is_project(r->op) && !r->l && r->exps && list_length(r->exps) == 1) {
						return rel_compare_exp(query, rel, ls, r->exps->h->data, compare_op, NULL, k.reduce, 0, 0);
					} else if (quantifier && r->card > CARD_ATOM) {
						/* flatten the quantifier */
						sql_subaggr *a;
	
						r = rel_groupby(sql, r, NULL);
						a = sql_bind_aggr(sql->sa, sql->session->schema, "null", exp_subtype(rs));
						assert(a);
						rs2 = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
						rs2 = rel_groupby_add_aggr(sql, r, rs2);
						if (compare_op[0] == '<')
					       		a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"max":"min", exp_subtype(rs));
						else if (compare_op[0] == '>')
					       		a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"min":"max", exp_subtype(rs));
						else /* (compare_op[0] == '=')*/ /* only = ALL */
					       		a = sql_bind_aggr(sql->sa, sql->session->schema, "all", exp_subtype(rs));
	
						rs = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
						//if (quantifier == 2 && (compare_op[0] == '<' || compare_op[0] == '>'))
							/* do not skip nulls in case of >,>=,<=,< */
							//append(rs1->l, exp_atom_bool(sql->sa, 0));
						rs = rel_groupby_add_aggr(sql, r, rs);
					} else if (r->card > CARD_ATOM) { 
						sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, compare_aggr_op(compare_op, quantifier), exp_subtype(rs));
	
						r = rel_groupby(sql, r, NULL);
	
						rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
						rs = rel_groupby_add_aggr(sql, r, rs);
						rs = exp_ref(sql->sa, rs);
					}
				}
				/*
				if (is_sql_sel(f) && r->card > CARD_ATOM && quantifier != 1) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, compare_aggr_op(compare_op, quantifier), exp_subtype(rs));
					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);

					r->r = rel_groupby(sql, r->r, NULL);
					rs = rel_groupby_add_aggr(sql, r->r, rs);
					rs = exp_ref(sql->sa, rs);
				}
				*/
				if (rel) { 
					rel = rel_crossproduct(sql->sa, rel, r, (!quantifier)?op_semi:op_join);
					set_dependent(rel);
				}
			}
		} else if (r) {
			if (list_length(r->exps) != 1) 
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column\n");

			rs = rel_lastexp(sql, r);
			if (r->card <= CARD_ATOM) {
				quantifier = 0; /* single value, ie. any/all -> simple compare operator */
			} else if (r->card > CARD_ATOM) {
				/* if single value (independed of relations), rewrite */
				if (is_project(r->op) && !r->l && r->exps && list_length(r->exps) == 1) {
					return rel_compare_exp(query, rel, ls, r->exps->h->data, compare_op, NULL, k.reduce, 0, 0);
				} else if (quantifier && r->card > CARD_ATOM) {
					/* flatten the quantifier */
					sql_subaggr *a;

					r = rel_groupby(sql, r, NULL);
					a = sql_bind_aggr(sql->sa, sql->session->schema, "null", exp_subtype(rs));
					assert(a);
					rs2 = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
					rs2 = rel_groupby_add_aggr(sql, r, rs2);
					if (compare_op[0] == '<')
					       	a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"max":"min", exp_subtype(rs));
					else if (compare_op[0] == '>')
					       	a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"min":"max", exp_subtype(rs));
					else /* (compare_op[0] == '=')*/ /* only = ALL */
					       	a = sql_bind_aggr(sql->sa, sql->session->schema, "all", exp_subtype(rs));

					rs = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
					//if (quantifier == 2 && (compare_op[0] == '<' || compare_op[0] == '>'))
						/* do not skip nulls in case of >,>=,<=,< */
						//append(rs1->l, exp_atom_bool(sql->sa, 0));
					rs = rel_groupby_add_aggr(sql, r, rs);
				} else if (r->card > CARD_ATOM) { 
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, compare_aggr_op(compare_op, quantifier), exp_subtype(rs));

					r = rel_groupby(sql, r, NULL);

					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
					rs = rel_groupby_add_aggr(sql, r, rs);
					rs = exp_ref(sql->sa, rs);
				}
			}
			rel = rel_crossproduct(sql->sa, rel, r, (!quantifier)?op_semi:op_join);
		}
	}
	if (!rs) 
		return NULL;
	return rel_compare_exp(query, rel, ls, rs, compare_op, rs2, k.reduce, quantifier, need_not);
}

static sql_exp*
_rel_nop(mvc *sql, sql_schema *s, char *fname, list *tl, sql_rel *rel, list *exps, sql_subtype *obj_type, int nr_args,
		 exp_kind ek)
{
	sql_subfunc *f = NULL;
	int table_func = (ek.card == card_relation);
	sql_ftype type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:
		   ((ek.card == card_relation)?F_UNION:F_FUNC));
	sql_ftype filt = (type == F_FUNC)?F_FILT:type;

	(void)filt;
	(void)nr_args;
	(void)obj_type;
	f = bind_func_(sql, s, fname, tl, type);
	if (f) {
		return exp_op(sql->sa, exps, f);
	} else if (list_length(tl)) { 
		int len, match = 0;
		list *funcs = sql_find_funcs(sql->sa, s, fname, list_length(tl), type); 
		if (!funcs)
			return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
		len = list_length(funcs);
		if (len > 1) {
			int i, score = 0; 
			node *n;

			for (i = 0, n = funcs->h; i<len; i++, n = n->next) {
				int cscore = score_func(n->data, tl);
				if (cscore > score) {
					score = cscore;
					match = i;
				}
			}
		}
		if (list_empty(funcs))
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);

		f = list_fetch(funcs, match);
		if (f->func->vararg) {
			return exp_op(sql->sa, exps, f);
		} else {
			node *n, *m;
			list *nexps = new_exp_list(sql->sa);
			sql_subtype *atp = NULL;
			sql_arg *aa = NULL;

			/* find largest any type argument */ 
			for (n = exps->h, m = f->func->ops->h; n && m; n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;

				if (!aa && a->type.type->eclass == EC_ANY) {
					atp = &e->tpe;
					aa = a;
				}
				if (aa && a->type.type->eclass == EC_ANY &&
				    e->tpe.type->localtype > atp->type->localtype){
					atp = &e->tpe;
					aa = a;
				}
			}
			for (n = exps->h, m = f->func->ops->h; n && m; n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;
				sql_subtype *ntp = &a->type;

				if (a->type.type->eclass == EC_ANY)
					ntp = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
				e = rel_check_type(sql, ntp, rel, e, type_equal);
				if (!e) {
					nexps = NULL;
					break;
				}
				if (table_func && e->card > CARD_ATOM) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e));

					e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, 0);
				}
				append(nexps, e);
			}
			/* dirty hack */
			if (f->res && aa)
				f->res->h->data = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
			if (nexps) 
				return exp_op(sql->sa, nexps, f);
		}
	}
	return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
}

static sql_exp *
rel_in_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, FALSE};

	dlist *dl = sc->data.lval;
	symbol *lo = dl->h->data.sym;
	dnode *n = dl->h->next;
	sql_rel *left = *rel, *right = NULL;
	sql_exp *l = NULL, *r = NULL;
	int vals_only = 1, l_is_value = 1, l_init = 0, l_used = 0, l_tuple = 0, l_group = 0;
	list *vals = NULL, *pexps = NULL;

	(void)l_tuple;

	/* no input, assume single value */
	if (!left) {
		left = *rel = rel_project_exp(sql->sa, exp_atom_bool(sql->sa, 1));
		reset_processed(left);
	}
	if (left && is_sql_sel(f) && !is_processed(left) && is_simple_project(left->op)) {
		pexps = left->exps;
		if (left->l) {
			left = left->l;
		} else {
			pexps = sa_list(sql->sa);
			l_init = 1;
		}
		if (!is_processed(left) && left->op == op_groupby)
			l_group = 1;
	}

	l = rel_value_exp(query, &left, lo, f, ek);
	if (!l)
		return NULL;
	if (l && exp_has_freevar(sql, l)) {
		l_is_value=0;
	}
	ek.card = card_set;

	if (n->type == type_list) {
		vals = sa_list(sql->sa);
		n = n->data.lval->h;
		for (; n; n = n->next) {
			sql_rel *z = NULL;

			r = rel_value_exp(query, &z, n->data.sym, f /* ie no result project */, ek);
			if (l && !r && l_init) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;

				r = rel_value_exp(query, &left, n->data.sym, f /* ie no result project */, ek);
				if (r && !pexps) 
					*rel = left; 
			}
			if (z && r) {
				sql_subaggr *ea = NULL;
				sql_exp *a, *id, *tid;
				list *exps;

				if (l_init || (l_group && is_project(left->op))) {
					l = rel_project_add_exp(sql, left, l);
					l = exp_ref(sql->sa, l);
				}
				if (!exp_is_atom(r)) {
					z = rel_label(sql, z, 0);
					r = rel_lastexp(sql, z);
				}
				z = rel_add_identity2(sql, z, &tid);
				tid = exp_ref(sql->sa, tid);
				exps = rel_projections(sql, left, NULL, 1/*keep names */, 1);
				left = rel_add_identity(sql, left, &id);
				id = exp_ref(sql->sa, id);
				left = rel_crossproduct(sql->sa, left, z, is_sql_sel(f)?op_left:op_join);
				if (!l_is_value || rel_has_freevar(sql, z))
					set_dependent(left);

				left = rel_groupby(sql, left, exp2list(sql->sa, id)); 
				left->exps = exps; 
				if (rel_convert_types(sql, left, z, &l, &r, 1, type_equal_no_any) < 0)
					return NULL;
				ea = sql_bind_aggr(sql->sa, sql->session->schema, sc->token==SQL_IN?"anyequal":"allnotequal", exp_subtype(r));
				a = exp_aggr1(sql->sa, l, ea, 0, 0, CARD_ATOM, 0);
				append(a->l, r);
				append(a->l, tid);
				if (!is_sql_sel(f))
					set_intern(a);
				r = rel_groupby_add_aggr(sql, left, a);
				r = exp_ref(sql->sa, r);

				if (is_sql_sel(f)) {
					if (pexps)
						left = rel_project(sql->sa, left, pexps);
					reset_processed(left);
#if 0
					/* value exp ie no select */
				} else {
					//rel_join_add_exp(sql->sa, left, r);
					left = rel_select(sql->sa, left, r);
#endif
				}
				*rel = left;
				return r;
			}
			if (l && !r && left) { /* possibly a (not) in function call */
				sql_rel *z = NULL;
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;

				query_push_outer(query, left);
				r = rel_value_exp(query, &z, n->data.sym, f /* ie no result project */, ek);
				query_pop_outer(query);
				if (z)
					r = rel_lastexp(sql, z);
				if (z && r) {
					sql_subaggr *ea = NULL;
					sql_exp *a, *id, *tid;
					list *exps;

					exps = rel_projections(sql, left, NULL, 1/*keep names */, 1);
					left = rel_add_identity(sql, left, &id);
					id = exp_ref(sql->sa, id);
					z = rel_add_identity2(sql, z, &tid);
					tid = exp_ref(sql->sa, tid);
					left = rel_crossproduct(sql->sa, left, z, is_sql_sel(f)?op_left:op_join);
					if (rel_has_freevar(sql, z))
						set_dependent(left);

					left = rel_groupby(sql, left, exp2list(sql->sa, id)); 
					left->exps = exps; 
					if (rel_convert_types(sql, left, z, &l, &r, 1, type_equal_no_any) < 0)
						return NULL;
					ea = sql_bind_aggr(sql->sa, sql->session->schema, sc->token==SQL_IN?"anyequal":"allnotequal", exp_subtype(r));
					a = exp_aggr1(sql->sa, l, ea, 0, 0, CARD_ATOM, 0);
					append(a->l, r);
					append(a->l, tid);
					r = rel_groupby_add_aggr(sql, left, a);
					r = exp_ref(sql->sa, r);

					if (is_sql_sel(f) && pexps) {
						left = rel_project(sql->sa, left, pexps);
						reset_processed(left);
					} else if (!is_sql_sel(f) && !is_sql_having(f)){
						rel_join_add_exp(sql->sa, left, r);
					}
					*rel = left;
					return r;
				}
			}
			if (!l || !r) 
				return NULL;
			append(vals, r);
		}
		if (vals_only) {
			sql_exp *e = NULL, *je = NULL;
			node *n;

			for(n=vals->h; n; n = n->next) {
				sql_exp *r = n->data, *ne;

				if (rel_convert_types(sql, NULL, NULL, &l, &r, 1, type_equal) < 0)
					return NULL;
				if (l_used) {
					sql_exp *ne = exp_compare(sql->sa, l, r, cmp_equal );
					if (e) {
						je = exp_or(sql->sa,
							append(sa_list(sql->sa), je),
							append(sa_list(sql->sa), ne), 0);
					} else {
						je = ne;
					}
				} 
				if (sc->token == SQL_NOT_IN)
					ne = rel_binop_(query, NULL, l, r, NULL, "<>", card_value);
				else
					ne = rel_binop_(query, NULL, l, r, NULL, "=", card_value);
				if (!e) {
					e = ne;
				} else if (sc->token == SQL_NOT_IN) {
					e = rel_binop_(query, NULL, e, ne, NULL, "and", card_value);
				} else {
					e = rel_binop_(query, NULL, e, ne, NULL, "or", card_value);
				}
			}
			if (l_used) 
				rel_join_add_exp(sql->sa, left, je);
			else if (!is_sql_sel(f) && !is_sql_farg(f)) {
				if (!is_select(left->op) || rel_is_ref(left))
					left = rel_select(sql->sa, left, e);
				else
					rel_select_add_exp(sql->sa, left, e);
			}
			if (!l_used && l_is_value && sc->token == SQL_NOT_IN) 
				set_anti(e);
			if (pexps) {
				if (!l_init)
					(*rel)->l = left;
				else /*if (l_used)*/
					*rel = left;
			} else {
				*rel = left;
			}
			return e;
		}
		if (right->processed)
			right = rel_label(sql, right, 0);
		right = rel_distinct(right);
		set_processed(right);
	} else {
		return sql_error(sql, 02, SQLSTATE(42000) "IN: missing inner query");
	}
	return NULL;
}

sql_exp *
rel_logical_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, FALSE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "SELECT: too many nested operators");

	switch (sc->token) {
	case SQL_OR:
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;

		sql_exp *ls = rel_logical_value_exp(query, rel, lo, f);
		sql_exp *rs = rel_logical_value_exp(query, rel, ro, f);

		if (!ls || !rs)
			return NULL;
		if (sc->token == SQL_OR)
			return rel_binop_(query, rel ? *rel : NULL, ls, rs, NULL, "or", card_value);
		else
			return rel_binop_(query, rel ? *rel : NULL, ls, rs, NULL, "and", card_value);
	}
	case SQL_FILTER:
		/* [ x,..] filter [ y,..] */
		/* todo add anti, [ x,..] not filter [ y,...] */
		/* no correlation */
	{
		dnode *ln = sc->data.lval->h->data.lval->h;
		dnode *rn = sc->data.lval->h->next->next->data.lval->h;
		dlist *filter_op = sc->data.lval->h->next->data.lval;
		char *fname = qname_fname(filter_op);
		char *sname = qname_schema(filter_op);
		list *exps, *tl;
		sql_schema *s = sql->session->schema;
		sql_subtype *obj_type = NULL;

		if (sname)
			s = mvc_bind_schema(sql, sname);

		exps = sa_list(sql->sa);
		tl = sa_list(sql->sa);
		for (; ln; ln = ln->next) {
			symbol *sym = ln->data.sym;

			sql_exp *e = rel_value_exp(query, rel, sym, f, ek);
			if (!e)
				return NULL;
			if (!obj_type)
				obj_type = exp_subtype(e);
			list_append(exps, e);
			append(tl, exp_subtype(e));
		}
		for (; rn; rn = rn->next) {
			symbol *sym = rn->data.sym;

			sql_exp *e = rel_value_exp(query, rel, sym, f, ek);
			if (!e)
				return NULL;
			list_append(exps, e);
			append(tl, exp_subtype(e));
		}
		/* find the predicate filter function */
		return _rel_nop(sql, s, fname, tl, rel ? *rel : NULL, exps, obj_type, list_length(exps), ek);
	}
	case SQL_COMPARE:
	{
		dnode *n = sc->data.lval->h;
		symbol *lo = n->data.sym;
		symbol *ro = n->next->next->data.sym;
		char *compare_op = n->next->data.sval;
		int quantifier = 0;

		sql_exp *rs = NULL, *ls;
		comp_type cmp_type = compare_str2type(compare_op);
		int need_not = 0;

		/* 
		 * = ANY -> IN, <> ALL -> NOT( = ANY) -> NOT IN
		 * = ALL -> all(groupby(all, nil)), <> ANY -> NOT ( = ALL )
		 */
		if (n->next->next->next)
			quantifier = n->next->next->next->data.i_val + 1; 
		assert(quantifier == 0 || quantifier == 1 || quantifier == 2);

		if ((quantifier == 1 && cmp_type == cmp_equal) ||
		    (quantifier == 2 && cmp_type == cmp_notequal)) {
			dlist *dl = dlist_create(sql->sa);
			/* map into IN/NOT IN */
			sc->token = cmp_type==cmp_equal?SQL_IN:SQL_NOT_IN;
			n->next->type = type_list;
			n->next->data.lval = dl;
			n->next->next->next = NULL; /* remove quantifier */
			dl->h = n->next->next;
			n->next->next = NULL; /* (remove comparison) moved righthand side */ 
			return rel_logical_value_exp(query, rel, sc, f);
		}	
		/* <> ANY -> NOT (= ALL) */
		if (quantifier == 1 && cmp_type == cmp_notequal) {
			need_not = 1;
			quantifier = 2;
			cmp_type = cmp_equal;
			compare_op[0] = '=';
			compare_op[1] = 0;
		}

		ls = rel_value_exp(query, rel, lo, f, ek);
		if (!ls)
			return NULL;

		if (ro->token != SQL_SELECT) {
			rs = rel_value_exp(query, rel, ro, f, ek);
			if (!rs)
				return NULL;
			if (rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &ls, &rs, 1, type_equal) < 0)
				return NULL;
			rs = rel_binop_(query, rel ? *rel : NULL, ls, rs, NULL, compare_op, card_value);
			if (need_not)
				rs = rel_unop_(query, rel ? *rel : NULL, rs, NULL, "not", card_value);
			return rs;
		} else {
			/* first try without current relation, too see if there
			are correlations with the outer relation */
			sql_rel *r = rel_subquery(query, NULL, ro, ek);
			sql_exp *rs2 = NULL;

			/* correlation, ie return new relation */
			if (!r && sql->session->status != -ERR_AMBIGUOUS) {
				sql_rel *outerp = NULL;

				if (*rel && is_sql_sel(f) && is_project((*rel)->op) && !is_processed((*rel))) {
					outerp = *rel;
					*rel = (*rel)->l;
				}
				if (!*rel)
					return NULL;

				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;

				query_push_outer(query, *rel);
				r = rel_subquery(query, NULL, ro, ek);
				query_pop_outer(query);

				/* get inner queries result value, ie
				   get last expression of r */
				if (r) {
					rs = rel_lastexp(sql, r);

					if (quantifier) {
						/* flatten the quantifier */
						sql_subaggr *a;
	
						r = rel_groupby(sql, r, NULL);
						a = sql_bind_aggr(sql->sa, sql->session->schema, "null", exp_subtype(rs));
						assert(a);
						rs2 = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
						rs2 = rel_groupby_add_aggr(sql, r, rs2);
						if (compare_op[0] == '<') /* todo handle <> */
					       		a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"max":"min", exp_subtype(rs));
						else if (compare_op[0] == '>')
					       		a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"min":"max", exp_subtype(rs));
						else /* (compare_op[0] == '=')*/ /* only = ALL */
					       		a = sql_bind_aggr(sql->sa, sql->session->schema, "all", exp_subtype(rs));
	
						rs = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
						//if (quantifier == 2 && (compare_op[0] == '<' || compare_op[0] == '>'))
							/* do not skip nulls in case of >,>=,<=,< */
							//append(rs1->l, exp_atom_bool(sql->sa, 0));
						rs = rel_groupby_add_aggr(sql, r, rs);
					} else if (is_sql_sel(f) && ek.card <= card_column && r->card > CARD_ATOM) {
						sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));
						rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);

						r = rel_groupby(sql, r, NULL);
						rs = rel_groupby_add_aggr(sql, r, rs);
						rs = exp_ref(sql->sa, rs);
					}
					/* remove empty projects */
					if (!is_processed(*rel) && is_sql_sel(f) && (*rel)->op == op_project && list_length((*rel)->exps) == 0 && !(*rel)->r && (*rel)->l) 
						*rel = (*rel)->l;

					*rel = rel_crossproduct(sql->sa, *rel, r, is_sql_sel(f)?op_left:op_join); 
					set_dependent(*rel);
					if (outerp) {
						outerp->l = *rel;
						*rel = outerp;
					}
				}
			} else if (r) {
				sql_rel *outerp = NULL;
				sql_rel *l = *rel;
				sql_exp *rls = ls;

				assert(is_project(r->op));
				if (list_length(r->exps) != 1)
					return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column\n");

				if (!l) {
					l = *rel = rel_project(sql->sa, NULL, new_exp_list(sql->sa));
					ls = rel_project_add_exp(sql, l, ls);
				} else if (is_sql_sel(f)) { /* allways add left side in case of selections phase */
#if 0
					if (!l->processed && is_project(l->op)) { /* add all expressions to the project */
						l->exps = list_merge(l->exps, rel_projections(sql, l->l, NULL, 1, 1), (fdup)NULL);
						l->exps = list_distinct(l->exps, (fcmp)exp_equal, (fdup)NULL);
					}
#endif
					if (*rel && (*rel)->l && is_sql_sel(f) && is_project((*rel)->op) && !is_processed((*rel))) {
						outerp = *rel;
						l = *rel = (*rel)->l;
					}
					if (!*rel)
						return NULL;
					if (!(rls = rel_find_exp(l, ls)) || rls == ls /* constant atom */)
					{
						if (!list_empty(l->exps))
							l = rel_project(sql->sa, l, rel_projections(sql, l, NULL, 1, 1));
						ls = rel_project_add_exp(sql, l, ls);
					}
				}
				rs = rel_lastexp(sql, r);
				if (quantifier && r->card > CARD_ATOM) {
					/* flatten the quantifier */
					sql_subaggr *a;

					r = rel_groupby(sql, r, NULL);
					a = sql_bind_aggr(sql->sa, sql->session->schema, "null", exp_subtype(rs));
					assert(a);
					rs2 = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
					rs2 = rel_groupby_add_aggr(sql, r, rs2);
					if (compare_op[0] == '<') /* todo handle <> */
					       	a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"max":"min", exp_subtype(rs));
					else if (compare_op[0] == '>')
					       	a = sql_bind_aggr(sql->sa, sql->session->schema, (quantifier==1)?"min":"max", exp_subtype(rs));
					else /* (compare_op[0] == '=')*/ /* only = ALL */
					       	a = sql_bind_aggr(sql->sa, sql->session->schema, "all", exp_subtype(rs));

					rs = exp_aggr1(sql->sa, rs, a, 0, 1, CARD_ATOM, 0);
					//if (quantifier == 2 && (compare_op[0] == '<' || compare_op[0] == '>'))
						/* do not skip nulls in case of >,>=,<=,< */
						//append(rs1->l, exp_atom_bool(sql->sa, 0));
					rs = rel_groupby_add_aggr(sql, r, rs);
				}
				if (r->card > CARD_ATOM) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));

					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
				}
				*rel = rel_crossproduct(sql->sa, l, r, op_join);
				if (outerp) {
					outerp->l = *rel;
					*rel = outerp;
				}
			}
			if (!rs) 
				return NULL;
			if (rel_convert_types(sql, rel ? *rel : NULL, r, &ls, &rs, 1, type_equal) < 0)
				return NULL;
			rs = rel_binop_(query, rel ? *rel : NULL, ls, rs, NULL, compare_op, card_value);
			if (rs2) { 
				sql_exp *nl = rel_unop_(query, rel ? *rel : NULL, rs, NULL, "isnull", card_value);
				rs = rel_nop_(query, rel ? *rel : NULL, rs, nl, rs2, NULL, NULL, (quantifier==1)?"any":"all", card_value);
			}
			if (need_not)
				rs = rel_unop_(query, rel ? *rel : NULL, rs, NULL, "not", card_value);
			return rs;
		}
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
		return rel_in_value_exp(query, rel, sc, f);
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
	{
		symbol *lo = sc->data.sym;
		sql_rel *orel = *rel, *sq = NULL;
		list *pexps = NULL;
		int needproj = 0, exists=(sc->token == SQL_EXISTS), is_value = is_sql_sel(f);
		sql_exp *le;

		if (ek.type == type_value)
			is_value = 1;

		/* no input, assume single value */
		if ((!orel || (is_project(orel->op) && !is_processed(orel) && !orel->l && list_empty(orel->exps))) && !query_has_outer(query))
			orel = *rel = rel_project_exp(sql->sa, exp_atom_bool(sql->sa, 1));
		ek.card = card_set;
		if (is_value && orel && is_project(orel->op) && !is_processed(orel)) {
			needproj = 1;
			pexps = orel->exps;
			*rel = orel->l;
		}

		le = rel_value_exp(query, &sq, lo, f, ek);
		if (!le && sql->session->status != -ERR_AMBIGUOUS) { /* correlated */
			sql_subaggr *ea = NULL;

			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = 0;

			query_push_outer(query, *rel);
			sq = rel_subquery(query, NULL, lo, ek);
			query_pop_outer(query);

			if (!sq)
				return NULL;

			if (*rel != orel) { /* remove proejct */
				orel->l = NULL;
				rel_destroy(orel);
			}

			//le = rel_lastexp(sql, sq);
			le = _rel_lastexp(sql, sq);
			if (is_value /*is_sql_sel(f)*/ && is_freevar(lastexp(sq))) {
				sql_exp *re, *jc, *null;

				re = rel_bound_exp(sql, sq);
				re = rel_project_add_exp(sql, sq, re);
				jc = rel_unop_(query, NULL, re, NULL, "isnull", card_value);
				null = exp_null(sql->sa, exp_subtype(le));
				le = rel_nop_(query, NULL, jc, null, le, NULL, NULL, "ifthenelse", card_value);
			}
			if (is_value) { /* aggr (not) exist */
				sq = rel_groupby(sql, sq, NULL);
				ea = sql_bind_aggr(sql->sa, sql->session->schema, exists?"exist":"not_exist", exp_subtype(le));
				le = exp_aggr1(sql->sa, le, ea, 0, 0, CARD_ATOM, 0);
				le = rel_groupby_add_aggr(sql, sq, le);
				le = exp_ref(sql->sa, le);
			} 
			*rel = rel_crossproduct(sql->sa, *rel, sq, is_value?op_left:exists?op_semi:op_anti); 
			set_dependent(*rel);
			if (*rel && needproj) {
				*rel = rel_project(sql->sa, *rel, pexps);
				reset_processed(*rel);
			} 
			return le;
		}
		if (!le)
			return NULL;

		le = rel_is_constant(&sq, le);

		if (!sq) {
			sql_subfunc *exists_func = NULL;
			
			if (exists)
				exists_func = sql_bind_func(sql->sa, sql->session->schema, "sql_exists", exp_subtype(le), NULL, F_FUNC);
			else
				exists_func = sql_bind_func(sql->sa, sql->session->schema, "sql_not_exists", exp_subtype(le), NULL, F_FUNC);

			if (!exists_func) 
				return sql_error(sql, 02, SQLSTATE(42000) "exist operator on type %s missing", exp_subtype(le)->type->sqlname);
			*rel = orel;
			return exp_unop(sql->sa, le, exists_func);
		} else {
			sql_subaggr *ea = NULL;

			if (*rel != orel) { /* remove proejct */
				orel->l = NULL;
				rel_destroy(orel);
			}

			if (!is_project(sq->op)) 
				sq = rel_project(sql->sa, sq, rel_projections(sql, sq, NULL, 1, 1));
			if (!exp_is_atom(le))
				le = _rel_lastexp(sql, sq);
			if (is_value) { /* aggr (not) exist */
				sq = rel_groupby(sql, sq, NULL);
				ea = sql_bind_aggr(sql->sa, sql->session->schema, exists?"exist":"not_exist", exp_subtype(le));
				le = exp_aggr1(sql->sa, le, ea, 0, 0, CARD_ATOM, 0);
				le = rel_groupby_add_aggr(sql, sq, le);
				le = exp_ref(sql->sa, le);
			}
			*rel = rel_crossproduct(sql->sa, *rel, sq, is_value?op_left:exists?op_semi:op_anti); 
			if (rel_has_freevar(sql, sq))
				set_dependent(*rel);
			if (*rel && needproj) {
				*rel = rel_project(sql->sa, *rel, pexps);
				reset_processed(*rel);
			} 
			return le;
		}
	}
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		int insensitive = sc->data.lval->h->next->next->data.i_val;
		int anti = (sc->token == SQL_NOT_LIKE) != (sc->data.lval->h->next->next->next->data.i_val != 0);
		sql_subtype *st = sql_bind_localtype("str");
		sql_exp *le = rel_value_exp(query, rel, lo, f, ek);
		sql_exp *re, *ee = NULL;
		char *like = insensitive ? (anti ? "not_ilike" : "ilike") : (anti ? "not_like" : "like");
		sql_schema *sys = mvc_bind_schema(sql, "sys");

		if (!le)
			return NULL;

		if (!exp_subtype(le)) 
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		lo = ro->data.lval->h->data.sym;
		/* like uses a single string pattern */
		ek.card = card_value;
		re = rel_value_exp(query, rel, lo, f, ek);
		if (!re)
			return NULL;
		if ((re = rel_check_type(sql, st, rel ? *rel : NULL, re, type_equal)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "LIKE: wrong type, should be string");
		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		}
		if (ee)
			return rel_nop_(query, rel ? *rel : NULL, le, re, ee, NULL, sys, like, card_value);
		return rel_binop_(query, rel ? *rel : NULL, le, re, sys, like, card_value);
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le = rel_value_exp(query, rel, lo, f, ek);
		sql_exp *re1 = rel_value_exp(query, rel, ro1, f, ek);
		sql_exp *re2 = rel_value_exp(query, rel, ro2, f, ek);
		sql_exp *e1 = NULL, *e2 = NULL;

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;

		if (rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &le, &re1, 1, type_equal) < 0 ||
			rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &le, &re2, 1, type_equal) < 0)
			return NULL;

		if (!re1 || !re2) 
			return NULL;

		if (symmetric) {
			sql_exp *tmp = NULL;
			sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(re1), exp_subtype(re2), F_FUNC);
			sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(re1), exp_subtype(re2), F_FUNC);

			if (!min || !max) {
				return sql_error(sql, 02, SQLSTATE(42000) "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
			}
			tmp = exp_binop(sql->sa, re1, re2, min);
			re2 = exp_binop(sql->sa, re1, re2, max);
			re1 = tmp;
		}

		if (sc->token == SQL_NOT_BETWEEN) {
			e1 = rel_binop_(query, rel ? *rel : NULL, le, re1, NULL, "<", card_value);
			e2 = rel_binop_(query, rel ? *rel : NULL, le, re2, NULL, ">", card_value);
		} else {
			e1 = rel_binop_(query, rel ? *rel : NULL, le, re1, NULL, ">=", card_value);
			e2 = rel_binop_(query, rel ? *rel : NULL, le, re2, NULL, "<=", card_value);
		}
		if (!e1 || !e2)
			return NULL;
		if (sc->token == SQL_NOT_BETWEEN) {
			return rel_binop_(query, rel ? *rel : NULL, e1, e2, NULL, "or", card_value);
		} else {
			return rel_binop_(query, rel ? *rel : NULL, e1, e2, NULL, "and", card_value);
		}
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *le = rel_value_exp(query, rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(query, rel ? *rel : NULL, le, NULL, "isnull", card_value);
		if (sc->token != SQL_IS_NULL)
			le = rel_unop_(query, rel ? *rel : NULL, le, NULL, "not", card_value);
		return le;
	}
	case SQL_NOT: {
		sql_exp *le = rel_logical_value_exp(query, rel, sc->data.sym, f);

		if (!le)
			return le;
		return rel_unop_(query, rel ? *rel : NULL, le, NULL, "not", card_value);
	}
	case SQL_ATOM: {
		/* TRUE or FALSE */
		AtomNode *an = (AtomNode *) sc;

		if (!an || !an->a) {
			assert(0);
			return exp_null(sql->sa, sql_bind_localtype("void"));
		} else {
			return exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		}
	}
	case SQL_IDENT:
	case SQL_COLUMN:
		return rel_column_ref(query, rel, sc, f);
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT: {
		*rel = rel_setquery(query, *rel, sc);
		if (*rel)
			return rel_lastexp(sql, *rel);
		return NULL;
	}
	case SQL_DEFAULT:
		return sql_error(sql, 02, SQLSTATE(42000) "DEFAULT keyword not allowed outside insert and update statements");
	default: {
		sql_exp *re, *le = rel_value_exp(query, rel, sc, f, ek);
		sql_subtype bt;

		if (!le)
			return NULL;
		re = exp_atom_bool(sql->sa, 1);
		sql_find_subtype(&bt, "boolean", 0, 0);
		if ((le = rel_check_type(sql, &bt, rel ? *rel : NULL, le, type_equal)) == NULL)
			return NULL;
		return rel_binop_(query, rel ? *rel : NULL, le, re, NULL, "=", 0);
	}
	}
}

static sql_rel *
rel_in_exp(sql_query *query, sql_rel *rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, TRUE};
	dlist *dl = sc->data.lval;
	symbol *lo = NULL;
	dnode *n = dl->h->next, *dn = NULL;
	sql_rel *left = NULL;
	sql_exp *l = NULL, *e, *r = NULL;
	list *vals = NULL, *ll = sa_list(sql->sa);
	int l_is_value = 1, l_outer = 0, l_used = 0, l_tuple = 0;
	list *pexps = NULL;

	/* no input, assume single value */
	if (!rel && !query_has_outer(query)) 
		rel = rel_project_exp(sql->sa, exp_atom_bool(sql->sa, 1));
	left = rel;
	if (left && is_sql_sel(f) && !is_processed(left) && is_simple_project(left->op)) {
		pexps = left->exps;
		if (left->l) {
			left = left->l;
		} else {
			pexps = sa_list(sql->sa);
		}
	}

	/* complex case */
	if (dl->h->type == type_list) { /* (a,b..) in (.. ) */
		dn = dl->h->data.lval->h;
		lo = dn->data.sym;
		dn = dn->next;
	} else {
		lo = dl->h->data.sym;
	}
	while(lo) {
		l = rel_value_exp(query, &left, lo, f, ek);
		if (!l)
			return NULL;

		if (l && exp_has_freevar(sql, l)) {
			l_outer=1;
			l_is_value=0;
		}

		ek.card = card_set;
		append(ll, l);
		lo = NULL;
		if (dn) {
			lo = dn->data.sym;
			dn = dn->next;
			l_tuple = 1;
		}
	}
	if (list_length(ll) > 1)
		ek.card = card_relation;

	/* list of values or subqueries */
	if (n->type == type_list) {
		vals = sa_list(sql->sa);
		n = dl->h->next;
		n = n->data.lval->h;

		for (; n; n = n->next) {
			sql_rel *z = NULL;

			r = rel_value_exp(query, &z, n->data.sym, f /* ie no result project */, ek);
			if (!r) {
				sql_rel *oleft = left;
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;

				r = rel_value_exp(query, &left, n->data.sym, f /* ie no result project */, ek);
				if (r)
					l_used = is_join(left->op);
				if (oleft != left)
					l_outer = 1;
			}
			if (!r) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;

				query_push_outer(query, left);
				r = rel_value_exp(query, &z, n->data.sym, f /* ie no result project */, ek);
				query_pop_outer(query);
				if (!r)
					return NULL;
				if (z)
					l_used = is_join(z->op);
			}
			if (l_tuple && z) { /* later union them together (now just one) */
				node *n, *m;

				if (rel_has_freevar(sql, z)) {
					if (z->op == op_join) {
						if (!is_sql_sel(f))
							z->op = sc->token==SQL_IN?op_semi:op_anti;
						left = z;
						z = left->r;
					} else {
						left = rel_crossproduct(sql->sa, left, z, is_sql_sel(f)?op_left:op_join);
						if (!is_sql_sel(f))
							left->op = sc->token==SQL_IN?op_semi:op_anti;
					}
				} else {
					left = rel_crossproduct(sql->sa, left, z, is_sql_sel(f)?op_left:op_join);
					if (!is_sql_sel(f))
						left->op = sc->token==SQL_IN?op_semi:op_anti;
				}
				for (n = z->exps->h, m = ll->h; n && m; n = n->next, m = m->next) {
					sql_exp *l = m->data;
					sql_exp *r = n->data;

					if (rel_convert_types(sql, left, z, &l, &r, 1, type_equal_no_any) < 0)
						return NULL;
					exp_label(sql->sa, r, ++sql->label);
					r = exp_ref(sql->sa, r);
					e = exp_compare(sql->sa, l, r, sc->token==SQL_IN?mark_in:mark_notin); 
					rel_join_add_exp(sql->sa, left, e);
				}
				if (is_sql_sel(f)) {
					if (pexps)
						left = rel_project(sql->sa, left, pexps);
					reset_processed(left);
				}
				rel = left;
				return rel;
			} else if ((z || l_used) && r) { /* left is single value/column */
				if (!is_sql_sel(f)) {
					if (z) {
						if (exp_is_atom(r) && is_simple_project(z->op) && !z->l) 
							rel_project_add_exp(sql, z, r);
						r = rel_lastexp(sql, z);
					}
					if (rel_convert_types(sql, left, z, &l, &r, 1, type_equal_no_any) < 0)
						return NULL;
					r = exp_compare(sql->sa, l, r, (z||l_outer)?(sc->token==SQL_IN?mark_in:mark_notin):
									 (sc->token==SQL_IN?cmp_equal:cmp_notequal)); 
					if (z) {
						left = rel_crossproduct(sql->sa, left, z, sc->token==SQL_IN?op_semi:op_anti);
						if (rel_has_freevar(sql, z))
							set_dependent(left);
					} else if (l_outer) {
						left->op = sc->token==SQL_IN?op_semi:op_anti;
					}
					rel_join_add_exp(sql->sa, left, r);
					return left;
				} else {
					sql_subaggr *ea = NULL;
					sql_exp *a, *tid;

					if (!exp_is_atom(l) && !exp_name(l)) {
						left = rel_project(sql->sa, left, rel_projections(sql, left, NULL, 1, 1));
						rel_project_add_exp(sql, left, l);
						exp_label(sql->sa, l, ++sql->label);
						l = exp_ref(sql->sa, l);
					} else if (!exp_is_atom(l) && exp_name(l)) {
						l = exp_ref(sql->sa, l);
					}
					set_freevar(l);
					/* label to solve name conflicts with outer query */
					z = rel_add_identity2(sql, z, &tid);
					tid = exp_ref(sql->sa, tid);
					if (!exp_is_atom(r) && exp_name(r)) {
						z = rel_label(sql, z, 0);
						r = _rel_lastexp(sql, z);
					}
					if (rel_convert_types(sql, left, z, &l, &r, 1, type_equal_no_any) < 0)
						return NULL;
					ea = sql_bind_aggr(sql->sa, sql->session->schema, sc->token==SQL_IN?"anyequal":"allnotequal", exp_subtype(r));
					a = exp_aggr1(sql->sa, l, ea, 0, 0, CARD_ATOM, 0);
					append(a->l, r);
					append(a->l, tid);
					z = rel_groupby(sql, z, NULL); 
					r = rel_groupby_add_aggr(sql, z, a);
					set_processed(z);
					left = rel_crossproduct(sql->sa, left, z, op_left);
					set_dependent(left);

					if (pexps)
						left = rel_project(sql->sa, left, pexps);
					rel_project_add_exp(sql, left, r);
					reset_processed(left);
					rel = left;
					return rel;
				}
			}
			if (l_is_value) {
				sql_exp *e;

				l = ll->h->data;
				if (rel_convert_types(sql, left, z, &l, &r, 1, type_equal_no_any) < 0)
					return NULL;
				e = exp_compare(sql->sa, l, r, cmp_equal );
				if (!e)
					return NULL;
				list_append(vals, e);
			} else {
				list_append(vals, r);
			}
		}
		if (!l_tuple && !l_outer && list_length(vals)) { /* correct types */
			sql_subtype *st;
			list *nvals = new_exp_list(sql->sa);
			node *n;

			if (list_length(ll) != 1)
				return sql_error(sql, 02, SQLSTATE(42000) "IN: incorrect left hand side");

			if (!l_is_value) {
				l = ll->h->data;
				/* convert types first (l type maybe unknown) */
				n = vals->h;
				r = n->data;
				if (rel_convert_types(sql, left, NULL, &l, &r, 1, type_equal) < 0)
					return NULL;
				list_append(nvals, r);
				st = exp_subtype(l);
				for (; n; n = n->next) {
					if ((r = rel_check_type(sql, st, NULL, n->data, type_equal)) == NULL)
						return NULL;
					list_append(nvals, r);
				}
				e = exp_in(sql->sa, l, nvals, sc->token==SQL_NOT_IN?cmp_notin:cmp_in);
			} else {
				sql_exp *cur = NULL;
				for (n = vals->h; n; n = n->next) {
					sql_exp *r = (sql_exp*)n->data;
					if (cur) {
						cur = exp_or(sql->sa,
							list_append(sa_list(sql->sa), cur),
							list_append(sa_list(sql->sa), r), 0);
					} else {
						cur = r;
					}
				}
				e = cur;
			}
			if (l_used) 
				rel_join_add_exp(sql->sa, left, e);
			else if (!is_select(left->op) || rel_is_ref(left))
				left = rel_select(sql->sa, left, e);
			else
				rel_select_add_exp(sql->sa, left, e);
			if (!l_used && l_is_value && sc->token == SQL_NOT_IN) 
				set_anti(e);
			if (pexps){
				left = rel_project(sql->sa, left, pexps);
				rel->l = left;
			} else {
				rel = left;
			}
			return rel;
		}
	} else {
		return sql_error(sql, 02, SQLSTATE(42000) "IN: missing inner query");
	}
	return NULL;
}

sql_rel *
rel_logical_exp(sql_query *query, sql_rel *rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, TRUE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "SELECT: too many nested operators");

	switch (sc->token) {
	case SQL_OR:
	{
		list *exps = NULL, *lexps = NULL, *rexps = NULL;

		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;

		sql_rel *lr, *rr;

		if (!rel)
			return NULL;

		lr = rel;
		rr = rel_dup(lr);

		if (is_outerjoin(rel->op) && !is_processed(rel)) {
			int pushdown = sql->pushdown;

			exps = rel->exps;
			sql->pushdown = 0;

			lr = rel_select_copy(sql->sa, lr, sa_list(sql->sa));
			lr = rel_logical_exp(query, lr, lo, f);
			rr = rel_select_copy(sql->sa, rr, sa_list(sql->sa));
			rr = rel_logical_exp(query, rr, ro, f);
			if (lr && rr && lr->l == rr->l) {
				lexps = lr->exps;
				lr = lr->l;
				rexps = rr->exps;
				rr = rr->l;
			}
			rel = NULL;
			sql->pushdown = pushdown;
		} else {
			lr = rel_logical_exp(query, lr, lo, f);
			rr = rel_logical_exp(query, rr, ro, f);
		}

		if (!lr || !rr)
			return NULL;
		return rel_or(sql, rel, lr, rr, exps, lexps, rexps);
	}
	case SQL_AND:
	{
		/* split into 2 lists, simle logical expressions and or's */
		list *nors = sa_list(sql->sa);
		list *ors = sa_list(sql->sa);

		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		node *n;

		while (lo->token == SQL_AND) {
			symbol *s;

			sc = lo;
			lo = sc->data.lval->h->data.sym;
			s = sc->data.lval->h->next->data.sym;

			if (s->token != SQL_OR)
				list_prepend(nors, s);
			else 
				list_prepend(ors, s);
		}
		if (lo->token != SQL_OR)
			list_prepend(nors, lo);
		else 
			list_prepend(ors, lo);
		if (ro->token != SQL_OR)
			append(nors, ro);
		else 
			append(ors, ro);

		for(n=nors->h; n; n = n->next) {
			symbol *lo = n->data;
			rel = rel_logical_exp(query, rel, lo, f);
			if (!rel)
				return NULL;
		}
		for(n=ors->h; n; n = n->next) {
			symbol *lo = n->data;
			rel = rel_logical_exp(query, rel, lo, f);
			if (!rel)
				return NULL;
		}
		return rel;
		/*
		rel = rel_logical_exp(query, rel, lo, f);
		if (!rel)
			return NULL;
		return rel_logical_exp(query, rel, ro, f);
		*/
	}
	case SQL_FILTER:
		/* [ x,..] filter [ y,..] */
		/* todo add anti, [ x,..] NOT filter [ y,...] */
		/* no correlation */
	{
		dnode *ln = sc->data.lval->h->data.lval->h;
		dnode *rn = sc->data.lval->h->next->next->data.lval->h;
		dlist *filter_op = sc->data.lval->h->next->data.lval;
		char *fname = qname_fname(filter_op);
		char *sname = qname_schema(filter_op);
		list *l, *r;

		l = sa_list(sql->sa);
		r = sa_list(sql->sa);
		for (; ln; ln = ln->next) {
			symbol *sym = ln->data.sym;

			sql_exp *e = rel_value_exp(query, &rel, sym, f, ek);
			if (!e)
				return NULL;
			list_append(l, e);
		}
		for (; rn; rn = rn->next) {
			symbol *sym = rn->data.sym;

			sql_exp *e = rel_value_exp(query, &rel, sym, f, ek);
			if (!e)
				return NULL;
			list_append(r, e);
		}
		return rel_filter(sql, rel, l, r, sname, fname, 0);
	}
	case SQL_COMPARE:
	{
		dnode *n = sc->data.lval->h;
		symbol *lo = n->data.sym;
		symbol *ro = n->next->next->data.sym;
		char *compare_op = n->next->data.sval;
		int quantifier = 0;

		/* 
		 * = ANY -> IN, <> ALL -> NOT( = ANY) -> NOT IN
		 * = ALL -> all(groupby(all, nil)), <> ANY -> NOT ( = ALL )
		 */
		if (n->next->next->next)
			quantifier = n->next->next->next->data.i_val + 1; 
		assert(quantifier == 0 || quantifier == 1 || quantifier == 2);
		return rel_compare(query, rel, sc, lo, ro, NULL, compare_op, f, ek, quantifier);
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
		return rel_in_exp(query, rel, sc, f);
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
	{
		symbol *lo = sc->data.sym;
		sql_rel *orel = rel, *sq = NULL;
		list *pexps = NULL;
		int needproj = 0, exists=(sc->token == SQL_EXISTS);

		ek.card = card_set;
		if (orel && is_project(orel->op) && !is_processed(orel)) {
			needproj = 1;
			pexps = orel->exps;
			rel = orel->l;
		}

		sq = rel_subquery(query, NULL, lo, ek);
		if (!sq && rel && sql->session->status != -ERR_AMBIGUOUS) { /* correlation */
			sql_subaggr *ea = NULL;
			sql_exp *le;

			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';

			query_push_outer(query, rel);
			sq = rel_subquery(query, NULL, lo, ek);
			query_pop_outer(query);

			if (!sq)
				return NULL;

			if (rel != orel) { /* remove project */
				orel->l = NULL;
				rel_destroy(orel);
			}

			//le = rel_lastexp(sql, sq);
			le = _rel_lastexp(sql, sq);
			if (is_sql_sel(f)) { /* aggr (not) exist */
				sq = rel_groupby(sql, sq, NULL);
				ea = sql_bind_aggr(sql->sa, sql->session->schema, exists?"exist":"not_exist", exp_subtype(le));
				le = exp_aggr1(sql->sa, le, ea, 0, 0, CARD_ATOM, 0);
				le = rel_groupby_add_aggr(sql, sq, le);
				le = exp_ref(sql->sa, le);
			}
			rel = rel_crossproduct(sql->sa, rel, sq, is_sql_sel(f)?op_left:exists?op_semi:op_anti); 
			set_dependent(rel);
			if (rel && needproj) {
				rel = rel_project(sql->sa, rel, pexps);
				reset_processed(rel);
			} 
			return rel;
		}
		if (!sq || !rel)
			return NULL;
		if (!rel)
			assert(0);
		rel = rel_crossproduct(sql->sa, rel, sq, op_join);
		if (sc->token == SQL_EXISTS) {
			rel->op = op_semi;
		} else {	
			rel->op = op_anti;
		}
		return rel;
	}
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		int insensitive = sc->data.lval->h->next->next->data.i_val;
		int anti = (sc->token == SQL_NOT_LIKE) != (sc->data.lval->h->next->next->next->data.i_val != 0);
		sql_subtype *st = sql_bind_localtype("str");
		sql_exp *le = rel_value_exp(query, &rel, lo, f, ek);
		sql_exp *re, *ee = NULL;

		if (!le)
			return NULL;

		if (!exp_subtype(le)) 
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		} else {
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, "")));
		}
		ro = ro->data.lval->h->data.sym;
		re = rel_value_exp(query, &rel, ro, f, ek);
		if (!re)
			return NULL;
		if ((re = rel_check_type(sql, st, rel, re, type_equal)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "LIKE: wrong type, should be string");
		if ((le = rel_check_type(sql, st, rel, le, type_equal)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "LIKE: wrong type, should be string");
		return rel_filter_exp_(sql, rel, le, re, ee, (insensitive ? "ilike" : "like"), anti);
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le = rel_value_exp(query, &rel, lo, f, ek);
		sql_exp *re1 = rel_value_exp(query, &rel, ro1, f, ek);
		sql_exp *re2 = rel_value_exp(query, &rel, ro2, f, ek);
		int flag = 0;

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;

		if (rel_convert_types(sql, rel, rel, &le, &re1, 1, type_equal) < 0 ||
		    rel_convert_types(sql, rel, rel, &le, &re2, 1, type_equal) < 0)
			return NULL;

		if (!re1 || !re2) 
			return NULL;

		/* for between 3 columns we use the between operator */
		if (symmetric && re1->card == CARD_ATOM && re2->card == CARD_ATOM) {
			sql_exp *tmp = NULL;
			sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(re1), exp_subtype(re2), F_FUNC);
			sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(re1), exp_subtype(re2), F_FUNC);

			if (!min || !max) {
				return sql_error(sql, 02, SQLSTATE(42000) "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
			}
			tmp = exp_binop(sql->sa, re1, re2, min);
			re2 = exp_binop(sql->sa, re1, re2, max);
			re1 = tmp;
			symmetric = 0;
			if (!re1 || !re2) 
				return NULL;
		}

		flag = (symmetric)?CMP_SYMMETRIC:0;

		if (le->card == CARD_ATOM) {
			sql_exp *e1, *e2;
			if (sc->token == SQL_NOT_BETWEEN) {
				e1 = rel_binop_(query, rel, le, re1, NULL, "<", card_value);
				e2 = rel_binop_(query, rel, le, re2, NULL, ">", card_value);
			} else {
				e1 = rel_binop_(query, rel, le, re1, NULL, ">=", card_value);
				e2 = rel_binop_(query, rel, le, re2, NULL, "<=", card_value);
			}
			if (!e1 || !e2)
				return NULL;
			if (sc->token == SQL_NOT_BETWEEN) {
				e1 = rel_binop_(query, rel, e1, e2, NULL, "or", card_value);
			} else {
				e1 = rel_binop_(query, rel, e1, e2, NULL, "and", card_value);
			}
			e2 = exp_atom_bool(sql->sa, 1);
			rel = rel_select(sql->sa, rel, exp_compare(sql->sa,  e1, e2, cmp_equal));
		} else if (sc->token == SQL_NOT_BETWEEN) {
			rel = rel_compare_exp_(query, rel, le, re1, re2, 3|CMP_BETWEEN|flag, 1, 0);
		} else {
			rel = rel_compare_exp_(query, rel, le, re1, re2, 3|CMP_BETWEEN|flag, 0, 0);
		}
		return rel;
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *re, *le = rel_value_exp(query, &rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(query, rel, le, NULL, "isnull", card_value);
		if (sc->token == SQL_IS_NULL)
			re = exp_atom_bool(sql->sa, 1);
		else
			re = exp_atom_bool(sql->sa, 0);
		le = exp_compare(sql->sa, le, re, cmp_equal);
		return rel_select(sql->sa, rel, le);
	}
	case SQL_NOT: {
		sql_exp *re, *le;
		switch (sc->data.sym->token) {
		case SQL_IN:
			sc->data.sym->token = SQL_NOT_IN;
			return rel_logical_exp(query, rel, sc->data.sym, f);
		case SQL_NOT_IN:
			sc->data.sym->token = SQL_IN;
			return rel_logical_exp(query, rel, sc->data.sym, f);
		/* todo also handle BETWEEN and EXISTS */
		default:
			break;
		} 
		le = rel_value_exp(query, &rel, sc->data.sym, f|sql_farg, ek);

		if (!le)
			return NULL;
		le = rel_unop_(query, rel, le, NULL, "not", card_value);
		if (le == NULL)
			return NULL;
		re = exp_atom_bool(sql->sa, 1);
		le = exp_compare(sql->sa, le, re, cmp_equal);
		return rel_select(sql->sa, rel, le);
	}
	case SQL_ATOM: {
		/* TRUE or FALSE */
		AtomNode *an = (AtomNode *) sc;
		sql_exp *e = exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		return rel_select(sql->sa, rel, e);
	}
	case SQL_IDENT:
	case SQL_COLUMN: {
		sql_rel *or = rel;
		sql_exp *e = rel_column_ref(query, &rel, sc, f);

		if (e) {
			sql_subtype bt;

			sql_find_subtype(&bt, "boolean", 0, 0);
			e = rel_check_type(sql, &bt, rel, e, type_equal);
		}
		if (!e || or != rel)
			return NULL;
		return rel_select(sql->sa, rel, e);
	}
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		return rel_setquery(query, rel, sc);
	case SQL_DEFAULT:
		return sql_error(sql, 02, SQLSTATE(42000) "DEFAULT keyword not allowed outside insert and update statements");
	default: {
		sql_exp *re, *le = rel_value_exp(query, &rel, sc, f, ek);

		if (!le)
			return NULL;
		re = exp_atom_bool(sql->sa, 1);
		if (rel_convert_types(sql, rel, NULL, &le, &re, 1, type_equal) < 0)
			return NULL;
		le = exp_compare(sql->sa, le, re, cmp_equal);
		return rel_select(sql->sa, rel, le);
	}
	}
	/* never reached, as all switch cases have a `return` */
}

static sql_exp *
rel_op(mvc *sql, symbol *se, exp_kind ek )
{
	dnode *l = se->data.lval->h;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	return rel_op_(sql, s, fname, ek);
}

sql_exp *
rel_unop_(sql_query *query, sql_rel *rel, sql_exp *e, sql_schema *s, char *fname, int card)
{
	mvc *sql = query->sql;
	sql_subfunc *f = NULL;
	sql_subtype *t = NULL;
	sql_ftype type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:
		   ((card == card_relation)?F_UNION:F_FUNC));

	if (!s)
		s = sql->session->schema;
	t = exp_subtype(e);
	f = bind_func(sql, s, fname, t, NULL, type);
	/* try to find the function without a type, and convert
	 * the value to the type needed by this function!
	 */
	if (!f && (f = find_func(sql, s, fname, 1, type, NULL)) != NULL && check_card(card, f)) {

		if (!f->func->vararg) {
			sql_arg *a = f->func->ops->h->data;

			e = rel_check_type(sql, &a->type, rel, e, type_equal);
		}
		if (!e) 
			f = NULL;
	}
	if (f && check_card(card, f)) {
		if (f->func->fix_scale == INOUT) {
			sql_subtype *res = f->res->h->data;
			res->digits = t->digits;
			res->scale = t->scale;
		}
		if (card == card_relation && e->card > CARD_ATOM) {
			sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e));

			e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, 0);
		}
		return exp_unop(sql->sa, e, f);
	} else if (e) {
		if (t) {
			char *type = t->type->sqlname;

			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such unary operator '%s(%s)'", fname, type);
		} else {
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such unary operator '%s(?)'", fname);
		}
	}
	return NULL;
}

static sql_exp * _rel_aggr(sql_query *query, sql_rel **rel, int distinct, sql_schema *s, char *aname, dnode *arguments, int f);
static sql_exp *rel_aggr(sql_query *query, sql_rel **rel, symbol *se, int f);

static sql_exp *
rel_unop(sql_query *query, sql_rel **rel, symbol *se, int fs, exp_kind ek)
{
	mvc *sql = query->sql;
	dnode *l = se->data.lval->h;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	sql_exp *e = NULL;
	sql_subfunc *f = NULL;
	sql_subtype *t = NULL;
	sql_ftype type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:F_FUNC);

	if (sname)
		s = mvc_bind_schema(sql, sname);

	if (!s)
		return NULL;
	f = find_func(sql, s, fname, 1, F_AGGR, NULL);
	if (f) { 
		e = rel_aggr(query, rel, se, fs);
		if (e)
			return e;
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
	}
	e = rel_value_exp(query, rel, l->next->data.sym, fs, iek);
	if (!e) {
		if (!f && *rel && (*rel)->card == CARD_AGGR) {
			if (is_sql_having(fs) || is_sql_orderby(fs))
				return NULL;
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such aggregate '%s'", fname);
		}
		return NULL;
	}

	t = exp_subtype(e);
	if (!t) {
		f = find_func(sql, s, fname, 1, type, NULL);
		if (!f)
			f = find_func(sql, s, fname, 1, F_AGGR, NULL);
		if (f) {
			sql_arg *a = f->func->ops->h->data;

			t = &a->type;
			if (rel_set_type_param(sql, t, rel ? *rel : NULL, e, 1) < 0)
				return NULL;
		}
	} else {
		f = bind_func(sql, s, fname, t, NULL, type);
		if (!f)
			f = bind_func(sql, s, fname, t, NULL, F_AGGR);
	}
	if (f && IS_AGGR(f->func))
		return _rel_aggr(query, rel, 0, s, fname, l->next, fs);

	if (f && type_has_tz(t) && f->func->fix_scale == SCALE_FIX) {
		/* set timezone (using msec (.3)) */
		sql_subtype *intsec = sql_bind_subtype(sql->sa, "sec_interval", 10 /*hour to second */, 3);
		atom *a = atom_int(sql->sa, intsec, sql->timezone);
		sql_exp *tz = exp_atom(sql->sa, a);

		e = rel_binop_(query, rel ? *rel : NULL, e, tz, NULL, "sql_add", ek.card);
		if (!e)
			return NULL;
	}
	return rel_unop_(query, rel ? *rel : NULL, e, s, fname, ek.card);
}

#define is_addition(fname) (strcmp(fname, "sql_add") == 0)
#define is_subtraction(fname) (strcmp(fname, "sql_sub") == 0)

sql_exp *
rel_binop_(sql_query *query, sql_rel *rel, sql_exp *l, sql_exp *r, sql_schema *s, char *fname, int card)
{
	mvc *sql = query->sql;
	sql_exp *res = NULL;
	sql_subtype *t1, *t2;
	sql_subfunc *f = NULL;
	sql_ftype type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:((card == card_relation)?F_UNION:F_FUNC));
	if (card == card_loader)
		card = card_none;
	t1 = exp_subtype(l);
	t2 = exp_subtype(r);

	if (!s)
		s = sql->session->schema;

	/* handle param's early */
	if (!t1 || !t2) {
		if (t2 && !t1 && rel_set_type_param(sql, t2, rel, l, 1) < 0)
			return NULL;
		if (t1 && !t2 && rel_set_type_param(sql, t1, rel, r, 1) < 0)
			return NULL;
		t1 = exp_subtype(l);
		t2 = exp_subtype(r);
	}

	if (!t1 || !t2)
		return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");

	if ((is_addition(fname) || is_subtraction(fname)) && 
		((t1->type->eclass == EC_NUM && t2->type->eclass == EC_NUM) ||
		 (t1->type->eclass == EC_BIT && t2->type->eclass == EC_BIT))) {
		sql_subtype ntp;

		sql_find_numeric(&ntp, t1->type->localtype, t1->digits+1);
		l = rel_check_type(sql, &ntp, rel, l, type_equal);
		sql_find_numeric(&ntp, t2->type->localtype, t2->digits+1);
		r = rel_check_type(sql, &ntp, rel, r, type_equal);
		t1 = exp_subtype(l);
		t2 = exp_subtype(r);
	}

	f = bind_func(sql, s, fname, t1, t2, type);
	if (!f && is_commutative(fname)) {
		f = bind_func(sql, s, fname, t2, t1, type);
		if (f) {
			sql_subtype *tmp = t1;
			t1 = t2;
			t2 = tmp;
			res = l;
			l = r;
			r = res;
		}
	}
	if (f && check_card(card,f)) {
		if (f->func->fix_scale == SCALE_FIX) {
			l = exp_fix_scale(sql, t2, l, 0, 0);
			r = exp_fix_scale(sql, t1, r, 0, 0);
		} else if (f->func->fix_scale == SCALE_EQ) {
			sql_arg *a1 = f->func->ops->h->data;
			sql_arg *a2 = f->func->ops->h->next->data;
			t1 = &a1->type;
			t2 = &a2->type;
			l = exp_fix_scale(sql, t1, l, 0, 0);
			r = exp_fix_scale(sql, t2, r, 0, 0);
		} else if (f->func->fix_scale == SCALE_DIV) {
			l = exp_scale_algebra(sql, f, rel, l, r);
		} else if (f->func->fix_scale == SCALE_MUL) {
			exp_sum_scales(f, l, r);
		} else if (f->func->fix_scale == DIGITS_ADD) {
			sql_subtype *res = f->res->h->data;
			res->digits = (t1->digits && t2->digits)?t1->digits + t2->digits:0;
		}
		if (card == card_relation && l->card > CARD_ATOM) {
			sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(l));

			l = exp_aggr1(sql->sa, l, zero_or_one, 0, 0, CARD_ATOM, 0);
		}
		if (card == card_relation && r->card > CARD_ATOM) {
			sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(r));

			r = exp_aggr1(sql->sa, r, zero_or_one, 0, 0, CARD_ATOM, 0);
		}
		/* bind types of l and r */
		t1 = exp_subtype(l);
		t2 = exp_subtype(r);
		if (t1->type->eclass == EC_ANY || t2->type->eclass == EC_ANY) {
			sql_exp *ol = l;
			sql_exp *or = r;

			if (t1->type->eclass == EC_ANY && t2->type->eclass == EC_ANY) {
				sql_subtype *s = sql_bind_localtype("str");
				l = rel_check_type(sql, s, rel, l, type_equal);
				r = rel_check_type(sql, s, rel, r, type_equal);
			} else if (t1->type->eclass == EC_ANY) {
				l = rel_check_type(sql, t2, rel, l, type_equal);
			} else {
				r = rel_check_type(sql, t1, rel, r, type_equal);
			}
			if (l && r) 
				return exp_binop(sql->sa, l, r, f);
			
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			f = NULL;

			l = ol;
			r = or;
		}
		if (f)
			return exp_binop(sql->sa, l, r, f);
	} else {
		sql_exp *ol = l;
		sql_exp *or = r;

		if (!EC_NUMBER(t1->type->eclass)) {
			sql_subfunc *prev = NULL;

			while((f = bind_member_func(sql, s, fname, t1, 2, prev)) != NULL) {
				/* try finding function based on first argument */
				node *m = f->func->ops->h;
				sql_arg *a = m->data;

				prev = f;
				if (!check_card(card,f))
					continue;

				l = rel_check_type(sql, &a->type, rel, l, type_equal);
				a = m->next->data;
				r = rel_check_type(sql, &a->type, rel, r, type_equal);
				if (l && r)
					return exp_binop(sql->sa, l, r, f);

				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				l = ol;
				r = or;
		    }
		}
		/* try finding function based on both arguments */
		if (rel_convert_types(sql, rel, rel, &l, &r, 1/*fix scale*/, type_equal) >= 0){
			/* try operators */
			t1 = exp_subtype(l);
			t2 = exp_subtype(r);
			f = bind_func(sql, s, fname, t1, t2, type);
			if (f && check_card(card,f)) {
				if (f->func->fix_scale == SCALE_FIX) {
					l = exp_fix_scale(sql, t2, l, 0, 0);
					r = exp_fix_scale(sql, t1, r, 0, 0);
				} else if (f->func->fix_scale == SCALE_EQ) {
					sql_arg *a1 = f->func->ops->h->data;
					sql_arg *a2 = f->func->ops->h->next->data;
					t1 = &a1->type;
					t2 = &a2->type;
					l = exp_fix_scale(sql, t1, l, 0, 0);
					r = exp_fix_scale(sql, t2, r, 0, 0);
				} else if (f->func->fix_scale == SCALE_DIV) {
					l = exp_scale_algebra(sql, f, rel, l, r);
				} else if (f->func->fix_scale == SCALE_MUL) {
					exp_sum_scales(f, l, r);
				} else if (f->func->fix_scale == DIGITS_ADD) {
					sql_subtype *res = f->res->h->data;
					res->digits = (t1->digits && t2->digits)?t1->digits + t2->digits:0;
				}
				return exp_binop(sql->sa, l, r, f);
			}
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		l = ol;
		r = or;
		t1 = exp_subtype(l);
		(void) exp_subtype(r);

		if ((f = bind_member_func(sql, s, fname, t1, 2, NULL)) != NULL && check_card(card,f)) {
			/* try finding function based on first argument */
			node *m = f->func->ops->h;
			sql_arg *a = m->data;

			l = rel_check_type(sql, &a->type, rel, l, type_equal);
			a = m->next->data;
			r = rel_check_type(sql, &a->type, rel, r, type_equal);
			if (l && r) 
				return exp_binop(sql->sa, l, r, f);
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		l = ol;
		r = or;
		/* everything failed, fall back to bind on function name only */
		if ((f = find_func(sql, s, fname, 2, type, NULL)) != NULL && check_card(card,f)) {

			if (!f->func->vararg) {
				node *m = f->func->ops->h;
				sql_arg *a = m->data;

				l = rel_check_type(sql, &a->type, rel, l, type_equal);
				a = m->next->data;
				r = rel_check_type(sql, &a->type, rel, r, type_equal);
			}
			if (l && r)
				return exp_binop(sql->sa, l, r, f);
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		l = ol;
		r = or;
	}
	res = sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such binary operator '%s(%s,%s)'", fname,
			exp_subtype(l)->type->sqlname,
			exp_subtype(r)->type->sqlname);
	return res;
}

static int
rel_check_card(sql_rel *rel, sql_exp *l , sql_exp *r)
{
	if (rel && rel->card == CARD_AGGR && l->card != r->card && l->card > CARD_ATOM && r->card > CARD_ATOM) {
		if(l->card == CARD_AGGR || r->card == CARD_AGGR)
			return 1;
	}
	return 0;
}

static sql_rel *
rel_find_groupby(sql_rel *groupby)
{
	if (groupby && !is_processed(groupby) && !is_base(groupby->op)) { 
		while(!is_processed(groupby) && !is_base(groupby->op)) {
			if (groupby->op == op_groupby || !groupby->l)
				break;
			if (groupby->l)
				groupby = groupby->l;
		}
		if (groupby && groupby->op == op_groupby)
			return groupby;
	}
	return NULL;
}

static sql_exp *
rel_binop(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	mvc *sql = query->sql;
	dnode *dl = se->data.lval->h;
	sql_exp *l, *r;
	sql_rel *orel = *rel;
	char *fname = qname_fname(dl->data.lval);
	char *sname = qname_schema(dl->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	sql_ftype type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:F_FUNC);

	sql_subfunc *sf = NULL;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (!s)
		return NULL;

	l = rel_value_exp(query, rel, dl->next->data.sym, f, iek);
	r = rel_value_exp(query, rel, dl->next->next->data.sym, f, iek);
	if (l && *rel && exp_card(l) > CARD_AGGR && rel_find_groupby(*rel)) {
		/* TODO fix error */
		return NULL;
	}

	if (!l || !r) {
		*rel = orel;
		sf = find_func(sql, s, fname, 2, F_AGGR, NULL);
	}
	if (!sf && (!l || !r) && *rel && (*rel)->card == CARD_AGGR) {
		if (is_sql_having(f) || is_sql_orderby(f))
			return NULL;
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such aggregate '%s'", fname);
	}
	if (!l && !r && sf) { /* possibly we cannot resolve the argument as the function maybe an aggregate */
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		return rel_aggr(query, rel, se, f);
	}
	if (type == F_FUNC) {
		sf = find_func(sql, s, fname, 2, F_AGGR, NULL);
		if (sf) {
			if (!l || !r) { /* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';
			}
			return _rel_aggr(query, rel, 0, s, fname, dl->next, f);
		}
	}

	if (!l || !r)
		return NULL;

	if (rel_check_card(*rel, l, r)) 
		return NULL;
	return rel_binop_(query, rel ? *rel : NULL, l, r, s, fname, ek.card);
}

sql_exp *
rel_nop_(sql_query *query, sql_rel *rel, sql_exp *a1, sql_exp *a2, sql_exp *a3, sql_exp *a4, sql_schema *s, char *fname,
		 int card)
{
	mvc *sql = query->sql;
	list *tl = sa_list(sql->sa);
	sql_subfunc *f = NULL;
	sql_ftype type = (card == card_none)?F_PROC:((card == card_relation)?F_UNION:F_FUNC);

	(void) rel;
	append(tl, exp_subtype(a1));
	append(tl, exp_subtype(a2));
	append(tl, exp_subtype(a3));
	if (a4)
		append(tl, exp_subtype(a4));

	if (!s)
		s = sql->session->schema;
	f = bind_func_(sql, s, fname, tl, type);
	if (!f)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
	if (!a4)
		return exp_op3(sql->sa, a1,a2,a3,f);
	return exp_op4(sql->sa, a1,a2,a3,a4,f);
}

static sql_exp *
rel_nop(sql_query *query, sql_rel **rel, symbol *se, int fs, exp_kind ek)
{
	mvc *sql = query->sql;
	int nr_args = 0;
	dnode *l = se->data.lval->h;
	dnode *ops = l->next->data.lval->h;
	list *exps = new_exp_list(sql->sa);
	list *tl = sa_list(sql->sa);
	sql_subfunc *f = NULL;
	sql_subtype *obj_type = NULL;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	int err = 0;

	for (; ops; ops = ops->next, nr_args++) {
		sql_exp *e = rel_value_exp(query, rel, ops->data.sym, fs, iek);
		sql_subtype *tpe;

		if (!e) 
			err = 1;
		append(exps, e);
		if (e) {
			tpe = exp_subtype(e);
			if (!nr_args)
				obj_type = tpe;
			append(tl, tpe);
		}
	}
	if (sname)
		s = mvc_bind_schema(sql, sname);
	
	/* first try aggregate */
	f = find_func(sql, s, fname, nr_args, F_AGGR, NULL);
	if (!f && err && *rel && (*rel)->card == CARD_AGGR) {
		if (is_sql_having(fs) || is_sql_orderby(fs))
			return NULL;
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such aggregate '%s'", fname);
	}
	if (f) {
		if (err) {
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
		}
		return _rel_aggr(query, rel, 0, s, fname, l->next->data.lval->h, fs);
	}
	if (err)
		return NULL;
	return _rel_nop(sql, s, fname, tl, rel ? *rel : NULL, exps, obj_type, nr_args, ek);
}

static void
rel_intermediates_add_exp(mvc *sql, sql_rel *p, sql_rel *op, sql_exp *in)
{
	while(op && p != op) {
		sql_rel *pp = op;

		while(pp->l && pp->l != p) 
			pp = pp->l;
		if (pp && pp->l == p && pp->op == op_project) {
			in = exp_ref(sql->sa, in);
			in = rel_project_add_exp(sql, pp, in);
		}
		p = pp;
	}
}

static sql_exp *
rel_aggr_intern(sql_query *query, sql_rel **rel, int distinct, sql_schema *s, char *aname, dnode *args, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_subaggr *a = NULL;
	int no_nil = 0, group = 0, freevar = 1, p2g = 0;
	sql_rel *groupby = *rel, *sel = NULL, *gr, *og = NULL;
	list *exps = NULL;

	/* find having select */
	if (groupby && !is_processed(groupby) && is_sql_having(f)) { 
		og = groupby;
		while(!is_processed(groupby) && !is_base(groupby->op)) {
			if (is_select(groupby->op) || !groupby->l)
				break;
			if (groupby->l)
				groupby = groupby->l;
		}
		if (groupby && is_select(groupby->op) && !is_processed(groupby)) {
			group = 1;
			sel = groupby;
			/* At the end we switch back to the old projection relation og. 
			 * During the partitioning and ordering we add the expressions to the intermediate relations. */
		}
		if (!sel)
			groupby = og;
		if (sel && sel->l)
			groupby = sel->l;
	}

	/* find groupby */
	if (groupby && !is_processed(groupby) && !is_base(groupby->op)) { 
		og = groupby;
		groupby = rel_find_groupby(groupby);
		if (groupby)
			group = 1;
		else
			groupby = og;
	}

	if (!groupby) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: missing group by",
				       uaname ? toUpperCopy(uaname, aname) : aname);
		if (uaname)
			GDKfree(uaname);
		return e;
	} else if (is_sql_groupby(f) || (is_sql_partitionby(f) && groupby->op != op_groupby)) {
		const char *clause = is_sql_groupby(f) ? "GROUP BY":"PARTITION BY";
		char *uaname = GDKmalloc(strlen(aname) + 1);
		sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate function '%s' not allowed in %s clause",
							   uaname ? toUpperCopy(uaname, aname) : aname, aname, clause);
		if (uaname)
			GDKfree(uaname);
		return e;
	} else if (!query_has_outer(query) && is_sql_where(f)) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in WHERE clause",
				       uaname ? toUpperCopy(uaname, aname) : aname);
		if (uaname)
			GDKfree(uaname);
		return e;
	} else if (is_sql_aggr(f)) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions cannot be nested",
				       uaname ? toUpperCopy(uaname, aname) : aname);
		if (uaname)
			GDKfree(uaname);
		return e;
	}

	if (groupby->op != op_groupby) { 		/* implicit groupby */
		sql_rel *np = rel_project2groupby(sql, groupby);

		p2g = 1;
		if (*rel == groupby) {
			*rel = np;
		} else {
			sql_rel *l = *rel;
			while(l->l && l->l != groupby) {
				l = l->l;
			}
			if (l->l && l->l == groupby)
				l->l = np;
		}
	}
	if (!*rel)
		return NULL;

	if (!args->data.sym) {	/* count(*) case */
		sql_exp *e;

		if (strcmp(aname, "count") != 0) {
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: unable to perform '%s(*)'",
					       uaname ? toUpperCopy(uaname, aname) : aname, aname);
			if (uaname)
				GDKfree(uaname);
			if (p2g)
				*rel = revert_project2groupby(*rel);
			return e;
		}
		a = sql_bind_aggr(sql->sa, s, aname, NULL);
		e = exp_aggr(sql->sa, NULL, a, distinct, 0, groupby->card, 0);

		if (is_sql_orderby(f)) {
			e = rel_groupby_add_aggr(sql, groupby, e);
			if (!group)
				return e;
		}
		if (*rel != groupby || !is_sql_sel(f)) { /* selection */
			sql_rel *l = NULL;

			if (!is_sql_orderby(f))
				e = rel_groupby_add_aggr(sql, groupby, e);
			if (!group)
				return e;
			if (og)
				l = og->l;
			if (l && is_sql_sel(f))
				rel_intermediates_add_exp(sql, groupby, l, e);
			else if (sel) {
				rel_intermediates_add_exp(sql, groupby, sel->l, e);
				if (sel != *rel)
					rel_intermediates_add_exp(sql, sel, *rel, e);
			} else
				rel_intermediates_add_exp(sql, groupby, *rel, e);
		}
		return e;
	} 

	exps = sa_list(sql->sa);

	/* use cnt as nils shouldn't be counted */
	gr = groupby;

	no_nil = 1;

	if (gr && gr->op == op_project && gr->l)
		gr = gr->l;
	for (	; args; args = args->next ) {
		sql_rel *gl = gr->l;
		sql_exp *e = rel_value_exp(query, &gl, args->data.sym, f | sql_aggr, ek);

		if (gl != gr->l) 
			gr->l = gl;
		if (!e || !exp_subtype(e)) { /* we also do not expect parameters here */
			if (p2g)
				*rel = revert_project2groupby(*rel);
			return NULL;
		}
		freevar &= exp_has_freevar(sql, e);
		list_append(exps, e);
	}

	a = sql_bind_aggr_(sql->sa, s, aname, exp_types(sql->sa, exps));
	if (!a && list_length(exps) > 1) { 
		sql_subtype *t1 = exp_subtype(exps->h->data);
		a = sql_bind_member_aggr(sql->sa, s, aname, exp_subtype(exps->h->data), list_length(exps));
		bool is_group_concat = (!a && strcmp(s->base.name, "sys") == 0 && strcmp(aname, "group_concat") == 0);

		if (list_length(exps) != 2 || (!EC_NUMBER(t1->type->eclass) || !a || is_group_concat || subtype_cmp(
						&((sql_arg*)a->aggr->ops->h->data)->type,
						&((sql_arg*)a->aggr->ops->h->next->data)->type) != 0) )  {
			if(!a && is_group_concat) {
				sql_subtype *tstr = sql_bind_localtype("str");
				list *sargs = sa_list(sql->sa);
				if (list_length(exps) >= 1)
					append(sargs, tstr);
				if (list_length(exps) == 2)
					append(sargs, tstr);
				a = sql_bind_aggr_(sql->sa, s, aname, sargs);
			}
			if (a) {
				node *n, *op = a->aggr->ops->h;
				list *nexps = sa_list(sql->sa);

				for (n = exps->h ; a && op && n; op = op->next, n = n->next ) {
					sql_arg *arg = op->data;
					sql_exp *e = n->data;

					e = rel_check_type(sql, &arg->type, *rel, e, type_equal); /* rel is a valid pointer */
					if (!e)
						a = NULL;
					list_append(nexps, e);
				}
				if (a && list_length(nexps))  /* count(col) has |exps| != |nexps| */
					exps = nexps;
			}
		} else {
			sql_exp *l = exps->h->data, *ol = l;
			sql_exp *r = exps->h->next->data, *or = r;
			sql_subtype *t2 = exp_subtype(r);

			if (rel_convert_types(sql, *rel, *rel, &l, &r, 1/*fix scale*/, type_equal) >= 0){
				list *tps = sa_list(sql->sa);

				t1 = exp_subtype(l);
				list_append(tps, t1);
				t2 = exp_subtype(r);
				list_append(tps, t2);
				a = sql_bind_aggr_(sql->sa, s, aname, tps);
			}
			if (!a) {
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				l = ol;
				r = or;
			} else {
				list *nexps = sa_list(sql->sa);

				append(nexps,l);
				append(nexps,r);
				exps = nexps;
			}
		}
	}
	if (!a) { /* find aggr + convert */
		/* try larger numeric type */
		node *n;
		list *nexps = sa_list(sql->sa);

		for (n = exps->h ;  n; n = n->next ) {
			sql_exp *e = n->data;

			/* cast up, for now just dec to double */
			e = rel_numeric_supertype(sql, e);
			if (!e)
				break;
			list_append(nexps, e);
		}
		a = sql_bind_aggr_(sql->sa, s, aname, exp_types(sql->sa, nexps));
		if (a && list_length(nexps))  /* count(col) has |exps| != |nexps| */
			exps = nexps;
		if (!a) {
			a = sql_find_aggr(sql->sa, s, aname);
			if (a) {
				node *n, *op = a->aggr->ops->h;
				list *nexps = sa_list(sql->sa);

				for (n = exps->h ; a && op && n; op = op->next, n = n->next ) {
					sql_arg *arg = op->data;
					sql_exp *e = n->data;

					e = rel_check_type(sql, &arg->type, *rel, e, type_equal); /* rel is a valid pointer */
					if (!e)
						a = NULL;
					list_append(nexps, e);
				}
				if (a && list_length(nexps))  /* count(col) has |exps| != |nexps| */
					exps = nexps;
			}
		}
	}
	if (a && execute_priv(sql,a->aggr)) {
		sql_exp *e = exp_aggr(sql->sa, exps, a, distinct, no_nil, groupby->card, have_nil(exps));

		if (is_sql_orderby(f)) {
			e = rel_groupby_add_aggr(sql, groupby, e);
			if (!group)
				return e;
		}
		if (*rel != groupby || !is_sql_sel(f)) { /* selection */
			sql_rel *l = NULL;

			if (!is_sql_orderby(f))
				e = rel_groupby_add_aggr(sql, groupby, e);
			if (!group)
				return e;
			if (og)
				l = og->l;
			if (l && is_sql_sel(f))
				rel_intermediates_add_exp(sql, groupby, l, e);
			else if (sel) {
				rel_intermediates_add_exp(sql, groupby, sel->l, e);
				if (sel != *rel)
					rel_intermediates_add_exp(sql, sel, *rel, e);
			} else
				rel_intermediates_add_exp(sql, groupby, *rel, e);
		}
		return e;
	} else {
		sql_exp *e;
		char *type = "unknown";
		char *uaname = GDKmalloc(strlen(aname) + 1);

		if (exps->h) {
			sql_exp *e = exps->h->data;
			type = exp_subtype(e)->type->sqlname;
		}

		e = sql_error(sql, 02, SQLSTATE(42000) "%s: no such operator '%s(%s)'",
			      uaname ? toUpperCopy(uaname, aname) : aname, aname, type);

		if (uaname)
			GDKfree(uaname);
		if (p2g)
			*rel = revert_project2groupby(*rel);
		return e;
	}
}

static sql_exp *
_rel_aggr(sql_query *query, sql_rel **rel, int distinct, sql_schema *s, char *aname, dnode *args, int f)
{
	sql_rel *orel = *rel;
	mvc *sql = query->sql;
	sql_query *iquery = query_create(sql);
	sql_exp *e = NULL;

	/* 3 cases: 
	 * 	1 no outer
	 * 		just call rel_aggr_intern
	 *  	2 outer aggr on outer column
	 *  		call aggr intern with outer as input argument
	 *		add reference too the result
	 *  	3 combined aggr on outer/inner
	 *  		call aggr intern with query, but set_processed(outer)
	 */
	e = rel_aggr_intern(iquery, rel, distinct, s, aname, args, f);
	if (!e && query_has_outer(query)) {
		sql_rel *outer = query_fetch_outer(query, 0);

		sql->session->status = 0;
		sql->errstr[0] = '\0';
		e = rel_aggr_intern(iquery, &outer, distinct, s, aname, args, sql_sel/*f*/);
		if (e) {
			if (!is_project(outer->op))
				outer = rel_project(sql->sa, outer, NULL);
			query->outer->values [0] = outer;
			exp_label(sql->sa, e, ++sql->label);
			e = rel_project_add_exp(sql, outer, e);
			e->card = CARD_ATOM;
			set_freevar(e);
		} else {
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			if (query_fetch_outer(query, 0)->op == op_groupby)
				set_processed(query_fetch_outer(query, 0));
			*rel = orel;
			e = rel_aggr_intern(query, rel, distinct, s, aname, args, f);
			reset_processed(query_fetch_outer(query, 0));
		}
	}
	return e;
}

static sql_exp *
rel_aggr(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;
	dnode *d = l->h->next;
	int distinct = 0;
	char *aname = qname_fname(l->h->data.lval);
	char *sname = qname_schema(l->h->data.lval);
	sql_schema *s = query->sql->session->schema;

	if (l->h->next->type == type_int) {
		distinct = l->h->next->data.i_val;
		d = l->h->next->next;
	}

	if (sname)
		s = mvc_bind_schema(query->sql, sname);
	return _rel_aggr( query, rel, distinct, s, aname, d, f);
}

static sql_exp *
rel_case(sql_query *query, sql_rel **rel, tokens token, symbol *opt_cond, dlist *when_search_list, symbol *opt_else, int f)
{
	mvc *sql = query->sql;
	sql_subtype *tpe = NULL;
	list *conds = new_exp_list(sql->sa);
	list *results = new_exp_list(sql->sa);
	dnode *dn = when_search_list->h;
	sql_subtype *restype = NULL, rtype, bt;
	sql_exp *res = NULL, *else_exp = NULL;
	node *n, *m;
	exp_kind ek = {type_value, card_column, FALSE};

	sql_find_subtype(&bt, "boolean", 0, 0);
	if (dn) {
		sql_exp *cond = NULL, *result = NULL;

		/* NULLIF(e1,e2) == CASE WHEN e1=e2 THEN NULL ELSE e1 END */
		if (token == SQL_NULLIF) {
			sql_exp *e1, *e2;

			e1 = rel_value_exp(query, rel, dn->data.sym, f, ek);
			e2 = rel_value_exp(query, rel, dn->next->data.sym, f, ek);
			if (e1 && e2) {
				cond = rel_binop_(query, rel ? *rel : NULL, e1, e2, NULL, "=", card_value);
				result = exp_null(sql->sa, exp_subtype(e1));
				else_exp = exp_copy(sql->sa, e1);	/* ELSE case */
			}
			/* COALESCE(e1,e2) == CASE WHEN e1
			   IS NOT NULL THEN e1 ELSE e2 END */
		} else if (token == SQL_COALESCE) {
			cond = rel_value_exp(query, rel, dn->data.sym, f, ek);

			if (cond) {
				result = exp_copy(sql->sa, cond);
				cond = rel_unop_(query, rel ? *rel : NULL, rel_unop_(query, rel ? *rel : NULL, cond, NULL, "isnull",
								 card_value), NULL, "not", card_value);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(query, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(query, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &l, &r, 1, type_equal) < 0)
					return NULL;
				cond = rel_binop_(query, rel ? *rel : NULL, l, r, NULL, "=", card_value);
			} else {
				cond = rel_logical_value_exp(query, rel, when->h->data.sym, f);
			}
			result = rel_value_exp(query, rel, when->h->next->data.sym, f, ek);
		}
		if (!cond || !result) 
			return NULL;
		list_prepend(conds, cond);
		list_prepend(results, result);

		restype = exp_subtype(result);

		if (token == SQL_NULLIF)
			dn = NULL;
		else
			dn = dn->next;
	}
	if (!restype) 
		return sql_error(sql, 02, SQLSTATE(42000) "result type missing");
	/* for COALESCE we skip the last (else part) */
	for (; dn && (token != SQL_COALESCE || dn->next); dn = dn->next) {
		sql_exp *cond = NULL, *result = NULL;

		if (token == SQL_COALESCE) {
			cond = rel_value_exp(query, rel, dn->data.sym, f, ek);

			if (cond) {
				result = exp_copy(sql->sa, cond);
				cond = rel_unop_(query, rel ? *rel : NULL, rel_unop_(query, rel ? *rel : NULL, cond, NULL, "isnull",
								 card_value), NULL, "not", card_value);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(query, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(query, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &l, &r, 1, type_equal) < 0)
					return NULL;
				cond = rel_binop_(query, rel ? *rel : NULL, l, r, NULL, "=", card_value);
			} else {
				cond = rel_logical_value_exp(query, rel, when->h->data.sym, f);
			}
			result = rel_value_exp(query, rel, when->h->next->data.sym, f, ek);
		}
		if (!cond || !result) 
			return NULL;
		list_prepend(conds, cond);
		list_prepend(results, result);

		tpe = exp_subtype(result);
		if (!tpe) 
			return sql_error(sql, 02, SQLSTATE(42000) "result type missing");
		supertype(&rtype, restype, tpe);
		restype = &rtype;
	}
	if (opt_else || else_exp) {
		sql_exp *result = else_exp;

		if (!result && !(result = rel_value_exp(query, rel, opt_else, f, ek))) 
			return NULL;

		tpe = exp_subtype(result);
		if (tpe && restype) {
			supertype(&rtype, restype, tpe);
			tpe = &rtype;
		}
		restype = tpe;
		if (restype->type->localtype == TYPE_void) /* NULL */
			restype = sql_bind_localtype("str");

		if (!result || !(result = rel_check_type(sql, restype, rel ? *rel : NULL, result, type_equal)))
			return NULL;
		res = result;

		if (!res) 
			return NULL;
	} else {
		if (restype->type->localtype == TYPE_void) /* NULL */
			restype = sql_bind_localtype("str");
		res = exp_null(sql->sa, restype);
	}

	for (n = conds->h, m = results->h; n && m; n = n->next, m = m->next) {
		sql_exp *cond = n->data;
		sql_exp *result = m->data;

		if (!(result = rel_check_type(sql, restype, rel ? *rel : NULL, result, type_equal)))
			return NULL;

		if (!(cond = rel_check_type(sql, &bt, rel ? *rel : NULL, cond, type_equal)))
			return NULL;

		/* remove any null's in the condition */
		if (has_nil(cond) && token != SQL_COALESCE) {
			sql_exp *condnil = rel_unop_(query, rel ? *rel : NULL, cond, NULL, "isnull", card_value);
			cond = exp_copy(sql->sa, cond);
			cond = rel_nop_(query, rel ? *rel : NULL, condnil, exp_atom_bool(sql->sa, 0), cond, NULL, NULL,
							"ifthenelse", card_value);
		}
		if (!cond || !result || !res)
			return NULL;
		res = rel_nop_(query, rel ? *rel : NULL, cond, result, res, NULL, NULL, "ifthenelse", card_value);
		if (!res) 
			return NULL;
		/* ugh overwrite res type */
		((sql_subfunc*)res->f)->res->h->data = sql_create_subtype(sql->sa, restype->type, restype->digits, restype->scale);
	}
	return res;
}

static sql_exp *
rel_case_exp(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;

	if (se->token == SQL_COALESCE) {
		symbol *opt_else = l->t->data.sym;

		return rel_case(query, rel, se->token, NULL, l, opt_else, f);
	} else if (se->token == SQL_NULLIF) {
		return rel_case(query, rel, se->token, NULL, l, NULL, f);
	} else if (l->h->type == type_list) {
		dlist *when_search_list = l->h->data.lval;
		symbol *opt_else = l->h->next->data.sym;

		return rel_case(query, rel, SQL_CASE, NULL, when_search_list, opt_else, f);
	} else {
		symbol *scalar_exp = l->h->data.sym;
		dlist *when_value_list = l->h->next->data.lval;
		symbol *opt_else = l->h->next->next->data.sym;

		return rel_case(query, rel, SQL_CASE, scalar_exp, when_value_list, opt_else, f);
	}
}

static sql_exp *
rel_cast(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	mvc *sql = query->sql;
	dlist *dl = se->data.lval;
	symbol *s = dl->h->data.sym;
	sql_subtype *tpe = &dl->h->next->data.typeval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *e = rel_value_exp(query, rel, s, f, ek);

	if (!e)
		return NULL;
	/* strings may need too be truncated */
	if (tpe ->type ->localtype == TYPE_str) {
		if (tpe->digits > 0) {
			sql_subtype *et = exp_subtype(e);
			sql_subtype *it = sql_bind_localtype("int");
			sql_subfunc *c = sql_bind_func(sql->sa, sql->session->schema, "truncate", et, it, F_FUNC);
			if (c)
				e = exp_binop(sql->sa, e, exp_atom_int(sql->sa, tpe->digits), c);
		}
	}
	if (e) 
		e = rel_check_type(sql, tpe, rel ? *rel : NULL, e, type_cast);
	if (e)
		exp_label(sql->sa, e, ++sql->label);
	return e;
}

static sql_exp *
rel_next_value_for( mvc *sql, symbol *se )
{
	char *seq = qname_table(se->data.lval);
	char *sname = qname_schema(se->data.lval);
	sql_schema *s = NULL;
	sql_subtype t;
	sql_subfunc *f;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02,
			SQLSTATE(3F000) "NEXT VALUE FOR: no such schema '%s'", sname);
	if (!s)
		s = sql->session->schema;

	if (!find_sql_sequence(s, seq) && !stack_find_rel_view(sql, seq))
		return sql_error(sql, 02, SQLSTATE(42000) "NEXT VALUE FOR: "
			"no such sequence '%s'.'%s'", s->base.name, seq);
	sql_find_subtype(&t, "varchar", 0, 0);
	f = sql_bind_func(sql->sa, s, "next_value_for", &t, &t, F_FUNC);
	assert(f);
	return exp_binop(sql->sa, exp_atom_str(sql->sa, s->base.name, &t),
			exp_atom_str(sql->sa, seq, &t), f);
}

/* some users like to use aliases already in the groupby */
static sql_exp *
rel_selection_ref(sql_query *query, sql_rel **rel, symbol *grp, dlist *selection )
{
	sql_allocator *sa = query->sql->sa;
	dnode *n;
	dlist *gl = grp->data.lval;
	char *name = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	if (dlist_length(gl) > 1)
		return NULL;
	if (!selection)
		return NULL;

	name = gl->h->data.sval;
	for (n = selection->h; n; n = n->next) {
		/* we only look for columns */
		tokens to = n->data.sym->token;
		if (to == SQL_COLUMN || to == SQL_IDENT) {
			dlist *l = n->data.sym->data.lval;
			/* AS name */
			if (l->h->next->data.sval &&
					strcmp(l->h->next->data.sval, name) == 0){
				sql_exp *ve = rel_value_exp(query, rel, l->h->data.sym, sql_sel, ek);
				if (ve) {
					dlist *l = dlist_create(sa);
					symbol *sym;
					exp_setname(sa, ve, NULL, name);
					/* now we should rewrite the selection
					   such that it uses the new group
					   by column
					*/
					dlist_append_string(sa, l,
						sa_strdup(sa, name));
					sym = symbol_create_list(sa, to, l);
					l = dlist_create(sa);
					dlist_append_symbol(sa, l, sym);
					/* no alias */
					dlist_append_symbol(sa, l, NULL);
					n->data.sym = symbol_create_list(sa, to, l);
				
				}
				return ve;
			}
		}
	}
	return NULL;
}

static list *
rel_group_by(sql_query *query, sql_rel **rel, symbol *groupby, dlist *selection, int f )
{
	mvc *sql = query->sql;
	dnode *o = groupby->data.lval->h;
	list *exps = new_exp_list(sql->sa);

	for (; o; o = o->next) {
		symbol *grp = o->data.sym;
		int is_last = 1;
		exp_kind ek = {type_value, card_value, TRUE};
		sql_exp *e = rel_value_exp2(query, rel, grp, f, ek, &is_last);

		if (!e) {
			char buf[ERRSIZE];
			/* reset error */
			sql->session->status = 0;
			strcpy(buf, sql->errstr);
			sql->errstr[0] = '\0';

			e = rel_selection_ref(query, rel, grp, selection);
			if (!e) {
				if (sql->errstr[0] == 0)
					strcpy(sql->errstr, buf);
				return NULL;
			}
		}
		if(e->type != e_column) { //store group by expressions in the stack
			if(!stack_push_groupby_expression(sql, grp, e))
				return NULL;
		}
		append(exps, e);
	}
	return exps;
}

/* find selection expressions matching the order by column expression */

/* first limit to simple columns only */
static sql_exp *
rel_order_by_simple_column_exp(mvc *sql, sql_rel *r, symbol *column_r)
{
	sql_exp *e = NULL;
	dlist *l = column_r->data.lval;

	if (!r || !is_project(r->op) || column_r->type == type_int)
		return NULL;
	assert(column_r->token == SQL_COLUMN && column_r->type == type_list);

	r = r->l;
	if (!r)
		return e;
	if (dlist_length(l) == 1) {
		char *name = l->h->data.sval;
		e = rel_bind_column(sql, r, name, sql_sel | sql_orderby);
	}
	if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *name = l->h->next->data.sval;

		e = rel_bind_column2(sql, r, tname, name, sql_sel | sql_orderby);
	}
	if (e) 
		return e;
	return sql_error(sql, 02, SQLSTATE(42000) "ORDER BY: absolute column names not supported");
}

static list *
rel_projections_(mvc *sql, sql_rel *rel)
{
	list *rexps, *exps ;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "query too complex: running out of stack space");

	if (is_subquery(rel) && is_project(rel->op))
		return new_exp_list(sql->sa);

	switch(rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		exps = rel_projections_(sql, rel->l);
		rexps = rel_projections_(sql, rel->r);
		exps = list_merge( exps, rexps, (fdup)NULL);
		return exps;
	case op_groupby:
	case op_project:
	case op_table:
	case op_basetable:
	case op_ddl:

	case op_union:
	case op_except:
	case op_inter:

		exps = new_exp_list(sql->sa);
		if (rel->exps) {
			node *en;

			for (en = rel->exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				if (e) {
					if (e->type == e_column) {
						sql_exp *oe = e;
						e = exp_ref(sql->sa, e);
						exp_setname(sql->sa, e, oe->l, oe->r);
					}
					append(exps, e);
				}
			}
		}
		if (is_groupby(rel->op) && rel->r) {
			list *l = rel->r;
			node *en;

			for (en = l->h; en; en = en->next) {
				sql_exp *e = en->data;
				if (e) {
					if (e->type == e_column) {
						sql_exp *oe = e;
						e = exp_ref(sql->sa, e);
						exp_setname(sql->sa, e, oe->l, oe->r);
					}
					append(exps, e);
				}
			}
		}
		return exps;
	case op_semi:
	case op_anti:

	case op_select:
	case op_topn:
	case op_sample:
		return rel_projections_(sql, rel->l);
	default:
		return NULL;
	}
}

/* exp_rewrite */
static sql_exp * exp_rewrite(mvc *sql, sql_exp *e, sql_rel *t);

static list *
exps_rename(mvc *sql, list *l, sql_rel *r) 
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	for(n=l->h; n; n=n->next) {
		sql_exp *arg = n->data;

		arg = exp_rewrite(sql, arg, r);
		if (!arg) 
			return NULL;
		append(nl, arg);
	}
	return nl;
}

static sql_exp *
exp_rewrite(mvc *sql, sql_exp *e, sql_rel *r) 
{
	sql_exp *l, *ne = NULL;

	switch(e->type) {
	case e_column:
		if (e->l) { 
			e = exps_bind_column2(r->exps, e->l, e->r);
		} else {
			e = exps_bind_column(r->exps, e->r, NULL);
		}
		if (!e)
			return NULL;
		return exp_propagate(sql->sa, exp_column(sql->sa, e->l, e->r, exp_subtype(e), exp_card(e), has_nil(e), is_intern(e)), e);
	case e_aggr:
	case e_cmp: 
		return NULL;
	case e_convert:
		l = exp_rewrite(sql, e->l, r);
		if (l)
			ne = exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		break;
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_rename(sql, l, r);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			ne = exp_op(sql->sa, nl, e->f);
		else 
			ne = exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
		break;
	}	
	case e_atom:
	case e_psm:
		return e;
	}
	return ne;
}

/* second complex columns only */
static sql_exp *
rel_order_by_column_exp(sql_query *query, sql_rel **R, symbol *column_r, int f)
{
	mvc *sql = query->sql;
	sql_rel *r = *R;
	sql_exp *e = NULL;
	exp_kind ek = {type_value, card_column, FALSE};
	int added_project = 0;

	if (is_sql_orderby(f)) {
		assert(is_project(r->op));
		r = r->l;
	}
	if (!r)
		return e;

	if (!is_project(r->op) || is_set(r->op)) {
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
		(*R)->l = r;
		added_project = 1;
	}

	if (!e) {
		e = rel_value_exp(query, &r, column_r, sql_sel | sql_orderby, ek);
		/* add to internal project */
		if (e && is_processed(r) && !is_groupby(r->op)) {
			e = rel_project_add_exp(sql, r, e);
			e = exp_ref(sql->sa, e);
			if (added_project || is_sql_orderby(f)) {
				e = rel_project_add_exp(sql, *R, e);
				e = exp_ref(sql->sa, e);
			}
		}
		/* try with reverted aliases */
		if (!e && r && sql->session->status != -ERR_AMBIGUOUS) {
			list *proj = rel_projections_(sql, r);
			sql_rel *nr;

			if (!proj)
				return NULL;
			nr = rel_project(sql->sa, r, proj);
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';

			e = rel_value_exp(query, &nr, column_r, sql_sel | sql_orderby, ek);
			if (e) {
				/* first rewrite e back into current column names */
				e = exp_rewrite(sql, e, nr);
				
				if (!is_groupby(r->op)) {
					e = rel_project_add_exp(sql, r, e);
					e = exp_ref(sql->sa, e);
					if (added_project || is_sql_orderby(f)) {
						e = rel_project_add_exp(sql, *R, e);
						e = exp_ref(sql->sa, e);
					}
				}
			}
		}
	}
	if (e)
		return e;
	return NULL;
}

static dlist *
simple_selection(symbol *sq)
{
	if (sq->token == SQL_SELECT) {
		SelectNode *sn;

 		sn = (SelectNode *) sq;

		if (!sn->from && !sn->where && !sn->distinct && !sn->window && dlist_length(sn->selection) == 1)
			return sn->selection;
	}
	return NULL;
}

static list *
rel_order_by(sql_query *query, sql_rel **R, symbol *orderby, int f )
{
	mvc *sql = query->sql;
	sql_rel *rel = *R;
	sql_rel *or = rel;
	list *exps = new_exp_list(sql->sa);
	dnode *o = orderby->data.lval->h;
	dlist *selection = NULL;

	if (is_sql_orderby(f)) {
		assert(is_project(rel->op));
		rel = rel->l;
	}
	
	for (; o; o = o->next) {
		symbol *order = o->data.sym;

		if (order->token == SQL_COLUMN || order->token == SQL_IDENT) {
			symbol *col = order->data.lval->h->data.sym;
			int direction = order->data.lval->h->next->data.i_val;
			sql_exp *e = NULL;

			assert(order->data.lval->h->next->type == type_int);
			if ((selection = simple_selection(col)) != NULL) {
				dnode *o = selection->h;
				order = o->data.sym;
				col = order->data.lval->h->data.sym;
				/* remove optional name from selection */
				order->data.lval->h->next = NULL;
			}
			if (col->token == SQL_COLUMN || col->token == SQL_IDENT || col->token == SQL_ATOM) {
				int is_last = 0;
				exp_kind ek = {type_value, card_column, FALSE};

				e = rel_value_exp2(query, &rel, col, f, ek, &is_last);

				if (e && e->card <= CARD_ATOM) {
					sql_subtype *tpe = &e->tpe;
					/* integer atom on the stack */
					if (e->type == e_atom &&
					    tpe->type->eclass == EC_NUM) {
						atom *a = e->l?e->l:sql->args[e->flag];
						int nr = (int)atom_get_int(a);

						e = exps_get_exp(rel->exps, nr);
						if (!e)
							return NULL;
						e = exp_ref(sql->sa, e);
						/* do not cache this query */
						if (e)
							scanner_reset_key(&sql->scanner);
					} else if (e->type == e_atom) {
						return sql_error(sql, 02, SQLSTATE(42000) "order not of type SQL_COLUMN");
					}
				} else if (e && exp_card(e) > rel->card) {
					if (e && exp_name(e)) {
						return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(e));
					} else {
						return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
					}
				}
			}

			if (!e && sql->session->status != -ERR_AMBIGUOUS && (col->token == SQL_COLUMN || col->token == SQL_IDENT)) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				e = rel_order_by_simple_column_exp(sql, rel, col);
				if (e && e->card > rel->card) 
					e = NULL;
				if (e)
					e = rel_project_add_exp(sql, rel, e);
			}
			if (rel && !e && sql->session->status != -ERR_AMBIGUOUS) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				/* check for project->select->groupby */
				if (rel && is_project(rel->op) && is_sql_orderby(f)) {
					sql_rel *s = rel->l;
					sql_rel *p = rel;
					sql_rel *g = s;

					if (is_select(s->op) && !is_processed(s)) /* having ? */
						g = s->l;
					if (is_groupby(g->op)) { /* check for is processed */
						e = rel_order_by_column_exp(query, &g, col, sql_sel);
						if (e && e->card > rel->card && e->card != CARD_ATOM)
							e = NULL;
						if (e && !is_select(p->op)) {
							e = rel_project_add_exp(sql, p, e);
							e = exp_ref(sql->sa, e);
						}
						if (!e && sql->session->status != -ERR_AMBIGUOUS) {
							/* reset error */
							sql->session->status = 0;
							sql->errstr[0] = '\0';
						}
					}
				}
				if (!e)
					e = rel_order_by_column_exp(query, &rel, col, f);
				if (!e)
					e = rel_order_by_column_exp(query, &rel, col, sql_sel);
				if (e && e->card > rel->card && e->card != CARD_ATOM)
					e = NULL;
			}
			if (!e) 
				return NULL;
			set_direction(e, direction);
			append(exps, e);
		} else {
			return sql_error(sql, 02, SQLSTATE(42000) "order not of type SQL_COLUMN");
		}
	}
	if (or != rel)
		or->l = rel;
	return exps;
}

static int
generate_window_bound(tokens sql_token, bool first_half)
{
	switch(sql_token) {
		case SQL_PRECEDING:
			return first_half ? BOUND_FIRST_HALF_PRECEDING : BOUND_SECOND_HALF_PRECEDING;
		case SQL_FOLLOWING:
			return first_half ? BOUND_FIRST_HALF_FOLLOWING : BOUND_SECOND_HALF_FOLLOWING;
		case SQL_CURRENT_ROW:
			return first_half ? CURRENT_ROW_PRECEDING : CURRENT_ROW_FOLLOWING;
		default:
			assert(0);
	}
	return 0;
}

/* window functions */
static sql_exp*
generate_window_bound_call(mvc *sql, sql_exp **estart, sql_exp **eend, sql_schema *s, sql_exp *pe, sql_exp *e,
						   sql_exp *start, sql_exp *fend, int frame_type, int excl, tokens t1, tokens t2)
{
	list *rargs1 = sa_list(sql->sa), *rargs2 = sa_list(sql->sa), *targs1 = sa_list(sql->sa), *targs2 = sa_list(sql->sa);
	sql_subfunc *dc1, *dc2;
	sql_subtype *it = sql_bind_localtype("int");

	if(pe) {
		append(targs1, exp_subtype(pe));
		append(targs2, exp_subtype(pe));
		append(rargs1, exp_copy(sql->sa, pe));
		append(rargs2, exp_copy(sql->sa, pe));
	}
	append(rargs1, exp_copy(sql->sa, e));
	append(rargs2, exp_copy(sql->sa, e));
	append(targs1, exp_subtype(e));
	append(targs2, exp_subtype(e));
	append(targs1, it);
	append(targs2, it);
	append(targs1, it);
	append(targs2, it);
	append(targs1, it);
	append(targs2, it);
	append(targs1, exp_subtype(start));
	append(targs2, exp_subtype(fend));

	dc1 = sql_bind_func_(sql->sa, s, "window_bound", targs1, F_ANALYTIC);
	dc2 = sql_bind_func_(sql->sa, s, "window_bound", targs2, F_ANALYTIC);
	if (!dc1 || !dc2)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function 'window_bound' not found");
	append(rargs1, exp_atom_int(sql->sa, frame_type));
	append(rargs2, exp_atom_int(sql->sa, frame_type));
	append(rargs1, exp_atom_int(sql->sa, generate_window_bound(t1, true)));
	append(rargs2, exp_atom_int(sql->sa, generate_window_bound(t2, false)));
	append(rargs1, exp_atom_int(sql->sa, excl));
	append(rargs2, exp_atom_int(sql->sa, excl));
	append(rargs1, start);
	append(rargs2, fend);

	*estart = exp_op(sql->sa, rargs1, dc1);
	*eend = exp_op(sql->sa, rargs2, dc2);
	return e; //return something to say there were no errors
}

static sql_exp*
calculate_window_bound(sql_query *query, sql_rel *p, tokens token, symbol *bound, sql_exp *ie, int frame_type, int f)
{
	mvc *sql = query->sql;
	sql_subtype *bt, *it = sql_bind_localtype("int"), *lon = sql_bind_localtype("lng"), *iet;
	sql_class bclass = EC_ANY;
	sql_exp *res = NULL;

	if((bound->token == SQL_PRECEDING || bound->token == SQL_FOLLOWING || bound->token == SQL_CURRENT_ROW) && bound->type == type_int) {
		atom *a = NULL;
		bt = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? lon : exp_subtype(ie);
		bclass = bt->type->eclass;

		if((bound->data.i_val == UNBOUNDED_PRECEDING_BOUND || bound->data.i_val == UNBOUNDED_FOLLOWING_BOUND)) {
			if(EC_NUMBER(bclass))
				a = atom_general(sql->sa, bt, NULL);
			else
				a = atom_general(sql->sa, it, NULL);
		} else if(bound->data.i_val == CURRENT_ROW_BOUND) {
			if(EC_NUMBER(bclass))
				a = atom_zero_value(sql->sa, bt);
			else
				a = atom_zero_value(sql->sa, it);
		} else {
			assert(0);
		}
		res = exp_atom(sql->sa, a);
	} else { //arbitrary expression case
		int is_last = 0;
		exp_kind ek = {type_value, card_column, FALSE};
		const char* bound_desc = (token == SQL_PRECEDING) ? "PRECEDING" : "FOLLOWING";
		iet = exp_subtype(ie);

		assert(token == SQL_PRECEDING || token == SQL_FOLLOWING);
		if (bound->token == SQL_NULL ||
		    (bound->token == SQL_IDENT &&
		     bound->data.lval->h->type == type_int &&
		     sql->args[bound->data.lval->h->data.i_val]->isnull)) {
			return sql_error(sql, 02, SQLSTATE(42000) "%s offset must not be NULL", bound_desc);
		}
		res = rel_value_exp2(query, &p, bound, f, ek, &is_last);
		if(!res)
			return NULL;
		bt = exp_subtype(res);
		if(bt)
			bclass = bt->type->eclass;
		if(!bt || !(bclass == EC_NUM || EC_INTERVAL(bclass) || bclass == EC_DEC || bclass == EC_FLT))
			return sql_error(sql, 02, SQLSTATE(42000) "%s offset must be of a countable SQL type", bound_desc);
		if((frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) && bclass != EC_NUM) {
			char *err = subtype2string(bt);
			if(!err)
				return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
			(void) sql_error(sql, 02, SQLSTATE(42000) "Values on %s boundary on %s frame can't be %s type", bound_desc,
							 (frame_type == FRAME_ROWS) ? "rows":"groups", err);
			_DELETE(err);
			return NULL;
		}
		if(frame_type == FRAME_RANGE) {
			if(bclass == EC_FLT && iet->type->eclass != EC_FLT)
				return sql_error(sql, 02, SQLSTATE(42000) "Values in input aren't floating-point while on %s boundary are", bound_desc);
			if(bclass != EC_FLT && iet->type->eclass == EC_FLT)
				return sql_error(sql, 02, SQLSTATE(42000) "Values on %s boundary aren't floating-point while on input are", bound_desc);
			if(bclass == EC_DEC && iet->type->eclass != EC_DEC)
				return sql_error(sql, 02, SQLSTATE(42000) "Values in input aren't decimals while on %s boundary are", bound_desc);
			if(bclass != EC_DEC && iet->type->eclass == EC_DEC)
				return sql_error(sql, 02, SQLSTATE(42000) "Values on %s boundary aren't decimals while on input are", bound_desc);
			if(bclass != EC_SEC && iet->type->eclass == EC_TIME) {
				char *err = subtype2string(iet);
				if(!err)
					return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
				(void) sql_error(sql, 02, SQLSTATE(42000) "For %s input the %s boundary must be an interval type up to the day", err, bound_desc);
				_DELETE(err);
				return NULL;
			}
			if(EC_INTERVAL(bclass) && !EC_TEMP(iet->type->eclass)) {
				char *err = subtype2string(iet);
				if(!err)
					return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
				(void) sql_error(sql, 02, SQLSTATE(42000) "For %s input the %s boundary must be an interval type", err, bound_desc);
				_DELETE(err);
				return NULL;
			}
		}
	}
	return res;
}

static dlist*
get_window_clauses(mvc *sql, char* ident, symbol **partition_by_clause, symbol **order_by_clause, symbol **frame_clause)
{
	dlist *window_specification = NULL;
	char *window_ident;
	int pos;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "SELECT: too many nested window definitions");

	if((window_specification = stack_get_window_def(sql, ident, &pos)) == NULL)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window '%s' not found", ident);

	//avoid infinite lookups
	if(stack_check_var_visited(sql, pos))
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cyclic references to window '%s' found", ident);
	stack_set_var_visited(sql, pos);

	if(window_specification->h->next->data.sym) {
		if(*partition_by_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of PARTITION BY clause from window '%s'", ident);
		*partition_by_clause = window_specification->h->next->data.sym;
	}
	if(window_specification->h->next->next->data.sym) {
		if(*order_by_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of ORDER BY clause from window '%s'", ident);
		*order_by_clause = window_specification->h->next->next->data.sym;
	}
	if(window_specification->h->next->next->next->data.sym) {
		if(*frame_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of frame clause from window '%s'", ident);
		*frame_clause = window_specification->h->next->next->next->data.sym;
	}

	window_ident = window_specification->h->data.sval;
	if(window_ident && !get_window_clauses(sql, window_ident, partition_by_clause, order_by_clause, frame_clause))
		return NULL; //the error was already set

	return window_specification; //return something to say there were no errors
}

static sql_exp*
opt_groupby_add_exp(mvc *sql, sql_rel *p, sql_rel *pp, sql_exp *in)
{
	sql_exp *found;

	if (p->op == op_groupby) {
		if (!exp_name(in))
			exp_label(sql->sa, in, ++sql->label);
		found = exps_find_exp( p->exps, in);
		if (!found)
			append(p->exps, in);
		else
			in = found;
		in = exp_ref(sql->sa, in);
	} else if (pp && pp->op == op_groupby) {
		if (!exp_name(in))
			exp_label(sql->sa, in, ++sql->label);
		found = exps_find_exp( p->exps, in);
		if (!found) {
			sql_rel *l = p->l;

			while (l && l != pp && !is_base(l->op)) {
				if (!exps_find_exp(l->exps, in)) {
					if (is_project(l->op))
						append(l->exps, exp_copy(sql->sa, in));
				} else
					break;
				l = l->l;
			}
			append(p->exps, in);
		} else
			in = found;
		in = exp_ref(sql->sa, in);
	}
	return in;
}

/*
 * select x, y, rank_op() over (partition by x order by y) as, ...
                aggr_op(z) over (partition by y order by x) as, ...
 * from table [x,y,z,w,v]
 *
 * project and order by over x,y / y,x
 * a = project( table ) [ x, y, z, w, v ], [ x, y]
 * b = project( table ) [ x, y, z, w, v ], [ y, x]
 *
 * project with order dependent operators, ie combined prev/current value
 * aa = project (a) [ x, y, r = rank_op(diff(x) (marks a new partition), rediff(diff(x), y) (marks diff value with in partition)), z, w, v ]
 * project(aa) [ aa.x, aa.y, aa.r ] -- only keep current output list 
 * bb = project (b) [ x, y, a = aggr_op(z, diff(y), rediff(diff(y), x)), z, w, v ]
 * project(bb) [ bb.x, bb.y, bb.a ]  -- only keep current output list
 */
static sql_exp *
rel_rankop(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	mvc *sql = query->sql;
	node *n;
	dlist *l = se->data.lval, *window_specification = NULL;
	symbol *window_function = l->h->data.sym, *partition_by_clause = NULL, *order_by_clause = NULL, *frame_clause = NULL;
	char *aname = NULL, *sname = NULL, *window_ident = NULL;
	sql_subfunc *wf = NULL;
	sql_exp *in = NULL, *pe = NULL, *oe = NULL, *call = NULL, *start = NULL, *eend = NULL, *fstart = NULL, *fend = NULL;
	sql_rel *r = *rel, *p, *pp, *g = NULL;
	list *gbe = NULL, *obe = NULL, *args = NULL, *types = NULL, *fargs = NULL;
	sql_schema *s = sql->session->schema;
	dnode *dn = window_function->data.lval->h;
	int distinct = 0, project_added = 0, is_last, frame_type, pos, group = 0, nf = f;
	bool is_nth_value, supports_frames;

	stack_clear_frame_visited_flag(sql); //clear visited flags before iterating

	if(l->h->next->type == type_list) {
		window_specification = l->h->next->data.lval;
	} else if (l->h->next->type == type_string) {
		const char* window_alias = l->h->next->data.sval;
		if((window_specification = stack_get_window_def(sql, window_alias, &pos)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window '%s' not found", window_alias);
		stack_set_var_visited(sql, pos);
	} else {
		assert(0);
	}

	window_ident = window_specification->h->data.sval;
	partition_by_clause = window_specification->h->next->data.sym;
	order_by_clause = window_specification->h->next->next->data.sym;
	frame_clause = window_specification->h->next->next->next->data.sym;

	if(window_ident && !get_window_clauses(sql, window_ident, &partition_by_clause, &order_by_clause, &frame_clause))
		return NULL;

	frame_type = order_by_clause ? FRAME_RANGE : FRAME_ROWS;
	aname = qname_fname(dn->data.lval);
	sname = qname_schema(dn->data.lval);

	if (sname)
		s = mvc_bind_schema(sql, sname);

	is_nth_value = (strcmp(s->base.name, "sys") == 0 && strcmp(aname, "nth_value") == 0);
	supports_frames = (window_function->token != SQL_RANK) || is_nth_value ||
					  (strcmp(s->base.name, "sys") == 0 && ((strcmp(aname, "first_value") == 0) || strcmp(aname, "last_value") == 0));

	if (is_sql_where(f) || is_sql_groupby(f) || is_sql_having(f) || is_sql_orderby(f) || is_sql_partitionby(f)) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		const char *clause = is_sql_where(f)?"WHERE":is_sql_groupby(f)?"GROUP BY":is_sql_having(f)?"HAVING":is_sql_orderby(f)?"ORDER BY":"PARTITION BY";
		(void) sql_error(sql, 02, SQLSTATE(42000) "%s: window function '%s' not allowed in %s clause",
						 uaname ? toUpperCopy(uaname, aname) : aname, aname, clause);
		if (uaname)
			GDKfree(uaname);
		return NULL;
	}

	/* 
	 * We need to keep track of the input relation, pp (projection)
	 * which may in the first step (which could be in the partitioning, ordering or window operator) change into a group by.
	 * then we project the partitioning/ordering + require result columns (p).
	 * followed by the projection with window operators. 
	 */
	/* window operations are only allowed in the projection */
	if (r && r->op != op_project) {
		*rel = r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
		reset_processed(r);
		project_added = 1;
	}
	if (!is_sql_sel(f) || !r || r->op != op_project || is_processed(r))
		return sql_error(sql, 02, SQLSTATE(42000) "OVER: only possible within the selection");

	/* outer project (*rel) r  r->l will be rewritten!
	 * 	in case of project_added (ie outer project added) we add the expression to r 
	 * rank project (new or existing non processed project) pp (needed for rank)
	 * inner project/groupby p (if groupby group is set)
	 * op used to reset p
	 */
	p = r->l;
	if(!p || (!is_joinop(p->op) && !p->exps->h)) { //no from clause, use a constant as the expression to project
		sql_exp *exp = exp_atom_lng(sql->sa, 0);
		exp_label(sql->sa, exp, ++sql->label);
		if (!p) {
			p = rel_project(sql->sa, NULL, sa_list(sql->sa));
			set_processed(p);
		}
		append(p->exps, exp);
	}

	if (p && p->op != op_project) {
		p = rel_project(sql->sa, p, rel_projections(sql, p, NULL, 1, 1));
		reset_processed(p);
	}
	pp = p;

	g = p;
	while(g && !group) {
		if (g && g->op == op_groupby) {
			group = 1;
		} else if (g->l && !is_processed(g) && !is_base(g->op)) {
			g = g->l;
		} else {
			g = NULL;
		}
	}
	/* Partition By */
	if (partition_by_clause) {
		gbe = rel_group_by(query, &pp, partition_by_clause, NULL /* cannot use (selection) column references, as this result is a selection column */, nf);
		if (!gbe && !group) { /* try with implicit groupby */
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			p = pp = rel_project(sql->sa, p, sa_list(sql->sa));
			reset_processed(p);
			gbe = rel_group_by(query, &p, partition_by_clause, NULL /* cannot use (selection) column references, as this result is a selection column */, f);
		}
		if (!gbe)
			return NULL;

		if (p->op == op_groupby) {
			sql_rel *npp = pp;

			pp = g = p;
			p = rel_project(sql->sa, npp, rel_projections(sql, npp, NULL, 1, 0));
			reset_processed(p);
		}

		for(n = gbe->h ; n ; n = n->next) {
			sql_exp *en = n->data;

			n->data = en = opt_groupby_add_exp(sql, p, group?g:pp, en);
			set_direction(en, 1);
		}
		p->r = gbe;
	}
	/* Order By */
	if (order_by_clause) {
		obe = rel_order_by(query, &pp, order_by_clause, nf);
		if (!obe && !group && !gbe) { /* try with implicit groupby */
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			p = pp = rel_project(sql->sa, p, sa_list(sql->sa));
			reset_processed(p);
			obe = rel_order_by(query, &p, order_by_clause, f);
		}
		if (!obe)
			return NULL;

		if (p->op == op_groupby) {
			sql_rel *npp = pp;

			pp = g = p;
			p = rel_project(sql->sa, npp, rel_projections(sql, npp, NULL, 1, 0));
			reset_processed(p);
		}

		for(n = obe->h ; n ; n = n->next) {
			sql_exp *oexp = n->data, *nexp;

			if (is_sql_sel(f) && pp->op == op_project && !is_processed(pp) && !rel_find_exp(pp, oexp)) {
				append(pp->exps, oexp);
				if (!exp_name(oexp))
					exp_label(sql->sa, oexp, ++sql->label);
				oexp = exp_ref(sql->sa, oexp);
			}
			n->data = nexp = opt_groupby_add_exp(sql, p, group?g:pp, oexp);
			if (is_ascending(oexp))
				set_direction(nexp, 1);
			if (nulls_last(oexp))
				set_direction(nexp, 2);
		}
		if (p->r) {
			p->r = list_merge(sa_list(sql->sa), p->r, (fdup)NULL); /* make sure the p->r is a different list than the gbe list */
			for(n = obe->h ; n ; n = n->next) {
				sql_exp *e1 = n->data;
				bool found = false;

				for(node *nn = ((list*)p->r)->h ; nn && !found ; nn = nn->next) {
					sql_exp *e2 = nn->data;
					//the partition expression order should be the same as the one in the order by clause (if it's in there as well)
					if(!exp_equal(e1, e2)) {
						if(is_ascending(e1))
							e2->flag |= ASCENDING;
						else
							e2->flag &= ~ASCENDING;
						found = true;
					}
				}
				if(!found)
					append(p->r, e1);
			}
		} else {
			p->r = obe;
		}
	}

	fargs = sa_list(sql->sa);
	if (window_function->token == SQL_RANK) { //rank function call
		dlist* dnn = window_function->data.lval->h->next->data.lval;
		bool is_ntile = (strcmp(s->base.name, "sys") == 0 && strcmp(aname, "ntile") == 0),
			 is_lag = (strcmp(s->base.name, "sys") == 0 && strcmp(aname, "lag") == 0),
			 is_lead = (strcmp(s->base.name, "sys") == 0 && strcmp(aname, "lead") == 0);
		int nfargs = 0;

		if(!dnn || is_ntile) { //pass an input column for analytic functions that don't require it
			sql_rel *lr = p;

			if (!lr || !is_project(lr->op)) {
				p = pp = rel_project(sql->sa, p, rel_projections(sql, p, NULL, 1, 0));
				reset_processed(p);
				lr = p->l;
			}
			in = lr->exps->h->data;
			in = exp_ref(sql->sa, in);
			if(!in)
				return NULL;
			append(fargs, in);
			nfargs++;
		}
		if(dnn) {
			for(dnode *nn = dnn->h ; nn ; nn = nn->next) {
				is_last = 0;
				exp_kind ek = {type_value, card_column, FALSE};
				in = rel_value_exp2(query, &p, nn->data.sym, f, ek, &is_last);
				if(!in)
					return NULL;
				if(is_ntile && nfargs == 1) { //ntile first argument null handling case
					sql_subtype *empty = sql_bind_localtype("void");
					if(subtype_cmp(&(in->tpe), empty) == 0) {
						sql_subtype *to = sql_bind_localtype("bte");
						in = exp_convert(sql->sa, in, empty, to);
					}
				} else if(is_nth_value && nfargs == 1) { //nth_value second argument null handling case
					sql_subtype *empty = sql_bind_localtype("void");
					if(subtype_cmp(&(in->tpe), empty) == 0) {
						sql_rel *lr = p->l;
						sql_exp *ep = lr->exps->h->data;
						in = exp_convert(sql->sa, in, empty, &(ep->tpe));
					}
				} else if((is_lag || is_lead) && nfargs == 2) { //lag and lead 3rd arg must have same type as 1st arg
					sql_exp *first = (sql_exp*) fargs->h->data;
					if(!(in = rel_check_type(sql, &first->tpe, p, in, type_equal)))
						return NULL;
				}
				in = opt_groupby_add_exp(sql, p, pp, in);
				append(fargs, in);
				nfargs++;
			}
		}
	} else { //aggregation function call
		dnode *n = dn->next;

		if (n) {
			if (!n->next->data.sym) { /* count(*) */
				sql_rel *lr = p->l;

				if (!lr || !is_project(lr->op)) {
					p = pp = rel_project(sql->sa, p, rel_projections(sql, p, NULL, 1, 0));
					reset_processed(p);
					lr = p->l;
				}
				in = lr->exps->h->data;
				in = exp_ref(sql->sa, in);
				append(fargs, in);
				append(fargs, exp_atom_bool(sql->sa, 0)); //don't ignore nills
			} else {
				sql_rel *lop = p;
				is_last = 0;
				exp_kind ek = {type_value, card_column, FALSE};

				distinct = n->data.i_val;
				/*
				 * all aggregations implemented in a window have 1 and only 1 argument only, so for now no further
				 * symbol compilation is required
				 */
				in = rel_value_exp2(query, &p, n->next->data.sym, f, ek, &is_last);
				if (!in && !group && !obe && !gbe) { /* try with implicit groupby */
					/* reset error */
					sql->session->status = 0;
					sql->errstr[0] = '\0';
					p = rel_project(sql->sa, lop, sa_list(sql->sa));
					reset_processed(p);
					in = rel_value_exp2(query, &p, n->next->data.sym, f, ek, &is_last);
				}
				if(!in)
					return NULL;
				in = opt_groupby_add_exp(sql, p, pp, in);
				if (group) 
					//rel_intermediates_add_exp(sql, p, g, in);
					in = opt_groupby_add_exp(sql, p, g, in);
				append(fargs, in);
				if(strcmp(s->base.name, "sys") == 0 && strcmp(aname, "count") == 0) {
					sql_subtype *empty = sql_bind_localtype("void"), *bte = sql_bind_localtype("bte");
					sql_exp* eo = fargs->h->data;
					//corner case, if the argument is null convert it into something countable such as bte
					if(subtype_cmp(&(eo->tpe), empty) == 0)
						fargs->h->data = exp_convert(sql->sa, eo, empty, bte);
					append(fargs, exp_atom_bool(sql->sa, 1)); //ignore nills
				}
			}
		}
	}

	if (distinct)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: DISTINCT clause is not implemented for window functions");

	if (p->exps && list_length(p->exps)) {
		p = rel_project(sql->sa, p, sa_list(sql->sa));
		reset_processed(p);
	}

	/* diff for partitions */
	if (gbe) {
		sql_subtype *bt = sql_bind_localtype("bit");

		for( n = gbe->h; n; n = n->next)  {
			sql_subfunc *df;
			sql_exp *e = n->data;

			args = sa_list(sql->sa);
			if (pe) { 
				df = bind_func(sql, s, "diff", bt, exp_subtype(e), F_ANALYTIC);
				append(args, pe);
			} else {
				df = bind_func(sql, s, "diff", exp_subtype(e), NULL, F_ANALYTIC);
			}
			if (!df)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function '%s' not found", "diff" );
			append(args, e);
			pe = exp_op(sql->sa, args, df);
		}
	} else {
		pe = exp_atom_bool(sql->sa, 0);
	}
	/* diff for orderby */
	if (obe) {
		sql_subtype *bt = sql_bind_localtype("bit");

		for( n = obe->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *df;

			args = sa_list(sql->sa);
			if (oe) { 
				df = bind_func(sql, s, "diff", bt, exp_subtype(e), F_ANALYTIC);
				append(args, oe);
			} else {
				df = bind_func(sql, s, "diff", exp_subtype(e), NULL, F_ANALYTIC);
			}
			if (!df)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function '%s' not found", "diff" );
			append(args, e);
			oe = exp_op(sql->sa, args, df);
		}
	} else {
		oe = exp_atom_bool(sql->sa, 0);
	}

	/* Frame */
	if(frame_clause) {
		dnode *d = frame_clause->data.lval->h;
		symbol *wstart = d->data.sym, *wend = d->next->data.sym, *rstart = wstart->data.lval->h->data.sym,
			   *rend = wend->data.lval->h->data.sym;
		int excl = d->next->next->next->data.i_val;
		frame_type = d->next->next->data.i_val;
		sql_exp *ie = obe ? obe->t->data : in;

		if(!supports_frames)
			return sql_error(sql, 02, SQLSTATE(42000) "OVER: frame extend only possible with aggregation and first_value, last_value and nth_value functions");
		if(!obe && frame_type == FRAME_GROUPS)
			return sql_error(sql, 02, SQLSTATE(42000) "GROUPS frame requires an order by expression");
		if(wstart->token == SQL_FOLLOWING && wend->token == SQL_PRECEDING)
			return sql_error(sql, 02, SQLSTATE(42000) "FOLLOWING offset must come after PRECEDING offset");
		if(wstart->token == SQL_CURRENT_ROW && wend->token == SQL_PRECEDING)
			return sql_error(sql, 02, SQLSTATE(42000) "CURRENT ROW offset must come after PRECEDING offset");
		if(wstart->token == SQL_FOLLOWING && wend->token == SQL_CURRENT_ROW)
			return sql_error(sql, 02, SQLSTATE(42000) "FOLLOWING offset must come after CURRENT ROW offset");
		if(wstart->token != SQL_CURRENT_ROW && wend->token != SQL_CURRENT_ROW && wstart->token == wend->token &&
		   (frame_type != FRAME_ROWS && frame_type != FRAME_ALL))
			return sql_error(sql, 02, SQLSTATE(42000) "Non-centered windows are only supported in row frames");
		if(!obe && frame_type == FRAME_RANGE) {
			bool ok_preceding = false, ok_following = false;
			if((wstart->token == SQL_PRECEDING || wstart->token == SQL_CURRENT_ROW) &&
			   (rstart->token == SQL_PRECEDING || rstart->token == SQL_CURRENT_ROW) && rstart->type == type_int &&
			   (rstart->data.i_val == UNBOUNDED_PRECEDING_BOUND || rstart->data.i_val == CURRENT_ROW_BOUND))
				ok_preceding = true;
			if((wend->token == SQL_FOLLOWING || wend->token == SQL_CURRENT_ROW) &&
			   (rend->token == SQL_FOLLOWING || rend->token == SQL_CURRENT_ROW) && rend->type == type_int &&
			   (rend->data.i_val == UNBOUNDED_FOLLOWING_BOUND || rend->data.i_val == CURRENT_ROW_BOUND))
				ok_following = true;
			if(!ok_preceding || !ok_following)
				return sql_error(sql, 02, SQLSTATE(42000) "RANGE frame with PRECEDING/FOLLOWING offset requires an order by expression");
			frame_type = FRAME_ALL; //special case, iterate the entire partition
		}

		if((fstart = calculate_window_bound(query, p, wstart->token, rstart, ie, frame_type, f)) == NULL)
			return NULL;
		if((fend = calculate_window_bound(query, p, wend->token, rend, ie, frame_type, f)) == NULL)
			return NULL;
		if(generate_window_bound_call(sql, &start, &eend, s, gbe ? pe : NULL, ie, fstart, fend, frame_type, excl,
									  wstart->token, wend->token) == NULL)
			return NULL;
	} else if (supports_frames) { //for analytic functions with no frame clause, we use the standard default values
		sql_exp *ie = obe ? obe->t->data : in;
		sql_subtype *it = sql_bind_localtype("int"), *lon = sql_bind_localtype("lng"), *bt;
		unsigned char sclass;

		bt = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? lon : exp_subtype(ie);
		sclass = bt->type->eclass;
		if(sclass == EC_POS || sclass == EC_NUM || sclass == EC_DEC || EC_INTERVAL(sclass)) {
			fstart = exp_null(sql->sa, bt);
			if(order_by_clause)
				fend = exp_atom(sql->sa, atom_zero_value(sql->sa, bt));
			else
				fend = exp_null(sql->sa, bt);
		} else {
			fstart = exp_null(sql->sa, it);
			if(order_by_clause)
				fend = exp_atom(sql->sa, atom_zero_value(sql->sa, it));
			else
				fend = exp_null(sql->sa, it);
		}
		if(!obe)
			frame_type = FRAME_ALL;

		if(generate_window_bound_call(sql, &start, &eend, s, gbe ? pe : NULL, ie, fstart, fend, frame_type, EXCLUDE_NONE,
									  SQL_PRECEDING, SQL_FOLLOWING) == NULL)
			return NULL;
	}

	if (!pe || !oe)
		return NULL;

	if(!supports_frames) {
		append(fargs, pe);
		append(fargs, oe);
	}

	types = exp_types(sql->sa, fargs);
	wf = bind_func_(sql, s, aname, types, F_ANALYTIC);
	if (!wf) {
		wf = sql_find_func_by_name(sql->sa, NULL, aname, list_length(types), F_ANALYTIC);
		if (wf) {
			node *op = wf->func->ops->h;
			list *nexps = sa_list(sql->sa);

			for (n = fargs->h ; wf && op && n; op = op->next, n = n->next ) {
				sql_arg *arg = op->data;
				sql_exp *e = n->data;

				e = rel_check_type(sql, &arg->type, NULL, e, type_equal);
				if (!e) {
					wf = NULL;
					break;
				}
				list_append(nexps, e);
			}
			if (wf && list_length(nexps))
				fargs = nexps;
			else
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function '%s' not found", aname );
		} else {
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function '%s' not found", aname );
		}
	}
	args = sa_list(sql->sa);
	for(node *nn = fargs->h ; nn ; nn = nn->next)
		append(args, (sql_exp*) nn->data);
	if (supports_frames) {
		append(args, start);
		append(args, eend);
	}
	call = exp_op(sql->sa, args, wf);
	exp_label(sql->sa, call, ++sql->label);
	r->l = p;
	p->exps = list_merge(p->exps, rel_projections(sql, p->l, NULL, 1, 1), NULL);
	append(p->exps, call);
	call = exp_ref(sql->sa, call);
	if (project_added) {
		append(r->exps, call);
		call = exp_ref(sql->sa, call);
	}
	return call;
}

sql_exp *
rel_value_exp2(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek, int *is_last)
{
	mvc *sql = query->sql;
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "SELECT: too many nested operators");

	if (rel && *rel && (*rel)->card == CARD_AGGR) { //group by expression case, handle it before
		sql_exp *exp = stack_get_groupby_expression(sql, se);
		if (sql->errstr[0] != '\0')
			return NULL;
		if (exp) {
			sql_exp *res = exp_ref(sql->sa, exp);
			if(se->token == SQL_AGGR) {
				dlist *l = se->data.lval;
				int distinct = l->h->next->data.i_val;
				if (distinct)
					set_distinct(res);
			}
			return res;
		}
	}

	switch (se->token) {
	case SQL_OP:
		return rel_op(sql, se, ek);
	case SQL_UNOP:
		return rel_unop(query, rel, se, f, ek);
	case SQL_BINOP:
		return rel_binop(query, rel, se, f, ek);
	case SQL_NOP:
		return rel_nop(query, rel, se, f, ek);
	case SQL_AGGR:
		return rel_aggr(query, rel, se, f);
	case SQL_RANK:
		return rel_rankop(query, rel, se, f);
	case SQL_IDENT:
	case SQL_COLUMN:
		return rel_column_ref(query, rel, se, f );
	case SQL_NAME:
		return rel_var_ref(sql, se->data.sval, 1);
	case SQL_WITH: 
	case SQL_SELECT: {
		sql_rel *r;

		if (se->token == SQL_WITH)
			r = rel_with_query(query, se);
		else {
			dlist *selection = NULL;

			if ((selection = simple_selection(se)) != NULL) {
				dnode *o = selection->h;
				symbol *sym = o->data.sym;
				symbol *col = sym->data.lval->h->data.sym;
				char *aname = NULL;
				sql_exp *e;

				/* optional name from selection */
				if (sym->data.lval->h->next) /* optional name */
					aname = sym->data.lval->h->next->data.sval;
				if (!(*rel))
					*rel = rel_project(sql->sa, NULL, append(sa_list(sql->sa), exp_atom_bool(sql->sa, 1)));
				e = rel_value_exp2(query, rel, col, f, ek, is_last);
				if (aname)
					exp_setname(sql->sa, e, NULL, aname);
				*is_last = 1;
				return e;
			}
			r = rel_subquery(query, NULL, se, ek);
		}

		if (r) {
			sql_exp *e;

			if (ek.card <= card_set && is_project(r->op) && list_length(r->exps) > 1) 
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column");
			e = _rel_lastexp(sql, r);

			/* group by needed ? */
			if (e->card >= CARD_ATOM && e->card > (unsigned) ek.card) {

				int processed = is_processed(r);

				sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e));

				if (ek.card > card_column) {
					list *args = new_exp_list(sql->sa);
					assert(0);
					list_append(args, e);
					list_append(args, exp_atom_bool(sql->sa, 0)); /* no error */
					e = exp_aggr(sql->sa, args, zero_or_one, 0, 0, CARD_ATOM, 0);
				} else
					e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, 0);
				r = rel_groupby(sql, r, NULL);
				e = rel_groupby_add_aggr(sql, r, e);
				if (processed)
					set_processed(r);
			}
			/* single row */
			if (!rel)
				return NULL;
			if (*rel) {
				sql_rel *p = *rel;


				/* in the selection phase we should have project/groupbys, unless 
				 * this is the value (column) for the aggregation then the 
				 * crossproduct is pushed under the project/groupby.  */ 
				if (is_sql_sel(f) && r->op == op_project && list_length(r->exps) == 1 && exps_are_atoms(r->exps) && !r->l) {
					sql_exp *ne = r->exps->h->data;

					exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
					e = ne;
				} else { 
					if (is_sql_sel(f) && is_project(p->op) && !is_processed(p)) {
						if (p->l) {
							p->l = rel_crossproduct(sql->sa, p->l, r, op_join);
						} else {
							p->l = r;
						}
					} else {
						*rel = rel_crossproduct(sql->sa, p, r, is_sql_sel(f)?op_left:op_join);
					}
				}
				*is_last = 1;
				return e;
			} else {
				if (exp_card(e) <= CARD_ATOM) {

					*rel = r;
					while(r && e && !is_atom(e->type) && is_column(e->type)) {
						sql_exp *ne = NULL;

						if (e->l && e->r)
							ne = rel_bind_column2(sql, r, e->l, e->r, 0);
						else if (!e->l && e->r && /* DISABLES CODE */ (0)) {
							ne = rel_bind_column(sql, r, e->r, 0);
						}
						if (ne) {
							e = ne;
							r = r->l;
						} else 
							break;
					}
					if (e && is_atom(e->type))
						*rel = NULL;
				} else {
					*rel = r;
				}
			}
			*is_last=1;
			return e;
		}
		if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			sql_exp *rs = NULL;
			sql_rel *outerp = NULL;

			if (*rel && is_sql_sel(f) && /*is_project((*rel)->op)*/ (*rel)->op == op_project && !is_processed((*rel))) {
				outerp = *rel;
				*rel = (*rel)->l;
			}
			if (!*rel)
				return NULL;

			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';

			query_push_outer(query, *rel);
			r = rel_subquery(query, NULL, se, ek);
			query_pop_outer(query);
			if (r) {
				rs = _rel_lastexp(sql, r);

				if (ek.card <= card_set && is_project(r->op) && list_length(r->exps) > 1) 
					return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column");
				if (is_sql_sel(f) && is_freevar(lastexp(r))) {
					sql_exp *re, *jc, *null;
				       
					re = rel_bound_exp(sql, r);
					re = rel_project_add_exp(sql, r, re);
					jc = rel_unop_(query, NULL, re, NULL, "isnull", card_value);
					null = exp_null(sql->sa, exp_subtype(rs));
					rs = rel_nop_(query, NULL, jc, null, rs, NULL, NULL, "ifthenelse", card_value);
				}
				if (is_sql_sel(f) && ek.card <= card_column && r->card > CARD_ATOM) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));
					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);

					r = rel_groupby(sql, r, NULL);
					rs = rel_groupby_add_aggr(sql, r, rs);
					rs = exp_ref(sql->sa, rs);
				}
				/* remove empty projects */
				if (!is_processed(*rel) && is_sql_sel(f) && (*rel)->op == op_project && list_length((*rel)->exps) == 0 && !(*rel)->r && (*rel)->l) 
					*rel = (*rel)->l;

				*rel = rel_crossproduct(sql->sa, *rel, r, is_sql_sel(f)?op_left:op_join); 
				set_dependent(*rel);
				if (outerp) {
					outerp->l = *rel;
					*rel = outerp;
				} 
				*is_last = 1;
			}
			return rs;
		}
		if (!r)
			return NULL;
		return rel_find_lastexp(*rel);
	}
	case SQL_TABLE: {
		/* turn a subquery into a tabular result */
		*rel = rel_selects(query, se->data.sym /*, *rel, se->data.sym, ek*/);
		if (*rel)
			return rel_find_lastexp(*rel);
		return NULL;
	}
	case SQL_PARAMETER:{
		if (sql->emode != m_prepare)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameters ('?') not allowed in normal queries, use PREPARE");
		assert(se->type == type_int);
		return exp_atom_ref(sql->sa, se->data.i_val, NULL);
	}
	case SQL_NULL:
		return exp_null(sql->sa, sql_bind_localtype("void"));
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;

		if (!an || !an->a) {
			return exp_null(sql->sa, sql_bind_localtype("void"));
		} else {
			return exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		}
	}
	case SQL_NEXT:
		return rel_next_value_for(sql, se);
	case SQL_CAST:
		return rel_cast(query, rel, se, f);
	case SQL_CASE:
	case SQL_COALESCE:
	case SQL_NULLIF:
		return rel_case_exp(query, rel, se, f);
	case SQL_DEFAULT:
		return sql_error(sql, 02, SQLSTATE(42000) "DEFAULT keyword not allowed outside insert and update statements");
	case SQL_XMLELEMENT:
	case SQL_XMLFOREST:
	case SQL_XMLCOMMENT:
	case SQL_XMLATTRIBUTE:
	case SQL_XMLCONCAT:
	case SQL_XMLDOCUMENT:
	case SQL_XMLPI:
	case SQL_XMLTEXT:
		return rel_xml(query, rel, se, f, ek);
	default:
		return rel_logical_value_exp(query, rel, se, f);
	}
}

sql_exp *
rel_value_exp(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	int is_last = 0;
	sql_exp *e;
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(query->sql, 10, SQLSTATE(42000) "SELECT: too many nested operators");

	e = rel_value_exp2(query, rel, se, f, ek, &is_last);
	if (e && (se->token == SQL_SELECT || se->token == SQL_TABLE) && !is_last) {
		assert(*rel);
		return rel_lastexp(query->sql, *rel);
	}
	return e;
}

static sql_exp *
column_exp(sql_query *query, sql_rel **rel, symbol *column_e, int f)
{
	dlist *l = column_e->data.lval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *ve;

	if (f == sql_sel && rel && *rel && (*rel)->card < CARD_AGGR)
		ek.card = card_value;
       	ve = rel_value_exp(query, rel, l->h->data.sym, f, ek);
	if (!ve)
		return NULL;
	/* AS name */
	if (ve && l->h->next->data.sval)
		exp_setname(query->sql->sa, ve, NULL, l->h->next->data.sval);
	return ve;
}

static list *
rel_table_exp(sql_query *query, sql_rel **rel, symbol *column_e )
{
	mvc *sql = query->sql;
	if (column_e->token == SQL_TABLE && column_e->data.lval->h->type == type_symbol) {
		sql_rel *r;

		if (!is_project((*rel)->op))
			return NULL;
		r = rel_named_table_function( query, (*rel)->l, column_e, 0);
	
		if (!r)
			return NULL;
		*rel = r;
		return sa_list(sql->sa); 
	} else if (column_e->token == SQL_TABLE) {
		char *tname = column_e->data.lval->h->data.sval;
		list *exps;
	
		if ((exps = rel_table_projections(sql, *rel, tname, 0)) != NULL)
			return exps;
		if (!tname)
			return sql_error(sql, 02,
				SQLSTATE(42000) "Table expression without table name");
		return sql_error(sql, 02,
				SQLSTATE(42000) "Column expression Table '%s' unknown", tname);
	}
	return NULL;
}

sql_exp *
rel_column_exp(sql_query *query, sql_rel **rel, symbol *column_e, int f)
{
	if (column_e->token == SQL_COLUMN || column_e->token == SQL_IDENT) {
		return column_exp(query, rel, column_e, f);
	}
	return NULL;
}

static sql_rel *
rel_simple_select(sql_query *query, sql_rel *rel, symbol *where, dlist *selection, int distinct)
{
	mvc *sql = query->sql;
	dnode *n = 0;
	sql_rel *inner;

	if (!selection)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: the selection or from part is missing");
	if (where) {
		sql_rel *r;

		if(!rel)
			rel = rel_project(sql->sa, NULL, list_append(new_exp_list(sql->sa), exp_atom_bool(sql->sa, 1)));
		r = rel_logical_exp(query, rel, where, sql_where);
		if (!r)
			return NULL;
		rel = r;
	}
	if (!rel || rel->op != op_project || !list_empty(rel->exps))
		rel = rel_project(sql->sa, rel, new_exp_list(sql->sa));
	inner = rel;
	for (n = selection->h; n; n = n->next ) {
		/* Here we could get real column expressions (including single
		 * atoms) but also table results. Therefore we try both
		 * rel_column_exp and rel_table_exp.
		 */
		sql_rel *o_inner = inner;
	       	list *te = NULL, *pre_prj = rel_projections(sql, o_inner, NULL, 1, 1);
		sql_exp *ce = rel_column_exp(query, &inner, n->data.sym, sql_sel);

		if (inner != o_inner) {  /* relation got rewritten */
			if (!inner)
				return NULL;
			rel = inner;
		}

		if (ce && exp_subtype(ce)) {
			/* we need a project */
			if (!is_simple_project(inner->op) /*&& is_processed(inner) */) {
				if (inner != o_inner && pre_prj) {
					inner = rel_project(sql->sa, inner, pre_prj);
					reset_processed(inner);
				} else
					inner = rel_project(sql->sa, inner, new_exp_list(sql->sa));
			}
			ce = rel_project_add_exp(sql, inner, ce);
			rel = inner;
			continue;
		} else if (!ce) {
			te = rel_table_exp(query, &rel, n->data.sym );
		} else 
			ce = NULL;
		if (!ce && !te)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery result missing");
		/* here we should merge the column expressions we obtained
		 * so far with the table expression, ie t1.* or a subquery
		 */
		list_merge( rel->exps, te, (fdup)NULL);
	}
	if (rel)
		set_processed(rel);

	if (rel && distinct)
		rel = rel_distinct(rel);

	return rel;
}

static sql_rel *
join_on_column_name(sql_query *query, sql_rel *rel, sql_rel *t1, sql_rel *t2, int op, int l_nil, int r_nil)
{
	mvc *sql = query->sql;
	int nr = ++sql->label, found = 0, full = (op != op_join);
	char name[16], *nme;
	list *exps = rel_projections(sql, t1, NULL, 1, 0);
	list *r_exps = rel_projections(sql, t2, NULL, 1, 0);
	list *outexps = new_exp_list(sql->sa);
	node *n;

	nme = number2name(name, 16, nr);
	if (!exps)
		return NULL;
	for (n = exps->h; n; n = n->next) {
		sql_exp *le = n->data;
		const char *nm = exp_name(le);
		sql_exp *re = exps_bind_column(r_exps, nm, NULL);

		if (re) {
			found = 1;
			rel = rel_compare_exp(query, rel, le, re, "=", NULL, TRUE, 0, 0);
			if (full) {
				sql_exp *cond = rel_unop_(query, rel, le, NULL, "isnull", card_value);
				le = rel_nop_(query, rel, cond, re, le, NULL, NULL, "ifthenelse", card_value);
			}
			exp_setname(sql->sa, le, nme, sa_strdup(sql->sa, nm));
			append(outexps, le);
			list_remove_data(r_exps, re);
		} else {
			if (l_nil)
				set_has_nil(le);
			append(outexps, le);
		}
	}
	if (!found) {
		sql_error(sql, 02, SQLSTATE(42000) "JOIN: no columns of tables '%s' and '%s' match", rel_name(t1)?rel_name(t1):"", rel_name(t2)?rel_name(t2):"");
		rel_destroy(rel);
		return NULL;
	}
	for (n = r_exps->h; n; n = n->next) {
		sql_exp *re = n->data;
		if (r_nil)
			set_has_nil(re);
		append(outexps, re);
	}
	rel = rel_project(sql->sa, rel, outexps);
	return rel;
}


static int
exp_is_not_intern(sql_exp *e)
{
	return is_intern(e)?-1:0;
}

static void
rel_remove_internal_exp(sql_rel *rel)
{
	if (rel->exps) {
		list *n_exps = list_select(rel->exps, rel, (fcmp)&exp_is_not_intern, (fdup)NULL);

		rel->exps = n_exps;
	}
}

static sql_rel *
rel_select_exp(sql_query *query, sql_rel *rel, SelectNode *sn, exp_kind ek)
{
	mvc *sql = query->sql;
	dnode *n;
	//int aggr = 0;
	sql_rel *inner = NULL;

	assert(sn->s.token == SQL_SELECT);
	if (!sn->selection)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: the selection or from part is missing");

	if (!sn->from)
		return rel_simple_select(query, rel, sn->where, sn->selection, sn->distinct);

	if (sn->where) {
		sql_rel *r = rel_logical_exp(query, rel, sn->where, sql_where);
		if (!r) {
			if (sql->errstr[0] == 0)
				return sql_error(sql, 02, SQLSTATE(42000) "Subquery result missing");
			return NULL;
		}
		rel = r;
		set_processed(rel);
	}

	if (rel) {
		if (rel && sn->groupby) {
			list *gbe = rel_group_by(query, &rel, sn->groupby, sn->selection, sql_sel | sql_groupby );

			if (!gbe)
				return NULL;
			rel = rel_groupby(sql, rel, gbe);
		}
	}

	if (sn->having) {
		/* having implies group by, ie if not supplied do a group by */
		if (rel->op != op_groupby)
			rel = rel_groupby(sql, rel, NULL);
	}

	n = sn->selection->h;
	rel = rel_project(sql->sa, rel, new_exp_list(sql->sa));
	inner = rel;
	for (; n; n = n->next) {
		/* Here we could get real column expressions
		 * (including single atoms) but also table results.
		 * Therefor we try both rel_column_exp
		 * and rel_table_exp.

		 * TODO
			the rel_table_exp should simply return a new
			relation
		 */
		sql_rel *o_inner = inner;
		list *te = NULL, *pre_prj = o_inner->exps;//*pre_prj = rel_projections(sql, o_inner, NULL, 1, 1);
		sql_rel *pre_rel = o_inner;
		sql_exp *ce = rel_column_exp(query, &inner, n->data.sym, sql_sel);

		if (inner != o_inner) {  /* relation got rewritten */
			if (!inner)
				return NULL;
			rel = inner;
		}

		if (ce && exp_subtype(ce)) {
			if (rel->card < ce->card) {
				if (exp_name(ce)) {
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ce));
				} else {
					return sql_error(sql, 05, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
			/*
			   because of the selection, the inner
			   relation may change.
			   We try hard to keep a projection
			   around this inner relation.
			*/
			if (!is_project(inner->op)) {
				if (inner != o_inner && pre_prj) {
					pre_prj = rel_projections(sql, pre_rel, NULL, 1, 1);
					inner = rel_project(sql->sa, inner, pre_prj);
					reset_processed(inner);
				} else
					inner = rel_project(sql->sa, inner, new_exp_list(sql->sa));
			}
			ce = rel_project_add_exp(sql, inner, ce);
			rel = inner;
			continue;
		} else if (!ce) {
			te = rel_table_exp(query, &rel, n->data.sym);
		} else 
			ce = NULL;
		if (!ce && !te) {
			if (sql->errstr[0])
				return NULL;
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery result missing");
		}
		/* here we should merge the column expressions we
		 * obtained so far with the table expression, ie
		 * t1.* or a subquery.
		 */
		list_merge( rel->exps, te, (fdup)NULL);
	}

	if (sn->having) {
		inner = rel->l;
		assert(is_project(rel->op) && inner);
	
		if (inner && inner->op == op_groupby)
			set_processed(inner);
		inner = rel_logical_exp(query, inner, sn->having, sql_having);

		if (!inner)
			return NULL;
		if (inner -> exps && exps_card(inner->exps) > CARD_AGGR)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cannot compare sets with values, probably an aggregate function missing");
		rel -> l = inner;
	}

	if (rel && sn->distinct)
		rel = rel_distinct(rel);

	if (rel && sn->orderby) {
		list *obe = NULL;

		set_processed(rel);
		rel = rel_orderby(sql, rel);
		obe = rel_order_by(query, &rel, sn->orderby, sql_orderby);
		if (!obe)
			return NULL;
		rel->r = obe;
	}
	if (!rel)
		return NULL;

	if (sn->limit || sn->offset) {
		sql_subtype *lng = sql_bind_localtype("lng");
		list *exps = new_exp_list(sql->sa);

		if (sn->limit) {
			sql_exp *l = rel_value_exp( query, NULL, sn->limit, 0, ek);

			if (!l || !(l=rel_check_type(sql, lng, NULL, l, type_equal)))
				return NULL;
			if ((ek.card != card_relation && sn->limit) &&
				(ek.card == card_value && sn->limit)) {
				sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(l));
				l = exp_aggr1(sql->sa, l, zero_or_one, 0, 0, CARD_ATOM, 0);
			}
			append(exps, l);
		} else
			append(exps, NULL);
		if (sn->offset) {
			sql_exp *o = rel_value_exp( query, NULL, sn->offset, 0, ek);
			if (!o || !(o=rel_check_type(sql, lng, NULL, o, type_equal)))
				return NULL;
			append(exps, o);
		}
		rel = rel_topn(sql->sa, rel, exps);
	}

	if (sn->sample) {
		list *exps = new_exp_list(sql->sa);
		
		dlist* sample_parameters = sn->sample->data.lval;

		sql_exp *sample_size = rel_value_exp( query, NULL, sample_parameters->h->data.sym, 0, ek);
		if (!sample_size)
			return NULL;
		append(exps, sample_size);

		if (sample_parameters->cnt == 2) {
			sql_exp *seed_value = rel_value_exp( query, NULL, sample_parameters->h->next->data.sym, 0, ek);
			if (!seed_value)
				return NULL;
			append(exps, seed_value);
		}

		rel = rel_sample(sql->sa, rel, exps);
	}
	set_processed(rel);
	return rel;
}

static sql_rel*
rel_unique_names(mvc *sql, sql_rel *rel)
{
	node *n;
	list *l;

	if (!is_project(rel->op))
		return rel;
       	l = sa_list(sql->sa);
	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (exp_name(e) && exps_bind_column2(l, exp_relname(e), exp_name(e))) 
			exp_label(sql->sa, e, ++sql->label);
		append(l,e);
	}
	rel->exps = l;
	return rel;
}

static sql_rel *
rel_query(sql_query *query, sql_rel *rel, symbol *sq, int toplevel, exp_kind ek)
{
	mvc *sql = query->sql;
	sql_rel *res = NULL;
	SelectNode *sn = NULL;

	//assert(!rel);
	if (sq->token != SQL_SELECT)
		return table_ref(query, rel, sq, 0);

	/* select ... into is currently not handled here ! */
 	sn = (SelectNode *) sq;
	if (sn->into)
		return NULL;

	if (ek.card != card_relation && sn->orderby)
		return sql_error(sql, 01, SQLSTATE(42000) "SELECT: ORDER BY only allowed on outermost SELECT");

	if (sn->window) {
		dlist *wl = sn->window->data.lval;
		for (dnode *n = wl->h; n ; n = n->next) {
			dlist *wd = n->data.sym->data.lval;
			const char *name = wd->h->data.sval;
			dlist *wdef = wd->h->next->data.lval;
			if(stack_get_window_def(sql, name, NULL)) {
				return sql_error(sql, 01, SQLSTATE(42000) "SELECT: Redefinition of window '%s'", name);
			} else if(!stack_push_window_def(sql, name, wdef)) {
				return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);
			}
		}
	}

	if (sn->from) {		/* keep variable list with tables and names */
		dlist *fl = sn->from->data.lval;
		dnode *n = NULL;
		sql_rel *fnd = NULL;

		for (n = fl->h; n ; n = n->next) {
			int lateral = check_is_lateral(n->data.sym);

			/* just used current expression */
			fnd = table_ref(query, NULL, n->data.sym, lateral);
			if (!fnd && res && lateral && sql->session->status != -ERR_AMBIGUOUS) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;

				query_push_outer(query, res);
				fnd = table_ref(query, NULL, n->data.sym, lateral);
				query_pop_outer(query);
			}
			if (!fnd)
				break;
			if (res) {
				res = rel_crossproduct(sql->sa, res, fnd, op_join);
				if (lateral)
					set_dependent(res);
			} else {
				res = fnd;
			}
		}
		if (!fnd) {
			if (res)
				rel_destroy(res);
			return NULL;
		}
	} else if (toplevel || !res) {	/* only on top level query */
		return rel_simple_select(query, rel, sn->where, sn->selection, sn->distinct);
	}
	if (res)
		rel = rel_select_exp(query, res, sn, ek);
	if (!rel && res) 
		rel_destroy(res);
	return rel;
}

static sql_rel *
rel_setquery_(sql_query *query, sql_rel *l, sql_rel *r, dlist *cols, int op )
{
	mvc *sql = query->sql;
	sql_rel *rel;

	if (!cols) {
		list *ls, *rs;

		l = rel_unique_names(sql, l);
		r = rel_unique_names(sql, r);
		ls = rel_projections(sql, l, NULL, 0, 1);
		rs = rel_projections(sql, r, NULL, 0, 1);
		rel = rel_setop_check_types(sql, l, r, ls, rs, (operator_type)op);
	} else {
		rel = rel_setop(sql->sa, l, r, (operator_type)op);
	}
	if (rel) {
		rel->exps = rel_projections(sql, rel, NULL, 0, 1);
		set_processed(rel);
	}
	return rel;
}

static sql_rel *
rel_setquery(sql_query *query, sql_rel *rel, symbol *q)
{
	mvc *sql = query->sql;
	sql_rel *res = NULL;
	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int dist = n->next->data.i_val, used = 0;
	dlist *corresponding = n->next->next->data.lval;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	sql_rel *t1, *t2; 

	assert(n->next->type == type_int);
	t1 = table_ref(query, NULL, tab_ref1, 0);
	if (rel && !t1 && sql->session->status != -ERR_AMBIGUOUS) {
		used = 1;

		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = 0;
		query_push_outer(query, rel);
		t1 = table_ref(query, NULL, tab_ref1, 0);
		query_pop_outer(query);
	}
	if (!t1)
		return NULL;
	t2 = table_ref(query, NULL, tab_ref2, 0);
	if (rel && !t2 && sql->session->status != -ERR_AMBIGUOUS) {
		used = 1;

		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = 0;
		query_push_outer(query, rel);
		t2 = table_ref(query, NULL, tab_ref2, 0);
		query_pop_outer(query);
	}
	if (!t2)
		return NULL;

	rel_remove_internal_exp(t1);
	rel_remove_internal_exp(t2);
	if (list_length(t1->exps) != list_length(t2->exps)) {
		int t1nrcols = list_length(t1->exps);
		int t2nrcols = list_length(t2->exps);
		char *op = "UNION";
		if (q->token == SQL_EXCEPT)
			op = "EXCEPT";
		else if (q->token == SQL_INTERSECT)
			op = "INTERSECT";
		rel_destroy(t1);
		rel_destroy(t2);
		return sql_error(sql, 02, SQLSTATE(42000) "%s: column counts (%d and %d) do not match", op, t1nrcols, t2nrcols);
	}
	if ( q->token == SQL_UNION) {
		/* For EXCEPT/INTERSECT the group by is always done within the implementation */
		if (t1 && dist)
			t1 = rel_distinct(t1);
		if (t2 && dist)
			t2 = rel_distinct(t2);
		res = rel_setquery_(query, t1, t2, corresponding, op_union );
	}
	if ( q->token == SQL_EXCEPT)
		res = rel_setquery_(query, t1, t2, corresponding, op_except );
	if ( q->token == SQL_INTERSECT)
		res = rel_setquery_(query, t1, t2, corresponding, op_inter );
	if (res && dist)
		res = rel_distinct(res);
	if (rel && used) {
		res = rel_crossproduct(sql->sa, rel, res, op_left);
		res->card = rel->card;
		set_dependent(res);
		res = rel_project(sql->sa, res, rel_projections(sql, res, NULL, 1, 1));
	}
	return res;
}

static sql_rel *
rel_joinquery_(sql_query *query, sql_rel *rel, symbol *tab1, int natural, jt jointype, symbol *tab2, symbol *js)
{
	mvc *sql = query->sql;
	operator_type op = op_join;
	sql_rel *t1 = NULL, *t2 = NULL, *inner;
	int l_nil = 0, r_nil = 0, lateral = 0;

	assert(!rel);
	switch(jointype) {
	case jt_inner: op = op_join;
		break;
	case jt_left: op = op_left;
		r_nil = 1;
		break;
	case jt_right: op = op_right;
		l_nil = 1;
		break;
	case jt_full: op = op_full;
		l_nil = 1;
		r_nil = 1;
		break;
	case jt_union:
		/* fool compiler */
		return NULL;
	}

	lateral = check_is_lateral(tab2);
	t1 = table_ref(query, NULL, tab1, 0);
	if (rel && !t1 && sql->session->status != -ERR_AMBIGUOUS) {
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = 0;
		t1 = table_ref(query, NULL, tab1, 0);
	}
	if (t1) {
		t2 = table_ref(query, NULL, tab2, 0);
		if (lateral && !t2 && sql->session->status != -ERR_AMBIGUOUS) {
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = 0;

			query_push_outer(query, t1);
			t2 = table_ref(query, NULL, tab2, 0);
			query_pop_outer(query);
		}
	}
	if (rel)
		rel_destroy(rel);
	if (!t1 || !t2)
		return NULL;

	if (!lateral && rel_name(t1) && rel_name(t2) && strcmp(rel_name(t1), rel_name(t2)) == 0) {
		sql_error(sql, 02, SQLSTATE(42000) "SELECT: '%s' on both sides of the JOIN expression;", rel_name(t1));
		rel_destroy(t1);
		rel_destroy(t2);
		return NULL;
	}

	inner = rel = rel_crossproduct(sql->sa, t1, t2, op_join);
	inner->op = op;
	if (lateral)
		set_dependent(inner);

	if (js && natural) {
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cannot have a NATURAL JOIN with a join specification (ON or USING);");
	}
	if (!js && !natural) {
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: must have NATURAL JOIN or a JOIN with a join specification (ON or USING);");
	}

	if (js && js->token != SQL_USING) {	/* On sql_logical_exp */
		rel = rel_logical_exp(query, rel, js, sql_where);
	} else if (js) {	/* using */
		char rname[16], *rnme;
		dnode *n = js->data.lval->h;
		list *outexps = new_exp_list(sql->sa), *exps;
		node *m;

		rnme = number2name(rname, 16, ++sql->label);
		for (; n; n = n->next) {
			char *nm = n->data.sval;
			sql_exp *cond;
			sql_exp *ls = rel_bind_column(sql, t1, nm, sql_where);
			sql_exp *rs = rel_bind_column(sql, t2, nm, sql_where);

			if (!ls || !rs) {
				sql_error(sql, 02, SQLSTATE(42000) "JOIN: tables '%s' and '%s' do not have a matching column '%s'\n", rel_name(t1)?rel_name(t1):"", rel_name(t2)?rel_name(t2):"", nm);
				rel_destroy(rel);
				return NULL;
			}
			rel = rel_compare_exp(query, rel, ls, rs, "=", NULL, TRUE, 0, 0);
			if (op != op_join) {
				cond = rel_unop_(query, rel, ls, NULL, "isnull", card_value);
				if (rel_convert_types(sql, t1, t2, &ls, &rs, 1, type_equal) < 0)
					return NULL;
				ls = rel_nop_(query, rel, cond, rs, ls, NULL, NULL, "ifthenelse", card_value);
			}
			exp_setname(sql->sa, ls, rnme, nm);
			append(outexps, ls);
			if (!rel) 
				return NULL;
		}
		exps = rel_projections(sql, t1, NULL, 1, 1);
		for (m = exps->h; m; m = m->next) {
			const char *nm = exp_name(m->data);
			int fnd = 0;

			for (n = js->data.lval->h; n; n = n->next) {
				if (strcmp(nm, n->data.sval) == 0) {
					fnd = 1;
					break;
				}
			}
			if (!fnd) {
				sql_exp *ls = m->data;
				if (l_nil)
					set_has_nil(ls);
				append(outexps, ls);
			}
		}
		exps = rel_projections(sql, t2, NULL, 1, 1);
		for (m = exps->h; m; m = m->next) {
			const char *nm = exp_name(m->data);
			int fnd = 0;

			for (n = js->data.lval->h; n; n = n->next) {
				if (strcmp(nm, n->data.sval) == 0) {
					fnd = 1;
					break;
				}
			}
			if (!fnd) {
				sql_exp *rs = m->data;
				if (r_nil)
					set_has_nil(rs);
				append(outexps, rs);
			}
		}
		rel = rel_project(sql->sa, rel, outexps);
	} else {		/* ! js -> natural join */
		rel = join_on_column_name(query, rel, t1, t2, op, l_nil, r_nil);
	}
	if (!rel)
		return NULL;
	if (inner && is_outerjoin(inner->op))
		set_processed(inner);
	set_processed(rel);
	return rel;
}

static sql_rel *
rel_joinquery(sql_query *query, sql_rel *rel, symbol *q)
{

	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int natural = n->next->data.i_val;
	jt jointype = (jt) n->next->next->data.i_val;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	symbol *joinspec = n->next->next->next->next->data.sym;

	assert(n->next->type == type_int);
	assert(n->next->next->type == type_int);
	return rel_joinquery_(query, rel, tab_ref1, natural, jointype, tab_ref2, joinspec);
}

static sql_rel *
rel_crossquery(sql_query *query, sql_rel *rel, symbol *q)
{
	dnode *n = q->data.lval->h;
	symbol *tab1 = n->data.sym;
	symbol *tab2 = n->next->data.sym;
	sql_rel *t1 = table_ref(query, rel, tab1, 0);
	sql_rel *t2 = NULL;
       
	if (t1)
		t2 = table_ref(query, rel, tab2, 0);
	if (!t1 || !t2)
		return NULL;

	rel = rel_crossproduct(query->sql->sa, t1, t2, op_join);
	return rel;
}
	
static sql_rel *
rel_unionjoinquery(sql_query *query, sql_rel *rel, symbol *q)
{
	mvc *sql = query->sql;
	dnode *n = q->data.lval->h;
	sql_rel *lv = table_ref(query, rel, n->data.sym, 0);
	sql_rel *rv = NULL;
	int all = n->next->data.i_val;
	list *lexps, *rexps;
	node *m;
	int found = 0;

	if (lv)
       		rv = table_ref(query, rel, n->next->next->data.sym, 0);
	assert(n->next->type == type_int);
	if (!lv || !rv)
		return NULL;

	lexps = rel_projections(sql, lv, NULL, 1, 1);
	/* find the matching columns (all should match?)
	 * union these
	 * if !all do a distinct operation at the end
	 */
	/* join all result columns ie join(lh,rh) on column_name */
	rexps = new_exp_list(sql->sa);
	for (m = lexps->h; m; m = m->next) {
		sql_exp *le = m->data;
		sql_exp *rc = rel_bind_column(sql, rv, exp_name(le), sql_where);
			
		if (!rc && all)
			break;
		if (rc) {
			found = 1;
			append(rexps, rc);
		}
	}
	if (!found) {
		rel_destroy(rel);
		return NULL;
	}
	lv = rel_project(sql->sa, lv, lexps);
	rv = rel_project(sql->sa, rv, rexps);
	rel = rel_setop(sql->sa, lv, rv, op_union);
	rel->exps = rel_projections(sql, rel, NULL, 0, 1);
	set_processed(rel);
	if (!all)
		rel = rel_distinct(rel);
	return rel;
}

sql_rel *
rel_subquery(sql_query *query, sql_rel *rel, symbol *sq, exp_kind ek)
{
	int toplevel = 0;

	if (!rel || (rel->op == op_project &&
		(!rel->exps || list_length(rel->exps) == 0)))
		toplevel = 1;

	return rel_query(query, rel, sq, toplevel, ek);
}

sql_rel *
rel_selects(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_WITH:
		ret = rel_with_query(query, s);
		sql->type = Q_TABLE;
		break;
	case SQL_VALUES:
		ret = rel_values(query, s);
		sql->type = Q_TABLE;
		break;
	case SQL_SELECT: {
		exp_kind ek = {type_value, card_relation, TRUE};
		SelectNode *sn = (SelectNode *) s;

		if(!stack_push_frame(sql, "SELECT"))
			return sql_error(sql, 02, SQLSTATE(HY001) MAL_MALLOC_FAIL);

		if (sn->into) {
			sql->type = Q_SCHEMA;
			ret = rel_select_with_into(query, s);
		} else {
			ret = rel_subquery(query, NULL, s, ek);
			sql->type = Q_TABLE;
		}
		stack_pop_frame(sql);
	}	break;
	case SQL_JOIN:
		ret = rel_joinquery(query, NULL, s);
		sql->type = Q_TABLE;
		break;
	case SQL_CROSS:
		ret = rel_crossquery(query, NULL, s);
		sql->type = Q_TABLE;
		break;
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		ret = rel_setquery(query, NULL, s);
		sql->type = Q_TABLE;
		break;
	default:
		return NULL;
	}
	if (!ret && sql->errstr[0] == 0)
		(void) sql_error(sql, 02, SQLSTATE(42000) "relational query without result");
	return ret;
}

sql_rel *
schema_selects(sql_query *query, sql_schema *schema, symbol *s)
{
	sql_rel *res;
	sql_schema *os = query->sql->session->schema;

	query->sql->session->schema = schema;
	res = rel_selects(query, s);
	query->sql->session->schema = os;
	return res;
}

sql_rel *
rel_loader_function(sql_query *query, symbol* fcall, list *fexps, sql_subfunc **loader_function)
{
	mvc *sql = query->sql;
	list *exps = NULL, *tl;
	exp_kind ek = { type_value, card_relation, TRUE };
	sql_rel *sq = NULL;
	sql_exp *e = NULL;
	symbol *sym = fcall;
	dnode *l = sym->data.lval->h;
	char *sname = qname_schema(l->data.lval);
	char *fname = qname_fname(l->data.lval);
	char *tname = NULL;
	node *en;
	sql_schema *s = sql->session->schema;
	sql_subfunc* sf;

	tl = sa_list(sql->sa);
	exps = new_exp_list(sql->sa);
	if (l->next) { /* table call with subquery */
		if (l->next->type == type_symbol && l->next->data.sym->token == SQL_SELECT) {
			if (l->next->next != NULL)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: '%s' requires a single sub query", fname);
			sq = rel_subquery(query, NULL, l->next->data.sym, ek);
		} else if (l->next->type == type_symbol || l->next->type == type_list) {
			dnode *n;
			exp_kind iek = {type_value, card_column, TRUE};
			list *exps = sa_list (sql->sa);

			if (l->next->type == type_symbol)
				n = l->next;
			else 
				n = l->next->data.lval->h;
			for ( ; n; n = n->next) {
				sql_exp *e = rel_value_exp(query, NULL, n->data.sym, sql_sel, iek);

				if (!e)
					return NULL;
				append(exps, e);
			}
			sq = rel_project(sql->sa, NULL, exps);
		}

		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		if (!sq)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
		for (en = sq->exps->h; en; en = en->next) {
			sql_exp *e = en->data;

			append(exps, e = exp_alias_or_copy(sql, tname, exp_name(e), NULL, e));
			append(tl, exp_subtype(e));
		}
	}
	if (sname)
		s = mvc_bind_schema(sql, sname);

	e = find_table_function_type(sql, s, fname, exps, tl, F_LOADER, &sf);
	if (!e || !sf)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);

	if (loader_function)
		*loader_function = sf;

	return rel_table_func(sql->sa, sq, e, fexps, (sq != NULL));
}
