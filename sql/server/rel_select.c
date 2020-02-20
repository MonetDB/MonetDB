/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
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
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return NULL;

	if (!tname)
		return _rel_projections(sql, rel, NULL, 1, 0, 1);

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
			int rename = 0;
			node *en;

			/* first check alias */
			if (!is_base(rel->op) && !level) {
				list *exps = sa_list(sql->sa);

				for (en = rel->exps->h; en && !rename; en = en->next) {
					sql_exp *e = en->data;;

					if ((is_basecol(e) && exp_relname(e) && strcmp(exp_relname(e), tname) == 0) ||
					    (is_basecol(e) && !exp_relname(e) && e->l && strcmp(e->l, tname) == 0)) {
						if (exp_name(e) && exps_bind_column2(exps, tname, exp_name(e)))
							rename = 1;
						else
							append(exps, e);
					}
				}
			}

			exps = new_exp_list(sql->sa);
			for (en = rel->exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				if (is_basecol(e) && exp_relname(e) && strcmp(exp_relname(e), tname) == 0) {
					if (rename)
						append(exps, exp_alias_ref(sql, e));
					else 
						append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
				}
				if (is_basecol(e) && !exp_relname(e) && e->l && strcmp(e->l, tname) == 0) {
					if (rename)
						append(exps, exp_alias_ref(sql, e));
					else
						append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
				}

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
lastexp(sql_rel *rel) 
{
	if (!is_processed(rel) || is_topn(rel->op) || is_sample(rel->op))
		rel = rel_parent(rel);
	assert(list_length(rel->exps));
	assert(is_project(rel->op));
	return rel->exps->t->data;
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
	if (!exp_name(e)) 
		exp_label(sql->sa, e, ++sql->label);
	return exp_ref(sql->sa, e);
}

static sql_rel *
rel_orderby(mvc *sql, sql_rel *l)
{
	sql_rel *rel = rel_create(sql->sa);
	if (!rel)
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
static sql_rel * rel_setquery(sql_query *query, symbol *sq);
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
		if (is_topn(sq->op) || (is_project(sq->op) && sq->r) || is_base(sq->op)) {
			sq = rel_project(sql->sa, sq, rel_projections(sql, sq, NULL, 1, 0));
			osq = sq;
		}
		if (columnrefs && dlist_length(columnrefs) > list_length(sq->exps))
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

				/*
				if (exp_name(e) && exps_bind_column2(l, tname, exp_name(e)))
					return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "SELECT: Duplicate column name '%s.%s'", tname, exp_name(e));
					*/
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

	if (!stack_push_frame(sql, "WITH"))
		return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		if (!stack_push_rel_view(sql, name, nrel)) {
			stack_pop_frame(sql);
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		sql_rel *tq = rel_setquery(query, q);

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
bind_func_(mvc *sql, sql_schema *s, char *fname, list *ops, sql_ftype type)
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && 
	    list_cmp(sql->forward->ops, ops, (fcmp)&arg_subtype_cmp) == 0 &&
	    execute_priv(sql, sql->forward) && type == sql->forward->type) 
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
	sf = sql_bind_func_(sql->sa, s, fname, ops, type);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
bind_func(mvc *sql, sql_schema *s, char *fname, sql_subtype *t1, sql_subtype *t2, sql_ftype type)
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
		     	subtype_cmp(sql->forward->ops->h->next->data, t2) == 0)) && type == sql->forward->type) {
			return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
		}
	}
	sf = sql_bind_func(sql->sa, s, fname, t1, t2, type);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
bind_member_func(mvc *sql, sql_schema *s, char *fname, sql_subtype *t, int nrargs, sql_ftype type, sql_subfunc *prev)
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && list_length(sql->forward->ops) == nrargs &&
		is_subtype(t, &((sql_arg *) sql->forward->ops->h->data)->type) && execute_priv(sql, sql->forward) && type == sql->forward->type) 
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, t);
	sf = sql_bind_member(sql->sa, s, fname, t, type, nrargs, prev);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_subfunc *
find_func(mvc *sql, sql_schema *s, char *fname, int len, sql_ftype type, sql_subfunc *prev )
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && list_length(sql->forward->ops) == len && execute_priv(sql, sql->forward) && type == sql->forward->type) 
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

		if (!t)
			continue;

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

static int
rel_set_type_param(mvc *sql, sql_subtype *type, sql_rel *rel, sql_exp *rel_exp, int upcast)
{
	sql_rel *r = rel;
	int is_rel = exp_is_rel(rel_exp);

	if (!type || !rel_exp || (rel_exp->type != e_atom && rel_exp->type != e_column && !is_rel))
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

	if (is_rel)
		r = (sql_rel*) rel_exp->l;

	if ((rel_exp->type == e_atom && (rel_exp->l || rel_exp->r || rel_exp->f)) || rel_exp->type == e_column || is_rel) {
		/* it's not a parameter set possible parameters below */
		const char *relname = exp_relname(rel_exp), *expname = exp_name(rel_exp);
		if (rel_set_type_recurse(sql, type, r, &relname, &expname) < 0)
			return -1;
	} else if (set_type_param(sql, type, rel_exp->flag) != 0)
		return -1;

	rel_exp->tpe = *type;
	return 0;
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
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
				sql_subtype *t = exp_subtype(e);

				if (!aa && a->type.type->eclass == EC_ANY) {
					atp = t;
					aa = a;
				}
				if (aa && a->type.type->eclass == EC_ANY && t && atp &&
				    t->type->localtype > atp->type->localtype){
					atp = t;
					aa = a;
				}
			}
			for (n = exps->h, m = (*sf)->func->ops->h; n && m; n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;
				sql_subtype *ntp = &a->type;

				if (a->type.type->eclass == EC_ANY && atp)
					ntp = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
				e = rel_check_type(sql, ntp, NULL, e, type_equal);
				if (!e) {
					nexps = NULL;
					break;
				}
				if (e->card > CARD_ATOM) {
					sql_subfunc *zero_or_one = sql_bind_func(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e), NULL, F_AGGR);
					e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, has_nil(e));
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
	if (l->next)
		l = l->next; /* skip distinct */
	if (l->next) { /* table call with subquery */
		if (l->next->type == type_symbol && l->next->data.sym->token == SQL_SELECT) {
			if (l->next->next != NULL)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: '%s' requires a single sub query", fname);
			if (!(sq = rel_subquery(query, NULL, l->next->data.sym, ek)))
				return NULL;
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

	if (sq) {
		for (node *n = sq->exps->h, *m = sf->func->ops->h ; n && m ; n = n->next, m = m->next) {
			sql_exp *e = (sql_exp*) n->data;
			sql_arg *a = (sql_arg*) m->data;
			if (!exp_subtype(e) && rel_set_type_param(sql, &(a->type), sq, e, 0) < 0)
				return NULL;
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
	rel = rel_table_func(sql->sa, rel, e, exps, (sq)?TABLE_FROM_RELATION:TABLE_PROD_FUNC);
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

static sql_exp*
exp_values_set_supertype(mvc *sql, sql_exp *values)
{
	list *vals = values->f, *nexps;
	sql_subtype *tpe = exp_subtype(vals->h->data);

	if (tpe)
		values->tpe = *tpe;

	for (node *m = vals->h; m; m = m->next) {
		sql_exp *e = m->data;
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
			values->tpe = super;
			tpe = &values->tpe;
		} else {
			tpe = ttpe;
		}
	}

	if (tpe) {
		/* if the expression is a parameter set its type */
		for (node *m = vals->h; m; m = m->next) {
			sql_exp *e = m->data;
			if (e->type == e_atom && !e->l && !e->r && !e->f && !e->tpe.type) {
				if (set_type_param(sql, tpe, e->flag) == 0)
					e->tpe = *tpe;
				else
					return NULL;
			}
		}
		values->tpe = *tpe;
		nexps = sa_list(sql->sa);
		for (node *m = vals->h; m; m = m->next) {
			sql_exp *e = m->data;
			e = rel_check_type(sql, &values->tpe, NULL, e, type_equal);
			if (!e)
				return NULL;
			append(nexps, e); 
		}
		values->f = nexps;
	}
	return values;
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
	for (m = exps->h; m; m = m->next)
		m->data = exp_values_set_supertype(sql, (sql_exp*) m->data);

	r = rel_project(sql->sa, NULL, exps);
	r->nrcols = list_length(exps);
	r->card = dlist_length(rowlist) == 1 ? CARD_ATOM : CARD_MULTI;
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

static sql_rel *
rel_reduce_on_column_privileges(mvc *sql, sql_rel *rel, sql_table *t)
{
	list *exps = sa_list(sql->sa);

	for (node *n = rel->exps->h, *m = t->columns.set->h; n && m; n = n->next, m = m->next) {
		sql_exp *e = n->data;
		sql_column *c = m->data;

		if (column_privs(sql, c, PRIV_SELECT)) {
			append(exps, e);
		}
	}
	if (!list_empty(exps)) {
		rel->exps = exps;
		return rel;
	}
	return NULL;
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
		int allowed = 1;

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
			allowed = 0;
		}
		if (tableref->data.lval->h->next->data.sym) {	/* AS */
			tname = tableref->data.lval->h->next->data.sym->data.lval->h->data.sval;
		}
		if (temp_table && !t) {
			node *n;
			int needed = !is_simple_project(temp_table->op);

			for (n = temp_table->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!exp_relname(e) || strcmp(exp_relname(e), tname) != 0)
					needed = 1;
			}

			if (needed) {
				list *exps = rel_projections(sql, temp_table, NULL, 1, 1);

				temp_table = rel_project(sql->sa, temp_table, exps);
				for (n = exps->h; n; n = n->next) {
					sql_exp *e = n->data;

					noninternexp_setname(sql->sa, e, tname, NULL);
					set_basecol(e);
				}
			}
			if (allowed)
				return temp_table;
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: access denied for %s to table '%s.%s'", stack_get_string(sql, "current_user"), s->base.name, tname);
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
				assert(is_project(rel->op));
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
			if (!allowed) 
				rel = rel_reduce_on_column_privileges(sql, rel, t);
			if (allowed && rel)
				return rel;
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: access denied for %s to table '%s.%s'", stack_get_string(sql, "current_user"), s->base.name, tname);
		}
		if ((isMergeTable(t) || isReplicaTable(t)) && list_empty(t->members.set))
			return sql_error(sql, 02, SQLSTATE(42000) "MERGE or REPLICA TABLE should have at least one table associated");
		res = rel_basetable(sql, t, tname);
		if (!allowed) {
			res = rel_reduce_on_column_privileges(sql, res, t);
			if (!res)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: access denied for %s to table '%s.%s'", stack_get_string(sql, "current_user"), s->base.name, tname);
		}
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

static int
is_groupby_col(sql_rel *gb, sql_exp *e)
{
	gb = rel_find_groupby(gb);

	if (gb) {
		if (exp_relname(e)) { 
			if (exp_name(e) && exps_bind_column2(gb->r, exp_relname(e), exp_name(e))) 
				return 1;
		} else {
			if (exp_name(e) && exps_bind_column(gb->r, exp_name(e), NULL, 1)) 
				return 1;
		}
	}
	return 0;
}

static sql_exp *
rel_column_ref(sql_query *query, sql_rel **rel, symbol *column_r, int f)
{
	mvc *sql = query->sql;
	sql_exp *exp = NULL;
	dlist *l = NULL;
	sql_rel *inner = rel?*rel:NULL;

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

		if (!exp && inner)
			exp = rel_bind_column(sql, inner, name, f, 0);
		if (!exp && inner && is_sql_having(f) && inner->op == op_select)
			inner = inner->l;
		if (!exp && inner && (is_sql_having(f) || is_sql_aggr(f)) && is_groupby(inner->op)) {
			exp = rel_bind_column(sql, inner->l, name, f, 0);
		}
		if (!exp && query && query_has_outer(query)) {
			int i;
			sql_rel *outer;

			for (i=query_has_outer(query)-1; i>= 0 && !exp && (outer = query_fetch_outer(query,i)); i--) {
				exp = rel_bind_column(sql, outer, name, f, 0);
				if (!exp && (is_sql_having(f) || is_sql_aggr(f)) && is_groupby(outer->op)) {
					exp = rel_bind_column(sql, outer->l, name, f, 0);
				}
				if (exp && is_simple_project(outer->op) && !rel_find_exp(outer, exp)) {
					exp = rel_project_add_exp(sql, outer, exp);
				}
				if (exp)
					break;
			}
			if (exp && outer && outer->card <= CARD_AGGR && exp->card > CARD_AGGR && (!is_sql_aggr(f) || is_sql_farg(f)))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
			if (exp) { 
				if (is_groupby(outer->op) && !is_sql_aggr(f)) {
					exp = rel_groupby_add_aggr(sql, outer, exp);
					exp->card = CARD_ATOM;
				} else if (is_groupby(outer->op) && is_sql_aggr(f) && exps_find_match_exp(outer->exps, exp))
					exp = exp_ref(sql->sa, exp);
				else
					exp->card = CARD_ATOM;
				set_freevar(exp, i);
			}
			if (exp && outer && is_join(outer->op))
				set_dependent(outer);
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
		if (!exp && !var)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: identifier '%s' unknown", name);
		if (exp && inner && inner->card <= CARD_AGGR && exp->card > CARD_AGGR && (is_sql_sel(f) || is_sql_having(f)) && !is_sql_aggr(f))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
		if (exp && inner && is_groupby(inner->op) && !is_sql_aggr(f) && !is_freevar(exp))
			exp = rel_groupby_add_aggr(sql, inner, exp);
	} else if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;

		if (!exp && rel && inner)
			exp = rel_bind_column2(sql, inner, tname, cname, f);
		if (!exp && inner && is_sql_having(f) && inner->op == op_select)
			inner = inner->l;
		if (!exp && inner && (is_sql_having(f) || is_sql_aggr(f)) && is_groupby(inner->op)) {
			exp = rel_bind_column2(sql, inner->l, tname, cname, f);
		}
		if (!exp && query && query_has_outer(query)) {
			int i;
			sql_rel *outer;

			for (i=query_has_outer(query)-1; i>= 0 && !exp && (outer = query_fetch_outer(query,i)); i--) {
				exp = rel_bind_column2(sql, outer, tname, cname, f | sql_outer);
				if (!exp && (is_sql_having(f) || is_sql_aggr(f)) && is_groupby(outer->op)) {
					exp = rel_bind_column2(sql, outer->l, tname, cname, f);
				}
				if (exp && is_simple_project(outer->op) && !rel_find_exp(outer, exp)) {
					exp = rel_project_add_exp(sql, outer, exp);
				}
				if (exp)
					break;
			}
			if (exp && outer && outer->card <= CARD_AGGR && exp->card > CARD_AGGR && (!is_sql_aggr(f) || is_sql_farg(f)))
				return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
			if (exp) {
				if (is_groupby(outer->op) && !is_sql_aggr(f)) {
					exp = rel_groupby_add_aggr(sql, outer, exp);
					exp->card = CARD_ATOM;
				} else if (is_groupby(outer->op) && is_sql_aggr(f) && exps_find_match_exp(outer->exps, exp))
					exp = exp_ref(sql->sa, exp);
				else
					exp->card = CARD_ATOM;
				set_freevar(exp, i);
			}
			if (exp && outer && is_join(outer->op))
				set_dependent(outer);
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
		if (!exp)
			return sql_error(sql, 02, SQLSTATE(42S22) "SELECT: no such column '%s.%s'", tname, cname);
		if (exp && inner && inner->card <= CARD_AGGR && exp->card > CARD_AGGR && (is_sql_sel(f) || is_sql_having(f)) && !is_sql_aggr(f))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
		if (exp && inner && is_groupby(inner->op) && !is_sql_aggr(f) && !is_freevar(exp))
			exp = rel_groupby_add_aggr(sql, inner, exp);
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
rel_binop_check_types(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, int upcast)
{
	sql_subtype *t1 = exp_subtype(ls), *t2 = exp_subtype(rs);
	
	if (!t1 || !t2) {
		if (t2 && !t1 && rel_set_type_param(sql, t2, rel, ls, upcast) < 0)
			return -1;
		if (t1 && !t2 && rel_set_type_param(sql, t1, rel, rs, upcast) < 0)
			return -1;
	}
	if (!exp_subtype(ls) && !exp_subtype(rs)) {
		(void) sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");
		return -1;
	}
	return 0;
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
rel_check_type(mvc *sql, sql_subtype *t, sql_rel *rel, sql_exp *exp, check_type tpe)
{
	int c, err = 0;
	sql_exp* nexp = NULL;
	sql_subtype *fromtype = exp_subtype(exp);

	if ((!fromtype || !fromtype->type) && rel_set_type_param(sql, t, rel, exp, 0) == 0)
		return exp;

	/* first try cheap internal (in-place) conversions ! */
	if ((nexp = exp_convert_inplace(sql, t, exp)) != NULL)
		return nexp;

	if (fromtype && subtype_cmp(t, fromtype) != 0) {
		if (EC_INTERVAL(fromtype->type->eclass) && (t->type->eclass == EC_NUM || t->type->eclass == EC_POS) && t->digits < fromtype->digits) {
			err = 1; /* conversion from interval to num depends on the number of digits */
		} else {
			c = sql_type_convert(fromtype->type->eclass, t->type->eclass);
			if (!c || (c == 2 && tpe == type_set) || (c == 3 && tpe != type_cast)) {
				err = 1;
			} else {
				exp = exp_convert(sql->sa, exp, fromtype, t);
			}
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
rel_convert_types(mvc *sql, sql_rel *ll, sql_rel *rr, sql_exp **L, sql_exp **R, int scale_fixing, check_type tpe)
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
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ls));
		else
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (exps_card(r) > rel->card) {
		sql_exp *rs = l->h->data;
		if (exp_name(rs))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(rs));
		else
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
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

	if (quantifier || exp_is_rel(ls) || exp_is_rel(rs)) {
		if (rs2) {
			e = exp_compare2(sql->sa, ls, rs, rs2, type);
			if (anti)
				set_anti(e);
		} else {
			if (rel_binop_check_types(sql, rel, ls, rs, 0) < 0)
				return NULL;
			e = exp_compare_func(sql, ls, rs, rs2, compare_func((comp_type)type, quantifier?0:anti), quantifier);
			if (anti && quantifier)
				e = rel_unop_(sql, NULL, e, NULL, "not", card_value);
		}
		return rel_select(sql->sa, rel, e);
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

	if (!rel)
		return rel_select(sql->sa, rel_project_exp(sql->sa, exp_atom_bool(sql->sa, 1)), e);

	/* atom or row => select */
	if (ls->card > rel->card) {
		if (exp_name(ls))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ls));
		else
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card > rel->card || (rs2 && rs2->card > rel->card)) {
		if (exp_name(rs))
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(rs));
		else
			return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
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
		e = rel_binop_(sql, rel, ls, rs, NULL, compare_op, card_value);

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
	type = compare_str2type(compare_op);
	if (type == cmp_filter) 
		return rel_filter_exp_(sql, rel, ls, rs, esc, compare_op, 0);
	return rel_compare_exp_(query, rel, ls, rs, esc, type, need_not, quantifier);
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
		compare_op = "=";
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
	if (quantifier)
		ek.card = card_set;

	rs = rel_value_exp(query, &rel, ro, f, ek);
	if (!rs)
		return NULL;
	if (ro2) {
		rs2 = rel_value_exp(query, &rel, ro2, f, ek);
		if (!rs2)
			return NULL;
	}
	if (ls->card > rs->card && rs->card == CARD_AGGR && is_sql_having(f))
		return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", exp_relname(ls), exp_name(ls));
	if (rs->card > ls->card && ls->card == CARD_AGGR && is_sql_having(f))
		return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", exp_relname(rs), exp_name(rs));
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
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
				sql_subtype *t = exp_subtype(e);

				if (!aa && a->type.type->eclass == EC_ANY) {
					atp = t;
					aa = a;
				}
				if (aa && a->type.type->eclass == EC_ANY && t && atp &&
				    t->type->localtype > atp->type->localtype){
					atp = t;
					aa = a;
				}
			}
			for (n = exps->h, m = f->func->ops->h; n && m; n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;
				sql_subtype *ntp = &a->type;

				if (a->type.type->eclass == EC_ANY && atp)
					ntp = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
				e = rel_check_type(sql, ntp, rel, e, type_equal);
				if (!e) {
					nexps = NULL;
					break;
				}
				if (table_func && e->card > CARD_ATOM) {
					sql_subfunc *zero_or_one = sql_bind_func(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e), NULL, F_AGGR);

					e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, has_nil(e));
				}
				append(nexps, e);
			}
			/* dirty hack */
			if (f->res && aa && atp)
				f->res->h->data = sql_create_subtype(sql->sa, atp->type, atp->digits, atp->scale);
			if (nexps) 
				return exp_op(sql->sa, nexps, f);
		}
	}
	return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
}

static sql_exp *
exp_exist(sql_query *query, sql_rel *rel, sql_exp *le, int exists)
{
	mvc *sql = query->sql;
	sql_subfunc *exists_func = NULL;
	sql_subtype *t;

	if (!exp_name(le))
		exp_label(sql->sa, le, ++sql->label);
	if (!exp_subtype(le) && rel_set_type_param(sql, sql_bind_localtype("bit"), rel, le, 0) < 0) /* workaround */
		return NULL;
	t = exp_subtype(le);

	if (exists)
		exists_func = sql_bind_func(sql->sa, sql->session->schema, "sql_exists", t, NULL, F_FUNC);
	else
		exists_func = sql_bind_func(sql->sa, sql->session->schema, "sql_not_exists", t, NULL, F_FUNC);

	if (!exists_func) 
		return sql_error(sql, 02, SQLSTATE(42000) "exist operator on type %s missing", t->type->sqlname);
	return exp_unop(sql->sa, le, exists_func);
}

static sql_exp *
rel_exists_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f)
{
	exp_kind ek = {type_value, card_exists, FALSE};
	sql_exp *le, *e;

	le = rel_value_exp(query, rel, sc->data.sym, f, ek);
	if (!le) 
		return NULL;
	e = exp_exist(query, rel ? *rel : NULL, le, sc->token == SQL_EXISTS);
	if (e) {
		/* only freevar should have CARD_AGGR */
		e->card = CARD_ATOM;
	}
	return e;
}

static sql_rel *
rel_exists_exp(sql_query *query, sql_rel *rel, symbol *sc, int f) 
{
	exp_kind ek = {type_value, card_exists, TRUE};
	mvc *sql = query->sql;
	sql_rel *sq = NULL;

	if (rel)
		query_push_outer(query, rel, f);
	sq = rel_subquery(query, NULL, sc->data.sym, ek);
	if (rel)
		rel = query_pop_outer(query);
	assert(!is_sql_sel(f));
	if (sq) {
		sql_exp *e = exp_rel(sql, sq);
		e = exp_exist(query, rel, e, sc->token == SQL_EXISTS);
		if (e) {
			/* only freevar should have CARD_AGGR */
			e->card = CARD_ATOM;
		}
		rel = rel_select_add_exp(sql->sa, rel, e);
		return rel;
	}
	return NULL;
}

static sql_exp *
rel_in_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, TRUE};
	dlist *dl = sc->data.lval;
	symbol *lo = NULL;
	dnode *n = dl->h->next, *dn = NULL;
	sql_exp *le = NULL, *re, *e = NULL;
	list *ll = sa_list(sql->sa);
	int is_tuple = 0;

	/* complex case */
	if (dl->h->type == type_list) { /* (a,b..) in (.. ) */
		dn = dl->h->data.lval->h;
		lo = dn->data.sym;
		dn = dn->next;
	} else {
		lo = dl->h->data.sym;
	}
	for( ; lo; lo = dn?dn->data.sym:NULL, dn = dn?dn->next:NULL ) {
		le = rel_value_exp(query, rel, lo, f, ek);
		if (!le)
			return NULL;
		ek.card = card_set;
		append(ll, le);
	}
	if (list_length(ll) == 1) {
		le = ll->h->data;
	} else {
		le = exp_values(sql->sa, ll);
		exp_label(sql->sa, le, ++sql->label);
		ek.card = card_relation;
		is_tuple = 1;
	}
	/* list of values or subqueries */
	if (n->type == type_list) {
		sql_exp *values;
		list *vals = sa_list(sql->sa);

		n = dl->h->next;
		n = n->data.lval->h;

		for (; n; n = n->next) {
			re = rel_value_exp(query, rel, n->data.sym, f, ek);
			if (!re)
				return NULL;
			if (is_tuple && !exp_is_rel(re)) 
				return sql_error(sql, 02, SQLSTATE(42000) "Cannot match a tuple to a single value");
			if (is_tuple)
				re = exp_rel_label(sql, re);
			append(vals, re);
		}

		values = exp_values(sql->sa, vals);
		exp_label(sql->sa, values, ++sql->label);
		if (is_tuple) { 
			sql_exp *e_rel = (sql_exp *) vals->h->data;
			list *le_vals = le->f, *rel_vals = ((sql_rel*)e_rel->l)->exps;

			for (node *m = le_vals->h, *o = rel_vals->h ; m && o ; m = m->next, o = o->next) {
				sql_exp *e = m->data, *f = o->data;

				if (rel_binop_check_types(sql, rel ? *rel : NULL, e, f, 0) < 0)
					return NULL;
			}
		} else { /* if it's not a tuple, enforce coersion on the type for every element on the list */
			values = exp_values_set_supertype(sql, values);

			if (rel_binop_check_types(sql, rel ? *rel : NULL, le, values, 0) < 0)
				return NULL;
		}
		e = exp_in_func(sql, le, values, (sc->token == SQL_IN), is_tuple);
	}
	if (e && le)
		e->card = le->card;
	return e;
}

static sql_rel *
rel_in_exp(sql_query *query, sql_rel *rel, symbol *sc, int f) 
{
	mvc *sql = query->sql;
	sql_exp *e = rel_in_value_exp(query, &rel, sc, f);

	assert(!is_sql_sel(f));
	if (!e || !rel)
		return NULL;
	rel = rel_select_add_exp(sql->sa, rel, e);
	return rel;
}

sql_exp *
rel_logical_value_exp(sql_query *query, sql_rel **rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, FALSE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

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
			return rel_binop_(sql, rel ? *rel : NULL, ls, rs, NULL, "or", card_value);
		else
			return rel_binop_(sql, rel ? *rel : NULL, ls, rs, NULL, "and", card_value);
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
			compare_op = "=";
		}

		ls = rel_value_exp(query, rel, lo, f, ek);
		if (!ls)
			return NULL;
		if (quantifier)
			ek.card = card_set;

		rs = rel_value_exp(query, rel, ro, f, ek);
		if (!rs)
			return NULL;

		if (rel_binop_check_types(sql, rel ? *rel : NULL, ls, rs, 0) < 0)
			return NULL;
		ls = exp_compare_func(sql, ls, rs, NULL, compare_func(compare_str2type(compare_op), quantifier?0:need_not), quantifier);
		if (need_not && quantifier)
			ls = rel_unop_(sql, NULL, ls, NULL, "not", card_value);
		return ls;
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
		return rel_in_value_exp(query, rel, sc, f);
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
		return rel_exists_value_exp(query, rel, sc, f);
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
			return rel_nop_(sql, rel ? *rel : NULL, le, re, ee, NULL, sys, like, card_value);
		return rel_binop_(sql, rel ? *rel : NULL, le, re, sys, like, card_value);
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
		sql_subtype *t1, *t2, *t3;
		sql_exp *e1 = NULL, *e2 = NULL;

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;

		t1 = exp_subtype(le);
		t2 = exp_subtype(re1);
		t3 = exp_subtype(re2);

		if (!t1 && (t2 || t3) && rel_binop_check_types(sql, rel ? *rel : NULL, le, t2 ? re1 : re2, 0) < 0)
			return NULL;
		if (!t2 && (t1 || t3) && rel_binop_check_types(sql, rel ? *rel : NULL, le, t1 ? le : re2, 0) < 0)
			return NULL;
		if (!t3 && (t1 || t2) && rel_binop_check_types(sql, rel ? *rel : NULL, le, t1 ? le : re1, 0) < 0)
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

			if (!min || !max)
				return sql_error(sql, 02, SQLSTATE(42000) "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
			tmp = exp_binop(sql->sa, re1, re2, min);
			re2 = exp_binop(sql->sa, re1, re2, max);
			re1 = tmp;
		}

		if (sc->token == SQL_NOT_BETWEEN) {
			e1 = rel_binop_(sql, rel ? *rel : NULL, le, re1, NULL, "<", card_value);
			e2 = rel_binop_(sql, rel ? *rel : NULL, le, re2, NULL, ">", card_value);
		} else {
			e1 = rel_binop_(sql, rel ? *rel : NULL, le, re1, NULL, ">=", card_value);
			e2 = rel_binop_(sql, rel ? *rel : NULL, le, re2, NULL, "<=", card_value);
		}
		if (!e1 || !e2)
			return NULL;
		if (sc->token == SQL_NOT_BETWEEN) {
			return rel_binop_(sql, rel ? *rel : NULL, e1, e2, NULL, "or", card_value);
		} else {
			return rel_binop_(sql, rel ? *rel : NULL, e1, e2, NULL, "and", card_value);
		}
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *le = rel_value_exp(query, rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, rel ? *rel : NULL, le, NULL, "isnull", card_value);
		set_has_no_nil(le);
		if (sc->token != SQL_IS_NULL)
			le = rel_unop_(sql, rel ? *rel : NULL, le, NULL, "not", card_value);
		set_has_no_nil(le);
		return le;
	}
	case SQL_NOT: {
		sql_exp *le = rel_logical_value_exp(query, rel, sc->data.sym, f);

		if (!le)
			return le;
		return rel_unop_(sql, rel ? *rel : NULL, le, NULL, "not", card_value);
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
		if (rel && *rel)
			query_push_outer(query, *rel, f);
		sql_rel *sq = rel_setquery(query, sc);
		if (rel && *rel)
			*rel = query_pop_outer(query);
		if (sq)
			return exp_rel(sql, sq);
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
		return rel_binop_(sql, rel ? *rel : NULL, le, re, NULL, "=", 0);
	}
	}
}

sql_rel *
rel_logical_exp(sql_query *query, sql_rel *rel, symbol *sc, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, TRUE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

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
			if (!lr)
				return NULL;
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
			if (!lr)
				return NULL;
			rr = rel_logical_exp(query, rr, ro, f);
		}

		if (!lr || !rr)
			return NULL;
		return rel_or(sql, rel, lr, rr, exps, lexps, rexps);
	}
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		rel = rel_logical_exp(query, rel, lo, f);
		if (!rel)
			return NULL;
		return rel_logical_exp(query, rel, ro, f);
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
		return rel_exists_exp(query, rel , sc, f);
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
		sql_subtype *t1, *t2, *t3;
		int flag = 0;

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;

		t1 = exp_subtype(le);
		t2 = exp_subtype(re1);
		t3 = exp_subtype(re2);

		if (!t1 && (t2 || t3) && rel_binop_check_types(sql, rel, le, t2 ? re1 : re2, 0) < 0)
			return NULL;
		if (!t2 && (t1 || t3) && rel_binop_check_types(sql, rel, le, t1 ? le : re2, 0) < 0)
			return NULL;
		if (!t3 && (t1 || t2) && rel_binop_check_types(sql, rel, le, t1 ? le : re1, 0) < 0)
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

			if (!min || !max)
				return sql_error(sql, 02, SQLSTATE(42000) "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
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
				e1 = rel_binop_(sql, rel, le, re1, NULL, "<", card_value);
				e2 = rel_binop_(sql, rel, le, re2, NULL, ">", card_value);
			} else {
				e1 = rel_binop_(sql, rel, le, re1, NULL, ">=", card_value);
				e2 = rel_binop_(sql, rel, le, re2, NULL, "<=", card_value);
			}
			if (!e1 || !e2)
				return NULL;
			if (sc->token == SQL_NOT_BETWEEN) {
				e1 = rel_binop_(sql, rel, e1, e2, NULL, "or", card_value);
			} else {
				e1 = rel_binop_(sql, rel, e1, e2, NULL, "and", card_value);
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
		le = rel_unop_(sql, rel, le, NULL, "isnull", card_value);
		set_has_no_nil(le);
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
		default:
			break;
		} 
		le = rel_value_exp(query, &rel, sc->data.sym, f|sql_farg, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, rel, le, NULL, "not", card_value);
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
		assert(!rel);
		return rel_setquery(query, sc);
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

static sql_exp * _rel_aggr(sql_query *query, sql_rel **rel, int distinct, sql_schema *s, char *aname, dnode *arguments, int f);
static sql_exp *rel_aggr(sql_query *query, sql_rel **rel, symbol *se, int f);

static sql_exp *
rel_op(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek )
{
	mvc *sql = query->sql;
	dnode *l = se->data.lval->h;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;
	sql_subfunc *sf = NULL;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (!s)
		return NULL;

	sf = find_func(sql, s, fname, 0, F_AGGR, NULL);
	if (!sf && *rel && (*rel)->card == CARD_AGGR) {
		if (is_sql_having(f) || is_sql_orderby(f))
			return NULL;
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such aggregate '%s'", fname);
	}
	if (sf)
		return _rel_aggr(query, rel, 0, s, fname, NULL, f);
	return rel_op_(sql, s, fname, ek);
}

sql_exp *
rel_unop_(mvc *sql, sql_rel *rel, sql_exp *e, sql_schema *s, char *fname, int card)
{
	sql_subfunc *f = NULL;
	sql_subtype *t = exp_subtype(e);
	sql_ftype type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:
		   ((card == card_relation)?F_UNION:F_FUNC));

	if (!s)
		s = sql->session->schema;

	/* handle param's early */
	if (!t) {
		f = find_func(sql, s, fname, 1, type, NULL);
		if (!f)
			f = find_func(sql, s, fname, 1, F_AGGR, NULL);
		if (f) {
			sql_arg *a = f->func->ops->h->data;

			t = &a->type;
			if (rel_set_type_param(sql, t, rel, e, 1) < 0)
				return NULL;
		}
	 } else {
		f = bind_func(sql, s, fname, t, NULL, type);
		if (!f)
			f = bind_func(sql, s, fname, t, NULL, F_AGGR);
	}

	if (f && type_has_tz(t) && f->func->fix_scale == SCALE_FIX) {
		/* set timezone (using msec (.3)) */
		sql_subtype *intsec = sql_bind_subtype(sql->sa, "sec_interval", 10 /*hour to second */, 3);
		atom *a = atom_int(sql->sa, intsec, sql->timezone);
		sql_exp *tz = exp_atom(sql->sa, a);

		e = rel_binop_(sql, rel, e, tz, NULL, "sql_add", card);
		if (!e)
			return NULL;
	}

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
			sql_subfunc *zero_or_one = sql_bind_func(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e), NULL, F_AGGR);

			e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, has_nil(e));
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

static sql_exp *
rel_unop(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	mvc *sql = query->sql;
	dnode *l = se->data.lval->h;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	sql_exp *e = NULL;
	sql_subfunc *sf = NULL;
	sql_ftype type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:F_FUNC);

	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (!s)
		return NULL;

	e = rel_value_exp(query, rel, l->next->next->data.sym, f|sql_farg, iek);
	if (!e)
		sf = find_func(sql, s, fname, 1, F_AGGR, NULL);

	if (!sf && !e && *rel && (*rel)->card == CARD_AGGR) {
		if (is_sql_having(f) || is_sql_orderby(f))
			return NULL;
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such aggregate '%s'", fname);
	}
	if (!e && sf) { /* possibly we cannot resolve the argument as the function maybe an aggregate */
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		return rel_aggr(query, rel, se, f);
	}
	if (type == F_FUNC) {
		sf = find_func(sql, s, fname, 1, F_AGGR, NULL);
		if (sf) {
			if (!e) { /* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';
			}
			return _rel_aggr(query, rel, l->next->data.i_val, s, fname, l->next->next, f);
		}
	}
	if (!e)
		return NULL;
	return rel_unop_(sql, rel ? *rel : NULL, e, s, fname, ek.card);
}

#define is_addition(fname) (strcmp(fname, "sql_add") == 0)
#define is_subtraction(fname) (strcmp(fname, "sql_sub") == 0)

sql_exp *
rel_binop_(mvc *sql, sql_rel *rel, sql_exp *l, sql_exp *r, sql_schema *s, char *fname, int card)
{
	sql_exp *res = NULL;
	sql_subtype *t1 = exp_subtype(l), *t2 = exp_subtype(r);
	sql_subfunc *f = NULL;
	sql_ftype type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:((card == card_relation)?F_UNION:F_FUNC));
	if (card == card_loader)
		card = card_none;

	if (!s)
		s = sql->session->schema;

	/* handle param's early */
	if (!t1 || !t2) {
		f = sql_resolve_function_with_undefined_parameters(sql->sa, s, fname, list_append(list_append(sa_list(sql->sa), t1), t2), type);
		if (f) { /* add types using f */
			if (!t1) 
				rel_set_type_param(sql, arg_type(f->func->ops->h->data), rel, l, 1);
			if (!t2)
				rel_set_type_param(sql, arg_type(f->func->ops->h->next->data), rel, r, 1);
			f = NULL;

			if (!exp_subtype(l) || !exp_subtype(r))
				return sql_error(sql, 01, SQLSTATE(42000) "Cannot have a parameter (?) on both sides of an expression");
		} else if (rel_binop_check_types(sql, rel, l, r, 1) < 0)
			return NULL;

		t1 = exp_subtype(l);
		t2 = exp_subtype(r);
		assert(t1 && t2);
	}

	if (!f && (is_addition(fname) || is_subtraction(fname)) && 
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

	if (!f)
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
			sql_subfunc *zero_or_one = sql_bind_func(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(l), NULL, F_AGGR);

			l = exp_aggr1(sql->sa, l, zero_or_one, 0, 0, CARD_ATOM, has_nil(l));
		}
		if (card == card_relation && r->card > CARD_ATOM) {
			sql_subfunc *zero_or_one = sql_bind_func(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(r), NULL, F_AGGR);

			r = exp_aggr1(sql->sa, r, zero_or_one, 0, 0, CARD_ATOM, has_nil(r));
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

			while((f = bind_member_func(sql, s, fname, t1, 2, type, prev)) != NULL) {
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

		if ((f = bind_member_func(sql, s, fname, t1, 2, type, NULL)) != NULL && check_card(card,f)) {
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

static sql_exp *
rel_binop(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	mvc *sql = query->sql;
	dnode *dl = se->data.lval->h;
	sql_exp *l, *r;
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

	l = rel_value_exp(query, rel, dl->next->next->data.sym, f|sql_farg, iek);
	r = rel_value_exp(query, rel, dl->next->next->next->data.sym, f|sql_farg, iek);

	if (!l || !r)
		sf = find_func(sql, s, fname, 2, F_AGGR, NULL);
	if (!sf && (!l || !r) && *rel && (*rel)->card == CARD_AGGR) {
		if (mvc_status(sql) || is_sql_having(f) || is_sql_orderby(f))
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
			return _rel_aggr(query, rel, dl->next->data.i_val, s, fname, dl->next->next, f);
		}
	}

	if (!l || !r)
		return NULL;
	return rel_binop_(sql, rel ? *rel : NULL, l, r, s, fname, ek.card);
}

sql_exp *
rel_nop_(mvc *sql, sql_rel *rel, sql_exp *a1, sql_exp *a2, sql_exp *a3, sql_exp *a4, sql_schema *s, char *fname,
		 int card)
{
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
	dnode *ops = l->next->next->data.lval->h;
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
		sql_exp *e = rel_value_exp(query, rel, ops->data.sym, fs|sql_farg, iek);
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
		return _rel_aggr(query, rel, l->next->data.i_val, s, fname, l->next->next->data.lval->h, fs);
	}
	if (err)
		return NULL;
	return _rel_nop(sql, s, fname, tl, rel ? *rel : NULL, exps, obj_type, nr_args, ek);
}

static sql_exp *
_rel_aggr(sql_query *query, sql_rel **rel, int distinct, sql_schema *s, char *aname, dnode *args, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_subfunc *a = NULL;
	int no_nil = 0, group = 0, has_freevar = 0;
	unsigned int all_freevar = 0;
	sql_rel *groupby = *rel, *sel = NULL, *gr, *og = NULL, *res = groupby;
	sql_rel *subquery = NULL;
	list *exps = NULL;
	bool is_grouping = !strcmp(aname, "grouping"), has_args = false;

	if (!query_has_outer(query)) {
		if (!groupby) {
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: missing group by",
							uaname ? toUpperCopy(uaname, aname) : aname);
			if (uaname)
				GDKfree(uaname);
			return e;
		} else if (is_sql_groupby(f)) {
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate function '%s' not allowed in GROUP BY clause",
								uaname ? toUpperCopy(uaname, aname) : aname, aname);
			if (uaname)
				GDKfree(uaname);
			return e;
		} else if (is_sql_join(f)) { /* the is_sql_join test must come before is_sql_where, because the join conditions are handled with sql_where */
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions not allowed in JOIN conditions",
						uaname ? toUpperCopy(uaname, aname) : aname);
			if (uaname)
				GDKfree(uaname);
			return e;
		} else if (is_sql_where(f)) {
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
	}

	exps = sa_list(sql->sa);
	if (args && args->data.sym) {
		int all_aggr = query_has_outer(query);
		all_freevar = 1;
		for (	; args && args->data.sym; args = args->next ) {
			int base = (!groupby || !is_project(groupby->op) || is_base(groupby->op) || is_processed(groupby));
			sql_rel *gl = base?groupby:groupby->l, *ogl = gl; /* handle case of subqueries without correlation */
			sql_exp *e = rel_value_exp(query, &gl, args->data.sym, (f | sql_aggr)& ~sql_farg, ek);

			has_args = true;
			if (gl && gl != ogl) {
				if (!base)
					groupby->l = subquery = gl;
				else
					groupby = subquery = gl;
			}
			if (!e)
				return NULL;
			if (!exp_subtype(e)) { /* we also do not expect parameters here */
				char *uaname = GDKmalloc(strlen(aname) + 1);
				sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: parameters not allowed as arguments to aggregate functions",
						uaname ? toUpperCopy(uaname, aname) : aname);
				if (uaname)
					GDKfree(uaname);
				return e;
			}
			all_aggr &= (exp_card(e) <= CARD_AGGR && !exp_is_atom(e) && !is_func(e->type) && (!is_groupby(groupby->op) || !groupby->r || !exps_find_exp(groupby->r, e)));
			has_freevar |= exp_has_freevar(sql, e);
			all_freevar &= (is_freevar(e)>0);
			list_append(exps, e);
		}
		if (all_aggr && !all_freevar) {
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate functions cannot be nested",
				       uaname ? toUpperCopy(uaname, aname) : aname);
			if (uaname)
				GDKfree(uaname);
			return e;
		}
		if (is_sql_groupby(f) && !all_freevar) {
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: aggregate function '%s' not allowed in GROUP BY clause",
							   uaname ? toUpperCopy(uaname, aname) : aname, aname);
			if (uaname)
				GDKfree(uaname);
			return e;
		}
	}

	if (all_freevar) { /* case 2, ie use outer */
		sql_exp *exp = NULL;
		/* find proper relation, base on freevar (stack hight) */
		for (node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (all_freevar<is_freevar(e))
				all_freevar = is_freevar(e);
			exp = e;
		}
		int sql_state = query_fetch_outer_state(query,all_freevar-1);
		res = groupby = query_fetch_outer(query, all_freevar-1);
		if (exp && is_sql_aggr(sql_state) && !is_groupby_col(res, exp)) {
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 05, SQLSTATE(42000) "%s: aggregate function calls cannot be nested",
							   uaname ? toUpperCopy(uaname, aname) : aname);
			if (uaname)
				GDKfree(uaname);
			return e;
		}
	}

	/* find having select */
	if (!subquery && groupby && !is_processed(groupby) && is_sql_having(f)) { 
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
	if (!subquery && groupby && !is_processed(groupby) && !is_base(groupby->op)) { 
		og = groupby;
		groupby = rel_find_groupby(groupby);
		if (groupby)
			group = 1;
		else
			groupby = og;
	}

	if (!groupby && exps_card(exps) > CARD_ATOM) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: missing group by",
				       uaname ? toUpperCopy(uaname, aname) : aname);
		if (uaname)
			GDKfree(uaname);
		return e;
	}

	if (!subquery && groupby && groupby->op != op_groupby) { 		/* implicit groupby */
		res = groupby = rel_groupby(sql, groupby, NULL);
	}
	if (subquery) {
		assert(!all_freevar);
		res = groupby;
		if (is_sql_sel(f) && is_left(subquery->op) && !is_groupby(groupby->op)) {
			res = groupby = rel_groupby(sql, groupby, NULL);
			exps_set_freevar(sql, exps, groupby); /* mark free variables */
		} else if (!is_groupby(groupby->op)) {
			res = groupby = rel_groupby(sql, groupby, NULL);
		}
		assert(!is_base(groupby->op));
	}
	if ((!exps || exps_card(exps) > CARD_ATOM) && (!res || !groupby))
		return NULL;

	if (all_freevar) {
		query_update_outer(query, res, all_freevar-1);
	} else {
		*rel = res;
	}

	if (!has_args) {	/* count(*) case */
		sql_exp *e;

		if (strcmp(aname, "count") != 0) {
			char *uaname = GDKmalloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, SQLSTATE(42000) "%s: unable to perform '%s(*)'",
					       uaname ? toUpperCopy(uaname, aname) : aname, aname);
			if (uaname)
				GDKfree(uaname);
			return e;
		}
		a = sql_bind_func(sql->sa, s, aname, sql_bind_localtype("void"), NULL, F_AGGR);
		e = exp_aggr(sql->sa, NULL, a, distinct, 0, groupby?groupby->card:CARD_ATOM, 0);

		if (!groupby)
			return e;
		e = rel_groupby_add_aggr(sql, groupby, e);
		if (!group && !all_freevar)
			return e;
		if (all_freevar) {
			if (is_simple_project(res->op)) {
				assert(0);
				e = rel_project_add_exp(sql, res, e);
				e = exp_ref(sql->sa, e);
			}
			e->card = CARD_ATOM;
			set_freevar(e, all_freevar-1);
			return e;
		}
		return e;
	} 

	/* use cnt as nils shouldn't be counted */
	no_nil = 1;

	gr = groupby;
	if (gr && gr->op == op_project && gr->l)
		gr = gr->l;

	if (is_grouping) {
		sql_subtype *tpe;
		list *l = (list*) groupby->r;

		if (list_length(l) <= 7)
			tpe = sql_bind_localtype("bte");
		else if (list_length(l) <= 15)
			tpe = sql_bind_localtype("sht");
		else if (list_length(l) <= 31)
			tpe = sql_bind_localtype("int");
		else if (list_length(l) <= 63)
			tpe = sql_bind_localtype("lng");
#ifdef HAVE_HGE
		else if (list_length(l) <= 127)
			tpe = sql_bind_localtype("hge");
#endif
		else
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: GROUPING the number of grouping columns is larger"
								" than the maximum number of representable bits from this server (%d > %d)", list_length(l),
#ifdef HAVE_HGE
							 127
#else
							 63
#endif
							);
		a = sql_bind_func_result(sql->sa, s, aname, F_AGGR, tpe, 1, exp_subtype(exps->h->data));
	} else
		a = sql_bind_func_(sql->sa, s, aname, exp_types(sql->sa, exps), F_AGGR);

	if (!a && list_length(exps) > 1) {
		sql_subtype *t1 = exp_subtype(exps->h->data);
		a = sql_bind_member(sql->sa, s, aname, exp_subtype(exps->h->data), F_AGGR, list_length(exps), NULL);

		if (list_length(exps) != 2 || (!EC_NUMBER(t1->type->eclass) || !a || subtype_cmp(
						&((sql_arg*)a->func->ops->h->data)->type,
						&((sql_arg*)a->func->ops->h->next->data)->type) != 0) )  {
			if (a) {
				node *n, *op = a->func->ops->h;
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
				a = sql_bind_func_(sql->sa, s, aname, tps, F_AGGR);
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
		a = sql_bind_func_(sql->sa, s, aname, exp_types(sql->sa, nexps), F_AGGR);
		if (a && list_length(nexps))  /* count(col) has |exps| != |nexps| */
			exps = nexps;
		if (!a) {
			list *aggrs = sql_find_funcs(sql->sa, s, aname, list_length(exps), F_AGGR);
			for (node *m = aggrs->h ; m; m = m->next) {
				list *nexps = sa_list(sql->sa);
				node *n, *op;
				a = (sql_subfunc *) m->data;
				op = a->func->ops->h;

				for (n = exps->h ; a && op && n; op = op->next, n = n->next ) {
					sql_arg *arg = op->data;
					sql_exp *e = n->data;

					e = rel_check_type(sql, &arg->type, *rel, e, type_equal); /* rel is a valid pointer */
					if (!e)
						a = NULL;
					list_append(nexps, e);
				}
				if (a) {
					if (list_length(nexps)) /* count(col) has |exps| != |nexps| */
						exps = nexps;
					/* reset error */
					sql->session->status = 0;
					sql->errstr[0] = '\0';
					break;
				}
			}
		}
	}
	if (a && execute_priv(sql,a->func)) {
		sql_exp *e = exp_aggr(sql->sa, exps, a, distinct, no_nil, groupby?groupby->card:CARD_ATOM, have_nil(exps));

		if (!groupby)
			return e;
		e = rel_groupby_add_aggr(sql, groupby, e);
		if (!group && !all_freevar)
			return e;
		if (all_freevar) {
			exps_reset_freevar(exps);
			if (is_simple_project(res->op)) {
				assert(0);
				e = rel_project_add_exp(sql, res, e);
				e = exp_ref(sql->sa, e);
			}
			e->card = CARD_ATOM;
			set_freevar(e, all_freevar-1);
			return e;
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
		return e;
	}
}

static sql_exp *
rel_aggr(sql_query *query, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;
	dnode *d = l->h->next->next;
	int distinct = l->h->next->data.i_val;
	char *aname = qname_fname(l->h->data.lval);
	char *sname = qname_schema(l->h->data.lval);
	sql_schema *s = query->sql->session->schema;

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
				cond = rel_binop_(sql, rel ? *rel : NULL, e1, e2, NULL, "=", card_value);
				result = exp_null(sql->sa, exp_subtype(e1));
				else_exp = exp_ref_save(sql, e1);	/* ELSE case */
			}
			/* COALESCE(e1,e2) == CASE WHEN e1
			   IS NOT NULL THEN e1 ELSE e2 END */
		} else if (token == SQL_COALESCE) {
			cond = rel_value_exp(query, rel, dn->data.sym, f, ek);

			if (cond) {
				sql_exp *le;

				result = exp_ref_save(sql, cond);
				le = rel_unop_(sql, rel ? *rel : NULL, cond, NULL, "isnull", card_value);
				set_has_no_nil(le);
				cond = rel_unop_(sql, rel ? *rel : NULL, le, NULL, "not", card_value);
				set_has_no_nil(cond);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(query, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(query, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &l, &r, 1, type_equal) < 0)
					return NULL;
				cond = rel_binop_(sql, rel ? *rel : NULL, l, r, NULL, "=", card_value);
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
	/* for COALESCE we skip the last (else part) */
	for (; dn && (token != SQL_COALESCE || dn->next); dn = dn->next) {
		sql_exp *cond = NULL, *result = NULL;

		if (token == SQL_COALESCE) {
			cond = rel_value_exp(query, rel, dn->data.sym, f, ek);

			if (cond) {
				sql_exp *le;

				result = exp_ref_save(sql, cond);
				le = rel_unop_(sql, rel ? *rel : NULL, cond, NULL, "isnull", card_value);
				set_has_no_nil(le);
				cond = rel_unop_(sql, rel ? *rel : NULL, le, NULL, "not", card_value);
				set_has_no_nil(cond);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(query, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(query, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, rel ? *rel : NULL, rel ? *rel : NULL, &l, &r, 1, type_equal) < 0)
					return NULL;
				cond = rel_binop_(sql, rel ? *rel : NULL, l, r, NULL, "=", card_value);
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
		if (tpe && restype) {
			supertype(&rtype, restype, tpe);
			restype = &rtype;
		} else if (tpe) {
			restype = tpe;
		}
	}
	if (opt_else || else_exp) {
		sql_exp *result = else_exp;

		if (!result && !(result = rel_value_exp(query, rel, opt_else, f, ek))) 
			return NULL;

		tpe = exp_subtype(result);
		if (tpe && restype) {
			supertype(&rtype, restype, tpe);
			restype = &rtype;
		} else if (tpe) {
			restype = tpe;
		}

		if (!restype)
			return sql_error(sql, 02, SQLSTATE(42000) "Result type missing");
		if (restype->type->localtype == TYPE_void) /* NULL */
			restype = sql_bind_localtype("str");

		if (!result || !(result = rel_check_type(sql, restype, rel ? *rel : NULL, result, type_equal)))
			return NULL;
		res = result;

		if (!res) 
			return NULL;
	} else {
		if (!restype)
			return sql_error(sql, 02, SQLSTATE(42000) "Result type missing");
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

		if (!cond || !result || !res)
			return NULL;
		res = rel_nop_(sql, rel ? *rel : NULL, cond, result, res, NULL, NULL, "ifthenelse", card_value);
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
	dlist *gl;
	char *name = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	if (grp->token != SQL_COLUMN && grp->token != SQL_IDENT)
		return NULL;
	gl = grp->data.lval;
	if (dlist_length(gl) > 1)
		return NULL;
	if (!selection)
		return NULL;

	name = gl->h->data.sval;
	for (dnode *n = selection->h; n; n = n->next) {
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

static sql_exp*
rel_group_column(sql_query *query, sql_rel **rel, symbol *grp, dlist *selection, int f)
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_value, TRUE};
	sql_exp *e = rel_value_exp2(query, rel, grp, f, ek);

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
	return e;
}

static list*
list_power_set(sql_allocator *sa, list* input) /* cube */
{
	list *res = sa_list(sa);
	/* N stores total number of subsets */
	int N = (int) pow(2, input->cnt);

	/* generate each subset one by one */
	for (int i = 0; i < N; i++) {
		list *ll = sa_list(sa);
		int j = 0; /* check every bit of i */
		for (node *n = input->h ; n ; n = n->next) {
			/* if j'th bit of i is set, then append */
			if (i & (1 << j))
				list_prepend(ll, n->data);
			j++;
		}
		list_prepend(res, ll);
	}
	return res;
}

static list*
list_rollup(sql_allocator *sa, list* input)
{
	list *res = sa_list(sa);

	for (int counter = input->cnt; counter > 0; counter--) {
		list *ll = sa_list(sa);
		int j = 0;
		for (node *n = input->h; n && j < counter; j++, n = n->next)
			list_append(ll, n->data);
		list_append(res, ll);
	}
	list_append(res, sa_list(sa)); /* global aggregate case */
	return res;
}

static int
list_equal(list* list1, list* list2)
{
	for (node *n = list1->h; n ; n = n->next) {
		sql_exp *e = (sql_exp*) n->data;
		if (!exps_find_exp(list2, e))
			return 1;
	}
	for (node *n = list2->h; n ; n = n->next) {
		sql_exp *e = (sql_exp*) n->data;
		if (!exps_find_exp(list1, e))
			return 1;
	}
	return 0;
}

static list*
lists_cartesian_product_and_distinct(sql_allocator *sa, list *l1, list *l2)
{
	list *res = sa_list(sa);

	/* for each list of l2, merge into each list of l1 while removing duplicates */
	for (node *n = l1->h ; n ; n = n->next) {
		list *sub_list = (list*) n->data;

		for (node *m = l2->h ; m ; m = m->next) {
			list *other = (list*) m->data;
			list_append(res, list_distinct(list_merge(list_dup(sub_list, (fdup) NULL), other, (fdup) NULL), (fcmp) list_equal, (fdup) NULL));
		}
	}
	return res;
}

static list*
rel_groupings(sql_query *query, sql_rel **rel, symbol *groupby, dlist *selection, int f, bool grouping_sets, list **sets)
{
	mvc *sql = query->sql;
	list *exps = new_exp_list(sql->sa);

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	for (dnode *o = groupby->data.lval->h; o; o = o->next) {
		symbol *grouping = o->data.sym;
		list *next_set = NULL;

		if (grouping->token == SQL_GROUPING_SETS) { /* call recursively, and merge the genererated sets */
			list *other = rel_groupings(query, rel, grouping, selection, f, true, &next_set);
			if (!other)
				return NULL;
			exps = list_distinct(list_merge(exps, other, (fdup) NULL), (fcmp) exp_equal, (fdup) NULL);
		} else {
			dlist *dl = grouping->data.lval;
			if (dl) {
				list *set_cols = new_exp_list(sql->sa); /* columns and combination of columns to be used for the next set */

				for (dnode *oo = dl->h; oo; oo = oo->next) {
					symbol *grp = oo->data.sym;
					list *next_tuple = new_exp_list(sql->sa); /* next tuple of columns */

					if (grp->token == SQL_COLUMN_GROUP) { /* set of columns */
						assert(is_sql_group_totals(f));
						for (dnode *ooo = grp->data.lval->h; ooo; ooo = ooo->next) {
							symbol *elm = ooo->data.sym;
							sql_exp *e = rel_group_column(query, rel, elm, selection, f);
							if (!e)
								return NULL;
							assert(e->type == e_column);
							list_append(next_tuple, e);
							list_append(exps, e);
						}
					} else { /* single column or expression */
						sql_exp *e = rel_group_column(query, rel, grp, selection, f);
						if (!e)
							return NULL;
						if (e->type != e_column) { /* store group by expressions in the stack */
							if (is_sql_group_totals(f)) {
								(void) sql_error(sql, 02, SQLSTATE(42000) "GROUP BY: grouping expressions not possible with ROLLUP, CUBE and GROUPING SETS");
								return NULL;
							}
							if (!stack_push_groupby_expression(sql, grp, e))
								return NULL;
						}
						list_append(next_tuple, e);
						list_append(exps, e);
					}
					list_append(set_cols, next_tuple);
				}
				if (is_sql_group_totals(f)) {
					if (grouping->token == SQL_ROLLUP)
						next_set = list_rollup(sql->sa, set_cols);
					else if (grouping->token == SQL_CUBE)
						next_set = list_power_set(sql->sa, set_cols);
					else /* the list of sets is not used in the "GROUP BY a, b, ..." case */
						next_set = list_append(new_exp_list(sql->sa), set_cols);
				}
			} else if (is_sql_group_totals(f) && grouping_sets) /* The GROUP BY () case is the global aggregate which is always added by ROLLUP and CUBE */
				next_set = list_append(new_exp_list(sql->sa), new_exp_list(sql->sa));
		}
		if (is_sql_group_totals(f)) { /* if there are no sets, set the found one, otherwise calculate cartesian product and merge the distinct ones */
			assert(next_set);
			if (!*sets)
				*sets = next_set;
			else
				*sets = grouping_sets ? list_merge(*sets, next_set, (fdup) NULL) : lists_cartesian_product_and_distinct(sql->sa, *sets, next_set);
		}
	}
	return exps;
}

static list*
rel_partition_groupings(sql_query *query, sql_rel **rel, symbol *partitionby, dlist *selection, int f)
{
	mvc *sql = query->sql;
	dnode *o = partitionby->data.lval->h;
	list *exps = new_exp_list(sql->sa);

	for (; o; o = o->next) {
		symbol *grp = o->data.sym;
		exp_kind ek = {type_value, card_value, TRUE};
		sql_exp *e = rel_value_exp2(query, rel, grp, f, ek);

		if (!e) {
			int status = sql->session->status;
			char buf[ERRSIZE];

			/* reset error */
			sql->session->status = 0;
			strcpy(buf, sql->errstr);
			sql->errstr[0] = '\0';

			e = rel_selection_ref(query, rel, grp, selection);
			if (!e) {
				if (sql->errstr[0] == 0) {
					sql->session->status = status;
					strcpy(sql->errstr, buf);
				}
				return NULL;
			}
		}
		if (exp_is_rel(e))
			return sql_error(sql, 02, SQLSTATE(42000) "PARTITION BY: subqueries not allowed in PARTITION BY clause");
		if (e->type != e_column) { /* store group by expressions in the stack */
			if (!stack_push_groupby_expression(sql, grp, e))
				return NULL;
		}
		if (e->card > CARD_AGGR)
			e->card = CARD_AGGR;
		append(exps, e);
	}
	return exps;
}

/* find selection expressions matching the order by column expression */

/* first limit to simple columns only */
static sql_exp *
rel_order_by_simple_column_exp(mvc *sql, sql_rel *r, symbol *column_r, int f)
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
		e = rel_bind_column(sql, r, name, f, 0);
	}
	if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *name = l->h->next->data.sval;

		e = rel_bind_column2(sql, r, tname, name, f);
	}
	if (e) 
		return e;
	return sql_error(sql, 02, SQLSTATE(42000) "ORDER BY: absolute column names not supported");
}

/* second complex columns only */
static sql_exp *
rel_order_by_column_exp(sql_query *query, sql_rel **R, symbol *column_r, int f)
{
	mvc *sql = query->sql;
	sql_rel *r = *R, *p = NULL;
	sql_exp *e = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	if (!r)
		return e;

	if (r && is_simple_project(r->op) && is_processed(r)) {
		p = r;
		r = r->l;
	}

	e = rel_value_exp(query, &r, column_r, f, ek);

	if (r && !p)
		*R = r;
	else if (r)
		p->l = r;
	if (e && p) {
		e = rel_project_add_exp(sql, p, e);
		return e;
	}
	if (e && r && is_project(r->op)) {
		sql_exp * found = exps_find_exp(r->exps, e);

		if (!found) {
			append(r->exps, e);
		} else {
			e = found;
		}
		if (!exp_name(e))
			exp_label(sql->sa, e, ++sql->label);
		e = exp_ref(sql->sa, e);
	}
	return e;
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
rel_order_by(sql_query *query, sql_rel **R, symbol *orderby, int f)
{
	mvc *sql = query->sql;
	sql_rel *rel = *R, *or = rel; /* the order by relation */
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
				exp_kind ek = {type_value, card_column, FALSE};

				e = rel_value_exp2(query, &rel, col, f, ek);

				if (e && e->card <= CARD_ATOM) {
					sql_subtype *tpe = exp_subtype(e);
					/* integer atom on the stack */
					if (!is_sql_window(f) && e->type == e_atom &&
					    tpe->type->eclass == EC_NUM) {
						atom *a = e->l?e->l:sql->args[e->flag];
						int nr = (int)atom_get_int(a);

						e = exps_get_exp(rel->exps, nr);
						if (!e)
							return sql_error(sql, 02, SQLSTATE(42000) "SELECT: the order by column number (%d) is not in the number of projections range (%d)", nr, list_length(rel->exps));
						e = exp_ref(sql->sa, e);
						/* do not cache this query */
						if (e)
							scanner_reset_key(&sql->scanner);
					} else if (e->type == e_atom) {
						return sql_error(sql, 02, SQLSTATE(42000) "order not of type SQL_COLUMN");
					}
				} else if (e && exp_card(e) > rel->card) {
					if (e && exp_name(e)) {
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(e));
					} else {
						return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
					}
				}
				if (e && rel && is_project(rel->op)) {
					sql_exp * found = exps_find_exp(rel->exps, e);

					if (!found) {
						append(rel->exps, e);
					} else {
						e = found;
					}
					e = exp_ref(sql->sa, e);
				}
			}

			if (!e && sql->session->status != -ERR_AMBIGUOUS && (col->token == SQL_COLUMN || col->token == SQL_IDENT)) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				e = rel_order_by_simple_column_exp(sql, rel, col, sql_sel | sql_orderby | (f & sql_group_totals));
				if (e && e->card > rel->card) 
					e = NULL;
				if (e)
					e = rel_project_add_exp(sql, rel, e);
			}
			if (rel && !e && sql->session->status != -ERR_AMBIGUOUS) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				if (!e)
					e = rel_order_by_column_exp(query, &rel, col, sql_sel | sql_orderby | (f & sql_group_totals));
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
	if (is_sql_orderby(f) && or != rel)
		or->l = rel;
	if (is_sql_window(f))
		*R = rel;
	return exps;
}

static int
generate_window_bound(tokens sql_token, bool first_half)
{
	switch (sql_token) {
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

	if (pe) {
		append(targs1, exp_subtype(pe));
		append(targs2, exp_subtype(pe));
		append(rargs1, exp_copy(sql, pe));
		append(rargs2, exp_copy(sql, pe));
	}
	append(rargs1, exp_copy(sql, e));
	append(rargs2, exp_copy(sql, e));
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
	return e; /* return something to say there were no errors */
}

static sql_exp*
calculate_window_bound(sql_query *query, sql_rel *p, tokens token, symbol *bound, sql_exp *ie, int frame_type, int f)
{
	mvc *sql = query->sql;
	sql_subtype *bt, *it = sql_bind_localtype("int"), *lon = sql_bind_localtype("lng"), *iet;
	sql_class bclass = EC_ANY;
	sql_exp *res = NULL;

	if ((bound->token == SQL_PRECEDING || bound->token == SQL_FOLLOWING || bound->token == SQL_CURRENT_ROW) && bound->type == type_int) {
		atom *a = NULL;
		bt = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? lon : exp_subtype(ie);
		bclass = bt->type->eclass;

		if ((bound->data.i_val == UNBOUNDED_PRECEDING_BOUND || bound->data.i_val == UNBOUNDED_FOLLOWING_BOUND)) {
			if (EC_NUMBER(bclass))
				a = atom_general(sql->sa, bt, NULL);
			else
				a = atom_general(sql->sa, it, NULL);
		} else if (bound->data.i_val == CURRENT_ROW_BOUND) {
			if (EC_NUMBER(bclass))
				a = atom_zero_value(sql->sa, bt);
			else
				a = atom_zero_value(sql->sa, it);
		} else {
			assert(0);
		}
		res = exp_atom(sql->sa, a);
	} else { /* arbitrary expression case */
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
		res = rel_value_exp2(query, &p, bound, f, ek);
		if (!res)
			return NULL;
		if (!(bt = exp_subtype(res))) {
			sql_subtype *t = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? lon : exp_subtype(ie);
			if (rel_set_type_param(sql, t, p, res, 0) < 0) /* workaround */
				return NULL;
			bt = exp_subtype(res);
		}
		bclass = bt->type->eclass;
		if (!(bclass == EC_NUM || EC_INTERVAL(bclass) || bclass == EC_DEC || bclass == EC_FLT))
			return sql_error(sql, 02, SQLSTATE(42000) "%s offset must be of a countable SQL type", bound_desc);
		if ((frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) && bclass != EC_NUM) {
			char *err = subtype2string(bt);
			if (!err)
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			(void) sql_error(sql, 02, SQLSTATE(42000) "Values on %s boundary on %s frame can't be %s type", bound_desc,
							 (frame_type == FRAME_ROWS) ? "rows":"groups", err);
			_DELETE(err);
			return NULL;
		}
		if (frame_type == FRAME_RANGE) {
			if (bclass == EC_FLT && iet->type->eclass != EC_FLT)
				return sql_error(sql, 02, SQLSTATE(42000) "Values in input aren't floating-point while on %s boundary are", bound_desc);
			if (bclass != EC_FLT && iet->type->eclass == EC_FLT)
				return sql_error(sql, 02, SQLSTATE(42000) "Values on %s boundary aren't floating-point while on input are", bound_desc);
			if (bclass == EC_DEC && iet->type->eclass != EC_DEC)
				return sql_error(sql, 02, SQLSTATE(42000) "Values in input aren't decimals while on %s boundary are", bound_desc);
			if (bclass != EC_DEC && iet->type->eclass == EC_DEC)
				return sql_error(sql, 02, SQLSTATE(42000) "Values on %s boundary aren't decimals while on input are", bound_desc);
			if (bclass != EC_SEC && iet->type->eclass == EC_TIME) {
				char *err = subtype2string(iet);
				if (!err)
					return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				(void) sql_error(sql, 02, SQLSTATE(42000) "For %s input the %s boundary must be an interval type up to the day", err, bound_desc);
				_DELETE(err);
				return NULL;
			}
			if (EC_INTERVAL(bclass) && !EC_TEMP(iet->type->eclass)) {
				char *err = subtype2string(iet);
				if (!err)
					return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if ((window_specification = stack_get_window_def(sql, ident, &pos)) == NULL)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window '%s' not found", ident);

	/* avoid infinite lookups */
	if (stack_check_var_visited(sql, pos))
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cyclic references to window '%s' found", ident);
	stack_set_var_visited(sql, pos);

	if (window_specification->h->next->data.sym) {
		if (*partition_by_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of PARTITION BY clause from window '%s'", ident);
		*partition_by_clause = window_specification->h->next->data.sym;
	}
	if (window_specification->h->next->next->data.sym) {
		if (*order_by_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of ORDER BY clause from window '%s'", ident);
		*order_by_clause = window_specification->h->next->next->data.sym;
	}
	if (window_specification->h->next->next->next->data.sym) {
		if (*frame_clause)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: redefinition of frame clause from window '%s'", ident);
		*frame_clause = window_specification->h->next->next->next->data.sym;
	}

	window_ident = window_specification->h->data.sval;
	if (window_ident && !get_window_clauses(sql, window_ident, partition_by_clause, order_by_clause, frame_clause))
		return NULL; /* the error was already set */

	return window_specification; /* return something to say there were no errors */
}

static char*
window_function_arg_types_2str(list* types, int N)
{
	char *arg_list = NULL;
	int i = 0;

	for (node *n = types->h; n && i < N; n = n->next) {
		char *tpe = subtype2string((sql_subtype *) n->data);
		
		if (arg_list) {
			char *t = arg_list;
			arg_list = sql_message("%s, %s", arg_list, tpe);
			_DELETE(t);
			_DELETE(tpe);
		} else {
			arg_list = tpe;
		}
		i++;
	}
	return arg_list;
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
	sql_exp *in = NULL, *pe = NULL, *oe = NULL, *call = NULL, *start = NULL, *eend = NULL, *fstart = NULL, *fend = NULL, *ie = NULL;
	sql_rel *p;
	list *gbe = NULL, *obe = NULL, *args = NULL, *types = NULL, *fargs = NULL;
	sql_schema *s = sql->session->schema;
	dnode *dn = window_function->data.lval->h, *dargs = NULL;
	int distinct = 0, frame_type, pos, nf = f, nfargs = 0;
	bool is_nth_value, supports_frames;

	stack_clear_frame_visited_flag(sql); /* clear visited flags before iterating */

	if (l->h->next->type == type_list) {
		window_specification = l->h->next->data.lval;
	} else if (l->h->next->type == type_string) {
		const char* window_alias = l->h->next->data.sval;
		if ((window_specification = stack_get_window_def(sql, window_alias, &pos)) == NULL)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window '%s' not found", window_alias);
		stack_set_var_visited(sql, pos);
	} else {
		assert(0);
	}

	window_ident = window_specification->h->data.sval;
	partition_by_clause = window_specification->h->next->data.sym;
	order_by_clause = window_specification->h->next->next->data.sym;
	frame_clause = window_specification->h->next->next->next->data.sym;

	if (window_ident && !get_window_clauses(sql, window_ident, &partition_by_clause, &order_by_clause, &frame_clause))
		return NULL;

	frame_type = order_by_clause ? FRAME_RANGE : FRAME_ROWS;
	aname = qname_fname(dn->data.lval);
	sname = qname_schema(dn->data.lval);

	if (sname)
		s = mvc_bind_schema(sql, sname);

	is_nth_value = !strcmp(aname, "nth_value");
	supports_frames = window_function->token != SQL_RANK || is_nth_value || !strcmp(aname, "first_value") || !strcmp(aname, "last_value");

	if (is_sql_join(f) || is_sql_where(f) || is_sql_groupby(f) || is_sql_having(f)) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		const char *clause = is_sql_join(f)?"JOIN conditions":is_sql_where(f)?"WHERE clause":is_sql_groupby(f)?"GROUP BY clause":"HAVING clause";
		(void) sql_error(sql, 02, SQLSTATE(42000) "%s: window function '%s' not allowed in %s",
						 uaname ? toUpperCopy(uaname, aname) : aname, aname, clause);
		if (uaname)
			GDKfree(uaname);
		return NULL;
	} else if (is_sql_aggr(f)) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		(void) sql_error(sql, 02, SQLSTATE(42000) "%s: window functions not allowed inside aggregation functions",
						 uaname ? toUpperCopy(uaname, aname) : aname);
		if (uaname)
			GDKfree(uaname);
		return NULL;
	} else if (is_sql_window(f)) {
		char *uaname = GDKmalloc(strlen(aname) + 1);
		(void) sql_error(sql, 02, SQLSTATE(42000) "%s: window functions cannot be nested",
						 uaname ? toUpperCopy(uaname, aname) : aname);
		if (uaname)
			GDKfree(uaname);
		return NULL;
	}

	/* window operations are only allowed in the projection */
	if (!is_sql_sel(f))
		return sql_error(sql, 02, SQLSTATE(42000) "OVER: only possible within the selection");

	p = *rel;
	/* Partition By */
	if (partition_by_clause) {
		gbe = rel_partition_groupings(query, &p, partition_by_clause, NULL /* cannot use (selection) column references, as this result is a selection column */, nf | sql_window);
		if (!gbe)
			return NULL;
		for (n = gbe->h ; n ; n = n->next) {
			sql_exp *en = n->data;

			set_ascending(en);
			set_nulls_first(en);
		}
	}

	/* Order By */
	if (order_by_clause) {
		obe = rel_order_by(query, &p, order_by_clause, nf | sql_window);
		if (!obe)
			return NULL;
	}

	fargs = sa_list(sql->sa);
	if (window_function->token == SQL_RANK) { /* rank function call */
		dlist *dl = dn->next->next->data.lval;
		bool is_ntile = !strcmp(aname, "ntile"), is_lag = !strcmp(aname, "lag"), is_lead = !strcmp(aname, "lead");

		distinct = dn->next->data.i_val;
		if (!dl || is_ntile) { /* pass an input column for analytic functions that don't require it */
			in = rel_first_column(sql, p);
			if (!in)
				return NULL;
			if (!exp_name(in))
				exp_label(sql->sa, in, ++sql->label);
			in = exp_ref(sql->sa, in);
			append(fargs, in);
			in = exp_ref_save(sql, in);
			nfargs++;
		}
		if (dl)
			for (dargs = dl->h ; dargs ; dargs = dargs->next) {
				exp_kind ek = {type_value, card_column, FALSE};
				sql_subtype *empty = sql_bind_localtype("void"), *bte = sql_bind_localtype("bte");

				in = rel_value_exp2(query, &p, dargs->data.sym, f | sql_window, ek);
				if (!in)
					return NULL;
				if (!exp_subtype(in)) { /* we also do not expect parameters here */
					char *uaname = GDKmalloc(strlen(aname) + 1);
					(void) sql_error(sql, 02, SQLSTATE(42000) "%s: parameters not allowed as arguments to window functions",
									uaname ? toUpperCopy(uaname, aname) : aname);
					if (uaname)
						GDKfree(uaname);
					return NULL;
				}

				/* corner case, if the argument is null convert it into something countable such as bte */
				if (subtype_cmp(exp_subtype(in), empty) == 0)
					in = exp_convert(sql->sa, in, empty, bte);
				if ((is_lag || is_lead) && nfargs == 2) { /* lag and lead 3rd arg must have same type as 1st arg */
					sql_exp *first = (sql_exp*) fargs->h->data;
					if (!(in = rel_check_type(sql, exp_subtype(first), p, in, type_equal)))
						return NULL;
				}
				if (!in)
					return NULL;

				append(fargs, in);
				in = exp_ref_save(sql, in);
				nfargs++;
			}
	} else { /* aggregation function call */
		distinct = dn->next->data.i_val;
		for (dargs = dn->next->next ; dargs && dargs->data.sym ; dargs = dargs->next) {
			exp_kind ek = {type_value, card_column, FALSE};
			sql_subtype *empty = sql_bind_localtype("void"), *bte = sql_bind_localtype("bte");

			in = rel_value_exp2(query, &p, dargs->data.sym, f | sql_window, ek);
			if (!in)
				return NULL;
			if (!exp_subtype(in)) { /* we also do not expect parameters here */
				char *uaname = GDKmalloc(strlen(aname) + 1);
				(void) sql_error(sql, 02, SQLSTATE(42000) "%s: parameters not allowed as arguments to window functions",
								uaname ? toUpperCopy(uaname, aname) : aname);
				if (uaname)
					GDKfree(uaname);
				return NULL;
			}

			/* corner case, if the argument is null convert it into something countable such as bte */
			if (subtype_cmp(exp_subtype(in), empty) == 0)
				in = exp_convert(sql->sa, in, empty, bte);
			if (!in)
				return NULL;

			append(fargs, in);
			in = exp_ref_save(sql, in);
			nfargs++;

			if (!strcmp(aname, "count"))
				append(fargs, exp_atom_bool(sql->sa, 1)); /* ignore nills */
		}

		if (!nfargs) { /* count(*) */
			if (window_function->token == SQL_AGGR && strcmp(aname, "count") != 0) {
				char *uaname = GDKmalloc(strlen(aname) + 1);
				(void) sql_error(sql, 02, SQLSTATE(42000) "%s: unable to perform '%s(*)'",
								uaname ? toUpperCopy(uaname, aname) : aname, aname);
				if (uaname)
					GDKfree(uaname);
				return NULL;
			}

			in = rel_first_column(sql, p);
			if (!exp_name(in))
				exp_label(sql->sa, in, ++sql->label);
			in = exp_ref(sql->sa, in);
			append(fargs, in);
			append(fargs, exp_atom_bool(sql->sa, 0)); /* don't ignore nills */
			in = exp_ref_save(sql, in);
		}
	}

	if (distinct)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: DISTINCT clause is not implemented for window functions");

	/* diff for partitions */
	if (gbe) {
		sql_subtype *bt = sql_bind_localtype("bit");

		for( n = gbe->h; n; n = n->next)  {
			sql_subfunc *df;
			sql_exp *e = n->data;

			if (!exp_subtype(e))
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameters not allowed at PARTITION BY clause from window functions");

			e = exp_copy(sql, e);
			args = sa_list(sql->sa);
			if (pe) { 
				df = bind_func(sql, s, "diff", bt, exp_subtype(e), F_ANALYTIC);
				append(args, pe);
			} else {
				df = bind_func(sql, s, "diff", exp_subtype(e), NULL, F_ANALYTIC);
			}
			if (!df)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function 'diff' not found");
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
			sql_subfunc *df;
			sql_exp *e = n->data;

			if (!exp_subtype(e))
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: parameters not allowed at ORDER BY clause from window functions");

			e = exp_copy(sql, e);
			args = sa_list(sql->sa);
			if (oe) { 
				df = bind_func(sql, s, "diff", bt, exp_subtype(e), F_ANALYTIC);
				append(args, oe);
			} else {
				df = bind_func(sql, s, "diff", exp_subtype(e), NULL, F_ANALYTIC);
			}
			if (!df)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: function 'diff' not found");
			append(args, e);
			oe = exp_op(sql->sa, args, df);
		}
	} else {
		oe = exp_atom_bool(sql->sa, 0);
	}

	if (frame_clause || supports_frames)
		ie = exp_copy(sql, obe ? (sql_exp*) obe->t->data : in);

	/* Frame */
	if (frame_clause) {
		dnode *d = frame_clause->data.lval->h;
		symbol *wstart = d->data.sym, *wend = d->next->data.sym, *rstart = wstart->data.lval->h->data.sym,
			   *rend = wend->data.lval->h->data.sym;
		int excl = d->next->next->next->data.i_val;
		frame_type = d->next->next->data.i_val;

		if (!supports_frames)
			return sql_error(sql, 02, SQLSTATE(42000) "OVER: frame extend only possible with aggregation and first_value, last_value and nth_value functions");
		if (!obe && frame_type == FRAME_GROUPS)
			return sql_error(sql, 02, SQLSTATE(42000) "GROUPS frame requires an order by expression");
		if (wstart->token == SQL_FOLLOWING && wend->token == SQL_PRECEDING)
			return sql_error(sql, 02, SQLSTATE(42000) "FOLLOWING offset must come after PRECEDING offset");
		if (wstart->token == SQL_CURRENT_ROW && wend->token == SQL_PRECEDING)
			return sql_error(sql, 02, SQLSTATE(42000) "CURRENT ROW offset must come after PRECEDING offset");
		if (wstart->token == SQL_FOLLOWING && wend->token == SQL_CURRENT_ROW)
			return sql_error(sql, 02, SQLSTATE(42000) "FOLLOWING offset must come after CURRENT ROW offset");
		if (wstart->token != SQL_CURRENT_ROW && wend->token != SQL_CURRENT_ROW && wstart->token == wend->token &&
		   (frame_type != FRAME_ROWS && frame_type != FRAME_ALL))
			return sql_error(sql, 02, SQLSTATE(42000) "Non-centered windows are only supported in row frames");
		if (!obe && frame_type == FRAME_RANGE) {
			bool ok_preceding = false, ok_following = false;
			if ((wstart->token == SQL_PRECEDING || wstart->token == SQL_CURRENT_ROW) &&
			   (rstart->token == SQL_PRECEDING || rstart->token == SQL_CURRENT_ROW) && rstart->type == type_int &&
			   (rstart->data.i_val == UNBOUNDED_PRECEDING_BOUND || rstart->data.i_val == CURRENT_ROW_BOUND))
				ok_preceding = true;
			if ((wend->token == SQL_FOLLOWING || wend->token == SQL_CURRENT_ROW) &&
			   (rend->token == SQL_FOLLOWING || rend->token == SQL_CURRENT_ROW) && rend->type == type_int &&
			   (rend->data.i_val == UNBOUNDED_FOLLOWING_BOUND || rend->data.i_val == CURRENT_ROW_BOUND))
				ok_following = true;
			if (!ok_preceding || !ok_following)
				return sql_error(sql, 02, SQLSTATE(42000) "RANGE frame with PRECEDING/FOLLOWING offset requires an order by expression");
			frame_type = FRAME_ALL; /* special case, iterate the entire partition */
		}

		if ((fstart = calculate_window_bound(query, p, wstart->token, rstart, ie, frame_type, f | sql_window)) == NULL)
			return NULL;
		if ((fend = calculate_window_bound(query, p, wend->token, rend, ie, frame_type, f | sql_window)) == NULL)
			return NULL;
		if (generate_window_bound_call(sql, &start, &eend, s, gbe ? pe : NULL, ie, fstart, fend, frame_type, excl,
									   wstart->token, wend->token) == NULL)
			return NULL;
	} else if (supports_frames) { /* for analytic functions with no frame clause, we use the standard default values */
		sql_subtype *it = sql_bind_localtype("int"), *lon = sql_bind_localtype("lng"), *bt;
		unsigned char sclass;

		bt = (frame_type == FRAME_ROWS || frame_type == FRAME_GROUPS) ? lon : exp_subtype(ie);
		sclass = bt->type->eclass;
		if (sclass == EC_POS || sclass == EC_NUM || sclass == EC_DEC || EC_INTERVAL(sclass)) {
			fstart = exp_null(sql->sa, bt);
			if (order_by_clause)
				fend = exp_atom(sql->sa, atom_zero_value(sql->sa, bt));
			else
				fend = exp_null(sql->sa, bt);
		} else {
			fstart = exp_null(sql->sa, it);
			if (order_by_clause)
				fend = exp_atom(sql->sa, atom_zero_value(sql->sa, it));
			else
				fend = exp_null(sql->sa, it);
		}
		if (!obe)
			frame_type = FRAME_ALL;

		if (generate_window_bound_call(sql, &start, &eend, s, gbe ? pe : NULL, ie, fstart, fend, frame_type, EXCLUDE_NONE,
									   SQL_PRECEDING, SQL_FOLLOWING) == NULL)
			return NULL;
	}

	if (!pe || !oe)
		return NULL;

	if (!supports_frames) {
		append(fargs, pe);
		append(fargs, oe);
	}

	types = exp_types(sql->sa, fargs);
	wf = bind_func_(sql, s, aname, types, F_ANALYTIC);
	if (!wf) {
		wf = find_func(sql, s, aname, list_length(types), F_ANALYTIC, NULL);
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
			else {
				char *arg_list = nfargs ? window_function_arg_types_2str(types, nfargs) : NULL;
				sql_error(sql, 02, SQLSTATE(42000) "SELECT: window function '%s(%s)' not found", aname, arg_list ? arg_list : "");
				_DELETE(arg_list);
				return NULL;
			}
		} else {
			char *arg_list = nfargs ? window_function_arg_types_2str(types, nfargs) : NULL;
			sql_error(sql, 02, SQLSTATE(42000) "SELECT: window function '%s(%s)' not found", aname, arg_list ? arg_list : "");
			_DELETE(arg_list);
			return NULL;
		}
	}
	args = sa_list(sql->sa);
	for(node *nn = fargs->h ; nn ; nn = nn->next)
		append(args, (sql_exp*) nn->data);
	if (supports_frames) {
		append(args, start);
		append(args, eend);
	}
	call = exp_rank_op(sql->sa, args, gbe, obe, wf);
	*rel = p;
	return call;
}

sql_exp *
rel_value_exp2(sql_query *query, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	mvc *sql = query->sql;
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (rel && *rel && (*rel)->card <= CARD_AGGR) { /* group by expression case, handle it before */
		sql_exp *exp = stack_get_groupby_expression(sql, se);
		if (sql->errstr[0] != '\0')
			return NULL;
		if (exp) {
			sql_exp *res;

			if (!exp_name(exp))
				exp_label(sql->sa, exp, ++sql->label);
			res  = exp_ref(sql->sa, exp);
			res->card = (*rel)->card;
			if (se->token == SQL_AGGR) {
				dlist *l = se->data.lval;
				int distinct = l->h->next->data.i_val;
				if (distinct)
					set_distinct(res);
			}
			if (!query_has_outer(query) && is_groupby((*rel)->op)) 
				res = rel_groupby_add_aggr(sql, *rel, res);
			return res;
		}
	}

	switch (se->token) {
	case SQL_OP:
		return rel_op(query, rel, se, f, ek);
	case SQL_UNOP:
		return rel_unop(query, rel, se, f, ek);
	case SQL_BINOP:
		return rel_binop(query, rel, se, f, ek);
	case SQL_NOP:
		return rel_nop(query, rel, se, f, ek);
	case SQL_AGGR:
		return rel_aggr(query, rel, se, f);
	case SQL_WINDOW:
		return rel_rankop(query, rel, se, f);
	case SQL_IDENT:
	case SQL_COLUMN:
		return rel_column_ref(query, rel, se, f );
	case SQL_NAME:
		return rel_var_ref(sql, se->data.sval, 1);
	case SQL_VALUES:
	case SQL_WITH: 
	case SQL_SELECT: {
		sql_rel *r = NULL;

		if (se->token == SQL_WITH) {
			r = rel_with_query(query, se);
		} else if (se->token == SQL_VALUES) {
			r = rel_values(query, se);
		} else {
			assert(se->token == SQL_SELECT);
			if (rel && *rel)
				query_push_outer(query, *rel, f);
			r = rel_subquery(query, NULL, se, ek);
			if (rel && *rel)
				*rel = query_pop_outer(query);
		}
		if (!r)
			return NULL;
		if (ek.card <= card_set && is_project(r->op) && list_length(r->exps) > 1) 
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: subquery must return only one column");
		if (list_length(r->exps) == 1) { /* for now don't rename multi attribute results */
			sql_exp *e = lastexp(r);
			if (!has_label(e))
				exp_label(sql->sa, e, ++sql->label);
			if (ek.card < card_set && r->card > CARD_ATOM) {
				sql_subtype *t = exp_subtype(e); /* parameters don't have a type defined, for those use 'void' one */
				sql_subfunc *zero_or_one = sql_bind_func(sql->sa, sql->session->schema, "zero_or_one", t ? t : sql_bind_localtype("void"), NULL, F_AGGR);

				e = exp_ref(sql->sa, e);
				e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, has_nil(e));
				r = rel_groupby(sql, r, NULL);
				(void)rel_groupby_add_aggr(sql, r, e);
			}
		}
		return exp_rel(sql, r);
	}
	case SQL_TABLE: {
		/* turn a subquery into a tabular result */
		*rel = rel_selects(query, se->data.sym);
		if (*rel)
			return lastexp(*rel);
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
	case SQL_RANK:
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: window function %s requires an OVER clause", qname_fname(se->data.lval->h->data.lval));
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
	sql_exp *e;
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(query->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	e = rel_value_exp2(query, rel, se, f, ek);
	if (e && (se->token == SQL_SELECT || se->token == SQL_TABLE) && !exp_is_rel(e)) {
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
		sql_rel *project = *rel, *groupby = NULL;

		/* if there's a group by relation in the tree, skip it for the '*' case and use the underlying projection */
		if (project) {
			while (is_groupby(project->op) || is_select(project->op)) {
				if (is_groupby(project->op))
					groupby = project;
				if (project->l)
					project = project->l;
			}
			assert(project);
		}

		if ((exps = rel_table_projections(sql, project, tname, 0)) != NULL && !list_empty(exps)) {
			if (groupby) {
				groupby->exps = list_distinct(list_merge(groupby->exps, exps, (fdup) NULL), (fcmp) exp_equal, (fdup) NULL);
				for (node *n = groupby->exps->h ; n ; n = n->next) {
					sql_exp *e = n->data;

					if (e->card > groupby->card) {
						if (exp_name(e))
							return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(e));
						else
							return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
					}
				}
			}

			return exps;
		}
		if (!tname)
			return sql_error(sql, 02, SQLSTATE(42000) "Table expression without table name");
		return sql_error(sql, 02, SQLSTATE(42000) "Column expression Table '%s' unknown", tname);
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

static sql_rel*
rel_where_groupby_nodes(sql_query *query, sql_rel *rel, SelectNode *sn, int *group_totals)
{
	mvc *sql = query->sql;

	if (sn->where) {
		rel = rel_logical_exp(query, rel, sn->where, sql_where);
		if (!rel) {
			if (sql->errstr[0] == 0)
				return sql_error(sql, 02, SQLSTATE(42000) "Subquery result missing");
			return NULL;
		}
	}

	if (rel && sn->groupby) {
		list *gbe, *sets = NULL;
		for (dnode *o = sn->groupby->data.lval->h; o ; o = o->next) {
			symbol *grouping = o->data.sym;
			if (grouping->token == SQL_ROLLUP || grouping->token == SQL_CUBE || grouping->token == SQL_GROUPING_SETS) {
				*group_totals |= sql_group_totals;
				break;
			}
		}
		gbe = rel_groupings(query, &rel, sn->groupby, sn->selection, sql_sel | sql_groupby | *group_totals, false, &sets);
		if (!gbe)
			return NULL;
		rel = rel_groupby(sql, rel, gbe);
		if (sets && list_length(sets) > 1) { /* if there is only one combination, there is no reason to generate unions */
			prop *p = prop_create(sql->sa, PROP_GROUPINGS, rel->p);
			p->value = sets;
			rel->p = p;
		}
	}

	if (rel && sn->having) {
		/* having implies group by, ie if not supplied do a group by */
		if (rel->op != op_groupby)
			rel = rel_groupby(sql, rel, NULL);
	}

	return rel;
}

static sql_rel*
rel_having_limits_nodes(sql_query *query, sql_rel *rel, SelectNode *sn, exp_kind ek, int group_totals)
{
	mvc *sql = query->sql;

	if (sn->having) {
		sql_rel *inner = NULL;
		int single_value = 1;

		if (is_project(rel->op) && rel->l) {
			inner = rel->l;
			single_value = 0;
		}
	
		if (inner && inner->op == op_groupby)
			set_processed(inner);
		inner = rel_logical_exp(query, inner, sn->having, sql_having | group_totals);

		if (!inner)
			return NULL;
		if (inner->exps && exps_card(inner->exps) > CARD_AGGR)
			return sql_error(sql, 02, SQLSTATE(42000) "SELECT: cannot compare sets with values, probably an aggregate function missing");
		if (!single_value)
			rel->l = inner;
	}

	if (rel && sn->distinct)
		rel = rel_distinct(rel);

	if (rel && sn->orderby) {
		list *obe = NULL;
		sql_rel *sel = NULL, *l = rel->l;

		/* project( select ) */
		if (sn->having && is_select(l->op)) {
			sel = l;
			rel->l = l->l;
		}
		rel = rel_orderby(sql, rel);
		set_processed(rel);
		obe = rel_order_by(query, &rel, sn->orderby, sql_orderby | group_totals);
		if (!obe)
			return NULL;
		rel->r = obe;
		if (sel) {
			sql_rel *o = rel, *p = o->l;
			p->l = sel;
		}
	}
	if (!rel)
		return NULL;

	if (sn->limit || sn->offset) {
		sql_subtype *lng = sql_bind_localtype("lng");
		list *exps = new_exp_list(sql->sa);

		if (sn->limit) {
			sql_exp *l = rel_value_exp(query, NULL, sn->limit, 0, ek);

			if (!l || !(l=rel_check_type(sql, lng, NULL, l, type_equal)))
				return NULL;
			if ((ek.card != card_relation && sn->limit) &&
				(ek.card == card_value && sn->limit)) {
				sql_subfunc *zero_or_one = sql_bind_func(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(l), NULL, F_AGGR);
				l = exp_aggr1(sql->sa, l, zero_or_one, 0, 0, CARD_ATOM, has_nil(l));
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

	if (sn->sample || sn->seed) {
		list *exps = new_exp_list(sql->sa);

		if (sn->sample) {
			sql_exp *s = rel_value_exp(query, NULL, sn->sample, 0, ek);
			if (!s)
				return NULL;
			if (!exp_subtype(s) && rel_set_type_param(sql, sql_bind_localtype("lng"), NULL, s, 0) < 0)
				return NULL;
			append(exps, s);
		} else if (sn->seed)
			return sql_error(sql, 02, SQLSTATE(42000) "SEED: cannot have SEED without SAMPLE");
		else
			append(exps, NULL);
		if (sn->seed) {
			sql_exp *e = rel_value_exp(query, NULL, sn->seed, 0, ek);
			if (!e || !(e=rel_check_type(sql, sql_bind_localtype("int"), NULL, e, type_equal)))
				return NULL;
			append(exps, e);
		}
		rel = rel_sample(sql->sa, rel, exps);
	}

	if (rel)
		set_processed(rel);
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

	nme = number2name(name, sizeof(name), nr);
	if (!exps)
		return NULL;
	for (n = exps->h; n; n = n->next) {
		sql_exp *le = n->data;
		const char *nm = exp_name(le);
		sql_exp *re = exps_bind_column(r_exps, nm, NULL, 0);

		if (re) {
			found = 1;
			rel = rel_compare_exp(query, rel, le, re, "=", NULL, TRUE, 0, 0);
			if (full) {
				sql_exp *cond = rel_unop_(sql, rel, le, NULL, "isnull", card_value);
				set_has_no_nil(cond);
				le = rel_nop_(sql, rel, cond, re, le, NULL, NULL, "ifthenelse", card_value);
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
	sql_rel *inner = NULL;
	int group_totals = 0;
	list *pexps = NULL;

	assert(sn->s.token == SQL_SELECT);
	if (!sn->selection)
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: the selection or from part is missing");

	if (!rel)
		rel = rel_project(sql->sa, NULL, append(new_exp_list(sql->sa), exp_atom_bool(sql->sa, 1)));
	rel = rel_where_groupby_nodes(query, rel, sn, &group_totals);
	if (sql->session->status) /* rel might be NULL as input, so we have to check for the session status for errors */
		return NULL;

	inner = rel;
	pexps = sa_list(sql->sa);
	for (dnode *n = sn->selection->h; n; n = n->next) {
		/* Here we could get real column expressions
		 * (including single atoms) but also table results.
		 * Therefor we try both rel_column_exp
		 * and rel_table_exp.
		 */
		list *te = NULL;
		sql_exp *ce = rel_column_exp(query, &inner, n->data.sym, sql_sel | group_totals);

		if (ce && (exp_subtype(ce) || (ce->type == e_atom && !ce->l && !ce->f))) { /* Allow parameters to be propagated */
			pexps = append(pexps, ce);
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
		pexps = list_merge(pexps, te, (fdup)NULL);
	}
	if (rel && is_groupby(rel->op) && !sn->groupby) {
		for (node *n=pexps->h; n; n = n->next) {
			sql_exp *ce = n->data;
			if (rel->card < ce->card) {
				if (exp_name(ce)) {
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", exp_name(ce));
				} else {
					return sql_error(sql, ERR_GROUPBY, SQLSTATE(42000) "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
		}
	}
	rel = rel_project(sql->sa, rel, pexps);

	rel = rel_having_limits_nodes(query, rel, sn, ek, group_totals);
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

		if (exp_relname(e)) { 
			if (exp_name(e) && exps_bind_column2(l, exp_relname(e), exp_name(e))) 
				exp_label(sql->sa, e, ++sql->label);
		} else {
			if (exp_name(e) && exps_bind_column(l, exp_name(e), NULL, 0)) 
				exp_label(sql->sa, e, ++sql->label);
		}
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
			if (stack_get_window_def(sql, name, NULL)) {
				return sql_error(sql, 01, SQLSTATE(42000) "SELECT: Redefinition of window '%s'", name);
			} else if (!stack_push_window_def(sql, name, wdef)) {
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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

				query_push_outer(query, res, sql_from);
				fnd = table_ref(query, NULL, n->data.sym, lateral);
				res = query_pop_outer(query);
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
	} else if (toplevel || !res) /* only on top level query */
		return rel_select_exp(query, rel, sn, ek);

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
rel_setquery(sql_query *query, symbol *q)
{
	mvc *sql = query->sql;
	sql_rel *res = NULL;
	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int distinct = n->next->data.i_val;
	dlist *corresponding = n->next->next->data.lval;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	sql_rel *t1, *t2; 

	assert(n->next->type == type_int);
	t1 = table_ref(query, NULL, tab_ref1, 0);
	if (!t1)
		return NULL;
	t2 = table_ref(query, NULL, tab_ref2, 0);
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
		if (t1 && distinct)
			t1 = rel_distinct(t1);
		if (t2 && distinct)
			t2 = rel_distinct(t2);
		res = rel_setquery_(query, t1, t2, corresponding, op_union );
	}
	if ( q->token == SQL_EXCEPT)
		res = rel_setquery_(query, t1, t2, corresponding, op_except );
	if ( q->token == SQL_INTERSECT)
		res = rel_setquery_(query, t1, t2, corresponding, op_inter );
	if (res && distinct)
		res = rel_distinct(res);
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

			query_push_outer(query, t1, sql_from);
			t2 = table_ref(query, NULL, tab2, 0);
			t1 = query_pop_outer(query);
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
		rel = rel_logical_exp(query, rel, js, sql_where | sql_join);
	} else if (js) {	/* using */
		char rname[16], *rnme;
		dnode *n = js->data.lval->h;
		list *outexps = new_exp_list(sql->sa), *exps;
		node *m;

		rnme = number2name(rname, sizeof(rname), ++sql->label);
		for (; n; n = n->next) {
			char *nm = n->data.sval;
			sql_exp *cond;
			sql_exp *ls = rel_bind_column(sql, t1, nm, sql_where, 0);
			sql_exp *rs = rel_bind_column(sql, t2, nm, sql_where, 0);

			if (!ls || !rs) {
				sql_error(sql, 02, SQLSTATE(42000) "JOIN: tables '%s' and '%s' do not have a matching column '%s'\n", rel_name(t1)?rel_name(t1):"", rel_name(t2)?rel_name(t2):"", nm);
				rel_destroy(rel);
				return NULL;
			}
			rel = rel_compare_exp(query, rel, ls, rs, "=", NULL, TRUE, 0, 0);
			if (op != op_join) {
				cond = rel_unop_(sql, rel, ls, NULL, "isnull", card_value);
				set_has_no_nil(cond);
				if (rel_convert_types(sql, t1, t2, &ls, &rs, 1, type_equal) < 0)
					return NULL;
				ls = rel_nop_(sql, rel, cond, rs, ls, NULL, NULL, "ifthenelse", card_value);
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
		sql_exp *rc = rel_bind_column(sql, rv, exp_name(le), sql_where, 0);
			
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
	mvc *sql = query->sql;
	int toplevel = 0;

	if (!stack_push_frame(sql, "SELECT"))
		return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);

	if (!rel || (rel->op == op_project &&
		(!rel->exps || list_length(rel->exps) == 0)))
		toplevel = 1;

	rel = rel_query(query, rel, sq, toplevel, ek);
	stack_pop_frame(sql);
	return rel;
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

		if (sn->into) {
			sql->type = Q_SCHEMA;
			ret = rel_select_with_into(query, s);
		} else {
			ret = rel_subquery(query, NULL, s, ek);
			sql->type = Q_TABLE;
		}
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
		ret = rel_setquery(query, s);
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
	if (l->next)
		l = l->next; /* skip distinct */
	if (l->next) { /* table call with subquery */
		if (l->next->type == type_symbol && l->next->data.sym->token == SQL_SELECT) {
			if (l->next->next != NULL)
				return sql_error(sql, 02, SQLSTATE(42000) "SELECT: '%s' requires a single sub query", fname);
			if (!(sq = rel_subquery(query, NULL, l->next->data.sym, ek)))
				return NULL;
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

	if (sq) {
		for (node *n = sq->exps->h, *m = sf->func->ops->h ; n && m ; n = n->next, m = m->next) {
			sql_exp *e = (sql_exp*) n->data;
			sql_arg *a = (sql_arg*) m->data;
			if (!exp_subtype(e) && rel_set_type_param(sql, &(a->type), sq, e, 0) < 0)
				return NULL;
		}
	}

	if (loader_function)
		*loader_function = sf;

	return rel_table_func(sql->sa, sq, e, fexps, (sq)?TABLE_FROM_RELATION:TABLE_PROD_FUNC);
}
