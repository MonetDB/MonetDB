/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2017 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_select.h"
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
#include "rel_remote.h"
#include "rel_sequence.h"
#ifdef HAVE_HGE
#include "mal.h"		/* for have_hge */
#endif

#define VALUE_FUNC(f) (f->func->type == F_FUNC || f->func->type == F_FILT)
#define check_card(card,f) ((card == card_none && !f->res) || (CARD_VALUE(card) && f->res && VALUE_FUNC(f)) || card == card_loader || (card == card_relation && f->func->type == F_UNION))

static void
rel_setsubquery(sql_rel*r)
{
	if (rel_is_ref(r))
		return;
	if (r->l && !is_base(r->op))
		rel_setsubquery(r->l);
	if (r->r && is_join(r->op))
		rel_setsubquery(r->r);
	set_subquery(r);
}

/* return all expressions, with table name == tname */
static list *
rel_table_projections( mvc *sql, sql_rel *rel, char *tname, int level )
{
	list *exps;

	if (!rel)
		return NULL;

	if (!tname) {
		if (is_project(rel->op) && rel->l)
			return rel_projections(sql, rel->l, NULL, 1, 0);
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
	case op_apply:
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
				if (!is_intern(e) && e->rname && strcmp(e->rname, tname) == 0)
					append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
				if (!is_intern(e) && !e->rname && e->l && strcmp(e->l, tname) == 0)
					append(exps, exp_alias_or_copy(sql, tname, exp_name(e), rel, e));
			}
			if (exps && list_length(exps))
				return exps;
		}
	default:
		return NULL;
	}
}

static sql_rel*
rel_project_exp(sql_allocator *sa, sql_exp *e)
{
	sql_rel *rel = rel_project(sa, NULL, append(new_exp_list(sa), e));

	set_processed(rel);
	return rel;
}

static sql_rel*
rel_parent( sql_rel *rel )
{
	if (is_project(rel->op) || rel->op == op_topn || rel->op == op_sample) {
		sql_rel *l = rel->l;
		if (is_project(l->op))
			return l;
	}
	if (is_apply(rel->op))
		return rel->r;
	return rel;
}

static sql_exp *
rel_lastexp(mvc *sql, sql_rel *rel )
{
	sql_exp *e;

	if (!is_processed(rel))
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
	return exp_column(sql->sa, e->rname, e->name, exp_subtype(e), e->card, has_nil(e), is_intern(e));
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

		g->card = CARD_ATOM; /* no groupby expressions */
		g->op = op_groupby;
		g->r = new_exp_list(sql->sa); /* add empty groupby column list */
		
		for (en = g->exps->h; en; en = en->next) {
			sql_exp *e = en->data;

			if (e->card > g->card) {
				if (e->type == e_column && e->r) {
					return sql_error(sql, 02, "cannot use non GROUP BY column '%s' in query results without an aggregate function", (char *) e->r);
				} else {
					return sql_error(sql, 02, "cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
		}
		return rel_project(sql->sa, g, rel_projections(sql, g, NULL, 1, 1));
	}
	return NULL;
}

static sql_rel *
rel_orderby(mvc *sql, sql_rel *l)
{
	sql_rel *rel = rel_create(sql->sa);

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
static sql_rel * rel_setquery(mvc *sql, sql_rel *rel, symbol *sq);
static sql_rel * rel_joinquery(mvc *sql, sql_rel *rel, symbol *sq);
static sql_rel * rel_crossquery(mvc *sql, sql_rel *rel, symbol *q);
static sql_rel * rel_unionjoinquery(mvc *sql, sql_rel *rel, symbol *sq);

static sql_rel *
rel_table_optname(mvc *sql, sql_rel *sq, symbol *optname)
{
	if (optname && optname->token == SQL_NAME) {
		dlist *columnrefs = NULL;
		char *tname = optname->data.lval->h->data.sval;
		list *l = sa_list(sql->sa);

		columnrefs = optname->data.lval->h->next->data.lval;
		if (columnrefs && sq->exps) {
			dnode *d = columnrefs->h;
			node *ne = sq->exps->h;

			MT_lock_set(&sq->exps->ht_lock);
			sq->exps->ht = NULL;
			MT_lock_unset(&sq->exps->ht_lock);
			for (; d && ne; d = d->next, ne = ne->next) {
				sql_exp *e = ne->data;

				if (exps_bind_column2(l, tname, d->data.sval))
					return sql_error(sql, ERR_AMBIGUOUS, "SELECT: Duplicate column name '%s.%s'", tname, d->data.sval);
				exp_setname(sql->sa, e, tname, d->data.sval );
				append(l, e);
			}
		}
		if (!columnrefs && sq->exps) {
			node *ne = sq->exps->h;

			for (; ne; ne = ne->next) {
				sql_exp *e = ne->data;

				if (exp_name(e) && exps_bind_column2(l, tname, exp_name(e)))
					return sql_error(sql, ERR_AMBIGUOUS, "SELECT: Duplicate column name '%s.%s'", tname, exp_name(e));
				noninternexp_setname(sql->sa, e, tname, NULL );
				append(l, e);
			}
		}
	}
	return sq;
}

static sql_rel *
rel_subquery_optname(mvc *sql, sql_rel *rel, symbol *query)
{
	SelectNode *sn = (SelectNode *) query;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = rel_subquery(sql, rel, query, ek, APPLY_JOIN);

	assert(query->token == SQL_SELECT);
	if (!sq)
		return NULL;

	return rel_table_optname(sql, sq, sn->name);
}

sql_rel *
rel_with_query(mvc *sql, symbol *q ) 
{
	dnode *d = q->data.lval->h;
	symbol *select = d->next->data.sym;
	sql_rel *rel;

	stack_push_frame(sql, "WITH");
	/* first handle all with's (ie inlined views) */
	for (d = d->data.lval->h; d; d = d->next) {
		symbol *sym = d->data.sym;
		dnode *dn = sym->data.lval->h;
		char *name = qname_table(dn->data.lval);
		sql_rel *nrel;

		if (frame_find_var(sql, name)) {
			return sql_error(sql, 01, "Variable '%s' already declared", name);
		}
		nrel = rel_semantic(sql, sym);
		if (!nrel) {  
			stack_pop_frame(sql);
			return NULL;
		}
		stack_push_rel_view(sql, name, nrel);
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

			for (; ne; ne = ne->next) 
				noninternexp_setname(sql->sa, ne->data, name, NULL );
		}
	}
	rel = rel_semantic(sql, select);
	stack_pop_frame(sql);
	return rel;
}

static sql_rel *
query_exp_optname(mvc *sql, sql_rel *r, symbol *q)
{
	switch (q->token) {
	case SQL_WITH:
	{
		sql_rel *tq = rel_with_query(sql, q);

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
		sql_rel *tq = rel_setquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_JOIN:
	{
		sql_rel *tq = rel_joinquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_CROSS:
	{
		sql_rel *tq = rel_crossquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	case SQL_UNIONJOIN:
	{
		sql_rel *tq = rel_unionjoinquery(sql, r, q);

		if (!tq)
			return NULL;
		return rel_table_optname(sql, tq, q->data.lval->t->data.sym);
	}
	default:
		(void) sql_error(sql, 02, "case %d %s\n", q->token, token2string(q->token));
	}
	return NULL;
}

static sql_subfunc *
bind_func_(mvc *sql, sql_schema *s, char *fname, list *ops, int type )
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
bind_func(mvc *sql, sql_schema *s, char *fname, sql_subtype *t1, sql_subtype *t2, int type )
{
	sql_subfunc *sf = NULL;

	assert(t1);
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
find_func(mvc *sql, sql_schema *s, char *fname, int len, int type, sql_subfunc *prev )
{
	sql_subfunc *sf = NULL;

	if (sql->forward && strcmp(fname, sql->forward->base.name) == 0 && list_length(sql->forward->ops) == len && execute_priv(sql, sql->forward)) 
		return sql_dup_subfunc(sql->sa, sql->forward, NULL, NULL);
	sf = sql_find_func(sql->sa, s, fname, len, type, prev);
	if (sf && execute_priv(sql, sf->func))
		return sf;
	return NULL;
}

static sql_exp *
find_table_function(mvc *sql, sql_schema *s, char *fname, list *exps, list *tl)
{
	sql_exp *e = NULL;
	sql_subfunc * sf = bind_func_(sql, s, fname, tl, F_UNION);

	if (sf) {
		e = exp_op(sql->sa, exps, sf);
	} else if (list_length(tl)) { 
		sql_subfunc * prev = NULL;

		while(!e && (sf = bind_member_func(sql, s, fname, tl->h->data, list_length(tl), prev)) != NULL) {
			node *n, *m;
			list *nexps;

			prev = sf;
			if (sf->func->vararg) {
				e = exp_op(sql->sa, exps, sf);
			} else {
	       			nexps = new_exp_list(sql->sa);
				for (n = exps->h, m = sf->func->ops->h; n && m; n = n->next, m = m->next) {
					sql_arg *a = m->data;
					sql_exp *e = n->data;

					if (a->type.type->eclass == EC_ANY) {
						sql_subtype *st = &e->tpe;
						sql_init_subtype(&a->type, st->type, st->digits, st->scale);
					}
					e = rel_check_type(sql, &a->type, e, type_equal);
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
					e = exp_op(sql->sa, nexps, sf);
			}
		}
		prev = NULL;
		while(!e && (sf = find_func(sql, s, fname, list_length(tl), F_UNION, prev)) != NULL) {
			node *n, *m;
			list *nexps;

			prev = sf;
			if (sf->func->vararg) {
				e = exp_op(sql->sa, exps, sf);
			} else {
       				nexps = new_exp_list(sql->sa);
				for (n = exps->h, m = sf->func->ops->h; n && m; n = n->next, m = m->next) {
					sql_arg *a = m->data;
					sql_exp *e = n->data;

					if (a->type.type->eclass == EC_ANY) {
						sql_subtype *st = &e->tpe;
						sql_init_subtype(&a->type, st->type, st->digits, st->scale);
					}
					e = rel_check_type(sql, &a->type, e, type_equal);
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
					e = exp_op(sql->sa, nexps, sf);
			}
		}
	}
	return e;
}

static sql_rel *
rel_named_table_function(mvc *sql, sql_rel *rel, symbol *query, int lateral)
{
	list *exps = NULL, *tl;
	node *m;
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = NULL, *outer = NULL;
	sql_exp *e = NULL;
	sql_subfunc *sf = NULL;
	symbol *sym = query->data.lval->h->data.sym;
	dnode *l = sym->data.lval->h;
	char *tname = NULL;
	char *fname = qname_fname(l->data.lval); 
	char *sname = qname_schema(l->data.lval);
	node *en;
	sql_schema *s = sql->session->schema;
	sql_subtype *oid = sql_bind_localtype("oid");
		
	tl = sa_list(sql->sa);
	exps = new_exp_list(sql->sa);
	if (l->next) { /* table call with subquery */
		if (l->next->type == type_symbol && l->next->data.sym->token == SQL_SELECT) {
			if (l->next->next != NULL)
				return sql_error(sql, 02, "SELECT: '%s' requires a single sub query", fname);
	       		sq = rel_subquery(sql, NULL, l->next->data.sym, ek, 0 /*apply*/);
		} else if (l->next->type == type_symbol || l->next->type == type_list) {
			dnode *n;
			exp_kind iek = {type_value, card_column, TRUE};
			list *exps = sa_list (sql->sa);

			if (lateral && rel)
				outer = rel = rel_dup(rel);

			if (l->next->type == type_symbol)
				n = l->next;
			else 
				n = l->next->data.lval->h;
			for ( ; n; n = n->next) {
				sql_exp *e = rel_value_exp(sql, lateral?&rel:NULL, n->data.sym, sql_sel, iek);

				if (!e)
					return NULL;
				append(exps, e);
			}
			sq = rel_project(sql->sa, lateral?rel:NULL, exps);
		}

		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		if (!sq || (!lateral && rel))
			return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
		for (en = sq->exps->h; en; en = en->next) {
			sql_exp *e = en->data;

			append(exps, e=exp_alias_or_copy(sql, tname, exp_name(e), NULL, e));
			append(tl, exp_subtype(e));
		}
		if (lateral && outer) {
			sql_exp *tid = rel_find_column(sql->sa, outer, rel_name(outer), TID);
			append(exps, tid);
			append(tl, exp_subtype(tid));
		}
	}
		
	if (sname)
		s = mvc_bind_schema(sql, sname);
	e = find_table_function(sql, s, fname, exps, tl);
	if (!e && lateral && outer) { /* no lateral version, create wrapper */
		sql_exp *tid = rel_find_column(sql->sa, outer, rel_name(outer), TID);
		list_remove_data(exps, exps->t->data);
		list_remove_data(tl, tl->t->data);

		e = find_table_function(sql, s, fname, exps, tl);
		append(tl, exp_subtype(tid));
		if (e) { /* create internal function which calls the union function per value,
			    returns result of union function with tid */
			int nr = 0;
			list *args = NULL, *nexps, *res; /* list of arguments, named A1 to AN? */
			sql_func *f = NULL;
			sql_exp *ie, *ae = NULL;
			sql_arg *a;
			node *n;
			char *nfname = sa_strdup(sql->sa, fname);
				
			nfname[0] = 'L';
			args = sa_list(sql->sa);
			nexps = sa_list(sql->sa);
			sf = e->f;
			res = list_dup(sf->func->res, NULL);
			a = SA_ZNEW(sql->sa, sql_arg);
			a->name = sa_strdup(sql->sa, TID);
			a->type = *exp_subtype(tid);
			append(res, a);
			if (!args || !nexps)
				return NULL;
			for(n = tl->h, m = exps->h, nr = 0; n; n = n->next, nr++, m = (m)?m->next:NULL) {
				sql_subtype *tpe = n->data;
				sql_exp *e = NULL;

				a = SA_ZNEW(sql->sa, sql_arg);
				if (m)
			       		e = m->data;
				a->name = sa_strdup(sql->sa, (e)?exp_name(e):TID);
				a->type = *tpe;
				append(args, a);

				ae = exp_param(sql->sa, a->name, &a->type, 0);
				if (n->next)
					append(nexps, ae);
			}
			f = mvc_create_func(sql, sql->sa, s, nfname, args, res, F_UNION, FUNC_LANG_SQL, "user", "intern", "intern", FALSE, sf->func->vararg);
			/* call normal table function */
			ie = exp_op(sql->sa, nexps, sf);
			nexps = sa_list(sql->sa);
			/* project the result column, add table.column name */
			res = new_exp_list(sql->sa);
			if (!ie || !ae || !nexps || !res) 
				return NULL;
			for (m = sf->func->res->h; m; m = m->next) {
				sql_exp *e;
				sql_arg *a = m->data;

				append(res, e = exp_column(sql->sa, tname, a->name, &a->type, CARD_MULTI, 1, 0));
				append(nexps, e);
			}
			append(sq->exps, tid);
			rel = rel_table_func(sql->sa, sq, ie, res, (sq != NULL));
			append(nexps, ae);
			f->rel = rel_project(sql->sa, rel, nexps);

			/* create sub function for lateral version of the table function */ 
			sf = sql_dup_subfunc(sql->sa, f, tl, NULL);
			if (!f->rel || !rel || !sf)
				return NULL;
		}
		exps = list_dup(exps, NULL);
		append(exps, tid);
		e = exp_op(sql->sa, exps, sf);
	}
	if (!e)
		return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
	rel = sq;

	if (query->data.lval->h->next->data.sym)
		tname = query->data.lval->h->next->data.sym->data.lval->h->data.sval;
	else
		tname = make_label(sql->sa, ++sql->label);

	/* column or table function */
	sf = e->f;
	if (e->type != e_func || sf->func->type != F_UNION) {
		(void) sql_error(sql, 02, "SELECT: '%s' does not return a table", exp_func_name(e));
		return NULL;
	}

	/* for each column add table.column name */
	exps = new_exp_list(sql->sa);
	for (m = sf->func->res->h; m; m = m->next) {
		sql_arg *a = m->data;

		append(exps, exp_column(sql->sa, tname, a->name, &a->type, CARD_MULTI, 1, 0));
	}
	rel = rel_table_func(sql->sa, rel, e, exps, (sq != NULL));
	if (lateral && outer) {
		sql_exp *tid = rel_find_column(sql->sa, outer, rel_name(outer), TID);

		rel = rel_crossproduct(sql->sa, outer, rel, op_join);
		rel->exps = new_exp_list(sql->sa); 
		e = exp_compare(sql->sa, 
				exp_column(sql->sa, exp_relname(tid), exp_name(tid), oid, CARD_MULTI, 0, 1), 
				exp_column(sql->sa, tname, TID, oid, CARD_MULTI, 0, 1), cmp_equal);
		append(rel->exps, e);
	}
	return rel;
}

static sql_exp *
rel_op_(mvc *sql, sql_schema *s, char *fname, exp_kind ek)
{
	sql_subfunc *f = NULL;
	int type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:
		   ((ek.card == card_relation)?F_UNION:F_FUNC));

	f = sql_bind_func(sql->sa, s, fname, NULL, NULL, type);
	if (f && check_card(ek.card, f)) {
		return exp_op(sql->sa, NULL, f);
	} else {
		return sql_error(sql, 02,
			"SELECT: no such operator '%s'", fname);
	}
}

/* special class of table returning function, but with a table input as well */
static sql_rel *
rel_named_table_operator(mvc *sql, sql_rel *rel, symbol *query)
{
	exp_kind ek = {type_value, card_relation, TRUE};
	sql_rel *sq = rel_subquery(sql, rel, query->data.lval->h->data.sym, ek, APPLY_JOIN);
	list *exps;
	node *en;
	char *tname = NULL;

	if (!sq)
		return NULL;
	
	if (query->data.lval->h->next->data.sym) {
		dlist *column_spec = query->data.lval->h->next->data.sym->data.lval->h->next->data.lval;

		tname = query->data.lval->h->next->data.sym->data.lval->h->data.sval;
		if (column_spec) {
			dnode *n = column_spec->h;

			sq = rel_project(sql->sa, sq, rel_projections(sql, sq, NULL, 1, 1));
			set_processed(sq);
			for (en = sq->exps->h; n && en; n = n->next, en = en->next) 
				exp_setname(sql->sa, en->data, tname, n->data.sval );
		}
	}

	exps = new_exp_list(sql->sa);
	for (en = sq->exps->h; en; en = en->next) {
		sql_exp *e = en->data;
		append(exps, exp_column(sql->sa, tname, exp_name(e), exp_subtype(e), CARD_MULTI, has_nil(e), 0));
	}
	return rel_relational_func(sql->sa, sq, exps);
}

static sql_rel *
rel_values( mvc *sql, symbol *tableref)
{
	sql_rel *r = NULL;
	dlist *rowlist = tableref->data.lval;
	symbol *optname = rowlist->t->data.sym;
	dnode *o;
	node *m;
	list *exps = sa_list(sql->sa); 

	exp_kind ek = {type_value, card_value, TRUE};
	if (!rowlist->h)
		r = rel_project(sql->sa, NULL, NULL);

	/* last element in the list is the table_name */
	for (o = rowlist->h; o->next; o = o->next) {
		dlist *values = o->data.lval;

		if (r && list_length(r->exps) != dlist_length(values)) {
			return sql_error(sql, 02, "VALUES: number of values doesn't match");
		} else {
			dnode *n;

			if (list_empty(exps)) {
				for (n = values->h; n; n = n->next) {
					sql_exp *vals = exp_values(sql->sa, sa_list(sql->sa));

					exp_label(sql->sa, vals, ++sql->label);
					list_append(exps, vals);
				}
			}
			for (n = values->h, m = exps->h; n && m; 
					n = n->next, m = m->next) {
				sql_exp *vals = m->data;
				list *vals_list = vals->f;
				sql_exp *e = rel_value_exp(sql, NULL, n->data.sym, sql_sel, ek);
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
		for (n = vals_list->h; n; n = n->next) {
			sql_exp *e = n->data;
			
			e = rel_check_type(sql, &vals->tpe, e, type_equal);
			if (!e)
				return NULL;
			append(nexps, e); 
		}
		vals->f = nexps;
	}
	r = rel_project(sql->sa, NULL, exps);
	set_processed(r);
	rel_table_optname(sql, r, optname);
	return r;
}

static int
check_is_lateral(symbol *tableref) 
{
	if (tableref->token == SQL_NAME ||
	    tableref->token == SQL_TABLE ||
	    tableref->token == SQL_TABLE_OPERATOR) {
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
table_ref(mvc *sql, sql_rel *rel, symbol *tableref, int lateral)
{
	char *tname = NULL;
	sql_table *t = NULL;

	if (tableref->token == SQL_NAME) {
		dlist *name = tableref->data.lval->h->data.lval;
		sql_rel *temp_table = NULL;
		char *sname = qname_schema(name);
		sql_schema *s = NULL;

		tname = qname_table(name);

		if (dlist_length(name) > 2)
			return sql_error(sql, 02, "3F000!SELECT: only a schema and table name expected");

		if (sname && !(s=mvc_bind_schema(sql,sname)))
			return sql_error(sql, 02, "3F000!SELECT: no such schema '%s'", sname);
		if (!t && !sname) {
			t = stack_find_table(sql, tname);
			if (!t && sql->use_views) 
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
			return sql_error(sql, 02, "42S02!SELECT: no such table '%s'", tname);
		} else if (!temp_table && !table_privs(sql, t, PRIV_SELECT)) {
			return sql_error(sql, 02, "SELECT: access denied for %s to table '%s.%s'", stack_get_string(sql, "current_user"), s->base.name, tname);
		}
		if (tableref->data.lval->h->next->data.sym) {	/* AS */
			tname = tableref->data.lval->h->next->data.sym->data.lval->h->data.sval;
		}
		if (temp_table && !t) {
			node *n;
			list *exps = rel_projections(sql, temp_table, NULL, 1, 1);

			temp_table = rel_project(sql->sa, temp_table, exps);
			set_processed(temp_table);
			for (n = exps->h; n; n = n->next)
				noninternexp_setname(sql->sa, n->data, tname, NULL);
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
				}
			}
			return rel;
		}
		if ((isMergeTable(t) || isReplicaTable(t)) && list_empty(t->tables.set))
			return sql_error(sql, 02, "MERGE or REPLICA TABLE should have at least one table associated");

		return rel_basetable(sql, t, tname);
	} else if (tableref->token == SQL_VALUES) {
		return rel_values(sql, tableref);
	} else if (tableref->token == SQL_TABLE) {
		return rel_named_table_function(sql, rel, tableref, lateral);
	} else if (tableref->token == SQL_TABLE_OPERATOR) {
		return rel_named_table_operator(sql, rel, tableref);
	} else if (tableref->token == SQL_SELECT) {
		return rel_subquery_optname(sql, rel, tableref);
	} else {
		return query_exp_optname(sql, rel, tableref);
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
		return sql_error(sql, 02, "SELECT: '@""%s' unknown", name);
	} else {
		return sql_error(sql, 02, "SELECT: identifier '%s' unknown", name);
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
rel_column_ref(mvc *sql, sql_rel **rel, symbol *column_r, int f)
{
	sql_exp *exp = NULL;
	dlist *l = NULL;

	assert(column_r->token == SQL_COLUMN && column_r->type == type_list);
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
		
		if (rel && *rel)
			exp = rel_bind_column(sql, *rel, name, f);
		if (exp) {
			if (var || a)
				return sql_error(sql, ERR_AMBIGUOUS, "SELECT: identifier '%s' ambiguous", name);
		} else if (a) {
			if (var)
				return sql_error(sql, ERR_AMBIGUOUS, "SELECT: identifier '%s' ambiguous", name);
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
			if (rel && *rel && (*rel)->card == CARD_AGGR && f == sql_sel) {
				sql_rel *gb = *rel;

				while(gb->l && !is_groupby(gb->op))
					gb = gb->l;
				if (gb && gb->l && rel_bind_column(sql, gb->l, name, f)) 
					return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", name);
			}
			return sql_error(sql, 02, "SELECT: identifier '%s' unknown", name);
		}
		
	} else if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *cname = l->h->next->data.sval;

		if (rel && *rel)
			exp = rel_bind_column2(sql, *rel, tname, cname, f);

		/* some views are just in the stack,
		   like before and after updates views */
		if (!exp && sql->use_views) {
			sql_rel *v = stack_find_rel_view(sql, tname);

			if (v) {
				if (*rel)
					*rel = rel_crossproduct(sql->sa, *rel, v, op_join);
				else
					*rel = v;
				exp = rel_bind_column(sql, *rel, cname, f);
			}
		}
		if (!exp) {
			if (rel && *rel && (*rel)->card == CARD_AGGR && f == sql_sel) {
				sql_rel *gb = *rel;

				while(gb->l && !is_groupby(gb->op))
					gb = gb->l;
				if (gb && gb->l && rel_bind_column2(sql, gb->l, tname, cname, f))
					return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s.%s' in query results without an aggregate function", tname, cname);
			}
			return sql_error(sql, 02, "42S22!SELECT: no such column '%s.%s'", tname, cname);
		}
	} else if (dlist_length(l) >= 3) {
		return sql_error(sql, 02, "TODO: column names of level >= 3");
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
rel_set_type_param(mvc *sql, sql_subtype *type, sql_exp *param, int upcast)
{
	if (!type || !param || param->type != e_atom)
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

	if (set_type_param(sql, type, param->flag) == 0) {
		param->tpe = *type;
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
	if (exp->type != e_atom || exp->r /* named */ || exp->f /* list */) 
		return NULL;

	if (exp->l)
		a = exp->l;
	else
		a = sql_bind_arg(sql, exp->flag);

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

		return rel_check_type(sql, dtp, e, type_cast);
	}
	if (tp->type->eclass == EC_NUM) {
#ifdef HAVE_HGE
		sql_subtype *ltp = sql_bind_localtype(have_hge ? "hge" : "lng");
#else
		sql_subtype *ltp = sql_bind_localtype("lng");
#endif

		return rel_check_type(sql, ltp, e, type_cast);
	}
	return e;
}

sql_exp *
rel_check_type(mvc *sql, sql_subtype *t, sql_exp *exp, int tpe)
{
	int err = 0;
	sql_exp* nexp = NULL;
	sql_subtype *fromtype = exp_subtype(exp);
	
	if ((!fromtype || !fromtype->type) && rel_set_type_param(sql, t, exp, 0) == 0)
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
		sql_exp *res = sql_error(
			sql, 03,
			"types %s(%d,%d) and %s(%d,%d) are not equal%s%s%s",
			fromtype->type->sqlname,
			fromtype->digits,
			fromtype->scale,
			t->type->sqlname,
			t->digits,
			t->scale,
			(exp->type == e_column ? " for column '" : ""),
			(exp->type == e_column ? exp->name : ""),
			(exp->type == e_column ? "'" : "")
		);
		return res;
	}
	return exp;
}

static sql_exp *
exp_sum_scales(mvc *sql, sql_subfunc *f, sql_exp *l, sql_exp *r)
{
	sql_arg *ares = f->func->res->h->data;

	if (strcmp(f->func->imp, "*") == 0 && ares->type.type->scale == SCALE_FIX) {
		sql_subtype t;
		sql_subtype *lt = exp_subtype(l);
		sql_subtype *rt = exp_subtype(r);
		sql_subtype *res = f->res->h->data;

		res->scale = lt->scale + rt->scale;
		res->digits = lt->digits + rt->digits;

		/* HACK alert: digits should be less than max */
#ifdef HAVE_HGE
		if (have_hge) {
			if (ares->type.type->radix == 10 && res->digits > 39)
				res->digits = 39;
			if (ares->type.type->radix == 2 && res->digits > 128)
				res->digits = 128;
		} else
#endif
		{

			if (ares->type.type->radix == 10 && res->digits > 19)
				res->digits = 19;
			if (ares->type.type->radix == 2 && res->digits > 64)
				res->digits = 64;
		}

		/* sum of digits may mean we need a bigger result type
		 * as the function don't support this we need to
		 * make bigger input types!
		 */

		/* numeric types are fixed length */
		if (ares->type.type->eclass == EC_NUM) {
			sql_find_numeric(&t, ares->type.type->localtype, res->digits);
		} else {
			sql_find_subtype(&t, ares->type.type->sqlname, res->digits, res->scale);
		}
		if (type_cmp(t.type, ares->type.type) != 0) {
			/* do we need to convert to the a larger localtype
			   int * int may not fit in an int, so we need to
			   convert to lng * int.
			 */
			sql_subtype nlt;

			sql_init_subtype(&nlt, t.type, res->digits, lt->scale);
			l = rel_check_type( sql, &nlt, l, type_equal );
		}
		*res = t;
	}
	return l;
}

static sql_exp *
exp_scale_algebra(mvc *sql, sql_subfunc *f, sql_exp *l, sql_exp *r)
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
		l = rel_check_type( sql, &nlt, l, type_equal );

		sql_find_subtype(res, lt->type->sqlname, digits, scale);
	}
	return l;
}

int
rel_convert_types(mvc *sql, sql_exp **L, sql_exp **R, int scale_fixing, int tpe)
{
	sql_exp *ls = *L;
	sql_exp *rs = *R;
	sql_subtype *lt = exp_subtype(ls);
	sql_subtype *rt = exp_subtype(rs);

	if (!rt && !lt) {
		sql_error(sql, 01, "Cannot have a parameter (?) on both sides of an expression");
		return -1;
	}
	if (rt && (!lt || !lt->type))
		 return rel_set_type_param(sql, rt, ls, 0);
	if (lt && (!rt || !rt->type))
		 return rel_set_type_param(sql, lt, rs, 0);

	if (rt && lt) {
		sql_subtype *i = lt;
		sql_subtype *r = rt;

		if (subtype_cmp(lt, rt) != 0) {
			sql_subtype super;

			supertype(&super, r, i);
			if (scale_fixing) {
				/* convert ls to super type */
				ls = rel_check_type(sql, &super, ls, tpe);
				/* convert rs to super type */
				rs = rel_check_type(sql, &super, rs, tpe);
			} else {
				/* convert ls to super type */
				super.scale = lt->scale;
				ls = rel_check_type(sql, &super, ls, tpe);
				/* convert rs to super type */
				super.scale = rt->scale;
				rs = rel_check_type(sql, &super, rs, tpe);
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

			e = rel_check_type(sql, &a->type, e, type_equal);
			if (!e)
				return NULL;
			list_append(nexps, e);
		}
		l = nexps;
		nexps = sa_list(sql->sa);
		for(n=r->h; m && n; m = m->next, n = n->next) {
			sql_arg *a = m->data;
			sql_exp *e = n->data;

			e = rel_check_type(sql, &a->type, e, type_equal);
			if (!e)
				return NULL;
			list_append(nexps, e);
		}
		r = nexps;
	}
	if (!f) {
		return sql_error(sql, 02, "SELECT: no such FILTER function '%s'", filter_op);
		return NULL;
	}
	e = exp_filter(sql->sa, l, r, f, anti);

	/* atom or row => select */
	if (exps_card(l) > rel->card) {
		sql_exp *ls = l->h->data;
		if (ls->name)
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", ls->name);
		else
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (exps_card(r) > rel->card) {
		sql_exp *rs = l->h->data;
		if (rs->name)
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", rs->name);
		else
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (exps_card(r) <= CARD_ATOM && exps_are_atoms(r)) {
		if (exps_card(l) == exps_card(r) || rel->processed)  /* bin compare op */
			return rel_select(sql->sa, rel, e);

		if (/*is_semi(rel->op) ||*/ is_outerjoin(rel->op)) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		}
		/* push select into the given relation */
		return rel_push_select(sql, rel, L, e);
	} else { /* join */
		if (is_semi(rel->op) || (is_outerjoin(rel->op) && !is_processed(rel))) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		}
		/* push join into the given relation */
		return rel_push_join(sql, rel, L, R, NULL, e);
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
rel_compare_exp_(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs, sql_exp *rs2, int type, int anti )
{
	sql_exp *L = ls, *R = rs, *e = NULL;

	if (!rs2) {

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
			return sql_error(sql, 01, "Cannot have a parameter (?) on both sides of an expression");
		if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0) 
			return NULL;
		e = exp_compare(sql->sa, ls, rs, type);
	} else {
		if ((rs = rel_check_type(sql, exp_subtype(ls), rs, type_equal)) == NULL ||
	   	    (rs2 && (rs2 = rel_check_type(sql, exp_subtype(ls), rs2, type_equal)) == NULL)) 
			return NULL;
		e = exp_compare2(sql->sa, ls, rs, rs2, type);
	}
	if (anti)
		set_anti(e);

	/* atom or row => select */
	if (ls->card > rel->card) {
		if (ls->name)
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", ls->name);
		else
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card > rel->card || (rs2 && rs2->card > rel->card)) {
		if (rs->name)
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", rs->name);
		else
			return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
	}
	if (rs->card <= CARD_ATOM && exp_is_atom(rs) && 
	   (!rs2 || (rs2->card <= CARD_ATOM && exp_is_atom(rs2)))) {
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
		if (is_semi(rel->op) || (is_outerjoin(rel->op) && !is_processed((rel)))) {
			rel_join_add_exp(sql->sa, rel, e);
			return rel;
		}
		/* push join into the given relation */
		return rel_push_join(sql, rel, L, R, rs2, e);
	}
}

static sql_rel *
rel_compare_exp(mvc *sql, sql_rel *rel, sql_exp *ls, sql_exp *rs,
		char *compare_op, sql_exp *esc, int reduce)
{
	comp_type type = cmp_equal;

	if (!ls || !rs)
		return NULL;

	if (!rel || !reduce) {
		/* TODO to handle filters here */
		sql_exp *e;

		if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0) 
			return NULL;
		e = rel_binop_(sql, ls, rs, NULL, compare_op, card_value);

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
	return rel_compare_exp_(sql, rel, ls, rs, esc, type, 0);
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
rel_compare(mvc *sql, sql_rel *rel, symbol *lo, symbol *ro, symbol *ro2,
		char *compare_op, int f, exp_kind k, int quantifier)
{
	sql_exp *rs = NULL, *rs2 = NULL, *ls;
	exp_kind ek = {type_value, card_column, FALSE};

	if (!ro2 && lo->token == SQL_SELECT) { /* swap subquery to the right hand side */
		symbol *tmp = lo;

		lo = ro;
		ro = tmp;

		if (compare_op[0] == '>')
			compare_op[0] = '<';
		else if (compare_op[0] == '<')
			compare_op[0] = '>';
	}

	ls = rel_value_exp(sql, &rel, lo, f, ek);
	if (!ls)
		return NULL;
	if (ro->token != SQL_SELECT) {
		rs = rel_value_exp(sql, &rel, ro, f, ek);
		if (ro2) {
			rs2 = rel_value_exp(sql, &rel, ro2, f, ek);
			if (!rs2)
				return NULL;
		}
	} else {
		/* first try without current relation, too see if there
		   are correlations with the outer relation */
		sql_rel *r = rel_subquery(sql, NULL, ro, ek, APPLY_JOIN);

		/* NOT handled filter case */
		if (ro2)
			return NULL;
		if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = 0;
			r = rel_subquery(sql, rel, ro, ek, f == sql_sel?APPLY_LOJ:APPLY_JOIN);

			/* get inner queries result value, ie
			   get last expression of r */
			if (r) {
				rs = rel_lastexp(sql, r);

				if (f == sql_sel && r->card > CARD_ATOM && quantifier != 1) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, compare_aggr_op(compare_op, quantifier), exp_subtype(rs));
					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);

					/* group by the right of the apply */
					r->r = rel_groupby(sql, r->r, NULL);
					rs = rel_groupby_add_aggr(sql, r->r, rs);
					rs = exp_column(sql->sa, exp_relname(rs), exp_name(rs), exp_subtype(rs), exp_card(rs), has_nil(rs), is_intern(rs));
				}
				rel = r;
			}
		} else if (r) {
			rel_setsubquery(r);
			rs = rel_lastexp(sql, r);
			if (r->card > CARD_ATOM) {
				/* if single value (independed of relations), rewrite */
				if (is_project(r->op) && !r->l && r->exps && list_length(r->exps) == 1) {
					return rel_compare_exp(sql, rel, ls, r->exps->h->data, compare_op, NULL, k.reduce);
				} else if (quantifier != 1) { 
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, compare_aggr_op(compare_op, quantifier), exp_subtype(rs));

					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
				}
			}
			rel = rel_crossproduct(sql->sa, rel, r, op_semi);
		}
	}
	if (!rs) 
		return NULL;
	return rel_compare_exp(sql, rel, ls, rs, compare_op, rs2, k.reduce);
}

static sql_exp*
_rel_nop( mvc *sql, sql_schema *s, char *fname, list *tl, list *exps, sql_subtype *obj_type, int nr_args, exp_kind ek)
{
	sql_subfunc *f = NULL;
	int table_func = (ek.card == card_relation);
	int type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:
		   ((ek.card == card_relation)?F_UNION:F_FUNC));
	int filt = (type == F_FUNC)?F_FILT:type;

	f = bind_func_(sql, s, fname, tl, type);
	if (f) {
		return exp_op(sql->sa, exps, f);
	} else if (obj_type && (f = bind_member_func(sql, s, fname, obj_type, nr_args, NULL)) != NULL) { 
		sql_subfunc *prev = NULL;
		node *n, *m;
		list *nexps;

		while((f = bind_member_func(sql, s, fname, obj_type, nr_args, prev)) != NULL) { 
			prev = f;
			if (f->func->type != type && f->func->type != filt)
				continue;
			if (f->func->vararg) 
				return exp_op(sql->sa, exps, f);
	       		nexps = new_exp_list(sql->sa);
			for (n = exps->h, m = f->func->ops->h; n && m;
				  	n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;

				if (a->type.type->eclass == EC_ANY) {
					sql_subtype *st = &e->tpe;
					sql_init_subtype(&a->type, st->type, st->digits, st->scale);
				}
				e = rel_check_type(sql, &a->type, e, type_equal);
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
			if (nexps) 
				return exp_op(sql->sa, nexps, f);
		}
	} else if ((f = find_func(sql, s, fname, nr_args, type, NULL)) != NULL) {
		sql_subfunc *prev = NULL;
		node *n, *m;
		list *nexps;

		while((f = find_func(sql, s, fname, nr_args, type, prev)) != NULL) { 
			prev = f;
			if (f->func->type != type && f->func->type != filt)
				continue;
			if (f->func->vararg) 
				return exp_op(sql->sa, exps, f);
	       		nexps = new_exp_list(sql->sa);
			for (n = exps->h, m = f->func->ops->h; n && m;
			  	n = n->next, m = m->next) {
				sql_arg *a = m->data;
				sql_exp *e = n->data;

				if (a->type.type->eclass == EC_ANY) {
					sql_subtype *st = &e->tpe;
					sql_init_subtype(&a->type, st->type, st->digits, st->scale);
				}
				e = rel_check_type(sql, &a->type, e, type_equal);
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
			if (nexps) 
				return exp_op(sql->sa, nexps, f);
		}
	}
	return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
}

sql_exp *
rel_logical_value_exp(mvc *sql, sql_rel **rel, symbol *sc, int f)
{
	exp_kind ek = {type_value, card_column, FALSE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	switch (sc->token) {
	case SQL_OR:
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;

		sql_exp *ls = rel_logical_value_exp(sql, rel, lo, f);
		sql_exp *rs = rel_logical_value_exp(sql, rel, ro, f);

		if (!ls || !rs)
			return NULL;
		if (sc->token == SQL_OR)
			return rel_binop_(sql, ls, rs, NULL, "or", card_value);
		else
			return rel_binop_(sql, ls, rs, NULL, "and", card_value);
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

			sql_exp *e = rel_value_exp(sql, rel, sym, f, ek);
			if (!e)
				return NULL;
			if (!obj_type)
				obj_type = exp_subtype(e);
			list_append(exps, e);
			append(tl, exp_subtype(e));
		}
		for (; rn; rn = rn->next) {
			symbol *sym = rn->data.sym;

			sql_exp *e = rel_value_exp(sql, rel, sym, f, ek);
			if (!e)
				return NULL;
			list_append(exps, e);
			append(tl, exp_subtype(e));
		}
		/* find the predicate filter function */
		return _rel_nop(sql, s, fname, tl, exps, obj_type, list_length(exps), ek);
	}
	case SQL_COMPARE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->next->data.sym;
		char *compare_op = sc->data.lval->h->next->data.sval;

		/* currently we don't handle the (universal and existential)
		   quantifiers (all and any/some) */

		sql_exp *rs = NULL, *ls = rel_value_exp(sql, rel, lo, f, ek);

		if (!ls)
			return NULL;

		if (ro->token != SQL_SELECT) {
			rs = rel_value_exp(sql, rel, ro, f, ek);
			if (!rs)
				return NULL;
			if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0)
				return NULL;
			return rel_binop_(sql, ls, rs, NULL, compare_op, card_value);
		} else {
			/* first try without current relation, too see if there
			are correlations with the outer relation */
			sql_rel *r = rel_subquery(sql, NULL, ro, ek, APPLY_JOIN);
	
			/* correlation, ie return new relation */
			if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			

				sql_exp *e;

				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;
				if (!r)
					r = rel_subquery(sql, *rel, ro, ek, f == sql_sel?APPLY_LOJ:APPLY_JOIN);

				/* get inner queries result value, ie
				   get last expression of r */
				if (r) {
					rs = rel_lastexp(sql, r);
					*rel = r;
					e = exp_compare(sql->sa, ls, rs, compare_str2type(compare_op));
					if (f != sql_sel)
						return e;
			
					/* For selection we need to convert back into Boolean */
					ls = rel_unop_(sql, ls, NULL, "isnull", card_value);
					rs = exp_atom_bool(sql->sa, 0);
					return rel_binop_(sql, ls, rs, NULL, "=", card_value);
				}
			} else if (r) {
				sql_rel *l = *rel;

				if (!l) {
					l = *rel = rel_project(sql->sa, NULL, new_exp_list(sql->sa));
					rel_project_add_exp(sql, l, ls);
				} else if (f == sql_sel) { /* allways add left side in case of selections phase */
					if (!l->processed) { /* add all expressions to the project */
						l->exps = list_merge(l->exps, rel_projections(sql, l->l, NULL, 1, 1), (fdup)NULL);
						l->exps = list_distinct(l->exps, (fcmp)exp_equal, (fdup)NULL);
						set_processed(l);
					}
					if (!rel_find_exp(l, ls))
						rel_project_add_exp(sql, l, ls);
				}
				rel_setsubquery(r);
				rs = rel_lastexp(sql, r);
				if (r->card > CARD_ATOM) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));

					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);
				}
				*rel = rel_crossproduct(sql->sa, l, r, op_join);
			}
			if (!rs) 
				return NULL;
			if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0) 
				return NULL;
			return rel_binop_(sql, ls, rs, NULL, compare_op, card_value);
		}
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
	{
		dlist *dl = sc->data.lval;
		symbol *lo = dl->h->data.sym;
		dnode *n = dl->h->next;
		sql_rel *left = NULL, *right = NULL, *outer = *rel;
		sql_exp *l = NULL, *r = NULL;
		int needproj = 0, vals_only = 1;
		list *vals = NULL, *pexps = NULL;

		if (outer && f == sql_sel && is_project(outer->op) && !is_processed(outer) && outer->l && !list_empty(outer->exps)) {
			needproj = 1;
			pexps = outer->exps;
			*rel = outer->l;
		}

		l = rel_value_exp(sql, rel, lo, f, ek);
		if (!l)
			return NULL;
		ek.card = card_set;
		if (!left)
			left = *rel;

		if (!left || (!left->l && f == sql_sel)) {
			needproj = (left != NULL);
			left = rel_project_exp(sql->sa, l);
		}
		if (left && is_project(left->op) && list_empty(left->exps))
			left = left->l;

		if (n->type == type_list) {
			sql_subtype *st = exp_subtype(l);

			vals = new_exp_list(sql->sa);
			n = n->data.lval->h;
			for (; n; n = n->next) {
				symbol *sval = n->data.sym;
				/* without correlation first */
				sql_rel *z = NULL, *rl;

				r = rel_value_exp(sql, &z, sval, f, ek);
				if (l && IS_ANY(st->type->eclass)){
					l = rel_check_type(sql, exp_subtype(r), l, type_equal);
					if (l)
						st = exp_subtype(l);
				}
				if (!l || !r || !(r=rel_check_type(sql, st, r, type_equal))) {
					rel_destroy(right);
					return NULL;
				}
				if (z) {
					vals_only = 0;
					rl = z;
				} else {
					list_append(vals, r);
					rl = rel_project_exp(sql->sa, r);
				}
				if (right) {
					rl = rel_setop(sql->sa, right, rl, op_union);
					rl->exps = rel_projections(sql, rl, NULL, 0, 1);
					set_processed(rl);
				}
				right = rl;
			}
			if (right->processed)
				right = rel_label(sql, right, 0);
			right = rel_distinct(right);
		} else {
			return sql_error(sql, 02, "IN: missing inner query");
		}
		if (right) {
			sql_exp *e = NULL;
		
			if (vals_only) {
				node *n;

				rel_destroy(right);
				for(n=vals->h; n; n = n->next) {
					sql_exp *r = n->data, *ne;

					if (sc->token == SQL_NOT_IN)
						ne = rel_binop_(sql, l, r, NULL, "<>", card_value);
					else
						ne = rel_binop_(sql, l, r, NULL, "=", card_value);
					if (!e) {
						e = ne;
					} else if (sc->token == SQL_NOT_IN) {
						e = rel_binop_(sql, e, ne, NULL, "and", card_value);
					} else {
						e = rel_binop_(sql, e, ne, NULL, "or", card_value);
					}
				}
				*rel = outer;
				return e;
			}
			r = rel_lastexp(sql, right);
			if (rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
				return NULL;
			left = rel_crossproduct(sql->sa, left, right, op_join);
			left->op = op_left;
			set_processed(left);
			e = exp_compare(sql->sa, l, r, cmp_equal );
			rel_join_add_exp(sql->sa, left, e);
			if (*rel && needproj)
				left = *rel = rel_project(sql->sa, left, pexps);
			else
				*rel = left;
			if (sc->token == SQL_NOT_IN)
				e = rel_binop_(sql, l, r, NULL, "<>", card_value);
			else
				e = rel_binop_(sql, l, r, NULL, "=", card_value);
			return e;
		}
		return NULL;
	}
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
	{
		symbol *lo = sc->data.sym;
		sql_exp *le = rel_value_exp(sql, rel, lo, f, ek);
		sql_subfunc *f = NULL;

		if (!le)
			return NULL;

		if (sc->token != SQL_EXISTS)
			f = sql_bind_func(sql->sa, sql->session->schema, "sql_not_exists", exp_subtype(le), NULL, F_FUNC);
		else
			f = sql_bind_func(sql->sa, sql->session->schema, "sql_exists", exp_subtype(le), NULL, F_FUNC);

		if (!f) 
			return sql_error(sql, 02, "exist operator on type %s missing", exp_subtype(le)->type->sqlname);
		return exp_unop(sql->sa, le, f);
	}
	case SQL_LIKE:
	case SQL_NOT_LIKE:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;
		int insensitive = sc->data.lval->h->next->next->data.i_val;
		int anti = (sc->token == SQL_NOT_LIKE) != (sc->data.lval->h->next->next->next->data.i_val != 0);
		sql_subtype *st = sql_bind_localtype("str");
		sql_exp *le = rel_value_exp(sql, rel, lo, f, ek);
		sql_exp *re, *ee = NULL;
		char *like = insensitive ? (anti ? "not_ilike" : "ilike") : (anti ? "not_like" : "like");
		sql_schema *sys = mvc_bind_schema(sql, "sys");

		if (!le)
			return NULL;

		if (!exp_subtype(le)) 
			return sql_error(sql, 02, "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		lo = ro->data.lval->h->data.sym;
		/* like uses a single string pattern */
		ek.card = card_value;
		re = rel_value_exp(sql, rel, lo, f, ek);
		if (!re)
			return NULL;
		if (!exp_subtype(re)) {
			if (rel_set_type_param(sql, st, re, 0) == -1) 
				return sql_error(sql, 02, "LIKE: wrong type, should be string");
		} else if ((re = rel_check_type(sql, st, re, type_equal)) == NULL) {
			return sql_error(sql, 02, "LIKE: wrong type, should be string");
		}
		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		}
		if (ee)
			return rel_nop_(sql, le, re, ee, NULL, sys, like, card_value);
		return rel_binop_(sql, le, re, sys, like, card_value);
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le = rel_value_exp(sql, rel, lo, f, ek);
		sql_exp *re1 = rel_value_exp(sql, rel, ro1, f, ek);
		sql_exp *re2 = rel_value_exp(sql, rel, ro2, f, ek);
		sql_exp *e1 = NULL, *e2 = NULL;

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;
		
		if (rel_convert_types(sql, &le, &re1, 1, type_equal) < 0 ||
				rel_convert_types(sql, &le, &re2, 1, type_equal) < 0)
			return NULL;

		if (!re1 || !re2) 
			return NULL;

		if (symmetric) {
			sql_exp *tmp = NULL;
			sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(re1), exp_subtype(re2), F_FUNC);
			sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(re1), exp_subtype(re2), F_FUNC);

			if (!min || !max) {
				return sql_error(sql, 02, "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
			}
			tmp = exp_binop(sql->sa, re1, re2, min);
			re2 = exp_binop(sql->sa, re1, re2, max);
			re1 = tmp;
		}

		if (sc->token == SQL_NOT_BETWEEN) {
			e1 = rel_binop_(sql, le, re1, NULL, "<", card_value);
			e2 = rel_binop_(sql, le, re2, NULL, ">", card_value);
		} else {
			e1 = rel_binop_(sql, le, re1, NULL, ">=", card_value);
			e2 = rel_binop_(sql, le, re2, NULL, "<=", card_value);
		}
		if (!e1 || !e2)
			return NULL;
		if (sc->token == SQL_NOT_BETWEEN) {
			return rel_binop_(sql, e1, e2, NULL, "or", card_value);
		} else {
			return rel_binop_(sql, e1, e2, NULL, "and", card_value);
		}
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *le = rel_value_exp(sql, rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, le, NULL, "isnull", card_value);
		if (sc->token != SQL_IS_NULL)
			le = rel_unop_(sql, le, NULL, "not", card_value);
		return le;
	}
	case SQL_NOT: {
		sql_exp *le = rel_logical_value_exp(sql, rel, sc->data.sym, f);

		if (!le)
			return le;
		return rel_unop_(sql, le, NULL, "not", card_value);
	}
	case SQL_ATOM: {
		/* TRUE or FALSE */
		AtomNode *an = (AtomNode *) sc;

		if (!an || !an->a) {
			assert(0);
			return exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("void"), NULL));
		} else {
			return exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		}
	}
	case SQL_COLUMN:
		return rel_column_ref(sql, rel, sc, f);
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT: {
		*rel = rel_setquery(sql, *rel, sc);
		if (*rel)
			return rel_lastexp(sql, *rel);
		return NULL;
	}
	default: {
		sql_exp *re, *le = rel_value_exp(sql, rel, sc, f, ek);
		sql_subtype bt;

		if (!le)
			return NULL;
		re = exp_atom_bool(sql->sa, 1);
		sql_find_subtype(&bt, "boolean", 0, 0);
		if ((le = rel_check_type(sql, &bt, le, type_equal)) == NULL) 
			return NULL;
		return rel_binop_(sql, le, re, NULL, "=", 0);
	}
	}
}

sql_rel *
rel_logical_exp(mvc *sql, sql_rel *rel, symbol *sc, int f)
{
	exp_kind ek = {type_value, card_column, TRUE};

	if (!sc)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

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
			lr = rel_logical_exp(sql, lr, lo, f);
			if (lr) {
				lexps = lr->exps;
				lr = lr->l;
			}
			rr = rel_select_copy(sql->sa, rr, sa_list(sql->sa));
			rr = rel_logical_exp(sql, rr, ro, f);
			if (rr) {
				rexps = rr->exps;
				rr = rr->l;
			}
			sql->pushdown = pushdown;
		} else {
			lr = rel_logical_exp(sql, lr, lo, f);
			rr = rel_logical_exp(sql, rr, ro, f);
		}

		if (!lr || !rr)
			return NULL;
		return rel_or(sql, lr, rr, exps, lexps, rexps);
	}
	case SQL_AND:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		symbol *ro = sc->data.lval->h->next->data.sym;

		rel = rel_logical_exp(sql, rel, lo, f);
		if (!rel)
			return NULL;
		return rel_logical_exp(sql, rel, ro, f);
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

			sql_exp *e = rel_value_exp(sql, &rel, sym, f, ek);
			if (!e)
				return NULL;
			list_append(l, e);
		}
		for (; rn; rn = rn->next) {
			symbol *sym = rn->data.sym;

			sql_exp *e = rel_value_exp(sql, &rel, sym, f, ek);
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
		/* currently we don't handle the (universal and existential)
		   quantifiers (all and any/some) */
		if (n->next->next->next)
			quantifier = n->next->next->next->data.i_val + 1; 
		assert(quantifier == 0 || quantifier == 1 || quantifier == 2);
		return rel_compare(sql, rel, lo, ro, NULL, compare_op, f, ek, quantifier);
	}
	/* Set Member ship */
	case SQL_IN:
	case SQL_NOT_IN:
	{
		dlist *dl = sc->data.lval;
		symbol *lo = NULL;
		dnode *n = dl->h->next, *dn = NULL;
		sql_rel *left = NULL, *right = NULL, *outer = rel;
		sql_exp *l = NULL, *e, *r = NULL, *ident = NULL;
		list *vals = NULL, *ll = sa_list(sql->sa);
		int correlated = 0;
		int l_is_value = 1, r_is_rel = 0;
		list *pexps = NULL;

		/* complex case */
		if (dl->h->type == type_list) { /* (a,b..) in (.. ) */
			dn = dl->h->data.lval->h;
			lo = dn->data.sym;
			dn = dn->next;
		} else {
			lo = dl->h->data.sym;
		}
		while(lo) {
			l = rel_value_exp(sql, &left, lo, f, ek);
			if (!l && sql->session->status != -ERR_AMBIGUOUS) {
				l_is_value = 0;
				/* reset error */
				left = rel;
				sql->session->status = 0;
				sql->errstr[0] = 0;
				l = rel_value_exp(sql, &left, lo, f, ek);
			} else if (!left) {
				left = rel_project_exp(sql->sa, exp_label(sql->sa, l, ++sql->label));
			}
			if (!l)
				return NULL;

			ek.card = card_set;

			/* first remove the NULLs */
			if (sc->token == SQL_NOT_IN &&
		    	    l->card != CARD_ATOM && has_nil(l)) {
				sql_exp *ol;

				rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
			       	pexps = rel_projections(sql, rel, NULL, 1, 1);
				l = exp_label(sql->sa, l, ++sql->label);
				append(rel->exps, l);
				ol = l;
				l = exp_column(sql->sa, exp_relname(ol), exp_name(ol), exp_subtype(ol), ol->card, has_nil(ol), is_intern(ol));
				e = rel_unop_(sql, l, NULL, "isnull", card_value);
				e = exp_compare(sql->sa, e, exp_atom_bool(sql->sa, 0), cmp_equal);
				left = rel = rel_select(sql->sa, rel, e);
				l = exp_column(sql->sa, exp_relname(ol), exp_name(ol), exp_subtype(ol), ol->card, has_nil(ol), is_intern(ol));
			}

			append(ll, l);
			lo = NULL;
			if (dn) {
				lo = dn->data.sym;
				dn = dn->next;
			}
		}

		/* list of values or subqueries */
		if (n->type == type_list) {
			sql_rel *z = NULL;

			vals = new_exp_list(sql->sa);
			n = dl->h->next;
			n = n->data.lval->h;

			/* Simple value list first */
			for (; n; n = n->next) {
				r = rel_value_exp(sql, &z, n->data.sym, f, ek);
				if (z || !r)
					break;
				l = ll->h->data;
				if (rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
					return NULL;
				ll->h->data = l;
				list_append(vals, r);
			}
			if (!n) { /* correct types */
				sql_subtype *st;
				list *nvals = new_exp_list(sql->sa);
				node *n;

				if (list_length(ll) != 1)
					return sql_error(sql, 02, "IN: incorrect left hand side");

				l = ll->h->data;
				st = exp_subtype(l);
				for (n=vals->h; n; n = n->next) {
					if ((r = rel_check_type(sql, st, n->data, type_equal)) == NULL) 
						return NULL;
					list_append(nvals, r);
				}
				e = exp_in(sql->sa, l, nvals, sc->token==SQL_NOT_IN?cmp_notin:cmp_in);
				rel = rel_select(sql->sa, rel, e);
				if (pexps) 
					rel = rel_project(sql->sa, rel, pexps);
				return rel;
			} else { /* complex case */
				vals = new_exp_list(sql->sa);
				n = dl->h->next;
				n = n->data.lval->h;
				for (; n; n = n->next) {
					symbol *sval = n->data.sym;
					/* without correlation first */
					sql_rel *z = NULL;
					sql_rel *rl;

					r = rel_value_exp(sql, &z, sval, f, ek);
					if (z)
						r_is_rel = 1;
					if (!r && sql->session->status != -ERR_AMBIGUOUS) {
						/* reset error */
						sql->session->status = 0;
						sql->errstr[0] = 0;

						if (l_is_value) {
							outer = rel = rel_add_identity(sql, rel_dup(outer), &ident);
							ident = exp_column(sql->sa, exp_relname(ident), exp_name(ident), exp_subtype(ident), ident->card, has_nil(ident), is_intern(ident));
						} else
							rel = left = rel_dup(left);
						r = rel_value_exp(sql, &rel, sval, f, ek);
						if (r && !is_project(rel->op)) {
							rel = rel_project(sql->sa, rel, NULL);
							rel_project_add_exp(sql, rel, r);
							if (ident)
								rel_project_add_exp(sql, rel, ident);
						}
						z = rel;
						correlated = 1;
					}
					if (!r) {
						rel_destroy(right);
						return NULL;
					}
					if (z) {
						rl = z;
					} else {
						list_append(vals, r);
						rl = rel_project_exp(sql->sa, exp_label(sql->sa, r, ++sql->label));
					}
					if (right) {
						rl = rel_setop(sql->sa, right, rl, op_union);
						rl->exps = rel_projections(sql, rl, NULL, 0, 1);
						set_processed(rl);
					}
					right = rl;
				}
			}
			if (!correlated) {
				if (right->processed)
					right = rel_label(sql, right, 0);
			}
		} else {
			return sql_error(sql, 02, "IN: missing inner query");
		}

		if (right) {
			node *n, *m;
			list *rexps, *jexps = sa_list(sql->sa);


			/* list through all left/right expressions */
			rexps = right->exps;
			if (!is_project(right->op) || (list_length(ll)+((ident)?1:0)) != list_length(rexps)) {
				if (list_length(ll) == 1)
					return sql_error(sql, 02, "IN: inner query should return a single column");
				return NULL;
			}

			for (n=ll->h, m=rexps->h; n && m; n = n->next, m = m->next) {
				sql_exp *l = n->data;
				sql_exp *r = m->data;

				r = exp_alias_or_copy(sql, exp_relname(r), exp_name(r), right, r);
				if (rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
					return NULL;
				e = exp_compare(sql->sa, l, r, cmp_equal );
				append(jexps, e);
			}
			if (correlated && l_is_value) {
				sql_exp *le;

				le = exps_bind_column2(right->exps, exp_relname(ident), exp_name(ident));
				exp_label(sql->sa, le, ++sql->label);
				le = exp_column(sql->sa, exp_relname(le), exp_name(le), exp_subtype(le), le->card, has_nil(le), is_intern(le));

				right = rel_select(sql->sa, right, NULL);
				right->exps = jexps;

				rel = rel_crossproduct(sql->sa, outer, right, op_join);
				rel->exps = sa_list(sql->sa);
				le = exp_compare(sql->sa, ident, le, cmp_equal);
				append(rel->exps, le);
			} else {
				rel = rel_crossproduct(sql->sa, left, right, op_join);
				rel->exps = jexps;
			}
			if (correlated || l_is_value || r_is_rel) {
				rel->op = (sc->token == SQL_IN)?op_semi:op_anti;
			} else if (sc->token == SQL_NOT_IN) {
				rel->op = op_left;
				set_processed(rel);
				e = rel_unop_(sql, r, NULL, "isnull", card_value);
				r = exp_atom_bool(sql->sa, 1);
				e = exp_compare(sql->sa,  e, r, cmp_equal);
				rel = rel_select(sql->sa, rel, e);
			}
			if (!correlated && l_is_value && outer)
				rel = rel_crossproduct(sql->sa, rel_dup(outer), rel, op_join);
			rel = rel_project(sql->sa, rel, rel_projections(sql, outer, NULL, 1, 1));
			set_processed(rel);
			return rel;
		}
		return right;
	}
	case SQL_EXISTS:
	case SQL_NOT_EXISTS:
	{
		symbol *lo = sc->data.sym;
		sql_rel *r;
		int apply = APPLY_EXISTS;

		if (sc->token != SQL_EXISTS)
			apply = APPLY_NOTEXISTS;
		ek.card = card_set;
		r = rel_subquery(sql, NULL, lo, ek, apply);
		if (!r && rel && sql->session->status != -ERR_AMBIGUOUS) { /* correlation */
			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';
			r = rel_subquery(sql, rel, lo, ek, apply);
			return r;
		}
		if (!r || !rel)
			return NULL;
		r = rel = rel_crossproduct(sql->sa, rel, r, op_join);
		if (sc->token == SQL_EXISTS) {
			r->op = op_semi;
		} else {	
			r->op = op_anti;
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
		sql_exp *le = rel_value_exp(sql, &rel, lo, f, ek);
		sql_exp *re, *ee = NULL;

		if (!le)
			return NULL;

		if (!exp_subtype(le)) 
			return sql_error(sql, 02, "SELECT: parameter not allowed on "
					"left hand side of LIKE operator");

		/* Do we need to escape ? */
		if (dlist_length(ro->data.lval) == 2) {
			char *escape = ro->data.lval->h->next->data.sval;
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, escape)));
		} else {
			ee = exp_atom(sql->sa, atom_string(sql->sa, st, sa_strdup(sql->sa, "")));
		}
		ro = ro->data.lval->h->data.sym;
		re = rel_value_exp(sql, &rel, ro, f, ek);
		if (!re)
			return NULL;
		if (!exp_subtype(re)) {
			if (rel_set_type_param(sql, st, re, 0) == -1) 
				return sql_error(sql, 02, "LIKE: wrong type, should be string");
		} else if ((re = rel_check_type(sql, st, re, type_equal)) == NULL) {
			return sql_error(sql, 02, "LIKE: wrong type, should be string");
		}
		if ((le = rel_check_type(sql, st, le, type_equal)) == NULL) 
			return sql_error(sql, 02, "LIKE: wrong type, should be string");
		return rel_filter_exp_(sql, rel, le, re, ee, (insensitive ? "ilike" : "like"), anti);
	}
	case SQL_BETWEEN:
	case SQL_NOT_BETWEEN:
	{
		symbol *lo = sc->data.lval->h->data.sym;
		int symmetric = sc->data.lval->h->next->data.i_val;
		symbol *ro1 = sc->data.lval->h->next->next->data.sym;
		symbol *ro2 = sc->data.lval->h->next->next->next->data.sym;
		sql_exp *le = rel_value_exp(sql, &rel, lo, f, ek);
		sql_exp *re1 = rel_value_exp(sql, &rel, ro1, f, ek);
		sql_exp *re2 = rel_value_exp(sql, &rel, ro2, f, ek);
		int flag = 0;

		assert(sc->data.lval->h->next->type == type_int);
		if (!le || !re1 || !re2) 
			return NULL;

		if (rel_convert_types(sql, &le, &re1, 1, type_equal) < 0 ||
		    rel_convert_types(sql, &le, &re2, 1, type_equal) < 0)
			return NULL;

		if (!re1 || !re2) 
			return NULL;

		/* for between 3 columns we use the between operator */
		if (symmetric && re1->card == CARD_ATOM && re2->card == CARD_ATOM) {
			sql_exp *tmp = NULL;
			sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(re1), exp_subtype(re2), F_FUNC);
			sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(re1), exp_subtype(re2), F_FUNC);

			if (!min || !max) {
				return sql_error(sql, 02, "min or max operator on types %s %s missing", exp_subtype(re1)->type->sqlname, exp_subtype(re2)->type->sqlname);
			}
			tmp = exp_binop(sql->sa, re1, re2, min);
			re2 = exp_binop(sql->sa, re1, re2, max);
			re1 = tmp;
			symmetric = 0;
		}

		flag = (symmetric)?CMP_SYMMETRIC:0;

		if (le->card == CARD_ATOM) {
			sql_exp *e1, *e2;
			if (sc->token == SQL_NOT_BETWEEN) {
				e1 = rel_binop_(sql, le, re1, NULL, "<", card_value);
				e2 = rel_binop_(sql, le, re2, NULL, ">", card_value);
			} else {
				e1 = rel_binop_(sql, le, re1, NULL, ">=", card_value);
				e2 = rel_binop_(sql, le, re2, NULL, "<=", card_value);
			}
			if (!e1 || !e2)
				return NULL;
			if (sc->token == SQL_NOT_BETWEEN) {
				e1 = rel_binop_(sql, e1, e2, NULL, "or", card_value);
			} else {
				e1 = rel_binop_(sql, e1, e2, NULL, "and", card_value);
			}
			e2 = exp_atom_bool(sql->sa, 1);
			rel = rel_select(sql->sa, rel, exp_compare(sql->sa,  e1, e2, cmp_equal));
		} else if (sc->token == SQL_NOT_BETWEEN) {
			rel = rel_compare_exp_(sql, rel, le, re1, re2, 3|flag, 1);
		} else {
			rel = rel_compare_exp_(sql, rel, le, re1, re2, 3|flag, 0);
		}
		return rel;
	}
	case SQL_IS_NULL:
	case SQL_IS_NOT_NULL:
	/* is (NOT) NULL */
	{
		sql_exp *re, *le = rel_value_exp(sql, &rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, le, NULL, "isnull", card_value);
		if (sc->token == SQL_IS_NULL)
			re = exp_atom_bool(sql->sa, 1);
		else
			re = exp_atom_bool(sql->sa, 0);
		le = exp_compare(sql->sa, le, re, cmp_equal);
		return rel_select(sql->sa, rel, le);
	}
	case SQL_NOT: {
		sql_exp *re, *le = rel_value_exp(sql, &rel, sc->data.sym, f, ek);

		if (!le)
			return NULL;
		le = rel_unop_(sql, le, NULL, "not", card_value);
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
	case SQL_COLUMN: {
		sql_rel *or = rel;
		sql_exp *e = rel_column_ref(sql, &rel, sc, f);

		if (e) {
			sql_subtype bt;

			sql_find_subtype(&bt, "boolean", 0, 0);
			e = rel_check_type(sql, &bt, e, type_equal);
		}
		if (!e || or != rel)
			return NULL;
		return rel_select(sql->sa, rel, e);
	}
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		return rel_setquery(sql, rel, sc);
	default: {
		sql_exp *re, *le = rel_value_exp(sql, &rel, sc, f, ek);

		if (!le)
			return NULL;
		re = exp_atom_bool(sql->sa, 1);
		if (rel_convert_types(sql, &le, &re, 1, type_equal) < 0) 
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
rel_unop_(mvc *sql, sql_exp *e, sql_schema *s, char *fname, int card)
{
	sql_subfunc *f = NULL;
	sql_subtype *t = NULL;
	int type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:
		   ((card == card_relation)?F_UNION:F_FUNC));

	if (!s)
		s = sql->session->schema;
	t = exp_subtype(e);
	f = bind_func(sql, s, fname, t, NULL, type);
	/* try to find the function without a type, and convert
	 * the value to the type needed by this function!
	 */
	if (!f &&
	   (f = find_func(sql, s, fname, 1, type, NULL)) != NULL && check_card(card, f)) {

		if (!f->func->vararg) {
			sql_arg *a = f->func->ops->h->data;

			e = rel_check_type(sql, &a->type, e, type_equal);
		}
		if (!e) 
			f = NULL;
	}
	if (f && check_card(card, f)) {
		sql_arg *ares = f->func->res?f->func->res->h->data:NULL;
		if (ares && ares->type.scale == INOUT) {
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
		char *type = exp_subtype(e)->type->sqlname;

		return sql_error(sql, 02, "SELECT: no such unary operator '%s(%s)'", fname, type);
	}
	return NULL;
}

static sql_exp * _rel_aggr(mvc *sql, sql_rel **rel, int distinct, sql_schema *s, char *aname, dnode *arguments, int f);
static sql_exp *rel_aggr(mvc *sql, sql_rel **rel, symbol *se, int f);

static sql_exp *
rel_unop(mvc *sql, sql_rel **rel, symbol *se, int fs, exp_kind ek)
{
	dnode *l = se->data.lval->h;
	char *fname = qname_fname(l->data.lval);
	char *sname = qname_schema(l->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	sql_exp *e = NULL;
	sql_subfunc *f = NULL;
	sql_subtype *t = NULL;
	int type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:F_FUNC);

	if (sname)
		s = mvc_bind_schema(sql, sname);

	if (!s)
		return NULL;
	f = find_func(sql, s, fname, 1, F_AGGR, NULL);
	if (f) { 
		e = rel_aggr(sql, rel, se, fs);
		if (e)
			return e;
	}
	/* reset error */
	sql->session->status = 0;
	sql->errstr[0] = '\0';
       	e = rel_value_exp(sql, rel, l->next->data.sym, fs, iek);
	if (!e)
		return NULL;

	t = exp_subtype(e);
	if (!t) {
		f = find_func(sql, s, fname, 1, type, NULL);
		if (!f)
			f = find_func(sql, s, fname, 1, F_AGGR, NULL);
		if (f) {
			sql_arg *a = f->func->ops->h->data;

			t = &a->type;
			if (rel_set_type_param(sql, t, e, 1) < 0)
				return NULL;
		}
	} else {
		f = bind_func(sql, s, fname, t, NULL, type);
		if (!f)
			f = bind_func(sql, s, fname, t, NULL, F_AGGR);
	}
	if (f && IS_AGGR(f->func))
		return _rel_aggr(sql, rel, 0, s, fname, l->next, fs);

	if (f && type_has_tz(t) && f->func->fix_scale == SCALE_FIX) {
		/* set timezone (using msec (.3)) */
		sql_subtype *intsec = sql_bind_subtype(sql->sa, "sec_interval", 10 /*hour to second */, 3);
		atom *a = atom_int(sql->sa, intsec, sql->timezone);
		sql_exp *tz = exp_atom(sql->sa, a);

		e = rel_binop_(sql, e, tz, NULL, "sql_add", ek.card);
		if (!e)
			return NULL;
	}
	return rel_unop_(sql, e, s, fname, ek.card);
}


#define is_addition(fname) (strcmp(fname, "sql_add") == 0)
#define is_subtraction(fname) (strcmp(fname, "sql_sub") == 0)

sql_exp *
rel_binop_(mvc *sql, sql_exp *l, sql_exp *r, sql_schema *s,
		char *fname, int card)
{
	sql_exp *res = NULL;
	sql_subtype *t1, *t2;
	sql_subfunc *f = NULL;
	int type = (card == card_loader)?F_LOADER:((card == card_none)?F_PROC:
		   ((card == card_relation)?F_UNION:F_FUNC));
	if (card == card_loader) {
		card = card_none;
	}
	t1 = exp_subtype(l);
	t2 = exp_subtype(r);

	if (!s)
		s = sql->session->schema;

	/* handle param's early */
	if (!t1 || !t2) {
		if (t2 && !t1 && rel_set_type_param(sql, t2, l, 1) < 0)
			return NULL;
		if (t1 && !t2 && rel_set_type_param(sql, t1, r, 1) < 0)
			return NULL;
		t1 = exp_subtype(l);
		t2 = exp_subtype(r);
	}

	if (!t1 || !t2)
		return sql_error(sql, 01, "Cannot have a parameter (?) on both sides of an expression");

	if ((is_addition(fname) || is_subtraction(fname)) && 
		((t1->type->eclass == EC_NUM && t2->type->eclass == EC_NUM) ||
		 (t1->type->eclass == EC_BIT && t2->type->eclass == EC_BIT))) {
		sql_subtype ntp;

		sql_find_numeric(&ntp, t1->type->localtype, t1->digits+1);
		l = rel_check_type(sql, &ntp, l, type_equal);
		sql_find_numeric(&ntp, t2->type->localtype, t2->digits+1);
		r = rel_check_type(sql, &ntp, r, type_equal);
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
			l = exp_scale_algebra(sql, f, l, r);
		} else if (f->func->fix_scale == SCALE_MUL) {
			l = exp_sum_scales(sql, f, l, r);
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
		if (IS_ANY(t1->type->eclass) || IS_ANY(t2->type->eclass)) {
			sql_exp *ol = l;
			sql_exp *or = r;

			if (IS_ANY(t1->type->eclass) && IS_ANY(t2->type->eclass)) {
				sql_subtype *s = sql_bind_localtype("str");
				l = rel_check_type(sql, s, l, type_equal);
				r = rel_check_type(sql, s, r, type_equal);
			} else if (IS_ANY(t1->type->eclass)) {
				l = rel_check_type(sql, t2, l, type_equal);
			} else {
				r = rel_check_type(sql, t1, r, type_equal);
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

			l = rel_check_type(sql, &a->type, l, type_equal);
			a = m->next->data;
			r = rel_check_type(sql, &a->type, r, type_equal);
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
		if (rel_convert_types(sql, &l, &r, 1/*fix scale*/, type_equal) >= 0){
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
					l = exp_scale_algebra(sql, f, l, r);
				} else if (f->func->fix_scale == SCALE_MUL) {
					l = exp_sum_scales(sql, f, l, r);
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

			l = rel_check_type(sql, &a->type, l, type_equal);
			a = m->next->data;
			r = rel_check_type(sql, &a->type, r, type_equal);
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

				l = rel_check_type(sql, &a->type, l, type_equal);
				a = m->next->data;
				r = rel_check_type(sql, &a->type, r, type_equal);
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
	res = sql_error(sql, 02, "SELECT: no such binary operator '%s(%s,%s)'", fname,
			exp_subtype(l)->type->sqlname,
			exp_subtype(r)->type->sqlname);
	return res;
}

static sql_exp *
rel_binop(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	dnode *dl = se->data.lval->h;
	sql_exp *l, *r;
	char *fname = qname_fname(dl->data.lval);
	char *sname = qname_schema(dl->data.lval);
	sql_schema *s = sql->session->schema;
	exp_kind iek = {type_value, card_column, FALSE};
	int type = (ek.card == card_loader)?F_LOADER:((ek.card == card_none)?F_PROC:F_FUNC);

	sql_subfunc *sf = NULL;

	if (sname)
		s = mvc_bind_schema(sql, sname);
	if (!s)
		return NULL;

	l = rel_value_exp(sql, rel, dl->next->data.sym, f, iek);
	r = rel_value_exp(sql, rel, dl->next->next->data.sym, f, iek);
	if (!l && !r)
		sf = find_func(sql, s, fname, 2, F_AGGR, NULL);
	if (!l && !r && sf) { /* possibly we cannot resolve the argument as the function maybe an aggregate */
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';
		return rel_aggr(sql, rel, se, f);
	}

	if (type == F_FUNC) {
		sf = find_func(sql, s, fname, 2, F_AGGR, NULL);
		if (sf) {
			if (!l || !r) { /* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';
			}
			return _rel_aggr(sql, rel, 0, s, fname, dl->next, f);
		}
	}

	if (!l || !r) 
		return NULL;

	return rel_binop_(sql, l, r, s, fname, ek.card);
}

sql_exp *
rel_nop_(mvc *sql, sql_exp *a1, sql_exp *a2, sql_exp *a3, sql_exp *a4, sql_schema *s, char *fname, int card)
{
	list *tl = sa_list(sql->sa);
	sql_subfunc *f = NULL;
	int type = (card == card_none)?F_PROC:
		   ((card == card_relation)?F_UNION:F_FUNC);

	append(tl, exp_subtype(a1));
	append(tl, exp_subtype(a2));
	append(tl, exp_subtype(a3));
	if (a4)
		append(tl, exp_subtype(a4));

	if (!s)
		s = sql->session->schema;
	f = bind_func_(sql, s, fname, tl, type);
	if (!f)
		return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
	if (!a4)
		return exp_op3(sql->sa, a1,a2,a3,f);
	return exp_op4(sql->sa, a1,a2,a3,a4,f);
}


static sql_exp *
rel_nop(mvc *sql, sql_rel **rel, symbol *se, int fs, exp_kind ek)
{
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

	for (; ops; ops = ops->next, nr_args++) {
		sql_exp *e = rel_value_exp(sql, rel, ops->data.sym, fs, iek);
		sql_subtype *tpe;

		if (!e) 
			return NULL;
		append(exps, e);
		tpe = exp_subtype(e);
		if (!nr_args)
			obj_type = tpe;
		append(tl, tpe);
	}
	if (sname)
		s = mvc_bind_schema(sql, sname);
	
	/* first try aggregate */
	f = find_func(sql, s, fname, nr_args, F_AGGR, NULL);
	if (f)
		return _rel_aggr(sql, rel, 0, s, fname, l->next->data.lval->h, fs);
	return _rel_nop(sql, s, fname, tl, exps, obj_type, nr_args, ek);

}

static sql_exp *
_rel_aggr(mvc *sql, sql_rel **rel, int distinct, sql_schema *s, char *aname, dnode *args, int f)
{
	exp_kind ek = {type_value, card_column, FALSE};
	sql_subaggr *a = NULL;
	int no_nil = 0;
	sql_rel *groupby = *rel, *gr;
	list *exps = NULL;

	if (!groupby) {
		char *uaname = malloc(strlen(aname) + 1);
		sql_exp *e = sql_error(sql, 02, "%s: missing group by",
				toUpperCopy(uaname, aname));
		free(uaname);
		return e;
	}

	if (f == sql_having && is_select(groupby->op))
		groupby = groupby->l;

	if (groupby->l && groupby->op == op_project) {
		sql_rel *r = groupby->l;	
		if (r->op == op_groupby) {
			groupby = r;
		} else if (r->op == op_select && r->l) {
			/* a having after a groupby */
			r = r->l;
			if (r->op == op_groupby)
				groupby = r;
		}
	}

	if (groupby->op != op_groupby)		/* implicit groupby */
		*rel = rel_project2groupby(sql, groupby);
	if (!*rel)
		return NULL;

	if (f == sql_where) {
		char *uaname = malloc(strlen(aname) + 1);
		sql_exp *e = sql_error(sql, 02, "%s: not allowed in WHERE clause",
				toUpperCopy(uaname, aname));
		free(uaname);
		return e;
	}
	
	if (!args->data.sym) {	/* count(*) case */
		sql_exp *e;

		if (strcmp(aname, "count") != 0) {
			char *uaname = malloc(strlen(aname) + 1);
			sql_exp *e = sql_error(sql, 02, "%s: unable to perform '%s(*)'",
					toUpperCopy(uaname, aname), aname);
			free(uaname);
			return e;
		}
		a = sql_bind_aggr(sql->sa, s, aname, NULL);
		e = exp_aggr(sql->sa, NULL, a, distinct, 0, groupby->card, 0);
		if (*rel == groupby && f == sql_sel) /* selection */
			return e;
		return rel_groupby_add_aggr(sql, groupby, e);
	} 

	exps = sa_list(sql->sa);

	/* use cnt as nils shouldn't be counted */
	gr = groupby->l;

	no_nil = 1;

	reset_processed(groupby);
	for (	; args; args = args->next ) {
		sql_exp *e = rel_value_exp(sql, &gr, args->data.sym, f, ek);

		if (gr && e && is_project(gr->op) && !is_set(gr->op) && e->type != e_column) {
			rel_project_add_exp(sql, gr, e);
			e = exp_alias_or_copy(sql, exp_relname(e), exp_name(e), gr->l, e);
		}
		if (!e || !exp_subtype(e)) { /* we also do not expect parameters here */
			set_processed(groupby);
			return NULL;
		}
		list_append(exps, e);
	}
	set_processed(groupby);
	groupby->l = gr;

	a = sql_bind_aggr_(sql->sa, s, aname, exp_types(sql->sa, exps));
	if (!a && list_length(exps) > 1) { 
		a = sql_bind_member_aggr(sql->sa, s, aname, exp_subtype(exps->h->data), list_length(exps));
		if (a) {
			node *n, *op = a->aggr->ops->h;
			list *nexps = sa_list(sql->sa);

			for (n = exps->h ; a && op && n; op = op->next, n = n->next ) {
				sql_arg *arg = op->data;
				sql_exp *e = n->data;

				e = rel_check_type(sql, &arg->type, e, type_equal);
				if (!e)
					a = NULL;
				list_append(nexps, e);
			}
			if (a && list_length(nexps))  /* count(col) has |exps| != |nexps| */
				exps = nexps;
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

					e = rel_check_type(sql, &arg->type, e, type_equal);
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

		if (*rel != groupby || f != sql_sel) /* selection */
			e = rel_groupby_add_aggr(sql, groupby, e);
		return e;
	} else {
		sql_exp *e;
		char *type = "unknown";
		char *uaname = malloc(strlen(aname) + 1);

		if (exps->h) {
			sql_exp *e = exps->h->data;
			type = exp_subtype(e)->type->sqlname;
		}

		e = sql_error(sql, 02, "%s: no such operator '%s(%s)'",
				toUpperCopy(uaname, aname), aname, type);

		free(uaname);
		return e;
	}
}

static sql_exp *
rel_aggr(mvc *sql, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;
	dnode *d = l->h->next;
	int distinct = 0;
	char *aname = qname_fname(l->h->data.lval);
	char *sname = qname_schema(l->h->data.lval);
	sql_schema *s = sql->session->schema;

	if (l->h->next->type == type_int) {
		distinct = l->h->next->data.i_val;
		d = l->h->next->next;
	}
	if (sname)
		s = mvc_bind_schema(sql, sname);
	return _rel_aggr( sql, rel, distinct, s, aname, d, f);
}

static sql_exp *
rel_case(mvc *sql, sql_rel **rel, int token, symbol *opt_cond, dlist *when_search_list, symbol *opt_else, int f)
{
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

			e1 = rel_value_exp(sql, rel, dn->data.sym, f, ek);
			e2 = rel_value_exp(sql, rel, dn->next->data.sym, f, ek);
			if (e1 && e2) {
				cond = rel_binop_(sql, e1, e2, NULL, "=", card_value);
				result = exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(e1), NULL));
				else_exp = e1;	/* ELSE case */
			}
			/* COALESCE(e1,e2) == CASE WHEN e1
			   IS NOT NULL THEN e1 ELSE e2 END */
		} else if (token == SQL_COALESCE) {
			cond = rel_value_exp(sql, rel, dn->data.sym, f, ek);

			if (cond) {
				result = cond;
				cond = rel_unop_(sql, rel_unop_(sql, cond, NULL, "isnull", card_value), NULL, "not", card_value);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(sql, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(sql, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
					return NULL;
				cond = rel_binop_(sql, l, r, NULL, "=", card_value);
			} else {
				cond = rel_logical_value_exp(sql, rel, when->h->data.sym, sql_sel);
			}
			result = rel_value_exp(sql, rel, when->h->next->data.sym, f, ek);
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
		return sql_error(sql, 02, "result type missing");
	/* for COALESCE we skip the last (else part) */
	for (; dn && (token != SQL_COALESCE || dn->next); dn = dn->next) {
		sql_exp *cond = NULL, *result = NULL;

		if (token == SQL_COALESCE) {
			cond = rel_value_exp(sql, rel, dn->data.sym, f, ek);

			if (cond) {
				result = cond;
				cond = rel_unop_(sql, rel_unop_(sql, cond, NULL, "isnull", card_value), NULL, "not", card_value);
			}
		} else {
			dlist *when = dn->data.sym->data.lval;

			if (opt_cond) {
				sql_exp *l = rel_value_exp(sql, rel, opt_cond, f, ek);
				sql_exp *r = rel_value_exp(sql, rel, when->h->data.sym, f, ek);
				if (!l || !r || rel_convert_types(sql, &l, &r, 1, type_equal) < 0) 
					return NULL;
				cond = rel_binop_(sql, l, r, NULL, "=", card_value);
			} else {
				cond = rel_logical_value_exp(sql, rel, when->h->data.sym, sql_sel);
			}
			result = rel_value_exp(sql, rel, when->h->next->data.sym, sql_sel, ek);
		}
		if (!cond || !result) 
			return NULL;
		list_prepend(conds, cond);
		list_prepend(results, result);

		tpe = exp_subtype(result);
		if (!tpe) 
			return sql_error(sql, 02, "result type missing");
		supertype(&rtype, restype, tpe);
		restype = &rtype;
	}
	if (opt_else || else_exp) {
		sql_exp *result = else_exp;

		if (!result && !(result = rel_value_exp(sql, rel, opt_else, f, ek))) 
			return NULL;

		tpe = exp_subtype(result);
		if (tpe && restype) {
			supertype(&rtype, restype, tpe);
			tpe = &rtype;
		}
		restype = tpe;
		if (restype->type->localtype == TYPE_void) /* NULL */
			restype = sql_bind_localtype("str");

		if (!result || !(result = rel_check_type(sql, restype, result, type_equal))) 
			return NULL;
		res = result;

		if (!res) 
			return NULL;
	} else {
		if (restype->type->localtype == TYPE_void) /* NULL */
			restype = sql_bind_localtype("str");
		res = exp_atom(sql->sa, atom_general(sql->sa, restype, NULL));
	}

	for (n = conds->h, m = results->h; n && m; n = n->next, m = m->next) {
		sql_exp *cond = n->data;
		sql_exp *result = m->data;

		if (!(result = rel_check_type(sql, restype, result, type_equal))) 
			return NULL;

		if (!(cond = rel_check_type(sql, &bt, cond, type_equal))) 
			return NULL;

		/* remove any null's in the condition */
		if (has_nil(cond) && token != SQL_COALESCE) {
			sql_exp *condnil = rel_unop_(sql, cond, NULL, "isnull", card_value);
			cond = rel_nop_(sql, condnil, exp_atom_bool(sql->sa, 0), cond, NULL, NULL, "ifthenelse", card_value);
		}
		res = rel_nop_(sql, cond, result, res, NULL, NULL, "ifthenelse", card_value);
		if (!res) 
			return NULL;
		/* ugh overwrite res type */
	       	((sql_subfunc*)res->f)->res->h->data = sql_create_subtype(sql->sa, restype->type, restype->digits, restype->scale);
	}
	return res;
}

static sql_exp *
rel_case_exp(mvc *sql, sql_rel **rel, symbol *se, int f)
{
	dlist *l = se->data.lval;

	if (se->token == SQL_COALESCE) {
		symbol *opt_else = l->t->data.sym;

		return rel_case(sql, rel, se->token, NULL, l, opt_else, f);
	} else if (se->token == SQL_NULLIF) {
		return rel_case(sql, rel, se->token, NULL, l, NULL, f);
	} else if (l->h->type == type_list) {
		dlist *when_search_list = l->h->data.lval;
		symbol *opt_else = l->h->next->data.sym;

		return rel_case(sql, rel, SQL_CASE, NULL, when_search_list, opt_else, f);
	} else {
		symbol *scalar_exp = l->h->data.sym;
		dlist *when_value_list = l->h->next->data.lval;
		symbol *opt_else = l->h->next->next->data.sym;

		return rel_case(sql, rel, SQL_CASE, scalar_exp, when_value_list, opt_else, f);
	}
}

static sql_exp *
rel_cast(mvc *sql, sql_rel **rel, symbol *se, int f)
{

	dlist *dl = se->data.lval;
	symbol *s = dl->h->data.sym;
	sql_subtype *tpe = &dl->h->next->data.typeval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *e = rel_value_exp(sql, rel, s, f, ek);

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
		e = rel_check_type(sql, tpe, e, type_cast);
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
			"3F000!NEXT VALUE FOR: no such schema '%s'", sname);
	if (!s)
		s = sql->session->schema;

	if (!find_sql_sequence(s, seq) && !stack_find_rel_view(sql, seq))
		return sql_error(sql, 02, "NEXT VALUE FOR: "
			"no such sequence '%s'.'%s'", s->base.name, seq);
	sql_find_subtype(&t, "varchar", 0, 0);
	f = sql_bind_func(sql->sa, s, "next_value_for", &t, &t, F_FUNC);
	assert(f);
	return exp_binop(sql->sa, exp_atom_str(sql->sa, s->base.name, &t),
			exp_atom_str(sql->sa, seq, &t), f);
}

/* some users like to use aliases already in the groupby */
static sql_exp *
rel_selection_ref(mvc *sql, sql_rel **rel, symbol *grp, dlist *selection )
{
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
		if (n->data.sym->token == SQL_COLUMN) {
			dlist *l = n->data.sym->data.lval;
			/* AS name */
			if (l->h->next->data.sval &&
					strcmp(l->h->next->data.sval, name) == 0){
				sql_exp *ve = rel_value_exp(sql, rel, l->h->data.sym, sql_sel, ek);
				if (ve) {
					dlist *l = dlist_create(sql->sa);
					symbol *sym;
					exp_setname(sql->sa, ve, NULL, name);
					/* now we should rewrite the selection
					   such that it uses the new group
					   by column
					*/
					dlist_append_string(sql->sa, l,
						sa_strdup(sql->sa, name));
					sym = symbol_create_list(sql->sa, SQL_COLUMN, l);
					l = dlist_create(sql->sa);
					dlist_append_symbol(sql->sa, l, sym);
					/* no alias */
					dlist_append_symbol(sql->sa, l, NULL);
					n->data.sym = symbol_create_list(sql->sa, SQL_COLUMN, l);
				
				}
				return ve;
			}
		}
	}
	return NULL;
}

static list *
rel_group_by(mvc *sql, sql_rel **rel, symbol *groupby, dlist *selection, int f )
{
	//sql_rel *or = rel;
	dnode *o = groupby->data.lval->h;
	list *exps = new_exp_list(sql->sa);

	for (; o; o = o->next) {
		symbol *grp = o->data.sym;
		sql_exp *e = rel_column_ref(sql, rel, grp, f);

		/*
		if (or != rel)
			return NULL;
			*/
		if (!e) {
			char buf[ERRSIZE];
			/* reset error */
			sql->session->status = 0;
			strcpy(buf, sql->errstr);
			sql->errstr[0] = '\0';

			e = rel_selection_ref(sql, rel, grp, selection);
			if (!e) {
				if (sql->errstr[0] == 0)
					strcpy(sql->errstr, buf);
				return NULL;
			}
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
	set_processed(r);
	if (dlist_length(l) == 1) {
		char *name = l->h->data.sval;
		e = rel_bind_column(sql, r, name, sql_sel);
	}
	if (dlist_length(l) == 2) {
		char *tname = l->h->data.sval;
		char *name = l->h->next->data.sval;

		e = rel_bind_column2(sql, r, tname, name, sql_sel);
	}
	if (!e) {
		/* now we need to rewrite r
			project( project( all exps + order_exps),
				order_exps, exps )
		*/
	}
	if (e)
		return e;
	return sql_error(sql, 02, "ORDER BY: absolute column names not supported");
}

static list *
rel_projections_(mvc *sql, sql_rel *rel)
{
	list *rexps, *exps ;

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
						e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), exp_card(e), has_nil(e), is_intern(e));
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
						e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), exp_card(e), has_nil(e), is_intern(e));
						exp_setname(sql->sa, e, oe->l, oe->r);
					}
					append(exps, e);
				}
			}
		}
		return exps;
	case op_apply:
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
		return exp_column(sql->sa, e->l, e->r, exp_subtype(e), exp_card(e), has_nil(e), is_intern(e));
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
rel_order_by_column_exp(mvc *sql, sql_rel **R, symbol *column_r, int f)
{
	sql_rel *r = *R;
	sql_exp *e = NULL;
	exp_kind ek = {type_value, card_column, FALSE};

	if (f == sql_orderby) {
		assert(is_project(r->op));
		r = r->l;
	}
	if (!r)
		return e;

	if (!is_project(r->op) || is_set(r->op)) {
		r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
		(*R)->l = r;
		set_processed(r);
	}

	if (!e) {
		sql_rel *or = r;

		e = rel_value_exp(sql, &r, column_r, sql_sel, ek);
		if (r && or != r)
			(*R)->l = r;
		/* add to internal project */
		if (e && is_processed(r)) {
			rel_project_add_exp(sql, r, e);
			e = rel_lastexp(sql, r);
		}
		/* try with reverted aliases */
		if (!e && r && sql->session->status != -ERR_AMBIGUOUS) {
			sql_rel *nr = rel_project(sql->sa, r, rel_projections_(sql, r));

			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';

			set_processed(nr);
			e = rel_value_exp(sql, &nr, column_r, sql_sel, ek);
			if (e) {
				/* first rewrite e back into current column names */
				e = exp_rewrite(sql, e, nr);
				
				rel_project_add_exp(sql, r, e);
				e = rel_lastexp(sql, r);
			}
		}
	}
	if (e)
		return e;
	return sql_error(sql, 02, "ORDER BY: absolute column names not supported");
}


static list *
rel_order_by(mvc *sql, sql_rel **R, symbol *orderby, int f )
{
	sql_rel *rel = *R;
	sql_rel *or = rel;
	list *exps = new_exp_list(sql->sa);
	dnode *o = orderby->data.lval->h;

	if (f == sql_orderby) {
		assert(is_project(rel->op));
		rel = rel->l;
		or = rel;
	}
	
	for (; o; o = o->next) {
		symbol *order = o->data.sym;

		if (order->token == SQL_COLUMN) {
			symbol *col = order->data.lval->h->data.sym;
			int direction = order->data.lval->h->next->data.i_val;
			sql_exp *e = NULL;

			if (col->token == SQL_COLUMN || col->token == SQL_ATOM) {
				int is_last = 0;
				exp_kind ek = {type_value, card_column, FALSE};

				e = rel_value_exp2(sql, &rel, col, f, ek, &is_last);

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
						e = exp_column(sql->sa, e->rname, exp_name(e), exp_subtype(e), exp_card(e), has_nil(e), is_intern(e));
						/* do not cache this query */
						if (e)
							scanner_reset_key(&sql->scanner);
					} else if (e->type == e_atom) {
						return sql_error(sql, 02, "order not of type SQL_COLUMN\n");
					}
				}
			}

			assert(order->data.lval->h->next->type == type_int);
			if (or != rel)
				return NULL;
			if (!e && sql->session->status != -ERR_AMBIGUOUS && col->token == SQL_COLUMN) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				e = rel_order_by_simple_column_exp(sql, rel, col);
				if (e && e->card != rel->card) 
					e = NULL;
			}
			if (!e && sql->session->status != -ERR_AMBIGUOUS) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = '\0';

				e = rel_order_by_column_exp(sql, &rel, col, f);
				if (e && e->card != rel->card) 
					e = NULL;
			}
			if (!e) 
				return NULL;
			set_direction(e, direction);
			append(exps, e);
		} else {
			return sql_error(sql, 02, "order not of type SQL_COLUMN\n");
		}
	}
	*R = rel;
	return exps;
}

static list *
rel_frame(mvc *sql, symbol *frame, list *exps)
{
	/* units, extent, exclusion */
	dnode *d = frame->data.lval->h;

	/* RANGE vs UNITS */
	sql_exp *units = exp_atom_int(sql->sa, d->next->next->data.i_val);
	sql_exp *start = exp_atom_int(sql->sa, d->data.i_val);
	sql_exp *end   = exp_atom_int(sql->sa, d->next->data.i_val);
	sql_exp *excl  = exp_atom_int(sql->sa, d->next->next->next->data.i_val);
	append(exps, units);
	append(exps, start);
	append(exps, end);
	append(exps, excl);
	return exps;
}

/* window functions */

/*
 * select x, y, rank_op() over (partition by x order by y) as, ...
                aggr_op(z) over (partition by y order by x) as, ...
 * from table [x,y,z,w,v]
 *
 * project and order by over x,y / y,x
 * a = project( table ) [ x, y, z, w, v ], [ x, y]
 * b = project( table ) [ x, y, z, w, v ], [ y, x]
 *
 * project with order dependend operators, ie combined prev/current value 
 * aa = project (a) [ x, y, r = rank_op(diff(x) (marks a new partition), rediff(diff(x), y) (marks diff value with in partition)), z, w, v ]
 * project(aa) [ aa.x, aa.y, aa.r ] -- only keep current output list 
 * bb = project (b) [ x, y, a = aggr_op(z, diff(y), rediff(diff(y), x)), z, w, v ]
 * project(bb) [ bb.x, bb.y, bb.a ]  -- only keep current output list
 */
static sql_exp *
rel_rankop(mvc *sql, sql_rel **rel, symbol *se, int f)
{
	node *n;
	dlist *l = se->data.lval;
	symbol *window_function = l->h->data.sym;
	dlist *window_specification = l->h->next->data.lval;
	char *aname = NULL;
	char *sname = NULL;
	sql_subfunc *wf = NULL;
	sql_exp *e = NULL, *pe = NULL, *oe = NULL;
	sql_rel *r = *rel, *p;
	list *gbe = NULL, *obe = NULL, *fbe = NULL, *args, *types;
	sql_schema *s = sql->session->schema;
	int distinct = 0, project_added = 0;
	
	if (window_function->token == SQL_RANK) {
		aname = qname_fname(window_function->data.lval);
		sname = qname_schema(window_function->data.lval);
	} else { /* window aggr function */
		dnode *n = window_function->data.lval->h;
		aname = qname_fname(n->data.lval);
		sname = qname_schema(n->data.lval);
	}
	if (sname)
		s = mvc_bind_schema(sql, sname);

	if (f == sql_where) {
		char *uaname = malloc(strlen(aname) + 1);
		e = sql_error(sql, 02, "%s: not allowed in WHERE clause",
				toUpperCopy(uaname, aname));
		free(uaname);
		return e;
	}

	/* window operations are only allowed in the projection */
	if (r && r->op != op_project) {
		*rel = r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
		project_added = 1;
	}
	if (f != sql_sel || !r || r->op != op_project || is_processed(r))
		return sql_error(sql, 02, "OVER: only possible within the selection");

	p = r->l;
	p = rel_project(sql->sa, p, rel_projections(sql, p, NULL, 1, 1));

	/* Partition By */
	if (window_specification->h->data.sym) {
		gbe = rel_group_by(sql, &p, window_specification->h->data.sym, NULL /* cannot use (selection) column references, as this result is a selection column */, f );
		if (!gbe)
			return NULL;
		p->r = gbe;
	}
	/* Order By */
	if (window_specification->h->next->data.sym) {
		sql_rel *g;
		obe = rel_order_by(sql, &p, window_specification->h->next->data.sym, f);
		if (!obe)
			return NULL;
		/* conditionaly? */
		g = p->l;
		if (g->op == op_groupby) {
			list_merge(p->exps, obe, (fdup)NULL);
			p->exps = list_distinct(p->exps, (fcmp)exp_equal, (fdup)NULL);
		}
		if (p->r) {
			p->r = list_merge(sa_list(sql->sa), p->r, (fdup)NULL);
			list_merge(p->r, obe, (fdup)NULL);
		} else {
			p->r = obe;
		}
	}
	/* Frame */
	if (window_specification->h->next->next->data.sym) {
		fbe = new_exp_list(sql->sa);
		fbe = rel_frame(sql, window_specification->h->next->next->data.sym, fbe);
		if (!fbe)
			return NULL;
	}

	if (window_function->token == SQL_RANK) {
		e = p->exps->h->data; 
		e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), exp_card(e), has_nil(e), is_intern(e));
	} else {
		dnode *n = window_function->data.lval->h->next;

		if (n) {
			int is_last = 0;
			exp_kind ek = {type_value, card_column, FALSE};

			distinct = n->data.i_val;
			e = rel_value_exp2(sql, &p, n->next->data.sym, f, ek, &is_last);
		}
	}
	(void)distinct;

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
				return sql_error(sql, 02, "SELECT: function '%s' not found", "diff" );
			append(args, e);
			pe = exp_op(sql->sa, args, df);
		}
	} else {
		pe = exp_atom_bool(sql->sa, 0);
	}
	/* diff for orderby */
	if (obe) {
		sql_subtype *bt = sql_bind_localtype("bit");

		for( n = obe->h; n; n = n->next)  {
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
				return sql_error(sql, 02, "SELECT: function '%s' not found", "diff" );
			append(args, e);
			oe = exp_op(sql->sa, args, df);
		}
	} else {
		oe = exp_atom_bool(sql->sa, 0);
	}

	types = sa_list(sql->sa);
	append(types, exp_subtype(e));
	append(types, exp_subtype(pe));
	append(types, exp_subtype(oe));
	wf = bind_func_(sql, s, aname, types, F_ANALYTIC);
	if (!wf)
		return sql_error(sql, 02, "SELECT: function '%s' not found", aname );
	args = sa_list(sql->sa);
	append(args, e);
	append(args, pe);
	append(args, oe);
	e = exp_op(sql->sa, args, wf);

	r->l = p = rel_project(sql->sa, p, rel_projections(sql, p, NULL, 1, 1));
	set_processed(p);
	append(p->exps, e);
	e = rel_lastexp(sql, p);
	if (project_added) {
		append(r->exps, e);
		e = rel_lastexp(sql, r);
	}
	return e;
}

sql_exp *
rel_value_exp2(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek, int *is_last)
{
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	switch (se->token) {
	case SQL_OP:
		return rel_op(sql, se, ek);
	case SQL_UNOP:
		return rel_unop(sql, rel, se, f, ek);
	case SQL_BINOP:
		return rel_binop(sql, rel, se, f, ek);
	case SQL_NOP:
		return rel_nop(sql, rel, se, f, ek);
	case SQL_AGGR:
		return rel_aggr(sql, rel, se, f);
	case SQL_RANK:
		return rel_rankop(sql, rel, se, f);
	case SQL_COLUMN:
		return rel_column_ref(sql, rel, se, f );
	case SQL_NAME:
		return rel_var_ref(sql, se->data.sval, 1);
	case SQL_SELECT: {
		sql_rel *r;

		r = rel_subquery(sql, NULL, se, ek, APPLY_JOIN);
		if (r) {
			sql_exp *e;

			if (ek.card <= card_column && is_project(r->op) && list_length(r->exps) > 1) 
				return sql_error(sql, 02, "SELECT: subquery must return only one column");
			e = rel_lastexp(sql, r);

			/* group by needed ? */
			if (e->card > CARD_ATOM && e->card > ek.card) {
				int processed = is_processed(r);
				sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(e));

				e = exp_aggr1(sql->sa, e, zero_or_one, 0, 0, CARD_ATOM, 0);
				r = rel_groupby(sql, r, NULL);
				e = rel_groupby_add_aggr(sql, r, e);
				if (processed)
					set_processed(r);
			}
			/* single row */
			if (*rel) {
				sql_rel *p = *rel;

				rel_setsubquery(r);
				/* in the selection phase we should have project/groupbys, unless 
				 * this is the value (column) for the aggregation then the 
				 * crossproduct is pushed under the project/groupby.  */ 
				if (f == sql_sel && r->op == op_project && list_length(r->exps) == 1 && exps_are_atoms(r->exps)) {
					sql_exp *ne = r->exps->h->data;

					exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
					e = ne;
				} else if (f == sql_sel && is_project(p->op) && !is_processed(p)) {
					if (p->l) {
						p->l = rel_crossproduct(sql->sa, p->l, r, op_join);
					} else {
						p->l = r;
					}
				} else {
					*rel = rel_crossproduct(sql->sa, p, r, op_join);
				}
				*is_last = 1;
				return e;
			} else {
				*rel = r;
			}
			*is_last=1;
			return e;
		}
		if (!r && sql->session->status != -ERR_AMBIGUOUS) {
			sql_exp *rs = NULL;

			if (!*rel)
				return NULL;

			/* reset error */
			sql->session->status = 0;
			sql->errstr[0] = '\0';

			/* add unique */
			*rel = r = rel_subquery(sql, *rel, se, ek, f == sql_sel?APPLY_LOJ:APPLY_JOIN);
			if (r) {
				rs = rel_lastexp(sql, r);
				if (f == sql_sel && exp_card(rs) > CARD_ATOM && r->card > CARD_ATOM && r->r) {
					sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(rs));
					rs = exp_aggr1(sql->sa, rs, zero_or_one, 0, 0, CARD_ATOM, 0);

					/* group by the right of the apply */
					r->r = rel_groupby(sql, r->r, NULL);
					rs = rel_groupby_add_aggr(sql, r->r, rs);
					rs = exp_column(sql->sa, exp_relname(rs), exp_name(rs), exp_subtype(rs), exp_card(rs), has_nil(rs), is_intern(rs));
				} else if (f == sql_sel && !r->r) {
					*rel = rel_project(sql->sa, *rel, new_exp_list(sql->sa));
				}
			}
			return rs;
		}
		if (!r)
			return NULL;
		return rel_find_lastexp(*rel);
	}
	case SQL_TABLE: {
		/* turn a subquery into a tabular result */
		*rel = rel_selects(sql, se->data.sym /*, *rel, se->data.sym, ek*/);
		if (*rel)
			return rel_find_lastexp(*rel);
		return NULL;
	}
	case SQL_PARAMETER:{
		if (sql->emode != m_prepare)
			return sql_error(sql, 02, "SELECT: parameters ('?') not allowed in normal queries, use PREPARE");
		assert(se->type == type_int);
		return exp_atom_ref(sql->sa, se->data.i_val, NULL);
	}
	case SQL_NULL:
		return exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("void"), NULL));
	case SQL_ATOM:{
		AtomNode *an = (AtomNode *) se;

		if (!an || !an->a) {
			return exp_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("void"), NULL));
		} else {
			return exp_atom(sql->sa, atom_dup(sql->sa, an->a));
		}
	}
	case SQL_NEXT:
		return rel_next_value_for(sql, se);
	case SQL_CAST:
		return rel_cast(sql, rel, se, f);
	case SQL_CASE:
	case SQL_COALESCE:
	case SQL_NULLIF:
		return rel_case_exp(sql, rel, se, f);
	case SQL_XMLELEMENT:
	case SQL_XMLFOREST:
	case SQL_XMLCOMMENT:
	case SQL_XMLATTRIBUTE:
	case SQL_XMLCONCAT:
	case SQL_XMLDOCUMENT:
	case SQL_XMLPI:
	case SQL_XMLTEXT:
		return rel_xml(sql, rel, se, f, ek);
	default:
		return rel_logical_value_exp(sql, rel, se, f);
	}
}

sql_exp *
rel_value_exp(mvc *sql, sql_rel **rel, symbol *se, int f, exp_kind ek)
{
	int is_last = 0;
	sql_exp *e;
	if (!se)
		return NULL;

	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	e = rel_value_exp2(sql, rel, se, f, ek, &is_last);
	if (e && (se->token == SQL_SELECT || se->token == SQL_TABLE) && !is_last) {
		assert(*rel);
		return rel_lastexp(sql, *rel);
	}
	return e;
}

static sql_exp *
column_exp(mvc *sql, sql_rel **rel, symbol *column_e, int f)
{
	dlist *l = column_e->data.lval;
	exp_kind ek = {type_value, card_column, FALSE};
	sql_exp *ve = rel_value_exp(sql, rel, l->h->data.sym, f, ek);

	if (!ve)
		return NULL;
	/* AS name */
	if (ve && l->h->next->data.sval)
		exp_setname(sql->sa, ve, NULL, l->h->next->data.sval);
	return ve;
}

static list *
rel_table_exp(mvc *sql, sql_rel **rel, symbol *column_e )
{
	if (column_e->token == SQL_TABLE && column_e->data.lval->h->type == type_symbol) {
		sql_rel *r;


		if (!is_project((*rel)->op))
			return NULL;
		r = rel_named_table_function( sql, (*rel)->l, column_e, 0);
	
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
				"Table expression without table name");
		return sql_error(sql, 02,
				"Column expression Table '%s' unknown", tname);
	}
	return NULL;
}

sql_exp *
rel_column_exp(mvc *sql, sql_rel **rel, symbol *column_e, int f)
{
	if (column_e->token == SQL_COLUMN) {
		return column_exp(sql, rel, column_e, f);
	}
	return NULL;
}

static sql_rel *
rel_simple_select(mvc *sql, sql_rel *rel, symbol *where, dlist *selection, int distinct)
{
	dnode *n = 0;
	sql_rel *inner;

	if (!selection)
		return sql_error(sql, 02, "SELECT: the selection or from part is missing");
	if (where) {
		sql_rel *r = rel_logical_exp(sql, rel, where, sql_where);
		if (!r)
			return NULL;
		rel = r;
	}
	if (!rel || rel->op != op_project)
		rel = rel_project(sql->sa, rel, new_exp_list(sql->sa));
	inner = rel;
	for (n = selection->h; n; n = n->next ) {
		/* Here we could get real column expressions (including single
		 * atoms) but also table results. Therefore we try both
		 * rel_column_exp and rel_table_exp.
		 */
		sql_rel *o_inner = inner;
	       	list *te = NULL, *pre_prj = rel_projections(sql, o_inner, NULL, 1, 1);
		sql_exp *ce = rel_column_exp(sql, &inner, n->data.sym, sql_sel);

		if (inner != o_inner) {  /* relation got rewritten */
			if (!inner)
				return NULL;
			rel = inner;
		}

		if (ce && exp_subtype(ce)) {
			/* new relational, we need to rewrite */
			if (!is_project(inner->op)) {
				if (inner != o_inner && pre_prj) 
					inner = rel_project(sql->sa, inner, pre_prj);
				else
					inner = rel_project(sql->sa, inner, new_exp_list(sql->sa));
			}
			rel_project_add_exp(sql, inner, ce);
			rel = inner;
			continue;
		} else if (!ce) {
			te = rel_table_exp(sql, &rel, n->data.sym );
		} else 
			ce = NULL;
		if (!ce && !te)
			return sql_error(sql, 02, "SELECT: subquery result missing");
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
join_on_column_name(mvc *sql, sql_rel *rel, sql_rel *t1, sql_rel *t2, int op, int l_nil, int r_nil)
{
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
		const char *nm = le->name;
		sql_exp *re = exps_bind_column(r_exps, nm, NULL);

		if (re) {
			found = 1;
			rel = rel_compare_exp(sql, rel, le, re, "=", NULL, TRUE);
			if (full) {
				sql_exp *cond;
				cond = rel_unop_(sql, le, NULL, "isnull", card_value);
				le = rel_nop_(sql, cond, re, le, NULL, NULL, "ifthenelse", card_value);
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
		sql_error(sql, 02, "JOIN: no columns of tables '%s' and '%s' match", rel_name(t1)?rel_name(t1):"", rel_name(t2)?rel_name(t2):"");
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
	set_processed(rel);
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
rel_select_exp(mvc *sql, sql_rel *rel, SelectNode *sn, exp_kind ek)
{
	dnode *n;
	int aggr = 0;
	sql_rel *inner = NULL;

	assert(sn->s.token == SQL_SELECT);
	if (!sn->selection)
		return sql_error(sql, 02, "SELECT: the selection or from part is missing");

	if (!sn->from)
		return rel_simple_select(sql, rel, sn->where, sn->selection, sn->distinct);

	if (sn->where) {
		sql_rel *r = rel_logical_exp(sql, rel, sn->where, sql_where);
		if (!r) {
			if (sql->errstr[0] == 0)
				return sql_error(sql, 02, "Subquery result missing");
			return NULL;
		}
		rel = r;
	}

	if (rel) {
		if (rel && sn->groupby) {
			list *gbe = rel_group_by(sql, &rel, sn->groupby, sn->selection, sql_sel );

			if (!gbe)
				return NULL;
			rel = rel_groupby(sql, rel, gbe);
			aggr = 1;
		}
		if (!sn->having)
			set_processed(rel);
	}

	if (sn->having) {
		/* having implies group by, ie if not supplied do a group by */
		if (rel->op != op_groupby)
			rel = rel_groupby(sql,  rel, NULL);
		aggr = 1;
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
	       	list *te = NULL, *pre_prj = rel_projections(sql, o_inner, NULL, 1, 1);
		sql_exp *ce = rel_column_exp(sql, &inner, n->data.sym, sql_sel);

		if (inner != o_inner) {  /* relation got rewritten */
			if (!inner)
				return NULL;
			if (is_groupby(inner->op))
				aggr = 1;
			rel = inner;
		}

		if (ce && exp_subtype(ce)) {
			if (rel->card < ce->card) {
				if (ce->name) {
					return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column '%s' in query results without an aggregate function", ce->name);
				} else {
					return sql_error(sql, 02, "SELECT: cannot use non GROUP BY column in query results without an aggregate function");
				}
			}
			/*
			   because of the selection, the inner
			   relation may change.
			   We try hard to keep a projection
			   around this inner relation.
			*/
			if (!is_project(inner->op)) {
				if (inner != o_inner && pre_prj) 
					inner = rel_project(sql->sa, inner, pre_prj);
				else
					inner = rel_project(sql->sa, inner, new_exp_list(sql->sa));
			}
			rel_project_add_exp(sql, inner, ce);
			rel = inner;
			continue;
		} else if (!ce) {
			te = rel_table_exp(sql, &rel, n->data.sym);
		} else 
			ce = NULL;
		if (!ce && !te) {
			if (sql->errstr[0])
				return NULL;
			return sql_error(sql, 02, "SELECT: subquery result missing");
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
	
		inner = rel_logical_exp(sql, inner, sn->having, sql_having);

		if (!inner)
			return NULL;
		if (inner -> exps && exps_card(inner->exps) > CARD_AGGR)
			return sql_error(sql, 02, "SELECT: cannot compare sets with values, probably an aggregate function missing");
		rel -> l = inner;
	}
	if (rel)
		set_processed(rel);
	if (aggr && rel) {
		sql_rel *l = rel;
		while(l && !is_groupby(l->op))
			if (is_project(l->op) || is_select(l->op))
				l = l -> l;
			else
				l = NULL;
		if (l)
			set_processed(l);
	}

	if (rel && sn->distinct)
		rel = rel_distinct(rel);

	if (rel && sn->orderby) {
		list *obe = NULL;

		rel = rel_orderby(sql, rel);
		obe = rel_order_by(sql, &rel, sn->orderby, sql_orderby);
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
			sql_exp *l = rel_value_exp( sql, NULL, sn->limit, 0, ek);

			if (!l || !(l=rel_check_type(sql, lng, l, type_equal)))
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
			sql_exp *o = rel_value_exp( sql, NULL, sn->offset, 0, ek);
			if (!o || !(o=rel_check_type(sql, lng, o, type_equal)))
				return NULL;
			append(exps, o);
		}
		rel = rel_topn(sql->sa, rel, exps);
	}

	if (sn->sample) {
		list *exps = new_exp_list(sql->sa);
		sql_exp *o = rel_value_exp( sql, NULL, sn->sample, 0, ek);
		if (!o)
			return NULL;
		append(exps, o);
		rel = rel_sample(sql->sa, rel, exps);
	}

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
rel_query(mvc *sql, sql_rel *rel, symbol *sq, int toplevel, exp_kind ek, int apply)
{
	sql_rel *res = NULL, *outer = NULL;
	list *applyexps = NULL;
	SelectNode *sn = NULL;
	int used = 0;
	int old = sql->use_views;

	if (sq->token != SQL_SELECT)
		return table_ref(sql, rel, sq, 0);

	/* select ... into is currently not handled here ! */
 	sn = (SelectNode *) sq;
	if (sn->into)
		return NULL;

	if (ek.card != card_relation && sn->orderby)
		return sql_error(sql, 01, "SELECT: ORDER BY only allowed on outermost SELECT");


	sql->use_views = 1;
	if (sn->from) {		/* keep variable list with tables and names */
		dlist *fl = sn->from->data.lval;
		dnode *n = NULL;
		sql_rel *fnd = NULL;

		for (n = fl->h; n ; n = n->next) {
			int lateral = check_is_lateral(n->data.sym);

			fnd = table_ref(sql, NULL, n->data.sym, 0);
			if (!fnd && (rel || lateral) && sql->session->status != -ERR_AMBIGUOUS) {
				/* reset error */
				sql->session->status = 0;
				sql->errstr[0] = 0;
				if (used && rel)
					rel = rel_dup(rel);
				if (!used && (!sn->lateral && !lateral) && rel) {
					sql_rel *o = rel;

					/* remove the outer (running) project */
					if (!is_processed(o) && is_project(o->op))
						o = rel->l;
					outer = rel;
					/* create dummy single row project */
					rel = rel_project(sql->sa, NULL, applyexps = rel_projections(sql, o, NULL, 1, 1)); 
				}
				if (lateral) {
					list *pre_exps = rel_projections(sql, res, NULL, 1, 1);
					fnd = table_ref(sql, res, n->data.sym, lateral);
					if (fnd && is_project(fnd->op)) 
						fnd->exps = list_merge(fnd->exps, pre_exps, (fdup)NULL);
				} else {
					fnd = table_ref(sql, rel, n->data.sym, 0);
				}
				used = 1;
			}

			if (!fnd)
				break;
			if (res && !lateral)
				res = rel_crossproduct(sql->sa, res, fnd, op_join);
			else
				res = fnd;
		}
		if (!fnd) {
			if (res)
				rel_destroy(res);
			return NULL;
		}
		if (rel && !used && !sn->lateral) {
			sql_rel *o = rel;

			/* remove the outer (running) project */
			if (!is_processed(o) && is_project(o->op))
				o = rel->l;
			rel_setsubquery(res);
			outer = rel;
			/* create dummy single row project */
			rel = rel_project(sql->sa, NULL, applyexps = rel_projections(sql, o, NULL, 1, 1)); 
			res = rel_crossproduct(sql->sa, rel, res, op_join);
		} else if (rel && !used && sn->lateral) {
			res = rel_crossproduct(sql->sa, rel, res, op_join);
		}
	} else if (toplevel || !res) {	/* only on top level query */
		sql->use_views = old;
		return rel_simple_select(sql, rel, sn->where, sn->selection, sn->distinct);
	}
	sql->use_views = old;
	if (res)
		rel = rel_select_exp(sql, res, sn, ek);
	if (rel && outer) {
		if (apply == APPLY_EXISTS || apply == APPLY_NOTEXISTS) {
			/* remove useless project */
			if (is_project(rel->op))
				rel = rel->l;
		}
		/* remove empty projects */
		if (is_project(outer->op) && (!outer->exps || list_length(outer->exps) == 0)) 
			outer = outer->l;
		/* add all columns to the outer (running) project */
		else if (!is_processed(outer) && is_project(outer->op)) {
			list *exps = rel_projections(sql, outer->l, NULL, 1, 1 );

			list_merge(outer->exps, exps, (fdup)NULL);
			outer->exps = list_distinct(outer->exps, (fcmp)exp_equal, (fdup)NULL);
		}
		rel = rel_crossproduct(sql->sa, outer, rel, op_apply);
		rel->exps = applyexps;
		rel->flag = apply;
	}
	return rel;
}

static sql_rel *
rel_setquery_(mvc *sql, sql_rel *l, sql_rel *r, dlist *cols, int op )
{
	sql_rel *rel;

	if (!cols) {
		list *ls, *rs, *nls, *nrs;
		node *n, *m;
		int changes = 0;

		l = rel_unique_names(sql, l);
		r = rel_unique_names(sql, r);
		ls = rel_projections(sql, l, NULL, 0, 1);
		rs = rel_projections(sql, r, NULL, 0, 1);
		nls = new_exp_list(sql->sa);
		nrs = new_exp_list(sql->sa);
		for (n = ls->h, m = rs->h; n && m; n = n->next, m = m->next) {
			sql_exp *le = n->data, *lb = le;
			sql_exp *re = m->data, *rb = re;

			if ((rel_convert_types(sql, &le, &re, 1, type_set) < 0))
				return NULL;
			if (le != lb || re != rb)
				changes = 1;
			append(nls, le);
			append(nrs, re);
		}
		if (changes) {
			l = rel_project(sql->sa, l, nls);
			r = rel_project(sql->sa, r, nrs);
			set_processed(l);
			set_processed(r);
		}
	}
	rel = rel_setop(sql->sa, l, r, (operator_type)op);
	rel->exps = rel_projections(sql, rel, NULL, 0, 1);
	set_processed(rel);
	return rel;
}


static sql_rel *
rel_setquery(mvc *sql, sql_rel *rel, symbol *q)
{
	sql_rel *res = NULL;
	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int dist = n->next->data.i_val, used = 0;
	dlist *corresponding = n->next->next->data.lval;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	sql_rel *t1, *t2; 

	assert(n->next->type == type_int);
	t1 = table_ref(sql, NULL, tab_ref1, 0);
	if (rel && !t1 && sql->session->status != -ERR_AMBIGUOUS) {
		sql_rel *r = rel;

		r = rel_project( sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
		set_processed(r);
		used = 1;

		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = 0;
		t1 = table_ref(sql, r, tab_ref1, 0);
	}
	if (!t1)
		return NULL;
	t2 = table_ref(sql, NULL, tab_ref2, 0);
	if (rel && !t2 && sql->session->status != -ERR_AMBIGUOUS) {
		sql_rel *r = rel;

		if (used)
			rel = rel_dup(rel);
		r = rel_project( sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
		set_processed(r);

		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = 0;
		t2 = table_ref(sql, r, tab_ref2, 0);
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
		return sql_error(sql, 02, "%s: column counts (%d and %d) do not match", op, t1nrcols, t2nrcols);
	}
	if ( q->token == SQL_UNION) {
		/* For EXCEPT/INTERSECT the group by is always done within the implementation */
		if (t1 && dist)
			t1 = rel_distinct(t1);
		if (t2 && dist)
			t2 = rel_distinct(t2);
		res = rel_setquery_(sql, t1, t2, corresponding, op_union );
	}
	if ( q->token == SQL_EXCEPT)
		res = rel_setquery_(sql, t1, t2, corresponding, op_except );
	if ( q->token == SQL_INTERSECT)
		res = rel_setquery_(sql, t1, t2, corresponding, op_inter );
	if (res && dist)
		res = rel_distinct(res);
	return res;
}



static sql_rel *
rel_joinquery_(mvc *sql, sql_rel *rel, symbol *tab1, int natural, jt jointype, symbol *tab2, symbol *js)
{
	operator_type op = op_join;
	sql_rel *t1 = NULL, *t2 = NULL, *inner;
	int l_nil = 0, r_nil = 0, lateral = 0;

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
	t1 = table_ref(sql, rel, tab1, 0);
	if (t1) {
		if (lateral) {
			list *pre_exps = rel_projections(sql, t1, NULL, 1, 1);
			t2 = table_ref(sql, t1, tab2, 0);
			if (t2) {
		 		if (!is_project(t2->op))
					assert(0);
				t2->exps = list_merge(t2->exps, pre_exps, (fdup)NULL);
			}
		} else {
			t2 = table_ref(sql, rel, tab2, 0);
		}
	}
	if (!t1 || !t2)
		return NULL;

	if (!lateral && rel_name(t1) && rel_name(t2) && strcmp(rel_name(t1), rel_name(t2)) == 0) {
		sql_error(sql, 02, "SELECT: '%s' on both sides of the JOIN expression;", rel_name(t1));
		rel_destroy(t1);
		rel_destroy(t2);
		return NULL;
	}

	if (!lateral) {
		inner = rel = rel_crossproduct(sql->sa, t1, t2, op_join);
	} else {
		inner = rel = t2;
		/* find join */
		while (is_project(inner->op))
			inner = inner->l;
	}
	inner->op = op;

	if (js && natural) {
		return sql_error(sql, 02, "SELECT: cannot have a NATURAL JOIN with a join specification (ON or USING);");
	}
	if (!js && !natural) {
		return sql_error(sql, 02, "SELECT: must have NATURAL JOIN or a JOIN with a join specification (ON or USING);");
	}

	if (js && js->token != SQL_USING) {	/* On sql_logical_exp */
		rel = rel_logical_exp(sql, rel, js, sql_where);
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
				sql_error(sql, 02, "JOIN: tables '%s' and '%s' do not have a matching column '%s'\n", rel_name(t1)?rel_name(t1):"", rel_name(t2)?rel_name(t2):"", nm);
				rel_destroy(rel);
				return NULL;
			}
			rel = rel_compare_exp(sql, rel, ls, rs, "=", NULL, TRUE);
			if (op != op_join) {
				cond = rel_unop_(sql, ls, NULL, "isnull", card_value);
				if (rel_convert_types(sql, &ls, &rs, 1, type_equal) < 0)
					return NULL;
				ls = rel_nop_(sql, cond, rs, ls, NULL, NULL, "ifthenelse", card_value);
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
		rel = join_on_column_name(sql, rel, t1, t2, op, l_nil, r_nil);
	}
	if (!rel)
		return NULL;
	if (inner && is_outerjoin(inner->op))
		set_processed(inner);
	set_processed(rel);
	return rel;
}

static sql_rel *
rel_joinquery(mvc *sql, sql_rel *rel, symbol *q)
{

	dnode *n = q->data.lval->h;
	symbol *tab_ref1 = n->data.sym;
	int natural = n->next->data.i_val;
	jt jointype = (jt) n->next->next->data.i_val;
	symbol *tab_ref2 = n->next->next->next->data.sym;
	symbol *joinspec = n->next->next->next->next->data.sym;

	assert(n->next->type == type_int);
	assert(n->next->next->type == type_int);
	return rel_joinquery_(sql, rel, tab_ref1, natural, jointype, tab_ref2, joinspec);
}

static sql_rel *
rel_crossquery(mvc *sql, sql_rel *rel, symbol *q)
{
	dnode *n = q->data.lval->h;
	symbol *tab1 = n->data.sym;
	symbol *tab2 = n->next->data.sym;
	sql_rel *t1 = table_ref(sql, rel, tab1, 0);
	sql_rel *t2 = NULL;
       
	if (t1)
		t2 = table_ref(sql, rel, tab2, 0);
	if (!t1 || !t2)
		return NULL;

	rel = rel_crossproduct(sql->sa, t1, t2, op_join);
	return rel;
}
	
static sql_rel *
rel_unionjoinquery(mvc *sql, sql_rel *rel, symbol *q)
{
	dnode *n = q->data.lval->h;
	sql_rel *lv = table_ref(sql, rel, n->data.sym, 0);
	sql_rel *rv = NULL;
	int all = n->next->data.i_val;
	list *lexps, *rexps;
	node *m;
	int found = 0;

	if (lv)
       		rv = table_ref(sql, rel, n->next->next->data.sym, 0);
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
		sql_exp *rc = rel_bind_column(sql, rv, le->name, sql_where);
			
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
	set_processed(lv);
	set_processed(rv);
	rel = rel_setop(sql->sa, lv, rv, op_union);
	rel->exps = rel_projections(sql, rel, NULL, 0, 1);
	set_processed(rel);
	if (!all)
		rel = rel_distinct(rel);
	return rel;
}

sql_rel *
rel_subquery(mvc *sql, sql_rel *rel, symbol *sq, exp_kind ek, int apply)
{
	int toplevel = 0;

	if (!rel || (rel->op == op_project &&
		(!rel->exps || list_length(rel->exps) == 0)))
		toplevel = 1;

	return rel_query(sql, rel, sq, toplevel, ek, apply);
}

sql_rel *
rel_selects(mvc *sql, symbol *s)
{
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_WITH:
		ret = rel_with_query(sql, s);
		sql->type = Q_TABLE;
		break;
	case SQL_SELECT: {
		exp_kind ek = {type_value, card_relation, TRUE};
 		SelectNode *sn = (SelectNode *) s;

		if (sn->into) {
			sql->type = Q_SCHEMA;
			return rel_select_with_into(sql, s);
		}
		ret = rel_subquery(sql, NULL, s, ek, APPLY_JOIN);
		sql->type = Q_TABLE;
	}	break;
	case SQL_JOIN:
		ret = rel_joinquery(sql, NULL, s);
		sql->type = Q_TABLE;
		break;
	case SQL_CROSS:
		ret = rel_crossquery(sql, NULL, s);
		sql->type = Q_TABLE;
		break;
	case SQL_UNION:
	case SQL_EXCEPT:
	case SQL_INTERSECT:
		ret = rel_setquery(sql, NULL, s);
		sql->type = Q_TABLE;
		break;
	default:
		return NULL;
	}
	if (!ret && sql->errstr[0] == 0)
		(void) sql_error(sql, 02, "relational query without result");
	return ret;
}

sql_rel *
schema_selects(mvc *sql, sql_schema *schema, symbol *s)
{
	sql_rel *res;
	sql_schema *os = sql->session->schema;

	sql->session->schema = schema;
	res = rel_selects(sql, s);
	sql->session->schema = os;
	return res;
}

