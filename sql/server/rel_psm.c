/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_psm.h"
#include "rel_semantic.h"
#include "rel_schema.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_updates.h"
#include "sql_privileges.h"

static list *sequential_block(mvc *sql, sql_subtype *restype, list *restypelist, dlist *blk, char *opt_name, int is_func);

static sql_rel *
rel_psm_block(sql_allocator *sa, list *l)
{
	if (l) {
		sql_rel *r = rel_create(sa);
		if(!r)
			return NULL;

		r->op = op_ddl;
		r->flag = DDL_PSM;
		r->exps = l;
		return r;
	}
	return NULL;
}

sql_rel *
rel_psm_stmt(sql_allocator *sa, sql_exp *e)
{
	if (e) {
		list *l = sa_list(sa);
		if(!l)
			return NULL;

		list_append(l, e);
		return rel_psm_block(sa, l);
	}
	return NULL;
}

/* SET variable = value and set (variable1, .., variableN) = (query) */
static sql_exp *
psm_set_exp(mvc *sql, dnode *n)
{
	symbol *val = n->next->data.sym;
	sql_exp *e = NULL;
	int level = 0, is_last = 0;
	sql_subtype *tpe = NULL;
	sql_rel *rel = NULL;
	sql_exp *res = NULL;
	int single = (n->type == type_string);


	if (single) {
		exp_kind ek = {type_value, card_value, FALSE};
		const char *name = n->data.sval;
		/* name can be 
			'parameter of the function' (ie in the param list)
			or a local or global variable, declared earlier
		*/

		/* check if variable is known from the stack */
		if (!stack_find_var(sql, name)) {
			sql_arg *a = sql_bind_param(sql, name);

			if (!a) /* not parameter, ie local var ? */
				return sql_error(sql, 01, "Variable %s unknown", name);
			tpe = &a->type;
		} else { 
			tpe = stack_find_type(sql, name);
		}

		e = rel_value_exp2(sql, &rel, val, sql_sel, ek, &is_last);
		if (!e || (rel && e->card > CARD_AGGR))
			return NULL;

		level = stack_find_frame(sql, name);
		e = rel_check_type(sql, tpe, e, type_cast); 
		if (!e)
			return NULL;
		if (rel) {
			sql_exp *er = exp_rel(sql, rel);
			list *b = sa_list(sql->sa);

			append(b, er);
			append(b, exp_set(sql->sa, name, e, level));
			res = exp_rel(sql, rel_psm_block(sql->sa, b));
		} else {
			res = exp_set(sql->sa, name, e, level);
		}
	} else { /* multi assignment */
		exp_kind ek = {type_value, (single)?card_column:card_relation, FALSE};
		sql_rel *rel_val = rel_subquery(sql, NULL, val, ek, APPLY_JOIN);
		dlist *vars = n->data.lval;
		dnode *m;
		node *n;
		list *b;

		if (!rel_val || !is_project(rel_val->op) ||
			    dlist_length(vars) != list_length(rel_val->exps)) {
			return sql_error(sql, 02, "SET: Number of variables not equal to number of supplied values");
		}

	       	b = sa_list(sql->sa);
		if (rel_val) {
			sql_exp *er = exp_rel(sql, rel_val);

			append(b, er);
		}

		for(m = vars->h, n = rel_val->exps->h; n && m; n = n->next, m = m->next) {
			char *vname = m->data.sval;
			sql_exp *v = n->data;

			if (!stack_find_var(sql, vname)) {
				sql_arg *a = sql_bind_param(sql, vname);

				if (!a) /* not parameter, ie local var ? */
					return sql_error(sql, 01, "Variable %s unknown", vname);
				tpe = &a->type;
			} else { 
				tpe = stack_find_type(sql, vname);
			}

			if (!exp_name(v))
				exp_label(sql->sa, v, ++sql->label);
			v = exp_column(sql->sa, exp_relname(v), exp_name(v), exp_subtype(v), v->card, has_nil(v), is_intern(v));

			level = stack_find_frame(sql, vname);
			v = rel_check_type(sql, tpe, v, type_cast); 
			if (!v)
				return NULL;
			if (v->card > CARD_AGGR) {
				sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(v));
				assert(zero_or_one);
				v = exp_aggr1(sql->sa, v, zero_or_one, 0, 0, CARD_ATOM, 0);
			}
			append(b, exp_set(sql->sa, vname, v, level));
		}
		res = exp_rel(sql, rel_psm_block(sql->sa, b));
	}
	return res;
}

static sql_exp*
rel_psm_call(mvc * sql, symbol *se)
{
	sql_subtype *t;
	sql_exp *res = NULL;
	exp_kind ek = {type_value, card_none, FALSE};
	sql_rel *rel = NULL;

	res = rel_value_exp(sql, &rel, se, sql_sel, ek);
	if (!res || rel || ((t=exp_subtype(res)) && t->type))  /* only procedures */
		return sql_error(sql, 01, "function calls are ignored");
	return res;
}

static list *
rel_psm_declare(mvc *sql, dnode *n)
{
	list *l = sa_list(sql->sa);

	while(n) { /* list of 'identfiers with type' */
		dnode *ids = n->data.sym->data.lval->h->data.lval->h;
		sql_subtype *ctype = &n->data.sym->data.lval->h->next->data.typeval;
		while(ids) {
			const char *name = ids->data.sval;
			sql_exp *r = NULL;

			/* check if we overwrite a scope local variable declare x; declare x; */
			if (frame_find_var(sql, name)) {
				return sql_error(sql, 01, 
					"Variable '%s' already declared", name);
			}
			/* variables are put on stack, 
 			 * TODO make sure on plan/explain etc they only 
 			 * exist during plan phase */
			stack_push_var(sql, name, ctype);
			r = exp_var(sql->sa, sa_strdup(sql->sa, name), ctype, sql->frame);
			append(l, r);
			ids = ids->next;
		}
		n = n->next;
	}
	return l;
}

static sql_exp *
rel_psm_declare_table(mvc *sql, dnode *n)
{
	sql_rel *rel = NULL;
	dlist *qname = n->next->data.lval;
	const char *name = qname_table(qname);
	const char *sname = qname_schema(qname);
	sql_table *t;

	if (sname)  /* not allowed here */
		return sql_error(sql, 02, "DECLARE TABLE: qualified name not allowed");
	if (frame_find_var(sql, name)) 
		return sql_error(sql, 01, "Variable '%s' already declared", name);
	
	assert(n->next->next->next->type == type_int);
	
	rel = rel_create_table(sql, cur_schema(sql), SQL_DECLARED_TABLE, NULL, name, n->next->next->data.sym, n->next->next->next->data.i_val, NULL, 0);

	if (!rel || rel->op != op_ddl || rel->flag != DDL_CREATE_TABLE)
		return NULL;

	t = (sql_table*)((atom*)((sql_exp*)rel->exps->t->data)->l)->data.val.pval;
	stack_push_table(sql, name, rel, t);
	return exp_table(sql->sa, sa_strdup(sql->sa, name), t, sql->frame);
}

/* [ label: ]
   while (cond) do 
	statement_list
   end [ label ]
   currently we only parse the labels, they cannot be used as there is no

   support for LEAVE and ITERATE (sql multi-level break and continue)
 */
static sql_exp * 
rel_psm_while_do( mvc *sql, sql_subtype *res, list *restypelist, dnode *w, int is_func )
{
	if (!w)
		return NULL;
	if (w->type == type_symbol) { 
		sql_exp *cond;
		list *whilestmts;
		dnode *n = w;
		sql_rel *rel = NULL;

		cond = rel_logical_value_exp(sql, &rel, n->data.sym, sql_sel); 
		n = n->next;
		whilestmts = sequential_block(sql, res, restypelist, n->data.lval, n->next->data.sval, is_func);

		if (sql->session->status || !cond || !whilestmts) 
			return NULL;
		if (rel) {
			sql_exp *er = exp_rel(sql, rel);
			list *b = sa_list(sql->sa);

			append(b, er);
			append(b, exp_while( sql->sa, cond, whilestmts ));
			return exp_rel(sql, rel_psm_block(sql->sa, b));
		}
		return exp_while( sql->sa, cond, whilestmts );
	}
	return NULL;
}


/* if (cond) then statement_list
   [ elseif (cond) then statement_list ]*
   [ else statement_list ]
   end if
 */
static list * 
psm_if_then_else( mvc *sql, sql_subtype *res, list *restypelist, dnode *elseif, int is_func)
{
	if (!elseif)
		return NULL;
	assert(elseif->type == type_symbol); 
	if (elseif->data.sym && elseif->data.sym->token == SQL_IF) {
		sql_exp *cond;
		list *ifstmts, *elsestmts;
		dnode *n = elseif->data.sym->data.lval->h;
		sql_rel *rel = NULL;

		cond = rel_logical_value_exp(sql, &rel, n->data.sym, sql_sel); 
		n = n->next;
		ifstmts = sequential_block(sql, res, restypelist, n->data.lval, NULL, is_func);
		n = n->next;
		elsestmts = psm_if_then_else( sql, res, restypelist, n, is_func);

		if (sql->session->status || !cond || !ifstmts) 
			return NULL;
		if (rel) {
			sql_exp *er = exp_rel(sql, rel);
			list *b = sa_list(sql->sa);

			append(b, er);
			append(b, exp_if(sql->sa, cond, ifstmts, elsestmts));
			return b;
		}
		return append(sa_list(sql->sa), exp_if( sql->sa, cond, ifstmts, elsestmts));
	} else { /* else */
		symbol *e = elseif->data.sym;

		if (e==NULL || (e->token != SQL_ELSE))
			return NULL;
		return sequential_block( sql, res, restypelist, e->data.lval, NULL, is_func);
	}
}

static sql_exp * 
rel_psm_if_then_else( mvc *sql, sql_subtype *res, list *restypelist, dnode *elseif, int is_func)
{
	if (!elseif)
		return NULL;
	if (elseif->next && elseif->type == type_symbol) { /* if or elseif */
		sql_exp *cond;
		list *ifstmts, *elsestmts;
		dnode *n = elseif;
		sql_rel *rel = NULL;

		cond = rel_logical_value_exp(sql, &rel, n->data.sym, sql_sel); 
		n = n->next;
		ifstmts = sequential_block(sql, res, restypelist, n->data.lval, NULL, is_func);
		n = n->next;
		elsestmts = psm_if_then_else( sql, res, restypelist, n, is_func);
		if (sql->session->status || !cond || !ifstmts) 
			return NULL;
		if (rel) {
			sql_exp *er = exp_rel(sql, rel);
			list *b = sa_list(sql->sa);

			append(b, er);
			append(b, exp_if(sql->sa, cond, ifstmts, elsestmts));
			return exp_rel(sql, rel_psm_block(sql->sa, b));
		}
		return exp_if( sql->sa, cond, ifstmts, elsestmts);
	}
	return NULL;
}

/* 	1
	CASE
	WHEN search_condition THEN statements
	[ WHEN search_condition THEN statements ]
	[ ELSE statements ]
	END CASE

	2
	CASE case_value
	WHEN when_value THEN statements
	[ WHEN when_value THEN statements ]
	[ ELSE statements ]
	END CASE
 */
static list * 
rel_psm_case( mvc *sql, sql_subtype *res, list *restypelist, dnode *case_when, int is_func )
{
	list *case_stmts = sa_list(sql->sa);

	if (!case_when)
		return NULL;

	/* case 1 */
	if (case_when->type == type_symbol) {
		dnode *n = case_when;
		symbol *case_value = n->data.sym;
		dlist *when_statements = n->next->data.lval;
		dlist *else_statements = n->next->next->data.lval;
		list *else_stmt = NULL;
		sql_rel *rel = NULL;
		exp_kind ek = {type_value, card_value, FALSE};
		sql_exp *v = rel_value_exp(sql, &rel, case_value, sql_sel, ek);

		if (!v)
			return NULL;
		if (rel)
			return sql_error(sql, 02, "CASE: No SELECT statements allowed within the CASE condition");
		if (else_statements) {
			else_stmt = sequential_block( sql, res, restypelist, else_statements, NULL, is_func);
			if (!else_stmt) 
				return NULL;
		}
		n = when_statements->h;
		while(n) {
			dnode *m = n->data.sym->data.lval->h;
			sql_exp *cond=0, *when_value = rel_value_exp(sql, &rel, m->data.sym, sql_sel, ek);
			list *if_stmts = NULL;
			sql_exp *case_stmt = NULL;

			if (!when_value || rel ||
			   (cond = rel_binop_(sql, v, when_value, NULL, "=", card_value)) == NULL || 
			   (if_stmts = sequential_block( sql, res, restypelist, m->next->data.lval, NULL, is_func)) == NULL ) {
				if (rel)
					return sql_error(sql, 02, "CASE: No SELECT statements allowed within the CASE condition");
				return NULL;
			}
			case_stmt = exp_if(sql->sa, cond, if_stmts, NULL);
			list_append(case_stmts, case_stmt);
			n = n->next;
		}
		if (else_stmt)
			list_merge(case_stmts, else_stmt, NULL);
		return case_stmts;
	} else { 
		/* case 2 */
		dnode *n = case_when;
		dlist *whenlist = n->data.lval;
		dlist *else_statements = n->next->data.lval;
		list *else_stmt = NULL;

		if (else_statements) {
			else_stmt = sequential_block( sql, res, restypelist, else_statements, NULL, is_func);
			if (!else_stmt) 
				return NULL;
		}
		n = whenlist->h;
		while(n) {
			dnode *m = n->data.sym->data.lval->h;
			sql_rel *rel = NULL;
			sql_exp *cond = rel_logical_value_exp(sql, &rel, m->data.sym, sql_sel);
			list *if_stmts = NULL;
			sql_exp *case_stmt = NULL;

			if (!cond || rel ||
			   (if_stmts = sequential_block( sql, res, restypelist, m->next->data.lval, NULL, is_func)) == NULL ) {
				if (rel)
					return sql_error(sql, 02, "CASE: No SELECT statements allowed within the CASE condition");
				return NULL;
			}
			case_stmt = exp_if(sql->sa, cond, if_stmts, NULL);
			list_append(case_stmts, case_stmt);
			n = n->next;
		}
		if (else_stmt)
			list_merge(case_stmts, else_stmt, NULL);
		return case_stmts;
	}
}

/* return val;
 */
static list * 
rel_psm_return( mvc *sql, sql_subtype *restype, list *restypelist, symbol *return_sym )
{
	exp_kind ek = {type_value, card_value, FALSE};
	sql_exp *res;
	sql_rel *rel = NULL;
	int is_last = 0;
	list *l = sa_list(sql->sa);

	if (restypelist)
		ek.card = card_relation;
	res = rel_value_exp2(sql, &rel, return_sym, sql_sel, ek, &is_last);
	if (!res)
		return NULL;
	if (ek.card != card_relation && (!res || !restype ||
           	(res = rel_check_type(sql, restype, res, type_equal)) == NULL))
		return (!restype)?sql_error(sql, 02, "RETURN: return type does not match"):NULL;
	else if (ek.card == card_relation && !rel)
		return NULL;
	
	if (rel && ek.card != card_relation)
		append(l, exp_rel(sql, rel));
	else if (rel && !is_ddl(rel->op)) {
		list *exps = sa_list(sql->sa);
		node *n, *m;
		int isproject = (rel->op == op_project);
		list *oexps = rel->exps;
		sql_rel *l = rel->l;

		if (is_topn(rel->op) || is_sample(rel->op))
			oexps = l->exps;
		for (n = oexps->h, m = restypelist->h; n && m; n = n->next, m = m->next) {
			sql_exp *e = n->data;
			sql_arg *ce = m->data;
			const char *cname = exp_name(e);
			char name[16];

			if (!cname)
				cname = sa_strdup(sql->sa, number2name(name, 16, ++sql->label));
			if (!isproject) 
				e = exp_column(sql->sa, exp_relname(e), cname, exp_subtype(e), exp_card(e), has_nil(e), is_intern(e));
			e = rel_check_type(sql, &ce->type, e, type_equal);
			if (!e)
				return NULL;
			append(exps, e);
		}
		if (isproject)
			rel -> exps = exps;
		else
			rel = rel_project(sql->sa, rel, exps);
		res = exp_rel(sql, rel);
	} else if (rel && restypelist){ /* handle return table-var */
		list *exps = sa_list(sql->sa);
		sql_table *t = rel_ddl_table_get(rel);
		node *n, *m;
		const char *tname = t->base.name;

		if (cs_size(&t->columns) != list_length(restypelist))
			return sql_error(sql, 02, "RETURN: number of columns do not match");
		for (n = t->columns.set->h, m = restypelist->h; n && m; n = n->next, m = m->next) {
			sql_column *c = n->data;
			sql_arg *ce = m->data;
			sql_exp *e = exp_alias(sql->sa, tname, c->base.name, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0);

			e = rel_check_type(sql, &ce->type, e, type_equal);
			if (!e)
				return NULL;
			append(exps, e);
		}
		rel = rel_project(sql->sa, rel, exps);
		res = exp_rel(sql, rel);
	}
	append(l, exp_return(sql->sa, res, stack_nr_of_declared_tables(sql)));
	return l;
}

static list *
rel_select_into( mvc *sql, symbol *sq, exp_kind ek)
{
	SelectNode *sn = (SelectNode*)sq;
	dlist *into = sn->into;
	node *m;
	dnode *n;
	sql_rel *r;
	list *nl = NULL;

	/* SELECT ... INTO var_list */
	sn->into = NULL;
	r = rel_subquery(sql, NULL, sq, ek, APPLY_JOIN);
	if (!r) 
		return NULL;
	nl = sa_list(sql->sa);
	append(nl, exp_rel(sql, r));
	for (m = r->exps->h, n = into->h; m && n; m = m->next, n = n->next) {
		sql_subtype *tpe = NULL;
		char *nme = n->data.sval;
		sql_exp *v = m->data;
		int level;

		if (!stack_find_var(sql, nme)) 
			return sql_error(sql, 02, "SELECT INTO: variable '%s' unknown", nme);
		/* dynamic check for single values */
		if (v->card > CARD_AGGR) {
			sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", exp_subtype(v));
			assert(zero_or_one);
			v = exp_aggr1(sql->sa, v, zero_or_one, 0, 0, CARD_ATOM, 0);
		}
		tpe = stack_find_type(sql, nme);
		level = stack_find_frame(sql, nme);
		if (!v || !(v = rel_check_type(sql, tpe, v, type_equal))) 
			return NULL;
		v = exp_set(sql->sa, nme, v, level);
		list_append(nl, v);
	}
	return nl;
}

extern sql_rel *
rel_select_with_into(mvc *sql, symbol *sq)
{
	exp_kind ek = {type_value, card_row, TRUE};
	list *reslist = rel_select_into(sql, sq, ek);
	if (!reslist)
		return NULL;
	return rel_psm_block(sql->sa, reslist);
}

static int has_return( list *l );

static int
exp_has_return(sql_exp *e) 
{
	if (e->type == e_psm) {
		if (e->flag & PSM_RETURN) 
			return 1;
		if (e->flag & PSM_IF) 
			return has_return(e->r) && (!e->f || has_return(e->f));
	}
	return 0;
}

static int
has_return( list *l )
{
	node *n = l->t;
	sql_exp *e = n->data;

	/* last statment of sequential block */
	if (exp_has_return(e)) 
		return 1;
	return 0;
}

static list *
sequential_block (mvc *sql, sql_subtype *restype, list *restypelist, dlist *blk, char *opt_label, int is_func) 
{
	list *l=0;
	dnode *n;

	assert(!restype || !restypelist);

 	if (THRhighwater())
		return sql_error(sql, 10, "SELECT: too many nested operators");

	if (blk->h)
 		l = sa_list(sql->sa);
	stack_push_frame(sql, opt_label);
	for (n = blk->h; n; n = n->next ) {
		sql_exp *res = NULL;
		list *reslist = NULL;
		symbol *s = n->data.sym;

		switch (s->token) {
		case SQL_SET:
			res = psm_set_exp(sql, s->data.lval->h);
			break;
		case SQL_DECLARE:
			reslist = rel_psm_declare(sql, s->data.lval->h);
			break;
		case SQL_CREATE_TABLE: 
			res = rel_psm_declare_table(sql, s->data.lval->h);
			break;
		case SQL_WHILE:
			res = rel_psm_while_do(sql, restype, restypelist, s->data.lval->h, is_func);
			break;
		case SQL_IF:
			res = rel_psm_if_then_else(sql, restype, restypelist, s->data.lval->h, is_func);
			break;
		case SQL_CASE:
			reslist = rel_psm_case(sql, restype, restypelist, s->data.lval->h, is_func);
			break;
		case SQL_CALL:
			res = rel_psm_call(sql, s->data.sym);
			break;
		case SQL_RETURN:
			/*If it is not a function it cannot have a return statement*/
			if (!is_func)
				res = sql_error(sql, 01, 
					"Return statement in the procedure body");
			else {
				/* should be last statement of a sequential_block */
				if (n->next) { 
					res = sql_error(sql, 01, 
						"Statement after return");
				} else {
					res = NULL;
					reslist = rel_psm_return(sql, restype, restypelist, s->data.sym);
				}
			}
			break;
		case SQL_SELECT: { /* row selections (into variables) */
			exp_kind ek = {type_value, card_row, TRUE};
			reslist = rel_select_into(sql, s, ek);
		}	break;
		case SQL_COPYFROM:
		case SQL_BINCOPYFROM:
		case SQL_INSERT:
		case SQL_UPDATE:
		case SQL_DELETE: {
			sql_rel *r = rel_updates(sql, s);
			if (!r)
				return NULL;
			res = exp_rel(sql, r);
		}	break;
		default:
			res = sql_error(sql, 01, 
			 "Statement '%s' is not a valid flow control statement",
			 token2string(s->token));
		}
		if (!res && !reslist) {
			l = NULL;
			break;
		}
		if (res)
			list_append(l, res);
		else
			list_merge(l, reslist, NULL);
	}
	stack_pop_frame(sql);
	return l;
}

static int
arg_cmp(void *A, void *N) 
{
	sql_arg *a = A;
	char *name = N;
	return strcmp(a->name, name);
}

static list *
result_type(mvc *sql, symbol *res) 
{
	if (res->token == SQL_TYPE) {
		sql_subtype *st = &res->data.lval->h->data.typeval;
		sql_arg *a = sql_create_arg(sql->sa, "result", st, ARG_OUT);

		return list_append(sa_list(sql->sa), a);
	} else if (res->token == SQL_TABLE) {
		sql_arg *a;
		dnode *n = res->data.lval->h;
		list *types = sa_list(sql->sa);

		for(;n; n = n->next->next) {
			sql_subtype *ct = &n->next->data.typeval;

			if (list_find(types, n->data.sval, &arg_cmp) != NULL)
				return sql_error(sql, ERR_AMBIGUOUS, "CREATE FUNC: identifier '%s' ambiguous", n->data.sval);

		       	a = sql_create_arg(sql->sa, n->data.sval, ct, ARG_OUT);
			list_append(types, a);
		}
		return types;
	}
	return NULL;
}

static list *
create_type_list(mvc *sql, dlist *params, int param)
{
	sql_subtype *par_subtype;
	list * type_list = sa_list(sql->sa);
	dnode * n = NULL;
	
	if (params) {
		for (n = params->h; n; n = n->next) {
			dnode *an = n;
	
			if (param) {
				an = n->data.lval->h;
				par_subtype = &an->next->data.typeval;
				if (par_subtype && !par_subtype->type) /* var arg */
					return type_list;
				list_append(type_list, par_subtype);
			} else { 
				par_subtype = &an->data.typeval;
				list_prepend(type_list, par_subtype);
			}
		}
	}
	return type_list;
}

static sql_rel*
rel_create_function(sql_allocator *sa, const char *sname, sql_func *f)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_clob(sa, sname));
	if (f)
		append(exps, exp_atom_clob(sa, f->base.name));
	append(exps, exp_atom_ptr(sa, f));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_CREATE_FUNCTION;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_create_func(mvc *sql, dlist *qname, dlist *params, symbol *res, dlist *ext_name, dlist *body, int type, int lang, int replace)
{
	const char *fname = qname_table(qname);
	const char *sname = qname_schema(qname);
	sql_schema *s = NULL;
	sql_func *f = NULL;
	sql_subfunc *sf;
	dnode *n;
	list *type_list = NULL, *restype = NULL;
	int instantiate = (sql->emode == m_instantiate);
	int deps = (sql->emode == m_deps);
	int create = (!instantiate && !deps);
	bit vararg = FALSE;

	char is_table = (res && res->token == SQL_TABLE);
	char is_aggr = (type == F_AGGR);
	char is_func = (type != F_PROC);
	char is_loader = (type == F_LOADER);

	char *F = is_loader?"LOADER":(is_aggr?"AGGREGATE":(is_func?"FUNCTION":"PROCEDURE"));
	char *fn = is_loader?"loader":(is_aggr ? "aggregate" : (is_func ? "function" : "procedure"));
	char *KF = type==F_FILT?"FILTER ": type==F_UNION?"UNION ": "";
	char *kf = type == F_FILT ? "filter " : type == F_UNION ? "union " : "";

	assert(res || type == F_PROC || type == F_FILT || type == F_LOADER);

	if (is_table)
		type = F_UNION;

	if (STORE_READONLY && create) 
		return sql_error(sql, 06, "schema statements cannot be executed on a readonly database.");
			
	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "3F000!CREATE %s%s: no such schema '%s'", KF, F, sname);
	if (s == NULL)
		s = cur_schema(sql);

	type_list = create_type_list(sql, params, 1);
	if ((sf = sql_bind_func_(sql->sa, s, fname, type_list, type)) != NULL && create) {
		if (replace) {
			sql_func *func = sf->func;
			int action = 0;
			if (!mvc_schema_privs(sql, s)) {
				return sql_error(sql, 02, "CREATE OR REPLACE %s%s: access denied for %s to schema ;'%s'", KF, F, stack_get_string(sql, "current_user"), s->base.name);
			}
			if (mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, NULL))
				return sql_error(sql, 02, "CREATE OR REPLACE %s%s: there are database objects dependent on %s%s %s;", KF, F, kf, fn, func->base.name);
			if (!func->s) {
				return sql_error(sql, 02, "CREATE OR REPLACE %s%s: not allowed to replace system %s%s %s;", KF, F, kf, fn, func->base.name);
			}

			mvc_drop_func(sql, s, func, action);
			sf = NULL;
		} else {
			if (params) {
				char *arg_list = NULL;
				node *n;
				
				for (n = type_list->h; n; n = n->next) {
					char *tpe =  subtype2string((sql_subtype *) n->data);
					
					if (arg_list) {
						char *t = arg_list;
						arg_list = sql_message("%s, %s", arg_list, tpe);
						_DELETE(t);
						_DELETE(tpe);
					} else {
						arg_list = tpe;
					}
				}
				(void)sql_error(sql, 02, "CREATE %s%s: name '%s' (%s) already in use", KF, F, fname, arg_list);
				_DELETE(arg_list);
				list_destroy(type_list);
				return NULL;
			} else {
				list_destroy(type_list);
				return sql_error(sql, 02, "CREATE %s%s: name '%s' already in use", KF, F, fname);
			}
		}
	}
	list_destroy(type_list);
	if (create && !mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, "CREATE %s%s: insufficient privileges "
				"for user '%s' in schema '%s'", KF, F,
				stack_get_string(sql, "current_user"), s->base.name);
	} else {
		char *q = QUERY(sql->scanner);
		list *l = NULL;

	 	if (params) {
			for (n = params->h; n; n = n->next) {
				dnode *an = n->data.lval->h;
				sql_add_param(sql, an->data.sval, &an->next->data.typeval);
			}
			l = sql->params;
			if (l && list_length(l) == 1) {
				sql_arg *a = l->h->data;

				if (strcmp(a->name, "*") == 0) {
					l = NULL;
					vararg = TRUE;
				}
			}
		}
		if (!l)
			l = sa_list(sql->sa);
		if (res) {
			restype = result_type(sql, res);
			if (!restype)
				return sql_error(sql, 01,
						"CREATE %s%s: failed to get restype", KF, F);
		}
		if (body && lang > FUNC_LANG_SQL) {
			char *lang_body = body->h->data.sval;
			char *mod = 	
					(lang == FUNC_LANG_R)?"rapi":
					(lang == FUNC_LANG_C)?"capi":
					(lang == FUNC_LANG_J)?"japi":
					(lang == FUNC_LANG_PY)?"pyapi":
 					(lang == FUNC_LANG_MAP_PY)?"pyapimap":"unknown";
			sql->params = NULL;
			if (create) {
				f = mvc_create_func(sql, sql->sa, s, fname, l, restype, type, lang,  mod, fname, lang_body, (type == F_LOADER)?TRUE:FALSE, vararg);
			} else if (!sf) {
				return sql_error(sql, 01, "CREATE %s%s: R function %s.%s not bound", KF, F, s->base.name, fname );
			} /*else {
				sql_func *f = sf->func;
				f->mod = _STRDUP("rapi");
				f->imp = _STRDUP("eval");
				if (res && restype)
					f->res = restype;
				f->sql = 0;
				f->lang = FUNC_LANG_INT;
			}*/
		} else if (body) {
			sql_arg *ra = (restype && !is_table)?restype->h->data:NULL;
			list *b = NULL;
			sql_schema *old_schema = cur_schema(sql);

			if (create) { /* needed for recursive functions */
				q = query_cleaned(q);
				sql->forward = f = mvc_create_func(sql, sql->sa, s, fname, l, restype, type, lang, "user", q, q, FALSE, vararg);
				GDKfree(q);
			}
			sql->session->schema = s;
			b = sequential_block(sql, (ra)?&ra->type:NULL, ra?NULL:restype, body, NULL, is_func);
			sql->forward = NULL;
			sql->session->schema = old_schema;
			sql->params = NULL;
			if (!b) 
				return NULL;
		
			/* check if we have a return statement */
			if (is_func && restype && !has_return(b)) {
				return sql_error(sql, 01,
						"CREATE %s%s: missing return statement", KF, F);
			}
			if (!is_func && !restype && has_return(b)) {
				return sql_error(sql, 01, "CREATE %s%s: procedures "
						"cannot have return statements", KF, F);
			}

			/* in execute mode we instantiate the function */
			if (instantiate || deps) {
				return rel_psm_block(sql->sa, b);
			}
		} else {
			char *fmod = qname_module(ext_name);
			char *fnme = qname_fname(ext_name);

			if (!fmod || !fnme)
				return NULL;
			sql->params = NULL;
			if (create) {
				q = query_cleaned(q);
				f = mvc_create_func(sql, sql->sa, s, fname, l, restype, type, lang, fmod, fnme, q, FALSE, vararg);
				GDKfree(q);
			} else if (!sf) {
				return sql_error(sql, 01, "CREATE %s%s: external name %s.%s not bound (%s.%s)", KF, F, fmod, fnme, s->base.name, fname );
			} else {
				sql_func *f = sf->func;
				if (!f->mod || strcmp(f->mod, fmod))
					f->mod = _STRDUP(fmod);
				if (!f->imp || strcmp(f->imp, fnme)) 
					f->imp = (f->sa)?sa_strdup(f->sa, fnme):_STRDUP(fnme);
				f->sql = 0; /* native */
				f->lang = FUNC_LANG_INT;
			}
		}
	}
	return rel_create_function(sql->sa, s->base.name, f);
}

static sql_rel*
rel_drop_function(sql_allocator *sa, const char *sname, const char *name, int nr, int type, int action)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, name));
	append(exps, exp_atom_int(sa, nr));
	append(exps, exp_atom_int(sa, type));
	append(exps, exp_atom_int(sa, action));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_DROP_FUNCTION;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

sql_func *
resolve_func( mvc *sql, sql_schema *s, const char *name, dlist *typelist, int type, char *op) 
{
	sql_func *func = NULL;
	list *list_func = NULL, *type_list = NULL;
	char is_aggr = (type == F_AGGR);
	char is_func = (type != F_PROC && type != F_LOADER);
	char *F = is_aggr?"AGGREGATE":(is_func?"FUNCTION":"PROCEDURE");
	char *f = is_aggr?"aggregate":(is_func?"function":"procedure");
	char *KF = type==F_FILT?"FILTER ": type==F_UNION?"UNION ": "";
	char *kf = type==F_FILT?"filter ": type==F_UNION?"union ": "";

	if (typelist) {	
		sql_subfunc *sub_func;

		type_list = create_type_list(sql, typelist, 0);
		sub_func = sql_bind_func_(sql->sa, s, name, type_list, type);
		if (!sub_func && type == F_FUNC) {
			sub_func = sql_bind_func_(sql->sa, s, name, type_list, F_UNION);
			type = sub_func?F_UNION:F_FUNC;
		}
		if ( sub_func && sub_func->func->type == type)
			func = sub_func->func;
	} else {
		list_func = schema_bind_func(sql, s, name, type);
		if (!list_func && type == F_FUNC) 
			list_func = schema_bind_func(sql,s,name, F_UNION);
		if (list_func && list_func->cnt > 1) {
			list_destroy(list_func);
			return sql_error(sql, 02, "%s %s%s: there are more than one %s%s called '%s', please use the full signature", op, KF, F, kf, f,name);
		}
		if (list_func && list_func->cnt == 1)
			func = (sql_func*) list_func->h->data;
	}

	if (!func) { 
		if (typelist) {
			char *arg_list = NULL;
			node *n;
			
			if (type_list->cnt > 0) {
				void *e;
				for (n = type_list->h; n; n = n->next) {
					char *tpe =  subtype2string((sql_subtype *) n->data);
				
					if (arg_list) {
						char *t = arg_list;
						arg_list = sql_message("%s, %s", arg_list, tpe);
						_DELETE(tpe);
						_DELETE(t);
					} else {
						arg_list = tpe;
					}
				}
				list_destroy(list_func);
				list_destroy(type_list);
				e = sql_error(sql, 02, "%s %s%s: no such %s%s '%s' (%s)", op, KF, F, kf, f, name, arg_list);
				_DELETE(arg_list);
				return e;
			}
			list_destroy(list_func);
			list_destroy(type_list);
			return sql_error(sql, 02, "%s %s%s: no such %s%s '%s' ()", op, KF, F, kf, f, name);

		} else {
			return sql_error(sql, 02, "%s %s%s: no such %s%s '%s'", op, KF, F, kf, f, name);
		}
	} else if (((is_func && type != F_FILT) && !func->res) || 
		   (!is_func && func->res)) {
		list_destroy(list_func);
		list_destroy(type_list);
		return sql_error(sql, 02, "%s %s%s: cannot drop %s '%s'", KF, F, is_func?"procedure":"function", op, name);
	}

	list_destroy(list_func);
	list_destroy(type_list);
	return func;
}

static sql_rel* 
rel_drop_func(mvc *sql, dlist *qname, dlist *typelist, int drop_action, int type)
{
	const char *name = qname_table(qname);
	const char *sname = qname_schema(qname);
	sql_schema *s = NULL;
	sql_func *func = NULL;

	char is_aggr = (type == F_AGGR);
	char is_func = (type != F_PROC);
	char *F = is_aggr?"AGGREGATE":(is_func?"FUNCTION":"PROCEDURE");
	char *KF = type==F_FILT?"FILTER ": type==F_UNION?"UNION ": "";

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "3F000!DROP %s%s: no such schema '%s'", KF, F, sname);

	if (s == NULL) 
		s =  cur_schema(sql);
	
	func = resolve_func(sql, s, name, typelist, type, "DROP");
	if (!func && !sname) {
		s = tmp_schema(sql);
		func = resolve_func(sql, s, name, typelist, type, "DROP");
	}
	if (func)
		return rel_drop_function(sql->sa, s->base.name, name, func->base.id, type, drop_action);
	return NULL;
}

static sql_rel* 
rel_drop_all_func(mvc *sql, dlist *qname, int drop_action, int type)
{
	const char *name = qname_table(qname);
	const char *sname = qname_schema(qname);
	sql_schema *s = NULL;
	list * list_func = NULL; 

	char is_aggr = (type == F_AGGR);
	char is_func = (type != F_PROC);
	char *F = is_aggr?"AGGREGATE":(is_func?"FUNCTION":"PROCEDURE");
	char *f = is_aggr?"aggregate":(is_func?"function":"procedure");
	char *KF = type==F_FILT?"FILTER ": type==F_UNION?"UNION ": "";
	char *kf = type==F_FILT?"filter ": type==F_UNION?"union ": "";

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "3F000!DROP %s%s: no such schema '%s'", KF, F, sname);

	if (s == NULL) 
		s =  cur_schema(sql);
	
	list_func = schema_bind_func(sql, s, name, type);
	if (!list_func) 
		return sql_error(sql, 02, "DROP ALL %s%s: no such %s%s '%s'", KF, F, kf, f, name);
	list_destroy(list_func);
	return rel_drop_function(sql->sa, s->base.name, name, -1, type, drop_action);
}

static sql_rel *
rel_create_trigger(mvc *sql, const char *sname, const char *tname, const char *triggername, int time, int orientation, int event, const char *old_name, const char *new_name, symbol *condition, const char *query)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_str(sql->sa, sname, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sql->sa, tname, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sql->sa, triggername, sql_bind_localtype("str") ));
	append(exps, exp_atom_int(sql->sa, time));
	append(exps, exp_atom_int(sql->sa, orientation));
	append(exps, exp_atom_int(sql->sa, event));
	append(exps, exp_atom_str(sql->sa, old_name, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sql->sa, new_name, sql_bind_localtype("str") ));
	(void)condition;
	append(exps, exp_atom_str(sql->sa, NULL, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sql->sa, query, sql_bind_localtype("str") ));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_CREATE_TRIGGER;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = 0;
	return rel;
}

static void
_stack_push_table(mvc *sql, const char *tname, sql_table *t)
{
	sql_rel *r = rel_basetable(sql, t, tname );
		
	stack_push_rel_view(sql, tname, r);
}

static sql_rel *
create_trigger(mvc *sql, dlist *qname, int time, symbol *trigger_event, dlist *tqname, dlist *opt_ref, dlist *triggered_action)
{
	const char *triggername = qname_table(qname);
	const char *sname = qname_schema(tqname);
	const char *tname = qname_table(tqname);
	sql_schema *ss = cur_schema(sql);
	sql_table *t = NULL;
	int instantiate = (sql->emode == m_instantiate);
	int create = (!instantiate && sql->emode != m_deps);
	list *sq = NULL;
	sql_rel *r = NULL;

	dlist *columns = trigger_event->data.lval;
	const char *old_name = NULL, *new_name = NULL; 
	dlist *stmts = triggered_action->h->next->next->data.lval;
	symbol *condition = triggered_action->h->next->data.sym;
	
	if (!sname)
		sname = ss->base.name;

	if (sname && !(ss = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "3F000!CREATE TRIGGER: no such schema '%s'", sname);

	if (opt_ref) {
		dnode *dl = opt_ref->h;
		for ( ; dl; dl = dl->next) {
			/* list (new(1)/old(0)), char */
			char *n = dl->data.lval->h->next->data.sval;

			assert(dl->data.lval->h->type == type_int);
			if (!dl->data.lval->h->data.i_val) /*?l_val?*/
				old_name = n;
			else
				new_name = n;
		}
	}
	if (create && !mvc_schema_privs(sql, ss)) 
		return sql_error(sql, 02, "CREATE TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), ss->base.name);
	if (create && mvc_bind_trigger(sql, ss, triggername) != NULL) 
		return sql_error(sql, 02, "CREATE TRIGGER: name '%s' already in use", triggername);
	
	if (create && !(t = mvc_bind_table(sql, ss, tname)))
		return sql_error(sql, 02, "CREATE TRIGGER: unknown table '%s'", tname);
	if (create && isView(t)) 
		return sql_error(sql, 02, "CREATE TRIGGER: cannot create trigger on view '%s'", tname);
	
	if (create) {
		int event = (trigger_event->token == SQL_INSERT)?0:
			    (trigger_event->token == SQL_DELETE)?1:2;
		int orientation = triggered_action->h->data.i_val;
		char *q = query_cleaned(QUERY(sql->scanner));

		assert(triggered_action->h->type == type_int);
		r = rel_create_trigger(sql, t->s->base.name, t->base.name, triggername, time, orientation, event, old_name, new_name, condition, q);
		GDKfree(q);
		return r;
	}

	if (!instantiate) {
		t = mvc_bind_table(sql, ss, tname);
		stack_push_frame(sql, "OLD-NEW");
		/* we need to add the old and new tables */
		if (!instantiate && new_name)
			_stack_push_table(sql, new_name, t);
		if (!instantiate && old_name)
			_stack_push_table(sql, old_name, t);
	}
	if (condition) {
		sql_rel *rel = NULL;

		if (new_name) /* in case of updates same relations is available via both names */ 
			rel = stack_find_rel_view(sql, new_name);
		if (!rel && old_name)
			rel = stack_find_rel_view(sql, old_name);
		if (rel)
			rel = rel_logical_exp(sql, rel, condition, sql_where);
		if (!rel)
			return NULL;
		/* transition tables */
		/* insert: rel_select(table [new], searchcondition) */
		/* delete: rel_select(table [old], searchcondition) */
		/* update: rel_select(table [old,new]), searchcondition) */
		if (new_name)
			stack_update_rel_view(sql, new_name, rel);
		if (old_name)
			stack_update_rel_view(sql, old_name, new_name?rel_dup(rel):rel);
	}
	sq = sequential_block(sql, NULL, NULL, stmts, NULL, 1);
	r = rel_psm_block(sql->sa, sq);

	/* todo trigger_columns */
	(void)columns;
	return r;
}

static sql_rel *
rel_drop_trigger(mvc *sql, const char *sname, const char *tname)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_str(sql->sa, sname, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sql->sa, tname, sql_bind_localtype("str") ));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = DDL_DROP_TRIGGER;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
drop_trigger(mvc *sql, dlist *qname)
{
	const char *sname = qname_schema(qname);
	const char *tname = qname_table(qname);
	sql_schema *ss = cur_schema(sql);

	if (!sname)
		sname = ss->base.name;

	if (sname && !(ss = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "3F000!DROP TRIGGER: no such schema '%s'", sname);

	if (!mvc_schema_privs(sql, ss)) 
		return sql_error(sql, 02, "DROP TRIGGER: access denied for %s to schema ;'%s'", stack_get_string(sql, "current_user"), ss->base.name);
	return rel_drop_trigger(sql, ss->base.name, tname);
}

static sql_rel *
psm_analyze(mvc *sql, char *analyzeType, dlist *qname, dlist *columns, symbol *sample, int minmax )
{
	exp_kind ek = {type_value, card_value, FALSE};
	sql_exp *sample_exp = NULL, *call, *mm_exp = NULL;
	const char *sname = NULL, *tname = NULL;
	list *tl = sa_list(sql->sa);
	list *exps = sa_list(sql->sa), *analyze_calls = sa_list(sql->sa);
	sql_subfunc *f = NULL;

	append(exps, mm_exp = exp_atom_int(sql->sa, minmax));
	append(tl, exp_subtype(mm_exp));
	if (sample) {
		sql_subtype *tpe = sql_bind_localtype("lng");

       		sample_exp = rel_value_exp( sql, NULL, sample, 0, ek);
		if (sample_exp)
			sample_exp = rel_check_type(sql, tpe, sample_exp, type_cast); 
	} else {
		sample_exp = exp_atom_lng(sql->sa, 0);
	}
	append(exps, sample_exp);
	append(tl, exp_subtype(sample_exp));

	if (qname) {
		if (qname->h->next)
			sname = qname_schema(qname);
		else
			sname = qname_table(qname);
		if (!sname)
			sname = cur_schema(sql)->base.name;
		if (qname->h->next)
			tname = qname_table(qname);
	}
	/* call analyze( [schema, [ table ]], opt_sample_size, opt_minmax ) */
	if (sname) {
		sql_exp *sname_exp = exp_atom_clob(sql->sa, sname);

		append(exps, sname_exp);
		append(tl, exp_subtype(sname_exp));
	}
	if (tname) {
		sql_exp *tname_exp = exp_atom_clob(sql->sa, tname);

		append(exps, tname_exp);
		append(tl, exp_subtype(tname_exp));

		if (columns)
			append(tl, exp_subtype(tname_exp));
	}
	if (!columns) {
		f = sql_bind_func_(sql->sa, mvc_bind_schema(sql, "sys"), analyzeType, tl, F_PROC);
		if (!f)
			return sql_error(sql, 01, "Analyze procedure missing");
		call = exp_op(sql->sa, exps, f);
		append(analyze_calls, call);
	} else {
		dnode *n;

		f = sql_bind_func_(sql->sa, mvc_bind_schema(sql, "sys"), analyzeType, tl, F_PROC);
		if (!f)
			return sql_error(sql, 01, "Analyze procedure missing");
		for( n = columns->h; n; n = n->next) {
			const char *cname = n->data.sval;
			list *nexps = list_dup(exps, NULL);
			sql_exp *cname_exp = exp_atom_clob(sql->sa, cname);

			append(nexps, cname_exp);
			/* call analyze( opt_minmax, opt_sample_size, sname, tname, cname) */
			call = exp_op(sql->sa, nexps, f);
			append(analyze_calls, call);
		}
	}
	return rel_psm_block(sql->sa, analyze_calls);
}

static sql_rel*
create_table_from_loader(mvc *sql, dlist *qname, symbol *fcall)
{
	sql_schema *s = NULL;
	char *sname = qname_schema(qname);
	char *tname = qname_table(qname);
	sql_exp *import = NULL;
	exp_kind ek = {type_value, card_loader, FALSE};
	sql_rel *res = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, "3F000!CREATE TABLE: no such schema '%s'", sname);

	if (mvc_bind_table(sql, s, tname)) {
		return sql_error(sql, 02, "42S01!CREATE TABLE: name '%s' already in use", tname);
	} else if (!mvc_schema_privs(sql, s)){
		return sql_error(sql, 02, "42000!CREATE TABLE: insufficient privileges for user '%s' in schema '%s'", stack_get_string(sql, "current_user"), s->base.name);
	}

	import = rel_value_exp(sql, &res, fcall, sql_sel, ek);
	if (!import) {
		return NULL;
	}
	((sql_subfunc*) import->f)->sname = sname ? sa_zalloc(sql->sa, strlen(sname) + 1) : NULL;
	((sql_subfunc*) import->f)->tname = tname ? sa_zalloc(sql->sa, strlen(tname) + 1) : NULL;

	if (sname) strcpy(((sql_subfunc*) import->f)->sname, sname);
	if (tname) strcpy(((sql_subfunc*) import->f)->tname, tname);

	return rel_psm_stmt(sql->sa, import);
}

sql_rel *
rel_psm(mvc *sql, symbol *s)
{
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_CREATE_FUNC:
	{
		dlist *l = s->data.lval;
		int type = l->h->next->next->next->next->next->data.i_val;
		int lang = l->h->next->next->next->next->next->next->data.i_val;
		int repl = l->h->next->next->next->next->next->next->next->data.i_val;

		ret = rel_create_func(sql, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym, l->h->next->next->next->data.lval, l->h->next->next->next->next->data.lval, type, lang, repl);
		sql->type = Q_SCHEMA;
	} 	break;
	case SQL_DROP_FUNC:
	{
		dlist *l = s->data.lval;
		dlist *qname = l->h->data.lval;
		dlist *typelist = l->h->next->data.lval;
		int type = l->h->next->next->data.i_val;
		int all = l->h->next->next->next->data.i_val;
		int drop_action = l->h->next->next->next->next->data.i_val;

		if (STORE_READONLY) 
			return sql_error(sql, 06, "schema statements cannot be executed on a readonly database.");
			
		if (all)
			ret = rel_drop_all_func(sql, qname, drop_action, type);
		else
			ret = rel_drop_func(sql, qname, typelist, drop_action, type);

		sql->type = Q_SCHEMA;
	}	break;
	case SQL_SET:
		ret = rel_psm_stmt(sql->sa, psm_set_exp(sql, s->data.lval->h));
		sql->type = Q_SCHEMA;
		break;
	case SQL_DECLARE:
		ret = rel_psm_block(sql->sa, rel_psm_declare(sql, s->data.lval->h));
		sql->type = Q_SCHEMA;
		break;
	case SQL_CALL:
		ret = rel_psm_stmt(sql->sa, rel_psm_call(sql, s->data.sym));
		sql->type = Q_UPDATE;
		break;
	case SQL_CREATE_TABLE_LOADER:
	{
	    dlist *l = s->data.lval;
	    dlist *qname = l->h->data.lval;
	    symbol *sym = l->h->next->data.sym;

	    ret = create_table_from_loader(sql, qname, sym);
	    sql->type = Q_UPDATE;
	}	break;
	case SQL_CREATE_TRIGGER:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->type == type_int);
		ret = create_trigger(sql, l->h->data.lval, l->h->next->data.i_val, l->h->next->next->data.sym, l->h->next->next->next->data.lval, l->h->next->next->next->next->data.lval, l->h->next->next->next->next->next->data.lval);
		sql->type = Q_SCHEMA;
	}
		break;

	case SQL_DROP_TRIGGER:
	{
		dlist *l = s->data.lval;

		ret = drop_trigger(sql, l);
		sql->type = Q_SCHEMA;
	}
		break;

	case SQL_ANALYZE: {
		dlist *l = s->data.lval;

		ret = psm_analyze(sql, "analyze", l->h->data.lval /* qualified table name */, l->h->next->data.lval /* opt list of column */, l->h->next->next->data.sym /* opt_sample_size */, l->h->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	} 	break;
	default:
		return sql_error(sql, 01, "schema statement unknown symbol(" PTRFMT ")->token = %s", PTRFMTCAST s, token2string(s->token));
	}
	return ret;
}
