/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_psm.h"
#include "rel_semantic.h"
#include "rel_schema.h"
#include "rel_select.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_updates.h"
#include "sql_privileges.h"

#define psm_zero_or_one(exp) \
	do { \
		if (exp && exp->card > CARD_AGGR) { \
			sql_subfunc *zero_or_one = sql_bind_func(sql, "sys", "zero_or_one", exp_subtype(exp), NULL, F_AGGR); \
			assert(zero_or_one); \
			exp = exp_aggr1(sql->sa, exp, zero_or_one, 0, 0, CARD_ATOM, has_nil(exp)); \
		} \
	} while(0)

static list *sequential_block(sql_query *query, sql_subtype *restype, list *restypelist, dlist *blk, char *opt_name, int is_func);

sql_rel *
rel_psm_block(sql_allocator *sa, list *l)
{
	if (l) {
		sql_rel *r = rel_create(sa);
		if(!r)
			return NULL;

		r->op = op_ddl;
		r->flag = ddl_psm;
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

/* SET [ schema '.' ] variable = value and set ( [ schema1 '.' ] variable1, .., [ schemaN '.' ] variableN) = (query) */
static sql_exp *
psm_set_exp(sql_query *query, dnode *n)
{
	mvc *sql = query->sql;
	dlist *qname = n->data.lval;
	symbol *val = n->next->data.sym;
	sql_exp *res = NULL, *e = NULL;
	int level = 0, single = (qname->h->type == type_string);
	sql_rel *rel = NULL;
	sql_subtype *tpe;

	if (single) {
		exp_kind ek = {type_value, card_value, FALSE};
		const char *sname = qname_schema(qname);
		const char *vname = qname_schema_object(qname);
		sql_var *var = NULL;
		sql_arg *a = NULL;

		if (!find_variable_on_scope(sql, sname, vname, &var, &a, &tpe, &level, "SET"))
			return NULL;
		if (!(e = rel_value_exp2(query, &rel, val, sql_sel | sql_psm, ek)))
			return NULL;
		psm_zero_or_one(e);

		if (!(e = exp_check_type(sql, tpe, rel, e, type_cast)))
			return NULL;
		res = exp_set(sql->sa, var && var->sname ? sa_strdup(sql->sa, var->sname) : NULL, sa_strdup(sql->sa, vname), e, level);
	} else { /* multi assignment */
		exp_kind ek = {type_relation, card_value, FALSE};
		sql_rel *rel_val = rel_subquery(query, NULL, val, ek);
		dlist *vars = n->data.lval;
		dnode *m;
		node *n;
		list *b;

		if (!rel_val)
			return NULL;
		if (!is_project(rel_val->op))
			return sql_error(sql, 02, SQLSTATE(42000) "SET: The subquery is not a projection");
		if (dlist_length(vars) != list_length(rel_val->exps))
			return sql_error(sql, 02, SQLSTATE(42000) "SET: Number of variables not equal to number of supplied values");
		rel_val = rel_return_zero_or_one(sql, rel_val, ek);

		b = sa_list(sql->sa);
		append(b, exp_rel(sql, rel_val));

		for (m = vars->h, n = rel_val->exps->h; n && m; n = n->next, m = m->next) {
			dlist *nqname = m->data.lval;
			const char *sname = qname_schema(nqname);
			const char *vname = qname_schema_object(nqname);
			sql_exp *v = n->data;
			sql_var *var = NULL;
			sql_arg *a = NULL;

			if (!find_variable_on_scope(sql, sname, vname, &var, &a, &tpe, &level, "SET"))
				return NULL;

			v = exp_ref(sql, v);
			if (!(v = exp_check_type(sql, tpe, rel_val, v, type_cast)))
				return NULL;
			append(b, exp_set(sql->sa, var && var->sname ? sa_strdup(sql->sa, var->sname) : NULL, sa_strdup(sql->sa, vname), v, level));
		}
		res = exp_rel(sql, rel_psm_block(sql->sa, b));
	}
	return res;
}

static sql_exp*
rel_psm_call(sql_query * query, symbol *se)
{
	mvc *sql = query->sql;
	sql_subtype *t;
	sql_exp *res = NULL;
	exp_kind ek = {type_value, card_none, FALSE};
	sql_rel *rel = NULL;

	res = rel_value_exp(query, &rel, se, sql_sel | psm_call, ek);
	if (!res || rel || ((t=exp_subtype(res)) && t->type))  /* only procedures */
		return sql_error(sql, 01, SQLSTATE(42000) "Function calls are ignored");
	return res;
}

static list *
rel_psm_declare(mvc *sql, dnode *n)
{
	list *l = sa_list(sql->sa);

	while (n) { /* list of 'identfiers with type' */
		dnode *ids = n->data.sym->data.lval->h->data.lval->h;
		sql_subtype *ctype = &n->data.sym->data.lval->h->next->data.typeval;
		while (ids) {
			dlist *qname = ids->data.lval;
			const char *sname = qname_schema(qname);
			const char *tname = qname_schema_object(qname);
			sql_exp *r = NULL;
			sql_arg *a;

			if (sname)
				return sql_error(sql, 01, SQLSTATE(42000) "DECLARE: Declared variables don't have a schema");
			/* find if there's a parameter with the same name */
			if (sql->frame == 1 && (a = sql_bind_param(sql, tname)))
				return sql_error(sql, 01, SQLSTATE(42000) "DECLARE: Variable '%s' declared as a parameter", tname);
			/* check if we overwrite a scope local variable declare x; declare x; */
			if (frame_find_var(sql, tname))
				return sql_error(sql, 01, SQLSTATE(42000) "DECLARE: Variable '%s' already declared", tname);
			/* variables are put on stack, globals on a separate list */
			if (!frame_push_var(sql, tname, ctype))
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			r = exp_var(sql->sa, NULL, sa_strdup(sql->sa, tname), ctype, sql->frame);
			append(l, r);
			ids = ids->next;
		}
		n = n->next;
	}
	return l;
}

static sql_exp *
rel_psm_declare_table(sql_query *query, dnode *n)
{
	mvc *sql = query->sql;
	sql_rel *rel = NULL, *baset = NULL;
	dlist *qname = n->next->data.lval;
	const char *sname = qname_schema(qname);
	const char *name = qname_schema_object(qname);
	sql_table *t;

	if (sname)
		return sql_error(sql, 01, SQLSTATE(42000) "DECLARE TABLE: Declared tables don't have a schema");

	assert(n->next->next->next->type == type_int);
	rel = rel_create_table(query, SQL_DECLARED_TABLE, sname, name, false, n->next->next->data.sym,
						   n->next->next->next->data.i_val, NULL, NULL, NULL, false, NULL,
						   n->next->next->next->next->next->next->data.i_val);

	if (!rel)
		return NULL;
	if (rel->op == op_ddl) {
		baset = rel;
	} else if (rel->op == op_insert) {
		baset = rel->l;
	} else {
		assert(0);
	}
	assert(baset->flag == ddl_create_table);
	t = (sql_table*)((atom*)((sql_exp*)baset->exps->t->data)->l)->data.val.pval;
	if (!frame_push_table(sql, t))
		return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
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
rel_psm_while_do( sql_query *query, sql_subtype *res, list *restypelist, dnode *w, int is_func )
{
	mvc *sql = query->sql;
	if (!w)
		return NULL;
	if (w->type == type_symbol) {
		sql_exp *cond;
		list *whilestmts;
		dnode *n = w;
		sql_rel *rel = NULL;
		exp_kind ek = {type_value, card_value, FALSE};

		cond = rel_logical_value_exp(query, &rel, n->data.sym, sql_sel | sql_psm, ek);
		psm_zero_or_one(cond);
		n = n->next;
		whilestmts = sequential_block(query, res, restypelist, n->data.lval, n->next->data.sval, is_func);

		if (sql->session->status || !cond || !whilestmts)
			return NULL;

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
psm_if_then_else( sql_query *query, sql_subtype *res, list *restypelist, dnode *elseif, int is_func)
{
	mvc *sql = query->sql;
	if (!elseif)
		return NULL;
	assert(elseif->type == type_symbol);
	if (elseif->data.sym && elseif->data.sym->token == SQL_IF) {
		sql_exp *cond;
		list *ifstmts, *elsestmts;
		dnode *n = elseif->data.sym->data.lval->h;
		sql_rel *rel = NULL;
		exp_kind ek = {type_value, card_value, FALSE};

		cond = rel_logical_value_exp(query, &rel, n->data.sym, sql_sel | sql_psm, ek);
		psm_zero_or_one(cond);
		n = n->next;
		ifstmts = sequential_block(query, res, restypelist, n->data.lval, NULL, is_func);
		n = n->next;
		elsestmts = psm_if_then_else( query, res, restypelist, n, is_func);

		if (sql->session->status || !cond || !ifstmts)
			return NULL;

		return append(sa_list(sql->sa), exp_if( sql->sa, cond, ifstmts, elsestmts));
	} else { /* else */
		symbol *e = elseif->data.sym;

		if (e==NULL || (e->token != SQL_ELSE))
			return NULL;
		return sequential_block(query, res, restypelist, e->data.lval, NULL, is_func);
	}
}

static sql_exp *
rel_psm_if_then_else( sql_query *query, sql_subtype *res, list *restypelist, dnode *elseif, int is_func)
{
	mvc *sql = query->sql;
	if (!elseif)
		return NULL;
	if (elseif->next && elseif->type == type_symbol) { /* if or elseif */
		sql_exp *cond;
		list *ifstmts, *elsestmts;
		dnode *n = elseif;
		sql_rel *rel = NULL;
		exp_kind ek = {type_value, card_value, FALSE};

		cond = rel_logical_value_exp(query, &rel, n->data.sym, sql_sel | sql_psm, ek);
		psm_zero_or_one(cond);
		n = n->next;
		ifstmts = sequential_block(query, res, restypelist, n->data.lval, NULL, is_func);
		n = n->next;
		elsestmts = psm_if_then_else( query, res, restypelist, n, is_func);
		if (sql->session->status || !cond || !ifstmts)
			return NULL;

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
static sql_exp *
rel_psm_case( sql_query *query, sql_subtype *res, list *restypelist, dnode *case_when, int is_func )
{
	mvc *sql = query->sql;
	sql_exp *case_stmt = NULL, *last_if = NULL, *ifst = NULL;
	list *else_stmt = NULL;

	if (!case_when)
		return NULL;

	/* case 1 */
	if (case_when->type == type_symbol) {
		dnode *n = case_when;
		symbol *case_value = n->data.sym;
		dlist *when_statements = n->next->data.lval;
		dlist *else_statements = n->next->next->data.lval;
		sql_rel *rel = NULL;
		exp_kind ek = {type_value, card_value, FALSE};
		sql_exp *v = rel_value_exp(query, &rel, case_value, sql_sel | sql_psm, ek);

		psm_zero_or_one(v);
		if (!v)
			return NULL;
		if (else_statements && !(else_stmt = sequential_block(query, res, restypelist, else_statements, NULL, is_func)))
			return NULL;

		n = when_statements->h;
		while(n) {
			dnode *m = n->data.sym->data.lval->h;
			sql_exp *cond=0, *when_value = rel_value_exp(query, &rel, m->data.sym, sql_sel | sql_psm, ek);
			list *if_stmts = NULL;

			psm_zero_or_one(when_value);
			if (!when_value ||
			   (cond = rel_binop_(sql, rel, v, when_value, "sys", "=", card_value)) == NULL ||
			   (if_stmts = sequential_block(query, res, restypelist, m->next->data.lval, NULL, is_func)) == NULL ) {
				return NULL;
			}
			psm_zero_or_one(cond);
			ifst = exp_if(sql->sa, cond, if_stmts, NULL);
			if (last_if) { /* chain if statements for case, keep the last if */
				last_if->f = list_append(sa_list(sql->sa), ifst);
				last_if = ifst;
			} else {
				case_stmt = last_if = ifst;
			}
			n = n->next;
		}
	} else {
		/* case 2 */
		dnode *n = case_when;
		dlist *whenlist = n->data.lval;
		dlist *else_statements = n->next->data.lval;

		if (else_statements && !(else_stmt = sequential_block(query, res, restypelist, else_statements, NULL, is_func)))
			return NULL;

		n = whenlist->h;
		while(n) {
			dnode *m = n->data.sym->data.lval->h;
			sql_rel *rel = NULL;
			exp_kind ek = {type_value, card_value, FALSE};
			sql_exp *cond = rel_logical_value_exp(query, &rel, m->data.sym, sql_sel | sql_psm, ek);
			list *if_stmts = NULL;

			psm_zero_or_one(cond);
			if (!cond ||
			   (if_stmts = sequential_block(query, res, restypelist, m->next->data.lval, NULL, is_func)) == NULL ) {
				return NULL;
			}
			ifst = exp_if(sql->sa, cond, if_stmts, NULL);
			if (last_if) { /* chain if statements for case, keep the last if */
				last_if->f = list_append(sa_list(sql->sa), ifst);
				last_if = ifst;
			} else {
				case_stmt = last_if = ifst;
			}
			n = n->next;
		}
	}
	if (else_stmt) {
		assert(case_stmt && last_if && !last_if->f);
		last_if->f = else_stmt;
	}
	return case_stmt;
}

/* return val;
 */
static sql_exp *
rel_psm_return( sql_query *query, sql_subtype *restype, list *restypelist, symbol *return_sym )
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_value, FALSE};
	sql_exp *res = NULL;
	sql_rel *rel = NULL;
	bool requires_proj = false;

	if (restypelist)
		ek.card = card_relation;
	else if (return_sym->token == SQL_TABLE)
		return sql_error(sql, 02, SQLSTATE(42000) "RETURN: TABLE return not allowed for non table returning functions");
	if (return_sym->token == SQL_COLUMN && restypelist) { /* RETURN x; where x is a reference to a table */
		dlist *l = return_sym->data.lval;
		const char *sname = qname_schema(l);
		const char *tname = qname_schema_object(l);
		sql_table *t = NULL;

		if (!(t = find_table_or_view_on_scope(sql, NULL, sname, tname, "RETURN", false)))
			return NULL;
		if (isDeclaredTable(t)) {
			rel = rel_table(sql, ddl_create_table, "sys", t, SQL_DECLARED_TABLE);
		} else {
			rel = rel_basetable(sql, t, t->base.name);
			rel = rel_base_add_columns(sql, rel);
		}
	} else { /* other cases */
		res = rel_value_exp2(query, &rel, return_sym, sql_sel, ek);
		if (!res)
			return NULL;
		if (!rel && exp_is_rel(res)) {
			rel = exp_rel_get_rel(sql->sa, res);
			if (rel && !restypelist && !is_groupby(rel->op)) { /* On regular functions return zero or 1 rows for every row */
				rel->card = CARD_MULTI;
				rel = rel_return_zero_or_one(sql, rel, ek);
				if (list_length(rel->exps) != 1)
					return sql_error(sql, 02, SQLSTATE(42000) "RETURN: must return a single column");
				res = exp_ref(sql, (sql_exp*) rel->exps->t->data);
				requires_proj = true;
			}
		}
	}

	if (ek.card != card_relation && (!restype || (res = exp_check_type(sql, restype, rel, res, type_equal)) == NULL))
		return (!restype)?sql_error(sql, 02, SQLSTATE(42000) "RETURN: return type does not match"):NULL;
	else if (ek.card == card_relation && !rel)
		return NULL;

	if (requires_proj) {
		rel = rel_project(sql->sa, rel, list_append(sa_list(sql->sa), res));
		res = exp_rel(sql, rel);
	}

	if (rel && !is_ddl(rel->op) && ek.card == card_relation) {
		list *exps = sa_list(sql->sa), *oexps = rel->exps;
		node *n, *m;
		int isproject = (rel->op == op_project);
		sql_rel *l = rel->l, *oexps_rel = rel;

		if (is_topn(rel->op) || is_sample(rel->op)) {
			oexps_rel = l;
			oexps = l->exps;
		}
		if (list_length(oexps) != list_length(restypelist))
			return sql_error(sql, 02, SQLSTATE(42000) "RETURN: number of columns do not match");
		for (n = oexps->h, m = restypelist->h; n && m; n = n->next, m = m->next) {
			sql_exp *e = n->data;
			sql_arg *ce = m->data;
			const char *cname = exp_name(e);
			char name[16];

			if (!cname)
				cname = sa_strdup(sql->sa, number2name(name, sizeof(name), ++sql->label));
			if (!isproject)
				e = exp_ref(sql, e);
			e = exp_check_type(sql, &ce->type, oexps_rel, e, type_equal);
			if (!e)
				return NULL;
			append(exps, e);
		}
		if (isproject)
			rel->exps = exps;
		else
			rel = rel_project(sql->sa, rel, exps);
		res = exp_rel(sql, rel);
	} else if (rel && restypelist) { /* handle return table-var */
		list *exps = sa_list(sql->sa);
		sql_table *t = rel_ddl_table_get(rel);
		node *n, *m;
		const char *tname = t->base.name;

		if (ol_length(t->columns) != list_length(restypelist))
			return sql_error(sql, 02, SQLSTATE(42000) "RETURN: number of columns do not match");
		for (n = ol_first_node(t->columns), m = restypelist->h; n && m; n = n->next, m = m->next) {
			sql_column *c = n->data;
			sql_arg *ce = m->data;
			sql_exp *e = exp_column(sql->sa, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0);

			e = exp_check_type(sql, &ce->type, rel, e, type_equal);
			if (!e)
				return NULL;
			append(exps, e);
		}
		rel = rel_project(sql->sa, rel, exps);
		res = exp_rel(sql, rel);
	}
	res = exp_return(sql->sa, res, stack_nr_of_declared_tables(sql));
	if (ek.card != card_relation)
		res->card = CARD_ATOM;
	else
		res->card = CARD_MULTI;
	return res;
}

static list *
rel_select_into( sql_query *query, symbol *sq, exp_kind ek)
{
	mvc *sql = query->sql;
	SelectNode *sn = (SelectNode*)sq;
	dlist *into = sn->into;
	node *m;
	dnode *n;
	sql_rel *r;
	list *nl = NULL;

	/* SELECT ... INTO var_list */
	sn->into = NULL;
	r = rel_subquery(query, NULL, sq, ek);
	if (!r)
		return NULL;
	if (!is_project(r->op))
		return sql_error(sql, 02, SQLSTATE(42000) "SELECT INTO: The subquery is not a projection");
	if (list_length(r->exps) != dlist_length(into))
		return sql_error(sql, 02, SQLSTATE(21S01) "SELECT INTO: number of values doesn't match number of variables to set");
	r = rel_return_zero_or_one(sql, r, ek);
	nl = sa_list(sql->sa);
	append(nl, exp_rel(sql, r));
	for (m = r->exps->h, n = into->h; m && n; m = m->next, n = n->next) {
		dlist *qname = n->data.lval;
		const char *sname = qname_schema(qname);
		const char *vname = qname_schema_object(qname);
		sql_exp *v = m->data;
		int level;
		sql_var *var;
		sql_subtype *tpe;
		sql_arg *a = NULL;

		if (!find_variable_on_scope(sql, sname, vname, &var, &a, &tpe, &level, "SELECT INTO"))
			return NULL;

		v = exp_ref(sql, v);
		if (!(v = exp_check_type(sql, tpe, r, v, type_equal)))
			return NULL;
		v = exp_set(sql->sa, var && var->sname ? sa_strdup(sql->sa, var->sname) : NULL, sa_strdup(sql->sa, vname), v, level);
		list_append(nl, v);
	}
	return nl;
}

extern sql_rel *
rel_select_with_into(sql_query *query, symbol *sq)
{
	exp_kind ek = {type_relation, card_value, TRUE};
	list *reslist = rel_select_into(query, sq, ek);
	if (!reslist)
		return NULL;
	return rel_psm_block(query->sql->sa, reslist);
}

static int while_exps_find_one_return(list *l);

static int
while_exp_find_one_return(sql_exp *e)
{
	if (e->flag & PSM_RETURN)
		return 1;
	if (e->flag & PSM_WHILE)
		return while_exps_find_one_return(e->r);
	if (e->flag & PSM_IF)
		return while_exps_find_one_return(e->r) || (e->f && while_exps_find_one_return(e->f));
	return 0;
}

static int
while_exps_find_one_return(list *l)
{
	int res = 0;
	for (node *n = l->h ; n && !res; n = n->next)
		res |= while_exp_find_one_return(n->data);
	return res;
}

static int has_return( list *l );

static int
exp_has_return(sql_exp *e)
{
	if (e->type == e_psm) {
		if (e->flag & PSM_RETURN)
			return 1;
		if (e->flag & PSM_IF) /* for if, both sides must exist and both must have a return */
			return has_return(e->r) && e->f && has_return(e->f);
		if (e->flag & PSM_WHILE) /* for while, at least one of the statements must have a return */
			return while_exps_find_one_return(e->r);
	}
	return 0;
}

static int
has_return( list *l )
{
	node *n = l->t;

	/* last statment of sequential block */
	if (n && exp_has_return(n->data))
		return 1;
	return 0;
}

static list *
sequential_block(sql_query *query, sql_subtype *restype, list *restypelist, dlist *blk, char *opt_label, int is_func)
{
	mvc *sql = query->sql;
	list *l=0;
	dnode *n;

	assert(!restype || !restypelist);

 	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (blk->h)
 		l = sa_list(sql->sa);
	if (!stack_push_frame(sql, opt_label))
		return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	for (n = blk->h; n; n = n->next ) {
		sql_exp *res = NULL;
		list *reslist = NULL;
		symbol *s = n->data.sym;

		switch (s->token) {
		case SQL_SET:
			res = psm_set_exp(query, s->data.lval->h);
			break;
		case SQL_DECLARE:
			reslist = rel_psm_declare(sql, s->data.lval->h);
			break;
		case SQL_DECLARE_TABLE:
		case SQL_CREATE_TABLE:
			res = rel_psm_declare_table(query, s->data.lval->h);
			break;
		case SQL_WHILE:
			res = rel_psm_while_do(query, restype, restypelist, s->data.lval->h, is_func);
			break;
		case SQL_IF:
			res = rel_psm_if_then_else(query, restype, restypelist, s->data.lval->h, is_func);
			break;
		case SQL_CASE:
			res = rel_psm_case(query, restype, restypelist, s->data.lval->h, is_func);
			break;
		case SQL_CALL:
			res = rel_psm_call(query, s->data.sym);
			break;
		case SQL_RETURN:
			/*If it is not a function it cannot have a return statement*/
			if (!is_func)
				res = sql_error(sql, 01, SQLSTATE(42000) "Return statement in the procedure body");
			else if (n->next) /* should be last statement of a sequential_block */
				res = sql_error(sql, 01, SQLSTATE(42000) "Statement after return");
			else
				res = rel_psm_return(query, restype, restypelist, s->data.sym);
			break;
		case SQL_SELECT: { /* row selections (into variables) */
			exp_kind ek = {type_value, card_row, TRUE};
			reslist = rel_select_into(query, s, ek);
		}	break;
		case SQL_COPYFROM:
		case SQL_BINCOPYFROM:
		case SQL_INSERT:
		case SQL_UPDATE:
		case SQL_DELETE:
		case SQL_TRUNCATE:
		case SQL_MERGE: {
			sql_rel *r = rel_updates(query, s);
			if (!r) {
				stack_pop_frame(sql);
				return NULL;
			}
			res = exp_rel(sql, r);
		}	break;
		default:
			res = sql_error(sql, 01, SQLSTATE(42000) "Statement '%s' is not a valid flow control statement",
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
				return sql_error(sql, ERR_AMBIGUOUS, SQLSTATE(42000) "CREATE FUNC: identifier '%s' ambiguous", n->data.sval);

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
	rel->flag = ddl_create_function;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
rel_create_func(sql_query *query, dlist *qname, dlist *params, symbol *res, dlist *ext_name, dlist *body, sql_ftype type, sql_flang lang, int replace)
{
	mvc *sql = query->sql;
	const char *fname = qname_schema_object(qname);
	const char *sname = qname_schema(qname);
	sql_schema *s = cur_schema(sql);
	sql_func *f = NULL;
	sql_subfunc *sf;
	dnode *n;
	list *type_list = NULL, *restype = NULL;
	int instantiate = (sql->emode == m_instantiate);
	int deps = (sql->emode == m_deps);
	int create = (!instantiate && !deps);
	bit vararg = FALSE;
	char *F = NULL, *fn = NULL, is_func;

	if (res && res->token == SQL_TABLE)
		type = F_UNION;

	FUNC_TYPE_STR(type, F, fn)

	is_func = (type != F_PROC && type != F_LOADER);
	assert(lang != FUNC_LANG_INT);

	if (create && store_readonly(sql->session->tr->store))
		return sql_error(sql, 06, SQLSTATE(42000) "Schema statements cannot be executed on a readonly database.");

	if (res && type == F_PROC)
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: procedures cannot have return parameters", F);
	else if (res && (type == F_FILT || type == F_LOADER))
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: %s functions don't have to specify a return type", F, fn);
	else if (!res && !(type == F_PROC || type == F_FILT || type == F_LOADER))
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: %ss require a return type", F, fn);
	else if (lang == FUNC_LANG_MAL && type == F_LOADER)
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: %s functions creation via MAL not supported", F, fn);
	else if (lang == FUNC_LANG_SQL && !(type == F_FUNC || type == F_PROC || type == F_UNION))
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: %s functions creation via SQL not supported", F, fn);
	else if (LANG_EXT(lang) && !(type == F_FUNC || type == F_AGGR || type == F_UNION || type == F_LOADER))
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: %ss creation via external programming languages not supported", F, fn);

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "CREATE %s: no such schema '%s'", F, sname);

	type_list = create_type_list(sql, params, 1);
	if (type == F_FUNC || type == F_AGGR || type == F_FILT) {
		sql_ftype ftpyes[3] = {F_FUNC, F_AGGR, F_FILT};

		for (int i = 0; i < 3; i++) {
			if (ftpyes[i] != type) {
				if (sql_bind_func_(sql, s->base.name, fname, type_list, ftpyes[i]))
					return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: there's %s with the name '%s' and the same parameters, which causes ambiguous calls", F,
									 (ftpyes[i] == F_AGGR) ? "an aggregate" : (ftpyes[i] == F_FILT) ? "a filter function" : "a function", fname);
				sql->session->status = 0; /* if the function was not found clean the error */
				sql->errstr[0] = '\0';
			}
		}
	}

	if ((sf = sql_bind_func_(sql, s->base.name, fname, type_list, type)) != NULL && create) {
		if (replace) {
			sql_func *func = sf->func;
			if (!mvc_schema_privs(sql, s))
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE OR REPLACE %s: access denied for %s to schema '%s'", F, get_string_global_var(sql, "current_user"), s->base.name);
			if (mvc_check_dependency(sql, func->base.id, !IS_PROC(func) ? FUNC_DEPENDENCY : PROC_DEPENDENCY, NULL))
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE OR REPLACE %s: there are database objects dependent on %s %s;", F, fn, func->base.name);
			if (!func->s)
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE OR REPLACE %s: not allowed to replace system %s %s;", F, fn, func->base.name);
			if (mvc_drop_func(sql, s, func, 0))
				return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
			sf = NULL;
		} else {
			if (params) {
				char *arg_list = NULL;
				node *n;

				for (n = type_list->h; n; n = n->next) {
					char *tpe =  sql_subtype_string(sql->ta, (sql_subtype *) n->data);

					if (arg_list) {
						arg_list = sa_message(sql->ta, "%s, %s", arg_list, tpe);
					} else {
						arg_list = tpe;
					}
				}
				(void)sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: name '%s' (%s) already in use", F, fname, arg_list ? arg_list : "");
				list_destroy(type_list);
				return NULL;
			} else {
				list_destroy(type_list);
				return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: name '%s' already in use", F, fname);
			}
		}
	} else {
		sql->session->status = 0; /* if the function was not found clean the error */
		sql->errstr[0] = '\0';
	}
	list_destroy(type_list);
	if (create && !mvc_schema_privs(sql, s)) {
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE %s: insufficient privileges for user '%s' in schema '%s'", F,
						 get_string_global_var(sql, "current_user"), s->base.name);
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
				return sql_error(sql, 01, SQLSTATE(42000) "CREATE %s: failed to get restype", F);
		}
		if (body && LANG_EXT(lang)) {
			const char *lang_body = body->h->data.sval, *mod = "unknown", *slang = "Unknown";
			switch (lang) {
			case FUNC_LANG_R:
				mod = "rapi";
				slang = "R";
				break;
			case FUNC_LANG_C:
				mod = "capi";
				slang = "C";
				break;
			case FUNC_LANG_CPP:
				mod = "capi";
				slang = "CPP";
				break;
			case FUNC_LANG_J:
				mod = "japi";
				slang = "Javascript";
				break;
			case FUNC_LANG_PY:
				mod = "pyapi";
				slang = "Python";
				break;
			case FUNC_LANG_MAP_PY:
				mod = "pyapimap";
				slang = "Python";
				break;
			case FUNC_LANG_PY3:
				mod = "pyapi3";
				slang = "Python";
				break;
			case FUNC_LANG_MAP_PY3:
				mod = "pyapi3map";
				slang = "Python";
				break;
			default:
				assert(0);
			}

			if (type == F_LOADER && !(lang == FUNC_LANG_PY || lang == FUNC_LANG_PY3))
				return sql_error(sql, 01, SQLSTATE(42000) "CREATE %s: Language name \"Python[3]\" expected", F);

			sql->params = NULL;
			if (create) {
				f = mvc_create_func(sql, sql->sa, s, fname, l, restype, type, lang, mod, fname, lang_body, (type == F_LOADER)?TRUE:FALSE, vararg, FALSE);
			} else if (!sf) {
				return sql_error(sql, 01, SQLSTATE(42000) "CREATE %s: %s function %s.%s not bound", F, slang, s->base.name, fname);
			}
		} else if (body) { /* SQL implementation */
			sql_arg *ra = (restype && type != F_UNION)?restype->h->data:NULL;
			list *b = NULL;
			sql_schema *os = cur_schema(sql);

			if (create) { /* needed for recursive functions */
				q = query_cleaned(sql->ta, q);
				sql->forward = f = mvc_create_func(sql, sql->sa, s, fname, l, restype, type, lang, sql_shared_module_name, q, q, FALSE, vararg, FALSE);
			}
			sql->session->schema = s;
			b = sequential_block(query, (ra)?&ra->type:NULL, ra?NULL:restype, body, NULL, is_func);
			sql->forward = NULL;
			sql->session->schema = os;
			sql->params = NULL;
			if (!b)
				return NULL;

			/* check if we have a return statement */
			if (is_func && restype && !has_return(b))
				return sql_error(sql, 01, SQLSTATE(42000) "CREATE %s: missing return statement", F);
			if (!is_func && !restype && has_return(b))
				return sql_error(sql, 01, SQLSTATE(42000) "CREATE %s: %ss cannot have return statements", F, fn);
			/* in execute mode we instantiate the function */
			if (instantiate || deps)
				return rel_psm_block(sql->sa, b);
		} else { /* MAL implementation */
			char *fmod = qname_module(ext_name);
			char *fnme = qname_schema_object(ext_name);
			int clientid = sql->clientid;

			if (!fmod || !fnme)
				return NULL;
			sql->params = NULL;
			if (create) {
				q = query_cleaned(sql->ta, q);
				f = mvc_create_func(sql, sql->sa, s, fname, l, restype, type, lang, fmod, fnme, q, FALSE, vararg, FALSE);
			} else if (!sf) {
				return sql_error(sql, 01, SQLSTATE(42000) "CREATE %s: external name %s.%s not bound (%s.%s)", F, fmod, fnme, s->base.name, fname );
			} else {
				sql_func *f = sf->func;
				if (!f->mod || strcmp(f->mod, fmod)) {
					_DELETE(f->mod);
					f->mod = SA_STRDUP(NULL, fmod) ;//(f->sa)?sa_strdup(f->sa, fmod):sa_strdup(sql->pa, fmod);
				}
				if (!f->imp || strcmp(f->imp, fnme)) {
					_DELETE(f->imp);
					f->imp = SA_STRDUP(NULL, fnme);//(f->sa)?sa_strdup(f->sa, fnme):sa_strdup(sql->pa, fnme);
				}
				if (!f->mod || !f->imp)
					return sql_error(sql, 02, SQLSTATE(HY013) "CREATE %s: could not allocate space", F);
				f->sql = 0; /* native */
				f->lang = FUNC_LANG_INT;
			}
			if (!f)
				f = sf->func;
			assert(f);
			if (!backend_resolve_function(&clientid, f))
				return sql_error(sql, 01, SQLSTATE(3F000) "CREATE %s: external name %s.%s not bound (%s.%s)", F, fmod, fnme, s->base.name, fname );
		}
	}
	return rel_create_function(sql->sa, s->base.name, f);
}

static sql_rel*
rel_drop_function(sql_allocator *sa, const char *sname, const char *name, int nr, sql_ftype type, int action)
{
	sql_rel *rel = rel_create(sa);
	list *exps = new_exp_list(sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_clob(sa, sname));
	append(exps, exp_atom_clob(sa, name));
	append(exps, exp_atom_int(sa, nr));
	append(exps, exp_atom_int(sa, (int) type));
	append(exps, exp_atom_int(sa, action));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_drop_function;
	rel->exps = exps;
	rel->card = 0;
	rel->nrcols = 0;
	return rel;
}

sql_func *
resolve_func(mvc *sql, const char *sname, const char *name, dlist *typelist, sql_ftype type, const char *op, int if_exists)
{
	sql_func *func = NULL;
	list *list_func = NULL, *type_list = NULL;
	char is_func = (type != F_PROC && type != F_LOADER), *F = NULL, *fn = NULL;

	FUNC_TYPE_STR(type, F, fn)

	if (typelist) {
		sql_subfunc *sub_func;

		type_list = create_type_list(sql, typelist, 0);
		sub_func = sql_bind_func_(sql, sname, name, type_list, type);
		if (!sub_func && type == F_FUNC) {
			sql->session->status = 0; /* if the function was not found clean the error */
			sql->errstr[0] = '\0';
			sub_func = sql_bind_func_(sql, sname, name, type_list, F_UNION);
			type = sub_func?F_UNION:F_FUNC;
		}
		if ( sub_func && sub_func->func->type == type)
			func = sub_func->func;
	} else {
		list_func = sql_find_funcs_by_name(sql, sname, name, type);
		if (!list_func && type == F_FUNC) {
			sql->session->status = 0; /* if the function was not found clean the error */
			sql->errstr[0] = '\0';
			list_func = sql_find_funcs_by_name(sql, sname, name, F_UNION);
		}
		if (list_func && list_func->cnt > 1) {
			list_destroy(list_func);
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "%s %s: there are more than one %s called '%s', please use the full signature", op, F, fn, name);
		}
		if (list_func && list_func->cnt == 1)
			func = (sql_func*) list_func->h->data;
	}

	if (!func) {
		void *e = NULL;
		if (typelist) {
			char *arg_list = NULL;
			node *n;

			if (type_list->cnt > 0) {
				for (n = type_list->h; n; n = n->next) {
					char *tpe =  sql_subtype_string(sql->ta, (sql_subtype *) n->data);

					if (arg_list) {
						arg_list = sa_message(sql->ta, "%s, %s", arg_list, tpe);
					} else {
						arg_list = tpe;
					}
				}
				list_destroy(list_func);
				list_destroy(type_list);
				if (!if_exists)
					e = sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "%s %s: no such %s '%s' (%s)", op, F, fn, name, arg_list);
				return e;
			}
			list_destroy(list_func);
			list_destroy(type_list);
			if (!if_exists)
				e = sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "%s %s: no such %s '%s' ()", op, F, fn, name);
			return e;
		} else {
			if (!if_exists)
				e = sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "%s %s: no such %s '%s'", op, F, fn, name);
			return e;
		}
	} else if (((is_func && type != F_FILT) && !func->res) || (!is_func && func->res)) {
		list_destroy(list_func);
		list_destroy(type_list);
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "%s %s: cannot drop %s '%s'", op, F, fn, name);
	}

	list_destroy(list_func);
	list_destroy(type_list);
	return func;
}

static sql_rel*
rel_drop_func(mvc *sql, dlist *qname, dlist *typelist, int drop_action, sql_ftype type, int if_exists)
{
	const char *name = qname_schema_object(qname);
	const char *sname = qname_schema(qname);
	sql_func *func = NULL;
	char *F = NULL, *fn = NULL;

	FUNC_TYPE_STR(type, F, fn)

	if (!(func = resolve_func(sql, sname, name, typelist, type, "DROP", if_exists))) {
		if (if_exists) {
			sql->errstr[0] = '\0'; /* reset function not found error */
			sql->session->status = 0;
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		}
		return NULL;
	}
	if (!func->s) /* attempting to drop a system function */
		return sql_error(sql, 02, SQLSTATE(42000) "DROP %s: cannot drop system %s '%s'", F, fn, name);
	if (!mvc_schema_privs(sql, func->s))
		return sql_error(sql, 02, SQLSTATE(42000) "DROP %s: insufficient privileges for user '%s' in schema '%s'", F, get_string_global_var(sql, "current_user"), func->s->base.name);
	return rel_drop_function(sql->sa, func->s->base.name, name, func->base.id, type, drop_action);
}

static sql_rel*
rel_drop_all_func(mvc *sql, dlist *qname, int drop_action, sql_ftype type)
{
	const char *name = qname_schema_object(qname);
	const char *sname = qname_schema(qname);
	sql_schema *s = cur_schema(sql);
	list *list_func = NULL;
	char *F = NULL, *fn = NULL;

	FUNC_TYPE_STR(type, F, fn)

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, 02, SQLSTATE(3F000) "DROP ALL %s: no such schema '%s'", F, sname);
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(42000) "DROP ALL %s: insufficient privileges for user '%s' in schema '%s'", F, get_string_global_var(sql, "current_user"), s->base.name);

	if (!(list_func = sql_find_funcs_by_name(sql, s->base.name, name, type)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "DROP ALL %s: no such %s '%s'", F, fn, name);
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
	rel->flag = ddl_create_trigger;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = 0;
	return rel;
}

static sql_rel_view*
_stack_push_table(mvc *sql, const char *tname, sql_table *t)
{
	sql_rel *r = rel_basetable(sql, t, tname );
	rel_base_use_all(sql, r);
	r = rewrite_basetable(sql, r);
	return stack_push_rel_view(sql, tname, r);
}

static sql_rel *
create_trigger(sql_query *query, dlist *qname, int time, symbol *trigger_event, dlist *tqname, dlist *opt_ref, dlist *triggered_action, int replace)
{
	mvc *sql = query->sql;
	const char *triggerschema = qname_schema(qname);
	const char *triggername = qname_schema_object(qname);
	const char *sname = qname_schema(tqname);
	const char *tname = qname_schema_object(tqname);
	int instantiate = (sql->emode == m_instantiate);
	int create = (!instantiate && sql->emode != m_deps), event, orientation;
	sql_schema *ss = cur_schema(sql), *old_schema = cur_schema(sql);
	sql_table *t = NULL;
	sql_trigger *st = NULL;
	list *sq = NULL;
	sql_rel *r = NULL;
	char *q, *base = replace ? "CREATE OR REPLACE TRIGGER" : "CREATE TRIGGER";
	dlist *columns = trigger_event->data.lval;
	const char *old_name = NULL, *new_name = NULL;
	dlist *stmts = triggered_action->h->next->next->data.lval;
	symbol *condition = triggered_action->h->next->data.sym;
	int8_t old_useviews = sql->use_views;

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

	if (sname && !(ss = mvc_bind_schema(sql, sname)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "%s: no such schema '%s'", base, sname);

	if (create) {
		if (triggerschema)
			return sql_error(sql, 02, SQLSTATE(42000) "%s: a trigger will be placed on the respective table's schema, specify the schema on the table reference, ie ON clause instead", base);
		if (!(t = mvc_bind_table(sql, ss, tname)))
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42S02) "%s: no such table %s%s%s'%s'", base, sname ? "'":"", sname ? sname : "", sname ? "'.":"", tname);
		if (!mvc_schema_privs(sql, ss))
			return sql_error(sql, 02, SQLSTATE(42000) "%s: access denied for %s to schema '%s'", base, get_string_global_var(sql, "current_user"), ss->base.name);
		if (isView(t))
			return sql_error(sql, 02, SQLSTATE(42000) "%s: cannot create trigger on view '%s'", base, tname);
		if ((st = mvc_bind_trigger(sql, ss, triggername)) != NULL) {
			if (replace) {
				if (mvc_drop_trigger(sql, ss, st))
					return sql_error(sql, 02, SQLSTATE(HY013) "%s: %s", base, MAL_MALLOC_FAIL);
			} else {
				return sql_error(sql, 02, SQLSTATE(42000) "%s: name '%s' already in use", base, triggername);
			}
		}
		switch (trigger_event->token) {
			case SQL_INSERT: {
				if (old_name)
					return sql_error(sql, 02, SQLSTATE(42000) "%s: old name not allowed at insert events", base);
				event = 0;
			}	break;
			case SQL_DELETE: {
				if (new_name)
					return sql_error(sql, 02, SQLSTATE(42000) "%s: new name not allowed at delete events", base);
				event = 1;
			}	break;
			case SQL_TRUNCATE: {
				if (new_name)
					return sql_error(sql, 02, SQLSTATE(42000) "%s: new name not allowed at truncate events", base);
				event = 3;
			}	break;
			case SQL_UPDATE: {
				if (old_name && new_name && !strcmp(old_name, new_name))
					return sql_error(sql, 02, SQLSTATE(42000) "%s: old and new names cannot be the same", base);
				if (!old_name && new_name && !strcmp("old", new_name))
					return sql_error(sql, 02, SQLSTATE(42000) "%s: old and new names cannot be the same", base);
				if (!new_name && old_name && !strcmp("new", old_name))
					return sql_error(sql, 02, SQLSTATE(42000) "%s: old and new names cannot be the same", base);
				event = 2;
			}	break;
			default:
				return sql_error(sql, 02, SQLSTATE(42000) "%s: invalid event: %s", base, token2string(trigger_event->token));
		}

		assert(triggered_action->h->type == type_int);
		orientation = triggered_action->h->data.i_val;
		q = query_cleaned(sql->ta, QUERY(sql->scanner));
		r = rel_create_trigger(sql, t->s->base.name, t->base.name, triggername, time, orientation, event, old_name, new_name, condition, q);
		return r;
	}

	if (!instantiate) {
		t = mvc_bind_table(sql, ss, tname);
		if (!stack_push_frame(sql, "%OLD-NEW"))
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		/* we need to add the old and new tables */
		if (new_name && !_stack_push_table(sql, new_name, t)) {
			stack_pop_frame(sql);
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
		if (old_name && !_stack_push_table(sql, old_name, t)) {
			stack_pop_frame(sql);
			return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		}
	}
	if (condition) {
		sql_rel *rel = NULL;

		if (new_name) /* in case of updates same relations is available via both names */
			rel = stack_find_rel_view(sql, new_name);
		if (!rel && old_name)
			rel = stack_find_rel_view(sql, old_name);
		if (!rel)
			rel = stack_find_rel_view(sql, "old");
		if (!rel)
			rel = stack_find_rel_view(sql, "new");
		rel = rel_logical_exp(query, rel, condition, sql_where);
		if (!rel) {
			if (!instantiate)
				stack_pop_frame(sql);
			return NULL;
		}
		/* transition tables */
		/* insert: rel_select(table [new], searchcondition) */
		/* delete: rel_select(table [old], searchcondition) */
		/* update: rel_select(table [old,new]), searchcondition) */
		if (new_name)
			stack_update_rel_view(sql, new_name, rel);
		if (old_name)
			stack_update_rel_view(sql, old_name, new_name?rel_dup(rel):rel);
	}
	sql->use_views = 1; /* leave the 'use_views' hack to where it belongs */
	sql->session->schema = ss;
	sq = sequential_block(query, NULL, NULL, stmts, NULL, 1);
	sql->session->schema = old_schema;
	sql->use_views = old_useviews;
	if (!sq) {
		if (!instantiate)
			stack_pop_frame(sql);
		return NULL;
	}
	r = rel_psm_block(sql->sa, sq);

	if (!instantiate)
		stack_pop_frame(sql);
	/* todo trigger_columns */
	(void)columns;
	return r;
}

static sql_rel *
rel_drop_trigger(mvc *sql, const char *sname, const char *tname, int if_exists)
{
	sql_rel *rel = rel_create(sql->sa);
	list *exps = new_exp_list(sql->sa);
	if(!rel || !exps)
		return NULL;

	append(exps, exp_atom_str(sql->sa, sname, sql_bind_localtype("str") ));
	append(exps, exp_atom_str(sql->sa, tname, sql_bind_localtype("str") ));
	append(exps, exp_atom_int(sql->sa, if_exists));
	rel->l = NULL;
	rel->r = NULL;
	rel->op = op_ddl;
	rel->flag = ddl_drop_trigger;
	rel->exps = exps;
	rel->card = CARD_MULTI;
	rel->nrcols = 0;
	return rel;
}

static sql_rel *
drop_trigger(mvc *sql, dlist *qname, int if_exists)
{
	const char *sname = qname_schema(qname);
	const char *tname = qname_schema_object(qname);
	sql_trigger *tr = NULL;

	if (!(tr = find_trigger_on_scope(sql, sname, tname, "DROP TRIGGER"))) {
		if (if_exists) {
			sql->errstr[0] = '\0'; /* reset trigger not found error */
			sql->session->status = 0;
			return rel_psm_block(sql->sa, new_exp_list(sql->sa));
		}
		return NULL;
	}
	if (!mvc_schema_privs(sql, tr->t->s))
		return sql_error(sql, 02, SQLSTATE(3F000) "DROP TRIGGER: access denied for %s to schema '%s'", get_string_global_var(sql, "current_user"), tr->t->s->base.name);
	return rel_drop_trigger(sql, tr->t->s->base.name, tname, if_exists);
}

static sql_rel *
psm_analyze(sql_query *query, char *analyzeType, dlist *qname, dlist *columns, symbol *sample, int minmax )
{
	mvc *sql = query->sql;
	exp_kind ek = {type_value, card_value, FALSE};
	sql_exp *sample_exp = NULL, *call, *mm_exp = NULL;
	const char *sname = qname_schema(qname), *tname = qname_schema_object(qname);
	list *tl = sa_list(sql->sa);
	list *exps = sa_list(sql->sa), *analyze_calls = sa_list(sql->sa);
	sql_subfunc *f = NULL;

	append(exps, mm_exp = exp_atom_int(sql->sa, minmax));
	append(tl, exp_subtype(mm_exp));
	if (sample) {
		sql_rel *rel = NULL;
		sample_exp = rel_value_exp(query, &rel, sample, sql_sel | sql_psm, ek);
		psm_zero_or_one(sample_exp);
		if (!sample_exp || !(sample_exp = exp_check_type(sql, sql_bind_localtype("lng"), NULL, sample_exp, type_cast)))
			return NULL;
	} else {
		sample_exp = exp_atom_lng(sql->sa, 0);
	}
	append(exps, sample_exp);
	append(tl, exp_subtype(sample_exp));

	if (sname && tname) {
		sql_table *t = NULL;

		if (!(t = find_table_or_view_on_scope(sql, NULL, sname, tname, "ANALYZE", false)))
			return NULL;
		if (isDeclaredTable(t))
			return sql_error(sql, 02, SQLSTATE(42000) "Cannot analyze a declared table");
		sname = t->s->base.name;
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
		if (!(f = sql_bind_func_(sql, "sys", analyzeType, tl, F_PROC)))
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "Analyze procedure missing");
		call = exp_op(sql->sa, exps, f);
		append(analyze_calls, call);
	} else {
		if (!sname || !tname)
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "Analyze schema or table name missing");
		if (!(f = sql_bind_func_(sql, "sys", analyzeType, tl, F_PROC)))
			return sql_error(sql, ERR_NOTFOUND, SQLSTATE(42000) "Analyze procedure missing");
		for(dnode *n = columns->h; n; n = n->next) {
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
create_table_from_loader(sql_query *query, dlist *qname, symbol *fcall)
{
	mvc *sql = query->sql;
	sql_schema *s = cur_schema(sql);
	char *sname = qname_schema(qname);
	char *tname = qname_schema_object(qname);
	sql_subfunc *loader = NULL;
	sql_rel *rel = NULL;
	sql_table *t = NULL;

	if (sname && !(s = mvc_bind_schema(sql, sname)))
		return sql_error(sql, ERR_NOTFOUND, SQLSTATE(3F000) "CREATE TABLE FROM LOADER: no such schema '%s'", sname);
	if ((t = mvc_bind_table(sql, s, tname)))
		return sql_error(sql, 02, SQLSTATE(42S01) "CREATE TABLE FROM LOADER: name '%s' already in use", tname);
	if (!mvc_schema_privs(sql, s))
		return sql_error(sql, 02, SQLSTATE(42000) "CREATE TABLE FROM LOADER: insufficient privileges for user '%s' in schema '%s'", get_string_global_var(sql, "current_user"), s->base.name);

	rel = rel_loader_function(query, fcall, new_exp_list(sql->sa), &loader);
	if (!rel || !loader)
		return NULL;

	loader->sname = s ? sa_strdup(sql->sa, s->base.name) : NULL;
	loader->tname = tname ? sa_strdup(sql->sa, tname) : NULL;

	return rel;
}

sql_rel *
rel_psm(sql_query *query, symbol *s)
{
	mvc *sql = query->sql;
	sql_rel *ret = NULL;

	switch (s->token) {
	case SQL_CREATE_FUNC:
	{
		dlist *l = s->data.lval;
		sql_ftype type = (sql_ftype) l->h->next->next->next->next->next->data.i_val;
		sql_flang lang = (sql_flang) l->h->next->next->next->next->next->next->data.i_val;
		int repl = l->h->next->next->next->next->next->next->next->data.i_val;

		ret = rel_create_func(query, l->h->data.lval, l->h->next->data.lval, l->h->next->next->data.sym, l->h->next->next->next->data.lval, l->h->next->next->next->next->data.lval, type, lang, repl);
		sql->type = Q_SCHEMA;
	} 	break;
	case SQL_DROP_FUNC:
	{
		dlist *l = s->data.lval;
		dlist *qname = l->h->data.lval;
		dlist *typelist = l->h->next->data.lval;
		sql_ftype type = (sql_ftype) l->h->next->next->data.i_val;
		int if_exists = l->h->next->next->next->data.i_val;
		int all = l->h->next->next->next->next->data.i_val;
		int drop_action = l->h->next->next->next->next->next->data.i_val;

		if (store_readonly(sql->session->tr->store))
			return sql_error(sql, 06, SQLSTATE(42000) "Schema statements cannot be executed on a readonly database.");

		if (all)
			ret = rel_drop_all_func(sql, qname, drop_action, type);
		else
			ret = rel_drop_func(sql, qname, typelist, drop_action, type, if_exists);

		sql->type = Q_SCHEMA;
	}	break;
	case SQL_SET:
		ret = rel_psm_stmt(sql->sa, psm_set_exp(query, s->data.lval->h));
		sql->type = Q_SCHEMA;
		break;
	case SQL_DECLARE:
		return sql_error(sql, 02, SQLSTATE(42000) "Variables cannot be declared on the global scope");
	case SQL_CALL:
		sql->type = Q_UPDATE;
		ret = rel_psm_stmt(sql->sa, rel_psm_call(query, s->data.sym));
		break;
	case SQL_CREATE_TABLE_LOADER:
	{
		dlist *l = s->data.lval;
		dlist *qname = l->h->data.lval;
		symbol *sym = l->h->next->data.sym;

		ret = create_table_from_loader(query, qname, sym);
		if (ret == NULL)
			return NULL;
		ret = rel_psm_stmt(sql->sa, exp_rel(sql, ret));
		sql->type = Q_SCHEMA;
	}	break;
	case SQL_CREATE_TRIGGER:
	{
		dlist *l = s->data.lval;

		assert(l->h->next->type == type_int);
		ret = create_trigger(query, l->h->data.lval, l->h->next->data.i_val, l->h->next->next->data.sym, l->h->next->next->next->data.lval, l->h->next->next->next->next->data.lval, l->h->next->next->next->next->next->data.lval, l->h->next->next->next->next->next->next->data.i_val);
		sql->type = Q_SCHEMA;
	} break;
	case SQL_DROP_TRIGGER:
	{
		dlist *l = s->data.lval;
		dlist *qname = l->h->data.lval;
		int if_exists = l->h->next->data.i_val;

		ret = drop_trigger(sql, qname, if_exists);
		sql->type = Q_SCHEMA;
	} break;
	case SQL_ANALYZE: {
		dlist *l = s->data.lval;

		ret = psm_analyze(query, "analyze", l->h->data.lval /* qualified table name */, l->h->next->data.lval /* opt list of column */, l->h->next->next->data.sym /* opt_sample_size */, l->h->next->next->next->data.i_val);
		sql->type = Q_UPDATE;
	} 	break;
	default:
		return sql_error(sql, 01, SQLSTATE(42000) "Schema statement unknown symbol(%p)->token = %s", s, token2string(s->token));
	}
	return ret;
}
