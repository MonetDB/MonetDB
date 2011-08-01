/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://www.monetdb.org/Legal/MonetDBLicense
 *
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 *
 * The Original Code is the MonetDB Database System.
 *
 * The Initial Developer of the Original Code is CWI.
 * Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
 * Copyright August 2008-2011 MonetDB B.V.
 * All Rights Reserved.
 */


#include "monetdb_config.h"
#include "rel_subquery.h"
#include "rel_select.h"
#include "rel_exp.h"
#include "rel_optimizer.h"
#include "rel_bin.h"
#include "sql_env.h"

stmt *
sql_unop_(mvc *sql, sql_schema *s, char *fname, stmt *rs)
{
	sql_subtype *rt = NULL;
	sql_subfunc *f = NULL;

	if (!s)
		s = sql->session->schema;
	rt = tail_type(rs);
	f = sql_bind_func(sql->sa, s, fname, rt, NULL);
	/* try to find the function without a type, and convert
	 * the value to the type needed by this function!
	 */
	if (!f && (f = sql_find_func(sql->sa, s, fname, 1)) != NULL) {
		sql_arg *a = f->func->ops->h->data;

		rs = check_types(sql, &a->type, rs, type_equal);
		if (!rs) 
			f = NULL;
	}
	if (f) {
		if (f->func->res.scale == INOUT) {
			f->res.digits = rt->digits;
			f->res.scale = rt->scale;
		}
		return stmt_unop(sql->sa, rs, f);
	} else if (rs) {
		char *type = tail_type(rs)->type->sqlname;

		return sql_error(sql, 02, "SELECT: no such unary operator '%s(%s)'", fname, type);
	}
	return NULL;
}

stmt *
sql_binop_(mvc *sql, sql_schema *s, char *fname, stmt *ls, stmt *rs)
{
	stmt *res = NULL;
	sql_subtype *t1, *t2;
	sql_subfunc *f = NULL;

	t1 = tail_type(ls);
	t2 = tail_type(rs);

	if (!s)
		s = sql->session->schema;
	f = sql_bind_func(sql->sa, s, fname, t1, t2);
	if (!f && is_commutative(fname)) {
		f = sql_bind_func(sql->sa, s, fname, t2, t1);
		if (f) {
			sql_subtype *tmp = t1;
			t1 = t2;	
			t2 = tmp;
			res = ls;		
			ls = rs;
			rs = res;
		}
	}
	if (f) {
		if (f->func->fix_scale == SCALE_FIX) {
			ls = fix_scale(sql, t2, ls, 0, 0);
			rs = fix_scale(sql, t1, rs, 0, 0);
		} else if (f->func->fix_scale == SCALE_DIV) {
			ls = scale_algebra(sql, f, ls, rs);
		} else if (f->func->fix_scale == SCALE_MUL) {
			ls = sum_scales(sql, f, ls, rs);
		} else if (f->func->fix_scale == DIGITS_ADD) {
			f->res.digits = t1->digits + t2->digits;
		}
		return stmt_binop(sql->sa, ls, rs, f);
	} else {
		int digits = t1->digits + t2->digits;
		stmt *ols = ls;
		stmt *ors = rs;

		/* try finding function based on first argument */
		if (!EC_NUMBER(t1->type->eclass) &&
		   (f = sql_bind_member(sql->sa, s, fname, t1, 2)) != NULL) {
			node *m = f->func->ops->h;
			sql_arg *a = m->data;

			ls = check_types(sql, &a->type, ls, type_equal);
			a = m->next->data;
			rs = check_types(sql, &a->type, rs, type_equal);
			if (ls && rs) 
				return stmt_binop(sql->sa, ls, rs, f);
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		ls = ols;
		rs = ors;
		/* try finding function based on both arguments */
		if (convert_types(sql, &ls, &rs, 1, type_equal) >= 0) {
			/* try operators */
			t1 = tail_type(ls);
			t2 = tail_type(rs);
			f = sql_bind_func(sql->sa, s, fname, t1, t2);
			if (f) {
				if (f->func->fix_scale == SCALE_FIX) {
					ls = fix_scale(sql, t2, ls, 0, 0);
					rs = fix_scale(sql, t1, rs, 0, 0);
				} else if (f->func->fix_scale == SCALE_DIV) {
					ls = scale_algebra(sql, f, ls, rs);
				} else if (f->func->fix_scale == SCALE_MUL) {
					ls = sum_scales(sql, f, ls, rs);
				} else if (f->func->fix_scale == DIGITS_ADD) {
					f->res.digits = digits;
				}
				return stmt_binop(sql->sa, ls, rs, f);
			}
		}
		/* reset error */
		sql->session->status = 0;
		sql->errstr[0] = '\0';

		ls = ols;
		rs = ors;
		/* everything failed, fall back to bind on function name only */
		if ((f = sql_find_func(sql->sa, s, fname, 2)) != NULL) {
			node *m = f->func->ops->h;
			sql_arg *a = m->data;

			ls = check_types(sql, &a->type, ls, type_equal);
			a = m->next->data;
			rs = check_types(sql, &a->type, rs, type_equal);
			if (ls && rs) 
				return stmt_binop(sql->sa, ls, rs, f);
		}
	}
	if (rs && ls)
		res = sql_error(sql, 02, "SELECT: no such binary operator '%s(%s,%s)'", fname, tail_type(ls)->type->sqlname, tail_type(rs)->type->sqlname);
	return res;
}

stmt *
sql_Nop_(mvc *sql, char *fname, stmt *a1, stmt *a2, stmt *a3, stmt *a4)
{
	list *sl = list_new(sql->sa);
	list *tl = list_create(NULL);
	sql_subfunc *f = NULL;

	list_append(sl, a1);
	list_append(tl, tail_type(a1));
	list_append(sl, a2);
	list_append(tl, tail_type(a2));
	list_append(sl, a3);
	list_append(tl, tail_type(a3));
	if (a4) {
		list_append(sl, a4);
		list_append(tl, tail_type(a4));
	}

	f = sql_bind_func_(sql->sa, sql->session->schema, fname, tl);
	list_destroy(tl);
	if (f)
		return stmt_Nop(sql->sa, stmt_list(sql->sa, sl), f);
	return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
}

static stmt *
_subquery(mvc *sql, symbol *sq, exp_kind ek)
{
	stmt *s = NULL;

	int status = sql->session->status;
	sql_rel *r = rel_subquery(sql, NULL, sq, ek);

	if (!r) {
		if (!sql->errstr[0]) 
			sql->session->status = status;
		return NULL;
	} else {
		r = rel_optimizer(sql, r);
		s = rel_bin(sql, r);
	}
	return s;
}

/* single value only */
stmt *
select_into( mvc *sql, symbol *sq, exp_kind ek) 
{
	SelectNode *sn = (SelectNode*)sq;
	dlist *into = sn->into;
	/* SELECT ... INTO var_list */
	stmt *s;

	sn->into = NULL;
	s = _subquery(sql, sq, ek);
	if (into && s) {
		list *rl = s->op4.lval;
		node *m;
		dnode *n;
		list *nl = list_new(sql->sa);

		assert(s->type == st_list);
		for (m = rl->h, n = into->h; m && n; m = m->next, n = n->next) {
			sql_subtype *tpe = NULL;
			char *nme = n->data.sval;
			stmt *a = NULL, *v = m->data;
			int level;

			if (!stack_find_var(sql, nme)) 
				return sql_error(sql, 02, "SELECT INTO: variable '%s' unknown", nme);
			/* dynamic check for single values */
			if (!v->key) {
				sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", tail_type(v));
				assert(zero_or_one);
				v = stmt_aggr(sql->sa, v, NULL, zero_or_one, 1);
			}
			tpe = stack_find_type(sql, nme);
			level = stack_find_frame(sql, nme);
			v = check_types(sql, tpe, v, type_equal); 
			if (!v) 
				return NULL;
			a = stmt_assign(sql->sa, nme, v, level);
			list_append(nl, a);
		}
		s = stmt_list(sql->sa, nl);
	}
	return s;
}

static stmt *
find_order(stmt *s)
{
	if (s->type == st_limit) 
		assert(s->op1->type == st_order || s->op1->type == st_reorder);
	else
		assert(s->type == st_order || s->type == st_reorder);
		
	while(s->type == st_reorder)
		s = s->op1;
	return s;
}

static stmt *
sql_reorder(mvc *sql, stmt *order, stmt *s) 
{
	list *l = list_new(sql->sa);
	node *n;
	/* we need to keep the order by column, to propagate the sort property*/
	stmt *o = find_order(order);
	stmt *x = o->op1;

	order = stmt_mark(sql->sa, stmt_reverse(sql->sa, order), 0);
	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *sc = n->data;
		char *cname = column_name(sql->sa, sc);
		char *tname = table_name(sql->sa, sc);

		if (sc != x)
			sc = stmt_reverse(sql->sa, stmt_order(sql->sa, stmt_reverse(sql->sa, stmt_join(sql->sa, order, sc, cmp_equal)), 1));
		else	/* first order by column */
			sc = stmt_mark(sql->sa, o, 0);
		sc = stmt_alias(sql->sa, sc, tname, cname );
		list_append(l, sc);
		
	}
	return stmt_list(sql->sa, l);
}

stmt *
value_exp(mvc *sql, symbol *sq, int f, exp_kind ek)
{
	int is_last = 0;
	stmt *s = NULL;
	int status = sql->session->status;
	sql_rel *r = NULL;
	sql_exp *e = rel_value_exp2(sql, &r, sq, f, ek, &is_last);

	if (!r && !e) {
		if (!sql->errstr[0]) 
			sql->session->status = status;
		return NULL;
	} else {
		if (r) {
			r = rel_optimizer(sql, r);
			s = rel_bin(sql, r);
		} else {
			s = exp_bin(sql, e, NULL, NULL, NULL, NULL);
		}

		if (s && s->type == st_list && !s->op4.lval->h) {
			assert(0);
			s = NULL;
		}

		if (r)
			rel_destroy(r);
	}
	/* we need a relation */
	if (ek.card == card_relation && s && s->type == st_ordered) {
		stmt *order = s->op1;
		stmt *ns = s->op2;
			
		s = sql_reorder(sql, order, ns);
	}
	if (ek.card == card_relation && s) {
		if (s->type == st_list && s->nrcols == 0 && s->key) {
			/* row to columns */
			node *n;
			list *l = list_new(sql->sa);

			for(n=s->op4.lval->h; n; n = n->next)
				list_append(l, const_column(sql->sa, (stmt*)n->data));
			s = stmt_list(sql->sa, l);
		}
		s = stmt_table(sql->sa, s, 1);
	}
	/* single column */
	if (ek.card != card_relation && s && s->type == st_list) {
		stmt *ns = s->op4.lval->h->data; 

		s = ns;
	}
	/* single value */
	if (ek.card == card_value && s && !s->key) {
		sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", tail_type(s));
		assert(zero_or_one);
		s = stmt_aggr(sql->sa, s, NULL, zero_or_one, 1);
	}
	return s;
}

/* Used for default values,
	select default
	       next value 
	       now 
 */

sql_exp *
rel_parse_val(mvc *m, char *query, char emode)
{
	mvc o = *m;
	sql_exp *e = NULL;
	buffer *b;
	char *n;
	int len = _strlen(query);
	exp_kind ek = {type_value, card_value, FALSE};
	stream *s;

	m->qc = NULL;

	m->caching = 0;
	m->emode = emode;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	strncpy(n, query, len);
	query = n;
	query[len] = '\n';
	query[len+1] = 0;
	len++;
	buffer_init(b, query, len);
	s = buffer_rastream(b, "sqlstatement");
	scanner_init(&m->scanner, bstream_create(s, b->len), NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	/*m->args = NULL;*/
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	/* via views we give access to protected objects */
	m->user_id = USER_MONETDB;

	(void) sqlparse(m);	
	
	/* get out the single value as we don't want an enclosing projection! */
	if (m->sym && m->sym->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *)m->sym;
		if (sn->selection->h->data.sym->token == SQL_COLUMN) {
			int is_last = 0;
			sql_rel *r = NULL;
			symbol* sq = sn->selection->h->data.sym->data.lval->h->data.sym;
			e = rel_value_exp2(m, &r, sq, sql_sel, ek, &is_last);
		}
	}
	GDKfree(query);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;
		char errstr[ERRSIZE];

		strcpy(errstr, m->errstr);
		*m = o;
		m->session->status = status;
		strcpy(m->errstr, errstr);
	} else {
		*m = o;
	}
	return e;
}

stmt *
rel_parse_value(mvc *m, char *query, char emode)
{
	mvc o = *m;
	stmt *s = NULL;
	buffer *b;
	char *n;
	int len = _strlen(query);
	exp_kind ek = {type_value, card_value, FALSE};
	stream *sr;

	m->qc = NULL;

	m->caching = 0;
	m->emode = emode;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	strncpy(n, query, len);
	query = n;
	query[len] = '\n';
	query[len+1] = 0;
	len++;
	buffer_init(b, query, len);
	sr = buffer_rastream(b, "sqlstatement");
	scanner_init(&m->scanner, bstream_create(sr, b->len), NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	/*m->args = NULL;*/
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	/* via views we give access to protected objects */
	m->user_id = USER_MONETDB;

	(void) sqlparse(m);	/* blindly ignore errors */
	
	/* get out the single value as we don't want an enclosing projection! */
	if (m->sym->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *)m->sym;
		if (sn->selection->h->data.sym->token == SQL_COLUMN) {
			s = value_exp(m, sn->selection->h->data.sym->data.lval->h->data.sym, sql_sel, ek);
		}
	}
	GDKfree(query);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;
		char errstr[ERRSIZE];

		strcpy(errstr, m->errstr);
		*m = o;
		m->session->status = status;
		strcpy(m->errstr, errstr);
	} else {
		*m = o;
	}
	return s;
}

stmt *
logical_value_exp(mvc *sql, symbol *sc, int f, exp_kind ek)
{
	stmt *s = NULL;
	int status = sql->session->status;
	sql_rel *r = NULL;
	sql_exp *e = rel_logical_value_exp(sql, &r, sc, f);

	if (!r && !e) {
		if (!sql->errstr[0]) 
			sql->session->status = status;
		return NULL;
	} else {
		if (r) {
			r = rel_optimizer(sql, r);
			s = rel_bin(sql, r);
		} else {
			s = exp_bin(sql, e, NULL, NULL, NULL, NULL);
		}

		if (s && s->type == st_list && !s->op4.lval->h) {
			assert(0);
			s = NULL;
		}

		if (r)
			rel_destroy(r);
	}
	/* we need a relation */
	if (ek.card == card_relation && s && s->type == st_ordered) {
		stmt *order = s->op1;
		stmt *ns = s->op2;
			
		s = sql_reorder(sql, order, ns);
	}
	if (ek.card == card_relation && s)
		s = stmt_table(sql->sa, s, 1);
	/* single column */
	if (ek.card != card_relation && s && s->type == st_list) {
		stmt *ns = s->op4.lval->h->data; 

		s = ns;
	}
	/* single value */
	if (ek.card == card_value && s && !s->key) {
		sql_subaggr *zero_or_one = sql_bind_aggr(sql->sa, sql->session->schema, "zero_or_one", tail_type(s));
		assert(zero_or_one);
		s = stmt_aggr(sql->sa, s, NULL, zero_or_one, 1);
	}
	return s;
}
