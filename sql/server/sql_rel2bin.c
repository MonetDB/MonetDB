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
 * Copyright August 2008-2012 MonetDB B.V.
 * All Rights Reserved.
 */


#include "monetdb_config.h"
#include "sql_rel2bin.h"
#include "sql_stack.h"
#include "sql_env.h"
#include <stdio.h>
#include "rel_semantic.h"

static stmt *
join_hash_key( mvc *sql, list *l ) 
{
	node *m;
	sql_subtype *it, *wrd;
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(sql->sa, 1 + ((sizeof(wrd)*8)-1)/(list_length(l)+1));

	it = sql_bind_localtype("int");
	wrd = sql_bind_localtype("wrd");
	for (m = l->h; m; m = m->next) {
		stmt *s = m->data;

		if (h) {
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", wrd, it, tail_type(s), wrd);

			h = stmt_Nop(sql->sa, stmt_list(sql->sa,  list_append( list_append( list_append(sa_list(sql->sa), h), bits), s )), xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", tail_type(s), NULL, wrd);
			h = stmt_unop(sql->sa, s, hf);
		}
	}
	return h;
}


/* TODO find out if the columns have an (hash) index */
static stmt *
releqjoin( mvc *sql, int flag, list *l1, list *l2 )
{
	node *n1 = l1->h, *n2 = l2->h;
	stmt *l, *r, *res;

	if (list_length(l1) <= 1) {
		l = l1->h->data;
		r = l2->h->data;
		return stmt_join(sql->sa, l, r, cmp_equal);
	}
	if (flag == NO_HASH) {
		l = n1->data;
		r = n2->data;
		n1 = n1->next;
		n2 = n2->next;
		res = stmt_join(sql->sa, l, r, cmp_equal);
	} else { /* need hash */
		l = join_hash_key(sql, l1);
		r = join_hash_key(sql, l2);
		res = stmt_join(sql->sa, l, r, cmp_equal);
	}
	l = stmt_result(sql->sa, res, 0);
	r = stmt_result(sql->sa, res, 1);
	for (; n1 && n2; n1 = n1->next, n2 = n2->next) {
		stmt *ld = n1->data;
		stmt *rd = n2->data;
		stmt *le = stmt_reorder_project(sql->sa, l, ld );
		stmt *re = stmt_reorder_project(sql->sa, r, rd );
		/* intentional both tail_type's of le (as re sometimes is a
		   find for bulk loading */
		sql_subfunc *f=sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(le), tail_type(le), F_FUNC);

		stmt * cmp;

		assert(f);

		/* TODO use uselect only */
		cmp = stmt_binop(sql->sa, le, re, f);

		cmp = stmt_uselect(sql->sa, cmp, stmt_bool(sql->sa, 1), cmp_equal, NULL);

		/* TODO the intersect may break the order!! */
		l = stmt_inter(sql->sa, l, cmp);
		r = stmt_inter(sql->sa, r, cmp);
	}
	res = stmt_join(sql->sa, stmt_reverse(sql->sa, l), stmt_reverse(sql->sa, r), cmp_equal);
	return res;
}

stmt *
rel2bin(mvc *c, stmt *s)
{
	assert(!(s->optimized < 2 && s->rewritten));
	if (s->optimized >= 2) {
		if (s->rewritten)
			return s->rewritten;
		else
			return s;
	}

	switch (s->type) {
		/* first just return those statements which we cannot optimize,
		 * such as schema manipulation, transaction managment, 
		 * and user authentication.
		 */
	case st_none:
	case st_rs_column:
	case st_dbat:
	case st_tid:

	case st_atom:
	case st_export:
	case st_var:
	case st_table_clear:

		s->optimized = 2;
		return s;

	case st_releqjoin:{
		stmt *res;
		list *l1 = sa_list(c->sa);
		list *l2 = sa_list(c->sa);
		node *n1, *n2;
	
		for (n1 = s->op1->op4.lval->h, n2 = s->op2->op4.lval->h; n1 && n2; n1 = n1->next, n2 = n2->next) {
			list_append(l1, rel2bin(c, n1->data));
			list_append(l2, rel2bin(c, n2->data));
		}
		res = releqjoin(c, s->flag, l1, l2);
		s->optimized = res->optimized = 2;
		if (res != s) {
			assert(s->rewritten==NULL);
			s->rewritten = res;
		}
		return res;
	}

	case st_tinter: 
	case st_tdiff:
	case st_limit: 
	case st_limit2: 
	case st_sample: 
	case st_join:
	case st_join2:
	case st_joinN:

	case st_reverse:
	case st_uselect:
	case st_uselect2: 

	case st_temp:
	case st_single:
	case st_inter:
	case st_diff:
	case st_union:
	case st_mirror:
	case st_const:
	case st_mark:
	case st_result:
	case st_gen_group:
	case st_group:
	case st_unique:
	case st_order:
	case st_reorder:
	case st_ordered:

	case st_alias:
	case st_exception:
	case st_trans:
	case st_catalog:

	case st_aggr:
	case st_unop:
	case st_binop:
	case st_Nop:
	case st_convert:

	case st_affected_rows:
	case st_table:

	case st_cond:
	case st_control_end:
	case st_return:
	case st_assign:

	case st_output: 

	case st_append_col:
	case st_update_col:
	case st_append_idx:
	case st_update_idx:
	case st_delete:
	case st_append:

		/* work around some bad recursion */
		if (s->type == st_append && s->op1->type == st_append) {
			stmt *p = s->op1, *n = NULL;
			sql_stack *stk = sql_stack_new(c->sa, 1024);

			sql_stack_push(stk, p);
			while(p->type == st_append && p->op1->optimized != 2) {
				p = p->op1;
				sql_stack_push(stk, p);
			}
			while( (p = sql_stack_pop(stk)) != NULL) {
				if (n)
					p->op1 = n;
				n = rel2bin(c, p);
			}
			if (n)
				s -> op1 = n;
		}
		if (s->op1) {
			stmt *os = s->op1;
			stmt *ns = rel2bin(c, os);

			assert(ns != s);
			s->op1 = ns;
		}

		if (s->op2) {
			stmt *os = s->op2;
			stmt *ns = rel2bin(c, os);

			assert(ns != s);
			s->op2 = ns;
		}
		if (s->op3) {
			stmt *os = s->op3;
			stmt *ns = rel2bin(c, os);
	
			assert(ns != s);
			s->op3 = ns;
		}
		s->optimized = 2;
		return s;

	case st_list:{

		stmt *res = NULL;
		node *n;
		list *l = s->op4.lval;
		list *nl = NULL;

		nl = sa_list(c->sa);
		for (n = l->h; n; n = n->next) {
			stmt *ns = rel2bin(c, n->data);

			list_append(nl, ns);
		}
		res = stmt_list(c->sa, nl);
		s->optimized = res->optimized = 2;
		if (res != s) {
			assert(s->rewritten==NULL);
			s->rewritten = res;
		}
		return res;
	}

	case st_bat:
	case st_idxbat:
		s->optimized = 2;
		return s;

	default:
		assert(0);	/* these should have been rewriten by now */
	}
	return s;
}
