/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "rel_bin.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_psm.h"
#include "rel_prop.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_unnest.h"
#include "rel_optimizer.h"
#include "sql_env.h"
#include "sql_optimizer.h"

#define OUTER_ZERO 64

static stmt * exp_bin(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel);
static stmt * rel_bin(backend *be, sql_rel *rel);
static stmt * subrel_bin(backend *be, sql_rel *rel, list *refs);

static stmt *check_types(backend *be, sql_subtype *ct, stmt *s, check_type tpe);

static stmt *
stmt_selectnil( backend *be, stmt *col)
{
	sql_subtype *t = tail_type(col);
	stmt *n = stmt_atom(be, atom_general(be->mvc->sa, t, NULL));
	stmt *nn = stmt_uselect2(be, col, n, n, 3, NULL, 0);
	return nn;
}

static stmt *
sql_unop_(backend *be, sql_schema *s, const char *fname, stmt *rs)
{
	mvc *sql = be->mvc;
	sql_subtype *rt = NULL;
	sql_subfunc *f = NULL;

	if (!s)
		s = sql->session->schema;
	rt = tail_type(rs);
	f = sql_bind_func(sql->sa, s, fname, rt, NULL, F_FUNC);
	/* try to find the function without a type, and convert
	 * the value to the type needed by this function!
	 */
	if (!f && (f = sql_find_func(sql->sa, s, fname, 1, F_FUNC, NULL)) != NULL) {
		sql_arg *a = f->func->ops->h->data;

		rs = check_types(be, &a->type, rs, type_equal);
		if (!rs) 
			f = NULL;
	}
	if (f) {
		/*
		if (f->func->res.scale == INOUT) {
			f->res.digits = rt->digits;
			f->res.scale = rt->scale;
		}
		*/
		return stmt_unop(be, rs, f);
	} else if (rs) {
		char *type = tail_type(rs)->type->sqlname;

		return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such unary operator '%s(%s)'", fname, type);
	}
	return NULL;
}

static stmt *
refs_find_rel(list *refs, sql_rel *rel)
{
	node *n;

	for(n=refs->h; n; n = n->next->next) {
		sql_rel *ref = n->data;
		stmt *s = n->next->data;
		
		if (rel == ref) 
			return s;
	}
	return NULL;
}

static void 
print_stmtlist(sql_allocator *sa, stmt *l)
{
	node *n;
	if (l) {
		for (n = l->op4.lval->h; n; n = n->next) {
			const char *rnme = table_name(sa, n->data);
			const char *nme = column_name(sa, n->data);

			TRC_INFO(SQL_RELATION, "%s.%s\n", rnme ? rnme : "(null!)", nme ? nme : "(null!)");
		}
	}
}

static stmt *
list_find_column(backend *be, list *l, const char *rname, const char *name ) 
{
	stmt *res = NULL;
	node *n;

	if (!l)
		return NULL;
	MT_lock_set(&l->ht_lock);
	if (!l->ht && list_length(l) > HASH_MIN_SIZE) {
		l->ht = hash_new(l->sa, MAX(list_length(l), l->expected_cnt), (fkeyvalue)&stmt_key);
		if (l->ht == NULL) {
			MT_lock_unset(&l->ht_lock);
			return NULL;
		}

		for (n = l->h; n; n = n->next) {
			const char *nme = column_name(be->mvc->sa, n->data);
			if (nme) {
				int key = hash_key(nme);

				if (hash_add(l->ht, key, n->data) == NULL) {
					MT_lock_unset(&l->ht_lock);
					return NULL;
				}
			}
		}
	}
	if (l->ht) {
		int key = hash_key(name);
		sql_hash_e *e = l->ht->buckets[key&(l->ht->size-1)];

		if (rname) {
			for (; e; e = e->chain) {
				stmt *s = e->value;
				const char *rnme = table_name(be->mvc->sa, s);
				const char *nme = column_name(be->mvc->sa, s);

				if (rnme && strcmp(rnme, rname) == 0 &&
		 	            strcmp(nme, name) == 0) {
					res = s;
					break;
				}
			}
		} else {
			for (; e; e = e->chain) {
				stmt *s = e->value;
				const char *rnme = table_name(be->mvc->sa, s);
				const char *nme = column_name(be->mvc->sa, s);

				if (!rnme && nme && strcmp(nme, name) == 0) {
					res = s;
					break;
				}
			}
		}
		MT_lock_unset(&l->ht_lock);
		if (!res)
			return NULL;
		return res;
	}
	MT_lock_unset(&l->ht_lock);
	if (rname) {
		for (n = l->h; n; n = n->next) {
			const char *rnme = table_name(be->mvc->sa, n->data);
			const char *nme = column_name(be->mvc->sa, n->data);

			if (rnme && strcmp(rnme, rname) == 0 && 
				    strcmp(nme, name) == 0) {
				res = n->data;
				break;
			}
		}
	} else {
		for (n = l->h; n; n = n->next) {
			const char *rnme = table_name(be->mvc->sa, n->data);
			const char *nme = column_name(be->mvc->sa, n->data);

			if (!rnme && nme && strcmp(nme, name) == 0) {
				res = n->data;
				break;
			}
		}
	}
	if (!res)
		return NULL;
	return res;
}

static stmt *
bin_find_column( backend *be, stmt *sub, const char *rname, const char *name ) 
{
	return list_find_column( be, sub->op4.lval, rname, name);
}

static list *
bin_find_columns( backend *be, stmt *sub, const char *name ) 
{
	node *n;
	list *l = sa_list(be->mvc->sa);

	for (n = sub->op4.lval->h; n; n = n->next) {
		const char *nme = column_name(be->mvc->sa, n->data);

		if (strcmp(nme, name) == 0) 
			append(l, n->data);
	}
	if (list_length(l)) 
		return l;
	return NULL;
}

static stmt *column(backend *be, stmt *val )
{
	if (val->nrcols == 0)
		return const_column(be, val);
	return val;
}

static stmt *create_const_column(backend *be, stmt *val )
{
	if (val->nrcols == 0)
		val = const_column(be, val);
	return stmt_append(be, stmt_temp(be, tail_type(val)), val);
}

static stmt *
bin_first_column(backend *be, stmt *sub ) 
{
	node *n = sub->op4.lval->h;
	stmt *c = n->data;

	if (c->nrcols == 0)
		return const_column(be, c);
	return c;
}

static stmt *
row2cols(backend *be, stmt *sub)
{
	if (sub->nrcols == 0 && sub->key) {
		node *n; 
		list *l = sa_list(be->mvc->sa);

		for (n = sub->op4.lval->h; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(be->mvc->sa, sc);
			const char *tname = table_name(be->mvc->sa, sc);

			sc = column(be, sc);
			list_append(l, stmt_alias(be, sc, tname, cname));
		}
		sub = stmt_list(be, l);
	}
	return sub;
}

static stmt*
distinct_value_list(backend *be, list *vals, stmt ** last_null_value)
{
	node *n;
	stmt *s;

	/* create bat append values */
	s = stmt_temp(be, exp_subtype(vals->h->data));
	for( n = vals->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(be, e, NULL, NULL, NULL, NULL, NULL, NULL);

		if (exp_is_null(be->mvc, e))
			*last_null_value = i;

		if (!i)
			return NULL;

		s = stmt_append(be, s, i);
	}

	/* Probably faster to filter out the values directly in the underlying list of atoms.
	   But for now use groupby to filter out duplicate values. */
	stmt* groupby = stmt_group(be, s, NULL, NULL, NULL, 1);
	stmt* ext = stmt_result(be, groupby, 1);

	return stmt_project(be, ext, s);
}

static stmt *
stmt_selectnonil( backend *be, stmt *col, stmt *s )
{
	sql_subtype *t = tail_type(col);
	stmt *n = stmt_atom(be, atom_general(be->mvc->sa, t, NULL));
	stmt *nn = stmt_uselect2(be, col, n, n, 3, s, 1);
	return nn;
}

static stmt *
handle_in_exps(backend *be, sql_exp *ce, list *nl, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int in, int use_r) 
{
	mvc *sql = be->mvc;
	node *n;
	stmt *s = NULL, *c = exp_bin(be, ce, left, right, grp, ext, cnt, NULL);

	if(!c)
		return NULL;

	if (c->nrcols == 0) {
		sql_subtype *bt = sql_bind_localtype("bit");
		sql_subfunc *cmp = (in)
			?sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(c), tail_type(c), F_FUNC)
			:sql_bind_func(sql->sa, sql->session->schema, "<>", tail_type(c), tail_type(c), F_FUNC);
		sql_subfunc *a = (in)?sql_bind_func(sql->sa, sql->session->schema, "or", bt, bt, F_FUNC)
				     :sql_bind_func(sql->sa, sql->session->schema, "and", bt, bt, F_FUNC);

		for( n = nl->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *i = exp_bin(be, use_r?e->r:e, left, right, grp, ext, cnt, NULL);
			if(!i)
				return NULL;

			i = stmt_binop(be, c, i, cmp); 
			if (s)
				s = stmt_binop(be, s, i, a);
			else
				s = i;

		}
		if (sel) 
			s = stmt_uselect(be, 
				stmt_const(be, bin_first_column(be, left), s), 
				stmt_bool(be, 1), cmp_equal, sel, 0); 
	} else if (list_length(nl) < 16) {
		comp_type cmp = (in)?cmp_equal:cmp_notequal;

		if (!in)
			s = sel;
		for( n = nl->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *i = exp_bin(be, use_r?e->r:e, left, right, grp, ext, cnt, NULL);
			if(!i)
				return NULL;

			if (in) { 
				i = stmt_uselect(be, c, i, cmp, sel, 0); 
				if (s)
					s = stmt_tunion(be, s, i); 
				else
					s = i;
			} else {
				s = stmt_uselect(be, c, i, cmp, s, 0); 
			}
		}
	} else {
		/* TODO: handle_in_exps should contain all necessary logic for in-expressions to be SQL compliant.
		   For non-SQL-standard compliant behavior, e.g. PostgreSQL backwards compatibility, we should
		   make sure that this behavior is replicated by the sql optimizer and not handle_in_exps. */

		stmt* last_null_value = NULL;  /* CORNER CASE ALERT: See description below. */

		/* The actual in-value-list should not contain duplicates to ensure that final join results are unique. */
		s = distinct_value_list(be, nl, &last_null_value);

		if (last_null_value) {
			/* The actual in-value-list should not contain null values. */
			s = stmt_project(be, stmt_selectnonil(be, s, NULL), s);
		}

		s = stmt_join(be, c, s, in, cmp_left);
		s = stmt_result(be, s, 0);

		if (!in) {
			if (last_null_value) {
				/* CORNER CASE ALERT:
				   In case of a not-in-expression with the associated in-value-list containing a null value,
				   the entire in-predicate is forced to always return false, i.e. an empty candidate list.
				   This is similar to postgres behavior.
				   TODO: However I do not think this behavior is in accordance with SQL standard 2003.

				   Ugly trick to return empty candidate list, because for all x it holds that: (x == null) == false.
				   list* singleton_bat = sa_list(sql->sa);
				   list_append(singleton_bat, null_value); */
				s = stmt_uselect(be, c, last_null_value, cmp_equal, NULL, 0);
				return s;
			}
			else {
				/* BACK TO HAPPY FLOW:
				   Make sure that null values are never returned. */
				stmt* non_nulls;
				non_nulls = stmt_selectnonil(be, c, NULL);
				s = stmt_tdiff(be, non_nulls, s);
				s = stmt_project(be, s, non_nulls);
			}
		}

		if (sel) {
			stmt* oid_intersection;
			oid_intersection = stmt_tinter(be, s, sel);
			s = stmt_project(be, oid_intersection, s);
			s = stmt_result(be, s, 0);
		}
	}
	return s;
}

static stmt *
value_list(backend *be, list *vals, stmt *left, stmt *sel) 
{
	node *n;
	stmt *s;
	sql_subtype *type = exp_subtype(vals->h->data);

	if (!type)
		return sql_error(be->mvc, 02, SQLSTATE(42000) "Could not infer the type of a value list column");
	/* create bat append values */
	s = stmt_temp(be, type);
	for( n = vals->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(be, e, left, NULL, NULL, NULL, NULL, sel);

		if (!i)
			return NULL;

		if (list_length(vals) == 1)
			return i;
		s = stmt_append(be, s, i);
	}
	return s;
}

static stmt *
exp_list(backend *be, list *exps, stmt *l, stmt *r, stmt *grp, stmt *ext, stmt *cnt, stmt *sel) 
{
	mvc *sql = be->mvc;
	node *n;
	list *nl = sa_list(sql->sa);

	for( n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(be, e, l, r, grp, ext, cnt, sel);
		if(!i)
			return NULL;

		if (n->next && i->type == st_table) /* relational statement */
			l = i->op1;
		else
			append(nl, i);
	}
	return stmt_list(be, nl);
}

static stmt *
exp_count_no_nil_arg( sql_exp *e, stmt *ext, sql_exp *ae, stmt *as ) 
{
	/* small optimization, ie use candidates directly on count(*) */
	if (!need_distinct(e) && !ext && as && (!need_no_nil(e) || !ae || !has_nil(ae))) { 
		/* skip alias statements */
		while (as->type == st_alias)
			as = as->op1;
		/* use candidate */
	       	if (as && as->type == st_join && as->flag == cmp_project) {
			if (as->op1 && (as->op1->type != st_result || as->op1->op1->type != st_group)) /* exclude a subquery with select distinct under the count */
				as = as->op1;
		}
	}
	return as;
}

stmt *
exp_bin(backend *be, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel) 
{
	mvc *sql = be->mvc;
	stmt *s = NULL;

 	if (THRhighwater())
		return sql_error(be->mvc, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!e) {
		assert(0);
		return NULL;
	}

	switch(e->type) {
	case e_psm:
		if (e->flag & PSM_SET) {
			stmt *r = exp_bin(be, e->l, left, right, grp, ext, cnt, sel);
			if(!r)
				return NULL;
			if (e->card <= CARD_ATOM && r->nrcols > 0) /* single value, get result from bat */
				r = stmt_fetch(be, r);
			return stmt_assign(be, exp_name(e), r, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_VAR) {
			if (e->f)
				return stmt_vars(be, exp_name(e), e->f, 1, GET_PSM_LEVEL(e->flag));
			else
				return stmt_var(be, exp_name(e), &e->tpe, 1, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_RETURN) {
			sql_exp *l = e->l;
			stmt *r = exp_bin(be, l, left, right, grp, ext, cnt, sel);

			if (!r)
				return NULL;
			/* handle table returning functions */
			if (l->type == e_psm && l->flag & PSM_REL) {
				stmt *lst = r->op1;
				if (r->type == st_table && lst->nrcols == 0 && lst->key && e->card > CARD_ATOM) {
					node *n;
					list *l = sa_list(sql->sa);

					for(n=lst->op4.lval->h; n; n = n->next)
						list_append(l, const_column(be, (stmt*)n->data));
					r = stmt_list(be, l);
				}
				if (r->type == st_list)
					r = stmt_table(be, r, 1);
			}
			return stmt_return(be, r, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_WHILE) {
			/* while is a if - block true with leave statement
	 		 * needed because the condition needs to be inside this outer block */
			stmt *ifstmt = stmt_cond(be, stmt_bool(be, 1), NULL, 0, 0);
			stmt *cond = exp_bin(be, e->l, left, right, grp, ext, cnt, sel);
			stmt *wstmt;

			if(!cond)
				return NULL;
			wstmt = stmt_cond(be, cond, ifstmt, 1, 0);

			if (!exp_list(be, e->r, left, right, grp, ext, cnt, sel))
				return NULL;
			(void)stmt_control_end(be, wstmt);
			return stmt_control_end(be, ifstmt);
		} else if (e->flag & PSM_IF) {
			stmt *cond = exp_bin(be, e->l, left, right, grp, ext, cnt, sel);
			stmt *ifstmt, *res;

			if(!cond)
				return NULL;
			ifstmt = stmt_cond(be, cond, NULL, 0, 0);
			if (!exp_list(be, e->r, left, right, grp, ext, cnt, sel))
				return NULL;
			res = stmt_control_end(be, ifstmt);
			if (e->f) {
				stmt *elsestmt = stmt_cond(be, cond, NULL, 0, 1);

				if (!exp_list(be, e->f, left, right, grp, ext, cnt, sel))
					return NULL;
				res = stmt_control_end(be, elsestmt);
			}
			return res;
		} else if (e->flag & PSM_REL) {
			sql_rel *rel = e->l;
			stmt *r = rel_bin(be, rel);

			if (!r)
				return NULL;
			if (is_modify(rel->op) || is_ddl(rel->op)) 
				return r;
			return stmt_table(be, r, 1);
		} else if (e->flag & PSM_EXCEPTION) {
			stmt *cond = exp_bin(be, e->l, left, right, grp, ext, cnt, sel);
			return stmt_exception(be, cond, (const char *) e->r, 0);
		}
		break;
	case e_atom: {
		if (e->l) { 			/* literals */
			atom *a = e->l;
			s = stmt_atom(be, atom_dup(sql->sa, a));
		} else if (e->r) { 		/* parameters */
			s = stmt_var(be, sa_strdup(sql->sa, e->r), e->tpe.type?&e->tpe:NULL, 0, e->flag);
		} else if (e->f) { 		/* values */
			s = value_list(be, e->f, left, sel);
		} else { 			/* arguments */
			s = stmt_varnr(be, e->flag, e->tpe.type?&e->tpe:NULL);
		}
	}	break;
	case e_convert: {
		/* if input is type any NULL or column of nulls, change type */
		list *tps = e->r;
		sql_subtype *from = tps->h->data;
		sql_subtype *to = tps->h->next->data;
		stmt *l;

		if (from->type->localtype == 0) {
			l = stmt_atom(be, atom_general(sql->sa, to, NULL));
		} else {
			l = exp_bin(be, e->l, left, right, grp, ext, cnt, sel);
		}
		if (!l)
			return NULL;
		s = stmt_convert(be, l, from, to, NULL);
	} 	break;
	case e_func: {
		node *en;
		list *l = sa_list(sql->sa), *exps = e->l;
		sql_subfunc *f = e->f;
		stmt *rows = NULL, *cond_execution = NULL;
		char name[16], *nme = NULL;

		if (f->func->side_effect && left) {
			if (!exps || list_empty(exps))
				append(l, 
				stmt_const(be, 
					bin_first_column(be, left), 
					stmt_atom_int(be, 0)));
			else if (exps_card(exps) < CARD_MULTI) {
				rows = bin_first_column(be, left);
			}
		}
		assert(!e->r);
		if (exps) {
			int nrcols = 0;
			for (en = exps->h; en; en = en->next) {
				sql_exp *e = en->data;
				stmt *es;

				es = exp_bin(be, e, left, right, grp, ext, cnt, sel);

				if (!es) 
					return NULL;

				if (rows && en == exps->h && f->func->type != F_LOADER)
					es = stmt_const(be, rows, es);
				else if (f->func->type == F_ANALYTIC && es->nrcols == 0) {
					if (en == exps->h)
						es = stmt_const(be, bin_first_column(be, left), es); /* ensure the first argument is a column */
					if (!f->func->s && !strcmp(f->func->base.name, "window_bound")
						&& exps->h->next && list_length(f->func->ops) == 6 && en == exps->h->next)
						es = stmt_const(be, bin_first_column(be, left), es);
				}
				/* last argument is condition, change into candidate list */
				if (f->func->type != F_ANALYTIC && !en->next && !f->func->varres && !f->func->vararg && list_length(exps) > list_length(f->func->ops)) {
					if (es->nrcols) {
						if (!nrcols) {
							node *n;
							list *nl = sa_list(sql->sa);
							for (n = l->h; n; n = n->next) {
								stmt *s = n->data;
								s = stmt_const(be, es, s);
								list_append(nl, s);
							}
							l = nl;

						}
						es = stmt_uselect(be, es, stmt_bool(be,1), cmp_equal, NULL, 0);
					} else /* need a condition */
						cond_execution = es;
				}
				if (es->nrcols > nrcols)
					nrcols = es->nrcols;
				if (!cond_execution)
					list_append(l,es);
			}
			if (sel && strcmp(sql_func_mod(f->func), "calc") == 0 && nrcols && strcmp(sql_func_imp(f->func), "ifthenelse") != 0)
				list_append(l,sel);
		}
		if (cond_execution) {
			/* var_x = nil; */
			nme = number2name(name, sizeof(name), ++sql->label);
			(void)stmt_var(be, nme, exp_subtype(e), 1, 2);
			/* if_barrier ... */
			cond_execution = stmt_cond(be, cond_execution, NULL, 0, 0);
		}
		if (f->func->rel) 
			s = stmt_func(be, stmt_list(be, l), sa_strdup(sql->sa, f->func->base.name), f->func->rel, (f->func->type == F_UNION));
		else
			s = stmt_Nop(be, stmt_list(be, l), e->f); 
		if (!s)
			return NULL;
		if (cond_execution) {
			/* var_x = s */
			(void)stmt_assign(be, nme, s, 2);
			/* endif_barrier */
			(void)stmt_control_end(be, cond_execution);
			s = stmt_var(be, nme, exp_subtype(e), 0, 2);
		}
	} 	break;
	case e_aggr: {
		list *attr = e->l; 
		stmt *as = NULL;
		sql_subfunc *a = e->f;

		assert(sel == NULL);
		if (attr && attr->h) { 
			node *en;
			list *l = sa_list(sql->sa);

			for (en = attr->h; en; en = en->next) {
				sql_exp *at = en->data;

				as = exp_bin(be, at, left, right, NULL, NULL, NULL, sel);

				if (as && as->nrcols <= 0 && left && (a->func->base.name[0] != 'm' || en->next || en == attr->h)) 
					as = stmt_const(be, bin_first_column(be, left), as);
				if (en == attr->h && !en->next && exp_aggr_is_count(e))
					as = exp_count_no_nil_arg(e, ext, at, as);
				/* insert single value into a column */
				if (as && as->nrcols <= 0 && !left && (a->func->base.name[0] != 'm' || en->next || en == attr->h))
					as = const_column(be, as);

				if (!as)
					return NULL;
				if (need_distinct(e)){ 
					stmt *g = stmt_group(be, as, grp, ext, cnt, 1);
					stmt *next = stmt_result(be, g, 1); 
						
					as = stmt_project(be, next, as);
					if (grp)
						grp = stmt_project(be, next, grp);
				}
				append(l, as);
			}
			as = stmt_list(be, l);
		} else {
			/* count(*) may need the default group (relation) and
			   and/or an attribute to count */
			if (grp) {
				as = grp;
			} else if (left) {
				as = bin_first_column(be, left);
				as = exp_count_no_nil_arg(e, ext, NULL, as);
			} else {
				/* create dummy single value in a column */
				as = stmt_atom_lng(be, 0);
				as = const_column(be, as);
			}
		}
		s = stmt_aggr(be, as, grp, ext, a, 1, need_no_nil(e) /* ignore nil*/, !zero_if_empty(e) );
		if (find_prop(e->p, PROP_COUNT)) /* propagate count == 0 ipv NULL in outer joins */
			s->flag |= OUTER_ZERO;
	} 	break;
	case e_column: {
		if (right) /* check relation names */
			s = bin_find_column(be, right, e->l, e->r);
		if (!s && left) 
			s = bin_find_column(be, left, e->l, e->r);
		if (s && grp)
			s = stmt_project(be, ext, s);
		if (!s && right) {
			TRC_CRITICAL(SQL_RELATION, "Could not find %s.%s\n", (char*)e->l, (char*)e->r);
			print_stmtlist(sql->sa, left);
			print_stmtlist(sql->sa, right);
			if (!s) {
				TRC_ERROR(SQL_RELATION, "Query: '%s'\n", sql->query);
			}
			assert(s);
			return NULL;
		}
	}	break;
	case e_cmp: {
		stmt *l = NULL, *r = NULL, *r2 = NULL;
		int swapped = 0, is_select = 0;
		sql_exp *re = e->r, *re2 = e->f;

		/* general predicate, select and join */
		if (e->flag == cmp_filter) {
			list *args;
			list *ops;
			node *n;
			int first = 1;

		       	ops = sa_list(sql->sa);
		       	args = e->l;
			for( n = args->h; n; n = n->next ) {
				s = NULL;
				if (!swapped)
					s = exp_bin(be, n->data, left, NULL, grp, ext, cnt, NULL); 
				if (!s && (first || swapped)) {
					s = exp_bin(be, n->data, right, NULL, grp, ext, cnt, NULL); 
					swapped = 1;
				}
				if (!s) 
					return s;
				if (s->nrcols == 0 && first)
					s = stmt_const(be, bin_first_column(be, swapped?right:left), s); 
				list_append(ops, s);
				first = 0;
			}
			l = stmt_list(be, ops);
		       	ops = sa_list(sql->sa);
			args = e->r;
			for( n = args->h; n; n = n->next ) {
				s = exp_bin(be, n->data, (swapped || !right)?left:right, NULL, grp, ext, cnt, NULL); 
				if (!s) 
					return s;
				list_append(ops, s);
			}
			r = stmt_list(be, ops);

			if (left && right && exps_card(e->r) > CARD_ATOM) {
				sql_subfunc *f = e->f;
				return stmt_genjoin(be, l, r, f, is_anti(e), swapped);
			}
			assert(!swapped);
			s = stmt_genselect(be, l, r, e->f, sel, is_anti(e));
			return s;
		}
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			return handle_in_exps(be, e->l, e->r, left, right, grp, ext, cnt, sel, (e->flag == cmp_in), 0);
		}
		if (e->flag == cmp_or && (!right || right->nrcols == 1)) {
			sql_subtype *bt = sql_bind_localtype("bit");
			list *l = e->l;
			node *n;
			stmt *sel1 = NULL, *sel2 = NULL;
			int anti = is_anti(e);

			sel1 = sel;
			sel2 = sel;
			for( n = l->h; n; n = n->next ) {
				sql_exp *c = n->data;
				stmt *sin = (sel1 && sel1->nrcols)?sel1:NULL;

				/* propagate the anti flag */
				if (anti) 
					set_anti(c);
				s = exp_bin(be, c, left, right, grp, ext, cnt, sin); 
				if (!s) 
					return s;

				if (!sin && sel1 && sel1->nrcols == 0 && s->nrcols == 0) {
					sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, anti?"or":"and", bt, bt, F_FUNC);
					assert(f);
					s = stmt_binop(be, sel1, s, f);
				} else if (sel1 && (sel1->nrcols == 0 || s->nrcols == 0)) {
					stmt *predicate = bin_first_column(be, left);
				
					predicate = stmt_const(be, predicate, stmt_bool(be, 1));
					if (s->nrcols == 0)
						s = stmt_uselect(be, predicate, s, cmp_equal, sel1, anti);
					else
						s = stmt_uselect(be, predicate, sel1, cmp_equal, s, anti);
				}
				sel1 = s;
			}
			l = e->r;
			for( n = l->h; n; n = n->next ) {
				sql_exp *c = n->data;
				stmt *sin = (sel2 && sel2->nrcols)?sel2:NULL;

				/* propagate the anti flag */
				if (anti) 
					set_anti(c);
				s = exp_bin(be, c, left, right, grp, ext, cnt, sin); 
				if (!s) 
					return s;

				if (!sin && sel2 && sel2->nrcols == 0 && s->nrcols == 0) {
					sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, anti?"or":"and", bt, bt, F_FUNC);
					assert(f);
					s = stmt_binop(be, sel2, s, f);
				} else if (sel2 && (sel2->nrcols == 0 || s->nrcols == 0)) {
					stmt *predicate = bin_first_column(be, left);
				
					predicate = stmt_const(be, predicate, stmt_bool(be, 1));
					if (s->nrcols == 0)
						s = stmt_uselect(be, predicate, s, cmp_equal, sel2, anti);
					else
						s = stmt_uselect(be, predicate, sel2, cmp_equal, s, anti);
				}
				sel2 = s;
			}
			if (sel1->nrcols == 0 && sel2->nrcols == 0) {
				sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, anti?"and":"or", bt, bt, F_FUNC);
				assert(f);
				return stmt_binop(be, sel1, sel2, f);
			}
			if (sel1->nrcols == 0) {
				stmt *predicate = bin_first_column(be, left);
				
				predicate = stmt_const(be, predicate, stmt_bool(be, 1));
				sel1 = stmt_uselect(be, predicate, sel1, cmp_equal, NULL, 0/*anti*/);
			}
			if (sel2->nrcols == 0) {
				stmt *predicate = bin_first_column(be, left);
				
				predicate = stmt_const(be, predicate, stmt_bool(be, 1));
				sel2 = stmt_uselect(be, predicate, sel2, cmp_equal, NULL, 0/*anti*/);
			}
			if (anti)
				return stmt_project(be, stmt_tinter(be, sel1, sel2), sel1);
			return stmt_tunion(be, sel1, sel2);
		}
		if (e->flag == cmp_or && right) {  /* join */
			assert(0);
		}

		/* mark use of join indices */
		if (right && find_prop(e->p, PROP_JOINIDX) != NULL) 
			sql->opt_stats[0]++; 

		if (!l) {
			l = exp_bin(be, e->l, left, NULL, grp, ext, cnt, sel);
			swapped = 0;
		}
		if (!l && right) {
 			l = exp_bin(be, e->l, right, NULL, grp, ext, cnt, sel);
			swapped = 1;
		}
		if (swapped || !right)
 			r = exp_bin(be, re, left, NULL, grp, ext, cnt, sel);
		else
 			r = exp_bin(be, re, right, NULL, grp, ext, cnt, sel);
		if (!r && !swapped) {
 			r = exp_bin(be, re, left, NULL, grp, ext, cnt, sel);
			is_select = 1;
		}
		if (!r && swapped) {
 			r = exp_bin(be, re, right, NULL, grp, ext, cnt, sel);
			is_select = 1;
		}
		if (re2)
 			r2 = exp_bin(be, re2, left, right, grp, ext, cnt, sel);

		if (!l || !r || (re2 && !r2)) {
			TRC_ERROR(SQL_RELATION, "Query: '%s'\n", sql->query);
			return NULL;
		}

		/*
		if (left && right && !is_select &&
		   ((l->nrcols && (r->nrcols || (r2 && r2->nrcols))) || 
		     re->card > CARD_ATOM || 
		    (re2 && re2->card > CARD_ATOM))) {
		    */
		(void)is_select;
		if (left && right) {
			if (l->nrcols == 0)
				l = stmt_const(be, bin_first_column(be, swapped?right:left), l); 
			if (r->nrcols == 0)
				r = stmt_const(be, bin_first_column(be, swapped?left:right), r); 
			if (r2 && r2->nrcols == 0)
				r2 = stmt_const(be, bin_first_column(be, swapped?left:right), r2); 
			if (r2) {
				s = stmt_join2(be, l, r, r2, (comp_type)e->flag, is_anti(e), swapped);
			} else if (swapped) {
				s = stmt_join(be, r, l, is_anti(e), swap_compare((comp_type)e->flag));
			} else {
				s = stmt_join(be, l, r, is_anti(e), (comp_type)e->flag);
			}
		} else {
			if (r2) {
				if (l->nrcols == 0 && r->nrcols == 0 && r2->nrcols == 0) {
					sql_subtype *bt = sql_bind_localtype("bit");
					sql_subfunc *lf = sql_bind_func(sql->sa, sql->session->schema,
							compare_func(range2lcompare(e->flag), 0),
							tail_type(l), tail_type(r), F_FUNC);
					sql_subfunc *rf = sql_bind_func(sql->sa, sql->session->schema,
							compare_func(range2rcompare(e->flag), 0),
							tail_type(l), tail_type(r), F_FUNC);
					sql_subfunc *a = sql_bind_func(sql->sa, sql->session->schema,
							"and", bt, bt, F_FUNC);

					if (is_atom(re->type) && re->l && atom_null((atom*)re->l) &&
					    is_atom(re2->type) && re2->l && atom_null((atom*)re2->l)) {
						s = sql_unop_(be, NULL, "isnull", l);
					} else {
						assert(lf && rf && a);
						s = stmt_binop(be, 
							stmt_binop(be, l, r, lf), 
							stmt_binop(be, l, r2, rf), a);
					}
					if (is_anti(e)) {
						sql_subfunc *a = sql_bind_func(sql->sa, sql->session->schema, "not", bt, NULL, F_FUNC);
						s = stmt_unop(be, s, a);
					}
				} else if (((e->flag&3) != 3) /* both sides closed use between implementation */ && l->nrcols > 0 && r->nrcols > 0 && r2->nrcols > 0) {
					s = stmt_uselect(be, l, r, range2lcompare(e->flag),
					    stmt_uselect(be, l, r2, range2rcompare(e->flag), sel, is_anti(e)), is_anti(e));
				} else {
					s = stmt_uselect2(be, l, r, r2, (comp_type)e->flag, sel, is_anti(e));
				}
			} else {
				/* value compare or select */
				if (l->nrcols == 0 && r->nrcols == 0 && (e->flag == mark_in || e->flag == mark_notin)) {
					sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(l), tail_type(l), F_FUNC);
					assert(f);
					s = stmt_binop(be, l, r, f);
				} else if (l->nrcols == 0 && r->nrcols == 0) {
					sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema,
							compare_func((comp_type)e->flag, is_anti(e)),
							tail_type(l), tail_type(l), F_FUNC);
					assert(f);
					s = stmt_binop(be, l, r, f);
				} else {
					/* this can still be a join (as relational algebra and single value subquery results still means joins */
					s = stmt_uselect(be, l, r, (comp_type)e->flag, sel, is_anti(e));
				}
			}
		}
	 }	break;
	default:
		;
	}
	return s;
}

static stmt *
stmt_col( backend *be, sql_column *c, stmt *del) 
{ 
	stmt *sc = stmt_bat(be, c, RDONLY, del?del->partition:0);

	if (isTable(c->t) && c->t->access != TABLE_READONLY &&
	   (!isNew(c) || !isNew(c->t) /* alter */) &&
	   (c->t->persistence == SQL_PERSIST || c->t->persistence == SQL_DECLARED_TABLE) && !c->t->commit_action) {
		stmt *i = stmt_bat(be, c, RD_INS, 0);
		stmt *u = stmt_bat(be, c, RD_UPD_ID, del?del->partition:0);
		sc = stmt_project_delta(be, sc, u, i);
		sc = stmt_project(be, del, sc);
	} else if (del) { /* always handle the deletes */
		sc = stmt_project(be, del, sc);
	}
	return sc;
}

static stmt *
stmt_idx( backend *be, sql_idx *i, stmt *del) 
{ 
	stmt *sc = stmt_idxbat(be, i, RDONLY, del?del->partition:0);

	if (isTable(i->t) && i->t->access != TABLE_READONLY &&
	   (!isNew(i) || !isNew(i->t) /* alter */) &&
	   (i->t->persistence == SQL_PERSIST || i->t->persistence == SQL_DECLARED_TABLE) && !i->t->commit_action) {
		stmt *ic = stmt_idxbat(be, i, RD_INS, 0);
		stmt *u = stmt_idxbat(be, i, RD_UPD_ID, del?del->partition:0);
		sc = stmt_project_delta(be, sc, u, ic);
		sc = stmt_project(be, del, sc);
	} else if (del) { /* always handle the deletes */
		sc = stmt_project(be, del, sc);
	}
	return sc;
}

#if 0
static stmt *
check_table_types(backend *be, list *types, stmt *s, check_type tpe)
{
	mvc *sql = be->mvc;
	//const char *tname;
	stmt *tab = s;
	int temp = 0;

	if (s->type != st_table) {
		return sql_error(
			sql, 03,
			SQLSTATE(42000) "single value and complex type are not equal");
	}
	tab = s->op1;
	temp = s->flag;
	if (tab->type == st_var) {
		sql_table *tbl = NULL;//tail_type(tab)->comp_type;
		stmt *dels = stmt_tid(be, tbl, 0);
		node *n, *m;
		list *l = sa_list(sql->sa);
		
		stack_find_var(sql, tab->op1->op4.aval->data.val.sval);

		for (n = types->h, m = tbl->columns.set->h; 
			n && m; n = n->next, m = m->next) 
		{
			sql_subtype *ct = n->data;
			sql_column *dtc = m->data;
			stmt *dtcs = stmt_col(be, dtc, dels);
			stmt *r = check_types(be, ct, dtcs, tpe);
			if (!r) 
				return NULL;
			//r = stmt_alias(be, r, tbl->base.name, c->base.name);
			list_append(l, r);
		}
	 	return stmt_table(be, stmt_list(be, l), temp);
	} else if (tab->type == st_list) {
		node *n, *m;
		list *l = sa_list(sql->sa);
		for (n = types->h, m = tab->op4.lval->h; 
			n && m; n = n->next, m = m->next) 
		{
			sql_subtype *ct = n->data;
			stmt *r = check_types(be, ct, m->data, tpe);
			if (!r) 
				return NULL;
			//tname = table_name(sql->sa, r);
			//r = stmt_alias(be, r, tname, c->base.name);
			list_append(l, r);
		}
		return stmt_table(be, stmt_list(be, l), temp);
	} else { /* single column/value */
		stmt *r;
		sql_subtype *st = tail_type(tab), *ct;

		if (list_length(types) != 1) {
			stmt *res = sql_error(
				sql, 03,
				SQLSTATE(42000) "single value of type %s and complex type are not equal",
				st->type->sqlname
			);
			return res;
		}
		ct = types->h->data;
		r = check_types(be, ct, tab, tpe);
		//tname = table_name(sql->sa, r);
		//r = stmt_alias(be, r, tname, c->base.name);
		return stmt_table(be, r, temp);
	}
}
#endif

static void
sql_convert_arg(mvc *sql, int nr, sql_subtype *rt)
{
	atom *a = sql_bind_arg(sql, nr);

	if (atom_null(a)) {
		if (a->data.vtype != rt->type->localtype) {
			a->data.vtype = rt->type->localtype;
			VALset(&a->data, a->data.vtype, (ptr) ATOMnilptr(a->data.vtype));
		}
	}
	a->tpe = *rt;
}

/* try to do an inplace convertion 
 * 
 * inplace conversion is only possible if the s is an variable.
 * This is only done to be able to map more cached queries onto the same 
 * interface.
 */
static stmt *
inplace_convert(backend *be, sql_subtype *ct, stmt *s)
{
	atom *a;

	/* exclude named variables */
	if (s->type != st_var || (s->op1 && s->op1->op4.aval->data.val.sval) || 
		(ct->scale && ct->type->eclass != EC_FLT))
		return s;

	a = sql_bind_arg(be->mvc, s->flag);
	if (atom_cast(be->mvc->sa, a, ct)) {
		stmt *r = stmt_varnr(be, s->flag, ct);
		sql_convert_arg(be->mvc, s->flag, ct);
		return r;
	}
	return s;
}

static int
stmt_set_type_param(mvc *sql, sql_subtype *type, stmt *param)
{
	if (!type || !param || param->type != st_var)
		return -1;

	if (set_type_param(sql, type, param->flag) == 0) {
		param->op4.typeval = *type;
		return 0;
	}
	return -1;
}

/* check_types tries to match the ct type with the type of s if they don't
 * match s is converted. Returns NULL on failure.
 */
static stmt *
check_types(backend *be, sql_subtype *ct, stmt *s, check_type tpe)
{
	mvc *sql = be->mvc;
	int c = 0;
	sql_subtype *t = NULL, *st = NULL;

 	st = tail_type(s);
	if ((!st || !st->type) && stmt_set_type_param(sql, ct, s) == 0) {
		return s;
	} else if (!st) {
		return sql_error(sql, 02, SQLSTATE(42000) "statement has no type information");
	}

	/* first try cheap internal (inplace) convertions ! */
	s = inplace_convert(be, ct, s);
	t = st = tail_type(s);

	/* check if the types are the same */
	if (t && subtype_cmp(t, ct) != 0) {
		t = NULL;
	}

	if (!t) {	/* try to convert if needed */
		if (EC_INTERVAL(st->type->eclass) && (ct->type->eclass == EC_NUM || ct->type->eclass == EC_POS) && ct->digits < st->digits) {
			s = NULL; /* conversion from interval to num depends on the number of digits */
		} else {
			c = sql_type_convert(st->type->eclass, ct->type->eclass);
			if (!c || (c == 2 && tpe == type_set) || (c == 3 && tpe != type_cast)) { 
				s = NULL;
			} else {
				s = stmt_convert(be, s, st, ct, NULL);
			}
		}
	}
	if (!s) {
		stmt *res = sql_error(
			sql, 03,
			SQLSTATE(42000) "types %s(%u,%u) (%s) and %s(%u,%u) (%s) are not equal",
			st->type->sqlname,
			st->digits,
			st->scale,
			st->type->base.name,
			ct->type->sqlname,
			ct->digits,
			ct->scale,
			ct->type->base.name
		);
		return res;
	}
	return s;
}

static stmt *
sql_Nop_(backend *be, const char *fname, stmt *a1, stmt *a2, stmt *a3, stmt *a4)
{
	mvc *sql = be->mvc;
	list *sl = sa_list(sql->sa);
	list *tl = sa_list(sql->sa);
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

	f = sql_bind_func_(sql->sa, sql->session->schema, fname, tl, F_FUNC);
	if (f)
		return stmt_Nop(be, stmt_list(be, sl), f);
	return sql_error(sql, 02, SQLSTATE(42000) "SELECT: no such operator '%s'", fname);
}

static stmt *
rel_parse_value(backend *be, char *query, char emode)
{
	mvc *m = be->mvc;
	mvc o = *m;
	stmt *s = NULL;
	buffer *b;
	char *n;
	size_t len = _strlen(query);
	exp_kind ek = {type_value, card_value, FALSE};
	stream *sr;
	bstream *bs;

	m->qc = NULL;

	m->caching = 0;
	m->emode = emode;
	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	if (b == NULL || n == NULL) {
		GDKfree(b);
		GDKfree(n);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(n, len + 2, "%s\n", query);
	query = n;
	len++;
	buffer_init(b, query, len);
	sr = buffer_rastream(b, "sqlstatement");
	if (sr == NULL) {
		buffer_destroy(b);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	bs = bstream_create(sr, b->len);
	if(bs == NULL) {
		buffer_destroy(b);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	scanner_init(&m->scanner, bs, NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	/*m->args = NULL;*/
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';

	(void) sqlparse(m);	/* blindly ignore errors */
	
	/* get out the single value as we don't want an enclosing projection! */
	if (m->sym->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *)m->sym;
		if (sn->selection->h->data.sym->token == SQL_COLUMN || sn->selection->h->data.sym->token == SQL_IDENT) {
			sql_rel *rel = NULL;
			sql_query *query = query_create(m);
			sql_exp *e = rel_value_exp2(query, &rel, sn->selection->h->data.sym->data.lval->h->data.sym, sql_sel, ek);

			if (!rel)
				s = exp_bin(be, e, NULL, NULL, NULL, NULL, NULL, NULL); 
		}
	}
	GDKfree(query);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);

	m->sym = NULL;
	o.vars = m->vars;	/* may have been realloc'ed */
	o.sizevars = m->sizevars;
	o.query = m->query;
	if (m->session->status || m->errstr[0]) {
		int status = m->session->status;

		memcpy(o.errstr, m->errstr, sizeof(o.errstr));
		*m = o;
		m->session->status = status;
	} else {
		*m = o;
	}
	return s;
}

static stmt *
stmt_rename(backend *be, sql_exp *exp, stmt *s )
{
	const char *name = exp_name(exp);
	const char *rname = exp_relname(exp);
	stmt *o = s;

	if (!name && exp_is_atom(exp))
		name = sa_strdup(be->mvc->sa, "single_value");
	assert(name);
	s = stmt_alias(be, s, rname, name);
	if (o->flag & OUTER_ZERO)
		s->flag |= OUTER_ZERO;
	return s;
}

static stmt *
rel2bin_sql_table(backend *be, sql_table *t) 
{
	mvc *sql = be->mvc;
	list *l = sa_list(sql->sa);
	node *n;
	stmt *dels = stmt_tid(be, t, 0);
			
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		stmt *sc = stmt_col(be, c, dels);

		list_append(l, sc);
	}
	/* TID column */
	if (t->columns.set->h) { 
		/* tid function  sql.tid(t) */
		const char *rnme = t->base.name;

		stmt *sc = dels?dels:stmt_tid(be, t, 0);
		sc = stmt_alias(be, sc, rnme, TID);
		list_append(l, sc);
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			stmt *sc = stmt_idx(be, i, dels);
			const char *rnme = t->base.name;

			/* index names are prefixed, to make them independent */
			sc = stmt_alias(be, sc, rnme, sa_strconcat(sql->sa, "%", i->base.name));
			list_append(l, sc);
		}
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_basetable(backend *be, sql_rel *rel)
{
	mvc *sql = be->mvc;
	sql_table *t = rel->l;
	sql_column *c = rel->r;
	list *l = sa_list(sql->sa);
	stmt *dels;
	node *en;

	if (!t && c)
		t = c->t;

	dels = stmt_tid(be, t, rel->flag == REL_PARTITION);

	/* add aliases */
	assert(rel->exps);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		const char *rname = exp_relname(exp)?exp_relname(exp):exp->l;
		const char *oname = exp->r;
		stmt *s = NULL;

		if (is_func(exp->type)) {
			list *exps = exp->l;
			sql_exp *cexp = exps->h->data;
			const char *cname = cexp->r;
			list *l = sa_list(sql->sa);

			c = find_sql_column(t, cname);
			s = stmt_col(be, c, dels);
			append(l, s);
			if (exps->h->next) {
				sql_exp *at = exps->h->next->data;
				stmt *u = exp_bin(be, at, NULL, NULL, NULL, NULL, NULL, NULL);
				if(!u)
					return NULL;

				append(l, u);
			}
			s = stmt_Nop(be, stmt_list(be, l), exp->f);
		} else if (oname[0] == '%' && strcmp(oname, TID) == 0) {
			/* tid function  sql.tid(t) */
			const char *rnme = t->base.name;

			s = dels?dels:stmt_tid(be, t, 0);
			s = stmt_alias(be, s, rnme, TID);
		} else if (oname[0] == '%') { 
			sql_idx *i = find_sql_idx(t, oname+1);

			/* do not include empty indices in the plan */
			if ((hash_index(i->type) && list_length(i->columns) <= 1) || !idx_has_column(i->type))
				continue;
			s = stmt_idx(be, i, dels);
		} else {
			sql_column *c = find_sql_column(t, oname);

			s = stmt_col(be, c, dels);
		}
		s->tname = rname;
		s->cname = exp_name(exp);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static int 
alias_cmp( stmt *s, const char *nme )
{
	return strcmp(s->cname, nme);
}

static list* exps2bin_args(backend *be, list *exps, list *args);

static list *
exp2bin_args(backend *be, sql_exp *e, list *args)
{
	mvc *sql = be->mvc;

	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!e)
		return args;
	switch(e->type){
	case e_column:
	case e_psm:
		return args;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			args = exps2bin_args(be, e->l, args);
			args = exps2bin_args(be, e->r, args);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			args = exp2bin_args(be, e->l, args);
			args = exps2bin_args(be, e->r, args);
		} else {
			args = exp2bin_args(be, e->l, args);
			args = exp2bin_args(be, e->r, args);
			if (e->f)
				args = exp2bin_args(be, e->f, args);
		}
		return args;
	case e_convert:
		if (e->l)
			return exp2bin_args(be, e->l, args);
		break;
	case e_aggr:
	case e_func: 
		if (e->l)
			return exps2bin_args(be, e->l, args);
		break;
	case e_atom:
		if (e->l) {
			return args;
		} else if (e->f) {
			return exps2bin_args(be, e->f, args);
		} else if (e->r) {
			char nme[64];

			snprintf(nme, 64, "A%s", (char*)e->r);
			if (!list_find(args, nme, (fcmp)&alias_cmp)) {
				stmt *s = stmt_var(be, e->r, &e->tpe, 0, 0);

				s = stmt_alias(be, s, NULL, sa_strdup(sql->sa, nme));
				list_append(args, s);
			}
		} else {
			char nme[16];

			snprintf(nme, 16, "A%d", e->flag);
			if (!list_find(args, nme, (fcmp)&alias_cmp)) {
				atom *a = sql->args[e->flag];
				stmt *s = stmt_varnr(be, e->flag, &a->tpe);

				s = stmt_alias(be, s, NULL, sa_strdup(sql->sa, nme));
				list_append(args, s);
			}
		}
	}
	return args;
}

static list *
exps2bin_args(backend *be, list *exps, list *args)
{
	node *n;

	if (!exps)
		return args;
	for (n = exps->h; n; n = n->next)
		args = exp2bin_args(be, n->data, args);
	return args;
}

static list *
rel2bin_args(backend *be, sql_rel *rel, list *args)
{
	if (THRhighwater())
		return sql_error(be->mvc, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (!rel)
		return args;
	switch(rel->op) {
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
		args = rel2bin_args(be, rel->l, args);
		args = rel2bin_args(be, rel->r, args);
		break;
	case op_groupby: 
		if (rel->r) 
			args = exps2bin_args(be, rel->r, args);
		/* fall through */
	case op_project:
	case op_select: 
	case op_topn: 
	case op_sample: 
		if (rel->exps)
			args = exps2bin_args(be, rel->exps, args);
		args = rel2bin_args(be, rel->l, args);
		break;
	case op_ddl: 
		args = rel2bin_args(be, rel->l, args);
		if (rel->r)
			args = rel2bin_args(be, rel->r, args);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		args = rel2bin_args(be, rel->r, args);
		break;
	}
	return args;
}

typedef struct trigger_input {
	sql_table *t;
	stmt *tids;
	stmt **updates;
	int type; /* insert 1, update 2, delete 3, truncate 4 */
	const char *on;
	const char *nn;
} trigger_input;

static stmt *
rel2bin_table(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l; 
	stmt *sub = NULL, *osub = NULL;
	node *en, *n;
	sql_exp *op = rel->r;

	if (rel->flag == 2) {
		trigger_input *ti = rel->l;
		l = sa_list(sql->sa);

		for(n = ti->t->columns.set->h; n; n = n->next) {
			sql_column *c = n->data;

			if (ti->type == 2) { /* updates */
				stmt *s = stmt_col(be, c, ti->tids);
				append(l, stmt_alias(be, s, ti->on, c->base.name));
			}
			if (ti->updates[c->colnr]) {
				append(l, stmt_alias(be, ti->updates[c->colnr], ti->nn, c->base.name));
			} else {
				stmt *s = stmt_col(be, c, ti->tids);
				append(l, stmt_alias(be, s, ti->nn, c->base.name));
				assert(ti->type != 1);
			}
		}
		sub = stmt_list(be, l);
		return sub;
	} else if (op) {
		int i;
		sql_subfunc *f = op->f;
		stmt *psub = NULL;
			
		if (rel->l) { /* first construct the sub relation */
			sql_rel *l = rel->l;
			if (l->op == op_ddl) {
				sql_table *t = rel_ddl_table_get(l);

				if (t)
					sub = rel2bin_sql_table(be, t);
			} else {
				sub = subrel_bin(be, rel->l, refs);
			}
			if (!sub) 
				return NULL;
		}

		psub = exp_bin(be, op, sub, NULL, NULL, NULL, NULL, NULL); /* table function */
		if (!f || !psub) { 
			assert(0);
			return NULL;	
		}
		l = sa_list(sql->sa);
		if (f->func->res) {
			if (f->func->varres) {
				for(i=0, en = rel->exps->h, n = f->res->h; en; en = en->next, n = n->next, i++ ) {
					sql_exp *exp = en->data;
					sql_subtype *st = n->data;
					const char *rnme = exp_relname(exp)?exp_relname(exp):exp->l;
					stmt *s = stmt_rs_column(be, psub, i, st); 
			
					s = stmt_alias(be, s, rnme, exp_name(exp));
					list_append(l, s);
				}
			} else {
				assert(list_length(f->func->res) == list_length(rel->exps));
				node *m;
				for(i = 0, n = f->func->res->h, m = rel->exps->h; n && m; n = n->next, m = m->next, i++ ) {
					sql_arg *a = n->data;
					sql_exp *exp = m->data;
					stmt *s = stmt_rs_column(be, psub, i, &a->type); 
					const char *rnme = exp_relname(exp)?exp_relname(exp):exp_find_rel_name(op);
		
					s = stmt_alias(be, s, rnme, a->name);
					list_append(l, s);
				}
				if (list_length(f->res) == list_length(f->func->res) + 1) {
					/* add missing %TID% column */
					sql_subtype *t = f->res->t->data;
					stmt *s = stmt_rs_column(be, psub, i, t); 
					const char *rnme = exp_find_rel_name(op);
		
					s = stmt_alias(be, s, rnme, TID);
					list_append(l, s);
				}
			}
		}
		if (rel->flag == TABLE_PROD_FUNC && sub && sub->nrcols) { 
			assert(0);
			list_merge(l, sub->op4.lval, NULL);
			osub = sub;
		}
		sub = stmt_list(be, l);
	} else if (rel->l) { /* handle sub query via function */
		int i;
		char name[16], *nme;
		sql_rel *fr;

		nme = number2name(name, sizeof(name), ++sql->remote);

		l = rel2bin_args(be, rel->l, sa_list(sql->sa));
		if (!l)
			return NULL;
		sub = stmt_list(be, l);
		if (!(sub = stmt_func(be, sub, sa_strdup(sql->sa, nme), rel->l, 0)))
			return NULL;
		fr = rel->l;
		l = sa_list(sql->sa);
		for(i = 0, n = rel->exps->h; n; n = n->next, i++ ) {
			sql_exp *c = n->data;
			stmt *s = stmt_rs_column(be, sub, i, exp_subtype(c)); 
			const char *nme = exp_name(c);
			const char *rnme = exp_relname(c);

			s = stmt_alias(be, s, rnme, nme);
			if (fr->card <= CARD_ATOM) /* single value, get result from bat */
				s = stmt_fetch(be, s);
			list_append(l, s);
		}
		sub = stmt_list(be, l);
	}
	if (!sub) { 
		assert(0);
		return NULL;	
	}
	l = sa_list(sql->sa);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		const char *rnme = exp_relname(exp)?exp_relname(exp):exp->l;
		stmt *s = bin_find_column(be, sub, exp->l, exp->r);

		if (!s) {
			assert(0);
			return NULL;
		}
		if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(be, bin_first_column(be, sub), s);
		s = stmt_alias(be, s, rnme, exp_name(exp));
		list_append(l, s);
	}
	if (osub && osub->nrcols) 
		list_merge(l, osub->op4.lval, NULL);
	sub = stmt_list(be, l);
	return sub;
}

static stmt *
rel2bin_hash_lookup(backend *be, sql_rel *rel, stmt *left, stmt *right, sql_idx *i, node *en ) 
{
	mvc *sql = be->mvc;
	node *n;
	sql_subtype *it = sql_bind_localtype("int");
	sql_subtype *lng = sql_bind_localtype("lng");
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(be, 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1));
	sql_exp *e = en->data;
	sql_exp *l = e->l;
	stmt *idx = bin_find_column(be, left, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	int swap_exp = 0, swap_rel = 0;
	comp_type comp = cmp_equal;

	if (!idx) {
		swap_exp = 1;
		l = e->r;
		idx = bin_find_column(be, left, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx && right) {
		swap_exp = 0;
		swap_rel = 1;
		l = e->l;
		idx = bin_find_column(be, right, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx && right) {
		swap_exp = 1;
		swap_rel = 1;
		l = e->r;
		idx = bin_find_column(be, right, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx)
		return NULL;
	/* should be in key order! */
	for( en = rel->exps->h, n = i->columns->h; en && n; en = en->next, n = n->next ) {
		sql_exp *e = en->data;
		stmt *s = NULL;

		if (e->type == e_cmp && e->flag == cmp_equal) {
			sql_exp *ee = (swap_exp)?e->l:e->r;
			if (swap_rel)
				s = exp_bin(be, ee, left, NULL, NULL, NULL, NULL, NULL);
			else
				s = exp_bin(be, ee, right, NULL, NULL, NULL, NULL, NULL);
		}

		if (!s) 
			return NULL;
		if (h) {
			sql_subfunc *xor = sql_bind_func_result(sql->sa, sql->session->schema, "rotate_xor_hash", F_FUNC, lng, 3, lng, it, tail_type(s));

			h = stmt_Nop(be, stmt_list(be, list_append( list_append(
				list_append(sa_list(sql->sa), h), bits), s)), xor);
			comp = cmp_equal_nil;
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", F_FUNC, lng, 1, tail_type(s));

			h = stmt_unop(be, s, hf);
		}
	}
	if (h && h->nrcols) {
		if (!swap_rel) {
			return stmt_join(be, idx, h, 0, comp);
		} else {
			return stmt_join(be, h, idx, 0, comp);
		}
	} else {
		return stmt_uselect(be, idx, h, comp, NULL, 0);
	}
}

static stmt *
join_hash_key( backend *be, list *l ) 
{
	mvc *sql = be->mvc;
	node *m;
	sql_subtype *it, *lng;
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(be, 1 + ((sizeof(lng)*8)-1)/(list_length(l)+1));

	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = l->h; m; m = m->next) {
		stmt *s = m->data;

		if (h) {
			sql_subfunc *xor = sql_bind_func_result(sql->sa, sql->session->schema, "rotate_xor_hash", F_FUNC, lng, 3, lng, it, tail_type(s));

			h = stmt_Nop(be, stmt_list(be, list_append( list_append( list_append(sa_list(sql->sa), h), bits), s )), xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", F_FUNC, lng, 1, tail_type(s));
			h = stmt_unop(be, s, hf);
		}
	}
	return h;
}

static stmt *
releqjoin( backend *be, list *l1, list *l2, int used_hash, comp_type cmp_op, int need_left )
{
	mvc *sql = be->mvc;
	node *n1 = l1->h, *n2 = l2->h;
	stmt *l, *r, *res;

	if (list_length(l1) <= 1) {
		l = l1->h->data;
		r = l2->h->data;
		r =  stmt_join(be, l, r, 0, cmp_op);
		if (need_left)
			r->flag = cmp_left;
		return r;
	}
	if (used_hash) {
		l = n1->data;
		r = n2->data;
		n1 = n1->next;
		n2 = n2->next;
		res = stmt_join(be, l, r, 0, cmp_op);
	} else { /* need hash */
		l = join_hash_key(be, l1);
		r = join_hash_key(be, l2);
		res = stmt_join(be, l, r, 0, cmp_op == cmp_equal ? cmp_equal_nil : cmp_op);
	}
	if (need_left) 
		res->flag = cmp_left;
	l = stmt_result(be, res, 0);
	r = stmt_result(be, res, 1);
	for (; n1 && n2; n1 = n1->next, n2 = n2->next) {
		stmt *ld = n1->data;
		stmt *rd = n2->data;
		stmt *le = stmt_project(be, l, ld );
		stmt *re = stmt_project(be, r, rd );
		/* intentional both tail_type's of le (as re sometimes is a find for bulk loading */
		sql_subfunc *f = NULL;
		stmt * cmp;
		list *ops;

		f = sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(le), tail_type(le), F_FUNC);
		assert(f);

		ops = sa_list(be->mvc->sa);
		list_append(ops, le);
		list_append(ops, re);
		if (cmp_op == cmp_equal_nil)
			list_append(ops, stmt_bool(be, 1));
		cmp = stmt_Nop(be, stmt_list(be, ops), f);
		cmp = stmt_uselect(be, cmp, stmt_bool(be, 1), cmp_equal, NULL, 0);
		l = stmt_project(be, cmp, l );
		r = stmt_project(be, cmp, r );
	}
	res = stmt_join(be, l, r, 0, cmp_joined);
	return res;
}

static stmt *
rel2bin_join(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l; 
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL, *jl, *jr;
	stmt *ld = NULL, *rd = NULL;
	int need_left = (rel->flag == LEFT_JOIN);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(be, left);
	right = row2cols(be, right);
	/* 
 	 * split in 2 steps, 
 	 * 	first cheap join(s) (equality or idx) 
 	 * 	second selects/filters 
	 */
	if (!list_empty(rel->exps)) {
		int used_hash = 0;
		int idx = 0, i;
		list *jexps = sa_list(sql->sa);
		list *lje = sa_list(sql->sa);
		list *rje = sa_list(sql->sa);
		char *handled = SA_ZNEW_ARRAY(sql->sa, char, list_length(rel->exps));

		/* get equi-joins/filters first */
		if (list_length(rel->exps) > 1) {
			for( en = rel->exps->h, i=0; en; en = en->next, i++) {
				sql_exp *e = en->data;
				if (e->type == e_cmp && (e->flag == cmp_equal || e->flag == cmp_filter)) {
					if ( !((rel_find_exp(rel->l, e->l) && rel_find_exp(rel->l, e->r))  ||
					       (rel_find_exp(rel->r, e->l) && rel_find_exp(rel->r, e->r)))) {
						append(jexps, e);
						handled[i] = 1;
					}
				}
			}
			if (list_empty(jexps)) {
				stmt *l = bin_first_column(be, left);
				stmt *r = bin_first_column(be, right);
				join = stmt_join(be, l, r, 0, cmp_all); 
			}
			for( en = rel->exps->h, i=0; en; en = en->next, i++) {
				sql_exp *e = en->data;
				if (!handled[i])
					append(jexps, e);
			}
			rel->exps = jexps;
		}

		/* generate a relational join */
		if (join)
			en = rel->exps->h;
		else
		for( en = rel->exps->h; en; en = en->next ) {
			int join_idx = sql->opt_stats[0];
			sql_exp *e = en->data;
			stmt *s = NULL;
			prop *p;

			/* only handle simple joins here */
			if ((exp_has_func(e) && e->flag != cmp_filter) ||
			    e->flag == cmp_or || (e->f && e->anti) ||
			      (e->type == e_cmp && e->flag == cmp_equal &&
			      ((rel_find_exp(rel->l, e->l) && rel_find_exp(rel->l, e->r))  ||
			       (rel_find_exp(rel->r, e->l) && rel_find_exp(rel->r, e->r)))) ) {
				if (!join && !list_length(lje)) {
					stmt *l = bin_first_column(be, left);
					stmt *r = bin_first_column(be, right);
					join = stmt_join(be, l, r, 0, cmp_all);
				}
				break;
			}
			if (list_length(lje) && (idx || e->type != e_cmp || (e->flag != cmp_equal && e->flag != cmp_filter) ||
			   (join && e->flag == cmp_filter)))
				break;
			if (e->type == e_cmp && e->flag == cmp_equal &&
			      ((rel_find_exp(rel->l, e->l) && rel_find_exp(rel->l, e->r))  ||
			       (rel_find_exp(rel->r, e->l) && rel_find_exp(rel->r, e->r)))) {
				break;
			}

			/* handle possible index lookups */
			/* expressions are in index order ! */
			if (!join &&
			    (p=find_prop(e->p, PROP_HASHCOL)) != NULL) {
				sql_idx *i = p->value;
			
				join = s = rel2bin_hash_lookup(be, rel, left, right, i, en);
				if (s) {
					list_append(lje, s->op1);
					list_append(rje, s->op2);
					used_hash = 1;
				}
			}

			s = exp_bin(be, e, left, right, NULL, NULL, NULL, NULL);
			if (!s) {
				assert(0);
				return NULL;
			}
			if (join_idx != sql->opt_stats[0])
				idx = 1;

			if (s->type != st_join && 
			    s->type != st_join2 && 
			    s->type != st_joinN) {
				/* predicate */
				if (!list_length(lje) && s->nrcols == 0) { 
					stmt *l = bin_first_column(be, left);
					stmt *r = bin_first_column(be, right);

					l = stmt_uselect(be, stmt_const(be, l, stmt_bool(be, 1)), s, cmp_equal, NULL, 0);
					join = stmt_join(be, l, r, 0, cmp_all);
					continue;
				}
				if (!join) {
					stmt *l = bin_first_column(be, left);
					stmt *r = bin_first_column(be, right);
					join = stmt_join(be, l, r, 0, cmp_all); 
				}
				break;
			}

			if (!join) 
				join = s;
			list_append(lje, s->op1);
			list_append(rje, s->op2);
		}
		if (list_length(lje) > 1) {
			join = releqjoin(be, lje, rje, used_hash, cmp_equal, need_left);
		} else if (!join) {
			join = stmt_join(be, lje->h->data, rje->h->data, 0, cmp_equal);
			if (need_left)
				join->flag = cmp_left;
		}
	} else {
		stmt *l = bin_first_column(be, left);
		stmt *r = bin_first_column(be, right);
		join = stmt_join(be, l, r, 0, cmp_all); 
	}
	jl = stmt_result(be, join, 0);
	jr = stmt_result(be, join, 1);
	if (en) {
		stmt *sub, *sel = NULL;
		list *nl;

		/* construct relation */
		nl = sa_list(sql->sa);

		/* first project using equi-joins */
		for( n = left->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jl, column(be, c) );
	
			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		for( n = right->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jr, column(be, c) );

			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		sub = stmt_list(be, nl);

		/* continue with non equi-joins */
		for( ; en; en = en->next ) {
			stmt *s = exp_bin(be, en->data, sub, NULL, NULL, NULL, NULL, sel);

			if (!s) {
				assert(0);
				return NULL;
			}
			if (s->nrcols == 0) {
				stmt *l = bin_first_column(be, sub);
				s = stmt_uselect(be, stmt_const(be, l, stmt_bool(be, 1)), s, cmp_equal, sel, 0);
			}
			sel = s;
		}
		/* recreate join output */
		jl = stmt_project(be, sel, jl); 
		jr = stmt_project(be, sel, jr); 
	}

	/* construct relation */
	l = sa_list(sql->sa);

	if (rel->op == op_left || rel->op == op_full) {
		/* we need to add the missing oid's */
		ld = stmt_mirror(be, bin_first_column(be, left));
		ld = stmt_tdiff(be, ld, jl);
	}
	if (rel->op == op_right || rel->op == op_full) {
		/* we need to add the missing oid's */
		rd = stmt_mirror(be, bin_first_column(be, right));
		rd = stmt_tdiff(be, rd, jr);
	}

	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(be, jl, column(be, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = create_const_column(be, s);
		if (rel->op == op_left || rel->op == op_full)
			s = stmt_append(be, s, stmt_project(be, ld, c));
		if (rel->op == op_right || rel->op == op_full) 
			s = stmt_append(be, s, stmt_const(be, rd, (c->flag&OUTER_ZERO)?stmt_atom_lng(be, 0):stmt_atom(be, atom_general(sql->sa, tail_type(c), NULL))));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	for( n = right->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(be, jr, column(be, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = create_const_column(be, s);
		if (rel->op == op_left || rel->op == op_full) 
			s = stmt_append(be, s, stmt_const(be, ld, (c->flag&OUTER_ZERO)?stmt_atom_lng(be, 0):stmt_atom(be, atom_general(sql->sa, tail_type(c), NULL))));
		if (rel->op == op_right || rel->op == op_full) 
			s = stmt_append(be, s, stmt_project(be, rd, c));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static int
exp_is_mark(sql_exp *e)
{
	if (e->type == e_cmp && 
		(e->flag == mark_in || e->flag == mark_notin ||
		 e->flag == mark_exists || e->flag == mark_notexists)) 
		return 1;
	return 0;
}

static stmt *
rel2bin_antijoin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l, *jexps = NULL, *mexps = NULL;
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(be, left);
	right = row2cols(be, right);

	if (rel->exps) {
		jexps = sa_list(sql->sa);
		mexps = sa_list(sql->sa);

		for( en = rel->exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			if (e->type != e_cmp)
				assert(0);
			if (exp_is_mark(e))
				append(mexps, e);
			else
				append(jexps, e);
		}
	}
	/* handle join-ing conditions first */
	if (!list_empty(jexps)) {
		if (list_empty(mexps))
			mexps = jexps;
	}
	/* handle mark conditions second */
	if (!list_empty(mexps)) { 
		assert(list_length(mexps) == 1);
		for( en = mexps->h; en; en = en->next ) {
			sql_exp *e = en->data;
			stmt *ls = exp_bin(be, e->l, left, right, NULL, NULL, NULL, NULL);
			stmt *rs = exp_bin(be, e->r, left, right, NULL, NULL, NULL, NULL);

			if (ls->nrcols == 0)
				ls = stmt_const(be, bin_first_column(be, left), ls);
			if (rs->nrcols == 0)
				rs = stmt_const(be, bin_first_column(be, right), rs);
			join = stmt_tdiff2(be, ls, rs);
		}
	}

	/* construct relation */
	l = sa_list(sql->sa);

	/* project all the left columns */
	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(be, join, column(be, c));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_semijoin(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l; 
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL, *jl, *jr, *c;
	int semi_used = 0;
	int semi_disabled = mvc_debug_on(sql, 2048);

	if (rel->op == op_anti && !list_empty(rel->exps) && list_length(rel->exps) == 1 && ((sql_exp*)rel->exps->h->data)->flag == mark_notin)
		return rel2bin_antijoin(be, rel, refs);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(be, left);
	right = row2cols(be, right);
	/* 
 	 * split in 2 steps, 
 	 * 	first cheap join(s) (equality or idx) 
 	 * 	second selects/filters 
	 */
	if (!semi_disabled && rel->op != op_anti && rel->exps && list_length(rel->exps) == 1) {
		sql_exp *e = rel->exps->h->data;

		if (e->type == e_cmp && (e->flag == cmp_equal || e->flag == mark_in) && !e->anti && !e->f) {
			stmt *r, *l = exp_bin(be, e->l, left, NULL, NULL, NULL, NULL, NULL);
			int swap = 0;

			if (!l) {
				swap = 1;
				l = exp_bin(be, e->l, right, NULL, NULL, NULL, NULL, NULL);
			}
			r = exp_bin(be, e->r, left, right, NULL, NULL, NULL, NULL);

			if (swap) {
				stmt *t = l;
				l = r;
				r = t;
			}

			if (!l || !r)
				return NULL;
			join = stmt_semijoin(be, column(be, l), column(be, r)); 
			if (join)
				join = stmt_result(be, join, 0);
			if (!join)
				return NULL;
			semi_used = 1;
		}
	}
		
	if (!semi_used && rel->exps) {
		int idx = 0;
		list *jexps = sa_list(sql->sa);
		list *lje = sa_list(sql->sa);
		list *rje = sa_list(sql->sa);

		/* get equi-joins/filters first */
		if (list_length(rel->exps) > 1) {
			for( en = rel->exps->h; en; en = en->next ) {
				sql_exp *e = en->data;
				if (e->type == e_cmp && (e->flag == cmp_equal || e->flag == cmp_filter))
					list_append(jexps, e);
			}
			for( en = rel->exps->h; en; en = en->next ) {
				sql_exp *e = en->data;
				if (e->type != e_cmp || (e->flag != cmp_equal && e->flag != cmp_filter))
					list_append(jexps, e);
			}
			rel->exps = jexps;
		}

		for( en = rel->exps->h; en; en = en->next ) {
			int join_idx = sql->opt_stats[0];
			sql_exp *e = en->data;
			stmt *s = NULL;

			/* only handle simple joins here */
			if ((exp_has_func(e) && e->flag != cmp_filter) ||
			    e->flag == cmp_or || (e->f && e->anti)) {
				if (!join && !list_length(lje)) {
					stmt *l = bin_first_column(be, left);
					stmt *r = bin_first_column(be, right);
					join = stmt_join(be, l, r, 0, cmp_all); 
				}
				break;
			}
			if (list_length(lje) && (idx || e->type != e_cmp || (e->flag != cmp_equal && e->flag != cmp_filter) ||
			   (join && e->flag == cmp_filter)))
				break;

			s = exp_bin(be, en->data, left, right, NULL, NULL, NULL, NULL);
			if (!s) {
				assert(0);
				return NULL;
			}
			if (join_idx != sql->opt_stats[0])
				idx = 1;
			/* stop on first non equality join */
			if (!join) {
				if (s->type != st_join && s->type != st_join2 && s->type != st_joinN) {
					if (!en->next && (s->type == st_uselect || s->type == st_uselect2))
						join = s;
					else
						break;
				}
				join = s;
			} else if (s->type != st_join && s->type != st_join2 && s->type != st_joinN) {
				/* handle select expressions */
				break;
			}
			if (s->type == st_join || s->type == st_join2 || s->type == st_joinN) { 
				list_append(lje, s->op1);
				list_append(rje, s->op2);
			}
		}
		if (list_length(lje) > 1) {
			join = releqjoin(be, lje, rje, 0 /* no hash used */, cmp_equal, 0);
		} else if (!join && list_length(lje) == list_length(rje) && list_length(lje)) {
			join = stmt_join(be, lje->h->data, rje->h->data, 0, cmp_equal);
		} else if (!join) {
			stmt *l = bin_first_column(be, left);
			stmt *r = bin_first_column(be, right);
			join = stmt_join(be, l, r, 0, cmp_all); 
		}
	} else if (!semi_used) {
		stmt *l = bin_first_column(be, left);
		stmt *r = bin_first_column(be, right);
		join = stmt_join(be, l, r, 0, cmp_all); 
	}
	if (!semi_used)
		jl = stmt_result(be, join, 0);
	if (!semi_used && en) {
		stmt *sub, *sel = NULL;
		list *nl;

		jr = stmt_result(be, join, 1);
		/* construct relation */
		nl = sa_list(sql->sa);

		/* first project after equi-joins */
		for( n = left->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jl, column(be, c) );
	
			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		for( n = right->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(be, jr, column(be, c) );

			s = stmt_alias(be, s, rnme, nme);
			list_append(nl, s);
		}
		sub = stmt_list(be, nl);

		/* continue with non equi-joins */
		for( ; en; en = en->next ) {
			stmt *s = exp_bin(be, en->data, sub, NULL, NULL, NULL, NULL, sel);

			if (!s) {
				assert(0);
				return NULL;
			}
			if (s->nrcols == 0) {
				stmt *l = bin_first_column(be, sub);
				s = stmt_uselect(be, stmt_const(be, l, stmt_bool(be, 1)), s, cmp_equal, sel, 0);
			}
			sel = s;
		}
		/* recreate join output */
		jl = stmt_project(be, sel, jl); 
	}

	/* construct relation */
	l = sa_list(sql->sa);

	if (!semi_used) {
		/* We did a full join, thats too much. 
	   	Reduce this using difference and intersect */
		c = stmt_mirror(be, left->op4.lval->h->data);
		if (rel->op == op_anti) {
			join = stmt_tdiff(be, c, jl);
		} else {
			join = stmt_tinter(be, c, jl);
		}
	}

	/* project all the left columns */
	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(be, join, column(be, c));

		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_distinct(backend *be, stmt *s, stmt **distinct)
{
	mvc *sql = be->mvc;
	node *n;
	stmt *g = NULL, *grp = NULL, *ext = NULL, *cnt = NULL;
	list *rl = sa_list(sql->sa), *tids;

	/* single values are unique */
	if (s->key && s->nrcols == 0)
		return s;

	/* Use 'all' tid columns */
	if (/* DISABLES CODE */ (0) && (tids = bin_find_columns(be, s, TID)) != NULL) {
		for (n = tids->h; n; n = n->next) {
			stmt *t = n->data;

			g = stmt_group(be, column(be, t), grp, ext, cnt, !n->next);
			grp = stmt_result(be, g, 0); 
			ext = stmt_result(be, g, 1); 
			cnt = stmt_result(be, g, 2); 
		}
	} else {
		for (n = s->op4.lval->h; n; n = n->next) {
			stmt *t = n->data;

			g = stmt_group(be, column(be, t), grp, ext, cnt, !n->next);
			grp = stmt_result(be, g, 0); 
			ext = stmt_result(be, g, 1); 
			cnt = stmt_result(be, g, 2); 
		}
	}
	if (!ext)
		return NULL;

	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *t = n->data;

		list_append(rl, stmt_project(be, ext, t));
	}

	if (distinct)
		*distinct = ext;
	s = stmt_list(be, rl);
	return s;
}

static stmt *
rel_rename(backend *be, sql_rel *rel, stmt *sub)
{
	if (rel->exps) {
		node *en, *n;
		list *l = sa_list(be->mvc->sa);

		for( en = rel->exps->h, n = sub->op4.lval->h; en && n; en = en->next, n = n->next ) {
			sql_exp *exp = en->data;
			stmt *s = n->data;

			if (!s) {
				assert(0);
				return NULL;
			}
			s = stmt_rename(be, exp, s);
			list_append(l, s);
		}
		sub = stmt_list(be, l);
	}
	return sub;
}

static stmt *
rel2bin_union(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l; 
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right) 
		return NULL;

	/* construct relation */
	l = sa_list(sql->sa);
	for( n = left->op4.lval->h, m = right->op4.lval->h; n && m;
		 n = n->next, m = m->next ) {
		stmt *c1 = n->data;
		stmt *c2 = m->data;
		const char *rnme = table_name(sql->sa, c1);
		const char *nme = column_name(sql->sa, c1);
		stmt *s;

		s = stmt_append(be, create_const_column(be, c1), c2);
		s = stmt_alias(be, s, rnme, nme);
		list_append(l, s);
	}
	sub = stmt_list(be, l);

	sub = rel_rename(be, rel, sub);
	if (need_distinct(rel)) 
		sub = rel2bin_distinct(be, sub, NULL);
	return sub;
}

static stmt *
rel2bin_except(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_subtype *lng = sql_bind_localtype("lng");
	list *stmts; 
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;
	sql_subfunc *min;

	stmt *lg = NULL, *rg = NULL;
	stmt *lgrp = NULL, *rgrp = NULL;
	stmt *lext = NULL, *rext = NULL, *next = NULL;
	stmt *lcnt = NULL, *rcnt = NULL, *ncnt = NULL, *zero = NULL;
	stmt *s, *lm, *rm;
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(be, left);
	right = row2cols(be, right);

	/*
	 * The multi column except is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of L with min(L.count,R.count), 
	 * number of rows.
	 */
	for (n = left->op4.lval->h; n; n = n->next) {
		lg = stmt_group(be, column(be, n->data), lgrp, lext, lcnt, !n->next);
		lgrp = stmt_result(be, lg, 0);
		lext = stmt_result(be, lg, 1);
		lcnt = stmt_result(be, lg, 2);
	}
	for (n = right->op4.lval->h; n; n = n->next) {
		rg = stmt_group(be, column(be, n->data), rgrp, rext, rcnt, !n->next);
		rgrp = stmt_result(be, rg, 0);
		rext = stmt_result(be, rg, 1);
		rcnt = stmt_result(be, rg, 2);
	}

	if (!lg || !rg) 
		return NULL;

	if (need_distinct(rel)) {
		lcnt = stmt_const(be, lcnt, stmt_atom_lng(be, 1));
		rcnt = stmt_const(be, rcnt, stmt_atom_lng(be, 1));
	}

	/* now find the matching groups */
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(be, n->data);
		stmt *r = column(be, m->data);

		l = stmt_project(be, lext, l);
		r = stmt_project(be, rext, r);
		list_append(lje, l);
		list_append(rje, r);
	}
	s = releqjoin(be, lje, rje, 1 /* cannot use hash */, cmp_equal_nil, 0);
	lm = stmt_result(be, s, 0);
	rm = stmt_result(be, s, 1);

	s = stmt_mirror(be, lext);
	s = stmt_tdiff(be, s, lm);

	/* first we find those missing in R */
	next = stmt_project(be, s, lext);
	ncnt = stmt_project(be, s, lcnt);
	zero = stmt_const(be, s, stmt_atom_lng(be, 0));

	/* ext, lcount, rcount */
	lext = stmt_project(be, lm, lext);
	lcnt = stmt_project(be, lm, lcnt);
	rcnt = stmt_project(be, rm, rcnt);

	/* append those missing in L */
	lext = stmt_append(be, lext, next);
	lcnt = stmt_append(be, lcnt, ncnt);
	rcnt = stmt_append(be, rcnt, zero);

 	min = sql_bind_func(sql->sa, sql->session->schema, "sql_sub", lng, lng, F_FUNC);
	s = stmt_binop(be, lcnt, rcnt, min); /* use count */

	/* now we have gid,cnt, blowup to full groupsizes */
	s = stmt_gen_group(be, lext, s);

	/* project columns of left hand expression */
	stmts = sa_list(sql->sa);
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(be, n->data);
		const char *rnme = NULL;
		const char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_project(be, s, c1);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(be, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(be, stmts);
	return rel_rename(be, rel, sub);
}

static stmt *
rel2bin_inter(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_subtype *lng = sql_bind_localtype("lng");
	list *stmts; 
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;
 	sql_subfunc *min;

	stmt *lg = NULL, *rg = NULL;
	stmt *lgrp = NULL, *rgrp = NULL;
	stmt *lext = NULL, *rext = NULL;
	stmt *lcnt = NULL, *rcnt = NULL;
	stmt *s, *lm, *rm;
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(be, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(be, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(be, left);

	/*
	 * The multi column intersect is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of L with min(L.count,R.count), 
	 * number of rows.
	 */
	for (n = left->op4.lval->h; n; n = n->next) {
		lg = stmt_group(be, column(be, n->data), lgrp, lext, lcnt, !n->next);
		lgrp = stmt_result(be, lg, 0);
		lext = stmt_result(be, lg, 1);
		lcnt = stmt_result(be, lg, 2);
	}
	for (n = right->op4.lval->h; n; n = n->next) {
		rg = stmt_group(be, column(be, n->data), rgrp, rext, rcnt, !n->next);
		rgrp = stmt_result(be, rg, 0);
		rext = stmt_result(be, rg, 1);
		rcnt = stmt_result(be, rg, 2);
	}

	if (!lg || !rg) 
		return NULL;

	if (need_distinct(rel)) {
		lcnt = stmt_const(be, lcnt, stmt_atom_lng(be, 1));
		rcnt = stmt_const(be, rcnt, stmt_atom_lng(be, 1));
	}

	/* now find the matching groups */
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(be, n->data);
		stmt *r = column(be, m->data);

		l = stmt_project(be, lext, l);
		r = stmt_project(be, rext, r);
		list_append(lje, l);
		list_append(rje, r);
	}
	s = releqjoin(be, lje, rje, 1 /* cannot use hash */, cmp_equal_nil, 0);
	lm = stmt_result(be, s, 0);
	rm = stmt_result(be, s, 1);
		
	/* ext, lcount, rcount */
	lext = stmt_project(be, lm, lext);
	lcnt = stmt_project(be, lm, lcnt);
	rcnt = stmt_project(be, rm, rcnt);

 	min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", lng, lng, F_FUNC);
	s = stmt_binop(be, lcnt, rcnt, min);

	/* now we have gid,cnt, blowup to full groupsizes */
	s = stmt_gen_group(be, lext, s);

	/* project columns of left hand expression */
	stmts = sa_list(sql->sa);
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(be, n->data);
		const char *rnme = NULL;
		const char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_project(be, s, c1);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(be, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(be, stmts);
	return rel_rename(be, rel, sub);
}

static stmt *
sql_reorder(backend *be, stmt *order, stmt *s) 
{
	list *l = sa_list(be->mvc->sa);
	node *n;

	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *sc = n->data;
		const char *cname = column_name(be->mvc->sa, sc);
		const char *tname = table_name(be->mvc->sa, sc);

		sc = stmt_project(be, order, sc);
		sc = stmt_alias(be, sc, tname, cname );
		list_append(l, sc);
	}
	return stmt_list(be, l);
}

static sql_exp*
topn_limit( sql_rel *rel )
{
	if (rel->exps) {
		sql_exp *limit = rel->exps->h->data;

		return limit;
	}
	return NULL;
}

static sql_exp*
topn_offset( sql_rel *rel )
{
	if (rel->exps && list_length(rel->exps) > 1) {
		sql_exp *offset = rel->exps->h->next->data;

		return offset;
	}
	return NULL;
}

static stmt *
rel2bin_project(backend *be, sql_rel *rel, list *refs, sql_rel *topn)
{
	mvc *sql = be->mvc;
	list *pl; 
	node *en, *n;
	stmt *sub = NULL, *psub = NULL;
	stmt *l = NULL;

	if (topn) {
		sql_exp *le = topn_limit(topn);
		sql_exp *oe = topn_offset(topn);

		if (!le) { /* Don't push only offset */
			topn = NULL;
		} else {
			l = exp_bin(be, le, NULL, NULL, NULL, NULL, NULL, NULL);
			if(!l)
				return NULL;
			if (oe) {
				sql_subtype *lng = sql_bind_localtype("lng");
				sql_subfunc *add = sql_bind_func_result(sql->sa, sql->session->schema, "sql_add", F_FUNC, lng, 2, lng, lng);
				stmt *o = exp_bin(be, oe, NULL, NULL, NULL, NULL, NULL, NULL);
				if(!o)
					return NULL;
				l = stmt_binop(be, l, o, add);
			}
		}
	}

	if (!rel->exps) 
		return stmt_none(be);

	if (rel->l) { /* first construct the sub relation */
		sql_rel *l = rel->l;
		if (l->op == op_ddl) {
			sql_table *t = rel_ddl_table_get(l);

			if (t)
				sub = rel2bin_sql_table(be, t);
		} else {
			sub = subrel_bin(be, rel->l, refs);
		}
		if (!sub) 
			return NULL;
	}

	pl = sa_list(sql->sa);
	if (sub)
		pl->expected_cnt = list_length(sub->op4.lval);
	psub = stmt_list(be, pl);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		stmt *s = exp_bin(be, exp, sub, NULL /*psub*/, NULL, NULL, NULL, NULL);

		if (!s) /* try with own projection as well */
			s = exp_bin(be, exp, sub, psub, NULL, NULL, NULL, NULL);
		if (!s) /* error */
			return NULL;
		/* single value with limit */
		if (topn && rel->r && sub && sub->nrcols == 0 && s->nrcols == 0)
			s = const_column(be, s);
		else if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(be, bin_first_column(be, sub), s);

		s = stmt_rename(be, exp, s);
		column_name(sql->sa, s); /* save column name */
		list_append(pl, s);
	}
	stmt_set_nrcols(psub);

	/* In case of a topn 
		if both order by and distinct: then get first order by col 
		do topn on it. Project all again! Then rest
	*/
	if (topn && rel->r) {
		list *oexps = rel->r, *npl = sa_list(sql->sa);
		/* distinct, topn returns atleast N (unique groups) */
		int distinct = need_distinct(rel);
		stmt *limit = NULL, *lpiv = NULL, *lgid = NULL; 

		for (n=oexps->h; n; n = n->next) {
			sql_exp *orderbycole = n->data; 
 			int last = (n->next == NULL);

			stmt *orderbycolstmt = exp_bin(be, orderbycole, sub, psub, NULL, NULL, NULL, NULL); 

			if (!orderbycolstmt) 
				return NULL;
			
			/* handle constants */
			if (orderbycolstmt->nrcols == 0 && !last) /* no need to sort on constant */
				continue; 
			orderbycolstmt = column(be, orderbycolstmt);
			if (!limit) {	/* topn based on a single column */
				limit = stmt_limit(be, orderbycolstmt, NULL, NULL, stmt_atom_lng(be, 0), l, distinct, is_ascending(orderbycole), nulls_last(orderbycole), last, 1);
			} else { 	/* topn based on 2 columns */
				limit = stmt_limit(be, orderbycolstmt, lpiv, lgid, stmt_atom_lng(be, 0), l, distinct, is_ascending(orderbycole), nulls_last(orderbycole), last, 1);
			}
			if (!limit) 
				return NULL;
			lpiv = limit;
			if (!last) {
				lpiv = stmt_result(be, limit, 0);
				lgid = stmt_result(be, limit, 1);
			}
		}

		limit = lpiv; 
		for ( n=pl->h ; n; n = n->next) 
			list_append(npl, stmt_project(be, limit, column(be, n->data)));
		psub = stmt_list(be, npl);

		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		pl = sub->op4.lval;
		npl = sa_list(sql->sa);
		for ( n=pl->h ; n; n = n->next) {
			list_append(npl, stmt_project(be, limit, column(be, n->data))); 
		}
		sub = stmt_list(be, npl);
	}
	if (need_distinct(rel)) {
		stmt *distinct = NULL;
		psub = rel2bin_distinct(be, psub, &distinct);
		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		if (sub && distinct) {
			list *npl = sa_list(sql->sa);
			
			pl = sub->op4.lval;
			for ( n=pl->h ; n; n = n->next) 
				list_append(npl, stmt_project(be, distinct, column(be, n->data))); 
			sub = stmt_list(be, npl);
		}
	}
	if (/*(!topn || need_distinct(rel)) &&*/ rel->r) {
		list *oexps = rel->r;
		stmt *orderby_ids = NULL, *orderby_grp = NULL;

		for (en = oexps->h; en; en = en->next) {
			stmt *orderby = NULL;
			sql_exp *orderbycole = en->data; 
			stmt *orderbycolstmt = exp_bin(be, orderbycole, sub, psub, NULL, NULL, NULL, NULL); 

			if (!orderbycolstmt) {
				assert(0);
				return NULL;
			}
			/* single values don't need sorting */
			if (orderbycolstmt->nrcols == 0) 
				continue;
			if (orderby_ids)
				orderby = stmt_reorder(be, orderbycolstmt, is_ascending(orderbycole), nulls_last(orderbycole), orderby_ids, orderby_grp);
			else
				orderby = stmt_order(be, orderbycolstmt, is_ascending(orderbycole), nulls_last(orderbycole));
			orderby_ids = stmt_result(be, orderby, 1);
			orderby_grp = stmt_result(be, orderby, 2);
		}
		if (orderby_ids)
			psub = sql_reorder(be, orderby_ids, psub);
	}
	return psub;
}

static stmt *
rel2bin_predicate(backend *be) 
{
	return const_column(be, stmt_bool(be, 1));
}

static stmt *
rel2bin_select(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l; 
	node *en, *n;
	stmt *sub = NULL, *sel = NULL;
	stmt *predicate = NULL;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
		if (!sub) 
			return NULL;	
		sub = row2cols(be, sub);
	}
	if (!sub && !predicate) 
		predicate = rel2bin_predicate(be);
	/*
	else if (!predicate)
		predicate = stmt_const(be, bin_first_column(be, sub), stmt_bool(be, 1));
		*/
	if (!rel->exps || !rel->exps->h) {
		if (sub)
			return sub;
		if (predicate)
			return predicate;
		return stmt_const(be, bin_first_column(be, sub), stmt_bool(be, 1));
	}
	if (!sub && predicate) {
		list *l = sa_list(sql->sa);
		assert(predicate);
		append(l, predicate);
		sub = stmt_list(be, l);
	}
	/* handle possible index lookups */
	/* expressions are in index order ! */
	if (sub && (en = rel->exps->h) != NULL) { 
		sql_exp *e = en->data;
		prop *p;

		if ((p=find_prop(e->p, PROP_HASHCOL)) != NULL) {
			sql_idx *i = p->value;
			
			sel = rel2bin_hash_lookup(be, rel, sub, NULL, i, en);
		}
	} 
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *e = en->data;
		stmt *s = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, sel);

		if (!s) {
			assert(0);
			return NULL;
		}
		if (s->nrcols == 0){
			if (!predicate && sub)
				predicate = stmt_const(be, bin_first_column(be, sub), stmt_bool(be, 1));
			if (e->type != e_cmp) {
				sql_subtype *bt = sql_bind_localtype("bit");

				s = stmt_convert(be, s, exp_subtype(e), bt, NULL);
			}
			sel = stmt_uselect(be, predicate, s, cmp_equal, sel, 0);
		} else if (e->type != e_cmp) {
			sel = stmt_uselect(be, s, stmt_bool(be, 1), cmp_equal, sel, 0);
		} else {
			sel = s;
		}
	}

	/* construct relation */
	l = sa_list(sql->sa);
	if (sub && sel) {
		for( n = sub->op4.lval->h; n; n = n->next ) {
			stmt *col = n->data;
	
			if (col->nrcols == 0) /* constant */
				col = stmt_const(be, sel, col);
			else
				col = stmt_project(be, sel, col);
			list_append(l, col);
		}
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_groupby(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l, *aggrs, *gbexps = sa_list(sql->sa);
	node *n, *en;
	stmt *sub = NULL, *cursub;
	stmt *groupby = NULL, *grp = NULL, *ext = NULL, *cnt = NULL;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
		if (!sub)
			return NULL;	
	}

	if (sub && sub->type == st_list && sub->op4.lval->h && !((stmt*)sub->op4.lval->h->data)->nrcols) {
		list *newl = sa_list(sql->sa);
		node *n;

		for(n=sub->op4.lval->h; n; n = n->next) {
			const char *cname = column_name(sql->sa, n->data);
			const char *tname = table_name(sql->sa, n->data);
			stmt *s = column(be, n->data);

			s = stmt_alias(be, s, tname, cname );
			append(newl, s);
		}
		sub = stmt_list(be, newl);
	}

	/* groupby columns */

	/* Keep groupby columns, sub that they can be lookup in the aggr list */
	if (rel->r) {
		list *exps = rel->r; 

		for( en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data; 
			stmt *gbcol = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL); 
	
			if (!gbcol) {
				assert(0);
				return NULL;
			}
			if (!gbcol->nrcols)
				gbcol = stmt_const(be, bin_first_column(be, sub), gbcol);
			groupby = stmt_group(be, gbcol, grp, ext, cnt, !en->next);
			grp = stmt_result(be, groupby, 0);
			ext = stmt_result(be, groupby, 1);
			cnt = stmt_result(be, groupby, 2);
			gbcol = stmt_alias(be, gbcol, exp_find_rel_name(e), exp_name(e));
			list_append(gbexps, gbcol);
		}
	}
	/* now aggregate */
	l = sa_list(sql->sa);
	aggrs = rel->exps;
	cursub = stmt_list(be, l);
	for( n = aggrs->h; n; n = n->next ) {
		sql_exp *aggrexp = n->data;

		stmt *aggrstmt = NULL;

		/* first look in the current aggr list (l) and group by column list */
		if (l && !aggrstmt && aggrexp->type == e_column) 
			aggrstmt = list_find_column(be, l, aggrexp->l, aggrexp->r);
		if (gbexps && !aggrstmt && aggrexp->type == e_column) {
			aggrstmt = list_find_column(be, gbexps, aggrexp->l, aggrexp->r);
			if (aggrstmt && groupby) {
				aggrstmt = stmt_project(be, ext, aggrstmt);
				if (list_length(gbexps) == 1) 
					aggrstmt->key = 1;
			}
		}

		if (!aggrstmt)
			aggrstmt = exp_bin(be, aggrexp, sub, NULL, grp, ext, cnt, NULL); 
		/* maybe the aggr uses intermediate results of this group by,
		   therefore we pass the group by columns too 
		 */
		if (!aggrstmt) 
			aggrstmt = exp_bin(be, aggrexp, sub, cursub, grp, ext, cnt, NULL); 
		if (!aggrstmt) {
			assert(0);
			return NULL;
		}

		aggrstmt = stmt_rename(be, aggrexp, aggrstmt);
		list_append(l, aggrstmt);
	}
	stmt_set_nrcols(cursub);
	return cursub;
}

static stmt *
rel2bin_topn(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	sql_exp *oe = NULL, *le = NULL;
	stmt *sub = NULL, *l = NULL, *o = NULL;
	node *n;

	if (rel->l) { /* first construct the sub relation */
		sql_rel *rl = rel->l;

		if (rl->op == op_project) {
			sub = rel2bin_project(be, rl, refs, rel);
		} else {
			sub = subrel_bin(be, rl, refs);
		}
	}
	if (!sub) 
		return NULL;	

	le = topn_limit(rel);
	oe = topn_offset(rel);

	n = sub->op4.lval->h;
	if (n) {
		stmt *limit = NULL, *sc = n->data;
		const char *cname = column_name(sql->sa, sc);
		const char *tname = table_name(sql->sa, sc);
		list *newl = sa_list(sql->sa);

		if (le)
			l = exp_bin(be, le, NULL, NULL, NULL, NULL, NULL, NULL);
		if (oe)
			o = exp_bin(be, oe, NULL, NULL, NULL, NULL, NULL, NULL);

		if (!l) 
			l = stmt_atom_lng_nil(be);
		if (!o)
			o = stmt_atom_lng(be, 0);
		if (!l || !o)
			return NULL;

		sc = column(be, sc);
		limit = stmt_limit(be, stmt_alias(be, sc, tname, cname), NULL, NULL, o, l, 0,0,0,0,0);

		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(sql->sa, sc);
			const char *tname = table_name(sql->sa, sc);
		
			sc = column(be, sc);
			sc = stmt_project(be, limit, sc);
			list_append(newl, stmt_alias(be, sc, tname, cname));
		}
		sub = stmt_list(be, newl);
	}
	return sub;
}

static stmt *
rel2bin_sample(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *newl;
	stmt *sub = NULL, *sample_size = NULL, *sample = NULL, *seed = NULL;
	node *n;

	if (rel->l) /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
	if (!sub)
		return NULL;

	n = sub->op4.lval->h;
	newl = sa_list(sql->sa);

	if (n) {
		stmt *sc = n->data;
		const char *cname = column_name(sql->sa, sc);
		const char *tname = table_name(sql->sa, sc);

		sample_size = exp_bin(be, rel->exps->h->data, NULL, NULL, NULL, NULL, NULL, NULL);
		if (!sample_size)
			sample_size = stmt_atom_lng_nil(be);

		if (rel->exps->cnt == 2) {
			seed = exp_bin(be, rel->exps->h->next->data, NULL, NULL, NULL, NULL, NULL, NULL);
			if(!seed)
				return NULL;
		}

		sc = column(be, sc);
		sample = stmt_sample(be, stmt_alias(be, sc, tname, cname),sample_size, seed);

		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(sql->sa, sc);
			const char *tname = table_name(sql->sa, sc);
		
			sc = column(be, sc);
			sc = stmt_project(be, sample, sc);
			list_append(newl, stmt_alias(be, sc, tname, cname));
		}
	}
	sub = stmt_list(be, newl);
	return sub;
}

stmt *
sql_parse(backend *be, sql_allocator *sa, const char *query, char mode)
{
	mvc *m = be->mvc;
	mvc *o = NULL;
	stmt *sq = NULL;
	buffer *b;
	char *nquery;
	size_t len = _strlen(query);
	stream *buf;
	bstream * bst;

 	if (THRhighwater())
		return sql_error(m, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	o = MNEW(mvc);
	if (!o)
		return NULL;
	*o = *m;

	m->qc = NULL;

	m->caching = 0;
	m->emode = mode;
	be->depth++;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	if (b == 0) {
		*m = *o;
		GDKfree(o);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	nquery = GDKmalloc(len + 1 + 1);
	if (nquery == 0) {
		*m = *o;
		GDKfree(o);
		GDKfree(b);
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	snprintf(nquery, len + 2, "%s\n", query);
	len++;
	buffer_init(b, nquery, len);
	buf = buffer_rastream(b, "sqlstatement");
	if(buf == NULL) {
		*m = *o;
		GDKfree(o);
		GDKfree(b);
		GDKfree(nquery);
		be->depth--;
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	if((bst = bstream_create(buf, b->len)) == NULL) {
		close_stream(buf);
		*m = *o;
		GDKfree(o);
		GDKfree(b);
		GDKfree(nquery);
		be->depth--;
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	scanner_init( &m->scanner, bst, NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	m->errstr[ERRSIZE-1] = '\0';

	/* create private allocator */
	m->sa = (sa)?sa:sa_create();
	if (!m->sa) {
		bstream_destroy(bst);
		*m = *o;
		GDKfree(o);
		GDKfree(b);
		GDKfree(nquery);
		be->depth--;
		return sql_error(m, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}

	if (sqlparse(m) || !m->sym) {
		/* oops an error */
		snprintf(m->errstr, ERRSIZE, "An error occurred when executing "
				"internal query: %s", nquery);
	} else {
		sql_query *query = query_create(m);
		sql_rel *r = rel_semantic(query, m->sym);

		if (r)
			r = sql_processrelation(m, r, 1);
		if (r)
			sq = rel_bin(be, r);
	}

	GDKfree(nquery);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);
	be->depth--;
	if (m->sa && m->sa != sa)
		sa_destroy(m->sa);
	m->sym = NULL;
	{
		int status = m->session->status;
		int sizevars = m->sizevars, topvars = m->topvars;
		sql_var *vars = m->vars;
		/* cascade list maybe removed */
		list *cascade_action = m->cascade_action;

		strcpy(o->errstr, m->errstr);
		*m = *o;
		m->sizevars = sizevars;
		m->topvars = topvars;
		m->vars = vars;
		m->session->status = status;
		m->cascade_action = cascade_action;
	}
	_DELETE(o);
	return sq;
}

static stmt *
insert_check_ukey(backend *be, list *inserts, sql_key *k, stmt *idx_inserts)
{
	mvc *sql = be->mvc;
/* pkey's cannot have NULLs, ukeys however can
   current implementation switches on 'NOT NULL' on primary key columns */

	char *msg = NULL;
	stmt *res;

	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subtype *bt = sql_bind_localtype("bit");
	stmt *dels = stmt_tid(be, k->t, 0);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", F_FUNC, bt, 2, lng, lng);

	if (list_length(k->columns) > 1) {
		node *m;
		stmt *s = list_fetch(inserts, 0), *ins = s;
		sql_subfunc *sum;
		stmt *ssum = NULL;
		stmt *col = NULL;

		s = ins;
		/* 1st stage: find out if original contains same values */
		if (s->key && s->nrcols == 0) {
			s = NULL;
			if (k->idx && hash_index(k->idx->type))
				s = stmt_uselect(be, stmt_idx(be, k->idx, dels), idx_inserts, cmp_equal_nil, s, 0);
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *cs = list_fetch(inserts, c->c->colnr); 

				col = stmt_col(be, c->c, dels);
				if ((k->type == ukey) && stmt_has_null(col)) {
					stmt *nn = stmt_selectnonil(be, col, s);
					s = stmt_uselect( be, col, cs, cmp_equal, nn, 0);
				} else {
					s = stmt_uselect( be, col, cs, cmp_equal, s, 0);
				}
			}
		} else {
			list *lje = sa_list(sql->sa);
			list *rje = sa_list(sql->sa);
			if (k->idx && hash_index(k->idx->type)) {
				list_append(lje, stmt_idx(be, k->idx, dels));
				list_append(rje, idx_inserts);
			}
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *cs = list_fetch(inserts, c->c->colnr); 

				col = stmt_col(be, c->c, dels);
				list_append(lje, col);
				list_append(rje, cs);
			}
			s = releqjoin(be, lje, rje, 1 /* hash used */, cmp_equal, 0);
			s = stmt_result(be, s, 0);
		}
		s = stmt_binop(be, stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), ne);

		/* 2e stage: find out if inserted are unique */
		if ((!idx_inserts && ins->nrcols) || (idx_inserts && idx_inserts->nrcols)) {	/* insert columns not atoms */
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", F_FUNC, bt, 2, bt, bt);
			stmt *orderby_ids = NULL, *orderby_grp = NULL;

			/* implementation uses sort key check */
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *orderby;
				stmt *cs = list_fetch(inserts, c->c->colnr); 

				if (orderby_grp)
					orderby = stmt_reorder(be, cs, 1, 0, orderby_ids, orderby_grp);
				else
					orderby = stmt_order(be, cs, 1, 0);
				orderby_ids = stmt_result(be, orderby, 1);
				orderby_grp = stmt_result(be, orderby, 2);
			}

			if (!orderby_grp || !orderby_ids)
				return NULL;

			sum = sql_bind_func(sql->sa, sql->session->schema, "not_unique", tail_type(orderby_grp), NULL, F_AGGR);
			ssum = stmt_aggr(be, orderby_grp, NULL, NULL, sum, 1, 0, 1);
			/* combine results */
			s = stmt_binop(be, s, ssum, or);
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, "INSERT INTO: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "INSERT INTO: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	} else {		/* single column key */
		sql_kc *c = k->columns->h->data;
		stmt *s = list_fetch(inserts, c->c->colnr), *h = s;

		s = stmt_col(be, c->c, dels);
		if ((k->type == ukey) && stmt_has_null(s)) {
			stmt *nn = stmt_selectnonil(be, s, NULL);
			s = stmt_project(be, nn, s);
		}
		if (h->nrcols) {
			s = stmt_join(be, s, h, 0, cmp_equal);
			/* s should be empty */
			s = stmt_result(be, s, 0);
			s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
		} else {
			s = stmt_uselect(be, s, h, cmp_equal, NULL, 0);
			/* s should be empty */
			s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
		}
		/* s should be empty */
		s = stmt_binop(be, s, stmt_atom_lng(be, 0), ne);

		/* 2e stage: find out if inserts are unique */
		if (h->nrcols) {	/* insert multiple atoms */
			sql_subfunc *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", F_FUNC, bt, 2, bt, bt);
			stmt *ssum, *ss;

			stmt *g = list_fetch(inserts, c->c->colnr), *ins = g;

			/* inserted vaules may be null */
			if ((k->type == ukey) && stmt_has_null(ins)) {
				stmt *nn = stmt_selectnonil(be, ins, NULL);
				ins = stmt_project(be, nn, ins);
			}
		
			g = stmt_group(be, ins, NULL, NULL, NULL, 1);
			ss = stmt_result(be, g, 2); /* use count */
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_func(sql->sa, sql->session->schema, "sum", lng, NULL, F_AGGR);
			ssum = stmt_aggr(be, ss, NULL, NULL, sum, 1, 0, 1);
			ssum = sql_Nop_(be, "ifthenelse", sql_unop_(be, NULL, "isnull", ssum), stmt_atom_lng(be, 0), ssum, NULL);
			count_sum = stmt_binop(be, check_types(be, tail_type(ssum), stmt_aggr(be, ss, NULL, NULL, cnt, 1, 0, 1), type_equal), ssum, ne);

			/* combine results */
			s = stmt_binop(be, s, count_sum, or);
		}
		if (k->type == pkey) {
			msg = sa_message( sql->sa,"INSERT INTO: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "INSERT INTO: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	}
	return res;
}

static stmt *
insert_check_fkey(backend *be, list *inserts, sql_key *k, stmt *idx_inserts, stmt *pin)
{
	mvc *sql = be->mvc;
	char *msg = NULL;
	stmt *cs = list_fetch(inserts, 0), *s = cs;
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", F_FUNC, bt, 2, lng, lng);

	if (pin && list_length(pin->op4.lval)) 
		s = pin->op4.lval->h->data;
	if (s->key && s->nrcols == 0) {
		s = stmt_binop(be, stmt_aggr(be, idx_inserts, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 1), ne);
	} else {
		/* releqjoin.count <> inserts[col1].count */
		s = stmt_binop(be, stmt_aggr(be, idx_inserts, NULL, NULL, cnt, 1, 0, 1), stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1), ne);
	}

	/* s should be empty */
	msg = sa_message(sql->sa, "INSERT INTO: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(be, s, msg, 00001);
}

static stmt *
sql_insert_key(backend *be, list *inserts, sql_key *k, stmt *idx_inserts, stmt *pin)
{
	/* int insert = 1;
	 * while insert and has u/pkey and not defered then
	 *      if u/pkey values exist then
	 *              insert = 0
	 * while insert and has fkey and not defered then
	 *      find id of corresponding u/pkey  
	 *      if (!found)
	 *              insert = 0
	 * if insert
	 *      insert values
	 *      insert fkey/pkey index
	 */
	if (k->type == pkey || k->type == ukey) {
		return insert_check_ukey(be, inserts, k, idx_inserts );
	} else {		/* foreign keys */
		return insert_check_fkey(be, inserts, k, idx_inserts, pin );
	}
}

static int
sql_stack_add_inserted( mvc *sql, const char *name, sql_table *t, stmt **updates) 
{
	/* Put single relation of updates and old values on to the stack */
	sql_rel *r = NULL;
	node *n;
	list *exps = sa_list(sql->sa);
	trigger_input *ti = SA_NEW(sql->sa, trigger_input);

	ti->t = t;
	ti->tids = NULL;
	ti->updates = updates;
	ti->type = 1;
	ti->nn = name;
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_exp *ne = exp_column(sql->sa, name, c->base.name, &c->type, CARD_MULTI, c->null, 0);

		append(exps, ne);
	}
	r = rel_table_func(sql->sa, NULL, NULL, exps, 2);
	r->l = ti;

	return stack_push_rel_view(sql, name, r) ? 1 : 0;
}

static int
sql_insert_triggers(backend *be, sql_table *t, stmt **updates, int time)
{
	mvc *sql = be->mvc;
	node *n;
	int res = 1;

	if (!t->triggers.set)
		return res;

	for (n = t->triggers.set->h; n; n = n->next) {
		sql_trigger *trigger = n->data;

		if(!stack_push_frame(sql, "OLD-NEW"))
			return 0;
		if (trigger->event == 0 && trigger->time == time) {
			const char *n = trigger->new_name;

			/* add name for the 'inserted' to the stack */
			if (!n) n = "new";

			if(!sql_stack_add_inserted(sql, n, t, updates)) {
				stack_pop_frame(sql);
				return 0;
			}
			if (!sql_parse(be, sql->sa, trigger->statement, m_instantiate)) {
				stack_pop_frame(sql);
				return 0;
			}
		}
		stack_pop_frame(sql);
	}
	return res;
}

static void 
sql_insert_check_null(backend *be, sql_table *t, list *inserts) 
{
	mvc *sql = be->mvc;
	node *m, *n;
	sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);

	for (n = t->columns.set->h, m = inserts->h; n && m; 
		n = n->next, m = m->next) {
		stmt *i = m->data;
		sql_column *c = n->data;

		if (!c->null) {
			stmt *s = i;
			char *msg = NULL;

			if (!(s->key && s->nrcols == 0)) {
				s = stmt_selectnil(be, i);
				s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
			} else {
				sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->type, NULL, F_FUNC);

				s = stmt_unop(be, i, isnil);
			}
			msg = sa_message(sql->sa, "INSERT INTO: NOT NULL constraint violated for column %s.%s", c->t->base.name, c->base.name);
			(void)stmt_exception(be, s, msg, 00001);
		}
	}
}

static stmt ** 
table_update_stmts(mvc *sql, sql_table *t, int *Len)
{
	*Len = list_length(t->columns.set);
	return SA_ZNEW_ARRAY(sql->sa, stmt *, *Len);
}

static stmt *
rel2bin_insert(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l;
	stmt *inserts = NULL, *insert = NULL, *s, *ddl = NULL, *pin = NULL, **updates, *ret = NULL;
	int idx_ins = 0, constraint = 1, len = 0;
	node *n, *m;
	sql_rel *tr = rel->l, *prel = rel->r;
	sql_table *t = NULL;

	if ((rel->flag&UPD_NO_CONSTRAINT)) 
		constraint = 0;
	if ((rel->flag&UPD_COMP)) {  /* special case ! */
		idx_ins = 1;
		prel = rel->l;
		rel = rel->r;
		tr = rel->l;
	}

	if (tr->op == op_basetable) {
		t = tr->l;
	} else {
		ddl = subrel_bin(be, tr, refs);
		if (!ddl)
			return NULL;
		t = rel_ddl_table_get(tr);
	}

	if (rel->r) /* first construct the inserts relation */
		inserts = subrel_bin(be, rel->r, refs);

	if (!inserts)
		return NULL;	

	if (idx_ins)
		pin = refs_find_rel(refs, prel);

	if (constraint && !be->first_statement_generated)
		sql_insert_check_null(be, (be->cur_append && t->p) ? t->p : t, inserts->op4.lval);

	l = sa_list(sql->sa);

	updates = table_update_stmts(sql, t, &len); 
	for (n = t->columns.set->h, m = inserts->op4.lval->h; n && m; n = n->next, m = m->next) {
		sql_column *c = n->data;

		updates[c->colnr] = m->data;
	}

/* before */
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_insert_triggers(be, up, updates, 0))
				return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_insert_triggers(be, t, updates, 0)) 
		return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", t->base.name);

	if (t->idxs.set)
	for (n = t->idxs.set->h; n && m; n = n->next, m = m->next) {
		stmt *is = m->data;
		sql_idx *i = n->data;

		if (non_updatable_index(i->type)) /* Some indexes don't hold delta structures */
			continue;
		if (hash_index(i->type) && list_length(i->columns) <= 1)
			is = NULL;
		if (i->key && constraint) {
			stmt *ckeys = sql_insert_key(be, inserts->op4.lval, i->key, is, pin);

			list_append(l, ckeys);
		}
		if (!insert)
			insert = is;
		if (is)
			is = stmt_append_idx(be, i, is);
	}

	for (n = t->columns.set->h, m = inserts->op4.lval->h; 
		n && m; n = n->next, m = m->next) {

		stmt *ins = m->data;
		sql_column *c = n->data;

		insert = stmt_append_col(be, c, ins, rel->flag&UPD_LOCKED);
		append(l,insert);
	}
	if (!insert)
		return NULL;

	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_insert_triggers(be, up, updates, 1))
				return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_insert_triggers(be, t, updates, 1)) 
		return sql_error(sql, 02, SQLSTATE(27000) "INSERT INTO: triggers failed for table '%s'", t->base.name);
	if (ddl) {
		ret = ddl;
		list_prepend(l, ddl);
	} else {
		if (insert->op1->nrcols == 0) {
			s = stmt_atom_lng(be, 1);
		} else {
			s = stmt_aggr(be, insert->op1, NULL, NULL, sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
		}
		ret = s;
	}

	if (be->cur_append) /* building the total number of rows affected across all tables */
		ret->nr = add_to_merge_partitions_accumulator(be, ret->nr);

	if (ddl)
		return stmt_list(be, l);
	else
		return ret;
}

static int
is_idx_updated(sql_idx * i, stmt **updates)
{
	int update = 0;
	node *m;

	for (m = i->columns->h; m; m = m->next) {
		sql_kc *ic = m->data;

		if (updates[ic->c->colnr]) {
			update = 1;
			break;
		}
	}
	return update;
}

static int
first_updated_col(stmt **updates, int cnt)
{
	int i;

	for (i = 0; i < cnt; i++) {
		if (updates[i])
			return i;
	}
	return -1;
}

static stmt *
update_check_ukey(backend *be, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol)
{
	mvc *sql = be->mvc;
	char *msg = NULL;
	stmt *res = NULL;

	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne;

	(void)tids;
	ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", F_FUNC, bt, 2, lng, lng);
	if (list_length(k->columns) > 1) {
		stmt *dels = stmt_tid(be, k->t, 0);
		node *m;
		stmt *s = NULL;

		/* 1st stage: find out if original (without the updated) 
			do not contain the same values as the updated values. 
			This is done using a relation join and a count (which 
			should be zero)
	 	*/
		if (!isNew(k)) {
			stmt *nu_tids = stmt_tdiff(be, dels, tids); /* not updated ids */
			list *lje = sa_list(sql->sa);
			list *rje = sa_list(sql->sa);

			if (k->idx && hash_index(k->idx->type)) {
				list_append(lje, stmt_idx(be, k->idx, nu_tids));
				list_append(rje, idx_updates);
			}
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd;

				assert(updates);
				if (updates[c->c->colnr]) {
					upd = updates[c->c->colnr];
				} else {
					upd = stmt_project(be, tids, stmt_col(be, c->c, dels));
				}
				list_append(lje, stmt_col(be, c->c, nu_tids));
				list_append(rje, upd);
			}
			s = releqjoin(be, lje, rje, 1 /* hash used */, cmp_equal, 0);
			s = stmt_result(be, s, 0);
			s = stmt_binop(be, stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), ne);
		}

		/* 2e stage: find out if the updated are unique */
		if (!updates || updates[updcol]->nrcols) {	/* update columns not atoms */
			sql_subfunc *sum;
			stmt *count_sum = NULL, *ssum;
			stmt *g = NULL, *grp = NULL, *ext = NULL, *Cnt = NULL;
			stmt *cand = NULL;
			stmt *ss;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", F_FUNC, bt, 2, bt, bt);

			/* also take the hopefully unique hash keys, to reduce
			   (re)group costs */
			if (k->idx && hash_index(k->idx->type)) {
				g = stmt_group(be, idx_updates, grp, ext, Cnt, 0);
				grp = stmt_result(be, g, 0);
				ext = stmt_result(be, g, 1);
				Cnt = stmt_result(be, g, 2);

				/* continue only with groups with a cnt > 1 */
				cand = stmt_uselect(be, Cnt, stmt_atom_lng(be, 1), cmp_gt, NULL, 0);
				/* project cand on ext and Cnt */
				Cnt = stmt_project(be, cand, Cnt);
				ext = stmt_project(be, cand, ext);

				/* join groups with extend to retrieve all oid's of the original
				 * bat that belong to a group with Cnt >1 */
				g = stmt_join(be, grp, ext, 0, cmp_equal);
				cand = stmt_result(be, g, 0);
				grp = stmt_project(be, cand, grp);
			}

			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd;

				if (updates && updates[c->c->colnr]) {
					upd = updates[c->c->colnr];
					/*
				} else if (updates) {
					//upd = updates[updcol]->op1;
					//upd = stmt_project(be, upd, stmt_col(be, c->c, dels));
					upd = stmt_project(be, tids, stmt_col(be, c->c, dels));
					*/
				} else {
					upd = stmt_project(be, tids, stmt_col(be, c->c, dels));
				}

				/* apply cand list first */
				if (cand)
					upd = stmt_project(be, cand, upd);

				/* remove nulls */
				if ((k->type == ukey) && stmt_has_null(upd)) {
					stmt *nn = stmt_selectnonil(be, upd, NULL);
					upd = stmt_project(be, nn, upd);
					if (grp)
						grp = stmt_project(be, nn, grp);
					if (cand)
						cand = stmt_project(be, nn, cand);
				}

				/* apply group by on groups with Cnt > 1 */
				g = stmt_group(be, upd, grp, ext, Cnt, !m->next);
				grp = stmt_result(be, g, 0);
				ext = stmt_result(be, g, 1);
				Cnt = stmt_result(be, g, 2);
			}
			ss = Cnt; /* use count */
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_func(sql->sa, sql->session->schema, "sum", lng, NULL, F_AGGR);
			ssum = stmt_aggr(be, ss, NULL, NULL, sum, 1, 0, 1);
			ssum = sql_Nop_(be, "ifthenelse", sql_unop_(be, NULL, "isnull", ssum), stmt_atom_lng(be, 0), ssum, NULL);
			count_sum = stmt_binop(be, stmt_aggr(be, ss, NULL, NULL, cnt, 1, 0, 1), check_types(be, lng, ssum, type_equal), ne);

			/* combine results */
			if (s) 
				s = stmt_binop(be, s, count_sum, or);
			else
				s = count_sum;
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, "UPDATE: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "UPDATE: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	} else {		/* single column key */
		stmt *dels = stmt_tid(be, k->t, 0);
		sql_kc *c = k->columns->h->data;
		stmt *s = NULL, *h = NULL, *o;

		/* s should be empty */
		if (!isNew(k)) {
			stmt *nu_tids = stmt_tdiff(be, dels, tids); /* not updated ids */
			assert (updates);

			h = updates[c->c->colnr];
			o = stmt_col(be, c->c, nu_tids);
			s = stmt_join(be, o, h, 0, cmp_equal);
			s = stmt_result(be, s, 0);
			s = stmt_binop(be, stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), ne);
		}

		/* 2e stage: find out if updated are unique */
		if (!h || h->nrcols) {	/* update columns not atoms */
			sql_subfunc *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", F_FUNC, bt, 2, bt, bt);
			stmt *ssum, *ss;
			stmt *upd;
			stmt *g;

			if (updates) {
 				upd = updates[c->c->colnr];
			} else {
 				upd = stmt_col(be, c->c, dels);
			}

			/* remove nulls */
			if ((k->type == ukey) && stmt_has_null(upd)) {
				stmt *nn = stmt_selectnonil(be, upd, NULL);
				upd = stmt_project(be, nn, upd);
			}

			g = stmt_group(be, upd, NULL, NULL, NULL, 1);
			ss = stmt_result(be, g, 2); /* use count */

			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_func(sql->sa, sql->session->schema, "sum", lng, NULL, F_AGGR);
			ssum = stmt_aggr(be, ss, NULL, NULL, sum, 1, 0, 1);
			ssum = sql_Nop_(be, "ifthenelse", sql_unop_(be, NULL, "isnull", ssum), stmt_atom_lng(be, 0), ssum, NULL);
			count_sum = stmt_binop(be, check_types(be, tail_type(ssum), stmt_aggr(be, ss, NULL, NULL, cnt, 1, 0, 1), type_equal), ssum, ne);

			/* combine results */
			if (s)
				s = stmt_binop(be, s, count_sum, or);
			else
				s = count_sum;
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, "UPDATE: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "UPDATE: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(be, s, msg, 00001);
	}
	return res;
}

/*
         A referential constraint is satisfied if one of the following con-
         ditions is true, depending on the <match option> specified in the
         <referential constraint definition>:

         -  If no <match type> was specified then, for each row R1 of the
            referencing table, either at least one of the values of the
            referencing columns in R1 shall be a null value, or the value of
            each referencing column in R1 shall be equal to the value of the
            corresponding referenced column in some row of the referenced
            table.

         -  If MATCH FULL was specified then, for each row R1 of the refer-
            encing table, either the value of every referencing column in R1
            shall be a null value, or the value of every referencing column
            in R1 shall not be null and there shall be some row R2 of the
            referenced table such that the value of each referencing col-
            umn in R1 is equal to the value of the corresponding referenced
            column in R2.

         -  If MATCH PARTIAL was specified then, for each row R1 of the
            referencing table, there shall be some row R2 of the refer-
            enced table such that the value of each referencing column in
            R1 is either null or is equal to the value of the corresponding
            referenced column in R2.
*/

static stmt *
update_check_fkey(backend *be, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol, stmt *pup)
{
	mvc *sql = be->mvc;
	char *msg = NULL;
	stmt *s, *cur, *null = NULL, *cntnulls;
	sql_subtype *lng = sql_bind_localtype("lng"), *bt = sql_bind_localtype("bit");
	sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", F_FUNC, bt, 2, lng, lng);
	sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", F_FUNC, bt, 2, bt, bt);
	node *m;

	if (!idx_updates)
		return NULL;
	/* releqjoin.count <> updates[updcol].count */
	if (pup && list_length(pup->op4.lval)) {
		cur = pup->op4.lval->h->data;
	} else if (updates) {
		cur = updates[updcol];
	} else {
		sql_kc *c = k->columns->h->data;
		stmt *dels = stmt_tid(be, k->t, 0);
		assert(0);
		cur = stmt_col(be, c->c, dels);
	}
	s = stmt_binop(be, stmt_aggr(be, idx_updates, NULL, NULL, cnt, 1, 0, 1), stmt_aggr(be, cur, NULL, NULL, cnt, 1, 0, 1), ne);

	for (m = k->columns->h; m; m = m->next) {
		sql_kc *c = m->data;

		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		if (c->c->null) {
			stmt *upd, *nn;

			if (updates && updates[c->c->colnr]) {
				upd = updates[c->c->colnr];
			} else if (updates && updcol >= 0) {
				assert(0);
				//upd = updates[updcol]->op1;
				//upd = stmt_project(be, upd, stmt_col(be, c->c, tids));
				upd = stmt_col(be, c->c, tids);
			} else { /* created idx/key using alter */ 
				upd = stmt_col(be, c->c, tids);
			}
			nn = stmt_selectnil(be, upd);
			if (null)
				null = stmt_tunion(be, null, nn);
			else
				null = nn;
		}
	}
	if (null) {
		cntnulls = stmt_aggr(be, null, NULL, NULL, cnt, 1, 0, 1); 
	} else {
		cntnulls = stmt_atom_lng(be, 0);
	}
	s = stmt_binop(be, s, 
		stmt_binop(be, stmt_aggr(be, stmt_selectnil(be, idx_updates), NULL, NULL, cnt, 1, 0, 1), cntnulls , ne), or);

	/* s should be empty */
	msg = sa_message(sql->sa, "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(be, s, msg, 00001);
}

static stmt *
join_updated_pkey(backend *be, sql_key * k, stmt *tids, stmt **updates)
{
	mvc *sql = be->mvc;
	char *msg = NULL;
	int nulls = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt *s = NULL, *dels = stmt_tid(be, rk->t, 0), *fdels, *cnteqjoin;
	stmt *null = NULL, *rows;
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", F_FUNC, bt, 2, lng, lng);
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	fdels = stmt_tid(be, k->idx->t, 0);
	rows = stmt_idx(be, k->idx, fdels);

	rows = stmt_join(be, rows, tids, 0, cmp_equal); /* join over the join index */
	rows = stmt_result(be, rows, 0);

	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd, *col;

		if (updates[c->c->colnr]) {
			upd = updates[c->c->colnr];
		} else {
			assert(0);
			//upd = updates[updcol]->op1;
			upd = stmt_project(be, tids, stmt_col(be, c->c, dels));
		}
		if (c->c->null) {	/* new nulls (MATCH SIMPLE) */
			stmt *nn = stmt_selectnil(be, upd);
			if (null)
				null = stmt_tunion(be, null, nn);
			else
				null = nn;
			nulls = 1;
		}
		col = stmt_col(be, fc->c, rows);
		list_append(lje, upd);
		list_append(rje, col);
	}
	s = releqjoin(be, lje, rje, 1 /* hash used */, cmp_equal, 0);
	s = stmt_result(be, s, 0);

	/* add missing nulls */
	cnteqjoin = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
	if (nulls) {
		sql_subfunc *add = sql_bind_func_result(sql->sa, sql->session->schema, "sql_add", F_FUNC, lng, 2, lng, lng);
		cnteqjoin = stmt_binop(be, cnteqjoin, stmt_aggr(be, null, NULL, NULL, cnt, 1, 0, 1), add);
	}

	/* releqjoin.count <> updates[updcol].count */
	s = stmt_binop(be, cnteqjoin, stmt_aggr(be, rows, NULL, NULL, cnt, 1, 0, 1), ne);

	/* s should be empty */
	msg = sa_message(sql->sa, "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(be, s, msg, 00001);
}

static list * sql_update(backend *be, sql_table *t, stmt *rows, stmt **updates);

static stmt*
sql_delete_set_Fkeys(backend *be, sql_key *k, stmt *ftids /* to be updated rows of fkey table */, int action)
{
	mvc *sql = be->mvc;
	list *l = NULL;
	int len = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt **new_updates;
	sql_table *t = mvc_bind_table(sql, k->t->s, k->t->base.name);

	new_updates = table_update_stmts(sql, t, &len);
	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		stmt *upd = NULL;

		if (action == ACT_SET_DEFAULT) {
			if (fc->c->def) {
				stmt *sq;
				char *msg, *typestr = subtype2string2(&fc->c->type);
				if(!typestr)
					return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				msg = sa_message(sql->sa, "select cast(%s as %s);", fc->c->def, typestr);
				_DELETE(typestr);
				sq = rel_parse_value(be, msg, sql->emode);
				if (!sq) 
					return NULL;
				upd = sq;
			}  else {
				upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
			}
		} else {
			upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
		}
		
		if (!upd || (upd = check_types(be, &fc->c->type, upd, type_equal)) == NULL) 
			return NULL;

		if (upd->nrcols <= 0) 
			upd = stmt_const(be, ftids, upd);
		
		new_updates[fc->c->colnr] = upd;
	}
	if ((l = sql_update(be, t, ftids, new_updates)) == NULL) 
		return NULL;
	return stmt_list(be, l);
}

static stmt*
sql_update_cascade_Fkeys(backend *be, sql_key *k, stmt *utids, stmt **updates, int action)
{
	mvc *sql = be->mvc;
	list *l = NULL;
	int len = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt **new_updates;
	stmt *rows;
	sql_table *t = mvc_bind_table(sql, k->t->s, k->t->base.name);
	stmt *ftids, *upd_ids;

	ftids = stmt_tid(be, k->idx->t, 0);
	rows = stmt_idx(be, k->idx, ftids);

	rows = stmt_join(be, rows, utids, 0, cmp_equal); /* join over the join index */
	upd_ids = stmt_result(be, rows, 1);
	rows = stmt_result(be, rows, 0);
	rows = stmt_project(be, rows, ftids);
		
	new_updates = table_update_stmts(sql, t, &len);
	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd = NULL;

		if (!updates[c->c->colnr]) {
			continue;
		} else if (action == ACT_CASCADE) {
			upd = updates[c->c->colnr];
		} else if (action == ACT_SET_DEFAULT) {
			if (fc->c->def) {
				stmt *sq;
				char *msg, *typestr = subtype2string2(&fc->c->type);
				if(!typestr)
					return sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
				msg = sa_message(sql->sa, "select cast(%s as %s);", fc->c->def, typestr);
				_DELETE(typestr);
				sq = rel_parse_value(be, msg, sql->emode);
				if (!sq) 
					return NULL;
				upd = sq;
			} else {
				upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
			}
		} else if (action == ACT_SET_NULL) {
			upd = stmt_atom(be, atom_general(sql->sa, &fc->c->type, NULL));
		}

		if (!upd || (upd = check_types(be, &fc->c->type, upd, type_equal)) == NULL) 
			return NULL;

		if (upd->nrcols <= 0) 
			upd = stmt_const(be, upd_ids, upd);
		else
			upd = stmt_project(be, upd_ids, upd);
		
		new_updates[fc->c->colnr] = upd;
	}

	if ((l = sql_update(be, t, rows, new_updates)) == NULL) 
		return NULL;
	return stmt_list(be, l);
}


static int
cascade_ukey(backend *be, stmt **updates, sql_key *k, stmt *tids) 
{
	sql_ukey *uk = (sql_ukey*)k;

	if (uk->keys && list_length(uk->keys) > 0) {
		node *n;
		for(n = uk->keys->h; n; n = n->next) {
			sql_key *fk = n->data;

			/* All rows of the foreign key table which are
			   affected by the primary key update should all
			   match one of the updated primary keys again.
			 */
			switch (((sql_fkey*)fk)->on_update) {
				case ACT_NO_ACTION: 
					break;
				case ACT_SET_NULL: 
				case ACT_SET_DEFAULT: 
				case ACT_CASCADE: 
					if (!sql_update_cascade_Fkeys(be, fk, tids, updates, ((sql_fkey*)fk)->on_update))
						return -1;
					break;
				default:	/*RESTRICT*/
					if (!join_updated_pkey(be, fk, tids, updates))
						return -1;
			}
		}
	}
	return 0;
}

static void
sql_update_check_key(backend *be, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol, list *l, stmt *pup)
{
	stmt *ckeys;

	if (k->type == pkey || k->type == ukey) {
		ckeys = update_check_ukey(be, updates, k, tids, idx_updates, updcol);
	} else { /* foreign keys */
		ckeys = update_check_fkey(be, updates, k, tids, idx_updates, updcol, pup);
	}
	list_append(l, ckeys);
}

static stmt *
hash_update(backend *be, sql_idx * i, stmt *rows, stmt **updates, int updcol)
{
	mvc *sql = be->mvc;
	/* calculate new value */
	node *m;
	sql_subtype *it, *lng;
	int bits = 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1);
	stmt *h = NULL, *tids;

	if (list_length(i->columns) <= 1)
		return NULL;

	tids = stmt_tid(be, i->t, 0);
	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = i->columns->h; m; m = m->next ) {
		sql_kc *c = m->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr];
		} else if (updates && updcol >= 0) {
			assert(0);
			//upd = updates[updcol]->op1;
			//upd = rows;
			//upd = stmt_project(be, upd, stmt_col(be, c->c, tids));
			upd = stmt_col(be, c->c, rows);
		} else { /* created idx/key using alter */ 
			upd = stmt_col(be, c->c, tids);
		}

		if (h && i->type == hash_idx)  { 
			sql_subfunc *xor = sql_bind_func_result(sql->sa, sql->session->schema, "rotate_xor_hash", F_FUNC, lng, 3, lng, it, &c->c->type);

			h = stmt_Nop(be, stmt_list( be, list_append( list_append(
				list_append(sa_list(sql->sa), h), 
				stmt_atom_int(be, bits)),  upd)),
				xor);
		} else if (h)  { 
			stmt *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql->sa, sql->session->schema, "left_shift", F_FUNC, lng, 2, lng, it);
			sql_subfunc *lor = sql_bind_func_result(sql->sa, sql->session->schema, "bit_or", F_FUNC, lng, 2, lng, lng);
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", F_FUNC, lng, 1, &c->c->type);

			h = stmt_binop(be, h, stmt_atom_int(be, bits), lsh); 
			h2 = stmt_unop(be, upd, hf);
			h = stmt_binop(be, h, h2, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", F_FUNC, lng, 1, &c->c->type);
			h = stmt_unop(be, upd, hf);
			if (i->type == oph_idx)
				break;
		}
	}
	return h;
}

static stmt *
join_idx_update(backend *be, sql_idx * i, stmt *ftids, stmt **updates, int updcol)
{
	mvc *sql = be->mvc;
	node *m, *o;
	sql_key *rk = &((sql_fkey *) i->key)->rkey->k;
	stmt *s = NULL, *ptids = stmt_tid(be, rk->t, 0), *l, *r;
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr];
		} else if (updates && updcol >= 0) {
			assert(0);
			//upd = updates[updcol]->op1;
			//upd = stmt_project(be, upd, stmt_col(be, c->c, ftids));
			upd = stmt_col(be, c->c, ftids);
		} else { /* created idx/key using alter */ 
			upd = stmt_col(be, c->c, ftids);
		}

		list_append(lje, check_types(be, &rc->c->type, upd, type_equal));
		list_append(rje, stmt_col(be, rc->c, ptids));
	}
	s = releqjoin(be, lje, rje, 0 /* use hash */, cmp_equal, 0);
	l = stmt_result(be, s, 0);
	r = stmt_result(be, s, 1);
	r = stmt_project(be, r, ptids);
	return stmt_left_project(be, ftids, l, r);
}

static int
cascade_updates(backend *be, sql_table *t, stmt *rows, stmt **updates)
{
	mvc *sql = be->mvc;
	node *n;

	if (!t->idxs.set)
		return 0;

	for (n = t->idxs.set->h; n; n = n->next) {
		sql_idx *i = n->data;

		/* check if update is needed, 
		 * ie atleast on of the idx columns is updated 
		 */
		if (is_idx_updated(i, updates) == 0)
			continue;

		if (i->key) {
			if (!(sql->cascade_action && list_find_id(sql->cascade_action, i->key->base.id))) {
				sql_key *k = i->key;
				int *local_id = SA_NEW(sql->sa, int);
				if (!sql->cascade_action) 
					sql->cascade_action = sa_list(sql->sa);
				*local_id = i->key->base.id;
				list_append(sql->cascade_action, local_id);
				if (k->type == pkey || k->type == ukey) {
					if (cascade_ukey(be, updates, k, rows))
						return -1;
				}
			}
		}
	}
	return 0;
}

static list *
update_idxs_and_check_keys(backend *be, sql_table *t, stmt *rows, stmt **updates, list *l, stmt *pup)
{
	mvc *sql = be->mvc;
	node *n;
	int updcol;
	list *idx_updates = sa_list(sql->sa);

	if (!t->idxs.set)
		return idx_updates;

	updcol = first_updated_col(updates, list_length(t->columns.set));
	for (n = t->idxs.set->h; n; n = n->next) {
		sql_idx *i = n->data;
		stmt *is = NULL;

		/* check if update is needed, 
		 * ie atleast on of the idx columns is updated 
		 */
		if (is_idx_updated(i, updates) == 0)
			continue;

		if (hash_index(i->type)) {
			is = hash_update(be, i, rows, updates, updcol);
		} else if (i->type == join_idx) {
			if (updcol < 0)
				return NULL;
			is = join_idx_update(be, i, rows, updates, updcol);
		}
		if (i->key) 
			sql_update_check_key(be, updates, i->key, rows, is, updcol, l, pup);
		if (is) 
			list_append(idx_updates, stmt_update_idx(be, i, rows, is));
	}
	return idx_updates;
}

static int
sql_stack_add_updated(mvc *sql, const char *on, const char *nn, sql_table *t, stmt *tids, stmt **updates)
{
	/* Put single relation of updates and old values on to the stack */
	sql_rel *r = NULL;
	node *n;
	list *exps = sa_list(sql->sa);
	trigger_input *ti = SA_NEW(sql->sa, trigger_input);

	ti->t = t;
	ti->tids = tids;
	ti->updates = updates;
	ti->type = 2;
	ti->on = on;
	ti->nn = nn;
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if (updates[c->colnr]) {
			sql_exp *oe = exp_column(sql->sa, on, c->base.name, &c->type, CARD_MULTI, c->null, 0);
			sql_exp *ne = exp_column(sql->sa, nn, c->base.name, &c->type, CARD_MULTI, c->null, 0);

			append(exps, oe);
			append(exps, ne);
		} else {
			sql_exp *oe = exp_column(sql->sa, on, c->base.name, &c->type, CARD_MULTI, c->null, 0);
			sql_exp *ne = exp_column(sql->sa, nn, c->base.name, &c->type, CARD_MULTI, c->null, 0);

			append(exps, oe);
			append(exps, ne);
		}
	}
	r = rel_table_func(sql->sa, NULL, NULL, exps, 2);
	r->l = ti;
		
	/* put single table into the stack with 2 names, needed for the psm code */
	if(!stack_push_rel_view(sql, on, r) || !stack_push_rel_view(sql, nn, rel_dup(r)))
		return 0;
	return 1;
}

static int
sql_update_triggers(backend *be, sql_table *t, stmt *tids, stmt **updates, int time )
{
	mvc *sql = be->mvc;
	node *n;
	int res = 1;

	if (!t->triggers.set)
		return res;

	for (n = t->triggers.set->h; n; n = n->next) {
		sql_trigger *trigger = n->data;

		if(!stack_push_frame(sql, "OLD-NEW"))
			return 0;
		if (trigger->event == 2 && trigger->time == time) {
			/* add name for the 'inserted' to the stack */
			const char *n = trigger->new_name;
			const char *o = trigger->old_name;

			if (!n) n = "new"; 
			if (!o) o = "old"; 

			if(!sql_stack_add_updated(sql, o, n, t, tids, updates)) {
				stack_pop_frame(sql);
				return 0;
			}

			if (!sql_parse(be, sql->sa, trigger->statement, m_instantiate)) {
				stack_pop_frame(sql);
				return 0;
			}
		}
		stack_pop_frame(sql);
	}
	return res;
}

static void
sql_update_check_null(backend *be, sql_table *t, stmt **updates)
{
	mvc *sql = be->mvc;
	node *n;
	sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if (updates[c->colnr] && !c->null) {
			stmt *s = updates[c->colnr];
			char *msg = NULL;

			if (!(s->key && s->nrcols == 0)) {
				s = stmt_selectnil(be, updates[c->colnr]);
				s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
			} else {
				sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->type, NULL, F_FUNC);

				s = stmt_unop(be, updates[c->colnr], isnil);
			}
			msg = sa_message(sql->sa, "UPDATE: NOT NULL constraint violated for column '%s.%s'", c->t->base.name, c->base.name);
			(void)stmt_exception(be, s, msg, 00001);
		}
	}
}

/* updates: an array of table width, per column holds the values for the to be updated rows  */
static list *
sql_update(backend *be, sql_table *t, stmt *rows, stmt **updates)
{
	mvc *sql = be->mvc;
	list *idx_updates = NULL;
	int i, nr_cols = list_length(t->columns.set);
	list *l = sa_list(sql->sa);
	node *n;

	if (!be->first_statement_generated)
		sql_update_check_null(be, (be->cur_append && t->p) ? t->p : t, updates);

	/* check keys + get idx */
	idx_updates = update_idxs_and_check_keys(be, t, rows, updates, l, NULL);
	if (!idx_updates) {
		assert(0);
		return sql_error(sql, 02, SQLSTATE(42000) "UPDATE: failed to update indexes for table '%s'", t->base.name);
	}

/* before */
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, rows, updates, 0))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_update_triggers(be, t, rows, updates, 0)) 
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

/* apply updates */
	for (i = 0, n = t->columns.set->h; i < nr_cols && n; i++, n = n->next) { 
		sql_column *c = n->data;

		if (updates[i])
	       		append(l, stmt_update_col(be, c, rows, updates[i]));
	}
	if (cascade_updates(be, t, rows, updates))
		return sql_error(sql, 02, SQLSTATE(42000) "UPDATE: cascade failed for table '%s'", t->base.name);

/* after */
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, rows, updates, 1))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_update_triggers(be, t, rows, updates, 1)) 
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

/* cascade ?? */
	return l;
}

/* updates with empty list is alter with create idx or keys */
static stmt *
rel2bin_update(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	stmt *update = NULL, **updates = NULL, *tids, *s, *ddl = NULL, *pup = NULL, *cnt;
	list *l = sa_list(sql->sa);
	int nr_cols, updcol, idx_ups = 0;
	node *m;
	sql_rel *tr = rel->l, *prel = rel->r;
	sql_table *t = NULL;

	if ((rel->flag&UPD_COMP)) {  /* special case ! */
		idx_ups = 1;
		prel = rel->l;
		rel = rel->r;
		tr = rel->l;
	}
	if (tr->op == op_basetable) {
		t = tr->l;
	} else {
		ddl = subrel_bin(be, tr, refs);
		if (!ddl)
			return NULL;
		t = rel_ddl_table_get(tr);

		/* no columns to update (probably an new pkey!) */
		if (!rel->exps) 
			return ddl;
	}

	if (rel->r) /* first construct the update relation */
		update = subrel_bin(be, rel->r, refs);

	if (!update)
		return NULL;

	if (idx_ups)
		pup = refs_find_rel(refs, prel);

	updates = table_update_stmts(sql, t, &nr_cols);
	tids = update->op4.lval->h->data;

	/* lookup the updates */
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_column *c = find_sql_column(t, exp_name(ce));

		if (c) 
			updates[c->colnr] = bin_find_column(be, update, ce->l, ce->r);
	}
	if (!be->first_statement_generated)
		sql_update_check_null(be, (be->cur_append && t->p) ? t->p : t, updates);

	/* check keys + get idx */
	updcol = first_updated_col(updates, list_length(t->columns.set));
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_idx *i = find_sql_idx(t, exp_name(ce)+1);
		stmt *update_idx, *is = NULL;

		if (i) {
			if (non_updatable_index(i->type)) /* Some indexes don't hold delta structures */
				continue;

			update_idx = bin_find_column(be, update, ce->l, ce->r);
			if (update_idx)
				is = update_idx;
			if (hash_index(i->type) && list_length(i->columns) <= 1) {
				is = NULL;
				update_idx = NULL;
			}
			if (i->key) 
				sql_update_check_key(be, (updcol>=0)?updates:NULL, i->key, tids, update_idx, updcol, l, pup);
			if (is) 
				list_append(l, stmt_update_idx(be,  i, tids, is));
		}
	}

/* before */
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, tids, updates, 0))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_update_triggers(be, t, tids, updates, 0)) 
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

/* apply the update */
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_column *c = find_sql_column(t, exp_name(ce));

		if (c) 
			append(l, stmt_update_col(be,  c, tids, updates[c->colnr]));
	}

	if (cascade_updates(be, t, tids, updates))
		return sql_error(sql, 02, SQLSTATE(42000) "UPDATE: cascade failed for table '%s'", t->base.name);

/* after */
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_update_triggers(be, up, tids, updates, 1))
				return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_update_triggers(be, t, tids, updates, 1)) 
		return sql_error(sql, 02, SQLSTATE(27000) "UPDATE: triggers failed for table '%s'", t->base.name);

	if (ddl) {
		list_prepend(l, ddl);
		cnt = stmt_list(be, l);
	} else {
		s = stmt_aggr(be, tids, NULL, NULL, sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
		cnt = s;
	}

	if (be->cur_append) /* building the total number of rows affected across all tables */
		cnt->nr = add_to_merge_partitions_accumulator(be, cnt->nr);

	if (sql->cascade_action) 
		sql->cascade_action = NULL;
	return cnt;
}

static int
sql_stack_add_deleted(mvc *sql, const char *name, sql_table *t, stmt *tids, int type)
{
	/* Put single relation of updates and old values on to the stack */
	sql_rel *r = NULL;
	node *n;
	list *exps = sa_list(sql->sa);
	trigger_input *ti = SA_NEW(sql->sa, trigger_input);

	ti->t = t;
	ti->tids = tids;
	ti->updates = NULL;
	ti->type = type;
	ti->nn = name;
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		sql_exp *ne = exp_column(sql->sa, name, c->base.name, &c->type, CARD_MULTI, c->null, 0);

		append(exps, ne);
	}
	r = rel_table_func(sql->sa, NULL, NULL, exps, 2);
	r->l = ti;

	return stack_push_rel_view(sql, name, r) ? 1 : 0;
}

static int
sql_delete_triggers(backend *be, sql_table *t, stmt *tids, int time, int firing_type, int internal_type)
{
	mvc *sql = be->mvc;
	node *n;
	int res = 1;

	if (!t->triggers.set)
		return res;

	for (n = t->triggers.set->h; n; n = n->next) {
		sql_trigger *trigger = n->data;

		if(!stack_push_frame(sql, "OLD-NEW"))
			return 0;
		if (trigger->event == firing_type && trigger->time == time) {
			/* add name for the 'deleted' to the stack */
			const char *o = trigger->old_name;

			if (!o) o = "old";

			if(!sql_stack_add_deleted(sql, o, t, tids, internal_type)) {
				stack_pop_frame(sql);
				return 0;
			}

			if (!sql_parse(be, sql->sa, trigger->statement, m_instantiate)) {
				stack_pop_frame(sql);
				return 0;
			}
		}
		stack_pop_frame(sql);
	}
	return res;
}

static stmt * sql_delete(backend *be, sql_table *t, stmt *rows);

static stmt *
sql_delete_cascade_Fkeys(backend *be, sql_key *fk, stmt *ftids)
{
	sql_table *t = mvc_bind_table(be->mvc, fk->t->s, fk->t->base.name);
	return sql_delete(be, t, ftids);
}

static void 
sql_delete_ukey(backend *be, stmt *utids /* deleted tids from ukey table */, sql_key *k, list *l, char* which, int cascade)
{
	mvc *sql = be->mvc;
	sql_ukey *uk = (sql_ukey*)k;

	if (uk->keys && list_length(uk->keys) > 0) {
		sql_subtype *lng = sql_bind_localtype("lng");
		sql_subtype *bt = sql_bind_localtype("bit");
		node *n;
		for(n = uk->keys->h; n; n = n->next) {
			char *msg = NULL;
			sql_subfunc *cnt = sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR);
			sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", F_FUNC, bt, 2, lng, lng);
			sql_key *fk = n->data;
			stmt *s, *tids;

			tids = stmt_tid(be, fk->idx->t, 0);
			s = stmt_idx(be, fk->idx, tids);
			s = stmt_join(be, s, utids, 0, cmp_equal); /* join over the join index */
			s = stmt_result(be, s, 0);
			tids = stmt_project(be, s, tids);
			if(cascade) { /* for truncate statements with the cascade option */
				s = sql_delete_cascade_Fkeys(be, fk, tids);
				list_prepend(l, s);
			} else {
				switch (((sql_fkey*)fk)->on_delete) {
					case ACT_NO_ACTION:
						break;
					case ACT_SET_NULL:
					case ACT_SET_DEFAULT:
						s = sql_delete_set_Fkeys(be, fk, tids, ((sql_fkey*)fk)->on_delete);
						list_prepend(l, s);
						break;
					case ACT_CASCADE:
						s = sql_delete_cascade_Fkeys(be, fk, tids);
						list_prepend(l, s);
						break;
					default:	/*RESTRICT*/
						/* The overlap between deleted primaries and foreign should be empty */
						s = stmt_binop(be, stmt_aggr(be, tids, NULL, NULL, cnt, 1, 0, 1), stmt_atom_lng(be, 0), ne);
						msg = sa_message(sql->sa, "%s: FOREIGN KEY constraint '%s.%s' violated", which, fk->t->base.name, fk->base.name);
						s = stmt_exception(be, s, msg, 00001);
						list_prepend(l, s);
				}
			}
		}
	}
}

static int
sql_delete_keys(backend *be, sql_table *t, stmt *rows, list *l, char* which, int cascade)
{
	mvc *sql = be->mvc;
	int res = 1;
	node *n;

	if (!t->keys.set)
		return res;

	for (n = t->keys.set->h; n; n = n->next) {
		sql_key *k = n->data;

		if (k->type == pkey || k->type == ukey) {
			if (!(sql->cascade_action && list_find_id(sql->cascade_action, k->base.id))) {
				int *local_id = SA_NEW(sql->sa, int);
				if (!sql->cascade_action) 
					sql->cascade_action = sa_list(sql->sa);
				
				*local_id = k->base.id;
				list_append(sql->cascade_action, local_id); 
				sql_delete_ukey(be, rows, k, l, which, cascade);
			}
		}
	}
	return res;
}

static stmt *
sql_delete(backend *be, sql_table *t, stmt *rows)
{
	mvc *sql = be->mvc;
	stmt *v = NULL, *s = NULL;
	list *l = sa_list(sql->sa);

	if (rows) {
		v = rows;
	} else { /* delete all */
		v = stmt_tid(be, t, 0);
	}

/* before */
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_delete_triggers(be, up, v, 0, 1, 3))
				return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_delete_triggers(be, t, v, 0, 1, 3))
		return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", t->base.name);

	if (!sql_delete_keys(be, t, v, l, "DELETE", 0))
		return sql_error(sql, 02, SQLSTATE(42000) "DELETE: failed to delete indexes for table '%s'", t->base.name);

	if (rows) { 
		sql_subtype to;

		sql_find_subtype(&to, "oid", 0, 0);
		list_append(l, stmt_delete(be, t, rows));
	} else { /* delete all */
		/* first column */
		s = stmt_table_clear(be, t);
		list_append(l, s);
	}

/* after */
	if (be->cur_append && !be->first_statement_generated) {
		for(sql_table *up = t->p ; up ; up = up->p) {
			if (!sql_delete_triggers(be, up, v, 1, 1, 3))
				return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", up->base.name);
		}
	}
	if (!sql_delete_triggers(be, t, v, 1, 1, 3))
		return sql_error(sql, 02, SQLSTATE(27000) "DELETE: triggers failed for table '%s'", t->base.name);
	if (rows)
		s = stmt_aggr(be, rows, NULL, NULL, sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
	if (be->cur_append) /* building the total number of rows affected across all tables */
		s->nr = add_to_merge_partitions_accumulator(be, s->nr);
	return s;
}

static stmt *
rel2bin_delete(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	stmt *rows = NULL, *stdelete = NULL;
	sql_rel *tr = rel->l;
	sql_table *t = NULL;

	if (tr->op == op_basetable)
		t = tr->l;
	else
		assert(0/*ddl statement*/);

	if (rel->r) { /* first construct the deletes relation */
		rows = subrel_bin(be, rel->r, refs);
		if (!rows)
			return NULL;
	}
	if (rows && rows->type == st_list) {
		stmt *s = rows;
		rows = s->op4.lval->h->data;
	}
	stdelete = sql_delete(be, t, rows);
	if (sql->cascade_action)
		sql->cascade_action = NULL;
	return stdelete;
}

struct tablelist {
	sql_table *table;
	struct tablelist* next;
};

static void /* inspect the other tables recursively for foreign key dependencies */
check_for_foreign_key_references(mvc *sql, struct tablelist* list, struct tablelist* next_append, sql_table *t, int cascade, int *error) {
	node *n;
	int found;
	struct tablelist* new_node, *node_check;

	if (*error)
		return;

	if (t->keys.set) { /* Check for foreign key references */
		for (n = t->keys.set->h; n; n = n->next) {
			sql_key *k = n->data;

			if (k->type == ukey || k->type == pkey) {
				sql_ukey *uk = (sql_ukey *) k;

				if (uk->keys && list_length(uk->keys)) {
					node *l = uk->keys->h;

					for (; l; l = l->next) {
						k = l->data;
						/* make sure it is not a self referencing key */
						if (k->t != t && !cascade) {
							node *n = t->columns.set->h;
							sql_column *c = n->data;
							size_t n_rows = store_funcs.count_col(sql->session->tr, c, 1);
							size_t n_deletes = store_funcs.count_del(sql->session->tr, c->t);
							assert (n_rows >= n_deletes);
							if(n_rows - n_deletes > 0) {
								sql_error(sql, 02, SQLSTATE(23000) "TRUNCATE: FOREIGN KEY %s.%s depends on %s", k->t->base.name, k->base.name, t->base.name);
								*error = 1;
								return;
							}
						} else if (k->t != t) {
							found = 0;
							for (node_check = list; node_check; node_check = node_check->next) {
								if (node_check->table == k->t)
									found = 1;
							}
							if (!found) {
								if ((new_node = MNEW(struct tablelist)) == NULL) {
									sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
									*error = 1;
									return;
								}
								new_node->table = k->t;
								new_node->next = NULL;
								next_append->next = new_node;
								check_for_foreign_key_references(sql, list, new_node, k->t, cascade, error);
							}
						}
					}
				}
			}
		}
	}
}

static stmt *
sql_truncate(backend *be, sql_table *t, int restart_sequences, int cascade)
{
	mvc *sql = be->mvc;
	list *l = sa_list(sql->sa);
	stmt *v, *ret = NULL, *other = NULL;
	const char *next_value_for = "next value for \"sys\".\"seq_";
	char *seq_name = NULL;
	str seq_pos = NULL;
	sql_column *col = NULL;
	sql_sequence *seq = NULL;
	sql_schema *sche = NULL;
	sql_table *next = NULL;
	sql_trans *tr = sql->session->tr;
	node *n = NULL;
	int error = 0;
	struct tablelist* new_list = MNEW(struct tablelist), *list_node, *aux;

	if (!new_list) {
		sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
		error = 1;
		goto finalize;
	}

	new_list->table = t;
	new_list->next = NULL;
	check_for_foreign_key_references(sql, new_list, new_list, t, cascade, &error);
	if (error)
		goto finalize;

	for (list_node = new_list; list_node; list_node = list_node->next) {
		next = list_node->table;
		sche = next->s;

		if (restart_sequences) { /* restart the sequences if it's the case */
			for (n = next->columns.set->h; n; n = n->next) {
				col = n->data;
				if (col->def && (seq_pos = strstr(col->def, next_value_for))) {
					seq_name = _STRDUP(seq_pos + (strlen(next_value_for) - strlen("seq_")));
					if (!seq_name) {
						sql_error(sql, 02, SQLSTATE(HY013) MAL_MALLOC_FAIL);
						error = 1;
						goto finalize;
					}
					seq_name[strlen(seq_name)-1] = '\0';
					seq = find_sql_sequence(sche, seq_name);
					if (seq) {
						if (!sql_trans_sequence_restart(tr, seq, seq->start)) {
							sql_error(sql, 02, SQLSTATE(HY005) "Could not restart sequence %s.%s", sche->base.name, seq_name);
							error = 1;
							goto finalize;
						}
						seq->base.wtime = sche->base.wtime = tr->wtime = tr->wstime;
						tr->schema_updates++;
					}
					_DELETE(seq_name);
				}
			}
		}

		v = stmt_tid(be, next, 0);

		/* before */
		if (be->cur_append && !be->first_statement_generated) {
			for (sql_table *up = t->p ; up ; up = up->p) {
				if (!sql_delete_triggers(be, up, v, 0, 3, 4)) {
					sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", up->base.name);
					error = 1;
					goto finalize;
				}
			}
		}
		if (!sql_delete_triggers(be, next, v, 0, 3, 4)) {
			sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", next->base.name);
			error = 1;
			goto finalize;
		}

		if (!sql_delete_keys(be, next, v, l, "TRUNCATE", cascade)) {
			sql_error(sql, 02, SQLSTATE(42000) "TRUNCATE: failed to delete indexes for table '%s'", next->base.name);
			error = 1;
			goto finalize;
		}

		other = stmt_table_clear(be, next);
		list_append(l, other);
		if (next == t)
			ret = other;

		/* after */
		if (be->cur_append && !be->first_statement_generated) {
			for (sql_table *up = t->p ; up ; up = up->p) {
				if (!sql_delete_triggers(be, up, v, 1, 3, 4)) {
					sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", up->base.name);
					error = 1;
					goto finalize;
				}
			}
		}
		if (!sql_delete_triggers(be, next, v, 1, 3, 4)) {
			sql_error(sql, 02, SQLSTATE(27000) "TRUNCATE: triggers failed for table '%s'", next->base.name);
			error = 1;
			goto finalize;
		}

		if (be->cur_append) /* building the total number of rows affected across all tables */
			other->nr = add_to_merge_partitions_accumulator(be, other->nr);
	}

finalize:
	for (list_node = new_list; list_node;) {
		aux = list_node->next;
		_DELETE(list_node);
		list_node = aux;
	}

	if (error)
		return NULL;
	return ret;
}

#define E_ATOM_INT(e) ((atom*)((sql_exp*)e)->l)->data.val.ival
#define E_ATOM_STRING(e) ((atom*)((sql_exp*)e)->l)->data.val.sval

static stmt *
rel2bin_truncate(backend *be, sql_rel *rel)
{
	mvc *sql = be->mvc;
	stmt *truncate = NULL;
	sql_rel *tr = rel->l;
	sql_table *t = NULL;
	node *n = NULL;
	int restart_sequences, cascade;

	if (tr->op == op_basetable)
		t = tr->l;
	else
		assert(0/*ddl statement*/);

	n = rel->exps->h;
	restart_sequences = E_ATOM_INT(n->data);
	cascade = E_ATOM_INT(n->next->data);

	truncate = sql_truncate(be, t, restart_sequences, cascade);
	if (sql->cascade_action)
		sql->cascade_action = NULL;
	return truncate;
}

static stmt *
rel2bin_output(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	node *n;
	const char *tsep, *rsep, *ssep, *ns;
	const char *fn   = NULL;
	int onclient = 0;
	stmt *s = NULL, *fns = NULL;
	list *slist = sa_list(sql->sa);

	if (rel->l)  /* first construct the sub relation */
		s = subrel_bin(be, rel->l, refs);
	if (!s) 
		return NULL;	

	if (!rel->exps) 
		return s;
	n = rel->exps->h;
	tsep = sa_strdup(sql->sa, E_ATOM_STRING(n->data));
	rsep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->data));
	ssep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->data));
	ns   = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->next->data));

	if (n->next->next->next->next) {
		fn = E_ATOM_STRING(n->next->next->next->next->data);
		fns = stmt_atom_string(be, sa_strdup(sql->sa, fn));
		onclient = E_ATOM_INT(n->next->next->next->next->next->data);
	}
	list_append(slist, stmt_export(be, s, tsep, rsep, ssep, ns, onclient, fns));
	if (s->type == st_list && ((stmt*)s->op4.lval->h->data)->nrcols != 0) {
		stmt *cnt = stmt_aggr(be, s->op4.lval->h->data, NULL, NULL, sql_bind_func(sql->sa, sql->session->schema, "count", sql_bind_localtype("void"), NULL, F_AGGR), 1, 0, 1);
		return cnt;
	} else {
		return stmt_atom_lng(be, 1);
	}
}

static stmt *
rel2bin_list(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	stmt *l = NULL, *r = NULL;
	list *slist = sa_list(sql->sa);

	(void)refs;

	if (find_prop(rel->p, PROP_DISTRIBUTE) && be->cur_append == 0) /* create affected rows accumulator */
		create_merge_partitions_accumulator(be);

	if (rel->l)  /* first construct the sub relation */
		l = subrel_bin(be, rel->l, refs);
	if (rel->r)  /* first construct the sub relation */
		r = subrel_bin(be, rel->r, refs);
	if (!l || !r)
		return NULL;
	list_append(slist, l);
	list_append(slist, r);
	return stmt_list(be, slist);
}

static stmt *
rel2bin_psm(backend *be, sql_rel *rel) 
{
	mvc *sql = be->mvc;
	node *n;
	list *l = sa_list(sql->sa);
	stmt *sub = NULL;

	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *s = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL);
		if(!s)
			return NULL;

		if (s && s->type == st_table) /* relational statement */
			sub = s->op1;
		else
			append(l, s);
	}
	return stmt_list(be, l);
}

static stmt *
rel2bin_partition_limits(backend *be, sql_rel *rel, list *refs)
{
	stmt *l = NULL, *r = NULL;
	node *n = NULL;
	list *slist = sa_list(be->mvc->sa);

	if (rel->l)  /* first construct the sub relation */
		l = subrel_bin(be, rel->l, refs);
	if (rel->r)  /* first construct the sub relation */
		r = subrel_bin(be, rel->r, refs);

	assert(rel->exps);
	assert(rel->flag == ddl_alter_table_add_range_partition || rel->flag == ddl_alter_table_add_list_partition);

	if (rel->exps) {
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *s = exp_bin(be, e, l, r, NULL, NULL, NULL, NULL);
			append(slist, s);
		}
	}
	return stmt_catalog(be, rel->flag, stmt_list(be, slist));
}

static stmt *
rel2bin_exception(backend *be, sql_rel *rel, list *refs)
{
	stmt *l = NULL, *r = NULL;
	node *n = NULL;
	list *slist = sa_list(be->mvc->sa);

	if (find_prop(rel->p, PROP_DISTRIBUTE) && be->cur_append == 0) /* create affected rows accumulator */
		create_merge_partitions_accumulator(be);

	if (rel->l)  /* first construct the sub relation */
		l = subrel_bin(be, rel->l, refs);
    if (rel->r)  /* first construct the sub relation */
		r = subrel_bin(be, rel->r, refs);

	if (rel->exps) {
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *s = exp_bin(be, e, l, r, NULL, NULL, NULL, NULL);
			append(slist, s);
		}
	} else { /* if there is no exception condition, just generate a statement list */
		list_append(slist, l);
		list_append(slist, r);
	}
	return stmt_list(be, slist);
}

static stmt *
rel2bin_seq(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	node *en = rel->exps->h;
	stmt *restart, *sname, *seq, *seqname, *sl = NULL;
	list *l = sa_list(sql->sa);

	if (rel->l) { /* first construct the sub relation */
		sl = subrel_bin(be, rel->l, refs);
		if(!sl)
			return NULL;
	}

	restart = exp_bin(be, en->data, sl, NULL, NULL, NULL, NULL, NULL);
	sname = exp_bin(be, en->next->data, sl, NULL, NULL, NULL, NULL, NULL);
	seqname = exp_bin(be, en->next->next->data, sl, NULL, NULL, NULL, NULL, NULL);
	seq = exp_bin(be, en->next->next->next->data, sl, NULL, NULL, NULL, NULL, NULL);
	if (!restart || !sname || !seqname || !seq)
		return NULL;

	(void)refs;
	append(l, sname);
	append(l, seqname);
	append(l, seq);
	append(l, restart);
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_trans(backend *be, sql_rel *rel, list *refs) 
{
	node *en = rel->exps->h;
	stmt *chain = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	stmt *name = NULL;

	if (!chain)
		return NULL;

	(void)refs;
	if (en->next) {
		name = exp_bin(be, en->next->data, NULL, NULL, NULL, NULL, NULL, NULL);
		if (!name)
			return NULL;
	}
	return stmt_trans(be, rel->flag, chain, name);
}

static stmt *
rel2bin_catalog(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	node *en = rel->exps->h;
	stmt *action = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	stmt *sname = NULL, *name = NULL, *ifexists = NULL;
	list *l = sa_list(sql->sa);

	if (!action)
		return NULL;

	(void)refs;
	en = en->next;
	sname = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	if (!sname)
		return NULL;
	if (en->next) {
		name = exp_bin(be, en->next->data, NULL, NULL, NULL, NULL, NULL, NULL);
		if (!name)
			return NULL;
	} else {
		name = stmt_atom_string_nil(be);
	}
	if (en->next && en->next->next) {
		ifexists = exp_bin(be, en->next->next->data, NULL, NULL, NULL, NULL, NULL, NULL);
		if (!ifexists)
			return NULL;
	} else {
		ifexists = stmt_atom_int(be, 0);
	}
	append(l, sname);
	append(l, name);
	append(l, ifexists);
	append(l, action);
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_catalog_table(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	node *en = rel->exps->h;
	stmt *action = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	stmt *table = NULL, *sname, *tname = NULL, *ifexists = NULL;
	list *l = sa_list(sql->sa);

	if (!action)
		return NULL;

	(void)refs;
	en = en->next;
	sname = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	if (!sname)
		return NULL;
	en = en->next;
	if (en) {
		tname = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
		if (!tname)
			return NULL;
		en = en->next;
	}
	append(l, sname);
	assert(tname);
	append(l, tname);
	if (rel->flag != ddl_drop_table && rel->flag != ddl_drop_view && rel->flag != ddl_drop_constraint) {
		if (en) {
			table = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
			if (!table)
				return NULL;
		}
		append(l, table);
	} else {
		if (en) {
			ifexists = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
			if (!ifexists)
				return NULL;
		} else {
			ifexists = stmt_atom_int(be, 0);
		}
		append(l, ifexists);
	}
	append(l, action);
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_catalog2(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	node *en;
	list *l = sa_list(sql->sa);

	(void)refs;
	for (en = rel->exps->h; en; en = en->next) {
		stmt *es = NULL;

		if (en->data) {
			es = exp_bin(be, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
			if (!es) 
				return NULL;
		} else {
			es = stmt_atom_string_nil(be);
		}
		append(l,es);
	}
	return stmt_catalog(be, rel->flag, stmt_list(be, l));
}

static stmt *
rel2bin_ddl(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	stmt *s = NULL;

	switch (rel->flag) {
		case ddl_output:
			s = rel2bin_output(be, rel, refs);
			sql->type = Q_TABLE;
			break;
		case ddl_list:
			s = rel2bin_list(be, rel, refs);
			break;
		case ddl_psm:
			s = rel2bin_psm(be, rel);
			break;
		case ddl_exception:
			s = rel2bin_exception(be, rel, refs);
			sql->type = Q_UPDATE;
			break;
		case ddl_create_seq:
		case ddl_alter_seq:
			s = rel2bin_seq(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_alter_table_add_range_partition:
		case ddl_alter_table_add_list_partition:
			s = rel2bin_partition_limits(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_release:
		case ddl_commit:
		case ddl_rollback:
		case ddl_trans:
			s = rel2bin_trans(be, rel, refs);
			sql->type = Q_TRANS;
			break;
		case ddl_create_schema:
		case ddl_drop_schema:
			s = rel2bin_catalog(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_create_table:
		case ddl_drop_table:
		case ddl_create_view:
		case ddl_drop_view:
		case ddl_drop_constraint:
		case ddl_alter_table:
			s = rel2bin_catalog_table(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		case ddl_drop_seq:
		case ddl_create_type:
		case ddl_drop_type:
		case ddl_drop_index:
		case ddl_create_function:
		case ddl_drop_function:
		case ddl_create_trigger:
		case ddl_drop_trigger:
		case ddl_grant_roles:
		case ddl_revoke_roles:
		case ddl_grant:
		case ddl_revoke:
		case ddl_grant_func:
		case ddl_revoke_func:
		case ddl_create_user:
		case ddl_drop_user:
		case ddl_alter_user:
		case ddl_rename_user:
		case ddl_create_role:
		case ddl_drop_role:
		case ddl_alter_table_add_table:
		case ddl_alter_table_del_table:
		case ddl_alter_table_set_access:
		case ddl_comment_on:
		case ddl_rename_schema:
		case ddl_rename_table:
		case ddl_rename_column:
			s = rel2bin_catalog2(be, rel, refs);
			sql->type = Q_SCHEMA;
			break;
		default:
			assert(0);
	}
	return s;
}

static stmt *
subrel_bin(backend *be, sql_rel *rel, list *refs) 
{
	mvc *sql = be->mvc;
	stmt *s = NULL;

	if (THRhighwater())
		return sql_error(be->mvc, 10, SQLSTATE(42000) "Query too complex: running out of stack space");;

	if (!rel)
		return s;
	if (rel_is_ref(rel)) {
		s = refs_find_rel(refs, rel);
		/* needs a proper fix!! */
		if (s)
			return s;
	}
	switch (rel->op) {
	case op_basetable:
		s = rel2bin_basetable(be, rel);
		sql->type = Q_TABLE;
		break;
	case op_table:
		s = rel2bin_table(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
		s = rel2bin_join(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_semi:
	case op_anti:
		s = rel2bin_semijoin(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_union: 
		s = rel2bin_union(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_except: 
		s = rel2bin_except(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_inter: 
		s = rel2bin_inter(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_project:
		s = rel2bin_project(be, rel, refs, NULL);
		sql->type = Q_TABLE;
		break;
	case op_select: 
		s = rel2bin_select(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_groupby: 
		s = rel2bin_groupby(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_topn: 
		s = rel2bin_topn(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_sample:
		s = rel2bin_sample(be, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_insert: 
		s = rel2bin_insert(be, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_update: 
		s = rel2bin_update(be, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_delete: 
		s = rel2bin_delete(be, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_truncate:
		s = rel2bin_truncate(be, rel);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_ddl:
		s = rel2bin_ddl(be, rel, refs);
		break;
	}
	if (s && rel_is_ref(rel)) {
		list_append(refs, rel);
		list_append(refs, s);
	}
	return s;
}

stmt *
rel_bin(backend *be, sql_rel *rel) 
{
	mvc *sql = be->mvc;
	list *refs = sa_list(sql->sa);
	mapi_query_t sqltype = sql->type;
	stmt *s = subrel_bin(be, rel, refs);

	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	return s;
}

stmt *
output_rel_bin(backend *be, sql_rel *rel ) 
{
	mvc *sql = be->mvc;
	list *refs = sa_list(sql->sa);
	mapi_query_t sqltype = sql->type;
	stmt *s;

	if (refs == NULL)
		return NULL;
	s = subrel_bin(be, rel, refs);
	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	if (!is_ddl(rel->op) && s && s->type != st_none && sql->type == Q_TABLE)
		s = stmt_output(be, s);
	if (sqltype == Q_UPDATE && s && (s->type != st_list || be->cur_append)) {
		if (be->cur_append) { /* finish the output bat */
			s->nr = be->cur_append;
			be->cur_append = 0;
			be->first_statement_generated = false;
		}
		s = stmt_affected_rows(be, s);
	}
	return s;
}
