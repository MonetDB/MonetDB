/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2016 MonetDB B.V.
 */

#include "monetdb_config.h"

#include "rel_bin.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_psm.h"
#include "rel_prop.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_optimizer.h"
#include "sql_env.h"

#define OUTER_ZERO 64

static stmt * subrel_bin(mvc *sql, sql_rel *rel, list *refs);

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

			fprintf(stderr, "%s.%s\n", rnme ? rnme : "(null!)", nme ? nme : "(null!)");
		}
	}
}

static stmt *
list_find_column(sql_allocator *sa, list *l, const char *rname, const char *name ) 
{
	stmt *res = NULL;
	node *n;

	if (!l)
		return NULL;
	MT_lock_set(&l->ht_lock);
	if (!l->ht && list_length(l) > HASH_MIN_SIZE) {
		l->ht = hash_new(l->sa, MAX(list_length(l), l->expected_cnt), (fkeyvalue)&stmt_key);

		for (n = l->h; n; n = n->next) {
			const char *nme = column_name(sa, n->data);
			int key = hash_key(nme);

			hash_add(l->ht, key, n->data);
		}
	}
	if (l->ht) {
		int key = hash_key(name);
		sql_hash_e *e = l->ht->buckets[key&(l->ht->size-1)];

		if (rname) {
			for (; e; e = e->chain) {
				stmt *s = e->value;
				const char *rnme = table_name(sa, s);
				const char *nme = column_name(sa, s);

				if (rnme && strcmp(rnme, rname) == 0 &&
		 	            strcmp(nme, name) == 0) {
					res = s;
					break;
				}
			}
		} else {
			for (; e; e = e->chain) {
				stmt *s = e->value;
				const char *nme = column_name(sa, s);

				if (nme && strcmp(nme, name) == 0) {
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
			const char *rnme = table_name(sa, n->data);
			const char *nme = column_name(sa, n->data);

			if (rnme && strcmp(rnme, rname) == 0 && 
				    strcmp(nme, name) == 0) {
				res = n->data;
				break;
			}
		}
	} else {
		for (n = l->h; n; n = n->next) {
			const char *nme = column_name(sa, n->data);

			if (nme && strcmp(nme, name) == 0) {
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
bin_find_column( sql_allocator *sa, stmt *sub, const char *rname, const char *name ) 
{
	return list_find_column( sa, sub->op4.lval, rname, name);
}

static list *
bin_find_columns(mvc *sql, stmt *sub, const char *name ) 
{
	node *n;
	list *l = sa_list(sql->sa);

	for (n = sub->op4.lval->h; n; n = n->next) {
		const char *nme = column_name(sql->sa, n->data);

		if (strcmp(nme, name) == 0) 
			append(l, n->data);
	}
	if (list_length(l)) 
		return l;
	return NULL;
}

static stmt *column(sql_allocator *sa, stmt *val )
{
	if (val->nrcols == 0)
		return const_column(sa, val);
	return val;
}

static stmt *Column(sql_allocator *sa, stmt *val )
{
	if (val->nrcols == 0)
		val = const_column(sa, val);
	return stmt_append(sa, stmt_temp(sa, tail_type(val)), val);
}

static stmt *
bin_first_column(sql_allocator *sa, stmt *sub ) 
{
	node *n = sub->op4.lval->h;
	stmt *c = n->data;

	if (c->nrcols == 0)
		return const_column(sa, c);
	return c;
}

static stmt *
row2cols(mvc *sql, stmt *sub)
{
	if (sub->nrcols == 0 && sub->key) {
		node *n; 
		list *l = sa_list(sql->sa);

		for (n = sub->op4.lval->h; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(sql->sa, sc);
			const char *tname = table_name(sql->sa, sc);

			sc = column(sql->sa, sc);
			list_append(l, stmt_alias(sql->sa, sc, tname, cname));
		}
		sub = stmt_list(sql->sa, l);
	}
	return sub;
}

static stmt *
handle_in_exps( mvc *sql, sql_exp *ce, list *nl, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel, int in, int use_r) 
{
	node *n;
	stmt *s = NULL, *c = exp_bin(sql, ce, left, right, grp, ext, cnt, NULL);

	if (c->nrcols == 0) {
		sql_subtype *bt = sql_bind_localtype("bit");
		sql_subfunc *cmp = (in)
			?sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(c), tail_type(c), F_FUNC)
			:sql_bind_func(sql->sa, sql->session->schema, "!=", tail_type(c), tail_type(c), F_FUNC);
		sql_subfunc *a = (in)?sql_bind_func(sql->sa, sql->session->schema, "or", bt, bt, F_FUNC)
				     :sql_bind_func(sql->sa, sql->session->schema, "and", bt, bt, F_FUNC);

		for( n = nl->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *i = exp_bin(sql, use_r?e->r:e, left, right, grp, ext, cnt, NULL);
			
			i = stmt_binop(sql->sa, c, i, cmp); 
			if (s)
				s = stmt_binop(sql->sa, s, i, a);
			else
				s = i;

		}
		if (sel) 
			s = stmt_uselect(sql->sa, 
				stmt_const(sql->sa, bin_first_column(sql->sa, left), s), 
				stmt_bool(sql->sa, 1), cmp_equal, sel); 
	} else {
		comp_type cmp = (in)?cmp_equal:cmp_notequal;

		if (!in)
			s = sel;
		for( n = nl->h; n; n = n->next) {
			sql_exp *e = n->data;
			stmt *i = exp_bin(sql, use_r?e->r:e, left, right, grp, ext, cnt, NULL);
			
			if (in) { 
				i = stmt_uselect(sql->sa, c, i, cmp, sel); 
				if (s)
					s = stmt_tunion(sql->sa, s, i); 
				else
					s = i;
			} else {
				s = stmt_uselect(sql->sa, c, i, cmp, s); 
			}
		}
	}
	return s;
}

static stmt *
value_list( mvc *sql, list *vals) 
{
	node *n;
	stmt *s;

	/* create bat append values */
	s = stmt_temp(sql->sa, exp_subtype(vals->h->data));
	for( n = vals->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(sql, e, NULL, NULL, NULL, NULL, NULL, NULL);

		if (list_length(vals) == 1)
			return i;
		
		s = stmt_append(sql->sa, s, i);
	}
	return s;
}

static stmt *
exp_list( mvc *sql, list *exps, stmt *l, stmt *r, stmt *grp, stmt *ext, stmt *cnt, stmt *sel) 
{
	node *n;
	list *nl = sa_list(sql->sa);

	for( n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(sql, e, l, r, grp, ext, cnt, sel);
		
		if (n->next && i && i->type == st_table) /* relational statement */
			l = i->op1;
		else
			append(nl, i);
	}
	return stmt_list(sql->sa, nl);
}

stmt *
exp_bin(mvc *sql, sql_exp *e, stmt *left, stmt *right, stmt *grp, stmt *ext, stmt *cnt, stmt *sel) 
{
	stmt *s = NULL;

	if (!e) {
		assert(0);
		return NULL;
	}

	switch(e->type) {
	case e_psm:
		if (e->flag & PSM_SET) {
			stmt *r = exp_bin(sql, e->l, left, right, grp, ext, cnt, sel);
			return stmt_assign(sql->sa, e->name, r, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_VAR) {
			if (e->f) /* TODO TABLE */
				return stmt_vars(sql->sa, e->name, e->f, 1, GET_PSM_LEVEL(e->flag));
			else
				return stmt_var(sql->sa, e->name, &e->tpe, 1, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_RETURN) {
			sql_exp *l = e->l;
			stmt *r = exp_bin(sql, l, left, right, grp, ext, cnt, sel);

			/* handle table returning functions */
			if (l->type == e_psm && l->flag & PSM_REL) {
				stmt *lst = r->op1;
				if (r->type == st_table && lst->nrcols == 0 && lst->key) {
					node *n;
					list *l = sa_list(sql->sa);

					for(n=lst->op4.lval->h; n; n = n->next)
						list_append(l, const_column(sql->sa, (stmt*)n->data));
					r = stmt_list(sql->sa, l);
				}
				if (r->type == st_list)
					r = stmt_table(sql->sa, r, 1);
			}
			return stmt_return(sql->sa, r, GET_PSM_LEVEL(e->flag));
		} else if (e->flag & PSM_WHILE) {
			stmt *cond = exp_bin(sql, e->l, left, right, grp, ext, cnt, sel);
			stmt *stmts = exp_list(sql, e->r, left, right, grp, ext, cnt, sel);
			return stmt_while(sql->sa, cond, stmts);
		} else if (e->flag & PSM_IF) {
			stmt *cond = exp_bin(sql, e->l, left, right, grp, cnt, ext, sel);
			stmt *stmts = exp_list(sql, e->r, left, right, grp, cnt, ext, sel);
			stmt *estmts = NULL;
			if (e->f)
				estmts = exp_list(sql, e->f, left, right, grp, ext, cnt, sel);
			return stmt_if(sql->sa, cond, stmts, estmts);
		} else if (e->flag & PSM_REL) {
			sql_rel *rel = e->l;
			stmt *r = rel_bin(sql, rel);

#if 0
			if (r->type == st_list && r->nrcols == 0 && r->key) {
				/* row to columns */
				node *n;
				list *l = sa_list(sql->sa);

				for(n=r->op4.lval->h; n; n = n->next)
					list_append(l, const_column(sql->sa, (stmt*)n->data));
				r = stmt_list(sql->sa, l);
			}
#endif
			if (is_modify(rel->op) || is_ddl(rel->op)) 
				return r;
			return stmt_table(sql->sa, r, 1);
		}
		break;
	case e_atom: {
		if (e->l) { 			/* literals */
			atom *a = e->l;
			s = stmt_atom(sql->sa, atom_dup(sql->sa, a));
		} else if (e->r) { 		/* parameters */
			s = stmt_var(sql->sa, sa_strdup(sql->sa, e->r), e->tpe.type?&e->tpe:NULL, 0, e->flag);
		} else if (e->f) { 		/* values */
			s = value_list(sql, e->f);
		} else { 			/* arguments */
			s = stmt_varnr(sql->sa, e->flag, e->tpe.type?&e->tpe:NULL);
		}
	}	break;
	case e_convert: {
		/* if input is type any NULL or column of nulls, change type */
		list *tps = e->r;
		sql_subtype *from = tps->h->data;
		sql_subtype *to = tps->h->next->data;
		stmt *l;

		if (from->type->localtype == 0) {
			l = stmt_atom(sql->sa, atom_general(sql->sa, to, NULL));
		} else {
	       		l = exp_bin(sql, e->l, left, right, grp, ext, cnt, sel);
		}
		if (!l) 
			return NULL;
		s = stmt_convert(sql->sa, l, from, to);
	} 	break;
	case e_func: {
		node *en;
		list *l = sa_list(sql->sa), *exps = e->l, *obe = e->r;
		sql_subfunc *f = e->f;
		stmt *orderby_val = NULL, *orderby_ids = NULL, *orderby_grp = NULL;

		if (!obe && exps) {
			int nrcols = 0;
			for (en = exps->h; en; en = en->next) {
				stmt *es;

				es = exp_bin(sql, en->data, left, right, grp, ext, cnt, sel);
				if (!es) 
					return NULL;
				if (es->nrcols > nrcols)
					nrcols = es->nrcols;
				if (es && es->nrcols <= 0 && left && has_side_effect(es)) 
					es = stmt_const(sql->sa, bin_first_column(sql->sa, left), es);
				list_append(l,es);
			}
			if (sel && strcmp(sql_func_mod(f->func), "calc") == 0 && nrcols && strcmp(sql_func_imp(f->func), "ifthenelse") != 0)
				list_append(l,sel);
		}
		/* Window expressions are handled differently.
		   ->l == group by expression list
		   ->r == order by expression list
		   If both lists are empty, we pass a single 
		 	column for the inner relation
		 */
		if (obe) {
			stmt *g = NULL, *grp = NULL, *ext = NULL, *cnt = NULL;
			stmt *orderby = NULL;
			stmt *col = NULL;
		
			assert(0);
			if (exps) {
				for (en = exps->h; en; en = en->next) {
					stmt *es;

					es = exp_bin(sql, en->data, left, right, NULL, NULL, NULL, sel);
					col = es;
					if (!es) 
						return NULL;
					g = stmt_group(sql->sa, es, grp, ext, cnt);
					grp = stmt_result(sql->sa, g, 0);
					ext = stmt_result(sql->sa, g, 1);
					cnt = stmt_result(sql->sa, g, 2);
				}
			}
			/* order on the group first */
			stmt_group_done(g);
			if (g) {
				orderby = stmt_order(sql->sa, grp, 1);
				orderby_val = stmt_result(sql->sa, orderby, 0);
				orderby_ids = stmt_result(sql->sa, orderby, 1);
				orderby_grp = stmt_result(sql->sa, orderby, 2);
			}
			for (en = obe->h; en; en = en->next) {
				sql_exp *orderbycole = en->data; 
				stmt *orderbycols = exp_bin(sql, orderbycole, left, right, NULL, NULL, NULL, sel); 

				if (!orderbycols) 
					return NULL;
				if (orderby_ids)
					orderby = stmt_reorder(sql->sa, orderbycols, is_ascending(orderbycole), orderby_ids, orderby_grp);
				else
					orderby = stmt_order(sql->sa, orderbycols, is_ascending(orderbycole));
				col = orderbycols;
				if (g)
					orderby_val = stmt_result(sql->sa, orderby, 0);
				else
					orderby_val = stmt_result(sql->sa, orderby, 2);
				orderby_ids = stmt_result(sql->sa, orderby, 1);
				orderby_grp = stmt_result(sql->sa, orderby, 2);
			}
			if (!orderby_val && left) 
				orderby_val = stmt_mirror(sql->sa, bin_first_column(sql->sa, left));
			if (!orderby_val)
				return NULL;
			list_append(l, orderby_val);
			if (!g && col) {
				list_append(l, orderby_ids);
				list_append(l, col);
			}
			if (g) {
				list_append(l, orderby_ids);
				list_append(l, grp);
				list_append(l, ext);
			}
		}
		/*
		if (strcmp(f->func->base.name, "identity") == 0) 
			s = stmt_mirror(sql->sa, l->h->data);
		else
		*/
		if (f->func->rel) 
			s = stmt_func(sql->sa, stmt_list(sql->sa, l), sa_strdup(sql->sa, f->func->base.name), f->func->rel, (f->func->type == F_UNION));
		else
			s = stmt_Nop(sql->sa, stmt_list(sql->sa, l), e->f); 
	} 	break;
	case e_aggr: {
		list *attr = e->l; 
		stmt *as = NULL;
		sql_subaggr *a = e->f;

		assert(sel == NULL);
		if (attr && attr->h) { 
			node *en;
			list *l = sa_list(sql->sa);

			for (en = attr->h; en; en = en->next) {
				sql_exp *at = en->data;

				as = exp_bin(sql, at, left, right, NULL, NULL, NULL, sel);

				if (as && as->nrcols <= 0 && left) 
					as = stmt_const(sql->sa, bin_first_column(sql->sa, left), as);
				/* insert single value into a column */
				if (as && as->nrcols <= 0 && !left)
					as = const_column(sql->sa, as);

				if (!as) 
					return NULL;	
				if (need_distinct(e)){ 
					stmt *g = stmt_group(sql->sa, as, grp, ext, cnt);
					stmt *next = stmt_result(sql->sa, g, 1); 
						
					as = stmt_project(sql->sa, next, as);
					if (grp)
						grp = stmt_project(sql->sa, next, grp);
					stmt_group_done(g);
				}
				append(l, as);
			}
			as = stmt_list(sql->sa, l);
		} else {
			/* count(*) may need the default group (relation) and
			   and/or an attribute to count */
			if (grp) {
				as = grp;
			} else if (left) {
				as = bin_first_column(sql->sa, left);
			} else {
				/* create dummy single value in a column */
				as = stmt_atom_lng(sql->sa, 0);
				as = const_column(sql->sa, as);
			}
		}
		s = stmt_aggr(sql->sa, as, grp, ext, a, 1, need_no_nil(e) /* ignore nil*/ );
		if (find_prop(e->p, PROP_COUNT)) /* propagate count == 0 ipv NULL in outer joins */
			s->flag |= OUTER_ZERO;
	} 	break;
	case e_column: {
		if (right) /* check relation names */
			s = bin_find_column(sql->sa, right, e->l, e->r);
		if (!s && left) 
			s = bin_find_column(sql->sa, left, e->l, e->r);
		if (s && grp)
			s = stmt_project(sql->sa, ext, s);
		if (!s && right) {
			fprintf(stderr, "could not find %s.%s\n", (char*)e->l, (char*)e->r);
			print_stmtlist(sql->sa, left);
			print_stmtlist(sql->sa, right);
		}
	 }	break;
	case e_cmp: {
		stmt *l = NULL, *r = NULL, *r2 = NULL;
		int swapped = 0, is_select = 0;
		sql_exp *re = e->r, *re2 = e->f;

		/* general predicate, select and join */
		if (get_cmp(e) == cmp_filter) {
			list *args;
			list *ops;
			node *n;
			int first = 1;

		       	ops = sa_list(sql->sa);
		       	args = e->l;
			for( n = args->h; n; n = n->next ) {
				s = NULL;
				if (!swapped)
					s = exp_bin(sql, n->data, left, NULL, grp, ext, cnt, NULL); 
				if (!s && (first || swapped)) {
					s = exp_bin(sql, n->data, right, NULL, grp, ext, cnt, NULL); 
					swapped = 1;
				}
				if (!s) 
					return s;
				if (s->nrcols == 0 && first)
					s = stmt_const(sql->sa, bin_first_column(sql->sa, swapped?right:left), s); 
				list_append(ops, s);
				first = 0;
			}
			l = stmt_list(sql->sa, ops);
		       	ops = sa_list(sql->sa);
			args = e->r;
			for( n = args->h; n; n = n->next ) {
				s = exp_bin(sql, n->data, (swapped || !right)?left:right, NULL, grp, ext, cnt, NULL); 
				if (!s) 
					return s;
				list_append(ops, s);
			}
			r = stmt_list(sql->sa, ops);

			if (left && right && exps_card(e->r) > CARD_ATOM) {
				sql_subfunc *f = e->f;
				stmt *j = stmt_genjoin(sql->sa, l, r, f, swapped);

				if (j && is_anti(e))
					j->flag |= ANTI;
				return j;
			}
			assert(!swapped);
			s = stmt_genselect(sql->sa, l, r, e->f, sel);
			if (s && is_anti(e))
				s->flag |= ANTI;
			return s;
		}
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			return handle_in_exps(sql, e->l, e->r, left, right, grp, ext, cnt, sel, (e->flag == cmp_in), 0);
		}
		if (e->flag == cmp_or && (!right || right->nrcols == 1)) {
			list *l = e->l;
			node *n;
			stmt *sel1 = NULL, *sel2 = NULL;

			sel1 = sel;
			sel2 = sel;
			for( n = l->h; n; n = n->next ) {
				s = exp_bin(sql, n->data, left, right, grp, ext, cnt, sel1); 
				if (!s) 
					return s;
				if (sel1 && sel1->nrcols == 0 && s->nrcols == 0) {
					sql_subtype *bt = sql_bind_localtype("bit");
					sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, "and", bt, bt, F_FUNC);
					assert(f);
					s = stmt_binop(sql->sa, sel1, s, f);
				}
				if (sel1 && sel1->nrcols && s->nrcols == 0) {
					stmt *predicate = bin_first_column(sql->sa, left);
				
					predicate = stmt_const(sql->sa, predicate, stmt_bool(sql->sa, 1));
					s = stmt_uselect(sql->sa, predicate, s, cmp_equal, sel1);
				}
				sel1 = s;
			}
			l = e->r;
			for( n = l->h; n; n = n->next ) {
				s = exp_bin(sql, n->data, left, right, grp, ext, cnt, sel2); 
				if (!s) 
					return s;
				if (sel2 && sel2->nrcols == 0 && s->nrcols == 0) {
					sql_subtype *bt = sql_bind_localtype("bit");
					sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, "and", bt, bt, F_FUNC);
					assert(f);
					s = stmt_binop(sql->sa, sel2, s, f);
				}
				if (sel2 && sel2->nrcols && s->nrcols == 0) {
					stmt *predicate = bin_first_column(sql->sa, left);
				
					predicate = stmt_const(sql->sa, predicate, stmt_bool(sql->sa, 1));
					s = stmt_uselect(sql->sa, predicate, s, cmp_equal, sel2);
				}
				sel2 = s;
			}
			if (sel1->nrcols == 0 && sel2->nrcols == 0) {
				sql_subtype *bt = sql_bind_localtype("bit");
				sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, "or", bt, bt, F_FUNC);
				assert(f);
				return stmt_binop(sql->sa, sel1, sel2, f);
			}
			if (sel1->nrcols == 0) {
				stmt *predicate = bin_first_column(sql->sa, left);
				
				predicate = stmt_const(sql->sa, predicate, stmt_bool(sql->sa, 1));
				sel1 = stmt_uselect(sql->sa, predicate, sel1, cmp_equal, NULL);
			}
			if (sel2->nrcols == 0) {
				stmt *predicate = bin_first_column(sql->sa, left);
				
				predicate = stmt_const(sql->sa, predicate, stmt_bool(sql->sa, 1));
				sel2 = stmt_uselect(sql->sa, predicate, sel2, cmp_equal, NULL);
			}
			return stmt_tunion(sql->sa, sel1, sel2);
		}
		if (e->flag == cmp_or && right) {  /* join */
			assert(0);
		}

		/* mark use of join indices */
		if (right && find_prop(e->p, PROP_JOINIDX) != NULL) 
			sql->opt_stats[0]++; 

		if (!l) {
			l = exp_bin(sql, e->l, left, NULL, grp, ext, cnt, sel);
			swapped = 0;
		}
		if (!l && right) {
 			l = exp_bin(sql, e->l, right, NULL, grp, ext, cnt, sel);
			swapped = 1;
		}
		if (swapped || !right)
 			r = exp_bin(sql, re, left, NULL, grp, ext, cnt, sel);
		else
 			r = exp_bin(sql, re, right, NULL, grp, ext, cnt, sel);
		if (!r && !swapped) {
 			r = exp_bin(sql, re, left, NULL, grp, ext, cnt, sel);
			is_select = 1;
		}
		if (!r && swapped) {
 			r = exp_bin(sql, re, right, NULL, grp, ext, cnt, sel);
			is_select = 1;
		}
		if (re2)
 			r2 = exp_bin(sql, re2, left, right, grp, ext, cnt, sel);

		if (!l || !r || (re2 && !r2)) {
			assert(0);
			return NULL;
		}

		if (left && right && !is_select &&
		   ((l->nrcols && (r->nrcols || (r2 && r2->nrcols))) || 
		     re->card > CARD_ATOM || 
		    (re2 && re2->card > CARD_ATOM))) {
			if (l->nrcols == 0)
				l = stmt_const(sql->sa, bin_first_column(sql->sa, swapped?right:left), l); 
			if (r->nrcols == 0)
				r = stmt_const(sql->sa, bin_first_column(sql->sa, swapped?left:right), r); 
			if (r2) {
				s = stmt_join2(sql->sa, l, r, r2, (comp_type)e->flag, swapped);
			} else if (swapped) {
				s = stmt_join(sql->sa, r, l, swap_compare((comp_type)e->flag));
			} else {
				s = stmt_join(sql->sa, l, r, (comp_type)e->flag);
			}
		} else {
			if (r2) {
				if (l->nrcols == 0 && r->nrcols == 0 && r2->nrcols == 0) {
					sql_subtype *bt = sql_bind_localtype("bit");
					sql_subfunc *lf = sql_bind_func(sql->sa, sql->session->schema,
							compare_func(range2lcompare(e->flag)),
							tail_type(l), tail_type(r), F_FUNC);
					sql_subfunc *rf = sql_bind_func(sql->sa, sql->session->schema,
							compare_func(range2rcompare(e->flag)),
							tail_type(l), tail_type(r), F_FUNC);
					sql_subfunc *a = sql_bind_func(sql->sa, sql->session->schema,
							"and", bt, bt, F_FUNC);
					assert(lf && rf && a);
					s = stmt_binop(sql->sa, 
						stmt_binop(sql->sa, l, r, lf), 
						stmt_binop(sql->sa, l, r2, rf), a);
				} else if (((e->flag&3) != 3) /* both sides closed use between implementation */ && l->nrcols > 0 && r->nrcols > 0 && r2->nrcols > 0) {
					s = stmt_uselect(sql->sa, l, r, range2lcompare(e->flag),
					    stmt_uselect(sql->sa, l, r2, range2rcompare(e->flag), sel));
				} else {
					s = stmt_uselect2(sql->sa, l, r, r2, (comp_type)e->flag, sel);
				}
			} else {
				/* value compare or select */
				if (l->nrcols == 0 && r->nrcols == 0) {
					sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema,
							compare_func((comp_type)e->flag),
							tail_type(l), tail_type(r), F_FUNC);
					assert(f);
					s = stmt_binop(sql->sa, l, r, f);
				} else {
					/* this can still be a join (as relational algebra and single value subquery results still means joins */
					s = stmt_uselect(sql->sa, l, r, (comp_type)e->flag, sel);
				}
			}
		}
		if (is_anti(e))
			s->flag |= ANTI;
	 }	break;
	default:
		;
	}
	return s;
}

static stmt *check_types(mvc *sql, sql_subtype *ct, stmt *s, check_type tpe);

static stmt *
stmt_col( mvc *sql, sql_column *c, stmt *del) 
{ 
	stmt *sc = stmt_bat(sql->sa, c, RDONLY);

	if (del)
		sc->partition = del->partition;
	if (isTable(c->t) && c->t->access != TABLE_READONLY &&
	   (c->base.flag != TR_NEW || c->t->base.flag != TR_NEW /* alter */) &&
	   (c->t->persistence == SQL_PERSIST || c->t->persistence == SQL_DECLARED_TABLE) && !c->t->commit_action) {
		stmt *i = stmt_bat(sql->sa, c, RD_INS);
		stmt *u = stmt_bat(sql->sa, c, RD_UPD_ID);
		sc = stmt_project_delta(sql->sa, sc, u, i);
		sc = stmt_project(sql->sa, del, sc);
		if (del)
			u->partition = del->partition;
	} else if (del) { /* always handle the deletes */
		sc = stmt_project(sql->sa, del, sc);
	}
	return sc;
}

static stmt *
stmt_idx( mvc *sql, sql_idx *i, stmt *del) 
{ 
	stmt *sc = stmt_idxbat(sql->sa, i, RDONLY);

	if (del)
		sc->partition = del->partition;
	if (isTable(i->t) && i->t->access != TABLE_READONLY &&
	   (i->base.flag != TR_NEW || i->t->base.flag != TR_NEW /* alter */) &&
	   (i->t->persistence == SQL_PERSIST || i->t->persistence == SQL_DECLARED_TABLE) && !i->t->commit_action) {
		stmt *ic = stmt_idxbat(sql->sa, i, RD_INS);
		stmt *u = stmt_idxbat(sql->sa, i, RD_UPD_ID);
		sc = stmt_project_delta(sql->sa, sc, u, ic);
		sc = stmt_project(sql->sa, del, sc);
		if (del)
			u->partition = del->partition;
	} else if (del) { /* always handle the deletes */
		sc = stmt_project(sql->sa, del, sc);
	}
	return sc;
}

#if 0
static stmt *
check_table_types(mvc *sql, list *types, stmt *s, check_type tpe)
{
	//const char *tname;
	stmt *tab = s;
	int temp = 0;

	if (s->type != st_table) {
		return sql_error(
			sql, 03,
			"single value and complex type are not equal");
	}
	tab = s->op1;
	temp = s->flag;
	if (tab->type == st_var) {
		sql_table *tbl = NULL;//tail_type(tab)->comp_type;
		stmt *dels = stmt_tid(sql->sa, tbl);
		node *n, *m;
		list *l = sa_list(sql->sa);
		
		stack_find_var(sql, tab->op1->op4.aval->data.val.sval);

		for (n = types->h, m = tbl->columns.set->h; 
			n && m; n = n->next, m = m->next) 
		{
			sql_subtype *ct = n->data;
			sql_column *dtc = m->data;
			stmt *dtcs = stmt_col(sql, dtc, dels);
			stmt *r = check_types(sql, ct, dtcs, tpe);
			if (!r) 
				return NULL;
			//r = stmt_alias(sql->sa, r, tbl->base.name, c->base.name);
			list_append(l, r);
		}
	 	return stmt_table(sql->sa, stmt_list(sql->sa, l), temp);
	} else if (tab->type == st_list) {
		node *n, *m;
		list *l = sa_list(sql->sa);
		for (n = types->h, m = tab->op4.lval->h; 
			n && m; n = n->next, m = m->next) 
		{
			sql_subtype *ct = n->data;
			stmt *r = check_types(sql, ct, m->data, tpe);
			if (!r) 
				return NULL;
			//tname = table_name(sql->sa, r);
			//r = stmt_alias(sql->sa, r, tname, c->base.name);
			list_append(l, r);
		}
		return stmt_table(sql->sa, stmt_list(sql->sa, l), temp);
	} else { /* single column/value */
		stmt *r;
		sql_subtype *st = tail_type(tab), *ct;

		if (list_length(types) != 1) {
			stmt *res = sql_error(
				sql, 03,
				"single value of type %s and complex type are not equal",
				st->type->sqlname
			);
			return res;
		}
		ct = types->h->data;
		r = check_types(sql, ct, tab, tpe);
		//tname = table_name(sql->sa, r);
		//r = stmt_alias(sql->sa, r, tname, c->base.name);
		return stmt_table(sql->sa, r, temp);
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
			VALinit(&a->data, a->data.vtype, ATOMnilptr(a->data.vtype));
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
inplace_convert(mvc *sql, sql_subtype *ct, stmt *s)
{
	atom *a;

	/* exclude named variables */
	if (s->type != st_var || (s->op1 && s->op1->op4.aval->data.val.sval) || 
		(ct->scale && ct->type->eclass != EC_FLT))
		return s;

	a = sql_bind_arg(sql, s->flag);
	if (atom_cast(a, ct)) {
		stmt *r = stmt_varnr(sql->sa, s->flag, ct);
		sql_convert_arg(sql, s->flag, ct);
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
check_types(mvc *sql, sql_subtype *ct, stmt *s, check_type tpe)
{
	int c = 0;
	sql_subtype *t = NULL, *st = NULL;

	/*
	if (ct->types) 
		return check_table_types(sql, ct->types, s, tpe);
		*/

 	st = tail_type(s);
	if ((!st || !st->type) && stmt_set_type_param(sql, ct, s) == 0) {
		return s;
	} else if (!st) {
		return sql_error(sql, 02, "statement has no type information");
	}

	/* first try cheap internal (inplace) convertions ! */
	s = inplace_convert(sql, ct, s);
	t = st = tail_type(s);

	/* check if the types are the same */
	if (t && subtype_cmp(t, ct) != 0) {
		t = NULL;
	}

	if (!t) {	/* try to convert if needed */
		c = sql_type_convert(st->type->eclass, ct->type->eclass);
		if (!c || (c == 2 && tpe == type_set) || 
                   (c == 3 && tpe != type_cast)) { 
			s = NULL;
		} else {
			s = stmt_convert(sql->sa, s, st, ct);
		}
	} 
	if (!s) {
		stmt *res = sql_error(
			sql, 03,
			"types %s(%d,%d) (%s) and %s(%d,%d) (%s) are not equal",
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
sql_unop_(mvc *sql, sql_schema *s, const char *fname, stmt *rs)
{
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

		rs = check_types(sql, &a->type, rs, type_equal);
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
		return stmt_unop(sql->sa, rs, f);
	} else if (rs) {
		char *type = tail_type(rs)->type->sqlname;

		return sql_error(sql, 02, "SELECT: no such unary operator '%s(%s)'", fname, type);
	}
	return NULL;
}

static stmt *
sql_Nop_(mvc *sql, const char *fname, stmt *a1, stmt *a2, stmt *a3, stmt *a4)
{
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
		return stmt_Nop(sql->sa, stmt_list(sql->sa, sl), f);
	return sql_error(sql, 02, "SELECT: no such operator '%s'", fname);
}

static stmt *
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

	(void) sqlparse(m);	/* blindly ignore errors */
	
	/* get out the single value as we don't want an enclosing projection! */
	if (m->sym->token == SQL_SELECT) {
		SelectNode *sn = (SelectNode *)m->sym;
		if (sn->selection->h->data.sym->token == SQL_COLUMN) {
			int is_last = 0;
			sql_rel *rel = NULL;
			sql_exp *e = rel_value_exp2(m, &rel, sn->selection->h->data.sym->data.lval->h->data.sym, sql_sel, ek, &is_last);

			if (!rel)
				s = exp_bin(m, e, NULL, NULL, NULL, NULL, NULL, NULL); 
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


static stmt *
stmt_rename(mvc *sql, sql_rel *rel, sql_exp *exp, stmt *s )
{
	const char *name = exp->name;
	const char *rname = exp->rname;
	stmt *o = s;

	(void)rel;
	if (!name && exp->type == e_column && exp->r)
		name = exp->r;
	if (!name)
		name = column_name(sql->sa, s);
	if (!rname && exp->type == e_column && exp->l)
		rname = exp->l;
	if (!rname)
		rname = table_name(sql->sa, s);
	s = stmt_alias(sql->sa, s, rname, name);
	if (o->flag & OUTER_ZERO)
		s->flag |= OUTER_ZERO;
	return s;
}

static stmt *
rel2bin_sql_table(mvc *sql, sql_table *t) 
{
	list *l = sa_list(sql->sa);
	node *n;
	stmt *dels = stmt_tid(sql->sa, t);
			
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;
		stmt *sc = stmt_col(sql, c, dels);

		list_append(l, sc);
	}
	/* TID column */
	if (t->columns.set->h) { 
		/* tid function  sql.tid(t) */
		const char *rnme = t->base.name;

		stmt *sc = dels?dels:stmt_tid(sql->sa, t);
		sc = stmt_alias(sql->sa, sc, rnme, TID);
		list_append(l, sc);
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			stmt *sc = stmt_idx(sql, i, dels);
			const char *rnme = t->base.name;

			/* index names are prefixed, to make them independent */
			sc = stmt_alias(sql->sa, sc, rnme, sa_strconcat(sql->sa, "%", i->base.name));
			list_append(l, sc);
		}
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_basetable( mvc *sql, sql_rel *rel)
{
	sql_table *t = rel->l;
	sql_column *c = rel->r;
	list *l = sa_list(sql->sa);
	stmt *dels;
	node *en;

	if (!t && c)
		t = c->t;
       	dels = stmt_tid(sql->sa, t);
	if (rel->flag == REL_PARTITION)
		dels->partition = 1;

	/* add aliases */
	assert(rel->exps);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		const char *rname = exp->rname?exp->rname:exp->l;
		const char *oname = exp->r;
		stmt *s = NULL;

		if (is_func(exp->type)) {
			list *exps = exp->l;
			sql_exp *cexp = exps->h->data;
			const char *cname = cexp->r;
			list *l = sa_list(sql->sa);

		       	c = find_sql_column(t, cname);
			s = stmt_col(sql, c, dels);
			append(l, s);
			if (exps->h->next) {
				sql_exp *at = exps->h->next->data;
				stmt *u = exp_bin(sql, at, NULL, NULL, NULL, NULL, NULL, NULL);

				append(l, u);
			}
			s = stmt_Nop(sql->sa, stmt_list(sql->sa, l), exp->f);
		} else if (oname[0] == '%' && strcmp(oname, TID) == 0) {
			/* tid function  sql.tid(t) */
			const char *rnme = t->base.name;

			s = dels?dels:stmt_tid(sql->sa, t);
			s = stmt_alias(sql->sa, s, rnme, TID);
		} else if (oname[0] == '%') { 
			sql_idx *i = find_sql_idx(t, oname+1);

			s = stmt_idx(sql, i, dels);
		} else {
			sql_column *c = find_sql_column(t, oname);

			s = stmt_col(sql, c, dels);
		}
		s->tname = rname;
		s->cname = exp->name;
		list_append(l, s);
	}
	return stmt_list(sql->sa, l);
}

static int 
alias_cmp( stmt *s, const char *nme )
{
	return strcmp(s->cname, nme);
}

static list* exps2bin_args( mvc *sql, list *exps, list *args);

static list *
exp2bin_args( mvc *sql, sql_exp *e, list *args)
{
	if (!e)
		return args;
	switch(e->type){
	case e_column:
	case e_psm:
		return args;
	case e_cmp:
		if (e->flag == cmp_or || get_cmp(e) == cmp_filter) {
			args = exps2bin_args(sql, e->l, args);
			args = exps2bin_args(sql, e->r, args);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			args = exp2bin_args(sql, e->l, args);
			args = exps2bin_args(sql, e->r, args);
		} else {
			args = exp2bin_args(sql, e->l, args);
			args = exp2bin_args(sql, e->r, args);
			if (e->f)
				args = exp2bin_args(sql, e->f, args);
		}
		return args;
	case e_convert:
		if (e->l)
			return exp2bin_args(sql, e->l, args);
		break;
	case e_aggr:
	case e_func: 
		if (e->l)
			return exps2bin_args(sql, e->l, args);
		break;
	case e_atom:
		if (e->l) {
			return args;
		} else if (e->f) {
			return exps2bin_args(sql, e->f, args);
		} else if (e->r) {
			char nme[64];

			snprintf(nme, 64, "A%s", (char*)e->r);
			if (!list_find(args, nme, (fcmp)&alias_cmp)) {
				stmt *s = stmt_var(sql->sa, e->r, &e->tpe, 0, 0);

				s = stmt_alias(sql->sa, s, NULL, sa_strdup(sql->sa, nme));
				list_append(args, s);
			}
		} else {
			char nme[16];

			snprintf(nme, 16, "A%d", e->flag);
			if (!list_find(args, nme, (fcmp)&alias_cmp)) {
				atom *a = sql->args[e->flag];
				stmt *s = stmt_varnr(sql->sa, e->flag, &a->tpe);

				s = stmt_alias(sql->sa, s, NULL, sa_strdup(sql->sa, nme));
				list_append(args, s);
			}
		}
	}
	return args;
}

static list *
exps2bin_args( mvc *sql, list *exps, list *args)
{
	node *n;

	if (!exps)
		return args;
	for (n = exps->h; n; n = n->next)
		args = exp2bin_args(sql, n->data, args);
	return args;
}

static list *
rel2bin_args( mvc *sql, sql_rel *rel, list *args)
{
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

	case op_apply: 
	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		args = rel2bin_args(sql, rel->l, args);
		args = rel2bin_args(sql, rel->r, args);
		break;
	case op_groupby: 
		if (rel->r) 
			args = exps2bin_args(sql, rel->r, args);
	case op_project:
	case op_select: 
	case op_topn: 
	case op_sample: 
		if (rel->exps)
			args = exps2bin_args(sql, rel->exps, args);
		args = rel2bin_args(sql, rel->l, args);
		break;
	case op_ddl: 
		args = rel2bin_args(sql, rel->l, args);
		if (rel->r)
			args = rel2bin_args(sql, rel->r, args);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		args = rel2bin_args(sql, rel->r, args);
		break;
	}
	return args;
}

static stmt *
rel2bin_table( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	stmt *sub = NULL, *osub = NULL;
	node *en, *n;
	sql_exp *op = rel->r;

	if (op) {
		int i;
		sql_subfunc *f = op->f;
		stmt *psub = NULL;
			
		if (rel->l) { /* first construct the sub relation */
			sql_rel *l = rel->l;
			if (l->op == op_ddl) {
				sql_table *t = rel_ddl_table_get(l);

				if (t)
					sub = rel2bin_sql_table(sql, t);
			} else {
				sub = subrel_bin(sql, rel->l, refs);
			}
			if (!sub) 
				return NULL;
		}

		psub = exp_bin(sql, op, sub, NULL, NULL, NULL, NULL, NULL); /* table function */
		if (!f || !psub) { 
			assert(0);
			return NULL;	
		}
		l = sa_list(sql->sa);
		if (f->func->varres) {
			for(i=0, en = rel->exps->h, n = f->res->h; en; en = en->next, n = n->next, i++ ) {
				sql_exp *exp = en->data;
				sql_subtype *st = n->data;
				const char *rnme = exp->rname?exp->rname:exp->l;
				stmt *s = stmt_rs_column(sql->sa, psub, i, st); 
		
				s = stmt_alias(sql->sa, s, rnme, exp->name);
				list_append(l, s);
			}
		} else {
			for(i = 0, n = f->func->res->h; n; n = n->next, i++ ) {
				sql_arg *a = n->data;
				stmt *s = stmt_rs_column(sql->sa, psub, i, &a->type); 
				const char *rnme = exp_find_rel_name(op);
	
				s = stmt_alias(sql->sa, s, rnme, a->name);
				list_append(l, s);
			}
			if (list_length(f->res) == list_length(f->func->res) + 1) {
				/* add missing %TID% column */
				sql_subtype *t = f->res->t->data;
				stmt *s = stmt_rs_column(sql->sa, psub, i, t); 
				const char *rnme = exp_find_rel_name(op);
	
				s = stmt_alias(sql->sa, s, rnme, TID);
				list_append(l, s);
			}
		}
		if (!rel->flag && sub && sub->nrcols) { 
			assert(0);
			list_merge(l, sub->op4.lval, NULL);
			osub = sub;
		}
		sub = stmt_list(sql->sa, l);
	} else if (rel->l) { /* handle sub query via function */
		int i;
		char name[16], *nme;

		nme = number2name(name, 16, ++sql->label);

		l = rel2bin_args(sql, rel->l, sa_list(sql->sa));
		sub = stmt_list(sql->sa, l);
		sub = stmt_func(sql->sa, sub, sa_strdup(sql->sa, nme), rel->l, 0);
		l = sa_list(sql->sa);
		for(i = 0, n = rel->exps->h; n; n = n->next, i++ ) {
			sql_exp *c = n->data;
			stmt *s = stmt_rs_column(sql->sa, sub, i, exp_subtype(c)); 
			const char *nme = exp_name(c);
			const char *rnme = NULL;

			s = stmt_alias(sql->sa, s, rnme, nme);
			list_append(l, s);
		}
		sub = stmt_list(sql->sa, l);
	}
	if (!sub) { 
		assert(0);
		return NULL;	
	}
	l = sa_list(sql->sa);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		const char *rnme = exp->rname?exp->rname:exp->l;
		stmt *s;

		/* no relation names */
		if (exp->l)
			exp->l = NULL;
		s = exp_bin(sql, exp, sub, NULL, NULL, NULL, NULL, NULL);

		if (!s) {
			assert(0);
			return NULL;
		}
		if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(sql->sa, bin_first_column(sql->sa, sub), s);
		s = stmt_alias(sql->sa, s, rnme, exp->name);
		list_append(l, s);
	}
	if (osub && osub->nrcols) 
		list_merge(l, osub->op4.lval, NULL);
	sub = stmt_list(sql->sa, l);
	return sub;
}

static stmt *
rel2bin_hash_lookup( mvc *sql, sql_rel *rel, stmt *left, stmt *right, sql_idx *i, node *en ) 
{
	node *n;
	sql_subtype *it = sql_bind_localtype("int");
	sql_subtype *lng = sql_bind_localtype("lng");
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(sql->sa, 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1));
	sql_exp *e = en->data;
	sql_exp *l = e->l;
	stmt *idx = bin_find_column(sql->sa, left, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	int swap_exp = 0, swap_rel = 0;

	if (!idx) {
		swap_exp = 1;
		l = e->r;
		idx = bin_find_column(sql->sa, left, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx && right) {
		swap_exp = 0;
		swap_rel = 1;
		l = e->l;
		idx = bin_find_column(sql->sa, right, l->l, sa_strconcat(sql->sa, "%", i->base.name));
	}
	if (!idx && right) {
		swap_exp = 1;
		swap_rel = 1;
		l = e->r;
		idx = bin_find_column(sql->sa, right, l->l, sa_strconcat(sql->sa, "%", i->base.name));
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
				s = exp_bin(sql, ee, left, NULL, NULL, NULL, NULL, NULL);
			else
				s = exp_bin(sql, ee, right, NULL, NULL, NULL, NULL, NULL);
		}

		if (!s) 
			return NULL;
		if (h) {
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", lng, it, tail_type(s), lng);

			h = stmt_Nop(sql->sa, stmt_list(sql->sa, list_append( list_append(
				list_append(sa_list(sql->sa), h), bits), s)), xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", tail_type(s), NULL, lng);

			h = stmt_unop(sql->sa, s, hf);
		}
	}
	if (h->nrcols) {
		if (!swap_rel) {
			return stmt_join(sql->sa, idx, h, cmp_equal);
		} else {
			return stmt_join(sql->sa, h, idx, cmp_equal);
		}
	} else {
		return stmt_uselect(sql->sa, idx, h, cmp_equal, NULL);
	}
}

static stmt *
join_hash_key( mvc *sql, list *l ) 
{
	node *m;
	sql_subtype *it, *lng;
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(sql->sa, 1 + ((sizeof(lng)*8)-1)/(list_length(l)+1));

	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = l->h; m; m = m->next) {
		stmt *s = m->data;

		if (h) {
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", lng, it, tail_type(s), lng);

			h = stmt_Nop(sql->sa, stmt_list(sql->sa, list_append( list_append( list_append(sa_list(sql->sa), h), bits), s )), xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", tail_type(s), NULL, lng);
			h = stmt_unop(sql->sa, s, hf);
		}
	}
	return h;
}

static stmt *
releqjoin( mvc *sql, list *l1, list *l2, int used_hash, comp_type cmp_op, int need_left )
{
	node *n1 = l1->h, *n2 = l2->h;
	stmt *l, *r, *res;

	if (list_length(l1) <= 1) {
		l = l1->h->data;
		r = l2->h->data;
		r =  stmt_join(sql->sa, l, r, cmp_op);
		if (need_left)
			r->flag = cmp_left;
		return r;
	}
	if (used_hash) {
		l = n1->data;
		r = n2->data;
		n1 = n1->next;
		n2 = n2->next;
		res = stmt_join(sql->sa, l, r, cmp_op);
	} else { /* need hash */
		l = join_hash_key(sql, l1);
		r = join_hash_key(sql, l2);
		res = stmt_join(sql->sa, l, r, cmp_op);
	}
	if (need_left) 
		res->flag = cmp_left;
	l = stmt_result(sql->sa, res, 0);
	r = stmt_result(sql->sa, res, 1);
	for (; n1 && n2; n1 = n1->next, n2 = n2->next) {
		stmt *ld = n1->data;
		stmt *rd = n2->data;
		stmt *le = stmt_project(sql->sa, l, ld );
		stmt *re = stmt_project(sql->sa, r, rd );
		/* intentional both tail_type's of le (as re sometimes is a find for bulk loading */
		sql_subfunc *f = NULL;
		stmt * cmp;

		if (cmp_op == cmp_equal) 
			f = sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(le), tail_type(le), F_FUNC);
		else 
			f = sql_bind_func(sql->sa, sql->session->schema, "=", tail_type(le), tail_type(le), F_FUNC);
		assert(f);

		cmp = stmt_binop(sql->sa, le, re, f);
		cmp = stmt_uselect(sql->sa, cmp, stmt_bool(sql->sa, 1), cmp_equal, NULL);
		l = stmt_project(sql->sa, cmp, l );
		r = stmt_project(sql->sa, cmp, r );
	}
	res = stmt_join(sql->sa, l, r, cmp_joined);
	return res;
}

static stmt *
rel2bin_join( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL, *jl, *jr;
	stmt *ld = NULL, *rd = NULL;
	int need_left = (rel->flag == LEFT_JOIN);

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);
	right = row2cols(sql, right);
	/* 
 	 * split in 2 steps, 
 	 * 	first cheap join(s) (equality or idx) 
 	 * 	second selects/filters 
	 */
	if (rel->exps) {
		int used_hash = 0;
		int idx = 0;
		list *jexps = sa_list(sql->sa);
		list *lje = sa_list(sql->sa);
		list *rje = sa_list(sql->sa);

		/* get equi-joins/filters first */
		if (list_length(rel->exps) > 1) {
			for( en = rel->exps->h; en; en = en->next ) {
				sql_exp *e = en->data;
				if (e->type == e_cmp && (e->flag == cmp_equal || e->flag == cmp_filter))
					append(jexps, e);
			}
			for( en = rel->exps->h; en; en = en->next ) {
				sql_exp *e = en->data;
				if (e->type != e_cmp || (e->flag != cmp_equal && e->flag != cmp_filter))
					append(jexps, e);
			}
			rel->exps = jexps;
		}

		/* generate a relational join */
		for( en = rel->exps->h; en; en = en->next ) {
			int join_idx = sql->opt_stats[0];
			sql_exp *e = en->data;
			stmt *s = NULL;
			prop *p;

			/* only handle simple joins here */		
			if (exp_has_func(e) && e->flag != cmp_filter) {
				if (!join && !list_length(lje)) {
					stmt *l = bin_first_column(sql->sa, left);
					stmt *r = bin_first_column(sql->sa, right);
					join = stmt_join(sql->sa, l, r, cmp_all); 
				}
				break;
			}
			if (list_length(lje) && (idx || e->type != e_cmp || (e->flag != cmp_equal && e->flag != cmp_filter) ||
			   (join && e->flag == cmp_filter)))
				break;

			/* handle possible index lookups */
			/* expressions are in index order ! */
			if (!join &&
			    (p=find_prop(e->p, PROP_HASHCOL)) != NULL) {
				sql_idx *i = p->value;
			
				join = s = rel2bin_hash_lookup(sql, rel, left, right, i, en);
				if (s) {
					list_append(lje, s->op1);
					list_append(rje, s->op2);
					used_hash = 1;
				}
			}

			s = exp_bin(sql, e, left, right, NULL, NULL, NULL, NULL);
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
					stmt *l = bin_first_column(sql->sa, left);
					stmt *r = bin_first_column(sql->sa, right);

					l = stmt_uselect(sql->sa, stmt_const(sql->sa, l, stmt_bool(sql->sa, 1)), s, cmp_equal, NULL);
					join = stmt_join(sql->sa, l, r, cmp_all);
					continue;
				}
				if (!join) {
					stmt *l = bin_first_column(sql->sa, left);
					stmt *r = bin_first_column(sql->sa, right);
					join = stmt_join(sql->sa, l, r, cmp_all); 
				}
				break;
			}

			if (!join) 
				join = s;
			list_append(lje, s->op1);
			list_append(rje, s->op2);
		}
		if (list_length(lje) > 1) {
			join = releqjoin(sql, lje, rje, used_hash, cmp_equal, need_left);
		} else if (!join) {
			join = stmt_join(sql->sa, lje->h->data, rje->h->data, cmp_equal);
			if (need_left)
				join->flag = cmp_left;
		}
	} else {
		stmt *l = bin_first_column(sql->sa, left);
		stmt *r = bin_first_column(sql->sa, right);
		join = stmt_join(sql->sa, l, r, cmp_all); 
	}
	jl = stmt_result(sql->sa, join, 0);
	jr = stmt_result(sql->sa, join, 1);
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
			stmt *s = stmt_project(sql->sa, jl, column(sql->sa, c) );
	
			s = stmt_alias(sql->sa, s, rnme, nme);
			list_append(nl, s);
		}
		for( n = right->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(sql->sa, jr, column(sql->sa, c) );

			s = stmt_alias(sql->sa, s, rnme, nme);
			list_append(nl, s);
		}
		sub = stmt_list(sql->sa, nl);

		/* continue with non equi-joins */
		for( ; en; en = en->next ) {
			stmt *s = exp_bin(sql, en->data, sub, NULL, NULL, NULL, NULL, sel);

			if (!s) {
				assert(0);
				return NULL;
			}
			sel = s;
		}
		/* recreate join output */
		jl = stmt_project(sql->sa, sel, jl); 
		jr = stmt_project(sql->sa, sel, jr); 
	}

	/* construct relation */
	l = sa_list(sql->sa);

	if (rel->op == op_left || rel->op == op_full) {
		/* we need to add the missing oid's */
		ld = stmt_mirror(sql->sa, bin_first_column(sql->sa, left));
		ld = stmt_tdiff(sql->sa, ld, jl);
	}
	if (rel->op == op_right || rel->op == op_full) {
		/* we need to add the missing oid's */
		rd = stmt_mirror(sql->sa, bin_first_column(sql->sa, right));
		rd = stmt_tdiff(sql->sa, rd, jr);
	}

	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(sql->sa, jl, column(sql->sa, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = Column(sql->sa, s);
		if (rel->op == op_left || rel->op == op_full)
			s = stmt_append(sql->sa, s, stmt_project(sql->sa, ld, c));
		if (rel->op == op_right || rel->op == op_full) 
			s = stmt_append(sql->sa, s, stmt_const(sql->sa, rd, (c->flag&OUTER_ZERO)?stmt_atom_lng(sql->sa, 0):stmt_atom(sql->sa, atom_general(sql->sa, tail_type(c), NULL))));

		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	for( n = right->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(sql->sa, jr, column(sql->sa, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = Column(sql->sa, s);
		if (rel->op == op_left || rel->op == op_full) 
			s = stmt_append(sql->sa, s, stmt_const(sql->sa, ld, (c->flag&OUTER_ZERO)?stmt_atom_lng(sql->sa, 0):stmt_atom(sql->sa, atom_general(sql->sa, tail_type(c), NULL))));
		if (rel->op == op_right || rel->op == op_full) 
			s = stmt_append(sql->sa, s, stmt_project(sql->sa, rd, c));

		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_semijoin( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	node *en = NULL, *n;
	stmt *left = NULL, *right = NULL, *join = NULL, *jl, *jr, *c;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);
	right = row2cols(sql, right);
	/* 
 	 * split in 2 steps, 
 	 * 	first cheap join(s) (equality or idx) 
 	 * 	second selects/filters 
	 */
	if (rel->exps) {
		int idx = 0;
		list *lje = sa_list(sql->sa);
		list *rje = sa_list(sql->sa);

		for( en = rel->exps->h; en; en = en->next ) {
			int join_idx = sql->opt_stats[0];
			sql_exp *e = en->data;
			stmt *s = NULL;

			/* only handle simple joins here */		
			if (list_length(lje) && (idx || e->type != e_cmp || e->flag != cmp_equal))
				break;

			s = exp_bin(sql, en->data, left, right, NULL, NULL, NULL, NULL);
			if (!s) 
				return NULL;
			if (join_idx != sql->opt_stats[0])
				idx = 1;
			/* stop on first non equality join */
			if (!join) {
				join = s;
			} else if (s->type != st_join && s->type != st_join2 && s->type != st_joinN) {
				/* handle select expressions */
				assert(0);
				return NULL;
			}
			if (s->type == st_join || s->type == st_join2 || s->type == st_joinN) { 
				list_append(lje, s->op1);
				list_append(rje, s->op2);
			}
		}
		if (list_length(lje) > 1) {
			join = releqjoin(sql, lje, rje, 0 /* no hash used */, cmp_equal, 0);
		} else if (!join) {
			join = stmt_join(sql->sa, lje->h->data, rje->h->data, cmp_equal);
		}
	} else {
		stmt *l = bin_first_column(sql->sa, left);
		stmt *r = bin_first_column(sql->sa, right);
		join = stmt_join(sql->sa, l, r, cmp_all); 
	}
	jl = stmt_result(sql->sa, join, 0);
	jr = stmt_result(sql->sa, join, 1);
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
			stmt *s = stmt_project(sql->sa, jl, column(sql->sa, c) );
	
			s = stmt_alias(sql->sa, s, rnme, nme);
			list_append(nl, s);
		}
		for( n = right->op4.lval->h; n; n = n->next ) {
			stmt *c = n->data;
			const char *rnme = table_name(sql->sa, c);
			const char *nme = column_name(sql->sa, c);
			stmt *s = stmt_project(sql->sa, jr, column(sql->sa, c) );

			s = stmt_alias(sql->sa, s, rnme, nme);
			list_append(nl, s);
		}
		sub = stmt_list(sql->sa, nl);

		/* continue with non equi-joins */
		for( ; en; en = en->next ) {
			stmt *s = exp_bin(sql, en->data, sub, NULL, NULL, NULL, NULL, sel);

			if (!s) {
				assert(0);
				return NULL;
			}
			sel = s;
		}
		/* recreate join output */
		jl = stmt_project(sql->sa, sel, jl); 
	}

	/* construct relation */
	l = sa_list(sql->sa);

	/* We did a full join, thats too much. 
	   Reduce this using difference and intersect */
	c = stmt_mirror(sql->sa, left->op4.lval->h->data);
	if (rel->op == op_anti) {
		join = stmt_tdiff(sql->sa, c, jl);
	} else {
		join = stmt_tinter(sql->sa, c, jl);
	}

	/* project all the left columns */
	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		const char *rnme = table_name(sql->sa, c);
		const char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(sql->sa, join, column(sql->sa, c));

		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_distinct(mvc *sql, stmt *s, stmt **distinct)
{
	node *n;
	stmt *g = NULL, *grp = NULL, *ext = NULL, *cnt = NULL;
	list *rl = sa_list(sql->sa), *tids;

	/* single values are unique */
	if (s->key && s->nrcols == 0)
		return s;

	/* Use 'all' tid columns */
	if ((tids = bin_find_columns(sql, s, TID)) != NULL) {
		for (n = tids->h; n; n = n->next) {
			stmt *t = n->data;

			g = stmt_group(sql->sa, column(sql->sa, t), grp, ext, cnt);
			grp = stmt_result(sql->sa, g, 0); 
			ext = stmt_result(sql->sa, g, 1); 
			cnt = stmt_result(sql->sa, g, 2); 
		}
	} else {
		for (n = s->op4.lval->h; n; n = n->next) {
			stmt *t = n->data;

			g = stmt_group(sql->sa, column(sql->sa, t), grp, ext, cnt);
			grp = stmt_result(sql->sa, g, 0); 
			ext = stmt_result(sql->sa, g, 1); 
			cnt = stmt_result(sql->sa, g, 2); 
		}
	}
	stmt_group_done(g);
	if (!ext)
		return NULL;

	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *t = n->data;

		list_append(rl, stmt_project(sql->sa, ext, t));
	}

	if (distinct)
		*distinct = ext;
	s = stmt_list(sql->sa, rl);
	return s;
}

static stmt *
rel_rename(mvc *sql, sql_rel *rel, stmt *sub)
{
	if (rel->exps) {
		node *en, *n;
		list *l = sa_list(sql->sa);

		for( en = rel->exps->h, n = sub->op4.lval->h; en && n; en = en->next, n = n->next ) {
			sql_exp *exp = en->data;
			stmt *s = n->data;

			if (!s) {
				assert(0);
				return NULL;
			}
			s = stmt_rename(sql, rel, exp, s);
			list_append(l, s);
		}
		sub = stmt_list(sql->sa, l);
	}
	return sub;
}

static stmt *
rel2bin_union( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
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

		s = stmt_append(sql->sa, Column(sql->sa, c1), c2);
		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	sub = stmt_list(sql->sa, l);

	sub = rel_rename(sql, rel, sub);
	if (need_distinct(rel)) 
		sub = rel2bin_distinct(sql, sub, NULL);
	return sub;
}

static stmt *
rel2bin_except( mvc *sql, sql_rel *rel, list *refs)
{
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
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);
	right = row2cols(sql, right);

	/*
	 * The multi column except is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of L with min(L.count,R.count), 
	 * number of rows.
	 */
	for (n = left->op4.lval->h; n; n = n->next) {
		lg = stmt_group(sql->sa, column(sql->sa, n->data), lgrp, lext, lcnt);
		lgrp = stmt_result(sql->sa, lg, 0);
		lext = stmt_result(sql->sa, lg, 1);
		lcnt = stmt_result(sql->sa, lg, 2);
	}
	for (n = right->op4.lval->h; n; n = n->next) {
		rg = stmt_group(sql->sa, column(sql->sa, n->data), rgrp, rext, rcnt);
		rgrp = stmt_result(sql->sa, rg, 0);
		rext = stmt_result(sql->sa, rg, 1);
		rcnt = stmt_result(sql->sa, rg, 2);
	}

	if (!lg || !rg) 
		return NULL;
	stmt_group_done(lg);
	stmt_group_done(rg);

	if (need_distinct(rel)) {
		lcnt = stmt_const(sql->sa, lcnt, stmt_atom_lng(sql->sa, 1));
		rcnt = stmt_const(sql->sa, rcnt, stmt_atom_lng(sql->sa, 1));
	}

	/* now find the matching groups */
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(sql->sa, n->data);
		stmt *r = column(sql->sa, m->data);

		l = stmt_project(sql->sa, lext, l);
		r = stmt_project(sql->sa, rext, r);
		list_append(lje, l);
		list_append(rje, r);
	}
	s = releqjoin(sql, lje, rje, 1 /* cannot use hash */, cmp_equal_nil, 0);
	lm = stmt_result(sql->sa, s, 0);
	rm = stmt_result(sql->sa, s, 1);

	s = stmt_mirror(sql->sa, lext);
	s = stmt_tdiff(sql->sa, s, lm);

	/* first we find those missing in R */
	next = stmt_project(sql->sa, s, lext);
	ncnt = stmt_project(sql->sa, s, lcnt);
	zero = stmt_const(sql->sa, s, stmt_atom_lng(sql->sa, 0));

	/* ext, lcount, rcount */
	lext = stmt_project(sql->sa, lm, lext);
	lcnt = stmt_project(sql->sa, lm, lcnt);
	rcnt = stmt_project(sql->sa, rm, rcnt);

	/* append those missing in L */
	lext = stmt_append(sql->sa, lext, next);
	lcnt = stmt_append(sql->sa, lcnt, ncnt);
	rcnt = stmt_append(sql->sa, rcnt, zero);

 	min = sql_bind_func(sql->sa, sql->session->schema, "sql_sub", lng, lng, F_FUNC);
	s = stmt_binop(sql->sa, lcnt, rcnt, min); /* use count */

	/* now we have gid,cnt, blowup to full groupsizes */
	s = stmt_gen_group(sql->sa, lext, s);

	/* project columns of left hand expression */
	stmts = sa_list(sql->sa);
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(sql->sa, n->data);
		const char *rnme = NULL;
		const char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_project(sql->sa, s, c1);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(sql->sa, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(sql->sa, stmts);
	return rel_rename(sql, rel, sub);
}

static stmt *
rel2bin_inter( mvc *sql, sql_rel *rel, list *refs)
{
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
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);

	/*
	 * The multi column intersect is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of L with min(L.count,R.count), 
	 * number of rows.
	 */
	for (n = left->op4.lval->h; n; n = n->next) {
		lg = stmt_group(sql->sa, column(sql->sa, n->data), lgrp, lext, lcnt);
		lgrp = stmt_result(sql->sa, lg, 0);
		lext = stmt_result(sql->sa, lg, 1);
		lcnt = stmt_result(sql->sa, lg, 2);
	}
	for (n = right->op4.lval->h; n; n = n->next) {
		rg = stmt_group(sql->sa, column(sql->sa, n->data), rgrp, rext, rcnt);
		rgrp = stmt_result(sql->sa, rg, 0);
		rext = stmt_result(sql->sa, rg, 1);
		rcnt = stmt_result(sql->sa, rg, 2);
	}

	if (!lg || !rg) 
		return NULL;
	stmt_group_done(lg);
	stmt_group_done(rg);

	if (need_distinct(rel)) {
		lcnt = stmt_const(sql->sa, lcnt, stmt_atom_lng(sql->sa, 1));
		rcnt = stmt_const(sql->sa, rcnt, stmt_atom_lng(sql->sa, 1));
	}

	/* now find the matching groups */
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(sql->sa, n->data);
		stmt *r = column(sql->sa, m->data);

		l = stmt_project(sql->sa, lext, l);
		r = stmt_project(sql->sa, rext, r);
		list_append(lje, l);
		list_append(rje, r);
	}
	s = releqjoin(sql, lje, rje, 1 /* cannot use hash */, cmp_equal_nil, 0);
	lm = stmt_result(sql->sa, s, 0);
	rm = stmt_result(sql->sa, s, 1);
		
	/* ext, lcount, rcount */
	lext = stmt_project(sql->sa, lm, lext);
	lcnt = stmt_project(sql->sa, lm, lcnt);
	rcnt = stmt_project(sql->sa, rm, rcnt);

 	min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", lng, lng, F_FUNC);
	s = stmt_binop(sql->sa, lcnt, rcnt, min);

	/* now we have gid,cnt, blowup to full groupsizes */
	s = stmt_gen_group(sql->sa, lext, s);

	/* project columns of left hand expression */
	stmts = sa_list(sql->sa);
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(sql->sa, n->data);
		const char *rnme = NULL;
		const char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_project(sql->sa, s, c1);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(sql->sa, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(sql->sa, stmts);
	return rel_rename(sql, rel, sub);
}

static stmt *
sql_reorder(mvc *sql, stmt *order, stmt *s) 
{
	list *l = sa_list(sql->sa);
	node *n;

	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *sc = n->data;
		const char *cname = column_name(sql->sa, sc);
		const char *tname = table_name(sql->sa, sc);

		sc = stmt_project(sql->sa, order, sc);
		sc = stmt_alias(sql->sa, sc, tname, cname );
		list_append(l, sc);
	}
	return stmt_list(sql->sa, l);
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
rel2bin_project( mvc *sql, sql_rel *rel, list *refs, sql_rel *topn)
{
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
			l = exp_bin(sql, le, NULL, NULL, NULL, NULL, NULL, NULL);
			if (oe) {
				sql_subtype *lng = sql_bind_localtype("lng");
				sql_subfunc *add = sql_bind_func_result(sql->sa, sql->session->schema, "sql_add", lng, lng, lng);
				stmt *o = exp_bin(sql, oe, NULL, NULL, NULL, NULL, NULL, NULL);
				l = stmt_binop(sql->sa, l, o, add);
			}
		}
	}

	if (!rel->exps) 
		return stmt_none(sql->sa);

	if (rel->l) { /* first construct the sub relation */
		sql_rel *l = rel->l;
		if (l->op == op_ddl) {
			sql_table *t = rel_ddl_table_get(l);

			if (t)
				sub = rel2bin_sql_table(sql, t);
		} else {
			sub = subrel_bin(sql, rel->l, refs);
		}
		if (!sub) 
			return NULL;
	}

	pl = sa_list(sql->sa);
	if (sub)
		pl->expected_cnt = list_length(sub->op4.lval);
	psub = stmt_list(sql->sa, pl);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		stmt *s = exp_bin(sql, exp, sub, psub, NULL, NULL, NULL, NULL);

		if (!s) {
			assert(0);
			return NULL;
		}
		/* single value with limit */
		if (topn && rel->r && sub && sub->nrcols == 0)
			s = const_column(sql->sa, s);
		else if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(sql->sa, bin_first_column(sql->sa, sub), s);
			
		s = stmt_rename(sql, rel, exp, s);
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

			stmt *orderbycolstmt = exp_bin(sql, orderbycole, sub, psub, NULL, NULL, NULL, NULL); 

			if (!orderbycolstmt) 
				return NULL;
			
			/* handle constants */
			orderbycolstmt = column(sql->sa, orderbycolstmt);
			if (!limit) {	/* topn based on a single column */
				limit = stmt_limit(sql->sa, orderbycolstmt, stmt_atom_lng(sql->sa, 0), l, LIMIT_FLAG(distinct, is_ascending(orderbycole), last, 1));
			} else { 	/* topn based on 2 columns */
				limit = stmt_limit2(sql->sa, orderbycolstmt, lpiv, lgid, stmt_atom_lng(sql->sa, 0), l, LIMIT_FLAG(distinct, is_ascending(orderbycole), last, 1));
			}
			if (!limit) 
				return NULL;
			lpiv = limit;
			if (!last) {
				lpiv = stmt_result(sql->sa, limit, 0);
				lgid = stmt_result(sql->sa, limit, 1);
			}
		}

		limit = lpiv; 
		for ( n=pl->h ; n; n = n->next) 
			list_append(npl, stmt_project(sql->sa, limit, column(sql->sa, n->data)));
		psub = stmt_list(sql->sa, npl);

		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		pl = sub->op4.lval;
		npl = sa_list(sql->sa);
		for ( n=pl->h ; n; n = n->next) {
			list_append(npl, stmt_project(sql->sa, limit, column(sql->sa, n->data))); 
		}
		sub = stmt_list(sql->sa, npl);
	}
	if (need_distinct(rel)) {
		stmt *distinct = NULL;
		psub = rel2bin_distinct(sql, psub, &distinct);
		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		if (sub) {
			list *npl = sa_list(sql->sa);
			
			pl = sub->op4.lval;
			for ( n=pl->h ; n; n = n->next) 
				list_append(npl, stmt_project(sql->sa, distinct, column(sql->sa, n->data))); 
			sub = stmt_list(sql->sa, npl);
		}
	}
	if (/*(!topn || need_distinct(rel)) &&*/ rel->r) {
		list *oexps = rel->r;
		stmt *orderby_ids = NULL, *orderby_grp = NULL;

		for (en = oexps->h; en; en = en->next) {
			stmt *orderby = NULL;
			sql_exp *orderbycole = en->data; 
			stmt *orderbycolstmt = exp_bin(sql, orderbycole, sub, psub, NULL, NULL, NULL, NULL); 

			if (!orderbycolstmt) {
				assert(0);
				return NULL;
			}
			/* single values don't need sorting */
			if (orderbycolstmt->nrcols == 0) {
				orderby_ids = NULL;
				break;
			}
			if (orderby_ids)
				orderby = stmt_reorder(sql->sa, orderbycolstmt, is_ascending(orderbycole), orderby_ids, orderby_grp);
			else
				orderby = stmt_order(sql->sa, orderbycolstmt, is_ascending(orderbycole));
			orderby_ids = stmt_result(sql->sa, orderby, 1);
			orderby_grp = stmt_result(sql->sa, orderby, 2);
		}
		if (orderby_ids)
			psub = sql_reorder(sql, orderby_ids, psub);
	}
	return psub;
}

static stmt *
rel2bin_predicate(mvc *sql) 
{
	return const_column(sql->sa, stmt_bool(sql->sa, 1));
}

static stmt *
rel2bin_select( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	node *en, *n;
	stmt *sub = NULL, *sel = NULL;
	stmt *predicate = NULL;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(sql, rel->l, refs);
		if (!sub) 
			return NULL;	
		sub = row2cols(sql, sub);
	}
	if (!sub && !predicate) 
		predicate = rel2bin_predicate(sql);
	else if (!predicate)
		predicate = stmt_const(sql->sa, bin_first_column(sql->sa, sub), stmt_bool(sql->sa, 1));
	if (!rel->exps || !rel->exps->h) {
		if (sub)
			return sub;
		return predicate;
	}
	if (!sub && predicate) {
		list *l = sa_list(sql->sa);
		append(l, predicate);
		sub = stmt_list(sql->sa, l);
	}
	/* handle possible index lookups */
	/* expressions are in index order ! */
	if (sub && (en = rel->exps->h) != NULL) { 
		sql_exp *e = en->data;
		prop *p;

		if ((p=find_prop(e->p, PROP_HASHCOL)) != NULL) {
			sql_idx *i = p->value;
			
			sel = rel2bin_hash_lookup(sql, rel, sub, NULL, i, en);
		}
	} 
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *e = en->data;
		stmt *s = exp_bin(sql, e, sub, NULL, NULL, NULL, NULL, sel);

		if (!s) {
			assert(0);
			return NULL;
		}
		if (s->nrcols == 0){
			sel = stmt_uselect(sql->sa, predicate, s, cmp_equal, sel);
		} else if (e->type != e_cmp) {
			sel = stmt_uselect(sql->sa, s, stmt_bool(sql->sa, 1), cmp_equal, NULL);
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
				col = stmt_const(sql->sa, sel, col);
			else
				col = stmt_project(sql->sa, sel, col);
			list_append(l, col);
		}
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_groupby( mvc *sql, sql_rel *rel, list *refs)
{
	list *l, *aggrs, *gbexps = sa_list(sql->sa);
	node *n, *en;
	stmt *sub = NULL, *cursub;
	stmt *groupby = NULL, *grp = NULL, *ext = NULL, *cnt = NULL;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(sql, rel->l, refs);
		if (!sub)
			return NULL;	
	}

	if (sub && sub->type == st_list && sub->op4.lval->h && !((stmt*)sub->op4.lval->h->data)->nrcols) {
		list *newl = sa_list(sql->sa);
		node *n;

		for(n=sub->op4.lval->h; n; n = n->next) {
			const char *cname = column_name(sql->sa, n->data);
			const char *tname = table_name(sql->sa, n->data);
			stmt *s = column(sql->sa, n->data);

			s = stmt_alias(sql->sa, s, tname, cname );
			append(newl, s);
		}
		sub = stmt_list(sql->sa, newl);
	}

	/* groupby columns */

	/* Keep groupby columns, sub that they can be lookup in the aggr list */
	if (rel->r) {
		list *exps = rel->r; 

		for( en = exps->h; en; en = en->next ) {
			sql_exp *e = en->data; 
			stmt *gbcol = exp_bin(sql, e, sub, NULL, NULL, NULL, NULL, NULL); 
	
			if (!gbcol) {
				assert(0);
				return NULL;
			}
			groupby = stmt_group(sql->sa, gbcol, grp, ext, cnt);
			grp = stmt_result(sql->sa, groupby, 0);
			ext = stmt_result(sql->sa, groupby, 1);
			cnt = stmt_result(sql->sa, groupby, 2);
			gbcol = stmt_alias(sql->sa, gbcol, exp_find_rel_name(e), exp_name(e));
			list_append(gbexps, gbcol);
		}
	}
	stmt_group_done(groupby);
	/* now aggregate */
	l = sa_list(sql->sa);
	aggrs = rel->exps;
	cursub = stmt_list(sql->sa, l);
	for( n = aggrs->h; n; n = n->next ) {
		sql_exp *aggrexp = n->data;

		stmt *aggrstmt = NULL;

		/* first look in the current aggr list (l) and group by column list */
		if (l && !aggrstmt && aggrexp->type == e_column) 
			aggrstmt = list_find_column(sql->sa, l, aggrexp->l, aggrexp->r);
		if (gbexps && !aggrstmt && aggrexp->type == e_column) {
			aggrstmt = list_find_column(sql->sa, gbexps, aggrexp->l, aggrexp->r);
			if (aggrstmt && groupby) {
				aggrstmt = stmt_project(sql->sa, ext, aggrstmt);
				if (list_length(gbexps) == 1) 
					aggrstmt->key = 1;
			}
		}

		if (!aggrstmt)
			aggrstmt = exp_bin(sql, aggrexp, sub, NULL, grp, ext, cnt, NULL); 
		/* maybe the aggr uses intermediate results of this group by,
		   therefore we pass the group by columns too 
		 */
		if (!aggrstmt) 
			aggrstmt = exp_bin(sql, aggrexp, sub, cursub, NULL, NULL, NULL, NULL); 
		if (!aggrstmt) {
			assert(0);
			return NULL;
		}

		aggrstmt = stmt_rename(sql, rel, aggrexp, aggrstmt);
		list_append(l, aggrstmt);
	}
	stmt_set_nrcols(cursub);
	return cursub;
}

static stmt *
rel2bin_topn( mvc *sql, sql_rel *rel, list *refs)
{
	sql_exp *oe = NULL, *le = NULL;
	stmt *sub = NULL, *l = NULL, *o = NULL;
	node *n;

	if (rel->l) { /* first construct the sub relation */
		sql_rel *rl = rel->l;

		if (rl->op == op_project) {
			sub = rel2bin_project(sql, rl, refs, rel);
		} else {
			sub = subrel_bin(sql, rl, refs);
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
			l = exp_bin(sql, le, NULL, NULL, NULL, NULL, NULL, NULL);
		if (oe)
			o = exp_bin(sql, oe, NULL, NULL, NULL, NULL, NULL, NULL);

		if (!l) 
			l = stmt_atom_lng_nil(sql->sa);
		if (!o)
			o = stmt_atom_lng(sql->sa, 0);

		sc = column(sql->sa, sc);
		limit = stmt_limit(sql->sa, stmt_alias(sql->sa, sc, tname, cname), o, l, LIMIT_FLAG(0,0,0,0));

		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(sql->sa, sc);
			const char *tname = table_name(sql->sa, sc);
		
			sc = column(sql->sa, sc);
			sc = stmt_project(sql->sa, limit, sc);
			list_append(newl, stmt_alias(sql->sa, sc, tname, cname));
		}
		sub = stmt_list(sql->sa, newl);
	}
	return sub;
}

static stmt *
rel2bin_sample( mvc *sql, sql_rel *rel, list *refs)
{
	list *newl;
	stmt *sub = NULL, *s = NULL, *sample = NULL;
	node *n;

	if (rel->l) /* first construct the sub relation */
		sub = subrel_bin(sql, rel->l, refs);
	if (!sub)
		return NULL;

	n = sub->op4.lval->h;
	newl = sa_list(sql->sa);

	if (n) {
		stmt *sc = n->data;
		const char *cname = column_name(sql->sa, sc);
		const char *tname = table_name(sql->sa, sc);

		s = exp_bin(sql, rel->exps->h->data, NULL, NULL, NULL, NULL, NULL, NULL);

		if (!s)
			s = stmt_atom_lng_nil(sql->sa);

		sc = column(sql->sa, sc);
		sample = stmt_sample(sql->sa, stmt_alias(sql->sa, sc, tname, cname),s);

		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			const char *cname = column_name(sql->sa, sc);
			const char *tname = table_name(sql->sa, sc);
		
			sc = column(sql->sa, sc);
			sc = stmt_project(sql->sa, sample, sc);
			list_append(newl, stmt_alias(sql->sa, sc, tname, cname));
		}
	}
	sub = stmt_list(sql->sa, newl);
	return sub;
}

stmt *
sql_parse(mvc *m, sql_allocator *sa, char *query, char mode)
{
	mvc *o = NULL;
	stmt *sq = NULL;
	buffer *b;
	char *n;
	int len = _strlen(query);
	stream *buf;

 	if (THRhighwater())
		return sql_error(m, 10, "SELECT: too many nested operators");

	o = MNEW(mvc);
	if (!o)
		return NULL;
	*o = *m;

	m->qc = NULL;

	m->caching = 0;
	m->emode = mode;

	b = (buffer*)GDKmalloc(sizeof(buffer));
	n = GDKmalloc(len + 1 + 1);
	strncpy(n, query, len);
	query = n;
	query[len] = '\n';
	query[len+1] = 0;
	len++;
	buffer_init(b, query, len);
	buf = buffer_rastream(b, "sqlstatement");
	scanner_init( &m->scanner, bstream_create(buf, b->len), NULL);
	m->scanner.mode = LINE_1; 
	bstream_next(m->scanner.rs);

	m->params = NULL;
	m->argc = 0;
	m->sym = NULL;
	m->errstr[0] = '\0';
	m->errstr[ERRSIZE-1] = '\0';

	/* create private allocator */
	m->sa = (sa)?sa:sa_create();

	if (sqlparse(m) || !m->sym) {
		/* oops an error */
		snprintf(m->errstr, ERRSIZE, "An error occurred when executing "
				"internal query: %s", query);
	} else {
		sql_rel *r = rel_semantic(m, m->sym);

		if (r) {
			r = rel_optimizer(m, r);
			sq = rel_bin(m, r);
		}
	}

	GDKfree(query);
	GDKfree(b);
	bstream_destroy(m->scanner.rs);
	if (m->sa && m->sa != sa)
		sa_destroy(m->sa);
	m->sym = NULL;
	{
		char *e = NULL;
		int status = m->session->status;
		int sizevars = m->sizevars, topvars = m->topvars;
		sql_var *vars = m->vars;
		/* cascade list maybe removed */
		list *cascade_action = m->cascade_action;

		if (m->session->status || m->errstr[0]) {
			e = _STRDUP(m->errstr);
			if (!e) {
				_DELETE(o);
				return NULL;
			}
		}
		*m = *o;
		m->sizevars = sizevars;
		m->topvars = topvars;
		m->vars = vars;
		m->session->status = status;
		m->cascade_action = cascade_action;
		if (e) {
			strncpy(m->errstr, e, ERRSIZE);
			m->errstr[ERRSIZE - 1] = '\0';
			_DELETE(e);
		}
	}
	_DELETE(o);
	return sq;
}

static stmt *
stmt_selectnonil( mvc *sql, stmt *col, stmt *s )
{
	sql_subtype *t = tail_type(col);
	stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
	stmt *nn = stmt_uselect2(sql->sa, col, n, n, 3, s);
	nn->flag |= ANTI; 
	return nn;
}

static stmt *
stmt_selectnil( mvc *sql, stmt *col)
{
	sql_subtype *t = tail_type(col);
	stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
	stmt *nn = stmt_uselect2(sql->sa, col, n, n, 3, NULL);
	return nn;
}

static stmt *
insert_check_ukey(mvc *sql, list *inserts, sql_key *k, stmt *idx_inserts)
{
/* pkey's cannot have NULLs, ukeys however can
   current implementation switches on 'NOT NULL' on primary key columns */

	char *msg = NULL;
	stmt *res;

	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subtype *bt = sql_bind_localtype("bit");
	stmt *dels = stmt_tid(sql->sa, k->t);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", lng, lng, bt);

	if (list_length(k->columns) > 1) {
		node *m;
		stmt *s = list_fetch(inserts, 0), *ins = s->op1;
		sql_subaggr *sum;
		stmt *ssum = NULL;
		stmt *col = NULL;

		s = ins;
		/* 1st stage: find out if original contains same values */
		if (s->key && s->nrcols == 0) {
			s = NULL;
			if (k->idx && hash_index(k->idx->type))
				s = stmt_uselect(sql->sa, stmt_idx(sql, k->idx, dels), idx_inserts, cmp_equal, s);
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *cs = list_fetch(inserts, c->c->colnr); 

				col = stmt_col(sql, c->c, dels);
				if ((k->type == ukey) && stmt_has_null(col)) {
					stmt *nn = stmt_selectnonil(sql, col, s);
					s = stmt_uselect( sql->sa, col, cs->op1, cmp_equal, nn);
				} else {
					s = stmt_uselect( sql->sa, col, cs->op1, cmp_equal, s);
				}
			}
		} else {
			list *lje = sa_list(sql->sa);
			list *rje = sa_list(sql->sa);
			if (k->idx && hash_index(k->idx->type)) {
				list_append(lje, stmt_idx(sql, k->idx, dels));
				list_append(rje, idx_inserts);
			}
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *cs = list_fetch(inserts, c->c->colnr); 

				col = stmt_col(sql, c->c, dels);
				list_append(lje, col);
				list_append(rje, cs->op1);
			}
			s = releqjoin(sql, lje, rje, 1 /* hash used */, cmp_equal, 0);
			s = stmt_result(sql->sa, s, 0);
		}
		s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0), stmt_atom_lng(sql->sa, 0), ne);

		/* 2e stage: find out if inserted are unique */
		if ((!idx_inserts && ins->nrcols) || (idx_inserts && idx_inserts->nrcols)) {	/* insert columns not atoms */
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);
			stmt *orderby_ids = NULL, *orderby_grp = NULL;

			/* implementation uses subsort key check */
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *orderby;
				stmt *cs = list_fetch(inserts, c->c->colnr); 

				if (orderby_grp)
					orderby = stmt_reorder(sql->sa, cs->op1, 1, orderby_ids, orderby_grp);
				else
					orderby = stmt_order(sql->sa, cs->op1, 1);
				orderby_ids = stmt_result(sql->sa, orderby, 1);
				orderby_grp = stmt_result(sql->sa, orderby, 2);
			}

			if (!orderby_grp || !orderby_ids)
				return NULL;

			sum = sql_bind_aggr(sql->sa, sql->session->schema, "not_unique", tail_type(orderby_grp));
			ssum = stmt_aggr(sql->sa, orderby_grp, NULL, NULL, sum, 1, 0);
			/* combine results */
			s = stmt_binop(sql->sa, s, ssum, or);
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, "INSERT INTO: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "INSERT INTO: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(sql->sa, s, msg, 00001);
	} else {		/* single column key */
		sql_kc *c = k->columns->h->data;
		stmt *s = list_fetch(inserts, c->c->colnr), *h = s->op1;

		s = stmt_col(sql, c->c, dels);
		if ((k->type == ukey) && stmt_has_null(s)) {
			stmt *nn = stmt_selectnonil(sql, s, NULL);
			s = stmt_project(sql->sa, nn, s);
		}
		if (h->nrcols) {
			s = stmt_join(sql->sa, s, h, cmp_equal);
			/* s should be empty */
			s = stmt_result(sql->sa, s, 0);
			s = stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0);
		} else {
			s = stmt_uselect(sql->sa, s, h, cmp_equal, NULL);
			/* s should be empty */
			s = stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0);
		}
		/* s should be empty */
		s = stmt_binop(sql->sa, s, stmt_atom_lng(sql->sa, 0), ne);

		/* 2e stage: find out if inserts are unique */
		if (h->nrcols) {	/* insert multiple atoms */
			sql_subaggr *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);
			stmt *ssum, *ss;

			stmt *g = list_fetch(inserts, c->c->colnr), *ins = g->op1;

			/* inserted vaules may be null */
			if ((k->type == ukey) && stmt_has_null(ins)) {
				stmt *nn = stmt_selectnonil(sql, ins, NULL);
				ins = stmt_project(sql->sa, nn, ins);
			}
		
			g = stmt_group(sql->sa, ins, NULL, NULL, NULL);
			stmt_group_done(g);
			ss = stmt_result(sql->sa, g, 2); /* use count */
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_aggr(sql->sa, sql->session->schema, "sum", lng);
			ssum = stmt_aggr(sql->sa, ss, NULL, NULL, sum, 1, 0);
			ssum = sql_Nop_(sql, "ifthenelse", sql_unop_(sql, NULL, "isnull", ssum), stmt_atom_lng(sql->sa, 0), ssum, NULL);
			count_sum = stmt_binop(sql->sa, check_types(sql, tail_type(ssum), stmt_aggr(sql->sa, ss, NULL, NULL, cnt, 1, 0), type_equal), ssum, ne);

			/* combine results */
			s = stmt_binop(sql->sa, s, count_sum, or);
		}
		if (k->type == pkey) {
			msg = sa_message( sql->sa,"INSERT INTO: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "INSERT INTO: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(sql->sa, s, msg, 00001);
	}
	return res;
}

static stmt *
insert_check_fkey(mvc *sql, list *inserts, sql_key *k, stmt *idx_inserts, stmt *pin)
{
	char *msg = NULL;
	stmt *cs = list_fetch(inserts, 0), *s = cs->op1;
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", lng, lng, bt);

	(void) sql;		/* unused! */

	if (pin && list_length(pin->op4.lval)) 
		s = pin->op4.lval->h->data;
	if (s->key && s->nrcols == 0) {
		s = stmt_binop(sql->sa, stmt_aggr(sql->sa, idx_inserts, NULL, NULL, cnt, 1, 0), stmt_atom_lng(sql->sa, 1), ne);
	} else {
		/* releqjoin.count <> inserts[col1].count */
		s = stmt_binop(sql->sa, stmt_aggr(sql->sa, idx_inserts, NULL, NULL, cnt, 1, 0), stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0), ne);
	}

	/* s should be empty */
	msg = sa_message(sql->sa, "INSERT INTO: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(sql->sa, s, msg, 00001);
}

static stmt *
sql_insert_key(mvc *sql, list *inserts, sql_key *k, stmt *idx_inserts, stmt *pin)
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
		return insert_check_ukey(sql, inserts, k, idx_inserts );
	} else {		/* foreign keys */
		return insert_check_fkey(sql, inserts, k, idx_inserts, pin );
	}
}

static void
sql_stack_add_inserted( mvc *sql, const char *name, sql_table *t) 
{
	sql_rel *r = rel_basetable(sql, t, name );
		
	stack_push_rel_view(sql, name, r);
}

static int
sql_insert_triggers(mvc *sql, sql_table *t, list *l)
{
	node *n;
	int res = 1;

	if (!t->triggers.set)
		return res;

	for (n = t->triggers.set->h; n; n = n->next) {
		sql_trigger *trigger = n->data;

		stack_push_frame(sql, "OLD-NEW");
		if (trigger->event == 0) { 
			stmt *s = NULL;
			const char *n = trigger->new_name;

			/* add name for the 'inserted' to the stack */
			if (!n) n = "new"; 
	
			sql_stack_add_inserted(sql, n, t);
			s = sql_parse(sql, sql->sa, trigger->statement, m_instantiate);
			
			if (!s) 
				return 0;
			if (trigger -> time )
				list_append(l, s);
			else
				list_prepend(l, s);
		}
		stack_pop_frame(sql);
	}
	return res;
}

static void 
sql_insert_check_null(mvc *sql, sql_table *t, list *inserts, list *l) 
{
	node *m, *n;
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);

	for (n = t->columns.set->h, m = inserts->h; n && m; 
		n = n->next, m = m->next) {
		stmt *i = m->data;
		sql_column *c = n->data;

		if (!c->null) {
			stmt *s = i->op1;
			char *msg = NULL;

			if (!(s->key && s->nrcols == 0)) {
				s = stmt_selectnil(sql, i->op1);
				s = stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0);
			} else {
				sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->type, NULL, F_FUNC);

				s = stmt_unop(sql->sa, i->op1, isnil);
			}
			msg = sa_message(sql->sa, "INSERT INTO: NOT NULL constraint violated for column %s.%s", c->t->base.name, c->base.name);
			s = stmt_exception(sql->sa, s, msg, 00001);

			list_prepend(l, s);
		}
	}
}

static stmt *
rel2bin_insert( mvc *sql, sql_rel *rel, list *refs)
{
	list *newl, *l;
	stmt *inserts = NULL, *insert = NULL, *s, *ddl = NULL, *pin = NULL;
	int idx_ins = 0, constraint = 1;
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
		ddl = subrel_bin(sql, tr, refs);
		if (!ddl)
			return NULL;
		t = rel_ddl_table_get(tr);
	}

	if (rel->r) /* first construct the inserts relation */
		inserts = subrel_bin(sql, rel->r, refs);

	if (!inserts)
		return NULL;	

	if (idx_ins)
		pin = refs_find_rel(refs, prel);

	newl = sa_list(sql->sa);
	for (n = t->columns.set->h, m = inserts->op4.lval->h; 
		n && m; n = n->next, m = m->next) {

		stmt *ins = m->data;
		sql_column *c = n->data;

		insert = ins = stmt_append_col(sql->sa, c, ins);
		if (rel->flag&UPD_LOCKED) /* fake append (done in the copy into) */
			ins->flag = 1;
		list_append(newl, ins);
	}
	l = sa_list(sql->sa);

	if (t->idxs.set)
	for (n = t->idxs.set->h; n && m; n = n->next, m = m->next) {
		stmt *is = m->data;
		sql_idx *i = n->data;

		if ((hash_index(i->type) && list_length(i->columns) <= 1) ||
		    i->type == no_idx)
			is = NULL;
		if (i->key && constraint) {
			stmt *ckeys = sql_insert_key(sql, newl, i->key, is, pin);

			list_append(l, ckeys);
		}
		if (!insert)
			insert = is;
		if (is)
			is = stmt_append_idx(sql->sa, i, is);
		if ((rel->flag&UPD_LOCKED) && is) /* fake append (done in the copy into) */
			is->flag = 1;
		if (is)
			list_append(newl, is);
	}
	if (!insert)
		return NULL;

	if (constraint)
		sql_insert_check_null(sql, t, newl, l);
	l = list_append(l, stmt_list(sql->sa, newl));
	if (!sql_insert_triggers(sql, t, l)) 
		return sql_error(sql, 02, "INSERT INTO: triggers failed for table '%s'", t->base.name);
	if (insert->op1->nrcols == 0) {
		s = stmt_atom_lng(sql->sa, 1);
	} else {
		s = stmt_aggr(sql->sa, insert->op1, NULL, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1, 0);
	}
	if (ddl)
		list_prepend(l, ddl);
	else
		list_append(l, stmt_affected_rows(sql->sa, s));
	return stmt_list(sql->sa, l);
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

static stmt ** 
table_update_stmts(mvc *sql, sql_table *t, int *Len)
{
	stmt **updates;
	int i, len = list_length(t->columns.set);
	node *m;

	*Len = len;
	updates = SA_NEW_ARRAY(sql->sa, stmt *, len);
	for (m = t->columns.set->h, i = 0; m; m = m->next, i++) {
		sql_column *c = m->data;

		/* update the column number, for correct array access */
		c->colnr = i;
		updates[i] = NULL;
	}
	return updates;
}

static stmt *
update_check_ukey(mvc *sql, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol)
{
	char *msg = NULL;
	stmt *res = NULL;

	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne;

	(void)tids;
	ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", lng, lng, bt);
	if (list_length(k->columns) > 1) {
		stmt *dels = stmt_tid(sql->sa, k->t);
		node *m;
		stmt *s = NULL;

		/* 1st stage: find out if original (without the updated) 
			do not contain the same values as the updated values. 
			This is done using a relation join and a count (which 
			should be zero)
	 	*/
		if (!isNew(k)) {
			stmt *nu_tids = stmt_tdiff(sql->sa, dels, tids); /* not updated ids */
			list *lje = sa_list(sql->sa);
			list *rje = sa_list(sql->sa);

			if (k->idx && hash_index(k->idx->type)) {
				list_append(lje, stmt_idx(sql, k->idx, nu_tids));
				list_append(rje, idx_updates);
			}
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd;

				assert(updates);
				if (updates[c->c->colnr]) {
					upd = updates[c->c->colnr]->op2;
				} else {
					upd = stmt_project(sql->sa, tids, stmt_col(sql, c->c, dels));
				}
				list_append(lje, stmt_col(sql, c->c, nu_tids));
				list_append(rje, upd);
			}
			s = releqjoin(sql, lje, rje, 1 /* hash used */, cmp_equal, 0);
			s = stmt_result(sql->sa, s, 0);
			s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0), stmt_atom_lng(sql->sa, 0), ne);
		}

		/* 2e stage: find out if the updated are unique */
		if (!updates || updates[updcol]->op2->nrcols) {	/* update columns not atoms */
			sql_subaggr *sum;
			stmt *count_sum = NULL, *ssum;
			stmt *g = NULL, *grp = NULL, *ext = NULL, *Cnt = NULL;
			stmt *cand = NULL;
			stmt *ss;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);

			/* also take the hopefully unique hash keys, to reduce
			   (re)group costs */
			if (k->idx && hash_index(k->idx->type)) {
				g = stmt_group(sql->sa, idx_updates, grp, ext, Cnt);
				grp = stmt_result(sql->sa, g, 0);
				ext = stmt_result(sql->sa, g, 1);
				Cnt = stmt_result(sql->sa, g, 2);

				/* continue only with groups with a cnt > 1 */
				cand = stmt_uselect(sql->sa, Cnt, stmt_atom_lng(sql->sa, 1), cmp_gt, NULL);
				/* project cand on ext and Cnt */
				Cnt = stmt_project(sql->sa, cand, Cnt);
				ext = stmt_project(sql->sa, cand, ext);

				/* join groups with extend to retrieve all oid's of the original
				 * bat that belong to a group with Cnt >1 */
				g = stmt_join(sql->sa, grp, ext, cmp_equal);
				cand = stmt_result(sql->sa, g, 0);
				grp = stmt_project(sql->sa, cand, grp);
			}

			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd;

				if (updates && updates[c->c->colnr]) {
					upd = updates[c->c->colnr]->op2;
				} else if (updates) {
					upd = updates[updcol]->op1;
					upd = stmt_project(sql->sa, upd, stmt_col(sql, c->c, dels));
				} else {
					upd = stmt_project(sql->sa, tids, stmt_col(sql, c->c, dels));
				}

				/* apply cand list first */
				if (cand)
					upd = stmt_project(sql->sa, cand, upd);

				/* remove nulls */
				if ((k->type == ukey) && stmt_has_null(upd)) {
					stmt *nn = stmt_selectnonil(sql, upd, NULL);
					upd = stmt_project(sql->sa, nn, upd);
					if (grp)
						grp = stmt_project(sql->sa, nn, grp);
					if (cand)
						cand = stmt_project(sql->sa, nn, cand);
				}

				/* apply group by on groups with Cnt > 1 */
				g = stmt_group(sql->sa, upd, grp, ext, Cnt);
				grp = stmt_result(sql->sa, g, 0);
				ext = stmt_result(sql->sa, g, 1);
				Cnt = stmt_result(sql->sa, g, 2);
			}
			stmt_group_done(g);
			ss = Cnt; /* use count */
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_aggr(sql->sa, sql->session->schema, "sum", lng);
			ssum = stmt_aggr(sql->sa, ss, NULL, NULL, sum, 1, 0);
			ssum = sql_Nop_(sql, "ifthenelse", sql_unop_(sql, NULL, "isnull", ssum), stmt_atom_lng(sql->sa, 0), ssum, NULL);
			count_sum = stmt_binop(sql->sa, stmt_aggr(sql->sa, ss, NULL, NULL, cnt, 1, 0), check_types(sql, lng, ssum, type_equal), ne);

			/* combine results */
			if (s) 
				s = stmt_binop(sql->sa, s, count_sum, or);
			else
				s = count_sum;
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, "UPDATE: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "UPDATE: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(sql->sa, s, msg, 00001);
	} else {		/* single column key */
		stmt *dels = stmt_tid(sql->sa, k->t);
		sql_kc *c = k->columns->h->data;
		stmt *s = NULL, *h = NULL, *o;

		/* s should be empty */
		if (!isNew(k)) {
			stmt *nu_tids = stmt_tdiff(sql->sa, dels, tids); /* not updated ids */
			assert (updates);

			h = updates[c->c->colnr]->op2;
			o = stmt_col(sql, c->c, nu_tids);
			s = stmt_join(sql->sa, o, h, cmp_equal);
			s = stmt_result(sql->sa, s, 0);
			s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0), stmt_atom_lng(sql->sa, 0), ne);
		}

		/* 2e stage: find out if updated are unique */
		if (!h || h->nrcols) {	/* update columns not atoms */
			sql_subaggr *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);
			stmt *ssum, *ss;
			stmt *upd;
			stmt *g;

			if (updates) {
 				upd = updates[c->c->colnr]->op2;
			} else {
 				upd = stmt_col(sql, c->c, dels);
			}

			/* remove nulls */
			if ((k->type == ukey) && stmt_has_null(upd)) {
				stmt *nn = stmt_selectnonil(sql, upd, NULL);
				upd = stmt_project(sql->sa, nn, upd);
			}

			g = stmt_group(sql->sa, upd, NULL, NULL, NULL);
			stmt_group_done(g);
			ss = stmt_result(sql->sa, g, 2); /* use count */

			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_aggr(sql->sa, sql->session->schema, "sum", lng);
			ssum = stmt_aggr(sql->sa, ss, NULL, NULL, sum, 1, 0);
			ssum = sql_Nop_(sql, "ifthenelse", sql_unop_(sql, NULL, "isnull", ssum), stmt_atom_lng(sql->sa, 0), ssum, NULL);
			count_sum = stmt_binop(sql->sa, check_types(sql, tail_type(ssum), stmt_aggr(sql->sa, ss, NULL, NULL, cnt, 1, 0), type_equal), ssum, ne);

			/* combine results */
			if (s)
				s = stmt_binop(sql->sa, s, count_sum, or);
			else
				s = count_sum;
		}

		if (k->type == pkey) {
			msg = sa_message(sql->sa, "UPDATE: PRIMARY KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
		} else {
			msg = sa_message(sql->sa, "UPDATE: UNIQUE constraint '%s.%s' violated", k->t->base.name, k->base.name);
		}
		res = stmt_exception(sql->sa, s, msg, 00001);
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
update_check_fkey(mvc *sql, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol, stmt *pup)
{
	char *msg = NULL;
	stmt *s, *cur, *null = NULL, *cntnulls;
	sql_subtype *lng = sql_bind_localtype("lng"), *bt = sql_bind_localtype("bit");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", lng, lng, bt);
	sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);
	node *m;

	if (!idx_updates)
		return NULL;
	/* releqjoin.count <> updates[updcol].count */
	if (pup && list_length(pup->op4.lval)) {
		cur = pup->op4.lval->h->data;
	} else if (updates) {
		cur = updates[updcol]->op2;
	} else {
		sql_kc *c = k->columns->h->data;
		stmt *dels = stmt_tid(sql->sa, k->t);
		cur = stmt_col(sql, c->c, dels);
	}
	s = stmt_binop(sql->sa, stmt_aggr(sql->sa, idx_updates, NULL, NULL, cnt, 1, 0), stmt_aggr(sql->sa, cur, NULL, NULL, cnt, 1, 0), ne);

	for (m = k->columns->h; m; m = m->next) {
		sql_kc *c = m->data;

		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		if (c->c->null) {
			stmt *upd, *nn;

			if (updates && updates[c->c->colnr]) {
				upd = updates[c->c->colnr]->op2;
			} else if (updates && updcol >= 0) {
				upd = updates[updcol]->op1;
				upd = stmt_project(sql->sa, upd, stmt_col(sql, c->c, tids));
			} else { /* created idx/key using alter */ 
				upd = stmt_col(sql, c->c, tids);
			}
			nn = stmt_selectnil(sql, upd);
			if (null)
				null = stmt_tunion(sql->sa, null, nn);
			else
				null = nn;
		}
	}
	if (null) {
		cntnulls = stmt_aggr(sql->sa, null, NULL, NULL, cnt, 1, 0); 
	} else {
		cntnulls = stmt_atom_lng(sql->sa, 0);
	}
	s = stmt_binop(sql->sa, s, 
		stmt_binop(sql->sa, stmt_aggr(sql->sa, stmt_selectnil(sql, idx_updates), NULL, NULL, cnt, 1, 0), cntnulls , ne), or);

	/* s should be empty */
	msg = sa_message(sql->sa, "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(sql->sa, s, msg, 00001);
}

static stmt *
join_updated_pkey(mvc *sql, sql_key * k, stmt *tids, stmt **updates, int updcol)
{
	char *msg = NULL;
	int nulls = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt *s = NULL, *dels = stmt_tid(sql->sa, rk->t), *fdels, *cnteqjoin;
	stmt *null = NULL, *rows;
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", lng, lng, bt);
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	fdels = stmt_tid(sql->sa, k->idx->t);
	rows = stmt_idx(sql, k->idx, fdels);

	rows = stmt_join(sql->sa, rows, tids, cmp_equal); /* join over the join index */
	rows = stmt_result(sql->sa, rows, 0);

	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd, *col;

		if (updates[c->c->colnr]) {
			upd = updates[c->c->colnr]->op2;
		} else {
			upd = updates[updcol]->op1;
			upd = stmt_project(sql->sa, upd, stmt_col(sql, c->c, dels));
		}
		if (c->c->null) {	/* new nulls (MATCH SIMPLE) */
			stmt *nn = stmt_selectnil(sql, upd);
			if (null)
				null = stmt_tunion(sql->sa, null, nn);
			else
				null = nn;
			nulls = 1;
		}
		col = stmt_col(sql, fc->c, rows);
		list_append(lje, upd);
		list_append(rje, col);
	}
	s = releqjoin(sql, lje, rje, 1 /* hash used */, cmp_equal, 0);
	s = stmt_result(sql->sa, s, 0);

	/* add missing nulls */
	cnteqjoin = stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0);
	if (nulls) {
		sql_subfunc *add = sql_bind_func_result(sql->sa, sql->session->schema, "sql_add", lng, lng, lng);
		cnteqjoin = stmt_binop(sql->sa, cnteqjoin, stmt_aggr(sql->sa, null, NULL, NULL, cnt, 1, 0), add);
	}

	/* releqjoin.count <> updates[updcol].count */
	s = stmt_binop(sql->sa, cnteqjoin, stmt_aggr(sql->sa, rows, NULL, NULL, cnt, 1, 0), ne);

	/* s should be empty */
	msg = sa_message(sql->sa, "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(sql->sa, s, msg, 00001);
}

static list * sql_update(mvc *sql, sql_table *t, stmt *rows, stmt **updates);

static stmt*
sql_delete_set_Fkeys(mvc *sql, sql_key *k, stmt *ftids /* to be updated rows of fkey table */, int action)
{
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
				char *msg = sa_message(sql->sa, "select %s;", fc->c->def);
				sq = rel_parse_value(sql, msg, sql->emode);
				if (!sq) 
					return NULL;
				upd = sq;
			}  else {
				upd = stmt_atom(sql->sa, atom_general(sql->sa, &fc->c->type, NULL));
			}
		} else {
			upd = stmt_atom(sql->sa, atom_general(sql->sa, &fc->c->type, NULL));
		}
		
		if (!upd || (upd = check_types(sql, &fc->c->type, upd, type_equal)) == NULL) 
			return NULL;

		if (upd->nrcols <= 0) 
			upd = stmt_const(sql->sa, ftids, upd);
		
		new_updates[fc->c->colnr] = stmt_update_col(sql->sa, fc->c, ftids, upd);
	}

	if ((l = sql_update(sql, t, ftids, new_updates)) == NULL) 
		return NULL;
	return stmt_list(sql->sa, l);
}

static stmt*
sql_update_cascade_Fkeys(mvc *sql, sql_key *k, stmt *utids, stmt **updates, int action)
{
	list *l = NULL;
	int len = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt **new_updates;
	stmt *rows;
	sql_table *t = mvc_bind_table(sql, k->t->s, k->t->base.name);
	stmt *ftids, *upd_ids;

	ftids = stmt_tid(sql->sa, k->idx->t);
	rows = stmt_idx(sql, k->idx, ftids);

	rows = stmt_join(sql->sa, rows, utids, cmp_equal); /* join over the join index */
	upd_ids = stmt_result(sql->sa, rows, 1);
	rows = stmt_result(sql->sa, rows, 0);
	rows = stmt_project(sql->sa, rows, ftids);
		
	new_updates = table_update_stmts(sql, t, &len);
	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd = NULL;

		if (!updates[c->c->colnr]) {
			continue;
		} else if (action == ACT_CASCADE) {
			upd = updates[c->c->colnr]->op2;
		} else if (action == ACT_SET_DEFAULT) {
			if (fc->c->def) {
				stmt *sq;
				char *msg = sa_message(sql->sa, "select %s;", fc->c->def);
				sq = rel_parse_value(sql, msg, sql->emode);
				if (!sq) 
					return NULL;
				upd = sq;
			} else {
				upd = stmt_atom(sql->sa, atom_general(sql->sa, &fc->c->type, NULL));
			}
		} else if (action == ACT_SET_NULL) {
			upd = stmt_atom(sql->sa, atom_general(sql->sa, &fc->c->type, NULL));
		}

		if (!upd || (upd = check_types(sql, &fc->c->type, upd, type_equal)) == NULL) 
			return NULL;

		if (upd->nrcols <= 0) 
			upd = stmt_const(sql->sa, upd_ids, upd);
		else
			upd = stmt_project(sql->sa, upd_ids, upd);
		
		new_updates[fc->c->colnr] = stmt_update_col(sql->sa, fc->c, rows, upd);
	}

	if ((l = sql_update(sql, t, rows, new_updates)) == NULL) 
		return NULL;
	return stmt_list(sql->sa, l);
}


static void 
cascade_ukey(mvc *sql, stmt **updates, sql_key *k, stmt *tids, int updcol, list *cascade) 
{
	sql_ukey *uk = (sql_ukey*)k;

	if (uk->keys && list_length(uk->keys) > 0) {
		node *n;
		for(n = uk->keys->h; n; n = n->next) {
			sql_key *fk = n->data;
			stmt *s = NULL;

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
					s = sql_update_cascade_Fkeys(sql, fk, tids, updates, ((sql_fkey*)fk)->on_update);
					list_append(cascade, s);
					break;
				default:	/*RESTRICT*/
					s = join_updated_pkey(sql, fk, tids, updates, updcol);
					list_append(cascade, s);
			}
		}
	}
}

static void
sql_update_check_key(mvc *sql, stmt **updates, sql_key *k, stmt *tids, stmt *idx_updates, int updcol, list *l, list *cascade, stmt *pup)
{
	stmt *ckeys;

	if (k->type == pkey || k->type == ukey) {
		ckeys = update_check_ukey(sql, updates, k, tids, idx_updates, updcol);
		if (cascade)
			cascade_ukey(sql, updates, k, tids, updcol, cascade);
	} else { /* foreign keys */
		ckeys = update_check_fkey(sql, updates, k, tids, idx_updates, updcol, pup);
	}
	list_append(l, ckeys);
}

static stmt *
hash_update(mvc *sql, sql_idx * i, stmt **updates, int updcol)
{
	/* calculate new value */
	node *m;
	sql_subtype *it, *lng;
	int bits = 1 + ((sizeof(lng)*8)-1)/(list_length(i->columns)+1);
	stmt *h = NULL, *tids;

	if (list_length(i->columns) <= 1)
		return NULL;

	tids = stmt_tid(sql->sa, i->t);
	it = sql_bind_localtype("int");
	lng = sql_bind_localtype("lng");
	for (m = i->columns->h; m; m = m->next ) {
		sql_kc *c = m->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr]->op2;
		} else if (updates && updcol >= 0) {
			upd = updates[updcol]->op1;
			upd = stmt_project(sql->sa, upd, stmt_col(sql, c->c, tids));
		} else { /* created idx/key using alter */ 
			upd = stmt_col(sql, c->c, tids);
		}

		if (h && i->type == hash_idx)  { 
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", lng, it, &c->c->type, lng);

			h = stmt_Nop(sql->sa, stmt_list( sql->sa, list_append( list_append(
				list_append(sa_list(sql->sa), h), 
				stmt_atom_int(sql->sa, bits)),  upd)),
				xor);
		} else if (h)  { 
			stmt *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql->sa, sql->session->schema, "left_shift", lng, it, lng);
			sql_subfunc *lor = sql_bind_func_result(sql->sa, sql->session->schema, "bit_or", lng, lng, lng);
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, lng);

			h = stmt_binop(sql->sa, h, stmt_atom_int(sql->sa, bits), lsh); 
			h2 = stmt_unop(sql->sa, upd, hf);
			h = stmt_binop(sql->sa, h, h2, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, lng);
			h = stmt_unop(sql->sa, upd, hf);
			if (i->type == oph_idx)
				break;
		}
	}
	return h;
}

static stmt *
join_idx_update(mvc *sql, sql_idx * i, stmt *ftids, stmt **updates, int updcol)
{
	node *m, *o;
	sql_key *rk = &((sql_fkey *) i->key)->rkey->k;
	stmt *s = NULL, *ptids = stmt_tid(sql->sa, rk->t), *l, *r;
	list *lje = sa_list(sql->sa);
	list *rje = sa_list(sql->sa);

	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr]->op2;
		} else if (updates && updcol >= 0) {
			upd = updates[updcol]->op1;
			upd = stmt_project(sql->sa, upd, stmt_col(sql, c->c, ftids));
		} else { /* created idx/key using alter */ 
			upd = stmt_col(sql, c->c, ftids);
		}

		list_append(lje, check_types(sql, &rc->c->type, upd, type_equal));
		list_append(rje, stmt_col(sql, rc->c, ptids));
	}
	s = releqjoin(sql, lje, rje, 0 /* use hash */, cmp_equal, 0);
	l = stmt_result(sql->sa, s, 0);
	r = stmt_result(sql->sa, s, 1);
	r = stmt_project(sql->sa, r, ptids);
	return stmt_left_project(sql->sa, ftids, l, r);
}

static list *
update_idxs_and_check_keys(mvc *sql, sql_table *t, stmt *rows, stmt **updates, list *l, list **cascades)
{
	node *n;
	int updcol;
	list *idx_updates = sa_list(sql->sa);

	if (!t->idxs.set)
		return idx_updates;

	*cascades = sa_list(sql->sa);
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
			is = hash_update(sql, i, updates, updcol);
		} else if (i->type == join_idx) {
			if (updcol < 0)
				return NULL;
			is = join_idx_update(sql, i, rows, updates, updcol);
		}
		if (i->key) {
			if (!(sql->cascade_action && list_find_id(sql->cascade_action, i->key->base.id))) {
				int *local_id = SA_NEW(sql->sa, int);
				if (!sql->cascade_action) 
					sql->cascade_action = sa_list(sql->sa);
				
				*local_id = i->key->base.id;
				list_append(sql->cascade_action, local_id);
				sql_update_check_key(sql, updates, i->key, rows, is, updcol, l, *cascades, NULL);
			}
		}
		if (is) 
			list_append(idx_updates, stmt_update_idx(sql->sa, i, rows, is));
	}
	return idx_updates;
}

static void
sql_stack_add_updated(mvc *sql, const char *on, const char *nn, sql_table *t)
{
	sql_rel *or = rel_basetable(sql, t, on );
	sql_rel *nr = rel_basetable(sql, t, nn );
		
	stack_push_rel_view(sql, on, or);
	stack_push_rel_view(sql, nn, nr);
}

static int
sql_update_triggers(mvc *sql, sql_table *t, list *l, int time )
{
	node *n;
	int res = 1;

	if (!t->triggers.set)
		return res;

	for (n = t->triggers.set->h; n; n = n->next) {
		sql_trigger *trigger = n->data;

		stack_push_frame(sql, "OLD-NEW");
		if (trigger->event == 2 && trigger->time == time) {
			stmt *s = NULL;
	
			/* add name for the 'inserted' to the stack */
			const char *n = trigger->new_name;
			const char *o = trigger->old_name;
	
			if (!n) n = "new"; 
			if (!o) o = "old"; 
	
			sql_stack_add_updated(sql, o, n, t);
			s = sql_parse(sql, sql->sa, trigger->statement, m_instantiate);
			if (!s) 
				return 0;
			list_append(l, s);
		}
		stack_pop_frame(sql);
	}
	return res;
}


static void
sql_update_check_null(mvc *sql, sql_table *t, stmt **updates, list *l)
{
	node *n;
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);

	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		if (updates[c->colnr] && !c->null) {
			stmt *s = updates[c->colnr]->op2;
			char *msg = NULL;

			if (!(s->key && s->nrcols == 0)) {
				s = stmt_selectnil(sql, updates[c->colnr]->op2);
				s = stmt_aggr(sql->sa, s, NULL, NULL, cnt, 1, 0);
			} else {
				sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->type, NULL, F_FUNC);

				s = stmt_unop(sql->sa, updates[c->colnr]->op2, isnil);
			}
			msg = sa_message(sql->sa, "UPDATE: NOT NULL constraint violated for column '%s.%s'", c->t->base.name, c->base.name);
			s = stmt_exception(sql->sa, s, msg, 00001);

			list_append(l, s);
		}
	}
}

static list *
sql_update(mvc *sql, sql_table *t, stmt *rows, stmt **updates)
{
	list *idx_updates = NULL, *cascades = NULL;
	int i, nr_cols = list_length(t->columns.set);
	list *l = sa_list(sql->sa);

	sql_update_check_null(sql, t, updates, l);

	/* check keys + get idx */
	idx_updates = update_idxs_and_check_keys(sql, t, rows, updates, l, &cascades);
	if (!idx_updates) {
		return sql_error(sql, 02, "UPDATE: failed to update indexes for table '%s'", t->base.name);
	}

/* before */
	if (!sql_update_triggers(sql, t, l, 0)) 
		return sql_error(sql, 02, "UPDATE: triggers failed for table '%s'", t->base.name);

/* apply updates */
	list_merge(l, idx_updates, NULL);
	for (i = 0; i < nr_cols; i++) 
		if (updates[i])
			list_append(l, updates[i]);

/* after */
	if (!sql_update_triggers(sql, t, l, 1)) 
		return sql_error(sql, 02, "UPDATE: triggers failed for table '%s'", t->base.name);

/* cascade */
	list_merge(l, cascades, NULL);
	return l;
}

/* updates with empty list is alter with create idx or keys */
static stmt *
rel2bin_update( mvc *sql, sql_rel *rel, list *refs)
{
	stmt *update = NULL, **updates = NULL, *tids, *s, *ddl = NULL, *pup = NULL;
	list *l = sa_list(sql->sa), *cascades = NULL;
	int nr_cols, updcol, i, idx_ups = 0;
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
		ddl = subrel_bin(sql, tr, refs);
		if (!ddl)
			return NULL;
		t = rel_ddl_table_get(tr);

		/* no columns to update (probably an new pkey!) */
		if (!rel->exps) 
			return ddl;
	}

	if (rel->r) /* first construct the update relation */
		update = subrel_bin(sql, rel->r, refs);

	if (!update)
		return NULL;

	if (idx_ups)
		pup = refs_find_rel(refs, prel);

	updates = table_update_stmts(sql, t, &nr_cols);
	tids = update->op4.lval->h->data;

	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_column *c = find_sql_column(t, ce->name);

		if (c) {
			stmt *s = bin_find_column(sql->sa, update, ce->l, ce->r);
			updates[c->colnr] = stmt_update_col(sql->sa,  c, tids, s);
		}
	}
	sql_update_check_null(sql, t, updates, l);

	/* check keys + get idx */
	cascades = sa_list(sql->sa);
	updcol = first_updated_col(updates, list_length(t->columns.set));
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_idx *i = find_sql_idx(t, ce->name+1);

		if (i) {
			stmt *update_idx = bin_find_column(sql->sa, update, ce->l, ce->r), *is = NULL;

			if (update_idx)
				is = update_idx;
			if ((hash_index(i->type) && list_length(i->columns) <= 1) || i->type == no_idx) {
				is = NULL;
				update_idx = NULL;
			}
			if (i->key) {
				if (!(sql->cascade_action && list_find_id(sql->cascade_action, i->key->base.id))) {
					int *local_id = SA_NEW(sql->sa, int);
					if (!sql->cascade_action) 
						sql->cascade_action = sa_list(sql->sa);
				
					*local_id = i->key->base.id;
					list_append(sql->cascade_action, local_id);
					sql_update_check_key(sql, (updcol>=0)?updates:NULL, i->key, tids, update_idx, updcol, l, cascades, pup);
				}
			}
			if (is) 
				list_append(l, stmt_update_idx(sql->sa,  i, tids, is));
		}
	}

/* before */
	if (!sql_update_triggers(sql, t, l, 0)) 
		return sql_error(sql, 02, "UPDATE: triggers failed for table '%s'", t->base.name);

/* apply updates */
	for (i = 0; i < nr_cols; i++)
		if (updates[i])
			list_append(l, updates[i]);

/* after */
	if (!sql_update_triggers(sql, t, l, 1)) 
		return sql_error(sql, 02, "UPDATE: triggers failed for table '%s'", t->base.name);

/* cascade */
	list_merge(l, cascades, NULL);
	if (ddl) {
		list_prepend(l, ddl);
	} else {
		s = stmt_aggr(sql->sa, tids, NULL, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1, 0);
		list_append(l, stmt_affected_rows(sql->sa, s));
	}

	if (sql->cascade_action) 
		sql->cascade_action = NULL;
	return stmt_list(sql->sa, l);
}
 
static void
sql_stack_add_deleted(mvc *sql, const char *name, sql_table *t)
{
	sql_rel *r = rel_basetable(sql, t, name );
		
	stack_push_rel_view(sql, name, r);
}

static int
sql_delete_triggers(mvc *sql, sql_table *t, list *l)
{
	node *n;
	int res = 1;

	if (!t->triggers.set)
		return res;

	for (n = t->triggers.set->h; n; n = n->next) {
		sql_trigger *trigger = n->data;

		stack_push_frame(sql, "OLD-NEW");
		if (trigger->event == 1) {
			stmt *s = NULL;
	
			/* add name for the 'deleted' to the stack */
			const char *o = trigger->old_name;
		
			if (!o) o = "old"; 
		
			sql_stack_add_deleted(sql, o, t);
			s = sql_parse(sql, sql->sa, trigger->statement, m_instantiate);

			if (!s) 
				return 0;
			if (trigger -> time )
				list_append(l, s);
			else
				list_prepend(l, s);
		}
		stack_pop_frame(sql);
	}
	return res;
}

static stmt * sql_delete(mvc *sql, sql_table *t, stmt *rows);

static stmt *
sql_delete_cascade_Fkeys(mvc *sql, sql_key *fk, stmt *ftids)
{
	sql_table *t = mvc_bind_table(sql, fk->t->s, fk->t->base.name);
	return sql_delete(sql, t, ftids);
}

static void 
sql_delete_ukey(mvc *sql, stmt *utids /* deleted tids from ukey table */, sql_key *k, list *l) 
{
	sql_ukey *uk = (sql_ukey*)k;

	if (uk->keys && list_length(uk->keys) > 0) {
		sql_subtype *lng = sql_bind_localtype("lng");
		sql_subtype *bt = sql_bind_localtype("bit");
		node *n;
		for(n = uk->keys->h; n; n = n->next) {
			char *msg = NULL;
			sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
			sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", lng, lng, bt);
			sql_key *fk = n->data;
			stmt *s, *tids;

			tids = stmt_tid(sql->sa, fk->idx->t);
			s = stmt_idx(sql, fk->idx, tids);
			s = stmt_join(sql->sa, s, utids, cmp_equal); /* join over the join index */
			s = stmt_result(sql->sa, s, 0);
			tids = stmt_project(sql->sa, s, tids);
			switch (((sql_fkey*)fk)->on_delete) {
				case ACT_NO_ACTION: 
					break;
				case ACT_SET_NULL: 
				case ACT_SET_DEFAULT: 
					s = sql_delete_set_Fkeys(sql, fk, tids, ((sql_fkey*)fk)->on_delete);
					list_prepend(l, s);
					break;
				case ACT_CASCADE: 
					s = sql_delete_cascade_Fkeys(sql, fk, tids);
					list_prepend(l, s);
					break;
				default:	/*RESTRICT*/
					/* The overlap between deleted primaries and foreign should be empty */
					s = stmt_binop(sql->sa, stmt_aggr(sql->sa, tids, NULL, NULL, cnt, 1, 0), stmt_atom_lng(sql->sa, 0), ne);
					msg = sa_message(sql->sa, "DELETE: FOREIGN KEY constraint '%s.%s' violated", fk->t->base.name, fk->base.name);
					s = stmt_exception(sql->sa, s, msg, 00001);
					list_prepend(l, s);
			}
		}
	}
}

static int
sql_delete_keys(mvc *sql, sql_table *t, stmt *rows, list *l)
{
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
				sql_delete_ukey(sql, rows, k, l);
			}
		}
	}
	return res;
}

static stmt * 
sql_delete(mvc *sql, sql_table *t, stmt *rows)
{
	stmt *v, *s = NULL;
	list *l = sa_list(sql->sa);

	if (rows) { 
		sql_subtype to;

		sql_find_subtype(&to, "oid", 0, 0);
		v = rows;
		list_append(l, stmt_delete(sql->sa, t, rows));
	} else { /* delete all */
		/* first column */
		v = stmt_tid(sql->sa, t);
		s = stmt_table_clear(sql->sa, t);
		list_append(l, s);
	}

	if (!sql_delete_triggers(sql, t, l)) 
		return sql_error(sql, 02, "DELETE: triggers failed for table '%s'", t->base.name);
	if (!sql_delete_keys(sql, t, v, l)) 
		return sql_error(sql, 02, "DELETE: failed to delete indexes for table '%s'", t->base.name);
	if (rows) 
		s = stmt_aggr(sql->sa, rows, NULL, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1, 0);
	list_append(l, stmt_affected_rows(sql->sa, s));
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_delete( mvc *sql, sql_rel *rel, list *refs)
{
	stmt *rows = NULL, *delete;
	sql_rel *tr = rel->l;
	sql_table *t = NULL;

	if (tr->op == op_basetable)
		t = tr->l;
	else
		assert(0/*ddl statement*/);

	if (rel->r) { /* first construct the deletes relation */
		rows = subrel_bin(sql, rel->r, refs);
		if (!rows) 
			return NULL;	
	}
	if (rows && rows->type == st_list) {
		stmt *s = rows;
		rows = s->op4.lval->h->data;
	}
	delete = sql_delete(sql, t, rows); 
	if (sql->cascade_action) 
		sql->cascade_action = NULL;
	return delete;
}

#define E_ATOM_INT(e) ((atom*)((sql_exp*)e)->l)->data.val.lval
#define E_ATOM_STRING(e) ((atom*)((sql_exp*)e)->l)->data.val.sval

static stmt *
rel2bin_output(mvc *sql, sql_rel *rel, list *refs) 
{
	node *n;
	const char *tsep, *rsep, *ssep, *ns;
	const char *fn   = NULL;
	stmt *s = NULL, *fns = NULL;
	list *slist = sa_list(sql->sa);

	if (rel->l)  /* first construct the sub relation */
		s = subrel_bin(sql, rel->l, refs);
	if (!s) 
		return NULL;	

	if (!rel->exps){ /* rows affacted */
		list_append(slist, stmt_affected_rows(sql->sa, s));
		return stmt_list(sql->sa, slist);
	}
	n = rel->exps->h;
	tsep = sa_strdup(sql->sa, E_ATOM_STRING(n->data));
	rsep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->data));
	ssep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->data));
	ns   = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->next->data));

	if (n->next->next->next->next) {
		fn = E_ATOM_STRING(n->next->next->next->next->data);
		fns = stmt_atom_string(sql->sa, sa_strdup(sql->sa, fn));
	}
	list_append(slist, stmt_export(sql->sa, s, tsep, rsep, ssep, ns, fns));
	if (s->type == st_list && ((stmt*)s->op4.lval->h->data)->nrcols != 0) {
		stmt *cnt = stmt_aggr(sql->sa, s->op4.lval->h->data, NULL, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1, 0);
		list_append(slist, stmt_affected_rows(sql->sa, cnt));
	} else {
		list_append(slist, stmt_affected_rows(sql->sa, stmt_atom_lng(sql->sa, 1)));
	}
	return stmt_list(sql->sa, slist);
}

static stmt *
rel2bin_list(mvc *sql, sql_rel *rel, list *refs) 
{
	stmt *l = NULL, *r = NULL;
	list *slist = sa_list(sql->sa);

	(void)refs;
	if (rel->l)  /* first construct the sub relation */
		l = subrel_bin(sql, rel->l, refs);
	if (rel->r)  /* first construct the sub relation */
		r = subrel_bin(sql, rel->r, refs);
	if (!l || !r)
		return NULL;
	list_append(slist, l);
	list_append(slist, r);
	return stmt_list(sql->sa, slist);
}

static stmt *
rel2bin_psm(mvc *sql, sql_rel *rel) 
{
	node *n;
	list *l = sa_list(sql->sa);
	stmt *sub = NULL;

	for(n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *s = exp_bin(sql, e, sub, NULL, NULL, NULL, NULL, NULL);

		if (s && s->type == st_table) /* relational statement */
			sub = s->op1;
		else
			append(l, s);
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_seq(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en = rel->exps->h;
	stmt *restart, *sname, *seq, *sl = NULL;
	list *l = sa_list(sql->sa);

	if (rel->l)  /* first construct the sub relation */
		sl = subrel_bin(sql, rel->l, refs);

	restart = exp_bin(sql, en->data, sl, NULL, NULL, NULL, NULL, NULL);
	sname = exp_bin(sql, en->next->data, sl, NULL, NULL, NULL, NULL, NULL);
	seq = exp_bin(sql, en->next->next->data, sl, NULL, NULL, NULL, NULL, NULL);

	(void)refs;
	append(l, sname);
	append(l, seq);
	append(l, restart);
	return stmt_catalog(sql->sa, rel->flag, stmt_list(sql->sa, l));
}

static stmt *
rel2bin_trans(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en = rel->exps->h;
	stmt *chain = exp_bin(sql, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	stmt *name = NULL;

	(void)refs;
	if (en->next)
		name = exp_bin(sql, en->next->data, NULL, NULL, NULL, NULL, NULL, NULL);
	return stmt_trans(sql->sa, rel->flag, chain, name);
}

static stmt *
rel2bin_catalog(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en = rel->exps->h;
	stmt *action = exp_bin(sql, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	stmt *sname = NULL, *name = NULL;
	list *l = sa_list(sql->sa);

	(void)refs;
	en = en->next;
	sname = exp_bin(sql, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	if (en->next) {
		name = exp_bin(sql, en->next->data, NULL, NULL, NULL, NULL, NULL, NULL);
	} else {
		name = stmt_atom_string_nil(sql->sa);
	}
	append(l, sname);
	append(l, name);
	append(l, action);
	return stmt_catalog(sql->sa, rel->flag, stmt_list(sql->sa, l));
}

static stmt *
rel2bin_catalog_table(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en = rel->exps->h;
	stmt *action = exp_bin(sql, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	stmt *table = NULL, *sname;
	list *l = sa_list(sql->sa);

	(void)refs;
	en = en->next;
	sname = exp_bin(sql, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	en = en->next;
	if (en) 
		table = exp_bin(sql, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
	append(l, sname);
	append(l, table);
	append(l, action);
	return stmt_catalog(sql->sa, rel->flag, stmt_list(sql->sa, l));
}

static stmt *
rel2bin_catalog2(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en;
	list *l = sa_list(sql->sa);

	(void)refs;
	for (en = rel->exps->h; en; en = en->next) {
		stmt *es = NULL;

		if (en->data) {
			es = exp_bin(sql, en->data, NULL, NULL, NULL, NULL, NULL, NULL);
			if (!es) 
				return NULL;
		} else {
			es = stmt_atom_string_nil(sql->sa);
		}
		append(l,es);
	}
	return stmt_catalog(sql->sa, rel->flag, stmt_list(sql->sa, l));
}

static stmt *
rel2bin_ddl(mvc *sql, sql_rel *rel, list *refs) 
{
	stmt *s = NULL;

	if (rel->flag == DDL_OUTPUT) {
		s = rel2bin_output(sql, rel, refs);
		sql->type = Q_TABLE;
	} else if (rel->flag <= DDL_LIST) {
		s = rel2bin_list(sql, rel, refs);
	} else if (rel->flag <= DDL_PSM) {
		s = rel2bin_psm(sql, rel);
	} else if (rel->flag <= DDL_ALTER_SEQ) {
		s = rel2bin_seq(sql, rel, refs);
		sql->type = Q_SCHEMA;
	} else if (rel->flag <= DDL_DROP_SEQ) {
		s = rel2bin_catalog2(sql, rel, refs);
		sql->type = Q_SCHEMA;
	} else if (rel->flag <= DDL_TRANS) {
		s = rel2bin_trans(sql, rel, refs);
		sql->type = Q_TRANS;
	} else if (rel->flag <= DDL_DROP_SCHEMA) {
		s = rel2bin_catalog(sql, rel, refs);
		sql->type = Q_SCHEMA;
	} else if (rel->flag <= DDL_ALTER_TABLE) {
		s = rel2bin_catalog_table(sql, rel, refs);
		sql->type = Q_SCHEMA;
	} else if (rel->flag <= DDL_ALTER_TABLE_SET_ACCESS) {
		s = rel2bin_catalog2(sql, rel, refs);
		sql->type = Q_SCHEMA;
	}
	return s;
}

static stmt *
subrel_bin(mvc *sql, sql_rel *rel, list *refs) 
{
	stmt *s = NULL;

	if (THRhighwater())
		return NULL;

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
		s = rel2bin_basetable(sql, rel);
		sql->type = Q_TABLE;
		break;
	case op_table:
		s = rel2bin_table(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
		s = rel2bin_join(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_apply:
		assert(0);
	case op_semi:
	case op_anti:
		s = rel2bin_semijoin(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_union: 
		s = rel2bin_union(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_except: 
		s = rel2bin_except(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_inter: 
		s = rel2bin_inter(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_project:
		s = rel2bin_project(sql, rel, refs, NULL);
		sql->type = Q_TABLE;
		break;
	case op_select: 
		s = rel2bin_select(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_groupby: 
		s = rel2bin_groupby(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_topn: 
		s = rel2bin_topn(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_sample:
		s = rel2bin_sample(sql, rel, refs);
		sql->type = Q_TABLE;
		break;
	case op_insert: 
		s = rel2bin_insert(sql, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_update: 
		s = rel2bin_update(sql, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_delete: 
		s = rel2bin_delete(sql, rel, refs);
		if (sql->type == Q_TABLE)
			sql->type = Q_UPDATE;
		break;
	case op_ddl:
		s = rel2bin_ddl(sql, rel, refs);
		break;
	}
	if (s && rel_is_ref(rel)) {
		list_append(refs, rel);
		list_append(refs, s);
	}
	return s;
}

stmt *
rel_bin(mvc *sql, sql_rel *rel) 
{
	list *refs = sa_list(sql->sa);
	int sqltype = sql->type;
	stmt *s = subrel_bin( sql, rel, refs);

	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	if (s && s->type == st_list && s->op4.lval->t) {
		stmt *cnt = s->op4.lval->t->data;
		if (cnt && cnt->type == st_affected_rows)
			list_remove_data(s->op4.lval, cnt);
	}
	return s;
}

stmt *
output_rel_bin(mvc *sql, sql_rel *rel ) 
{
	list *refs = sa_list(sql->sa);
	int sqltype = sql->type;
	stmt *s = subrel_bin( sql, rel, refs);

	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	if (!is_ddl(rel->op) && s && s->type != st_none && sql->type == Q_TABLE)
		s = stmt_output(sql->sa, s);
	return s;
}
