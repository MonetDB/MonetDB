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

#include "rel_bin.h"
#include "rel_exp.h"
#include "rel_prop.h"
/* needed for recursion (ie triggers) */
#include "rel_select.h"
#include "rel_updates.h"
#include "rel_subquery.h"

static stmt * subrel_bin(mvc *sql, sql_rel *rel, list *refs);

static char *TID = "%TID%";

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
print_stmtlist(sql_allocator *sa, stmt *l )
{
	node *n;
	if (l)
	for (n = l->op4.lval->h; n; n = n->next) {
		char *rnme = table_name(sa, n->data);
		char *nme = column_name(sa, n->data);

		printf("%s.%s\n", rnme, nme);
	}
}

static stmt *
list_find_column(sql_allocator *sa, list *l, char *rname, char *name ) 
{
	stmt *res = NULL;
	node *n;

	if (rname) {
		for (n = l->h; n; n = n->next) {
			char *rnme = table_name(sa, n->data);
			char *nme = column_name(sa, n->data);

			if (rnme && strcmp(rnme, rname) == 0 && 
				    strcmp(nme, name) == 0) {
				res = n->data;
				break;
			}
		}
	} else {
		for (n = l->h; n; n = n->next) {
			char *nme = column_name(sa, n->data);

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
bin_find_column( sql_allocator *sa, stmt *sub, char *rname, char *name ) 
{
	return list_find_column( sa, sub->op4.lval, rname, name);
}

static list *
bin_find_columns(mvc *sql, stmt *sub, char *name ) 
{
	node *n;
	list *l = list_new(sql->sa);

	for (n = sub->op4.lval->h; n; n = n->next) {
		char *nme = column_name(sql->sa, n->data);

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
		list *l = list_new(sql->sa);

		for (n = sub->op4.lval->h; n; n = n->next) {
			stmt *sc = n->data;
			char *cname = column_name(sql->sa, sc);
			char *tname = table_name(sql->sa, sc);

			sc = column(sql->sa, sc);
			list_append(l, stmt_alias(sql->sa, sc, tname, cname));
		}
		sub = stmt_list(sql->sa, l);
	}
	return sub;
}

/* Here we also recognize 'IN'.
 * We change that into a 
 * mark(reverse(semijoin( reverse(column), bat_of_vals)), 0). 
 */
static int
are_equality_exps( list *exps ) 
{
	if (list_length(exps) == 1) {
		sql_exp *e = exps->h->data, *le = e->l, *re = e->r;

		if (e->type == e_cmp && e->flag == cmp_equal && le->card != CARD_ATOM && re->card == CARD_ATOM)
			return 1;
		if (e->type == e_cmp && e->flag == cmp_or)
			return (are_equality_exps(e->l) && 
				are_equality_exps(e->r));
	}
	return 0;
}

static void 
get_exps( list *n, list *l )
{
	sql_exp *e = l->h->data, *re = e->r;

	if (e->type == e_cmp && e->flag == cmp_equal && re->card == CARD_ATOM)
		list_append(n, e);
	if (e->type == e_cmp && e->flag == cmp_or) {
		get_exps(n, e->l);
		get_exps(n, e->r);
	}
}

static stmt *
handle_in_exps( mvc *sql, sql_exp *ce, list *nl, stmt *left, stmt *right, group *grp, int in, int use_r) 
{
	node *n;
	stmt *s, *c;

	/* create bat append values */
	s = stmt_temp(sql->sa, exp_subtype(ce));
	for( n = nl->h; n; n = n->next) {
		sql_exp *e = n->data;
		stmt *i = exp_bin(sql, use_r?e->r:e, left, right, grp, NULL);
		
		s = stmt_append(sql->sa, s, i);
	}
	c = exp_bin(sql, ce, left, right, grp, NULL);
	/*s = stmt_mark_tail(sql->sa, stmt_reverse(sql->sa, stmt_semijoin(sql->sa, stmt_reverse(sql->sa, c), stmt_reverse(sql->sa, s))), 0);*/
	/* not really a projection join, therefore make sure left values are unique !! */
	c = column(sql->sa, c);
	if (in)
		s = stmt_project(sql->sa, c, stmt_reverse(sql->sa, stmt_unique(sql->sa, s, NULL)));
	else 
		s = stmt_reverse(sql->sa, stmt_diff(sql->sa, stmt_reverse(sql->sa, c), stmt_reverse(sql->sa, stmt_unique(sql->sa, s, NULL))));
	s = stmt_const(sql->sa, s, NULL);
	return s;
}

/* For now this only works if all or's are part of the 'IN' */
static stmt *
handle_equality_exps( mvc *sql, list *l, list *r, stmt *left, stmt *right, group *grp )
{
	node *n;
	sql_exp *ce = NULL;
	list *nl = new_exp_list(sql->sa);

	get_exps(nl, l);
	get_exps(nl, r);

	for( n = nl->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (!ce) {
			ce = e->l;
			if (!is_column(ce->type))
				return NULL;
		}
		if (!exp_match(ce, e->l)) 
			return NULL;
	} 
	return handle_in_exps( sql, ce, nl, left, right, grp, 1, 1);
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
		stmt *i = exp_bin(sql, e, NULL, NULL, NULL, NULL);
		
		s = stmt_append(sql->sa, s, i);
	}
	return s;
}

stmt *
exp_bin(mvc *sql, sql_exp *e, stmt *left, stmt *right, group *grp, stmt *sel) 
{
	stmt *s = NULL;

	if (!e) {
		assert(0);
		return NULL;
	}

	switch(e->type) {
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
		stmt *l = exp_bin(sql, e->l, left, right, grp, sel);
		list *tps = e->r;
		sql_subtype *from = tps->h->data;
		sql_subtype *to = tps->h->next->data;
		if (!l) 
			return NULL;
		s = stmt_convert(sql->sa, l, from, to);
	} 	break;
	case e_func: {
		node *en;
		list *l = list_new(sql->sa), *exps = e->l, *obe = e->r;
		sql_subfunc *f = e->f;

		if (!obe && exps) {
			for (en = exps->h; en; en = en->next) {
				stmt *es;

				es = exp_bin(sql, en->data, left, right, grp, sel);
				if (!es) 
					return NULL;
				list_append(l,es);
			}
		}
		/* Window expressions are handled differently.
		   ->l == group by expression list
		   ->r == order by expression list
		   If both lists are empty, we pass a single 
		 	column for the inner relation
		 */
		if (obe) {
			group *g = NULL;
			stmt *orderby = NULL;
		
			if (exps) {
				for (en = exps->h; en; en = en->next) {
					stmt *es;

					es = exp_bin(sql, en->data, left, right, NULL, sel);
					if (!es) 
						return NULL;
					g = grp_create(sql->sa, es, g);
				}
			}
			/* order on the group first */
			grp_done(g);
			if (g) 
				orderby = stmt_order(sql->sa, g->grp, 1);
			for (en = obe->h; en; en = en->next) {
				sql_exp *orderbycole = en->data; 
				stmt *orderbycols = exp_bin(sql, orderbycole, left, right, NULL, sel); 

				if (!orderbycols) 
					return NULL;
				if (orderby)
					orderby = stmt_reorder(sql->sa, orderby, orderbycols, is_ascending(orderbycole));
				else
					orderby = stmt_order(sql->sa, orderbycols, is_ascending(orderbycole));
			}
			if (!orderby && left)
				orderby = stmt_mirror(sql->sa, bin_first_column(sql->sa, left));
			if (!orderby) 
				return NULL;
			list_append(l, orderby);
			if (g) {
				list_append(l, g->grp);
				list_append(l, g->ext);
			}
		}
		if (strcmp(f->func->base.name, "identity") == 0) 
			s = stmt_mirror(sql->sa, l->h->data);
		else
			s = stmt_Nop(sql->sa, stmt_list(sql->sa, l), e->f); 
	} 	break;
	case e_aggr: {
		sql_exp *at = NULL;
		list *attr = e->l; 
		stmt *as = NULL;
		stmt *as2 = NULL;
		sql_subaggr *a = e->f;
		group *g = grp;

		assert(sel == NULL);
		if (attr && attr->h) { 
			at = attr->h->data;
			as = exp_bin(sql, at, left, right, NULL, sel);
			if (list_length(attr) == 2)
				as2 = exp_bin(sql, attr->h->next->data, left, right, NULL, sel);
			/* insert single value into a column */
			if (as && as->nrcols <= 0 && !left)
				as = const_column(sql->sa, as);
		} else {
			/* count(*) may need the default group (relation) and
			   and/or an attribute to count */
			if (g) {
				as = grp->grp;
			} else if (left) {
				as = bin_first_column(sql->sa, left);
			} else {
				/* create dummy single value in a column */
				as = stmt_atom_wrd(sql->sa, 0);
				as = const_column(sql->sa, as);
			}
		}
		if (!as) 
			return NULL;	

		if (as->nrcols <= 0 && left) 
			as = stmt_const(sql->sa, bin_first_column(sql->sa, left), as);
		/* inconsistent sql requires NULL != NULL, ie unknown
		 * but also NULL means no values, which means 'ignore'
		 *
		 * so here we need to ignore NULLs
		 */
		if (need_no_nil(e) && at && has_nil(at) && attr) {
			sql_subtype *t = exp_subtype(at);
			stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
			as = stmt_select2(sql->sa, as, n, n, 0);
		}
		if (need_distinct(e)){ 
			if (g)
				as = stmt_unique(sql->sa, as, grp);
			else
				as = stmt_unique(sql->sa, as, NULL);
		}
		if (as2) 
			s = stmt_aggr2(sql->sa, stmt_reverse(sql->sa, as), as2, a );
		else
			s = stmt_aggr(sql->sa, as, g, a, 1 );
		/* HACK: correct cardinality for window functions */
		if (e->card > CARD_AGGR)
			s->nrcols = 2;
	} 	break;
	case e_column: {
		if (right) /* check relation names */
			s = bin_find_column(sql->sa, right, e->l, e->r);
		if (!s && left) 
			s = bin_find_column(sql->sa, left, e->l, e->r);
		if (s && grp)
			s = stmt_join(sql->sa, grp->ext, s, cmp_equal);
		if (!s && right) {
			printf("could not find %s.%s\n", (char*)e->l, (char*)e->r);
			print_stmtlist(sql->sa, left);
			print_stmtlist(sql->sa, right);
		}
		if (s && sel)
			s = stmt_semijoin(sql->sa, s, sel);
	 }	break;
	case e_cmp: {
		stmt *l = NULL, *r = NULL, *r2 = NULL;
		int swapped = 0, is_select = 0;
		sql_exp *re = e->r, *re2 = e->f;
		prop *p;

		if (e->flag == cmp_filter)
			re2 = NULL;
		if (e->flag == cmp_in || e->flag == cmp_notin) {
			return handle_in_exps(sql, e->l, e->r, left, right, grp, (e->flag == cmp_in), 0);
		}
		if (e->flag == cmp_or) {
			list *l = e->l;
			node *n;
			stmt *sel1, *sel2;

			/* Here we also recognize 'IN'.
			 * We change that into a 
			 * reverse(semijoin( reverse(column), bat_of_vals)). 
			 */
			if (are_equality_exps(e->l) && are_equality_exps(e->r))
				if ((s = handle_equality_exps(sql, e->l, e->r, left, right, grp)) != NULL)
					return s;
			sel1 = stmt_relselect_init(sql->sa);
			sel2 = stmt_relselect_init(sql->sa);
			for( n = l->h; n; n = n->next ) {
				s = exp_bin(sql, n->data, left, right, grp, sel); 
				if (!s) 
					return s;
				stmt_relselect_fill(sel1, s);
			}
			l = e->r;
			for( n = l->h; n; n = n->next ) {
				s = exp_bin(sql, n->data, left, right, grp, sel); 
				if (!s) 
					return s;
				stmt_relselect_fill(sel2, s);
			}
			if (sel1->nrcols == 0 && sel2->nrcols == 0) {
				sql_subtype *bt = sql_bind_localtype("bit");
				sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, "or", bt, bt, F_FUNC);
				assert(f);
				return stmt_binop(sql->sa, sel1, sel2, f);
			}
			return stmt_union(sql->sa, sel1,sel2);
		}
		/* here we handle join indices */
		if ((p=find_prop(e->p, PROP_JOINIDX)) != NULL) {
			sql_idx *i = p->value;
			sql_exp *el = e->l;
			sql_exp *er = e->r;
			char *iname = sa_strconcat(sql->sa, "%", i->base.name);

			/* find out left and right */
			l = bin_find_column(sql->sa, left, el->l, iname);
			if (!l) {
				swapped = 1;
				l = bin_find_column(sql->sa, right, el->l, iname);
				r = bin_find_column(sql->sa, left, er->l, TID);
			} else {
				r = bin_find_column(sql->sa, right, er->l, TID);
			}
			/* small performance improvement, ie use idx directly */
			if (l->type == st_alias && 
			    l->op1->type == st_idxbat &&
			    r->type == st_alias && 
			    r->op1->type == st_mirror) {
				s = l;
			} else if (swapped)
				s = stmt_join(sql->sa, r, stmt_reverse(sql->sa, l), cmp_equal);
			else
				s = stmt_join(sql->sa, l, stmt_reverse(sql->sa, r), cmp_equal);
			sql->opt_stats[0]++; 
			assert(sel==NULL);
			break;
		}
		if (!l) {
			l = exp_bin(sql, e->l, left, NULL, grp, sel);
			swapped = 0;
		}
		if (!l && right) {
 			l = exp_bin(sql, e->l, right, NULL, grp, sel);
			swapped = 1;
		}
		if (swapped || !right)
 			r = exp_bin(sql, e->r, left, NULL, grp, sel);
		else
 			r = exp_bin(sql, e->r, right, NULL, grp, sel);
		if (!r && !swapped) {
 			r = exp_bin(sql, e->r, left, NULL, grp, sel);
			is_select = 1;
		}
		if (!r && swapped) {
 			r = exp_bin(sql, e->r, right, NULL, grp, sel);
			is_select = 1;
		}
		if (re2)
 			r2 = exp_bin(sql, e->f, left, right, grp, sel);
		if (!l || !r || (re2 && !r2)) {
			assert(0);
			return NULL;
		}

		/* the escape character of like is in the right expression */
		if (e->flag == cmp_notlike || e->flag == cmp_like ||
		    e->flag == cmp_notilike || e->flag == cmp_ilike)
		{
			if (!e->f)
				r2 = stmt_atom_string(sql->sa, sa_strdup(sql->sa, ""));
			if (!l || !r || !r2) {
				assert(0);
				return NULL;
			}
			if (l->nrcols == 0) {
				stmt *lstmt;
				char *likef = (e->flag == cmp_notilike || e->flag == cmp_ilike ?
					"ilike" : "like");
				sql_subtype *s = sql_bind_localtype("str");
				sql_subfunc *like = sql_bind_func3(sql->sa, sql->session->schema, likef, s, s, s, F_FUNC);
				list *ops = list_new(sql->sa);

				assert(s && like);
				
				list_append(ops, l);
				list_append(ops, r);
				list_append(ops, r2);
				lstmt = stmt_Nop(sql->sa, stmt_list(sql->sa, ops), like);
				if (e->flag == cmp_notlike || e->flag == cmp_notilike) {
					sql_subtype *bt = sql_bind_localtype("bit");
					sql_subfunc *not = sql_bind_func(sql->sa, sql->session->schema, "not", bt, NULL, F_FUNC);
					lstmt = stmt_unop(sql->sa, lstmt, not);
				}
				return lstmt;
			}
                        if (left && right && re->card > CARD_ATOM && !is_select) {
                                /* create l and r, gen operator func */
                                char *like = (e->flag == cmp_like || e->flag == cmp_notlike)?"like":"ilike";
                                int anti = (e->flag == cmp_notlike || e->flag == cmp_notilike);
                                sql_subtype *s = sql_bind_localtype("str");
                                sql_subfunc *f = sql_bind_func3(sql->sa, sql->session->schema, like, s, s, s, F_FUNC);

                                stmt *j = stmt_joinN(sql->sa, stmt_list(sql->sa, append(list_new(sql->sa),l)), stmt_list(sql->sa, append(append(list_new(sql->sa),r),r2)), f);
                                if (is_anti(e) || anti)
                                        j->flag |= ANTI;
                                return j;
                        }
			return stmt_likeselect(sql->sa, l, r, r2, (comp_type)e->flag);
		}
		/* TODO general predicate, select and join */
		if (e->flag == cmp_filter) 
			return stmt_genselect(sql->sa, l, r, e->f);
		if (left && right && !is_select &&
		   ((l->nrcols && (r->nrcols || (r2 && r2->nrcols))) || 
		     re->card > CARD_ATOM || 
		    (re2 && re2->card > CARD_ATOM))) {
			if (l->nrcols == 0)
				l = stmt_const(sql->sa, bin_first_column(sql->sa, swapped?right:left), l); 
			if (r->nrcols == 0)
				r = stmt_const(sql->sa, bin_first_column(sql->sa, swapped?left:right), r); 
			if (r2) {
				s = stmt_join2(sql->sa, l, r, r2, (comp_type)e->flag);
				if (swapped) 
					s = stmt_reverse(sql->sa, s);
			} else if (swapped) {
				s = stmt_join(sql->sa, r, stmt_reverse(sql->sa, l), swap_compare((comp_type)e->flag));
			} else {
				s = stmt_join(sql->sa, l, stmt_reverse(sql->sa, r), (comp_type)e->flag);
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
				} else if (l->nrcols > 0 && r->nrcols > 0 && r2->nrcols > 0) {
					s = stmt_semijoin(sql->sa, 
						stmt_uselect(sql->sa, l, r, range2lcompare(e->flag)),
						stmt_uselect(sql->sa, l, r2, range2rcompare(e->flag)));
				} else {
					s = stmt_uselect2(sql->sa, l, r, r2, (comp_type)e->flag);
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
					/* this can still be a join (as relationa algebra and single value subquery results still means joins */
					s = stmt_uselect(sql->sa, l, r, (comp_type)e->flag);
					/* so we still need the proper side */
					if (swapped)
						s = stmt_reverse(sql->sa, s);
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


static stmt *
stmt_rename(mvc *sql, sql_rel *rel, sql_exp *exp, stmt *s )
{
	char *name = exp->name;
	char *rname = exp->rname;

	(void)rel;
	if (!name && exp->type == e_column && exp->r)
		name = exp->r;
	if (!name)
		name = column_name(sql->sa, s);
	else
		name = sa_strdup(sql->sa, name);
	if (!rname && exp->type == e_column && exp->l)
		rname = exp->l;
	if (!rname)
		rname = table_name(sql->sa, s);
	else
		rname = sa_strdup(sql->sa, rname);
	s = stmt_alias(sql->sa, s, rname, name);
	return s;
}

static stmt *
rel2bin_sql_table(mvc *sql, sql_table *t) 
{
	list *l = list_new(sql->sa);
	node *n;
	stmt *ts;
	char *tname = t->base.name, *rnme;
			
	ts = stmt_basetable(sql->sa, t, t->base.name);
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		stmt *sc = stmt_bat(sql->sa, c, ts, RDONLY);
		list_append(l, sc);
	}
	/* TID column */
	rnme = sa_strdup(sql->sa, tname);
	if (t->columns.set->h) { 
		sql_column *c = t->columns.set->h->data;
		stmt *sc = stmt_bat(sql->sa, c, ts, RDONLY);

		sc = stmt_mirror(sql->sa, sc);
		sc = stmt_alias(sql->sa, sc, rnme, sa_strdup(sql->sa, TID));
		list_append(l, sc);
	}

	if (t->idxs.set) {
		char *rnme = sa_strdup(sql->sa, tname);
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			stmt *sc = stmt_idxbat(sql->sa, i, RDONLY);

			/* index names are prefixed, to make them independent */
			sc = stmt_alias(sql->sa, sc, rnme, sa_strconcat(sql->sa, "%", i->base.name));
			list_append(l, sc);
		}
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_basetable( mvc *sql, sql_rel *rel, list *refs)
{
	list *l = list_new(sql->sa);
	stmt *ts, *sub = NULL;
	sql_table *t = rel->l;
	node *n;
			
	(void)refs;
	ts = stmt_basetable(sql->sa, t, t->base.name);
	assert(rel->exps);
	for (n = t->columns.set->h; n; n = n->next) {
		sql_column *c = n->data;

		stmt *sc = stmt_bat(sql->sa, c, ts, RDONLY);
		list_append(l, sc);
	}
	/* TID column */
	if (t->columns.set->h) { 
		sql_column *c = t->columns.set->h->data;
		stmt *sc = stmt_bat(sql->sa, c, ts, RDONLY);
		char *rnme = sa_strdup(sql->sa, t->base.name);

		sc = stmt_mirror(sql->sa, sc);
		sc = stmt_alias(sql->sa, sc, rnme, sa_strdup(sql->sa, TID));
		list_append(l, sc);
	}
	if (t->idxs.set) {
		for (n = t->idxs.set->h; n; n = n->next) {
			sql_idx *i = n->data;
			stmt *sc = stmt_idxbat(sql->sa, i, RDONLY);
			char *rnme = sa_strdup(sql->sa, t->base.name);

			/* index names are prefixed, to make them independent */
			sc = stmt_alias(sql->sa, sc, rnme, sa_strconcat(sql->sa, "%", i->base.name));
			list_append(l, sc);
		}
	}

	sub = stmt_list(sql->sa, l);
	/* add aliases */
	if (rel->exps) {
		node *en;

		l = list_new(sql->sa);
		for( en = rel->exps->h; en; en = en->next ) {
			sql_exp *exp = en->data;
			stmt *s = bin_find_column(sql->sa, sub, exp->l, exp->r);
			char *rname = exp->rname?exp->rname:exp->l;
	
			if (!s) {
				assert(0);
				return NULL;
			}
			rname = rname?sa_strdup(sql->sa, rname):NULL;
			s = stmt_alias(sql->sa, s, rname, sa_strdup(sql->sa, exp->name));
			list_append(l, s);
		}
		sub = stmt_list(sql->sa, l);
	}
	return sub;
}

static stmt *
rel2bin_table( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	stmt *sub = NULL;
	node *en, *n;
	int i;
	sql_exp *op = rel->r;
	sql_subfunc *f = op->f;
	sql_table *t = f->res.comp_type;
			
	if (!t)
		t = f->func->res.comp_type;
	if (rel->l)
		sub = subrel_bin(sql, rel->l, refs);
	sub = exp_bin(sql, rel->r, sub, NULL, NULL, NULL); /* table function */
	if (!sub || !t) { 
		assert(0);
		return NULL;	
	}

	l = list_new(sql->sa);
	for(i = 0, n = t->columns.set->h; n; n = n->next, i++ ) {
		sql_column *c = n->data;
		stmt *s = stmt_rs_column(sql->sa, sub, i, &c->type); 
		char *nme = c->base.name;
		char *rnme = exp_find_rel_name(op);

		rnme = (rnme)?sa_strdup(sql->sa, rnme):NULL;
		s = stmt_alias(sql->sa, s, rnme, sa_strdup(sql->sa, nme));
		list_append(l, s);
	}
	sub = stmt_list(sql->sa, l);

	l = list_new(sql->sa);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		char *rnme = exp->rname?exp->rname:exp->l;
		stmt *s;
	       
		/* no relation names */
		if (exp->l)
			exp->l = NULL;
		s = exp_bin(sql, exp, sub, NULL, NULL, NULL);

		if (!s) {
			assert(0);
			return NULL;
		}
		if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(sql->sa, bin_first_column(sql->sa, sub), s);
		rnme = (rnme)?sa_strdup(sql->sa, rnme):NULL;
		s = stmt_alias(sql->sa, s, rnme, sa_strdup(sql->sa, exp->name));
		list_append(l, s);
	}
	sub = stmt_list(sql->sa, l);
	return sub;
}

static int
equi_join(stmt *j)
{
	if (j->flag == cmp_equal)
		return 0;
	return -1;
}

static int
not_equi_join(stmt *j)
{
	if (j->flag != cmp_equal)
		return 0;
	return -1;
}

static stmt *
rel2bin_join( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	node *en, *n;
	stmt *left = NULL, *right = NULL, *join = NULL, *jl, *jr;
	stmt *ld = NULL, *rd = NULL;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);
	right = row2cols(sql, right);
	if (rel->exps) {
		int idx = 0;
		list *jns = list_new(sql->sa);

		/* generate a stmt_reljoin */
		for( en = rel->exps->h; en; en = en->next ) {
			int join_idx = sql->opt_stats[0];
			stmt *s = exp_bin(sql, en->data, left, right, NULL, NULL);

			if (!s) {
				assert(0);
				return NULL;
			}
			if (join_idx != sql->opt_stats[0])
				idx = 1;
			if (!join) {
				join = s;
			} else if (s->type != st_join && 
				   s->type != st_join2 && 
				   s->type != st_joinN) {
				if (s->type == st_reverse) {
					stmt *rs = s->op1;

					if (rs->type == st_join || 
				   	    rs->type == st_join2 || 
				   	    rs->type == st_joinN) {
						list_append(jns, s);
						continue;
					}
				}
				/* handle select expressions */
				/*assert(0);*/
				if (s->h == join->h) {
					join = stmt_semijoin(sql->sa, join,s);
				} else {
					join = stmt_reverse(sql->sa, join);
					join = stmt_semijoin(sql->sa, join,s);
					join = stmt_reverse(sql->sa, join);
				}
				continue;
			}
			list_append(jns, s);
		}
		if (list_length(jns) > 1) {
			int o = 1, *O = &o;
			/* move all equi joins into a releqjoin */
			list *eqjns = list_select(jns, O, (fcmp)&equi_join, NULL);
			if (!idx && list_length(eqjns) > 1) {
				list *neqjns = list_select(jns, O, (fcmp)&not_equi_join, NULL);
				join = stmt_reljoin(sql->sa, stmt_releqjoin1(sql->sa, eqjns), neqjns);
			} else {
				join = stmt_reljoin(sql->sa, NULL, jns);
			}
		} else {
			join = jns->h->data; 
		}
	} else {
		stmt *l = bin_first_column(sql->sa, left);
		stmt *r = bin_first_column(sql->sa, right);
		join = stmt_join(sql->sa, l, stmt_reverse(sql->sa, r), cmp_all); 
	}

	/* construct relation */
	l = list_new(sql->sa);

	jl = stmt_reverse(sql->sa, stmt_mark_tail(sql->sa, join,0));
	if (rel->op == op_left || rel->op == op_full) {
		/* we need to add the missing oid's */
		ld = stmt_diff(sql->sa, bin_first_column(sql->sa, left), stmt_reverse(sql->sa, jl));
		ld = stmt_mark(sql->sa, stmt_reverse(sql->sa, ld), 0);
	}
	jr = stmt_reverse(sql->sa, stmt_mark_tail(sql->sa, stmt_reverse(sql->sa, join),0));
	if (rel->op == op_right || rel->op == op_full) {
		/* we need to add the missing oid's */
		rd = stmt_diff(sql->sa, bin_first_column(sql->sa, right), stmt_reverse(sql->sa, jr));
		rd = stmt_mark(sql->sa, stmt_reverse(sql->sa, rd), 0);
	}

	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		char *rnme = table_name(sql->sa, c);
		char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(sql->sa, jl, column(sql->sa, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = Column(sql->sa, s);
		if (rel->op == op_left || rel->op == op_full)
			s = stmt_append(sql->sa, s, stmt_join(sql->sa, ld, c, cmp_equal));
		if (rel->op == op_right || rel->op == op_full) 
			s = stmt_append(sql->sa, s, stmt_const(sql->sa, rd, stmt_atom(sql->sa, atom_general(sql->sa, tail_type(c), NULL))));

		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	for( n = right->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		char *rnme = table_name(sql->sa, c);
		char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(sql->sa, jr, column(sql->sa, c) );

		/* as append isn't save, we append to a new copy */
		if (rel->op == op_left || rel->op == op_full || rel->op == op_right)
			s = Column(sql->sa, s);
		if (rel->op == op_left || rel->op == op_full) 
			s = stmt_append(sql->sa, s, stmt_const(sql->sa, ld, stmt_atom(sql->sa, atom_general(sql->sa, tail_type(c), NULL))));
		if (rel->op == op_right || rel->op == op_full) 
			s = stmt_append(sql->sa, s, stmt_join(sql->sa, rd, c, cmp_equal));

		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_semijoin( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	node *en, *n;
	stmt *left = NULL, *right = NULL, *join = NULL;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);
	right = row2cols(sql, right);
	if (rel->exps) {
		for( en = rel->exps->h; en; en = en->next ) {
			stmt *s = exp_bin(sql, en->data, left, right, NULL, NULL);

			if (!s) {
				assert(0);
				return NULL;
			}
			if (!join) {
				join = s;
			} else {
				/* break column join */
				stmt *l = stmt_mark(sql->sa, stmt_reverse(sql->sa, join), 100);
				stmt *r = stmt_mark(sql->sa, join, 100);
				stmt *ld = s->op1;
				stmt *rd = stmt_reverse(sql->sa, s->op2);
				stmt *le = stmt_join(sql->sa, l, ld, cmp_equal);
				stmt *re = stmt_join(sql->sa, r, rd, cmp_equal);

				sql_subfunc *f = sql_bind_func(sql->sa, sql->session->schema, compare_func((comp_type)s->flag), tail_type(le), tail_type(le), F_FUNC);
				stmt * cmp;

				assert(f);

				cmp = stmt_binop(sql->sa, le, re, f);

				cmp = stmt_uselect(sql->sa, cmp, stmt_bool(sql->sa, 1), cmp_equal);

				l = stmt_semijoin(sql->sa, l, cmp);
				r = stmt_semijoin(sql->sa, r, cmp);
				join = stmt_join(sql->sa, stmt_reverse(sql->sa, l), r, cmp_equal);
			}
		}
	} else {
		/* TODO: this case could use some optimization */
		stmt *l = bin_first_column(sql->sa, left);
		stmt *r = bin_first_column(sql->sa, right);
		join = stmt_join(sql->sa, l, stmt_reverse(sql->sa, r), cmp_all); 
	}

	/* construct relation */
	l = list_new(sql->sa);

	/* We did a full join, thats too much. 
	   Reduce this using difference and semijoin */
	if (rel->op == op_anti) {
		stmt *c = left->op4.lval->h->data;
		join = stmt_diff(sql->sa, c, join);
	} else {
		stmt *c = left->op4.lval->h->data;
		join = stmt_semijoin(sql->sa, c, join);
	}

	join = stmt_reverse(sql->sa, stmt_mark_tail(sql->sa, join,0));

	/* semijoin all the left columns */
	for( n = left->op4.lval->h; n; n = n->next ) {
		stmt *c = n->data;
		char *rnme = table_name(sql->sa, c);
		char *nme = column_name(sql->sa, c);
		stmt *s = stmt_project(sql->sa, join, column(sql->sa, c));

		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_distinct(mvc *sql, stmt *s)
{
	node *n;
	group *grp = NULL;
	list *rl = list_new(sql->sa), *tids;

	/* single values are unique */
	if (s->key && s->nrcols == 0)
		return s;

	/* Use 'all' tid columns */
	if ((tids = bin_find_columns(sql, s, TID)) != NULL) {
		for (n = tids->h; n; n = n->next) {
			stmt *t = n->data;

			grp = grp_create(sql->sa, column(sql->sa, t), grp);
		}
	} else {
		for (n = s->op4.lval->h; n; n = n->next) {
			stmt *t = n->data;

			grp = grp_create(sql->sa, column(sql->sa, t), grp);
		}
	}
	grp_done(grp);

	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *t = n->data;

		list_append(rl, stmt_project(sql->sa, grp->ext, t));
	}

	s = stmt_list(sql->sa, rl);
	return s;
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
	l = list_new(sql->sa);
	for( n = left->op4.lval->h, m = right->op4.lval->h; n && m; 
		n = n->next, m = m->next ) {
		stmt *c1 = n->data;
		stmt *c2 = m->data;
		char *rnme = table_name(sql->sa, c1);
		char *nme = column_name(sql->sa, c1);
		stmt *s;

		/* append isn't save, ie use union 
			(also not save loses unique head oids) 

		   so we create append on copies.
			TODO: mark columns non base columns, ie were no
			copy is needed
		*/
		s = stmt_append(sql->sa, Column(sql->sa, c1), c2);
		s = stmt_alias(sql->sa, s, rnme, nme);
		list_append(l, s);
	}
	sub = stmt_list(sql->sa, l);

	/* union exp list is a rename only */
	if (rel->exps) {
		node *en, *n;
		list *l = list_new(sql->sa);

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

	if (need_distinct(rel)) 
		sub = rel2bin_distinct(sql, sub);
	return sub;
}

static stmt *
rel2bin_except( mvc *sql, sql_rel *rel, list *refs)
{
	list *stmts; 
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;

	group *lgrp = NULL, *rgrp = NULL;
	stmt *s, *lm, *ls = NULL, *rs = NULL, *ld = NULL;
	sql_subaggr *a;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);

	/* construct relation */
	stmts = list_new(sql->sa);
	/*
	 * The multi column intersect is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of A with min(A.count,B.count), 
	 * number of rows.
	 * 
	 * The problem with this approach is that the groups should
	 * have equal group identifiers. So we take the union of all
	 * columns before the group by.
	 */
	for (n = left->op4.lval->h; n; n = n->next) 
		lgrp = grp_create(sql->sa, column(sql->sa, n->data), lgrp);
	for (n = right->op4.lval->h; n; n = n->next) 
		rgrp = grp_create(sql->sa, column(sql->sa, n->data), rgrp);

	if (!lgrp || !rgrp) 
		return NULL;
	grp_done(lgrp);
	grp_done(rgrp);

 	a = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	ls = stmt_aggr(sql->sa, lgrp->grp, lgrp, a, 1); 
	rs = stmt_aggr(sql->sa, rgrp->grp, rgrp, a, 1); 

	/* now find the matching groups */
	s = stmt_releqjoin_init(sql->sa);
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(sql->sa, n->data);
		stmt *r = column(sql->sa, m->data);

		l = stmt_join(sql->sa, lgrp->ext, l, cmp_equal);
		r = stmt_join(sql->sa, rgrp->ext, r, cmp_equal);
		stmt_releqjoin_fill(s, l, r);
	}

	/* the join of the groups removed those in A but not in B,
	 * we need these later so keep these in 'ld' */
	ld = stmt_diff(sql->sa, ls, s);
		
	/*if (!distinct) */
	{
		sql_subfunc *sub;

		lm = stmt_reverse(sql->sa, stmt_mark_tail(sql->sa, s,0));
		ls = stmt_join(sql->sa, lm,ls,cmp_equal);
		rs = stmt_join(sql->sa, stmt_mark(sql->sa, s,0),rs,cmp_equal);

 		sub = sql_bind_func(sql->sa, sql->session->schema, "sql_sub", tail_type(ls), tail_type(rs), F_FUNC);
		/*s = sql_binop_(sql, NULL, "sql_sub", ls, rs);*/
		s = stmt_binop(sql->sa, ls, rs, sub);
		s = stmt_select(sql->sa, s, stmt_atom_wrd(sql->sa, 0), cmp_gt);

		/* A ids */
		s = stmt_join(sql->sa, stmt_reverse(sql->sa, lm), s, cmp_equal);
		/* now we need to add the groups which weren't in B */
		s = stmt_union(sql->sa, ld,s);
		/* now we have gid,cnt, blowup to full groupsizes */
		s = stmt_gen_group(sql->sa, s);
	/*
	} else {
		s = ld;
	*/
	}
	s = stmt_mark_tail(sql->sa, s, 500); 
	/* from gid back to A id's */
	s = stmt_reverse(sql->sa, stmt_join(sql->sa, lgrp->ext, s, cmp_equal));

	/* project columns of left hand expression */
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(sql->sa, n->data);
		char *rnme = NULL;
		char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_join(sql->sa, s, c1, cmp_equal);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(sql->sa, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(sql->sa, stmts);

	/* TODO put in sep function !!!, and add to all is_project(op) */
	/* except can be a projection too */
	if (rel->exps) {
		node *en;
		list *l = list_new(sql->sa);

		for( en = rel->exps->h; en; en = en->next ) {
			sql_exp *exp = en->data;
			stmt *s = exp_bin(sql, exp, sub, NULL, NULL, NULL);

			if (!s) {
				assert(0);
				return NULL;
			}
			s = stmt_rename(sql, rel, exp, s);
			list_append(l, s);
		}
		sub = stmt_list(sql->sa, l);
	}

	if (need_distinct(rel))
		sub = rel2bin_distinct(sql, sub);
	return sub;
}

static stmt *
rel2bin_inter( mvc *sql, sql_rel *rel, list *refs)
{
	list *stmts; 
	node *n, *m;
	stmt *left = NULL, *right = NULL, *sub;

	group *lgrp = NULL, *rgrp = NULL;
	stmt *s, *lm, *ls = NULL, *rs = NULL;
	sql_subaggr *a;

	if (rel->l) /* first construct the left sub relation */
		left = subrel_bin(sql, rel->l, refs);
	if (rel->r) /* first construct the right sub relation */
		right = subrel_bin(sql, rel->r, refs);
	if (!left || !right) 
		return NULL;	
	left = row2cols(sql, left);

	/* construct relation */
	stmts = list_new(sql->sa);
	/*
	 * The multi column intersect is handled using group by's and
	 * group size counts on both sides of the intersect. We then
	 * return for each group of A with min(A.count,B.count), 
	 * number of rows.
	 * 
	 * The problem with this approach is that the groups should
	 * have equal group identifiers. So we take the union of all
	 * columns before the group by.
	 */
	for (n = left->op4.lval->h; n; n = n->next) 
		lgrp = grp_create(sql->sa, column(sql->sa, n->data), lgrp);
	for (n = right->op4.lval->h; n; n = n->next) 
		rgrp = grp_create(sql->sa, column(sql->sa, n->data), rgrp);

	if (!lgrp || !rgrp) 
		return NULL;
	grp_done(lgrp);
	grp_done(rgrp);

 	a = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	ls = stmt_aggr(sql->sa, lgrp->grp, lgrp, a, 1); 
	rs = stmt_aggr(sql->sa, rgrp->grp, rgrp, a, 1); 

	/* now find the matching groups */
	s = stmt_releqjoin_init(sql->sa);
	for (n = left->op4.lval->h, m = right->op4.lval->h; n && m; n = n->next, m = m->next) {
		stmt *l = column(sql->sa, n->data);
		stmt *r = column(sql->sa, m->data);

		l = stmt_join(sql->sa, lgrp->ext, l, cmp_equal);
		r = stmt_join(sql->sa, rgrp->ext, r, cmp_equal);
		stmt_releqjoin_fill(s, l, r);
	}
		
	/*if (!distinct) */
	{
		sql_subfunc *min;

		lm = stmt_reverse(sql->sa, stmt_mark_tail(sql->sa, s,0));
		ls = stmt_join(sql->sa, lm,ls,cmp_equal);
		rs = stmt_join(sql->sa, stmt_mark(sql->sa, s,0),rs,cmp_equal);

 		min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", tail_type(ls), tail_type(rs), F_FUNC);
		/*s = sql_binop_(sql, NULL, "sql_min", ls, rs);*/
		s = stmt_binop(sql->sa, ls, rs, min);
		/* A ids */
		s = stmt_join(sql->sa, stmt_reverse(sql->sa, lm), s, cmp_equal);
		/* now we have gid,cnt, blowup to full groupsizes */
		s = stmt_gen_group(sql->sa, s);
	}
	s = stmt_mark_tail(sql->sa, s, 500); 
	/* from gid back to A id's */
	s = stmt_reverse(sql->sa, stmt_join(sql->sa, lgrp->ext, s, cmp_equal));

	/* project columns of left hand expression */
	for (n = left->op4.lval->h; n; n = n->next) {
		stmt *c1 = column(sql->sa, n->data);
		char *rnme = NULL;
		char *nme = column_name(sql->sa, c1);

		/* retain name via the stmt_alias */
		c1 = stmt_join(sql->sa, s, c1, cmp_equal);

		rnme = table_name(sql->sa, c1);
		c1 = stmt_alias(sql->sa, c1, rnme, nme);
		list_append(stmts, c1);
	}
	sub = stmt_list(sql->sa, stmts);

	/* TODO put in sep function !!!, and add to all is_project(op) */
	/* intersection can be a projection too */
	if (rel->exps) {
		node *en;
		list *l = list_new(sql->sa);

		for( en = rel->exps->h; en; en = en->next ) {
			sql_exp *exp = en->data;
			stmt *s = exp_bin(sql, exp, sub, NULL, NULL, NULL);

			if (!s) {
				assert(0);
				return NULL;
			}
			s = stmt_rename(sql, rel, exp, s);
			list_append(l, s);
		}
		sub = stmt_list(sql->sa, l);
	}

	if (need_distinct(rel))
		sub = rel2bin_distinct(sql, sub);
	return sub;
}

static stmt *
sql_reorder(mvc *sql, stmt *order, stmt *s) 
{
	list *l = list_new(sql->sa);
	node *n;

	/* we need to keep the order by column, to propagate the sort property*/
	order = stmt_mark(sql->sa, stmt_reverse(sql->sa, order), 0);
	for (n = s->op4.lval->h; n; n = n->next) {
		stmt *sc = n->data;
		char *cname = column_name(sql->sa, sc);
		char *tname = table_name(sql->sa, sc);

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

		if (!le) { /* for now only handle topn 
				including limit, ie not just offset */
			topn = NULL;
		} else {
			l = exp_bin(sql, le, NULL, NULL, NULL, NULL);
			if (oe) {
				sql_subtype *wrd = sql_bind_localtype("wrd");
				sql_subfunc *add = sql_bind_func_result(sql->sa, sql->session->schema, "sql_add", wrd, wrd, wrd);
				stmt *o = exp_bin(sql, oe, NULL, NULL, NULL, NULL);
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
		if (sub->type == st_ordered) {
			stmt *n = sql_reorder(sql, sub->op1, sub->op2);
			sub = n;
		}
	}

	pl = list_new(sql->sa);
	psub = stmt_list(sql->sa, pl);
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *exp = en->data;
		stmt *s = exp_bin(sql, exp, sub, NULL, NULL, NULL);

		if (!s)
			s = exp_bin(sql, exp, sub, psub, NULL, NULL);
		if (!s) {
			assert(0);
			return NULL;
		}
		if (sub && sub->nrcols >= 1 && s->nrcols == 0)
			s = stmt_const(sql->sa, bin_first_column(sql->sa, sub), s);
			
		s = stmt_rename(sql, rel, exp, s);
		list_append(pl, s);
	}
	stmt_set_nrcols(psub);

	/* In case of a topn 
		if both order by and distinct: then get first order by col early
			do topn on it. Project all again! Then rest
         */
	if (topn && rel->r) {
		list *oexps = rel->r, *npl = list_new(sql->sa);
		/* including bounds, topn returns atleast N */
		int including = need_including(topn) || need_distinct(rel);
		stmt *limit = NULL; 

		for (n=oexps->h; n; n = n->next) {
			sql_exp *orderbycole = n->data; 
 			int inc = including || n->next;

			stmt *orderbycolstmt = exp_bin(sql, orderbycole, sub, psub, NULL, NULL); 

			if (!orderbycolstmt) 
				return NULL;
			
			if (!limit) {	/* topn based on a single column */
				limit = stmt_limit(sql->sa, orderbycolstmt, stmt_atom_wrd(sql->sa, 0), l, LIMIT_DIRECTION(is_ascending(orderbycole), 1, inc));
			} else { 	/* topn based on 2 columns */
				stmt *obc = stmt_project(sql->sa, stmt_mirror(sql->sa, limit), orderbycolstmt);
				limit = stmt_limit2(sql->sa, limit, obc, stmt_atom_wrd(sql->sa, 0), l, LIMIT_DIRECTION(is_ascending(orderbycole), 1, inc));
			}
			if (!limit) 
				return NULL;
		}

		limit = stmt_mirror(sql->sa, limit);
		for ( n=pl->h ; n; n = n->next) 
			list_append(npl, stmt_project(sql->sa, limit, column(sql->sa, n->data)));
		psub = stmt_list(sql->sa, npl);

		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		pl = sub->op4.lval;
		npl = list_new(sql->sa);
		for ( n=pl->h ; n; n = n->next) {
			list_append(npl, stmt_project(sql->sa, limit, column(sql->sa, n->data))); 
		}
		sub = stmt_list(sql->sa, npl);
	}
	if (need_distinct(rel)) {
		psub = rel2bin_distinct(sql, psub);
		/* also rebuild sub as multiple orderby expressions may use the sub table (ie aren't part of the result columns) */
		if (sub) {
			list *npl = list_new(sql->sa);
			stmt *distinct = stmt_mirror(sql->sa, psub->op4.lval->h->data);
			
			pl = sub->op4.lval;
			for ( n=pl->h ; n; n = n->next) 
				list_append(npl, stmt_project(sql->sa, distinct, column(sql->sa, n->data))); 
			sub = stmt_list(sql->sa, npl);
		}
	}
	if ((!topn || need_distinct(rel)) && rel->r) {
		list *oexps = rel->r;
		stmt *orderby = NULL;

		for (en = oexps->h; en; en = en->next) {
			sql_exp *orderbycole = en->data; 
			stmt *orderbycolstmt = exp_bin(sql, orderbycole, sub, psub, NULL, NULL); 

			if (!orderbycolstmt) {
				assert(0);
				return NULL;
			}
			/* single values don't need sorting */
			if (orderbycolstmt->nrcols == 0) {
				orderby = NULL;
				break;
			}
			if (orderby)
				orderby = stmt_reorder(sql->sa, orderby, orderbycolstmt, is_ascending(orderbycole));
			else
				orderby = stmt_order(sql->sa, orderbycolstmt, is_ascending(orderbycole));
		}
		if (orderby)
			/*psub = stmt_ordered(sql->sa, orderby, psub);*/
			psub = sql_reorder(sql, orderby, psub);
	}
	return psub;
}

static stmt *
rel2bin_predicate(mvc *sql) 
{
	return const_column(sql->sa, stmt_bool(sql->sa, 1));
}

static stmt *
rel2bin_hash_lookup( mvc *sql, sql_rel *rel, stmt *sub, sql_idx *i, node *en ) 
{
	sql_subtype *it = sql_bind_localtype("int");
	sql_subtype *wrd = sql_bind_localtype("wrd");
	stmt *h = NULL;
	stmt *bits = stmt_atom_int(sql->sa, 1 + ((sizeof(wrd)*8)-1)/(list_length(i->columns)+1));
	sql_exp *e = en->data;
	sql_exp *l = e->l;
	stmt *idx = bin_find_column(sql->sa, sub, l->l, sa_strconcat(sql->sa, "%", i->base.name));

	/* TODO should be in key order! */
	for( en = rel->exps->h; en; en = en->next ) {
		sql_exp *e = en->data;
		stmt *s = NULL;

		if (e->type == e_cmp && e->flag == cmp_equal)
			s = exp_bin(sql, e->r, NULL, NULL, NULL, NULL);

		if (!s) 
			return NULL;
		if (h) {
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", wrd, it, tail_type(s), wrd);

			h = stmt_Nop(sql->sa, stmt_list(sql->sa, list_append( list_append(
				list_append(list_new(sql->sa), h), bits), s)), xor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", tail_type(s), NULL, wrd);

			h = stmt_unop(sql->sa, s, hf);
		}
	}
	return stmt_uselect(sql->sa, idx, h, cmp_equal);
}


static stmt *
rel2bin_select( mvc *sql, sql_rel *rel, list *refs)
{
	list *l; 
	node *en, *n;
	stmt *sub = NULL, *sel = NULL;
	stmt *predicate = NULL;

	if (!rel->exps) {
		assert(0);
		return NULL;
	}

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(sql, rel->l, refs);
		if (!sub) 
			return NULL;	
		sub = row2cols(sql, sub);
	} else {
		predicate = rel2bin_predicate(sql);
	}
	if (!rel->exps->h) {
		if (sub)
			return sub;
		return predicate;
	}
	/* handle possible index lookups */
	/* expressions are in index order ! */
	if (sub && (en = rel->exps->h) != NULL) { 
		sql_exp *e = en->data;
		prop *p;

		if ((p=find_prop(e->p, PROP_HASHIDX)) != NULL) {
			sql_idx *i = p->value;
			
			sel = rel2bin_hash_lookup(sql, rel, sub, i, en);
		}
	} 
	if (!sel) {
		sel = stmt_relselect_init(sql->sa);
		for( en = rel->exps->h; en; en = en->next ) {
			/*stmt *s = exp_bin(sql, en->data, sub, NULL, NULL, sel);*/
			stmt *s = exp_bin(sql, en->data, sub, NULL, NULL, NULL);
	
			if (!s) {
				assert(0);
				return NULL;
			}
			if (s->nrcols == 0){ 
				if (!predicate) 
					predicate = rel2bin_predicate(sql);
				predicate = stmt_select(sql->sa, predicate, s, cmp_equal);
			} else {
				stmt_relselect_fill(sel, s);
			}
		}
	}

	if (predicate && sel) {
		if (list_length(sel->op1->op4.lval) == 0) {
			sel = NULL;
		} else {
			sel = stmt_join(sql->sa, sel, predicate, cmp_all);
			predicate = NULL;
			if (!sub)
				predicate = sel;
		}
	}
	/* construct relation */
	l = list_new(sql->sa);
	if (sub && sel) {
		sel = stmt_mark(sql->sa, stmt_reverse(sql->sa, sel),0);
		for( n = sub->op4.lval->h; n; n = n->next ) {
			stmt *col = n->data;
	
			if (col->nrcols == 0) /* constant */
				col = stmt_const(sql->sa, sel, col);
			else
				col = stmt_project(sql->sa, sel, col);
			list_append(l, col);
		}
	} else if (sub && predicate) {
		stmt *h = NULL;
		n = sub->op4.lval->h;
		h = stmt_join(sql->sa,  column(sql->sa, n->data), predicate, cmp_all);
		h = stmt_reverse(sql->sa, stmt_mark_tail(sql->sa, h, 0)); 
		for( n = sub->op4.lval->h; n; n = n->next ) {
			stmt *col = n->data;
	
			if (col->nrcols == 0) /* constant */
				col = stmt_const(sql->sa, h, col);
			else
				col = stmt_join(sql->sa, h, col, cmp_equal);
			list_append(l, col);
		}
	} else if (predicate) {
		list_append(l, predicate);
	}
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_groupby( mvc *sql, sql_rel *rel, list *refs)
{
	list *l, *aggrs, *gbexps = list_new(sql->sa);
	node *n, *en;
	stmt *sub = NULL, *cursub;
	group *groupby = NULL;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(sql, rel->l, refs);
		if (!sub)
			return NULL;	
	}

	if (sub && sub->type == st_list && sub->op4.lval->h && !((stmt*)sub->op4.lval->h->data)->nrcols) {
		list *newl = list_new(sql->sa);
		node *n;

		for(n=sub->op4.lval->h; n; n = n->next) {
			char *cname = column_name(sql->sa, n->data);
			char *tname = table_name(sql->sa, n->data);
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
			stmt *gbcol = exp_bin(sql, e, sub, NULL, NULL, NULL); 
	
			if (!gbcol) {
				assert(0);
				return NULL;
			}
			groupby = grp_create(sql->sa, gbcol, groupby);
			gbcol = stmt_alias(sql->sa, gbcol, exp_find_rel_name(e), exp_name(e));
			list_append(gbexps, gbcol);
		}
	}
	grp_done(groupby);
	/* now aggregate */
	l = list_new(sql->sa);
	aggrs = rel->exps;
	cursub = stmt_list(sql->sa, l);
	for( n = aggrs->h; n; n = n->next ) {
		sql_exp *aggrexp = n->data;

		stmt *aggrstmt = NULL;

		/* first look in the group by column list */
		if (gbexps && !aggrstmt && aggrexp->type == e_column) {
			aggrstmt = list_find_column(sql->sa, gbexps, aggrexp->l, aggrexp->r);
			if (aggrstmt && groupby)
				aggrstmt = stmt_join(sql->sa, groupby->ext, aggrstmt, cmp_equal);
		}

		if (!aggrstmt)
			aggrstmt = exp_bin(sql, aggrexp, sub, NULL, groupby, NULL); 
		/* maybe the aggr uses intermediate results of this group by,
		   therefore we pass the group by columns too 
		 */
		if (!aggrstmt) 
			aggrstmt = exp_bin(sql, aggrexp, sub, cursub, groupby, NULL); 
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
	list *newl;
	sql_exp *oe = NULL, *le = NULL;
	stmt *sub = NULL, *order = NULL, *l = NULL, *o = NULL;
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

	if (sub->type == st_ordered) {
		stmt *s = sub->op2;
		order = column(sql->sa, sub->op1);
		sub = s;
	}
	n = sub->op4.lval->h;
	newl = list_new(sql->sa);

	if (n) {
		stmt *limit = NULL;
		/*
		sql_rel *rl = rel->l;
		int including = (rl && need_distinct(rl)) || need_including(rel);
		*/
		int including = need_including(rel);

		if (le)
			l = exp_bin(sql, le, NULL, NULL, NULL, NULL);
		if (oe)
			o = exp_bin(sql, oe, NULL, NULL, NULL, NULL);

		if (!l) 
			l = stmt_atom_wrd_nil(sql->sa);
		if (!o)
			o = stmt_atom_wrd(sql->sa, 0);

		if (order) {
		 	limit = stmt_limit(sql->sa, order, o, l, LIMIT_DIRECTION(0,0,including));
		} else {
			stmt *sc = n->data;
			char *cname = column_name(sql->sa, sc);
			char *tname = table_name(sql->sa, sc);

			sc = column(sql->sa, sc);
			limit = stmt_limit(sql->sa, stmt_alias(sql->sa, sc, tname, cname), o, l, LIMIT_DIRECTION(0,0,including));
		}

		limit = stmt_mirror(sql->sa, limit);
		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			char *cname = column_name(sql->sa, sc);
			char *tname = table_name(sql->sa, sc);
		
			sc = column(sql->sa, sc);
			sc = stmt_project(sql->sa, limit, sc);
			list_append(newl, stmt_alias(sql->sa, sc, tname, cname));
		}
		if (order) 
			order = stmt_project(sql->sa, limit, order);
	}
	sub = stmt_list(sql->sa, newl);
	if (order) 
		return stmt_ordered(sql->sa, order, sub);
	return sub;
}

static stmt *
rel2bin_sample( mvc *sql, sql_rel *rel, list *refs)
{
	list *newl;
	stmt *sub = NULL, *s = NULL, *sample = NULL;
	node *n;

	if (rel->l) { /* first construct the sub relation */
		sub = subrel_bin(sql, rel->l, refs);
		if (!sub)
			return NULL;
	}

	n = sub->op4.lval->h;
	newl = list_new(sql->sa);

	if (n) {
		stmt *sc = n->data;
		char *cname = column_name(sql->sa, sc);
		char *tname = table_name(sql->sa, sc);

		s = exp_bin(sql, rel->exps->h->data, NULL, NULL, NULL, NULL);

		if (!s)
			s = stmt_atom_wrd_nil(sql->sa);

		sc = column(sql->sa, sc);
		sample = stmt_sample(sql->sa, stmt_alias(sql->sa, sc, tname, cname),s);

		sample = stmt_mirror(sql->sa, sample);
		for ( ; n; n = n->next) {
			stmt *sc = n->data;
			char *cname = column_name(sql->sa, sc);
			char *tname = table_name(sql->sa, sc);
		
			sc = column(sql->sa, sc);
			sc = stmt_project(sql->sa, sample, sc);
			list_append(newl, stmt_alias(sql->sa, sc, tname, cname));
		}
	}
	sub = stmt_list(sql->sa, newl);
	return sub;
}

static stmt *
nth( list *l, int n)
{
	int i;
	node *m;

	for (i=0, m = l->h; i<n && m; i++, m = m->next) ; 
	if (m)
		return m->data;
	return NULL;
}

static stmt *
insert_check_ukey(mvc *sql, list *inserts, sql_key *k, stmt *idx_inserts)
{
/* pkey's cannot have NULLs, ukeys however can
   current implementation switches on 'NOT NULL' on primary key columns */

	char *msg = NULL;
	stmt *res;

	sql_subtype *wrd = sql_bind_localtype("wrd");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subtype *bt = sql_bind_localtype("bit");
	stmt *ts = stmt_basetable(sql->sa, k->t, k->t->base.name);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", wrd, wrd, bt);

	if (list_length(k->columns) > 1) {
		node *m;
		stmt *s = nth(inserts, 0)->op1;
		sql_subaggr *sum;
		stmt *ssum = NULL;
		stmt *col = NULL;

		/* 1st stage: find out if original contains same values */
		if (s->key && s->nrcols == 0) {
			s = stmt_relselect_init(sql->sa);
			if (k->idx && hash_index(k->idx->type))
				stmt_relselect_fill(s, stmt_uselect(sql->sa, stmt_idxbat(sql->sa, k->idx, RDONLY), idx_inserts, cmp_equal));
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;

				col = stmt_bat(sql->sa, c->c, ts, RDONLY);
				if ((k->type == ukey) && stmt_has_null(col)) {
					sql_subtype *t = tail_type(col);
					stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
					col = stmt_select2(sql->sa, col, n, n, 0);
				}
				stmt_relselect_fill(s, stmt_uselect( sql->sa, col, nth(inserts, c->c->colnr)->op1, cmp_equal));
			}
		} else {
			s = stmt_releqjoin_init(sql->sa);
			if (k->idx && hash_index(k->idx->type))
				stmt_releqjoin_fill(s, stmt_idxbat(sql->sa, k->idx, RDONLY), idx_inserts);
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;

				col = stmt_bat(sql->sa, c->c, ts, RDONLY);
				if ((k->type == ukey) && stmt_has_null(col)) {
					sql_subtype *t = tail_type(col);
					stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
					col = stmt_select2(sql->sa, col, n, n, 0);
				}
				stmt_releqjoin_fill(s, col, nth(inserts, c->c->colnr)->op1);
			}
		}
		s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, cnt, 1), stmt_atom_wrd(sql->sa, 0), ne);

		/* 2e stage: find out if inserted are unique */
		if ((!idx_inserts && nth(inserts,0)->nrcols) || (idx_inserts && idx_inserts->nrcols)) {	/* insert columns not atoms */
			stmt *ss = NULL;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);
			/* implementation uses sort,refine, key check */
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;

				if (ss)  
					ss = stmt_reorder(sql->sa, ss, nth(inserts, c->c->colnr)->op1, 1);
				else
					ss = stmt_order(sql->sa, nth(inserts, c->c->colnr)->op1, 1);
			}

			sum = sql_bind_aggr(sql->sa, sql->session->schema, "not_unique", tail_type(ss));
			ssum = stmt_aggr(sql->sa, ss, NULL, sum, 1);
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
		stmt *s, *h = nth(inserts, c->c->colnr)->op1;

		s = stmt_bat(sql->sa, c->c, ts, RDONLY);
		if ((k->type == ukey) && stmt_has_null(s)) {
			sql_subtype *t = tail_type(h);
			stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
			s = stmt_select2(sql->sa, s, n, n, 0);
		}
		if (h->nrcols) {
			s = stmt_join(sql->sa, s, stmt_reverse(sql->sa, h), cmp_equal);
			/* s should be empty */
			s = stmt_aggr(sql->sa, s, NULL, cnt, 1);
		} else {
			s = stmt_uselect(sql->sa, s, h, cmp_equal);
			/* s should be empty */
			s = stmt_aggr(sql->sa, s, NULL, cnt, 1);
		}
		/* s should be empty */
		s = stmt_binop(sql->sa, s, stmt_atom_wrd(sql->sa, 0), ne);

		/* 2e stage: find out if inserts are unique */
		if (h->nrcols) {	/* insert multiple atoms */
			sql_subaggr *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);
			stmt *ssum, *ss;

			stmt *ins = nth(inserts, c->c->colnr)->op1;
			group *g = grp_create(sql->sa, ins, NULL);

			grp_done(g);
			ss = stmt_aggr(sql->sa, g->grp, g, cnt, 1);
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_aggr(sql->sa, sql->session->schema, "sum", tail_type(ss));
			ssum = stmt_aggr(sql->sa, ss, NULL, sum, 1);
			ssum = sql_Nop_(sql, "ifthenelse", sql_unop_(sql, NULL, "isnull", ssum), stmt_atom_wrd(sql->sa, 0), ssum, NULL);
			count_sum = stmt_binop(sql->sa, check_types(sql, tail_type(ssum), stmt_aggr(sql->sa, ss, NULL, cnt, 1), type_equal), ssum, ne);

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
	stmt *s = nth(inserts, 0)->op1;
	sql_subtype *wrd = sql_bind_localtype("wrd");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", wrd, wrd, bt);

	(void) sql;		/* unused! */

	if (pin && list_length(pin->op4.lval)) 
		s = pin->op4.lval->h->data;
	if (s->key && s->nrcols == 0) {
		s = stmt_binop(sql->sa, stmt_aggr(sql->sa, idx_inserts, NULL, cnt, 1), stmt_atom_wrd(sql->sa, 1), ne);
	} else {
		/* releqjoin.count <> inserts[col1].count */
		s = stmt_binop(sql->sa, stmt_aggr(sql->sa, idx_inserts, NULL, cnt, 1), stmt_aggr(sql->sa, s, NULL, cnt, 1), ne);
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
sql_stack_add_inserted( mvc *sql, char *name, sql_table *t) 
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
		int *trigger_id = NEW(int);
		*trigger_id = trigger->base.id;

		stack_push_frame(sql, "OLD-NEW");
		if (trigger->event == 0) { 
			stmt *s = NULL;
			char *n = trigger->new_name;

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
				s = stmt_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
				s = stmt_uselect(sql->sa, i->op1, s, cmp_equal);
				s = stmt_aggr(sql->sa, s, NULL, cnt, 1);
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
	int idx_ins = 0;
	node *n, *m;
	sql_rel *tr = rel->l, *prel = rel->r;
	sql_table *t = NULL;

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

	if (inserts->type == st_ordered) {
		stmt *n = sql_reorder(sql, inserts->op1, inserts->op1);
		inserts = n;
	}

	if (idx_ins)
		pin = refs_find_rel(refs, prel);

	newl = list_new(sql->sa);
	for (n = t->columns.set->h, m = inserts->op4.lval->h; 
		n && m; n = n->next, m = m->next) {

		stmt *ins = m->data;
		sql_column *c = n->data;

		insert = ins = stmt_append_col(sql->sa, c, ins);
		if (rel->flag&UPD_LOCKED) /* fake append (done in the copy into) */
			ins->flag = 1;
		list_append(newl, ins);
	}
	l = list_new(sql->sa);

	if (t->idxs.set)
	for (n = t->idxs.set->h; n && m; n = n->next, m = m->next) {
		stmt *is = m->data;
		sql_idx *i = n->data;

		if ((hash_index(i->type) && list_length(i->columns) <= 1) ||
		    i->type == no_idx)
			is = NULL;
		if (i->key) {
			stmt *ckeys = sql_insert_key(sql, newl, i->key, is, pin);

			list_prepend(l, ckeys);
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

	l = list_append(l, stmt_list(sql->sa, newl));
	sql_insert_check_null(sql, t, newl, l);
	if (!sql_insert_triggers(sql, t, l)) 
		return sql_error(sql, 02, "INSERT INTO: triggers failed for table '%s'", t->base.name);
	if (insert->op1->nrcols == 0) {
		s = stmt_atom_wrd(sql->sa, 1);
	} else {
		s = stmt_aggr(sql->sa, insert->op1, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1);
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
table_update_stmts(sql_table *t, int *Len)
{
	stmt **updates;
	int i, len = list_length(t->columns.set);
	node *m;

	*Len = len;
	updates = NEW_ARRAY(stmt *, len);
	for (m = t->columns.set->h, i = 0; m; m = m->next, i++) {
		sql_column *c = m->data;

		/* update the column number, for correct array access */
		c->colnr = i;
		updates[i] = NULL;
	}
	return updates;
}

static stmt *
update_check_ukey(mvc *sql, stmt **updates, sql_key *k, stmt *idx_updates, int updcol)
{
	char *msg = NULL;
	stmt *res = NULL;

	sql_subtype *wrd = sql_bind_localtype("wrd");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne;

	ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", wrd, wrd, bt);
	if (list_length(k->columns) > 1) {
		stmt *ts = stmt_basetable(sql->sa, k->t, k->t->base.name);
		node *m;
		stmt *s = NULL;

		/* 1st stage: find out if original (without the updated) 
			do not contain the same values as the updated values. 
			This is done using a relation join and a count (which 
			should be zero)
	 	*/
		/* TODO split null removal and join/group (to make mkey save) */
		if (!isNew(k)) {
			s = stmt_releqjoin_init(sql->sa);
			if (k->idx && hash_index(k->idx->type))
				stmt_releqjoin_fill(s, stmt_diff(sql->sa, stmt_idxbat(sql->sa, k->idx, RDONLY), idx_updates), idx_updates);
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd, *l;

				assert(updates);
				if (updates[c->c->colnr]) {
					upd = updates[c->c->colnr]->op1;
				} else {
					upd = stmt_semijoin(sql->sa, stmt_bat(sql->sa, c->c, ts, RDONLY), updates[updcol]->op1);
				}
				if ((k->type == ukey) && stmt_has_null(upd)) {
					sql_subtype *t = tail_type(upd);
					stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
					upd = stmt_select2(sql->sa, upd, n, n, 0);
				}

				l = stmt_diff(sql->sa, stmt_bat(sql->sa, c->c, ts, RDONLY), upd);
				stmt_releqjoin_fill(s, l, upd);

			}
			s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, cnt, 1), stmt_atom_wrd(sql->sa, 0), ne);
		}

		/* 2e stage: find out if the updated are unique */
		if (!updates || updates[updcol]->op1->nrcols) {	/* update columns not atoms */
			sql_subaggr *sum;
			stmt *count_sum = NULL, *ssum;
			group *g = NULL;
			stmt *ss;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);

			/* also take the hopefully unique hash keys, to reduce
			   (re)group costs */
			if (k->idx && hash_index(k->idx->type))
				g = grp_create(sql->sa, idx_updates, g);
			for (m = k->columns->h; m; m = m->next) {
				sql_kc *c = m->data;
				stmt *upd;

				if (updates && updates[c->c->colnr]) {
					upd = updates[c->c->colnr]->op1;
				} else if (updates) {
					upd = updates[updcol]->op1;
					upd = stmt_semijoin(sql->sa, stmt_bat(sql->sa, c->c, ts, RDONLY), upd);
				} else {
					upd = stmt_bat(sql->sa, c->c, ts, RDONLY);
				}
				/* remove nulls */
				if ((k->type == ukey) && stmt_has_null(upd)) {
					sql_subtype *t = tail_type(upd);
					stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
					upd = stmt_select2(sql->sa, upd, n, n, 0);
				}

				g = grp_create(sql->sa, upd, g);
			}
			grp_done(g);
			ss = stmt_aggr(sql->sa, g->grp, g, cnt, 1);
			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_aggr(sql->sa, sql->session->schema, "sum", tail_type(ss));
			ssum = stmt_aggr(sql->sa, ss, NULL, sum, 1);
			ssum = sql_Nop_(sql, "ifthenelse", sql_unop_(sql, NULL, "isnull", ssum), stmt_atom_wrd(sql->sa, 0), ssum, NULL);
			count_sum = stmt_binop(sql->sa, stmt_aggr(sql->sa, ss, NULL, cnt, 1), check_types(sql, wrd, ssum, type_equal), ne);

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
		stmt *ts = stmt_basetable(sql->sa, k->t, k->t->base.name);
		sql_kc *c = k->columns->h->data;
		stmt *s = NULL, *h = NULL, *o;

		/* s should be empty */
		if (!isNew(k)) {
			assert (updates);

			h = updates[c->c->colnr]->op1;
			o = stmt_diff(sql->sa, stmt_bat(sql->sa, c->c, ts, RDONLY), h);
			s = stmt_join(sql->sa, o, stmt_reverse(sql->sa, h), cmp_equal);
			s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, cnt, 1), stmt_atom_wrd(sql->sa, 0), ne);
		}

		/* 2e stage: find out if updated are unique */
		if (!h || h->nrcols) {	/* update columns not atoms */
			sql_subaggr *sum;
			stmt *count_sum = NULL;
			sql_subfunc *or = sql_bind_func_result(sql->sa, sql->session->schema, "or", bt, bt, bt);
			stmt *ssum, *ss;
			stmt *upd;
			group *g;

			if (updates) {
 				upd = updates[c->c->colnr]->op1;
			} else {
 				upd = stmt_bat(sql->sa, c->c, ts, RDONLY);
			}
			/* remove nulls */
			if ((k->type == ukey) && stmt_has_null(upd)) {
				sql_subtype *t = tail_type(upd);
				stmt *n = stmt_atom(sql->sa, atom_general(sql->sa, t, NULL));
				upd = stmt_select2(sql->sa, upd, n, n, 0);
			}

			g = grp_create(sql->sa, upd, NULL);
			grp_done(g);
			ss = stmt_aggr(sql->sa, g->grp, g, cnt, 1);

			/* (count(ss) <> sum(ss)) */
			sum = sql_bind_aggr(sql->sa, sql->session->schema, "sum", tail_type(ss));
			ssum = stmt_aggr(sql->sa, ss, NULL, sum, 1);
			ssum = sql_Nop_(sql, "ifthenelse", sql_unop_(sql, NULL, "isnull", ssum), stmt_atom_wrd(sql->sa, 0), ssum, NULL);
			count_sum = stmt_binop(sql->sa, check_types(sql, tail_type(ssum), stmt_aggr(sql->sa, ss, NULL, cnt, 1), type_equal), ssum, ne);

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

static stmt *
update_check_fkey(mvc *sql, stmt **updates, sql_key *k, stmt *idx_updates, int updcol, stmt *pup)
{
	char *msg = NULL;
	stmt *s;
	sql_subtype *wrd = sql_bind_localtype("wrd");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", wrd, wrd, bt);
	stmt *cur;

	if (!idx_updates)
		return NULL;
	/* releqjoin.count <> updates[updcol].count */
	if (pup && list_length(pup->op4.lval)) {
		cur = pup->op4.lval->h->data;
	} else if (updates) {
		cur = updates[updcol]->op1;
	} else {
		sql_kc *c = k->columns->h->data;
		stmt *ts = stmt_basetable(sql->sa, k->t, k->t->base.name);
		cur = stmt_bat(sql->sa, c->c, ts, RDONLY);
	}
	s = stmt_binop(sql->sa, stmt_aggr(sql->sa, idx_updates, NULL, cnt, 1), stmt_aggr(sql->sa, cur, NULL, cnt, 1), ne);

	/* s should be empty */
	msg = sa_message(sql->sa, "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(sql->sa, s, msg, 00001);
}

static stmt *
join_updated_pkey(mvc *sql, sql_key * k, stmt **updates, int updcol)
{
	char *msg = NULL;
	int nulls = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt *s = NULL, *ts = stmt_basetable(sql->sa, rk->t, rk->t->base.name), *fts;
	stmt *null = NULL, *rows;
	sql_subtype *wrd = sql_bind_localtype("wrd");
	sql_subtype *bt = sql_bind_localtype("bit");
	sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
	sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", wrd, wrd, bt);

	fts = stmt_basetable(sql->sa, k->idx->t, k->idx->t->base.name);
	s = stmt_releqjoin_init(sql->sa);

	rows = stmt_idxbat(sql->sa, k->idx, RDONLY);
	rows = stmt_semijoin(sql->sa, stmt_reverse(sql->sa, rows), updates[updcol]->op1);
	rows = stmt_reverse(sql->sa, rows);

	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd;

		if (updates[c->c->colnr]) {
			upd = updates[c->c->colnr]->op1;
		} else {
			upd = updates[updcol]->op1;
			upd = stmt_semijoin(sql->sa, stmt_bat(sql->sa, c->c, ts, RDONLY), upd);
		}
		if (c->c->null) {	/* new nulls (MATCH SIMPLE) */
			stmt *nn = upd;

			nn = stmt_uselect(sql->sa, nn, stmt_atom(sql->sa, atom_general(sql->sa, &c->c->type, NULL)), cmp_equal);
			if (null)
				null = stmt_semijoin(sql->sa, null, nn);
			else
				null = nn;
			nulls = 1;
		}
		stmt_releqjoin_fill(s, upd, stmt_semijoin(sql->sa, stmt_bat(sql->sa, fc->c, fts, RDONLY), rows ));
	}
	/* add missing nulls */
	if (nulls)
		s = stmt_union(sql->sa, s, stmt_const(sql->sa, null, stmt_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("oid"), NULL))));

	/* releqjoin.count <> updates[updcol].count */
	s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, cnt, 1), stmt_aggr(sql->sa, rows, NULL, cnt, 1), ne);

	/* s should be empty */
	msg = sa_message(sql->sa, "UPDATE: FOREIGN KEY constraint '%s.%s' violated", k->t->base.name, k->base.name);
	return stmt_exception(sql->sa, s, msg, 00001);
}

static list * sql_update(mvc *sql, sql_table *t, stmt **updates);

static stmt*
sql_delete_set_Fkeys(mvc *sql, sql_key *k, stmt *rows, int action)
{
	list *l = NULL;
	int len = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt **new_updates;
	sql_table *t = mvc_bind_table(sql, k->t->s, k->t->base.name);

	new_updates = table_update_stmts(t, &len);
	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		stmt *upd = NULL;

		if (action == ACT_SET_DEFAULT) {
			if (fc->c->def) {
				stmt *sq;
				char *msg = sql_message( "select %s;", fc->c->def);
				sq = rel_parse_value(sql, msg, sql->emode);
				_DELETE(msg);
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
			upd = stmt_const(sql->sa, rows, upd);
		
		new_updates[fc->c->colnr] = stmt_update_col(sql->sa, fc->c, upd);
	}

	if ((l = sql_update(sql, t, new_updates)) == NULL) 
		return NULL;
	return stmt_list(sql->sa, l);
}

static stmt*
sql_update_cascade_Fkeys(mvc *sql, sql_key *k, int updcol, stmt **updates, int action)
{
	list *l = NULL;
	int len = 0;
	node *m, *o;
	sql_key *rk = &((sql_fkey*)k)->rkey->k;
	stmt **new_updates;
	stmt *rows;
	sql_table *t = mvc_bind_table(sql, k->t->s, k->t->base.name);

	rows = stmt_idxbat(sql->sa, k->idx, RDONLY);
	rows = stmt_semijoin(sql->sa, stmt_reverse(sql->sa, rows), updates[updcol]->op1);
	rows = stmt_reverse(sql->sa, rows);
		
	new_updates = table_update_stmts(t, &len);
	for (m = k->idx->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *fc = m->data;
		sql_kc *c = o->data;
		stmt *upd = NULL;

		if (!updates[c->c->colnr]) {
			continue;
		} else if (action == ACT_CASCADE) {
			upd = updates[c->c->colnr]->op1;
		} else if (action == ACT_SET_DEFAULT) {
			if (fc->c->def) {
				stmt *sq;
				char *msg = sql_message( "select %s;", fc->c->def);
				sq = rel_parse_value(sql, msg, sql->emode);
				_DELETE(msg);
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
			upd = stmt_const(sql->sa, rows, upd);
		else
			upd = stmt_join(sql->sa, rows, upd, cmp_equal);
		
		new_updates[fc->c->colnr] = stmt_update_col(sql->sa, fc->c, upd);
	}

	if ((l = sql_update(sql, t, new_updates)) == NULL) 
		return NULL;
	return stmt_list(sql->sa, l);
}


static void 
cascade_ukey(mvc *sql, stmt **updates, sql_key *k, int updcol, list *cascade) 
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
					s = sql_update_cascade_Fkeys(sql, fk, updcol, updates, ((sql_fkey*)fk)->on_update);
					list_append(cascade, s);
					break;
				default:	/*RESTRICT*/
					s = join_updated_pkey(sql, fk, updates, updcol);
					list_append(cascade, s);
			}
		}
	}
}

static void
sql_update_check_key(mvc *sql, stmt **updates, sql_key *k, stmt *idx_updates, int updcol, list *l, list *cascade, stmt *pup)
{
	stmt *ckeys;

	if (k->type == pkey || k->type == ukey) {
		ckeys = update_check_ukey(sql, updates, k, idx_updates, updcol);
		if (cascade)
			cascade_ukey(sql, updates, k, updcol, cascade);
	} else { /* foreign keys */
		ckeys = update_check_fkey(sql, updates, k, idx_updates, updcol, pup);
	}
	list_append(l, ckeys);
}

static stmt *
hash_update(mvc *sql, sql_idx * i, stmt **updates, int updcol)
{
	/* calculate new value */
	node *m;
	sql_subtype *it, *wrd;
	int bits = 1 + ((sizeof(wrd)*8)-1)/(list_length(i->columns)+1);
	stmt *h = NULL, *ts, *o = NULL;

	if (list_length(i->columns) <= 1)
		return NULL;

	ts = stmt_basetable(sql->sa, i->t, i->t->base.name);
	it = sql_bind_localtype("int");
	wrd = sql_bind_localtype("wrd");
	for (m = i->columns->h; m; m = m->next ) {
		sql_kc *c = m->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr]->op1;
		} else if (updates && updcol >= 0) {
			upd = updates[updcol]->op1;
			upd = stmt_semijoin(sql->sa, stmt_bat(sql->sa, c->c, ts, RDONLY), upd);
		} else { /* created idx/key using alter */ 
			upd = stmt_bat(sql->sa, c->c, ts, RDONLY);
		}

		if (h && i->type == hash_idx)  { 
			sql_subfunc *xor = sql_bind_func_result3(sql->sa, sql->session->schema, "rotate_xor_hash", wrd, it, &c->c->type, wrd);

			h = stmt_Nop(sql->sa, stmt_list( sql->sa, list_append( list_append(
				list_append(list_new(sql->sa), h), 
				stmt_atom_int(sql->sa, bits)), 
				stmt_join(sql->sa, o, upd, cmp_equal))), 
				xor);
		} else if (h)  { 
			stmt *h2;
			sql_subfunc *lsh = sql_bind_func_result(sql->sa, sql->session->schema, "left_shift", wrd, it, wrd);
			sql_subfunc *lor = sql_bind_func_result(sql->sa, sql->session->schema, "bit_or", wrd, wrd, wrd);
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, wrd);

			h = stmt_binop(sql->sa, h, stmt_atom_int(sql->sa, bits), lsh); 
			h2 = stmt_unop(sql->sa, 
				stmt_join(sql->sa, o, upd, cmp_equal), hf);
			h = stmt_binop(sql->sa, h, h2, lor);
		} else {
			sql_subfunc *hf = sql_bind_func_result(sql->sa, sql->session->schema, "hash", &c->c->type, NULL, wrd);
			o = stmt_mark(sql->sa, stmt_reverse(sql->sa, upd), 40); 
			h = stmt_unop(sql->sa, stmt_mark(sql->sa, upd, 40), hf);
			if (i->type == oph_idx)
				break;
		}
	}
	return stmt_join(sql->sa, stmt_reverse(sql->sa, o), h, cmp_equal);
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
join_idx_update(mvc *sql, sql_idx * i, stmt **updates, int updcol)
{
	int nulls = 0, len;
	node *m, *o;
	sql_key *rk = &((sql_fkey *) i->key)->rkey->k;
	stmt *s = NULL, *rts = stmt_basetable(sql->sa, rk->t, rk->t->base.name), *ts;
	stmt *null = NULL, *nnull = NULL;
	stmt **new_updates = table_update_stmts(i->t, &len);
	sql_column *updcolumn = NULL; 

	ts = stmt_basetable(sql->sa, i->t, i->t->base.name);
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		stmt *upd;

		if (updates && updates[c->c->colnr]) {
			upd = updates[c->c->colnr]->op1;
		} else if (updates && updcol >= 0) {
			upd = updates[updcol]->op1;
			upd = stmt_semijoin(sql->sa, stmt_bat(sql->sa, c->c, ts, RDONLY), upd);
		} else { /* created idx/key using alter */ 
			upd = stmt_bat(sql->sa, c->c, ts, RDONLY);
			updcolumn = c->c;
		}
		new_updates[c->c->colnr] = upd;

		/* FOR MATCH FULL/SIMPLE/PARTIAL see above */
		/* Currently only the default MATCH SIMPLE is supported */
		if (c->c->null) {
			stmt *nn = upd;

			nn = stmt_uselect(sql->sa, nn, stmt_atom(sql->sa, atom_general(sql->sa, &c->c->type, NULL)), cmp_equal);
			if (null)
				null = stmt_union(sql->sa, null, nn);
			else
				null = nn;
			nulls = 1;
		}

	}

	/* we only need to check non null values */
	if (nulls && updates) 
		/* convert nulls to table ids */
		nnull = stmt_diff(sql->sa, updates[updcol]->op1, null);
	else if (nulls) /* no updates (only new idx/key) */
		nnull = stmt_diff(sql->sa, stmt_bat(sql->sa, updcolumn, ts, RDONLY), null);


	s = stmt_releqjoin_init(sql->sa);
	for (m = i->columns->h, o = rk->columns->h; m && o; m = m->next, o = o->next) {
		sql_kc *c = m->data;
		sql_kc *rc = o->data;
		stmt *upd = new_updates[c->c->colnr];

		if (nulls) /* remove nulls */
			upd = stmt_semijoin(sql->sa, upd, nnull); 
		stmt_releqjoin_fill(s, check_types(sql, &rc->c->type, upd, type_equal), stmt_bat(sql->sa, rc->c, rts, RDONLY));
	}
	/* add missing nulls */
	if (nulls)
		s = stmt_union(sql->sa, s, stmt_const(sql->sa, null, stmt_atom(sql->sa, atom_general(sql->sa, sql_bind_localtype("oid"), NULL))));

	_DELETE(new_updates);
	return s;
}

static list *
update_idxs_and_check_keys(mvc *sql, sql_table *t, stmt **updates, list *l, list **cascades)
{
	node *n;
	int updcol;
	list *idx_updates = list_new(sql->sa);

	if (!t->idxs.set)
		return idx_updates;

	*cascades = list_new(sql->sa);
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
			is = join_idx_update(sql, i, updates, updcol);
		}
		if (i->key) {
			if (!(sql->cascade_action && list_find_id(sql->cascade_action, i->key->base.id))) {
				int *local_id = GDKmalloc(sizeof(int));
				if (!sql->cascade_action) 
					sql->cascade_action = list_create((fdestroy) GDKfree);
				
				*local_id = i->key->base.id;
				list_append(sql->cascade_action, local_id);
				sql_update_check_key(sql, updates, i->key, is, updcol, l, *cascades, NULL);
			}
		}
		if (is) 
			list_append(idx_updates, stmt_update_idx(sql->sa, i, is));
	}
	return idx_updates;
}

static void
sql_stack_add_updated(mvc *sql, char *on, char *nn, sql_table *t)
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
		int *trigger_id = NEW(int);
		*trigger_id = trigger->base.id;

		stack_push_frame(sql, "OLD-NEW");
		if (trigger->event == 2 && trigger->time == time) {
			stmt *s = NULL;
	
			/* add name for the 'inserted' to the stack */
			char *n = trigger->new_name;
			char *o = trigger->old_name;
	
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
			stmt *s = updates[c->colnr]->op1;
			char *msg = NULL;

			if (!(s->key && s->nrcols == 0)) {
				s = stmt_atom(sql->sa, atom_general(sql->sa, &c->type, NULL));
				s = stmt_uselect(sql->sa, updates[c->colnr]->op1, s, cmp_equal);
				s = stmt_aggr(sql->sa, s, NULL, cnt, 1);
			} else {
				sql_subfunc *isnil = sql_bind_func(sql->sa, sql->session->schema, "isnull", &c->type, NULL, F_FUNC);

				s = stmt_unop(sql->sa, updates[c->colnr]->op1, isnil);
			}
			msg = sa_message(sql->sa, "UPDATE: NOT NULL constraint violated for column '%s.%s'", c->t->base.name, c->base.name);
			s = stmt_exception(sql->sa, s, msg, 00001);

			list_append(l, s);
		}
	}
}

static list *
sql_update(mvc *sql, sql_table *t, stmt **updates)
{
	list *idx_updates = NULL, *cascades = NULL;
	int i, nr_cols = list_length(t->columns.set);
	list *l = list_new(sql->sa);

	sql_update_check_null(sql, t, updates, l);

	/* check keys + get idx */
	idx_updates = update_idxs_and_check_keys(sql, t, updates, l, &cascades);
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
	_DELETE(updates);
	return l;
}

/* updates with empty list is alter with create idx or keys */
static stmt *
rel2bin_update( mvc *sql, sql_rel *rel, list *refs)
{
	stmt *update = NULL, **updates = NULL, *tid, *s, *ddl = NULL, *pup = NULL;
	list *l = list_new(sql->sa), *idx_updates = NULL, *cascades = NULL;
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

	updates = table_update_stmts(t, &nr_cols);
	tid = update->op4.lval->h->data;

	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_column *c = find_sql_column(t, ce->name);

		if (c) {
			stmt *s = bin_find_column(sql->sa, update, ce->l, ce->r);
			s = stmt_join(sql->sa, stmt_reverse(sql->sa, tid), s, cmp_equal);
			updates[c->colnr] = stmt_update_col(sql->sa,  c, s);
		}
	}
	sql_update_check_null(sql, t, updates, l);

	/* check keys + get idx */
	cascades = list_new(sql->sa);
	updcol = first_updated_col(updates, list_length(t->columns.set));
	for (m = rel->exps->h; m; m = m->next) {
		sql_exp *ce = m->data;
		sql_idx *i = find_sql_idx(t, ce->name);

		if (i) {
			stmt *is = bin_find_column(sql->sa, update, ce->l, ce->r);

			is = stmt_join(sql->sa, stmt_reverse(sql->sa, tid), is, cmp_equal);
			if ((hash_index(i->type) && list_length(i->columns) <= 1) || i->type == no_idx)
				is = NULL;
			if (i->key) {
				if (!(sql->cascade_action && list_find_id(sql->cascade_action, i->key->base.id))) {
					int *local_id = GDKmalloc(sizeof(int));
					if (!sql->cascade_action) 
						sql->cascade_action = list_create((fdestroy) GDKfree);
				
					*local_id = i->key->base.id;
					list_append(sql->cascade_action, local_id);
					sql_update_check_key(sql, (updcol>=0)?updates:NULL, i->key, is, updcol, l, cascades, pup);
				}
			}
			if (is) 
				list_append(l, stmt_update_idx(sql->sa,  i, is));
		}
	}

/* before */
	if (!sql_update_triggers(sql, t, l, 0)) {
		_DELETE(updates);
		return sql_error(sql, 02, "UPDATE: triggers failed for table '%s'", t->base.name);
	}

/* apply updates */
	list_merge(l, idx_updates, NULL);
	for (i = 0; i < nr_cols; i++) 
		if (updates[i])
			list_append(l, updates[i]);
	_DELETE(updates);

/* after */
	if (!sql_update_triggers(sql, t, l, 1)) 
		return sql_error(sql, 02, "UPDATE: triggers failed for table '%s'", t->base.name);

/* cascade */
	list_merge(l, cascades, NULL);
	if (ddl) {
		list_prepend(l, ddl);
	} else {
		s = stmt_aggr(sql->sa, tid, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1);
		list_append(l, stmt_affected_rows(sql->sa, s));
	}

	if (sql->cascade_action) {
		list_destroy(sql->cascade_action);
		sql->cascade_action = NULL;
	}
	return stmt_list(sql->sa, l);
}
 
static void
sql_stack_add_deleted(mvc *sql, char *name, sql_table *t)
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
		int *trigger_id = NEW(int);
		*trigger_id = trigger->base.id;

		stack_push_frame(sql, "OLD-NEW");
		if (trigger->event == 1) {
			stmt *s = NULL;
	
			/* add name for the 'deleted' to the stack */
			char *o = trigger->old_name;
		
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

static stmt * sql_delete(mvc *sql, sql_table *t, stmt *delete);

static stmt *
sql_delete_cascade_Fkeys(mvc *sql, stmt *s, sql_key *fk)
{
	sql_table *t = mvc_bind_table(sql, fk->t->s, fk->t->base.name);
	return sql_delete(sql, t, s);
}

static void 
sql_delete_ukey(mvc *sql, stmt *deletes, sql_key *k, list *l) 
{
	sql_ukey *uk = (sql_ukey*)k;

	if (uk->keys && list_length(uk->keys) > 0) {
		sql_subtype *wrd = sql_bind_localtype("wrd");
		sql_subtype *bt = sql_bind_localtype("bit");
		node *n;
		for(n = uk->keys->h; n; n = n->next) {
			char *msg = NULL;
			sql_subaggr *cnt = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
			sql_subfunc *ne = sql_bind_func_result(sql->sa, sql->session->schema, "<>", wrd, wrd, bt);
			sql_key *fk = n->data;
			stmt *s;

			s = stmt_idxbat(sql->sa, fk->idx, RDONLY);
			s = stmt_semijoin(sql->sa, stmt_reverse(sql->sa, s), deletes);
			switch (((sql_fkey*)fk)->on_delete) {
				case ACT_NO_ACTION: 
					break;
				case ACT_SET_NULL: 
				case ACT_SET_DEFAULT: 
					s = stmt_reverse(sql->sa, s);
					s = sql_delete_set_Fkeys(sql, fk, s, ((sql_fkey*)fk)->on_delete);
            			        list_prepend(l, s);
					break;
				case ACT_CASCADE: 
					s = sql_delete_cascade_Fkeys(sql, s, fk);
            			        list_prepend(l, s);
					break;
				default:	/*RESTRICT*/
					/* The overlap between deleted primaries and foreign should be empty */
		                        s = stmt_binop(sql->sa, stmt_aggr(sql->sa, s, NULL, cnt, 1), stmt_atom_wrd(sql->sa, 0), ne);
					msg = sa_message(sql->sa, "DELETE: FOREIGN KEY constraint '%s.%s' violated", fk->t->base.name, fk->base.name);
		                        s = stmt_exception(sql->sa, s, msg, 00001);
            			        list_prepend(l, s);
			}
		}
	}
}

static int
sql_delete_keys(mvc *sql, sql_table *t, stmt *deletes, list *l)
{
	int res = 1;
	node *n;

	if (!t->keys.set)
		return res;

	for (n = t->keys.set->h; n; n = n->next) {
		sql_key *k = n->data;

		if (k->type == pkey || k->type == ukey) {
			if (!(sql->cascade_action && list_find_id(sql->cascade_action, k->base.id))) {
				int *local_id = GDKmalloc(sizeof(int));
				if (!sql->cascade_action) 
					sql->cascade_action = list_create((fdestroy) GDKfree);
				
				*local_id = k->base.id;
				list_append(sql->cascade_action, local_id); 
				sql_delete_ukey(sql, deletes, k, l);
			}
		}
	}
	return res;
}

static stmt * 
sql_delete(mvc *sql, sql_table *t, stmt *delete)
{
	stmt *v, *s = NULL;
	list *l = list_new(sql->sa);

	if (delete) { 
		sql_subtype to;

		sql_find_subtype(&to, "oid", 0, 0);
		v = stmt_const(sql->sa, stmt_reverse(sql->sa, delete), stmt_atom(sql->sa, atom_general(sql->sa, &to, NULL)));
		list_append(l, stmt_delete(sql->sa, t, stmt_reverse(sql->sa, v)));
	} else { /* delete all */
		/* first column */
		v = stmt_mirror(sql->sa, stmt_bat(sql->sa, t->columns.set->h->data, stmt_basetable(sql->sa, t, t->base.name), RDONLY));
		s = stmt_table_clear(sql->sa, t);
		list_append(l, s);
	}

	if (!sql_delete_triggers(sql, t, l)) 
		return sql_error(sql, 02, "DELETE: triggers failed for table '%s'", t->base.name);
	if (!sql_delete_keys(sql, t, v, l)) 
		return sql_error(sql, 02, "DELETE: failed to delete indexes for table '%s'", t->base.name);
	if (delete) 
		s = stmt_aggr(sql->sa, delete, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1);
	list_append(l, stmt_affected_rows(sql->sa, s));
	return stmt_list(sql->sa, l);
}

static stmt *
rel2bin_delete( mvc *sql, sql_rel *rel, list *refs)
{
	stmt *delete = NULL;
	sql_rel *tr = rel->l;
	sql_table *t = NULL;

	if (tr->op == op_basetable)
		t = tr->l;
	else
		assert(0/*ddl statement*/);

	if (rel->r) { /* first construct the deletes relation */
		delete = subrel_bin(sql, rel->r, refs);
		if (!delete) 
			return NULL;	
	}
	if (delete && delete->type == st_list) {
		stmt *s = delete;
		delete = s->op4.lval->h->data;
	}
	delete = sql_delete(sql, t, delete); 
	if (sql->cascade_action) {
		list_destroy(sql->cascade_action);
		sql->cascade_action = NULL;
	}
	return delete;
}

#define E_ATOM_INT(e) ((atom*)((sql_exp*)e)->l)->data.val.lval
#define E_ATOM_STRING(e) ((atom*)((sql_exp*)e)->l)->data.val.sval

static stmt *
rel2bin_output(mvc *sql, sql_rel *rel, list *refs) 
{
	node *n = rel->exps->h;
	char *tsep = sa_strdup(sql->sa, E_ATOM_STRING(n->data));
	char *rsep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->data));
	char *ssep = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->data));
	char *ns   = sa_strdup(sql->sa, E_ATOM_STRING(n->next->next->next->data));
	char *fn   = NULL;
	stmt *s = NULL, *fns = NULL;
	list *slist = list_new(sql->sa);

	if (rel->l)  /* first construct the sub relation */
		s = subrel_bin(sql, rel->l, refs);
	if (!s) 
		return NULL;	

	if (n->next->next->next->next) {
		fn = E_ATOM_STRING(n->next->next->next->next->data);
		fns = stmt_atom_string(sql->sa, sa_strdup(sql->sa, fn));
	}
	list_append(slist, stmt_export(sql->sa, s, tsep, rsep, ssep, ns, fns));
	if (s->type == st_list && ((stmt*)s->op4.lval->h->data)->nrcols != 0) {
		stmt *cnt = stmt_aggr(sql->sa, s->op4.lval->h->data, NULL, sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL), 1);
		list_append(slist, stmt_affected_rows(sql->sa, cnt));
	} else {
		list_append(slist, stmt_affected_rows(sql->sa, stmt_atom_wrd(sql->sa, 1)));
	}
	return stmt_list(sql->sa, slist);
}

static stmt *
rel2bin_list(mvc *sql, sql_rel *rel, list *refs) 
{
	stmt *l = NULL, *r = NULL;
	list *slist = list_new(sql->sa);

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
rel2bin_seq(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en = rel->exps->h;
	stmt *restart, *sname, *seq, *sl = NULL;
	list *l = list_new(sql->sa);

	if (rel->l)  /* first construct the sub relation */
		sl = subrel_bin(sql, rel->l, refs);

	restart = exp_bin(sql, en->data, sl, NULL, NULL, NULL);
	sname = exp_bin(sql, en->next->data, sl, NULL, NULL, NULL);
	seq = exp_bin(sql, en->next->next->data, sl, NULL, NULL, NULL);

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
	stmt *chain = exp_bin(sql, en->data, NULL, NULL, NULL, NULL);
	stmt *name = NULL;

	(void)refs;
	if (en->next)
		name = exp_bin(sql, en->next->data, NULL, NULL, NULL, NULL);
	return stmt_trans(sql->sa, rel->flag, chain, name);
}

static stmt *
rel2bin_catalog(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en = rel->exps->h;
	stmt *action = exp_bin(sql, en->data, NULL, NULL, NULL, NULL);
	stmt *sname = NULL, *name = NULL;
	list *l = list_new(sql->sa);

	(void)refs;
	en = en->next;
	sname = exp_bin(sql, en->data, NULL, NULL, NULL, NULL);
	if (en->next) {
		name = exp_bin(sql, en->next->data, NULL, NULL, NULL, NULL);
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
	stmt *action = exp_bin(sql, en->data, NULL, NULL, NULL, NULL);
	stmt *table = NULL, *sname;
	list *l = list_new(sql->sa);

	(void)refs;
	en = en->next;
	sname = exp_bin(sql, en->data, NULL, NULL, NULL, NULL);
	en = en->next;
	if (en) 
		table = exp_bin(sql, en->data, NULL, NULL, NULL, NULL);
	append(l, sname);
	append(l, table);
	append(l, action);
	return stmt_catalog(sql->sa, rel->flag, stmt_list(sql->sa, l));
}

static stmt *
rel2bin_catalog2(mvc *sql, sql_rel *rel, list *refs) 
{
	node *en;
	list *l = list_new(sql->sa);

	(void)refs;
	for (en = rel->exps->h; en; en = en->next) {
		stmt *es = NULL;

		if (en->data) {
			es = exp_bin(sql, en->data, NULL, NULL, NULL, NULL);
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
	} else if (rel->flag <= DDL_DROP_ROLE) {
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
		s = rel2bin_basetable(sql, rel, refs);
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
	list *refs = list_create(NULL);
	int sqltype = sql->type;
	stmt *s = subrel_bin( sql, rel, refs);

	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	list_destroy(refs);

	if (s && s->type == st_list) {
		stmt *cnt = s->op4.lval->t->data;
		if (cnt && cnt->type == st_affected_rows)
			list_remove_data(s->op4.lval, cnt);
	}
	return s;
}

stmt *
output_rel_bin(mvc *sql, sql_rel *rel ) 
{
	list *refs = list_create(NULL);
	int sqltype = sql->type;
	stmt *s = subrel_bin( sql, rel, refs);

	if (sqltype == Q_SCHEMA)
		sql->type = sqltype;  /* reset */

	if (!is_ddl(rel->op) && s && s->type != st_none && sql->type == Q_TABLE)
		s = stmt_output(sql->sa, s);
	list_destroy(refs);
	return s;
}
