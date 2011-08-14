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


/*#define DEBUG*/

#include "monetdb_config.h"
#include "rel_optimizer.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "sql_env.h"

typedef struct global_props {
	int cnt[MAXOPS];
} global_props;

typedef sql_rel *(*rewrite_fptr)(int *changes, mvc *sql, sql_rel *rel);
typedef int (*find_prop_fptr)(mvc *sql, sql_rel *rel);

static sql_subfunc *find_func( mvc *sql, char *name, list *exps );

/* The important task of the relational optimizer is to optimize the
   join order. 

   The current implementation chooses the join order based on 
   select counts, ie if one of the join sides has been reduced using
   a select this join is choosen over one without such selections. 
 */

/* currently we only find simple column expressions */
static sql_column *
name_find_column( sql_rel *rel, char *rname, char *name, int pnr, sql_rel **bt ) 
{
	sql_exp *alias = NULL;
	sql_column *c = NULL;

	switch (rel->op) {
	case op_basetable: {
		node *cn;
		sql_table *t = rel->l;

		if (rel->exps) {
			sql_exp *e;
		       
			if (rname)
				e = exps_bind_column2(rel->exps, rname, name);
			else
				e = exps_bind_column(rel->exps, name, NULL);
			if (!e || e->type != e_column) 
				return NULL;
			if (e->l)
				rname = e->l;
			name = e->r;
		}
		if (rname && strcmp(t->base.name, rname) != 0)
			return NULL;
		for (cn = t->columns.set->h; cn; cn = cn->next) {
			sql_column *c = cn->data;
			if (strcmp(c->base.name, name) == 0) {
				*bt = rel;
				if (pnr < 0 || (c->t->p &&
				    list_position(c->t->p->tables.set, c->t) == pnr))
					return c;
			}
		}
		break;
	}
	case op_table:
		/* table func */
		return NULL;
	case op_ddl: 
		if (is_updateble(rel))
			return name_find_column( rel->l, rname, name, pnr, bt);
		return NULL;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
		/* first right (possible subquery) */
		c = name_find_column( rel->r, rname, name, pnr, bt);
		if (!c) 
			c = name_find_column( rel->l, rname, name, pnr, bt);
		return c;
	case op_select: 
	case op_topn: 
		return name_find_column( rel->l, rname, name, pnr, bt);
	case op_union: 
	case op_inter: 
	case op_except: 

		if (pnr >= 0 || pnr == -2) {
			/* first right (possible subquery) */
			c = name_find_column( rel->r, rname, name, pnr, bt);
			if (!c) 
				c = name_find_column( rel->l, rname, name, pnr, bt);
			return c;
		}
		return NULL;

	case op_project:
	case op_groupby:
		if (!rel->exps)
			break;
		if (rname)
			alias = exps_bind_column2(rel->exps, rname, name);
		else
			alias = exps_bind_column(rel->exps, name, NULL);
		if (is_groupby(rel->op) && alias && alias->type == e_column && rel->r) {
			if (alias->l)
				alias = exps_bind_column2(rel->r, alias->l, alias->r);
			else
				alias = exps_bind_column(rel->r, alias->r, NULL);
		}
		if (!alias && rel->l) {
			/* Group by column not found as alias in projection 
			 * list, fall back to check plain input columns */
			return name_find_column( rel->l, rname, name, pnr, bt);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		break;
	}
	if (alias) { /* we found an expression with the correct name, but
			we need sql_columns */
		if (rel->l && alias->type == e_column) /* real alias */
			return name_find_column(rel->l, alias->l, alias->r, pnr, bt);
	}
	return NULL;
}

static sql_column *
exp_find_column( sql_rel *rel, sql_exp *exp, int pnr )
{
	if (exp->type == e_column) { 
		sql_rel *bt = NULL;
		return name_find_column(rel, exp->l, exp->r, pnr, &bt);
	}
	return NULL;
}

static sql_column *
exp_find_column_( sql_rel *rel, sql_exp *exp, int pnr, sql_rel **bt )
{
	if (exp->type == e_column) 
		return name_find_column(rel, exp->l, exp->r, pnr, bt);
	return NULL;
}


static sql_exp *
list_find_exp( list *exps, sql_exp *e)
{
	sql_exp *ne = NULL;

	assert(e->type == e_column);
	if ((e->l && (ne=exps_bind_column2(exps, e->l, e->r)) != NULL) ||
	    ((ne=exps_bind_column(exps, e->r, NULL)) != NULL))
		return ne;
	return NULL;
}

/* find in the list of expression an expression which uses e */ 
static sql_exp *
exp_uses_exp( list *exps, sql_exp *e)
{
	node *n;
	char *rname = exp_find_rel_name(e);
	char *name = exp_name(e);

	if (!exps)
		return NULL;

	for ( n = exps->h; n; n = n->next) {
		sql_exp *u = n->data;

		if (u->l && rname && strcmp(u->l, rname) == 0 &&
		    u->r && name && strcmp(u->r, name) == 0) 
			return u;
		if (!u->l && !rname &&
		    u->r && name && strcmp(u->r, name) == 0) 
			return u;
	}
	return NULL;
}

static int
kc_column_cmp(sql_kc *kc, sql_column *c)
{
	/* return on equality */
	return !(c == kc->c);
}

static int
join_properties(sql_rel *rel) 
{
	if (rel->exps) {
		list *join_cols = list_create(NULL);
		node *en;

		/* simply using the expressions should also work ! */
		for ( en = rel->exps->h; en; en = en->next ) {
			sql_exp *e = en->data;

			if (e->type == e_cmp && e->flag == cmp_equal) {
				sql_column *lc = exp_find_column(rel, e->l, -2);
				sql_column *rc = exp_find_column(rel, e->r, -2);

				if (lc && rc) {
					append(join_cols, lc);
					append(join_cols, rc);
				}
			}
		}
		list_destroy(join_cols);
	}
	return 0;
}

static void
rel_properties(mvc *sql, global_props *gp, sql_rel *rel) 
{
	gp->cnt[(int)rel->op]++;
	switch (rel->op) {
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
		rel_properties(sql, gp, rel->l);
		rel_properties(sql, gp, rel->r);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_ddl:
		if (rel->l) 
			rel_properties(sql, gp, rel->l);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		if (rel->r) 
			rel_properties(sql, gp, rel->r);
		break;
	}

	switch (rel->op) {
	case op_basetable:
	case op_table:
		rel->p = prop_create(sql->sa, PROP_COUNT, rel->p);
		break;
	case op_join: 
		join_properties(rel);
		break;
	case op_left: 
	case op_right: 
	case op_full: 

	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		break;

	case op_project:
	case op_groupby: 
	case op_topn: 
	case op_select: 
		break;

	case op_insert:
	case op_update:
	case op_delete:
	case op_ddl:
		break;
	}
}

static sql_exp * exp_copy( sql_allocator *sa, sql_exp *e);

static list *
exps_copy( sql_allocator *sa, list *exps)
{
	node *n;
	list *nl = new_exp_list(sa);

	for(n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data;

		arg = exp_copy(sa, arg);
		if (!arg) 
			return NULL;
		append(nl, arg);
	}
	return nl;
}

static sql_exp *
exp_copy( sql_allocator *sa, sql_exp * e)
{
	sql_exp *l, *r, *r2, *ne = NULL;

	switch(e->type){
	case e_column:
		ne = exp_column(sa, e->l, e->r, exp_subtype(e), e->card, has_nil(e), is_intern(e));
		ne->flag = e->flag;
		break;
	case e_cmp:
		if (e->flag == cmp_or) {
			list *l = exps_copy(sa, e->l);
			list *r = exps_copy(sa, e->r);
			if (l && r)
				ne = exp_or(sa, l,r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_exp *l = exp_copy(sa, e->l);
			list *r = exps_copy(sa, e->r);
			if (l && r)
				ne = exp_in(sa, l, r, e->flag);
		} else {
			l = exp_copy(sa, e->l);
			r = exp_copy(sa, e->r);
			if (e->f) {
				r2 = exp_copy(sa, e->f);
				if (l && r && r2)
					ne = exp_compare2(sa, l, r, r2, e->flag);
			} else if (l && r) {
				ne = exp_compare(sa, l, r, e->flag);
			}
		}
		break;
	case e_convert:
		l = exp_copy(sa, e->l);
		if (l)
			ne = exp_convert(sa, l, exp_fromtype(e), exp_totype(e));
		break;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_copy(sa, l);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			ne = exp_op(sa, nl, e->f);
		else 
			ne = exp_aggr(sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
		break;
	}	
	case e_atom:
		if (e->l)
			ne = exp_atom(sa, e->l);
		else if (!e->r)
			ne = exp_atom_ref(sa, e->flag, &e->tpe);
		else 
			ne = exp_param(sa, e->r, &e->tpe, e->flag);
		break;
	}
	if (ne && e->p)
		ne->p = prop_copy(sa, e->p);
	if (e->name)
		exp_setname(sa, ne, exp_find_rel_name(e), exp_name(e));
	return ne;
}

static void
get_relations(sql_rel *rel, list *rels)
{
	if (!rel_is_ref(rel) && rel->op == op_join && rel->exps == NULL) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		
		get_relations(l, rels);
		get_relations(r, rels);
		rel->l = NULL;
		rel->r = NULL;
		rel_destroy(rel);
	} else {
		append(rels, rel);
	}
}

static int
exp_count(int *cnt, int seqnr, sql_exp *e) 
{
	(void)seqnr;
	if (!e)
		return 0;
	if (find_prop(e->p, PROP_JOINIDX))
		*cnt += 100;
	if (find_prop(e->p, PROP_HASHIDX)) 
		*cnt += 100;
	switch(e->type) {
	case e_cmp:
		if (!is_complex_exp(e->flag)) {
			exp_count(cnt, seqnr, e->l); 
			exp_count(cnt, seqnr, e->r);
			if (e->f)
				exp_count(cnt, seqnr, e->f);
		}
		switch (e->flag) {
		case cmp_equal:
			*cnt += 90;
			return 90;
		case cmp_notequal:
			*cnt += 7;
			return 7;
		case cmp_gt:
		case cmp_gte:
		case cmp_lt:
		case cmp_lte:
			*cnt += 6;
			if (e->f){ /* range */
				*cnt += 6;
				return 12;
			}
			return 6;
		case cmp_like:
		case cmp_notlike:
		case cmp_ilike:
		case cmp_notilike:
			*cnt += 2;
			return 2;
		case cmp_in: 
		case cmp_notin: {
			list *l = e->r;
			int c = 9 - 10*list_length(l);
			*cnt += c;
			return c;
		}
		case cmp_or: /* prefer union over like */
			*cnt += 3;
			return 3;
		default:
			return 0;
		}
	case e_column:
		*cnt += 1;
		return 1;
	case e_atom:
		*cnt += 10;
		return 10;
	case e_func:
		/* functions are more expensive, depending on the number of columns involved. */ 
		if (e->card == CARD_ATOM)
			return 0;
		*cnt -= 5*list_length(e->l);
		return 5*list_length(e->l);
	default:
		*cnt -= 5;
		return -5;
	}
}

static int
exp_keyvalue(sql_exp *e) 
{
	int cnt = 0;
	exp_count(&cnt, 0, e);
	return cnt;
}

static int
rel_has_exp(sql_rel *rel, sql_exp *e) 
{
	if (rel_find_exp(rel, e) != NULL) 
		return 0;
	return -1;
}

static sql_rel *
find_rel(list *rels, sql_exp *e)
{
	node *n = list_find(rels, e, (fcmp)&rel_has_exp);
	if (n) 
		return n->data;
	return NULL;
}

static sql_rel *
find_one_rel(list *rels, sql_exp *e)
{
	node *n;
	sql_rel *fnd = NULL;

	for(n = rels->h; n; n = n->next) {
		if (rel_has_exp(n->data, e) == 0) {
			if (fnd)
				return NULL;
			fnd = n->data;
		}
	}
	return fnd;
}

static int
joinexp_cmp(list *rels, sql_exp *h, sql_exp *key)
{
	sql_rel *h_l = find_rel(rels, h->l);
	sql_rel *h_r = find_rel(rels, h->r);
	sql_rel *key_l = find_rel(rels, key->l);
	sql_rel *key_r  = find_rel(rels, key->r);

	assert (!h || !key || (h->type == e_cmp && key->type == e_cmp));
	if (h_l == key_l && h_r == key_r)
		return 0;
	if (h_r == key_l && h_l == key_r)
		return 0;
        return -1;
}

static sql_exp *
joinexp_col(sql_exp *e, sql_rel *r)
{
	if (e->type == e_cmp) {
		if (rel_has_exp(r, e->l) >= 0) 
			return e->l;
		return e->r;
	}
	assert(0);
	return NULL;
}

static sql_column *
table_colexp(sql_exp *e, sql_rel *r)
{
	sql_table *t = r->l;

	if (e->type == e_column) {
		char *name = e->name;
		node *cn;

		if (r->exps) { /* use alias */
			for (cn = r->exps->h; cn; cn = cn->next) {
				sql_exp *ce = cn->data;
				if (strcmp(ce->name, name) == 0) {
					name = ce->r;
					break;
				}
			}
		}
		for (cn = t->columns.set->h; cn; cn = cn->next) {
			sql_column *c = cn->data;
			if (strcmp(c->base.name, name) == 0) 
				return c;
		}
	}
	return NULL;
}

static int
exp_joins_rels(sql_exp *e, list *rels)
{
	sql_rel *l, *r;

	assert (e->type == e_cmp);
		
	l = find_rel(rels, e->l);
	r = find_rel(rels, e->r);
	if (l && r)
		return 0;
	return -1;
}

static list *
matching_joins(list *rels, list *exps, sql_exp *je) 
{
	sql_rel *l, *r;

	assert (je->type == e_cmp);
		
	l = find_rel(rels, je->l);
	r = find_rel(rels, je->r);
	if (l && r) {
		list *res;
		list *n_rels = list_create(NULL);	

		append(n_rels, l);
		append(n_rels, r);
		res = list_select(exps, n_rels, (fcmp) &exp_joins_rels, (fdup)NULL);
		list_destroy(n_rels);
		return res; 
	}
	return list_create(NULL);
}

static int
sql_column_kc_cmp(sql_column *c, sql_kc *kc)
{
	/* return on equality */
	return (c->colnr - kc->c->colnr);
}

static sql_idx *
find_fk_index(sql_table *l, list *lcols, sql_table *r, list *rcols)
{
	if (l->idxs.set) {
		node *in;
	   	for(in = l->idxs.set->h; in; in = in->next){
	    		sql_idx *li = in->data;
			if (li->type == join_idx) {
		        	sql_key *rk = &((sql_fkey*)li->key)->rkey->k;
				fcmp cmp = (fcmp)&sql_column_kc_cmp;

              			if (rk->t == r && 
				    list_match(lcols, li->columns, cmp) == 0 &&
				    list_match(rcols, rk->columns, cmp) == 0) {
					return li;
				}
			}
		}
	}
	return NULL;
}

static sql_rel *
find_basetable( sql_rel *r)
{
	if (!r)
		return NULL;
	switch(r->op) {
	case op_basetable:	
		return r;
	case op_project:
	case op_select:
		return find_basetable(r->l);
	default:
		return NULL;
	}
}

static list *
order_join_expressions(sql_allocator *sa, list *dje, list *rels)
{
	list *res = list_new(sa);
	node *n = NULL;
	int i, j, *keys, *pos, cnt = list_length(dje);

	keys = (int*)malloc(cnt*sizeof(int));
	pos = (int*)malloc(cnt*sizeof(int));
	for (n = dje->h, i = 0; n; n = n->next, i++) {
		sql_exp *e = n->data;

		keys[i] = exp_keyvalue(e);
		/* add some weigth for the selections */
		if (e->type == e_cmp) {
			sql_rel *l = find_rel(rels, e->l);
			sql_rel *r = find_rel(rels, e->r);

			if (l && is_select(l->op) && l->exps)
				keys[i] += list_length(l->exps)*10;
			if (r && is_select(r->op) && r->exps)
				keys[i] += list_length(r->exps)*10;
		}
		pos[i] = i;
	}
	/* sort descending */
	if (cnt > 1) 
		GDKqsort_rev(keys, pos, NULL, cnt, sizeof(int), sizeof(int), TYPE_int);
	for(j=0; j<cnt; j++) {
		for(n = dje->h, i = 0; i != pos[j]; n = n->next, i++) 
			;
		list_append(res, n->data);
	}
	free(keys);
	free(pos);
	return res;
}

static list *
find_fk(sql_allocator *sa, list *rels, list *exps) 
{
	node *djn;
	list *sdje, *aje, *dje;

	/* first find the distinct join expressions */
	aje = list_select(exps, (void*)1, (fcmp) &exp_is_join, (fdup)NULL);
	dje = list_distinct2(aje, rels, (fcmp2) &joinexp_cmp, (fdup)NULL);
	for(djn=dje->h; djn; djn = djn->next) {
		/* equal join expressions */
		sql_idx *idx = NULL;
		sql_exp *je = djn->data, *le = je->l, *re = je->r; 

		if (!find_prop(je->p, PROP_JOINIDX)) {
			int swapped = 0;
			list *aaje = matching_joins(rels, aje, je);
			list *eje = list_select(aaje, (void*)1, (fcmp) &exp_is_eqjoin, (fdup)NULL);
			sql_rel *lr = find_rel(rels, le);
			sql_rel *rr = find_rel(rels, re);

			sql_table *l, *r;
			list *lexps = list_map(eje, lr, (fmap) &joinexp_col);
			list *rexps = list_map(eje, rr, (fmap) &joinexp_col);
			list *lcols, *rcols;
			
			lr = find_basetable(lr);
			rr = find_basetable(rr);
			if (!lr || !rr) 
				continue;
			l = lr->l;
			r = rr->l;
			lcols = list_map(lexps, lr, (fmap) &table_colexp);
			rcols = list_map(rexps, rr, (fmap) &table_colexp);
			if (list_length(lcols) != list_length(rcols)) {
				lcols->destroy = NULL;
				rcols->destroy = NULL;
				continue;
			}

			idx = find_fk_index(l, lcols, r, rcols); 
			if (!idx) {
				idx = find_fk_index(r, rcols, l, lcols); 
				swapped = 1;
			} 

			if (idx) { 	
				prop *p;
				node *n;
	
				/* Remove all other join expressions */
				for (n = eje->h; n; n = n->next) {
					if (je != n->data)
						list_remove_data(exps, n->data);
				}
				/* Add the join index using PROP_JOINIDX  */
				if (swapped) {
					sql_exp *s = je->l;
					je->l = je->r;
					je->r = s;
				}
				je->p = p = prop_create(sa, PROP_JOINIDX, je->p);
				p->value = idx;
			}
			lcols->destroy = NULL;
			rcols->destroy = NULL;
		}
	}

	/* sort expressions on weighted number of reducing operators */
	sdje = order_join_expressions(sa, dje, rels);
	return sdje;
}

static sql_rel *
order_joins(mvc *sql, list *rels, list *exps)
{
	sql_rel *top = NULL, *l = NULL, *r = NULL;
	sql_exp *cje;
	node *djn;
	list *sdje, *n_rels = list_create(NULL);
	int fnd = 0;

	/* find foreign keys and reorder the expressions on reducing quality */
	sdje = find_fk(sql->sa, rels, exps);

	/* open problem, some expressions use more then 2 relations */
	/* For example a.x = b.y * c.z; */
	if (list_length(rels) >= 2 && sdje->h) {
		/* get the first expression */
		cje = sdje->h->data;

		/* find the involved relations */

		/* complex expressions may touch multiple base tables 
		 * Should be push up to extra selection.
		 * */
		l = find_one_rel(rels, cje->l);
		r = find_one_rel(rels, cje->r);

		if (l && r) {
			list_remove_data(sdje, cje);
			list_remove_data(exps, cje);
		}
	}
	if (l && r) {
		list_remove_data(rels, l);
		list_remove_data(rels, r);
		list_append(n_rels, l);
		list_append(n_rels, r);

		/* Create a relation between l and r. Since the calling 
	   	   functions rewrote the join tree, into a list of expressions 
	   	   and a list of (simple) relations, there are no outer joins 
	   	   involved, we can simply do a crossproduct here.
	 	 */
		top = rel_crossproduct(sql->sa, l, r, op_join);
		rel_join_add_exp(sql->sa, top, cje);

		/* all other join expressions on these 2 relations */
		while((djn = list_find(exps, n_rels, (fcmp)&exp_joins_rels)) != NULL) {
			sql_exp *e = djn->data;

			rel_join_add_exp(sql->sa, top, e);
			list_remove_data(exps, e);
		}
		/* Remove other joins on the current 'n_rels' set in the distinct list too */
		while((djn = list_find(sdje, n_rels, (fcmp)&exp_joins_rels)) != NULL) 
			list_remove_data(sdje, djn->data);
		fnd = 1;
	}
	/* build join tree using the ordered list */
	while(list_length(exps) && fnd) {
		fnd = 0;
		/* find the first expression which could be added */
		for(djn = sdje->h; djn && !fnd; djn = (!fnd)?djn->next:NULL) {
			node *ln, *rn, *en;
			
			cje = djn->data;
			ln = list_find(n_rels, cje->l, (fcmp)&rel_has_exp);
			rn = list_find(n_rels, cje->r, (fcmp)&rel_has_exp);

			if (ln || rn) {
				/* remove the expression from the lists */
				list_remove_data(sdje, cje);
				list_remove_data(exps, cje);
			}
			if (ln && rn) {
				assert(0);
				/* create a selection on the current */
				l = ln->data;
				r = rn->data;
				rel_join_add_exp(sql->sa, top, cje);
				fnd = 1;
			} else if (ln || rn) {
				if (ln) {
					l = ln->data;
					r = find_rel(rels, cje->r);
				} else {
					l = rn->data;
					r = find_rel(rels, cje->l);
				}
				list_remove_data(rels, r);
				append(n_rels, r);

				/* create a join using the current expression */
				top = rel_crossproduct(sql->sa, top, r, op_join);
				rel_join_add_exp(sql->sa, top, cje);

				/* all join expressions on these tables */
				while((en = list_find(exps, n_rels, (fcmp)&exp_joins_rels)) != NULL) {
					sql_exp *e = en->data;
					rel_join_add_exp(sql->sa, top, e);
					list_remove_data(exps, e);
				}
				/* Remove other joins on the current 'n_rels' 
				   set in the distinct list too */
				while((en = list_find(sdje, n_rels, (fcmp)&exp_joins_rels)) != NULL) 
					list_remove_data(sdje, en->data);
				fnd = 1;
			}
		}
	}
	list_destroy(n_rels);
	if (list_length(rels)) { /* more relations */
		node *n;
		for(n=rels->h; n; n = n->next) {
			if (top)
				top = rel_crossproduct(sql->sa, top, n->data, op_join);
			else 
				top = n->data;
		}
	}
	if (list_length(exps)) { /* more expressions (add selects) */
		node *n;
		top = rel_select(sql->sa, top, NULL);
		for(n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* find the involved relations */

			/* complex expressions may touch multiple base tables 
		 	 * Should be push up to extra selection. */
			/*
			l = find_one_rel(rels, e->l);
			r = find_one_rel(rels, e->r);

			if (l && r) 
			*/
			if (exp_is_join_exp(e) == 0)
				rel_join_add_exp(sql->sa, top->l, e);
			else
				rel_select_add_exp(top, e);
		}
	}
	return top;
}

static int
rel_neg_in_size(sql_rel *r)
{
	if (is_union(r->op) && r->nrcols == 0) 
		return -1 + rel_neg_in_size(r->l);
	if (is_project(r->op) && r->nrcols == 0) 
		return -1;
	return 0;
}

static list *
push_in_join_down(mvc *sql, list *rels, list *exps)
{
	node *n;
	int restart = 1;
	list *nrels;

	/* we should sort these first, ie small in's before large one's */
	nrels = list_sort(rels, (fkeyvalue)&rel_neg_in_size, (fdup)&rel_dup);

	/* we need to cleanup, the new refs ! */
	rels->destroy = (fdestroy)rel_destroy;
	list_destroy(rels);
	rels = nrels;

	/* one of the rels should be a op_union with nrcols == 0 */
	while(restart) {
	    for(n = rels->h; n; n = n->next) {
		sql_rel *r = n->data;
	
		restart = 0;
		if ((is_union(r->op) || is_project(r->op)) && r->nrcols == 0) {
			/* next step find expression on this relation */
			node *m;
			sql_rel *l = NULL;
			sql_exp *je = NULL;

			for(m = exps->h; !je && m; m = m->next) {
				sql_exp *e = m->data;

				if (e->type == e_cmp && e->flag == cmp_equal) {
					/* in values are on 
						the right of the join */
					if (rel_has_exp(r, e->r) >= 0) 
						je = e;
				}
			}
			/* with this expression find other relation */
			if (je && (l = find_rel(rels, je->l)) != NULL) {
				sql_rel *nr = rel_crossproduct(sql->sa, l, r, op_join);

				rel_join_add_exp(sql->sa, nr, je);
				list_append(rels, nr); 
				list_remove_data(rels, l);
				list_remove_data(rels, r);
				list_remove_data(exps, je);
				restart = 1;
				break;
			}

		}
	    }
	}
	return rels;
}

static sql_rel *
reorder_join(mvc *sql, sql_rel *rel)
{
	list *exps = rel->exps;
	list *rels;

	(void)sql;
	if (!exps) /* crosstable, ie order not important */
		return rel;
	rel->exps = NULL; /* should be all crosstables by now */
 	rels = list_create(NULL);
	if (is_outerjoin(rel->op)) {
		int cnt = 0;
		/* try to use an join index also for outer joins */
		list_append(rels, rel->l);
		list_append(rels, rel->r);
		cnt = list_length(exps);
		rel->exps = find_fk(sql->sa, rels, exps);
		if (list_length(rel->exps) != cnt) {
			rel->exps = list_dup(exps, (fdup)NULL);
		}
	} else { 
 		get_relations(rel, rels);
		if (list_length(rels) > 1) {
			rels = push_in_join_down(sql, rels, exps);
			rel = order_joins(sql, rels, exps);
		} else {
			rel->exps = exps;
			exps = NULL;
		}
	}
	list_destroy(rels);
	return rel;
}

static list *
push_up_join_exps( sql_rel *rel) 
{
	if (rel_is_ref(rel))
		return NULL;

	switch(rel->op) {
	case op_join: {
		sql_rel *rl = rel->l;
		sql_rel *rr = rel->r;
		list *l, *r;

		if (rel_is_ref(rl) && rel_is_ref(rr)) {
			l = rel->exps;
			rel->exps = NULL;
			return l;
		}
		l = push_up_join_exps(rl);
		r = push_up_join_exps(rr);
		if (l && r) {
			l = list_merge(l, r, (fdup)NULL);
			r = NULL;
		}
		if (rel->exps) {
			if (l && !r)
				r = l;
			l = list_merge(rel->exps, r, (fdup)NULL);
		}
		rel->exps = NULL;
		return l;
	}
	default:
		return NULL;
	}
}

static sql_rel *
rel_join_order(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)*changes;
	if (is_join(rel->op) && rel->exps && !rel_is_ref(rel)) {
		if (rel->op == op_join)
			rel->exps = push_up_join_exps(rel);
		rel = reorder_join(sql, rel);
	}
	return rel;
}

/* exp_rename */
static sql_exp * exp_rename(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

static list *
exps_rename(mvc *sql, list *l, sql_rel *f, sql_rel *t) 
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	for(n=l->h; n; n=n->next) {
		sql_exp *arg = n->data;

		arg = exp_rename(sql, arg, f, t);
		if (!arg) 
			return NULL;
		append(nl, arg);
	}
	return nl;
}

/* exp_rename */
static sql_exp *
exp_rename(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t) 
{
	sql_exp *ne = NULL, *l, *r, *r2;

	switch(e->type) {
	case e_column:
		if (e->l) { 
			ne = exps_bind_column2(f->exps, e->l, e->r);
			/* if relation name matches expressions relation name, find column based on column name alone */
		} else {
			ne = exps_bind_column(f->exps, e->r, NULL);
		}
		if (!ne)
			return e;
		e = NULL;
		if (ne->name && ne->r && ne->l) 
			e = rel_bind_column2(sql, t, ne->l, ne->r, 0);
		if (!e && ne->r)
			e = rel_bind_column(sql, t, ne->r, 0);
		sql->session->status = 0;
		sql->errstr[0] = 0;
		if (!e && exp_is_atom(ne))
			return ne;
		return e;
	case e_cmp: 
		if (e->flag == cmp_or) {
			list *l = exps_rename(sql, e->l, f, t);
			list *r = exps_rename(sql, e->r, f, t);
			if (l && r)
				ne = exp_or(sql->sa, l,r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_exp *l = exp_rename(sql, e->l, f, t);
			list *r = exps_rename(sql, e->r, f, t);
			if (l && r)
				ne = exp_in(sql->sa, l, r, e->flag);
		} else {
			l = exp_rename(sql, e->l, f, t);
			r = exp_rename(sql, e->r, f, t);
			if (e->f) {
				r2 = exp_rename(sql, e->f, f, t);
				if (l && r && r2)
					ne = exp_compare2(sql->sa, l, r, r2, e->flag);
			} else if (l && r) {
				ne = exp_compare(sql->sa, l, r, e->flag);
			}
		}
		break;
	case e_convert:
		l = exp_rename(sql, e->l, f, t);
		if (l)
			ne = exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		break;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_rename(sql, l, f, t);
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
		return e;
	}
	if (ne && e->p)
		ne->p = prop_copy(sql->sa, e->p);
	return ne;
}

/* push the expression down, ie translate colum references 
	from relation f into expression of relation t 
*/ 

static sql_exp * _exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

static list *
exps_push_down(mvc *sql, list *exps, sql_rel *f, sql_rel *t)
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	for(n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data, *narg = NULL;

		narg = _exp_push_down(sql, arg, f, t);
		if (!narg) 
			return NULL;
		if (arg->p)
			narg->p = prop_copy(sql->sa, arg->p);
		append(nl, narg);
	}
	return nl;
}

static sql_exp *
_exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t) 
{
	int flag = e->flag;
	sql_exp *ne = NULL, *l, *r, *r2;

	switch(e->type) {
	case e_column:
		if (is_union(f->op)) {
			int p = list_position(f->exps, rel_find_exp(f, e));

			return list_fetch(t->exps, p);
		}
		if (e->l) { 
			ne = rel_bind_column2(sql, f, e->l, e->r, 0);
			/* if relation name matches expressions relation name, find column based on column name alone */
		}
		if (!ne && !e->l)
			ne = rel_bind_column(sql, f, e->r, 0);
		if (!ne)
			return NULL;
		e = NULL;
		if (ne->name && ne->rname)
			e = rel_bind_column2(sql, t, ne->rname, ne->name, 0);
		if (!e && ne->name && !ne->rname)
			e = rel_bind_column(sql, t, ne->name, 0);
		if (!e && ne->name && ne->r && ne->l) 
			e = rel_bind_column2(sql, t, ne->l, ne->r, 0);
		if (!e && ne->r && !ne->l)
			e = rel_bind_column(sql, t, ne->r, 0);
		sql->session->status = 0;
		sql->errstr[0] = 0;
		if (e && flag)
			e->flag = flag;
		/* if the upper exp was an alias, keep this */ 
		if (e && ne->rname) 
			exp_setname(sql->sa, e, ne->rname, ne->name);
		return e;
	case e_cmp: 
		if (e->flag == cmp_or) {
			list *l = exps_push_down(sql, e->l, f, t);
			list *r = exps_push_down(sql, e->r, f, t);

			if (!l || !r) 
				return NULL;
			return exp_or(sql->sa, l, r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *r;

			l = _exp_push_down(sql, e->l, f, t);
			r = exps_push_down(sql, e->r, f, t);
			if (!l || !r)
				return NULL;
			return exp_in(sql->sa, l, r, e->flag);
		} else {
			l = _exp_push_down(sql, e->l, f, t);
			r = _exp_push_down(sql, e->r, f, t);
			if (e->f) {
				r2 = _exp_push_down(sql, e->f, f, t);
				if (l && r && r2)
					return exp_compare2(sql->sa, l, r, r2, e->flag);
			} else if (l && r) {
				return exp_compare(sql->sa, l, r, e->flag);
			}
		}
		return NULL;
	case e_convert:
		l = _exp_push_down(sql, e->l, f, t);
		if (l)
			return exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		return NULL;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_push_down(sql, l, f, t);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			return exp_op(sql->sa, nl, e->f);
		else 
			return exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
	}	
	case e_atom:
		return e;
	}
	return NULL;
}

static sql_exp *
exp_push_down(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t) 
{
	return _exp_push_down(sql, e, f, t);
}


/* some projections results are order dependend (row_number etc) */
static int 
project_unsafe(sql_rel *rel)
{
	sql_rel *sub = rel->l;
	node *n;

	if (need_distinct(rel) || rel->r /* order by */)
		return 1;
	if (!rel->exps)
		return 0;
	/* projects without sub and projects around ddl's cannot be changed */
	if (!sub || (sub && sub->op == op_ddl))
		return 1;
	for(n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		/* aggr func in project ! */
		if (e->type == e_func && e->card == CARD_AGGR)
			return 1;
	}
	return 0;
}


/*
 * Push Count inside crossjoin down, and multiply the results
 * 
 *     project (                                project(
 *          group by (                               crossproduct (
 *		crossproduct(                             project (
 *		     L,			 =>                    group by (
 *		     R                                              L
 *		) [ ] [ count NOT NULL ]                       ) [ ] [ count NOT NULL ]
 *          )                                             ),
 *     ) [ NOT NULL ]                                     project (
 *                                                              group by (
 *                                                                  R
 *                                                              ) [ ] [ count NOT NULL ]
 *                                                        )
 *                                                   ) [ sql_mul(.., .. NOT NULL) ]
 *                                              )
 */



static sql_rel *
rel_push_count_down(int *changes, mvc *sql, sql_rel *rel)
{
	sql_rel *r = rel->l;

	if (is_groupby(rel->op) && !rel_is_ref(rel) &&
            r && !r->exps && r->op == op_join && !(rel_is_ref(r)) &&
            ((sql_exp *) rel->exps->h->data)->type == e_aggr &&
            strcmp(((sql_subaggr *) ((sql_exp *) rel->exps->h->data)->f)->aggr->base.name, "count") == 0) {
/* TODO check for count(*) */
	    	sql_exp *nce, *oce;
		sql_rel *gbl, *gbr;		/* Group By */
		sql_rel *cp;			/* Cross Product */
		sql_subfunc *mult;
		list *args;
		char *rname = NULL, *name = NULL;
		sql_rel *srel;

		oce = rel->exps->h->data;
		if (oce->l) /* we only handle COUNT(*) */ 
			return rel;
		rname = oce->rname;
		name  = oce->name;

 		args = new_exp_list(sql->sa);
		srel = r->l;
		{
			sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
			sql_exp *cnt, *e = exp_aggr(sql->sa, NULL, cf, need_distinct(oce), need_no_nil(oce), oce->card, 0);

			exp_label(sql->sa, e, ++sql->label);
			cnt = exp_column(sql->sa, NULL, exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			gbl = rel_groupby(sql->sa, rel_dup(srel), NULL);
			rel_groupby_add_aggr(sql, gbl, e);
			append(args, cnt);
		}

		srel = r->r;
		{
			sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
			sql_exp *cnt, *e = exp_aggr(sql->sa, NULL, cf, need_distinct(oce), need_no_nil(oce), oce->card, 0);

			exp_label(sql->sa, e, ++sql->label);
			cnt = exp_column(sql->sa, NULL, exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			gbr = rel_groupby(sql->sa, rel_dup(srel), NULL);
			rel_groupby_add_aggr(sql, gbr, e);
			append(args, cnt);
		}

		mult = find_func(sql, "sql_mul", args);
		cp = rel_crossproduct(sql->sa, gbl, gbr, op_join);

		nce = exp_op(sql->sa, args, mult);
		exp_setname(sql->sa, nce, rname, name );

		rel_destroy(rel);
		rel = rel_project(sql->sa, cp, append(new_exp_list(sql->sa), nce));

		(*changes)++;
	}
	
	return rel;
}

/*
 * Push TopN (only LIMIT, no ORDER BY) down through projections underneath crossproduct, i.e.,
 *
 *     topn(                          topn(
 *         project(                       project(
 *             crossproduct(                  crossproduct(
 *                 L,           =>                topn( L )[ n ],
 *                 R                              topn( R )[ n ]
 *             )                              )
 *         )[ Cs ]*                       )[ Cs ]*
 *     )[ n ]                         )[ n ]
 *
 *  (TODO: in case of n==1 we can omit the original top-level TopN)
 *
 * also push topn under (non reordering) projections.
 */

static list *
sum_limit_offset(sql_allocator *sa, list *exps )
{
	list *nexps = new_exp_list(sa);
	wrd l = 0;
	node *n;

	/* if the expression list only consists of a limit expression, 
	 * we copy it */
	if (list_length(exps) == 1 && exps->h->data)
		return append(nexps, exps->h->data);
	for (n = exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (e) {
			atom *a = e->l;

			assert(e->type == e_atom);
			l += a->data.val.wval;
		}
	}
	return append(nexps, exp_atom_wrd(sa, l));
}

static int 
topn_save_exps( list *exps )
{
	node *n;

	/* Limit only expression lists are always save */
	if (list_length(exps) == 1)
		return 1;
	for (n = exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (!e || e->type != e_atom) 
			return 0;
	}
	return 1;
}

static void
rel_no_rename_exps( list *exps )
{
	node *n;

	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		e->rname = e->l;
		e->name = e->r;
	}
}

static void
rel_rename_exps( mvc *sql, list *exps1, list *exps2)
{
	node *n, *m;

	assert(list_length(exps1) == list_length(exps2)); 
	for (n = exps1->h, m = exps2->h; n && m; n = n->next, m = m->next) {
		sql_exp *e1 = n->data;
		sql_exp *e2 = m->data;
		char *rname = e1->rname;

		if (!rname && e1->type == e_column && e1->l && e2->rname && 
		    strcmp(e1->l, e2->rname) == 0)
			rname = e2->rname;
		exp_setname(sql->sa, e2, rname, e1->name );
	}
}

static sql_rel *
rel_push_topn_down(int *changes, mvc *sql, sql_rel *rel) 
{
	sql_rel *rl, *r = rel->l;

	if (rel->op == op_topn && topn_save_exps(rel->exps)) {
		sql_rel *rp = NULL;

		/* duplicate topn + [ project-order ] under union */
		if (r)
			rp = r->l;
		if (r && r->exps && r->op == op_project && !(rel_is_ref(r)) && r->r && r->l &&
		    rp->op == op_union) {
			sql_rel *u = rp, *ou = u, *x;
			sql_rel *ul = u->l;
			sql_rel *ur = u->r;

			/* only push topn once */
			x = ul;
			while(x->op == op_project && x->l)
				x = x->l;
			if (x && x->op == op_topn)
				return rel;
			x = ur;
			while(x->op == op_project && x->l)
				x = x->l;
			if (x && x->op == op_topn)
				return rel;

			ul = rel_dup(ul);
			ur = rel_dup(ur);
			if (!is_project(ul->op)) 
				ul = rel_project(sql->sa, ul, 
					rel_projections(sql, ul, NULL, 1, 1));
			if (!is_project(ur->op)) 
				ur = rel_project(sql->sa, ur, 
					rel_projections(sql, ur, NULL, 1, 1));
			rel_rename_exps(sql, u->exps, ul->exps);
			rel_rename_exps(sql, u->exps, ur->exps);

			/* introduce projects under the set */
			ul = rel_project(sql->sa, ul, NULL);
			ul->exps = exps_copy(sql->sa, r->exps);
			ul->r = exps_copy(sql->sa, r->r);
			ul = rel_topn(sql->sa, ul, sum_limit_offset(sql->sa, rel->exps));
			ur = rel_project(sql->sa, ur, NULL);
			ur->exps = exps_copy(sql->sa, r->exps);
			ur->r = exps_copy(sql->sa, r->r);
			ur = rel_topn(sql->sa, ur, sum_limit_offset(sql->sa, rel->exps));
			u = rel_setop(sql->sa, ul, ur, op_union);
			u->exps = exps_copy(sql->sa, r->exps); 
			/* zap names */
			rel_no_rename_exps(u->exps);
			rel_destroy(ou);

			r->l = u;
			(*changes)++;
			return rel;
		}

		/* pass through projections */
		while (r && is_project(r->op) && !need_distinct(r) &&
			!(rel_is_ref(r)) &&
			!r->r && (rl = r->l) != NULL && is_project(rl->op)) {
			/* ensure there is no order by */
			if (!r->r) {
				r = r->l;
			} else {
				r = NULL;
			}
		}
		if (r && r != rel && r->op == op_project && !(rel_is_ref(r)) && !r->r && r->l) {
			r = rel_topn(sql->sa, r, sum_limit_offset(sql->sa, rel->exps));
		}

		/* push topn under crossproduct */
		if (r && !r->exps && r->op == op_join && !(rel_is_ref(r)) &&
		    ((sql_rel *)r->l)->op != op_topn && ((sql_rel *)r->r)->op != op_topn) {
			r->l = rel_topn(sql->sa, r->l, sum_limit_offset(sql->sa, rel->exps));
			r->r = rel_topn(sql->sa, r->r, sum_limit_offset(sql->sa, rel->exps));
			(*changes)++;
			return rel;
		}
/* TODO */
#if 0
		/* duplicate topn + [ project-order ] under join on independend always matching joins */
		if (r)
			rp = r->l;
		if (r && r->exps && r->op == op_project && !(rel_is_ref(r)) && r->r && r->l &&
		    rp->op == op_join && rp->exps && rp->exps->h && ((prop*)((sql_exp*)rp->exps->h->data)->p)->kind == PROP_FETCH &&
		    ((sql_rel *)rp->l)->op != op_topn && ((sql_rel *)rp->r)->op != op_topn) {
			/* TODO check if order by columns are independend of join conditions */
			r->l = rel_topn(sql->sa, r->l, sum_limit_offset(sql->sa, rel->exps));
			r->r = rel_topn(sql->sa, r->r, sum_limit_offset(sql->sa, rel->exps));
			(*changes)++;
			return rel;
		}
#endif
	}
	return rel;
}

/* merge projection */

/* push an expression through a projection. 
 * The result should again used in a projection.
 */
static sql_exp * exp_push_down_prj(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

static list *
exps_push_down_prj(mvc *sql, list *exps, sql_rel *f, sql_rel *t)
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	for(n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data, *narg = NULL;

		narg = exp_push_down_prj(sql, arg, f, t);
		if (!narg) 
			return NULL;
		if (arg->p)
			narg->p = prop_copy(sql->sa, arg->p);
		append(nl, narg);
	}
	return nl;
}

static sql_exp *
exp_push_down_prj(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t) 
{
	sql_exp *ne = NULL, *l, *r, *r2;

	assert(is_project(f->op));

	switch(e->type) {
	case e_column:
		if (e->l) 
			ne = exps_bind_column2(f->exps, e->l, e->r);
		if (!ne && !e->l)
			ne = exps_bind_column(f->exps, e->r, NULL);
		if (!ne || (ne->type != e_column && ne->type != e_atom))
			return NULL;
		/* possibly a groupby column is renamed */
		if (is_groupby(f->op) && f->r) {
			sql_exp *gbe = NULL;
			if (ne->l) 
				gbe = exps_bind_column2(f->r, ne->l, ne->r);
			if (!ne && !e->l)
				gbe = exps_bind_column(f->r, ne->r, NULL);
			ne = gbe;
			if (!ne || (ne->type != e_column && ne->type != e_atom))
				return NULL;
		}
		if (ne->type == e_atom) 
			e = exp_copy(sql->sa, ne);
		else
			e = exp_alias(sql->sa, e->rname, exp_name(e), ne->l, ne->r, exp_subtype(e), e->card, has_nil(e), is_intern(e));
		return e;
	case e_cmp: 
		if (e->flag == cmp_or) {
			list *l = exps_push_down_prj(sql, e->l, f, t);
			list *r = exps_push_down_prj(sql, e->r, f, t);

			if (!l || !r) 
				return NULL;
			return exp_or(sql->sa, l, r);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_exp *l = exp_push_down_prj(sql, e->l, f, t);
			list *r = exps_push_down_prj(sql, e->r, f, t);

			if (!l || !r) 
				return NULL;
			return exp_in(sql->sa, l, r, e->flag);
		} else {
			l = exp_push_down_prj(sql, e->l, f, t);
			r = exp_push_down_prj(sql, e->r, f, t);
			if (e->f) {
				r2 = exp_push_down_prj(sql, e->f, f, t);
				if (l && r && r2)
					return exp_compare2(sql->sa, l, r, r2, e->flag);
			} else if (l && r) {
				return exp_compare(sql->sa, l, r, e->flag);
			}
		}
		return NULL;
	case e_convert:
		l = exp_push_down_prj(sql, e->l, f, t);
		if (l)
			return exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		return NULL;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_push_down_prj(sql, l, f, t);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			return exp_op(sql->sa, nl, e->f);
		else 
			return exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
	}	
	case e_atom:
		return e;
	}
	return NULL;
}

static sql_rel *
rel_merge_projects(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = rel->exps;
	sql_rel *prj = rel->l;
	node *n;

	if (rel->op == op_project && 
	    prj && prj->op == op_project && !(rel_is_ref(prj)) && !prj->r) {
		int all = 1;

		if (project_unsafe(rel) || project_unsafe(prj))
			return rel;

		/* here we need to fix aliases */
		rel->exps = new_exp_list(sql->sa); 

		/* for each exp check if we can rename it */
		for (n = exps->h; n && all; n = n->next) { 
			sql_exp *e = n->data, *ne = NULL;

			ne = exp_push_down_prj(sql, e, prj, prj->l);
			if (ne) {
				exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
				list_append(rel->exps, ne);
			} else {
				all = 0;
			}
		}
		if (all) {
			/* we can now remove the intermediate project */
			/* push order by expressions */
			if (rel->r) {
				list *nr = new_exp_list(sql->sa), *res = rel->r; 
				for (n = res->h; n; n = n->next) { 
					sql_exp *e = n->data, *ne = NULL;
	
					ne = exp_push_down_prj(sql, e, prj, prj->l);
					if (ne) {
						exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
						list_append(nr, ne);
					} else {
						all = 0;
					}
				}
				if (all) {
					list_destroy(res);
					rel->r = nr;
				} else {
					/* leave as is */
					rel->exps = exps;
					return rel;
				}
			}
			rel->l = prj->l;
			prj->l = NULL;
			rel_destroy(prj);
			(*changes)++;
			return rel_merge_projects(changes, sql, rel);
		} else {
			/* leave as is */
			rel->exps = exps;
		}
		return rel;
	}
	return rel;
}

static sql_subfunc *
find_func( mvc *sql, char *name, list *exps )
{
	list * l = list_create(NULL); 
	node *n;

	for(n = exps->h; n; n = n->next)
		append(l, exp_subtype(n->data)); 
	return sql_bind_func_(sql->sa, sql->session->schema, name, l);
}

static sql_exp *
sql_div_fixup( mvc *sql, sql_exp *e, sql_exp *cond, int lr )
{
	list *args = e->l;
	sql_exp *le = args->h->data, *o;
	sql_exp *re = args->h->next->data;
	sql_subfunc *ifthen;

	/* if (cond) then val else const */
	args = new_exp_list(sql->sa);
	append(args, cond);
	if (!lr)
		append(args, re);
	o = exp_atom_wrd(sql->sa, 1);
	append(args, exp_convert(sql->sa, o, exp_subtype(o), exp_subtype(re)));
	if (lr)
		append(args, re);
	ifthen = find_func(sql, "ifthenelse", args);
	assert(ifthen);
	re = exp_op(sql->sa, args, ifthen);

	return exp_binop(sql->sa, le, re, e->f);
}

static list *
exps_case_fixup( mvc *sql, list *exps, sql_exp *cond, int lr )
{
	node *n;

	if (exps) {
		list *nexps = new_exp_list(sql->sa);
		for( n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (e->type == e_func && e->l && !is_rank_op(e) ) {
				sql_subfunc *f = e->f;

				if (!f->func->s && !strcmp(f->func->base.name, "sql_div")) 
					e = sql_div_fixup(sql, e, cond, lr);
				else 
					e->l = exps_case_fixup(sql, e->l, cond, lr);

			}
			append(nexps, e);
		}
		return nexps;
	}
	return exps;
}

static sql_exp *
exp_case_fixup( mvc *sql, sql_exp *e )
{
	/* only functions need fix up */
	if (e->type == e_func && e->l && !is_rank_op(e) ) {
		list *l = new_exp_list(sql->sa), *args = e->l;
		node *n;
		sql_exp *ne;
		sql_subfunc *f = e->f;

		/* first fixup arguments */
		for (n=args->h; n; n=n->next) {
			sql_exp *a = exp_case_fixup(sql, n->data);
			list_append(l, a);
		}
		ne = exp_op(sql->sa, l, f);
		exp_setname(sql->sa, ne, e->rname, e->name );

		/* ifthenelse with one of the sides an 'sql_div' */
		args = ne->l;
		if (!f->func->s && !strcmp(f->func->base.name,"ifthenelse")) { 
			sql_exp *cond = args->h->data, *nne; 
			sql_exp *a1 = args->h->next->data; 
			sql_exp *a2 = args->h->next->next->data; 
			sql_subfunc *a1f = a1->f;
			sql_subfunc *a2f = a2->f;

			/* rewrite right hands of div */
			if (a1->type == e_func && !a1f->func->s && 
			     !strcmp(a1f->func->base.name, "sql_div")) {
				a1 = sql_div_fixup(sql, a1, cond, 0);
			} else if (a1->type == e_func && a1->l) { 
				a1->l = exps_case_fixup(sql, a1->l, cond, 0); 
			}
			if  (a2->type == e_func && !a2f->func->s && 
			     !strcmp(a2f->func->base.name, "sql_div")) { 
				a2 = sql_div_fixup(sql, a2, cond, 1);
			} else if (a2->type == e_func && a2->l) { 
				a2->l = exps_case_fixup(sql, a2->l, cond, 1); 
			}
			nne = exp_op3(sql->sa, cond, a1, a2, ne->f);
			exp_setname(sql->sa, nne, ne->rname, ne->name );
			ne = nne;
		}
		return ne;
	}
	if (e->type == e_convert) {
		sql_exp *e1 = exp_case_fixup(sql, e->l);
		sql_exp *ne = exp_convert(sql->sa, e1, exp_fromtype(e), exp_totype(e));
		exp_setname(sql->sa, ne, e->rname, e->name);
		return ne;
	} 
	return e;
}

static sql_rel *
rel_case_fixup(int *changes, mvc *sql, sql_rel *rel) 
{
	
	(void)changes; /* only go through it once, ie don't mark for changes */
	if (rel->op == op_project && rel->exps) {
		list *exps = rel->exps;
		node *n;

		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = exp_case_fixup( sql, n->data );
		
			if (!e) 
				return NULL;
			list_append(rel->exps, e);
		}
	} 
	return rel;
}

static sql_rel *
rel_find_ref( sql_rel *r)
{
	while (!rel_is_ref(r) && r->l && 
	      (is_project(r->op) || is_select(r->op) /*|| is_join(r->op)*/))
		r = r->l;
	if (rel_is_ref(r))
		return r;
	return NULL;
}

static sql_rel *
rel_find_select( sql_rel *r)
{
	while (!is_select(r->op) && r->l && is_project(r->op))
		r = r->l;
	if (is_select(r->op))
		return r;
	return NULL;
}

static int
rel_match_projections(sql_rel *l, sql_rel *r)
{
	node *n, *m;
	list *le = l->exps;
	list *re = r->exps;

	if (!le || !re)
		return 0;
	if (list_length(le) != list_length(re))
		return 0;

	for (n = le->h, m = re->h; n && m; n = n->next, m = m->next) 
		if (!exp_match(n->data, m->data))
			return 0;
	return 1;
}

static int
exps_has_predicate( list *l )
{
	node *n;

	for( n = l->h; n; n = n->next){
		sql_exp *e = n->data;

		if (e->card <= CARD_ATOM)
			return 1;
	}
	return 0;
}

static sql_rel *
rel_merge_union(int *changes, mvc *sql, sql_rel *rel) 
{
	sql_rel *l = rel->l;
	sql_rel *r = rel->r;
	sql_rel *ref = NULL;

	if (is_union(rel->op) && 
	    l && is_project(l->op) && !project_unsafe(l) &&
	    r && is_project(r->op) && !project_unsafe(r) &&	
	    (ref = rel_find_ref(l)) != NULL && ref == rel_find_ref(r)) {
		/* Find selects and try to merge */
		sql_rel *ls = rel_find_select(l);
		sql_rel *rs = rel_find_select(r);
		
		/* can we merge ? */
		if (!ls || !rs) 
			return rel;

		/* merge any extra projects */
		if (l->l != ls) 
			rel->l = l = rel_merge_projects(changes, sql, l);
		if (r->l != rs) 
			rel->r = r = rel_merge_projects(changes, sql, r);
		
		if (!rel_match_projections(l,r)) 
			return rel;

		/* for now only union(project*(select(R),project*(select(R))) */
		if (ls != l->l || rs != r->l || 
		    ls->l != rs->l || !rel_is_ref(ls->l))
			return rel;

		if (!ls->exps || !rs->exps || 
		    exps_has_predicate(ls->exps) || 
		    exps_has_predicate(rs->exps))
			return rel;

		/* merge, ie. add 'or exp' */
		(*changes)++;
		ls->exps = append(new_exp_list(sql->sa), exp_or(sql->sa, ls->exps, rs->exps));
		rs->exps = NULL;
		rel = rel_inplace_project(sql->sa, rel, rel_dup(rel->l), rel->exps);
		set_processed(rel);
		return rel;
	}
	return rel;
}

static int
exps_cse( sql_allocator *sa, list *oexps, list *l, list *r )
{
	list *nexps;
	node *n, *m;
	char *lu, *ru;
	int lc = 0, rc = 0, match = 0, res = 0;

	/* first recusive exps_cse */
	nexps = new_exp_list(sa);
	for (n = l->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or) {
			res = exps_cse(sa, nexps, e->l, e->r); 
		} else {
			append(nexps, e);
		}
	}
	l = nexps;

	nexps = new_exp_list(sa);
	for (n = r->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or) {
			res = exps_cse(sa, nexps, e->l, e->r); 
		} else {
			append(nexps, e);
		}
	}
	r = nexps;

	lu = calloc(list_length(l), sizeof(char));
	ru = calloc(list_length(r), sizeof(char));
	for (n = l->h, lc = 0; n; n = n->next, lc++) {
		sql_exp *le = n->data;

		for ( m = r->h, rc = 0; m; m = m->next, rc++) {
			sql_exp *re = m->data;

			if (!ru[rc] && exp_match_exp(le,re)) {
				lu[lc] = 1;
				ru[rc] = 1;
				match = 1;
			}
		}
	}
	if (match) {
		list *nl = new_exp_list(sa);
		list *nr = new_exp_list(sa);

		for (n = l->h, lc = 0; n; n = n->next, lc++) 
			if (!lu[lc])
				append(nl, n->data);
		for (n = r->h, rc = 0; n; n = n->next, rc++) 
			if (!ru[rc])
				append(nr, n->data);

		if (list_length(nl) && list_length(nr)) 
			append(oexps, exp_or(sa, nl, nr)); 

		for (n = l->h, lc = 0; n; n = n->next, lc++) {
			if (lu[lc])
				append(oexps, n->data);
		}
		res = 1;
	} else {
		append(oexps, exp_or(sa, list_dup(l, (fdup)NULL), 
				     list_dup(r, (fdup)NULL)));
	}
	free(lu);
	free(ru);
	return res;
}


static sql_rel *
rel_select_cse(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)sql;
	if (is_select(rel->op) && rel->exps) { 
		list *nexps = new_exp_list(sql->sa);
		node *n;

		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or) {
				/* split the common expressions */
				*changes += exps_cse(sql->sa, nexps, e->l, e->r);
			} else {
				append(nexps, e);
			}
		}
		rel->exps = nexps;
	}
	return rel;
}

static sql_rel *
rel_project_cse(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)changes;
	(void)sql;
	if (is_project(rel->op) && rel->exps) { 
		list *nexps = new_exp_list(sql->sa);
		node *n, *m;

		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e1 = n->data;

			if (e1->type != e_column && e1->type != e_atom && e1->name) {
				for (m=nexps->h; m; m = m->next){
					sql_exp *e2 = m->data;
				
					if (exp_name(e2) && exp_match_exp(e1, e2)) {
						sql_exp *ne = exp_alias(sql->sa, e1->rname, exp_name(e1), e2->rname, exp_name(e2), exp_subtype(e2), e2->card, has_nil(e2), is_intern(e1));
						e1 = ne;
						break;
					}
				}
			}
			append(nexps, e1);
		}
		rel->exps = nexps;
	}
	return rel;
}

static list *
exps_merge_rse( mvc *sql, list *l, list *r )
{
	node *n, *m, *o;
	list *nexps = NULL, *lexps, *rexps;

 	lexps = new_exp_list(sql->sa);
	for (n = l->h; n; n = n->next) {
		sql_exp *e = n->data;
	
		if (e->type == e_cmp && e->flag == cmp_or) {
			list *nexps = exps_merge_rse(sql, e->l, e->r);
			for (o = nexps->h; o; o = o->next) 
				append(lexps, o->data);
		} else {
			append(lexps, e);
		}
	}
 	rexps = new_exp_list(sql->sa);
	for (n = r->h; n; n = n->next) {
		sql_exp *e = n->data;
	
		if (e->type == e_cmp && e->flag == cmp_or) {
			list *nexps = exps_merge_rse(sql, e->l, e->r);
			for (o = nexps->h; o; o = o->next) 
				append(rexps, o->data);
		} else {
			append(rexps, e);
		}
	}

 	nexps = new_exp_list(sql->sa);

	/* merge merged lists first ? */
	for (n = lexps->h; n; n = n->next) {
		sql_exp *le = n->data, *re, *fnd = NULL;

		if (le->type != e_cmp || le->flag == cmp_or)
			continue;
		for (m = rexps->h; !fnd && m; m = m->next) {
			re = m->data;
			if (exps_match_col_exps(le, re))
				fnd = re;
		}
		/* cases
		 * 1) 2 values (cmp_equal)
		 * 2) 1 value (cmp_equal), and cmp_in 
		 * 	(also cmp_in, cmp_equal)
		 * 3) 2 cmp_in
		 * 4) ranges 
		 */
		if (fnd) {
			re = fnd;
			fnd = NULL;
			if (le->flag == cmp_equal && re->flag == cmp_equal) {
				list *exps = new_exp_list(sql->sa);

				append(exps, le->r);
				append(exps, re->r);
				fnd = exp_in(sql->sa, le->l, exps, cmp_in);
			} else if (le->flag == cmp_equal && re->flag == cmp_in){
				list *exps = new_exp_list(sql->sa);
				
				append(exps, le->r);
				list_merge(exps, re->r, NULL);
				fnd = exp_in(sql->sa, le->l, exps, cmp_in);
			} else if (le->flag == cmp_in && re->flag == cmp_equal){
				list *exps = new_exp_list(sql->sa);
				
				append(exps, re->r);
				list_merge(exps, le->r, NULL);
				fnd = exp_in(sql->sa, le->l, exps, cmp_in);
			} else if (le->flag == cmp_in && re->flag == cmp_in){
				list *exps = new_exp_list(sql->sa);

				list_merge(exps, le->r, NULL);
				list_merge(exps, re->r, NULL);
				fnd = exp_in(sql->sa, le->l, exps, cmp_in);
			} else if (le->f && re->f && 
				   le->flag == re->flag) {
				sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(le->r), exp_subtype(re->r));
				sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(le->f), exp_subtype(re->f));
				sql_exp *mine, *maxe;

				if (!min || !max)
					continue;
				mine = exp_binop(sql->sa, le->r, re->r, min);
				maxe = exp_binop(sql->sa, le->f, re->f, max);
				fnd = exp_compare2(sql->sa, le->l, mine, maxe, le->flag);
			}
			if (fnd)
				append(nexps, fnd);
		}
	}
	return nexps;
}


/* merge related sub expressions 
 * 
 * ie   (x = a and y > 1 and y < 5) or 
 *      (x = c and y > 1 and y < 10) or 
 *      (x = e and y > 1 and y < 20) 
 * ->
 *     ((x = a and y > 1 and b < 5) or 
 *      (x = c and y > 1 and y < 5) or 
 *      (x = e and y > 1 and y < 5)) and
 *     	 x in (a,b,c) and
 *     	 y > 1 and y < 20
 * */
static sql_rel *
rel_merge_rse(int *changes, mvc *sql, sql_rel *rel) 
{
	/* only execute once per select */
	(void)*changes;

	if (is_select(rel->op) && rel->exps) { 
		node *n, *o;
		list *nexps = new_exp_list(sql->sa);

		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or) {
				/* possibly merge related expressions */
				list *ps = exps_merge_rse(sql, e->l, e->r);
				for (o = ps->h; o; o = o->next) 
					append(nexps, o->data);
			}
		}
		if (list_length(nexps))
		       for (o = nexps->h; o; o = o->next)
				append(rel->exps, o->data);
	}
	return rel;
}

/*
 * Rewrite aggregations over union all.
 *	groupby ([ union all (a, b) ], [gbe], [ count, sum ] )
 *
 * into
 * 	groupby ( [ union all( groupby( a, [gbe], [ count, sum] ), [ groupby( b, [gbe], [ count, sum] )) , [gbe], [sum, sum] ) 
 */
static sql_rel *
rel_push_aggr_down(int *changes, mvc *sql, sql_rel *rel) 
{
	if (rel->op == op_groupby && rel->l) {
		sql_rel *u = rel->l, *ou = u;
		sql_rel *g = rel;
		sql_rel *ul = u->l;
		sql_rel *ur = u->r;
		node *n, *m;
		list *lgbe = NULL, *rgbe = NULL, *gbe = NULL, *exps = NULL;

		if (u->op == op_project)
			u = u->l;

		if (!u || !is_union(u->op) || need_distinct(u) || !u->exps || rel_is_ref(u)) 
			return rel;

		ul = u->l;
		ur = u->r;

		/* make sure we don't create group by on group by's */
		if (ul->op == op_groupby || ur->op == op_groupby) 
			return rel;

		rel->subquery = 0;
		/* distinct should be done over the full result */
		for (n = g->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subaggr *af = e->f;

			if (e->type == e_atom || 
			    e->type == e_func || 
			   (e->type == e_aggr && 
			   ((strcmp(af->aggr->base.name, "sum") && 
			     strcmp(af->aggr->base.name, "count") &&
			     strcmp(af->aggr->base.name, "min") &&
			     strcmp(af->aggr->base.name, "max")) ||
			   need_distinct(e))))
				return rel; 
		}

		ul = rel_dup(ul);
		ur = rel_dup(ur);
		if (!is_project(ul->op)) 
			ul = rel_project(sql->sa, ul, 
				rel_projections(sql, ul, NULL, 1, 1));
		if (!is_project(ur->op)) 
			ur = rel_project(sql->sa, ur, 
				rel_projections(sql, ur, NULL, 1, 1));
		rel_rename_exps(sql, u->exps, ul->exps);
		rel_rename_exps(sql, u->exps, ur->exps);
		if (u != ou) {
			ul = rel_project(sql->sa, ul, NULL);
			ul->exps = exps_copy(sql->sa, ou->exps);
			rel_rename_exps(sql, ou->exps, ul->exps);
			ur = rel_project(sql->sa, ur, NULL);
			ur->exps = exps_copy(sql->sa, ou->exps);
			rel_rename_exps(sql, ou->exps, ur->exps);
		}	

		if (g->r && list_length(g->r) > 0) {
			list *gbe = g->r;

			lgbe = exps_copy(sql->sa, gbe);
			rgbe = exps_copy(sql->sa, gbe);
		}
		ul = rel_groupby(sql->sa, ul, NULL);
		ul->r = lgbe;
		ul->nrcols = g->nrcols;
		ul->card = g->card;
		ul->exps = exps_copy(sql->sa, g->exps);

		ur = rel_groupby(sql->sa, ur, NULL);
		ur->r = rgbe;
		ur->nrcols = g->nrcols;
		ur->card = g->card;
		ur->exps = exps_copy(sql->sa, g->exps);

		/* group by on primary keys which define the partioning scheme 
		 * don't need a finalizing group by */
		/* how to check if a partion is based on some primary key ? 
		 * */
		if (rel->r && list_length(rel->r)) {
			node *n;

			for (n = ((list*)rel->r)->h; n; n = n->next) {
				sql_exp *gbe = n->data;

				if (find_prop(gbe->p, PROP_HASHIDX)) {
					fcmp cmp = (fcmp)&kc_column_cmp;
					sql_column *c = exp_find_column(rel->l, gbe, -2);

					/* check if key is partition key */
					if (c && c->t->p && list_find(c->t->pkey->k.columns, c, cmp) != NULL) {
						(*changes)++;
						return rel_inplace_setop(rel, ul, ur, op_union,
					       	       rel_projections(sql, rel, NULL, 1, 1));
					}
				}
			}
		}

		u = rel_setop(sql->sa, ul, ur, op_union);
		u->exps = rel_projections(sql, rel, NULL, 1, 1);

		if (rel->r) {
			list *ogbe = rel->r;

			gbe = new_exp_list(sql->sa);
			for (n = ogbe->h; n; n = n->next) { 
				sql_exp *e = n->data, *ne;

				ne = list_find_exp( u->exps, e);
				assert(ne);
				ne = exp_column(sql->sa, exp_find_rel_name(ne), exp_name(ne), exp_subtype(ne), ne->card, has_nil(ne), is_intern(ne));
				append(gbe, ne);
			}
		}
		exps = new_exp_list(sql->sa); 
		for (n = u->exps->h, m = rel->exps->h; n && m; n = n->next, m = m->next) {
			sql_exp *ne, *e = n->data, *oa = m->data;

			if (oa->type == e_aggr) {
				sql_subaggr *f = oa->f;
				int cnt = strcmp(f->aggr->base.name,"count")==0;
				sql_subaggr *a = sql_bind_aggr(sql->sa, sql->session->schema, (cnt)?"sum":f->aggr->base.name, exp_subtype(e));

				assert(a);
				/* union of aggr result may have nils 
			   	 * because sum/count of empty set */
				set_has_nil(e);
				e = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
				ne = exp_aggr1(sql->sa, e, a, need_distinct(e), 1, e->card, 1);
			} else {
				ne = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			}
			exp_setname(sql->sa, ne, exp_find_rel_name(oa), exp_name(oa));
			append(exps, ne);
		}
		(*changes)++;
		return rel_inplace_groupby( rel, u, gbe, exps);
	}
	return rel;
}

/*
 * Push select down, pushes the selects through (simple) projections. Also
 * it cleans up the projections which become useless.
 */
static sql_rel *
rel_push_select_down(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = NULL;
	sql_rel *r = NULL;
	node *n;

	if (rel_is_ref(rel))
		return rel;

	/* merge 2 selects */
	r = rel->l;
	if (is_select(rel->op) && r && is_select(r->op) && !(rel_is_ref(r))) {
		(void)list_merge(r->exps, rel->exps, (fdup)NULL);
		rel->l = NULL;
		rel_destroy(rel);
		(*changes)++;
		return rel_push_select_down(changes, sql, r);
	}
	/* 
	 * Push select through semi/anti join 
	 * 	select (semi(A,B)) == semi(select(A), B) 
	 */
	if (is_select(rel->op) && r && is_semi(r->op) && !(rel_is_ref(r))) {
		rel->l = r->l;
		r->l = rel;
		(*changes)++;
		/* 
		 * if A has 2 references (ie used on both sides of 
		 * the semi join), we also push the select into A. 
		 */
		if (rel_is_ref(rel->l) && rel->l == rel_find_ref(r->r)){
			sql_rel *lx = rel->l;
			sql_rel *rx = r->r;
			if (lx->ref.refcnt == 2 && !rel_is_ref(rx)) {
				while (rx->l && !rel_is_ref(rx->l) &&
	      			       (is_project(rx->op) || 
					is_select(rx->op) ||
					is_join(rx->op)))
						rx = rx->l;
				/* probably we need to introduce a project */
				rel_destroy(rel->l);
				lx = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
				r->l = lx;
				rx->l = rel_dup(lx);
			}
		}
		return r;
	}
	exps = rel->exps;

	if (rel->op == op_project && 
	    r && r->op == op_project && !(rel_is_ref(r))) 
		return rel_merge_projects(changes, sql, rel);

	/* push select through join */
	if (is_select(rel->op) && r && is_join(r->op) && !(rel_is_ref(r))) {
		sql_rel *jl = r->l;
		sql_rel *jr = r->r;
		int left = r->op == op_join || r->op == op_left;
		int right = r->op == op_join || r->op == op_right;

		if (r->op == op_full)
			return rel;

		/* introduce selects under the join (if needed) */
		if (!is_select(jl->op))
			r->l = jl = rel_select(sql->sa, jl, NULL);
		if (!is_select(jr->op))
			r->r = jr = rel_select(sql->sa, jr, NULL);
		
		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data, *ne = NULL;
			int done = 0;
	
			if (left)
				ne = exp_push_down(sql, e, jl, jl);
			if (ne && ne != e) {
				done = 1; 
				rel_select_add_exp(jl, ne);
			} else if (right) {
				ne = exp_push_down(sql, e, jr, jr);
				if (ne && ne != e) {
					done = 1; 
					rel_select_add_exp(jr, ne);
				}
			}
			if (!done)
				append(rel->exps, e);
			*changes += done;
		}
		return rel;
	}

	/* merge select and cross product ? */
	if (is_select(rel->op) && r && r->op == op_join && !(rel_is_ref(r))) {
		list *exps = rel->exps;

		if (!r->exps)
			r->exps = new_exp_list(sql->sa); 
		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data;

			if (exp_is_join_exp(e) == 0) {
				append(r->exps, e);
				(*changes)++;
			} else {
				append(rel->exps, e);
			}
		}
		return rel;
	}

	if (is_select(rel->op) && r && r->op == op_project && !(rel_is_ref(r))){
		list *exps = rel->exps;
		sql_rel *pl;
		/* we cannot push through rank (row_number etc) functions or
		   projects with distinct */
		if (!r->l || project_unsafe(r))
			return rel;

		/* here we need to fix aliases */
		rel->exps = new_exp_list(sql->sa); 
		pl = r->l;
		/* introduce selects under the project (if needed) */
		if (!is_select(pl->op))
			r->l = pl = rel_select(sql->sa, pl, NULL);

		/* for each exp check if we can rename it */
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data, *ne = NULL;

			/* sometimes we also have functions in the expression list (TODO change them to e_cmp (predicates like (1=0))) */
			if (e->type == e_cmp) {
				ne = exp_push_down_prj(sql, e, r, pl);

				/* can we move it down */
				if (ne && ne != e) {
					rel_select_add_exp(pl, ne);
					(*changes)++;
				} else {
					append(rel->exps, (ne)?ne:e);
				}
			} else {
				list_append(rel->exps, e);
			}
		}
		return rel;
	}
	return rel;
}

static sql_rel *
rel_push_select_down_join(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = NULL;
	sql_rel *r = NULL;
	node *n;

	exps = rel->exps;
	r = rel->l;

	/* push select through join */
	if (is_select(rel->op) && r && r->op == op_join && !(rel_is_ref(r))) {
		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data;
			if (e->type == e_cmp && !e->f && !is_complex_exp(e->flag)) {
				sql_exp *re = e->r;

				if (re->card >= CARD_AGGR) {
					rel->l = rel_push_join(sql->sa, r, e->l, re, e);

				} else {
					rel->l = rel_push_select(sql->sa, r, e->l, e);
				}
				/* only pushed down selects are counted */
				if (r == rel->l) {
					(*changes)++;
				} else { /* Do not introduce an extra select */
					sql_rel *r = rel->l;

					rel->l = r->l;
					r->l = NULL;
					list_append(rel->exps, e);
					rel_destroy(r);
				}
				assert(r == rel->l);
			} else {
				list_append(rel->exps, e);
			} 
		}
		return rel;
	}
	return rel;
}

static sql_rel *
rel_remove_empty_select(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)sql;

	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op) || is_project(rel->op) || rel->op == op_topn) && rel->l) {
		sql_rel *l = rel->l;
		if (is_select(l->op) && !(rel_is_ref(l)) &&
		   (!l->exps || list_length(l->exps) == 0)) {
			rel->l = l->l;
			l->l = NULL;
			rel_destroy(l);
			(*changes)++;
		} 
	}
	if ((is_join(rel->op) || is_semi(rel->op) || is_set(rel->op)) && rel->r) {
		sql_rel *r = rel->r;
		if (is_select(r->op) && !(rel_is_ref(r)) &&
	   	   (!r->exps || list_length(r->exps) == 0)) {
			rel->r = r->l;
			r->l = NULL;
			rel_destroy(r);
			(*changes)++;
		}
	} 
	if (is_join(rel->op) && rel->exps && list_length(rel->exps) == 0) 
		rel->exps = NULL;
	return rel;
}

/*
 * Push {semi}joins down, pushes the joins through group by expressions. 
 * When the join is on the group by columns, we can push the joins left
 * under the group by.
 *
 * {semi}join( A, groupby( B ) [gbe][aggrs] ) [ gbe == A.x ]
 * ->
 * {semi}join( A, groupby( semijoin(B,A) [gbe == A.x] ) [gbe][aggrs] ) [ gbe == A.x ]
 */
static sql_rel *
rel_push_join_down(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = NULL;

	(void)*changes;
	if (((is_join(rel->op) && rel->exps) || is_semi(rel->op)) && rel->l) {
		sql_rel *gb = rel->r, *ogb = gb, *l = NULL, *rell = rel->l;

		if (gb->op == op_project)
			gb = gb->l;

		if (is_basetable(rell->op))
			return rel;

		exps = rel->exps;
		if (gb && gb->op == op_groupby && gb->r && list_length(gb->r)) {
			list *jes = new_exp_list(sql->sa);
			node *n, *m;
			list *gbes = gb->r;
			/* find out if all group by expressions are used in the join */
			for(n = gbes->h; n; n = n->next) {
				sql_exp *gbe = n->data;
				int fnd = 0;
				char *rname = NULL, *name = NULL;

				/* project in between, ie find alias */
				/* first find expression in expression list */
				gbe = exp_uses_exp( gb->exps, gbe);
				if (!gbe)
					continue;
				if (ogb != gb) 
					gbe = exp_uses_exp( ogb->exps, gbe);
				if (gbe) {
					rname = exp_find_rel_name(gbe);
					name = exp_name(gbe);
				}

				for (m = exps->h; m && !fnd; m = m->next) {
					sql_exp *je = m->data;

					if (je->card >= CARD_ATOM && je->type == e_cmp && 
					    !is_complex_exp(je->flag)) {
						/* expect right expression to match */
						sql_exp *r = je->r;

						if (r == 0 || r->type != e_column)
							continue;
						if (r->l && rname && strcmp(r->l, rname) == 0 && strcmp(r->r, name)==0) {
							fnd = 1;
						} else if (!r->l && !rname  && strcmp(r->r, name)==0) {
							fnd = 1;
						}
						if (fnd) {
							sql_exp *re = exp_push_down_prj(sql, r, gb, gb->l);
							if (!re) {
								fnd = 0;
							} else {
								je = exp_compare(sql->sa, je->l, re, je->flag);
								list_append(jes, je);
							}
						}
					}
				}
				if (!fnd) 
					return rel;
			}
			l = rel_dup(rel->l);

			/* push join's left side (as semijoin) down group by */
			l = gb->l = rel_crossproduct(sql->sa, gb->l, l, op_semi);
			l->exps = jes;
			return rel;
		} 
	}
	return rel;
}

/*
 * Push semijoins down, pushes the semijoin through a join. 
 *
 * semijoin( join(A, B) [ A.x == B.y ], C ) [ A.z == C.c ]
 * ->
 * join( semijoin(A, C) [ A.z == C.c ], B ) [ A.x == B.y ]
 */
#if 0
static sql_rel *
rel_push_semijoin_down(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = NULL;

	(void)*changes;
	if (is_semi(rel->op) && rel->exps && rel->l) {
		sql_rel *l = rel->l, *ol = l, *ll = NULL, *lr = NULL;
		sql_rel *r = rel->r;
		list *exps = rel->exps, *nsexps, *njexps;

		/* handle project 
		if (l->op == op_project && !need_distinct(l))
			l = l->l;
		*/

		if (!is_join(l->op)  
			return rel;

		ll = l->l;
		lr = l->r;
		/* semijoin shouldn't be based on right relation of join */)
		for(n = exps->h; n; n = n->next) {
			sql_exp *sje = n->data;

			if (is_complex_exp(e->flag) || 
			    rel_has_exp(lr, sje->l) ||
			    rel_has_exp(lr, sje->r))
				return rel;
			}
		} 
		nsexps = list_new(sql->sa);
		for(n = exps->h; n; n = n->next) {
			sql_exp *sje = n->data;
		} 
		njexps = list_new(sql->sa);
		if (l->exps) {
			for(n = l->exps->h; n; n = n->next) {
				sql_exp *sje = n->data;
			} 
		}
		l = rel_crossproduce(sql->sa, ll, r, op_semi);
		l->exps = nsexps;
		rel = rel_inplace_join(rel, l, lr, op_join, njexps);
	}
	return rel;
}
#endif

static int
rel_is_join_on_pkey( sql_rel *rel ) 
{
	node *n;

	if (!rel || !rel->exps)
		return 0;
	for (n = rel->exps->h; n; n = n->next){
		sql_exp *je = n->data;

		if (je->type == e_cmp && je->flag == cmp_equal &&
		    find_prop(((sql_exp*)je->l)->p, PROP_HASHIDX)) { /* aligned PKEY JOIN */
			fcmp cmp = (fcmp)&kc_column_cmp;
			sql_exp *e = je->l;
			sql_column *c = exp_find_column(rel, e, -2);

			if (c && c->t->p && list_find(c->t->pkey->k.columns, c, cmp) != NULL) 
				return 1;
		}
	}
	return 0;
}

static int
rel_part_nr( sql_rel *rel, sql_exp *e )
{
	sql_column *c;
	sql_table *pp;
	assert(e->type == e_cmp);

	c = exp_find_column(rel, e->l, -1); 
	if (!c)
		c = exp_find_column(rel, e->r, -1); 
	if (!c)
		return -1;
	pp = c->t;
	if (pp->p)
		return list_position(pp->p->tables.set, pp);
	return -1;
}

static int
rel_uses_part_nr( sql_rel *rel, sql_exp *e, int pnr )
{
	sql_column *c;
	assert(e->type == e_cmp);

	/* 
	 * following case fails. 
	 *
	 * semijoin( A1, union [A1, A2] )
	 * The union will never return proper column (from A2).
	 * ie need different solution (probaly pass pnr).
	 */
	c = exp_find_column(rel, e->l, pnr); 
	if (!c)
		c = exp_find_column(rel, e->r, pnr); 
	if (c) {
		sql_table *pp = c->t;
		if (pp->p && list_position(pp->p->tables.set, pp) == pnr)
			return 1;
	}
	/* for projects we may need to do a rename! */
	if (is_project(rel->op) || is_topn(rel->op))
		return rel_uses_part_nr( rel->l, e, pnr);

	if (is_union(rel->op) || is_join(rel->op) || is_semi(rel->op)) {
		if (rel_uses_part_nr( rel->l, e, pnr))
			return 1;
		if (!is_semi(rel->op) && rel_uses_part_nr( rel->r, e, pnr))
			return 1;
	}
	return 0;
}

/*
 * Push (semi)joins down unions, this is basically for merge tables, where
 * we know that the fk-indices are split over two clustered merge tables.
 */
static sql_rel *
rel_push_join_down_union(int *changes, mvc *sql, sql_rel *rel) 
{
	if (((is_join(rel->op) && !is_outerjoin(rel->op)) || is_semi(rel->op)) && rel->exps) {
		sql_rel *l = rel->l, *r = rel->r, *ol = l, *or = r;
		list *exps = rel->exps;
		sql_exp *je = exps->h->data;

		if (!l || !r || need_distinct(l) || need_distinct(r))
			return rel;
		if (l->op == op_project)
			l = l->l;
		if (r->op == op_project)
			r = r->l;

		/* both sides only if we have a join index */
		if (!l || !r ||(is_union(l->op) && is_union(r->op) && 
			!find_prop(je->p, PROP_JOINIDX) && /* FKEY JOIN */
			!rel_is_join_on_pkey(rel))) /* aligned PKEY JOIN */
			return rel;

		ol->subquery = or->subquery = 0;
		if ((is_union(l->op) && !need_distinct(l)) && !is_union(r->op)){
			sql_rel *nl, *nr;
			sql_rel *ll = rel_dup(l->l), *lr = rel_dup(l->r);

			/* join(union(a,b), c) -> union(join(a,c), join(b,c)) */
			if (!is_project(ll->op))
				ll = rel_project(sql->sa, ll, 
					rel_projections(sql, ll, NULL, 1, 1));
			if (!is_project(lr->op))
				lr = rel_project(sql->sa, lr, 
					rel_projections(sql, lr, NULL, 1, 1));
			rel_rename_exps(sql, l->exps, ll->exps);
			rel_rename_exps(sql, l->exps, lr->exps);
			if (l != ol) {
				ll = rel_project(sql->sa, ll, NULL);
				ll->exps = exps_copy(sql->sa, ol->exps);
				lr = rel_project(sql->sa, lr, NULL);
				lr->exps = exps_copy(sql->sa, ol->exps);
			}	
			nl = rel_crossproduct(sql->sa, ll, rel_dup(or), rel->op);
			nr = rel_crossproduct(sql->sa, lr, rel_dup(or), rel->op);
			nl->exps = exps_copy(sql->sa, exps);
			nr->exps = exps_copy(sql->sa, exps);
			(*changes)++;
			return rel_inplace_setop(rel, nl, nr, op_union, rel_projections(sql, rel, NULL, 1, 1));
		} else if (is_union(l->op) && !need_distinct(l) &&
			   is_union(r->op) && !need_distinct(r)) {
			sql_rel *nl, *nr;
			sql_rel *ll = rel_dup(l->l), *lr = rel_dup(l->r);
			sql_rel *rl = rel_dup(r->l), *rr = rel_dup(r->r);

			/* join(union(a,b), union(c,d)) -> union(join(a,c), join(b,d)) */
			if (!is_project(ll->op))
				ll = rel_project(sql->sa, ll, 
					rel_projections(sql, ll, NULL, 1, 1));
			if (!is_project(lr->op))
				lr = rel_project(sql->sa, lr, 
					rel_projections(sql, lr, NULL, 1, 1));
			rel_rename_exps(sql, l->exps, ll->exps);
			rel_rename_exps(sql, l->exps, lr->exps);
			if (l != ol) {
				ll = rel_project(sql->sa, ll, NULL);
				ll->exps = exps_copy(sql->sa, ol->exps);
				lr = rel_project(sql->sa, lr, NULL);
				lr->exps = exps_copy(sql->sa, ol->exps);
			}	
			if (!is_project(rl->op))
				rl = rel_project(sql->sa, rl, 
					rel_projections(sql, rl, NULL, 1, 1));
			if (!is_project(rr->op))
				rr = rel_project(sql->sa, rr, 
					rel_projections(sql, rr, NULL, 1, 1));
			rel_rename_exps(sql, r->exps, rl->exps);
			rel_rename_exps(sql, r->exps, rr->exps);
			if (r != or) {
				rl = rel_project(sql->sa, rl, NULL);
				rl->exps = exps_copy(sql->sa, or->exps);
				rr = rel_project(sql->sa, rr, NULL);
				rr->exps = exps_copy(sql->sa, or->exps);
			}	
			nl = rel_crossproduct(sql->sa, ll, rl, rel->op);
			nr = rel_crossproduct(sql->sa, lr, rr, rel->op);
			nl->exps = exps_copy(sql->sa, exps);
			nr->exps = exps_copy(sql->sa, exps);

			(*changes)++;
			return rel_inplace_setop(rel, nl, nr, op_union, rel_projections(sql, rel, NULL, 1, 1));
		} else if (!is_union(l->op) && 
			   is_union(r->op) && !need_distinct(r) &&
			   !is_semi(rel->op)) {
			sql_rel *nl, *nr;
			sql_rel *rl = rel_dup(r->l), *rr = rel_dup(r->r);

			/* join(a, union(b,c)) -> union(join(a,b), join(a,c)) */
			if (!is_project(rl->op))
				rl = rel_project(sql->sa, rl, 
					rel_projections(sql, rl, NULL, 1, 1));
			if (!is_project(rr->op))
				rr = rel_project(sql->sa, rr, 
					rel_projections(sql, rr, NULL, 1, 1));
			rel_rename_exps(sql, r->exps, rl->exps);
			rel_rename_exps(sql, r->exps, rr->exps);
			if (r != or) {
				rl = rel_project(sql->sa, rl, NULL);
				rl->exps = exps_copy(sql->sa, or->exps);
				rr = rel_project(sql->sa, rr, NULL);
				rr->exps = exps_copy(sql->sa, or->exps);
			}	
			nl = rel_crossproduct(sql->sa, rel_dup(ol), rl, rel->op);
			nr = rel_crossproduct(sql->sa, rel_dup(ol), rr, rel->op);
			nl->exps = exps_copy(sql->sa, exps);
			nr->exps = exps_copy(sql->sa, exps);

			(*changes)++;
			return rel_inplace_setop(rel, nl, nr, op_union, rel_projections(sql, rel, NULL, 1, 1));
		/* {semi}join ( A1, union (A2, B)) [A1.partkey = A2.partkey] ->
		 * {semi}join ( A1, A2 ) 
		 * and
		 * {semi}join ( A1, union (B, A2)) [A1.partkey = A2.partkey] ->
		 * {semi}join ( A1, A2 ) 
		 * (ie a single part on the left)
		 *
		 * Howto detect that a relation isn't matching.
		 *
		 * partitioning is currently done only on pkey/fkey's
		 * ie only matching per part if join is on pkey/fkey (parts)
		 *
		 * and part numbers should match.
		 *
		 * */
		} else if (!is_union(l->op) && 
			   is_union(r->op) && !need_distinct(r) &&
			   is_semi(rel->op) && rel_is_join_on_pkey(rel)) {
			/* use first join expression, to find part nr */
			sql_exp *je = rel->exps->h->data;
			int lpnr = rel_part_nr(l, je);
			sql_rel *rl = r->l;
			sql_rel *rr = r->r;

			if (lpnr < 0)
				return rel;
			/* case 1: uses left not right */
			if (rel_uses_part_nr(rl, je, lpnr) && 
			   !rel_uses_part_nr(rr, je, lpnr)) {
				sql_rel *nl;

				rl = rel_dup(rl);
				if (!is_project(rl->op))
					rl = rel_project(sql->sa, rl, 
					rel_projections(sql, rl, NULL, 1, 1));
				rel_rename_exps(sql, r->exps, rl->exps);
				if (r != or) {
					rl = rel_project(sql->sa, rl, NULL);
					rl->exps = exps_copy(sql->sa, or->exps);
				}	
				nl = rel_crossproduct(sql->sa, rel_dup(ol), rl, rel->op);
				nl->exps = exps_copy(sql->sa, exps);
				(*changes)++;
				return rel_inplace_project(sql->sa, rel, nl, rel_projections(sql, rel, NULL, 1, 1));
			/* case 2: uses right not left */
			} else if (!rel_uses_part_nr(rl, je, lpnr) && 
				    rel_uses_part_nr(rr, je, lpnr)) {
				sql_rel *nl;

				rr = rel_dup(rr);
				if (!is_project(rr->op))
					rr = rel_project(sql->sa, rr, 
						rel_projections(sql, rr, NULL, 1, 1));
				rel_rename_exps(sql, r->exps, rr->exps);
				if (r != or) {
					rr = rel_project(sql->sa, rr, NULL);
					rr->exps = exps_copy(sql->sa, or->exps);
				}	
				nl = rel_crossproduct(sql->sa, rel_dup(ol), rr, rel->op);
				nl->exps = exps_copy(sql->sa, exps);
				(*changes)++;
				return rel_inplace_project(sql->sa, rel, nl, rel_projections(sql, rel, NULL, 1, 1));
			}
		}
	}
	return rel;
}

static int 
rel_is_empty( sql_rel *rel )
{
	(void)rel;
	if ((is_join(rel->op) || is_semi(rel->op)) && rel->exps) {
		sql_rel *l = rel->l, *r = rel->r;

		if (rel_is_empty(l) || (is_join(rel->op) && rel_is_empty(r)))
			return 1;
		/* check */
		if (rel_is_join_on_pkey(rel)) {
			sql_exp *je = rel->exps->h->data;
			int lpnr = rel_part_nr(l, je);

			if (lpnr >= 0 && !rel_uses_part_nr(r, je, lpnr))
				return 1;
		}
	}
	if (!is_union(rel->op) && 
			(is_project(rel->op) || is_topn(rel->op)) && rel->l)
		return rel_is_empty(rel->l);
	return 0;
}

/* non overlapping partitions should be removed */
static sql_rel *
rel_remove_empty_join(mvc *sql, sql_rel *rel, int *changes) 
{
	/* recurse check rel_is_empty 
	 * For half empty unions replace by projects
	 * */
	if (is_union(rel->op)) {
		sql_rel *l = rel->l, *r = rel->r;

		rel->l = l = rel_remove_empty_join(sql, l, changes);
		rel->r = r = rel_remove_empty_join(sql, r, changes);
		if (rel_is_empty(l)) {
			(*changes)++;
			return rel_inplace_project(sql->sa, rel, rel_dup(r), rel->exps);
		} else if (rel_is_empty(r)) {
			(*changes)++;
			return rel_inplace_project(sql->sa, rel, rel_dup(l), rel->exps);
		}
	} else if ((is_project(rel->op) || is_topn(rel->op) || is_select(rel->op)) && rel->l) {
		rel->l = rel_remove_empty_join(sql, rel->l, changes);
	} else if (is_join(rel->op)) {
		rel->l = rel_remove_empty_join(sql, rel->l, changes);
		rel->r = rel_remove_empty_join(sql, rel->r, changes);
	}
	return rel;
}

static sql_rel *
rel_push_select_down_union(int *changes, mvc *sql, sql_rel *rel) 
{
	if (is_select(rel->op) && rel->l && rel->exps) {
		sql_rel *u = rel->l, *ou = u;
		sql_rel *s = rel;
		sql_rel *ul = u->l;
		sql_rel *ur = u->r;

		if (u->op == op_project)
			u = u->l;

		if (!u || !is_union(u->op) || need_distinct(u) || !u->exps || rel_is_ref(u))
			return rel;

		ul = u->l;
		ur = u->r;

		rel->subquery = 0;
		u->subquery = 0;
		ul->subquery = 0;
		ur->subquery = 0;
		ul = rel_dup(ul);
		ur = rel_dup(ur);
		if (!is_project(ul->op)) 
			ul = rel_project(sql->sa, ul, 
				rel_projections(sql, ul, NULL, 1, 1));
		if (!is_project(ur->op)) 
			ur = rel_project(sql->sa, ur, 
				rel_projections(sql, ur, NULL, 1, 1));
		rel_rename_exps(sql, u->exps, ul->exps);
		rel_rename_exps(sql, u->exps, ur->exps);

		if (u != ou) {
			ul = rel_project(sql->sa, ul, NULL);
			ul->exps = exps_copy(sql->sa, ou->exps);
			rel_rename_exps(sql, ou->exps, ul->exps);
			ur = rel_project(sql->sa, ur, NULL);
			ur->exps = exps_copy(sql->sa, ou->exps);
			rel_rename_exps(sql, ou->exps, ur->exps);
		}	

		/* introduce selects under the set (if needed) */
		ul = rel_select(sql->sa, ul, NULL);
		ur = rel_select(sql->sa, ur, NULL);
		
		ul->exps = exps_copy(sql->sa, s->exps);
		ur->exps = exps_copy(sql->sa, s->exps);

		rel = rel_inplace_setop(rel, ul, ur, op_union, rel_projections(sql, rel, NULL, 1, 1));
		(*changes)++;
		return rel;
	}
	return rel;
}

static int
exps_unique( list *exps )
{
	node *n;

	if ((n = exps->h) != NULL) {
		sql_exp *e = n->data;

		if (e && find_prop(e->p, PROP_HASHIDX))
			return 1;
	}
	return 0;
}

static sql_rel *
rel_push_project_down_union(int *changes, mvc *sql, sql_rel *rel) 
{
	if (rel->op == op_project && rel->l && rel->exps && !rel->r) {
		int need_distinct = need_distinct(rel);
		sql_rel *u = rel->l;
		sql_rel *p = rel;
		sql_rel *ul = u->l;
		sql_rel *ur = u->r;

		if (!u || !is_union(u->op) || need_distinct(u) || !u->exps || rel_is_ref(u))
			return rel;

		rel->subquery = 0;
		u->subquery = 0;
		ul = rel_dup(ul);
		ur = rel_dup(ur);

		if (!is_project(ul->op)) 
			ul = rel_project(sql->sa, ul, 
				rel_projections(sql, ul, NULL, 1, 1));
		if (!is_project(ur->op)) 
			ur = rel_project(sql->sa, ur, 
				rel_projections(sql, ur, NULL, 1, 1));
		need_distinct = (need_distinct && 
				(!exps_unique(ul->exps) ||
				 !exps_unique(ur->exps)));
		rel_rename_exps(sql, u->exps, ul->exps);
		rel_rename_exps(sql, u->exps, ur->exps);

		/* introduce projects under the set */
		ul = rel_project(sql->sa, ul, NULL);
		if (need_distinct)
			set_distinct(ul);
		ur = rel_project(sql->sa, ur, NULL);
		if (need_distinct)
			set_distinct(ur);
		
		ul->exps = exps_copy(sql->sa, p->exps);
		ur->exps = exps_copy(sql->sa, p->exps);

		rel = rel_inplace_setop(rel, ul, ur, op_union,
			rel_projections(sql, rel, NULL, 1, 1));
		if (need_distinct)
			set_distinct(rel);
		(*changes)++;
		return rel;
	}
	return rel;
}

static sql_exp *
rel_find_aggr_exp(sql_allocator *sa, sql_rel *rel, list *exps, sql_exp *e, char *name)
{
 	list *ea = e->l;
	sql_exp *a = NULL, *eae;
	node *n;

	(void)rel;
	if (list_length(ea) != 1)
		return NULL;
	eae = ea->h->data;
	if (eae->type != e_column)
		return NULL;
	for( n = exps->h; n; n = n->next) {
		a = n->data;
		
		if (a->type == e_aggr) {
			sql_subaggr *af = a->f;
			list *aa = a->l;
			
			/* TODO handle distinct and no-nil etc ! */
			if (strcmp(af->aggr->base.name, name) == 0 &&
				/* TODO handle count (has no args!!) */
			    aa && list_length(aa) == 1) {
				sql_exp *aae = aa->h->data;

				if (eae->type == e_column &&
				    ((!aae->l && !eae->l) ||
				    (aae->l && eae->l &&
				    strcmp(aae->l, eae->l) == 0)) &&
				    (aae->r && eae->r &&
				    strcmp(aae->r, eae->r) == 0)) 
					return exp_column(sa, a->rname, exp_name(a), exp_subtype(a), a->card, has_nil(a), is_intern(a));
			}
		}
	}
	return NULL;
}

/* TODO for count we need remove useless 'converts' etc */
/* rewrite avg into sum/count */
static sql_rel *
rel_avg2sum_count(int *changes, mvc *sql, sql_rel *rel)
{
	if (is_groupby(rel->op) && !rel_is_ref(rel)) {
		list *pexps, *nexps = new_exp_list(sql->sa), *avgs = new_exp_list(sql->sa);
		list *aexps = new_exp_list(sql->sa); /* alias list */
		node *m, *n;

		/* Find all avg's */
		for (m = rel->exps->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subaggr *a = e->f;
				
				if (strcmp(a->aggr->base.name, "avg") == 0) {
					append(avgs, e);
					continue; 
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column && 
			   (!list_find_exp(rel->r, e) && 
			    !rel_find_exp(rel->l, e)))  
				append(aexps, e);
			else 
				append(nexps, e);
		}
		if (!list_length(avgs)) 
			return rel;

		/* For each avg, find count and sum */
		for (m = avgs->h; m; m = m->next) {
			list *args;
			sql_exp *avg = m->data, *navg, *cond, *cnt_d;
			sql_exp *cnt = rel_find_aggr_exp(sql->sa, rel, nexps, avg, "count");
			sql_exp *sum = rel_find_aggr_exp(sql->sa, rel, nexps, avg, "sum");
			sql_subfunc *div, *ifthen, *cmp;
			sql_subtype *dbl_t;
			char *rname = NULL, *name = NULL;

			rname = avg->rname;
			name = avg->name;
			if (!cnt) {
				list *l = avg->l;
				sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", exp_subtype(l->h->data));
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), cf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				exp_label(sql->sa, e, ++sql->label);
				append(nexps, e);
				cnt = exp_column(sql->sa, NULL, exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			}
			if (!sum) {
				list *l = avg->l;
				sql_subaggr *sf = sql_bind_aggr(sql->sa, sql->session->schema, "sum", exp_subtype(l->h->data));
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), sf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				exp_label(sql->sa, e, ++sql->label);
				append(nexps, e);
				sum = exp_column(sql->sa, NULL, exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			}
			/* create new sum/cnt exp */

			/* For now we always convert to dbl */
			dbl_t = sql_bind_localtype("dbl");
			cnt_d = exp_convert(sql->sa, cnt, exp_subtype(cnt), dbl_t);
			sum = exp_convert(sql->sa, sum, exp_subtype(sum), dbl_t);

			args = new_exp_list(sql->sa);
			append(args, cnt);
			append(args, exp_atom_wrd(sql->sa, 0));
			cmp = find_func(sql, "=", args);
			assert(cmp);
			cond = exp_op(sql->sa, args, cmp);

			args = new_exp_list(sql->sa);
			append(args, cond);
			append(args, exp_atom(sql->sa, atom_general(sql->sa, dbl_t, NULL)));
			append(args, cnt_d);
			ifthen = find_func(sql, "ifthenelse", args);
			assert(ifthen);
			cnt_d = exp_op(sql->sa, args, ifthen);

			args = new_exp_list(sql->sa);
			append(args, sum);
			append(args, cnt_d);
			div = find_func(sql, "sql_div", args);
			assert(div);
			navg = exp_op(sql->sa, args, div);

			exp_setname(sql->sa, navg, rname, name );
			m->data = navg; 
		}
		pexps = new_exp_list(sql->sa);
		for (m = rel->exps->h, n = avgs->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subaggr *a = e->f;
				
				if (strcmp(a->aggr->base.name, "avg") == 0) {
					sql_exp *avg = n->data;

					append(pexps, avg);
					n = n->next;
					continue; 
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column && !rel_find_exp(rel->l, e))  
				append(pexps, e);
			else 
				append(pexps, exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e)));
		}
		rel->exps = nexps;
		rel = rel_project(sql->sa, rel, pexps);
		set_processed(rel);
		(*changes)++;
	}
	return rel;
}

/* Compute the efficiency of using this expression early in a group by list */
static int
score_gbe( mvc *sql, sql_rel *rel, sql_exp *e)
{
	int res = 10;
	sql_subtype *t = exp_subtype(e);
	sql_column *c = NULL;

	/* can we find out if the underlying table is sorted */
	if ( (c = exp_find_column(rel, e, -2)) != NULL) {
		if (mvc_is_sorted (sql, c)) 
			res += 500;
	}

	/* is the column selective */

	/* prefer the shorter var types over the longer onces */
	if (!EC_FIXED(t->type->eclass) && t->digits)
		res -= t->digits;
	/* smallest type first */
	if (EC_FIXED(t->type->eclass))
		res -= t->type->eclass; 
	return res;
}

/* reorder group by expressions */
static sql_rel *
rel_groupby_order(int *changes, mvc *sql, sql_rel *rel) 
{
	list *gbe = rel->r;

	(void)*changes;
	if (is_groupby(rel->op) && list_length(gbe) > 1 && list_length(gbe)<9) {
		node *n;
		int i, *scores = calloc(list_length(gbe), sizeof(int));

		for (i = 0, n = gbe->h; n; i++, n = n->next) {
			scores[i] = score_gbe(sql, rel, n->data);
		}
		rel->r = list_keysort(gbe, scores, (fdup)NULL);

		free(scores);
	}
	return rel;
}


/* reduce group by expressions based on pkey info 
 *
 * The reduced group by and aggr expressions are restored via
 * a join with the base table (ie which is similar to late projection).
 */

static sql_rel *
rel_reduce_groupby_exps(int *changes, mvc *sql, sql_rel *rel) 
{
	list *gbe = rel->r;

	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel)) {
		node *n, *m;
		signed char *scores = malloc(list_length(gbe));
		int k, j, i;
		sql_column *c;
		sql_table **tbls;
		sql_rel **bts, *bt = NULL;

		gbe = rel->r;
		tbls = (sql_table**)malloc(sizeof(sql_table*)*list_length(gbe));
		bts = (sql_rel**)malloc(sizeof(sql_rel*)*list_length(gbe));
		for (k = 0, i = 0, n = gbe->h; n; n = n->next, k++) {
			sql_exp *e = n->data;

			c = exp_find_column_(rel, e, -2, &bt);
			if (c) {
				for(j = 0; j < i; j++)
					if (c->t == tbls[j] && bts[j] == bt)
						break;
				tbls[j] = c->t;
				bts[j] = bt;
				i += (j == i);
			}
		}
		if (i) { /* forall tables find pkey and 
				remove useless other columns */
			for(j = 0; j < i; j++) {
				int l, nr = 0, cnr = 0;

				k = list_length(gbe);
				memset(scores, 0, list_length(gbe));
				if (tbls[j]->pkey) {
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						fcmp cmp = (fcmp)&kc_column_cmp;
						sql_exp *e = n->data;

						c = exp_find_column_(rel, e, -2, &bt);
						if (c && c->t == tbls[j] && bts[j] == bt &&  
						    list_find(tbls[j]->pkey->k.columns, c, cmp) != NULL) {
							scores[l] = 1;
							nr ++;
						} else if (c && c->t == tbls[j] && bts[j] == bt) { 
							/* Okay we can cleanup a group by column */
							scores[l] = -1;
							cnr ++;
						}
					}
				}
				if (nr) { 
					int all = (list_length(tbls[j]->pkey->k.columns) == nr); 
					sql_kc *kc = tbls[j]->pkey->k.columns->h->data; 

					c = kc->c;
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						/* pkey based group by */
						if (scores[l] == 1 && ((all || 
						   /* first of key */
						   (c == exp_find_column(rel, e, -2))) && !find_prop(e->p, PROP_HASHIDX)))
							e->p = prop_create(sql->sa, PROP_HASHIDX, e->p);
					}
					for (m = rel->exps->h; m; m = m->next ){
						sql_exp *e = m->data;

						for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
							sql_exp *gb = n->data;

							/* pkey based group by */
							if (scores[l] == 1 && exp_match_exp(e,gb) && find_prop(gb->p, PROP_HASHIDX) && !find_prop(e->p, PROP_HASHIDX)) {
								e->p = prop_create(sql->sa, PROP_HASHIDX, e->p);
								break;
							}

						}
					}
				}
				if (cnr && nr && list_length(tbls[j]->pkey->k.columns) == nr) {
					char rname[16], *rnme = number2name(rname, 16, ++sql->label);
					sql_rel *r = rel_basetable(sql, tbls[j], rnme);
					list *ngbe = new_exp_list(sql->sa);
					list *exps = rel->exps, *nexps = new_exp_list(sql->sa);
					list *lpje = new_exp_list(sql->sa);

					r = rel_project(sql->sa, r, new_exp_list(sql->sa));
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						/* keep the group by columns which form a primary key
						 * of this table. And those unrelated to this table. */
						if (scores[l] != -1) 
							append(ngbe, e); 
						/* primary key's are used in the late projection */
						if (scores[l] == 1) {
							sql_column *c = exp_find_column_(rel, e, -2, &bt);
							
							sql_exp *rs = exp_column(sql->sa, rnme, c->base.name, exp_subtype(e), rel->card, has_nil(e), is_intern(e));
							append(r->exps, rs);

							e = exp_compare(sql->sa, e, rs, cmp_equal);
							append(lpje, e);

							e->p = prop_create(sql->sa, PROP_FETCH, e->p);
						}
					}
					rel->r = ngbe;
					/* remove gbe also from aggr list */
					for (m = exps->h; m; m = m->next ){
						sql_exp *e = m->data;
						int fnd = 0;

						for (l = 0, n = gbe->h; l < k && n && !fnd; l++, n = n->next) {
							sql_exp *gb = n->data;

							if (scores[l] == -1 && exp_match_exp(e,gb)) {
								sql_column *c = exp_find_column_(rel, e, -2, &bt);
								sql_exp *rs = exp_column(sql->sa, rnme, c->base.name, exp_subtype(e), rel->card, has_nil(e), is_intern(e));
								exp_setname(sql->sa, rs, exp_find_rel_name(e), exp_name(e));
								append(r->exps, rs);
								fnd = 1;
							}
						}
						if (!fnd)
							append(nexps, e);
					}
					/* new reduce aggr expression list */
					rel->exps = nexps;
					rel = rel_crossproduct(sql->sa, rel, r, op_join);
					rel->exps = lpje;
					/* only one reduction at a time */
					*changes = 1;
					free(bts);
					free(tbls);
					free(scores);
					return rel;
				} 
				gbe = rel->r;
			}
		}
		free(bts);
		free(tbls);
		free(scores);
	}
	return rel;
}

static sql_exp *split_aggr_and_project(mvc *sql, list *aexps, sql_exp *e);

static void
list_split_aggr_and_project(mvc *sql, list *aexps, list *exps)
{
	node *n;

	if (!exps)
		return ;
	for(n = exps->h; n; n = n->next) 
		n->data = split_aggr_and_project(sql, aexps, n->data);
}

static sql_exp *
split_aggr_and_project(mvc *sql, list *aexps, sql_exp *e)
{
	switch(e->type) {
	case e_aggr:
		/* add to the aggrs */
		list_append(aexps, e);
		if (!exp_name(e)) {
			exp_label(sql->sa, e, ++sql->label);
			e->rname = e->name;
		}
		return exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
	case e_cmp:
		/* e_cmp's should exist in an aggr expression list */
		assert(0);
	case e_convert:
		e->l = split_aggr_and_project(sql, aexps, e->l);
		return e;
	case e_func: 
		list_split_aggr_and_project(sql, aexps, e->l);
	case e_column: /* constants and columns shouldn't be rewriten */
	case e_atom:
		return e;
	}
	return NULL;
}

/* Pushing projects up the tree. Done very early in the optimizer.
 * Makes later steps easier. 
 */
static sql_rel *
rel_push_project_up(int *changes, mvc *sql, sql_rel *rel) 
{
	/* project/project cleanup is done later */
	if (is_join(rel->op) || is_select(rel->op)) {
		node *n;
		list *exps = NULL, *l_exps, *r_exps;
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		sql_rel *t;

		/* Don't rewrite refs, non projections or constant or 
		   order by projections  */
		if (!l || rel_is_ref(l) || 
		   (is_join(rel->op) && (!r || rel_is_ref(r))) ||
		   (is_select(rel->op) && l->op != op_project) ||
		   (is_join(rel->op) && l->op != op_project && r->op != op_project) ||
		  ((l->op == op_project && (!l->l || l->r || project_unsafe(l))) ||
		   (is_join(rel->op) && (is_subquery(r) ||
		    (r->op == op_project && (!r->l || r->r || project_unsafe(r))))))) 
			return rel;

		if (l->op == op_project && l->l) {
			/* Go through the list of project expressions.
			   Check if they can be pushed up, ie are they not
			   changing or introducing any columns used
			   by the upper operator. */

			exps = new_exp_list(sql->sa);
			for (n = l->exps->h; n; n = n->next) { 
				sql_exp *e = n->data;

				if (is_column(e->type) && exp_is_atom(e)) {
					list_append(exps, e);
				} else if (e->type == e_column /*||
					   e->type == e_func ||
					   e->type == e_convert*/) {
					if (e->name && e->name[0] == 'L')
						return rel;
					list_append(exps, e);
				} else {
					return rel;
				}
			}
		} else {
			exps = rel_projections(sql, l, NULL, 1, 1);
		}
		/* also handle right hand of join */
		if (is_join(rel->op) && r->op == op_project && r->l) {
			/* Here we also check all expressions of r like above
			   but also we need to check for ambigious names. */ 

			for (n = r->exps->h; n; n = n->next) { 
				sql_exp *e = n->data;

				if (is_column(e->type) && exp_is_atom(e)) {
					list_append(exps, e);
				} else if (e->type == e_column /*||
					   e->type == e_func ||
					   e->type == e_convert*/) {
					if (e->name && e->name[0] == 'L')
						return rel;
					list_append(exps, e);
				} else {
					return rel;
				}
			}
		} else if (is_join(rel->op)) {
			list *r_exps = rel_projections(sql, r, NULL, 1, 1);

			list_merge(exps, r_exps, (fdup)NULL);
		}
		/* Here we should check for ambigious names ? */
		if (is_join(rel->op) && r) {
			t = (l->op == op_project && l->l)?l->l:l;
			l_exps = rel_projections(sql, t, NULL, 1, 1);
			t = (r->op == op_project && r->l)?r->l:r;
			r_exps = rel_projections(sql, t, NULL, 1, 1);
			for(n = l_exps->h; n; n = n->next) {
				sql_exp *e = n->data;
	
				if (exp_is_atom(e))
					continue;
				if ((e->l && exps_bind_column2(r_exps, e->l, e->r) != NULL) || 
				   (exps_bind_column(r_exps, e->r, NULL) != NULL && (!e->l || !e->r)))
					return rel;
			}
			for(n = r_exps->h; n; n = n->next) {
				sql_exp *e = n->data;
	
				if (exp_is_atom(e))
					continue;
				if ((e->l && exps_bind_column2(l_exps, e->l, e->r) != NULL) || 
				   (exps_bind_column(l_exps, e->r, NULL) != NULL && (!e->l || !e->r)))
					return rel;
			}
		}

		/* rename operator expressions */
		if (l->op == op_project) {
			/* rewrite rel from rel->l into rel->l->l */
			if (rel->exps) {
				list *nexps = new_exp_list(sql->sa);

				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data;
	
					e = exp_rename(sql, e, l, l->l);
					assert(e);
					list_append(nexps, e);
				}
				rel->exps = nexps;
			}
			rel->l = l->l;
			l->l = NULL;
			rel_destroy(l);
		}
		if (is_join(rel->op) && r->op == op_project) {
			/* rewrite rel from rel->r into rel->r->l */
			if (rel->exps) {
				list *nexps = new_exp_list(sql->sa);

				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data;

					e = exp_rename(sql, e, r, r->l);
					assert(e);
					list_append(nexps, e);
				}
				rel->exps = nexps;
			}
			rel->r = r->l;
			r->l = NULL;
			rel_destroy(r);
		} 
		/* Done, ie introduce new project */
		exps_fix_card(exps, rel->card);
		(*changes)++;
		return rel_inplace_project(sql->sa, rel, NULL, exps);
	}
	if (is_groupby(rel->op) && !rel_is_ref(rel) && rel->exps) {
		node *n;
		int fnd = 0;
		list *aexps, *pexps;

		/* check if some are expressions aren't e_aggr */
		for (n = rel->exps->h; n && !fnd; n = n->next) {
			sql_exp *e = n->data;

			if (e->type != e_aggr && e->type != e_column)
				fnd = 1;
		}
		/* only aggr, no rewrite needed */
		if (!fnd) 
			return rel;

		aexps = list_new(sql->sa);
		pexps = list_new(sql->sa);
		for ( n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne = NULL;

			switch (e->type) {	
			case e_atom: /* move over to the projection */
				list_append(pexps, e);
				break;
			case e_func: 
			case e_convert: 
				list_append(pexps, e);
				list_split_aggr_and_project(sql, aexps, e->l);
				break;
			default: /* simple alias */
				list_append(aexps, e);
				ne = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
				list_append(pexps, ne);
				break;
			}
		}
		(*changes)++;
		rel->exps = aexps;
		return rel_inplace_project( sql->sa, rel, NULL, pexps);
	}
	return rel;
}

static int
exp_mark_used(sql_rel *subrel, sql_exp *e)
{
	sql_exp *ne = NULL;

	switch(e->type) {
	case e_column:
		ne = rel_find_exp(subrel, e);
		break;
	case e_convert:
		return exp_mark_used(subrel, e->l);
	case e_aggr:
	case e_func: {
		if (e->l) {
			list *l = e->l;
			node *n = l->h;
	
			for (;n != NULL; n = n->next) 
				exp_mark_used(subrel, n->data);
		}
		/* rank operators have a second list of arguments */
		if (e->r) {
			list *l = e->r;
			node *n = l->h;
	
			for (;n != NULL; n = n->next) 
				exp_mark_used(subrel, n->data);
		}
		break;
	}
	case e_cmp:
		if (e->flag == cmp_or) {
			list *l = e->l;
			node *n;
	
			for (n = l->h; n != NULL; n = n->next) 
				exp_mark_used(subrel, n->data);
			l = e->r;
			for (n = l->h; n != NULL; n = n->next) 
				exp_mark_used(subrel, n->data);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *r = e->r;
			node *n;

			exp_mark_used(subrel, e->l);
			for (n = r->h; n != NULL; n = n->next)
				exp_mark_used(subrel, n->data);
		} else {
			exp_mark_used(subrel, e->l);
			exp_mark_used(subrel, e->r);
			if (e->f)
				exp_mark_used(subrel, e->f);
		}
		break;
	case e_atom:
		/* atoms are used in e_cmp */
		e->used = 1;
		/* return 0 as constants may require a full column ! */
		return 0;
	}
	if (ne) {
		ne->used = 1;
		return ne->used;
	}
	return 0;
}

static void
positional_exps_mark_used( sql_rel *rel, sql_rel *subrel )
{
	if (!rel->exps) 
		assert(0);

	if (is_topn(subrel->op) && subrel->l)
		subrel = subrel->l;
	if (rel->exps && subrel->exps) {
		node *n, *m;
		for (n=rel->exps->h, m=subrel->exps->h; n && m; n = n->next, m = m->next) {
			sql_exp *e = n->data;
			sql_exp *se = m->data;

			if (e->used)
				se->used = 1;
		}
	}
}

static void
exps_mark_used(sql_rel *rel, sql_rel *subrel)
{
	int nr = 0;
	if (rel->exps) {
		node *n;
		int len = list_length(rel->exps), i;
		sql_exp **exps = (sql_exp**)malloc(sizeof(sql_exp*) * len);

		for (n=rel->exps->h, i = 0; n; n = n->next, i++) 
			exps[i] = n->data;

		for (i = len-1; i >= 0; i--) {
			sql_exp *e = exps[i];

			if (!is_project(rel->op) || e->used) {
				if (is_project(rel->op))
					nr += exp_mark_used(rel, e);
				nr += exp_mark_used(subrel, e);
			}
		}
		free(exps);
	}
	/* for count/rank we need atleast one column */
	if (!nr && (is_project(subrel->op) || is_base(subrel->op)) && subrel->exps->h) {
		sql_exp *e = subrel->exps->h->data;
		e->used = 1;
	}
	if (rel->r && (rel->op == op_project || rel->op  == op_groupby)) {
		list *l = rel->r;
		node *n;

		for (n=l->h; n; n = n->next) {
			sql_exp *e = n->data;

			exp_mark_used(rel, e);
			/* possibly project/groupby uses columns from the inner */ 
			exp_mark_used(subrel, e);
		}
	}
}

static void
exps_used(list *l)
{
	node *n;

	if (l) {
		for (n = l->h; n; n = n->next) {
			sql_exp *e = n->data;
	
			if (e)
				e->used = 1;
		}
	}
}

static void
rel_used(sql_rel *rel)
{
	if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op)) {
		if (rel->l) 
			rel_used(rel->l);
		if (rel->r) 
			rel_used(rel->r);
	} else if (is_topn(rel->op) || is_select(rel->op)) {
		rel_used(rel->l);
		rel = rel->l;
	}
	if (rel->exps) {
		exps_used(rel->exps);
		if (rel->r && (rel->op == op_project || rel->op  == op_groupby))
			exps_used(rel->r);
	}
}

static void
rel_mark_used(mvc *sql, sql_rel *rel, int proj)
{
	(void)sql;

	if (proj && (need_distinct(rel))) 
		rel_used(rel);

	switch(rel->op) {
	case op_basetable:
	case op_table:
		break;

	case op_topn: 
		if (proj) {
			rel = rel ->l;
			rel_mark_used(sql, rel, proj);
			break;
		}
	case op_project:
	case op_groupby: 
		if (proj && rel->l) {
			exps_mark_used(rel, rel->l);
			rel_mark_used(sql, rel->l, 0);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_ddl:
		break;

	case op_select: 
		if (rel->l) {
			exps_mark_used(rel, rel->l);
			rel_mark_used(sql, rel->l, 0);
		}
		break;

	case op_union: 
	case op_inter: 
	case op_except: 
		/* For now we mark all union expression as used */

		/* Later we should (in case of union all) remove unused
		 * columns from the projection.
		 * 
 		 * Project part of union is based on column position.
		 */
		if (proj && (need_distinct(rel) || !rel->exps)) {
			rel_used(rel);
			if (!rel->exps) {
				rel_used(rel->l);
				rel_used(rel->r);
			}
			rel_mark_used(sql, rel->l, 0);
			rel_mark_used(sql, rel->r, 0);
		} else if (proj && !need_distinct(rel)) {
			positional_exps_mark_used(rel, rel->l);
			positional_exps_mark_used(rel, rel->r);
			rel_mark_used(sql, rel->l, 1);
			rel_mark_used(sql, rel->r, 1);
			/* based on child check union expression list */
			positional_exps_mark_used(rel->l, rel);
		}
		break;

	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
		exps_mark_used(rel, rel->l);
		exps_mark_used(rel, rel->r);
		rel_mark_used(sql, rel->l, 0);
		rel_mark_used(sql, rel->r, 0);
		break;
	}
}

static sql_rel * rel_dce_sub(mvc *sql, sql_rel *rel);
static sql_rel * rel_dce(mvc *sql, sql_rel *rel);

static sql_rel *
rel_remove_unused(mvc *sql, sql_rel *rel) 
{
	if (!rel || rel_is_ref(rel))
		return rel;

	switch(rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (isMergeTable(t)) 
			return rel;
	}
	case op_table:
		if (rel->exps) {
			node *n;
			list *exps = new_exp_list(sql->sa);

			for(n=rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (e->used || is_intern(e))
					append(exps, e);
			}
			/* atleast one (needed for crossproducts, count(*), rank() and single value projections) !, handled by exps_mark_used */
			assert(list_length(exps) > 0);
			rel->exps = exps;
		}
		return rel;

	case op_topn: 

		if (rel->l)
			rel->l = rel_remove_unused(sql, rel->l);
		return rel;

	case op_project:
	case op_groupby: 

	case op_union: 
	case op_inter: 
	case op_except: 

		if (rel->l && rel->exps) {
			node *n;
			list *exps = new_exp_list(sql->sa);

			for(n=rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (e->used || is_intern(e))
					append(exps, e);
			}
			/* atleast one (needed for crossproducts, count(*), rank() and single value projections) !, handled by exps_mark_used */
			assert(list_length(exps) > 0);
			rel->exps = exps;
		}
		return rel;

	case op_insert:
	case op_update:
	case op_delete:

	case op_select: 

	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
	case op_ddl:
		return rel;
	}
	return rel;
}

static sql_rel *
rel_dce_down(mvc *sql, sql_rel *rel, int skip_proj) 
{
	if (!rel)
		return rel;

	if (!skip_proj && rel_is_ref(rel))
		return rel_dce(sql, rel);

	switch(rel->op) {
	case op_basetable:
	case op_table:

		if (!skip_proj)
			rel_dce_sub(sql, rel);

	case op_insert:
	case op_update:
	case op_delete:
	case op_ddl:

		return rel;

	case op_topn: 
	case op_project:
	case op_groupby: 

		if (skip_proj && rel->l)
			rel->l = rel_dce_down(sql, rel->l, is_topn(rel->op));
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		return rel;

	case op_union: 
	case op_inter: 
	case op_except: 
		if (skip_proj) {
			if (rel->l)
				rel->l = rel_dce_down(sql, rel->l, 0);
			if (rel->r)
				rel->r = rel_dce_down(sql, rel->r, 0);
		}
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		return rel;

	case op_select: 
		if (rel->l)
			rel->l = rel_dce_down(sql, rel->l, 0);
		return rel;

	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
		if (rel->l)
			rel->l = rel_dce_down(sql, rel->l, 0);
		if (rel->r)
			rel->r = rel_dce_down(sql, rel->r, 0);
		return rel;
	}
	return rel;
}

/* DCE
 *
 * Based on top relation expressions mark sub expressions as used.
 * Then recurse down until the projections. Clean them up and repeat.
 */

static sql_rel *
rel_dce_sub(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;
	
	/* 
  	 * Mark used up until the next project 
	 * For setops we need to first mark, then remove 
         * because of positional dependency 
 	 */
	rel_mark_used(sql, rel, 1);
	rel = rel_remove_unused(sql, rel);
	rel_dce_down(sql, rel, 1);
	return rel;
}

/* add projects under set ops */
static sql_rel *
rel_add_projects(mvc *sql, sql_rel *rel) 
{
	if (!rel)
		return rel;

	switch(rel->op) {
	case op_basetable:
	case op_table:

	case op_insert:
	case op_update:
	case op_delete:
	case op_ddl:

		return rel;

	case op_union: 
	case op_inter: 
	case op_except: 

		/* We can only reduce the list of expressions of an set op
		 * if the projection under it can also be reduced.
		 */
		if (rel->l) {
			sql_rel *l = rel->l;

			l->subquery = 0;
			if (!is_project(l->op) && !need_distinct(rel))
				l = rel_project(sql->sa, l, rel_projections(sql, l, NULL, 1, 1));
			rel->l = rel_add_projects(sql, l);
		}
		if (rel->r) {
			sql_rel *r = rel->r;

			r->subquery = 0;
			if (!is_project(r->op) && !need_distinct(rel))
				r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
			rel->r = rel_add_projects(sql, r);
		}
		return rel;

	case op_topn: 
	case op_project:
	case op_groupby: 
	case op_select: 
		if (rel->l)
			rel->l = rel_add_projects(sql, rel->l);
		return rel;

	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
		if (rel->l)
			rel->l = rel_add_projects(sql, rel->l);
		if (rel->r)
			rel->r = rel_add_projects(sql, rel->r);
		return rel;
	}
	return rel;
}

static sql_rel *
rel_dce(mvc *sql, sql_rel *rel)
{
	rel = rel_add_projects(sql, rel);
	rel_used(rel);
	rel_dce_sub(sql, rel);
	return rel;
}

static int
index_exp(sql_exp *e, sql_idx *i) 
{
	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		switch(i->type) {
		case hash_idx:
		case oph_idx:
			if (e->flag == cmp_equal)
				return 0;
		case join_idx:
		default:
			return -1;
		}
	}
	return -1;
}

static sql_column *
selectexp_col(sql_exp *e, sql_rel *r) 
{
	sql_table *t = r->l;

	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		sql_exp *ec = e->l;

		if (ec->type == e_column) {
			char *name = ec->name;
			node *cn;

			if (r->exps) { /* use alias */
				for (cn = r->exps->h; cn; cn = cn->next) {
					sql_exp *ce = cn->data;
					if (strcmp(ce->name, name) == 0) {
						name = ce->r;
						break;
					}
				}
			}
			for (cn = t->columns.set->h; cn; cn = cn->next) {
				sql_column *c = cn->data;
				if (strcmp(c->base.name, name) == 0) 
					return c;
			}
		}
	}
	return NULL;
}

static sql_idx *
find_index(sql_allocator *sa, sql_rel *r, list **EXPS)
{
	sql_rel *b;
	sql_table *t;

	if ((b = find_basetable(r)) == NULL) 
		return NULL;

	/* any (partial) match of the expressions with the index columns */
	/* Depending on the index type we may need full matches and only
	   limited number of cmp types (hash only equality etc) */
	/* Depending on the index type we should (in the rel_bin) generate
	   more code, ie for spatial index add post filter etc, for hash
	   compute hash value and use index */
 	t = b->l;
	if (t->idxs.set) {
		node *in;

		/* find the columns involved in the selection over this base table*/
	   	for(in = t->idxs.set->h; in; in = in->next) {
			list *exps, *cols;
	    		sql_idx *i = in->data;
			fcmp cmp = (fcmp)&sql_column_kc_cmp;

			/* join indices are only interesting for joins */
			if (i->type == join_idx || list_length(i->columns) <= 1)
				continue;
			/* based on the index type, find qualifying exps */
			exps = list_select(r->exps, i, (fcmp) &index_exp, (fdup)NULL);
			/* now we obtain the columns, move into sql_column_kc_cmp! */
			cols = list_map(exps, b, (fmap) &selectexp_col);

			/* Match the index columns with the expression columns. 
			   TODO, Allow partial matches ! */
			if (list_match(cols, i->columns, cmp) == 0) {
				/* re-order exps in index order */
				node *n, *m;
				list *es = list_new(sa);

				for(n = i->columns->h; n; n = n->next) {
					int i = 0;
					for(m = cols->h; m; m = m->next, i++) {
						if (cmp(m->data, n->data) == 0){
							sql_exp *e = list_fetch(exps, i);
							list_append(es, e);
							break;
						}
					}
				}
				/* fix the destroy function */
				cols->destroy = NULL;
				*EXPS = es;
				return i;
			}
			cols->destroy = NULL;
		}
	}
	return NULL;
}

static sql_rel *
rel_select_use_index(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)changes;
	(void)sql;
	if (is_select(rel->op)) {
		list *exps = NULL;
		sql_idx *i = find_index(sql->sa, rel, &exps);
			
		if (i) {
			prop *p;
			node *n;
	
			/* add PROP_HASHIDX to all column exps */
			for( n = exps->h; n; n = n->next) { 
				sql_exp *e = n->data;

				p = find_prop(e->p, PROP_HASHIDX);
				if (!p)
					e->p = p = prop_create(sql->sa, PROP_HASHIDX, e->p);
				p->value = i;
			}
			/* add the remaining exps to the new exp list */
			if (list_length(rel->exps) > list_length(exps)) {
				for( n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data;
					if (!list_find(exps, e, (fcmp)&exp_cmp))
						list_append(exps, e);
				}
			}
			rel->exps = exps;
		}
	}
	return rel;
}

/* TODO CSE */
#if 0
static list *
exp_merge(list *exps)
{
	node *n, *m;
	for (n=exps->h; n && n->next; n = n->next) {
		sql_exp *e = n->data;
		/*sql_exp *le = e->l;*/
		sql_exp *re = e->r;

		if (e->type == e_cmp && e->flag == cmp_or)
			continue;

		/* only look for gt, gte, lte, lt */
		if (re->card == CARD_ATOM && e->flag < cmp_equal) {
			for (m=n->next; m; m = m->next) {
				sql_exp *f = m->data;
				/*sql_exp *lf = f->l;*/
				sql_exp *rf = f->r;

				if (rf->card == CARD_ATOM && f->flag < cmp_equal) {
					printf("possible candidate\n");
				}
			}
		}
	}
	return exps;
}
#endif

static sql_rel *
rel_select_order(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)changes;
	(void)sql;
	if (is_select(rel->op) && rel->exps && list_length(rel->exps)>1) {
		list *exps = NULL;
			
		exps = list_sort(rel->exps, (fkeyvalue)&exp_keyvalue, (fdup)NULL);
		/*rel->exps = exp_merge(exps);*/
		rel->exps = exps;
	}
	return rel;
}

static sql_rel *
rel_simplify_like_select(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)sql;
	if (is_select(rel->op) && rel->exps) {
		node *n;
		list *exps = list_new(sql->sa);
			
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_like) {
				sql_exp *fmt = e->r;
				sql_exp *esc = e->f;
				int rewrite = 0;

				if (fmt->type == e_convert)
					fmt = fmt->l;
				/* check for simple like expression */
				if (is_atom(fmt->type)) {
					atom *fa = NULL;

					if (fmt->l) {
						fa = fmt->l;
					/* simple numbered argument */
					} else if (!fmt->r && !fmt->f) {
						fa = sql->args[fmt->flag];

					}
					if (fa && fa->data.vtype == TYPE_str && 
					    !strchr(fa->data.val.sval, '%') &&
					    !strchr(fa->data.val.sval, '_'))
						rewrite = 1;
				}
				if (rewrite && esc && is_atom(esc->type)) {
			 		atom *ea = NULL;

					if (esc->l) {
						ea = esc->l;
					/* simple numbered argument */
					} else if (!esc->r && !esc->f) {
						ea = sql->args[esc->flag];

					}
					if (ea && (ea->data.vtype != TYPE_str ||
					    strlen(ea->data.val.sval) != 0))
						rewrite = 0;
				}
				if (rewrite) { 	/* rewrite to cmp_equal ! */
					sql_exp *ne = exp_compare(sql->sa, e->l, e->r, cmp_equal);
					/* if rewriten don't cache this query */
					list_append(exps, ne);
					sql->caching = 0;
					(*changes)++;
				} else {
					list_append(exps, e);
				}
			} else {
				list_append(exps, e);
			}
		}
		rel->exps = exps;
	}
	return rel;
}

static list *
exp_merge_range(sql_allocator *sa, list *exps)
{
	node *n, *m;
	for (n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_exp *le = e->l;
		sql_exp *re = e->r;

		/* handle the and's in the or lists */
		if (e->type == e_cmp && e->flag == cmp_or) {
			e->l = exp_merge_range(sa, e->l);
			e->r = exp_merge_range(sa, e->r);
		/* only look for gt, gte, lte, lt */
		} else if (n->next &&
		    e->type == e_cmp && e->flag < cmp_equal && !e->f && 
		    re->card == CARD_ATOM) {
			for (m=n->next; m; m = m->next) {
				sql_exp *f = m->data;
				sql_exp *lf = f->l;
				sql_exp *rf = f->r;

				if (f->type == e_cmp && f->flag < cmp_equal && !f->f &&
				    rf->card == CARD_ATOM && 
				    exp_match_exp(le, lf)) {
					sql_exp *ne;
					int swap = 0, lt = 0, gt = 0;
					/* for now only   c1 <[=] x <[=] c2 */ 

					swap = lt = (e->flag == cmp_lt || e->flag == cmp_lte);
					gt = !lt;

					if (gt && 
					   (f->flag == cmp_gt ||
					    f->flag == cmp_gte)) 
						continue;
					if (lt && 
					   (f->flag == cmp_lt ||
					    f->flag == cmp_lte)) 
						continue;
					if (!swap) 
						ne = exp_compare2(sa, le, re, rf, compare2range(e->flag, f->flag));
					else
						ne = exp_compare2(sa, le, rf, re, compare2range(f->flag, e->flag));

					list_remove_data(exps, e);
					list_remove_data(exps, f);
					list_append(exps, ne);
					return exp_merge_range(sa, exps);
				}
			}
		} else if (n->next &&
			   e->type == e_cmp && e->flag < cmp_equal && !e->f && 
		    	   re->card > CARD_ATOM) {
			for (m=n->next; m; m = m->next) {
				sql_exp *f = m->data;
				sql_exp *lf = f->l;
				sql_exp *rf = f->r;

				if (f->type == e_cmp && f->flag < cmp_equal && !f->f  &&
				    rf->card > CARD_ATOM) {
					sql_exp *ne;
					int swap = 0, lt = 0, gt = 0;
					comp_type ef = (comp_type) e->flag, ff = (comp_type) f->flag;
				
					/* is left swapped ? */
				     	if (exp_match_exp(re, lf)) {
						sql_exp *t = re; 

						re = le;
						le = t;
						ef = swap_compare(ef);
					}
					/* is right swapped ? */
				     	if (exp_match_exp(le, rf)) {
						sql_exp *t = rf; 

						rf = lf;
						lf = t;
						ff = swap_compare(ff);
					}

				    	if (!exp_match_exp(le, lf))
						continue;

					/* for now only   c1 <[=] x <[=] c2 */ 
					swap = lt = (ef == cmp_lt || ef == cmp_lte);
					gt = !lt;

					if (gt && (ff == cmp_gt || ff == cmp_gte)) 
						continue;
					if (lt && (ff == cmp_lt || ff == cmp_lte)) 
						continue;
					if (!swap) 
						ne = exp_compare2(sa, le, re, rf, compare2range(ef, ff));
					else
						ne = exp_compare2(sa, le, rf, re, compare2range(ff, ef));

					list_remove_data(exps, e);
					list_remove_data(exps, f);
					list_append(exps, ne);
					return exp_merge_range(sa, exps);
				}
			}
		}
	}
	return exps;
}

static sql_rel *
rel_find_range(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)changes;
	if ((is_join(rel->op) || is_select(rel->op)) && rel->exps && list_length(rel->exps)>1) 
		rel->exps = exp_merge_range(sql->sa, rel->exps);
	return rel;
}

/* 
 * Casting decimal values on both sides of a compare expression is expensive,
 * both in preformance (cpu cost) and memory requirements (need for large 
 * types). 
 */

static int
reduce_scale(atom *a)
{
	if (a->data.vtype == TYPE_lng) {
		lng v = a->data.val.lval;
		int i = 0;

		while( (v/10)*10 == v ) {
			i++;
			v /= 10;
		}
		a->data.val.lval = v;
		return i;
	}
	if (a->data.vtype == TYPE_int) {
		int v = a->data.val.ival;
		int i = 0;

		while( (v/10)*10 == v ) {
			i++;
			v /= 10;
		}
		a->data.val.lval = v;
		return i;
	}
	if (a->data.vtype == TYPE_sht) {
		sht v = a->data.val.shval;
		int i = 0;

		while( (v/10)*10 == v ) {
			i++;
			v /= 10;
		}
		a->data.val.lval = v;
		return i;
	}
	return 0;
}

static sql_rel *
rel_project_reduce_casts(int *changes, mvc *sql, sql_rel *rel) 
{
	if (is_project(rel->op) && list_length(rel->exps)) {
		list *exps = rel->exps;
		node *n;

		for (n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e && e->type == e_func) {
				sql_subfunc *f = e->f;

				if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && f->res.scale > 0) {
					list *args = e->l;
					sql_exp *h = args->h->data;
					sql_exp *t = args->t->data;
					atom *a;

					if ((is_atom(h->type) && (a = exp_value(h, sql->args, sql->argc)) != NULL) ||
					    (is_atom(t->type) && (a = exp_value(t, sql->args, sql->argc)) != NULL)) {
						int rs = reduce_scale(a);

						f->res.scale -= rs; 
						if (rs)
							(*changes)+= rs;
					}
				}
			}
		}
	}
	return rel;
}

static sql_rel *
rel_reduce_casts(int *changes, mvc *sql, sql_rel *rel) 
{
	*changes = 0; 

	(void)sql;
	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op)) && 
			rel->exps && list_length(rel->exps)) {
		list *exps = rel->exps;
		node *n;

		for (n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_exp *le = e->l;
			sql_exp *re = e->r;
	
			/* handle the and's in the or lists */
			if (e->type != e_cmp || 
			   (e->flag != cmp_lt && e->flag != cmp_gt)) 
				continue;
			/* rewrite e if left or right is cast */
			if (le->type == e_convert || re->type == e_convert) {
				sql_rel *l = rel->l, *r = rel->r;

				/* if convert on left then find
				 * mul or div on right which increased
				 * scale!
				 *
				 * TODO handle select case
				 */
				(void)l;
				if (le->type == e_convert && re->type == e_column && r && is_project(r->op)) {
					sql_exp *nre = rel_find_exp(r, re);
					sql_subtype *tt = exp_totype(le);
					sql_subtype *ft = exp_fromtype(le);

					if (nre && nre->type == e_func) {
						sql_subfunc *f = nre->f;

						if (!f->func->s && !strcmp(f->func->base.name, "sql_mul")) {
							list *args = nre->l;
							sql_exp *ce = args->t->data;
							sql_subtype *fst = exp_subtype(args->h->data);
							atom *a;

							if (fst->scale == ft->scale &&
							   (a = exp_value(ce, sql->args, sql->argc)) != NULL) {
								lng v = 1;
								/* multiply with smallest value, then scale and (round) */
								int scale = tt->scale - ft->scale;
								int rs = reduce_scale(a);

								scale -= rs;

								args = new_exp_list(sql->sa);
								while(scale > 0) {
									scale--;
									v *= 10;
								}
								append(args, re);
								append(args, exp_atom_lng(sql->sa, v));
								f = find_func(sql, "scale_down", args);
								nre = exp_op(sql->sa, args, f);
								e = exp_compare(sql->sa, le->l, nre, e->flag);
							}
						}
					}
				}
			}
			n->data = e;	
		}
	}
	return rel;
}

static int
is_identity( sql_exp *e, sql_rel *r)
{
	switch(e->type) {
	case e_column:
		if (r && is_project(r->op)) {
			sql_exp *re = NULL;
			if (e->l)
				re = exps_bind_column2(r->exps, e->l, e->r);
			if (!re && ((char*)e->r)[0] == 'L')
				re = exps_bind_column(r->exps, e->r, NULL);
			if (re)
				return is_identity(re, r->l);
		}
		return 0;
	case e_func: {
		sql_subfunc *f = e->f;
		return (strcmp(f->func->base.name, "identity") == 0);
	}
	default:
		return 0;
	}
}

static int
is_identity_of(sql_exp *e, sql_rel *l) 
{
	if (e->type != e_cmp)
		return 0;
	if (!is_identity(e->l, l) || !is_identity(e->r, l))
		return 0;
	return 1;
}

/* rewrite {semi,anti}join (A, join(A,B)) into {semi,anti}join (A,B) */
/* TODO: handle A, join(B,A) as well */

/* More general case is (join reduction)
 *
   	{semi,anti}join (A, join(A,B) [A.c1 == B.c1]) [ A.c1 == B.c1 ]
	into {semi,anti}join (A,B) [ A.c1 == B.c1 ] 
*/
	
static sql_rel *
rel_rewrite_semijoin(int *changes, mvc *sql, sql_rel *rel)
{
	(void)sql;
	if (is_semi(rel->op)) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		sql_rel *rl = (r->l)?r->l:NULL;

		if (l->ref.refcnt == 2 && 
		   ((is_join(r->op) && l == r->l) || 
		    (is_project(r->op) && rl && is_join(rl->op) && l == rl->l))){
			sql_rel *or = r;

			if (!rel->exps || list_length(rel->exps) != 1 ||
			    !is_identity_of(rel->exps->h->data, l)) 
				return rel;
			
			if (is_project(r->op)) 
				r = rl;

			rel->r = rel_dup(r->r);
			/* maybe rename exps ? */
			rel->exps = r->exps;
			r->exps = NULL;
			rel_destroy(or);
			(*changes)++;
		}
	}
	if (is_semi(rel->op)) {
		sql_rel *l = rel->l, *rl;
		sql_rel *r = rel->r, *or = r;

		if (r)
			rl = r->l;
		if (r && is_project(r->op)) {
			r = rl;
			if (r)
				rl = r->l;
		}

		if (l && r && rl && 
		    is_basetable(l->op) && is_basetable(rl->op) &&
		    is_join(r->op) && l->l == rl->l)
		{
			node *n, *m;

			if (!rel->exps || !r->exps ||
		       	    list_length(rel->exps) != list_length(r->exps)) 
				return rel;
			list *exps = new_exp_list(sql->sa);

			/* are the join conditions equal */
			for (n = rel->exps->h, m = r->exps->h;
			     n && m; n = n->next, m = m->next)
			{
				sql_exp *le = NULL, *oe = n->data;
				sql_exp *re = NULL, *ne = m->data;
				sql_column *cl;  
				
				if (oe->type != e_cmp || ne->type != e_cmp ||
				    oe->flag != cmp_equal || 
				    ne->flag != cmp_equal)
					return rel;

				if ((cl = exp_find_column(rel->l, oe->l, -2)) != NULL)
					le = oe->l;
				else if ((cl = exp_find_column(rel->l, oe->r, -2)) != NULL)
					le = oe->r;

				if (exp_find_column(rl, ne->l, -2) == cl)
					re = oe->r;
				else if (exp_find_column(rl, ne->r, -2) == cl)
					re = oe->l;
				if (!re)
					return rel;
				ne = exp_compare(sql->sa, le, re, cmp_equal);
				append(exps, ne);
			}

			rel->r = rel_dup(r->r);
			/* maybe rename exps ? */
			rel->exps = exps;
			rel_destroy(or);
			(*changes)++;
		}
	}
	return rel;
}

/* antijoin(a, union(b,c)) -> antijoin(antijoin(a,b), c) */
static sql_rel *
rel_rewrite_antijoin(int *changes, mvc *sql, sql_rel *rel)
{
	if (rel->op == op_anti) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;

		if (l && !rel_is_ref(l) && 
		    r && !rel_is_ref(r) && is_union(r->op)) {
			sql_rel *rl = rel_dup(r->l), *nl;
			sql_rel *rr = rel_dup(r->r);

			if (!is_project(rl->op)) 
				rl = rel_project(sql->sa, rl, 
					rel_projections(sql, rl, NULL, 1, 1));
			if (!is_project(rr->op)) 
				rr = rel_project(sql->sa, rr,
					rel_projections(sql, rr, NULL, 1, 1));
			rel_rename_exps(sql, r->exps, rl->exps);
			rel_rename_exps(sql, r->exps, rr->exps);

			nl = rel_crossproduct(sql->sa, rel->l, rl, op_anti);
			nl->exps = exps_copy(sql->sa, rel->exps);
			rel->l = nl;
			rel->r = rr;
			rel_destroy(r);
			(*changes)++;
			return rel;
		}
	}
	return rel;
}

static sql_rel *
rel_semijoin_use_fk(int *changes, mvc *sql, sql_rel *rel)
{
	(void)changes;
	if (is_semi(rel->op) && rel->exps) {
		list *exps = rel->exps;
		list *rels = rels = list_create(NULL);

		rel->exps = NULL;
		append(rels, rel->l);
		append(rels, rel->r);
		(void) find_fk( sql->sa, rels, exps);

		rel->exps = exps;
	}
	return rel;
}

/* rewrite sqltype into backend types */
static sql_rel *
rel_rewrite_types(int *changes, mvc *sql, sql_rel *rel)
{
	(void)sql;
	(void)changes;
	return rel;
}

/* rewrite merge tables into union of base tables and call optimizer again */
static sql_rel *
rel_merge_table_rewrite(int *changes, mvc *sql, sql_rel *rel)
{
	if (is_basetable(rel->op)) {
		sql_table *t = rel->l;

		if (isMergeTable(t)) {
			/* instantiate merge tabel */
			sql_rel *nrel = NULL;
			char *tname = exp_find_rel_name(rel->exps->h->data);

			node *n;

			if (list_empty(t->tables.set)) 
				return rel;
			assert(!rel_is_ref(rel));
			(*changes)++;
			if (t->tables.set) {
				for (n = t->tables.set->h; n; n = n->next) {
					sql_table *pt = n->data;
					sql_rel *prel = _rel_basetable(sql->sa, pt, tname);
					node *n, *m;

					/* rename (mostly the idxs) */
					for (n = rel->exps->h, m = prel->exps->h; n && m; n = n->next, m = m->next ) {
						sql_exp *e = n->data;
						sql_exp *ne = m->data;

						exp_setname(sql->sa, ne, e->rname, e->name);
					}
					if (nrel) { 
						nrel = rel_setop(sql->sa, nrel, prel, op_union);
						nrel->exps = rel_projections(sql, rel, NULL, 1, 1);
					} else {
						nrel = prel;
					}
				}
			}
			if (nrel)
				nrel->exps = rel->exps;
			rel_destroy(rel);
			return nrel;
		}
	}
	return rel;
}

static sql_rel *
rewrite(mvc *sql, sql_rel *rel, rewrite_fptr rewriter, int *has_changes) 
{
	int changes = 0;

	if (!rel)
		return rel;

	switch (rel->op) {
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
		rel->l = rewrite(sql, rel->l, rewriter, has_changes);
		rel->r = rewrite(sql, rel->r, rewriter, has_changes);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
		rel->l = rewrite(sql, rel->l, rewriter, has_changes);
		break;
	case op_ddl: 
		rel->l = rewrite(sql, rel->l, rewriter, has_changes);
		if (rel->r)
			rel->r = rewrite(sql, rel->r, rewriter, has_changes);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rewrite(sql, rel->r, rewriter, has_changes);
		break;
	}
	//if (rel_is_ref(rel)) {
		/*
		int refs = rel->ref.refcnt;
		sql_rel *r = rewriter(&changes, sql, rel);

		if (changes && r != rel) {
			*rel = *r;
			rel->ref.refcnt = refs;
		}
		*/
		//return rel;
	//}
	rel = rewriter(&changes, sql, rel);
	if (changes) {
		(*has_changes)++;
		return rewrite(sql, rel, rewriter, has_changes);
	}
	return rel;
}

static sql_rel *
rewrite_topdown(mvc *sql, sql_rel *rel, rewrite_fptr rewriter, int *has_changes) 
{
	if (!rel)
		return rel;

	//if (!rel_is_ref(rel))
	rel = rewriter(has_changes, sql, rel);
	switch (rel->op) {
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
		rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
		rel->r = rewrite_topdown(sql, rel->r, rewriter, has_changes);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
		rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
		break;
	case op_ddl: 
		rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
		if (rel->r)
			rel->r = rewrite_topdown(sql, rel->r, rewriter, has_changes);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rewrite_topdown(sql, rel->r, rewriter, has_changes);
		break;
	}
	return rel;
}

static sql_rel *
_rel_optimizer(mvc *sql, sql_rel *rel, int level) 
{
	int changes = 0, e_changes = 0;
	global_props gp; 

	memset(&gp, 0, sizeof(global_props));
	rel_properties(sql, &gp, rel);

#ifdef DEBUG
{
	int i;
	for (i = 0; i < MAXOPS; i++) {
		if (gp.cnt[i]> 0)
			printf("%s %d\n", op2string((operator_type)i), gp.cnt[i]);
	}
}
#endif
	/* simple merging of projects */
	if (gp.cnt[op_project]) {
		rel = rewrite(sql, rel, &rel_merge_projects, &changes);
		rel = rewrite(sql, rel, &rel_case_fixup, &changes);
	}

	if (gp.cnt[op_join] || 
	    gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full] || 
	    gp.cnt[op_semi] || gp.cnt[op_anti] ||
	    gp.cnt[op_select]) {
		rel = rewrite(sql, rel, &rel_find_range, &changes);
		rel = rel_project_reduce_casts(&changes, sql, rel);
		rel = rewrite(sql, rel, &rel_reduce_casts, &changes);
	}

	if (gp.cnt[op_union]) {
		rel = rewrite(sql, rel, &rel_merge_union, &changes); 
		rel = rewrite(sql, rel, &rel_select_cse, &changes); 
	}

	/* Remove unused expressions */
	rel = rel_dce(sql, rel);
	if (gp.cnt[op_project]) 
		rel = rewrite(sql, rel, &rel_project_cse, &changes);

	/* push (simple renaming) projections up */
	if (gp.cnt[op_project])
		rel = rewrite(sql, rel, &rel_push_project_up, &changes); 

	rel = rewrite(sql, rel, &rel_rewrite_types, &changes); 

	if (gp.cnt[op_anti] || gp.cnt[op_semi]) {
		/* rewrite semijoin (A, join(A,B)) into semijoin (A,B) */
		rel = rewrite(sql, rel, &rel_rewrite_semijoin, &changes);
		/* push semijoin through join */
		//rel = rewrite(sql, rel, &rel_push_semijoin_down, &changes);
		/* antijoin(a, union(b,c)) -> antijoin(antijoin(a,b), c) */
		rel = rewrite(sql, rel, &rel_rewrite_antijoin, &changes);
		if (level <= 0)
			rel = rewrite_topdown(sql, rel, &rel_semijoin_use_fk, &changes);
	}

	if (gp.cnt[op_select]) {
		rel = rewrite_topdown(sql, rel, &rel_push_select_down, &changes); 
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
		/* only once */
		if (level <= 0)
			rel = rewrite(sql, rel, &rel_merge_rse, &changes); 
	}

	if (gp.cnt[op_select] && gp.cnt[op_join]) {
		rel = rewrite_topdown(sql, rel, &rel_push_select_down_join, &changes); 
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	if (gp.cnt[op_join] && gp.cnt[op_groupby]) {
		rel = rewrite_topdown(sql, rel, &rel_push_count_down, &changes);
		if (level <= 0)
			rel = rewrite_topdown(sql, rel, &rel_push_join_down, &changes); 

		/* push_join_down introduces semijoins */
		/* rewrite semijoin (A, join(A,B)) into semijoin (A,B) */
		rel = rewrite(sql, rel, &rel_rewrite_semijoin, &changes);
	}

	if (gp.cnt[op_select])
		rel = rewrite(sql, rel, &rel_push_select_down_union, &changes); 

	if (gp.cnt[op_groupby]) {
		rel = rewrite_topdown(sql, rel, &rel_avg2sum_count, &changes); 
		rel = rewrite_topdown(sql, rel, &rel_push_aggr_down, &changes);
		rel = rewrite(sql, rel, &rel_groupby_order, &changes); 
		rel = rewrite(sql, rel, &rel_reduce_groupby_exps, &changes); 
	}

	if (gp.cnt[op_join] || gp.cnt[op_left] || 
	    gp.cnt[op_semi] || gp.cnt[op_anti]) {
		rel = rel_remove_empty_join(sql, rel, &changes);
		if (!gp.cnt[op_update])
			rel = rewrite(sql, rel, &rel_join_order, &changes); 
		rel = rewrite(sql, rel, &rel_push_join_down_union, &changes); 
		/* rel_join_order may introduce empty selects */
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	if (gp.cnt[op_select] && !sql->emode == m_prepare) 
		rel = rewrite(sql, rel, &rel_simplify_like_select, &changes); 

	if (gp.cnt[op_select]) 
		rel = rewrite(sql, rel, &rel_select_order, &changes); 

	if (gp.cnt[op_select])
		rel = rewrite(sql, rel, &rel_select_use_index, &changes); 

	if (gp.cnt[op_project])
		rel = rewrite(sql, rel, &rel_push_project_down_union, &changes);

	if (!changes && gp.cnt[op_topn]) {
		rel = rewrite_topdown(sql, rel, &rel_push_topn_down, &changes); 
		changes = 0;
	}

	rel = rewrite(sql, rel, &rel_merge_table_rewrite, &changes);

	if (changes && level > 10)
		assert(0);

	if (changes)
		return _rel_optimizer(sql, rel, ++level);

	/* optimize */
	return rel;
}

sql_rel *
rel_optimizer(mvc *sql, sql_rel *rel) 
{
	return _rel_optimizer(sql, rel, 0);
}
