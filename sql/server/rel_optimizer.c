/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
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
name_find_column( sql_rel *rel, char *rname, char *name ) 
{
	sql_exp *alias = NULL;
	sql_column *c = NULL;

	switch (rel->op) {
	case op_basetable: {
		node *cn;
		sql_table *t = rel->l;

		if (rname && strcmp(t->base.name, rname) != 0)
			return NULL;
		if (rel->exps) {
			sql_exp *rename = exps_bind_column(rel->exps, name, NULL);
			if (!rename ||
			     rename->type != e_column ||
			    (rename->l && strcmp(t->base.name, rename->l) != 0))
				return NULL;
			name = rename->r;
		}
		for (cn = t->columns.set->h; cn; cn = cn->next) {
			sql_column *c = cn->data;
			if (strcmp(c->base.name, name) == 0) {
				return c;
			}
		}
	}
	case op_table:
		if (rel->exps)
			(void) exps_bind_column(rel->exps, name, NULL);
		/* table func */
		return NULL;
	case op_ddl: {
		node *cn;
		sql_table *t = rel_ddl_table_get( rel );

		if (t && (!rname || strcmp(t->base.name, rname) == 0))
		for (cn = t->columns.set->h; cn; cn = cn->next) {
			sql_column *c = cn->data;
			if (strcmp(c->base.name, name) == 0) {
				return c;
			}
		}
		return name_find_column( rel->l, rname, name);
	} 
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
		/* first right (possible subquery) */
		c = name_find_column( rel->r, rname, name);
		if (!c) 
			c = name_find_column( rel->l, rname, name);
		return c;
	case op_select: 
	case op_topn: 
		return name_find_column( rel->l, rname, name);
	case op_union: 
	case op_inter: 
	case op_except: 
	case op_project:
	case op_groupby: 
		if (!rel->exps)
			break;
		alias = exps_bind_column(rel->exps, name, NULL);
		if (!alias && rel->l) {
			/* group by column not found as alias in projection list;
			   fall back to check plain input columns */
			return name_find_column( rel->l, rname, name);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		break;
	}
	if (alias) { /* we found an expression with the correct name, but
			we need sql_columns */
		if (alias->type == e_column) /* real alias */
			return name_find_column( rel->l, alias->l, alias->r );
	}
	return NULL;
}

static sql_column *
exp_find_column( sql_rel *rel, sql_exp *exp )
{
	(void)rel;
	if (exp->type == e_column) { 
		return name_find_column(rel, exp->l, exp->r);
	}
	return NULL;
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
				sql_column *lc = exp_find_column(rel, e->l); 
				sql_column *rc = exp_find_column(rel, e->r); 

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
		/* TODO real copy */
		return e;
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
		/* TODO real copy */
		return e;
	}
	if (ne && e->p)
		ne->p = prop_copy(sa, e->p);
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
		case cmp_notin: 
			*cnt += 9;
			return 9;
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

	assert (h->type == e_cmp && key->type == e_cmp);
	assert (h_l && h_r && key_l && key_r);
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

static int
kc_column_cmp(sql_kc *kc, sql_column *c)
{
	/* return on equality */
	return !(c == kc->c);
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

list *
order_join_expressions(sql_allocator *sa, list *dje, list *rels)
{
	list *res = list_new(sa);
	node *n = NULL;
	int i, j, *keys, *pos, cnt = list_length(dje);

	keys = (int*)alloca(cnt*sizeof(int));
	pos = (int*)alloca(cnt*sizeof(int));
	for (n = dje->h, i = 0; n; n = n->next, i++) {
		sql_exp *e = n->data;

		keys[i] = exp_keyvalue(e);
		/* add some weigth for the selections */
		if (e->type == e_cmp) {
			sql_rel *l = find_rel(rels, e->l);
			sql_rel *r = find_rel(rels, e->r);

			if (l && is_select(l->op))
				keys[i] += list_length(l->exps)*10;
			if (r && is_select(r->op))
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
	*changes = 0; 
	if (is_join(rel->op) && rel->exps) {
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
		sql_exp *arg = n->data;

		arg = _exp_push_down(sql, arg, f, t);
		if (!arg) 
			return NULL;
		append(nl, arg);
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
			list *l;
			list *r;

			l = exps_push_down(sql, e->l, f, t);
			r = exps_push_down(sql, e->r, f, t);
			if (!l || !r) 
				return NULL;
			return exp_or(sql->sa, l, r);
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
	/* projects around ddl's cannot be changed */
	if (sub && sub->op == op_ddl)
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

	if (is_groupby(rel->op) &&
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

static sql_rel *
rel_push_topn_down(int *changes, mvc *sql, sql_rel *rel) 
{
	sql_rel *rl, *r = rel->l;

	if (rel->op == op_topn && topn_save_exps(rel->exps)) {
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
		if (r && r != rel && is_project(r->op) && !(rel_is_ref(r)) && !r->r && r->l) {
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
	}
	return rel;
}

static sql_rel *
rel_merge_projects(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = rel->exps;
	sql_rel *r = rel->l;
	node *n;

	if (rel->op == op_project && 
	    r && r->op == op_project && !(rel_is_ref(r))) {
		int all = 1;

		if (project_unsafe(rel) || project_unsafe(r))
			return rel;

		/* here we need to fix aliases */
		rel->exps = new_exp_list(sql->sa); 
		/* for each exp check if we can rename it */
		for (n = exps->h; n && all; n = n->next) { 
			sql_exp *e = n->data, *ne = NULL;
			sql_rel *prj = r;

			if (e->type != e_column) {
				all = 0;
			} else {	
				ne = exp_push_down(sql, e, prj, prj->l);
				/* can we move it down */
				if (ne && ne != e) {
					char *tname = e->rname;
					/* we need to keep the alias */
					if (!tname && ne->rname) {
						tname = ne->rname;
						ne->rname = NULL;  /* prevent from being freed prematurely in exp_setname */
					}
					exp_setname(sql->sa, ne, tname, e->name);
					list_append(rel->exps, ne);
				} else {
					all = 0;
				}
			}
		}
		if (all) {
			/* we can now remove the intermediate project */
			/* push order by expressions */
			if (rel->r) {
				list *nr = new_exp_list(sql->sa), *res = rel->r; 
				for (n = res->h; n; n = n->next) { 
					sql_exp *e = n->data, *ne = NULL;
					sql_rel *prj = r;
	
					ne = exp_push_down(sql, e, prj, prj->l);
					/* can we move it down */
					if (ne && ne != e) {
						char *tname = e->rname;
						/* we need to keep the alias */
						if (!tname && ne->rname) {
							tname = ne->rname;
							ne->rname = NULL;  /* prevent from being freed prematurely in exp_setname */
						}
						exp_setname(sql->sa, ne, tname, e->name);
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
			rel->l = r->l;
			r->l = NULL;
			rel_destroy(r);
			(*changes)++;
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
			sql_subfunc *ifthen;

			/* TODO we should find the div recursively ! */

			/* rewrite right hands of div */
			if (a1->type == e_func && !a1f->func->s && 
			     !strcmp(a1f->func->base.name, "sql_div")) {
				list *args = a1->l;
				sql_exp *le = args->h->data, *o;
				sql_exp *re = args->h->next->data;

				/* if (cond) then val else const */
				args = new_exp_list(sql->sa);
				append(args, cond);
				append(args, re);
				o = exp_atom_wrd(sql->sa, 1);
				append(args, exp_convert(sql->sa, o, exp_subtype(o), exp_subtype(re)));
				ifthen = find_func(sql, "ifthenelse", args);
				assert(ifthen);
				re = exp_op(sql->sa, args, ifthen);

				a1 = exp_binop(sql->sa, le, re, a1->f);
			}
			if  (a2->type == e_func && !a2f->func->s && 
			     !strcmp(a2f->func->base.name, "sql_div")) { 
				list *args = a2->l;
				sql_exp *le = args->h->data, *o;
				sql_exp *re = args->h->next->data;

				/* if (cond) then const else val */
				args = new_exp_list(sql->sa);
				append(args, cond);
				o = exp_atom_wrd(sql->sa, 1);
				append(args, exp_convert(sql->sa, o, exp_subtype(o), exp_subtype(re)));
				append(args, re);
				ifthen = find_func(sql, "ifthenelse", args);
				assert(ifthen);
				re = exp_op(sql->sa, args, ifthen);

				a2 = exp_binop(sql->sa, le, re, a2->f);
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

	if (rel->op == op_union && 
	    l && is_project(l->op) && !project_unsafe(l) &&
	    r && is_project(r->op) && !project_unsafe(r) &&	
	    (ref = rel_find_ref(l)) != NULL && ref == rel_find_ref(r)) {
		/* Find selects and try to merge */
		sql_rel *ls = rel_find_select(l), *nls;
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
		*changes = 1;
		nls = rel_project(sql->sa, rel->l, rel->exps);
		set_processed(nls);
		rel->l = NULL;
		rel->exps = NULL;
		ls->exps = append(new_exp_list(sql->sa), exp_or(sql->sa, ls->exps, rs->exps));
		rs->exps = NULL;
		rel_destroy(rel);
		return nls;
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

	lu = alloca(list_length(l));
	ru = alloca(list_length(r));
	memset(lu, 0, list_length(l));
	memset(ru, 0, list_length(r));
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
	return res;
}


static sql_rel *
rel_select_cse(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)sql;
	if (is_select(rel->op) && !rel_is_ref(rel) && rel->exps) { 
		list *nexps = new_exp_list(sql->sa);
		node *n;

		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or) {
				/* split the common expressions */
				*changes = exps_cse(sql->sa, nexps, e->l, e->r);
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
	if (is_project(rel->op) && !rel_is_ref(rel) && rel->exps) { 
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
exps_merge_rse( sql_allocator *sa, list *l, list *r )
{
	node *n, *m, *o;
	list *nexps = NULL, *lexps, *rexps;

 	lexps = new_exp_list(sa);
	for (n = l->h; n; n = n->next) {
		sql_exp *e = n->data;
	
		if (e->type == e_cmp && e->flag == cmp_or) {
			list *nexps = exps_merge_rse(sa, e->l, e->r);
			if (nexps) {
				for (o = nexps->h; o; o = o->next) 
					append(lexps, o->data);
				nexps = NULL;
			}
		}
	}
 	rexps = new_exp_list(sa);
	for (n = r->h; n; n = n->next) {
		sql_exp *e = n->data;
	
		if (e->type == e_cmp && e->flag == cmp_or) {
			list *nexps = exps_merge_rse(sa, e->l, e->r);
			if (nexps) {
				for (o = nexps->h; o; o = o->next) 
					append(rexps, o->data);
				nexps = NULL;
			}
		}
	}

	/* merge merged lists first ? */
 	nexps = new_exp_list(sa);

	for (n = lexps->h; n; n = n->next) {
		sql_exp *le = n->data, *re, *fnd = NULL;

		if (le->type != e_cmp || !is_complex_exp(le->flag))
			continue;
		for (m = rexps->h; m; m = m->next) {
			re = m->data;
			if (exps_match_col_exps(le, re))
				fnd = re;
		}
		for (m = r->h; !fnd && m; m = m->next) {
			re = m->data;
			if (exps_match_col_exps(le, re))
				fnd = re;
		}
		if (fnd) {
			re = fnd;
			fnd = exp_or(sa, append(new_exp_list(sa),le), 
		       		         append(new_exp_list(sa),re));
			append(nexps, fnd);
		}
	}
	for (n = l->h; n; n = n->next) {
		sql_exp *le = n->data, *re, *fnd = NULL;

		if (le->type != e_cmp /*|| 
		   (le->flag != cmp_or && le->flag != cmp_equal)*/)
			continue;

		for (m = lexps->h; !fnd && m; m = m->next) {
			re = m->data;
			if (exps_match_col_exps(le, re))
				fnd = re;
		}
		if (fnd)
			continue;
		for (m = rexps->h; m; m = m->next) {
			re = m->data;
			if (exps_match_col_exps(le, re))
				fnd = re;
		}
		for (m = r->h; !fnd && m; m = m->next) {
			re = m->data;
			if (exps_match_col_exps(le, re))
				fnd = re;
		}
		if (fnd) {
			re = fnd;
			fnd = exp_or(sa, append(new_exp_list(sa),le), 
		       		         append(new_exp_list(sa),re));
			append(nexps, fnd);
		}
	}
	return nexps;
}


/* merge related sub expressions */
static sql_rel *
rel_merge_rse(int *changes, mvc *sql, sql_rel *rel) 
{
	/* only execute once per select, ie don't return changes */

	(void)sql;
	*changes = 0;
	if (is_select(rel->op) && !rel_is_ref(rel) && rel->exps) { 
		node *n, *o;
		list *nexps = NULL;

		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or) {
				/* possibly merge related expressions */
				list *lexps = exps_merge_rse(sql->sa, e->l, e->r);
				if (lexps) {
					if (!nexps) {
						nexps = lexps;
					} else {
						for (o = lexps->h; o; o = o->next) 
							append(nexps, o->data);
						lexps = NULL;
					}
/*
				} else {
					if (!nexps) 
						nexps = new_exp_list(sql->sa);
					append(nexps, e);
*/
				}
			}
		}
		if (nexps) {
			for (o = nexps->h; o; o = o->next) 
				append(rel->exps, o->data);
			nexps = NULL;
		}
/*
		if (nexps) 
			rel->exps = nexps;
*/
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
	sql_rel *u = rel->l;

	/* TODO disjoint partitions don't need the last group by */
	if (rel->op == op_groupby && 
	    u && is_union(u->op) && !need_distinct(u) && u->exps) {
		sql_rel *g = rel;
		sql_rel *l = u->l;
		sql_rel *r = u->r;
		list *lexps, *rexps;

		if (!is_project(l->op)) 
			u->l = l = rel_project(sql->sa, l, rel_projections(sql, l, NULL, 1, 0));
		if (!is_project(r->op)) 
			u->r = r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 0));
	       	lexps = l->exps;
		rexps = r->exps;
		/* make sure we don't create group by on group by's */
		if (l->op != op_groupby && r->op != op_groupby) {
			node *n, *m;
			list *lgbe = NULL, *rgbe = NULL;

			/* distinct should be done over the full result */
			for (n = g->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_subaggr *af = e->f;

				/* TODO: constants should stay on top groupby */
				if (e->type == e_atom || 
				   (e->type == e_aggr && 
				   ((strcmp(af->aggr->base.name, "sum") && 
				     strcmp(af->aggr->base.name, "count") &&
				     strcmp(af->aggr->base.name, "min") &&
				     strcmp(af->aggr->base.name, "max")) ||
				   need_distinct(e))))
					return rel; 
			}

			if (g->r && list_length(g->r) > 0) {
				list *gbe = g->r;
				lgbe = new_exp_list(sql->sa);
				rgbe = new_exp_list(sql->sa);

				for (n = gbe->h; n; n = n->next) {
					sql_exp *e = n->data, *ne;

				  	ne = exp_copy(sql->sa, e);
					append(lgbe, ne);
					exp_setname(sql->sa, ne, e->rname?e->rname:exp_find_rel_name(e), exp_name(e));
				  	ne = exp_copy(sql->sa, e);
					append(rgbe, ne);
					exp_setname(sql->sa, ne, e->rname?e->rname:exp_find_rel_name(e), exp_name(e));
				}
			}
			l = rel_groupby(sql->sa, rel_dup(l), NULL);
			r = rel_groupby(sql->sa, rel_dup(r), NULL);
			l->r = lgbe;
			l->nrcols = g->nrcols;
			l->card = g->card;
			r->r = rgbe;
			r->nrcols = g->nrcols;
			r->card = g->card;
			for (n = g->exps->h; n; n = n->next) {
				sql_exp *e = n->data, *nle, *nre, *ne;
				list *ol, *nll = NULL, *nlr = NULL; 
				node *m;

				/* recreate aggr expression, find base expressions on lower relation */
				if (e->type == e_aggr) {
				  ol = e->l;
				  if (ol) {
				    nll = new_exp_list(sql->sa); 
				    nlr = new_exp_list(sql->sa);
				    for(m = ol->h; m; m = m->next){
					sql_exp *oe = m->data;

				  	ne = exp_push_down(sql, oe, u, l->l );
					assert(ne);
					append(nll, ne);
				  	ne = exp_push_down(sql, oe, u, r->l );
					assert(ne);
					append(nlr, ne);
				    }
				  }
				  nle = exp_aggr(sql->sa, nll, e->f, need_distinct(e), need_no_nil(e), e->card, 0);
				  nre = exp_aggr(sql->sa, nlr, e->f, need_distinct(e), need_no_nil(e), e->card, 0);
				} else {
				  nle = exp_copy(sql->sa, e);
				  nre = exp_copy(sql->sa, e);
				}
				rel_groupby_add_aggr(sql, l, nle);
				exp_setname(sql->sa, nle, e->rname?e->rname:exp_find_rel_name(e), exp_name(e));
				rel_groupby_add_aggr(sql, r, nre);
				exp_setname(sql->sa, nre, e->rname?e->rname:exp_find_rel_name(e), exp_name(e));
			}
			u = rel_setop(sql->sa, l, r, op_union);
			u->exps = rel_projections(sql, r, NULL, 0, 1);
			g = rel_groupby(sql->sa, u, NULL);
			if (rel->r) {
				list *gbe = new_exp_list(sql->sa), *ogbe = rel->r;
				
				for (n = ogbe->h; n; n = n->next) {
					sql_exp *e = n->data, *ne;

					ne = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
					exp_setname(sql->sa, ne, e->rname?e->rname:exp_find_rel_name(e), exp_name(e));
					append(gbe, ne);
				}
				g->r = gbe;
			}
			g->nrcols = rel->nrcols;
			g->card = rel->card;
			for (n = u->exps->h, m = rel->exps->h; n && m; n = n->next, m = m->next) {
				sql_exp *ne, *e = n->data, *oa = m->data;
				if (oa->type == e_aggr) {
					sql_subaggr *f = oa->f;
					int cnt = strcmp(f->aggr->base.name,"count")==0;
					sql_subaggr *a = sql_bind_aggr(sql->sa, sql->session->schema, (cnt)?"sum":f->aggr->base.name, exp_subtype(e));

					assert(a);
					/* union of aggr result may have nils 
				   	because sum/count of empty set */
					set_has_nil(e);
					e = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
					ne = exp_aggr1(sql->sa, e, a, need_distinct(e), 1, e->card, 1);
				} else {
					ne = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
				}
				exp_setname(sql->sa, ne, oa->rname?oa->rname:exp_find_rel_name(oa), exp_name(oa));
				rel_groupby_add_aggr(sql, g, ne);
			}
			rel_destroy(rel);
			(*changes)++;
			return g;
		}
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

	(void)sql;
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
				*changes += 1;
			} else {
				append(rel->exps, e);
			}
		}
		return rel;
	}

	/* push select through set */
	if (is_select(rel->op) && r && is_set(r->op) && !(rel_is_ref(r))) {
		sql_rel *res = r;
		sql_rel *sl = r->l;
		sql_rel *sr = r->r;

		/* introduce selects under the set (if needed) */
		if (!is_select(sl->op))
			r->l = sl = rel_select(sql->sa, sl, NULL);
		if (!is_select(sr->op))
			r->r = sr = rel_select(sql->sa, sr, NULL);
		
		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data, *ne = NULL;
			if (e->type == e_cmp) {
				int err = 0;
				ne = exp_push_down(sql, e, r, sl);
				if (ne && ne != e) {
					rel_select_add_exp(sl, ne);
				} else 
					err = 1;
				ne = exp_push_down(sql, e, r, sr);
				if (ne && ne != e) {
					rel_select_add_exp(sr, ne);
				} else 
					err = 1;

				if (err) {
					list_append(rel->exps, e);
					res = rel;
				} else
					(*changes)++;
			} else {
				list_append(rel->exps, e);
				res = rel;
			}
		}
		return res;
	}

	if (is_select(rel->op) && r && r->op == op_project && !(rel_is_ref(r))) {
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
				ne = exp_push_down(sql, e, r, pl);

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
 * Push joins down, pushes the joins through group by expressions. 
 * When the join is on the group by columns, we can push the joins left
 * under the group by.
 */
static sql_rel *
rel_push_join_down(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = NULL;
	sql_rel *gb = NULL;

	*changes = 0;

	if (((is_join(rel->op) && rel->exps) || is_semi(rel->op)) && rel->l) {
		gb = rel->r;
		exps = rel->exps;
		if (gb->op == op_groupby && gb->r && list_length(gb->r)) { 
			list *jes = new_exp_list(sql->sa);
			node *n, *m;
			list *gbes = gb->r;
			/* find out if all group by expressions are 
			   used in the join */
			for(n = gbes->h; n; n = n->next) {
				sql_exp *gbe = n->data;
				int fnd = 0;
				char *rname = gbe->rname;
				for (m = exps->h; m && !fnd; m = m->next) {
					sql_exp *je = m->data;

					if (je->card >= CARD_ATOM && je->type == e_cmp && !is_complex_exp(je->flag)) {
						/* expect right expression to match */
						sql_exp *r = je->r;
						if (r == 0 || r->type != e_column)
							continue;
						if (r->l && rname && strcmp(r->l, rname) == 0 && strcmp(r->r,gbe->name)==0) {
							fnd = 1;
						} else if (!r->l && !rname  && strcmp(r->r,gbe->name)==0) {
							fnd = 1;
						}
						if (fnd) {
							sql_exp *re = exp_push_down(sql, r, gb, gb->l);
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
			if (is_join(rel->op)) {
				sql_rel *l = rel_dup(rel->l);

				/* push join's left side (as semijoin) 
					down group by */
				/* now we need to translate the names using the join expressions */
				l = gb->l = rel_crossproduct(sql->sa, gb->l, l, op_semi);
				l->exps = jes;
			} else { /* semi join */
				/* rewrite group by into project */
				gb->op = op_project;
				gb->r = NULL;
			}
			return rel;
		} 
	}
	return rel;
}

/*
 * Push (semi)joins down unions, this is basically for merge tables, where
 * we know that the fk-indices are split over two clustered merge tables.
 *
 * TODO: There are possibly projections around the unions !
 */
static sql_rel *
rel_push_join_down_union(int *changes, mvc *sql, sql_rel *rel) 
{
	*changes = 0;

	if ((is_join(rel->op) || is_semi(rel->op)) && rel->exps) {
		sql_rel *l = rel->l, *r = rel->r, *ol = l, *or = r;
		list *exps = rel->exps;
		sql_exp *je = exps->h->data;

		if (!find_prop(je->p, PROP_JOINIDX))
			return rel;
		if (l->op == op_project)
			l = l->l;
		if (r->op == op_project)
			r = r->l;
		if (l->op == op_union && r->op != op_union) {
			sql_rel *nl, *nr, *u;
			sql_rel *ll = rel_dup(l->l), *lr = rel_dup(l->r);

			/* join(union(a,b), c) -> union(join(a,c), join(b,c)) */
			if (l != ol) {
				ll = rel_project(sql->sa, ll, NULL);
				ll->exps = rel_projections(sql, ol, NULL, 1, 1);
				lr = rel_project(sql->sa, lr, NULL);
				lr->exps = rel_projections(sql, ol, NULL, 1, 1);
			}	
			nl = rel_crossproduct(sql->sa, ll, rel_dup(or), rel->op);
			nr = rel_crossproduct(sql->sa, lr, rel_dup(or), rel->op);
			nl->exps = exps_copy(sql->sa, exps);
			nr->exps = exps_copy(sql->sa, exps);

			u = rel_setop(sql->sa, nl, nr, op_union);
			u->exps = rel_projections(sql, rel, NULL, 1, 1);
			rel_destroy(rel);
			*changes = 1;
			return u;
		} else if (l->op == op_union && r->op == op_union) {
			sql_rel *nl, *nr, *u;
			sql_rel *ll = rel_dup(l->l), *lr = rel_dup(l->r);
			sql_rel *rl = rel_dup(r->l), *rr = rel_dup(r->r);

			/* check if fk is split over both sides */

			/* join(union(a,b), union(c,d)) -> union(join(a,c), join(b,d)) */
			if (l != ol) {
				ll = rel_project(sql->sa, ll, NULL);
				ll->exps = rel_projections(sql, ol, NULL, 1, 1);
				lr = rel_project(sql->sa, lr, NULL);
				lr->exps = rel_projections(sql, ol, NULL, 1, 1);
			}	
			if (r != or) {
				rl = rel_project(sql->sa, rl, NULL);
				rl->exps = rel_projections(sql, or, NULL, 1, 1);
				rr = rel_project(sql->sa, rr, NULL);
				rr->exps = rel_projections(sql, or, NULL, 1, 1);
			}	
			nl = rel_crossproduct(sql->sa, ll, rl, rel->op);
			nr = rel_crossproduct(sql->sa, lr, rr, rel->op);
			nl->exps = exps_copy(sql->sa, exps);
			nr->exps = exps_copy(sql->sa, exps);

			u = rel_setop(sql->sa, nl, nr, op_union);
			u->exps = rel_projections(sql, rel, NULL, 1, 1);
			rel_destroy(rel);
			*changes = 1;
			return u;
		}
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
	*changes = 0;
	if (is_groupby(rel->op)) {
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
			if (e->type == e_column && !rel_find_exp(rel->l, e))  
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
				append(pexps, exp_column(sql->sa, e->rname?e->rname:exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e)));
		}
		rel->exps = nexps;
		rel = rel_project(sql->sa, rel, pexps);
		set_processed(rel);
		*changes = 1;
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
	if ( (c = exp_find_column(rel, e)) != NULL) {
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

	*changes = 0;
	if (is_groupby(rel->op) && list_length(gbe) > 1 && list_length(gbe)<9) {
		node *n;
		int i, *scores = alloca(sizeof(int) * list_length(gbe));

		memset(scores, 0, sizeof(int)*list_length(gbe));
		for (i = 0, n = gbe->h; n; i++, n = n->next) {
			scores[i] = score_gbe(sql, rel, n->data);
		}
		rel->r = list_keysort(gbe, scores, (fdup)NULL);
	}
	return rel;
}

/* reduce group by expressions based on pkey info */
static sql_rel *
rel_reduce_groupby_exps(int *changes, mvc *sql, sql_rel *rel) 
{
	list *gbe = rel->r;

	*changes = 0;
	(void)sql;
	if (is_groupby(rel->op) && list_length(gbe) > 1 && list_length(gbe)<9) {
		node *n;
		char *scores = alloca(list_length(gbe));
		int k, j, i;
		sql_column *c;
		sql_table **tbls;

		gbe = rel->r;
		tbls = (sql_table**)alloca(sizeof(sql_table*)*list_length(gbe));
		for (k = 0, i = 0, n = gbe->h; n; n = n->next, k++) {
			sql_exp *e = n->data;

			c = exp_find_column(rel, e);
			if (c) {
				for(j = 0; j < i; j++)
					if (c->t == tbls[j])
						break;
				tbls[j] = c->t;
				i += (j == i);
			}
		}
		if (i) { /* forall tables find pkey and 
				remove useless other columns */
			for(j = 0; j < i; j++) {
				int l, nr = 0;

				k = list_length(gbe);
				memset(scores, 0, list_length(gbe));
				if (tbls[j]->pkey) {
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						fcmp cmp = (fcmp)&kc_column_cmp;
						sql_exp *e = n->data;

						c = exp_find_column(rel, e);
						if (c && list_find(tbls[j]->pkey->k.columns, c, cmp) != NULL) {
							scores[l] = 1;
							nr ++;
						}
					}
				}
				if (nr && list_length(tbls[j]->pkey->k.columns) == nr) {
					list *ngbe = new_exp_list(sql->sa);
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						if (scores[l])
							append(ngbe, e); 
					}
					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						c = exp_find_column(rel, e);
						if (!c || c->t != tbls[j]) 
							append(ngbe, e); 
					}
					rel->r = ngbe;
				} 
				gbe = rel->r;
			}
		}
	}
	return rel;
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
		rel = rel_project(sql->sa, rel, exps);
		set_processed(rel);
		*changes = 1;
		return rel;
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
		sql_exp **exps = (sql_exp**)alloca(sizeof(sql_exp*) * len);

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
			rel_mark_used(sql, rel->l, 0);
			rel_mark_used(sql, rel->r, 0);
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

			if (!is_project(l->op) && !need_distinct(rel))
				l = rel_project(sql->sa, l, rel_projections(sql, l, NULL, 1, 1));
			rel->l = rel_add_projects(sql, l);
		}
		if (rel->r) {
			sql_rel *r = rel->r;

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
	(void)sql;
	*changes = 0; 
	if (is_select(rel->op)) {
		list *exps = NULL;
		sql_idx *i = find_index(sql->sa, rel, &exps);
			
		if (i) {
			prop *p;
			node *n;
	
			/* add PROP_HASHIDX to all column exps */
			for( n = exps->h; n; n = n->next) { 
				sql_exp *e = n->data;

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
	(void)sql;
	*changes = 0; 
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
	*changes = 0; 
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
					*changes = 1; 
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
	*changes = 0; 
	if ((is_join(rel->op) || is_select(rel->op)) && rel->exps && list_length(rel->exps)>1) 
		rel->exps = exp_merge_range(sql->sa, rel->exps);
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
/* TODO handle A, join(B,A) as well */
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
	char *tname = rel_name(rel);
	if (is_basetable(rel->op)) {
		sql_table *t = rel->l;

		if (isMergeTable(t)) {
			/* instantiate merge tabel */
			sql_rel *nrel = NULL;

			node *n;

			if (list_empty(t->tables.set)) 
				return rel;
			*changes = 1;
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
	if (rel_is_ref(rel))
		return rel;
	rel = rewriter(&changes, sql, rel);
	if (changes) {
		*has_changes = 1;
		return rewrite(sql, rel, rewriter, has_changes);
	}
	return rel;
}

static sql_rel *
rewrite_topdown(mvc *sql, sql_rel *rel, rewrite_fptr rewriter, int *has_changes) 
{
	if (!rel)
		return rel;

	if (!rel_is_ref(rel))
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

	/* TODO
		expensive (functions) in select and/or join expressions should
		be push-up into new projections after (ie executed when the
		tables are reduced allready).

		select bla = true/false should be rewriten into select bla.
		and not(bla).
		(were bla is an compare expression)
	*/
	/* make parts conditional (via debug flag) */

	/* simple merging of projects */
	if (gp.cnt[op_project]) {
		rel = rewrite(sql, rel, &rel_merge_projects, &changes);
		rel = rewrite(sql, rel, &rel_case_fixup, &changes);
	}

	if (gp.cnt[op_join] || 
	    gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full] || 
	    gp.cnt[op_select]) 
		rel = rewrite(sql, rel, &rel_find_range, &changes);

	if (gp.cnt[op_union]) {
		rel = rewrite(sql, rel, &rel_merge_union, &changes); 
		rel = rewrite(sql, rel, &rel_select_cse, &changes); 
		rel = rewrite(sql, rel, &rel_merge_rse, &changes); 
	}

	/* Remove unused expressions */
	rel = rel_dce(sql, rel);
	if (gp.cnt[op_project]) 
		rel = rewrite(sql, rel, &rel_project_cse, &changes);

	/* push (simple renaming) projections up */
	if (gp.cnt[op_project])
		rel = rewrite(sql, rel, &rel_push_project_up, &changes); 

	rel = rewrite(sql, rel, &rel_rewrite_types, &changes); 

	/* rewrite {semi,anti}join (A, join(A,B)) into {semi,anti}join (A,B) */
	if (gp.cnt[op_anti] || gp.cnt[op_semi]) {
		rel = rewrite(sql, rel, &rel_rewrite_semijoin, &changes);
		rel = rewrite_topdown(sql, rel, &rel_semijoin_use_fk, &changes);
	}

	if (gp.cnt[op_select]) 
		rel = rewrite_topdown(sql, rel, &rel_push_select_down, &changes); 

	if (gp.cnt[op_select]) 
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 

	if (gp.cnt[op_select] && gp.cnt[op_join]) {
		rel = rewrite_topdown(sql, rel, &rel_push_select_down_join, &changes); 
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	if (gp.cnt[op_topn])
		rel = rewrite_topdown(sql, rel, &rel_push_topn_down, &changes); 

	/* TODO push select up. Sounds bad, but isn't. In case of an join-idx we want the selection on
	   the 'unique/primary (right hand side)' done before the (fake)-join and the selections on the foreign 
	   part done after. */
	
	if (gp.cnt[op_join] && gp.cnt[op_groupby]) {
		rel = rewrite_topdown(sql, rel, &rel_push_count_down, &changes);
		rel = rewrite_topdown(sql, rel, &rel_push_join_down, &changes); 
	}

	if (gp.cnt[op_groupby]) {
		rel = rewrite_topdown(sql, rel, &rel_avg2sum_count, &changes); 
		rel = rewrite_topdown(sql, rel, &rel_push_aggr_down, &changes);
		rel = rewrite(sql, rel, &rel_groupby_order, &changes); 
		rel = rewrite(sql, rel, &rel_reduce_groupby_exps, &changes); 
	}

	if (gp.cnt[op_join] || gp.cnt[op_left] || 
	    gp.cnt[op_semi] || gp.cnt[op_anti]) {
		if (!gp.cnt[op_update])
			rel = rewrite(sql, rel, &rel_join_order, &changes); 
		rel = rewrite(sql, rel, &rel_push_join_down_union, &changes); 
		/* sometime rel_join_order introduces empty selects */
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	if (gp.cnt[op_select] && !sql->emode == m_prepare) 
		rel = rewrite(sql, rel, &rel_simplify_like_select, &changes); 

	if (gp.cnt[op_select]) 
		rel = rewrite(sql, rel, &rel_select_order, &changes); 

	if (gp.cnt[op_select])
		rel = rewrite(sql, rel, &rel_select_use_index, &changes); 

	rel = rewrite(sql, rel, &rel_merge_table_rewrite, &changes);
	if (changes && level <= 1)
		return _rel_optimizer(sql, rel, ++level);

	/* optimize */
	return rel;
}

sql_rel *
rel_optimizer(mvc *sql, sql_rel *rel) 
{
	return _rel_optimizer(sql, rel, 0);
}
