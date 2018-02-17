/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*#define DEBUG*/

#include "monetdb_config.h"
#include "rel_optimizer.h"
#include "rel_rel.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_planner.h"
#include "sql_mvc.h"
#ifdef HAVE_HGE
#include "mal.h"		/* for have_hge */
#endif
#include "mtime.h"

#define new_func_list(sa) sa_list(sa)
#define new_col_list(sa) sa_list(sa)

typedef struct global_props {
	int cnt[MAXOPS];
} global_props;

typedef sql_rel *(*rewrite_fptr)(int *changes, mvc *sql, sql_rel *rel);
typedef sql_rel *(*rewrite_rel_fptr)(mvc *sql, sql_rel *rel, rewrite_fptr rewriter, int *has_changes);
typedef int (*find_prop_fptr)(mvc *sql, sql_rel *rel);

static sql_rel * rewrite_topdown(mvc *sql, sql_rel *rel, rewrite_fptr rewriter, int *has_changes);
static sql_rel * rewrite(mvc *sql, sql_rel *rel, rewrite_fptr rewriter, int *has_changes) ;
static sql_rel * rel_remove_empty_select(int *changes, mvc *sql, sql_rel *rel);

static sql_subfunc *find_func( mvc *sql, char *name, list *exps );

/* The important task of the relational optimizer is to optimize the
   join order. 

   The current implementation chooses the join order based on 
   select counts, ie if one of the join sides has been reduced using
   a select this join is choosen over one without such selections. 
 */

/* currently we only find simple column expressions */
void *
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
		if (name && !t)
			return rel->r;
		if (rname && strcmp(t->base.name, rname) != 0)
			return NULL;
		for (cn = t->columns.set->h; cn; cn = cn->next) {
			sql_column *c = cn->data;
			if (strcmp(c->base.name, name) == 0) {
				*bt = rel;
				if (pnr < 0 || (c->t->p &&
				    list_position(c->t->p->members.set, c->t) == pnr))
					return c;
			}
		}
		if (t->idxs.set)
		for (cn = t->idxs.set->h; cn; cn = cn->next) {
			sql_idx *i = cn->data;
			if (strcmp(i->base.name, name+1 /* skip % */) == 0) {
				*bt = rel;
				if (pnr < 0 || (i->t->p &&
				    list_position(i->t->p->members.set, i->t) == pnr)) {
					sql_kc *c = i->columns->h->data;
					return c->c;
				}
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
	case op_apply: 
	case op_semi: 
	case op_anti: 
		/* first right (possible subquery) */
		c = name_find_column( rel->r, rname, name, pnr, bt);
		if (!c) 
			c = name_find_column( rel->l, rname, name, pnr, bt);
		return c;
	case op_select:
	case op_topn:
	case op_sample:
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
		if (is_groupby(rel->op) && !alias && rel->l) {
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

/* find column for the select/join expression */
static sql_column *
sjexp_col(sql_exp *e, sql_rel *r) 
{
	sql_column *res = NULL;

	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		res = exp_find_column(r, e->l, -2);
		if (!res)
			res = exp_find_column(r, e->r, -2);
	}
	return res;
}

static sql_exp *
list_find_exp( list *exps, sql_exp *e)
{
	sql_exp *ne = NULL;

	if (e->type != e_column)
		return NULL;
	if (( e->l && (ne=exps_bind_column2(exps, e->l, e->r)) != NULL) ||
	   ((!e->l && (ne=exps_bind_column(exps, e->r, NULL)) != NULL)))
		return ne;
	return NULL;
}

static int
kc_column_cmp(sql_kc *kc, sql_column *c)
{
	/* return on equality */
	return !(c == kc->c);
}

static void psm_exps_properties(mvc *sql, global_props *gp, list *exps);
static void rel_properties(mvc *sql, global_props *gp, sql_rel *rel);

static void
psm_exp_properties(mvc *sql, global_props *gp, sql_exp *e)
{
	/* only functions need fix up */
	if (e->type == e_psm) {
		if (e->flag & PSM_SET) {
			psm_exp_properties(sql, gp, e->l);
		} else if (e->flag & PSM_RETURN) {
			psm_exp_properties(sql, gp, e->l);
		} else if (e->flag & PSM_WHILE) {
			psm_exp_properties(sql, gp, e->l);
			psm_exps_properties(sql, gp, e->r);
		} else if (e->flag & PSM_IF) {
			psm_exp_properties(sql, gp, e->l);
			psm_exps_properties(sql, gp, e->r);
			if (e->f)
				psm_exps_properties(sql, gp, e->f);
		} else if (e->flag & PSM_REL) {
			rel_properties(sql, gp, e->l);
		}
	}
}

static void
psm_exps_properties(mvc *sql, global_props *gp, list *exps)
{
	node *n;

	if (!exps)
		return;
	for (n = exps->h; n; n = n->next) 
		psm_exp_properties(sql, gp, n->data);
}

static void
rel_properties(mvc *sql, global_props *gp, sql_rel *rel) 
{
	gp->cnt[(int)rel->op]++;
	switch (rel->op) {
	case op_basetable:
	case op_table:
		if (rel->op == op_table && rel->l && rel->flag != 2) 
			rel_properties(sql, gp, rel->l);
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
		rel_properties(sql, gp, rel->l);
		rel_properties(sql, gp, rel->r);
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
	case op_ddl:
		if (rel->op == op_ddl && rel->flag == DDL_PSM && rel->exps) 
			psm_exps_properties(sql, gp, rel->exps);
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
		if (!find_prop(rel->p, PROP_COUNT))
			rel->p = prop_create(sql->sa, PROP_COUNT, rel->p);
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
		break;

	case op_project:
	case op_groupby:
	case op_topn:
	case op_sample:
	case op_select:
		break;

	case op_insert:
	case op_update:
	case op_delete:
	case op_ddl:
		break;
	}
}

static sql_rel * rel_join_order(mvc *sql, sql_rel *rel) ;

static void
get_relations(mvc *sql, sql_rel *rel, list *rels)
{
	if (!rel_is_ref(rel) && rel->op == op_join && rel->exps == NULL) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		
		get_relations(sql, l, rels);
		get_relations(sql, r, rels);
		rel->l = NULL;
		rel->r = NULL;
		rel_destroy(rel);
	} else {
		rel = rel_join_order(sql, rel);
		append(rels, rel);
	}
}

static int
exp_count(int *cnt, sql_exp *e) 
{
	if (!e)
		return 0;
	if (find_prop(e->p, PROP_JOINIDX))
		*cnt += 100;
	if (find_prop(e->p, PROP_HASHCOL)) 
		*cnt += 100;
	if (find_prop(e->p, PROP_HASHIDX)) 
		*cnt += 100;
	switch(e->type) {
	case e_cmp:
		if (!is_complex_exp(e->flag)) {
			exp_count(cnt, e->l); 
			exp_count(cnt, e->r);
			if (e->f)
				exp_count(cnt, e->f);
		}	
		switch (get_cmp(e)) {
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
		case cmp_filter:
			if (exp_card(e->r) > CARD_AGGR) {
				/* filters for joins are special */
				*cnt += 1000;
				return 1000;
			}
			*cnt += 2;
			return 2;
		case cmp_in: 
		case cmp_notin: {
			list *l = e->r;
			int c = 9 - 10*list_length(l);
			*cnt += c;
			return c;
		}
		case cmp_or: /* prefer or over functions */
			*cnt += 3;
			return 3;
		default:
			return 0;
		}
	case e_column: 
		*cnt += 20;
		return 20;
	case e_atom:
		*cnt += 10;
		return 10;
	case e_func:
		/* functions are more expensive, depending on the number of columns involved. */ 
		if (e->card == CARD_ATOM)
			return 0;
		*cnt -= 5*list_length(e->l);
		return 5*list_length(e->l);
	case e_convert:
		/* functions are more expensive, depending on the number of columns involved. */ 
		if (e->card == CARD_ATOM)
			return 0;
		/* fall through */
	default:
		*cnt -= 5;
		return -5;
	}
}

static int
exp_keyvalue(sql_exp *e) 
{
	int cnt = 0;
	exp_count(&cnt, e);
	return cnt;
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
		const char *name = e->name;
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

int
exp_joins_rels(sql_exp *e, list *rels)
{
	sql_rel *l = NULL, *r = NULL;

	assert (e->type == e_cmp);
		
	if (get_cmp(e) == cmp_or) {
		l = NULL;
	} else if (get_cmp(e) == cmp_filter) {
		list *ll = e->l;
		list *lr = e->r;

		l = find_rel(rels, ll->h->data);
		r = find_rel(rels, lr->h->data);
	} else if (e->flag == cmp_in || e->flag == cmp_notin) {
		list *lr = e->r;

		l = find_rel(rels, e->l);
		if (lr && lr->h)
			r = find_rel(rels, lr->h->data);
	} else {
		l = find_rel(rels, e->l);
		r = find_rel(rels, e->r);
	}

	if (l && r)
		return 0;
	return -1;
}

static list *
matching_joins(sql_allocator *sa, list *rels, list *exps, sql_exp *je) 
{
	sql_rel *l, *r;

	assert (je->type == e_cmp);
		
	l = find_rel(rels, je->l);
	r = find_rel(rels, je->r);
	if (l && r) {
		list *res;
		list *n_rels = new_rel_list(sa);	

		append(n_rels, l);
		append(n_rels, r);
		res = list_select(exps, n_rels, (fcmp) &exp_joins_rels, (fdup)NULL);
		return res; 
	}
	return new_rel_list(sa);
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
		if (!r->l)
			return NULL;
		return r;
	case op_project:
	case op_select:
		return find_basetable(r->l);
	default:
		return NULL;
	}
}

static int
exps_count(list *exps) 
{
	node *n;
	int cnt = 0;

	if (!exps)
		return 0;
	for (n = exps->h; n; n=n->next)
		exp_count(&cnt, n->data);
	return cnt;
}

static list *
order_join_expressions(mvc *sql, list *dje, list *rels)
{
	list *res;
	node *n = NULL;
	int i, j, *keys, *pos, cnt = list_length(dje);
	int debug = mvc_debug_on(sql, 16);

	keys = (int*)malloc(cnt*sizeof(int));
	pos = (int*)malloc(cnt*sizeof(int));
	if (keys == NULL || pos == NULL) {
		if (keys)
			free(keys);
		if (pos)
			free(pos);
		return NULL;
	}
	res = sa_list(sql->sa);
	if (res == NULL) {
		free(keys);
		free(pos);
		return NULL;
	}
	for (n = dje->h, i = 0; n; n = n->next, i++) {
		sql_exp *e = n->data;

		keys[i] = exp_keyvalue(e);
		/* add some weight for the selections */
		if (e->type == e_cmp && !is_complex_exp(e->flag)) {
			sql_rel *l = find_rel(rels, e->l);
			sql_rel *r = find_rel(rels, e->r);

			if (l && is_select(l->op) && l->exps)
				keys[i] += list_length(l->exps)*10 + exps_count(l->exps)*debug;
			if (r && is_select(r->op) && r->exps)
				keys[i] += list_length(r->exps)*10 + exps_count(r->exps)*debug;
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

static int
find_join_rels(list **L, list **R, list *exps, list *rels)
{
	node *n;

	*L = sa_list(exps->sa);
	*R = sa_list(exps->sa);
	if (!exps || list_length(exps) <= 1)
		return -1;
	for(n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_rel *l = NULL, *r = NULL;

		if (!is_complex_exp(e->flag)){
			l = find_rel(rels, e->l);
			r = find_rel(rels, e->r);
		}
		if (l<r) {
			list_append(*L, l);
			list_append(*R, r);
		} else {
			list_append(*L, r);
			list_append(*R, l);
		}
	}
	return 0;
}

static list * 
distinct_join_exps(list *aje, list *lrels, list *rrels)
{
	node *n, *m, *o, *p;
	int len = 0, i, j;
	char *used = SA_NEW_ARRAY(aje->sa, char, len = list_length(aje));
	list *res = sa_list(aje->sa);

	memset(used, 0, len);
	assert(len == list_length(lrels));
	for(n = lrels->h, m = rrels->h, j = 0; n && m; 
	    n = n->next, m = m->next, j++) {
		if (n->data && m->data)
		for(o = n->next, p = m->next, i = j+1; o && p; 
		    o = o->next, p = p->next, i++) {
			if (o->data == n->data && p->data == m->data)
				used[i] = 1;
		}
	}
	for (i = 0, n = aje->h; i < len; n = n->next, i++) {
		if (!used[i])
			list_append(res, n->data);
	}
	return res;
}

static list *
find_fk( mvc *sql, list *rels, list *exps) 
{
	node *djn;
	list *sdje, *aje, *dje;
	list *lrels, *rrels;

	/* first find the distinct join expressions */
	aje = list_select(exps, rels, (fcmp) &exp_is_join, (fdup)NULL);
	/* add left/right relation */
	if (find_join_rels(&lrels, &rrels, aje, rels) < 0)
		dje = aje;
	else
		dje = distinct_join_exps(aje, lrels, rrels);
	for(djn=dje->h; djn; djn = djn->next) {
		/* equal join expressions */
		sql_idx *idx = NULL;
		sql_exp *je = djn->data, *le = je->l, *re = je->r; 

		if (is_complex_exp(je->flag))
			break;
		if (!find_prop(je->p, PROP_JOINIDX)) {
			int swapped = 0;
			list *aaje = matching_joins(sql->sa, rels, aje, je);
			list *eje = list_select(aaje, (void*)1, (fcmp) &exp_is_eqjoin, (fdup)NULL);
			sql_rel *lr = find_rel(rels, le), *olr = lr;
			sql_rel *rr = find_rel(rels, re), *orr = rr;
			sql_rel *bt = NULL;
			char *iname;

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
			lcols->destroy = NULL;
			rcols->destroy = NULL;
			if (list_length(lcols) != list_length(rcols)) 
				continue;

			idx = find_fk_index(l, lcols, r, rcols); 
			if (!idx) {
				idx = find_fk_index(r, rcols, l, lcols); 
				swapped = 1;
			} 

			if (idx && (iname = sa_strconcat( sql->sa, "%", idx->base.name)) != NULL &&
				   ((!swapped && name_find_column(olr, NULL, iname, -2, &bt) == NULL) ||
			            ( swapped && name_find_column(orr, NULL, iname, -2, &bt) == NULL))) 
				idx = NULL;

			if (idx) { 
				prop *p;
				node *n;
				sql_exp *t = NULL, *i = NULL;
	
				if (list_length(lcols) > 1 || !mvc_debug_on(sql, 512)) { 

					/* Add join between idx and TID */
					if (swapped) {
						sql_exp *s = je->l, *l = je->r;

						t = rel_find_column(sql->sa, olr, s->l, TID);
						i = rel_find_column(sql->sa, orr, l->l, iname);
						if (!t || !i) 
							continue;
						je = exp_compare(sql->sa, i, t, cmp_equal);
					} else {
						sql_exp *s = je->r, *l = je->l;

						t = rel_find_column(sql->sa, orr, s->l, TID);
						i = rel_find_column(sql->sa, olr, l->l, iname);
						if (!t || !i) 
							continue;
						je = exp_compare(sql->sa, i, t, cmp_equal);
					}

					/* Remove all join expressions */
					for (n = eje->h; n; n = n->next) 
						list_remove_data(exps, n->data);
					append(exps, je);
					djn->data = je;
				} else if (swapped) { /* else keep je for single column expressions */
					je = exp_compare(sql->sa, je->r, je->l, cmp_equal);
					/* Remove all join expressions */
					for (n = eje->h; n; n = n->next) 
						list_remove_data(exps, n->data);
					append(exps, je);
					djn->data = je;
				}
				je->p = p = prop_create(sql->sa, PROP_JOINIDX, je->p);
				p->value = idx;
			}
		}
	}

	/* sort expressions on weighted number of reducing operators */
	sdje = order_join_expressions(sql, dje, rels);
	return sdje;
}

static sql_rel *
order_joins(mvc *sql, list *rels, list *exps)
{
	sql_rel *top = NULL, *l = NULL, *r = NULL;
	sql_exp *cje;
	node *djn;
	list *sdje, *n_rels = new_rel_list(sql->sa);
	int fnd = 0;

	/* find foreign keys and reorder the expressions on reducing quality */
	sdje = find_fk(sql, rels, exps);

	if (list_length(rels) > 2 && mvc_debug_on(sql, 256)) {
		for(djn = sdje->h; djn; djn = djn->next ) {
			sql_exp *e = djn->data;
			list_remove_data(exps, e);
		}
		top =  rel_planner(sql, rels, sdje, exps);
		return top;
	}

	/* open problem, some expressions use more than 2 relations */
	/* For example a.x = b.y * c.z; */
	if (list_length(rels) >= 2 && sdje->h) {
		/* get the first expression */
		cje = sdje->h->data;

		/* find the involved relations */

		/* complex expressions may touch multiple base tables 
		 * Should be pushed up to extra selection.
		 * */
		if (cje->type != e_cmp || !is_complex_exp(cje->flag) || !find_prop(cje->p, PROP_HASHCOL) /*||
		   (cje->type == e_cmp && cje->f == NULL)*/) {
			l = find_one_rel(rels, cje->l);
			r = find_one_rel(rels, cje->r);
		}

		if (l && r && l != r) {
			list_remove_data(sdje, cje);
			list_remove_data(exps, cje);
		}
	}
	if (l && r && l != r) {
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
		for(djn = sdje->h; djn && !fnd && rels->h; djn = (!fnd)?djn->next:NULL) {
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
		set_processed(top);
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
				rel_select_add_exp(sql->sa, top, e);
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

static list *
push_up_join_exps( mvc *sql, sql_rel *rel) 
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
		l = push_up_join_exps(sql, rl);
		r = push_up_join_exps(sql, rr);
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
reorder_join(mvc *sql, sql_rel *rel)
{
	list *exps;
	list *rels;

	if (rel->op == op_join)
		rel->exps = push_up_join_exps(sql, rel);

       	exps = rel->exps;
	if (!exps) /* crosstable, ie order not important */
		return rel;
	rel->exps = NULL; /* should be all crosstables by now */
 	rels = new_rel_list(sql->sa);
	if (is_outerjoin(rel->op)) {
		int cnt = 0;
		/* try to use an join index also for outer joins */
		list_append(rels, rel->l);
		list_append(rels, rel->r);
		cnt = list_length(exps);
		rel->exps = find_fk(sql, rels, exps);
		if (list_length(rel->exps) != cnt) 
			rel->exps = order_join_expressions(sql, exps, rels);
	} else { 
 		get_relations(sql, rel, rels);
		if (list_length(rels) > 1) {
			rels = push_in_join_down(sql, rels, exps);
			rel = order_joins(sql, rels, exps);
		} else {
			rel->exps = exps;
			exps = NULL;
		}
	}
	return rel;
}

static sql_rel *
rel_join_order(mvc *sql, sql_rel *rel) 
{
	int e_changes = 0;

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
		break;

	case op_apply: 
	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->l = rel_join_order(sql, rel->l);
		rel->r = rel_join_order(sql, rel->r);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = rel_join_order(sql, rel->l);
		break;
	case op_ddl: 
		rel->l = rel_join_order(sql, rel->l);
		if (rel->r)
			rel->r = rel_join_order(sql, rel->r);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->l = rel_join_order(sql, rel->l);
		rel->r = rel_join_order(sql, rel->r);
		break;
	}
	if (is_join(rel->op) && rel->exps && !rel_is_ref(rel)) {
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
		rel = reorder_join(sql, rel);
	}
	(void)e_changes;
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
		if (!ne) {
			ne = mvc_find_subexp(sql, e->l?e->l:e->r, e->r);
			if (ne)
				return e;
		}
		if (!ne)
			return e;
		e = NULL;
		if (ne->name && ne->r && ne->l) 
			e = rel_bind_column2(sql, t, ne->l, ne->r, 0);
		if (!e && ne->r)
			e = rel_bind_column(sql, t, ne->r, 0);
		if (!e && ne->type == e_column) {
			e = mvc_find_subexp(sql, ne->l?ne->l:ne->r, ne->r);
			if (e)
				e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
		}
		sql->session->status = 0;
		sql->errstr[0] = 0;
		if (!e && exp_is_atom(ne))
			return ne;
		return e;
	case e_cmp: 
		if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
			list *l = exps_rename(sql, e->l, f, t);
			list *r = exps_rename(sql, e->r, f, t);
			if (l && r) {
				if (get_cmp(e) == cmp_filter) 
					ne = exp_filter(sql->sa, l, r, e->f, is_anti(e));
				else
					ne = exp_or(sql->sa, l, r, is_anti(e));
			}
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
	case e_psm:
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
		if (!ne || ne->type != e_column)
			return NULL;
		e = NULL;
		/*
		if (ne->name && ne->rname)
			e = rel_bind_column2(sql, t, ne->rname, ne->name, 0);
		if (!e && ne->name && !ne->rname)
			e = rel_bind_column(sql, t, ne->name, 0);
		if (!e && ne->name && ne->r && ne->l) 
			e = rel_bind_column2(sql, t, ne->l, ne->r, 0);
		if (!e && ne->r && !ne->l)
			e = rel_bind_column(sql, t, ne->r, 0);
			*/
		if (ne->l && ne->r)
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
		if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
			list *l, *r;

		       	l = exps_push_down(sql, e->l, f, t);
			if (!l)
				return NULL;
			r = exps_push_down(sql, e->r, f, t);
			if (!r) 
				return NULL;
			if (get_cmp(e) == cmp_filter) 
				return exp_filter(sql->sa, l, r, e->f, is_anti(e));
			return exp_or(sql->sa, l, r, is_anti(e));
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *r;

			l = _exp_push_down(sql, e->l, f, t);
			if (!l)
				return NULL;
			r = exps_push_down(sql, e->r, f, t);
			if (!r)
				return NULL;
			return exp_in(sql->sa, l, r, e->flag);
		} else {
			l = _exp_push_down(sql, e->l, f, t);
			if (!l)
				return NULL;
			r = _exp_push_down(sql, e->r, f, t);
			if (!r)
				return NULL;
			if (e->f) {
				r2 = _exp_push_down(sql, e->f, f, t);
				if (l && r && r2)
					return exp_compare2(sql->sa, l, r, r2, e->flag);
			} else if (l && r) {
				if (l->card < r->card)
					return exp_compare(sql->sa, r, l, swap_compare((comp_type)e->flag));
				else
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
	case e_psm:
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
		if (exp_unsafe(e))
			return 1;
	}
	return 0;
}

static int 
math_unsafe(sql_subfunc *f)
{
	if (!f->func->s) {
		if (strcmp(f->func->base.name, "sql_div") == 0 ||
		    strcmp(f->func->base.name, "sqrt") == 0 ||
		    strcmp(f->func->base.name, "atan") == 0 ) 
			return 1;
	}
	return 0;
}

static int 
can_push_func(sql_exp *e, sql_rel *rel, int *must)
{
	if (!e)
		return 0;
	switch(e->type) {
	case e_cmp: {
		int mustl = 0, mustr = 0, mustf = 0;
		sql_exp *l = e->l, *r = e->r, *f = e->f;

		if (get_cmp(e) == cmp_or || e->flag == cmp_in || e->flag == cmp_notin || get_cmp(e) == cmp_filter) 
			return 0;
		return ((l->type == e_column || can_push_func(l, rel, &mustl)) && (*must = mustl)) || 
	               (!f && (r->type == e_column || can_push_func(r, rel, &mustr)) && (*must = mustr)) || 
		       (f && 
	               (r->type == e_column || can_push_func(r, rel, &mustr)) && 
		       (f->type == e_column || can_push_func(f, rel, &mustf)) && (*must = (mustr || mustf)));
	}
	case e_convert:
		return can_push_func(e->l, rel, must);
	case e_func: {
		list *l = e->l;
		node *n;
		int res = 1, lmust = 0;
		
		if (e->f){
			sql_subfunc *f = e->f;
			if (math_unsafe(f) || f->func->type != F_FUNC)
				return 0;
		}
		if (l) for (n = l->h; n && res; n = n->next)
			res &= can_push_func(n->data, rel, &lmust);
		if (res && !lmust)
			return 1;
		(*must) |= lmust;
		return res;
	}
	case e_column:
		if (rel && !rel_find_exp(rel, e)) 
			return 0;
		(*must) = 1;
		/* fall through */
	case e_atom:
	default:
		return 1;
	}
}

static int
exps_can_push_func(list *exps, sql_rel *rel) 
{
	node *n;

	for(n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		int must = 0, mustl = 0, mustr = 0;

		if (is_joinop(rel->op) && ((can_push_func(e, rel->l, &mustl) && mustl) || (can_push_func(e, rel->r, &mustr) && mustr)))
			return 1;
		else if (is_select(rel->op) && can_push_func(e, NULL, &must) && must)
			return 1;
	}
	return 0;
}

static int
exp_needs_push_down(sql_exp *e)
{
	if (!e)
		return 0;
	switch(e->type) {
	case e_cmp: 
		if (get_cmp(e) == cmp_or || e->flag == cmp_in || e->flag == cmp_notin || get_cmp(e) == cmp_filter) 
			return 0;
		return exp_needs_push_down(e->l) || exp_needs_push_down(e->r) || (e->f && exp_needs_push_down(e->f));
	case e_convert:
		return exp_needs_push_down(e->l);
	case e_aggr: 
	case e_func: 
		return 1;
	case e_column:
	case e_atom:
	default:
		return 0;
	}
}

static int
exps_need_push_down( list *exps )
{
	node *n;
	for(n = exps->h; n; n = n->next) 
		if (exp_needs_push_down(n->data))
			return 1;
	return 0;
}

static sql_rel *
rel_push_func_down(int *changes, mvc *sql, sql_rel *rel) 
{
	if ((is_select(rel->op) || is_joinop(rel->op)) && rel->l && rel->exps && !(rel_is_ref(rel))) {
		list *exps = rel->exps;

		if (is_select(rel->op) &&  list_length(rel->exps) <= 1)  /* only push down when thats useful */
			return rel;
		if (exps_can_push_func(exps, rel) && exps_need_push_down(exps)) {
			sql_rel *nrel;
			sql_rel *l = rel->l, *ol = l;
			sql_rel *r = rel->r, *or = r;
			node *n;

			/* we need a full projection, group by's and unions cannot be extended
 			 * with more expressions */
			if (l->op != op_project) { 
				if (is_subquery(l))
					return rel;
				rel->l = l = rel_project(sql->sa, l, 
					rel_projections(sql, l, NULL, 1, 1));
			}
			if (is_joinop(rel->op) && r->op != op_project) {
				if (is_subquery(r))
					return rel;
				rel->r = r = rel_project(sql->sa, r, 
					rel_projections(sql, r, NULL, 1, 1));
			}
 			nrel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
			for(n = exps->h; n; n = n->next) {
				sql_exp *e = n->data, *ne = NULL;
				int must = 0, mustl = 0, mustr = 0;

				if (e->type == e_column)
					continue;
				if ((is_joinop(rel->op) && ((can_push_func(e, l, &mustl) && mustl) || (can_push_func(e, r, &mustr) && mustr))) ||
				    (is_select(rel->op) && can_push_func(e, NULL, &must) && must)) {
					must = 0; mustl = 0; mustr = 0;
					if (e->type != e_cmp) { /* predicate */
						if ((is_joinop(rel->op) && ((can_push_func(e, l, &mustl) && mustl) || (can_push_func(e, r, &mustr) && mustr))) ||
					    	    (is_select(rel->op) && can_push_func(e, NULL, &must) && must)) {
							exp_label(sql->sa, e, ++sql->label);
							if (mustr)
								append(r->exps, e);
							else
								append(l->exps, e);
							e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
							n->data = e;
							(*changes)++;
						}
					} else {
						ne = e->l;
						if ((is_joinop(rel->op) && ((can_push_func(ne, l, &mustl) && mustl) || (can_push_func(ne, r, &mustr) && mustr))) ||
					    	    (is_select(rel->op) && can_push_func(ne, NULL, &must) && must)) {
							exp_label(sql->sa, ne, ++sql->label);
							if (mustr)
								append(r->exps, ne);
							else
								append(l->exps, ne);
							ne = exp_column(sql->sa, exp_relname(ne), exp_name(ne), exp_subtype(ne), ne->card, has_nil(ne), is_intern(ne));
							(*changes)++;
						}
						e->l = ne;

						must = 0; mustl = 0; mustr = 0;
						ne = e->r;
						if ((is_joinop(rel->op) && ((can_push_func(ne, l, &mustl) && mustl) || (can_push_func(ne, r, &mustr) && mustr))) ||
					    	    (is_select(rel->op) && can_push_func(ne, NULL, &must) && must)) {
							exp_label(sql->sa, ne, ++sql->label);
							if (mustr)
								append(r->exps, ne);
							else
								append(l->exps, ne);
							ne = exp_column(sql->sa, exp_relname(ne), exp_name(ne), exp_subtype(ne), ne->card, has_nil(ne), is_intern(ne));
							(*changes)++;
						}
						e->r = ne;

						if (e->f) {
							must = 0; mustl = 0; mustr = 0;
							ne = e->f;
							if ((is_joinop(rel->op) && ((can_push_func(ne, l, &mustl) && mustl) || (can_push_func(ne, r, &mustr) && mustr))) ||
					            	    (is_select(rel->op) && can_push_func(ne, NULL, &must) && must)) {
								exp_label(sql->sa, ne, ++sql->label);
								if (mustr)
									append(r->exps, ne);
								else
									append(l->exps, ne);
								ne = exp_column(sql->sa, exp_relname(ne), exp_name(ne), exp_subtype(ne), ne->card, has_nil(ne), is_intern(ne));
								(*changes)++;
							}
							e->f = ne;
						}
					}
				}
			}
			if (*changes) {
				rel = nrel;
			} else {
				if (l != ol)
					rel->l = ol;
				if (is_joinop(rel->op) && r != or)
					rel->r = or;
			}
		}
	}
	if (rel->op == op_project && rel->l && rel->exps) {
		sql_rel *pl = rel->l;

		if (is_joinop(pl->op) && exps_can_push_func(rel->exps, rel)) {
			node *n;
			sql_rel *l = pl->l, *r = pl->r;
			list *nexps;

			if (l->op != op_project) { 
				if (is_subquery(l))
					return rel;
				pl->l = l = rel_project(sql->sa, l, 
					rel_projections(sql, l, NULL, 1, 1));
			}
			if (is_joinop(rel->op) && r->op != op_project) {
				if (is_subquery(r))
					return rel;
				pl->r = r = rel_project(sql->sa, r, 
					rel_projections(sql, r, NULL, 1, 1));
			}
			nexps = new_exp_list(sql->sa);
			for ( n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				int mustl = 0, mustr = 0;

				if ((can_push_func(e, l, &mustl) && mustl) || 
				    (can_push_func(e, r, &mustr) && mustr)) {
					if (mustl)
						append(l->exps, e);
					else
						append(r->exps, e);
				} else
					append(nexps, e);
			}
			rel->exps = nexps;
			(*changes)++;
		}
	}
	return rel;
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
	    /* currently only single count aggregation is handled, no other projects or aggregation */
	    list_length(rel->exps) == 1 && ((sql_exp *) rel->exps->h->data)->type == e_aggr &&
            strcmp(((sql_subaggr *) ((sql_exp *) rel->exps->h->data)->f)->aggr->base.name, "count") == 0) {
	    	sql_exp *nce, *oce;
		sql_rel *gbl, *gbr;		/* Group By */
		sql_rel *cp;			/* Cross Product */
		sql_subfunc *mult;
		list *args;
		const char *rname = NULL, *name = NULL;
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
			gbl = rel_groupby(sql, rel_dup(srel), NULL);
			rel_groupby_add_aggr(sql, gbl, e);
			append(args, cnt);
		}

		srel = r->r;
		{
			sql_subaggr *cf = sql_bind_aggr(sql->sa, sql->session->schema, "count", NULL);
			sql_exp *cnt, *e = exp_aggr(sql->sa, NULL, cf, need_distinct(oce), need_no_nil(oce), oce->card, 0);

			exp_label(sql->sa, e, ++sql->label);
			cnt = exp_column(sql->sa, NULL, exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			gbr = rel_groupby(sql, rel_dup(srel), NULL);
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
sum_limit_offset(mvc *sql, list *exps )
{
	list *nexps = new_exp_list(sql->sa);
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_subfunc *add;

	/* if the expression list only consists of a limit expression, 
	 * we copy it */
	if (list_length(exps) == 1 && exps->h->data)
		return append(nexps, exps->h->data);
	add = sql_bind_func_result(sql->sa, sql->session->schema, "sql_add", lng, lng, lng);
	return append(nexps, exp_op(sql->sa, exps, add));
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
	int pos = 0;
	node *n, *m;

	/* check if a column uses an alias earlier in the list */
	for (n = exps1->h, m = exps2->h; n && m; n = n->next, m = m->next, pos++) {
		sql_exp *e2 = m->data;

		if (e2->type == e_column) {
			sql_exp *ne = NULL;

			if (e2->l) 
				ne = exps_bind_column2(exps2, e2->l, e2->r);
			if (!ne && !e2->l)
				ne = exps_bind_column(exps2, e2->r, NULL);
			if (ne) {
				int p = list_position(exps2, ne);

				if (p < pos) {
					ne = list_fetch(exps1, p);
					if (e2->l)
						e2->l = (void *) exp_relname(ne);
					e2->r = (void *) exp_name(ne);
				}
			}
		}
	}

	assert(list_length(exps1) <= list_length(exps2)); 
	for (n = exps1->h, m = exps2->h; n && m; n = n->next, m = m->next) {
		sql_exp *e1 = n->data;
		sql_exp *e2 = m->data;
		const char *rname = e1->rname;

		if (!rname && e1->type == e_column && e1->l && e2->rname && 
		    strcmp(e1->l, e2->rname) == 0)
			rname = e2->rname;
		exp_setname(sql->sa, e2, rname, e1->name );
	}
	MT_lock_set(&exps2->ht_lock);
	exps2->ht = NULL;
	MT_lock_unset(&exps2->ht_lock);
}

static sql_rel *
rel_push_topn_down(int *changes, mvc *sql, sql_rel *rel) 
{
	sql_rel *rl, *r = rel->l;

	if (rel->op == op_topn && topn_save_exps(rel->exps)) {
		sql_rel *rp = NULL;

		if (r && r->op == op_project && need_distinct(r)) 
			return rel;
		/* duplicate topn direct under union */

		if (r && r->exps && r->op == op_union && !(rel_is_ref(r)) && r->l) {
			sql_rel *u = r, *x;
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

			ul = rel_topn(sql->sa, ul, sum_limit_offset(sql, rel->exps));
			ur = rel_topn(sql->sa, ur, sum_limit_offset(sql, rel->exps));
			u->l = ul;
			u->r = ur;
			(*changes)++;
			return rel;
		}
		/* duplicate topn + [ project-order ] under union */
		if (r)
			rp = r->l;
		if (r && r->exps && r->op == op_project && !(rel_is_ref(r)) && r->r && r->l &&
		    rp->op == op_union) {
			sql_rel *u = rp, *ou = u, *x;
			sql_rel *ul = u->l;
			sql_rel *ur = u->r;
			int add_r = 0;

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

			if (list_length(ul->exps) > list_length(r->exps))
				add_r = 1;
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
			/* possibly add order by column */
			if (add_r)
				ul->exps = list_merge(ul->exps, exps_copy(sql->sa, r->r), NULL);
			ul->r = exps_copy(sql->sa, r->r);
			ul = rel_topn(sql->sa, ul, sum_limit_offset(sql, rel->exps));
			ur = rel_project(sql->sa, ur, NULL);
			ur->exps = exps_copy(sql->sa, r->exps);
			/* possibly add order by column */
			if (add_r)
				ur->exps = list_merge(ur->exps, exps_copy(sql->sa, r->r), NULL);
			ur->r = exps_copy(sql->sa, r->r);
			ur = rel_topn(sql->sa, ur, sum_limit_offset(sql, rel->exps));
			u = rel_setop(sql->sa, ul, ur, op_union);
			u->exps = exps_alias(sql->sa, r->exps); 
			set_processed(u);
			/* possibly add order by column */
			if (add_r)
				u->exps = list_merge(u->exps, exps_copy(sql->sa, r->r), NULL);

			if (need_distinct(r)) {
				set_distinct(ul);
				set_distinct(ur);
			}

			/* zap names */
			rel_no_rename_exps(u->exps);
			rel_destroy(ou);

			ur = rel_project(sql->sa, u, exps_alias(sql->sa, r->exps));
			ur->r = r->r;
			r->l = NULL;

			if (need_distinct(r)) 
				set_distinct(ur);

			rel_destroy(r);
			rel->l = ur;
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
			r = rel_topn(sql->sa, r, sum_limit_offset(sql, rel->exps));
		}

		/* push topn under crossproduct */
		if (r && !r->exps && r->op == op_join && !(rel_is_ref(r)) &&
		    ((sql_rel *)r->l)->op != op_topn && ((sql_rel *)r->r)->op != op_topn) {
			r->l = rel_topn(sql->sa, r->l, sum_limit_offset(sql, rel->exps));
			r->r = rel_topn(sql->sa, r->r, sum_limit_offset(sql, rel->exps));
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
			r->l = rel_topn(sql->sa, r->l, sum_limit_offset(sql, rel->exps));
			r->r = rel_topn(sql->sa, r->r, sum_limit_offset(sql, rel->exps));
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
static sql_exp *
exp_push_down_prj(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

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
		if (ne && list_position(f->exps, ne) >= list_position(f->exps, e)) 
			return NULL;
		while (ne && f->op == op_project && ne->type == e_column) {
			sql_exp *oe = e, *one = ne;

			e = ne;
			ne = NULL;
			if (e->l)
				ne = exps_bind_column2(f->exps, e->l, e->r);
			if (!ne && !e->l)
				ne = exps_bind_column(f->exps, e->r, NULL);
			if (ne && ne != one && list_position(f->exps, ne) >= list_position(f->exps, one)) 
				ne = NULL;
			if (!ne || ne == one) {
				ne = one;
				e = oe;
				break;
			}
			if (ne->type != e_column && ne->type != e_atom)
				return NULL;
		}
		/* possibly a groupby/project column is renamed */
		if (is_groupby(f->op) && f->r) {
			sql_exp *gbe = NULL;
			if (ne->l) 
				gbe = exps_bind_column2(f->r, ne->l, ne->r);
			if (!gbe && !e->l)
				gbe = exps_bind_column(f->r, ne->r, NULL);
			ne = gbe;
			if (!ne || (ne->type != e_column && ne->type != e_atom))
				return NULL;
		}
		if (ne->type == e_atom) 
			e = exp_copy(sql->sa, ne);
		else
			e = exp_alias(sql->sa, e->rname, exp_name(e), ne->l, ne->r, exp_subtype(e), e->card, has_nil(e), is_intern(e));
		if (ne->p)
			e->p = prop_copy(sql->sa, ne->p);
		return e;
	case e_cmp: 
		if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
			list *l = exps_push_down_prj(sql, e->l, f, t);
			list *r = exps_push_down_prj(sql, e->r, f, t);

			if (!l || !r) 
				return NULL;
			if (get_cmp(e) == cmp_filter) 
				return exp_filter(sql->sa, l, r, e->f, is_anti(e));
			return exp_or(sql->sa, l, r, is_anti(e));
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
	        sql_exp *ne = NULL;

		if (e->type == e_func && exp_unsafe(e))
			return NULL;
		if (!l) {
			return e;
		} else {
			nl = exps_push_down_prj(sql, l, f, t);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			ne = exp_op(sql->sa, nl, e->f);
		else 
			ne = exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
		if (e->p)
			ne->p = prop_copy(sql->sa, e->p);
		return ne;
	}	
	case e_atom:
	case e_psm:
		if (e->type == e_atom && e->f) /* value list */
			return NULL;
		return e;
	}
	return NULL;
}

/* TODO: check for keys with more than one colun */
static int
exps_unique( list *exps )
{
	node *n;

	if ((n = exps->h) != NULL) {
		sql_exp *e = n->data;
		prop *p;

		if (e && (p = find_prop(e->p, PROP_HASHCOL)) != NULL) {
			sql_ukey *k = p->value;
			if (k && list_length(k->k.columns) <= 1)
				return 1;
		}
	}
	return 0;
}

static sql_rel *
rel_distinct_project2groupby(int *changes, mvc *sql, sql_rel *rel)
{
	sql_rel *l = rel->l;

	/* rewrite distinct project (table) [ constant ] -> project [ constant ] */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    exps_card(rel->exps) <= CARD_ATOM) {
		set_nodistinct(rel);
		rel->l = rel_topn(sql->sa, rel->l, append(sa_list(sql->sa), exp_atom_lng(sql->sa, 1)));
	}

	/* rewrite distinct project [ pk ] ( select ( table ) [ e op val ]) 
	 * into project [ pk ] ( select ( table )  */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    l->op == op_select && exps_unique(rel->exps)) 
		set_nodistinct(rel);
	/* rewrite distinct project [ gbe ] ( select ( groupby [ gbe ] [ gbe, e ] )[ e op val ]) 
	 * into project [ gbe ] ( select ( group etc ) */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && 
	    need_distinct(rel) && l->op == op_select){ 
		sql_rel *g = l->l;
		if (is_groupby(g->op)) {
			list *gbe = g->r;
			node *n;
			int fnd = 1;

			for (n = rel->exps->h; n && fnd; n = n->next) {
				sql_exp *e = n->data;

				if (e->card > CARD_ATOM) { 
					/* find e in gbe */
					sql_exp *ne = list_find_exp(g->exps, e);

					if (ne) 
						ne = list_find_exp( gbe, ne);
					fnd++;
					if (!ne) 
						fnd = 0;
				}
			}
			if (fnd == (list_length(gbe)+1)) 
				set_nodistinct(rel);
		}
	}
	if (rel->op == op_project && rel->l && 
	    need_distinct(rel) && exps_card(rel->exps) > CARD_ATOM) {
		node *n;
		list *exps = new_exp_list(sql->sa), *gbe = new_exp_list(sql->sa);
		list *obe = rel->r; /* we need to readd the ordering later */

		if (obe) { 
			int fnd = 0;

			for(n = obe->h; n && !fnd; n = n->next) { 
				sql_exp *e = n->data;

				if (e->type != e_column) 
					fnd = 1;
				else if (exps_bind_column2(rel->exps, e->l, e->r) == 0) 
					fnd = 1;
			}
			if (fnd)
				return rel;
		}
		rel->l = rel_project(sql->sa, rel->l, rel->exps);

		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne;

			if (!exp_name(e))
				exp_label(sql->sa, e, ++sql->label);
			ne = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), exp_card(e), has_nil(e), 0);
			if (e->card > CARD_ATOM) { /* no need to group by on constants */
				append(gbe, ne);
			}
			append(exps, ne);
		}
		rel->op = op_groupby;
		rel->exps = exps;
		rel->r = gbe;
		set_nodistinct(rel);
		if (obe) {
			/* add order again */
			rel = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 1));
			rel->r = obe;
		}
		*changes = 1;
	}
	return rel;
}

static int
exp_shares_exps( sql_exp *e, list *shared, lng *uses)
{
	switch(e->type) {
	case e_cmp: /* not in projection list */
	case e_psm:
		assert(0);
	case e_atom:
		return 0;
	case e_column: 
		{
			sql_exp *ne = NULL;
			if (e->l) 
				ne = exps_bind_column2(shared, e->l, e->r);
			if (!ne && !e->l)
				ne = exps_bind_column(shared, e->r, NULL);
			if (!ne)
				return 0;
			if (ne && ne->type != e_column) {
				lng used = (lng) 1 << list_position(shared, ne);
				if (used & *uses)
					return 1;
				*uses &= used;
				return 0;
			}
			if (ne && ne != e && (list_position(shared, e) < 0 || list_position(shared, e) > list_position(shared, ne))) 
				/* maybe ne refers to a local complex exp */
				return exp_shares_exps( ne, shared, uses);
			return 0;
		}
	case e_convert:
		return exp_shares_exps(e->l, shared, uses);

	case e_aggr:
	case e_func: 
		{
			list *l = e->l;
			node *n;

			if (!l)
				return 0;
			for (n = l->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_shares_exps( e, shared, uses))
					return 1;
			}
		}
	}
	return 0;
}

static int
exps_share_expensive_exp( list *exps, list *shared )
{
	node *n;
	lng uses = 0;

	if (!exps || !shared)
		return 0;
	for (n = exps->h; n; n = n->next){
		sql_exp *e = n->data;

		if (exp_shares_exps( e, shared, &uses))
			return 1;
	}
	return 0;
}

static int ambigious_ref( list *exps, sql_exp *e);
static int
ambigious_refs( list *exps, list *refs) 
{
	node *n;

	if (!refs)
		return 0;
	for(n=refs->h; n; n = n->next) {
		if (ambigious_ref(exps, n->data))
			return 1;
	}
	return 0;
}

static int
ambigious_ref( list *exps, sql_exp *e) 
{
	sql_exp *ne = NULL;

	if (e->type == e_column) {
		if (e->l) 
			ne = exps_bind_column2(exps, e->l, e->r);
		if (!ne && !e->l)
			ne = exps_bind_column(exps, e->r, NULL);
		if (ne && e != ne) 
			return 1;
	}
	if (e->type == e_func) 
		return ambigious_refs(exps, e->l);
	return 0;
}

/* merge 2 projects into the lower one */
static sql_rel *
rel_merge_projects(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = rel->exps;
	sql_rel *prj = rel->l;
	node *n;

	if (rel->op == op_project && 
	    prj && prj->op == op_project && !(rel_is_ref(prj)) && !prj->r) {
		int all = 1;

		if (project_unsafe(rel) || project_unsafe(prj) || exps_share_expensive_exp(rel->exps, prj->exps))
			return rel;
	
		/* here we need to fix aliases */
		rel->exps = new_exp_list(sql->sa); 

		/* for each exp check if we can rename it */
		for (n = exps->h; n && all; n = n->next) { 
			sql_exp *e = n->data, *ne = NULL;

			/* We do not handle expressions pointing back in the list */
			if (ambigious_ref(exps, e)) {
				all = 0;
				break;
			}
			ne = exp_push_down_prj(sql, e, prj, prj->l);
			/* check if the refered alias name isn't used twice */
			if (ne && ambigious_ref(rel->exps, ne)) {
				all = 0;
				break;
			}
			/*
			if (ne && ne->type == e_column) { 
				sql_exp *nne = NULL;

				if (ne->l)
					nne = exps_bind_column2(rel->exps, ne->l, ne->r);
				if (!nne && !ne->l)
					nne = exps_bind_column(rel->exps, ne->r, NULL);
				if (nne && ne != nne && nne != e) {
					all = 0;
					break;
				}
			}
			*/
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
	list * l = new_func_list(sql->sa); 
	node *n;

	for(n = exps->h; n; n = n->next)
		append(l, exp_subtype(n->data)); 
	return sql_bind_func_(sql->sa, sql->session->schema, name, l, F_FUNC);
}

/* f (args) unsafe  (examples a/b iff b == 0 and sqrt(a iff a<0))
 */
static sql_exp *
math_unsafe_fixup_unop( mvc *sql, sql_exp *e, sql_exp *le, sql_exp *cond, int lr)
{
	list *args = new_exp_list(sql->sa);
	sql_subfunc *ifthen;
	sql_exp *o;

	/* if (cond) then val else const */
	append(args, cond);
	if (!lr)
		append(args, le);
	o = exp_atom_lng(sql->sa, 1);
	append(args, exp_convert(sql->sa, o, exp_subtype(o), exp_subtype(le)));
	if (lr)
		append(args, le);
	ifthen = find_func(sql, "ifthenelse", args);
	assert(ifthen);
	le = exp_op(sql->sa, args, ifthen);
	return exp_unop(sql->sa, le, e->f);
}

static sql_exp *
math_unsafe_fixup_binop( mvc *sql, sql_exp *e, sql_exp *le, sql_exp *re, sql_exp *cond, int lr )
{
	list *args = new_exp_list(sql->sa);
	sql_subfunc *ifthen;
	sql_exp *o;

	/* if (cond) then val else const */
	append(args, cond);
	if (!lr)
		append(args, re);
	o = exp_atom_lng(sql->sa, 1);
	append(args, exp_convert(sql->sa, o, exp_subtype(o), exp_subtype(re)));
	if (lr)
		append(args, re);
	ifthen = find_func(sql, "ifthenelse", args);
	assert(ifthen);
	re = exp_op(sql->sa, args, ifthen);

	return exp_binop(sql->sa, le, re, e->f);
}

static sql_exp *
math_unsafe_fixup( mvc *sql, sql_exp *e, sql_exp *cond, int lr )
{
	list *args = e->l;

	if (args && args->h && args->h->next)
		return math_unsafe_fixup_binop(sql, e, args->h->data, args->h->next->data, cond, lr);
	else
		return math_unsafe_fixup_unop(sql, e, args->h->data, cond, lr);
}

static int 
exp_find_math_unsafe( sql_exp *e)
{
	if (!e)
		return 0;
	switch(e->type) {
	case e_aggr:
	case e_func: 
		{
			list *l = e->l;
			node *n;
			sql_subfunc *f = e->f;

			if (math_unsafe(f))
				return 1;
			if (!l)
				return 0;
			for (n = l->h; n; n = n->next) {
				sql_exp *ne = n->data;

				if (exp_find_math_unsafe(ne))
					return 1;
			}
		}
		/* fall through */
	case e_convert:
		return exp_find_math_unsafe(e->l);
	case e_column: 
	case e_cmp:
	case e_psm:
	case e_atom:
	default:
		return 0;
	}
}

static sql_exp * exp_math_unsafe_fixup( mvc *sql, sql_exp *e, sql_exp *cond, int lr );

static list *
exps_case_fixup( mvc *sql, list *exps, sql_exp *cond, int lr )
{
	node *n;

	if (exps) {
		list *nexps = new_exp_list(sql->sa);
		for( n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (is_func(e->type) && e->l && !is_analytic(e) ) {
				sql_subfunc *f = e->f;

				if (math_unsafe(f)) {
					e = math_unsafe_fixup(sql, e, cond, lr);
				} else {
					list *l = exps_case_fixup(sql, e->l, cond, lr);
					sql_exp *ne = exp_op(sql->sa, l, f);
					exp_setname(sql->sa, ne, e->rname, e->name );
					e = ne;
				}
			} else if (e->type == e_convert) {
				sql_exp *l = exp_math_unsafe_fixup(sql, e->l, cond, lr);
				sql_exp *ne = exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
				e = ne;
			}
			append(nexps, e);
		}
		return nexps;
	}
	return exps;
}

static sql_exp *
exp_math_unsafe_fixup( mvc *sql, sql_exp *e, sql_exp *cond, int lr )
{
	if (is_func(e->type) && e->l && !is_analytic(e) ) {
		sql_subfunc *f = e->f;

		if (math_unsafe(f)) {
			e = math_unsafe_fixup(sql, e, cond, lr);
		} else {
			list *l = exps_case_fixup(sql, e->l, cond, lr);
			sql_exp *ne = exp_op(sql->sa, l, f);
			exp_setname(sql->sa, ne, e->rname, e->name );
			e = ne;
		}
	}
	return e;
}

static sql_exp *
exp_case_fixup( mvc *sql, sql_rel *rel, sql_exp *e )
{
	/* only functions need fix up */
	if (e->type == e_psm) {
		if (e->flag & PSM_SET) {
			/* todo */
		} else if (e->flag & PSM_VAR) {
			/* todo */
		} else if (e->flag & PSM_RETURN) {
			e->l = exp_case_fixup(sql, rel, e->l);
		} else if (e->flag & PSM_WHILE) {
			e->l = exp_case_fixup(sql, rel, e->l);
			e->r = exps_case_fixup(sql, e->r, NULL, 0);
		} else if (e->flag & PSM_IF) {
			e->l = exp_case_fixup(sql, rel, e->l);
			e->r = exps_case_fixup(sql, e->r, NULL, 0);
			if (e->f)
				e->f = exps_case_fixup(sql, e->f, NULL, 0);
		} else if (e->flag & PSM_REL) {
		}
		return e;
	}
	if (e->type == e_func && e->l && !is_analytic(e) ) {
		list *l = new_exp_list(sql->sa), *args = e->l;
		node *n;
		sql_exp *ne;
		sql_subfunc *f = e->f;

		/* first fixup arguments */
		for (n=args->h; n; n=n->next) {
			sql_exp *a = exp_case_fixup(sql, rel, n->data);
			list_append(l, a);
		}
		ne = exp_op(sql->sa, l, f);
		exp_setname(sql->sa, ne, e->rname, e->name );

		/* ifthenelse with one of the sides an 'sql_div' */
		args = ne->l;
		if (!f->func->s && !strcmp(f->func->base.name, "ifthenelse")) { 
			sql_exp *cond = args->h->data, *nne; 
			sql_exp *a1 = args->h->next->data; 
			sql_exp *a2 = args->h->next->next->data; 

			if (rel) {
				exp_label(sql->sa, cond, ++sql->label);
				append(rel->exps, cond);
				cond = exp_column(sql->sa, exp_find_rel_name(cond), exp_name(cond), exp_subtype(cond), cond->card, has_nil(cond), is_intern(cond));
			}
			/* rewrite right hands of div */
			if ((a1->type == e_func || a1->type == e_convert) && exp_find_math_unsafe(a1)) {
				a1 = exp_math_unsafe_fixup(sql, a1, cond, 0);
			} else if (a1->type == e_func && a1->l) { 
				a1->l = exps_case_fixup(sql, a1->l, cond, 0); 
			}
			if  ((a2->type == e_func || a2->type == e_convert) && exp_find_math_unsafe(a2)) {
				a2 = exp_math_unsafe_fixup(sql, a2, cond, 1);
			} else if (a2->type == e_func && a2->l) { 
				a2->l = exps_case_fixup(sql, a2->l, cond, 1); 
			}
			assert(cond && a1 && a2);
			nne = exp_op3(sql->sa, cond, a1, a2, ne->f);
			exp_setname(sql->sa, nne, ne->rname, ne->name );
			ne = nne;
		}
		return ne;
	}
	if (e->type == e_convert) {
		sql_exp *e1 = exp_case_fixup(sql, rel, e->l);
		sql_exp *ne = exp_convert(sql->sa, e1, exp_fromtype(e), exp_totype(e));
		exp_setname(sql->sa, ne, e->rname, e->name);
		return ne;
	} 
	if (e->type == e_aggr) {
		list *l = NULL, *args = e->l;
		node *n;
		sql_exp *ne;
		sql_subaggr *f = e->f;

		/* first fixup arguments */
		if (args) {
 			l = new_exp_list(sql->sa);
			for (n=args->h; n; n=n->next) {
				sql_exp *a = exp_case_fixup(sql, rel, n->data);
				list_append(l, a);
			}
		}
		ne = exp_aggr(sql->sa, l, f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
		exp_setname(sql->sa, ne, e->rname, e->name );
		return ne;
	}
	return e;
}

static sql_rel *
rel_case_fixup(int *changes, mvc *sql, sql_rel *rel) 
{
	
	(void)changes; /* only go through it once, ie don't mark for changes */
	if ((is_project(rel->op) || (rel->op == op_ddl && rel->flag == DDL_PSM)) && rel->exps) {
		list *exps = rel->exps;
		node *n;
		int needed = 0;
		sql_rel *res = rel;
		int push_down = 0;

		for (n = exps->h; n && !needed; n = n->next) { 
			sql_exp *e = n->data;

			if (e->type == e_func || e->type == e_convert ||
			    e->type == e_aggr || e->type == e_psm) 
				needed = 1;
		}
		if (!needed)
			return rel;

		/* get proper output first, then rewrite lower project (such that it can split expressions) */
		push_down = is_simple_project(rel->op) && !rel->r && !rel_is_ref(rel);
		if (push_down)
			res = rel_project(sql->sa, rel, rel_projections(sql, rel, NULL, 1, 2));

		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = exp_case_fixup( sql, push_down?rel:NULL, n->data );
		
			if (!e) 
				return NULL;
			list_append(rel->exps, e);
		}
		return res;
	} 
	return rel;
}

static sql_exp *
exp_simplify_math( mvc *sql, sql_exp *e, int *changes)
{
	if (e->type == e_func || e->type == e_aggr) {
		list *l = e->l;
		sql_subfunc *f = e->f;
		node *n;
		sql_exp *le;

		if (list_length(l) < 1)
			return e;

		le = l->h->data;
		if (!exp_subtype(le) || (!EC_COMPUTE(exp_subtype(le)->type->eclass) && exp_subtype(le)->type->eclass != EC_DEC))
			return e;
		if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;
			/* 0*a = 0 */
			if (exp_is_atom(le) && exp_is_zero(sql, le) && exp_is_not_null(sql, re)) {
				(*changes)++;
				exp_setname(sql->sa, le, exp_relname(e), exp_name(e));
				return le;
			}
			/* a*0 = 0 */
			if (exp_is_atom(re) && exp_is_zero(sql, re) && exp_is_not_null(sql, le)) {
				(*changes)++;
				exp_setname(sql->sa, re, exp_relname(e), exp_name(e));
				return re;
			}
			/* 1*a = a
			if (exp_is_atom(le) && exp_is_one(sql, le)) {
				(*changes)++;
				exp_setname(sql->sa, re, exp_relname(e), exp_name(e));
				return re;
			}
			*/
			/* a*1 = a
			if (exp_is_atom(re) && exp_is_one(sql, re)) {
				(*changes)++;
				exp_setname(sql->sa, le, exp_relname(e), exp_name(e));
				return le;
			}
			*/
			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, le);
				atom *ra = exp_flatten(sql, re);

				if (la && ra) {
					atom *a = atom_mul(la, ra);

					if (a) {
						sql_exp *ne = exp_atom(sql->sa, a);
						(*changes)++;
						exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
						return ne;
					}
				}
			}
			/* move constants to the right, ie c*A = A*c */
			/* TODO beware of overflow,  */
			else if (exp_is_atom(le)) {
				l->h->data = re;
				l->h->next->data = le;
				(*changes)++;
				return e;
			}
			/* change a*a into pow(a,2), later change pow(a,2) back into a*a */
			if (exp_equal(le, re)==0 && exp_subtype(le)->type->eclass == EC_FLT) {
				/* pow */
				list *l;
				sql_exp *ne;
				sql_subfunc *pow = sql_bind_func(sql->sa, sql->session->schema, "power", exp_subtype(le), exp_subtype(re), F_FUNC);
				assert(pow);
				if (exp_subtype(le)->type->localtype == TYPE_flt)
					re = exp_atom_flt(sql->sa, 2);
				else
					re = exp_atom_dbl(sql->sa, 2);
				l = sa_list(sql->sa);
				append(l, le);
				append(l, re);
				(*changes)++;
				ne = exp_op(sql->sa, l, pow);
				exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
				return ne;
			}
			/* change a*pow(a,n) or pow(a,n)*a into pow(a,n+1) */
			if (is_func(le->type)) { 
				list *l = le->l;
				sql_subfunc *f = le->f;

				if (!f->func->s && !strcmp(f->func->base.name, "power") && list_length(l) == 2) {
					sql_exp *lle = l->h->data;
					sql_exp *lre = l->h->next->data;
					if (exp_equal(re, lle)==0) {
						if (atom_inc(exp_value(sql, lre, sql->args, sql->argc))) {
							(*changes)++;
							exp_setname(sql->sa, le, exp_relname(e), exp_name(e));
							return le;
						}
					}
				}
				if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && list_length(l) == 2) {
					sql_exp *lle = l->h->data;
					sql_exp *lre = l->h->next->data;
					if (!exp_is_atom(lle) && exp_is_atom(lre) && exp_is_atom(re)) {
						/* (x*c1)*c2 -> x * (c1*c2) */
						list *l = sa_list(sql->sa);
						append(l, lre);
						append(l, re);
						le->l = l;
						l = e->l;
						l->h->data = lle;
						l->h->next->data = le;
						(*changes)++;
						return e;
					}
				}
			}
		}
		if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;
			if (exp_is_atom(le) && exp_is_zero(sql, le)) {
				(*changes)++;
				exp_setname(sql->sa, re, exp_relname(e), exp_name(e));
				return re;
			}
			if (exp_is_atom(re) && exp_is_zero(sql, re)) {
				(*changes)++;
				exp_setname(sql->sa, le, exp_relname(e), exp_name(e));
				return le;
			}
			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, le);
				atom *ra = exp_flatten(sql, re);

				if (la && ra) {
					atom *a = atom_add(la, ra);

					if (a) {
						sql_exp *ne = exp_atom(sql->sa, a);
						(*changes)++;
						exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
						return ne;
					}
				}
			}
			/* move constants to the right, ie c+A = A+c */
			else if (exp_is_atom(le)) {
				l->h->data = re;
				l->h->next->data = le;
				(*changes)++;
				return e;
			} else if (is_func(le->type)) {
				list *ll = le->l;
				sql_subfunc *f = le->f;
				if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(ll) == 2) {
					sql_exp *lle = ll->h->data;
					sql_exp *lre = ll->h->next->data;

					if (exp_is_atom(lle) && exp_is_atom(lre))
						return e;
					if (!exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)+y -> (x+y) + c1 */
						ll->h->next->data = re; 
						l->h->next->data = lre;
						l->h->data = exp_simplify_math(sql, le, changes);
						(*changes)++;
						return e;
					}
					if (exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)+c2 -> (c2+c1) + x */
						ll->h->data = re; 
						l->h->next->data = lle;
						l->h->data = exp_simplify_math(sql, le, changes);
						(*changes)++;
						return e;
					}
				}
			}
			/*
			if (is_func(re->type)) {
				list *ll = re->l;
				sql_subfunc *f = re->f;
				if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(ll) == 2) {
					if (exp_is_atom(le)) {
						* c1+(x+y) -> (x+y) + c1 *
						l->h->data = re;
						l->h->next->data = le;
						(*changes)++;
						return e;
					}
				}
			}
			*/
		}
		if (!f->func->s && !strcmp(f->func->base.name, "sql_sub") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;

			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, le);
				atom *ra = exp_flatten(sql, re);

				if (la && ra) {
					atom *a = atom_sub(la, ra);

					if (a) {
						sql_exp *ne = exp_atom(sql->sa, a);
						(*changes)++;
						exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
						return ne;
					}
				}
			}
			if (exp_equal(le,re) == 0) { /* a - a = 0 */
				atom *a;
				sql_exp *ne;

				if (exp_subtype(le)->type->eclass == EC_NUM) {
					a = atom_int(sql->sa, exp_subtype(le), 0);
				} else if (exp_subtype(le)->type->eclass == EC_FLT) {
					a = atom_float(sql->sa, exp_subtype(le), 0);
				} else {
					return e;
				}
				ne = exp_atom(sql->sa, a);
				(*changes)++;
				exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
				return ne;
			}
			if (is_func(le->type)) {
				list *ll = le->l;
				sql_subfunc *f = le->f;
				if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(ll) == 2) {
					sql_exp *lle = ll->h->data;
					sql_exp *lre = ll->h->next->data;
					if (exp_equal(re, lre) == 0) {
						/* (x+a)-a = x*/
						exp_setname(sql->sa, lle, exp_relname(e), exp_name(e));
						(*changes)++;
						return lle;
					}
					if (exp_is_atom(lle) && exp_is_atom(lre))
						return e;
					if (!exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)-y -> (x-y) + c1 */
						ll->h->next->data = re; 
						l->h->next->data = lre;
						le->f = e->f;
						e->f = f;
						l->h->data = exp_simplify_math(sql, le, changes);
						(*changes)++;
						return e;
					}
					if (exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)-c2 -> (c1-c2) + x */
						ll->h->data = lre; 
						ll->h->next->data = re; 
						l->h->next->data = lle;
						le->f = e->f;
						e->f = f;
						l->h->data = exp_simplify_math(sql, le, changes);
						(*changes)++;
						return e;
					}
				}
			}
		}
		if (l)
			for (n = l->h; n; n = n->next) 
				n->data = exp_simplify_math(sql, n->data, changes);
	}
	if (e->type == e_convert)
		e->l = exp_simplify_math(sql, e->l, changes);
	return e;
}

static sql_rel *
rel_simplify_math(int *changes, mvc *sql, sql_rel *rel) 
{
	
	if ((is_project(rel->op) || (rel->op == op_ddl && rel->flag == DDL_PSM)) && rel->exps) {
		list *exps = rel->exps;
		node *n;
		int needed = 0;

		for (n = exps->h; n && !needed; n = n->next) { 
			sql_exp *e = n->data;

			if (e->type == e_func || e->type == e_convert ||
			    e->type == e_aggr || e->type == e_psm) 
				needed = 1;
		}
		if (!needed)
			return rel;

		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = exp_simplify_math( sql, n->data, changes);
		
			if (!e) 
				return NULL;
			list_append(rel->exps, e);
		}
	} 
	if (*changes) /* if rewritten don't cache this query */
		sql->caching = 0;
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
		ls->exps = append(new_exp_list(sql->sa), exp_or(sql->sa, ls->exps, rs->exps, 0));
		rs->exps = NULL;
		rel = rel_inplace_project(sql->sa, rel, rel_dup(rel->l), rel->exps);
		set_processed(rel);
		return rel;
	}
	return rel;
}

static int
exps_cse( mvc *sql, list *oexps, list *l, list *r )
{
	list *nexps;
	node *n, *m;
	char *lu, *ru;
	int lc = 0, rc = 0, match = 0, res = 0;

	/* first recusive exps_cse */
	nexps = new_exp_list(sql->sa);
	for (n = l->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or) {
			res = exps_cse(sql, nexps, e->l, e->r); 
		} else {
			append(nexps, e);
		}
	}
	l = nexps;

	nexps = new_exp_list(sql->sa);
	for (n = r->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or) {
			res = exps_cse(sql, nexps, e->l, e->r); 
		} else {
			append(nexps, e);
		}
	}
	r = nexps;

	/* simplify  true or .. and .. or true */
	if (list_length(l) == list_length(r) && list_length(l) == 1) {
		sql_exp *le = l->h->data, *re = r->h->data;

		if (exp_is_true(sql, le)) {
			append(oexps, le);
			return 1;
		}
		if (exp_is_true(sql, re)) {
			append(oexps, re);
			return 1;
		}
	}

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
		list *nl = new_exp_list(sql->sa);
		list *nr = new_exp_list(sql->sa);

		for (n = l->h, lc = 0; n; n = n->next, lc++) 
			if (!lu[lc])
				append(nl, n->data);
		for (n = r->h, rc = 0; n; n = n->next, rc++) 
			if (!ru[rc])
				append(nr, n->data);

		if (list_length(nl) && list_length(nr)) 
			append(oexps, exp_or(sql->sa, nl, nr, 0)); 

		for (n = l->h, lc = 0; n; n = n->next, lc++) {
			if (lu[lc])
				append(oexps, n->data);
		}
		res = 1;
	} else {
		append(oexps, exp_or(sql->sa, list_dup(l, (fdup)NULL), 
				     list_dup(r, (fdup)NULL), 0));
	}
	free(lu);
	free(ru);
	return res;
}

static int
are_equality_exps( list *exps, sql_exp **L) 
{
	sql_exp *l = *L;

	if (list_length(exps) == 1) {
		sql_exp *e = exps->h->data, *le = e->l, *re = e->r;

		if (e->type == e_cmp && e->flag == cmp_equal && le->card != CARD_ATOM && re->card == CARD_ATOM) {
			if (!l) {
				*L = l = le;
				if (!is_column(le->type))
					return 0;
			}
			return (exp_match(l, le));
		}
		if (e->type == e_cmp && e->flag == cmp_or)
			return (are_equality_exps(e->l, L) && 
				are_equality_exps(e->r, L));
	}
	return 0;
}

static void 
get_exps( list *n, list *l )
{
	sql_exp *e = l->h->data, *re = e->r;

	if (e->type == e_cmp && e->flag == cmp_equal && re->card == CARD_ATOM)
		list_append(n, re);
	if (e->type == e_cmp && e->flag == cmp_or) {
		get_exps(n, e->l);
		get_exps(n, e->r);
	}
}

static sql_exp *
equality_exps_2_in( mvc *sql, sql_exp *ce, list *l, list *r)
{
	list *nl = new_exp_list(sql->sa);

	get_exps(nl, l);
	get_exps(nl, r);

	return exp_in( sql->sa, ce, nl, cmp_in);
}

static sql_rel *
rel_select_cse(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)sql;
	if (is_select(rel->op) && rel->exps) { 
		node *n;
		list *nexps;
		int needed = 0;

		for (n=rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or) 
				needed = 1;
		}
		if (!needed)
			return rel;

 		nexps = new_exp_list(sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *l = NULL;

			if (e->type == e_cmp && e->flag == cmp_or && are_equality_exps(e->l, &l) && are_equality_exps(e->r, &l) && l) {
				(*changes)++;
				append(nexps, equality_exps_2_in(sql, l, e->l, e->r));
			} else {
				append(nexps, e);
			}
		}
		rel->exps = nexps;
	}
	if ((is_select(rel->op) || is_join(rel->op)) && rel->exps) { 
		node *n;
		list *nexps;
		int needed = 0;

		for (n=rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or) 
				needed = 1;
		}
		if (!needed)
			return rel;
 		nexps = new_exp_list(sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or) {
				/* split the common expressions */
				*changes += exps_cse(sql, nexps, e->l, e->r);
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
	if (is_project(rel->op) && rel->exps) { 
		node *n, *m;
		list *nexps;
		int needed = 0;

		for (n=rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e1 = n->data;

			if (e1->type != e_column && e1->type != e_atom && e1->name) {
				for (m=n->next; m; m = m->next){
					sql_exp *e2 = m->data;
				
					if (exp_name(e2) && exp_match_exp(e1, e2)) 
						needed = 1;
				}
			}
		}

		if (!needed)
			return rel;

		nexps = new_exp_list(sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e1 = n->data;

			if (e1->type != e_column && e1->type != e_atom && e1->name) {
				for (m=nexps->h; m; m = m->next){
					sql_exp *e2 = m->data;
				
					if (exp_name(e2) && exp_match_exp(e1, e2)) {
						sql_exp *ne = exp_alias(sql->sa, e1->rname, exp_name(e1), e2->rname, exp_name(e2), exp_subtype(e2), e2->card, has_nil(e2), is_intern(e1));
						if (e2->p)
							ne->p = prop_copy(sql->sa, e2->p);
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
			} else if (le->f && re->f && /* merge ranges */
				   le->flag == re->flag && le->flag <= cmp_lt) {
				sql_subfunc *min = sql_bind_func(sql->sa, sql->session->schema, "sql_min", exp_subtype(le->r), exp_subtype(re->r), F_FUNC);
				sql_subfunc *max = sql_bind_func(sql->sa, sql->session->schema, "sql_max", exp_subtype(le->f), exp_subtype(re->f), F_FUNC);
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
 *     ((x = a and y > 1 and y < 5) or 
 *      (x = c and y > 1 and y < 10) or 
 *      (x = e and y > 1 and y < 20)) and
 *     	 x in (a,c,e) and
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

/* find in the list of expression an expression which uses e */ 
static sql_exp *
exp_uses_exp( list *exps, sql_exp *e)
{
	node *n;
	const char *rname = exp_relname(e);
	const char *name = exp_name(e);

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
		ul = rel_groupby(sql, ul, NULL);
		ul->r = lgbe;
		ul->nrcols = g->nrcols;
		ul->card = g->card;
		ul->exps = exps_copy(sql->sa, g->exps);

		ur = rel_groupby(sql, ur, NULL);
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

				if (find_prop(gbe->p, PROP_HASHCOL)) {
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
		set_processed(u);

		if (rel->r) {
			list *ogbe = rel->r;

			gbe = new_exp_list(sql->sa);
			for (n = ogbe->h; n; n = n->next) { 
				sql_exp *e = n->data, *ne;

				ne = exp_uses_exp( rel->exps, e);
				assert(ne);
				ne = list_find_exp( u->exps, ne);
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
				if (/* DISABLES CODE */ (0) && cnt)
					ne->p = prop_create(sql->sa, PROP_COUNT, ne->p);
			} else {
				ne = exp_copy(sql->sa, oa);
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
 * More general 
 * 	groupby(
 * 	 [ outer ] join(
 * 	    project(
 * 	      table(A) [ c1, c2, .. ] 
 * 	    ) [ c1, c2, identity(c2) as I, .. ], 
 * 	    table(B) [ c1, c2, .. ]
 * 	  ) [ A.c1 = B.c1 ]
 * 	) [ I ] [ a1, a2, .. ]
 *
 * ->
 *
 * 	[ outer ] join(
 * 	  project(
 * 	    table(A) [ c1, c2, .. ] 
 * 	  ) [ c1, c2, .. ], 
 * 	  groupby (
 * 	    table(B) [ c1, c2, .. ]
 * 	  ) [ B.c1 ] [ a1, a2, .. ]
 * 	) [ A.c1 = B.c1 ]
 */
static sql_rel *
gen_push_groupby_down(int *changes, mvc *sql, sql_rel *rel) 
{
	sql_rel *j = rel->l;
	list *gbe = rel->r;

	(void)changes;
	if (rel->op == op_groupby && list_length(gbe) == 1 && is_outerjoin(j->op)) {
		sql_rel *jl = j->l, *jr = j->r, *cr;
		sql_exp *gb = gbe->h->data, *e;
		node *n;
		int left = 1;
		list *aggrs, *gbe;

		if (jl->op == op_project &&
		    (e = list_find_exp( jl->exps, gb)) != NULL &&
		     find_prop(e->p, PROP_HASHCOL) != NULL) {
			left = 0;
			cr = jr;
		} else if (jr->op == op_project &&
		    (e = list_find_exp( jr->exps, gb)) != NULL &&
		     find_prop(e->p, PROP_HASHCOL) != NULL) {
			left = 1;
			cr = jl;
		} else {
			return rel;
		}

		/* only add aggr (based on left/right), and repeat the group by column */
		aggrs = sa_list(sql->sa);
		if (rel->exps) for (n = rel->exps->h; n; n = n->next) {
			sql_exp *ce = n->data;

			if (ce->type == e_aggr) {
				list *args = ce->l;

				/* check args are part of left/right */
				if (!list_empty(args) && rel_has_exps(cr, args) < 0)
					return rel;
				if (rel->op != op_join && strcmp(((sql_subaggr*)ce->f)->aggr->base.name, "count") == 0)
					ce->p = prop_create(sql->sa, PROP_COUNT, ce->p);
				list_append(aggrs, ce); 
			}
		}

		/* find gb in left or right and should be unique */
		gbe = sa_list(sql->sa);
		/* push groupby to right, group on join exps */
		if (j->exps) for (n = j->exps->h; n; n = n->next) {
			sql_exp *ce = n->data, *e;

			/* get left/right hand of e_cmp */
			assert(ce->type == e_cmp);
			if (ce->flag != cmp_equal)
				return rel;
			e = rel_find_exp(cr, ce->l);
			if (!e)
				e = rel_find_exp(cr, ce->r);
			if (!e)
				return rel;
			e = exp_alias(sql->sa, e->rname, exp_name(e), e->rname, exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			list_append(gbe, e);
		}
		if (!left) 
			cr = j->r = rel_groupby(sql, cr, gbe);
		else 
			cr = j->l = rel_groupby(sql, cr, gbe);
		cr->exps = list_merge( cr->exps, aggrs, (fdup)NULL);
		rel -> l = NULL;
		rel_destroy(rel);
		return j;
	}
	return rel;
}

/*
 * Rewrite group(project(join(A,Dict)[a.i==dict.i])[...dict.n])[dict.n][ ... dict.n ]
 * into
 * 	project(join(groupby (A)[a.i],[a.i]), Dict)[a.i==dict.i])[dict.n] 
 *
 */
static sql_rel *
rel_push_groupby_down(int *changes, mvc *sql, sql_rel *rel) 
{
	sql_rel *p = rel->l;
	list *gbe = rel->r;

	if (rel->op == op_groupby && gbe && p && p->op == op_project) {
		sql_rel *j = p->l;
		sql_rel *jl, *jr;
		node *n;

		if (!j || j->op != op_join || list_length(j->exps) != 1)
			return gen_push_groupby_down(changes, sql, rel);
		jl = j->l;
		jr = j->r;

		/* check if jr is a dict with index and var still used */
		if (jr->op != op_basetable || jr->l || !jr->r || list_length(jr->exps) != 2) 
			return gen_push_groupby_down(changes, sql, rel);

		/* check if group by is done on dict column */
		for(n = gbe->h; n; n = n->next) {
			sql_exp *ge = n->data, *pe = NULL, *e = NULL;

			/* find group by exp in project, then in dict */
			pe = rel_find_exp(p, ge);
			if (pe) /* find project exp in right hand of join, ie dict */
				e = rel_find_exp(jr, pe);
			if (pe && e) {  /* Rewrite: join with dict after the group by */
				list *pexps = rel_projections(sql, rel, NULL, 1, 1), *npexps;
				node *m;
				sql_exp *ne = j->exps->h->data; /* join exp */
				p->l = jl;	/* Project now only on the left side of the join */

				ne = ne->l; 	/* The left side of the compare is the index of the left */

				/* find ge reference in new projection list */
				npexps = sa_list(sql->sa);
				for (m = pexps->h; m; m = m->next) {
					sql_exp *a = m->data;

					if (exp_refers(ge, a)) { 
						sql_exp *sc = jr->exps->t->data;
						sql_exp *e = exp_column(sql->sa, exp_relname(sc), exp_name(sc), exp_subtype(sc), sc->card, has_nil(sc), is_intern(sc));
						exp_setname(sql->sa, e, exp_relname(a), exp_name(a));
						a = e;
					}
					append(npexps, a);
				}

				/* find ge in aggr list */
				for (m = rel->exps->h; m; m = m->next) {
					sql_exp *a = m->data;

					if (exp_match_exp(a, ge) || exp_refers(ge, a)) {
						a = exp_column(sql->sa, exp_relname(ne), exp_name(ne), exp_subtype(ne), ne->card, has_nil(ne), is_intern(ne));
						exp_setname(sql->sa, a, exp_relname(ne), exp_name(ne));
						m->data = a;
					}
				}

				/* change alias pe, ie project out the index  */
				pe->l = (void*)exp_relname(ne); 
				pe->r = (void*)exp_name(ne);
				exp_setname(sql->sa, pe, exp_relname(ne), exp_name(ne));

				/* change alias ge */
				ge->l = (void*)exp_relname(pe); 
				ge->r = (void*)exp_name(pe);
				exp_setname(sql->sa, ge, exp_relname(pe), exp_name(pe));

				/* zap both project and groupby name hash tables (as we changed names above) */
				rel->exps->ht = NULL;
				((list*)rel->r)->ht = NULL;
				p->exps->ht = NULL;
				
				/* add join */
				j->l = rel;
				rel = rel_project(sql->sa, j, npexps);
				(*changes)++;
			}
		}
		(void)sql;
	}
	return rel;
}

/*
 * Push select down, pushes the selects through (simple) projections. Also
 * it cleans up the projections which become useless.
 */

/* TODO push select expressions in outer joins down */
static sql_rel *
rel_push_select_down(int *changes, mvc *sql, sql_rel *rel) 
{
	list *exps = NULL;
	sql_rel *r = NULL;
	node *n;

	if (rel_is_ref(rel)) {
		if (is_select(rel->op) && rel->exps) {
			/* add inplace empty select */
			sql_rel *l = rel_select(sql->sa, rel->l, NULL);

			if (!l->exps)
				l->exps = sa_list(sql->sa);
			(void)list_merge(l->exps, rel->exps, (fdup)NULL);
			rel->exps = NULL;
			rel->l = l;
			(*changes)++;
		}
		return rel;
	}

	/* don't make changes for empty selects */
	if (is_select(rel->op) && (!rel->exps || list_length(rel->exps) == 0)) 
		return rel;

	/* merge 2 selects */
	r = rel->l;
	if (is_select(rel->op) && r && r->exps && is_select(r->op) && !(rel_is_ref(r))) {
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
	if (is_select(rel->op) && r && (is_join(r->op) || is_apply(r->op)) && !(rel_is_ref(r))) {
		sql_rel *jl = r->l;
		sql_rel *jr = r->r;
		int left = r->op == op_join || r->op == op_left;
		int right = r->op == op_join || r->op == op_right;

		if (is_apply(r->op)) {
			left = right = 1;
			if (r->flag == APPLY_LOJ)
				right = 0;
		}
		if (r->op == op_full)
			return rel;

		/* introduce selects under the join (if needed) */
		set_processed(jl);
		set_processed(jr);
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
				rel_select_add_exp(sql->sa, jl, ne);
			} else if (right) {
				ne = exp_push_down(sql, e, jr, jr);
				if (ne && ne != e) {
					done = 1; 
					rel_select_add_exp(sql->sa, jr, ne);
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
		set_processed(pl);
		if (!is_select(pl->op) || rel_is_ref(pl))
			r->l = pl = rel_select(sql->sa, pl, NULL);

		/* for each exp check if we can rename it */
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data, *ne = NULL;

			if (e->type == e_cmp) {
				ne = exp_push_down_prj(sql, e, r, pl);

				/* can we move it down */
				if (ne && ne != e && pl->exps) {
					rel_select_add_exp(sql->sa, pl, ne);
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
	if (is_select(rel->op) && exps && r && r->op == op_join && !(rel_is_ref(r))) {
		rel->exps = new_exp_list(sql->sa); 
		for (n = exps->h; n; n = n->next) { 
			sql_exp *e = n->data;
			if (e->type == e_cmp && !e->f && !is_complex_exp(e->flag)) {
				sql_rel *nr = NULL;
				sql_exp *re = e->r, *ne = rel_find_exp(r, re);

				if (ne && ne->card >= CARD_AGGR) /* possibly changed because of apply rewrites */
					re->card = ne->card;

				if (re->card >= CARD_AGGR) {
					nr = rel_push_join(sql, r, e->l, re, NULL, e);
				} else {
					nr = rel_push_select(sql, r, e->l, e);
				}
				if (nr)
					rel->l = nr;
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

	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op) || is_project(rel->op) || is_topn(rel->op) || is_sample(rel->op)) && rel->l) {
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
 * under the group by. This should only be done, iff the new semijoin would
 * reduce the input table to the groupby. So there should be a reduction 
 * (selection) on the table A and this should be propagated to the groupby via
 * for example a primary key.
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
	if (!rel_is_ref(rel) && ((is_join(rel->op) || is_semi(rel->op)) && rel->l && rel->exps)) {
		sql_rel *gb = rel->r, *ogb = gb, *l = NULL, *rell = rel->l;

		if (gb->op == op_project)
			gb = gb->l;

		if (is_basetable(rell->op) || rel_is_ref(rell))
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
				const char *rname = NULL, *name = NULL;

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

				if (!name) 
					return rel;

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
							sql_exp *le = je->l;
							sql_exp *re = exp_push_down_prj(sql, r, gb, gb->l);
							if (!re || (list_length(jes) == 0 && !find_prop(le->p, PROP_HASHCOL))) {
								fnd = 0;
							} else {
								je = exp_compare(sql->sa, le, re, je->flag);
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
 *
 * also push simple expressions of a semijoin down if they only
 * involve the left sided of the semijoin.
 */
static sql_rel *
rel_push_semijoin_down(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)*changes;

	/* first push down the expressions involving only A */
	if (is_semi(rel->op) && rel->exps && rel->l) {
		list *exps = rel->exps, *nexps = sa_list(sql->sa);
		node *n;

		for(n = exps->h; n; n = n->next) {
			sql_exp *sje = n->data;

			if (n != exps->h && sje->type == e_cmp &&
			    !is_complex_exp(sje->flag) &&
			     rel_has_exp(rel->l, sje->l) >= 0 &&
			     rel_has_exp(rel->l, sje->r) >= 0) {
				rel->l = rel_select(sql->sa, rel->l, NULL);
				rel_select_add_exp(sql->sa, rel->l, sje);
			} else {
				append(nexps, sje);
			}
		} 
		rel->exps = nexps;
	}
	if (is_semi(rel->op) && rel->exps && rel->l) {
		operator_type op = rel->op, lop;
		node *n;
		sql_rel *l = rel->l, *ll = NULL, *lr = NULL;
		sql_rel *r = rel->r;
		list *exps = rel->exps, *nsexps, *njexps;
		int left = 1, right = 1;

		/* handle project 
		if (l->op == op_project && !need_distinct(l))
			l = l->l;
		*/

		if (!is_join(l->op) || rel_is_ref(l))
			return rel;

		lop = l->op;
		ll = l->l;
		lr = l->r;
		/* semijoin shouldn't be based on right relation of join */
		for(n = exps->h; n; n = n->next) {
			sql_exp *sje = n->data;

			if (sje->type != e_cmp)
				return rel;
			if (right &&
				(is_complex_exp(sje->flag) || 
			    	rel_has_exp(lr, sje->l) >= 0 ||
			    	rel_has_exp(lr, sje->r) >= 0)) {
				right = 0;
			}
			if (right)
				left = 0;
			if (!right && left &&
				(is_complex_exp(sje->flag) || 
			    	rel_has_exp(ll, sje->l) >= 0 ||
			    	rel_has_exp(ll, sje->r) >= 0)) {
				left = 0;
			}
			if (!right && !left)
				return rel;
		} 
		nsexps = exps_copy(sql->sa, rel->exps);
		njexps = exps_copy(sql->sa, l->exps);
		if (right)
			l = rel_crossproduct(sql->sa, rel_dup(ll), rel_dup(r), op);
		else
			l = rel_crossproduct(sql->sa, rel_dup(lr), rel_dup(r), op);
		l->exps = nsexps;
		if (right)
			l = rel_crossproduct(sql->sa, l, rel_dup(lr), lop);
		else
			l = rel_crossproduct(sql->sa, l, rel_dup(ll), lop);
		l->exps = njexps;
		rel_destroy(rel);
		rel = l;
	}
	return rel;
}

static int
rel_is_join_on_pkey( sql_rel *rel ) 
{
	node *n;

	if (!rel || !rel->exps)
		return 0;
	for (n = rel->exps->h; n; n = n->next){
		sql_exp *je = n->data;

		if (je->type == e_cmp && je->flag == cmp_equal &&
		    find_prop(((sql_exp*)je->l)->p, PROP_HASHCOL)) { /* aligned PKEY JOIN */
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
		return list_position(pp->p->members.set, pp);
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
		if (pp->p && list_position(pp->p->members.set, pp) == pnr)
			return 1;
	}
	/* for projects we may need to do a rename! */
	if (is_project(rel->op) || is_topn(rel->op) || is_sample(rel->op))
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
	if ((is_join(rel->op) && !is_outerjoin(rel->op)) || is_semi(rel->op)) {
		sql_rel *l = rel->l, *r = rel->r, *ol = l, *or = r;
		list *exps = rel->exps;
		sql_exp *je = !list_empty(exps)?exps->h->data:NULL;

		if (!l || !r || need_distinct(l) || need_distinct(r))
			return rel;
		if (l->op == op_project)
			l = l->l;
		if (r->op == op_project)
			r = r->l;

		/* both sides only if we have a join index */
		if (!l || !r ||(is_union(l->op) && is_union(r->op) && 
			je && !find_prop(je->p, PROP_JOINIDX) && /* FKEY JOIN */
			!rel_is_join_on_pkey(rel))) /* aligned PKEY JOIN */
			return rel;
		if (is_semi(rel->op) && is_union(l->op) && je && !find_prop(je->p, PROP_JOINIDX))
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
			nl = rel_project(sql->sa, nl, rel_projections(sql, nl, NULL, 1, 1));
			nr = rel_project(sql->sa, nr, rel_projections(sql, nr, NULL, 1, 1));
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
			nl = rel_project(sql->sa, nl, rel_projections(sql, nl, NULL, 1, 1));
			nr = rel_project(sql->sa, nr, rel_projections(sql, nr, NULL, 1, 1));
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
			nl = rel_project(sql->sa, nl, rel_projections(sql, nl, NULL, 1, 1));
			nr = rel_project(sql->sa, nr, rel_projections(sql, nr, NULL, 1, 1));
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
	if ((is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
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
	} else if ((is_project(rel->op) || is_topn(rel->op) || is_select(rel->op)
				|| is_sample(rel->op)) && rel->l) {
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
		set_processed(ul);
		set_processed(ur);
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

static sql_rel *
rel_push_project_down_union(int *changes, mvc *sql, sql_rel *rel) 
{
	/* first remove distinct if already unique */
	if (rel->op == op_project && need_distinct(rel) && rel->exps && exps_unique(rel->exps))
		set_nodistinct(rel);

	if (rel->op == op_project && rel->l && rel->exps && !rel->r) {
		int need_distinct = need_distinct(rel);
		sql_rel *u = rel->l;
		sql_rel *p = rel;
		sql_rel *ul = u->l;
		sql_rel *ur = u->r;

		if (!u || !is_union(u->op) || need_distinct(u) || !u->exps || rel_is_ref(u) || project_unsafe(rel))
			return rel;
		/* don't push project down union of single values */
		if ((is_project(ul->op) && !ul->l) || (is_project(ur->op) && !ur->l))
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
		rel->l = rel_merge_projects(changes, sql, rel->l);
		rel->r = rel_merge_projects(changes, sql, rel->r);
		return rel;
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

		for (i = 0, n = gbe->h; n; i++, n = n->next) 
			scores[i] = score_gbe(sql, rel, n->data);
		rel->r = list_keysort(gbe, scores, (fdup)NULL);
		free(scores);
	}
	return rel;
}


/* reduce group by expressions based on pkey info 
 *
 * The reduced group by and (derived) aggr expressions are restored via
 * extra (new) aggregate columns.
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
		if (scores == NULL || tbls == NULL || bts == NULL) {
			if (scores)
				free(scores);
			if (tbls)
				free(tbls);
			if (bts)
				free(bts);
			return NULL;
		}
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
			/* TODO also remove group by columns which are related to
			 * the other columns using a foreign-key join (n->1), ie 1
			 * on the to be removed side.
			 */
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
						   (c == exp_find_column(rel, e, -2))) && !find_prop(e->p, PROP_HASHCOL)))
							e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
					}
					for (m = rel->exps->h; m; m = m->next ){
						sql_exp *e = m->data;

						for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
							sql_exp *gb = n->data;

							/* pkey based group by */
							if (scores[l] == 1 && exp_match_exp(e,gb) && find_prop(gb->p, PROP_HASHCOL) && !find_prop(e->p, PROP_HASHCOL)) {
								e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);
								break;
							}

						}
					}
				}
				if (cnr && nr && list_length(tbls[j]->pkey->k.columns) == nr) {
					list *ngbe = new_exp_list(sql->sa);
					list *exps = rel->exps, *nexps = new_exp_list(sql->sa);

					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						/* keep the group by columns which form a primary key
						 * of this table. And those unrelated to this table. */
						if (scores[l] != -1) 
							append(ngbe, e); 
					}
					rel->r = ngbe;
					/* rewrite gbe and aggr, in the aggr list */
					for (m = exps->h; m; m = m->next ){
						sql_exp *e = m->data;
						int fnd = 0;

						for (l = 0, n = gbe->h; l < k && n && !fnd; l++, n = n->next) {
							sql_exp *gb = n->data;

							if (scores[l] == -1 && exp_refers(gb, e)) {
								sql_exp *rs = exp_column(sql->sa, gb->l?gb->l:exp_relname(gb), gb->r?gb->r:exp_name(gb), exp_subtype(gb), rel->card, has_nil(gb), is_intern(gb));
								exp_setname(sql->sa, rs, exp_find_rel_name(e), exp_name(e));
								e = rs;
								fnd = 1;
							}
						}
						append(nexps, e);
					}
					/* new reduced aggr expression list */
					assert(list_length(nexps)>0);
					rel->exps = nexps;
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
	/* remove constants from group by list */
	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel)) {
		int i;
		node *n;
		
		for (i = 0, n = gbe->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (exp_is_atom(e))
				i++;
		}
		if (i) {
			list *ngbe = new_exp_list(sql->sa);
			list *dgbe = new_exp_list(sql->sa);

			for (n = gbe->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (!exp_is_atom(e))
					append(ngbe, e);
				/* we need at least one gbe */
				else if (!n->next && list_empty(ngbe))
					append(ngbe, e);
				else
					append(dgbe, e);
			}
			rel->r = ngbe;
			if (!list_empty(dgbe)) {
				/* use atom's directly in the aggr expr list */
				list *nexps = new_exp_list(sql->sa);

				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *ne = NULL;

					if (e->type == e_column) {
						if (e->l) 
							ne = exps_bind_column2(dgbe, e->l, e->r);
						else
							ne = exps_bind_column(dgbe, e->r, NULL);
						if (ne) {
							ne = exp_copy(sql->sa, ne);
							exp_setname(sql->sa, ne, e->rname, e->name);
							e = ne;
						}
					}
					append(nexps, e);
				}
				rel->exps = nexps;
				(*changes)++;
			}
		}
	}
	return rel;
}

/* Rewrite group by expressions with distinct 
 *
 * ie select a, count(distinct b) from c where ... groupby a;
 * No other aggregations should be present
 *
 * Rewrite the more general case, good for parallel execution
 *
 * groupby(R) [e,f] [ aggr1 a distinct, aggr2 b distinct, aggr3 c, aggr4 d]
 *
 * into
 *
 * groupby(
 * 	groupby(R) [e,f,a,b] [ a, b, aggr3 c, aggr4 d]
 * ) [e,f]( aggr1 a distinct, aggr2 b distinct, aggr3_phase2 c, aggr4_phase2 d)
 */

static sql_rel *
rel_groupby_distinct2(int *changes, mvc *sql, sql_rel *rel) 
{
	list *ngbes = sa_list(sql->sa), *gbes, *naggrs = sa_list(sql->sa), *aggrs = sa_list(sql->sa);
	sql_rel *l;
	node *n;

	gbes = rel->r;
	if (!gbes) 
		return rel;

	/* check if each aggr is, rewritable (max,min,sum,count) 
	 *  			  and only has one argument */
	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subaggr *af = e->f;

		if (e->type == e_aggr && 
		   (strcmp(af->aggr->base.name, "sum") && 
		     strcmp(af->aggr->base.name, "count") &&
		     strcmp(af->aggr->base.name, "min") &&
		     strcmp(af->aggr->base.name, "max"))) 
			return rel; 
	}

	for (n = gbes->h; n; n = n->next) {
		sql_exp *e = n->data;

		e = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
		append(ngbes, e);
	}

	/* 1 for each aggr(distinct v) add the attribute expression v to gbes and aggrs list
	 * 2 for each aggr(z) add aggr_phase2('z') to the naggrs list 
	 * 3 for each group by col, add also to the naggrs list 
	 * */
	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_aggr && need_distinct(e)) { /* 1 */
			/* need column expression */
			list *args = e->l;
			sql_exp *v = args->h->data;
			append(gbes, v);
			if (!exp_name(v))
				exp_label(sql->sa, v, ++sql->label);
			v = exp_column(sql->sa, exp_find_rel_name(v), exp_name(v), exp_subtype(v), v->card, has_nil(v), is_intern(v));
			append(aggrs, v);
			v = exp_aggr1(sql->sa, v, e->f, need_distinct(e), 1, e->card, 1);
			exp_setname(sql->sa, v, exp_find_rel_name(e), exp_name(e));
			append(naggrs, v);
		} else if (e->type == e_aggr && !need_distinct(e)) {
			sql_exp *v;
			sql_subaggr *f = e->f;
			int cnt = strcmp(f->aggr->base.name,"count")==0;
			sql_subaggr *a = sql_bind_aggr(sql->sa, sql->session->schema, (cnt)?"sum":f->aggr->base.name, exp_subtype(e));

			append(aggrs, e);
			if (!exp_name(e))
				exp_label(sql->sa, e, ++sql->label);
			set_has_nil(e);
			v = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			v = exp_aggr1(sql->sa, v, a, 0, 1, e->card, 1);
			if (cnt)
				set_zero_if_empty(v);
			exp_setname(sql->sa, v, exp_find_rel_name(e), exp_name(e));
			append(naggrs, v);
		} else { /* group by col */
			if (list_find_exp(gbes, e) || !list_find_exp(naggrs, e)) { 
				append(aggrs, e);
	
				e = exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			}
			append(naggrs, e);
		}
	}

	l = rel->l = rel_groupby(sql, rel->l, gbes);
	l->exps = aggrs;
	rel->r = ngbes;
	rel->exps = naggrs;
	(*changes)++;
	return rel;
}

static sql_rel *
rel_groupby_distinct(int *changes, mvc *sql, sql_rel *rel) 
{
	if (is_groupby(rel->op)) {
		sql_rel *l = rel->l;
		if (!l || is_groupby(l->op))
			return rel;
	}
	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel)) {
		node *n;
		int nr = 0;
		list *gbe, *ngbe, *arg, *exps, *nexps;
		sql_exp *distinct = NULL, *darg;
		sql_rel *l = NULL;

		for (n=rel->exps->h; n && nr <= 2; n = n->next) {
			sql_exp *e = n->data;
			if (need_distinct(e)) {
				distinct = n->data;
				nr++;
			}
		}
		if (nr < 1 || distinct->type != e_aggr)
			return rel;
		if ((nr > 1 || list_length(rel->r) + nr != list_length(rel->exps)))
			return rel_groupby_distinct2(changes, sql, rel);
		arg = distinct->l;
		if (list_length(arg) != 1 || list_length(rel->r) + nr != list_length(rel->exps)) 
			return rel;

		gbe = rel->r;
		ngbe = sa_list(sql->sa);
		exps = sa_list(sql->sa);
		nexps = sa_list(sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (e != distinct) {
				e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
				append(ngbe, e);
				append(exps, e);
				e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
				append(nexps, e);
			}
		}

		darg = arg->h->data;
		list_append(gbe, darg = exp_copy(sql->sa, darg));
		exp_label(sql->sa, darg, ++sql->label);

		darg = exp_column(sql->sa, exp_relname(darg), exp_name(darg), exp_subtype(darg), darg->card, has_nil(darg), is_intern(darg));
		list_append(exps, darg);
		darg = exp_column(sql->sa, exp_relname(darg), exp_name(darg), exp_subtype(darg), darg->card, has_nil(darg), is_intern(darg));
		arg->h->data = darg;
		l = rel->l = rel_groupby(sql, rel->l, gbe);
		l->exps = exps;
		rel->r = ngbe;
		rel->exps = nexps;
		set_nodistinct(distinct);
		append(nexps, distinct);
		(*changes)++;
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
		if (!exp_name(e)) {
			exp_label(sql->sa, e, ++sql->label);
			e->rname = e->name;
		}
		list_append(aexps, e);
		return exp_column(sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
	case e_cmp:
		/* e_cmp's shouldn't exist in an aggr expression list */
		assert(0);
	case e_convert:
		e->l = split_aggr_and_project(sql, aexps, e->l);
		return e;
	case e_func: 
		list_split_aggr_and_project(sql, aexps, e->l);
		return e;
	case e_column: /* constants and columns shouldn't be rewriten */
	case e_atom:
	case e_psm:
		return e;
	}
	return NULL;
}

static sql_exp *
exp_use_consts(mvc *sql, sql_exp *e, list *consts);

static list *
exps_use_consts(mvc *sql, list *exps, list *consts)
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	if (!exps)
		return sa_list(sql->sa);
	for(n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data, *narg = NULL;

		narg = exp_use_consts(sql, arg, consts);
		if (!narg) 
			return NULL;
		if (arg->p)
			narg->p = prop_copy(sql->sa, arg->p);
		append(nl, narg);
	}
	return nl;
}

static sql_exp *
exp_use_consts(mvc *sql, sql_exp *e, list *consts) 
{
	sql_exp *ne = NULL, *l, *r, *r2;

	switch(e->type) {
	case e_column:
		if (e->l) 
			ne = exps_bind_column2(consts, e->l, e->r);
		if (!ne && !e->l)
			ne = exps_bind_column(consts, e->r, NULL);
		if (!ne)
			return e;
		return ne;
	case e_cmp: 
		if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
			list *l = exps_use_consts(sql, e->l, consts);
			list *r = exps_use_consts(sql, e->r, consts);

			if (!l || !r) 
				return NULL;
			if (get_cmp(e) == cmp_filter) 
				return exp_filter(sql->sa, l, r, e->f, is_anti(e));
			return exp_or(sql->sa, l, r, is_anti(e));
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_exp *l = exp_use_consts(sql, e->l, consts);
			list *r = exps_use_consts(sql, e->r, consts);

			if (!l || !r) 
				return NULL;
			return exp_in(sql->sa, l, r, e->flag);
		} else {
			l = exp_use_consts(sql, e->l, consts);
			r = exp_use_consts(sql, e->r, consts);
			if (e->f) {
				r2 = exp_use_consts(sql, e->f, consts);
				if (l && r && r2)
					return exp_compare2(sql->sa, l, r, r2, e->flag);
			} else if (l && r) {
				return exp_compare(sql->sa, l, r, e->flag);
			}
		}
		return NULL;
	case e_convert:
		l = exp_use_consts(sql, e->l, consts);
		if (l)
			return exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		return NULL;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_use_consts(sql, l, consts);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			return exp_op(sql->sa, nl, e->f);
		else 
			return exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
	}	
	case e_atom:
	case e_psm:
		return e;
	}
	return NULL;
}

static list *
exps_remove_dictexps(mvc *sql, list *exps, sql_rel *r)
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	if (!exps)
		return nl;
	for(n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data;

		if (!list_find_exp(r->exps, arg->l) && !list_find_exp(r->exps, arg->r)) 
			append(nl, arg);
	}
	return nl;
}

static sql_rel *
rel_remove_join(int *changes, mvc *sql, sql_rel *rel)
{
	if (is_join(rel->op) && !is_outerjoin(rel->op) && /* DISABLES CODE */ (0)) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		int lconst = 0, rconst = 0;

		if (!l || rel_is_ref(l) || !r || rel_is_ref(r) ||
		   (l->op != op_project && r->op != op_project)) 
			return rel;
		if (l->op == op_project && exps_are_atoms(l->exps))
			lconst = 1;
		if (r->op == op_project && exps_are_atoms(r->exps))
			rconst = 1;
		if (lconst || rconst) {
			(*changes)++;
			/* use constant (instead of alias) in expressions */
			if (lconst) {
				sql_rel *s = l;
				l = r;
				r = s;
			}
			rel->exps = exps_use_consts(sql, rel->exps, r->exps);
			/* change into select */
			rel->op = op_select;
			rel->l = l;
			rel->r = NULL;
			/* wrap in a project including, the constant columns */
			l->subquery = 0;
			rel = rel_project(sql->sa, rel, rel_projections(sql, l, NULL, 1, 1));
			list_merge(rel->exps, r->exps, (fdup)NULL);
		}
	}
	if (is_join(rel->op) && /* DISABLES CODE */ (0)) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		int ldict = 0, rdict = 0;

		if (!l || rel_is_ref(l) || !r || rel_is_ref(r) ||
		   (l->op != op_basetable && r->op != op_basetable)) 
			return rel;
		/* check if dict (last column) isn't used, one column only */
		if (l->op == op_basetable && !l->l && list_length(l->exps) <= 1)
			ldict = 1;
		if (r->op == op_basetable && !r->l && list_length(r->exps) <= 1)
			rdict = 1;
		if (!ldict && !rdict)
			return rel;
		(*changes)++;

		assert(0);
		if (ldict) {
			sql_rel *s = l;
			l = r;
			r = s;
		}
		rel->exps = exps_remove_dictexps(sql, rel->exps, r);
		/* change into select */
		rel->op = op_select;
		rel->l = l;
		rel->r = NULL;
		/* wrap in a project including, the dict/index columns */
		l->subquery = 0;
		rel = rel_project(sql->sa, rel, rel_projections(sql, l, NULL, 1, 1));
		list_merge(rel->exps, r->exps, (fdup)NULL);
	}
	/* project (join (A,B)[ A.x = B.y ] ) [project_cols] -> project (A) [project_cols]
	 * where non of the project_cols are from B and x=y is a foreign key join (B is the unique side)
	 * and there are no filters on B
	 */
	if (0 && is_project(rel->op)) {
		sql_rel *j = rel->l;

		if (is_join(j->op)) {
			node *n;
			sql_rel *l = j->l;
			sql_rel *r = j->r;

			if (!l || rel_is_ref(l) || !r || rel_is_ref(r) || r->op != op_basetable || r->l)
				return rel;

			/* check if all projection cols can be found in l */
			for(n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (!rel_find_exp(l, e))
					return rel;

			}
			assert(0);
			(*changes)++;
			rel->l = l;
			rel->r = NULL;
			l->subquery = 0;
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

		   		/* we cannot rewrite projection with atomic values from outer joins */
				if (is_column(e->type) && exp_is_atom(e) && !(is_right(rel->op) || is_full(rel->op))) {
					list_append(exps, e);
				} else if (e->type == e_column) {
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

		   		/* we cannot rewrite projection with atomic values from outer joins */
				if (is_column(e->type) && exp_is_atom(e) && !(is_left(rel->op) || is_full(rel->op))) {
					list_append(exps, e);
				} else if (e->type == e_column) {
					if (e->name && e->name[0] == 'L')
						return rel;
					list_append(exps, e);
				} else {
					return rel;
				}
			}
		} else if (is_join(rel->op)) {
			list *r_exps = rel_projections(sql, r, NULL, 1, 2);

			list_merge(exps, r_exps, (fdup)NULL);
		}
		/* Here we should check for ambigious names ? */
		if (is_join(rel->op) && r) {
			t = (l->op == op_project && l->l)?l->l:l;
			l_exps = rel_projections(sql, t, NULL, 1, 1);
			/* conflict with old right expressions */
			r_exps = rel_projections(sql, r, NULL, 1, 1);
			for(n = l_exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				const char *rname = exp_relname(e);
				const char *name = exp_name(e);
	
				if (exp_is_atom(e))
					continue;
				if ((rname && exps_bind_column2(r_exps, rname, name) != NULL) || 
				    (!rname && exps_bind_column(r_exps, name, NULL) != NULL)) 
					return rel;
			}
			t = (r->op == op_project && r->l)?r->l:r;
			r_exps = rel_projections(sql, t, NULL, 1, 1);
			/* conflict with new right expressions */
			for(n = l_exps->h; n; n = n->next) {
				sql_exp *e = n->data;
	
				if (exp_is_atom(e))
					continue;
				if ((e->l && exps_bind_column2(r_exps, e->l, e->r) != NULL) || 
				   (exps_bind_column(r_exps, e->r, NULL) != NULL && (!e->l || !e->r)))
					return rel;
			}
			/* conflict with new left expressions */
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

			if (e->type != e_aggr && e->type != e_column) {
				fnd = 1;
			}
		}
		/* only aggr, no rewrite needed */
		if (!fnd) 
			return rel;

		aexps = sa_list(sql->sa);
		pexps = sa_list(sql->sa);
		for ( n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne = NULL;

			switch (e->type) {	
			case e_atom: /* move over to the projection */
				list_append(pexps, e);
				break;
			case e_func: 
				list_append(pexps, e);
				list_split_aggr_and_project(sql, aexps, e->l);
				break;
			case e_convert: 
				list_append(pexps, e);
				e->l = split_aggr_and_project(sql, aexps, e->l);
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
	int nr = 0;
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
				nr += exp_mark_used(subrel, n->data);
		}
		break;
	}
	case e_cmp:
		if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
			list *l = e->l;
			node *n;
	
			for (n = l->h; n != NULL; n = n->next) 
				nr += exp_mark_used(subrel, n->data);
			l = e->r;
			for (n = l->h; n != NULL; n = n->next) 
				nr += exp_mark_used(subrel, n->data);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			list *r = e->r;
			node *n;

			nr += exp_mark_used(subrel, e->l);
			for (n = r->h; n != NULL; n = n->next)
				nr += exp_mark_used(subrel, n->data);
		} else {
			nr += exp_mark_used(subrel, e->l);
			nr += exp_mark_used(subrel, e->r);
			if (e->f)
				nr += exp_mark_used(subrel, e->f);
		}
		break;
	case e_atom:
		/* atoms are used in e_cmp */
		e->used = 1;
		/* return 0 as constants may require a full column ! */
		return 0;
	case e_psm:
		e->used = 1;
		break;
	}
	if (ne) {
		ne->used = 1;
		return ne->used;
	}
	return nr;
}

static void
positional_exps_mark_used( sql_rel *rel, sql_rel *subrel )
{
	if (!rel->exps) 
		assert(0);

	if ((is_topn(subrel->op) || is_sample(subrel->op)) && subrel->l)
		subrel = subrel->l;
	/* everything is used within the set operation */
	if (rel->exps && subrel->exps) {
		node *m;
		for (m=subrel->exps->h; m; m = m->next) {
			sql_exp *se = m->data;

			se->used = 1;
		}
	}
}

static void
exps_mark_used(sql_allocator *sa, sql_rel *rel, sql_rel *subrel)
{
	int nr = 0;

	if (rel->r && (rel->op == op_project || rel->op  == op_groupby)) {
		list *l = rel->r;
		node *n;

		for (n=l->h; n; n = n->next) {
			sql_exp *e = n->data;

			exp_mark_used(rel, e);
		}
	}

	if (rel->exps) {
		node *n;
		int len = list_length(rel->exps), i;
		sql_exp **exps = SA_NEW_ARRAY(sa, sql_exp*, len);

		for (n=rel->exps->h, i = 0; n; n = n->next, i++) {
			sql_exp *e = exps[i] = n->data;

			nr += e->used;
		}

		if (!nr && is_project(rel->op)) /* project atleast one column */
			exps[0]->used = 1; 

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
	if (subrel && !nr && (is_project(subrel->op) || is_base(subrel->op)) && subrel->exps->h) {
		sql_exp *e = subrel->exps->h->data;
		e->used = 1;
	}
	if (rel->r && (rel->op == op_project || rel->op  == op_groupby)) {
		list *l = rel->r;
		node *n;

		for (n=l->h; n; n = n->next) {
			sql_exp *e = n->data;

		//	exp_mark_used(rel, e);
			/* possibly project/groupby uses columns from the inner */ 
			exp_mark_used(subrel, e);
		}
	}
}

static void exps_used(list *l);

static void
exp_used(sql_exp *e)
{
	if (e) {
		e->used = 1;
		if ((e->type == e_func || e->type == e_aggr) && e->l)
			exps_used(e->l);
	}
}

static void
exps_used(list *l)
{
	if (l) {
		node *n;

		for (n = l->h; n; n = n->next) 
			exp_used(n->data);
	}
}

static void
rel_used(sql_rel *rel)
{
	if (!rel)
		return;
	if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op)) {
		if (rel->l) 
			rel_used(rel->l);
		if (rel->r) 
			rel_used(rel->r);
	} else if (is_topn(rel->op) || is_select(rel->op) || is_sample(rel->op)) {
		rel_used(rel->l);
		rel = rel->l;
	} else if (rel->op == op_table && rel->r) {
		exp_used(rel->r);
	}
	if (rel && rel->exps) {
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

		if (rel->op == op_table && rel->l && rel->flag != 2) {
			rel_used(rel);
			if (rel->r)
				exp_mark_used(rel->l, rel->r);
			rel_mark_used(sql, rel->l, proj);
		}
		break;

	case op_topn:
	case op_sample:
		if (proj) {
			rel = rel ->l;
			rel_mark_used(sql, rel, proj);
			break;
		}
		/* fall through */
	case op_project:
	case op_groupby: 
		if (proj && rel->l) {
			exps_mark_used(sql->sa, rel, rel->l);
			rel_mark_used(sql, rel->l, 0);
		} else if (proj) {
			exps_mark_used(sql->sa, rel, NULL);
		}
		break;
	case op_update:
	case op_delete:
		if (proj && rel->r) {
			sql_rel *r = rel->r;
			if (r->exps && r->exps->h) { /* TID is used */
				sql_exp *e = r->exps->h->data;
				e->used = 1;
			}
			exps_mark_used(sql->sa, rel, rel->r);
			rel_mark_used(sql, rel->r, 0);
		}
		break;

	case op_insert:
	case op_ddl:
		break;

	case op_select:
		if (rel->l) {
			exps_mark_used(sql->sa, rel, rel->l);
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
			sql_rel *l = rel->l;

			positional_exps_mark_used(rel, l);
		//	rel_mark_used(sql, rel->l, 1);
			/* based on child check set expression list */
			if (is_project(l->op) && need_distinct(l))
				positional_exps_mark_used(l, rel);
			positional_exps_mark_used(rel, rel->r);
		//	rel_mark_used(sql, rel->r, 1);
		}
		break;

	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
		exps_mark_used(sql->sa, rel, rel->l);
		exps_mark_used(sql->sa, rel, rel->r);
		rel_mark_used(sql, rel->l, 0);
		rel_mark_used(sql, rel->r, 0);
		break;
	case op_apply: 
		break;
	}
}

static sql_rel * rel_dce_sub(mvc *sql, sql_rel *rel, list *refs);

static sql_rel *
rel_remove_unused(mvc *sql, sql_rel *rel) 
{
	int needed = 0;

	if (!rel)
		return rel;

	switch(rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isReplicaTable(t)) /* TODO fix rewriting in rel_distribute.c */
			return rel;
	}
	/* fall through */
	case op_table:
		if (rel->exps) {
			node *n;
			list *exps;

			for(n=rel->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!e->used)
					needed = 1;
			}

			if (!needed)
				return rel;

			exps = new_exp_list(sql->sa);
			for(n=rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (e->used)
					append(exps, e);
			}
			/* atleast one (needed for crossproducts, count(*), rank() and single value projections) !, handled by exps_mark_used */
			if (list_length(exps) == 0)
				append(exps, rel->exps->h->data);
			rel->exps = exps;
		}
		return rel;

	case op_topn:
	case op_sample:

		if (rel->l)
			rel->l = rel_remove_unused(sql, rel->l);
		return rel;

	case op_project:
	case op_groupby: 
	case op_apply: 

		if (/*rel->l &&*/ rel->exps) {
			node *n;
			list *exps;

			for(n=rel->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!e->used)
					needed = 1;
			}
			if (!needed)
				return rel;

 			exps = new_exp_list(sql->sa);
			for(n=rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (e->used)
					append(exps, e);
			}
			/* atleast one (needed for crossproducts, count(*), rank() and single value projections) */
			if (list_length(exps) <= 0)
				append(exps, rel->exps->h->data);
			rel->exps = exps;
		}
		return rel;

	case op_union: 
	case op_inter: 
	case op_except: 

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

static void
rel_dep_graph( char *deps, list *refs, sql_rel *parent, sql_rel *rel) 
{
	if (!parent)
		return ;

	if (rel_is_ref(rel) && parent != rel) {
		int n = list_length(refs);
		int pnr = list_position(refs, parent);
		int cnr = list_position(refs, rel);

		deps[pnr*n + cnr] = 1;
		parent = rel;
	}

	switch(rel->op) {
	case op_table:
	case op_topn: 
	case op_sample: 
	case op_project:
	case op_groupby: 
	case op_select: 

		if (rel->l && (rel->op != op_table || rel->flag != 2))
			rel_dep_graph(deps, refs, parent, rel->l);

	case op_basetable:
	case op_insert:
	case op_ddl:
		break;

	case op_update:
	case op_delete:

		if (rel->r)
			rel_dep_graph(deps, refs, parent, rel->r);
		break;


	case op_union: 
	case op_inter: 
	case op_except: 
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 

		if (rel->l)
			rel_dep_graph(deps, refs, parent, rel->l);
		if (rel->r)
			rel_dep_graph(deps, refs, parent, rel->r);
		break;

	case op_apply: 
		assert(0);
	}
}

/*
extern void _rel_print(mvc *sql, sql_rel *rel);

static void 
print_deps(mvc *sql, char *deps, list *refs) 
{
	int i, j;
	int n = list_length(refs);

	for (i=0; i<n; i++) {
		sql_rel *r = list_fetch(refs, i);
		printf("dep %d\n", i);
		_rel_print(sql,r);
	}
	for (i=0; i<n; i++) {
		for (j=0; j<n; j++) {
			printf("%c ", i==j?'x' : deps[i*n + j]?'1':'0');
		}
		printf("\n");
	}

}	
*/

static int 
depends_on(int nr, char *deps, int n, int dnr) 
{
	for(;dnr < n; dnr++) {
		if (dnr == nr)
			dnr++;
		if (deps[nr*n + dnr])
			return dnr;
	}
	return -1;
}

static void
flatten_dep(list *nrefs, list *refs, int nr, char *deps, int n) 
{
	int dnr = 0;

	if (deps[nr*n + nr])
		return;
	for (;(dnr = depends_on(nr, deps, n, dnr)) >= 0 && dnr < n; dnr++) 
		flatten_dep(nrefs, refs, dnr, deps, n);
	if (!deps[nr*n + nr]) {
		list_prepend(nrefs, list_fetch(refs,nr));
		deps[nr*n+nr] = 1; /* mark done */
	}
}

static list *
flatten_dep_graph(mvc *sql, char *deps, list *refs)
{
	list *nrefs = sa_list(sql->sa);
	int n = list_length(refs), nr = 0;

	for (nr = 0; nr < n; nr++) {
		if (deps[nr*n + nr])
			continue;
		flatten_dep(nrefs, refs, nr, deps, n);
	}
	return nrefs;
}

static list *
rel_dependencies(mvc *sql, list *refs)
{
	int n = list_length(refs);

	if (n > 1) {
		char *deps = SA_NEW_ARRAY(sql->sa, char, n*n);
		node *m;

		memset(deps, 0, n*n);
		for (m = refs->h; m; m = m->next) {
			rel_dep_graph(deps, refs, m->data, m->data);
		}
		refs = flatten_dep_graph(sql, deps, refs);
		//print_deps(sql, deps, refs);
	}
	return refs;
}

static void
rel_dce_refs(mvc *sql, sql_rel *rel, list *refs) 
{
	if (!rel)
		return ;

	switch(rel->op) {
	case op_table:
	case op_topn: 
	case op_sample: 
	case op_project:
	case op_groupby: 
	case op_select: 

		if (rel->l && (rel->op != op_table || rel->flag != 2))
			rel_dce_refs(sql, rel->l, refs);

	case op_basetable:
	case op_insert:
	case op_ddl:
		break;

	case op_update:
	case op_delete:

		if (rel->r)
			rel_dce_refs(sql, rel->r, refs);
		break;


	case op_union: 
	case op_inter: 
	case op_except: 
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 

		if (rel->l)
			rel_dce_refs(sql, rel->l, refs);
		if (rel->r)
			rel_dce_refs(sql, rel->r, refs);
		break;

	case op_apply: 
		assert(0);
	}

	if (rel_is_ref(rel) && !list_find(refs, rel, NULL)) 
		list_prepend(refs, rel);
}

static sql_rel *
rel_dce_down(mvc *sql, sql_rel *rel, list *refs, int skip_proj) 
{
	if (!rel)
		return rel;

	if (!skip_proj && rel_is_ref(rel)) {
		if (!list_find(refs, rel, NULL))
			list_append(refs, rel);
		return rel;
	}

	switch(rel->op) {
	case op_basetable:
	case op_table:

		if (skip_proj && rel->l && rel->op == op_table && rel->flag != 2)
			rel->l = rel_dce_down(sql, rel->l, refs, 0);
		if (!skip_proj)
			rel_dce_sub(sql, rel, refs);
		/* fall through */

	case op_insert:
	case op_ddl:

		return rel;

	case op_update:
	case op_delete:

		if (skip_proj && rel->r)
			rel->r = rel_dce_down(sql, rel->r, refs, 0);
		if (!skip_proj)
			rel_dce_sub(sql, rel, refs);
		return rel;

	case op_topn: 
	case op_sample: 
	case op_project:
	case op_groupby: 

		if (skip_proj && rel->l)
			rel->l = rel_dce_down(sql, rel->l, refs, is_topn(rel->op) || is_sample(rel->op));
		if (!skip_proj)
			rel_dce_sub(sql, rel, refs);
		return rel;

	case op_union: 
	case op_inter: 
	case op_except: 
		if (skip_proj) {
			if (rel->l)
				rel->l = rel_dce_down(sql, rel->l, refs, 0);
			if (rel->r)
				rel->r = rel_dce_down(sql, rel->r, refs, 0);
		}
		if (!skip_proj)
			rel_dce_sub(sql, rel, refs);
		return rel;

	case op_select: 
		if (rel->l)
			rel->l = rel_dce_down(sql, rel->l, refs, 0);
		return rel;

	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 
	case op_semi: 
	case op_anti: 
		if (rel->l)
			rel->l = rel_dce_down(sql, rel->l, refs, 0);
		if (rel->r)
			rel->r = rel_dce_down(sql, rel->r, refs, 0);
		return rel;
	case op_apply: 
		assert(0);
	}
	return rel;
}

/* DCE
 *
 * Based on top relation expressions mark sub expressions as used.
 * Then recurse down until the projections. Clean them up and repeat.
 */

static sql_rel *
rel_dce_sub(mvc *sql, sql_rel *rel, list *refs)
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
	rel_dce_down(sql, rel, refs, 1);
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
	case op_sample: 
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
	case op_apply: 
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

sql_rel *
rel_dce(mvc *sql, sql_rel *rel)
{
	list *refs = sa_list(sql->sa);
	node *n;

	if (sql->sqs) {
		node *n;

		for(n = sql->sqs->h; n; n = n->next) {
			sql_var *v = n->data;
			sql_rel *i = v->rel;

			while (!rel_is_ref(i) && i->l && !is_base(i->op))
				i = i->l;
			if (i)
				rel_used(i);
		}
	}

	rel_dce_refs(sql, rel, refs);
	rel = rel_add_projects(sql, rel);
	rel_used(rel);
	rel_dce_sub(sql, rel, refs);

	if (refs) {
		refs = rel_dependencies(sql, refs);
		for (n = refs->h; n; n = n->next)
			rel_dce_sub(sql, n->data, refs);
	}
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
			/* fall through */
		case join_idx:
		default:
			return -1;
		}
	}
	return -1;
}

static sql_idx *
find_index(sql_allocator *sa, sql_rel *rel, sql_rel *sub, list **EXPS)
{
	node *n;

	/* any (partial) match of the expressions with the index columns */
	/* Depending on the index type we may need full matches and only
	   limited number of cmp types (hash only equality etc) */
	/* Depending on the index type we should (in the rel_bin) generate
	   more code, ie for spatial index add post filter etc, for hash
	   compute hash value and use index */

	if (sub->exps && rel->exps) 
	for(n = sub->exps->h; n; n = n->next) {
		prop *p;
		sql_exp *e = n->data;

		if ((p = find_prop(e->p, PROP_HASHIDX)) != NULL) {
			list *exps, *cols;
	    		sql_idx *i = p->value;
			fcmp cmp = (fcmp)&sql_column_kc_cmp;

			/* join indices are only interesting for joins */
			if (i->type == join_idx || list_length(i->columns) <= 1)
				continue;
			/* based on the index type, find qualifying exps */
			exps = list_select(rel->exps, i, (fcmp) &index_exp, (fdup)NULL);
			if (!exps || !list_length(exps))
				continue;
			/* now we obtain the columns, move into sql_column_kc_cmp! */
			cols = list_map(exps, sub, (fmap) &sjexp_col);

			/* TODO check that at most 2 relations are involved */

			/* Match the index columns with the expression columns. 
			   TODO, Allow partial matches ! */
			if (list_match(cols, i->columns, cmp) == 0) {
				/* re-order exps in index order */
				node *n, *m;
				list *es = sa_list(sa);

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
				e->used = 1;
				return i;
			}
			cols->destroy = NULL;
		}
	}
	return NULL;
}

static sql_rel *
rel_use_index(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)changes;
	(void)sql;
	if (rel->l && (is_select(rel->op) || is_join(rel->op))) {
		list *exps = NULL;
		sql_idx *i = find_index(sql->sa, rel, rel->l, &exps);
		int left = 1;

		if (!i && is_join(rel->op))
			i = find_index(sql->sa, rel, rel->l, &exps);
		if (!i && is_join(rel->op)) {
			left = 0;
			i = find_index(sql->sa, rel, rel->r, &exps);
		}
			
		if (i) {
			prop *p;
			node *n;
			int single_table = 1;
			sql_exp *re = NULL;
	
			for( n = exps->h; n && single_table; n = n->next) { 
				sql_exp *e = n->data;
				sql_exp *nre = e->r;

				if (is_join(rel->op) && 
				 	((left && !rel_find_exp(rel->l, e->l)) ||
				 	(!left && !rel_find_exp(rel->r, e->l)))) 
					nre = e->l;
				single_table = (!re || (exp_relname(nre) && exp_relname(re) && strcmp(exp_relname(nre), exp_relname(re)) == 0));
				re = nre;
			}
			if (single_table) { /* add PROP_HASHCOL to all column exps */
				for( n = exps->h; n; n = n->next) { 
					sql_exp *e = n->data;

					/* swapped ? */
					if (is_join(rel->op) && 
					 	((left && !rel_find_exp(rel->l, e->l)) ||
					 	(!left && !rel_find_exp(rel->r, e->l)))) 
						n->data = e = exp_compare(sql->sa, e->r, e->l, cmp_equal);
					p = find_prop(e->p, PROP_HASHCOL);
					if (!p)
						e->p = p = prop_create(sql->sa, PROP_HASHCOL, e->p);
					p->value = i;
				}
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

static int
score_se( mvc *sql, sql_rel *rel, sql_exp *e)
{
	int score = 0;
	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		score += score_gbe(sql, rel, e->l);
	}
	score += exp_keyvalue(e);
	return score;
}

static sql_rel *
rel_select_order(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)changes;
	(void)sql;
	if (is_select(rel->op) && rel->exps && list_length(rel->exps)>1) {
		int i, *scores = calloc(list_length(rel->exps), sizeof(int));
		node *n;

		for (i = 0, n = rel->exps->h; n; i++, n = n->next) 
			scores[i] = score_se(sql, rel, n->data);
		rel->exps = list_keysort(rel->exps, scores, (fdup)NULL);
		free(scores);
	}
	return rel;
}

static sql_rel *
rel_simplify_like_select(int *changes, mvc *sql, sql_rel *rel) 
{
	(void)sql;
	if (is_select(rel->op) && rel->exps) {
		node *n;
		list *exps = sa_list(sql->sa);
			
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			list *l = e->l;
			list *r = e->r;

			if (e->type == e_cmp && get_cmp(e) == cmp_filter && strcmp(((sql_subfunc*)e->f)->func->base.name, "like") == 0 && list_length(l) == 1 && list_length(r) <= 2 && !(e->flag & ANTISEL)) {
				list *r = e->r;
				sql_exp *fmt = r->h->data;
				sql_exp *esc = (r->h->next)?r->h->next->data:NULL;
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
					list *l = e->l;
					list *r = e->r;
					sql_exp *ne = exp_compare(sql->sa, l->h->data, r->h->data, cmp_equal);
					/* if rewritten don't cache this query */
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

static sql_rel *
rel_simplify_predicates(int *changes, mvc *sql, sql_rel *rel) 
{
	if ((is_select(rel->op) || is_join(rel->op)) && rel->exps) {
		node *n;
		list *exps = sa_list(sql->sa);
			
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (is_atom(e->type) && e->l) { /* direct literal */
				atom *a = e->l;
				int flag = a->data.val.bval;

				/* remove simple select true expressions */
				if (flag)
					continue;
			}
			if (is_atom(e->type) && !e->l && !e->r) { /* numbered variable */
				atom *a = sql->args[e->flag];
				int flag = a->data.val.bval;

				/* remove simple select true expressions */
				if (flag) {
					sql->caching = 0;
					break;
				}
			}
			if (e->type == e_cmp && get_cmp(e) == cmp_equal) {
				sql_exp *l = e->l;
				sql_exp *r = e->r;

				if (l->type == e_func) {
					sql_subfunc *f = l->f;
					
					/* rewrite isnull(x) = TRUE/FALSE => x =/<> NULL */
					if (!f->func->s && !strcmp(f->func->base.name, "isnull") && 
					     is_atom(r->type) && r->l) { /* direct literal */
						atom *a = r->l;
						int flag = a->data.val.bval;
						list *args = l->l;

						assert(list_length(args) == 1);
						l = args->h->data;
						r = exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(l), NULL));
						e = exp_compare2(sql->sa, l, r, r, 3);
						if (e && !flag)
							set_anti(e);
					} else if (!f->func->s && !strcmp(f->func->base.name, "not")) {
						if (is_atom(r->type) && r->l) { /* direct literal */
							atom *a = r->l;
							list *args = l->l;
							sql_exp *inner = args->h->data;
							sql_subfunc *inf = inner->f;

							assert(list_length(args) == 1);

							/* not(not(x)) = TRUE/FALSE => x = TRUE/FALSE */
							if (inner->type == e_func && 
							    !inf->func->s && 
							    !strcmp(inf->func->base.name, "not")) {
								args = inner->l;

								assert(list_length(args) == 1);
								l = args->h->data;
								e = exp_compare(sql->sa, l, r, e->flag);
							/* rewrite not(=/<>(a,b)) = TRUE/FALSE => a=b of a<>b */
							} else if (inner->type == e_func && 
							    !inf->func->s && 
							    (!strcmp(inf->func->base.name, "=") ||
							     !strcmp(inf->func->base.name, "<>"))) {
								int flag = a->data.val.bval;
								args = inner->l;

								if (!strcmp(inf->func->base.name, "<>"))
									flag = !flag;
								assert(list_length(args) == 2);
								l = args->h->data;
								r = args->h->next->data;
								e = exp_compare(sql->sa, l, r, (!flag)?cmp_equal:cmp_notequal);
							} else if (a && a->data.vtype == TYPE_bit) {
								/* change atom's value on right */
								l = args->h->data;
								a->data.val.bval = !a->data.val.bval;
								e = exp_compare(sql->sa, l, r, e->flag);
								(*changes)++;
							}
						}
					}
				}
				list_append(exps, e);
			} else {
				list_append(exps, e);
			}
		}
		rel->exps = exps;
	}
	return rel;
}

static void split_exps(mvc *sql, list *exps, sql_rel *rel);

static int
exp_match_exp_cmp( sql_exp *e1, sql_exp *e2)
{
	if (exp_match_exp(e1,e2))
		return 0;
	return -1;
}

static int
exp_refers_cmp( sql_exp *e1, sql_exp *e2)
{
	if (exp_refers(e1,e2))
		return 0;
	return -1;
}

static sql_exp *
add_exp_too_project(mvc *sql, sql_exp *e, sql_rel *rel)
{
	node *n = list_find(rel->exps, e, (fcmp)&exp_match_exp_cmp);

	/* if not matching we may refer to an older expression */
	if (!n) 
		n = list_find(rel->exps, e, (fcmp)&exp_refers_cmp);
	if (!n) {
		exp_label(sql->sa, e, ++sql->label);
		append(rel->exps, e);
	} else {
		e = n->data;
	}
	e = exp_column(sql->sa, exp_relname(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_intern(e));
	return e;
}

static void
add_exps_too_project(mvc *sql, list *exps, sql_rel *rel)
{
	node *n;

	if (!exps)
		return;
	for(n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (!exp_is_atom(e) && e->type != e_column)
			n->data = add_exp_too_project(sql, e, rel);
	}
}

static sql_exp *
split_exp(mvc *sql, sql_exp *e, sql_rel *rel)
{
	switch(e->type) {
	case e_column:
		return e;
	case e_convert:
		e->l = split_exp(sql, e->l, rel);
		return e;
	case e_aggr:
	case e_func: 
		if (!is_analytic(e) && !exp_has_sideeffect(e)) {
			split_exps(sql, e->l, rel);
			add_exps_too_project(sql, e->l, rel);
		}
		return e;
	case e_cmp:	
		if (get_cmp(e) == cmp_or) {
			split_exps(sql, e->l, rel);
			split_exps(sql, e->r, rel);
		} else if (e->flag == cmp_in || e->flag == cmp_notin || get_cmp(e) == cmp_filter) {
			e->l = split_exp(sql, e->l, rel);
			split_exps(sql, e->r, rel);
		} else {
			e->l = split_exp(sql, e->l, rel);
			e->r = split_exp(sql, e->r, rel);
			if (e->f) {
				assert(0);
				e->f = split_exp(sql, e->f, rel);
			}
		}
		return e;
	case e_psm:	
	case e_atom:
		return e;
	}
	return e;
}

static void
split_exps(mvc *sql, list *exps, sql_rel *rel)
{
	node *n;

	if (!exps)
		return;
	for(n=exps->h; n; n = n->next){
		sql_exp *e = n->data;

		e = split_exp(sql, e, rel);
		n->data = e;
	}
}

static sql_rel *
rel_split_project(int *changes, mvc *sql, sql_rel *rel, int top) 
{
	if (is_project(rel->op) && list_length(rel->exps) && rel->l) {
		list *exps = rel->exps;
		node *n;
		int funcs = 0;
		sql_rel *nrel;

		/* are there functions */
		for (n=exps->h; n && !funcs; n = n->next) {
			sql_exp *e = n->data;

			funcs = exp_has_func(e);
		}
		/* introduce extra project */
		if (funcs && rel->op != op_project) {
			nrel = rel_project(sql->sa, rel->l, 
				rel_projections(sql, rel->l, NULL, 1, 1));
			rel->l = nrel;
			/* recursively split all functions and add those to the projection list */
			split_exps(sql, rel->exps, nrel);
			if (nrel->l)
				nrel->l = rel_split_project(changes, sql, nrel->l, is_topn(rel->op)?top:0);
			return rel;
		} else if (funcs && !top && !rel->r) {
			/* projects can have columns point back into the expression list, ie
			 * create a new list including the split expressions */
			node *n;
			list *exps = rel->exps;

			rel->exps = sa_list(sql->sa);
			for (n=exps->h; n; n = n->next) 
				append(rel->exps, split_exp(sql, n->data, rel));
		}
	}
	if (is_set(rel->op) || is_basetable(rel->op))
		return rel;
	if (rel->l)
		rel->l = rel_split_project(changes, sql, rel->l, 
			(is_topn(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
	if (is_join(rel->op) && rel->r)
		rel->r = rel_split_project(changes, sql, rel->r, 
			(is_topn(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
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
					sql_exp *ne, *t;
					int swap = 0, lt = 0, gt = 0;
					comp_type ef = (comp_type) e->flag, ff = (comp_type) f->flag;
				
					/* both swapped ? */
				     	if (exp_match_exp(re, rf)) {
						t = re; 
						re = le;
						le = t;
						ef = swap_compare(ef);
						t = rf;
						rf = lf;
						lf = t;
						ff = swap_compare(ff);
					}

					/* is left swapped ? */
				     	if (exp_match_exp(re, lf)) {
						t = re; 
						re = le;
						le = t;
						ef = swap_compare(ef);
					}

					/* is right swapped ? */
				     	if (exp_match_exp(le, rf)) {
						t = rf; 
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
#ifdef HAVE_HGE
	if (a->data.vtype == TYPE_hge) {
		hge v = a->data.val.hval;
		int i = 0;

		if (v != 0) 
                        while( (v/10)*10 == v ) {
                                i++;
                                v /= 10;
                        }
		a->data.val.hval = v;
		return i;
	}
#endif
	if (a->data.vtype == TYPE_lng) {
		lng v = a->data.val.lval;
		int i = 0;

		if (v != 0) 
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

		if (v != 0) 
                        while( (v/10)*10 == v ) {
                                i++;
                                v /= 10;
                        }
		a->data.val.ival = v;
		return i;
	}
	if (a->data.vtype == TYPE_sht) {
		sht v = a->data.val.shval;
		int i = 0;

		if (v != 0) 
                        while( (v/10)*10 == v ) {
                                i++;
                                v /= 10;
                        }
		a->data.val.shval = v;
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
				sql_subtype *res = f->res->h->data; 

				if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && res->scale > 0) {
					list *args = e->l;
					sql_exp *h = args->h->data;
					sql_exp *t = args->t->data;
					atom *a;

					if ((is_atom(h->type) && (a = exp_value(sql, h, sql->args, sql->argc)) != NULL) ||
					    (is_atom(t->type) && (a = exp_value(sql, t, sql->args, sql->argc)) != NULL)) {
						int rs = reduce_scale(a);

						res->scale -= rs; 
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
	(void)sql;
	(void)changes;
	if ((is_join(rel->op) || is_semi(rel->op) || is_select(rel->op)) && 
			rel->exps && list_length(rel->exps)) {
		list *exps = rel->exps;
		node *n;

		for (n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_exp *le = e->l;
			sql_exp *re = e->r;
	
			/* handle the and's in the or lists */
			if (e->type != e_cmp || !is_theta_exp(e->flag) || e->f)
				continue;
			/* rewrite e if left or right is a cast */
			if (le->type == e_convert || re->type == e_convert) {
				sql_rel *r = rel->r;
				sql_subtype *st = exp_subtype(re);

				/* e_convert(le) ==, <(=), >(=), !=  e_atom(re), conversion between integers only */
				if (le->type == e_convert && is_simple_atom(re) && st->type->eclass == EC_NUM) {
					sql_subtype *tt = exp_totype(le);
					sql_subtype *ft = exp_fromtype(le);

					if (tt->type->eclass != EC_NUM || ft->type->eclass != EC_NUM || tt->type->localtype < ft->type->localtype)
						continue;

					/* tt->type larger then tt->type, ie empty result, ie change into > max */
					re = exp_atom_max( sql->sa, ft);
					if (!re)
						continue;
					/* the ==, > and >=  change to l > max, the !=, < and <=  change to l < max */
					if (e->flag == cmp_equal || e->flag == cmp_gt || e->flag == cmp_gte)
						e = exp_compare(sql->sa, le->l, re, cmp_gt);
					else
						e = exp_compare(sql->sa, le->l, re, cmp_lt);
					sql->caching = 0;
				} else
				/* if convert on left then find
				 * mul or div on right which increased
				 * scale!
				 */
				if (le->type == e_convert && re->type == e_column && (e->flag == cmp_lt || e->flag == cmp_gt) && r && is_project(r->op)) {
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
							   (a = exp_value(sql, ce, sql->args, sql->argc)) != NULL) {
#ifdef HAVE_HGE
								hge v = 1;
#else
								lng v = 1;
#endif
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
#ifdef HAVE_HGE
								append(args, have_hge ? exp_atom_hge(sql->sa, v) : exp_atom_lng(sql->sa, (lng) v));
#else
								append(args, exp_atom_lng(sql->sa, v));
#endif
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
is_identity_of(sql_exp *e, sql_rel *l) 
{
	if (e->type != e_cmp)
		return 0;
	if (!is_identity(e->l, l) || !is_identity(e->r, l))
		return 0;
	return 1;
}


static sql_rel *
rel_rewrite_semijoin(int *changes, mvc *sql, sql_rel *rel)
{
	(void)sql;
	if (is_semi(rel->op)) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		sql_rel *rl = (r->l)?r->l:NULL;
		int on_identity = 1;

		if (!rel->exps || list_length(rel->exps) != 1 || !is_identity_of(rel->exps->h->data, l))
			on_identity = 0;
			
		/* rewrite {semi,anti}join (A, join(A,B)) into {semi,anti}join (A,B) 
		 * and     {semi,anti}join (A, join(B,A)) into {semi,anti}join (A,B) 
		 * Where the semi/anti join is done using the identity */
		if (on_identity && l->ref.refcnt == 2 && ((is_join(r->op) && (l == r->l || l == r->r)) || 
		   (is_project(r->op) && rl && is_join(rl->op) && (l == rl->l || l == rl->r)))){
			sql_rel *or = r;

			if (is_project(r->op)) 
				r = rl;

			if (l == r->r)
				rel->r = rel_dup(r->l);
			else
				rel->r = rel_dup(r->r);

			rel->exps = r->exps;
			r->exps = NULL;
			rel_destroy(or);
			(*changes)++;
		}
	}
	if (is_semi(rel->op)) {
		sql_rel *l = rel->l, *rl = NULL;
		sql_rel *r = rel->r, *or = r;

		if (r)
			rl = r->l;
		if (r && is_project(r->op)) {
			r = rl;
			if (r)
				rl = r->l;
		}

		/* More general case is (join reduction)
   		   {semi,anti}join (A, join(A,B) [A.c1 == B.c1]) [ A.c1 == B.c1 ]
		   into {semi,anti}join (A,B) [ A.c1 == B.c1 ] 

		   for semijoin also A.c1 == B.k1 ] [ A.c1 == B.k2 ] could be rewriten
		 */
		if (l && r && rl && 
		    is_basetable(l->op) && is_basetable(rl->op) &&
		    is_join(r->op) && l->l == rl->l)
		{
			node *n, *m;
			list *exps;

			if (!rel->exps || !r->exps ||
		       	    list_length(rel->exps) != list_length(r->exps)) 
				return rel;
			exps = new_exp_list(sql->sa);

			/* are the join conditions equal */
			for (n = rel->exps->h, m = r->exps->h;
			     n && m; n = n->next, m = m->next)
			{
				sql_exp *le = NULL, *oe = n->data;
				sql_exp *re = NULL, *ne = m->data;
				sql_column *cl;  
				int equal = 0;
				
				if (oe->type != e_cmp || ne->type != e_cmp ||
				    oe->flag != cmp_equal || 
				    ne->flag != cmp_equal)
					return rel;

				if ((cl = exp_find_column(rel->l, oe->l, -2)) != NULL) {
					le = oe->l;
					re = oe->r;
				} else if ((cl = exp_find_column(rel->l, oe->r, -2)) != NULL) {
					le = oe->r;
					re = oe->l;
				} else
					return rel;

				if (exp_find_column(rl, ne->l, -2) == cl) {
					sql_exp *e = (or != r)?rel_find_exp(or, re):re;

					equal = exp_match_exp(ne->r, e);
					if (!equal)
						return rel;
					re = ne->r;
				} else if (exp_find_column(rl, ne->r, -2) == cl) {
					sql_exp *e = (or != r)?rel_find_exp(or, re):re;

					equal = exp_match_exp(ne->l, e);
					if (!equal)
						return rel;
					re = ne->l;
				} else
					return rel;

				ne = exp_compare(sql->sa, le, re, cmp_equal);
				append(exps, ne);
			}

			rel->r = rel_dup(r->r);
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
		list *rels = new_rel_list(sql->sa);

		rel->exps = NULL;
		append(rels, rel->l);
		append(rels, rel->r);
		(void) find_fk( sql, rels, exps);

		rel->exps = exps;
	}
	return rel;
}

/* leftouterjoin(a,b)[ a.C op b.D or a.E op2 b.F ]) -> 
 * union(
 * 	join(a,b)[ a.C op b.D or a.E op2 b. F ], 
 * 	project( 
 * 		antijoin(a,b) [a.C op b.D or a.E op2 b.F ])
 *	 	[ a.*, NULL * foreach column of b]
 * )
 */
static int
exps_nr_of_or(list *exps)
{
	int ors = 0;
	node *n;

	if (!exps)
		return ors;
	for(n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or)
			ors++;
	}
	return ors;
}

static void 
add_nulls(mvc *sql, sql_rel *rel, sql_rel *r)
{
	list *exps;
	node *n;

	exps = rel_projections(sql, r, NULL, 1, 1);
	for(n = exps->h; n; n = n->next) {
		sql_exp *e = n->data, *ne;

		ne = exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(e), NULL));
		exp_setname(sql->sa, ne, exp_relname(e), exp_name(e));
		append(rel->exps, ne);
	}
}

static sql_rel *
rel_split_outerjoin(int *changes, mvc *sql, sql_rel *rel)
{
	if ((rel->op == op_left || rel->op == op_right || rel->op == op_full) &&
	    list_length(rel->exps) == 1 && exps_nr_of_or(rel->exps) == list_length(rel->exps)) { 
		sql_rel *l = rel->l, *nl, *nll, *nlr;
		sql_rel *r = rel->r, *nr;
		sql_exp *e;
		list *exps;

		nll = rel_crossproduct(sql->sa, rel_dup(l), rel_dup(r), op_join); 
		nlr = rel_crossproduct(sql->sa, rel_dup(l), rel_dup(r), op_join); 

		/* TODO find or exp, ie handle rest with extra joins */
		/* expect only a single or expr for now */
		assert(list_length(rel->exps) == 1);
	       	e = rel->exps->h->data;
		nll->exps = exps_copy(sql->sa, e->l);
		nlr->exps = exps_copy(sql->sa, e->r);
		nl = rel_or( sql, NULL, nll, nlr, NULL, NULL, NULL);

		if (rel->op == op_left || rel->op == op_full) {
			/* split in 2 anti joins */
			nr = rel_crossproduct(sql->sa, rel_dup(l), rel_dup(r), op_anti);
			nr->exps = exps_copy(sql->sa, e->l);
			nr = rel_crossproduct(sql->sa, nr, rel_dup(r), op_anti);
			nr->exps = exps_copy(sql->sa, e->r);

			/* project left */
			nr = rel_project(sql->sa, nr, 
				rel_projections(sql, l, NULL, 1, 1));
			/* add null's for right */
			add_nulls( sql, nr, r);
			exps = rel_projections(sql, nl, NULL, 1, 1);
			nl = rel_setop(sql->sa, nl, nr, op_union);
			nl->exps = exps;
			set_processed(nl);
		}
		if (rel->op == op_right || rel->op == op_full) {
			/* split in 2 anti joins */
			nr = rel_crossproduct(sql->sa, rel_dup(r), rel_dup(l), op_anti);
			nr->exps = exps_copy(sql->sa, e->l);
			nr = rel_crossproduct(sql->sa, nr, rel_dup(l), op_anti);
			nr->exps = exps_copy(sql->sa, e->r);

			nr = rel_project(sql->sa, nr, sa_list(sql->sa));
			/* add null's for left */
			add_nulls( sql, nr, l);
			/* project right */
			nr->exps = list_merge(nr->exps, 
				rel_projections(sql, r, NULL, 1, 1),
				(fdup)NULL);
			exps = rel_projections(sql, nl, NULL, 1, 1);
			nl = rel_setop(sql->sa, nl, nr, op_union);
			nl->exps = exps;
			set_processed(nl);
		}

		rel_destroy(rel);
		*changes = 1;
		rel = nl;
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

static sql_exp *
exp_indexcol(mvc *sql, sql_exp *e, const char *tname, const char *cname, int de, bit unique)
{
	sql_subtype *rt = sql_bind_localtype(de==1?"bte":de==2?"sht":"int");
	sql_exp *u = exp_atom_bool(sql->sa, unique);
	sql_subfunc *f = sql_bind_func_result(sql->sa, mvc_bind_schema(sql,"sys"), "index", exp_subtype(e), exp_subtype(u), rt);

	e = exp_binop(sql->sa, e, u, f);
	exp_setname(sql->sa, e, tname, cname);
	return e;
}

static sql_exp *
exp_stringscol(mvc *sql, sql_exp *e, const char *tname, const char *cname)
{
	sql_subfunc *f = sql_bind_func(sql->sa, mvc_bind_schema(sql,"sys"), "strings", exp_subtype(e), NULL, F_FUNC);

	e = exp_unop(sql->sa, e, f);
	exp_setname(sql->sa, e, tname, cname);
	return e;
}

static sql_rel *
rel_dicttable(mvc *sql, sql_column *c, const char *tname, int de)
{
	sql_rel *rel = rel_create(sql->sa);
	sql_exp *e, *ie;
	int nr = 0;
	char name[16], *nme;
	if(!rel)
		return NULL;

	e = exp_alias(sql->sa, tname, c->base.name, tname, c->base.name, &c->type, CARD_MULTI, c->null, 0);
	rel->l = NULL;
	rel->r = c;
	rel->op = op_basetable; 
	rel->exps = new_exp_list(sql->sa);

	ie = exp_indexcol(sql, e, tname, c->base.name, de, 1);
        nr = ++sql->label;
	nme = sa_strdup(sql->sa, number2name(name, 16, nr));
	exp_setname(sql->sa, ie, nme, nme);
	append(rel->exps, ie);

	ie = exp_stringscol(sql, e, tname, c->base.name);
        nr = ++sql->label;
	nme = sa_strdup(sql->sa, number2name(name, 16, nr));
	exp_setname(sql->sa, ie, nme, nme);
	append(rel->exps, ie);
	e->p = prop_create(sql->sa, PROP_HASHCOL, e->p);

	rel->card = CARD_MULTI;
	rel->nrcols = 2;
	return rel;
}

/* rewrite merge tables into union of base tables and call optimizer again */
static sql_rel *
rel_add_dicts(int *changes, mvc *sql, sql_rel *rel)
{
	if (is_basetable(rel->op) && rel->l) {
		node *n;
		sql_table *t = rel->l;
		list *l = sa_list(sql->sa), *vcols = NULL, *pexps = sa_list(sql->sa);

		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne = NULL;
			const char *rname = e->rname?e->rname:e->l;
			const char *oname = e->r;
			int de;

			if (!is_func(e->type) && oname[0] != '%') { 
				sql_column *c = find_sql_column(t, oname);

				if (EC_VARCHAR(c->type.type->eclass) && (de = store_funcs.double_elim_col(sql->session->tr, c)) != 0) {
					int nr = ++sql->label;
					char name[16], *nme;
					sql_rel *vt = rel_dicttable(sql, c, rname, de);

					nme = sa_strdup(sql->sa, number2name(name, 16, nr));
					if (!vcols)
						vcols = sa_list(sql->sa);
					append(vcols, vt);
					e = exp_indexcol(sql, e, nme, nme, de, 0);
					ne = exp_column(sql->sa, e->rname, e->name, exp_subtype(e), e->card, has_nil(e), is_intern(e));
					append(vcols, ne);
					append(vcols, n->data);
					(*changes)++;
				}
			}
			list_append(l, e);
			if (!ne)
				list_append(pexps, e);
		}
		rel->exps = l;

		/* add joins for double_eliminated (large) columns */
		if (vcols) {
			node *n;

			for(n = vcols->h; n; n = n->next->next->next) {
				sql_rel *vt = n->data;
				sql_exp *ic = n->next->data, *vti = NULL, *vtv;
				sql_exp *c = n->next->next->data, *cmp;
				const char *rname = c->rname?c->rname:c->l;
				const char *oname = c->r;
	
				rel = rel_crossproduct(sql->sa, rel, vt, op_join);
				vti = vt->exps->h->data;
				vtv = vt->exps->h->next->data;
				vti = exp_column(sql->sa, vti->rname, vti->name, exp_subtype(vti), vti->card, has_nil(vti), is_intern(vti));
				cmp = exp_compare(sql->sa, ic, vti, cmp_equal);
				cmp->p = prop_create(sql->sa, PROP_FETCH, cmp->p);
				rel_join_add_exp( sql->sa, rel, cmp);
	
				vtv = exp_column(sql->sa, vtv->rname, vtv->name, exp_subtype(vtv), vtv->card, has_nil(vtv), is_intern(vtv));
				exp_setname(sql->sa, vtv, rname, oname);
				append(pexps, vtv);
			}
			rel = rel_project(sql->sa, rel, pexps);
		}
	}
	return rel;
}

static int
find_col_exp( list *exps, sql_exp *e)
{
	node *n;
	int nr = 0; 

	for (n=exps->h; n; n=n->next, nr++){
		if (n->data == e)
			return nr;
	}
	return -1;
}

static int
exp_range_overlap( mvc *sql, sql_exp *e, void *min, void *max, atom *emin, atom *emax)
{
	sql_subtype *t = exp_subtype(e);

	if (!min || !max || !emin || !emax)
		return 0;

	if (strcmp("nil", (char*)min) == 0)
		return 0;
	if (strcmp("nil", (char*)max) == 0)
		return 0;

	if (t->type->localtype == TYPE_dbl) {
		atom *cmin = atom_general(sql->sa, t, min);
		atom *cmax = atom_general(sql->sa, t, max);

		if (emax->d < cmin->data.val.dval || emin->d > cmax->data.val.dval)
			return 0;
	}
	if (t->type->localtype == TYPE_bte) {
		atom *cmin = atom_general(sql->sa, t, min);
		atom *cmax = atom_general(sql->sa, t, max);

		if (emax->data.val.btval < cmin->data.val.btval || emin->data.val.btval > cmax->data.val.btval)
			return 0;
	}
	if (t->type->localtype == TYPE_sht) {
		atom *cmin = atom_general(sql->sa, t, min);
		atom *cmax = atom_general(sql->sa, t, max);

		if (emax->data.val.shval < cmin->data.val.shval || emin->data.val.shval > cmax->data.val.shval)
			return 0;
	}
	if (t->type->localtype == TYPE_int || t->type->localtype == TYPE_date) {
		atom *cmin = atom_general(sql->sa, t, min);
		atom *cmax = atom_general(sql->sa, t, max);

		if (emax->data.val.ival < cmin->data.val.ival || emin->data.val.ival > cmax->data.val.ival)
			return 0;
	}
	if (t->type->localtype == TYPE_lng || t->type->localtype == TYPE_timestamp) {
		atom *cmin = atom_general(sql->sa, t, min);
		atom *cmax = atom_general(sql->sa, t, max);

		if (emax->data.val.lval < cmin->data.val.lval || emin->data.val.lval > cmax->data.val.lval)
			return 0;
	}
	return 1;
}

static sql_rel *
rel_rename_part(mvc *sql, sql_rel *p, char *tname, sql_table *mt) 
{
	node *n, *m;

	assert(list_length(p->exps) >= list_length(mt->columns.set));
	for( n = p->exps->h, m = mt->columns.set->h; n && m; n = n->next, m = m->next) {
		sql_exp *ne = n->data;
		sql_column *c = m->data;

		exp_setname(sql->sa, ne, tname, c->base.name);
	}
	if (n) /* skip TID */
		n = n->next;
	if (mt->idxs.set) {
		/* also possible index name mismatches */
		for( m = mt->idxs.set->h; n && m; m = m->next) {
			sql_exp *ne = n->data;
			sql_idx *i = m->data;
			char *iname = NULL;

			if (hash_index(i->type) && list_length(i->columns) <= 1)
				continue;

			iname = sa_strconcat( sql->sa, "%", i->base.name);
			exp_setname(sql->sa, ne, tname, iname);
			n = n->next;
		}
	}
	return p;
}


/* rewrite merge tables into union of base tables and call optimizer again */
static sql_rel *
rel_merge_table_rewrite(int *changes, mvc *sql, sql_rel *rel)
{
	sql_rel *sel = NULL;

	if (is_select(rel->op) && rel->l) {
		sel = rel;
		rel = rel->l;
	}
	if (is_basetable(rel->op) && rel->l) {
		sql_table *t = rel->l;

		if (isMergeTable(t)) {
			/* instantiate merge tabel */
			sql_rel *nrel = NULL;
			char *tname = t->base.name;
			list *cols = NULL, *low = NULL, *high = NULL;

			if (list_empty(t->members.set)) 
				return rel;
			if (sel) {
				node *n;

				/* no need to reduce the tables list */
				if (list_length(t->members.set) <= 1) 
					return sel;

				cols = sa_list(sql->sa);
				low = sa_list(sql->sa);
				high = sa_list(sql->sa);
				for(n = sel->exps->h; n; n = n->next) {
					sql_exp *e = n->data;	
					atom *lval = NULL, *hval = NULL;

					if (e->type == e_cmp && (e->flag == cmp_equal || e->f )) {
						sql_exp *l = e->r;
						sql_exp *h = e->f;
						sql_exp *c = e->l;

						c = rel_find_exp(rel, c);
						lval = exp_flatten(sql, l);
						if (!h)
							hval = lval;
						else if (h) 
							hval = exp_flatten(sql, h);
						if (c && lval && hval) {
							append(cols, c);
							append(low, lval);
							append(high, hval);
						}
					}
					/* handle in lists */
					if (e->type == e_cmp && e->flag == cmp_in) {
						list *vals = e->r;
						sql_exp *c = e->l;
						node *n;
						list *vlist = sa_list(sql->sa);

						c = rel_find_exp(rel, c);
						if (c) {
							for ( n = vals->h; n; n = n->next) {
								sql_exp *l = n->data;
								atom *lval = exp_flatten(sql, l);

								if (!lval)
									break;
								append(vlist, lval);
							}
							if (!n) {
								append(cols, c);
								append(low, NULL); /* mark high as value list */
								append(high, vlist);
							}
						}
					}
				}
			}
			(*changes)++;
			if (t->members.set) {
				list *tables = sa_list(sql->sa);
				node *nt;
				int *pos = NULL, nr = list_length(rel->exps), first = 1;

				/* rename (mostly the idxs) */
				pos = SA_NEW_ARRAY(sql->sa, int, nr);
				memset(pos, 0, sizeof(int)*nr);
				for (nt = t->members.set->h; nt; nt = nt->next) {
					sql_part *pd = nt->data;
					sql_table *pt = find_sql_table(t->s, pd->base.name);
					sql_rel *prel = rel_basetable(sql, pt, tname);
					node *n;
					int skip = 0, j;
					list *exps = NULL;

					/* do not include empty partitions */
					if ((nrel || nt->next) && 
					   pt && isTable(pt) && pt->access == TABLE_READONLY && !store_funcs.count_col(sql->session->tr, pt->columns.set->h->data, 1)){
						continue;
					}

					prel = rel_rename_part(sql, prel, tname, t);

					MT_lock_set(&prel->exps->ht_lock);
					prel->exps->ht = NULL;
					MT_lock_unset(&prel->exps->ht_lock);
					exps = sa_list(sql->sa);
					for (n = rel->exps->h, j=0; n && (!skip || first); n = n->next, j++) {
						sql_exp *e = n->data, *ne = NULL;
						int i;

						if (e)
							ne = exps_bind_column2(prel->exps, e->l, e->r);
						if (!e || !ne) {
							(*changes)--;
							assert(0);
							return rel;
						}
						if (pt && isTable(pt) && pt->access == TABLE_READONLY && sel && (nrel || nt->next) && 
							((first && (i=find_col_exp(cols, e)) != -1) ||
							(!first && pos[j] > 0))) {
							/* check if the part falls within the bounds of the select expression else skip this (keep at least on part-table) */
							void *min, *max;
							sql_column *col = NULL;
							sql_rel *bt = NULL;

							if (first)
								pos[j] = i + 1;
							i = pos[j] - 1;
							col = name_find_column(prel, e->l, e->r, -2, &bt);
							assert(col);
							if (sql_trans_ranges(sql->session->tr, col, &min, &max)) {
								atom *lval = list_fetch(low,i);
								atom *hval = list_fetch(high,i);

								if (lval && !exp_range_overlap(sql, e, min, max, lval, hval))
									skip = 1;
								else if (!lval) {
									node *n;
									list *l = list_fetch(high,i);

									skip = 1;
									for (n = l->h; n && skip; n = n->next) {
										hval = lval = n->data;

										if (exp_range_overlap(sql, e, min, max, lval, hval))
											skip = 0;
									}
								}
							}
						}
						assert(e->type == e_column);
						exp_setname(sql->sa, ne, e->l, e->r);
						append(exps, ne);
					}
					prel->exps = exps;
					first = 0;
					if (!skip) {
						append(tables, prel);
						nrel = prel;
					} else {
						sql->caching = 0;
					}
				}
				while (list_length(tables) > 1) {
					list *ntables = sa_list(sql->sa);
					node *n;

					for(n=tables->h; n && n->next; n = n->next->next) {
						sql_rel *l = n->data;
						sql_rel *r = n->next->data;
						nrel = rel_setop(sql->sa, l, r, op_union);
						nrel->exps = rel_projections(sql, rel, NULL, 1, 1);
						set_processed(nrel);
						append(ntables, nrel);
					}
					if (n)
						append(ntables, n->data);
					tables = ntables;
				}
			}
			if (nrel && list_length(t->members.set) == 1) {
				nrel = rel_project(sql->sa, nrel, rel->exps);
			} else if (nrel)
				nrel->exps = rel->exps;
			rel_destroy(rel);
			if (sel) {
				int changes = 0;
				sel->l = nrel;
				sel = rewrite_topdown(sql, sel, &rel_push_select_down_union, &changes); 
				if (changes)
					sel = rewrite(sql, sel, &rel_push_project_up, &changes); 
				return sel;
			}
			return nrel;
		}
	}
	if (sel)
		return sel;
	return rel;
}

/* TODO move all apply related stuff in to rel_apply.c/h */
static int exps_uses_exps(list *users, list *exps);

static int
exp_uses_exps(sql_exp *e, list *exps)
{
	sql_exp *ne = NULL;

	switch(e->type) {
	case e_column:
		if (e->l) {
			ne = exps_bind_column2(exps, e->l, e->r);
		} else {
			ne = exps_bind_column(exps, e->r, NULL);
		}
		return (ne != NULL);
	case e_convert:
		return exp_uses_exps(e->l, exps);
	case e_aggr:
	case e_func: 
		if (e->l) 
			return exps_uses_exps(e->l, exps);
		break;
	case e_cmp:	
		if (get_cmp(e) == cmp_or) {
			return (exps_uses_exps(e->l, exps) || exps_uses_exps(e->r, exps));
		} else if (e->flag == cmp_in || e->flag == cmp_notin || get_cmp(e) == cmp_filter) {
			return (exp_uses_exps(e->l, exps) || exps_uses_exps(e->r, exps));
		} else {
			return (exp_uses_exps(e->l, exps) || exp_uses_exps(e->r, exps));
		}
	case e_psm:	
		return 0;
	case e_atom:
		return 0;
	}
	return 0;
}

static int
exps_uses_exps(list *users, list *exps)
{
	node *n;

	if (!users)
		return 0;
	for(n=users->h; n; n = n->next) 
		if (exp_uses_exps(n->data, exps))
			return 1;
	return 0;
}

static int
rel_uses_exps(sql_rel *rel, list *exps )
{
	if (!exps || !rel)
		return 0;
	if (rel->op == op_project && !rel->l && rel->exps && exps_uses_exps(rel->exps, exps)) 
		return 1;
	switch(rel->op) {
	case op_basetable:
	case op_table:
		return 0;
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
		return (rel_uses_exps(rel->l, exps) ||	rel_uses_exps(rel->r, exps)); 
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		return (rel_uses_exps(rel->l, exps)); 
	case op_ddl: 
		if (rel_uses_exps(rel->l, exps))
			return 1;
		if (rel->r)
			return rel_uses_exps(rel->r, exps);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		return rel_uses_exps(rel->r, exps);
	}
	return 0;
}

static sql_exp * exp_apply_rename(mvc *sql, sql_exp *e, list *aliases, int always);

static list *
exps_apply_rename(mvc *sql, list *l, list *aliases, int always) 
{
	node *n;
	list *nl = new_exp_list(sql->sa);

	if (!l || !l->h)
		return nl;
	for(n=l->h; n; n=n->next) {
		sql_exp *arg = n->data, *narg;

		narg = exp_apply_rename(sql, arg, aliases, always);
		if (!narg) 
			narg = arg;
		narg->flag = arg->flag;
		append(nl, narg);
	}
	return nl;
}

static sql_exp *
exp_apply_rename(mvc *sql, sql_exp *e, list *aliases, int setname) 
{
	sql_exp *ne = NULL, *l;

	switch(e->type) {
	case e_column:
		ne = exps_bind_alias(aliases, e->l, e->r);
		if (ne && ne->used && !setname) {
			ne = exp_column(sql->sa, exp_relname(ne), exp_name(ne), exp_subtype(e), e->card, has_nil(e), is_intern(e));
			if (e && e->rname && e->rname[0] == 'L')
				exp_setname(sql->sa, ne, e->rname, e->name);
		} else if (ne && !ne->used)
			ne = NULL;
		break;
	case e_cmp: 
		if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
			list *l = exps_apply_rename(sql, e->l, aliases, setname);
			list *r = exps_apply_rename(sql, e->r, aliases, setname);
			if (l && r) {
				if (get_cmp(e) == cmp_filter) 
					ne = exp_filter(sql->sa, l, r, e->f, is_anti(e));
				else
					ne = exp_or(sql->sa, l, r, is_anti(e));
			}
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			sql_exp *l = exp_apply_rename(sql, e->l, aliases, setname);
			list *r = exps_apply_rename(sql, e->r, aliases, setname);

			if (!l)
				l = e->l;
			if (l && r)
				ne = exp_in(sql->sa, l, r, e->flag);
		} else {
			sql_exp *l = exp_apply_rename(sql, e->l, aliases, setname);
			sql_exp *r = exp_apply_rename(sql, e->r, aliases, setname);

			if (!l) 
				l = e->l;
			if (!r) 
				r = e->r;
			if (e->f) {
				sql_exp *r2 = exp_apply_rename(sql, e->f, aliases, setname);
				if (!r2)
					r2 = e->f;
				if (l && r && r2)
					ne = exp_compare2(sql->sa, l, r, r2, e->flag);
			} else if (l && r) {
				ne = exp_compare(sql->sa, l, r, e->flag);
			}
		}
		break;
	case e_convert:
		l = exp_apply_rename(sql, e->l, aliases, setname);
		if (l)
			ne = exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		break;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!l) {
			return e;
		} else {
			nl = exps_apply_rename(sql, l, aliases, setname);
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
	if (ne && e->p)
		ne->p = prop_copy(sql->sa, e->p);
	if (ne && !ne->used && e->rname)
		exp_setname(sql->sa, ne, e->rname, e->name);
	return ne;
}

static sql_rel *
rel_rename(mvc *sql, sql_rel *rel, list *conflicts)
{
	if (!rel)
		return rel;

	switch(rel->op) {
	case op_basetable:
	case op_table: {
		rel->exps = exps_apply_rename(sql, rel->exps, conflicts, 1);
		return rel;
	}
	case op_select: 
	case op_topn: 
	case op_sample: 
		rel->exps = exps_apply_rename(sql, rel->exps, conflicts, 0);
		rel->l = rel_rename(sql, rel->l, conflicts);
		return rel;
	case op_project:
	case op_groupby: 
		if (rel->r)
			rel->r = exps_apply_rename(sql, rel->r, conflicts, 0);
		rel->exps = exps_apply_rename(sql, rel->exps, conflicts, 0);
		rel->l = rel_rename(sql, rel->l, conflicts);
		return rel;
	case op_ddl: 
		rel->l = rel_rename(sql, rel->l, conflicts);
		if (rel->r)
			rel->r = rel_rename(sql, rel->r, conflicts);
		return rel;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->exps = exps_apply_rename(sql, rel->exps, conflicts, 0);
		if (!is_semi(rel->op))
			rel->l = rel_rename(sql, rel->l, conflicts);
		rel->r = rel_rename(sql, rel->r, conflicts);
		return rel;
	case op_apply:
		rel->exps = exps_apply_rename(sql, rel->exps, conflicts, 0);
		rel->l = rel_rename(sql, rel->l, conflicts);
		rel->r = rel_rename(sql, rel->r, conflicts);
		return rel;
	case op_insert:
	case op_update:
	case op_delete:
		rel->exps = exps_apply_rename(sql, rel->exps, conflicts, 0);
		rel->r = rel_rename(sql, rel->r, conflicts);
		return rel;
	}
	assert(0);
	return rel;
}

static int
exps_count_conflicts(list *conflicts)
{
	node *n;
	int cnt = 0;

	if (!conflicts)
		return 0;
	for (n = conflicts->h; n; n = n->next) {
		sql_exp *e = n->data;

		cnt += e->used;
	}
	return cnt;
}

static void exp_mark_conflicts(mvc *sql, sql_exp *e, list *conflicts, int always);

static void
exps_mark_conflicts(mvc *sql, list *exps, list *conflicts, int always)
{
	node *n;

	if (!exps)
		return ;
	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		exp_mark_conflicts(sql, e, conflicts, always);
	}
}

static void 
exp_mark_conflicts(mvc *sql, sql_exp *e, list *conflicts, int always)
{
	if (e->type == e_column) {
		sql_exp *ne = exps_bind_alias(conflicts, exp_relname(e), exp_name(e));

		if (ne && (always || strcmp(exp_name(e), e->r) != 0 || strcmp(exp_relname(e), e->l) != 0)) {
			if (!ne->used)
				exp_label_table(sql->sa, ne, ++sql->label);
			ne->used = 1;
		}
	}
}

static void exp_find_conflicts(mvc *sql, sql_exp *e, list *aexps, list *conflicts);

static void
exps_find_conflicts(mvc *sql, list *exps, list *aexps, list *conflicts)
{
	node *n;

	if (!exps)
		return ;
	for (n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		exp_find_conflicts(sql, e, aexps, conflicts);
	}
}

static void 
exp_find_conflicts(mvc *sql, sql_exp *e, list *aexps, list *conflicts)
{
	switch(e->type) {
	case e_column: {
		sql_exp *ne = exps_bind_column2(aexps, e->l, e->r);
		if (ne) {
			ne = e;
			/* create a new expression */
			e = exp_column(sql->sa, e->l, e->r, exp_subtype(e), e->card, has_nil(e), is_intern(e));
			if (ne->p)
				e->p = prop_copy(sql->sa, ne->p);
			append(conflicts, e);
		}
		break;
	}
	case e_cmp: 
		if (get_cmp(e) == cmp_or || get_cmp(e) == cmp_filter) {
			exps_find_conflicts(sql, e->l, aexps, conflicts);
			exps_find_conflicts(sql, e->r, aexps, conflicts);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			exps_find_conflicts(sql, e->l, aexps, conflicts);
		} else {
			exp_find_conflicts(sql, e->l, aexps, conflicts);
			exp_find_conflicts(sql, e->r, aexps, conflicts);
			if (e->f) 
				exp_find_conflicts(sql, e->r, aexps, conflicts);
		}
		break;
	case e_convert:
		exp_find_conflicts(sql, e->l, aexps, conflicts);
		break;
	case e_aggr:
	case e_func: 
		if (e->l)
			exps_find_conflicts(sql, e->l, aexps, conflicts);
		break;
	case e_atom:
	case e_psm:
		break;
	}
}

static sql_rel * rel_apply_rename(mvc *sql, sql_rel *rel);

static sql_rel *
rel_find_conflicts(mvc *sql, sql_rel *rel, list *exps, list *conflicts)
{
	if (!rel)
		return rel;

	switch(rel->op) {
	case op_basetable:
	case op_table: 
		if (rel->op == op_basetable && rel->l)
			exps_find_conflicts(sql, rel->exps, exps, conflicts);
		exps_mark_conflicts(sql, rel->exps, conflicts, 1); 
		return rel;
	case op_select: 
		exps_find_conflicts(sql, rel->exps, exps, conflicts);
		/* fall through */
	case op_topn: 
	case op_sample: 
		rel->l = rel_find_conflicts(sql, rel->l, exps, conflicts);
		return rel;
	case op_project:
		if (rel->l)
			exps_find_conflicts(sql, rel->exps, exps, conflicts);
		if (rel->l && rel_uses_exps(rel->l, exps))
			rel->l = rel_find_conflicts(sql, rel->l, exps, conflicts);
		/* if project produces given names, then we have a conflict */
		if (rel->l)
			exps_mark_conflicts(sql, rel->exps, conflicts, 0); 
		return rel;
	case op_groupby: 
		exps_find_conflicts(sql, rel->exps, exps, conflicts);
		if (rel->r)
			exps_find_conflicts(sql, rel->r, exps, conflicts);
		rel->l = rel_find_conflicts(sql, rel->l, exps, conflicts);
		exps_mark_conflicts(sql, rel->exps, conflicts, 0); 
		return rel;
	case op_ddl: 
		return rel;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except:
		exps_find_conflicts(sql, rel->exps, exps, conflicts);
		rel->l = rel_find_conflicts(sql, rel->l, exps, conflicts);
		if (!is_semi(rel->op))
			rel->r = rel_find_conflicts(sql, rel->r, exps, conflicts);
		exps_mark_conflicts(sql, rel->exps, conflicts, 0); 
		return rel;
	case op_apply:
		/* First rename the lower level apply */
		rel = rel_apply_rename(sql, rel);
		rel->r = rel_find_conflicts(sql, rel->r, exps, conflicts);
		return rel;
	case op_insert:
	case op_update:
	case op_delete:
		exps_find_conflicts(sql, rel->exps, exps, conflicts);
		rel->r = rel_find_conflicts(sql, rel->r, exps, conflicts);
		return rel;
	}
	assert(0);
	return rel;
}

/* Before the apply rewriter, rename all expression which conflict with the
 * apply expressions. First we walk the tree to find the conflicts. The we
 * rename all these expressions.
 */
static sql_rel *
rel_apply_rename(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return rel;
	switch(rel->op) {
	case op_basetable:
		return rel;
	case op_table:
		if (rel->l && rel->flag != 2)
			rel->l = rel_apply_rename(sql, rel->l);
		return rel;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = rel_apply_rename(sql, rel->l);
		return rel;
	case op_ddl: 
		rel->l = rel_apply_rename(sql, rel->l);
		if (rel->r)
			rel->r = rel_apply_rename(sql, rel->r);
		return rel;
	case op_join:
	case op_left:
	case op_right:
	case op_full:

	case op_semi: 
	case op_anti: 

	case op_union: 
	case op_inter: 
	case op_except: 
		rel->l = rel_apply_rename(sql, rel->l);
		rel->r = rel_apply_rename(sql, rel->r);
		return rel;
	case op_apply: {
		list *conflicts = new_exp_list(sql->sa);

		rel->r = rel_find_conflicts(sql, rel->r, rel->exps, conflicts);
		if (list_length(conflicts) && exps_count_conflicts(conflicts)) 
			rel->r = rel_rename(sql, rel->r, conflicts);
		return rel;
	}
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rel_apply_rename(sql, rel->r);
		return rel;
	}
	assert(0);
	return rel;
}

static int
exps_from_rel( list *exps, sql_rel *rel )
{
	node *n;

	if (!rel || !exps)
		return 0;
	for (n=exps->h; n; n=n->next) {
		sql_exp *e = n->data;
		if (/*!exp_is_atom(e) &&*/ rel_find_exp(rel, e) )
			return 1;
	}
	return 0;
}


static sql_rel *
rel_apply(mvc *sql, sql_rel *l, sql_rel *r, list *exps, int flag)
{
	sql_rel *nl = rel_crossproduct(sql->sa, l, r, op_apply);
	assert(l&&r);
	nl->exps = exps;
	nl->flag = flag;
	return nl;
}

/* push down apply until its gone */
static sql_rel *
rel_apply_rewrite(int *changes, mvc *sql, sql_rel *rel) 
{
	sql_rel *l, *r, *topn = NULL;

	if (rel->op == op_project && rel->exps) { /* check card */
		node *n;

		for(n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_column && e->card < CARD_AGGR) {
				sql_exp *p = rel_find_exp(rel->l, e);

				if (p && p->card >= CARD_AGGR)
					e->card = p->card;
			}
		}
	}
	if (rel->op != op_apply)
		return rel;

	l = rel->l;
	r = rel->r;
	if (!rel->exps || list_length(rel->exps) == 0) {
		rel->op = op_join;
		(*changes)++;
		assert(0);
	}
	if (rel->flag == APPLY_EXISTS || rel->flag == APPLY_NOTEXISTS) { /* rewrite as semijoin (difference/join)*/ /* rewrite as antijoin (minus/join) */
		sql_exp *ident, *le = NULL;
		int flag = rel->flag;

		l = rel_add_identity(sql, rel_dup(l), &ident);
		r = rel_apply(sql, l, rel_dup(r), rel->exps, APPLY_JOIN);
		rel_destroy(rel);
		ident = exp_column(sql->sa, exp_relname(ident), exp_name(ident), exp_subtype(ident), ident->card, has_nil(ident), is_intern(ident));
		r = rel_label(sql, r, 0);

		/* look up the identity columns and label these */
		le = rel_bind_column(sql, r, ident->name, 0);
		l = rel_crossproduct(sql->sa, rel_dup(l), r, flag==APPLY_EXISTS?op_semi:op_anti);
		l->exps = new_exp_list(sql->sa);
		le = exp_compare(sql->sa, ident, le, cmp_equal);
		append(l->exps, le);

		(*changes)++;
		return l;
	}
	if (rel->flag == APPLY_LOJ && is_join(r->op)) {
		sql_rel *rl = r->l, *rr = r->r;
		int lused = 0;

		if (is_project(rl->op) && !rl->l)
			lused = rel_uses_exps(rl, rel->exps);
		if (lused) {
			list *exps = r->exps;

			l = rel_dup(l);
			r = rel_dup(rr);
			l = rel_crossproduct(sql->sa, l, r, op_left);
			l->exps = exps;
			(*changes)++;
			rel_destroy(rel);
			return l;
		}
	}
	if (rel->flag == APPLY_LOJ && r->op == op_project && exps_from_rel(r->exps, rel->l)) {
		sql_exp *ident, *le = NULL;

		l = rel_add_identity(sql, rel_dup(l), &ident);
		r = rel_apply(sql, l, rel_dup(r), rel->exps, APPLY_JOIN);
		ident = exp_column(sql->sa, exp_relname(ident), exp_name(ident), exp_subtype(ident), ident->card, has_nil(ident), is_intern(ident));

		rel_destroy(rel);
		rel = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));

		/* look up the identity column and label these */
		le = exps_bind_column2(rel->exps, exp_relname(ident), exp_name(ident));
		exp_label(sql->sa, le, ++sql->label);
		rel->exps->ht = NULL;
		le = exp_column(sql->sa, exp_relname(le), exp_name(le), exp_subtype(le), le->card, has_nil(le), is_intern(le));

		l = rel_crossproduct(sql->sa, rel_dup(l), rel, op_left);
		l->exps = new_exp_list(sql->sa);
		le = exp_compare(sql->sa, ident, le, cmp_equal);
		append(l->exps, le);
		(*changes)++;
		return l;
	}
	/* table function (TODO should output any input cols) */
	if (r->op == op_table && r->l && rel->flag != 2) {
		assert(0);
		r->l = rel->l;
		return r;
	} 
	if (r->op == op_table || r->op == op_basetable) {
		assert(0);
		if (rel->flag == APPLY_LOJ)
			rel->op = op_left;
		else
			rel->op = op_join;
		rel->exps = NULL;
		return rel;
	}
	if (r->op == op_union) { 
		list *p;
		sql_rel *nl, *nr;

		assert( rel_uses_exps(r->l, rel->exps) && rel_uses_exps(r->r, rel->exps));
		/* apply returns all input columns + the output of the inner relation */
		/* so we prepend the input colunms to the unions expression list */
		/* Push the apply down the union */
		p = rel_projections(sql, rel, NULL, 1, 1);
		nl = rel_apply(sql, rel_dup(rel->l), rel_dup(r->l), rel->exps, rel->flag);
		nr = rel_apply(sql, rel_dup(rel->l), rel_dup(r->r), rel->exps, rel->flag);
		nl = rel_project(sql->sa, nl, rel_projections(sql, nl, NULL, 1, 1));
		nr = rel_project(sql->sa, nr, rel_projections(sql, nr, NULL, 1, 1));
		l = rel_setop(sql->sa, nl, nr, op_union);
		l->flag = r->flag;
		l->exps = p; //list_merge(p, r->exps, (fdup)NULL);
		assert(list_length(nl->exps) == list_length(nr->exps) && list_length(nl->exps) == list_length(l->exps));
		set_processed(l);
		rel_destroy(rel);
		(*changes)++;
		return l;
	}
	if (r->op == op_topn || r->op == op_sample) {
		/* first handle project, then topn/sample */
		topn = rel_dup(r);
		r = r->l;
	}
	if (r->op == op_project) { /* merge projections */
		if (!r->l) { 
			sql_rel *nrel = rel_dup(l);

			rel_destroy(rel);
			rel = nrel;
		} else {
			list *p = rel_projections(sql, l, NULL, 1, 1);

			p = list_merge(p, r->exps, (fdup)NULL);
			r = rel_apply(sql, rel_dup(l), rel_dup(r->l), rel->exps, rel->flag);
			rel_destroy(rel);
			rel = rel_project(sql->sa, r, p);
		}
		if (topn) {
			topn->l = rel;
			rel = topn;
		}
		(*changes)++;
		return rel;
	}
	if (r->op == op_select) { 
		sql_rel *n = rel_apply(sql, rel_dup(rel->l), rel_dup(r->l), rel->exps, rel->flag);
		sql_rel *ns = rel_select(sql->sa, n, NULL);

		ns->exps = exps_copy(sql->sa, r->exps);
		rel_destroy(rel);
		(*changes)++;
		return ns;
	}
	if (is_join(r->op) || is_semi(r->op)) {
		/* if exps do not use the apply variables, push up the join */
		int lused = rel_uses_exps(r->l, rel->exps);
		int rused = rel_uses_exps(r->r, rel->exps);

		if (lused && !rused){
			sql_rel *nl = rel_apply(sql, rel_dup(rel->l), rel_dup(r->l), rel->exps, rel->flag);
			sql_rel *rr = rel_dup(r->r);

			nl = rel_crossproduct(sql->sa, nl, rr, r->op);
			nl->exps = exps_copy(sql->sa, r->exps);
			rel_destroy(rel);
			rel = nl; 
		} else if (rused && !lused){
			sql_rel *nr = rel_apply(sql, rel_dup(rel->l), rel_dup(r->r), rel->exps, rel->flag);
			sql_rel *rl = rel_dup(r->l);

			nr = rel_crossproduct(sql->sa, rl, nr, r->op);
			nr->exps = exps_copy(sql->sa, r->exps);
			rel_destroy(rel);
			rel = nr; 
		} else if (rused && lused) { /* both used */
			/* apply (R, (semi/anti(l,r)) -> semi/anti( apply(R,l), apply(R,r)) */
			sql_rel *nl = rel_apply(sql, rel_dup(rel->l), rel_dup(r->l), rel->exps, rel->flag);
			sql_rel *nr = rel_apply(sql, rel_dup(rel->l), rel_dup(r->r), rel->exps, rel->flag);

			l = rel_crossproduct(sql->sa, nl, nr, r->op);
			l->exps = exps_copy(sql->sa, r->exps);
			rel_destroy(rel);
			(*changes)++;
			return l;
		} else { /* both unused */
			int flag = rel->flag;

			assert(is_join(r->op) || is_semi(r->op));
			if (is_join(r->op)) {
				list *exps = r->exps;

				r = rel_crossproduct(sql->sa, rel_dup(r->l), rel_dup(r->r), flag == APPLY_LOJ?op_left:op_join);
				r->exps = exps_copy(sql->sa, exps);
			} else if (is_semi(r->op)) {
				assert(flag != APPLY_LOJ);
				r = rel_dup(r);
			}
			rel_destroy(rel);
			(*changes)++;
			return r;
		}
		(*changes)++;
		return rel;
	}
	if (r->op == op_groupby) { /* groupby */ 
		node *n;
		sql_exp *ident;
		list *ogbe = r->r, *aggr = NULL, *gbe = new_exp_list(sql->sa);
		int has_gbe = (ogbe && list_length(ogbe) > 0);

		/* add project + identity around l */
		l = rel_add_identity(sql, rel_dup(l), &ident);
		ident = exp_column(sql->sa, exp_relname(ident), exp_name(ident), exp_subtype(ident), ident->card, has_nil(ident), is_intern(ident));
		list_append(gbe, ident);

		aggr = rel_projections(sql, l, NULL, 1, 1); /* columns of R */
		if (has_gbe)
			list_merge(gbe, ogbe, (fdup)NULL);
		for(n=r->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* count_nil(*) -> count(t.TID) */
			if (!has_gbe && e->type == e_aggr && strcmp(((sql_subaggr *)e->f)->aggr->base.name, "count") == 0 && !e->l) {
				sql_rel *rl = r->l;
				sql_exp *col = NULL;
				list *l = new_exp_list(sql->sa);

				/* find right hand TID column */
				if (is_join(rl->op)) {
					sql_rel *r = rl->r;
					if (!is_project(r->op)) 
						rl->r = r = rel_project(sql->sa, r, rel_projections(sql, r, NULL, 1, 1));
					col = r->exps->t->data;
					/* if not TID column create one */
					if (strcmp(exp_name(col), TID) != 0) {
						col = exp_column(sql->sa, exp_relname(col), exp_name(col), exp_subtype(col), col->card, has_nil(col), is_intern(col));
						col = exp_unop(sql->sa, col, sql_bind_func(sql->sa, NULL, "identity", exp_subtype(col), NULL, F_FUNC));
						exp_label(sql->sa, col, ++sql->label);
						append(r->exps, col);
					}
				} else if (!is_project(rl->op)) {	
					rl = rel_project(sql->sa, rl, rel_projections(sql, rl, NULL, 1, 1));
					r->l = rl;
					col = rl->exps->t->data;
				} else if (is_project(rl->op) && rl->exps) {
					col = rl->exps->t->data;
					col = exp_column(sql->sa, exp_relname(col), exp_name(col), exp_subtype(col), col->card, has_nil(col), is_intern(col));
					col = exp_unop(sql->sa, col, sql_bind_func(sql->sa, NULL, "identity", exp_subtype(col), NULL, F_FUNC));
					exp_label(sql->sa, col, ++sql->label);
					append(rl->exps, col);
				}
				assert(col);

				col = exp_column(sql->sa, exp_relname(col), exp_name(col), exp_subtype(col), col->card, has_nil(col), is_intern(col));
				e->l = l;
				append(l, col);
				set_no_nil(e);
			}
			if (e->type == e_aggr && e->card < CARD_AGGR) /* also fix projects, see above */
				e->card = CARD_AGGR;
			append(aggr, e);
		}
		l = rel_apply(sql, l, rel_dup(r->l), rel->exps, rel->flag);
		l = rel_groupby(sql, l, gbe);

		list_merge(l->exps, aggr, (fdup)NULL);	

		rel_destroy(rel);
		(*changes)++;
		rel = l;
	}
	return rel;
}

static list * rewrite_exps(mvc *sql, list *l, rewrite_rel_fptr rewrite_rel, rewrite_fptr rewriter, int *has_changes);

static sql_exp *
rewrite_exp(mvc *sql, sql_exp *e, rewrite_rel_fptr rewrite_rel, rewrite_fptr rewriter, int *has_changes)
{
	if (e->type != e_psm)
		return e;
	if (e->flag & PSM_VAR) 
		return e;
	if (e->flag & PSM_SET || e->flag & PSM_RETURN) {
		e->l = rewrite_exp(sql, e->l, rewrite_rel, rewriter, has_changes);
	}
	if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
		e->l = rewrite_exp(sql, e->l, rewrite_rel, rewriter, has_changes);
		e->r = rewrite_exps(sql, e->r, rewrite_rel, rewriter, has_changes);
		if (e->f)
			e->f = rewrite_exps(sql, e->f, rewrite_rel, rewriter, has_changes);
		return e;
	}
	if (e->flag & PSM_REL) 
		e->l = rewrite_rel(sql, e->l, rewriter, has_changes);
	return e;
}

static list *
rewrite_exps(mvc *sql, list *l, rewrite_rel_fptr rewrite_rel, rewrite_fptr rewriter, int *has_changes)
{
	node *n;

	if (!l)
		return l;
	for(n = l->h; n; n = n->next) 
		n->data = rewrite_exp(sql, n->data, rewrite_rel, rewriter, has_changes);
	return l;
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

	case op_apply: 
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
	case op_sample: 
		rel->l = rewrite(sql, rel->l, rewriter, has_changes);
		break;
	case op_ddl: 
		if (rel->flag == DDL_PSM && rel->exps) 
			rel->exps = rewrite_exps(sql, rel->exps, &rewrite, rewriter, has_changes);
		rel->l = rewrite(sql, rel->l, rewriter, has_changes);
		if (rel->r)
			rel->r = rewrite(sql, rel->r, rewriter, has_changes);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->l = rewrite(sql, rel->l, rewriter, has_changes);
		rel->r = rewrite(sql, rel->r, rewriter, has_changes);
		break;
	}
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

	rel = rewriter(has_changes, sql, rel);
	switch (rel->op) {
	case op_basetable:
	case op_table:
		if (rel->op == op_table && rel->l && rel->flag != 2) 
			rel->l = rewrite(sql, rel->l, rewriter, has_changes);
		if (rel->op == op_table && rel->l && rel->flag != 2) 
			rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
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
		rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
		rel->r = rewrite_topdown(sql, rel->r, rewriter, has_changes);
		break;
	case op_project:
	case op_select: 
	case op_groupby: 
	case op_topn: 
	case op_sample: 
		rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
		break;
	case op_ddl: 
		if (rel->flag == DDL_PSM && rel->exps) 
			rewrite_exps(sql, rel->exps, &rewrite_topdown, rewriter, has_changes);
		rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
		if (rel->r)
			rel->r = rewrite_topdown(sql, rel->r, rewriter, has_changes);
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->l = rewrite_topdown(sql, rel->l, rewriter, has_changes);
		rel->r = rewrite_topdown(sql, rel->r, rewriter, has_changes);
		break;
	}
	return rel;
}

static sql_rel *
optimize_rel(mvc *sql, sql_rel *rel, int *g_changes, int level) 
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
	if (gp.cnt[op_project] || gp.cnt[op_ddl]) {
		rel = rewrite(sql, rel, &rel_merge_projects, &changes);
		if (level <= 0) {
			rel = rewrite(sql, rel, &rel_case_fixup, &changes);
			rel = rewrite(sql, rel, &rel_simplify_math, &changes);
			rel = rewrite(sql, rel, &rel_distinct_project2groupby, &changes);
		}
	}
	/* push (simple renaming) projections up */
	if (gp.cnt[op_project]) 
		rel = rewrite(sql, rel, &rel_push_project_up, &changes); 
	if (level <= 0 && (gp.cnt[op_project] || gp.cnt[op_groupby])) 
		rel = rel_split_project(&changes, sql, rel, 1);

	if ((gp.cnt[op_select] || gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full]) && level <= 0)
		rel = rewrite(sql, rel, &rel_simplify_predicates, &changes); 

	/* join's/crossproducts between a relation and a constant (row).
	 * could be rewritten 
	 *
	 * also joins between a relation and a DICT (which isn't used)
	 * could be removed.
	 * */
	if (gp.cnt[op_join] && gp.cnt[op_project])
		rel = rewrite(sql, rel, &rel_remove_join, &changes); 

	if (gp.cnt[op_join] || 
	    gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full] || 
	    gp.cnt[op_semi] || gp.cnt[op_anti] ||
	    gp.cnt[op_select]) {
		rel = rewrite(sql, rel, &rel_find_range, &changes);
		rel = rel_project_reduce_casts(&changes, sql, rel);
		rel = rewrite(sql, rel, &rel_reduce_casts, &changes);
	}

	if (gp.cnt[op_union])
		rel = rewrite(sql, rel, &rel_merge_union, &changes); 

	if (gp.cnt[op_select] || gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full])
		rel = rewrite(sql, rel, &rel_select_cse, &changes); 

	if (gp.cnt[op_project]) 
		rel = rewrite(sql, rel, &rel_project_cse, &changes);

	rel = rewrite(sql, rel, &rel_rewrite_types, &changes); 

	if (gp.cnt[op_anti] || gp.cnt[op_semi]) {
		/* rewrite semijoin (A, join(A,B)) into semijoin (A,B) */
		rel = rewrite(sql, rel, &rel_rewrite_semijoin, &changes);
		/* push semijoin through join */
		rel = rewrite(sql, rel, &rel_push_semijoin_down, &changes);
		/* antijoin(a, union(b,c)) -> antijoin(antijoin(a,b), c) */
		rel = rewrite(sql, rel, &rel_rewrite_antijoin, &changes);
		if (level <= 0)
			rel = rewrite_topdown(sql, rel, &rel_semijoin_use_fk, &changes);
	}

	if (gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full]) 
		rel = rewrite_topdown(sql, rel, &rel_split_outerjoin, &changes);

	if (gp.cnt[op_select] || gp.cnt[op_semi]) {
		/* only once */
		if (level <= 0)
			rel = rewrite(sql, rel, &rel_merge_rse, &changes); 

		rel = rewrite_topdown(sql, rel, &rel_push_select_down, &changes); 
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	if (gp.cnt[op_select] && gp.cnt[op_join]) {
		rel = rewrite_topdown(sql, rel, &rel_push_select_down_join, &changes); 
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	/* rewrite all applies */
	if (level <= 0 && gp.cnt[op_apply]) {
		rel = rel_apply_rename(sql, rel); 
		rel = rewrite(sql, rel, &rel_apply_rewrite, &changes);
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
		rel = rewrite_topdown(sql, rel, &rel_push_select_down_union, &changes); 

	if (gp.cnt[op_groupby]) {
		rel = rewrite_topdown(sql, rel, &rel_push_aggr_down, &changes);
		rel = rewrite_topdown(sql, rel, &rel_push_groupby_down, &changes);
		rel = rewrite(sql, rel, &rel_groupby_order, &changes); 
		rel = rewrite(sql, rel, &rel_reduce_groupby_exps, &changes); 
		rel = rewrite(sql, rel, &rel_groupby_distinct, &changes); 
	}

	if (gp.cnt[op_join] || gp.cnt[op_left] || gp.cnt[op_semi] || gp.cnt[op_anti]) {
		rel = rel_remove_empty_join(sql, rel, &changes);
		if (!gp.cnt[op_update])
			rel = rel_join_order(sql, rel);
		rel = rewrite(sql, rel, &rel_push_join_down_union, &changes); 
		/* rel_join_order may introduce empty selects */
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	if (gp.cnt[op_select] && sql->emode != m_prepare) 
		rel = rewrite(sql, rel, &rel_simplify_like_select, &changes); 

	if (gp.cnt[op_select]) 
		rel = rewrite(sql, rel, &rel_select_order, &changes); 

	if (gp.cnt[op_select] || gp.cnt[op_join])
		rel = rewrite(sql, rel, &rel_use_index, &changes); 

	if (gp.cnt[op_project]) 
		rel = rewrite_topdown(sql, rel, &rel_push_project_down_union, &changes);

	/* Remove unused expressions */
	if (level <= 0)
		rel = rel_dce(sql, rel);

	if (gp.cnt[op_join] || gp.cnt[op_left] || gp.cnt[op_right] || gp.cnt[op_full] || 
	    gp.cnt[op_semi] || gp.cnt[op_anti] || gp.cnt[op_select]) {
		rel = rewrite(sql, rel, &rel_push_func_down, &changes);
		rel = rewrite_topdown(sql, rel, &rel_push_select_down, &changes); 
		rel = rewrite(sql, rel, &rel_remove_empty_select, &e_changes); 
	}

	if (!changes && gp.cnt[op_topn]) {
		rel = rewrite_topdown(sql, rel, &rel_push_topn_down, &changes); 
		changes = 0;
	}

	rel = rewrite_topdown(sql, rel, &rel_merge_table_rewrite, &changes);
	if (level <= 0 && mvc_debug_on(sql,8))
		rel = rewrite_topdown(sql, rel, &rel_add_dicts, &changes);
	*g_changes = changes;
	return rel;
}

static void
rel_reset_subquery(sql_rel *rel)
{
	if (!rel)
		return;

	rel->subquery = 0;
	switch(rel->op){
	case op_basetable:
	case op_table:
	case op_ddl:

	case op_insert:
	case op_update:
	case op_delete:
		break;
	case op_select:
	case op_topn:
	case op_sample:

	case op_project:
	case op_groupby:
		if (rel->l)
			rel_reset_subquery(rel->l);
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
		if (rel->l)
			rel_reset_subquery(rel->l);
		if (rel->r)
			rel_reset_subquery(rel->r);
	}

}

static sql_rel *
optimize(mvc *sql, sql_rel *rel) 
{
	int level = 0, changes = 1;

	rel_reset_subquery(rel);
	for( ;rel && level < 20 && changes; level++) 
		rel = optimize_rel(sql, rel, &changes, level);
	return rel;
}

sql_rel *
rel_optimizer(mvc *sql, sql_rel *rel) 
{
	rel = optimize(sql, rel);
	if (sql->sqs) {
		node *n;

		for(n = sql->sqs->h; n; n = n->next) {
			sql_var *v = n->data;

			v->rel = optimize(sql, v->rel);
		}
	}
	return rel;
}
