/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "rel_optimizer.h"
#include "rel_rel.h"
#include "rel_basetable.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_planner.h"
#include "rel_propagate.h"
#include "rel_distribute.h"
#include "rel_rewriter.h"
#include "sql_mvc.h"
#include "sql_privileges.h"

typedef struct global_props {
	int cnt[ddl_maxops];
	uint8_t
		instantiate:1,
		needs_mergetable_rewrite:1,
		needs_remote_replica_rewrite:1,
		needs_distinct:1;
} global_props;

static int
find_member_pos(list *l, sql_table *t)
{
	int i = 0;
	if (l) {
		for (node *n = l->h; n ; n = n->next, i++) {
			sql_part *pt = n->data;
			if (pt->member == t->base.id)
				return i;
		}
	}
	return -1;
}

/* The important task of the relational optimizer is to optimize the
   join order.

   The current implementation chooses the join order based on
   select counts, ie if one of the join sides has been reduced using
   a select this join is choosen over one without such selections.
 */

/* currently we only find simple column expressions */
sql_column *
name_find_column( sql_rel *rel, const char *rname, const char *name, int pnr, sql_rel **bt )
{
	sql_exp *alias = NULL;
	sql_column *c = NULL;

	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (rel->exps) {
			sql_exp *e;

			if (rname)
				e = exps_bind_column2(rel->exps, rname, name, NULL);
			else
				e = exps_bind_column(rel->exps, name, NULL, NULL, 0);
			if (!e || e->type != e_column)
				return NULL;
			if (e->l)
				rname = e->l;
			name = e->r;
		}
		if (rname && strcmp(t->base.name, rname) != 0)
			return NULL;
		sql_table *mt = rel_base_get_mergetable(rel);
		if (ol_length(t->columns)) {
			for (node *cn = ol_first_node(t->columns); cn; cn = cn->next) {
				sql_column *c = cn->data;
				if (strcmp(c->base.name, name) == 0) {
					*bt = rel;
					if (pnr < 0 || (mt &&
						find_member_pos(mt->members, c->t) == pnr))
						return c;
				}
			}
		}
		if (name[0] == '%' && ol_length(t->idxs)) {
			for (node *cn = ol_first_node(t->idxs); cn; cn = cn->next) {
				sql_idx *i = cn->data;
				if (strcmp(i->base.name, name+1 /* skip % */) == 0) {
					*bt = rel;
					if (pnr < 0 || (mt &&
						find_member_pos(mt->members, i->t) == pnr)) {
						sql_kc *c = i->columns->h->data;
						return c->c;
					}
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
		/* first right (possible subquery) */
		c = name_find_column( rel->r, rname, name, pnr, bt);
		/* fall through */
	case op_semi:
	case op_anti:
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
			alias = exps_bind_column2(rel->exps, rname, name, NULL);
		else
			alias = exps_bind_column(rel->exps, name, NULL, NULL, 1);
		if (is_groupby(rel->op) && alias && alias->type == e_column && !list_empty(rel->r)) {
			if (alias->l)
				alias = exps_bind_column2(rel->r, alias->l, alias->r, NULL);
			else
				alias = exps_bind_column(rel->r, alias->r, NULL, NULL, 1);
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
	case op_truncate:
	case op_merge:
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
	if (( e->l && (ne=exps_bind_column2(exps, e->l, e->r, NULL)) != NULL) ||
	   ((!e->l && (ne=exps_bind_column(exps, e->r, NULL, NULL, 1)) != NULL)))
		return ne;
	return NULL;
}

static int
kc_column_cmp(sql_kc *kc, sql_column *c)
{
	/* return on equality */
	return !(c == kc->c);
}

static sql_rel *
rel_properties(visitor *v, sql_rel *rel)
{
	global_props *gp = (global_props*)v->data;

	/* Don't flag any changes here! */
	gp->cnt[(int)rel->op]++;
	gp->needs_distinct |= need_distinct(rel);
	if (gp->instantiate && is_basetable(rel->op)) {
		mvc *sql = v->sql;
		sql_table *t = (sql_table *) rel->l;
		sql_part *pt;

		/* If the plan has a merge table or a child of one, then rel_merge_table_rewrite has to run */
		gp->needs_mergetable_rewrite |= (isMergeTable(t) || (t->s && t->s->parts && (pt = partition_find_part(sql->session->tr, t, NULL))));
		gp->needs_remote_replica_rewrite |= (isRemote(t) || isReplicaTable(t));
	}
	return rel;
}

static sql_rel * rel_join_order(visitor *v, sql_rel *rel) ;

static void
get_relations(visitor *v, sql_rel *rel, list *rels)
{
	if (!rel_is_ref(rel) && rel->op == op_join && rel->exps == NULL) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;

		get_relations(v, l, rels);
		get_relations(v, r, rels);
		rel->l = NULL;
		rel->r = NULL;
		rel_destroy(rel);
	} else {
		rel = rel_join_order(v, rel);
		append(rels, rel);
	}
}

static void
get_inner_relations(mvc *sql, sql_rel *rel, list *rels)
{
	if (!rel_is_ref(rel) && is_join(rel->op)) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;

		get_inner_relations(sql, l, rels);
		get_inner_relations(sql, r, rels);
	} else {
		append(rels, rel);
	}
}

static int
exp_count(int *cnt, sql_exp *e)
{
	int flag;
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
 		flag = e->flag;
		switch (flag) {
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
			if (e->l) {
				sql_exp *l = e->l;
				sql_subtype *t = exp_subtype(l);
				if (EC_TEMP(t->type->eclass)) /* give preference to temporal ranges */
					*cnt += 90;
			}
			if (e->f){ /* range */
				*cnt += 6;
				return 12;
			}
			return 6;
		case cmp_filter:
			if (exps_card(e->r) > CARD_AGGR) {
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
		case mark_in:
		case mark_notin:
			*cnt += 0;
			return 0;
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
		if (rel_has_exp(r, e->l, false) >= 0)
			return e->l;
		return e->r;
	}
	assert(0);
	return NULL;
}

static int
rel_has_exp2(sql_rel *r, sql_exp *e)
{
	return rel_has_exp(r, e, false);
}

static sql_column *
table_colexp(sql_exp *e, sql_rel *r)
{
	sql_table *t = r->l;

	if (e->type == e_column) {
		const char *name = exp_name(e);
		node *cn;

		if (r->exps) { /* use alias */
			for (cn = r->exps->h; cn; cn = cn->next) {
				sql_exp *ce = cn->data;
				if (strcmp(exp_name(ce), name) == 0) {
					name = ce->r;
					break;
				}
			}
		}
		for (cn = ol_first_node(t->columns); cn; cn = cn->next) {
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

	if (e->flag == cmp_or) {
		l = NULL;
	} else if (e->flag == cmp_filter) {
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
		list *n_rels = sa_list(sa);

		append(n_rels, l);
		append(n_rels, r);
		res = list_select(exps, n_rels, (fcmp) &exp_joins_rels, (fdup)NULL);
		return res;
	}
	return sa_list(sa);
}

static int
sql_column_kc_cmp(sql_column *c, sql_kc *kc)
{
	/* return on equality */
	return (c->colnr - kc->c->colnr);
}

static sql_idx *
find_fk_index(mvc *sql, sql_table *l, list *lcols, sql_table *r, list *rcols)
{
	sql_trans *tr = sql->session->tr;

	if (l->idxs) {
		node *in;
		for (in = ol_first_node(l->idxs); in; in = in->next){
			sql_idx *li = in->data;
			if (li->type == join_idx) {
				sql_key *rk = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)li->key)->rkey);
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
	list *res = sa_list(sql->sa);
	node *n = NULL;
	int i, *keys, cnt = list_length(dje);
	void **data;

	if (cnt == 0)
		return res;

	keys = SA_NEW_ARRAY(sql->ta, int, cnt);
	data = SA_NEW_ARRAY(sql->ta, void*, cnt);

	for (n = dje->h, i = 0; n; n = n->next, i++) {
		sql_exp *e = n->data;

		keys[i] = exp_keyvalue(e);
		/* add some weight for the selections */
		if (e->type == e_cmp && !is_complex_exp(e->flag)) {
			sql_rel *l = find_rel(rels, e->l);
			sql_rel *r = find_rel(rels, e->r);

			if (l && is_select(l->op) && l->exps)
				keys[i] += list_length(l->exps)*10 + exps_count(l->exps);
			if (r && is_select(r->op) && r->exps)
				keys[i] += list_length(r->exps)*10 + exps_count(r->exps);
		}
		data[i] = n->data;
	}
	/* sort descending */
	GDKqsort(keys, data, NULL, cnt, sizeof(int), sizeof(void *), TYPE_int, true, true);
	for(i=0; i<cnt; i++) {
		list_append(res, data[i]);
	}
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
	int len = list_length(aje), i, j;
	char *used = SA_ZNEW_ARRAY(aje->sa, char, len);
	list *res = sa_list(aje->sa);

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

			idx = find_fk_index(sql, l, lcols, r, rcols);
			if (!idx) {
				idx = find_fk_index(sql, r, rcols, l, lcols);
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
						list_remove_data(exps, NULL, n->data);
					append(exps, je);
					djn->data = je;
				} else if (swapped) { /* else keep je for single column expressions */
					je = exp_compare(sql->sa, je->r, je->l, cmp_equal);
					/* Remove all join expressions */
					for (n = eje->h; n; n = n->next)
						list_remove_data(exps, NULL, n->data);
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
order_joins(visitor *v, list *rels, list *exps)
{
	sql_rel *top = NULL, *l = NULL, *r = NULL;
	sql_exp *cje;
	node *djn;
	list *sdje, *n_rels = sa_list(v->sql->sa);
	int fnd = 0;
	unsigned int rsingle;

	/* find foreign keys and reorder the expressions on reducing quality */
	sdje = find_fk(v->sql, rels, exps);

	if (list_length(rels) > 2 && mvc_debug_on(v->sql, 256)) {
		for(djn = sdje->h; djn; djn = djn->next ) {
			sql_exp *e = djn->data;
			list_remove_data(exps, NULL, e);
		}
		top =  rel_planner(v->sql, rels, sdje, exps);
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
		if (cje->type != e_cmp || is_complex_exp(cje->flag) || !find_prop(cje->p, PROP_HASHCOL) ||
		   (cje->type == e_cmp && cje->f == NULL)) {
			l = find_one_rel(rels, cje->l);
			r = find_one_rel(rels, cje->r);
		}

		if (l && r && l != r) {
			list_remove_data(sdje, NULL, cje);
			list_remove_data(exps, NULL, cje);
		}
	}
	if (l && r && l != r) {
		list_remove_data(rels, NULL, l);
		list_remove_data(rels, NULL, r);
		list_append(n_rels, l);
		list_append(n_rels, r);

		/* Create a relation between l and r. Since the calling
	   	   functions rewrote the join tree, into a list of expressions
	   	   and a list of (simple) relations, there are no outer joins
	   	   involved, we can simply do a crossproduct here.
	 	 */
		rsingle = is_single(r);
		reset_single(r);
		top = rel_crossproduct(v->sql->sa, l, r, op_join);
		if (rsingle)
			set_single(r);
		rel_join_add_exp(v->sql->sa, top, cje);

		/* all other join expressions on these 2 relations */
		for (node *en = exps->h; en; ) {
			node *next = en->next;
			sql_exp *e = en->data;
			if (rel_rebind_exp(v->sql, top, e)) {
				rel_join_add_exp(v->sql->sa, top, e);
				list_remove_data(exps, NULL, e);
			}
			en = next;
		}
		/* Remove other joins on the current 'n_rels' set in the distinct list too */
		for (node *en = sdje->h; en; ) {
			node *next = en->next;
			sql_exp *e = en->data;
			if (rel_rebind_exp(v->sql, top, e))
				list_remove_data(sdje, NULL, en->data);
			en = next;
		}
		fnd = 1;
	}
	/* build join tree using the ordered list */
	while(list_length(exps) && fnd) {
		fnd = 0;
		/* find the first expression which could be added */
		if (list_length(sdje) > 1)
			sdje = order_join_expressions(v->sql, sdje, rels);
		for(djn = sdje->h; djn && !fnd && rels->h; djn = (!fnd)?djn->next:NULL) {
			node *ln, *rn, *en;

			cje = djn->data;
			ln = list_find(n_rels, cje->l, (fcmp)&rel_has_exp2);
			rn = list_find(n_rels, cje->r, (fcmp)&rel_has_exp2);

			if (ln && rn) {
				assert(0);
				/* create a selection on the current */
				l = ln->data;
				r = rn->data;
				rel_join_add_exp(v->sql->sa, top, cje);
				fnd = 1;
			} else if (ln || rn) {
				if (ln) {
					l = ln->data;
					r = find_rel(rels, cje->r);
				} else {
					l = rn->data;
					r = find_rel(rels, cje->l);
				}
				if (!r) {
					fnd = 1; /* not really, but this bails out */
					list_remove_data(sdje, NULL, cje); /* handle later as select */
					continue;
				}

				/* remove the expression from the lists */
				list_remove_data(sdje, NULL, cje);
				list_remove_data(exps, NULL, cje);

				list_remove_data(rels, NULL, r);
				append(n_rels, r);

				/* create a join using the current expression */
				rsingle = is_single(r);
				reset_single(r);
				top = rel_crossproduct(v->sql->sa, top, r, op_join);
				if (rsingle)
					set_single(r);
				rel_join_add_exp(v->sql->sa, top, cje);

				/* all join expressions on these tables */
				for (en = exps->h; en; ) {
					node *next = en->next;
					sql_exp *e = en->data;
					if (rel_rebind_exp(v->sql, top, e)) {
						rel_join_add_exp(v->sql->sa, top, e);
						list_remove_data(exps, NULL, e);
					}
					en = next;
				}
				/* Remove other joins on the current 'n_rels'
				   set in the distinct list too */
				for (en = sdje->h; en; ) {
					node *next = en->next;
					sql_exp *e = en->data;
					if (rel_rebind_exp(v->sql, top, e))
						list_remove_data(sdje, NULL, en->data);
					en = next;
				}
				fnd = 1;
			}
		}
	}
	if (list_length(rels)) { /* more relations */
		node *n;
		for(n=rels->h; n; n = n->next) {
			sql_rel *nr = n->data;

			if (top) {
				rsingle = is_single(nr);
				reset_single(nr);
				top = rel_crossproduct(v->sql->sa, top, nr, op_join);
				if (rsingle)
					set_single(nr);
			} else
				top = nr;
		}
	}
	if (list_length(exps)) { /* more expressions (add selects) */
		top = rel_select(v->sql->sa, top, NULL);
		for(node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* find the involved relations */

			/* complex expressions may touch multiple base tables
		 	 * Should be push up to extra selection. */
			/*
			l = find_one_rel(rels, e->l);
			r = find_one_rel(rels, e->r);

			if (l && r)
			*/
			if (exp_is_join_exp(e) == 0) {
				sql_rel *nr = NULL;
				if (is_theta_exp(e->flag)) {
					nr = rel_push_join(v->sql, top->l, e->l, e->r, e->f, e, 0);
				} else if (e->flag == cmp_filter || e->flag == cmp_or) {
					sql_exp *l = exps_find_one_multi_exp(e->l), *r = exps_find_one_multi_exp(e->r);
					if (l && r)
						nr = rel_push_join(v->sql, top->l, l, r, NULL, e, 0);
				}
				if (!nr)
					rel_join_add_exp(v->sql->sa, top->l, e);
			} else
				rel_select_add_exp(v->sql->sa, top, e);
		}
		if (list_empty(top->exps)) { /* empty select */
			sql_rel *l = top->l;
			top->l = NULL;
			rel_destroy(top);
			top = l;
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

static void _rel_destroy(void *dummy, sql_rel *rel)
{
	(void)dummy;
	rel_destroy(rel);
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
	rels->destroy = (fdestroy)_rel_destroy;
	list_destroy(rels);
	rels = nrels;

	/* one of the rels should be a op_union with nrcols == 0 */
	while (restart) {
		for (n = rels->h; n; n = n->next) {
			sql_rel *r = n->data;

			restart = 0;
			if (is_project(r->op) && r->nrcols == 0) {
				/* next step find expression on this relation */
				node *m;
				sql_rel *l = NULL;
				sql_exp *je = NULL;

				for(m = exps->h; !je && m; m = m->next) {
					sql_exp *e = m->data;

					if (e->type == e_cmp && e->flag == cmp_equal) {
						/* in values are on
							the right of the join */
						if (rel_has_exp(r, e->r, false) >= 0)
							je = e;
					}
				}
				/* with this expression find other relation */
				if (je && (l = find_rel(rels, je->l)) != NULL) {
					unsigned int rsingle = is_single(r);
					reset_single(r);
					sql_rel *nr = rel_crossproduct(sql->sa, l, r, op_join);
					if (rsingle)
						set_single(r);
					rel_join_add_exp(sql->sa, nr, je);
					list_append(rels, nr);
					list_remove_data(rels, NULL, l);
					list_remove_data(rels, NULL, r);
					list_remove_data(exps, NULL, je);
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
		} else if (!l) {
			l = r;
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
reorder_join(visitor *v, sql_rel *rel)
{
	list *exps;
	list *rels;

	if (rel->op == op_join && !rel_is_ref(rel))
		rel->exps = push_up_join_exps(v->sql, rel);

	exps = rel->exps;
	if (!exps) /* crosstable, ie order not important */
		return rel;
	rel->exps = NULL; /* should be all crosstables by now */
 	rels = sa_list(v->sql->sa);
	if (is_outerjoin(rel->op) || is_single(rel)) {
		sql_rel *l, *r;
		int cnt = 0;
		/* try to use an join index also for outer joins */
 		get_inner_relations(v->sql, rel, rels);
		cnt = list_length(exps);
		rel->exps = find_fk(v->sql, rels, exps);
		if (list_length(rel->exps) != cnt)
			rel->exps = order_join_expressions(v->sql, exps, rels);
		l = rel->l;
		r = rel->r;
		if (is_join(l->op))
			rel->l = reorder_join(v, rel->l);
		if (is_join(r->op))
			rel->r = reorder_join(v, rel->r);
	} else {
 		get_relations(v, rel, rels);
		if (list_length(rels) > 1) {
			rels = push_in_join_down(v->sql, rels, exps);
			rel = order_joins(v, rels, exps);
		} else {
			rel->exps = exps;
			exps = NULL;
		}
	}
	return rel;
}

static sql_rel *
rel_join_order(visitor *v, sql_rel *rel)
{
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

	case op_semi:
	case op_anti:

	case op_union:
	case op_inter:
	case op_except:
	case op_merge:
		rel->l = rel_join_order(v, rel->l);
		rel->r = rel_join_order(v, rel->r);
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		rel->l = rel_join_order(v, rel->l);
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			rel->l = rel_join_order(v, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel->l = rel_join_order(v, rel->l);
			rel->r = rel_join_order(v, rel->r);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rel_join_order(v, rel->r);
		break;
	case op_truncate:
		break;
	}
	if (is_join(rel->op) && rel->exps && !rel_is_ref(rel)) {
		if (rel && !rel_is_ref(rel))
			rel = reorder_join(v, rel);
	} else if (is_join(rel->op)) {
		rel->l = rel_join_order(v, rel->l);
		rel->r = rel_join_order(v, rel->r);
	}
	return rel;
}

/* exp_rename */
static sql_exp * exp_rename(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t);

static list *
exps_rename(mvc *sql, list *l, sql_rel *f, sql_rel *t)
{
	if (list_empty(l))
		return l;
	for (node *n=l->h; n; n=n->next)
		n->data = exp_rename(sql, n->data, f, t);
	return l;
}

/* exp_rename */
static sql_exp *
exp_rename(mvc *sql, sql_exp *e, sql_rel *f, sql_rel *t)
{
	sql_exp *ne = NULL;

	switch(e->type) {
	case e_column:
		if (e->l) {
			ne = exps_bind_column2(f->exps, e->l, e->r, NULL);
			/* if relation name matches expressions relation name, find column based on column name alone */
		} else {
			ne = exps_bind_column(f->exps, e->r, NULL, NULL, 1);
		}
		if (!ne)
			return e;
		e = NULL;
		if (exp_name(ne) && ne->r && ne->l)
			e = rel_bind_column2(sql, t, ne->l, ne->r, 0);
		if (!e && ne->r)
			e = rel_bind_column(sql, t, ne->r, 0, 1);
		if (!e) {
			sql->session->status = 0;
			sql->errstr[0] = 0;
			if (exp_is_atom(ne))
				return ne;
		}
		return exp_ref(sql, e);
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			e->l = exps_rename(sql, e->l, f, t);
			e->r = exps_rename(sql, e->r, f, t);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = exp_rename(sql, e->l, f, t);
			e->r = exps_rename(sql, e->r, f, t);
		} else {
			e->l = exp_rename(sql, e->l, f, t);
			e->r = exp_rename(sql, e->r, f, t);
			if (e->f)
				e->f = exp_rename(sql, e->f, f, t);
		}
		break;
	case e_convert:
		e->l = exp_rename(sql, e->l, f, t);
		break;
	case e_aggr:
	case e_func:
		e->l = exps_rename(sql, e->l, f, t);
		break;
	case e_atom:
		e->f = exps_rename(sql, e->f, f, t);
		break;
	case e_psm:
		break;
	}
	return e;
}

static int
can_push_func(sql_exp *e, sql_rel *rel, int *must, int depth)
{
	switch(e->type) {
	case e_cmp: {
		sql_exp *l = e->l, *r = e->r, *f = e->f;

		if (e->flag == cmp_or || e->flag == cmp_in || e->flag == cmp_notin || e->flag == cmp_filter)
			return 0;
		if (depth > 0) { /* for comparisons under the top ones, they become functions */
			int lmust = 0;
			int res = can_push_func(l, rel, &lmust, depth + 1) && can_push_func(r, rel, &lmust, depth + 1) &&
					(!f || can_push_func(f, rel, &lmust, depth + 1));
			if (res && !lmust)
				return 1;
			(*must) |= lmust;
			return res;
		} else {
			int mustl = 0, mustr = 0, mustf = 0;
			return ((l->type == e_column || can_push_func(l, rel, &mustl, depth + 1)) && (*must = mustl)) ||
					((r->type == e_column || can_push_func(r, rel, &mustr, depth + 1)) && (*must = mustr)) ||
					((f && (f->type == e_column || can_push_func(f, rel, &mustf, depth + 1)) && (*must = mustf)));
		}
	}
	case e_convert:
		return can_push_func(e->l, rel, must, depth + 1);
	case e_aggr:
	case e_func: {
		list *l = e->l;
		int res = 1, lmust = 0;

		if (exp_unsafe(e, 0))
			return 0;
		if (l) for (node *n = l->h; n && res; n = n->next)
			res &= can_push_func(n->data, rel, &lmust, depth + 1);
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
	default:
		return 1;
	}
}

static int
exps_can_push_func(list *exps, sql_rel *rel)
{
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		int mustl = 0, mustr = 0;

		if ((is_joinop(rel->op) || is_select(rel->op)) && ((can_push_func(e, rel->l, &mustl, 0) && mustl)))
			return 1;
		if (is_joinop(rel->op) && can_push_func(e, rel->r, &mustr, 0) && mustr)
			return 1;
	}
	return 0;
}

static int
exp_needs_push_down(sql_exp *e)
{
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_in || e->flag == cmp_notin || e->flag == cmp_filter)
			return 0;
		return exp_needs_push_down(e->l) || exp_needs_push_down(e->r) || (e->f && exp_needs_push_down(e->f));
	case e_convert:
		return exp_needs_push_down(e->l);
	case e_aggr:
	case e_func:
		if (!e->l || exps_are_atoms(e->l))
			return 0;
		return 1;
	case e_atom:
		if (!e->f || exps_are_atoms(e->f))
			return 0;
		return 1;
	case e_column:
	default:
		return 0;
	}
}

static int
exps_need_push_down( list *exps )
{
	for(node *n = exps->h; n; n = n->next)
		if (exp_needs_push_down(n->data))
			return 1;
	return 0;
}

static sql_exp *exp_push_single_func_down(visitor *v, sql_rel *rel, sql_rel *ol, sql_rel *or, sql_exp *e, int depth);

static list *
exps_push_single_func_down(visitor *v, sql_rel *rel, sql_rel *ol, sql_rel *or, list *exps, int depth)
{
	if (mvc_highwater(v->sql))
		return exps;

	for (node *n = exps->h; n; n = n->next)
		if ((n->data = exp_push_single_func_down(v, rel, ol, or, n->data, depth)) == NULL)
			return NULL;
	return exps;
}

static sql_exp *
exp_push_single_func_down(visitor *v, sql_rel *rel, sql_rel *ol, sql_rel *or, sql_exp *e, int depth)
{
	if (mvc_highwater(v->sql))
		return e;

	switch(e->type) {
	case e_cmp: {
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			if ((e->l = exps_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
				return NULL;
			if ((e->r = exps_push_single_func_down(v, rel, ol, or, e->r, depth + 1)) == NULL)
				return NULL;
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			if ((e->l = exp_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
				return NULL;
			if ((e->r = exps_push_single_func_down(v, rel, ol, or, e->r, depth + 1)) == NULL)
				return NULL;
		} else {
			if ((e->l = exp_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
				return NULL;
			if ((e->r = exp_push_single_func_down(v, rel, ol, or, e->r, depth + 1)) == NULL)
				return NULL;
			if (e->f && (e->f = exp_push_single_func_down(v, rel, ol, or, e->f, depth + 1)) == NULL)
				return NULL;
		}
	} break;
	case e_convert:
		if ((e->l = exp_push_single_func_down(v, rel, ol, or, e->l, depth + 1)) == NULL)
			return NULL;
		break;
	case e_aggr:
	case e_func: {
		sql_rel *l = rel->l, *r = rel->r;
		int must = 0, mustl = 0, mustr = 0;

		if (exp_unsafe(e, 0))
			return e;
		if (!e->l || exps_are_atoms(e->l))
			return e;
		if ((is_joinop(rel->op) && ((can_push_func(e, l, &mustl, depth + 1) && mustl) || (can_push_func(e, r, &mustr, depth + 1) && mustr))) ||
			(is_select(rel->op) && can_push_func(e, l, &must, depth + 1) && must)) {
			exp_label(v->sql->sa, e, ++v->sql->label);
			/* we need a full projection, group by's and unions cannot be extended with more expressions */
			if (mustr) {
				if (r == or) /* don't project twice */
					rel->r = r = rel_project(v->sql->sa, r, rel_projections(v->sql, r, NULL, 1, 1));
				list_append(r->exps, e);
			} else {
				if (l == ol) /* don't project twice */
					rel->l = l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
				list_append(l->exps, e);
			}
			e = exp_ref(v->sql, e);
			v->changes++;
		}
	} break;
	case e_atom: {
		if (e->f && (e->f = exps_push_single_func_down(v, rel, ol, or, e->f, depth + 1)) == NULL)
			return NULL;
	} break;
	case e_column:
	case e_psm:
		break;
	}
	return e;
}

static inline sql_rel *
rel_push_func_down(visitor *v, sql_rel *rel)
{
	if ((is_select(rel->op) || is_joinop(rel->op)) && rel->l && rel->exps && !(rel_is_ref(rel))) {
		int changes = v->changes;
		sql_rel *l = rel->l, *r = rel->r;

		/* only push down when is useful */
		if ((is_select(rel->op) && list_length(rel->exps) <= 1) || rel_is_ref(l) || (is_joinop(rel->op) && rel_is_ref(r)))
			return rel;
		if (exps_can_push_func(rel->exps, rel) && exps_need_push_down(rel->exps) && !exps_push_single_func_down(v, rel, l, r, rel->exps, 0))
			return NULL;
		if (v->changes > changes) /* once we get a better join order, we can try to remove this projection */
			return rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
	}
	if (is_simple_project(rel->op) && rel->l && rel->exps) {
		sql_rel *pl = rel->l;

		if (is_joinop(pl->op) && exps_can_push_func(rel->exps, rel)) {
			sql_rel *l = pl->l, *r = pl->r, *ol = l, *or = r;

			for (node *n = rel->exps->h; n; ) {
				node *next = n->next;
				sql_exp *e = n->data;
				int mustl = 0, mustr = 0;

				if ((can_push_func(e, l, &mustl, 0) && mustl) || (can_push_func(e, r, &mustr, 0) && mustr)) {
					if (mustl) {
						if (l == ol) /* don't project twice */
							pl->l = l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
						list_append(l->exps, e);
						list_remove_node(rel->exps, NULL, n);
						v->changes++;
					} else {
						if (r == or) /* don't project twice */
							pl->r = r = rel_project(v->sql->sa, r, rel_projections(v->sql, r, NULL, 1, 1));
						list_append(r->exps, e);
						list_remove_node(rel->exps, NULL, n);
						v->changes++;
					}
				}
				n = next;
			}
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
static inline sql_rel *
rel_push_count_down(visitor *v, sql_rel *rel)
{
	sql_rel *r = rel->l;

	if (is_groupby(rel->op) && !rel_is_ref(rel) && list_empty(rel->r) &&
		r && !r->exps && r->op == op_join && !(rel_is_ref(r)) &&
		/* currently only single count aggregation is handled, no other projects or aggregation */
		list_length(rel->exps) == 1 && exp_aggr_is_count(rel->exps->h->data)) {
		sql_exp *nce, *oce, *cnt1 = NULL, *cnt2 = NULL;
		sql_rel *gbl = NULL, *gbr = NULL;	/* Group By */
		sql_rel *cp = NULL;					/* Cross Product */
		sql_rel *srel;

		oce = rel->exps->h->data;
		if (oce->l) /* we only handle COUNT(*) */
			return rel;

		srel = r->l;
		{
			sql_subfunc *cf = sql_bind_func(v->sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
			sql_exp *e = exp_aggr(v->sql->sa, NULL, cf, need_distinct(oce), need_no_nil(oce), oce->card, 0);

			exp_label(v->sql->sa, e, ++v->sql->label);
			cnt1 = exp_ref(v->sql, e);
			gbl = rel_groupby(v->sql, rel_dup(srel), NULL);
			set_processed(gbl);
			rel_groupby_add_aggr(v->sql, gbl, e);
		}

		srel = r->r;
		{
			sql_subfunc *cf = sql_bind_func(v->sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
			sql_exp *e = exp_aggr(v->sql->sa, NULL, cf, need_distinct(oce), need_no_nil(oce), oce->card, 0);

			exp_label(v->sql->sa, e, ++v->sql->label);
			cnt2 = exp_ref(v->sql, e);
			gbr = rel_groupby(v->sql, rel_dup(srel), NULL);
			set_processed(gbr);
			rel_groupby_add_aggr(v->sql, gbr, e);
		}

		cp = rel_crossproduct(v->sql->sa, gbl, gbr, op_join);

		if (!(nce = rel_binop_(v->sql, NULL, cnt1, cnt2, "sys", "sql_mul", card_value))) {
			v->sql->session->status = 0;
			v->sql->errstr[0] = '\0';
			return rel; /* error, fallback to original expression */
		}
		/* because of remote plans, make sure "sql_mul" returns bigint. The cardinality is atomic, so no major performance penalty */
		if (subtype_cmp(exp_subtype(oce), exp_subtype(nce)) != 0)
			nce = exp_convert(v->sql->sa, nce, exp_subtype(nce), exp_subtype(oce));
		if (exp_name(oce))
			exp_prop_alias(v->sql->sa, nce, oce);

		rel_destroy(rel);
		rel = rel_project(v->sql->sa, cp, append(new_exp_list(v->sql->sa), nce));
		set_processed(rel);

		v->changes++;
	}

	return rel;
}

static bool
check_projection_on_foreignside(sql_rel *r, list *pexps, int fk_left)
{
	/* projection columns from the foreign side */
	if (list_empty(pexps))
		return true;
	for (node *n = pexps->h; n; n = n->next) {
		sql_exp *pe = n->data;

		if (pe && is_atom(pe->type))
			continue;
		if (pe && !is_alias(pe->type))
			return false;
		/* check for columns from the pk side, then keep the join with the pk */
		if ((fk_left && rel_find_exp(r->r, pe)) || (!fk_left && rel_find_exp(r->l, pe)))
			return false;
	}
	return true;
}

static sql_rel *
rel_simplify_project_fk_join(mvc *sql, sql_rel *r, list *pexps, list *orderexps, int *changes)
{
	sql_rel *rl = r->l, *rr = r->r, *nr = NULL;
	sql_exp *je, *le, *nje, *re;
	int fk_left = 1;

	/* check for foreign key join */
	if (list_length(r->exps) != 1)
		return r;
	if (!(je = exps_find_prop(r->exps, PROP_JOINIDX)) || je->flag != cmp_equal)
		return r;
	/* je->l == foreign expression, je->r == primary expression */
	if (rel_find_exp(r->l, je->l)) {
		fk_left = 1;
	} else if (rel_find_exp(r->r, je->l)) {
		fk_left = 0;
	} else { /* not found */
		return r;
	}

	/* primary side must be a full table */
	if ((fk_left && (!is_left(r->op) && !is_full(r->op)) && !is_basetable(rr->op)) ||
		(!fk_left && (!is_right(r->op) && !is_full(r->op)) && !is_basetable(rl->op)))
		return r;

	if (!check_projection_on_foreignside(r, pexps, fk_left) || !check_projection_on_foreignside(r, orderexps, fk_left))
		return r;

	/* rewrite, ie remove pkey side if possible */
	le = (sql_exp*)je->l, re = (sql_exp*)je->l;

	/* both have NULL and there are semantics, the join cannot be removed */
	if (is_semantics(je) && has_nil(le) && has_nil(re))
		return r;

	(*changes)++;
	/* if the foreign key column doesn't have NULL values, then return it */
	if (!has_nil(le) || is_full(r->op) || (fk_left && is_left(r->op)) || (!fk_left && is_right(r->op))) {
		if (fk_left) {
			nr = r->l;
			r->l = NULL;
		} else {
			nr = r->r;
			r->r = NULL;
		}
		rel_destroy(r);
		return nr;
	}

	/* remove NULL values, ie generate a select not null */
	nje = exp_compare(sql->sa, exp_ref(sql, le), exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(le), NULL)), cmp_equal);
	set_anti(nje);
	set_has_no_nil(nje);
	set_semantics(nje);
	if (fk_left) {
		nr = r->l;
		r->l = NULL;
	} else {
		nr = r->r;
		r->r = NULL;
	}
	rel_destroy(r);
	return rel_select(sql->sa, nr, nje);
}

static sql_rel *
rel_simplify_count_fk_join(mvc *sql, sql_rel *r, list *gexps, list *gcols, int *changes)
{
	sql_rel *rl = r->l, *rr = r->r, *nr = NULL;
	sql_exp *je, *le, *nje, *re, *oce;
	int fk_left = 1;

	/* check for foreign key join */
	if (list_length(r->exps) != 1)
		return r;
	if (!(je = exps_find_prop(r->exps, PROP_JOINIDX)) || je->flag != cmp_equal)
		return r;
	/* je->l == foreign expression, je->r == primary expression */
	if (rel_find_exp(r->l, je->l)) {
		fk_left = 1;
	} else if (rel_find_exp(r->r, je->l)) {
		fk_left = 0;
	} else { /* not found */
		return r;
	}

	oce = gexps->h->data;
	if (oce->l) /* we only handle COUNT(*) */
		return r;

	/* primary side must be a full table */
	if ((fk_left && (!is_left(r->op) && !is_full(r->op)) && !is_basetable(rr->op)) ||
		(!fk_left && (!is_right(r->op) && !is_full(r->op)) && !is_basetable(rl->op)))
		return r;

	if (fk_left && is_join(rl->op) && !rel_is_ref(rl)) {
		rl = rel_simplify_count_fk_join(sql, rl, gexps, gcols, changes);
		r->l = rl;
	}
	if (!fk_left && is_join(rr->op) && !rel_is_ref(rr)) {
		rr = rel_simplify_count_fk_join(sql, rr, gexps, gcols, changes);
		r->r = rr;
	}

	if (!check_projection_on_foreignside(r, gcols, fk_left))
		return r;

	/* rewrite, ie remove pkey side if possible */
	le = (sql_exp*)je->l, re = (sql_exp*)je->l;

	/* both have NULL and there are semantics, the join cannot be removed */
	if (is_semantics(je) && has_nil(le) && has_nil(re))
		return r;

	(*changes)++;
	/* if the foreign key column doesn't have NULL values, then return it */
	if (!has_nil(le) || is_full(r->op) || (fk_left && is_left(r->op)) || (!fk_left && is_right(r->op))) {
		if (fk_left) {
			nr = r->l;
			r->l = NULL;
		} else {
			nr = r->r;
			r->r = NULL;
		}
		rel_destroy(r);
		return nr;
	}

	/* remove NULL values, ie generate a select not null */
	nje = exp_compare(sql->sa, exp_ref(sql, le), exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(le), NULL)), cmp_equal);
	set_anti(nje);
	set_has_no_nil(nje);
	set_semantics(nje);
	if (fk_left) {
		nr = r->l;
		r->l = NULL;
	} else {
		nr = r->r;
		r->r = NULL;
	}
	rel_destroy(r);
	return rel_select(sql->sa, nr, nje);
}

/*
 * Handle (left/right/outer/natural) join fk-pk rewrites
 *   1 group by ( fk-pk-join () ) [ count(*) ] -> group by ( fk )
 *   2 project ( fk-pk-join () ) [ fk-column ] -> project (fk table)[ fk-column ]
 *   3 project ( fk1-pk1-join( fk2-pk2-join()) [ fk-column, pk1 column ] -> project (fk1-pk1-join)[ fk-column, pk1 column ]
 */
static inline sql_rel *
rel_simplify_fk_joins(visitor *v, sql_rel *rel)
{
	sql_rel *r = NULL;

	if (is_simple_project(rel->op))
		r = rel->l;

	while (is_simple_project(rel->op) && r && list_length(r->exps) == 1 && (is_join(r->op) || r->op == op_semi) && !(rel_is_ref(r))) {
		sql_rel *or = r;

		r = rel_simplify_project_fk_join(v->sql, r, rel->exps, rel->r, &v->changes);
		if (r == or)
			return rel;
		rel->l = r;
	}

	if (!is_groupby(rel->op))
		return rel;

	r = rel->l;
	while(r && is_simple_project(r->op))
		r = r->l;

	while (is_groupby(rel->op) && !rel_is_ref(rel) && r && (is_join(r->op) || r->op == op_semi) && list_length(r->exps) == 1 && !(rel_is_ref(r)) &&
		   /* currently only single count aggregation is handled, no other projects or aggregation */
		   list_length(rel->exps) == 1 && exp_aggr_is_count(rel->exps->h->data)) {
		sql_rel *or = r;

		r = rel_simplify_count_fk_join(v->sql, r, rel->exps, rel->r, &v->changes);
		if (r == or)
			return rel;
		rel->l = r;
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
sum_limit_offset(mvc *sql, sql_rel *rel)
{
	/* for sample we always propagate, or if the expression list only consists of a limit expression, we copy it */
	if (is_sample(rel->op) || list_length(rel->exps) == 1)
		return exps_copy(sql, rel->exps);
	assert(list_length(rel->exps) == 2);
	sql_subtype *lng = sql_bind_localtype("lng");
	sql_exp *add = rel_binop_(sql, NULL, exp_copy(sql, rel->exps->h->data), exp_copy(sql, rel->exps->h->next->data), "sys", "sql_add", card_value);
	/* for remote plans, make sure the output type is a bigint */
	if (subtype_cmp(lng, exp_subtype(add)) != 0)
		add = exp_convert(sql->sa, add, exp_subtype(add), lng);
	return list_append(sa_list(sql->sa), add);
}

static int
topn_sample_save_exps( list *exps )
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

		exp_setalias(e, e->l, e->r);
	}
}

static void
rel_rename_exps( mvc *sql, list *exps1, list *exps2)
{
	int pos = 0;
	node *n, *m;

	(void)sql;
	/* check if a column uses an alias earlier in the list */
	for (n = exps1->h, m = exps2->h; n && m; n = n->next, m = m->next, pos++) {
		sql_exp *e2 = m->data;

		if (e2->type == e_column) {
			sql_exp *ne = NULL;

			if (e2->l)
				ne = exps_bind_column2(exps2, e2->l, e2->r, NULL);
			if (!ne && !e2->l)
				ne = exps_bind_column(exps2, e2->r, NULL, NULL, 1);
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
		const char *rname = exp_relname(e1);

		if (!rname && e1->type == e_column && e1->l && exp_relname(e2) &&
		    strcmp(e1->l, exp_relname(e2)) == 0)
			rname = exp_relname(e2);
		exp_setalias(e2, rname, exp_name(e1));
	}
	list_hash_clear(exps2);
}

static sql_rel *
rel_push_topn_and_sample_down(visitor *v, sql_rel *rel)
{
	sql_rel *rp = NULL, *r = rel->l;

	if ((is_topn(rel->op) || is_sample(rel->op)) && topn_sample_save_exps(rel->exps)) {
		sql_rel *(*func) (sql_allocator *, sql_rel *, list *) = is_topn(rel->op) ? rel_topn : rel_sample;

		/* nested topN relations */
		if (r && is_topn(rel->op) && is_topn(r->op) && !rel_is_ref(r)) {
			sql_exp *topN1 = rel->exps->h->data, *topN2 = r->exps->h->data;
			sql_exp *offset1 = list_length(rel->exps) > 1 ? rel->exps->h->next->data : NULL;
			sql_exp *offset2 = list_length(r->exps) > 1 ? r->exps->h->next->data : NULL;

			if (topN1->l && topN2->l && (!offset1 || offset1->l) && (!offset2 || offset2->l)) { /* no parameters */
				bool changed = false;

				if ((!offset1 || (offset1->type == e_atom && offset1->l)) && (!offset2 || (offset2->type == e_atom && offset2->l))) { /* only atoms */
					if (!offset1 && offset2) {
						list_append(rel->exps, exp_copy(v->sql, offset2));
						changed = true;
					} else if (offset1 && offset2) { /* sum offsets */
						atom *b1 = (atom *)offset1->l, *b2 = (atom *)offset2->l, *c = atom_add(v->sql->sa, b1, b2);

						if (!c) /* error, don't apply optimization, WARNING because of this the offset optimization must come before the limit one */
							return rel;
						if (atom_cmp(c, b2) < 0) /* overflow */
							c = atom_int(v->sql->sa, sql_bind_localtype("lng"), GDK_lng_max);
						offset1->l = c;
						changed = true;
					}
				}

				if (topN1->type == e_atom && topN1->l && topN2->type == e_atom && topN2->l) { /* only atoms */
					atom *a1 = (atom *)topN1->l, *a2 = (atom *)topN2->l;

					if (!a2->isnull && (a1->isnull || atom_cmp(a1, a2) >= 0)) { /* topN1 is not set or is larger than topN2 */
						rel->exps->h->data = exp_copy(v->sql, topN2);
						changed = true;
					}
				}

				if (changed) {
					rel->l = r->l;
					r->l = NULL;
					rel_destroy(r);
					v->changes++;
					return rel;
				}
			}
		}

		if (r && is_simple_project(r->op) && need_distinct(r))
			return rel;

		/* push topn/sample under projections */
		if (!rel_is_ref(rel) && r && is_simple_project(r->op) && !need_distinct(r) && !rel_is_ref(r) && r->l && list_empty(r->r)) {
			sql_rel *x = r, *px = x;

			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r)) {
				px = x;
				x = x->l;
			}
			/* only push topn once */
			if (x && x->op == rel->op)
				return rel;

			rel->l = x;
			px->l = rel;
			rel = r;
			v->changes++;
			return rel;
		}

		/* duplicate topn/sample direct under union or crossproduct */
		if (r && !rel_is_ref(r) && r->l && r->r && ((is_union(r->op) && r->exps) || (r->op == op_join && list_empty(r->exps)))) {
			sql_rel *u = r, *x;
			sql_rel *ul = u->l;
			sql_rel *ur = u->r;
			bool changed = false;

			x = ul;
			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
				x = x->l;
			if (x && x->op != rel->op) { /* only push topn once */
				ul = func(v->sql->sa, ul, sum_limit_offset(v->sql, rel));
				u->l = ul;
				changed = true;
			}

			x = ur;
			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
				x = x->l;
			if (x && x->op != rel->op) { /* only push topn once */
				ur = func(v->sql->sa, ur, sum_limit_offset(v->sql, rel));
				u->r = ur;
				changed = true;
			}

			if (changed)
				v->changes++;
			return rel;
		}

		/* duplicate topn/sample + [ project-order ] under union */
		if (r)
			rp = r->l;
		if (r && r->exps && is_simple_project(r->op) && !rel_is_ref(r) && !list_empty(r->r) && r->l && is_union(rp->op)) {
			sql_rel *u = rp, *ou = u, *x, *ul = u->l, *ur = u->r;
			list *rcopy = NULL;

			/* only push topn/sample once */
			x = ul;
			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
				x = x->l;
			if (x && x->op == rel->op)
				return rel;
			x = ur;
			while (is_simple_project(x->op) && !need_distinct(x) && !rel_is_ref(x) && x->l && list_empty(x->r))
				x = x->l;
			if (x && x->op == rel->op)
				return rel;

			rcopy = exps_copy(v->sql, r->r);
			for (node *n = rcopy->h ; n ; n = n->next) {
				sql_exp *e = n->data;
				set_descending(e); /* remove ordering properties for projected columns */
				set_nulls_first(e);
			}
			ul = rel_dup(ul);
			ur = rel_dup(ur);
			if (!is_project(ul->op))
				ul = rel_project(v->sql->sa, ul,
					rel_projections(v->sql, ul, NULL, 1, 1));
			if (!is_project(ur->op))
				ur = rel_project(v->sql->sa, ur,
					rel_projections(v->sql, ur, NULL, 1, 1));
			rel_rename_exps(v->sql, u->exps, ul->exps);
			rel_rename_exps(v->sql, u->exps, ur->exps);

			/* introduce projects under the set */
			ul = rel_project(v->sql->sa, ul, NULL);
			ul->exps = exps_copy(v->sql, r->exps);
			/* possibly add order by column */
			ul->exps = list_distinct(list_merge(ul->exps, exps_copy(v->sql, rcopy), NULL), (fcmp) exp_equal, (fdup) NULL);
			ul->nrcols = list_length(ul->exps);
			ul->r = exps_copy(v->sql, r->r);
			ul = func(v->sql->sa, ul, sum_limit_offset(v->sql, rel));

			ur = rel_project(v->sql->sa, ur, NULL);
			ur->exps = exps_copy(v->sql, r->exps);
			/* possibly add order by column */
			ur->exps = list_distinct(list_merge(ur->exps, exps_copy(v->sql, rcopy), NULL), (fcmp) exp_equal, (fdup) NULL);
			ur->nrcols = list_length(ur->exps);
			ur->r = exps_copy(v->sql, r->r);
			ur = func(v->sql->sa, ur, sum_limit_offset(v->sql, rel));

			u = rel_setop(v->sql->sa, ul, ur, op_union);
			u->exps = exps_alias(v->sql, r->exps);
			u->nrcols = list_length(u->exps);
			set_processed(u);
			/* possibly add order by column */
			u->exps = list_distinct(list_merge(u->exps, rcopy, NULL), (fcmp) exp_equal, (fdup) NULL);
			if (need_distinct(r)) {
				set_distinct(ul);
				set_distinct(ur);
			}

			/* zap names */
			rel_no_rename_exps(u->exps);
			rel_destroy(ou);

			ur = rel_project(v->sql->sa, u, exps_alias(v->sql, r->exps));
			ur->r = r->r;
			r->l = NULL;

			if (need_distinct(r))
				set_distinct(ur);

			rel_destroy(r);
			rel->l = ur;
			v->changes++;
			return rel;
		}
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
		narg = exp_propagate(sql->sa, narg, arg);
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
			ne = exps_bind_column2(f->exps, e->l, e->r, NULL);
		if (!ne && !e->l)
			ne = exps_bind_column(f->exps, e->r, NULL, NULL, 1);
		if (!ne || (ne->type != e_column && (ne->type != e_atom || ne->f)))
			return NULL;
		while (ne && has_label(ne) && f->op == op_project && ne->type == e_column) {
			sql_exp *oe = e, *one = ne;

			e = ne;
			ne = NULL;
			if (e->l)
				ne = exps_bind_column2(f->exps, e->l, e->r, NULL);
			if (!ne && !e->l)
				ne = exps_bind_column(f->exps, e->r, NULL, NULL, 1);
			if (ne && ne != one && list_position(f->exps, ne) >= list_position(f->exps, one))
				ne = NULL;
			if (!ne || ne == one) {
				ne = one;
				e = oe;
				break;
			}
			if (ne->type != e_column && (ne->type != e_atom || ne->f))
				return NULL;
		}
		/* possibly a groupby/project column is renamed */
		if (is_groupby(f->op) && !list_empty(f->r)) {
			sql_exp *gbe = NULL;
			if (ne->l)
				gbe = exps_bind_column2(f->r, ne->l, ne->r, NULL);
			if (!gbe && !e->l)
				gbe = exps_bind_column(f->r, ne->r, NULL, NULL, 1);
			ne = gbe;
			if (!ne || (ne->type != e_column && (ne->type != e_atom || ne->f)))
				return NULL;
		}
		if (ne->type == e_atom)
			e = exp_copy(sql, ne);
		else
			e = exp_alias(sql->sa, exp_relname(e), exp_name(e), ne->l, ne->r, exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
		return exp_propagate(sql->sa, e, ne);
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			list *l = exps_push_down_prj(sql, e->l, f, t);
			list *r = exps_push_down_prj(sql, e->r, f, t);

			if (!l || !r)
				return NULL;
			if (e->flag == cmp_filter)
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
					ne = exp_compare2(sql->sa, l, r, r2, e->flag, is_symmetric(e));
			} else if (l && r) {
				ne = exp_compare(sql->sa, l, r, e->flag);
			}
		}
		if (!ne)
			return NULL;
		return exp_propagate(sql->sa, ne, e);
	case e_convert:
		l = exp_push_down_prj(sql, e->l, f, t);
		if (l)
			return exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		return NULL;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;
		sql_exp *ne = NULL;

		if (e->type == e_func && exp_unsafe(e,0))
			return NULL;
		if (!list_empty(l)) {
			nl = exps_push_down_prj(sql, l, f, t);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			ne = exp_op(sql->sa, nl, e->f);
		else
			ne = exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
		return exp_propagate(sql->sa, ne, e);
	}
	case e_atom: {
		list *l = e->f, *nl = NULL;

		if (!list_empty(l)) {
			nl = exps_push_down_prj(sql, l, f, t);
			if (!nl)
				return NULL;
			ne = exp_values(sql->sa, nl);
		} else {
			ne = exp_copy(sql, e);
		}
		return exp_propagate(sql->sa, ne, e);
	}
	case e_psm:
		if (e->type == e_atom && e->f) /* value list */
			return NULL;
		return e;
	}
	return NULL;
}

static int
rel_is_unique(sql_rel *rel)
{
	switch(rel->op) {
	case op_semi:
	case op_anti:
	case op_inter:
	case op_except:
	case op_topn:
	case op_sample:
		return rel_is_unique(rel->l);
	case op_table:
	case op_basetable:
		return 1;
	default:
		return 0;
	}
}

/* WARNING exps_unique doesn't check for duplicate NULL values */
int
exps_unique(mvc *sql, sql_rel *rel, list *exps)
{
	int nr = 0, need_check = 0;
	sql_ukey *k = NULL;

	if (list_empty(exps))
		return 0;
	for(node *n = exps->h; n ; n = n->next) {
		sql_exp *e = n->data;
		prop *p;

		if (!is_unique(e)) { /* ignore unique columns */
			need_check++;
			if (!k && (p = find_prop(e->p, PROP_HASHCOL))) /* at the moment, use only one k */
				k = p->value;
		}
	}
	if (!need_check) /* all have unique property return */
		return 1;
	if (!k || list_length(k->k.columns) != need_check)
		return 0;
	if (rel) {
		char *matched = SA_ZNEW_ARRAY(sql->sa, char, list_length(k->k.columns));
		fcmp cmp = (fcmp)&kc_column_cmp;
		for(node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_column *c;
			node *m;

			if (is_unique(e))
				continue;
			if ((c = exp_find_column(rel, e, -2)) != NULL && (m = list_find(k->k.columns, c, cmp)) != NULL) {
				int pos = list_position(k->k.columns, m->data);
				if (!matched[pos])
					nr++;
				matched[pos] = 1;
			}
		}
		if (nr == list_length(k->k.columns))
			return rel_is_unique(rel);
	}
	return 0;
}

static sql_column *
exp_is_pkey(sql_rel *rel, sql_exp *e)
{
	if (find_prop(e->p, PROP_HASHCOL)) { /* aligned PKEY JOIN */
		fcmp cmp = (fcmp)&kc_column_cmp;
		sql_column *c = exp_find_column(rel, e, -2);

		if (c && c->t->pkey && list_find(c->t->pkey->k.columns, c, cmp) != NULL)
			return c;
	}
	return NULL;
}

static sql_exp *
rel_is_join_on_pkey(sql_rel *rel, bool pk_fk) /* pk_fk is used to verify is a join on pk-fk */
{
	if (!rel || !rel->exps)
		return NULL;
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *je = n->data;

		if (je->type == e_cmp && je->flag == cmp_equal &&
			(exp_is_pkey(rel, je->l) || exp_is_pkey(rel, je->r)) &&
			(!pk_fk || find_prop(je->p, PROP_JOINIDX)))
			return je;
	}
	return NULL;
}

/* if all arguments to a distinct aggregate are unique, remove 'distinct' property */
static inline sql_rel *
rel_distinct_aggregate_on_unique_values(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !list_empty(rel->exps)) {
		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *exp = (sql_exp*) n->data;

			if (exp->type == e_aggr && need_distinct(exp)) {
				bool all_unique = true;
				list *l = exp->l;

				for (node *m = l->h; m && all_unique; m = m->next) {
					sql_exp *arg = (sql_exp*) m->data;

					all_unique &= arg->type == e_column && is_unique(arg) && (!is_semantics(exp) || !has_nil(arg));
				}
				if (!all_unique && exps_card(l) > CARD_ATOM)
					all_unique = exps_unique(v->sql, rel, l) && (!is_semantics(exp) || !have_nil(l));
				if (all_unique) {
					set_nodistinct(exp);
					v->changes++;
				}
			}
		}
	}
	return rel;
}

static bool
has_no_selectivity(mvc *sql, sql_rel *rel)
{
	if (!rel)
		return true;

	switch(rel->op){
	case op_basetable:
	case op_truncate:
	case op_table:
		return true;
	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:
		return has_no_selectivity(sql, rel->l);
	case op_ddl:
	case op_insert:
	case op_update:
	case op_delete:
	case op_merge:
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_union:
	case op_inter:
	case op_except:
	case op_select:
		return false;
	}
	return rel;
}

/*
 * Remove a redundant join
 *
 * join (L, Distinct Project(join(L,P) [ p.key == l.lkey]) [p.key]) [ p.key == l.lkey]
 * =>
 * join(L, P) [p.key==l.lkey]
 */
static sql_rel *
rel_remove_redundant_join(visitor *v, sql_rel *rel)
{
	if ((is_join(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		sql_rel *l = rel->l, *r = rel->r, *b, *p = NULL, *j;

		if (is_basetable(l->op) && is_simple_project(r->op) && need_distinct(r)) {
			b = l;
			p = r;
			j = p->l;
		} else if (is_basetable(r->op) && is_simple_project(l->op) && need_distinct(l)) {
			b = r;
			p = l;
			j = p->l;
		}
		if (!p || !j || j->op != rel->op)
			return rel;
		/* j must have b->l (ie table) */
		sql_rel *jl = j->l, *jr = j->r;
		if ((is_basetable(jl->op) && jl->l == b->l) ||
		    (is_basetable(jr->op) && jr->l == b->l)) {
			int left = 0;
			if (is_basetable(jl->op) && jl->l == b->l)
				left = 1;
			if (!list_empty(p->exps)) {
				for (node *n=p->exps->h; n; n = n->next) { /* all exps of 'p' must be bound to the opposite side */
					sql_exp *e = n->data;

					if (!rel_rebind_exp(v->sql, left ? jr : jl, e))
						return rel;
				}
			}
			if (exp_match_list(j->exps, rel->exps)) {
				p->l = (left)?rel_dup(jr):rel_dup(jl);
				rel_destroy(j);
				set_nodistinct(p);
				v->changes++;
				return rel;
			}
		}
	}
	return rel;
}

static sql_column *
is_fk_column_of_pk(mvc *sql, sql_rel *rel, sql_column *pkc, sql_exp *e) /* test if e is a foreing key column for the pk on pkc */
{
	sql_trans *tr = sql->session->tr;
	sql_column *c = exp_find_column(rel, e, -2);

	if (c) {
		sql_table *t = c->t;

		for (node *n = ol_first_node(t->idxs); n; n = n->next) {
			sql_idx *li = n->data;

			if (li->type == join_idx) {
				for (node *m = li->columns->h ; m ; m = m->next) {
					sql_kc *fkc = m->data;

					if (strcmp(fkc->c->base.name, c->base.name) == 0) { /* same fkey column */
						sql_key *fkey = (sql_key*)os_find_id(tr->cat->objects, tr, ((sql_fkey*)li->key)->rkey);

						if (strcmp(fkey->t->base.name, pkc->t->base.name) == 0) { /* to same pk table */
							for (node *o = fkey->columns->h ; o ; o = n->next) {
								sql_kc *kc = m->data;

								if (strcmp(kc->c->base.name, pkc->base.name) == 0) /* to same pk table column */
									return c;
							}
						}
					}
				}
			}
		}
	}
	return NULL;
}

static sql_rel *
rel_distinct_project2groupby(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l;

	/* rewrite distinct project (table) [ constant ] -> project [ constant ] */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    exps_card(rel->exps) <= CARD_ATOM) {
		set_nodistinct(rel);
		if (rel->card > CARD_ATOM) /* if the projection just contains constants, then no topN is needed */
			rel->l = rel_topn(v->sql->sa, rel->l, append(sa_list(v->sql->sa), exp_atom_lng(v->sql->sa, 1)));
		v->changes++;
	}

	/* rewrite distinct project [ pk ] ( select ( table ) [ e op val ])
	 * into project [ pk ] ( select/semijoin ( table )  */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    (l->op == op_select || l->op == op_semi) && exps_unique(v->sql, rel, rel->exps) &&
		(!have_semantics(l->exps) || !have_nil(rel->exps))) {
		set_nodistinct(rel);
		v->changes++;
	}

	/* rewrite distinct project ( join(p,f) [ p.pk = f.fk ] ) [ p.pk ]
	 * 	into project( (semi)join(p,f) [ p.pk = f.fk ] ) [ p.pk ] */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ && need_distinct(rel) &&
	    l && (is_select(l->op) || l->op == op_join) && rel_is_join_on_pkey(l, true) /* [ pk == fk ] */) {
		sql_exp *found = NULL, *pk = NULL, *fk = NULL;
		bool all_exps_atoms = true;
		sql_column *pkc = NULL;

		for (node *m = l->exps->h ; m ; m = m->next) { /* find a primary key join */
			sql_exp *je = (sql_exp *) m->data;
			sql_exp *le = je->l, *re = je->r;

			if (!find_prop(je->p, PROP_JOINIDX)) /* must be a pk-fk join expression */
				continue;

			if ((pkc = exp_is_pkey(l, le))) { /* le is the primary key */
				all_exps_atoms = true;

				for (node *n = rel->exps->h; n && all_exps_atoms; n = n->next) {
					sql_exp *e = (sql_exp *) n->data;

					if (exp_match(e, le) || exp_refers(e, le))
						found = e;
					else if (e->card > CARD_ATOM)
						all_exps_atoms = false;
				}
				pk = le;
				fk = re;
			}
			if (!found && (pkc = exp_is_pkey(l, re))) { /* re is the primary key */
				all_exps_atoms = true;

				for (node *n = rel->exps->h; n && all_exps_atoms; n = n->next) {
					sql_exp *e = (sql_exp *) n->data;

					if (exp_match(e, re) || exp_refers(e, re))
						found = e;
					else if (e->card > CARD_ATOM)
						all_exps_atoms = false;
				}
				pk = re;
				fk = le;
			}
		}

		if (all_exps_atoms && found) { /* rel must have the same primary key on the projection list */
			/* if the foreign key has no selectivity, the join can be removed */
			if (!(rel_is_ref(l)) && ((rel_find_exp(l->l, fk) && is_fk_column_of_pk(v->sql, l->l, pkc, fk) && has_no_selectivity(v->sql, l->l)) ||
				(l->r && rel_find_exp(l->r, fk) && is_fk_column_of_pk(v->sql, l->r, pkc, fk) && has_no_selectivity(v->sql, l->r)))) {
				sql_rel *side = (rel_find_exp(l->l, pk) != NULL)?l->l:l->r;

				rel->l = rel_dup(side);
				rel_destroy(l);
				v->changes++;
				set_nodistinct(rel);
				return rel;
			}
			/* if the join has no multiple references it can be re-written into a semijoin */
			if (l->op == op_join && !(rel_is_ref(l)) && list_length(rel->exps) == 1) { /* other expressions may come from the other side */
				if (l->r && rel_find_exp(l->r, pk)) {
					sql_rel *temp = l->l;
					l->l = l->r;
					l->r = temp;

					l->op = op_semi;
				} else if (rel_find_exp(l->l, pk)) {
					l->op = op_semi;
				}
			}
			v->changes++;
			set_nodistinct(rel);
			return rel;
		}
	}
	/* rewrite distinct project [ gbe ] ( select ( groupby [ gbe ] [ gbe, e ] )[ e op val ])
	 * into project [ gbe ] ( select ( group etc ) */
	if (rel->op == op_project && rel->l && !rel->r /* no order by */ &&
	    need_distinct(rel) && l->op == op_select){
		sql_rel *g = l->l;
		if (is_groupby(g->op)) {
			list *used = sa_list(v->sql->sa);
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
					if (ne && !list_find_exp(used, ne)) {
						fnd++;
						list_append(used, ne);
					}
					if (!ne)
						fnd = 0;
				}
			}
			if (fnd == (list_length(gbe)+1)) {
				v->changes++;
				set_nodistinct(rel);
			}
		}
	}
	if (rel->op == op_project && rel->l &&
	    need_distinct(rel) && exps_card(rel->exps) > CARD_ATOM) {
		node *n;
		list *exps = new_exp_list(v->sql->sa), *gbe = new_exp_list(v->sql->sa);
		list *obe = rel->r; /* we need to read the ordering later */

		if (obe) {
			int fnd = 0;

			for(n = obe->h; n && !fnd; n = n->next) {
				sql_exp *e = n->data;

				if (e->type != e_column)
					fnd = 1;
				else if (exps_bind_column2(rel->exps, e->l, e->r, NULL) == 0)
					fnd = 1;
			}
			if (fnd)
				return rel;
		}
		rel->l = rel_project(v->sql->sa, rel->l, rel->exps);

		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne;

			ne = exp_ref(v->sql, e);
			if (e->card > CARD_ATOM && !list_find_exp(gbe, ne)) /* no need to group by on constants, or the same column multiple times */
				append(gbe, ne);
			append(exps, ne);
		}
		rel->op = op_groupby;
		rel->exps = exps;
		rel->r = gbe;
		set_nodistinct(rel);
		if (obe) {
			/* add order again */
			rel = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
			rel->r = obe;
		}
		v->changes++;
	}
	return rel;
}

static bool exp_shares_exps(sql_exp *e, list *shared, uint64_t *uses);

static bool
exps_shares_exps(list *exps, list *shared, uint64_t *uses)
{
	if (!exps || !shared)
		return false;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (exp_shares_exps(e, shared, uses))
			return true;
	}
	return false;
}

static bool
exp_shares_exps(sql_exp *e, list *shared, uint64_t *uses)
{
	switch(e->type) {
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter)
			return exps_shares_exps(e->l, shared, uses) || exps_shares_exps(e->r, shared, uses);
		else if (e->flag == cmp_in || e->flag == cmp_notin)
			return exp_shares_exps(e->l, shared, uses) || exps_shares_exps(e->r, shared, uses);
		else
			return exp_shares_exps(e->l, shared, uses) || exp_shares_exps(e->r, shared, uses) || (e->f && exp_shares_exps(e->f, shared, uses));
	case e_atom:
		if (e->f)
			return exps_shares_exps(e->f, shared, uses);
		return false;
	case e_column:
		{
			sql_exp *ne = NULL;
			if (e->l)
				ne = exps_bind_column2(shared, e->l, e->r, NULL);
			if (!ne && !e->l)
				ne = exps_bind_column(shared, e->r, NULL, NULL, 1);
			if (!ne)
				return false;
			if (ne->type != e_column) {
				int i = list_position(shared, ne);
				if (i < 0)
					return false;
				uint64_t used = (uint64_t) 1 << i;
				if (used & *uses)
					return true;
				*uses |= used;
				return false;
			}
			if (ne != e && (list_position(shared, e) < 0 || list_position(shared, e) > list_position(shared, ne)))
				/* maybe ne refers to a local complex exp */
				return exp_shares_exps(ne, shared, uses);
			return false;
		}
	case e_convert:
		return exp_shares_exps(e->l, shared, uses);
	case e_aggr:
	case e_func:
		return exps_shares_exps(e->l, shared, uses);
	case e_psm:
		assert(0);  /* not in projection list */
	}
	return false;
}

static bool
exps_share_expensive_exp(list *exps, list *shared )
{
	uint64_t uses = 0;

	if (!exps || !shared)
		return false;
	if (list_length(shared) > 64)
		return true;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (exp_shares_exps(e, shared, &uses))
			return true;
	}
	return false;
}

static bool ambigious_ref( list *exps, sql_exp *e);
static bool
ambigious_refs( list *exps, list *refs)
{
	node *n;

	if (!refs)
		return false;
	for(n=refs->h; n; n = n->next) {
		if (ambigious_ref(exps, n->data))
			return true;
	}
	return false;
}

static bool
ambigious_ref( list *exps, sql_exp *e)
{
	sql_exp *ne = NULL;

	if (e->type == e_column) {
		if (e->l)
			ne = exps_bind_column2(exps, e->l, e->r, NULL);
		if (!ne && !e->l)
			ne = exps_bind_column(exps, e->r, NULL, NULL, 1);
		if (ne && e != ne)
			return true;
	}
	if (e->type == e_func)
		return ambigious_refs(exps, e->l);
	return false;
}

/* merge 2 projects into the lower one */
static sql_rel *
rel_merge_projects(visitor *v, sql_rel *rel)
{
	list *exps = rel->exps;
	sql_rel *prj = rel->l;
	node *n;

	if (rel->op == op_project &&
	    prj && prj->op == op_project && !(rel_is_ref(prj)) && list_empty(prj->r)) {
		int all = 1;

		if (project_unsafe(rel,0) || project_unsafe(prj,0) || exps_share_expensive_exp(rel->exps, prj->exps))
			return rel;

		/* here we need to fix aliases */
		rel->exps = new_exp_list(v->sql->sa);

		/* for each exp check if we can rename it */
		for (n = exps->h; n && all; n = n->next) {
			sql_exp *e = n->data, *ne = NULL;

			/* We do not handle expressions pointing back in the list */
			if (ambigious_ref(exps, e)) {
				all = 0;
				break;
			}
			ne = exp_push_down_prj(v->sql, e, prj, prj->l);
			/* check if the refered alias name isn't used twice */
			if (ne && ambigious_ref(rel->exps, ne)) {
				all = 0;
				break;
			}
			if (ne) {
				if (exp_name(e))
					exp_prop_alias(v->sql->sa, ne, e);
				list_append(rel->exps, ne);
			} else {
				all = 0;
			}
		}
		if (all) {
			/* we can now remove the intermediate project */
			/* push order by expressions */
			if (!list_empty(rel->r)) {
				list *nr = new_exp_list(v->sql->sa), *res = rel->r;
				for (n = res->h; n; n = n->next) {
					sql_exp *e = n->data, *ne = NULL;

					ne = exp_push_down_prj(v->sql, e, prj, prj->l);
					if (ne) {
						if (exp_name(e))
							exp_prop_alias(v->sql->sa, ne, e);
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
			v->changes++;
			return rel_merge_projects(v, rel);
		} else {
			/* leave as is */
			rel->exps = exps;
		}
		return rel;
	}
	return rel;
}

static inline int
str_ends_with(const char *s, const char *suffix)
{
	size_t slen = strlen(s), suflen = strlen(suffix);
	if (suflen > slen)
		return 1;
	return strncmp(s + slen - suflen, suffix, suflen);
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

		/* if the function has no null semantics we can return NULL if one of the arguments is NULL */
		if (!f->func->semantics && f->func->type != F_PROC) {
			for (node *n = l->h ; n ; n = n->next) {
				sql_exp *arg = n->data;

				if (exp_is_atom(arg) && exp_is_null(arg)) {
					sql_exp *ne = exp_null(sql->sa, exp_subtype(e));
					(*changes)++;
					if (exp_name(e))
						exp_prop_alias(sql->sa, ne, e);
					return ne;
				}
			}
		}
		if (!f->func->s && list_length(l) == 2 && str_ends_with(sql_func_imp(f->func), "_no_nil") == 0) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;

			/* if "_no_nil" is in the name of the
			 * implementation function (currently either
			 * min_no_nil or max_no_nil), in which case we
			 * ignore the NULL and return the other value */

			if (exp_is_atom(le) && exp_is_null(le)) {
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, re, e);
				return re;
			}
			if (exp_is_atom(re) && exp_is_null(re)) {
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
		}

		le = l->h->data;
		if (!EC_COMPUTE(exp_subtype(le)->type->eclass) && exp_subtype(le)->type->eclass != EC_DEC)
			return e;

		if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;
			sql_subtype *et = exp_subtype(e);

			/* 0*a = 0 */
			if (exp_is_atom(le) && exp_is_zero(le) && exp_is_atom(re) && exp_is_not_null(re)) {
				(*changes)++;
				le = exp_zero(sql->sa, et);
				if (subtype_cmp(exp_subtype(e), exp_subtype(le)) != 0)
					le = exp_convert(sql->sa, le, exp_subtype(le), exp_subtype(e));
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
			/* a*0 = 0 */
			if (exp_is_atom(re) && exp_is_zero(re) && exp_is_atom(le) && exp_is_not_null(le)) {
				(*changes)++;
				re = exp_zero(sql->sa, et);
				if (subtype_cmp(exp_subtype(e), exp_subtype(re)) != 0)
					re = exp_convert(sql->sa, re, exp_subtype(re), exp_subtype(e));
				if (exp_name(e))
					exp_prop_alias(sql->sa, re, e);
				return re;
			}
			/* 1*a = a
			if (exp_is_atom(le) && exp_is_one(le)) {
				(*changes)++;
				if (subtype_cmp(exp_subtype(e), exp_subtype(re)) != 0)
					re = exp_convert(sql->sa, re, exp_subtype(re), exp_subtype(e));
				if (exp_name(e))
					exp_prop_alias(sql->sa, re, e);
				return re;
			}
			*/
			/* a*1 = a
			if (exp_is_atom(re) && exp_is_one(re)) {
				(*changes)++;
				if (subtype_cmp(exp_subtype(e), exp_subtype(le)) != 0)
					le = exp_convert(sql->sa, le, exp_subtype(le), exp_subtype(e));
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
			*/
			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, true, le);
				atom *ra = exp_flatten(sql, true, re);

				if (la && ra && subtype_cmp(atom_type(la), atom_type(ra)) == 0 && subtype_cmp(atom_type(la), exp_subtype(e)) == 0) {
					atom *a = atom_mul(sql->sa, la, ra);

					if (a && (a = atom_cast(sql->sa, a, exp_subtype(e)))) {
						sql_exp *ne = exp_atom(sql->sa, a);
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
			/* change a*a into pow(a,2), later change pow(a,2) back into a*a */
			if (/* DISABLES CODE */ (0) && exp_equal(le, re)==0 && exp_subtype(le)->type->eclass == EC_FLT) {
				/* pow */
				list *l;
				sql_exp *ne;
				sql_subfunc *pow = sql_bind_func(sql, "sys", "power", exp_subtype(le), exp_subtype(re), F_FUNC);
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
				if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
					ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
				if (exp_name(e))
					exp_prop_alias(sql->sa, ne, e);
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
						atom *a = exp_value(sql, lre);
						if (a && (a = atom_inc(sql->sa, a))) {
							lre->l = a;
							lre->r = NULL;
							if (subtype_cmp(exp_subtype(e), exp_subtype(le)) != 0)
								le = exp_convert(sql->sa, le, exp_subtype(le), exp_subtype(e));
							(*changes)++;
							if (exp_name(e))
								exp_prop_alias(sql->sa, le, e);
							return le;
						}
					}
				}
				if (!f->func->s && !strcmp(f->func->base.name, "sql_mul") && list_length(l) == 2) {
					sql_exp *lle = l->h->data;
					sql_exp *lre = l->h->next->data;
					if (!exp_is_atom(lle) && exp_is_atom(lre) && exp_is_atom(re)) {
						/* (x*c1)*c2 -> x * (c1*c2) */
						sql_exp *ne = NULL;

						if (!(le = rel_binop_(sql, NULL, lre, re, "sys", "sql_mul", card_value))) {
							sql->session->status = 0;
							sql->errstr[0] = '\0';
							return e; /* error, fallback to original expression */
						}
						if (!(ne = rel_binop_(sql, NULL, lle, le, "sys", "sql_mul", card_value))) {
							sql->session->status = 0;
							sql->errstr[0] = '\0';
							return e; /* error, fallback to original expression */
						}
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
		}
		if (!f->func->s && !strcmp(f->func->base.name, "sql_add") && list_length(l) == 2) {
			sql_exp *le = l->h->data;
			sql_exp *re = l->h->next->data;
			if (exp_is_atom(le) && exp_is_zero(le)) {
				if (subtype_cmp(exp_subtype(e), exp_subtype(re)) != 0)
					re = exp_convert(sql->sa, re, exp_subtype(re), exp_subtype(e));
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, re, e);
				return re;
			}
			if (exp_is_atom(re) && exp_is_zero(re)) {
				if (subtype_cmp(exp_subtype(e), exp_subtype(le)) != 0)
					le = exp_convert(sql->sa, le, exp_subtype(le), exp_subtype(e));
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, le, e);
				return le;
			}
			if (exp_is_atom(le) && exp_is_atom(re)) {
				atom *la = exp_flatten(sql, true, le);
				atom *ra = exp_flatten(sql, true, re);

				if (la && ra) {
					atom *a = atom_add(sql->sa, la, ra);

					if (a) {
						sql_exp *ne = exp_atom(sql->sa, a);
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
			if (is_func(le->type)) {
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
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
						(*changes)++;
						return e;
					}
					if (exp_is_atom(re) && exp_is_atom(lre)) {
						/* (x+c1)+c2 -> (c2+c1) + x */
						ll->h->data = re;
						l->h->next->data = lle;
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
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
				atom *la = exp_flatten(sql, true, le);
				atom *ra = exp_flatten(sql, true, re);

				if (la && ra) {
					atom *a = atom_sub(sql->sa, la, ra);

					if (a) {
						sql_exp *ne = exp_atom(sql->sa, a);
						if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
							ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
						(*changes)++;
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						return ne;
					}
				}
			}
			if (exp_is_not_null(le) && exp_is_not_null(re) && exp_equal(le,re) == 0) { /* a - a = 0 */
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
				if (subtype_cmp(exp_subtype(e), exp_subtype(ne)) != 0)
					ne = exp_convert(sql->sa, ne, exp_subtype(ne), exp_subtype(e));
				(*changes)++;
				if (exp_name(e))
					exp_prop_alias(sql->sa, ne, e);
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
						if (subtype_cmp(exp_subtype(e), exp_subtype(lle)) != 0)
							lle = exp_convert(sql->sa, lle, exp_subtype(lle), exp_subtype(e));
						if (exp_name(e))
							exp_prop_alias(sql->sa, lle, e);
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
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
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
						if (!(l->h->data = exp_simplify_math(sql, le, changes)))
							return NULL;
						(*changes)++;
						return e;
					}
				}
			}
		}
		if (l)
			for (n = l->h; n; n = n->next)
				if (!(n->data = exp_simplify_math(sql, n->data, changes)))
					return NULL;
	}
	if (e->type == e_convert)
		if (!(e->l = exp_simplify_math(sql, e->l, changes)))
			return NULL;
	return e;
}

static inline sql_rel *
rel_simplify_math(visitor *v, sql_rel *rel)
{
	if ((is_simple_project(rel->op) || is_groupby(rel->op) || (rel->op == op_ddl && rel->flag == ddl_psm)) && rel->exps) {
		int needed = 0, ochanges = 0;

		for (node *n = rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_func || e->type == e_convert || e->type == e_aggr || e->type == e_psm)
				needed = 1;
		}
		if (!needed)
			return rel;

		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *ne = exp_simplify_math(v->sql, n->data, &ochanges);

			if (!ne)
				return NULL;
			n->data = ne;
		}
		v->changes += ochanges;
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

static inline sql_rel *
rel_merge_union(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l;
	sql_rel *r = rel->r;
	sql_rel *ref = NULL;

	if (is_union(rel->op) &&
	    l && is_project(l->op) && !project_unsafe(l,0) &&
	    r && is_project(r->op) && !project_unsafe(r,0) &&
	    (ref = rel_find_ref(l)) != NULL && ref == rel_find_ref(r)) {
		/* Find selects and try to merge */
		sql_rel *ls = rel_find_select(l);
		sql_rel *rs = rel_find_select(r);

		/* can we merge ? */
		if (!ls || !rs)
			return rel;

		/* merge any extra projects */
		if (l->l != ls)
			rel->l = l = rel_merge_projects(v, l);
		if (r->l != rs)
			rel->r = r = rel_merge_projects(v, r);

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
		v->changes++;
		ls->exps = append(new_exp_list(v->sql->sa), exp_or(v->sql->sa, ls->exps, rs->exps, 0));
		rs->exps = NULL;
		rel = rel_inplace_project(v->sql->sa, rel, rel_dup(rel->l), rel->exps);
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

	if (list_length(l) == 0 || list_length(r) == 0)
		return 0;

	/* first recusive exps_cse */
	nexps = new_exp_list(sql->sa);
	for (n = l->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e)) {
			res = exps_cse(sql, nexps, e->l, e->r);
		} else {
			append(nexps, e);
		}
	}
	l = nexps;

	nexps = new_exp_list(sql->sa);
	for (n = r->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e)) {
			res = exps_cse(sql, nexps, e->l, e->r);
		} else {
			append(nexps, e);
		}
	}
	r = nexps;

	/* simplify  true or .. and .. or true */
	if (list_length(l) == list_length(r) && list_length(l) == 1) {
		sql_exp *le = l->h->data, *re = r->h->data;

		if (exp_is_true(le)) {
			append(oexps, le);
			return 1;
		}
		if (exp_is_true(re)) {
			append(oexps, re);
			return 1;
		}
	}

	lu = SA_ZNEW_ARRAY(sql->ta, char, list_length(l));
	ru = SA_ZNEW_ARRAY(sql->ta, char, list_length(r));
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
	return res;
}

static int
are_equality_exps( list *exps, sql_exp **L)
{
	sql_exp *l = *L;

	if (list_length(exps) == 1) {
		sql_exp *e = exps->h->data, *le = e->l, *re = e->r;

		if (e->type == e_cmp && e->flag == cmp_equal && le->card != CARD_ATOM && re->card == CARD_ATOM && !is_semantics(e)) {
			if (!l) {
				*L = l = le;
				if (!is_column(le->type))
					return 0;
			}
			return (exp_match(l, le));
		}
		if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e) && !is_semantics(e))
			return (are_equality_exps(e->l, L) && are_equality_exps(e->r, L));
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

static list *
merge_ors(mvc *sql, list *exps, int *changes)
{
	list *nexps = NULL;
	int needed = 0;

	for (node *n = exps->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e))
			needed = 1;
	}

	if (needed) {
		nexps = new_exp_list(sql->sa);
		for (node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data, *l = NULL;

			if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e) && are_equality_exps(e->l, &l) && are_equality_exps(e->r, &l) && l) {
				(*changes)++;
				append(nexps, equality_exps_2_in(sql, l, e->l, e->r));
			} else {
				append(nexps, e);
			}
		}
	} else {
		nexps = exps;
	}

	for (node *n = nexps->h; n ; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or) {
			e->l = merge_ors(sql, e->l, changes);
			e->r = merge_ors(sql, e->r, changes);
		}
	}

	return nexps;
}

#define TRIVIAL_NOT_EQUAL_CMP(e) \
	((e)->type == e_cmp && (e)->flag == cmp_notequal && !is_anti((e)) && !is_semantics((e)) && ((sql_exp*)(e)->l)->card != CARD_ATOM && ((sql_exp*)(e)->r)->card == CARD_ATOM)

static list *
merge_notequal(mvc *sql, list *exps, int *changes)
{
	list *inequality_groups = NULL, *nexps = NULL;
	int needed = 0;

	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (TRIVIAL_NOT_EQUAL_CMP(e)) {
			bool appended = false;

			if (inequality_groups) {
				for (node *m = inequality_groups->h; m && !appended; m = m->next) {
					list *next = m->data;
					sql_exp *first = (sql_exp*) next->h->data;

					if (exp_match(first->l, e->l)) {
						list_append(next, e);
						appended = true;
					}
				}
			}
			if (!appended) {
				if (!inequality_groups)
					inequality_groups = new_exp_list(sql->sa);
				list_append(inequality_groups, list_append(new_exp_list(sql->sa), e));
			}
		}
	}

	if (inequality_groups) { /* if one list of inequalities has more than one entry, then the re-write is needed */
		for (node *n = inequality_groups->h; n; n = n->next) {
			list *next = n->data;

			if (list_length(next) > 1)
				needed = 1;
		}
	}

	if (needed) {
		nexps = new_exp_list(sql->sa);
		for (node *n = inequality_groups->h; n; n = n->next) {
			list *next = n->data;
			sql_exp *first = (sql_exp*) next->h->data;

			if (list_length(next) > 1) {
				list *notin = new_exp_list(sql->sa);

				for (node *m = next->h; m; m = m->next) {
					sql_exp *e = m->data;
					list_append(notin, e->r);
				}
				list_append(nexps, exp_in(sql->sa, first->l, notin, cmp_notin));
			} else {
				list_append(nexps, first);
			}
		}

		for (node *n = exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (!TRIVIAL_NOT_EQUAL_CMP(e))
				list_append(nexps, e);
		}
		(*changes)++;
	} else {
		nexps = exps;
	}

	for (node *n = nexps->h; n ; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or) {
			e->l = merge_notequal(sql, e->l, changes);
			e->r = merge_notequal(sql, e->r, changes);
		}
	}

	return nexps;
}

static int
is_numeric_upcast(sql_exp *e)
{
	if (is_convert(e->type)) {
		sql_subtype *f = exp_fromtype(e);
		sql_subtype *t = exp_totype(e);

		if (f->type->eclass == t->type->eclass && EC_COMPUTE(f->type->eclass)) {
			if (f->type->localtype < t->type->localtype)
				return 1;
		}
	}
	return 0;
}

/* optimize (a = b) or (a is null and b is null) -> a = b with null semantics */
static sql_exp *
try_rewrite_equal_or_is_null(mvc *sql, sql_rel *rel, sql_exp *or, list *l1, list *l2)
{
	if (list_length(l1) == 1) {
		bool valid = true, first_is_null_found = false, second_is_null_found = false;
		sql_exp *cmp = l1->h->data;
		sql_exp *first = cmp->l, *second = cmp->r;

		if (is_compare(cmp->type) && !is_anti(cmp) && !cmp->f && cmp->flag == cmp_equal) {
			int fupcast = is_numeric_upcast(first), supcast = is_numeric_upcast(second);
			for(node *n = l2->h ; n && valid; n = n->next) {
				sql_exp *e = n->data, *l = e->l, *r = e->r;

				if (is_compare(e->type) && e->flag == cmp_equal && !e->f &&
					!is_anti(e) && is_semantics(e)) {
					int lupcast = is_numeric_upcast(l);
					int rupcast = is_numeric_upcast(r);
					sql_exp *rr = rupcast ? r->l : r;

					if (rr->type == e_atom && rr->l && atom_null(rr->l)) {
						if (exp_match_exp(fupcast?first->l:first, lupcast?l->l:l))
							first_is_null_found = true;
						else if (exp_match_exp(supcast?second->l:second, lupcast?l->l:l))
							second_is_null_found = true;
						else
							valid = false;
					} else {
						valid = false;
					}
				} else {
					valid = false;
				}
			}
			if (valid && first_is_null_found && second_is_null_found) {
				sql_subtype super;

				supertype(&super, exp_subtype(first), exp_subtype(second)); /* first and second must have the same type */
				if (!(first = exp_check_type(sql, &super, rel, first, type_equal)) ||
					!(second = exp_check_type(sql, &super, rel, second, type_equal))) {
						sql->session->status = 0;
						sql->errstr[0] = 0;
						return or;
					}
				sql_exp *res = exp_compare(sql->sa, first, second, cmp->flag);
				set_semantics(res);
				if (exp_name(or))
					exp_prop_alias(sql->sa, res, or);
				return res;
			}
		}
	}
	return or;
}

static list *
merge_cmp_or_null(mvc *sql, sql_rel *rel, list *exps, int *changes)
{
	for (node *n = exps->h; n ; n = n->next) {
		sql_exp *e = n->data;

		if (is_compare(e->type) && e->flag == cmp_or && !is_anti(e)) {
			sql_exp *ne = try_rewrite_equal_or_is_null(sql, rel, e, e->l, e->r);
			if (ne != e) {
				(*changes)++;
				n->data = ne;
			}
			ne = try_rewrite_equal_or_is_null(sql, rel, e, e->r, e->l);
			if (ne != e) {
				(*changes)++;
				n->data = ne;
			}
		}
	}
	return exps;
}

static inline sql_rel *
rel_select_cse(visitor *v, sql_rel *rel)
{
	if (is_select(rel->op) && rel->exps)
		rel->exps = merge_ors(v->sql, rel->exps, &v->changes); /* x = 1 or x = 2 => x in (1, 2)*/

	if (is_select(rel->op) && rel->exps)
		rel->exps = merge_notequal(v->sql, rel->exps, &v->changes); /* x <> 1 and x <> 2 => x not in (1, 2)*/

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && rel->exps)
		rel->exps = merge_cmp_or_null(v->sql, rel, rel->exps, &v->changes); /* (a = b) or (a is null and b is null) -> a = b with null semantics */

	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && rel->exps) {
		node *n;
		list *nexps;
		int needed = 0;

		for (n=rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e))
				needed = 1;
		}
		if (!needed)
			return rel;
		nexps = new_exp_list(v->sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e)) {
				/* split the common expressions */
				v->changes += exps_cse(v->sql, nexps, e->l, e->r);
			} else {
				append(nexps, e);
			}
		}
		rel->exps = nexps;
	}
	return rel;
}

static inline sql_rel *
rel_project_cse(visitor *v, sql_rel *rel)
{
	if (is_project(rel->op) && rel->exps) {
		node *n, *m;
		list *nexps;
		int needed = 0;

		for (n=rel->exps->h; n && !needed; n = n->next) {
			sql_exp *e1 = n->data;

			if (e1->type != e_column && !exp_is_atom(e1) && exp_name(e1)) {
				for (m=n->next; m; m = m->next){
					sql_exp *e2 = m->data;

					if (exp_name(e2) && exp_match_exp(e1, e2))
						needed = 1;
				}
			}
		}

		if (!needed)
			return rel;

		nexps = new_exp_list(v->sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e1 = n->data;

			if (e1->type != e_column && !exp_is_atom(e1) && exp_name(e1)) {
				for (m=nexps->h; m; m = m->next){
					sql_exp *e2 = m->data;

					if (exp_name(e2) && exp_match_exp(e1, e2) && (e1->type != e_column || exps_bind_column2(nexps, exp_relname(e1), exp_name(e1), NULL) == e1)) {
						sql_exp *ne = exp_alias(v->sql->sa, exp_relname(e1), exp_name(e1), exp_relname(e2), exp_name(e2), exp_subtype(e2), e2->card, has_nil(e2), is_unique(e2), is_intern(e1));

						ne = exp_propagate(v->sql->sa, ne, e1);
						exp_prop_alias(v->sql->sa, ne, e1);
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
exps_merge_select_rse( mvc *sql, list *l, list *r, bool *merged)
{
	node *n, *m, *o;
	list *nexps = NULL, *lexps, *rexps;
	bool lmerged = true, rmerged = true;

 	lexps = new_exp_list(sql->sa);
	for (n = l->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e) && !is_semantics(e)) {
			lmerged = false;
			list *nexps = exps_merge_select_rse(sql, e->l, e->r, &lmerged);
			for (o = nexps->h; o; o = o->next)
				append(lexps, o->data);
		} else {
			append(lexps, e);
		}
	}
	if (lmerged)
		lmerged = (list_length(lexps) == 1);
 	rexps = new_exp_list(sql->sa);
	for (n = r->h; n; n = n->next) {
		sql_exp *e = n->data;

		if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e) && !is_semantics(e)) {
			rmerged = false;
			list *nexps = exps_merge_select_rse(sql, e->l, e->r, &rmerged);
			for (o = nexps->h; o; o = o->next)
				append(rexps, o->data);
		} else {
			append(rexps, e);
		}
	}
	if (rmerged)
		rmerged = (list_length(r) == 1);

 	nexps = new_exp_list(sql->sa);

	/* merge merged lists first ? */
	for (n = lexps->h; n; n = n->next) {
		sql_exp *le = n->data, *re, *fnd = NULL;

		if (le->type != e_cmp || le->flag == cmp_or || is_anti(le) || is_semantics(le) || is_symmetric(le))
			continue;
		for (m = rexps->h; !fnd && m; m = m->next) {
			re = m->data;
			if (exps_match_col_exps(le, re))
				fnd = re;
		}
		if (fnd && (is_anti(fnd) || is_semantics(fnd)))
			continue;
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
			if (is_anti(le) || is_anti(re) || is_symmetric(re))
				continue;
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
				sql_exp *mine = NULL, *maxe = NULL;

				if (!(mine = rel_binop_(sql, NULL, le->r, re->r, "sys", "sql_min", card_value))) {
					sql->session->status = 0;
					sql->errstr[0] = '\0';
					continue;
				}
				if (!(maxe = rel_binop_(sql, NULL, le->f, re->f, "sys", "sql_max", card_value))) {
					sql->session->status = 0;
					sql->errstr[0] = '\0';
					continue;
				}
				fnd = exp_compare2(sql->sa, exp_copy(sql, le->l), mine, maxe, le->flag, 0);
				lmerged = false;
			}
			if (fnd) {
				append(nexps, fnd);
				*merged = (fnd && lmerged && rmerged);
			}
		}
	}
	return nexps;
}

static sql_exp *
rel_merge_project_rse(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void) depth;

	if (is_simple_project(rel->op) && is_func(e->type) && e->l) {
		list *fexps = e->l;
		sql_subfunc *f = e->f;

		/* is and function */
		if (strcmp(f->func->base.name, "and") == 0 && list_length(fexps) == 2) {
			sql_exp *l = list_fetch(fexps, 0);
			sql_exp *r = list_fetch(fexps, 1);

			/* check merge into single between */
			if (is_func(l->type) && is_func(r->type)) {
				list *lfexps = l->l;
				list *rfexps = r->l;
				sql_subfunc *lff = l->f;
				sql_subfunc *rff = r->f;

				if (((strcmp(lff->func->base.name, ">=") == 0 || strcmp(lff->func->base.name, ">") == 0) && list_length(lfexps) == 2) &&
				    ((strcmp(rff->func->base.name, "<=") == 0 || strcmp(rff->func->base.name, "<") == 0) && list_length(rfexps) == 2)) {
					sql_exp *le = list_fetch(lfexps,0), *lf = list_fetch(rfexps,0);
					int c_le = is_numeric_upcast(le), c_lf = is_numeric_upcast(lf);

					if (exp_equal(c_le?le->l:le, c_lf?lf->l:lf) == 0) {
						sql_exp *re = list_fetch(lfexps, 1), *rf = list_fetch(rfexps, 1), *ne = NULL;
						sql_subtype super;

						supertype(&super, exp_subtype(le), exp_subtype(lf)); /* le/re and lf/rf must have the same type */
						if (!(le = exp_check_type(v->sql, &super, rel, le, type_equal)) ||
							!(re = exp_check_type(v->sql, &super, rel, re, type_equal)) ||
							!(rf = exp_check_type(v->sql, &super, rel, rf, type_equal))) {
								v->sql->session->status = 0;
								v->sql->errstr[0] = 0;
								return e;
							}
						if ((ne = exp_compare2(v->sql->sa, le, re, rf, compare_funcs2range(lff->func->base.name, rff->func->base.name), 0))) {
							if (exp_name(e))
								exp_prop_alias(v->sql->sa, ne, e);
							e = ne;
							v->changes++;
						}
					}
				}
			}
		}
	}
	return e;
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
 *
 * for single expression or's we can do better
 *		x in (a, b, c) or x in (d, e, f)
 *		->
 *		x in (a, b, c, d, e, f)
 * */
static inline sql_rel *
rel_merge_select_rse(visitor *v, sql_rel *rel)
{
	/* only execute once per select */
	if ((is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) && rel->exps && !rel->used) {
		node *n, *o;
		list *nexps = new_exp_list(v->sql->sa);

		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e) && !is_semantics(e)) {
				/* possibly merge related expressions */
				bool merged = false;

				list *ps = exps_merge_select_rse(v->sql, e->l, e->r, &merged);
				for (o = ps->h; o; o = o->next)
					append(nexps, o->data);
				if (merged)
					v->changes++;
				else
					append(nexps, e);
			} else {
				append(nexps, e);
			}
		}
		rel->exps = nexps;
		rel->used = 1;
	}
	return rel;
}

static sql_exp *list_exps_uses_exp(list *exps, const char *rname, const char *name);

static sql_exp*
exp_uses_exp(sql_exp *e, const char *rname, const char *name)
{
	sql_exp *res = NULL;

	switch (e->type) {
		case e_psm:
			break;
		case e_atom: {
			if (e->f)
				return list_exps_uses_exp(e->f, rname, name);
		} break;
		case e_convert:
			return exp_uses_exp(e->l, rname, name);
		case e_column: {
			if (e->l && rname && strcmp(e->l, rname) == 0 &&
				e->r && name && strcmp(e->r, name) == 0)
				return e;
			if (!e->l && !rname &&
				e->r && name && strcmp(e->r, name) == 0)
				return e;
		} break;
		case e_func:
		case e_aggr: {
			if (e->l)
				return list_exps_uses_exp(e->l, rname, name);
		} 	break;
		case e_cmp: {
			if (e->flag == cmp_in || e->flag == cmp_notin) {
				if ((res = exp_uses_exp(e->l, rname, name)))
					return res;
				return list_exps_uses_exp(e->r, rname, name);
			} else if (e->flag == cmp_or || e->flag == cmp_filter) {
				if ((res = list_exps_uses_exp(e->l, rname, name)))
					return res;
				return list_exps_uses_exp(e->r, rname, name);
			} else {
				if ((res = exp_uses_exp(e->l, rname, name)))
					return res;
				if ((res = exp_uses_exp(e->r, rname, name)))
					return res;
				if (e->f)
					return exp_uses_exp(e->f, rname, name);
			}
		} break;
	}
	return NULL;
}

static sql_exp *
list_exps_uses_exp(list *exps, const char *rname, const char *name)
{
	sql_exp *res = NULL;

	if (!exps)
		return NULL;
	for (node *n = exps->h; n && !res; n = n->next) {
		sql_exp *e = n->data;
		res = exp_uses_exp(e, rname, name);
	}
	return res;
}

/* find in the list of expression an expression which uses e */
static sql_exp *
exps_uses_exp(list *exps, sql_exp *e)
{
	return list_exps_uses_exp(exps, exp_relname(e), exp_name(e));
}

static bool
exps_uses_any(list *exps, list *l)
{
	bool uses_any = false;

	for (node *n = l->h; n && !uses_any; n = n->next) {
		sql_exp *e = n->data;
		uses_any |= list_exps_uses_exp(exps, exp_relname(e), exp_name(e)) != NULL;
	}

	return uses_any;
}

/*
 * Rewrite aggregations over union all.
 *	groupby ([ union all (a, b) ], [gbe], [ count, sum ] )
 *
 * into
 * 	groupby ( [ union all( groupby( a, [gbe], [ count, sum] ), [ groupby( b, [gbe], [ count, sum] )) , [gbe], [sum, sum] )
 */
static inline sql_rel *
rel_push_aggr_down(visitor *v, sql_rel *rel)
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

		if (!u || !is_union(u->op) || need_distinct(u) || is_single(u) || !u->exps || rel_is_ref(u))
			return rel;

		ul = u->l;
		ur = u->r;

		/* make sure we don't create group by on group by's */
		if (ul->op == op_groupby || ur->op == op_groupby)
			return rel;

		/* distinct should be done over the full result */
		for (n = g->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			sql_subfunc *af = e->f;

			if (e->type == e_atom ||
			    e->type == e_func ||
			   (e->type == e_aggr &&
			   ((strcmp(af->func->base.name, "sum") &&
			     strcmp(af->func->base.name, "count") &&
			     strcmp(af->func->base.name, "min") &&
			     strcmp(af->func->base.name, "max")) ||
			   need_distinct(e))))
				return rel;
		}

		ul = rel_dup(ul);
		ur = rel_dup(ur);
		if (!is_project(ul->op))
			ul = rel_project(v->sql->sa, ul,
				rel_projections(v->sql, ul, NULL, 1, 1));
		if (!is_project(ur->op))
			ur = rel_project(v->sql->sa, ur,
				rel_projections(v->sql, ur, NULL, 1, 1));
		rel_rename_exps(v->sql, u->exps, ul->exps);
		rel_rename_exps(v->sql, u->exps, ur->exps);
		if (u != ou) {
			ul = rel_project(v->sql->sa, ul, NULL);
			ul->exps = exps_copy(v->sql, ou->exps);
			rel_rename_exps(v->sql, ou->exps, ul->exps);
			ur = rel_project(v->sql->sa, ur, NULL);
			ur->exps = exps_copy(v->sql, ou->exps);
			rel_rename_exps(v->sql, ou->exps, ur->exps);
		}

		if (g->r && list_length(g->r) > 0) {
			list *gbe = g->r;

			lgbe = exps_copy(v->sql, gbe);
			rgbe = exps_copy(v->sql, gbe);
		}
		ul = rel_groupby(v->sql, ul, NULL);
		ul->r = lgbe;
		ul->nrcols = g->nrcols;
		ul->card = g->card;
		ul->exps = exps_copy(v->sql, g->exps);
		ul->nrcols = list_length(ul->exps);

		ur = rel_groupby(v->sql, ur, NULL);
		ur->r = rgbe;
		ur->nrcols = g->nrcols;
		ur->card = g->card;
		ur->exps = exps_copy(v->sql, g->exps);
		ur->nrcols = list_length(ur->exps);

		/* group by on primary keys which define the partioning scheme
		 * don't need a finalizing group by */
		/* how to check if a partition is based on some primary key ?
		 * */
		if (!list_empty(rel->r)) {
			for (node *n = ((list*)rel->r)->h; n; n = n->next) {
				sql_exp *e = n->data;
				sql_column *c = NULL;

				if ((c = exp_is_pkey(rel, e)) && partition_find_part(v->sql->session->tr, c->t, NULL)) {
					/* check if key is partition key */
					v->changes++;
					return rel_inplace_setop(v->sql, rel, ul, ur, op_union,
											 rel_projections(v->sql, rel, NULL, 1, 1));
				}
			}
		}

		if (!list_empty(rel->r)) {
			list *ogbe = rel->r;

			gbe = new_exp_list(v->sql->sa);
			for (n = ogbe->h; n; n = n->next) {
				sql_exp *e = n->data, *ne;

				/* group by in aggreation list */
				ne = exps_uses_exp( rel->exps, e);
				if (ne)
					ne = list_find_exp( ul->exps, ne);
				if (!ne) {
					/* e only in the ul/ur->r (group by list) */
					ne = exp_ref(v->sql, e);
					list_append(ul->exps, ne);
					ne = exp_ref(v->sql, e);
					list_append(ur->exps, ne);
				}
				assert(ne);
				ne = exp_ref(v->sql, ne);
				append(gbe, ne);
			}
		}

		u = rel_setop(v->sql->sa, ul, ur, op_union);
		rel_setop_set_exps(v->sql, u, rel_projections(v->sql, ul, NULL, 1, 1), false);
		set_processed(u);

		exps = new_exp_list(v->sql->sa);
		for (n = u->exps->h, m = rel->exps->h; n && m; n = n->next, m = m->next) {
			sql_exp *ne, *e = n->data, *oa = m->data;

			if (oa->type == e_aggr) {
				sql_subfunc *f = oa->f;
				int cnt = exp_aggr_is_count(oa);
				sql_subfunc *a = sql_bind_func(v->sql, "sys", (cnt)?"sum":f->func->base.name, exp_subtype(e), NULL, F_AGGR);

				assert(a);
				/* union of aggr result may have nils
			   	 * because sum/count of empty set */
				set_has_nil(e);
				e = exp_ref(v->sql, e);
				ne = exp_aggr1(v->sql->sa, e, a, need_distinct(e), 1, e->card, 1);
			} else {
				ne = exp_copy(v->sql, oa);
			}
			exp_setname(v->sql->sa, ne, exp_find_rel_name(oa), exp_name(oa));
			append(exps, ne);
		}
		v->changes++;
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
gen_push_groupby_down(mvc *sql, sql_rel *rel, int *changes)
{
	sql_rel *j = rel->l;
	list *gbe = rel->r;

	if (rel->op == op_groupby && list_length(gbe) == 1 && j->op == op_join){
		sql_rel *jl = j->l, *jr = j->r, *cr, *cl;
		sql_exp *gb = gbe->h->data, *e;
		node *n;
		int left = 1;
		list *aggrs, *aliases, *gbe;

		if (!is_identity(gb, jl) && !is_identity(gb, jr))
			return rel;
		if (jl->op == op_project &&
		    (e = list_find_exp( jl->exps, gb)) != NULL &&
		     find_prop(e->p, PROP_HASHCOL) != NULL) {
			left = 0;
			cr = jr;
			cl = jl;
		} else if (jr->op == op_project &&
		    (e = list_find_exp( jr->exps, gb)) != NULL &&
		     find_prop(e->p, PROP_HASHCOL) != NULL) {
			left = 1;
			cr = jl;
			cl = jr;
		} else {
			return rel;
		}

		if ((left && is_base(jl->op)) || (!left && is_base(jr->op))||
		    (left && is_select(jl->op)) || (!left && is_select(jr->op))
		    || rel_is_join_on_pkey(j, false))
			return rel;

		/* only add aggr (based on left/right), and repeat the group by column */
		aggrs = sa_list(sql->sa);
		aliases = sa_list(sql->sa);
		if (rel->exps) for (n = rel->exps->h; n; n = n->next) {
			sql_exp *ce = n->data;

			if (exp_is_atom(ce))
				list_append(aliases, ce);
			else if (ce->type == e_column) {
				if (rel_has_exp(cl, ce, false) == 0) /* collect aliases outside groupby */
					list_append(aliases, ce);
				else
					list_append(aggrs, ce);
			} else if (ce->type == e_aggr) {
				list *args = ce->l;

				/* check args are part of left/right */
				if (!list_empty(args) && rel_has_exps(cl, args, false) == 0)
					return rel;
				if (rel->op != op_join && exp_aggr_is_count(ce))
					ce->p = prop_create(sql->sa, PROP_COUNT, ce->p);
				list_append(aggrs, ce);
			}
		}
		/* TODO move any column expressions (aliases) into the project list */

		/* find gb in left or right and should be unique */
		gbe = sa_list(sql->sa);
		/* push groupby to right, group on join exps */
		if (j->exps) for (n = j->exps->h; n; n = n->next) {
			sql_exp *ce = n->data, *l = ce->l, *r = ce->r, *e;

			/* get left/right hand of e_cmp */
			assert(ce->type == e_cmp);
			if (ce->flag == cmp_equal && is_alias(l->type) && is_alias(r->type) &&
				(((e = rel_find_exp(cr, l)) && rel_find_exp(cl, r)) ||
				 ((e = rel_find_exp(cr, r)) && rel_find_exp(cl, l)))) {
				e = exp_ref(sql, e);
				list_append(gbe, e);
			} else {
				return rel;
			}
		}
		if (!left)
			cr = j->r = rel_groupby(sql, cr, gbe);
		else
			cr = j->l = rel_groupby(sql, cr, gbe);
		cr->exps = list_merge(cr->exps, aggrs, (fdup)NULL);
		set_processed(cr);
		if (!is_project(cl->op))
			cl = rel_project(sql->sa, cl,
				rel_projections(sql, cl, NULL, 1, 1));
		cl->exps = list_merge(cl->exps, aliases, (fdup)NULL);
		set_processed(cl);
		if (!left)
			j->l = cl;
		else
			j->r = cl;
		rel -> l = NULL;
		rel_destroy(rel);

		if (list_empty(cr->exps) && list_empty(j->exps)) { /* remove crossproduct */
			sql_rel *r = cl;
			if (!left)
				j->l = NULL;
			else
				j->r = NULL;
			rel_destroy(j);
			j = r;
		}
		(*changes)++;
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
static inline sql_rel *
rel_push_groupby_down(visitor *v, sql_rel *rel)
{
	sql_rel *p = rel->l;
	list *gbe = rel->r;

	if (rel->op == op_groupby && gbe && p && is_join(p->op))
		return gen_push_groupby_down(v->sql, rel, &v->changes);
	if (rel->op == op_groupby && gbe && p && p->op == op_project) {
		sql_rel *j = p->l;
		sql_rel *jl, *jr;
		node *n;

		if (!j || j->op != op_join || list_length(j->exps) != 1)
			return gen_push_groupby_down(v->sql, rel, &v->changes);
		jl = j->l;
		jr = j->r;

		/* check if jr is a dict with index and var still used */
		if (jr->op != op_basetable || jr->l || !jr->r || list_length(jr->exps) != 2)
			return gen_push_groupby_down(v->sql, rel, &v->changes);

		/* check if group by is done on dict column */
		for(n = gbe->h; n; n = n->next) {
			sql_exp *ge = n->data, *pe = NULL, *e = NULL;

			/* find group by exp in project, then in dict */
			pe = rel_find_exp(p, ge);
			if (pe) /* find project exp in right hand of join, ie dict */
				e = rel_find_exp(jr, pe);
			if (pe && e) {  /* Rewrite: join with dict after the group by */
				list *pexps = rel_projections(v->sql, rel, NULL, 1, 1), *npexps;
				node *m;
				sql_exp *ne = j->exps->h->data; /* join exp */
				p->l = jl;	/* Project now only on the left side of the join */

				ne = ne->l; 	/* The left side of the compare is the index of the left */

				/* find ge reference in new projection list */
				npexps = sa_list(v->sql->sa);
				for (m = pexps->h; m; m = m->next) {
					sql_exp *a = m->data;

					if (exp_refers(ge, a)) {
						sql_exp *sc = jr->exps->t->data;
						sql_exp *e = exp_ref(v->sql, sc);
						if (exp_name(a))
							exp_prop_alias(v->sql->sa, e, a);
						a = e;
					}
					append(npexps, a);
				}

				/* find ge in aggr list */
				for (m = rel->exps->h; m; m = m->next) {
					sql_exp *a = m->data;

					if (exp_match_exp(a, ge) || exp_refers(ge, a)) {
						a = exp_ref(v->sql, ne);
						if (exp_name(ne))
							exp_prop_alias(v->sql->sa, a, ne);
						m->data = a;
					}
				}

				/* change alias pe, ie project out the index  */
				pe->l = (void*)exp_relname(ne);
				pe->r = (void*)exp_name(ne);
				if (exp_name(ne))
					exp_prop_alias(v->sql->sa, pe, ne);

				/* change alias ge */
				ge->l = (void*)exp_relname(pe);
				ge->r = (void*)exp_name(pe);
				if (exp_name(pe))
					exp_prop_alias(v->sql->sa, ge, pe);

				/* zap both project and groupby name hash tables (as we changed names above) */
				list_hash_clear(rel->exps);
				list_hash_clear((list*)rel->r);
				list_hash_clear(p->exps);

				/* add join */
				j->l = rel;
				rel = rel_project(v->sql->sa, j, npexps);
				v->changes++;
			}
		}
	}
	return rel;
}

/* 
 * Gets the column expressions of a diff function and adds them to "columns".
 * The diff function has two possible argument types: either a sql_exp representing a column
 * or a sql_exp representing another diff function, therefore this function is recursive.
 */
static void
get_diff_function_columns(sql_exp *diffExp, list *columns) {
	list *args = diffExp->l;
	
	for (node *arg = args->h; arg; arg = arg->next) {
		sql_exp *exp = arg->data;
		
		// diff function
		if (exp->type == e_func) {
			get_diff_function_columns(exp, columns);
		}
		// column
		else {
			list_append(columns, exp);
		}
	}
}

/* 
 * Builds a list of aggregation key columns to be used by the select push down algorithm, namely for
 * window functions. Returns NULL if the window function does not partition by any column
 */
static list *
get_aggregation_key_columns(sql_allocator *sa, sql_rel *r) {
	for (node* n = r->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		
		if (e->type == e_func) {
			sql_subfunc *f = e->f;
			
			// aggregation function
			if (!strcmp(f->func->base.name, "rank")) {
				list* rankArguments = e->l;
				// the partition key is the second argument
				sql_exp *partitionExp = rankArguments->h->next->data;

				// check if the key contains any columns, i.e., is a diff function
				if (partitionExp->type == e_func) {
					// get columns to list
					list *aggColumns = sa_list(sa);
					get_diff_function_columns(partitionExp, aggColumns);
					return aggColumns;
				}
				// the function has no aggregation columns (e_atom of boolean)
				else {
					return NULL;
				}

			}
		}
	}
	return NULL;
}

/*
 * Checks if a filter column is also used as an aggregation key, so it can be later safely pushed down.
 */
static int
filter_column_in_aggregation_columns(sql_exp *column, list *aggColumns) {
	/* check if it is a column or an e_convert, and get the actual column if it is the latter */
	if (column->type == e_convert) {
		column = column->l;
	}

	char *tableName = column->l;
	char *columnName = column->r;

	for (node *n = aggColumns->h; n; n = n->next) {
		sql_exp *aggCol = n->data;
		char *aggColTableName = aggCol->l;
		char *aggColColumnName = aggCol->r;
		
		if (!strcmp(tableName, aggColTableName) && !strcmp(columnName, aggColColumnName)) {
			/* match */
			return 1;
		}
	}

	/* no matches found */
	return 0;
}


/*
 * Push select down, pushes the selects through (simple) projections. Also
 * it cleans up the projections which become useless.
 *
 * WARNING - Make sure to call try_remove_empty_select macro before returning so we ensure
 * possible generated empty selects won't never be generated
 */
static sql_rel *
rel_push_select_down(visitor *v, sql_rel *rel)
{
	list *exps = NULL;
	sql_rel *r = NULL;
	node *n;

	if (rel_is_ref(rel)) {
		if (is_select(rel->op) && rel->exps) {
			/* add inplace empty select */
			sql_rel *l = rel_select(v->sql->sa, rel->l, NULL);

			l->exps = rel->exps;
			rel->exps = NULL;
			rel->l = l;
			v->changes++;
		}
		return rel;
	}

	/* don't make changes for empty selects */
	if (is_select(rel->op) && list_empty(rel->exps))
		return try_remove_empty_select(v, rel);

	/* merge 2 selects */
	r = rel->l;
	if (is_select(rel->op) && r && r->exps && is_select(r->op) && !(rel_is_ref(r)) && !exps_have_func(rel->exps)) {
		(void)list_merge(r->exps, rel->exps, (fdup)NULL);
		rel->l = NULL;
		rel_destroy(rel);
		v->changes++;
		return try_remove_empty_select(v, r);
	}
	/*
	 * Push select through semi/anti join
	 * 	select (semi(A,B)) == semi(select(A), B)
	 */
	if (is_select(rel->op) && r && is_semi(r->op) && !(rel_is_ref(r))) {
		rel->l = r->l;
		r->l = rel;
		v->changes++;
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
				lx = rel_project(v->sql->sa, rel, rel_projections(v->sql, rel, NULL, 1, 1));
				r->l = lx;
				rx->l = rel_dup(lx);
			}
		}
		return r;
	}
	exps = rel->exps;

	/* push select through join */
	if (is_select(rel->op) && r && is_join(r->op) && !rel_is_ref(r) && !is_single(r)){
		sql_rel *jl = r->l;
		sql_rel *jr = r->r;
		int left = r->op == op_join || r->op == op_left;
		int right = r->op == op_join || r->op == op_right;

		if (r->op == op_full)
			return rel;

		/* introduce selects under the join (if needed) */
		set_processed(jl);
		set_processed(jr);
		for (n = exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (left && rel_rebind_exp(v->sql, jl, e)) {
				if (!is_select(jl->op) || rel_is_ref(jl))
					r->l = jl = rel_select(v->sql->sa, jl, NULL);
				rel_select_add_exp(v->sql->sa, jl, e);
				list_remove_node(exps, NULL, n);
				v->changes++;
			} else if (right && rel_rebind_exp(v->sql, jr, e)) {
				if (!is_select(jr->op) || rel_is_ref(jr))
					r->r = jr = rel_select(v->sql->sa, jr, NULL);
				rel_select_add_exp(v->sql->sa, jr, e);
				list_remove_node(exps, NULL, n);
				v->changes++;
			}
			n = next;
		}
	}

	/* merge select and cross product ? */
	if (is_select(rel->op) && r && r->op == op_join && !rel_is_ref(r) && !is_single(r)){
		for (n = exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (exp_is_join(e, NULL) == 0) {
				if (!r->exps)
					r->exps = new_exp_list(v->sql->sa);
				append(r->exps, e);
				list_remove_node(exps, NULL, n);
				v->changes++;
			}
			n = next;
		}
	}

	if (is_select(rel->op) && r && r->op == op_project && !rel_is_ref(r) && !is_single(r)){
		sql_rel *pl = r->l;
		/* we cannot push through window functions (for safety I disabled projects over DDL too) */
		if (pl && pl->op != op_ddl && !exps_have_unsafe(r->exps, 0)) {
			/* introduce selects under the project (if needed) */
			set_processed(pl);
			for (n = exps->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data, *ne = NULL;

				if (e->type == e_cmp) {
					ne = exp_push_down_prj(v->sql, e, r, pl);

					/* can we move it down */
					if (ne && ne != e && pl->exps) {
						if (!is_select(pl->op) || rel_is_ref(pl))
							r->l = pl = rel_select(v->sql->sa, pl, NULL);
						rel_select_add_exp(v->sql->sa, pl, ne);
						list_remove_node(exps, NULL, n);
						v->changes++;
					}
				}
				n = next;
			}
		}
		
		/* push filters if they match the aggregation key on a window function */
		else if (pl && pl->op != op_ddl && exps_have_unsafe(r->exps, 0)) {
			set_processed(pl);
			/* list of aggregation key columns */
			list *aggColumns = get_aggregation_key_columns(v->sql->sa, r);

			/* aggregation keys found, check if any filter matches them */
			if (aggColumns) {
				for (n = exps->h; n;) {
					node *next = n->next;
					sql_exp *e = n->data, *ne = NULL;
					
					if (e->type == e_cmp) {
						/* simple comparison filter */
						if (e->flag == cmp_gt || e->flag == cmp_gte || e->flag == cmp_lte || e->flag == cmp_lt
							|| e->flag == cmp_equal || e->flag == cmp_notequal || e->flag == cmp_in || e->flag == cmp_notin
							|| (e->flag == cmp_filter && ((list*)e->l)->cnt == 1)) {
							sql_exp* column;
							/* the column in 'like' filters is stored inside a list */
							if (e->flag == cmp_filter) {
								column = ((list*)e->l)->h->data;
							}
							else {
								column = e->l;
							}

							/* check if the expression matches any aggregation key, meaning we can 
							   try to safely push it down */
							if (filter_column_in_aggregation_columns(column, aggColumns)) {
								ne = exp_push_down_prj(v->sql, e, r, pl);

								/* can we move it down */
								if (ne && ne != e && pl->exps) {
									if (!is_select(pl->op) || rel_is_ref(pl))
										r->l = pl = rel_select(v->sql->sa, pl, NULL);
									rel_select_add_exp(v->sql->sa, pl, ne);
									list_remove_node(exps, NULL, n);
									v->changes++;
								}
							}
						}
					}
					n = next;
				}

				/* cleanup list */
				list_destroy(aggColumns);
			}
		}
	}

	/* try push select under set relation */
	if (is_select(rel->op) && r && is_set(r->op) && !list_empty(r->exps) && !rel_is_ref(r) && !is_single(r) && !list_empty(exps)) {
		sql_rel *u = r, *ul = u->l, *ur = u->r;

		ul = rel_dup(ul);
		ur = rel_dup(ur);
		if (!is_project(ul->op))
			ul = rel_project(v->sql->sa, ul,
				rel_projections(v->sql, ul, NULL, 1, 1));
		if (!is_project(ur->op))
			ur = rel_project(v->sql->sa, ur,
				rel_projections(v->sql, ur, NULL, 1, 1));
		rel_rename_exps(v->sql, u->exps, ul->exps);
		rel_rename_exps(v->sql, u->exps, ur->exps);

		/* introduce selects under the set */
		ul = rel_select(v->sql->sa, ul, NULL);
		ul->exps = exps_copy(v->sql, exps);
		ur = rel_select(v->sql->sa, ur, NULL);
		ur->exps = exps_copy(v->sql, exps);

		rel = rel_inplace_setop(v->sql, rel, ul, ur, u->op, rel_projections(v->sql, rel, NULL, 1, 1));
		if (need_distinct(u))
			set_distinct(rel);
		v->changes++;
	}

	return try_remove_empty_select(v, rel);
}

static inline sql_rel *
rel_push_join_exps_down(visitor *v, sql_rel *rel)
{
	/* push select exps part of join expressions down */
	if ((is_innerjoin(rel->op) || is_left(rel->op) || is_right(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		int left = is_innerjoin(rel->op) || is_right(rel->op) || is_semi(rel->op);
		int right = is_innerjoin(rel->op) || is_left(rel->op) || is_semi(rel->op);

		for (node *n = rel->exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (left && rel_rebind_exp(v->sql, rel->l, e)) { /* select expressions on left */
				sql_rel *l = rel->l;
				if (!is_select(l->op) || rel_is_ref(l)) {
					set_processed(l);
					rel->l = l = rel_select(v->sql->sa, rel->l, NULL);
				}
				rel_select_add_exp(v->sql->sa, rel->l, e);
				list_remove_node(rel->exps, NULL, n);
				v->changes++;
			} else if (right && (rel->op != op_anti || (e->flag != mark_notin && e->flag != mark_in)) &&
					   rel_rebind_exp(v->sql, rel->r, e)) { /* select expressions on right */
				sql_rel *r = rel->r;
				if (!is_select(r->op) || rel_is_ref(r)) {
					set_processed(r);
					rel->r = r = rel_select(v->sql->sa, rel->r, NULL);
				}
				rel_select_add_exp(v->sql->sa, rel->r, e);
				list_remove_node(rel->exps, NULL, n);
				v->changes++;
			}
			n = next;
		}
		if (is_join(rel->op) && list_empty(rel->exps))
			rel->exps = NULL; /* crossproduct */
	}
	return rel;
}

static bool
point_select_on_unique_column(sql_rel *rel)
{
	if (is_select(rel->op) && !list_empty(rel->exps)) {
		for (node *n = rel->exps->h; n ; n = n->next) {
			sql_exp *e = n->data, *el = e->l, *er = e->r, *found = NULL;

			if (is_compare(e->type) && e->flag == cmp_equal) {
				if (is_numeric_upcast(el))
					el = el->l;
				if (is_numeric_upcast(er))
					er = er->l;
				if (is_alias(el->type) && exp_is_atom(er) && (found = rel_find_exp(rel->l, el)) &&
					is_unique(found) && (!is_semantics(e) || !has_nil(found) || !has_nil(er)))
					return true;
				if (is_alias(er->type) && exp_is_atom(el) && (found = rel_find_exp(rel->l, er)) &&
					is_unique(found) && (!is_semantics(e) || !has_nil(el) || !has_nil(found)))
					return true;
			}
		}
	}
	return false;
}

/*
 * A point select on an unique column reduces the number of rows to 1. If the same select is under a
 * join, the opposite side's select can be pushed above the join.
 */
static sql_rel *
rel_push_select_up(visitor *v, sql_rel *rel)
{
	if ((is_innerjoin(rel->op) || is_left(rel->op) || is_right(rel->op) || is_semi(rel->op)) && !is_single(rel)) {
		sql_rel *l = rel->l, *r = rel->r;
		bool can_pushup_left = is_select(l->op) && !rel_is_ref(l) && !is_single(l) && (is_innerjoin(rel->op) || is_left(rel->op) || is_semi(rel->op)),
			 can_pushup_right = is_select(r->op) && !rel_is_ref(r) && !is_single(r) && (is_innerjoin(rel->op) || is_right(rel->op));

		if (can_pushup_left || can_pushup_right) {
			if (can_pushup_left)
				can_pushup_left = point_select_on_unique_column(r);
			if (can_pushup_right)
				can_pushup_right = point_select_on_unique_column(l);

			/* if both selects retrieve one row each, it's not worth it to push both up */
			if (can_pushup_left && !can_pushup_right) {
				sql_rel *ll = l->l;
				rel->l = ll;
				l->l = rel;
				rel = l;
				assert(is_select(rel->op));
				v->changes++;
			} else if (!can_pushup_left && can_pushup_right) {
				sql_rel *rl = r->l;
				rel->r = rl;
				r->l = rel;
				rel = r;
				assert(is_select(rel->op));
				v->changes++;
			}
		}
	}
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
static inline sql_rel *
rel_push_join_down(visitor *v, sql_rel *rel)
{
	if (!rel_is_ref(rel) && ((is_left(rel->op) || rel->op == op_join || is_semi(rel->op)) && rel->l && rel->exps)) {
		sql_rel *gb = rel->r, *ogb = gb, *l = NULL, *rell = rel->l;

		if (is_simple_project(gb->op) && !rel_is_ref(gb))
			gb = gb->l;

		if (rel_is_ref(rell) || !gb || rel_is_ref(gb))
			return rel;

		if (is_groupby(gb->op) && gb->r && list_length(gb->r)) {
			list *exps = rel->exps, *jes = new_exp_list(v->sql->sa), *gbes = gb->r;
			node *n, *m;
			/* find out if all group by expressions are used in the join */
			for(n = gbes->h; n; n = n->next) {
				sql_exp *gbe = n->data;
				int fnd = 0;
				const char *rname = NULL, *name = NULL;

				/* project in between, ie find alias */
				/* first find expression in expression list */
				gbe = exps_uses_exp( gb->exps, gbe);
				if (!gbe)
					continue;
				if (ogb != gb)
					gbe = exps_uses_exp( ogb->exps, gbe);
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
							sql_exp *re = exp_push_down_prj(v->sql, r, gb, gb->l);
							if (!re || (list_length(jes) == 0 && !find_prop(le->p, PROP_HASHCOL))) {
								fnd = 0;
							} else {
								int anti = is_anti(je), semantics = is_semantics(je);

								je = exp_compare(v->sql->sa, le, re, je->flag);
								if (anti) set_anti(je);
								if (semantics) set_semantics(je);
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
			l = gb->l = rel_crossproduct(v->sql->sa, gb->l, l, op_semi);
			l->exps = jes;
			v->changes++;
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
 *
 * in some cases the other way is usefull, ie push join down
 * semijoin. When the join reduces (ie when there are selects on it).
 *
 * At the moment, we only flag changes by this optimizer on the first level of optimization
 */
static inline sql_rel *
rel_push_semijoin_down_or_up(visitor *v, sql_rel *rel)
{
	int level = *(int*)v->data;

	if (rel->op == op_join && rel->exps && rel->l) {
		sql_rel *l = rel->l, *r = rel->r;

		if (is_semi(l->op) && !rel_is_ref(l) && is_select(r->op) && !rel_is_ref(r)) {
			rel->l = l->l;
			l->l = rel;
			if (level <= 0)
				v->changes++;
			return l;
		}
	}
	/* also case with 2 joins */
	/* join ( join ( semijoin(), table), select (table)); */
	if (rel->op == op_join && rel->exps && rel->l) {
		sql_rel *l = rel->l, *r = rel->r;
		sql_rel *ll;

		if (is_join(l->op) && !rel_is_ref(l) && is_select(r->op) && !rel_is_ref(r)) {
			ll = l->l;
			if (is_semi(ll->op) && !rel_is_ref(ll)) {
				l->l = ll->l;
				ll->l = rel;
				if (level <= 0)
					v->changes++;
				return ll;
			}
		}
	}
	/* first push down the expressions involving only A */
	if (rel->op == op_semi && rel->exps && rel->l) {
		for (node *n = rel->exps->h; n;) {
			node *next = n->next;
			sql_exp *e = n->data;

			if (n != rel->exps->h && e->type == e_cmp && rel_rebind_exp(v->sql, rel->l, e)) {
				sql_rel *l = rel->l;
				if (!is_select(l->op) || rel_is_ref(l)) {
					set_processed(l);
					rel->l = l = rel_select(v->sql->sa, rel->l, NULL);
				}
				rel_select_add_exp(v->sql->sa, rel->l, e);
				list_remove_node(rel->exps, NULL, n);
				if (level <= 0)
					v->changes++;
			}
			n = next;
		}
	}
	if (rel->op == op_semi && rel->exps && rel->l) {
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

		if (!is_join(l->op) || is_full(l->op) || rel_is_ref(l) || is_single(l))
			return rel;

		lop = l->op;
		ll = l->l;
		lr = l->r;

		/* check which side is used and other exps are atoms or from right of semijoin */
		for(n = exps->h; n; n = n->next) {
			sql_exp *sje = n->data;

			if (sje->type != e_cmp || is_complex_exp(sje->flag))
				return rel;
			/* sje->l from ll and sje->r/f from semijoin r ||
			 * sje->l from semijoin r and sje->r/f from ll ||
			 * sje->l from lr and sje->r/f from semijoin r ||
			 * sje->l from semijoin r and sje->r/f from lr */
			if (left &&
			   ((rel_rebind_exp(v->sql, ll, sje->l) && rel_rebind_exp(v->sql, rel->r, sje->r) && (!sje->f || rel_rebind_exp(v->sql, rel->r, sje->f))) ||
			    (rel_rebind_exp(v->sql, rel->r, sje->l) && rel_rebind_exp(v->sql, ll, sje->r) && (!sje->f || rel_rebind_exp(v->sql, ll, sje->f)))))
				right = 0;
			else
				left = 0;
			if (right &&
			   ((rel_rebind_exp(v->sql, lr, sje->l) && rel_rebind_exp(v->sql, rel->r, sje->r) && (!sje->f || rel_rebind_exp(v->sql, rel->r, sje->f))) ||
			    (rel_rebind_exp(v->sql, rel->r, sje->l) && rel_rebind_exp(v->sql, lr, sje->r) && (!sje->f || rel_rebind_exp(v->sql, lr, sje->f)))))
				left = 0;
			else
				right = 0;
			if (!right && !left)
				return rel;
		}
		if (left && is_right(lop))
			return rel;
		if (right && is_left(lop))
			return rel;
		nsexps = exps_copy(v->sql, rel->exps);
		njexps = exps_copy(v->sql, l->exps);
		if (left)
			l = rel_crossproduct(v->sql->sa, rel_dup(ll), rel_dup(r), op);
		else
			l = rel_crossproduct(v->sql->sa, rel_dup(lr), rel_dup(r), op);
		l->exps = nsexps;
		if (left)
			l = rel_crossproduct(v->sql->sa, l, rel_dup(lr), lop);
		else
			l = rel_crossproduct(v->sql->sa, rel_dup(ll), l, lop);
		l->exps = njexps;
		rel_destroy(rel);
		rel = l;
		if (level <= 0)
			v->changes++;
	}
	return rel;
}

static int
rel_part_nr( sql_rel *rel, sql_exp *e )
{
	sql_column *c = NULL;
	sql_rel *bt = NULL;
	assert(e->type == e_cmp);

	c = exp_find_column_(rel, e->l, -1, &bt);
	if (!c)
		c = exp_find_column_(rel, e->r, -1, &bt);
	if (!c && e->f)
		c = exp_find_column_(rel, e->f, -1, &bt);
	if (!c || !bt || !rel_base_get_mergetable(bt))
		return -1;
	sql_table *pp = c->t;
	sql_table *mt = rel_base_get_mergetable(bt);
	return find_member_pos(mt->members, pp);
}

static int
rel_uses_part_nr( sql_rel *rel, sql_exp *e, int pnr )
{
	sql_column *c = NULL;
	sql_rel *bt = NULL;
	assert(e->type == e_cmp);

	/*
	 * following case fails.
	 *
	 * semijoin( A1, union [A1, A2] )
	 * The union will never return proper column (from A2).
	 * ie need different solution (probaly pass pnr).
	 */
	c = exp_find_column_(rel, e->l, pnr, &bt);
	if (!c)
		c = exp_find_column_(rel, e->r, pnr, &bt);
	if (c && bt && rel_base_get_mergetable(bt)) {
		sql_table *pp = c->t;
		sql_table *mt = rel_base_get_mergetable(bt);
		if (find_member_pos(mt->members, pp) == pnr)
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
static inline sql_rel *
rel_push_join_down_union(visitor *v, sql_rel *rel)
{
	if ((is_join(rel->op) && !is_outerjoin(rel->op) && !is_single(rel)) || is_semi(rel->op)) {
		sql_rel *l = rel->l, *r = rel->r, *ol = l, *or = r;
		list *exps = rel->exps;
		sql_exp *je = NULL;

		if (!l || !r || need_distinct(l) || need_distinct(r) || rel_is_ref(l) || rel_is_ref(r))
			return rel;
		if (l->op == op_project)
			l = l->l;
		if (r->op == op_project)
			r = r->l;

		/* both sides only if we have a join index */
		if (!l || !r ||(is_union(l->op) && is_union(r->op) &&
			!(je = rel_is_join_on_pkey(rel, true)))) /* aligned PKEY-FKEY JOIN */
			return rel;
		if (is_semi(rel->op) && is_union(l->op) && !je)
			return rel;

		if ((is_union(l->op) && !need_distinct(l) && !is_single(l)) && !is_union(r->op)){
			sql_rel *nl, *nr;
			sql_rel *ll = rel_dup(l->l), *lr = rel_dup(l->r);

			/* join(union(a,b), c) -> union(join(a,c), join(b,c)) */
			if (!is_project(ll->op))
				ll = rel_project(v->sql->sa, ll,
					rel_projections(v->sql, ll, NULL, 1, 1));
			if (!is_project(lr->op))
				lr = rel_project(v->sql->sa, lr,
					rel_projections(v->sql, lr, NULL, 1, 1));
			rel_rename_exps(v->sql, l->exps, ll->exps);
			rel_rename_exps(v->sql, l->exps, lr->exps);
			if (l != ol) {
				ll = rel_project(v->sql->sa, ll, NULL);
				ll->exps = exps_copy(v->sql, ol->exps);
				lr = rel_project(v->sql->sa, lr, NULL);
				lr->exps = exps_copy(v->sql, ol->exps);
			}
			nl = rel_crossproduct(v->sql->sa, ll, rel_dup(or), rel->op);
			nr = rel_crossproduct(v->sql->sa, lr, rel_dup(or), rel->op);
			nl->exps = exps_copy(v->sql, exps);
			nr->exps = exps_copy(v->sql, exps);
			nl = rel_project(v->sql->sa, nl, rel_projections(v->sql, nl, NULL, 1, 1));
			nr = rel_project(v->sql->sa, nr, rel_projections(v->sql, nr, NULL, 1, 1));
			v->changes++;
			return rel_inplace_setop(v->sql, rel, nl, nr, op_union, rel_projections(v->sql, rel, NULL, 1, 1));
		} else if (is_union(l->op) && !need_distinct(l) && !is_single(l) &&
			   is_union(r->op) && !need_distinct(r) && !is_single(r) && je) {
			sql_rel *nl, *nr;
			sql_rel *ll = rel_dup(l->l), *lr = rel_dup(l->r);
			sql_rel *rl = rel_dup(r->l), *rr = rel_dup(r->r);

			/* join(union(a,b), union(c,d)) -> union(join(a,c), join(b,d)) */
			if (!is_project(ll->op))
				ll = rel_project(v->sql->sa, ll,
					rel_projections(v->sql, ll, NULL, 1, 1));
			if (!is_project(lr->op))
				lr = rel_project(v->sql->sa, lr,
					rel_projections(v->sql, lr, NULL, 1, 1));
			rel_rename_exps(v->sql, l->exps, ll->exps);
			rel_rename_exps(v->sql, l->exps, lr->exps);
			if (l != ol) {
				ll = rel_project(v->sql->sa, ll, NULL);
				ll->exps = exps_copy(v->sql, ol->exps);
				lr = rel_project(v->sql->sa, lr, NULL);
				lr->exps = exps_copy(v->sql, ol->exps);
			}
			if (!is_project(rl->op))
				rl = rel_project(v->sql->sa, rl,
					rel_projections(v->sql, rl, NULL, 1, 1));
			if (!is_project(rr->op))
				rr = rel_project(v->sql->sa, rr,
					rel_projections(v->sql, rr, NULL, 1, 1));
			rel_rename_exps(v->sql, r->exps, rl->exps);
			rel_rename_exps(v->sql, r->exps, rr->exps);
			if (r != or) {
				rl = rel_project(v->sql->sa, rl, NULL);
				rl->exps = exps_copy(v->sql, or->exps);
				rr = rel_project(v->sql->sa, rr, NULL);
				rr->exps = exps_copy(v->sql, or->exps);
			}
			nl = rel_crossproduct(v->sql->sa, ll, rl, rel->op);
			nr = rel_crossproduct(v->sql->sa, lr, rr, rel->op);
			nl->exps = exps_copy(v->sql, exps);
			nr->exps = exps_copy(v->sql, exps);
			nl = rel_project(v->sql->sa, nl, rel_projections(v->sql, nl, NULL, 1, 1));
			nr = rel_project(v->sql->sa, nr, rel_projections(v->sql, nr, NULL, 1, 1));
			v->changes++;
			return rel_inplace_setop(v->sql, rel, nl, nr, op_union, rel_projections(v->sql, rel, NULL, 1, 1));
		} else if (!is_union(l->op) &&
			   is_union(r->op) && !need_distinct(r) && !is_single(r) &&
			   !is_semi(rel->op)) {
			sql_rel *nl, *nr;
			sql_rel *rl = rel_dup(r->l), *rr = rel_dup(r->r);

			/* join(a, union(b,c)) -> union(join(a,b), join(a,c)) */
			if (!is_project(rl->op))
				rl = rel_project(v->sql->sa, rl,
					rel_projections(v->sql, rl, NULL, 1, 1));
			if (!is_project(rr->op))
				rr = rel_project(v->sql->sa, rr,
					rel_projections(v->sql, rr, NULL, 1, 1));
			rel_rename_exps(v->sql, r->exps, rl->exps);
			rel_rename_exps(v->sql, r->exps, rr->exps);
			if (r != or) {
				rl = rel_project(v->sql->sa, rl, NULL);
				rl->exps = exps_copy(v->sql, or->exps);
				rr = rel_project(v->sql->sa, rr, NULL);
				rr->exps = exps_copy(v->sql, or->exps);
			}
			nl = rel_crossproduct(v->sql->sa, rel_dup(ol), rl, rel->op);
			nr = rel_crossproduct(v->sql->sa, rel_dup(ol), rr, rel->op);
			nl->exps = exps_copy(v->sql, exps);
			nr->exps = exps_copy(v->sql, exps);
			nl = rel_project(v->sql->sa, nl, rel_projections(v->sql, nl, NULL, 1, 1));
			nr = rel_project(v->sql->sa, nr, rel_projections(v->sql, nr, NULL, 1, 1));
			v->changes++;
			return rel_inplace_setop(v->sql, rel, nl, nr, op_union, rel_projections(v->sql, rel, NULL, 1, 1));
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
			   is_union(r->op) && !need_distinct(r) && !is_single(r) &&
			   is_semi(rel->op) && (je = rel_is_join_on_pkey(rel, true))) {
			/* use first join expression, to find part nr */
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
					rl = rel_project(v->sql->sa, rl,
					rel_projections(v->sql, rl, NULL, 1, 1));
				rel_rename_exps(v->sql, r->exps, rl->exps);
				if (r != or) {
					rl = rel_project(v->sql->sa, rl, NULL);
					rl->exps = exps_copy(v->sql, or->exps);
				}
				nl = rel_crossproduct(v->sql->sa, rel_dup(ol), rl, rel->op);
				nl->exps = exps_copy(v->sql, exps);
				v->changes++;
				return rel_inplace_project(v->sql->sa, rel, nl, rel_projections(v->sql, rel, NULL, 1, 1));
			/* case 2: uses right not left */
			} else if (!rel_uses_part_nr(rl, je, lpnr) &&
				    rel_uses_part_nr(rr, je, lpnr)) {
				sql_rel *nl;

				rr = rel_dup(rr);
				if (!is_project(rr->op))
					rr = rel_project(v->sql->sa, rr,
						rel_projections(v->sql, rr, NULL, 1, 1));
				rel_rename_exps(v->sql, r->exps, rr->exps);
				if (r != or) {
					rr = rel_project(v->sql->sa, rr, NULL);
					rr->exps = exps_copy(v->sql, or->exps);
				}
				nl = rel_crossproduct(v->sql->sa, rel_dup(ol), rr, rel->op);
				nl->exps = exps_copy(v->sql, exps);
				v->changes++;
				return rel_inplace_project(v->sql->sa, rel, nl, rel_projections(v->sql, rel, NULL, 1, 1));
			}
		}
	}
	return rel;
}

static inline sql_rel *
rel_push_join_down_outer(visitor *v, sql_rel *rel)
{
	if (is_join(rel->op) && !is_outerjoin(rel->op) && !is_single(rel) && !list_empty(rel->exps) && !rel_is_ref(rel)) {
		sql_rel *l = rel->l, *r = rel->r;

		if (is_left(r->op) && (is_select(l->op) || (is_join(l->op) && !is_outerjoin(l->op))) && !rel_is_ref(l) &&
				!rel_is_ref(r)) {
			sql_rel *rl = r->l;
			sql_rel *rr = r->r;
			if (rel_is_ref(rl) || rel_is_ref(rr))
				return rel;
			/* join exps should only include l and r.l */
			list *njexps = sa_list(v->sql->sa);
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *je = n->data;

				assert(je->type == e_cmp);
				if (je->f)
					return rel;
				if ((rel_find_exp(l, je->l) && rel_find_exp(rl, je->r)) || (rel_find_exp(l, je->r) && rel_find_exp(rl, je->l))) {
					list_append(njexps, je);
				} else {
					return rel;
				}
			}
			sql_rel *nl = rel_crossproduct(v->sql->sa, rel_dup(l), rl, rel->op);
			r->l = nl;
			nl->exps = njexps;
			rel_dup(r);
			rel_destroy(rel);
			rel = r;
			v->changes++;
		}
	}
	return rel;
}

#define NO_PROJECTION_FOUND 0
#define MAY_HAVE_DUPLICATE_NULLS 1
#define ALL_VALUES_DISTINCT 2

static int
find_projection_for_join2semi(sql_rel *rel)
{
	if (is_simple_project(rel->op) || is_groupby(rel->op) || is_inter(rel->op) || is_except(rel->op) || is_base(rel->op) || (is_union(rel->op) && need_distinct(rel))) {
		if (rel->card < CARD_AGGR) /* const or groupby without group by exps */
			return ALL_VALUES_DISTINCT;
		if (list_length(rel->exps) == 1) {
			sql_exp *e = rel->exps->h->data;
			/* a single group by column in the projection list from a group by relation is guaranteed to be unique, but not an aggregate */
			if (e->type == e_column) {
				sql_rel *res = NULL;
				sql_exp *found = NULL;
				bool underjoin = false;

				/* if just one groupby column is projected or the relation needs distinct values and one column is projected or is a primary key, it will be distinct */
				if ((is_groupby(rel->op) && list_length(rel->r) == 1 && exps_find_exp(rel->r, e)) || (need_distinct(rel) && list_length(rel->exps) == 1))
					return ALL_VALUES_DISTINCT;
				if (is_unique(e))
					return has_nil(e) ? MAY_HAVE_DUPLICATE_NULLS : ALL_VALUES_DISTINCT;

				if ((is_simple_project(rel->op) || is_groupby(rel->op) || is_inter(rel->op) || is_except(rel->op)) &&
					(found = rel_find_exp_and_corresponding_rel(rel->l, e, false, &res, &underjoin)) && !underjoin) { /* grouping column on inner relation */
					if (need_distinct(res) && list_length(res->exps) == 1)
						return ALL_VALUES_DISTINCT;
					if (is_unique(found))
						return has_nil(e) ? MAY_HAVE_DUPLICATE_NULLS : ALL_VALUES_DISTINCT;
					if (found->type == e_column && found->card <= CARD_AGGR) {
						if (!is_groupby(res->op) && list_length(res->exps) != 1)
							return NO_PROJECTION_FOUND;
						for (node *n = res->exps->h ; n ; n = n->next) { /* must be the single column in the group by expression list */
							sql_exp *e = n->data;
							if (e != found && e->type == e_column)
								return NO_PROJECTION_FOUND;
						}
						return ALL_VALUES_DISTINCT;
					}
				}
			}
		}
	}
	return NO_PROJECTION_FOUND;
}

static sql_rel *
find_candidate_join2semi(visitor *v, sql_rel *rel, bool *swap)
{
	/* generalize possibility : we need the visitor 'step' here */
	if (rel_is_ref(rel)) /* if the join has multiple references, it's dangerous to convert it into a semijoin */
		return NULL;
	if (rel->op == op_join && !list_empty(rel->exps)) {
		sql_rel *l = rel->l, *r = rel->r;
		int foundr = NO_PROJECTION_FOUND, foundl = NO_PROJECTION_FOUND, found = NO_PROJECTION_FOUND;
		bool ok = false;

		foundr = find_projection_for_join2semi(r);
		if (foundr < ALL_VALUES_DISTINCT)
			foundl = find_projection_for_join2semi(l);
		if (foundr && foundr > foundl) {
			*swap = false;
			found = foundr;
		} else if (foundl) {
			*swap = true;
			found = foundl;
		}

		if (found > NO_PROJECTION_FOUND) {
			/* if all join expressions can be pushed down or have function calls, then it cannot be rewritten into a semijoin */
			for (node *n=rel->exps->h; n && !ok; n = n->next) {
				sql_exp *e = n->data;

				ok |= e->type == e_cmp && e->flag == cmp_equal && !exp_has_func(e) && !rel_rebind_exp(v->sql, l, e) && !rel_rebind_exp(v->sql, r, e) &&
					(found == ALL_VALUES_DISTINCT || !is_semantics(e) || !has_nil((sql_exp *)e->l) || !has_nil((sql_exp *)e->r));
			}
		}

		if (ok)
			return rel;
	}
	if (is_join(rel->op) || is_semi(rel->op)) {
		sql_rel *c;

		if ((c=find_candidate_join2semi(v, rel->l, swap)) != NULL ||
		    (c=find_candidate_join2semi(v, rel->r, swap)) != NULL)
			return c;
	}
	if (is_topn(rel->op) || is_sample(rel->op))
		return find_candidate_join2semi(v, rel->l, swap);
	return NULL;
}

static int
subrel_uses_exp_outside_subrel(sql_rel *rel, list *l, sql_rel *c)
{
	if (rel == c)
		return 0;
	/* for subrel only expect joins (later possibly selects) */
	if (is_join(rel->op) || is_semi(rel->op)) {
		if (exps_uses_any(rel->exps, l))
			return 1;
		if (subrel_uses_exp_outside_subrel(rel->l, l, c) ||
		    subrel_uses_exp_outside_subrel(rel->r, l, c))
			return 1;
	}
	if (is_topn(rel->op) || is_sample(rel->op))
		return subrel_uses_exp_outside_subrel(rel->l, l, c);
	return 0;
}

static int
rel_uses_exp_outside_subrel(sql_rel *rel, list *l, sql_rel *c)
{
	/* for now we only expect sub relations of type project, selects (rel) or join/semi */
	if (is_simple_project(rel->op) || is_groupby(rel->op) || is_select(rel->op)) {
		if (!list_empty(rel->exps) && exps_uses_any(rel->exps, l))
			return 1;
		if ((is_simple_project(rel->op) || is_groupby(rel->op)) && !list_empty(rel->r) && exps_uses_any(rel->r, l))
			return 1;
		if (rel->l)
			return subrel_uses_exp_outside_subrel(rel->l, l, c);
	}
	if (is_topn(rel->op) || is_sample(rel->op))
		return subrel_uses_exp_outside_subrel(rel->l, l, c);
	return 1;
}

static inline sql_rel *
rel_join2semijoin(visitor *v, sql_rel *rel)
{
	if ((is_simple_project(rel->op) || is_groupby(rel->op)) && rel->l) {
		bool swap = false;
		sql_rel *l = rel->l;
		sql_rel *c = find_candidate_join2semi(v, l, &swap);

		if (c) {
			/* 'p' is a project */
			sql_rel *p = swap ? c->l : c->r;

			/* now we need to check if ce is only used at the level of c */
			if (!rel_uses_exp_outside_subrel(rel, p->exps, c)) {
				c->op = op_semi;
				if (swap) {
					sql_rel *tmp = c->r;
					c->r = c->l;
					c->l = tmp;
				}
				v->changes++;
			}
		}
	}
	return rel;
}

static int
exp_is_rename(sql_exp *e)
{
	return (e->type == e_column);
}

static int
exp_is_useless_rename(sql_exp *e)
{
	return (e->type == e_column &&
			((!e->l && !exp_relname(e)) ||
			 (e->l && exp_relname(e) && strcmp(e->l, exp_relname(e)) == 0)) &&
			strcmp(e->r, exp_name(e)) == 0);
}

static list *
rel_used_projections(mvc *sql, list *exps, list *users)
{
	list *nexps = sa_list(sql->sa);
	bool *used = SA_ZNEW_ARRAY(sql->ta, bool, list_length(exps));
	int i = 0;

	for(node *n = users->h; n; n = n->next) {
		sql_exp *e = n->data, *ne = NULL;
		if ((e->l && (ne = exps_bind_column2(exps, e->l, e->r, NULL))) || (ne = exps_bind_column(exps, e->r, NULL, NULL, 1))) {
			used[list_position(exps, ne)] = 1;
		}
	}
	for(node *n = exps->h; n; n = n->next, i++) {
		sql_exp *e = n->data;
		if (is_intern(e) || used[i])
			append(nexps, e);
	}
	return nexps;
}

/* move projects down with the goal op removing them completely (ie push renames/reduced lists into basetable)
 * for some cases we can directly remove iff renames rename into same alias
 * */
static sql_rel *
rel_push_project_down(visitor *v, sql_rel *rel)
{
	/* for now only push down renames */
	if (v->depth > 1 && is_simple_project(rel->op) && !need_distinct(rel) && !rel_is_ref(rel) && rel->l && !rel->r &&
			v->parent &&
			!is_modify(v->parent->op) && !is_topn(v->parent->op) && !is_sample(v->parent->op) &&
			!is_ddl(v->parent->op) && !is_set(v->parent->op) &&
			list_check_prop_all(rel->exps, (prop_check_func)&exp_is_rename)) {
		sql_rel *l = rel->l;

		if (rel_is_ref(l))
			return rel;
		if (is_basetable(l->op)) {
			if (list_check_prop_all(rel->exps, (prop_check_func)&exp_is_useless_rename)) {
				/* TODO reduce list (those in the project + internal) */
				rel->l = NULL;
				l->exps = rel_used_projections(v->sql, l->exps, rel->exps);
				rel_destroy(rel);
				v->changes++;
				return l;
			}
			return rel;
		} else if (list_check_prop_all(rel->exps, (prop_check_func)&exp_is_useless_rename)) {
			if ((is_project(l->op) && list_length(l->exps) == list_length(rel->exps)) ||
				((v->parent && is_project(v->parent->op)) &&
				 (is_set(l->op) || is_select(l->op) || is_join(l->op) || is_semi(l->op) || is_topn(l->op) || is_sample(l->op)))) {
				rel->l = NULL;
				rel_destroy(rel);
				v->changes++;
				return l;
			}
		}
	}
	return rel;
}

static inline sql_rel *
rel_push_project_down_union(visitor *v, sql_rel *rel)
{
	/* first remove distinct if already unique */
	if (rel->op == op_project && need_distinct(rel) && rel->exps && exps_unique(v->sql, rel, rel->exps) && !have_nil(rel->exps)) {
		set_nodistinct(rel);
		if (exps_card(rel->exps) <= CARD_ATOM && rel->card > CARD_ATOM) /* if the projection just contains constants, then no topN is needed */
			rel->l = rel_topn(v->sql->sa, rel->l, append(sa_list(v->sql->sa), exp_atom_lng(v->sql->sa, 1)));
		v->changes++;
	}

	if (rel->op == op_project && rel->l && rel->exps && list_empty(rel->r)) {
		int need_distinct = need_distinct(rel);
		sql_rel *u = rel->l;
		sql_rel *p = rel;
		sql_rel *ul = u->l;
		sql_rel *ur = u->r;

		if (!u || !is_union(u->op) || need_distinct(u) || !u->exps || rel_is_ref(u) || project_unsafe(rel,0))
			return rel;
		/* don't push project down union of single values */
		if ((is_project(ul->op) && !ul->l) || (is_project(ur->op) && !ur->l))
			return rel;

		ul = rel_dup(ul);
		ur = rel_dup(ur);

		if (!is_project(ul->op))
			ul = rel_project(v->sql->sa, ul,
				rel_projections(v->sql, ul, NULL, 1, 1));
		if (!is_project(ur->op))
			ur = rel_project(v->sql->sa, ur,
				rel_projections(v->sql, ur, NULL, 1, 1));
		need_distinct = (need_distinct &&
				(!exps_unique(v->sql, ul, ul->exps) || have_nil(ul->exps) ||
				 !exps_unique(v->sql, ur, ur->exps) || have_nil(ur->exps)));
		rel_rename_exps(v->sql, u->exps, ul->exps);
		rel_rename_exps(v->sql, u->exps, ur->exps);

		/* introduce projects under the set */
		ul = rel_project(v->sql->sa, ul, NULL);
		if (need_distinct)
			set_distinct(ul);
		ur = rel_project(v->sql->sa, ur, NULL);
		if (need_distinct)
			set_distinct(ur);

		ul->exps = exps_copy(v->sql, p->exps);
		ur->exps = exps_copy(v->sql, p->exps);

		rel = rel_inplace_setop(v->sql, rel, ul, ur, op_union,
			rel_projections(v->sql, rel, NULL, 1, 1));
		if (need_distinct)
			set_distinct(rel);
		if (is_single(u))
			set_single(rel);
		v->changes++;
		rel->l = rel_merge_projects(v, rel->l);
		rel->r = rel_merge_projects(v, rel->r);
		return rel;
	}
	return rel;
}

static int
sql_class_base_score(visitor *v, sql_column *c, sql_subtype *t, bool equality_based)
{
	int de;

	if (!t)
		return 0;
	switch (ATOMstorage(t->type->localtype)) {
		case TYPE_bte:
			return 150 - 8;
		case TYPE_sht:
			return 150 - 16;
		case TYPE_int:
			return 150 - 32;
		case TYPE_void:
		case TYPE_lng:
			return 150 - 64;
		case TYPE_uuid:
#ifdef HAVE_HGE
		case TYPE_hge:
#endif
			return 150 - 128;
		case TYPE_flt:
			return 75 - 24;
		case TYPE_dbl:
			return 75 - 53;
		default: {
			if (equality_based && c && v->storage_based_opt && (de = mvc_is_duplicate_eliminated(v->sql, c)))
				return 150 - de * 8;
			/* strings and blobs not duplicate eliminated don't get any points here */
			return 0;
		}
	}
}

/* Compute the efficiency of using this expression earl	y in a group by list */
static int
score_gbe(visitor *v, sql_rel *rel, sql_exp *e)
{
	int res = 0;
	sql_subtype *t = exp_subtype(e);
	sql_column *c = exp_find_column(rel, e, -2);

	if (e->card == CARD_ATOM) /* constants are trivial to group */
		res += 1000;
	/* can we find out if the underlying table is sorted */
	if (is_unique(e) || find_prop(e->p, PROP_HASHCOL) || (c && v->storage_based_opt && mvc_is_unique(v->sql, c))) /* distinct columns */
		res += 700;
	if (c && v->storage_based_opt && mvc_is_sorted(v->sql, c))
		res += 500;
	if (find_prop(e->p, PROP_HASHIDX)) /* has hash index */
		res += 200;

	/* prefer the shorter var types over the longer ones */
	res += sql_class_base_score(v, c, t, true); /* smaller the type, better */
	return res;
}

/* reorder group by expressions */
static inline sql_rel *
rel_groupby_order(visitor *v, sql_rel *rel)
{
	int *scores = NULL;
	sql_exp **exps = NULL;

	if (is_groupby(rel->op) && list_length(rel->r) > 1) {
		node *n;
		list *gbe = rel->r;
		int i, ngbe = list_length(gbe);
		scores = SA_NEW_ARRAY(v->sql->ta, int, ngbe);
		exps = SA_NEW_ARRAY(v->sql->ta, sql_exp*, ngbe);

		/* first sorting step, give priority for integers and sorted columns */
		for (i = 0, n = gbe->h; n; i++, n = n->next) {
			exps[i] = n->data;
			scores[i] = score_gbe(v, rel, exps[i]);
		}
		GDKqsort(scores, exps, NULL, ngbe, sizeof(int), sizeof(void *), TYPE_int, true, true);

		/* second sorting step, give priority to strings with lower number of digits */
		for (i = ngbe - 1; i && !scores[i]; i--); /* find expressions with no score from the first round */
		if (scores[i])
			i++;
		if (ngbe - i > 1) {
			for (int j = i; j < ngbe; j++) {
				sql_subtype *t = exp_subtype(exps[j]);
				scores[j] = t ? t->digits : 0;
			}
			/* the less number of digits the better, order ascending */
			GDKqsort(scores + i, exps + i, NULL, ngbe - i, sizeof(int), sizeof(void *), TYPE_int, false, true);
		}

		for (i = 0, n = gbe->h; n; i++, n = n->next)
			n->data = exps[i];
	}

	return rel;
}

/* reduce group by expressions based on pkey info
 *
 * The reduced group by and (derived) aggr expressions are restored via
 * extra (new) aggregate columns.
 */
static inline sql_rel *
rel_reduce_groupby_exps(visitor *v, sql_rel *rel)
{
	list *gbe = rel->r;

	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel) && list_length(gbe)) {
		node *n, *m;
		int k, j, i, ngbe = list_length(gbe);
		int8_t *scores = SA_NEW_ARRAY(v->sql->ta, int8_t, ngbe);
		sql_column *c;
		sql_table **tbls = SA_NEW_ARRAY(v->sql->ta, sql_table*, ngbe);
		sql_rel **bts = SA_NEW_ARRAY(v->sql->ta, sql_rel*, ngbe), *bt = NULL;

		gbe = rel->r;
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
							e->p = prop_create(v->sql->sa, PROP_HASHCOL, e->p);
					}
					for (m = rel->exps->h; m; m = m->next ){
						sql_exp *e = m->data;

						for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
							sql_exp *gb = n->data;

							/* pkey based group by */
							if (scores[l] == 1 && exp_match_exp(e,gb) && find_prop(gb->p, PROP_HASHCOL) && !find_prop(e->p, PROP_HASHCOL)) {
								e->p = prop_create(v->sql->sa, PROP_HASHCOL, e->p);
								break;
							}

						}
					}
				}
				if (cnr && nr && list_length(tbls[j]->pkey->k.columns) == nr) {
					list *ngbe = new_exp_list(v->sql->sa);

					for (l = 0, n = gbe->h; l < k && n; l++, n = n->next) {
						sql_exp *e = n->data;

						/* keep the group by columns which form a primary key
						 * of this table. And those unrelated to this table. */
						if (scores[l] != -1)
							append(ngbe, e);
					}
					rel->r = ngbe;
					/* rewrite gbe and aggr, in the aggr list */
					for (m = rel->exps->h; m; m = m->next ){
						sql_exp *e = m->data;
						int fnd = 0;

						for (l = 0, n = gbe->h; l < k && n && !fnd; l++, n = n->next) {
							sql_exp *gb = n->data;

							if (scores[l] == -1 && exp_refers(gb, e)) {
								sql_exp *rs = exp_column(v->sql->sa, gb->l?gb->l:exp_relname(gb), gb->r?gb->r:exp_name(gb), exp_subtype(gb), rel->card, has_nil(gb), is_unique(gb), is_intern(gb));
								exp_setname(v->sql->sa, rs, exp_find_rel_name(e), exp_name(e));
								e = rs;
								fnd = 1;
							}
						}
						m->data = e;
					}
					/* new reduced aggr expression list */
					assert(list_length(rel->exps)>0);
					/* only one reduction at a time */
					list_hash_clear(rel->exps);
					v->changes++;
					return rel;
				}
				gbe = rel->r;
			}
		}
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
			list *ngbe = new_exp_list(v->sql->sa);
			list *dgbe = new_exp_list(v->sql->sa);

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

				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *ne = NULL;

					if (e->type == e_column) {
						if (e->l)
							ne = exps_bind_column2(dgbe, e->l, e->r, NULL);
						else
							ne = exps_bind_column(dgbe, e->r, NULL, NULL, 1);
						if (ne) {
							ne = exp_copy(v->sql, ne);
							exp_prop_alias(v->sql->sa, ne, e);
							e = ne;
						}
					}
					n->data = e;
				}
				list_hash_clear(rel->exps);
				v->changes++;
			}
		}
	}
	return rel;
}

#if 0
static sql_rel *
rel_groupby_distinct2(visitor *v, sql_rel *rel)
{
	list *ngbes = sa_list(v->sql->sa), *gbes, *naggrs = sa_list(v->sql->sa), *aggrs = sa_list(v->sql->sa);
	sql_rel *l;
	node *n;

	gbes = rel->r;
	if (!gbes)
		return rel;

	/* check if each aggr is, rewritable (max,min,sum,count)
	 *  			  and only has one argument */
	for (n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_subfunc *af = e->f;

		if (e->type == e_aggr &&
		   (strcmp(af->func->base.name, "sum") &&
		     strcmp(af->func->base.name, "count") &&
		     strcmp(af->func->base.name, "min") &&
		     strcmp(af->func->base.name, "max")))
			return rel;
	}

	for (n = gbes->h; n; n = n->next) {
		sql_exp *e = n->data;

		e = exp_column(v->sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
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
				exp_label(v->sql->sa, v, ++v->sql->label);
			v = exp_column(v->sql->sa, exp_find_rel_name(v), exp_name(v), exp_subtype(v), v->card, has_nil(v), is_unique(v), is_intern(v));
			append(aggrs, v);
			v = exp_aggr1(v->sql->sa, v, e->f, need_distinct(e), 1, e->card, 1);
			exp_setname(v->sql->sa, v, exp_find_rel_name(e), exp_name(e));
			append(naggrs, v);
		} else if (e->type == e_aggr && !need_distinct(e)) {
			sql_exp *v;
			sql_subfunc *f = e->f;
			int cnt = exp_aggr_is_count(e);
			sql_subfunc *a = sql_bind_func(v->sql, "sys", (cnt)?"sum":f->func->base.name, exp_subtype(e), NULL, F_AGGR);

			append(aggrs, e);
			if (!exp_name(e))
				exp_label(v->sql->sa, e, ++v->sql->label);
			set_has_nil(e);
			v = exp_column(v->sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
			v = exp_aggr1(v->sql->sa, v, a, 0, 1, e->card, 1);
			if (cnt)
				set_zero_if_empty(v);
			exp_setname(v->sql->sa, v, exp_find_rel_name(e), exp_name(e));
			append(naggrs, v);
		} else { /* group by col */
			if (list_find_exp(gbes, e) || !list_find_exp(naggrs, e)) {
				append(aggrs, e);

				e = exp_column(v->sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
			}
			append(naggrs, e);
		}
	}

	l = rel->l = rel_groupby(v->sql, rel->l, gbes);
	l->exps = aggrs;
	rel->r = ngbes;
	rel->exps = naggrs;
	v->changes++;
	return rel;
}
#endif

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
static inline sql_rel *
rel_groupby_distinct(visitor *v, sql_rel *rel)
{
	node *n;

	if (is_groupby(rel->op)) {
		sql_rel *l = rel->l;
		if (!l || is_groupby(l->op))
			return rel;
	}
	if (is_groupby(rel->op) && rel->r && !rel_is_ref(rel)) {
		int nr = 0, anr = 0;
		list *gbe, *ngbe, *arg, *exps, *nexps;
		sql_exp *distinct = NULL, *darg, *found;
		sql_rel *l = NULL;

		for (n=rel->exps->h; n && nr <= 2; n = n->next) {
			sql_exp *e = n->data;
			if (need_distinct(e)) {
				distinct = n->data;
				nr++;
			}
			anr += is_aggr(e->type);
		}
		if (nr < 1 || distinct->type != e_aggr)
			return rel;
		if (nr > 1 || anr > nr)
			return rel;//rel_groupby_distinct2(v, rel);
		arg = distinct->l;
		if (list_length(arg) != 1 || list_length(rel->r) + nr != list_length(rel->exps))
			return rel;

		gbe = rel->r;
		ngbe = sa_list(v->sql->sa);
		exps = sa_list(v->sql->sa);
		nexps = sa_list(v->sql->sa);
		for (n=rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			if (e != distinct) {
				if (e->type == e_aggr) { /* copy the arguments to the aggregate */
					list *args = e->l;
					if (args) {
						for (node *n = args->h ; n ; n = n->next) {
							sql_exp *e = n->data;
							list_append(ngbe, exp_copy(v->sql, e));
							list_append(exps, exp_copy(v->sql, e));
						}
					}
				} else {
					e = exp_ref(v->sql, e);
					append(ngbe, e);
					append(exps, e);
				}
				if (e->type == e_aggr) /* aggregates must be copied */
					e = exp_copy(v->sql, e);
				else
					e = exp_ref(v->sql, e);
				append(nexps, e);
			}
		}

		darg = arg->h->data;
		if ((found = exps_find_exp(gbe, darg))) { /* first find if the aggregate argument already exists in the grouping list */
			darg = exp_ref(v->sql, found);
		} else {
			list_append(gbe, darg = exp_copy(v->sql, darg));
			exp_label(v->sql->sa, darg, ++v->sql->label);
			darg = exp_ref(v->sql, darg);
		}
		list_append(exps, darg);
		darg = exp_ref(v->sql, darg);
		arg->h->data = darg;
		l = rel->l = rel_groupby(v->sql, rel->l, gbe);
		l->exps = exps;
		set_processed(l);
		rel->r = ngbe;
		rel->exps = nexps;
		set_nodistinct(distinct);
		append(nexps, distinct);
		v->changes++;
	}
	return rel;
}

static sql_exp *split_aggr_and_project(mvc *sql, list *aexps, sql_exp *e);

static void
list_split_aggr_and_project(mvc *sql, list *aexps, list *exps)
{
	if (list_empty(exps))
		return ;
	for(node *n = exps->h; n; n = n->next)
		n->data = split_aggr_and_project(sql, aexps, n->data);
}

static sql_exp *
split_aggr_and_project(mvc *sql, list *aexps, sql_exp *e)
{
	switch(e->type) {
	case e_aggr:
		/* add to the aggrs */
		if (!exp_name(e))
			exp_label(sql->sa, e, ++sql->label);
		list_append(aexps, e);
		return exp_ref(sql, e);
	case e_cmp:
		/* e_cmp's shouldn't exist in an aggr expression list */
		assert(0);
	case e_convert:
		e->l = split_aggr_and_project(sql, aexps, e->l);
		return e;
	case e_func:
		list_split_aggr_and_project(sql, aexps, e->l);
		return e;
	case e_atom:
	case e_column: /* constants and columns shouldn't be rewriten */
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
	if (list_empty(exps))
		return exps;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *arg = n->data, *narg = NULL;

		narg = exp_use_consts(sql, arg, consts);
		if (!narg)
			return NULL;
		narg = exp_propagate(sql->sa, narg, arg);
		n->data = narg;
	}
	return exps;
}

static sql_exp *
exp_use_consts(mvc *sql, sql_exp *e, list *consts)
{
	sql_exp *ne = NULL, *l, *r, *r2;

	switch(e->type) {
	case e_column:
		if (e->l)
			ne = exps_bind_column2(consts, e->l, e->r, NULL);
		if (!ne && !e->l)
			ne = exps_bind_column(consts, e->r, NULL, NULL, 1);
		if (!ne)
			return e;
		return ne;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			list *l = exps_use_consts(sql, e->l, consts);
			list *r = exps_use_consts(sql, e->r, consts);

			if (!l || !r)
				return NULL;
			if (e->flag == cmp_filter)
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
					ne = exp_compare2(sql->sa, l, r, r2, e->flag, is_symmetric(e));
			} else if (l && r) {
				ne = exp_compare(sql->sa, l, r, e->flag);
			}
		}
		if (!ne)
			return NULL;
		return exp_propagate(sql->sa, ne, e);
	case e_convert:
		l = exp_use_consts(sql, e->l, consts);
		if (l)
			return exp_convert(sql->sa, l, exp_fromtype(e), exp_totype(e));
		return NULL;
	case e_aggr:
	case e_func: {
		list *l = e->l, *nl = NULL;

		if (!list_empty(l)) {
			nl = exps_use_consts(sql, l, consts);
			if (!nl)
				return NULL;
		}
		if (e->type == e_func)
			return exp_op(sql->sa, nl, e->f);
		else
			return exp_aggr(sql->sa, nl, e->f, need_distinct(e), need_no_nil(e), e->card, has_nil(e));
	}
	case e_atom: {
		list *l = e->f, *nl = NULL;

		if (!list_empty(l)) {
			nl = exps_use_consts(sql, l, consts);
			if (!nl)
				return NULL;
			return exp_values(sql->sa, nl);
		} else {
			return exp_copy(sql, e);
		}
	}
	case e_psm:
		return e;
	}
	return NULL;
}

static list *
exps_remove_dictexps(list *exps, sql_rel *r)
{
	if (list_empty(exps))
		return exps;
	for (node *n = exps->h; n;) {
		node *next = n->next;
		sql_exp *arg = n->data;

		if (list_find_exp(r->exps, arg->l) || list_find_exp(r->exps, arg->r))
			list_remove_node(exps, NULL, n);
		n = next;
	}
	return exps;
}

static sql_rel *
rel_remove_join(visitor *v, sql_rel *rel)
{
	if (is_join(rel->op) && !is_outerjoin(rel->op)) {
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
			v->changes++;
			/* use constant (instead of alias) in expressions */
			if (lconst) {
				sql_rel *s = l;
				l = r;
				r = s;
			}
			rel->exps = exps_use_consts(v->sql, rel->exps, r->exps);
			/* change into select */
			rel->op = op_select;
			rel->l = l;
			rel->r = NULL;
			/* wrap in a project including, the constant columns */
			rel = rel_project(v->sql->sa, rel, rel_projections(v->sql, l, NULL, 1, 1));
			list_merge(rel->exps, r->exps, (fdup)NULL);
		}
	}
	if (is_join(rel->op)) {
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
		v->changes++;

		assert(0);
		if (ldict) {
			sql_rel *s = l;
			l = r;
			r = s;
		}
		rel->exps = exps_remove_dictexps(rel->exps, r);
		/* change into select */
		rel->op = op_select;
		rel->l = l;
		rel->r = NULL;
		/* wrap in a project including, the dict/index columns */
		rel = rel_project(v->sql->sa, rel, rel_projections(v->sql, l, NULL, 1, 1));
		list_merge(rel->exps, r->exps, (fdup)NULL);
	}
	/* project (join (A,B)[ A.x = B.y ] ) [project_cols] -> project (A) [project_cols]
	 * where non of the project_cols are from B and x=y is a foreign key join (B is the unique side)
	 * and there are no filters on B
	 */
	if (is_project(rel->op)) {
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
			v->changes++;
			rel->l = l;
			rel->r = NULL;
		}
	}
	return rel;
}

/* Pushing projects up the tree. Done very early in the optimizer.
 * Makes later steps easier.
 */
static sql_rel *
rel_push_project_up(visitor *v, sql_rel *rel)
{
	if (is_simple_project(rel->op) && rel->l && !rel_is_ref(rel)) {
		sql_rel *l = rel->l;
		if (is_simple_project(l->op))
			return rel_merge_projects(v, rel);
	}

	/* project/project cleanup is done later */
	if (is_join(rel->op) || is_select(rel->op)) {
		node *n;
		list *exps = NULL, *l_exps, *r_exps;
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;
		sql_rel *t;
		int nlexps = 0, i = 0;

		/* Don't rewrite refs, non projections or constant or
		   order by projections  */
		if (!l || rel_is_ref(l) || is_topn(l->op) || is_sample(l->op) ||
		   (is_join(rel->op) && (!r || rel_is_ref(r))) ||
		   (is_left(rel->op) && (rel->flag&MERGE_LEFT) /* can't push projections above merge statments left joins */) ||
		   (is_select(rel->op) && l->op != op_project) ||
		   (is_join(rel->op) && ((l->op != op_project && r->op != op_project) || is_topn(r->op) || is_sample(r->op))) ||
		  ((l->op == op_project && (!l->l || l->r || project_unsafe(l,is_select(rel->op)))) ||
		   (is_join(rel->op) && (r->op == op_project && (!r->l || r->r || project_unsafe(r,0))))))
			return rel;

		if (l->op == op_project && l->l) {
			/* Go through the list of project expressions.
			   Check if they can be pushed up, ie are they not
			   changing or introducing any columns used
			   by the upper operator. */

			exps = new_exp_list(v->sql->sa);
			for (n = l->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				/* we cannot rewrite projection with atomic values from outer joins */
				if (is_column(e->type) && exp_is_atom(e) && !(is_right(rel->op) || is_full(rel->op))) {
					list_append(exps, e);
				} else if (e->type == e_column) {
					if (has_label(e))
						return rel;
					list_append(exps, e);
				} else {
					return rel;
				}
			}
		} else {
			exps = rel_projections(v->sql, l, NULL, 1, 1);
		}
		nlexps = list_length(exps);
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
					if (has_label(e))
						return rel;
					list_append(exps, e);
				} else {
					return rel;
				}
			}
		} else if (is_join(rel->op)) {
			list *r_exps = rel_projections(v->sql, r, NULL, 1, 1);
			list_merge(exps, r_exps, (fdup)NULL);
		}
		/* Here we should check for ambigious names ? */
		if (is_join(rel->op) && r) {
			t = (l->op == op_project && l->l)?l->l:l;
			l_exps = rel_projections(v->sql, t, NULL, 1, 1);
			/* conflict with old right expressions */
			r_exps = rel_projections(v->sql, r, NULL, 1, 1);
			for(n = l_exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				const char *rname = exp_relname(e);
				const char *name = exp_name(e);

				if (exp_is_atom(e))
					continue;
				if ((rname && exps_bind_column2(r_exps, rname, name, NULL) != NULL) ||
				    (!rname && exps_bind_column(r_exps, name, NULL, NULL, 1) != NULL))
					return rel;
			}
			t = (r->op == op_project && r->l)?r->l:r;
			r_exps = rel_projections(v->sql, t, NULL, 1, 1);
			/* conflict with new right expressions */
			for(n = l_exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_is_atom(e))
					continue;
				if ((e->l && exps_bind_column2(r_exps, e->l, e->r, NULL) != NULL) ||
				   (exps_bind_column(r_exps, e->r, NULL, NULL, 1) != NULL && (!e->l || !e->r)))
					return rel;
			}
			/* conflict with new left expressions */
			for(n = r_exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_is_atom(e))
					continue;
				if ((e->l && exps_bind_column2(l_exps, e->l, e->r, NULL) != NULL) ||
				   (exps_bind_column(l_exps, e->r, NULL, NULL, 1) != NULL && (!e->l || !e->r)))
					return rel;
			}
		}

		/* rename operator expressions */
		if (l->op == op_project) {
			/* rewrite rel from rel->l into rel->l->l */
			if (rel->exps) {
				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *ne;

					ne = exp_rename(v->sql, e, l, l->l);
					assert(ne);
					if (ne != e && exp_name(e))
						exp_propagate(v->sql->sa, ne, e);
					n->data = ne;
				}
			}
			rel->l = l->l;
			l->l = NULL;
			rel_destroy(l);
		}
		if (is_join(rel->op) && r->op == op_project) {
			/* rewrite rel from rel->r into rel->r->l */
			if (rel->exps) {
				for (n = rel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *ne;

					ne = exp_rename(v->sql, e, r, r->l);
					assert(ne);
					if (ne != e && exp_name(e))
						exp_propagate(v->sql->sa, ne, e);
					n->data = ne;
				}
			}
			rel->r = r->l;
			r->l = NULL;
			rel_destroy(r);
		}
		/* Done, ie introduce new project */
		exps_fix_card(exps, rel->card);
		/* Fix nil flag */
		if (!list_empty(exps)) {
			for (n = exps->h ; n && i < nlexps ; n = n->next, i++) {
				sql_exp *e = n->data;

				if (is_right(rel->op) || is_full(rel->op))
					set_has_nil(e);
				set_not_unique(e);
			}
			for (; n ; n = n->next) {
				sql_exp *e = n->data;

				if (is_left(rel->op) || is_full(rel->op))
					set_has_nil(e);
				set_not_unique(e);
			}
		}
		v->changes++;
		return rel_inplace_project(v->sql->sa, rel, NULL, exps);
	}
	if (is_groupby(rel->op) && !rel_is_ref(rel) && rel->exps && list_length(rel->exps) > 1) {
		node *n;
		int fnd = 0;
		list *aexps, *pexps;

		/* check if some are expressions aren't e_aggr */
		for (n = rel->exps->h; n && !fnd; n = n->next) {
			sql_exp *e = n->data;

			if (e->type != e_aggr && e->type != e_column && e->type != e_atom) {
				fnd = 1;
			}
		}
		/* only aggr, no rewrite needed */
		if (!fnd)
			return rel;

		aexps = sa_list(v->sql->sa);
		pexps = sa_list(v->sql->sa);
		for (n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data, *ne = NULL;

			switch (e->type) {
			case e_atom: /* move over to the projection */
				list_append(pexps, e);
				break;
			case e_func:
				list_append(pexps, e);
				list_split_aggr_and_project(v->sql, aexps, e->l);
				break;
			case e_convert:
				list_append(pexps, e);
				e->l = split_aggr_and_project(v->sql, aexps, e->l);
				break;
			default: /* simple alias */
				list_append(aexps, e);
				ne = exp_column(v->sql->sa, exp_find_rel_name(e), exp_name(e), exp_subtype(e), e->card, has_nil(e), is_unique(e), is_intern(e));
				list_append(pexps, ne);
				break;
			}
		}
		v->changes++;
		rel->exps = aexps;
		return rel_inplace_project( v->sql->sa, rel, NULL, pexps);
	}
	return rel;
}

/* if local_proj is >= -1, the current expression is from the same projection
   if local_proj is -1, then we don't care about self references (eg used to check for order by exps) */
static int exp_mark_used(sql_rel *subrel, sql_exp *e, int local_proj);

static int
exps_mark_used(sql_rel *subrel, list *l, int local_proj)
{
	int nr = 0;
	if (list_empty(l))
		return nr;

	for (node *n = l->h; n != NULL; n = n->next)
		nr += exp_mark_used(subrel, n->data, local_proj);
	return nr;
}

static int
exp_mark_used(sql_rel *subrel, sql_exp *e, int local_proj)
{
	int nr = 0;
	sql_exp *ne = NULL;

	switch(e->type) {
	case e_column:
		ne = rel_find_exp(subrel, e);
		/* if looking in the same projection, make sure 'ne' is projected before the searched column */
		if (ne && local_proj > -1 && list_position(subrel->exps, ne) >= local_proj)
			ne = NULL;
		break;
	case e_convert:
		return exp_mark_used(subrel, e->l, local_proj);
	case e_aggr:
	case e_func: {
		if (e->l)
			nr += exps_mark_used(subrel, e->l, local_proj);
		assert(!e->r);
		break;
	}
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			nr += exps_mark_used(subrel, e->l, local_proj);
			nr += exps_mark_used(subrel, e->r, local_proj);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			nr += exp_mark_used(subrel, e->l, local_proj);
			nr += exps_mark_used(subrel, e->r, local_proj);
		} else {
			nr += exp_mark_used(subrel, e->l, local_proj);
			nr += exp_mark_used(subrel, e->r, local_proj);
			if (e->f)
				nr += exp_mark_used(subrel, e->f, local_proj);
		}
		break;
	case e_atom:
		/* atoms are used in e_cmp */
		e->used = 1;
		/* return 0 as constants may require a full column ! */
		if (e->f)
			nr += exps_mark_used(subrel, e->f, local_proj);
		return nr;
	case e_psm:
		if (e->flag & PSM_SET || e->flag & PSM_RETURN || e->flag & PSM_EXCEPTION) {
			nr += exp_mark_used(subrel, e->l, local_proj);
		} else if (e->flag & PSM_WHILE || e->flag & PSM_IF) {
			nr += exp_mark_used(subrel, e->l, local_proj);
			nr += exps_mark_used(subrel, e->r, local_proj);
			if (e->flag == PSM_IF && e->f)
				nr += exps_mark_used(subrel, e->f, local_proj);
		}
		e->used = 1;
		break;
	}
	if (ne && e != ne) {
		if (local_proj == -2 || ne->type != e_column || (has_label(ne) || (ne->alias.rname && ne->alias.rname[0] == '%')) || (subrel->l && !rel_find_exp(subrel->l, e)))
			ne->used = 1;
		return ne->used;
	}
	return nr;
}

static void
positional_exps_mark_used( sql_rel *rel, sql_rel *subrel )
{
	assert(rel->exps);

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
rel_exps_mark_used(sql_allocator *sa, sql_rel *rel, sql_rel *subrel)
{
	int nr = 0;

	if (rel->r && (is_simple_project(rel->op) || is_groupby(rel->op))) {
		list *l = rel->r;
		node *n;

		for (n=l->h; n; n = n->next) {
			sql_exp *e = n->data;

			e->used = 1;
			exp_mark_used(rel, e, -1);
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

		if (!nr && is_project(rel->op) && len > 0) /* project at least one column if exists */
			exps[0]->used = 1;

		for (i = len-1; i >= 0; i--) {
			sql_exp *e = exps[i];

			if (!is_project(rel->op) || e->used) {
				if (is_project(rel->op))
					nr += exp_mark_used(rel, e, i);
				nr += exp_mark_used(subrel, e, -2);
			}
		}
	}
	/* for count/rank we need atleast one column */
	if (!nr && subrel && (is_project(subrel->op) || is_base(subrel->op)) && !list_empty(subrel->exps) &&
		(is_simple_project(rel->op) && project_unsafe(rel, 0))) {
		sql_exp *e = subrel->exps->h->data;
		e->used = 1;
	}
	if (rel->r && (is_simple_project(rel->op) || is_groupby(rel->op))) {
		list *l = rel->r;
		node *n;

		for (n=l->h; n; n = n->next) {
			sql_exp *e = n->data;

			e->used = 1;
			/* possibly project/groupby uses columns from the inner */
			exp_mark_used(subrel, e, -2);
		}
	}
}

static void exps_used(list *l);

static void
exp_used(sql_exp *e)
{
	if (e) {
		e->used = 1;

		switch (e->type) {
		case e_convert:
			exp_used(e->l);
			break;
		case e_func:
		case e_aggr:
			exps_used(e->l);
			break;
		case e_cmp:
			if (e->flag == cmp_or || e->flag == cmp_filter) {
				exps_used(e->l);
				exps_used(e->r);
			} else if (e->flag == cmp_in || e->flag == cmp_notin) {
				exp_used(e->l);
				exps_used(e->r);
			} else {
				exp_used(e->l);
				exp_used(e->r);
				if (e->f)
					exp_used(e->f);
			}
			break;
		default:
			break;
		}
	}
}

static void
exps_used(list *l)
{
	if (l) {
		for (node *n = l->h; n; n = n->next)
			exp_used(n->data);
	}
}

static void
rel_used(sql_rel *rel)
{
	if (!rel)
		return;
	if (is_join(rel->op) || is_set(rel->op) || is_semi(rel->op) || is_modify(rel->op)) {
		rel_used(rel->l);
		rel_used(rel->r);
	} else if (is_topn(rel->op) || is_select(rel->op) || is_sample(rel->op)) {
		rel_used(rel->l);
		rel = rel->l;
	} else if (is_ddl(rel->op)) {
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			rel_used(rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel_used(rel->l);
			rel_used(rel->r);
		} else if (rel->flag == ddl_psm) {
			exps_used(rel->exps);
		}
	} else if (rel->op == op_table) {
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			rel_used(rel->l);
		exp_used(rel->r);
	}
	if (rel && rel->exps) {
		exps_used(rel->exps);
		if (rel->r && (is_simple_project(rel->op) || is_groupby(rel->op)))
			exps_used(rel->r);
	}
}

static void
rel_mark_used(mvc *sql, sql_rel *rel, int proj)
{
	if (proj && (need_distinct(rel)))
		rel_used(rel);

	switch(rel->op) {
	case op_basetable:
	case op_truncate:
	case op_insert:
		break;

	case op_table:

		if (rel->l && rel->flag != TRIGGER_WRAPPER) {
			rel_used(rel);
			if (rel->r)
				exp_mark_used(rel->l, rel->r, -2);
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
			rel_exps_mark_used(sql->sa, rel, rel->l);
			rel_mark_used(sql, rel->l, 0);
		} else if (proj) {
			rel_exps_mark_used(sql->sa, rel, NULL);
		}
		break;
	case op_update:
	case op_delete:
		if (proj && rel->r) {
			sql_rel *r = rel->r;

			if (!list_empty(r->exps)) {
				for (node *n = r->exps->h; n; n = n->next) {
					sql_exp *e = n->data;
					const char *nname = exp_name(e);

					if (nname[0] == '%' && strcmp(nname, TID) == 0) { /* TID is used */
						e->used = 1;
						break;
					}
				}
			}
			rel_exps_mark_used(sql->sa, rel, rel->r);
			rel_mark_used(sql, rel->r, 0);
		}
		break;

	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_mark_used(sql, rel->l, 0);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_mark_used(sql, rel->l, 0);
			if (rel->r)
				rel_mark_used(sql, rel->r, 0);
		}
		break;

	case op_select:
		if (rel->l) {
			rel_exps_mark_used(sql->sa, rel, rel->l);
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
			rel_exps_mark_used(sql->sa, rel, l);
			rel_mark_used(sql, rel->l, 0);
			/* based on child check set expression list */
			if (is_project(l->op) && need_distinct(l))
				positional_exps_mark_used(l, rel);
			positional_exps_mark_used(rel, rel->r);
			rel_exps_mark_used(sql->sa, rel, rel->r);
			rel_mark_used(sql, rel->r, 0);
		}
		break;

	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_merge:
		rel_exps_mark_used(sql->sa, rel, rel->l);
		rel_exps_mark_used(sql->sa, rel, rel->r);
		rel_mark_used(sql, rel->l, 0);
		rel_mark_used(sql, rel->r, 0);
		break;
	}
}

static sql_rel * rel_dce_sub(mvc *sql, sql_rel *rel);

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
		if (rel->exps && (rel->op != op_table || !IS_TABLE_PROD_FUNC(rel->flag))) {
			for(node *n=rel->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!e->used)
					needed = 1;
			}

			if (!needed)
				return rel;

			for(node *n=rel->exps->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data;

				/* atleast one (needed for crossproducts, count(*), rank() and single value projections) !, handled by rel_exps_mark_used */
				if (!e->used && list_length(rel->exps) > 1)
					list_remove_node(rel->exps, NULL, n);
				n = next;
			}
		}
		if (rel->op == op_table && (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION))
			rel->l = rel_remove_unused(sql, rel->l);
		return rel;

	case op_topn:
	case op_sample:

		if (rel->l)
			rel->l = rel_remove_unused(sql, rel->l);
		return rel;

	case op_project:
	case op_groupby:

		if (/*rel->l &&*/ rel->exps) {
			for(node *n=rel->exps->h; n && !needed; n = n->next) {
				sql_exp *e = n->data;

				if (!e->used)
					needed = 1;
			}
			if (!needed)
				return rel;

			for(node *n=rel->exps->h; n;) {
				node *next = n->next;
				sql_exp *e = n->data;

				/* atleast one (needed for crossproducts, count(*), rank() and single value projections) */
				if (!e->used && list_length(rel->exps) > 1)
					list_remove_node(rel->exps, NULL, n);
				n = next;
			}
		}
		return rel;

	case op_union:
	case op_inter:
	case op_except:

	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
	case op_merge:

	case op_select:

	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
		return rel;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel->l = rel_remove_unused(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel->l = rel_remove_unused(sql, rel->l);
			if (rel->r)
				rel->r = rel_remove_unused(sql, rel->r);
		}
		return rel;
	}
	return rel;
}

static void
rel_dce_refs(mvc *sql, sql_rel *rel, list *refs)
{
	if (!rel || (rel_is_ref(rel) && list_find(refs, rel, NULL)))
		return ;

	switch(rel->op) {
	case op_table:
	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:
	case op_select:

		if (rel->l && (rel->op != op_table || rel->flag != TRIGGER_WRAPPER))
			rel_dce_refs(sql, rel->l, refs);
		break;

	case op_basetable:
	case op_insert:
	case op_truncate:
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
	case op_merge:

		if (rel->l)
			rel_dce_refs(sql, rel->l, refs);
		if (rel->r)
			rel_dce_refs(sql, rel->r, refs);
		break;
	case op_ddl:

		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_dce_refs(sql, rel->l, refs);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_dce_refs(sql, rel->l, refs);
			if (rel->r)
				rel_dce_refs(sql, rel->r, refs);
		} break;
	}

	if (rel_is_ref(rel) && !list_find(refs, rel, NULL))
		list_prepend(refs, rel);
}

static sql_rel *
rel_dce_down(mvc *sql, sql_rel *rel, int skip_proj)
{
	if (!rel)
		return rel;

	if (!skip_proj && rel_is_ref(rel))
		return rel;

	switch(rel->op) {
	case op_basetable:
	case op_table:

		if (skip_proj && rel->l && rel->op == op_table && rel->flag != TRIGGER_WRAPPER)
			rel->l = rel_dce_down(sql, rel->l, 0);
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		/* fall through */

	case op_truncate:
		return rel;

	case op_insert:
		rel_used(rel->r);
		rel_dce_sub(sql, rel->r);
		return rel;

	case op_update:
	case op_delete:

		if (skip_proj && rel->r)
			rel->r = rel_dce_down(sql, rel->r, 0);
		if (!skip_proj)
			rel_dce_sub(sql, rel);
		return rel;

	case op_topn:
	case op_sample:
	case op_project:
	case op_groupby:

		if (skip_proj && rel->l)
			rel->l = rel_dce_down(sql, rel->l, is_topn(rel->op) || is_sample(rel->op));
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
	case op_merge:
		if (rel->l)
			rel->l = rel_dce_down(sql, rel->l, 0);
		if (rel->r)
			rel->r = rel_dce_down(sql, rel->r, 0);
		return rel;

	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel->l = rel_dce_down(sql, rel->l, 0);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel->l = rel_dce_down(sql, rel->l, 0);
			if (rel->r)
				rel->r = rel_dce_down(sql, rel->r, 0);
		}
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
	case op_truncate:
		return rel;
	case op_insert:
	case op_update:
	case op_delete:
		if (rel->r)
			rel->r = rel_add_projects(sql, rel->r);
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
	case op_sample:
	case op_project:
	case op_groupby:
	case op_select:
	case op_table:
		if (rel->l && (rel->op != op_table || rel->flag != TRIGGER_WRAPPER))
			rel->l = rel_add_projects(sql, rel->l);
		return rel;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_semi:
	case op_anti:
	case op_merge:
		if (rel->l)
			rel->l = rel_add_projects(sql, rel->l);
		if (rel->r)
			rel->r = rel_add_projects(sql, rel->r);
		return rel;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel->l = rel_add_projects(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel->l = rel_add_projects(sql, rel->l);
			if (rel->r)
				rel->r = rel_add_projects(sql, rel->r);
		}
		return rel;
	}
	return rel;
}

sql_rel *
rel_dce(mvc *sql, sql_rel *rel)
{
	list *refs = sa_list(sql->sa);

	rel_dce_refs(sql, rel, refs);
	if (refs) {
		node *n;

		for(n = refs->h; n; n = n->next) {
			sql_rel *i = n->data;

			while (!rel_is_ref(i) && i->l && !is_base(i->op))
				i = i->l;
			if (i)
				rel_used(i);
		}
	}
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
			if (list_empty(exps))
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

static inline sql_rel *
rel_use_index(visitor *v, sql_rel *rel)
{
	list *exps = NULL;
	sql_idx *i = find_index(v->sql->sa, rel, rel->l, &exps);
	int left = 1;

	assert(is_select(rel->op) || is_join(rel->op));
	if (!i && is_join(rel->op)) {
		left = 0;
		i = find_index(v->sql->sa, rel, rel->r, &exps);
	}

	if (i) {
		prop *p;
		node *n;
		int single_table = 1;
		sql_exp *re = NULL;

		for( n = exps->h; n && single_table; n = n->next) {
			sql_exp *e = n->data;
			sql_exp *nre = e->r;

			if (is_join(rel->op) && ((left && !rel_find_exp(rel->l, e->l)) || (!left && !rel_find_exp(rel->r, e->l))))
				nre = e->l;
			single_table = (!re || (exp_relname(nre) && exp_relname(re) && strcmp(exp_relname(nre), exp_relname(re)) == 0));
			re = nre;
		}
		if (single_table) { /* add PROP_HASHCOL to all column exps */
			for( n = exps->h; n; n = n->next) {
				sql_exp *e = n->data;
				int anti = is_anti(e), semantics = is_semantics(e);

				/* swapped ? */
				if (is_join(rel->op) && ((left && !rel_find_exp(rel->l, e->l)) || (!left && !rel_find_exp(rel->r, e->l))))
					n->data = e = exp_compare(v->sql->sa, e->r, e->l, cmp_equal);
				if (anti) set_anti(e);
				if (semantics) set_semantics(e);
				p = find_prop(e->p, PROP_HASHCOL);
				if (!p)
					e->p = p = prop_create(v->sql->sa, PROP_HASHCOL, e->p);
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
	return rel;
}

static int
score_se_base(visitor *v, sql_rel *rel, sql_exp *e)
{
	int res = 0;
	sql_subtype *t = exp_subtype(e);
	sql_column *c = NULL;

	/* can we find out if the underlying table is sorted */
	if ((c = exp_find_column(rel, e, -2)) && v->storage_based_opt && mvc_is_sorted(v->sql, c))
		res += 600;

	/* prefer the shorter var types over the longer ones */
	res += sql_class_base_score(v, c, t, is_equality_or_inequality_exp(e->flag)); /* smaller the type, better */
	return res;
}

static int
score_se(visitor *v, sql_rel *rel, sql_exp *e)
{
	int score = 0;
	if (e->type == e_cmp && !is_complex_exp(e->flag)) {
		sql_exp *l = e->l;

		while (l->type == e_cmp) { /* go through nested comparisons */
			sql_exp *ll;

			if (l->flag == cmp_filter || l->flag == cmp_or)
				ll = ((list*)l->l)->h->data;
			else
				ll = l->l;
			if (ll->type != e_cmp)
				break;
			l = ll;
		}
		score += score_se_base(v, rel, l);
	}
	score += exp_keyvalue(e);
	return score;
}

static inline sql_rel *
rel_select_order(visitor *v, sql_rel *rel)
{
	int *scores = NULL;
	sql_exp **exps = NULL;

	if (is_select(rel->op) && list_length(rel->exps) > 1) {
		node *n;
		int i, nexps = list_length(rel->exps);
		scores = SA_NEW_ARRAY(v->sql->ta, int, nexps);
		exps = SA_NEW_ARRAY(v->sql->ta, sql_exp*, nexps);

		for (i = 0, n = rel->exps->h; n; i++, n = n->next) {
			exps[i] = n->data;
			scores[i] = score_se(v, rel, n->data);
		}
		GDKqsort(scores, exps, NULL, nexps, sizeof(int), sizeof(void *), TYPE_int, true, true);

		for (i = 0, n = rel->exps->h; n; i++, n = n->next)
			n->data = exps[i];
	}

	return rel;
}

static inline sql_rel *
rel_simplify_like_select(visitor *v, sql_rel *rel)
{
	if (is_select(rel->op) && !list_empty(rel->exps)) {
		for (node *n = rel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;
			list *l = e->l;
			list *r = e->r;

			if (e->type == e_cmp && e->flag == cmp_filter && strcmp(((sql_subfunc*)e->f)->func->base.name, "like") == 0 && list_length(l) == 1 && list_length(r) == 3) {
				list *r = e->r;
				sql_exp *fmt = r->h->data;
				sql_exp *esc = r->h->next->data;
				sql_exp *isen = r->h->next->next->data;
				int rewrite = 0, isnull = 0;

				if (fmt->type == e_convert)
					fmt = fmt->l;
				/* check for simple like expression */
				if (exp_is_null(fmt)) {
					isnull = 1;
				} else if (is_atom(fmt->type)) {
					atom *fa = NULL;

					if (fmt->l)
						fa = fmt->l;
					if (fa && fa->data.vtype == TYPE_str && !strchr(fa->data.val.sval, '%') && !strchr(fa->data.val.sval, '_'))
						rewrite = 1;
				}
				if (rewrite && !isnull) { /* check escape flag */
					if (exp_is_null(esc)) {
						isnull = 1;
					} else {
						atom *ea = esc->l;

						if (!is_atom(esc->type) || !ea)
							rewrite = 0;
						else if (ea->data.vtype != TYPE_str || strlen(ea->data.val.sval) != 0)
							rewrite = 0;
					}
				}
				if (rewrite && !isnull) { /* check insensitive flag */
					if (exp_is_null(isen)) {
						isnull = 1;
					} else {
						atom *ia = isen->l;

						if (!is_atom(isen->type) || !ia)
							rewrite = 0;
						else if (ia->data.vtype != TYPE_bit || ia->data.val.btval == 1)
							rewrite = 0;
					}
				}
				if (isnull) {
					rel->exps = list_append(sa_list(v->sql->sa), exp_null(v->sql->sa, sql_bind_localtype("bit")));
					v->changes++;
					return rel;
				} else if (rewrite) {	/* rewrite to cmp_equal ! */
					list *l = e->l;
					list *r = e->r;
					n->data = exp_compare(v->sql->sa, l->h->data, r->h->data, is_anti(e) ? cmp_notequal : cmp_equal);
					v->changes++;
				}
			}
		}
	}
	return rel;
}

static sql_exp *
rel_simplify_predicates(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)depth;
	if (is_func(e->type) && list_length(e->l) == 3 && is_case_func((sql_subfunc*)e->f) /*is_ifthenelse_func((sql_subfunc*)e->f)*/) {
		list *args = e->l;
		sql_exp *ie = args->h->data;

		if (exp_is_true(ie)) { /* ifthenelse(true, x, y) -> x */
			sql_exp *res = args->h->next->data;
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, res, e);
			v->changes++;
			return res;
		} else if (exp_is_false(ie) || exp_is_null(ie)) { /* ifthenelse(false or null, x, y) -> y */
			sql_exp *res = args->h->next->next->data;
			if (exp_name(e))
				exp_prop_alias(v->sql->sa, res, e);
			v->changes++;
			return res;
		}
	}
	if (is_select(rel->op) || is_join(rel->op) || is_semi(rel->op)) {
		if (is_compare(e->type) && is_semantics(e) && (e->flag == cmp_equal || e->flag == cmp_notequal) && exp_is_null(e->r)) {
			/* simplify 'is null' predicates on constants */
			if (exp_is_null(e->l)) {
				int nval = e->flag == cmp_equal;
				if (is_anti(e)) nval = !nval;
				e = exp_atom_bool(v->sql->sa, nval);
				v->changes++;
				return e;
			} else if (exp_is_not_null(e->l)) {
				int nval = e->flag == cmp_notequal;
				if (is_anti(e)) nval = !nval;
				e = exp_atom_bool(v->sql->sa, nval);
				v->changes++;
				return e;
			}
		}
		if (is_atom(e->type) && ((!e->l && !e->r && !e->f) || e->r)) /* prepared statement parameter or argument */
			return e;
		if (is_atom(e->type) && e->l) { /* direct literal */
			atom *a = e->l;
			int flag = a->data.val.bval;

			/* remove simple select true expressions */
			if (flag)
				return e;
		}
		if (is_compare(e->type) && is_theta_exp(e->flag)) {
			sql_exp *l = e->l;
			sql_exp *r = e->r;

			if (is_func(l->type) && (e->flag == cmp_equal || e->flag == cmp_notequal)) {
				sql_subfunc *f = l->f;

				/* rewrite isnull(x) = TRUE/FALSE => x =/<> NULL */
				if (!f->func->s && is_isnull_func(f)) {
					list *args = l->l;
					sql_exp *ie = args->h->data;

					if (!has_nil(ie) || exp_is_not_null(ie)) { /* is null on something that is never null, is always false */
						ie = exp_atom_bool(v->sql->sa, 0);
						v->changes++;
						e->l = ie;
					} else if (exp_is_null(ie)) { /* is null on something that is always null, is always true */
						ie = exp_atom_bool(v->sql->sa, 1);
						v->changes++;
						e->l = ie;
					} else if (is_atom(r->type) && r->l) { /* direct literal */
						atom *a = r->l;

						if (a->isnull) {
							if (is_semantics(e)) { /* isnull(x) = NULL -> false, isnull(x) <> NULL -> true */
								int flag = e->flag == cmp_notequal;
								if (is_anti(e))
									flag = !flag;
								e = exp_atom_bool(v->sql->sa, flag);
							} else /* always NULL */
								e = exp_null(v->sql->sa, sql_bind_localtype("bit"));
							v->changes++;
						} else {
							int flag = a->data.val.bval;

							assert(list_length(args) == 1);
							l = args->h->data;
							if (exp_subtype(l)) {
								r = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(l), NULL));
								e = exp_compare(v->sql->sa, l, r, e->flag);
								if (e && !flag)
									set_anti(e);
								if (e)
									set_semantics(e);
								v->changes++;
							}
						}
					}
				} else if (!f->func->s && is_not_func(f)) {
					if (is_atom(r->type) && r->l) { /* direct literal */
						atom *a = r->l;
						list *args = l->l;
						sql_exp *inner = args->h->data;
						sql_subfunc *inf = inner->f;

						assert(list_length(args) == 1);

						/* not(not(x)) = TRUE/FALSE => x = TRUE/FALSE */
						if (is_func(inner->type) &&
							!inf->func->s &&
							is_not_func(inf)) {
							int anti = is_anti(e), is_semantics = is_semantics(e);

							args = inner->l;
							assert(list_length(args) == 1);
							l = args->h->data;
							e = exp_compare(v->sql->sa, l, r, e->flag);
							if (anti) set_anti(e);
							if (is_semantics) set_semantics(e);
							v->changes++;
						/* rewrite not(=/<>(a,b)) = TRUE/FALSE => a=b / a<>b */
						} else if (is_func(inner->type) &&
							!inf->func->s &&
							(!strcmp(inf->func->base.name, "=") ||
							 !strcmp(inf->func->base.name, "<>"))) {
							int flag = a->data.val.bval;
							sql_exp *ne;
							args = inner->l;

							if (!strcmp(inf->func->base.name, "<>"))
								flag = !flag;
							if (e->flag == cmp_notequal)
								flag = !flag;
							assert(list_length(args) == 2);
							l = args->h->data;
							r = args->h->next->data;
							ne = exp_compare(v->sql->sa, l, r, (!flag)?cmp_equal:cmp_notequal);
							if (a->isnull)
								e->l = ne;
							else
								e = ne;
							v->changes++;
						} else if (a && a->data.vtype == TYPE_bit) {
							int anti = is_anti(e), is_semantics = is_semantics(e);

							/* change atom's value on right */
							l = args->h->data;
							if (!a->isnull)
								r = exp_atom_bool(v->sql->sa, !a->data.val.bval);
							e = exp_compare(v->sql->sa, l, r, e->flag);
							if (anti) set_anti(e);
							if (is_semantics) set_semantics(e);
							v->changes++;
						}
					}
				}
			} else if (is_atom(l->type) && is_atom(r->type) && !is_semantics(e) && !e->f) {
				/* compute comparisons on atoms */
				if (exp_is_null(l) || exp_is_null(r)) {
					e = exp_null(v->sql->sa, sql_bind_localtype("bit"));
					v->changes++;
				} else if (l->l && r->l) {
					int res = atom_cmp(l->l, r->l);
					bool flag = !is_anti(e);

					if (res == 0)
						e = exp_atom_bool(v->sql->sa, (e->flag == cmp_equal || e->flag == cmp_gte || e->flag == cmp_lte) ? flag : !flag);
					else if (res > 0)
						e = exp_atom_bool(v->sql->sa, (e->flag == cmp_gt || e->flag == cmp_gte || e->flag == cmp_notequal) ? flag : !flag);
					else
						e = exp_atom_bool(v->sql->sa, (e->flag == cmp_lt || e->flag == cmp_lte || e->flag == cmp_notequal) ? flag : !flag);
					v->changes++;
				}
			}
		}
	}
	return e;
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
		sql_exp *ne = n->data;

		if (rel && rel->l) {
			if ((exp_relname(ne) && exp_name(ne) && rel_bind_column2(sql, rel->l, exp_relname(ne), exp_name(ne), 0)) ||
			   (!exp_relname(ne) && exp_name(ne) && rel_bind_column(sql, rel->l, exp_name(ne), 0, 1))) {
				exp_label(sql->sa, e, ++sql->label);
				append(rel->exps, e);
				ne = e;
			}
		}
		e = ne;
	}
	e = exp_ref(sql, e);
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

		if (e->type != e_column && !exp_is_atom(e))
			n->data = add_exp_too_project(sql, e, rel);
	}
}

static sql_exp *
split_exp(mvc *sql, sql_exp *e, sql_rel *rel)
{
	if (exp_is_atom(e))
		return e;
	switch(e->type) {
	case e_column:
		return e;
	case e_convert:
		e->l = split_exp(sql, e->l, rel);
		return e;
	case e_aggr:
	case e_func:
		if (!is_analytic(e) && !exp_has_sideeffect(e)) {
			sql_subfunc *f = e->f;
			if (e->type == e_func && !f->func->s && is_caselike_func(f) /*is_ifthenelse_func(f)*/) {
				return e;
			} else {
				split_exps(sql, e->l, rel);
				add_exps_too_project(sql, e->l, rel);
			}
		}
		return e;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			split_exps(sql, e->l, rel);
			split_exps(sql, e->r, rel);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = split_exp(sql, e->l, rel);
			split_exps(sql, e->r, rel);
		} else {
			e->l = split_exp(sql, e->l, rel);
			e->r = split_exp(sql, e->r, rel);
			if (e->f)
				e->f = split_exp(sql, e->f, rel);
		}
		return e;
	case e_atom:
	case e_psm:
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
rel_split_project(visitor *v, sql_rel *rel, int top)
{
	if (mvc_highwater(v->sql))
		return rel;

	if (!rel)
		return NULL;
	if (is_project(rel->op) && list_length(rel->exps) && (is_groupby(rel->op) || rel->l) && !need_distinct(rel) && !is_single(rel)) {
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
			nrel = rel_project(v->sql->sa, rel->l,
				rel_projections(v->sql, rel->l, NULL, 1, 1));
			rel->l = nrel;
			/* recursively split all functions and add those to the projection list */
			split_exps(v->sql, rel->exps, nrel);
			if (nrel->l && !(nrel->l = rel_split_project(v, nrel->l, (is_topn(rel->op)||is_sample(rel->op))?top:0)))
				return NULL;
			return rel;
		} else if (funcs && !top && list_empty(rel->r)) {
			/* projects can have columns point back into the expression list, ie
			 * create a new list including the split expressions */
			node *n;
			list *exps = rel->exps;

			rel->exps = sa_list(v->sql->sa);
			for (n=exps->h; n; n = n->next)
				append(rel->exps, split_exp(v->sql, n->data, rel));
		} else if (funcs && top && rel_is_ref(rel) && list_empty(rel->r)) {
			/* inplace */
			list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
			sql_rel *l = rel_project(v->sql->sa, rel->l, NULL);
			rel->l = l;
			l->exps = rel->exps;
			rel->exps = exps;
		}
	}
	if (is_set(rel->op) || is_basetable(rel->op))
		return rel;
	if (rel->l) {
		rel->l = rel_split_project(v, rel->l, (is_topn(rel->op)||is_sample(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
		if (!rel->l)
			return NULL;
	}
	if ((is_join(rel->op) || is_semi(rel->op)) && rel->r) {
		rel->r = rel_split_project(v, rel->r, (is_topn(rel->op)||is_sample(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
		if (!rel->r)
			return NULL;
	}
	return rel;
}

static void select_split_exps(mvc *sql, list *exps, sql_rel *rel);

static sql_exp *
select_split_exp(mvc *sql, sql_exp *e, sql_rel *rel)
{
	switch(e->type) {
	case e_column:
		return e;
	case e_convert:
		e->l = select_split_exp(sql, e->l, rel);
		return e;
	case e_aggr:
	case e_func:
		if (!is_analytic(e) && !exp_has_sideeffect(e)) {
			sql_subfunc *f = e->f;
			if (e->type == e_func && !f->func->s && is_caselike_func(f) /*is_ifthenelse_func(f)*/)
				return add_exp_too_project(sql, e, rel);
		}
		return e;
	case e_cmp:
		if (e->flag == cmp_or || e->flag == cmp_filter) {
			select_split_exps(sql, e->l, rel);
			select_split_exps(sql, e->r, rel);
		} else if (e->flag == cmp_in || e->flag == cmp_notin) {
			e->l = select_split_exp(sql, e->l, rel);
			select_split_exps(sql, e->r, rel);
		} else {
			e->l = select_split_exp(sql, e->l, rel);
			e->r = select_split_exp(sql, e->r, rel);
			if (e->f)
				e->f = select_split_exp(sql, e->f, rel);
		}
		return e;
	case e_atom:
	case e_psm:
		return e;
	}
	return e;
}

static void
select_split_exps(mvc *sql, list *exps, sql_rel *rel)
{
	node *n;

	if (!exps)
		return;
	for(n=exps->h; n; n = n->next){
		sql_exp *e = n->data;

		e = select_split_exp(sql, e, rel);
		n->data = e;
	}
}

static sql_rel *
rel_split_select(visitor *v, sql_rel *rel, int top)
{
	if (mvc_highwater(v->sql))
		return rel;

	if (!rel)
		return NULL;
	if (is_select(rel->op) && list_length(rel->exps) && rel->l) {
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
			nrel = rel_project(v->sql->sa, rel->l,
				rel_projections(v->sql, rel->l, NULL, 1, 1));
			rel->l = nrel;
			/* recursively split all functions and add those to the projection list */
			select_split_exps(v->sql, rel->exps, nrel);
			if (nrel->l && !(nrel->l = rel_split_project(v, nrel->l, (is_topn(rel->op)||is_sample(rel->op))?top:0)))
				return NULL;
			return rel;
		} else if (funcs && !top && list_empty(rel->r)) {
			/* projects can have columns point back into the expression list, ie
			 * create a new list including the split expressions */
			node *n;
			list *exps = rel->exps;

			rel->exps = sa_list(v->sql->sa);
			for (n=exps->h; n; n = n->next)
				append(rel->exps, select_split_exp(v->sql, n->data, rel));
		} else if (funcs && top && rel_is_ref(rel) && list_empty(rel->r)) {
			/* inplace */
			list *exps = rel_projections(v->sql, rel, NULL, 1, 1);
			sql_rel *l = rel_project(v->sql->sa, rel->l, NULL);
			rel->l = l;
			l->exps = rel->exps;
			rel->exps = exps;
		}
	}
	if (is_set(rel->op) || is_basetable(rel->op))
		return rel;
	if (rel->l) {
		rel->l = rel_split_select(v, rel->l, (is_topn(rel->op)||is_sample(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
		if (!rel->l)
			return NULL;
	}
	if ((is_join(rel->op) || is_semi(rel->op)) && rel->r) {
		rel->r = rel_split_select(v, rel->r, (is_topn(rel->op)||is_sample(rel->op)||is_ddl(rel->op)||is_modify(rel->op))?top:0);
		if (!rel->r)
			return NULL;
	}
	return rel;
}

static list *
exp_merge_range(visitor *v, sql_rel *rel, list *exps)
{
	node *n, *m;
	for (n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_exp *le = e->l;
		sql_exp *re = e->r;

		/* handle the and's in the or lists */
		if (e->type == e_cmp && e->flag == cmp_or && !is_anti(e)) {
			e->l = exp_merge_range(v, rel, e->l);
			e->r = exp_merge_range(v, rel, e->r);
		/* only look for gt, gte, lte, lt */
		} else if (n->next &&
		    e->type == e_cmp && e->flag < cmp_equal && !e->f &&
		    re->card == CARD_ATOM && !is_anti(e)) {
			for (m=n->next; m; m = m->next) {
				sql_exp *f = m->data;
				sql_exp *lf = f->l;
				sql_exp *rf = f->r;
				int c_le = is_numeric_upcast(le), c_lf = is_numeric_upcast(lf);

				if (f->type == e_cmp && f->flag < cmp_equal && !f->f &&
				    rf->card == CARD_ATOM && !is_anti(f) &&
				    exp_match_exp(c_le?le->l:le, c_lf?lf->l:lf)) {
					sql_exp *ne;
					int swap = 0, lt = 0, gt = 0;
					sql_subtype super;
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

					supertype(&super, exp_subtype(le), exp_subtype(lf));
					if (!(rf = exp_check_type(v->sql, &super, rel, rf, type_equal)) ||
						!(le = exp_check_type(v->sql, &super, rel, le, type_equal)) ||
						!(re = exp_check_type(v->sql, &super, rel, re, type_equal))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = 0;
							continue;
						}
					if (!swap)
						ne = exp_compare2(v->sql->sa, le, re, rf, compare2range(e->flag, f->flag), 0);
					else
						ne = exp_compare2(v->sql->sa, le, rf, re, compare2range(f->flag, e->flag), 0);

					list_remove_data(exps, NULL, e);
					list_remove_data(exps, NULL, f);
					list_append(exps, ne);
					v->changes++;
					return exp_merge_range(v, rel, exps);
				}
			}
		} else if (n->next &&
			   e->type == e_cmp && e->flag < cmp_equal && !e->f &&
		    	   re->card > CARD_ATOM && !is_anti(e)) {
			for (m=n->next; m; m = m->next) {
				sql_exp *f = m->data;
				sql_exp *lf = f->l;
				sql_exp *rf = f->r;

				if (f->type == e_cmp && f->flag < cmp_equal && !f->f  &&
				    rf->card > CARD_ATOM && !is_anti(f)) {
					sql_exp *ne, *t;
					int swap = 0, lt = 0, gt = 0;
					comp_type ef = (comp_type) e->flag, ff = (comp_type) f->flag;
					int c_re = is_numeric_upcast(re), c_rf = is_numeric_upcast(rf);
					int c_le = is_numeric_upcast(le), c_lf = is_numeric_upcast(lf), c;
					sql_subtype super;

					/* both swapped ? */
					if (exp_match_exp(c_re?re->l:re, c_rf?rf->l:rf)) {
						t = re;
						re = le;
						le = t;
						c = c_re; c_re = c_le; c_le = c;
						ef = swap_compare(ef);
						t = rf;
						rf = lf;
						lf = t;
						c = c_rf; c_rf = c_lf; c_lf = c;
						ff = swap_compare(ff);
					}

					/* is left swapped ? */
					if (exp_match_exp(c_re?re->l:re, c_lf?lf->l:lf)) {
						t = re;
						re = le;
						le = t;
						c = c_re; c_re = c_le; c_le = c;
						ef = swap_compare(ef);
					}

					/* is right swapped ? */
					if (exp_match_exp(c_le?le->l:le, c_rf?rf->l:rf)) {
						t = rf;
						rf = lf;
						lf = t;
						c = c_rf; c_rf = c_lf; c_lf = c;
						ff = swap_compare(ff);
					}

					if (!exp_match_exp(c_le?le->l:le, c_lf?lf->l:lf))
						continue;

					/* for now only   c1 <[=] x <[=] c2 */
					swap = lt = (ef == cmp_lt || ef == cmp_lte);
					gt = !lt;

					if (gt && (ff == cmp_gt || ff == cmp_gte))
						continue;
					if (lt && (ff == cmp_lt || ff == cmp_lte))
						continue;

					supertype(&super, exp_subtype(le), exp_subtype(lf));
					if (!(rf = exp_check_type(v->sql, &super, rel, rf, type_equal)) ||
						!(le = exp_check_type(v->sql, &super, rel, le, type_equal)) ||
						!(re = exp_check_type(v->sql, &super, rel, re, type_equal))) {
							v->sql->session->status = 0;
							v->sql->errstr[0] = 0;
							continue;
						}
					if (!swap)
						ne = exp_compare2(v->sql->sa, le, re, rf, compare2range(ef, ff), 0);
					else
						ne = exp_compare2(v->sql->sa, le, rf, re, compare2range(ff, ef), 0);

					list_remove_data(exps, NULL, e);
					list_remove_data(exps, NULL, f);
					list_append(exps, ne);
					v->changes++;
					return exp_merge_range(v, rel, exps);
				}
			}
		}
	}
	return exps;
}

/*
 * Casting decimal values on both sides of a compare expression is expensive,
 * both in preformance (cpu cost) and memory requirements (need for large
 * types).
 */

#define reduce_scale_tpe(tpe, uval) \
	do { \
		tpe v = uval; \
		if (v != 0) { \
			while( (v/10)*10 == v ) { \
				i++; \
				v /= 10; \
			} \
			nval = v; \
		} \
	} while (0)

static atom *
reduce_scale(mvc *sql, atom *a)
{
	int i = 0;
	atom *na = a;
#ifdef HAVE_HGE
	hge nval = 0;
#else
	lng nval = 0;
#endif

#ifdef HAVE_HGE
	if (a->data.vtype == TYPE_hge) {
		reduce_scale_tpe(hge, a->data.val.hval);
	} else
#endif
	if (a->data.vtype == TYPE_lng) {
		reduce_scale_tpe(lng, a->data.val.lval);
	} else if (a->data.vtype == TYPE_int) {
		reduce_scale_tpe(int, a->data.val.ival);
	} else if (a->data.vtype == TYPE_sht) {
		reduce_scale_tpe(sht, a->data.val.shval);
	} else if (a->data.vtype == TYPE_bte) {
		reduce_scale_tpe(bte, a->data.val.btval);
	}
	if (i) {
		na = atom_int(sql->sa, &a->tpe, nval);
		if (na->tpe.scale)
			na->tpe.scale -= i;
	}
	return na;
}

static sql_rel *
rel_project_reduce_casts(visitor *v, sql_rel *rel)
{
	if (!rel)
		return NULL;
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
					atom *ha = exp_value(v->sql, h), *ta = exp_value(v->sql, t);

					if (ha || ta) {
						atom *a = ha ? ha : ta;
						atom *na = reduce_scale(v->sql, a);

						if (na != a) {
							int rs = a->tpe.scale - na->tpe.scale;
							res->scale -= rs;
							if (ha) {
								h->r = NULL;
								h->l = na;
							} else {
								t->r = NULL;
								t->l = na;
							}
							v->changes++;
						}
					}
				}
			}
		}
	}
	return rel;
}

static inline sql_rel *
rel_reduce_casts(visitor *v, sql_rel *rel)
{
	list *exps = rel->exps;
	assert(!list_empty(rel->exps));

	for (node *n=exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_exp *le = e->l;
		sql_exp *re = e->r;

		/* handle the and's in the or lists */
		if (e->type != e_cmp || !is_theta_exp(e->flag) || e->f)
			continue;
		/* rewrite e if left or right is a cast */
		if (le->type == e_convert || re->type == e_convert) {
			sql_rel *r = rel->r;

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

						if (fst->scale && fst->scale == ft->scale && is_atom(ce->type) && ce->l) {
							atom *a = ce->l;
							int anti = is_anti(e);
							sql_exp *arg1, *arg2;
#ifdef HAVE_HGE
							hge val = 1;
#else
							lng val = 1;
#endif
							/* multiply with smallest value, then scale and (round) */
							int scale = (int) tt->scale - (int) ft->scale, rs = 0;
							atom *na = reduce_scale(v->sql, a);

							if (na != a) {
								rs = a->tpe.scale - na->tpe.scale;
								ce->l = na;
							}
							scale -= rs;

							while(scale > 0) {
								scale--;
								val *= 10;
							}
							arg1 = re;
#ifdef HAVE_HGE
							arg2 = exp_atom_hge(v->sql->sa, val);
#else
							arg2 = exp_atom_lng(v->sql->sa, val);
#endif
							if (!(nre = rel_binop_(v->sql, NULL, arg1, arg2, "sys", "scale_down", card_value))) {
								v->sql->session->status = 0;
								v->sql->errstr[0] = '\0';
								continue;
							}
							e = exp_compare(v->sql->sa, le->l, nre, e->flag);
							if (anti) set_anti(e);
							v->changes++;
						}
					}
				}
			}
		}
		n->data = e;
	}
	return rel;
}

static int
is_identity_of(sql_exp *e, sql_rel *l)
{
	if (e->type != e_cmp)
		return 0;
	if (!is_identity(e->l, l) || !is_identity(e->r, l) || (e->f && !is_identity(e->f, l)))
		return 0;
	return 1;
}

static inline sql_rel *
rel_rewrite_semijoin(visitor *v, sql_rel *rel)
{
	assert(is_semi(rel->op));
	{
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
			v->changes++;
		}
	}
	{
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
			exps = new_exp_list(v->sql->sa);

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
				    ne->flag != cmp_equal || is_anti(oe) || is_anti(ne))
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

				ne = exp_compare(v->sql->sa, le, re, cmp_equal);
				append(exps, ne);
			}

			rel->r = rel_dup(r->r);
			rel->exps = exps;
			rel_destroy(or);
			v->changes++;
		}
	}
	return rel;
}

/* antijoin(a, union(b,c)) -> antijoin(antijoin(a,b), c) */
static inline sql_rel *
rel_rewrite_antijoin(visitor *v, sql_rel *rel)
{
	sql_rel *l = rel->l;
	sql_rel *r = rel->r;

	assert(rel->op == op_anti);
	if (l && !rel_is_ref(l) && r && !rel_is_ref(r) && is_union(r->op) && !is_single(r)) {
		sql_rel *rl = rel_dup(r->l), *nl;
		sql_rel *rr = rel_dup(r->r);

		if (!is_project(rl->op))
			rl = rel_project(v->sql->sa, rl,
				rel_projections(v->sql, rl, NULL, 1, 1));
		if (!is_project(rr->op))
			rr = rel_project(v->sql->sa, rr,
				rel_projections(v->sql, rr, NULL, 1, 1));
		rel_rename_exps(v->sql, r->exps, rl->exps);
		rel_rename_exps(v->sql, r->exps, rr->exps);

		nl = rel_crossproduct(v->sql->sa, rel->l, rl, op_anti);
		nl->exps = exps_copy(v->sql, rel->exps);
		rel->l = nl;
		rel->r = rr;
		rel_destroy(r);
		v->changes++;
		return rel;
	}
	return rel;
}

static sql_rel *
rel_semijoin_use_fk(visitor *v, sql_rel *rel)
{
	if (is_semi(rel->op) && rel->exps) {
		list *exps = rel->exps;
		list *rels = sa_list(v->sql->sa);

		rel->exps = NULL;
		append(rels, rel->l);
		append(rels, rel->r);
		(void) find_fk( v->sql, rels, exps);

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
		if (exp_name(e))
			exp_prop_alias(sql->sa, ne, e);
		append(rel->exps, ne);
	}
}

static sql_rel *
rel_split_outerjoin(visitor *v, sql_rel *rel)
{
	if ((rel->op == op_left || rel->op == op_right || rel->op == op_full) &&
	    list_length(rel->exps) == 1 && exps_nr_of_or(rel->exps) == list_length(rel->exps)) {
		sql_rel *l = rel->l, *nl, *nll, *nlr;
		sql_rel *r = rel->r, *nr;
		sql_exp *e;
		list *exps;

		nll = rel_crossproduct(v->sql->sa, rel_dup(l), rel_dup(r), op_join);
		nlr = rel_crossproduct(v->sql->sa, rel_dup(l), rel_dup(r), op_join);

		/* TODO find or exp, ie handle rest with extra joins */
		/* expect only a single or expr for now */
		assert(list_length(rel->exps) == 1);
	       	e = rel->exps->h->data;
		nll->exps = exps_copy(v->sql, e->l);
		nlr->exps = exps_copy(v->sql, e->r);
		if (!(nl = rel_or( v->sql, NULL, nll, nlr, NULL, NULL, NULL)))
			return NULL;

		if (rel->op == op_left || rel->op == op_full) {
			/* split in 2 anti joins */
			nr = rel_crossproduct(v->sql->sa, rel_dup(l), rel_dup(r), op_anti);
			nr->exps = exps_copy(v->sql, e->l);
			nr = rel_crossproduct(v->sql->sa, nr, rel_dup(r), op_anti);
			nr->exps = exps_copy(v->sql, e->r);

			/* project left */
			nr = rel_project(v->sql->sa, nr,
				rel_projections(v->sql, l, NULL, 1, 1));
			/* add null's for right */
			add_nulls( v->sql, nr, r);
			exps = rel_projections(v->sql, nl, NULL, 1, 1);
			nl = rel_setop(v->sql->sa, nl, nr, op_union);
			rel_setop_set_exps(v->sql, nl, exps, false);
			set_processed(nl);
		}
		if (rel->op == op_right || rel->op == op_full) {
			/* split in 2 anti joins */
			nr = rel_crossproduct(v->sql->sa, rel_dup(r), rel_dup(l), op_anti);
			nr->exps = exps_copy(v->sql, e->l);
			nr = rel_crossproduct(v->sql->sa, nr, rel_dup(l), op_anti);
			nr->exps = exps_copy(v->sql, e->r);

			nr = rel_project(v->sql->sa, nr, sa_list(v->sql->sa));
			/* add null's for left */
			add_nulls( v->sql, nr, l);
			/* project right */
			nr->exps = list_merge(nr->exps,
				rel_projections(v->sql, r, NULL, 1, 1),
				(fdup)NULL);
			exps = rel_projections(v->sql, nl, NULL, 1, 1);
			nl = rel_setop(v->sql->sa, nl, nr, op_union);
			rel_setop_set_exps(v->sql, nl, exps, false);
			set_processed(nl);
		}

		rel_destroy(rel);
		v->changes++;
		rel = nl;
	}
	return rel;
}

static int
exp_range_overlap(atom *min, atom *max, atom *emin, atom *emax, bool min_exclusive, bool max_exclusive)
{
	if (!min || !max || !emin || !emax || min->isnull || max->isnull || emin->isnull || emax->isnull)
		return 0;

	if ((!min_exclusive && VALcmp(&(emax->data), &(min->data)) < 0) || (min_exclusive && VALcmp(&(emax->data), &(min->data)) <= 0))
		return 0;
	if ((!max_exclusive && VALcmp(&(emin->data), &(max->data)) > 0) || (max_exclusive && VALcmp(&(emin->data), &(max->data)) >= 0))
		return 0;
	return 1;
}

typedef struct {
	atom *lval;
	atom *hval;
	bte anti:1,
		semantics:1;
	int flag;
	list *values;
} range_limit;

typedef struct {
	list *cols;
	list *ranges;
	sql_rel *sel;
} merge_table_prune_info;

static sql_rel *
merge_table_prune_and_unionize(visitor *v, sql_rel *mt_rel, merge_table_prune_info *info)
{
	if (mvc_highwater(v->sql))
		return sql_error(v->sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	sql_rel *nrel = NULL;
	sql_table *mt = (sql_table*) mt_rel->l;
	const char *mtalias = exp_relname(mt_rel->exps->h->data);
	list *tables = sa_list(v->sql->sa);

	for (node *nt = mt->members->h; nt; nt = nt->next) {
		sql_part *pd = nt->data;
		sql_table *pt = find_sql_table_id(v->sql->session->tr, mt->s, pd->member);
		sqlstore *store = v->sql->session->tr->store;
		int skip = 0, allowed = 1;

		/* At the moment we throw an error in the optimizer, but later this rewriter should move out from the optimizers */
		if ((isMergeTable(pt) || isReplicaTable(pt)) && list_empty(pt->members))
			return sql_error(v->sql, 02, SQLSTATE(42000) "The %s '%s.%s' should have at least one table associated",
							 TABLE_TYPE_DESCRIPTION(pt->type, pt->properties), pt->s->base.name, pt->base.name);
		/* Do not include empty partitions */
		if (isTable(pt) && pt->access == TABLE_READONLY && !store->storage_api.count_col(v->sql->session->tr, ol_first_node(pt->columns)->data, 0))
			continue;

		if (!table_privs(v->sql, pt, PRIV_SELECT)) /* Test for privileges */
			allowed = 0;

		for (node *n = mt_rel->exps->h; n && !skip; n = n->next) { /* for each column of the child table */
			sql_exp *e = n->data;
			int i = 0;
			bool first_attempt = true;
			atom *cmin = NULL, *cmax = NULL, *rmin = NULL, *rmax = NULL;
			list *inlist = NULL;
			const char *cname = e->r;
			sql_column *mt_col = NULL, *col = NULL;

			if (cname[0] == '%') /* Ignore TID and indexes here */
				continue;

			mt_col = ol_find_name(mt->columns, exp_name(e))->data;
			col = ol_fetch(pt->columns, mt_col->colnr);
			assert(e && e->type == e_column && col);
			if (!allowed && !column_privs(v->sql, col, PRIV_SELECT))
				return sql_error(v->sql, 02, SQLSTATE(42000) "The user %s SELECT permissions on table '%s.%s' don't match %s '%s.%s'", get_string_global_var(v->sql, "current_user"),
								 pt->s->base.name, pt->base.name, TABLE_TYPE_DESCRIPTION(mt->type, mt->properties), mt->s->base.name, mt->base.name);
			if (isTable(pt) && info && !list_empty(info->cols) && ATOMlinear(exp_subtype(e)->type->localtype)) {
				for (node *nn = info->cols->h ; nn && !skip; nn = nn->next) { /* test if it passes all predicates around it */
					if (nn->data == e) {
						range_limit *next = list_fetch(info->ranges, i);
						atom *lval = next->lval, *hval = next->hval;
						list *values = next->values;

						/* I don't handle cmp_in or cmp_notin cases with anti or null semantics yet */
						if (next->flag == cmp_in && (next->anti || next->semantics))
							continue;

						assert(col && (lval || values));
						if (!skip && pt->access == TABLE_READONLY) {
							/* check if the part falls within the bounds of the select expression else skip this (keep at least on part-table) */
							if (!cmin && !cmax && first_attempt) {
								void *min = NULL, *max = NULL;
								if (sql_trans_ranges(v->sql->session->tr, col, &min, &max) && min && max) {
									cmin = atom_general_ptr(v->sql->sa, &col->type, min);
									cmax = atom_general_ptr(v->sql->sa, &col->type, max);
								}
								first_attempt = false; /* no more attempts to read from storage */
							}

							if (cmin && cmax) {
								if (lval) {
									if (!next->semantics && ((lval && lval->isnull) || (hval && hval->isnull))) {
										skip = 1; /* NULL values don't match, skip them */
									} else if (!next->semantics) {
										if (next->flag == cmp_equal) {
											skip |= next->anti ? exp_range_overlap(cmin, cmax, lval, hval, false, false) != 0 :
																	exp_range_overlap(cmin, cmax, lval, hval, false, false) == 0;
										} else if (hval != lval) { /* range case */
											comp_type lower = range2lcompare(next->flag), higher = range2rcompare(next->flag);
											skip |= next->anti ? exp_range_overlap(cmin, cmax, lval, hval, higher == cmp_lt, lower == cmp_gt) != 0 :
																	exp_range_overlap(cmin, cmax, lval, hval, higher == cmp_lt, lower == cmp_gt) == 0;
										} else {
											switch (next->flag) {
												case cmp_gt:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) < 0 : VALcmp(&(lval->data), &(cmax->data)) >= 0;
													break;
												case cmp_gte:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) <= 0 : VALcmp(&(lval->data), &(cmax->data)) > 0;
													break;
												case cmp_lt:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) < 0 : VALcmp(&(cmin->data), &(lval->data)) >= 0;
													break;
												case cmp_lte:
													skip |= next->anti ? VALcmp(&(lval->data), &(cmax->data)) <= 0 : VALcmp(&(cmin->data), &(lval->data)) > 0;
													break;
												default:
													break;
											}
										}
									}
								} else if (next->flag == cmp_in) {
									int nskip = 1;
									for (node *m = values->h; m && nskip; m = m->next) {
										atom *a = m->data;

										if (a->isnull)
											continue;
										nskip &= exp_range_overlap(cmin, cmax, a, a, false, false) == 0;
									}
									skip |= nskip;
								}
							}
						}
						if (!skip && isPartitionedByColumnTable(mt) && strcmp(mt->part.pcol->base.name, col->base.name) == 0) {
							if (!next->semantics && ((lval && lval->isnull) || (hval && hval->isnull))) {
								skip = 1; /* NULL values don't match, skip them */
							} else if (next->semantics) {
								/* TODO NOT NULL prunning for partitions that just hold NULL values is still missing */
								skip |= next->flag == cmp_equal && !next->anti && lval && lval->isnull ? pd->with_nills == 0 : 0; /* *= NULL case */
							} else {
								if (isRangePartitionTable(mt)) {
									if (!rmin || !rmax) { /* initialize lazily */
										rmin = atom_general_ptr(v->sql->sa, &col->type, pd->part.range.minvalue);
										rmax = atom_general_ptr(v->sql->sa, &col->type, pd->part.range.maxvalue);
									}

									/* Prune range partitioned tables */
									if (rmin->isnull && rmax->isnull) {
										if (pd->with_nills == 1) /* the partition just holds null values, skip it */
											skip = 1;
										/* otherwise it holds all values in the range, cannot be pruned */
									} else if (rmin->isnull) { /* MINVALUE to limit */
										if (lval) {
											if (hval != lval) { /* range case */
												/* There's need to call range2lcompare, because the partition's upper limit is always exclusive */
												skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
											} else {
												switch (next->flag) { /* upper limit always exclusive */
													case cmp_equal:
													case cmp_gt:
													case cmp_gte:
														skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
														break;
													default:
														break;
												}
											}
										} else if (next->flag == cmp_in) {
											int nskip = 1;
											for (node *m = values->h; m && nskip; m = m->next) {
												atom *a = m->data;

												if (a->isnull)
													continue;
												nskip &= VALcmp(&(a->data), &(rmax->data)) >= 0;
											}
											skip |= nskip;
										}
									} else if (rmax->isnull) { /* limit to MAXVALUE */
										if (lval) {
											if (hval != lval) { /* range case */
												comp_type higher = range2rcompare(next->flag);
												if (higher == cmp_lt) {
													skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) < 0 : VALcmp(&(rmin->data), &(hval->data)) >= 0;
												} else if (higher == cmp_lte) {
													skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) <= 0 : VALcmp(&(rmin->data), &(hval->data)) > 0;
												} else {
													assert(0);
												}
											} else {
												switch (next->flag) {
													case cmp_lt:
														skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) < 0 : VALcmp(&(rmin->data), &(hval->data)) >= 0;
														break;
													case cmp_equal:
													case cmp_lte:
														skip |= next->anti ? VALcmp(&(rmin->data), &(hval->data)) <= 0 : VALcmp(&(rmin->data), &(hval->data)) > 0;
														break;
													default:
														break;
												}
											}
										} else if (next->flag == cmp_in) {
											int nskip = 1;
											for (node *m = values->h; m && nskip; m = m->next) {
												atom *a = m->data;

												if (a->isnull)
													continue;
												nskip &= VALcmp(&(rmin->data), &(a->data)) > 0;
											}
											skip |= nskip;
										}
									} else { /* limit1 to limit2 (general case), limit2 is exclusive */
										bool max_differ_min = ATOMcmp(col->type.type->localtype, &rmin->data.val, &rmax->data.val) != 0;

										if (lval) {
											if (next->flag == cmp_equal) {
												skip |= next->anti ? exp_range_overlap(rmin, rmax, lval, hval, false, max_differ_min) != 0 :
																		exp_range_overlap(rmin, rmax, lval, hval, false, max_differ_min) == 0;
											} else if (hval != lval) { /* For the between case */
												comp_type higher = range2rcompare(next->flag);
												skip |= next->anti ? exp_range_overlap(rmin, rmax, lval, hval, higher == cmp_lt, max_differ_min) != 0 :
																		exp_range_overlap(rmin, rmax, lval, hval, higher == cmp_lt, max_differ_min) == 0;
											} else {
												switch (next->flag) {
													case cmp_gt:
														skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
														break;
													case cmp_gte:
														if (max_differ_min)
															skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) < 0 : VALcmp(&(lval->data), &(rmax->data)) >= 0;
														else
															skip |= next->anti ? VALcmp(&(lval->data), &(rmax->data)) <= 0 : VALcmp(&(lval->data), &(rmax->data)) > 0;
														break;
													case cmp_lt:
														skip |= next->anti ? VALcmp(&(rmin->data), &(lval->data)) < 0 : VALcmp(&(rmin->data), &(lval->data)) >= 0;
														break;
													case cmp_lte:
														skip |= next->anti ? VALcmp(&(rmin->data), &(lval->data)) <= 0 : VALcmp(&(rmin->data), &(lval->data)) > 0;
														break;
													default:
														break;
												}
											}
										} else if (next->flag == cmp_in) {
											int nskip = 1;
											for (node *m = values->h; m && nskip; m = m->next) {
												atom *a = m->data;

												if (a->isnull)
													continue;
												nskip &= exp_range_overlap(rmin, rmax, a, a, false, max_differ_min) == 0;
											}
											skip |= nskip;
										}
									}
								}

								if (isListPartitionTable(mt) && (next->flag == cmp_equal || next->flag == cmp_in) && !next->anti) {
									/* if we find a value equal to one of the predicates, we don't prune */
									/* if the partition just holds null values, it will be skipped */
									if (!inlist) { /* initialize lazily */
										inlist = sa_list(v->sql->sa);
										for (node *m = pd->part.values->h; m; m = m->next) {
											sql_part_value *spv = (sql_part_value*) m->data;
											atom *pa = atom_general_ptr(v->sql->sa, &col->type, spv->value);

											list_append(inlist, pa);
										}
									}

									if (next->flag == cmp_equal) {
										int nskip = 1;
										for (node *m = inlist->h; m && nskip; m = m->next) {
											atom *pa = m->data;
											assert(!pa->isnull);
											nskip &= VALcmp(&(pa->data), &(lval->data)) != 0;
										}
										skip |= nskip;
									} else if (next->flag == cmp_in) {
										for (node *o = values->h; o && !skip; o = o->next) {
											atom *a = o->data;
											int nskip = 1;

											if (a->isnull)
												continue;
											for (node *m = inlist->h; m && nskip; m = m->next) {
												atom *pa = m->data;
												assert(!pa->isnull);
												nskip &= VALcmp(&(pa->data), &(a->data)) != 0;
											}
											skip |= nskip;
										}
									}
								}
							}
						}
					}
					i++;
				}
			}
		}
		if (!skip)
			append(tables, rel_rename_part(v->sql, rel_basetable(v->sql, pt, pt->base.name), mt_rel, mtalias));
	}
	if (list_empty(tables)) { /* No table passed the predicates, generate dummy relation */
		list *converted = sa_list(v->sql->sa);
		nrel = rel_project_exp(v->sql, exp_atom_bool(v->sql->sa, 1));
		nrel = rel_select(v->sql->sa, nrel, exp_atom_bool(v->sql->sa, 0));

		for (node *n = mt_rel->exps->h ; n ; n = n->next) {
			sql_exp *e = n->data, *a = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL));
			exp_prop_alias(v->sql->sa, a, e);
			list_append(converted, a);
		}
		nrel = rel_project(v->sql->sa, nrel, converted);
	} else { /* Unionize children tables */
		for (node *n = tables->h; n ; n = n->next) {
			sql_rel *next = n->data;
			sql_table *subt = (sql_table *) next->l;

			if (isMergeTable(subt)) { /* apply select predicate recursively for nested merge tables */
				if (!(next = merge_table_prune_and_unionize(v, next, info)))
					return NULL;
			} else if (info) { /* propagate select under union */
				next = rel_select(v->sql->sa, next, NULL);
				next->exps = exps_copy(v->sql, info->sel->exps);
			}

			if (nrel) {
				nrel = rel_setop(v->sql->sa, nrel, next, op_union);
				rel_setop_set_exps(v->sql, nrel, rel_projections(v->sql, mt_rel, NULL, 1, 1), true);
				set_processed(nrel);
			} else {
				nrel = next;
			}
		}
	}
	rel_destroy(mt_rel);
	return nrel;
}

/* rewrite merge tables into union of base tables */
static sql_rel *
rel_merge_table_rewrite(visitor *v, sql_rel *rel)
{
	if (is_modify(rel->op)) {
		sql_query *query = query_create(v->sql);
		return rel_propagate(query, rel, &v->changes);
	} else {
		sql_rel *bt = rel, *sel = NULL;

		if (is_select(rel->op)) {
			sel = rel;
			bt = rel->l;
		}
		if (is_basetable(bt->op) && rel_base_table(bt) && isMergeTable((sql_table*)bt->l)) {
			sql_table *mt = rel_base_table(bt);
			merge_table_prune_info *info = NULL;

			if (list_empty(mt->members)) /* in DDL statement cases skip if mergetable is empty */
				return rel;
			if (sel && !list_empty(sel->exps)) { /* prepare prunning information once */
				info = SA_NEW(v->sql->sa, merge_table_prune_info);
				*info = (merge_table_prune_info) {
					.cols = sa_list(v->sql->sa),
					.ranges = sa_list(v->sql->sa),
					.sel = sel
				};
				for (node *n = sel->exps->h; n; n = n->next) {
					sql_exp *e = n->data, *c = e->l;
					int flag = e->flag;

					if (e->type != e_cmp || (!is_theta_exp(flag) && flag != cmp_in) || is_symmetric(e) || !(c = rel_find_exp(rel, c)))
						continue;

					if (flag == cmp_gt || flag == cmp_gte || flag == cmp_lte || flag == cmp_lt || flag == cmp_equal) {
						sql_exp *l = e->r, *h = e->f;
						atom *lval = exp_flatten(v->sql, v->value_based_opt, l);
						atom *hval = h ? exp_flatten(v->sql, v->value_based_opt, h) : lval;

						if (lval && hval) {
							range_limit *next = SA_NEW(v->sql->sa, range_limit);

							*next = (range_limit) {
								.lval = lval,
								.hval = hval,
								.flag = flag,
								.anti = is_anti(e),
								.semantics = is_semantics(e),
							};
							list_append(info->cols, c);
							list_append(info->ranges, next);
						}
					}
					if (flag == cmp_in) { /* handle in lists */
						list *vals = e->r, *vlist = sa_list(v->sql->sa);

						node *m = NULL;
						for (m = vals->h; m; m = m->next) {
							sql_exp *l = m->data;
							atom *lval = exp_flatten(v->sql, v->value_based_opt, l);

							if (!lval)
								break;
							list_append(vlist, lval);
						}
						if (!m) {
							range_limit *next = SA_NEW(v->sql->sa, range_limit);

							*next = (range_limit) {
								.values = vlist, /* mark high as value list */
								.flag = flag,
								.anti = is_anti(e),
								.semantics = is_semantics(e),
							};
							list_append(info->cols, c);
							list_append(info->ranges, next);
						}
					}
				}
			}
			if (!(rel = merge_table_prune_and_unionize(v, bt, info)))
				return NULL;
			if (sel) {
				sel->l = NULL; /* The mt relation has already been destroyed */
				rel_destroy(sel);
			}
			v->changes++;
		}
	}
	return rel;
}

static inline bool
is_non_trivial_select_applied_to_outer_join(sql_rel *rel)
{
	return is_select(rel->op) && rel->exps && is_outerjoin(((sql_rel*) rel->l)->op);
}

extern list *list_append_before(list *l, node *n, void *data);

static void replace_column_references_with_nulls_2(mvc *sql, list* crefs, sql_exp* e);

static void
replace_column_references_with_nulls_1(mvc *sql, list* crefs, list* exps) {
    if (list_empty(exps))
        return;
    for(node* n = exps->h; n; n=n->next) {
        sql_exp* e = n->data;
        replace_column_references_with_nulls_2(sql, crefs, e);
    }
}

static void
replace_column_references_with_nulls_2(mvc *sql, list* crefs, sql_exp* e) {
	if (e == NULL) {
		return;
	}

	switch (e->type) {
	case e_column:
		{
			sql_exp *c = NULL;
			if (e->l) {
				c = exps_bind_column2(crefs, e->l, e->r, NULL);
			} else {
				c = exps_bind_column(crefs, e->r, NULL, NULL, 1);
			}
			if (c) {
				e->type = e_atom;
				e->l = atom_general(sql->sa, &e->tpe, NULL);
				e->r = e->f = NULL;
			}
		}
		break;
	case e_cmp:
		switch (e->flag) {
		case cmp_gt:
		case cmp_gte:
		case cmp_lte:
		case cmp_lt:
		case cmp_equal:
		case cmp_notequal:
		{
			sql_exp* l = e->l;
			sql_exp* r = e->r;
			sql_exp* f = e->f;

			replace_column_references_with_nulls_2(sql, crefs, l);
			replace_column_references_with_nulls_2(sql, crefs, r);
			replace_column_references_with_nulls_2(sql, crefs, f);
			break;
		}
		case cmp_filter:
		case cmp_or:
		{
			list* l = e->l;
			list* r = e->r;
			replace_column_references_with_nulls_1(sql, crefs, l);
			replace_column_references_with_nulls_1(sql, crefs, r);
			break;
		}
		case cmp_in:
		case cmp_notin:
		{
			sql_exp* l = e->l;
			list* r = e->r;
			replace_column_references_with_nulls_2(sql, crefs, l);
			replace_column_references_with_nulls_1(sql, crefs, r);
			break;
		}
		default:
			break;
		}
		break;
	case e_func:
	{
		list* l = e->l;
		replace_column_references_with_nulls_1(sql, crefs, l);
		break;
	}
	case e_convert:
	{
		sql_exp* l = e->l;
		replace_column_references_with_nulls_2(sql, crefs, l);
		break;
	}
	default:
		break;
	}
}

static sql_rel *
out2inner(visitor *v, sql_rel* sel, sql_rel* join, sql_rel* inner_join_side, operator_type new_type) {

	/* handle inner_join relations with a simple select */
	if (is_select(inner_join_side->op) && inner_join_side->l)
		inner_join_side = inner_join_side->l;
    if (!is_base(inner_join_side->op) && !is_simple_project(inner_join_side->op)) {
        // Nothing to do here.
        return sel;
    }

    list* inner_join_column_references = inner_join_side->exps;
    list* select_predicates = exps_copy(v->sql, sel->exps);

    for(node* n = select_predicates->h; n; n=n->next) {
        sql_exp* e = n->data;
        replace_column_references_with_nulls_2(v->sql, inner_join_column_references, e);

        if (exp_is_false(e)) {
            join->op = new_type;
            v->changes++;
            break;
        }
    }

    return sel;
}

static inline sql_rel *
rel_out2inner(visitor *v, sql_rel *rel) {

    if (!is_non_trivial_select_applied_to_outer_join(rel)) {
        // Nothing to do here.
        return rel;
    }

    sql_rel* join = (sql_rel*) rel->l;

    if (rel_is_ref(join)) {
        /* Do not alter a multi-referenced join relation.
         * This is problematic (e.g. in the case of the plan of a merge statement)
		 * basically because there are no guarantees on the other container relations.
		 * In particular there is no guarantee that the other referencing relations are
		 * select relations with null-rejacting predicates on the inner join side.
         */
        return rel;
    }

    sql_rel* inner_join_side;
    if (is_left(join->op)) {
        inner_join_side = join->r;
        return out2inner(v, rel, join, inner_join_side, op_join);
    }
    else if (is_right(join->op)) {
        inner_join_side = join->l;
        return out2inner(v, rel, join, inner_join_side, op_join);
    }
    else /*full outer join*/ {
        // First check if left side can degenerate from full outer join to just right outer join.
        inner_join_side = join->r;
        rel = out2inner(v, rel, join, inner_join_side, op_right);
        /* Now test if the right side can degenerate to
         * a normal inner join or a left outer join
         * depending on the result of previous call to out2inner.
         */

        inner_join_side = join->l;
        return out2inner(v, rel, join, inner_join_side, is_right(join->op)? op_join: op_left);
    }
}

static sql_rel*
exp_skip_output_parts(sql_rel *rel)
{
	while ((is_topn(rel->op) || is_project(rel->op) || is_sample(rel->op)) && rel->l) {
		if (is_union(rel->op) || (is_groupby(rel->op) && list_empty(rel->r)))
			return rel;			/* a group-by with no columns is a plain aggregate and hence always returns one row */
		rel = rel->l;
	}
	return rel;
}

/* return true if the given expression is guaranteed to have no rows */
static int
exp_is_zero_rows(visitor *v, sql_rel *rel, sql_rel *sel)
{
	if (!rel || mvc_highwater(v->sql))
		return 0;
	rel = exp_skip_output_parts(rel);
	if (is_select(rel->op) && rel->l) {
		sel = rel;
		rel = exp_skip_output_parts(rel->l);
	}

	sql_table *t = is_basetable(rel->op) && rel->l ? rel->l : NULL;
	bool table_readonly = t && isTable(t) && t->access == TABLE_READONLY;

	if (sel && !list_empty(sel->exps) && (v->value_based_opt || table_readonly)) {
		for (node *n = sel->exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			/* if the expression is false, then the select is empty */
			if (v->value_based_opt && (exp_is_false(e) || exp_is_null(e)))
				return 1;
			if (table_readonly && e->type == e_cmp && (e->flag == cmp_equal || e->f)) {
				/* half-ranges are theoretically optimizable here, but not implemented */
				sql_exp *c = e->l;
				if (c->type == e_column) {
					sql_exp *l = e->r;
					sql_exp *h = e->f;

					atom *lval = exp_flatten(v->sql, v->value_based_opt, l);
					atom *hval = h ? exp_flatten(v->sql, v->value_based_opt, h) : lval;
					if (lval && hval) {
						sql_rel *bt;
						sql_column *col = name_find_column(sel, exp_relname(c), exp_name(c), -2, &bt);
						void *min = NULL, *max = NULL;
						atom *amin, *amax;
						sql_subtype *ct = exp_subtype(c);

						if (col
							&& col->t == t
							&& sql_trans_ranges(v->sql->session->tr, col, &min, &max)
							&& min && max
							&& (amin = atom_general_ptr(v->sql->sa, ct, min)) && (amax = atom_general_ptr(v->sql->sa, ct, max))
							&& !exp_range_overlap(amin, amax, lval, hval, false, false)) {
							return 1;
						}
					}
				}
			}
		}
	}
	if ((is_innerjoin(rel->op) || is_left(rel->op) || is_right(rel->op) || is_semi(rel->op)) && !list_empty(rel->exps)) {
		sql_exp *je;

		/* check non overlaping pk-fk join */
		if ((je = rel_is_join_on_pkey(rel, true))) {
			int lpnr = rel_part_nr(rel->l, je);

			if (lpnr >= 0 && !rel_uses_part_nr(rel->r, je, lpnr))
				return 1;
		}
		return (((is_innerjoin(rel->op) || is_left(rel->op) || is_semi(rel->op)) && exp_is_zero_rows(v, rel->l, sel)) ||
			((is_innerjoin(rel->op) || is_right(rel->op)) && exp_is_zero_rows(v, rel->r, sel)));
	}
	/* global aggregates always return 1 row */
	if (is_simple_project(rel->op) || (is_groupby(rel->op) && !list_empty(rel->r)) || is_select(rel->op) ||
		is_topn(rel->op) || is_sample(rel->op) || is_inter(rel->op) || is_except(rel->op)) {
		if (rel->l)
			return exp_is_zero_rows(v, rel->l, sel);
	} else if (is_innerjoin(rel->op) && list_empty(rel->exps)) { /* cartesian product */
		return exp_is_zero_rows(v, rel->l, sel) || exp_is_zero_rows(v, rel->r, sel);
	}
	return 0;
}

/* discard sides of UNION or UNION ALL which cannot produce any rows, as per
statistics, similarly to the merge table optimizer, e.g.
	select * from a where x between 1 and 2 union all select * from b where x between 1 and 2
->	select * from b where x between 1 and 2   [assuming a has no rows with 1<=x<=2]
*/
static inline sql_rel *
rel_remove_union_partitions(visitor *v, sql_rel *rel)
{
	if (!is_union(rel->op) || rel_is_ref(rel))
		return rel;
	int left_zero_rows = !rel_is_ref(rel->l) && exp_is_zero_rows(v, rel->l, NULL);
	int right_zero_rows = !rel_is_ref(rel->r) && exp_is_zero_rows(v, rel->r, NULL);

	if (left_zero_rows && right_zero_rows) {
		/* generate dummy relation */
		list *converted = sa_list(v->sql->sa);
		sql_rel *nrel = rel_project_exp(v->sql, exp_atom_bool(v->sql->sa, 1));
		nrel = rel_select(v->sql->sa, nrel, exp_atom_bool(v->sql->sa, 0));

		for (node *n = rel->exps->h ; n ; n = n->next) {
			sql_exp *e = n->data, *a = exp_atom(v->sql->sa, atom_general(v->sql->sa, exp_subtype(e), NULL));
			exp_prop_alias(v->sql->sa, a, e);
			list_append(converted, a);
		}
		rel_destroy(rel);
		v->changes++;
		return rel_project(v->sql->sa, nrel, converted);
	} else if (left_zero_rows) {
		sql_rel *r = rel->r;
		if (!is_project(r->op))
			r = rel_project(v->sql->sa, r, rel_projections(v->sql, r, NULL, 1, 1));
		rel_rename_exps(v->sql, rel->exps, r->exps);
		rel->r = NULL;
		rel_destroy(rel);
		v->changes++;
		return r;
	} else if (right_zero_rows) {
		sql_rel *l = rel->l;
		if (!is_project(l->op))
			l = rel_project(v->sql->sa, l, rel_projections(v->sql, l, NULL, 1, 1));
		rel_rename_exps(v->sql, rel->exps, l->exps);
		rel->l = NULL;
		rel_destroy(rel);
		v->changes++;
		return l;
	}
	return rel;
}

static sql_rel *
rel_first_level_optimizations(visitor *v, sql_rel *rel)
{
	/* rel_simplify_math optimizer requires to clear the hash, so make sure it runs last in this batch */
	if (v->value_based_opt)
		rel = rel_simplify_math(v, rel);
	return rel;
}

/* pack optimizers into a single function call to avoid iterations in the AST */
static sql_rel *
rel_optimize_select_and_joins_bottomup(visitor *v, sql_rel *rel)
{
	if (!rel || (!is_join(rel->op) && !is_semi(rel->op) && !is_select(rel->op)) || list_empty(rel->exps))
		return rel;
	int level = *(int*) v->data;

	if (rel)
		rel->exps = exp_merge_range(v, rel, rel->exps);
	if (v->value_based_opt)
		rel = rel_reduce_casts(v, rel);
	rel = rel_select_cse(v, rel);
	if (level == 1)
		rel = rel_merge_select_rse(v, rel);
	if (v->value_based_opt && level <= 1)
		rel = rel_simplify_like_select(v, rel);
	rel = rewrite_simplify(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_unions_bottomup(visitor *v, sql_rel *rel)
{
	rel = rel_remove_union_partitions(v, rel);
	rel = rel_merge_union(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_unions_topdown(visitor *v, sql_rel *rel)
{
	rel = rel_push_project_down_union(v, rel);
	rel = rel_push_join_down_union(v, rel);
	return rel;
}

static inline sql_rel *
rel_basecount(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !rel_is_ref(rel) && rel->l && list_empty(rel->r) &&
		list_length(rel->exps) == 1 && exp_aggr_is_count(rel->exps->h->data)) {
		sql_rel *bt = rel->l;
		sql_exp *e = rel->exps->h->data;
		if (is_basetable(bt->op) && list_empty(e->l)) { /* count(*) */
			/* change into select cnt('schema','table') */
			sql_table *t = bt->l;
			/* I need to get the declared table's frame number to make this work correctly for those */
			if (!isTable(t) || isDeclaredTable(t))
				return rel;
			sql_subfunc *cf = sql_bind_func(v->sql, "sys", "cnt", sql_bind_localtype("str"), sql_bind_localtype("str"), F_FUNC);
			list *exps = sa_list(v->sql->sa);
			append(exps, exp_atom_str(v->sql->sa, t->s->base.name, sql_bind_localtype("str")));
			append(exps, exp_atom_str(v->sql->sa, t->base.name, sql_bind_localtype("str")));
			sql_exp *ne = exp_op(v->sql->sa, exps, cf);

			ne = exp_propagate(v->sql->sa, ne, e);
			exp_setname(v->sql->sa, ne, exp_find_rel_name(e), exp_name(e));
			rel_destroy(rel);
			rel = rel_project(v->sql->sa, NULL, append(sa_list(v->sql->sa), ne));
			v->changes++;
		}
	}
	return rel;
}

static inline sql_rel *
rel_simplify_count(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) && !list_empty(rel->exps)) {
		mvc *sql = v->sql;
		int ncountstar = 0;

		/* Convert count(no null) into count(*) */
		for (node *n = rel->exps->h; n ; n = n->next) {
			sql_exp *e = n->data;

			if (exp_aggr_is_count(e) && !need_distinct(e)) {
				if (list_length(e->l) == 0) {
					ncountstar++;
				} else if (list_length(e->l) == 1 && exp_is_not_null((sql_exp*)((list*)e->l)->h->data)) {
					sql_subfunc *cf = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR);
					sql_exp *ne = exp_aggr(sql->sa, NULL, cf, 0, 0, e->card, 0);
					if (exp_name(e))
						exp_prop_alias(sql->sa, ne, e);
					n->data = ne;
					ncountstar++;
					v->changes++;
				}
			}
		}
		/* With multiple count(*), use exp_ref to reduce the number of calls to this aggregate */
		if (ncountstar > 1) {
			sql_exp *count_star = NULL;
			for (node *n = rel->exps->h; n ; n = n->next) {
				sql_exp *e = n->data;

				if (exp_aggr_is_count(e) && !need_distinct(e) && list_length(e->l) == 0) {
					if (!count_star) {
						count_star = e;
					} else {
						sql_exp *ne = exp_ref(sql, count_star);
						if (exp_name(e))
							exp_prop_alias(sql->sa, ne, e);
						n->data = ne;
						v->changes++;
					}
				}
			}
		}
	}
	return rel;
}

static sql_rel *
rel_optimize_projections(visitor *v, sql_rel *rel)
{
	rel = rel_project_cse(v, rel);

	if (!rel || !is_groupby(rel->op))
		return rel;

	rel = rel_push_aggr_down(v, rel);
	rel = rel_push_groupby_down(v, rel);
	rel = rel_groupby_order(v, rel);
	rel = rel_reduce_groupby_exps(v, rel);
	rel = rel_distinct_aggregate_on_unique_values(v, rel);
	rel = rel_groupby_distinct(v, rel);
	rel = rel_push_count_down(v, rel);
	/* only when value_based_opt is on, ie not for dependency resolution */
	if (v->value_based_opt) {
		rel = rel_simplify_count(v, rel);
		rel = rel_basecount(v, rel);
	}
	return rel;
}

static sql_rel *
rel_optimize_joins(visitor *v, sql_rel *rel)
{
	rel = rel_push_join_exps_down(v, rel);
	rel = rel_out2inner(v, rel);
	rel = rel_join2semijoin(v, rel);
	rel = rel_push_join_down_outer(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_semi_and_anti(visitor *v, sql_rel *rel)
{
	/* rewrite semijoin (A, join(A,B)) into semijoin (A,B) */
	if (rel && is_semi(rel->op))
		rel = rel_rewrite_semijoin(v, rel);
	/* push semijoin through join */
	if (rel && (is_semi(rel->op) || is_innerjoin(rel->op)))
		rel = rel_push_semijoin_down_or_up(v, rel);
	/* antijoin(a, union(b,c)) -> antijoin(antijoin(a,b), c) */
	if (rel && rel->op == op_anti)
		rel = rel_rewrite_antijoin(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_select_and_joins_topdown(visitor *v, sql_rel *rel)
{
	/* push_join_down introduces semijoins */
	int level = *(int*) v->data;
	if (level <= 0)
		rel = rel_push_join_down(v, rel);

	rel = rel_simplify_fk_joins(v, rel);
	rel = rel_push_select_down(v, rel);
	if (rel && rel->l && (is_select(rel->op) || is_join(rel->op)))
		rel = rel_use_index(v, rel);

	rel = rel_select_order(v, rel);
	return rel;
}

static sql_rel *
rel_push_func_and_select_down(visitor *v, sql_rel *rel)
{
	if (rel)
		rel = rel_push_func_down(v, rel);
	if (rel)
		rel = rel_push_select_down(v, rel);
	return rel;
}

static sql_rel *
optimize_rel(visitor *v, sql_rel *rel, global_props *gp)
{
	int level = *(int*)v->data;

	TRC_DEBUG_IF(SQL_REWRITER) {
		int i;
		for (i = 0; i < ddl_maxops; i++) {
			if (gp->cnt[i]> 0)
				TRC_DEBUG_ENDIF(SQL_REWRITER, "%s %d\n", op2string((operator_type)i), gp->cnt[i]);
		}
	}

	if (level <= 0 && gp->cnt[op_select])
		rel = rel_split_select(v, rel, 1);

	/* simple merging of projects */
	if (gp->cnt[op_project] || gp->cnt[op_groupby] || gp->cnt[op_ddl]) {
		rel = rel_visitor_bottomup(v, rel, &rel_push_project_down);
		rel = rel_visitor_bottomup(v, rel, &rel_merge_projects);

		/* push (simple renaming) projections up */
		if (gp->cnt[op_project])
			rel = rel_visitor_bottomup(v, rel, &rel_push_project_up);
		if (level <= 0 && (gp->cnt[op_project] || gp->cnt[op_groupby]))
			rel = rel_split_project(v, rel, 1);
		if (level <= 0) {
			if (gp->cnt[op_left] || gp->cnt[op_right] || gp->cnt[op_full] || gp->cnt[op_join] || gp->cnt[op_semi] || gp->cnt[op_anti])
				rel = rel_visitor_bottomup(v, rel, &rel_remove_redundant_join); /* this optimizer has to run before rel_first_level_optimizations */
			rel = rel_visitor_bottomup(v, rel, &rel_first_level_optimizations);
		}
	}

	if (level <= 1 && v->value_based_opt)
		rel = rel_exp_visitor_bottomup(v, rel, &rel_simplify_predicates, false);

	/* join's/crossproducts between a relation and a constant (row).
	 * could be rewritten
	 *
	 * also joins between a relation and a DICT (which isn't used)
	 * could be removed.
	 * */
	if (gp->cnt[op_join] && gp->cnt[op_project] && /* DISABLES CODE */ (0))
		rel = rel_visitor_bottomup(v, rel, &rel_remove_join);

	if (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right] || gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti] || gp->cnt[op_select]) {
		rel = rel_visitor_bottomup(v, rel, &rel_optimize_select_and_joins_bottomup);
		if (level == 1)
			rel = rel_visitor_bottomup(v, rel, &rewrite_reset_used); /* reset used flag, used by rel_merge_select_rse */
	}

	if (v->value_based_opt)
		rel = rel_project_reduce_casts(v, rel);

	if (gp->cnt[op_union])
		rel = rel_visitor_bottomup(v, rel, &rel_optimize_unions_bottomup);

	if ((gp->cnt[op_left] || gp->cnt[op_right] || gp->cnt[op_full]) && /* DISABLES CODE */ (0))
		rel = rel_visitor_topdown(v, rel, &rel_split_outerjoin);

	if (level <= 1 && gp->cnt[op_project])
		rel = rel_exp_visitor_bottomup(v, rel, &rel_merge_project_rse, false);

	if (gp->cnt[op_groupby] || gp->cnt[op_project] || gp->cnt[op_union] || gp->cnt[op_inter] || gp->cnt[op_except])
		rel = rel_visitor_topdown(v, rel, &rel_optimize_projections);

	if (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right] || gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti]) {
		rel = rel_visitor_topdown(v, rel, &rel_optimize_joins);
		if (!gp->cnt[op_update])
			rel = rel_join_order(v, rel);
	}

	/* Important -> Re-write semijoins after rel_join_order */
	if (gp->cnt[op_anti] || gp->cnt[op_semi]) {
		rel = rel_visitor_bottomup(v, rel, &rel_optimize_semi_and_anti);
		if (level <= 0)
			rel = rel_visitor_topdown(v, rel, &rel_semijoin_use_fk);
	}

	/* Important -> Make sure rel_push_select_down gets called after rel_join_order,
	   because pushing down select expressions makes rel_join_order more difficult */
	if (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right] || gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti] || gp->cnt[op_select])
		rel = rel_visitor_topdown(v, rel, &rel_optimize_select_and_joins_topdown);

	if (gp->cnt[op_union])
		rel = rel_visitor_topdown(v, rel, &rel_optimize_unions_topdown);

	/* Remove unused expressions */
	if (level <= 0)
		rel = rel_dce(v->sql, rel);

	if (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right] || gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti] || gp->cnt[op_select])
		rel = rel_visitor_topdown(v, rel, &rel_push_func_and_select_down);

	if (gp->cnt[op_topn] || gp->cnt[op_sample])
		rel = rel_visitor_topdown(v, rel, &rel_push_topn_and_sample_down);

	if (level == 0 && gp->needs_distinct && gp->cnt[op_project])
		rel = rel_visitor_bottomup(v, rel, &rel_distinct_project2groupby);

	/* Some merge table rewrites require two tree iterations. Later I could improve this to use only one iteration */
	if (gp->needs_mergetable_rewrite)
		rel = rel_visitor_topdown(v, rel, &rel_merge_table_rewrite);

	return rel;
}

/* make sure the outer project (without order by or distinct) has all the aliases */
static sql_rel *
rel_keep_renames(mvc *sql, sql_rel *rel)
{
	if (!rel || !is_simple_project(rel->op) || (!rel->r && !need_distinct(rel)) || list_length(rel->exps) <= 1)
		return rel;

	int needed = 0;
	for(node *n = rel->exps->h; n && !needed; n = n->next) {
		sql_exp *e = n->data;

		if (exp_name(e) && (e->type != e_column || strcmp(exp_name(e), e->r) != 0))
			needed = 1;
	}
	if (!needed)
		return rel;

	list *new_outer_exps = sa_list(sql->sa);
	list *new_inner_exps = sa_list(sql->sa);
	for(node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data, *ie, *oe;
		const char *rname = exp_relname(e);
		const char *name = exp_name(e);

		exp_label(sql->sa, e, ++sql->label);
		ie = e;
		oe = exp_ref(sql, ie);
		exp_setname(sql->sa, oe, rname, name);
		append(new_inner_exps, ie);
		append(new_outer_exps, oe);
	}
	rel->exps = new_inner_exps;
	rel = rel_project(sql->sa, rel, new_outer_exps);
	return rel;
}

/* if only merge table rewrite is needed return 1, everything return 2, nothing 0 */
static int
need_optimization(mvc *sql, sql_rel *rel, int instantiate)
{
	if (rel && rel->card <= CARD_ATOM) {
		if (is_insert(rel->op)) {
			int opt = 0;
			sql_rel *l = (sql_rel *) rel->l;

			if (is_basetable(l->op) && instantiate) {
				sql_table *t = (sql_table *) l->l;
				sql_part *pt;

				/* I don't expect inserts on remote or replica tables yet */
				assert(!isRemote(t) && !isReplicaTable(t));
				/* If the plan has a merge table or a child of a partitioned one, then optimization cannot be skipped */
				if (isMergeTable(t) || (t->s && t->s->parts && (pt = partition_find_part(sql->session->tr, t, NULL))))
					opt = 1;
			}
			return rel->r ? MAX(need_optimization(sql, rel->r, instantiate), opt) : opt;
		}
		if (is_simple_project(rel->op))
			return rel->l ? need_optimization(sql, rel->l, instantiate) : 0;
	}
	return 2;
}

/* 'instantiate' means to rewrite logical tables: (merge, remote, replica tables) */
sql_rel *
rel_optimizer(mvc *sql, sql_rel *rel, int instantiate, int value_based_opt, int storage_based_opt)
{
	int level = 0, opt = 0;
	visitor v = { .sql = sql, .value_based_opt = value_based_opt, .storage_based_opt = storage_based_opt, .data = &level, .changes = 1 };
	global_props gp = (global_props) {.cnt = {0}, .instantiate = (uint8_t)instantiate};

	rel = rel_keep_renames(sql, rel);
	if (!rel || !(opt = need_optimization(sql, rel, instantiate)))
		return rel;

	for( ;rel && level < 20 && v.changes; level++) {
		v.changes = 0;
		gp = (global_props) {.cnt = {0}, .instantiate = (uint8_t)instantiate};
		v.data = &gp;
		rel = rel_visitor_topdown(&v, rel, &rel_properties); /* collect relational tree properties */
		v.data = &level;
		if (opt == 2) {
			rel = optimize_rel(&v, rel, &gp);
		} else if (gp.needs_mergetable_rewrite) { /* the merge table rewriter may have to run */
			rel = rel_visitor_topdown(&v, rel, &rel_merge_table_rewrite);
		}
	}
#ifndef NDEBUG
	assert(level < 20);
#endif
	/* Run the following optimizers only once at the end to avoid an infinite optimization loop */
	if (opt == 2)
		rel = rel_visitor_bottomup(&v, rel, &rel_push_select_up);

	/* merge table rewrites may introduce remote or replica tables */
	if (gp.needs_mergetable_rewrite || gp.needs_remote_replica_rewrite) {
		v.data = NULL;
		rel = rel_visitor_bottomup(&v, rel, &rel_rewrite_remote);
		v.data = NULL;
		rel = rel_visitor_bottomup(&v, rel, &rel_rewrite_replica);
		v.data = &level;
		rel = rel_visitor_bottomup(&v, rel, &rel_remote_func);
	}
	return rel;
}
