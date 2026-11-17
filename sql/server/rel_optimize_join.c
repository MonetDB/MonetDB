
#include "monetdb_config.h"
#include "rel_optimizer_private.h"
#include "rel_statistics.h"
//#include "rel_basetable.h"
#include "rel_rewriter.h"
//#include "sql_storage.h"

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

static sql_rel *
remove_blocking_selects( mvc *sql, sql_rel *p, sql_rel *rel)
{
	if (rel_is_ref(rel))
		return rel;

	switch(rel->op) {
	case op_select:
		if (p) {
			sql_rel *l = rel->l;
			if (is_single(rel) || l->op != op_join)
				return rel;
			if (!list_empty(rel->exps)) {
				p->exps = list_join(p->exps, rel->exps);
				rel->exps = NULL;
			}
			if (list_empty(rel->exps)) {
				sql_rel *l = rel->l;
				rel->l = NULL;
				rel_destroy(sql, rel);
				return remove_blocking_selects( sql, p, l);
			}
		}
		return rel;
	case op_join:
		rel->l = remove_blocking_selects(sql, rel, rel->l);
		rel->r = remove_blocking_selects(sql, rel, rel->r);
		return rel;
	default:
		return rel;
	}
}

static void
get_relations(visitor *v, sql_rel *rel, list *rels)
{
	if (list_empty(rel->attr) && !rel_is_ref(rel) && rel->op == op_join && rel->exps == NULL && !is_single(rel)) {
		sql_rel *l = rel->l;
		sql_rel *r = rel->r;

		get_relations(v, l, rels);
		get_relations(v, r, rels);
		rel->l = NULL;
		rel->r = NULL;
		rel_destroy(v->sql, rel);
	} else {
		append(rels, rel);
	}
}

static sql_rel *
rel_estimate(visitor *v, sql_rel *rel)
{
	/* Don't prune updates as pruning will possibly result in removing the joins which therefore cannot be used for constraint checking */
	sql_rel *l = rel->l, *r = rel->r;

	/*
	if (is_join(rel->op) || is_semi(rel->op)) {
		rel->l = rel_estimate(v, rel->l);
		rel->r = rel_estimate(v, rel->r);
	}
	if (is_join(rel->op))
		rel->exps = exps_exp_visitor_bottomup(v, rel, rel->exps, 0, &rel_propagate_statistics, false);
	*/

	switch (rel->op) {
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	{
		/* propagate row count */
		BUN lv = get_rel_count(l), rv = get_rel_count(r), r_uniques_estimate = BUN_MAX, join_idx_estimate = BUN_MAX;

		//assert (lv < 1000000000 && rv < 1000000000);
		if (!list_empty(rel->exps) && !is_single(rel) && rel->op == op_join) {
			for (node *n = rel->exps->h ; n ; n = n->next) {
				sql_exp *e = n->data, *el = e->l, *er = e->r;
				BUN uniques_estimate = BUN_MAX;

				if (find_prop(e->p, PROP_JOINIDX)) {
					prop *lsp = find_prop(l->p, PROP_SELECTIVITY), *rsp = find_prop(r->p, PROP_SELECTIVITY);
					dbl ls = lsp ? lsp->value.dval : 1.0;
					dbl rs = rsp ? rsp->value.dval : 1.0;
					join_idx_estimate = lv>rv?lv:rv;
					join_idx_estimate = (BUN)(join_idx_estimate * ls * rs);
					if (join_idx_estimate < 1)
						join_idx_estimate = 1;
					dbl s = ((dbl)join_idx_estimate/(lv*rv));
					prop *sp = prop_create(v->sql->sa, PROP_SELECTIVITY, (prop *) e->p);
					sp->value.dval = s;
					e->p = sp;
				} else if (e->type == e_cmp && e->flag == cmp_equal) {
					/* if one of the sides is unique, the cardinality will be at most the cardinality of the other side */
					if (!is_semantics(e) || !has_nil(el) || !has_nil(er)) {
						BUN lu = 1, ru = 1; /* initialize with worst case */
						prop *p = NULL;
						if ((p = find_prop(el->p, PROP_NUNIQUES)))
							lu = (BUN) p->value.dval;
						if ((p = find_prop(er->p, PROP_NUNIQUES)))
							ru = (BUN) p->value.dval;
						if (is_unique(el) || lu > lv) {
							BUN ncount = (is_right(rel->op) || is_full(rel->op) || !lv || lu < lv) ? MAX(lv, rv) : rv/(lu/lv);
							if (ncount == 0)
								ncount = MAX(lv, rv);
							uniques_estimate = MIN(uniques_estimate, ncount);
						} else if (is_unique(er) || ru > rv) {
							BUN ncount = (is_left(rel->op) || is_full(rel->op) || !rv || ru < rv) ? MAX(lv, rv) : lv/(ru/rv);
							if (ncount == 0)
								ncount = MAX(lv, rv);
							uniques_estimate = MIN(uniques_estimate, ncount);
						}
						if (uniques_estimate < BUN_MAX) {
							if (uniques_estimate < r_uniques_estimate)
								r_uniques_estimate = uniques_estimate;
							dbl s = ((dbl)uniques_estimate/(lv*rv));
							prop *sp = prop_create(v->sql->sa, PROP_SELECTIVITY, (prop *) e->p);
							sp->value.dval = s;
							e->p = sp;
						} else {
							dbl mfl = (dbl)lv/lu;
							dbl mfr = (dbl)rv/ru;
							dbl mf = MIN(lv/mfl, rv/mfr);
							dbl u = mf * mfl * mfr;
							dbl s = u/(lv*rv);
							prop *sp = prop_create(v->sql->sa, PROP_SELECTIVITY, (prop *) e->p);
							sp->value.dval = s;
							e->p = sp;
						}
					}
				}
			}
		}
		if (is_single(rel) || is_left(rel->op)) {
			set_count_prop(v->sql->sa, rel, lv);
		} else if (is_right(rel->op)) {
			set_count_prop(v->sql->sa, rel, rv);
		} else if (is_full(rel->op)) {
			set_count_prop(v->sql->sa, rel, MAX(lv,rv));
		} else if (join_idx_estimate < BUN_MAX) {
			set_count_prop(v->sql->sa, rel, join_idx_estimate);
		} else if (r_uniques_estimate < BUN_MAX) {
			set_count_prop(v->sql->sa, rel, r_uniques_estimate);
		} else if (list_length(rel->exps) == 1 && (exp_is_false(rel->exps->h->data) || exp_is_null(rel->exps->h->data))) {
			/* corner cases for outer joins */
			if (is_left(rel->op)) {
				set_count_prop(v->sql->sa, rel, lv);
			} else if (is_right(rel->op)) {
				set_count_prop(v->sql->sa, rel, rv);
			} else if (is_full(rel->op) && lv != BUN_NONE && rv != BUN_NONE) {
				set_count_prop(v->sql->sa, rel, (rv > (BUN_MAX - lv)) ? BUN_MAX : (lv + rv)); /* overflow check */
			} else if (lv != BUN_NONE && rv != BUN_NONE) {
				set_count_prop(v->sql->sa, rel, 0);
			}
		} else if (lv == 0) {
			set_count_prop(v->sql->sa, rel, (is_right(rel->op) || is_full(rel->op)) ? rv : 0);
		} else if (rv == 0) {
			set_count_prop(v->sql->sa, rel, (is_left(rel->op) || is_full(rel->op)) ? lv : 0);
		} else if (lv != BUN_NONE && rv != BUN_NONE) {
			dbl sel = 1.0;
			if (!list_empty(rel->exps)) {
				for (node *n = rel->exps->h ; n ; n = n->next) {
					sql_exp *e = n->data;
					prop *sp = NULL;
					if ((sp = find_prop(e->p, PROP_SELECTIVITY)) == NULL) {
						dbl s = exp_estimate_selectivity(v->sql, e, rel);
						/* store selectivity on predicate for later use */
						sp = prop_create(v->sql->sa, PROP_SELECTIVITY, e->p);
						sp->value.dval = s;
						e->p = sp;
					}
					sel *= sp->value.dval;
				}
			}
			dbl est = (dbl)lv * (dbl)rv * sel;
			if (est <= 1)
				est = 2;
			set_count_prop(v->sql->sa, rel, est > (dbl)BUN_MAX ? BUN_MAX : (BUN)est);
			prop *sp = NULL;
			if ((sp = find_prop(rel->p, PROP_SELECTIVITY)) == NULL) {
				/* store selectivity on predicate for later use */
				rel->p = sp = prop_create(v->sql->sa, PROP_SELECTIVITY, rel->p);
				sp->value.dval = sel;
			}
		}
	}	break;
	case op_anti:
		set_count_prop(v->sql->sa, rel, get_rel_count(l));
		break;
	case op_semi:
		if (list_length(rel->exps) == 1 && (exp_is_false(rel->exps->h->data) || exp_is_null(rel->exps->h->data))) {
			set_count_prop(v->sql->sa, rel, 0);
		} else {
			if (!list_empty(rel->exps) && !is_single(rel)) {
				BUN cnt = get_rel_count(l);
				dbl sel = 1.0;
				for (node *n = rel->exps->h ; n ; n = n->next) {
					sql_exp *e = n->data;
					dbl s = exp_estimate_selectivity(v->sql, e, rel);
					/* store selectivity on predicate for later use */
					prop *sp = prop_create(v->sql->sa, PROP_SELECTIVITY, (prop *) e->p);
					sp->value.dval = s;
					e->p = sp;
					sel *= s;
				}
				BUN est = cnt == 0 ? 0 : (BUN)((dbl)cnt * sel);
				if (est > cnt)
					est = cnt;
				if (est <= 1 && cnt > 0)
					est = 2;
				set_count_prop(v->sql->sa, rel, est);
				prop *sp = NULL;
				if ((sp = find_prop(rel->p, PROP_SELECTIVITY)) == NULL) {
					/* store selectivity on predicate for later use */
					rel->p = sp = prop_create(v->sql->sa, PROP_SELECTIVITY, rel->p);
					sp->value.dval = sel;
				}
			} else {
				set_count_prop(v->sql->sa, rel, get_rel_count(l));
			}
		}
		break;
	case op_groupby:
		if (!list_empty(rel->r) && rel->l) {
			BUN current = get_rel_count(rel), lower = get_rel_count(rel->l);
			if (lower < current)
				set_count_prop(v->sql->sa, rel, lower);
		}
		break;
	default:
		break;
	}
	return rel;
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
		*cnt += 1000;
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
		case cmp_con:
		case cmp_dis: {
						  /*
			list *l = e->l;
			int c = 9 - 10*list_length(l);
			*cnt += c;
			return c;
			*/
			*cnt += 3;
			return 3;
		}
		case cmp_in:
		case cmp_notin: {
			list *l = e->r;
			int c = 9 - 10*list_length(l);
			*cnt += c;
			return c;
		}
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

int
exp_keyvalue(sql_exp *e)
{
	int cnt = 0;
	exp_count(&cnt, e);
	return cnt;
}

extern void _exp_print(mvc *sql, sql_exp *e);
extern void _exps_print(mvc *sql, list *l);
extern void _rel_print(mvc *sql, sql_rel *rel);

static sql_exp *
find_unique(sql_exp *e, sql_rel *rel)
{
	if (is_select(rel->op) || is_basetable(rel->op) || is_simple_project(rel->op))
		return rel_find_exp(rel, e);
	if (is_join(rel->op)) {
		if (rel->lunique)
			return find_unique(e, rel->l);
		if (rel->runique)
			return find_unique(e, rel->r);
	}
	return NULL;
}

static void
exp_set_uniques(sql_exp *e, sql_rel *l, sql_rel *r)
{
	if (e->type == e_cmp) {
		if (e->flag == cmp_equal || e->flag == cmp_notequal) {
			exp_set_uniques(e->l, l, r);
			exp_set_uniques(e->r, l, r);
		}
	} else if (e->type == e_column) {
		sql_exp *be = find_unique(e, l);
		if (!be)
			be = find_unique(e, r);
		if (be && is_unique(be))
			set_unique(e);
		else
			set_not_unique(e);
	}
}


static prop *
exp_selectivity(visitor *v, sql_exp *e, sql_rel *l, sql_rel *r, prop *p)
{
	BUN lv = get_rel_count(l), rv = get_rel_count(r), join_idx_estimate = BUN_MAX;
	sql_exp *el = e->l, *er = e->r;
	BUN uniques_estimate = BUN_MAX;
	prop *jip = find_prop(e->p, PROP_JOINIDX);
	bool runique = true;

	if (jip) {
		/* check if TID is still unique in the join tree */
		sql_exp *be = find_unique(e->r, r);
		if (!be || !is_unique(be)) {
			jip = NULL;
			runique = false;
		}
	}
	if (jip) {
		join_idx_estimate = lv>rv ? lv : rv;

		prop *rp = lv>rv ? find_prop(r->p, PROP_SELECTIVITY) : find_prop(l->p, PROP_SELECTIVITY);
		dbl sel = rp ? rp->value.dval : 1;
		dbl s = ((dbl)join_idx_estimate/(lv*rv)) * sel;
		if (!p)
			p = prop_create(v->sql->sa, PROP_SELECTIVITY, (prop *) e->p);
		p->value.dval = s;
		e->p = p;
	} else if (e->type == e_cmp && e->flag == cmp_equal) {
		/* if one of the sides is unique, the cardinality will be at most the cardinality of the other side */
		if (!is_semantics(e) || !has_nil(el) || !has_nil(er)) {
			BUN lu = 1, ru = 1; /* initialize with worst case */
			prop *up = NULL;
			if (el->type == e_column && (is_select(l->op) || is_basetable(l->op)))
				el = rel_find_exp(l, el);
			if ((up = find_prop(el->p, PROP_NUNIQUES)))
				lu = MIN((BUN) up->value.dval, lv);
			if (er->type == e_column && (is_select(r->op) || is_basetable(r->op)))
				er = rel_find_exp(r, er);
			if ((up = find_prop(er->p, PROP_NUNIQUES)))
				ru = MIN((BUN) up->value.dval, rv);
			if (is_unique(el) || (is_unique(er) && runique))
				uniques_estimate = MIN(uniques_estimate, MAX(lv, rv));
			if (is_unique(el) && is_unique(er) && runique)
				uniques_estimate = MIN(lu, ru);
			if (uniques_estimate != BUN_MAX) {
				dbl s = ((dbl)uniques_estimate/(lv*rv));
				if (!p)
					p = prop_create(v->sql->sa, PROP_SELECTIVITY, (prop *) e->p);
				p->value.dval = s;
				e->p = p;
			} else {
				dbl mfl = (dbl)lv/lu;
				dbl mfr = (dbl)rv/ru;
				dbl mf = MIN(lv/mfl, rv/mfr);
				dbl u = mf * mfl * mfr;
				dbl s = u/(lv*rv);
				if (!p)
					p = prop_create(v->sql->sa, PROP_SELECTIVITY, (prop *) e->p);
				p->value.dval = s;
				e->p = p;
			}
		}
	} else {
		dbl s = exp_estimate_selectivity(v->sql, e, l);
		/* store selectivity on predicate for later use */
		if (!p)
			p = prop_create(v->sql->sa, PROP_SELECTIVITY, e->p);
		p->value.dval = s;
		e->p = p;
	}
	return p;
}

static int VERBOSE = 0;

static dbl
exp_cost(visitor *v, sql_exp *e, ulng max, sql_rel *l, sql_rel *r)
{
	dbl sel = 1.0, cost = 0;
	prop *p = find_prop(e->p, PROP_SELECTIVITY);

	if (!l || !r)
		return 0;
	exp_set_uniques(e, l, r);
	p = exp_selectivity(v, e, l, r, p);
	if (p)
		sel = p->value.dval;
	if (e->type == e_cmp && !is_complex_exp(e->flag)) { /* todo other cases !! */
		BUN lcard = 1, rcard = 1;
		if (l) {
			lcard = get_rel_count(l);
			if (lcard == BUN_NONE)
				lcard = (BUN)max;
		}
		if (r) {
			rcard = get_rel_count(r);
			if (rcard == BUN_NONE)
				rcard = (BUN)max;
		}
		BUN n = l->nrcols + r->nrcols; /* not correct includes join exps */
		cost = ( (dbl)lcard + (dbl)rcard + (lcard*rcard * sel * n * 8) );
		if (VERBOSE) {
			printf("cost %F %F " BUNFMT " " BUNFMT " ", cost, sel, lcard, rcard);
			_exp_print(v->sql, e);
		}
	}
	return cost;
}

static dbl
exps_cost(visitor *v, list *exps, ulng max, sql_rel *l, sql_rel *r)
{
	dbl cost = 0;
	for(node *n = exps->h; n; n=n->next)
		cost += exp_cost(v, n->data, max, l, r);
	return cost;
}

static list *
order_join_expressions_pp(mvc *sql, list *dje, list *rels)
{
	node *n;
	int cnt = list_length(dje);

	if (cnt <= 1)
		return dje;

	allocator *ta = MT_thread_getallocator();
	list *res = sa_list(sql->sa);
	int i;
	dbl *keys = SA_NEW_ARRAY(ta, dbl, cnt);
	void **data = SA_NEW_ARRAY(ta, void*, cnt);

	ulng max = 0;
	for (n = rels->h; n; n = n->next) {
		sql_rel *rel = n->data;
		BUN card = get_rel_count(rel);
		if (card != BUN_NONE && max < (ulng)card)
			max = (ulng)card;
	}
	for (n = dje->h, i = 0; n; n = n->next, i++) {
		sql_exp *e = n->data;
		dbl sel = 1.0;
		prop *p = find_prop(e->p, PROP_SELECTIVITY);

		if (p)
			sel = p->value.dval;
		if (e->type == e_cmp && !is_complex_exp(e->flag)) { /* todo other cases !! */
			sql_rel *l = find_rel(rels, e->l);
			sql_rel *r = find_rel(rels, e->r);
			BUN lcard = 1, rcard = 1;
			if (l) {
				lcard = get_rel_count(l);
				if (lcard == BUN_NONE)
					lcard = (BUN)max;
			}
			if (r) {
				rcard = get_rel_count(r);
				if (rcard == BUN_NONE)
					rcard = (BUN)max;
			}
			keys[i] =  ( (dbl)lcard + (dbl)rcard + (lcard*rcard * sel) );
			if (VERBOSE) {
				printf("%F %F ", keys[i], sel);
				_exp_print(sql, e);
			}
		}
		data[i] = n->data;
	}
	/* sort ascending */
	GDKqsort(keys, data, NULL, cnt, sizeof(dbl), sizeof(void *), TYPE_dbl, false, true);
	for(i=0; i<cnt; i++) {
		list_append(res, data[i]);
		sql_exp *e = data[i];
		if (VERBOSE) {
			printf("%F ", keys[i]);
			_exp_print(sql, e);
		}
	}
	return res;
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
	node *n;
	int cnt = list_length(dje);

	if (cnt <= 1)
		return dje;

	allocator *ta = MT_thread_getallocator();
	list *res = sa_list(sql->sa);
	int i, *keys = SA_NEW_ARRAY(ta, int, cnt);
	void **data = SA_NEW_ARRAY(ta, void*, cnt);

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
	for(i=0; i<cnt; i++)
		list_append(res, data[i]);
	return res;
}

static int
exp_find_one_rel( sql_rel **rels, int nr, sql_exp *e)
{
	int fnd = 0;

	for(int i = 1; i<=nr; i++) {
		if (rel_has_exp(rels[i], e, false) == 0) {
			if (fnd)
				return 0;
			fnd = i;
		}
	}
	return fnd;
}

static int
exps_find_one_rel( sql_rel **rels, int nr, list *exps)
{
	int fnd = 0;

	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (exp_is_atom(e))
			continue;
		int nfnd = exp_find_one_rel(rels, nr, n->data);
		if (nfnd != fnd && fnd)
			return 0;
		fnd = nfnd;
		if (!fnd)
			return 0;
	}
	return fnd;
}

/* TODO move popcount and popcount64 into gdk_*.h, used in gdk_cand, strimps and here */
static inline int
popcount64(uint64_t x)
{
#ifdef __has_builtin
#if __has_builtin(__builtin_popcountll)
	return (uint32_t) __builtin_popcountll(x);
#define BUILTIN_USED
#endif
#endif
#ifndef BUILTIN_USED
#if defined(_MSC_VER)
#if SIZEOF_OID == 4
	/* no __popcnt64 on 32 bit Windows */
	return (int) (__popcnt((uint32_t) x) + __popcnt((uint32_t) (x >> 32)));
#else
	return (uint32_t) __popcnt64(x);
#endif
#else
	x = (x & UINT64_C(0x5555555555555555)) + ((x >> 1) & UINT64_C(0x5555555555555555));
	x = (x & UINT64_C(0x3333333333333333)) + ((x >> 2) & UINT64_C(0x3333333333333333));
	x = (x & UINT64_C(0x0F0F0F0F0F0F0F0F)) + ((x >> 4) & UINT64_C(0x0F0F0F0F0F0F0F0F));
	return (x * UINT64_C(0x0101010101010101)) >> 56;
#endif
#endif
#undef BUILTIN_USED
}

static int
unique_pairs(uint16_t *r1, uint16_t *r2, int nr_exps, int *used)
{
	if (nr_exps <= 1)
		return nr_exps;
	int duplicates = 0;
	for (int i = 0; i<nr_exps; i++) {
		used[i] = 1;
		for (int j = i+1; j<nr_exps; j++) {
			if (!used[j] && r1[i] == r1[j] && r2[i] == r2[j]) {
				used[j] = 1;
				duplicates++;
			}
		}
	}
	assert(nr_exps - duplicates);
	return nr_exps - duplicates;
}

struct jo_pair {
	uint16_t l;
	uint16_t r;
	dbl cost;
	list *exps;
};


static void
mark_join_sides(allocator *ta, list *rels, list *exps,
		ulng *Max, sql_rel ***Rels_a, uint16_t **R1, uint16_t **R2, int **R3)
{
	int nr_exps = list_length(exps), nr_rels = list_length(rels), ci = 1;

	sql_rel **rels_a = SA_NEW_ARRAY(ta, sql_rel*, nr_rels+1); /* don't use slot 0 */
	rels_a[0] = NULL;
	ulng max = 0;
	for (node *n = rels->h; n; n = n->next, ci++) {
		rels_a[ci] = n->data;
		sql_rel *rel = n->data;
		BUN card = get_rel_count(rel);
		if (card != BUN_NONE && max < (ulng)card)
			max = (ulng)card;
	}
	uint16_t *r1 = SA_NEW_ARRAY(ta, uint16_t, nr_exps);
	uint16_t *r2 = SA_NEW_ARRAY(ta, uint16_t, nr_exps);
	/* change r3 into rest list's */
	int *r3 = SA_NEW_ARRAY(ta, int, nr_exps);

	ci = 0;
	for (node *n = exps->h; n; n = n->next, ci++) {
		sql_exp *cje = n->data;

		r1[ci] = r2[ci] = r3[ci] = 0;
		if (cje->type == e_cmp) {
			cje->tmp = ci;
			r1[ci] = cje->flag == cmp_filter ? exps_find_one_rel(rels_a, nr_rels, cje->l) : exp_find_one_rel(rels_a, nr_rels, cje->l);
			r2[ci] = cje->flag == cmp_filter ? exps_find_one_rel(rels_a, nr_rels, cje->r) : exp_find_one_rel(rels_a, nr_rels, cje->r);
			if (cje->f && cje->flag != cmp_filter) {
				r3[ci] = exp_find_one_rel(rels_a, nr_rels, cje->f);
				if (r3[ci] == r2[ci] || r3[ci] == r1[ci])
					r3[ci] = 0;
				if (r3[ci] != r2[ci] && r2[ci] == r1[ci]) {
					r2[ci] = r3[ci];
					r3[ci] = 0;
				}
			}
		}
	}
	*Max = max;
	*Rels_a = rels_a;
	*R1 = r1;
	*R2 = r2;
	*R3 = r3;
}

static sql_rel *
order_joins_bushy( visitor *v, list *rels, list *exps)
{
	unsigned int rsingle;

	/* loop of finding join pairs */
	allocator *ta = MT_thread_getallocator();
	int nr_exps = list_length(exps), nr_rels = list_length(rels);
	ulng max = 0;
	sql_rel **rels_a = NULL;
	uint16_t *r1, *r2;
	int *r3;

	mark_join_sides(ta, rels, exps, &max, &rels_a, &r1, &r2, &r3);

	/* find number of unique pairs */
	int *useda = SA_ZNEW_ARRAY(ta, int, nr_exps);
	int nr_pairs = unique_pairs(r1, r2, nr_exps, useda);
	struct jo_pair *pairs = SA_ZNEW_ARRAY(ta, struct jo_pair, nr_pairs);
	int used = 0, i = 0;
	for (node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		for (i = 0; i < used; i++) {
			if (pairs[i].l == r1[e->tmp] && pairs[i].r == r2[e->tmp])
				break;
		}
		if (i == used) {
			pairs[i].l = r1[e->tmp];
			pairs[i].r = r2[e->tmp];
			pairs[i].exps = sa_list(ta);
			pairs[i].cost = exp_cost(v, e, max, rels_a[r1[e->tmp]], rels_a[r2[e->tmp]]);
			used++;
		} else {
			if (VERBOSE)
				printf("change cost\n");
			pairs[i].cost += exp_cost(v, e, max, rels_a[r1[e->tmp]], rels_a[r2[e->tmp]]);
		}
		append(pairs[i].exps, e);
	}

	/* open problem, some expressions use more than 2 relations */
	/* For example a.x = b.y * c.z; */
	void *data = v->data;
	while(list_length(rels) >= 2) {
		sql_rel *l = NULL, *r = NULL;
		sql_exp *cje;
		int min_pos = 0;
		for(int i = 1; i<nr_pairs; i++) {
			if (!pairs[min_pos].cost || (pairs[i].cost && pairs[min_pos].cost > pairs[i].cost))
				min_pos = i;
		}
		if (pairs[min_pos].cost == 0) {
			if (VERBOSE)
				printf("max cost 0\n");
			break;
		}
		cje = pairs[min_pos].exps->h->data;
		if (VERBOSE)
			printf("%d %F\n", min_pos, pairs[min_pos].cost);
		pairs[min_pos].cost = 0;
		//assert(list_length(pairs[min_pos].exps->h->data) == 1);
		if (VERBOSE)
			_exps_print(v->sql, pairs[min_pos].exps);

		list_remove_data(exps, NULL, cje);
		if (cje->type == e_cmp) {
			l = rels_a[r1[cje->tmp]];
			r = rels_a[r2[cje->tmp]];
		}

		if (l && r && l != r) {
			list_remove_data(rels, NULL, l);
			list_remove_data(rels, NULL, r);
			/* Create a relation between l and r. Since the calling
			   functions rewrote the join tree, into a list of expressions
			   and a list of (simple) relations, there are no outer joins
			   involved, we can simply do a crossproduct here.
			   */
			rsingle = is_single(r);
			reset_single(r);
			exp_set_uniques(cje, l, r);
			sql_rel *top = rel_crossproduct(v->sql->sa, l, r, op_join);
			if (rsingle)
				set_single(r);
			rel_join_add_exp(v->sql->sa, top, cje);
			sql_exp *cjel = cje->l, *cjer = cje->r;
			if (is_unique(cjer)) {
				if (rel_find_exp(l, cjer))
					top->runique = is_basetable(r->op) || is_select(r->op) || (r->lunique || r->runique);
				else
					top->lunique = is_basetable(l->op) || is_select(l->op) || (l->lunique || l->runique);
			}
			if (is_unique(cjel)) {
				if (rel_find_exp(l, cjel))
					top->runique = is_basetable(r->op) || is_select(r->op) || (r->lunique || r->runique);
				else
					top->lunique = is_basetable(l->op) || is_select(l->op) || (l->lunique || l->runique);
			}

			for(int i = 0; i<nr_rels+1; i++) {
				if (rels_a[i] == l || rels_a[i] == r)
					rels_a[i] = top;
			}

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
			top = rel_estimate(v, top); /* we need stats */
			append(rels, top);

			uint64_t lr = r1[cje->tmp], rr = r2[cje->tmp];
			for (int i = 0; i<nr_pairs; i++) {
				int changed = 0;
				if (!pairs[i].cost)
					continue;
				if (pairs[i].l == lr || pairs[i].l == rr) {
					changed++;
				} else if (pairs[i].r == lr || pairs[i].r == rr) {
					changed++;
				}
				if (changed) /* TODO merge pairs if the have (after change) same l/r */
					pairs[i].cost = exps_cost(v, pairs[i].exps, max, rels_a[pairs[i].l], rels_a[pairs[i].r]) - 10;
			}
		} else {
			if (l && l == r) {
				if (!is_join(l->op) && !is_semi(l->op)) {
				//	assert(0);
					sql_rel *top = rel_select(v->sql->sa, l, cje);
					list_remove_data(rels, NULL, l);
					append(rels, top);
					for(int i = 0; i<nr_rels+1; i++) {
						if (rels_a[i] == l)
							rels_a[i] = top;
					}
					l = top;
				} else {
					rel_join_add_exp(v->sql->sa, l, cje);
				}

				/* all other join expressions on these 2 relations */
				for (node *en = exps->h; en; ) {
					node *next = en->next;
					sql_exp *e = en->data;
					if (rel_rebind_exp(v->sql, l, e)) {
						rel_join_add_exp(v->sql->sa, l, e);
						list_remove_data(exps, NULL, e);
					}
					en = next;
				}

			} else {
				if (VERBOSE)
					printf("l == r == NULL\n");
				break;
			}
		}
	}
	sql_rel *top = NULL;
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
				top = rel_estimate(v, top); /* we need stats */
			} else
				top = nr;
		}
	}
	if (list_length(exps)) { /* more expressions (add selects) */
		assert(top);
		top = rel_select(v->sql->sa, top, NULL);
		for(node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (exp_is_join_exp(e) == 0) {
				sql_rel *nr = NULL;
				if (is_theta_exp(e->flag)) {
					nr = rel_push_join(v->sql, top->l, e->l, e->r, e->f, e, 0);
				} else if (e->flag == cmp_filter) {
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
			rel_destroy(v->sql, top);
			top = l;
		}
	}
	if (top)
		top = rel_estimate(v, top); /* we need stats */
	v->data = data;
	return top;
}

static sql_rel *
order_joins(visitor *v, list *rels, list *exps)
{
	bool pipeline_mode = MT_thread_get_qry_ctx()->pipeline_mode;
	sql_rel *top = NULL, *l = NULL, *r = NULL, *f = NULL;
	sql_exp *cje;
	node *djn;
	list *sdje, *n_rels = NULL;
	int fnd = 0;
	unsigned int rsingle;
	int direct = 1;

	/* first find the distinct join expressions */
	list *aje = list_select(exps, rels, (fcmp) &exp_is_join, (fdup)NULL);
	/* add left/right relation */
	list *lrels, *rrels, *dje;
	if (find_join_rels(&lrels, &rrels, aje, rels) < 0)
		dje = aje;
	else
		dje = distinct_join_exps(aje, lrels, rrels);
	/* sort expressions on weighted number of reducing operators */
	for(djn = dje->h; djn; djn = djn->next ) {
		sql_exp *e = djn->data;
		list_remove_data(exps, NULL, e);
	}
	sdje = order_join_expressions(v->sql, dje, rels);

	allocator *ta = MT_thread_getallocator();
	int nr_exps = list_length(sdje), nr_rels = list_length(rels), ci = 1;
	if (nr_rels > 64) {
		direct = 0;
		n_rels = sa_list(ta);
	}
	sql_rel **rels_a = SA_NEW_ARRAY(ta, sql_rel*, nr_rels+1); /* don't use slot 0 */
	rels_a[0] = NULL;
	for (node *n = rels->h; n; n = n->next, ci++) {
		rels_a[ci] = n->data;
	}
	ulng *h = SA_NEW_ARRAY(ta, ulng, nr_exps), rel_mask = 0;	/* bit field (for > 64 its an imprint) */
	uint16_t *r1 = SA_NEW_ARRAY(ta, uint16_t, nr_exps);
	uint16_t *r2 = SA_NEW_ARRAY(ta, uint16_t, nr_exps);
	/* change r3 into rest list's */
	int *r3 = SA_NEW_ARRAY(ta, int, nr_exps);

	ci = 0;
	for (node *n = sdje->h; n; n = n->next, ci++) {
		sql_exp *cje = n->data;

		h[ci] = r1[ci] = r2[ci] = r3[ci] = 0;
		if (cje->type == e_cmp) {
			cje->tmp = ci;
			r1[ci] = cje->flag == cmp_filter ? exps_find_one_rel(rels_a, nr_rels, cje->l) : exp_find_one_rel(rels_a, nr_rels, cje->l);
			r2[ci] = cje->flag == cmp_filter ? exps_find_one_rel(rels_a, nr_rels, cje->r) : exp_find_one_rel(rels_a, nr_rels, cje->r);
			if (r1[ci])
				h[ci] |= ((ulng)1)<<((r1[ci]-1)%64);
			if (r2[ci])
				h[ci] |= ((ulng)1)<<((r2[ci]-1)%64);
			if (cje->f && cje->flag != cmp_filter) {
				r3[ci] = exp_find_one_rel(rels_a, nr_rels, cje->f);
				if (r3[ci] == r2[ci] || r3[ci] == r1[ci])
					r3[ci] = 0;
				if (r3[ci])
					h[ci] |= ((ulng)1)<<((r3[ci]-1)%64);
			}
		}
	}
	/* open problem, some expressions use more than 2 relations */
	/* For example a.x = b.y * c.z; */
	if (list_length(rels) >= 2 && sdje->h) {
		for (node *n = sdje->h; n && (!l || !r); n = n->next, ci++) {
			cje = n->data;

			if (n->next && r3[cje->tmp])
				continue;

			/* complex expressions may touch multiple base tables
			 * Should be pushed up to extra selection.
			 * */
			if (0 && popcount64(h[cje->tmp]) > 2)
				assert(0);
			/* find the involved relations */
			if (cje->type == e_cmp) {
				l = rels_a[r1[cje->tmp]];
				r = rels_a[r2[cje->tmp]];
				if (l && r)
					rel_mask |= h[cje->tmp];
			}
		}
		cje->tmp = 0;

		if (l && r && l != r)
			list_remove_data(sdje, NULL, cje);
	}

	if (l && r && l != r) {
		list_remove_data(rels, NULL, l);
		list_remove_data(rels, NULL, r);
		if (!direct) {
			list_append(n_rels, l);
			list_append(n_rels, r);
		}

		/* Create a relation between l and r. Since the calling
		   functions rewrote the join tree, into a list of expressions
		   and a list of (simple) relations, there are no outer joins
		   involved, we can simply do a crossproduct here.
		   */
		rsingle = is_single(r);
		reset_single(r);
		top = rel_crossproduct(v->sql->sa, l, r, op_join);
		if (pipeline_mode)
			top = rel_estimate(v, top); /* we need stats */
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
		fnd = 1;
	}
	/* build join tree using the ordered list */
	while(list_length(sdje) && fnd) {
		fnd = 0;
		/* find the first expression which could be added */
		for(djn = sdje->h; djn && !fnd && rels->h; djn = (!fnd)?djn->next:NULL) {
			node *en;
			l = r = f = NULL;
			int needs3 = 0;

			cje = djn->data;
			if ((h[cje->tmp] & rel_mask) > 0) {
				if (rel_mask & (((ulng)1)<<((r1[cje->tmp]-1)%64)))
					l = rels_a[r1[cje->tmp]];
				if (rel_mask & (((ulng)1)<<((r2[cje->tmp]-1)%64)))
					r = rels_a[r2[cje->tmp]];
				if (cje->f && r3[cje->tmp]) {
					needs3 = 1;
					if (rel_mask & (((ulng)1)<<((r3[cje->tmp]-1)%64)))
						f = rels_a[r3[cje->tmp]];
				}
			}
			if (!direct) { /* check if at least one side in n_rels */
				if (l && !list_find(n_rels, l, NULL))
					l = NULL;
				if (r && !list_find(n_rels, r, NULL))
					r = NULL;
				if (f && !list_find(n_rels, f, NULL))
					f = NULL;
			}

			if ((!needs3 && l && r) || (needs3 && l && r && f)) {
				assert(0);
				/* create a selection on the current */
				rel_join_add_exp(v->sql->sa, top, cje);
				fnd = 1;
			} else if ((!needs3 && (l || r)) || (needs3 && (l || r || f))) {
				sql_rel *nr[2]= {NULL, NULL};
				rel_mask |= h[cje->tmp];
				int i = 0;
				if (!l)
					nr[i++] = rels_a[r1[cje->tmp]];
				if (!r)
					nr[i++] = rels_a[r2[cje->tmp]];
				if (needs3 && !f)
					nr[i++] = rels_a[r3[cje->tmp]];
				if (!nr[0]) {
					fnd = 1; /* not really, but this bails out */
					list_remove_data(sdje, NULL, cje); /* handle later as select */
					if (!list_find(exps, cje, NULL))
						append(exps, cje);
					continue;
				}
				/* remove the expression from the lists */
				list_remove_data(sdje, NULL, cje);

				list_remove_data(rels, NULL, nr[0]);
				if (!direct)
					append(n_rels, nr[0]);
				if (i > 1 && nr[1]) {
					list_remove_data(rels, NULL, nr[1]);
					if (!direct)
						append(n_rels, nr[1]);
				}

				/* create a join using the current expression */
				rsingle = is_single(nr[0]);
				reset_single(nr[0]);
				top = rel_crossproduct(v->sql->sa, top, nr[0], op_join);
				if (rsingle)
					set_single(nr[0]);
				if (i > 1 && nr[1]) {
					rsingle = is_single(nr[1]);
					reset_single(nr[1]);
					top = rel_crossproduct(v->sql->sa, top, nr[1], op_join);
					if (rsingle)
						set_single(nr[1]);
				}
				rel_join_add_exp(v->sql->sa, top, cje);
				top = rel_get_statistics_(v, top); /* we need stats */

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
					if ((direct && ((e->flag <= cmp_notequal && (h[e->tmp] & rel_mask) == h[e->tmp] && h[e->tmp]) || (e->flag > cmp_notequal && rel_rebind_exp(v->sql, top, e))))  ||
					    (!direct && rel_rebind_exp(v->sql, top, e))) {
						rel_join_add_exp(v->sql->sa, top, e);
						list_remove_data(sdje, NULL, en->data);
					}
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
	if (list_length(sdje)) {
		if (list_empty(exps))
			exps = sdje;
		else
			exps = list_join(exps, sdje);
	}
	if (list_length(exps)) { /* more expressions (add selects) */
		top = rel_select(v->sql->sa, top, NULL);
		for(node *n=exps->h; n; n = n->next) {
			sql_exp *e = n->data;

			if (exp_is_join_exp(e) == 0) {
				sql_rel *nr = NULL;
				if (is_theta_exp(e->flag)) {
					nr = rel_push_join(v->sql, top->l, e->l, e->r, e->f, e, 0);
				} else if (e->flag == cmp_filter) {
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
			rel_destroy(v->sql, top);
			top = l;
		}
	}
	return top;
}

static int
rel_neg_in_size(sql_rel *r)
{
	if (is_munion(r->op) && r->nrcols == 0) {
		list *l = r->l;
		int n = 0;
		for(node *m = l->h; m; m = m->next) {
			sql_rel *i = m->data;
			n += rel_neg_in_size(i);
		}
		return -1 + n;
	}
	if (is_project(r->op) && r->nrcols == 0)
		return -1;
	return 0;
}

static void _rel_destroy(void *dummy, sql_rel *rel)
{
	(void)dummy;
	rel_destroy(NULL, rel);
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


static sql_rel * reorder_join(visitor *v, sql_rel *rel);
/* join order2: reorder joins using statistical information (selectivity/cardinality).
 * Runs after the main statistics pass, ensuring up-to-date cardinality estimates
 * are available for cost-based join ordering decisions.
 * For hash-based joins, cost ~ sum of input/output cardinalities. */

static sql_rel *
rel_join_order2_(visitor *v, sql_rel *rel, int skip)
{
	if (!rel)
		return rel;

	if (v->opt >= 0 && rel->opt >= v->opt) /* only once */
		return rel;

	switch (rel->op) {
	case op_basetable:
		break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			rel->l = rel_join_order2_(v, rel->l, false);
		break;
	case op_join:
		rel->l = rel_join_order2_(v, rel->l, true);
		rel->r = rel_join_order2_(v, rel->r, true);
		break;

	case op_left:
	case op_right:
	case op_full:
		rel->l = rel_join_order2_(v, rel->l, false);
		rel->r = rel_join_order2_(v, rel->r, false);
		break;

	case op_semi:
	case op_anti:

	case op_inter:
	case op_except:
		rel->l = rel_join_order2_(v, rel->l, false);
		rel->r = rel_join_order2_(v, rel->r, false);
		break;
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			n->data = rel_join_order2_(v, n->data, false);
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		rel->l = rel_join_order2_(v, rel->l, false);
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			rel->l = rel_join_order2_(v, rel->l, false);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel->l = rel_join_order2_(v, rel->l, false);
			rel->r = rel_join_order2_(v, rel->r, false);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rel_join_order2_(v, rel->r, false);
		break;
	case op_truncate:
	case op_buildhash:
	case op_probehash:
	case op_partition:
		break;
	}
	if (is_innerjoin(rel->op) && !is_single(rel) && !rel_is_ref(rel) && list_empty(rel->attr)) {
		if (!skip)
			rel = reorder_join(v, rel);
	} else {
		rel = rel_estimate(v, rel);
	}
	if (rel && v->opt >= 0)
		rel->opt = v->opt;
	return rel;
}

static sql_rel *
rel_join_order2(visitor *v, global_props *gp, sql_rel *rel)
{
	(void)gp;
	v->data = NULL;
	if (v->opt >= 0 && rel)
		v->opt = rel->opt+1;
	/* propagate statistics first to get up-to-date cardinality and selectivity */
	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);
	sql_rel *r = rel_join_order2_(v, rel, false);
	ma_close(&ta_state);
	return r;
}

run_optimizer
bind_join_order2(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (flag & join_order) ? rel_join_order2 : NULL;
}

static list *
push_up_join_exps( mvc *sql, sql_rel *rel)
{
	if (rel_is_ref(rel))
		return NULL;

	if (is_single(rel))
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
			l = list_join(l, r);
			r = NULL;
		} else if (!l) {
			l = r;
			r = NULL;
		}
		if (rel->exps) {
			assert(!r);
			if (l)
				l = list_join(rel->exps, l);
			else
				l = rel->exps;
		}
		rel->exps = NULL;
		return l;
	}
	default:
		return NULL;
	}
}

static sql_rel *rel_join_order_(visitor *v, sql_rel *rel);

static sql_rel *
reorder_join(visitor *v, sql_rel *rel)
{
	bool pipeline_mode = MT_thread_get_qry_ctx()->pipeline_mode;

	list *exps, *rels;
	allocator *ta = MT_thread_getallocator();

	if (is_innerjoin(rel->op) && !is_single(rel) && !rel_is_ref(rel) && list_empty(rel->attr)) {
		if (list_empty(rel->exps)) {
			sql_rel *l = rel->l, *r = rel->r;
			if (!is_innerjoin(l->op) && !is_innerjoin(r->op))
				return rel;
		}
		/* remove blocking selects */
		rel = remove_blocking_selects(v->sql, NULL, rel);

		rel->exps = push_up_join_exps(v->sql, rel);
	}

	if (!is_innerjoin(rel->op) || is_single(rel) || rel_is_ref(rel) || list_empty(rel->exps) || !list_empty(rel->attr)) {
		if (!list_empty(rel->exps)) { /* cannot add join idxs to cross products */
			exps = rel->exps;
			rel->exps = NULL; /* should be all crosstables by now */
			rels = sa_list(ta);
			/* try to use an join index also for outer joins */
			get_inner_relations(v->sql, rel, rels);
			int cnt = list_length(exps);
			rel->exps = exps;
			if (list_length(rel->exps) != cnt) {
				if (pipeline_mode)
					rel->exps = order_join_expressions_pp(v->sql, exps, rels);
				else
					rel->exps = order_join_expressions(v->sql, exps, rels);
			}
		}
		rel->l = rel_join_order_(v, rel->l);
		rel->r = rel_join_order_(v, rel->r);
	} else {
		exps = rel->exps;
		rel->exps = NULL; /* should be all crosstables by now */
		rels = sa_list(ta);
		get_relations(v, rel, rels);
		if (list_length(rels) > 1) {
			rels = push_in_join_down(v->sql, rels, exps);
			if (pipeline_mode)
				rel = order_joins_bushy(v, rels, exps);
			else
				rel = order_joins(v, rels, exps);
		} else {
			rel->exps = exps;
		}
	}
	return rel;
}

static sql_rel *
rel_join_order_(visitor *v, sql_rel *rel)
{
	if (!rel)
		return rel;

	if (v->opt >= 0 && rel->opt >= v->opt) /* only once */
		return rel;

	switch (rel->op) {
	case op_basetable:
		break;
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			rel->l = rel_join_order_(v, rel->l);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		break;

	case op_semi:
	case op_anti:

	case op_inter:
	case op_except:
		rel->l = rel_join_order_(v, rel->l);
		rel->r = rel_join_order_(v, rel->r);
		break;
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			n->data = rel_join_order_(v, n->data);
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
	case op_sample:
		rel->l = rel_join_order_(v, rel->l);
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			rel->l = rel_join_order_(v, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			rel->l = rel_join_order_(v, rel->l);
			rel->r = rel_join_order_(v, rel->r);
		}
		break;
	case op_insert:
	case op_update:
	case op_delete:
		rel->r = rel_join_order_(v, rel->r);
		break;
	case op_truncate:
	case op_buildhash:
	case op_probehash:
	case op_partition:
		break;
	}
	if (is_join(rel->op))
		rel = reorder_join(v, rel);
	return rel;
}

static sql_rel *
rel_join_order(visitor *v, global_props *gp, sql_rel *rel)
{
	(void) gp;
	if (v->opt >= 0 && rel)
		v->opt = rel->opt+1;
	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);
	sql_rel *r = rel_join_order_(v, rel);
	ma_close(&ta_state);
	return r;
}

run_optimizer
bind_join_order(visitor *v, global_props *gp)
{
	bool pipeline_mode = MT_thread_get_qry_ctx()->pipeline_mode;
	int flag = v->sql->sql_optimizer;
	return !pipeline_mode && gp->opt_level == 1 && gp->opt_cycle < 10 && !gp->cnt[op_update] && (gp->cnt[op_join] || gp->cnt[op_left] ||
		gp->cnt[op_right] || gp->cnt[op_full]) && (flag & join_order) ? rel_join_order : NULL;
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

static list *
matching_joins(allocator *sa, list *rels, list *exps, sql_exp *je)
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


static void
find_fk( mvc *sql, list *rels, list *exps)
{
	node *djn;
	list *aje, *dje;
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

			if (idx && (iname = ma_strconcat( sql->sa, "%", idx->base.name)) != NULL &&
				   ((!swapped && name_find_column(olr, NULL, iname, -2, &bt) == NULL) ||
			            ( swapped && name_find_column(orr, NULL, iname, -2, &bt) == NULL)))
				idx = NULL;

			if (idx) {
				prop *p;
				node *n;
				sql_exp *t = NULL, *i = NULL;
				list *jexps = sa_list(sql->sa);

				if (list_length(lcols) > 1 || !mvc_debug_on(sql, 512)) {

					/* Add join between idx and TID */
					if (swapped) {
						sql_exp *s = je->l, *l = je->r;

						t = rel_find_column(sql, olr, s->l, TID);
						i = rel_find_column(sql, orr, l->l, iname);
						if (!t || !i)
							continue;
						t->p = NULL;
						i->p = NULL;
						je = exp_compare(sql->sa, i, t, cmp_equal);
					} else {
						sql_exp *s = je->r, *l = je->l;

						t = rel_find_column(sql, orr, s->l, TID);
						i = rel_find_column(sql, olr, l->l, iname);
						if (!t || !i)
							continue;
						t->p = NULL;
						i->p = NULL;
						je = exp_compare(sql->sa, i, t, cmp_equal);
					}
					//t->p = prop_create(sql->sa, PROP_HASHCOL, NULL);

					/* Remove all join expressions */
					for (n = eje->h; n; n = n->next) {
						list_remove_data(exps, NULL, n->data);
						append(jexps, n->data);
					}
					append(exps, je);
					djn->data = je;
				} else if (swapped) { /* else keep je for single column expressions */
					je = exp_compare(sql->sa, je->r, je->l, cmp_equal);
					/* Remove all join expressions */
					for (n = eje->h; n; n = n->next) {
						list_remove_data(exps, NULL, n->data);
						append(jexps, n->data);
					}
					append(exps, je);
					djn->data = je;
				}
				je->p = p = prop_create(sql->sa, PROP_JOINIDX, je->p);
				p->value.pval = idx;
				p->exps = jexps;
			}
		}
	}
}


static sql_rel *
rel_join_use_fk(visitor *v, sql_rel *rel)
{
	/* Don't look at the same relation twice */
	if (are_fkeys_found(rel->used))
		return rel;
	if ((is_semi(rel->op)) && !list_empty(rel->exps)) {
		list *exps = rel->exps;
		list *rels = sa_list(v->sql->sa);

		rel->exps = NULL;
		append(rels, rel->l);
		append(rels, rel->r);
		find_fk( v->sql, rels, exps);

		rel->exps = exps;
	}
	if (is_innerjoin(rel->op) && !is_single(rel) && !rel_is_ref(rel) && list_empty(rel->attr)) {
		if (list_empty(rel->exps)) {
			sql_rel *l = rel->l, *r = rel->r;
			if (!is_innerjoin(l->op) && !is_innerjoin(r->op))
				return rel;
		}
		allocator *ta = MT_thread_getallocator();

		/* remove blocking selects */
		rel = remove_blocking_selects(v->sql, NULL, rel);
		rel->exps = push_up_join_exps(v->sql, rel);

		if (list_empty(rel->exps))
			return rel;
		list *exps = rel->exps;
		rel->exps = NULL; /* should be all crosstables by now */
		list *rels = sa_list(ta);
		get_relations(v, rel, rels);
		/* mark done */
		for (node *rn = rels->h; rn; rn = rn->next) {
			sql_rel *r = rn->data;
			r->used |= fkeys_found;
		}

		if (list_length(rels) > 1) {
			find_fk( v->sql, rels, exps);
			node *n = rels->h;
			sql_rel *top = n->data;
			list_remove_node(rels, NULL, n);
			while (list_length(rels)) {
				node *n = rels->h;
				sql_rel *r = n->data;
				top = rel_crossproduct(v->sql->sa, top, r, op_join);
				list_remove_node(rels, NULL, n);
			}
			rel = top;
		} else {
			assert(0);
		}
		rel->exps = exps;
	}
	return rel;
}

static sql_rel *
transitivity_rule(visitor *v, sql_rel *rel)
{
	if (!rel || rel_is_ref(rel) || is_single(rel) || !is_innerjoin(rel->op) || !list_empty(rel->attr))
		return rel;
	rel->exps = push_up_join_exps(v->sql, rel);
	list *exps = rel->exps;
	if (list_empty(exps))
		return rel;
	list *nexps = NULL;
	for(node *n = exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		sql_exp *el = e->l, *er = e->r;
		if (e->type == e_cmp && e->flag == cmp_equal && !is_anti(e) && !is_semantics(e) &&
			el->type == e_column && er->type == e_column) {
			for(node *m = n->next; m; m = m->next) {
				sql_exp *f = m->data;
				sql_exp *fl = f->l, *fr = f->r;
				if (f->type == e_cmp && f->flag == cmp_equal && !is_anti(f) && !is_semantics(f) &&
					fl->type == e_column && fr->type == e_column) {
					bool c1 = el->nid == fl->nid, c2 = el->nid == fr->nid, c3 = er->nid == fl->nid, c4 = er->nid == fr->nid;
					if (c1 || c2 || c3 || c4) {
						sql_exp *l = (c1||c2) ? er : el, *r = (c1||c3) ? fr : fl;
						l = exp_ref(v->sql, l);
						r = exp_ref(v->sql, r);
						sql_exp *ne = exp_compare(v->sql->sa, l, r, cmp_equal);
						if (!nexps)
							nexps = sa_list(v->sql->sa);
						if (list_find(exps, ne, (fcmp)&exp_match_exp_cmp) == NULL)
							append(nexps, ne);
					}
				}
			}
		}
	}
	if (nexps)
		rel->exps = list_join(exps, nexps);
	return rel;
}

static sql_rel *
rel_optimize_joins_topdown_(visitor *v, sql_rel *rel)
{
	bool pipeline_mode = MT_thread_get_qry_ctx()->pipeline_mode;
	if (pipeline_mode)
		rel = transitivity_rule(v, rel);
	rel = rel_join_use_fk(v, rel);
	return rel;
}

static sql_rel *
rel_optimize_joins_topdown(visitor *v, global_props *gp, sql_rel *rel)
{
	if (gp->opt_cycle > 0)
		return rel;
	allocator *ta = MT_thread_getallocator();
	allocator_state ta_state = ma_open(ta);

	rel = rel_visitor_topdown(v, rel, &rel_optimize_joins_topdown_);
	ma_close(&ta_state);
	v->data = gp;
	return rel;
}

run_optimizer
bind_optimize_joins_topdown(visitor *v, global_props *gp)
{
	int flag = v->sql->sql_optimizer;
	return gp->opt_level == 1 && (gp->cnt[op_join] || gp->cnt[op_left] || gp->cnt[op_right] ||
		   gp->cnt[op_full] || gp->cnt[op_semi] || gp->cnt[op_anti] ||
		   gp->cnt[op_select]) && (flag & optimize_joins_topdown) ? rel_optimize_joins_topdown : NULL;
}

