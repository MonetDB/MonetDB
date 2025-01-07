/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * This file contains the slice-based partitioned version of binary algebras for GROUP BY,
 * JOIN, ORDER BY, etc.
 */

#include "monetdb_config.h"

#include "bin_partition_by_slice.h"
#include "bin_partition.h"
#include "rel_bin.h"
#include "rel_exp.h"
#include "rel_rewriter.h"
#include "mal_builder.h"
#include "sql_pp_statement.h"

/*
 * The groupby execution plan:
 * The various choices:
 *			global - grouped aggregation
 *			cardinality
 *			relation or expression properties
 *			parallel pipeline execution
 *			complicating expressions such as distinct
 */

static lng
exp_getcard(mvc *sql, sql_rel *rel, sql_exp *e)
{
	BUN est = get_rel_count(rel);
	lng cnt;
	sql_subtype *t = exp_subtype(e);
	prop *p;

	if ((p = find_prop(e->p, PROP_NUNIQUES)))
		est = p->value.dval;

	if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
		cnt = 85000000;
	} else {
		cnt = (lng) est;
	}
	if (e->type == e_column && t && t->type->localtype == TYPE_str) {
		sql_column *c = name_find_column(rel, e->l, e->r, -1, NULL);

		if (c) {
			int de = mvc_is_duplicate_eliminated(sql, c);
			if (de)
				cnt = de;
		}
	}
	/* for now only based on type info, later use propagated cardinality estimation */
	switch (ATOMstorage(t->type->localtype)) {
		case TYPE_bte:
			return MIN(256,cnt);
		case TYPE_sht:
			return MIN(64*1024,cnt);
		default:
			break;
	}
	return cnt;
}

/* return true iff groupby can (ie only simple aggregation, which allows for 2 phases)
 * and cardinality estimation is low enough for extra resources for aggregation per thread.
 */
bool
rel_groupby_2_phases(mvc *sql, sql_rel *rel)
{
	BUN est = get_rel_count(rel);
	lng card = 1, cnt;
	bool global = list_empty(rel->r);

	if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
		cnt = 85000000;
	} else {
		cnt = (lng) est;
	}
	if (!list_empty(rel->r)) {
		list *l = rel->r;
		for( node *n = l->h; n; n = n->next ) {
			sql_exp *e = n->data;
			lng lcard = exp_getcard(sql, rel, e);
			if (lcard == cnt) {
				card = cnt;
				break;
			}
			card *= lcard; /* TODO check for overflow */
		}
	}
	if (card > 64*1024) /* TODO add tunable */
		return false;
	for(node *n = rel->exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (is_aggr(e->type)) {
			sql_subfunc *sf = e->f;

			if (need_distinct(e) && !global)
				return false;
			if (!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
			    strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
			    strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0)) {
				return false;
			}
		}
	}
	return true;
}

bool
exp_need_serialize(sql_exp *e)
{
	if (is_aggr(e->type)) {
		sql_subfunc *sf = e->f;

		if (!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
		    strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
		    strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0)) {
			return true;
		}
	}
	return false;
}

bool
rel_groupby_serialize(sql_rel *rel)
{
	for(node *n = rel->exps->h; n; n = n->next ) {
		if (exp_need_serialize(n->data))
			return true;
	}
	return false;
}

bool
rel_groupby_can_pp(sql_rel *rel, bool _2phases)
{
	if (!is_groupby(rel->op))
		return false;

	if (list_empty(rel->r) && !_2phases)
		return false;

	/* more checks needed */
	return true;
}

/* initialize the result variable for the parallel execution */
list *
rel_groupby_prepare_pp(list **aggrresults, list **serializedresults, backend *be, sql_rel *rel, bool _2phases, bool need_serialize)
{
	if (!_2phases && list_empty(rel->r)) /* cannot handle global aggregation without 2 phases */
		return NULL;

	list *shared = NULL;
	BUN est = get_rel_count(rel->l);
	lng estimate, card = 1;
	if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
		estimate = 85000000;
	} else {
		estimate = (lng) est;
	}

	shared = sa_list(be->mvc->sa); /* list of ints (variable numbers* */
	*aggrresults = sa_list(be->mvc->sa);
	if (need_serialize)
		*serializedresults = sa_list(be->mvc->sa);
	if (is_groupby(rel->op) && list_empty(rel->r) && !list_empty(rel->exps)) { /* global aggregation */
		for( node *n = rel->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;
			sql_subtype *t = exp_subtype(e), *it = NULL;
			int tt = t->type->localtype;
			int avg = e->type == e_aggr && strcmp(sf->func->base.name, "avg") == 0;
			int sum = e->type == e_aggr && strcmp(sf->func->base.name, "sum") == 0 && EC_APPNUM(t->type->eclass);
			bool serialize = need_serialize && exp_need_serialize(e);

			if (serialize) {
				int curhash = 0;
				list *inputs = e->l; /* for each input create bat */
				int need_distinct = need_distinct(e);
				for(node *n = inputs->h; n; n = n->next) {
					sql_exp *e = n->data;
					if (!exp_is_scalar(e)) {
						sql_subtype *t = exp_subtype(e);
						stmt *s = stmt_bat_new(be, t, estimate*1.1);
						if (!s)
							return NULL;
						append(*serializedresults, &s->nr);
						if (need_distinct) { /* create shared bat, for hash table */
							sql_subtype *t = exp_subtype(e);
							int estimate = exp_getcard(be->mvc, rel->l /* count before group by */, e);
							if (estimate<0) {
								assert(0);
								estimate = 85000000;
							}

							InstrPtr q = stmt_hash_new(be, t->type->localtype, estimate, curhash); /* pushed already */
							if (q == NULL)
								return NULL;
							assert(!e->shared);
							curhash = e->shared = q->argv[0];
						}
					}
				}
				continue;
			}
			if (avg)
				it = first_arg_subtype(e);
			if (avg && EC_APPNUM(t->type->eclass) && it && !EC_APPNUM(it->type->eclass))
				t = it;

			stmt *cc = const_column(be, stmt_atom(be, atom_general(be->mvc->sa, t, NULL, 0)));
			if (!cc)
				return NULL;
			append(shared, &cc->nr);

			if (avg || sum) { /* remainder (or compensation) and count */
				cc = const_column(be, EC_APPNUM(t->type->eclass) ? stmt_atom(be, atom_float(be->mvc->sa, t, 0)) : stmt_atom_lng(be, 0));
				if (!cc)
					return NULL;
				append(shared, &cc->nr);

				cc = const_column(be, stmt_atom_lng(be, 0));
				if (!cc)
					return NULL;
				append(shared, &cc->nr);
			}

			InstrPtr q = newAssignment(be->mb);
			if (!q)
				return NULL;
			q = pushNil(be->mb, q, tt);
			append(*aggrresults, q->argv);
			pushInstruction(be->mb, q);

			if (need_distinct(e)) { /* create shared bat, for hash table */
				list *el = e->l;
				sql_exp *a = el->h->data;
				sql_subtype *t = exp_subtype(a);
				int estimate = exp_getcard(be->mvc, rel->l /* count before group by */, a);
				if (estimate<0) {
					assert(0);
					estimate = 85000000;
				}

				InstrPtr q = stmt_hash_new(be, t->type->localtype, estimate, 0); /* pushed already */
				if (q == NULL)
					return NULL;
				assert(!e->shared);
				e->shared = q->argv[0];
			}
		}
	} else if (is_groupby(rel->op) && !list_empty(rel->r) && !list_empty(rel->exps)) {
		int curhash = 0;

		list *gbexps = rel->r;
		if (need_serialize) {
			stmt *s = stmt_bat_new(be, sql_bind_localtype("oid"), estimate*1.1);
			if (!s)
				return NULL;
			append(*serializedresults, &s->nr);
		}
		for(node *n = gbexps->h; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subtype *t = exp_subtype(e);
			/* ext */
			lng ncard = exp_getcard(be->mvc, rel, e);
			card *= ncard;
			if (card > estimate || ncard >= estimate)
				card = estimate;

			assert(card >= 0);
			if (card > INT_MAX)
				card = INT_MAX;

			InstrPtr q = stmt_hash_new(be, t->type->localtype, card, curhash);
			if (q == NULL)
				return NULL;
			curhash = getArg(q,0);
			append(shared, q->argv);
			append(*aggrresults, q->argv);
		}
		if (card < estimate)
			estimate = card;
		for( node *n = rel->exps->h; n; n = n->next ) {
			int grphash = curhash;
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;
			sql_subtype *t = exp_subtype(e), *it = NULL;
			int avg = e->type == e_aggr && strcmp(sf->func->base.name, "avg") == 0;
			int sum = e->type == e_aggr && strcmp(sf->func->base.name, "sum") == 0 && EC_APPNUM(t->type->eclass);
			bool serialize = need_serialize && exp_need_serialize(e);

			if (serialize) {
				list *inputs = e->l; /* for each input create bat */
				int need_distinct = need_distinct(e);
				for(node *n = inputs->h; n; n = n->next) {
					sql_exp *e = n->data;
					if (!exp_is_scalar(e)) {
						sql_subtype *t = exp_subtype(e);
						stmt *s = stmt_bat_new(be, t, estimate*1.1);
						if (!s)
							return NULL;
						append(*serializedresults, &s->nr);
						if (need_distinct) { /* create shared bat, for hash table */
							sql_subtype *t = exp_subtype(e);
							BUN est = get_rel_count(rel->l);
							lng estimate;

							if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
								estimate = 85000000;
							} else {
								estimate = (lng) est;
							}

							InstrPtr q = stmt_hash_new(be, t->type->localtype, estimate, curhash);
							if (q == NULL)
								return NULL;
							assert(!e->shared);
							curhash = e->shared = q->argv[0];
						}
					}
				}
				if (need_distinct) { /* need reduced group (ids) result */
					stmt *s = stmt_bat_new(be, sql_bind_localtype("oid"), estimate*1.1);
					if (!s)
						return NULL;
					append(*serializedresults, &s->nr);
				}
				curhash = grphash;
				continue;
			}

			if (avg)
				it = first_arg_subtype(e);
			if (avg && EC_APPNUM(t->type->eclass) && it && !EC_APPNUM(it->type->eclass))
				t = it;

			stmt *s = stmt_bat_new(be, t, estimate*1.1);
			if (s == NULL)
				return NULL;
			append(shared, &s->nr);
			append(*aggrresults, &s->nr);

			if (avg || sum) { /* remainder (or compensation) and count */
				s = stmt_bat_new(be, EC_APPNUM(t->type->eclass) ? t: sql_bind_localtype("lng"), estimate*1.1);
				if (s == NULL)
					return NULL;
				append(shared, &s->nr);
				append(*aggrresults, &s->nr);

				s = stmt_bat_new(be, sql_bind_localtype("lng"), estimate*1.1);
				if (s == NULL)
					return NULL;
				append(shared, &s->nr);
				append(*aggrresults, &s->nr);
			}

			if (need_distinct(e)) { /* create shared bat, for hash table */
				list *el = e->l;
				sql_exp *a = el->h->data;
				sql_subtype *t = exp_subtype(a);

				int estimate = exp_getcard(be->mvc, rel->l /* count before group by */, a);
				if (estimate<0) {
					assert(0);
					estimate = 85000000;
				}

				InstrPtr q = stmt_hash_new(be, t->type->localtype, estimate, curhash);
				if (q == NULL)
					return NULL;
				assert(!e->shared);
				e->shared = q->argv[0];
			}
		}
	} else {
		return NULL;
	}
	return shared;
}

stmt *
rel_groupby_combine_pp(backend *be, sql_rel *rel, list *gbstmts, stmt *grp, stmt *ext, stmt *cnt, stmt *cursub, stmt *pp, list *sub, stmt **cnt_aggr)
{
	node *o = sub->h;
	(void)grp; (void)ext; (void)cnt;
	list *shared = NULL;
	assert(be->pipeline);

	/* combine concurrent results */
	if (rel && list_empty(rel->r)) { /* global case */
		assert(list_empty(gbstmts));
		list *sub = cursub->op4.lval;

		/* phase 2 */
		shared = sa_list(be->mvc->sa);
		for (node *n = rel->exps->h, *m = o, *o = sub->h; n && m && o;
				n = n->next, m = m->next, o = o->next) {
			/* min -> min, max -> max, sum -> sum, count -> sum */
			sql_exp *e = n->data;
			sql_subtype *tpe = exp_subtype(e), *it = NULL;
			sql_subfunc *sf = e->f;
			int *v = m->data;
			stmt *i = o->data;

			if (e->type == e_column) {
				stmt *s = list_find_column(be, shared, e->l, e->r);
				assert(s);
				s = stmt_alias(be, s, e->alias.label, exp_relname(e), exp_name(e));
				append(shared, s);
				continue;
			}

			assert(e->type == e_aggr);
			char *name = NULL;
			int avg = 0, sum = 0;
			if (strcmp(sf->func->base.name, "min") == 0 ||
			    strcmp(sf->func->base.name, "max") == 0 ||
			    (avg= (strcmp(sf->func->base.name, "avg") == 0)) ||
				(sum= (strcmp(sf->func->base.name, "sum") == 0)) ||
			    strcmp(sf->func->base.name, "prod") == 0) {
				name = sf->func->base.name;
			} else {
				assert(strcmp(sf->func->base.name, "count") == 0);
				name = "sum";
			}
			sum &= EC_APPNUM(tpe->type->eclass);
			if (avg)
				it = first_arg_subtype(e);
			if (avg && EC_APPNUM(tpe->type->eclass) && it && !EC_APPNUM(it->type->eclass))
				tpe = it;
			InstrPtr q = newStmt(be->mb, getName("lockedaggr"), getName(name));
			if (avg || sum) { /* remainder (or compensation) and count */
				m = m->next;
				q = pushReturn(be->mb, q, *(int*)m->data);
				m = m->next;
				q = pushReturn(be->mb, q, *(int*)m->data);
				q->inout = 0;
			}
			q = pushArgument(be->mb, q, getArg(pp->q, 2));
			q = pushArgument(be->mb, q, i->nr);
			if (avg || sum) { /* remainder (or compensation) and count */
				q = pushArgument(be->mb, q, getArg(i->q, 1));
				q = pushArgument(be->mb, q, getArg(i->q, 2));
			}
			pushInstruction(be->mb, q);
			getArg(q, 0) = *v;
			stmt *s = stmt_none(be);
			s->op4.typeval = *tpe;
			s->nr = *v;
			s->q = q;
			s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
			s->q = q;
			if (i == *cnt_aggr)
				*cnt_aggr = s;
			append(shared, s);
		}
	} else if (rel && !list_empty(rel->r)) {
		list *sub = cursub->op4.lval;

		/* phase 2 of grouping */
		list *gbexps = rel->r, *ngbstmts = sa_list(be->mvc->sa);
		stmt *grp = NULL, *ext = NULL, *cnt = NULL;
		for(node *n = gbexps->h, *m = gbstmts->h; n && m; n = n->next, m = m->next, o = o->next) {
			sql_exp *e = n->data;
			stmt *gstmt = m->data;

			stmt *groupby = be->pipeline? stmt_group_partitioned(be, gstmt, grp, ext, cnt) : stmt_group(be, gstmt, grp, ext, cnt, 0);
			/* reuse extend ! */
			getArg(groupby->q, 1) = *(int*)o->data;
			groupby->q->inout = 1;
			grp = stmt_result(be, groupby, 0);
			ext = stmt_result(be, groupby, 1);
			gstmt = stmt_alias(be, gstmt, e->alias.label, exp_find_rel_name(e), exp_name(e));
			list_append(ngbstmts, gstmt);
		}
		gbstmts = ngbstmts;

		shared = sa_list(be->mvc->sa);
		for (node *n = rel->exps->h, *m = o, *o = sub->h; n && m && o;
				n = n->next, m = m->next, o = o->next) {
			/* min -> min, max -> max, sum -> sum, count -> sum */
			sql_exp *e = n->data;
			sql_subtype *tpe = exp_subtype(e), *it = NULL;
			sql_subfunc *sf = e->f;
			int *v = m->data;
			stmt *i = o->data;
			InstrPtr q;

			if (e->type == e_aggr) {
				char *name = NULL;
				int avg = 0, sum = 0;
				if (strcmp(sf->func->base.name, "min") == 0 ||
					strcmp(sf->func->base.name, "max") == 0 ||
					(avg= (strcmp(sf->func->base.name, "avg") == 0)) ||
					(sum= (strcmp(sf->func->base.name, "sum") == 0)) ||
					strcmp(sf->func->base.name, "prod") == 0) {
					name = sf->func->base.name;
				} else {
					assert(strcmp(sf->func->base.name, "count") == 0);
					name = "sum";
				}
				sum &= EC_APPNUM(tpe->type->eclass);
				if (avg)
					it = first_arg_subtype(e);
				if (avg && EC_APPNUM(tpe->type->eclass) && it && !EC_APPNUM(it->type->eclass))
					tpe = it;
				q = newStmt(be->mb, getName("aggr"), getName(name));
				if (avg || sum) { /* remainder (or compensation) and count */
					m = m->next;
					q = pushReturn(be->mb, q, *(int*)m->data);
					m = m->next;
					q = pushReturn(be->mb, q, *(int*)m->data);
					q->inout = 0;
				}
				q = pushArgument(be->mb, q, grp->nr);
				q = pushArgument(be->mb, q, i->nr);
				if (avg || sum) { /* remainder (or compensation) and count */
					q = pushArgument(be->mb, q, getArg(i->q, 1));
					q = pushArgument(be->mb, q, getArg(i->q, 2));
				}
				q = pushArgument(be->mb, q, getArg(pp->q, 2));
				q = pushArgument(be->mb, q, grp->nr);
			} else {
				q = newStmt(be->mb, getName("algebra"), projectionRef);
				q = pushArgument(be->mb, q, grp->nr);
				q = pushArgument(be->mb, q, i->nr);
				q = pushArgument(be->mb, q, getArg(pp->q, 2));
			}
			getArg(q, 0) = *v;
			q->inout = 0;
			pushInstruction(be->mb, q);
			stmt *s = stmt_pp_alias(be, q, e, 0);
			if (i == *cnt_aggr)
				*cnt_aggr = s;
			append(shared, s);
		}
	} else {
		return NULL;
	}
	cursub = stmt_list(be, shared);
	stmt_set_nrcols(cursub);
	return cursub;
}

stmt *
rel_groupby_finish_pp(backend *be, sql_rel *rel, stmt *cursub, bool _2phases)
{
	(void)_2phases;
	list *shared = cursub->op4.lval;

	if (shared) { /* for avg(integers) compute (dbl)avg + ((dbl)rest)/count) */
		for(node *n = shared->h, *m = rel->exps->h; n && m; n = n->next, m = m->next) {
			sql_exp *e = m->data;
			if (is_aggr(e->type)) {
				if (exp_need_serialize(e))
					continue;
				sql_subfunc *sf = e->f;
				sql_subtype *tpe = exp_subtype(e);
				if (/*!list_empty(rel->r) &&*/ strcmp(sf->func->base.name, "sum") == 0 && EC_APPNUM(tpe->type->eclass)) {
					stmt *s = n->data;
					InstrPtr q = s->q;
					int sum = getArg(q, 0), rem = getArg(q, 1);

					q = newStmtArgs(be->mb, batcalcRef, "+", 5); // todo use compute_sum
					setVarType(be->mb, getArg(q, 0), newBatType(tpe->type->localtype));
					pushArgument(be->mb, q, sum);
					pushArgument(be->mb, q, rem);
					pushNilBat(be->mb, q);
					pushNilBat(be->mb, q);
					pushInstruction(be->mb, q);

					s = stmt_none(be);
					s->op4.typeval = *tpe;
					s->nr = getArg(q, 0);
					s->q = q;
					s->key = s->nrcols = exp_card(e);
					s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
					n->data = s;
				} else if (strcmp(sf->func->base.name, "avg") == 0 && EC_APPNUM(tpe->type->eclass)) {
					stmt *s = n->data;
					InstrPtr q = s->q;
					int avg = getArg(q, 0), rem = getArg(q, 1), cnt = getArg(q, 2);

					q = newStmtArgs(be->mb, aggrRef, "compute_avg", 4);
					setVarType(be->mb, getArg(q, 0), newBatType(TYPE_dbl));
					pushArgument(be->mb, q, avg);
					pushArgument(be->mb, q, rem);
					pushArgument(be->mb, q, cnt);
					pushInstruction(be->mb, q);

					s = stmt_none(be);
					s->op4.typeval = *tpe;
					s->nr = getArg(q, 0);
					s->q = q;
					s->key = s->nrcols = exp_card(e);
					s = stmt_alias(be, s, e->alias.label, exp_find_rel_name(e), exp_name(e));
					n->data = s;
				}
			}
		}
	}
	/* we combined into a bat, ie lets fetch results */
	if (shared && list_empty(rel->r)) {
		list *nshared = sa_list(be->mvc->sa);
		for(node *n = shared->h; n; n = n->next) {
			stmt *s = n->data;
			s = stmt_fetch(be, s);
			append(nshared, s);
		}
		shared = nshared;
	}
	cursub = stmt_list(be, shared);
	stmt_set_nrcols(cursub);
	return cursub;
}


stmt *
rel_groupby_count_gt_0(backend *be, stmt *cursub, stmt *cnt, int *ext)
{
	stmt *c = stmt_uselect(be, cnt, stmt_atom_lng(be, 0), cmp_gt, NULL, 0, 1);
	if (!c)
		return NULL;

	list *shared = cursub->op4.lval, *nl = sa_list(be->mvc->sa);
	if (shared) {
		for(node *n = shared->h; n; n = n->next) {
			stmt *col = (stmt*)n->data;
			col = stmt_project(be, c, col);
			append(nl, col);
		}
	}
	if (*ext) {
		InstrPtr q = newStmt(be->mb, algebraRef, projectionRef);
		q = pushArgument(be->mb, q, c->nr);
		q = pushArgument(be->mb, q, *ext);
		pushInstruction(be->mb, q);
		*ext = getDestVar(q);
	}
	return stmt_list(be, nl);
}
