/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * For copyright information, see the file debian/copyright.
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
#include "sql_gencode.h"
#include "sql_pp_statement.h"
#include "sql_scenario.h"
#include "bin_partition_by_value.h"

/*
 * The groupby execution plan:
 * The various choices:
 *			global - grouped aggregation
 *			cardinality
 *			relation or expression properties
 *			parallel pipeline execution
 *			complicating expressions such as distinct
 */

lng
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
			if (de && de < cnt)
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
static bool
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
			    strcmp(sf->func->base.name, "null") == 0 ||
			    strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0)) {
				return false;
			}
		}
	}
	return true;
}

static int
pp_append(backend *be, int serialized_grpids, int claimed, int grp, int resultset )
{
	InstrPtr q = newStmt(be->mb, batRef, appendRef);
	if (q == NULL)
		return -1;
	q = pushArgument(be->mb, q, serialized_grpids);
	q = pushArgument(be->mb, q, claimed);
	q = pushArgument(be->mb, q, grp);
	q = pushBit(be->mb, q, TRUE);
	q = pushArgument(be->mb, q, resultset);
	pushInstruction(be->mb, q);
	return 0;
}

static bool
exp_need_serialize(sql_exp *e)
{
	if (is_aggr(e->type)) {
		sql_subfunc *sf = e->f;

		if (!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
		    strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
			strcmp(sf->func->base.name, "null") == 0 ||
		    strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0)) {
			return true;
		}
	}
	return false;
}

static bool
rel_groupby_serialize(sql_rel *rel)
{
	for(node *n = rel->exps->h; n; n = n->next ) {
		if (exp_need_serialize(n->data))
			return true;
	}
	return false;
}

#ifndef NDEBUG
static bool
rel_groupby_can_pp(sql_rel *rel, bool _2phases)
{
	if (!is_groupby(rel->op))
		return false;

	if (list_empty(rel->r) && !_2phases)
		return false;

	/* more checks needed */
	return true;
}
#endif

typedef struct pp_aggr_t {
	bte eclass; // any or all
	char *name;
	bte nr;
	int intermediate_types[3]; /* 0-255 -> type, 256 type of result, 257 first arg type */
	bool finalize;
} pp_aggr_t;

pp_aggr_t aggrs[10] =
{
	{EC_FLT, "avg", 3, {257, 257, TYPE_lng}, true}, //decimals needs compansation in same integer type
	{EC_ANY, "avg", 3, {257, TYPE_lng, TYPE_lng}, true},
	{EC_FLT, "sum", 3, {256, 256, TYPE_lng}, false}, //floats kahan/neumair need rsum, rcom and rcnt
	{EC_ANY, "sum", 1, {256}, false},
	{EC_ANY, "count", 1, {TYPE_lng}, false},
	{EC_ANY, "min", 1, {256}, false},
	{EC_ANY, "max", 1, {256}, false},
	{EC_ANY, "prod", 1, {256}, false},
	{EC_ANY, "null", 1, {TYPE_bit}, false},
};

/* initialize the result variable for the parallel execution */
static list *
rel_groupby_prepare_pp(list **aggrresults, list **serializedresults, backend *be, sql_rel *rel, bool _2phases, bool need_serialize, int nrparts)
{
	if (!is_groupby(rel->op) || list_empty(rel->exps) || (!_2phases && list_empty(rel->r))) /* cannot handle global aggregation without 2 phases */
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
	if (list_empty(rel->r)) { /* global aggregation */
		for( node *n = rel->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;
			sql_subtype *t = exp_subtype(e), *it = NULL;
			int tt = t->type->localtype;
			int avg = e->type == e_aggr && strcmp(sf->func->base.name, "avg") == 0;
			int sum = e->type == e_aggr && strcmp(sf->func->base.name, "sum") == 0 && EC_APPNUM(t->type->eclass);
			int cnt = e->type == e_aggr && !e->l && strcmp(sf->func->base.name, "count") == 0;
			bool serialize = need_serialize && exp_need_serialize(e);

			if (serialize) {
				int curhash = 0;
				list *inputs = e->l; /* for each input create bat */
				list *r = e->r;
				int need_distinct = need_distinct(e);
				for(node *n = inputs->h; n; n = n->next) {
					sql_exp *e = n->data;
					if (!exp_is_scalar(e)) {
						sql_subtype *t = exp_subtype(e);
						stmt *s = stmt_bat_new(be, t, estimate*1.1);
						if (!s)
							return NULL;
						append(*serializedresults, s);
						if (need_distinct) { /* create shared bat, for hash table */
							int estimate = exp_getcard(be->mvc, rel->l /* count before group by */, e);
							if (estimate<0) {
								assert(0);
								estimate = 85000000;
							}

							stmt *s = stmt_oahash_new(be, t, estimate, curhash, 0); /* pushed already */
							if (s == NULL)
								return NULL;
							assert(!e->shared);
							curhash = e->shared = s->nr; /* pass hash table statment via expression */
						}
					}
				}
				if (r) {
					list *obe = r->h->data;
					for(node *n = obe->h; n; n = n->next) {
						sql_exp *e = n->data;
						sql_subtype *t = exp_subtype(e);
						stmt *s = stmt_bat_new(be, t, estimate*1.1);
						if (!s)
							return NULL;
						append(*serializedresults, s);
					}
				}
				continue;
			}
			if (avg)
				it = first_arg_subtype(e);
			if (avg && EC_APPNUM(t->type->eclass) && it && !EC_APPNUM(it->type->eclass))
				t = it;

			stmt *cc = const_column(be, cnt? stmt_atom_lng(be, 0) : stmt_atom(be, atom_general(be->mvc->sa, t, NULL, 0)));
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

				stmt *s = stmt_oahash_new(be, t, estimate, 0, 0); /* pushed already */
				if (s == NULL)
					return NULL;
				assert(!e->shared);
				e->shared = s->nr;
			}
		}
	} else {
		int curhash = 0;

		list *gbexps = rel->r;
		if (need_serialize) {
			stmt *s = stmt_bat_new(be, sql_fetch_localtype(TYPE_oid), estimate*1.1);
			if (!s)
				return NULL;
			append(*serializedresults, s);
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

			stmt *s = stmt_oahash_new(be, t, nrparts?256:card, curhash, nrparts);
			if (s == NULL)
				return NULL;
			curhash = s->nr;
			append(shared, s->q->argv);
			append(*aggrresults, s->q->argv);
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
				list *r = e->r;
				int need_distinct = need_distinct(e);

				if (need_distinct) { /* need reduced group (ids) result */
					stmt *s = stmt_bat_new(be, sql_fetch_localtype(TYPE_oid), estimate*1.1);
					if (!s)
						return NULL;
					append(*serializedresults, s);
				}
				for(node *n = inputs->h; n; n = n->next) {
					sql_exp *e = n->data;
					if (!exp_is_scalar(e)) {
						sql_subtype *t = exp_subtype(e);
						stmt *s = stmt_bat_new(be, t, estimate*1.1);
						if (!s)
							return NULL;
						append(*serializedresults, s);
						if (need_distinct) { /* create shared bat, for hash table */
							sql_subtype *t = exp_subtype(e);
							BUN est = get_rel_count(rel->l);
							lng estimate;

							if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
								estimate = 85000000;
							} else {
								estimate = (lng) est;
							}

							stmt *s = stmt_oahash_new(be, t, estimate, grphash, 0);
							if (s == NULL)
								return NULL;
							assert(!e->shared);
							grphash = e->shared = s->nr;
						}
					}
				}
				if (r) {
					list *obe = r->h->data;
					for(node *n = obe->h; n; n = n->next) {
						sql_exp *e = n->data;
						sql_subtype *t = exp_subtype(e);
						stmt *s = stmt_bat_new(be, t, estimate*1.1);
						if (!s)
							return NULL;
						append(*serializedresults, s);
					}
				}
				continue;
			}

			if (avg)
				it = first_arg_subtype(e);
			if (avg && EC_APPNUM(t->type->eclass) && it && !EC_APPNUM(it->type->eclass))
				t = it;

			stmt *s = nrparts?stmt_mat_new(be, t, 256):stmt_bat_new(be, t, estimate*1.1);
			if (s == NULL)
				return NULL;
			append(shared, &s->nr);
			append(*aggrresults, &s->nr);

			if (avg || sum) { /* remainder (or compensation) and count */
				s = nrparts?stmt_mat_new(be, EC_APPNUM(t->type->eclass) ? t: sql_fetch_localtype(TYPE_lng), 256):
					stmt_bat_new(be, EC_APPNUM(t->type->eclass) ? t: sql_fetch_localtype(TYPE_lng), estimate*1.1);
				if (s == NULL)
					return NULL;
				append(shared, &s->nr);
				append(*aggrresults, &s->nr);

				s = nrparts?stmt_mat_new(be, sql_fetch_localtype(TYPE_lng), 256):
					stmt_bat_new(be, sql_fetch_localtype(TYPE_lng), estimate*1.1);
				if (s == NULL)
					return NULL;
				append(shared, &s->nr);
				append(*aggrresults, &s->nr);
			}

			if (need_distinct(e)) { /* create shared bat, for hash table */
				list *el = e->l;
				sql_exp *a = el->h->data;
				sql_subtype *t = exp_subtype(a);

				lng estimate = exp_getcard(be->mvc, rel->l /* count before group by */, a);
				if (estimate<0) {
					assert(0);
					estimate = 85000000;
				}
				if ((BUN)estimate < est) /* unique count * current (group) card */
					estimate *= card;
				if ((BUN)estimate > est)
					estimate = est;

				assert(!nrparts);
				stmt *s = stmt_oahash_new(be, t, nrparts?256:estimate, curhash, nrparts);
				if (s == NULL)
					return NULL;
				assert(!e->shared);
				e->shared = s->nr;
			}
		}
	}
	return shared;
}

static stmt *
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

			/* just a sanity check that, at this point, we indeed only get these expected aggr. funcs. */
			assert(strcmp(sf->func->base.name, "min") == 0
				|| strcmp(sf->func->base.name, "max") == 0
				|| strcmp(sf->func->base.name, "avg") == 0
				|| strcmp(sf->func->base.name, "sum") == 0
				|| strcmp(sf->func->base.name, "prod") == 0
				|| strcmp(sf->func->base.name, "null") == 0
				|| strcmp(sf->func->base.name, "count") == 0
			);

			if (strcmp(sf->func->base.name, "count") == 0) {
				name = "sum";
			} else {
			    avg = (strcmp(sf->func->base.name, "avg") == 0);
				sum = (strcmp(sf->func->base.name, "sum") == 0);
				name = sf->func->base.name;
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
			/* min -> min, max -> max, sum -> sum, count -> sum, null -> cnull */
			sql_exp *e = n->data;
			sql_subtype *tpe = exp_subtype(e), *it = NULL;
			sql_subfunc *sf = e->f;
			int *v = m->data;
			stmt *i = o->data;
			InstrPtr q;

			if (e->type == e_aggr) {
				char *name = NULL;
				int avg = 0, sum = 0;

				/* just a sanity check that, at this point, we indeed only get these expected aggr. funcs. */
				assert(strcmp(sf->func->base.name, "min") == 0
						|| strcmp(sf->func->base.name, "max") == 0
						|| strcmp(sf->func->base.name, "avg") == 0
						|| strcmp(sf->func->base.name, "sum") == 0
						|| strcmp(sf->func->base.name, "prod") == 0
						|| strcmp(sf->func->base.name, "null") == 0
						|| strcmp(sf->func->base.name, "count") == 0
					  );
				if (strcmp(sf->func->base.name, "count") == 0) {
					name = "sum";
				} else if (strcmp(sf->func->base.name, "null") == 0) {
					name = "cnull"; /* this name change is only needed in the per group case */
				} else {
					avg = (strcmp(sf->func->base.name, "avg") == 0);
					sum = (strcmp(sf->func->base.name, "sum") == 0);
					name = sf->func->base.name;
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

static stmt *
rel_groupby_finish_pp(backend *be, sql_rel *rel, stmt *cursub, bool _2phases)
{
	(void)_2phases;
	list *shared = cursub->op4.lval;
	int partition = cursub->partition;

	if (shared) { /* for avg(integers) compute (dbl)avg + ((dbl)rest)/count) */
		for(node *n = shared->h, *m = rel->exps->h; n && m; m = m->next) {
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
			n = n->next;
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
	cursub->partition = partition;
	return cursub;
}

static list *
mats_fetch(backend *be, list *shared, list *aggrresults, int mid)
{
	if (list_length(shared) != list_length(aggrresults)) {
		return NULL;
	}
	list *nshared = sa_list(be->mvc->sa);
	for(node *n = shared->h; n; n = n->next) {
		InstrPtr mp = newStmt(be->mb, "mat", "fetch");
		mp = pushArgument(be->mb, mp, *(int*)n->data);
		mp = pushArgument(be->mb, mp, mid);
		pushInstruction(be->mb, mp);
		append(nshared, mp->argv);
	}
	return nshared;
}

static void
piggy_back(backend *be, sql_rel *rel, stmt *cursub, list *aggrs)
{
	if (list_empty(rel->r)) {
		for(node *n = aggrs->h, *m = cursub->op4.lval->h; n && m; n = n->next, m = m->next ) {
			sql_exp *aggrexp = n->data;
			stmt *s = m->data;
			if (is_aggr(aggrexp->type)) {
				int min = 1;
				sql_subfunc *sf = aggrexp->f;
				list *l = aggrexp->l;
				if (list_length(l) == 1) {
					sql_exp *e = l->h->data;
					sql_column *c = exp_find_column(rel, e, -2);
					if (c && (strcmp(sf->func->base.name, "min") == 0 || ((min=strcmp(sf->func->base.name, "max")) == 0)) && !need_distinct(aggrexp)) {
						/* gen sql.set_min/set_max('schema','table','column', getArg(m->nr, 0) ) */
						char *fname = min?"set_min":"set_max";

						InstrPtr q = newStmt(be->mb, sqlRef, fname);
						q = pushStr(be->mb, q, c->t->s->base.name);
						q = pushStr(be->mb, q, c->t->base.name);
						q = pushStr(be->mb, q, c->base.name);
						(void) pushArgument(be->mb, q, getArg(s->q, 0));
						pushInstruction(be->mb, q);
					} else if (c && (strcmp(sf->func->base.name, "count") == 0) && need_distinct(aggrexp)) {
						InstrPtr q = newStmt(be->mb, sqlRef, "set_count_distinct");
						q = pushStr(be->mb, q, c->t->s->base.name);
						q = pushStr(be->mb, q, c->t->base.name);
						q = pushStr(be->mb, q, c->base.name);
						(void) pushArgument(be->mb, q, getArg(s->q, 0));
						pushInstruction(be->mb, q);
					}
				}
			}
		}
	} else if (list_length(rel->r) == 1) {
		list *gbe = rel->r;
		sql_exp *e = gbe->h->data;
		sql_column *c = exp_find_column(rel, e, -2);
		if (c) {
			stmt *s = cursub->op4.lval->h->data;
			sql_subfunc *cnt = sql_bind_func(be->mvc, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
			s = stmt_aggr(be, s, NULL, NULL, cnt, 1, 0, 1);
			/* beware is over estimation because of empty rows (to be remove by the select cout > 0) later */
			InstrPtr q = newStmt(be->mb, sqlRef, "set_count_distinct");
			q = pushStr(be->mb, q, c->t->s->base.name);
			q = pushStr(be->mb, q, c->t->base.name);
			q = pushStr(be->mb, q, c->base.name);
			(void) pushArgument(be->mb, q, getArg(s->q, 0));
			pushInstruction(be->mb, q);
		}
	}
}

stmt *
rel2bin_groupby_pp(backend *be, sql_rel *rel, list *refs)
{
	mvc *sql = be->mvc;
	list *l, *aggrs, *gbexps = sa_list(sql->sa), *aggrresults = NULL, *serializedresults = NULL, *shared = NULL;
	list *oaggrresults = NULL;
	node *n, *en, *m = NULL, *sn = NULL;
	stmt *sub = NULL, *cursub;
	stmt *groupby = NULL, *grp = NULL, *ext = NULL, *cnt_aggr = NULL;
	bool _2phases = rel_groupby_2_phases(be->mvc, rel);
	bool value_partition = SQLrunning && rel->parallel && !_2phases && rel_groupby_partition(rel);
	int neededpp = rel->partition && get_need_pipeline(be);
	int need_serialize = rel_groupby_serialize(rel); /* return if some of the aggregates require serialization (or fallback implementation) */
	sql_rel *inner = rel->l;
	int nrparts = 0;
	stmt *mat = NULL;

	if ((inner && inner->op == op_partition) || value_partition) {
		sub = rel2bin_partition(be, rel, refs);
		if (!sub)
			return NULL;
		mat = sub->op4.lval->h->data;
		sub->partition = 1;
		nrparts = mat_nr_parts(be, mat->nr);
	}

	assert(SQLrunning && rel->parallel && rel_groupby_can_pp(rel, _2phases));
	assert(!be->pipeline);

	int claimed = 0, prs = 0;
	stmt *serialized_grpids = NULL;
	if (need_serialize) {
		InstrPtr q = newStmt(be->mb, "pipeline", "resultset");
		pushInstruction(be->mb, q);
		prs = getDestVar(q);
	}

	sql_rel *p = rel->l;
	int is_base = (p && is_basetable(p->op));

	stmt *pp = NULL;

	shared = rel_groupby_prepare_pp(&aggrresults, &serializedresults, be, rel, _2phases, need_serialize, nrparts);

	if (!sub && (!rel->spb || pp_can_not_start(be->mvc, rel->l))) {
		set_need_pipeline(be);
	} else {
		int nr_parts = nrparts==0 ? pp_nr_slices(rel->l) : 0;
		int source = pp_counter(be, nr_parts, nrparts == 0 ? -1: nrparts, false);

		if (be->pp) {
			stmt_concat_add_source(be);
		} else {
			set_pipeline(be, stmt_pp_start_generator(be, source, true));
		}
		if (nrparts == 0)
			be->nrparts = nr_parts;
		int seqnr = pp_counter_get(be, source);
		if (nrparts) { /* map sequence number (source) into mat_id and slice_id */
			InstrPtr ctr = mat_counters_get(be, mat, seqnr);
			sub = mats_fetch_slices(be, sub, getArg(ctr, 0), getArg(ctr, 1));
			/* fetch result bats */
			shared = mats_fetch(be, shared, aggrresults, getArg(ctr, 0));
			oaggrresults = aggrresults;
			aggrresults = shared;
		}
	}
	if (!sub && rel->l) { /* first construct the sub relation */
		sub = subrel_bin(be, rel->l, refs);
		sub = subrel_project(be, sub, refs, rel->l);
		if (!sub)
			return NULL;
	}
	pp = get_pipeline(be);
	if (!pp) {
		rel2bin_slicer_pp(be, sub);
		pp = get_pipeline(be);
	}

	/* groupby columns */
	if (aggrresults)
		m = aggrresults->h;
	if (serializedresults)
		sn = serializedresults->h;

	/* Keep groupby columns, so that they can be looked up in the aggr list */
	if (rel->r) {
		list *exps = rel->r;
		for (en = exps->h; en; en = en->next) {
			sql_exp *e = en->data;
			stmt *gbcol = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);

			if (!gbcol) {
				assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
				return NULL;
			}
			if (!gbcol->nrcols)
				gbcol = stmt_const(be, bin_find_smallest_column(be, sub), gbcol);
			groupby = stmt_group_partitioned(be, gbcol, grp, ext, NULL);

			/* use global (shared (extend)) result */
			if (groupby && m) {
				if (!_2phases)
					getArg(groupby->q, 1) = *(int*)m->data;
				m = m->next;
			}
			if (be->pipeline && groupby)
				groupby->q->inout = 1;

			grp = stmt_result(be, groupby, 0);
			ext = stmt_result(be, groupby, 1);
			gbcol = stmt_alias(be, gbcol, e->alias.label, exp_find_rel_name(e), exp_name(e));
			list_append(gbexps, gbcol);
		}
		if (_2phases) { /* reproject group by exps */
			list *ngbexps = sa_list(sql->sa);
			for( en = gbexps->h; en; en = en->next ) {
				stmt *gbcol = en->data;
				if (gbcol && groupby) {
					gbcol = stmt_project(be, grp /*ext*/, gbcol);
					gbcol->q = pushArgument(be->mb, gbcol->q, be->pipeline);
					gbcol->q->inout = 1;
					if (list_length(gbexps) == 1)
						gbcol->key = 1;
				}
				list_append(ngbexps, gbcol);
			}
			gbexps = ngbexps;
		}
		/* if need_serialize keep group ids */
		if (need_serialize && m && grp) {
				sql_subfunc *cnt = sql_bind_func(be->mvc, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
				int pipeline = be->pipeline;
				be->pipeline = 0;
				stmt *nrrows = stmt_aggr(be, grp, NULL, NULL, cnt, 1, 0, 1);
				be->pipeline = pipeline;
				claimed = pp_claim(be, prs, nrrows->nr);

				serialized_grpids = sn->data;
				if (pp_append(be, serialized_grpids->nr, claimed, grp->nr, prs) < 0) /* use claimed offset */
					return NULL;
				sn = sn->next;
		}
	}
	/* now aggregate */
	l = sa_list(sql->sa);
	if (l == NULL)
		return NULL;
	aggrs = rel->exps;
	cursub = stmt_list(be, l);
	if (cursub == NULL)
		return NULL;

	if (aggrs && !aggrs->h && ext)
		list_append(l, ext);
	stmt *pgrp = grp;
	for (n = aggrs->h; n; n = n->next) {
		int iclaimed = claimed;
		sql_exp *aggrexp = n->data;
		stmt *aggrstmt = NULL;

		/* lookup already serialized cols (becarefull with distinct!) */
		if (need_serialize && exp_need_serialize(aggrexp)) {
			node *n, *m;
			/* keep data */
			list *input = aggrexp->l, *l = sa_list(sql->sa);
			list *r = aggrexp->r;

			for (n = input->h; n; n = n->next) {
				sql_exp *e = n->data;
				if (!exp_is_scalar(e)) {
					stmt *i = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
					if (!i)
						return NULL;
					append(l, i);
				}
			}
			if (r) {
				list *obe = r->h->data;
				if (obe && obe->h) {
					for (n = obe->h; n; n = n->next) {
						sql_exp *e = n->data;
						stmt *i = exp_bin(be, e, sub, NULL, NULL, NULL, NULL, NULL, 0, 0, 0);
						if (!i)
							return NULL;
						append(l, i);
					}
				}
			}
			if (need_distinct(aggrexp)) {
				sql_exp *prev = NULL;
				for (n = input->h, m = l->h; n && m; n = n->next) {
					sql_exp *e = n->data;
					if (!exp_is_scalar(e)) {
						if (!prev) {
							prev = e;
							continue;
						}
						stmt *a = m->data;
						m = m->next;

						assert(prev->shared);
						/* group.group or group.derive, except last */
						stmt *groupby = stmt_group_partitioned(be, a, grp, ext, NULL);
						groupby->q->inout = 1;
						getArg(groupby->q, 1) = prev->shared;

						grp = stmt_result(be, groupby, 0);
						ext = stmt_result(be, groupby, 1);
						prev = e;
					}
				}
				if (prev) {
					assert(m);
					stmt *u = stmt_unique_sharedout(be, m->data, prev->shared);
					if (u == NULL)
						return NULL;
					if (pgrp)
						u->q = pushArgument(be->mb, u->q, grp->nr);
					grp = stmt_result(be, u, 0);
				}

				/* reset claim */
				if (grp) {
					sql_subfunc *cnt = sql_bind_func(be->mvc, "sys", "count", sql_fetch_localtype(TYPE_void), NULL, F_AGGR, true, true);
					int pipeline = be->pipeline;
					be->pipeline = 0;
					stmt *nrrows = stmt_aggr(be, grp, NULL, NULL, cnt, 1, 0, 1);
					be->pipeline = pipeline;
					iclaimed = pp_claim(be, prs, nrrows->nr);
				}

				list *nl = sa_list(be->mvc->sa);
				/* project the unique grp ids */
				if (pgrp && pgrp != grp) {
					stmt *g = stmt_project(be, grp, pgrp);
					append(nl, g);
				}
				/* project the unique rows */
				for (node *m = l->h; m; m = m->next) {
					stmt *a = stmt_project(be, grp, m->data);
					append(nl, a);
				}
				l = nl;
			}
			m = l->h;
			if (pgrp && need_distinct(aggrexp) && m) {
				stmt *i = m->data;
				m = m->next;
				/* append */
				if (i && sn) {
					stmt *oi = sn->data;
					if (pp_append(be, oi->nr, iclaimed, i->nr, prs) < 0)
						return NULL;
					sn = sn->next;
				}
			}
			for (n = input->h; n && m; n = n->next) {
				sql_exp *e = n->data;
				if (!exp_is_scalar(e)) {
					stmt *i = m->data;
					m = m->next;
					assert(iclaimed);
					/* append */
					if (i && sn) {
						stmt *oi = sn->data;
						if (pp_append(be, oi->nr, iclaimed, i->nr, prs) < 0)
							return NULL;
						sn = sn->next;
					}
				}
			}
			if (r) {
				list *obe = r->h->data;
				if (obe && obe->h) {
					for (n = obe->h; n && m; n = n->next, m = m->next) {
						stmt *i = m->data;
						/* append */
						if (i && sn) {
							stmt *oi = sn->data;
							if (pp_append(be, oi->nr, iclaimed, i->nr, prs) < 0)
								return NULL;
							sn = sn->next;
						}
					}
				}
			}
			grp = pgrp;
			continue;
		}
		/* first look in the current aggr list (l) and group by column list */
		if (l && !aggrstmt && aggrexp->type == e_column)
			aggrstmt = list_find_column_nid(be, l, aggrexp->nid);
		if (gbexps && !aggrstmt && aggrexp->type == e_column) {
			aggrstmt = list_find_column_nid(be, gbexps, aggrexp->nid);
			if ((!be->pipeline || !_2phases) && aggrstmt && groupby) {
				aggrstmt = stmt_project(be, be->pipeline?grp:ext, aggrstmt);
				if (be->pipeline) {
					aggrstmt->q = pushArgument(be->mb, aggrstmt->q, be->pipeline);
					aggrstmt->q->inout = 1;
				}
				if (list_length(gbexps) == 1)
					aggrstmt->key = 1;
			}
		}

		if (!aggrstmt)
			aggrstmt = exp_bin(be, aggrexp, sub, NULL, grp, ext, NULL, NULL, 0, 0, 0);
		/* maybe the aggr uses intermediate results of this group by,
		   therefore we pass the group by columns too
		 */
		if (!aggrstmt)
			aggrstmt = exp_bin(be, aggrexp, sub, cursub, grp, ext, NULL, NULL, 0, 0, 0);
		if (!aggrstmt) {
			assert(sql->session->status == -10); /* Stack overflow errors shouldn't terminate the server */
			return NULL;
		}

		/* use global (shared) result */
		if (aggrstmt && m) {
			if (!_2phases)
				aggrstmt->nr = getArg(aggrstmt->q, 0) = *(int*)m->data;
			m = m->next;
			if (!_2phases && is_aggr(aggrexp->type)) {
				sql_subfunc *sf = aggrexp->f;
				if (strcmp(sf->func->base.name, "avg") == 0) {
					getArg(aggrstmt->q, 1) = *(int*)m->data;
					m = m->next;
					sql_subtype *res = sf->res->h->data;
					int restype = res->type->localtype;
					if (rel->r || restype != tail_type(aggrstmt->op1)->type->localtype || restype != TYPE_dbl) {
						getArg(aggrstmt->q, 2) = *(int*)m->data;
						m = m->next;
					}
				}
				sql_subtype *t = exp_subtype(aggrexp);
				if (strcmp(sf->func->base.name, "sum") == 0 && EC_APPNUM(t->type->eclass)) {
					getArg(aggrstmt->q, 1) = *(int*)m->data;
					m = m->next;
					getArg(aggrstmt->q, 2) = *(int*)m->data;
					m = m->next;
				}
			}
		}
		if (be->pipeline && aggrstmt)
			aggrstmt->q->inout = 0;

		if (!aggrstmt->nrcols && ext && ext->nrcols) {
			assert(!be->pipeline);
			aggrstmt = stmt_const(be, ext, aggrstmt);
		}

		aggrstmt = stmt_rename(be, aggrexp, aggrstmt);
		list_append(l, aggrstmt);
	}
	stmt_set_nrcols(cursub);

	(void)stmt_pp_jump(be, pp, be->nrparts);
	if (_2phases)
		cursub = rel_groupby_combine_pp(be, rel, gbexps, grp, ext, NULL, cursub, pp, shared, &cnt_aggr);
	(void)cnt_aggr;
	(void)stmt_pp_end(be, pp);

	/* pack partitions */
	if (oaggrresults) {
		node *m = oaggrresults->h; /* skip hash */
		for(node *n = gbexps->h; n && m; n = n->next, m = m->next)
			;
		if (rel->partition) {
			list *nl = sa_list(be->mvc->sa);
			node *o = rel->exps->h;
			for(node *n = l->h; n && m && o; n = n->next, m = m->next, o = o->next ) {
				sql_exp *e = o->data;
				stmt *aggrstmt = n->data;
				stmt *ns = stmt_create(be->mvc->sa, st_none);
				ns->nrcols = 1;
				ns->key = 1;
				ns->aggr = 1;
				ns->nr = *(int*)m->data;
				ns->op4.typeval = *tail_type(aggrstmt);
				ns = stmt_alias(be, ns, e->alias.label, exp_relname(e), exp_name(e));
				list_append(nl, ns);
			}
			cursub = stmt_list(be, nl);
			cursub->partition = 1;
		} else
		if (!neededpp) {
			list *nl = sa_list(be->mvc->sa);
			for(node *n = l->h; n && m; n = n->next, m = m->next ) {
				stmt *aggrstmt = n->data;

				InstrPtr mp = newStmt(be->mb, "mat", "pack");
				mp = pushArgument(be->mb, mp, *(int*)m->data);
				pushInstruction(be->mb, mp);
				/* keep names from aggrstmt */
				aggrstmt = stmt_instruction(be, mp, aggrstmt);

				list_append(nl, aggrstmt);
			}
			cursub = stmt_list(be, nl);
		} else {
			stmt *pp = stmt_pp_start_nrparts(be, 256);
			set_pipeline(be, pp);
			int ppnr = be->pipeline;
			be->pipeline = 0;

			list *nl = sa_list(be->mvc->sa);
			for(node *n = l->h; n && m; n = n->next, m = m->next ) {
				stmt *aggrstmt = n->data;

				InstrPtr mp = newStmt(be->mb, "mat", "fetch");
				mp = pushArgument(be->mb, mp, *(int*)m->data);
				mp = pushArgument(be->mb, mp, be->pp);
				pushInstruction(be->mb, mp);
				/* keep names from aggrstmt */
				aggrstmt = stmt_instruction(be, mp, aggrstmt);

				list_append(nl, aggrstmt);
			}
			be->pipeline = ppnr;
			cursub = stmt_list(be, nl);
		}
	}
	cursub = rel_groupby_finish_pp(be, rel, cursub, _2phases);

	int sext = 0;
	if (need_serialize && ext) {
		InstrPtr q = newStmt(be->mb, "hash", "ext");
		(void) pushArgument(be->mb, q, ext->nr);
		sext = q->argv[0];
		pushInstruction(be->mb, q);
	}

	/* post pipeline aggregation */
	l = cursub->op4.lval;
	if (need_serialize) {
		if (serializedresults)
			sn = serializedresults->h;
		if (serialized_grpids)
			sn = sn->next;
		for (n = aggrs->h; n; n = n->next) {
			sql_exp *aggrexp = n->data;
			if (exp_need_serialize(aggrexp)) {
				list *r = aggrexp->r;
				sql_subfunc *af = aggrexp->f;
				if (backend_create_subfunc(be, af, NULL) < 0)
					return NULL;
				str aggrF = grp?SA_NEW_ARRAY(be->mvc->sa, char, strlen(af->func->imp) + 4):af->func->imp;
				if (!aggrF)
					return NULL;
				if (grp)
					stpcpy(stpcpy(aggrF, "sub"), af->func->imp);

				/* get correct group ids */
				stmt *ogrp = serialized_grpids;
				if (need_distinct(aggrexp)) {
					ogrp = sn->data;
					sn = sn->next;
				}

				/* first dump scalars */
				list *scalars = sa_list(be->mvc->sa);
				list *inputs = aggrexp->l;
				for(node *z = inputs->h; z; z = z->next) {
					sql_exp *e = z->data;
					if (exp_is_scalar(e)) {
						stmt *s = exp_bin(be, e, NULL, NULL /*psub*/, NULL, NULL, NULL, NULL, 0, 0, 0);
						if (z == inputs->h || ogrp)
							s = stmt_project(be, ogrp, s);
						else
							s = const_column(be, s);
						append(scalars, s);
					}
				}

				InstrPtr q = newStmtArgs(be->mb, af->func->mod, aggrF, 10);
				if (!q)
					return NULL;
				if (LANG_EXT(af->func->lang))
					q = pushPtr(be->mb, q, af->func);
				int restype = exp_subtype(aggrexp)->type->localtype;
				if (grp) {
					restype = newBatType(restype);
					setVarType(be->mb, getArg(q, 0), restype);
				}
				if (af->func->lang == FUNC_LANG_R ||
					af->func->lang >= FUNC_LANG_PY ||
					af->func->lang == FUNC_LANG_C ||
					af->func->lang == FUNC_LANG_CPP) {
					setVarType(be->mb, getArg(q, 0), restype);
					if (af->func->lang == FUNC_LANG_C) {
						q = pushBit(be->mb, q, 0);
					} else if (af->func->lang == FUNC_LANG_CPP) {
						q = pushBit(be->mb, q, 1);
					}
					q = pushStr(be->mb, q, af->func->query);
				}

				node *osn = sn;
				if (r) { /* check new ordered aggregation */
					/* first move to the order by serialized results */
					for(node *z = inputs->h; z; z = z->next) {
						sql_exp *e = z->data;
						if (!exp_is_scalar(e))
							sn = sn->next;
					}
					list *obe = r->h->data;
					if (obe && obe->h) {
						stmt *orderby = NULL, *orderby_ids, *orderby_grp;
						/* order by */
						if (grp) {
							orderby = stmt_order(be, ogrp, true, true);

							orderby_ids = stmt_result(be, orderby, 1);
							orderby_grp = stmt_result(be, orderby, 2);
						}
						for (node *n = obe->h; n; n = n->next, sn = sn->next) {
							sql_exp *oe = n->data;
							stmt *os = sn->data;
							if (orderby)
								orderby = stmt_reorder(be, os, is_ascending(oe), nulls_last(oe), orderby_ids, orderby_grp);
							else
								orderby = stmt_order(be, os, is_ascending(oe), nulls_last(oe));
							orderby_ids = stmt_result(be, orderby, 1);
							orderby_grp = stmt_result(be, orderby, 2);
						}
						/* depending on type of aggr project input or ordered column */
						for (node *n = osn; n != sn; n = n->next)
							n->data = stmt_project(be, orderby_ids, n->data);
						if (grp)
							ogrp = stmt_project(be, orderby_ids, ogrp);
					}
				}
				sn = osn;
				for(node *z = inputs->h, *cn = scalars->h; z; z = z->next) {
					sql_exp *e = z->data;
					if (!exp_is_scalar(e)) {
						stmt *i = sn->data;
						(void) pushArgument(be->mb, q, i->nr);
						sn = sn->next;
					} else {
						stmt *i = cn->data;
						(void) pushArgument(be->mb, q, i->nr);
						cn = cn->next;
					}
				}
				if (ogrp)
					(void) pushArgument(be->mb, q, ogrp->nr);
				if (sext)
					(void) pushArgument(be->mb, q, sext);
				if (grp && LANG_INT_OR_MAL(af->func->lang))
					q = pushBit(be->mb, q, need_no_nil(aggrexp));
				pushInstruction(be->mb, q);

				stmt *s = stmt_none(be);
				s->op4.typeval = *exp_subtype(aggrexp);
				s->nr = q->argv[0];
				s->q = q;
				s->nrcols = grp?grp->nrcols:2;
				stmt *aggrstmt = stmt_rename(be, aggrexp, s);
				list_append(l, aggrstmt);
			}
		}
	}

	if (pp && is_base && cursub) /* for now just piggy back global aggregation on basetables */
		piggy_back(be, rel, cursub, aggrs);

	/* GROUP BY ends the current pipeline() block.  If needed, start a new
	 * block to partition the result of this GROUP BY for the upper-level
	 * operators, e.g. topN. */

	assert(!neededpp);
	if (!rel->r && list_length(aggrs) == 1) {
		sql_exp *cnt = aggrs->h->data;
		stmt *cntstmt = l->h->data;
		if (cnt->type == e_aggr && !cnt->l && cnt->intern && add_to_rowcount_accumulator(be, cntstmt->nr) < 0)
			return sql_error(sql, 10, SQLSTATE(HY013) MAL_MALLOC_FAIL);
	}
	return cursub;
}

