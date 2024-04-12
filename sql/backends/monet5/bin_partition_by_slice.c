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
#include "opt_prelude.h"
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
rel_groupby_pp(sql_rel *rel, bool _2phases)
{
	if (!is_groupby(rel->op))
		return false;

	for(node *n = rel->exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (is_aggr(e->type)) {
			sql_subfunc *sf = e->f;

			if (!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
			    strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
			    strcmp(sf->func->base.name, "sum") == 0 || strcmp(sf->func->base.name, "prod") == 0)) {
				return false;
			}
		}
	}

	if (list_empty(rel->r) && !_2phases)
		return false;
	/* more checks needed */
	return true;
}

/* initialize the result variable for the parallel execution */
list *
rel_groupby_prepare_pp(list **aggrresults, backend *be, sql_rel *rel, bool _2phases)
{
	if (!_2phases && list_empty(rel->r)) /* cannot handle global aggregation without 2 phases */
		return NULL;

	list *shared = NULL;
	if (is_groupby(rel->op) && list_empty(rel->r) && !list_empty(rel->exps)) { /* global aggregation */
		shared = sa_list(be->mvc->sa); /* list of ints (variable numbers* */
		*aggrresults = sa_list(be->mvc->sa);
		for( node *n = rel->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;
			sql_subtype *t = exp_subtype(e);
			int tt = t->type->localtype;
			int avg = e->type == e_aggr && strcmp(sf->func->base.name, "avg") == 0;

			stmt *cc = const_column(be, stmt_atom(be, atom_general(be->mvc->sa, t, NULL, 0)));
			if (!cc)
				return NULL;
			append(shared, &cc->nr);

			if (avg) {
				/* remainder and count */
				if (!EC_APPNUM(t->type->eclass)) {
					cc = const_column(be, stmt_atom_lng(be, 0));
					if (!cc)
						return NULL;
					append(shared, &cc->nr);
				}

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
			pushInstruction(be->mb, q);
		}
	} else if (is_groupby(rel->op) && !list_empty(rel->r) && !list_empty(rel->exps)) {
		BUN est = get_rel_count(rel->l);
		lng estimate, card = 1;
		int curhash = 0;

		if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max) {
			estimate = 85000000;
		} else {
			estimate = (lng) est;
		}
		shared = sa_list(be->mvc->sa); /* list of ints (variable numbers) */
		list *gbexps = rel->r;
		*aggrresults = sa_list(be->mvc->sa);
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
			sql_exp *e = n->data;
			sql_subfunc *sf = e->f;
			sql_subtype *t = exp_subtype(e);

			InstrPtr q = stmt_bat_new(be, t->type->localtype, estimate*1.1);
			if (q == NULL)
				return NULL;
			append(shared, q->argv);
			append(*aggrresults, q->argv);

			if (e->type == e_aggr && strcmp(sf->func->base.name, "avg") == 0) {
				if (!EC_APPNUM(t->type->eclass)) {
					q = stmt_bat_new(be, TYPE_lng, estimate*1.1);
					if (q == NULL)
						return NULL;
					append(shared, q->argv);
					append(*aggrresults, q->argv);
				}

				q = stmt_bat_new(be, TYPE_lng, estimate*1.1);
				if (q == NULL)
					return NULL;
				append(shared, q->argv);
				append(*aggrresults, q->argv);
			}

			if (need_distinct(e)) { /* create shared bat, for hash table */
				list *el = e->l;
				sql_exp *a = el->h->data;
				sql_subtype *t = exp_subtype(a);
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
				e->shared = q->argv[0];
			}
		}
	} else {
		return NULL;
	}
	return shared;
}

