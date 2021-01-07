/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_query.h"
#include "rel_rel.h"

static stacked_query *
sq_create( sql_allocator *sa, sql_rel *rel, int sql_state)
{
	stacked_query *q = SA_NEW(sa, stacked_query);

	assert(rel);
	*q = (stacked_query) {
		.rel = rel,
		.sql_state = sql_state,
		.grouped = is_groupby(rel->op),
		.groupby = 0, /* not used for groupby of inner */
	};
	return q;
}

sql_query *
query_create( mvc *sql)
{
	sql_query *q = SA_NEW(sql->sa, sql_query);

	q->sql = sql;
	q->outer = sql_stack_new(sql->sa, 32);
	return q;
}

void
query_push_outer(sql_query *q, sql_rel *r, int sql_state)
{
	stacked_query *sq = sq_create(q->sql->sa, r, sql_state);
	assert(sq);
	sql_stack_push(q->outer, sq);
}

sql_rel *
query_pop_outer(sql_query *q)
{
	stacked_query *sq = sql_stack_pop(q->outer);
	sql_rel *r = sq->rel;
	return r;
}

sql_rel *
query_fetch_outer(sql_query *q, int i)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);
	if (!sq)
		return NULL;
	return sq->rel;
}

int
query_fetch_outer_state(sql_query *q, int i)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);
	if (!sq)
		return 0;
	return sq->sql_state;
}

void
query_update_outer(sql_query *q, sql_rel *r, int i)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);
	sq->rel = r;
	sq->last_used = NULL;
	sq->used_card = 0;
	sq->grouped = is_groupby(r->op);
}

int
query_has_outer(sql_query *q)
{
	return sql_stack_top(q->outer);
}

int
query_outer_used_exp(sql_query *q, int i, sql_exp *e, int f)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);

	if (is_sql_aggr(f) && !is_sql_farg(f) && sq->groupby) /* cannot user outer both for inner groupby and aggregation */
		return -1;
	if (is_sql_groupby(f))
		sq->groupby = 1;

	if (is_sql_aggr(f) && !is_sql_farg(f) && sq->groupby && sq->last_used && sq->used_card > CARD_AGGR) /* used full relation */
		return -1;
	if (!is_sql_aggr(f) && sq->grouped && e->card > CARD_AGGR)
		return -1;

	sq->last_used = e;
	sq->used_card = sq->rel->card;
	assert( (!is_sql_aggr(f) && sq->grouped == 0 && e->card != CARD_AGGR) || /* outer is a none grouped relation */
		(!is_sql_aggr(f) && sq->grouped == 1 && e->card <= CARD_AGGR) || /* outer is groupbed, ie only return aggregations or groupby cols */
		(is_sql_aggr(f) && !is_sql_farg(f) && !sq->grouped && e->card != CARD_AGGR) || /* a column/constant to be aggregated */
		(is_sql_aggr(f) && !is_sql_farg(f) && sq->grouped && e->card != CARD_AGGR) || /* a column/constant to be aggregated */
		(is_sql_aggr(f) && is_sql_farg(f) && sq->grouped && e->card <= CARD_AGGR) ||  /* groupby ( function (group by col)) */
		(is_sql_aggr(f) && sq->grouped && e->card <= CARD_AGGR) ); /* nested aggregations is handled later */
	return 0;
}

void
query_outer_pop_last_used(sql_query *q, int i)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);

	sq->last_used = NULL;
	sq->used_card = 0;
	sq->sql_state = 0;
}

int
query_outer_aggregated(sql_query *q, int i, sql_exp *e)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);

	assert(sq->grouped);
	sq->last_used = e;
	sq->used_card = sq->rel->card;
	return 0;
}

int
query_outer_used_card(sql_query *q, int i)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);

	return sq->used_card;
}

sql_exp *
query_outer_last_used(sql_query *q, int i)
{
	stacked_query *sq = sql_stack_fetch(q->outer, i);

	return sq->last_used;
}
