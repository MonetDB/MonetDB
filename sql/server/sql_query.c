/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2020 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_query.h"

static stacked_query *
sq_create( sql_allocator *sa, sql_rel *rel, int sql_state)
{
	stacked_query *q = SA_NEW(sa, stacked_query);

	assert(rel);
	q->rel = rel;
	q->sql_state = sql_state;
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
}

int 
query_has_outer(sql_query *q)
{
	return sql_stack_top(q->outer);
}
