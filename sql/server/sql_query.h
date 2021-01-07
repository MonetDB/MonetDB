/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#ifndef _SQL_QUERY_H_
#define _SQL_QUERY_H_

#include "sql_relation.h"
#include "sql_stack.h"
#include "sql_mvc.h"

typedef struct stacked_query {
	sql_rel *rel;
	int sql_state;
	sql_exp *last_used;
	int used_card;
	int grouped;
	int groupby;
} stacked_query;

typedef struct sql_query {
	mvc *sql;
	sql_stack *outer;
} sql_query;

extern sql_query *query_create(mvc *sql);
extern void query_push_outer(sql_query *q, sql_rel *r, int sql_state);
extern sql_rel *query_pop_outer(sql_query *q);
extern sql_rel *query_fetch_outer(sql_query *q, int i);
extern int query_fetch_outer_state(sql_query *q, int i);
extern void query_update_outer(sql_query *q, sql_rel *r, int i);
extern int query_has_outer(sql_query *q); /* returns number of outer relations */

extern int query_outer_used_exp(sql_query *q, int i, sql_exp *e, int f);
extern void query_outer_pop_last_used(sql_query *q, int i);
extern int query_outer_aggregated(sql_query *q, int i, sql_exp *e);
extern int query_outer_used_card(sql_query *q, int i);
extern sql_exp *query_outer_last_used(sql_query *q, int i);

#endif
