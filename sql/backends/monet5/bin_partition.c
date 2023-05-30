/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

/*
 * This file contains shared functions for bin_partition_by_value and
 * bin_partition_by_slice
 */

#include "monetdb_config.h"

#include "bin_partition.h"
#include "rel_rel.h"

void
set_need_pipeline(backend *be)
{
	if(be->need_pipeline)
		assert(0);
	be->need_pipeline = true;
}

bool
get_need_pipeline(backend *be)
{
	bool r = be->need_pipeline;
	be->need_pipeline = false;
	return r;
}

void
set_pipeline(backend *be, stmt *pp)
{
	be->ppstmt = pp;
}

stmt *
get_pipeline(backend *be)
{
	return be->ppstmt;
}

static sql_rel*
rel_is_not_pp_safe(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) || rel->op == op_table)
		*((int*)v->data) = 1;
	return rel;
}

bool
pp_can_not_start(mvc *sql, sql_rel *rel)
{
	int set = 0;
	visitor v = { .sql = sql, .data = &set };
	rel = rel_visitor_bottomup(&v, rel, &rel_is_not_pp_safe);
	return set;
}

