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
 * This file contains shared functions for bin_partition_by_value and
 * bin_partition_by_slice
 */

#include "monetdb_config.h"

#include "bin_partition.h"
#include "rel_rel.h"
#include "rel_rewriter.h"

static sql_rel*
rel_is_not_pp_safe(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op) ||
	    rel->op == op_table ||
		rel->op == op_topn ||
		rel->oahash ||
		((is_join(rel->op) || is_semi(rel->op)) && rel->spb)) {
		assert(0);
		*((int*)v->data) = 1;
	}
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

/* Computes the number of slices we going to horizontally divide a table into.
 */
#define PP_MIN_SIZE (64*1024)
#define PP_MAX_SIZE (128*1024)
int
pp_nr_slices(sql_rel *rel)
{
	BUN est = get_rel_count(rel);

	if (est == BUN_NONE || (ulng) est > (ulng) GDK_lng_max)
		est = 85000000;

	int nr_slices = 1;

	if (est < PP_MIN_SIZE)
		nr_slices = 1;
	else if (est/GDKnr_threads < PP_MIN_SIZE)
		nr_slices = (int)(est/PP_MIN_SIZE);
	else
	    nr_slices =	(int)(est/PP_MAX_SIZE);
	FORCEMITODEBUG
	if (nr_slices < GDKnr_threads)
		nr_slices = GDKnr_threads;
	FORCEMITODEBUG
	if (GDKnr_threads == 1)
		nr_slices = 8;

	if (nr_slices == 0)
		return 1;
	assert(nr_slices > 0);
	return nr_slices;
}

