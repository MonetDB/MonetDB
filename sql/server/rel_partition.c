/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "sql_query.h"
#include "rel_partition.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_rewriter.h"

static int rel_partition_(mvc *sql, sql_rel *rel, int pb);

/* Returns the row count of a base table or any count info we can get fom the
 * PROP_COUNT of this 'rel' (i.e.  get_rel_count()). */
static lng
rel_getcount(mvc *sql, sql_rel *rel)
{
	if (!sql->session->tr)
		return 0;

	switch(rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isTable(t)) {
			sqlstore *store = sql->session->tr->store;
			lng nr =  (lng)store->storage_api.count_col(sql->session->tr, ol_first_node(t->columns)->data, 0);
			assert(nr >= 0);
			return nr;
		}
		return 0;
	}
	case op_groupby:
		if (rel->l && rel->r)
			return rel_getcount(sql, rel->l);
		return 1; /* Global GROUP BY always returns 1 row. */
	default:
		if (rel->l)
			return rel_getcount(sql, rel->l);
		if (rel->p)
			return get_rel_count(rel);
		return 0;
	}
}

static void
find_basetables(mvc *sql, sql_rel *rel, list *tables )
{
	if (mvc_highwater(sql)) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return;
	}

	if (!rel || rel_is_ref(rel))
		return;
	switch (rel->op) {
	case op_basetable: {
		sql_table *t = rel->l;

		if (t && isTable(t))
			append(tables, rel);
		break;
	}
	case op_table:
		if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
			if (rel->l)
				find_basetables(sql, rel->l, tables);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_union:
	case op_inter:
	case op_except:
	case op_insert:
	case op_update:
	case op_delete:
	case op_merge:
		if (rel->l)
			find_basetables(sql, rel->l, tables);
		if (rel->r)
			find_basetables(sql, rel->r, tables);
		break;
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			find_basetables(sql, n->data, tables);
		break;
	case op_semi:
	case op_anti:
	case op_groupby:
		return ;
	case op_project:
	case op_select:
	case op_topn:
	case op_sample:
	case op_truncate:
		if (rel->l)
			find_basetables(sql, rel->l, tables);
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq/* || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			if (rel->l)
				find_basetables(sql, rel->l, tables);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				find_basetables(sql, rel->l, tables);
			if (rel->r)
				find_basetables(sql, rel->r, tables);
		}
		break;
	}
}

/* To start parallel processing within a (query plan) graph, we need to mark
 * the places where partitioning is needed, and where to start or end a
 * parallel block.
 * REL_PARTITION: partition the table via bind (needed)
 * SPB: Start Parallel Block
 * EPB: End Parallel Block
 * NPB: currently not used
 */
#define REL_PARTITION 1
#define SPB 2
#define EPB 3
#define NPB 4

static int
rel_mark_partition(sql_rel *rel)
{
	int res = 0;

	if (!rel)
		return 0;
	switch (rel->op) {
	case op_basetable:
	case op_table:
		return rel->partition;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
	case op_union:
	case op_inter:
	case op_except:
	case op_insert:
	case op_update:
	case op_delete:
	case op_merge:
		if (rel->l) {
			res = rel_mark_partition(rel->l);
			if (res == REL_PARTITION)
				rel->spb = 1;
			if (res) {
				rel->partition = 1;
				res = EPB;
			}
		}
		if (!res && rel->r) {
			res = rel_mark_partition(rel->r);
			if (res == REL_PARTITION)
				rel->spb = 1;
			if (res) {
				rel->partition = 2;
				res = EPB;
			}
		}
		break;
	case op_semi:
	case op_anti:
	case op_groupby:
	case op_project:
	case op_select:
	case op_topn:
	case op_sample:
	case op_truncate:
		if ((is_simple_project(rel->op) || is_select(rel->op)) && exps_have_unsafe(rel->exps, 1))
			return 0;
		if (rel->l)
			res = rel_mark_partition(rel->l);
		if (res == REL_PARTITION || res == EPB) {
			rel->partition = 1;
			if (is_semi(rel->op) && res == REL_PARTITION)
				rel->spb = 1;
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq/* || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			if (rel->l) {
				res = rel_mark_partition(rel->l);
				if (res)
					rel->partition = res;
			}
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l) {
				res = rel_mark_partition(rel->l);
				if (res)
					rel->partition = res;
			} else if (!res && rel->r) {
				res = rel_mark_partition(rel->r);
				if (res)
					rel->partition = 2;
			}
		}
		break;
    case op_munion:
		for (node *n = ((list*)rel->l)->h; n; n = n->next) {
			// TODO: how are we going to mark rel->partition?
			res = rel_mark_partition(n->data);
		}
	}
	return res;
}

static int
_rel_partition(mvc *sql, sql_rel *rel)
{
	list *tables = sa_list(sql->sa);
	/* find basetable relations */
	/* mark one (largest) with partition */
	find_basetables(sql, rel, tables);
	if (list_length(tables)) {
		sql_rel *r;
		node *n;
		int i, mi = 0;
		lng *sizes = SA_NEW_ARRAY(sql->sa, lng, list_length(tables)), m = 0;

		for(i=0, n = tables->h; n; i++, n = n->next) {
			r = n->data;
			sizes[i] = rel_getcount(sql, r);
			if (sizes[i] > m) {
				m = sizes[i];
				mi = i;
			}
		}
		for(i=0, n = tables->h; i<mi; i++, n = n->next)
			;
		r = n->data;
		/*  TODO, we now pick first (okay?)! In case of self joins we need to pick the correct table */
		r->partition = 1;
	}
	/* Now that we've marked the (largest) table for partition, we go over
	 * this 'rel' (sub)tree to process all relational operators based on this
	 * knowledge. */
	return rel_mark_partition(rel);
}

static int
has_groupby(sql_rel *rel)
{
	if (!rel)
		return 0;

	switch (rel->op) {
		case op_groupby:
			return 1;
		case op_join:
		case op_left:
		case op_right:
		case op_full:

		case op_semi:
		case op_anti:

		case op_union:
		case op_inter:
		case op_except:

		case op_merge:
			return has_groupby(rel->l) || has_groupby(rel->r);
		case op_munion:
			for (node *n = ((list*)rel->l)->h; n; n = n->next)
				if (has_groupby(n->data))
					return 1;
			return 0;
		case op_project:
		case op_select:
		case op_topn:
		case op_sample:
			return has_groupby(rel->l);
		case op_insert:
		case op_update:
		case op_delete:
		case op_truncate:
			return has_groupby(rel->r);
		case op_ddl:
			if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view)
				return has_groupby(rel->l);
			if (rel->flag == ddl_list || rel->flag == ddl_exception)
				return has_groupby(rel->l) || has_groupby(rel->r);
			return 0;
		case op_table:
			if (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION)
				return has_groupby(rel->l);
			return 0;
		case op_basetable:
			return 0;
	}
	return 0;
}

static bool
rel_groupby_partition_safe(sql_rel *rel)
{
	for(node *n = rel->exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (is_aggr(e->type)) {
			sql_subfunc *sf = e->f;
			int sum = 0;

			if ((e->l && exps_are_atoms(e->l)) || /* e.g. SUM(42) */
				!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
				  strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
				 (sum = (strcmp(sf->func->base.name, "sum") == 0)) || strcmp(sf->func->base.name, "prod") == 0))
				return false;
		}
	}
	return true;
}

static int
do_oahash_join(sql_rel *rel)
{
	ATOMIC_TYPE use_oahash = (1U<<19);
	if (!(GDKdebug & use_oahash))
		return 0;

	// TODO full outer and anti-join
	if (rel->op == op_full || rel->op == op_anti)
		return 0;

	if (!rel->exps)
		return 1;

	/* only for equi-joins. */
	for (node *n = rel->exps->h; n; n = n->next) {
		sql_exp *e = n->data;
		if (!is_compare(e->type) || e->flag != cmp_equal)
			return 0;
	}

	return 1;
}

static int
rel_partition_(mvc *sql, sql_rel *rel, int pb)
{
	int res = 0, lres = 0, rres = 0;

	if (mvc_highwater(sql)) {
		sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}
	if (rel_is_ref(rel))
		pb = 0;

	if (is_basetable(rel->op)) {
		if (pb) {
			rel->partition = 1;
			res = REL_PARTITION;
		}
	} else if (is_groupby(rel->op)) {
		bool safe = rel_groupby_partition_safe(rel) && !rel_is_ref(rel);
		if (rel->l)
			/* if `safe`, process this GROUP BY + subtree in a `pb`. */
			res = rel_partition_(sql, rel->l, safe?SPB:0);
		if (safe) {
			rel->parallel = 1;
			if (res == REL_PARTITION)
				/* partition via bind (still) needed in the subtree,
				 * let's start one at this `rel`. */
				rel->spb = 1;
			if (pb) {
				/* If the supertree is also in a `pb`, a new `pb` should be
				 * started after this GROUP BY ends (to partition its results)
				 */
				rel->partition = 1;
				if (res) // TODO: maybe we should remove this condition, since we don't care about the subtree, instead, we always want to inform upper tree that we're starting a PB here.
					res = SPB;
			} else
				/* GROUP BY is a blocking operation, so it always ends a `pb`
				 * if it has started one.
				 */
				res = EPB;
		}
	} else if (is_topn(rel->op)) {
		/* e.g. pp is not useful for "SELECT 42 LIMIT 2" */
		bool pp_useful = (rel_getcount(sql, rel->l) > 1);
		/* op_topn always has rel->l */
		res = rel_partition_(sql, rel->l, pp_useful?SPB:pb);
		if (pp_useful) {
			rel->parallel = 1;
			if (res == REL_PARTITION)
				/* partition via bind (still) needed in the subtree,
				 * let's start on at this `rel`. */
				rel->spb = 1;
			if (pb) {
				rel->partition = 1;
				if (res)
					res = SPB;
			} else
				res = EPB;
		}
		/* else: !pp_useful: either there was no 'pb' at all, or a 'pb'
		 * has been started in the subtree (e.g. by a GROUP BY). In the
		 * 2nd case, don't try to end the 'pb', instead, leave it to
		 * the upper tree to end it, and this topN might be computed
		 * multiple times */
	} else if (is_simple_project(rel->op) || is_select(rel->op) || is_sample(rel->op)) {
		if (pb && (is_simple_project(rel->op) || is_select(rel->op)) && exps_have_unsafe(rel->exps, 1))
			return 0;
		if (rel->l)
			res = rel_partition_(sql, rel->l, pb);
		if (res == SPB)
			rel->spb = 1;
		if (res == REL_PARTITION)
			rel->partition = 1;
	} else if (is_semi(rel->op)) {
		if (do_oahash_join(rel)) {
			rel->oahash = 2;
		}

		if (rel->l)
			res = rel_partition_(sql, rel->l, pb);
		if (!res)
			return 0;
		/* We always use a 'pb' for rel->l of a semijoin. But, if this
		 * semijoin is not inside an active 'pb' (EPB: 'pb' ended by
		 * subtree, REL_PARTITION: 'pb' hasn't started), it needs to
		 * start a 'pb' itself. */
		if (res == EPB || res == REL_PARTITION) {
			rel->partition = 1;
			if (pb && res == REL_PARTITION) //{ TODO: seems that we should give 'res' its proper value here, which is SPB.
				rel->spb = 1;
				//res = SPB;
			//}
		}
		// TODO: the following block code should probably be removed.
		//       Instead of force returning a 0, the code above should
		//       assign 'res' the proper value
		sql_rel *r = rel->r;
		if (!is_basetable(r->op))
			return 0;
	} else if (is_set(rel->op) || is_merge(rel->op)) {
		if (rel->l)
			lres = rel_partition_(sql, rel->l, 0);
		if (rel->r)
			rres = rel_partition_(sql, rel->r, 0);
		if (lres == EPB)
			rel->partition = 1;
		if (rres == EPB)
			rel->partition = 1;
		if (pb)
			rel->spb = 1;
		if (!lres || !rres)
			return 0;
		res = pb;
	} else if (is_insert(rel->op) || is_update(rel->op) || is_delete(rel->op) || is_truncate(rel->op)) {
		if (rel->r && rel->card <= CARD_AGGR)
			res = rel_partition_(sql, rel->r, pb);
	} else if (is_join(rel->op)) {
		if (do_oahash_join(rel)) {

			sql_rel *l = rel->l, *r = rel->r;
			(void) rel_partition_(sql, l, 1);
			(void) rel_partition_(sql, r, 1);

			if (rel->op == op_left)
				rel->oahash = 2;
			else if (rel->op == op_right)
				rel->oahash = 1;
			else if (rel_getcount(sql, l) < rel_getcount(sql, r))
				rel->oahash = 1;
			else
				rel->oahash = 2;

			if(is_basetable(l->op))
				l->partition = 1;
			if(is_basetable(r->op))
				r->partition = 1;
			rel->parallel = 1;
		}

		if (pb && is_outerjoin(rel->op))
			return 0;

		bool l = has_groupby(rel->l), r = has_groupby(rel->r);
		if (0 && (l || r)) {
			int lres = rel_partition_(sql, rel->l, 0);
			int rres = rel_partition_(sql, rel->r, 0);
			if (!lres || !rres)
				return 0;
			if (l && lres == EPB && !r && rres == REL_PARTITION)
				res = SPB;
			if (pb) {
				rel->partition = l?1:2;
				rel->spb = 1;
			}
		} else {
			if (is_left(rel->op)) /* and pb == 0 */
				return rel_partition_(sql, rel->l, pb);
			/* For now we only try to partition in case of a equi-join.
			 * The other joins are too complex to handle. */
			if (pb) /* and rel->op == op_join */
				res = _rel_partition(sql, rel);
		}
	} else if (is_ddl(rel->op)) {
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				res = rel_partition_(sql, rel->l, pb);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				res = rel_partition_(sql, rel->l, pb);
			if (rel->r)
				res = rel_partition_(sql, rel->r, pb);
		}
	} else if (rel->op == op_table) {
		if ((IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) && rel->l)
			res = rel_partition_(sql, rel->l, pb);
		return 0;
	} else {
		assert(0);
	}
	if (rel_is_ref(rel))
		return 0;
	return res;
}

int
rel_partition(mvc *sql, sql_rel *rel)
{
	return rel_partition_(sql, rel, 0);
}
