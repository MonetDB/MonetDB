/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2021 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_query.h"
#include "rel_partition.h"
#include "rel_optimizer.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"
#include "rel_updates.h"
#include "sql_env.h"

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
			return (lng)store->storage_api.count_col(sql->session->tr, ol_first_node(t->columns)->data, 0);
		}
		if (!t && rel->r) /* dict */
			return (lng)sql_trans_dist_count(sql->session->tr, rel->r);
		return 0;
	}
	default:
		assert(0);
		return 0;
	}
}

static void
find_basetables(mvc *sql, sql_rel *rel, list *tables )
{
	if (THRhighwater()) {
		(void) sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return;
	}

	if (!rel)
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
		if (rel->l)
			find_basetables(sql, rel->l, tables);
		if (rel->r)
			find_basetables(sql, rel->r, tables);
		break;
	case op_semi:
	case op_anti:
	case op_groupby:
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

static sql_rel *
_rel_partition(mvc *sql, sql_rel *rel)
{
	list *tables = sa_list(sql->sa);
	/* find basetable relations */
	/* mark one (largest) with REL_PARTITION */
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
		r->flag = REL_PARTITION;
	}
	return rel;
}

static int
has_groupby(sql_rel *rel)
{
	if (!rel)
		return 0;
	if (is_groupby(rel->op))
		return 1;
	if (is_join(rel->op) || is_semi(rel->op) || is_set(rel->op))
		return has_groupby(rel->l) || has_groupby(rel->r);
	if (is_simple_project(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op))
		return has_groupby(rel->l);
	if (is_modify(rel->op))
		return has_groupby(rel->r);
	if (is_ddl(rel->op)) {
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view)
			return has_groupby(rel->l);
		if (rel->flag == ddl_list || rel->flag == ddl_exception)
			return has_groupby(rel->l) || has_groupby(rel->r);
	}
	if (rel->op == op_table && (IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION))
		return has_groupby(rel->l);
	return 0;
}

sql_rel *
rel_partition(mvc *sql, sql_rel *rel)
{
	if (THRhighwater())
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	if (is_basetable(rel->op)) {
		rel->flag = REL_PARTITION;
	} else if (is_simple_project(rel->op) || is_select(rel->op) || is_groupby(rel->op) || is_topn(rel->op) || is_sample(rel->op)) {
		if (rel->l)
			rel_partition(sql, rel->l);
	} else if (is_modify(rel->op)) {
		if (rel->r && rel->card <= CARD_AGGR)
			rel_partition(sql, rel->r);
	} else if (is_semi(rel->op) || is_set(rel->op)) {
		if (rel->l)
			rel_partition(sql, rel->l);
		if (rel->r)
			rel_partition(sql, rel->r);
	} else if (is_join(rel->op)) {
		if (has_groupby(rel->l) || has_groupby(rel->r)) {
			if (rel->l)
				rel_partition(sql, rel->l);
			if (rel->r)
				rel_partition(sql, rel->r);
		} else
			_rel_partition(sql, rel);
	} else if (is_ddl(rel->op)) {
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_partition(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_partition(sql, rel->l);
			if (rel->r)
				rel_partition(sql, rel->r);
		}
	} else if (rel->op == op_table) {
		if ((IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) && rel->l)
			rel_partition(sql, rel->l);
	} else {
		assert(0);
	}
	return rel;
}
