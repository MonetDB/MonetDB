/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2018 MonetDB B.V.
 */

/*#define DEBUG*/

#include "monetdb_config.h"
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

		if (t && isTable(t))
			return (lng)store_funcs.count_col(sql->session->tr, t->columns.set->h->data, 1);
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
find_basetables( sql_rel *rel, list *tables )
{
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
		break;
	case op_join: 
	case op_left: 
	case op_right: 
	case op_full: 

	case op_semi: 
	case op_anti: 
	case op_apply: 

	case op_union: 
	case op_inter: 
	case op_except: 
		if (rel->l)
			find_basetables(rel->l, tables); 
		if (rel->r)
			find_basetables(rel->r, tables); 
		break;
	case op_groupby: 
	case op_project:
	case op_select: 
	case op_topn: 
	case op_sample: 
		if (rel->l)
			find_basetables(rel->l, tables); 
		break;
	case op_ddl: 
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		if (rel->r)
			find_basetables(rel->r, tables); 
		break;
	}
}

static sql_rel *
_rel_partition(mvc *sql, sql_rel *rel) 
{
	list *tables = sa_list(sql->sa); 
	/* find basetable relations */
	/* mark one (largest) with REL_PARTITION */
	find_basetables(rel, tables); 
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
	if (rel->op == op_groupby) 
		return 1;
	if (is_join(rel->op)) 
		return has_groupby(rel->l) || has_groupby(rel->r);
	if ((is_select(rel->op) || is_project(rel->op)) && rel->l) 
		return has_groupby(rel->l);
	return 0;
}

sql_rel *
rel_partition(mvc *sql, sql_rel *rel) 
{
	(void)sql;
	if (rel->op == op_basetable) {
		rel->flag = REL_PARTITION;
	} else if ((rel->op == op_topn || rel->op == op_sample || rel->op == op_select) && rel->l) {
		rel_partition(sql, rel->l);
	} else if (is_modify(rel->op) && rel->card <= CARD_AGGR) {
		if (rel->r)
			rel_partition(sql, rel->r);
	} else if (is_project(rel->op) && rel->l) {
		rel_partition(sql, rel->l);
	} else if (rel->op == op_semi && rel->l && rel->r) {
		rel_partition(sql, rel->l);
		rel_partition(sql, rel->r);
	} else if (rel->op == op_anti && rel->l && rel->r) {
		rel_partition(sql, rel->l);
		rel_partition(sql, rel->r);
	} else if (is_join(rel->op)) {
		if (has_groupby(rel->l) || has_groupby(rel->r)) {
			rel_partition(sql, rel->l);
			rel_partition(sql, rel->r);
		}
		else
			_rel_partition(sql, rel);
	}
	return rel;
}
