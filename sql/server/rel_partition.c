/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 1997 - July 2008 CWI, August 2008 - 2022 MonetDB B.V.
 */

#include "monetdb_config.h"
#include "sql_query.h"
#include "rel_partition.h"
#include "rel_exp.h"
#include "rel_prop.h"
#include "rel_dump.h"
#include "rel_select.h"

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
	case op_basetable: {
	case op_table:
		return rel->partition;
	}
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
	return rel_mark_partition(rel);
}

static int
has_groupby(sql_rel *rel)
{
	if (!rel)
		return 0;
	if (is_groupby(rel->op))
		return 1;
	if (is_join(rel->op) || is_semi(rel->op) || is_set(rel->op) || is_merge(rel->op))
		return has_groupby(rel->l) || has_groupby(rel->r);
	if (is_simple_project(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op))
		return has_groupby(rel->l);
	if (is_insert(rel->op) || is_update(rel->op) || is_delete(rel->op) || is_truncate(rel->op))
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

/*
 * REL_PARTITION (need partition via bind).
 */

static bool
rel_groupby_partition_safe(sql_rel *rel)
{
	for(node *n = rel->exps->h; n; n = n->next ) {
		sql_exp *e = n->data;

		if (is_aggr(e->type)) {
			sql_subfunc *sf = e->f;
			int sum = 0;

			if ((e->l && exps_are_atoms(e->l)) ||
				!(strcmp(sf->func->base.name, "min") == 0 || strcmp(sf->func->base.name, "max") == 0 ||
			      strcmp(sf->func->base.name, "avg") == 0 || strcmp(sf->func->base.name, "count") == 0 ||
			     (sum = (strcmp(sf->func->base.name, "sum") == 0)) || strcmp(sf->func->base.name, "prod") == 0))
				return false;
			if (sum && list_length(e->l) == 1) {
				list *l = e->l;
				sql_exp *i = l->h->data;
				sql_subtype *t = exp_subtype(i);

				if (EC_APPNUM(t->type->eclass))
					/* TODO in case of a safe range (to be defined) or user override we could still do simple dbl/float sums * */
					return false;
			}
		}
	}
	return true;
}

static int
rel_partition_(mvc *sql, sql_rel *rel, int pb)
{
	int res = 0, lres = 0, rres = 0;
	sql_rel *l = rel->l;

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
			res = rel_partition_(sql, rel->l, safe?SPB:pb);
		if (safe) {
			rel->parallel = 1;
			if (res == REL_PARTITION)
				rel->spb = 1;
			if (pb) {
				rel->partition = 1;
				if (res)
					res = SPB;
			} else
				res = EPB;
		}
	} else if (is_topn(rel->op) && (l && (!is_simple_project(l->op) || list_empty(l->r)))) {
		bool safe = !has_groupby(rel->l); /* no partitioning after a group by */
		if (rel->l)
			res = rel_partition_(sql, rel->l, safe?SPB:pb);
		if (safe) {
			rel->parallel = 1;
			if (res == REL_PARTITION)
				rel->spb = 1;
			if (pb) {
				rel->partition = 1;
				if (res)
					res = SPB;
			} else
				res = EPB;
		}
	} else if (is_simple_project(rel->op) || is_select(rel->op) || is_topn(rel->op) || is_sample(rel->op)) {
		if (pb && is_simple_project(rel->op) && rel->r)
			return 0;
		//if (pb && exps_have_unsafe(rel->exps, 1))
		if (pb && (is_simple_project(rel->op) || is_select(rel->op)) && exps_have_unsafe(rel->exps, 1))
			return 0;
		if (rel->l)
			res = rel_partition_(sql, rel->l, pb);
		if (res == SPB)
			rel->spb = 1;
		if (res == REL_PARTITION)
			rel->partition = 1;
	} else if (is_semi(rel->op)) {
		//if (rel->op == op_anti) /* no partitioning for anti joins jet */
			//return 0;
		if (rel->l)
			res = rel_partition_(sql, rel->l, pb);
		if (!res)
			return 0;
		if (res == EPB || res == REL_PARTITION) {
			rel->partition = 1;
			if (pb && res == REL_PARTITION)
				rel->spb = 1;
		}
		sql_rel *r = rel->r;
		if (!is_basetable(r->op))
		   return 0;
	} else if (is_set(rel->op) || is_merge(rel->op)) {
		if (rel->l)
			lres = rel_partition_(sql, rel->l, 0);
		if (rel->r && !is_semi(rel->op))
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
		if (pb && is_outerjoin(rel->op))
			return 0;
		/* TODo also move this into rel_partition_ */
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
			if (is_left(rel->op))
				return rel_partition_(sql, rel->l, pb);
			if (pb)
				res =_rel_partition(sql, rel);
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
