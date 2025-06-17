/*
 * SPDX-License-Identifier: MPL-2.0
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0.  If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2024, 2025 MonetDB Foundation;
 * Copyright August 2008 - 2023 MonetDB B.V.;
 * Copyright 1997 - July 2008 CWI.
 */

#include "monetdb_config.h"
#include "rel_physical.h"
#include "rel_optimizer_private.h"
#include "rel_rewriter.h"
#include "rel_exp.h"
#include "rel_rel.h"
#include "sql_storage.h"
#include "rel_bin.h"

#define IS_ORDER_BASED_AGGR(fname, argc) (\
				(argc == 2 && (strcmp((fname), "quantile") == 0 || strcmp((fname), "quantile_avg") == 0)) || \
				(argc == 1 && (strcmp((fname), "median") == 0 || strcmp((fname), "median_avg") == 0)))


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

		if (t && isTable(t) && t->persistence != SQL_DECLARED_TABLE) {
			sqlstore *store = sql->session->tr->store;
			lng nr = (lng)store->storage_api.count_col(sql->session->tr, ol_first_node(t->columns)->data, 0);
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
	case op_munion:
		assert(rel->l);
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			find_basetables(sql, n->data, tables);
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

	switch (rel->op) {
		case op_groupby:
			return 1;
		case op_join:
		case op_left:
		case op_right:
		case op_full:

		case op_semi:
		case op_anti:

		case op_inter:
		case op_except:
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

static sql_rel *
rel_partition(mvc *sql, sql_rel *rel)
{
	if (mvc_highwater(sql))
		return sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");

	switch (rel->op) {
	case op_basetable:
	case op_sample:
		rel->flag = REL_PARTITION;
		break;
	case op_project:
	case op_select:
	case op_groupby:
	case op_topn:
		if (rel->l)
			rel_partition(sql, rel->l);
		break;
	case op_semi:
	case op_anti:

	case op_inter:
	case op_except:
		if (rel->l)
			rel_partition(sql, rel->l);
		if (rel->r)
			rel_partition(sql, rel->r);
		break;
	case op_munion:
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			rel_partition(sql, n->data);
		break;
	case op_insert:
	case op_update:
	case op_delete:
	case op_truncate:
		if (rel->r && rel->card <= CARD_AGGR)
			rel_partition(sql, rel->r);
		break;
	case op_join:
	case op_left:
	case op_right:
	case op_full:
		if (has_groupby(rel->l) || has_groupby(rel->r)) {
			if (rel->l)
				rel_partition(sql, rel->l);
			if (rel->r)
				rel_partition(sql, rel->r);
		} else {
			_rel_partition(sql, rel);
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view) {
			if (rel->l)
				rel_partition(sql, rel->l);
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l)
				rel_partition(sql, rel->l);
			if (rel->r)
				rel_partition(sql, rel->r);
		}
		break;
	case op_table:
		if ((IS_TABLE_PROD_FUNC(rel->flag) || rel->flag == TABLE_FROM_RELATION) && rel->l)
			rel_partition(sql, rel->l);
		break;
	default:
		assert(0);
		break;
	}
	return rel;
}

static sql_rel *
rel_add_orderby(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op)) {
		if (rel->exps && !rel->r) { /* find quantiles */
			sql_exp *obe = NULL, *oberef = NULL;
			for(node *n = rel->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (is_aggr(e->type)) {
					sql_subfunc *af = e->f;
					list *aa = e->l;

					/* for now we only handle one sort order */
					if (aa && IS_ORDER_BASED_AGGR(af->func->base.name, list_length(aa))) {
						sql_exp *nobe = aa->h->data;
						if (nobe && !obe) {
							sql_rel *l = rel->l = rel_project(v->sql->sa, rel->l, rel_projections(v->sql, rel->l, NULL, 1, 1));
							obe = nobe;
							oberef = nobe;
							if (l) {
								if (!is_alias(nobe->type)) {
									oberef = nobe = exp_label(v->sql->sa, exp_copy(v->sql, nobe), ++v->sql->label);
									append(l->exps, nobe);
								}
								set_nulls_first(nobe);
								set_ascending(nobe);
								aa->h->data = exp_ref(v->sql, nobe);
								list *o = l->r = sa_list(v->sql->sa);
								if (o)
									append(o, nobe);
							}
						} else if (exp_match_exp(nobe, obe)) {
							aa->h->data = exp_ref(v->sql, oberef);
						}
					}
				}
			}
			return rel;
		}
	}
	return rel;
}

static sql_exp *
exp_timezone(visitor *v, sql_rel *rel, sql_exp *e, int depth)
{
	(void)depth;
	(void)rel;
	if (e && e->type == e_func) {
		list *l = e->l;
		sql_subfunc *f = e->f;
		const char *fname = f->func->base.name;
		if (list_length(l) == 2) {
		   if (strcmp(fname, "timestamp_to_str") == 0 || strcmp(fname, "time_to_str") == 0) {
                sql_exp *e = l->h->data;
                sql_subtype *t = exp_subtype(e);
                if (t->type->eclass == EC_TIMESTAMP_TZ || t->type->eclass == EC_TIME_TZ) {
                    sql_exp *offset = exp_atom_lng(v->sql->sa, v->sql->timezone);
                    list_append(l, offset);
                }
            } else if (strcmp(fname, "str_to_timestamp") == 0 || strcmp(fname, "str_to_time") == 0 || strcmp(fname, "str_to_date") == 0) {
                sql_exp *offset = exp_atom_lng(v->sql->sa, v->sql->timezone);
                list_append(l, offset);
            }
		}
	}
	return e;
}

sql_rel *
rel_physical(mvc *sql, sql_rel *rel)
{
	visitor v = { .sql = sql };

	rel = rel_visitor_bottomup(&v, rel, &rel_add_orderby);
	if (!sql->recursive)
		(void)rel_partition(sql, rel);
	rel = rel_exp_visitor_topdown(&v, rel, &exp_timezone, true);

#ifdef HAVE_HGE
	if (rel && sql->no_int128) {
		sql_rel *r = rel;
		if (is_topn(r->op))
				r = r->l;
		if (r && is_project(r->op) && !list_empty(r->exps)) {
			for (node *n = r->exps->h; n; n = n->next) {
				sql_exp *e = n->data;

				if (exp_subtype(e)->type->localtype == TYPE_hge) /* down cast */
					e = n->data = exp_convert(sql, e, exp_subtype(e), sql_bind_localtype("lng"));
			}
		}
	}
#endif
	return rel;
}
