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
	case op_left:
	case op_right:
	case op_full:
	case op_munion:
		assert(rel->l);
		append(tables, rel);
		/*
		for (node *n = ((list*)rel->l)->h; n; n = n->next)
			find_basetables(sql, n->data, tables);
			*/
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
 * A nested parallel blocks is lifted by an extra reference, making sure the inner
 * block is executed before the outer block.
 */
#define REL_PARTITION 1
#define SPB 2
#define EPB 3
#define NPB 4

static int
rel_mark_partition(mvc *sql, sql_rel *rel)
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
	case op_inter:
	case op_except:
	case op_insert:
	case op_update:
	case op_delete:
	case op_merge:
		if (rel->l) {
			res = rel_mark_partition(sql, rel->l);
			if (res == REL_PARTITION)
				rel->spb = 1;
			if (res) {
				rel->partition = 1;
				res = EPB;
			}
		}
		if (!res && rel->r) {
			res = rel_mark_partition(sql, rel->r);
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
		if ((is_simple_project(rel->op) || is_select(rel->op)) && exps_have_unsafe(rel->exps, 1, false)) {

			return 0;
		}
		if (rel->l)
			res = rel_mark_partition(sql, rel->l);
		if (res == REL_PARTITION || res == EPB) {
			rel->partition = 1;
			if (is_semi(rel->op) && res == REL_PARTITION)
				rel->spb = 1;
		}
		break;
	case op_ddl:
		if (rel->flag == ddl_output || rel->flag == ddl_create_seq || rel->flag == ddl_alter_seq/* || rel->flag == ddl_alter_table || rel->flag == ddl_create_table || rel->flag == ddl_create_view*/) {
			if (rel->l) {
				res = rel_mark_partition(sql, rel->l);
				if (res)
					rel->partition = res;
			}
		} else if (rel->flag == ddl_list || rel->flag == ddl_exception) {
			if (rel->l) {
				res = rel_mark_partition(sql, rel->l);
				if (res)
					rel->partition = res;
			} else if (!res && rel->r) {
				res = rel_mark_partition(sql, rel->r);
				if (res)
					rel->partition = 2;
			}
		}
		break;
    case op_munion:
		if (need_distinct(rel) || is_single(rel))
				break;
		for (node *n = ((list*)rel->l)->h; n; n = n->next) {
			int lres = rel_mark_partition(sql, n->data);
			if (lres) {
				rel->partition = 1;
				res = lres;
			}
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
	return rel_mark_partition(sql, rel);
}

#if 0
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
#endif

static bool
rel_groupby_partition_safe(sql_rel *rel)
{
	if (rel->l) {
		sql_rel *l = rel->l;
		if (is_simple_project(l->op) && list_empty(l->exps))
			return false;
	}
	if (list_empty(rel->r)) {
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
	}
	if (!list_empty(rel->r)) {
		for(node *n = rel->exps->h; n; n = n->next ) {
			sql_exp *e = n->data;

			if (is_aggr(e->type)) {
				sql_subfunc *sf = e->f;
				if (sf->func->lang == FUNC_LANG_R || sf->func->lang == FUNC_LANG_PY)
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

	if (mvc_highwater(sql)) {
		sql_error(sql, 10, SQLSTATE(42000) "Query too complex: running out of stack space");
		return 0;
	}
	if (find_prop(rel->p, PROP_REMOTE))
		return 0;
	if (rel_is_ref(rel))
		pb = 0;

	if (is_basetable(rel->op)) {
		if (pb) {
			rel->spb = 1;
			rel->partition = 1;
			res = SPB;
		}
	} else if (is_groupby(rel->op)) {
		bool safe = rel_groupby_partition_safe(rel) && !rel_is_ref(rel);
		if (rel->l)
			/* if `safe`, process this GROUP BY + subtree in a `pb`. */
			res = rel_partition_(sql, rel->l, safe?SPB:0);
		if (safe) {
			rel->parallel = 1;
			if (res == REL_PARTITION)
				/* partition via bind (still) needed in the subtree, let's start one at this `rel`. */
				rel->spb = 1; // spb + parallel means, the pb for the blocking operator is started here
			if (pb) {
				rel_dup(rel); // inc-ref nested parallel block
				res = 0;
			} else {
				/* GROUP BY is a blocking operation, so it always ends a `pb`
				 * if it has started one.
				 */
				res = EPB;
			}
		}
	} else if (is_topn(rel->op)) {
		/* e.g. pp is not useful for "SELECT 42 LIMIT 2" */
		bool pp_useful = (get_rel_count(rel->l) > 1) && !(list_length(rel->exps) > 1) /* no offset */;
		/* op_topn always has rel->l */
		res = rel_partition_(sql, rel->l, pp_useful?SPB:pb);
		if (pp_useful && res) { /* topn is blocking */
			rel->parallel = 1;
			if (res == REL_PARTITION)
				/* partition via bind (still) needed in the subtree,
				 * let's start on at this `rel`. */
				rel->spb = 1; // spb + parallel means, the pb for the blocking operator is started here
			if (pb) { /* nested */
				rel_dup(rel);
				res = 0;
				rel->partition = 1; // ??
				//if (res)
					//res = SPB;
			} //else
				res = EPB;
		}
		/* else: !pp_useful: either there was no 'pb' at all, or a 'pb'
		 * has been started in the subtree (e.g. by a GROUP BY). In the
		 * 2nd case, don't try to end the 'pb', instead, leave it to
		 * the upper tree to end it, and this topN might be computed
		 * multiple times */
	} else if (is_simple_project(rel->op) || is_select(rel->op) || is_sample(rel->op)) {
		if (pb && (is_simple_project(rel->op) || is_select(rel->op)) && exps_have_unsafe(rel->exps, 1, false)) {
			rel_dup(rel); // inc-ref unsafe exps (ie order dependent)
			rel->spb = 1; // ? after ?
			return 0;
		}
		if (rel->l)
			res = rel_partition_(sql, rel->l, pb?pb:!list_empty(rel->r)?SPB:0);
		/* handle streaming projections and blocking order by */
		if (list_empty(rel->r)) {
			if (pb) {
				rel->spb = (res == REL_PARTITION);
				if (rel->spb)
					res = SPB;
			} else {
				if (res == REL_PARTITION)
					rel->partition = 1;
			}
		} else {
			rel->parallel = 1;
			rel->spb = (res == REL_PARTITION);
			if (pb) { /* nested */
				rel_dup(rel);
				res = 0;
				rel->partition = 1; // ??
			} else {
				res = EPB;
			}
		}
	} else if (is_semi(rel->op)) {
		if (rel->l && rel->op != op_anti)
			res = rel_partition_(sql, rel->l, pb);
		if (!res)
			return 0;
		/* We always use a 'pb' for rel->l of a semijoin. But, if this
		 * semijoin is not inside an active 'pb' (EPB: 'pb' ended by
		 * subtree, REL_PARTITION: 'pb' hasn't started), it needs to
		 * start a 'pb' itself. */
		if (res == EPB || res == REL_PARTITION) {
			rel->partition = 1;
			if (pb && res == REL_PARTITION) {
				rel->spb = 1;
				res = SPB;
				return res;
			}
		}
		// TODO: the following block code should probably be removed.
		//       Instead of force returning a 0, the code above should
		//       assign 'res' the proper value
		sql_rel *r = rel->r;
		if (!is_basetable(r->op)) {
			return 0;
		}
	} else if (rel->op == op_munion) {
		list *rels = rel->l;
		if (is_recursive(rel) || need_distinct(rel) || is_single(rel))
			return 0;
		for(node *n = rels->h; n; n = n->next) {
			int lres = rel_partition_(sql, n->data, pb);
			if (lres == EPB) {
				rel->partition = 1;
				if (pb)
					rel_dup(n->data); // nested
			}
		}
		if (pb)
			rel->spb = 1;
		res = pb;
	} else if (is_set(rel->op) || is_merge(rel->op)) {
		if (pb) { /* somewhat simplified */
			rel->spb = 1;
			return SPB;
		}
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
		if (rel->r /*&& rel->card <= CARD_AGGR*/)
			res = rel_partition_(sql, rel->r, pb);
		if (rel->returning) {
			if (pb)
				rel->spb = 1;
			res = pb;
		}
	} else if (is_join(rel->op)) {
		if (pb && is_outerjoin(rel->op))
			return 0;
		if (is_left(rel->op)) /* and pb == 0 */
			return rel_partition_(sql, rel->l, pb);
		/* For now we only try to partition in case of a equi-join.
		 * The other joins are too complex to handle. */
		if (pb) { /* and rel->op == op_join */
			if (!rel->partition)
				res = _rel_partition(sql, rel);
			if (res) {
				int lres = rel_partition_(sql, rel->l, (rel->partition==1 && rel->spb)?pb:0);
				if (lres == EPB && pb)
					rel_dup(rel->l);
				int rres = rel_partition_(sql, rel->r, (rel->partition==2 && rel->spb)?pb:0);
				if (rres == EPB && pb)
					rel_dup(rel->r);
				if (pb)
					res = 0;
			}
			if (!res) {
				rel->spb = 1;
				res = SPB;
			}
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
		sql_exp *op = rel->r;
        if (rel->flag != TRIGGER_WRAPPER && op) {
            sql_subfunc *f = op->f;
            if (f->func->lang == FUNC_LANG_INT && (strcmp(f->func->base.name, "file_loader") == 0)) {
				if (pb)
					rel->spb = 1;
				return pb;
            }
		}
		return 0;
	} else {
		assert(0);
	}
	if (rel_is_ref(rel))
		return 0;
	return res;
}

static sql_subfunc *
find_func( mvc *sql, char *name, list *exps )
{
	list * l = sa_list(sql->sa);
	node *n;

	for(n = exps->h; n; n = n->next)
		append(l, exp_subtype(n->data));
	return sql_bind_func_(sql, "sys", name, l, F_FUNC, false, true);

}

static sql_exp *
rel_find_aggr_exp(mvc *sql, sql_rel *rel, list *exps, sql_exp *e, char *name)
{
 	list *ea = e->l;
	sql_exp *a = NULL, *eae;
	node *n;

	(void)rel;
	if (list_length(ea) != 1)
		return NULL;
	eae = ea->h->data;
	if (eae->type != e_column)
		return NULL;
	for( n = exps->h; n; n = n->next) {
		a = n->data;

		if (a->type == e_aggr) {
			sql_subfunc *af = a->f;
			list *aa = a->l;

			/* TODO handle distinct and no-nil etc ! */
			if (strcmp(af->func->base.name, name) == 0 &&
				/* TODO handle count (has no args!!) */
			    aa && list_length(aa) == 1) {
				sql_exp *aae = aa->h->data;

				if (eae->type == e_column &&
				    ((!aae->l && !eae->l) ||
				    (aae->l && eae->l &&
				    strcmp(aae->l, eae->l) == 0)) &&
				    (aae->r && eae->r &&
				    strcmp(aae->r, eae->r) == 0))
					return exp_ref(sql, a);
			}
		}
	}
	return NULL;
}

static sql_exp *
find_aggr_exp(mvc *sql, list *exps, char *name)
{
	node *n;

	for( n = exps->h; n; n = n->next) {
		sql_exp *a = n->data;

		if (a->type == e_aggr) {
			sql_subfunc *af = a->f;

			if (strcmp(af->func->base.name, name) == 0)
				return exp_ref(sql, a);
		}
	}
	return NULL;
}

/* Filter out the 0's introducted by the parallel group by.
 * Must be done before using the values. */
static sql_rel *
rel_count_gt_zero(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op) && rel->parallel) {
		list *exps, *gbe;

		gbe = rel->r;
		if (!gbe || list_empty(gbe) || is_rewrite_gt_zero_used(rel->used))
			return rel;
		/* introduce select * from l where cnt > 0 */
		/* find count */
		if (list_empty(rel->exps)) /* no result expressions, just project the extends */
			rel->exps = rel_projections(sql, rel, NULL, 1, 1);
		exps = rel_projections(sql, rel, NULL, 1, 1);
		sql_exp *e = find_aggr_exp(sql, rel->exps, "count"), *ea = e;
		if (e && e->type == e_column)
			ea = exps_find_exp(rel->exps, e);
		if (!ea || !list_empty(ea->l)) {
			sql_subfunc *cf = sql_bind_func(sql, "sys", "count", sql_bind_localtype("void"), NULL, F_AGGR, true, true);

			e = exp_aggr(sql->sa, NULL, cf, 0, 0, CARD_AGGR, 0);

			exp_label(sql->sa, e, ++sql->label);
			append(rel->exps, e);
			e = exp_ref(sql, e);
		}
		rel->used |= rewrite_gt_zero_used;
		e = exp_compare(sql->sa, e, exp_atom_lng(sql->sa, 0), cmp_notequal);
		rel = rel_select(sql->sa, rel, e);
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
		rel = rel_project(sql->sa, rel, exps);
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
	}
	return rel;
}


/* rewrite avg into sum/count */
static sql_rel *
rel_avg_rewrite(visitor *v, sql_rel *rel)
{
	mvc *sql = v->sql;
	if (is_groupby(rel->op) && rel->parallel) {
		list *pexps, *nexps = new_exp_list(sql->sa), *avgs = new_exp_list(sql->sa);
		list *aexps = new_exp_list(sql->sa); /* alias list */
		node *m, *n;

		if (mvc_debug_on(sql, 64)) /* disable rewriter with sql_debug=64 */
			return rel;

		/* Find all avg's */
		for (m = rel->exps->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					sql_subtype *rt = exp_subtype(e);
					sql_subtype *it = first_arg_subtype(e);
					if ((EC_APPNUM(rt->type->eclass) && !EC_APPNUM(it->type->eclass)) || /* always rewrite floating point average */
						(rt->type->localtype > it->type->localtype)) {	/* always rewrite if result type is large enough */
						append(avgs, e);
						continue;
					}
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column &&
			   (!list_find_exp(rel->r, e) &&
			    !rel_find_exp(rel->l, e)))
				append(aexps, e);
			else
				append(nexps, e);
		}
		if (!list_length(avgs))
			return rel;

		/* For each avg, find count and sum */
		for (m = avgs->h; m; m = m->next) {
			list *args;
			sql_exp *avg = m->data, *navg, *cond, *cnt_d;
			sql_exp *cnt = rel_find_aggr_exp(sql, rel, nexps, avg, "count");
			sql_exp *sum = rel_find_aggr_exp(sql, rel, nexps, avg, "sum");
			sql_subfunc *div, *ifthen, *cmp;
			list *l = avg->l;
			sql_subtype *avg_input_t = exp_subtype(l->h->data);


			/* create nsum/cnt exp */
			if (!cnt) {
				sql_subfunc *cf = sql_bind_func_(sql, "sys", "count", append(sa_list(sql->sa), avg_input_t), F_AGGR, false, true);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), cf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				append(nexps, e);
				cnt = exp_ref(sql, e);
			}
			if (!sum) {
				sql_subfunc *sf = sql_bind_func_(sql, "sys", "sum", append(sa_list(sql->sa), avg_input_t), F_AGGR, false, true);
				sql_exp *e = exp_aggr(sql->sa, list_dup(avg->l, (fdup)NULL), sf, need_distinct(avg), need_no_nil(avg), avg->card, has_nil(avg));

				append(nexps, e);
				sum = exp_ref(sql, e);
			}
			cnt_d = cnt;

			sql_subtype *avg_t = exp_subtype(avg);
			sql_subtype *dbl_t = sql_bind_localtype("dbl");
			if (subtype_cmp(avg_t, dbl_t) == 0 || EC_INTERVAL(avg_t->type->eclass)) {
				/* check for count = 0 (or move into funcs) */
				args = new_exp_list(sql->sa);
				append(args, cnt);
				append(args, exp_atom_lng(sql->sa, 0));
				cmp = find_func(sql, "=", args);
				assert(cmp);
				cond = exp_op(sql->sa, args, cmp);

				args = new_exp_list(sql->sa);
				append(args, cond);
				append(args, exp_atom(sql->sa, atom_general(sql->sa, exp_subtype(cnt_d), NULL, 0)));
				/* TODO only ifthenelse if value column may have nil's*/
				append(args, cnt_d);
				ifthen = find_func(sql, "ifthenelse", args);
				assert(ifthen);
				cnt_d = exp_op(sql->sa, args, ifthen);

				if (subtype_cmp(avg_t, dbl_t) == 0) {
					cnt_d = exp_convert(sql, cnt, exp_subtype(cnt), dbl_t);
					sum = exp_convert(sql, sum, exp_subtype(sum), dbl_t);
				}

				args = new_exp_list(sql->sa);

				sql_subtype *st = exp_subtype(sum);
				sql_subtype *ct = exp_subtype(cnt_d);
				/* convert sum flt -> dbl */
				if (st->type->eclass == EC_FLT && ct->type->eclass == EC_FLT && st->type->localtype < ct->type->localtype) {
					sum = exp_convert(sql, sum, st, ct);
				} else if (st->type->eclass == EC_FLT) {
					if (ct->type != st->type) {
						sql_subtype *dbl_t = sql_bind_localtype("dbl");
						if (ct->type->eclass != EC_FLT || st->type == dbl_t->type)
							cnt_d = exp_convert(sql, cnt_d, exp_subtype(cnt_d), st);
					}
				}
				append(args, sum);
				append(args, cnt_d);
				div = find_func(sql, "sql_div", args);
				assert(div);
				navg = exp_op(sql->sa, args, div);
			} else {
				args = sa_list(sql->sa);
				append(args, sum);
				append(args, cnt_d);
				div = find_func(sql, "num_div", args);
				assert(div);
				navg = exp_op(sql->sa, args, div);
			}

			if (subtype_cmp(exp_subtype(avg), exp_subtype(navg)) != 0)
				navg = exp_convert(sql, navg, exp_subtype(navg), exp_subtype(avg));

			exp_prop_alias(sql->sa, navg, avg);
			assert(navg);
			m->data = navg;
		}
		pexps = new_exp_list(sql->sa);
		for (m = rel->exps->h, n = avgs->h; m; m = m->next) {
			sql_exp *e = m->data;

			if (e->type == e_aggr) {
				sql_subfunc *a = e->f;

				if (strcmp(a->func->base.name, "avg") == 0) {
					sql_subtype *rt = exp_subtype(e);
					sql_subtype *it = first_arg_subtype(e);
					if ((EC_APPNUM(rt->type->eclass) && !EC_APPNUM(it->type->eclass)) || /* always rewrite floating point average */
						(rt->type->localtype > it->type->localtype)) {	/* always rewrite if result type is large enough */
						sql_exp *avg = n->data;

						append(pexps, avg);
						n = n->next;
						continue;
					}
				}
			}
			/* alias for local aggr exp */
			if (e->type == e_column && !rel_find_exp(rel->l, e))
				append(pexps, e);
			else
				append(pexps, exp_ref(sql, e));
		}
		sql_rel *nrel = rel_groupby(sql, rel_dup(rel->l), rel->r);
		nrel->parallel = rel->parallel;
		nrel->partition = rel->partition;
		nrel->spb = rel->spb;
		set_count_prop(v->sql->sa, nrel, get_rel_count(rel));
		rel_destroy(rel);
		nrel->exps = nexps;
		rel = rel_project(sql->sa, nrel, pexps);
		set_count_prop(v->sql->sa, rel, get_rel_count(rel->l));
		set_processed(rel);
		v->changes++;
	}
	return rel;
}

static sql_rel *
rel_add_orderby(visitor *v, sql_rel *rel)
{
	if (is_groupby(rel->op)) {
		if (list_empty(rel->exps)) /* empty */
			return rel_project_exp(v->sql, exp_atom_bool(v->sql->sa, 1));
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
	rel = rel_visitor_bottomup(&v, rel, &rel_avg_rewrite);

	if (!sql->recursive)
	(void)rel_partition_(sql, rel, 0);

	rel = rel_visitor_bottomup(&v, rel, &rel_count_gt_zero);
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
